//! Port of `github.com/VictoriaMetrics/metrics/push.go`: periodic push of
//! metrics in Prometheus text exposition format to a remote URL.
//!
//! PORT NOTE: Go pushes with `net/http` and cancels via `context.Context`;
//! the port uses the house std-TCP HTTP/1.1 client style (one connection per
//! request, `Connection: close`, https via [`crate::tlsutil`], see
//! esl-storage's `http_client.rs` for the pattern) and cancels via
//! [`PushCancel`] (a bool + condvar standing in for `ctx.Done()`); the
//! per-push `context.WithTimeout` maps to socket connect/read/write
//! timeouts.
//!
//! PORT NOTE: only the entry points used by `lib/pushmetrics` are ported
//! (`InitPushExtWithOptions` -> [`init_push_ext_with_options`],
//! `PushMetricsExt` -> [`push_metrics_ext`]); the remaining Go wrappers
//! (`InitPush`, `InitPushProcessMetrics`, `Set::InitPush`, ...) are thin
//! sugar with no EsLogs callers.

use std::io::{Read, Write as IoWrite};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::{Condvar, LazyLock, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use flate2::Compression;
use flate2::write::GzEncoder;

use super::{MetricsWriter, Set, validator};
use crate::tlsutil;

/// The list of options, which may be applied to
/// [`init_push_ext_with_options`] (Go `PushOptions`).
#[derive(Default, Clone)]
pub struct PushOptions {
    /// An optional comma-separated list of `label="value"` labels, which must
    /// be added to all the metrics before pushing them to the push URL.
    pub extra_labels: String,

    /// An optional list of HTTP headers to add to every push request.
    ///
    /// Every item in the list must have the form `Header: value`.
    pub headers: Vec<String>,

    /// Whether to disable HTTP request body compression before sending the
    /// metrics to the push URL. By default the compression is enabled.
    pub disable_compression: bool,

    /// The HTTP request method to use when pushing metrics. By default the
    /// method is GET (like Go).
    pub method: String,
}

/// Cancellation signal for the periodic push workers spawned by
/// [`init_push_ext_with_options`] (Go passes a `context.Context` instead).
#[derive(Default)]
pub struct PushCancel {
    canceled: Mutex<bool>,
    cond: Condvar,
}

impl PushCancel {
    pub fn new() -> Self {
        PushCancel::default()
    }

    /// Signals all push workers sharing this handle to stop.
    pub fn cancel(&self) {
        let mut canceled = self.canceled.lock().unwrap_or_else(|e| e.into_inner());
        *canceled = true;
        self.cond.notify_all();
    }

    /// Waits up to `timeout`; returns true when canceled.
    fn wait(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut canceled = self.canceled.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if *canceled {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, _) = self
                .cond
                .wait_timeout(canceled, deadline - now)
                .unwrap_or_else(|e| e.into_inner());
            canceled = guard;
        }
    }
}

/// Sets up periodic push for metrics obtained by calling `write_metrics` with
/// the given interval (Go `InitPushExtWithOptions`).
///
/// The `write_metrics` callback must write metrics to its argument in
/// Prometheus text exposition format without timestamps and trailing
/// comments.
///
/// The periodic push is stopped by calling [`PushCancel::cancel`] on
/// `cancel`; joining the returned handle waits until the background worker
/// exits (Go's `opts.WaitGroup`).
///
/// It is OK calling this multiple times with different push URLs - in this
/// case metrics are pushed to all the provided URLs.
pub fn init_push_ext_with_options(
    cancel: Arc<PushCancel>,
    push_url: &str,
    interval: Duration,
    write_metrics: MetricsWriter,
    opts: &PushOptions,
) -> Result<JoinHandle<()>, String> {
    let pc = PushContext::new(push_url, opts)?;

    // Validate interval.
    //
    // PORT NOTE: `std::time::Duration` cannot be negative, so only the zero
    // case of Go's `interval <= 0` check is reachable.
    if interval.is_zero() {
        return Err(format!("interval must be positive; got {interval:?}"));
    }
    push_metrics_set()
        .get_or_create_float_counter(&format!(
            "metrics_push_interval_seconds{{url={}}}",
            crate::flagutil::go_quote(&pc.push_url_redacted)
        ))
        .set(interval.as_secs_f64());

    let handle = std::thread::spawn(move || {
        loop {
            if cancel.wait(interval) {
                return;
            }
            // Go bounds every periodic push with `interval + 1s`.
            if let Err(err) = pc.push_metrics(&write_metrics, interval + Duration::from_secs(1)) {
                eprintln!("ERROR: metrics.push: {err}");
            }
        }
    });
    Ok(handle)
}

/// Pushes metrics generated by `write_metrics` to `push_url` once
/// (Go `PushMetricsExt`).
///
/// PORT NOTE: the Go `ctx` deadline maps to the `timeout` socket cap.
pub fn push_metrics_ext(
    push_url: &str,
    write_metrics: &MetricsWriter,
    opts: &PushOptions,
    timeout: Duration,
) -> Result<(), String> {
    let pc = PushContext::new(push_url, opts)?;
    pc.push_metrics(write_metrics, timeout)
}

struct PushContext {
    url: ParsedUrl,
    method: String,
    push_url_redacted: String,
    extra_labels: String,
    /// Parsed `Name: value` pairs with canonical MIME header names.
    headers: Vec<(String, String)>,
    disable_compression: bool,
    tls: Option<tlsutil::TlsClientConfig>,
}

impl PushContext {
    /// Go `newPushContext`.
    fn new(push_url: &str, opts: &PushOptions) -> Result<PushContext, String> {
        // Validate push_url.
        let url = parse_push_url(push_url)?;

        let method = if opts.method.is_empty() {
            "GET".to_string()
        } else {
            opts.method.clone()
        };

        // Validate extra_labels.
        let extra_labels = opts.extra_labels.clone();
        validator::validate_tags(&extra_labels)
            .map_err(|err| format!("invalid extraLabels={extra_labels:?}: {err}"))?;

        // Validate headers.
        let mut headers = Vec::with_capacity(opts.headers.len());
        for h in &opts.headers {
            let Some(n) = h.find(':') else {
                return Err(format!("missing `:` delimiter in the header {h:?}"));
            };
            let name = canonical_mime_header_key(h[..n].trim());
            let value = h[n + 1..].trim().to_string();
            headers.push((name, value));
        }

        // PORT NOTE: Go's `http.Client` carries the system TLS config; the
        // port builds a default rustls client config eagerly for https URLs.
        let tls = if url.scheme == "https" {
            Some(
                tlsutil::new_tls_client_config(&tlsutil::TLSConfig::default()).map_err(|err| {
                    format!("cannot initialize tls for pushURL={push_url:?}: {err}")
                })?,
            )
        } else {
            None
        };

        let push_url_redacted = url.redacted.clone();
        // Register the per-URL push metrics like Go (the counters are
        // GetOrCreate'd once here and looked up by name on every push).
        let s = push_metrics_set();
        let q = crate::flagutil::go_quote(&push_url_redacted);
        let _ = s.get_or_create_counter(&format!("metrics_push_total{{url={q}}}"));
        let _ = s.get_or_create_counter(&format!("metrics_push_bytes_pushed_total{{url={q}}}"));
        let _ = s.get_or_create_histogram(&format!("metrics_push_block_size_bytes{{url={q}}}"));
        let _ = s.get_or_create_histogram(&format!("metrics_push_duration_seconds{{url={q}}}"));
        let _ = s.get_or_create_counter(&format!("metrics_push_errors_total{{url={q}}}"));

        Ok(PushContext {
            url,
            method,
            push_url_redacted,
            extra_labels,
            headers,
            disable_compression: opts.disable_compression,
            tls,
        })
    }

    /// Go `pushContext.pushMetrics`: builds the payload and performs one push
    /// request.
    fn push_metrics(&self, write_metrics: &MetricsWriter, timeout: Duration) -> Result<(), String> {
        let mut bb = String::new();
        write_metrics(&mut bb);

        if !self.extra_labels.is_empty() {
            bb = add_extra_labels(String::new(), &bb, &self.extra_labels);
        }
        let mut body = bb.into_bytes();
        if !self.disable_compression {
            let mut zw = GzEncoder::new(Vec::new(), Compression::default());
            zw.write_all(&body).map_err(|err| {
                format!(
                    "BUG: cannot write {} bytes to gzip writer: {err}",
                    body.len()
                )
            })?;
            body = zw
                .finish()
                .map_err(|err| format!("BUG: cannot flush metrics to gzip writer: {err}"))?;
        }

        // Update metrics.
        let s = push_metrics_set();
        let q = crate::flagutil::go_quote(&self.push_url_redacted);
        s.get_or_create_counter(&format!("metrics_push_total{{url={q}}}"))
            .inc();
        s.get_or_create_counter(&format!("metrics_push_bytes_pushed_total{{url={q}}}"))
            .add(body.len() as u64);
        s.get_or_create_histogram(&format!("metrics_push_block_size_bytes{{url={q}}}"))
            .update(body.len() as f64);

        // Perform the request.
        let start_time = Instant::now();
        let res = self.do_request(&body, timeout);
        s.get_or_create_histogram(&format!("metrics_push_duration_seconds{{url={q}}}"))
            .update_duration(start_time);
        match res {
            Ok((status_code, resp_body)) => {
                if status_code / 100 != 2 {
                    s.get_or_create_counter(&format!("metrics_push_errors_total{{url={q}}}"))
                        .inc();
                    return Err(format!(
                        "unexpected status code in response from {:?}: {status_code}; expecting 2xx; response body: {:?}",
                        self.push_url_redacted,
                        String::from_utf8_lossy(&resp_body)
                    ));
                }
                Ok(())
            }
            Err(err) => {
                s.get_or_create_counter(&format!("metrics_push_errors_total{{url={q}}}"))
                    .inc();
                Err(format!(
                    "cannot push metrics to {:?}: {err}",
                    self.push_url_redacted
                ))
            }
        }
    }

    /// Sends one HTTP/1.1 request with the given body; returns the response
    /// status code and body.
    fn do_request(&self, body: &[u8], timeout: Duration) -> Result<(u16, Vec<u8>), String> {
        let mut req = format!(
            "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Type: text/plain\r\n",
            self.method, self.url.path_query, self.url.host_port
        );
        // Set the needed headers; `Content-Type` is allowed to be overridden
        // (Go adds them after the defaults).
        for (name, value) in &self.headers {
            req.push_str(name);
            req.push_str(": ");
            req.push_str(value);
            req.push_str("\r\n");
        }
        if !self.disable_compression {
            req.push_str("Content-Encoding: gzip\r\n");
        }
        if let Some(auth) = &self.url.auth_header {
            req.push_str("Authorization: ");
            req.push_str(auth);
            req.push_str("\r\n");
        }
        req.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));

        let addr = (self.url.host.as_str(), self.url.port)
            .to_socket_addrs()
            .map_err(|err| format!("cannot resolve {:?}: {err}", self.url.host_port))?
            .next()
            .ok_or_else(|| format!("cannot resolve {:?}: no addresses", self.url.host_port))?;
        let tcp = TcpStream::connect_timeout(&addr, timeout).map_err(|err| err.to_string())?;
        tcp.set_read_timeout(Some(timeout))
            .map_err(|err| err.to_string())?;
        tcp.set_write_timeout(Some(timeout))
            .map_err(|err| err.to_string())?;

        let mut resp = Vec::new();
        match &self.tls {
            Some(cfg) => {
                let mut stream = tlsutil::client_connect(cfg, &self.url.host, tcp)?;
                stream
                    .write_all(req.as_bytes())
                    .map_err(|e| e.to_string())?;
                stream.write_all(body).map_err(|e| e.to_string())?;
                // `Connection: close` - read to EOF (a clean TLS close or a
                // reset after the response are both fine).
                let _ = stream.read_to_end(&mut resp);
            }
            None => {
                let mut stream = tcp;
                stream
                    .write_all(req.as_bytes())
                    .map_err(|e| e.to_string())?;
                stream.write_all(body).map_err(|e| e.to_string())?;
                stream.read_to_end(&mut resp).map_err(|e| e.to_string())?;
            }
        }

        parse_response(&resp)
    }
}

/// Parses a buffered HTTP/1.1 response into (status_code, body).
fn parse_response(resp: &[u8]) -> Result<(u16, Vec<u8>), String> {
    let header_end = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "malformed HTTP response: missing header terminator".to_string())?;
    let head = String::from_utf8_lossy(&resp[..header_end]);
    let status_line = head.lines().next().unwrap_or("");
    let mut parts = status_line.split_ascii_whitespace();
    let _version = parts.next();
    let status_code: u16 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("malformed HTTP status line {status_line:?}"))?;
    Ok((status_code, resp[header_end + 4..].to_vec()))
}

/// The parsed push URL (a stand-in for Go's `url.URL` subset used by
/// `push.go`).
struct ParsedUrl {
    scheme: String,
    /// Hostname without port (for connecting/SNI).
    host: String,
    port: u16,
    /// `host[:port]` as written in the URL (for the Host header).
    host_port: String,
    /// Request URI (path plus optional query), always starting with `/`.
    path_query: String,
    /// `Basic ...` header value when the URL carries userinfo (Go's
    /// `http.NewRequest` does the same).
    auth_header: Option<String>,
    /// The URL with the password replaced by `xxxxx` (Go `URL.Redacted`).
    redacted: String,
}

fn parse_push_url(push_url: &str) -> Result<ParsedUrl, String> {
    let unsupported_scheme =
        || format!("unsupported scheme in pushURL={push_url:?}; expecting 'http' or 'https'");
    let Some((scheme, rest)) = push_url.split_once("://") else {
        return Err(unsupported_scheme());
    };
    if scheme != "http" && scheme != "https" {
        return Err(unsupported_scheme());
    }

    // Drop the fragment, split the authority from the path+query.
    let rest = rest.split('#').next().unwrap_or(rest);
    let (authority, path_query) = match rest.find(['/', '?']) {
        Some(n) if rest.as_bytes()[n] == b'?' => (&rest[..n], format!("/{}", &rest[n..])),
        Some(n) => (&rest[..n], rest[n..].to_string()),
        None => (rest, "/".to_string()),
    };

    // Split the userinfo from host[:port].
    let (userinfo, host_port) = match authority.rsplit_once('@') {
        Some((u, hp)) => (Some(u), hp),
        None => (None, authority),
    };
    if host_port.is_empty() {
        return Err(format!("missing host in pushURL={push_url:?}"));
    }

    // Split host and port (IPv6 hosts are bracketed).
    let default_port: u16 = if scheme == "https" { 443 } else { 80 };
    let (host, port) = if let Some(rest) = host_port.strip_prefix('[') {
        let Some((h, p)) = rest.split_once(']') else {
            return Err(format!("cannot parse pushURL={push_url:?}: missing ']'"));
        };
        let port = match p.strip_prefix(':') {
            Some(p) => p
                .parse()
                .map_err(|err| format!("cannot parse pushURL={push_url:?}: invalid port: {err}"))?,
            None => default_port,
        };
        (h.to_string(), port)
    } else {
        match host_port.rsplit_once(':') {
            Some((h, p)) => {
                let port = p.parse().map_err(|err| {
                    format!("cannot parse pushURL={push_url:?}: invalid port: {err}")
                })?;
                (h.to_string(), port)
            }
            None => (host_port.to_string(), default_port),
        }
    };

    // Basic auth from userinfo, like Go's `http.NewRequest`.
    //
    // Go's `http.NewRequest` sets basic auth from `url.Userinfo.Username()` /
    // `.Password()`, which are percent-decoded (Go splits the raw userinfo on
    // the first ':' first, then unescapes each half). `Redacted()` keeps the
    // encoded username, so only the credentials fed to base64 are decoded here.
    let (auth_header, redacted) = match userinfo {
        Some(u) => {
            let (user, pass) = match u.split_once(':') {
                Some((user, pass)) => (user, Some(pass)),
                None => (u, None),
            };
            let user_dec = crate::httpserver::percent_decode(user, false);
            let pass_dec = crate::httpserver::percent_decode(pass.unwrap_or(""), false);
            let creds = format!("{user_dec}:{pass_dec}");
            let auth = format!("Basic {}", base64_std_encode(creds.as_bytes()));
            let redacted = match pass {
                Some(_) => format!(
                    "{scheme}://{user}:xxxxx@{host_port}{}",
                    &push_url[push_url.len() - (rest.len() - authority.len())..]
                ),
                None => push_url.to_string(),
            };
            (Some(auth), redacted)
        }
        None => (None, push_url.to_string()),
    };

    Ok(ParsedUrl {
        scheme: scheme.to_string(),
        host,
        port,
        host_port: host_port.to_string(),
        path_query,
        auth_header,
        redacted,
    })
}

/// Standard base64 encoding (RFC 4648, with padding); replaces Go's
/// `encoding/base64.StdEncoding` for the basic-auth header (same helper as
/// esl-storage's http_client.rs).
fn base64_std_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(n >> 18) as usize & 0x3f] as char);
        out.push(ALPHABET[(n >> 12) as usize & 0x3f] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

/// Canonicalizes a MIME header key like Go's
/// `textproto.CanonicalMIMEHeaderKey` (`foo-bar` -> `Foo-Bar`), so pushed
/// header names match Go's `http.Header.Add`.
fn canonical_mime_header_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper = true;
    for c in s.chars() {
        if upper {
            out.extend(c.to_uppercase());
        } else {
            out.extend(c.to_lowercase());
        }
        upper = c == '-';
    }
    out
}

/// The private set holding the `metrics_push_*` self-metrics
/// (Go `pushMetricsSet`), exported via `write_process_metrics`.
static PUSH_METRICS_SET: LazyLock<Set> = LazyLock::new(Set::new);

fn push_metrics_set() -> &'static Set {
    &PUSH_METRICS_SET
}

/// Go `writePushMetrics`.
pub(super) fn write_push_metrics(w: &mut String) {
    push_metrics_set().write_prometheus(w);
}

/// Adds `extra_labels` to every metric line of `src`, appending to `dst`
/// (Go `addExtraLabels`).
fn add_extra_labels(mut dst: String, src: &str, extra_labels: &str) -> String {
    for line in src.lines() {
        let line = line.trim();
        if line.is_empty() {
            // Skip empty lines.
            continue;
        }
        if line.starts_with('#') {
            // Copy comments as is.
            dst.push_str(line);
            dst.push('\n');
            continue;
        }
        match line.find('{') {
            Some(n) => {
                dst.push_str(&line[..n + 1]);
                dst.push_str(extra_labels);
                dst.push(',');
                dst.push_str(&line[n + 1..]);
            }
            None => {
                let Some(n) = line.rfind(' ') else {
                    panic!(
                        "BUG: missing whitespace between metric name and metric value in Prometheus text exposition line {line:?}"
                    );
                };
                dst.push_str(&line[..n]);
                dst.push('{');
                dst.push_str(extra_labels);
                dst.push('}');
                dst.push_str(&line[n..]);
            }
        }
        dst.push('\n');
    }
    dst
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::super::{MetricsWriter, Set};
    use super::{
        PushCancel, PushOptions, add_extra_labels, init_push_ext_with_options, push_metrics_ext,
    };

    // Port of push_test.go TestAddExtraLabels.
    #[test]
    fn test_add_extra_labels() {
        let f = |s: &str, extra_labels: &str, expected_result: &str| {
            let result = add_extra_labels(String::new(), s, extra_labels);
            assert_eq!(
                result, expected_result,
                "unexpected result; got\n{result}\nwant\n{expected_result}"
            );
        };
        f("", r#"foo="bar""#, "");
        f("a 123", r#"foo="bar""#, "a{foo=\"bar\"} 123\n");
        f(
            r#"a{b="c"} 1.3"#,
            r#"foo="bar""#,
            "a{foo=\"bar\",b=\"c\"} 1.3\n",
        );
        f(
            r#"a{b="c}{"} 1.3"#,
            r#"foo="bar",baz="x""#,
            "a{foo=\"bar\",baz=\"x\",b=\"c}{\"} 1.3\n",
        );
        f(
            "foo 1\nbar{a=\"x\"} 2\n",
            r#"foo="bar""#,
            "foo{foo=\"bar\"} 1\nbar{foo=\"bar\",a=\"x\"} 2\n",
        );
        f(
            "\nfoo 1\n# some counter\n# type foobar counter\n\t  foobar{a=\"b\",c=\"d\"} 4",
            r#"x="y""#,
            "foo{x=\"y\"} 1\n# some counter\n# type foobar counter\nfoobar{x=\"y\",a=\"b\",c=\"d\"} 4\n",
        );
    }

    // Port of push_test.go TestInitPushFailure (via InitPushExtWithOptions,
    // which carries the same validation; the Go test goes through the
    // InitPush sugar wrapper).
    #[test]
    fn test_init_push_failure() {
        let f = |push_url: &str, interval: Duration, extra_labels: &str| {
            let opts = PushOptions {
                extra_labels: extra_labels.to_string(),
                ..Default::default()
            };
            let writer: MetricsWriter = Arc::new(|_w: &mut String| {});
            let res = init_push_ext_with_options(
                Arc::new(PushCancel::new()),
                push_url,
                interval,
                writer,
                &opts,
            );
            assert!(
                res.is_err(),
                "expecting non-nil error for pushURL={push_url:?} interval={interval:?} extraLabels={extra_labels:?}"
            );
        };
        let second = Duration::from_secs(1);

        // Invalid url.
        f("foobar", second, "");
        f("aaa://foobar", second, "");
        f("http:///bar", second, "");

        // Non-positive interval (PORT NOTE: Duration cannot be negative in
        // Rust, so only Go's zero case is portable).
        f("http://foobar", Duration::ZERO, "");

        // Invalid extraLabels.
        f("http://foobar", second, "foo");
        f("http://foobar", second, "foo{bar");
        f("http://foobar", second, "foo=bar");
        f("http://foobar", second, "foo='bar'");
        f("http://foobar", second, r#"foo="bar",baz"#);
        f("http://foobar", second, r#"{foo="bar"}"#);
        f("http://foobar", second, r#"a{foo="bar"}"#);
    }

    /// A one-shot HTTP test server (Go `httptest.NewServer`): records the
    /// headers and body of the first request, replies 200 to every request.
    ///
    /// Returns (url, receiver-of-(headers, body)).
    fn start_test_server() -> (String, mpsc::Receiver<(String, Vec<u8>)>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("cannot bind test server");
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut first = true;
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let mut r = BufReader::new(stream);
                let mut line = String::new();
                if r.read_line(&mut line).is_err() {
                    return;
                }
                let mut headers = Vec::new();
                let mut content_length = 0usize;
                loop {
                    let mut h = String::new();
                    if r.read_line(&mut h).is_err() {
                        return;
                    }
                    let h = h.trim_end().to_string();
                    if h.is_empty() {
                        break;
                    }
                    if let Some((name, value)) = h.split_once(':')
                        && name.eq_ignore_ascii_case("content-length")
                    {
                        content_length = value.trim().parse().unwrap_or(0);
                    }
                    headers.push(h);
                }
                let mut body = vec![0u8; content_length];
                if r.read_exact(&mut body).is_err() {
                    return;
                }
                let mut stream = r.into_inner();
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
                if first {
                    first = false;
                    // Mirror Go's `Header.WriteSubset` exclusions: drop the
                    // headers the transport adds on its own (Host,
                    // Connection, Content-Length here), sort the rest.
                    headers.retain(|h| {
                        let name = h.split(':').next().unwrap_or("").to_ascii_lowercase();
                        name != "host" && name != "connection" && name != "content-length"
                    });
                    headers.sort();
                    let mut hs = String::new();
                    for h in &headers {
                        hs.push_str(h);
                        hs.push_str("\r\n");
                    }
                    if tx.send((hs, body)).is_err() {
                        return;
                    }
                }
            }
        });
        (url, rx)
    }

    fn gunzip(data: &[u8]) -> Vec<u8> {
        let mut zr = flate2::read::GzDecoder::new(data);
        let mut out = Vec::new();
        zr.read_to_end(&mut out)
            .expect("cannot read data from gzip reader");
        out
    }

    fn new_test_set() -> Arc<Set> {
        let s = Arc::new(Set::new());
        let c = s.new_counter("foo");
        c.set(1234);
        let _ = s.new_gauge("bar", Some(Box::new(|| 42.12)));
        s
    }

    // Port of push_test.go TestInitPushWithOptions (via a std-TCP test
    // server standing in for httptest.NewServer; the metrics writer is a
    // closure over the Set, standing in for `s.InitPushWithOptions`).
    #[test]
    fn test_init_push_with_options() {
        // The expected payloads depend on the global metadata switch staying
        // off; hold the registry lock like the other metadata-sensitive tests.
        let _guard = crate::metrics::testutil::global_registry_lock();
        let f = |opts: PushOptions, expected_headers: &str, expected_data: &str| {
            let (url, rx) = start_test_server();
            let s = new_test_set();
            let s2 = Arc::clone(&s);
            let writer: MetricsWriter = Arc::new(move |w: &mut String| s2.write_prometheus(w));
            let cancel = Arc::new(PushCancel::new());
            let disable_compression = opts.disable_compression;
            let handle = init_push_ext_with_options(
                Arc::clone(&cancel),
                &url,
                Duration::from_millis(1),
                writer,
                &opts,
            )
            .expect("unexpected error");
            let (req_headers, mut req_data) =
                rx.recv_timeout(Duration::from_secs(5)).expect("timeout!");
            // Stop the periodic pusher.
            cancel.cancel();
            handle.join().unwrap();
            if !disable_compression {
                req_data = gunzip(&req_data);
            }
            assert_eq!(
                req_headers, expected_headers,
                "unexpected request headers; got\n{req_headers}\nwant\n{expected_headers}"
            );
            let req_data = String::from_utf8(req_data).unwrap();
            assert_eq!(
                req_data, expected_data,
                "unexpected data; got\n{req_data}\nwant\n{expected_data}"
            );
        };

        // Default PushOptions.
        f(
            PushOptions::default(),
            "Content-Encoding: gzip\r\nContent-Type: text/plain\r\n",
            "bar 42.12\nfoo 1234\n",
        );

        // Disable compression on the pushed request body.
        f(
            PushOptions {
                disable_compression: true,
                ..Default::default()
            },
            "Content-Type: text/plain\r\n",
            "bar 42.12\nfoo 1234\n",
        );

        // Add extra labels.
        f(
            PushOptions {
                extra_labels: r#"label1="value1",label2="value2""#.to_string(),
                ..Default::default()
            },
            "Content-Encoding: gzip\r\nContent-Type: text/plain\r\n",
            "bar{label1=\"value1\",label2=\"value2\"} 42.12\nfoo{label1=\"value1\",label2=\"value2\"} 1234\n",
        );

        // Add extra headers.
        f(
            PushOptions {
                headers: vec!["Foo: Bar".to_string(), "baz:aaaa-bbb".to_string()],
                ..Default::default()
            },
            "Baz: aaaa-bbb\r\nContent-Encoding: gzip\r\nContent-Type: text/plain\r\nFoo: Bar\r\n",
            "bar 42.12\nfoo 1234\n",
        );
    }

    // Port of push_test.go TestPushMetrics (via push_metrics_ext, standing in
    // for `s.PushMetrics`).
    #[test]
    fn test_push_metrics() {
        // See test_init_push_with_options about the registry lock.
        let _guard = crate::metrics::testutil::global_registry_lock();
        let f = |opts: PushOptions, expected_headers: &str, expected_data: &str| {
            let (url, rx) = start_test_server();
            let s = new_test_set();
            let s2 = Arc::clone(&s);
            let writer: MetricsWriter = Arc::new(move |w: &mut String| s2.write_prometheus(w));
            let disable_compression = opts.disable_compression;
            push_metrics_ext(&url, &writer, &opts, Duration::from_secs(5))
                .expect("unexpected error");
            let (req_headers, mut req_data) =
                rx.recv_timeout(Duration::from_secs(5)).expect("timeout!");
            if !disable_compression {
                req_data = gunzip(&req_data);
            }
            assert_eq!(
                req_headers, expected_headers,
                "unexpected request headers; got\n{req_headers}\nwant\n{expected_headers}"
            );
            let req_data = String::from_utf8(req_data).unwrap();
            assert_eq!(
                req_data, expected_data,
                "unexpected data; got\n{req_data}\nwant\n{expected_data}"
            );
        };

        // Default PushOptions.
        f(
            PushOptions::default(),
            "Content-Encoding: gzip\r\nContent-Type: text/plain\r\n",
            "bar 42.12\nfoo 1234\n",
        );

        // Disable compression on the pushed request body.
        f(
            PushOptions {
                disable_compression: true,
                ..Default::default()
            },
            "Content-Type: text/plain\r\n",
            "bar 42.12\nfoo 1234\n",
        );

        // Add extra labels.
        f(
            PushOptions {
                extra_labels: r#"label1="value1",label2="value2""#.to_string(),
                ..Default::default()
            },
            "Content-Encoding: gzip\r\nContent-Type: text/plain\r\n",
            "bar{label1=\"value1\",label2=\"value2\"} 42.12\nfoo{label1=\"value1\",label2=\"value2\"} 1234\n",
        );

        // Add extra headers.
        f(
            PushOptions {
                headers: vec!["Foo: Bar".to_string(), "baz:aaaa-bbb".to_string()],
                ..Default::default()
            },
            "Baz: aaaa-bbb\r\nContent-Encoding: gzip\r\nContent-Type: text/plain\r\nFoo: Bar\r\n",
            "bar 42.12\nfoo 1234\n",
        );
    }

    // Rust-only sanity tests for the URL parser standing in for Go's
    // `net/url` + `http.NewRequest` behavior.
    #[test]
    fn test_parse_push_url() {
        let u = super::parse_push_url("http://foo:8428/api/v1/import/prometheus?x=1").unwrap();
        assert_eq!(u.scheme, "http");
        assert_eq!(u.host, "foo");
        assert_eq!(u.port, 8428);
        assert_eq!(u.host_port, "foo:8428");
        assert_eq!(u.path_query, "/api/v1/import/prometheus?x=1");
        assert!(u.auth_header.is_none());
        assert_eq!(u.redacted, "http://foo:8428/api/v1/import/prometheus?x=1");

        let u = super::parse_push_url("https://user:pass@bar/path").unwrap();
        assert_eq!(u.scheme, "https");
        assert_eq!(u.host, "bar");
        assert_eq!(u.port, 443);
        assert_eq!(u.path_query, "/path");
        // base64("user:pass") == "dXNlcjpwYXNz"
        assert_eq!(u.auth_header.as_deref(), Some("Basic dXNlcjpwYXNz"));
        assert_eq!(u.redacted, "https://user:xxxxx@bar/path");

        // Percent-encoded userinfo is decoded before base64 (Go's
        // url.Userinfo.Username()/Password() return the unescaped values),
        // while Redacted() keeps the encoded username.
        let u = super::parse_push_url("http://us%40er:p%40ss@bar/path").unwrap();
        assert_eq!(
            u.auth_header.as_deref(),
            Some(format!("Basic {}", super::base64_std_encode(b"us@er:p@ss")).as_str())
        );
        assert_eq!(u.redacted, "http://us%40er:xxxxx@bar/path");

        assert!(super::parse_push_url("foobar").is_err());
        assert!(super::parse_push_url("aaa://foobar").is_err());
        assert!(super::parse_push_url("http:///bar").is_err());
    }
}

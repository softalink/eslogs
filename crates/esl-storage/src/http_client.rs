//! Minimal HTTP/1.1 client shared by [`crate::netinsert`] and
//! [`crate::netselect`].
//!
//! PORT NOTE: this module does not correspond to a Go source file. Go's
//! `app/eslstorage/netinsert` and `netselect` use `net/http` + `lib/promauth` +
//! `lib/httputil.NewTransport`; the Rust workspace has no HTTP client
//! (esl-common's `httpserver` is server-only), so this module provides a small
//! std-TCP client in the style of `bench/loadgen` (raw HTTP/1.1 over
//! `std::net::TcpStream`, MSVC-portable). Divergences from Go's transport,
//! shared by both netinsert and netselect:
//!   * one TCP connection per request — no keep-alive pooling;
//!   * responses are buffered in memory instead of being streamed;
//!   * https (`-storageNode.tls`) is spoken via `esl_common::tlsutil` (rustls
//!     over the same blocking TCP stream); the TLS config is built eagerly in
//!     `Options::new_config`, so broken cert/CA files fail at startup instead
//!     of on the first https request like Go's lazy `getTLSConfigCached`;
//!   * no context cancellation — an in-flight request runs to completion
//!     (Go cancels via `contextutil.NewStopChanContext`).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use esl_common::tlsutil::{self, TLSConfig, TlsClientConfig};

use crate::proxy::ProxyConfig;

/// Connect/read/write timeout applied to every request.
///
/// PORT NOTE: Go's transport has no total request timeout; a fixed cap keeps
/// the blocking std-TCP client from hanging forever on a dead peer.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// promauth stand-in (subset used by newAuthConfigForStorageNode)
// ---------------------------------------------------------------------------

/// Basic-auth part of the storage-node auth options
/// (Go `promauth.BasicAuthConfig`).
#[derive(Debug, Default, Clone)]
pub struct BasicAuthConfig {
    pub username: String,
    pub username_file: String,
    pub password: String,
    pub password_file: String,
}

/// Storage-node auth options (Go `promauth.Options`, subset: BasicAuth,
/// BearerToken/BearerTokenFile and TLSConfig).
#[derive(Debug, Default, Clone)]
pub struct Options {
    pub basic_auth: Option<BasicAuthConfig>,
    pub bearer_token: String,
    pub bearer_token_file: String,
    /// TLS options; `Some` when the connection must use https.
    ///
    /// PORT NOTE: Go always carries a `promauth.TLSConfig` and decides
    /// http-vs-https separately via the `-*.tls` flag; the port folds the
    /// toggle into `Option` (the config is only ever used for https).
    pub tls_config: Option<TLSConfig>,
}

impl Options {
    /// Builds an [`AuthConfig`] from the options (Go `promauth.Options.NewConfig`).
    pub fn new_config(&self) -> Result<AuthConfig, String> {
        let has_basic = self.basic_auth.is_some();
        let has_bearer = !self.bearer_token.is_empty() || !self.bearer_token_file.is_empty();
        if has_basic && has_bearer {
            return Err(
                "both basic auth and bearer token are set; only one can be set".to_string(),
            );
        }
        if !self.bearer_token.is_empty() && !self.bearer_token_file.is_empty() {
            return Err(
                "both bearer_token and bearer_token_file are set; only one can be set".to_string(),
            );
        }
        if let Some(ba) = &self.basic_auth {
            if !ba.username.is_empty() && !ba.username_file.is_empty() {
                return Err(
                    "both username and username_file are set; only one can be set".to_string(),
                );
            }
            if !ba.password.is_empty() && !ba.password_file.is_empty() {
                return Err(
                    "both password and password_file are set; only one can be set".to_string(),
                );
            }
        }
        let tls = match &self.tls_config {
            Some(tc) => Some(
                tlsutil::new_tls_client_config(tc)
                    .map_err(|err| format!("cannot initialize tls: {err}"))?,
            ),
            None => None,
        };
        Ok(AuthConfig {
            basic_auth: self.basic_auth.clone(),
            bearer_token: self.bearer_token.clone(),
            bearer_token_file: self.bearer_token_file.clone(),
            tls,
        })
    }
}

/// Auth config used for setting the `Authorization` request header
/// (Go `promauth.Config`, header subset).
///
/// PORT NOTE: Go re-reads `*File` sources once per second via a background
/// refresher; the port re-reads them on every `get_auth_header` call (requests
/// to storage nodes are infrequent enough for this to be equivalent).
#[derive(Debug, Default)]
pub struct AuthConfig {
    basic_auth: Option<BasicAuthConfig>,
    bearer_token: String,
    bearer_token_file: String,
    tls: Option<TlsClientConfig>,
}

impl AuthConfig {
    /// Returns the TLS client config when the node must be reached via https
    /// (Go decides this via the separate `isTLS` argument; see [`Options`]).
    pub fn tls(&self) -> Option<&TlsClientConfig> {
        self.tls.as_ref()
    }

    /// Returns the `Authorization` header value, or an empty string when no
    /// auth is configured (Go `promauth.Config.SetHeaders` subset).
    pub fn get_auth_header(&self) -> Result<String, String> {
        if !self.bearer_token.is_empty() {
            return Ok(format!("Bearer {}", self.bearer_token));
        }
        if !self.bearer_token_file.is_empty() {
            let token = read_trimmed_file(&self.bearer_token_file)?;
            return Ok(format!("Bearer {token}"));
        }
        if let Some(ba) = &self.basic_auth {
            let username = if !ba.username_file.is_empty() {
                read_trimmed_file(&ba.username_file)?
            } else {
                ba.username.clone()
            };
            let password = if !ba.password_file.is_empty() {
                read_trimmed_file(&ba.password_file)?
            } else {
                ba.password.clone()
            };
            let creds = format!("{username}:{password}");
            return Ok(format!("Basic {}", base64_std_encode(creds.as_bytes())));
        }
        Ok(String::new())
    }
}

fn read_trimmed_file(path: &str) -> Result<String, String> {
    match std::fs::read_to_string(path) {
        Ok(s) => Ok(s.trim_end_matches(['\r', '\n']).to_string()),
        Err(err) => Err(format!("cannot read auth secret from {path:?}: {err}")),
    }
}

/// Standard base64 encoding (RFC 4648, with padding); replaces Go's
/// `encoding/base64.StdEncoding` for the basic-auth header.
pub fn base64_std_encode(data: &[u8]) -> String {
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

// ---------------------------------------------------------------------------
// HTTP/1.1 request/response
// ---------------------------------------------------------------------------

/// A fully buffered HTTP response.
#[derive(Debug)]
pub struct HttpResponse {
    pub status_code: u16,
    /// Response headers with lower-cased names, in arrival order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Returns the first header value for `name` (case-insensitive), or "".
    pub fn header(&self, name: &str) -> &str {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    }
}

/// Sends a single HTTP/1.1 request to `addr` (a `host:port` TCP address) and
/// returns the buffered response. `tls` upgrades the connection to https.
///
/// `headers` are emitted verbatim; `Host`, `Content-Length` and
/// `Connection: close` are added automatically.
pub fn do_request(
    addr: &str,
    tls: Option<&TlsClientConfig>,
    method: &str,
    path_and_query: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
) -> Result<HttpResponse, String> {
    do_request_with_timeout(
        addr,
        tls,
        method,
        path_and_query,
        headers,
        body,
        REQUEST_TIMEOUT,
        None,
    )
}

/// [`do_request`] with a caller-supplied connect/read/write timeout
/// (esl-agent's remotewrite client maps `-remoteWrite.sendTimeout` here).
///
/// When `proxy` is `Some`, the TCP connection to `addr` is tunnelled through
/// the proxy (esl-agent's `-remoteWrite.proxyURL`); the target's own TLS
/// upgrade and the HTTP request/response are unchanged. When `proxy` is `None`
/// the behaviour is identical to a direct connect.
#[allow(clippy::too_many_arguments)]
pub fn do_request_with_timeout(
    addr: &str,
    tls: Option<&TlsClientConfig>,
    method: &str,
    path_and_query: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
    timeout: Duration,
    proxy: Option<&ProxyConfig>,
) -> Result<HttpResponse, String> {
    use std::net::ToSocketAddrs;

    let mut stream = match proxy {
        None => {
            let sock_addr = addr
                .to_socket_addrs()
                .map_err(|err| format!("cannot resolve {addr:?}: {err}"))?
                .next()
                .ok_or_else(|| format!("cannot resolve {addr:?}: no addresses"))?;
            TcpStream::connect_timeout(&sock_addr, timeout)
                .map_err(|err| format!("cannot connect to {addr:?}: {err}"))?
        }
        Some(proxy) => {
            let (host, port) = split_host_port(addr)?;
            crate::proxy::connect_via_proxy(proxy, host, port, timeout)?
        }
    };
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let _ = stream.set_nodelay(true);

    // When the user overrode the TLS server name, use it as the `Host` header
    // too (Go: `req.Host = ac.tlsServerName` in promauth.Config.SetHeaders).
    let host_header = match tls {
        Some(cfg) if !cfg.server_name.is_empty() => &cfg.server_name,
        _ => addr,
    };
    let mut req = Vec::with_capacity(256 + body.map_or(0, <[u8]>::len));
    req.extend_from_slice(format!("{method} {path_and_query} HTTP/1.1\r\n").as_bytes());
    req.extend_from_slice(format!("Host: {host_header}\r\n").as_bytes());
    req.extend_from_slice(b"Connection: close\r\n");
    for (name, value) in headers {
        req.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    req.extend_from_slice(
        format!("Content-Length: {}\r\n", body.map_or(0, <[u8]>::len)).as_bytes(),
    );
    req.extend_from_slice(b"\r\n");
    if let Some(body) = body {
        req.extend_from_slice(body);
    }

    match tls {
        None => {
            stream
                .write_all(&req)
                .map_err(|err| format!("cannot send request to {addr:?}: {err}"))?;
            read_response(&mut stream, addr)
        }
        Some(cfg) => {
            let host = host_without_port(addr);
            let mut tls_stream = tlsutil::client_connect(cfg, host, stream)?;
            tls_stream
                .write_all(&req)
                .map_err(|err| format!("cannot send request to {addr:?}: {err}"))?;
            tls_stream
                .flush()
                .map_err(|err| format!("cannot send request to {addr:?}: {err}"))?;
            read_response(&mut TolerantEofReader(&mut tls_stream), addr)
        }
    }
}

/// Strips the `:port` suffix from a `host:port` address (IPv6 literals keep
/// Go's `[host]:port` bracket form); the result feeds TLS SNI/verification.
fn host_without_port(addr: &str) -> &str {
    if let Some(rest) = addr.strip_prefix('[')
        && let Some(end) = rest.find(']')
    {
        return &rest[..end];
    }
    match addr.rsplit_once(':') {
        // An unbracketed IPv6 literal contains more colons; treat it as a
        // bare host without port.
        Some((host, _)) if !host.contains(':') => host,
        _ => addr,
    }
}

/// Splits a `host:port` address into its host (bracket-stripped for IPv6) and
/// numeric port, for handing the target to the proxy tunnel. The port is
/// required here — every caller builds `addr` with an explicit port
/// (`RemoteUrl::addr` / storage-node addresses always include one).
fn split_host_port(addr: &str) -> Result<(&str, u16), String> {
    let host = host_without_port(addr);
    let port_str = if let Some(rest) = addr.strip_prefix('[') {
        // Bracketed IPv6: the port (if any) follows `]`.
        rest.rsplit_once("]:").map(|(_, p)| p)
    } else {
        match addr.rsplit_once(':') {
            // Unbracketed IPv6 literal without port — no port present.
            Some((h, _)) if h.contains(':') => None,
            Some((_, p)) => Some(p),
            None => None,
        }
    };
    let port_str =
        port_str.ok_or_else(|| format!("cannot determine target port from {addr:?} for proxy"))?;
    let port = port_str
        .parse::<u16>()
        .map_err(|_| format!("invalid target port in {addr:?}"))?;
    Ok((host, port))
}

/// Maps rustls' "peer closed connection without sending TLS close_notify"
/// `UnexpectedEof` to a clean EOF.
///
/// PORT NOTE: the response framing (`Content-Length`/chunked) is validated by
/// `read_response` afterwards, so a truncating attacker is still detected —
/// same trust model as Go's net/http, which tolerates missing close_notify
/// once the declared body length has been read.
struct TolerantEofReader<'a, R: Read>(&'a mut R);

impl<R: Read> Read for TolerantEofReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.0.read(buf) {
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(0),
            other => other,
        }
    }
}

/// Reads and parses a buffered HTTP/1.1 response from `stream`.
fn read_response<R: Read>(stream: &mut R, addr: &str) -> Result<HttpResponse, String> {
    let mut raw = Vec::with_capacity(4096);
    stream
        .read_to_end(&mut raw)
        .map_err(|err| format!("cannot read response from {addr:?}: {err}"))?;

    let header_end = find_subslice(&raw, b"\r\n\r\n")
        .ok_or_else(|| format!("cannot find the end of response headers from {addr:?}"))?;
    let head = std::str::from_utf8(&raw[..header_end])
        .map_err(|err| format!("non-utf8 response headers from {addr:?}: {err}"))?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            format!("cannot parse response status line {status_line:?} from {addr:?}")
        })?;

    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    let mut resp_headers: Vec<(String, String)> = Vec::new();
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        if name == "content-length" {
            content_length = value.parse().ok();
        } else if name == "transfer-encoding" && value.eq_ignore_ascii_case("chunked") {
            chunked = true;
        }
        resp_headers.push((name, value.to_string()));
    }

    let raw_body = &raw[header_end + 4..];
    let body = if chunked {
        decode_chunked(raw_body)
            .map_err(|err| format!("cannot decode chunked response from {addr:?}: {err}"))?
    } else if let Some(n) = content_length {
        if raw_body.len() < n {
            return Err(format!(
                "truncated response from {addr:?}: got {} out of {n} body bytes",
                raw_body.len()
            ));
        }
        raw_body[..n].to_vec()
    } else {
        // `Connection: close` response without explicit framing: the body runs
        // until EOF.
        raw_body.to_vec()
    };

    Ok(HttpResponse {
        status_code,
        headers: resp_headers,
        body,
    })
}

/// Decodes a `Transfer-Encoding: chunked` body.
fn decode_chunked(mut src: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(src.len());
    loop {
        let line_end = find_subslice(src, b"\r\n").ok_or("missing chunk-size line")?;
        let size_str = std::str::from_utf8(&src[..line_end]).map_err(|_| "non-utf8 chunk size")?;
        let size_str = size_str.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_str, 16).map_err(|_| "cannot parse chunk size")?;
        src = &src[line_end + 2..];
        if size == 0 {
            return Ok(out);
        }
        if src.len() < size + 2 {
            return Err("truncated chunk".to_string());
        }
        out.extend_from_slice(&src[..size]);
        src = &src[size + 2..];
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---------------------------------------------------------------------------
// multipart/form-data request bodies (netselect)
// ---------------------------------------------------------------------------

/// Builds an in-memory `multipart/form-data` request body from `args` and
/// returns `(body, content_type)` (Go `newMultipartRequestBody`, which uses
/// `mime/multipart.Writer`).
pub fn new_multipart_request_body(args: &[(String, String)]) -> (Vec<u8>, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static BOUNDARY_COUNTER: AtomicU64 = AtomicU64::new(0);

    let n = BOUNDARY_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let boundary = format!("eslstorageboundary{nanos:016x}{n:08x}");

    let mut body = Vec::with_capacity(256);
    for (k, v) in args {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        let escaped = k.replace('\\', "\\\\").replace('"', "\\\"");
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{escaped}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(v.as_bytes());
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    let content_type = format!("multipart/form-data; boundary={boundary}");
    (body, content_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: no upstream test file corresponds to this module (see the
    // module docs); the tests below cover the port-specific helpers.

    #[test]
    fn test_base64_std_encode() {
        assert_eq!(base64_std_encode(b""), "");
        assert_eq!(base64_std_encode(b"f"), "Zg==");
        assert_eq!(base64_std_encode(b"fo"), "Zm8=");
        assert_eq!(base64_std_encode(b"foo"), "Zm9v");
        assert_eq!(base64_std_encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64_std_encode(b"user:pass"), "dXNlcjpwYXNz");
    }

    #[test]
    fn test_decode_chunked() {
        let body = b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n";
        assert_eq!(decode_chunked(body).unwrap(), b"Wikipedia");
        assert!(decode_chunked(b"zz\r\n").is_err());
    }

    #[test]
    fn test_new_multipart_request_body() {
        let args = vec![
            ("query".to_string(), "*".to_string()),
            ("na\"me".to_string(), "value".to_string()),
        ];
        let (body, content_type) = new_multipart_request_body(&args);
        let body = String::from_utf8(body).unwrap();
        assert!(content_type.starts_with("multipart/form-data; boundary="));
        let boundary = content_type.split('=').next_back().unwrap();
        assert!(body.contains(&format!("--{boundary}\r\n")));
        assert!(body.ends_with(&format!("--{boundary}--\r\n")));
        assert!(body.contains("Content-Disposition: form-data; name=\"query\"\r\n\r\n*\r\n"));
        assert!(body.contains("name=\"na\\\"me\""));
    }

    #[test]
    fn test_auth_config_headers() {
        let opts = Options::default();
        let ac = opts.new_config().unwrap();
        assert_eq!(ac.get_auth_header().unwrap(), "");

        let opts = Options {
            basic_auth: Some(BasicAuthConfig {
                username: "user".to_string(),
                password: "pass".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let ac = opts.new_config().unwrap();
        assert_eq!(ac.get_auth_header().unwrap(), "Basic dXNlcjpwYXNz");

        let opts = Options {
            bearer_token: "abc".to_string(),
            ..Default::default()
        };
        let ac = opts.new_config().unwrap();
        assert_eq!(ac.get_auth_header().unwrap(), "Bearer abc");

        // basic auth + bearer token cannot be set simultaneously
        let opts = Options {
            basic_auth: Some(BasicAuthConfig::default()),
            bearer_token: "abc".to_string(),
            ..Default::default()
        };
        assert!(opts.new_config().is_err());

        // TLS config is built (and validated) eagerly by new_config.
        let opts = Options {
            tls_config: Some(TLSConfig {
                insecure_skip_verify: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(opts.new_config().unwrap().tls().is_some());
        let opts = Options::default();
        assert!(opts.new_config().unwrap().tls().is_none());

        // A broken TLS config (cert without key) fails eagerly, mirroring
        // Go's fatal error path for a broken auth config.
        let opts = Options {
            tls_config: Some(TLSConfig {
                cert_file: "/nonexistent/cert.pem".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(opts.new_config().is_err());
    }

    #[test]
    fn test_host_without_port() {
        assert_eq!(host_without_port("example.com:9428"), "example.com");
        assert_eq!(host_without_port("127.0.0.1:9428"), "127.0.0.1");
        assert_eq!(host_without_port("[::1]:9428"), "::1");
        assert_eq!(host_without_port("example.com"), "example.com");
        assert_eq!(host_without_port("::1"), "::1");
    }

    /// Spawns a one-shot https server that reads one request and answers with
    /// a fixed 200 response, returning the captured request head+body.
    fn spawn_https_server(
        close_notify: bool,
    ) -> (String, std::path::PathBuf, std::thread::JoinHandle<Vec<u8>>) {
        let ck = rcgen::generate_simple_self_signed(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .unwrap();
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "esl-http-client-tls-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("cert.pem");
        std::fs::write(&cert_path, ck.cert.pem()).unwrap();
        let key_path = dir.join("key.pem");
        std::fs::write(&key_path, ck.key_pair.serialize_pem()).unwrap();
        let server_cfg = esl_common::tlsutil::get_server_tls_config(
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap(),
            "",
            &[],
        )
        .unwrap();

        let ln = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = ln.local_addr().unwrap().to_string();
        let handle = std::thread::spawn(move || {
            let (tcp, _) = ln.accept().unwrap();
            // A client aborting the handshake (untrusted-cert test) is fine.
            let Ok(mut stream) = esl_common::tlsutil::server_accept(&server_cfg, tcp) else {
                return Vec::new();
            };
            let mut req = vec![0u8; 4096];
            let mut n = 0;
            while !req[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                let m = stream.read(&mut req[n..]).unwrap();
                assert!(m > 0, "request truncated");
                n += m;
            }
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nresponse",
                )
                .unwrap();
            if close_notify {
                stream.conn.send_close_notify();
            }
            let _ = stream.flush();
            req.truncate(n);
            req
        });
        (addr, cert_path, handle)
    }

    #[test]
    fn test_https_round_trip() {
        // Once with a graceful TLS shutdown, once with a bare TCP close (the
        // TolerantEofReader must treat the missing close_notify as EOF since
        // the Content-Length framing is already satisfied).
        for close_notify in [true, false] {
            let (addr, cert_path, handle) = spawn_https_server(close_notify);
            let opts = Options {
                tls_config: Some(TLSConfig {
                    ca_file: cert_path.to_str().unwrap().to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let ac = opts.new_config().unwrap();
            let resp = do_request(&addr, ac.tls(), "GET", "/ping", &[], None).unwrap();
            assert_eq!(resp.status_code, 200);
            assert_eq!(resp.body, b"response");
            let req = handle.join().unwrap();
            let head = String::from_utf8_lossy(&req);
            assert!(head.starts_with("GET /ping HTTP/1.1\r\n"), "{head}");
            assert!(head.contains(&format!("Host: {addr}\r\n")), "{head}");
        }
    }

    #[test]
    fn test_https_server_name_override_sets_host_header() {
        let (addr, cert_path, handle) = spawn_https_server(true);
        let opts = Options {
            tls_config: Some(TLSConfig {
                ca_file: cert_path.to_str().unwrap().to_string(),
                server_name: "localhost".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let ac = opts.new_config().unwrap();
        let resp = do_request(&addr, ac.tls(), "GET", "/", &[], None).unwrap();
        assert_eq!(resp.status_code, 200);
        let req = handle.join().unwrap();
        let head = String::from_utf8_lossy(&req);
        // Go: req.Host = ac.tlsServerName when the server name is overridden.
        assert!(head.contains("Host: localhost\r\n"), "{head}");
    }

    #[test]
    fn test_https_rejects_untrusted_cert() {
        let (addr, _cert_path, handle) = spawn_https_server(true);
        let opts = Options {
            tls_config: Some(TLSConfig::default()),
            ..Default::default()
        };
        let ac = opts.new_config().unwrap();
        let err = do_request(&addr, ac.tls(), "GET", "/", &[], None).unwrap_err();
        assert!(err.contains("handshake"), "{err}");
        let _ = handle.join(); // server thread panics on the aborted handshake
    }
}

//! Minimal HTTP/1.1 client shared by [`crate::netinsert`] and
//! [`crate::netselect`].
//!
//! PORT NOTE: this module does not correspond to a Go source file. Go's
//! `app/eslstorage/netinsert` and `netselect` use `net/http` + `lib/promauth` +
//! `lib/httputil.NewTransport`; the Rust workspace has no HTTP client
//! (esl-common's `httpserver` is server-only), so this module provides a small
//! std-TCP client in the style of `bench/loadgen` (raw HTTP/1.1 over
//! `std::net::TcpStream`, MSVC-portable, dependency-free). Divergences from
//! Go's transport, shared by both netinsert and netselect:
//!   * one TCP connection per request — no keep-alive pooling;
//!   * responses are buffered in memory instead of being streamed;
//!   * TLS (`-storageNode.tls`) is not supported: `Options::new_config` fails
//!     when TLS is requested, mirroring Go's fatal error path for a broken
//!     auth config;
//!   * no context cancellation — an in-flight request runs to completion
//!     (Go cancels via `contextutil.NewStopChanContext`).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

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
/// BearerToken/BearerTokenFile and the TLS toggle used for validation).
#[derive(Debug, Default, Clone)]
pub struct Options {
    pub basic_auth: Option<BasicAuthConfig>,
    pub bearer_token: String,
    pub bearer_token_file: String,
    /// Whether any of the `-storageNode.tls*` flags requested TLS for the node.
    pub needs_tls: bool,
}

impl Options {
    /// Builds an [`AuthConfig`] from the options (Go `promauth.Options.NewConfig`).
    pub fn new_config(&self) -> Result<AuthConfig, String> {
        if self.needs_tls {
            // PORT NOTE: no TLS stack in the workspace; see the module docs.
            return Err(
                "TLS connections to -storageNode are not supported by this port".to_string(),
            );
        }
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
        Ok(AuthConfig {
            basic_auth: self.basic_auth.clone(),
            bearer_token: self.bearer_token.clone(),
            bearer_token_file: self.bearer_token_file.clone(),
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
}

impl AuthConfig {
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

// ---------------------------------------------------------------------------
// HTTP/1.1 request/response
// ---------------------------------------------------------------------------

/// A fully buffered HTTP response.
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
/// returns the buffered response.
///
/// `headers` are emitted verbatim; `Host`, `Content-Length` and
/// `Connection: close` are added automatically.
pub fn do_request(
    addr: &str,
    method: &str,
    path_and_query: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
) -> Result<HttpResponse, String> {
    do_request_with_timeout(addr, method, path_and_query, headers, body, REQUEST_TIMEOUT)
}

/// [`do_request`] with a caller-supplied connect/read/write timeout
/// (esl-agent's remotewrite client maps `-remoteWrite.sendTimeout` here).
pub fn do_request_with_timeout(
    addr: &str,
    method: &str,
    path_and_query: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
    timeout: Duration,
) -> Result<HttpResponse, String> {
    use std::net::ToSocketAddrs;

    let sock_addr = addr
        .to_socket_addrs()
        .map_err(|err| format!("cannot resolve {addr:?}: {err}"))?
        .next()
        .ok_or_else(|| format!("cannot resolve {addr:?}: no addresses"))?;
    let mut stream = TcpStream::connect_timeout(&sock_addr, timeout)
        .map_err(|err| format!("cannot connect to {addr:?}: {err}"))?;
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let _ = stream.set_nodelay(true);

    let mut req = Vec::with_capacity(256 + body.map_or(0, <[u8]>::len));
    req.extend_from_slice(format!("{method} {path_and_query} HTTP/1.1\r\n").as_bytes());
    req.extend_from_slice(format!("Host: {addr}\r\n").as_bytes());
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

    stream
        .write_all(&req)
        .map_err(|err| format!("cannot send request to {addr:?}: {err}"))?;

    read_response(&mut stream, addr)
}

/// Reads and parses a buffered HTTP/1.1 response from `stream`.
fn read_response(stream: &mut TcpStream, addr: &str) -> Result<HttpResponse, String> {
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

        // TLS is not supported by the port
        let opts = Options {
            needs_tls: true,
            ..Default::default()
        };
        assert!(opts.new_config().is_err());
    }
}

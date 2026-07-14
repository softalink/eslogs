//! Proxy support for the std-TCP [`crate::http_client`], used by esl-agent's
//! remote-write client for `-remoteWrite.proxyURL`.
//!
//! PORT NOTE: this module does not correspond to a Go source file. Go's
//! `app/vlagent/remotewrite/client.go` wires a proxy into `net/http` via
//! `tr.Proxy = http.ProxyURL(pu)`, which delegates the handshake to the stdlib
//! (`http`/`https` via HTTP `CONNECT`, `socks5` via `golang.org/x/net/proxy`).
//! The port has no `net/http`, so it speaks the proxy protocols directly over
//! the same blocking `std::net::TcpStream` the house client already uses:
//!   * `socks5://` — RFC 1928 (SOCKS5) with optional RFC 1929 username/password
//!     authentication;
//!   * `http://`   — HTTP `CONNECT` tunneling (RFC 9110 §9.3.6) over plain TCP.
//!
//! Residual divergence from Go: `https://` proxies (a TLS connection to the
//! proxy itself, with the target's own TLS then layered on top) are not
//! supported. The house connect path returns a concrete `TcpStream` and the
//! target TLS upgrade ([`esl_common::tlsutil::client_connect`]) also consumes a
//! concrete `TcpStream`, so TLS-over-TLS is not representable without
//! generalising the whole request path to boxed stream objects. `https://`
//! proxies are the rarest case; [`ProxyConfig::parse`] accepts them but
//! [`connect_via_proxy`] returns a clear error. `http://` and `socks5://`
//! proxies (including to `https` targets — the common case) are fully
//! supported, since the tunnelled leg is plain TCP that the target TLS upgrade
//! wraps unchanged.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::http_client::base64_std_encode;

/// The proxy scheme (Go requires the URL to start with `http://`, `https://`
/// or `socks5://`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyScheme {
    Http,
    Https,
    Socks5,
}

/// A parsed `-remoteWrite.proxyURL` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyConfig {
    pub scheme: ProxyScheme,
    /// Proxy host without port (IPv6 literals without brackets).
    pub host: String,
    pub port: u16,
    /// Userinfo from the proxy URL; secret (the flag is registered secret).
    pub username: Option<String>,
    pub password: Option<String>,
}

impl ProxyConfig {
    /// Parses a proxy URL of the form
    /// `scheme://[user[:pass]@]host[:port]`. The scheme must be one of `http`,
    /// `https` or `socks5` (Go: "proxy URL must start with http://, https:// or
    /// socks5://").
    pub fn parse(proxy_url: &str) -> Result<ProxyConfig, String> {
        let (scheme_str, rest) = proxy_url.split_once("://").ok_or_else(|| {
            format!("proxy URL {proxy_url:?} must start with http://, https:// or socks5://")
        })?;
        let scheme = match scheme_str {
            "http" => ProxyScheme::Http,
            "https" => ProxyScheme::Https,
            "socks5" => ProxyScheme::Socks5,
            other => {
                return Err(format!(
                    "unsupported proxy scheme {other:?} in {proxy_url:?}; want http, https or socks5"
                ));
            }
        };

        // Strip any path/query/fragment — a proxy URL is just an authority.
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");

        // Split optional `userinfo@` prefix (rightmost '@', like url.Parse).
        let (userinfo, hostport) = match authority.rsplit_once('@') {
            Some((ui, hp)) => (Some(ui), hp),
            None => (None, authority),
        };
        let (username, password) = match userinfo {
            None => (None, None),
            Some(ui) => match ui.split_once(':') {
                Some((u, p)) => (Some(percent_decode(u)), Some(percent_decode(p))),
                None => (Some(percent_decode(ui)), None),
            },
        };

        let default_port = match scheme {
            ProxyScheme::Http => 80,
            ProxyScheme::Https => 443,
            ProxyScheme::Socks5 => 1080,
        };
        let (host, port) = split_host_port(hostport, default_port)?;
        if host.is_empty() {
            return Err(format!("missing proxy host in {proxy_url:?}"));
        }

        Ok(ProxyConfig {
            scheme,
            host,
            port,
            username,
            password,
        })
    }

    /// Proxy address to dial (`host:port`).
    fn addr(&self) -> String {
        if self.host.contains(':') {
            // IPv6 literal — bracket it for the socket-address parser.
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// Splits a `host[:port]` authority (with IPv6 bracket support) into host and
/// port, filling in `default_port` when the port is absent.
fn split_host_port(hostport: &str, default_port: u16) -> Result<(String, u16), String> {
    if let Some(rest) = hostport.strip_prefix('[') {
        // Bracketed IPv6 literal: `[addr]` or `[addr]:port`.
        let end = rest
            .find(']')
            .ok_or_else(|| format!("unterminated IPv6 literal in {hostport:?}"))?;
        let host = rest[..end].to_string();
        let after = &rest[end + 1..];
        let port = match after.strip_prefix(':') {
            Some(p) => parse_port(p)?,
            None if after.is_empty() => default_port,
            None => {
                return Err(format!(
                    "invalid characters after IPv6 literal in {hostport:?}"
                ));
            }
        };
        return Ok((host, port));
    }
    match hostport.rsplit_once(':') {
        // Unbracketed IPv6 literal (multiple colons) — treat as a bare host.
        Some((host, _)) if host.contains(':') => Ok((hostport.to_string(), default_port)),
        Some((host, port)) => Ok((host.to_string(), parse_port(port)?)),
        None => Ok((hostport.to_string(), default_port)),
    }
}

fn parse_port(s: &str) -> Result<u16, String> {
    s.parse::<u16>()
        .map_err(|_| format!("invalid proxy port {s:?}"))
}

/// Minimal RFC 3986 percent-decoding for proxy userinfo (Go's `url.Parse`
/// decodes it). Invalid escapes are left verbatim.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Opens a TCP connection to `target_host:target_port` tunnelled through
/// `proxy`, returning the tunnelled stream ready for the target's own request
/// (and, for an https target, its TLS upgrade). The `timeout` is applied to the
/// proxy connect and to every read/write of the handshake.
pub fn connect_via_proxy(
    proxy: &ProxyConfig,
    target_host: &str,
    target_port: u16,
    timeout: Duration,
) -> Result<TcpStream, String> {
    if proxy.scheme == ProxyScheme::Https {
        // Fail fast before dialing: an https proxy needs TLS to the proxy and
        // then the target's own TLS on top (TLS-over-TLS), which the
        // concrete-`TcpStream` connect path cannot represent.
        return Err(
            "https:// proxy is not supported by this port (would require TLS-over-TLS); \
             use an http:// or socks5:// proxy instead"
                .to_string(),
        );
    }

    let proxy_addr = proxy.addr();
    let sock_addr = proxy_addr
        .to_socket_addrs()
        .map_err(|err| format!("cannot resolve proxy {proxy_addr:?}: {err}"))?
        .next()
        .ok_or_else(|| format!("cannot resolve proxy {proxy_addr:?}: no addresses"))?;
    let mut stream = TcpStream::connect_timeout(&sock_addr, timeout)
        .map_err(|err| format!("cannot connect to proxy {proxy_addr:?}: {err}"))?;
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let _ = stream.set_nodelay(true);

    match proxy.scheme {
        ProxyScheme::Socks5 => {
            socks5_handshake(&mut stream, proxy, target_host, target_port)?;
            Ok(stream)
        }
        ProxyScheme::Http => {
            http_connect(&mut stream, proxy, target_host, target_port)?;
            Ok(stream)
        }
        // Https is rejected above before dialing.
        ProxyScheme::Https => unreachable!("https proxy handled before connect"),
    }
}

// ---------------------------------------------------------------------------
// SOCKS5 (RFC 1928) + username/password auth (RFC 1929)
// ---------------------------------------------------------------------------

const SOCKS5_VERSION: u8 = 0x05;
const SOCKS5_AUTH_NONE: u8 = 0x00;
const SOCKS5_AUTH_USERPASS: u8 = 0x02;
const SOCKS5_AUTH_NO_ACCEPTABLE: u8 = 0xFF;
const SOCKS5_CMD_CONNECT: u8 = 0x01;
const SOCKS5_ATYP_IPV4: u8 = 0x01;
const SOCKS5_ATYP_DOMAIN: u8 = 0x03;
const SOCKS5_ATYP_IPV6: u8 = 0x04;

/// Performs the SOCKS5 greeting, optional RFC 1929 auth, and CONNECT request
/// (RFC 1928). On success the stream is a raw tunnel to the target.
fn socks5_handshake<S: Read + Write>(
    stream: &mut S,
    proxy: &ProxyConfig,
    target_host: &str,
    target_port: u16,
) -> Result<(), String> {
    let has_auth = proxy.username.is_some();

    // RFC 1928 §3: greeting = VER, NMETHODS, METHODS.
    let methods: &[u8] = if has_auth {
        &[SOCKS5_AUTH_NONE, SOCKS5_AUTH_USERPASS]
    } else {
        &[SOCKS5_AUTH_NONE]
    };
    let mut greeting = vec![SOCKS5_VERSION, methods.len() as u8];
    greeting.extend_from_slice(methods);
    write_all(stream, &greeting, "socks5 greeting")?;

    // Method-selection reply = VER, METHOD.
    let mut sel = [0u8; 2];
    read_exact(stream, &mut sel, "socks5 method selection")?;
    if sel[0] != SOCKS5_VERSION {
        return Err(format!(
            "socks5 proxy returned unexpected version {}",
            sel[0]
        ));
    }
    match sel[1] {
        SOCKS5_AUTH_NONE => {}
        SOCKS5_AUTH_USERPASS if has_auth => socks5_userpass_auth(stream, proxy)?,
        SOCKS5_AUTH_NO_ACCEPTABLE => {
            return Err("socks5 proxy rejected all offered auth methods".to_string());
        }
        other => {
            return Err(format!(
                "socks5 proxy selected unsupported auth method {other}"
            ));
        }
    }

    // RFC 1928 §4: CONNECT request = VER, CMD, RSV, ATYP, DST.ADDR, DST.PORT.
    let mut req = vec![SOCKS5_VERSION, SOCKS5_CMD_CONNECT, 0x00];
    append_socks5_addr(&mut req, target_host, target_port)?;
    write_all(stream, &req, "socks5 connect request")?;

    // Reply = VER, REP, RSV, ATYP, BND.ADDR, BND.PORT.
    let mut head = [0u8; 4];
    read_exact(stream, &mut head, "socks5 connect reply")?;
    if head[0] != SOCKS5_VERSION {
        return Err(format!(
            "socks5 proxy returned unexpected reply version {}",
            head[0]
        ));
    }
    if head[1] != 0x00 {
        return Err(format!(
            "socks5 proxy CONNECT failed: {}",
            socks5_reply_message(head[1])
        ));
    }
    // Consume the bound address so the stream is positioned at the tunnel.
    let atyp = head[3];
    let addr_len = match atyp {
        SOCKS5_ATYP_IPV4 => 4,
        SOCKS5_ATYP_IPV6 => 16,
        SOCKS5_ATYP_DOMAIN => {
            let mut len = [0u8; 1];
            read_exact(stream, &mut len, "socks5 bound domain length")?;
            len[0] as usize
        }
        other => {
            return Err(format!(
                "socks5 proxy returned unknown ATYP {other} in reply"
            ));
        }
    };
    let mut discard = vec![0u8; addr_len + 2]; // address + 2-byte port
    read_exact(stream, &mut discard, "socks5 bound address")?;
    Ok(())
}

/// RFC 1929 username/password sub-negotiation.
fn socks5_userpass_auth<S: Read + Write>(
    stream: &mut S,
    proxy: &ProxyConfig,
) -> Result<(), String> {
    let username = proxy.username.as_deref().unwrap_or("");
    let password = proxy.password.as_deref().unwrap_or("");
    if username.len() > 255 || password.len() > 255 {
        return Err("socks5 username/password must be at most 255 bytes each".to_string());
    }
    // VER=0x01, ULEN, UNAME, PLEN, PASSWD.
    let mut msg = vec![0x01u8, username.len() as u8];
    msg.extend_from_slice(username.as_bytes());
    msg.push(password.len() as u8);
    msg.extend_from_slice(password.as_bytes());
    write_all(stream, &msg, "socks5 auth")?;

    let mut resp = [0u8; 2];
    read_exact(stream, &mut resp, "socks5 auth reply")?;
    // RFC 1929: STATUS 0x00 == success.
    if resp[1] != 0x00 {
        return Err("socks5 proxy rejected username/password authentication".to_string());
    }
    Ok(())
}

/// Appends a SOCKS5 address (ATYP + addr + 2-byte big-endian port). IPv4/IPv6
/// literals use their numeric ATYP; anything else is sent as a domain name so
/// the proxy resolves it (matching Go's socks5 dialer, which hands the hostname
/// to the proxy).
fn append_socks5_addr(buf: &mut Vec<u8>, host: &str, port: u16) -> Result<(), String> {
    if let Ok(v4) = host.parse::<std::net::Ipv4Addr>() {
        buf.push(SOCKS5_ATYP_IPV4);
        buf.extend_from_slice(&v4.octets());
    } else if let Ok(v6) = host.parse::<std::net::Ipv6Addr>() {
        buf.push(SOCKS5_ATYP_IPV6);
        buf.extend_from_slice(&v6.octets());
    } else {
        let bytes = host.as_bytes();
        if bytes.len() > 255 {
            return Err(format!("socks5 target host {host:?} is too long"));
        }
        buf.push(SOCKS5_ATYP_DOMAIN);
        buf.push(bytes.len() as u8);
        buf.extend_from_slice(bytes);
    }
    buf.extend_from_slice(&port.to_be_bytes());
    Ok(())
}

/// Maps a SOCKS5 REP code to a human-readable message (RFC 1928 §6).
fn socks5_reply_message(rep: u8) -> String {
    let s = match rep {
        0x01 => "general SOCKS server failure",
        0x02 => "connection not allowed by ruleset",
        0x03 => "network unreachable",
        0x04 => "host unreachable",
        0x05 => "connection refused",
        0x06 => "TTL expired",
        0x07 => "command not supported",
        0x08 => "address type not supported",
        _ => return format!("unknown reply code {rep}"),
    };
    s.to_string()
}

// ---------------------------------------------------------------------------
// HTTP CONNECT tunneling (RFC 9110 §9.3.6)
// ---------------------------------------------------------------------------

/// Builds the HTTP `CONNECT` request bytes for `target_host:target_port`,
/// including a `Proxy-Authorization: Basic ...` header when the proxy carries
/// credentials.
pub fn build_http_connect_request(
    target_host: &str,
    target_port: u16,
    username: Option<&str>,
    password: Option<&str>,
) -> Vec<u8> {
    let host_port = format!("{target_host}:{target_port}");
    let mut req = Vec::with_capacity(128);
    req.extend_from_slice(format!("CONNECT {host_port} HTTP/1.1\r\n").as_bytes());
    req.extend_from_slice(format!("Host: {host_port}\r\n").as_bytes());
    if let Some(user) = username {
        let pass = password.unwrap_or("");
        let creds = format!("{user}:{pass}");
        let encoded = base64_std_encode(creds.as_bytes());
        req.extend_from_slice(format!("Proxy-Authorization: Basic {encoded}\r\n").as_bytes());
    }
    req.extend_from_slice(b"\r\n");
    req
}

/// Sends an HTTP `CONNECT` and validates a 2xx reply. On success the stream is
/// a raw tunnel to the target.
fn http_connect<S: Read + Write>(
    stream: &mut S,
    proxy: &ProxyConfig,
    target_host: &str,
    target_port: u16,
) -> Result<(), String> {
    let req = build_http_connect_request(
        target_host,
        target_port,
        proxy.username.as_deref(),
        proxy.password.as_deref(),
    );
    write_all(stream, &req, "http CONNECT request")?;

    // Read until the end of the response headers (`\r\n\r\n`).
    let mut raw = Vec::with_capacity(256);
    let mut byte = [0u8; 1];
    loop {
        let n = stream
            .read(&mut byte)
            .map_err(|err| format!("cannot read http CONNECT reply: {err}"))?;
        if n == 0 {
            return Err("http proxy closed the connection during CONNECT".to_string());
        }
        raw.push(byte[0]);
        if raw.ends_with(b"\r\n\r\n") {
            break;
        }
        if raw.len() > 64 * 1024 {
            return Err("http CONNECT reply headers too large".to_string());
        }
    }

    let head = String::from_utf8_lossy(&raw);
    let status_line = head.lines().next().unwrap_or("");
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("cannot parse http CONNECT status line {status_line:?}"))?;
    if !(200..300).contains(&status_code) {
        return Err(format!(
            "http proxy CONNECT failed with status {status_code}: {status_line:?}"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// small IO helpers (uniform error messages with the handshake step name)
// ---------------------------------------------------------------------------

fn write_all<S: Write>(stream: &mut S, buf: &[u8], step: &str) -> Result<(), String> {
    stream
        .write_all(buf)
        .and_then(|()| stream.flush())
        .map_err(|err| format!("cannot send {step} to proxy: {err}"))
}

fn read_exact<S: Read>(stream: &mut S, buf: &mut [u8], step: &str) -> Result<(), String> {
    stream
        .read_exact(buf)
        .map_err(|err| format!("cannot read {step} from proxy: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn parse_http_without_userinfo() {
        let p = ProxyConfig::parse("http://proxy.example:3128").unwrap();
        assert_eq!(p.scheme, ProxyScheme::Http);
        assert_eq!(p.host, "proxy.example");
        assert_eq!(p.port, 3128);
        assert_eq!(p.username, None);
        assert_eq!(p.password, None);
    }

    #[test]
    fn parse_defaults_ports_per_scheme() {
        assert_eq!(ProxyConfig::parse("http://h").unwrap().port, 80);
        assert_eq!(ProxyConfig::parse("https://h").unwrap().port, 443);
        assert_eq!(ProxyConfig::parse("socks5://h").unwrap().port, 1080);
    }

    #[test]
    fn parse_with_userinfo() {
        let p = ProxyConfig::parse("socks5://user:pass@10.0.0.1:1080").unwrap();
        assert_eq!(p.scheme, ProxyScheme::Socks5);
        assert_eq!(p.host, "10.0.0.1");
        assert_eq!(p.port, 1080);
        assert_eq!(p.username.as_deref(), Some("user"));
        assert_eq!(p.password.as_deref(), Some("pass"));
    }

    #[test]
    fn parse_username_only_and_percent_encoding() {
        let p = ProxyConfig::parse("http://us%40er:p%3Aass@proxy:8080").unwrap();
        // %40 == '@', %3A == ':'
        assert_eq!(p.username.as_deref(), Some("us@er"));
        assert_eq!(p.password.as_deref(), Some("p:ass"));
    }

    #[test]
    fn parse_ipv6_proxy_host() {
        let p = ProxyConfig::parse("socks5://[::1]:1080").unwrap();
        assert_eq!(p.host, "::1");
        assert_eq!(p.port, 1080);
        assert_eq!(p.addr(), "[::1]:1080");
    }

    #[test]
    fn parse_rejects_unknown_and_missing_scheme() {
        assert!(ProxyConfig::parse("ftp://proxy:21").is_err());
        assert!(ProxyConfig::parse("proxy:1080").is_err());
        assert!(ProxyConfig::parse("socks5://").is_err());
    }

    #[test]
    fn build_connect_request_without_auth() {
        let req = build_http_connect_request("example.com", 443, None, None);
        let s = String::from_utf8(req).unwrap();
        assert_eq!(
            s,
            "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"
        );
    }

    #[test]
    fn build_connect_request_with_auth() {
        let req = build_http_connect_request("example.com", 8080, Some("user"), Some("pass"));
        let s = String::from_utf8(req).unwrap();
        // base64("user:pass") == dXNlcjpwYXNz
        assert!(
            s.contains("Proxy-Authorization: Basic dXNlcjpwYXNz\r\n"),
            "{s}"
        );
        assert!(
            s.starts_with("CONNECT example.com:8080 HTTP/1.1\r\n"),
            "{s}"
        );
        assert!(s.ends_with("\r\n\r\n"));
    }

    #[test]
    fn append_socks5_addr_encodes_domain_ipv4_ipv6() {
        // Domain
        let mut buf = Vec::new();
        append_socks5_addr(&mut buf, "example.com", 443).unwrap();
        assert_eq!(buf[0], SOCKS5_ATYP_DOMAIN);
        assert_eq!(buf[1] as usize, "example.com".len());
        assert_eq!(&buf[2..2 + 11], b"example.com");
        assert_eq!(&buf[buf.len() - 2..], &443u16.to_be_bytes());

        // IPv4
        let mut buf = Vec::new();
        append_socks5_addr(&mut buf, "127.0.0.1", 80).unwrap();
        assert_eq!(buf, vec![SOCKS5_ATYP_IPV4, 127, 0, 0, 1, 0, 80]);

        // IPv6
        let mut buf = Vec::new();
        append_socks5_addr(&mut buf, "::1", 80).unwrap();
        assert_eq!(buf[0], SOCKS5_ATYP_IPV6);
        assert_eq!(buf.len(), 1 + 16 + 2);
    }

    #[test]
    fn socks5_handshake_no_auth_over_cursor() {
        // Fake proxy scripted reply: method selection (no auth) + CONNECT success.
        let mut server_out = Vec::new();
        server_out.extend_from_slice(&[SOCKS5_VERSION, SOCKS5_AUTH_NONE]);
        // Reply: VER, REP=0, RSV, ATYP=IPv4, BND.ADDR(4), BND.PORT(2)
        server_out.extend_from_slice(&[
            SOCKS5_VERSION,
            0x00,
            0x00,
            SOCKS5_ATYP_IPV4,
            0,
            0,
            0,
            0,
            0,
            0,
        ]);
        let mut fake = DuplexCursor::new(server_out);
        let proxy = ProxyConfig::parse("socks5://proxy:1080").unwrap();
        socks5_handshake(&mut fake, &proxy, "example.com", 443).unwrap();

        // Client should have sent greeting [05,01,00] then CONNECT for the domain.
        let sent = fake.written();
        assert_eq!(&sent[..3], &[SOCKS5_VERSION, 1, SOCKS5_AUTH_NONE]);
        assert_eq!(sent[3], SOCKS5_VERSION);
        assert_eq!(sent[4], SOCKS5_CMD_CONNECT);
        assert_eq!(sent[6], SOCKS5_ATYP_DOMAIN);
    }

    #[test]
    fn socks5_handshake_userpass_over_cursor() {
        let mut server_out = Vec::new();
        // Select username/password auth.
        server_out.extend_from_slice(&[SOCKS5_VERSION, SOCKS5_AUTH_USERPASS]);
        // Auth success.
        server_out.extend_from_slice(&[0x01, 0x00]);
        // CONNECT success (domain-typed bound addr, len 0).
        server_out.extend_from_slice(&[SOCKS5_VERSION, 0x00, 0x00, SOCKS5_ATYP_DOMAIN, 0, 0, 0]);
        let mut fake = DuplexCursor::new(server_out);
        let proxy = ProxyConfig::parse("socks5://user:pass@proxy:1080").unwrap();
        socks5_handshake(&mut fake, &proxy, "10.0.0.9", 9000).unwrap();

        let sent = fake.written();
        // Greeting offers both no-auth and user/pass.
        assert_eq!(
            &sent[..4],
            &[SOCKS5_VERSION, 2, SOCKS5_AUTH_NONE, SOCKS5_AUTH_USERPASS]
        );
        // Somewhere after greeting the RFC 1929 auth message appears: 01 04 'user' 04 'pass'.
        let auth_marker = [0x01u8, 4, b'u', b's', b'e', b'r', 4, b'p', b'a', b's', b's'];
        assert!(
            sent.windows(auth_marker.len()).any(|w| w == auth_marker),
            "auth bytes not found in {sent:?}"
        );
    }

    #[test]
    fn socks5_handshake_reports_connect_failure() {
        let server_out = vec![
            SOCKS5_VERSION,
            SOCKS5_AUTH_NONE,
            SOCKS5_VERSION,
            0x05, // REP=connection refused
            0x00,
            SOCKS5_ATYP_IPV4,
            0,
            0,
            0,
            0,
            0,
            0,
        ];
        let mut fake = DuplexCursor::new(server_out);
        let proxy = ProxyConfig::parse("socks5://proxy:1080").unwrap();
        let err = socks5_handshake(&mut fake, &proxy, "example.com", 443).unwrap_err();
        assert!(err.contains("connection refused"), "{err}");
    }

    #[test]
    fn http_connect_success_over_cursor() {
        let mut fake = DuplexCursor::new(b"HTTP/1.1 200 Connection established\r\n\r\n".to_vec());
        let proxy = ProxyConfig::parse("http://proxy:3128").unwrap();
        http_connect(&mut fake, &proxy, "example.com", 443).unwrap();
        let sent = String::from_utf8(fake.written()).unwrap();
        assert!(
            sent.starts_with("CONNECT example.com:443 HTTP/1.1\r\n"),
            "{sent}"
        );
    }

    #[test]
    fn http_connect_rejects_non_2xx() {
        let mut fake = DuplexCursor::new(b"HTTP/1.1 407 Proxy Auth Required\r\n\r\n".to_vec());
        let proxy = ProxyConfig::parse("http://proxy:3128").unwrap();
        let err = http_connect(&mut fake, &proxy, "example.com", 443).unwrap_err();
        assert!(err.contains("407"), "{err}");
    }

    #[test]
    fn https_proxy_is_documented_residual() {
        let proxy = ProxyConfig::parse("https://proxy:443").unwrap();
        let err =
            connect_via_proxy(&proxy, "example.com", 443, Duration::from_secs(1)).unwrap_err();
        assert!(err.contains("TLS-over-TLS"), "{err}");
    }

    /// End-to-end SOCKS5 over a real loopback TCP listener acting as the proxy;
    /// after the handshake the "proxy" echoes bytes, proving the returned
    /// stream is a usable tunnel.
    #[test]
    fn connect_via_socks5_real_listener_echoes() {
        let ln = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = ln.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut s, _) = ln.accept().unwrap();
            // Greeting.
            let mut greeting = [0u8; 3];
            s.read_exact(&mut greeting).unwrap();
            assert_eq!(greeting[0], SOCKS5_VERSION);
            s.write_all(&[SOCKS5_VERSION, SOCKS5_AUTH_NONE]).unwrap();
            // CONNECT request: VER,CMD,RSV,ATYP,...
            let mut head = [0u8; 4];
            s.read_exact(&mut head).unwrap();
            let n = match head[3] {
                SOCKS5_ATYP_IPV4 => 4,
                SOCKS5_ATYP_IPV6 => 16,
                SOCKS5_ATYP_DOMAIN => {
                    let mut l = [0u8; 1];
                    s.read_exact(&mut l).unwrap();
                    l[0] as usize
                }
                _ => panic!("bad atyp"),
            };
            let mut rest = vec![0u8; n + 2];
            s.read_exact(&mut rest).unwrap();
            // Success reply.
            s.write_all(&[SOCKS5_VERSION, 0, 0, SOCKS5_ATYP_IPV4, 0, 0, 0, 0, 0, 0])
                .unwrap();
            // Echo one byte.
            let mut b = [0u8; 1];
            s.read_exact(&mut b).unwrap();
            s.write_all(&b).unwrap();
        });

        let proxy = ProxyConfig::parse(&format!("socks5://{}:{}", addr.ip(), addr.port())).unwrap();
        let mut tunnel =
            connect_via_proxy(&proxy, "example.com", 443, Duration::from_secs(5)).unwrap();
        tunnel.write_all(b"Z").unwrap();
        let mut echoed = [0u8; 1];
        tunnel.read_exact(&mut echoed).unwrap();
        assert_eq!(&echoed, b"Z");
        handle.join().unwrap();
    }

    /// End-to-end HTTP CONNECT over a real loopback listener + echo.
    #[test]
    fn connect_via_http_real_listener_echoes() {
        let ln = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = ln.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut s, _) = ln.accept().unwrap();
            let mut raw = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                s.read_exact(&mut byte).unwrap();
                raw.push(byte[0]);
                if raw.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            assert!(raw.starts_with(b"CONNECT example.com:443 HTTP/1.1\r\n"));
            s.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .unwrap();
            let mut b = [0u8; 1];
            s.read_exact(&mut b).unwrap();
            s.write_all(&b).unwrap();
        });

        let proxy = ProxyConfig::parse(&format!("http://{}:{}", addr.ip(), addr.port())).unwrap();
        let mut tunnel =
            connect_via_proxy(&proxy, "example.com", 443, Duration::from_secs(5)).unwrap();
        tunnel.write_all(b"Q").unwrap();
        let mut echoed = [0u8; 1];
        tunnel.read_exact(&mut echoed).unwrap();
        assert_eq!(&echoed, b"Q");
        handle.join().unwrap();
    }

    /// A read/write fake that serves scripted server bytes on `read` and
    /// captures everything the client `write`s.
    struct DuplexCursor {
        incoming: Cursor<Vec<u8>>,
        outgoing: Vec<u8>,
    }

    impl DuplexCursor {
        fn new(server_bytes: Vec<u8>) -> Self {
            Self {
                incoming: Cursor::new(server_bytes),
                outgoing: Vec::new(),
            }
        }
        fn written(&self) -> Vec<u8> {
            self.outgoing.clone()
        }
    }

    impl Read for DuplexCursor {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.incoming.read(buf)
        }
    }

    impl Write for DuplexCursor {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.outgoing.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}

//! Port of Softalink LLC `lib/flagutil/password.go`.

use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::FlagValue;

/// A flag holding a password.
///
/// The password value is hidden when calling `to_string()` for security
/// reasons, since the returned value can be put in logs. Call
/// [`Password::get`] for obtaining the real password value.
///
/// If the flag value is `file:///path/to/file`, `http://host/path` or
/// `https://host/path`, then its contents are automatically re-read from the
/// given source (Go `Password`). The value is refreshed on access at most once
/// every two seconds; a fetch failure keeps the previously loaded value.
#[derive(Debug, Default)]
pub struct Password {
    next_refresh_timestamp: AtomicU64,

    value: Mutex<String>,

    /// The name of the flag.
    flagname: String,

    /// Either a url or a path to a file with the password.
    source_path: String,
}

impl Password {
    /// Returns a new empty Password with the given flag name, like Go
    /// `NewPassword`.
    pub fn new(flagname: &str) -> Self {
        Password {
            next_refresh_timestamp: AtomicU64::new(0),
            value: Mutex::new(String::new()),
            flagname: flagname.to_string(),
            source_path: String::new(),
        }
    }

    /// Returns the name of the flag.
    pub fn name(&self) -> &str {
        &self.flagname
    }

    /// Returns the current password value.
    ///
    /// It re-reads the value from `file:///path/to/file` if it was passed to
    /// [`Password::set`].
    pub fn get(&self) -> String {
        self.maybe_reread_password();
        self.value.lock().unwrap().clone()
    }

    fn maybe_reread_password(&self) {
        if self.source_path.is_empty() {
            // Fast path - nothing to re-read.
            return;
        }
        let ts_curr = unix_timestamp();
        let ts_next = self.next_refresh_timestamp.load(Ordering::SeqCst);
        if ts_curr < ts_next {
            // Fast path - nothing to re-read.
            return;
        }

        // Re-read the password from self.source_path.
        self.next_refresh_timestamp
            .store(ts_curr + 2, Ordering::SeqCst);
        match read_password_from_file_or_http(&self.source_path) {
            Ok(s) => {
                *self.value.lock().unwrap() = s;
            }
            Err(err) => {
                // Cannot use the logger, since it can be uninitialized yet.
                eprintln!(
                    "flagutil: fall back to the previous password for -{}, since failed to re-read it from {:?}: {}",
                    self.flagname, self.source_path, err
                );
            }
        }
    }

    /// Parses `value`, like Go `Password.Set`.
    pub fn set(&mut self, value: &str) -> Result<(), String> {
        self.next_refresh_timestamp.store(0, Ordering::SeqCst);
        if let Some(path) = value.strip_prefix("file://") {
            self.source_path = path.to_string();
            // Do not attempt to read the password from source_path now, since
            // the file may not exist yet. The password will be read on the
            // first access via Password::get. Generate a random password for
            // now in order to prevent unauthorized access to protected
            // resources while the source_path file doesn't exist.
            self.init_random_value();
            return Ok(());
        }
        if value.starts_with("http://") || value.starts_with("https://") {
            self.source_path = value.to_string();
            self.init_random_value();
            return Ok(());
        }
        self.source_path = String::new();
        *self.value.lock().unwrap() = value.to_string();
        Ok(())
    }

    fn init_random_value(&self) {
        let buf = random_bytes_64();
        // PORT NOTE: Go stores the 64 raw random bytes as a string; Rust
        // strings must be valid UTF-8, so map every byte to a printable ASCII
        // char while keeping the 64-byte length.
        let s: String = buf.iter().map(|&b| (b % 94 + 33) as char).collect();
        *self.value.lock().unwrap() = s;
    }
}

impl fmt::Display for Password {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("secret")
    }
}

impl FlagValue for Password {
    fn parse_flag(s: &str) -> Result<Self, String> {
        let mut p = Password::new("");
        p.set(s)?;
        Ok(p)
    }
}

/// PORT NOTE: `lib/fasttime` isn't wired in yet; use `SystemTime` directly.
fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Port of `fscore.ReadPasswordFromFileOrHTTP` for local files: reads the
/// file and trims trailing whitespace.
fn read_password_from_file_or_http(path: &str) -> Result<String, String> {
    if path.starts_with("http://") || path.starts_with("https://") {
        // Go `fscore.ReadFileOrHTTP`: fetch over http/https and trim trailing
        // whitespace. TLS is verified via `crate::tlsutil` (rustls + the bundled
        // roots).
        let data = fetch_http(path)?;
        return Ok(String::from_utf8_lossy(&data).trim_end().to_string());
    }
    let data =
        std::fs::read_to_string(path).map_err(|err| format!("cannot read {path:?}: {err}"))?;
    Ok(data.trim_end().to_string())
}

/// Connect/read/write timeout for fetching an http(s):// password source.
const HTTP_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Fetches the body of an `http(s)://` URL with a single HTTP/1.0 GET (Go's
/// `http.Get`, reduced to what a password source needs). HTTP/1.0 forces an
/// identity-encoded response the server terminates by closing the connection,
/// so the body is simply everything after the headers — no chunked decoding.
/// https connections are TLS-verified through `crate::tlsutil`.
fn fetch_http(url: &str) -> Result<Vec<u8>, String> {
    use std::io::{Read, Write};
    use std::net::{TcpStream, ToSocketAddrs};

    let (https, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(format!("unsupported scheme in {url:?}"));
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = split_authority(authority, if https { 443 } else { 80 })?;
    let addr = format!("{host}:{port}");

    let sock_addr = addr
        .to_socket_addrs()
        .map_err(|err| format!("cannot resolve {addr:?}: {err}"))?
        .next()
        .ok_or_else(|| format!("cannot resolve {addr:?}: no addresses"))?;
    let tcp = TcpStream::connect_timeout(&sock_addr, HTTP_FETCH_TIMEOUT)
        .map_err(|err| format!("cannot connect to {addr:?}: {err}"))?;
    let _ = tcp.set_read_timeout(Some(HTTP_FETCH_TIMEOUT));
    let _ = tcp.set_write_timeout(Some(HTTP_FETCH_TIMEOUT));

    let req = format!("GET {path} HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    let raw = if https {
        let tc = crate::tlsutil::TLSConfig {
            server_name: host.clone(),
            ..Default::default()
        };
        let cfg = crate::tlsutil::new_tls_client_config(&tc)?;
        let mut s = crate::tlsutil::client_connect(&cfg, &host, tcp)?;
        s.write_all(req.as_bytes())
            .map_err(|err| format!("cannot send request to {url:?}: {err}"))?;
        let mut raw = Vec::new();
        s.read_to_end(&mut raw)
            .map_err(|err| format!("cannot read response from {url:?}: {err}"))?;
        raw
    } else {
        let mut s = tcp;
        s.write_all(req.as_bytes())
            .map_err(|err| format!("cannot send request to {url:?}: {err}"))?;
        let mut raw = Vec::new();
        s.read_to_end(&mut raw)
            .map_err(|err| format!("cannot read response from {url:?}: {err}"))?;
        raw
    };

    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| format!("malformed HTTP response from {url:?}"))?;
    let status = parse_http_status(&raw[..sep])?;
    let body = &raw[sep + 4..];
    if status != 200 {
        let shown = &body[..body.len().min(4 * 1024)];
        return Err(format!(
            "unexpected status code when fetching {url:?}: {status}, expecting 200; response: {:?}",
            String::from_utf8_lossy(shown)
        ));
    }
    Ok(body.to_vec())
}

/// Splits a URL authority into `(host, port)`, defaulting the port and handling
/// bracketed IPv6 literals (`[::1]:8080`).
fn split_authority(authority: &str, default_port: u16) -> Result<(String, u16), String> {
    if let Some(rest) = authority.strip_prefix('[') {
        // IPv6 literal: `[addr]` or `[addr]:port`.
        let close = rest
            .find(']')
            .ok_or_else(|| format!("invalid IPv6 authority {authority:?}"))?;
        let host = rest[..close].to_string();
        let after = &rest[close + 1..];
        let port = match after.strip_prefix(':') {
            Some(p) => p
                .parse()
                .map_err(|_| format!("invalid port in {authority:?}"))?,
            None => default_port,
        };
        return Ok((host, port));
    }
    match authority.rsplit_once(':') {
        Some((h, p)) => Ok((
            h.to_string(),
            p.parse()
                .map_err(|_| format!("invalid port in {authority:?}"))?,
        )),
        None => Ok((authority.to_string(), default_port)),
    }
}

/// Parses the HTTP status code from a response's status line.
fn parse_http_status(head: &[u8]) -> Result<u16, String> {
    let line = head
        .split(|&b| b == b'\r' || b == b'\n')
        .next()
        .unwrap_or(&[]);
    let line = String::from_utf8_lossy(line);
    line.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("cannot parse HTTP status line {line:?}"))
}

/// Returns 64 cryptographically secure random bytes, like Go's `crypto/rand`.
///
/// Uses `/dev/urandom` on Unix and `BCryptGenRandom` (system-preferred RNG) on
/// Windows. Like Go's `crypto/rand` (which panics on failure), a read failure
/// panics rather than degrading to a weak generator. Exotic targets that are
/// neither Unix nor Windows fall back to a clock-seeded hash (not
/// cryptographically secure); the port only builds for Linux and Windows.
fn random_bytes_64() -> [u8; 64] {
    #[cfg(unix)]
    {
        use std::io::Read;
        let mut buf = [0u8; 64];
        match std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut buf)) {
            Ok(()) => buf,
            Err(err) => panic!("cannot read /dev/urandom for secure random bytes: {err}"),
        }
    }
    #[cfg(windows)]
    {
        // SAFETY: BCryptGenRandom fills `buf` (a valid 64-byte buffer) with
        // cryptographically secure bytes from the OS RNG; a null algorithm
        // handle with BCRYPT_USE_SYSTEM_PREFERRED_RNG selects the system RNG.
        #[link(name = "bcrypt")]
        unsafe extern "system" {
            fn BCryptGenRandom(
                h_algorithm: *mut core::ffi::c_void,
                pb_buffer: *mut u8,
                cb_buffer: u32,
                dw_flags: u32,
            ) -> i32;
        }
        const BCRYPT_USE_SYSTEM_PREFERRED_RNG: u32 = 0x0000_0002;
        let mut buf = [0u8; 64];
        let status = unsafe {
            BCryptGenRandom(
                core::ptr::null_mut(),
                buf.as_mut_ptr(),
                buf.len() as u32,
                BCRYPT_USE_SYSTEM_PREFERRED_RNG,
            )
        };
        // BCryptGenRandom returns an NTSTATUS; non-negative means success.
        if status < 0 {
            panic!("BCryptGenRandom failed with NTSTATUS {status:#x}");
        }
        buf
    }
    #[cfg(not(any(unix, windows)))]
    {
        use std::hash::{BuildHasher, Hasher, RandomState};

        let mut buf = [0u8; 64];
        let mut hasher = RandomState::new().build_hasher();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        hasher.write_u32(nanos);
        hasher.write_u32(std::process::id());
        for chunk in buf.chunks_mut(8) {
            hasher.write_u64(hasher.finish());
            let h = hasher.finish().to_le_bytes();
            chunk.copy_from_slice(&h[..chunk.len()]);
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_http_source() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        // A one-shot HTTP/1.0 server that returns a password with a trailing
        // newline (which Go — and the port — trim).
        let ln = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = ln.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut conn, _) = ln.accept().expect("accept");
            let mut buf = [0u8; 512];
            let _ = conn.read(&mut buf);
            conn.write_all(
                b"HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\n\r\nhttp-fetched-secret\n",
            )
            .unwrap();
        });

        let mut p = Password::new("apw");
        p.set(&format!("http://127.0.0.1:{port}/secret")).unwrap();
        // Before the fetch it holds a random value, never the URL.
        assert_ne!(p.to_string(), "http-fetched-secret");
        assert_eq!(p.get(), "http-fetched-secret");
        server.join().unwrap();
    }

    #[test]
    fn test_split_authority() {
        assert_eq!(split_authority("host", 443).unwrap(), ("host".into(), 443));
        assert_eq!(
            split_authority("host:8443", 443).unwrap(),
            ("host".into(), 8443)
        );
        assert_eq!(
            split_authority("[::1]:9000", 80).unwrap(),
            ("::1".into(), 9000)
        );
        assert_eq!(split_authority("[::1]", 80).unwrap(), ("::1".into(), 80));
    }

    #[test]
    fn test_password() {
        let mut p = Password::new("foo");

        // Verify that to_string returns "secret".
        let expected_secret = "secret";
        assert_eq!(p.to_string(), expected_secret);

        // Set a regular password.
        let expected_password = "top-secret-password";
        p.set(expected_password)
            .unwrap_or_else(|err| panic!("cannot set password: {err}"));
        for _ in 0..5 {
            assert_eq!(p.get(), expected_password, "unexpected password");
            assert_eq!(p.to_string(), expected_secret);
        }

        // Read the password from file by absolute path.
        //
        // PORT NOTE: the Go test also reads via a relative
        // `testdata/password.txt` path; the file (with the same trailing
        // newlines) is created in a temp dir here, since cargo test cwd
        // handling differs from `go test`.
        let local_pass_file = std::env::temp_dir().join(format!(
            "esl-common-password-test-{}.txt",
            std::process::id()
        ));
        std::fs::write(&local_pass_file, "foo-bar-baz\n\n\n").unwrap();
        let expected_password = "foo-bar-baz";
        let path = format!("file://{}", local_pass_file.display());
        p.set(&path)
            .unwrap_or_else(|err| panic!("cannot set password to file: {err}"));
        for _ in 0..5 {
            assert_eq!(p.get(), expected_password, "unexpected password");
            assert_eq!(p.to_string(), expected_secret);
        }
        std::fs::remove_file(&local_pass_file).unwrap();

        // Try reading the password from a non-existing url.
        p.set("http://127.0.0.1:56283/aaa/bb?cc=dd")
            .unwrap_or_else(|err| panic!("unexpected error: {err}"));
        for _ in 0..5 {
            let s = p.get();
            assert_eq!(
                s.len(),
                64,
                "unexpected password obtained: {s:?}; must be random 64-byte password"
            );
            assert_eq!(p.to_string(), expected_secret);
        }
    }
}

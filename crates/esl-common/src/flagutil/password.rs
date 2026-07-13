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
/// If the flag value is `file:///path/to/file`, then its contents are
/// automatically re-read from the given file.
///
/// PORT NOTE: `http://`/`https://` sources are accepted like in Go, but
/// re-reading over HTTP is not implemented in this port (no HTTP client
/// dependency); such sources always fall back to the previously stored
/// random value.
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
        // PORT NOTE: HTTP fetching isn't implemented in this port.
        return Err(format!(
            "cannot fetch {path:?}: http fetching isn't supported in this port"
        ));
    }
    let data =
        std::fs::read_to_string(path).map_err(|err| format!("cannot read {path:?}: {err}"))?;
    Ok(data.trim_end().to_string())
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

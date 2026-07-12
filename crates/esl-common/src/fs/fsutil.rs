//! Port of Softalink LLC `lib/fs/fsutil` (fsutil.go + concurrency.go).

use std::sync::{Condvar, LazyLock, Mutex, OnceLock};

use crate::flagutil::{Flag, FlagValue};

/// Returns true if fsync must be disabled.
///
/// The fsync is disabled in tests, since it significantly slows down tests
/// which work with files. The fsync can be enabled in tests by setting the
/// DISABLE_FSYNC_FOR_TESTING environment variable to false.
///
/// The fsync is enabled for ordinary programs. It can be disabled by setting
/// the DISABLE_FSYNC_FOR_TESTING environment variable to true.
pub fn is_fsync_disabled() -> bool {
    *IS_FSYNC_DISABLED.get_or_init(is_fsync_disabled_internal)
}

static IS_FSYNC_DISABLED: OnceLock<bool> = OnceLock::new();

fn is_fsync_disabled_internal() -> bool {
    let s = std::env::var("DISABLE_FSYNC_FOR_TESTING").ok();
    is_fsync_disabled_for_value(s.as_deref())
}

// PORT NOTE: the Go test mutates the DISABLE_FSYNC_FOR_TESTING environment
// variable around isFsyncDisabledInternal() calls; `std::env::set_var` is
// unsafe with concurrent test threads in Rust, so the env-dependent logic is
// split into this pure function which the test drives directly.
fn is_fsync_disabled_for_value(s: Option<&str>) -> bool {
    let s = s.unwrap_or("");
    if s.is_empty() {
        return is_testing();
    }
    // strconv.ParseBool semantics; parse errors mean "fsync enabled".
    <bool as FlagValue>::parse_flag(s).unwrap_or(false)
}

// PORT NOTE: Go's testing.Testing() reports whether the binary was built by
// `go test`. Rust has no exact equivalent: cfg!(test) covers this crate's own
// unit tests, and the executable-in-`deps` heuristic covers Cargo-built test
// binaries of dependent crates (Cargo places them in target/<profile>/deps/).
fn is_testing() -> bool {
    if cfg!(test) {
        return true;
    }
    std::env::current_exe().is_ok_and(|p| {
        p.parent()
            .is_some_and(|d| d.file_name().is_some_and(|n| n == "deps"))
    })
}

static MAX_CONCURRENCY: Flag<usize> = Flag::new(
    "fs.maxConcurrency",
    "The maximum number of concurrent goroutines to work with files; smaller values may help reducing Go scheduling latency \
on systems with small number of CPU cores; higher values may help reducing data ingestion latency on systems with high-latency storage such as NFS or Ceph",
    get_default_concurrency,
);

fn get_default_concurrency() -> usize {
    // PORT NOTE: lib/cgroup is ported separately; std::thread::available_parallelism()
    // stands in for cgroup.AvailableCPUs() here.
    let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
    (16 * cpus).min(256)
}

/// A counting semaphore for limiting the concurrency of operations with files.
///
/// PORT NOTE: Go returns a `chan struct{}` used as a counting semaphore;
/// the Rust port is a Mutex/Condvar semaphore with the same capacity
/// semantics. `acquire()` corresponds to `concurrencyCh <- struct{}{}` and
/// dropping the returned permit corresponds to `<-concurrencyCh`.
pub struct ConcurrencyCh {
    cap: usize,
    state: Mutex<usize>,
    cv: Condvar,
}

/// A held slot in [`ConcurrencyCh`]; released on drop.
pub struct ConcurrencyPermit<'a> {
    ch: &'a ConcurrencyCh,
}

impl ConcurrencyCh {
    /// Blocks until a slot is available and occupies it.
    pub fn acquire(&self) -> ConcurrencyPermit<'_> {
        let mut n = self.state.lock().unwrap();
        while *n >= self.cap {
            n = self.cv.wait(n).unwrap();
        }
        *n += 1;
        ConcurrencyPermit { ch: self }
    }
}

impl Drop for ConcurrencyPermit<'_> {
    fn drop(&mut self) {
        let mut n = self.ch.state.lock().unwrap();
        *n -= 1;
        self.ch.cv.notify_one();
    }
}

/// Returns a semaphore for limiting the concurrency of operations with files.
pub fn get_concurrency_ch() -> &'static ConcurrencyCh {
    static CH: LazyLock<ConcurrencyCh> = LazyLock::new(|| ConcurrencyCh {
        cap: *MAX_CONCURRENCY.get(),
        state: Mutex::new(0),
        cv: Condvar::new(),
    });
    &CH
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_fsync_disabled_internal() {
        let f = |env_var_value: &str, result_expected: bool| {
            let result = is_fsync_disabled_for_value(Some(env_var_value));
            assert_eq!(
                result, result_expected,
                "unexpected value for DISABLE_FSYNC_FOR_TESTING={env_var_value:?}; got {result}; want {result_expected}"
            );
        };

        // fsync must be unconditionally disabled in tests
        f("", true);

        f("TRUE", true);
        f("True", true);
        f("true", true);
        f("T", true);
        f("t", true);
        f("1", true);
        f("FALSE", false);
        f("False", false);
        f("false", false);
        f("F", false);
        f("f", false);
        f("0", false);

        f("unsupported", false);
        f("tRuE", false);
    }
}

//! Port of Softalink LLC `lib/memory`.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::flagutil::{Bytes, Flag};

static ALLOWED_PERCENT: Flag<f64> = Flag::new(
    "memory.allowedPercent",
    "Allowed percent of system memory Softalink LLC caches may occupy. See also -memory.allowedBytes. Too low a value may increase cache miss rate usually resulting in higher CPU and disk IO usage. Too high a value may evict too much data from the OS page cache which will result in higher disk IO usage",
    || 60.0,
);
crate::register_flag!(ALLOWED_PERCENT);

static ALLOWED_BYTES: Flag<Bytes> = Flag::new(
    "memory.allowedBytes",
    "Allowed size of system memory Softalink LLC caches may occupy. This option overrides -memory.allowedPercent if set to a non-zero value. Too low a value may increase the cache miss rate usually resulting in higher CPU and disk IO usage. Too high a value may evict too much data from the OS page cache resulting in higher disk IO usage. The process may behave unexpectedly if this flag is set too small (e.g., 1 byte).",
    || Bytes::with_default(0),
);
crate::register_flag!(ALLOWED_BYTES);

struct MemLimits {
    allowed: usize,
    remaining: usize,
}

static LIMITS: OnceLock<MemLimits> = OnceLock::new();

// The detected system memory limit, exported via `process_memory_limit_bytes`.
// Like the Go package-level `memoryLimit` var, it stays 0 until the first
// allowed()/remaining() call runs init_once.
static MEMORY_LIMIT: AtomicU64 = AtomicU64::new(0);

/// Registers the `process_memory_limit_bytes` gauge, mirroring the Go
/// package-level `metrics.NewGauge` var.
///
/// PORT NOTE: Go registers this at package init; the port registers it from
/// `appmetrics::init_start_time` (the package-init stand-in). Like in Go, the
/// gauge reports 0 until the first `allowed()`/`remaining()` call detects the
/// memory limit.
pub(crate) fn register_metrics() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        crate::metrics::new_gauge(
            "process_memory_limit_bytes",
            Some(Box::new(|| MEMORY_LIMIT.load(Ordering::Relaxed) as f64)),
        );
    });
}

// PORT NOTE: the Go package panics when initOnce runs before flag.Parse; Rust
// flags resolve lazily through flagutil, so there is no parsed-state to check.
fn init_once() -> MemLimits {
    let memory_limit = sys_total_memory() as i64;
    MEMORY_LIMIT.store(memory_limit as u64, Ordering::Relaxed);
    let allowed_bytes = ALLOWED_BYTES.get().n;
    if allowed_bytes <= 0 {
        let allowed_percent = *ALLOWED_PERCENT.get();
        if !(1.0..=100.0).contains(&allowed_percent) {
            crate::fatalf!(
                "FATAL: -memory.allowedPercent must be in the range [1...100]; got {allowed_percent}"
            );
        }
        let percent = allowed_percent / 100.0;
        let allowed_memory = (memory_limit as f64 * percent) as i64;
        let remaining_memory = memory_limit - allowed_memory;
        if remaining_memory <= 0 {
            crate::fatalf!(
                "BUG: remaining memory {remaining_memory} bytes cannot be less than or equal to zero, detected system memory limit {memory_limit} bytes, -memory.allowedPercent={allowed_percent}"
            );
        }
        crate::infof!(
            "limiting caches to {allowed_memory} bytes, leaving {remaining_memory} bytes to the OS according to -memory.allowedPercent={allowed_percent}, system memory limit {memory_limit} bytes"
        );
        MemLimits {
            allowed: allowed_memory as usize,
            remaining: remaining_memory as usize,
        }
    } else {
        let allowed_memory = ALLOWED_BYTES.get().int_n() as i64;
        if allowed_memory < 1024 * 1024 {
            // It's fair to print a hint if the allowedBytes is set to too small, typically by misconfiguration.
            crate::warnf!(
                "allowed memory {allowed_memory} bytes set by -memory.allowedBytes is low. The process may behave unexpectedly."
            );
        }
        let remaining_memory = memory_limit - allowed_memory;
        let allowed_bytes_str = ALLOWED_BYTES.get().to_string();
        if remaining_memory <= 0 {
            crate::fatalf!(
                "FATAL: remaining memory {remaining_memory} bytes cannot be less than or equal to zero, detected system memory limit {memory_limit} bytes, -memory.allowedBytes={allowed_bytes_str}"
            );
        }
        crate::infof!(
            "limiting caches to {allowed_memory} bytes, leaving {remaining_memory} bytes to the OS according to -memory.allowedBytes={allowed_bytes_str}, system memory limit {memory_limit} bytes"
        );
        MemLimits {
            allowed: allowed_memory as usize,
            remaining: remaining_memory as usize,
        }
    }
}

/// Returns the amount of system memory allowed to use by the app.
pub fn allowed() -> usize {
    LIMITS.get_or_init(init_once).allowed
}

/// Returns the amount of memory remaining to the OS.
pub fn remaining() -> usize {
    LIMITS.get_or_init(init_once).remaining
}

#[cfg(target_os = "linux")]
fn sys_total_memory() -> usize {
    // SAFETY: sysinfo(2) only writes into the zero-initialized struct passed to it.
    let mut si: libc::sysinfo = unsafe { std::mem::zeroed() };
    if unsafe { libc::sysinfo(&mut si) } != 0 {
        crate::panicf!(
            "FATAL: error in syscall.Sysinfo: {}",
            std::io::Error::last_os_error()
        );
    }
    const MAX_INT: u64 = i64::MAX as u64;
    let totalram = si.totalram as u64;
    let unit = u64::from(si.mem_unit);
    let mut total_mem = MAX_INT as usize;
    if totalram > 0 && MAX_INT / totalram > unit {
        total_mem = (totalram * unit) as usize;
    }
    let mem = crate::cgroup::get_memory_limit();
    if mem > 0 && mem as u64 <= total_mem as u64 {
        return mem as usize;
    }
    // Try reading hierarchical memory limit.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/699
    let mem = crate::cgroup::get_hierarchical_memory_limit();
    if mem > 0 && mem as u64 <= total_mem as u64 {
        return mem as usize;
    }
    total_mem
}

#[cfg(windows)]
fn sys_total_memory() -> usize {
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

    // SAFETY: GlobalMemoryStatusEx only writes into the struct whose dwLength
    // is set to the struct size, as required by the API.
    let mut msx: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
    msx.dwLength = std::mem::size_of::<MEMORYSTATUSEX>() as u32;
    let r = unsafe { GlobalMemoryStatusEx(&mut msx) };
    if r == 0 {
        crate::panicf!(
            "FATAL: error in GlobalMemoryStatusEx: {}",
            std::io::Error::last_os_error()
        );
    }
    match usize::try_from(msx.ullTotalPhys) {
        Ok(n) => n,
        Err(_) => {
            crate::panicf!(
                "FATAL: int overflow for msx.ullTotalPhys={}",
                msx.ullTotalPhys
            );
            unreachable!()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The Go package has no tests; this pins that -memory.allowedBytes goes
    // through flagutil.Bytes and accepts size suffixes like Go's
    // flagutil.NewBytes (the flag itself cannot be set from a test, since
    // flags read the process arguments).
    #[test]
    fn test_allowed_bytes_accepts_size_suffixes() {
        use crate::flagutil::FlagValue as _;
        for (value, want) in [
            ("64MiB", 64i64 * 1024 * 1024),
            ("1KiB", 1024),
            ("1KB", 1000),
            ("0.25GiB", 256 * 1024 * 1024),
            ("123", 123),
        ] {
            let b = Bytes::parse_flag(value)
                .unwrap_or_else(|err| panic!("cannot parse {value:?}: {err}"));
            assert_eq!(b.n, want, "unexpected value for {value:?}");
        }
    }

    // The Go package has no tests; this sanity-checks the default
    // -memory.allowedPercent=60 split on the host.
    #[test]
    fn test_allowed_and_remaining() {
        let a = allowed();
        let r = remaining();
        assert!(a > 0, "allowed()={a} must be positive");
        assert!(r > 0, "remaining()={r} must be positive");
        // With the 60% default, allowed must exceed remaining.
        assert!(a > r, "allowed()={a} must be greater than remaining()={r}");
    }
}

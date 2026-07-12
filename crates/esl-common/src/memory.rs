//! Port of Softalink LLC `lib/memory`.

use std::sync::OnceLock;

use crate::flagutil::Flag;

static ALLOWED_PERCENT: Flag<f64> = Flag::new(
    "memory.allowedPercent",
    "Allowed percent of system memory Softalink LLC caches may occupy. See also -memory.allowedBytes. Too low a value may increase cache miss rate usually resulting in higher CPU and disk IO usage. Too high a value may evict too much data from the OS page cache which will result in higher disk IO usage",
    || 60.0,
);

// PORT NOTE: Go declares this via flagutil.NewBytes, which accepts KB/MB/GB
// (and KiB/MiB/GiB) suffixes. The Bytes flag type isn't ported to
// esl_common::flagutil yet, so this parses a plain integer number of bytes.
static ALLOWED_BYTES: Flag<i64> = Flag::new(
    "memory.allowedBytes",
    "Allowed size of system memory Softalink LLC caches may occupy. This option overrides -memory.allowedPercent if set to a non-zero value. Too low a value may increase the cache miss rate usually resulting in higher CPU and disk IO usage. Too high a value may evict too much data from the OS page cache resulting in higher disk IO usage. The process may behave unexpectedly if this flag is set too small (e.g., 1 byte).",
    || 0,
);

struct MemLimits {
    allowed: usize,
    remaining: usize,
}

static LIMITS: OnceLock<MemLimits> = OnceLock::new();

// PORT NOTE: the Go package panics when initOnce runs before flag.Parse and
// exports a `process_memory_limit_bytes` gauge via lib/metrics. Rust flags
// resolve lazily through flagutil (so there is no parsed-state to check) and
// the metrics package isn't ported yet.
fn init_once() -> MemLimits {
    let memory_limit = sys_total_memory() as i64;
    let allowed_bytes = *ALLOWED_BYTES.get();
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
        let allowed_memory = allowed_bytes;
        if allowed_memory < 1024 * 1024 {
            // It's fair to print a hint if the allowedBytes is set to too small, typically by misconfiguration.
            crate::warnf!(
                "allowed memory {allowed_memory} bytes set by -memory.allowedBytes is low. The process may behave unexpectedly."
            );
        }
        let remaining_memory = memory_limit - allowed_memory;
        if remaining_memory <= 0 {
            crate::fatalf!(
                "FATAL: remaining memory {remaining_memory} bytes cannot be less than or equal to zero, detected system memory limit {memory_limit} bytes, -memory.allowedBytes={allowed_bytes}"
            );
        }
        crate::infof!(
            "limiting caches to {allowed_memory} bytes, leaving {remaining_memory} bytes to the OS according to -memory.allowedBytes={allowed_bytes}, system memory limit {memory_limit} bytes"
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

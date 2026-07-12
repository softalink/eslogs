//! Port of `github.com/VictoriaMetrics/metrics/process_metrics_windows.go`.
//!
//! Uses `windows-sys` instead of the lazily-loaded psapi/kernel32 procs Go
//! resolves at runtime; the exposed metric set is identical.

use windows_sys::Win32::Foundation::FILETIME;
use windows_sys::Win32::System::ProcessStatus::{
    K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS_EX,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetProcessHandleCount, GetProcessTimes,
};

use super::{write_counter_float64, write_counter_uint64, write_gauge_uint64};

/// 100ns FILETIME units between 1601-01-01 and the unix epoch.
const EPOCH_DIFF_100NS: u64 = 116_444_736_000_000_000;

fn filetime_100ns(ft: &FILETIME) -> u64 {
    (u64::from(ft.dwHighDateTime) << 32) | u64::from(ft.dwLowDateTime)
}

pub(super) fn write_process_metrics(w: &mut String) {
    // SAFETY: GetCurrentProcess returns a pseudo handle that needs no
    // cleanup; the out-params are plain structs owned by this frame.
    unsafe {
        let h = GetCurrentProcess();
        let mut start_time: FILETIME = std::mem::zeroed();
        let mut exit_time: FILETIME = std::mem::zeroed();
        let mut stime: FILETIME = std::mem::zeroed();
        let mut utime: FILETIME = std::mem::zeroed();
        if GetProcessTimes(h, &mut start_time, &mut exit_time, &mut stime, &mut utime) == 0 {
            eprintln!("ERROR: metrics: cannot read process times");
            return;
        }
        let mut mc: PROCESS_MEMORY_COUNTERS_EX = std::mem::zeroed();
        mc.cb = size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32;
        if K32GetProcessMemoryInfo(h, std::ptr::from_mut(&mut mc).cast(), mc.cb) == 0 {
            eprintln!("ERROR: metrics: cannot read process memory information");
            return;
        }
        let stime_seconds = filetime_100ns(&stime) as f64 / 1e7;
        let utime_seconds = filetime_100ns(&utime) as f64 / 1e7;
        write_counter_float64(w, "process_cpu_seconds_system_total", stime_seconds);
        write_counter_float64(
            w,
            "process_cpu_seconds_total",
            stime_seconds + utime_seconds,
        );
        write_counter_float64(w, "process_cpu_seconds_user_total", utime_seconds);
        write_counter_uint64(w, "process_pagefaults_total", u64::from(mc.PageFaultCount));
        write_gauge_uint64(
            w,
            "process_start_time_seconds",
            filetime_100ns(&start_time).saturating_sub(EPOCH_DIFF_100NS) / 10_000_000,
        );
        write_gauge_uint64(w, "process_virtual_memory_bytes", mc.PrivateUsage as u64);
        write_gauge_uint64(
            w,
            "process_resident_memory_peak_bytes",
            mc.PeakWorkingSetSize as u64,
        );
        write_gauge_uint64(w, "process_resident_memory_bytes", mc.WorkingSetSize as u64);
    }
}

pub(super) fn write_fd_metrics(w: &mut String) {
    // SAFETY: pseudo process handle plus an out-param owned by this frame.
    unsafe {
        let h = GetCurrentProcess();
        let mut count: u32 = 0;
        if GetProcessHandleCount(h, &mut count) == 0 {
            eprintln!("ERROR: metrics: cannot determine open file descriptors count");
            return;
        }
        // It seems to be a hard-coded limit for 64-bit systems:
        // https://learn.microsoft.com/en-us/archive/blogs/markrussinovich/pushing-the-limits-of-windows-handles#maximum-number-of-handles
        write_gauge_uint64(w, "process_max_fds", 16777216);
        write_gauge_uint64(w, "process_open_fds", u64::from(count));
    }
}

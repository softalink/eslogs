//! Port of `github.com/VictoriaMetrics/metrics/process_metrics_linux.go`.
//!
//! PORT NOTE: errors are reported with `eprintln!`, mirroring Go's
//! `log.Printf` default destination (stderr); the metrics package doesn't use
//! `lib/logger` upstream either.
//!
//! PORT NOTE: Go captures the initial PSI snapshot (`psiMetricsStart`) at
//! package init; Rust has no life-before-main, so the port captures it when
//! [`init_psi_baseline`] is first called (`appmetrics::init_start_time`, i.e.
//! at httpserver start) or lazily on the first metrics write.

use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

use super::{write_counter_float64, write_counter_uint64, write_gauge_uint64};

/// See <https://github.com/prometheus/procfs/blob/a4ac0826abceb44c40fc71daed2b301db498b93e/proc_stat.go#L40>.
const USER_HZ: f64 = 100.0;

/// Different environments may have different page size.
static PAGE_SIZE_BYTES: LazyLock<u64> =
    LazyLock::new(|| unsafe { libc::sysconf(libc::_SC_PAGESIZE) as u64 });

/// See <http://man7.org/linux/man-pages/man5/proc.5.html>. Only the fields
/// used by the exposed metrics are kept.
struct ProcStat {
    minflt: u64,
    majflt: u64,
    utime: u64,
    stime: u64,
    num_threads: u64,
    vsize: u64,
    rss: u64,
}

pub(super) fn write_process_metrics(w: &mut String) {
    let stat_filepath = "/proc/self/stat";
    let data = match std::fs::read_to_string(stat_filepath) {
        Ok(data) => data,
        Err(err) => {
            eprintln!("ERROR: metrics: cannot open {stat_filepath}: {err}");
            return;
        }
    };

    // Search for the end of the command.
    let Some(n) = data.rfind(") ") else {
        eprintln!(
            "ERROR: metrics: cannot find command in parentheses in {data:?} read from {stat_filepath}"
        );
        return;
    };
    let data = &data[n + 2..];

    let Some(p) = parse_proc_stat(data) else {
        eprintln!("ERROR: metrics: cannot parse {data:?} read from {stat_filepath}");
        return;
    };

    // It is expensive obtaining `process_open_fds` when a big number of file
    // descriptors is opened, so don't do it here. See write_fd_metrics instead.

    let utime = p.utime as f64 / USER_HZ;
    let stime = p.stime as f64 / USER_HZ;

    // Calculate the total time by dividing the sum of utime and stime by
    // USER_HZ. This reduces possible floating-point precision loss.
    let total_time = (p.utime + p.stime) as f64 / USER_HZ;

    write_counter_float64(w, "process_cpu_seconds_system_total", stime);
    write_counter_float64(w, "process_cpu_seconds_total", total_time);
    write_counter_float64(w, "process_cpu_seconds_user_total", utime);
    write_counter_uint64(w, "process_major_pagefaults_total", p.majflt);
    write_counter_uint64(w, "process_minor_pagefaults_total", p.minflt);
    write_gauge_uint64(w, "process_num_threads", p.num_threads);
    write_gauge_uint64(w, "process_resident_memory_bytes", p.rss * *PAGE_SIZE_BYTES);
    write_gauge_uint64(w, "process_start_time_seconds", *START_TIME_SECONDS);
    write_gauge_uint64(w, "process_virtual_memory_bytes", p.vsize);
    write_process_mem_metrics(w);
    write_io_metrics(w);
    write_psi_metrics(w);
}

/// Parses the whitespace-separated `/proc/self/stat` fields following the
/// `comm` value (Go scans them with `fmt.Fscanf`).
fn parse_proc_stat(data: &str) -> Option<ProcStat> {
    let fields: Vec<&str> = data.split_ascii_whitespace().collect();
    // state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt
    // cmajflt utime stime cutime cstime priority nice num_threads
    // itrealvalue starttime vsize rss
    if fields.len() < 22 {
        return None;
    }
    Some(ProcStat {
        minflt: fields[7].parse().ok()?,
        majflt: fields[9].parse().ok()?,
        utime: fields[11].parse().ok()?,
        stime: fields[12].parse().ok()?,
        num_threads: fields[17].parse().ok()?,
        vsize: fields[20].parse().ok()?,
        rss: fields[21].parse().ok()?,
    })
}

fn write_io_metrics(w: &mut String) {
    let io_filepath = "/proc/self/io";
    // Do not spam the logs with errors - this error cannot be fixed without a
    // process restart (see https://github.com/VictoriaMetrics/metrics/issues/42);
    // Go logs it once, the port skips the metrics silently.
    let data = std::fs::read_to_string(io_filepath).unwrap_or_default();

    let get_int = |s: &str| -> u64 {
        let Some(n) = s.find(' ') else {
            eprintln!("ERROR: metrics: cannot find whitespace in {s:?} at {io_filepath}");
            return 0;
        };
        match s[n + 1..].parse() {
            Ok(v) => v,
            Err(err) => {
                eprintln!("ERROR: metrics: cannot parse {s:?} at {io_filepath}: {err}");
                0
            }
        }
    };
    let mut rchar = 0u64;
    let mut wchar = 0u64;
    let mut syscr = 0u64;
    let mut syscw = 0u64;
    let mut read_bytes = 0u64;
    let mut write_bytes = 0u64;
    for s in data.lines() {
        let s = s.trim();
        if s.starts_with("rchar: ") {
            rchar = get_int(s);
        } else if s.starts_with("wchar: ") {
            wchar = get_int(s);
        } else if s.starts_with("syscr: ") {
            syscr = get_int(s);
        } else if s.starts_with("syscw: ") {
            syscw = get_int(s);
        } else if s.starts_with("read_bytes: ") {
            read_bytes = get_int(s);
        } else if s.starts_with("write_bytes: ") {
            write_bytes = get_int(s);
        }
    }
    write_gauge_uint64(w, "process_io_read_bytes_total", rchar);
    write_gauge_uint64(w, "process_io_written_bytes_total", wchar);
    write_gauge_uint64(w, "process_io_read_syscalls_total", syscr);
    write_gauge_uint64(w, "process_io_write_syscalls_total", syscw);
    write_gauge_uint64(w, "process_io_storage_read_bytes_total", read_bytes);
    write_gauge_uint64(w, "process_io_storage_written_bytes_total", write_bytes);
}

/// Writes PSI total metrics for the current process to `w`
/// (Go `writePSIMetrics`).
///
/// See <https://docs.kernel.org/accounting/psi.html>
fn write_psi_metrics(w: &mut String) {
    let Some(start) = &*PSI_METRICS_START else {
        // Failed to initialize PSI metrics.
        return;
    };

    let m = match get_psi_metrics() {
        Ok(Some(m)) => m,
        // The cgroup v2 path disappeared; nothing to expose.
        Ok(None) => return,
        Err(err) => {
            eprintln!("ERROR: metrics: cannot expose PSI metrics: {err}");
            return;
        }
    };

    write_counter_float64(
        w,
        "process_pressure_cpu_waiting_seconds_total",
        psi_total_secs(m.cpu_some.wrapping_sub(start.cpu_some)),
    );
    write_counter_float64(
        w,
        "process_pressure_cpu_stalled_seconds_total",
        psi_total_secs(m.cpu_full.wrapping_sub(start.cpu_full)),
    );

    write_counter_float64(
        w,
        "process_pressure_io_waiting_seconds_total",
        psi_total_secs(m.io_some.wrapping_sub(start.io_some)),
    );
    write_counter_float64(
        w,
        "process_pressure_io_stalled_seconds_total",
        psi_total_secs(m.io_full.wrapping_sub(start.io_full)),
    );

    write_counter_float64(
        w,
        "process_pressure_memory_waiting_seconds_total",
        psi_total_secs(m.mem_some.wrapping_sub(start.mem_some)),
    );
    write_counter_float64(
        w,
        "process_pressure_memory_stalled_seconds_total",
        psi_total_secs(m.mem_full.wrapping_sub(start.mem_full)),
    );
}

fn psi_total_secs(microsecs: u64) -> f64 {
    // PSI total stats is in microseconds according to
    // https://docs.kernel.org/accounting/psi.html . Convert it to seconds.
    microsecs as f64 / 1e6
}

/// The initial PSI metric values on program start; needed in order to make
/// sure the exposed PSI metrics start from zero (Go `psiMetricsStart`,
/// captured at package init — see the module PORT NOTE).
static PSI_METRICS_START: LazyLock<Option<PsiMetrics>> =
    LazyLock::new(|| match get_psi_metrics() {
        Ok(m) => m,
        Err(err) => {
            eprintln!("INFO: metrics: disable exposing PSI metrics because of failed init: {err}");
            None
        }
    });

/// Captures the initial PSI snapshot (see [`PSI_METRICS_START`]).
pub(super) fn init_psi_baseline() {
    LazyLock::force(&PSI_METRICS_START);
}

#[derive(Default)]
struct PsiMetrics {
    cpu_some: u64,
    cpu_full: u64,
    io_some: u64,
    io_full: u64,
    mem_some: u64,
    mem_full: u64,
}

/// Returns the current PSI totals, or `None` when the process doesn't run
/// under cgroup v2 (Go `getPSIMetrics` returning `nil, nil`).
fn get_psi_metrics() -> Result<Option<PsiMetrics>, String> {
    let cgroup_path = get_cgroup_v2_path();
    if cgroup_path.is_empty() {
        // Do nothing, since PSI requires cgroup v2, and the process doesn't
        // run under cgroup v2.
        return Ok(None);
    }

    let (cpu_some, cpu_full) = read_psi_totals(&cgroup_path, "cpu.pressure")?;
    let (io_some, io_full) = read_psi_totals(&cgroup_path, "io.pressure")?;
    let (mem_some, mem_full) = read_psi_totals(&cgroup_path, "memory.pressure")?;

    Ok(Some(PsiMetrics {
        cpu_some,
        cpu_full,
        io_some,
        io_full,
        mem_some,
        mem_full,
    }))
}

/// Parses the `some`/`full` totals from a cgroup v2 pressure file
/// (Go `readPSITotals`).
fn read_psi_totals(cgroup_path: &str, stats_name: &str) -> Result<(u64, u64), String> {
    let file_path = format!("{cgroup_path}/{stats_name}");
    let data = std::fs::read_to_string(&file_path).map_err(|err| err.to_string())?;

    let mut some = 0u64;
    let mut full = 0u64;
    for line in data.lines() {
        let line = line.trim();
        if !line.starts_with("some ") && !line.starts_with("full ") {
            continue;
        }

        let Some((_, total)) = line.split_once("total=") else {
            return Err(format!(
                "cannot find total from the line {line:?} at {file_path:?}"
            ));
        };
        let microsecs: u64 = total
            .parse()
            .map_err(|err| format!("cannot parse total={total:?} at {file_path:?}: {err}"))?;

        if line.starts_with("some ") {
            some = microsecs;
        } else {
            full = microsecs;
        }
    }
    Ok((some, full))
}

/// Returns the cgroup v2 path for the current process, or an empty string
/// when the process doesn't run under cgroup v2 (Go `getCgroupV2Path`).
fn get_cgroup_v2_path() -> String {
    let Ok(data) = std::fs::read_to_string("/proc/self/cgroup") else {
        return String::new();
    };
    let Some((_, rest)) = data.split_once("::") else {
        return String::new();
    };
    let path = format!("/sys/fs/cgroup{}", rest.trim());

    // Drop trailing slash if it exists. This prevents from '//' in the
    // constructed paths by the caller.
    match path.strip_suffix('/') {
        Some(p) => p.to_string(),
        None => path,
    }
}

/// The process start time as a unix timestamp.
///
/// PORT NOTE: Go captures `time.Now().Unix()` at package init, which is only
/// an approximation of the process start; Rust has no life-before-main, so
/// the port derives the exact value from the boot time (`btime` in
/// `/proc/stat`) plus the process start tick in `/proc/self/stat`, falling
/// back to the current time on parse failures.
static START_TIME_SECONDS: LazyLock<u64> = LazyLock::new(|| {
    read_start_time_seconds().unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    })
});

fn read_start_time_seconds() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let btime: u64 = stat
        .lines()
        .find_map(|line| line.strip_prefix("btime "))?
        .trim()
        .parse()
        .ok()?;
    let self_stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<&str> = self_stat[self_stat.rfind(") ")? + 2..]
        .split_ascii_whitespace()
        .collect();
    let starttime_ticks: u64 = fields.get(19)?.parse().ok()?;
    Some(btime + starttime_ticks / USER_HZ as u64)
}

/// Writes `process_max_fds` and `process_open_fds` metrics to `w`.
pub(super) fn write_fd_metrics(w: &mut String) {
    let total_open_fds = match get_open_fds_count("/proc/self/fd") {
        Ok(n) => n,
        Err(err) => {
            eprintln!("ERROR: metrics: cannot determine open file descriptors count: {err}");
            return;
        }
    };
    let max_open_fds = match get_max_files_limit("/proc/self/limits") {
        Ok(n) => n,
        Err(err) => {
            eprintln!("ERROR: metrics: cannot determine the limit on open file descriptors: {err}");
            return;
        }
    };
    write_gauge_uint64(w, "process_max_fds", max_open_fds);
    write_gauge_uint64(w, "process_open_fds", total_open_fds);
}

fn get_open_fds_count(path: &str) -> Result<u64, String> {
    let dir = std::fs::read_dir(path).map_err(|err| err.to_string())?;
    let mut total_open_fds = 0u64;
    for entry in dir {
        entry.map_err(|err| format!("unexpected error at read_dir: {err}"))?;
        total_open_fds += 1;
    }
    Ok(total_open_fds)
}

fn get_max_files_limit(path: &str) -> Result<u64, String> {
    let data = std::fs::read_to_string(path).map_err(|err| err.to_string())?;
    const PREFIX: &str = "Max open files";
    for s in data.lines() {
        let Some(rest) = s.strip_prefix(PREFIX) else {
            continue;
        };
        let text = rest.trim();
        // Extract the soft limit.
        let Some(n) = text.find(' ') else {
            return Err(format!("cannot extract soft limit from {s:?}"));
        };
        let text = &text[..n];
        if text == "unlimited" {
            return Ok(u64::MAX);
        }
        return text
            .parse()
            .map_err(|err| format!("cannot parse soft limit from {s:?}: {err}"));
    }
    Err("cannot find max open files limit".to_string())
}

/// See <https://man7.org/linux/man-pages/man5/procfs.5.html>.
#[derive(Default, PartialEq, Eq, Debug)]
struct MemStats {
    vm_peak: u64,
    rss_peak: u64,
    rss_anon: u64,
    rss_file: u64,
    rss_shmem: u64,
}

fn write_process_mem_metrics(w: &mut String) {
    let ms = match get_mem_stats("/proc/self/status") {
        Ok(ms) => ms,
        Err(err) => {
            eprintln!("ERROR: metrics: cannot determine memory status: {err}");
            return;
        }
    };
    write_gauge_uint64(w, "process_virtual_memory_peak_bytes", ms.vm_peak);
    write_gauge_uint64(w, "process_resident_memory_peak_bytes", ms.rss_peak);
    write_gauge_uint64(w, "process_resident_memory_anon_bytes", ms.rss_anon);
    write_gauge_uint64(w, "process_resident_memory_file_bytes", ms.rss_file);
    write_gauge_uint64(w, "process_resident_memory_shared_bytes", ms.rss_shmem);
}

fn get_mem_stats(path: &str) -> Result<MemStats, String> {
    let data = std::fs::read_to_string(path).map_err(|err| err.to_string())?;
    let mut ms = MemStats::default();
    for s in data.lines() {
        if !s.starts_with("Vm") && !s.starts_with("Rss") {
            continue;
        }
        // Extract the key and value.
        let line: Vec<&str> = s.split_ascii_whitespace().collect();
        if line.len() != 3 {
            return Err(format!(
                "unexpected number of fields found in {s:?}; got {}; want 3",
                line.len()
            ));
        }
        let mem_stat_name = line[0];
        let mem_stat_value = line[1];
        let value: u64 = mem_stat_value
            .parse()
            .map_err(|err| format!("cannot parse number from {s:?}: {err}"))?;
        if line[2] != "kB" {
            return Err(format!("expecting kB value in {s:?}; got {:?}", line[2]));
        }
        let value = value * 1024;
        match mem_stat_name {
            "VmPeak:" => ms.vm_peak = value,
            "VmHWM:" => ms.rss_peak = value,
            "RssAnon:" => ms.rss_anon = value,
            "RssFile:" => ms.rss_file = value,
            "RssShmem:" => ms.rss_shmem = value,
            _ => {}
        }
    }
    Ok(ms)
}

#[cfg(test)]
mod tests {
    use super::{MemStats, get_max_files_limit, get_mem_stats, get_open_fds_count};

    /// The upstream testdata/ fixtures live next to this module
    /// (src/metrics/testdata/, copied verbatim from the Go repository).
    fn testdata(name: &str) -> String {
        format!("{}/src/metrics/testdata/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    // Port of process_metrics_linux_test.go.
    #[test]
    fn test_get_max_files_limit() {
        let f = |want: u64, path: &str, want_err: bool| match get_max_files_limit(path) {
            Ok(got) => {
                assert!(!want_err, "expecting error for {path:?}");
                assert_eq!(got, want, "unexpected result at get_max_files_limit");
            }
            Err(err) => assert!(want_err, "unexpected error: {err}"),
        };
        f(1024, &testdata("limits"), false);
        f(0, &testdata("bad_path"), true);
        f(0, &testdata("limits_bad"), true);
    }

    #[test]
    fn test_get_open_fds_count() {
        // PORT NOTE: the Go repository commits five empty files under
        // testdata/fd/; the port creates them in a temp dir instead of
        // checking in empty fixtures.
        let fd_dir = std::env::temp_dir().join(format!("esl-metrics-fd-{}", std::process::id()));
        std::fs::create_dir_all(&fd_dir).unwrap();
        for i in 0..5 {
            std::fs::write(fd_dir.join(i.to_string()), "").unwrap();
        }

        let f = |want: u64, path: &str, want_err: bool| match get_open_fds_count(path) {
            Ok(got) => {
                assert!(!want_err, "expecting error for {path:?}");
                assert_eq!(got, want, "unexpected result at get_open_fds_count");
            }
            Err(err) => assert!(want_err, "unexpected error: {err}"),
        };
        f(5, &fd_dir.to_string_lossy(), false);
        f(0, &fd_dir.join("0").to_string_lossy(), true);
        f(0, &testdata("limits"), true);

        let _ = std::fs::remove_dir_all(&fd_dir);
    }

    // PORT NOTE: Rust-only test; upstream has no readPSITotals test at the
    // vendored metrics version (v1.43.2).
    #[test]
    fn test_read_psi_totals() {
        let dir = std::env::temp_dir().join(format!("esl-metrics-psi-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_string_lossy().to_string();

        std::fs::write(
            dir.join("cpu.pressure"),
            "some avg10=0.00 avg60=0.00 avg300=0.00 total=123456\n\
             full avg10=0.00 avg60=0.00 avg300=0.00 total=789\n",
        )
        .unwrap();
        assert_eq!(
            super::read_psi_totals(&dir_s, "cpu.pressure").unwrap(),
            (123456, 789)
        );

        std::fs::write(dir.join("io.pressure"), "some avg10=0.00 total=x\n").unwrap();
        assert!(super::read_psi_totals(&dir_s, "io.pressure").is_err());

        std::fs::write(dir.join("memory.pressure"), "some avg10=0.00\n").unwrap();
        assert!(super::read_psi_totals(&dir_s, "memory.pressure").is_err());

        assert!(super::read_psi_totals(&dir_s, "no.such.file").is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_get_mem_stats() {
        let f = |want: MemStats, path: &str, want_err: bool| match get_mem_stats(path) {
            Ok(got) => {
                assert!(!want_err, "expecting error for {path:?}");
                assert_eq!(got, want, "unexpected result at get_mem_stats");
            }
            Err(err) => assert!(want_err, "unexpected error: {err}"),
        };
        f(
            MemStats {
                vm_peak: 2130489344,
                rss_peak: 200679424,
                rss_anon: 121602048,
                rss_file: 11362304,
                rss_shmem: 0,
            },
            &testdata("status"),
            false,
        );
        f(MemStats::default(), &testdata("status_bad"), true);
    }
}

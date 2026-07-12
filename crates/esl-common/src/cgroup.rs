//! Port of Softalink LLC `lib/cgroup`.

use std::fs;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, Ordering};

/// Returns the number of available CPU cores for the app.
///
/// The number is rounded to the next integer value if fractional number of CPU
/// cores are available.
pub fn available_cpus() -> usize {
    static AVAILABLE_CPUS: OnceLock<usize> = OnceLock::new();
    *AVAILABLE_CPUS.get_or_init(compute_available_cpus)
}

// PORT NOTE: Go's AvailableCPUs returns runtime.GOMAXPROCS(-1), which the
// package init() lowers to the cgroup CPU quota. Rust has no GOMAXPROCS, so
// the same computation happens lazily here and is cached: an explicitly set
// GOMAXPROCS environment variable wins (like in Go), otherwise the CPU count
// is clamped to the cgroup quota. On systems without cgroups (e.g. Windows)
// every cgroup file read fails, the quota stays unset and the plain CPU count
// is returned — exactly like the Go build on windows.
// PORT NOTE: the `process_cpu_cores_available` gauge registered by the Go
// init() is not ported, since the metrics package isn't ported yet.
fn compute_available_cpus() -> usize {
    let num_cpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    if let Ok(v) = std::env::var("GOMAXPROCS") {
        // Do not override explicitly set GOMAXPROCS.
        if let Ok(n) = v.parse::<usize>()
            && n > 0
        {
            return n;
        }
    }
    let cpu_quota = get_cpu_quota();
    if cpu_quota <= 0.0 {
        return num_cpu;
    }
    // Round gomaxprocs to the floor of cpuQuota, since Go runtime doesn't work well
    // with fractional available CPU cores.
    let mut gomaxprocs = cpu_quota as usize;
    if gomaxprocs == 0 {
        gomaxprocs = 1;
    }
    if cpu_quota > gomaxprocs as f64 {
        crate::warnf!(
            "rounding CPU quota {cpu_quota:.1} to {gomaxprocs} CPUs for performance reasons - see https://docs.victoriametrics.com/victoriametrics/bestpractices/#kubernetes"
        );
    }
    if gomaxprocs > num_cpu {
        // There is no sense in setting more GOMAXPROCS than the number of available CPU cores.
        gomaxprocs = num_cpu;
    }
    gomaxprocs
}

static GOGC: AtomicI32 = AtomicI32::new(0);

/// Returns GOGC value for the currently running process.
pub fn get_gogc() -> i32 {
    GOGC.load(Ordering::SeqCst)
}

/// Sets GOGC to the given value unless it is already set via environment variable.
///
/// PORT NOTE: Go tunes its garbage collector via `debug.SetGCPercent`. Rust
/// has no GC, so this only records the value (still honoring the `GOGC`
/// environment variable like Go does) so that `get_gogc()` reports the same
/// number the Go binary would.
pub fn set_gogc(gogc_new: i32) {
    match std::env::var("GOGC") {
        Ok(v) if !v.is_empty() => {
            let n = v.parse::<f64>().unwrap_or(100.0);
            GOGC.store(n as i32, Ordering::SeqCst);
        }
        _ => GOGC.store(gogc_new, Ordering::SeqCst),
    }
}

/// Returns cgroup memory limit.
pub fn get_memory_limit() -> i64 {
    // Try determining the amount of memory inside docker container.
    // See https://stackoverflow.com/questions/42187085/check-mem-limit-within-a-docker-container
    //
    // Read memory limit according to https://unix.stackexchange.com/questions/242718/how-to-find-out-how-much-memory-lxc-container-is-allowed-to-consume
    // This should properly determine the limit inside lxc container.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/84
    if let Ok(n) = get_mem_stat("memory.limit_in_bytes") {
        return n;
    }
    match get_mem_stat_v2("memory.max") {
        Ok(n) if n > 0 => n,
        _ => 0,
    }
}

fn get_mem_stat_v2(stat_name: &str) -> Result<i64, String> {
    // See https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#memory-interface-files
    get_mem_limit_v2("/sys/fs/cgroup", "/proc/self/cgroup", stat_name)
}

fn get_mem_limit_v2(sysfs_prefix: &str, cgroup_path: &str, stat_name: &str) -> Result<i64, String> {
    let mut sub_path = read_cgroup_v2_sub_path(cgroup_path).unwrap_or_else(|_| "/".to_string());
    let mut min_limit: i64 = -1;
    loop {
        // travers sub path hierarchy and use a minimal value for stat
        if let Ok(data) = fs::read_to_string(path_join(&[sysfs_prefix, &sub_path, stat_name])) {
            let s = data.trim();
            if s != "max" {
                let n = s
                    .parse::<i64>()
                    .map_err(|err| format!("cannot parse {stat_name} at {sub_path}: {err}"))?;
                if n > 0 && (min_limit < 0 || n < min_limit) {
                    min_limit = n;
                }
            }
        }
        if sub_path == "/" || sub_path == "." {
            break;
        }
        sub_path = path_dir(&sub_path);
    }
    Ok(min_limit)
}

fn get_mem_stat(stat_name: &str) -> Result<i64, String> {
    get_stat_generic(
        stat_name,
        "/sys/fs/cgroup/memory",
        "/proc/self/cgroup",
        "memory",
    )
}

/// Returns hierarchical memory limit.
/// <https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt>
pub fn get_hierarchical_memory_limit() -> i64 {
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/699
    get_hierarchical_memory_limit_at("/sys/fs/cgroup/memory", "/proc/self/cgroup").unwrap_or(0)
}

fn get_hierarchical_memory_limit_at(sysfs_prefix: &str, cgroup_path: &str) -> Result<i64, String> {
    let data = get_file_contents("memory.stat", sysfs_prefix, cgroup_path, "memory")?;
    let mem_stat = grep_first_match(&data, "hierarchical_memory_limit", 1, " ")?;
    mem_stat.parse::<i64>().map_err(|err| err.to_string())
}

fn get_cpu_quota() -> f64 {
    let cpu_quota = match get_cpu_quota_generic() {
        Ok(q) => q,
        Err(_) => return 0.0,
    };
    if cpu_quota <= 0.0 {
        // The quota isn't set. This may be the case in multilevel containers.
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/685#issuecomment-674423728
        return get_online_cpu_count();
    }
    cpu_quota
}

fn get_cpu_quota_generic() -> Result<f64, String> {
    if let Ok(quota_us) = get_cpu_stat("cpu.cfs_quota_us")
        && let Ok(period_us) = get_cpu_stat("cpu.cfs_period_us")
    {
        return Ok(quota_us as f64 / period_us as f64);
    }
    get_cpu_quota_v2("/sys/fs/cgroup", "/proc/self/cgroup")
}

fn get_cpu_stat(stat_name: &str) -> Result<i64, String> {
    get_stat_generic(stat_name, "/sys/fs/cgroup/cpu", "/proc/self/cgroup", "cpu,")
}

fn get_online_cpu_count() -> f64 {
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/685#issuecomment-674423728
    let Ok(data) = fs::read_to_string("/sys/devices/system/cpu/online") else {
        return -1.0;
    };
    let n = count_cpus(&data) as f64;
    if n <= 0.0 {
        return -1.0;
    }
    n
}

// See https://www.freedesktop.org/software/systemd/man/latest/systemd.slice.html
fn get_cpu_quota_v2(sysfs_prefix: &str, cgroup_path: &str) -> Result<f64, String> {
    let mut sub_path = read_cgroup_v2_sub_path(cgroup_path).unwrap_or_else(|_| "/".to_string());
    let mut min_quota: f64 = -1.0;
    loop {
        // travers sub path hierarchy and use a minimal value for stat
        if let Ok(data) = fs::read_to_string(path_join(&[sysfs_prefix, &sub_path, "cpu.max"])) {
            let quota = parse_cpu_max(data.trim())
                .map_err(|err| format!("cannot parse cpu.max at {sub_path}: {err}"))?;
            if quota > 0.0 && (min_quota < 0.0 || quota < min_quota) {
                min_quota = quota;
            }
        }
        if sub_path == "/" || sub_path == "." {
            break;
        }
        sub_path = path_dir(&sub_path);
    }
    Ok(min_quota)
}

// See https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#cpu
fn parse_cpu_max(data: &str) -> Result<f64, String> {
    let bounds: Vec<&str> = data.split(' ').collect();
    if bounds.len() > 2 {
        return Err(format!(
            "unexpected line format: want 'quota period'; got: {data}"
        ));
    }
    if bounds[0] == "max" {
        return Ok(-1.0);
    }
    let quota = bounds[0]
        .parse::<u64>()
        .map_err(|err| format!("cannot parse quota: {err}"))?;
    // The default is “max 100000”.
    let mut period: u64 = 100_000;
    if bounds.len() == 2 {
        period = bounds[1]
            .parse::<u64>()
            .map_err(|err| format!("cannot parse period: {err}"))?;
        if period == 0 {
            return Err("zero value for period is not allowed".to_string());
        }
    }
    Ok(quota as f64 / period as f64)
}

fn count_cpus(data: &str) -> i32 {
    let data = data.trim();
    let mut n: i32 = 0;
    for s in data.split(',') {
        n += 1;
        if !s.contains('-') {
            if s.parse::<i32>().is_err() {
                return -1;
            }
            continue;
        }
        let bounds: Vec<&str> = s.split('-').collect();
        if bounds.len() != 2 {
            return -1;
        }
        let Ok(start) = bounds[0].parse::<i32>() else {
            return -1;
        };
        let Ok(end) = bounds[1].parse::<i32>() else {
            return -1;
        };
        n += end - start;
    }
    n
}

fn get_stat_generic(
    stat_name: &str,
    sysfs_prefix: &str,
    cgroup_path: &str,
    cgroup_grep_line: &str,
) -> Result<i64, String> {
    let data = get_file_contents(stat_name, sysfs_prefix, cgroup_path, cgroup_grep_line)?;
    let data = data.trim();
    data.parse::<i64>()
        .map_err(|err| format!("cannot parse {cgroup_path:?}: {err}"))
}

fn get_file_contents(
    stat_name: &str,
    sysfs_prefix: &str,
    cgroup_path: &str,
    cgroup_grep_line: &str,
) -> Result<String, String> {
    let file_path = path_join(&[sysfs_prefix, stat_name]);
    if let Ok(data) = fs::read_to_string(&file_path) {
        return Ok(data);
    }
    let cgroup_data = fs::read_to_string(cgroup_path).map_err(|err| err.to_string())?;
    let sub_path = grep_first_match(&cgroup_data, cgroup_grep_line, 2, ":").map_err(|err| {
        format!("cannot find cgroup path for {cgroup_grep_line:?} in {cgroup_path:?}: {err}")
    })?;
    let file_path = path_join(&[sysfs_prefix, &sub_path, stat_name]);
    fs::read_to_string(&file_path).map_err(|err| err.to_string())
}

/// Reads cgroupv2 sub-path, for example `0::/user.slice/user-1000.slice/session-5.scope`.
/// See <https://www.freedesktop.org/software/systemd/man/latest/systemd.slice.html>
/// and <https://docs.oracle.com/en/operating-systems/oracle-linux/9/systemd/SystemdMngCgroupsV2.html#SystemdScopes>
fn read_cgroup_v2_sub_path(cgroup_path: &str) -> Result<String, String> {
    let data = fs::read_to_string(cgroup_path).map_err(|err| err.to_string())?;
    grep_first_match(&data, "", 2, ":")
}

/// Searches match line at data and returns item from it by index with given delimiter.
fn grep_first_match(
    data: &str,
    matches: &str,
    index: usize,
    delimiter: &str,
) -> Result<String, String> {
    for s in data.split('\n') {
        if !s.contains(matches) {
            continue;
        }
        let parts: Vec<&str> = s.split(delimiter).collect();
        if index < parts.len() {
            return Ok(parts[index].trim().to_string());
        }
    }
    Err(format!("cannot find {matches:?} in {data:?}"))
}

// PORT NOTE: Go's `path.Join` treats a "/"-prefixed component as relative to
// the already-joined prefix, while Rust's `Path::join` replaces the base with
// absolute components. These helpers keep the Go `path.Join`/`path.Dir`
// semantics for the cgroup sub-paths (which always start with "/").
fn path_join(parts: &[&str]) -> String {
    let mut s = parts
        .iter()
        .filter(|p| !p.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join("/");
    while s.contains("//") {
        s = s.replace("//", "/");
    }
    s
}

fn path_dir(p: &str) -> String {
    match p.rfind('/') {
        Some(0) => "/".to_string(),
        Some(i) => p[..i].to_string(),
        None => ".".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;

    // PORT NOTE: the Go tests read the checked-in `lib/cgroup/testdata` tree.
    // The Rust port recreates the same tree (same file contents) in a unique
    // temporary directory, so the tests stay self-contained in this module.
    struct TestData {
        root: PathBuf,
    }

    impl TestData {
        fn new() -> TestData {
            static SEQ: AtomicUsize = AtomicUsize::new(0);
            let root = std::env::temp_dir().join(format!(
                "esl-cgroup-testdata-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::SeqCst)
            ));
            let docker = "docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db";
            let self_cgroup = "\
12:perf_event:/docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db
11:rdma:/
10:pids:/docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db
9:freezer:/docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db
8:memory:/docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db
7:devices:/docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db
6:cpuset:/docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db
5:hugetlb:/docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db
4:net_cls,net_prio:/docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db
3:blkio:/docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db
2:cpu,cpuacct:/docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db
1:name=systemd:/docker/74c9abf42b88b9a35b1b56061b08303e56fd1707fe5c5b4df93324dedb36b5db
0::/system.slice/containerd.service";
            let files: &[(String, &str)] = &[
                ("cgroup/cpu.cfs_quota_us".to_string(), "10"),
                ("cgroup/cpu.cfs_period_us".to_string(), "500000"),
                ("cgroup/cpu.max".to_string(), "200000 100000\n"),
                ("cgroup/cpu_unset/cpu.max".to_string(), "max\n"),
                ("cgroup/cpu_onlymax/cpu.max".to_string(), "200000\n"),
                (
                    "cgroup/memory.limit_in_bytes".to_string(),
                    "523372036854771712",
                ),
                ("cgroup/memory.max".to_string(), "523372036854771712\n"),
                (
                    "cgroup/memory.stat".to_string(),
                    "rss 2\nhierarchical_memory_limit 120\nhierarchical_memsw_limit 17\ntotal_cache 18",
                ),
                (format!("{docker}/cpu.cfs_quota_us"), "-1"),
                (format!("{docker}/cpu.cfs_period_us"), "100000"),
                (
                    format!("{docker}/memory.limit_in_bytes"),
                    "9223372036854771712",
                ),
                (
                    format!("{docker}/memory.stat"),
                    "rss 2\nhierarchical_memory_limit 16\nhierarchical_memsw_limit 17\ntotal_cache 18",
                ),
                ("self/cgroup".to_string(), self_cgroup),
                ("self/cgroupv2".to_string(), "0::/"),
                (
                    "self/cgroupv2_slice".to_string(),
                    "0::/esm.slice/esmagent.service",
                ),
                ("v2slice/cpu.max".to_string(), "max 100000"),
                ("v2slice/memory.max".to_string(), "max"),
                ("v2slice/esm.slice/cpu.max".to_string(), "200000 100000"),
                ("v2slice/esm.slice/memory.max".to_string(), "1073741824"),
                (
                    "v2slice/esm.slice/esmagent.service/cpu.max".to_string(),
                    "max 100000",
                ),
                (
                    "v2slice/esm.slice/esmagent.service/memory.max".to_string(),
                    "max",
                ),
            ];
            for (rel, contents) in files {
                let p = root.join(rel);
                fs::create_dir_all(p.parent().unwrap()).unwrap();
                fs::write(&p, contents).unwrap();
            }
            TestData { root }
        }

        fn path(&self, rel: &str) -> String {
            format!("{}/{}", self.root.display(), rel)
        }
    }

    impl Drop for TestData {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn test_count_cpus() {
        let f = |s: &str, n_expected: i32| {
            let n = count_cpus(s);
            assert_eq!(
                n, n_expected,
                "unexpected result from countCPUs({s:?}); got {n}; want {n_expected}"
            );
        };
        f("", -1);
        f("1", 1);
        f("234", 1);
        f("1,2", 2);
        f("0-1", 2);
        f("0-0", 1);
        f("1-2,3,5-9,200-210", 19);
        f("0-3", 4);
        f("0-6", 7);
    }

    #[test]
    fn test_get_cpu_quota_v2() {
        let td = TestData::new();
        let f = |sys_prefix: &str, cgroup_path: &str, expected_cpu: f64| {
            let got = get_cpu_quota_v2(sys_prefix, cgroup_path).unwrap_or_else(|err| {
                panic!(
                    "unexpected error: {err}, sysPrefix: {sys_prefix}, cgroupPath: {cgroup_path}"
                )
            });
            assert_eq!(
                got, expected_cpu,
                "unexpected result from getCPUQuotaV2({sys_prefix}, {cgroup_path}), got {got}, want {expected_cpu}"
            );
        };
        f(&td.path("cgroup"), &td.path("self/cgroupv2"), 2.0);
        f(&td.path("cgroup/cpu_unset"), "", -1.0);
        f(&td.path("cgroup/cpu_onlymax"), "", 2.0);

        // systemd slice
        f(&td.path("v2slice"), &td.path("self/cgroupv2_slice"), 2.0);
    }

    #[test]
    fn test_get_hierarchical_memory_limit_success() {
        let td = TestData::new();
        let f = |sys_path: &str, cgroup_path: &str, want: i64| {
            let got = get_hierarchical_memory_limit_at(sys_path, cgroup_path)
                .unwrap_or_else(|err| panic!("unexpected error: {err}"));
            assert_eq!(got, want, "unexpected result, got: {got}, want {want}");
        };
        f(&td.path(""), &td.path("self/cgroup"), 16);
        f(&td.path("cgroup"), &td.path("self/cgroup"), 120);
    }

    #[test]
    fn test_get_mem_limit_v2() {
        let td = TestData::new();
        let f = |sys_prefix: &str, cgroup_path: &str, want: i64| {
            let got = get_mem_limit_v2(sys_prefix, cgroup_path, "memory.max")
                .unwrap_or_else(|err| panic!("unexpected error: {err}"));
            assert_eq!(got, want, "unexpected result, got: {got}, want {want}");
        };
        f(
            &td.path("cgroup"),
            &td.path("self/cgroupv2"),
            523372036854771712,
        );
        // systemd slice
        f(
            &td.path("v2slice"),
            &td.path("self/cgroupv2_slice"),
            1073741824,
        );
    }

    #[test]
    fn test_get_hierarchical_memory_limit_failure() {
        let td = TestData::new();
        let f = |sys_path: &str, cgroup_path: &str| {
            let got = get_hierarchical_memory_limit_at(sys_path, cgroup_path);
            assert!(got.is_err(), "expecting non-nil error");
        };
        f(&td.path(""), &td.path("none_existing_folder"));
    }

    #[test]
    fn test_get_stat_generic_success() {
        let td = TestData::new();
        let f = |stat_name: &str,
                 sysfs_prefix: &str,
                 cgroup_path: &str,
                 cgroup_grep_line: &str,
                 want: i64| {
            let got = get_stat_generic(stat_name, sysfs_prefix, cgroup_path, cgroup_grep_line)
                .unwrap_or_else(|err| panic!("unexpected error: {err}"));
            assert_eq!(got, want, "unexpected result, got: {got}, want {want}");
        };
        f(
            "cpu.cfs_quota_us",
            &td.path(""),
            &td.path("self/cgroup"),
            "cpu,",
            -1,
        );
        f(
            "cpu.cfs_quota_us",
            &td.path("cgroup"),
            &td.path("self/cgroup"),
            "cpu,",
            10,
        );
        f(
            "cpu.cfs_period_us",
            &td.path(""),
            &td.path("self/cgroup"),
            "cpu,",
            100000,
        );
        f(
            "cpu.cfs_period_us",
            &td.path("cgroup"),
            &td.path("self/cgroup"),
            "cpu,",
            500000,
        );
        f(
            "memory.limit_in_bytes",
            &td.path(""),
            &td.path("self/cgroup"),
            "memory",
            9223372036854771712,
        );
        f(
            "memory.limit_in_bytes",
            &td.path("cgroup"),
            &td.path("self/cgroup"),
            "memory",
            523372036854771712,
        );
        f(
            "memory.max",
            &td.path("cgroup"),
            &td.path("self/cgroupv2"),
            "",
            523372036854771712,
        );
    }

    #[test]
    fn test_get_stat_generic_failure() {
        let td = TestData::new();
        let f = |stat_name: &str, sysfs_prefix: &str, cgroup_path: &str, cgroup_grep_line: &str| {
            let got = get_stat_generic(stat_name, sysfs_prefix, cgroup_path, cgroup_grep_line);
            assert!(got.is_err(), "expecting non-nil error");
        };
        f(
            "cpu.cfs_quota_us",
            &td.path(""),
            &td.path("missing_folder"),
            "cpu,",
        );
        f(
            "cpu.cfs_period_us",
            &td.path(""),
            &td.path("missing_folder"),
            "cpu,",
        );
        f(
            "memory.limit_in_bytes",
            &td.path(""),
            &td.path("none_existing_folder"),
            "memory",
        );
        f(
            "memory.max",
            &td.path(""),
            &td.path("none_existing_folder"),
            "",
        );
    }

    #[test]
    fn test_parse_cpu_max() {
        assert_eq!(parse_cpu_max("max 100000"), Ok(-1.0));
        assert_eq!(parse_cpu_max("200000 100000"), Ok(2.0));
        assert_eq!(parse_cpu_max("200000"), Ok(2.0));
        assert!(parse_cpu_max("1 2 3").is_err());
        assert!(parse_cpu_max("foo").is_err());
        assert!(parse_cpu_max("200000 0").is_err());
    }

    #[test]
    fn test_available_cpus() {
        assert!(available_cpus() >= 1);
    }
}

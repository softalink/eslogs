//! Port of Softalink LLC `lib/appmetrics` (`appmetrics.go`, `osmetrics.go`,
//! `osmetrics_linux.go`, `osmetrics_windows.go`, `osmetrics_other.go`):
//! the `/metrics` page payload composed from the metrics registry
//! ([`crate::metrics`]), process metrics and app-level gauges.
//!
//! PORT NOTE: Go exports every registered command-line flag as
//! `flag{name=..., value=..., is_set=...}` gauges via `flag.VisitAll`; the
//! port exports them as `esm_flag{...}` (rebranded name) through
//! [`crate::flagutil::visit_all_flags`], which covers resolved flags plus
//! explicitly set ones — see its PORT NOTE for the coverage difference.
//!
//! PORT NOTE: `vm_os_info` release detection uses `RtlGetVersion` on Windows
//! upstream; the port reports `os="windows"` without a release to avoid
//! depending on ntdll. On Linux the release comes from `uname(2)` like Go.

use std::fmt::Write;
use std::sync::{LazyLock, Mutex, Once};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::flagutil::Flag;
use crate::metrics::{write_gauge_uint64, write_metadata_if_needed};
use crate::{buildinfo, cgroup, flagutil, memory, metrics};

static EXPOSE_METADATA: Flag<bool> = Flag::new(
    "metrics.exposeMetadata",
    "Whether to expose TYPE and HELP metadata at the /metrics page, which is exposed at -httpListenAddr . \
     The metadata may be needed when the /metrics page is consumed by systems, which require this information. \
     For example, Managed Prometheus in Google Cloud - \
     https://cloud.google.com/stackdriver/docs/managed-prometheus/troubleshooting#missing-metric-type",
    || false,
);

static EXPOSE_METADATA_ONCE: Once = Once::new();

fn init_expose_metadata() {
    metrics::expose_metadata(*EXPOSE_METADATA.get());
}

/// Initializes the app start time used by `esm_app_start_timestamp` and
/// `esm_app_uptime_seconds`.
///
/// PORT NOTE: Go captures the start time at package init; call this early in
/// `main` (the ported httpserver does it when a server starts) so the first
/// `/metrics` scrape doesn't become the start time. It also captures the
/// process-metrics baselines (the PSI snapshot on Linux) which Go takes at
/// package init.
pub fn init_start_time() {
    LazyLock::force(&START_TIME);
    metrics::init_process_metrics();
}

static START_TIME: LazyLock<(Instant, u64)> = LazyLock::new(|| {
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    (Instant::now(), unix)
});

/// Writes all the registered metrics to `w` in Prometheus exposition format
/// (Go `WritePrometheusMetrics`).
///
/// The output is cached for one second to protect against scrape storms.
pub fn write_prometheus_metrics(w: &mut String) {
    EXPOSE_METADATA_ONCE.call_once(init_expose_metadata);

    static CACHE: Mutex<Option<(Instant, String)>> = Mutex::new(None);

    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let stale = match &*cache {
        Some((t, _)) => t.elapsed() > Duration::from_secs(1),
        None => true,
    };
    if stale {
        let mut bb = String::new();
        write_prometheus_metrics_uncached(&mut bb);
        write_os_metrics(&mut bb);
        *cache = Some((Instant::now(), bb));
    }
    if let Some((_, bb)) = &*cache {
        w.push_str(bb);
    }
}

fn write_prometheus_metrics_uncached(w: &mut String) {
    metrics::write_prometheus(w, true);
    metrics::write_fd_metrics(w);

    write_gauge_uint64(
        w,
        &format!(
            "esm_app_version{{version={:?}, short_version={:?}}}",
            buildinfo::version(),
            buildinfo::short_version()
        ),
        1,
    );
    write_gauge_uint64(w, "esm_allowed_memory_bytes", memory::allowed() as u64);
    write_gauge_uint64(
        w,
        "esm_available_memory_bytes",
        (memory::allowed() + memory::remaining()) as u64,
    );
    write_gauge_uint64(
        w,
        "esm_available_cpu_cores",
        cgroup::available_cpus() as u64,
    );
    write_gauge_uint64(w, "esm_gogc", cgroup::get_gogc() as u64);

    // Export the start time and uptime in seconds.
    let (started, start_unix) = *START_TIME;
    write_gauge_uint64(w, "esm_app_start_timestamp", start_unix);
    write_gauge_uint64(w, "esm_app_uptime_seconds", started.elapsed().as_secs());

    // Export flags as metrics (Go iterates `flag.VisitAll`; see the
    // module-level PORT NOTE about the `esm_flag` name and coverage).
    write_metadata_if_needed(w, "esm_flag", "gauge");
    flagutil::visit_all_flags(|name, value, is_set| {
        let lname = name.to_lowercase();
        let value = if flagutil::is_secret_flag(&lname) {
            // Do not expose passwords and keys to prometheus.
            "secret"
        } else {
            value
        };
        let _ = writeln!(
            w,
            "esm_flag{{name={}, value={}, is_set=\"{}\"}} 1",
            flagutil::go_quote(name),
            flagutil::go_quote(value),
            if is_set { "true" } else { "false" }
        );
    });
}

fn write_os_metrics(w: &mut String) {
    let (name, release) = os_info();
    if !name.is_empty() {
        write_gauge_uint64(
            w,
            &format!("esm_os_info{{os={name:?}, release={release:?}}}"),
            1,
        );
    }
}

#[cfg(target_os = "linux")]
fn os_info() -> (&'static str, String) {
    // SAFETY: uname only fills the zero-initialized struct owned by this
    // frame.
    let release = unsafe {
        let mut uts: libc::utsname = std::mem::zeroed();
        if libc::uname(&mut uts) == 0 {
            let bytes: Vec<u8> = uts
                .release
                .iter()
                .take_while(|&&c| c != 0)
                .map(|&c| c as u8)
                .collect();
            String::from_utf8_lossy(&bytes).into_owned()
        } else {
            String::new()
        }
    };
    ("linux", release)
}

#[cfg(windows)]
fn os_info() -> (&'static str, String) {
    ("windows", String::new())
}

#[cfg(not(any(target_os = "linux", windows)))]
fn os_info() -> (&'static str, String) {
    (std::env::consts::OS, String::new())
}

#[cfg(test)]
mod tests {
    use super::write_prometheus_metrics;

    #[test]
    fn test_write_prometheus_metrics() {
        // The expose-metadata Once toggles the global metadata switch; hold
        // the metrics registry lock so parallel metadata tests can't race it.
        let _guard = crate::metrics::testutil::global_registry_lock();
        super::init_start_time();
        // Resolve a flag so the esm_flag export has at least one entry.
        let _ = *super::EXPOSE_METADATA.get();
        let mut bb = String::new();
        write_prometheus_metrics(&mut bb);
        for needle in [
            "esm_app_version{version=",
            "esm_allowed_memory_bytes ",
            "esm_available_memory_bytes ",
            "esm_available_cpu_cores ",
            "esm_app_start_timestamp ",
            "esm_app_uptime_seconds ",
            "esm_flag{name=\"metrics.exposeMetadata\", value=\"false\", is_set=\"false\"} 1",
        ] {
            assert!(bb.contains(needle), "missing {needle:?} in\n{bb}");
        }
        for line in bb.lines() {
            assert!(!line.is_empty(), "unexpected empty line in\n{bb}");
        }
    }
}

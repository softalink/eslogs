//! Port of `github.com/VictoriaMetrics/metrics` (this file:
//! `vendor/github.com/VictoriaMetrics/metrics/metrics.go`).
//!
//! Prometheus-compatible metrics registry: register metrics via the `new_*`
//! / `get_or_create_*` functions, expose them on `/metrics` via
//! [`write_prometheus`] and update them during the application lifetime.
//!
//! PORT NOTE: metrics are written into a `String` (`std::fmt::Write`) instead
//! of Go's `io.Writer`; the `/metrics` handler buffers the response anyway.
//!
//! PORT NOTE: `WriteProcessMetrics` in Go emits `go_*` runtime metrics
//! (`go_metrics.go`) and push-mode metrics (`push.go`); neither has a Rust
//! equivalent (no Go runtime, no push mode ported), so [`write_process_metrics`]
//! emits only the OS-level `process_*` series.
//!
//! PORT NOTE: `prometheus_histogram.go` (`le`-bucket histograms) is not
//! ported — nothing in the EsLogs port uses it.

use std::fmt::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

mod counter;
mod floatcounter;
mod gauge;
mod histogram;
#[cfg(target_os = "linux")]
mod process_metrics_linux;
#[cfg(windows)]
mod process_metrics_windows;
mod set;
mod summary;
mod validator;

pub use counter::Counter;
pub use floatcounter::FloatCounter;
pub use gauge::Gauge;
pub use histogram::Histogram;
pub use set::Set;
pub use summary::Summary;
pub use validator::validate_metric;

/// A callback which appends metrics in Prometheus text exposition format.
pub type MetricsWriter = Arc<dyn Fn(&mut String) + Send + Sync>;

/// The boxed gauge callback accepted by [`new_gauge`] and Set::new_gauge.
pub type GaugeFn = Box<dyn Fn() -> f64 + Send + Sync>;

/// One registered metric of any supported kind.
///
/// PORT NOTE: Go models this as the unexported `metric` interface plus type
/// assertions; the port uses an enum, which makes the `get_or_create_*` kind
/// checks explicit.
#[derive(Clone)]
pub(crate) enum MetricValue {
    Counter(Arc<Counter>),
    FloatCounter(Arc<FloatCounter>),
    Gauge(Arc<Gauge>),
    Histogram(Arc<Histogram>),
    Summary(Arc<Summary>),
    /// Auxiliary `metric{quantile="..."}` series owned by a Summary.
    QuantileValue(Arc<Summary>, usize),
}

impl MetricValue {
    pub(crate) fn marshal_to(&self, prefix: &str, w: &mut String) {
        match self {
            MetricValue::Counter(c) => c.marshal_to(prefix, w),
            MetricValue::FloatCounter(fc) => fc.marshal_to(prefix, w),
            MetricValue::Gauge(g) => g.marshal_to(prefix, w),
            MetricValue::Histogram(h) => h.marshal_to(prefix, w),
            MetricValue::Summary(sm) => sm.marshal_to(prefix, w),
            MetricValue::QuantileValue(sm, idx) => sm.marshal_quantile_value_to(*idx, prefix, w),
        }
    }

    pub(crate) fn metric_type(&self) -> &'static str {
        match self {
            MetricValue::Counter(_) | MetricValue::FloatCounter(_) => "counter",
            MetricValue::Gauge(_) => "gauge",
            // See the comment in histogram.go: the Prometheus data model
            // requires histograms to expose `le` labels, so the vmrange-based
            // histogram is exposed as untyped.
            MetricValue::Histogram(_) => "untyped",
            MetricValue::Summary(_) | MetricValue::QuantileValue(..) => "summary",
        }
    }

    /// The Go type name used in `BUG: metric %q isn't a ...` panics.
    pub(crate) fn kind_name(&self) -> &'static str {
        match self {
            MetricValue::Counter(_) => "Counter",
            MetricValue::FloatCounter(_) => "FloatCounter",
            MetricValue::Gauge(_) => "Gauge",
            MetricValue::Histogram(_) => "Histogram",
            MetricValue::Summary(_) => "Summary",
            MetricValue::QuantileValue(..) => "quantileValue",
        }
    }
}

/// A metric with its registered name (Go `namedMetric`).
#[derive(Clone)]
pub(crate) struct NamedMetric {
    pub(crate) name: Arc<str>,
    pub(crate) metric: MetricValue,
    /// Whether it is an auxiliary metric (only set for the per-quantile
    /// series of a Summary). Affects sorting and forbids direct unregister.
    pub(crate) is_aux: bool,
}

/// Locks a mutex, ignoring poisoning.
///
/// PORT NOTE: the registry panics on programmer errors exactly like Go
/// (double registration, kind mismatch, ...). Go panics don't poison locks;
/// recovering the guard here keeps the registry usable after a caught panic,
/// matching Go's `recover()` semantics.
pub(crate) fn lock_ignore_poison<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

static DEFAULT_SET: LazyLock<Arc<Set>> = LazyLock::new(|| Arc::new(Set::new()));

/// Returns the default metrics set (Go `GetDefaultSet`).
pub fn default_set() -> &'static Arc<Set> {
    &DEFAULT_SET
}

// Go registers defaultSet in init(); the port registers it when the
// registered-sets list is first touched.
static REGISTERED_SETS: LazyLock<Mutex<Vec<Arc<Set>>>> =
    LazyLock::new(|| Mutex::new(vec![Arc::clone(default_set())]));

/// Registers the given set `s` for metrics export via the global
/// [`write_prometheus`] call.
///
/// See also [`unregister_set`].
pub fn register_set(s: Arc<Set>) {
    let mut sets = lock_ignore_poison(&REGISTERED_SETS);
    if !sets.iter().any(|x| Arc::ptr_eq(x, &s)) {
        sets.push(s);
    }
}

/// Stops exporting metrics for the given `s` via the global
/// [`write_prometheus`] call.
///
/// If `destroy_set` is set to true, then `s.unregister_all_metrics()` is
/// called on `s` after unregistering it, so `s` becomes destroyed. Otherwise
/// the `s` can be registered again in the set by passing it to
/// [`register_set`].
pub fn unregister_set(s: &Arc<Set>, destroy_set: bool) {
    {
        let mut sets = lock_ignore_poison(&REGISTERED_SETS);
        sets.retain(|x| !Arc::ptr_eq(x, s));
    }
    if destroy_set {
        s.unregister_all_metrics();
    }
}

/// Registers a `write_metrics` callback for including metrics in the output
/// generated by [`write_prometheus`].
///
/// The callback must write metrics to `w` in Prometheus text exposition
/// format without timestamps and trailing comments. The last line generated
/// by the callback must end with `\n`.
pub fn register_metrics_writer(write_metrics: impl Fn(&mut String) + Send + Sync + 'static) {
    default_set().register_metrics_writer(write_metrics);
}

/// Writes all the metrics in Prometheus format from the default set, all the
/// added sets and metrics writers to `w`.
///
/// Additional sets can be registered via the [`register_set`] call.
/// Additional metric writers can be registered via the
/// [`register_metrics_writer`] call.
///
/// If `expose_process_metrics` is true, then various `process_*` metrics are
/// exposed for the current process.
pub fn write_prometheus(w: &mut String, expose_process_metrics: bool) {
    let mut sets: Vec<Arc<Set>> = lock_ignore_poison(&REGISTERED_SETS).clone();
    sets.sort_by_key(|s| Arc::as_ptr(s) as usize);
    for s in &sets {
        s.write_prometheus(w);
    }
    if expose_process_metrics {
        write_process_metrics(w);
    }
}

/// Writes additional process metrics in Prometheus format to `w`
/// (Go `WriteProcessMetrics`).
///
/// The following `process_*` metrics are exposed for the currently running
/// process:
///
///   - `process_cpu_seconds_system_total` - CPU time spent in syscalls
///   - `process_cpu_seconds_user_total` - CPU time spent in userspace
///   - `process_cpu_seconds_total` - CPU time spent by the process
///   - `process_major_pagefaults_total` - page faults resulted in disk IO
///   - `process_minor_pagefaults_total` - page faults resolved without disk IO
///   - `process_resident_memory_bytes` - recently accessed memory (aka RSS)
///   - `process_resident_memory_peak_bytes` - the maximum RSS memory usage
///   - `process_resident_memory_anon_bytes` - RSS for memory-mapped files
///   - `process_resident_memory_file_bytes` - RSS for memory allocated by the process
///   - `process_resident_memory_shared_bytes` - RSS for memory shared between multiple processes
///   - `process_virtual_memory_bytes` - virtual memory usage
///   - `process_virtual_memory_peak_bytes` - the maximum virtual memory usage
///   - `process_num_threads` - the number of threads
///   - `process_start_time_seconds` - process start time as unix timestamp
///   - `process_io_read_bytes_total` - the number of bytes read via syscalls
///   - `process_io_written_bytes_total` - the number of bytes written via syscalls
///   - `process_io_read_syscalls_total` - the number of read syscalls
///   - `process_io_write_syscalls_total` - the number of write syscalls
///   - `process_io_storage_read_bytes_total` - the number of bytes actually read from disk
///   - `process_io_storage_written_bytes_total` - the number of bytes actually written to disk
///
/// See also [`write_fd_metrics`].
pub fn write_process_metrics(w: &mut String) {
    #[cfg(target_os = "linux")]
    process_metrics_linux::write_process_metrics(w);
    #[cfg(windows)]
    process_metrics_windows::write_process_metrics(w);
    #[cfg(not(any(target_os = "linux", windows)))]
    // PORT NOTE: mirrors process_metrics_other.go — no process metrics on
    // unsupported systems.
    let _ = w;
}

/// Writes `process_max_fds` and `process_open_fds` metrics to `w`.
pub fn write_fd_metrics(w: &mut String) {
    #[cfg(target_os = "linux")]
    process_metrics_linux::write_fd_metrics(w);
    #[cfg(windows)]
    process_metrics_windows::write_fd_metrics(w);
    #[cfg(not(any(target_os = "linux", windows)))]
    let _ = w;
}

/// Removes metric with the given name from the default set.
///
/// See also [`unregister_all_metrics`].
pub fn unregister_metric(name: &str) -> bool {
    default_set().unregister_metric(name)
}

/// Unregisters all the metrics from the default set.
///
/// It also unregisters the callbacks passed to [`register_metrics_writer`].
pub fn unregister_all_metrics() {
    default_set().unregister_all_metrics();
}

/// Returns the sorted list of all the metric names from the default set.
pub fn list_metric_names() -> Vec<String> {
    default_set().list_metric_names()
}

/// Allows enabling adding TYPE and HELP metadata to the exposed metrics
/// globally.
///
/// It is safe to call this method multiple times. It is allowed to change it
/// in runtime. It is set to false by default.
pub fn expose_metadata(v: bool) {
    EXPOSE_METADATA.store(v, Ordering::Relaxed);
}

pub(crate) fn is_metadata_enabled() -> bool {
    EXPOSE_METADATA.load(Ordering::Relaxed)
}

static EXPOSE_METADATA: AtomicBool = AtomicBool::new(false);

/// Writes a gauge metric with the given name and value to `w` in Prometheus
/// text exposition format.
pub fn write_gauge_uint64(w: &mut String, name: &str, value: u64) {
    write_metric_uint64(w, name, "gauge", value);
}

/// Writes a gauge metric with the given name and value to `w` in Prometheus
/// text exposition format.
pub fn write_gauge_float64(w: &mut String, name: &str, value: f64) {
    write_metric_float64(w, name, "gauge", value);
}

/// Writes a counter metric with the given name and value to `w` in Prometheus
/// text exposition format.
pub fn write_counter_uint64(w: &mut String, name: &str, value: u64) {
    write_metric_uint64(w, name, "counter", value);
}

/// Writes a counter metric with the given name and value to `w` in Prometheus
/// text exposition format.
pub fn write_counter_float64(w: &mut String, name: &str, value: f64) {
    write_metric_float64(w, name, "counter", value);
}

fn write_metric_uint64(w: &mut String, metric_name: &str, metric_type: &str, value: u64) {
    write_metadata_if_needed(w, metric_name, metric_type);
    let _ = writeln!(w, "{metric_name} {value}");
}

fn write_metric_float64(w: &mut String, metric_name: &str, metric_type: &str, value: f64) {
    write_metadata_if_needed(w, metric_name, metric_type);
    w.push_str(metric_name);
    w.push(' ');
    write_g(w, value);
    w.push('\n');
}

/// Writes HELP and TYPE metadata for the given `metric_name` and
/// `metric_type` if this is globally enabled via [`expose_metadata`].
///
/// If the metadata exposition isn't enabled, then this function is no-op.
pub fn write_metadata_if_needed(w: &mut String, metric_name: &str, metric_type: &str) {
    if !is_metadata_enabled() {
        return;
    }
    let metric_family = get_metric_family(metric_name);
    write_metadata(w, metric_family, metric_type);
}

pub(crate) fn write_metadata(w: &mut String, metric_family: &str, metric_type: &str) {
    let _ = writeln!(w, "# HELP {metric_family}");
    let _ = writeln!(w, "# TYPE {metric_family} {metric_type}");
}

pub(crate) fn get_metric_family(metric_name: &str) -> &str {
    match metric_name.find('{') {
        Some(n) => &metric_name[..n],
        None => metric_name,
    }
}

/// Appends `v` to `w` the way Go's `%g` verb formats a float64: the shortest
/// decimal representation that round-trips, switching to exponent notation
/// when the decimal exponent is < -4 or >= 21 (Go `strconv.FormatFloat(v,
/// 'g', -1, 64)`).
pub(crate) fn write_g(w: &mut String, v: f64) {
    if v.is_nan() {
        w.push_str("NaN");
        return;
    }
    if v.is_infinite() {
        w.push_str(if v > 0.0 { "+Inf" } else { "-Inf" });
        return;
    }
    // `{:e}` produces the shortest round-trip mantissa, e.g. "1.2345675e6".
    let s = format!("{v:e}");
    let (mantissa, e) = s.split_once('e').expect("`{:e}` always contains 'e'");
    let exp: i32 = e.parse().expect("exponent is a valid integer");
    let neg = mantissa.starts_with('-');
    if neg {
        w.push('-');
    }
    let digits: Vec<u8> = mantissa.bytes().filter(u8::is_ascii_digit).collect();
    if !(-4..21).contains(&exp) {
        // Exponent form: `1.5e+21`, `1e-05`.
        w.push(digits[0] as char);
        if digits.len() > 1 {
            w.push('.');
            for &d in &digits[1..] {
                w.push(d as char);
            }
        }
        let _ = write!(
            w,
            "e{}{:02}",
            if exp < 0 { '-' } else { '+' },
            exp.unsigned_abs()
        );
        return;
    }
    // Fixed form with the decimal point after `exp + 1` digits.
    let decpt = exp + 1;
    if decpt <= 0 {
        w.push_str("0.");
        for _ in 0..-decpt {
            w.push('0');
        }
        for &d in &digits {
            w.push(d as char);
        }
    } else if (decpt as usize) >= digits.len() {
        for &d in &digits {
            w.push(d as char);
        }
        for _ in 0..(decpt as usize - digits.len()) {
            w.push('0');
        }
    } else {
        for &d in &digits[..decpt as usize] {
            w.push(d as char);
        }
        w.push('.');
        for &d in &digits[decpt as usize..] {
            w.push(d as char);
        }
    }
}

/// New* delegating functions on the default set (Go top-level API).
///
/// The name must be a valid Prometheus-compatible metric with possible
/// labels, e.g. `foo`, `foo{bar="baz"}`, `foo{bar="baz",aaa="b"}`.
pub fn new_counter(name: &str) -> Arc<Counter> {
    default_set().new_counter(name)
}

/// Returns the registered counter with the given name or creates a new one.
///
/// Performance tip: prefer [`new_counter`] instead of `get_or_create_counter`.
pub fn get_or_create_counter(name: &str) -> Arc<Counter> {
    default_set().get_or_create_counter(name)
}

/// Registers and returns a new [`FloatCounter`] with the given name.
pub fn new_float_counter(name: &str) -> Arc<FloatCounter> {
    default_set().new_float_counter(name)
}

/// Returns the registered [`FloatCounter`] with the given name or creates a
/// new one.
pub fn get_or_create_float_counter(name: &str) -> Arc<FloatCounter> {
    default_set().get_or_create_float_counter(name)
}

/// Registers and returns a gauge with the given name, which calls `f` to
/// obtain the gauge value.
///
/// If `f` is `None`, then it is expected that the gauge value is changed via
/// `set()`, `inc()`, `dec()` and `add()` calls.
pub fn new_gauge(name: &str, f: Option<GaugeFn>) -> Arc<Gauge> {
    default_set().new_gauge(name, f)
}

/// Returns the registered gauge with the given name or creates a new one.
pub fn get_or_create_gauge(name: &str, f: Option<GaugeFn>) -> Arc<Gauge> {
    default_set().get_or_create_gauge(name, f)
}

/// Creates and returns a new histogram with the given name.
pub fn new_histogram(name: &str) -> Arc<Histogram> {
    default_set().new_histogram(name)
}

/// Returns the registered histogram with the given name or creates a new one.
pub fn get_or_create_histogram(name: &str) -> Arc<Histogram> {
    default_set().get_or_create_histogram(name)
}

/// Creates and returns a new summary with the given name.
pub fn new_summary(name: &str) -> Arc<Summary> {
    default_set().new_summary(name)
}

/// Creates and returns a new summary with the given name, window and
/// quantiles.
pub fn new_summary_ext(name: &str, window: std::time::Duration, quantiles: &[f64]) -> Arc<Summary> {
    default_set().new_summary_ext(name, window, quantiles)
}

/// Returns the registered summary with the given name or creates a new one.
pub fn get_or_create_summary(name: &str) -> Arc<Summary> {
    default_set().get_or_create_summary(name)
}

/// Returns the registered summary with the given name, window and quantiles
/// or creates a new one.
pub fn get_or_create_summary_ext(
    name: &str,
    window: std::time::Duration,
    quantiles: &[f64],
) -> Arc<Summary> {
    default_set().get_or_create_summary_ext(name, window, quantiles)
}

pub(crate) fn add_tag(name: &str, tag: &str) -> String {
    if name.is_empty() || !name.ends_with('}') {
        return format!("{name}{{{tag}}}");
    }
    let name = &name[..name.len() - 1];
    if name.is_empty() {
        panic!("BUG: metric name cannot be empty");
    }
    if name.ends_with('{') {
        // Case for the empty labels set `metric_name{}`.
        return format!("{name}{tag}}}");
    }
    format!("{name},{tag}}}")
}

pub(crate) fn split_metric_name(name: &str) -> (&str, &str) {
    match name.find('{') {
        Some(n) => (&name[..n], &name[n..]),
        None => (name, ""),
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    use std::sync::{Mutex, MutexGuard};

    /// PORT NOTE: Go runs the package tests sequentially in one process, so
    /// they can freely share the global default set. Rust runs tests in
    /// parallel; every test whose assertions depend on the global registry
    /// state takes this lock to restore the sequential semantics.
    static GLOBAL_TEST_LOCK: Mutex<()> = Mutex::new(());

    pub(crate) fn global_registry_lock() -> MutexGuard<'static, ()> {
        GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn expect_panic(context: &str, f: impl FnOnce()) {
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        assert!(res.is_err(), "expecting panic in {context}");
    }

    /// Go `testConcurrent`: runs `f` from 5 concurrent threads.
    ///
    /// PORT NOTE: Go guards against deadlocks with a 5-second timeout; scoped
    /// threads always join, so the port relies on the test harness timeout.
    pub(crate) fn test_concurrent(f: impl Fn() -> Result<(), String> + Sync) {
        const CONCURRENCY: usize = 5;
        std::thread::scope(|scope| {
            let f = &f;
            let mut handles = Vec::with_capacity(CONCURRENCY);
            for _ in 0..CONCURRENCY {
                handles.push(scope.spawn(f));
            }
            for h in handles {
                h.join()
                    .expect("worker panicked")
                    .expect("unexpected error");
            }
        });
    }

    pub(crate) fn test_marshal_to(m: &super::MetricValue, prefix: &str, result_expected: &str) {
        let mut bb = String::new();
        m.marshal_to(prefix, &mut bb);
        assert_eq!(
            bb, result_expected,
            "unexpected marshaled metric;\ngot\n{bb:?}\nwant\n{result_expected:?}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::{expect_panic, global_registry_lock, test_concurrent};
    use super::*;

    // Port of metrics_test.go TestWriteMetrics.
    #[test]
    fn test_write_metrics_gauge_uint64() {
        let _guard = global_registry_lock();
        let mut bb = String::new();
        write_gauge_uint64(&mut bb, "foo", 123);
        assert_eq!(bb, "foo 123\n");

        expose_metadata(true);
        bb.clear();
        write_gauge_uint64(&mut bb, "foo", 123);
        expose_metadata(false);
        assert_eq!(bb, "# HELP foo\n# TYPE foo gauge\nfoo 123\n");
    }

    #[test]
    fn test_write_metrics_gauge_float64() {
        let _guard = global_registry_lock();
        let mut bb = String::new();
        write_gauge_float64(&mut bb, "foo", 1.23);
        assert_eq!(bb, "foo 1.23\n");

        expose_metadata(true);
        bb.clear();
        write_gauge_float64(&mut bb, "foo", 1.23);
        expose_metadata(false);
        assert_eq!(bb, "# HELP foo\n# TYPE foo gauge\nfoo 1.23\n");
    }

    #[test]
    fn test_write_metrics_counter_uint64() {
        let _guard = global_registry_lock();
        let mut bb = String::new();
        write_counter_uint64(&mut bb, "foo_total", 123);
        assert_eq!(bb, "foo_total 123\n");

        expose_metadata(true);
        bb.clear();
        write_counter_uint64(&mut bb, "foo_total", 123);
        expose_metadata(false);
        assert_eq!(
            bb,
            "# HELP foo_total\n# TYPE foo_total counter\nfoo_total 123\n"
        );
    }

    #[test]
    fn test_write_metrics_counter_float64() {
        let _guard = global_registry_lock();
        let mut bb = String::new();
        write_counter_float64(&mut bb, "foo_total", 1.23);
        assert_eq!(bb, "foo_total 1.23\n");

        expose_metadata(true);
        bb.clear();
        write_counter_float64(&mut bb, "foo_total", 1.23);
        expose_metadata(false);
        assert_eq!(
            bb,
            "# HELP foo_total\n# TYPE foo_total counter\nfoo_total 1.23\n"
        );
    }

    #[test]
    fn test_unregister_all_metrics() {
        let _guard = global_registry_lock();
        for j in 0..3 {
            for i in 0..10 {
                let _ = new_counter(&format!("counter_{i}"));
                let _ = new_summary(&format!("summary_{i}"));
                let _ = new_histogram(&format!("histogram_{i}"));
                let _ = new_gauge(&format!("gauge_{i}"), Some(Box::new(|| 0.0)));
            }
            assert!(
                !list_metric_names().is_empty(),
                "unexpected empty list of metrics on iteration {j}"
            );
            unregister_all_metrics();
            let mns = list_metric_names();
            assert!(
                mns.is_empty(),
                "unexpected metric names after unregister_all_metrics call on iteration {j}: {mns:?}"
            );
        }
    }

    #[test]
    fn test_register_metrics_writer() {
        let _guard = global_registry_lock();
        register_metrics_writer(|w| {
            write_counter_uint64(w, r#"counter{label="abc"}"#, 1234);
            write_gauge_float64(w, r#"gauge{a="b",c="d"}"#, -34.43);
        });

        let mut bb = String::new();
        write_prometheus(&mut bb, false);
        let data = bb;

        unregister_all_metrics();

        let expected_line = "counter{label=\"abc\"} 1234\n";
        assert!(
            data.contains(expected_line),
            "missing {expected_line:?} in\n{data}"
        );
        let expected_line = "gauge{a=\"b\",c=\"d\"} -34.43\n";
        assert!(
            data.contains(expected_line),
            "missing {expected_line:?} in\n{data}"
        );
    }

    #[test]
    fn test_register_unregister_set() {
        let _guard = global_registry_lock();
        const METRIC_NAME: &str = "metric_from_set";
        const METRIC_VALUE: u64 = 123;
        let s = Arc::new(Set::new());
        let c = s.new_counter(METRIC_NAME);
        c.set(METRIC_VALUE);

        register_set(Arc::clone(&s));
        let mut bb = String::new();
        write_prometheus(&mut bb, false);
        let expected_line = format!("{METRIC_NAME} {METRIC_VALUE}\n");
        assert!(
            bb.contains(&expected_line),
            "missing {expected_line:?} in\n{bb}"
        );

        unregister_set(&s, true);
        bb.clear();
        write_prometheus(&mut bb, false);
        assert!(
            !bb.contains(&expected_line),
            "unexpected {expected_line:?} in\n{bb}"
        );
    }

    #[test]
    fn test_invalid_name() {
        let f = |name: &str| {
            expect_panic(&format!("new_counter({name:?})"), || {
                new_counter(name);
            });
            expect_panic(&format!("new_gauge({name:?})"), || {
                new_gauge(name, Some(Box::new(|| 0.0)));
            });
            expect_panic(&format!("new_summary({name:?})"), || {
                new_summary(name);
            });
            expect_panic(&format!("get_or_create_counter({name:?})"), || {
                get_or_create_counter(name);
            });
            expect_panic(&format!("get_or_create_gauge({name:?})"), || {
                get_or_create_gauge(name, Some(Box::new(|| 0.0)));
            });
            expect_panic(&format!("get_or_create_summary({name:?})"), || {
                get_or_create_summary(name);
            });
            expect_panic(&format!("get_or_create_histogram({name:?})"), || {
                get_or_create_histogram(name);
            });
        };
        f("");
        f("foo{");
        f("foo}");
        f("foo{bar");
        f("foo{bar=");
        f(r#"foo{bar=""#);
        f(r#"foo{bar="baz"#);
        f(r#"foo{bar="baz""#);
        f(r#"foo{bar="baz","#);
        f(r#"foo{bar="baz",}"#);
    }

    #[test]
    fn test_double_register_new_counter() {
        let name = "NewCounterDoubleRegister";
        new_counter(name);
        expect_panic(name, || {
            new_counter(name);
        });
    }

    #[test]
    fn test_double_register_new_gauge() {
        let name = "NewGaugeDoubleRegister";
        new_gauge(name, Some(Box::new(|| 0.0)));
        expect_panic(name, || {
            new_gauge(name, Some(Box::new(|| 0.0)));
        });
    }

    #[test]
    fn test_double_register_new_summary() {
        let name = "NewSummaryDoubleRegister";
        new_summary(name);
        expect_panic(name, || {
            new_summary(name);
        });
    }

    #[test]
    fn test_double_register_new_histogram() {
        let name = "NewHistogramDoubleRegister";
        new_histogram(name);
        expect_panic(name, || {
            new_summary(name);
        });
    }

    #[test]
    fn test_get_or_create_not_counter() {
        let name = "GetOrCreateNotCounter";
        new_summary(name);
        expect_panic(name, || {
            get_or_create_counter(name);
        });
    }

    #[test]
    fn test_get_or_create_not_gauge() {
        let name = "GetOrCreateNotGauge";
        new_counter(name);
        expect_panic(name, || {
            get_or_create_gauge(name, Some(Box::new(|| 0.0)));
        });
    }

    #[test]
    fn test_get_or_create_not_summary() {
        let name = "GetOrCreateNotSummary";
        new_counter(name);
        expect_panic(name, || {
            get_or_create_summary(name);
        });
    }

    #[test]
    fn test_get_or_create_not_histogram() {
        let name = "GetOrCreateNotHistogram";
        new_counter(name);
        expect_panic(name, || {
            get_or_create_histogram(name);
        });
    }

    fn test_write_prometheus_impl() -> Result<(), String> {
        let mut bb = String::new();
        write_prometheus(&mut bb, false);
        let result_without_process_metrics = bb.clone();
        bb.clear();
        write_prometheus(&mut bb, true);
        let result_with_process_metrics = bb;
        if result_with_process_metrics.len() <= result_without_process_metrics.len() {
            return Err(format!(
                "result with process metrics must contain more data than the result without process metrics; got\n{result_with_process_metrics:?}\nvs\n{result_without_process_metrics:?}"
            ));
        }
        Ok(())
    }

    // PORT NOTE: Go's testWritePrometheus asserts on all platforms because
    // go_* runtime metrics are always emitted; the port only emits OS-level
    // process metrics, so the assertion is gated to the supported systems.
    #[cfg(any(target_os = "linux", windows))]
    #[test]
    fn test_write_prometheus_serial() {
        let _guard = global_registry_lock();
        test_write_prometheus_impl().unwrap();
    }

    #[cfg(any(target_os = "linux", windows))]
    #[test]
    fn test_write_prometheus_concurrent() {
        let _guard = global_registry_lock();
        test_concurrent(test_write_prometheus_impl);
    }

    // Rust-only sanity tests for the Go `%g` formatting helper used by
    // gauges, float counters, histogram sums and summaries.
    #[test]
    fn test_write_g() {
        let f = |v: f64, expected: &str| {
            let mut s = String::new();
            write_g(&mut s, v);
            assert_eq!(s, expected, "unexpected %g formatting of {v}");
        };
        f(0.0, "0");
        f(1.23, "1.23");
        f(-34.43, "-34.43");
        f(123.0, "123");
        f(0.1, "0.1");
        f(0.0001, "0.0001");
        f(0.00001, "1e-05");
        f(1e21, "1e+21");
        f(1.5e21, "1.5e+21");
        f(1e20, "100000000000000000000");
        f(1234567.5, "1234567.5");
        f(f64::NAN, "NaN");
        f(f64::INFINITY, "+Inf");
        f(f64::NEG_INFINITY, "-Inf");
    }
}

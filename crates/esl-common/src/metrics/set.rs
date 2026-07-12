//! Port of `github.com/VictoriaMetrics/metrics/set.go`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::summary::{
    DEFAULT_SUMMARY_QUANTILES, DEFAULT_SUMMARY_WINDOW, is_equal_quantiles, new_summary_internal,
    register_summary, unregister_summary,
};
use super::validator::validate_metric;
use super::{
    Counter, FloatCounter, Gauge, GaugeFn, Histogram, MetricValue, MetricsWriter, NamedMetric,
    Summary, add_tag, get_metric_family, is_metadata_enabled, lock_ignore_poison, write_g,
    write_metadata,
};

/// Set is a set of metrics.
///
/// Metrics belonging to a set are exported separately from global metrics.
///
/// [`Set::write_prometheus`] must be called for exporting metrics from the
/// set.
pub struct Set {
    inner: Mutex<SetInner>,
}

#[derive(Default)]
struct SetInner {
    a: Vec<NamedMetric>,
    m: HashMap<Arc<str>, NamedMetric>,
    summaries: Vec<Arc<Summary>>,

    metrics_writers: Vec<MetricsWriter>,
}

impl Default for Set {
    fn default() -> Self {
        Set::new()
    }
}

impl Set {
    /// Creates a new set of metrics.
    ///
    /// Pass the set to [`super::register_set`] in order to export its metrics
    /// via the global [`super::write_prometheus`] call.
    pub fn new() -> Set {
        Set {
            inner: Mutex::new(SetInner::default()),
        }
    }

    /// Writes all the metrics from the set to `w` in Prometheus format.
    pub fn write_prometheus(&self, w: &mut String) {
        // Collect all the metrics in an in-memory buffer in order to prevent
        // from long locking due to slow w (mirrors the Go code shape; in the
        // port the metrics writers still run outside the lock).
        let (sa, metrics_writers) = {
            let mut inner = lock_ignore_poison(&self.inner);
            for sm in &inner.summaries {
                sm.update_quantiles();
            }
            // The sorting groups metrics of one family together and puts the
            // auxiliary summary quantile series before the summary itself.
            // See https://github.com/VictoriaMetrics/metrics/pull/99#issuecomment-3277072175
            inner.a.sort_by(|i, j| {
                let f_name1 = get_metric_family(&i.name);
                let f_name2 = get_metric_family(&j.name);
                f_name1
                    .cmp(f_name2)
                    .then_with(|| j.is_aux.cmp(&i.is_aux))
                    .then_with(|| i.name.cmp(&j.name))
            });
            (inner.a.clone(), inner.metrics_writers.clone())
        };

        // Call marshal_to without the global lock, since certain metric types
        // such as Gauge can call a callback, which, in turn, can try locking
        // the set again.
        let mut bb = String::new();
        let mut metrics_with_metadata_buf = String::new();
        let mut prev_metric_family = "";
        for nm in &sa {
            if !is_metadata_enabled() {
                nm.metric.marshal_to(&nm.name, &mut bb);
                continue;
            }

            metrics_with_metadata_buf.clear();
            nm.metric
                .marshal_to(&nm.name, &mut metrics_with_metadata_buf);
            if metrics_with_metadata_buf.is_empty() {
                continue;
            }

            let metric_family = get_metric_family(&nm.name);
            if metric_family != prev_metric_family {
                // Write metadata only once per metric family.
                let metric_type = nm.metric.metric_type();
                write_metadata(&mut bb, metric_family, metric_type);
                prev_metric_family = metric_family;
            }
            bb.push_str(&metrics_with_metadata_buf);
        }
        w.push_str(&bb);

        for write_metrics in &metrics_writers {
            write_metrics(w);
        }
    }

    /// Creates and returns a new histogram in the set with the given name.
    ///
    /// The name must be a valid Prometheus-compatible metric with possible
    /// labels, e.g. `foo`, `foo{bar="baz"}`, `foo{bar="baz",aaa="b"}`.
    /// Panics if the name is invalid or already registered.
    pub fn new_histogram(&self, name: &str) -> Arc<Histogram> {
        let h = Arc::new(Histogram::default());
        self.register_metric(name, MetricValue::Histogram(Arc::clone(&h)));
        h
    }

    /// Returns the registered histogram in the set with the given name or
    /// creates a new one.
    ///
    /// Performance tip: prefer [`Set::new_histogram`] instead.
    pub fn get_or_create_histogram(&self, name: &str) -> Arc<Histogram> {
        let nm = self.get_or_register(name, || {
            MetricValue::Histogram(Arc::new(Histogram::default()))
        });
        match nm {
            MetricValue::Histogram(h) => h,
            other => panic!(
                "BUG: metric {name:?} isn't a Histogram. It is {}",
                other.kind_name()
            ),
        }
    }

    /// Registers and returns a new counter with the given name in the set.
    ///
    /// Panics if the name is invalid or already registered.
    pub fn new_counter(&self, name: &str) -> Arc<Counter> {
        let c = Arc::new(Counter::default());
        self.register_metric(name, MetricValue::Counter(Arc::clone(&c)));
        c
    }

    /// Returns the registered counter in the set with the given name or
    /// creates a new one.
    ///
    /// Performance tip: prefer [`Set::new_counter`] instead.
    pub fn get_or_create_counter(&self, name: &str) -> Arc<Counter> {
        let nm = self.get_or_register(name, || MetricValue::Counter(Arc::new(Counter::default())));
        match nm {
            MetricValue::Counter(c) => c,
            other => panic!(
                "BUG: metric {name:?} isn't a Counter. It is {}",
                other.kind_name()
            ),
        }
    }

    /// Registers and returns a new [`FloatCounter`] with the given name in
    /// the set.
    ///
    /// Panics if the name is invalid or already registered.
    pub fn new_float_counter(&self, name: &str) -> Arc<FloatCounter> {
        let fc = Arc::new(FloatCounter::default());
        self.register_metric(name, MetricValue::FloatCounter(Arc::clone(&fc)));
        fc
    }

    /// Returns the registered [`FloatCounter`] in the set with the given name
    /// or creates a new one.
    ///
    /// Performance tip: prefer [`Set::new_float_counter`] instead.
    pub fn get_or_create_float_counter(&self, name: &str) -> Arc<FloatCounter> {
        let nm = self.get_or_register(name, || {
            MetricValue::FloatCounter(Arc::new(FloatCounter::default()))
        });
        match nm {
            MetricValue::FloatCounter(fc) => fc,
            other => panic!(
                "BUG: metric {name:?} isn't a Counter. It is {}",
                other.kind_name()
            ),
        }
    }

    /// Registers and returns a gauge with the given name in the set, which
    /// calls `f` to obtain the gauge value (or stores the value directly when
    /// `f` is `None`).
    ///
    /// Panics if the name is invalid or already registered.
    pub fn new_gauge(&self, name: &str, f: Option<GaugeFn>) -> Arc<Gauge> {
        let g = Arc::new(Gauge::with_callback(f));
        self.register_metric(name, MetricValue::Gauge(Arc::clone(&g)));
        g
    }

    /// Returns the registered gauge with the given name in the set or creates
    /// a new one.
    ///
    /// Performance tip: prefer [`Set::new_gauge`] instead.
    pub fn get_or_create_gauge(&self, name: &str, f: Option<GaugeFn>) -> Arc<Gauge> {
        let mut f = Some(f);
        let nm = self.get_or_register(name, || {
            MetricValue::Gauge(Arc::new(Gauge::with_callback(
                f.take().expect("gauge factory called at most once"),
            )))
        });
        match nm {
            MetricValue::Gauge(g) => g,
            other => panic!(
                "BUG: metric {name:?} isn't a Gauge. It is {}",
                other.kind_name()
            ),
        }
    }

    /// Creates and returns a new summary with the given name in the set.
    ///
    /// Panics if the name is invalid or already registered.
    pub fn new_summary(&self, name: &str) -> Arc<Summary> {
        self.new_summary_ext(name, DEFAULT_SUMMARY_WINDOW, &DEFAULT_SUMMARY_QUANTILES)
    }

    /// Creates and returns a new summary in the set with the given name,
    /// window and quantiles.
    ///
    /// Panics if the name is invalid or already registered.
    pub fn new_summary_ext(&self, name: &str, window: Duration, quantiles: &[f64]) -> Arc<Summary> {
        if let Err(err) = validate_metric(name) {
            panic!("BUG: invalid metric name {name:?}: {err}");
        }
        let sm = new_summary_internal(window, quantiles);

        let mut inner = lock_ignore_poison(&self.inner);
        inner.must_register(name, MetricValue::Summary(Arc::clone(&sm)), false);
        register_summary(&sm);
        inner.register_summary_quantiles(name, &sm);
        inner.summaries.push(Arc::clone(&sm));
        sm
    }

    /// Returns the registered summary with the given name in the set or
    /// creates a new one.
    ///
    /// Performance tip: prefer [`Set::new_summary`] instead.
    pub fn get_or_create_summary(&self, name: &str) -> Arc<Summary> {
        self.get_or_create_summary_ext(name, DEFAULT_SUMMARY_WINDOW, &DEFAULT_SUMMARY_QUANTILES)
    }

    /// Returns the registered summary with the given name, window and
    /// quantiles in the set or creates a new one.
    ///
    /// Performance tip: prefer [`Set::new_summary_ext`] instead.
    pub fn get_or_create_summary_ext(
        &self,
        name: &str,
        window: Duration,
        quantiles: &[f64],
    ) -> Arc<Summary> {
        let nm = {
            let inner = lock_ignore_poison(&self.inner);
            inner.m.get(name).cloned()
        };
        let nm = match nm {
            Some(nm) => nm.metric,
            None => {
                // Slow path - create and register the missing summary.
                if let Err(err) = validate_metric(name) {
                    panic!("BUG: invalid metric name {name:?}: {err}");
                }
                let sm = new_summary_internal(window, quantiles);
                let mut inner = lock_ignore_poison(&self.inner);
                let metric = match inner.m.get(name) {
                    Some(nm) => nm.metric.clone(),
                    None => {
                        let nm = NamedMetric {
                            name: Arc::from(name),
                            metric: MetricValue::Summary(Arc::clone(&sm)),
                            is_aux: false,
                        };
                        inner.m.insert(Arc::clone(&nm.name), nm.clone());
                        inner.a.push(nm.clone());
                        register_summary(&sm);
                        inner.register_summary_quantiles(name, &sm);
                        nm.metric
                    }
                };
                inner.summaries.push(Arc::clone(&sm));
                metric
            }
        };
        let sm = match nm {
            MetricValue::Summary(sm) => sm,
            other => panic!(
                "BUG: metric {name:?} isn't a Summary. It is {}",
                other.kind_name()
            ),
        };
        assert!(
            sm.window == window,
            "BUG: invalid window requested for the summary {name:?}; requested {window:?}; need {:?}",
            sm.window
        );
        assert!(
            is_equal_quantiles(&sm.quantiles, quantiles),
            "BUG: invalid quantiles requested from the summary {name:?}; requested {quantiles:?}; need {:?}",
            sm.quantiles
        );
        sm
    }

    fn register_metric(&self, name: &str, m: MetricValue) {
        if let Err(err) = validate_metric(name) {
            panic!("BUG: invalid metric name {name:?}: {err}");
        }
        let mut inner = lock_ignore_poison(&self.inner);
        inner.must_register(name, m, false);
    }

    /// The common double-checked slow path shared by the `get_or_create_*`
    /// methods (Go duplicates it per type).
    fn get_or_register(&self, name: &str, create: impl FnOnce() -> MetricValue) -> MetricValue {
        {
            let inner = lock_ignore_poison(&self.inner);
            if let Some(nm) = inner.m.get(name) {
                return nm.metric.clone();
            }
        }
        // Slow path - create and register the missing metric.
        if let Err(err) = validate_metric(name) {
            panic!("BUG: invalid metric name {name:?}: {err}");
        }
        let metric = create();
        let mut inner = lock_ignore_poison(&self.inner);
        match inner.m.get(name) {
            Some(nm) => nm.metric.clone(),
            None => {
                let nm = NamedMetric {
                    name: Arc::from(name),
                    metric,
                    is_aux: false,
                };
                inner.m.insert(Arc::clone(&nm.name), nm.clone());
                inner.a.push(nm.clone());
                nm.metric
            }
        }
    }

    /// Removes the metric with the given name from the set.
    ///
    /// True is returned if the metric has been removed.
    /// False is returned if the given metric is missing in the set.
    pub fn unregister_metric(&self, name: &str) -> bool {
        let mut inner = lock_ignore_poison(&self.inner);
        let Some(nm) = inner.m.get(name).cloned() else {
            return false;
        };
        if nm.is_aux {
            // Do not allow deleting auxiliary metrics such as
            // `summary_metric{quantile="..."}`. Such metrics must be deleted
            // via the parent metric name, e.g. `summary_metric`.
            return false;
        }
        inner.unregister_metric_locked(&nm)
    }

    /// De-registers all metrics registered in the set.
    ///
    /// It also de-registers the callbacks passed to
    /// [`Set::register_metrics_writer`].
    pub fn unregister_all_metrics(&self) {
        let metric_names = self.list_metric_names();
        for name in &metric_names {
            self.unregister_metric(name);
        }

        lock_ignore_poison(&self.inner).metrics_writers.clear();
    }

    /// Returns the sorted list of all the metrics in the set.
    ///
    /// The returned list doesn't include metrics generated by the callbacks
    /// passed to [`Set::register_metrics_writer`].
    pub fn list_metric_names(&self) -> Vec<String> {
        let inner = lock_ignore_poison(&self.inner);
        let mut metric_names: Vec<String> = inner
            .m
            .values()
            .filter(|nm| !nm.is_aux)
            .map(|nm| nm.name.to_string())
            .collect();
        metric_names.sort();
        metric_names
    }

    /// Registers a `write_metrics` callback for including metrics in the
    /// output generated by [`Set::write_prometheus`].
    ///
    /// The callback must write metrics to `w` in Prometheus text exposition
    /// format without timestamps and trailing comments. The last line
    /// generated by the callback must end with `\n`.
    ///
    /// It is OK to register multiple callbacks - all of them will be called
    /// sequentially for generating the output.
    pub fn register_metrics_writer(
        &self,
        write_metrics: impl Fn(&mut String) + Send + Sync + 'static,
    ) {
        lock_ignore_poison(&self.inner)
            .metrics_writers
            .push(Arc::new(write_metrics));
    }

    #[cfg(test)]
    pub(crate) fn len_for_tests(&self) -> (usize, usize) {
        let inner = lock_ignore_poison(&self.inner);
        (inner.m.len(), inner.a.len())
    }
}

impl SetInner {
    /// Registers the given metric with the given name.
    ///
    /// Panics if the given name was already registered before.
    fn must_register(&mut self, name: &str, m: MetricValue, is_aux: bool) {
        if self.m.contains_key(name) {
            panic!("BUG: metric {name:?} is already registered");
        }
        let nm = NamedMetric {
            name: Arc::from(name),
            metric: m,
            is_aux,
        };
        self.m.insert(Arc::clone(&nm.name), nm.clone());
        self.a.push(nm);
    }

    fn register_summary_quantiles(&mut self, name: &str, sm: &Arc<Summary>) {
        for (i, q) in sm.quantiles.iter().enumerate() {
            let mut tag = String::from("quantile=\"");
            write_g(&mut tag, *q);
            tag.push('"');
            let quantile_value_name = add_tag(name, &tag);
            self.must_register(
                &quantile_value_name,
                MetricValue::QuantileValue(Arc::clone(sm), i),
                true,
            );
        }
    }

    fn unregister_metric_locked(&mut self, nm: &NamedMetric) -> bool {
        let name = &nm.name;
        self.m.remove(name);

        let delete_from_list = |a: &mut Vec<NamedMetric>, metric_name: &str| match a
            .iter()
            .position(|nm| &*nm.name == metric_name)
        {
            Some(i) => {
                a.remove(i);
            }
            None => {
                panic!("BUG: cannot find metric {metric_name:?} in the list of registered metrics")
            }
        };

        // Remove the metric from the ordered list.
        delete_from_list(&mut self.a, name);

        let MetricValue::Summary(sm) = &nm.metric else {
            // There is no need in cleaning up non-summary metrics.
            return true;
        };

        // Clean up the registry from the per-quantile metrics.
        for q in &sm.quantiles {
            let mut tag = String::from("quantile=\"");
            write_g(&mut tag, *q);
            tag.push('"');
            let quantile_value_name = add_tag(name, &tag);
            self.m.remove(quantile_value_name.as_str());
            delete_from_list(&mut self.a, &quantile_value_name);
        }

        // Remove sm from the list of summaries.
        let len_before = self.summaries.len();
        self.summaries.retain(|xsm| !Arc::ptr_eq(xsm, sm));
        assert!(
            self.summaries.len() < len_before,
            "BUG: cannot find summary {name:?} in the list of registered summaries"
        );
        unregister_summary(sm);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::testutil::test_concurrent;
    use crate::metrics::{
        expose_metadata, get_or_create_counter, get_or_create_gauge, get_or_create_histogram,
        get_or_create_summary, unregister_metric,
    };
    use std::time::Instant;

    // Port of set_test.go.
    #[test]
    fn test_new_set() {
        let mut ss = Vec::new();
        for _ in 0..10 {
            ss.push(Set::new());
        }
        for s in &ss {
            for j in 0..10 {
                let c = s.new_counter(&format!("counter_{j}"));
                c.inc();
                assert_eq!(c.get(), 1, "unexpected counter value");
                let g = s.new_gauge(&format!("gauge_{j}"), Some(Box::new(|| 123.0)));
                assert_eq!(g.get(), 123.0, "unexpected gauge value");
                let _sm = s.new_summary(&format!("summary_{j}"));
                let _h = s.new_histogram(&format!("histogram_{j}"));
            }
        }
    }

    #[test]
    fn test_set_list_metric_names() {
        let s = Set::new();
        let expect = ["cnt1", "cnt2", "cnt3"];
        // Initialize a few counters.
        for n in &expect {
            let c = s.new_counter(n);
            c.inc();
        }

        let list = s.list_metric_names();

        assert_eq!(
            list.len(),
            expect.len(),
            "Metrics count is wrong for listing"
        );
        for e in &expect {
            assert!(
                list.iter().any(|n| n == e),
                "Metric {e} not found in listing"
            );
        }
    }

    #[test]
    fn test_set_unregister_all_metrics() {
        let s = Set::new();
        for j in 0..3 {
            let mut expected_metrics_count = 0;
            for i in 0..10 {
                let _ = s.new_counter(&format!("counter_{i}"));
                let _ = s.new_summary(&format!("summary_{i}"));
                let _ = s.new_histogram(&format!("histogram_{i}"));
                let _ = s.new_gauge(&format!("gauge_{i}"), Some(Box::new(|| 0.0)));
                expected_metrics_count += 4;
            }
            let mns = s.list_metric_names();
            assert_eq!(
                mns.len(),
                expected_metrics_count,
                "unexpected number of metric names on iteration {j};\nmetric names:\n{mns:?}"
            );
            s.unregister_all_metrics();
            let mns = s.list_metric_names();
            assert!(
                mns.is_empty(),
                "unexpected metric names after unregister_all_metrics call on iteration {j}: {mns:?}"
            );
        }
    }

    #[test]
    fn test_set_unregister_metric() {
        let s = Set::new();
        const C_NAME: &str = "counter_1";
        const SM_NAME: &str = "summary_1";
        // Initialize a few metrics.
        let c = s.new_counter(C_NAME);
        c.inc();
        let sm = s.new_summary(SM_NAME);
        sm.update(1.0);

        // Unregister existing metrics.
        assert!(
            s.unregister_metric(C_NAME),
            "unregister_metric({C_NAME}) must return true"
        );
        assert!(
            s.unregister_metric(SM_NAME),
            "unregister_metric({SM_NAME}) must return true"
        );

        // Unregister twice must return false.
        assert!(
            !s.unregister_metric(C_NAME),
            "unregister_metric({C_NAME}) must return false on unregistered metric"
        );
        assert!(
            !s.unregister_metric(SM_NAME),
            "unregister_metric({SM_NAME}) must return false on unregistered metric"
        );

        // Verify that the registry is empty.
        let (m_len, a_len) = s.len_for_tests();
        assert_eq!(m_len, 0, "expected metrics map to be empty");
        assert_eq!(a_len, 0, "expected metrics list to be empty");

        // Validate that the metrics are removed.
        let names = s.list_metric_names();
        assert!(
            !names.iter().any(|n| n == C_NAME || n == SM_NAME),
            "Metric counter_1 and summary_1 must not be listed anymore after unregister"
        );

        // Re-registering with the same names is supposed to be successful.
        s.new_counter(C_NAME).inc();
        s.new_summary(SM_NAME).update(1.0);
    }

    /// Tests concurrent access to metrics during registering and
    /// unregistering.
    #[test]
    fn test_register_unregister() {
        // The workers below share the global default set, so hold the global
        // registry lock for the duration of the test (see testutil).
        let _guard = crate::metrics::testutil::global_registry_lock();
        const WORKERS: usize = 16;
        const ITERATIONS: usize = 1000;
        std::thread::scope(|scope| {
            for _ in 0..WORKERS {
                scope.spawn(|| {
                    let now = Instant::now();
                    for i in 0..ITERATIONS {
                        let iteration = i % 5;
                        let counter = format!(r#"counter{{iteration="{iteration}"}}"#);
                        get_or_create_counter(&counter).add(i as u64);
                        unregister_metric(&counter);

                        let histogram = format!(r#"histogram{{iteration="{iteration}"}}"#);
                        get_or_create_histogram(&histogram).update_duration(now);
                        unregister_metric(&histogram);

                        let gauge = format!(r#"gauge{{iteration="{iteration}"}}"#);
                        get_or_create_gauge(&gauge, Some(Box::new(|| 1.0)));
                        unregister_metric(&gauge);

                        let summary = format!(r#"summary{{iteration="{iteration}"}}"#);
                        get_or_create_summary(&summary).update(i as f64);
                        unregister_metric(&summary);
                    }
                });
            }
        });
    }

    #[test]
    fn test_metadata_for_after_summary_window() {
        let _guard = crate::metrics::testutil::global_registry_lock();
        expose_metadata(true);
        let result = std::panic::catch_unwind(|| {
            let s = Set::new();
            let test_summary_quantiles_noop = [0.5, 0.9, 0.97, 0.99, 1.0];
            let test_window = Duration::from_millis(50);
            s.new_summary_ext(
                "test_summary_expire_quick",
                test_window,
                &test_summary_quantiles_noop,
            )
            .update(1.0);
            std::thread::sleep(4 * test_window);

            let mut bb = String::new();
            s.write_prometheus(&mut bb);

            let expect = "# HELP test_summary_expire_quick\n# TYPE test_summary_expire_quick summary\ntest_summary_expire_quick_sum 1\ntest_summary_expire_quick_count 1\n";
            assert_eq!(bb, expect, "unexpected summary metric names:\n{bb}");
        });
        expose_metadata(false);
        if let Err(e) = result {
            std::panic::resume_unwind(e);
        }
    }

    // Additional exercise of test_concurrent path for sets, mirroring the Go
    // package's use in other tests.
    #[test]
    fn test_set_counter_concurrent() {
        let s = Set::new();
        let c = s.new_counter("counter_concurrent");
        test_concurrent(|| {
            for _ in 0..10 {
                c.inc();
            }
            Ok(())
        });
        assert_eq!(c.get(), 50);
    }
}

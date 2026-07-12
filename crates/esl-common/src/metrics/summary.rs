//! Port of `github.com/VictoriaMetrics/metrics/summary.go`, plus inline
//! ports of its two tiny dependencies: `github.com/valyala/histogram`
//! (the `Fast` sampling histogram used for quantile estimation) and
//! `github.com/valyala/fastrand` (the xorshift RNG seeding the reservoir).

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use super::{lock_ignore_poison, split_metric_name, write_g};

pub(crate) const DEFAULT_SUMMARY_WINDOW: Duration = Duration::from_secs(5 * 60);

pub(crate) const DEFAULT_SUMMARY_QUANTILES: [f64; 5] = [0.5, 0.9, 0.97, 0.99, 1.0];

/// Summary implements a summary with a sliding window: quantiles are
/// estimated over the last `window` of observations.
pub struct Summary {
    inner: Mutex<SummaryInner>,

    pub(crate) quantiles: Vec<f64>,

    pub(crate) window: Duration,
}

struct SummaryInner {
    curr: FastHistogram,
    next: FastHistogram,

    quantile_values: Vec<f64>,

    sum: f64,
    count: u64,
}

pub(crate) fn new_summary_internal(window: Duration, quantiles: &[f64]) -> Arc<Summary> {
    // Copy the quantiles in order to prevent their modification by the caller.
    let quantiles = quantiles.to_vec();
    validate_quantiles(&quantiles);
    Arc::new(Summary {
        inner: Mutex::new(SummaryInner {
            curr: FastHistogram::new(),
            next: FastHistogram::new(),
            quantile_values: vec![0.0; quantiles.len()],
            sum: 0.0,
            count: 0,
        }),
        quantiles,
        window,
    })
}

fn validate_quantiles(quantiles: &[f64]) {
    for q in quantiles {
        assert!(
            (0.0..=1.0).contains(q),
            "BUG: quantile must be in the range [0..1]; got {q}"
        );
    }
}

impl Summary {
    /// Updates the summary with `v`.
    pub fn update(&self, v: f64) {
        let mut sm = lock_ignore_poison(&self.inner);
        sm.curr.update(v);
        sm.next.update(v);
        sm.sum += v;
        sm.count += 1;
    }

    /// Updates the request duration based on the given start time.
    pub fn update_duration(&self, start_time: Instant) {
        let d = start_time.elapsed().as_secs_f64();
        self.update(d);
    }

    pub(crate) fn marshal_to(&self, prefix: &str, w: &mut String) {
        // Marshal only *_sum and *_count values.
        // Quantile values should be already updated by the caller via
        // update_quantiles(); they are marshaled later via
        // marshal_quantile_value_to.
        let (sum, count) = {
            let sm = lock_ignore_poison(&self.inner);
            (sm.sum, sm.count)
        };

        if count > 0 {
            let (name, filters) = split_metric_name(prefix);
            if sum as i64 as f64 == sum {
                // Marshal integer sum without scientific notation.
                let _ = writeln!(w, "{name}_sum{filters} {}", sum as i64);
            } else {
                let _ = write!(w, "{name}_sum{filters} ");
                write_g(w, sum);
                w.push('\n');
            }
            let _ = writeln!(w, "{name}_count{filters} {count}");
        }
    }

    pub(crate) fn update_quantiles(&self) {
        let mut sm = lock_ignore_poison(&self.inner);
        let mut qv = std::mem::take(&mut sm.quantile_values);
        qv.clear();
        sm.curr.quantiles(&mut qv, &self.quantiles);
        sm.quantile_values = qv;
    }

    /// Marshals the auxiliary `metric{quantile="..."}` series (Go
    /// `quantileValue.marshalTo`).
    pub(crate) fn marshal_quantile_value_to(&self, idx: usize, prefix: &str, w: &mut String) {
        let v = lock_ignore_poison(&self.inner).quantile_values[idx];
        if !v.is_nan() {
            w.push_str(prefix);
            w.push(' ');
            write_g(w, v);
            w.push('\n');
        }
    }

    #[cfg(test)]
    pub(crate) fn sum_and_count(&self) -> (f64, u64) {
        let sm = lock_ignore_poison(&self.inner);
        (sm.sum, sm.count)
    }

    #[cfg(test)]
    pub(crate) fn quantile_values(&self) -> Vec<f64> {
        lock_ignore_poison(&self.inner).quantile_values.clone()
    }
}

pub(crate) fn is_equal_quantiles(a: &[f64], b: &[f64]) -> bool {
    // Do not use direct slice equality, since NaN != NaN; Go compares
    // element-wise as well.
    if a.len() != b.len() {
        return false;
    }
    for i in 0..a.len() {
        if a[i] != b[i] {
            return false;
        }
    }
    true
}

/// Registers `sm` for the periodic window swap. Spawns the swap-cron thread
/// for the window on first use (Go `registerSummaryLocked`).
pub(crate) fn register_summary(sm: &Arc<Summary>) {
    let window = sm.window;
    let mut summaries = lock_ignore_poison(&SUMMARIES);
    let sms = summaries.entry(window).or_default();
    sms.push(Arc::clone(sm));
    if sms.len() == 1 {
        std::thread::spawn(move || summaries_swap_cron(window));
    }
}

pub(crate) fn unregister_summary(sm: &Arc<Summary>) {
    let window = sm.window;
    let mut summaries = lock_ignore_poison(&SUMMARIES);
    let sms = summaries.entry(window).or_default();
    let len_before = sms.len();
    sms.retain(|x| !Arc::ptr_eq(x, sm));
    assert!(
        sms.len() < len_before,
        "BUG: cannot find registered summary"
    );
}

fn summaries_swap_cron(window: Duration) {
    loop {
        std::thread::sleep(window / 2);
        let summaries = lock_ignore_poison(&SUMMARIES);
        if let Some(sms) = summaries.get(&window) {
            for sm in sms {
                let mut inner = lock_ignore_poison(&sm.inner);
                let inner = &mut *inner;
                std::mem::swap(&mut inner.curr, &mut inner.next);
                inner.next.reset();
            }
        }
    }
}

static SUMMARIES: LazyLock<Mutex<HashMap<Duration, Vec<Arc<Summary>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const MAX_SAMPLES: usize = 1000;

/// Inline port of `github.com/valyala/histogram` `Fast`: a fast sampling
/// histogram holding up to [`MAX_SAMPLES`] reservoir samples.
///
/// It cannot be used from concurrently running threads without external
/// synchronization.
struct FastHistogram {
    max: f64,
    min: f64,
    count: u64,

    a: Vec<f64>,
    tmp: Vec<f64>,
    rng: Rng,
}

impl FastHistogram {
    fn new() -> FastHistogram {
        let mut f = FastHistogram {
            max: f64::NEG_INFINITY,
            min: f64::INFINITY,
            count: 0,
            a: Vec::new(),
            tmp: Vec::new(),
            rng: Rng { x: 0 },
        };
        f.reset();
        f
    }

    /// Resets the histogram.
    fn reset(&mut self) {
        self.max = f64::NEG_INFINITY;
        self.min = f64::INFINITY;
        self.count = 0;
        self.a.clear();
        self.tmp.clear();
        // Reset the rng state in order to get repeatable results for the
        // same sequence of values passed to update().
        self.rng.seed(1);
    }

    /// Updates the histogram with `v`.
    fn update(&mut self, v: f64) {
        if v > self.max {
            self.max = v;
        }
        if v < self.min {
            self.min = v;
        }

        self.count += 1;
        if self.a.len() < MAX_SAMPLES {
            self.a.push(v);
            return;
        }
        let n = self.rng.uint32n(self.count as u32) as usize;
        if n < self.a.len() {
            self.a[n] = v;
        }
    }

    /// Appends the quantile values for the given `phis` to `dst`.
    fn quantiles(&mut self, dst: &mut Vec<f64>, phis: &[f64]) {
        self.tmp.clear();
        self.tmp.extend_from_slice(&self.a);
        self.tmp.sort_by(f64::total_cmp);
        for &phi in phis {
            let q = quantile_sorted(&self.tmp, self.min, self.max, phi);
            dst.push(q);
        }
    }
}

fn quantile_sorted(tmp: &[f64], min: f64, max: f64, phi: f64) -> f64 {
    if tmp.is_empty() || phi.is_nan() {
        return f64::NAN;
    }
    if phi <= 0.0 {
        return min;
    }
    if phi >= 1.0 {
        return max;
    }
    let mut idx = (phi * (tmp.len() - 1) as f64 + 0.5) as usize;
    if idx >= tmp.len() {
        idx = tmp.len() - 1;
    }
    tmp[idx]
}

/// Inline port of `github.com/valyala/fastrand` `RNG`: an xorshift
/// pseudorandom number generator.
struct Rng {
    x: u32,
}

impl Rng {
    fn uint32(&mut self) -> u32 {
        while self.x == 0 {
            self.x = get_random_u32();
        }

        // See https://en.wikipedia.org/wiki/Xorshift
        let mut x = self.x;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.x = x;
        x
    }

    fn uint32n(&mut self, max_n: u32) -> u32 {
        let x = self.uint32();
        // See http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
        ((u64::from(x) * u64::from(max_n)) >> 32) as u32
    }

    fn seed(&mut self, n: u32) {
        self.x = n;
    }
}

fn get_random_u32() -> u32 {
    let x = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    ((x >> 32) ^ x) as u32
}

#[cfg(test)]
mod tests {
    use crate::metrics::testutil::{
        expect_panic, global_registry_lock, test_concurrent, test_marshal_to,
    };
    use crate::metrics::{
        MetricValue, get_or_create_summary, get_or_create_summary_ext, new_summary,
        new_summary_ext, write_prometheus,
    };
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::{DEFAULT_SUMMARY_QUANTILES, DEFAULT_SUMMARY_WINDOW};

    // Port of summary_test.go.
    #[test]
    fn test_summary_serial() {
        let _guard = global_registry_lock();
        let name = "TestSummarySerial";
        let s = new_summary(name);

        // Verify that the summary isn't visible in the output of
        // write_prometheus, since it doesn't contain any values yet.
        let mut bb = String::new();
        write_prometheus(&mut bb, false);
        assert!(
            !bb.contains(name),
            "summary {name} shouldn't be visible in the write_prometheus output; got\n{bb}"
        );

        // Write data to summary.
        for i in 0..2000 {
            s.update(f64::from(i));
            let t = Instant::now();
            s.update_duration(t - Duration::from_millis(i as u64));
        }

        // Make sure the summary prints <prefix>_sum and <prefix>_count on
        // marshal_to call.
        let (sum, count) = s.sum_and_count();
        let mut sum_str = String::new();
        crate::metrics::write_g(&mut sum_str, sum);
        let sm = MetricValue::Summary(Arc::clone(&s));
        test_marshal_to(
            &sm,
            "prefix",
            &format!("prefix_sum {sum_str}\nprefix_count {count}\n"),
        );
        test_marshal_to(
            &sm,
            r#"m{foo="bar"}"#,
            &format!("m_sum{{foo=\"bar\"}} {sum_str}\nm_count{{foo=\"bar\"}} {count}\n"),
        );

        // Verify quantile_values.
        s.update_quantiles();
        let qv = s.quantile_values();
        assert_eq!(qv[qv.len() - 1], 1999.0, "unexpected quantile_values[last]");

        // Make sure the summary becomes visible in the output of
        // write_prometheus, since now it contains values.
        bb.clear();
        write_prometheus(&mut bb, false);
        assert!(
            bb.contains(name),
            "missing summary {name} in the write_prometheus output; got\n{bb}"
        );
    }

    #[test]
    fn test_summary_concurrent() {
        let name = "SummaryConcurrent";
        let s = new_summary(name);
        test_concurrent(|| {
            for i in 0..10 {
                s.update(f64::from(i));
            }
            Ok(())
        });
        test_marshal_to(
            &MetricValue::Summary(s),
            "prefix",
            "prefix_sum 225\nprefix_count 50\n",
        );
    }

    #[test]
    fn test_summary_with_tags() {
        let _guard = global_registry_lock();
        let name = r#"TestSummary{tag="foo"}"#;
        let s = new_summary(name);
        s.update(123.0);

        let mut bb = String::new();
        write_prometheus(&mut bb, false);
        let name_prefix_with_tag = r#"TestSummary{tag="foo",quantile=""#;
        assert!(
            bb.contains(name_prefix_with_tag),
            "missing summary prefix {name_prefix_with_tag} in the write_prometheus output; got\n{bb}"
        );
    }

    #[test]
    fn test_summary_with_empty_tags() {
        let _guard = global_registry_lock();
        let name = "TestSummary2{}";
        let s = new_summary(name);
        s.update(123.0);

        let mut bb = String::new();
        write_prometheus(&mut bb, false);
        let name_prefix_with_tag = r#"TestSummary2{quantile=""#;
        assert!(
            bb.contains(name_prefix_with_tag),
            "missing summary prefix {name_prefix_with_tag} in the write_prometheus output; got\n{bb}"
        );
    }

    #[test]
    fn test_summary_invalid_quantiles() {
        let name = "SummaryInvalidQuantiles";
        expect_panic(name, || {
            new_summary_ext(name, Duration::from_secs(60), &[123.0, -234.0]);
        });
    }

    #[test]
    fn test_summary_small_window() {
        let _guard = global_registry_lock();
        let name = "SummarySmallWindow";
        let window = Duration::from_millis(20);
        let quantiles = [0.1, 0.2, 0.3];
        let s = new_summary_ext(name, window, &quantiles);
        for _ in 0..2000 {
            s.update(123.0);
        }
        // Wait for the window update and verify that the summary has been
        // cleared.
        std::thread::sleep(2 * window);
        let mut bb = String::new();
        write_prometheus(&mut bb, false);
        // <name>_sum and <name>_count are present in the output.
        // Only <name>{quantile} shouldn't be present.
        let name = format!("{name}{{");
        assert!(
            !bb.contains(&name),
            "summary {name} cannot be present in the write_prometheus output; got\n{bb}"
        );
    }

    #[test]
    fn test_get_or_create_summary_invalid_window() {
        let name = "GetOrCreateSummaryInvalidWindow";
        get_or_create_summary_ext(name, DEFAULT_SUMMARY_WINDOW, &DEFAULT_SUMMARY_QUANTILES);
        expect_panic(name, || {
            get_or_create_summary_ext(name, DEFAULT_SUMMARY_WINDOW / 2, &DEFAULT_SUMMARY_QUANTILES);
        });
    }

    #[test]
    fn test_get_or_create_summary_invalid_quantiles() {
        let name = "GetOrCreateSummaryInvalidQuantiles";
        get_or_create_summary_ext(name, DEFAULT_SUMMARY_WINDOW, &DEFAULT_SUMMARY_QUANTILES);
        expect_panic(name, || {
            get_or_create_summary_ext(name, DEFAULT_SUMMARY_WINDOW, &[0.1, 0.2]);
        });
        let mut quantiles = DEFAULT_SUMMARY_QUANTILES.to_vec();
        let last = quantiles.len() - 1;
        quantiles[last] /= 2.0;
        expect_panic(name, || {
            get_or_create_summary_ext(name, DEFAULT_SUMMARY_WINDOW, &quantiles);
        });
    }

    #[test]
    fn test_get_or_create_summary_serial() {
        let name = "GetOrCreateSummarySerial";
        test_get_or_create_summary(name).unwrap();
    }

    #[test]
    fn test_get_or_create_summary_concurrent() {
        let name = "GetOrCreateSummaryConcurrent";
        test_concurrent(|| test_get_or_create_summary(name));
    }

    fn test_get_or_create_summary(name: &str) -> Result<(), String> {
        let s1 = get_or_create_summary(name);
        for _ in 0..10 {
            let s2 = get_or_create_summary(name);
            if !Arc::ptr_eq(&s1, &s2) {
                return Err("unexpected summary returned".to_string());
            }
        }
        Ok(())
    }
}

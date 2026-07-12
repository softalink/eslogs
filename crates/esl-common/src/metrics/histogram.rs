//! Port of `github.com/VictoriaMetrics/metrics/histogram.go`.

use std::fmt::Write;
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

use super::{add_tag, lock_ignore_poison, split_metric_name, write_g};

const E10_MIN: i32 = -9;
const E10_MAX: i32 = 18;
pub(crate) const BUCKETS_PER_DECIMAL: usize = 18;
const DECIMAL_BUCKETS_COUNT: usize = (E10_MAX - E10_MIN) as usize;
pub(crate) const BUCKETS_COUNT: usize = DECIMAL_BUCKETS_COUNT * BUCKETS_PER_DECIMAL;

pub(crate) static BUCKET_MULTIPLIER: LazyLock<f64> =
    LazyLock::new(|| 10f64.powf(1.0 / BUCKETS_PER_DECIMAL as f64));

/// Histogram is a histogram for non-negative values with automatically
/// created buckets.
///
/// See <https://medium.com/@valyala/improving-histogram-usability-for-prometheus-and-grafana-bc7e5df0e350>
///
/// Each bucket contains a counter for values in the given range. Each
/// non-empty bucket is exposed via the following metric:
///
/// ```text
/// <metric_name>_bucket{<optional_tags>,vmrange="<start>...<end>"} <counter>
/// ```
///
/// Where:
///
///   - `<metric_name>` is the metric name passed to `new_histogram`
///   - `<optional_tags>` is optional tags for the `<metric_name>`
///   - `<start>` and `<end>` - start and end values for the given bucket
///   - `<counter>` - the number of hits to the given bucket during `update`
///     calls
///
/// Histogram buckets can be converted to Prometheus-like buckets with `le`
/// labels with the `prometheus_buckets(<metric_name>_bucket)` function from
/// MetricsQL.
///
/// A zero histogram is usable.
#[derive(Default)]
pub struct Histogram {
    // The mutex guarantees a synchronous update for all the counters and sum.
    inner: Mutex<HistogramInner>,
}

struct HistogramInner {
    /// Counters for histogram buckets, allocated per decimal on first use.
    decimal_buckets: [Option<Box<[u64; BUCKETS_PER_DECIMAL]>>; DECIMAL_BUCKETS_COUNT],

    /// The number of values which hit the lower bucket.
    lower: u64,

    /// The number of values which hit the upper bucket.
    upper: u64,

    /// The sum of all the values put into the histogram.
    sum: f64,
}

impl Default for HistogramInner {
    fn default() -> Self {
        HistogramInner {
            decimal_buckets: std::array::from_fn(|_| None),
            lower: 0,
            upper: 0,
            sum: 0.0,
        }
    }
}

impl Histogram {
    /// Resets the given histogram.
    pub fn reset(&self) {
        let mut h = lock_ignore_poison(&self.inner);
        for db in h.decimal_buckets.iter_mut().flatten() {
            db.fill(0);
        }
        h.lower = 0;
        h.upper = 0;
        h.sum = 0.0;
    }

    /// Updates the histogram with `v`.
    ///
    /// Negative values and NaNs are ignored.
    pub fn update(&self, v: f64) {
        if v.is_nan() || v < 0.0 {
            // Skip NaNs and negative values.
            return;
        }
        let bucket_idx = (v.log10() - f64::from(E10_MIN)) * BUCKETS_PER_DECIMAL as f64;
        let mut h = lock_ignore_poison(&self.inner);
        h.sum += v;
        if bucket_idx < 0.0 {
            h.lower += 1;
        } else if bucket_idx >= BUCKETS_COUNT as f64 {
            h.upper += 1;
        } else {
            let mut idx = bucket_idx as usize;
            if bucket_idx == idx as f64 && idx > 0 {
                // Edge case for 10^n values, which must go to the lower
                // bucket according to the Prometheus logic for `le`-based
                // histograms.
                idx -= 1;
            }
            let decimal_bucket_idx = idx / BUCKETS_PER_DECIMAL;
            let offset = idx % BUCKETS_PER_DECIMAL;
            let db = h.decimal_buckets[decimal_bucket_idx]
                .get_or_insert_with(|| Box::new([0u64; BUCKETS_PER_DECIMAL]));
            db[offset] += 1;
        }
    }

    /// Merges `src` into the histogram.
    pub fn merge(&self, src: &Histogram) {
        let mut h = lock_ignore_poison(&self.inner);
        let src = lock_ignore_poison(&src.inner);

        h.lower += src.lower;
        h.upper += src.upper;
        h.sum += src.sum;

        for (i, db_src) in src.decimal_buckets.iter().enumerate() {
            let Some(db_src) = db_src else {
                continue;
            };
            let db_dst =
                h.decimal_buckets[i].get_or_insert_with(|| Box::new([0u64; BUCKETS_PER_DECIMAL]));
            for (j, v) in db_src.iter().enumerate() {
                db_dst[j] += v;
            }
        }
    }

    /// Calls `f` for all buckets with non-zero counters.
    ///
    /// `vmrange` contains a `"<start>...<end>"` string with the bucket
    /// bounds. The lower bound isn't included in the bucket, while the upper
    /// bound is included. This is required to be compatible with
    /// Prometheus-style histogram buckets with `le` (less or equal) labels.
    pub fn visit_non_zero_buckets(&self, mut f: impl FnMut(&str, u64)) {
        let h = lock_ignore_poison(&self.inner);
        if h.lower > 0 {
            f(LOWER_BUCKET_RANGE.as_str(), h.lower);
        }
        for (decimal_bucket_idx, db) in h.decimal_buckets.iter().enumerate() {
            let Some(db) = db else {
                continue;
            };
            for (offset, &count) in db.iter().enumerate() {
                if count > 0 {
                    let bucket_idx = decimal_bucket_idx * BUCKETS_PER_DECIMAL + offset;
                    f(get_vmrange(bucket_idx), count);
                }
            }
        }
        if h.upper > 0 {
            f(UPPER_BUCKET_RANGE.as_str(), h.upper);
        }
    }

    /// Updates the request duration based on the given start time.
    pub fn update_duration(&self, start_time: Instant) {
        let d = start_time.elapsed().as_secs_f64();
        self.update(d);
    }

    fn get_sum(&self) -> f64 {
        lock_ignore_poison(&self.inner).sum
    }

    pub(crate) fn marshal_to(&self, prefix: &str, w: &mut String) {
        let mut count_total = 0u64;
        self.visit_non_zero_buckets(|vmrange, count| {
            let tag = format!("vmrange={vmrange:?}");
            let metric_name = add_tag(prefix, &tag);
            let (name, labels) = split_metric_name(&metric_name);
            let _ = writeln!(w, "{name}_bucket{labels} {count}");
            count_total += count;
        });
        if count_total == 0 {
            return;
        }
        let (name, labels) = split_metric_name(prefix);
        let sum = self.get_sum();
        if sum as i64 as f64 == sum {
            let _ = writeln!(w, "{name}_sum{labels} {}", sum as i64);
        } else {
            let _ = write!(w, "{name}_sum{labels} ");
            write_g(w, sum);
            w.push('\n');
        }
        let _ = writeln!(w, "{name}_count{labels} {count_total}");
    }
}

pub(crate) fn get_vmrange(bucket_idx: usize) -> &'static str {
    &BUCKET_RANGES[bucket_idx]
}

/// Formats `v` the way Go's `%.3e` verb does, e.g. `8.799e+01`: a mantissa
/// with three decimals plus a sign-prefixed exponent of at least two digits.
fn format_e3(v: f64) -> String {
    let s = format!("{v:.3e}");
    let (mantissa, e) = s.split_once('e').expect("`{:e}` always contains 'e'");
    let exp: i32 = e.parse().expect("exponent is a valid integer");
    format!(
        "{mantissa}e{}{:02}",
        if exp < 0 { '-' } else { '+' },
        exp.unsigned_abs()
    )
}

static LOWER_BUCKET_RANGE: LazyLock<String> =
    LazyLock::new(|| format!("0...{}", format_e3(10f64.powi(E10_MIN))));
static UPPER_BUCKET_RANGE: LazyLock<String> =
    LazyLock::new(|| format!("{}...+Inf", format_e3(10f64.powi(E10_MAX))));

static BUCKET_RANGES: LazyLock<Vec<String>> = LazyLock::new(|| {
    let mut ranges = Vec::with_capacity(BUCKETS_COUNT);
    let mut v = 10f64.powi(E10_MIN);
    let mut start = format_e3(v);
    for _ in 0..BUCKETS_COUNT {
        v *= *BUCKET_MULTIPLIER;
        let end = format_e3(v);
        ranges.push(format!("{start}...{end}"));
        start = end;
    }
    ranges
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::testutil::{global_registry_lock, test_concurrent, test_marshal_to};
    use crate::metrics::{MetricValue, get_or_create_histogram, new_histogram, write_prometheus};
    use std::sync::Arc;
    use std::time::Duration;

    // Port of histogram_test.go.
    #[test]
    fn test_histogram_merge() {
        let name = "TestHistogramMerge";
        let h = new_histogram(name);
        // Write data to histogram.
        for i in 98..218 {
            h.update(f64::from(i));
        }

        let b = new_histogram("test");
        for i in 98..218 {
            b.update(f64::from(i));
        }

        h.merge(&b);

        // Make sure the histogram prints <prefix>_bucket on marshal_to call.
        test_marshal_to(
            &MetricValue::Histogram(h),
            "prefix",
            r#"prefix_bucket{vmrange="8.799e+01...1.000e+02"} 6
prefix_bucket{vmrange="1.000e+02...1.136e+02"} 26
prefix_bucket{vmrange="1.136e+02...1.292e+02"} 32
prefix_bucket{vmrange="1.292e+02...1.468e+02"} 34
prefix_bucket{vmrange="1.468e+02...1.668e+02"} 40
prefix_bucket{vmrange="1.668e+02...1.896e+02"} 46
prefix_bucket{vmrange="1.896e+02...2.154e+02"} 52
prefix_bucket{vmrange="2.154e+02...2.448e+02"} 4
prefix_sum 37800
prefix_count 240
"#,
        );
    }

    #[test]
    fn test_get_vmrange() {
        let f = |bucket_idx: usize, vmrange_expected: &str| {
            let vmrange = get_vmrange(bucket_idx);
            assert_eq!(
                vmrange, vmrange_expected,
                "unexpected vmrange for bucket_idx={bucket_idx}"
            );
        };
        f(0, "1.000e-09...1.136e-09");
        f(1, "1.136e-09...1.292e-09");
        f(BUCKETS_PER_DECIMAL - 1, "8.799e-09...1.000e-08");
        f(BUCKETS_PER_DECIMAL, "1.000e-08...1.136e-08");
        f(
            BUCKETS_PER_DECIMAL * (-E10_MIN) as usize - 1,
            "8.799e-01...1.000e+00",
        );
        f(
            BUCKETS_PER_DECIMAL * (-E10_MIN) as usize,
            "1.000e+00...1.136e+00",
        );
        f(
            BUCKETS_PER_DECIMAL * (E10_MAX - E10_MIN) as usize - 1,
            "8.799e+17...1.000e+18",
        );
    }

    #[test]
    fn test_histogram_serial() {
        let _guard = global_registry_lock();
        let name = "TestHistogramSerial";
        let h = new_histogram(name);

        // Verify that the histogram is invisible in the output of
        // write_prometheus when it has no data.
        let mut bb = String::new();
        write_prometheus(&mut bb, false);
        assert!(
            !bb.contains(name),
            "histogram {name} shouldn't be visible in the write_prometheus output; got\n{bb}"
        );

        // Write data to histogram.
        for i in 98..218 {
            h.update(f64::from(i));
        }

        // Make sure the histogram prints <prefix>_bucket on marshal_to call.
        let hm = MetricValue::Histogram(Arc::clone(&h));
        test_marshal_to(
            &hm,
            "prefix",
            r#"prefix_bucket{vmrange="8.799e+01...1.000e+02"} 3
prefix_bucket{vmrange="1.000e+02...1.136e+02"} 13
prefix_bucket{vmrange="1.136e+02...1.292e+02"} 16
prefix_bucket{vmrange="1.292e+02...1.468e+02"} 17
prefix_bucket{vmrange="1.468e+02...1.668e+02"} 20
prefix_bucket{vmrange="1.668e+02...1.896e+02"} 23
prefix_bucket{vmrange="1.896e+02...2.154e+02"} 26
prefix_bucket{vmrange="2.154e+02...2.448e+02"} 2
prefix_sum 18900
prefix_count 120
"#,
        );
        test_marshal_to(
            &hm,
            "\t  m{foo=\"bar\"}",
            "\t  m_bucket{foo=\"bar\",vmrange=\"8.799e+01...1.000e+02\"} 3
\t  m_bucket{foo=\"bar\",vmrange=\"1.000e+02...1.136e+02\"} 13
\t  m_bucket{foo=\"bar\",vmrange=\"1.136e+02...1.292e+02\"} 16
\t  m_bucket{foo=\"bar\",vmrange=\"1.292e+02...1.468e+02\"} 17
\t  m_bucket{foo=\"bar\",vmrange=\"1.468e+02...1.668e+02\"} 20
\t  m_bucket{foo=\"bar\",vmrange=\"1.668e+02...1.896e+02\"} 23
\t  m_bucket{foo=\"bar\",vmrange=\"1.896e+02...2.154e+02\"} 26
\t  m_bucket{foo=\"bar\",vmrange=\"2.154e+02...2.448e+02\"} 2
\t  m_sum{foo=\"bar\"} 18900
\t  m_count{foo=\"bar\"} 120
",
        );

        // Verify reset.
        h.reset();
        bb.clear();
        write_prometheus(&mut bb, false);
        assert!(
            !bb.contains(name),
            "unexpected histogram {name} in the write_prometheus output; got\n{bb}"
        );

        // Verify supported ranges.
        for e10 in -100..100 {
            for offset in 0..BUCKETS_PER_DECIMAL {
                let m = 1.0 + BUCKET_MULTIPLIER.powf(offset as f64);
                let f1 = m * 10f64.powi(e10);
                h.update(f1);
                let f2 = (m + 0.5 * *BUCKET_MULTIPLIER) * 10f64.powi(e10);
                h.update(f2);
                let f3 = (m + 2.0 * *BUCKET_MULTIPLIER) * 10f64.powi(e10);
                h.update(f3);
            }
        }
        h.update_duration(Instant::now() - Duration::from_secs(60));

        // Verify edge cases.
        h.update(0.0);
        h.update(f64::INFINITY);
        h.update(f64::NEG_INFINITY);
        h.update(f64::NAN);
        h.update(-123.0);
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1096
        h.update(f64::from_bits(0x3e112e0be826d695));

        // Make sure the histogram becomes visible in the output of
        // write_prometheus, since now it contains values.
        bb.clear();
        write_prometheus(&mut bb, false);
        assert!(
            bb.contains(name),
            "missing histogram {name} in the write_prometheus output; got\n{bb}"
        );
    }

    #[test]
    fn test_histogram_concurrent() {
        let name = "HistogramConcurrent";
        let h = new_histogram(name);
        test_concurrent(|| {
            let mut f = 0.6;
            while f < 1.4 {
                h.update(f);
                f += 0.1;
            }
            Ok(())
        });
        test_marshal_to(
            &MetricValue::Histogram(Arc::clone(&h)),
            "prefix",
            r#"prefix_bucket{vmrange="5.995e-01...6.813e-01"} 5
prefix_bucket{vmrange="6.813e-01...7.743e-01"} 5
prefix_bucket{vmrange="7.743e-01...8.799e-01"} 5
prefix_bucket{vmrange="8.799e-01...1.000e+00"} 10
prefix_bucket{vmrange="1.000e+00...1.136e+00"} 5
prefix_bucket{vmrange="1.136e+00...1.292e+00"} 5
prefix_bucket{vmrange="1.292e+00...1.468e+00"} 5
prefix_sum 38
prefix_count 40
"#,
        );

        let mut labels = Vec::new();
        let mut counts = Vec::new();
        h.visit_non_zero_buckets(|label, count| {
            labels.push(label.to_string());
            counts.push(count);
        });
        let labels_expected = [
            "5.995e-01...6.813e-01",
            "6.813e-01...7.743e-01",
            "7.743e-01...8.799e-01",
            "8.799e-01...1.000e+00",
            "1.000e+00...1.136e+00",
            "1.136e+00...1.292e+00",
            "1.292e+00...1.468e+00",
        ];
        assert_eq!(labels, labels_expected, "unexpected labels");
        let counts_expected = [5u64, 5, 5, 10, 5, 5, 5];
        assert_eq!(counts, counts_expected, "unexpected counts");
    }

    #[test]
    fn test_histogram_with_tags() {
        let _guard = global_registry_lock();
        let name = r#"TestHistogram{tag="foo"}"#;
        let h = new_histogram(name);
        h.update(123.0);

        let mut bb = String::new();
        write_prometheus(&mut bb, false);
        let name_prefix_with_tag =
            "TestHistogram_bucket{tag=\"foo\",vmrange=\"1.136e+02...1.292e+02\"} 1\n";
        assert!(
            bb.contains(name_prefix_with_tag),
            "missing histogram {name_prefix_with_tag} in the write_prometheus output; got\n{bb}"
        );
    }

    #[test]
    fn test_histogram_with_empty_tags() {
        let _guard = global_registry_lock();
        let name = "TestHistogram2{}";
        let h = new_histogram(name);
        h.update(123.0);

        let mut bb = String::new();
        write_prometheus(&mut bb, false);
        let name_prefix_with_tag = "TestHistogram2_bucket{vmrange=\"1.136e+02...1.292e+02\"} 1\n";
        assert!(
            bb.contains(name_prefix_with_tag),
            "missing histogram {name_prefix_with_tag} in the write_prometheus output; got\n{bb}"
        );
    }

    #[test]
    fn test_get_or_create_histogram_serial() {
        let name = "GetOrCreateHistogramSerial";
        test_get_or_create_histogram(name).unwrap();
    }

    #[test]
    fn test_get_or_create_histogram_concurrent() {
        let name = "GetOrCreateHistogramConcurrent";
        test_concurrent(|| test_get_or_create_histogram(name));
    }

    fn test_get_or_create_histogram(name: &str) -> Result<(), String> {
        let h1 = get_or_create_histogram(name);
        for _ in 0..10 {
            let h2 = get_or_create_histogram(name);
            if !Arc::ptr_eq(&h1, &h2) {
                return Err("unexpected histogram returned".to_string());
            }
        }
        Ok(())
    }
}

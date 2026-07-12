//! Port of `lib/logstorage/stats_histogram.go` — the `histogram(field)` stats
//! function, which buckets numeric field values into Softalink LLC histogram
//! buckets.
//!
//! PORT NOTE — allocator / captured config: see `crate::stats` and
//! `crate::stats_json_values`. The processor captures `field_name` at
//! `new_stats_processor` time instead of downcasting the `StatsFunc` per call.
//!
//! PORT NOTE — histogram: `metrics.Histogram`
//! (`vendor/github.com/VictoriaMetrics/metrics/histogram.go`) is ported inline
//! here as [`Histogram`] since only this stats function needs it. Only the
//! `Update`/`Merge`/`Reset`/`VisitNonZeroBuckets` surface and the exact bucket
//! layout (`getVMRange`) are ported — the metrics-registry and marshaling parts
//! are dropped.
//!
//! PORT NOTE — `parseStatsHistogram` is deferred until the parser (`lexer`) is
//! ported; [`StatsHistogram::new`] is exposed for the future parser and tests.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;
use esl_common::stringsutil::less_natural;

use crate::block_result::BlockResult;
use crate::prefix_filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::values_encoder::{
    ValueType, marshal_uint64_string, try_parse_bytes, try_parse_duration, try_parse_float64,
    unmarshal_float64, unmarshal_int64, unmarshal_uint8, unmarshal_uint16, unmarshal_uint32,
    unmarshal_uint64,
};

/// The `histogram(field)` stats function.
///
/// Port of Go's `statsHistogram`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StatsHistogram {
    pub(crate) field_name: String,
}

impl StatsHistogram {
    /// PORT NOTE: replaces the parser-driven `parseStatsHistogram`
    /// constructor (deferred). Exposed for the future parser and tests.
    pub(crate) fn new(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
        }
    }
}

impl StatsFunc for StatsHistogram {
    fn to_string(&self) -> String {
        format!(
            "histogram({})",
            crate::stream_filter::quote_token_if_needed(&self.field_name)
        )
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter(&self.field_name);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsHistogramProcessor {
            field_name: self.field_name.clone(),
            ..Default::default()
        })
    }
}

/// Port of Go's `statsHistogramProcessor`.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct StatsHistogramProcessor {
    h: Histogram,

    /// `buckets_map` is populated only by `import_state`; it holds additional
    /// state for `h`.
    buckets_map: Option<HashMap<String, u64>>,

    // Captured config (see module docs).
    field_name: String,
}

impl StatsHistogramProcessor {
    fn get_complete_buckets_map(&self) -> HashMap<String, u64> {
        let mut m = self.buckets_map.clone().unwrap_or_default();
        self.h.visit_non_zero_buckets(|vmrange, count| {
            *m.entry(vmrange.to_string()).or_insert(0) += count;
        });
        m
    }
}

impl StatsProcessor for StatsHistogramProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        let c = br.get_column_by_name(&self.field_name);

        if br.column_is_const(c) {
            let v = br.column_get_value_at_row(c, 0).to_string();
            if let Some(f) = try_parse_number(&v) {
                for _ in 0..br.rows_len() {
                    self.h.update(f);
                }
            }
            return 0;
        }

        let vt = br.column_value_type(c);
        if vt == ValueType::UINT8 {
            let values = br.column_get_values_encoded(c).unwrap_or(&[]);
            for v in values {
                self.h.update(unmarshal_uint8(v) as f64);
            }
        } else if vt == ValueType::UINT16 {
            let values = br.column_get_values_encoded(c).unwrap_or(&[]);
            for v in values {
                self.h.update(unmarshal_uint16(v) as f64);
            }
        } else if vt == ValueType::UINT32 {
            let values = br.column_get_values_encoded(c).unwrap_or(&[]);
            for v in values {
                self.h.update(unmarshal_uint32(v) as f64);
            }
        } else if vt == ValueType::UINT64 {
            let values = br.column_get_values_encoded(c).unwrap_or(&[]);
            for v in values {
                self.h.update(unmarshal_uint64(v) as f64);
            }
        } else if vt == ValueType::INT64 {
            let values = br.column_get_values_encoded(c).unwrap_or(&[]);
            for v in values {
                self.h.update(unmarshal_int64(v) as f64);
            }
        } else if vt == ValueType::FLOAT64 {
            let values = br.column_get_values_encoded(c).unwrap_or(&[]);
            for v in values {
                self.h.update(unmarshal_float64(v));
            }
        } else if vt == ValueType::IPV4 || vt == ValueType::TIMESTAMP_ISO8601 {
            // skip ipv4/iso8601 values, since they cannot be represented as numbers
        } else {
            let values: Vec<String> = br
                .column_get_values(c)
                .iter()
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .collect();
            for v in &values {
                if let Some(f) = try_parse_number(v) {
                    self.h.update(f);
                }
            }
        }

        0
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        let c = br.get_column_by_name(&self.field_name);

        if br.column_is_const(c) {
            let v = br.column_get_value_at_row(c, 0).to_string();
            if let Some(f) = try_parse_number(&v) {
                self.h.update(f);
            }
            return 0;
        }

        let vt = br.column_value_type(c);
        if vt == ValueType::UINT8 {
            let v = br.column_get_values_encoded(c).unwrap_or(&[])[row_index].clone();
            self.h.update(unmarshal_uint8(&v) as f64);
        } else if vt == ValueType::UINT16 {
            let v = br.column_get_values_encoded(c).unwrap_or(&[])[row_index].clone();
            self.h.update(unmarshal_uint16(&v) as f64);
        } else if vt == ValueType::UINT32 {
            let v = br.column_get_values_encoded(c).unwrap_or(&[])[row_index].clone();
            self.h.update(unmarshal_uint32(&v) as f64);
        } else if vt == ValueType::UINT64 {
            let v = br.column_get_values_encoded(c).unwrap_or(&[])[row_index].clone();
            self.h.update(unmarshal_uint64(&v) as f64);
        } else if vt == ValueType::INT64 {
            let v = br.column_get_values_encoded(c).unwrap_or(&[])[row_index].clone();
            self.h.update(unmarshal_int64(&v) as f64);
        } else if vt == ValueType::FLOAT64 {
            let v = br.column_get_values_encoded(c).unwrap_or(&[])[row_index].clone();
            self.h.update(unmarshal_float64(&v));
        } else if vt == ValueType::IPV4 || vt == ValueType::TIMESTAMP_ISO8601 {
            // skip ipv4/iso8601 values, since they cannot be represented as numbers
        } else {
            let v = br.column_get_value_at_row(c, row_index).to_string();
            if let Some(f) = try_parse_number(&v) {
                self.h.update(f);
            }
        }

        0
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsHistogramProcessor>()
            .expect("merge_state: other must be a StatsHistogramProcessor");
        self.h.merge(&src.h);

        if let Some(sm) = &src.buckets_map {
            for (vmrange, count) in sm {
                let dst = self.buckets_map.get_or_insert_with(HashMap::new);
                *dst.entry(vmrange.clone()).or_insert(0) += count;
            }
        }
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let m = self.get_complete_buckets_map();

        encoding::marshal_var_uint64(dst, m.len() as u64);
        for (vmrange, count) in &m {
            encoding::marshal_bytes(dst, vmrange.as_bytes());
            encoding::marshal_var_uint64(dst, *count);
        }
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        self.h.reset();

        let (buckets_len, n) = encoding::unmarshal_var_uint64(src);
        if n <= 0 {
            return Err("cannot unmarshal bucketsLen".to_string());
        }
        let mut src = &src[n as usize..];

        let mut state_size_increase = 0i64;
        let mut m = HashMap::with_capacity(buckets_len as usize);
        for _ in 0..buckets_len {
            let (v, n) = encoding::unmarshal_bytes(src);
            let v = match v {
                Some(v) if n > 0 => v,
                _ => return Err("cannot unmarshal vmrange".to_string()),
            };
            let vmrange = String::from_utf8_lossy(v).into_owned();
            src = &src[n as usize..];

            let (count, n) = encoding::unmarshal_var_uint64(src);
            if n <= 0 {
                return Err("cannot unmarshal bucket count".to_string());
            }
            src = &src[n as usize..];

            state_size_increase += std::mem::size_of::<String>() as i64
                + vmrange.len() as i64
                + std::mem::size_of::<u64>() as i64;
            m.insert(vmrange, count);
        }
        if !src.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left after decoding histogram; len(tail)={}",
                src.len()
            ));
        }

        self.buckets_map = if m.is_empty() { None } else { Some(m) };

        Ok(state_size_increase)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let m = self.get_complete_buckets_map();

        if m.is_empty() {
            dst.extend_from_slice(b"[]");
            return;
        }

        let mut vmranges: Vec<&String> = m.keys().collect();
        vmranges.sort_by(|a, b| {
            if less_natural(a, b) {
                Ordering::Less
            } else if less_natural(b, a) {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });

        dst.push(b'[');
        for vmrange in &vmranges {
            dst.extend_from_slice(b"{\"vmrange\":\"");
            dst.extend_from_slice(vmrange.as_bytes());
            dst.extend_from_slice(b"\",\"hits\":");
            marshal_uint64_string(dst, m[*vmrange]);
            dst.extend_from_slice(b"},");
        }
        dst.pop(); // trailing ','
        dst.push(b']');
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// PORT NOTE: duplicates `block_result.rs`'s private `try_parse_number` (the
/// same acknowledged duplication as `filter_range.rs`), until it is promoted to
/// a shared `pub(crate)` location.
fn try_parse_number(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    if let Some(f) = try_parse_float64(s) {
        return Some(f);
    }
    if let Some(nsecs) = try_parse_duration(s) {
        return Some(nsecs as f64);
    }
    if let Some(bytes) = try_parse_bytes(s) {
        return Some(bytes as f64);
    }
    if is_likely_number(s) {
        if let Ok(f) = s.parse::<f64>() {
            return Some(f);
        }
        if let Some(n) = parse_int_go(s) {
            return Some(n as f64);
        }
    }
    None
}

fn parse_int_go(s: &str) -> Option<i64> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (radix, digits) =
        if let Some(h) = body.strip_prefix("0x").or_else(|| body.strip_prefix("0X")) {
            (16, h)
        } else if let Some(o) = body.strip_prefix("0o").or_else(|| body.strip_prefix("0O")) {
            (8, o)
        } else if let Some(b) = body.strip_prefix("0b").or_else(|| body.strip_prefix("0B")) {
            (2, b)
        } else {
            (10, body)
        };
    let digits = digits.replace('_', "");
    let n = i64::from_str_radix(&digits, radix).ok()?;
    Some(if neg { -n } else { n })
}

fn is_likely_number(s: &str) -> bool {
    let b = s.as_bytes();
    if b.is_empty() {
        return false;
    }
    let c = b[0];
    if !c.is_ascii_digit() && c != b'-' && c != b'+' && c != b'.' {
        return false;
    }
    if s.matches('.').count() > 1 {
        return false;
    }
    if s.contains(':') || s.matches('-').count() > 2 {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// metrics.Histogram port (see module docs).
// ---------------------------------------------------------------------------

const E10_MIN: i32 = -9;
const E10_MAX: i32 = 18;
const BUCKETS_PER_DECIMAL: usize = 18;
const DECIMAL_BUCKETS_COUNT: usize = (E10_MAX - E10_MIN) as usize;
const BUCKETS_COUNT: usize = DECIMAL_BUCKETS_COUNT * BUCKETS_PER_DECIMAL;

fn bucket_multiplier() -> f64 {
    10f64.powf(1.0 / BUCKETS_PER_DECIMAL as f64)
}

/// Port of Softalink LLC `metrics.Histogram` (numeric surface only).
///
/// PORT NOTE: Go's `decimalBuckets [27]*[18]uint64` is represented as a
/// `Vec<Option<Vec<u64>>>` of length 27. `Reset` zeroes allocated buckets in
/// place (leaving them allocated), exactly like Go — so `PartialEq` between a
/// never-touched and a reset-after-use histogram differs, matching Go's
/// `reflect.DeepEqual` (nil pointer vs pointer-to-zero-array).
#[derive(Clone, Debug, PartialEq)]
struct Histogram {
    decimal_buckets: Vec<Option<Vec<u64>>>,
    lower: u64,
    upper: u64,
    sum: f64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            decimal_buckets: (0..DECIMAL_BUCKETS_COUNT).map(|_| None).collect(),
            lower: 0,
            upper: 0,
            sum: 0.0,
        }
    }
}

impl Histogram {
    fn reset(&mut self) {
        for b in self.decimal_buckets.iter_mut().flatten() {
            for x in b.iter_mut() {
                *x = 0;
            }
        }
        self.lower = 0;
        self.upper = 0;
        self.sum = 0.0;
    }

    /// Updates the histogram with `v`. Negative values and NaNs are ignored.
    fn update(&mut self, v: f64) {
        if v.is_nan() || v < 0.0 {
            return;
        }
        let bucket_idx = (v.log10() - E10_MIN as f64) * BUCKETS_PER_DECIMAL as f64;
        self.sum += v;
        if bucket_idx < 0.0 {
            self.lower += 1;
        } else if bucket_idx >= BUCKETS_COUNT as f64 {
            self.upper += 1;
        } else {
            let mut idx = bucket_idx as usize;
            if bucket_idx == idx as f64 && idx > 0 {
                // Edge case for 10^n values, which must go to the lower bucket.
                idx -= 1;
            }
            let decimal_bucket_idx = idx / BUCKETS_PER_DECIMAL;
            let offset = idx % BUCKETS_PER_DECIMAL;
            let db = self.decimal_buckets[decimal_bucket_idx]
                .get_or_insert_with(|| vec![0u64; BUCKETS_PER_DECIMAL]);
            db[offset] += 1;
        }
    }

    fn merge(&mut self, src: &Histogram) {
        self.lower += src.lower;
        self.upper += src.upper;
        self.sum += src.sum;

        for (i, db_src) in src.decimal_buckets.iter().enumerate() {
            if let Some(b_src) = db_src {
                let b_dst =
                    self.decimal_buckets[i].get_or_insert_with(|| vec![0u64; BUCKETS_PER_DECIMAL]);
                for j in 0..BUCKETS_PER_DECIMAL {
                    b_dst[j] += b_src[j];
                }
            }
        }
    }

    /// Calls `f` for all buckets with non-zero counters. The lower bound isn't
    /// included in the bucket, the upper bound is.
    fn visit_non_zero_buckets(&self, mut f: impl FnMut(&str, u64)) {
        if self.lower > 0 {
            f(lower_bucket_range(), self.lower);
        }
        for (decimal_bucket_idx, db) in self.decimal_buckets.iter().enumerate() {
            if let Some(b) = db {
                for (offset, &count) in b.iter().enumerate() {
                    if count > 0 {
                        let bucket_idx = decimal_bucket_idx * BUCKETS_PER_DECIMAL + offset;
                        f(get_vmrange(bucket_idx), count);
                    }
                }
            }
        }
        if self.upper > 0 {
            f(upper_bucket_range(), self.upper);
        }
    }
}

static BUCKET_RANGES: OnceLock<Vec<String>> = OnceLock::new();
static LOWER_BUCKET_RANGE: OnceLock<String> = OnceLock::new();
static UPPER_BUCKET_RANGE: OnceLock<String> = OnceLock::new();

fn get_vmrange(bucket_idx: usize) -> &'static str {
    BUCKET_RANGES.get_or_init(init_bucket_ranges)[bucket_idx].as_str()
}

fn init_bucket_ranges() -> Vec<String> {
    let mut ranges = Vec::with_capacity(BUCKETS_COUNT);
    let mut v = 10f64.powi(E10_MIN);
    let mut start = format_e3(v);
    for _ in 0..BUCKETS_COUNT {
        v *= bucket_multiplier();
        let end = format_e3(v);
        ranges.push(format!("{start}...{end}"));
        start = end;
    }
    ranges
}

fn lower_bucket_range() -> &'static str {
    LOWER_BUCKET_RANGE
        .get_or_init(|| format!("0...{}", format_e3(10f64.powi(E10_MIN))))
        .as_str()
}

fn upper_bucket_range() -> &'static str {
    UPPER_BUCKET_RANGE
        .get_or_init(|| format!("{}...+Inf", format_e3(10f64.powi(E10_MAX))))
        .as_str()
}

/// Formats `v` like Go's `fmt.Sprintf("%.3e", v)`: a 3-fractional-digit
/// mantissa, `e`, an explicit sign, and a zero-padded (min 2 digit) exponent.
fn format_e3(v: f64) -> String {
    let s = format!("{v:.3e}");
    let (mantissa, exp) = s.split_once('e').expect("scientific format has 'e'");
    let exp: i32 = exp.parse().expect("valid exponent");
    let sign = if exp < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exp.abs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    // Port of TestStatsHistogram (bucketing), exercised directly on the
    // processor (the parser/pipe wiring is deferred).
    #[test]
    fn test_stats_histogram() {
        let rows = vec![
            vec![field("_msg", "abc"), field("a", "2"), field("b", "3")],
            vec![field("_msg", "def"), field("a", "1.9")],
            vec![field("a", "3.05"), field("b", "54")],
        ];

        let sh = StatsHistogram::new("a");
        let mut shp = sh.new_stats_processor();

        let mut br = BlockResult::default();
        br.must_init_from_rows(&rows);
        shp.update_stats_for_all_rows(&sh, &mut br);

        let mut dst = Vec::new();
        shp.finalize_stats(&sh, &mut dst, None);
        assert_eq!(
            String::from_utf8(dst).unwrap(),
            r#"[{"vmrange":"1.896e+00...2.154e+00","hits":2},{"vmrange":"2.783e+00...3.162e+00","hits":1}]"#
        );
    }

    // Port of TestStatsHistogram_ExportImportState.
    #[test]
    fn test_stats_histogram_export_import_state() {
        fn check(shp: &StatsHistogramProcessor, data_len_expected: usize) {
            let mut data = Vec::new();
            shp.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected, "unexpected len(data)");

            let mut shp2 = StatsHistogramProcessor::default();
            shp2.import_state(&data, None).unwrap();
            assert_eq!(shp, &shp2, "unexpected state imported");
        }

        // Zero state
        let shp = StatsHistogramProcessor::default();
        check(&shp, 1);

        // Non-zero state
        let mut buckets_map = HashMap::new();
        buckets_map.insert("1.896e+00...2.154e+00".to_string(), 2344u64);
        buckets_map.insert("2.783e+00...3.162e+00".to_string(), 3289u64);
        let shp = StatsHistogramProcessor {
            buckets_map: Some(buckets_map),
            ..Default::default()
        };
        check(&shp, 49);
    }
}

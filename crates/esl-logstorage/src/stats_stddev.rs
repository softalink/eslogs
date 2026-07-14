//! Port of `stats_stddev.go`: the `stddev(fields...)` stats function.
//!
//! Uses Welford's online algorithm (as in Go / ClickHouse). See
//! [`crate::stats_min`] for the config-capture PORT NOTE.
//!
//! Output format: Go writes the result with
//! `strconv.AppendFloat(dst, stddev, 'f', -1, 64)` (shortest round-trip,
//! never exponential). Rust's f64 `Display` produces digit-identical output
//! for all finite values (verified against Go across extreme magnitudes,
//! e.g. 1e±300); non-finite values are spelled via
//! [`marshal_float64_string`] ("NaN"/"+Inf"/"-Inf", matching Go).

use std::any::Any;
use std::sync::atomic::AtomicBool;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_min::{field_names_string, get_matching_columns};
use crate::values_encoder::{marshal_float64, marshal_float64_string, unmarshal_float64};

/// Port of `statsStddev`.
pub(crate) struct StatsStddev {
    field_filters: Vec<String>,
}

/// Port of `parseStatsStddev`. Empty filters default to `["*"]`.
pub(crate) fn new_stats_stddev(mut field_filters: Vec<String>) -> StatsStddev {
    if field_filters.is_empty() {
        field_filters.push("*".to_string());
    }
    StatsStddev { field_filters }
}

impl StatsFunc for StatsStddev {
    fn to_string(&self) -> String {
        format!("stddev({})", field_names_string(&self.field_filters))
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsStddevProcessor {
            field_filters: self.field_filters.clone(),
            avg: 0.0,
            q: 0.0,
            count: 0.0,
        })
    }
}

/// Port of `statsStddevProcessor`.
pub(crate) struct StatsStddevProcessor {
    field_filters: Vec<String>,
    avg: f64,
    q: f64,
    count: f64,
}

impl StatsStddevProcessor {
    fn update_state(&mut self, f: f64) {
        let delta = f - self.avg;
        let count_new = self.count + 1.0;
        let avg_new = self.avg + delta / count_new;
        let q_new = self.q + delta * (f - avg_new);
        self.avg = avg_new;
        self.q = q_new;
        self.count = count_new;
    }
}

impl StatsProcessor for StatsStddevProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        let rows_len = br.rows_len();
        let cols = get_matching_columns(br, &self.field_filters);
        for c in cols {
            for row_idx in 0..rows_len {
                if let Some(f) = br.column_get_float_value_at_row(c, row_idx) {
                    self.update_state(f);
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
        let cols = get_matching_columns(br, &self.field_filters);
        for c in cols {
            if let Some(f) = br.column_get_float_value_at_row(c, row_index) {
                self.update_state(f);
            }
        }
        0
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsStddevProcessor>()
            .expect("merge_state: other must be StatsStddevProcessor");

        let delta = src.avg - self.avg;
        let count_new = src.count + self.count;
        if count_new == 0.0 {
            return;
        }
        let avg_new = (self.count * self.avg + src.count * src.avg) / count_new;
        let q_new = self.q + src.q + delta * delta * (self.count * src.count) / count_new;
        self.avg = avg_new;
        self.q = q_new;
        self.count = count_new;
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        marshal_float64(dst, self.avg);
        marshal_float64(dst, self.q);
        marshal_float64(dst, self.count);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        if src.len() != 24 {
            return Err(format!(
                "cannot unmarshal stddev from {} bytes; need 24 bytes",
                src.len()
            ));
        }
        self.avg = unmarshal_float64(&src[0..8]);
        self.q = unmarshal_float64(&src[8..16]);
        self.count = unmarshal_float64(&src[16..24]);
        Ok(0)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let stddev = (self.q / self.count).sqrt();
        marshal_float64_string(dst, stddev);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// PORT NOTE — deferred tests: `TestParseStatsStddev*` (lexer) and
// `TestStatsStddev` (`expectPipeResults`). Pure computation covered below.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn run(filters: &[&str], block: &[Vec<Field>]) -> String {
        let sf = new_stats_stddev(filters.iter().map(|s| s.to_string()).collect());
        let mut sp = sf.new_stats_processor();
        let mut br = BlockResult::default();
        br.must_init_from_rows(block);
        sp.update_stats_for_all_rows(&sf, &mut br);
        let mut dst = Vec::new();
        sp.finalize_stats(&sf, &mut dst, None);
        String::from_utf8(dst).unwrap()
    }

    #[test]
    fn test_stddev_basic() {
        // values 1 and 3: mean 2, variance (1+1)/2 = 1, stddev 1
        let block = vec![vec![field("a", "1")], vec![field("a", "3")]];
        assert_eq!(run(&["a"], &block), "1");
    }

    #[test]
    fn test_stddev_zero() {
        let block = vec![vec![field("a", "5")], vec![field("a", "5")]];
        assert_eq!(run(&["a"], &block), "0");
    }

    /// Extreme magnitudes render like Go `strconv.AppendFloat(.., 'f', -1,
    /// 64)`: full decimal digits (never exponent form), and Go's spellings
    /// for non-finite results.
    #[test]
    fn test_stddev_extreme_magnitude_rendering() {
        // Two samples 0 and 2^501 (exactly representable): stddev =
        // sqrt((2^501 * 2^500) / 2) = 2^500, rendered in Go's `'f'` prec=-1
        // shortest-round-trip form (17 significant digits + trailing zeros,
        // NOT the exact 151-digit integer expansion).
        let mut sp = StatsStddevProcessor {
            field_filters: vec!["a".to_string()],
            avg: 0.0,
            q: 0.0,
            count: 0.0,
        };
        sp.update_state(0.0);
        sp.update_state((2.0f64).powi(501));
        let sf = new_stats_stddev(vec!["a".to_string()]);
        let mut dst = Vec::new();
        sp.finalize_stats(&sf, &mut dst, None);
        assert_eq!(
            String::from_utf8(dst).unwrap(),
            "3273390607896142000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        );

        // Overflowed q → +Inf stddev, spelled like Go.
        sp.q = f64::INFINITY;
        let mut dst = Vec::new();
        sp.finalize_stats(&sf, &mut dst, None);
        assert_eq!(String::from_utf8(dst).unwrap(), "+Inf");
    }

    #[test]
    fn test_stddev_export_import_merge() {
        let sf = new_stats_stddev(vec!["a".to_string()]);
        let mut a = sf.new_stats_processor();
        let mut br1 = BlockResult::default();
        br1.must_init_from_rows(&[vec![field("a", "1")]]);
        a.update_stats_for_all_rows(&sf, &mut br1);

        let mut buf = Vec::new();
        a.export_state(&mut buf, None);
        assert_eq!(buf.len(), 24);
        let mut a2 = sf.new_stats_processor();
        a2.import_state(&buf, None).unwrap();

        let mut b = sf.new_stats_processor();
        let mut br2 = BlockResult::default();
        br2.must_init_from_rows(&[vec![field("a", "3")]]);
        b.update_stats_for_all_rows(&sf, &mut br2);

        a2.merge_state(&sf, b.as_ref());
        let mut dst = Vec::new();
        a2.finalize_stats(&sf, &mut dst, None);
        assert_eq!(String::from_utf8(dst).unwrap(), "1");
    }
}

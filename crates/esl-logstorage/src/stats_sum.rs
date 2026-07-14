//! Port of `stats_sum.go` — the `sum(...)` stats function.
//!
//! Also hosts [`get_matching_columns`], the port of Go's `getMatchingColumns`
//! (`block_result.go`), shared by `sum`, `sum_len`, `avg`, `row_any` and
//! `rate_sum`.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use crate::block_result::{BlockResult, ColRef};
use crate::prefix_filter::{self, Filter};
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_count::{field_names_string, is_single_field};
use crate::values_encoder::{marshal_float64, unmarshal_float64};

/// Returns the columns of `br` matching `filters` (Go `getMatchingColumns`).
///
/// PORT NOTE: Go pools the returned `matchingColumns`; here it returns an owned
/// `Vec<ColRef>` since `ColRef` is a cheap index handle. Empty columns for
/// unmatched non-wildcard filters are materialized via `get_column_by_name`,
/// matching `getMatchingColumnsSlow`.
pub(crate) fn get_matching_columns(br: &mut BlockResult, filters: &[String]) -> Vec<ColRef> {
    if is_single_field(filters) {
        return vec![br.get_column_by_name(&filters[0])];
    }

    let cs = br.get_columns();
    let mut dst = Vec::new();
    for &c in &cs {
        if prefix_filter::match_filters_bytes(filters, br.column_name(c)) {
            dst.push(c);
        }
    }
    for f in filters {
        if prefix_filter::is_wildcard_filter(f) {
            continue;
        }
        let mut need_empty = true;
        for &c in &cs {
            if br.column_name(c) == f.as_bytes() {
                need_empty = false;
                break;
            }
        }
        if need_empty {
            dst.push(br.get_column_by_name(f));
        }
    }
    dst
}

/// `sum(...)` stats function.
pub struct StatsSum {
    pub(crate) field_filters: Vec<String>,
}

/// Builds a [`StatsSum`] from already-parsed field filters (Go `parseStatsSum`).
pub(crate) fn new_stats_sum(field_filters: Vec<String>) -> StatsSum {
    StatsSum { field_filters }
}

impl StatsFunc for StatsSum {
    fn to_string(&self) -> String {
        format!("sum({})", field_names_string(&self.field_filters))
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsSumProcessor {
            sum: f64::NAN,
            field_filters: self.field_filters.clone(),
        })
    }
}

#[derive(Default, Debug)]
pub(crate) struct StatsSumProcessor {
    pub(crate) sum: f64,
    pub(crate) field_filters: Vec<String>,
}

impl StatsSumProcessor {
    pub(crate) fn update_state(&mut self, f: f64) {
        if self.sum.is_nan() {
            self.sum = f;
        } else {
            self.sum += f;
        }
    }
}

impl StatsProcessor for StatsSumProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        for c in get_matching_columns(br, &self.field_filters) {
            let (f, count) = br.column_sum_values(c);
            if count > 0 {
                self.update_state(f);
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
        for c in get_matching_columns(br, &self.field_filters) {
            if let Some(f) = br.column_get_float_value_at_row(c, row_index) {
                self.update_state(f);
            }
        }
        0
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsSumProcessor>()
            .expect("BUG: mergeState with mismatched processor type");
        if !src.sum.is_nan() {
            self.update_state(src.sum);
        }
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        marshal_float64(dst, self.sum);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        if src.len() != 8 {
            return Err(format!(
                "unexpected state length; got {} bytes; want 8 bytes",
                src.len()
            ));
        }
        self.sum = unmarshal_float64(src);
        Ok(0)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        crate::values_encoder::marshal_float64_string(dst, self.sum);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: parse/pipe tests deferred (need parser + pipe_stats).
    #[test]
    fn test_stats_sum_export_import_state() {
        fn f(ssp: &StatsSumProcessor, data_len_expected: usize) {
            let mut data = Vec::new();
            ssp.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected);

            let mut ssp2 = StatsSumProcessor::default();
            let state_size = ssp2.import_state(&data, None).unwrap();
            assert_eq!(state_size, 0);
            // Compare the numeric state (bit-for-bit to handle NaN/zero).
            assert_eq!(ssp.sum.to_bits(), ssp2.sum.to_bits());
            assert_eq!(ssp.field_filters, ssp2.field_filters);
        }

        // zero value
        let ssp = StatsSumProcessor::default();
        f(&ssp, 8);

        // non-empty value
        let ssp = StatsSumProcessor {
            sum: 234.34,
            ..Default::default()
        };
        f(&ssp, 8);
    }
}

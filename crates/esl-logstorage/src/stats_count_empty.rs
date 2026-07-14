//! Port of `stats_count_empty.go` — the `count_empty(...)` stats function.
//!
//! PORT NOTE: like `stats_count`, the processor owns a clone of its field
//! filters (Go's `statsFunc` has no downcast hook) and Go's per-`valueType`
//! encoded fast paths are collapsed into decoded-value scans; the result is
//! identical because a decoded numeric/time value is never the empty string.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::{self, Filter};
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_count::is_single_field;
use crate::values_encoder::ValueType;

/// `count_empty(...)` stats function.
pub struct StatsCountEmpty {
    field_filters: Vec<Vec<u8>>,
}

/// Builds a [`StatsCountEmpty`] from already-parsed field filters
/// (Go `parseStatsCountEmpty`'s tail).
pub(crate) fn new_stats_count_empty(field_filters: Vec<Vec<u8>>) -> StatsCountEmpty {
    StatsCountEmpty { field_filters }
}

impl StatsFunc for StatsCountEmpty {
    fn to_string(&self) -> String {
        format!(
            "count_empty({})",
            crate::stats_count::field_names_string(&self.field_filters)
        )
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsCountEmptyProcessor {
            rows_count: 0,
            field_filters: self.field_filters.clone(),
        })
    }
}

#[derive(Default, PartialEq, Debug)]
pub(crate) struct StatsCountEmptyProcessor {
    rows_count: u64,
    field_filters: Vec<Vec<u8>>,
}

impl StatsProcessor for StatsCountEmptyProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        let rows_len = br.rows_len();

        if is_single_field(&self.field_filters) {
            let c = br.get_column_by_name(&self.field_filters[0]);
            if br.column_is_const(c) {
                if br.column_get_value_at_row(c, 0).is_empty() {
                    self.rows_count += rows_len as u64;
                }
                return 0;
            }
            if br.column_is_time(c) {
                return 0;
            }
            let mut n = 0u64;
            for v in br.column_get_values(c) {
                if v.is_empty() {
                    n += 1;
                }
            }
            self.rows_count += n;
            return 0;
        }

        // Slow path - count rows whose value is empty for all the fields
        // enumerated inside count_empty().
        let mut non_empty = vec![false; rows_len];
        for c in br.get_columns() {
            if !prefix_filter::match_filters(&self.field_filters, br.column_name(c)) {
                continue;
            }
            if br.column_is_const(c) {
                if !br.column_get_value_at_row(c, 0).is_empty() {
                    return 0;
                }
                continue;
            }
            if br.column_is_time(c) {
                return 0;
            }
            let vt = br.column_value_type(c);
            if vt != ValueType::STRING && vt != ValueType::DICT {
                // Numeric columns are never empty -> no all-empty rows.
                return 0;
            }
            for (i, v) in br.column_get_values(c).iter().enumerate() {
                if !v.is_empty() {
                    non_empty[i] = true;
                }
            }
        }
        self.rows_count += non_empty.iter().filter(|&&b| !b).count() as u64;
        0
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        if is_single_field(&self.field_filters) {
            let c = br.get_column_by_name(&self.field_filters[0]);
            if br.column_get_value_at_row(c, row_index).is_empty() {
                self.rows_count += 1;
            }
            return 0;
        }

        // Slow path - count the row if all enumerated fields are empty.
        for c in br.get_columns() {
            if !prefix_filter::match_filters(&self.field_filters, br.column_name(c)) {
                continue;
            }
            if !br.column_get_value_at_row(c, row_index).is_empty() {
                return 0;
            }
        }
        self.rows_count += 1;
        0
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsCountEmptyProcessor>()
            .expect("BUG: mergeState with mismatched processor type");
        self.rows_count += src.rows_count;
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        encoding::marshal_var_uint64(dst, self.rows_count);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (rows_count, n) = encoding::unmarshal_var_uint64(src);
        if n <= 0 {
            return Err("cannot unmarshal rowsCount".to_string());
        }
        let src = &src[n as usize..];
        self.rows_count = rows_count;
        if !src.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left; len(tail)={}",
                src.len()
            ));
        }
        Ok(0)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        dst.extend_from_slice(self.rows_count.to_string().as_bytes());
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
    fn test_stats_count_empty_export_import_state() {
        fn f(scp: &StatsCountEmptyProcessor, data_len_expected: usize) {
            let mut data = Vec::new();
            scp.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected);

            let mut scp2 = StatsCountEmptyProcessor::default();
            let state_size = scp2.import_state(&data, None).unwrap();
            assert_eq!(state_size, 0);
            assert_eq!(scp, &scp2);
        }

        let scp = StatsCountEmptyProcessor::default();
        f(&scp, 1);

        let scp = StatsCountEmptyProcessor {
            rows_count: 234,
            ..Default::default()
        };
        f(&scp, 2);
    }
}

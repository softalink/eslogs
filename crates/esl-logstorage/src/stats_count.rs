//! Port of `stats_count.go` — the `count(...)` stats function.
//!
//! PORT NOTE: Go's `statsFunc` interface has no downcast hook, so each processor
//! owns a clone of the fields it needs from its `StatsFunc` (the `sf` parameter
//! required by the trait is unused). Go's per-`valueType` encoded fast paths in
//! `updateStatsForAllRows` are collapsed into decoded-value scans via
//! `block_result` accessors (`column_get_values` / `column_get_value_at_row`);
//! the result is identical because a decoded numeric/time value is never the
//! empty string.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::{self, Filter};
use crate::stats::{StatsFunc, StatsProcessor};

/// Renders a list of field filters as a comma-separated, quoted string
/// (Go `fieldNamesString`).
pub(crate) fn field_names_string(fields: &[String]) -> String {
    fields
        .iter()
        .map(|f| crate::filter_generic::quote_field_filter_if_needed(f))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Reports whether `filters` selects exactly one concrete (non-wildcard) field
/// (Go `isSingleField`).
pub(crate) fn is_single_field(filters: &[String]) -> bool {
    filters.len() == 1 && !prefix_filter::is_wildcard_filter(&filters[0])
}

/// `count(...)` stats function.
pub struct StatsCount {
    field_filters: Vec<String>,
}

/// Builds a [`StatsCount`] from already-parsed field filters (Go
/// `parseStatsCount`'s tail; the `parseStatsFuncFieldFilters` lexer step is part
/// of the not-yet-ported parser).
pub(crate) fn new_stats_count(field_filters: Vec<String>) -> StatsCount {
    StatsCount { field_filters }
}

impl StatsFunc for StatsCount {
    fn to_string(&self) -> String {
        format!("count({})", field_names_string(&self.field_filters))
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        if prefix_filter::match_all(&self.field_filters) {
            // Special case for count() - it doesn't need loading any additional fields.
            return;
        }
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsCountProcessor {
            rows_count: 0,
            field_filters: self.field_filters.clone(),
        })
    }
}

#[derive(Default, PartialEq, Debug)]
pub(crate) struct StatsCountProcessor {
    rows_count: u64,
    field_filters: Vec<String>,
}

impl StatsProcessor for StatsCountProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        let rows_len = br.rows_len();

        if prefix_filter::match_all(&self.field_filters) {
            // Fast path - unconditionally count all the rows.
            self.rows_count += rows_len as u64;
            return 0;
        }

        if is_single_field(&self.field_filters) {
            let c = br.get_column_by_name(&self.field_filters[0]);
            if br.column_is_const(c) {
                if !br.column_get_value_at_row(c, 0).is_empty() {
                    self.rows_count += rows_len as u64;
                }
                return 0;
            }
            let mut n = 0u64;
            for v in br.column_get_values(c) {
                if !v.is_empty() {
                    n += 1;
                }
            }
            self.rows_count += n;
            return 0;
        }

        // Slow path - count rows containing at least a single non-empty value
        // for the fields enumerated inside count().
        let mut non_empty = vec![false; rows_len];
        for c in br.get_columns() {
            if !prefix_filter::match_filters_bytes(&self.field_filters, br.column_name(c)) {
                continue;
            }
            if br.column_is_const(c) {
                if !br.column_get_value_at_row(c, 0).is_empty() {
                    self.rows_count += rows_len as u64;
                    return 0;
                }
                continue;
            }
            for (i, v) in br.column_get_values(c).iter().enumerate() {
                if !v.is_empty() {
                    non_empty[i] = true;
                }
            }
        }
        self.rows_count += non_empty.iter().filter(|&&b| b).count() as u64;
        0
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        if prefix_filter::match_all(&self.field_filters) {
            self.rows_count += 1;
            return 0;
        }

        if is_single_field(&self.field_filters) {
            let c = br.get_column_by_name(&self.field_filters[0]);
            if !br.column_get_value_at_row(c, row_index).is_empty() {
                self.rows_count += 1;
            }
            return 0;
        }

        // Slow path - count the row if at least a single enumerated field is non-empty.
        for c in br.get_columns() {
            if !prefix_filter::match_filters_bytes(&self.field_filters, br.column_name(c)) {
                continue;
            }
            if !br.column_get_value_at_row(c, row_index).is_empty() {
                self.rows_count += 1;
                return 0;
            }
        }
        0
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsCountProcessor>()
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

    // PORT NOTE: TestParseStatsCountSuccess/Failure and TestStatsCount need the
    // not-yet-ported lexer/parser and pipe_stats pipeline; deferred. Only the
    // pure export/import test is ported here.
    #[test]
    fn test_stats_count_export_import_state() {
        fn f(scp: &StatsCountProcessor, data_len_expected: usize) {
            let mut data = Vec::new();
            scp.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected);

            let mut scp2 = StatsCountProcessor::default();
            let state_size = scp2.import_state(&data, None).unwrap();
            assert_eq!(state_size, 0);
            assert_eq!(scp, &scp2);
        }

        let scp = StatsCountProcessor::default();
        f(&scp, 1);

        let scp = StatsCountProcessor {
            rows_count: 234,
            ..Default::default()
        };
        f(&scp, 2);
    }
}

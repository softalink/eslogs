//! Port of EsLogs `lib/logstorage/stats_values.go`.
//!
//! `values(fields...)` collects every value (with duplicates) across the
//! matching fields, in collection order, optionally capped by `limit`. It
//! reuses [`crate::stats_uniq_values::get_matching_columns`] and
//! [`crate::stats_uniq_values::marshal_json_array`].
//!
//! # PORT NOTES
//!
//! * Same allocator / `sf` / immutable-`merge_state` notes as
//!   [`crate::stats_count_uniq`].
//!
//! * **Dict fast path folded.** Go's `updateStatsForAllRowsColumn` has a
//!   dedicated `valueTypeDict` branch; block_result.rs does not expose dict
//!   internals, so DICT folds into the generic decoded-values path. The
//!   collected values are identical (one per row); only the internal
//!   allocation/accounting differs.
//!
//! * **State-size delta.** Each collected value costs its byte length plus
//!   [`SIZE_OF_STRING`] (Go `unsafe.Sizeof(string)` = 16), matching Go's
//!   accounting.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding::{
    marshal_bytes, marshal_var_uint64, unmarshal_bytes, unmarshal_var_uint64,
};

use crate::block_result::{BlockResult, ColRef};
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_count_uniq::{SIZE_OF_STRING, field_names_string};
use crate::stats_uniq_values::{get_matching_columns, marshal_json_array};

// ---------------------------------------------------------------------------
// StatsValues (StatsFunc)
// ---------------------------------------------------------------------------

/// `values(fields...)` stats function (Go `statsValues`).
#[derive(Debug, Default, Clone)]
pub struct StatsValues {
    pub(crate) field_filters: Vec<Vec<u8>>,
    pub(crate) limit: u64,
}

impl StatsValues {
    /// Constructs a `values` function (exposed for the future parser).
    #[allow(dead_code)] // consumed by the not-yet-ported stats parser.
    pub(crate) fn new(field_filters: Vec<Vec<u8>>, limit: u64) -> Self {
        Self {
            field_filters,
            limit,
        }
    }
}

impl StatsFunc for StatsValues {
    fn to_string(&self) -> String {
        let mut s = format!("values({})", field_names_string(&self.field_filters));
        if self.limit > 0 {
            s += &format!(" limit {}", self.limit);
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsValuesProcessor {
            field_filters: self.field_filters.clone(),
            limit: self.limit,
            ..Default::default()
        })
    }
}

// ---------------------------------------------------------------------------
// StatsValuesProcessor
// ---------------------------------------------------------------------------

/// Accumulates `values` state for one group (Go `statsValuesProcessor`).
#[derive(Debug, Default, PartialEq)]
pub struct StatsValuesProcessor {
    pub(crate) field_filters: Vec<Vec<u8>>,
    pub(crate) limit: u64,
    pub(crate) values: Vec<Vec<u8>>,
}

impl StatsValuesProcessor {
    fn limit_reached(&self) -> bool {
        self.limit > 0 && self.values.len() as u64 > self.limit
    }

    fn update_stats_for_all_rows_column(&mut self, br: &mut BlockResult, r: ColRef) -> i64 {
        let rows = br.rows_len();
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0).to_vec();
            let mut inc = v.len() as i64;
            for _ in 0..rows {
                self.values.push(v.clone());
            }
            inc += rows as i64 * SIZE_OF_STRING;
            return inc;
        }

        // Generic path (also covers DICT — see module PORT NOTE): append every
        // row value, sharing the clone across consecutive equal values.
        let mut inc = 0i64;
        let values = br.column_get_values(r);
        let mut v_prev: Vec<u8> = Vec::new();
        for value in values {
            if self.values.is_empty() || *value != v_prev {
                v_prev = value.clone();
                inc += v_prev.len() as i64;
            }
            self.values.push(v_prev.clone());
        }
        inc += rows as i64 * SIZE_OF_STRING;
        inc
    }

    fn update_stats_for_row_column(
        &mut self,
        br: &mut BlockResult,
        r: ColRef,
        row_idx: usize,
    ) -> i64 {
        let v = if br.column_is_const(r) {
            br.column_get_value_at_row(r, 0).to_vec()
        } else {
            br.column_get_value_at_row(r, row_idx).to_vec()
        };
        let inc = v.len() as i64;
        self.values.push(v);
        inc + SIZE_OF_STRING
    }
}

impl StatsProcessor for StatsValuesProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        if self.limit_reached() {
            return 0;
        }
        let mut inc = 0i64;
        let mc = get_matching_columns(br, &self.field_filters.clone());
        for r in mc {
            inc += self.update_stats_for_all_rows_column(br, r);
        }
        inc
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_idx: usize,
    ) -> i64 {
        if self.limit_reached() {
            return 0;
        }
        let mut inc = 0i64;
        let mc = get_matching_columns(br, &self.field_filters.clone());
        for r in mc {
            inc += self.update_stats_for_row_column(br, r, row_idx);
        }
        inc
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        if self.limit_reached() {
            return;
        }
        let src = other
            .as_any()
            .downcast_ref::<StatsValuesProcessor>()
            .expect("merge_state: other must be a StatsValuesProcessor");
        self.values.extend(src.values.iter().cloned());
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        marshal_var_uint64(dst, self.values.len() as u64);
        for v in &self.values {
            marshal_bytes(dst, v);
        }
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (values_len, n) = unmarshal_var_uint64(src);
        if n <= 0 {
            return Err("cannot unmarshal valuesLen".to_string());
        }
        let mut src = &src[n as usize..];

        let mut values: Vec<Vec<u8>> = Vec::with_capacity(values_len as usize);
        let mut state_size = SIZE_OF_STRING * values_len as i64;
        for _ in 0..values_len {
            let (v, nn) = unmarshal_bytes(src);
            if nn <= 0 {
                return Err("cannot unmarshal value".to_string());
            }
            let v = v.unwrap();
            src = &src[nn as usize..];
            state_size += v.len() as i64;
            values.push(v.to_vec());
        }
        self.values = values;

        if !src.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left; len(tail)={}",
                src.len()
            ));
        }
        Ok(state_size)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let items: &[Vec<u8>] = if self.limit > 0 && self.values.len() as u64 > self.limit {
            &self.values[..self.limit as usize]
        } else {
            &self.values
        };
        marshal_json_array(dst, items);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: TestParseStatsValuesSuccess/Failure (parser) are deferred until
    // the stats parser is ported.

    fn new_processor() -> StatsValuesProcessor {
        StatsValuesProcessor::default()
    }

    fn check(svp: &StatsValuesProcessor, data_len_expected: usize) {
        let mut data = Vec::new();
        svp.export_state(&mut data, None);
        assert_eq!(data.len(), data_len_expected, "unexpected dataLen");

        let mut svp2 = new_processor();
        svp2.import_state(&data, None).unwrap();
        assert_eq!(*svp, svp2, "unexpected state imported");
    }

    #[test]
    fn test_stats_values_export_import_state() {
        // empty state
        let svp = new_processor();
        check(&svp, 1);

        // non-empty state
        let mut svp = new_processor();
        svp.values = vec![b"foo".to_vec(), b"bar".to_vec(), b"baz".to_vec()];
        check(&svp, 13);
    }
}

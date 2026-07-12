//! Port of `stats_sum_len.go` — the `sum_len(...)` stats function.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_count::field_names_string;
use crate::stats_sum::get_matching_columns;

/// `sum_len(...)` stats function.
pub struct StatsSumLen {
    field_filters: Vec<String>,
}

/// Builds a [`StatsSumLen`] from already-parsed field filters
/// (Go `parseStatsSumLen`).
pub(crate) fn new_stats_sum_len(field_filters: Vec<String>) -> StatsSumLen {
    StatsSumLen { field_filters }
}

impl StatsFunc for StatsSumLen {
    fn to_string(&self) -> String {
        format!("sum_len({})", field_names_string(&self.field_filters))
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsSumLenProcessor {
            sum_len: 0,
            field_filters: self.field_filters.clone(),
        })
    }
}

#[derive(Default, PartialEq, Debug)]
pub(crate) struct StatsSumLenProcessor {
    sum_len: u64,
    field_filters: Vec<String>,
}

impl StatsProcessor for StatsSumLenProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        for c in get_matching_columns(br, &self.field_filters) {
            self.sum_len += br.column_sum_len_values(c);
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
            self.sum_len += br.column_get_value_at_row(c, row_index).len() as u64;
        }
        0
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsSumLenProcessor>()
            .expect("BUG: mergeState with mismatched processor type");
        self.sum_len += src.sum_len;
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        encoding::marshal_var_uint64(dst, self.sum_len);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (sum_len, n) = encoding::unmarshal_var_uint64(src);
        if n <= 0 {
            return Err("cannot unmarshal sumLen".to_string());
        }
        let src = &src[n as usize..];
        self.sum_len = sum_len;
        if !src.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left; len(tail)={}",
                src.len()
            ));
        }
        Ok(0)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        dst.extend_from_slice(self.sum_len.to_string().as_bytes());
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
    fn test_stats_sum_len_export_import_state() {
        fn f(ssp: &StatsSumLenProcessor, data_len_expected: usize) {
            let mut data = Vec::new();
            ssp.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected);

            let mut ssp2 = StatsSumLenProcessor::default();
            let state_size = ssp2.import_state(&data, None).unwrap();
            assert_eq!(state_size, 0);
            assert_eq!(ssp, &ssp2);
        }

        // zero value
        let ssp = StatsSumLenProcessor::default();
        f(&ssp, 1);

        // non-empty value
        let ssp = StatsSumLenProcessor {
            sum_len: 234,
            ..Default::default()
        };
        f(&ssp, 2);
    }
}

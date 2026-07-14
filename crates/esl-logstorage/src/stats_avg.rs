//! Port of `stats_avg.go` — the `avg(...)` stats function.
//!
//! PORT NOTE: `parseStatsFuncFields`/`parseStatsFuncArgs`/
//! `parseStatsFuncFieldFilters` (the lexer-driven arg parsers in `stats_avg.go`)
//! belong to the not-yet-ported parser and are omitted here.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_count::field_names_string;
use crate::stats_sum::get_matching_columns;
use crate::values_encoder::{marshal_float64, unmarshal_float64};

/// `avg(...)` stats function.
pub struct StatsAvg {
    field_filters: Vec<Vec<u8>>,
}

/// Builds a [`StatsAvg`] from already-parsed field filters (Go `parseStatsAvg`).
pub(crate) fn new_stats_avg(field_filters: Vec<Vec<u8>>) -> StatsAvg {
    StatsAvg { field_filters }
}

impl StatsFunc for StatsAvg {
    fn to_string(&self) -> String {
        format!("avg({})", field_names_string(&self.field_filters))
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsAvgProcessor {
            sum: 0.0,
            count: 0,
            field_filters: self.field_filters.clone(),
        })
    }
}

#[derive(Default, Debug)]
pub(crate) struct StatsAvgProcessor {
    sum: f64,
    count: u64,
    field_filters: Vec<Vec<u8>>,
}

impl StatsProcessor for StatsAvgProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        for c in get_matching_columns(br, &self.field_filters) {
            let (f, count) = br.column_sum_values(c);
            self.sum += f;
            self.count += count as u64;
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
                self.sum += f;
                self.count += 1;
            }
        }
        0
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsAvgProcessor>()
            .expect("BUG: mergeState with mismatched processor type");
        self.sum += src.sum;
        self.count += src.count;
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        marshal_float64(dst, self.sum);
        encoding::marshal_var_uint64(dst, self.count);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        if src.len() < 8 {
            return Err(format!(
                "cannot unmarshal avg from {} bytes; need 8 bytes",
                src.len()
            ));
        }
        self.sum = unmarshal_float64(&src[..8]);
        let src = &src[8..];

        let (count, n) = encoding::unmarshal_var_uint64(src);
        if n <= 0 {
            return Err("cannot unmarshal count".to_string());
        }
        self.count = count;
        let src = &src[n as usize..];
        if !src.is_empty() {
            return Err(format!("unexpected tail left; len(tail)={}", src.len()));
        }
        Ok(0)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let avg = self.sum / self.count as f64;
        crate::values_encoder::marshal_float64_string(dst, avg);
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
    fn test_stats_avg_export_import_state() {
        fn f(sap: &StatsAvgProcessor, data_len_expected: usize) {
            let mut data = Vec::new();
            sap.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected);

            let mut sap2 = StatsAvgProcessor::default();
            let state_size = sap2.import_state(&data, None).unwrap();
            assert_eq!(state_size, 0);
            assert_eq!(sap.sum.to_bits(), sap2.sum.to_bits());
            assert_eq!(sap.count, sap2.count);
            assert_eq!(sap.field_filters, sap2.field_filters);
        }

        let sap = StatsAvgProcessor::default();
        f(&sap, 9);

        let sap = StatsAvgProcessor {
            sum: 123.3243,
            count: 234,
            ..Default::default()
        };
        f(&sap, 10);
    }
}

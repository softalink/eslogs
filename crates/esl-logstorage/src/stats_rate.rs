//! Port of `stats_rate.go` — the `rate()` stats function.
//!
//! PORT NOTE: Go keeps `stepSeconds` on the `statsRate` func and reads it back
//! in `finalizeStats` via a downcast of `sf`. The Rust `StatsFunc` trait has no
//! downcast hook, so the processor is given a copy of `step_seconds` at
//! construction and the `sf` parameter is unused.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::values_encoder::marshal_float64_string;

/// `rate()` stats function.
///
/// `step_seconds` must be updated by the caller before `new_stats_processor`.
pub struct StatsRate {
    pub(crate) step_seconds: f64,
}

/// Builds a [`StatsRate`] (Go `parseStatsRate` — `rate()` takes no args).
pub(crate) fn new_stats_rate() -> StatsRate {
    StatsRate { step_seconds: 0.0 }
}

impl StatsFunc for StatsRate {
    fn to_string(&self) -> String {
        "rate()".to_string()
    }

    fn update_needed_fields(&self, _pf: &mut Filter) {
        // No columns need fetching for rate() - the number of matching rows is
        // blockResult.rowsLen.
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsRateProcessor {
            rows_count: 0,
            step_seconds: self.step_seconds,
        })
    }
}

#[derive(Default, PartialEq, Debug)]
pub(crate) struct StatsRateProcessor {
    rows_count: u64,
    step_seconds: f64,
}

impl StatsProcessor for StatsRateProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        self.rows_count += br.rows_len() as u64;
        0
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        _br: &mut BlockResult,
        _row_index: usize,
    ) -> i64 {
        self.rows_count += 1;
        0
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsRateProcessor>()
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
        let mut rate = self.rows_count as f64;
        if self.step_seconds > 0.0 {
            rate /= self.step_seconds;
        }
        marshal_float64_string(dst, rate);
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
    fn test_stats_rate_export_import_state() {
        fn f(srp: &StatsRateProcessor, data_len_expected: usize) {
            let mut data = Vec::new();
            srp.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected);

            let mut srp2 = StatsRateProcessor::default();
            let state_size = srp2.import_state(&data, None).unwrap();
            assert_eq!(state_size, 0);
            assert_eq!(srp, &srp2);
        }

        let srp = StatsRateProcessor::default();
        f(&srp, 1);

        let srp = StatsRateProcessor {
            rows_count: 234,
            ..Default::default()
        };
        f(&srp, 2);
    }
}

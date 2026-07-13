//! Port of `stats_rate_sum.go` — the `rate_sum(...)` stats function.
//!
//! `rate_sum` is `sum` divided by the step, so it wraps a
//! [`StatsSumProcessor`](crate::stats_sum::StatsSumProcessor). Per the same
//! downcast limitation as `stats_rate`, `step_seconds` is copied into the
//! processor at construction rather than read from `sf` in `finalize_stats`.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_sum::{StatsSum, StatsSumProcessor, new_stats_sum};
use crate::values_encoder::marshal_float64_string;

/// `rate_sum(...)` stats function.
///
/// `step_seconds` must be updated by the caller before `new_stats_processor`.
pub struct StatsRateSum {
    ss: StatsSum,
    pub(crate) step_seconds: f64,
}

/// Builds a [`StatsRateSum`] from already-parsed field filters
/// (Go `parseStatsRateSum`).
pub(crate) fn new_stats_rate_sum(field_filters: Vec<String>) -> StatsRateSum {
    StatsRateSum {
        ss: new_stats_sum(field_filters),
        step_seconds: 0.0,
    }
}

impl StatsFunc for StatsRateSum {
    fn to_string(&self) -> String {
        format!(
            "rate_sum({})",
            crate::stats_count::field_names_string(&self.ss.field_filters)
        )
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.ss.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsRateSumProcessor {
            ssp: StatsSumProcessor {
                sum: f64::NAN,
                field_filters: self.ss.field_filters.clone(),
            },
            step_seconds: self.step_seconds,
        })
    }

    fn set_rate_step_seconds(&mut self, step_seconds: f64) {
        self.step_seconds = step_seconds;
    }
}

#[derive(Default, Debug)]
pub(crate) struct StatsRateSumProcessor {
    ssp: StatsSumProcessor,
    step_seconds: f64,
}

impl StatsProcessor for StatsRateSumProcessor {
    fn update_stats_for_all_rows(&mut self, sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        self.ssp.update_stats_for_all_rows(sf, br)
    }

    fn update_stats_for_row(
        &mut self,
        sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        self.ssp.update_stats_for_row(sf, br, row_index)
    }

    fn merge_state(&mut self, sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsRateSumProcessor>()
            .expect("BUG: mergeState with mismatched processor type");
        self.ssp.merge_state(sf, &src.ssp);
    }

    fn export_state(&self, dst: &mut Vec<u8>, stop: Option<&AtomicBool>) {
        self.ssp.export_state(dst, stop);
    }

    fn import_state(&mut self, src: &[u8], stop: Option<&AtomicBool>) -> Result<i64, String> {
        self.ssp.import_state(src, stop)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let mut rate = self.ssp.sum;
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
    fn test_stats_rate_sum_export_import_state() {
        fn f(srp: &StatsRateSumProcessor, data_len_expected: usize) {
            let mut data = Vec::new();
            srp.export_state(&mut data, None);
            assert_eq!(data.len(), data_len_expected);

            let mut srp2 = StatsRateSumProcessor::default();
            let state_size = srp2.import_state(&data, None).unwrap();
            assert_eq!(state_size, 0);
            assert_eq!(srp.ssp.sum.to_bits(), srp2.ssp.sum.to_bits());
            assert_eq!(srp.ssp.field_filters, srp2.ssp.field_filters);
            assert_eq!(srp.step_seconds.to_bits(), srp2.step_seconds.to_bits());
        }

        let srp = StatsRateSumProcessor::default();
        f(&srp, 8);

        let mut srp = StatsRateSumProcessor::default();
        srp.ssp.sum = 234.0;
        f(&srp, 8);
    }
}

//! Port of `running_stats_sum.go` — the running `sum(...)` stats function.
//!
//! PORT NOTE: the `runningStatsFunc` / `runningStatsProcessor` interfaces live
//! in `pipe_running_stats.go`, which is not yet ported; these types expose the
//! same operations as inherent methods for now (see `running_stats_count`).

use crate::prefix_filter::Filter;
use crate::rows::Field;
use crate::running_stats_count::for_each_matching_field;
use crate::stats_count::field_names_string;
use crate::values_encoder::{marshal_float64_string, try_parse_float64_bytes};

/// Running `sum(...)` stats function.
pub struct RunningStatsSum {
    field_filters: Vec<String>,
}

/// Builds a [`RunningStatsSum`] from already-parsed field filters
/// (Go `parseRunningStatsSum`).
pub(crate) fn new_running_stats_sum(field_filters: Vec<String>) -> RunningStatsSum {
    RunningStatsSum { field_filters }
}

impl std::fmt::Display for RunningStatsSum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sum({})", field_names_string(&self.field_filters))
    }
}

impl RunningStatsSum {
    pub(crate) fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    pub(crate) fn new_running_stats_processor(&self) -> RunningStatsSumProcessor {
        RunningStatsSumProcessor { sum: f64::NAN }
    }
}

pub(crate) struct RunningStatsSumProcessor {
    sum: f64,
}

impl RunningStatsSumProcessor {
    pub(crate) fn update_running_stats(&mut self, sf: &RunningStatsSum, row: &[Field]) {
        for_each_matching_field(row, &sf.field_filters, |v| {
            if let Some(f) = try_parse_float64_bytes(v) {
                if self.sum.is_nan() {
                    self.sum = f;
                } else {
                    self.sum += f;
                }
            }
        });
    }

    pub(crate) fn get_running_stats(&self) -> Vec<u8> {
        let mut dst = Vec::new();
        marshal_float64_string(&mut dst, self.sum);
        dst
    }
}

// --- Trait wiring to pipe_running_stats (added by the pipe_running_stats port) ---

impl crate::pipe_running_stats::RunningStatsFunc for RunningStatsSum {
    fn update_needed_fields(&self, pf: &mut crate::prefix_filter::Filter) {
        self.update_needed_fields(pf)
    }

    fn new_running_stats_processor(
        &self,
    ) -> Box<dyn crate::pipe_running_stats::RunningStatsProcessor> {
        Box::new(self.new_running_stats_processor())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl crate::pipe_running_stats::RunningStatsProcessor for RunningStatsSumProcessor {
    fn update_running_stats(
        &mut self,
        sf: &dyn crate::pipe_running_stats::RunningStatsFunc,
        row: &[crate::rows::Field],
    ) {
        let sf = sf
            .as_any()
            .downcast_ref::<RunningStatsSum>()
            .expect("BUG: RunningStatsSumProcessor received wrong RunningStatsFunc type");
        self.update_running_stats(sf, row)
    }

    fn get_running_stats(&self) -> Vec<u8> {
        self.get_running_stats()
    }
}

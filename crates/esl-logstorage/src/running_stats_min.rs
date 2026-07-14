//! Port of `running_stats_min.go` — the running `min(...)` stats function.
//!
//! PORT NOTE: the `runningStatsFunc` / `runningStatsProcessor` interfaces live
//! in `pipe_running_stats.go`, which is not yet ported. Following the sibling
//! [`crate::running_stats_count`], these types expose the same operations as
//! inherent methods; they will `impl` the traits once `pipe_running_stats`
//! lands.

use crate::prefix_filter::Filter;
use crate::rows::Field;
use crate::running_stats_count::for_each_matching_field;
use crate::stats_min::{field_names_string, less_bytes};

/// Running `min(...)` stats function.
pub struct RunningStatsMin {
    field_filters: Vec<String>,
}

/// Port of `parseRunningStatsMin`. Empty filters default to `["*"]`.
pub(crate) fn new_running_stats_min(mut field_filters: Vec<String>) -> RunningStatsMin {
    if field_filters.is_empty() {
        field_filters.push("*".to_string());
    }
    RunningStatsMin { field_filters }
}

impl std::fmt::Display for RunningStatsMin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "min({})", field_names_string(&self.field_filters))
    }
}

impl RunningStatsMin {
    pub(crate) fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    pub(crate) fn new_running_stats_processor(&self) -> RunningStatsMinProcessor {
        RunningStatsMinProcessor::default()
    }
}

/// Port of `runningStatsMinProcessor`.
#[derive(Default)]
pub(crate) struct RunningStatsMinProcessor {
    min: Vec<u8>,
    has_items: bool,
}

impl RunningStatsMinProcessor {
    pub(crate) fn update_running_stats(&mut self, sf: &RunningStatsMin, row: &[Field]) {
        for_each_matching_field(row, &sf.field_filters, |v| {
            if !self.has_items {
                self.min = v.to_owned();
                self.has_items = true;
                return;
            }
            if less_bytes(v, &self.min) {
                self.min = v.to_owned();
            }
        });
    }

    pub(crate) fn get_running_stats(&self) -> Vec<u8> {
        self.min.clone()
    }
}

// --- Trait wiring to pipe_running_stats (added by the pipe_running_stats port) ---

impl crate::pipe_running_stats::RunningStatsFunc for RunningStatsMin {
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

impl crate::pipe_running_stats::RunningStatsProcessor for RunningStatsMinProcessor {
    fn update_running_stats(
        &mut self,
        sf: &dyn crate::pipe_running_stats::RunningStatsFunc,
        row: &[crate::rows::Field],
    ) {
        let sf = sf
            .as_any()
            .downcast_ref::<RunningStatsMin>()
            .expect("BUG: RunningStatsMinProcessor received wrong RunningStatsFunc type");
        self.update_running_stats(sf, row)
    }

    fn get_running_stats(&self) -> Vec<u8> {
        self.get_running_stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_running_min() {
        let sf = new_running_stats_min(vec!["a".to_string()]);
        let mut sp = sf.new_running_stats_processor();
        for v in ["5", "3", "8", "1", "9"] {
            sp.update_running_stats(&sf, &[field("a", v)]);
        }
        assert_eq!(sp.get_running_stats(), b"1");
    }

    #[test]
    fn test_running_min_to_string_and_needed_fields() {
        let sf = new_running_stats_min(vec!["a".to_string()]);
        assert_eq!(sf.to_string(), "min(a)");
        let mut pf = Filter::default();
        sf.update_needed_fields(&mut pf);
        assert!(pf.match_string("a"));
    }
}

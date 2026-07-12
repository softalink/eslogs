//! Port of `running_stats_max.go` — the running `max(...)` stats function.
//!
//! Mirror of [`crate::running_stats_min`]; see it for the interface PORT NOTE.

use crate::prefix_filter::Filter;
use crate::rows::Field;
use crate::running_stats_count::for_each_matching_field;
use crate::stats_min::{field_names_string, less_string};

/// Running `max(...)` stats function.
pub struct RunningStatsMax {
    field_filters: Vec<String>,
}

/// Port of `parseRunningStatsMax`. Empty filters default to `["*"]`.
pub(crate) fn new_running_stats_max(mut field_filters: Vec<String>) -> RunningStatsMax {
    if field_filters.is_empty() {
        field_filters.push("*".to_string());
    }
    RunningStatsMax { field_filters }
}

impl std::fmt::Display for RunningStatsMax {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "max({})", field_names_string(&self.field_filters))
    }
}

impl RunningStatsMax {
    pub(crate) fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    pub(crate) fn new_running_stats_processor(&self) -> RunningStatsMaxProcessor {
        RunningStatsMaxProcessor::default()
    }
}

/// Port of `runningStatsMaxProcessor`.
#[derive(Default)]
pub(crate) struct RunningStatsMaxProcessor {
    max: String,
    has_items: bool,
}

impl RunningStatsMaxProcessor {
    pub(crate) fn update_running_stats(&mut self, sf: &RunningStatsMax, row: &[Field]) {
        for_each_matching_field(row, &sf.field_filters, |v| {
            if !self.has_items {
                self.max = v.to_owned();
                self.has_items = true;
                return;
            }
            if less_string(&self.max, v) {
                self.max = v.to_owned();
            }
        });
    }

    pub(crate) fn get_running_stats(&self) -> String {
        self.max.clone()
    }
}

// --- Trait wiring to pipe_running_stats (added by the pipe_running_stats port) ---

impl crate::pipe_running_stats::RunningStatsFunc for RunningStatsMax {
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

impl crate::pipe_running_stats::RunningStatsProcessor for RunningStatsMaxProcessor {
    fn update_running_stats(
        &mut self,
        sf: &dyn crate::pipe_running_stats::RunningStatsFunc,
        row: &[crate::rows::Field],
    ) {
        let sf = sf
            .as_any()
            .downcast_ref::<RunningStatsMax>()
            .expect("BUG: RunningStatsMaxProcessor received wrong RunningStatsFunc type");
        self.update_running_stats(sf, row)
    }

    fn get_running_stats(&self) -> String {
        self.get_running_stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn test_running_max() {
        let sf = new_running_stats_max(vec!["a".to_string()]);
        let mut sp = sf.new_running_stats_processor();
        for v in ["5", "3", "8", "1", "9"] {
            sp.update_running_stats(&sf, &[field("a", v)]);
        }
        assert_eq!(sp.get_running_stats(), "9");
    }

    #[test]
    fn test_running_max_to_string_and_needed_fields() {
        let sf = new_running_stats_max(vec!["a".to_string()]);
        assert_eq!(sf.to_string(), "max(a)");
        let mut pf = Filter::default();
        sf.update_needed_fields(&mut pf);
        assert!(pf.match_string("a"));
    }
}

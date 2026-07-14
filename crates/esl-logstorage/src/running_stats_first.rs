//! Port of `running_stats_first.go` — the running `first(field)` stats function
//! (optionally with an `offset`).
//!
//! PORT NOTE: see [`crate::running_stats_min`] for the interface PORT NOTE. The
//! `offset` is provided pre-parsed here (the lexer/parser is not yet ported).

use crate::prefix_filter::Filter;
use crate::rows::Field;
use crate::stream_filter::quote_token_if_needed;

/// Running `first(field)` stats function.
pub struct RunningStatsFirst {
    field_name: String,
    offset: usize,
}

/// Port of `parseRunningStatsFirst` (constructor form). `offset` defaults to 0.
pub(crate) fn new_running_stats_first(field_name: String, offset: usize) -> RunningStatsFirst {
    RunningStatsFirst { field_name, offset }
}

impl std::fmt::Display for RunningStatsFirst {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "first({})", quote_token_if_needed(&self.field_name))?;
        if self.offset > 0 {
            write!(f, " offset {}", self.offset)?;
        }
        Ok(())
    }
}

impl RunningStatsFirst {
    pub(crate) fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filter(&self.field_name);
    }

    pub(crate) fn new_running_stats_processor(&self) -> RunningStatsFirstProcessor {
        RunningStatsFirstProcessor::default()
    }
}

/// Port of `runningStatsFirstProcessor`.
#[derive(Default)]
pub(crate) struct RunningStatsFirstProcessor {
    value: Vec<u8>,
    rows_seen: usize,
}

impl RunningStatsFirstProcessor {
    pub(crate) fn update_running_stats(&mut self, sf: &RunningStatsFirst, row: &[Field]) {
        if self.rows_seen == sf.offset {
            for f in row {
                if f.name == sf.field_name {
                    self.value = f.value.clone();
                    break;
                }
            }
        }
        self.rows_seen += 1;
    }

    pub(crate) fn get_running_stats(&self) -> Vec<u8> {
        self.value.clone()
    }
}

// --- Trait wiring to pipe_running_stats (added by the pipe_running_stats port) ---

impl crate::pipe_running_stats::RunningStatsFunc for RunningStatsFirst {
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

impl crate::pipe_running_stats::RunningStatsProcessor for RunningStatsFirstProcessor {
    fn update_running_stats(
        &mut self,
        sf: &dyn crate::pipe_running_stats::RunningStatsFunc,
        row: &[crate::rows::Field],
    ) {
        let sf = sf
            .as_any()
            .downcast_ref::<RunningStatsFirst>()
            .expect("BUG: RunningStatsFirstProcessor received wrong RunningStatsFunc type");
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
    fn test_first_no_offset() {
        let sf = new_running_stats_first("a".to_string(), 0);
        let mut sp = sf.new_running_stats_processor();
        sp.update_running_stats(&sf, &[field("a", "one")]);
        sp.update_running_stats(&sf, &[field("a", "two")]);
        assert_eq!(sp.get_running_stats(), b"one");
    }

    #[test]
    fn test_first_with_offset() {
        let sf = new_running_stats_first("a".to_string(), 2);
        let mut sp = sf.new_running_stats_processor();
        for v in ["r0", "r1", "r2", "r3"] {
            sp.update_running_stats(&sf, &[field("a", v)]);
        }
        // offset 2 -> the 3rd row (index 2)
        assert_eq!(sp.get_running_stats(), b"r2");
    }

    #[test]
    fn test_first_to_string() {
        assert_eq!(
            new_running_stats_first("a".to_string(), 3).to_string(),
            "first(a) offset 3"
        );
        assert_eq!(
            new_running_stats_first("a".to_string(), 0).to_string(),
            "first(a)"
        );
    }

    #[test]
    fn test_first_needed_fields() {
        let sf = new_running_stats_first("a".to_string(), 0);
        let mut pf = Filter::default();
        sf.update_needed_fields(&mut pf);
        assert!(pf.match_string("a"));
    }
}

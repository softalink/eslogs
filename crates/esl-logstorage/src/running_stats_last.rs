//! Port of `running_stats_last.go` — the running `last(field)` stats function
//! (optionally with an `offset`).
//!
//! PORT NOTE: see [`crate::running_stats_min`] for the interface PORT NOTE. The
//! `offset` is provided pre-parsed here (the lexer/parser is not yet ported).

use crate::prefix_filter::Filter;
use crate::rows::Field;
use crate::stream_filter::quote_token_if_needed;

/// Running `last(field)` stats function.
pub struct RunningStatsLast {
    field_name: String,
    offset: usize,
}

/// Port of `parseRunningStatsLast` (constructor form). `offset` defaults to 0.
pub(crate) fn new_running_stats_last(field_name: String, offset: usize) -> RunningStatsLast {
    RunningStatsLast { field_name, offset }
}

impl std::fmt::Display for RunningStatsLast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "last({})", quote_token_if_needed(&self.field_name))?;
        if self.offset > 0 {
            write!(f, " offset {}", self.offset)?;
        }
        Ok(())
    }
}

impl RunningStatsLast {
    pub(crate) fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filter(&self.field_name);
    }

    pub(crate) fn new_running_stats_processor(&self) -> RunningStatsLastProcessor {
        RunningStatsLastProcessor {
            offset: self.offset,
            values: Vec::new(),
        }
    }
}

/// Port of `runningStatsLastProcessor`.
pub(crate) struct RunningStatsLastProcessor {
    offset: usize,
    values: Vec<String>,
}

impl RunningStatsLastProcessor {
    pub(crate) fn update_running_stats(&mut self, sf: &RunningStatsLast, row: &[Field]) {
        let mut value = String::new();
        for f in row {
            if f.name == sf.field_name {
                value = f.value.clone();
                break;
            }
        }

        self.values.push(value);
        if self.values.len() > self.offset + 1 {
            let drop = self.values.len() - (self.offset + 1);
            self.values.drain(0..drop);
        }
    }

    pub(crate) fn get_running_stats(&self) -> String {
        if self.values.len() <= self.offset {
            return String::new();
        }
        self.values[self.values.len() - self.offset - 1].clone()
    }
}

// --- Trait wiring to pipe_running_stats (added by the pipe_running_stats port) ---

impl crate::pipe_running_stats::RunningStatsFunc for RunningStatsLast {
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

impl crate::pipe_running_stats::RunningStatsProcessor for RunningStatsLastProcessor {
    fn update_running_stats(
        &mut self,
        sf: &dyn crate::pipe_running_stats::RunningStatsFunc,
        row: &[crate::rows::Field],
    ) {
        let sf = sf
            .as_any()
            .downcast_ref::<RunningStatsLast>()
            .expect("BUG: RunningStatsLastProcessor received wrong RunningStatsFunc type");
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
    fn test_last_no_offset() {
        let sf = new_running_stats_last("a".to_string(), 0);
        let mut sp = sf.new_running_stats_processor();
        for v in ["one", "two", "three"] {
            sp.update_running_stats(&sf, &[field("a", v)]);
        }
        assert_eq!(sp.get_running_stats(), "three");
    }

    #[test]
    fn test_last_with_offset() {
        let sf = new_running_stats_last("a".to_string(), 1);
        let mut sp = sf.new_running_stats_processor();
        for v in ["r0", "r1", "r2", "r3"] {
            sp.update_running_stats(&sf, &[field("a", v)]);
        }
        // offset 1 -> the row before last -> "r2"
        assert_eq!(sp.get_running_stats(), "r2");
    }

    #[test]
    fn test_last_offset_beyond_seen_is_empty() {
        let sf = new_running_stats_last("a".to_string(), 5);
        let mut sp = sf.new_running_stats_processor();
        sp.update_running_stats(&sf, &[field("a", "r0")]);
        assert_eq!(sp.get_running_stats(), "");
    }

    #[test]
    fn test_last_to_string_and_needed_fields() {
        let sf = new_running_stats_last("a".to_string(), 2);
        assert_eq!(sf.to_string(), "last(a) offset 2");
        let mut pf = Filter::default();
        sf.update_needed_fields(&mut pf);
        assert!(pf.match_string("a"));
    }
}

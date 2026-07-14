//! Port of `running_stats_count.go` — the running `count(...)` stats function.
//!
//! Also hosts [`for_each_matching_field`], the shared helper Go defines in this
//! file and reuses across the other `running_stats_*` functions.
//!
//! PORT NOTE: the `runningStatsFunc` / `runningStatsProcessor` interfaces live
//! in `pipe_running_stats.go`, which is not yet ported. Until then these types
//! expose the same operations as inherent methods; they will `impl` the traits
//! once `pipe_running_stats` lands.

use crate::prefix_filter::{self, Filter};
use crate::rows::Field;
use crate::stats_count::{field_names_string, is_single_field};

/// Invokes `callback` for every value of `fields` matching `field_filters`
/// (Go `forEachMatchingField`).
///
/// For a single concrete field with no matching entry, `callback("")` is
/// invoked once, mirroring the Go fast path.
pub(crate) fn for_each_matching_field(
    fields: &[Field],
    field_filters: &[Vec<u8>],
    mut callback: impl FnMut(&[u8]),
) {
    if is_single_field(field_filters) {
        // Fast path for a single field.
        let mut found = false;
        let field_name = &field_filters[0];
        for f in fields {
            if &f.name == field_name {
                callback(&f.value);
                found = true;
            }
        }
        if !found {
            callback(b"");
        }
        return;
    }

    for f in fields {
        if prefix_filter::match_filters(field_filters, &f.name) {
            callback(&f.value);
        }
    }
}

/// Running `count(...)` stats function.
pub struct RunningStatsCount {
    field_filters: Vec<Vec<u8>>,
}

/// Builds a [`RunningStatsCount`] from already-parsed field filters
/// (Go `parseRunningStatsCount`).
pub(crate) fn new_running_stats_count(field_filters: Vec<Vec<u8>>) -> RunningStatsCount {
    RunningStatsCount { field_filters }
}

impl std::fmt::Display for RunningStatsCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "count({})", field_names_string(&self.field_filters))
    }
}

impl RunningStatsCount {
    pub(crate) fn update_needed_fields(&self, pf: &mut Filter) {
        if prefix_filter::match_all(&self.field_filters) {
            // Special case for count() - it doesn't need loading any additional fields.
            return;
        }
        pf.add_allow_filters(&self.field_filters);
    }

    pub(crate) fn new_running_stats_processor(&self) -> RunningStatsCountProcessor {
        RunningStatsCountProcessor { rows_count: 0 }
    }
}

#[derive(Default)]
pub(crate) struct RunningStatsCountProcessor {
    rows_count: u64,
}

impl RunningStatsCountProcessor {
    pub(crate) fn update_running_stats(&mut self, sf: &RunningStatsCount, row: &[Field]) {
        if prefix_filter::match_all(&sf.field_filters) {
            self.rows_count += 1;
            return;
        }

        let mut matched = false;
        for_each_matching_field(row, &sf.field_filters, |v| {
            if !v.is_empty() {
                matched = true;
            }
        });
        if matched {
            self.rows_count += 1;
        }
    }

    pub(crate) fn get_running_stats(&self) -> Vec<u8> {
        self.rows_count.to_string().into_bytes()
    }
}

// --- Trait wiring to pipe_running_stats (added by the pipe_running_stats port) ---

impl crate::pipe_running_stats::RunningStatsFunc for RunningStatsCount {
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

impl crate::pipe_running_stats::RunningStatsProcessor for RunningStatsCountProcessor {
    fn update_running_stats(
        &mut self,
        sf: &dyn crate::pipe_running_stats::RunningStatsFunc,
        row: &[crate::rows::Field],
    ) {
        let sf = sf
            .as_any()
            .downcast_ref::<RunningStatsCount>()
            .expect("BUG: RunningStatsCountProcessor received wrong RunningStatsFunc type");
        self.update_running_stats(sf, row)
    }

    fn get_running_stats(&self) -> Vec<u8> {
        self.get_running_stats()
    }
}

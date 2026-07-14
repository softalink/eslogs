//! Port of `lib/logstorage/stats_json_values_topk.go` — the top-K processor for
//! `json_values(...) sort by (...) limit N`.
//!
//! Reuses [`StatsJSONValuesSortedEntry`], [`stats_json_values_less`] and the
//! state (un)marshaling helpers from the `sorted` module.
//!
//! PORT NOTE — heap comparator: Go keeps entries in a `container/heap`
//! (`statsJSONValuesTopkHeap`) whose `Less` is
//! `!statsJSONValuesLess(h.sortFields, ...)`. Go never assigns `h.sortFields`,
//! so the comparator degenerates to `!statsJSONValuesLess(nil, a, b)` ==
//! `!false` == constant `true`. This port replicates `container/heap`'s
//! `fix`/`up`/`down` exactly with that same (empty-`sortFields`) comparator, so
//! the retained entry set is byte-for-byte identical to Go's — including the
//! degenerate heap ordering. See `heap_less` below.
//!
//! See `crate::stats_json_values` for the shared allocator / captured-config /
//! parser PORT NOTEs.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use crate::block_result::{BlockResult, ColRef};
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_json_values::{BySortField, get_matching_columns, marshal_json_values};
use crate::stats_json_values_sorted::{
    StatsJSONValuesSortedEntry, less_to_ordering, new_stats_json_values_sorted_entry,
    stats_json_values_less, stats_json_values_sorted_marshal_state,
    stats_json_values_sorted_unmarshal_state,
};

/// Port of Go's `statsJSONValuesTopkProcessor` (with the heap's `entries`
/// inlined; Go's `h.sortFields` is unused — see module docs).
#[derive(Debug, Default, PartialEq)]
pub(crate) struct StatsJSONValuesTopkProcessor {
    pub(crate) sort_fields_len: usize,

    /// Go's `h.entries`.
    pub(crate) entries: Vec<StatsJSONValuesSortedEntry>,

    sort_columns: Vec<Vec<Vec<u8>>>,
    sort_values_buf: Vec<Vec<u8>>,

    // Captured config (see `crate::stats_json_values` docs).
    pub(crate) field_filters: Vec<Vec<u8>>,
    pub(crate) sort_fields: Vec<BySortField>,
    pub(crate) limit: u64,
}

impl StatsJSONValuesTopkProcessor {
    fn init_sort_columns(&mut self, br: &mut BlockResult) {
        let names: Vec<Vec<u8>> = self.sort_fields.iter().map(|sf| sf.name.clone()).collect();
        self.sort_columns.clear();
        for name in &names {
            let c = br.get_column_by_name(name);
            let values: Vec<Vec<u8>> = br.column_get_values(c).to_vec();
            self.sort_columns.push(values);
        }
    }

    fn update_state_for_row(&mut self, br: &mut BlockResult, cs: &[ColRef], row_idx: usize) -> i64 {
        self.sort_values_buf = self
            .sort_columns
            .iter()
            .map(|values| values[row_idx].clone())
            .collect();

        if (self.entries.len() as u64) < self.limit {
            let e = new_stats_json_values_sorted_entry(br, cs, &self.sort_values_buf, row_idx);
            let delta = e.size_bytes();
            self.entries.push(e);
            let last = self.entries.len() - 1;
            heap_fix(&mut self.entries, last);
            return delta;
        }

        // Fast path - the current entry isn't smaller than the heap top.
        if !stats_json_values_less(
            &self.sort_fields,
            &self.sort_values_buf,
            &self.entries[0].sort_values,
        ) {
            return 0;
        }

        // Slow path - replace the top entry with the current entry.
        let e = new_stats_json_values_sorted_entry(br, cs, &self.sort_values_buf, row_idx);
        let bytes_allocated = e.size_bytes() - self.entries[0].size_bytes();
        self.entries[0] = e;
        heap_fix(&mut self.entries, 0);
        bytes_allocated
    }
}

impl StatsProcessor for StatsJSONValuesTopkProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        self.init_sort_columns(br);

        let mc = get_matching_columns(br, &self.field_filters);
        let mut state_size_increase = 0;
        for row_idx in 0..br.rows_len() {
            state_size_increase += self.update_state_for_row(br, &mc, row_idx);
        }
        state_size_increase
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        self.init_sort_columns(br);

        let mc = get_matching_columns(br, &self.field_filters);
        self.update_state_for_row(br, &mc, row_index)
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsJSONValuesTopkProcessor>()
            .expect("merge_state: other must be a StatsJSONValuesTopkProcessor");
        self.entries.extend(src.entries.iter().cloned());
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        stats_json_values_sorted_marshal_state(dst, &self.entries);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (entries, state_size_increase) =
            stats_json_values_sorted_unmarshal_state(src, self.sort_fields_len)?;
        self.entries = entries;
        Ok(state_size_increase)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let mut order: Vec<usize> = (0..self.entries.len()).collect();
        order.sort_by(|&i, &j| {
            less_to_ordering(
                &self.sort_fields,
                &self.entries[i].sort_values,
                &self.entries[j].sort_values,
            )
        });
        if order.len() as u64 > self.limit {
            order.truncate(self.limit as usize);
        }

        let values: Vec<Vec<u8>> = order
            .iter()
            .map(|&i| self.entries[i].value.clone())
            .collect();
        marshal_json_values(dst, &values);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ---------------------------------------------------------------------------
// container/heap port (see module docs for the degenerate comparator).
// ---------------------------------------------------------------------------

/// The heap's `Less`. Mirrors Go's `statsJSONValuesTopkHeap.Less` with the
/// never-assigned (empty) `sortFields`, which makes it constant `true`.
fn heap_less(entries: &[StatsJSONValuesSortedEntry], i: usize, j: usize) -> bool {
    !stats_json_values_less(&[], &entries[i].sort_values, &entries[j].sort_values)
}

fn heap_up(entries: &mut [StatsJSONValuesSortedEntry], mut j: usize) {
    while j > 0 {
        let i = (j - 1) / 2;
        if i == j || !heap_less(entries, j, i) {
            break;
        }
        entries.swap(i, j);
        j = i;
    }
}

fn heap_down(entries: &mut [StatsJSONValuesSortedEntry], i0: usize, n: usize) -> bool {
    let mut i = i0;
    loop {
        let j1 = 2 * i + 1;
        if j1 >= n {
            break;
        }
        let mut j = j1;
        let j2 = j1 + 1;
        if j2 < n && heap_less(entries, j2, j1) {
            j = j2;
        }
        if !heap_less(entries, j, i) {
            break;
        }
        entries.swap(i, j);
        i = j;
    }
    i > i0
}

fn heap_fix(entries: &mut [StatsJSONValuesSortedEntry], i: usize) {
    let n = entries.len();
    if !heap_down(entries, i, n) {
        heap_up(entries, i);
    }
}

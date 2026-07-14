//! Port of EsLogs `lib/logstorage/stats_uniq_values.go`.
//!
//! `uniq_values(fields...)` collects the sorted set of unique non-empty values
//! across the matching fields (optionally capped by `limit`). Unlike
//! `count_uniq`, every value is tracked as a string (no numeric partitioning).
//!
//! This module hosts the `pub(crate)` helpers shared with
//! [`crate::stats_values`]: [`get_matching_columns`] and [`marshal_json_array`],
//! plus [`marshal_json_values`] and the `less_string`/`sort_strings` natural
//! comparison (Go's `pipe_sort_topk.go` `lessString`, homed here pending a
//! shared port).
//!
//! # PORT NOTES
//!
//! * Same allocator / `sf` / sequential-merge / immutable-`merge_state` /
//!   `&self`-export notes as [`crate::stats_count_uniq`].
//!
//! * **Dict fast path folded.** Go's `updateStatsForAllRowsColumn` iterates the
//!   column dictionary via `forEachDictValue`; block_result.rs does not expose
//!   dict internals, so DICT folds into the decoded-values path. Both feed
//!   `update_state`, which dedups, so the collected set is identical.
//!
//! * **`merge_items_parallel` sequential.** Go shards the final set union across
//!   CPUs and merges with a heap; the port computes the sorted set union
//!   directly (identical result). The `concurrency` field is retained (the pipe
//!   sets it) but unused by the sequential merge.
//!
//! * **`try_parse_number`/`is_likely_number`/`parse_int_go`** are copied here
//!   because block_result.rs keeps them private; dedup once a shared home
//!   exists.

use std::any::Any;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;

use esl_common::encoding::{
    marshal_bytes, marshal_var_uint64, unmarshal_bytes, unmarshal_var_uint64,
};
use esl_common::stringsutil::{json_string_bytes_append, less_natural};

use crate::block_result::{BlockResult, ColRef};
use crate::prefix_filter::{Filter, is_wildcard_filter, match_filters};
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_count_uniq::{SIZE_OF_STRING, field_names_string, need_stop};
use crate::values_encoder::{
    try_parse_bytes, try_parse_duration, try_parse_float64, try_parse_int64,
    try_parse_timestamp_rfc3339_nano, try_parse_uint64,
};

// ---------------------------------------------------------------------------
// Shared helpers (also used by stats_values)
// ---------------------------------------------------------------------------

fn is_single_field(filters: &[Vec<u8>]) -> bool {
    filters.len() == 1 && !is_wildcard_filter(&filters[0])
}

/// Returns the columns matching the given field filters (Go
/// `getMatchingColumns`). Non-wildcard filters that match nothing get an empty
/// column, matching Go's behavior.
pub(crate) fn get_matching_columns(br: &mut BlockResult, filters: &[Vec<u8>]) -> Vec<ColRef> {
    if is_single_field(filters) {
        return vec![br.get_column_by_name(&filters[0])];
    }

    let cols = br.get_columns();
    let mut dst: Vec<ColRef> = Vec::new();
    for &r in &cols {
        if match_filters(filters, br.column_name(r)) {
            dst.push(r);
        }
    }
    for f in filters {
        if is_wildcard_filter(f) {
            continue;
        }
        let mut need_empty = true;
        for &r in &cols {
            if br.column_name(r) == f.as_slice() {
                need_empty = false;
                break;
            }
        }
        if need_empty {
            dst.push(br.get_column_by_name(f));
        }
    }
    dst
}

/// Appends the JSON array representation of `items` to `dst`, JSON-escaping each
/// element (Go `marshalJSONArray`).
pub(crate) fn marshal_json_array(dst: &mut Vec<u8>, items: &[Vec<u8>]) {
    if items.is_empty() {
        dst.extend_from_slice(b"[]");
        return;
    }
    dst.push(b'[');
    json_string_bytes_append(dst, &items[0]);
    for item in &items[1..] {
        dst.push(b',');
        json_string_bytes_append(dst, item);
    }
    dst.push(b']');
}

/// Appends the JSON array representation of `items` to `dst`, treating each
/// element as pre-encoded JSON (Go `marshalJSONValues`).
#[allow(dead_code)] // PORT NOTE: used by stats_json_values (Layer-5, not yet ported).
pub(crate) fn marshal_json_values(dst: &mut Vec<u8>, items: &[Vec<u8>]) {
    if items.is_empty() {
        dst.extend_from_slice(b"[]");
        return;
    }
    dst.push(b'[');
    dst.extend_from_slice(&items[0]);
    for item in &items[1..] {
        dst.push(b',');
        dst.extend_from_slice(item);
    }
    dst.push(b']');
}

/// Compares two strings using EsLogs' typed natural ordering (Go
/// `lessString`).
pub(crate) fn less_string(a: &str, b: &str) -> bool {
    if a == b {
        return false;
    }
    if let (Some(ia), Some(ib)) = (try_parse_int64(a), try_parse_int64(b)) {
        return ia < ib;
    }
    if let (Some(ua), Some(ub)) = (try_parse_uint64(a), try_parse_uint64(b)) {
        return ua < ub;
    }
    if let (Some(ta), Some(tb)) = (
        try_parse_timestamp_rfc3339_nano(a),
        try_parse_timestamp_rfc3339_nano(b),
    ) {
        return ta < tb;
    }
    if let (Some(fa), Some(fb)) = (try_parse_number(a), try_parse_number(b)) {
        return fa < fb;
    }
    less_natural(a, b)
}

/// Sorts `a` in place using [`less_string`] (Go `sortStrings`).
pub(crate) fn sort_strings(a: &mut [Vec<u8>]) {
    a.sort_by(|x, y| {
        if x == y {
            return std::cmp::Ordering::Equal;
        }
        // Checked UTF-8 views: values with invalid UTF-8 fail every typed
        // parse in Go too, so they fall back to plain byte ordering.
        let less = match (std::str::from_utf8(x), std::str::from_utf8(y)) {
            (Ok(xs), Ok(ys)) => less_string(xs, ys),
            _ => x < y,
        };
        if less {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        }
    });
}

fn set_to_sorted_slice(m: &HashSet<Vec<u8>>) -> Vec<Vec<u8>> {
    let mut items: Vec<Vec<u8>> = m.iter().cloned().collect();
    sort_strings(&mut items);
    items
}

// PORT NOTE: copies of block_result.rs's private numeric parsing helpers.
fn try_parse_number(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    if let Some(f) = try_parse_float64(s) {
        return Some(f);
    }
    if let Some(nsecs) = try_parse_duration(s) {
        return Some(nsecs as f64);
    }
    if let Some(bytes) = try_parse_bytes(s) {
        return Some(bytes as f64);
    }
    if is_likely_number(s) {
        if let Ok(f) = s.parse::<f64>() {
            return Some(f);
        }
        if let Some(n) = parse_int_go(s) {
            return Some(n as f64);
        }
    }
    None
}

fn parse_int_go(s: &str) -> Option<i64> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (radix, digits) =
        if let Some(h) = body.strip_prefix("0x").or_else(|| body.strip_prefix("0X")) {
            (16, h)
        } else if let Some(o) = body.strip_prefix("0o").or_else(|| body.strip_prefix("0O")) {
            (8, o)
        } else if let Some(b) = body.strip_prefix("0b").or_else(|| body.strip_prefix("0B")) {
            (2, b)
        } else {
            (10, body)
        };
    let digits = digits.replace('_', "");
    let n = i64::from_str_radix(&digits, radix).ok()?;
    Some(if neg { -n } else { n })
}

fn is_likely_number(s: &str) -> bool {
    let b = s.as_bytes();
    if b.is_empty() {
        return false;
    }
    let c = b[0];
    if !c.is_ascii_digit() && c != b'-' && c != b'+' && c != b'.' {
        return false;
    }
    if s.matches('.').count() > 1 {
        return false;
    }
    if s.contains(':') || s.matches('-').count() > 2 {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// StatsUniqValues (StatsFunc)
// ---------------------------------------------------------------------------

/// `uniq_values(fields...)` stats function (Go `statsUniqValues`).
#[derive(Debug, Default, Clone)]
pub struct StatsUniqValues {
    pub(crate) field_filters: Vec<Vec<u8>>,
    pub(crate) limit: u64,
}

impl StatsUniqValues {
    /// Constructs a `uniq_values` function (exposed for the future parser).
    #[allow(dead_code)] // consumed by the not-yet-ported stats parser.
    pub(crate) fn new(field_filters: Vec<Vec<u8>>, limit: u64) -> Self {
        Self {
            field_filters,
            limit,
        }
    }
}

impl StatsFunc for StatsUniqValues {
    fn to_string(&self) -> String {
        let mut s = format!("uniq_values({})", field_names_string(&self.field_filters));
        if self.limit > 0 {
            s += &format!(" limit {}", self.limit);
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsUniqValuesProcessor {
            field_filters: self.field_filters.clone(),
            limit: self.limit,
            concurrency: 1,
            ..Default::default()
        })
    }
}

// ---------------------------------------------------------------------------
// StatsUniqValuesProcessor
// ---------------------------------------------------------------------------

/// Accumulates `uniq_values` state for one group (Go
/// `statsUniqValuesProcessor`).
#[derive(Debug, Default, PartialEq)]
pub struct StatsUniqValuesProcessor {
    pub(crate) field_filters: Vec<Vec<u8>>,
    pub(crate) limit: u64,
    // PORT NOTE: retained for parity (the pipe sets it), but the sequential
    // set-union merge does not need it.
    #[allow(dead_code)]
    pub(crate) concurrency: usize,
    pub(crate) m: HashSet<Vec<u8>>,
    pub(crate) ms: Vec<HashSet<Vec<u8>>>,
}

impl StatsUniqValuesProcessor {
    fn limit_reached(&self) -> bool {
        self.limit > 0 && self.m.len() as u64 > self.limit
    }

    fn update_state(&mut self, v: &[u8]) -> i64 {
        if v.is_empty() {
            return 0;
        }
        if self.m.contains(v) {
            return 0;
        }
        self.m.insert(v.to_vec());
        v.len() as i64 + SIZE_OF_STRING
    }

    fn update_stats_for_all_rows_column(&mut self, br: &mut BlockResult, r: ColRef) -> i64 {
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0).to_vec();
            return self.update_state(&v);
        }
        // Slow path (also covers DICT — see module PORT NOTE): unique values
        // across all rows, deduping consecutive equal values.
        let mut inc = 0i64;
        let values = br.column_get_values(r);
        for i in 0..values.len() {
            if i > 0 && values[i - 1] == values[i] {
                continue;
            }
            inc += self.update_state(&values[i]);
        }
        inc
    }

    fn update_stats_for_row_column(
        &mut self,
        br: &mut BlockResult,
        r: ColRef,
        row_idx: usize,
    ) -> i64 {
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0).to_vec();
            return self.update_state(&v);
        }
        let v = br.column_get_value_at_row(r, row_idx).to_vec();
        self.update_state(&v)
    }

    /// Sorted set union of `m` and `ms` (Go `mergeItemsParallel`).
    fn merge_items_parallel(&self) -> Vec<Vec<u8>> {
        if self.ms.is_empty() {
            return set_to_sorted_slice(&self.m);
        }
        let mut union: HashSet<Vec<u8>> = HashSet::new();
        for s in &self.ms {
            for k in s {
                union.insert(k.clone());
            }
        }
        for k in &self.m {
            union.insert(k.clone());
        }
        set_to_sorted_slice(&union)
    }
}

impl StatsProcessor for StatsUniqValuesProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        if self.limit_reached() {
            return 0;
        }
        let mut inc = 0i64;
        let mc = get_matching_columns(br, &self.field_filters.clone());
        for r in mc {
            inc += self.update_stats_for_all_rows_column(br, r);
        }
        inc
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_idx: usize,
    ) -> i64 {
        if self.limit_reached() {
            return 0;
        }
        let mut inc = 0i64;
        let mc = get_matching_columns(br, &self.field_filters.clone());
        for r in mc {
            inc += self.update_stats_for_row_column(br, r, row_idx);
        }
        inc
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        if self.limit_reached() {
            return;
        }
        let src = other
            .as_any()
            .downcast_ref::<StatsUniqValuesProcessor>()
            .expect("merge_state: other must be a StatsUniqValuesProcessor");
        // PORT NOTE: Go postpones merging very large maps by moving them into
        // `ms`; the immutable `other` forces a clone here (same outcome).
        if src.m.len() > 100_000 {
            self.ms.push(src.m.clone());
            return;
        }
        for k in &src.m {
            self.m.insert(k.clone());
        }
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let items = self.merge_items_parallel();
        marshal_var_uint64(dst, items.len() as u64);
        for v in &items {
            marshal_bytes(dst, v);
        }
    }

    fn import_state(&mut self, src: &[u8], stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (items_len, n) = unmarshal_var_uint64(src);
        if n <= 0 {
            return Err("cannot unmarshal itemsLen".to_string());
        }
        let mut src = &src[n as usize..];
        if items_len > src.len() as u64 {
            return Err(format!(
                "too big itemsLen={items_len}; it mustn't exceed {}",
                src.len()
            ));
        }

        let mut m: HashSet<Vec<u8>> = HashSet::with_capacity(items_len as usize);
        let mut state_size = 0i64;
        for _ in 0..items_len {
            let (v, nn) = unmarshal_bytes(src);
            if nn <= 0 {
                return Err("cannot unmarshal item".to_string());
            }
            let v = v.unwrap();
            src = &src[nn as usize..];
            state_size += SIZE_OF_STRING + v.len() as i64;
            m.insert(v.to_vec());
            if need_stop(stop) {
                return Ok(0);
            }
        }
        self.m = m;
        if !src.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left; len(tail)={}",
                src.len()
            ));
        }
        Ok(state_size)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let mut items = self.merge_items_parallel();
        if self.limit > 0 && items.len() as u64 > self.limit {
            items.truncate(self.limit as usize);
        }
        marshal_json_array(dst, &items);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: TestParseStatsUniqValuesSuccess/Failure (parser) and
    // TestStatsUniqValues (expectPipeResults) are deferred until the stats
    // parser and pipe_stats are ported.

    fn strset(items: &[&str]) -> HashSet<Vec<u8>> {
        items.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn test_sort_strings() {
        fn f(s: &str, expected: &str) {
            let mut a: Vec<Vec<u8>> = if s.is_empty() {
                vec![Vec::new()]
            } else {
                s.split(',').map(|p| p.as_bytes().to_vec()).collect()
            };
            sort_strings(&mut a);
            let joined: Vec<&str> = a
                .iter()
                .map(|v| esl_common::bytesutil::to_unsafe_string(v))
                .collect();
            assert_eq!(joined.join(","), expected);
        }

        f("", "");
        f("1", "1");
        f("foo,bar,baz", "bar,baz,foo");
        f("100ms,1.5s,1.23s", "100ms,1.23s,1.5s");
        f("10KiB,10KB,5.34K", "5.34K,10KB,10KiB");
        f("v1.10.9,v1.10.10,v1.9.0", "v1.9.0,v1.10.9,v1.10.10");
        f("10s,123,100M", "123,100M,10s");
    }

    fn new_processor() -> StatsUniqValuesProcessor {
        StatsUniqValuesProcessor {
            concurrency: 2,
            ..Default::default()
        }
    }

    fn check(sup: &StatsUniqValuesProcessor, data_len_expected: usize) {
        let mut data = Vec::new();
        sup.export_state(&mut data, None);
        assert_eq!(data.len(), data_len_expected, "unexpected dataLen");

        let mut sup2 = new_processor();
        sup2.import_state(&data, None).unwrap();

        let items_expected = sup.merge_items_parallel();
        let items = sup2.merge_items_parallel();
        assert_eq!(items, items_expected, "unexpected state imported");
    }

    #[test]
    fn test_stats_uniq_values_export_import_state() {
        // empty state
        let sup = new_processor();
        check(&sup, 1);

        // non-empty m
        let mut sup = new_processor();
        sup.m = strset(&["foo", "bar", "baz"]);
        check(&sup, 13);

        // non-empty ms
        let mut sup = new_processor();
        sup.ms = vec![strset(&["foo", "bar", "baz"]), strset(&["foo", "aa", ""])];
        check(&sup, 17);
    }
}

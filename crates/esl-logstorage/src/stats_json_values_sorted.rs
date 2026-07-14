//! Port of `lib/logstorage/stats_json_values_sorted.go` — the sorted processor
//! for `json_values(...) sort by (...)` without a limit.
//!
//! Shares [`StatsJSONValues`], [`BySortField`], [`marshal_json_values`] and
//! [`get_matching_columns`] with the base module. The entry type, the
//! `stats_json_values_less` comparator and the state (un)marshaling helpers
//! defined here are reused by the `topk` variant.
//!
//! See `crate::stats_json_values` for the shared allocator / captured-config /
//! parser PORT NOTEs.

use std::any::Any;
use std::cmp::Ordering;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;
use esl_common::stringsutil::less_natural;

use crate::block_result::{BlockResult, ColRef};
use crate::rows::{Field, marshal_fields_to_json};
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_json_values::{BySortField, get_matching_columns, marshal_json_values};
use crate::values_encoder::{
    try_parse_bytes, try_parse_duration, try_parse_float64, try_parse_int64,
    try_parse_timestamp_rfc3339_nano, try_parse_uint64,
};

/// A collected value together with the field values used for sorting it.
///
/// Port of Go's `statsJSONValuesSortedEntry`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StatsJSONValuesSortedEntry {
    /// The JSON-encoded value itself (raw bytes; log values are arbitrary bytes).
    pub(crate) value: Vec<u8>,

    /// Values for the sort fields, used for sorting.
    pub(crate) sort_values: Vec<Vec<u8>>,
}

impl StatsJSONValuesSortedEntry {
    /// PORT NOTE: Go's `sizeBytes` uses `unsafe.Sizeof`; this approximates the
    /// same accounting with `size_of`. The value is only used for memory-limit
    /// bookkeeping and is not asserted by tests.
    pub(crate) fn size_bytes(&self) -> i64 {
        std::mem::size_of::<Self>() as i64
            + self.value.len() as i64
            + std::mem::size_of::<Vec<u8>>() as i64 * self.sort_values.capacity() as i64
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(value: &str, sort_values: Vec<String>) -> Self {
        Self {
            value: value.as_bytes().to_vec(),
            sort_values: sort_values.into_iter().map(String::into_bytes).collect(),
        }
    }
}

/// Port of Go's `newStatsJSONValuesSortedEntry`.
pub(crate) fn new_stats_json_values_sorted_entry(
    br: &mut BlockResult,
    cs: &[ColRef],
    sort_values: &[Vec<u8>],
    row_idx: usize,
) -> StatsJSONValuesSortedEntry {
    let fields: Vec<Field> = cs
        .iter()
        .map(|&c| {
            let name = br.column_name(c).to_vec();
            let value = br.column_get_value_at_row(c, row_idx).to_vec();
            Field { name, value }
        })
        .collect();

    let mut buf = Vec::new();
    marshal_fields_to_json(&mut buf, &fields);

    StatsJSONValuesSortedEntry {
        value: buf,
        sort_values: sort_values.to_vec(),
    }
}

/// Port of Go's `statsJSONValuesSortedProcessor`.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct StatsJSONValuesSortedProcessor {
    pub(crate) sort_fields_len: usize,

    pub(crate) entries: Vec<StatsJSONValuesSortedEntry>,

    sort_columns: Vec<Vec<Vec<u8>>>,
    sort_values_buf: Vec<Vec<u8>>,

    // Captured config (see `crate::stats_json_values` docs).
    pub(crate) field_filters: Vec<Vec<u8>>,
    pub(crate) sort_fields: Vec<BySortField>,
}

impl StatsJSONValuesSortedProcessor {
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

        let e = new_stats_json_values_sorted_entry(br, cs, &self.sort_values_buf, row_idx);
        let delta = e.size_bytes() + std::mem::size_of::<StatsJSONValuesSortedEntry>() as i64;
        self.entries.push(e);
        delta
    }
}

impl StatsProcessor for StatsJSONValuesSortedProcessor {
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
            .downcast_ref::<StatsJSONValuesSortedProcessor>()
            .expect("merge_state: other must be a StatsJSONValuesSortedProcessor");
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

/// Total ordering derived from [`stats_json_values_less`], for sorting.
pub(crate) fn less_to_ordering(sfs: &[BySortField], a: &[Vec<u8>], b: &[Vec<u8>]) -> Ordering {
    if stats_json_values_less(sfs, a, b) {
        Ordering::Less
    } else if stats_json_values_less(sfs, b, a) {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

/// Port of Go's `statsJSONValuesLess`.
pub(crate) fn stats_json_values_less(sfs: &[BySortField], a: &[Vec<u8>], b: &[Vec<u8>]) -> bool {
    for (i, sf) in sfs.iter().enumerate() {
        let (sa, sb) = (&a[i], &b[i]);
        if sa == sb {
            continue;
        }
        if less_bytes(sa, sb) {
            return !sf.is_desc;
        }
        return sf.is_desc;
    }
    false
}

/// Byte-native wrapper around [`less_string`]: valid-UTF-8 values order like
/// [`less_string`]; invalid UTF-8 fails every parse (as in Go) and falls back
/// to plain byte ordering, which equals Go string ordering.
fn less_bytes(a: &[u8], b: &[u8]) -> bool {
    if a == b {
        return false;
    }
    match (std::str::from_utf8(a), std::str::from_utf8(b)) {
        (Ok(sa), Ok(sb)) => less_string(sa, sb),
        _ => a < b,
    }
}

/// Port of Go's `lessString` (`pipe_sort_topk.go`).
fn less_string(a: &str, b: &str) -> bool {
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

/// PORT NOTE: duplicates `block_result.rs`'s private `try_parse_number` (the
/// same acknowledged duplication as `filter_range.rs`), until it is promoted to
/// a shared `pub(crate)` location.
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

/// Port of Go's `statsJSONValuesSortedMarshalState`.
pub(crate) fn stats_json_values_sorted_marshal_state(
    dst: &mut Vec<u8>,
    entries: &[StatsJSONValuesSortedEntry],
) {
    encoding::marshal_var_uint64(dst, entries.len() as u64);
    for e in entries {
        encoding::marshal_bytes(dst, &e.value);
        for v in &e.sort_values {
            encoding::marshal_bytes(dst, v);
        }
    }
}

/// Port of Go's `statsJSONValuesSortedUnmarshalState`.
pub(crate) fn stats_json_values_sorted_unmarshal_state(
    src: &[u8],
    sort_fields_len: usize,
) -> Result<(Vec<StatsJSONValuesSortedEntry>, i64), String> {
    let (entries_len, n) = encoding::unmarshal_var_uint64(src);
    if n <= 0 {
        return Err("cannot unmarshal entriesLen".to_string());
    }
    let mut src = &src[n as usize..];

    let mut entries = Vec::with_capacity(entries_len as usize);
    let mut state_size_increase =
        std::mem::size_of::<StatsJSONValuesSortedEntry>() as i64 * entries_len as i64;

    for _ in 0..entries_len {
        let (v, n) = encoding::unmarshal_bytes(src);
        let v = match v {
            Some(v) if n > 0 => v,
            _ => return Err("cannot unmarshal value".to_string()),
        };
        src = &src[n as usize..];
        let value = v.to_vec();

        let mut sort_values = Vec::with_capacity(sort_fields_len);
        for _ in 0..sort_fields_len {
            let (v, n) = encoding::unmarshal_bytes(src);
            let v = match v {
                Some(v) if n > 0 => v,
                _ => return Err("cannot unmarshal sort value".to_string()),
            };
            src = &src[n as usize..];
            sort_values.push(v.to_vec());
        }

        let e = StatsJSONValuesSortedEntry { value, sort_values };
        state_size_increase += e.size_bytes();
        entries.push(e);
    }

    if !src.is_empty() {
        return Err(format!(
            "unexpected tail left after unmarshaling values; len(tail)={}",
            src.len()
        ));
    }

    Ok((entries, state_size_increase))
}

// PORT NOTE: `parseStatsJSONValues` (which builds `statsJSONValuesSortedProcessor`
// via the lexer) is deferred until the parser is ported. Construction is
// exercised through `StatsJSONValues::new_stats_processor`.

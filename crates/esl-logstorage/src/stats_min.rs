//! Port of `stats_min.go`: the `min(...)` stats function.
//!
//! This module also hosts the small helpers shared across the min/max/quantile
//! stats ports (`less_string`, `field_names_string`, `get_matching_columns`),
//! which live in `pipe_sort_topk.go`, `pipe_stats.go` and `block_result.go` in
//! the Go source. They are `pub(crate)` here so the sibling stats modules reuse
//! a single implementation.
//!
//! PORT NOTE — capturing config in the processor. Go's `statsProcessor` methods
//! receive the `statsFunc` and downcast it (`sf.(*statsMin)`). The frozen
//! `crate::stats::StatsFunc` trait has no `as_any`, so each processor instead
//! captures the config it needs (here: `field_filters`) at construction time.
//! The `sf` parameters are therefore unused.
//!
//! PORT NOTE — column fast paths. Go's `updateStateForColumn` uses the column's
//! encoded min bound plus `blockResult.isFull()` / `getMinTimestamp()` as
//! optimizations, then falls back to scanning the column's decoded values for a
//! non-full block. Those internals are private to `block_result.rs`, and every
//! block result constructible today (pipe-backed) is non-full, so this port
//! scans the decoded values (`column_get_values`) directly. The computed min is
//! identical.

use std::any::Any;
use std::sync::atomic::AtomicBool;

use esl_common::encoding;
use esl_common::stringsutil;

use crate::block_result::{BlockResult, ColRef};
use crate::parser::quote_field_filter_if_needed;
use crate::prefix_filter::{self, Filter};
use crate::stats::{StatsFunc, StatsProcessor};
use crate::values_encoder::{
    try_parse_bytes, try_parse_duration, try_parse_float64, try_parse_int64,
    try_parse_timestamp_rfc3339_nano, try_parse_uint64,
};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Port of `lessString` (`pipe_sort_topk.go`). Orders numeric-looking strings
/// numerically and falls back to natural string ordering.
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
    stringsutil::less_natural(a, b)
}

/// Byte-native wrapper around [`less_string`] for raw log field values (Go
/// strings are arbitrary bytes). Valid-UTF-8 values order exactly like
/// [`less_string`]; invalid UTF-8 fails every numeric parse (as in Go) and
/// falls back to plain byte ordering, which matches Go string ordering.
pub(crate) fn less_bytes(a: &[u8], b: &[u8]) -> bool {
    if a == b {
        return false;
    }
    match (std::str::from_utf8(a), std::str::from_utf8(b)) {
        (Ok(sa), Ok(sb)) => less_string(sa, sb),
        _ => a < b,
    }
}

/// Port of `tryParseNumber` (`block_result.go`).
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

/// Port of `strconv.ParseInt(s, 0, 64)` base detection.
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

/// Port of `isLikelyNumber` (`block_result.go`).
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

/// Port of `fieldNamesString` (`pipe_stats.go`).
pub(crate) fn field_names_string(fields: &[Vec<u8>]) -> String {
    fields
        .iter()
        .map(|f| quote_field_filter_if_needed(f))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Port of `isSingleField` (`block_result.go`).
pub(crate) fn is_single_field(filters: &[Vec<u8>]) -> bool {
    filters.len() == 1 && !prefix_filter::is_wildcard_filter(&filters[0])
}

/// Port of `getMatchingColumns` / `getMatchingColumnsSlow` (`block_result.go`),
/// returning the matching columns as [`ColRef`] handles.
pub(crate) fn get_matching_columns(br: &mut BlockResult, filters: &[Vec<u8>]) -> Vec<ColRef> {
    if is_single_field(filters) {
        return vec![br.get_column_by_name(&filters[0])];
    }

    let cols = br.get_columns();
    let mut dst = Vec::new();
    for &c in &cols {
        if prefix_filter::match_filters(filters, br.column_name(c)) {
            dst.push(c);
        }
    }

    // Add empty columns for non-wildcard filters not matching a real column.
    for f in filters {
        if prefix_filter::is_wildcard_filter(f) {
            continue;
        }
        let mut need_empty = true;
        for &c in &cols {
            if br.column_name(c) == f.as_slice() {
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

// ---------------------------------------------------------------------------
// statsMin
// ---------------------------------------------------------------------------

/// Port of `statsMin`.
pub(crate) struct StatsMin {
    field_filters: Vec<Vec<u8>>,
}

/// Port of `parseStatsMin` (constructor only; the lexer is supplied by the
/// future parser). Empty filters default to `["*"]`, matching
/// `parseStatsFuncFieldFilters`.
pub(crate) fn new_stats_min(mut field_filters: Vec<Vec<u8>>) -> StatsMin {
    if field_filters.is_empty() {
        field_filters.push(b"*".to_vec());
    }
    StatsMin { field_filters }
}

impl StatsFunc for StatsMin {
    fn to_string(&self) -> String {
        format!("min({})", field_names_string(&self.field_filters))
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.field_filters);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsMinProcessor {
            field_filters: self.field_filters.clone(),
            min: Vec::new(),
            has_items: false,
        })
    }
}

/// Port of `statsMinProcessor`.
pub(crate) struct StatsMinProcessor {
    field_filters: Vec<Vec<u8>>,
    min: Vec<u8>,
    has_items: bool,
}

impl StatsMinProcessor {
    fn needs_update_state(&self, v: &[u8]) -> bool {
        !self.has_items || less_bytes(v, &self.min)
    }

    fn set_state(&mut self, v: &[u8]) {
        self.min = v.to_vec();
        self.has_items = true;
    }

    fn update_state_string(&mut self, v: &[u8]) {
        if self.needs_update_state(v) {
            self.set_state(v);
        }
    }
}

impl StatsProcessor for StatsMinProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        let min_len = self.min.len();
        let cols = get_matching_columns(br, &self.field_filters);
        for c in cols {
            let values = br.column_get_values(c);
            for v in values {
                if self.needs_update_state(v) {
                    self.set_state(v);
                }
            }
        }
        self.min.len() as i64 - min_len as i64
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_index: usize,
    ) -> i64 {
        let min_len = self.min.len();
        let cols = get_matching_columns(br, &self.field_filters);
        for c in cols {
            let v = br.column_get_value_at_row(c, row_index);
            if self.needs_update_state(v) {
                self.set_state(v);
            }
        }
        // PORT NOTE: Go returns `minLen - len(smp.min)` here (opposite sign to
        // update_stats_for_all_rows); kept verbatim for fidelity.
        min_len as i64 - self.min.len() as i64
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        let src = other
            .as_any()
            .downcast_ref::<StatsMinProcessor>()
            .expect("merge_state: other must be StatsMinProcessor");
        if src.has_items {
            self.update_state_string(&src.min);
        }
    }

    fn export_state(&self, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        if !self.has_items {
            dst.push(0);
            return;
        }
        dst.push(1);
        encoding::marshal_bytes(dst, &self.min);
    }

    fn import_state(&mut self, src: &[u8], _stop: Option<&AtomicBool>) -> Result<i64, String> {
        if src.is_empty() {
            return Err("missing `hasItems`".to_string());
        }
        self.has_items = src[0] == 1;
        let mut src = &src[1..];

        if self.has_items {
            let (min_value, n) = encoding::unmarshal_bytes(src);
            if n <= 0 {
                return Err("cannot unmarshal min value".to_string());
            }
            self.min = min_value.unwrap_or_default().to_vec();
            src = &src[n as usize..];
        } else {
            self.min = Vec::new();
        }

        if !src.is_empty() {
            return Err(format!(
                "unexpected tail left after decoding min value; len(tail)={}",
                src.len()
            ));
        }

        Ok(self.min.len() as i64)
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        dst.extend_from_slice(&self.min);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// PORT NOTE — deferred tests. `TestParseStatsMinSuccess` / `...Failure` need the
// LogsQL lexer, and `TestStatsMin` needs `expectPipeResults` (the `| stats`
// pipe). Both are deferred until the parser / pipe_stats land. The pure
// computation is covered below.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    // PORT NOTE: the Go `expectPipeResults` harness routes rows with different
    // field sets into separate blocks (so a missing field is never padded with
    // an empty value inside a block). This helper mirrors that by feeding each
    // group of same-field rows as its own `BlockResult`.
    fn run_min(filters: &[&str], blocks: &[Vec<Vec<Field>>]) -> String {
        let sf = new_stats_min(filters.iter().map(|s| s.as_bytes().to_vec()).collect());
        let mut sp = sf.new_stats_processor();
        for block in blocks {
            let mut br = BlockResult::default();
            br.must_init_from_rows(block);
            sp.update_stats_for_all_rows(&sf, &mut br);
        }
        let mut dst = Vec::new();
        sp.finalize_stats(&sf, &mut dst, None);
        String::from_utf8(dst).unwrap()
    }

    // Rows from TestStatsMin, one block per distinct field set.
    fn sample_blocks() -> Vec<Vec<Vec<Field>>> {
        vec![
            vec![vec![field("_msg", "abc"), field("a", "2"), field("b", "3")]],
            vec![vec![field("_msg", "def"), field("a", "1")]],
            vec![vec![field("a", "3"), field("b", "54")]],
        ]
    }

    #[test]
    fn test_less_string_numeric() {
        assert!(less_string("1", "2"));
        assert!(!less_string("2", "1"));
        assert!(less_string("1", "abc"));
        assert!(!less_string("abc", "1"));
        assert!(!less_string("2", "2"));
        assert!(!less_string("10", "9")); // both ints -> 10 < 9 is false
    }

    #[test]
    fn test_stats_min_wildcard() {
        // min(*) as x -> "1"
        assert_eq!(run_min(&["*"], &sample_blocks()), "1");
    }

    #[test]
    fn test_stats_min_single_field() {
        // min(a) -> "1"; min(b) -> "" (a block lacks b); min(c) -> "" (no block has c)
        assert_eq!(run_min(&["a"], &sample_blocks()), "1");
        assert_eq!(run_min(&["b"], &sample_blocks()), "");
        assert_eq!(run_min(&["c"], &sample_blocks()), "");
    }

    #[test]
    fn test_stats_min_export_import_roundtrip() {
        let sf = new_stats_min(vec![b"a".to_vec()]);
        let mut sp = sf.new_stats_processor();
        let mut br = BlockResult::default();
        br.must_init_from_rows(&[vec![field("a", "5")], vec![field("a", "2")]]);
        sp.update_stats_for_all_rows(&sf, &mut br);

        let mut buf = Vec::new();
        sp.export_state(&mut buf, None);

        let mut sp2 = sf.new_stats_processor();
        sp2.import_state(&buf, None).unwrap();
        let mut dst = Vec::new();
        sp2.finalize_stats(&sf, &mut dst, None);
        assert_eq!(String::from_utf8(dst).unwrap(), "2");
    }

    #[test]
    fn test_stats_min_merge() {
        let sf = new_stats_min(vec![b"a".to_vec()]);
        let mut a = sf.new_stats_processor();
        let mut b = sf.new_stats_processor();

        let mut br1 = BlockResult::default();
        br1.must_init_from_rows(&[vec![field("a", "7")], vec![field("a", "5")]]);
        a.update_stats_for_all_rows(&sf, &mut br1);

        let mut br2 = BlockResult::default();
        br2.must_init_from_rows(&[vec![field("a", "3")], vec![field("a", "9")]]);
        b.update_stats_for_all_rows(&sf, &mut br2);

        a.merge_state(&sf, b.as_ref());
        let mut dst = Vec::new();
        a.finalize_stats(&sf, &mut dst, None);
        assert_eq!(String::from_utf8(dst).unwrap(), "3");
    }
}

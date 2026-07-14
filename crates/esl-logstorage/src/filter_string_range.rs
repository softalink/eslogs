//! Port of EsLogs `lib/logstorage/filter_string_range.go`.
//!
//! `FilterStringRange` matches the string range `[min_value..max_value)` — the
//! min is included, the max is excluded.
//!
//! The range bounds are raw bytes, like Go strings, so the parser's
//! `MAX_STRING_RANGE_VALUE` sentinel is Go's exact
//! `maxStringRangeValue = string([]byte{255,255,255,255})`.

use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::FieldFilter;
use crate::filter_generic::{FilterGeneric, clone_column_header, new_filter_generic};
use crate::filter_phrase::{
    apply_to_block_result_generic, match_encoded_values_dict, to_float64_string, to_ipv4_string,
    to_timestamp_iso8601_string, visit_values,
};
use crate::filter_prefix::{
    to_int64_string, to_uint8_string, to_uint16_string, to_uint32_string, to_uint64_string,
};
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::ValueType;

// ---------------------------------------------------------------------------
// FilterStringRange
// ---------------------------------------------------------------------------

/// `FilterStringRange` matches the given string range `[min_value..max_value)`.
///
/// Example LogsQL: `string_range(minValue, maxValue)`.
pub(crate) struct FilterStringRange {
    /// Raw-byte bounds (Go strings are arbitrary bytes; the `>`/`>=` forms use
    /// the non-UTF-8 `MAX_STRING_RANGE_VALUE` sentinel as the max).
    pub(crate) min_value: Vec<u8>,
    pub(crate) max_value: Vec<u8>,
    pub(crate) string_repr: String,
}

/// Builds a string-range filter for `field_name`.
pub(crate) fn new_filter_string_range(
    field_name: &str,
    min_value: impl AsRef<[u8]>,
    max_value: impl AsRef<[u8]>,
    string_repr: &str,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterStringRange {
            min_value: min_value.as_ref().to_vec(),
            max_value: max_value.as_ref().to_vec(),
            string_repr: string_repr.to_string(),
        }),
    )
}

impl FieldFilter for FilterStringRange {
    fn to_string(&self) -> String {
        self.string_repr.clone()
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_string_range(v, &self.min_value, &self.max_value)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let min_value = self.min_value.clone();
        let max_value = self.max_value.clone();

        if min_value > max_value {
            bm.reset_bits();
            return;
        }

        apply_to_block_result_generic(br, bm, field_name, "", |v, _| {
            match_string_range(v, &min_value, &max_value)
        });
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let min_value = self.min_value.clone();
        let max_value = self.max_value.clone();

        if min_value > max_value {
            bm.reset_bits();
            return;
        }

        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_string_range(&v, &min_value, &max_value) {
                bm.reset_bits();
            }
            return;
        }

        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                if !match_string_range(b"", &min_value, &max_value) {
                    bm.reset_bits();
                }
                return;
            }
        };

        match ch.value_type {
            ValueType::STRING => match_string_by_string_range(bs, &ch, bm, &min_value, &max_value),
            ValueType::DICT => {
                match_values_dict_by_string_range(bs, &ch, bm, &min_value, &max_value)
            }
            ValueType::UINT8 => match_uint8_by_string_range(bs, &ch, bm, &min_value, &max_value),
            ValueType::UINT16 => match_uint16_by_string_range(bs, &ch, bm, &min_value, &max_value),
            ValueType::UINT32 => match_uint32_by_string_range(bs, &ch, bm, &min_value, &max_value),
            ValueType::UINT64 => match_uint64_by_string_range(bs, &ch, bm, &min_value, &max_value),
            ValueType::INT64 => match_int64_by_string_range(bs, &ch, bm, &min_value, &max_value),
            ValueType::FLOAT64 => {
                match_float64_by_string_range(bs, &ch, bm, &min_value, &max_value)
            }
            ValueType::IPV4 => match_ipv4_by_string_range(bs, &ch, bm, &min_value, &max_value),
            ValueType::TIMESTAMP_ISO8601 => {
                match_timestamp_iso8601_by_string_range(bs, &ch, bm, &min_value, &max_value)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// block_search helpers (Go filter_string_range.go)
// ---------------------------------------------------------------------------

fn match_timestamp_iso8601_by_string_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: &[u8],
    max_value: &[u8],
) {
    if min_value > b"9" as &[u8] || max_value < b"0" as &[u8] {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_timestamp_iso8601_string(&part_path, &mut bb, v);
        match_string_range(&bb, min_value, max_value)
    });
}

fn match_ipv4_by_string_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: &[u8],
    max_value: &[u8],
) {
    if min_value > b"9" as &[u8] || max_value < b"0" as &[u8] {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_ipv4_string(&part_path, &mut bb, v);
        match_string_range(&bb, min_value, max_value)
    });
}

fn match_float64_by_string_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: &[u8],
    max_value: &[u8],
) {
    if min_value > b"9" as &[u8] || max_value < b"+" as &[u8] {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_float64_string(&part_path, &mut bb, v);
        match_string_range(&bb, min_value, max_value)
    });
}

fn match_values_dict_by_string_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: &[u8],
    max_value: &[u8],
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_string_range(v, min_value, max_value)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

fn match_string_by_string_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: &[u8],
    max_value: &[u8],
) {
    visit_values(bs, ch, bm, |v| match_string_range(v, min_value, max_value));
}

fn match_uint8_by_string_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: &[u8],
    max_value: &[u8],
) {
    if min_value > b"9" as &[u8] || max_value < b"0" as &[u8] {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint8_string(&part_path, &mut bb, v);
        match_string_range(&bb, min_value, max_value)
    });
}

fn match_uint16_by_string_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: &[u8],
    max_value: &[u8],
) {
    if min_value > b"9" as &[u8] || max_value < b"0" as &[u8] {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint16_string(&part_path, &mut bb, v);
        match_string_range(&bb, min_value, max_value)
    });
}

fn match_uint32_by_string_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: &[u8],
    max_value: &[u8],
) {
    if min_value > b"9" as &[u8] || max_value < b"0" as &[u8] {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint32_string(&part_path, &mut bb, v);
        match_string_range(&bb, min_value, max_value)
    });
}

fn match_uint64_by_string_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: &[u8],
    max_value: &[u8],
) {
    if min_value > b"9" as &[u8] || max_value < b"0" as &[u8] {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint64_string(&part_path, &mut bb, v);
        match_string_range(&bb, min_value, max_value)
    });
}

fn match_int64_by_string_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: &[u8],
    max_value: &[u8],
) {
    if (min_value != b"-" && min_value > b"9" as &[u8])
        || (max_value != b"-" && max_value < b"0" as &[u8])
    {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_int64_string(&part_path, &mut bb, v);
        match_string_range(&bb, min_value, max_value)
    });
}

/// Port of Go `matchStringRange`.
///
/// PORT NOTE: Go compares plain strings by byte order; `&[u8]` `Ord` is the
/// same byte-wise lexicographic order, so `>=`/`<` match exactly.
pub(crate) fn match_string_range(s: &[u8], min_value: &[u8], max_value: &[u8]) -> bool {
    s >= min_value && s < max_value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_string_range() {
        assert!(match_string_range(b"abc", b"abc", b"abd"));
        assert!(!match_string_range(b"abd", b"abc", b"abd")); // max excluded
        assert!(!match_string_range(b"abb", b"abc", b"abd"));
        assert!(match_string_range(b"10", b"0", b"9a"));
        assert!(!match_string_range(b"", b"a", b"z"));
        // included min, excluded max
        assert!(match_string_range(b"a", b"a", b"b"));
        assert!(!match_string_range(b"b", b"a", b"b"));
    }

    #[test]
    fn test_max_string_range_sentinel_matches_high_bytes() {
        use crate::parser::MAX_STRING_RANGE_VALUE;
        // Go's maxStringRangeValue is 0xFF x4: a stored raw-byte value starting
        // with a byte in 0xF5..0xFF (invalid UTF-8, now representable) is below
        // the sentinel and therefore matches `foo:>bar` like in Go.
        assert!(match_string_range(
            b"\xfa\x01",
            b"bar",
            MAX_STRING_RANGE_VALUE
        ));
        assert!(match_string_range(b"\xfe", b"", MAX_STRING_RANGE_VALUE));
        // The sentinel itself is excluded (max-exclusive range).
        assert!(!match_string_range(
            b"\xff\xff\xff\xff",
            b"",
            MAX_STRING_RANGE_VALUE
        ));
    }
}

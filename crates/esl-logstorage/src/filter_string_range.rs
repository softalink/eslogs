//! Port of EsLogs `lib/logstorage/filter_string_range.go`.
//!
//! `FilterStringRange` matches the string range `[min_value..max_value)` — the
//! min is included, the max is excluded.
//!
//! PORT NOTE: Go's package-level `maxStringRangeValue = string([]byte{255,255,
//! 255,255})` is deferred to the parser port (its only consumer, `parser.go`);
//! it is not valid UTF-8 and is unused by the filter logic here.

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
    pub(crate) min_value: String,
    pub(crate) max_value: String,
    pub(crate) string_repr: String,
}

/// Builds a string-range filter for `field_name`.
pub(crate) fn new_filter_string_range(
    field_name: &str,
    min_value: &str,
    max_value: &str,
    string_repr: &str,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterStringRange {
            min_value: min_value.to_string(),
            max_value: max_value.to_string(),
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
    min_value: &str,
    max_value: &str,
) {
    if min_value > "9" || max_value < "0" {
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
    min_value: &str,
    max_value: &str,
) {
    if min_value > "9" || max_value < "0" {
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
    min_value: &str,
    max_value: &str,
) {
    if min_value > "9" || max_value < "+" {
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
    min_value: &str,
    max_value: &str,
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
    min_value: &str,
    max_value: &str,
) {
    visit_values(bs, ch, bm, |v| match_string_range(v, min_value, max_value));
}

fn match_uint8_by_string_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: &str,
    max_value: &str,
) {
    if min_value > "9" || max_value < "0" {
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
    min_value: &str,
    max_value: &str,
) {
    if min_value > "9" || max_value < "0" {
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
    min_value: &str,
    max_value: &str,
) {
    if min_value > "9" || max_value < "0" {
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
    min_value: &str,
    max_value: &str,
) {
    if min_value > "9" || max_value < "0" {
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
    min_value: &str,
    max_value: &str,
) {
    if (min_value != "-" && min_value > "9") || (max_value != "-" && max_value < "0") {
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
pub(crate) fn match_string_range(s: &[u8], min_value: &str, max_value: &str) -> bool {
    s >= min_value.as_bytes() && s < max_value.as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_string_range() {
        assert!(match_string_range(b"abc", "abc", "abd"));
        assert!(!match_string_range(b"abd", "abc", "abd")); // max excluded
        assert!(!match_string_range(b"abb", "abc", "abd"));
        assert!(match_string_range(b"10", "0", "9a"));
        assert!(!match_string_range(b"", "a", "z"));
        // included min, excluded max
        assert!(match_string_range(b"a", "a", "b"));
        assert!(!match_string_range(b"b", "a", "b"));
    }
}

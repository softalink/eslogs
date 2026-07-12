//! Port of EsLogs `lib/logstorage/filter_len_range.go`.
//!
//! `FilterLenRange` matches field values whose length (in runes) is within
//! `[min_len, max_len]`.

use esl_common::bytesutil::to_unsafe_string;
use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::FieldFilter;
use crate::filter_generic::{FilterGeneric, clone_column_header, new_filter_generic};
use crate::filter_phrase::{
    match_column_by_generic, match_encoded_values_dict, to_float64_string, to_ipv4_string,
    visit_values,
};
use crate::filter_prefix::{
    to_int64_string, to_uint8_string, to_uint16_string, to_uint32_string, to_uint64_string,
};
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::{ValueType, marshal_int64_string, marshal_uint64_string};

/// Length of the ISO8601 timestamp string `2006-01-02T15:04:05.000Z`.
const ISO8601_TIMESTAMP_LEN: u64 = 24;

// ---------------------------------------------------------------------------
// FilterLenRange
// ---------------------------------------------------------------------------

/// `FilterLenRange` matches field values with a length in `[min_len, max_len]`.
///
/// Example LogsQL: `len_range(10, 20)`.
pub(crate) struct FilterLenRange {
    pub(crate) min_len: u64,
    pub(crate) max_len: u64,
    pub(crate) string_repr: String,
}

/// Builds a len-range filter for `field_name`.
pub(crate) fn new_filter_len_range(
    field_name: &str,
    min_len: u64,
    max_len: u64,
    string_repr: &str,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterLenRange {
            min_len,
            max_len,
            string_repr: string_repr.to_string(),
        }),
    )
}

impl FieldFilter for FilterLenRange {
    fn to_string(&self) -> String {
        format!("len_range{}", self.string_repr)
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_len_range(v, self.min_len, self.max_len)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let min_len = self.min_len;
        let max_len = self.max_len;

        if min_len > max_len {
            bm.reset_bits();
            return;
        }

        // PORT NOTE: Go's per-`valueType` block-result path prunes on type-specific
        // length bounds then matches the decoded string length. Since every path
        // ultimately compares the rune-length of the decoded value, the port
        // matches all non-const columns via `match_len_range` on the decoded rows
        // (identical result; the type-specific pruning is only a micro-opt, and
        // ISO8601 columns always render to a fixed 24-rune string).
        let r = br.get_column_by_name(field_name);
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0);
            if !match_len_range(v, min_len, max_len) {
                bm.reset_bits();
            }
            return;
        }
        match_column_by_generic(br, bm, r, "", &|v, _| match_len_range(v, min_len, max_len));
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let min_len = self.min_len;
        let max_len = self.max_len;

        if min_len > max_len {
            bm.reset_bits();
            return;
        }

        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_len_range(&v, min_len, max_len) {
                bm.reset_bits();
            }
            return;
        }

        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns.
                if !match_len_range("", min_len, max_len) {
                    bm.reset_bits();
                }
                return;
            }
        };

        match ch.value_type {
            ValueType::STRING => match_string_by_len_range(bs, &ch, bm, min_len, max_len),
            ValueType::DICT => match_values_dict_by_len_range(bs, &ch, bm, min_len, max_len),
            ValueType::UINT8 => match_uint8_by_len_range(bs, &ch, bm, min_len, max_len),
            ValueType::UINT16 => match_uint16_by_len_range(bs, &ch, bm, min_len, max_len),
            ValueType::UINT32 => match_uint32_by_len_range(bs, &ch, bm, min_len, max_len),
            ValueType::UINT64 => match_uint64_by_len_range(bs, &ch, bm, min_len, max_len),
            ValueType::INT64 => match_int64_by_len_range(bs, &ch, bm, min_len, max_len),
            ValueType::FLOAT64 => match_float64_by_len_range(bs, &ch, bm, min_len, max_len),
            ValueType::IPV4 => match_ipv4_by_len_range(bs, &ch, bm, min_len, max_len),
            ValueType::TIMESTAMP_ISO8601 => {
                match_timestamp_iso8601_by_len_range(bm, min_len, max_len)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// block_search helpers (Go filter_len_range.go)
// ---------------------------------------------------------------------------

fn match_timestamp_iso8601_by_len_range(bm: &mut Bitmap, min_len: u64, max_len: u64) {
    if min_len > ISO8601_TIMESTAMP_LEN || max_len < ISO8601_TIMESTAMP_LEN {
        bm.reset_bits();
    }
}

fn match_ipv4_by_len_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_len: u64,
    max_len: u64,
) {
    if min_len > "255.255.255.255".len() as u64 || max_len < "0.0.0.0".len() as u64 {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_ipv4_string(&part_path, &mut bb, v);
        match_len_range(to_unsafe_string(&bb), min_len, max_len)
    });
}

fn match_float64_by_len_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_len: u64,
    max_len: u64,
) {
    if min_len > 24 || max_len == 0 {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_float64_string(&part_path, &mut bb, v);
        match_len_range(to_unsafe_string(&bb), min_len, max_len)
    });
}

fn match_values_dict_by_len_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_len: u64,
    max_len: u64,
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_len_range(v, min_len, max_len)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

fn match_string_by_len_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_len: u64,
    max_len: u64,
) {
    visit_values(bs, ch, bm, |v| {
        match_len_range(to_unsafe_string(v), min_len, max_len)
    });
}

fn match_uint8_by_len_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_len: u64,
    max_len: u64,
) {
    if min_len > 3 || max_len == 0 {
        bm.reset_bits();
        return;
    }
    if !match_min_max_value_len(ch, min_len, max_len) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint8_string(&part_path, &mut bb, v);
        match_len_range(to_unsafe_string(&bb), min_len, max_len)
    });
}

fn match_uint16_by_len_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_len: u64,
    max_len: u64,
) {
    if min_len > 5 || max_len == 0 {
        bm.reset_bits();
        return;
    }
    if !match_min_max_value_len(ch, min_len, max_len) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint16_string(&part_path, &mut bb, v);
        match_len_range(to_unsafe_string(&bb), min_len, max_len)
    });
}

fn match_uint32_by_len_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_len: u64,
    max_len: u64,
) {
    if min_len > 10 || max_len == 0 {
        bm.reset_bits();
        return;
    }
    if !match_min_max_value_len(ch, min_len, max_len) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint32_string(&part_path, &mut bb, v);
        match_len_range(to_unsafe_string(&bb), min_len, max_len)
    });
}

fn match_uint64_by_len_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_len: u64,
    max_len: u64,
) {
    if min_len > 20 || max_len == 0 {
        bm.reset_bits();
        return;
    }
    if !match_min_max_value_len(ch, min_len, max_len) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut bb = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint64_string(&part_path, &mut bb, v);
        match_len_range(to_unsafe_string(&bb), min_len, max_len)
    });
}

fn match_int64_by_len_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_len: u64,
    max_len: u64,
) {
    if min_len > 21 || max_len == 0 {
        bm.reset_bits();
        return;
    }

    let mut bb = Vec::new();
    marshal_int64_string(&mut bb, ch.min_value as i64);
    let mut maxv_len = bb.len();
    bb.clear();
    marshal_int64_string(&mut bb, ch.max_value as i64);
    if bb.len() > maxv_len {
        maxv_len = bb.len();
    }
    if (maxv_len as u64) < min_len {
        bm.reset_bits();
        return;
    }

    let part_path = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        to_int64_string(&part_path, &mut bb, v);
        match_len_range(to_unsafe_string(&bb), min_len, max_len)
    });
}

// ---------------------------------------------------------------------------
// Match helpers (Go filter_len_range.go)
// ---------------------------------------------------------------------------

/// Port of Go `matchLenRange` (length measured in unicode runes).
pub(crate) fn match_len_range(s: &str, min_len: u64, max_len: u64) -> bool {
    let s_len = s.chars().count() as u64;
    s_len >= min_len && s_len <= max_len
}

/// Port of Go `matchMinMaxValueLen`.
fn match_min_max_value_len(ch: &ColumnHeader, min_len: u64, max_len: u64) -> bool {
    let mut bb = Vec::new();
    marshal_uint64_string(&mut bb, ch.min_value);
    if max_len < bb.len() as u64 {
        return false;
    }
    bb.clear();
    marshal_uint64_string(&mut bb, ch.max_value);
    min_len <= bb.len() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_len_range() {
        assert!(match_len_range("", 0, 0));
        assert!(!match_len_range("", 1, 10));
        assert!(match_len_range("foo", 3, 3));
        assert!(match_len_range("foo", 1, 5));
        assert!(!match_len_range("foo", 4, 5));
        assert!(!match_len_range("foobar", 1, 5));
        // rune counting for multi-byte
        assert!(match_len_range("привет", 6, 6));
        assert!(!match_len_range("привет", 12, 12));
    }
}

//! Port of EsLogs `lib/logstorage/filter_range.go`.
//!
//! `FilterRange` matches values in the numeric range `[min_value..max_value]`.
//! This module also hosts the shared numeric-range conversion helpers
//! (`to_uint64_range` etc.), the `parse_math_number` helper, and the
//! `match_*_by_range` block-search helpers reused by `filter_ipv4_range` and
//! `filter_ipv6_range`.

use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::FieldFilter;
use crate::filter_generic::{FilterGeneric, clone_column_header, new_filter_generic};
use crate::filter_phrase::{match_column_by_generic, match_encoded_values_dict, visit_values};
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::{
    ValueType, unmarshal_float64, unmarshal_int64, unmarshal_ipv4, unmarshal_timestamp_iso8601,
    unmarshal_uint8, unmarshal_uint16, unmarshal_uint32, unmarshal_uint64,
};

// ---------------------------------------------------------------------------
// FilterRange
// ---------------------------------------------------------------------------

/// `FilterRange` matches the given range `[min_value..max_value]`.
///
/// Example LogsQL: `range(minValue, maxValue]`.
pub(crate) struct FilterRange {
    pub(crate) min_value: f64,
    pub(crate) max_value: f64,
    pub(crate) string_repr: String,
}

/// Builds a range filter for `field_name`.
pub(crate) fn new_filter_range(
    field_name: &str,
    min_value: f64,
    max_value: f64,
    string_repr: &str,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterRange {
            min_value,
            max_value,
            string_repr: string_repr.to_string(),
        }),
    )
}

impl FieldFilter for FilterRange {
    fn to_string(&self) -> String {
        self.string_repr.clone()
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_range(v, self.min_value, self.max_value)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let min_value = self.min_value;
        let max_value = self.max_value;

        if min_value > max_value {
            bm.reset_bits();
            return;
        }

        // PORT NOTE: Go's `applyToBlockResultByField` has per-`valueType` fast
        // paths that read the column's private `minValue`/`maxValue`/`dictValues`
        // for pruning. `BlockResult` does not expose those, so the port matches
        // the decoded per-row values via `match_range` (which parses the value).
        // The result is identical to Go's paths — `match_range` returns false for
        // values that fail to parse, mirroring Go's numeric/ipv4/time resets, and
        // `ceil`/`floor` range clamping only matters for the pruning micro-opt.
        let r = br.get_column_by_name(field_name);
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0);
            if !match_range(v, min_value, max_value) {
                bm.reset_bits();
            }
            return;
        }
        match_column_by_generic(br, bm, r, "", &|v, _| match_range(v, min_value, max_value));
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let min_value = self.min_value;
        let max_value = self.max_value;

        if min_value > max_value {
            bm.reset_bits();
            return;
        }

        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_range(&v, min_value, max_value) {
                bm.reset_bits();
            }
            return;
        }

        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns.
                bm.reset_bits();
                return;
            }
        };

        match ch.value_type {
            ValueType::STRING => match_string_by_range(bs, &ch, bm, min_value, max_value),
            ValueType::DICT => match_values_dict_by_range(bs, &ch, bm, min_value, max_value),
            ValueType::UINT8 => match_uint8_by_range(bs, &ch, bm, min_value, max_value),
            ValueType::UINT16 => match_uint16_by_range(bs, &ch, bm, min_value, max_value),
            ValueType::UINT32 => match_uint32_by_range(bs, &ch, bm, min_value, max_value),
            ValueType::UINT64 => match_uint64_by_range(bs, &ch, bm, min_value, max_value),
            ValueType::INT64 => match_int64_by_range(bs, &ch, bm, min_value, max_value),
            ValueType::FLOAT64 => match_float64_by_range(bs, &ch, bm, min_value, max_value),
            ValueType::IPV4 => {
                let (min_u32, max_u32) = to_uint32_range(min_value, max_value);
                match_ipv4_by_range(bs, &ch, bm, min_u32, max_u32);
            }
            ValueType::TIMESTAMP_ISO8601 => {
                match_timestamp_iso8601_by_range(bs, &ch, bm, min_value, max_value)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// block_search match helpers (Go filter_range.go). `pub(crate)`:
// `match_ipv4_by_range` is reused by filter_ipv4_range / filter_ipv6_range.
// ---------------------------------------------------------------------------

pub(crate) fn match_float64_by_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: f64,
    max_value: f64,
) {
    if min_value > f64::from_bits(ch.max_value) || max_value < f64::from_bits(ch.min_value) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        if v.len() != 8 {
            panicf!(
                "FATAL: {}: unexpected length for binary representation of floating-point number: got {}; want 8",
                part_path,
                v.len()
            );
        }
        let f = unmarshal_float64(v);
        f >= min_value && f <= max_value
    });
}

pub(crate) fn match_values_dict_by_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: f64,
    max_value: f64,
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_range(v, min_value, max_value)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

pub(crate) fn match_string_by_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: f64,
    max_value: f64,
) {
    visit_values(bs, ch, bm, |v| match_range(v, min_value, max_value));
}

pub(crate) fn match_uint8_by_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: f64,
    max_value: f64,
) {
    let (min_u, max_u) = to_uint64_range(min_value, max_value);
    if max_value < 0.0 || min_u > ch.max_value || max_u < ch.min_value {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        if v.len() != 1 {
            panicf!(
                "FATAL: {}: unexpected length for binary representation of uint8 number: got {}; want 1",
                part_path,
                v.len()
            );
        }
        let n = unmarshal_uint8(v) as u64;
        n >= min_u && n <= max_u
    });
}

pub(crate) fn match_uint16_by_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: f64,
    max_value: f64,
) {
    let (min_u, max_u) = to_uint64_range(min_value, max_value);
    if max_value < 0.0 || min_u > ch.max_value || max_u < ch.min_value {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        if v.len() != 2 {
            panicf!(
                "FATAL: {}: unexpected length for binary representation of uint16 number: got {}; want 2",
                part_path,
                v.len()
            );
        }
        let n = unmarshal_uint16(v) as u64;
        n >= min_u && n <= max_u
    });
}

pub(crate) fn match_uint32_by_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: f64,
    max_value: f64,
) {
    let (min_u, max_u) = to_uint64_range(min_value, max_value);
    if max_value < 0.0 || min_u > ch.max_value || max_u < ch.min_value {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        if v.len() != 4 {
            panicf!(
                "FATAL: {}: unexpected length for binary representation of uint32 number: got {}; want 4",
                part_path,
                v.len()
            );
        }
        let n = unmarshal_uint32(v) as u64;
        n >= min_u && n <= max_u
    });
}

pub(crate) fn match_uint64_by_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: f64,
    max_value: f64,
) {
    let (min_u, max_u) = to_uint64_range(min_value, max_value);
    if max_value < 0.0 || min_u > ch.max_value || max_u < ch.min_value {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        if v.len() != 8 {
            panicf!(
                "FATAL: {}: unexpected length for binary representation of uint64 number: got {}; want 8",
                part_path,
                v.len()
            );
        }
        let n = unmarshal_uint64(v);
        n >= min_u && n <= max_u
    });
}

pub(crate) fn match_int64_by_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: f64,
    max_value: f64,
) {
    let (min_i, max_i) = to_int64_range(min_value, max_value);
    if min_i > ch.max_value as i64 || max_i < ch.min_value as i64 {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        if v.len() != 8 {
            panicf!(
                "FATAL: {}: unexpected length for binary representation of int64 number; got {}; want 8",
                part_path,
                v.len()
            );
        }
        let n = unmarshal_int64(v);
        n >= min_i && n <= max_i
    });
}

pub(crate) fn match_timestamp_iso8601_by_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: f64,
    max_value: f64,
) {
    let (min_i, max_i) = to_int64_range(min_value, max_value);
    if max_value < 0.0 || min_i > ch.max_value as i64 || max_i < ch.min_value as i64 {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        if v.len() != 8 {
            panicf!(
                "FATAL: {}: unexpected length for binary representation of timestampISO8601: got {}; want 8",
                part_path,
                v.len()
            );
        }
        let n = unmarshal_timestamp_iso8601(v);
        n >= min_i && n <= max_i
    });
}

pub(crate) fn match_ipv4_by_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: u32,
    max_value: u32,
) {
    if ch.min_value > max_value as u64 || ch.max_value < min_value as u64 {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        if v.len() != 4 {
            panicf!(
                "FATAL: {}: unexpected length for binary representation of IPv4: got {}; want 4",
                part_path,
                v.len()
            );
        }
        let n = unmarshal_ipv4(v);
        n >= min_value && n <= max_value
    });
}

// ---------------------------------------------------------------------------
// Match / conversion helpers (Go filter_range.go + pipe_math.go)
// ---------------------------------------------------------------------------

/// Port of Go `matchRange`.
pub(crate) fn match_range(s: &[u8], min_value: f64, max_value: f64) -> bool {
    // Invalid UTF-8 cannot parse as a number/timestamp/ipv4, so it yields NaN
    // = no match, exactly like the failed parse in Go.
    let f = match std::str::from_utf8(s) {
        Ok(s) => parse_math_number(s),
        Err(_) => f64::NAN,
    };
    f >= min_value && f <= max_value
}

/// Go `parseMathNumber` (pipe_math.go): re-exported from the ported
/// `pipe_math` module for `filter_range` / `filter_le_field` / the parser's
/// `parse_number` (Go calls the same function from all of these).
pub(crate) use crate::pipe_math::parse_math_number;

/// Ceils `min_value`, floors `max_value`, and clamps both into `u64`.
/// Port of Go `toUint64Range`.
pub(crate) fn to_uint64_range(min_value: f64, max_value: f64) -> (u64, u64) {
    (
        to_uint64_clamp(min_value.ceil()),
        to_uint64_clamp(max_value.floor()),
    )
}

/// Clamps `f` into the `u64` range. Port of Go `toUint64Clamp`.
pub(crate) fn to_uint64_clamp(f: f64) -> u64 {
    if f < 0.0 {
        return 0;
    }
    if f > u64::MAX as f64 {
        return u64::MAX;
    }
    f as u64
}

/// Ceils `min_value`, floors `max_value`, and clamps both into `i64`.
/// Port of Go `toInt64Range`.
pub(crate) fn to_int64_range(min_value: f64, max_value: f64) -> (i64, i64) {
    (
        to_int64_clamp(min_value.ceil()),
        to_int64_clamp(max_value.floor()),
    )
}

/// Clamps `f` into the `i64` range. Port of Go `toInt64Clamp`.
pub(crate) fn to_int64_clamp(f: f64) -> i64 {
    if f < i64::MIN as f64 {
        return i64::MIN;
    }
    if f > i64::MAX as f64 {
        return i64::MAX;
    }
    f as i64
}

/// Ceils `min_value`, floors `max_value`, and clamps both into `u32`.
/// Port of Go `toUint32Range`.
pub(crate) fn to_uint32_range(min_value: f64, max_value: f64) -> (u32, u32) {
    (
        to_uint32_clamp(min_value.ceil()),
        to_uint32_clamp(max_value.floor()),
    )
}

/// Clamps `f` into the `u32` range. Port of Go `toUint32Clamp`.
pub(crate) fn to_uint32_clamp(f: f64) -> u32 {
    if f < 0.0 {
        return 0;
    }
    if f > u32::MAX as f64 {
        return u32::MAX;
    }
    f as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_uint64_range() {
        assert_eq!(to_uint64_range(-10.0, 20.0), (0, 20));
        assert_eq!(to_uint64_range(10.1, 20.9), (11, 20));
        assert_eq!(to_uint64_range(10.0, 10.0), (10, 10));
        assert_eq!(to_uint64_clamp(-1.0), 0);
        assert_eq!(to_uint64_clamp(u64::MAX as f64 * 2.0), u64::MAX);
    }

    #[test]
    fn test_to_int64_range() {
        assert_eq!(to_int64_range(-10.9, 20.9), (-10, 20));
        assert_eq!(to_int64_clamp(i64::MIN as f64 * 2.0), i64::MIN);
        assert_eq!(to_int64_clamp(i64::MAX as f64 * 2.0), i64::MAX);
    }

    #[test]
    fn test_to_uint32_range() {
        assert_eq!(to_uint32_range(-5.0, 300.9), (0, 300));
        assert_eq!(to_uint32_clamp(-1.0), 0);
        assert_eq!(to_uint32_clamp(u32::MAX as f64 * 2.0), u32::MAX);
    }

    #[test]
    fn test_match_range() {
        assert!(match_range(b"10", -10.0, 20.0));
        assert!(match_range(b"10", 10.0, 10.0));
        assert!(!match_range(b"10", 10.1, 20.0));
        assert!(!match_range(b"abc", -1e18, 1e18));
        assert!(match_range(b"10.5", 10.0, 11.0));
        // ipv4 parsed via parse_math_number
        assert!(match_range(b"0.0.0.1", 1.0, 1.0));
    }

    #[test]
    fn test_parse_math_number() {
        assert_eq!(parse_math_number("123"), 123.0);
        assert_eq!(parse_math_number("1.5"), 1.5);
        assert!(parse_math_number("").is_nan());
        assert!(parse_math_number("foobar").is_nan());
        // 0x prefix via is_likely_number/parse_int fallback
        assert_eq!(parse_math_number("0x10"), 16.0);
    }
}

//! Port of EsLogs `lib/logstorage/filter_ipv4_range.go`.
//!
//! `FilterIPv4Range` matches values in the IPv4 range `[min_value..max_value]`.

use esl_common::bytesutil::to_unsafe_string;
use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::FieldFilter;
use crate::filter_generic::{FilterGeneric, clone_column_header, new_filter_generic};
use crate::filter_phrase::{match_column_by_generic, match_encoded_values_dict, visit_values};
use crate::filter_range::match_ipv4_by_range;
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::{ValueType, marshal_ipv4_string, try_parse_ipv4_bytes};

// ---------------------------------------------------------------------------
// FilterIPv4Range
// ---------------------------------------------------------------------------

/// `FilterIPv4Range` matches the given ipv4 range `[min_value..max_value]`.
///
/// Example LogsQL: `ipv4_range(127.0.0.1, 127.0.0.255)`.
pub(crate) struct FilterIPv4Range {
    pub(crate) min_value: u32,
    pub(crate) max_value: u32,
}

/// Builds an ipv4-range filter for `field_name`.
pub(crate) fn new_filter_ipv4_range(
    field_name: &str,
    min_value: u32,
    max_value: u32,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterIPv4Range {
            min_value,
            max_value,
        }),
    )
}

impl FieldFilter for FilterIPv4Range {
    fn to_string(&self) -> String {
        let mut min_value = Vec::new();
        marshal_ipv4_string(&mut min_value, self.min_value);
        let mut max_value = Vec::new();
        marshal_ipv4_string(&mut max_value, self.max_value);
        format!(
            "ipv4_range({}, {})",
            to_unsafe_string(&min_value),
            to_unsafe_string(&max_value)
        )
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_ipv4_range(v, self.min_value, self.max_value)
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

        // PORT NOTE: Go's per-`valueType` block-result path matches only
        // string/dict/ipv4/const columns and resets everything else (uint*, int64,
        // float64, ipv6, time). The port routes all non-const columns through the
        // decoded per-row values with `match_ipv4_range`, which returns false for
        // any value that does not parse as an IPv4 in range — identical result to
        // Go's explicit resets, without needing the private column internals.
        let r = br.get_column_by_name(field_name);
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0);
            if !match_ipv4_range(v, min_value, max_value) {
                bm.reset_bits();
            }
            return;
        }
        match_column_by_generic(br, bm, r, "", &|v, _| {
            match_ipv4_range(v, min_value, max_value)
        });
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
            if !match_ipv4_range(&v, min_value, max_value) {
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
            ValueType::STRING => match_string_by_ipv4_range(bs, &ch, bm, min_value, max_value),
            ValueType::DICT => match_values_dict_by_ipv4_range(bs, &ch, bm, min_value, max_value),
            ValueType::IPV4 => match_ipv4_by_range(bs, &ch, bm, min_value, max_value),
            ValueType::UINT8
            | ValueType::UINT16
            | ValueType::UINT32
            | ValueType::UINT64
            | ValueType::INT64
            | ValueType::FLOAT64
            | ValueType::TIMESTAMP_ISO8601 => bm.reset_bits(),
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// helpers (Go filter_ipv4_range.go)
// ---------------------------------------------------------------------------

fn match_values_dict_by_ipv4_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: u32,
    max_value: u32,
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_ipv4_range(v, min_value, max_value)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

fn match_string_by_ipv4_range(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    min_value: u32,
    max_value: u32,
) {
    visit_values(bs, ch, bm, |v| match_ipv4_range(v, min_value, max_value));
}

/// Port of Go `matchIPv4Range`.
pub(crate) fn match_ipv4_range(s: &[u8], min_value: u32, max_value: u32) -> bool {
    match try_parse_ipv4_bytes(s) {
        Some(n) => n >= min_value && n <= max_value,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_ipv4_range() {
        // 127.0.0.1 .. 127.0.0.255
        let min = try_parse_ipv4_bytes(b"127.0.0.1").unwrap();
        let max = try_parse_ipv4_bytes(b"127.0.0.255").unwrap();
        assert!(match_ipv4_range(b"127.0.0.1", min, max));
        assert!(match_ipv4_range(b"127.0.0.128", min, max));
        assert!(match_ipv4_range(b"127.0.0.255", min, max));
        assert!(!match_ipv4_range(b"127.0.1.0", min, max));
        assert!(!match_ipv4_range(b"127.0.0.0", min, max));
        assert!(!match_ipv4_range(b"foobar", min, max));
        assert!(!match_ipv4_range(b"", min, max));
    }

    #[test]
    fn test_to_string() {
        let f = FilterIPv4Range {
            min_value: try_parse_ipv4_bytes(b"1.2.3.4").unwrap(),
            max_value: try_parse_ipv4_bytes(b"5.6.7.8").unwrap(),
        };
        assert_eq!(f.to_string(), "ipv4_range(1.2.3.4, 5.6.7.8)");
    }
}

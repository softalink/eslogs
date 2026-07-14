//! Port of EsLogs `lib/logstorage/filter_le_field.go`.
//!
//! `FilterLeField` matches rows where `field_name <= other_field_name`
//! (or `<` when `exclude_equal_values` is set — the `lt_field` form).

use std::sync::OnceLock;

use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::{BlockResult, ColRef, get_block_result, put_block_result};
use crate::block_search::BlockSearch;
use crate::filter::Filter;
use crate::filter_generic::{clone_column_header, quote_field_name_if_needed};
use crate::filter_range::parse_math_number;
use crate::log_rows::get_canonical_column_name_bytes;
use crate::prefix_filter;
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::{ValueType, unmarshal_float64, unmarshal_int64};

/// `FilterLeField` matches rows where `field_name` is `<=` (or `<`) the
/// `other_field_name`.
///
/// Example LogsQL: `fieldName:le_field(otherField)`.
pub(crate) struct FilterLeField {
    pub(crate) field_name: Vec<u8>,
    pub(crate) other_field_name: Vec<u8>,
    pub(crate) exclude_equal_values: bool,
    prefix_filter: OnceLock<prefix_filter::Filter>,
}

/// Builds a le_field / lt_field filter.
pub(crate) fn new_filter_le_field(
    field_name: &[u8],
    other_field_name: &[u8],
    exclude_equal_values: bool,
) -> FilterLeField {
    FilterLeField {
        field_name: get_canonical_column_name_bytes(field_name).to_vec(),
        other_field_name: get_canonical_column_name_bytes(other_field_name).to_vec(),
        exclude_equal_values,
        prefix_filter: OnceLock::new(),
    }
}

impl FilterLeField {
    fn get_prefix_filter(&self) -> &prefix_filter::Filter {
        self.prefix_filter.get_or_init(|| {
            let mut pf = prefix_filter::Filter::default();
            pf.add_allow_filters(&[self.field_name.as_slice(), self.other_field_name.as_slice()]);
            pf
        })
    }

    fn apply_filter_string(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap) {
        // PORT NOTE: see filter_eq_field.rs — mirrors Go's `applyFilterString`
        // slow path, which depends on block_result's not-yet-implemented
        // block-search read path.
        let exclude = self.exclude_equal_values;
        let mut br = get_block_result();
        br.must_init(bs, bm);
        let pf = self.get_prefix_filter().clone();
        br.init_columns(&pf);

        let r = br.get_column_by_name(&self.field_name);
        let r_other = br.get_column_by_name(&self.other_field_name);
        let values: Vec<Vec<u8>> = br.column_get_values(r).to_vec();
        let values_other: Vec<Vec<u8>> = br.column_get_values(r_other).to_vec();

        let mut src_idx = 0;
        bm.for_each_set_bit(|_| {
            let ok = le_values_string(&values[src_idx], &values_other[src_idx], exclude);
            src_idx += 1;
            ok
        });

        put_block_result(br);
    }

    fn apply_filter_dict(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        ch: &ColumnHeader,
        ch_other: &ColumnHeader,
    ) {
        let exclude = self.exclude_equal_values;
        let ve: Vec<Vec<u8>> = bs.get_values_for_column(ch).to_vec();
        let ve_other: Vec<Vec<u8>> = bs.get_values_for_column(ch_other).to_vec();
        bm.for_each_set_bit(|idx| {
            let di = ve[idx][0] as usize;
            let dio = ve_other[idx][0] as usize;
            le_values_string(
                &ch.values_dict.values[di],
                &ch_other.values_dict.values[dio],
                exclude,
            )
        });
    }

    fn apply_filter_uint(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        ch: &ColumnHeader,
        ch_other: &ColumnHeader,
    ) {
        let exclude = self.exclude_equal_values;
        let ve: Vec<Vec<u8>> = bs.get_values_for_column(ch).to_vec();
        let ve_other: Vec<Vec<u8>> = bs.get_values_for_column(ch_other).to_vec();
        bm.for_each_set_bit(|idx| le_values_string(&ve[idx], &ve_other[idx], exclude));
    }

    fn apply_filter_int64(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        ch: &ColumnHeader,
        ch_other: &ColumnHeader,
    ) {
        let exclude = self.exclude_equal_values;
        let ve: Vec<Vec<u8>> = bs.get_values_for_column(ch).to_vec();
        let ve_other: Vec<Vec<u8>> = bs.get_values_for_column(ch_other).to_vec();
        bm.for_each_set_bit(|idx| {
            let n = unmarshal_int64(&ve[idx]);
            let n_other = unmarshal_int64(&ve_other[idx]);
            le_values_int64(n, n_other, exclude)
        });
    }

    fn apply_filter_float64(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        ch: &ColumnHeader,
        ch_other: &ColumnHeader,
    ) {
        let exclude = self.exclude_equal_values;
        let ve: Vec<Vec<u8>> = bs.get_values_for_column(ch).to_vec();
        let ve_other: Vec<Vec<u8>> = bs.get_values_for_column(ch_other).to_vec();
        bm.for_each_set_bit(|idx| {
            let f = unmarshal_float64(&ve[idx]);
            let f_other = unmarshal_float64(&ve_other[idx]);
            le_values_float64(f, f_other, exclude)
        });
    }
}

impl Filter for FilterLeField {
    fn to_string(&self) -> String {
        let func_name = if self.exclude_equal_values {
            "lt_field"
        } else {
            "le_field"
        };
        format!(
            "{}{}({})",
            quote_field_name_if_needed(&self.field_name),
            func_name,
            crate::parser::quote_token_bytes_if_needed(&self.other_field_name)
        )
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter(&self.field_name);
        pf.add_allow_filter(&self.other_field_name);
    }

    fn match_row(&self, fields: &[Field]) -> bool {
        let v = get_field_value_by_name(fields, &self.field_name);
        let v_other = get_field_value_by_name(fields, &self.other_field_name);
        le_values_string(v, v_other, self.exclude_equal_values)
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        let exclude = self.exclude_equal_values;

        if self.field_name == self.other_field_name {
            if exclude {
                bm.reset_bits();
            }
            return;
        }

        let c = br.get_column_by_name(&self.field_name);
        let c_other = br.get_column_by_name(&self.other_field_name);

        if br.column_is_const(c) && br.column_is_const(c_other) {
            let v = br.column_get_value_at_row(c, 0).to_vec();
            let v_other = br.column_get_value_at_row(c_other, 0);
            if !le_values_string(&v, v_other, exclude) {
                bm.reset_bits();
            }
            return;
        }
        if br.column_is_time(c) && br.column_is_time(c_other) {
            // c and c_other are the single `_time` column, so they are equal.
            if exclude {
                bm.reset_bits();
            }
            return;
        }

        if br.column_value_type(c) != br.column_value_type(c_other) {
            // Slow path - differing value types, compare decoded string values.
            apply_filter_le_string(br, bm, c, c_other, exclude);
            return;
        }

        match br.column_value_type(c) {
            ValueType::STRING | ValueType::DICT => {
                apply_filter_le_string(br, bm, c, c_other, exclude)
            }
            ValueType::UINT8
            | ValueType::UINT16
            | ValueType::UINT32
            | ValueType::UINT64
            | ValueType::IPV4
            | ValueType::TIMESTAMP_ISO8601 => apply_filter_le_uint(br, bm, c, c_other, exclude),
            ValueType::INT64 => apply_filter_le_int64(br, bm, c, c_other, exclude),
            ValueType::FLOAT64 => apply_filter_le_float64(br, bm, c, c_other, exclude),
            other => panicf!("FATAL: unknown valueType={}", other.0),
        }
    }

    fn apply_to_block_search(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap) {
        let exclude = self.exclude_equal_values;

        if self.field_name == self.other_field_name {
            if exclude {
                bm.reset_bits();
            }
            return;
        }

        let v = bs.get_const_column_value(&self.field_name);
        let v_other = bs.get_const_column_value(&self.other_field_name);
        if !v.is_empty() || !v_other.is_empty() {
            if !v.is_empty() && !v_other.is_empty() {
                if !le_values_string(&v, &v_other, exclude) {
                    bm.reset_bits();
                }
                return;
            }
            self.apply_filter_string(bs, bm);
            return;
        }

        let ch = bs
            .get_column_header(&self.field_name)
            .map(clone_column_header);
        let ch_other = bs
            .get_column_header(&self.other_field_name)
            .map(clone_column_header);
        let (ch, ch_other) = match (ch, ch_other) {
            (None, None) => {
                if exclude {
                    bm.reset_bits();
                }
                return;
            }
            (Some(ch), Some(ch_other)) => (ch, ch_other),
            _ => {
                self.apply_filter_string(bs, bm);
                return;
            }
        };

        if ch.value_type != ch_other.value_type {
            // Slow path - differing value types, compare decoded string values.
            self.apply_filter_string(bs, bm);
            return;
        }

        match ch.value_type {
            ValueType::STRING => self.apply_filter_string(bs, bm),
            ValueType::DICT => self.apply_filter_dict(bs, bm, &ch, &ch_other),
            ValueType::UINT8
            | ValueType::UINT16
            | ValueType::UINT32
            | ValueType::UINT64
            | ValueType::IPV4
            | ValueType::TIMESTAMP_ISO8601 => self.apply_filter_uint(bs, bm, &ch, &ch_other),
            ValueType::INT64 => self.apply_filter_int64(bs, bm, &ch, &ch_other),
            ValueType::FLOAT64 => self.apply_filter_float64(bs, bm, &ch, &ch_other),
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// block_result helpers (Go filter_le_field.go)
// ---------------------------------------------------------------------------

fn apply_filter_le_string(
    br: &mut BlockResult,
    bm: &mut Bitmap,
    c: ColRef,
    c_other: ColRef,
    exclude: bool,
) {
    let values: Vec<Vec<u8>> = br.column_get_values(c).to_vec();
    let values_other: Vec<Vec<u8>> = br.column_get_values(c_other).to_vec();
    bm.for_each_set_bit(|idx| le_values_string(&values[idx], &values_other[idx], exclude));
}

fn apply_filter_le_uint(
    br: &mut BlockResult,
    bm: &mut Bitmap,
    c: ColRef,
    c_other: ColRef,
    exclude: bool,
) {
    let ve: Vec<Vec<u8>> = br
        .column_get_values_encoded(c)
        .map(<[Vec<u8>]>::to_vec)
        .unwrap_or_default();
    let ve_other: Vec<Vec<u8>> = br
        .column_get_values_encoded(c_other)
        .map(<[Vec<u8>]>::to_vec)
        .unwrap_or_default();
    bm.for_each_set_bit(|idx| le_values_string(&ve[idx], &ve_other[idx], exclude));
}

fn apply_filter_le_int64(
    br: &mut BlockResult,
    bm: &mut Bitmap,
    c: ColRef,
    c_other: ColRef,
    exclude: bool,
) {
    let ve: Vec<Vec<u8>> = br
        .column_get_values_encoded(c)
        .map(<[Vec<u8>]>::to_vec)
        .unwrap_or_default();
    let ve_other: Vec<Vec<u8>> = br
        .column_get_values_encoded(c_other)
        .map(<[Vec<u8>]>::to_vec)
        .unwrap_or_default();
    bm.for_each_set_bit(|idx| {
        let n = unmarshal_int64(&ve[idx]);
        let n_other = unmarshal_int64(&ve_other[idx]);
        le_values_int64(n, n_other, exclude)
    });
}

fn apply_filter_le_float64(
    br: &mut BlockResult,
    bm: &mut Bitmap,
    c: ColRef,
    c_other: ColRef,
    exclude: bool,
) {
    let ve: Vec<Vec<u8>> = br
        .column_get_values_encoded(c)
        .map(<[Vec<u8>]>::to_vec)
        .unwrap_or_default();
    let ve_other: Vec<Vec<u8>> = br
        .column_get_values_encoded(c_other)
        .map(<[Vec<u8>]>::to_vec)
        .unwrap_or_default();
    bm.for_each_set_bit(|idx| {
        let f = unmarshal_float64(&ve[idx]);
        let f_other = unmarshal_float64(&ve_other[idx]);
        le_values_float64(f, f_other, exclude)
    });
}

// ---------------------------------------------------------------------------
// Value comparators (Go filter_le_field.go)
// ---------------------------------------------------------------------------

/// Port of Go `leValuesString`: numeric comparison when both parse as numbers,
/// else plain string comparison (`&[u8]` Ord equals Go string order).
pub(crate) fn le_values_string(a: &[u8], b: &[u8], exclude_equal_values: bool) -> bool {
    // Invalid UTF-8 cannot be a valid number, so the checked parse fails
    // exactly like Go's parseMathNumber on such bytes.
    let f_a = std::str::from_utf8(a).map_or(f64::NAN, parse_math_number);
    if !f_a.is_nan() {
        let f_b = std::str::from_utf8(b).map_or(f64::NAN, parse_math_number);
        if !f_b.is_nan() {
            return if exclude_equal_values {
                f_a < f_b
            } else {
                f_a <= f_b
            };
        }
    }
    if exclude_equal_values { a < b } else { a <= b }
}

/// Port of Go `leValuesInt64`.
pub(crate) fn le_values_int64(a: i64, b: i64, exclude_equal_values: bool) -> bool {
    if exclude_equal_values { a < b } else { a <= b }
}

/// Port of Go `leValuesFloat64`.
pub(crate) fn le_values_float64(a: f64, b: f64, exclude_equal_values: bool) -> bool {
    if exclude_equal_values { a < b } else { a <= b }
}

// PORT NOTE: TestFilterLeField / TestFilterLtField use `testFilterMatchForColumns`
// (`Storage`/`searchParallel`, not yet ported). The whole-filter cases are
// deferred with the other filter search-integration tests; the comparators and
// `match_row`/`to_string` are unit-tested here.
#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_to_string() {
        assert_eq!(
            new_filter_le_field(b"foo", b"bar", false).to_string(),
            "foo:le_field(bar)"
        );
        assert_eq!(
            new_filter_le_field(b"foo", b"bar", true).to_string(),
            "foo:lt_field(bar)"
        );
    }

    #[test]
    fn test_le_values_string_numeric() {
        // both numeric -> numeric compare
        assert!(le_values_string(b"9", b"10", false));
        assert!(le_values_string(b"9", b"10", true));
        assert!(le_values_string(b"10", b"10", false));
        assert!(!le_values_string(b"10", b"10", true));
        assert!(!le_values_string(b"11", b"10", false));
    }

    #[test]
    fn test_le_values_string_lexicographic() {
        // non-numeric -> string compare
        assert!(le_values_string(b"abc", b"abd", false));
        assert!(!le_values_string(b"abd", b"abc", false));
        assert!(le_values_string(b"abc", b"abc", false));
        assert!(!le_values_string(b"abc", b"abc", true));
    }

    #[test]
    fn test_match_row() {
        let f = new_filter_le_field(b"foo", b"bar", false);
        assert!(f.match_row(&[field("foo", "9"), field("bar", "10")]));
        assert!(!f.match_row(&[field("foo", "11"), field("bar", "10")]));
        let lt = new_filter_le_field(b"foo", b"bar", true);
        assert!(!lt.match_row(&[field("foo", "10"), field("bar", "10")]));
    }
}

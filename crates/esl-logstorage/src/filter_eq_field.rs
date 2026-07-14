//! Port of EsLogs `lib/logstorage/filter_eq_field.go`.
//!
//! `FilterEqField` matches rows where two fields hold equivalent values.

use std::sync::OnceLock;

use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::{BlockResult, ColRef, get_block_result, put_block_result};
use crate::block_search::BlockSearch;
use crate::filter::Filter;
use crate::filter_generic::{clone_column_header, quote_field_name_if_needed};
use crate::log_rows::get_canonical_column_name_bytes;
use crate::prefix_filter;
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::ValueType;

/// `FilterEqField` matches rows where `field_name` equals `other_field_name`.
///
/// Example LogsQL: `fieldName:eq_field(otherField)`.
pub(crate) struct FilterEqField {
    pub(crate) field_name: Vec<u8>,
    pub(crate) other_field_name: Vec<u8>,
    prefix_filter: OnceLock<prefix_filter::Filter>,
}

/// Builds an eq_field filter.
pub(crate) fn new_filter_eq_field(field_name: &[u8], other_field_name: &[u8]) -> FilterEqField {
    FilterEqField {
        field_name: get_canonical_column_name_bytes(field_name).to_vec(),
        other_field_name: get_canonical_column_name_bytes(other_field_name).to_vec(),
        prefix_filter: OnceLock::new(),
    }
}

impl FilterEqField {
    fn get_prefix_filter(&self) -> &prefix_filter::Filter {
        self.prefix_filter.get_or_init(|| {
            let mut pf = prefix_filter::Filter::default();
            pf.add_allow_filters(&[self.field_name.as_slice(), self.other_field_name.as_slice()]);
            pf
        })
    }

    fn apply_filter_string(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap) {
        // PORT NOTE: mirrors Go's `applyFilterString`, which builds a filtered
        // `blockResult` and compares the decoded per-row values. It relies on the
        // block-search-backed `BlockResult` read path (`init_columns` + encoded
        // reads), which is not yet implemented in block_result.rs; this slow path
        // becomes functional once that lands. It is not reachable via the ported
        // surface today (the query pipeline is unported).
        let mut br = get_block_result();
        br.must_init(bs, bm);
        let pf = self.get_prefix_filter().clone();
        br.init_columns(&pf);

        let r = br.get_column_by_name(&self.field_name);
        let r_other = br.get_column_by_name(&self.other_field_name);
        let values: Vec<Vec<u8>> = br.column_get_values(r).to_vec();
        let values_other = br.column_get_values(r_other).to_vec();

        let mut src_idx = 0;
        bm.for_each_set_bit(|_| {
            let ok = values[src_idx] == values_other[src_idx];
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
        let ve: Vec<Vec<u8>> = bs.get_values_for_column(ch).to_vec();
        let ve_other: Vec<Vec<u8>> = bs.get_values_for_column(ch_other).to_vec();
        bm.for_each_set_bit(|idx| {
            let di = ve[idx][0] as usize;
            let dio = ve_other[idx][0] as usize;
            ch.values_dict.values[di] == ch_other.values_dict.values[dio]
        });
    }

    fn apply_filter_bin_value(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        ch: &ColumnHeader,
        ch_other: &ColumnHeader,
    ) {
        let ve: Vec<Vec<u8>> = bs.get_values_for_column(ch).to_vec();
        let ve_other: Vec<Vec<u8>> = bs.get_values_for_column(ch_other).to_vec();
        bm.for_each_set_bit(|idx| ve[idx] == ve_other[idx]);
    }
}

impl Filter for FilterEqField {
    fn to_string(&self) -> String {
        format!(
            "{}eq_field({})",
            quote_field_name_if_needed(&self.field_name),
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
        v == v_other
    }

    fn apply_to_block_result(&self, br: &mut BlockResult, bm: &mut Bitmap) {
        if self.field_name == self.other_field_name {
            return;
        }

        let c = br.get_column_by_name(&self.field_name);
        let c_other = br.get_column_by_name(&self.other_field_name);

        if br.column_is_const(c) && br.column_is_const(c_other) {
            let v = br.column_get_value_at_row(c, 0).to_vec();
            let v_other = br.column_get_value_at_row(c_other, 0);
            if v != v_other {
                bm.reset_bits();
            }
            return;
        }
        if br.column_is_time(c) && br.column_is_time(c_other) {
            // c and c_other are the single `_time` column, so they are equal.
            return;
        }

        if br.column_value_type(c) != br.column_value_type(c_other) {
            // Slow path - differing value types, compare decoded string values.
            apply_filter_eq_string(br, bm, c, c_other);
            return;
        }

        match br.column_value_type(c) {
            ValueType::STRING | ValueType::DICT => apply_filter_eq_string(br, bm, c, c_other),
            ValueType::UINT8
            | ValueType::UINT16
            | ValueType::UINT32
            | ValueType::UINT64
            | ValueType::INT64
            | ValueType::FLOAT64
            | ValueType::IPV4
            | ValueType::TIMESTAMP_ISO8601 => apply_filter_eq_bin_values(br, bm, c, c_other),
            other => panicf!("FATAL: unknown valueType={}", other.0),
        }
    }

    fn apply_to_block_search(&self, bs: &mut BlockSearch<'_>, bm: &mut Bitmap) {
        if self.field_name == self.other_field_name {
            return;
        }

        let v = bs.get_const_column_value(&self.field_name);
        let v_other = bs.get_const_column_value(&self.other_field_name);
        if !v.is_empty() || !v_other.is_empty() {
            if !v.is_empty() && !v_other.is_empty() {
                if v != v_other {
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
            (None, None) => return,
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
            | ValueType::INT64
            | ValueType::FLOAT64
            | ValueType::IPV4
            | ValueType::TIMESTAMP_ISO8601 => self.apply_filter_bin_value(bs, bm, &ch, &ch_other),
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// block_result helpers (Go filter_eq_field.go)
// ---------------------------------------------------------------------------

fn apply_filter_eq_string(br: &mut BlockResult, bm: &mut Bitmap, c: ColRef, c_other: ColRef) {
    let values: Vec<Vec<u8>> = br.column_get_values(c).to_vec();
    let values_other: Vec<Vec<u8>> = br.column_get_values(c_other).to_vec();
    bm.for_each_set_bit(|idx| values[idx] == values_other[idx]);
}

fn apply_filter_eq_bin_values(br: &mut BlockResult, bm: &mut Bitmap, c: ColRef, c_other: ColRef) {
    let ve: Vec<Vec<u8>> = br
        .column_get_values_encoded(c)
        .map(<[Vec<u8>]>::to_vec)
        .unwrap_or_default();
    let ve_other: Vec<Vec<u8>> = br
        .column_get_values_encoded(c_other)
        .map(<[Vec<u8>]>::to_vec)
        .unwrap_or_default();
    bm.for_each_set_bit(|idx| ve[idx] == ve_other[idx]);
}

// PORT NOTE: TestFilterEqField uses `testFilterMatchForColumns`
// (`Storage`/`searchParallel`, not yet ported); the whole-filter cases are
// deferred with the other filter search-integration tests. `match_row` and
// `to_string` are unit-tested here.
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
        let f = new_filter_eq_field(b"foo", b"bar");
        assert_eq!(f.to_string(), "foo:eq_field(bar)");
    }

    #[test]
    fn test_match_row() {
        let f = new_filter_eq_field(b"foo", b"bar");
        assert!(f.match_row(&[field("foo", "x"), field("bar", "x")]));
        assert!(!f.match_row(&[field("foo", "x"), field("bar", "y")]));
        // both missing -> both empty -> equal
        assert!(f.match_row(&[field("baz", "x")]));
    }
}

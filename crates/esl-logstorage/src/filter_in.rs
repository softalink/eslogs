//! Port of EsLogs `lib/logstorage/filter_in.go`.
//!
//! `FilterIn` matches any exact value from the values set. It also hosts the
//! `pub(crate)` "match any value" block helpers shared with `filter_contains_any`.

use std::collections::HashSet;

use esl_common::bytesutil::to_unsafe_string;
use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::{BlockResult, ColRef};
use crate::block_search::BlockSearch;
use crate::filter::{FieldFilter, Filter};
use crate::filter_generic::{FilterGeneric, clone_column_header, new_filter_generic};
use crate::filter_phrase::{
    match_bloom_filter_all_tokens, match_encoded_values_dict, visit_values,
};
use crate::in_values::InValues;
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::ValueType;

// It is faster to match every row in the block instead of checking too big
// number of tokenSets against the bloom filter.
const MAX_TOKEN_SETS_TO_INIT: usize = 1000;

/// `FilterIn` matches any exact value from the values set.
///
/// Example LogsQL: `in("foo", "bar baz")` or `in(<subquery> | fields x)`.
///
/// The subquery form carries the rendered subquery in
/// [`InValues::q_text`]; its values are resolved by
/// `storage_search::init_subqueries` before filter execution.
pub(crate) struct FilterIn {
    pub(crate) values: InValues,
}

pub(crate) fn new_filter_in_values(field_name: &str, values: Vec<String>) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterIn {
            values: InValues::new(values),
        }),
    )
}

/// Builds an `in(<subquery>)` filter (Go `parseFilterIn` with `iv.q` set).
pub(crate) fn new_filter_in_query(
    field_name: &str,
    q_text: String,
    q_field_name: String,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterIn {
            values: InValues::new_from_query(q_text, q_field_name),
        }),
    )
}

impl FilterIn {
    fn match_column_by_string_values(
        br: &mut BlockResult,
        bm: &mut Bitmap,
        r: ColRef,
        string_values: &HashSet<String>,
    ) {
        let values = br.column_get_values(r);
        bm.for_each_set_bit(|idx| string_values.contains(to_unsafe_string(&values[idx])));
    }
}

impl FieldFilter for FilterIn {
    fn to_string(&self) -> String {
        format!("in({})", self.values.string())
    }

    fn in_values(&self) -> Option<&InValues> {
        Some(&self.values)
    }

    fn in_values_mut(&mut self) -> Option<&mut InValues> {
        Some(&mut self.values)
    }

    fn new_with_values(&self, field_name: &str, values: Vec<String>) -> Option<Box<dyn Filter>> {
        Some(Box::new(new_filter_in_values(field_name, values)))
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        let string_values = self.values.get_string_values();
        string_values.contains(v)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        if self.values.is_empty() {
            bm.reset_bits();
            return;
        }

        let r = br.get_column_by_name(field_name);
        if br.column_is_const(r) {
            let string_values = self.values.get_string_values();
            let v = br.column_get_value_at_row(r, 0);
            if !string_values.contains(v) {
                bm.reset_bits();
            }
            return;
        }
        if br.column_is_time(r) {
            let string_values = self.values.get_string_values();
            Self::match_column_by_string_values(br, bm, r, string_values);
            return;
        }

        match br.column_value_type(r) {
            // PORT NOTE: Go's `valueTypeDict` case maps encoded dict indices
            // through a per-dict-entry table built from `c.dictValues`.
            // `BlockResult` does not expose `dictValues`, so the port routes the
            // dict case through the already-decoded per-row string values, like
            // the shared block-result helpers. The result is identical.
            ValueType::STRING | ValueType::DICT => {
                let string_values = self.values.get_string_values();
                Self::match_column_by_string_values(br, bm, r, string_values);
            }
            ValueType::UINT8 => {
                let bin_values = self.values.get_uint8_values();
                match_column_by_bin_values(br, bm, r, bin_values.is_empty(), |v| {
                    bin_values.contains(v)
                });
            }
            ValueType::UINT16 => {
                let bin_values = self.values.get_uint16_values();
                match_column_by_bin_values(br, bm, r, bin_values.is_empty(), |v| {
                    bin_values.contains(v)
                });
            }
            ValueType::UINT32 => {
                let bin_values = self.values.get_uint32_values();
                match_column_by_bin_values(br, bm, r, bin_values.is_empty(), |v| {
                    bin_values.contains(v)
                });
            }
            ValueType::UINT64 => {
                let bin_values = self.values.get_uint64_values();
                match_column_by_bin_values(br, bm, r, bin_values.is_empty(), |v| {
                    bin_values.contains(v)
                });
            }
            ValueType::INT64 => {
                let bin_values = self.values.get_int64_values();
                match_column_by_bin_values(br, bm, r, bin_values.is_empty(), |v| {
                    bin_values.contains(v)
                });
            }
            ValueType::FLOAT64 => {
                let bin_values = self.values.get_float64_values();
                match_column_by_bin_values(br, bm, r, bin_values.is_empty(), |v| {
                    bin_values.contains(v)
                });
            }
            ValueType::IPV4 => {
                let bin_values = self.values.get_ipv4_values();
                match_column_by_bin_values(br, bm, r, bin_values.is_empty(), |v| {
                    bin_values.contains(v)
                });
            }
            ValueType::TIMESTAMP_ISO8601 => {
                let bin_values = self.values.get_timestamp_iso8601_values();
                match_column_by_bin_values(br, bm, r, bin_values.is_empty(), |v| {
                    bin_values.contains(v)
                });
            }
            other => panicf!("FATAL: unknown valueType={}", other.0),
        }
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        if self.values.is_empty() {
            bm.reset_bits();
            return;
        }

        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            let string_values = self.values.get_string_values();
            if !string_values.contains(&v) {
                bm.reset_bits();
            }
            return;
        }

        // Verify whether filter matches other columns.
        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns. It matches anything
                // only for the empty value.
                let string_values = self.values.get_string_values();
                if !string_values.contains("") {
                    bm.reset_bits();
                }
                return;
            }
        };

        let (common_tokens, token_sets) = self.values.get_tokens_hashes_any();

        match ch.value_type {
            ValueType::STRING => {
                let string_values = self.values.get_string_values();
                match_any_value(
                    bs,
                    &ch,
                    bm,
                    string_values.is_empty(),
                    common_tokens,
                    token_sets,
                    |v| string_values.contains(to_unsafe_string(v)),
                );
            }
            ValueType::DICT => {
                let string_values = self.values.get_string_values();
                match_values_dict_by_any_value(bs, &ch, bm, string_values);
            }
            ValueType::UINT8 => {
                let bin_values = self.values.get_uint8_values();
                match_any_value(
                    bs,
                    &ch,
                    bm,
                    bin_values.is_empty(),
                    common_tokens,
                    token_sets,
                    |v| bin_values.contains(v),
                );
            }
            ValueType::UINT16 => {
                let bin_values = self.values.get_uint16_values();
                match_any_value(
                    bs,
                    &ch,
                    bm,
                    bin_values.is_empty(),
                    common_tokens,
                    token_sets,
                    |v| bin_values.contains(v),
                );
            }
            ValueType::UINT32 => {
                let bin_values = self.values.get_uint32_values();
                match_any_value(
                    bs,
                    &ch,
                    bm,
                    bin_values.is_empty(),
                    common_tokens,
                    token_sets,
                    |v| bin_values.contains(v),
                );
            }
            ValueType::UINT64 => {
                let bin_values = self.values.get_uint64_values();
                match_any_value(
                    bs,
                    &ch,
                    bm,
                    bin_values.is_empty(),
                    common_tokens,
                    token_sets,
                    |v| bin_values.contains(v),
                );
            }
            ValueType::INT64 => {
                let bin_values = self.values.get_int64_values();
                match_any_value(
                    bs,
                    &ch,
                    bm,
                    bin_values.is_empty(),
                    common_tokens,
                    token_sets,
                    |v| bin_values.contains(v),
                );
            }
            ValueType::FLOAT64 => {
                let bin_values = self.values.get_float64_values();
                match_any_value(
                    bs,
                    &ch,
                    bm,
                    bin_values.is_empty(),
                    common_tokens,
                    token_sets,
                    |v| bin_values.contains(v),
                );
            }
            ValueType::IPV4 => {
                let bin_values = self.values.get_ipv4_values();
                match_any_value(
                    bs,
                    &ch,
                    bm,
                    bin_values.is_empty(),
                    common_tokens,
                    token_sets,
                    |v| bin_values.contains(v),
                );
            }
            ValueType::TIMESTAMP_ISO8601 => {
                let bin_values = self.values.get_timestamp_iso8601_values();
                match_any_value(
                    bs,
                    &ch,
                    bm,
                    bin_values.is_empty(),
                    common_tokens,
                    token_sets,
                    |v| bin_values.contains(v),
                );
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

/// Port of Go `matchColumnByBinValues`.
///
/// PORT NOTE: Go takes a `map[string]struct{}` of binary values; the port takes
/// a membership closure so the caller can back it with either a `HashSet<String>`
/// (string columns) or a `HashSet<Vec<u8>>` (numeric columns).
pub(crate) fn match_column_by_bin_values(
    br: &mut BlockResult,
    bm: &mut Bitmap,
    r: ColRef,
    is_empty: bool,
    member: impl Fn(&[u8]) -> bool,
) {
    if is_empty {
        bm.reset_bits();
        return;
    }
    let values_encoded = br
        .column_get_values_encoded(r)
        .expect("BUG: non-const, non-time column must have values_encoded");
    bm.for_each_set_bit(|idx| member(&values_encoded[idx]));
}

/// Port of Go `matchAnyValue`.
pub(crate) fn match_any_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    is_empty: bool,
    common_tokens: &[u64],
    token_sets: &[Vec<u64>],
    member: impl Fn(&[u8]) -> bool,
) {
    if is_empty {
        bm.reset_bits();
        return;
    }
    if !match_bloom_filter_any_token_set(bs, ch, common_tokens, token_sets) {
        bm.reset_bits();
        return;
    }
    visit_values(bs, ch, bm, |v| member(v));
}

/// Port of Go `matchBloomFilterAnyTokenSet`.
pub(crate) fn match_bloom_filter_any_token_set(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    common_tokens: &[u64],
    token_sets: &[Vec<u64>],
) -> bool {
    if !match_bloom_filter_all_tokens(bs, ch, common_tokens) {
        return false;
    }
    if token_sets.len() > MAX_TOKEN_SETS_TO_INIT
        || token_sets.len() as u64 > 10 * bs.block_header().rows_count
    {
        // It is faster to match every row in the block against all the values
        // instead of using the bloom filter for too big a number of tokenSets.
        return true;
    }
    token_sets.iter().any(|ts| bs.bloom_contains_all(ch, ts))
}

/// Port of Go `matchValuesDictByAnyValue`.
fn match_values_dict_by_any_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    values: &HashSet<String>,
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(values.contains(v)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

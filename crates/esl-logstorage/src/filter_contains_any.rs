//! Port of EsLogs `lib/logstorage/filter_contains_any.go`.
//!
//! `FilterContainsAny` matches logs containing any of the given values.

use esl_common::bytesutil::to_unsafe_string;
use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::{BlockResult, ColRef};
use crate::block_search::BlockSearch;
use crate::filter::{FieldFilter, Filter};
use crate::filter_generic::{FilterGeneric, clone_column_header, new_filter_generic};
use crate::filter_in::{match_any_value, match_column_by_bin_values};
use crate::filter_phrase::{
    match_bloom_filter_all_tokens, match_encoded_values_dict, match_phrase, to_float64_string,
    to_ipv4_string, to_timestamp_iso8601_string, visit_values,
};
use crate::filter_prefix::to_int64_string;
use crate::in_values::InValues;
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::ValueType;

/// `FilterContainsAny` matches any value from the values.
///
/// Example LogsQL: `contains_any("foo", "bar baz")`.
pub(crate) struct FilterContainsAny {
    pub(crate) values: InValues,
}

pub(crate) fn new_filter_contains_any_values(
    field_name: &str,
    values: Vec<String>,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterContainsAny {
            values: InValues::new(values),
        }),
    )
}

/// Builds a `contains_any(<subquery>)` filter (Go `parseFilterContainsAny`
/// with `iv.q` set).
pub(crate) fn new_filter_contains_any_query(
    field_name: &str,
    q_text: String,
    q_field_name: String,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterContainsAny {
            values: InValues::new_from_query(q_text, q_field_name),
        }),
    )
}

/// Port of Go `matchAnyPhrase`.
pub(crate) fn match_any_phrase<S: AsRef<str>>(v: &str, phrases: &[S]) -> bool {
    phrases.iter().any(|p| match_phrase(v, p.as_ref()))
}

impl FilterContainsAny {
    fn match_column_by_string_values(&self, br: &mut BlockResult, bm: &mut Bitmap, r: ColRef) {
        let phrases = &self.values.values;
        let values = br.column_get_values(r);
        bm.for_each_set_bit(|idx| match_any_phrase(to_unsafe_string(&values[idx]), phrases));
    }
}

impl FieldFilter for FilterContainsAny {
    fn to_string(&self) -> String {
        format!("contains_any({})", self.values.string())
    }

    fn in_values(&self) -> Option<&InValues> {
        Some(&self.values)
    }

    fn in_values_mut(&mut self) -> Option<&mut InValues> {
        Some(&mut self.values)
    }

    fn new_with_values(&self, field_name: &str, values: Vec<String>) -> Option<Box<dyn Filter>> {
        Some(Box::new(new_filter_contains_any_values(field_name, values)))
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_any_phrase(v, &self.values.values)
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
        if self.values.has_empty_value() {
            // Special case - empty value matches everything.
            return;
        }

        let r = br.get_column_by_name(field_name);
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0).to_string();
            if !match_any_phrase(&v, &self.values.values) {
                bm.reset_bits();
            }
            return;
        }
        if br.column_is_time(r) {
            self.match_column_by_string_values(br, bm, r);
            return;
        }

        match br.column_value_type(r) {
            // PORT NOTE: Go's `valueTypeDict` case maps encoded dict indices
            // through a per-dict-entry table built from `c.dictValues`.
            // `BlockResult` does not expose `dictValues`, so the port routes the
            // dict case through the already-decoded per-row values. Identical.
            ValueType::STRING | ValueType::DICT => {
                self.match_column_by_string_values(br, bm, r);
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
            ValueType::INT64
            | ValueType::FLOAT64
            | ValueType::IPV4
            | ValueType::TIMESTAMP_ISO8601 => {
                self.match_column_by_string_values(br, bm, r);
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
        if self.values.has_empty_value() {
            // Special case - empty value matches everything.
            return;
        }

        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_any_phrase(&v, &self.values.values) {
                bm.reset_bits();
            }
            return;
        }

        // Verify whether filter matches other columns.
        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns. It matches anything
                // only for the empty phrase.
                if !match_any_phrase("", &self.values.values) {
                    bm.reset_bits();
                }
                return;
            }
        };

        let (common_tokens, token_sets) = self.values.get_tokens_hashes_any();

        match ch.value_type {
            ValueType::STRING => {
                match_any_phrase_string(bs, &ch, bm, &self.values.values, common_tokens, token_sets)
            }
            ValueType::DICT => match_any_phrase_dict(bs, &ch, bm, &self.values.values),
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
                match_any_phrase_int64(bs, &ch, bm, &self.values.values, common_tokens, token_sets)
            }
            ValueType::FLOAT64 => match_any_phrase_float64(
                bs,
                &ch,
                bm,
                &self.values.values,
                common_tokens,
                token_sets,
            ),
            ValueType::IPV4 => {
                match_any_phrase_ipv4(bs, &ch, bm, &self.values.values, common_tokens, token_sets)
            }
            ValueType::TIMESTAMP_ISO8601 => match_any_phrase_timestamp_iso8601(
                bs,
                &ch,
                bm,
                &self.values.values,
                common_tokens,
                token_sets,
            ),
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

fn match_any_phrase_string(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    common_tokens: &[u64],
    token_sets: &[Vec<u64>],
) {
    if phrases.is_empty() {
        bm.reset_bits();
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, common_tokens) {
        bm.reset_bits();
        return;
    }
    match_values_any_phrase(bs, ch, bm, phrases, token_sets, |v, ph| {
        match_any_phrase(to_unsafe_string(v), ph)
    });
}

/// Port of Go `matchValuesAnyPhrase`.
///
/// PORT NOTE: Go pools the filtered phrase subset via `getStringBucket`; the
/// port collects it into a local `Vec<&str>` (the pooled reuse is dropped, the
/// matching semantics are identical).
fn match_values_any_phrase(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    token_sets: &[Vec<u64>],
    match_fn: impl Fn(&[u8], &[&str]) -> bool,
) {
    let filtered: Vec<&str> = {
        phrases
            .iter()
            .enumerate()
            .filter(|(i, _)| bs.bloom_contains_all(ch, &token_sets[*i]))
            .map(|(_, p)| p.as_str())
            .collect()
    };
    if filtered.is_empty() {
        bm.reset_bits();
        return;
    }
    visit_values(bs, ch, bm, |v| match_fn(v, &filtered));
}

fn match_any_phrase_int64(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    common_tokens: &[u64],
    token_sets: &[Vec<u64>],
) {
    if phrases.is_empty() {
        bm.reset_bits();
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, common_tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    match_values_any_phrase(bs, ch, bm, phrases, token_sets, |v, ph| {
        let mut bb = Vec::new();
        to_int64_string(&pp, &mut bb, v);
        match_any_phrase(to_unsafe_string(&bb), ph)
    });
}

fn match_any_phrase_float64(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    common_tokens: &[u64],
    token_sets: &[Vec<u64>],
) {
    if phrases.is_empty() {
        bm.reset_bits();
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, common_tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    match_values_any_phrase(bs, ch, bm, phrases, token_sets, |v, ph| {
        let mut bb = Vec::new();
        to_float64_string(&pp, &mut bb, v);
        match_any_phrase(to_unsafe_string(&bb), ph)
    });
}

fn match_any_phrase_ipv4(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    common_tokens: &[u64],
    token_sets: &[Vec<u64>],
) {
    if phrases.is_empty() {
        bm.reset_bits();
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, common_tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    match_values_any_phrase(bs, ch, bm, phrases, token_sets, |v, ph| {
        let mut bb = Vec::new();
        to_ipv4_string(&pp, &mut bb, v);
        match_any_phrase(to_unsafe_string(&bb), ph)
    });
}

fn match_any_phrase_timestamp_iso8601(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    common_tokens: &[u64],
    token_sets: &[Vec<u64>],
) {
    if phrases.is_empty() {
        bm.reset_bits();
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, common_tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    match_values_any_phrase(bs, ch, bm, phrases, token_sets, |v, ph| {
        let mut bb = Vec::new();
        to_timestamp_iso8601_string(&pp, &mut bb, v);
        match_any_phrase(to_unsafe_string(&bb), ph)
    });
}

fn match_any_phrase_dict(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_any_phrase(v, phrases)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

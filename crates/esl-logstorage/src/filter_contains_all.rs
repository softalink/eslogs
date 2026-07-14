//! Port of EsLogs `lib/logstorage/filter_contains_all.go`.
//!
//! `FilterContainsAll` matches logs containing all the given values.

use std::collections::HashSet;

use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::{BlockResult, ColRef};
use crate::block_search::BlockSearch;
use crate::filter::{FieldFilter, Filter};
use crate::filter_generic::{FilterGeneric, clone_column_header, new_filter_generic};
use crate::filter_phrase::{
    match_bloom_filter_all_tokens, match_encoded_values_dict, match_phrase, to_float64_string,
    to_ipv4_string, to_timestamp_iso8601_string, visit_values,
};
use crate::filter_prefix::to_int64_string;
use crate::in_values::InValues;
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::ValueType;

/// `FilterContainsAll` matches logs containing all the given values.
///
/// Example LogsQL: `contains_all("foo", "bar baz")`.
pub(crate) struct FilterContainsAll {
    pub(crate) values: InValues,
}

pub(crate) fn new_filter_contains_all_values(
    field_name: &str,
    values: Vec<String>,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterContainsAll {
            values: InValues::new(values),
        }),
    )
}

/// Builds a `contains_all(<subquery>)` filter (Go `parseFilterContainsAll`
/// with `iv.q` set).
pub(crate) fn new_filter_contains_all_query(
    field_name: &str,
    q_text: String,
    q_field_name: String,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterContainsAll {
            values: InValues::new_from_query(q_text, q_field_name),
        }),
    )
}

/// Port of Go `matchAllPhrases`.
fn match_all_phrases<S: AsRef<str>>(v: &[u8], phrases: &[S]) -> bool {
    for phrase in phrases {
        let phrase = phrase.as_ref();
        if phrase.is_empty() {
            // Special case - empty phrase matches everything.
            continue;
        }
        if !match_phrase(v, phrase) {
            return false;
        }
    }
    true
}

impl FilterContainsAll {
    fn match_column_by_string_values(&self, br: &mut BlockResult, bm: &mut Bitmap, r: ColRef) {
        let phrases = &self.values.values;
        let values = br.column_get_values(r);
        bm.for_each_set_bit(|idx| match_all_phrases(&values[idx], phrases));
    }
}

impl FieldFilter for FilterContainsAll {
    fn to_string(&self) -> String {
        format!("contains_all({})", self.values.string())
    }

    fn in_values(&self) -> Option<&InValues> {
        Some(&self.values)
    }

    fn in_values_mut(&mut self) -> Option<&mut InValues> {
        Some(&mut self.values)
    }

    fn new_with_values(&self, field_name: &str, values: Vec<String>) -> Option<Box<dyn Filter>> {
        Some(Box::new(new_filter_contains_all_values(field_name, values)))
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_all_phrases(v, &self.values.values)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        if self.values.is_empty() || self.values.is_only_empty_value() {
            return;
        }

        let r = br.get_column_by_name(field_name);
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0);
            if !match_all_phrases(v, &self.values.values) {
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
                let non_empty = self.values.get_non_empty_values_len();
                match_column_by_all_bin_values(br, bm, r, bin_values, non_empty);
            }
            ValueType::UINT16 => {
                let bin_values = self.values.get_uint16_values();
                let non_empty = self.values.get_non_empty_values_len();
                match_column_by_all_bin_values(br, bm, r, bin_values, non_empty);
            }
            ValueType::UINT32 => {
                let bin_values = self.values.get_uint32_values();
                let non_empty = self.values.get_non_empty_values_len();
                match_column_by_all_bin_values(br, bm, r, bin_values, non_empty);
            }
            ValueType::UINT64 => {
                let bin_values = self.values.get_uint64_values();
                let non_empty = self.values.get_non_empty_values_len();
                match_column_by_all_bin_values(br, bm, r, bin_values, non_empty);
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
        if self.values.is_empty() || self.values.is_only_empty_value() {
            return;
        }

        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_all_phrases(&v, &self.values.values) {
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
                if !match_all_phrases(b"", &self.values.values) {
                    bm.reset_bits();
                }
                return;
            }
        };

        let tokens = self.values.get_tokens_hashes_all();

        match ch.value_type {
            ValueType::STRING => match_all_phrases_string(bs, &ch, bm, &self.values.values, tokens),
            ValueType::DICT => match_all_phrases_dict(bs, &ch, bm, &self.values.values),
            ValueType::UINT8 => {
                let bin_values = self.values.get_uint8_values();
                let non_empty = self.values.get_non_empty_values_len();
                match_all_values(bs, &ch, bm, bin_values, non_empty, tokens);
            }
            ValueType::UINT16 => {
                let bin_values = self.values.get_uint16_values();
                let non_empty = self.values.get_non_empty_values_len();
                match_all_values(bs, &ch, bm, bin_values, non_empty, tokens);
            }
            ValueType::UINT32 => {
                let bin_values = self.values.get_uint32_values();
                let non_empty = self.values.get_non_empty_values_len();
                match_all_values(bs, &ch, bm, bin_values, non_empty, tokens);
            }
            ValueType::UINT64 => {
                let bin_values = self.values.get_uint64_values();
                let non_empty = self.values.get_non_empty_values_len();
                match_all_values(bs, &ch, bm, bin_values, non_empty, tokens);
            }
            ValueType::INT64 => match_all_phrases_int64(bs, &ch, bm, &self.values.values, tokens),
            ValueType::FLOAT64 => {
                match_all_phrases_float64(bs, &ch, bm, &self.values.values, tokens)
            }
            ValueType::IPV4 => match_all_phrases_ipv4(bs, &ch, bm, &self.values.values, tokens),
            ValueType::TIMESTAMP_ISO8601 => {
                match_all_phrases_timestamp_iso8601(bs, &ch, bm, &self.values.values, tokens)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

/// Port of Go `matchColumnByAllBinValues`.
fn match_column_by_all_bin_values(
    br: &mut BlockResult,
    bm: &mut Bitmap,
    r: ColRef,
    bin_values: &HashSet<Vec<u8>>,
    non_empty_values_len: usize,
) {
    if non_empty_values_len == 0 {
        return;
    }
    if non_empty_values_len != 1 || non_empty_values_len != bin_values.len() {
        bm.reset_bits();
        return;
    }
    let bin_value = bin_values.iter().next().unwrap().clone();
    let values_encoded = br
        .column_get_values_encoded(r)
        .expect("BUG: non-const, non-time column must have values_encoded");
    bm.for_each_set_bit(|idx| values_encoded[idx] == bin_value);
}

/// Port of Go `matchAllValues`.
fn match_all_values(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    bin_values: &HashSet<Vec<u8>>,
    non_empty_values_len: usize,
    tokens: &[u64],
) {
    if non_empty_values_len == 0 {
        return;
    }
    if non_empty_values_len != 1 || non_empty_values_len != bin_values.len() {
        bm.reset_bits();
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let bin_value = bin_values.iter().next().unwrap().clone();
    visit_values(bs, ch, bm, |v| v == bin_value);
}

fn match_all_phrases_string(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.is_empty() {
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let values = bs.get_values_for_column(ch);
    bm.for_each_set_bit(|idx| match_all_phrases(&values[idx], phrases));
}

fn match_all_phrases_int64(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.is_empty() {
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_int64_string(&pp, &mut bb, v);
        match_all_phrases(&bb, phrases)
    });
}

fn match_all_phrases_float64(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.is_empty() {
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_float64_string(&pp, &mut bb, v);
        match_all_phrases(&bb, phrases)
    });
}

fn match_all_phrases_ipv4(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.is_empty() {
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_ipv4_string(&pp, &mut bb, v);
        match_all_phrases(&bb, phrases)
    });
}

fn match_all_phrases_timestamp_iso8601(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.is_empty() {
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_timestamp_iso8601_string(&pp, &mut bb, v);
        match_all_phrases(&bb, phrases)
    });
}

fn match_all_phrases_dict(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_all_phrases(v, phrases)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

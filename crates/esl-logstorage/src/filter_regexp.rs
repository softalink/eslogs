//! Port of EsLogs `lib/logstorage/filter_regexp.go`.
//!
//! `FilterRegexp` matches the given regexp.

use std::sync::OnceLock;

use esl_common::bytesutil::to_unsafe_string;
use esl_common::panicf;
use esl_common::regexutil::Regex;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::bloomfilter::append_tokens_hashes;
use crate::filter::FieldFilter;
use crate::filter_generic::{
    FilterGeneric, clone_column_header, new_filter_generic, skip_first_last_token,
};
use crate::filter_phrase::{
    apply_to_block_result_generic, match_bloom_filter_all_tokens, match_encoded_values_dict,
    to_float64_string, to_ipv4_string, to_timestamp_iso8601_string, visit_values,
};
use crate::filter_prefix::{
    to_int64_string, to_uint8_string, to_uint16_string, to_uint32_string, to_uint64_string,
};
use crate::rows::{Field, get_field_value_by_name};
use crate::stream_filter::quote_token_if_needed;
use crate::tokenizer::tokenize_strings;
use crate::values_encoder::ValueType;

/// `FilterRegexp` matches the given regexp.
///
/// Example LogsQL: `re("regexp")`.
///
/// PORT NOTE: Go's `filterRegexp` holds only `re *regexutil.Regex` and derives
/// its `String()` from `re.String()`. The Rust `regexutil::Regex` does not
/// expose its source expression, so `new_filter_regexp` also receives `re_str`
/// (the original expression, which the caller/parser already has). `re_str`
/// equals what Go's `re.String()` returns, so `to_string` output is identical.
pub(crate) struct FilterRegexp {
    re: Regex,
    re_str: String,

    tokens_hashes: OnceLock<Vec<u64>>,
}

pub(crate) fn new_filter_regexp(field_name: &str, re: Regex, re_str: String) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterRegexp {
            re,
            re_str,
            tokens_hashes: OnceLock::new(),
        }),
    )
}

impl FilterRegexp {
    fn get_tokens_hashes(&self) -> &[u64] {
        self.tokens_hashes.get_or_init(|| {
            let literals = self.re.get_literals();
            let skipped: Vec<&str> = literals.iter().map(|l| skip_first_last_token(l)).collect();
            let mut toks: Vec<&str> = Vec::new();
            tokenize_strings(&mut toks, &skipped);
            let tokens: Vec<String> = toks.iter().map(|s| s.to_string()).collect();
            let mut hashes = Vec::new();
            append_tokens_hashes(&mut hashes, &tokens);
            hashes
        })
    }
}

impl FieldFilter for FilterRegexp {
    fn to_string(&self) -> String {
        format!("~{}", quote_token_if_needed(&self.re_str))
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        self.re.match_string(v)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        apply_to_block_result_generic(br, bm, field_name, "", |v, _| self.re.match_string(v));
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        // Verify whether filter matches const column.
        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !self.re.match_string(&v) {
                bm.reset_bits();
            }
            return;
        }

        // Verify whether filter matches other columns.
        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns.
                if !self.re.match_string("") {
                    bm.reset_bits();
                }
                return;
            }
        };

        let tokens = self.get_tokens_hashes().to_vec();
        let re = &self.re;

        match ch.value_type {
            ValueType::STRING => match_string_by_regexp(bs, &ch, bm, re, &tokens),
            ValueType::DICT => match_values_dict_by_regexp(bs, &ch, bm, re),
            ValueType::UINT8 => match_uint8_by_regexp(bs, &ch, bm, re, &tokens),
            ValueType::UINT16 => match_uint16_by_regexp(bs, &ch, bm, re, &tokens),
            ValueType::UINT32 => match_uint32_by_regexp(bs, &ch, bm, re, &tokens),
            ValueType::UINT64 => match_uint64_by_regexp(bs, &ch, bm, re, &tokens),
            ValueType::INT64 => match_int64_by_regexp(bs, &ch, bm, re, &tokens),
            ValueType::FLOAT64 => match_float64_by_regexp(bs, &ch, bm, re, &tokens),
            ValueType::IPV4 => match_ipv4_by_regexp(bs, &ch, bm, re, &tokens),
            ValueType::TIMESTAMP_ISO8601 => {
                match_timestamp_iso8601_by_regexp(bs, &ch, bm, re, &tokens)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

fn match_timestamp_iso8601_by_regexp(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    re: &Regex,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_timestamp_iso8601_string(&pp, &mut bb, v);
        re.match_string(to_unsafe_string(&bb))
    });
}

fn match_ipv4_by_regexp(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    re: &Regex,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_ipv4_string(&pp, &mut bb, v);
        re.match_string(to_unsafe_string(&bb))
    });
}

fn match_float64_by_regexp(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    re: &Regex,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_float64_string(&pp, &mut bb, v);
        re.match_string(to_unsafe_string(&bb))
    });
}

fn match_values_dict_by_regexp(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    re: &Regex,
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(re.match_string(v)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

fn match_string_by_regexp(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    re: &Regex,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    visit_values(bs, ch, bm, |v| re.match_string(to_unsafe_string(v)));
}

fn match_uint8_by_regexp(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    re: &Regex,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_uint8_string(&pp, &mut bb, v);
        re.match_string(to_unsafe_string(&bb))
    });
}

fn match_uint16_by_regexp(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    re: &Regex,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_uint16_string(&pp, &mut bb, v);
        re.match_string(to_unsafe_string(&bb))
    });
}

fn match_uint32_by_regexp(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    re: &Regex,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_uint32_string(&pp, &mut bb, v);
        re.match_string(to_unsafe_string(&bb))
    });
}

fn match_uint64_by_regexp(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    re: &Regex,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_uint64_string(&pp, &mut bb, v);
        re.match_string(to_unsafe_string(&bb))
    });
}

fn match_int64_by_regexp(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    re: &Regex,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let pp = bs.part_path();
    visit_values(bs, ch, bm, |v| {
        let mut bb = Vec::new();
        to_int64_string(&pp, &mut bb, v);
        re.match_string(to_unsafe_string(&bb))
    });
}

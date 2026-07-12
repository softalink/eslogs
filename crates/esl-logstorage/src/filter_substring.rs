//! Port of EsLogs `lib/logstorage/filter_substring.go`.
//!
//! `FilterSubstring` filters field entries by substring match. An empty
//! substring matches any string.

use std::sync::OnceLock;

use esl_common::bytesutil::to_unsafe_string;
use esl_common::panicf;

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
use crate::tokenizer::tokenize_strings;
use crate::values_encoder::{
    ValueType, try_parse_float64_exact, try_parse_int64, try_parse_uint64,
};

// ---------------------------------------------------------------------------
// FilterSubstring
// ---------------------------------------------------------------------------

/// `FilterSubstring` filters field entries by substring match.
pub(crate) struct FilterSubstring {
    pub(crate) substring: String,
    tokens_hashes: OnceLock<Vec<u64>>,
}

/// Builds a substring filter for `field_name`.
pub(crate) fn new_filter_substring(field_name: &str, substring: &str) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterSubstring {
            substring: substring.to_string(),
            tokens_hashes: OnceLock::new(),
        }),
    )
}

impl FilterSubstring {
    pub(crate) fn get_tokens_hashes(&self) -> &[u64] {
        self.tokens_hashes.get_or_init(|| {
            let s = skip_first_last_token(&self.substring);
            let mut toks: Vec<&str> = Vec::new();
            tokenize_strings(&mut toks, std::slice::from_ref(&s));
            let tokens: Vec<String> = toks.into_iter().map(|t| t.to_string()).collect();
            let mut hashes = Vec::new();
            append_tokens_hashes(&mut hashes, &tokens);
            hashes
        })
    }
}

impl FieldFilter for FilterSubstring {
    fn to_string(&self) -> String {
        format!(
            "*{}*",
            crate::stream_filter::quote_token_if_needed(&self.substring)
        )
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_substring(v, &self.substring)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        apply_to_block_result_generic(br, bm, field_name, &self.substring, |v, substring| {
            match_substring(v, substring)
        });
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let substring = self.substring.clone();

        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_substring(&v, &substring) {
                bm.reset_bits();
            }
            return;
        }

        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns. It matches
                // anything only for empty substring.
                if !substring.is_empty() {
                    bm.reset_bits();
                }
                return;
            }
        };

        let tokens = self.get_tokens_hashes().to_vec();

        match ch.value_type {
            ValueType::STRING => match_string_by_substring(bs, &ch, bm, &substring, &tokens),
            ValueType::DICT => match_values_dict_by_substring(bs, &ch, bm, &substring),
            ValueType::UINT8 => match_uint8_by_substring(bs, &ch, bm, &substring, &tokens),
            ValueType::UINT16 => match_uint16_by_substring(bs, &ch, bm, &substring, &tokens),
            ValueType::UINT32 => match_uint32_by_substring(bs, &ch, bm, &substring, &tokens),
            ValueType::UINT64 => match_uint64_by_substring(bs, &ch, bm, &substring, &tokens),
            ValueType::INT64 => match_int64_by_substring(bs, &ch, bm, &substring, &tokens),
            ValueType::FLOAT64 => match_float64_by_substring(bs, &ch, bm, &substring, &tokens),
            ValueType::IPV4 => match_ipv4_by_substring(bs, &ch, bm, &substring, &tokens),
            ValueType::TIMESTAMP_ISO8601 => {
                match_timestamp_iso8601_by_substring(bs, &ch, bm, &substring, &tokens)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// block_search helpers (Go filter_substring.go)
// ---------------------------------------------------------------------------

pub(crate) fn match_string_by_substring(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    substring: &str,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    visit_values(bs, ch, bm, |v| {
        match_substring(to_unsafe_string(v), substring)
    });
}

pub(crate) fn match_values_dict_by_substring(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    substring: &str,
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_substring(v, substring)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

pub(crate) fn match_uint8_by_substring(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    substring: &str,
    tokens: &[u64],
) {
    if substring.is_empty() {
        return;
    }
    match try_parse_uint64(substring) {
        Some(n) if n <= ch.max_value => {}
        _ => {
            bm.reset_bits();
            return;
        }
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint8_string(&part_path, &mut buf, v);
        match_substring(to_unsafe_string(&buf), substring)
    });
}

pub(crate) fn match_uint16_by_substring(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    substring: &str,
    tokens: &[u64],
) {
    if substring.is_empty() {
        return;
    }
    match try_parse_uint64(substring) {
        Some(n) if n <= ch.max_value => {}
        _ => {
            bm.reset_bits();
            return;
        }
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint16_string(&part_path, &mut buf, v);
        match_substring(to_unsafe_string(&buf), substring)
    });
}

pub(crate) fn match_uint32_by_substring(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    substring: &str,
    tokens: &[u64],
) {
    if substring.is_empty() {
        return;
    }
    match try_parse_uint64(substring) {
        Some(n) if n <= ch.max_value => {}
        _ => {
            bm.reset_bits();
            return;
        }
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint32_string(&part_path, &mut buf, v);
        match_substring(to_unsafe_string(&buf), substring)
    });
}

pub(crate) fn match_uint64_by_substring(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    substring: &str,
    tokens: &[u64],
) {
    if substring.is_empty() {
        return;
    }
    match try_parse_uint64(substring) {
        Some(n) if n <= ch.max_value => {}
        _ => {
            bm.reset_bits();
            return;
        }
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint64_string(&part_path, &mut buf, v);
        match_substring(to_unsafe_string(&buf), substring)
    });
}

pub(crate) fn match_int64_by_substring(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    substring: &str,
    tokens: &[u64],
) {
    if substring.is_empty() {
        return;
    }
    if substring != "-" {
        match try_parse_int64(substring) {
            Some(n) if n >= ch.min_value as i64 && n <= ch.max_value as i64 => {}
            _ => {
                bm.reset_bits();
                return;
            }
        }
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_int64_string(&part_path, &mut buf, v);
        match_substring(to_unsafe_string(&buf), substring)
    });
}

pub(crate) fn match_float64_by_substring(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    substring: &str,
    tokens: &[u64],
) {
    if substring.is_empty() {
        return;
    }
    let ok = try_parse_float64_exact(substring).is_some();
    if !ok
        && !substring.contains('.')
        && !substring.contains('+')
        && !substring.contains('-')
        && !substring.contains('e')
        && !substring.contains('E')
    {
        bm.reset_bits();
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_float64_string(&part_path, &mut buf, v);
        match_substring(to_unsafe_string(&buf), substring)
    });
}

pub(crate) fn match_ipv4_by_substring(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    substring: &str,
    tokens: &[u64],
) {
    if substring.is_empty() {
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_ipv4_string(&part_path, &mut buf, v);
        match_substring(to_unsafe_string(&buf), substring)
    });
}

pub(crate) fn match_timestamp_iso8601_by_substring(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    substring: &str,
    tokens: &[u64],
) {
    if substring.is_empty() {
        return;
    }
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_timestamp_iso8601_string(&part_path, &mut buf, v);
        match_substring(to_unsafe_string(&buf), substring)
    });
}

/// Port of Go `matchSubstring`.
pub(crate) fn match_substring(s: &str, substring: &str) -> bool {
    if substring.is_empty() {
        // Special case - empty substring matches anything.
        return true;
    }
    if substring.len() > s.len() {
        // Fast path - the substring is too long.
        return false;
    }
    s.contains(substring)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_substring() {
        fn f(s: &str, substring: &str, result_expected: bool) {
            let result = match_substring(s, substring);
            assert_eq!(result, result_expected, "s={s:?} substring={substring:?}");
        }

        f("", "", true);
        f("foo", "", true);
        f("", "foo", false);
        f("foo", "foo", true);
        f("foo bar", "foo", true);
        f("foo bar", "bar", true);
        f("a foo bar", "foo", true);
        f("a foo bar", "fo", true);
        f("a foo bar", "oo", true);
        f("a foo bar", "goo", false);
        f("foobar", "foo", true);
        f("foobar", "bar", true);
        f("foobar", "oob", true);
        f("foobar", "boob", false);
        f("afoobar foo", "foo", true);
        f("раз два (три!)", "три", true);
        f("", "foo bar", false);
        f("foo bar", "foo bar", true);
        f("(foo bar)", "foo bar", true);
        f("afoo bar", "foo bar", true);
        f("afoo bar", "afoo ba", true);
        f("foo bar! baz", "foo bar!", true);
        f("a.foo bar! baz", ".foo bar! ", true);
        f("foo bar! baz", "foo bar! b", true);
        f("255.255.255.255", "5", true);
        f("255.255.255.255", "55", true);
        f("255.255.255.255", "355", false);
        f("255.255.255.255", "255", true);
        f("255.255.255.255", "5.255", true);
        f("255.255.255.255", "255.25", true);
        f("255.255.255.255", "255.255", true);
        f("255.255.255.255", "255.2557", false);
    }
}

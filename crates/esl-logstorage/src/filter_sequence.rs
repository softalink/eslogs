//! Port of EsLogs `lib/logstorage/filter_sequence.go`.
//!
//! `FilterSequence` matches an ordered sequence of phrases. Example LogsQL:
//! `seq(foo, "bar baz")`.

use std::sync::OnceLock;

use esl_common::bytesutil::to_unsafe_string;
use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::bloomfilter::append_tokens_hashes;
use crate::filter::FieldFilter;
use crate::filter_exact::{
    match_int64_by_exact_value, match_uint8_by_exact_value, match_uint16_by_exact_value,
    match_uint32_by_exact_value, match_uint64_by_exact_value,
};
use crate::filter_generic::{FilterGeneric, clone_column_header, new_filter_generic};
use crate::filter_phrase::{
    apply_to_block_result_generic, get_phrase_pos, match_bloom_filter_all_tokens,
    match_encoded_values_dict, match_ipv4_by_phrase, match_timestamp_iso8601_by_phrase,
    to_float64_string, to_ipv4_string, to_timestamp_iso8601_string, visit_values,
};
use crate::rows::{Field, get_field_value_by_name};
use crate::tokenizer::tokenize_strings;
use crate::values_encoder::ValueType;

// ---------------------------------------------------------------------------
// FilterSequence
// ---------------------------------------------------------------------------

/// `FilterSequence` matches an ordered sequence of phrases.
pub(crate) struct FilterSequence {
    pub(crate) phrases: Vec<String>,
    non_empty_phrases: OnceLock<Vec<String>>,
    tokens_hashes: OnceLock<Vec<u64>>,
}

/// Builds a sequence filter for `field_name`.
pub(crate) fn new_filter_sequence(field_name: &str, phrases: Vec<String>) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterSequence {
            phrases,
            non_empty_phrases: OnceLock::new(),
            tokens_hashes: OnceLock::new(),
        }),
    )
}

impl FilterSequence {
    fn get_non_empty_phrases(&self) -> &[String] {
        self.non_empty_phrases.get_or_init(|| {
            self.phrases
                .iter()
                .filter(|p| !p.is_empty())
                .cloned()
                .collect()
        })
    }

    fn get_tokens_hashes(&self) -> &[u64] {
        self.tokens_hashes.get_or_init(|| {
            let phrases = self.get_non_empty_phrases();
            let mut toks: Vec<&str> = Vec::new();
            tokenize_strings(&mut toks, phrases);
            let tokens: Vec<String> = toks.into_iter().map(|s| s.to_string()).collect();
            let mut hashes = Vec::new();
            append_tokens_hashes(&mut hashes, &tokens);
            hashes
        })
    }
}

impl FieldFilter for FilterSequence {
    fn to_string(&self) -> String {
        let a: Vec<String> = self
            .phrases
            .iter()
            .map(|p| crate::stream_filter::quote_token_if_needed(p))
            .collect();
        format!("seq({})", a.join(","))
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let phrases = self.get_non_empty_phrases();
        let v = get_field_value_by_name(fields, field_name);
        match_sequence(v, phrases)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let phrases = self.get_non_empty_phrases();
        if phrases.is_empty() {
            return;
        }
        apply_to_block_result_generic(br, bm, field_name, "", |v, _| match_sequence(v, phrases));
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let phrases = self.get_non_empty_phrases().to_vec();

        if phrases.is_empty() {
            return;
        }

        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_sequence(&v, &phrases) {
                bm.reset_bits();
            }
            return;
        }

        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns. It matches
                // anything only for empty phrase.
                if !match_sequence("", &phrases) {
                    bm.reset_bits();
                }
                return;
            }
        };

        let tokens = self.get_tokens_hashes().to_vec();

        match ch.value_type {
            ValueType::STRING => match_string_by_sequence(bs, &ch, bm, &phrases, &tokens),
            ValueType::DICT => match_values_dict_by_sequence(bs, &ch, bm, &phrases),
            ValueType::UINT8 => match_uint8_by_sequence(bs, &ch, bm, &phrases, &tokens),
            ValueType::UINT16 => match_uint16_by_sequence(bs, &ch, bm, &phrases, &tokens),
            ValueType::UINT32 => match_uint32_by_sequence(bs, &ch, bm, &phrases, &tokens),
            ValueType::UINT64 => match_uint64_by_sequence(bs, &ch, bm, &phrases, &tokens),
            ValueType::INT64 => match_int64_by_sequence(bs, &ch, bm, &phrases, &tokens),
            ValueType::FLOAT64 => match_float64_by_sequence(bs, &ch, bm, &phrases, &tokens),
            ValueType::IPV4 => match_ipv4_by_sequence(bs, &ch, bm, &phrases, &tokens),
            ValueType::TIMESTAMP_ISO8601 => {
                match_timestamp_iso8601_by_sequence(bs, &ch, bm, &phrases, &tokens)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// block_search helpers (Go filter_sequence.go)
// ---------------------------------------------------------------------------

pub(crate) fn match_timestamp_iso8601_by_sequence(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.len() == 1 {
        match_timestamp_iso8601_by_phrase(bs, ch, bm, &phrases[0], tokens);
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
        match_sequence(to_unsafe_string(&buf), phrases)
    });
}

pub(crate) fn match_ipv4_by_sequence(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.len() == 1 {
        match_ipv4_by_phrase(bs, ch, bm, &phrases[0], tokens);
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
        match_sequence(to_unsafe_string(&buf), phrases)
    });
}

pub(crate) fn match_float64_by_sequence(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_float64_string(&part_path, &mut buf, v);
        match_sequence(to_unsafe_string(&buf), phrases)
    });
}

pub(crate) fn match_values_dict_by_sequence(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_sequence(v, phrases)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

pub(crate) fn match_string_by_sequence(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    visit_values(bs, ch, bm, |v| match_sequence(to_unsafe_string(v), phrases));
}

pub(crate) fn match_uint8_by_sequence(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.len() > 1 {
        bm.reset_bits();
        return;
    }
    match_uint8_by_exact_value(bs, ch, bm, &phrases[0], tokens);
}

pub(crate) fn match_uint16_by_sequence(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.len() > 1 {
        bm.reset_bits();
        return;
    }
    match_uint16_by_exact_value(bs, ch, bm, &phrases[0], tokens);
}

pub(crate) fn match_uint32_by_sequence(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.len() > 1 {
        bm.reset_bits();
        return;
    }
    match_uint32_by_exact_value(bs, ch, bm, &phrases[0], tokens);
}

pub(crate) fn match_uint64_by_sequence(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.len() > 1 {
        bm.reset_bits();
        return;
    }
    match_uint64_by_exact_value(bs, ch, bm, &phrases[0], tokens);
}

pub(crate) fn match_int64_by_sequence(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrases: &[String],
    tokens: &[u64],
) {
    if phrases.len() > 1 {
        bm.reset_bits();
        return;
    }
    match_int64_by_exact_value(bs, ch, bm, &phrases[0], tokens);
}

/// Port of Go `matchSequence`.
pub(crate) fn match_sequence<S: AsRef<str>>(s: &str, phrases: &[S]) -> bool {
    let mut s = s;
    for phrase in phrases {
        let phrase = phrase.as_ref();
        match get_phrase_pos(s, phrase) {
            Some(n) => s = &s[n + phrase.len()..],
            None => return false,
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_sequence() {
        fn f(s: &str, phrases: &[&str], result_expected: bool) {
            let result = match_sequence(s, phrases);
            assert_eq!(result, result_expected, "s={s:?} phrases={phrases:?}");
        }

        f("", &[""], true);
        f("foo", &[""], true);
        f("", &["foo"], false);
        f("foo", &["foo"], true);
        f("foo bar", &["foo"], true);
        f("foo bar", &["bar"], true);
        f("foo bar", &["foo bar"], true);
        f("foo bar", &["foo", "bar"], true);
        f("foo bar", &["foo", " bar"], true);
        f("foo bar", &["foo ", "bar"], true);
        f("foo bar", &["foo ", " bar"], false);
        f("foo bar", &["bar", "foo"], false);
    }
}

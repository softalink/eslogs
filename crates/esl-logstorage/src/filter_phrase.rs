//! Port of EsLogs `lib/logstorage/filter_phrase.go`.
//!
//! `FilterPhrase` filters field entries by phrase match (full text search).
//! This module also hosts the `pub(crate)` block-search / block-result value
//! helpers shared across the whole filter subsystem (`visit_values`,
//! `match_bloom_filter_all_tokens`, `match_encoded_values_dict`,
//! `apply_to_block_result_generic`, the `to_*_string` decoders, …).

use std::sync::OnceLock;

use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::{BlockResult, ColRef};
use crate::block_search::BlockSearch;
use crate::bloomfilter::append_tokens_hashes;
use crate::filter::FieldFilter;
use crate::filter_exact::{
    match_float64_by_exact_value, match_int64_by_exact_value, match_ipv4_by_exact_value,
    match_timestamp_iso8601_by_exact_value, match_uint8_by_exact_value,
    match_uint16_by_exact_value, match_uint32_by_exact_value, match_uint64_by_exact_value,
};
use crate::filter_generic::{
    FilterGeneric, RUNE_ERROR, index_bytes, new_filter_generic, rune_at, rune_before,
};
use crate::rows::{Field, get_field_value_by_name};
use crate::tokenizer::{is_token_rune, tokenize_strings};
use crate::values_encoder::{
    ValueType, marshal_float64_string, marshal_ipv4_string, marshal_timestamp_iso8601_string,
    try_parse_float64_exact, try_parse_int64, try_parse_ipv4, try_parse_timestamp_iso8601,
    try_parse_uint64, unmarshal_float64, unmarshal_ipv4, unmarshal_timestamp_iso8601,
};

// ---------------------------------------------------------------------------
// FilterPhrase
// ---------------------------------------------------------------------------

/// `FilterPhrase` filters field entries by phrase match (aka full text search).
///
/// An empty phrase matches only an empty string. A single-word phrase is the
/// simplest LogsQL query: `word`. The special case `""` matches any log entry
/// without the given field.
pub(crate) struct FilterPhrase {
    pub(crate) phrase: String,

    /// Cached token hashes (Go `filterPhrase.tokensHashes` behind `tokensOnce`).
    ///
    /// PORT NOTE: Go also caches `tokens` (`[]string`) for `getTokens`, consumed
    /// by the And/Or common-token optimization. That optimization requires
    /// `filter`-interface type introspection, deferred with the parser port, so
    /// only the token hashes needed by `applyToBlockSearch` are cached here.
    tokens_hashes: OnceLock<Vec<u64>>,
}

/// Builds a phrase filter for `field_name`.
pub(crate) fn new_filter_phrase(field_name: &str, phrase: &str) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterPhrase {
            phrase: phrase.to_string(),
            tokens_hashes: OnceLock::new(),
        }),
    )
}

impl FilterPhrase {
    pub(crate) fn get_tokens_hashes(&self) -> &[u64] {
        self.tokens_hashes.get_or_init(|| {
            let mut toks: Vec<&str> = Vec::new();
            tokenize_strings(&mut toks, std::slice::from_ref(&self.phrase));
            let tokens: Vec<String> = toks.into_iter().map(|s| s.to_string()).collect();
            let mut hashes = Vec::new();
            append_tokens_hashes(&mut hashes, &tokens);
            hashes
        })
    }
}

impl FieldFilter for FilterPhrase {
    fn to_string(&self) -> String {
        crate::stream_filter::quote_token_if_needed(&self.phrase)
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_phrase(v, &self.phrase)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        apply_to_block_result_generic(br, bm, field_name, &self.phrase, |v, phrase| {
            match_phrase(v, phrase)
        });
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let phrase = self.phrase.clone();

        // Verify whether fp matches const column.
        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_phrase(&v, &phrase) {
                bm.reset_bits();
            }
            return;
        }

        // Verify whether fp matches other columns.
        let ch = match bs.get_column_header(field_name) {
            Some(ch) => crate::filter_generic::clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns. It matches
                // anything only for empty phrase.
                if !phrase.is_empty() {
                    bm.reset_bits();
                }
                return;
            }
        };

        let tokens = self.get_tokens_hashes().to_vec();

        match ch.value_type {
            ValueType::STRING => match_string_by_phrase(bs, &ch, bm, &phrase, &tokens),
            ValueType::DICT => match_values_dict_by_phrase(bs, &ch, bm, &phrase),
            ValueType::UINT8 => match_uint8_by_exact_value(bs, &ch, bm, &phrase, &tokens),
            ValueType::UINT16 => match_uint16_by_exact_value(bs, &ch, bm, &phrase, &tokens),
            ValueType::UINT32 => match_uint32_by_exact_value(bs, &ch, bm, &phrase, &tokens),
            ValueType::UINT64 => match_uint64_by_exact_value(bs, &ch, bm, &phrase, &tokens),
            ValueType::INT64 => match_int64_by_exact_value(bs, &ch, bm, &phrase, &tokens),
            ValueType::FLOAT64 => match_float64_by_phrase(bs, &ch, bm, &phrase, &tokens),
            ValueType::IPV4 => match_ipv4_by_phrase(bs, &ch, bm, &phrase, &tokens),
            ValueType::TIMESTAMP_ISO8601 => {
                match_timestamp_iso8601_by_phrase(bs, &ch, bm, &phrase, &tokens)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// Phrase-specific block-search matchers
// ---------------------------------------------------------------------------

pub(crate) fn match_timestamp_iso8601_by_phrase(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrase: &str,
    tokens: &[u64],
) {
    if try_parse_timestamp_iso8601(phrase).is_some() {
        // Fast path - the phrase contains complete timestamp, so exact search.
        match_timestamp_iso8601_by_exact_value(bs, ch, bm, phrase, tokens);
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
        match_phrase(&buf, phrase)
    });
}

pub(crate) fn match_ipv4_by_phrase(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrase: &str,
    tokens: &[u64],
) {
    if try_parse_ipv4(phrase).is_some() {
        // Fast path - phrase contains the full IP address, so exact matching.
        match_ipv4_by_exact_value(bs, ch, bm, phrase, tokens);
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
        match_phrase(&buf, phrase)
    });
}

pub(crate) fn match_float64_by_phrase(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrase: &str,
    tokens: &[u64],
) {
    // The phrase may contain a part of the floating-point number, so search in
    // the string representation.
    let ok = try_parse_float64_exact(phrase).is_some();
    if !ok && phrase != "." && phrase != "+" && phrase != "-" {
        bm.reset_bits();
        return;
    }
    if matches!(phrase.find('.'), Some(n) if n > 0 && n < phrase.len() - 1) {
        // Fast path - the phrase contains the exact floating-point number.
        match_float64_by_exact_value(bs, ch, bm, phrase, tokens);
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
        match_phrase(&buf, phrase)
    });
}

pub(crate) fn match_values_dict_by_phrase(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrase: &str,
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_phrase(v, phrase)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

pub(crate) fn match_string_by_phrase(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrase: &str,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    visit_values(bs, ch, bm, |v| match_phrase(v, phrase));
}

// ---------------------------------------------------------------------------
// Phrase matching primitives
// ---------------------------------------------------------------------------

/// Port of Go `matchPhrase`.
///
/// Both the haystack `s` and the phrase are raw value bytes (Go strings are
/// arbitrary bytes); `&str` phrases coerce via `AsRef<[u8]>`.
pub(crate) fn match_phrase(s: &[u8], phrase: impl AsRef<[u8]>) -> bool {
    let phrase = phrase.as_ref();
    if phrase.is_empty() {
        // Special case - empty phrase matches only empty string.
        return s.is_empty();
    }
    get_phrase_pos(s, phrase).is_some()
}

/// Port of Go `getPhrasePos`: returns the byte offset of the phrase within `s`
/// with token boundaries respected, or `None` if the phrase is not found.
pub(crate) fn get_phrase_pos(s: &[u8], phrase: impl AsRef<[u8]>) -> Option<usize> {
    let pb = phrase.as_ref();
    if pb.is_empty() {
        return Some(0);
    }
    let sb = s;
    if pb.len() > sb.len() {
        return None;
    }

    let starts_with_token = is_token_rune(rune_at(pb, 0));
    let ends_with_token = is_token_rune(rune_before(pb, pb.len()));

    let mut pos = 0usize;
    loop {
        let n = index_bytes(&sb[pos..], pb)?;
        pos += n;
        // Make sure that the found phrase contains non-token chars at the
        // beginning and at the end.
        if starts_with_token && pos > 0 {
            let r = rune_before(sb, pos);
            if r == RUNE_ERROR || is_token_rune(r) {
                pos += 1;
                continue;
            }
        }
        if ends_with_token && pos + pb.len() < sb.len() {
            let r = rune_at(sb, pos + pb.len());
            if r == RUNE_ERROR || is_token_rune(r) {
                pos += 1;
                continue;
            }
        }
        return Some(pos);
    }
}

// ---------------------------------------------------------------------------
// Shared block-search column helpers (Go filter_phrase.go)
// ---------------------------------------------------------------------------

/// Port of Go `matchEncodedValuesDict`.
pub(crate) fn match_encoded_values_dict(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    encoded_values: &[u8],
) {
    if !encoded_values.contains(&1) {
        // Fast path - the phrase is missing in the valuesDict.
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    // Slow path - iterate over values.
    visit_values(bs, ch, bm, |v| {
        if v.len() != 1 {
            panicf!(
                "FATAL: {}: unexpected length for dict value: got {}; want 1",
                part_path,
                v.len()
            );
        }
        let idx = v[0] as usize;
        if idx >= encoded_values.len() {
            panicf!(
                "FATAL: {}: too big index for dict value; got {}; must be smaller than {}",
                part_path,
                idx,
                encoded_values.len()
            );
        }
        encoded_values[idx] == 1
    });
}

/// Port of Go `visitValues`.
///
/// PORT NOTE: Go's callback receives the encoded value as a `string`; the port
/// passes the raw `&[u8]` (the encoded values are binary — see the block_search
/// "Encoded-values representation" contract). String matchers reinterpret it via
/// `to_unsafe_string`; numeric matchers decode it via the `to_*_string` helpers.
pub(crate) fn visit_values(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    mut f: impl FnMut(&[u8]) -> bool,
) {
    if bm.is_zero() {
        // Fast path - nothing to visit.
        return;
    }
    let values = bs.get_values_for_column(ch);
    bm.for_each_set_bit(|idx| f(&values[idx]));
}

/// Port of Go `matchBloomFilterAllTokens`.
pub(crate) fn match_bloom_filter_all_tokens(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    tokens: &[u64],
) -> bool {
    if tokens.is_empty() {
        return true;
    }
    bs.bloom_contains_all(ch, tokens)
}

/// Port of Go `toFloat64String`.
pub(crate) fn to_float64_string(part_path: &str, buf: &mut Vec<u8>, v: &[u8]) {
    if v.len() != 8 {
        panicf!(
            "FATAL: {}: unexpected length for binary representation of floating-point number: got {}; want 8",
            part_path,
            v.len()
        );
    }
    let f = unmarshal_float64(v);
    buf.clear();
    marshal_float64_string(buf, f);
}

/// Port of Go `toIPv4String`.
pub(crate) fn to_ipv4_string(part_path: &str, buf: &mut Vec<u8>, v: &[u8]) {
    if v.len() != 4 {
        panicf!(
            "FATAL: {}: unexpected length for binary representation of IPv4: got {}; want 4",
            part_path,
            v.len()
        );
    }
    let ip = unmarshal_ipv4(v);
    buf.clear();
    marshal_ipv4_string(buf, ip);
}

/// Port of Go `toTimestampISO8601String`.
pub(crate) fn to_timestamp_iso8601_string(part_path: &str, buf: &mut Vec<u8>, v: &[u8]) {
    if v.len() != 8 {
        panicf!(
            "FATAL: {}: unexpected length for binary representation of ISO8601 timestamp: got {}; want 8",
            part_path,
            v.len()
        );
    }
    let timestamp = unmarshal_timestamp_iso8601(v);
    buf.clear();
    marshal_timestamp_iso8601_string(buf, timestamp);
}

// ---------------------------------------------------------------------------
// Shared block-result helpers (Go filter_phrase.go)
// ---------------------------------------------------------------------------

/// Port of Go `applyToBlockResultGeneric`.
///
/// PORT NOTE: Go's `valueTypeDict` case builds a per-dict-entry match table and
/// maps encoded indices through it. `BlockResult` does not expose the private
/// `dictValues`; the port instead routes the dict case through
/// `match_column_by_generic`, which matches the already-decoded per-row values.
/// The result is identical (only the dict fast-path micro-optimization differs).
pub(crate) fn apply_to_block_result_generic<F>(
    br: &mut BlockResult,
    bm: &mut Bitmap,
    field_name: &str,
    phrase: &str,
    match_func: F,
) where
    F: Fn(&[u8], &str) -> bool,
{
    let r = br.get_column_by_name(field_name);
    if br.column_is_const(r) {
        let v = br.column_get_value_at_row(r, 0);
        let matched = match_func(v, phrase);
        if !matched {
            bm.reset_bits();
        }
        return;
    }
    if br.column_is_time(r) {
        match_column_by_generic(br, bm, r, phrase, &match_func);
        return;
    }

    match br.column_value_type(r) {
        ValueType::STRING | ValueType::DICT => {
            match_column_by_generic(br, bm, r, phrase, &match_func)
        }
        ValueType::UINT8 => match try_parse_uint64(phrase) {
            Some(n) if n < (1 << 8) => match_column_by_generic(br, bm, r, phrase, &match_func),
            _ => bm.reset_bits(),
        },
        ValueType::UINT16 => match try_parse_uint64(phrase) {
            Some(n) if n < (1 << 16) => match_column_by_generic(br, bm, r, phrase, &match_func),
            _ => bm.reset_bits(),
        },
        ValueType::UINT32 => match try_parse_uint64(phrase) {
            Some(n) if n < (1 << 32) => match_column_by_generic(br, bm, r, phrase, &match_func),
            _ => bm.reset_bits(),
        },
        ValueType::UINT64 => {
            if try_parse_uint64(phrase).is_some() {
                match_column_by_generic(br, bm, r, phrase, &match_func);
            } else {
                bm.reset_bits();
            }
        }
        ValueType::INT64 => {
            if try_parse_int64(phrase).is_some() {
                match_column_by_generic(br, bm, r, phrase, &match_func);
            } else {
                bm.reset_bits();
            }
        }
        ValueType::FLOAT64 | ValueType::IPV4 | ValueType::TIMESTAMP_ISO8601 => {
            match_column_by_generic(br, bm, r, phrase, &match_func)
        }
        other => panicf!("FATAL: unknown valueType={}", other.0),
    }
}

/// Port of Go `matchColumnByPhraseGeneric`.
pub(crate) fn match_column_by_generic<F>(
    br: &mut BlockResult,
    bm: &mut Bitmap,
    r: ColRef,
    phrase: &str,
    match_func: &F,
) where
    F: Fn(&[u8], &str) -> bool,
{
    let values = br.column_get_values(r);
    bm.for_each_set_bit(|idx| match_func(&values[idx], phrase));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_phrase() {
        fn f(s: &str, phrase: &str, result_expected: bool) {
            let result = match_phrase(s.as_bytes(), phrase);
            assert_eq!(result, result_expected, "s={s:?} phrase={phrase:?}");
        }

        f("", "", true);
        f("foo", "", false);
        f("", "foo", false);
        f("foo", "foo", true);
        f("foo bar", "foo", true);
        f("foo bar", "bar", true);
        f("a foo bar", "foo", true);
        f("a foo bar", "fo", false);
        f("a foo bar", "oo", false);
        f("foobar", "foo", false);
        f("foobar", "bar", false);
        f("foobar", "oob", false);
        f("afoobar foo", "foo", true);
        f("раз два (три!)", "три", true);
        f("", "foo bar", false);
        f("foo bar", "foo bar", true);
        f("(foo bar)", "foo bar", true);
        f("afoo bar", "foo bar", false);
        f("afoo bar", "afoo ba", false);
        f("foo bar! baz", "foo bar!", true);
        f("a.foo bar! baz", ".foo bar! ", true);
        f("foo bar! baz", "foo bar! b", false);
        f("255.255.255.255", "5", false);
        f("255.255.255.255", "55", false);
        f("255.255.255.255", "255", true);
        f("255.255.255.255", "5.255", false);
        f("255.255.255.255", "255.25", false);
        f("255.255.255.255", "255.255", true);
    }
}

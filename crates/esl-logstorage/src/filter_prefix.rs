//! Port of EsLogs `lib/logstorage/filter_prefix.go`.
//!
//! `FilterPrefix` matches the given prefix. This module also hosts the
//! `pub(crate)` `to_uint*_string` / `to_int64_string` decoders shared with the
//! other value-oriented filters.

use std::sync::OnceLock;

use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::bloomfilter::append_tokens_hashes;
use crate::filter::FieldFilter;
use crate::filter_generic::{
    FilterGeneric, RUNE_ERROR, clone_column_header, get_tokens_skip_last_bytes, index_bytes,
    new_filter_generic, rune_at, rune_before,
};
use crate::filter_phrase::{
    apply_to_block_result_generic, match_bloom_filter_all_tokens, match_encoded_values_dict,
    to_float64_string, to_ipv4_string, to_timestamp_iso8601_string, visit_values,
};
use crate::rows::{Field, get_field_value_by_name};
use crate::tokenizer::is_token_rune;
use crate::values_encoder::{
    ValueType, marshal_int64_string, marshal_uint8_string, marshal_uint16_string,
    marshal_uint32_string, marshal_uint64_string, try_parse_float64_exact, try_parse_int64,
    try_parse_uint64, unmarshal_int64, unmarshal_uint8, unmarshal_uint16, unmarshal_uint32,
    unmarshal_uint64,
};

// ---------------------------------------------------------------------------
// FilterPrefix
// ---------------------------------------------------------------------------

/// `FilterPrefix` matches the given prefix. Example LogsQL: `prefix*` or
/// `"some prefix"*`. The special case `*` matches a non-empty value.
pub(crate) struct FilterPrefix {
    /// The prefix to match. Raw bytes like Go's `string` (a double-quoted
    /// `"\xff"` escape in query text denotes the raw byte 0xFF).
    pub(crate) prefix: Vec<u8>,
    tokens_hashes: OnceLock<Vec<u64>>,
}

/// Builds a prefix filter for `field_name`.
pub(crate) fn new_filter_prefix(field_name: &[u8], prefix: impl AsRef<[u8]>) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterPrefix {
            prefix: prefix.as_ref().to_vec(),
            tokens_hashes: OnceLock::new(),
        }),
    )
}

impl FilterPrefix {
    pub(crate) fn get_tokens_hashes(&self) -> &[u64] {
        self.tokens_hashes.get_or_init(|| {
            let tokens = get_tokens_skip_last_bytes(&self.prefix);
            let mut hashes = Vec::new();
            append_tokens_hashes(&mut hashes, &tokens);
            hashes
        })
    }
}

impl FieldFilter for FilterPrefix {
    fn is_empty_prefix(&self) -> bool {
        self.prefix.is_empty()
    }

    fn to_string(&self) -> String {
        if self.prefix.is_empty() {
            return "*".to_string();
        }
        // Lossless render: invalid UTF-8 re-quotes via Go strconv.Quote byte
        // semantics (`\xNN`), so parse -> render -> re-parse is stable.
        format!(
            "{}*",
            crate::stream_filter::quote_value_bytes_if_needed(&self.prefix)
        )
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &[u8]) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_prefix(v, &self.prefix)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &[u8],
    ) {
        apply_to_block_result_generic(br, bm, field_name, &self.prefix, |v, prefix| {
            match_prefix(v, prefix)
        });
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &[u8],
    ) {
        let prefix = self.prefix.clone();

        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_prefix(&v, &prefix) {
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

        let tokens = self.get_tokens_hashes().to_vec();

        match ch.value_type {
            ValueType::STRING => match_string_by_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::DICT => match_values_dict_by_prefix(bs, &ch, bm, &prefix),
            ValueType::UINT8 => match_uint8_by_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::UINT16 => match_uint16_by_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::UINT32 => match_uint32_by_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::UINT64 => match_uint64_by_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::INT64 => match_int64_by_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::FLOAT64 => match_float64_by_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::IPV4 => match_ipv4_by_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::TIMESTAMP_ISO8601 => {
                match_timestamp_iso8601_by_prefix(bs, &ch, bm, &prefix, &tokens)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// block_search helpers (Go filter_prefix.go)
// ---------------------------------------------------------------------------

pub(crate) fn match_timestamp_iso8601_by_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
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
        match_prefix(&buf, prefix)
    });
}

pub(crate) fn match_ipv4_by_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
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
        match_prefix(&buf, prefix)
    });
}

pub(crate) fn match_float64_by_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        return;
    }
    let ok = crate::filter_phrase::phrase_utf8(prefix)
        .and_then(try_parse_float64_exact)
        .is_some();
    if !ok
        && prefix != b"."
        && prefix != b"+"
        && prefix != b"-"
        && !prefix.starts_with(b"e")
        && !prefix.starts_with(b"E")
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
        match_prefix(&buf, prefix)
    });
}

pub(crate) fn match_values_dict_by_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
) {
    let prefix = prefix.as_ref();
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_prefix(v, prefix)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

pub(crate) fn match_string_by_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    visit_values(bs, ch, bm, |v| match_prefix(v, &prefix));
}

pub(crate) fn match_uint8_by_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        return;
    }
    match crate::filter_phrase::phrase_utf8(prefix).and_then(try_parse_uint64) {
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
        match_prefix(&buf, prefix)
    });
}

pub(crate) fn match_uint16_by_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        return;
    }
    match crate::filter_phrase::phrase_utf8(prefix).and_then(try_parse_uint64) {
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
        match_prefix(&buf, prefix)
    });
}

pub(crate) fn match_uint32_by_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        return;
    }
    match crate::filter_phrase::phrase_utf8(prefix).and_then(try_parse_uint64) {
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
        match_prefix(&buf, prefix)
    });
}

pub(crate) fn match_uint64_by_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        return;
    }
    match crate::filter_phrase::phrase_utf8(prefix).and_then(try_parse_uint64) {
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
        match_prefix(&buf, prefix)
    });
}

pub(crate) fn match_int64_by_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        return;
    }
    if prefix != b"-" {
        match crate::filter_phrase::phrase_utf8(prefix).and_then(try_parse_int64) {
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
        match_prefix(&buf, prefix)
    });
}

/// Port of Go `matchPrefix`.
///
/// The haystack `s` is raw value bytes (Go strings are arbitrary bytes).
pub(crate) fn match_prefix(s: &[u8], prefix: impl AsRef<[u8]>) -> bool {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        // Special case - empty prefix matches any non-empty string.
        return !s.is_empty();
    }
    let sb = s;
    let pb = prefix;
    if pb.len() > sb.len() {
        return false;
    }

    let starts_with_token = is_token_rune(rune_at(pb, 0));
    let mut offset = 0usize;
    loop {
        let n = match index_bytes(&sb[offset..], pb) {
            Some(n) => n,
            None => return false,
        };
        offset += n;
        // Make sure that the found phrase has non-token chars at the beginning.
        if starts_with_token && offset > 0 {
            let r = rune_before(sb, offset);
            if r == RUNE_ERROR || is_token_rune(r) {
                offset += 1;
                continue;
            }
        }
        return true;
    }
}

// ---------------------------------------------------------------------------
// uint / int decoders (Go filter_prefix.go)
// ---------------------------------------------------------------------------

/// Port of Go `toUint8String`.
pub(crate) fn to_uint8_string(part_path: &str, buf: &mut Vec<u8>, v: &[u8]) {
    if v.len() != 1 {
        panicf!(
            "FATAL: {}: unexpected length for binary representation of uint8 number: got {}; want 1",
            part_path,
            v.len()
        );
    }
    let n = unmarshal_uint8(v);
    buf.clear();
    marshal_uint8_string(buf, n);
}

/// Port of Go `toUint16String`.
pub(crate) fn to_uint16_string(part_path: &str, buf: &mut Vec<u8>, v: &[u8]) {
    if v.len() != 2 {
        panicf!(
            "FATAL: {}: unexpected length for binary representation of uint16 number: got {}; want 2",
            part_path,
            v.len()
        );
    }
    let n = unmarshal_uint16(v);
    buf.clear();
    marshal_uint16_string(buf, n);
}

/// Port of Go `toUint32String`.
pub(crate) fn to_uint32_string(part_path: &str, buf: &mut Vec<u8>, v: &[u8]) {
    if v.len() != 4 {
        panicf!(
            "FATAL: {}: unexpected length for binary representation of uint32 number: got {}; want 4",
            part_path,
            v.len()
        );
    }
    let n = unmarshal_uint32(v);
    buf.clear();
    marshal_uint32_string(buf, n);
}

/// Port of Go `toUint64String`.
pub(crate) fn to_uint64_string(part_path: &str, buf: &mut Vec<u8>, v: &[u8]) {
    if v.len() != 8 {
        panicf!(
            "FATAL: {}: unexpected length for binary representation of uint64 number: got {}; want 8",
            part_path,
            v.len()
        );
    }
    let n = unmarshal_uint64(v);
    buf.clear();
    marshal_uint64_string(buf, n);
}

/// Port of Go `toInt64String`.
pub(crate) fn to_int64_string(part_path: &str, buf: &mut Vec<u8>, v: &[u8]) {
    if v.len() != 8 {
        panicf!(
            "FATAL: {}: unexpected length for binary representation of int64 number; got {}; want 8",
            part_path,
            v.len()
        );
    }
    let n = unmarshal_int64(v);
    buf.clear();
    marshal_int64_string(buf, n);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_prefix() {
        fn f(s: &str, prefix: &str, result_expected: bool) {
            let result = match_prefix(s.as_bytes(), prefix);
            assert_eq!(result, result_expected, "s={s:?} prefix={prefix:?}");
        }

        f("", "", false);
        f("foo", "", true);
        f("", "foo", false);
        f("foo", "foo", true);
        f("foo bar", "foo", true);
        f("foo bar", "bar", true);
        f("a foo bar", "foo", true);
        f("a foo bar", "fo", true);
        f("a foo bar", "oo", false);
        f("foobar", "foo", true);
        f("foobar", "bar", false);
        f("foobar", "oob", false);
        f("afoobar foo", "foo", true);
        f("раз два (три!)", "три", true);
        f("", "foo bar", false);
        f("foo bar", "foo bar", true);
        f("(foo bar)", "foo bar", true);
        f("afoo bar", "foo bar", false);
        f("afoo bar", "afoo ba", true);
        f("foo bar! baz", "foo bar!", true);
        f("a.foo bar! baz", ".foo bar! ", true);
        f("foo bar! baz", "foo bar! b", true);
        f("255.255.255.255", "5", false);
        f("255.255.255.255", "55", false);
        f("255.255.255.255", "255", true);
        f("255.255.255.255", "5.255", false);
        f("255.255.255.255", "255.25", true);
        f("255.255.255.255", "255.255", true);
    }
}

//! Port of EsLogs `lib/logstorage/filter_exact_prefix.go`.
//!
//! `FilterExactPrefix` matches the exact prefix. Example LogsQL: `="foo bar"*`.

use std::sync::OnceLock;

use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::bloomfilter::{BLOOM_FILTER_HASHES_COUNT, append_tokens_hashes};
use crate::filter::FieldFilter;
use crate::filter_generic::{
    FilterGeneric, clone_column_header, get_tokens_skip_last_bytes, new_filter_generic,
};
use crate::filter_phrase::{
    apply_to_block_result_generic, match_bloom_filter_all_tokens, match_encoded_values_dict,
    to_float64_string, to_ipv4_string, to_timestamp_iso8601_string, visit_values,
};
use crate::filter_prefix::{
    to_int64_string, to_uint8_string, to_uint16_string, to_uint32_string, to_uint64_string,
};
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::{ValueType, try_parse_int64, try_parse_uint64};

// ---------------------------------------------------------------------------
// FilterExactPrefix
// ---------------------------------------------------------------------------

/// `FilterExactPrefix` matches the exact prefix. Example LogsQL: `="foo bar"*`.
pub(crate) struct FilterExactPrefix {
    /// The exact prefix to match. Raw bytes like Go's `string` (a
    /// double-quoted `"\xff"` escape in query text denotes the raw byte 0xFF).
    pub(crate) prefix: Vec<u8>,
    tokens_hashes: OnceLock<Vec<u64>>,
}

/// Builds an exact-prefix filter for `field_name`.
pub(crate) fn new_filter_exact_prefix(
    field_name: &[u8],
    prefix: impl AsRef<[u8]>,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterExactPrefix {
            prefix: prefix.as_ref().to_vec(),
            tokens_hashes: OnceLock::new(),
        }),
    )
}

impl FilterExactPrefix {
    pub(crate) fn get_tokens_hashes(&self) -> &[u64] {
        self.tokens_hashes.get_or_init(|| {
            let tokens = get_tokens_skip_last_bytes(&self.prefix);
            let mut hashes = Vec::new();
            append_tokens_hashes(&mut hashes, &tokens);
            hashes
        })
    }
}

impl FieldFilter for FilterExactPrefix {
    fn to_string(&self) -> String {
        // Lossless render: invalid UTF-8 re-quotes via Go strconv.Quote byte
        // semantics (`\xNN`), so parse -> render -> re-parse is stable.
        format!(
            "={}*",
            crate::stream_filter::quote_value_bytes_if_needed(&self.prefix)
        )
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &[u8]) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_exact_prefix(v, &self.prefix)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &[u8],
    ) {
        apply_to_block_result_generic(br, bm, field_name, &self.prefix, |v, prefix| {
            match_exact_prefix(v, prefix)
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
            if !match_exact_prefix(&v, &prefix) {
                bm.reset_bits();
            }
            return;
        }

        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns.
                if !match_exact_prefix(b"", &prefix) {
                    bm.reset_bits();
                }
                return;
            }
        };

        let tokens = self.get_tokens_hashes().to_vec();

        match ch.value_type {
            ValueType::STRING => match_string_by_exact_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::DICT => match_values_dict_by_exact_prefix(bs, &ch, bm, &prefix),
            ValueType::UINT8 => match_uint8_by_exact_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::UINT16 => match_uint16_by_exact_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::UINT32 => match_uint32_by_exact_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::UINT64 => match_uint64_by_exact_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::INT64 => match_int64_by_exact_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::FLOAT64 => match_float64_by_exact_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::IPV4 => match_ipv4_by_exact_prefix(bs, &ch, bm, &prefix, &tokens),
            ValueType::TIMESTAMP_ISO8601 => {
                match_timestamp_iso8601_by_exact_prefix(bs, &ch, bm, &prefix, &tokens)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// block_search helpers (Go filter_exact_prefix.go)
// ---------------------------------------------------------------------------

pub(crate) fn match_timestamp_iso8601_by_exact_prefix(
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
    if !(&b"0"[..]..=&b"9"[..]).contains(&prefix) || !match_bloom_filter_all_tokens(bs, ch, tokens)
    {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_timestamp_iso8601_string(&part_path, &mut buf, v);
        match_exact_prefix(&buf, prefix)
    });
}

pub(crate) fn match_ipv4_by_exact_prefix(
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
    if !(&b"0"[..]..=&b"9"[..]).contains(&prefix)
        || tokens.len() > 3 * BLOOM_FILTER_HASHES_COUNT
        || !match_bloom_filter_all_tokens(bs, ch, tokens)
    {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_ipv4_string(&part_path, &mut buf, v);
        match_exact_prefix(&buf, prefix)
    });
}

pub(crate) fn match_float64_by_exact_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        // An empty prefix matches all the values.
        return;
    }
    if tokens.len() > 2 * BLOOM_FILTER_HASHES_COUNT
        || !match_bloom_filter_all_tokens(bs, ch, tokens)
    {
        bm.reset_bits();
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_float64_string(&part_path, &mut buf, v);
        match_exact_prefix(&buf, prefix)
    });
}

pub(crate) fn match_values_dict_by_exact_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
) {
    let prefix = prefix.as_ref();
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_exact_prefix(v, prefix)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

pub(crate) fn match_string_by_exact_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    visit_values(bs, ch, bm, |v| match_exact_prefix(v, prefix));
}

pub(crate) fn match_uint8_by_exact_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if !match_min_max_exact_prefix(ch, bm, prefix, tokens) {
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint8_string(&part_path, &mut buf, v);
        match_exact_prefix(&buf, prefix)
    });
}

pub(crate) fn match_uint16_by_exact_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if !match_min_max_exact_prefix(ch, bm, prefix, tokens) {
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint16_string(&part_path, &mut buf, v);
        match_exact_prefix(&buf, prefix)
    });
}

pub(crate) fn match_uint32_by_exact_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if !match_min_max_exact_prefix(ch, bm, prefix, tokens) {
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint32_string(&part_path, &mut buf, v);
        match_exact_prefix(&buf, prefix)
    });
}

pub(crate) fn match_uint64_by_exact_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if !match_min_max_exact_prefix(ch, bm, prefix, tokens) {
        return;
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_uint64_string(&part_path, &mut buf, v);
        match_exact_prefix(&buf, prefix)
    });
}

pub(crate) fn match_int64_by_exact_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        // An empty prefix matches all the values.
        return;
    }
    if !tokens.is_empty() {
        // Non-empty tokens means that the prefix contains at least two tokens.
        // Multiple tokens cannot match any uint value.
        bm.reset_bits();
        return;
    }
    if prefix != b"-" {
        match crate::filter_phrase::phrase_utf8(prefix).and_then(try_parse_int64) {
            Some(n) if n <= ch.max_value as i64 && n >= ch.min_value as i64 => {}
            _ => {
                bm.reset_bits();
                return;
            }
        }
    }
    let part_path = bs.part_path();
    let mut buf = Vec::new();
    visit_values(bs, ch, bm, |v| {
        to_int64_string(&part_path, &mut buf, v);
        match_exact_prefix(&buf, prefix)
    });
}

/// Port of Go `matchMinMaxExactPrefix`.
pub(crate) fn match_min_max_exact_prefix(
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix: impl AsRef<[u8]>,
    tokens: &[u64],
) -> bool {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        // An empty prefix matches all the values.
        return false;
    }
    if !tokens.is_empty() {
        // Non-empty tokens means that the prefix contains at least two tokens.
        // Multiple tokens cannot match any uint value.
        bm.reset_bits();
        return false;
    }
    match crate::filter_phrase::phrase_utf8(prefix).and_then(try_parse_uint64) {
        Some(n) if n <= ch.max_value => true,
        _ => {
            bm.reset_bits();
            false
        }
    }
}

/// Port of Go `matchExactPrefix`.
pub(crate) fn match_exact_prefix(s: &[u8], prefix: impl AsRef<[u8]>) -> bool {
    s.starts_with(prefix.as_ref())
}

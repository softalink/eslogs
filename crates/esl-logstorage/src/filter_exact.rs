//! Port of EsLogs `lib/logstorage/filter_exact.go`.
//!
//! `FilterExact` matches the exact value. This module also hosts the
//! `pub(crate)` `match_*_by_exact_value` / `match_binary_value` helpers reused
//! by `filter_phrase.rs` and `filter_sequence.rs`.

use std::sync::OnceLock;

use esl_common::encoding;
use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::{BlockResult, ColRef};
use crate::block_search::BlockSearch;
use crate::bloomfilter::append_tokens_hashes;
use crate::filter::FieldFilter;
use crate::filter_generic::{FilterGeneric, clone_column_header, new_filter_generic};
use crate::filter_phrase::{
    match_bloom_filter_all_tokens, match_encoded_values_dict, visit_values,
};
use crate::rows::{Field, get_field_value_by_name};
use crate::tokenizer::tokenize_bytes;
use crate::values_encoder::{
    ValueType, marshal_float64, try_parse_float64_exact, try_parse_int64, try_parse_ipv4,
    try_parse_timestamp_iso8601, try_parse_uint64, unmarshal_float64, unmarshal_int64,
    unmarshal_ipv4, unmarshal_timestamp_iso8601, unmarshal_uint8, unmarshal_uint16,
    unmarshal_uint32, unmarshal_uint64,
};

// ---------------------------------------------------------------------------
// FilterExact
// ---------------------------------------------------------------------------

/// `FilterExact` matches the exact value. Example LogsQL: `exact("foo bar")` or
/// `="foo bar"`.
pub(crate) struct FilterExact {
    /// The exact value to match. Raw bytes like Go's `string` (a double-quoted
    /// `"\xff"` escape in query text denotes the raw byte 0xFF).
    pub(crate) value: Vec<u8>,
    tokens_hashes: OnceLock<Vec<u64>>,
}

/// Builds an exact filter for `field_name`.
pub(crate) fn new_filter_exact(field_name: &str, value: impl AsRef<[u8]>) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterExact {
            value: value.as_ref().to_vec(),
            tokens_hashes: OnceLock::new(),
        }),
    )
}

impl FilterExact {
    pub(crate) fn get_tokens_hashes(&self) -> &[u64] {
        self.tokens_hashes.get_or_init(|| {
            // Byte tokenizer: matches the ingest-side hash_tokenizer, so
            // bloom lookups agree for raw-byte values too.
            let mut toks: Vec<&[u8]> = Vec::new();
            tokenize_bytes(&mut toks, std::slice::from_ref(&self.value));
            let mut hashes = Vec::new();
            append_tokens_hashes(&mut hashes, &toks);
            hashes
        })
    }
}

impl FieldFilter for FilterExact {
    fn to_string(&self) -> String {
        // Lossless render: invalid UTF-8 re-quotes via Go strconv.Quote byte
        // semantics (`\xNN`), so parse -> render -> re-parse is stable.
        format!(
            "={}",
            crate::stream_filter::quote_value_bytes_if_needed(&self.value)
        )
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        v == self.value.as_slice()
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let value = self.value.clone();
        let value_str = crate::filter_phrase::phrase_utf8(&value);

        let r = br.get_column_by_name(field_name);
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0);
            if v != value.as_slice() {
                bm.reset_bits();
            }
            return;
        }
        if br.column_is_time(r) {
            match_column_by_exact_value(br, bm, r, &value);
            return;
        }

        match br.column_value_type(r) {
            // PORT NOTE: Go special-cases DICT with a per-entry match table over
            // the private `dictValues`; the port routes it through the decoded
            // per-row values (identical result).
            ValueType::STRING | ValueType::DICT => match_column_by_exact_value(br, bm, r, &value),
            ValueType::UINT8 => {
                let n_needed = match value_str.and_then(try_parse_uint64) {
                    Some(n) if n < (1 << 8) => n as u8,
                    _ => {
                        bm.reset_bits();
                        return;
                    }
                };
                let ve = br.column_get_values_encoded(r).unwrap();
                bm.for_each_set_bit(|idx| unmarshal_uint8(&ve[idx]) == n_needed);
            }
            ValueType::UINT16 => {
                let n_needed = match value_str.and_then(try_parse_uint64) {
                    Some(n) if n < (1 << 16) => n as u16,
                    _ => {
                        bm.reset_bits();
                        return;
                    }
                };
                let ve = br.column_get_values_encoded(r).unwrap();
                bm.for_each_set_bit(|idx| unmarshal_uint16(&ve[idx]) == n_needed);
            }
            ValueType::UINT32 => {
                let n_needed = match value_str.and_then(try_parse_uint64) {
                    Some(n) if n < (1 << 32) => n as u32,
                    _ => {
                        bm.reset_bits();
                        return;
                    }
                };
                let ve = br.column_get_values_encoded(r).unwrap();
                bm.for_each_set_bit(|idx| unmarshal_uint32(&ve[idx]) == n_needed);
            }
            ValueType::UINT64 => {
                let n_needed = match value_str.and_then(try_parse_uint64) {
                    Some(n) => n,
                    None => {
                        bm.reset_bits();
                        return;
                    }
                };
                let ve = br.column_get_values_encoded(r).unwrap();
                bm.for_each_set_bit(|idx| unmarshal_uint64(&ve[idx]) == n_needed);
            }
            ValueType::INT64 => {
                let n_needed = match value_str.and_then(try_parse_int64) {
                    Some(n) => n,
                    None => {
                        bm.reset_bits();
                        return;
                    }
                };
                let ve = br.column_get_values_encoded(r).unwrap();
                bm.for_each_set_bit(|idx| unmarshal_int64(&ve[idx]) == n_needed);
            }
            ValueType::FLOAT64 => {
                let f_needed = match value_str.and_then(try_parse_float64_exact) {
                    Some(f) => f,
                    None => {
                        bm.reset_bits();
                        return;
                    }
                };
                let ve = br.column_get_values_encoded(r).unwrap();
                bm.for_each_set_bit(|idx| unmarshal_float64(&ve[idx]) == f_needed);
            }
            ValueType::IPV4 => {
                let ip_needed = match value_str.and_then(try_parse_ipv4) {
                    Some(ip) => ip,
                    None => {
                        bm.reset_bits();
                        return;
                    }
                };
                let ve = br.column_get_values_encoded(r).unwrap();
                bm.for_each_set_bit(|idx| unmarshal_ipv4(&ve[idx]) == ip_needed);
            }
            ValueType::TIMESTAMP_ISO8601 => {
                let ts_needed = match value_str.and_then(try_parse_timestamp_iso8601) {
                    Some(t) => t,
                    None => {
                        bm.reset_bits();
                        return;
                    }
                };
                let ve = br.column_get_values_encoded(r).unwrap();
                bm.for_each_set_bit(|idx| unmarshal_timestamp_iso8601(&ve[idx]) == ts_needed);
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
        let value = self.value.clone();

        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if value != v {
                bm.reset_bits();
            }
            return;
        }

        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns. It matches
                // anything only for empty value.
                if !value.is_empty() {
                    bm.reset_bits();
                }
                return;
            }
        };

        let tokens = self.get_tokens_hashes().to_vec();

        match ch.value_type {
            ValueType::STRING => match_string_by_exact_value(bs, &ch, bm, &value, &tokens),
            ValueType::DICT => match_values_dict_by_exact_value(bs, &ch, bm, &value),
            ValueType::UINT8 => match_uint8_by_exact_value(bs, &ch, bm, &value, &tokens),
            ValueType::UINT16 => match_uint16_by_exact_value(bs, &ch, bm, &value, &tokens),
            ValueType::UINT32 => match_uint32_by_exact_value(bs, &ch, bm, &value, &tokens),
            ValueType::UINT64 => match_uint64_by_exact_value(bs, &ch, bm, &value, &tokens),
            ValueType::INT64 => match_int64_by_exact_value(bs, &ch, bm, &value, &tokens),
            ValueType::FLOAT64 => match_float64_by_exact_value(bs, &ch, bm, &value, &tokens),
            ValueType::IPV4 => match_ipv4_by_exact_value(bs, &ch, bm, &value, &tokens),
            ValueType::TIMESTAMP_ISO8601 => {
                match_timestamp_iso8601_by_exact_value(bs, &ch, bm, &value, &tokens)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

// ---------------------------------------------------------------------------
// block_result helper
// ---------------------------------------------------------------------------

/// Port of Go `matchColumnByExactValue`.
pub(crate) fn match_column_by_exact_value(
    br: &mut BlockResult,
    bm: &mut Bitmap,
    r: ColRef,
    value: impl AsRef<[u8]>,
) {
    let value = value.as_ref();
    let values = br.column_get_values(r);
    bm.for_each_set_bit(|idx| values[idx].as_slice() == value);
}

// ---------------------------------------------------------------------------
// block_search helpers (Go filter_exact.go)
// ---------------------------------------------------------------------------

pub(crate) fn match_timestamp_iso8601_by_exact_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    value: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let value = value.as_ref();
    let n = match crate::filter_phrase::phrase_utf8(value).and_then(try_parse_timestamp_iso8601) {
        Some(n) if n >= ch.min_value as i64 && n <= ch.max_value as i64 => n,
        _ => {
            bm.reset_bits();
            return;
        }
    };
    let mut bb = Vec::new();
    encoding::marshal_uint64(&mut bb, n as u64);
    match_binary_value(bs, ch, bm, &bb, tokens);
}

pub(crate) fn match_ipv4_by_exact_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    value: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let value = value.as_ref();
    let n = match crate::filter_phrase::phrase_utf8(value).and_then(try_parse_ipv4) {
        Some(n) if (n as u64) >= ch.min_value && (n as u64) <= ch.max_value => n,
        _ => {
            bm.reset_bits();
            return;
        }
    };
    let mut bb = Vec::new();
    encoding::marshal_uint32(&mut bb, n);
    match_binary_value(bs, ch, bm, &bb, tokens);
}

pub(crate) fn match_float64_by_exact_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    value: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let value = value.as_ref();
    let f = match crate::filter_phrase::phrase_utf8(value).and_then(try_parse_float64_exact) {
        Some(f) if f >= f64::from_bits(ch.min_value) && f <= f64::from_bits(ch.max_value) => f,
        _ => {
            bm.reset_bits();
            return;
        }
    };
    let mut bb = Vec::new();
    marshal_float64(&mut bb, f);
    match_binary_value(bs, ch, bm, &bb, tokens);
}

pub(crate) fn match_values_dict_by_exact_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    value: impl AsRef<[u8]>,
) {
    let value = value.as_ref();
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(v.as_slice() == value));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

pub(crate) fn match_string_by_exact_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    value: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let value = value.as_ref();
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    visit_values(bs, ch, bm, |v| v == value);
}

pub(crate) fn match_uint8_by_exact_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    value: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let value = value.as_ref();
    let n = match crate::filter_phrase::phrase_utf8(value).and_then(try_parse_uint64) {
        Some(n) if n >= ch.min_value && n <= ch.max_value => n,
        _ => {
            bm.reset_bits();
            return;
        }
    };
    let bb = vec![n as u8];
    match_binary_value(bs, ch, bm, &bb, tokens);
}

pub(crate) fn match_uint16_by_exact_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    value: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let value = value.as_ref();
    let n = match crate::filter_phrase::phrase_utf8(value).and_then(try_parse_uint64) {
        Some(n) if n >= ch.min_value && n <= ch.max_value => n,
        _ => {
            bm.reset_bits();
            return;
        }
    };
    let mut bb = Vec::new();
    encoding::marshal_uint16(&mut bb, n as u16);
    match_binary_value(bs, ch, bm, &bb, tokens);
}

pub(crate) fn match_uint32_by_exact_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    value: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let value = value.as_ref();
    let n = match crate::filter_phrase::phrase_utf8(value).and_then(try_parse_uint64) {
        Some(n) if n >= ch.min_value && n <= ch.max_value => n,
        _ => {
            bm.reset_bits();
            return;
        }
    };
    let mut bb = Vec::new();
    encoding::marshal_uint32(&mut bb, n as u32);
    match_binary_value(bs, ch, bm, &bb, tokens);
}

pub(crate) fn match_uint64_by_exact_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    value: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let value = value.as_ref();
    let n = match crate::filter_phrase::phrase_utf8(value).and_then(try_parse_uint64) {
        Some(n) if n >= ch.min_value && n <= ch.max_value => n,
        _ => {
            bm.reset_bits();
            return;
        }
    };
    let mut bb = Vec::new();
    encoding::marshal_uint64(&mut bb, n);
    match_binary_value(bs, ch, bm, &bb, tokens);
}

pub(crate) fn match_int64_by_exact_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    value: impl AsRef<[u8]>,
    tokens: &[u64],
) {
    let value = value.as_ref();
    let n = match crate::filter_phrase::phrase_utf8(value).and_then(try_parse_int64) {
        Some(n) if n >= ch.min_value as i64 && n <= ch.max_value as i64 => n,
        _ => {
            bm.reset_bits();
            return;
        }
    };
    let mut bb = Vec::new();
    encoding::marshal_int64(&mut bb, n);
    match_binary_value(bs, ch, bm, &bb, tokens);
}

/// Port of Go `matchBinaryValue`.
pub(crate) fn match_binary_value(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    bin_value: &[u8],
    tokens: &[u64],
) {
    if !match_bloom_filter_all_tokens(bs, ch, tokens) {
        bm.reset_bits();
        return;
    }
    visit_values(bs, ch, bm, |v| v == bin_value);
}

//! Port of EsLogs `lib/logstorage/filter_any_case_prefix.go`.
//!
//! `FilterAnyCasePrefix` matches the given prefix in lower, upper and mixed case.

use std::sync::OnceLock;

use esl_common::panicf;
use esl_common::stringsutil::append_lowercase;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::bloomfilter::append_tokens_hashes;
use crate::filter::FieldFilter;
use crate::filter_any_case_phrase::is_ascii_lowercase;
use crate::filter_generic::{FilterGeneric, get_tokens_skip_last, new_filter_generic};
use crate::filter_phrase::{
    apply_to_block_result_generic, match_encoded_values_dict, visit_values,
};
use crate::filter_prefix::{
    match_float64_by_prefix, match_int64_by_prefix, match_ipv4_by_prefix, match_prefix,
    match_timestamp_iso8601_by_prefix, match_uint8_by_prefix, match_uint16_by_prefix,
    match_uint32_by_prefix, match_uint64_by_prefix,
};
use crate::rows::{Field, get_field_value_by_name};
use crate::stream_filter::quote_token_if_needed;
use crate::values_encoder::ValueType;

/// `FilterAnyCasePrefix` matches the given prefix in lower, upper and mixed case.
///
/// Example LogsQL: `i(prefix*)` or `i("some prefix"*)`. A special case `i(*)`
/// equals `*` and matches a non-empty value.
pub(crate) struct FilterAnyCasePrefix {
    prefix: String,

    prefix_lowercase: OnceLock<String>,
    prefix_uppercase: OnceLock<String>,

    /// Cached `(tokens_hashes, tokens_uppercase_hashes)` (Go `tokensOnce`).
    tokens: OnceLock<(Vec<u64>, Vec<u64>)>,
}

pub(crate) fn new_filter_any_case_prefix(field_name: &str, prefix: &str) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterAnyCasePrefix {
            prefix: prefix.to_string(),
            prefix_lowercase: OnceLock::new(),
            prefix_uppercase: OnceLock::new(),
            tokens: OnceLock::new(),
        }),
    )
}

impl FilterAnyCasePrefix {
    fn get_prefix_lowercase(&self) -> &str {
        self.prefix_lowercase
            .get_or_init(|| self.prefix.to_lowercase())
    }

    fn get_prefix_uppercase(&self) -> &str {
        self.prefix_uppercase
            .get_or_init(|| self.prefix.to_uppercase())
    }

    fn get_tokens(&self) -> &(Vec<u64>, Vec<u64>) {
        self.tokens.get_or_init(|| {
            let tokens = get_tokens_skip_last(&self.prefix);
            let mut tokens_hashes = Vec::new();
            append_tokens_hashes(&mut tokens_hashes, &tokens);

            let tokens_uppercase: Vec<String> = tokens.iter().map(|t| t.to_uppercase()).collect();
            let mut tokens_uppercase_hashes = Vec::new();
            append_tokens_hashes(&mut tokens_uppercase_hashes, &tokens_uppercase);

            (tokens_hashes, tokens_uppercase_hashes)
        })
    }
}

impl FieldFilter for FilterAnyCasePrefix {
    fn to_string(&self) -> String {
        if self.prefix.is_empty() {
            return "i(*)".to_string();
        }
        format!("i({}*)", quote_token_if_needed(&self.prefix))
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_any_case_prefix(v, self.get_prefix_lowercase())
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let prefix_lowercase = self.get_prefix_lowercase().to_string();
        apply_to_block_result_generic(br, bm, field_name, &prefix_lowercase, match_any_case_prefix);
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let prefix_lowercase = self.get_prefix_lowercase().to_string();

        // Verify whether fp matches const column.
        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_any_case_prefix(&v, &prefix_lowercase) {
                bm.reset_bits();
            }
            return;
        }

        // Verify whether fp matches other columns.
        let ch = match bs.get_column_header(field_name) {
            Some(ch) => crate::filter_generic::clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns.
                bm.reset_bits();
                return;
            }
        };

        let tokens = self.get_tokens().0.clone();

        match ch.value_type {
            ValueType::STRING => match_string_by_any_case_prefix(bs, &ch, bm, &prefix_lowercase),
            ValueType::DICT => match_values_dict_by_any_case_prefix(bs, &ch, bm, &prefix_lowercase),
            ValueType::UINT8 => match_uint8_by_prefix(bs, &ch, bm, &prefix_lowercase, &tokens),
            ValueType::UINT16 => match_uint16_by_prefix(bs, &ch, bm, &prefix_lowercase, &tokens),
            ValueType::UINT32 => match_uint32_by_prefix(bs, &ch, bm, &prefix_lowercase, &tokens),
            ValueType::UINT64 => match_uint64_by_prefix(bs, &ch, bm, &prefix_lowercase, &tokens),
            ValueType::INT64 => match_int64_by_prefix(bs, &ch, bm, &prefix_lowercase, &tokens),
            ValueType::FLOAT64 => match_float64_by_prefix(bs, &ch, bm, &prefix_lowercase, &tokens),
            ValueType::IPV4 => match_ipv4_by_prefix(bs, &ch, bm, &prefix_lowercase, &tokens),
            ValueType::TIMESTAMP_ISO8601 => {
                let prefix_uppercase = self.get_prefix_uppercase().to_string();
                let tokens_uppercase = self.get_tokens().1.clone();
                match_timestamp_iso8601_by_prefix(bs, &ch, bm, &prefix_uppercase, &tokens_uppercase)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

fn match_values_dict_by_any_case_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix_lowercase: &str,
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_any_case_prefix(v, prefix_lowercase)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

fn match_string_by_any_case_prefix(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    prefix_lowercase: &str,
) {
    visit_values(bs, ch, bm, |v| match_any_case_prefix(v, prefix_lowercase));
}

/// Port of Go `matchAnyCasePrefix`.
fn match_any_case_prefix(s: &[u8], prefix_lowercase: &str) -> bool {
    if prefix_lowercase.is_empty() {
        // Special case - empty prefix matches any non-empty string.
        return !s.is_empty();
    }
    if prefix_lowercase.len() > s.len() {
        return false;
    }

    if is_ascii_lowercase(s) {
        // Fast path - s is in lowercase.
        return match_prefix(s, prefix_lowercase);
    }

    // Slow path - convert s to lowercase before matching.
    // Lossy decode matches Go's rune-wise lowercasing (strings.Map/ToLower):
    // invalid bytes decode to U+FFFD before the case mapping is applied.
    let mut bb = Vec::new();
    append_lowercase(&mut bb, &String::from_utf8_lossy(s));
    match_prefix(&bb, prefix_lowercase)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_any_case_prefix() {
        fn f(s: &str, prefix_lowercase: &str, result_expected: bool) {
            let result = match_any_case_prefix(s.as_bytes(), prefix_lowercase);
            assert_eq!(
                result, result_expected,
                "s={s:?} prefix={prefix_lowercase:?}"
            );
        }

        // empty prefix matches non-empty strings
        f("", "", false);
        f("foo", "", true);
        f("тест", "", true);

        // empty string doesn't match non-empty prefix
        f("", "foo", false);
        f("", "тест", false);

        // full match
        f("foo", "foo", true);
        f("FOo", "foo", true);
        f("Test ТЕСт 123", "test тест 123", true);

        // prefix match
        f("foo", "f", true);
        f("foo тест bar", "те", true);
        f("foo ТЕСТ bar", "те", true);

        // mismatch
        f("foo", "o", false);
        f("тест", "foo", false);
        f("Тест", "ест", false);
    }
}

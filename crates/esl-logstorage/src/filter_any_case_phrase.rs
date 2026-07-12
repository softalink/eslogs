//! Port of EsLogs `lib/logstorage/filter_any_case_phrase.go`.
//!
//! `FilterAnyCasePhrase` filters field entries by case-insensitive phrase match.

use std::sync::OnceLock;

use esl_common::bytesutil::to_unsafe_string;
use esl_common::panicf;
use esl_common::stringsutil::append_lowercase;

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
use crate::filter_generic::{FilterGeneric, new_filter_generic};
use crate::filter_phrase::{
    apply_to_block_result_generic, match_encoded_values_dict, match_float64_by_phrase,
    match_ipv4_by_phrase, match_phrase, match_timestamp_iso8601_by_phrase, visit_values,
};
use crate::rows::{Field, get_field_value_by_name};
use crate::stream_filter::quote_token_if_needed;
use crate::tokenizer::tokenize_strings;
use crate::values_encoder::ValueType;

/// `FilterAnyCasePhrase` filters field entries by case-insensitive phrase match.
///
/// An example LogsQL query: `i(word)` or `i("word1 ... wordN")`.
pub(crate) struct FilterAnyCasePhrase {
    phrase: String,

    phrase_lowercase: OnceLock<String>,
    phrase_uppercase: OnceLock<String>,

    /// Cached `(tokens_hashes, tokens_hashes_uppercase)` (Go `tokensOnce`).
    tokens: OnceLock<(Vec<u64>, Vec<u64>)>,
}

pub(crate) fn new_filter_any_case_phrase(field_name: &str, phrase: &str) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterAnyCasePhrase {
            phrase: phrase.to_string(),
            phrase_lowercase: OnceLock::new(),
            phrase_uppercase: OnceLock::new(),
            tokens: OnceLock::new(),
        }),
    )
}

impl FilterAnyCasePhrase {
    fn get_phrase_lowercase(&self) -> &str {
        self.phrase_lowercase
            .get_or_init(|| self.phrase.to_lowercase())
    }

    fn get_phrase_uppercase(&self) -> &str {
        self.phrase_uppercase
            .get_or_init(|| self.phrase.to_uppercase())
    }

    fn get_tokens(&self) -> &(Vec<u64>, Vec<u64>) {
        self.tokens.get_or_init(|| {
            let mut toks: Vec<&str> = Vec::new();
            tokenize_strings(&mut toks, std::slice::from_ref(&self.phrase));
            let mut tokens_hashes = Vec::new();
            append_tokens_hashes(&mut tokens_hashes, &toks);

            let tokens_uppercase: Vec<String> = toks.iter().map(|t| t.to_uppercase()).collect();
            let mut tokens_hashes_uppercase = Vec::new();
            append_tokens_hashes(&mut tokens_hashes_uppercase, &tokens_uppercase);

            (tokens_hashes, tokens_hashes_uppercase)
        })
    }
}

impl FieldFilter for FilterAnyCasePhrase {
    fn to_string(&self) -> String {
        format!("i({})", quote_token_if_needed(&self.phrase))
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        let v = get_field_value_by_name(fields, field_name);
        match_any_case_phrase(v, self.get_phrase_lowercase())
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let phrase_lowercase = self.get_phrase_lowercase().to_string();
        apply_to_block_result_generic(br, bm, field_name, &phrase_lowercase, match_any_case_phrase);
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        let phrase_lowercase = self.get_phrase_lowercase().to_string();

        // Verify whether fp matches const column.
        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            if !match_any_case_phrase(&v, &phrase_lowercase) {
                bm.reset_bits();
            }
            return;
        }

        // Verify whether fp matches other columns.
        let ch = match bs.get_column_header(field_name) {
            Some(ch) => crate::filter_generic::clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns. It matches anything
                // only for the empty phrase.
                if !phrase_lowercase.is_empty() {
                    bm.reset_bits();
                }
                return;
            }
        };

        let tokens = self.get_tokens().0.clone();

        match ch.value_type {
            ValueType::STRING => match_string_by_any_case_phrase(bs, &ch, bm, &phrase_lowercase),
            ValueType::DICT => match_values_dict_by_any_case_phrase(bs, &ch, bm, &phrase_lowercase),
            ValueType::UINT8 => match_uint8_by_exact_value(bs, &ch, bm, &phrase_lowercase, &tokens),
            ValueType::UINT16 => {
                match_uint16_by_exact_value(bs, &ch, bm, &phrase_lowercase, &tokens)
            }
            ValueType::UINT32 => {
                match_uint32_by_exact_value(bs, &ch, bm, &phrase_lowercase, &tokens)
            }
            ValueType::UINT64 => {
                match_uint64_by_exact_value(bs, &ch, bm, &phrase_lowercase, &tokens)
            }
            ValueType::INT64 => match_int64_by_exact_value(bs, &ch, bm, &phrase_lowercase, &tokens),
            ValueType::FLOAT64 => match_float64_by_phrase(bs, &ch, bm, &phrase_lowercase, &tokens),
            ValueType::IPV4 => match_ipv4_by_phrase(bs, &ch, bm, &phrase_lowercase, &tokens),
            ValueType::TIMESTAMP_ISO8601 => {
                let phrase_uppercase = self.get_phrase_uppercase().to_string();
                let tokens_uppercase = self.get_tokens().1.clone();
                match_timestamp_iso8601_by_phrase(bs, &ch, bm, &phrase_uppercase, &tokens_uppercase)
            }
            other => panicf!("FATAL: {}: unknown valueType={}", bs.part_path(), other.0),
        }
    }
}

fn match_values_dict_by_any_case_phrase(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrase_lowercase: &str,
) {
    let mut bb = Vec::with_capacity(ch.values_dict.values.len());
    for v in &ch.values_dict.values {
        bb.push(u8::from(match_any_case_phrase(v, phrase_lowercase)));
    }
    match_encoded_values_dict(bs, ch, bm, &bb);
}

fn match_string_by_any_case_phrase(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    bm: &mut Bitmap,
    phrase_lowercase: &str,
) {
    visit_values(bs, ch, bm, |v| {
        match_any_case_phrase(to_unsafe_string(v), phrase_lowercase)
    });
}

/// Port of Go `matchAnyCasePhrase`.
fn match_any_case_phrase(s: &str, phrase_lowercase: &str) -> bool {
    if phrase_lowercase.is_empty() {
        // Special case - empty phrase matches only empty string.
        return s.is_empty();
    }
    if phrase_lowercase.len() > s.len() {
        return false;
    }

    if is_ascii_lowercase(s) {
        // Fast path - s is in lowercase.
        return match_phrase(s, phrase_lowercase);
    }

    // Slow path - convert s to lowercase before matching.
    let mut bb = Vec::new();
    append_lowercase(&mut bb, s);
    match_phrase(to_unsafe_string(&bb), phrase_lowercase)
}

/// Port of Go `isASCIILowercase`.
pub(crate) fn is_ascii_lowercase(s: &str) -> bool {
    for &c in s.as_bytes() {
        if c >= 0x80 || c.is_ascii_uppercase() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_any_case_phrase() {
        fn f(s: &str, phrase_lowercase: &str, result_expected: bool) {
            let result = match_any_case_phrase(s, phrase_lowercase);
            assert_eq!(
                result, result_expected,
                "s={s:?} phrase={phrase_lowercase:?}"
            );
        }

        // empty phrase matches only empty string
        f("", "", true);
        f("foo", "", false);
        f("тест", "", false);

        // empty string doesn't match non-empty phrase
        f("", "foo", false);
        f("", "тест", false);

        // full match
        f("foo", "foo", true);
        f("FOo", "foo", true);
        f("Test ТЕСт 123", "test тест 123", true);

        // phrase match
        f("a foo", "foo", true);
        f("foo тест bar", "тест", true);
        f("foo ТЕСТ bar", "тест bar", true);

        // mismatch
        f("foo", "fo", false);
        f("тест", "foo", false);
        f("Тест", "ест", false);
    }
}

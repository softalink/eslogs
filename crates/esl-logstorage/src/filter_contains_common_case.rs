//! Port of EsLogs `lib/logstorage/filter_contains_common_case.go`.
//!
//! `FilterContainsCommonCase` matches words and phrases where every capital
//! letter can be replaced with a small letter, plus all-capital words. It also
//! hosts the shared `get_common_case_phrases` helper used by
//! `filter_equals_common_case`.

use std::collections::HashSet;

use crate::bitmap::Bitmap;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::filter::FieldFilter;
use crate::filter_contains_any::FilterContainsAny;
use crate::filter_generic::{FilterGeneric, RUNE_ERROR, new_filter_generic};
use crate::in_values::InValues;
use crate::pattern_matcher::decode_rune;
use crate::rows::Field;

/// `FilterContainsCommonCase` matches words and phrases where every capital
/// letter can be replaced with a small letter, plus all capital words.
///
/// Example LogsQL: `contains_common_case("Error")` is equivalent to
/// `contains_any("Error", "error", "ERROR")`.
pub(crate) struct FilterContainsCommonCase {
    /// Raw phrase bytes (Go strings are arbitrary bytes; raw `\xNN` escapes
    /// in the query text carry through byte-exact).
    phrases: Vec<Vec<u8>>,

    contains_any: FilterContainsAny,
}

pub(crate) fn new_filter_contains_common_case(
    field_name: &[u8],
    phrases: Vec<Vec<u8>>,
) -> Result<FilterGeneric, String> {
    let common_case_phrases = get_common_case_phrases(&phrases)?;

    let fi = FilterContainsCommonCase {
        phrases,
        contains_any: FilterContainsAny {
            values: InValues::new(common_case_phrases),
        },
    };

    Ok(new_filter_generic(field_name, Box::new(fi)))
}

impl FieldFilter for FilterContainsCommonCase {
    fn to_string(&self) -> String {
        // Lossless render (Go quoteTokenIfNeeded, byte form): invalid UTF-8
        // re-quotes via Go strconv.Quote byte semantics (`\xNN`).
        let phrases = self
            .phrases
            .iter()
            .map(|p| crate::stream_filter::quote_value_bytes_if_needed(p))
            .collect::<Vec<_>>()
            .join(",");
        format!("contains_common_case({phrases})")
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &[u8]) -> bool {
        self.contains_any.match_row_by_field(fields, field_name)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &[u8],
    ) {
        self.contains_any
            .apply_to_block_result_by_field(br, bm, field_name);
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &[u8],
    ) {
        self.contains_any
            .apply_to_block_search_by_field(bs, bm, field_name);
    }
}

/// Port of Go `getCommonCasePhrases`.
///
/// Operates on raw phrase bytes: Go iterates runes over the raw string, where
/// each invalid UTF-8 byte decodes to `RuneError` (never uppercase, so it is
/// carried through the case variants byte-exact, except in the
/// `strings.ToUpper` variant which rewrites it to the replacement char —
/// mirrored by [`to_upper_bytes`]).
pub(crate) fn get_common_case_phrases(phrases: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, String> {
    let mut dst: Vec<Vec<u8>> = Vec::new();
    for phrase in phrases {
        let upper = count_upper_runes(phrase);
        if upper > 10 {
            return Err(format!(
                "too many common_case combinations for the {}; reduce the number of uppercase letters here",
                // Go %q over the raw phrase bytes.
                crate::stream_filter::go_quote_bytes(phrase)
            ));
        }
        append_common_case_phrases(&mut dst, b"", phrase);
    }

    // Deduplicate dst.
    let m: HashSet<Vec<u8>> = dst.into_iter().collect();
    let mut dst: Vec<Vec<u8>> = m.into_iter().collect();
    dst.sort();

    Ok(dst)
}

/// Port of Go `countUpperRunes` (Go-style rune decoding: an invalid byte
/// yields `RuneError`, which is not uppercase).
fn count_upper_runes(s: &[u8]) -> usize {
    let mut upper = 0;
    let mut b = s;
    while !b.is_empty() {
        let (c, size) = decode_rune(b);
        if c.is_uppercase() {
            upper += 1;
        }
        b = &b[size..];
    }
    upper
}

/// Byte form of Go `strings.ToUpper`: maps each decoded rune through the
/// scalar uppercase mapping (bit-identical to the previous `&str` path for
/// valid UTF-8); each invalid UTF-8 byte is rewritten to the replacement char
/// (Go `strings.Map` decodes it as `utf8.RuneError` and re-encodes it) —
/// display-lossy exactly like Go.
fn to_upper_bytes(s: &[u8]) -> Vec<u8> {
    if let Ok(v) = std::str::from_utf8(s) {
        return v.to_uppercase().into_bytes();
    }
    let mut out = Vec::with_capacity(s.len());
    let mut b = s;
    let mut buf = [0u8; 4];
    while !b.is_empty() {
        let (r, size) = decode_rune(b);
        if r == RUNE_ERROR && size == 1 {
            // Invalid UTF-8 byte: Go writes utf8.RuneError.
            out.extend_from_slice(RUNE_ERROR.encode_utf8(&mut buf).as_bytes());
        } else {
            for c in r.to_uppercase() {
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
        b = &b[size..];
    }
    out
}

/// Port of Go `appendCommonCasePhrases`.
fn append_common_case_phrases(dst: &mut Vec<Vec<u8>>, prefix: &[u8], phrase: &[u8]) {
    let mut base = Vec::with_capacity(prefix.len() + phrase.len());
    base.extend_from_slice(prefix);
    base.extend_from_slice(phrase);
    dst.push(base.clone());
    dst.push(to_upper_bytes(&base));

    let mut off = 0;
    while off < phrase.len() {
        let (c, size) = decode_rune(&phrase[off..]);
        // An invalid byte decodes to RuneError, which is not uppercase, so it
        // is skipped exactly like in Go (`unicode.IsUpper` is false for it).
        if !c.is_uppercase() {
            off += size;
            continue;
        }
        // `c` is uppercase, hence a validly decoded rune: `size` is its exact
        // UTF-8 width (Go `utf8.RuneLen(c)`; the -1 case is unreachable).
        let c_lower: String = c.to_lowercase().collect();

        let mut prefix_local = Vec::with_capacity(prefix.len() + off + size);
        prefix_local.extend_from_slice(prefix);
        prefix_local.extend_from_slice(&phrase[..off]);
        let phrase_tail = &phrase[off + size..];

        let mut with_lower = prefix_local.clone();
        with_lower.extend_from_slice(c_lower.as_bytes());
        append_common_case_phrases(dst, &with_lower, phrase_tail);

        let mut with_upper = prefix_local;
        let mut buf = [0u8; 4];
        with_upper.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
        append_common_case_phrases(dst, &with_upper, phrase_tail);

        off += size;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_common_case_phrases_success() {
        fn f(phrases: &[&str], result_expected: &[&str]) {
            let phrases: Vec<Vec<u8>> = phrases.iter().map(|s| s.as_bytes().to_vec()).collect();
            let result = get_common_case_phrases(&phrases).expect("unexpected error");
            let expected: Vec<Vec<u8>> = result_expected
                .iter()
                .map(|s| s.as_bytes().to_vec())
                .collect();
            assert_eq!(result, expected, "phrases={phrases:?}");
        }

        f(&[], &[]);
        f(&[""], &[""]);
        f(&["foo"], &["FOO", "foo"]);
        f(&["Foo"], &["FOO", "Foo", "foo"]);
        f(&["foo", "Foo"], &["FOO", "Foo", "foo"]);
        f(
            &["FOO"],
            &["FOO", "FOo", "FoO", "Foo", "fOO", "fOo", "foO", "foo"],
        );

        f(
            &["FooBar"],
            &["FOOBAR", "FooBar", "Foobar", "fooBar", "foobar"],
        );
        f(&["fooBar"], &["FOOBAR", "fooBar", "foobar"]);
    }

    #[test]
    fn test_get_common_case_phrases_raw_bytes() {
        // A raw invalid-UTF-8 byte decodes to RuneError (not uppercase), so
        // it is carried byte-exact through the case variants, except in the
        // strings.ToUpper variant, which rewrites it to U+FFFD (Go-exact).
        let phrases = vec![b"F\xffo".to_vec()];
        let result = get_common_case_phrases(&phrases).expect("unexpected error");
        let expected: Vec<Vec<u8>> = vec![
            b"F\xef\xbf\xbdO".to_vec(), // ToUpper variant: \xff -> U+FFFD
            b"F\xffo".to_vec(),
            b"f\xffo".to_vec(),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_get_common_case_phrases_failure() {
        fn f(phrases: &[&str]) {
            let phrases: Vec<Vec<u8>> = phrases.iter().map(|s| s.as_bytes().to_vec()).collect();
            let result = get_common_case_phrases(&phrases);
            assert!(result.is_err(), "expecting non-nil error");
        }

        // More than 10 uppercase chars.
        f(&["FOOBARBAZAB"]);
        f(&["FoOOBbARrBAZzABsdf"]);
    }
}

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
use crate::filter_generic::{FilterGeneric, new_filter_generic};
use crate::in_values::InValues;
use crate::rows::Field;
use crate::stream_filter::quote_token_if_needed;

/// `FilterContainsCommonCase` matches words and phrases where every capital
/// letter can be replaced with a small letter, plus all capital words.
///
/// Example LogsQL: `contains_common_case("Error")` is equivalent to
/// `contains_any("Error", "error", "ERROR")`.
pub(crate) struct FilterContainsCommonCase {
    phrases: Vec<String>,

    contains_any: FilterContainsAny,
}

pub(crate) fn new_filter_contains_common_case(
    field_name: &str,
    phrases: Vec<String>,
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
        let phrases = self
            .phrases
            .iter()
            .map(|p| quote_token_if_needed(p))
            .collect::<Vec<_>>()
            .join(",");
        format!("contains_common_case({phrases})")
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &str) -> bool {
        self.contains_any.match_row_by_field(fields, field_name)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        self.contains_any
            .apply_to_block_result_by_field(br, bm, field_name);
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &str,
    ) {
        self.contains_any
            .apply_to_block_search_by_field(bs, bm, field_name);
    }
}

/// Port of Go `getCommonCasePhrases`.
pub(crate) fn get_common_case_phrases(phrases: &[String]) -> Result<Vec<String>, String> {
    let mut dst: Vec<String> = Vec::new();
    for phrase in phrases {
        let upper = count_upper_runes(phrase);
        if upper > 10 {
            return Err(format!(
                "too many common_case combinations for the {phrase:?}; reduce the number of uppercase letters here"
            ));
        }
        append_common_case_phrases(&mut dst, "", phrase);
    }

    // Deduplicate dst.
    let m: HashSet<String> = dst.into_iter().collect();
    let mut dst: Vec<String> = m.into_iter().collect();
    dst.sort();

    Ok(dst)
}

/// Port of Go `countUpperRunes`.
fn count_upper_runes(s: &str) -> usize {
    s.chars().filter(|c| c.is_uppercase()).count()
}

/// Port of Go `appendCommonCasePhrases`.
fn append_common_case_phrases(dst: &mut Vec<String>, prefix: &str, phrase: &str) {
    let base = format!("{prefix}{phrase}");
    dst.push(base.clone());
    dst.push(base.to_uppercase());

    for (off, c) in phrase.char_indices() {
        if !c.is_uppercase() {
            continue;
        }
        let char_len = c.len_utf8();

        let c_lower: String = c.to_lowercase().collect();

        let prefix_local = format!("{prefix}{}", &phrase[..off]);
        let phrase_tail = &phrase[off + char_len..];

        append_common_case_phrases(dst, &format!("{prefix_local}{c_lower}"), phrase_tail);
        append_common_case_phrases(dst, &format!("{prefix_local}{c}"), phrase_tail);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_common_case_phrases_success() {
        fn f(phrases: &[&str], result_expected: &[&str]) {
            let phrases: Vec<String> = phrases.iter().map(|s| s.to_string()).collect();
            let result = get_common_case_phrases(&phrases).expect("unexpected error");
            let expected: Vec<String> = result_expected.iter().map(|s| s.to_string()).collect();
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
    fn test_get_common_case_phrases_failure() {
        fn f(phrases: &[&str]) {
            let phrases: Vec<String> = phrases.iter().map(|s| s.to_string()).collect();
            let result = get_common_case_phrases(&phrases);
            assert!(result.is_err(), "expecting non-nil error");
        }

        // More than 10 uppercase chars.
        f(&["FOOBARBAZAB"]);
        f(&["FoOOBbARrBAZzABsdf"]);
    }
}

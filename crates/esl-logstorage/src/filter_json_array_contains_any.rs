//! Port of EsLogs `lib/logstorage/filter_json_array_contains_any.go`.
//!
//! `FilterJSONArrayContainsAny` matches if the JSON array in the given field
//! contains any of the given values.

use std::cell::RefCell;
use std::sync::OnceLock;

use crate::bitmap::Bitmap;
use crate::block_header::ColumnHeader;
use crate::block_result::BlockResult;
use crate::block_search::BlockSearch;
use crate::bloomfilter::append_tokens_hashes;
use crate::filter::FieldFilter;
use crate::filter_generic::{FilterGeneric, clone_column_header, new_filter_generic};
use crate::filter_phrase::{
    match_bloom_filter_all_tokens, match_encoded_values_dict, visit_values,
};
use crate::json_parser::fastjson;
use crate::rows::{Field, get_field_value_by_name};
use crate::stream_filter::quote_value_bytes_if_needed;
use crate::tokenizer::tokenize_bytes;
use crate::values_encoder::ValueType;

thread_local! {
    // PORT NOTE: Go pools `fastjson.Parser` via the package-level `jspp`; the
    // port keeps a thread-local pool so the parse buffers are reused across
    // calls, matching Go's pooling.
    static JSON_PARSER_POOL: RefCell<Vec<fastjson::Parser>> = const { RefCell::new(Vec::new()) };
}

fn get_parser() -> fastjson::Parser {
    JSON_PARSER_POOL.with(|p| p.borrow_mut().pop().unwrap_or_default())
}

fn put_parser(p: fastjson::Parser) {
    JSON_PARSER_POOL.with(|pool| pool.borrow_mut().push(p));
}

/// `FilterJSONArrayContainsAny` matches if the JSON array in the given field
/// contains any of the given values.
///
/// Example LogsQL: `tags:json_array_contains_any("prod","dev")`.
/// Cached per-value `(tokenss, tokens_hashess)` (Go `tokenss` / `tokensHashess`).
type CachedTokens = (Vec<Vec<Vec<u8>>>, Vec<Vec<u64>>);

pub(crate) struct FilterJSONArrayContainsAny {
    /// Raw value bytes (Go strings are arbitrary bytes; raw `\xNN` escapes
    /// in the query text carry through byte-exact).
    values: Vec<Vec<u8>>,

    /// Cached `(tokenss, tokens_hashess)` (Go `tokensOnce`).
    tokens: OnceLock<CachedTokens>,
}

pub(crate) fn new_filter_json_array_contains_any(
    field_name: &[u8],
    values: Vec<Vec<u8>>,
) -> FilterGeneric {
    new_filter_generic(
        field_name,
        Box::new(FilterJSONArrayContainsAny {
            values,
            tokens: OnceLock::new(),
        }),
    )
}

impl FilterJSONArrayContainsAny {
    fn get_tokens(&self) -> &CachedTokens {
        self.tokens.get_or_init(|| {
            // Byte tokenizer: matches the ingest-side hash_tokenizer, so
            // bloom lookups agree for raw-byte values too.
            let mut tokenss: Vec<Vec<Vec<u8>>> = Vec::with_capacity(self.values.len());
            for v in &self.values {
                let mut toks: Vec<&[u8]> = Vec::new();
                tokenize_bytes(&mut toks, std::slice::from_ref(v));
                tokenss.push(toks.into_iter().map(|s| s.to_vec()).collect());
            }

            let mut tokens_hashess: Vec<Vec<u64>> = Vec::with_capacity(tokenss.len());
            for tokens in &tokenss {
                let mut hashes = Vec::new();
                append_tokens_hashes(&mut hashes, tokens);
                tokens_hashess.push(hashes);
            }

            (tokenss, tokens_hashess)
        })
    }
}

impl FieldFilter for FilterJSONArrayContainsAny {
    fn to_string(&self) -> String {
        // Lossless render (Go quoteTokenIfNeeded, byte form): invalid UTF-8
        // re-quotes via Go strconv.Quote byte semantics (`\xNN`).
        let args = self
            .values
            .iter()
            .map(|v| quote_value_bytes_if_needed(v))
            .collect::<Vec<_>>()
            .join(",");
        format!("json_array_contains_any({args})")
    }

    fn match_row_by_field(&self, fields: &[Field], field_name: &[u8]) -> bool {
        let tokenss = &self.get_tokens().0;
        let v = get_field_value_by_name(fields, field_name);
        match_json_array_contains_any(v, &self.values, tokenss)
    }

    fn apply_to_block_result_by_field(
        &self,
        br: &mut BlockResult,
        bm: &mut Bitmap,
        field_name: &[u8],
    ) {
        let tokenss = &self.get_tokens().0;

        let r = br.get_column_by_name(field_name);
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0).to_vec();
            if !match_json_array_contains_any(&v, &self.values, tokenss) {
                bm.reset_bits();
            }
            return;
        }
        if br.column_is_time(r) {
            bm.reset_bits();
            return;
        }

        match br.column_value_type(r) {
            // PORT NOTE: Go's `valueTypeDict` case builds a per-dict-entry table
            // from `c.dictValues`. `BlockResult` does not expose `dictValues`, so
            // the port routes the dict case through the already-decoded per-row
            // values. Identical result.
            ValueType::STRING | ValueType::DICT => {
                let values = br.column_get_values(r);
                bm.for_each_set_bit(|idx| {
                    match_json_array_contains_any(&values[idx], &self.values, tokenss)
                });
            }
            _ => bm.reset_bits(),
        }
    }

    fn apply_to_block_search_by_field(
        &self,
        bs: &mut BlockSearch<'_>,
        bm: &mut Bitmap,
        field_name: &[u8],
    ) {
        let v = bs.get_const_column_value(field_name);
        if !v.is_empty() {
            let tokenss = &self.get_tokens().0;
            if !match_json_array_contains_any(&v, &self.values, tokenss) {
                bm.reset_bits();
            }
            return;
        }

        // Verify whether filter matches other columns.
        let ch = match bs.get_column_header(field_name) {
            Some(ch) => clone_column_header(ch),
            None => {
                // Fast path - there are no matching columns.
                bm.reset_bits();
                return;
            }
        };

        match ch.value_type {
            ValueType::STRING => {
                if !match_any_tokens_hashess(bs, &ch, &self.get_tokens().1) {
                    bm.reset_bits();
                    return;
                }
                let tokenss = &self.get_tokens().0;
                visit_values(bs, &ch, bm, |v| {
                    match_json_array_contains_any(v, &self.values, tokenss)
                });
            }
            ValueType::DICT => {
                let tokenss = &self.get_tokens().0;
                let mut bb = Vec::with_capacity(ch.values_dict.values.len());
                for v in &ch.values_dict.values {
                    bb.push(u8::from(match_json_array_contains_any(
                        v,
                        &self.values,
                        tokenss,
                    )));
                }
                match_encoded_values_dict(bs, &ch, bm, &bb);
            }
            _ => bm.reset_bits(),
        }
    }
}

/// Port of Go `matchAnyTokensHashess`.
fn match_any_tokens_hashess(
    bs: &mut BlockSearch<'_>,
    ch: &ColumnHeader,
    tokens_hashess: &[Vec<u64>],
) -> bool {
    for tokens_hashes in tokens_hashess {
        if match_bloom_filter_all_tokens(bs, ch, tokens_hashes) {
            return true;
        }
    }
    false
}

/// Port of Go `matchJSONArrayContainsAny`.
///
/// The haystack `s` is raw value bytes (Go strings are arbitrary bytes).
fn match_json_array_contains_any(s: &[u8], values: &[Vec<u8>], tokenss: &[Vec<Vec<u8>>]) -> bool {
    if s.is_empty() {
        // Fast path for empty strings.
        return false;
    }

    let s = trim_json_whitespace(s);

    if !s.starts_with(b"[") {
        // Fast path - s is not a JSON array.
        return false;
    }

    if !match_any_tokenss(s, tokenss) {
        // Fast path - s doesn't contain any of the given values.
        return false;
    }

    // Slow path - parse JSON array at s and search for matching values.
    let mut p = get_parser();
    let ok = json_array_contains_any_slow(&mut p, s, values);
    put_parser(p);
    ok
}

fn json_array_contains_any_slow(p: &mut fastjson::Parser, s: &[u8], values: &[Vec<u8>]) -> bool {
    let v = match p.parse(s) {
        Ok(v) => v,
        Err(_) => return false,
    };
    if p.doc.value_type(v) != fastjson::JsonType::Array {
        return false;
    }

    let n = p.doc.array_len(v);
    for i in 0..n {
        let e = p.doc.array_element(v, i);
        // We only support checking against the string representation of values
        // in the array.
        match p.doc.value_type(e) {
            fastjson::JsonType::String => {
                let span = p.doc.string_span(e);
                let sv = p.doc.str_bytes(span);
                if values.iter().any(|x| x.as_slice() == sv) {
                    return true;
                }
            }
            fastjson::JsonType::Number
            | fastjson::JsonType::True
            | fastjson::JsonType::False
            | fastjson::JsonType::Null => {
                let mut bb = Vec::new();
                p.doc.marshal_value_to(e, &mut bb);
                if values.contains(&bb) {
                    return true;
                }
            }
            _ => {}
        }
    }

    false
}

/// Port of Go `matchAnyTokenss`.
fn match_any_tokenss(s: &[u8], tokenss: &[Vec<Vec<u8>>]) -> bool {
    tokenss.iter().any(|tokens| match_all_substrings(s, tokens))
}

/// Port of Go `matchAllSubstrings`.
fn match_all_substrings(s: &[u8], tokens: &[Vec<u8>]) -> bool {
    tokens
        .iter()
        .all(|token| crate::filter_generic::index_bytes(s, token).is_some())
}

/// Port of Go `trimJSONWhitespace`.
fn trim_json_whitespace(s: &[u8]) -> &[u8] {
    let is_ws = |c: u8| matches!(c, b' ' | b'\t' | b'\n' | b'\r');
    let start = s.iter().position(|&c| !is_ws(c)).unwrap_or(s.len());
    let end = s.iter().rposition(|&c| !is_ws(c)).map_or(start, |i| i + 1);
    &s[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_json_array_contains_any() {
        fn f(s: &str, values: &[&str], result_expected: bool) {
            let values: Vec<Vec<u8>> = values.iter().map(|v| v.as_bytes().to_vec()).collect();
            let mut tokenss: Vec<Vec<Vec<u8>>> = Vec::with_capacity(values.len());
            for v in &values {
                let mut toks: Vec<&[u8]> = Vec::new();
                tokenize_bytes(&mut toks, std::slice::from_ref(v));
                tokenss.push(toks.into_iter().map(|s| s.to_vec()).collect());
            }

            let result = match_json_array_contains_any(s.as_bytes(), &values, &tokenss);
            assert_eq!(result, result_expected, "s={s:?} values={values:?}");
        }

        // Empty values
        f("", &[], false);
        f("foo", &[], false);
        f("[]", &[], false);
        f(r#"["foo"]"#, &[], false);

        // Not JSON array
        f("", &["foo"], false);
        f("foo", &["foo"], false);
        f("{}", &["foo"], false);

        // JSON array doesn't contain the needed values
        f("[]", &["foo"], false);
        f(r#"["bar"]"#, &["foo"], false);
        f(r#"["bar","baz"]"#, &["foo"], false);
        f(r#"["bar","baz"]"#, &[""], false);
        f("[1,2]", &["3"], false);

        // JSON array contains the needed values
        f(r#"["foo"]"#, &["foo", "bar"], true);
        f(r#"["bar","foo"]"#, &["foo"], true);
        f(r#"[  "foo"  ,  "bar"  ]"#, &["abc", "foo", "bar"], true);
        f(r#"["foo","bar",""]"#, &[""], true);
        f(r#"["a","foo","b"]"#, &["x", "foo", "y"], true);

        // Mixed types
        f("[123]", &["123"], true);
        f("[true]", &["true"], true);
        f(r#"["123"]"#, &["123"], true);
        f("[null]", &["null"], true);

        // Leading and trailing whitespace (valid JSON)
        f(" \t\r\n[\"foo\"]  ", &["foo"], true);

        // Tricky cases
        f(r#"["foo bar"]"#, &["foo"], false); // partial match
        f(r#"["foobar"]"#, &["foo"], false); // partial match
        f(r#"["foo"]"#, &["fo"], false); // partial match

        // Escaped strings in JSON
        f(r#"["a\"b"]"#, &[r#"a"b"#], true); // \" escape => a"b
        f(r#"["a\nb"]"#, &["a\nb"], true); // \n escape
        f(r#"["a\/b"]"#, &["a/b"], true); // \/ escape is valid in JSON

        // The b => 'b' isn't found because of performance reasons (the
        // fast-path substring check runs against the raw, still-escaped JSON).
        f(r#"["a\u0062"]"#, &["ab"], false);

        // Nested structures (ignored by current implementation)
        f(r#"[{"a":"b"}]"#, &[r#"{"a":"b"}"#], false); // nested object ignored
        f(r#"[["a"]]"#, &[r#"["a"]"#], false); // nested array ignored

        // Mixed with simple value
        f(r#"[["a"], "b"]"#, &["b"], true);
    }
}

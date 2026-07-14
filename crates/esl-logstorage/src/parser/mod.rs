//! Port of EsLogs `lib/logstorage/parser.go` — the LogsQL query parser.
//!
//! This is the central integration piece for the LogsQL query language: it
//! owns the [`Query`] type, [`parse_query`]/[`ParseQuery`], and the grammar
//! that builds the ported filters (`filter_*`), pipes (`pipe_*`) and stats
//! functions (`stats_*`).
//!
//! Submodules:
//! * [`lexer_ext`] — extension methods completing the lexer hosted in
//!   `stream_filter.rs`.
//! * [`helpers`] — shared numeric / time / field parse helpers.
//! * [`parse_filter`] — the filter-expression grammar (all `parseFilterX`).
//! * [`parse_pipe`] — the `|` pipe-chain grammar (all `parsePipeX`).
//! * [`parse_stats`] — `stats` / `running_stats` grammar (all `parseStatsX`).
//! * [`query`] — [`Query`], `queryOptions`, `Filter`, and `ParseQuery*`.
//!
//! # PORT NOTES — optimize() status
//! Go's `parser.go` post-parse `optimize()` is ported for the filter passes
//! (`flattenFiltersAnd/Or`, `removeStarFilters`, `mergeFiltersStream`) and
//! the pipe passes (`optimizeOffsetLimitPipes`, `optimizeUniqLimitPipes`,
//! marking a leading `pipeFieldNames` as first pipe), expressed through
//! purpose-built `Filter`/`Pipe` trait hooks instead of Go's `copyFilter` +
//! type switches (see `query::optimize_no_subqueries`), as are
//! `optimizeFilterPipes` and the leading-`filter`-pipe merge (via the
//! rendered-pipe-string route). `updateFilterWithTimeOffset` remains deferred
//! with a PORT NOTE at its stub. The full lexer + parse grammar — the LogsQL
//! parity spec — is ported in full.

#[cfg(test)]
mod tests;

pub mod helpers;
pub mod lexer_ext;
pub mod parse_filter;
pub mod parse_pipe;
pub mod parse_stats;
pub mod query;
pub mod query_stats;

pub use query::{
    Filter, ParseFilter, ParseFilterAtTimestamp, ParseQuery, ParseQueryAtTimestamp, Query,
    can_apply_last_n_results_optimization,
};

/// Re-export of Go `strconv.Quote` (lives in `stream_filter.rs`).
pub(crate) use crate::stream_filter::go_quote;

// ---------------------------------------------------------------------------
// Token quoting (Go `quoteTokenIfNeeded` / `needQuoteToken` / ...).
//
// This module hosts the complete port of Go `needQuoteToken` (including the
// `isPipeName`/`isStatsFuncName` checks); `stream_filter.rs` hosts an earlier
// `quote_token_if_needed` wrapper whose `need_quote_token` delegates here, so
// the `filter_*`/`pipe_*` Display impls quote exactly like Go.
// ---------------------------------------------------------------------------

/// Port of Go `quoteTokenIfNeeded`.
pub(crate) fn quote_token_if_needed(s: &str) -> String {
    if !need_quote_token(s) {
        return s.to_string();
    }
    go_quote(s)
}

/// Byte form of [`quote_token_if_needed`] for raw-byte name payloads (parsed
/// field names are raw bytes end-to-end).
///
/// Valid UTF-8 renders bit-identically to the `&str` form; invalid UTF-8
/// always needs quoting in Go too (`needQuoteToken` sees the non-token
/// `RuneError` rune), and quotes with Go `strconv.Quote` byte semantics
/// (`\xNN` per invalid byte) — exactly what Go produces.
pub(crate) fn quote_token_bytes_if_needed(v: &[u8]) -> String {
    match std::str::from_utf8(v) {
        Ok(s) => quote_token_if_needed(s),
        Err(_) => crate::stream_filter::go_quote_bytes(v),
    }
}

/// Port of Go `quoteStringTokenIfNeeded`.
pub(crate) fn quote_string_token_if_needed(s: &str) -> String {
    if !need_quote_string_token(s) {
        return s.to_string();
    }
    go_quote(s)
}

/// Byte form of [`quote_string_token_if_needed`] for raw-byte value payloads.
///
/// Valid UTF-8 renders bit-identically to the `&str` form; invalid UTF-8
/// always needs quoting in Go too (`needQuoteToken` sees the non-token
/// `RuneError` rune), and quotes with Go `strconv.Quote` byte semantics
/// (`\xNN` per invalid byte) — exactly what Go produces.
pub(crate) fn quote_string_value_bytes_if_needed(v: &[u8]) -> String {
    match std::str::from_utf8(v) {
        Ok(s) => quote_string_token_if_needed(s),
        Err(_) => crate::stream_filter::go_quote_bytes(v),
    }
}

/// Port of Go `quoteFieldFilterIfNeeded`, over raw-byte field-filter payloads
/// (parsed field names/filters are raw bytes end-to-end). Valid UTF-8 renders
/// exactly like Go's string form; an invalid-UTF-8 wildcard prefix always
/// needs quoting in Go too (`needQuoteToken` sees the non-token `RuneError`
/// rune) and quotes with Go `strconv.Quote` byte semantics (`\xNN`).
pub(crate) fn quote_field_filter_if_needed(v: &[u8]) -> String {
    if !crate::prefix_filter::is_wildcard_filter(v) {
        return quote_token_bytes_if_needed(v);
    }
    let wildcard = &v[..v.len() - 1];
    match std::str::from_utf8(wildcard) {
        Ok(s) => {
            if s.is_empty() || !need_quote_token(s) {
                // `v` = valid-UTF-8 prefix + `'*'` — valid UTF-8 as a whole.
                return String::from_utf8(v.to_vec()).unwrap();
            }
            go_quote(s) + "*"
        }
        Err(_) => crate::stream_filter::go_quote_bytes(wildcard) + "*",
    }
}

/// Port of Go `needQuoteStringToken`.
pub(crate) fn need_quote_string_token(s: &str) -> bool {
    is_number_prefix(s) || need_quote_token(s)
}

/// Port of Go `isNumberPrefix`.
pub(crate) fn is_number_prefix(s: &str) -> bool {
    let mut s = s;
    if s.is_empty() {
        return false;
    }
    let b = s.as_bytes();
    if b[0] == b'-' || b[0] == b'+' {
        s = &s[1..];
        if s.is_empty() {
            return false;
        }
    }
    if s.len() >= 3 && s.eq_ignore_ascii_case("inf") {
        return true;
    }
    let b = s.as_bytes();
    if b[0] == b'.' {
        s = &s[1..];
        if s.is_empty() {
            return false;
        }
    }
    let c = s.as_bytes()[0];
    c.is_ascii_digit()
}

/// Port of Go `needQuoteToken`.
pub(crate) fn need_quote_token(s: &str) -> bool {
    if s == "." {
        return true;
    }
    let s_lower = s.to_lowercase();
    if RESERVED_KEYWORDS.contains(&s_lower.as_str()) {
        return true;
    }
    if parse_pipe::is_pipe_name(&s_lower) || parse_stats::is_stats_func_name(&s_lower) {
        return true;
    }
    s.chars().any(|r| !crate::tokenizer::is_token_rune(r))
}

/// Port of Go `reservedKeywords`.
pub(crate) const RESERVED_KEYWORDS: &[&str] = &[
    "",
    "and",
    "or",
    "not",
    "!",
    "(",
    ")",
    "{",
    "}",
    "=",
    "!=",
    "=~",
    "!~",
    ",",
    "|",
    ":",
    "*",
    "[",
    "]",
    "now",
    "offset",
    "-",
    "contains_all",
    "contains_any",
    "json_array_contains_any",
    "contains_common_case",
    "eq_field",
    "equals_common_case",
    "exact",
    "i",
    "in",
    "ipv4_range",
    "ipv6_range",
    "le_field",
    "len_range",
    "lt_field",
    "pattern_match",
    "pattern_match_full",
    "pattern_match_prefix",
    "pattern_match_suffix",
    "range",
    "re",
    "seq",
    "string_range",
    "value_type",
    "options",
    "if",
    "by",
    "as",
];

/// Maximum string range value (Go package-level `maxStringRangeValue` =
/// `string([]byte{255,255,255,255})`): four raw `0xFF` bytes, byte-wise
/// greater than any stored value, serving as a `+∞` sentinel for `foo:>value`
/// string-range upper bounds. Field values and the string-range bounds are raw
/// bytes, so the sentinel is Go's exact bytes (it never appears in the
/// `String()` representation).
pub(crate) const MAX_STRING_RANGE_VALUE: &[u8] = b"\xff\xff\xff\xff";

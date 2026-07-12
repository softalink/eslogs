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
//! the `optimizeOffsetLimitPipes` pipe pass, expressed through purpose-built
//! `Filter`/`Pipe` trait hooks instead of Go's `copyFilter` + type switches
//! (see `query::optimize_no_subqueries`), as are `optimizeFilterPipes` and
//! the leading-`filter`-pipe merge (via the rendered-pipe-string route). The
//! rewrites needing further `Pipe`-trait hooks (`optimizeUniqLimitPipes`,
//! marking a leading `pipeFieldNames` as first pipe) and
//! `updateFilterWithTimeOffset` remain deferred with PORT NOTEs at their
//! stubs. The full lexer + parse grammar — the LogsQL parity spec — is
//! ported in full.

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
    Filter, ParseFilter, ParseQuery, ParseQueryAtTimestamp, Query,
    can_apply_last_n_results_optimization,
};

/// Re-export of Go `strconv.Quote` (lives in `stream_filter.rs`).
pub(crate) use crate::stream_filter::go_quote;

// ---------------------------------------------------------------------------
// Token quoting (Go `quoteTokenIfNeeded` / `needQuoteToken` / ...).
//
// PORT NOTE: `stream_filter.rs` hosts an earlier `quote_token_if_needed` whose
// `need_quote_token` deliberately omits the `isPipeName`/`isStatsFuncName`
// checks (pipes/stats were unported then). The parser needs the *complete*
// version for faithful `String()` round-trips, so it is (re)defined here with
// those checks. `filter_*`/`pipe_*` Display impls still use the stream_filter
// copy; unifying them is a later cleanup.
// ---------------------------------------------------------------------------

/// Port of Go `quoteTokenIfNeeded`.
pub(crate) fn quote_token_if_needed(s: &str) -> String {
    if !need_quote_token(s) {
        return s.to_string();
    }
    go_quote(s)
}

/// Port of Go `quoteStringTokenIfNeeded`.
pub(crate) fn quote_string_token_if_needed(s: &str) -> String {
    if !need_quote_string_token(s) {
        return s.to_string();
    }
    go_quote(s)
}

/// Port of Go `quoteFieldFilterIfNeeded`.
/// Port of Go `quoteFieldFilterIfNeeded` (used by field-filter Display in the
/// upstream code; kept for parity though pipe/filter Display currently routes
/// through `stream_filter::quote_token_if_needed`).
#[allow(dead_code)]
pub(crate) fn quote_field_filter_if_needed(s: &str) -> String {
    if !crate::prefix_filter::is_wildcard_filter(s) {
        return quote_token_if_needed(s);
    }
    let wildcard = &s[..s.len() - 1];
    if wildcard.is_empty() || !need_quote_token(wildcard) {
        return s.to_string();
    }
    go_quote(wildcard) + "*"
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

/// Maximum string range value (Go package-level `maxStringRangeValue`,
/// deferred from `filter_string_range.rs` to this port).
///
/// PORT NOTE: Go uses `string([]byte{255,255,255,255})` — four raw `0xFF`
/// bytes, which is byte-wise greater than any valid UTF-8 string and serves as
/// a `+∞` sentinel for `foo:>value` string-range upper bounds. A Rust `&str`
/// cannot hold `0xFF` bytes, so the max codepoint (`U+10FFFF`, encoded
/// `F4 8F BF BF`) is used instead. This only affects `filter_string_range`
/// *matching* (execution), not `String()` round-trips (the sentinel never
/// appears in the string representation).
pub(crate) const MAX_STRING_RANGE_VALUE: &str = "\u{10FFFF}\u{10FFFF}\u{10FFFF}\u{10FFFF}";

//! Port of the LogsQL filter-expression grammar from `parser.go`
//! (`parseFilter` / `parseFilterOr` / `parseFilterAnd` / `parseFilterGeneric`
//! and all `parseFilterX` leaf parsers).
//!
//! Each parser builds a `Box<dyn Filter>` from the ported `filter_*`
//! constructors. See the module PORT NOTES in `parser/mod.rs` for the
//! optimize()/downcast deferrals.
//!
//! Subqueries are supported: `in(<subquery> | fields x)` /
//! `contains_any(<subquery>)` / `contains_all(<subquery>)` (via
//! [`parse_in_values`] → [`parse_in_query`]) and `_stream_id:in(<subquery>)`
//! (via [`parse_filter_stream_id_in`]). The parsed subquery is stored as
//! rendered query text (the established subquery pattern — see pipe_join.rs)
//! and resolved before execution by `storage_search::init_subqueries`.
//!
//! PORT NOTE: Go's `q.optimize()` visits subqueries after the top-level parse;
//! since the Rust filters store subqueries as rendered text, [`parse_in_query`]
//! applies the ported `optimize()` subset to the subquery before rendering it
//! (same round-trip result for the ported optimize subset).

use esl_common::regexutil::Regex;

use crate::filter::Filter;
use crate::log_rows::get_canonical_column_name_bytes;
use crate::parser::helpers::*;
use crate::parser::lexer_ext::LexerExt;
use crate::parser::parse_pipe::is_pipe_name;
use crate::parser::parse_stats::is_stats_func_name;
use crate::parser::{MAX_STRING_RANGE_VALUE, go_quote};
use crate::pattern_matcher::{PatternMatcherOption, new_pattern_matcher};
use crate::stream_filter::{Lexer, parse_args_in_parens_possible_wildcard, parse_stream_filter};
use crate::stream_id::StreamID;
use crate::values_encoder::try_parse_ipv4;

const INF: f64 = f64::INFINITY;
const NSECS_PER_DAY: i64 = 24 * 3600 * 1_000_000_000;

// filter_* constructors (boxed to `dyn Filter`).
use crate::filter_and::new_filter_and;
use crate::filter_any_case_phrase::new_filter_any_case_phrase;
use crate::filter_any_case_prefix::new_filter_any_case_prefix;
use crate::filter_contains_all::new_filter_contains_all_values;
use crate::filter_contains_any::new_filter_contains_any_values;
use crate::filter_contains_common_case::new_filter_contains_common_case;
use crate::filter_day_range::new_filter_day_range;
use crate::filter_eq_field::new_filter_eq_field;
use crate::filter_equals_common_case::new_filter_equals_common_case;
use crate::filter_exact::new_filter_exact;
use crate::filter_exact_prefix::new_filter_exact_prefix;
use crate::filter_in::new_filter_in_values;
use crate::filter_ipv4_range::new_filter_ipv4_range;
use crate::filter_ipv6_range::new_filter_ipv6_range;
use crate::filter_json_array_contains_any::new_filter_json_array_contains_any;
use crate::filter_le_field::new_filter_le_field;
use crate::filter_len_range::new_filter_len_range;
use crate::filter_noop::new_filter_noop;
use crate::filter_not::new_filter_not;
use crate::filter_or::new_filter_or;
use crate::filter_pattern_match::new_filter_pattern_match;
use crate::filter_phrase::new_filter_phrase;
use crate::filter_prefix::new_filter_prefix;
use crate::filter_range::new_filter_range;
use crate::filter_regexp::new_filter_regexp;
use crate::filter_sequence::new_filter_sequence;

type BoxFilter = Box<dyn Filter>;

/// Port of Go `parseFilter`.
pub(crate) fn parse_filter(
    lex: &mut Lexer,
    allow_pipe_keywords: bool,
) -> Result<BoxFilter, String> {
    if lex.is_query_part_trailer() {
        return Err("missing query".to_string());
    }
    if !allow_pipe_keywords {
        let first_token = lex.raw_token().to_lowercase();
        if first_token == "by" || is_pipe_name(&first_token) || is_stats_func_name(&first_token) {
            return Err(format!(
                "query filter cannot start with pipe keyword {}; see https://docs.victoriametrics.com/victorialogs/logsql/#query-syntax; please put the first word of the filter into quotes",
                go_quote(&first_token)
            ));
        }
    }
    parse_filter_or(lex, b"")
}

fn parse_filter_or(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    let mut filters: Vec<BoxFilter> = Vec::new();
    loop {
        let f = parse_filter_and(lex, field_name)?;
        filters.push(f);
        if lex.is_query_part_trailer() {
            if filters.len() == 1 {
                return Ok(filters.pop().unwrap());
            }
            return Ok(Box::new(new_filter_or(filters)));
        }
        if lex.is_keyword(&["or"]) {
            lex.next_token();
        }
    }
}

fn parse_filter_and(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    let mut filters: Vec<BoxFilter> = Vec::new();
    loop {
        let f = parse_filter_generic(lex, field_name)?;
        filters.push(f);
        if lex.is_keyword(&["or"]) || lex.is_query_part_trailer() {
            if filters.len() == 1 {
                return Ok(filters.pop().unwrap());
            }
            return Ok(Box::new(new_filter_and(filters)));
        }
        if lex.is_keyword(&["and"]) {
            lex.next_token();
        }
    }
}

fn parse_filter_generic(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    if lex.is_keyword(&[""]) {
        return Err(format!(
            "unexpected end of query after {}; expecting a filter",
            go_quote(lex.prev_raw_token())
        ));
    }

    if lex.is_keyword(&["("]) {
        lex.check_prev_adjacent_token(&["|", ":", "(", "!", "-", "not", "and", "or"])?;
    } else {
        lex.check_prev_adjacent_token(&["|", ":", "(", "!", "-"])?;
    }

    if lex.is_keyword(&["{"]) {
        return parse_filter_stream_internal(lex, field_name);
    }
    if lex.is_keyword(&["*"]) {
        return parse_filter_star(lex, field_name);
    }
    if lex.is_keyword(&["("]) {
        return parse_filter_parens(lex, field_name);
    }
    if lex.is_keyword(&[">"]) {
        return parse_filter_gt(lex, field_name);
    }
    if lex.is_keyword(&["<"]) {
        return parse_filter_lt(lex, field_name);
    }
    if lex.is_keyword(&["="]) {
        return parse_filter_eq(lex, field_name);
    }
    if lex.is_keyword(&["!="]) {
        return parse_filter_neq(lex, field_name);
    }
    if lex.is_keyword(&["~"]) {
        return parse_filter_tilda(lex, field_name);
    }
    if lex.is_keyword(&["!~"]) {
        return parse_filter_not_tilda(lex, field_name);
    }
    if lex.is_keyword(&["not", "!", "-"]) {
        return parse_filter_not(lex, field_name);
    }
    if lex.is_keyword(&["contains_all"]) {
        return parse_in_values(lex, field_name, InKind::ContainsAll);
    }
    if lex.is_keyword(&["contains_any"]) {
        return parse_in_values(lex, field_name, InKind::ContainsAny);
    }
    if lex.is_keyword(&["json_array_contains_any"]) {
        return parse_filter_json_array_contains_any(lex, field_name);
    }
    if lex.is_keyword(&["contains_common_case"]) {
        return parse_filter_contains_common_case(lex, field_name);
    }
    if lex.is_keyword(&["eq_field"]) {
        return parse_filter_eq_field(lex, field_name);
    }
    if lex.is_keyword(&["equals_common_case"]) {
        return parse_filter_equals_common_case(lex, field_name);
    }
    if lex.is_keyword(&["exact"]) {
        return parse_filter_exact(lex, field_name);
    }
    if lex.is_keyword(&["i"]) {
        return parse_any_case_filter(lex, field_name);
    }
    if lex.is_keyword(&["in"]) {
        return parse_in_values(lex, field_name, InKind::In);
    }
    if lex.is_keyword(&["ipv4_range"]) {
        return parse_filter_ipv4_range(lex, field_name);
    }
    if lex.is_keyword(&["ipv6_range"]) {
        return parse_filter_ipv6_range(lex, field_name);
    }
    if lex.is_keyword(&["le_field"]) {
        return parse_filter_le_field(lex, field_name, false);
    }
    if lex.is_keyword(&["len_range"]) {
        return parse_filter_len_range(lex, field_name);
    }
    if lex.is_keyword(&["lt_field"]) {
        return parse_filter_le_field(lex, field_name, true);
    }
    if lex.is_keyword(&["pattern_match"]) {
        return parse_filter_pattern_match(lex, field_name, PatternMatcherOption::Any);
    }
    if lex.is_keyword(&["pattern_match_full"]) {
        return parse_filter_pattern_match(lex, field_name, PatternMatcherOption::Full);
    }
    if lex.is_keyword(&["pattern_match_prefix"]) {
        return parse_filter_pattern_match(lex, field_name, PatternMatcherOption::Prefix);
    }
    if lex.is_keyword(&["pattern_match_suffix"]) {
        return parse_filter_pattern_match(lex, field_name, PatternMatcherOption::Suffix);
    }
    if lex.is_keyword(&["range"]) {
        return parse_filter_range(lex, field_name);
    }
    if lex.is_keyword(&["re"]) {
        return parse_filter_regexp(lex, field_name);
    }
    if lex.is_keyword(&["seq"]) {
        return parse_filter_sequence(lex, field_name);
    }
    if lex.is_keyword(&["string_range"]) {
        return parse_filter_string_range(lex, field_name);
    }
    if lex.is_keyword(&["value_type"]) {
        return parse_filter_value_type(lex, field_name);
    }
    if lex.is_keyword(&["_time"]) {
        return parse_filter_time_generic(lex, field_name);
    }
    if lex.is_keyword(&["_stream_id"]) {
        return parse_filter_stream_id(lex, field_name);
    }
    if lex.is_keyword(&["_stream"]) {
        return parse_filter_stream(lex, field_name);
    }
    parse_filter_phrase(lex, field_name)
}

fn parse_filter_phrase(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    let stop_tokens: &[&str] = if field_name.is_empty() { &[":"] } else { &[] };
    // `phrase_bytes` is the Go-parity raw payload (parser.go:329
    // strconv.Unquote: `"\xff"` in query text denotes the raw byte 0xFF).
    // The same raw bytes act as the field NAME when a ':' follows (field
    // names are raw bytes end-to-end).
    let (_, phrase_bytes) = lex.next_compound_token_ext_pair(stop_tokens)?;

    if !lex.is_skipped_space() && lex.is_keyword(&["*"]) {
        lex.next_token();
        if field_name.is_empty() && lex.is_keyword(&[":"]) {
            lex.next_token();
            let mut wildcard_name = phrase_bytes.clone();
            wildcard_name.push(b'*');
            return parse_filter_generic(lex, &wildcard_name);
        }
        return Ok(Box::new(new_filter_prefix(field_name, &phrase_bytes)));
    }

    if field_name.is_empty() && lex.is_keyword(&[":"]) {
        lex.next_token();
        return match phrase_bytes.as_slice() {
            b"_time" => parse_filter_time_internal(lex),
            b"_stream_id" => parse_filter_stream_id_internal(lex),
            b"_stream" => parse_filter_stream_internal(lex, b"_stream"),
            _ => parse_filter_generic(lex, &phrase_bytes),
        };
    }

    Ok(Box::new(new_filter_phrase(field_name, &phrase_bytes)))
}

fn parse_filter_parens(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    lex.next_token();
    let f = parse_filter_or(lex, field_name)?;
    if !lex.is_keyword(&[")"]) {
        return Err(format!("missing ')'; got {}", go_quote(&lex.token)));
    }
    lex.next_token();
    Ok(f)
}

fn parse_filter_not(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    lex.next_token();
    let mut f = parse_filter_generic(lex, field_name)?;
    // Go collapses double-negation via a type switch on *filterNot.
    // PORT NOTE: the port collapses through the `take_not_child` trait hook
    // (only `FilterNot` implements it).
    if let Some(child) = f.take_not_child() {
        return Ok(child);
    }
    Ok(Box::new(new_filter_not(f)))
}

fn parse_any_case_filter(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    parse_func_arg_maybe_prefix(lex, field_name, |phrase, is_prefix| {
        if is_prefix {
            Ok(Box::new(new_filter_any_case_prefix(field_name, phrase)))
        } else {
            Ok(Box::new(new_filter_any_case_phrase(field_name, phrase)))
        }
    })
}

fn parse_func_arg_maybe_prefix(
    lex: &mut Lexer,
    field_name: &[u8],
    callback: impl Fn(&[u8], bool) -> Result<BoxFilter, String>,
) -> Result<BoxFilter, String> {
    let lex_state = lex.clone();
    let func_name = lex.token.clone();
    lex.next_token();

    if !lex.is_keyword(&["("]) {
        *lex = lex_state;
        return parse_filter_phrase(lex, field_name);
    }
    lex.next_token();

    // Raw-byte payload (Go parser.go:329 strconv.Unquote semantics).
    let mut arg = Vec::new();
    let mut is_wildcard = lex.is_keyword(&["*"]);
    if is_wildcard {
        lex.next_token();
    } else {
        let token = lex
            .next_compound_token_bytes()
            .map_err(|e| format!("cannot read {func_name}() arg: {e}"))?;
        arg = token;
        if !lex.is_skipped_space() && lex.is_keyword(&["*"]) {
            lex.next_token();
            is_wildcard = true;
        }
    }
    if !lex.is_keyword(&[")"]) {
        return Err(format!(
            "missing ')' for {func_name}; got {}",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    callback(&arg, is_wildcard)
}

fn parse_filter_len_range(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    parse_func_args(lex, field_name, |func_name, args| {
        if args.len() != 2 {
            return Err(format!(
                "unexpected number of args for {func_name}(); got {}; want 2",
                args.len()
            ));
        }
        let min_len = parse_uint(&args[0])
            .map_err(|e| format!("cannot parse minLen at {func_name}(): {e}"))?;
        let max_len = parse_uint(&args[1])
            .map_err(|e| format!("cannot parse maxLen at {func_name}(): {e}"))?;
        let string_repr = format!("({}, {})", args[0], args[1]);
        Ok(Box::new(new_filter_len_range(
            field_name,
            min_len,
            max_len,
            &string_repr,
        )))
    })
}

fn parse_filter_string_range(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    // Raw-byte bounds (Go parser.go:329 strconv.Unquote semantics); the
    // string_repr re-quotes them losslessly (Go quoteTokenIfNeeded, byte form).
    parse_func_args_bytes(lex, field_name, |func_name, args| {
        if args.len() != 2 {
            return Err(format!(
                "unexpected number of args for {func_name}(); got {}; want 2",
                args.len()
            ));
        }
        let string_repr = format!(
            "{func_name}({}, {})",
            crate::stream_filter::quote_value_bytes_if_needed(&args[0]),
            crate::stream_filter::quote_value_bytes_if_needed(&args[1])
        );
        Ok(Box::new(
            crate::filter_string_range::new_filter_string_range(
                field_name,
                &args[0],
                &args[1],
                &string_repr,
            ),
        ))
    })
}

fn parse_filter_value_type(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    parse_func_arg(lex, field_name, |_, arg| {
        Ok(Box::new(crate::filter_value_type::new_filter_value_type(
            field_name, arg,
        )))
    })
}

fn parse_filter_json_array_contains_any(
    lex: &mut Lexer,
    field_name: &[u8],
) -> Result<BoxFilter, String> {
    // Raw-byte values (Go parser.go:329 strconv.Unquote semantics).
    parse_func_args_bytes(lex, field_name, |_, args| {
        Ok(Box::new(new_filter_json_array_contains_any(
            field_name, args,
        )))
    })
}

fn parse_filter_ipv4_range(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    parse_func_args(lex, field_name, |func_name, args| {
        if args.len() == 1 {
            let (min_v, max_v) = try_parse_ipv4_cidr(&args[0]).ok_or_else(|| {
                format!(
                    "cannot parse IPv4 address or IPv4 CIDR {} at {func_name}()",
                    go_quote(&args[0])
                )
            })?;
            return Ok(Box::new(new_filter_ipv4_range(field_name, min_v, max_v)));
        }
        if args.len() != 2 {
            return Err(format!(
                "unexpected number of args for {func_name}(); got {}; want 2",
                args.len()
            ));
        }
        let min_v = try_parse_ipv4(&args[0]).ok_or_else(|| {
            format!(
                "cannot parse lower bound ip {} in {func_name}()",
                go_quote(&args[0])
            )
        })?;
        let max_v = try_parse_ipv4(&args[1]).ok_or_else(|| {
            format!(
                "cannot parse upper bound ip {} in {func_name}()",
                go_quote(&args[1])
            )
        })?;
        Ok(Box::new(new_filter_ipv4_range(field_name, min_v, max_v)))
    })
}

fn parse_filter_ipv6_range(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    parse_func_args(lex, field_name, |func_name, args| {
        if args.len() == 1 {
            let (min_v, max_v) = try_parse_ipv6_cidr(&args[0]).ok_or_else(|| {
                format!(
                    "cannot parse IPv6 address or IPv6 CIDR {} at {func_name}()",
                    go_quote(&args[0])
                )
            })?;
            return Ok(Box::new(new_filter_ipv6_range(field_name, min_v, max_v)));
        }
        if args.len() != 2 {
            return Err(format!(
                "unexpected number of args for {func_name}(); got {}; want 2",
                args.len()
            ));
        }
        let min_v = try_parse_ipv6(&args[0]).ok_or_else(|| {
            format!(
                "cannot parse lower bound ip {} in {func_name}()",
                go_quote(&args[0])
            )
        })?;
        let max_v = try_parse_ipv6(&args[1]).ok_or_else(|| {
            format!(
                "cannot parse upper bound ip {} in {func_name}()",
                go_quote(&args[1])
            )
        })?;
        Ok(Box::new(new_filter_ipv6_range(field_name, min_v, max_v)))
    })
}

#[derive(Clone, Copy)]
enum InKind {
    In,
    ContainsAll,
    ContainsAny,
}

/// Port of Go `parseInValues`.
fn parse_in_values(lex: &mut Lexer, field_name: &[u8], kind: InKind) -> Result<BoxFilter, String> {
    // Try parsing in(arg1, ..., argN) at first
    let lex_state = lex.clone();
    let err_first = match parse_func_args_possible_wildcard_bytes(lex) {
        Ok(None) => return Ok(Box::new(new_filter_noop())),
        Ok(Some(args)) => {
            return Ok(match kind {
                InKind::In => Box::new(new_filter_in_values(field_name, args)),
                InKind::ContainsAll => Box::new(new_filter_contains_all_values(field_name, args)),
                InKind::ContainsAny => Box::new(new_filter_contains_any_values(field_name, args)),
            });
        }
        Err(e) => e,
    };
    let state_first = lex.clone();

    // Parse in(query | fields someField) then
    *lex = lex_state;
    lex.next_token();

    match parse_in_query(lex) {
        Err(_) => {
            // Return the previous error from parsing in(arg1, ..., argN) for
            // simpler debugging.
            *lex = state_first;
            Err(err_first)
        }
        Ok(None) => Ok(Box::new(new_filter_noop())),
        Ok(Some((q_text, q_field_name))) => Ok(match kind {
            InKind::In => Box::new(crate::filter_in::new_filter_in_query(
                field_name,
                q_text,
                q_field_name,
            )),
            InKind::ContainsAll => {
                Box::new(crate::filter_contains_all::new_filter_contains_all_query(
                    field_name,
                    q_text,
                    q_field_name,
                ))
            }
            InKind::ContainsAny => {
                Box::new(crate::filter_contains_any::new_filter_contains_any_query(
                    field_name,
                    q_text,
                    q_field_name,
                ))
            }
        }),
    }
}

/// Port of Go `parseInQuery`. Returns `None` for a star subquery (Go returns a
/// nil query), otherwise the subquery's rendered text plus the field name whose
/// values it yields.
///
/// PORT NOTE: Go keeps the parsed `*Query`; the Rust filters store its rendered
/// text (see the module docs), so the subquery is optimized (the ported subset,
/// as Go's top-level `optimize()` would via `visitSubqueries`) and rendered
/// here.
fn parse_in_query(lex: &mut Lexer) -> Result<Option<(String, Vec<u8>)>, String> {
    let mut q = crate::parser::query::parse_query_in_parens(lex)
        .map_err(|e| format!("cannot parse in(...) query: {e}"))?;
    if q.is_star_query() {
        return Ok(None);
    }
    let q_field_name = get_field_name_from_pipes(q.pipes())
        .map_err(|e| format!("cannot determine field name for values in 'in({q})': {e}"))?;
    q.optimize();
    Ok(Some((q.to_string(), q_field_name)))
}

/// Port of Go `getFieldNameFromPipes`.
fn get_field_name_from_pipes(pipes: &[Box<dyn crate::pipe::Pipe>]) -> Result<Vec<u8>, String> {
    let Some(last) = pipes.last() else {
        return Err("missing 'fields' or 'uniq' pipes at the end of query".to_string());
    };
    match last.in_query_field_name() {
        Some(res) => res,
        None => Err("missing 'fields' or 'uniq' pipe at the end of query".to_string()),
    }
}

/// Port of Go `parseFuncArgsPossibleWildcard`. Returns `None` on a wildcard arg.
fn parse_func_args_possible_wildcard(lex: &mut Lexer) -> Result<Option<Vec<String>>, String> {
    let func_name = lex.token.clone();
    lex.next_token();
    if !lex.is_keyword(&["("]) {
        return Err(format!(
            "the {} must be put in quotes",
            go_quote(&func_name)
        ));
    }
    let (args, is_wildcard) = parse_args_in_parens_possible_wildcard(lex)
        .map_err(|e| format!("cannot parse {func_name}(): {e}"))?;
    if is_wildcard {
        return Ok(None);
    }
    Ok(Some(args))
}

/// Byte form of [`parse_func_args_possible_wildcard`] for filters whose args
/// are raw-byte value payloads (Go parser.go:329 strconv.Unquote semantics):
/// `in()` / `contains_any()` / `contains_all()` literal values.
fn parse_func_args_possible_wildcard_bytes(
    lex: &mut Lexer,
) -> Result<Option<Vec<Vec<u8>>>, String> {
    let func_name = lex.token.clone();
    lex.next_token();
    if !lex.is_keyword(&["("]) {
        return Err(format!(
            "the {} must be put in quotes",
            go_quote(&func_name)
        ));
    }
    let (args, is_wildcard) =
        crate::stream_filter::parse_args_in_parens_possible_wildcard_bytes(lex)
            .map_err(|e| format!("cannot parse {func_name}(): {e}"))?;
    if is_wildcard {
        return Ok(None);
    }
    Ok(Some(args))
}

fn parse_filter_contains_common_case(
    lex: &mut Lexer,
    field_name: &[u8],
) -> Result<BoxFilter, String> {
    lex.next_token();
    // Raw-byte phrases (Go parser.go:329 strconv.Unquote semantics).
    let phrases = parse_args_in_parens_bytes(lex)
        .map_err(|e| format!("cannot parse 'contains_common_case(...)' args: {e}"))?;
    new_filter_contains_common_case(field_name, phrases)
        .map(|f| Box::new(f) as BoxFilter)
        .map_err(|e| format!("cannot parse 'contains_common_case(...)': {e}"))
}

fn parse_filter_equals_common_case(
    lex: &mut Lexer,
    field_name: &[u8],
) -> Result<BoxFilter, String> {
    lex.next_token();
    // Raw-byte phrases (Go parser.go:329 strconv.Unquote semantics).
    let phrases = parse_args_in_parens_bytes(lex)
        .map_err(|e| format!("cannot parse 'equals_common_case(...)' args: {e}"))?;
    new_filter_equals_common_case(field_name, phrases)
        .map(|f| Box::new(f) as BoxFilter)
        .map_err(|e| format!("cannot parse 'equals_common_case(...)': {e}"))
}

fn parse_filter_sequence(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    parse_func_args_bytes(lex, field_name, |_, args| {
        Ok(Box::new(new_filter_sequence(field_name, args)))
    })
}

fn parse_filter_eq_field(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    parse_func_arg_bytes(lex, field_name, |_, arg| {
        Ok(Box::new(new_filter_eq_field(field_name, arg)))
    })
}

fn parse_filter_le_field(
    lex: &mut Lexer,
    field_name: &[u8],
    exclude_equal: bool,
) -> Result<BoxFilter, String> {
    parse_func_arg_bytes(lex, field_name, |_, arg| {
        Ok(Box::new(new_filter_le_field(
            field_name,
            arg,
            exclude_equal,
        )))
    })
}

fn parse_filter_exact(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    parse_func_arg_maybe_prefix(lex, field_name, |phrase, is_prefix| {
        if is_prefix {
            Ok(Box::new(new_filter_exact_prefix(field_name, phrase)))
        } else {
            Ok(Box::new(new_filter_exact(field_name, phrase)))
        }
    })
}

fn parse_filter_pattern_match(
    lex: &mut Lexer,
    field_name: &[u8],
    pmo: PatternMatcherOption,
) -> Result<BoxFilter, String> {
    parse_func_arg(lex, field_name, |func_name, arg| {
        let pm = new_pattern_matcher(arg, pmo);
        Ok(Box::new(new_filter_pattern_match(
            field_name, func_name, pm,
        )))
    })
}

fn parse_filter_regexp(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    parse_func_arg(lex, field_name, |_, arg| {
        new_filter_regexp_opt(field_name, arg)
    })
}

/// Port of Go `newFilterRegexpOpt`.
fn new_filter_regexp_opt(field_name: &[u8], arg: &str) -> Result<BoxFilter, String> {
    if arg.is_empty() || arg == ".*" {
        return Ok(Box::new(new_filter_noop()));
    }
    if arg == ".+" {
        return Ok(Box::new(new_filter_prefix(field_name, "")));
    }
    let re = Regex::new(arg).map_err(|e| {
        format!(
            "invalid regexp {}:{}: {e}",
            // go_quote_bytes: display-only quoting of a raw-byte name in the
            // error message (Go %q over raw bytes).
            crate::stream_filter::go_quote_bytes(get_canonical_column_name_bytes(field_name)),
            go_quote(arg)
        )
    })?;
    Ok(Box::new(new_filter_regexp(field_name, re, arg.to_string())))
}

fn parse_filter_star(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    lex.next_token();

    if field_name.is_empty() && lex.is_keyword(&[":"]) {
        lex.next_token();
        return parse_filter_generic(lex, b"*");
    }

    if lex.is_skipped_space() || lex.is_query_part_trailer() {
        return Ok(Box::new(new_filter_prefix(field_name, "")));
    }

    // Raw-byte payload (Go parser.go:329 strconv.Unquote semantics); the
    // error messages quote it with Go %q byte semantics (`go_quote_bytes`).
    let phrase = lex
        .next_compound_token_bytes()
        .map_err(|e| format!("cannot read *substr* filter: {e}"))?;
    if lex.is_skipped_space() || !lex.is_keyword(&["*"]) {
        return Err(format!(
            "missing ending '*' in the *{}* filter",
            crate::stream_filter::go_quote_bytes(&phrase)
        ));
    }
    lex.next_token();
    if !lex.is_skipped_space() && !lex.is_query_part_trailer() {
        return Err(format!(
            "missing whitespace between *{}* and {}",
            crate::stream_filter::go_quote_bytes(&phrase),
            go_quote(&lex.token)
        ));
    }
    Ok(Box::new(crate::filter_substring::new_filter_substring(
        field_name, &phrase,
    )))
}

fn parse_filter_tilda(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    let op = lex.token.clone();
    lex.next_token();
    if lex.is_keyword(&["-"]) {
        return Err(format!(
            "regexp, which start with {}, must be put in quotes",
            go_quote(&lex.token)
        ));
    }
    if lex.is_skipped_space() && field_name.is_empty() {
        return Err(format!(
            "missing ':' in front of {}; see https://docs.victoriametrics.com/victorialogs/logsql/#filters",
            go_quote(&op)
        ));
    }
    let arg = lex.next_compound_token().map_err(|e| {
        format!(
            "cannot read regexp for field {}: {e}",
            // go_quote_bytes: display-only quoting of a raw-byte name in the
            // error message (Go %q over raw bytes).
            crate::stream_filter::go_quote_bytes(get_canonical_column_name_bytes(field_name))
        )
    })?;
    new_filter_regexp_opt(field_name, &arg)
}

fn parse_filter_not_tilda(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    let f = parse_filter_tilda(lex, field_name)?;
    Ok(Box::new(new_filter_not(f)))
}

fn parse_filter_eq(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    let op = lex.token.clone();
    lex.next_token();
    if lex.is_skipped_space() && field_name.is_empty() {
        return Err(format!(
            "missing ':' in front of {}; see https://docs.victoriametrics.com/victorialogs/logsql/#filters",
            go_quote(&op)
        ));
    }
    // Raw-byte payload (Go parser.go:329 strconv.Unquote semantics).
    let phrase = lex
        .next_compound_token_bytes()
        .map_err(|e| format!("cannot parse token after {}: {e}", go_quote(&op)))?;
    if !lex.is_skipped_space() && lex.is_keyword(&["*"]) {
        lex.next_token();
        return Ok(Box::new(new_filter_exact_prefix(field_name, &phrase)));
    }
    Ok(Box::new(new_filter_exact(field_name, &phrase)))
}

fn parse_filter_neq(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    let f = parse_filter_eq(lex, field_name)?;
    Ok(Box::new(new_filter_not(f)))
}

fn parse_filter_gt(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    lex.next_token();
    let mut include_min_value = false;
    let mut op = ">".to_string();
    if !lex.is_skipped_space() && lex.is_keyword(&["="]) {
        lex.next_token();
        include_min_value = true;
        op = ">=".to_string();
    }
    if lex.is_skipped_space() && field_name.is_empty() {
        return Err(missing_colon_err(&op));
    }
    let lex_state = lex.clone();
    match parse_number(lex) {
        Ok((mut min_value, f_str)) => {
            if !include_min_value {
                min_value = nextafter(min_value, INF);
            }
            let string_repr = format!("{op}{f_str}");
            Ok(Box::new(new_filter_range(
                field_name,
                min_value,
                INF,
                &string_repr,
            )))
        }
        Err(e) => {
            *lex = lex_state;
            match try_parse_filter_gt_string(lex, field_name, &op, include_min_value) {
                Some(f) => Ok(f),
                None => Err(format!("cannot parse [] as number: {e}")),
            }
        }
    }
}

fn parse_filter_lt(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    lex.next_token();
    let mut include_max_value = false;
    let mut op = "<".to_string();
    if !lex.is_skipped_space() && lex.is_keyword(&["="]) {
        lex.next_token();
        include_max_value = true;
        op = "<=".to_string();
    }
    if lex.is_skipped_space() && field_name.is_empty() {
        return Err(missing_colon_err(&op));
    }
    let lex_state = lex.clone();
    match parse_number(lex) {
        Ok((mut max_value, f_str)) => {
            if !include_max_value {
                max_value = nextafter(max_value, -INF);
            }
            let string_repr = format!("{op}{f_str}");
            Ok(Box::new(new_filter_range(
                field_name,
                -INF,
                max_value,
                &string_repr,
            )))
        }
        Err(e) => {
            *lex = lex_state;
            match try_parse_filter_lt_string(lex, field_name, &op, include_max_value) {
                Some(f) => Ok(f),
                None => Err(format!("cannot parse [] as number: {e}")),
            }
        }
    }
}

fn missing_colon_err(op: &str) -> String {
    format!(
        "missing ':' in front of {}; see https://docs.victoriametrics.com/victorialogs/logsql/#filters",
        go_quote(op)
    )
}

fn try_parse_filter_gt_string(
    lex: &mut Lexer,
    field_name: &[u8],
    op: &str,
    include_min_value: bool,
) -> Option<BoxFilter> {
    // Raw-byte bound (Go parser.go:329 strconv.Unquote semantics).
    let min_value_orig = lex.next_compound_token_bytes().ok()?;
    let mut min_value = min_value_orig.clone();
    if !include_min_value {
        min_value.push(0);
    }
    let string_repr = format!(
        "{op}{}",
        crate::parser::quote_string_value_bytes_if_needed(&min_value_orig)
    );
    Some(Box::new(
        crate::filter_string_range::new_filter_string_range(
            field_name,
            &min_value,
            MAX_STRING_RANGE_VALUE,
            &string_repr,
        ),
    ))
}

fn try_parse_filter_lt_string(
    lex: &mut Lexer,
    field_name: &[u8],
    op: &str,
    include_max_value: bool,
) -> Option<BoxFilter> {
    // Raw-byte bound (Go parser.go:329 strconv.Unquote semantics).
    let max_value_orig = lex.next_compound_token_bytes().ok()?;
    let mut max_value = max_value_orig.clone();
    if include_max_value {
        max_value.push(0);
    }
    let string_repr = format!(
        "{op}{}",
        crate::parser::quote_string_value_bytes_if_needed(&max_value_orig)
    );
    Some(Box::new(
        crate::filter_string_range::new_filter_string_range(
            field_name,
            "",
            &max_value,
            &string_repr,
        ),
    ))
}

fn parse_filter_range(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    let lex_state = lex.clone();
    let func_name = lex.token.clone();
    lex.next_token();

    let include_min_value = if lex.is_keyword(&["("]) {
        false
    } else if lex.is_keyword(&["["]) {
        true
    } else {
        *lex = lex_state;
        return parse_filter_phrase(lex, field_name);
    };
    lex.next_token();

    let (mut min_value, min_value_str) =
        parse_number(lex).map_err(|e| format!("cannot parse minValue in {func_name}(): {e}"))?;
    if !lex.is_keyword(&[","]) {
        return Err(format!(
            "unexpected token {} ater {} in {func_name}(); want ','",
            go_quote(&lex.token),
            go_quote(&min_value_str)
        ));
    }
    lex.next_token();

    let (mut max_value, max_value_str) =
        parse_number(lex).map_err(|e| format!("cannot parse maxValue in {func_name}(): {e}"))?;
    let include_max_value = if lex.is_keyword(&[")"]) {
        false
    } else if lex.is_keyword(&["]"]) {
        true
    } else {
        return Err(format!(
            "unexpected closing token {} in {func_name}(); want ')' or ']'",
            go_quote(&lex.token)
        ));
    };
    lex.next_token();

    let mut string_repr = "range".to_string();
    if include_min_value {
        string_repr.push('[');
    } else {
        string_repr.push('(');
        min_value = nextafter(min_value, INF);
    }
    string_repr += &format!("{min_value_str}, {max_value_str}");
    if include_max_value {
        string_repr.push(']');
    } else {
        string_repr.push(')');
        max_value = nextafter(max_value, -INF);
    }
    Ok(Box::new(new_filter_range(
        field_name,
        min_value,
        max_value,
        &string_repr,
    )))
}

// ---- func-arg helpers (Go parseFuncArg / parseFuncArgs) ----

fn parse_func_arg(
    lex: &mut Lexer,
    field_name: &[u8],
    callback: impl Fn(&str, &str) -> Result<BoxFilter, String>,
) -> Result<BoxFilter, String> {
    parse_func_args(lex, field_name, |func_name, args| {
        if args.len() != 1 {
            return Err(format!(
                "unexpected number of args for {func_name}(); got {}; want 1",
                args.len()
            ));
        }
        callback(func_name, &args[0])
    })
}

fn parse_func_args(
    lex: &mut Lexer,
    field_name: &[u8],
    callback: impl Fn(&str, Vec<String>) -> Result<BoxFilter, String>,
) -> Result<BoxFilter, String> {
    let lex_state = lex.clone();
    let func_name = lex.token.clone();
    lex.next_token();
    if !lex.is_keyword(&["("]) {
        *lex = lex_state;
        return parse_filter_phrase(lex, field_name);
    }
    let args = parse_args_in_parens(lex).map_err(|e| format!("cannot parse {func_name}(): {e}"))?;
    callback(&func_name, args)
}

/// Byte form of [`parse_func_args`] for filters whose args are raw-byte
/// phrase payloads (Go parser.go:329 strconv.Unquote semantics).
/// Single-raw-byte-arg form of [`parse_func_arg`] (Go strings are raw bytes;
/// used where the arg is a field name or byte payload).
fn parse_func_arg_bytes(
    lex: &mut Lexer,
    field_name: &[u8],
    callback: impl Fn(&str, &[u8]) -> Result<BoxFilter, String>,
) -> Result<BoxFilter, String> {
    parse_func_args_bytes(lex, field_name, |func_name, args| {
        if args.len() != 1 {
            return Err(format!(
                "unexpected number of args for {func_name}(); got {}; want 1",
                args.len()
            ));
        }
        callback(func_name, &args[0])
    })
}

fn parse_func_args_bytes(
    lex: &mut Lexer,
    field_name: &[u8],
    callback: impl Fn(&str, Vec<Vec<u8>>) -> Result<BoxFilter, String>,
) -> Result<BoxFilter, String> {
    let lex_state = lex.clone();
    let func_name = lex.token.clone();
    lex.next_token();
    if !lex.is_keyword(&["("]) {
        *lex = lex_state;
        return parse_filter_phrase(lex, field_name);
    }
    let args =
        parse_args_in_parens_bytes(lex).map_err(|e| format!("cannot parse {func_name}(): {e}"))?;
    callback(&func_name, args)
}

// ---- _time filters ----

fn parse_filter_time_generic(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    if !field_name.is_empty() {
        return parse_filter_phrase(lex, field_name);
    }
    let lex_state = lex.clone();
    lex.next_token();
    if !lex.is_keyword(&[":"]) {
        *lex = lex_state;
        return parse_filter_phrase(lex, b"");
    }
    lex.next_token();
    parse_filter_time_internal(lex)
}

fn parse_filter_time_internal(lex: &mut Lexer) -> Result<BoxFilter, String> {
    if lex.is_keyword(&["day_range"]) {
        return parse_filter_day_range(lex);
    }
    if lex.is_keyword(&["week_range"]) {
        return parse_filter_week_range(lex);
    }
    parse_filter_time_range(lex)
}

fn parse_filter_day_range(lex: &mut Lexer) -> Result<BoxFilter, String> {
    lex.next_token();
    let start_brace = if lex.is_keyword(&["["]) {
        lex.next_token();
        "["
    } else if lex.is_keyword(&["("]) {
        lex.next_token();
        "("
    } else {
        return Err("missing '[' or '(' at day_range filter".to_string());
    };

    let (mut start, start_str) = get_day_range_arg(lex)
        .map_err(|e| format!("cannot read `start` arg at day_range filter: {e}"))?;
    if !lex.is_keyword(&[","]) {
        return Err(format!(
            "unexpected token {}; want ','",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    let (mut end, end_str) = get_day_range_arg(lex)
        .map_err(|e| format!("cannot read `end` arg at day_range filter: {e}"))?;

    let end_brace = if lex.is_keyword(&["]"]) {
        lex.next_token();
        "]"
    } else if lex.is_keyword(&[")"]) {
        lex.next_token();
        ")"
    } else {
        return Err("missing ']' or ')' after day_range filter".to_string());
    };

    let mut offset = esl_common::timeutil::get_local_timezone_offset_nsecs();
    let mut offset_str = String::new();
    if lex.is_keyword(&["offset"]) {
        lex.next_token();
        let (d, s) = parse_duration(lex)
            .map_err(|e| format!("cannot parse offset in day_range filter: {e}"))?;
        offset = d;
        offset_str = format!(" offset {s}");
    }

    if start_brace == "(" {
        start += 1;
        if start > NSECS_PER_DAY {
            start = 0;
        }
    }
    if end_brace == ")" {
        end -= 1;
        if end < 0 {
            end = NSECS_PER_DAY - 1;
        }
    }
    let string_repr = format!("{start_brace}{start_str}, {end_str}{end_brace}{offset_str}");
    Ok(Box::new(new_filter_day_range(
        start,
        end,
        offset,
        &string_repr,
    )))
}

fn parse_filter_week_range(lex: &mut Lexer) -> Result<BoxFilter, String> {
    lex.next_token();
    let start_brace = if lex.is_keyword(&["["]) {
        lex.next_token();
        "["
    } else if lex.is_keyword(&["("]) {
        lex.next_token();
        "("
    } else {
        return Err("missing '[' or '(' at week_range filter".to_string());
    };
    let (mut start_day, start_str) = get_week_range_arg(lex)
        .map_err(|e| format!("cannot read `start` arg at week_range filter: {e}"))?;
    if !lex.is_keyword(&[","]) {
        return Err(format!(
            "unexpected token {}; want ','",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    let (mut end_day, end_str) = get_week_range_arg(lex)
        .map_err(|e| format!("cannot read `end` arg at week_range filter: {e}"))?;
    let end_brace = if lex.is_keyword(&["]"]) {
        lex.next_token();
        "]"
    } else if lex.is_keyword(&[")"]) {
        lex.next_token();
        ")"
    } else {
        return Err("missing ']' or ')' after week_range filter".to_string());
    };

    let mut offset = esl_common::timeutil::get_local_timezone_offset_nsecs();
    let mut offset_str = String::new();
    if lex.is_keyword(&["offset"]) {
        lex.next_token();
        let (d, s) = parse_duration(lex)
            .map_err(|e| format!("cannot parse offset in week_range filter: {e}"))?;
        offset = d;
        offset_str = format!(" offset {s}");
    }

    if start_brace == "(" {
        start_day += 1;
        if start_day > 6 {
            start_day = 0;
        }
    }
    if end_brace == ")" {
        end_day -= 1;
        if end_day < 0 {
            end_day = 6;
        }
    }
    let string_repr = format!("{start_brace}{start_str}, {end_str}{end_brace}{offset_str}");
    Ok(Box::new(crate::filter_week_range::new_filter_week_range(
        start_day,
        end_day,
        offset,
        &string_repr,
    )))
}

fn get_day_range_arg(lex: &mut Lexer) -> Result<(i64, String), String> {
    let arg_str = lex.next_compound_token()?;
    let mut offset = try_parse_hhmm(&arg_str)
        .ok_or_else(|| format!("cannot parse {} as 'hh:mm'", go_quote(&arg_str)))?;
    if offset >= NSECS_PER_DAY {
        offset = NSECS_PER_DAY - 1;
    }
    Ok((offset, arg_str))
}

fn get_week_range_arg(lex: &mut Lexer) -> Result<(i32, String), String> {
    let arg_str = lex.next_compound_token()?;
    let day = match arg_str.to_lowercase().as_str() {
        "sun" | "sunday" => 0,
        "mon" | "monday" => 1,
        "tue" | "tuesday" => 2,
        "wed" | "wednesday" => 3,
        "thu" | "thursday" => 4,
        "fri" | "friday" => 5,
        "sat" | "saturday" => 6,
        _ => return Err(format!("cannot parse {} as weekday", go_quote(&arg_str))),
    };
    Ok((day, arg_str))
}

fn parse_filter_time_range(lex: &mut Lexer) -> Result<BoxFilter, String> {
    if lex.is_keyword(&["offset"]) {
        let mut min_ts = i64::MIN;
        let mut max_ts = lex.current_timestamp();
        let (offset, offset_str) = parse_time_offset(lex)
            .map_err(|e| format!("cannot parse offset for _time filter []: {e}"))?;
        let _ = &mut min_ts;
        max_ts = sub_int64_no_overflow(max_ts, offset);
        return Ok(Box::new(crate::filter_time::new_filter_time(
            min_ts,
            max_ts,
            &offset_str,
        )));
    }

    let (mut min_ts, mut max_ts, mut string_repr) = parse_filter_time(lex)?;
    if lex.is_keyword(&["offset"]) {
        let (offset, offset_str) = parse_time_offset(lex)
            .map_err(|e| format!("cannot parse offset for _time filter [{string_repr}]: {e}"))?;
        min_ts = sub_int64_no_overflow(min_ts, offset);
        max_ts = sub_int64_no_overflow(max_ts, offset);
        string_repr = format!("{string_repr} {offset_str}");
    }
    Ok(Box::new(crate::filter_time::new_filter_time(
        min_ts,
        max_ts,
        &string_repr,
    )))
}

/// Returns `(min_ts, max_ts, string_repr)`. Port of Go `parseFilterTime`.
fn parse_filter_time(lex: &mut Lexer) -> Result<(i64, i64, String), String> {
    let start_time_include;
    if lex.is_keyword(&[">"]) {
        return parse_filter_time_gt(lex);
    } else if lex.is_keyword(&["<"]) {
        return parse_filter_time_lt(lex);
    } else if lex.is_keyword(&["["]) {
        lex.next_token();
        start_time_include = true;
    } else if lex.is_keyword(&["("]) {
        lex.next_token();
        start_time_include = false;
    } else {
        return parse_filter_time_eq(lex);
    }

    let (mut start_time, start_time_string) =
        parse_time(lex).map_err(|e| format!("cannot parse start time in _time filter: {e}"))?;
    if !lex.is_keyword(&[","]) {
        return Err(format!(
            "unexpected token after start time in _time filter: {}; want ','",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    let (mut end_time, end_time_string) =
        parse_time(lex).map_err(|e| format!("cannot parse end time in _time filter: {e}"))?;

    let end_time_include = if lex.is_keyword(&["]"]) {
        true
    } else if lex.is_keyword(&[")"]) {
        false
    } else {
        return Err(format!(
            "_time filter ends with unexpected token {}; it must end with ']' or ')'",
            go_quote(&lex.token)
        ));
    };
    lex.next_token();

    let mut string_repr = String::new();
    if start_time_include {
        string_repr.push('[');
    } else {
        string_repr.push('(');
        start_time += 1;
    }
    string_repr += &format!("{start_time_string},{end_time_string}");
    if end_time_include {
        string_repr.push(']');
        end_time = adjust_end_timestamp(end_time, &end_time_string);
    } else {
        string_repr.push(')');
        end_time -= 1;
    }
    Ok((start_time, end_time, string_repr))
}

fn parse_filter_time_gt(lex: &mut Lexer) -> Result<(i64, i64, String), String> {
    lex.next_token();
    let mut prefix = ">".to_string();
    if lex.is_keyword(&["="]) {
        lex.next_token();
        prefix = ">=".to_string();
    }
    if is_likely_timestamp(lex) {
        let (mut start_time, start_time_string) =
            parse_time(lex).map_err(|e| format!("cannot parse start time in _time filter: {e}"))?;
        if prefix == ">" {
            start_time += 1;
        }
        return Ok((start_time, i64::MAX, format!("{prefix}{start_time_string}")));
    }
    let (mut d, s) =
        parse_duration(lex).map_err(|e| format!("cannot parse duration at _time filter: {e}"))?;
    if d < 0 {
        d = -d;
    }
    if prefix == ">" {
        d += 1;
    }
    let max_ts = sub_int64_no_overflow(lex.current_timestamp(), d);
    Ok((i64::MIN, max_ts, format!("{prefix}{s}")))
}

fn parse_filter_time_lt(lex: &mut Lexer) -> Result<(i64, i64, String), String> {
    lex.next_token();
    let mut prefix = "<".to_string();
    if lex.is_keyword(&["="]) {
        lex.next_token();
        prefix = "<=".to_string();
    }
    if is_likely_timestamp(lex) {
        let (mut end_time, end_time_string) =
            parse_time(lex).map_err(|e| format!("cannot parse end time in _time filter: {e}"))?;
        if prefix == "<" {
            end_time -= 1;
        } else {
            end_time = adjust_end_timestamp(end_time, &end_time_string);
        }
        return Ok((i64::MIN, end_time, format!("{prefix}{end_time_string}")));
    }
    let (mut d, s) =
        parse_duration(lex).map_err(|e| format!("cannot parse duration at _time filter: {e}"))?;
    if d < 0 {
        d = -d;
    }
    if prefix == "<" {
        d -= 1;
    }
    let min_ts = sub_int64_no_overflow(lex.current_timestamp(), d);
    let max_ts = sub_int64_no_overflow(lex.current_timestamp(), 1);
    Ok((min_ts, max_ts, format!("{prefix}{s}")))
}

fn parse_filter_time_eq(lex: &mut Lexer) -> Result<(i64, i64, String), String> {
    let mut prefix = String::new();
    if lex.is_keyword(&["="]) {
        lex.next_token();
        prefix = "=".to_string();
    }
    if is_likely_timestamp(lex) {
        let (nsecs, s) = parse_time(lex).map_err(|e| format!("cannot parse _time filter: {e}"))?;
        let start_time = nsecs;
        let end_time = adjust_end_timestamp(start_time, &s);
        return Ok((start_time, end_time, format!("{prefix}{s}")));
    }
    let (mut d, s) =
        parse_duration(lex).map_err(|e| format!("cannot parse duration at _time filter: {e}"))?;
    if d < 0 {
        d = -d;
    }
    let min_ts = sub_int64_no_overflow(lex.current_timestamp(), d);
    let max_ts = sub_int64_no_overflow(lex.current_timestamp(), 1);
    Ok((min_ts, max_ts, format!("{prefix}{s}")))
}

// ---- _stream_id filters ----

fn parse_filter_stream_id(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    if !field_name.is_empty() {
        return parse_filter_phrase(lex, field_name);
    }
    let lex_state = lex.clone();
    lex.next_token();
    if !lex.is_keyword(&[":"]) {
        *lex = lex_state;
        return parse_filter_phrase(lex, b"");
    }
    lex.next_token();
    parse_filter_stream_id_internal(lex)
}

fn parse_filter_stream_id_internal(lex: &mut Lexer) -> Result<BoxFilter, String> {
    if lex.is_keyword(&["in"]) {
        return parse_filter_stream_id_in(lex);
    }
    let sid = parse_stream_id(lex).map_err(|e| format!("cannot parse _stream_id: {e}"))?;
    Ok(Box::new(crate::filter_stream_id::new_filter_stream_id(
        vec![sid],
    )))
}

fn parse_filter_stream_id_in(lex: &mut Lexer) -> Result<BoxFilter, String> {
    if !lex.is_keyword(&["in"]) {
        return Err(format!(
            "unexpected token {}; expecting 'in'",
            go_quote(&lex.token)
        ));
    }

    // Try parsing in(arg1, ..., argN) at first
    let lex_state = lex.clone();
    let parse_literal = |lex: &mut Lexer| -> Result<BoxFilter, String> {
        match parse_func_args_possible_wildcard(lex)? {
            None => Ok(Box::new(new_filter_noop())),
            Some(args) => {
                let mut stream_ids = Vec::with_capacity(args.len());
                for arg in &args {
                    let mut sid = StreamID::default();
                    if !sid.try_unmarshal_from_string(arg) {
                        return Err(format!(
                            "cannot unmarshal _stream_id from {}",
                            go_quote(arg)
                        ));
                    }
                    stream_ids.push(sid);
                }
                Ok(Box::new(crate::filter_stream_id::new_filter_stream_id(
                    stream_ids,
                )))
            }
        }
    };
    let err_first = match parse_literal(lex) {
        Ok(fs) => return Ok(fs),
        Err(e) => e,
    };
    let state_first = lex.clone();

    // Try parsing in(query)
    *lex = lex_state;
    lex.next_token();

    match parse_in_query(lex) {
        Err(_) => {
            // Return the previous error from parsing in(arg1, ..., argN) for
            // simpler debugging.
            *lex = state_first;
            Err(err_first)
        }
        Ok(None) => Ok(Box::new(new_filter_noop())),
        Ok(Some((q_text, q_field_name))) => Ok(Box::new(
            crate::filter_stream_id::new_filter_stream_id_from_query(q_text, q_field_name),
        )),
    }
}

fn parse_stream_id(lex: &mut Lexer) -> Result<StreamID, String> {
    let s = lex.next_compound_token()?;
    let mut sid = StreamID::default();
    if !sid.try_unmarshal_from_string(&s) {
        return Err(format!("cannot unmarshal _stream_id from {}", go_quote(&s)));
    }
    Ok(sid)
}

// ---- _stream filters ----

fn parse_filter_stream(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    if !field_name.is_empty() {
        return parse_filter_phrase(lex, field_name);
    }
    let lex_state = lex.clone();
    lex.next_token();
    if !lex.is_keyword(&[":"]) {
        *lex = lex_state;
        return parse_filter_phrase(lex, b"");
    }
    lex.next_token();
    parse_filter_stream_internal(lex, b"_stream")
}

fn parse_filter_stream_internal(lex: &mut Lexer, field_name: &[u8]) -> Result<BoxFilter, String> {
    if !field_name.is_empty() && field_name != b"_stream" {
        return Err(format!(
            "stream filter cannot be applied to {} field; it can be applied only to _stream field",
            crate::stream_filter::go_quote_bytes(field_name)
        ));
    }
    let sf = parse_stream_filter(lex)?;
    Ok(Box::new(crate::filter_stream::new_filter_stream(sf)))
}

//! Port of the LogsQL `|` pipe-chain grammar (`pipe.go` dispatch + every
//! `parsePipeX`).
//!
//! PORT NOTES:
//! * `join` / `union` subqueries are stored as query text (`query_text`), not an
//!   executable `Query` (matches the pipe structs' deferred subquery seam).

use std::sync::Arc;

use crate::if_filter::{IfFilter, parse_if_filter};
use crate::parser::helpers::*;
use crate::parser::lexer_ext::LexerExt;
use crate::parser::parse_filter::parse_filter;
use crate::parser::parse_stats::{
    is_stats_func_name, parse_pipe_running_stats, parse_pipe_stats,
    parse_pipe_stats_no_stats_keyword, parse_pipe_total_stats,
};
use crate::parser::query::parse_query_in_parens;
use crate::parser::{go_quote, quote_token_if_needed};
use crate::pipe::Pipe;
use crate::pipe_math::{MathEntry, MathExpr, PipeMath};
use crate::pipe_sort::{BySortField, PipeSort};
use crate::rows::Field;
use crate::stream_filter::Lexer;
use crate::values_encoder::try_parse_uint64;

const STREAM_CONTEXT_DEFAULT_TIME_WINDOW: i64 = 3600 * 1_000_000_000;
const FACETS_DEFAULT_LIMIT: u64 = 10;
const FACETS_DEFAULT_MAX_VALUES_PER_FIELD: u64 = 1000;
const FACETS_DEFAULT_MAX_VALUE_LEN: u64 = 128;
const TOP_DEFAULT_LIMIT: u64 = 10;

type BoxPipe = Box<dyn Pipe>;

/// Pipe keywords (Go `initPipeParsers` keys).
const PIPE_NAMES: &[&str] = &[
    "block_stats",
    "blocks_count",
    "coalesce",
    "collapse_nums",
    "copy",
    "cp",
    "decolorize",
    "del",
    "delete",
    "drop",
    "drop_empty_fields",
    "extract",
    "extract_regexp",
    "eval",
    "facets",
    "field_names",
    "field_values",
    "fields",
    "filter",
    "first",
    "format",
    "generate_sequence",
    "hash",
    "join",
    "json_array_len",
    "head",
    "keep",
    "last",
    "len",
    "limit",
    "math",
    "mv",
    "offset",
    "order",
    "pack_json",
    "pack_logfmt",
    "query_stats",
    "rename",
    "replace",
    "replace_regexp",
    "rm",
    "running_stats",
    "sample",
    "set_stream_fields",
    "skip",
    "sort",
    "split",
    "stats",
    "stats_remote",
    "stream_context",
    "time_add",
    "top",
    "total_stats",
    "union",
    "uniq",
    "unpack_json",
    "unpack_logfmt",
    "unpack_syslog",
    "unpack_words",
    "unroll",
    "where",
];

/// Port of Go `isPipeName`.
pub(crate) fn is_pipe_name(s: &str) -> bool {
    PIPE_NAMES.contains(&s.to_lowercase().as_str())
}

/// Port of Go `parsePipes`.
pub(crate) fn parse_pipes(lex: &mut Lexer) -> Result<Vec<BoxPipe>, String> {
    let mut pipes = Vec::new();
    loop {
        let p = parse_pipe(lex)?;
        pipes.push(p);
        if lex.is_query_part_trailer() {
            if !lex.is_keyword(&["|"]) {
                return Ok(pipes);
            }
            lex.next_token();
        } else {
            return Err(format!(
                "unexpected token after [{}]: {}; expecting '|', ';' or ')'",
                pipes.last().unwrap().to_string(),
                go_quote(&lex.token)
            ));
        }
    }
}

/// Port of Go `mustParsePipe` (pipe.go).
///
/// Parses a single pipe from `s` in the context of `timestamp`, panicking on
/// invalid input (callers pass compile-time-known pipe strings).
pub(crate) fn must_parse_pipe(s: &str, timestamp: i64) -> BoxPipe {
    let mut lex = Lexer::new_at(s, timestamp);
    let p = match parse_pipe(&mut lex) {
        Ok(p) => p,
        Err(err) => {
            esl_common::panicf!("BUG: cannot parse [{s}]: {err}");
            unreachable!()
        }
    };
    if !lex.is_end() {
        esl_common::panicf!(
            "BUG: unexpected tail left after parsing [{s}]: {}",
            lex.context()
        );
    }
    p
}

/// Port of Go `parsePipe`.
fn parse_pipe(lex: &mut Lexer) -> Result<BoxPipe, String> {
    macro_rules! disp {
        ($kws:expr, $name:literal, $parser:ident) => {
            if lex.is_keyword($kws) {
                return $parser(lex)
                    .map_err(|e| format!("cannot parse {} pipe: {e}", go_quote($name)));
            }
        };
    }
    disp!(&["block_stats"], "block_stats", parse_pipe_block_stats);
    disp!(&["blocks_count"], "blocks_count", parse_pipe_blocks_count);
    disp!(&["coalesce"], "coalesce", parse_pipe_coalesce);
    disp!(
        &["collapse_nums"],
        "collapse_nums",
        parse_pipe_collapse_nums
    );
    disp!(&["copy", "cp"], "copy", parse_pipe_copy);
    disp!(&["decolorize"], "decolorize", parse_pipe_decolorize);
    disp!(
        &["del", "delete", "drop", "rm"],
        "delete",
        parse_pipe_delete
    );
    disp!(
        &["drop_empty_fields"],
        "drop_empty_fields",
        parse_pipe_drop_empty_fields
    );
    disp!(&["extract"], "extract", parse_pipe_extract);
    disp!(
        &["extract_regexp"],
        "extract_regexp",
        parse_pipe_extract_regexp
    );
    disp!(&["eval", "math"], "math", parse_pipe_math);
    disp!(&["facets"], "facets", parse_pipe_facets);
    disp!(&["field_names"], "field_names", parse_pipe_field_names);
    disp!(&["field_values"], "field_values", parse_pipe_field_values);
    disp!(&["fields", "keep"], "fields", parse_pipe_fields);
    disp!(&["filter", "where"], "filter", parse_pipe_filter);
    disp!(&["first"], "first", parse_pipe_first);
    disp!(&["format"], "format", parse_pipe_format);
    disp!(
        &["generate_sequence"],
        "generate_sequence",
        parse_pipe_generate_sequence
    );
    disp!(&["hash"], "hash", parse_pipe_hash);
    disp!(&["join"], "join", parse_pipe_join);
    disp!(
        &["json_array_len"],
        "json_array_len",
        parse_pipe_json_array_len
    );
    disp!(&["head", "limit"], "limit", parse_pipe_limit);
    disp!(&["last"], "last", parse_pipe_last);
    disp!(&["len"], "len", parse_pipe_len);
    disp!(&["mv", "rename"], "rename", parse_pipe_rename);
    disp!(&["offset", "skip"], "offset", parse_pipe_offset);
    disp!(&["order", "sort"], "sort", parse_pipe_sort);
    disp!(&["pack_json"], "pack_json", parse_pipe_pack_json);
    disp!(&["pack_logfmt"], "pack_logfmt", parse_pipe_pack_logfmt);
    disp!(&["query_stats"], "query_stats", parse_pipe_query_stats);
    disp!(&["replace"], "replace", parse_pipe_replace);
    disp!(
        &["replace_regexp"],
        "replace_regexp",
        parse_pipe_replace_regexp
    );
    disp!(
        &["running_stats"],
        "running_stats",
        parse_pipe_running_stats
    );
    disp!(&["sample"], "sample", parse_pipe_sample);
    disp!(
        &["set_stream_fields"],
        "set_stream_fields",
        parse_pipe_set_stream_fields
    );
    disp!(&["split"], "split", parse_pipe_split);
    disp!(&["stats", "stats_remote"], "stats", parse_pipe_stats);
    disp!(
        &["stream_context"],
        "stream_context",
        parse_pipe_stream_context
    );
    disp!(&["time_add"], "time_add", parse_pipe_time_add);
    disp!(&["top"], "top", parse_pipe_top);
    disp!(&["total_stats"], "total_stats", parse_pipe_total_stats);
    disp!(&["union"], "union", parse_pipe_union);
    disp!(&["uniq"], "uniq", parse_pipe_uniq);
    disp!(&["unpack_json"], "unpack_json", parse_pipe_unpack_json);
    disp!(
        &["unpack_logfmt"],
        "unpack_logfmt",
        parse_pipe_unpack_logfmt
    );
    disp!(
        &["unpack_syslog"],
        "unpack_syslog",
        parse_pipe_unpack_syslog
    );
    disp!(&["unpack_words"], "unpack_words", parse_pipe_unpack_words);
    disp!(&["unroll"], "unroll", parse_pipe_unroll);

    if is_likely_stats_pipe(lex) {
        return parse_pipe_stats_no_stats_keyword(lex)
            .map_err(|e| format!("cannot parse 'stats' pipe: {e}"));
    }
    if is_likely_filter_pipe(lex) {
        return parse_pipe_filter_no_filter_keyword(lex)
            .map_err(|e| format!("cannot parse 'filter' pipe: {e}"));
    }
    Err(format!("unexpected pipe {}", go_quote(&lex.token)))
}

fn is_likely_stats_pipe(lex: &Lexer) -> bool {
    is_stats_func_name(lex.raw_token()) || lex.is_keyword(&["by", "("])
}

fn is_likely_filter_pipe(lex: &mut Lexer) -> bool {
    if lex.is_quoted_token() {
        return true;
    }
    if lex.is_keyword(&["*", "-", "~"]) {
        return true;
    }
    let lex_state = lex.clone();
    let ok = lex.next_compound_token_ext(&[":"]).is_ok() && lex.is_keyword(&[":"]);
    *lex = lex_state;
    ok
}

// ---- if() helpers ----

fn parse_optional_if(lex: &mut Lexer) -> Result<Option<IfFilter>, String> {
    if lex.is_keyword(&["if"]) {
        Ok(Some(parse_if_filter(lex)?))
    } else {
        Ok(None)
    }
}

fn to_arc_update_iff(iff: Option<IfFilter>) -> Option<Arc<crate::pipe_update::IfFilter>> {
    iff.map(|x| Arc::new(crate::pipe_update::IfFilter::new(x.f.clone())))
}

fn to_unpack_iff(iff: Option<IfFilter>) -> Option<crate::pipe_unpack::IfFilter> {
    iff.map(|x| crate::pipe_unpack::new_if_filter(x.f.clone()))
}

// ---------------------------------------------------------------------------
// pipe parsers
// ---------------------------------------------------------------------------

fn parse_pipe_block_stats(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["block_stats"], "block_stats")?;
    lex.next_token();
    Ok(Box::new(crate::pipe_block_stats::new_pipe_block_stats()))
}

fn parse_pipe_blocks_count(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["blocks_count"], "blocks_count")?;
    lex.next_token();
    let mut result_name = "blocks_count".to_string();
    if lex.is_keyword(&["as"]) {
        lex.next_token();
        result_name = parse_field_name(lex)
            .map_err(|e| format!("cannot parse result name for 'blocks_count': {e}"))?;
    } else if !lex.is_query_part_trailer() {
        result_name = parse_field_name(lex)
            .map_err(|e| format!("cannot parse result name for 'blocks_count': {e}"))?;
    }
    Ok(Box::new(crate::pipe_blocks_count::new_pipe_blocks_count(
        result_name,
    )))
}

fn parse_pipe_coalesce(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["coalesce"], "coalesce")?;
    lex.next_token();
    let src =
        parse_field_filters_in_parens(lex).map_err(|e| format!("cannot parse field names: {e}"))?;
    if src.is_empty() {
        return Err("coalesce requires at least one field name".to_string());
    }
    let mut default_value = String::new();
    if lex.is_keyword(&["default"]) {
        lex.next_token();
        default_value = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse default value: {e}"))?;
    }
    let mut dst_field = "_msg".to_string();
    if lex.is_keyword(&["as"]) {
        lex.next_token();
        dst_field =
            parse_field_name(lex).map_err(|e| format!("cannot parse result field name: {e}"))?;
    }
    Ok(Box::new(crate::pipe_coalesce::new_pipe_coalesce(
        src,
        dst_field,
        default_value,
    )))
}

fn parse_pipe_collapse_nums(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["collapse_nums"], "collapse_nums")?;
    lex.next_token();
    let iff = parse_optional_if(lex)?;
    let mut field = "_msg".to_string();
    if lex.is_keyword(&["at"]) {
        lex.next_token();
        field = parse_field_name(lex)
            .map_err(|e| format!("cannot parse 'at' field after 'collapse_nums': {e}"))?;
    }
    let mut is_prettify = false;
    if lex.is_keyword(&["prettify"]) {
        lex.next_token();
        is_prettify = true;
    }
    Ok(Box::new(crate::pipe_collapse_nums::new_pipe_collapse_nums(
        field,
        is_prettify,
        to_arc_update_iff(iff),
    )))
}

fn parse_pipe_copy(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["copy", "cp"], "copy")?;
    let (src, dst) = parse_src_dst_pairs(lex, "', '|', ';' or ')'")?;
    Ok(Box::new(crate::pipe_copy::new_pipe_copy(src, dst)))
}

fn parse_pipe_rename(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["rename", "mv"], "rename")?;
    let (src, dst) = parse_src_dst_pairs(lex, "', '|' or ')'")?;
    Ok(Box::new(crate::pipe_rename::new_pipe_rename(src, dst)))
}

fn parse_src_dst_pairs(lex: &mut Lexer, _tail: &str) -> Result<(Vec<String>, Vec<String>), String> {
    let mut src = Vec::new();
    let mut dst = Vec::new();
    loop {
        lex.next_token();
        let s = parse_field_filter(lex).map_err(|e| format!("cannot parse src field name: {e}"))?;
        if lex.is_keyword(&["as"]) {
            lex.next_token();
        }
        let d = parse_field_filter(lex).map_err(|e| format!("cannot parse dst field name: {e}"))?;
        src.push(s);
        dst.push(d);
        if lex.is_query_part_trailer() {
            return Ok((src, dst));
        }
        if !lex.is_keyword(&[","]) {
            return Err(format!(
                "unexpected token: {}; expecting ',', '|', ';' or ')'",
                go_quote(&lex.token)
            ));
        }
    }
}

fn parse_pipe_decolorize(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["decolorize"], "decolorize")?;
    lex.next_token();
    let mut field = "_msg".to_string();
    if !lex.is_query_part_trailer() {
        field = parse_field_name(lex)
            .map_err(|e| format!("cannot parse field name after 'decolorize': {e}"))?;
    }
    Ok(Box::new(crate::pipe_decolorize::new_pipe_decolorize(field)))
}

fn parse_pipe_delete(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["delete", "del", "rm", "drop"], "delete")?;
    lex.next_token();
    let fields = parse_comma_separated_fields(lex)?;
    Ok(Box::new(crate::pipe_delete::new_pipe_delete(fields)))
}

fn parse_pipe_drop_empty_fields(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["drop_empty_fields"], "drop_empty_fields")?;
    lex.next_token();
    Ok(Box::new(
        crate::pipe_drop_empty_fields::new_pipe_drop_empty_fields(),
    ))
}

fn parse_pipe_extract(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["extract"], "extract")?;
    lex.next_token();
    let iff = parse_optional_if(lex)?;
    let pattern_str = lex
        .next_compound_token()
        .map_err(|e| format!("cannot read 'pattern': {e}"))?;
    let mut from_field = "_msg".to_string();
    if lex.is_keyword(&["from"]) {
        lex.next_token();
        from_field =
            parse_field_name(lex).map_err(|e| format!("cannot parse 'from' field name: {e}"))?;
    }
    let (keep_original_fields, skip_empty_results) = parse_keep_skip(lex);
    let pe = crate::pipe_extract::new_pipe_extract(
        &pattern_str,
        from_field,
        keep_original_fields,
        skip_empty_results,
        to_unpack_iff(iff),
    )?;
    Ok(Box::new(pe))
}

fn parse_pipe_extract_regexp(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["extract_regexp"], "extract_regexp")?;
    lex.next_token();
    let iff = parse_optional_if(lex)?;
    let pattern_str = lex
        .next_compound_token()
        .map_err(|e| format!("cannot read 'pattern': {e}"))?;
    let mut from_field = "_msg".to_string();
    if lex.is_keyword(&["from"]) {
        lex.next_token();
        from_field =
            parse_field_name(lex).map_err(|e| format!("cannot parse 'from' field name: {e}"))?;
    }
    let (keep_original_fields, skip_empty_results) = parse_keep_skip(lex);
    let pe = crate::pipe_extract_regexp::new_pipe_extract_regexp(
        &pattern_str,
        from_field,
        keep_original_fields,
        skip_empty_results,
        to_unpack_iff(iff),
    )?;
    Ok(Box::new(pe))
}

fn parse_keep_skip(lex: &mut Lexer) -> (bool, bool) {
    if lex.is_keyword(&["keep_original_fields"]) {
        lex.next_token();
        (true, false)
    } else if lex.is_keyword(&["skip_empty_results"]) {
        lex.next_token();
        (false, true)
    } else {
        (false, false)
    }
}

fn parse_pipe_facets(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["facets"], "facets")?;
    lex.next_token();
    let mut limit = FACETS_DEFAULT_LIMIT;
    if crate::parser::is_number_prefix(&lex.token) {
        let (n, s) = parse_number(lex).map_err(|e| format!("cannot parse N in 'facets': {e}"))?;
        if n < 1.0 {
            return Err(format!(
                "value N in 'facets {s}' must be integer bigger than 0"
            ));
        }
        limit = n as u64;
    }
    let mut max_values_per_field = FACETS_DEFAULT_MAX_VALUES_PER_FIELD;
    let mut max_value_len = FACETS_DEFAULT_MAX_VALUE_LEN;
    let mut keep_const_fields = false;
    loop {
        if lex.is_keyword(&["max_values_per_field"]) {
            lex.next_token();
            let (n, s) =
                parse_number(lex).map_err(|e| format!("cannot parse max_values_per_field: {e}"))?;
            if n < 1.0 {
                return Err(format!(
                    "max_value_per_field must be integer bigger than 0; got {s}"
                ));
            }
            max_values_per_field = n as u64;
        } else if lex.is_keyword(&["max_value_len"]) {
            lex.next_token();
            let (n, s) =
                parse_number(lex).map_err(|e| format!("cannot parse max_value_len: {e}"))?;
            if n < 1.0 {
                return Err(format!(
                    "max_value_len must be integer bigger than 0; got {s}"
                ));
            }
            max_value_len = n as u64;
        } else if lex.is_keyword(&["keep_const_fields"]) {
            lex.next_token();
            keep_const_fields = true;
        } else {
            return Ok(Box::new(crate::pipe_facets::new_pipe_facets(
                limit,
                max_values_per_field,
                max_value_len,
                keep_const_fields,
            )));
        }
    }
}

fn parse_pipe_field_names(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["field_names"], "field_names")?;
    lex.next_token();
    let mut filter = String::new();
    if lex.is_keyword(&["filter"]) {
        lex.next_token();
        filter = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse filter inside 'field_names' pipe: {e}"))?;
    }
    let mut result_name = "name".to_string();
    if lex.is_keyword(&["as"]) {
        lex.next_token();
        result_name = parse_field_name(lex)
            .map_err(|e| format!("cannot parse result name for 'field_names': {e}"))?;
    } else if !lex.is_query_part_trailer() {
        result_name = parse_field_name(lex)
            .map_err(|e| format!("cannot parse result name for 'field_names': {e}"))?;
    }
    Ok(Box::new(crate::pipe_field_names::new_pipe_field_names(
        result_name,
        filter,
    )))
}

fn parse_pipe_field_values(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["field_values"], "field_values")?;
    lex.next_token();
    let field = parse_field_name_with_optional_parens(lex)
        .map_err(|e| format!("cannot parse field name for 'field_values': {e}"))?;
    let mut filter = String::new();
    if lex.is_keyword(&["filter"]) {
        lex.next_token();
        filter = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse filter for 'field_values': {e}"))?;
    }
    let mut limit = 0;
    if lex.is_keyword(&["limit"]) {
        limit = parse_limit(lex)?;
    }
    Ok(Box::new(crate::pipe_field_values::new_pipe_field_values(
        field, filter, limit,
    )))
}

fn parse_pipe_fields(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["fields", "keep"], "fields")?;
    lex.next_token();
    let fields = parse_comma_separated_fields(lex)?;
    Ok(Box::new(crate::pipe_fields::new_pipe_fields(fields)))
}

fn parse_pipe_filter(lex: &mut Lexer) -> Result<BoxPipe, String> {
    parse_pipe_filter_ext(lex, true)
}

pub(crate) fn parse_pipe_filter_no_filter_keyword(lex: &mut Lexer) -> Result<BoxPipe, String> {
    parse_pipe_filter_ext(lex, false)
}

fn parse_pipe_filter_ext(lex: &mut Lexer, need_keyword: bool) -> Result<BoxPipe, String> {
    if need_keyword {
        if !lex.is_keyword(&["filter", "where"]) {
            return Err(format!(
                "expecting 'filter' or 'where'; got {}",
                go_quote(&lex.token)
            ));
        }
        lex.next_token();
    }
    let f = parse_filter(lex, need_keyword).map_err(|e| format!("cannot parse 'filter': {e}"))?;
    Ok(Box::new(crate::pipe_filter::new_pipe_filter(Arc::from(f))))
}

fn parse_pipe_format(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["format"], "format")?;
    lex.next_token();
    let iff = parse_optional_if(lex)?;
    let format_str = lex
        .next_compound_token()
        .map_err(|e| format!("cannot read 'format': {e}"))?;
    let mut result_field = "_msg".to_string();
    if lex.is_keyword(&["as"]) {
        lex.next_token();
        result_field = parse_field_name(lex).map_err(|e| {
            format!("cannot parse result field after 'format {format_str:?} as': {e}")
        })?;
    }
    let (keep_original_fields, skip_empty_results) = parse_keep_skip(lex);
    let pf = crate::pipe_format::PipeFormat::new(
        format_str,
        result_field,
        keep_original_fields,
        skip_empty_results,
        to_arc_update_iff(iff),
    )?;
    Ok(Box::new(pf))
}

fn parse_pipe_generate_sequence(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["generate_sequence"], "generate_sequence")?;
    lex.next_token();
    if !crate::parser::is_number_prefix(&lex.token) {
        return Err(format!(
            "expecting the number of items to generate in 'generate_sequence' pipe; got {}",
            go_quote(&lex.token)
        ));
    }
    let (n, s) =
        parse_number(lex).map_err(|e| format!("cannot parse N in 'generate_sequence': {e}"))?;
    if n < 1.0 {
        return Err(format!(
            "value N in 'generate_sequence {s}' must be integer bigger than 0"
        ));
    }
    Ok(Box::new(
        crate::pipe_generate_sequence::new_pipe_generate_sequence(n as u64),
    ))
}

fn parse_pipe_hash(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["hash"], "hash")?;
    lex.next_token();
    let (field_name, result_field) = parse_field_optparen_as_result(lex, "hash")?;
    Ok(Box::new(crate::pipe_hash::new_pipe_hash(
        field_name,
        result_field,
    )))
}

fn parse_pipe_len(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["len"], "len")?;
    lex.next_token();
    let (field_name, result_field) = parse_field_optparen_as_result(lex, "len")?;
    Ok(Box::new(crate::pipe_len::new_pipe_len(
        field_name,
        result_field,
    )))
}

fn parse_pipe_json_array_len(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["json_array_len"], "json_array_len")?;
    lex.next_token();
    let (field_name, result_field) = parse_field_optparen_as_result(lex, "len")?;
    Ok(Box::new(
        crate::pipe_json_array_len::new_pipe_json_array_len(field_name, result_field),
    ))
}

/// Shared body for `len`/`hash`/`json_array_len`: field(optional parens),
/// optional `as`, optional result field defaulting to `_msg`.
fn parse_field_optparen_as_result(lex: &mut Lexer, func: &str) -> Result<(String, String), String> {
    let field_name = parse_field_name_with_optional_parens(lex)
        .map_err(|e| format!("cannot parse field name for '{func}' pipe: {e}"))?;
    let mut result_field = "_msg".to_string();
    if lex.is_keyword(&["as"]) {
        lex.next_token();
    }
    if !lex.is_query_part_trailer() {
        result_field = parse_field_name(lex).map_err(|e| {
            format!(
                "cannot parse result field after '{func}({})': {e}",
                quote_token_if_needed(&field_name)
            )
        })?;
    }
    Ok((field_name, result_field))
}

fn parse_pipe_limit(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["limit", "head"], "limit")?;
    lex.next_token();
    let mut limit = 10u64;
    if !lex.is_query_part_trailer() {
        let limit_str = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse rows limit: {e}"))?;
        limit = parse_uint(&limit_str)
            .map_err(|e| format!("cannot parse rows limit from {}: {e}", go_quote(&limit_str)))?;
    }
    Ok(Box::new(crate::pipe_limit::new_pipe_limit(limit)))
}

fn parse_pipe_offset(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["offset", "skip"], "offset")?;
    let op = lex.token.clone();
    lex.next_token();
    let token = lex
        .next_compound_token()
        .map_err(|e| format!("cannot parse '{op}': {e}"))?;
    let n = parse_uint(&token).map_err(|e| format!("cannot parse '{op} {token}': {e}"))?;
    Ok(Box::new(crate::pipe_offset::new_pipe_offset(n)))
}

fn parse_pipe_pack_json(lex: &mut Lexer) -> Result<BoxPipe, String> {
    let (fields, result_field) = parse_pack_common(lex, "pack_json")?;
    Ok(Box::new(crate::pipe_pack_json::new_pipe_pack_json(
        fields,
        result_field,
    )))
}

fn parse_pipe_pack_logfmt(lex: &mut Lexer) -> Result<BoxPipe, String> {
    let (fields, result_field) = parse_pack_common(lex, "pack_logfmt")?;
    Ok(Box::new(crate::pipe_pack_logfmt::new_pipe_pack_logfmt(
        fields,
        result_field,
    )))
}

fn parse_pack_common(lex: &mut Lexer, name: &str) -> Result<(Vec<String>, String), String> {
    expect_keyword(lex, &[name], name)?;
    lex.next_token();
    let mut field_filters = Vec::new();
    if lex.is_keyword(&["fields"]) {
        lex.next_token();
        field_filters =
            parse_field_filters_in_parens(lex).map_err(|e| format!("cannot parse fields: {e}"))?;
    }
    let mut result_field = "_msg".to_string();
    if lex.is_keyword(&["as"]) {
        lex.next_token();
    }
    if !lex.is_query_part_trailer() {
        result_field = parse_field_name(lex)
            .map_err(|e| format!("cannot parse result field for '{name}': {e}"))?;
    }
    Ok((field_filters, result_field))
}

fn parse_pipe_query_stats(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["query_stats"], "query_stats")?;
    lex.next_token();
    Ok(Box::new(crate::pipe_query_stats::new_pipe_query_stats()))
}

fn parse_pipe_replace(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["replace"], "replace")?;
    lex.next_token();
    let iff = parse_optional_if(lex)?;
    if !lex.is_keyword(&["("]) {
        return Err("missing '(' after 'replace'".to_string());
    }
    lex.next_token();
    let old_substr = lex
        .next_compound_token()
        .map_err(|e| format!("cannot parse oldSubstr in 'replace': {e}"))?;
    if !lex.is_keyword(&[","]) {
        return Err(format!(
            "missing ',' after 'replace({}'",
            go_quote(&old_substr)
        ));
    }
    lex.next_token();
    let new_substr = lex.next_compound_token().map_err(|e| {
        format!(
            "cannot parse newSubstr in 'replace({}': {e}",
            go_quote(&old_substr)
        )
    })?;
    if !lex.is_keyword(&[")"]) {
        return Err(format!(
            "missing ')' after 'replace({}, {}'",
            go_quote(&old_substr),
            go_quote(&new_substr)
        ));
    }
    lex.next_token();
    let mut field = "_msg".to_string();
    if lex.is_keyword(&["at"]) {
        lex.next_token();
        field = parse_field_name(lex).map_err(|e| format!("cannot parse 'at' field: {e}"))?;
    }
    let mut limit = 0;
    if lex.is_keyword(&["limit"]) {
        limit = parse_limit(lex)?;
    }
    Ok(Box::new(crate::pipe_replace::PipeReplace::new(
        field,
        old_substr,
        new_substr,
        limit,
        to_arc_update_iff(iff),
    )))
}

fn parse_pipe_replace_regexp(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["replace_regexp"], "replace_regexp")?;
    lex.next_token();
    let iff = parse_optional_if(lex)?;
    if !lex.is_keyword(&["("]) {
        return Err("missing '(' after 'replace_regexp'".to_string());
    }
    lex.next_token();
    let re_str = lex
        .next_compound_token()
        .map_err(|e| format!("cannot parse reStr in 'replace_regexp': {e}"))?;
    let re = crate::pipe_replace_regexp::regexp_compile(&re_str).map_err(|e| {
        format!(
            "cannot parse regexp {} in 'replace_regexp': {e}",
            go_quote(&re_str)
        )
    })?;
    if !lex.is_keyword(&[","]) {
        return Err(format!(
            "missing ',' after 'replace_regexp({}'",
            go_quote(&re_str)
        ));
    }
    lex.next_token();
    let replacement = lex.next_compound_token().map_err(|e| {
        format!(
            "cannot parse replacement in 'replace_regexp({}': {e}",
            go_quote(&re_str)
        )
    })?;
    if !lex.is_keyword(&[")"]) {
        return Err(format!(
            "missing ')' after 'replace_regexp({}, {}'",
            go_quote(&re_str),
            go_quote(&replacement)
        ));
    }
    lex.next_token();
    let mut field = "_msg".to_string();
    if lex.is_keyword(&["at"]) {
        lex.next_token();
        field = parse_field_name(lex).map_err(|e| format!("cannot parse 'at' field: {e}"))?;
    }
    let mut limit = 0;
    if lex.is_keyword(&["limit"]) {
        limit = parse_limit(lex)?;
    }
    Ok(Box::new(
        crate::pipe_replace_regexp::PipeReplaceRegexp::new(
            field,
            re,
            re_str,
            replacement,
            limit,
            to_arc_update_iff(iff),
        ),
    ))
}

fn parse_pipe_sample(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["sample"], "sample")?;
    lex.next_token();
    let sample_str = lex
        .next_compound_token()
        .map_err(|e| format!("cannot read 'sample': {e}"))?;
    let sample = parse_uint(&sample_str)
        .map_err(|e| format!("cannot parse sample from {}: {e}", go_quote(&sample_str)))?;
    if sample == 0 {
        return Err(format!(
            "unexpected sample={sample}; it must be bigger than 0"
        ));
    }
    Ok(Box::new(crate::pipe_sample::PipeSample::new(sample)))
}

fn parse_pipe_set_stream_fields(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["set_stream_fields"], "set_stream_fields")?;
    lex.next_token();
    let iff = parse_optional_if(lex)?;
    let stream_field_filters = parse_comma_separated_fields(lex)?;
    Ok(Box::new(
        crate::pipe_set_stream_fields::new_pipe_set_stream_fields(
            stream_field_filters,
            to_arc_update_iff(iff),
        ),
    ))
}

fn parse_pipe_split(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["split"], "split")?;
    lex.next_token();
    if lex.is_keyword(&["as", "from"]) {
        return Err(format!(
            "missing split separator in front of {}",
            go_quote(&lex.token)
        ));
    }
    let separator = lex
        .next_compound_token()
        .map_err(|e| format!("cannot read split separator: {e}"))?;
    let mut src_field = "_msg".to_string();
    if !lex.is_keyword(&["as"]) && !lex.is_query_part_trailer() {
        if lex.is_keyword(&["from"]) {
            lex.next_token();
        }
        src_field =
            parse_field_name(lex).map_err(|e| format!("cannot parse srcField name: {e}"))?;
    }
    let mut dst_field = src_field.clone();
    if !lex.is_query_part_trailer() {
        if lex.is_keyword(&["as"]) {
            lex.next_token();
        }
        dst_field =
            parse_field_name(lex).map_err(|e| format!("cannot parse dstField name: {e}"))?;
    }
    Ok(Box::new(crate::pipe_split::PipeSplit::new(
        separator, src_field, dst_field,
    )))
}

fn parse_pipe_stream_context(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["stream_context"], "stream_context")?;
    lex.next_token();
    let (lines_before, lines_after) = parse_stream_context_before_after(lex)?;
    let mut time_window = STREAM_CONTEXT_DEFAULT_TIME_WINDOW;
    if lex.is_keyword(&["time_window"]) {
        lex.next_token();
        let token = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse 'time_window': {e}"))?;
        let d = crate::values_encoder::try_parse_duration(&token).ok_or_else(|| {
            format!("cannot parse 'time_window {token}'; it must contain valid duration")
        })?;
        time_window = d;
    }
    Ok(Box::new(
        crate::pipe_stream_context::new_pipe_stream_context(lines_before, lines_after, time_window),
    ))
}

fn parse_stream_context_before_after(lex: &mut Lexer) -> Result<(usize, usize), String> {
    let mut before = 0i64;
    let mut after = 0i64;
    let mut before_set = false;
    let mut after_set = false;
    loop {
        if lex.is_keyword(&["before"]) {
            lex.next_token();
            let (f, s) = parse_number(lex)
                .map_err(|e| format!("cannot parse 'before' value in 'stream_context': {e}"))?;
            if f < 0.0 {
                return Err(format!(
                    "'before' value cannot be smaller than 0; got {}",
                    go_quote(&s)
                ));
            }
            before = f as i64;
            before_set = true;
        } else if lex.is_keyword(&["after"]) {
            lex.next_token();
            let (f, s) = parse_number(lex)
                .map_err(|e| format!("cannot parse 'after' value in 'stream_context': {e}"))?;
            if f < 0.0 {
                return Err(format!(
                    "'after' value cannot be smaller than 0; got {}",
                    go_quote(&s)
                ));
            }
            after = f as i64;
            after_set = true;
        } else {
            if !before_set && !after_set {
                return Err("missing 'before N' or 'after N' in 'stream_context'".to_string());
            }
            return Ok((before as usize, after as usize));
        }
    }
}

fn parse_pipe_time_add(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["time_add"], "time_add")?;
    lex.next_token();
    let (offset, offset_str) =
        parse_duration(lex).map_err(|e| format!("cannot parse offset: {e}"))?;
    let mut field = "_time".to_string();
    if lex.is_keyword(&["at"]) {
        lex.next_token();
        field = parse_field_name(lex).map_err(|e| format!("cannot read field name: {e}"))?;
    }
    Ok(Box::new(crate::pipe_time_add::new_pipe_time_add(
        field, -offset, offset_str,
    )))
}

fn parse_pipe_top(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["top"], "top")?;
    lex.next_token();
    let mut limit = TOP_DEFAULT_LIMIT;
    let mut limit_str = String::new();
    if crate::parser::is_number_prefix(&lex.token) {
        let (n, s) = parse_number(lex).map_err(|e| format!("cannot parse N in 'top': {e}"))?;
        if n < 1.0 {
            return Err(format!(
                "value N in 'top {s}' must be integer bigger than 0"
            ));
        }
        limit = n as u64;
        limit_str = s;
    }
    let mut need_fields = false;
    if lex.is_keyword(&["by"]) {
        lex.next_token();
        need_fields = true;
    }
    let mut by_fields = Vec::new();
    if lex.is_keyword(&["("]) {
        by_fields =
            parse_field_names_in_parens(lex).map_err(|e| format!("cannot parse 'by(...)': {e}"))?;
    } else if !lex.is_keyword(&["hits", "rank"]) && !lex.is_query_part_trailer() {
        by_fields =
            parse_comma_separated_fields(lex).map_err(|e| format!("cannot parse 'by ...': {e}"))?;
    } else if need_fields {
        return Err("missing fields after 'by'".to_string());
    }
    if by_fields.is_empty() {
        return Err("expecting at least a single field in 'by(...)'".to_string());
    }
    let mut hits_field_name = "hits".to_string();
    let mut rank_field_name = String::new();
    loop {
        if lex.is_keyword(&["hits"]) {
            lex.next_token();
            if lex.is_keyword(&["as"]) {
                lex.next_token();
            }
            hits_field_name = lex
                .next_compound_token()
                .map_err(|e| format!("cannot parse 'hits' name: {e}"))?;
        } else if lex.is_keyword(&["rank"]) {
            let r = parse_rank_field_name(lex)
                .map_err(|e| format!("cannot parse rank field name: {e}"))?;
            rank_field_name = get_unique_result_name(&r, &by_fields);
        } else {
            hits_field_name = get_unique_result_name(&hits_field_name, &by_fields);
            return Ok(Box::new(crate::pipe_top::new_pipe_top(
                by_fields,
                limit,
                limit_str,
                hits_field_name,
                rank_field_name,
            )));
        }
    }
}

fn parse_pipe_uniq(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["uniq"], "uniq")?;
    lex.next_token();
    let mut need_fields = false;
    if lex.is_keyword(&["by"]) {
        lex.next_token();
        need_fields = true;
    }
    let mut by_fields = Vec::new();
    if lex.is_keyword(&["("]) {
        by_fields =
            parse_field_names_in_parens(lex).map_err(|e| format!("cannot parse 'by(...)': {e}"))?;
    } else if !lex.is_keyword(&["filter", "with", "hits", "limit"]) && !lex.is_query_part_trailer()
    {
        by_fields =
            parse_comma_separated_fields(lex).map_err(|e| format!("cannot parse 'by ...': {e}"))?;
    } else if need_fields {
        return Err("missing fields after 'by'".to_string());
    }
    if by_fields.is_empty() {
        return Err("missing fields inside 'by(...)'".to_string());
    }
    let mut filter = String::new();
    if lex.is_keyword(&["filter"]) {
        lex.next_token();
        filter = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse filter inside 'uniq' pipe: {e}"))?;
        if by_fields.len() != 1 && !filter.is_empty() {
            return Err(format!(
                "the 'filter {}' inside 'uniq' pipe cannot be applied to multiple fields {:?}",
                quote_token_if_needed(&filter),
                by_fields
            ));
        }
    }
    if lex.is_keyword(&["with"]) {
        lex.next_token();
        if !lex.is_keyword(&["hits"]) {
            return Err("missing 'hits' after 'with'".to_string());
        }
    }
    let mut hits_field_name = String::new();
    if lex.is_keyword(&["hits"]) {
        lex.next_token();
        hits_field_name = get_unique_result_name("hits", &by_fields);
    }
    let mut limit = 0;
    if lex.is_keyword(&["limit"]) {
        limit = parse_limit(lex)?;
    }
    Ok(Box::new(crate::pipe_uniq::new_pipe_uniq(
        by_fields,
        filter,
        hits_field_name,
        limit,
    )))
}

// ---- sort / first / last ----

fn parse_pipe_sort(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["sort", "order"], "sort")?;
    lex.next_token();
    let mut by_fields = Vec::new();
    if lex.is_keyword(&["by", "("]) {
        if lex.is_keyword(&["by"]) {
            lex.next_token();
        }
        by_fields = parse_sort_fields(lex)?;
    }
    let mut is_desc = false;
    if lex.is_keyword(&["desc"]) {
        lex.next_token();
        is_desc = true;
    } else if lex.is_keyword(&["asc"]) {
        lex.next_token();
    }
    let mut offset = 0u64;
    let mut limit = 0u64;
    let mut rank_field_name = String::new();
    let mut partition_by_fields: Vec<String> = Vec::new();
    loop {
        if lex.is_keyword(&["offset"]) {
            let n = parse_offset(lex)?;
            if offset > 0 {
                return Err(format!(
                    "duplicate 'offset'; the previous one is {offset}; the new one is {n}"
                ));
            }
            offset = n;
        } else if lex.is_keyword(&["limit"]) {
            let n = parse_limit(lex)?;
            if limit > 0 {
                return Err(format!(
                    "duplicate 'limit'; the previous one is {limit}; the new one is {n}"
                ));
            }
            limit = n;
        } else if lex.is_keyword(&["rank"]) {
            rank_field_name = parse_rank_field_name(lex)
                .map_err(|e| format!("cannot read rank field name: {e}"))?;
        } else if lex.is_keyword(&["partition"]) {
            if !partition_by_fields.is_empty() {
                return Err("duplicate 'partition by'".to_string());
            }
            lex.next_token();
            if lex.is_keyword(&["by"]) {
                lex.next_token();
            }
            partition_by_fields = parse_field_names_in_parens(lex)
                .map_err(|e| format!("cannot parse 'partition by' args: {e}"))?;
        } else {
            if !partition_by_fields.is_empty() && limit == 0 {
                return Err("missing 'limit' for 'partition by'".to_string());
            }
            return Ok(Box::new(PipeSort::new(
                by_fields,
                is_desc,
                offset,
                limit,
                rank_field_name,
                partition_by_fields,
            )));
        }
    }
}

fn parse_sort_fields(lex: &mut Lexer) -> Result<Vec<BySortField>, String> {
    let raw =
        parse_by_sort_fields_raw(lex).map_err(|e| format!("cannot parse 'by' clause: {e}"))?;
    Ok(raw
        .into_iter()
        .map(|(n, d)| BySortField::new(n, d))
        .collect())
}

fn parse_pipe_first(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["first"], "first")?;
    lex.next_token();
    let ps = parse_pipe_last_first(lex)?;
    Ok(Box::new(crate::pipe_first::new_pipe_first(ps)))
}

fn parse_pipe_last(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["last"], "last")?;
    lex.next_token();
    let ps = parse_pipe_last_first(lex)?;
    Ok(Box::new(crate::pipe_last::new_pipe_last(ps)))
}

fn parse_pipe_last_first(lex: &mut Lexer) -> Result<PipeSort, String> {
    let mut limit = 1u64;
    if !lex.is_keyword(&["by", "partition", "rank", "("]) && !lex.is_query_part_trailer() {
        let s = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse number: {e}"))?;
        let n = try_parse_uint64(&s)
            .ok_or_else(|| format!("expecting number; got {}", go_quote(&s)))?;
        if n < 1 {
            return Err(format!("the number must be bigger than 0; got {n}"));
        }
        limit = n;
    }
    let mut by_fields = Vec::new();
    if lex.is_keyword(&["by", "("]) {
        if lex.is_keyword(&["by"]) {
            lex.next_token();
        }
        by_fields = parse_sort_fields(lex)?;
    }
    let mut partition_by_fields = Vec::new();
    if lex.is_keyword(&["partition"]) {
        lex.next_token();
        if lex.is_keyword(&["by"]) {
            lex.next_token();
        }
        partition_by_fields = parse_field_names_in_parens(lex)
            .map_err(|e| format!("cannot parse 'partition by' args: {e}"))?;
    }
    let mut rank_field_name = String::new();
    if lex.is_keyword(&["rank"]) {
        rank_field_name =
            parse_rank_field_name(lex).map_err(|e| format!("cannot read rank field name: {e}"))?;
    }
    Ok(PipeSort::new(
        by_fields,
        false,
        0,
        limit,
        rank_field_name,
        partition_by_fields,
    ))
}

// ---- join / union ----

fn parse_pipe_join(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["join"], "join")?;
    lex.next_token();
    if lex.is_keyword(&["by", "on"]) {
        lex.next_token();
    }
    let by_fields = parse_field_names_in_parens(lex)
        .map_err(|e| format!("cannot parse 'by(...)' at 'join': {e}"))?;
    if by_fields.is_empty() {
        return Err("'by(...)' at 'join' must contain at least a single field".to_string());
    }
    if by_fields.iter().any(|f| f == "*") {
        return Err("join by '*' isn't supported".to_string());
    }
    let mut query_text = None;
    let mut rows = None;
    if lex.is_keyword(&["rows"]) {
        rows = Some(parse_rows(lex).map_err(|e| format!("cannot parse rows inside 'join': {e}"))?);
    } else {
        let mut q = parse_query_in_parens(lex)
            .map_err(|e| format!("cannot parse subquery inside 'join': {e}"))?;
        // PORT NOTE: Go keeps the parsed `*Query` and optimizes it later via
        // the top-level `optimize()`'s `visitSubqueries`; the Rust pipe stores
        // rendered text, so the subquery is optimized before rendering (same
        // as `parse_in_query`).
        q.optimize();
        query_text = Some(q.to_string());
    }
    let mut is_inner = false;
    if lex.is_keyword(&["inner"]) {
        lex.next_token();
        is_inner = true;
    }
    let mut prefix = String::new();
    if lex.is_keyword(&["prefix"]) {
        lex.next_token();
        prefix = lex
            .next_compound_token()
            .map_err(|e| format!("cannot read prefix: {e}"))?;
        if !is_inner && lex.is_keyword(&["inner"]) {
            lex.next_token();
            is_inner = true;
        }
    }
    Ok(Box::new(crate::pipe_join::new_pipe_join(
        by_fields, rows, query_text, is_inner, prefix,
    )))
}

fn parse_pipe_union(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["union"], "union")?;
    lex.next_token();
    let mut query_text = None;
    let mut rows = None;
    if lex.is_keyword(&["rows"]) {
        rows = Some(parse_rows(lex).map_err(|e| format!("cannot parse rows inside 'union': {e}"))?);
    } else {
        let mut q = parse_query_in_parens(lex)
            .map_err(|e| format!("cannot parse subquery inside 'union': {e}"))?;
        // PORT NOTE: subquery optimized before rendering, mirroring Go's
        // `visitSubqueries` (see the 'join' subquery above).
        q.optimize();
        query_text = Some(q.to_string());
    }
    Ok(Box::new(crate::pipe_union::new_pipe_union(
        rows, query_text,
    )))
}

/// Port of Go `parseRows`.
fn parse_rows(lex: &mut Lexer) -> Result<Vec<Vec<Field>>, String> {
    if !lex.is_keyword(&["rows"]) {
        return Err("missing 'rows' prefix".to_string());
    }
    lex.next_token();
    if !lex.is_keyword(&["("]) {
        return Err("missing '(' after 'rows' prefix".to_string());
    }
    lex.next_token();
    let mut rows = Vec::new();
    while !lex.is_keyword(&[")"]) {
        let row = parse_row(lex)?;
        rows.push(row);
        if lex.is_keyword(&[","]) {
            lex.next_token();
        }
    }
    lex.next_token();
    Ok(rows)
}

/// Port of Go `parseRow`.
fn parse_row(lex: &mut Lexer) -> Result<Vec<Field>, String> {
    if !lex.is_keyword(&["{"]) {
        return Err(format!(
            "missing '{{'; got {} instead",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    let mut fields = Vec::new();
    while !lex.is_keyword(&["}"]) {
        let name = lex.token.clone();
        lex.next_token();
        if !lex.is_keyword(&[":", "="]) {
            return Err(format!(
                "missing ':' or '=' after {}; got [{}] instead",
                go_quote(&name),
                lex.token
            ));
        }
        lex.next_token();
        let value = lex
            .next_compound_token()
            .map_err(|e| format!("cannot read value after {}: {e}", go_quote(&name)))?;
        fields.push(Field {
            name: name.into_bytes(),
            value: value.into_bytes(),
        });
        if lex.is_keyword(&["}"]) {
            break;
        }
        if lex.is_keyword(&[","]) {
            lex.next_token();
        }
    }
    lex.next_token();
    Ok(fields)
}

// ---- unpack ----

fn parse_pipe_unpack_json(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["unpack_json"], "unpack_json")?;
    lex.next_token();
    let iff = parse_optional_if(lex)?;
    let mut from_field = "_msg".to_string();
    if !lex.is_keyword(&[
        "fields",
        "preserve_keys",
        "result_prefix",
        "keep_original_fields",
        "skip_empty_results",
    ]) && !lex.is_query_part_trailer()
    {
        if lex.is_keyword(&["from"]) {
            lex.next_token();
        }
        from_field =
            parse_field_name(lex).map_err(|e| format!("cannot parse 'from' field name: {e}"))?;
    }
    let mut field_filters = Vec::new();
    if lex.is_keyword(&["fields"]) {
        lex.next_token();
        field_filters = parse_field_filters_in_parens(lex)
            .map_err(|e| format!("cannot parse 'fields': {e}"))?;
    }
    let mut preserve_keys = Vec::new();
    if lex.is_keyword(&["preserve_keys"]) {
        lex.next_token();
        preserve_keys = parse_field_names_in_parens(lex)
            .map_err(|e| format!("cannot parse 'preserve_keys': {e}"))?;
    }
    let mut result_prefix = String::new();
    if lex.is_keyword(&["result_prefix"]) {
        lex.next_token();
        result_prefix = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse 'result_prefix': {e}"))?;
    }
    let (keep_original_fields, skip_empty_results) = parse_keep_skip(lex);
    Ok(Box::new(crate::pipe_unpack_json::new_pipe_unpack_json(
        from_field,
        field_filters,
        preserve_keys,
        result_prefix,
        keep_original_fields,
        skip_empty_results,
        to_unpack_iff(iff),
    )))
}

fn parse_pipe_unpack_logfmt(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["unpack_logfmt"], "unpack_logfmt")?;
    lex.next_token();
    let iff = parse_optional_if(lex)?;
    let mut from_field = "_msg".to_string();
    if !lex.is_keyword(&[
        "fields",
        "result_prefix",
        "keep_original_fields",
        "skip_empty_results",
    ]) && !lex.is_query_part_trailer()
    {
        if lex.is_keyword(&["from"]) {
            lex.next_token();
        }
        from_field =
            parse_field_name(lex).map_err(|e| format!("cannot parse 'from' field name: {e}"))?;
    }
    let mut field_filters = Vec::new();
    if lex.is_keyword(&["fields"]) {
        lex.next_token();
        field_filters = parse_field_filters_in_parens(lex)
            .map_err(|e| format!("cannot parse 'fields': {e}"))?;
    }
    let mut result_prefix = String::new();
    if lex.is_keyword(&["result_prefix"]) {
        lex.next_token();
        result_prefix = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse 'result_prefix': {e}"))?;
    }
    let (keep_original_fields, skip_empty_results) = parse_keep_skip(lex);
    Ok(Box::new(crate::pipe_unpack_logfmt::new_pipe_unpack_logfmt(
        from_field,
        field_filters,
        result_prefix,
        keep_original_fields,
        skip_empty_results,
        to_unpack_iff(iff),
    )))
}

fn parse_pipe_unpack_syslog(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["unpack_syslog"], "unpack_syslog")?;
    lex.next_token();
    let iff = parse_optional_if(lex)?;
    let mut from_field = "_msg".to_string();
    if !lex.is_keyword(&["offset", "result_prefix", "keep_original_fields"])
        && !lex.is_query_part_trailer()
    {
        if lex.is_keyword(&["from"]) {
            lex.next_token();
        }
        from_field =
            parse_field_name(lex).map_err(|e| format!("cannot parse 'from' field name: {e}"))?;
    }
    let mut offset_str = String::new();
    let mut offset_secs = 0i64;
    if lex.is_keyword(&["offset"]) {
        lex.next_token();
        let s = lex
            .next_compound_token()
            .map_err(|e| format!("cannot read 'offset': {e}"))?;
        let nsecs = crate::values_encoder::try_parse_duration(&s)
            .ok_or_else(|| format!("cannot parse 'offset' from {}", go_quote(&s)))?;
        offset_str = s;
        offset_secs = nsecs / 1_000_000_000;
    }
    let mut result_prefix = String::new();
    if lex.is_keyword(&["result_prefix"]) {
        lex.next_token();
        result_prefix = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse 'result_prefix': {e}"))?;
    }
    let mut keep_original_fields = false;
    if lex.is_keyword(&["keep_original_fields"]) {
        lex.next_token();
        keep_original_fields = true;
    }
    Ok(Box::new(crate::pipe_unpack_syslog::new_pipe_unpack_syslog(
        from_field,
        offset_str,
        offset_secs,
        result_prefix,
        keep_original_fields,
        to_unpack_iff(iff),
    )))
}

fn parse_pipe_unpack_words(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["unpack_words"], "unpack_words")?;
    lex.next_token();
    let mut src_field = "_msg".to_string();
    if !lex.is_keyword(&["drop_duplicates", "as"]) && !lex.is_query_part_trailer() {
        if lex.is_keyword(&["from"]) {
            lex.next_token();
        }
        src_field =
            parse_field_name(lex).map_err(|e| format!("cannot parse srcField name: {e}"))?;
    }
    let mut dst_field = src_field.clone();
    if !lex.is_keyword(&["drop_duplicates"]) && !lex.is_query_part_trailer() {
        if lex.is_keyword(&["as"]) {
            lex.next_token();
        }
        dst_field =
            parse_field_name(lex).map_err(|e| format!("cannot parse dstField name: {e}"))?;
    }
    let mut drop_duplicates = false;
    if lex.is_keyword(&["drop_duplicates"]) {
        lex.next_token();
        drop_duplicates = true;
    }
    Ok(Box::new(crate::pipe_unpack_words::new_pipe_unpack_words(
        src_field,
        dst_field,
        drop_duplicates,
    )))
}

fn parse_pipe_unroll(lex: &mut Lexer) -> Result<BoxPipe, String> {
    expect_keyword(lex, &["unroll"], "unroll")?;
    lex.next_token();
    let iff = parse_optional_if(lex)?;
    if lex.is_keyword(&["by"]) {
        lex.next_token();
    }
    let fields = if lex.is_keyword(&["("]) {
        parse_field_names_in_parens(lex).map_err(|e| format!("cannot parse 'by(...)': {e}"))?
    } else {
        parse_comma_separated_fields(lex).map_err(|e| format!("cannot parse 'by ...': {e}"))?
    };
    if fields.is_empty() {
        return Err("'by(...)' at 'unroll' must contain at least a single field".to_string());
    }
    if fields.iter().any(|f| f == "*") {
        return Err("unroll by '*' isn't supported".to_string());
    }
    Ok(Box::new(crate::pipe_unroll::new_pipe_unroll(
        fields,
        to_arc_update_iff(iff),
    )))
}

// ---- math ----

/// Port of Go `parsePipeMath` (pipe_math.go).
fn parse_pipe_math(lex: &mut Lexer) -> Result<BoxPipe, String> {
    if !lex.is_keyword(&["math", "eval"]) {
        return Err(format!(
            "unexpected token: {}; want 'math' or 'eval'",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();

    let mut entries: Vec<MathEntry> = Vec::new();
    loop {
        let e = parse_math_entry(lex)?;
        entries.push(e);

        if lex.is_keyword(&[","]) {
            lex.next_token();
        } else if lex.is_query_part_trailer() {
            return Ok(Box::new(PipeMath::new(entries)));
        } else {
            return Err(format!(
                "unexpected token after 'math' expression [{}]: {}; expecting ',', '|', ';' or ')'",
                entries.last().expect("entries is non-empty"),
                go_quote(&lex.token)
            ));
        }
    }
}

/// Port of Go `parseMathEntry` (pipe_math.go).
fn parse_math_entry(lex: &mut Lexer) -> Result<MathEntry, String> {
    let me = parse_math_expr(lex)?;

    let result_field = if lex.is_keyword(&[","]) || lex.is_query_part_trailer() {
        me.to_string()
    } else {
        if lex.is_keyword(&["as"]) {
            // skip optional 'as'
            lex.next_token();
        }
        parse_field_name(lex).map_err(|e| format!("cannot parse result name for [{me}]: {e}"))?
    };

    Ok(MathEntry::new(result_field, me))
}

/// Port of Go `parseMathExpr` (pipe_math.go); operator rebalancing lives in
/// [`MathExpr::new_binary_balanced`].
fn parse_math_expr(lex: &mut Lexer) -> Result<MathExpr, String> {
    // parse left operand
    let mut left = parse_math_expr_operand(lex)?;

    loop {
        if !crate::pipe_math::is_math_binary_op(&lex.token) {
            // There is no right operand
            return Ok(left);
        }

        // parse operator
        let op = lex.token.clone();
        lex.next_token();

        // parse right operand
        let right = parse_math_expr_operand(lex)
            .map_err(|e| format!("cannot parse operand after [{left} {op}]: {e}"))?;

        // balance operands according to their priority
        left = MathExpr::new_binary_balanced(&op, left, right);
    }
}

/// Port of Go `parseMathExprInParens` (pipe_math.go).
fn parse_math_expr_in_parens(lex: &mut Lexer) -> Result<MathExpr, String> {
    if !lex.is_keyword(&["("]) {
        return Err("missing '('".to_string());
    }
    lex.next_token();

    let mut me = parse_math_expr(lex)?;
    me.mark_wrapped_in_parens();

    if !lex.is_keyword(&[")"]) {
        return Err(format!("missing ')'; got {} instead", go_quote(&lex.token)));
    }
    lex.next_token();
    Ok(me)
}

/// Port of Go `parseMathExprOperand` (pipe_math.go).
fn parse_math_expr_operand(lex: &mut Lexer) -> Result<MathExpr, String> {
    if lex.is_keyword(&["("]) {
        return parse_math_expr_in_parens(lex);
    }

    if lex.is_keyword(&["abs"]) {
        return parse_math_expr_one_arg_func(lex, "abs", "accepts only one arg");
    }
    if lex.is_keyword(&["exp"]) {
        return parse_math_expr_one_arg_func(lex, "exp", "accepts only one arg");
    }
    if lex.is_keyword(&["ln"]) {
        return parse_math_expr_one_arg_func(lex, "ln", "accepts only one arg");
    }
    if lex.is_keyword(&["max"]) {
        return parse_math_expr_min_max(lex, "max");
    }
    if lex.is_keyword(&["min"]) {
        return parse_math_expr_min_max(lex, "min");
    }
    if lex.is_keyword(&["now"]) {
        return parse_math_expr_no_args_func(lex, "now");
    }
    if lex.is_keyword(&["rand"]) {
        return parse_math_expr_no_args_func(lex, "rand");
    }
    if lex.is_keyword(&["round"]) {
        return parse_math_expr_round(lex);
    }
    if lex.is_keyword(&["ceil"]) {
        return parse_math_expr_one_arg_func(lex, "ceil", "needs one arg");
    }
    if lex.is_keyword(&["floor"]) {
        return parse_math_expr_one_arg_func(lex, "floor", "needs one arg");
    }
    if lex.is_keyword(&["-"]) {
        return parse_math_expr_unary_minus(lex);
    }
    if lex.is_keyword(&["+"]) {
        // just skip unary plus
        lex.next_token();
        return parse_math_expr_operand(lex);
    }
    if crate::pipe_math::is_number_prefix(&lex.token) {
        return parse_math_expr_const_number(lex);
    }
    parse_math_expr_field_name(lex)
}

/// Go `parseMathExprAbs`/`Exp`/`Ln`/`Ceil`/`Floor`: a generic-func parse plus
/// the exactly-one-arg check (Go duplicates the wrapper per function; only the
/// error wording differs).
fn parse_math_expr_one_arg_func(
    lex: &mut Lexer,
    func_name: &str,
    arity_msg: &str,
) -> Result<MathExpr, String> {
    let me = parse_math_expr_generic_func(lex, func_name)?;
    if me.args_len() != 1 {
        return Err(format!(
            "'{func_name}' function {arity_msg}; got {} args: [{me}]",
            me.args_len()
        ));
    }
    Ok(me)
}

/// Go `parseMathExprMax` / `parseMathExprMin`.
fn parse_math_expr_min_max(lex: &mut Lexer, func_name: &str) -> Result<MathExpr, String> {
    let me = parse_math_expr_generic_func(lex, func_name)?;
    if me.args_len() < 2 {
        return Err(format!(
            "'{func_name}' function needs at least 2 args; got {} args: [{me}]",
            me.args_len()
        ));
    }
    Ok(me)
}

/// Go `parseMathExprNow` / `parseMathExprRand`.
fn parse_math_expr_no_args_func(lex: &mut Lexer, func_name: &str) -> Result<MathExpr, String> {
    if !lex.is_keyword(&[func_name]) {
        return Err(format!("missing '{func_name}' keyword"));
    }
    lex.next_token();

    let args = parse_math_func_args(lex)
        .map_err(|e| format!("cannot parse args for '{func_name}' function: {e}"))?;
    if !args.is_empty() {
        return Err(format!(
            "'{func_name}' function must have no args; got {} args",
            args.len()
        ));
    }
    Ok(MathExpr::new_func(func_name, Vec::new()))
}

/// Go `parseMathExprRound`.
fn parse_math_expr_round(lex: &mut Lexer) -> Result<MathExpr, String> {
    let me = parse_math_expr_generic_func(lex, "round")?;
    if me.args_len() != 1 && me.args_len() != 2 {
        return Err(format!(
            "'round' function needs 1 or 2 args; got {} args: [{me}]",
            me.args_len()
        ));
    }
    Ok(me)
}

/// Port of Go `parseMathExprGenericFunc` (pipe_math.go).
fn parse_math_expr_generic_func(lex: &mut Lexer, func_name: &str) -> Result<MathExpr, String> {
    if !lex.is_keyword(&[func_name]) {
        return Err(format!("missing {} keyword", go_quote(func_name)));
    }
    lex.next_token();

    let args = parse_math_func_args(lex).map_err(|e| {
        format!(
            "cannot parse args for {} function: {e}",
            go_quote(func_name)
        )
    })?;
    if args.is_empty() {
        return Err(format!(
            "{} function needs at least one arg",
            go_quote(func_name)
        ));
    }
    Ok(MathExpr::new_func(func_name, args))
}

/// Port of Go `parseMathFuncArgs` (pipe_math.go).
fn parse_math_func_args(lex: &mut Lexer) -> Result<Vec<MathExpr>, String> {
    if !lex.is_keyword(&["("]) {
        return Err("missing '('".to_string());
    }
    lex.next_token();

    let mut args = Vec::new();
    loop {
        if lex.is_keyword(&[")"]) {
            lex.next_token();
            return Ok(args);
        }

        let me = parse_math_expr(lex)?;
        args.push(me);

        if lex.is_keyword(&[")"]) {
            continue;
        }
        if lex.is_keyword(&[","]) {
            lex.next_token();
            continue;
        }
        return Err(format!(
            "unexpected token after [{}]: {}; want ',' or ')'",
            args.last().expect("args is non-empty"),
            go_quote(&lex.token)
        ));
    }
}

/// Port of Go `parseMathExprUnaryMinus` (pipe_math.go).
fn parse_math_expr_unary_minus(lex: &mut Lexer) -> Result<MathExpr, String> {
    if !lex.is_keyword(&["-"]) {
        return Err("missing '-'".to_string());
    }
    lex.next_token();

    let expr = parse_math_expr_operand(lex)?;
    Ok(MathExpr::new_unary_minus(expr))
}

/// Port of Go `parseMathExprConstNumber` (pipe_math.go).
fn parse_math_expr_const_number(lex: &mut Lexer) -> Result<MathExpr, String> {
    if !crate::pipe_math::is_number_prefix(&lex.token) {
        return Err(format!("cannot parse number from {}", go_quote(&lex.token)));
    }
    let num_str = lex
        .next_compound_math_token()
        .map_err(|e| format!("cannot parse number: {e}"))?;
    let f = crate::pipe_math::parse_math_number(&num_str);
    if f.is_nan() {
        return Err(format!("cannot parse number from {}", go_quote(&num_str)));
    }
    Ok(MathExpr::new_const(f, num_str))
}

/// Port of Go `parseMathExprFieldName` (pipe_math.go).
fn parse_math_expr_field_name(lex: &mut Lexer) -> Result<MathExpr, String> {
    let field_name = lex.next_compound_math_token()?;
    let field_name = crate::log_rows::get_canonical_column_name(&field_name);
    Ok(MathExpr::new_field(field_name))
}

// ---- helpers ----

fn expect_keyword(lex: &Lexer, kws: &[&str], name: &str) -> Result<(), String> {
    if !lex.is_keyword(kws) {
        return Err(format!("expecting '{name}'; got {}", go_quote(&lex.token)));
    }
    Ok(())
}

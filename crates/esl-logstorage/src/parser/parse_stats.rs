//! Port of the LogsQL `stats` / `running_stats` / `total_stats` grammar
//! (`pipe_stats.go`, `pipe_running_stats.go`, `stats_*.go`,
//! `running_stats_*.go`).
//!
//! PORT NOTES:
//! * `stats switch(...)` is NOT supported — the Rust `PipeStats` has no
//!   `pipeStatsSwitch`/`pipeStatsCase`/`appendToFuncs` types, and expanding
//!   cases needs cloning a `Box<dyn StatsFunc>` (not available). `switch`
//!   returns an error.
//! * `stats_remote` selects `PipeStatsMode::Remote` on the parsed `PipeStats`
//!   (Go `pipeStatsModeRemote`), making its processor export serialized
//!   states — the wire half of the cluster `stats` split (net_query_runner).
//! * `by (field:bucket offset off)` bucket-offset parsing uses
//!   [`try_parse_bucket_offset`] (port of Go `tryParseBucketOffset`), which —
//!   unlike `try_parse_bucket_size` — accepts negative offsets such as
//!   `offset -2h`.

use crate::block_result::{ByStatsField, try_parse_bucket_size};
use crate::filter::Filter;
use crate::parser::go_quote;
use crate::parser::helpers::*;
use crate::parser::lexer_ext::LexerExt;
use crate::parser::parse_filter::parse_filter;
use crate::pipe::Pipe;
use crate::prefix_filter;
use crate::stats::StatsFunc;
use crate::stream_filter::Lexer;

type BoxStats = Box<dyn StatsFunc>;

/// Stats function keywords (Go `initStatsFuncParsers` keys).
const STATS_FUNC_NAMES: &[&str] = &[
    "any",
    "avg",
    "count",
    "count_empty",
    "count_uniq",
    "count_uniq_hash",
    "field_max",
    "field_min",
    "histogram",
    "json_values",
    "max",
    "median",
    "min",
    "quantile",
    "rate",
    "rate_sum",
    "row_any",
    "row_max",
    "row_min",
    "stddev",
    "sum",
    "sum_len",
    "uniq_values",
    "values",
];

/// Port of Go `isStatsFuncName`.
pub(crate) fn is_stats_func_name(s: &str) -> bool {
    STATS_FUNC_NAMES.contains(&s.to_lowercase().as_str())
}

// ---------------------------------------------------------------------------
// stats pipe
// ---------------------------------------------------------------------------

/// Port of Go `parsePipeStats`.
pub(crate) fn parse_pipe_stats(lex: &mut Lexer) -> Result<Box<dyn Pipe>, String> {
    parse_pipe_stats_ext(lex, true)
}

/// Port of Go `parsePipeStatsNoStatsKeyword`.
pub(crate) fn parse_pipe_stats_no_stats_keyword(lex: &mut Lexer) -> Result<Box<dyn Pipe>, String> {
    parse_pipe_stats_ext(lex, false)
}

fn parse_pipe_stats_ext(
    lex: &mut Lexer,
    need_stats_keyword: bool,
) -> Result<Box<dyn Pipe>, String> {
    // Go `parsePipeStatsExt`: the `stats_remote` keyword selects
    // `pipeStatsModeRemote` (the cluster split's remote half, which exports
    // serialized states instead of finalized values).
    let mut is_remote = false;
    if need_stats_keyword {
        if lex.is_keyword(&["stats", "stats_remote"]) {
            is_remote = lex.is_keyword(&["stats_remote"]);
            lex.next_token();
        } else {
            return Err(format!(
                "expecting 'stats' or 'stats_remote'; got {}",
                go_quote(&lex.token)
            ));
        }
    }

    let mut by_fields: Vec<ByStatsField> = Vec::new();
    if lex.is_keyword(&["by", "("]) {
        if lex.is_keyword(&["by"]) {
            lex.next_token();
        }
        by_fields =
            parse_by_stats_fields(lex).map_err(|e| format!("cannot parse 'by' clause: {e}"))?;
    }

    let mut funcs = Vec::new();
    loop {
        let (e, e_str) = parse_stats_entry(lex)?;
        funcs.push(e);
        if lex.is_query_part_trailer() {
            break;
        }
        if !lex.is_keyword(&[","]) {
            return Err(format!(
                "unexpected token {} after [{e_str}]; want ',', '|', ';' or ')'",
                go_quote(&lex.token)
            ));
        }
        lex.next_token();
    }

    let mut ps = crate::pipe_stats::new_pipe_stats(by_fields, funcs)?;
    if is_remote {
        ps.set_mode(crate::pipe_stats::PipeStatsMode::Remote);
    }
    Ok(Box::new(ps))
}

/// Port of Go `parseByStatsFields`.
fn parse_by_stats_fields(lex: &mut Lexer) -> Result<Vec<ByStatsField>, String> {
    if !lex.is_keyword(&["("]) {
        return Err("missing `(`".to_string());
    }
    let mut bfs = Vec::new();
    loop {
        lex.next_token();
        if lex.is_keyword(&[")"]) {
            lex.next_token();
            return Ok(bfs);
        }
        let field_name = lex
            .next_compound_token_ext(&[":"])
            .map_err(|e| format!("cannot parse field name: {e}"))?;
        let field_name = crate::log_rows::get_canonical_column_name(&field_name).to_string();
        let mut bf = ByStatsField {
            name: field_name.clone(),
            ..Default::default()
        };
        if lex.is_keyword(&[":"]) {
            lex.next_token();
            let bucket_size_str = lex.next_compound_token().map_err(|e| {
                format!(
                    "cannot parse bucket size for field {}: {e}",
                    go_quote(&field_name)
                )
            })?;
            if bucket_size_str != "year" && bucket_size_str != "month" {
                let bucket_size = try_parse_bucket_size(&bucket_size_str).ok_or_else(|| {
                    format!(
                        "cannot parse bucket size for field {}: {}",
                        go_quote(&field_name),
                        go_quote(&bucket_size_str)
                    )
                })?;
                bf.bucket_size = bucket_size;
            }
            bf.bucket_size_str = bucket_size_str;
            if lex.is_keyword(&["offset"]) {
                lex.next_token();
                let bucket_offset_str = lex.next_compound_token().map_err(|e| {
                    format!(
                        "cannot parse offset token for {}: {e}",
                        go_quote(&field_name)
                    )
                })?;
                let bucket_offset =
                    try_parse_bucket_offset(&bucket_offset_str).ok_or_else(|| {
                        format!(
                            "cannot parse bucket offset for field {}: {}",
                            go_quote(&field_name),
                            go_quote(&bucket_offset_str)
                        )
                    })?;
                bf.bucket_offset_str = bucket_offset_str;
                bf.bucket_offset = bucket_offset;
            }
        }
        bfs.push(bf);
        if lex.is_keyword(&[")"]) {
            lex.next_token();
            return Ok(bfs);
        }
        if !lex.is_keyword(&[","]) {
            return Err(format!(
                "unexpected token: {}; expecting ',' or ')'",
                go_quote(&lex.token)
            ));
        }
    }
}

/// Port of Go `tryParseBucketOffset` (pipe_stats.go), which can have the
/// following formats:
///
/// - integer number: 12345
/// - floating-point number: 1.2345
/// - duration: 1.5s - it is converted to nanoseconds
/// - bytes: 1.5KiB
///
/// Unlike `try_parse_bucket_size`, negative offsets are allowed.
fn try_parse_bucket_offset(s: &str) -> Option<f64> {
    // Try parsing s as floating point number
    if let Some(f) = crate::values_encoder::try_parse_float64(s) {
        return Some(f);
    }

    // Try parsing s as duration (1s, 5m, etc.)
    if let Some(nsecs) = crate::values_encoder::try_parse_duration(s) {
        return Some(nsecs as f64);
    }

    // Try parsing s as bytes (KiB, MB, etc.)
    if let Some(n) = crate::values_encoder::try_parse_bytes(s) {
        return Some(n as f64);
    }

    None
}

/// Port of Go `parseStatsEntry` (the `switch` branch is unsupported). Returns
/// the built func plus its result-name (for the caller's error messages, since
/// `PipeStatsFunc.result_name` is private).
fn parse_stats_entry(
    lex: &mut Lexer,
) -> Result<(crate::pipe_stats::PipeStatsFunc, String), String> {
    let sf = parse_stats_func(lex)?;
    let sf_str = sf.to_string();

    if lex.is_keyword(&["switch"]) {
        return Err(format!(
            "cannot parse 'switch' for [{sf_str}]: 'stats ... switch(...)' is not supported by this port"
        ));
    }

    let mut iff_filter: Option<Box<dyn Filter>> = None;
    let mut iff_str = String::new();
    if lex.is_keyword(&["if"]) {
        let (fb, s) = parse_if_filter_boxed(lex)
            .map_err(|e| format!("cannot parse 'if' filter for [{sf_str}]: {e}"))?;
        iff_str = s;
        iff_filter = Some(fb);
    }

    let result_name = if lex.is_keyword(&[","]) || lex.is_query_part_trailer() {
        if iff_str.is_empty() {
            sf_str
        } else {
            format!("{sf_str} {iff_str}")
        }
    } else {
        if lex.is_keyword(&["as"]) {
            lex.next_token();
        }
        parse_field_name(lex)
            .map_err(|e| format!("cannot parse result name for [{sf_str}]: {e}"))?
    };

    let func = crate::pipe_stats::new_pipe_stats_func(sf, iff_filter, result_name.clone());
    Ok((func, result_name))
}

/// Parses an `if (...)` / `case (...)` clause and returns `(filter, "if (...)")`.
/// Local boxed variant of `if_filter::parse_if_filter` (which yields an `Arc`,
/// while `PipeStatsFunc.iff` needs a `Box<dyn Filter>`).
fn parse_if_filter_boxed(lex: &mut Lexer) -> Result<(Box<dyn Filter>, String), String> {
    if !lex.is_keyword(&["if", "case"]) {
        return Err(format!(
            "unexpected keyword {}; expecting 'if' or 'case'",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    if !lex.is_keyword(&["("]) {
        return Err(format!(
            "unexpected token {} after 'if'; expecting '('",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    if lex.is_keyword(&[")"]) {
        lex.next_token();
        let f: Box<dyn Filter> = Box::new(crate::filter_noop::new_filter_noop());
        let s = format!("if ({})", f.to_string());
        return Ok((f, s));
    }
    let f = parse_filter(lex, true).map_err(|e| format!("cannot parse 'if' filter: {e}"))?;
    if lex.is_keyword(&[";"]) {
        lex.next_token();
    }
    if !lex.is_keyword(&[")"]) {
        return Err(format!(
            "unexpected token {} after 'if' filter; expecting ')'",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    let s = format!("if ({})", f.to_string());
    Ok((f, s))
}

// ---------------------------------------------------------------------------
// stats function dispatch
// ---------------------------------------------------------------------------

/// Port of Go `parseStatsFunc`.
fn parse_stats_func(lex: &mut Lexer) -> Result<BoxStats, String> {
    macro_rules! try_func {
        ($name:literal, $parser:ident) => {
            if lex.is_keyword(&[$name]) {
                return $parser(lex)
                    .map_err(|e| format!("cannot parse {} func: {e}", go_quote($name)));
            }
        };
    }
    try_func!("any", parse_stats_any);
    try_func!("avg", parse_stats_avg);
    try_func!("count", parse_stats_count);
    try_func!("count_empty", parse_stats_count_empty);
    try_func!("count_uniq", parse_stats_count_uniq);
    try_func!("count_uniq_hash", parse_stats_count_uniq_hash);
    try_func!("field_max", parse_stats_field_max);
    try_func!("field_min", parse_stats_field_min);
    try_func!("histogram", parse_stats_histogram);
    try_func!("json_values", parse_stats_json_values);
    try_func!("max", parse_stats_max);
    try_func!("median", parse_stats_median);
    try_func!("min", parse_stats_min);
    try_func!("quantile", parse_stats_quantile);
    try_func!("rate", parse_stats_rate);
    try_func!("rate_sum", parse_stats_rate_sum);
    try_func!("row_any", parse_stats_row_any);
    try_func!("row_max", parse_stats_row_max);
    try_func!("row_min", parse_stats_row_min);
    try_func!("stddev", parse_stats_stddev);
    try_func!("sum", parse_stats_sum);
    try_func!("sum_len", parse_stats_sum_len);
    try_func!("uniq_values", parse_stats_uniq_values);
    try_func!("values", parse_stats_values);
    Err(format!("unknown stats func {}", go_quote(&lex.token)))
}

// ---- shared arg helpers ----

fn parse_stats_func_field_filters(lex: &mut Lexer, func_name: &str) -> Result<Vec<String>, String> {
    consume_func_keyword(lex, func_name)?;
    let mut fields = parse_field_filters_in_parens(lex)
        .map_err(|e| format!("cannot parse {} args: {e}", go_quote(func_name)))?;
    if fields.is_empty() {
        fields = vec!["*".to_string()];
    }
    Ok(fields)
}

fn parse_stats_func_fields(lex: &mut Lexer, func_name: &str) -> Result<Vec<String>, String> {
    consume_func_keyword(lex, func_name)?;
    let fields = parse_field_filters_in_parens(lex)
        .map_err(|e| format!("cannot parse {} args: {e}", go_quote(func_name)))?;
    for f in &fields {
        if prefix_filter::is_wildcard_filter(f) {
            return Err(format!(
                "unexpected wildcard filter {} inside {func_name}()",
                go_quote(f)
            ));
        }
    }
    Ok(fields)
}

fn parse_stats_func_args(lex: &mut Lexer, func_name: &str) -> Result<Vec<String>, String> {
    consume_func_keyword(lex, func_name)?;
    parse_field_names_in_parens(lex)
        .map_err(|e| format!("cannot parse {} args: {e}", go_quote(func_name)))
}

fn consume_func_keyword(lex: &mut Lexer, func_name: &str) -> Result<(), String> {
    if !lex.is_keyword(&[func_name]) {
        return Err(format!(
            "unexpected func; got {}; want {}",
            go_quote(&lex.token),
            go_quote(func_name)
        ));
    }
    lex.next_token();
    Ok(())
}

// ---- per-function parsers ----

fn parse_stats_count(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "count")?;
    Ok(Box::new(crate::stats_count::new_stats_count(ff)))
}

fn parse_stats_count_empty(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "count_empty")?;
    Ok(Box::new(crate::stats_count_empty::new_stats_count_empty(
        ff,
    )))
}

fn parse_stats_count_uniq(lex: &mut Lexer) -> Result<BoxStats, String> {
    let fields = parse_stats_func_fields(lex, "count_uniq")?;
    if fields.is_empty() {
        return Err("expecting at least a single field".to_string());
    }
    let mut limit = 0;
    if lex.is_keyword(&["limit"]) {
        limit = parse_limit(lex)?;
    }
    Ok(Box::new(crate::stats_count_uniq::StatsCountUniq::new(
        fields, limit,
    )))
}

fn parse_stats_count_uniq_hash(lex: &mut Lexer) -> Result<BoxStats, String> {
    let fields = parse_stats_func_fields(lex, "count_uniq_hash")?;
    if fields.is_empty() {
        return Err("expecting at least a single field".to_string());
    }
    let mut limit = 0;
    if lex.is_keyword(&["limit"]) {
        limit = parse_limit(lex)?;
    }
    Ok(Box::new(
        crate::stats_count_uniq_hash::StatsCountUniqHash::new(fields, limit),
    ))
}

fn parse_stats_sum(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "sum")?;
    Ok(Box::new(crate::stats_sum::new_stats_sum(ff)))
}

fn parse_stats_sum_len(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "sum_len")?;
    Ok(Box::new(crate::stats_sum_len::new_stats_sum_len(ff)))
}

fn parse_stats_avg(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "avg")?;
    Ok(Box::new(crate::stats_avg::new_stats_avg(ff)))
}

fn parse_stats_min(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "min")?;
    Ok(Box::new(crate::stats_min::new_stats_min(ff)))
}

fn parse_stats_max(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "max")?;
    Ok(Box::new(crate::stats_max::new_stats_max(ff)))
}

fn parse_stats_field_min(lex: &mut Lexer) -> Result<BoxStats, String> {
    let args = parse_stats_func_args(lex, "field_min")?;
    Ok(Box::new(crate::stats_field_min::new_stats_field_min(args)?))
}

fn parse_stats_field_max(lex: &mut Lexer) -> Result<BoxStats, String> {
    let args = parse_stats_func_args(lex, "field_max")?;
    Ok(Box::new(crate::stats_field_max::new_stats_field_max(args)?))
}

fn parse_stats_row_min(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "row_min")?;
    Ok(Box::new(crate::stats_row_min::new_stats_row_min(ff)?))
}

fn parse_stats_row_max(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "row_max")?;
    Ok(Box::new(crate::stats_row_max::new_stats_row_max(ff)?))
}

fn parse_stats_row_any(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "row_any")?;
    Ok(Box::new(crate::stats_row_any::new_stats_row_any(ff)))
}

fn parse_stats_median(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "median")?;
    Ok(Box::new(crate::stats_median::new_stats_median(ff)))
}

fn parse_stats_quantile(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "quantile")?;
    Ok(Box::new(crate::stats_quantile::new_stats_quantile(ff)?))
}

fn parse_stats_histogram(lex: &mut Lexer) -> Result<BoxStats, String> {
    let fields = parse_stats_func_fields(lex, "histogram")
        .map_err(|e| format!("cannot parse field name: {e}"))?;
    if fields.len() != 1 {
        return Err(format!(
            "'histogram' accepts only a single field; got {} fields",
            fields.len()
        ));
    }
    Ok(Box::new(crate::stats_histogram::StatsHistogram::new(
        fields[0].clone(),
    )))
}

fn parse_stats_rate(lex: &mut Lexer) -> Result<BoxStats, String> {
    let fields = parse_stats_func_fields(lex, "rate")?;
    if !fields.is_empty() {
        return Err(format!(
            "unexpected non-empty args for 'rate()' function: {}",
            go_quote(&fields.join(","))
        ));
    }
    Ok(Box::new(crate::stats_rate::new_stats_rate()))
}

fn parse_stats_rate_sum(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "rate_sum")?;
    Ok(Box::new(crate::stats_rate_sum::new_stats_rate_sum(ff)))
}

fn parse_stats_stddev(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "stddev")?;
    Ok(Box::new(crate::stats_stddev::new_stats_stddev(ff)))
}

fn parse_stats_uniq_values(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "uniq_values")?;
    let mut limit = 0;
    if lex.is_keyword(&["limit"]) {
        limit = parse_limit(lex)?;
    }
    Ok(Box::new(crate::stats_uniq_values::StatsUniqValues::new(
        ff, limit,
    )))
}

fn parse_stats_values(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "values")?;
    let mut limit = 0;
    if lex.is_keyword(&["limit"]) {
        limit = parse_limit(lex)?;
    }
    Ok(Box::new(crate::stats_values::StatsValues::new(ff, limit)))
}

fn parse_stats_json_values(lex: &mut Lexer) -> Result<BoxStats, String> {
    let ff = parse_stats_func_field_filters(lex, "json_values")?;
    let mut sort_fields = Vec::new();
    if lex.is_keyword(&["sort", "order"]) {
        lex.next_token();
        if lex.is_keyword(&["by"]) {
            lex.next_token();
        }
        let raw = parse_by_sort_fields_raw(lex).map_err(|e| format!("cannot parse 'sort': {e}"))?;
        sort_fields = raw
            .into_iter()
            .map(|(name, is_desc)| crate::stats_json_values::BySortField::new(name, is_desc))
            .collect();
    }
    let mut limit = 0;
    if lex.is_keyword(&["limit"]) {
        limit = parse_limit(lex)?;
    }
    Ok(Box::new(crate::stats_json_values::StatsJSONValues::new(
        ff,
        sort_fields,
        limit,
    )))
}

fn parse_stats_any(lex: &mut Lexer) -> Result<BoxStats, String> {
    let args = parse_stats_func_args(lex, "any")?;
    if args.len() != 1 {
        return Err(format!(
            "unexpected number of args for 'any' function; got {}; want 1; args: {}",
            args.len(),
            go_quote(&args.join(","))
        ));
    }
    Ok(Box::new(crate::stats_any::new_stats_any(args[0].clone())))
}

// ---------------------------------------------------------------------------
// running_stats / total_stats
// ---------------------------------------------------------------------------

use crate::pipe_running_stats::RunningStatsFunc;

type BoxRunning = Box<dyn RunningStatsFunc>;

/// Port of Go `parsePipeRunningStats`.
pub(crate) fn parse_pipe_running_stats(lex: &mut Lexer) -> Result<Box<dyn Pipe>, String> {
    if !lex.is_keyword(&["running_stats"]) {
        return Err(format!(
            "expecting `running_stats`; got {}",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    parse_pipe_running_stats_ext(lex, false)
}

/// Port of Go `parsePipeTotalStats`.
pub(crate) fn parse_pipe_total_stats(lex: &mut Lexer) -> Result<Box<dyn Pipe>, String> {
    if !lex.is_keyword(&["total_stats"]) {
        return Err(format!(
            "expecting `total_stats`; got {}",
            go_quote(&lex.token)
        ));
    }
    lex.next_token();
    parse_pipe_running_stats_ext(lex, true)
}

fn parse_pipe_running_stats_ext(lex: &mut Lexer, is_total: bool) -> Result<Box<dyn Pipe>, String> {
    let mut by_fields = Vec::new();
    if lex.is_keyword(&["by", "("]) {
        if lex.is_keyword(&["by"]) {
            lex.next_token();
        }
        by_fields = parse_field_names_in_parens(lex)
            .map_err(|e| format!("cannot parse 'by' clause: {e}"))?;
    }

    let mut seen_result_names: Vec<String> = Vec::new();
    let mut funcs = Vec::new();
    loop {
        let sf = parse_running_stats_func(lex)?;
        let sf_str = sf.to_string();
        let result_name = if lex.is_keyword(&[","]) || lex.is_query_part_trailer() {
            sf_str.clone()
        } else {
            if lex.is_keyword(&["as"]) {
                lex.next_token();
            }
            parse_field_name(lex)
                .map_err(|e| format!("cannot parse result name for [{sf_str}]: {e}"))?
        };
        if seen_result_names.contains(&result_name) {
            return Err(format!(
                "cannot use identical result name {} for [{sf_str}] and [{sf_str}]",
                go_quote(&result_name)
            ));
        }
        seen_result_names.push(result_name.clone());
        funcs.push(crate::pipe_running_stats::new_pipe_running_stats_func(
            sf,
            result_name,
        ));
        if lex.is_query_part_trailer() {
            return Ok(Box::new(crate::pipe_running_stats::new_pipe_running_stats(
                is_total, by_fields, funcs,
            )));
        }
        if !lex.is_keyword(&[","]) {
            return Err(format!(
                "unexpected token {} after [{sf_str}]; want ',', '|' or ')'",
                go_quote(&lex.token)
            ));
        }
        lex.next_token();
    }
}

fn parse_running_stats_func(lex: &mut Lexer) -> Result<BoxRunning, String> {
    macro_rules! try_func {
        ($name:literal, $parser:ident) => {
            if lex.is_keyword(&[$name]) {
                return $parser(lex)
                    .map_err(|e| format!("cannot parse {} func: {e}", go_quote($name)));
            }
        };
    }
    try_func!("count", parse_running_stats_count);
    try_func!("first", parse_running_stats_first);
    try_func!("last", parse_running_stats_last);
    try_func!("max", parse_running_stats_max);
    try_func!("min", parse_running_stats_min);
    try_func!("sum", parse_running_stats_sum);
    Err(format!("unknown stats func {}", go_quote(&lex.token)))
}

fn parse_running_stats_count(lex: &mut Lexer) -> Result<BoxRunning, String> {
    let ff = parse_stats_func_field_filters(lex, "count")?;
    Ok(Box::new(
        crate::running_stats_count::new_running_stats_count(ff),
    ))
}
fn parse_running_stats_min(lex: &mut Lexer) -> Result<BoxRunning, String> {
    let ff = parse_stats_func_field_filters(lex, "min")?;
    Ok(Box::new(crate::running_stats_min::new_running_stats_min(
        ff,
    )))
}
fn parse_running_stats_max(lex: &mut Lexer) -> Result<BoxRunning, String> {
    let ff = parse_stats_func_field_filters(lex, "max")?;
    Ok(Box::new(crate::running_stats_max::new_running_stats_max(
        ff,
    )))
}
fn parse_running_stats_sum(lex: &mut Lexer) -> Result<BoxRunning, String> {
    let ff = parse_stats_func_field_filters(lex, "sum")?;
    Ok(Box::new(crate::running_stats_sum::new_running_stats_sum(
        ff,
    )))
}

fn parse_running_stats_first(lex: &mut Lexer) -> Result<BoxRunning, String> {
    let (field_name, offset) = parse_running_first_last(lex, "first")?;
    Ok(Box::new(
        crate::running_stats_first::new_running_stats_first(field_name, offset),
    ))
}
fn parse_running_stats_last(lex: &mut Lexer) -> Result<BoxRunning, String> {
    let (field_name, offset) = parse_running_first_last(lex, "last")?;
    Ok(Box::new(crate::running_stats_last::new_running_stats_last(
        field_name, offset,
    )))
}

fn parse_running_first_last(lex: &mut Lexer, func_name: &str) -> Result<(String, usize), String> {
    let args = parse_stats_func_args(lex, func_name)?;
    if args.len() != 1 {
        return Err(format!(
            "unexpected number of args for the {func_name}() function; got {}; want 1; args: {}",
            args.len(),
            go_quote(&args.join(","))
        ));
    }
    let field_name = args[0].clone();
    let mut offset: usize = 0;
    if lex.is_keyword(&["offset"]) {
        lex.next_token();
        let offset_str = lex.token.clone();
        lex.next_token();
        let n: i64 = offset_str.parse().map_err(|_| {
            format!(
                "cannot parse offset={} at {func_name}({}): invalid integer",
                go_quote(&offset_str),
                go_quote(&field_name)
            )
        })?;
        if n < 0 {
            return Err(format!(
                "offset={n} cannot be negative at {func_name}({})",
                go_quote(&field_name)
            ));
        }
        offset = n as usize;
    }
    Ok((field_name, offset))
}

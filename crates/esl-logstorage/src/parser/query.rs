//! [`Query`], `queryOptions`, the public [`Filter`] wrapper, and the
//! `ParseQuery*` entry points — port of the `Query`/`queryOptions`/`Filter`
//! types and `ParseQuery*` functions from `parser.go`.

use std::fmt;

use esl_common::cgroup;

use crate::consts::MAX_PARALLEL_READERS;
use crate::filter::Filter as FilterTrait;
use crate::parser::go_quote;
use crate::parser::lexer_ext::LexerExt;
use crate::parser::parse_filter::parse_filter;
use crate::parser::parse_pipe::parse_pipes;
use crate::pipe::Pipe;
use crate::prefix_filter;
use crate::stream_filter::Lexer;
use crate::stream_id::StreamID;
use crate::values_encoder::{
    marshal_timestamp_rfc3339_nano_precise_string, sub_int64_no_overflow, try_parse_duration,
    try_parse_uint64,
};

/// Go `nsecsPerSecond` (consts.go).
const NSECS_PER_SECOND: i64 = 1_000_000_000;

/// Query options set via `options(...)` (Go `queryOptions`).
#[derive(Default)]
pub struct QueryOptions {
    pub(crate) need_print: bool,
    pub(crate) concurrency: u32,
    pub(crate) parallel_readers: u32,
    pub(crate) ignore_global_time_filter: Option<bool>,
    pub(crate) allow_partial_response: Option<bool>,
    pub(crate) time_offset: i64,
    pub(crate) time_offset_str: String,
    pub(crate) global_filter: Option<Box<dyn FilterTrait>>,
}

impl fmt::Display for QueryOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.need_print {
            return Ok(());
        }
        let mut a: Vec<String> = Vec::new();
        if self.concurrency > 0 {
            a.push(format!("concurrency={}", self.concurrency));
        }
        if self.parallel_readers > 0 {
            a.push(format!("parallel_readers={}", self.parallel_readers));
        }
        if let Some(v) = self.ignore_global_time_filter {
            a.push(format!("ignore_global_time_filter={v}"));
        }
        if let Some(v) = self.allow_partial_response {
            a.push(format!("allow_partial_response={v}"));
        }
        if !self.time_offset_str.is_empty() {
            a.push(format!("time_offset={}", self.time_offset_str));
        }
        if let Some(gf) = &self.global_filter {
            a.push(format!("global_filter=({})", gf.to_string()));
        }
        if a.is_empty() {
            return Ok(());
        }
        write!(f, "options({})", a.join(", "))
    }
}

/// Represents a parsed LogsQL query (Go `Query`).
pub struct Query {
    pub(crate) opts: QueryOptions,
    pub(crate) f: Box<dyn FilterTrait>,
    pub(crate) pipes: Vec<Box<dyn Pipe>>,
    /// Timestamp context used for parsing (Go `timestamp`).
    pub(crate) timestamp: i64,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let opts = self.opts.to_string();
        if !opts.is_empty() {
            write!(f, "{opts} ")?;
        }
        write!(f, "{}", self.f.to_string())?;
        for p in &self.pipes {
            write!(f, " | {}", p.to_string())?;
        }
        Ok(())
    }
}

impl Query {
    /// Returns the timestamp context (Go `GetTimestamp`).
    pub fn get_timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Drops all pipes (Go `DropAllPipes`).
    pub fn drop_all_pipes(&mut self) {
        self.pipes.clear();
    }

    /// Returns true if the query can be used in live tailing (Go `CanLiveTail`).
    pub fn can_live_tail(&self) -> bool {
        self.pipes.iter().all(|p| p.can_live_tail())
    }

    /// Returns true if all pipes can return the last N results
    /// (Go `CanReturnLastNResults`).
    pub fn can_return_last_n_results(&self) -> bool {
        self.pipes.iter().all(|p| p.can_return_last_n_results())
    }

    /// Port of Go `(*Query).optimize`.
    ///
    /// PORT NOTE: Go applies `optimizeNoSubqueries` to every subquery via
    /// `visitSubqueries`; the Rust subqueries are stored as rendered text and
    /// re-parsed (which re-runs `optimize` on them), so only the top-level
    /// query is visited here.
    pub(crate) fn optimize(&mut self) {
        self.optimize_no_subqueries();
    }

    /// Port of Go `(*Query).optimizeNoSubqueries`.
    ///
    /// PORT NOTE: Go type-switches on `*pipeFilter` for `optimizeFilterPipes`
    /// and the leading-`filter`-pipe merge; the `Pipe` trait has no downcast
    /// hook (pipe.rs is owned by another port slice), so both rewrites go
    /// through the rendered pipe string — the established render/re-parse
    /// divergence (`Query::clone`, `clone_pipe`). Two rewrites stay deferred
    /// pending `Pipe`-trait hooks: `optimizeUniqLimitPipes` (merging
    /// `uniq ... | limit N` needs a uniq-limit mutator) and marking a leading
    /// `pipeFieldNames` as first pipe (the first-pipe mode itself is
    /// unimplemented — see `Storage::get_field_names`).
    pub(crate) fn optimize_no_subqueries(&mut self) {
        let pipes = std::mem::take(&mut self.pipes);
        let pipes = optimize_offset_limit_pipes(pipes);
        self.pipes = optimize_filter_pipes(pipes, self.timestamp);

        // Merge `q | filter ...` into q.
        if let Some(expr) = self
            .pipes
            .first()
            .and_then(|p| pipe_filter_expr(p.as_ref()))
            && let Ok(fq) = ParseQueryAtTimestamp(&expr, self.timestamp)
            && fq.pipes.is_empty()
        {
            let f = std::mem::replace(&mut self.f, Box::new(crate::filter_noop::new_filter_noop()));
            self.f = merge_filters_and(f, fq.f);
            self.pipes.remove(0);
        }

        let f = std::mem::replace(&mut self.f, Box::new(crate::filter_noop::new_filter_noop()));
        self.f = optimize_filters(f);
    }

    /// Port of Go `isStarQuery`.
    ///
    /// PORT NOTE: Go type-switches on `*filterNoop` / `*filterGeneric` with an
    /// empty-prefix `filterPrefix` on `_msg`; both cases are exactly the
    /// `is_match_all` trait classification used by `removeStarFilters`.
    pub(crate) fn is_star_query(&self) -> bool {
        if !self.pipes.is_empty() {
            return false;
        }
        if self.opts.need_print {
            return false;
        }
        self.f.is_match_all()
    }

    /// Port of Go `hasFilterInWithQuery` over a whole query: true when the
    /// query's global filter, top-level filter or any pipe embeds an
    /// `in(<subquery>)` filter (the condition guarding Go
    /// `initFilterInValues`).
    pub(crate) fn has_filter_in_with_query(&self) -> bool {
        use crate::storage_search::has_filter_in_with_query_for_filter;
        if let Some(gf) = &self.opts.global_filter
            && has_filter_in_with_query_for_filter(gf.as_ref())
        {
            return true;
        }
        if has_filter_in_with_query_for_filter(self.f.as_ref()) {
            return true;
        }
        self.pipes.iter().any(|p| p.has_filter_in_with_query())
    }

    /// PORT NOTE: Go `initStatsRateFuncSteps` requires downcasting pipes to
    /// `pipeStats`; deferred (execution-only, does not affect `String()`).
    fn init_stats_rate_func_steps(&mut self) {}

    /// Returns the parsed pipe chain (Go accesses `q.pipes` directly).
    pub fn pipes(&self) -> &[Box<dyn Pipe>] {
        &self.pipes
    }

    /// Returns the query's top-level filter (Go `getFinalFilter`).
    ///
    /// PORT NOTE: Go combines `opts.globalFilter` with `q.f` via
    /// `newFilterAnd(...)` + `optimizeFilters`. Composing an owned filter needs
    /// the deferred `copyFilter`/downcast machinery, so this returns `q.f`
    /// directly. This is correct whenever `options(global_filter=...)` is unused
    /// (the common case); when a global filter is set it is simply not ANDed in.
    pub(crate) fn get_final_filter(&self) -> &dyn FilterTrait {
        self.f.as_ref()
    }

    /// Builds the set of columns needed by the pipe chain (Go `getNeededColumns`).
    ///
    /// Seeds an allow-all (`*`) prefix filter and walks the pipes in reverse,
    /// letting each pipe restrict/extend the needed fields via
    /// `update_needed_fields`.
    pub(crate) fn get_needed_columns(&self) -> prefix_filter::Filter {
        let mut pf = prefix_filter::Filter::default();
        pf.add_allow_filter("*");
        for p in self.pipes.iter().rev() {
            p.update_needed_fields(&mut pf);
        }
        pf
    }

    /// Returns the `[min, max]` `_time` bounds for the query (Go
    /// `GetFilterTimeRange`).
    pub fn get_filter_time_range(&self) -> (i64, i64) {
        let f = self.get_final_filter();
        get_filter_time_range(f)
    }

    /// Returns the streamID pre-filter for the query (Go `getStreamIDs`).
    ///
    /// PORT NOTE: Go type-switches on `*filterAnd` / `*filterOr` /
    /// `*filterStreamID`; the port dispatches through the `and_children` /
    /// `or_children` / `stream_ids` trait hooks.
    pub(crate) fn get_stream_ids(&self) -> Vec<StreamID> {
        let f = self.get_final_filter();
        if let Some(children) = f.and_children() {
            for child in children {
                let (stream_ids, ok) = get_stream_ids_from_filter_or(child.as_ref());
                if ok {
                    return stream_ids;
                }
            }
            return Vec::new();
        }
        get_stream_ids_from_filter_or(f).0
    }

    /// Returns the number of IO-bound parallel readers for the query
    /// (Go `GetParallelReaders`).
    pub fn get_parallel_readers(&self, default_parallel_readers: usize) -> usize {
        let mut n = self.opts.parallel_readers as usize;
        if n == 0 {
            n = self.opts.concurrency as usize;
        }
        if n == 0 {
            n = default_parallel_readers;
        }
        if n == 0 {
            n = 2 * cgroup::available_cpus();
        }
        if n > MAX_PARALLEL_READERS {
            n = MAX_PARALLEL_READERS;
        }
        n
    }

    /// Returns the query's `_time` offset in nanoseconds (Go `q.opts.timeOffset`).
    pub(crate) fn time_offset(&self) -> i64 {
        self.opts.time_offset
    }

    /// Returns the number of CPU-bound workers for the query (Go `GetConcurrency`).
    pub fn get_concurrency(&self) -> usize {
        let mut concurrency = cgroup::available_cpus();
        let c = self.opts.concurrency as usize;
        if c > 0 && c < concurrency {
            concurrency = c;
        }
        concurrency
    }

    /// Returns a copy of q at the given timestamp (Go `Clone`).
    ///
    /// Like Go, the copy is produced by re-parsing `self.to_string()` at
    /// `timestamp` (filters/pipes are trait objects without a copy hook).
    pub fn clone(&self, timestamp: i64) -> Query {
        let q_str = self.to_string();
        match ParseQueryAtTimestamp(&q_str, timestamp) {
            Ok(q) => q,
            Err(err) => {
                esl_common::panicf!("BUG: cannot parse {}: {err}", go_quote(&q_str));
                unreachable!()
            }
        }
    }

    /// Clones q at the given timestamp and adds `_time:[start, end]` filter to
    /// the cloned q (Go `CloneWithTimeFilter`).
    pub fn clone_with_time_filter(&self, timestamp: i64, start: i64, end: i64) -> Query {
        let mut q_copy = self.clone(timestamp);
        q_copy.add_time_filter(start, end);
        q_copy
    }

    /// Returns a query for optimized querying of the last `limit` results with
    /// the biggest `_time` values with an optional `offset`
    /// (Go `GetLastNResultsQuery`).
    ///
    /// `None` is returned if q cannot be used for optimized querying of the
    /// last N results (Go returns a nil query).
    pub fn get_last_n_results_query(&self) -> Option<(Query, u64, u64)> {
        let (start, end) = self.get_filter_time_range();
        if !can_apply_last_n_results_optimization(start, end) {
            // It is faster to execute the query as is on such a small time range.
            return None;
        }

        // Remember the trailing 'fields' and 'delete' pipes - they are moved in
        // front of the `sort` pipe below.
        let mut idx = self.pipes.len();
        while idx > 0 && self.pipes[idx - 1].is_fields_or_delete_pipe() {
            idx -= 1;
        }
        let tail_len = self.pipes.len() - idx;
        let pipes_len = idx;
        if pipes_len == 0 {
            return None;
        }

        // The query must end with one of the following pipes in order to be
        // eligible for the optimization:
        // - 'sort by (_time desc) offset <offset> limit <limit>'
        // - 'first <limit> by (_time desc)'
        // - 'last <limit> by (_time)'
        let p_last = &self.pipes[pipes_len - 1];
        let (offset, limit) = p_last.get_offset_limit()?;

        // Remove the `| sort ...` pipe from the query, add the tail pipes and
        // verify whether it can reliably return last N results with the biggest
        // _time values.
        let mut q_copy = self.clone(self.get_timestamp());
        if q_copy.pipes.len() != self.pipes.len() {
            return None;
        }
        let tail: Vec<Box<dyn Pipe>> = q_copy
            .pipes
            .drain(q_copy.pipes.len() - tail_len..)
            .collect();
        q_copy.pipes.truncate(pipes_len - 1);
        q_copy.pipes.extend(tail);
        if !q_copy.can_return_last_n_results() {
            return None;
        }

        // The query is eligible for last N results optimization.
        Some((q_copy, offset, limit))
    }

    /// Adds global filter `_time:[start ... end]` to q (Go `AddTimeFilter`).
    ///
    /// PORT NOTE: Go applies the filter to subqueries too (`visitSubqueries`
    /// over `join`/`union`/`in(...)` subqueries); the Rust subqueries are
    /// stored as rendered text and `visitSubqueries`-based propagation is
    /// deferred, so only the top-level query is updated (Go
    /// `addTimeFilterNoSubqueries`) — subqueries keep their own `_time`
    /// filters, without inheriting the global one.
    pub fn add_time_filter(&mut self, start: i64, end: i64) {
        if self.opts.ignore_global_time_filter == Some(true) {
            return;
        }

        let f = std::mem::replace(&mut self.f, Box::new(crate::filter_noop::new_filter_noop()));
        self.f = add_time_filter(f, start, end, self.opts.time_offset);

        self.init_stats_rate_func_steps();
    }

    /// Adds `extra_filters` to q (Go `AddExtraFilters`).
    ///
    /// PORT NOTE: Go takes `*Filter` and shares the filter across
    /// `visitSubqueries`; Rust filters are single-owner trait objects, so the
    /// method consumes the [`Filter`]. Subquery propagation
    /// (`visitSubqueries` over `join`/`union`/`in(...)` pipes) is deferred
    /// (see `add_time_filter`), so only the top-level query is updated
    /// (Go `addExtraFiltersNoSubqueries`).
    pub fn add_extra_filters(&mut self, extra_filters: Filter) {
        let Some(ef) = extra_filters.f else {
            return;
        };

        let mut f = std::mem::replace(&mut self.f, Box::new(crate::filter_noop::new_filter_noop()));
        let mut filters: Vec<Box<dyn FilterTrait>> = vec![ef];
        if let Some(children) = f.take_and_children() {
            filters.extend(children);
        } else {
            filters.push(f);
        }
        self.f = Box::new(crate::filter_and::new_filter_and(filters));

        // Go `addExtraFiltersNoSubqueries` runs the full optimize pass after
        // prepending the extra filters.
        self.optimize_no_subqueries();
    }

    /// Adds `| sort (_time) desc` pipe to q (Go `AddPipeSortByTimeDesc`).
    pub fn add_pipe_sort_by_time_desc(&mut self) {
        let s = "sort by (_time) desc";
        self.must_append_pipe(s);
    }

    /// Adds `| fields ...` pipe for the given fields to q (Go `AddPipeFields`).
    ///
    /// See <https://docs.victoriametrics.com/victorialogs/logsql/#fields-pipe>
    pub fn add_pipe_fields(&mut self, fields: &[String]) {
        let a: Vec<String> = fields
            .iter()
            .map(|field| crate::parser::quote_token_if_needed(field))
            .collect();
        let s = format!("fields {}", a.join(", "));
        self.must_append_pipe(&s);
    }

    /// Adds `| offset <offset> | limit <limit>` pipes to q
    /// (Go `AddPipeOffsetLimit`).
    pub fn add_pipe_offset_limit(&mut self, offset: u64, limit: u64) {
        let offset_str = format!("offset {offset}");
        self.must_append_pipe(&offset_str);

        let limit_str = format!("limit {limit}");
        self.must_append_pipe(&limit_str);

        // optimize the query, so the `offset` and `limit` pipes could be joined
        // with the preceding `sort` pipe.
        let pipes = std::mem::take(&mut self.pipes);
        self.pipes = optimize_offset_limit_pipes(pipes);
    }

    /// Port of Go `mustAppendPipe`.
    pub(crate) fn must_append_pipe(&mut self, s: &str) {
        let timestamp = self.get_timestamp();
        let p = crate::parser::parse_pipe::must_parse_pipe(s, timestamp);
        self.pipes.push(p);
    }
}

/// Returns true if there is sense for applying 'last N' optimization for the
/// query on the time range `[start, end]`
/// (Go `CanApplyLastNResultsOptimization`).
pub fn can_apply_last_n_results_optimization(start: i64, end: i64) -> bool {
    end / 2 - start / 2 > NSECS_PER_SECOND
}

/// Port of Go `getFilterTimeRange` (parser.go).
///
/// PORT NOTE: Go type-switches on `*filterAnd` / `*filterTime`; the port
/// dispatches through the `and_children` / `filter_time_range` trait hooks.
fn get_filter_time_range(f: &dyn FilterTrait) -> (i64, i64) {
    if let Some(children) = f.and_children() {
        let mut min_timestamp = i64::MIN;
        let mut max_timestamp = i64::MAX;
        for child in children {
            if let Some((ft_min, ft_max)) = child.filter_time_range() {
                if ft_min > min_timestamp {
                    min_timestamp = ft_min;
                }
                if ft_max < max_timestamp {
                    max_timestamp = ft_max;
                }
            }
        }
        return (min_timestamp, max_timestamp);
    }
    if let Some((min_timestamp, max_timestamp)) = f.filter_time_range() {
        return (min_timestamp, max_timestamp);
    }
    (i64::MIN, i64::MAX)
}

/// Port of Go `addTimeFilter` (parser.go, free function).
fn add_time_filter(
    f: Box<dyn FilterTrait>,
    start: i64,
    end: i64,
    offset: i64,
) -> Box<dyn FilterTrait> {
    // use nanosecond precision for [start, end] time range in order to avoid
    // automatic adjustement of timestamps for its' string representation.
    // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/587
    let mut buf = Vec::new();
    marshal_timestamp_rfc3339_nano_precise_string(&mut buf, start);
    let start_str = String::from_utf8_lossy(&buf).into_owned();
    buf.clear();
    marshal_timestamp_rfc3339_nano_precise_string(&mut buf, end);
    let end_str = String::from_utf8_lossy(&buf).into_owned();

    let min_timestamp = sub_int64_no_overflow(start, offset);
    let max_timestamp = sub_int64_no_overflow(end, offset);
    let string_repr = format!("[{start_str},{end_str}]");
    let ft: Box<dyn FilterTrait> = Box::new(crate::filter_time::new_filter_time(
        min_timestamp,
        max_timestamp,
        &string_repr,
    ));

    let mut f = f;
    if let Some(children) = f.take_and_children() {
        let mut filters = Vec::with_capacity(children.len() + 1);
        filters.push(ft);
        filters.extend(children);
        f = Box::new(crate::filter_and::new_filter_and(filters));
    } else {
        f = Box::new(crate::filter_and::new_filter_and(vec![ft, f]));
    }

    let f = flatten_filters_and(f);

    // Remove `*` filters after adding the `_time` filter, since they are no
    // longer needed.
    remove_star_filters(f)
}

/// Returns the filter expression when `p` is a `filter` pipe.
///
/// PORT NOTE: Go type-switches on `*pipeFilter`; the port classifies by the
/// rendered pipe string (`pipeFilter` is the only pipe rendering as
/// `filter <expr>`), the established render/re-parse divergence.
fn pipe_filter_expr(p: &dyn Pipe) -> Option<String> {
    let s = p.to_string();
    s.strip_prefix("filter ").map(str::to_string)
}

/// Port of Go `mergeFiltersAnd` (parser.go): ANDs `f1` and `f2`, folding
/// existing `filterAnd` children in via the `take_and_children` hook.
fn merge_filters_and(
    mut f1: Box<dyn FilterTrait>,
    mut f2: Box<dyn FilterTrait>,
) -> Box<dyn FilterTrait> {
    let mut filters: Vec<Box<dyn FilterTrait>> = Vec::new();
    if let Some(children) = f1.take_and_children() {
        filters.extend(children);
    } else {
        filters.push(f1);
    }
    if let Some(children) = f2.take_and_children() {
        filters.extend(children);
    } else {
        filters.push(f2);
    }
    Box::new(crate::filter_and::new_filter_and(filters))
}

/// Port of Go `optimizeFilterPipes` (parser.go): merges adjacent
/// `| filter ...` pipes into a single `filter` pipe.
///
/// PORT NOTE: Go merges the filter trees via `mergeFiltersAnd`; the port
/// re-parses the concatenated parenthesized expressions (see
/// `pipe_filter_expr`), which yields the same AND tree.
fn optimize_filter_pipes(mut pipes: Vec<Box<dyn Pipe>>, timestamp: i64) -> Vec<Box<dyn Pipe>> {
    let mut i = 1;
    while i < pipes.len() {
        let (Some(e1), Some(e2)) = (
            pipe_filter_expr(pipes[i - 1].as_ref()),
            pipe_filter_expr(pipes[i].as_ref()),
        ) else {
            i += 1;
            continue;
        };
        let merged = format!("filter ({e1}) ({e2})");
        pipes[i - 1] = crate::parser::parse_pipe::must_parse_pipe(&merged, timestamp);
        pipes.remove(i);
    }
    pipes
}

/// Port of Go `optimizeFilters` (parser.go).
fn optimize_filters(f: Box<dyn FilterTrait>) -> Box<dyn FilterTrait> {
    // flatten nested AND filters
    let f = flatten_filters_and(f);

    // flatten nested OR filters
    let f = flatten_filters_or(f);

    // Substitute '*' prefixFilter with filterNoop in order to avoid reading
    // _msg data.
    let f = remove_star_filters(f);

    // Merge multiple {...} filters into a single one.
    merge_filters_stream(f)
}

/// Port of Go `flattenFiltersAnd` (parser.go).
///
/// PORT NOTE: Go rewrites via `copyFilter` with a visit check; the port
/// flattens recursively through the `take_and_children` hook (same result).
fn flatten_filters_and(mut f: Box<dyn FilterTrait>) -> Box<dyn FilterTrait> {
    if let Some(children) = f.take_or_children() {
        // Recurse into OR children so nested `(a (b c)) or d` forms are
        // flattened too (Go's copyFilter walks the whole tree).
        let children: Vec<Box<dyn FilterTrait>> =
            children.into_iter().map(flatten_filters_and).collect();
        return Box::new(crate::filter_or::new_filter_or(children));
    }
    let Some(children) = f.take_and_children() else {
        return f;
    };
    let mut result_filters: Vec<Box<dyn FilterTrait>> = Vec::with_capacity(children.len());
    for child in children {
        let mut child = flatten_filters_and(child);
        if let Some(grandchildren) = child.take_and_children() {
            result_filters.extend(grandchildren);
        } else {
            result_filters.push(child);
        }
    }
    Box::new(crate::filter_and::new_filter_and(result_filters))
}

/// Port of Go `flattenFiltersOr` (parser.go).
///
/// PORT NOTE: like `flatten_filters_and`, the port flattens recursively
/// through the `take_or_children` hook instead of Go's `copyFilter`.
fn flatten_filters_or(mut f: Box<dyn FilterTrait>) -> Box<dyn FilterTrait> {
    if let Some(children) = f.take_and_children() {
        // Recurse into AND children so nested `(a or (b or c)) d` forms are
        // flattened too (Go's copyFilter walks the whole tree).
        let children: Vec<Box<dyn FilterTrait>> =
            children.into_iter().map(flatten_filters_or).collect();
        return Box::new(crate::filter_and::new_filter_and(children));
    }
    let Some(children) = f.take_or_children() else {
        return f;
    };
    let mut result_filters: Vec<Box<dyn FilterTrait>> = Vec::with_capacity(children.len());
    for child in children {
        let mut child = flatten_filters_or(child);
        if let Some(grandchildren) = child.take_or_children() {
            result_filters.extend(grandchildren);
        } else {
            result_filters.push(child);
        }
    }
    Box::new(crate::filter_or::new_filter_or(result_filters))
}

/// Port of Go `mergeFiltersStream` (parser.go): merges multiple `{...}`
/// filters ANDed at the top level into a single one, moved to the front.
fn merge_filters_stream(mut f: Box<dyn FilterTrait>) -> Box<dyn FilterTrait> {
    let Some(children) = f.take_and_children() else {
        return f;
    };
    let mut fss: Vec<crate::stream_filter::StreamFilter> = Vec::with_capacity(children.len());
    let mut other_filters: Vec<Box<dyn FilterTrait>> = Vec::with_capacity(children.len());
    for mut child in children {
        if let Some(sf) = child.take_stream_filter() {
            fss.push(sf);
        } else {
            other_filters.push(child);
        }
    }
    if fss.is_empty() {
        // Nothing to merge
        return Box::new(crate::filter_and::new_filter_and(other_filters));
    }

    let fss = merge_filters_stream_internal(fss);
    let mut filters: Vec<Box<dyn FilterTrait>> =
        Vec::with_capacity(fss.len() + other_filters.len());
    for sf in fss {
        filters.push(Box::new(crate::filter_stream::new_filter_stream(sf)));
    }
    filters.extend(other_filters);
    Box::new(crate::filter_and::new_filter_and(filters))
}

/// Port of Go `mergeFiltersStreamInternal` (parser.go).
fn merge_filters_stream_internal(
    fss: Vec<crate::stream_filter::StreamFilter>,
) -> Vec<crate::stream_filter::StreamFilter> {
    if fss.len() < 2 {
        return fss;
    }

    if fss.iter().any(|sf| sf.or_filters.len() != 1) {
        // Cannot merge or filters :(
        return fss;
    }

    let mut tfs = Vec::new();
    for mut sf in fss {
        tfs.extend(sf.or_filters.pop().expect("len checked").tag_filters);
    }
    vec![crate::stream_filter::StreamFilter {
        or_filters: vec![crate::stream_filter::AndStreamFilter { tag_filters: tfs }],
    }]
}

/// Port of Go `getStreamIDsFromFilterOr` (parser.go).
fn get_stream_ids_from_filter_or(f: &dyn FilterTrait) -> (Vec<StreamID>, bool) {
    if let Some(children) = f.or_children() {
        let mut stream_ids_filters = 0usize;
        let mut stream_ids: Vec<StreamID> = Vec::new();
        for child in children {
            let Some(ids) = child.stream_ids() else {
                return (Vec::new(), false);
            };
            stream_ids_filters += 1;
            stream_ids.extend_from_slice(ids);
        }
        return (stream_ids, stream_ids_filters > 0);
    }
    if let Some(ids) = f.stream_ids() {
        return (ids.to_vec(), true);
    }
    (Vec::new(), false)
}

/// Port of Go `optimizeOffsetLimitPipes` (parser.go).
pub(crate) fn optimize_offset_limit_pipes(mut pipes: Vec<Box<dyn Pipe>>) -> Vec<Box<dyn Pipe>> {
    loop {
        let pipes_len = pipes.len();
        pipes = optimize_offset_limit_pipes_internal(pipes);
        if pipes.len() == pipes_len {
            return pipes;
        }
    }
}

/// Port of Go `optimizeOffsetLimitPipesInternal` (parser.go).
///
/// PORT NOTE: Go type-switches on `*pipeOffset` / `*pipeLimit` / `*pipeSort`
/// and mutates them in place; the port dispatches through the
/// `offset_pipe_value` / `limit_pipe_value` / `sort_merge_*` trait hooks and
/// rebuilds offset/limit pipes via their constructors.
fn optimize_offset_limit_pipes_internal(mut pipes: Vec<Box<dyn Pipe>>) -> Vec<Box<dyn Pipe>> {
    use crate::pipe_limit::new_pipe_limit;
    use crate::pipe_offset::new_pipe_offset;

    // Replace '| offset X | limit Y' with '| limit X+Y | offset X'.
    // This reduces the number of rows processed by remote storage.
    for i in 0..pipes.len().saturating_sub(1) {
        let Some(offset) = pipes[i].offset_pipe_value() else {
            continue;
        };
        let Some(limit) = pipes[i + 1].limit_pipe_value() else {
            continue;
        };
        pipes[i] = Box::new(new_pipe_limit(limit + offset));
        pipes[i + 1] = Box::new(new_pipe_offset(offset));
    }

    // Merge 'offset X | offset Y' into 'offset X+Y'.
    let mut i = 1;
    while i < pipes.len() {
        let (Some(o1), Some(o2)) = (
            pipes[i - 1].offset_pipe_value(),
            pipes[i].offset_pipe_value(),
        ) else {
            i += 1;
            continue;
        };
        pipes[i - 1] = Box::new(new_pipe_offset(o1 + o2));
        pipes.remove(i);
    }

    // Merge 'limit N | limit M' into 'limit min(N, M)'.
    let mut i = 1;
    while i < pipes.len() {
        let (Some(l1), Some(l2)) = (pipes[i - 1].limit_pipe_value(), pipes[i].limit_pipe_value())
        else {
            i += 1;
            continue;
        };
        pipes[i - 1] = Box::new(new_pipe_limit(l1.min(l2)));
        pipes.remove(i);
    }

    // Replace '| limit X | offset Y' with 'limit 0' if Y >= X.
    let mut i = 1;
    while i < pipes.len() {
        let (Some(limit), Some(offset)) = (
            pipes[i - 1].limit_pipe_value(),
            pipes[i].offset_pipe_value(),
        ) else {
            i += 1;
            continue;
        };
        if offset < limit {
            i += 1;
            continue;
        }
        pipes[i - 1] = Box::new(new_pipe_limit(0));
        pipes.remove(i);
    }

    // Remove `offset 0`.
    let mut i = 0;
    while i < pipes.len() {
        if pipes[i].offset_pipe_value() != Some(0) {
            i += 1;
            continue;
        }
        pipes.remove(i);
    }

    // Merge '| sort ... | offset ... | limit ...' into '| sort ... offset ... limit ...'.
    pipes = optimize_sort_offset_pipes(pipes);
    pipes = optimize_sort_limit_pipes(pipes);

    pipes
}

/// Port of Go `optimizeSortOffsetPipes` (parser.go).
fn optimize_sort_offset_pipes(mut pipes: Vec<Box<dyn Pipe>>) -> Vec<Box<dyn Pipe>> {
    use crate::pipe_limit::new_pipe_limit;

    // Merge 'sort ... | offset ...' into 'sort ... offset ...'
    let mut i = 1;
    while i < pipes.len() {
        let Some(offset) = pipes[i].offset_pipe_value() else {
            i += 1;
            continue;
        };
        match pipes[i - 1].sort_merge_offset(offset) {
            None => {
                i += 1;
            }
            Some(true) => {
                pipes.remove(i);
            }
            Some(false) => {
                pipes[i - 1] = Box::new(new_pipe_limit(0));
                pipes.remove(i);
            }
        }
    }
    pipes
}

/// Port of Go `optimizeSortLimitPipes` (parser.go).
fn optimize_sort_limit_pipes(mut pipes: Vec<Box<dyn Pipe>>) -> Vec<Box<dyn Pipe>> {
    // Merge 'sort ... | limit ...' into 'sort ... limit ...'
    let mut i = 1;
    while i < pipes.len() {
        let Some(limit) = pipes[i].limit_pipe_value() else {
            i += 1;
            continue;
        };
        if limit == 0 {
            // The `limit 0` pipe makes the preceding `sort` pipe a no-op.
            if pipes[i - 1].sort_merge_offset(0).is_some() {
                pipes.remove(i - 1);
            } else {
                i += 1;
            }
            continue;
        }
        if pipes[i - 1].sort_merge_limit(limit) {
            pipes.remove(i);
        } else {
            i += 1;
        }
    }
    pipes
}

/// Port of Go `removeStarFilters` (parser.go).
///
/// Rewrites match-all `*` filters to `FilterNoop`, collapses OR filters with a
/// match-all branch to `FilterNoop`, and drops match-all branches from AND
/// filters. All rewrites are semantics-preserving (`noop` ≡ `*` ≡ match-all);
/// they exist so the search path skips reading the `_msg` column entirely.
fn remove_star_filters(mut f: Box<dyn FilterTrait>) -> Box<dyn FilterTrait> {
    use crate::filter_noop::new_filter_noop;

    if let Some(children) = f.take_or_children() {
        let children: Vec<Box<dyn FilterTrait>> =
            children.into_iter().map(remove_star_filters).collect();
        if children.iter().any(|c| c.is_match_all()) {
            return Box::new(new_filter_noop());
        }
        return Box::new(crate::filter_or::new_filter_or(children));
    }
    if let Some(children) = f.take_and_children() {
        let mut children: Vec<Box<dyn FilterTrait>> = children
            .into_iter()
            .map(remove_star_filters)
            .filter(|c| !c.is_match_all())
            .collect();
        return match children.len() {
            0 => Box::new(new_filter_noop()),
            1 => children.pop().expect("len checked"),
            _ => Box::new(crate::filter_and::new_filter_and(children)),
        };
    }
    if f.is_match_all() {
        return Box::new(new_filter_noop());
    }
    f
}

/// Parses `s` at the current time (Go `ParseQuery`).
#[allow(non_snake_case)]
pub fn ParseQuery(s: &str) -> Result<Query, String> {
    let timestamp = now_unix_nano();
    ParseQueryAtTimestamp(s, timestamp)
}

/// Parses `s` in the context of `timestamp` (Go `ParseQueryAtTimestamp`).
#[allow(non_snake_case)]
pub fn ParseQueryAtTimestamp(s: &str, timestamp: i64) -> Result<Query, String> {
    let mut lex = Lexer::new_at(s, timestamp);
    let mut q = parse_query(&mut lex)?;
    if !lex.is_end() {
        return Err(format!(
            "unexpected unparsed tail after [{q}]; context: [{}]; tail: [{}{}]",
            lex.context(),
            lex.raw_token(),
            lex.tail()
        ));
    }
    q.optimize();
    q.init_stats_rate_func_steps();
    Ok(q)
}

fn now_unix_nano() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Port of Go `parseQueryInParens`.
pub(crate) fn parse_query_in_parens(lex: &mut Lexer) -> Result<Query, String> {
    if !lex.is_keyword(&["("]) {
        return Err("missing '('".to_string());
    }
    lex.next_token();
    let q = parse_query(lex)?;
    if !lex.is_keyword(&[")"]) {
        return Err(format!("missing ')' after '({q}'"));
    }
    lex.next_token();
    Ok(q)
}

/// Port of Go `parseQuery`.
pub(crate) fn parse_query(lex: &mut Lexer) -> Result<Query, String> {
    let mut opts = QueryOptions::default();
    parse_query_options(&mut opts, lex).map_err(|e| {
        format!(
            "cannot parse query options: {e}; context: [{}]; see https://docs.victoriametrics.com/victorialogs/logsql/#query-options",
            lex.context()
        )
    })?;

    // PORT NOTE: Go pushes `opts` onto the lexer's queryOptions stack so nested
    // subqueries inherit defaults. Option inheritance into subqueries is
    // deferred (it would require cloning `Box<dyn Filter>` for `global_filter`).

    let f = parse_filter(lex, true).map_err(|e| format!("{e}; context: [{}]", lex.context()))?;

    let time_offset = opts.time_offset;
    let mut q = Query {
        f: update_filter_with_time_offset(f, time_offset),
        opts,
        pipes: Vec::new(),
        timestamp: lex.current_timestamp(),
    };

    if lex.is_keyword(&["|"]) {
        lex.next_token();
        let pipes = parse_pipes(lex).map_err(|e| format!("{e}; context: [{}]", lex.context()))?;
        q.pipes = pipes;
    }

    if lex.is_keyword(&[";"]) {
        lex.next_token();
    }

    Ok(q)
}

/// Port of Go `updateFilterWithTimeOffset`.
///
/// PORT NOTE: the `time_offset != 0` branch requires `copyFilter` to walk the
/// filter tree and shift `filterTime`/`filterDayRange`/`filterWeekRange`
/// bounds; that downcast machinery is deferred, so a non-zero offset leaves the
/// filter unchanged (only reachable via `options(time_offset=...)`).
fn update_filter_with_time_offset(
    f: Box<dyn FilterTrait>,
    _time_offset: i64,
) -> Box<dyn FilterTrait> {
    f
}

/// Port of Go `strconv.ParseBool`.
fn parse_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!("cannot parse {} as boolean", go_quote(s))),
    }
}

/// Port of Go `parseQueryOptions`.
fn parse_query_options(dst_opts: &mut QueryOptions, lex: &mut Lexer) -> Result<(), String> {
    // PORT NOTE: inheritance of parent options (Go's getQueryOptions) is
    // deferred; a top-level query has no parent, matching the common case.
    dst_opts.need_print = false;

    if !lex.is_keyword(&["options"]) {
        return Ok(());
    }
    lex.next_token();

    if !lex.is_keyword(&["("]) {
        return Err(
            "missing '(' after 'options' keyword; wrap 'options' into quotes if you are searching for this word in the log message".to_string(),
        );
    }
    lex.next_token();

    loop {
        if lex.is_keyword(&[")"]) {
            lex.next_token();
            return Ok(());
        }

        let option_name = lex
            .next_compound_token()
            .map_err(|e| format!("cannot parse the option name inside 'options': {e}"))?;
        if !lex.is_keyword(&["="]) {
            return Err(format!(
                "missing '=' after {} key; got {} instead",
                go_quote(&option_name),
                go_quote(&lex.token)
            ));
        }
        lex.next_token();

        match option_name.as_str() {
            "concurrency" => {
                let v = lex.next_compound_token().map_err(|e| {
                    format!("cannot read 'concurrency' value inside 'options': {e}")
                })?;
                let n = try_parse_uint64(&v).ok_or_else(|| {
                    format!(
                        "cannot parse 'concurrency={}' option as unsigned integer",
                        go_quote(&v)
                    )
                })?;
                dst_opts.concurrency = n as u32;
                dst_opts.need_print = true;
            }
            "parallel_readers" => {
                let v = lex.next_compound_token().map_err(|e| {
                    format!("cannot read 'parallel_readers' value inside 'options': {e}")
                })?;
                let n = try_parse_uint64(&v).ok_or_else(|| {
                    format!(
                        "cannot parse 'parallel_readers={}' option as unsigned integer",
                        go_quote(&v)
                    )
                })?;
                dst_opts.parallel_readers = n as u32;
                dst_opts.need_print = true;
            }
            "ignore_global_time_filter" => {
                let v = lex.next_compound_token().map_err(|e| {
                    format!("cannot read 'ignore_global_time_filter' value inside 'options': {e}")
                })?;
                let b = parse_bool(&v).map_err(|e| {
                    format!(
                        "cannot parse 'ignore_global_time_filter={}' option as boolean: {e}",
                        go_quote(&v)
                    )
                })?;
                dst_opts.ignore_global_time_filter = Some(b);
                dst_opts.need_print = true;
            }
            "allow_partial_response" => {
                let v = lex.next_compound_token().map_err(|e| {
                    format!("cannot read 'allow_partial_response' value inside 'options': {e}")
                })?;
                let b = parse_bool(&v).map_err(|e| {
                    format!(
                        "cannot parse 'allow_partial_response={}' option as boolean: {e}",
                        go_quote(&v)
                    )
                })?;
                dst_opts.allow_partial_response = Some(b);
                dst_opts.need_print = true;
            }
            "time_offset" => {
                let v = lex.next_compound_token().map_err(|e| {
                    format!("cannot read 'time_offset' value inside 'options': {e}")
                })?;
                let d = try_parse_duration(&v).ok_or_else(|| {
                    format!(
                        "cannot parse 'time_offset={}' option as duration",
                        go_quote(&v)
                    )
                })?;
                dst_opts.time_offset = d;
                dst_opts.time_offset_str = v;
                dst_opts.need_print = true;
            }
            "global_filter" => {
                let q = parse_query_in_parens(lex).map_err(|e| {
                    format!("cannot parse global_filter at 'options'; it must have the the following format: global_filter=(_time:5m); error: {e}")
                })?;
                if !q.pipes.is_empty() {
                    return Err(format!(
                        "global_filter at 'options' cannot contain pipes; it must contain only filters; got global_filter=({q})"
                    ));
                }
                dst_opts.global_filter = Some(q.f);
                dst_opts.need_print = true;
            }
            _ => {
                return Err(format!(
                    "unexpected option inside 'options': {}",
                    go_quote(&option_name)
                ));
            }
        }

        if lex.is_keyword(&[")"]) {
            lex.next_token();
            return Ok(());
        }
        if !lex.is_keyword(&[","]) {
            return Err(format!(
                "unexpected token inside the 'options(...)': {}; want ',' or ')'",
                go_quote(&lex.token)
            ));
        }
        lex.next_token();
    }
}

// ---------------------------------------------------------------------------
// Public `Filter` wrapper (Go `Filter` struct)
// ---------------------------------------------------------------------------

/// A parsed LogsQL filter (Go `Filter`).
pub struct Filter {
    f: Option<Box<dyn FilterTrait>>,
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.f {
            Some(inner) => write!(f, "{}", inner.to_string()),
            None => Ok(()),
        }
    }
}

impl Filter {
    /// Returns true if the filter matches a row (Go `(*Filter).MatchRow`).
    pub fn match_row(&self, row: &[crate::rows::Field]) -> bool {
        match &self.f {
            Some(inner) => inner.match_row(row),
            None => false,
        }
    }
}

/// Parses a LogsQL filter (Go `ParseFilter`).
#[allow(non_snake_case)]
pub fn ParseFilter(s: &str) -> Result<Filter, String> {
    ParseFilterAtTimestamp(s, now_unix_nano())
}

/// Parses a LogsQL filter at `timestamp` (Go `ParseFilterAtTimestamp`).
#[allow(non_snake_case)]
pub fn ParseFilterAtTimestamp(s: &str, timestamp: i64) -> Result<Filter, String> {
    let q = ParseQueryAtTimestamp(s, timestamp)?;
    if !q.pipes.is_empty() {
        let pipes: Vec<String> = q.pipes.iter().map(|p| p.to_string()).collect();
        return Err(format!(
            "unexpected pipes after the filter [{}]; pipes: {}",
            q.f.to_string(),
            pipes.join(", ")
        ));
    }
    Ok(Filter { f: Some(q.f) })
}

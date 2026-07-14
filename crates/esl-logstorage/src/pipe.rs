//! Pipe dispatch contract for the LogsQL `|` pipe chain.
//!
//! Port of the `pipe` / `pipeProcessor` interfaces from Go's `pipe.go`,
//! extracted into their own module so the ~56 `pipe_*.go` ports share one fixed
//! contract (as `filter.rs` and `stats.rs` do for their layers).
//!
//! # Dispatch (READ BEFORE PORTING A `pipe_*.go` FILE)
//!
//! A query is `filter | pipe1 | pipe2 | ...`. Each pipe is a struct that
//! `impl Pipe`; a running query builds a chain of `PipeProcessor`s, and each
//! processor **pushes** blocks to the next one (`pp_next`). This mirrors Go's
//! push model exactly.
//!
//! ## `Pipe` (planning) vs `PipeProcessor` (execution)
//! `Pipe` is the parsed, immutable description (shared `&self`, `Send + Sync`).
//! `Pipe::new_pipe_processor` creates a `PipeProcessor` wired to write to
//! `pp_next`. A `PipeProcessor`'s `write_block` is called **concurrently** from
//! worker threads (`worker_id` in `0..concurrency`), so a processor keeps
//! per-worker state (see [`crate::atomicutil`]-style sharding) behind shared
//! refs and only merges in `flush`. Hence `PipeProcessor: Send + Sync` and its
//! methods take `&self`.
//!
//! ## `&mut BlockResult`
//! `write_block` takes `&mut BlockResult` — Go says "it is OK to modify br
//! contents inside writeBlock", and the `BlockResult` accessors are `&mut self`
//! anyway. A processor must NOT hold a reference to `br` after `write_block`
//! returns (the caller reuses it); copy out what you need.
//!
//! # PORT NOTES — deliberately trimmed for single-node
//! The following `pipe`-interface method is **omitted** from this trait
//! because it only serves surfaces that stay deferred:
//!   * `visitSubqueries` — only needed by `Query.AddTimeFilter`/
//!     `AddExtraFilters`/`optimize` subquery propagation, which stays deferred
//!     (see the PORT NOTEs in `parser::query`).
//!
//! `splitToRemoteAndLocal` (the cluster query planner hook) IS ported — see
//! the marked block at the end of the trait and `net_query_runner.rs`.
//!
//! Subquery execution (`in(<subquery>)` filters, `join`/`union` subqueries) is
//! ported: `initFilterInValues`/`hasFilterInWithQuery` and the
//! `initJoinMap`/`initUnionQuery` type-switch targets are trait hooks below,
//! driven by `storage_search::init_subqueries` before the search starts.
//!
//! The `cancel func()` argument of Go's `newPipeProcessor` is folded into the
//! shared `stop` token: a processor signals "stop sending" by setting it.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::block_result::BlockResult;
use crate::prefix_filter;

/// Executes a `union` subquery (given as rendered query text) and streams its
/// block results to the given processor. Port of Go `runUnionQueryFunc`; wired
/// into `union` pipes by `storage_search::init_subqueries` via
/// [`Pipe::init_union_query`].
pub type RunUnionQueryFn = Arc<
    dyn Fn(
            &str,
            Arc<dyn PipeProcessor>,
            Option<Arc<std::sync::atomic::AtomicBool>>,
        ) -> Result<(), String>
        + Send
        + Sync,
>;

/// The `by (...)` / result-field structure of a `| stats ...` pipe, as needed
/// by `Query::get_stats_labels*` (Go downcasts to `*pipeStats` and reads
/// `byFields` / `funcs` directly).
///
/// `pub` because it appears in the `pub trait Pipe` surface; it is not
/// re-exported from the crate root.
pub struct StatsPipeFields {
    /// Names of the `by (...)` fields (Go `ps.byFields[i].name`; raw bytes).
    pub by_fields: Vec<Vec<u8>>,
    /// `(result_name, is_row_label)` per stats function, where `is_row_label`
    /// is true for `row_any` / `row_min` / `row_max` (Go type-switches on
    /// `statsRowAny` / `statsRowMin` / `statsRowMax`).
    pub funcs: Vec<(Vec<u8>, bool)>,
}

/// How a pipe placed after the last `| stats ...` pipe transforms the stats
/// label/metric field sets (Go: the big type-switch inside
/// `Query.GetStatsLabelsAddGroupingByTime`).
///
/// PORT NOTE: Go downcasts each trailing pipe; trait objects have no
/// downcasting, so each allowed pipe describes itself via
/// [`Pipe::stats_labels_tail_op`] and the shared transform logic lives in
/// `parser::query_stats`.
///
/// `pub` because it appears in the `pub trait Pipe` surface; it is not
/// re-exported from the crate root.
pub enum StatsTailOp {
    /// The pipe does not change the set of fields
    /// (`pipeFilter`, `pipeFirst`, `pipeLast`, `pipeSort`).
    Keep,
    /// `pipeLimit` / `pipeOffset`: allowed for instant queries only
    /// (disallowed when `step > 0`).
    OffsetLimit,
    /// `pipeRunningStats` (also covers `total_stats` via `is_total`).
    RunningStats {
        by_fields: Vec<Vec<u8>>,
        is_total: bool,
        result_names: Vec<Vec<u8>>,
    },
    /// `pipeMath`: adds the entries' result fields as metrics.
    Math { result_fields: Vec<Vec<u8>> },
    /// `pipeFields`: keeps only the matching fields.
    Fields { field_filters: Vec<Vec<u8>> },
    /// `pipeDelete`: drops the matching fields.
    Delete { field_filters: Vec<Vec<u8>> },
    /// `pipeCopy`: copies fields from `src` filters to `dst` filters.
    Copy {
        src: Vec<Vec<u8>>,
        dst: Vec<Vec<u8>>,
    },
    /// `pipeRename`: renames fields from `src` filters to `dst` filters.
    Rename {
        src: Vec<Vec<u8>>,
        dst: Vec<Vec<u8>>,
    },
    /// `pipeFormat`: generates an additional label field.
    Format { result_field: Vec<u8> },
    /// `pipeUnpackJSON`: generates additional label fields from `fields (...)`.
    UnpackJson { field_filters: Vec<Vec<u8>> },
}

/// Return type of [`Pipe::split_to_remote_and_local`]: the remote pipe (if
/// any) and the pipes to execute locally (Go returns `(pipe, []pipe)`).
pub type SplitPipesResult = (Option<Box<dyn Pipe>>, Vec<Box<dyn Pipe>>);

/// A parsed pipe (`| stats ...`, `| sort ...`, `| fields ...`, ...).
///
/// Port of Go's unexported `pipe` interface (single-node subset).
pub trait Pipe: Send + Sync {
    /// String representation of the pipe (Go `String()`).
    fn to_string(&self) -> String;

    /// Updates `pf` with the fields this pipe needs / drops at its input
    /// (Go `updateNeededFields`).
    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter);

    /// Creates a processor that writes its output to `pp_next`
    /// (Go `newPipeProcessor`). `concurrency` is the number of worker threads
    /// that may call [`PipeProcessor::write_block`] in parallel; `stop` is the
    /// shared cancellation token (also used to signal "stop sending" upstream).
    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor>;

    /// Whether this pipe can be used in live tailing (Go `canLiveTail`).
    fn can_live_tail(&self) -> bool {
        false
    }

    /// Whether this pipe can return the last N results ordered by `_time` desc,
    /// i.e. it does not modify `_time` (Go `canReturnLastNResults`).
    fn can_return_last_n_results(&self) -> bool {
        false
    }

    /// True for a `sort by (_time) desc limit N` pipe without partitions
    /// (not in Go). When such a pipe consumes the search output directly, the
    /// block scheduler feeds blocks newest-first so the pipe's top-N heap
    /// converges after the first block and skips the rest via its
    /// monotone-timestamps break.
    fn is_desc_time_topk(&self) -> bool {
        false
    }

    /// Whether this pipe emits output fields in a fixed order
    /// (Go `isFixedOutputFieldsOrder`).
    fn is_fixed_output_fields_order(&self) -> bool {
        false
    }

    /// Whether this pipe (recursively) contains an `in(subquery)` filter
    /// (Go `hasFilterInWithQuery`). Defaults to false; the pipes that can embed
    /// filters override it.
    fn has_filter_in_with_query(&self) -> bool {
        false
    }

    /// Resolves `in(<subquery>)` filters embedded in this pipe by substituting
    /// literal values obtained via `get_values(q_text, q_field_name)`
    /// (Go `initFilterInValues`).
    ///
    /// PORT NOTE: Go returns a new pipe (sharing unchanged parts); the Rust
    /// port rewrites the (query-owned) pipe in place. `timestamp` is the outer
    /// query timestamp, used to re-parse `Arc`-shared `if (...)` filters from
    /// their rendered text before rewriting (see
    /// `storage_search::init_filter_in_values_for_shared_filter`).
    fn init_filter_in_values(
        &mut self,
        _get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
        _timestamp: i64,
    ) -> Result<(), String> {
        Ok(())
    }

    /// True for `join` pipes (Go `hasJoinPipes`' `*pipeJoin` type-switch).
    fn is_join_pipe(&self) -> bool {
        false
    }

    /// Builds the join map of a `join` pipe, executing its subquery via
    /// `get_join_rows(q_text)` when the pipe was built from a subquery
    /// (Go `pipeJoin.initJoinMap`, dispatched from `initJoinMaps`).
    /// Default: no-op for all other pipes.
    fn init_join_map(
        &mut self,
        _get_join_rows: &mut crate::storage_search::GetJoinRowsFn<'_>,
    ) -> Result<(), String> {
        Ok(())
    }

    /// True for `union` pipes (Go `hasUnionPipes`' `*pipeUnion` type-switch).
    fn is_union_pipe(&self) -> bool {
        false
    }

    /// Wires the run-query callback into a `union` pipe so its processor can
    /// execute the union subquery at `flush` (Go `pipeUnion.initUnionQuery`
    /// with `eagerExecute == false`, dispatched from `initUnionQueries`).
    /// Default: no-op for all other pipes.
    ///
    /// PORT NOTE: Go's `eagerExecute` mode (cluster-only, used by
    /// `NewNetQueryRunner`) is the separate [`Pipe::init_union_query_eager`]
    /// hook below.
    fn init_union_query(&mut self, _run_query: &RunUnionQueryFn) -> Result<(), String> {
        Ok(())
    }

    /// True for `uniq` pipes (Go `isLastPipeUniq`'s `*pipeUniq` type-switch).
    fn is_uniq_pipe(&self) -> bool {
        false
    }

    /// True for `stream_context` pipes (Go `initStreamContextPipes`'
    /// `*pipeStreamContext` type-switch).
    fn is_stream_context_pipe(&self) -> bool {
        false
    }

    /// Wires the surrounding-logs re-execution seam plus the rendered
    /// `toFieldsFilters` tail into a `stream_context` pipe
    /// (Go `pipeStreamContext.withRunQuery`, dispatched from
    /// `initStreamContextPipes`). Default: no-op for all other pipes.
    fn init_stream_context_query(
        &mut self,
        _run_query: &crate::pipe_stream_context::RunQueryFn,
        _fields_filter: &str,
    ) {
    }

    /// `parseInQuery` support (Go `getFieldNameFromPipes` type-switch): for
    /// `fields`/`uniq` pipes, the single field name whose values an
    /// `in(<subquery>)` filter yields (or an error when the pipe has more than
    /// one field). `None` (the default) means the pipe cannot terminate an
    /// `in(<subquery>)` query.
    fn in_query_field_name(&self) -> Option<Result<Vec<u8>, String>> {
        None
    }

    /// `Query::get_last_n_results_query` support (Go `getOffsetLimitFromPipe`):
    /// returns `(offset, limit)` when this pipe is a `sort by (_time) desc`
    /// style pipe eligible for the last-N optimization.
    ///
    /// PORT NOTE: Go type-switches on `*pipeSort` / `*pipeFirst` / `*pipeLast`;
    /// trait objects have no downcasting, so those pipes override this hook
    /// instead (delegating to `get_offset_limit_from_pipe_sort`).
    fn get_offset_limit(&self) -> Option<(u64, u64)> {
        None
    }

    /// `Query::get_last_n_results_query` support: true for `fields` and
    /// `delete` pipes.
    ///
    /// PORT NOTE: Go type-switches on `*pipeFields` / `*pipeDelete` for the
    /// trailing-pipes scan inside `GetLastNResultsQuery`; the classification
    /// lives on the trait with a conservative default (see `get_offset_limit`).
    fn is_fields_or_delete_pipe(&self) -> bool {
        false
    }

    /// `optimizeOffsetLimitPipes` support: returns the offset of a `PipeOffset`.
    ///
    /// PORT NOTE: Go type-switches on `*pipeOffset`; the accessor lives on the
    /// trait with a `None` default (see `get_offset_limit`).
    fn offset_pipe_value(&self) -> Option<u64> {
        None
    }

    /// `optimizeOffsetLimitPipes` support: returns the limit of a `PipeLimit`.
    ///
    /// PORT NOTE: Go type-switches on `*pipeLimit` (see `offset_pipe_value`).
    fn limit_pipe_value(&self) -> Option<u64> {
        None
    }

    /// `optimizeSortOffsetPipes` support: merges a trailing `| offset N` into
    /// a `PipeSort`. Returns `None` for non-sort pipes, `Some(true)` when the
    /// offset was merged and `Some(false)` when the sort pipe must be replaced
    /// with `limit 0` (Go: `offset >= ps.limit`).
    ///
    /// PORT NOTE: Go mutates `*pipeSort` in place after a type switch (see
    /// `offset_pipe_value`).
    fn sort_merge_offset(&mut self, _offset: u64) -> Option<bool> {
        None
    }

    /// `optimizeSortLimitPipes` support: merges a trailing `| limit N` into a
    /// `PipeSort`. Returns true when merged (see `sort_merge_offset`).
    fn sort_merge_limit(&mut self, _limit: u64) -> bool {
        false
    }

    /// `optimizeUniqLimitPipes` support: merges a trailing `| limit N` into a
    /// `PipeUniq` (`uniq ... limit N`). Returns true when merged.
    ///
    /// PORT NOTE: Go mutates `*pipeUniq` in place after a type switch (see
    /// `sort_merge_limit`).
    fn uniq_merge_limit(&mut self, _limit: u64) -> bool {
        false
    }

    /// `optimizeNoSubqueries` support (Go's `*pipeFieldNames` type switch):
    /// called on the query's first pipe after optimization; `PipeFieldNames`
    /// sets `is_first_pipe`, enabling the columns-header fast path in its
    /// processor. Default: no-op for all other pipes.
    fn mark_first_pipe(&mut self) {}

    /// `Query::add_count_by_time_pipe` support (Go `isPipeSafeForHits`):
    /// whether hits grouped by `_time` may be calculated after this pipe.
    ///
    /// PORT NOTE: Go additionally sanitizes the subquery of a `pipeUnion` in
    /// place (`t.q.dropPipesUnsafeForHits()`); the Rust `PipeUnion` stores its
    /// subquery as rendered text, so the hook takes `&mut self` plus the query
    /// `timestamp` to re-parse/sanitize/re-render that text.
    fn is_safe_for_hits(&mut self, _timestamp: i64) -> bool {
        self.can_return_last_n_results()
    }

    /// `Query::get_stats_labels*` support: the `by (...)`/result fields of a
    /// `| stats ...` pipe; `None` for every other pipe (Go downcasts to
    /// `*pipeStats`).
    fn stats_pipe_fields(&self) -> Option<StatsPipeFields> {
        None
    }

    /// `Query::get_stats_labels*` support (Go `pipeStats.addByTimeField`):
    /// adds `_time:step offset <offset>` in front of the `by (...)` fields of
    /// a `| stats ...` pipe. Default: no-op for all other pipes.
    fn stats_add_by_time_field(&mut self, _step: i64, _offset: i64) {}

    /// `Query::init_stats_rate_func_steps` support (Go
    /// `pipeStats.initRateFuncs`/`initRateFuncsFromTimeBucket`): sets the
    /// per-second step on the `rate()`/`rate_sum()` funcs of a `| stats ...`
    /// pipe, preferring an explicit `_time` bucket over the query time range.
    /// Default: no-op for all other pipes.
    fn init_stats_rate_funcs(&mut self, _step: i64) {}

    /// `Query::get_stats_labels*` support (Go `Query.addPartitionByTime`):
    /// adds `partition by (_time)` to `sort` / `first` / `last` pipes.
    /// Default: no-op for all other pipes.
    fn add_partition_by_time(&mut self, _step: i64) {}

    /// `Query::get_stats_labels*` support: how this pipe transforms the stats
    /// label/metric fields when placed after the last `| stats ...` pipe.
    /// `None` (default) means the pipe is not allowed there.
    fn stats_labels_tail_op(&self) -> Option<StatsTailOp> {
        None
    }

    /// `Query::get_fixed_fields` support: true for the pipes that do not
    /// change the fixed fields (`pipeSort`, `pipeLimit`, `pipeOffset` in Go's
    /// `getFixedFields` type-switch).
    fn fixed_fields_transparent(&self) -> bool {
        false
    }

    /// `Query::get_fixed_fields` support: the fixed set of output fields for
    /// `stats` (`pipeStats.resultFields`) and wildcard-free `fields`
    /// (`pipeFields.resultFields`) pipes; `None` otherwise (including a
    /// `fields` pipe with wildcard filters — Go returns `ok == false` there,
    /// which `getFixedFields` maps to the same "cannot detect" result as the
    /// default case).
    fn fixed_result_fields(&self) -> Option<Vec<Vec<u8>>> {
        None
    }

    /// `Query::get_fixed_fields` support (Go `pipeSort.adjustResultFieldsOrder`):
    /// reorders `fields` according to the sort pipe's rank/by fields. `None`
    /// for non-sort pipes.
    fn sort_adjust_result_fields_order(&self, _fields: &[Vec<u8>]) -> Option<Vec<Vec<u8>>> {
        None
    }

    /// `Query::is_fixed_output_fields_order` support: for `union`/`join`
    /// pipes with a subquery, whether the subquery itself has a fixed output
    /// fields order; `None` for all other pipes (and for `union`/`join` with
    /// inline `rows(...)`).
    ///
    /// PORT NOTE: Go reads `t.q.IsFixedOutputFieldsOrder()` from the parsed
    /// subquery; the Rust pipes store the subquery as rendered text, so the
    /// implementations re-parse that text.
    fn subquery_is_fixed_output_fields_order(&self) -> Option<bool> {
        None
    }

    // ========================================================================
    // Cluster query-split hooks (net_query_runner port). Keep this block at
    // the END of the trait.
    // ========================================================================

    /// Splits this pipe into a remotely executed part and locally executed
    /// parts (Go `splitToRemoteAndLocal`).
    ///
    /// The `timestamp` is the query execution timestamp.
    ///
    /// * If the pipe can be executed remotely in full, the returned local
    ///   pipes must be empty.
    /// * If the pipe cannot be executed remotely, the returned remote pipe
    ///   must be `None`.
    /// * If the pipe must be executed remotely and locally, both returned
    ///   remote and local pipes must be non-empty.
    ///
    /// PORT NOTE: Go returns the pipe itself (a shared pointer) when a side
    /// keeps the pipe unchanged; boxed trait objects cannot be shared, so the
    /// implementations clone the pipe — usually via [`clone_pipe`]
    /// (render + re-parse, the established `Query::clone` divergence).
    fn split_to_remote_and_local(&self, timestamp: i64) -> SplitPipesResult;

    /// Eagerly executes a `union` pipe's subquery via `get_rows(q_text)` and
    /// replaces it with inline `rows(...)` (Go `pipeUnion.initUnionQuery` with
    /// `eagerExecute == true`; `NetQueryRunner` needs this so subquery results
    /// propagate to remote storage nodes). Default: no-op for all other pipes.
    ///
    /// PORT NOTE: Go folds the eager flag into `initUnionQuery`; the port
    /// keeps [`Pipe::init_union_query`] (the lazy single-node wiring) intact
    /// and adds the eager path as a separate hook.
    fn init_union_query_eager(
        &mut self,
        _get_rows: &mut crate::storage_search::GetJoinRowsFn<'_>,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Port of Go `pipe.visitSubqueries`: calls `visit` for every subquery
    /// embedded in this pipe (recursively). Overridden by the pipes that hold
    /// a subquery (`pipe_join`, `pipe_union`); leaves keep the no-op default.
    ///
    /// PORT NOTE: Go visits the parsed `*Query`; the join/union pipes store it
    /// as rendered text, so the overrides re-parse at `timestamp`, visit, and
    /// re-render. `pipe_filter`'s `Arc<dyn Filter>` and the `if (...)` filters
    /// of the iff-holding pipes are not visited — reachable only when the
    /// filter itself contains `in(subquery)`, a deferred ledger sub-case.
    fn visit_subqueries_mut(
        &mut self,
        _timestamp: i64,
        _visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
    }
}

/// Parses a single pipe from `s` at the given `timestamp`, panicking on
/// malformed input (Go `mustParsePipe`). Only for pipe text generated by the
/// pipes themselves (`splitToRemoteAndLocal`).
pub(crate) fn must_parse_pipe(s: &str, timestamp: i64) -> Box<dyn Pipe> {
    let mut pipes = must_parse_pipes(s, timestamp);
    if pipes.len() != 1 {
        esl_common::panicf!("BUG: expecting a single pipe in [{s}]; got {}", pipes.len());
    }
    pipes.remove(0)
}

/// Parses a `|`-separated pipe chain from `s` at the given `timestamp`,
/// panicking on malformed input (Go `mustParsePipes`).
pub(crate) fn must_parse_pipes(s: &str, timestamp: i64) -> Vec<Box<dyn Pipe>> {
    use crate::parser::lexer_ext::LexerExt;

    let mut lex = crate::stream_filter::Lexer::new_at(s, timestamp);
    let pipes = match crate::parser::parse_pipe::parse_pipes(&mut lex) {
        Ok(pipes) => pipes,
        Err(err) => {
            esl_common::panicf!("BUG: cannot parse [{s}]: {err}");
            unreachable!()
        }
    };
    if !lex.is_end() {
        esl_common::panicf!("BUG: unexpected tail left after parsing [{s}]");
    }
    pipes
}

/// Clones a pipe by re-parsing its rendered text (see the
/// [`Pipe::split_to_remote_and_local`] PORT NOTE).
pub(crate) fn clone_pipe(p: &dyn Pipe, timestamp: i64) -> Box<dyn Pipe> {
    must_parse_pipe(&p.to_string(), timestamp)
}

/// The execution half of a pipe: accepts blocks and pushes results downstream.
///
/// Port of Go's unexported `pipeProcessor` interface. `write_block` is called
/// concurrently by worker threads; `flush` runs once after all workers stop.
pub trait PipeProcessor: Send + Sync {
    /// Search-side block pruning (not in Go): called by the block scheduler
    /// with a block's timestamp range BEFORE the block is read and searched.
    /// Returning true proves the block cannot contribute to the result (e.g.
    /// a full desc-time top-N heap whose root beats `max_timestamp`), so the
    /// search skips the block without reading it. Only consulted on the head
    /// processor of the pipeline.
    fn block_skip_check(
        &self,
        _worker_id: usize,
        _min_timestamp: i64,
        _max_timestamp: i64,
    ) -> bool {
        false
    }

    /// Processes one block from worker `worker_id` and writes any output to the
    /// next processor (Go `writeBlock`). Must not retain `br` after returning.
    fn write_block(&self, worker_id: usize, br: &mut BlockResult);

    /// Flushes all accumulated data to the next processor after every worker
    /// has stopped (Go `flush`). Returns the first error that occurred, if any.
    fn flush(&self) -> Result<(), String>;
}

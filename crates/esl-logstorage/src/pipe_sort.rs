//! Port of `lib/logstorage/pipe_sort.go` — the `| sort ...` pipe.
//!
//! `pipe sort` buffers every input block, then in `flush` sorts all rows by the
//! `by (...)` fields (or by all columns) and streams the sorted rows to the next
//! processor, honoring `offset`, `rank` and (for the `limit` variant) top-N.
//!
//! When `limit > 0` the work is delegated to [`crate::pipe_sort_topk`], exactly
//! as Go's `newPipeProcessor` dispatches to `newPipeTopkProcessor`.
//!
//! PORT NOTE — parser: Go's `parsePipeSort`/`parseBySortFields`/`parseLimit`/
//! `parseOffset` consume the query `lexer`, which is not ported yet. The lexer
//! entry points are omitted; [`PipeSort::new`] builds the pipe from plain args
//! so a future parser can call it.
//!
//! PORT NOTE — `BySortField`: this is the real port of Go's `bySortField`
//! (which lives in `pipe_sort.go`). A minimal placeholder copy currently lives
//! in `crate::stats_json_values`; it should be replaced by a re-export of this
//! type once that module can be edited.
//!
//! PORT NOTE — merge: Go merges the per-shard pre-sorted rows with an
//! incremental k-way heap (`pipeSortProcessorShardsHeap`) to bound memory while
//! streaming. This port collects all row references across shards and performs a
//! single global sort with the same comparator; the emitted order is identical
//! (same strict-weak ordering), at the cost of holding all row refs at once.
//!
//! PORT NOTE — state-size accounting: Go tracks a fine-grained byte budget per
//! field. This port keeps the same budget/OOM-guard control flow (steal chunks
//! from a shared budget, `cancel` + stop when exhausted, and the same flush
//! error string) but charges the budget per block using `BlockResult::size_bytes`
//! plus the materialized numeric arrays, rather than per individual value.

use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering as AtomicOrdering};

use esl_common::memory;
use esl_common::stringsutil::less_natural;

use crate::block_result::{BlockResult, ResultColumn, get_block_result, put_block_result};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;
use crate::values_encoder::{
    marshal_timestamp_rfc3339_nano_string, marshal_uint64_string, try_parse_float64,
    try_parse_int64, try_parse_ipv4,
};

/// PORT NOTE: Go's `stateSizeBudgetChunk` (`pipe_stats.go`).
const STATE_SIZE_BUDGET_CHUNK: i64 = 1 << 20;

/// Flush emits a new output block once accumulated values reach this size.
const MAX_VALUES_LEN: usize = 1_000_000;

// ---------------------------------------------------------------------------
// bySortField
// ---------------------------------------------------------------------------

/// `by (...)` entry of the `sort` pipe (Go `bySortField`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BySortField {
    /// The name of the field to sort by.
    pub(crate) name: String,
    /// Whether sorting for this field is in descending order.
    pub(crate) is_desc: bool,
}

impl BySortField {
    pub(crate) fn new(name: impl Into<String>, is_desc: bool) -> Self {
        Self {
            name: name.into(),
            is_desc,
        }
    }
}

impl std::fmt::Display for BySortField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&quote_token_if_needed(&self.name))?;
        if self.is_desc {
            f.write_str(" desc")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// pipeSort
// ---------------------------------------------------------------------------

/// The `| sort ...` pipe (Go `pipeSort`).
#[derive(Clone, Debug, Default)]
pub(crate) struct PipeSort {
    /// Fields from the `by (...)` clause.
    pub(crate) by_fields: Vec<BySortField>,
    /// Whether to apply descending order to the whole result.
    pub(crate) is_desc: bool,
    /// How many results to skip.
    pub(crate) offset: u64,
    /// How many results to return (0 means all).
    pub(crate) limit: u64,
    /// The name of the field to store the row rank; empty if unset.
    pub(crate) rank_field_name: String,
    /// Fields for partitioning the sorted rows (only meaningful with `limit`).
    pub(crate) partition_by_fields: Vec<String>,
}

impl PipeSort {
    /// Builds a `sort` pipe from plain arguments (parser is deferred; see module
    /// PORT NOTE).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        by_fields: Vec<BySortField>,
        is_desc: bool,
        offset: u64,
        limit: u64,
        rank_field_name: impl Into<String>,
        partition_by_fields: Vec<String>,
    ) -> Self {
        Self {
            by_fields,
            is_desc,
            offset,
            limit,
            rank_field_name: rank_field_name.into(),
            partition_by_fields,
        }
    }
}

/// Port of Go `getOffsetLimitFromPipeSort` (parser.go).
///
/// Returns `(offset, limit)` when `ps` is a `sort by (_time) desc` style pipe
/// with a small enough offset/limit for the last-N results optimization.
pub(crate) fn get_offset_limit_from_pipe_sort(ps: &PipeSort) -> Option<(u64, u64)> {
    if ps.limit == 0 || ps.limit > 50_000 {
        return None;
    }
    if ps.offset > 50_000 {
        return None;
    }
    if !ps.rank_field_name.is_empty() {
        return None;
    }
    if !ps.partition_by_fields.is_empty() {
        return None;
    }
    if ps.by_fields.len() != 1 {
        return None;
    }
    if ps.by_fields[0].name != "_time" {
        return None;
    }
    let mut is_desc = ps.by_fields[0].is_desc;
    if ps.is_desc {
        is_desc = !is_desc;
    }
    if !is_desc {
        return None;
    }
    Some((ps.offset, ps.limit))
}

impl Pipe for PipeSort {
    fn get_offset_limit(&self) -> Option<(u64, u64)> {
        get_offset_limit_from_pipe_sort(self)
    }

    fn sort_merge_offset(&mut self, offset: u64) -> Option<bool> {
        // Go `optimizeSortOffsetPipes` body for the `*pipeSort` match.
        if self.limit > 0 && offset >= self.limit {
            return Some(false);
        }
        self.offset += offset;
        if self.limit > 0 {
            self.limit -= offset;
        }
        Some(true)
    }

    fn sort_merge_limit(&mut self, limit: u64) -> bool {
        // Go `optimizeSortLimitPipes` body for the `*pipeSort` match
        // (the `limit == 0` case is handled by the caller).
        if self.limit == 0 || limit < self.limit {
            self.limit = limit;
        }
        true
    }

    fn is_desc_time_topk(&self) -> bool {
        self.limit > 0
            && self.partition_by_fields.is_empty()
            && self.by_fields.len() == 1
            && self.by_fields[0].name == "_time"
            && (self.is_desc != self.by_fields[0].is_desc)
    }

    /// Port of Go `pipeSort.addPartitionByTime`.
    fn add_partition_by_time(&mut self, step: i64) {
        if step <= 0 {
            return;
        }
        if self.limit == 0 {
            return;
        }
        if !self.partition_by_fields.iter().any(|f| f == "_time") {
            self.partition_by_fields.push("_time".to_string());
        }
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        // The sort pipe does not change the set of fields.
        Some(crate::pipe::StatsTailOp::Keep)
    }

    /// Port of Go `pipeSort.splitToRemoteAndLocal`: the remote side sorts with
    /// `limit + offset` and no rank; the offset/rank are applied locally.
    fn split_to_remote_and_local(&self, _timestamp: i64) -> crate::pipe::SplitPipesResult {
        let mut p_remote = self.clone();
        p_remote.limit += p_remote.offset;
        p_remote.offset = 0;
        p_remote.rank_field_name = String::new();

        (Some(Box::new(p_remote)), vec![Box::new(self.clone())])
    }

    fn fixed_fields_transparent(&self) -> bool {
        true
    }

    /// Port of Go `pipeSort.adjustResultFieldsOrder`.
    fn sort_adjust_result_fields_order(&self, fields: &[String]) -> Option<Vec<String>> {
        let mut result: Vec<String> = Vec::new();

        if !self.rank_field_name.is_empty() {
            result.push(self.rank_field_name.clone());
        }

        let result_len = result.len();
        for bf in &self.by_fields {
            result.push(bf.name.clone());
        }
        let by_fields_end = result.len();

        for f in fields {
            if !result[result_len..by_fields_end].contains(f) {
                result.push(f.clone());
            }
        }

        Some(result)
    }

    fn to_string(&self) -> String {
        let mut s = String::from("sort");
        if !self.by_fields.is_empty() {
            let a: Vec<String> = self.by_fields.iter().map(|bf| bf.to_string()).collect();
            s.push_str(" by (");
            s.push_str(&a.join(", "));
            s.push(')');
        }
        if self.is_desc {
            s.push_str(" desc");
        }
        if !self.partition_by_fields.is_empty() {
            s.push_str(" partition by (");
            s.push_str(&crate::stats_count::field_names_string(
                &self.partition_by_fields,
            ));
            s.push(')');
        }
        if self.offset > 0 {
            s.push_str(&format!(" offset {}", self.offset));
        }
        if self.limit > 0 {
            s.push_str(&format!(" limit {}", self.limit));
        }
        if !self.rank_field_name.is_empty() {
            s.push_str(&rank_field_name_string(&self.rank_field_name));
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        if pf.match_nothing() {
            return;
        }
        if !self.rank_field_name.is_empty() {
            pf.add_deny_filter(&self.rank_field_name);
        }
        if self.by_fields.is_empty() {
            pf.add_allow_filter("*");
        } else {
            for bf in &self.by_fields {
                pf.add_allow_filter(&bf.name);
            }
        }
        pf.add_allow_filters(&self.partition_by_fields);
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        if self.limit > 0 {
            return crate::pipe_sort_topk::new_pipe_topk_processor(
                self,
                concurrency,
                stop,
                pp_next,
            );
        }
        new_pipe_sort_processor(self.clone(), concurrency, stop, pp_next)
    }
}

/// PORT NOTE: Go's `rankFieldNameString` lives in `pipe_top.go`, which is not
/// ported yet; this is a local copy until that module lands.
fn rank_field_name_string(rank_field_name: &str) -> String {
    let mut s = String::from(" rank");
    if rank_field_name != "rank" {
        s.push_str(" as ");
        s.push_str(&quote_token_if_needed(rank_field_name));
    }
    s
}

// ---------------------------------------------------------------------------
// materialized sort data
// ---------------------------------------------------------------------------

/// Data for a single `sort by (...)` column of a buffered block.
struct SortByColumn {
    /// Whether the column is `_time`.
    is_time: bool,
    /// String values per row (empty when `is_time`; then use block timestamps).
    values: Vec<Vec<u8>>,
    /// int64 numbers parsed from values (empty when `is_time`).
    i64_values: Vec<i64>,
    /// float64 numbers parsed from values (empty when `is_time`).
    f64_values: Vec<f64>,
}

/// A non-`by` column of a buffered block.
struct SortOtherColumn {
    name: Vec<u8>,
    values: Vec<Vec<u8>>,
}

/// A buffered block of logs for sorting (Go `sortBlock`), fully materialized.
struct SortBlock {
    by_columns: Vec<SortByColumn>,
    other_columns: Vec<SortOtherColumn>,
    /// Timestamps for the rows; populated when any `by` column is `_time`.
    timestamps: Vec<i64>,
}

/// Reference to a single buffered row (Go `sortRowRef`).
#[derive(Clone, Copy)]
struct SortRowRef {
    block_idx: usize,
    row_idx: usize,
}

#[derive(Default)]
struct SortShard {
    blocks: Vec<SortBlock>,
    row_refs: Vec<SortRowRef>,
    state_size_budget: i64,
}

fn bytes_str(b: &[u8]) -> &str {
    std::str::from_utf8(b).unwrap_or("")
}

/// Go `pipeSortProcessorShard.createInt64Values`.
pub(crate) fn create_int64_values(values: &[Vec<u8>]) -> Vec<i64> {
    values
        .iter()
        .map(|v| {
            let s = bytes_str(v);
            if let Some(i) = try_parse_int64(s) {
                return i;
            }
            if let Some(u) = try_parse_ipv4(s) {
                return u as i64;
            }
            // Do not try parsing timestamp and duration, since they may be
            // negative. This breaks sorting.
            0
        })
        .collect()
}

/// Go `pipeSortProcessorShard.createFloat64Values`.
pub(crate) fn create_float64_values(values: &[Vec<u8>]) -> Vec<f64> {
    values
        .iter()
        .map(|v| try_parse_float64(bytes_str(v)).unwrap_or(f64::NAN))
        .collect()
}

/// Go `marshalJSONKeyValue`.
pub(crate) fn marshal_json_key_value(dst: &mut Vec<u8>, k: &[u8], v: &[u8]) {
    esl_common::stringsutil::json_string_bytes_append(dst, k);
    dst.push(b':');
    esl_common::stringsutil::json_string_bytes_append(dst, v);
}

// ---------------------------------------------------------------------------
// processor
// ---------------------------------------------------------------------------

struct PipeSortProcessor {
    ps: PipeSort,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<SortShard>>,
    max_state_size: i64,
    state_size_budget: AtomicI64,
}

fn new_pipe_sort_processor(
    ps: PipeSort,
    concurrency: usize,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
) -> Arc<dyn PipeProcessor> {
    let max_state_size = (memory::allowed() as f64 * 0.2) as i64;
    let shards = (0..concurrency.max(1))
        .map(|_| Mutex::new(SortShard::default()))
        .collect();
    Arc::new(PipeSortProcessor {
        ps,
        stop,
        pp_next,
        shards,
        max_state_size,
        state_size_budget: AtomicI64::new(max_state_size),
    })
}

impl PipeSortProcessor {
    fn write_block_to_shard(&self, shard: &mut SortShard, br: &mut BlockResult) {
        let rows_len = br.rows_len();
        let cols = br.get_columns();
        let by_fields = &self.ps.by_fields;

        let block = if by_fields.is_empty() {
            // Sort by all the columns: marshal every column per row into a
            // single JSON-ish string and sort rows by the resulting string.
            let names: Vec<Vec<u8>> = cols.iter().map(|&r| br.column_name(r).to_vec()).collect();
            let mut col_values: Vec<Vec<Vec<u8>>> = Vec::with_capacity(cols.len());
            for &r in &cols {
                col_values.push(br.column_get_values(r).to_vec());
            }

            let mut values: Vec<Vec<u8>> = Vec::with_capacity(rows_len);
            for row in 0..rows_len {
                let mut bb: Vec<u8> = Vec::new();
                for (i, cv) in col_values.iter().enumerate() {
                    marshal_json_key_value(&mut bb, &names[i], &cv[row]);
                    bb.push(b',');
                }
                values.push(bb);
            }

            let by_columns = vec![SortByColumn {
                is_time: false,
                values,
                i64_values: vec![0; rows_len],
                f64_values: vec![f64::NAN; rows_len],
            }];
            let other_columns = names
                .into_iter()
                .zip(col_values)
                .map(|(name, values)| SortOtherColumn { name, values })
                .collect();
            SortBlock {
                by_columns,
                other_columns,
                timestamps: Vec::new(),
            }
        } else {
            let mut by_columns = Vec::with_capacity(by_fields.len());
            let mut any_time = false;
            for bf in by_fields {
                let r = br.get_column_by_name(&bf.name);
                if br.column_is_time(r) {
                    any_time = true;
                    by_columns.push(SortByColumn {
                        is_time: true,
                        values: Vec::new(),
                        i64_values: Vec::new(),
                        f64_values: Vec::new(),
                    });
                    continue;
                }
                let values = br.column_get_values(r).to_vec();
                let i64_values = create_int64_values(&values);
                let f64_values = create_float64_values(&values);
                by_columns.push(SortByColumn {
                    is_time: false,
                    values,
                    i64_values,
                    f64_values,
                });
            }
            let timestamps = if any_time {
                br.get_timestamps().to_vec()
            } else {
                Vec::new()
            };

            let mut other_columns = Vec::new();
            for &r in &cols {
                let name = br.column_name(r).to_vec();
                if by_fields.iter().any(|bf| bf.name.as_bytes() == name) {
                    continue;
                }
                let values = br.column_get_values(r).to_vec();
                other_columns.push(SortOtherColumn { name, values });
            }
            SortBlock {
                by_columns,
                other_columns,
                timestamps,
            }
        };

        shard.state_size_budget -= block_size_bytes(&block) as i64;
        shard.blocks.push(block);

        let block_idx = shard.blocks.len() - 1;
        for row_idx in 0..rows_len {
            shard.row_refs.push(SortRowRef { block_idx, row_idx });
        }
        shard.state_size_budget -= (rows_len * std::mem::size_of::<SortRowRef>()) as i64;
    }
}

fn block_size_bytes(b: &SortBlock) -> usize {
    let mut n = std::mem::size_of::<SortBlock>();
    n += b.timestamps.len() * std::mem::size_of::<i64>();
    for c in &b.by_columns {
        for v in &c.values {
            n += v.len();
        }
        n += c.i64_values.len() * std::mem::size_of::<i64>();
        n += c.f64_values.len() * std::mem::size_of::<f64>();
    }
    for c in &b.other_columns {
        n += c.name.len();
        for v in &c.values {
            n += v.len();
        }
    }
    n
}

impl PipeProcessor for PipeSortProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }
        let mut shard = self.shards[worker_id].lock().unwrap();
        while shard.state_size_budget < 0 {
            let remaining = self
                .state_size_budget
                .fetch_sub(STATE_SIZE_BUDGET_CHUNK, AtomicOrdering::SeqCst)
                - STATE_SIZE_BUDGET_CHUNK;
            if remaining < 0 {
                if remaining + STATE_SIZE_BUDGET_CHUNK >= 0 {
                    // Notify workers to stop calling write_block to save CPU.
                    self.stop.store(true, AtomicOrdering::SeqCst);
                }
                return;
            }
            shard.state_size_budget += STATE_SIZE_BUDGET_CHUNK;
        }
        self.write_block_to_shard(&mut shard, br);
    }

    fn flush(&self) -> Result<(), String> {
        if self.state_size_budget.load(AtomicOrdering::SeqCst) <= 0 {
            return Err(format!(
                "cannot calculate [{}], since it requires more than {}MB of memory",
                self.ps.to_string(),
                self.max_state_size / (1 << 20)
            ));
        }
        if self.stop.load(AtomicOrdering::SeqCst) {
            return Ok(());
        }

        // Collect all blocks and row references across shards.
        let mut all_blocks: Vec<SortBlock> = Vec::new();
        let mut all_refs: Vec<(usize, usize)> = Vec::new();
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap();
            let base = all_blocks.len();
            for rr in std::mem::take(&mut shard.row_refs) {
                all_refs.push((base + rr.block_idx, rr.row_idx));
            }
            all_blocks.append(&mut shard.blocks);
        }
        if all_refs.is_empty() {
            return Ok(());
        }

        let ps = &self.ps;
        all_refs.sort_by(|&(ba, ra), &(bb, rb)| {
            sort_block_cmp(ps, &all_blocks[ba], ra, &all_blocks[bb], rb)
        });

        self.emit_sorted(&all_blocks, &all_refs);
        Ok(())
    }
}

impl PipeSortProcessor {
    fn emit_sorted(&self, blocks: &[SortBlock], refs: &[(usize, usize)]) {
        let ps = &self.ps;
        let has_rank = !ps.rank_field_name.is_empty();

        let mut rcs: Vec<ResultColumn> = Vec::new();
        let mut cur_names: Vec<Vec<u8>> = Vec::new();
        let mut rows_count: usize = 0;
        let mut values_len: usize = 0;
        let mut rows_written: u64 = 0;

        for &(bi, ri) in refs {
            rows_written += 1;
            if rows_written <= ps.offset {
                continue;
            }
            let block = &blocks[bi];

            let mut names: Vec<Vec<u8>> = Vec::new();
            if has_rank {
                names.push(ps.rank_field_name.clone().into_bytes());
            }
            for bf in &ps.by_fields {
                names.push(bf.name.clone().into_bytes());
            }
            for oc in &block.other_columns {
                names.push(oc.name.clone());
            }

            if names != cur_names {
                if rows_count > 0 {
                    emit_block(&self.pp_next, &mut rcs, rows_count);
                    rows_count = 0;
                    values_len = 0;
                }
                rcs = names
                    .iter()
                    .map(|n| ResultColumn {
                        name: n.clone(),
                        values: Vec::new(),
                    })
                    .collect();
                cur_names = names;
            }

            let mut ci = 0;
            if has_rank {
                let mut buf = Vec::new();
                marshal_uint64_string(&mut buf, rows_written);
                rcs[ci].add_value(&buf);
                ci += 1;
            }
            for (i, _bf) in ps.by_fields.iter().enumerate() {
                let bc = &block.by_columns[i];
                let v: Vec<u8> = if bc.is_time {
                    let mut buf = Vec::new();
                    marshal_timestamp_rfc3339_nano_string(&mut buf, block.timestamps[ri]);
                    buf
                } else {
                    bc.values[ri].clone()
                };
                values_len += v.len();
                rcs[ci].add_value(&v);
                ci += 1;
            }
            for oc in &block.other_columns {
                let v = &oc.values[ri];
                values_len += v.len();
                rcs[ci].add_value(v);
                ci += 1;
            }

            rows_count += 1;
            if values_len >= MAX_VALUES_LEN {
                emit_block(&self.pp_next, &mut rcs, rows_count);
                rows_count = 0;
                values_len = 0;
            }
        }

        if rows_count > 0 {
            emit_block(&self.pp_next, &mut rcs, rows_count);
        }
    }
}

/// Sends the accumulated result columns to `pp_next` and clears their values.
/// Shared with [`crate::pipe_sort_topk`].
pub(crate) fn emit_block(
    pp_next: &Arc<dyn PipeProcessor>,
    rcs: &mut [ResultColumn],
    rows_count: usize,
) {
    let out: Vec<ResultColumn> = rcs
        .iter_mut()
        .map(|c| ResultColumn {
            name: c.name.clone(),
            values: std::mem::take(&mut c.values),
        })
        .collect();
    let mut br = get_block_result();
    br.set_result_columns(out, rows_count);
    pp_next.write_block(0, &mut br);
    put_block_result(br);
}

// ---------------------------------------------------------------------------
// comparator (Go sortBlockLess)
// ---------------------------------------------------------------------------

/// Comparator equivalent to Go's `sortBlockLess`, returning an [`Ordering`]:
/// `Less` when Go's `sortBlockLess(a, b)` is true.
fn sort_block_cmp(ps: &PipeSort, ba: &SortBlock, ra: usize, bb: &SortBlock, rb: usize) -> Ordering {
    let by_fields = &ps.by_fields;
    for idx in 0..ba.by_columns.len() {
        let ca = &ba.by_columns[idx];
        let cb = &bb.by_columns[idx];
        let mut is_desc = !by_fields.is_empty() && by_fields[idx].is_desc;
        if ps.is_desc {
            is_desc = !is_desc;
        }

        if ca.is_time && cb.is_time {
            let ta = ba.timestamps[ra];
            let tb = bb.timestamps[rb];
            if ta == tb {
                continue;
            }
            let less = if is_desc { tb < ta } else { ta < tb };
            return if less {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        if ca.is_time {
            // treat timestamps as smaller than other values
            return Ordering::Less;
        }
        if cb.is_time {
            return Ordering::Greater;
        }

        // Try sorting by int64 values first.
        let ua = ca.i64_values[ra];
        let ub = cb.i64_values[rb];
        if ua != 0 && ub != 0 {
            if ua == ub {
                continue;
            }
            let less = if is_desc { ub < ua } else { ua < ub };
            return if less {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        // Try sorting by float64 then.
        let fa = ca.f64_values[ra];
        let fb = cb.f64_values[rb];
        if !fa.is_nan() && !fb.is_nan() {
            if fa == fb {
                continue;
            }
            let less = if is_desc { fb < fa } else { fa < fb };
            return if less {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        // Fall back to natural string sorting.
        let sa = bytes_str(&ca.values[ra]);
        let sb = bytes_str(&cb.values[rb]);
        if sa == sb {
            continue;
        }
        // Do not use lessString() here, since int64/float64 were already tried.
        let less = if is_desc {
            less_natural(sb, sa)
        } else {
            less_natural(sa, sb)
        };
        return if less {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_by_sort_field_string() {
        assert_eq!(BySortField::new("x", false).to_string(), "x");
        assert_eq!(BySortField::new("x", true).to_string(), "x desc");
    }

    // PORT NOTE: Go's TestParsePipeSortSuccess/Failure exercise the query lexer,
    // which is deferred. These `to_string` checks reproduce the canonical
    // rendering asserted by the success cases, driving `PipeSort::new` directly.
    #[test]
    fn test_pipe_sort_to_string() {
        let f = |ps: PipeSort, want: &str| assert_eq!(Pipe::to_string(&ps), want);

        f(PipeSort::new(vec![], false, 0, 0, "", vec![]), "sort");
        f(
            PipeSort::new(vec![], false, 0, 0, "rank", vec![]),
            "sort rank",
        );
        f(
            PipeSort::new(vec![], false, 0, 0, "foo", vec![]),
            "sort rank as foo",
        );
        f(
            PipeSort::new(vec![BySortField::new("x", false)], false, 0, 0, "", vec![]),
            "sort by (x)",
        );
        f(
            PipeSort::new(vec![BySortField::new("x", false)], false, 0, 10, "", vec![]),
            "sort by (x) limit 10",
        );
        f(
            PipeSort::new(
                vec![BySortField::new("x", false)],
                false,
                20,
                10,
                "",
                vec![],
            ),
            "sort by (x) offset 20 limit 10",
        );
        f(
            PipeSort::new(
                vec![BySortField::new("x", false)],
                false,
                20,
                10,
                "bar",
                vec![],
            ),
            "sort by (x) offset 20 limit 10 rank as bar",
        );
        f(
            PipeSort::new(
                vec![BySortField::new("x", true), BySortField::new("y", false)],
                true,
                0,
                0,
                "",
                vec![],
            ),
            "sort by (x desc, y) desc",
        );
        f(
            PipeSort::new(
                vec![BySortField::new("a", false), BySortField::new("b", false)],
                false,
                0,
                10,
                "",
                vec!["y".to_string(), "z".to_string()],
            ),
            "sort by (a, b) partition by (y, z) limit 10",
        );
    }

    fn split(s: &str) -> Vec<String> {
        if s.is_empty() {
            Vec::new()
        } else {
            s.split(',').map(|x| x.to_string()).collect()
        }
    }

    // Port of Go's expectPipeNeededFields assertion, comparing sorted filter
    // sets (order-independent) since the Rust prefix_filter Display is unsorted.
    fn expect_needed_fields(
        ps: &PipeSort,
        allow: &str,
        deny: &str,
        allow_exp: &str,
        deny_exp: &str,
    ) {
        let mut pf = prefix_filter::Filter::default();
        let a = split(allow);
        let d = split(deny);
        if !a.is_empty() {
            pf.add_allow_filters(&a);
        }
        if !d.is_empty() {
            pf.add_deny_filters(&d);
        }
        ps.update_needed_fields(&mut pf);

        let mut got_allow = pf.get_allow_filters();
        got_allow.sort();
        let mut got_deny = pf.get_deny_filters();
        got_deny.sort();
        let mut exp_allow = split(allow_exp);
        exp_allow.sort();
        let mut exp_deny = split(deny_exp);
        exp_deny.sort();
        assert_eq!(got_allow, exp_allow, "allow mismatch");
        assert_eq!(got_deny, exp_deny, "deny mismatch");
    }

    // PORT NOTE: Go's TestPipeSortUpdateNeededFields parses each pipe string via
    // the lexer. Since the parser is deferred, a representative subset is ported
    // by constructing PipeSort directly with PipeSort::new.
    #[test]
    fn test_pipe_sort_update_needed_fields() {
        let by = |names: &[&str]| names.iter().map(|n| BySortField::new(*n, false)).collect();

        // all the needed fields
        expect_needed_fields(
            &PipeSort::new(vec![], false, 0, 0, "", vec![]),
            "*",
            "",
            "*",
            "",
        );
        expect_needed_fields(
            &PipeSort::new(by(&["s1", "s2"]), false, 0, 0, "", vec![]),
            "*",
            "",
            "*",
            "",
        );
        expect_needed_fields(
            &PipeSort::new(by(&["s1", "s2"]), false, 0, 0, "x", vec![]),
            "*",
            "",
            "*",
            "x",
        );

        // unneeded fields do not intersect with src
        expect_needed_fields(
            &PipeSort::new(by(&["s1", "s2"]), false, 0, 0, "", vec![]),
            "*",
            "f1,f2",
            "*",
            "f1,f2",
        );
        expect_needed_fields(
            &PipeSort::new(by(&["s1", "s2"]), false, 0, 0, "x", vec![]),
            "*",
            "f1,f2",
            "*",
            "f1,f2,x",
        );

        // unneeded fields intersect with src
        expect_needed_fields(
            &PipeSort::new(by(&["s1", "s2"]), false, 0, 0, "", vec![]),
            "*",
            "s1,f1,f2",
            "*",
            "f1,f2",
        );

        // needed fields do not intersect with src
        expect_needed_fields(
            &PipeSort::new(by(&["s1", "s2"]), false, 0, 0, "", vec![]),
            "f1,f2",
            "",
            "s1,s2,f1,f2",
            "",
        );

        // needed fields intersect with src
        expect_needed_fields(
            &PipeSort::new(by(&["s1", "s2"]), false, 0, 0, "", vec![]),
            "s1,f1,f2",
            "",
            "s1,s2,f1,f2",
            "",
        );
    }
}

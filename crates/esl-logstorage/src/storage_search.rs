//! Port of EsLogs `lib/logstorage/storage_search.go` — query execution
//! orchestration and the block-result / data-block interchange types.
//!
//! # Port scope and deferrals
//!
//! This module is the top of the query stack: `RunQuery` parses/optimizes a
//! [`crate::parser::Query`], finds partitions in the time range, searches parts
//! → blocks (via [`crate::block_search`]), applies the query filter, materializes
//! [`crate::block_result::BlockResult`]s and runs them through the pipe chain
//! (`crate::pipe::PipeProcessor`).
//!
//! The **self-contained interchange layer** is ported here in full and tested:
//! [`DataBlock`]/[`BlockColumn`] (the `WriteDataBlockFunc` payload with its
//! wire `Marshal`/`UnmarshalInplace`), [`ValueWithHits`], [`marshal_strings`]
//! and [`parse_stream_fields`].
//!
//! The **search orchestration** — [`run_query`], `run_pipes`, `search_parallel`,
//! `get_search_options`, `schedule_by_tenant_ids` / `schedule_by_stream_ids`
//! (`part.searchBy{TenantIDs,StreamIDs}`) — is implemented here, together with
//! [`crate::storage::Storage::get_partitions_for_time_range`],
//! [`crate::datadb::Datadb::get_parts_for_time_range`] and
//! [`crate::block_search::must_read_block_headers`]. The parser accessors it
//! needs (`get_final_filter`, `get_needed_columns`, `get_filter_time_range`,
//! `get_stream_ids`, `pipes`, `get_concurrency`, `get_parallel_readers`) live in
//! `parser::query`; `_stream` materialization is wired via
//! `BlockSearch::get_stream_str_slow` → `partition.idb`.
//!
//! Correctness fallbacks kept from the Go source (each carries a local PORT
//! NOTE):
//! * **Time-range pruning.** `get_filter_time_range` returns the full `i64`
//!   range (no partition/block time pruning); per-row time filtering still
//!   happens inside the filter.
//! * **Stream pre-filtering.** `get_stream_ids` / `getCommonStreamFilter`
//!   return empty/None (scan all streams by tenantID), so the search always
//!   goes through the tenantID path.
//! * **Subqueries / unions / joins / `filter in(subquery)`.**
//!   `initSubqueries`/`initUnionQueries`/`initJoinMaps`/`initFilterInValues`
//!   are deferred (single-node, no subqueries); `q` is executed directly.
//!
//! PORT NOTE: Go's `searchParallel` uses a channel of pooled
//! `blockSearchWorkBatch`es fed by concurrent partition searchers. Since
//! [`BlockSearchWork`] borrows `part`/`pso`, the port schedules all work up
//! front into a scope-local `Vec` and the scoped worker pool pulls items via a
//! shared cursor (see `search_parallel`).

use std::cell::RefCell;
use std::cmp::Ordering as Cmp;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use esl_common::bytesutil::to_unsafe_string;
use esl_common::encoding;

use crate::bitmap::get_bitmap;
use crate::block_header::BlockHeader;
use crate::block_result::BlockResult;
use crate::block_search::{
    BlockSearch, BlockSearchWork, PartitionSearchOptions, must_read_block_headers,
};
use crate::block_stream_reader::IndexBlockHeader;
use crate::datadb::PartWrapper;
use crate::parser::Query;
use crate::part::Part;
use crate::pattern::try_unquote_string;
use crate::pipe::PipeProcessor;
use crate::prefix_filter;
use crate::query_stats::QueryStats;
use crate::rows::Field;
use crate::storage::{PartitionWrapper, Storage};
use crate::stream_id::StreamID;
use crate::tenant_id::TenantID;

// ---------------------------------------------------------------------------
// DataBlock / BlockColumn
// ---------------------------------------------------------------------------

/// A single column of a [`DataBlock`].
///
/// PORT NOTE: Go's `BlockColumn.Values` is `[]string`; the port uses
/// `Vec<Vec<u8>>` to match the byte-oriented value convention used across the
/// block layer (column values may hold non-UTF-8 bytes).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BlockColumn {
    /// The column name.
    pub name: String,

    /// The column values.
    pub values: Vec<Vec<u8>>,
}

/// A single block of query-result data handed to a `WriteDataBlockFunc`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DataBlock {
    /// The columns in the data block.
    columns: Vec<BlockColumn>,
}

/// Marker byte for a const-valued column in the [`DataBlock`] wire format.
const VALUES_TYPE_CONST: u8 = 0;
/// Marker byte for a regular (per-row) column in the [`DataBlock`] wire format.
const VALUES_TYPE_REGULAR: u8 = 1;

impl DataBlock {
    /// Resets the data block for reuse.
    pub fn reset(&mut self) {
        self.columns.clear();
    }

    /// Returns the number of rows in the block.
    pub fn rows_count(&self) -> usize {
        if let Some(c) = self.columns.first() {
            c.values.len()
        } else {
            0
        }
    }

    /// Returns the columns of the block, optionally sorted by name.
    pub fn get_columns(&mut self, need_sort_columns: bool) -> &[BlockColumn] {
        if need_sort_columns {
            self.columns.sort_by(|a, b| a.name.cmp(&b.name));
        }
        &self.columns
    }

    /// Sets the columns of the block, taking ownership.
    pub fn set_columns(&mut self, columns: Vec<BlockColumn>) {
        self.columns = columns;
    }

    /// Returns the column with the given name, or `None`.
    pub fn get_column_by_name(&self, name: &str) -> Option<&BlockColumn> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Appends the parsed `_time` column values to `dst`
    /// (Go `DataBlock.GetTimestamps`).
    ///
    /// Returns false when the block has no `_time` column or a value cannot be
    /// parsed as an RFC3339 timestamp.
    pub fn get_timestamps(&self, dst: &mut Vec<i64>) -> bool {
        let Some(c) = self.get_column_by_name("_time") else {
            return false;
        };
        try_parse_timestamps(dst, &c.values)
    }

    /// Appends the marshaled block to `dst`.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        let rows_count = self.rows_count();
        encoding::marshal_var_uint64(dst, rows_count as u64);

        encoding::marshal_var_uint64(dst, self.columns.len() as u64);
        for c in &self.columns {
            encoding::marshal_bytes(dst, c.name.as_bytes());

            if c.values.len() != rows_count {
                esl_common::panicf!(
                    "BUG: the column {:?} must contain {} values; got {} values",
                    c.name,
                    rows_count,
                    c.values.len()
                );
            }
            if are_const_values(&c.values) {
                dst.push(VALUES_TYPE_CONST);
                encoding::marshal_bytes(dst, &c.values[0]);
            } else {
                dst.push(VALUES_TYPE_REGULAR);
                for v in &c.values {
                    encoding::marshal_bytes(dst, v);
                }
            }
        }
    }

    /// Unmarshals the block from `src`, returning the number of bytes consumed.
    ///
    /// PORT NOTE: Go's `UnmarshalInplace(src, valuesBuf)` returns the tail plus a
    /// reusable `valuesBuf` holding the unmarshaled column values as `string`
    /// views into `src`. The Rust port owns per-column `Vec<Vec<u8>>` (the
    /// established arena divergence for the block layer), so there is no
    /// `valuesBuf`; it returns the count of consumed bytes instead of the tail.
    pub fn unmarshal_inplace(&mut self, src: &[u8]) -> Result<usize, String> {
        let src_orig_len = src.len();
        self.reset();

        let (u64_rows, n) = encoding::unmarshal_var_uint64(src);
        if n <= 0 {
            return Err(format!(
                "cannot unmarshal the number of rows from len(src)={}",
                src.len()
            ));
        }
        let rows_count = u64_rows as usize;
        let mut src = &src[n as usize..];

        let (columns_len, n) = encoding::unmarshal_var_uint64(src);
        if n <= 0 {
            return Err(format!(
                "cannot unmarshal the number of columns from len(src)={}",
                src.len()
            ));
        }
        src = &src[n as usize..];

        let columns_len = columns_len as usize;
        let mut columns: Vec<BlockColumn> = Vec::with_capacity(columns_len);
        for i in 0..columns_len {
            let (name, n) = encoding::unmarshal_bytes(src);
            let name = match name {
                Some(name) if n > 0 => name.to_vec(),
                _ => {
                    return Err(format!(
                        "cannot unmarshal column name from len(src)={}",
                        src.len()
                    ));
                }
            };
            src = &src[n as usize..];

            if src.is_empty() {
                return Err(format!(
                    "missing value type for column {:?}",
                    to_unsafe_string(&name)
                ));
            }
            let values_type = src[0];
            src = &src[1..];

            let mut values: Vec<Vec<u8>> = Vec::with_capacity(rows_count);
            match values_type {
                VALUES_TYPE_CONST => {
                    let (v, n) = encoding::unmarshal_bytes(src);
                    let v = match v {
                        Some(v) if n > 0 => v.to_vec(),
                        _ => {
                            return Err(format!(
                                "cannot unmarshal const value for column #{} with name {:?} from len(src)={}",
                                i,
                                to_unsafe_string(&name),
                                src.len()
                            ));
                        }
                    };
                    src = &src[n as usize..];
                    for _ in 0..rows_count {
                        values.push(v.clone());
                    }
                }
                VALUES_TYPE_REGULAR => {
                    for j in 0..rows_count {
                        let (v, n) = encoding::unmarshal_bytes(src);
                        let v = match v {
                            Some(v) if n > 0 => v.to_vec(),
                            _ => {
                                return Err(format!(
                                    "cannot unmarshal value #{} out of {} values for column #{} with name {:?} from len(src)={}",
                                    j,
                                    rows_count,
                                    i,
                                    to_unsafe_string(&name),
                                    src.len()
                                ));
                            }
                        };
                        src = &src[n as usize..];
                        values.push(v);
                    }
                }
                other => {
                    return Err(format!("unexpected valuesType={other}"));
                }
            }

            columns.push(BlockColumn {
                name: to_unsafe_string(&name).to_string(),
                values,
            });
        }
        self.columns = columns;

        Ok(src_orig_len - src.len())
    }

    /// Initializes the block from a [`BlockResult`].
    pub fn must_init_from_block_result(&mut self, br: &mut BlockResult) {
        self.reset();

        let cs = br.get_columns();
        for r in cs {
            let name = br.column_name(r).to_string();
            let values = br.column_get_values(r).to_vec();
            self.columns.push(BlockColumn { name, values });
        }
    }

    /// Initializes the given [`BlockResult`] from this block.
    ///
    /// PORT NOTE: mirrors Go's `blockResult.mustInitFromDataBlock`. Uses
    /// [`BlockResult::set_result_columns`], which chooses const/string encoding.
    pub fn init_block_result(&self, br: &mut BlockResult) {
        let rows_len = self.rows_count();
        let rcs: Vec<crate::block_result::ResultColumn> = self
            .columns
            .iter()
            .map(|c| crate::block_result::ResultColumn {
                name: c.name.clone(),
                values: c.values.clone(),
            })
            .collect();
        br.set_result_columns(rcs, rows_len);
    }
}

// ---------------------------------------------------------------------------
// ValueWithHits
// ---------------------------------------------------------------------------
/// Port of Go `tryParseTimestamps` (block_result.go), reduced to the
/// `DataBlock::get_timestamps` use case: parses each value as an RFC3339
/// timestamp and appends it to `dst`; returns false on the first parse failure.
fn try_parse_timestamps(dst: &mut Vec<i64>, values: &[Vec<u8>]) -> bool {
    dst.reserve(values.len());
    for v in values {
        let Ok(v) = std::str::from_utf8(v) else {
            return false;
        };
        let Some(ts) = crate::values_encoder::try_parse_timestamp_rfc3339_nano(v) else {
            return false;
        };
        dst.push(ts);
    }
    true
}

/// A value together with the number of hits for it.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ValueWithHits {
    /// The value.
    pub value: String,
    /// The number of hits for the value.
    pub hits: u64,
}

impl ValueWithHits {
    /// Appends the marshaled `self` to `dst`.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        encoding::marshal_bytes(dst, self.value.as_bytes());
        encoding::marshal_uint64(dst, self.hits);
    }

    /// Unmarshals `self` from `src`, returning the number of bytes consumed.
    ///
    /// PORT NOTE: Go returns the remaining tail; the port returns the consumed
    /// byte count (`self.value` is owned rather than a view into `src`).
    pub fn unmarshal_inplace(&mut self, src: &[u8]) -> Result<usize, String> {
        let src_orig_len = src.len();

        let (value, n) = encoding::unmarshal_bytes(src);
        let value = match value {
            Some(value) if n > 0 => value,
            _ => return Err("cannot unmarshal value".to_string()),
        };
        let mut src = &src[n as usize..];
        self.value = to_unsafe_string(value).to_string();

        if src.len() < 8 {
            return Err("cannot unmarshal hits".to_string());
        }
        self.hits = encoding::unmarshal_uint64(src);
        src = &src[8..];

        Ok(src_orig_len - src.len())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Appends the marshaled strings `a` to `dst` (Go `marshalStrings`).
pub fn marshal_strings(dst: &mut Vec<u8>, a: &[String]) {
    for v in a {
        encoding::marshal_bytes(dst, v.as_bytes());
    }
}

/// Parses the `{name="value",...}` stream name in `s`, appending fields to
/// `dst` (Go `parseStreamFields`).
pub fn parse_stream_fields(mut dst: Vec<Field>, s: &str) -> Result<Vec<Field>, String> {
    if s.is_empty() || !s.starts_with('{') {
        return Err("missing '{' at the beginning of stream name".to_string());
    }
    let mut s = &s[1..];
    if s.is_empty() || !s.ends_with('}') {
        return Err("missing '}' at the end of stream name".to_string());
    }
    s = &s[..s.len() - 1];
    if s.is_empty() {
        return Ok(dst);
    }

    loop {
        let n = match s.find("=\"") {
            Some(n) => n,
            None => return Err(format!("cannot find field value in double quotes at [{s}]")),
        };
        let name = &s[..n];
        s = &s[n + 1..];

        let (value, n_offset) = match try_unquote_string(s, "") {
            Some((value, n_offset)) => (value, n_offset),
            None => {
                return Err(format!(
                    "cannot find parse field value in double quotes at [{s}]"
                ));
            }
        };
        s = &s[n_offset..];

        dst.push(Field {
            name: name.to_string(),
            value: value.clone(),
        });

        if s.is_empty() {
            return Ok(dst);
        }
        if !s.starts_with(',') {
            return Err(format!("missing ',' after {name}={value:?}"));
        }
        s = &s[1..];
    }
}

/// Returns true if `values` is non-empty and every value equals the first.
///
/// PORT NOTE: local copy of `areConstValues` (private to `block_result.rs`);
/// duplicated to keep this module self-contained.
fn are_const_values(values: &[Vec<u8>]) -> bool {
    if values.is_empty() {
        return false;
    }
    let v = &values[0];
    values[1..].iter().all(|x| x == v)
}

// ---------------------------------------------------------------------------
// RunQuery orchestration (port of storage_search.go's search spine)
// ---------------------------------------------------------------------------

/// Search options for a Storage search (Go `storageSearchOptions`).
///
/// PORT NOTE: `streamFilter`/`hiddenFieldsFilter`/subquery passes are deferred
/// (see the module deferral notes). `filter` is borrowed from the query (the
/// `Filter` trait has no clone hook and the query outlives the search).
struct StorageSearchOptions<'f> {
    tenant_ids: Vec<TenantID>,
    stream_ids: Vec<StreamID>,
    min_timestamp: i64,
    max_timestamp: i64,
    filter: &'f dyn crate::filter::Filter,
    fields_filter: prefix_filter::Filter,
    time_offset: i64,
    /// Feed blocks to the workers newest-first (see `Pipe::is_desc_time_topk`).
    desc_block_order: bool,
}

fn tenant_id_cmp(a: &TenantID, b: &TenantID) -> Cmp {
    if a.less(b) {
        Cmp::Less
    } else if b.less(a) {
        Cmp::Greater
    } else {
        Cmp::Equal
    }
}

fn stream_id_cmp(a: &StreamID, b: &StreamID) -> Cmp {
    if a.less(b) {
        Cmp::Less
    } else if b.less(a) {
        Cmp::Greater
    } else {
        Cmp::Equal
    }
}

/// Go `Storage.getSearchOptions`.
fn get_search_options<'q>(tenant_ids: &[TenantID], q: &'q Query) -> StorageSearchOptions<'q> {
    let mut stream_ids = q.get_stream_ids();
    stream_ids.sort_by(stream_id_cmp);

    let (min_timestamp, max_timestamp) = q.get_filter_time_range();
    let filter = q.get_final_filter();
    // PORT NOTE: Go's getCommonStreamFilter(ff) splits a common stream filter out
    // of the final filter; that split needs the deferred filter downcast, so the
    // full final filter is used and no stream pre-filter is produced.
    let fields_filter = q.get_needed_columns();

    let mut tenant_ids = tenant_ids.to_vec();
    tenant_ids.sort_by(tenant_id_cmp);

    StorageSearchOptions {
        tenant_ids,
        stream_ids,
        min_timestamp,
        max_timestamp,
        filter,
        fields_filter,
        time_offset: -q.time_offset(),
        desc_block_order: q
            .pipes()
            .first()
            .map(|p| p.is_desc_time_topk())
            .unwrap_or(false),
    }
}

/// Go `partition.getSearchOptions`.
///
/// PORT NOTE: Go pre-resolves stream filters here per partition
/// (`initStreamFilters` copies the filter tree binding `idb` + tenantIDs).
/// The port resolves them lazily inside `FilterStream` instead (per-idb
/// cache, see filter_stream.rs), so the shared filter passes through
/// unchanged. Go's `getCommonStreamFilter` block-scheduling pre-filter
/// (`sso.streamFilter` -> `pso.stream_ids`) remains unported — matching is
/// done per block header in `FilterStream::apply_to_block_search`, which
/// prunes before any column reads.
fn partition_search_options<'f>(sso: &StorageSearchOptions<'f>) -> PartitionSearchOptions<'f> {
    PartitionSearchOptions {
        tenant_ids: sso.tenant_ids.clone(),
        stream_ids: sso.stream_ids.clone(),
        min_timestamp: sso.min_timestamp,
        max_timestamp: sso.max_timestamp,
        filter: sso.filter,
        fields_filter: sso.fields_filter.clone(),
        hidden_fields_filter: prefix_filter::Filter::default(),
    }
}

/// Go `sort.Search`: returns the smallest `i` in `[0, n]` for which `f(i)` is
/// true, assuming `f` is false for a prefix and then true.
fn sort_search(n: usize, mut f: impl FnMut(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (0usize, n);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if f(mid) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// A function that consumes a result [`DataBlock`] for a given worker
/// (Go `WriteDataBlockFunc`).
///
/// The `usize` is the worker id; the callback must not retain the `DataBlock`
/// after it returns.
pub type WriteDataBlockFn = Arc<dyn Fn(usize, &mut DataBlock) + Send + Sync>;

/// The terminal pipe processor: converts each [`BlockResult`] into a
/// [`DataBlock`] and hands it to the user's write function (Go's
/// `WriteDataBlockFunc.newBlockResultWriter`).
///
/// PORT NOTE: Go pools per-worker `DataBlock`s via `atomicutil.Slice`; the port
/// allocates a fresh `DataBlock` per non-empty block (correctness over the pool
/// micro-optimization).
struct BlockResultWriter {
    f: WriteDataBlockFn,
}

impl PipeProcessor for BlockResultWriter {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }
        let mut db = DataBlock::default();
        db.must_init_from_block_result(br);
        (self.f)(worker_id, &mut db);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Go `runPipes`: wires the pipe chain between `search` and the terminal `sink`.
fn run_pipes<F>(
    pipes: &[Box<dyn crate::pipe::Pipe>],
    concurrency: usize,
    search: F,
    sink: Arc<dyn PipeProcessor>,
) -> Result<(), String>
where
    F: FnOnce(&AtomicBool, &Arc<dyn PipeProcessor>) -> Result<(), String>,
{
    let stop = Arc::new(AtomicBool::new(false));

    if pipes.is_empty() {
        // Fast path: no pipes — search writes directly to the terminal sink.
        return search(&stop, &sink);
    }

    let mut pp: Arc<dyn PipeProcessor> = sink;
    let mut pps: Vec<Arc<dyn PipeProcessor>> = Vec::with_capacity(pipes.len());
    for p in pipes.iter().rev() {
        pp = p.new_pipe_processor(concurrency, Arc::clone(&stop), pp);
        pps.push(Arc::clone(&pp));
    }
    // pps is inner→outer (last pipe first); reverse to first→last for flushing.
    pps.reverse();
    let head = pp;

    let err_search = search(&stop, &head);
    if err_search.is_err() {
        stop.store(true, Ordering::SeqCst);
    }

    let mut err_flush: Result<(), String> = Ok(());
    for pp in &pps {
        if let Err(e) = pp.flush()
            && err_flush.is_ok()
        {
            stop.store(true, Ordering::SeqCst);
            err_flush = Err(e);
        }
    }

    err_search?;
    err_flush
}

/// Runs `q` against `storage` for the given `tenant_ids`, streaming each result
/// [`DataBlock`] to `write_block_fn` (Go `Storage.RunQuery` / `runQuery`).
///
/// PORT NOTE: Go takes a `*QueryContext` (context/cancellation/tenantIDs/stats);
/// the port passes `tenant_ids` explicitly and drops context cancellation.
/// `initSubqueries`/`initUnionQueries`/`initJoinMaps`/`initFilterInValues` are
/// deferred (single-node, no subqueries); `q` is executed directly.
pub(crate) fn run_query(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q: &Query,
    write_block_fn: WriteDataBlockFn,
) -> Result<(), String> {
    let sso = get_search_options(tenant_ids, q);
    let qs = QueryStats::default();

    let concurrency = q.get_concurrency().max(1);
    let workers = q
        .get_parallel_readers(storage.default_parallel_readers)
        .max(1);

    let sink: Arc<dyn PipeProcessor> = Arc::new(BlockResultWriter { f: write_block_fn });

    let res = run_pipes(
        q.pipes(),
        concurrency,
        |stop, head| {
            let head_w = Arc::clone(head);
            let write_block = move |worker_id: usize, br: &mut BlockResult| {
                head_w.write_block(worker_id, br);
            };
            let head_s = Arc::clone(head);
            let skip_block = move |worker_id: usize, min_ts: i64, max_ts: i64| {
                head_s.block_skip_check(worker_id, min_ts, max_ts)
            };
            search_parallel(storage, workers, &sso, &qs, stop, &skip_block, &write_block);
            Ok(())
        },
        sink,
    );
    // Perf diagnostic (ESL_QUERY_TIMING=1): per-query scan counters.
    if std::env::var_os("ESL_QUERY_TIMING").is_some() {
        eprintln!(
            "ESL_QUERY_STATS blocks_processed={} rows_processed={} rows_found={}",
            qs.blocks_processed.load(Ordering::SeqCst),
            qs.rows_processed.load(Ordering::SeqCst),
            qs.rows_found.load(Ordering::SeqCst),
        );
    }
    res
}

/// Go `Storage.searchParallel`: fans block searches across `workers_count`
/// scoped worker threads and pushes matching [`BlockResult`]s to `write_block`.
///
/// PORT NOTE: Go uses a channel of pooled `blockSearchWorkBatch`es produced by
/// concurrent partition searchers and consumed by a worker pool. Because
/// [`BlockSearchWork`] borrows `part`/`pso`, the port cannot pool the work in a
/// `'static` channel; instead it schedules all block-search work up front into a
/// scope-local `Vec` and the scoped workers pull items via a shared cursor. The
/// scheduling read cost (reading block headers) is the same; only the
/// producer/consumer overlap is dropped.
fn search_parallel(
    storage: &Arc<Storage>,
    workers_count: usize,
    sso: &StorageSearchOptions<'_>,
    qs: &QueryStats,
    stop: &AtomicBool,
    skip_block: &(dyn Fn(usize, i64, i64) -> bool + Sync),
    write_block: &(dyn Fn(usize, &mut BlockResult) + Sync),
) {
    // Select partitions covering the time range (refCounted).
    let ptws: Vec<Arc<PartitionWrapper>> =
        storage.get_partitions_for_time_range(sso.min_timestamp, sso.max_timestamp);

    // Build one partitionSearchOptions per partition and collect the matching
    // parts (refCounted). Part pointers stay valid while `pws_hold` keeps each
    // part referenced (see PartWrapper::part_ptr).
    let mut psos: Vec<PartitionSearchOptions> = Vec::with_capacity(ptws.len());
    let mut pws_hold: Vec<Arc<PartWrapper>> = Vec::new();
    let mut part_refs: Vec<(*const Part<'static>, usize)> = Vec::new();
    for ptw in &ptws {
        let pso_idx = psos.len();
        psos.push(partition_search_options(sso));
        let pws = ptw
            .pt
            .ddb()
            .get_parts_for_time_range(sso.min_timestamp, sso.max_timestamp);
        for pw in pws {
            part_refs.push((pw.part_ptr(), pso_idx));
            pws_hold.push(pw);
        }
    }

    // Schedule all block-search work (single-threaded; reads block headers).
    // Declared here (not inside the scope) so it outlives the scoped workers.
    let sched_qs = QueryStats::default();
    let mut works: Vec<BlockSearchWork> = Vec::new();
    for (ptr, pso_idx) in &part_refs {
        // SAFETY: `ptr` was produced by `PartWrapper::part_ptr` for a part
        // held referenced in `pws_hold` for the whole scope; its refCount is
        // > 0, so the part is not closed and its address is pinned.
        let p: &Part = unsafe { &**ptr };
        schedule_part_search(&mut works, p, &psos[*pso_idx], &sched_qs);
    }

    if sso.desc_block_order {
        // Newest blocks first: the consuming desc-time top-N pipe fills its
        // heap from the first block it sees and rejects the remaining blocks
        // on their first (newest) row.
        works.sort_by_key(|w| std::cmp::Reverse(w.bh.timestamps_header.max_timestamp));
    }

    let works: &[BlockSearchWork] = &works;

    // PORT NOTE: the previous implementation spawned `workers_count` OS threads
    // per query via `std::thread::scope`. Thread creation is cheap on Linux but
    // expensive on Windows (vs Go's goroutines), which dominated the latency of
    // short queries there. rayon's global work-stealing pool is created once for
    // the process, so queries never spawn threads; per-worker scratch (`bitmap`,
    // `BlockResult`) is reused via a thread-local, and the `worker_id` handed to
    // `write_block` is the rayon worker index. Small queries (`workers_count <= 1`
    // or a single work item) run inline on the caller to skip the pool entirely.
    thread_local! {
        static SEARCH_SCRATCH: RefCell<(crate::bitmap::Bitmap, BlockResult)> =
            RefCell::new((get_bitmap(0), BlockResult::default()));
    }

    let process = |worker_id: usize, w: &BlockSearchWork| {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        if skip_block(
            worker_id,
            w.bh.timestamps_header.min_timestamp,
            w.bh.timestamps_header.max_timestamp,
        ) {
            // The consumer proved the block cannot contribute; account for it
            // in the stats without reading it.
            qs.blocks_processed.fetch_add(1, Ordering::SeqCst);
            qs.rows_processed
                .fetch_add(w.bh.rows_count, Ordering::SeqCst);
            return;
        }
        SEARCH_SCRATCH.with(|cell| {
            let (bm, br) = &mut *cell.borrow_mut();
            let mut bs = BlockSearch::new(qs, w.p, w.pso, w.bh.clone());
            bs.search(bm, br);
            let rows_found = br.rows_len() as u64;
            if rows_found > 0 {
                if sso.time_offset != 0 {
                    bs.sub_time_offset_to_timestamps(sso.time_offset);
                }
                write_block(worker_id, br);
            }
            qs.blocks_processed.fetch_add(1, Ordering::SeqCst);
            qs.rows_processed
                .fetch_add(w.bh.rows_count, Ordering::SeqCst);
            qs.rows_found.fetch_add(rows_found, Ordering::SeqCst);
        });
    };

    if workers_count <= 1 || works.len() <= 1 {
        for w in works {
            process(0, w);
        }
    } else {
        use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
        // Adaptive serial start (not in Go): process blocks inline until the
        // serial budget is spent. Cheap queries (bloom-pruned filters,
        // header-only stats) finish without waking the rayon pool — thread
        // wakeups are expensive on Windows — while heavy queries fan out
        // after at most ~one warm-up block. With desc_block_order the serial
        // phase also publishes the desc-time top-N skip threshold before the
        // fan-out, so parallel workers skip their blocks instead of each
        // paying for one full block.
        const ADAPTIVE_SERIAL_BUDGET: Duration = Duration::from_micros(300);
        let t0 = Instant::now();
        let mut idx = 0usize;
        while idx < works.len() && t0.elapsed() < ADAPTIVE_SERIAL_BUDGET {
            process(0, &works[idx]);
            idx += 1;
        }
        works[idx..].par_iter().for_each(|w| {
            let worker_id = rayon::current_thread_index().unwrap_or(0);
            process(worker_id, w);
        });
    }

    // Release parts and partitions.
    for pw in &pws_hold {
        pw.dec_ref();
    }
    storage.put_partitions(&ptws);
}

// ---------------------------------------------------------------------------
// ValuesWithHits query surface (Go GetFieldNames / GetFieldValues / GetStreams
// / GetStreamIDs / GetStreamFieldNames / GetStreamFieldValues)
// ---------------------------------------------------------------------------

impl Storage {
    /// Returns field names for the results of `q` (Go `Storage.GetFieldNames`).
    ///
    /// If `filter` is non-empty, then only the field names containing the
    /// filter substring are returned.
    ///
    /// PORT NOTE: Go takes a `*QueryContext`; the port passes `tenant_ids` and
    /// `q` explicitly (see [`Storage::run_query`]). Go clones `q` shallowly
    /// (sharing the filter and pipes); Rust filters/pipes are single-owner
    /// trait objects, so the query is cloned via re-parsing
    /// ([`Query::clone`]).
    pub fn get_field_names(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &Query,
        filter: &str,
    ) -> Result<Vec<ValueWithHits>, String> {
        let mut q_new = q.clone(q.get_timestamp());

        let mut pipe_str = "field_names".to_string();
        if !filter.is_empty() {
            pipe_str += " filter ";
            pipe_str += &crate::parser::quote_token_if_needed(filter);
        }
        // PORT NOTE: Go sets `pf.isFirstPipe = len(pipes) == 0`, which makes
        // the pipe read field names straight from the per-block columns header
        // instead of materializing the columns. The Rust `BlockResult` does
        // not carry a block-search/columns-header reference (Go `br.bs`), so
        // the pipe is left in its non-first-pipe mode — the exact path Go
        // takes for pre-v1 part formats: all columns are fetched and their
        // names counted. Same results, without the header-only optimization.
        let p = crate::parser::parse_pipe::must_parse_pipe(&pipe_str, q.get_timestamp());
        q_new.pipes.push(p);

        self.run_values_with_hits_query(tenant_ids, &q_new)
    }

    /// Returns unique values with the number of hits for the given
    /// `field_name` returned by `q` (Go `Storage.GetFieldValues`).
    ///
    /// If `filter` is non-empty, then only the field values containing the
    /// filter substring are returned.
    ///
    /// If `limit > 0`, then up to `limit` unique values are returned.
    pub fn get_field_values(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &Query,
        field_name: &str,
        filter: &str,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, String> {
        let mut q_new = q.clone(q.get_timestamp());

        let mut pipe_str = format!(
            "field_values {}",
            crate::parser::quote_token_if_needed(field_name)
        );
        if !filter.is_empty() {
            pipe_str += " filter ";
            pipe_str += &crate::parser::quote_token_if_needed(filter);
        }
        if limit > 0 {
            pipe_str += &format!(" limit {limit}");
        }
        let p = crate::parser::parse_pipe::must_parse_pipe(&pipe_str, q.get_timestamp());
        q_new.pipes.push(p);

        self.run_values_with_hits_query(tenant_ids, &q_new)
    }

    /// Returns stream field names for the results of `q`
    /// (Go `Storage.GetStreamFieldNames`).
    ///
    /// If `filter` is non-empty, then only the field names containing the
    /// filter substring are returned.
    pub fn get_stream_field_names(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &Query,
        filter: &str,
    ) -> Result<Vec<ValueWithHits>, String> {
        let streams = self.get_streams(tenant_ids, q, u64::MAX)?;

        let mut m: HashMap<String, u64> = HashMap::new();
        for_each_stream_field(&streams, |f, hits| {
            if !filter.is_empty() && !f.name.contains(filter) {
                return;
            }

            *m.entry(f.name.clone()).or_insert(0) += hits;
        });
        Ok(to_values_with_hits(m))
    }

    /// Returns stream field values for the given `field_name` and the results
    /// of `q` (Go `Storage.GetStreamFieldValues`).
    ///
    /// If `filter` is non-empty, then only the field values containing the
    /// filter substring are returned.
    ///
    /// If `limit > 0`, then up to `limit` unique values are returned.
    pub fn get_stream_field_values(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &Query,
        field_name: &str,
        filter: &str,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, String> {
        let streams = self.get_streams(tenant_ids, q, u64::MAX)?;

        let mut m: HashMap<String, u64> = HashMap::new();
        for_each_stream_field(&streams, |f, hits| {
            if !filter.is_empty() && !f.value.contains(filter) {
                return;
            }

            if f.name != field_name {
                return;
            }
            *m.entry(f.value.clone()).or_insert(0) += hits;
        });
        let mut values = to_values_with_hits(m);
        if limit > 0 && values.len() as u64 > limit {
            values.truncate(limit as usize);
            crate::pipe_field_values_local::reset_hits(&mut values);
        }
        Ok(values)
    }

    /// Returns the tenantIDs registered in partitions overlapping
    /// `[start, end]` (Go `Storage.GetTenantIDs`).
    ///
    /// PORT NOTE: Go fans the per-partition `idb.searchTenants()` scans across
    /// a worker pool; partition counts are tiny (one per day of retention), so
    /// the port scans them sequentially. Result de-duplication matches Go; the
    /// result order is unspecified in both (Go iterates a map).
    pub fn get_tenant_ids(
        self: &Arc<Storage>,
        start: i64,
        end: i64,
    ) -> Result<Vec<TenantID>, String> {
        let ptws = self.get_partitions_for_time_range(start, end);
        let mut uniq: Vec<TenantID> = Vec::new();
        for ptw in &ptws {
            for tid in ptw.pt.idb.search_tenants() {
                if !uniq.contains(&tid) {
                    uniq.push(tid);
                }
            }
        }
        self.put_partitions(&ptws);
        Ok(uniq)
    }

    /// Returns streams from the results of `q` (Go `Storage.GetStreams`).
    ///
    /// If `limit > 0`, then up to `limit` unique streams are returned.
    pub fn get_streams(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &Query,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, String> {
        self.get_field_values(tenant_ids, q, "_stream", "", limit)
    }

    /// Returns `_stream_id` field values from the results of `q`
    /// (Go `Storage.GetStreamIDs`).
    ///
    /// If `limit > 0`, then up to `limit` unique streams are returned.
    pub fn get_stream_ids(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &Query,
        limit: u64,
    ) -> Result<Vec<ValueWithHits>, String> {
        self.get_field_values(tenant_ids, q, "_stream_id", "", limit)
    }

    /// Go `Storage.runValuesWithHitsQuery`: runs `q` (which must end with a
    /// two-column `(value, hits)` pipe) and collects the sorted results.
    fn run_values_with_hits_query(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &Query,
    ) -> Result<Vec<ValueWithHits>, String> {
        let results: Arc<std::sync::Mutex<Vec<ValueWithHits>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        let results_w = Arc::clone(&results);
        let write_block: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
            if db.rows_count() == 0 {
                return;
            }

            let cs = db.get_columns(false);
            if cs.len() != 2 {
                esl_common::panicf!("BUG: expecting two columns; got {} columns", cs.len());
            }

            let column_values = &cs[0].values;
            let column_hits = &cs[1].values;

            let mut values_with_hits = Vec::with_capacity(column_values.len());
            for i in 0..column_values.len() {
                let hits_str = String::from_utf8_lossy(&column_hits[i]);
                let hits = crate::values_encoder::try_parse_uint64(&hits_str).unwrap_or(0);
                values_with_hits.push(ValueWithHits {
                    value: String::from_utf8_lossy(&column_values[i]).into_owned(),
                    hits,
                });
            }

            results_w.lock().unwrap().extend(values_with_hits);
        });

        self.run_query(tenant_ids, q, write_block)?;

        let mut results = std::mem::take(&mut *results.lock().unwrap());
        crate::pipe_field_values_local::sort_values_with_hits(&mut results);

        Ok(results)
    }
}

// PORT NOTE: Go's `getFieldValuesGeneric` / `isLastPipeUniq` / `getRows` are
// used only by `initSubqueries` (`in(subquery)` / `join` initialization),
// which is deferred (see the module deferral notes above); they are not
// ported yet.

/// Port of Go `toValuesWithHits` (`map[string]*uint64` becomes an owned map).
fn to_values_with_hits(m: HashMap<String, u64>) -> Vec<ValueWithHits> {
    let mut results: Vec<ValueWithHits> = m
        .into_iter()
        .map(|(value, hits)| ValueWithHits { value, hits })
        .collect();
    crate::pipe_field_values_local::sort_values_with_hits(&mut results);
    results
}

/// Port of Go `forEachStreamField`: calls `f` for every field parsed from the
/// `{name="value",...}` stream names in `streams`.
fn for_each_stream_field(streams: &[ValueWithHits], mut f: impl FnMut(&Field, u64)) {
    for vh in streams {
        let Ok(fields) = parse_stream_fields(Vec::new(), &vh.value) else {
            continue;
        };
        let hits = vh.hits;
        for field in &fields {
            f(field, hits);
        }
    }
}

/// Go `part.search`: dispatch to tenantID- or streamID-keyed block scheduling.
fn schedule_part_search<'s>(
    works: &mut Vec<BlockSearchWork<'s>>,
    p: &'s Part<'s>,
    pso: &'s PartitionSearchOptions<'s>,
    qs: &QueryStats,
) {
    if !pso.tenant_ids.is_empty() {
        schedule_by_tenant_ids(works, p, pso, qs);
    } else {
        schedule_by_stream_ids(works, p, pso, qs);
    }
}

/// Go `part.searchByTenantIDs`.
fn schedule_by_tenant_ids<'s>(
    works: &mut Vec<BlockSearchWork<'s>>,
    p: &'s Part<'s>,
    pso: &'s PartitionSearchOptions<'s>,
    qs: &QueryStats,
) {
    let mut tenant_ids: &[TenantID] = &pso.tenant_ids;
    let mut ibhs: &[IndexBlockHeader] = &p.index_block_headers;
    let mut bhs_buf: Vec<BlockHeader> = Vec::new();

    while !ibhs.is_empty() && !tenant_ids.is_empty() {
        // Locate a tenantID equal or bigger than the one in ibhs[0].
        let mut tenant_id: TenantID = tenant_ids[0];
        if tenant_id.less(&ibhs[0].stream_id.tenant_id) {
            tenant_id = ibhs[0].stream_id.tenant_id;
            let t = tenant_id;
            let m = sort_search(tenant_ids.len(), |i| !tenant_ids[i].less(&t));
            if m == tenant_ids.len() {
                break;
            }
            tenant_id = tenant_ids[m];
            tenant_ids = &tenant_ids[m..];
        }

        // Locate the indexBlockHeader with equal or bigger tenantID.
        let mut n = 0usize;
        if ibhs[0].stream_id.tenant_id.less(&tenant_id) {
            let t = tenant_id;
            n = sort_search(ibhs.len(), |i| !ibhs[i].stream_id.tenant_id.less(&t));
            n -= 1;
        }
        let ibh = &ibhs[n];
        let skip = pso.min_timestamp > ibh.max_timestamp || pso.max_timestamp < ibh.min_timestamp;
        if !skip {
            must_read_block_headers(&mut bhs_buf, ibh, p, qs);
        }
        ibhs = &ibhs[n + 1..];
        if skip {
            continue;
        }

        let mut bhs: &[BlockHeader] = &bhs_buf;
        loop {
            let t = tenant_id;
            let m = sort_search(bhs.len(), |i| !bhs[i].stream_id.tenant_id.less(&t));
            bhs = &bhs[m..];
            while !bhs.is_empty() && bhs[0].stream_id.tenant_id.equal(&tenant_id) {
                let bh = &bhs[0];
                let th = &bh.timestamps_header;
                if !(pso.min_timestamp > th.max_timestamp || pso.max_timestamp < th.min_timestamp) {
                    works.push(BlockSearchWork {
                        p,
                        pso,
                        bh: bh.clone(),
                    });
                }
                bhs = &bhs[1..];
            }
            if bhs.is_empty() {
                break;
            }
            // Find the next tenantID matching the one in bhs[0].
            tenant_id = bhs[0].stream_id.tenant_id;
            let t = tenant_id;
            let m = sort_search(tenant_ids.len(), |i| !tenant_ids[i].less(&t));
            if m == tenant_ids.len() {
                tenant_ids = &[];
                break;
            }
            tenant_id = tenant_ids[m];
            tenant_ids = &tenant_ids[m..];
        }
    }
}

/// Go `part.searchByStreamIDs`.
fn schedule_by_stream_ids<'s>(
    works: &mut Vec<BlockSearchWork<'s>>,
    p: &'s Part<'s>,
    pso: &'s PartitionSearchOptions<'s>,
    qs: &QueryStats,
) {
    let mut stream_ids: &[StreamID] = &pso.stream_ids;
    let mut ibhs: &[IndexBlockHeader] = &p.index_block_headers;
    let mut bhs_buf: Vec<BlockHeader> = Vec::new();

    while !ibhs.is_empty() && !stream_ids.is_empty() {
        let mut stream_id: StreamID = stream_ids[0];
        if stream_id.less(&ibhs[0].stream_id) {
            stream_id = ibhs[0].stream_id;
            let s = stream_id;
            let m = sort_search(stream_ids.len(), |i| !stream_ids[i].less(&s));
            if m == stream_ids.len() {
                break;
            }
            stream_id = stream_ids[m];
            stream_ids = &stream_ids[m..];
        }

        let mut n = 0usize;
        if ibhs[0].stream_id.less(&stream_id) {
            let s = stream_id;
            n = sort_search(ibhs.len(), |i| !ibhs[i].stream_id.less(&s));
            n -= 1;
        }
        let ibh = &ibhs[n];
        let skip = pso.min_timestamp > ibh.max_timestamp || pso.max_timestamp < ibh.min_timestamp;
        if !skip {
            must_read_block_headers(&mut bhs_buf, ibh, p, qs);
        }
        ibhs = &ibhs[n + 1..];
        if skip {
            continue;
        }

        let mut bhs: &[BlockHeader] = &bhs_buf;
        loop {
            let s = stream_id;
            let m = sort_search(bhs.len(), |i| !bhs[i].stream_id.less(&s));
            bhs = &bhs[m..];
            while !bhs.is_empty() && bhs[0].stream_id.equal(&stream_id) {
                let bh = &bhs[0];
                let th = &bh.timestamps_header;
                if !(pso.min_timestamp > th.max_timestamp || pso.max_timestamp < th.min_timestamp) {
                    works.push(BlockSearchWork {
                        p,
                        pso,
                        bh: bh.clone(),
                    });
                }
                bhs = &bhs[1..];
            }
            if bhs.is_empty() {
                break;
            }
            stream_id = bhs[0].stream_id;
            let s = stream_id;
            let m = sort_search(stream_ids.len(), |i| !stream_ids[i].less(&s));
            if m == stream_ids.len() {
                stream_ids = &[];
                break;
            }
            stream_id = stream_ids[m];
            stream_ids = &stream_ids[m..];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, values: &[&str]) -> BlockColumn {
        BlockColumn {
            name: name.to_string(),
            values: values.iter().map(|v| v.as_bytes().to_vec()).collect(),
        }
    }

    // -- End-to-end RunQuery integration test -------------------------------
    //
    // Ingests a handful of rows into a temp Storage and drives the three
    // benchmark queries through `Storage::run_query`, asserting the row counts
    // and the `stats count()` value.

    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;

    use crate::log_rows::get_log_rows;
    use crate::parser::ParseQuery;
    use crate::storage::{Storage, StorageConfig};
    use crate::tenant_id::TenantID;

    fn now_nanos() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    }

    fn run_query_temp_path(name: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("esl-logstorage-runquery-{name}-{n}"))
    }

    /// Sums the rows across every DataBlock streamed to `run_query`.
    fn count_rows(storage: &Arc<Storage>, tenant: TenantID, query: &str) -> u64 {
        let q = ParseQuery(query).expect("parse query");
        let total = Arc::new(AtomicU64::new(0));
        let total_cl = Arc::clone(&total);
        let write: WriteDataBlockFn = Arc::new(move |_wid, db: &mut DataBlock| {
            total_cl.fetch_add(db.rows_count() as u64, Ordering::SeqCst);
        });
        storage.run_query(&[tenant], &q, write).expect("run_query");
        total.load(Ordering::SeqCst)
    }

    #[test]
    fn test_run_query_end_to_end() {
        let path = run_query_temp_path("e2e");
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);

        let tenant = TenantID {
            account_id: 0,
            project_id: 0,
        };

        // Ingest 5 rows: 2 contain the token "error" in _msg, 3 do not.
        let msgs = [
            "connection error occurred",
            "all systems nominal",
            "disk error on node 3",
            "request completed ok",
            "cache warmed",
        ];
        let mut lr = get_log_rows(&["host"], &[], &[], &[], "");
        let base = now_nanos();
        for (i, msg) in msgs.iter().enumerate() {
            let mut fields = vec![
                Field {
                    name: "_msg".to_string(),
                    value: msg.to_string(),
                },
                Field {
                    name: "host".to_string(),
                    value: "node-1".to_string(),
                },
            ];
            lr.must_add(tenant, base + i as i64, &mut fields, -1);
        }
        s.must_add_rows(&lr);
        s.debug_flush();

        // Query 1: `*` matches all rows.
        assert_eq!(count_rows(&s, tenant, "*"), 5, "`*` must match all rows");

        // Query 2: `error` phrase matches the two rows with "error".
        assert_eq!(
            count_rows(&s, tenant, "error"),
            2,
            "`error` must match the two error rows"
        );

        // Query 3: `* | stats count() rows` yields a single row: count == 5.
        {
            let q = ParseQuery("* | stats count() rows").expect("parse stats query");
            let captured: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
            let cap = Arc::clone(&captured);
            let write: WriteDataBlockFn = Arc::new(move |_wid, db: &mut DataBlock| {
                let n = db.rows_count();
                for i in 0..n {
                    for c in &db.columns {
                        cap.lock().unwrap().push((
                            c.name.clone(),
                            String::from_utf8_lossy(&c.values[i]).into_owned(),
                        ));
                    }
                }
            });
            s.run_query(&[tenant], &q, write).expect("run_query stats");
            let rows = captured.lock().unwrap();
            assert_eq!(rows.len(), 1, "stats count() must emit exactly one value");
            assert_eq!(rows[0].0, "rows", "stats result column name");
            assert_eq!(rows[0].1, "5", "stats count() value");
        }

        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    // -- ValuesWithHits query-surface tests ----------------------------------
    //
    // Port of the `field_names-*` / `field_values-*` / `stream_field_*` /
    // `streams` / `stream_ids` subtests of Go `TestStorageRunQuery`
    // (storage_search_test.go). The storage layout matches the Go test:
    // 11 tenants x 3 streams x 5 blocks x 7 rows = 1155 rows.

    #[test]
    fn test_storage_run_query_values_with_hits() {
        const TENANTS_COUNT: u32 = 11;
        const STREAMS_PER_TENANT: usize = 3;
        const BLOCKS_PER_STREAM: usize = 5;
        const ROWS_PER_BLOCK: usize = 7;

        fn field(name: &str, value: &str) -> Field {
            Field {
                name: name.to_string(),
                value: value.to_string(),
            }
        }

        fn vh(value: &str, hits: u64) -> ValueWithHits {
            ValueWithHits {
                value: value.to_string(),
                hits,
            }
        }

        let path = run_query_temp_path("values-with-hits");
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);

        // fill the storage with data
        let mut all_tenant_ids: Vec<TenantID> = Vec::new();
        let base_timestamp = now_nanos() - 3600 * 1_000_000_000;
        let stream_tags = ["job", "instance"];
        for i in 0..TENANTS_COUNT {
            let tenant_id = TenantID {
                account_id: i,
                project_id: 10 * i + 1,
            };
            all_tenant_ids.push(tenant_id);
            for j in 0..STREAMS_PER_TENANT {
                let stream_id_value = format!("stream_id={j}");
                for k in 0..BLOCKS_PER_STREAM {
                    let mut lr = get_log_rows(&stream_tags, &[], &[], &[], "");
                    for m in 0..ROWS_PER_BLOCK {
                        let timestamp = base_timestamp + m as i64 * 1_000_000_000 + k as i64;
                        let mut fields = vec![
                            field("job", "foobar"),
                            field("instance", &format!("host-{j}:234")),
                            field("_msg", &format!("log message {m} at block {k}")),
                            field("source-file", "/foo/bar/baz"),
                            field("tenant.id", &tenant_id.to_string()),
                            field("stream-id", &stream_id_value),
                        ];
                        lr.must_add(tenant_id, timestamp, &mut fields, -1);
                    }
                    s.must_add_rows(&lr);
                }
            }
        }
        s.debug_flush();

        let parse = |q: &str| ParseQuery(q).expect("parse query");

        // field_names-all
        {
            let q = parse("*");
            let results = s
                .get_field_names(&all_tenant_ids, &q, "")
                .expect("get_field_names");
            let results_expected = vec![
                vh("_msg", 1155),
                vh("_stream", 1155),
                vh("_stream_id", 1155),
                vh("_time", 1155),
                vh("instance", 1155),
                vh("job", 1155),
                vh("source-file", 1155),
                vh("stream-id", 1155),
                vh("tenant.id", 1155),
            ];
            assert_eq!(results, results_expected, "field_names-all");
        }

        // field_names-with-filter
        {
            let q = parse("*");
            let results = s
                .get_field_names(&all_tenant_ids, &q, "o")
                .expect("get_field_names");
            let results_expected = vec![vh("job", 1155), vh("source-file", 1155)];
            assert_eq!(results, results_expected, "field_names-with-filter");
        }

        // field_names-some (Go filters with `_stream:{instance=~"host-1:.+"}`;
        // stream-filter execution resolves streamIDs against each partition's
        // indexdb — see filter_stream.rs).
        {
            let q = parse(r#"_stream:{instance=~"host-1:.+"}"#);
            let results = s
                .get_field_names(&all_tenant_ids, &q, "")
                .expect("get_field_names");
            let results_expected = vec![
                vh("_msg", 385),
                vh("_stream", 385),
                vh("_stream_id", 385),
                vh("_time", 385),
                vh("instance", 385),
                vh("job", 385),
                vh("source-file", 385),
                vh("stream-id", 385),
                vh("tenant.id", 385),
            ];
            assert_eq!(results, results_expected, "field_names-some");
        }

        // field_values-nolimit
        {
            let q = parse("*");
            let results = s
                .get_field_values(&all_tenant_ids, &q, "_stream", "", 0)
                .expect("get_field_values");
            let results_expected = vec![
                vh(r#"{instance="host-0:234",job="foobar"}"#, 385),
                vh(r#"{instance="host-1:234",job="foobar"}"#, 385),
                vh(r#"{instance="host-2:234",job="foobar"}"#, 385),
            ];
            assert_eq!(results, results_expected, "field_values-nolimit");
        }

        // field_values-with-filter
        {
            let q = parse("*");
            let results = s
                .get_field_values(&all_tenant_ids, &q, "_stream", "1:23", 0)
                .expect("get_field_values");
            let results_expected = vec![vh(r#"{instance="host-1:234",job="foobar"}"#, 385)];
            assert_eq!(results, results_expected, "field_values-with-filter");
        }

        // field_values-limit-reached
        {
            let q = parse("*");
            let results = s
                .get_field_values(&all_tenant_ids, &q, "_stream", "", 3)
                .expect("get_field_values");
            let results_expected = vec![
                vh(r#"{instance="host-0:234",job="foobar"}"#, 385),
                vh(r#"{instance="host-1:234",job="foobar"}"#, 385),
                vh(r#"{instance="host-2:234",job="foobar"}"#, 385),
            ];
            assert_eq!(results, results_expected, "field_values-limit-reached");
        }

        // field_values-limit-not-reached
        {
            let q = parse("instance:='host-1:234'");
            let results = s
                .get_field_values(&all_tenant_ids, &q, "_stream", "", 4)
                .expect("get_field_values");
            let results_expected = vec![vh(r#"{instance="host-1:234",job="foobar"}"#, 385)];
            assert_eq!(results, results_expected, "field_values-limit-not-reached");
        }

        // stream_field_names
        {
            let q = parse("*");
            let results = s
                .get_stream_field_names(&all_tenant_ids, &q, "")
                .expect("get_stream_field_names");
            let results_expected = vec![vh("instance", 1155), vh("job", 1155)];
            assert_eq!(results, results_expected, "stream_field_names");
        }

        // stream_field_names-with-filter
        {
            let q = parse("*");
            let results = s
                .get_stream_field_names(&all_tenant_ids, &q, "ob")
                .expect("get_stream_field_names");
            let results_expected = vec![vh("job", 1155)];
            assert_eq!(results, results_expected, "stream_field_names-with-filter");
        }

        // stream_field_values-nolimit
        {
            let q = parse("*");
            let results = s
                .get_stream_field_values(&all_tenant_ids, &q, "instance", "", 0)
                .expect("get_stream_field_values");
            let results_expected = vec![
                vh("host-0:234", 385),
                vh("host-1:234", 385),
                vh("host-2:234", 385),
            ];
            assert_eq!(results, results_expected, "stream_field_values-nolimit");
        }

        // stream_field_values-with-filter
        {
            let q = parse("*");
            let results = s
                .get_stream_field_values(&all_tenant_ids, &q, "instance", "t-2", 0)
                .expect("get_stream_field_values");
            let results_expected = vec![vh("host-2:234", 385)];
            assert_eq!(results, results_expected, "stream_field_values-with-filter");
        }

        // stream_field_values-limit
        {
            let q = parse("*");
            let values = s
                .get_stream_field_values(&all_tenant_ids, &q, "instance", "", 3)
                .expect("get_stream_field_values");
            let results_expected = vec![
                vh("host-0:234", 385),
                vh("host-1:234", 385),
                vh("host-2:234", 385),
            ];
            assert_eq!(values, results_expected, "stream_field_values-limit");
        }

        // streams
        {
            let q = parse("*");
            let results = s.get_streams(&all_tenant_ids, &q, 0).expect("get_streams");
            let results_expected = vec![
                vh(r#"{instance="host-0:234",job="foobar"}"#, 385),
                vh(r#"{instance="host-1:234",job="foobar"}"#, 385),
                vh(r#"{instance="host-2:234",job="foobar"}"#, 385),
            ];
            assert_eq!(results, results_expected, "streams");
        }

        // stream_ids
        {
            let q = parse("*");
            let mut results = s
                .get_stream_ids(&all_tenant_ids, &q, 0)
                .expect("get_stream_ids");

            // Verify the first 5 results with the smallest _stream_id value.
            results.sort_by(|a, b| a.value.cmp(&b.value));
            results.truncate(5);

            let results_expected = vec![
                vh("000000000000000140c1914be0226f8185f5b00551fb3b2d", 35),
                vh("000000000000000177edafcd46385c778b57476eb5b92233", 35),
                vh("0000000000000001f5b4cae620b5e85d6ef5f2107fe00274", 35),
                vh("000000010000000b40c1914be0226f8185f5b00551fb3b2d", 35),
                vh("000000010000000b77edafcd46385c778b57476eb5b92233", 35),
            ];
            assert_eq!(results, results_expected, "stream_ids");
        }

        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_value_with_hits_marshal_unmarshal() {
        let vh = ValueWithHits {
            value: "foo".to_string(),
            hits: 1234,
        };
        let mut data = Vec::new();
        vh.marshal(&mut data);

        let mut vh2 = ValueWithHits::default();
        let consumed = vh2.unmarshal_inplace(&data).expect("unmarshal");
        assert_eq!(consumed, data.len(), "unexpected non-empty tail");
        assert_eq!(vh, vh2);
    }

    #[test]
    fn test_data_block_marshal_unmarshal() {
        let check = |db: &DataBlock| {
            let mut data = Vec::new();
            db.marshal(&mut data);
            let mut db2 = DataBlock::default();
            let consumed = db2.unmarshal_inplace(&data).expect("unmarshal");
            assert_eq!(consumed, data.len(), "unexpected non-empty tail");
            assert_eq!(db, &db2);
        };

        // empty DataBlock
        check(&DataBlock::default());

        // Zero rows, non-zero columns
        check(&DataBlock {
            columns: vec![col("foo", &[]), col("bar", &[])],
        });

        // Non-zero rows, non-zero columns
        check(&DataBlock {
            columns: vec![
                col("foo", &["a", "b", "c"]),
                col("bar", &["", "sfdsffs", ""]),
            ],
        });

        // Const columns
        check(&DataBlock {
            columns: vec![col("foo", &["a", "a", "a"]), col("bar", &["x", "y", "z"])],
        });

        // Timestamp column
        check(&DataBlock {
            columns: vec![col(
                "_time",
                &[
                    "2025-01-20T10:20:30Z",
                    "2025-01-20T10:20:30.124Z",
                    "2025-01-20T10:20:30.123456789Z",
                ],
            )],
        });

        // Non-zero columns, plus timestamps column
        check(&DataBlock {
            columns: vec![
                col("foo", &["a", "a", "a"]),
                col(
                    "_time",
                    &[
                        "2025-01-20T10:20:30Z",
                        "2025-01-20T10:20:30.124Z",
                        "2025-01-20T10:20:30.123456789Z",
                    ],
                ),
            ],
        });
    }

    #[test]
    fn test_parse_stream_fields_success() {
        let check = |s: &str, expected: &str| {
            let fields = parse_stream_fields(Vec::new(), s).expect("parse");
            let mut out = Vec::new();
            crate::rows::marshal_fields_to_json(&mut out, &fields);
            assert_eq!(
                to_unsafe_string(&out),
                expected,
                "unexpected result for {s}"
            );
        };

        check("{}", "{}");
        check(r#"{foo="bar"}"#, r#"{"foo":"bar"}"#);
        check(r#"{a="b",c="d"}"#, r#"{"a":"b","c":"d"}"#);
        check(r#"{a="a=,b\"c}",b="d"}"#, r#"{"a":"a=,b\"c}","b":"d"}"#);
    }
}

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
//! **Stream pre-filtering** is ported: [`get_common_stream_filter`] extracts a
//! top-level `{...}` filter, `partition_search_options` resolves its
//! streamIDs against each partition's indexdb before any block is scheduled,
//! and `Query::get_stream_ids` extracts `_stream_id:...` literal lists — both
//! feed `pso.stream_ids`, so `schedule_by_stream_ids` binary-searches the
//! sorted block headers and skips non-matching blocks entirely (Go
//! `sso.streamFilter`/`sso.streamIDs` -> `pso.streamIDs`).
//!
//! **Subqueries** (`in(<subquery>)` filters, `join`/`union` subqueries, the
//! `stream_context` surrounding-logs seam) are ported: [`init_subqueries`]
//! mirrors Go `initSubqueries` (including `initStreamContextPipes`) and runs
//! at the top of [`run_query`] (see its PORT NOTE for the clone-by-reparse
//! divergence).
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
/// PORT NOTE: the `hiddenFieldsFilter` pass is deferred (see the module
/// deferral notes). `filter`/`stream_filter` are borrowed from the query (the
/// `Filter` trait has no clone hook and the query outlives the search).
struct StorageSearchOptions<'f> {
    tenant_ids: Vec<TenantID>,
    stream_ids: Vec<StreamID>,
    min_timestamp: i64,
    max_timestamp: i64,
    /// An optional stream filter to use for the search before applying the
    /// filter: per partition, its matching streamIDs are resolved against the
    /// partition indexdb so block scheduling skips non-matching blocks
    /// entirely (Go `sso.streamFilter` -> `pso.streamIDs`).
    stream_filter: Option<&'f crate::stream_filter::StreamFilter>,
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
    let stream_filter = get_common_stream_filter(filter);
    let fields_filter = q.get_needed_columns();

    let mut tenant_ids = tenant_ids.to_vec();
    tenant_ids.sort_by(tenant_id_cmp);

    StorageSearchOptions {
        tenant_ids,
        stream_ids,
        min_timestamp,
        max_timestamp,
        stream_filter,
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

/// Go `getCommonStreamFilter`: extracts the stream filter when the query's
/// top-level filter is (or ANDs with) a single `filterStream`.
///
/// PORT NOTE: Go also strips the extracted `filterStream` from the returned
/// filter (returning `(sf, remaining)`); the port borrows the query's filter
/// (no clone hook on the `Filter` trait), so the filter tree stays intact and
/// the extracted filter is re-checked per scheduled block by
/// `FilterStream::apply_to_block_search` — an O(1) cached-set containment test
/// against blocks that already passed the streamID scheduling, so results and
/// scheduling behavior match Go. Go's bare-`filterStream` arm returns `t.f`
/// without the `isEmpty` guard; the port guards both arms (an empty `{...}`
/// matches all streams, so pre-filtering by it would be wrong and Go never
/// resolves it either: `FilterStream` short-circuits empty filters).
fn get_common_stream_filter(
    f: &dyn crate::filter::Filter,
) -> Option<&crate::stream_filter::StreamFilter> {
    if let Some(children) = f.and_children() {
        for child in children {
            if let Some(sf) = child.as_stream_filter()
                && !sf.is_empty()
            {
                return Some(sf);
            }
        }
        return None;
    }
    f.as_stream_filter().filter(|sf| !sf.is_empty())
}

/// Go `intersectStreamIDs`.
fn intersect_stream_ids(a: Vec<StreamID>, b: &[StreamID]) -> Vec<StreamID> {
    let m: std::collections::HashSet<StreamID> = b.iter().copied().collect();
    a.into_iter().filter(|sid| m.contains(sid)).collect()
}

/// Go `getStreamIDsForTenantIDs`.
fn get_stream_ids_for_tenant_ids(
    stream_ids: &[StreamID],
    tenant_ids: &[TenantID],
) -> Vec<StreamID> {
    let m: std::collections::HashSet<TenantID> = tenant_ids.iter().copied().collect();
    stream_ids
        .iter()
        .filter(|sid| m.contains(&sid.tenant_id))
        .copied()
        .collect()
}

/// Go `partition.getSearchOptions`.
///
/// When the query carries a common stream filter (Go
/// `sso.streamFilter`), its matching streamIDs are resolved against this
/// partition's indexdb up front, so block scheduling
/// (`schedule_by_stream_ids`) skips non-matching blocks entirely.
///
/// PORT NOTE: Go additionally pre-binds the remaining stream filters here
/// (`initStreamFilters` copies the filter tree binding `idb` + tenantIDs).
/// The port resolves those lazily inside `FilterStream` instead (per-idb
/// cache, see filter_stream.rs), reading the query tenantIDs from
/// `stream_filter_tenant_ids`.
fn partition_search_options<'f>(
    sso: &StorageSearchOptions<'f>,
    pt: &crate::partition::Partition,
) -> PartitionSearchOptions<'f> {
    let mut tenant_ids = sso.tenant_ids.clone();
    let mut stream_ids: Vec<StreamID>;

    if let Some(sf) = sso.stream_filter {
        stream_ids = pt.idb.search_stream_ids(&tenant_ids, sf);
        if !sso.stream_ids.is_empty() {
            stream_ids = intersect_stream_ids(stream_ids, &sso.stream_ids);
        }
        // schedule_by_stream_ids assumes sorted streamIDs (Go relies on
        // searchStreamIDs returning them sorted).
        stream_ids.sort_by(stream_id_cmp);
        tenant_ids = Vec::new();
    } else if !sso.stream_ids.is_empty() {
        stream_ids = get_stream_ids_for_tenant_ids(&sso.stream_ids, &tenant_ids);
        tenant_ids = Vec::new();
    } else {
        stream_ids = Vec::new();
    }

    PartitionSearchOptions {
        tenant_ids,
        stream_ids,
        stream_filter_tenant_ids: sso.tenant_ids.clone(),
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

/// The error message returned by the query path when the caller-provided
/// cancel token is set (external cancellation, e.g. the HTTP client
/// disconnected). Mirrors Go's `context.Canceled` ("context canceled"), which
/// `Storage.RunQuery` returns when the request context is done.
pub const QUERY_CANCELED_ERROR: &str = "context canceled";

/// Returns true when `err` denotes an externally-canceled query
/// ([`QUERY_CANCELED_ERROR`], possibly wrapped with call-site context, like
/// Go's `errors.Is(err, context.Canceled)`). Handlers must treat such errors
/// as "the client is gone: do not write a response".
pub fn is_query_canceled_error(err: &str) -> bool {
    err.contains(QUERY_CANCELED_ERROR)
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
pub(crate) struct BlockResultWriter {
    pub(crate) f: WriteDataBlockFn,
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

/// Terminal pipe processor handing each raw [`BlockResult`] to a
/// `stream_context` [`WriteBlockResultFn`] callback (Go passes the
/// `writeBlock func(workerID uint, br *blockResult)` closure directly into
/// `runQuery`).
struct BlockResultCallbackSink {
    f: crate::pipe_stream_context::WriteBlockResultFn,
}

impl PipeProcessor for BlockResultCallbackSink {
    fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }
        (self.f)(br);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Go `runPipes`: wires the pipe chain between `search` and the terminal
/// `sink`. Shared with `net_query_runner` (Go reuses the same function).
pub(crate) fn run_pipes<F>(
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
/// the port passes `tenant_ids`, the optional external `cancel` token standing
/// in for `ctx.Done()`, and the shared per-query `qs` (Go `qctx.QueryStats`)
/// explicitly. `qs` is `Arc`-shared because subquery closures (`union`) must
/// hold it beyond a borrow. When `cancel` is set the block search aborts
/// promptly and the query returns `Err(`[`QUERY_CANCELED_ERROR`]`)`, like Go
/// returning `context.Canceled`. `cancel` is external-only: run_pipes' internal
/// stop flag (flipped by pipes on benign early-stops such as `limit`, and on
/// state-budget errors) stays per-run, mirroring Go's derived
/// `context.WithCancelCause` — sharing one flag for both would poison
/// sequential runs reusing the token (lastn/csv subqueries) and make benign
/// `limit` stops indistinguishable from disconnects.
pub(crate) fn run_query(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q: &Query,
    write_block_fn: WriteDataBlockFn,
    cancel: Option<&Arc<AtomicBool>>,
    qs: &Arc<QueryStats>,
) -> Result<(), String> {
    let sink: Arc<dyn PipeProcessor> = Arc::new(BlockResultWriter { f: write_block_fn });
    run_query_with_sink(storage, tenant_ids, q, sink, cancel, qs)
}

/// Go `Storage.runQuery`, with the terminal `writeBlock` generalized to a
/// [`PipeProcessor`] sink so `union` subqueries can stream their block results
/// straight into the outer pipeline (Go passes `writeBlockResultFunc`s around
/// instead).
fn run_query_with_sink(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q: &Query,
    sink: Arc<dyn PipeProcessor>,
    cancel: Option<&Arc<AtomicBool>>,
    qs: &Arc<QueryStats>,
) -> Result<(), String> {
    if let Some(c) = cancel
        && c.load(Ordering::SeqCst)
    {
        return Err(QUERY_CANCELED_ERROR.to_string());
    }

    let q_new = init_subqueries(storage, tenant_ids, q, cancel, qs)?;
    let q = q_new.as_ref().unwrap_or(q);

    let sso = get_search_options(tenant_ids, q);

    let concurrency = q.get_concurrency().max(1);
    let workers = q
        .get_parallel_readers(storage.default_parallel_readers)
        .max(1);

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
            search_parallel(
                storage,
                workers,
                &sso,
                qs,
                stop,
                cancel.map(|c| c.as_ref()),
                &skip_block,
                &write_block,
            );
            // Returning the canceled error here (Go: the search loop returning
            // ctx.Err()) makes run_pipes set its internal stop before flushing,
            // so the pipe flushes bail out too where they honor it.
            if let Some(c) = cancel
                && c.load(Ordering::SeqCst)
            {
                return Err(QUERY_CANCELED_ERROR.to_string());
            }
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
#[allow(clippy::too_many_arguments)] // mirrors Go's searchParallel parameter surface + cancel
fn search_parallel(
    storage: &Arc<Storage>,
    workers_count: usize,
    sso: &StorageSearchOptions<'_>,
    qs: &QueryStats,
    stop: &AtomicBool,
    cancel: Option<&AtomicBool>,
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
        psos.push(partition_search_options(sso, &ptw.pt));
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
    let mut works: Vec<BlockSearchWork> = Vec::new();
    for (ptr, pso_idx) in &part_refs {
        // SAFETY: `ptr` was produced by `PartWrapper::part_ptr` for a part
        // held referenced in `pws_hold` for the whole scope; its refCount is
        // > 0, so the part is not closed and its address is pinned.
        let p: &Part = unsafe { &**ptr };
        schedule_part_search(&mut works, p, &psos[*pso_idx], qs);
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
        if stop.load(Ordering::SeqCst) || cancel.is_some_and(|c| c.load(Ordering::SeqCst)) {
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
    /// PORT NOTE: Go takes a `*QueryContext`; the port passes `tenant_ids`,
    /// `q` and the optional external `cancel` token explicitly (see
    /// [`Storage::run_query`]). Go clones `q` shallowly (sharing the filter
    /// and pipes); Rust filters/pipes are single-owner trait objects, so the
    /// query is cloned via re-parsing ([`Query::clone`]).
    pub fn get_field_names(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &Query,
        filter: &str,
        cancel: Option<&Arc<AtomicBool>>,
        qs: &Arc<QueryStats>,
    ) -> Result<Vec<ValueWithHits>, String> {
        let mut q_new = q.clone(q.get_timestamp());

        let mut pipe_str = "field_names".to_string();
        if !filter.is_empty() {
            pipe_str += " filter ";
            pipe_str += &crate::parser::quote_token_if_needed(filter);
        }
        // Go sets `pf.isFirstPipe = len(pipes) == 0`, enabling the pipe's
        // columns-header fast path (names read per block without
        // materializing the columns).
        let mut p = crate::parser::parse_pipe::must_parse_pipe(&pipe_str, q.get_timestamp());
        if q_new.pipes.is_empty() {
            p.mark_first_pipe();
        }
        q_new.pipes.push(p);

        self.run_values_with_hits_query(tenant_ids, &q_new, cancel, qs)
    }

    /// Returns unique values with the number of hits for the given
    /// `field_name` returned by `q` (Go `Storage.GetFieldValues`).
    ///
    /// If `filter` is non-empty, then only the field values containing the
    /// filter substring are returned.
    ///
    /// If `limit > 0`, then up to `limit` unique values are returned.
    #[allow(clippy::too_many_arguments)] // mirrors Go's GetFieldValues + qctx surface
    pub fn get_field_values(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &Query,
        field_name: &str,
        filter: &str,
        limit: u64,
        cancel: Option<&Arc<AtomicBool>>,
        qs: &Arc<QueryStats>,
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

        self.run_values_with_hits_query(tenant_ids, &q_new, cancel, qs)
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
        cancel: Option<&Arc<AtomicBool>>,
        qs: &Arc<QueryStats>,
    ) -> Result<Vec<ValueWithHits>, String> {
        let streams = self.get_streams(tenant_ids, q, u64::MAX, cancel, qs)?;

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
    #[allow(clippy::too_many_arguments)] // mirrors Go's GetStreamFieldValues + qctx surface
    pub fn get_stream_field_values(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &Query,
        field_name: &str,
        filter: &str,
        limit: u64,
        cancel: Option<&Arc<AtomicBool>>,
        qs: &Arc<QueryStats>,
    ) -> Result<Vec<ValueWithHits>, String> {
        let streams = self.get_streams(tenant_ids, q, u64::MAX, cancel, qs)?;

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
        cancel: Option<&Arc<AtomicBool>>,
        qs: &Arc<QueryStats>,
    ) -> Result<Vec<ValueWithHits>, String> {
        self.get_field_values(tenant_ids, q, "_stream", "", limit, cancel, qs)
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
        cancel: Option<&Arc<AtomicBool>>,
        qs: &Arc<QueryStats>,
    ) -> Result<Vec<ValueWithHits>, String> {
        self.get_field_values(tenant_ids, q, "_stream_id", "", limit, cancel, qs)
    }

    /// Go `Storage.runValuesWithHitsQuery`: runs `q` (which must end with a
    /// two-column `(value, hits)` pipe) and collects the sorted results.
    fn run_values_with_hits_query(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &Query,
        cancel: Option<&Arc<AtomicBool>>,
        qs: &Arc<QueryStats>,
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

        crate::storage_search::run_query(self, tenant_ids, q, write_block, cancel, qs)?;

        let mut results = std::mem::take(&mut *results.lock().unwrap());
        crate::pipe_field_values_local::sort_values_with_hits(&mut results);

        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// Subquery initialization (Go initSubqueries / initFilterInValues /
// initJoinMaps / initUnionQueries and their helpers)
// ---------------------------------------------------------------------------

/// Port of Go `getFieldValuesFunc`: executes the subquery given as rendered
/// text and returns the unique values of the given field
/// (`fn(q_text, q_field_name)`).
pub(crate) type GetFieldValuesFn<'a> = dyn FnMut(&str, &str) -> Result<Vec<String>, String> + 'a;

/// Port of Go `getJoinRowsFunc`: executes the join subquery given as rendered
/// text and returns its result rows.
pub(crate) type GetJoinRowsFn<'a> = dyn FnMut(&str) -> Result<Vec<Vec<Field>>, String> + 'a;

/// Port of Go `hasFilterInWithQueryForFilter` (its type-switch on
/// `*filterGeneric` / `*filterStreamID` is the
/// [`crate::filter::Filter::has_filter_in_with_query`] hook).
pub(crate) fn has_filter_in_with_query_for_filter(f: &dyn crate::filter::Filter) -> bool {
    crate::filter::visit_filter_recursive(f, &mut |f| f.has_filter_in_with_query())
}

/// Port of Go `initFilterInValuesForFilter`: rewrites the filter tree,
/// resolving `in(<subquery>)` leaves into literal-values filters via
/// `get_values(q_text, q_field_name)`.
///
/// PORT NOTE: Go rewrites via the generic `copyFilter(visitFunc, copyFunc)`;
/// the port takes ownership and rebuilds the `and`/`or`/`not` composites
/// through the `take_*` hooks (like `remove_star_filters`), substituting
/// leaves via [`crate::filter::Filter::init_filter_in_values`]. Go's
/// `inValuesCache` (keyed by the subquery string) is folded into the
/// `get_values` closure built by [`init_subqueries`].
pub(crate) fn init_filter_in_values_for_filter(
    mut f: Box<dyn crate::filter::Filter>,
    get_values: &mut GetFieldValuesFn<'_>,
) -> Result<Box<dyn crate::filter::Filter>, String> {
    if let Some(children) = f.take_or_children() {
        let children = children
            .into_iter()
            .map(|c| init_filter_in_values_for_filter(c, get_values))
            .collect::<Result<Vec<_>, String>>()?;
        return Ok(Box::new(crate::filter_or::new_filter_or(children)));
    }
    if let Some(children) = f.take_and_children() {
        let children = children
            .into_iter()
            .map(|c| init_filter_in_values_for_filter(c, get_values))
            .collect::<Result<Vec<_>, String>>()?;
        return Ok(Box::new(crate::filter_and::new_filter_and(children)));
    }
    if let Some(child) = f.take_not_child() {
        let child = init_filter_in_values_for_filter(child, get_values)?;
        return Ok(Box::new(crate::filter_not::new_filter_not(child)));
    }
    if let Some(f_new) = f.init_filter_in_values(get_values)? {
        return Ok(f_new);
    }
    Ok(f)
}

/// [`init_filter_in_values_for_filter`] over an `Arc`-shared filter (the
/// `if (...)` filters embedded in pipes, and `pipe_filter`'s filter). Returns
/// `Some(new)` only when the filter embeds an `in(<subquery>)`.
///
/// PORT NOTE: Go rewrites the shared tree via `copyFilter`, sharing unchanged
/// children by pointer; `Arc<dyn Filter>` children cannot be re-owned, so the
/// tree is re-parsed from its rendered text at the query `timestamp` (the
/// established `Query::clone` render/re-parse divergence) and the owned tree
/// is rewritten.
pub(crate) fn init_filter_in_values_for_shared_filter(
    f: &Arc<dyn crate::filter::Filter>,
    get_values: &mut GetFieldValuesFn<'_>,
    timestamp: i64,
) -> Result<Option<Arc<dyn crate::filter::Filter>>, String> {
    if !has_filter_in_with_query_for_filter(f.as_ref()) {
        return Ok(None);
    }
    let text = f.to_string();
    let q = crate::parser::ParseQueryAtTimestamp(&text, timestamp)
        .map_err(|e| format!("BUG: cannot re-parse filter [{text}]: {e}"))?;
    if !q.pipes().is_empty() {
        return Err(format!(
            "BUG: unexpected pipes when re-parsing filter [{text}]"
        ));
    }
    let f_new = init_filter_in_values_for_filter(q.f, get_values)?;
    Ok(Some(Arc::from(f_new)))
}

/// Port of Go `initFilterInValues` (query level): rewrites the global filter,
/// the top-level filter and the pipe-embedded filters of `q`.
pub(crate) fn init_filter_in_values_for_query(
    q: &mut Query,
    get_values: &mut GetFieldValuesFn<'_>,
    timestamp: i64,
) -> Result<(), String> {
    if !q.has_filter_in_with_query() {
        return Ok(());
    }

    if let Some(gf) = q.opts.global_filter.take() {
        let gf = if has_filter_in_with_query_for_filter(gf.as_ref()) {
            init_filter_in_values_for_filter(gf, get_values)?
        } else {
            gf
        };
        q.opts.global_filter = Some(gf);
    }

    if has_filter_in_with_query_for_filter(q.f.as_ref()) {
        let f = std::mem::replace(&mut q.f, Box::new(crate::filter_noop::new_filter_noop()));
        q.f = init_filter_in_values_for_filter(f, get_values)?;
    }

    if q.pipes.iter().any(|p| p.has_filter_in_with_query()) {
        // Go initFilterInValuesForPipes rebuilds the pipe slice; the port
        // rewrites the (query-owned) pipes in place.
        for p in &mut q.pipes {
            p.init_filter_in_values(get_values, timestamp)?;
        }
    }

    Ok(())
}

/// Port of Go `isLastPipeUniq`.
pub(crate) fn is_last_pipe_uniq(pipes: &[Box<dyn crate::pipe::Pipe>]) -> bool {
    pipes.last().is_some_and(|p| p.is_uniq_pipe())
}

/// Port of Go `getFieldValuesGeneric`: appends `| uniq by (field_name)` to `q`
/// (unless it already ends with a `uniq` pipe) and collects the resulting
/// unique values.
///
/// PORT NOTE: Go shards the collected values per CPU with a chunked allocator;
/// the port uses a single mutex-guarded Vec (subquery value sets are small).
/// Go's `// TODO: track memory usage` applies here too.
fn get_field_values_generic(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q: &Query,
    field_name: &str,
    cancel: Option<&Arc<AtomicBool>>,
    qs: &Arc<QueryStats>,
) -> Result<Vec<String>, String> {
    let q_holder;
    let q = if is_last_pipe_uniq(q.pipes()) {
        q
    } else {
        let mut q_new = q.clone(q.get_timestamp());
        let quoted_field_name = crate::parser::quote_token_if_needed(field_name);
        q_new.must_append_pipe(&format!("uniq by ({quoted_field_name})"));
        q_holder = q_new;
        &q_holder
    };

    let values: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let values_w = Arc::clone(&values);
    let write_block: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
        if db.rows_count() == 0 {
            return;
        }

        let cs = db.get_columns(false);
        if cs.len() != 1 {
            esl_common::panicf!("BUG: expecting one column; got {} columns", cs.len());
        }

        let mut dst = values_w.lock().unwrap();
        for v in &cs[0].values {
            dst.push(String::from_utf8_lossy(v).into_owned());
        }
    });

    run_query(storage, tenant_ids, q, write_block, cancel, qs)?;

    let values = std::mem::take(&mut *values.lock().unwrap());
    Ok(values)
}

/// Port of Go `getRows`: runs `q` and collects its result rows (dropping
/// empty-valued fields), bounded by a state-size budget of 20% of the allowed
/// memory.
///
/// PORT NOTE: Go shards rows per worker (`atomicutil.Slice`) with per-shard
/// budget chunks stolen from the global budget and `unsafe.Sizeof`-based
/// accounting; the port uses one mutex-guarded Vec and subtracts an
/// equivalent per-block size estimate from the shared budget. Observable
/// behavior matches: rows are collected until the budget is exhausted, in
/// which case an error is returned.
fn get_rows(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q: &Query,
    cancel: Option<&Arc<AtomicBool>>,
    qs: &Arc<QueryStats>,
) -> Result<Vec<Vec<Field>>, String> {
    let max_state_size = (esl_common::memory::allowed() as f64 * 0.2) as i64;
    let state_size_budget = Arc::new(std::sync::atomic::AtomicI64::new(max_state_size));
    let rows: Arc<std::sync::Mutex<Vec<Vec<Field>>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

    let rows_w = Arc::clone(&rows);
    let budget_w = Arc::clone(&state_size_budget);
    let write_block: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
        if db.rows_count() == 0 {
            return;
        }
        if budget_w.load(Ordering::SeqCst) < 0 {
            // The state size is too big. Stop processing data in order to
            // avoid OOM crash.
            return;
        }

        let rows_count = db.rows_count();
        let cs = db.get_columns(false);

        let mut block_rows: Vec<Vec<Field>> = Vec::with_capacity(rows_count);
        let mut block_size = 0i64;
        for row_idx in 0..rows_count {
            let mut fields: Vec<Field> = Vec::with_capacity(cs.len());
            for c in cs {
                let v = &c.values[row_idx];
                if v.is_empty() {
                    continue;
                }
                let name = c.name.clone();
                let value = String::from_utf8_lossy(v).into_owned();
                block_size +=
                    (name.len() + value.len()) as i64 + 2 * std::mem::size_of::<String>() as i64;
                fields.push(Field { name, value });
            }
            block_size += std::mem::size_of::<Vec<Field>>() as i64;
            block_rows.push(fields);
        }
        budget_w.fetch_sub(block_size, Ordering::SeqCst);
        rows_w.lock().unwrap().extend(block_rows);
    });

    run_query(storage, tenant_ids, q, write_block, cancel, qs)?;

    if state_size_budget.load(Ordering::SeqCst) < 0 {
        return Err(format!(
            "cannot load rows for [{q}] because they occupy more than {}MB of memory",
            max_state_size / (1 << 20)
        ));
    }

    let rows = std::mem::take(&mut *rows.lock().unwrap());
    Ok(rows)
}

/// Port of Go `initSubqueries`: resolves `in(<subquery>)` filter values,
/// builds `join` maps, wires `union` subqueries and wires the
/// `stream_context` surrounding-logs seam (Go `initStreamContextPipes`)
/// before the search starts. Returns `None` when `q` embeds no subqueries
/// (it is executed as is).
///
/// PORT NOTE: Go rewrites shared filter/pipe trees via `cloneShallow` +
/// `copyFilter`; Rust filters/pipes are single-owner trait objects, so a query
/// with subqueries is cloned via re-parsing ([`Query::clone`]) and rewritten
/// in place. Go's `eagerExecute` mode (cluster-only, `NewNetQueryRunner`) is
/// ported in `net_query_runner::init_subqueries_net`.
fn init_subqueries(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q: &Query,
    cancel: Option<&Arc<AtomicBool>>,
    qs: &Arc<QueryStats>,
) -> Result<Option<Query>, String> {
    let has_in = q.has_filter_in_with_query();
    let has_join = q.pipes().iter().any(|p| p.is_join_pipe());
    let has_union = q.pipes().iter().any(|p| p.is_union_pipe());
    let has_stream_context = q.pipes().iter().any(|p| p.is_stream_context_pipe());
    if !has_in && !has_join && !has_union && !has_stream_context {
        return Ok(None);
    }

    let timestamp = q.get_timestamp();
    let mut q_new = q.clone(timestamp);

    if has_in {
        // Go `getValuesForQuery` caches subquery results in an `inValuesCache`
        // keyed by the subquery string; the cache is folded into the closure.
        let mut cache: HashMap<String, Vec<String>> = HashMap::new();
        let mut get_field_values =
            |q_text: &str, field_name: &str| -> Result<Vec<String>, String> {
                if let Some(values) = cache.get(q_text) {
                    return Ok(values.clone());
                }
                let q_sub = crate::parser::ParseQueryAtTimestamp(q_text, timestamp)
                    .map_err(|e| format!("BUG: cannot parse subquery [{q_text}]: {e}"))?;
                let values =
                    get_field_values_generic(storage, tenant_ids, &q_sub, field_name, cancel, qs)?;
                cache.insert(q_text.to_string(), values.clone());
                Ok(values)
            };
        init_filter_in_values_for_query(&mut q_new, &mut get_field_values, timestamp)
            .map_err(|e| format!("cannot initialize `in` subqueries: {e}"))?;
    }

    if has_join {
        // Go `initJoinMaps` (its `*pipeJoin` type-switch is the
        // `Pipe::init_join_map` hook).
        let mut get_join_rows = |q_text: &str| -> Result<Vec<Vec<Field>>, String> {
            let q_sub = crate::parser::ParseQueryAtTimestamp(q_text, timestamp)
                .map_err(|e| format!("BUG: cannot parse subquery [{q_text}]: {e}"))?;
            get_rows(storage, tenant_ids, &q_sub, cancel, qs)
        };
        for p in &mut q_new.pipes {
            p.init_join_map(&mut get_join_rows)
                .map_err(|e| format!("cannot initialize `join` subqueries: {e}"))?;
        }
    }

    if has_union {
        // Go `initUnionQueries` (its `*pipeUnion` type-switch is the
        // `Pipe::init_union_query` hook). The wired callback executes the
        // union subquery lazily at the union processor's `flush`, exactly like
        // Go's single-node path (`eagerExecute == false`).
        let storage_u = Arc::clone(storage);
        let tenant_ids_u: Vec<TenantID> = tenant_ids.to_vec();
        let cancel_u: Option<Arc<AtomicBool>> = cancel.cloned();
        let qs_u = Arc::clone(qs);
        let run_union_query: crate::pipe::RunUnionQueryFn =
            Arc::new(move |q_text, sink| -> Result<(), String> {
                let q_sub = crate::parser::ParseQueryAtTimestamp(q_text, timestamp)
                    .map_err(|e| format!("BUG: cannot parse subquery [{q_text}]: {e}"))?;
                run_query_with_sink(
                    &storage_u,
                    &tenant_ids_u,
                    &q_sub,
                    sink,
                    cancel_u.as_ref(),
                    &qs_u,
                )
            });
        for p in &mut q_new.pipes {
            p.init_union_query(&run_union_query)
                .map_err(|e| format!("cannot initialize 'union' subqueries: {e}"))?;
        }
    }

    if has_stream_context {
        // Go `initStreamContextPipes`: `stream_context` is only valid as the
        // first pipe (directly after the filter)...
        for i in 1..q_new.pipes.len() {
            if q_new.pipes[i].is_stream_context_pipe() {
                return Err(format!(
                    "[{}] pipe must go after [{}] filter; now it goes after the [{}] pipe",
                    q_new.pipes[i].to_string(),
                    q_new.f.to_string(),
                    q_new.pipes[i - 1].to_string()
                ));
            }
        }

        // ...where it gets the runQuery seam for fetching the surrounding
        // logs, scoped to the tenant encoded in each `_stream_id`
        // (Go `pc.withRunQuery(qctx, runQuery, fieldsFilter)` +
        // `executeQuery`'s ParseQuery/NewQueryContext).
        if q_new
            .pipes
            .first()
            .is_some_and(|p| p.is_stream_context_pipe())
        {
            let fields_filter =
                crate::net_query_runner::to_fields_filters(&q_new.get_needed_columns());

            let storage_sc = Arc::clone(storage);
            let cancel_sc: Option<Arc<AtomicBool>> = cancel.cloned();
            let qs_sc = Arc::clone(qs);
            let run_sc_query: crate::pipe_stream_context::RunQueryFn = Arc::new(
                move |stream_id, q_text, write_block| -> Result<(), String> {
                    let q_sub = crate::parser::ParseQuery(q_text)
                        .map_err(|e| format!("BUG: cannot parse query [{q_text}]: {e}"))?;
                    let Some(tenant_id) =
                        crate::pipe_stream_context::get_tenant_id_from_stream_id_string(stream_id)
                    else {
                        return Err(format!(
                            "BUG: cannot obtain tenantID from streamID {stream_id:?}"
                        ));
                    };
                    let sink: Arc<dyn PipeProcessor> =
                        Arc::new(BlockResultCallbackSink { f: write_block });
                    run_query_with_sink(
                        &storage_sc,
                        &[tenant_id],
                        &q_sub,
                        sink,
                        cancel_sc.as_ref(),
                        &qs_sc,
                    )
                },
            );
            q_new.pipes[0].init_stream_context_query(&run_sc_query, &fields_filter);
        }
    }

    Ok(Some(q_new))
}

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
        // pid + nanos keep the path unique across processes, so a test run
        // killed before its cleanup cannot leak its storage into a later run
        // of the same test (must_open_storage reopens existing data).
        std::env::temp_dir().join(format!(
            "esl-logstorage-runquery-{name}-{}-{}-{n}",
            std::process::id(),
            now_nanos()
        ))
    }

    /// A fresh `Arc<QueryStats>` for surfaces that require one.
    fn test_qs() -> Arc<QueryStats> {
        Arc::new(QueryStats::default())
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

    /// `stream_context` end-to-end: the surrounding-logs seam wired by
    /// `init_subqueries` (Go `initStreamContextPipes`) must return the
    /// before/after context rows around each matching row, and a misplaced
    /// `stream_context` pipe must fail like Go.
    #[test]
    fn test_run_query_stream_context_end_to_end() {
        let path = run_query_temp_path("stream-context");
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);

        // Non-default tenant: proves the seam derives the tenant from the
        // `_stream_id` string (Go getTenantIDFromStreamIDString).
        let tenant = TenantID {
            account_id: 3,
            project_id: 7,
        };

        let msgs = ["one", "two", "three", "four", "five"];
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
            lr.must_add(tenant, base + (i as i64) * 1_000_000, &mut fields, -1);
        }
        s.must_add_rows(&lr);
        s.debug_flush();

        // `three` matches one row; before 1 / after 1 adds `two` and `four`.
        let q = ParseQuery("three | stream_context before 1 after 1").expect("parse query");
        let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let cap = Arc::clone(&captured);
        let write: WriteDataBlockFn = Arc::new(move |_wid, db: &mut DataBlock| {
            let n = db.rows_count();
            let cs = db.get_columns(false);
            let mut dst = cap.lock().unwrap();
            for i in 0..n {
                for c in cs {
                    if c.name == "_msg" {
                        dst.push(String::from_utf8_lossy(&c.values[i]).into_owned());
                    }
                }
            }
        });
        s.run_query(&[tenant], &q, write)
            .expect("run_query stream_context");
        let got = captured.lock().unwrap().clone();
        assert_eq!(
            got,
            vec!["two".to_string(), "three".to_string(), "four".to_string()],
            "stream_context must return the matching row with its before/after context in _time order"
        );

        // Go initStreamContextPipes: stream_context anywhere but first errors.
        let q_bad =
            ParseQuery("* | limit 10 | stream_context after 1").expect("parse misplaced query");
        let noop: WriteDataBlockFn = Arc::new(|_wid, _db: &mut DataBlock| {});
        let err = s
            .run_query(&[tenant], &q_bad, noop)
            .expect_err("misplaced stream_context must fail");
        assert!(
            err.contains("pipe must go after"),
            "unexpected error: {err}"
        );

        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    /// `GetFieldNames` first-pipe fast mode (names read from the per-block
    /// columns-header index) must yield exactly the results of the slow
    /// all-columns mode. The slow mode is forced by a huge pass-through
    /// `limit` pipe in front, which keeps `field_names` out of first-pipe
    /// position without changing the matched rows.
    #[test]
    fn test_get_field_names_fast_path_matches_slow_path() {
        let path = run_query_temp_path("fieldnames-fastpath");
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);
        let all_tenant_ids = fill_run_query_fixture(&s);
        let qs = test_qs();

        let q_fast = ParseQuery("*").expect("parse query");
        let fast = s
            .get_field_names(&all_tenant_ids, &q_fast, "", None, &qs)
            .expect("get_field_names fast");

        let q_slow = ParseQuery("* | limit 999999999").expect("parse query");
        let slow = s
            .get_field_names(&all_tenant_ids, &q_slow, "", None, &qs)
            .expect("get_field_names slow");

        assert!(!fast.is_empty(), "fixture must yield field names");
        assert_eq!(
            fast, slow,
            "first-pipe fast mode must match the all-columns slow mode"
        );

        // The name filter takes the same fast path.
        let fast_filtered = s
            .get_field_names(&all_tenant_ids, &q_fast, "_stream", None, &qs)
            .expect("get_field_names fast filtered");
        let slow_filtered = s
            .get_field_names(&all_tenant_ids, &q_slow, "_stream", None, &qs)
            .expect("get_field_names slow filtered");
        assert_eq!(fast_filtered, slow_filtered);
        assert!(
            fast_filtered.iter().all(|vh| vh.value.contains("_stream")),
            "filtered names must contain the filter substring"
        );

        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    // -- getCommonStreamFilter scheduling pre-filter test ---------------------

    /// A top-level `{...}` filter must prune non-matching blocks at
    /// scheduling time (Go `getCommonStreamFilter` + partition
    /// `getSearchOptions` resolving `pso.streamIDs` before `part.search`),
    /// not merely per-block inside `FilterStream::apply_to_block_search`:
    /// `blocks_processed` counts every *scheduled* block, so it must drop to
    /// the matching stream's share while the results stay identical to the
    /// equivalent per-row field filter.
    #[test]
    fn test_common_stream_filter_prunes_block_scheduling() {
        let path = run_query_temp_path("stream-prune");
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);
        // 11 tenants x 3 streams x 5 blocks x 7 rows = 1155 rows.
        let all_tenant_ids = fill_run_query_fixture(&s);
        // Settle the part set: a background small-part merge between two
        // measurements below would legitimately change the per-query block
        // counts. The forced merge consolidates most of it, but background
        // merges may still consolidate small same-stream blocks afterwards,
        // so the per-row-filter comparison below brackets its measurement
        // with two `*` baselines instead of assuming a stable block count.
        s.must_force_merge("");

        // Runs `query` and returns its sorted result rows plus the number of
        // blocks that were scheduled and processed.
        let run = |query: &str| -> (Vec<TestRow>, u64) {
            let q = ParseQuery(query).unwrap_or_else(|e| panic!("cannot parse [{query}]: {e}"));
            let qs = test_qs();
            let rows: Arc<Mutex<Vec<TestRow>>> = Arc::new(Mutex::new(Vec::new()));
            let rows_w = Arc::clone(&rows);
            let write: WriteDataBlockFn = Arc::new(move |_wid, db: &mut DataBlock| {
                let rows_count = db.rows_count();
                let cs = db.get_columns(false);
                let mut dst = rows_w.lock().unwrap();
                for i in 0..rows_count {
                    let mut row: TestRow = cs
                        .iter()
                        .map(|c| {
                            (
                                c.name.clone(),
                                String::from_utf8_lossy(&c.values[i]).into_owned(),
                            )
                        })
                        .collect();
                    row.sort();
                    dst.push(row);
                }
            });
            s.run_query_with_stats(&all_tenant_ids, &q, write, None, &qs)
                .unwrap_or_else(|e| panic!("cannot run [{query}]: {e}"));
            let mut rows = std::mem::take(&mut *rows.lock().unwrap());
            rows.sort();
            (rows, qs.blocks_processed.load(Ordering::SeqCst))
        };

        // Baseline: `*` schedules every block.
        let (rows_all, blocks_all) = run("*");
        assert_eq!(rows_all.len(), 1155, "`*` must match all rows");

        // `{instance="host-1:234"}` selects 1 of the 3 streams per tenant.
        let (rows_stream, blocks_stream) = run(r#"{instance="host-1:234"}"#);
        assert_eq!(
            rows_stream.len(),
            385,
            "the stream filter must match one stream per tenant"
        );

        // Results must be identical to the equivalent per-row field filter
        // (which cannot use the scheduling pre-filter).
        let (_, blocks_all_before) = run("*");
        let (rows_field, blocks_field) = run("instance:='host-1:234'");
        let (_, blocks_all_after) = run("*");
        assert_eq!(
            rows_stream, rows_field,
            "stream-filter results must match the per-row filter results"
        );

        // The per-row filter still schedules every block: its block count
        // must sit between the `*` baselines measured around it (background
        // merges only ever reduce the total block count).
        assert!(
            blocks_all_after <= blocks_field && blocks_field <= blocks_all_before,
            "a per-row field filter must not prune scheduling: \
             processed {blocks_field} blocks; `*` processed {blocks_all_before} before \
             and {blocks_all_after} after"
        );
        // ...while the stream filter prunes non-matching blocks at scheduling
        // time: only the matching stream's third of the blocks is processed.
        assert!(
            blocks_stream <= blocks_all / 3,
            "stream-filter scheduling must prune non-matching blocks: \
             processed {blocks_stream} of {blocks_all} blocks"
        );
        assert!(blocks_stream > 0, "the matching stream must be scheduled");

        // An ANDed stream filter takes the same scheduling shortcut
        // (Go getCommonStreamFilter's `*filterAnd` arm).
        let (rows_and, blocks_and) = run(r#"{instance="host-1:234"} "message 3""#);
        assert_eq!(rows_and.len(), 55, "one row per block of the stream");
        assert!(
            blocks_and <= blocks_all / 3,
            "an ANDed stream filter must prune scheduling too: \
             processed {blocks_and} of {blocks_all} blocks"
        );

        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    // -- External cancellation (client-disconnect) tests ---------------------

    /// A pre-set cancel token aborts the query before any block is searched,
    /// returning the canceled error (Go: RunQuery returning ctx.Err()).
    #[test]
    fn test_run_query_with_cancel_preset() {
        let path = run_query_temp_path("cancel-preset");
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);
        let all_tenant_ids = fill_run_query_fixture(&s);

        let q = ParseQuery("*").expect("parse query");
        let calls = Arc::new(AtomicU64::new(0));
        let calls_cl = Arc::clone(&calls);
        let write: WriteDataBlockFn = Arc::new(move |_wid, _db: &mut DataBlock| {
            calls_cl.fetch_add(1, Ordering::SeqCst);
        });

        let cancel = Arc::new(AtomicBool::new(true));
        let err = s
            .run_query_with_cancel(&all_tenant_ids, &q, write, Some(&cancel))
            .expect_err("a pre-set cancel token must abort the query");
        assert_eq!(err, QUERY_CANCELED_ERROR);
        assert!(is_query_canceled_error(&err));
        assert!(
            is_query_canceled_error(&format!("cannot execute query: {err}")),
            "wrapped canceled errors must still be detected"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "no block may reach the sink for a pre-canceled query"
        );

        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    /// A cancel flipped mid-query (from the write-block callback, i.e. from
    /// inside the pipeline) aborts the search before all blocks are processed.
    /// `concurrency=1` forces the serial block-search path, which makes the
    /// abort point deterministic: exactly one block reaches the sink.
    #[test]
    fn test_run_query_with_cancel_mid_query() {
        let path = run_query_temp_path("cancel-mid");
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);
        let all_tenant_ids = fill_run_query_fixture(&s);

        // Baseline: without cancel, the fixture yields many blocks.
        let q = ParseQuery("options(concurrency=1) *").expect("parse query");
        let total_blocks = {
            let calls = Arc::new(AtomicU64::new(0));
            let calls_cl = Arc::clone(&calls);
            let write: WriteDataBlockFn = Arc::new(move |_wid, _db: &mut DataBlock| {
                calls_cl.fetch_add(1, Ordering::SeqCst);
            });
            s.run_query(&all_tenant_ids, &q, write).expect("run_query");
            calls.load(Ordering::SeqCst)
        };
        assert!(
            total_blocks > 1,
            "fixture must span multiple blocks; got {total_blocks}"
        );

        // Cancel from the first write_block call: every following block must
        // be skipped by the per-block cancel check in search_parallel.
        let calls = Arc::new(AtomicU64::new(0));
        let cancel = Arc::new(AtomicBool::new(false));
        let calls_cl = Arc::clone(&calls);
        let cancel_cl = Arc::clone(&cancel);
        let write: WriteDataBlockFn = Arc::new(move |_wid, _db: &mut DataBlock| {
            calls_cl.fetch_add(1, Ordering::SeqCst);
            cancel_cl.store(true, Ordering::SeqCst);
        });
        let err = s
            .run_query_with_cancel(&all_tenant_ids, &q, write, Some(&cancel))
            .expect_err("a mid-query cancel must surface the canceled error");
        assert!(is_query_canceled_error(&err), "got: {err}");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the serial search must abort right after the cancelling block \
             ({total_blocks} blocks total)"
        );

        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    // -- ValuesWithHits query-surface tests ----------------------------------
    //
    // Port of the `field_names-*` / `field_values-*` / `stream_field_*` /
    // `streams` / `stream_ids` subtests of Go `TestStorageRunQuery`
    // (storage_search_test.go). The storage layout matches the Go test:
    // 11 tenants x 3 streams x 5 blocks x 7 rows = 1155 rows.

    /// Fills `s` with the Go `TestStorageRunQuery` fixture layout:
    /// 11 tenants x 3 streams x 5 blocks x 7 rows = 1155 rows.
    fn fill_run_query_fixture(s: &Arc<Storage>) -> Vec<TenantID> {
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
        all_tenant_ids
    }

    #[test]
    fn test_storage_run_query_values_with_hits() {
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
        let all_tenant_ids = fill_run_query_fixture(&s);

        let parse = |q: &str| ParseQuery(q).expect("parse query");

        // field_names-all
        {
            let q = parse("*");
            let results = s
                .get_field_names(&all_tenant_ids, &q, "", None, &test_qs())
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
                .get_field_names(&all_tenant_ids, &q, "o", None, &test_qs())
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
                .get_field_names(&all_tenant_ids, &q, "", None, &test_qs())
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
                .get_field_values(&all_tenant_ids, &q, "_stream", "", 0, None, &test_qs())
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
                .get_field_values(&all_tenant_ids, &q, "_stream", "1:23", 0, None, &test_qs())
                .expect("get_field_values");
            let results_expected = vec![vh(r#"{instance="host-1:234",job="foobar"}"#, 385)];
            assert_eq!(results, results_expected, "field_values-with-filter");
        }

        // field_values-limit-reached
        {
            let q = parse("*");
            let results = s
                .get_field_values(&all_tenant_ids, &q, "_stream", "", 3, None, &test_qs())
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
                .get_field_values(&all_tenant_ids, &q, "_stream", "", 4, None, &test_qs())
                .expect("get_field_values");
            let results_expected = vec![vh(r#"{instance="host-1:234",job="foobar"}"#, 385)];
            assert_eq!(results, results_expected, "field_values-limit-not-reached");
        }

        // stream_field_names
        {
            let q = parse("*");
            let results = s
                .get_stream_field_names(&all_tenant_ids, &q, "", None, &test_qs())
                .expect("get_stream_field_names");
            let results_expected = vec![vh("instance", 1155), vh("job", 1155)];
            assert_eq!(results, results_expected, "stream_field_names");
        }

        // stream_field_names-with-filter
        {
            let q = parse("*");
            let results = s
                .get_stream_field_names(&all_tenant_ids, &q, "ob", None, &test_qs())
                .expect("get_stream_field_names");
            let results_expected = vec![vh("job", 1155)];
            assert_eq!(results, results_expected, "stream_field_names-with-filter");
        }

        // stream_field_values-nolimit
        {
            let q = parse("*");
            let results = s
                .get_stream_field_values(&all_tenant_ids, &q, "instance", "", 0, None, &test_qs())
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
                .get_stream_field_values(
                    &all_tenant_ids,
                    &q,
                    "instance",
                    "t-2",
                    0,
                    None,
                    &test_qs(),
                )
                .expect("get_stream_field_values");
            let results_expected = vec![vh("host-2:234", 385)];
            assert_eq!(results, results_expected, "stream_field_values-with-filter");
        }

        // stream_field_values-limit
        {
            let q = parse("*");
            let values = s
                .get_stream_field_values(&all_tenant_ids, &q, "instance", "", 3, None, &test_qs())
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
            let results = s
                .get_streams(&all_tenant_ids, &q, 0, None, &test_qs())
                .expect("get_streams");
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
                .get_stream_ids(&all_tenant_ids, &q, 0, None, &test_qs())
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

    // -- Subquery execution tests --------------------------------------------
    //
    // Port of the subquery subtests of Go `TestStorageRunQuery`
    // (storage_search_test.go): `in-filter-with-subquery-{match,mismatch}`,
    // `_stream_id-filter`, `in-filter-with-subquery-in-conditional-stats-mismatch`,
    // `query_stats-subquery`, `union-pipe`, `pipe-extract-if-filter-with-subquery*`
    // and `pipe-join{,-prefix,-inline-rows,-inline-rows-prefix}`.

    /// A collected result row as sorted `(name, value)` pairs.
    type TestRow = Vec<(String, String)>;

    #[test]
    fn test_storage_run_query_subqueries() {
        let path = run_query_temp_path("subqueries");
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);
        let all_tenant_ids = fill_run_query_fixture(&s);

        // Go `f` helper: run the query and compare the collected rows
        // (order-insensitive, like Go `assertRowsEqual`/`sortTestRows`).
        let f = |query: &str, rows_expected: &[&[(&str, &str)]]| {
            let q = ParseQuery(query).unwrap_or_else(|e| panic!("cannot parse [{query}]: {e}"));
            let rows: Arc<Mutex<Vec<TestRow>>> = Arc::new(Mutex::new(Vec::new()));
            let rows_w = Arc::clone(&rows);
            let write: WriteDataBlockFn = Arc::new(move |_wid, db: &mut DataBlock| {
                let rows_count = db.rows_count();
                let cs = db.get_columns(false);
                if cs.is_empty() {
                    return;
                }
                let mut dst = rows_w.lock().unwrap();
                for i in 0..rows_count {
                    let mut row: Vec<(String, String)> = cs
                        .iter()
                        .map(|c| {
                            (
                                c.name.clone(),
                                String::from_utf8_lossy(&c.values[i]).into_owned(),
                            )
                        })
                        .collect();
                    row.sort();
                    dst.push(row);
                }
            });
            s.run_query(&all_tenant_ids, &q, write)
                .unwrap_or_else(|e| panic!("cannot run [{query}]: {e}"));

            let mut rows = std::mem::take(&mut *rows.lock().unwrap());
            rows.sort();
            let mut rows_expected: Vec<Vec<(String, String)>> = rows_expected
                .iter()
                .map(|row| {
                    let mut row: Vec<(String, String)> = row
                        .iter()
                        .map(|(n, v)| (n.to_string(), v.to_string()))
                        .collect();
                    row.sort();
                    row
                })
                .collect();
            rows_expected.sort();
            assert_eq!(rows, rows_expected, "unexpected rows for [{query}]");
        };

        // in-filter-with-subquery-match
        f(
            "tenant.id:in(tenant.id:2 | fields tenant.id) | stats count() rows",
            &[&[("rows", "105")]],
        );

        // in-filter-with-subquery-mismatch
        f(
            "tenant.id:in(tenant.id:23243 | fields tenant.id) | stats count() rows",
            &[&[("rows", "0")]],
        );

        // _stream_id-filter (in(<subquery>))
        f(
            "_stream_id:in(tenant.id:2 | fields _stream_id) | stats count() rows",
            &[&[("rows", "105")]],
        );

        // query_stats-subquery, adapted: the Go case asserts the shared
        // QueryStats (which count the subquery's reads through the shared
        // qctx — the Rust QueryStats are per-run_query); the port asserts the
        // row counts instead. The subquery ends with a `uniq` pipe, so
        // get_field_values_generic must not append another one.
        f(
            r#"non-existing-field:in("message" | uniq tenant.id) | stats count() rows"#,
            &[&[("rows", "0")]],
        );
        f(
            r#"tenant.id:in("message" | uniq tenant.id) | stats count() rows"#,
            &[&[("rows", "1155")]],
        );

        // in-filter-with-subquery-in-conditional-stats-mismatch
        f(
            "* | stats \
                count() rows_total, \
                count() if (tenant.id:in(tenant.id:3 | fields tenant.id)) rows_nonzero, \
                count() if (tenant.id:in(tenant.id:23243 | fields tenant.id)) rows_zero",
            &[&[
                ("rows_total", "1155"),
                ("rows_nonzero", "105"),
                ("rows_zero", "0"),
            ]],
        );

        // union-pipe
        f(
            r#"{instance=~"host-1.+"} | union ({instance=~"host-2.+"}) | count() hits"#,
            &[&[("hits", "770")]],
        );

        // pipe-extract-if-filter-with-subquery
        f(
            r#"* | extract
                if (tenant.id:in(tenant.id:(3 or 4) | fields tenant.id))
                "host-<host>:" from instance
            | filter host:~"1|2"
            | uniq (tenant.id, host) with hits
            | sort by (tenant.id, host)"#,
            &[
                &[
                    ("tenant.id", "{accountID=3,projectID=31}"),
                    ("host", "1"),
                    ("hits", "35"),
                ],
                &[
                    ("tenant.id", "{accountID=3,projectID=31}"),
                    ("host", "2"),
                    ("hits", "35"),
                ],
                &[
                    ("tenant.id", "{accountID=4,projectID=41}"),
                    ("host", "1"),
                    ("hits", "35"),
                ],
                &[
                    ("tenant.id", "{accountID=4,projectID=41}"),
                    ("host", "2"),
                    ("hits", "35"),
                ],
            ],
        );

        // pipe-extract-if-filter-with-subquery-non-empty-host
        f(
            r#"* | extract
                if (tenant.id:in(tenant.id:3 | fields tenant.id))
                "host-<host>:" from instance
            | filter host:*
            | uniq (host) with hits
            | sort by (host)"#,
            &[
                &[("host", "0"), ("hits", "35")],
                &[("host", "1"), ("hits", "35")],
                &[("host", "2"), ("hits", "35")],
            ],
        );

        // pipe-extract-if-filter-with-subquery-empty-host
        f(
            r#"* | extract
                if (tenant.id:in(tenant.id:3 | fields tenant.id))
                "host-<host>:" from instance
            | filter host:""
            | uniq (host) with hits
            | sort by (host)"#,
            &[&[("host", ""), ("hits", "1050")]],
        );

        // pipe-join (left join)
        f(
            "'message 5' | stats by (instance) count() x \
            | join on (instance) ( \
                'block 0' instance:host-1 | stats by (instance) \
                    count() total, \
                    count_uniq(stream-id) streams, \
                    count_uniq(stream-id) x \
            )",
            &[
                &[("instance", "host-0:234"), ("x", "55")],
                &[("instance", "host-2:234"), ("x", "55")],
                &[
                    ("instance", "host-1:234"),
                    ("x", "55"),
                    ("total", "77"),
                    ("streams", "1"),
                ],
            ],
        );

        // pipe-join (inner join)
        f(
            "'message 5' | stats by (instance) count() x \
            | join on (instance) ( \
                'block 0' instance:host-1 | stats by (instance) \
                    count() total, \
                    count_uniq(stream-id) streams, \
                    count_uniq(stream-id) x \
            ) inner",
            &[&[
                ("instance", "host-1:234"),
                ("x", "55"),
                ("total", "77"),
                ("streams", "1"),
            ]],
        );

        // pipe-join-prefix
        f(
            "'message 5' | stats by (instance) count() x \
            | join on (instance) ( \
                'block 0' instance:host-1 | stats by (instance) \
                    count() total, \
                    count_uniq(stream-id) streams, \
                    count_uniq(stream-id) x \
            ) prefix \"abc.\"",
            &[
                &[("instance", "host-0:234"), ("x", "55")],
                &[("instance", "host-2:234"), ("x", "55")],
                &[
                    ("instance", "host-1:234"),
                    ("x", "55"),
                    ("abc.total", "77"),
                    ("abc.streams", "1"),
                    ("abc.x", "1"),
                ],
            ],
        );

        // pipe-join-inline-rows
        f(
            r#"'message 5' | stats by (instance) count() x
            | join on (instance) rows(
                {"instance":"host-0:234","foo":"bar"}
                {"instance":"host-2:234","abc":"def","x":"y","z":"qwe"}
            )"#,
            &[
                &[("instance", "host-0:234"), ("x", "55"), ("foo", "bar")],
                &[
                    ("instance", "host-2:234"),
                    ("x", "55"),
                    ("abc", "def"),
                    ("z", "qwe"),
                ],
                &[("instance", "host-1:234"), ("x", "55")],
            ],
        );

        // pipe-join-inline-rows-prefix
        f(
            r#"'message 5' | stats by (instance) count() x
            | join on (instance) rows(
                {"instance":"host-0:234","foo":"bar"}
                {"instance":"host-2:234","abc":"def","x":"y","z":"qwe"}
            ) prefix "abc.""#,
            &[
                &[("instance", "host-0:234"), ("x", "55"), ("abc.foo", "bar")],
                &[
                    ("instance", "host-2:234"),
                    ("x", "55"),
                    ("abc.abc", "def"),
                    ("abc.x", "y"),
                    ("abc.z", "qwe"),
                ],
                &[("instance", "host-1:234"), ("x", "55")],
            ],
        );

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

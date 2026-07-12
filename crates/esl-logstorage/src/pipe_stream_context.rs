//! Port of `pipe_stream_context.go` — the
//! `| stream_context [before N] [after N] [time_window D]` pipe.
//!
//! `stream_context` buffers the matching log rows per stream (keyed by
//! `_stream_id`) in [`PipeStreamContextProcessor::write_block`], then in
//! [`PipeStreamContextProcessor::flush`] it re-queries the storage for the logs
//! surrounding each matching row (the `before N` / `after N` context lines
//! within `time_window`), merges/deduplicates overlapping context windows, and
//! emits them ordered by `_time`.
//!
//! ## Deferred infrastructure (PORT NOTEs)
//!
//! * `parsePipeStreamContext` / `parsePipeStreamContextBeforeAfter` are
//!   lexer-dependent and deferred — build the pipe via [`new_pipe_stream_context`].
//! * `splitToRemoteAndLocal`, `initFilterInValues`, `visitSubqueries` are
//!   single-node/subquery plumbing and are omitted per the port conventions.
//! * The surrounding-log fetch re-executes a LogsQL query against storage. Go
//!   wires this via `withRunQuery(qctx, runQuery, fieldsFilter)`. The storage
//!   query engine (`ParseQuery`, `NewQueryContext`, tenant scoping,
//!   `toFieldsFilters`) is not ported here, so the re-execution seam is modelled
//!   as an injectable [`RunQueryFn`] (Go's `runQueryFunc`) attached via
//!   [`PipeStreamContext::with_run_query`]. The full before/after selection,
//!   sorting and de-duplication algorithm IS ported and runs on whatever blocks
//!   the seam yields; only "how a LogsQL string hits storage" is deferred.
//!   Without a `run_query`, `flush` returns a descriptive error instead of
//!   producing output.
//! * The `stateSizeBudget` memory guard (Go `memory.Allowed()` /
//!   `stateSizeBudgetChunk`) is dropped; the `maxStreams` / `maxRowsPerStream`
//!   count guards are kept.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::rows::{Field, get_field_value_by_name};
use crate::values_encoder::{
    NSECS_PER_HOUR, marshal_duration_string, marshal_timestamp_rfc3339_nano_string,
};

/// Default time window to search for surrounding logs in `stream_context`.
const PIPE_STREAM_CONTEXT_DEFAULT_TIME_WINDOW: i64 = NSECS_PER_HOUR;

// `stream_context` results are meant for humans; there is no point in fetching
// surrounding logs for a huge number of streams or rows-per-stream.
const PIPE_STREAM_CONTEXT_MAX_STREAMS: usize = 100;
const PIPE_STREAM_CONTEXT_MAX_ROWS_PER_STREAM: usize = 1000;

/// Re-executes a LogsQL query string, invoking `write_block` for every result
/// block. Port of Go's `runQueryFunc`.
///
/// PORT NOTE: Go's signature is
/// `func(qctx *QueryContext, writeBlock func(workerID uint, br *blockResult)) error`.
/// The `QueryContext` (parsed query, tenant scoping, partial-response and hidden
/// fields config assembled by `withRunQuery`) is deferred; here the caller
/// receives the already-formatted query string and yields blocks through the
/// callback.
pub(crate) type RunQueryFn =
    Arc<dyn Fn(&str, &mut dyn FnMut(&mut BlockResult)) -> Result<(), String> + Send + Sync>;

/// `pipeStreamContext` processes `| stream_context ...` queries.
pub struct PipeStreamContext {
    /// Number of lines to return before each matching line.
    pub(crate) lines_before: usize,
    /// Number of lines to return after each matching line.
    pub(crate) lines_after: usize,
    /// Time window, in nanoseconds, for searching surrounding logs.
    pub(crate) time_window: i64,

    /// Subquery re-execution seam (Go `runQuery`, set via `withRunQuery`).
    /// `None` until [`PipeStreamContext::with_run_query`] is called.
    pub(crate) run_query: Option<RunQueryFn>,
}

/// Constructs a `stream_context` pipe from already-parsed components.
///
/// PORT NOTE: Go's `parsePipeStreamContext` is lexer-dependent and deferred;
/// this constructor takes the parsed `before` / `after` line counts and the
/// time window directly. `lines_before`/`lines_after` are non-negative (Go
/// rejects negatives at parse time).
pub(crate) fn new_pipe_stream_context(
    lines_before: usize,
    lines_after: usize,
    time_window: i64,
) -> PipeStreamContext {
    PipeStreamContext {
        lines_before,
        lines_after,
        time_window,
        run_query: None,
    }
}

impl PipeStreamContext {
    /// Attaches the subquery re-execution seam. Port of Go's `withRunQuery`
    /// (qctx/fieldsFilter are folded into the closure; see module PORT NOTE).
    // Ported for Go parity; not yet wired into a caller (see PARITY.md).
    #[allow(dead_code)]
    pub(crate) fn with_run_query(mut self, run_query: RunQueryFn) -> Self {
        self.run_query = Some(run_query);
        self
    }
}

impl Pipe for PipeStreamContext {
    fn to_string(&self) -> String {
        let mut s = String::from("stream_context");
        if self.lines_before > 0 {
            s += &format!(" before {}", self.lines_before);
        }
        if self.lines_after > 0 {
            s += &format!(" after {}", self.lines_after);
        }
        if self.lines_before == 0 && self.lines_after == 0 {
            s += " after 0";
        }
        if self.time_window != PIPE_STREAM_CONTEXT_DEFAULT_TIME_WINDOW {
            let mut buf = Vec::new();
            marshal_duration_string(&mut buf, self.time_window);
            s += " time_window ";
            s += &String::from_utf8_lossy(&buf);
        }
        s
    }

    // canLiveTail, canReturnLastNResults, isFixedOutputFieldsOrder and
    // hasFilterInWithQuery are all false for stream_context, matching the Pipe
    // trait defaults, so they are left unoverridden.

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter("_time");
        pf.add_allow_filter("_stream_id");
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeStreamContextProcessorShard::default()))
            .collect();
        Arc::new(PipeStreamContextProcessor {
            lines_before: self.lines_before,
            lines_after: self.lines_after,
            time_window: self.time_window,
            run_query: self.run_query.clone(),
            pp_next,
            shards,
            stop,
        })
    }
}

// ---------------------------------------------------------------------------
// Buffered rows
// ---------------------------------------------------------------------------

/// A single buffered log row (Go `streamContextRow`).
///
/// PORT NOTE: Go's `sizeBytes()` fed the `stateSizeBudget` memory accounting,
/// which is dropped here, so it is not ported.
#[derive(Clone, Default)]
struct StreamContextRow {
    timestamp: i64,
    fields: Vec<Field>,
}

/// Port of Go `(*streamContextRow).less`: order by timestamp, then field
/// name/value pairs, then field count.
fn cmp_row(a: &StreamContextRow, b: &StreamContextRow) -> Ordering {
    a.timestamp.cmp(&b.timestamp).then_with(|| {
        for (af, bf) in a.fields.iter().zip(b.fields.iter()) {
            match af.name.cmp(&bf.name) {
                Ordering::Equal => {}
                o => return o,
            }
            match af.value.cmp(&bf.value) {
                Ordering::Equal => {}
                o => return o,
            }
        }
        a.fields.len().cmp(&b.fields.len())
    })
}

fn row_less(a: &StreamContextRow, b: &StreamContextRow) -> bool {
    cmp_row(a, b) == Ordering::Less
}

// ---------------------------------------------------------------------------
// Processor
// ---------------------------------------------------------------------------

struct PipeStreamContextProcessor {
    lines_before: usize,
    lines_after: usize,
    time_window: i64,
    run_query: Option<RunQueryFn>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeStreamContextProcessorShard>>,
    stop: Arc<AtomicBool>,
}

/// Per-shard buffer of matching rows keyed by `_stream_id` (Go
/// `pipeStreamContextProcessorShard.m`).
#[derive(Default)]
struct PipeStreamContextProcessorShard {
    m: HashMap<String, Vec<StreamContextRow>>,
}

impl PipeProcessor for PipeStreamContextProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let mut shard = self.shards[worker_id].lock().unwrap();

        if shard.m.len() > PIPE_STREAM_CONTEXT_MAX_STREAMS {
            // Too many streams for showing stream context; stop processing.
            self.stop.store(true, AtomicOrdering::Relaxed);
            return;
        }

        // Materialize everything out of `br` (see write_block borrow discipline)
        // before touching the locked shard.
        let cs = br.get_columns();
        let names: Vec<String> = cs.iter().map(|&c| br.column_name(c).to_string()).collect();
        let stream_id_col = br.get_column_by_name("_stream_id");
        let mut col_values: Vec<Vec<Vec<u8>>> = Vec::with_capacity(cs.len());
        for &c in &cs {
            col_values.push(br.column_get_values(c).to_vec());
        }
        let stream_id_values = br.column_get_values(stream_id_col).to_vec();
        let timestamps = br.get_timestamps().to_vec();

        for (i, &timestamp) in timestamps.iter().enumerate() {
            let mut fields = Vec::with_capacity(cs.len());
            for j in 0..cs.len() {
                fields.push(Field {
                    name: names[j].clone(),
                    value: String::from_utf8_lossy(&col_values[j][i]).into_owned(),
                });
            }
            let row = StreamContextRow { timestamp, fields };

            let stream_id = String::from_utf8_lossy(&stream_id_values[i]).into_owned();
            let rows = shard.m.entry(stream_id).or_default();
            rows.push(row);
            if rows.len() > PIPE_STREAM_CONTEXT_MAX_ROWS_PER_STREAM {
                // Too many rows for a single stream; stop processing.
                self.stop.store(true, AtomicOrdering::Relaxed);
                return;
            }
        }
    }

    fn flush(&self) -> Result<(), String> {
        // Merge per-stream state across shards.
        let mut m: HashMap<String, Vec<StreamContextRow>> = HashMap::new();
        for shard in &self.shards {
            let shard = shard.lock().unwrap();
            for (stream_id, rows_src) in shard.m.iter() {
                m.entry(stream_id.clone())
                    .or_default()
                    .extend(rows_src.iter().cloned());
            }
        }

        if m.is_empty() {
            return Ok(());
        }

        if m.len() > PIPE_STREAM_CONTEXT_MAX_STREAMS {
            return Err(format!(
                "logs from too many streams passed to 'stream_context': {}; the maximum supported \
                 number of streams, which can be passed to 'stream_context' is {}; narrow down the \
                 matching log streams with additional filters",
                m.len(),
                PIPE_STREAM_CONTEXT_MAX_STREAMS
            ));
        }

        let run_query = match &self.run_query {
            Some(f) => f.clone(),
            None => {
                // PORT NOTE: producing output requires re-querying storage for
                // surrounding logs; the query engine is deferred (see module
                // PORT NOTE). Attach a seam via `with_run_query`.
                return Err(
                    "PORT NOTE: 'stream_context' requires subquery re-execution \
                     (Go withRunQuery/runQueryFunc), which is deferred until the LogsQL query \
                     engine is ported"
                        .to_string(),
                );
            }
        };

        // Write output contexts in ascending order of their minimum timestamp.
        let mut wctx = WriteContext::new(self.pp_next.clone());
        let multi_stream = m.len() > 1;
        let stream_ids = get_stream_ids_sorted_by_min_row_timestamp(&m);
        for stream_id in &stream_ids {
            if self.stop.load(AtomicOrdering::Relaxed) {
                return Ok(());
            }

            let rows = &m[stream_id];
            if rows.len() > PIPE_STREAM_CONTEXT_MAX_ROWS_PER_STREAM {
                return Err(format!(
                    "too many logs from a single stream passed to 'stream_context': {}; the maximum \
                     supported number of logs, which can be passed to 'stream_context' is {}; narrow \
                     down the matching logs with additional filters",
                    rows.len(),
                    PIPE_STREAM_CONTEXT_MAX_ROWS_PER_STREAM
                ));
            }

            let stream_rowss = get_stream_rowss(self, stream_id, rows, &run_query)?;

            for stream_rows in &stream_rowss {
                for stream_row in stream_rows {
                    wctx.write_row(&stream_row.fields);
                }
                if (multi_stream || stream_rowss.len() > 1)
                    && let Some(last_row) = stream_rows.last()
                {
                    let fields = new_delimiter_row_fields(last_row, stream_id);
                    wctx.write_row(&fields);
                }
            }
        }

        wctx.flush();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Surrounding-log fetch (before/after selection, sort, de-duplication)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct TimeRange {
    start: i64,
    end: i64,
}

/// Port of Go `(*pipeStreamContextProcessor).getStreamRowss`.
fn get_stream_rowss(
    pcp: &PipeStreamContextProcessor,
    stream_id: &str,
    needed_rows: &[StreamContextRow],
    run_query: &RunQueryFn,
) -> Result<Vec<Vec<StreamContextRow>>, String> {
    let mut needed_timestamps: Vec<i64> = needed_rows.iter().map(|r| r.timestamp).collect();
    needed_timestamps.sort_unstable();

    let trs = get_time_ranges_for_stream_rowss(pcp, stream_id, &needed_timestamps, run_query)?;
    let mut rowss =
        get_stream_rowss_by_time_ranges(pcp, stream_id, &needed_timestamps, &trs, run_query)?;

    for rows in rowss.iter_mut() {
        rows.sort_by(cmp_row);
    }

    Ok(deduplicate_stream_rowss(rowss))
}

/// Port of Go `(*pipeStreamContextProcessor).getTimeRangesForStreamRowss`.
fn get_time_ranges_for_stream_rowss(
    pcp: &PipeStreamContextProcessor,
    stream_id: &str,
    needed_timestamps: &[i64],
    run_query: &RunQueryFn,
) -> Result<Vec<TimeRange>, String> {
    let tr = get_time_range_for_needed_timestamps(pcp, needed_timestamps);
    let time_filter = get_time_filter(tr.start, tr.end);
    let q_str = format!("_stream_id:{stream_id} {time_filter} | fields _time");

    let rowss = execute_query(pcp, &q_str, needed_timestamps, run_query)?;

    let mut trs = Vec::with_capacity(rowss.len());
    for rows in &rowss {
        if rows.is_empty() {
            // Surrounding rows for the given row were included into the previous
            // row.
            trs.push(TimeRange {
                start: i64::MIN,
                end: i64::MAX,
            });
            continue;
        }
        let mut min_timestamp = rows[0].timestamp;
        let mut max_timestamp = min_timestamp;
        for row in &rows[1..] {
            if row.timestamp < min_timestamp {
                min_timestamp = row.timestamp;
            } else if row.timestamp > max_timestamp {
                max_timestamp = row.timestamp;
            }
        }
        trs.push(TimeRange {
            start: min_timestamp,
            end: max_timestamp,
        });
    }
    Ok(trs)
}

/// Port of Go `(*pipeStreamContextProcessor).getTimeRangeForNeededTimestamps`.
fn get_time_range_for_needed_timestamps(
    pcp: &PipeStreamContextProcessor,
    needed_timestamps: &[i64],
) -> TimeRange {
    let mut tr = TimeRange {
        start: needed_timestamps[0],
        end: needed_timestamps[0],
    };
    for &ts in &needed_timestamps[1..] {
        if ts < tr.start {
            tr.start = ts;
        } else if ts > tr.end {
            tr.end = ts;
        }
    }
    if pcp.lines_before > 0 {
        tr.start -= pcp.time_window;
    }
    if pcp.lines_after > 0 {
        tr.end += pcp.time_window;
    }
    tr
}

/// Port of Go `(*pipeStreamContextProcessor).getStreamRowssByTimeRanges`.
fn get_stream_rowss_by_time_ranges(
    pcp: &PipeStreamContextProcessor,
    stream_id: &str,
    needed_timestamps: &[i64],
    trs: &[TimeRange],
    run_query: &RunQueryFn,
) -> Result<Vec<Vec<StreamContextRow>>, String> {
    let mut q_str = format!("_stream_id:{stream_id}");
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;
    let mut time_filters: Vec<String> = Vec::with_capacity(trs.len());
    for tr in trs {
        if tr.start == i64::MIN && tr.end == i64::MAX {
            continue;
        }
        if tr.start < min_timestamp {
            min_timestamp = tr.start;
        }
        if tr.end > max_timestamp {
            max_timestamp = tr.end;
        }
        time_filters.push(get_time_filter(tr.start, tr.end));
    }
    if min_timestamp <= max_timestamp {
        q_str.push(' ');
        q_str.push_str(&get_time_filter(min_timestamp, max_timestamp));
    }
    if time_filters.len() > 1 {
        q_str.push_str(" (");
        q_str.push_str(&time_filters.join(" OR "));
        q_str.push(')');
    }
    // PORT NOTE: Go appends `toFieldsFilters(pc.fieldsFilter)` here to restrict
    // the fetched fields to the query's needed set. `fieldsFilter` is wired by
    // the deferred `withRunQuery`, so no fields filter is appended.

    execute_query(pcp, &q_str, needed_timestamps, run_query)
}

/// Port of Go `getTimeFilter`.
fn get_time_filter(start: i64, end: i64) -> String {
    let mut start_buf = Vec::new();
    marshal_timestamp_rfc3339_nano_string(&mut start_buf, start);
    let mut end_buf = Vec::new();
    marshal_timestamp_rfc3339_nano_string(&mut end_buf, end);
    format!(
        "_time:[{}, {}]",
        String::from_utf8_lossy(&start_buf),
        String::from_utf8_lossy(&end_buf)
    )
}

/// Port of Go `(*pipeStreamContextProcessor).executeQuery`.
///
/// PORT NOTE: Go parses `q_str` into a `Query`, builds a tenant-scoped
/// `QueryContext` (via `getTenantIDFromStreamIDString`) and runs it through
/// `pc.runQuery`. Here the already-formatted `q_str` is handed to the injected
/// [`RunQueryFn`] seam; the per-block before/after accumulation below is a
/// faithful port of Go's `writeBlock` callback.
fn execute_query(
    pcp: &PipeStreamContextProcessor,
    q_str: &str,
    needed_timestamps: &[i64],
    run_query: &RunQueryFn,
) -> Result<Vec<Vec<StreamContextRow>>, String> {
    let mut context_rows: Vec<StreamContextRows> = needed_timestamps
        .iter()
        .map(|&t| StreamContextRows::new(t, pcp.lines_before, pcp.lines_after))
        .collect();
    // Snapshot the needed timestamps so the neighbor guards below can read them
    // without aliasing the mutably-borrowed `context_rows`.
    let needed: Vec<i64> = context_rows.iter().map(|c| c.needed_timestamp).collect();

    let mut callback = |br: &mut BlockResult| {
        for i in 0..context_rows.len() {
            if pcp.stop.load(AtomicOrdering::Relaxed) {
                return;
            }
            if !context_rows[i].can_update(br) {
                // Fast path - skip reading block timestamps for this ctx.
                continue;
            }
            let timestamps = br.get_timestamps().to_vec();
            for (j, &timestamp) in timestamps.iter().enumerate() {
                if i > 0 && timestamp <= needed[i - 1] {
                    continue;
                }
                if i + 1 < needed.len() && timestamp >= needed[i + 1] {
                    continue;
                }
                context_rows[i].update(br, j, timestamp);
            }
        }
    };
    run_query(q_str, &mut callback)?;

    let mut rowss = Vec::with_capacity(context_rows.len());
    for ctx in context_rows {
        let mut rows = ctx.rows_before;
        rows.extend(ctx.rows_matched);
        rows.extend(ctx.rows_after);
        rowss.push(rows);
    }
    Ok(rowss)
}

/// Port of Go `deduplicateStreamRowss`: drops leading empty context windows and
/// trims rows already emitted by the previous (overlapping) window.
fn deduplicate_stream_rowss(
    mut stream_rowss: Vec<Vec<StreamContextRow>>,
) -> Vec<Vec<StreamContextRow>> {
    let mut i = 0;
    while i < stream_rowss.len() && stream_rowss[i].is_empty() {
        i += 1;
    }
    if i >= stream_rowss.len() {
        return Vec::new();
    }
    let rest = stream_rowss.split_off(i);

    let mut result: Vec<Vec<StreamContextRow>> = Vec::new();
    let mut last_seen_row = rest[0].last().unwrap().clone();
    let mut iter = rest.into_iter();
    result.push(iter.next().unwrap());

    for stream_rows in iter {
        let mut j = 0;
        while j < stream_rows.len() && !row_less(&last_seen_row, &stream_rows[j]) {
            j += 1;
        }
        if j >= stream_rows.len() {
            continue;
        }
        let trimmed: Vec<StreamContextRow> = stream_rows[j..].to_vec();
        last_seen_row = trimmed.last().unwrap().clone();
        result.push(trimmed);
    }
    result
}

/// Port of Go `getStreamIDsSortedByMinRowTimestamp`.
fn get_stream_ids_sorted_by_min_row_timestamp(
    m: &HashMap<String, Vec<StreamContextRow>>,
) -> Vec<String> {
    let mut stream_timestamps: Vec<(String, i64)> = m
        .iter()
        .map(|(stream_id, rows)| {
            let mut min_timestamp = rows[0].timestamp;
            for r in &rows[1..] {
                if r.timestamp < min_timestamp {
                    min_timestamp = r.timestamp;
                }
            }
            (stream_id.clone(), min_timestamp)
        })
        .collect();
    stream_timestamps.sort_by_key(|t| t.1);
    stream_timestamps
        .into_iter()
        .map(|(stream_id, _)| stream_id)
        .collect()
}

/// Port of Go `newDelimiterRowFields`.
fn new_delimiter_row_fields(r: &StreamContextRow, stream_id: &str) -> Vec<Field> {
    let mut time_buf = Vec::new();
    marshal_timestamp_rfc3339_nano_string(&mut time_buf, r.timestamp + 1);
    vec![
        Field {
            name: "_time".to_string(),
            value: String::from_utf8_lossy(&time_buf).into_owned(),
        },
        Field {
            name: "_stream_id".to_string(),
            value: stream_id.to_string(),
        },
        Field {
            name: "_stream".to_string(),
            value: get_field_value_by_name(&r.fields, "_stream").to_string(),
        },
        Field {
            name: "_msg".to_string(),
            value: "---".to_string(),
        },
    ]
}

// ---------------------------------------------------------------------------
// Per-matched-row before/after accumulator (Go `streamContextRows`)
// ---------------------------------------------------------------------------

/// Accumulates the `before`/`after`/matched rows around one matching timestamp.
///
/// PORT NOTE: Go keeps `rowsBefore` as a min-heap and `rowsAfter` as a max-heap
/// (`container/heap`). Here they are plain `Vec`s with explicit extreme
/// tracking: the kept set (the N rows closest to `needed_timestamp`) and the
/// final output (re-sorted by `cmp_row` in `get_stream_rowss`) are identical;
/// only the internal storage order differs.
struct StreamContextRows {
    needed_timestamp: i64,
    lines_before: usize,
    lines_after: usize,
    rows_before: Vec<StreamContextRow>,
    rows_after: Vec<StreamContextRow>,
    rows_matched: Vec<StreamContextRow>,
}

impl StreamContextRows {
    fn new(needed_timestamp: i64, lines_before: usize, lines_after: usize) -> Self {
        Self {
            needed_timestamp,
            lines_before,
            lines_after,
            rows_before: Vec::new(),
            rows_after: Vec::new(),
            rows_matched: Vec::new(),
        }
    }

    /// (index, timestamp) of the smallest-timestamp buffered "before" row.
    fn rows_before_min(&self) -> (usize, i64) {
        let mut idx = 0;
        let mut ts = self.rows_before[0].timestamp;
        for (k, r) in self.rows_before.iter().enumerate().skip(1) {
            if r.timestamp < ts {
                ts = r.timestamp;
                idx = k;
            }
        }
        (idx, ts)
    }

    /// (index, timestamp) of the largest-timestamp buffered "after" row.
    fn rows_after_max(&self) -> (usize, i64) {
        let mut idx = 0;
        let mut ts = self.rows_after[0].timestamp;
        for (k, r) in self.rows_after.iter().enumerate().skip(1) {
            if r.timestamp > ts {
                ts = r.timestamp;
                idx = k;
            }
        }
        (idx, ts)
    }

    /// Port of Go `(*streamContextRows).canUpdate`.
    fn can_update(&self, br: &mut BlockResult) -> bool {
        if self.lines_before > 0 {
            if self.rows_before.len() < self.lines_before {
                return true;
            }
            let min_timestamp = self.rows_before_min().1;
            let max_timestamp = self.needed_timestamp;
            if br.intersects_time_range(min_timestamp, max_timestamp) {
                return true;
            }
        }

        if self.lines_after > 0 {
            if self.rows_after.len() < self.lines_after {
                return true;
            }
            let min_timestamp = self.needed_timestamp;
            let max_timestamp = self.rows_after_max().1;
            if br.intersects_time_range(min_timestamp, max_timestamp) {
                return true;
            }
        }

        if self.lines_before == 0 && self.lines_after == 0 {
            if self.rows_matched.is_empty() {
                return true;
            }
            let timestamp = self.rows_matched[0].timestamp;
            if br.intersects_time_range(timestamp, timestamp) {
                return true;
            }
        }

        false
    }

    /// Port of Go `(*streamContextRows).update`.
    fn update(&mut self, br: &mut BlockResult, row_idx: usize, row_timestamp: i64) {
        if row_timestamp < self.needed_timestamp {
            if self.lines_before == 0 {
                return;
            }
            if self.rows_before.len() < self.lines_before {
                let r = copy_row_at_idx(br, row_idx, row_timestamp);
                self.rows_before.push(r);
                return;
            }
            let (min_idx, min_ts) = self.rows_before_min();
            if row_timestamp <= min_ts {
                return;
            }
            self.rows_before[min_idx] = copy_row_at_idx(br, row_idx, row_timestamp);
            return;
        }

        if row_timestamp > self.needed_timestamp {
            if self.lines_after == 0 {
                return;
            }
            if self.rows_after.len() < self.lines_after {
                let r = copy_row_at_idx(br, row_idx, row_timestamp);
                self.rows_after.push(r);
                return;
            }
            let (max_idx, max_ts) = self.rows_after_max();
            if row_timestamp >= max_ts {
                return;
            }
            self.rows_after[max_idx] = copy_row_at_idx(br, row_idx, row_timestamp);
            return;
        }

        // row_timestamp == needed_timestamp
        let r = copy_row_at_idx(br, row_idx, row_timestamp);
        self.rows_matched.push(r);
    }
}

/// Port of Go `(*streamContextRows).copyRowAtIdx`.
fn copy_row_at_idx(br: &mut BlockResult, row_idx: usize, row_timestamp: i64) -> StreamContextRow {
    let cs = br.get_columns();
    if cs.is_empty() {
        return StreamContextRow {
            timestamp: row_timestamp,
            fields: Vec::new(),
        };
    }
    let names: Vec<String> = cs.iter().map(|&c| br.column_name(c).to_string()).collect();
    let mut fields = Vec::with_capacity(cs.len());
    for (k, &c) in cs.iter().enumerate() {
        let v = br.column_get_value_at_row(c, row_idx).to_string();
        fields.push(Field {
            name: names[k].clone(),
            value: v,
        });
    }
    StreamContextRow {
        timestamp: row_timestamp,
        fields,
    }
}

// ---------------------------------------------------------------------------
// Output block builder (Go `pipeStreamContextWriteContext`)
// ---------------------------------------------------------------------------

struct WriteContext {
    pp_next: Arc<dyn PipeProcessor>,
    rcs: Vec<ResultColumn>,
    br: BlockResult,
    rows_count: usize,
    values_len: usize,
}

impl WriteContext {
    fn new(pp_next: Arc<dyn PipeProcessor>) -> Self {
        Self {
            pp_next,
            rcs: Vec::new(),
            br: BlockResult::default(),
            rows_count: 0,
            values_len: 0,
        }
    }

    /// Port of Go `(*pipeStreamContextWriteContext).writeRow`.
    fn write_row(&mut self, row_fields: &[Field]) {
        let mut are_equal_columns = self.rcs.len() == row_fields.len();
        if are_equal_columns {
            for (i, f) in row_fields.iter().enumerate() {
                if self.rcs[i].name != f.name {
                    are_equal_columns = false;
                    break;
                }
            }
        }
        if !are_equal_columns {
            // Send the current block to ppNext and start a new column set.
            self.flush();
            self.rcs = row_fields
                .iter()
                .map(|f| ResultColumn {
                    name: f.name.clone(),
                    values: Vec::new(),
                })
                .collect();
        }

        for (i, f) in row_fields.iter().enumerate() {
            self.rcs[i].add_value(f.value.as_bytes());
            self.values_len += f.value.len();
        }

        self.rows_count += 1;
        if self.values_len >= 1_000_000 {
            self.flush();
        }
    }

    /// Port of Go `(*pipeStreamContextWriteContext).flush`.
    fn flush(&mut self) {
        self.values_len = 0;

        // Preserve the column skeleton (names) for reuse across the flush, since
        // `set_result_columns` consumes the `ResultColumn`s (Go reuses the same
        // slice via `resetValues`).
        let names: Vec<String> = self.rcs.iter().map(|rc| rc.name.clone()).collect();
        let rcs = std::mem::take(&mut self.rcs);

        self.br.set_result_columns(rcs, self.rows_count);
        self.rows_count = 0;
        self.pp_next.write_block(0, &mut self.br);
        self.br.reset();

        self.rcs = names
            .into_iter()
            .map(|name| ResultColumn {
                name,
                values: Vec::new(),
            })
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::{assert_needed_fields, assert_rows_eq, rows};

    // PORT NOTE: `TestParsePipeStreamContextSuccess` / `...Failure` exercise the
    // lexer-based `parsePipeStreamContext`, which is deferred; they are omitted
    // until the LogsQL parser is ported.

    fn stream_context(before: usize, after: usize) -> PipeStreamContext {
        new_pipe_stream_context(before, after, PIPE_STREAM_CONTEXT_DEFAULT_TIME_WINDOW)
    }

    // Build an RFC3339-nano `_time` string round-tripping VL's own marshaller,
    // so `BlockResult::get_timestamps` decodes it back to `nsec`.
    fn ts_str(nsec: i64) -> String {
        let mut buf = Vec::new();
        marshal_timestamp_rfc3339_nano_string(&mut buf, nsec);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_to_string() {
        assert_eq!(stream_context(5, 0).to_string(), "stream_context before 5");
        assert_eq!(stream_context(0, 10).to_string(), "stream_context after 10");
        assert_eq!(stream_context(0, 0).to_string(), "stream_context after 0");
        assert_eq!(
            stream_context(10, 20).to_string(),
            "stream_context before 10 after 20"
        );
    }

    // Port of `TestPipeStreamContextUpdateNeededFields`.
    #[test]
    fn test_pipe_stream_context_update_needed_fields() {
        // all the needed fields
        assert_needed_fields(&stream_context(10, 4), "*", "", "*", "");

        // plus unneeded fields
        assert_needed_fields(&stream_context(10, 4), "*", "f1,f2", "*", "f1,f2");
        assert_needed_fields(&stream_context(0, 4), "*", "_time,f1,_stream_id", "*", "f1");

        // needed fields
        assert_needed_fields(
            &stream_context(3, 0),
            "f1,f2",
            "",
            "_stream_id,_time,f1,f2",
            "",
        );
        assert_needed_fields(
            &stream_context(3, 0),
            "_time,f1,_stream_id",
            "",
            "_stream_id,_time,f1",
            "",
        );
    }

    #[test]
    fn test_cmp_row_orders_by_timestamp_then_fields() {
        let a = StreamContextRow {
            timestamp: 1,
            fields: vec![Field {
                name: "a".into(),
                value: "1".into(),
            }],
        };
        let b = StreamContextRow {
            timestamp: 2,
            fields: vec![Field {
                name: "a".into(),
                value: "0".into(),
            }],
        };
        assert!(row_less(&a, &b)); // timestamp wins
        assert!(!row_less(&b, &a));

        // equal timestamp -> compare fields
        let c = StreamContextRow {
            timestamp: 1,
            fields: vec![Field {
                name: "a".into(),
                value: "2".into(),
            }],
        };
        assert!(row_less(&a, &c)); // "1" < "2"

        // equal prefix, shorter is less
        let d = StreamContextRow {
            timestamp: 1,
            fields: vec![
                Field {
                    name: "a".into(),
                    value: "1".into(),
                },
                Field {
                    name: "b".into(),
                    value: "x".into(),
                },
            ],
        };
        assert!(row_less(&a, &d));
    }

    #[test]
    fn test_get_stream_ids_sorted_by_min_row_timestamp() {
        let mut m: HashMap<String, Vec<StreamContextRow>> = HashMap::new();
        m.insert(
            "later".into(),
            vec![
                StreamContextRow {
                    timestamp: 50,
                    fields: vec![],
                },
                StreamContextRow {
                    timestamp: 30,
                    fields: vec![],
                },
            ],
        );
        m.insert(
            "earlier".into(),
            vec![StreamContextRow {
                timestamp: 10,
                fields: vec![],
            }],
        );
        assert_eq!(
            get_stream_ids_sorted_by_min_row_timestamp(&m),
            vec!["earlier".to_string(), "later".to_string()]
        );
    }

    #[test]
    fn test_deduplicate_stream_rowss_trims_overlap() {
        let mk = |ts: i64| StreamContextRow {
            timestamp: ts,
            fields: vec![Field {
                name: "_msg".into(),
                value: ts.to_string(),
            }],
        };
        // Leading empty window is dropped; the second window's rows that are
        // <= the last seen row (ts 3) are trimmed.
        let input = vec![
            vec![],
            vec![mk(1), mk(2), mk(3)],
            vec![mk(2), mk(3), mk(4), mk(5)],
        ];
        let out = deduplicate_stream_rowss(input);
        assert_eq!(out.len(), 2);
        assert_eq!(
            out[0].iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            out[1].iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            vec![4, 5]
        );
    }

    #[test]
    fn test_new_delimiter_row_fields() {
        let r = StreamContextRow {
            timestamp: 100,
            fields: vec![Field {
                name: "_stream".into(),
                value: "{app=\"x\"}".into(),
            }],
        };
        let fields = new_delimiter_row_fields(&r, "stream-42");
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["_time", "_stream_id", "_stream", "_msg"]);
        assert_eq!(get_field_value_by_name(&fields, "_stream_id"), "stream-42");
        assert_eq!(get_field_value_by_name(&fields, "_stream"), "{app=\"x\"}");
        assert_eq!(get_field_value_by_name(&fields, "_msg"), "---");
        assert!(!get_field_value_by_name(&fields, "_time").is_empty());
    }

    #[test]
    fn test_write_block_buffers_rows_per_stream() {
        // Two streams interleaved in one block.
        let input = rows(&[
            &[("_time", &ts_str(10)), ("_stream_id", "s1"), ("_msg", "a")],
            &[("_time", &ts_str(20)), ("_stream_id", "s2"), ("_msg", "b")],
            &[("_time", &ts_str(30)), ("_stream_id", "s1"), ("_msg", "c")],
        ]);

        // Construct the concrete processor directly so the buffered per-stream
        // state can be inspected (it is otherwise hidden behind Arc<dyn ..>).
        let processor = PipeStreamContextProcessor {
            lines_before: 1,
            lines_after: 1,
            time_window: PIPE_STREAM_CONTEXT_DEFAULT_TIME_WINDOW,
            run_query: None,
            pp_next: Arc::new(crate::pipe_update::test_utils::CollectProcessor::default()),
            shards: vec![Mutex::new(PipeStreamContextProcessorShard::default())],
            stop: Arc::new(AtomicBool::new(false)),
        };
        let mut br = BlockResult::default();
        br.must_init_from_rows(&input);
        processor.write_block(0, &mut br);

        let shard = processor.shards[0].lock().unwrap();
        assert_eq!(shard.m.len(), 2);
        assert_eq!(shard.m["s1"].len(), 2);
        assert_eq!(shard.m["s2"].len(), 1);
        let s1_ts: Vec<i64> = shard.m["s1"].iter().map(|r| r.timestamp).collect();
        assert_eq!(s1_ts, vec![10, 30]);
        assert_eq!(
            get_field_value_by_name(&shard.m["s2"][0].fields, "_msg"),
            "b"
        );
    }

    #[test]
    fn test_flush_without_run_query_reports_deferred() {
        let stop = Arc::new(AtomicBool::new(false));
        let collector = Arc::new(crate::pipe_update::test_utils::CollectProcessor::default());
        let p = stream_context(1, 1);
        let pp = p.new_pipe_processor(1, stop, collector);

        let input = rows(&[&[("_time", &ts_str(10)), ("_stream_id", "s1"), ("_msg", "a")]]);
        let mut br = BlockResult::default();
        br.must_init_from_rows(&input);
        pp.write_block(0, &mut br);

        let err = pp.flush().unwrap_err();
        assert!(err.contains("PORT NOTE"), "unexpected error: {err}");
    }

    // End-to-end behavior test of the before/after selection, exercising the
    // injected `run_query` seam (Go `withRunQuery`).
    #[test]
    fn test_stream_context_before_after_via_run_query() {
        // Full stream s1: three logs at t=10,20,30. The matching row is t=20.
        // With before 1 / after 1 the surrounding logs t=10 and t=30 are added.
        let full_stream = rows(&[
            &[("_time", &ts_str(10)), ("_stream_id", "s1"), ("_msg", "a")],
            &[("_time", &ts_str(20)), ("_stream_id", "s1"), ("_msg", "b")],
            &[("_time", &ts_str(30)), ("_stream_id", "s1"), ("_msg", "c")],
        ]);
        let run_query: RunQueryFn = Arc::new(move |_q_str, cb| {
            let mut br = BlockResult::default();
            br.must_init_from_rows(&full_stream);
            cb(&mut br);
            Ok(())
        });

        let stop = Arc::new(AtomicBool::new(false));
        let collector = Arc::new(crate::pipe_update::test_utils::CollectProcessor::default());
        let p = stream_context(1, 1).with_run_query(run_query);
        let pp = p.new_pipe_processor(1, stop, collector.clone());

        // The matching input row (t=20) fed to the pipe.
        let matching = rows(&[&[("_time", &ts_str(20)), ("_stream_id", "s1"), ("_msg", "b")]]);
        let mut br = BlockResult::default();
        br.must_init_from_rows(&matching);
        pp.write_block(0, &mut br);
        pp.flush().unwrap();

        // Single stream + single context window => no delimiter row.
        assert_rows_eq(
            &collector.rows(),
            &rows(&[
                &[("_time", &ts_str(10)), ("_stream_id", "s1"), ("_msg", "a")],
                &[("_time", &ts_str(20)), ("_stream_id", "s1"), ("_msg", "b")],
                &[("_time", &ts_str(30)), ("_stream_id", "s1"), ("_msg", "c")],
            ]),
        );
    }
}

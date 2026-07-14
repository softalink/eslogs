//! Port of `lib/logstorage/pipe_sort_topk.go` — the top-N executor for the
//! `| sort ... limit N` pipe.
//!
//! It keeps at most `offset + limit` rows per partition per worker in a bounded
//! max-heap (worst row at the root), then in `flush` merges the per-partition
//! rows across workers and emits the top rows in sort order.
//!
//! It is reached only through [`crate::pipe_sort::PipeSort::new_pipe_processor`]
//! (Go's `newPipeProcessor` dispatch when `limit > 0`); there is no separate
//! pipe struct.
//!
//! PORT NOTE — parser: no parser here; the pipe is built by `crate::pipe_sort`.
//!
//! PORT NOTE — merge: Go pre-sorts each shard partition with a heap
//! (`sortRows`) and merges shards with an incremental k-way heap
//! (`pipeTopkRowsHeap`). This port concatenates the per-partition rows from all
//! shards and sorts them once with the same `topk_less` comparator, then emits
//! the `offset..offset+limit` window; the result is identical.
//!
//! PORT NOTE — `try_parse_number`: Go's `tryParseNumber` (defined in
//! `block_result.go`) is duplicated privately here, matching the existing copies
//! in `stats_uniq_values`/`filter_range`. When `pipe_math` exposes a canonical
//! `pub(crate)` version, these copies should switch to it.
//!
//! PORT NOTE — `topk_less` time branch: Go marshals `a.timestamp` in *both* the
//! `isTimeA` and `isTimeB` single-time branches (`pipe_sort_topk.go` lines
//! 679/682). That exact behavior is preserved.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering as AtomicOrdering};

use esl_common::encoding;
use esl_common::memory;
use esl_common::stringsutil::less_natural;

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_sort::{PipeSort, emit_block, marshal_json_key_value};
use crate::rows::Field;
use crate::values_encoder::{
    marshal_timestamp_rfc3339_nano_string, marshal_uint64_string, try_parse_bytes,
    try_parse_duration, try_parse_float64, try_parse_int64, try_parse_timestamp_rfc3339_nano,
    try_parse_uint64,
};

const STATE_SIZE_BUDGET_CHUNK: i64 = 1 << 20;
const MAX_VALUES_LEN: usize = 1_000_000;

// ---------------------------------------------------------------------------
// row
// ---------------------------------------------------------------------------

/// A row tracked by the top-N processor (Go `pipeTopkRow`).
#[derive(Clone, Default)]
struct PipeTopkRow {
    by_columns: Vec<Vec<u8>>,
    by_columns_is_time: Vec<bool>,
    other_columns: Vec<Field>,
    timestamp: i64,
}

impl PipeTopkRow {
    fn size_bytes(&self) -> i64 {
        let mut n = std::mem::size_of::<PipeTopkRow>();
        for v in &self.by_columns {
            n += v.len();
        }
        n += self.by_columns.len() * std::mem::size_of::<Vec<u8>>();
        n += self.by_columns_is_time.len();
        for f in &self.other_columns {
            n += f.name.len() + f.value.len();
        }
        n += self.other_columns.len() * std::mem::size_of::<Field>();
        n as i64
    }
}

// ---------------------------------------------------------------------------
// processor
// ---------------------------------------------------------------------------

/// Builds the top-N processor for a `sort ... limit N` pipe (Go
/// `newPipeTopkProcessor`).
pub(crate) fn new_pipe_topk_processor(
    ps: &PipeSort,
    concurrency: usize,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
) -> Arc<dyn PipeProcessor> {
    let max_state_size = (memory::allowed() as f64 * 0.2) as i64;
    let shards = (0..concurrency.max(1))
        .map(|_| Mutex::new(TopkShard::default()))
        .collect();
    Arc::new(PipeTopkProcessor {
        ps: ps.clone(),
        stop,
        pp_next,
        shards,
        max_state_size,
        state_size_budget: AtomicI64::new(max_state_size),
        global_full_root_ts: AtomicI64::new(i64::MIN),
    })
}

#[derive(Default)]
struct TopkShard {
    /// Per-partition bounded max-heap (worst row at index 0).
    rows_by_partition: HashMap<Vec<u8>, Vec<PipeTopkRow>>,
    state_size_budget: i64,
}

struct PipeTopkProcessor {
    ps: PipeSort,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<TopkShard>>,
    max_state_size: i64,
    state_size_budget: AtomicI64,
    /// For desc-time top-N (see `Pipe::is_desc_time_topk`): the newest known
    /// FULL-heap root timestamp across all shards. Once any shard holds
    /// `offset+limit` rows all >= R, a row older than R can never reach the
    /// global top-N, so whole blocks with `max_timestamp <= R` are skipped in
    /// `block_skip_check` without locking.
    global_full_root_ts: AtomicI64,
}

impl PipeTopkProcessor {
    fn write_block_to_shard(&self, shard: &mut TopkShard, br: &mut BlockResult) {
        let ps = &self.ps;
        let rows_len = br.rows_len();
        let cols = br.get_columns();

        // Materialize partition column values per row.
        let part_refs: Vec<_> = ps
            .partition_by_fields
            .iter()
            .map(|f| br.get_column_by_name(f))
            .collect();
        let part_vals: Vec<Vec<Vec<u8>>> = part_refs
            .iter()
            .map(|&r| br.column_get_values(r).to_vec())
            .collect();

        if ps.by_fields.is_empty() {
            // Sort by all fields.
            let names: Vec<Vec<u8>> = cols.iter().map(|&r| br.column_name(r).to_vec()).collect();
            let col_values: Vec<Vec<Vec<u8>>> = cols
                .iter()
                .map(|&r| br.column_get_values(r).to_vec())
                .collect();

            for row in 0..rows_len {
                let mut bb: Vec<u8> = Vec::new();
                for (i, cv) in col_values.iter().enumerate() {
                    marshal_json_key_value(&mut bb, &names[i], &cv[row]);
                    bb.push(b',');
                }
                let by_columns = vec![bb];
                let by_columns_is_time = vec![false];
                let key = build_partition_key(&part_vals, row);
                add_row(
                    shard,
                    ps,
                    key,
                    by_columns,
                    by_columns_is_time,
                    0,
                    &mut |dst| {
                        for (i, name) in names.iter().enumerate() {
                            dst.push(Field {
                                name: name.clone(),
                                value: col_values[i][row].clone(),
                            });
                        }
                    },
                );
            }
        } else {
            let by_fields = &ps.by_fields;
            let mut by_is_time = Vec::with_capacity(by_fields.len());
            let mut by_values: Vec<Vec<Vec<u8>>> = Vec::with_capacity(by_fields.len());
            for bf in by_fields {
                let r = br.get_column_by_name(&bf.name);
                let is_time = br.column_is_time(r);
                by_is_time.push(is_time);
                if is_time {
                    by_values.push(Vec::new());
                } else {
                    by_values.push(br.column_get_values(r).to_vec());
                }
            }

            // Other columns (those not in by_fields). Their values are NOT
            // copied here: most rows lose the top-N threshold test in
            // `add_row` and never need them, so they are materialized lazily
            // per accepted row (Go gets this for free since its getValues
            // returns strings referencing the block arena).
            let mut other_cols: Vec<(Vec<u8>, crate::block_result::ColRef)> = Vec::new();
            for &r in &cols {
                let name = br.column_name(r).to_vec();
                if by_fields.iter().any(|bf| bf.name == name) {
                    continue;
                }
                other_cols.push((name, r));
            }

            let any_time = by_is_time.iter().any(|&t| t);
            let timestamps = if any_time {
                br.get_timestamps().to_vec()
            } else {
                Vec::new()
            };

            let max_rows = ps.offset + ps.limit;

            // Sorted-by-_time block fast path (not in Go): when sorting
            // solely by `_time` without partitions, and the block timestamps
            // are monotone (they are — blocks store rows sorted by time),
            // iterate the block starting from its best end. The first row
            // that loses the full-heap threshold test proves every remaining
            // row loses too, so the rest of the block is skipped wholesale.
            // With N blocks this caps heap work at ~max_rows per shard
            // instead of one insert per row for time-ordered ingest.
            if by_fields.len() == 1
                && by_is_time[0]
                && ps.partition_by_fields.is_empty()
                && rows_len > 0
            {
                let mut is_desc = ps.is_desc;
                if by_fields[0].is_desc {
                    is_desc = !is_desc;
                }
                let ascending = timestamps.windows(2).all(|w| w[0] <= w[1]);
                let monotone = ascending || timestamps.windows(2).all(|w| w[0] >= w[1]);
                if monotone {
                    // Iterate best-first: largest timestamps first for desc
                    // order, smallest first for asc order.
                    let rev = is_desc == ascending;
                    for i in 0..rows_len {
                        let row = if rev { rows_len - 1 - i } else { i };
                        let timestamp = timestamps[row];
                        if let Some(rs) = shard.rows_by_partition.get([].as_slice())
                            && rs.len() as u64 >= max_rows
                            && !candidate_less(ps, &by_is_time, &[&b""[..]], timestamp, &rs[0])
                        {
                            // Monotone order: every remaining row loses too.
                            break;
                        }
                        add_row(
                            shard,
                            ps,
                            Vec::new(),
                            vec![Vec::new()],
                            by_is_time.clone(),
                            timestamp,
                            &mut |dst| {
                                for (name, r) in &other_cols {
                                    let vals = br.column_get_values(*r);
                                    dst.push(Field {
                                        name: name.clone(),
                                        value: vals[row].clone(),
                                    });
                                }
                            },
                        );
                    }
                    return;
                }
            }

            // Reject fast path: most rows lose the top-N threshold test once
            // the heap is full, so test with borrowed values before building
            // the owned `by_columns`/key/`PipeTopkRow` for `add_row` (Go's
            // reject path is allocation-free since its rows hold strings
            // referencing the block arena).
            let mut key_buf: Vec<u8> = Vec::new();
            let mut by_strs: Vec<&[u8]> = Vec::with_capacity(by_fields.len());
            for row in 0..rows_len {
                let timestamp = if any_time { timestamps[row] } else { 0 };
                key_buf.clear();
                for pv in &part_vals {
                    encoding::marshal_bytes(&mut key_buf, &pv[row]);
                }
                by_strs.clear();
                for (i, is_time) in by_is_time.iter().enumerate() {
                    by_strs.push(if *is_time {
                        &b""[..]
                    } else {
                        &by_values[i][row]
                    });
                }
                if let Some(rs) = shard.rows_by_partition.get(key_buf.as_slice())
                    && rs.len() as u64 >= max_rows
                    && !candidate_less(ps, &by_is_time, &by_strs, timestamp, &rs[0])
                {
                    continue;
                }

                let mut by_columns = Vec::with_capacity(by_fields.len());
                for (i, is_time) in by_is_time.iter().enumerate() {
                    if *is_time {
                        by_columns.push(Vec::new());
                    } else {
                        by_columns.push(by_values[i][row].clone());
                    }
                }
                let key = build_partition_key(&part_vals, row);
                add_row(
                    shard,
                    ps,
                    key,
                    by_columns,
                    by_is_time.clone(),
                    timestamp,
                    &mut |dst| {
                        for (name, r) in &other_cols {
                            let vals = br.column_get_values(*r);
                            dst.push(Field {
                                name: name.clone(),
                                value: vals[row].clone(),
                            });
                        }
                    },
                );
            }
        }
    }
}

fn build_partition_key(part_vals: &[Vec<Vec<u8>>], row: usize) -> Vec<u8> {
    let mut b = Vec::new();
    for pv in part_vals {
        encoding::marshal_bytes(&mut b, &pv[row]);
    }
    b
}

#[allow(clippy::too_many_arguments)]
fn add_row(
    shard: &mut TopkShard,
    ps: &PipeSort,
    partition_key: Vec<u8>,
    by_columns: Vec<Vec<u8>>,
    by_columns_is_time: Vec<bool>,
    timestamp: i64,
    fill_other_columns: &mut dyn FnMut(&mut Vec<Field>),
) {
    let max_rows = ps.offset + ps.limit;

    let is_new_partition = !shard.rows_by_partition.contains_key(&partition_key);
    let budget = &mut shard.state_size_budget;
    let rs = shard.rows_by_partition.entry(partition_key).or_default();
    if is_new_partition {
        *budget -= std::mem::size_of::<Vec<PipeTopkRow>>() as i64;
    }

    // Temporary row without other columns to test whether it must be stored.
    let tmp = PipeTopkRow {
        by_columns,
        by_columns_is_time,
        other_columns: Vec::new(),
        timestamp,
    };

    if rs.len() as u64 >= max_rows && !topk_less(ps, &tmp, &rs[0]) {
        // Fast path - nothing to add.
        return;
    }

    // Slow path - populate other columns and store.
    let mut row_new = tmp;
    fill_other_columns(&mut row_new.other_columns);

    *budget -= row_new.size_bytes();
    if (rs.len() as u64) < max_rows {
        heap_push(rs, ps, row_new);
    } else {
        *budget += rs[0].size_bytes();
        rs[0] = row_new;
        heap_fix0(rs, ps);
    }
}

// ---------------------------------------------------------------------------
// bounded max-heap (Go container/heap on pipeTopkRows.Less)
// ---------------------------------------------------------------------------

/// Go's `pipeTopkRows.Less(i, j)` — this is a max-heap keyed on sort order, so
/// the root is the "largest" (worst) row.
fn heap_less(rows: &[PipeTopkRow], ps: &PipeSort, i: usize, j: usize) -> bool {
    topk_less(ps, &rows[j], &rows[i])
}

fn heap_push(rows: &mut Vec<PipeTopkRow>, ps: &PipeSort, r: PipeTopkRow) {
    rows.push(r);
    let mut j = rows.len() - 1;
    while j > 0 {
        let parent = (j - 1) / 2;
        if !heap_less(rows, ps, j, parent) {
            break;
        }
        rows.swap(j, parent);
        j = parent;
    }
}

fn heap_fix0(rows: &mut [PipeTopkRow], ps: &PipeSort) {
    let n = rows.len();
    let mut i = 0;
    loop {
        let l = 2 * i + 1;
        if l >= n {
            break;
        }
        let mut j = l;
        let r = l + 1;
        if r < n && heap_less(rows, ps, r, l) {
            j = r;
        }
        if !heap_less(rows, ps, j, i) {
            break;
        }
        rows.swap(i, j);
        i = j;
    }
}

// ---------------------------------------------------------------------------
// PipeProcessor impl
// ---------------------------------------------------------------------------

impl PipeProcessor for PipeTopkProcessor {
    fn block_skip_check(&self, worker_id: usize, _min_timestamp: i64, max_timestamp: i64) -> bool {
        let ps = &self.ps;
        if !crate::pipe::Pipe::is_desc_time_topk(ps) {
            return false;
        }
        // Lock-free global check first: some shard already holds a full heap
        // whose worst row is newer than everything in this block.
        if max_timestamp <= self.global_full_root_ts.load(AtomicOrdering::SeqCst) {
            return true;
        }
        let max_rows = ps.offset + ps.limit;
        let shard = match self.shards.get(worker_id) {
            Some(s) => s.lock().unwrap(),
            None => return false,
        };
        match shard.rows_by_partition.get([].as_slice()) {
            // The block's best candidate carries max_timestamp; with a full
            // heap it must beat the root (strictly newer) to be inserted.
            Some(rs) if rs.len() as u64 >= max_rows => max_timestamp <= rs[0].timestamp,
            _ => false,
        }
    }

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
                    self.stop.store(true, AtomicOrdering::SeqCst);
                }
                return;
            }
            shard.state_size_budget += STATE_SIZE_BUDGET_CHUNK;
        }
        self.write_block_to_shard(&mut shard, br);
        if Pipe::is_desc_time_topk(&self.ps) {
            let max_rows = self.ps.offset + self.ps.limit;
            if let Some(rs) = shard.rows_by_partition.get([].as_slice())
                && rs.len() as u64 >= max_rows
            {
                self.global_full_root_ts
                    .fetch_max(rs[0].timestamp, AtomicOrdering::SeqCst);
            }
        }
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

        // Merge per-partition rows across all shards.
        let mut merged: HashMap<Vec<u8>, Vec<PipeTopkRow>> = HashMap::new();
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap();
            for (k, rows) in std::mem::take(&mut shard.rows_by_partition) {
                merged.entry(k).or_default().extend(rows);
            }
        }

        let mut keys: Vec<Vec<u8>> = merged.keys().cloned().collect();
        keys.sort();

        let ps = &self.ps;
        for k in keys {
            if self.stop.load(AtomicOrdering::SeqCst) {
                return Ok(());
            }
            let mut rows = merged.remove(&k).unwrap();
            rows.sort_by(|a, b| {
                if topk_less(ps, a, b) {
                    Ordering::Less
                } else if topk_less(ps, b, a) {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            });
            self.emit_partition(&rows);
        }
        Ok(())
    }
}

impl PipeTopkProcessor {
    fn emit_partition(&self, rows: &[PipeTopkRow]) {
        let ps = &self.ps;
        let has_rank = !ps.rank_field_name.is_empty();

        let mut rcs: Vec<ResultColumn> = Vec::new();
        let mut cur_names: Vec<Vec<u8>> = Vec::new();
        let mut rows_count: usize = 0;
        let mut values_len: usize = 0;
        let mut rows_written: u64 = 0;

        for r in rows {
            rows_written += 1;
            if rows_written <= ps.offset {
                continue;
            }
            if rows_written > ps.offset + ps.limit {
                break;
            }

            let mut names: Vec<Vec<u8>> = Vec::new();
            if has_rank {
                names.push(ps.rank_field_name.clone());
            }
            for bf in &ps.by_fields {
                names.push(bf.name.clone());
            }
            for c in &r.other_columns {
                names.push(c.name.clone());
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
                let v: Vec<u8> = if r.by_columns_is_time[i] {
                    let mut buf = Vec::new();
                    marshal_timestamp_rfc3339_nano_string(&mut buf, r.timestamp);
                    buf
                } else {
                    r.by_columns[i].clone()
                };
                values_len += v.len();
                rcs[ci].add_value(&v);
                ci += 1;
            }
            for c in &r.other_columns {
                let vb = c.value.as_slice();
                values_len += vb.len();
                rcs[ci].add_value(vb);
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

// ---------------------------------------------------------------------------
// comparators (Go topkLess / lessString)
// ---------------------------------------------------------------------------

/// Go `topkLess`.
/// Zero-alloc variant of [`topk_less`] for the reject fast path: the
/// candidate row `a` is described by its raw sort-key values (`by_strs`,
/// empty for `is_time` slots, plus `timestamp`) instead of an owned
/// [`PipeTopkRow`]. Must stay in lockstep with [`topk_less`].
fn candidate_less(
    ps: &PipeSort,
    by_is_time: &[bool],
    by_strs: &[&[u8]],
    timestamp: i64,
    b: &PipeTopkRow,
) -> bool {
    let by_fields = &ps.by_fields;
    for i in 0..by_strs.len() {
        let mut is_desc = ps.is_desc;
        if !by_fields.is_empty() && by_fields[i].is_desc {
            is_desc = !is_desc;
        }

        let is_time_a = by_is_time[i];
        let is_time_b = b.by_columns_is_time[i];

        if is_time_a && is_time_b {
            if timestamp == b.timestamp {
                continue;
            }
            return if is_desc {
                b.timestamp < timestamp
            } else {
                timestamp < b.timestamp
            };
        }

        // PORT NOTE: mirrors Go (and topk_less), which marshals a.timestamp
        // in both branches.
        let va: Cow<[u8]> = if is_time_a {
            Cow::Owned(ts_bytes(timestamp))
        } else {
            Cow::Borrowed(by_strs[i])
        };
        let vb: Cow<[u8]> = if is_time_b {
            Cow::Owned(ts_bytes(timestamp))
        } else {
            Cow::Borrowed(&b.by_columns[i])
        };

        if va == vb {
            continue;
        }
        return if is_desc {
            less_value_bytes(&vb, &va)
        } else {
            less_value_bytes(&va, &vb)
        };
    }
    false
}

fn topk_less(ps: &PipeSort, a: &PipeTopkRow, b: &PipeTopkRow) -> bool {
    let by_fields = &ps.by_fields;
    for i in 0..a.by_columns.len() {
        let mut is_desc = ps.is_desc;
        if !by_fields.is_empty() && by_fields[i].is_desc {
            is_desc = !is_desc;
        }

        let is_time_a = a.by_columns_is_time[i];
        let is_time_b = b.by_columns_is_time[i];

        if is_time_a && is_time_b {
            if a.timestamp == b.timestamp {
                continue;
            }
            return if is_desc {
                b.timestamp < a.timestamp
            } else {
                a.timestamp < b.timestamp
            };
        }

        // PORT NOTE: mirrors Go, which marshals a.timestamp in both branches.
        let va: Cow<[u8]> = if is_time_a {
            Cow::Owned(ts_bytes(a.timestamp))
        } else {
            Cow::Borrowed(&a.by_columns[i])
        };
        let vb: Cow<[u8]> = if is_time_b {
            Cow::Owned(ts_bytes(a.timestamp))
        } else {
            Cow::Borrowed(&b.by_columns[i])
        };

        if va == vb {
            continue;
        }
        return if is_desc {
            less_value_bytes(&vb, &va)
        } else {
            less_value_bytes(&va, &vb)
        };
    }
    false
}

fn ts_bytes(nsecs: i64) -> Vec<u8> {
    let mut b = Vec::new();
    marshal_timestamp_rfc3339_nano_string(&mut b, nsecs);
    b
}

/// Byte-value wrapper around [`less_string`]. Valid-UTF-8 values order exactly
/// like [`less_string`]; invalid UTF-8 fails every numeric parse (as in Go) and
/// falls back to plain byte ordering.
/// PORT NOTE: Go's lessNatural works on raw bytes; the byte fallback for
/// invalid UTF-8 is the closest byte-faithful behavior.
fn less_value_bytes(a: &[u8], b: &[u8]) -> bool {
    match (std::str::from_utf8(a), std::str::from_utf8(b)) {
        (Ok(a), Ok(b)) => less_string(a, b),
        _ => a < b,
    }
}

/// Go `lessString`.
pub(crate) fn less_string(a: &str, b: &str) -> bool {
    if a == b {
        return false;
    }
    if let (Some(ia), Some(ib)) = (try_parse_int64(a), try_parse_int64(b)) {
        return ia < ib;
    }
    if let (Some(ua), Some(ub)) = (try_parse_uint64(a), try_parse_uint64(b)) {
        return ua < ub;
    }
    if let (Some(ta), Some(tb)) = (
        try_parse_timestamp_rfc3339_nano(a),
        try_parse_timestamp_rfc3339_nano(b),
    ) {
        return ta < tb;
    }
    if let (Some(fa), Some(fb)) = (try_parse_number(a), try_parse_number(b)) {
        return fa < fb;
    }
    less_natural(a, b)
}

// PORT NOTE: private copy of Go's tryParseNumber (block_result.go); see module
// docs.
fn try_parse_number(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    if let Some(f) = try_parse_float64(s) {
        return Some(f);
    }
    if let Some(nsecs) = try_parse_duration(s) {
        return Some(nsecs as f64);
    }
    if let Some(bytes) = try_parse_bytes(s) {
        return Some(bytes as f64);
    }
    if is_likely_number(s) {
        if let Ok(f) = s.parse::<f64>() {
            return Some(f);
        }
        if let Some(n) = parse_int_go(s) {
            return Some(n as f64);
        }
    }
    None
}

fn parse_int_go(s: &str) -> Option<i64> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (radix, digits) =
        if let Some(h) = body.strip_prefix("0x").or_else(|| body.strip_prefix("0X")) {
            (16, h)
        } else if let Some(o) = body.strip_prefix("0o").or_else(|| body.strip_prefix("0O")) {
            (8, o)
        } else if let Some(b) = body.strip_prefix("0b").or_else(|| body.strip_prefix("0B")) {
            (2, b)
        } else {
            (10, body)
        };
    let digits = digits.replace('_', "");
    let n = i64::from_str_radix(&digits, radix).ok()?;
    Some(if neg { -n } else { n })
}

fn is_likely_number(s: &str) -> bool {
    let b = s.as_bytes();
    if b.is_empty() {
        return false;
    }
    let c = b[0];
    if !c.is_ascii_digit() && c != b'-' && c != b'+' && c != b'.' {
        return false;
    }
    if s.matches('.').count() > 1 {
        return false;
    }
    if s.contains(':') || s.matches('-').count() > 2 {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    // Port of Go's TestLessString.
    #[test]
    fn test_less_string() {
        let f = |a: &str, b: &str, want: bool| {
            assert_eq!(less_string(a, b), want, "less_string({a:?}, {b:?})");
        };

        f("", "", false);
        f("a", "", false);
        f("", "a", true);
        f("foo", "bar", false);
        f("bar", "foo", true);
        f("foo", "foo", false);
        f("foo1", "foo", false);
        f("foo", "foo1", true);

        // integers
        f("123", "9", false);
        f("9", "123", true);
        f("-123", "9", true);
        f("9", "-123", false);

        // floating point numbers
        f("1e3", "5", false);
        f("5", "1e3", true);

        // timestamps
        f("2025-01-15T10:20:30.1", "2025-01-15T10:20:30.09", false);
        f("2025-01-15T10:20:30.09", "2025-01-15T10:20:30.1", true);

        // versions
        f("v1.23.4", "v1.23.10", true);
        f("v1.23.10", "v1.23.4", false);

        // durations
        f("1h", "5s", false);
        f("5s", "1h", true);

        // bytes
        f("1MB", "5KB", false);
        f("5KB", "1MB", true);

        f("1.5M", "5.1K", false);
        f("5.1K", "1.5M", true);
        f("1.5M", "1.5M", false);
    }
}

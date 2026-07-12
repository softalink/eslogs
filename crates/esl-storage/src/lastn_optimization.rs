//! Port of EsLogs `app/eslstorage/lastnoptimization.go`: optimized
//! execution of `... | sort by (_time) desc | limit N` queries via adaptive
//! (binary-search) narrowing of the query time range.
//!
//! PORT NOTE: Go passes a `*logstorage.QueryContext` (query + tenantIDs +
//! context + query stats) through every helper; the ported `Storage::run_query`
//! surface takes `(tenant_ids, query, write_block)` plus an optional external
//! cancel token standing in for the context (no stats), so the helpers below
//! do the same: the same `cancel` is threaded through every binary-search
//! subquery, like Go sharing one request ctx across them.
//!
//! PORT NOTE: this module is not wired into esl-select yet. The wiring, mirroring
//! Go `eslstorage.RunQuery`, is [`crate::run_query`]: esl-select should call
//! `esl_storage::run_query(...)` instead of `Storage::run_query` so eligible
//! queries (detected via `Query::get_last_n_results_query`) take this path.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use esl_logstorage::parser::{Query, can_apply_last_n_results_optimization};
use esl_logstorage::query_stats::QueryStats;
use esl_logstorage::rows::Field;
use esl_logstorage::storage::Storage;
use esl_logstorage::storage_search::{BlockColumn, DataBlock, WriteDataBlockFn};
use esl_logstorage::tenant_id::TenantID;

/// Port of Go `runOptimizedLastNResultsQuery`.
///
/// Executes `q` (the query returned by `Query::get_last_n_results_query`, i.e.
/// with the trailing `sort by (_time) desc offset <offset> limit <limit>` pipe
/// removed) and streams the last `limit` rows (after `offset`) with the biggest
/// `_time` values to `write_block`.
#[allow(clippy::too_many_arguments)] // mirrors Go's runOptimizedLastNResultsQuery + qctx surface
pub fn run_optimized_last_n_results_query(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q: &Query,
    offset: u64,
    limit: u64,
    write_block: WriteDataBlockFn,
    cancel: Option<&Arc<AtomicBool>>,
    qs: &Arc<QueryStats>,
) -> Result<(), String> {
    let rows = get_last_n_query_results(storage, tenant_ids, q, offset + limit, cancel, qs)?;
    if offset >= rows.len() as u64 {
        return Ok(());
    }
    let rows = &rows[offset as usize..];

    let mut db = DataBlock::default();
    for r in rows {
        let columns: Vec<BlockColumn> = r
            .fields
            .iter()
            .map(|f| BlockColumn {
                name: f.name.clone(),
                values: vec![f.value.clone().into_bytes()],
            })
            .collect();
        db.set_columns(columns);
        write_block(0, &mut db);
    }
    Ok(())
}

/// Port of Go `getLastNQueryResults`.
fn get_last_n_query_results(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q_orig: &Query,
    limit: u64,
    cancel: Option<&Arc<AtomicBool>>,
    qs: &Arc<QueryStats>,
) -> Result<Vec<LogRow>, String> {
    let timestamp = q_orig.get_timestamp();

    let mut q = q_orig.clone(timestamp);
    q.add_pipe_offset_limit(0, 2 * limit);
    let rows = get_query_results(storage, tenant_ids, &q, cancel, qs)?;

    if (rows.len() as u64) < 2 * limit {
        // Fast path - the requested time range contains up to 2*limit rows.
        let rows = get_last_n_rows(rows, limit);
        return Ok(rows);
    }

    // Slow path - use binary search for adjusting time range for selecting up to 2*limit rows.
    let (mut start, mut end) = q.get_filter_time_range();
    end = end.saturating_add(1);
    start += end / 2 - start / 2;
    let mut n = limit;

    let mut rows_found: Vec<LogRow> = Vec::new();
    let mut last_non_empty_rows: Vec<LogRow> = Vec::new();

    loop {
        let mut q = q_orig.clone_with_time_filter(timestamp, start, end - 1);
        q.add_pipe_offset_limit(0, 2 * n);
        let rows = get_query_results(storage, tenant_ids, &q, cancel, qs)?;

        if end / 2 - start / 2 <= 0 {
            // The [start ... end) time range doesn't exceed a nanosecond, e.g. it cannot be adjusted more.
            // Return up to limit rows from the found rows and the last non-empty rows.
            rows_found.append(&mut last_non_empty_rows);
            rows_found.extend(rows);
            let rows_found = get_last_n_rows(rows_found, limit);
            return Ok(rows_found);
        }

        if rows.len() as u64 >= 2 * n {
            // The number of found rows on the [start ... end) time range exceeds 2*n,
            // so search for the rows on the adjusted time range [start+(end/2-start/2) ... end).
            if !can_apply_last_n_results_optimization(start, end) {
                // It is faster obtaining the last N logs as is on such a small time range instead of using binary search.
                let rows =
                    get_log_rows_last_n(storage, tenant_ids, q_orig, start, end, n, cancel, qs)?;
                rows_found.extend(rows);
                let rows_found = get_last_n_rows(rows_found, limit);
                return Ok(rows_found);
            }
            start += end / 2 - start / 2;
            last_non_empty_rows = rows;
            continue;
        }
        if (rows_found.len() + rows.len()) as u64 >= limit {
            // The found rows contains the needed limit rows with the biggest timestamps.
            rows_found.extend(rows);
            let rows_found = get_last_n_rows(rows_found, limit);
            return Ok(rows_found);
        }

        // The number of found rows is below the limit. This means the [start ... end) time range
        // doesn't cover the needed logs, so it must be extended.
        // Append the found rows to rowsFound, adjust n, so it doesn't take into account already found rows
        // and adjust the time range to search logs at [start-(end/2-start/2) ... start).
        n -= rows.len() as u64;
        rows_found.extend(rows);

        let d = end / 2 - start / 2;
        end = start;
        start -= d;
    }
}

/// Port of Go `getLogRowsLastN`.
#[allow(clippy::too_many_arguments)] // mirrors Go's getLogRowsLastN + qctx surface
fn get_log_rows_last_n(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q_orig: &Query,
    start: i64,
    end: i64,
    n: u64,
    cancel: Option<&Arc<AtomicBool>>,
    qs: &Arc<QueryStats>,
) -> Result<Vec<LogRow>, String> {
    let timestamp = q_orig.get_timestamp();
    let mut q = q_orig.clone_with_time_filter(timestamp, start, end);
    q.add_pipe_sort_by_time_desc();
    q.add_pipe_offset_limit(0, n);
    get_query_results(storage, tenant_ids, &q, cancel, qs)
}

/// Port of Go `getQueryResults`.
fn get_query_results(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q: &Query,
    cancel: Option<&Arc<AtomicBool>>,
    qs: &Arc<QueryStats>,
) -> Result<Vec<LogRow>, String> {
    let rows: Arc<Mutex<Vec<LogRow>>> = Arc::new(Mutex::new(Vec::new()));
    let err_local: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let rows_shared = Arc::clone(&rows);
    let err_shared = Arc::clone(&err_local);
    let write_block: WriteDataBlockFn =
        Arc::new(
            move |_worker_id, db: &mut DataBlock| match get_log_rows_from_data_block(db) {
                Ok(rows_local) => {
                    rows_shared.lock().unwrap().extend(rows_local);
                }
                Err(err) => {
                    *err_shared.lock().unwrap() = Some(err);
                }
            },
        );

    let result = crate::run_query_with_stats(storage, tenant_ids, q, write_block, cancel, qs);
    if let Some(err) = err_local.lock().unwrap().take() {
        return Err(err);
    }
    result?;

    let rows = std::mem::take(&mut *rows.lock().unwrap());
    Ok(rows)
}

/// Port of Go `getLogRowsFromDataBlock`.
fn get_log_rows_from_data_block(db: &mut DataBlock) -> Result<Vec<LogRow>, String> {
    let mut timestamps = Vec::new();
    if !db.get_timestamps(&mut timestamps) {
        return Err("missing _time field in the query results".to_string());
    }

    // There is no need to sort columns here, since they will be sorted by the caller.
    let columns = db.get_columns(false);

    // PORT NOTE: Go packs all rows' fields into one shared `fieldsBuf` arena and
    // slices per-row views out of it; the port keeps an owned `Vec<Field>` per
    // row (the established arena divergence for the block layer).
    let mut lrs = Vec::with_capacity(timestamps.len());
    for (i, &timestamp) in timestamps.iter().enumerate() {
        let mut fields = Vec::with_capacity(columns.len());

        // The _time column must go first, since the query results are sorted by _time.
        for c in columns {
            if c.name == "_time" {
                fields.push(Field {
                    name: "_time".to_string(),
                    value: String::from_utf8_lossy(&c.values[i]).into_owned(),
                });
            }
        }
        for c in columns {
            if c.name == "_time" {
                continue;
            }
            fields.push(Field {
                name: c.name.clone(),
                value: String::from_utf8_lossy(&c.values[i]).into_owned(),
            });
        }
        lrs.push(LogRow { timestamp, fields });
    }

    Ok(lrs)
}

/// Port of Go `logRow`.
struct LogRow {
    timestamp: i64,
    fields: Vec<Field>,
}

/// Port of Go `getLastNRows`.
fn get_last_n_rows(mut rows: Vec<LogRow>, limit: u64) -> Vec<LogRow> {
    sort_log_rows(&mut rows);
    if rows.len() as u64 > limit {
        rows.truncate(limit as usize);
    }
    rows
}

/// Port of Go `sortLogRows` (descending order by timestamp).
fn sort_log_rows(rows: &mut [LogRow]) {
    rows.sort_unstable_by_key(|r| std::cmp::Reverse(r.timestamp));
}

// PORT NOTE: upstream has no lastnoptimization_test.go; the tests below are
// port-specific end-to-end coverage of the optimized path against a temporary
// storage.
#[cfg(test)]
mod tests {
    use super::*;
    use esl_logstorage::log_rows::get_log_rows;
    use esl_logstorage::parser::ParseQueryAtTimestamp;
    use esl_logstorage::storage::StorageConfig;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn unique_path(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        // Include the pid so concurrent test processes (e.g. an interrupted
        // earlier run) never collide on the storage flock.
        let pid = std::process::id();
        std::env::temp_dir().join(format!("esl-storage-lastn-{name}-{pid}-{n}"))
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    /// Opens a temp storage with `rows_count` rows at distinct timestamps
    /// `base + i` seconds.
    fn open_with_rows(name: &str, rows_count: usize) -> (PathBuf, Arc<Storage>, i64) {
        let path = unique_path(name);
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);

        // Rows must be inside the retention window relative to the wall
        // clock, otherwise ingestion drops them; anchor near now.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        let base = now - (rows_count as i64 + 10) * 1_000_000_000;
        let stream_tags = ["job"];
        let mut lr = get_log_rows(&stream_tags, &[], &[], &[], "");
        let tenant_id = TenantID::default();
        for i in 0..rows_count {
            let mut fields = vec![
                field("job", "lastn-test"),
                field("", &format!("message {i}")),
                field("seq", &format!("{i}")),
            ];
            lr.must_add(
                tenant_id,
                base + (i as i64) * 1_000_000_000,
                &mut fields,
                -1,
            );
        }
        s.must_add_rows(&lr);
        s.debug_flush();
        (path, s, base)
    }

    /// Runs `query` through `crate::run_query` and returns `(seq, _time)`
    /// values of the returned rows in emission order.
    fn run_collect(s: &Arc<Storage>, query: &str, timestamp: i64) -> Vec<(String, String)> {
        let q = ParseQueryAtTimestamp(query, timestamp).unwrap();
        let out: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let out_shared = Arc::clone(&out);
        let write_block: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
            let mut seqs = Vec::new();
            let mut times = Vec::new();
            for c in db.get_columns(false) {
                for v in &c.values {
                    let v = String::from_utf8_lossy(v).into_owned();
                    if c.name == "seq" {
                        seqs.push(v);
                    } else if c.name == "_time" {
                        times.push(v);
                    }
                }
            }
            let mut out = out_shared.lock().unwrap();
            for (seq, time) in seqs.into_iter().zip(times) {
                out.push((seq, time));
            }
        });
        crate::run_query(s, &[TenantID::default()], &q, write_block).unwrap();
        out.lock().unwrap().clone()
    }

    #[test]
    fn test_get_last_n_results_query_detection() {
        let ts = 1_700_000_100_000_000_000_i64;

        // Eligible: trailing `sort by (_time) desc limit N` on an unbounded time range.
        let q = ParseQueryAtTimestamp("* | sort by (_time) desc limit 5", ts).unwrap();
        let (q_opt, offset, limit) = q.get_last_n_results_query().expect("must be eligible");
        assert_eq!((offset, limit), (0, 5));
        assert!(q_opt.pipes().is_empty());

        // Trailing `fields` pipe keeping _time stays eligible and is preserved.
        let q = ParseQueryAtTimestamp("* | sort by (_time) desc limit 5 | fields _time, seq", ts)
            .unwrap();
        let (q_opt, _, limit) = q.get_last_n_results_query().expect("must be eligible");
        assert_eq!(limit, 5);
        assert_eq!(q_opt.pipes().len(), 1);

        // Not eligible: no sort pipe.
        let q = ParseQueryAtTimestamp("* | stats count() rows", ts).unwrap();
        assert!(q.get_last_n_results_query().is_none());

        // Not eligible: too big limit.
        let q = ParseQueryAtTimestamp("* | sort by (_time) desc limit 100000", ts).unwrap();
        assert!(q.get_last_n_results_query().is_none());

        // Not eligible: ascending order.
        let q = ParseQueryAtTimestamp("* | sort by (_time) limit 5", ts).unwrap();
        assert!(q.get_last_n_results_query().is_none());

        // Not eligible: small explicit time range (optimization not worth it).
        let q = ParseQueryAtTimestamp(
            "_time:[2023-11-14T22:13:20Z,2023-11-14T22:13:21Z] | sort by (_time) desc limit 5",
            ts,
        )
        .unwrap();
        assert!(q.get_last_n_results_query().is_none());
    }

    #[test]
    fn test_run_query_last_n_fast_path() {
        // 4 rows, limit 3: the initial probe returns < 2*limit rows.
        let (path, s, base) = open_with_rows("fast", 4);
        let rows = run_collect(&s, "* | sort by (_time) desc limit 3", base + 100);
        let seqs: Vec<&str> = rows.iter().map(|(seq, _)| seq.as_str()).collect();
        assert_eq!(seqs, vec!["3", "2", "1"]);
        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_run_query_last_n_binary_search_path() {
        // 12 rows, limit 3: the initial probe returns 2*limit rows, which
        // triggers the adaptive time-range narrowing.
        let (path, s, base) = open_with_rows("slow", 12);
        let rows = run_collect(&s, "* | sort by (_time) desc limit 3", base + 100);
        let seqs: Vec<&str> = rows.iter().map(|(seq, _)| seq.as_str()).collect();
        assert_eq!(seqs, vec!["11", "10", "9"]);
        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_run_query_last_n_with_offset() {
        let (path, s, base) = open_with_rows("offset", 12);
        let rows = run_collect(&s, "* | sort by (_time) desc offset 2 limit 3", base + 100);
        let seqs: Vec<&str> = rows.iter().map(|(seq, _)| seq.as_str()).collect();
        assert_eq!(seqs, vec!["9", "8", "7"]);
        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }
}

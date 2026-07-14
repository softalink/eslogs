//! Port of live tailing (`/select/logsql/tail`) from
//! `app/eslselect/logsql/logsql.go`: `ProcessLiveTailRequest`, `tailProcessor`
//! and `sortLogRows`, plus `WriteJSONRows` from `query_response.qtpl.go`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use esl_common::httpserver::{Request, ResponseWriter};
use esl_logstorage::rows::{Field, skip_leading_fields_without_values};
use esl_logstorage::storage::Storage;
use esl_logstorage::storage_search::{DataBlock, WriteDataBlockFn};

use crate::logsql::{append_json_string, now_nsec, parse_common_args_with_config, parse_duration};

/// Go `tailOffsetNsecs`: how far back each refresh re-reads to pick up
/// late-arriving rows.
const TAIL_OFFSET_NSECS: i64 = 5_000_000_000;

/// Handles `/select/logsql/tail` (Go `ProcessLiveTailRequest`).
///
/// See <https://docs.victoriametrics.com/victorialogs/querying/#live-tailing>
///
/// PORT NOTE: Go streams via `http.Flusher` and stops when the request
/// context is canceled. The Rust `ResponseWriter` buffers responses, so this
/// uses the minimal [`ResponseWriter::flush_chunk`] streaming hook (chunked
/// transfer-encoding): each refresh pushes any new rows to the client, and a
/// `flush_chunk` error (client disconnect or server shutdown) terminates the
/// loop — the Rust equivalent of Go's `<-doneCh`. When no streaming transport
/// is attached (direct handler invocation outside a server connection), a
/// single query iteration runs and the buffered rows are returned as a
/// regular response.
///
fn live_tail_requests() -> &'static Arc<esl_common::metrics::Counter> {
    static C: LazyLock<Arc<esl_common::metrics::Counter>> =
        LazyLock::new(|| esl_common::metrics::new_counter("esl_live_tailing_requests"));
    &C
}

/// Minimal drop-guard mirroring Go's `defer` for the live-tail counter.
fn scopeguard<F: FnMut()>(f: F) -> impl Drop {
    struct Guard<F: FnMut()>(F);
    impl<F: FnMut()> Drop for Guard<F> {
        fn drop(&mut self) {
            (self.0)();
        }
    }
    Guard(f)
}

pub fn process_live_tail_request(storage: &Arc<Storage>, req: &Request, w: &mut ResponseWriter) {
    // Go increments liveTailRequests and decrements it via defer, so the
    // counter tracks the number of currently active live tails.
    live_tail_requests().inc();
    let _dec_on_return = scopeguard(|| live_tail_requests().dec());
    let ca = match parse_common_args_with_config(req, true) {
        Ok(ca) => ca,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };
    if !ca.q.can_live_tail() {
        w.errorf(
            req,
            &format!(
                "the query [{}] cannot be used in live tailing; \
                 see https://docs.victoriametrics.com/victorialogs/querying/#live-tailing for details",
                ca.q
            ),
        );
        return;
    }

    let refresh_interval = match parse_duration(req, "refresh_interval", "1s") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };
    if refresh_interval <= 0 {
        w.errorf(req, "'refresh_interval' must be bigger than zero");
        return;
    }

    let start_offset = match parse_duration(req, "start_offset", "5s") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    let offset = match parse_duration(req, "offset", "5s") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    let need_sort_fields = !ca.q.is_fixed_output_fields_order();
    let tp = Arc::new(TailProcessor::new(need_sort_fields));

    let mut end = now_nsec() - offset;
    let mut start = end - start_offset;

    w.set_header("Content-Type", "application/x-ndjson");
    w.set_header("Access-Control-Allow-Origin", "*");
    if w.flush_chunk().is_err() {
        return;
    }

    let q_orig = &ca.q;
    loop {
        // Go uses a time.Ticker, so the refresh period is fixed rather than
        // added on top of the query time; sleep only the remainder below.
        let iter_start = std::time::Instant::now();
        let q = q_orig.clone_with_time_filter(end, start, end);

        let tp_block = Arc::clone(&tp);
        let write_block: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
            tp_block.write_block(db);
        });
        // PORT NOTE: no disconnect-watcher token here (unlike the buffered
        // query handlers): this handler streams via flush_chunk, whose
        // non-blocking socket probes would race the watcher's. Disconnects
        // are instead observed by the flush_chunk call below, once per
        // refresh window — each windowed run_query scans a tail-sized time
        // range, so a mid-window cancel would save little.
        if let Err(e) = storage.run_query_with_stats(
            &ca.tenant_ids,
            &q,
            &ca.hidden_fields_filters,
            write_block,
            None,
            &ca.qs,
        ) {
            w.errorf(req, &format!("cannot execute tail query [{q}]: {e}"));
            return;
        }
        let result_rows = match tp.get_tail_rows() {
            Ok(rows) => rows,
            Err(e) => {
                w.errorf(
                    req,
                    &format!("cannot get tail results for query [{q}]: {e}"),
                );
                return;
            }
        };
        if !result_rows.is_empty() {
            write_json_rows(w, &result_rows);
        }
        // Push the new rows (if any) and probe the connection; an error means
        // the client is gone or the server is shutting down (Go `<-doneCh`).
        if w.flush_chunk().is_err() {
            return;
        }
        if !w.supports_streaming() {
            // Buffered mode (no live connection): return the first window.
            return;
        }

        let period = Duration::from_nanos(refresh_interval as u64);
        if let Some(remaining) = period.checked_sub(iter_start.elapsed()) {
            std::thread::sleep(remaining);
        }
        start = end - TAIL_OFFSET_NSECS;
        end = now_nsec() - offset;
    }
}

/// Builds the Go `JSONRows` payload (query_response.qtpl.go): each row becomes
/// one ndjson object; leading fields without values are skipped, as are
/// empty-valued fields after the first.
fn json_rows(rows: &[Vec<Field>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for fields in rows {
        let fields = skip_leading_fields_without_values(fields);
        if fields.is_empty() {
            continue;
        }
        buf.push(b'{');
        let f = &fields[0];
        append_json_string(&mut buf, &f.name);
        buf.push(b':');
        append_json_string(&mut buf, &f.value);
        for f in &fields[1..] {
            if f.value.is_empty() {
                continue;
            }
            buf.push(b',');
            append_json_string(&mut buf, &f.name);
            buf.push(b':');
            append_json_string(&mut buf, &f.value);
        }
        buf.extend_from_slice(b"}\n");
    }
    buf
}

/// Port of Go `WriteJSONRows` (query_response.qtpl.go).
fn write_json_rows(w: &mut ResponseWriter, rows: &[Vec<Field>]) {
    w.write_bytes(&json_rows(rows));
}

/// Go `logRow`.
struct LogRow {
    timestamp: i64,
    fields: Vec<Field>,
}

/// Go `sortLogRows`: stable sort by timestamp.
fn sort_log_rows(rows: &mut [LogRow]) {
    rows.sort_by_key(|r| r.timestamp);
}

/// Go `tailProcessor`.
///
/// PORT NOTE: Go carries a `cancel` func that aborts the running query when a
/// block without a `_time` field is seen; the Rust `run_query` surface has no
/// cancellation, so the recorded error makes the remaining block callbacks
/// no-ops instead and is reported by [`TailProcessor::get_tail_rows`].
struct TailProcessor {
    need_sort_fields: bool,
    state: Mutex<TailState>,
}

#[derive(Default)]
struct TailState {
    per_stream_rows: HashMap<Vec<u8>, Vec<LogRow>>,
    last_timestamps: HashMap<Vec<u8>, i64>,
    err: Option<String>,
}

impl TailProcessor {
    /// Go `newTailProcessor`.
    fn new(need_sort_fields: bool) -> Self {
        TailProcessor {
            need_sort_fields,
            state: Mutex::new(TailState::default()),
        }
    }

    /// Go `tailProcessor.writeBlock`.
    fn write_block(&self, db: &mut DataBlock) {
        if db.rows_count() == 0 {
            return;
        }

        let mut st = self.state.lock().unwrap();
        if st.err.is_some() {
            return;
        }

        // Make sure columns contain _time field, since it is needed for proper
        // tail work.
        let mut timestamps = Vec::new();
        if !db.get_timestamps(&mut timestamps) {
            st.err = Some("missing _time field".to_string());
            return;
        }

        // Copy block rows to per_stream_rows.
        let columns = db.get_columns(self.need_sort_fields);
        for (i, &timestamp) in timestamps.iter().enumerate() {
            let mut stream_id = Vec::new();
            let mut fields = Vec::with_capacity(columns.len());
            for c in columns {
                let value = c.values[i].clone();
                if c.name == b"_stream_id" {
                    stream_id = value.clone();
                }
                fields.push(Field {
                    name: c.name.clone(),
                    value,
                });
            }

            st.per_stream_rows
                .entry(stream_id)
                .or_default()
                .push(LogRow { timestamp, fields });
        }
    }

    /// Go `tailProcessor.getTailRows`.
    fn get_tail_rows(&self) -> Result<Vec<Vec<Field>>, String> {
        let mut st = self.state.lock().unwrap();
        if let Some(err) = &st.err {
            return Err(err.clone());
        }

        let per_stream_rows = std::mem::take(&mut st.per_stream_rows);
        let mut result_rows: Vec<LogRow> = Vec::new();
        for (stream_id, mut rows) in per_stream_rows {
            sort_log_rows(&mut rows);

            if let Some(&last_timestamp) = st.last_timestamps.get(&stream_id) {
                // Skip already written rows
                rows.retain(|r| r.timestamp > last_timestamp);
            }
            if let Some(last) = rows.last() {
                st.last_timestamps.insert(stream_id, last.timestamp);
                result_rows.extend(rows);
            }
        }

        sort_log_rows(&mut result_rows);

        Ok(result_rows.into_iter().map(|r| r.fields).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logsql::test_support::*;
    use esl_common::httpserver::serve;
    use esl_logstorage::log_rows::get_log_rows;
    use esl_logstorage::tenant_id::TenantID;
    use std::io::{Read, Write};
    use std::net::TcpStream;

    #[test]
    fn test_json_rows() {
        let rows = vec![
            vec![
                Field {
                    name: b"_msg".to_vec(),
                    value: b"hello".to_vec(),
                },
                Field {
                    name: b"empty".to_vec(),
                    value: Vec::new(),
                },
                Field {
                    name: b"host".to_vec(),
                    value: b"node-1".to_vec(),
                },
            ],
            // Leading empty-valued fields are skipped entirely.
            vec![
                Field {
                    name: b"lead".to_vec(),
                    value: Vec::new(),
                },
                Field {
                    name: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
            ],
            // Rows with no valued fields produce no output.
            vec![Field {
                name: b"only-empty".to_vec(),
                value: Vec::new(),
            }],
        ];
        assert_eq!(
            String::from_utf8(json_rows(&rows)).unwrap(),
            "{\"_msg\":\"hello\",\"host\":\"node-1\"}\n{\"k\":\"v\"}\n"
        );
        assert!(json_rows(&[]).is_empty());
    }

    #[test]
    fn test_tail_rejects_non_tailable_query() {
        let (storage, path) = open_storage_with_rows("tailbad", unique_nsec(), &[("x", "node-1")]);
        let storage_h = std::sync::Arc::clone(&storage);
        let handle = serve("127.0.0.1:0", move |req, w| {
            process_live_tail_request(&storage_h, req, w);
        })
        .expect("serve");
        let addr = handle.local_addr();

        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/tail?query={}",
                encode("* | stats count() rows")
            ),
        );
        assert_eq!(status, 400, "body={body}");
        assert!(
            body.contains("cannot be used in live tailing"),
            "body={body}"
        );

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_process_live_tail_request_streams_rows() {
        let (storage, path) = open_storage_with_rows(
            "tail",
            unique_nsec(),
            &[("tail-one", "node-1"), ("tail-two", "node-1")],
        );

        let storage_h = std::sync::Arc::clone(&storage);
        let handle = serve("127.0.0.1:0", move |req, w| {
            process_live_tail_request(&storage_h, req, w);
        })
        .expect("serve");
        let addr = handle.local_addr();

        // Wide start_offset so the just-ingested rows are inside the initial
        // window; offset=0 so newly added rows become visible immediately.
        let mut s = TcpStream::connect(addr).expect("connect");
        write!(
            s,
            "GET /select/logsql/tail?query={}&start_offset=1h&offset=0s&refresh_interval=100ms \
             HTTP/1.1\r\nHost: test\r\n\r\n",
            encode("*")
        )
        .expect("send request");
        s.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut collected = String::new();
        let mut ingested_late = false;
        loop {
            let mut chunk = [0u8; 4096];
            match s.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => collected.push_str(&String::from_utf8_lossy(&chunk[..n])),
                Err(_) => {} // read timeout: keep polling until the deadline
            }

            if collected.contains("tail-one") && collected.contains("tail-two") && !ingested_late {
                // Initial window received: ingest one more row and wait for the
                // periodic refresh to stream it.
                let tenant = TenantID::default();
                let mut lr = get_log_rows(&["host"], &[], &[], &[], "");
                let mut fields = vec![
                    Field {
                        name: b"_msg".to_vec(),
                        value: b"tail-late".to_vec(),
                    },
                    Field {
                        name: b"host".to_vec(),
                        value: b"node-1".to_vec(),
                    },
                ];
                lr.must_add(tenant, unique_nsec(), &mut fields, -1);
                storage.must_add_rows(&lr);
                storage.debug_flush();
                ingested_late = true;
            }
            if collected.contains("tail-late") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for tail rows; collected={collected}"
            );
        }

        assert!(
            collected.contains("Transfer-Encoding: chunked"),
            "collected={collected}"
        );
        assert!(
            collected.contains("Content-Type: application/x-ndjson"),
            "collected={collected}"
        );
        assert!(collected.contains("tail-one"), "collected={collected}");
        assert!(collected.contains("tail-two"), "collected={collected}");
        assert!(collected.contains("tail-late"), "collected={collected}");
        // Rows already streamed must not be re-sent by later refreshes
        // (per-stream lastTimestamps dedup).
        assert_eq!(
            collected.matches("tail-one").count(),
            1,
            "duplicate rows streamed; collected={collected}"
        );

        // Closing the client connection terminates the handler loop.
        drop(s);

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }
}

//! Port of `/select/logsql/hits` — Go `ProcessHitsRequest` in
//! `app/eslselect/logsql/logsql.go` plus the `hits_response.qtpl` response
//! writers (hand-rolled JSON, matching the house style in `logsql.rs`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use esl_common::httpserver::{Request, ResponseWriter};
use esl_logstorage::storage::Storage;
use esl_logstorage::storage_search::{BlockColumn, DataBlock, WriteDataBlockFn};
use esl_logstorage::values_encoder::try_parse_timestamp_rfc3339_nano;

use crate::logsql::{
    align_start_end_to_step, append_json_string, get_positive_int, parse_common_args,
    parse_duration, timestamp_to_string,
};

/// Go `hitsSeries`.
#[derive(Default)]
struct HitsSeries {
    hits_total: u64,
    timestamps: Vec<i64>,
    hits: Vec<u64>,
}

impl HitsSeries {
    /// Go `(*hitsSeries).sort`: co-sorts timestamps and hits by timestamp.
    fn sort(&mut self) {
        let mut pairs: Vec<(i64, u64)> = self
            .timestamps
            .iter()
            .copied()
            .zip(self.hits.iter().copied())
            .collect();
        pairs.sort_by_key(|&(ts, _)| ts);
        self.timestamps = pairs.iter().map(|&(ts, _)| ts).collect();
        self.hits = pairs.iter().map(|&(_, h)| h).collect();
    }
}

/// Handles `/select/logsql/hits` (Go `ProcessHitsRequest`).
///
/// See <https://docs.victoriametrics.com/victorialogs/querying/#querying-hits-stats>
pub fn process_hits_request(storage: &Arc<Storage>, req: &Request, w: &mut ResponseWriter) {
    let start_time = Instant::now();

    let mut ca = match parse_common_args(req) {
        Ok(ca) => ca,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Obtain step
    let step = match parse_duration(req, "step", "") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };
    if step <= 0 {
        w.errorf(req, "'step' must be bigger than zero");
        return;
    }

    // Obtain offset
    let offset = match parse_duration(req, "offset", "0s") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Obtain field entries
    let fields: Vec<String> = req
        .form_values("field")
        .iter()
        .map(|s| s.to_string())
        .collect();

    // Obtain limit on the number of top fields entries.
    let fields_limit = match get_positive_int(req, "fields_limit") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Add a pipe, which calculates hits over time with the given step and
    // offset for the given fields.
    ca.q.add_count_by_time_pipe(step, offset, &fields);

    let m: Arc<Mutex<HashMap<Vec<u8>, HitsSeries>>> = Arc::new(Mutex::new(HashMap::new()));
    let m_cl = Arc::clone(&m);
    let write_fn: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
        let rows_count = db.rows_count();
        if rows_count == 0 {
            return;
        }

        let columns = db.get_columns(false);
        let timestamp_values = &columns[0].values;
        let hits_values = &columns[columns.len() - 1].values;
        let field_columns = &columns[1..columns.len() - 1];

        for i in 0..rows_count {
            let ts_str = String::from_utf8_lossy(&timestamp_values[i]);
            let Some(timestamp_nsec) = try_parse_timestamp_rfc3339_nano(&ts_str) else {
                esl_common::panicf!("BUG: cannot parse timestamp={ts_str:?}");
                unreachable!()
            };
            let hits_str = String::from_utf8_lossy(&hits_values[i]);
            let hits: u64 = match hits_str.parse() {
                Ok(v) => v,
                Err(e) => {
                    esl_common::panicf!("BUG: cannot parse hitsStr={hits_str:?}: {e}");
                    unreachable!()
                }
            };

            let mut key = Vec::new();
            write_fields_for_hits(&mut key, field_columns, i);

            let mut m = m_cl.lock().unwrap();
            let hs = m.entry(key).or_default();
            hs.timestamps.push(timestamp_nsec);
            hs.hits.push(hits);
            hs.hits_total += hits;
        }
    });

    // Execute the query
    if let Err(e) = storage.run_query(&ca.tenant_ids, &ca.q, write_fn) {
        w.errorf(req, &format!("cannot execute query [{}]: {e}", ca.q));
        return;
    }

    let m = match Arc::try_unwrap(m) {
        Ok(m) => m.into_inner().unwrap(),
        // All query workers have joined, so take the map instead of cloning.
        Err(arc) => std::mem::take(&mut *arc.lock().unwrap()),
    };
    let mut m = get_top_hits_series(m, fields_limit);
    add_missing_zero_hits(&mut m, ca.start_aligned, ca.end_aligned, step, offset);

    // Write response headers
    w.set_header("Content-Type", "application/json");
    ca.write_response_headers(w, start_time);

    // Write response
    let mut body = Vec::new();
    write_hits_series(&mut body, m);
    w.write_bytes(&body);
}

/// Go `WriteFieldsForHits` (hits_response.qtpl): formats the per-series label
/// object `{"name":"value",...}` for row `row_idx`.
fn write_fields_for_hits(dst: &mut Vec<u8>, columns: &[BlockColumn], row_idx: usize) {
    dst.push(b'{');
    if !columns.is_empty() {
        append_json_string(dst, columns[0].name.as_bytes());
        dst.push(b':');
        append_json_string(dst, &columns[0].values[row_idx]);
        for c in &columns[1..] {
            dst.push(b',');
            append_json_string(dst, c.name.as_bytes());
            dst.push(b':');
            append_json_string(dst, &c.values[row_idx]);
        }
    }
    dst.push(b'}');
}

/// Go `addMissingZeroHits`: fills zero-hit buckets over `[start, end]` aligned
/// to the step, so every series covers the whole selected time range.
fn add_missing_zero_hits(
    m: &mut HashMap<Vec<u8>, HitsSeries>,
    mut start: i64,
    mut end: i64,
    step: i64,
    offset: i64,
) {
    if start == i64::MIN {
        start = i64::MAX;
        for hs in m.values() {
            if let Some(&min_ts) = hs.timestamps.iter().min() {
                start = start.min(min_ts);
            }
        }
    }

    if end == i64::MAX {
        end = i64::MIN;
        for hs in m.values() {
            if let Some(&max_ts) = hs.timestamps.iter().max() {
                end = end.max(max_ts);
            }
        }
    }

    (start, end) = align_start_end_to_step(start, end, step, offset);

    if start > end {
        // nothing to do
        return;
    }

    for hs in m.values_mut() {
        let mut ts = start;
        while ts <= end {
            if !hs.timestamps.contains(&ts) {
                hs.timestamps.push(ts);
                hs.hits.push(0);
            }

            match ts.checked_add(step) {
                // stop on int64 overflow
                None => break,
                Some(next) => ts = next,
            }
        }
    }
}

/// Go `getTopHitsSeries`: keeps the `fields_limit` series with the biggest
/// total hits and merges the rest into the `{}` series.
fn get_top_hits_series(
    m: HashMap<Vec<u8>, HitsSeries>,
    fields_limit: i64,
) -> HashMap<Vec<u8>, HitsSeries> {
    if fields_limit <= 0 || fields_limit as usize >= m.len() {
        return m;
    }
    let fields_limit = fields_limit as usize;

    let mut a: Vec<(Vec<u8>, HitsSeries)> = m.into_iter().collect();
    a.sort_by_key(|x| std::cmp::Reverse(x.1.hits_total));

    let mut hits_other: HashMap<i64, u64> = HashMap::new();
    for (_, hs) in &a[fields_limit..] {
        for (i, &timestamp) in hs.timestamps.iter().enumerate() {
            *hits_other.entry(timestamp).or_insert(0) += hs.hits[i];
        }
    }
    let mut hs_other = HitsSeries::default();
    for (timestamp, hits) in hits_other {
        hs_other.timestamps.push(timestamp);
        hs_other.hits.push(hits);
        hs_other.hits_total += hits;
    }

    let mut m_new: HashMap<Vec<u8>, HitsSeries> = HashMap::with_capacity(fields_limit + 1);
    for (fields_str, hs) in a.into_iter().take(fields_limit) {
        m_new.insert(fields_str, hs);
    }
    m_new.insert(b"{}".to_vec(), hs_other);

    m_new
}

/// Go `WriteHitsSeries` (hits_response.qtpl): `{"hits":[<series>,...]}` with
/// series ordered by their label-object key.
fn write_hits_series(dst: &mut Vec<u8>, mut m: HashMap<Vec<u8>, HitsSeries>) {
    let mut sorted_keys: Vec<Vec<u8>> = m.keys().cloned().collect();
    sorted_keys.sort();

    dst.extend_from_slice(b"{\"hits\":[");
    for (i, k) in sorted_keys.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        let hs = m.get_mut(k).expect("BUG: key must be present");
        write_hits_series_line(dst, k, hs);
    }
    dst.extend_from_slice(b"]}");
}

/// Go `hitsSeriesLine` (hits_response.qtpl).
fn write_hits_series_line(dst: &mut Vec<u8>, k: &[u8], hs: &mut HitsSeries) {
    hs.sort();

    dst.extend_from_slice(b"{\"fields\":");
    dst.extend_from_slice(k);
    dst.extend_from_slice(b",\"timestamps\":[");
    for (i, &ts) in hs.timestamps.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        append_json_string(dst, timestamp_to_string(ts).as_bytes());
    }
    dst.extend_from_slice(b"],\"values\":[");
    for (i, &v) in hs.hits.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        dst.extend_from_slice(v.to_string().as_bytes());
    }
    dst.extend_from_slice(b"],\"total\":");
    dst.extend_from_slice(hs.hits_total.to_string().as_bytes());
    dst.push(b'}');
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::logsql::test_support::{encode, http_get, open_storage_with_rows, unique_nsec};
    use esl_common::httpserver::serve;

    /// Round-trip test against a temp Storage: ingests rows and queries
    /// /select/logsql/hits through a real HTTP server.
    #[test]
    fn test_process_hits_request_roundtrip() {
        let base = unique_nsec();
        let rows = [
            ("connection error occurred", "node-1"),
            ("all systems nominal", "node-1"),
            ("disk error on node 3", "node-2"),
            ("request completed ok", "node-2"),
            ("cache warmed", "node-2"),
        ];
        let (storage, path) = open_storage_with_rows("hits", base, &rows);

        let storage_h = Arc::clone(&storage);
        let handle = serve("127.0.0.1:0", move |req, w| match req.path() {
            "/select/logsql/hits" => process_hits_request(&storage_h, req, w),
            _ => w.errorf(req, "unexpected path"),
        })
        .expect("serve");
        let addr = handle.local_addr();

        // All rows land in a single step bucket (step=1000d), no field grouping:
        // a single `{}` series with total=5.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/hits?query={}&step={}",
                encode("*"),
                encode("1000d")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert!(body.starts_with("{\"hits\":["), "body={body}");
        assert!(body.contains("\"fields\":{}"), "body={body}");
        assert!(body.contains("\"total\":5"), "body={body}");

        // Group by host: two series with totals 2 and 3.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/hits?query={}&step={}&field=host",
                encode("*"),
                encode("1000d")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert!(
            body.contains("\"fields\":{\"host\":\"node-1\"}"),
            "body={body}"
        );
        assert!(
            body.contains("\"fields\":{\"host\":\"node-2\"}"),
            "body={body}"
        );
        assert!(body.contains("\"total\":2"), "body={body}");
        assert!(body.contains("\"total\":3"), "body={body}");

        // fields_limit=1 keeps the top series and merges the rest into `{}`.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/hits?query={}&step={}&field=host&fields_limit=1",
                encode("*"),
                encode("1000d")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert!(
            body.contains("\"fields\":{\"host\":\"node-2\"}"),
            "top series must be kept; body={body}"
        );
        assert!(
            body.contains("\"fields\":{}"),
            "other series must be merged into {{}}; body={body}"
        );
        assert!(
            !body.contains("\"fields\":{\"host\":\"node-1\"}"),
            "body={body}"
        );

        // Missing step is an error.
        let (status, body) = http_get(addr, &format!("/select/logsql/hits?query={}", encode("*")));
        assert_eq!(status, 400, "body={body}");
        assert!(
            body.contains("cannot parse duration from the arg 'step='"),
            "body={body}"
        );

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_get_top_hits_series() {
        let mut m: HashMap<Vec<u8>, HitsSeries> = HashMap::new();
        for (key, total) in [("a", 10u64), ("b", 5), ("c", 1)] {
            m.insert(
                format!("{{\"k\":\"{key}\"}}").into_bytes(),
                HitsSeries {
                    hits_total: total,
                    timestamps: vec![0],
                    hits: vec![total],
                },
            );
        }
        let m = get_top_hits_series(m, 2);
        assert_eq!(m.len(), 3);
        assert!(m.contains_key(b"{\"k\":\"a\"}".as_slice()));
        assert!(m.contains_key(b"{\"k\":\"b\"}".as_slice()));
        let other = m.get(b"{}".as_slice()).expect("merged {} series");
        assert_eq!(other.hits_total, 1);
    }

    #[test]
    fn test_add_missing_zero_hits() {
        let mut m: HashMap<Vec<u8>, HitsSeries> = HashMap::new();
        m.insert(
            b"{}".to_vec(),
            HitsSeries {
                hits_total: 3,
                timestamps: vec![10],
                hits: vec![3],
            },
        );
        // start/end unset: the range is derived from the data; a single bucket
        // stays untouched.
        add_missing_zero_hits(&mut m, i64::MIN, i64::MAX, 10, 0);
        let hs = m.get(b"{}".as_slice()).unwrap();
        assert_eq!(hs.timestamps, vec![10]);

        // Explicit [0, 30) range with step=10 fills buckets 0 and 20.
        let mut m: HashMap<Vec<u8>, HitsSeries> = HashMap::new();
        m.insert(
            b"{}".to_vec(),
            HitsSeries {
                hits_total: 3,
                timestamps: vec![10],
                hits: vec![3],
            },
        );
        add_missing_zero_hits(&mut m, 0, 29, 10, 0);
        let hs = m.get_mut(b"{}".as_slice()).unwrap();
        hs.sort();
        assert_eq!(hs.timestamps, vec![0, 10, 20]);
        assert_eq!(hs.hits, vec![0, 3, 0]);
    }
}

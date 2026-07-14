//! Port of `/select/logsql/stats_query` and `/select/logsql/stats_query_range`
//! — Go `ProcessStatsQueryRequest` / `ProcessStatsQueryRangeRequest` in
//! `app/eslselect/logsql/logsql.go` plus the `stats_query_response.qtpl` /
//! `stats_query_range_response.qtpl` writers (hand-rolled JSON, matching the
//! house style in `logsql.rs`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use esl_common::httpserver::{Request, ResponseWriter};
use esl_logstorage::rows::{Field, marshal_fields_to_json};
use esl_logstorage::storage::Storage;
use esl_logstorage::storage_search::{DataBlock, WriteDataBlockFn, is_query_canceled_error};
use esl_logstorage::values_encoder::try_parse_timestamp_rfc3339_nano;

use crate::logsql::{
    JsonScanner, append_json_string, parse_common_args, parse_duration, send_prometheus_error,
};

/// Go `statsRow`.
struct StatsRow {
    name: Vec<u8>,
    labels: Vec<Field>,
    timestamp: i64,
    value: Vec<u8>,
}

/// Go `statsSeries` (the `key` lives as the map key / sort key).
struct StatsSeries {
    name: Vec<u8>,
    labels: Vec<Field>,
    points: Vec<StatsPoint>,
}

/// Go `statsPoint`.
struct StatsPoint {
    timestamp: i64,
    value: Vec<u8>,
}

/// Go `histogramBucket`.
struct HistogramBucket {
    vmrange: String,
    hits: u64,
}

/// Parses the output of the `histogram()` stats function:
/// `[{"vmrange":"...","hits":N},...]` (Go `json.Unmarshal` into
/// `[]histogramBucket`; unknown keys are ignored).
fn parse_histogram_buckets(s: &str) -> Result<Vec<HistogramBucket>, String> {
    let mut sc = JsonScanner::new(s);
    sc.skip_ws();
    sc.expect(b'[')?;
    let mut out = Vec::new();
    sc.skip_ws();
    if sc.peek() == Some(b']') {
        sc.advance();
    } else {
        loop {
            sc.skip_ws();
            sc.expect(b'{')?;
            let mut vmrange = String::new();
            let mut hits = 0u64;
            sc.skip_ws();
            if sc.peek() == Some(b'}') {
                sc.advance();
            } else {
                loop {
                    sc.skip_ws();
                    let key = sc.parse_string()?;
                    sc.skip_ws();
                    sc.expect(b':')?;
                    sc.skip_ws();
                    match sc.peek() {
                        Some(b'"') => {
                            // Go json.Unmarshal errors when `hits` is a string.
                            if key == "hits" {
                                return Err("cannot parse string as `hits` number".to_string());
                            }
                            let v = sc.parse_string()?;
                            if key == "vmrange" {
                                vmrange = v;
                            }
                        }
                        Some(b'0'..=b'9') => {
                            // Go json.Unmarshal errors when `vmrange` is a number.
                            if key == "vmrange" {
                                return Err("cannot parse number as `vmrange` string".to_string());
                            }
                            let v = sc.parse_u64()?;
                            if key == "hits" {
                                hits = v;
                            }
                        }
                        _ => return Err(format!("unexpected value for key {key:?}")),
                    }
                    sc.skip_ws();
                    match sc.peek() {
                        Some(b',') => sc.advance(),
                        Some(b'}') => {
                            sc.advance();
                            break;
                        }
                        _ => return Err("expecting ',' or '}' in JSON object".to_string()),
                    }
                }
            }
            out.push(HistogramBucket { vmrange, hits });
            sc.skip_ws();
            match sc.peek() {
                Some(b',') => sc.advance(),
                Some(b']') => {
                    sc.advance();
                    break;
                }
                _ => return Err("expecting ',' or ']' in JSON array".to_string()),
            }
        }
    }
    if !sc.at_end() {
        return Err("unexpected trailing data after JSON array".to_string());
    }
    Ok(out)
}

/// Returns true if `v` looks like the output of the `histogram()` stats
/// function (Go checks `v == "[]" || strings.HasPrefix(v, "[{\"vmrange\":\"")`).
fn is_histogram_value(v: &str) -> bool {
    v == "[]" || v.starts_with(r#"[{"vmrange":""#)
}

/// Handles `/select/logsql/stats_query` (Go `ProcessStatsQueryRequest`).
///
/// See <https://docs.victoriametrics.com/victorialogs/querying/#querying-log-stats>
pub fn process_stats_query_request(storage: &Arc<Storage>, req: &Request, w: &mut ResponseWriter) {
    let start_time = Instant::now();

    let mut ca = match parse_common_args(req) {
        Ok(ca) => ca,
        Err(e) => {
            send_prometheus_error(w, req, &e);
            return;
        }
    };

    let label_fields = match ca.q.get_stats_labels() {
        Ok(v) => v,
        Err(e) => {
            send_prometheus_error(w, req, &e);
            return;
        }
    };

    let rows: Arc<Mutex<Vec<StatsRow>>> = Arc::new(Mutex::new(Vec::new()));
    let rows_cl = Arc::clone(&rows);

    let timestamp = ca.q.get_timestamp();
    let write_fn: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
        let rows_count = db.rows_count();

        let columns = db.get_columns(false);
        for i in 0..rows_count {
            let mut labels: Vec<Field> = Vec::with_capacity(label_fields.len());
            for c in columns {
                if label_fields.iter().any(|lf| lf == &c.name) {
                    labels.push(Field {
                        name: c.name.clone(),
                        value: c.values[i].clone(),
                    });
                }
            }

            for c in columns {
                if label_fields.iter().any(|lf| lf == &c.name) {
                    continue;
                }

                let v = c.values[i].clone();
                // Histogram values are ASCII JSON; a value with invalid UTF-8
                // simply is not a histogram (matches Go's semantics).
                if let Some(vs) = std::str::from_utf8(&v)
                    .ok()
                    .filter(|s| is_histogram_value(s))
                {
                    // Special case - the value is the result of histogram()
                    // stats function. Convert it to values for individual
                    // buckets.
                    if let Ok(buckets) = parse_histogram_buckets(vs) {
                        let mut name = c.name.clone();
                        name.extend_from_slice(b"_bucket");
                        let mut bucket_rows: Vec<StatsRow> = Vec::with_capacity(buckets.len());
                        for bucket in buckets {
                            let mut bucket_labels = labels.clone();
                            bucket_labels.push(Field {
                                name: b"vmrange".to_vec(),
                                value: bucket.vmrange.into_bytes(),
                            });
                            bucket_rows.push(StatsRow {
                                name: name.clone(),
                                labels: bucket_labels,
                                timestamp,
                                value: bucket.hits.to_string().into_bytes(),
                            });
                        }
                        rows_cl.lock().unwrap().extend(bucket_rows);

                        continue;
                    }
                }

                let r = StatsRow {
                    name: c.name.clone(),
                    labels: labels.clone(),
                    timestamp,
                    value: v,
                };
                rows_cl.lock().unwrap().push(r);
            }
        }
    });

    // Execute the query, canceling on client disconnect (Go: request ctx).
    let cancel = w.watch_disconnect();
    if let Err(e) = storage.run_query_with_stats(
        &ca.tenant_ids,
        &ca.q,
        &ca.hidden_fields_filters,
        write_fn,
        cancel.as_deref(),
        &ca.qs,
    ) {
        if is_query_canceled_error(&e) {
            // The client disconnected: there is nobody to respond to.
            return;
        }
        send_prometheus_error(w, req, &format!("cannot execute query [{}]: {e}", ca.q));
        return;
    }

    let rows = match Arc::try_unwrap(rows) {
        Ok(m) => m.into_inner().unwrap(),
        // All query workers have joined, so take the rows instead of cloning.
        Err(arc) => std::mem::take(&mut *arc.lock().unwrap()),
    };

    // Write response headers
    w.set_header("Content-Type", "application/json");
    ca.write_response_headers(w, start_time);

    // Write response
    let mut body = Vec::new();
    write_stats_query_response(&mut body, &rows);
    w.write_bytes(&body);
}

/// Handles `/select/logsql/stats_query_range`
/// (Go `ProcessStatsQueryRangeRequest`).
///
/// See <https://docs.victoriametrics.com/victorialogs/querying/#querying-log-range-stats>
pub fn process_stats_query_range_request(
    storage: &Arc<Storage>,
    req: &Request,
    w: &mut ResponseWriter,
) {
    let start_time = Instant::now();

    let mut ca = match parse_common_args(req) {
        Ok(ca) => ca,
        Err(e) => {
            send_prometheus_error(w, req, &e);
            return;
        }
    };

    // Obtain step
    let step = match parse_duration(req, "step", "") {
        Ok(v) => v,
        Err(e) => {
            send_prometheus_error(w, req, &e);
            return;
        }
    };
    if step <= 0 {
        send_prometheus_error(w, req, "'step' must be bigger than zero");
        return;
    }

    // Obtain offset
    let offset = match parse_duration(req, "offset", "0s") {
        Ok(v) => v,
        Err(e) => {
            send_prometheus_error(w, req, &e);
            return;
        }
    };

    let label_fields = match ca.q.get_stats_labels_add_grouping_by_time(step, offset) {
        Ok(v) => v,
        Err(e) => {
            send_prometheus_error(w, req, &e);
            return;
        }
    };

    // The map is keyed by Go's `MarshalUint32(columnIdx) + name +
    // MarshalFieldsToJSON(labels)` byte key; sorting the keys reproduces Go's
    // final `rows[i].key < rows[j].key` order.
    let m: Arc<Mutex<HashMap<Vec<u8>, StatsSeries>>> = Arc::new(Mutex::new(HashMap::new()));
    let m_cl = Arc::clone(&m);

    // Go re-reads q.GetTimestamp() inside writeBlock for every row; the value
    // is constant for the query, so it is captured once here.
    let q_timestamp = ca.q.get_timestamp();
    let write_fn: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
        let rows_count = db.rows_count();

        let columns = db.get_columns(false);
        for i in 0..rows_count {
            // ts must be initialized to the query timestamp for every
            // processed log row.
            // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8312
            let mut ts = q_timestamp;
            let mut labels: Vec<Field> = Vec::with_capacity(label_fields.len());
            for c in columns {
                if c.name == b"_time" {
                    // R3: invalid UTF-8 fails the timestamp parse, exactly
                    // like Go's parse on arbitrary bytes.
                    if let Some(nsec) = std::str::from_utf8(&c.values[i])
                        .ok()
                        .and_then(try_parse_timestamp_rfc3339_nano)
                    {
                        ts = nsec;
                        continue;
                    }
                }
                if label_fields.iter().any(|lf| lf == &c.name) {
                    labels.push(Field {
                        name: c.name.clone(),
                        value: c.values[i].clone(),
                    });
                }
            }

            let mut column_idx: u32 = 0;
            for c in columns {
                if label_fields.iter().any(|lf| lf == &c.name) {
                    continue;
                }

                let v = c.values[i].clone();
                // Histogram values are ASCII JSON; a value with invalid UTF-8
                // simply is not a histogram (matches Go's semantics).
                if let Some(vs) = std::str::from_utf8(&v)
                    .ok()
                    .filter(|s| is_histogram_value(s))
                {
                    // Special case - the value is the result of histogram()
                    // stats function. Convert it to values for individual
                    // buckets.
                    if let Ok(buckets) = parse_histogram_buckets(vs) {
                        let mut name = c.name.clone();
                        name.extend_from_slice(b"_bucket");
                        for bucket in buckets {
                            let mut bucket_labels = labels.clone();
                            bucket_labels.push(Field {
                                name: b"vmrange".to_vec(),
                                value: bucket.vmrange.into_bytes(),
                            });
                            let p = StatsPoint {
                                timestamp: ts,
                                value: bucket.hits.to_string().into_bytes(),
                            };
                            add_point(&m_cl, &name, column_idx, bucket_labels, p);
                        }
                        column_idx += 1;

                        continue;
                    }
                }

                let p = StatsPoint {
                    timestamp: ts,
                    value: v,
                };
                add_point(&m_cl, &c.name, column_idx, labels.clone(), p);
                column_idx += 1;
            }
        }
    });

    // Execute the request, canceling on client disconnect (Go: request ctx).
    let cancel = w.watch_disconnect();
    if let Err(e) = storage.run_query_with_stats(
        &ca.tenant_ids,
        &ca.q,
        &ca.hidden_fields_filters,
        write_fn,
        cancel.as_deref(),
        &ca.qs,
    ) {
        if is_query_canceled_error(&e) {
            // The client disconnected: there is nobody to respond to.
            return;
        }
        send_prometheus_error(w, req, &format!("cannot execute query [{}]: {e}", ca.q));
        return;
    }

    let m = match Arc::try_unwrap(m) {
        Ok(m) => m.into_inner().unwrap(),
        // All query workers have joined, so take the map instead of cloning.
        Err(arc) => std::mem::take(&mut *arc.lock().unwrap()),
    };

    // Sort the collected stats by key and their points by _time.
    let mut rows: Vec<(Vec<u8>, StatsSeries)> = m.into_iter().collect();
    for (_, ss) in rows.iter_mut() {
        ss.points.sort_by_key(|p| p.timestamp);
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    // Write response headers
    w.set_header("Content-Type", "application/json");
    ca.write_response_headers(w, start_time);

    // Write response
    let mut body = Vec::new();
    write_stats_query_range_response(&mut body, &rows);
    w.write_bytes(&body);
}

/// Go's `addPoint` closure: appends `p` to the series keyed by
/// `MarshalUint32(columnIdx) + name + MarshalFieldsToJSON(labels)`.
fn add_point(
    m: &Mutex<HashMap<Vec<u8>, StatsSeries>>,
    name: &[u8],
    column_idx: u32,
    labels: Vec<Field>,
    p: StatsPoint,
) {
    let mut key = column_idx.to_be_bytes().to_vec();
    key.extend_from_slice(name);
    marshal_fields_to_json(&mut key, &labels);

    let mut m = m.lock().unwrap();
    let ss = m.entry(key).or_insert_with(|| StatsSeries {
        name: name.to_vec(),
        labels,
        points: Vec::new(),
    });
    ss.points.push(p);
}

/// Formats a float the way quicktemplate `N().F` does
/// (`strconv.AppendFloat(dst, f, 'f', -1, 64)`): shortest round-trip decimal,
/// never scientific notation (Rust's `Display` for `f64` matches).
fn format_timestamp_seconds(timestamp: i64) -> String {
    format!("{}", timestamp as f64 / 1e9)
}

/// Go `streamformatStatsRow` metric object:
/// `{"__name__":<name>[,<label>:<value>...]}`.
fn write_metric_object(dst: &mut Vec<u8>, name: &[u8], labels: &[Field]) {
    dst.extend_from_slice(b"{\"__name__\":");
    append_json_string(dst, name);
    for label in labels {
        dst.push(b',');
        append_json_string(dst, &label.name);
        dst.push(b':');
        append_json_string(dst, &label.value);
    }
    dst.push(b'}');
}

/// Go `WriteStatsQueryResponse` (stats_query_response.qtpl):
/// `{"status":"success","data":{"resultType":"vector","result":[...]}}`.
fn write_stats_query_response(dst: &mut Vec<u8>, rows: &[StatsRow]) {
    dst.extend_from_slice(br#"{"status":"success","data":{"resultType":"vector","result":["#);
    for (i, r) in rows.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        dst.extend_from_slice(b"{\"metric\":");
        write_metric_object(dst, &r.name, &r.labels);
        dst.extend_from_slice(b",\"value\":[");
        dst.extend_from_slice(format_timestamp_seconds(r.timestamp).as_bytes());
        dst.push(b',');
        append_json_string(dst, &r.value);
        dst.extend_from_slice(b"]}");
    }
    dst.extend_from_slice(b"]}}");
}

/// Go `WriteStatsQueryRangeResponse` (stats_query_range_response.qtpl):
/// `{"status":"success","data":{"resultType":"matrix","result":[...]}}`.
fn write_stats_query_range_response(dst: &mut Vec<u8>, rows: &[(Vec<u8>, StatsSeries)]) {
    dst.extend_from_slice(br#"{"status":"success","data":{"resultType":"matrix","result":["#);
    for (i, (_, ss)) in rows.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        dst.extend_from_slice(b"{\"metric\":");
        write_metric_object(dst, &ss.name, &ss.labels);
        dst.extend_from_slice(b",\"values\":[");
        for (j, p) in ss.points.iter().enumerate() {
            if j > 0 {
                dst.push(b',');
            }
            dst.push(b'[');
            dst.extend_from_slice(format_timestamp_seconds(p.timestamp).as_bytes());
            dst.push(b',');
            append_json_string(dst, &p.value);
            dst.push(b']');
        }
        dst.extend_from_slice(b"]}");
    }
    dst.extend_from_slice(b"]}}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::logsql::test_support::{encode, http_get, open_storage_with_rows, unique_nsec};
    use esl_common::httpserver::serve;

    /// Round-trip test against a temp Storage: ingests rows and queries
    /// /select/logsql/stats_query and /select/logsql/stats_query_range through
    /// a real HTTP server.
    #[test]
    fn test_process_stats_query_requests_roundtrip() {
        let base = unique_nsec();
        let rows = [
            ("connection error occurred", "node-1"),
            ("all systems nominal", "node-1"),
            ("disk error on node 3", "node-2"),
            ("request completed ok", "node-2"),
            ("cache warmed", "node-2"),
        ];
        let (storage, path) = open_storage_with_rows("stats-query", base, &rows);

        let storage_h = Arc::clone(&storage);
        let handle = serve("127.0.0.1:0", move |req, w| match req.path() {
            "/select/logsql/stats_query" => process_stats_query_request(&storage_h, req, w),
            "/select/logsql/stats_query_range" => {
                process_stats_query_range_request(&storage_h, req, w)
            }
            _ => w.errorf(req, "unexpected path"),
        })
        .expect("serve");
        let addr = handle.local_addr();

        // Instant stats: a single vector sample {__name__="rows"} = "5".
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/stats_query?query={}",
                encode("* | stats count() rows")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert!(
            body.starts_with(r#"{"status":"success","data":{"resultType":"vector","result":["#),
            "body={body}"
        );
        assert!(body.contains(r#""__name__":"rows""#), "body={body}");
        assert!(body.contains(r#","5"]"#), "body={body}");

        // Grouped stats: per-host label with counts 2 and 3.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/stats_query?query={}",
                encode("* | stats by (host) count() rows")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert!(
            body.contains(r#""__name__":"rows","host":"node-1""#),
            "body={body}"
        );
        assert!(
            body.contains(r#""__name__":"rows","host":"node-2""#),
            "body={body}"
        );
        assert!(body.contains(r#","2"]"#), "body={body}");
        assert!(body.contains(r#","3"]"#), "body={body}");

        // A query without a stats pipe is a Prometheus-format 422 error.
        let (status, body) = http_get(
            addr,
            &format!("/select/logsql/stats_query?query={}", encode("*")),
        );
        assert_eq!(status, 422, "body={body}");
        assert!(
            body.starts_with(r#"{"status":"error","errorType":"422","error":"#),
            "body={body}"
        );
        assert!(body.contains("missing"), "body={body}");

        // Range stats: all 5 rows land in one step bucket (step=1000d);
        // matrix result with a single ["ts","5"] point.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/stats_query_range?query={}&step={}",
                encode("* | stats count() rows"),
                encode("1000d")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert!(
            body.starts_with(r#"{"status":"success","data":{"resultType":"matrix","result":["#),
            "body={body}"
        );
        assert!(body.contains(r#""__name__":"rows""#), "body={body}");
        assert!(body.contains(r#","5"]"#), "body={body}");

        // Missing step on the range endpoint is a Prometheus-format error.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/stats_query_range?query={}",
                encode("* | stats count() rows")
            ),
        );
        assert_eq!(status, 422, "body={body}");
        assert!(
            body.contains("cannot parse duration from the arg 'step='"),
            "body={body}"
        );

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_parse_histogram_buckets() {
        let buckets = parse_histogram_buckets(
            r#"[{"vmrange":"1...10","hits":5},{"vmrange":"10...100","hits":7}]"#,
        )
        .expect("parse");
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].vmrange, "1...10");
        assert_eq!(buckets[0].hits, 5);
        assert_eq!(buckets[1].vmrange, "10...100");
        assert_eq!(buckets[1].hits, 7);

        assert!(parse_histogram_buckets("[]").expect("empty").is_empty());
        assert!(parse_histogram_buckets("[1]").is_err());
        assert!(parse_histogram_buckets(r#"[{"vmrange":1}]"#).is_err());
    }

    #[test]
    fn test_write_stats_query_response_shape() {
        let rows = vec![StatsRow {
            name: b"rows".to_vec(),
            labels: vec![Field {
                name: b"host".to_vec(),
                value: b"node-1".to_vec(),
            }],
            timestamp: 1_500_000_000_000_000_000,
            value: b"5".to_vec(),
        }];
        let mut dst = Vec::new();
        write_stats_query_response(&mut dst, &rows);
        assert_eq!(
            String::from_utf8_lossy(&dst),
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"rows","host":"node-1"},"value":[1500000000,"5"]}]}}"#
        );
    }
}

//! Port of `/select/logsql/facets` — Go `ProcessFacetsRequest` in
//! `app/eslselect/logsql/logsql.go` plus the `facets_response.qtpl` response
//! writers (hand-rolled JSON, matching the house style in `logsql.rs`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use esl_common::httpserver::{Request, ResponseWriter};
use esl_logstorage::parser::{ParseQueryAtTimestamp, Query};
use esl_logstorage::storage::Storage;
use esl_logstorage::storage_search::{DataBlock, WriteDataBlockFn, is_query_canceled_error};

use crate::logsql::{append_json_string, get_bool, get_positive_int, parse_common_args};

/// Go `facetEntry`.
struct FacetEntry {
    value: String,
    hits: String,
}

/// Handles `/select/logsql/facets` (Go `ProcessFacetsRequest`).
///
/// See <https://docs.victoriametrics.com/victorialogs/querying/#querying-facets>
pub fn process_facets_request(storage: &Arc<Storage>, req: &Request, w: &mut ResponseWriter) {
    let start_time = Instant::now();

    let mut ca = match parse_common_args(req) {
        Ok(ca) => ca,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    let limit = match get_positive_int(req, "limit") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };
    let max_values_per_field = match get_positive_int(req, "max_values_per_field") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };
    let max_value_len = match get_positive_int(req, "max_value_len") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };
    let keep_const_fields = get_bool(req, "keep_const_fields");

    ca.q = add_facets_pipe(
        &ca.q,
        limit,
        max_values_per_field,
        max_value_len,
        keep_const_fields,
    );

    let m: Arc<Mutex<HashMap<String, Vec<FacetEntry>>>> = Arc::new(Mutex::new(HashMap::new()));
    let m_cl = Arc::clone(&m);
    let write_fn: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
        let rows_count = db.rows_count();
        if rows_count == 0 {
            return;
        }

        let columns = db.get_columns(false);
        if columns.len() != 3 {
            esl_common::panicf!("BUG: expecting 3 columns; got {} columns", columns.len());
            unreachable!()
        }

        let field_names = &columns[0].values;
        let field_values = &columns[1].values;
        let hits = &columns[2].values;

        for i in 0..field_names.len() {
            let field_name = String::from_utf8_lossy(&field_names[i]).into_owned();
            let field_value = String::from_utf8_lossy(&field_values[i]).into_owned();
            let hits_str = String::from_utf8_lossy(&hits[i]).into_owned();

            let mut m = m_cl.lock().unwrap();
            m.entry(field_name).or_default().push(FacetEntry {
                value: field_value,
                hits: hits_str,
            });
        }
    });

    // Execute the query, canceling on client disconnect (Go: request ctx).
    let cancel = w.watch_disconnect();
    if let Err(e) =
        storage.run_query_with_stats(&ca.tenant_ids, &ca.q, write_fn, cancel.as_deref(), &ca.qs)
    {
        if is_query_canceled_error(&e) {
            // The client disconnected: there is nobody to respond to.
            return;
        }
        w.errorf(req, &format!("cannot execute query [{}]: {e}", ca.q));
        return;
    }

    let m = match Arc::try_unwrap(m) {
        Ok(m) => m.into_inner().unwrap(),
        // All query workers have joined, so take the map instead of cloning.
        Err(arc) => std::mem::take(&mut *arc.lock().unwrap()),
    };

    // Write response header
    w.set_header("Content-Type", "application/json");
    ca.write_response_headers(w, start_time);

    // Write response
    let mut body = Vec::new();
    write_facets_response(&mut body, m);
    w.write_bytes(&body);
}

/// Go `Query.AddFacetsPipe`: appends
/// `| facets [N] [max_values_per_field N] [max_value_len N] [keep_const_fields]`
/// to the query.
///
/// PORT NOTE: Go composes the pipe string and calls `q.mustAppendPipe(s)`;
/// `must_append_pipe` is crate-private in esl-logstorage, so the port composes
/// the same pipe string textually onto the query's `Display` round-trip and
/// re-parses (the parser `Display` parity is the ported spec). A failure here
/// is a round-trip bug, so it panics like Go `mustAppendPipe`. Caveat shared
/// with the engine's own `Query::clone` (which re-parses `Display` the same
/// way): `FilterAnd::to_string` does not parenthesize or-children (documented
/// filter_and.rs PORT NOTE), so `a (b or c)` filters lose their grouping on
/// re-parse; fixing that engine gap fixes this path too.
fn add_facets_pipe(
    q: &Query,
    limit: i64,
    max_values_per_field: i64,
    max_value_len: i64,
    keep_const_fields: bool,
) -> Query {
    let mut s = "facets".to_string();
    if limit > 0 {
        s += &format!(" {limit}");
    }
    if max_values_per_field > 0 {
        s += &format!(" max_values_per_field {max_values_per_field}");
    }
    if max_value_len > 0 {
        s += &format!(" max_value_len {max_value_len}");
    }
    if keep_const_fields {
        s += " keep_const_fields";
    }

    let composed = format!("{q} | {s}");
    match ParseQueryAtTimestamp(&composed, q.get_timestamp()) {
        Ok(q) => q,
        Err(e) => {
            esl_common::panicf!("BUG: cannot re-parse query with facets pipe [{composed}]: {e}");
            unreachable!()
        }
    }
}

/// PORT NOTE: Go sorts facet entries with `stringsutil.LessNatural` on the hits
/// strings; hits always come from the facets pipe counters (plain decimal
/// uint64), for which natural order equals numeric order, so the port compares
/// parsed u64 values (falling back to byte order for non-numeric strings).
fn hits_less_natural(a: &str, b: &str) -> bool {
    match (a.parse::<u64>(), b.parse::<u64>()) {
        (Ok(x), Ok(y)) => x < y,
        _ => a < b,
    }
}

/// Go `WriteFacetsResponse` (facets_response.qtpl):
/// `{"facets":[{"field_name":...,"values":[{"field_value":...,"hits":N},...]},...]}`
/// with field names sorted, and per-field entries sorted by hits desc then
/// value asc.
fn write_facets_response(dst: &mut Vec<u8>, mut m: HashMap<String, Vec<FacetEntry>>) {
    let mut sorted_keys: Vec<String> = m.keys().cloned().collect();
    sorted_keys.sort();

    dst.extend_from_slice(b"{\"facets\":[");
    for (i, k) in sorted_keys.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        let fes = m.get_mut(k).expect("BUG: key must be present");
        write_facets_line(dst, k, fes);
    }
    dst.extend_from_slice(b"]}");
}

/// Go `facetsLine` (facets_response.qtpl).
fn write_facets_line(dst: &mut Vec<u8>, k: &str, fes: &mut [FacetEntry]) {
    fes.sort_by(|a, b| {
        if a.hits == b.hits {
            a.value.cmp(&b.value)
        } else if hits_less_natural(&b.hits, &a.hits) {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        }
    });

    dst.extend_from_slice(b"{\"field_name\":");
    append_json_string(dst, k.as_bytes());
    dst.extend_from_slice(b",\"values\":[");
    for (i, fe) in fes.iter().enumerate() {
        if i > 0 {
            dst.push(b',');
        }
        dst.extend_from_slice(b"{\"field_value\":");
        append_json_string(dst, fe.value.as_bytes());
        dst.extend_from_slice(b",\"hits\":");
        dst.extend_from_slice(fe.hits.as_bytes());
        dst.push(b'}');
    }
    dst.extend_from_slice(b"]}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::logsql::test_support::{encode, http_get, open_storage_with_rows, unique_nsec};
    use esl_common::httpserver::serve;

    /// Round-trip test against a temp Storage: ingests rows and queries
    /// /select/logsql/facets through a real HTTP server.
    #[test]
    fn test_process_facets_request_roundtrip() {
        let base = unique_nsec();
        let rows = [
            ("connection error occurred", "node-1"),
            ("all systems nominal", "node-1"),
            ("disk error on node 3", "node-2"),
            ("request completed ok", "node-2"),
            ("cache warmed", "node-2"),
        ];
        let (storage, path) = open_storage_with_rows("facets", base, &rows);

        let storage_h = Arc::clone(&storage);
        let handle = serve("127.0.0.1:0", move |req, w| match req.path() {
            "/select/logsql/facets" => process_facets_request(&storage_h, req, w),
            _ => w.errorf(req, "unexpected path"),
        })
        .expect("serve");
        let addr = handle.local_addr();

        let (status, body) = http_get(
            addr,
            &format!("/select/logsql/facets?query={}", encode("*")),
        );
        assert_eq!(status, 200, "body={body}");
        assert!(body.starts_with("{\"facets\":["), "body={body}");
        // host facets: node-2 (3 hits) sorts before node-1 (2 hits).
        assert!(
            body.contains(
                "{\"field_name\":\"host\",\"values\":[\
                 {\"field_value\":\"node-2\",\"hits\":3},\
                 {\"field_value\":\"node-1\",\"hits\":2}]}"
            ),
            "body={body}"
        );

        // max_values_per_field=1 drops the host facet (2 unique values > 1).
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/facets?query={}&max_values_per_field=1",
                encode("*")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert!(!body.contains("\"field_name\":\"host\""), "body={body}");

        // Missing query arg is an error.
        let (status, body) = http_get(addr, "/select/logsql/facets");
        assert_eq!(status, 400, "body={body}");
        assert!(body.contains("`query` arg cannot be empty"), "body={body}");

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_write_facets_response_sorting() {
        let mut m: HashMap<String, Vec<FacetEntry>> = HashMap::new();
        m.insert(
            "b".to_string(),
            vec![
                FacetEntry {
                    value: "y".to_string(),
                    hits: "2".to_string(),
                },
                FacetEntry {
                    value: "x".to_string(),
                    hits: "10".to_string(),
                },
                FacetEntry {
                    value: "w".to_string(),
                    hits: "2".to_string(),
                },
            ],
        );
        m.insert(
            "a".to_string(),
            vec![FacetEntry {
                value: "v".to_string(),
                hits: "1".to_string(),
            }],
        );
        let mut dst = Vec::new();
        write_facets_response(&mut dst, m);
        // Field names sorted; entries sorted by hits desc (numeric), ties by
        // value asc.
        assert_eq!(
            String::from_utf8_lossy(&dst),
            "{\"facets\":[\
             {\"field_name\":\"a\",\"values\":[{\"field_value\":\"v\",\"hits\":1}]},\
             {\"field_name\":\"b\",\"values\":[\
             {\"field_value\":\"x\",\"hits\":10},\
             {\"field_value\":\"w\",\"hits\":2},\
             {\"field_value\":\"y\",\"hits\":2}]}]}"
        );
    }
}

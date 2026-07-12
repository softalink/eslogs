//! Port of EsLogs `app/eslselect`: LogsQL query endpoints and UI.
//!
//! Router mirrors `app/eslselect/main.go` (`RequestHandler`/`selectHandler`) and
//! `app/eslselect/logsql/logsql.go`. The benchmark query endpoint
//! `/select/logsql/query` is fully ported; the other logsql endpoints are
//! skeletons (see [`logsql`]). The embedded esmui web UI is out of scope and is
//! served as a minimal placeholder.

use std::sync::Arc;

use esl_common::buildinfo;
use esl_common::httpserver::{Request, ResponseWriter};
use esl_logstorage::storage::Storage;

pub mod csv;
pub mod esmui_assets;
pub mod internalselect;
mod logsql;
pub mod logsql_facets;
pub mod logsql_fields;
pub mod logsql_hits;
pub mod logsql_stats_query;
pub mod logsql_streams;
pub mod logsql_tail;

/// Handles `/select/*` requests for EsLogs.
///
/// Returns `true` if `req.path()` was a `/select/*` route this handler served,
/// and `false` otherwise (so the caller can try other handlers or return 404).
/// Mirrors Go `eslselect.RequestHandler` restricted to the `/select/*` prefix
/// (the `/delete/*` and `/internal/*` prefixes are out of scope for this port).
pub fn request_handler(storage: &Arc<Storage>, req: &mut Request, w: &mut ResponseWriter) -> bool {
    // Go normalizes duplicate slashes: strings.ReplaceAll(path, "//", "/").
    let path = req.path().replace("//", "/");

    if !path.starts_with("/select/") {
        return false;
    }

    if path == "/select/buildinfo" {
        process_buildinfo(req, w);
        return true;
    }
    if esmui_assets::request_handler(req, w) {
        return true;
    }

    match path.as_str() {
        "/select/logsql/query" => {
            logsql::process_query_request(storage, req, w);
            true
        }
        "/select/logsql/query_time_range" => {
            logsql::process_query_time_range_request(req, w);
            true
        }
        "/select/logsql/hits" => {
            logsql::process_hits_request(storage, req, w);
            true
        }
        "/select/logsql/facets" => {
            logsql_facets::process_facets_request(storage, req, w);
            true
        }
        "/select/logsql/stats_query" => {
            logsql_stats_query::process_stats_query_request(storage, req, w);
            true
        }
        "/select/logsql/stats_query_range" => {
            logsql_stats_query::process_stats_query_range_request(storage, req, w);
            true
        }
        "/select/logsql/field_names" => {
            logsql_fields::process_field_names_request(storage, req, w);
            true
        }
        "/select/logsql/field_values" => {
            logsql_fields::process_field_values_request(storage, req, w);
            true
        }
        "/select/logsql/streams" => {
            logsql_streams::process_streams_request(storage, req, w);
            true
        }
        "/select/logsql/stream_ids" => {
            logsql_streams::process_stream_ids_request(storage, req, w);
            true
        }
        "/select/logsql/stream_field_names" => {
            logsql_streams::process_stream_field_names_request(storage, req, w);
            true
        }
        "/select/logsql/stream_field_values" => {
            logsql_streams::process_stream_field_values_request(storage, req, w);
            true
        }
        "/select/logsql/tail" => {
            logsql_tail::process_live_tail_request(storage, req, w);
            true
        }
        // Unknown /select/* subpath: mirror Go processSelectRequest's default,
        // which returns false so the request is treated as unhandled (404).
        _ => false,
    }
}

/// Handles `/select/buildinfo` (Go `selectHandler` buildinfo branch).
fn process_buildinfo(req: &Request, w: &mut ResponseWriter) {
    if req.method() != "GET" {
        w.set_status(405);
        w.set_header("Content-Type", "application/json");
        w.write_str(&format!(
            r#"{{"status":"error","msg":"method {:?} isn't allowed"}}"#,
            req.method()
        ));
        return;
    }
    let v = {
        let short = buildinfo::short_version();
        if short.is_empty() {
            buildinfo::version().to_string()
        } else {
            short
        }
    };
    w.set_header("Content-Type", "application/json");
    w.write_str(&format!(
        r#"{{"status":"success","data":{{"version":{v:?}}}}}"#
    ));
}

/// Serves a minimal placeholder for `/select/esmui[/...]`.
///
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::path::PathBuf;

    use esl_common::httpserver::serve;
    use esl_logstorage::log_rows::get_log_rows;
    use esl_logstorage::rows::Field;
    use esl_logstorage::storage::StorageConfig;
    use esl_logstorage::tenant_id::TenantID;

    fn unique_nsec() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }

    /// Opens a temp Storage, ingests `msgs` as rows (each with `_msg` and a
    /// `host` stream field), and flushes.
    fn open_storage_with_rows(name: &str, msgs: &[&str]) -> (Arc<Storage>, PathBuf) {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "esl-select-{name}-{}-{}",
            std::process::id(),
            unique_nsec()
        ));
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);

        let tenant = TenantID::default();
        let mut lr = get_log_rows(&["host"], &[], &[], &[], "");
        let base = unique_nsec();
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
        (s, path)
    }

    /// Performs a raw HTTP/1.1 GET and returns (status_code, body).
    fn http_get(addr: SocketAddr, target: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).expect("connect");
        write!(
            stream,
            "GET {target} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"
        )
        .expect("write request");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("read response");
        let text = String::from_utf8_lossy(&raw);
        let idx = text.find("\r\n\r\n").expect("headers/body separator");
        let head = &text[..idx];
        let body = text[idx + 4..].to_string();
        let status: u16 = head
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .expect("status code");
        (status, body)
    }

    /// Minimal percent-encoding for query strings (space and pipe).
    fn encode(q: &str) -> String {
        q.replace(' ', "%20").replace('|', "%7C")
    }

    fn run_query(addr: SocketAddr, query: &str) -> (u16, String) {
        http_get(
            addr,
            &format!("/select/logsql/query?query={}", encode(query)),
        )
    }

    #[test]
    fn test_process_query_request_ndjson() {
        let msgs = [
            "connection error occurred",
            "all systems nominal",
            "disk error on node 3",
            "request completed ok",
            "cache warmed",
        ];
        let (storage, path) = open_storage_with_rows("query", &msgs);

        let storage_h = Arc::clone(&storage);
        let handle = serve("127.0.0.1:0", move |req, w| {
            request_handler(&storage_h, req, w);
        })
        .expect("serve");
        let addr = handle.local_addr();

        // `*` returns all 5 rows as ndjson objects, each carrying _msg + host.
        let (status, body) = run_query(addr, "*");
        assert_eq!(status, 200, "`*` status; body={body}");
        let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 5, "`*` must return 5 ndjson rows; body={body}");
        for line in &lines {
            assert!(
                line.starts_with('{') && line.ends_with('}'),
                "row is a JSON object: {line}"
            );
            assert!(line.contains("\"_msg\""), "row has _msg: {line}");
            assert!(line.contains("\"host\":\"node-1\""), "row has host: {line}");
        }

        // `error` matches exactly the two rows containing the "error" token.
        let (status, body) = run_query(addr, "error");
        assert_eq!(status, 200);
        let n = body.lines().filter(|l| !l.is_empty()).count();
        assert_eq!(n, 2, "`error` must return 2 rows; body={body}");
        assert!(body.contains("connection error occurred"));
        assert!(body.contains("disk error on node 3"));

        // `* | stats count() rows` returns a single row {"rows":"5"}.
        let (status, body) = run_query(addr, "* | stats count() rows");
        assert_eq!(status, 200);
        let lines: Vec<&str> = body.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 1, "stats must return one row; body={body}");
        assert!(
            lines[0].contains("\"rows\":\"5\""),
            "stats count() == 5: {}",
            lines[0]
        );

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_query_empty_arg_is_400() {
        let (storage, path) = open_storage_with_rows("empty", &["x"]);
        let storage_h = Arc::clone(&storage);
        let handle = serve("127.0.0.1:0", move |req, w| {
            request_handler(&storage_h, req, w);
        })
        .expect("serve");
        let addr = handle.local_addr();

        let (status, body) = http_get(addr, "/select/logsql/query");
        assert_eq!(status, 400, "missing query arg must be 400; body={body}");
        assert!(body.contains("`query` arg cannot be empty"), "body={body}");

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }
}

//! Port of the `/select/logsql/field_names` and `/select/logsql/field_values`
//! handlers from `app/eslselect/logsql/logsql.go`
//! (`ProcessFieldNamesRequest` / `ProcessFieldValuesRequest`) plus
//! `WriteValuesWithHitsJSON` from `logsql.qtpl.go`.
//!
//! The shared arg parsing (`parseCommonArgs` & friends) lives in
//! [`crate::logsql`]; the `ValuesWithHitsJSON` writer here is shared with the
//! stream enumeration endpoints in [`crate::logsql_streams`].

use std::sync::Arc;
use std::time::Instant;

use esl_common::httpserver::{Request, ResponseWriter};
use esl_logstorage::storage::Storage;
use esl_logstorage::storage_search::{ValueWithHits, is_query_canceled_error};

use crate::logsql::{append_json_string, get_positive_int, parse_common_args};

/// Builds the Go `ValuesWithHitsJSON` payload (logsql.qtpl.go):
/// `{"values":[{"value":"...","hits":N},...]}`.
fn values_with_hits_json(values: &[ValueWithHits]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"{\"values\":[");
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        buf.extend_from_slice(b"{\"value\":");
        append_json_string(&mut buf, v.value.as_bytes());
        buf.extend_from_slice(b",\"hits\":");
        buf.extend_from_slice(v.hits.to_string().as_bytes());
        buf.push(b'}');
    }
    buf.extend_from_slice(b"]}");
    buf
}

/// Port of Go `WriteValuesWithHitsJSON` (logsql.qtpl.go).
pub(crate) fn write_values_with_hits_json(w: &mut ResponseWriter, values: &[ValueWithHits]) {
    w.write_bytes(&values_with_hits_json(values));
}

/// Handles `/select/logsql/field_names` (Go `ProcessFieldNamesRequest`).
///
/// See <https://docs.victoriametrics.com/victorialogs/querying/#querying-field-names>
pub fn process_field_names_request(storage: &Arc<Storage>, req: &Request, w: &mut ResponseWriter) {
    let ca = match parse_common_args(req) {
        Ok(ca) => ca,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Filter is used for filtering the returned field names by the given
    // filter substring
    let filter = req.form_value("filter");

    // Obtain field names for the given query, canceling on client disconnect
    // (Go: the request context).
    let start_time = Instant::now();
    let cancel = w.watch_disconnect();
    let field_names = match storage.get_field_names(
        &ca.tenant_ids,
        &ca.q,
        &ca.hidden_fields_filters,
        filter,
        cancel.as_deref(),
        &ca.qs,
    ) {
        Ok(v) => v,
        Err(e) => {
            if is_query_canceled_error(&e) {
                // The client disconnected: there is nobody to respond to.
                return;
            }
            w.errorf(
                req,
                &format!("cannot obtain field names with filter={filter:?}: {e}"),
            );
            return;
        }
    };

    // Write response headers
    w.set_header("Content-Type", "application/json");
    ca.write_response_headers(w, start_time);

    // Write results
    write_values_with_hits_json(w, &field_names);
}

/// Handles `/select/logsql/field_values` (Go `ProcessFieldValuesRequest`).
///
/// See <https://docs.victoriametrics.com/victorialogs/querying/#querying-field-values>
pub fn process_field_values_request(storage: &Arc<Storage>, req: &Request, w: &mut ResponseWriter) {
    let ca = match parse_common_args(req) {
        Ok(ca) => ca,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Parse field query arg
    let field_name = req.form_value("field");
    if field_name.is_empty() {
        w.errorf(req, "missing 'field' query arg");
        return;
    }

    // Filter is used for filtering the returned field values by the given
    // filter substring
    let filter = req.form_value("filter");

    // Parse limit query arg
    let limit = match get_positive_int(req, "limit") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Obtain unique values for the given field, canceling on client
    // disconnect (Go: the request context).
    let start_time = Instant::now();
    let cancel = w.watch_disconnect();
    let values = match storage.get_field_values(
        &ca.tenant_ids,
        &ca.q,
        &ca.hidden_fields_filters,
        field_name,
        filter,
        limit as u64,
        cancel.as_deref(),
        &ca.qs,
    ) {
        Ok(v) => v,
        Err(e) => {
            if is_query_canceled_error(&e) {
                // The client disconnected: there is nobody to respond to.
                return;
            }
            w.errorf(
                req,
                &format!(
                    "cannot obtain values for field {field_name:?} with filter {filter:?}: {e}"
                ),
            );
            return;
        }
    };

    // Write response headers
    w.set_header("Content-Type", "application/json");
    ca.write_response_headers(w, start_time);

    // Write results
    write_values_with_hits_json(w, &values);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logsql::test_support::*;
    use esl_common::httpserver::serve;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};

    /// Like `test_support::http_get`, but also returns the response head so
    /// tests can assert on headers.
    pub(crate) fn http_get_with_head(addr: SocketAddr, target: &str) -> (u16, String, String) {
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
        let head = text[..idx].to_string();
        let body = text[idx + 4..].to_string();
        let status: u16 = head
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .expect("status code");
        (status, head, body)
    }

    fn rows() -> Vec<(&'static str, &'static str)> {
        vec![
            ("connection error occurred", "node-1"),
            ("all systems nominal", "node-1"),
            ("disk error on node 3", "node-1"),
            ("request completed ok", "node-2"),
            ("cache warmed", "node-2"),
        ]
    }

    #[test]
    fn test_process_field_names_and_values_requests() {
        let (storage, path) = open_storage_with_rows("fields", unique_nsec(), &rows());

        let storage_h = std::sync::Arc::clone(&storage);
        let handle = serve("127.0.0.1:0", move |req, w| match req.path() {
            "/select/logsql/field_names" => process_field_names_request(&storage_h, req, w),
            "/select/logsql/field_values" => process_field_values_request(&storage_h, req, w),
            _ => w.errorf(req, "unexpected path"),
        })
        .expect("serve");
        let addr = handle.local_addr();

        // field_names over `*` returns every field name with hits.
        let (status, head, body) = http_get_with_head(
            addr,
            &format!("/select/logsql/field_names?query={}", encode("*")),
        );
        assert_eq!(status, 200, "body={body}");
        assert!(
            head.contains("Content-Type: application/json"),
            "head={head}"
        );
        assert!(head.contains("ESL-Request-Duration-Seconds"), "head={head}");
        assert!(head.contains("AccountID: 0"), "head={head}");
        assert!(head.contains("ProjectID: 0"), "head={head}");
        assert!(body.starts_with("{\"values\":["), "body={body}");
        assert!(body.ends_with("]}"), "body={body}");
        assert!(
            body.contains("{\"value\":\"_msg\",\"hits\":5}"),
            "body={body}"
        );
        assert!(
            body.contains("{\"value\":\"host\",\"hits\":5}"),
            "body={body}"
        );

        // field_names honors the `filter` substring arg.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/field_names?query={}&filter=host",
                encode("*")
            ),
        );
        assert_eq!(status, 200);
        assert_eq!(body, "{\"values\":[{\"value\":\"host\",\"hits\":5}]}");

        // field_values for `host` returns per-value hits sorted by hits desc.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/field_values?query={}&field=host",
                encode("*")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert_eq!(
            body,
            "{\"values\":[{\"value\":\"node-1\",\"hits\":3},{\"value\":\"node-2\",\"hits\":2}]}"
        );

        // field_values narrows by the query filter.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/field_values?query={}&field=host",
                encode("error")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert_eq!(body, "{\"values\":[{\"value\":\"node-1\",\"hits\":2}]}");

        // field_values requires the `field` arg.
        let (status, body) = http_get(
            addr,
            &format!("/select/logsql/field_values?query={}", encode("*")),
        );
        assert_eq!(status, 400, "body={body}");
        assert!(body.contains("missing 'field' query arg"), "body={body}");

        // Missing query arg is a 400 from the common-args parsing.
        let (status, body) = http_get(addr, "/select/logsql/field_names");
        assert_eq!(status, 400, "body={body}");
        assert!(body.contains("`query` arg cannot be empty"), "body={body}");

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_values_with_hits_json_escaping() {
        let json = super::values_with_hits_json(&[
            ValueWithHits {
                value: "plain".to_string(),
                hits: 7,
            },
            ValueWithHits {
                value: "with \"quotes\"\nand newline".to_string(),
                hits: 1,
            },
        ]);
        assert_eq!(
            String::from_utf8(json).unwrap(),
            "{\"values\":[{\"value\":\"plain\",\"hits\":7},\
             {\"value\":\"with \\\"quotes\\\"\\nand newline\",\"hits\":1}]}"
        );

        assert_eq!(
            String::from_utf8(super::values_with_hits_json(&[])).unwrap(),
            "{\"values\":[]}"
        );
    }
}

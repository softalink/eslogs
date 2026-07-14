//! Port of the stream enumeration handlers from
//! `app/eslselect/logsql/logsql.go`: `ProcessStreamsRequest`,
//! `ProcessStreamIDsRequest`, `ProcessStreamFieldNamesRequest` and
//! `ProcessStreamFieldValuesRequest`.

use std::sync::Arc;
use std::time::Instant;

use esl_common::httpserver::{Request, ResponseWriter};
use esl_logstorage::storage::Storage;
use esl_logstorage::storage_search::is_query_canceled_error;

use crate::logsql::{get_positive_int, parse_common_args};
use crate::logsql_fields::write_values_with_hits_json;

/// Handles `/select/logsql/streams` (Go `ProcessStreamsRequest`).
///
/// See <https://docs.victoriametrics.com/victorialogs/querying/#querying-streams>
pub fn process_streams_request(storage: &Arc<Storage>, req: &Request, w: &mut ResponseWriter) {
    let ca = match parse_common_args(req) {
        Ok(ca) => ca,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Parse limit query arg
    let limit = match get_positive_int(req, "limit") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Obtain streams for the given query, canceling on client disconnect
    // (Go: the request context).
    let start_time = Instant::now();
    let cancel = w.watch_disconnect();
    let streams = match storage.get_streams(
        &ca.tenant_ids,
        &ca.q,
        &ca.hidden_fields_filters,
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
            w.errorf(req, &format!("cannot obtain streams: {e}"));
            return;
        }
    };

    // Write response headers
    w.set_header("Content-Type", "application/json");
    ca.write_response_headers(w, start_time);

    // Write results
    write_values_with_hits_json(w, &streams);
}

/// Handles `/select/logsql/stream_ids` (Go `ProcessStreamIDsRequest`).
///
/// See <https://docs.victoriametrics.com/victorialogs/querying/#querying-stream_ids>
pub fn process_stream_ids_request(storage: &Arc<Storage>, req: &Request, w: &mut ResponseWriter) {
    let ca = match parse_common_args(req) {
        Ok(ca) => ca,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Parse limit query arg
    let limit = match get_positive_int(req, "limit") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Obtain streamIDs for the given query
    let start_time = Instant::now();
    let cancel = w.watch_disconnect();
    let stream_ids = match storage.get_stream_ids(
        &ca.tenant_ids,
        &ca.q,
        &ca.hidden_fields_filters,
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
            w.errorf(req, &format!("cannot obtain stream_ids: {e}"));
            return;
        }
    };

    // Write response headers
    w.set_header("Content-Type", "application/json");
    ca.write_response_headers(w, start_time);

    // Write results
    write_values_with_hits_json(w, &stream_ids);
}

/// Handles `/select/logsql/stream_field_names`
/// (Go `ProcessStreamFieldNamesRequest`).
///
/// See <https://docs.victoriametrics.com/victorialogs/querying/#querying-stream-field-names>
pub fn process_stream_field_names_request(
    storage: &Arc<Storage>,
    req: &Request,
    w: &mut ResponseWriter,
) {
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

    // Obtain stream field names for the given query
    let start_time = Instant::now();
    let cancel = w.watch_disconnect();
    let names = match storage.get_stream_field_names(
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
                &format!("cannot obtain stream field names with filter {filter:?}: {e}"),
            );
            return;
        }
    };

    // Write response headers
    w.set_header("Content-Type", "application/json");
    ca.write_response_headers(w, start_time);

    // Write results
    write_values_with_hits_json(w, &names);
}

/// Handles `/select/logsql/stream_field_values`
/// (Go `ProcessStreamFieldValuesRequest`).
///
/// See <https://docs.victoriametrics.com/victorialogs/querying/#querying-stream-field-values>
pub fn process_stream_field_values_request(
    storage: &Arc<Storage>,
    req: &Request,
    w: &mut ResponseWriter,
) {
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

    // Obtain stream field values for the given query and the given field_name
    let start_time = Instant::now();
    let cancel = w.watch_disconnect();
    let values = match storage.get_stream_field_values(
        &ca.tenant_ids,
        &ca.q,
        &ca.hidden_fields_filters,
        field_name.as_bytes(),
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
                    "cannot obtain stream field values for field {field_name:?} with filter {filter:?}: {e}"
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

    #[test]
    fn test_process_streams_requests() {
        let rows = [
            ("connection error occurred", "node-1"),
            ("all systems nominal", "node-1"),
            ("disk error on node 3", "node-1"),
            ("request completed ok", "node-2"),
            ("cache warmed", "node-2"),
        ];
        let (storage, path) = open_storage_with_rows("streams", unique_nsec(), &rows);

        let storage_h = std::sync::Arc::clone(&storage);
        let handle = serve("127.0.0.1:0", move |req, w| match req.path() {
            "/select/logsql/streams" => process_streams_request(&storage_h, req, w),
            "/select/logsql/stream_ids" => process_stream_ids_request(&storage_h, req, w),
            "/select/logsql/stream_field_names" => {
                process_stream_field_names_request(&storage_h, req, w)
            }
            "/select/logsql/stream_field_values" => {
                process_stream_field_values_request(&storage_h, req, w)
            }
            _ => w.errorf(req, "unexpected path"),
        })
        .expect("serve");
        let addr = handle.local_addr();

        // streams: one entry per stream, sorted by hits desc.
        let (status, body) = http_get(
            addr,
            &format!("/select/logsql/streams?query={}", encode("*")),
        );
        assert_eq!(status, 200, "body={body}");
        assert_eq!(
            body,
            "{\"values\":[{\"value\":\"{host=\\\"node-1\\\"}\",\"hits\":3},\
             {\"value\":\"{host=\\\"node-2\\\"}\",\"hits\":2}]}"
        );

        // stream_ids: one opaque hex id per stream; verify the shape and the
        // per-stream hit counts.
        let (status, body) = http_get(
            addr,
            &format!("/select/logsql/stream_ids?query={}", encode("*")),
        );
        assert_eq!(status, 200, "body={body}");
        assert!(
            body.starts_with("{\"values\":[{\"value\":\""),
            "body={body}"
        );
        assert_eq!(body.matches("{\"value\":").count(), 2, "body={body}");
        assert!(body.contains("\"hits\":3"), "body={body}");
        assert!(body.contains("\"hits\":2"), "body={body}");

        // stream_field_names: `host` is the only stream field; hits accumulate
        // across streams.
        let (status, body) = http_get(
            addr,
            &format!("/select/logsql/stream_field_names?query={}", encode("*")),
        );
        assert_eq!(status, 200, "body={body}");
        assert_eq!(body, "{\"values\":[{\"value\":\"host\",\"hits\":5}]}");

        // stream_field_names honors the filter substring.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/stream_field_names?query={}&filter=nomatch",
                encode("*")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert_eq!(body, "{\"values\":[]}");

        // stream_field_values for `host`.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/stream_field_values?query={}&field=host",
                encode("*")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert_eq!(
            body,
            "{\"values\":[{\"value\":\"node-1\",\"hits\":3},{\"value\":\"node-2\",\"hits\":2}]}"
        );

        // stream_field_values narrows by the query filter.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/stream_field_values?query={}&field=host",
                encode("error")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert_eq!(body, "{\"values\":[{\"value\":\"node-1\",\"hits\":2}]}");

        // stream_field_values requires the `field` arg.
        let (status, body) = http_get(
            addr,
            &format!("/select/logsql/stream_field_values?query={}", encode("*")),
        );
        assert_eq!(status, 400, "body={body}");
        assert!(body.contains("missing 'field' query arg"), "body={body}");

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }
}

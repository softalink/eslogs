//! Port of EsLogs `app/eslinsert/splunk/splunk.go`.
//!
//! Handles the Splunk HTTP Event Collector (HEC) event protocol on
//! `/insert/splunk/services/collector/*` and the bare `/services/collector/*`
//! aliases.
//!
//! PORT NOTE: v1.51.0 upstream implements only the HEC *event* endpoints
//! (`/services/collector/event`, `/services/collector/event/1.0`) plus the
//! health endpoint; there is no HEC raw mode in the reference, so none is
//! ported.

use std::sync::{Arc, OnceLock};

use esl_common::flagutil::{ArrayString, Bytes, Flag};
use esl_common::httpserver::{Request, ResponseWriter};
use esl_common::{fatalf, warnf};

use esl_logstorage::json_scanner::{get_json_scanner, put_json_scanner};
use esl_logstorage::rows::rename_field;
use esl_logstorage::stream_tags::check_stream_field_names;
use esl_logstorage::tenant_id::{TenantID, parse_tenant_id};

use crate::common_params::{
    CommonParams, LogMessageProcessorTrait, LogRowsStorage, errorf_with_status,
    extract_timestamp_from_fields, get_common_params as insertutil_get_common_params,
};

static SPLUNK_STREAM_FIELDS: Flag<ArrayString> = Flag::new(
    "splunk.streamFields",
    "Comma-separated list of fields to use as log stream fields for logs ingested over Splunk protocol. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/splunk/#stream-fields",
    ArrayString::default,
);
static SPLUNK_IGNORE_FIELDS: Flag<ArrayString> = Flag::new(
    "splunk.ignoreFields",
    "Comma-separated list of fields to ignore for logs ingested over Splunk protocol. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/splunk/#dropping-fields",
    ArrayString::default,
);
static SPLUNK_PRESERVE_JSON_KEYS: Flag<ArrayString> = Flag::new(
    "splunk.preserveJSONKeys",
    "Comma-separated list of JSON keys that should be preserved from flattening \
     when ingested via Splunk protocol. See https://docs.victoriametrics.com/victorialogs/data-ingestion/splunk/ and \
     https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    ArrayString::default,
);
static SPLUNK_TIME_FIELD: Flag<String> = Flag::new(
    "splunk.timeField",
    "Field to use as a log timestamp for logs ingested via Splunk protocol. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/splunk/#time-field",
    || "time".to_string(),
);
static SPLUNK_MSG_FIELD: Flag<ArrayString> = Flag::new(
    "splunk.msgField",
    "Field to use as a log message for logs ingested via Splunk protocol. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/splunk/#message-field",
    ArrayString::default,
);
static SPLUNK_TENANT_ID: Flag<String> = Flag::new(
    "splunk.tenantID",
    "TenantID for logs ingested via the Splunk endpoint. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/splunk/#multitenancy",
    || "0:0".to_string(),
);
static SPLUNK_MAX_REQUEST_SIZE: Flag<Bytes> = Flag::new(
    "splunk.maxRequestSize",
    "The maximum size in bytes of a single Splunk request; \
     see https://docs.victoriametrics.com/victorialogs/data-ingestion/splunk/",
    || Bytes::with_default(64 * 1024 * 1024),
);

/// MustInit initializes Splunk parser.
///
/// This function must be called after flag parsing.
///
/// PORT NOTE: the Rust app layer has no per-protocol `MustInit` hook wired
/// from `main`, so [`tenant_id`] and [`stream_fields`] also initialize lazily
/// on first request; calling this simply forces (and validates) the
/// initialization eagerly like Go.
pub fn must_init() {
    tenant_id();
    stream_fields();
}

fn tenant_id() -> TenantID {
    static TENANT_ID: OnceLock<TenantID> = OnceLock::new();
    *TENANT_ID.get_or_init(|| {
        let s = SPLUNK_TENANT_ID.get();
        match parse_tenant_id(s) {
            Ok(t) => t,
            Err(err) => {
                fatalf!(
                    "cannot parse -splunk.tenantID={s:?}: {err}; see https://docs.victoriametrics.com/victorialogs/data-ingestion/splunk/"
                );
                unreachable!()
            }
        }
    })
}

fn stream_fields() -> &'static [String] {
    static STREAM_FIELDS: OnceLock<Vec<String>> = OnceLock::new();
    STREAM_FIELDS.get_or_init(|| {
        // Initialize streamFields
        let mut stream_fields: Vec<String> =
            DEFAULT_STREAM_FIELDS.iter().map(|s| s.to_string()).collect();
        if !SPLUNK_STREAM_FIELDS.get().is_empty() {
            stream_fields = SPLUNK_STREAM_FIELDS.get().0.clone();
        }
        let sf_refs: Vec<&str> = stream_fields.iter().map(String::as_str).collect();
        if let Err(err) = check_stream_field_names(&sf_refs) {
            fatalf!(
                "invalid stream field names in -splunk.streamFields={stream_fields:?}: {err}; see https://docs.victoriametrics.com/victorialogs/data-ingestion/splunk/#stream-fields"
            );
        }
        stream_fields
    })
}

const DEFAULT_STREAM_FIELDS: &[&str] = &["host", "source", "sourcetype"];

fn get_common_params(req: &Request) -> Result<CommonParams, String> {
    let mut cp = insertutil_get_common_params(req)?;
    if cp.tenant_id.account_id == 0 && cp.tenant_id.project_id == 0 {
        cp.tenant_id = tenant_id();
    }

    if !cp.is_time_field_set {
        cp.time_fields = vec![SPLUNK_TIME_FIELD.get().clone()];
    }
    if cp.stream_fields.is_empty() {
        cp.stream_fields = stream_fields().to_vec();
    }
    if cp.ignore_fields.is_empty() {
        cp.ignore_fields = SPLUNK_IGNORE_FIELDS.get().0.clone();
    }
    if cp.msg_fields.is_empty() {
        cp.msg_fields = get_msg_fields();
    }
    if cp.preserve_json_keys.is_empty() {
        cp.preserve_json_keys = SPLUNK_PRESERVE_JSON_KEYS.get().0.clone();
    }
    Ok(cp)
}

fn get_msg_fields() -> Vec<String> {
    if !SPLUNK_MSG_FIELD.get().is_empty() {
        return SPLUNK_MSG_FIELD.get().0.clone();
    }
    DEFAULT_MSG_FIELDS.iter().map(|s| s.to_string()).collect()
}

const DEFAULT_MSG_FIELDS: &[&str] = &["event", "event.log", "event.line", "event.message"];

/// RequestHandler processes splunk insert requests. Returns true if the path
/// was handled.
pub fn request_handler<S: LogRowsStorage>(
    storage: &Arc<S>,
    path: &str,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    match path {
        "/insert/splunk/services/collector/health" | "/services/collector/health" => {
            w.set_status(200);
        }
        "/insert/splunk/services/collector/event"
        | "/insert/splunk/services/collector/event/1.0"
        | "/services/collector/event"
        | "/services/collector/event/1.0" => {
            handle_collector_event(storage, req, w);
        }
        _ => return false,
    }
    true
}

/// PORT NOTE: Go's unexported `requestHandler`; renamed to avoid clashing
/// with the exported `RequestHandler` in snake_case.
fn handle_collector_event<S: LogRowsStorage>(
    storage: &Arc<S>,
    req: &mut Request,
    w: &mut ResponseWriter,
) {
    match req.method() {
        "OPTIONS" => {
            w.set_status(200);
            return;
        }
        "POST" => {
            w.set_header("Content-Type", "application/json");
        }
        _ => {
            w.set_status(405);
            return;
        }
    }

    let cp = match get_common_params(req) {
        Ok(cp) => cp,
        Err(err) => {
            w.errorf(req, &err);
            return;
        }
    };

    if let Err((msg, status)) = storage.can_write_data() {
        errorf_with_status(w, req, &msg, status);
        return;
    }

    // PORT NOTE: Go streams the body through
    // `protoparserutil.ReadUncompressedData`, which caps the *decompressed*
    // size at -splunk.maxRequestSize; the port's `Request::body_reader`
    // already decompresses per Content-Encoding, so the cap is checked after
    // reading the body in full.
    let data = match req.read_full_body() {
        Ok(d) => d,
        Err(err) => {
            w.errorf(req, &format!("cannot read Splunk request: {err}"));
            return;
        }
    };
    let max_request_size = SPLUNK_MAX_REQUEST_SIZE.get().int_n().max(0) as usize;
    if data.len() > max_request_size {
        w.errorf(
            req,
            &format!(
                "cannot read Splunk request: request size ({} bytes) exceeds -splunk.maxRequestSize={max_request_size}",
                data.len()
            ),
        );
        return;
    }

    let time_fields: Vec<&str> = cp.time_fields.iter().map(String::as_str).collect();
    let msg_fields: Vec<&str> = cp.msg_fields.iter().map(String::as_str).collect();
    let preserve_keys: Vec<&str> = cp.preserve_json_keys.iter().map(String::as_str).collect();

    let mut lmp = cp.new_log_message_processor(storage, "splunk");
    let res = process_event(&data, &mut lmp, &time_fields, &msg_fields, &preserve_keys);
    lmp.close();

    if let Err(err) = res {
        w.errorf(req, &format!("cannot read Splunk request: {err}"));
        return;
    }

    w.write_str(r#"{"text":"Success","code":0}"#);
}

fn process_event(
    data: &[u8],
    lmp: &mut impl LogMessageProcessorTrait,
    time_fields: &[&str],
    msg_fields: &[&str],
    preserve_keys: &[&str],
) -> Result<(), String> {
    let mut s = get_json_scanner();

    let mut n = 0usize;

    s.init(data, preserve_keys, "");
    while s.next_log_message() {
        let ts = match extract_timestamp_from_fields(time_fields, s.fields_mut()) {
            Ok(ts) => ts,
            Err(err) => {
                warnf!(
                    "splunk: failed to parse timestamp for JSON message #{}: {err}",
                    n + 1
                );
                continue;
            }
        };
        rename_field(s.fields_mut(), msg_fields, "_msg");
        lmp.add_row(ts, s.fields_mut(), -1);
        n += 1;
    }
    let err = s.error().map(|e| e.to_string());
    put_json_scanner(s);

    if let Some(err) = err {
        if n > 0 {
            warnf!("splunk: failed to parse JSON message #{}: {err}", n + 1);
            return Ok(());
        }
        return Err(format!("splunk: failed to parse whole event: {err}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common_params::TestLogMessageProcessor;

    #[test]
    fn test_process_data_success() {
        fn f(
            data: &str,
            time_field: &str,
            msg_field: &str,
            timestamps_expected: &[i64],
            result_expected: &str,
        ) {
            let time_fields = [time_field];
            let msg_fields = [msg_field];
            let mut tlp = TestLogMessageProcessor::default();
            if let Err(err) =
                process_event(data.as_bytes(), &mut tlp, &time_fields, &msg_fields, &[])
            {
                panic!("unexpected error: {err}");
            }

            if let Err(err) = tlp.verify(timestamps_expected, result_expected) {
                panic!("{err}");
            }
            tlp.must_close();
        }

        let data = concat!(
            r#"{"@timestamp":"2023-06-06T04:48:11.735Z","event":"foobar","source":"docker","host":"localhost"}"#,
            r#"{"@timestamp":"2023-06-06T04:48:12.735+01:00","event":"baz"}"#,
            r#"{"event":"xyz","@timestamp":"2023-06-06 04:48:13.735Z","x":"y"}"#
        );
        let time_field = "@timestamp";
        let msg_field = "event";
        let timestamps_expected = [
            1686026891735000000,
            1686023292735000000,
            1686026893735000000,
        ];
        let result_expected = "{\"_msg\":\"foobar\",\"source\":\"docker\",\"host\":\"localhost\"}\n\
             {\"_msg\":\"baz\"}\n\
             {\"_msg\":\"xyz\",\"x\":\"y\"}";
        f(
            data,
            time_field,
            msg_field,
            &timestamps_expected,
            result_expected,
        );

        // Non-existing msgField
        let data = concat!(
            r#"{"@timestamp":"2023-06-06T04:48:11.735Z","log":{"offset":71770,"file":{"path":"/var/log/auth.log"}},"message":"foobar"}"#,
            r#"{"@timestamp":"2023-06-06T04:48:12.735+01:00","message":"baz"}"#
        );
        let time_field = "@timestamp";
        let msg_field = "foobar";
        let timestamps_expected = [1686026891735000000, 1686023292735000000];
        let result_expected = "{\"log.offset\":\"71770\",\"log.file.path\":\"/var/log/auth.log\",\"message\":\"foobar\"}\n\
             {\"message\":\"baz\"}";
        f(
            data,
            time_field,
            msg_field,
            &timestamps_expected,
            result_expected,
        );
    }

    #[test]
    fn test_process_data_failure() {
        fn f(data: &str) {
            let mut tlp = TestLogMessageProcessor::default();
            let res = process_event(data.as_bytes(), &mut tlp, &["time"], &[], &[]);
            assert!(res.is_err(), "expected error, got nil for data={data:?}");

            if let Err(err) = tlp.verify(&[], "") {
                panic!("unexpected error: {err}");
            }
            tlp.must_close();
        }

        // invalid json
        f("foobar");

        f("foo\nbar");

        f("\nfoo\n\n");

        // contains invalid JSON
        f(r#"{"time":"foobar}"#);
    }
}

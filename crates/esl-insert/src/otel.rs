//! Port of EsLogs `app/eslinsert/opentelemetry/{opentelemetry.go, pb.go,
//! pb_json.go, fmt_buffer.go}`: OpenTelemetry (OTLP/HTTP) logs ingestion at
//! `/insert/opentelemetry/v1/logs` with protobuf-encoded payloads.
//!
//! The file mirrors the four Go source files with section comments; the ported
//! `opentelemetry_test.go` lives in the `tests` module at the bottom.
//!
//! The protobuf wire codec is `esl_common::easyproto` (the port of
//! `github.com/VictoriaMetrics/easyproto`).
//!
//! PORT NOTE: Go parses JSON-encoded array/kvlist bodies via the vendored
//! `valyala/fastjson` arena. The port models JSON values with the private
//! [`JsonValue`] enum instead.

use std::sync::{Arc, LazyLock};
use std::time::Instant;

use esl_common::easyproto;
use esl_common::flagutil::{Bytes, Flag};
use esl_common::httpserver::{Request, ResponseWriter};
use esl_common::metrics::{Counter, Summary};
use esl_common::stringsutil::json_string_bytes_append;

use esl_logstorage::rows::{Field, Fields, get_fields, put_fields, rename_field};

use crate::common_params::{
    LogMessageProcessor, LogRowsStorage, errorf_with_status, get_common_params,
    is_json_content_type, now_unix_nanos,
};

// ---------------------------------------------------------------------------
// opentelemetry.go
// ---------------------------------------------------------------------------

/// RequestHandler processes Opentelemetry insert requests. Returns true if the
/// path was handled.
static MAX_REQUEST_SIZE: Flag<Bytes> = Flag::new(
    "opentelemetry.maxRequestSize",
    "The maximum size in bytes of a single OpenTelemetry request",
    || Bytes::with_default(64 * 1024 * 1024),
);

pub fn request_handler<S: LogRowsStorage>(
    storage: &Arc<S>,
    path: &str,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    match path {
        // use the same path as opentelemetry collector
        // https://opentelemetry.io/docs/specs/otlp/#otlphttp-request
        "/insert/opentelemetry/v1/logs" => {
            let ct = req.content_type().to_string();
            if is_json_content_type(&ct) {
                w.errorf(
                    req,
                    "json encoding isn't supported for opentelemetry format. Use protobuf encoding",
                );
                return true;
            }
            handle_protobuf(storage, req, w);
            true
        }
        _ => false,
    }
}

static REQUESTS_PROTOBUF_TOTAL: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::new_counter(
        r#"esl_http_requests_total{path="/insert/opentelemetry/v1/logs",format="protobuf"}"#,
    )
});
static ERRORS_TOTAL: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::new_counter(
        r#"esl_http_errors_total{path="/insert/opentelemetry/v1/logs",format="protobuf"}"#,
    )
});
static REQUEST_PROTOBUF_DURATION: LazyLock<Arc<Summary>> = LazyLock::new(|| {
    esl_common::metrics::new_summary(
        r#"esl_http_request_duration_seconds{path="/insert/opentelemetry/v1/logs",format="protobuf"}"#,
    )
});

fn handle_protobuf<S: LogRowsStorage>(storage: &Arc<S>, req: &mut Request, w: &mut ResponseWriter) {
    let start_time = Instant::now();
    REQUESTS_PROTOBUF_TOTAL.inc();

    if let Err((msg, status)) = storage.can_write_data() {
        errorf_with_status(w, req, &msg, status);
        return;
    }

    let cp = match get_common_params(req) {
        Ok(cp) => cp,
        Err(err) => {
            w.errorf(
                req,
                &format!("cannot parse common params from request: {err}"),
            );
            return;
        }
    };

    // Go reads the body via protoparserutil.ReadUncompressedData, which honors
    // the Content-Encoding header and caps the *decompressed* size at
    // -opentelemetry.maxRequestSize (64MiB) during the read;
    // `read_full_body_limited` decompresses per Content-Encoding and applies
    // the same cap while reading, so a decompression bomb cannot fully
    // materialize.
    let data = match req.read_full_body_limited(
        MAX_REQUEST_SIZE.get().int_n() as i64,
        MAX_REQUEST_SIZE.name(),
    ) {
        Ok(d) => d,
        Err(err) => {
            w.errorf(
                req,
                &format!("cannot read OpenTelemetry protocol data: {err}"),
            );
            return;
        }
    };

    let use_default_stream_fields = cp.stream_fields.is_empty();
    let msg_fields: Vec<&str> = cp.msg_fields.iter().map(String::as_str).collect();

    let mut lmp = cp.new_log_message_processor(storage, "opentelemetry_protobuf");
    let res = push_protobuf_request(&data, &mut lmp, &msg_fields, use_default_stream_fields);
    lmp.close();

    if let Err(err) = res {
        ERRORS_TOTAL.inc();
        w.errorf(
            req,
            &format!("cannot read OpenTelemetry protocol data: {err}"),
        );
        return;
    }

    REQUEST_PROTOBUF_DURATION.update_duration(start_time);
}

/// PORT NOTE: Go's `pushProtobufRequest` accepts the
/// `insertutil.LogMessageProcessor` interface. The Rust port's
/// [`LogMessageProcessor`] is a concrete type, so this local trait mirrors the
/// interface and lets the ported test substitute a `TestLogMessageProcessor`
/// exactly like the Go test does.
trait InsertLogMessageProcessor {
    fn add_row(&mut self, timestamp: i64, fields: &mut [Field], stream_fields_len: isize);
}

impl<S: LogRowsStorage> InsertLogMessageProcessor for LogMessageProcessor<'_, S> {
    fn add_row(&mut self, timestamp: i64, fields: &mut [Field], stream_fields_len: isize) {
        LogMessageProcessor::add_row(self, timestamp, fields, stream_fields_len);
    }
}

fn push_protobuf_request<L: InsertLogMessageProcessor>(
    data: &[u8],
    lmp: &mut L,
    msg_fields: &[&str],
    use_default_stream_fields: bool,
) -> Result<(), String> {
    let mut push_logs = |timestamp: i64, fields: &mut [Field], stream_fields_len: usize| {
        rename_field(&mut fields[stream_fields_len..], msg_fields, "_msg");

        let stream_fields_len = if use_default_stream_fields {
            stream_fields_len as isize
        } else {
            -1
        };

        lmp.add_row(timestamp, fields, stream_fields_len);
    };

    decode_logs_data(data, &mut push_logs).map_err(|err| {
        format!(
            "cannot decode LogsData request from {} bytes: {err}",
            data.len()
        )
    })
}

// ---------------------------------------------------------------------------
// pb.go
// ---------------------------------------------------------------------------

/// The push_logs handler must store the log entry with the given args.
///
/// The handler must copy the fields before returning, since the caller can
/// change them, so they become invalid if not copied.
type PushLogsHandler<'a> = dyn FnMut(i64, &mut [Field], usize) + 'a;

/// decodeLogsData parses a LogsData protobuf message from src and calls the
/// provided push_logs for each decoded log record.
///
/// See <https://github.com/open-telemetry/opentelemetry-proto/blob/a5f0eac5b802f7ae51dfe41e5116fe5548955e64/opentelemetry/proto/logs/v1/logs.proto#L38>
fn decode_logs_data(src: &[u8], push_logs: &mut PushLogsHandler<'_>) -> Result<(), String> {
    // message LogsData {
    //   repeated ResourceLogs resource_logs = 1;
    // }

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        if fc.field_num == 1 {
            let data = fc
                .message_data()
                .ok_or_else(|| "cannot read ResourceLogs data".to_string())?;

            decode_resource_logs(data, push_logs)
                .map_err(|err| format!("cannot decode ResourceLogs: {err}"))?;
        }
    }
    Ok(())
}

fn decode_resource_logs(src: &[u8], push_logs: &mut PushLogsHandler<'_>) -> Result<(), String> {
    // PORT NOTE: Go also pools a fmtBuffer here; the port's fmt_buffer helpers
    // return owned Strings, so only the Fields pool is kept. The explicit
    // clear_up_to_capacity + put_fields below stand in for Go's deferred
    // cleanup and thus run on the error paths too.
    let mut fs = get_fields();
    let res = decode_resource_logs_internal(src, &mut fs, push_logs);
    fs.clear_up_to_capacity();
    put_fields(fs);
    res
}

fn decode_resource_logs_internal(
    src: &[u8],
    fs: &mut Fields,
    push_logs: &mut PushLogsHandler<'_>,
) -> Result<(), String> {
    // message ResourceLogs {
    //   Resource resource = 1;
    //   repeated ScopeLogs scope_logs = 2;
    // }

    // Decode resource
    let resource_data = easyproto::get_message_data(src, 1)
        .map_err(|err| format!("cannot find Resource: {err}"))?;
    if let Some(data) = resource_data {
        decode_resource(data, fs).map_err(|err| format!("cannot decode Resource: {err}"))?;
    }

    let stream_fields_len = fs.fields.len();

    // Decode scope_logs
    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        if fc.field_num == 2 {
            let data = fc
                .message_data()
                .ok_or_else(|| "cannot read ScopeLogs data".to_string())?;

            decode_scope_logs(data, fs, push_logs)
                .map_err(|err| format!("cannot decode ScopeLogs: {err}"))?;

            fs.fields.truncate(stream_fields_len);
        }
    }

    Ok(())
}

fn decode_resource(src: &[u8], fs: &mut Fields) -> Result<(), String> {
    // message Resource {
    //   repeated KeyValue attributes = 1;
    // }

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        if fc.field_num == 1 {
            let data = fc
                .message_data()
                .ok_or_else(|| "cannot read Attributes data".to_string())?;

            decode_key_value(data, fs, "")
                .map_err(|err| format!("cannot decode Attributes: {err}"))?;
        }
    }
    Ok(())
}

fn decode_scope_logs(
    src: &[u8],
    fs: &mut Fields,
    push_logs: &mut PushLogsHandler<'_>,
) -> Result<(), String> {
    // message ScopeLogs {
    //   InstrumentationScope scope = 1;
    //   repeated LogRecord log_records = 2;
    // }

    let stream_fields_len = fs.fields.len();

    let scope_data = easyproto::get_message_data(src, 1)
        .map_err(|err| format!("cannot read InstrumentationScope: {err}"))?;
    if let Some(data) = scope_data {
        decode_instrumentation_scope(data, fs)
            .map_err(|err| format!("cannot decode InstrumentationScope: {err}"))?;
    }

    let common_fields_len = fs.fields.len();

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        if fc.field_num == 2 {
            let data = fc
                .message_data()
                .ok_or_else(|| "cannot read LogRecord data".to_string())?;

            let (event_name, timestamp) = decode_log_record(data, fs)
                .map_err(|err| format!("cannot decode LogRecord: {err}"))?;
            if !event_name.is_empty() {
                // Insert event_name into stream fields.
                // PORT NOTE: Go appends a dummy field and shifts the tail by
                // hand; Vec::insert/remove is the equivalent.
                fs.fields.insert(
                    stream_fields_len,
                    Field {
                        name: "event_name".to_string(),
                        value: event_name,
                    },
                );

                push_logs(timestamp, &mut fs.fields, stream_fields_len + 1);

                // Return back common fields to their places before the next iteration
                fs.fields.remove(stream_fields_len);
                fs.fields.truncate(common_fields_len);
            } else {
                push_logs(timestamp, &mut fs.fields, stream_fields_len);

                fs.fields.truncate(common_fields_len);
            }
        }
    }
    Ok(())
}

fn decode_instrumentation_scope(src: &[u8], fs: &mut Fields) -> Result<(), String> {
    // See https://github.com/open-telemetry/opentelemetry-proto/blob/a5f0eac5b802f7ae51dfe41e5116fe5548955e64/opentelemetry/proto/common/v1/common.proto#L76
    //
    // message InstrumentationScope {
    //   string name = 1;
    //   string version = 2;
    //   repeated KeyValue attributes = 3;
    // }

    // Field VALUE paths: read as raw bytes so invalid UTF-8 is ingested
    // verbatim like Go instead of erroring.
    let name = easyproto::get_bytes(src, 1)
        .map_err(|err| format!("cannot read name: {err}"))?
        .unwrap_or(b"unknown");
    fs.add("scope.name", name);

    let version = easyproto::get_bytes(src, 2)
        .map_err(|err| format!("cannot read version: {err}"))?
        .unwrap_or(b"unknown");
    fs.add("scope.version", version);

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        if fc.field_num == 3 {
            let attributes_data = fc
                .message_data()
                .ok_or_else(|| "cannot read Attributes data".to_string())?;
            decode_key_value(attributes_data, fs, "scope.attributes")
                .map_err(|err| format!("cannot decode Attributes: {err}"))?;
        }
    }

    Ok(())
}

fn decode_log_record(src: &[u8], fs: &mut Fields) -> Result<(Vec<u8>, i64), String> {
    // See https://github.com/open-telemetry/opentelemetry-proto/blob/a5f0eac5b802f7ae51dfe41e5116fe5548955e64/opentelemetry/proto/logs/v1/logs.proto#L136
    //
    // message LogRecord {
    //   fixed64 time_unix_nano = 1;
    //   fixed64 observed_time_unix_nano = 11;
    //   SeverityNumber severity_number = 2;
    //   string severity_text = 3;
    //   AnyValue body = 5;
    //   repeated KeyValue attributes = 6;
    //   bytes trace_id = 9;
    //   bytes span_id = 10;
    //   string event_name = 12;
    // }

    let mut time_unix_nano = 0u64;
    let mut observed_time_unix_nano = 0u64;
    let mut severity_text: &[u8] = b"";
    let mut severity_number = 0i32;
    let mut event_name: &[u8] = b"";

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        match fc.field_num {
            1 => {
                time_unix_nano = fc
                    .fixed64()
                    .ok_or_else(|| "cannot read log record timestamp".to_string())?;
            }
            11 => {
                observed_time_unix_nano = fc
                    .fixed64()
                    .ok_or_else(|| "cannot read log record observed timestamp".to_string())?;
            }
            2 => {
                severity_number = fc
                    .int32()
                    .ok_or_else(|| "cannot read severity number".to_string())?;
            }
            3 => {
                // Field VALUE path: raw bytes, ingested verbatim like Go.
                severity_text = fc
                    .bytes()
                    .ok_or_else(|| "cannot read severity string".to_string())?;
            }
            5 => {
                let body = fc
                    .message_data()
                    .ok_or_else(|| "cannot read Body".to_string())?;
                decode_any_value(body, fs, "")
                    .map_err(|err| format!("cannot decode Body: {err}"))?;
            }
            6 => {
                let attributes_data = fc
                    .message_data()
                    .ok_or_else(|| "cannot read Attributes data".to_string())?;
                decode_key_value(attributes_data, fs, "")
                    .map_err(|err| format!("cannot decode Attributes: {err}"))?;
            }
            9 => {
                let trace_id = fc
                    .bytes()
                    .ok_or_else(|| "cannot read trace id".to_string())?;
                let trace_id_hex = format_hex(trace_id);
                fs.add("trace_id", &trace_id_hex);
            }
            10 => {
                let span_id = fc
                    .bytes()
                    .ok_or_else(|| "cannot read span id".to_string())?;
                let span_id_hex = format_hex(span_id);
                fs.add("span_id", &span_id_hex);
            }
            12 => {
                // Field VALUE path: raw bytes, ingested verbatim like Go.
                event_name = fc
                    .bytes()
                    .ok_or_else(|| "cannot read event_name".to_string())?;
            }
            _ => {}
        }
    }

    let severity_number_str = format_int(i64::from(severity_number));
    fs.add("severity_number", &severity_number_str);

    if severity_text.is_empty() {
        severity_text = format_severity(severity_number).as_bytes();
    }
    fs.add("severity_text", severity_text);

    let timestamp = if time_unix_nano > 0 {
        time_unix_nano as i64
    } else if observed_time_unix_nano > 0 {
        observed_time_unix_nano as i64
    } else {
        now_unix_nanos()
    };

    Ok((event_name.to_vec(), timestamp))
}

fn decode_key_value(src: &[u8], fs: &mut Fields, field_name_prefix: &str) -> Result<(), String> {
    // message KeyValue {
    //   string key = 1;
    //   AnyValue value = 2;
    // }

    // Decode key
    let key = match easyproto::get_string(src, 1)
        .map_err(|err| format!("cannot find Key in KeyValue: {err}"))?
    {
        Some(key) => key,
        None => {
            // Key is missing, skip it.
            // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/869#issuecomment-3631307996
            return Ok(());
        }
    };
    let field_name = format_sub_field_name(field_name_prefix, key);

    // Decode value
    let value_data = match easyproto::get_message_data(src, 2)
        .map_err(|err| format!("cannot find Value in KeyValue: {err}"))?
    {
        Some(data) => data,
        None => {
            // Value is null, skip it.
            return Ok(());
        }
    };

    decode_any_value(value_data, fs, &field_name)
        .map_err(|err| format!("cannot decode AnyValue: {err}"))?;

    Ok(())
}

fn decode_any_value(src: &[u8], fs: &mut Fields, field_name: &str) -> Result<(), String> {
    // message AnyValue {
    //   oneof value {
    //     string string_value = 1;
    //     bool bool_value = 2;
    //     int64 int_value = 3;
    //     double double_value = 4;
    //     ArrayValue array_value = 5;
    //     KeyValueList kvlist_value = 6;
    //     bytes bytes_value = 7;
    //   }
    // }

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        match fc.field_num {
            1 => {
                // Field VALUE path: raw bytes, ingested verbatim like Go.
                let string_value = fc
                    .bytes()
                    .ok_or_else(|| "cannot read StringValue".to_string())?;
                fs.add(field_name, string_value);
            }
            2 => {
                let bool_value = fc
                    .bool_value()
                    .ok_or_else(|| "cannot read BoolValue".to_string())?;
                let bool_value_str = if bool_value { "true" } else { "false" };
                fs.add(field_name, bool_value_str);
            }
            3 => {
                let int_value = fc
                    .int64()
                    .ok_or_else(|| "cannot read IntValue".to_string())?;
                let int_value_str = format_int(int_value);
                fs.add(field_name, &int_value_str);
            }
            4 => {
                let double_value = fc
                    .double()
                    .ok_or_else(|| "cannot read DoubleValue".to_string())?;
                let double_value_str = format_float(double_value);
                fs.add(field_name, &double_value_str);
            }
            5 => {
                let data = fc
                    .message_data()
                    .ok_or_else(|| "cannot read ArrayValue".to_string())?;

                // Encode arrays as JSON to match the behavior of /insert/jsonline
                let arr = decode_array_value_to_json(data)
                    .map_err(|err| format!("cannot decode ArrayValue: {err}"))?;
                let encoded_arr = encode_json_value(&arr);

                fs.add(field_name, &encoded_arr);
            }
            6 => {
                let data = fc
                    .message_data()
                    .ok_or_else(|| "cannot read KeyValueList".to_string())?;
                decode_key_value_list(data, fs, field_name)
                    .map_err(|err| format!("cannot decode KeyValueList: {err}"))?;
            }
            7 => {
                let bytes_value = fc
                    .bytes()
                    .ok_or_else(|| "cannot read BytesValue".to_string())?;
                let v = format_base64(bytes_value);
                fs.add(field_name, &v);
            }
            _ => {}
        }
    }
    Ok(())
}

fn decode_key_value_list(
    src: &[u8],
    fs: &mut Fields,
    field_name_prefix: &str,
) -> Result<(), String> {
    // message KeyValueList {
    //   repeated KeyValue values = 1;
    // }

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        if fc.field_num == 1 {
            let data = fc
                .message_data()
                .ok_or_else(|| "cannot read KeyValue data".to_string())?;
            decode_key_value(data, fs, field_name_prefix)
                .map_err(|err| format!("cannot decode KeyValue: {err}"))?;
        }
    }
    Ok(())
}

fn format_severity(severity: i32) -> &'static str {
    if severity < 0 || severity >= LOG_SEVERITIES.len() as i32 {
        return LOG_SEVERITIES[0];
    }
    LOG_SEVERITIES[severity as usize]
}

// See https://github.com/open-telemetry/opentelemetry-collector/blob/a0cbea73c189551d751d09659e306f48f594fd62/pdata/plog/severity_number.go#L41
static LOG_SEVERITIES: [&str; 25] = [
    "Unspecified",
    "Trace",
    "Trace2",
    "Trace3",
    "Trace4",
    "Debug",
    "Debug2",
    "Debug3",
    "Debug4",
    "Info",
    "Info2",
    "Info3",
    "Info4",
    "Warn",
    "Warn2",
    "Warn3",
    "Warn4",
    "Error",
    "Error2",
    "Error3",
    "Error4",
    "Fatal",
    "Fatal2",
    "Fatal3",
    "Fatal4",
];

// ---------------------------------------------------------------------------
// pb_json.go
// ---------------------------------------------------------------------------

/// A JSON value used for encoding OTLP array/kvlist bodies as JSON strings.
///
/// PORT NOTE: replaces the `fastjson.Value`/`fastjson.Arena` pair used by the
/// Go source (jsonArenaPool). Marshaling matches fastjson's compact output.
enum JsonValue {
    Null,
    Bool(bool),
    Int(i64),
    Double(f64),
    /// Raw string bytes (Go strings are arbitrary bytes); preserved verbatim
    /// through the JSON encoding like Go fastjson.
    Str(Vec<u8>),
    Arr(Vec<JsonValue>),
    Obj(Vec<(String, JsonValue)>),
}

impl JsonValue {
    /// Mirrors `fastjson.Value.Set`: replaces the value for an existing key or
    /// appends a new (key, value) entry. No-op for non-object values.
    fn set(&mut self, key: &str, value: JsonValue) {
        if let JsonValue::Obj(entries) = self {
            if let Some(entry) = entries.iter_mut().find(|(k, _)| k == key) {
                entry.1 = value;
                return;
            }
            entries.push((key.to_string(), value));
        }
    }

    fn marshal_to(&self, dst: &mut Vec<u8>) {
        match self {
            JsonValue::Null => dst.extend_from_slice(b"null"),
            JsonValue::Bool(b) => dst.extend_from_slice(if *b { b"true" } else { b"false" }),
            JsonValue::Int(i) => dst.extend_from_slice(format_int(*i).as_bytes()),
            JsonValue::Double(f) => dst.extend_from_slice(format_float(*f).as_bytes()),
            JsonValue::Str(s) => json_string_bytes_append(dst, s),
            JsonValue::Arr(items) => {
                dst.push(b'[');
                for (i, v) in items.iter().enumerate() {
                    if i > 0 {
                        dst.push(b',');
                    }
                    v.marshal_to(dst);
                }
                dst.push(b']');
            }
            JsonValue::Obj(entries) => {
                dst.push(b'{');
                for (i, (k, v)) in entries.iter().enumerate() {
                    if i > 0 {
                        dst.push(b',');
                    }
                    json_string_bytes_append(dst, k.as_bytes());
                    dst.push(b':');
                    v.marshal_to(dst);
                }
                dst.push(b'}');
            }
        }
    }
}

/// decodeArrayValueToJSON decodes a protobuf ArrayValue message into a JSON
/// array represented by [`JsonValue`].
fn decode_array_value_to_json(src: &[u8]) -> Result<JsonValue, String> {
    // message ArrayValue {
    //   repeated AnyValue values = 1;
    // }

    let mut dst = Vec::new();

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        if fc.field_num == 1 {
            let data = fc
                .message_data()
                .ok_or_else(|| "cannot read Value data".to_string())?;

            let v = decode_any_value_to_json(data)
                .map_err(|err| format!("cannot decode AnyValue: {err}"))?;
            dst.push(v);
        }
    }

    Ok(JsonValue::Arr(dst))
}

fn decode_any_value_to_json(src: &[u8]) -> Result<JsonValue, String> {
    // message AnyValue {
    //   oneof value {
    //     string string_value = 1;
    //     bool bool_value = 2;
    //     int64 int_value = 3;
    //     double double_value = 4;
    //     ArrayValue array_value = 5;
    //     KeyValueList kvlist_value = 6;
    //     bytes bytes_value = 7;
    //   }
    // }

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        match fc.field_num {
            1 => {
                // Field VALUE path: raw bytes, preserved verbatim like Go.
                let string_value = fc
                    .bytes()
                    .ok_or_else(|| "cannot read StringValue".to_string())?;
                return Ok(JsonValue::Str(string_value.to_vec()));
            }
            2 => {
                let bool_value = fc
                    .bool_value()
                    .ok_or_else(|| "cannot read BoolValue".to_string())?;
                return Ok(JsonValue::Bool(bool_value));
            }
            3 => {
                let int_value = fc
                    .int64()
                    .ok_or_else(|| "cannot read IntValue".to_string())?;
                // PORT NOTE: Go picks fastjson NewNumberInt when the value fits
                // into the platform int and NewNumberFloat64 otherwise; i64
                // always fits JsonValue::Int, so no fallback is needed.
                return Ok(JsonValue::Int(int_value));
            }
            4 => {
                let double_value = fc
                    .double()
                    .ok_or_else(|| "cannot read DoubleValue".to_string())?;
                return Ok(JsonValue::Double(double_value));
            }
            5 => {
                let data = fc
                    .message_data()
                    .ok_or_else(|| "cannot read ArrayValue".to_string())?;
                let arr = decode_array_value_to_json(data)
                    .map_err(|err| format!("cannot decode ArrayValue: {err}"))?;
                return Ok(arr);
            }
            6 => {
                let data = fc
                    .message_data()
                    .ok_or_else(|| "cannot read KeyValueList".to_string())?;
                let obj = decode_key_value_list_to_json(data)
                    .map_err(|err| format!("cannot decode KeyValueList: {err}"))?;
                return Ok(obj);
            }
            7 => {
                let bytes_value = fc
                    .bytes()
                    .ok_or_else(|| "cannot read BytesValue".to_string())?;
                let v = format_base64(bytes_value);
                return Ok(JsonValue::Str(v.into_bytes()));
            }
            _ => {}
        }
    }
    Ok(JsonValue::Null)
}

fn decode_key_value_list_to_json(src: &[u8]) -> Result<JsonValue, String> {
    // message KeyValueList {
    //   repeated KeyValue values = 1;
    // }

    let mut dst = JsonValue::Obj(Vec::new());

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        if fc.field_num == 1 {
            let data = fc
                .message_data()
                .ok_or_else(|| "cannot read Value data".to_string())?;

            decode_key_value_to_json(data, &mut dst)
                .map_err(|err| format!("cannot decode KeyValue: {err}"))?;
        }
    }
    Ok(dst)
}

fn decode_key_value_to_json(src: &[u8], dst: &mut JsonValue) -> Result<(), String> {
    // message KeyValue {
    //   string key = 1;
    //   AnyValue value = 2;
    // }

    // Decode key
    let field_name = match easyproto::get_string(src, 1)
        .map_err(|err| format!("cannot find Key in KeyValue: {err}"))?
    {
        Some(key) => key,
        None => {
            // Key is missing, skip it.
            // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/869#issuecomment-3631307996
            return Ok(());
        }
    };

    // Decode value
    let value_data = match easyproto::get_message_data(src, 2)
        .map_err(|err| format!("cannot find Value in KeyValue: {err}"))?
    {
        Some(data) => data,
        None => {
            // Value is null, skip it.
            return Ok(());
        }
    };

    let v = decode_any_value_to_json(value_data)
        .map_err(|err| format!("cannot decode AnyValue: {err}"))?;

    dst.set(field_name, v);

    Ok(())
}

// ---------------------------------------------------------------------------
// fmt_buffer.go
// ---------------------------------------------------------------------------
//
// PORT NOTE: Go's fmtBuffer amortizes allocations by appending formatted
// values into a pooled byte buffer and returning unsafe string views into it;
// the Rust `Field` stores owned Strings anyway, so the helpers below simply
// return owned Strings and the pool is dropped.

fn format_int(v: i64) -> String {
    v.to_string()
}

/// PORT NOTE: Go formats with strconv.AppendFloat(dst, v, 'f', -1, 64); Rust's
/// shortest-roundtrip Display matches it for finite values (Display never
/// switches to scientific notation either).
fn format_float(v: f64) -> String {
    v.to_string()
}

fn format_sub_field_name(prefix: &str, suffix: &str) -> String {
    if prefix.is_empty() {
        // There is no prefix, so just return the suffix as is.
        return suffix.to_string();
    }
    format!("{prefix}.{suffix}")
}

fn format_hex(src: &[u8]) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(src.len() * 2);
    for b in src {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Mirrors Go `base64.StdEncoding.AppendEncode` (standard alphabet, padded).
///
/// PORT NOTE: esl-common has no base64 helper and new external dependencies are
/// not allowed, so the encoder is implemented here.
fn format_base64(src: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut out = String::with_capacity(src.len().div_ceil(3) * 4);
    for chunk in src.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = u32::from(chunk.get(1).copied().unwrap_or(0));
        let b2 = u32::from(chunk.get(2).copied().unwrap_or(0));
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[(n >> 18) as usize & 63] as char);
        out.push(CHARS[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            CHARS[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            CHARS[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Encodes a [`JsonValue`] as compact JSON bytes (Go `fb.encodeJSONValue`).
fn encode_json_value(v: &JsonValue) -> Vec<u8> {
    let mut out = Vec::new();
    v.marshal_to(&mut out);
    out
}

// ---------------------------------------------------------------------------
// opentelemetry_test.go
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use esl_common::easyproto::{MarshalerPool, MessageMarshaler};
    use esl_logstorage::rows::marshal_fields_to_json;

    // -----------------------------------------------------------------
    // insertutil.TestLogMessageProcessor (Go test helper, ported here
    // because the port has no shared insertutil test module)
    // -----------------------------------------------------------------

    #[derive(Default)]
    struct TestLogMessageProcessor {
        timestamps: Vec<i64>,
        rows: Vec<String>,
    }

    impl InsertLogMessageProcessor for TestLogMessageProcessor {
        fn add_row(&mut self, timestamp: i64, fields: &mut [Field], stream_fields_len: isize) {
            assert!(
                stream_fields_len < 0,
                "BUG: streamFieldsLen must be negative; got {stream_fields_len}"
            );
            self.timestamps.push(timestamp);
            let mut buf = Vec::new();
            marshal_fields_to_json(&mut buf, fields);
            self.rows.push(String::from_utf8(buf).unwrap());
        }
    }

    impl TestLogMessageProcessor {
        /// Verifies the number of rows, timestamps and results after add_row calls.
        fn verify(&self, timestamps_expected: &[i64], result_expected: &str) -> Result<(), String> {
            let result = self.rows.join("\n");
            if self.rows.len() != timestamps_expected.len() {
                return Err(format!(
                    "unexpected rows read; got {}; want {};\nrows read:\n{result}\nrows wanted\n{result_expected}",
                    self.rows.len(),
                    timestamps_expected.len()
                ));
            }
            if self.timestamps != timestamps_expected {
                return Err(format!(
                    "unexpected timestamps;\ngot\n{:?}\nwant\n{:?}",
                    self.timestamps, timestamps_expected
                ));
            }
            if result != result_expected {
                return Err(format!(
                    "unexpected result;\ngot\n{result}\nwant\n{result_expected}"
                ));
            }
            Ok(())
        }
    }

    // -----------------------------------------------------------------
    // OTEL protobuf message structs + marshaling (test-only, mirrors the
    // logsData/resourceLogs/... structs in opentelemetry_test.go)
    // -----------------------------------------------------------------

    /// Mirrors Go `var mp easyproto.MarshalerPool`.
    static MP: MarshalerPool = MarshalerPool::new();

    /// logsData represents the corresponding OTEL protobuf message.
    #[derive(Default)]
    struct LogsData {
        resource_logs: Vec<ResourceLogs>,
    }

    impl LogsData {
        /// Marshals r to a protobuf message and appends it to dst.
        fn marshal_protobuf(&self, dst: &mut Vec<u8>) {
            let mut m = MP.get();
            self.marshal_protobuf_internal(&mut m.message_marshaler());
            m.marshal(dst);
            MP.put(m);
        }

        fn marshal_protobuf_internal(&self, mm: &mut MessageMarshaler<'_>) {
            for rm in &self.resource_logs {
                rm.marshal_protobuf(&mut mm.append_message(1));
            }
        }
    }

    /// resourceLogs represents the corresponding OTEL protobuf message.
    #[derive(Default)]
    struct ResourceLogs {
        resource: Resource,
        scope_logs: Vec<ScopeLogs>,
    }

    impl ResourceLogs {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            self.resource.marshal_protobuf(&mut mm.append_message(1));
            for sm in &self.scope_logs {
                sm.marshal_protobuf(&mut mm.append_message(2));
            }
        }
    }

    /// resource represents the corresponding OTEL protobuf message.
    #[derive(Default)]
    struct Resource {
        attributes: Vec<KeyValue>,
    }

    impl Resource {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            for a in &self.attributes {
                a.marshal_protobuf(&mut mm.append_message(1));
            }
        }
    }

    /// keyValue represents the corresponding OTEL protobuf message.
    #[derive(Default)]
    struct KeyValue {
        key: String,
        value: Option<AnyValue>,
    }

    impl KeyValue {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            if !self.key.is_empty() {
                mm.append_string(1, &self.key);
            }
            if let Some(value) = &self.value {
                value.marshal_protobuf(&mut mm.append_message(2));
            }
        }
    }

    /// anyValue represents the corresponding OTEL protobuf message.
    #[derive(Default)]
    struct AnyValue {
        string_value: Option<String>,
        bool_value: Option<bool>,
        int_value: Option<i64>,
        double_value: Option<f64>,
        array_value: Option<ArrayValue>,
        key_value_list: Option<KeyValueList>,
        bytes_value: Option<Vec<u8>>,
    }

    impl AnyValue {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            if let Some(v) = &self.string_value {
                mm.append_string(1, v);
            } else if let Some(v) = self.bool_value {
                mm.append_bool(2, v);
            } else if let Some(v) = self.int_value {
                mm.append_int64(3, v);
            } else if let Some(v) = self.double_value {
                mm.append_double(4, v);
            } else if let Some(v) = &self.array_value {
                v.marshal_protobuf(&mut mm.append_message(5));
            } else if let Some(v) = &self.key_value_list {
                v.marshal_protobuf(&mut mm.append_message(6));
            } else if let Some(v) = &self.bytes_value {
                mm.append_bytes(7, v);
            }
        }
    }

    /// arrayValue represents the corresponding OTEL protobuf message.
    ///
    /// PORT NOTE: Go uses `[]*anyValue` so JSON `null` entries decode to nil
    /// pointers; the port uses `Vec<Option<AnyValue>>` for the same effect.
    #[derive(Default)]
    struct ArrayValue {
        values: Vec<Option<AnyValue>>,
    }

    impl ArrayValue {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            for v in &self.values {
                // A nil anyValue in Go still creates the (empty) child message.
                let mut child = mm.append_message(1);
                if let Some(v) = v {
                    v.marshal_protobuf(&mut child);
                }
            }
        }
    }

    /// keyValueList represents the corresponding OTEL protobuf message.
    #[derive(Default)]
    struct KeyValueList {
        values: Vec<KeyValue>,
    }

    impl KeyValueList {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            for v in &self.values {
                v.marshal_protobuf(&mut mm.append_message(1));
            }
        }
    }

    /// scopeLogs represents the corresponding OTEL protobuf message.
    #[derive(Default)]
    struct ScopeLogs {
        scope: Option<InstrumentationScope>,
        log_records: Vec<LogRecord>,
    }

    impl ScopeLogs {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            if let Some(scope) = &self.scope {
                scope.marshal_protobuf(&mut mm.append_message(1));
            }
            for m in &self.log_records {
                m.marshal_protobuf(&mut mm.append_message(2));
            }
        }
    }

    /// instrumentationScope represents the corresponding OTEL protobuf message.
    /// See https://github.com/open-telemetry/opentelemetry-proto/blob/a5f0eac5b802f7ae51dfe41e5116fe5548955e64/opentelemetry/proto/common/v1/common.proto#L76
    #[derive(Default)]
    struct InstrumentationScope {
        name: String,
        version: String,
        attributes: Vec<KeyValue>,
    }

    impl InstrumentationScope {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            if !self.name.is_empty() {
                mm.append_string(1, &self.name);
            }
            if !self.version.is_empty() {
                mm.append_string(2, &self.version);
            }
            for m in &self.attributes {
                m.marshal_protobuf(&mut mm.append_message(3));
            }
        }
    }

    /// logRecord represents the corresponding OTEL protobuf message.
    /// See https://github.com/open-telemetry/oteps/blob/main/text/logs/0097-log-data-model.md
    #[derive(Default)]
    struct LogRecord {
        time_unix_nano: u64,
        observed_time_unix_nano: u64,
        severity_number: i32,
        severity_text: String,
        body: AnyValue,
        attributes: Vec<KeyValue>,
        trace_id: String,
        span_id: String,
        event_name: String,
    }

    impl LogRecord {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            mm.append_fixed64(1, self.time_unix_nano);
            mm.append_int32(2, self.severity_number);
            mm.append_string(3, &self.severity_text);
            self.body.marshal_protobuf(&mut mm.append_message(5));
            for a in &self.attributes {
                a.marshal_protobuf(&mut mm.append_message(6));
            }

            let trace_id =
                hex_decode(&self.trace_id).unwrap_or_else(|| self.trace_id.as_bytes().to_vec());
            mm.append_bytes(9, &trace_id);

            let span_id =
                hex_decode(&self.span_id).unwrap_or_else(|| self.span_id.as_bytes().to_vec());
            mm.append_bytes(10, &span_id);

            mm.append_fixed64(11, self.observed_time_unix_nano);

            mm.append_string(12, &self.event_name);
        }
    }

    /// Mirrors Go `hex.DecodeString`; returns None on invalid input so callers
    /// can fall back to the raw bytes like the Go test does.
    fn hex_decode(s: &str) -> Option<Vec<u8>> {
        if !s.len().is_multiple_of(2) {
            return None;
        }
        let mut out = Vec::with_capacity(s.len() / 2);
        for i in (0..s.len()).step_by(2) {
            out.push(u8::from_str_radix(&s[i..i + 2], 16).ok()?);
        }
        Some(out)
    }

    /// Decodes standard padded base64 (test fixtures only; panics on bad input).
    fn base64_decode(s: &str) -> Vec<u8> {
        fn val(c: u8) -> u32 {
            match c {
                b'A'..=b'Z' => u32::from(c - b'A'),
                b'a'..=b'z' => u32::from(c - b'a') + 26,
                b'0'..=b'9' => u32::from(c - b'0') + 52,
                b'+' => 62,
                b'/' => 63,
                _ => panic!("invalid base64 char {c:#x}"),
            }
        }

        let s = s.trim_end_matches('=').as_bytes();
        let mut out = Vec::with_capacity(s.len() * 3 / 4);
        for chunk in s.chunks(4) {
            let mut n = 0u32;
            for &c in chunk {
                n = (n << 6) | val(c);
            }
            match chunk.len() {
                4 => {
                    out.push((n >> 16) as u8);
                    out.push((n >> 8) as u8);
                    out.push(n as u8);
                }
                3 => {
                    let n = n << 6;
                    out.push((n >> 16) as u8);
                    out.push((n >> 8) as u8);
                }
                2 => {
                    let n = n << 12;
                    out.push((n >> 16) as u8);
                }
                _ => panic!("invalid base64 length"),
            }
        }
        out
    }

    // -----------------------------------------------------------------
    // JSON fixture parsing (replaces Go's encoding/json Decoder with
    // DisallowUnknownFields; unknown keys panic)
    // -----------------------------------------------------------------

    fn parse_json(s: &str) -> JsonValue {
        let mut p = JsonParser {
            b: s.as_bytes(),
            i: 0,
        };
        p.ws();
        let v = p.value();
        p.ws();
        assert_eq!(p.i, p.b.len(), "trailing data after JSON value");
        v
    }

    struct JsonParser<'a> {
        b: &'a [u8],
        i: usize,
    }

    impl JsonParser<'_> {
        fn ws(&mut self) {
            while self.i < self.b.len() && self.b[self.i].is_ascii_whitespace() {
                self.i += 1;
            }
        }

        fn value(&mut self) -> JsonValue {
            match self.b[self.i] {
                b'"' => JsonValue::Str(self.string().into_bytes()),
                b'{' => self.object(),
                b'[' => self.array(),
                b't' => {
                    self.lit("true");
                    JsonValue::Bool(true)
                }
                b'f' => {
                    self.lit("false");
                    JsonValue::Bool(false)
                }
                b'n' => {
                    self.lit("null");
                    JsonValue::Null
                }
                _ => self.number(),
            }
        }

        fn lit(&mut self, s: &str) {
            assert!(
                self.b[self.i..].starts_with(s.as_bytes()),
                "expected JSON literal {s:?}"
            );
            self.i += s.len();
        }

        fn number(&mut self) -> JsonValue {
            let start = self.i;
            while self.i < self.b.len()
                && matches!(
                    self.b[self.i],
                    b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'
                )
            {
                self.i += 1;
            }
            let raw = std::str::from_utf8(&self.b[start..self.i]).unwrap();
            if raw.contains(['.', 'e', 'E']) {
                JsonValue::Double(raw.parse().unwrap())
            } else {
                JsonValue::Int(raw.parse().unwrap())
            }
        }

        fn string(&mut self) -> String {
            self.i += 1; // consume '"'
            let mut s = String::new();
            loop {
                match self.b[self.i] {
                    b'"' => {
                        self.i += 1;
                        return s;
                    }
                    b'\\' => {
                        self.i += 1;
                        match self.b[self.i] {
                            b'"' => s.push('"'),
                            b'\\' => s.push('\\'),
                            b'/' => s.push('/'),
                            b'n' => s.push('\n'),
                            b'r' => s.push('\r'),
                            b't' => s.push('\t'),
                            other => panic!("unsupported JSON escape \\{}", other as char),
                        }
                        self.i += 1;
                    }
                    c => {
                        // The test fixtures are ASCII-only.
                        s.push(c as char);
                        self.i += 1;
                    }
                }
            }
        }

        fn object(&mut self) -> JsonValue {
            self.i += 1; // consume '{'
            let mut entries = Vec::new();
            self.ws();
            if self.b[self.i] == b'}' {
                self.i += 1;
                return JsonValue::Obj(entries);
            }
            loop {
                self.ws();
                let key = self.string();
                self.ws();
                assert_eq!(self.b[self.i], b':', "expected ':' in JSON object");
                self.i += 1;
                self.ws();
                let v = self.value();
                entries.push((key, v));
                self.ws();
                match self.b[self.i] {
                    b',' => self.i += 1,
                    b'}' => {
                        self.i += 1;
                        return JsonValue::Obj(entries);
                    }
                    c => panic!("unexpected byte {:?} in JSON object", c as char),
                }
            }
        }

        fn array(&mut self) -> JsonValue {
            self.i += 1; // consume '['
            let mut items = Vec::new();
            self.ws();
            if self.b[self.i] == b']' {
                self.i += 1;
                return JsonValue::Arr(items);
            }
            loop {
                self.ws();
                items.push(self.value());
                self.ws();
                match self.b[self.i] {
                    b',' => self.i += 1,
                    b']' => {
                        self.i += 1;
                        return JsonValue::Arr(items);
                    }
                    c => panic!("unexpected byte {:?} in JSON array", c as char),
                }
            }
        }
    }

    fn obj_entries<'v>(v: &'v JsonValue, what: &str) -> &'v [(String, JsonValue)] {
        match v {
            JsonValue::Obj(entries) => entries,
            _ => panic!("expected JSON object for {what}"),
        }
    }

    fn arr_items<'v>(v: &'v JsonValue, what: &str) -> &'v [JsonValue] {
        match v {
            JsonValue::Arr(items) => items,
            _ => panic!("expected JSON array for {what}"),
        }
    }

    fn str_value(v: &JsonValue, what: &str) -> String {
        match v {
            // Test fixtures are ASCII-only, so this cannot fail.
            JsonValue::Str(s) => String::from_utf8(s.clone()).expect("ASCII test fixture"),
            _ => panic!("expected JSON string for {what}"),
        }
    }

    fn u64_value(v: &JsonValue, what: &str) -> u64 {
        match v {
            JsonValue::Int(i) if *i >= 0 => *i as u64,
            _ => panic!("expected non-negative JSON integer for {what}"),
        }
    }

    fn i64_value(v: &JsonValue, what: &str) -> i64 {
        match v {
            JsonValue::Int(i) => *i,
            _ => panic!("expected JSON integer for {what}"),
        }
    }

    fn f64_value(v: &JsonValue, what: &str) -> f64 {
        match v {
            JsonValue::Int(i) => *i as f64,
            JsonValue::Double(f) => *f,
            _ => panic!("expected JSON number for {what}"),
        }
    }

    fn bool_json_value(v: &JsonValue, what: &str) -> bool {
        match v {
            JsonValue::Bool(b) => *b,
            _ => panic!("expected JSON bool for {what}"),
        }
    }

    fn resource_logs_vec_from_json(src: &str) -> Vec<ResourceLogs> {
        arr_items(&parse_json(src), "resourceLogs list")
            .iter()
            .map(resource_logs_from_json)
            .collect()
    }

    fn resource_logs_from_json(v: &JsonValue) -> ResourceLogs {
        let mut rl = ResourceLogs::default();
        for (k, val) in obj_entries(v, "resourceLogs") {
            match k.as_str() {
                "resource" => rl.resource = resource_from_json(val),
                "scopeLogs" => {
                    rl.scope_logs = arr_items(val, "scopeLogs")
                        .iter()
                        .map(scope_logs_from_json)
                        .collect();
                }
                _ => panic!("unknown resourceLogs field {k:?}"),
            }
        }
        rl
    }

    fn resource_from_json(v: &JsonValue) -> Resource {
        let mut r = Resource::default();
        for (k, val) in obj_entries(v, "resource") {
            match k.as_str() {
                "attributes" => {
                    r.attributes = arr_items(val, "attributes")
                        .iter()
                        .map(key_value_from_json)
                        .collect();
                }
                _ => panic!("unknown resource field {k:?}"),
            }
        }
        r
    }

    fn key_value_from_json(v: &JsonValue) -> KeyValue {
        let mut kv = KeyValue::default();
        for (k, val) in obj_entries(v, "keyValue") {
            match k.as_str() {
                "key" => kv.key = str_value(val, "key"),
                "value" => kv.value = Some(any_value_from_json(val)),
                _ => panic!("unknown keyValue field {k:?}"),
            }
        }
        kv
    }

    fn any_value_from_json(v: &JsonValue) -> AnyValue {
        let mut av = AnyValue::default();
        for (k, val) in obj_entries(v, "anyValue") {
            match k.as_str() {
                "stringValue" => av.string_value = Some(str_value(val, "stringValue")),
                "boolValue" => av.bool_value = Some(bool_json_value(val, "boolValue")),
                "intValue" => av.int_value = Some(i64_value(val, "intValue")),
                "doubleValue" => av.double_value = Some(f64_value(val, "doubleValue")),
                "arrayValue" => av.array_value = Some(array_value_from_json(val)),
                "keyValueList" => av.key_value_list = Some(key_value_list_from_json(val)),
                "bytesValue" => {
                    av.bytes_value = Some(base64_decode(&str_value(val, "bytesValue")));
                }
                _ => panic!("unknown anyValue field {k:?}"),
            }
        }
        av
    }

    fn array_value_from_json(v: &JsonValue) -> ArrayValue {
        let mut av = ArrayValue::default();
        for (k, val) in obj_entries(v, "arrayValue") {
            match k.as_str() {
                "values" => {
                    av.values = arr_items(val, "values")
                        .iter()
                        .map(|item| match item {
                            JsonValue::Null => None,
                            _ => Some(any_value_from_json(item)),
                        })
                        .collect();
                }
                _ => panic!("unknown arrayValue field {k:?}"),
            }
        }
        av
    }

    fn key_value_list_from_json(v: &JsonValue) -> KeyValueList {
        let mut kvl = KeyValueList::default();
        for (k, val) in obj_entries(v, "keyValueList") {
            match k.as_str() {
                "values" => {
                    kvl.values = arr_items(val, "values")
                        .iter()
                        .map(key_value_from_json)
                        .collect();
                }
                _ => panic!("unknown keyValueList field {k:?}"),
            }
        }
        kvl
    }

    fn scope_logs_from_json(v: &JsonValue) -> ScopeLogs {
        let mut sl = ScopeLogs::default();
        for (k, val) in obj_entries(v, "scopeLogs") {
            match k.as_str() {
                "scope" => sl.scope = Some(instrumentation_scope_from_json(val)),
                "logRecords" => {
                    sl.log_records = arr_items(val, "logRecords")
                        .iter()
                        .map(log_record_from_json)
                        .collect();
                }
                _ => panic!("unknown scopeLogs field {k:?}"),
            }
        }
        sl
    }

    fn instrumentation_scope_from_json(v: &JsonValue) -> InstrumentationScope {
        let mut s = InstrumentationScope::default();
        for (k, val) in obj_entries(v, "scope") {
            match k.as_str() {
                "name" => s.name = str_value(val, "name"),
                "version" => s.version = str_value(val, "version"),
                "attributes" => {
                    s.attributes = arr_items(val, "attributes")
                        .iter()
                        .map(key_value_from_json)
                        .collect();
                }
                _ => panic!("unknown scope field {k:?}"),
            }
        }
        s
    }

    fn log_record_from_json(v: &JsonValue) -> LogRecord {
        let mut lr = LogRecord::default();
        for (k, val) in obj_entries(v, "logRecord") {
            match k.as_str() {
                "timeUnixNano" => lr.time_unix_nano = u64_value(val, "timeUnixNano"),
                "observedTimeUnixNano" => {
                    lr.observed_time_unix_nano = u64_value(val, "observedTimeUnixNano");
                }
                "severityNumber" => {
                    lr.severity_number = i64_value(val, "severityNumber") as i32;
                }
                "severityText" => lr.severity_text = str_value(val, "severityText"),
                "body" => lr.body = any_value_from_json(val),
                "attributes" => {
                    lr.attributes = arr_items(val, "attributes")
                        .iter()
                        .map(key_value_from_json)
                        .collect();
                }
                "traceID" => lr.trace_id = str_value(val, "traceID"),
                "spanID" => lr.span_id = str_value(val, "spanID"),
                "eventName" => lr.event_name = str_value(val, "eventName"),
                _ => panic!("unknown logRecord field {k:?}"),
            }
        }
        lr
    }

    // -----------------------------------------------------------------
    // TestPushProtobufRequest
    // -----------------------------------------------------------------

    #[test]
    fn test_push_protobuf_request() {
        fn f(src: &str, timestamps_expected: &[i64], result_expected: &str) {
            let rls = resource_logs_vec_from_json(src);

            let lr = LogsData { resource_logs: rls };

            let mut p_data = Vec::new();
            lr.marshal_protobuf(&mut p_data);
            let mut tlp = TestLogMessageProcessor::default();
            if let Err(err) = push_protobuf_request(&p_data, &mut tlp, &[], false) {
                panic!("unexpected error when parsing protobuf data: {err}");
            }

            if let Err(err) = tlp.verify(timestamps_expected, result_expected) {
                panic!("{err}");
            }
        }

        // single line without resource attributes
        let data = r#"[{
		"scopeLogs": [{
			"logRecords": [{
				"timeUnixNano": 1234,
				"severityNumber": 1,
				"body": {
					"stringValue": "log-line-message"
				}
			}]
		}]
	}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected =
            r#"{"_msg":"log-line-message","severity_number":"1","severity_text":"Trace"}"#;
        f(data, timestamps_expected, results_expected);

        // single line with scope attributes
        let data = r#"[{
		"scopeLogs": [{
			"scope": {
				"name": "foo",
				"version": "v1.234.5",
				"attributes": [
					{"key":"abc","value":{"stringValue":"de"}},
					{"key":"x","value":{"stringValue":"aaa"}}
				]
			},
			"logRecords": [{
				"timeUnixNano": 1234,
				"severityNumber": 1,
				"body": {
					"stringValue": "log-line-message"
				}
			}]
		}]
	}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected = r#"{"scope.name":"foo","scope.version":"v1.234.5","scope.attributes.abc":"de","scope.attributes.x":"aaa","_msg":"log-line-message","severity_number":"1","severity_text":"Trace"}"#;
        f(data, timestamps_expected, results_expected);

        // severities mapping
        let data = r#"[{
		"scopeLogs": [{
			"logRecords": [
				{"timeUnixNano":1234,"severityNumber":1,"body":{"stringValue":"log-line-message"}},
				{"timeUnixNano":1235,"severityNumber":13,"body":{"stringValue":"log-line-message"}},
				{"timeUnixNano":1236,"severityNumber":24,"body":{"stringValue":"log-line-message"}}
			]
		}]
	}]"#;
        let timestamps_expected: &[i64] = &[1234, 1235, 1236];
        let results_expected = r#"{"_msg":"log-line-message","severity_number":"1","severity_text":"Trace"}
{"_msg":"log-line-message","severity_number":"13","severity_text":"Warn"}
{"_msg":"log-line-message","severity_number":"24","severity_text":"Fatal4"}"#;
        f(data, timestamps_expected, results_expected);

        // multi-line with resource attributes
        let data = r#"[{
		"resource": {
			"attributes": [
				{"key":"logger","value":{"stringValue":"context"}},
				{"key":"instance_id","value":{"intValue":10}},
				{"key":"","value":{"stringValue":"missing-key"}},
				{"key":"missing-value","value":{"stringValue":""}},
				{"key":"node_taints","value":{"keyValueList":{"values":
					[{"key":"role","value":{"stringValue":"dev"}},{"key":"cluster_load_percent","value":{"doubleValue":0.55}}]
				}}}
			]
		},
		"scopeLogs": [{
			"scope": {
				"name": "foo",
				"attributes": [
					{"key":"x","value":{"stringValue":"aaa"}}
				]
			},
			"logRecords": [
				{"timeUnixNano":1234,"severityNumber":1,"body":{"intValue":833}},
				{"timeUnixNano":1235,"severityNumber":25,"body":{"stringValue":"log-line-message-msg-2"}},
				{"timeUnixNano":1236,"severityNumber":-1,"body":{"stringValue":"log-line-message-msg-3"}},
				{"timeUnixNano":1237,"eventName":"abc","body":{"intValue":10}}
			]
		}]
	}]"#;
        let timestamps_expected: &[i64] = &[1234, 1235, 1236, 1237];
        let results_expected = r#"{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","scope.name":"foo","scope.version":"unknown","scope.attributes.x":"aaa","_msg":"833","severity_number":"1","severity_text":"Trace"}
{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","scope.name":"foo","scope.version":"unknown","scope.attributes.x":"aaa","_msg":"log-line-message-msg-2","severity_number":"25","severity_text":"Unspecified"}
{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","scope.name":"foo","scope.version":"unknown","scope.attributes.x":"aaa","_msg":"log-line-message-msg-3","severity_number":"-1","severity_text":"Unspecified"}
{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","event_name":"abc","scope.name":"foo","scope.version":"unknown","scope.attributes.x":"aaa","_msg":"10","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // multi-scope with resource attributes and multi-line
        let data = r#"[{
		"resource": {
			"attributes": [
				{"key":"logger","value":{"stringValue":"context"}},
				{"key":"instance_id","value":{"intValue":10}},
				{"key":"node_taints","value":{"keyValueList":{"values":[
					{"key":"role","value":{"stringValue":"dev"}},
					{"key":"cluster_load_percent","value":{"doubleValue":0.55}}
				]}}}
			]
		},
		"scopeLogs": [{
			"scope": {
				"attributes": [
					{"key":"abc","value":{"stringValue":"de"}}
				]
			},
			"logRecords": [
				{"timeUnixNano":1234,"severityNumber":1,"body":{"stringValue":"log-line-message"}},
				{"timeUnixNano":1235,"severityNumber":5,"body":{"stringValue":"log-line-message-msg-2"}}
			]
		}]
	},{
		"scopeLogs": [
			{
				"logRecords": [
					{"timeUnixNano":2345,"severityNumber":10,"body":{"stringValue":"log-line-resource-scope-1-0-0"}},
					{"timeUnixNano":2346,"severityNumber":10,"body":{"stringValue":"log-line-resource-scope-1-0-1"}}
				]
			},{
				"logRecords": [
					{"timeUnixNano":2347,"severityNumber":12,"body":{"stringValue":"log-line-resource-scope-1-1-0"}},
					{"observedTimeUnixNano":2348,"severityNumber":12,"body":{"stringValue":"log-line-resource-scope-1-1-1"},"traceID":"1234","spanID":"45"},
					{"observedTimeUnixNano":3333,"body":{"stringValue":"log-line-resource-scope-1-1-2"},"traceID":"4bf92f3577b34da6a3ce929d0e0e4736","spanID":"00f067aa0ba902b7"},
					{"timeUnixNano":432,"body":{"stringValue":"abcd"},"eventName":"foobar"}
				]
			}
		]
	}]"#;
        let timestamps_expected: &[i64] = &[1234, 1235, 2345, 2346, 2347, 2348, 3333, 432];
        let results_expected = r#"{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","scope.name":"unknown","scope.version":"unknown","scope.attributes.abc":"de","_msg":"log-line-message","severity_number":"1","severity_text":"Trace"}
{"logger":"context","instance_id":"10","node_taints.role":"dev","node_taints.cluster_load_percent":"0.55","scope.name":"unknown","scope.version":"unknown","scope.attributes.abc":"de","_msg":"log-line-message-msg-2","severity_number":"5","severity_text":"Debug"}
{"_msg":"log-line-resource-scope-1-0-0","severity_number":"10","severity_text":"Info2"}
{"_msg":"log-line-resource-scope-1-0-1","severity_number":"10","severity_text":"Info2"}
{"_msg":"log-line-resource-scope-1-1-0","severity_number":"12","severity_text":"Info4"}
{"_msg":"log-line-resource-scope-1-1-1","trace_id":"1234","span_id":"45","severity_number":"12","severity_text":"Info4"}
{"_msg":"log-line-resource-scope-1-1-2","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7","severity_number":"0","severity_text":"Unspecified"}
{"event_name":"foobar","_msg":"abcd","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // nested fields
        let data = r#"[{
		"scopeLogs": [{
			"logRecords": [{
				"timeUnixNano": 1234,
				"body": {
					"stringValue": "nested fields"
				},
				"attributes": [{
					"key": "error",
					"value": {
						"keyValueList": {
							"values": [{
								"key": "type",
								"value": {
									"stringValue": "document_parsing_exception"
								}
							}, {
								"key": "missing-value",
								"value": {
									"stringValue": ""
								}
							}, {
								"key": "",
								"value": {
									"stringValue": "missing-key"
								}
							}, {
								"key": "reason",
								"value": {
									"stringValue": "failed to parse field [_msg] of type [text]"
								}
							}, {
								"key": "caused_by",
								"value": {
									"keyValueList": {
										"values": [{
											"key": "type",
											"value": {
												"stringValue": "x_content_parse_exception"
											}
										}, {
											"key": "reason",
											"value": {
												"stringValue": "unexpected end-of-input in VALUE_STRING"
											}
										}, {
											"key": "caused_by",
											"value": {
												"keyValueList": {
													"values": [{
														"key": "type",
														"value": {
															"stringValue": "json_e_o_f_exception"
														}
													}, {
														"key": "reason",
														"value": {
															"stringValue": "eof"
														}
													}]
												}
											}
										}]
									}
								}
							}]
						}
					}
				}]
			}]
		}]
	}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected = concat!(
            r#"{"_msg":"nested fields","error.type":"document_parsing_exception","error.reason":"failed to parse field [_msg] of type [text]","#,
            r#""error.caused_by.type":"x_content_parse_exception","error.caused_by.reason":"unexpected end-of-input in VALUE_STRING","#,
            r#""error.caused_by.caused_by.type":"json_e_o_f_exception","error.caused_by.caused_by.reason":"eof","severity_number":"0","severity_text":"Unspecified"}"#
        );
        f(data, timestamps_expected, results_expected);

        // decode BytesValue
        let data = r#"[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"bytesValue":"Zm9vIGJhcg=="}}]}]}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected =
            r#"{"_msg":"Zm9vIGJhcg==","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // decode KeyValueList
        let data = r#"[{
		"scopeLogs": [{
			"logRecords": [{
				"timeUnixNano": 1234,
				"body": {
					"keyValueList": {
						"values": [{
							"key": "foo",
							"value": {
								"stringValue": "bar"
							}
						}, {
							"key": "bar",
							"value": {
								"stringValue": "buz"
							}
						}]
					}
				}
			}]
		}]
	}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected =
            r#"{"foo":"bar","bar":"buz","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // decode BoolValue
        let data =
            r#"[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"boolValue":true}}]}]}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected =
            r#"{"_msg":"true","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // decode StringValue of ArrayValue
        let data = r#"[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"stringValue":"foo bar"}]}}}]}]}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected =
            r#"{"_msg":"[\"foo bar\"]","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // decode ArrayValue of ArrayValue
        let data = r#"[{
		"scopeLogs": [{
			"logRecords": [{
				"timeUnixNano": 1234,
				"body": {
					"arrayValue": {
						"values": [{
							"arrayValue": {
								"values": [{
									"stringValue": "foo"
								}]
							}
						}, {
							"arrayValue": {
								"values": [{
									"stringValue": "bar"
								}]
							}
						}, {
							"arrayValue": {
								"values": [{
									"stringValue": "buz"
								}]
							}
						}]
					}
				}
			}]
		}]
	}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected = r#"{"_msg":"[[\"foo\"],[\"bar\"],[\"buz\"]]","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // decode BoolValue of ArrayValue
        let data = r#"[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"boolValue":true}]}}}]}]}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected =
            r#"{"_msg":"[true]","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // decode IntValue of ArrayValue
        let data = r#"[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"intValue":123}]}}}]}]}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected =
            r#"{"_msg":"[123]","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // decode DoubleValue of ArrayValue
        let data = r#"[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"doubleValue":123.45}]}}}]}]}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected =
            r#"{"_msg":"[123.45]","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // decode KeyValueList of ArrayValue
        let data = r#"[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"keyValueList":{"values":[{"key":"foo","value":{"stringValue":"bar"}}]}}]}}}]}]}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected =
            r#"{"_msg":"[{\"foo\":\"bar\"}]","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // decode bytes of ArrayValue
        let data = r#"[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[{"bytesValue":"Zm9vIGJhcg=="}]}}}]}]}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected =
            r#"{"_msg":"[\"Zm9vIGJhcg==\"]","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);

        // decode null in ArrayValue
        let data = r#"[{"scopeLogs":[{"logRecords":[{"timeUnixNano":1234,"body":{"arrayValue":{"values":[null,{}]}}}]}]}]"#;
        let timestamps_expected: &[i64] = &[1234];
        let results_expected =
            r#"{"_msg":"[null,null]","severity_number":"0","severity_text":"Unspecified"}"#;
        f(data, timestamps_expected, results_expected);
    }

    // -----------------------------------------------------------------
    // Helper tests (no Go counterparts; they cover the fmt_buffer helpers
    // that replace stdlib encoding/{hex,base64} and strconv)
    // -----------------------------------------------------------------

    #[test]
    fn test_format_base64() {
        assert_eq!(format_base64(b""), "");
        assert_eq!(format_base64(b"f"), "Zg==");
        assert_eq!(format_base64(b"fo"), "Zm8=");
        assert_eq!(format_base64(b"foo"), "Zm9v");
        assert_eq!(format_base64(b"foo bar"), "Zm9vIGJhcg==");
        assert_eq!(base64_decode("Zm9vIGJhcg=="), b"foo bar");
        assert_eq!(
            base64_decode(&format_base64(&[0xff, 0x00, 0x7f, 0x80])),
            [0xff, 0x00, 0x7f, 0x80]
        );
    }

    #[test]
    fn test_format_hex() {
        assert_eq!(format_hex(&[]), "");
        assert_eq!(format_hex(&[0x12, 0x34]), "1234");
        assert_eq!(format_hex(&[0x00, 0xff, 0x0a]), "00ff0a");
        assert_eq!(hex_decode("4bf92f").unwrap(), [0x4b, 0xf9, 0x2f]);
        assert_eq!(hex_decode("zz"), None);
        assert_eq!(hex_decode("123"), None);
    }

    #[test]
    fn test_format_severity() {
        assert_eq!(format_severity(-1), "Unspecified");
        assert_eq!(format_severity(0), "Unspecified");
        assert_eq!(format_severity(1), "Trace");
        assert_eq!(format_severity(24), "Fatal4");
        assert_eq!(format_severity(25), "Unspecified");
    }

    #[test]
    fn test_format_sub_field_name() {
        assert_eq!(format_sub_field_name("", "key"), "key");
        assert_eq!(
            format_sub_field_name("scope.attributes", "k"),
            "scope.attributes.k"
        );
    }
}

//! Port of EsLogs `app/eslinsert/datadog/datadog.go`.
//!
//! Handles the DataDog logs intake protocol: a JSON array of log records
//! POSTed to `/api/v2/logs` (or the `/insert/datadog/`-prefixed alias), with
//! `ddtags`/`ddsource` handling and the `/api/v1/validate` probe.
//!
//! Go enforces `-datadog.maxRequestSize` via
//! `protoparserutil.ReadUncompressedData` while streaming the body; the port
//! uses [`Request::read_full_body_limited`], which caps the decompressed size
//! during the read. The gzip/deflate/zstd/snappy `Content-Encoding`s Go
//! supports are decompressed transparently by [`Request::body_reader`] in
//! `esl_common::httpserver`.
//!
//! PORT NOTE: Go parses JSON via the vendored `valyala/fastjson`. esl-insert
//! only depends on `esl-common`/`esl-logstorage`, so a small dependency-free
//! JSON value parser lives in the [`json`] submodule below (same approach as
//! `loki.rs`).

use std::sync::{Arc, LazyLock};
use std::time::Instant;

use esl_common::flagutil::{ArrayString, Bytes, Flag};
use esl_common::httpserver::{Request, ResponseWriter};
use esl_common::metrics::{Counter, Summary};
use esl_common::writeconcurrencylimiter;

use esl_logstorage::rows::Field;
use esl_logstorage::stream_tags::check_stream_field_names;

use crate::common_params::{
    LogMessageProcessorTrait, LogRowsStorage, errorf_with_status, get_common_params, now_unix_nanos,
};

static DATADOG_STREAM_FIELDS: Flag<ArrayString> = Flag::new(
    "datadog.streamFields",
    "Comma-separated list of fields to use as log stream fields for logs ingested via DataDog protocol. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/datadog-agent/#stream-fields",
    ArrayString::default,
);
static DATADOG_IGNORE_FIELDS: Flag<ArrayString> = Flag::new(
    "datadog.ignoreFields",
    "Comma-separated list of fields to ignore for logs ingested via DataDog protocol. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/datadog-agent/#dropping-fields",
    ArrayString::default,
);
static MAX_REQUEST_SIZE: Flag<Bytes> = Flag::new(
    "datadog.maxRequestSize",
    "The maximum size in bytes of a single DataDog request",
    || Bytes::with_default(64 * 1024 * 1024),
);

/// RequestHandler processes Datadog insert requests. Returns true if the path
/// was handled.
pub fn request_handler<S: LogRowsStorage>(
    storage: &Arc<S>,
    path: &str,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    match path {
        "/api/v1/validate" | "/insert/datadog/api/v1/validate" => {
            w.write_str("{}");
            true
        }
        "/api/v2/logs" | "/insert/datadog/api/v2/logs" => {
            datadog_logs_ingestion(storage, req, w);
            true
        }
        _ => false,
    }
}

static V2_LOGS_REQUESTS_TOTAL: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::new_counter(
        r#"esl_http_requests_total{path="/insert/datadog/api/v2/logs"}"#,
    )
});
static V2_LOGS_REQUEST_DURATION: LazyLock<Arc<Summary>> = LazyLock::new(|| {
    esl_common::metrics::new_summary(
        r#"esl_http_request_duration_seconds{path="/insert/datadog/api/v2/logs"}"#,
    )
});

fn datadog_logs_ingestion<S: LogRowsStorage>(
    storage: &Arc<S>,
    req: &mut Request,
    w: &mut ResponseWriter,
) {
    let start_time = Instant::now();
    V2_LOGS_REQUESTS_TOTAL.inc();
    w.set_header("Content-Type", "application/json");

    let ts_value = req.header("dd-message-timestamp").to_string();
    let ts = if !ts_value.is_empty() && ts_value != "0" {
        match ts_value.parse::<i64>() {
            // The dd-message-timestamp header carries milliseconds.
            Ok(ts) => ts * 1_000_000,
            Err(err) => {
                w.errorf(
                    req,
                    &format!("could not parse dd-message-timestamp header value: {err}"),
                );
                return;
            }
        }
    } else {
        now_unix_nanos()
    };

    let mut cp = match get_common_params(req) {
        Ok(cp) => cp,
        Err(err) => {
            w.errorf(req, &err);
            return;
        }
    };

    if cp.stream_fields.is_empty() {
        let stream_fields = DATADOG_STREAM_FIELDS.get();
        let refs: Vec<&str> = stream_fields.iter().map(String::as_str).collect();
        if let Err(err) = check_stream_field_names(&refs) {
            w.errorf(
                req,
                &format!(
                    "invalid stream field names at -datadog.streamFields={stream_fields}: {err}"
                ),
            );
            return;
        }
        cp.stream_fields = stream_fields.0.clone();
    }

    if cp.ignore_fields.is_empty() {
        cp.ignore_fields = DATADOG_IGNORE_FIELDS.get().0.clone();
    }

    if let Err((msg, status)) = storage.can_write_data() {
        errorf_with_status(w, req, &msg, status);
        return;
    }

    // Go streams the body via protoparserutil.ReadUncompressedData, which
    // takes a writeconcurrencylimiter token for the read+parse and enforces
    // -datadog.maxRequestSize while reading; the port takes the token here and
    // caps the decompressed size during the read via read_full_body_limited.
    let _concurrency_guard = match writeconcurrencylimiter::inc_concurrency_guard() {
        Ok(g) => g,
        Err(err) => {
            errorf_with_status(
                w,
                req,
                &format!("cannot read DataDog protocol data: {}", err.err),
                err.status_code,
            );
            return;
        }
    };
    let data = match req.read_full_body_limited(
        MAX_REQUEST_SIZE.get().int_n() as i64,
        MAX_REQUEST_SIZE.name(),
    ) {
        Ok(d) => d,
        Err(err) => {
            w.errorf(req, &format!("cannot read DataDog protocol data: {err}"));
            return;
        }
    };

    let mut lmp = cp.new_log_message_processor(storage, "datadog");
    let res = read_logs_request(ts, &data, &mut lmp);
    lmp.close();

    if let Err(err) = res {
        w.errorf(req, &format!("cannot read DataDog protocol data: {err}"));
        return;
    }

    V2_LOGS_REQUEST_DURATION.update_duration(start_time);
    w.set_status(202);
    w.write_str("{}");
}

// datadog message field has two formats:
//   - regular log message with string text
//   - nested json format for serverless plugins
//     which has the following format:
//     {"message": {"message": "text","lamdba": {"arn": "string","requestID": "string"}, "timestamp": int64} }
//
// See https://github.com/DataDog/datadog-lambda-extension/blob/28b90c7e4e985b72d60b5f5a5147c69c7ac693c4/bottlecap/src/logs/lambda/mod.rs#L24
fn append_msg_fields(fields: &mut Vec<Field>, v: &json::Value) -> Result<(), String> {
    match v {
        json::Value::Str(val) => {
            fields.push(Field {
                name: "_msg".to_string(),
                value: val.as_bytes().to_vec(),
            });
        }
        json::Value::Obj(obj) => {
            for (k, v) in obj {
                match k.as_str() {
                    "message" => {
                        // Go's GetStringBytes yields "" for non-string values.
                        let val = v.as_str().unwrap_or("");
                        fields.push(Field {
                            name: "_msg".to_string(),
                            value: val.as_bytes().to_vec(),
                        });
                    }
                    "status" => {
                        let val = v.as_str().unwrap_or("");
                        fields.push(Field {
                            name: "status".to_string(),
                            value: val.as_bytes().to_vec(),
                        });
                    }
                    "lamdba" => {
                        let obj = v.as_object().ok_or_else(|| {
                            format!(
                                "unexpected lambda value type for {k:?}:{:?}; want object",
                                v.to_json_text()
                            )
                        })?;
                        for (k, v) in obj {
                            let val = v.as_str().ok_or_else(|| {
                                format!(
                                    "unexpected lambda label value type for {k:?}:{:?}; want string",
                                    v.to_json_text()
                                )
                            })?;
                            fields.push(Field {
                                name: k.clone(),
                                value: val.as_bytes().to_vec(),
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {
            return Err(format!("unsupported message type {:?}", v.type_name()));
        }
    }
    Ok(())
}

// readLogsRequest parses data according to DataDog logs format
// https://docs.datadoghq.com/api/latest/logs/#send-logs
fn read_logs_request(
    ts: i64,
    data: &[u8],
    lmp: &mut dyn LogMessageProcessorTrait,
) -> Result<(), String> {
    let mut ts = ts;
    let v = json::parse(data).map_err(|err| format!("cannot parse JSON request body: {err}"))?;
    let records = v
        .as_array()
        .ok_or_else(|| "cannot extract array from parsed JSON".to_string())?;

    let mut fields: Vec<Field> = Vec::new();
    for r in records {
        let o = r
            .as_object()
            .ok_or_else(|| "could not extract log record".to_string())?;
        for (k, v) in o {
            match k.as_str() {
                "message" => {
                    append_msg_fields(&mut fields, v)?;
                }
                "timestamp" => {
                    let val = v.as_i64().ok_or_else(|| {
                        format!("failed to parse timestamp for {k:?}:{:?}", v.to_json_text())
                    })?;
                    if val > 0 {
                        // The record timestamp carries milliseconds.
                        ts = val * 1_000_000;
                    }
                }
                "ddtags" => {
                    // https://docs.datadoghq.com/getting_started/tagging/
                    let val = v.as_str().ok_or_else(|| {
                        format!(
                            "unexpected label value type for {k:?}:{:?}; want string",
                            v.to_json_text()
                        )
                    })?;
                    let mut val = val;
                    loop {
                        let idx = val.find(',');
                        let pair = match idx {
                            Some(i) => {
                                let pair = &val[..i];
                                val = &val[i + 1..];
                                pair
                            }
                            None => val,
                        };
                        if !pair.is_empty() {
                            match pair.find(':') {
                                None => {
                                    // No tag value.
                                    fields.push(Field {
                                        name: pair.to_string(),
                                        value: b"no_label_value".to_vec(),
                                    });
                                }
                                Some(n) => {
                                    fields.push(Field {
                                        name: pair[..n].to_string(),
                                        value: pair.as_bytes()[n + 1..].to_vec(),
                                    });
                                }
                            }
                        }
                        if idx.is_none() {
                            break;
                        }
                    }
                }
                _ => {
                    let val = v.as_str().ok_or_else(|| {
                        format!(
                            "unexpected label value type for {k:?}:{:?}; want string",
                            v.to_json_text()
                        )
                    })?;
                    fields.push(Field {
                        name: k.clone(),
                        value: val.as_bytes().to_vec(),
                    });
                }
            }
        }
        lmp.add_row(ts, &mut fields, -1);
        fields.clear();
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Minimal JSON value parser
// ---------------------------------------------------------------------------

/// A small dependency-free JSON parser covering the DataDog logs request shape.
///
/// PORT NOTE: replaces the vendored `valyala/fastjson` used by the Go source
/// (same approach as the private parser in `loki.rs`; the two modules cannot
/// share it without touching files outside this port's scope).
mod json {
    use esl_common::stringsutil::json_string;

    pub enum Value {
        Str(String),
        /// number / `true` / `false` / `null`, kept as raw source text.
        Raw(String),
        Arr(Vec<Value>),
        Obj(Vec<(String, Value)>),
    }

    impl Value {
        pub fn as_str(&self) -> Option<&str> {
            match self {
                Value::Str(s) => Some(s),
                _ => None,
            }
        }

        pub fn as_i64(&self) -> Option<i64> {
            match self {
                Value::Raw(r) => r.parse::<i64>().ok(),
                _ => None,
            }
        }

        pub fn as_array(&self) -> Option<&[Value]> {
            match self {
                Value::Arr(a) => Some(a),
                _ => None,
            }
        }

        pub fn as_object(&self) -> Option<&[(String, Value)]> {
            match self {
                Value::Obj(o) => Some(o),
                _ => None,
            }
        }

        /// Mirrors Go `fastjson.Type.String()` for error messages.
        pub fn type_name(&self) -> &'static str {
            match self {
                Value::Str(_) => "string",
                Value::Arr(_) => "array",
                Value::Obj(_) => "object",
                Value::Raw(r) => match r.as_str() {
                    "true" => "true",
                    "false" => "false",
                    "null" => "null",
                    _ => "number",
                },
            }
        }

        /// The compact JSON text of the value, for error messages.
        pub fn to_json_text(&self) -> String {
            let mut out = String::new();
            self.write_json(&mut out);
            out
        }

        fn write_json(&self, out: &mut String) {
            match self {
                Value::Str(s) => out.push_str(&json_string(s)),
                Value::Raw(r) => out.push_str(r),
                Value::Arr(a) => {
                    out.push('[');
                    for (i, v) in a.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        v.write_json(out);
                    }
                    out.push(']');
                }
                Value::Obj(o) => {
                    out.push('{');
                    for (i, (k, v)) in o.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        out.push_str(&json_string(k));
                        out.push(':');
                        v.write_json(out);
                    }
                    out.push('}');
                }
            }
        }
    }

    pub fn parse(data: &[u8]) -> Result<Value, String> {
        let mut p = Parser { b: data, i: 0 };
        p.ws();
        let v = p.value()?;
        p.ws();
        if p.i != p.b.len() {
            return Err("trailing data after top-level JSON value".to_string());
        }
        Ok(v)
    }

    struct Parser<'a> {
        b: &'a [u8],
        i: usize,
    }

    impl Parser<'_> {
        fn ws(&mut self) {
            while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
                self.i += 1;
            }
        }

        fn value(&mut self) -> Result<Value, String> {
            if self.i >= self.b.len() {
                return Err("unexpected end of JSON input".to_string());
            }
            match self.b[self.i] {
                b'"' => Ok(Value::Str(self.string()?)),
                b'{' => self.object(),
                b'[' => self.array(),
                b't' => {
                    self.literal("true")?;
                    Ok(Value::Raw("true".to_string()))
                }
                b'f' => {
                    self.literal("false")?;
                    Ok(Value::Raw("false".to_string()))
                }
                b'n' => {
                    self.literal("null")?;
                    Ok(Value::Raw("null".to_string()))
                }
                _ => self.number(),
            }
        }

        fn literal(&mut self, lit: &str) -> Result<(), String> {
            let end = self.i + lit.len();
            if end <= self.b.len() && &self.b[self.i..end] == lit.as_bytes() {
                self.i = end;
                Ok(())
            } else {
                Err(format!("expected JSON literal {lit:?}"))
            }
        }

        fn number(&mut self) -> Result<Value, String> {
            let start = self.i;
            while self.i < self.b.len()
                && matches!(
                    self.b[self.i],
                    b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'
                )
            {
                self.i += 1;
            }
            if self.i == start {
                return Err(format!("unexpected byte {:#x} in JSON", self.b[self.i]));
            }
            let raw = std::str::from_utf8(&self.b[start..self.i])
                .map_err(|_| "invalid UTF-8 in JSON number".to_string())?;
            Ok(Value::Raw(raw.to_string()))
        }

        fn string(&mut self) -> Result<String, String> {
            // self.b[self.i] == b'"'
            self.i += 1;
            let mut s = String::new();
            while self.i < self.b.len() {
                let c = self.b[self.i];
                match c {
                    b'"' => {
                        self.i += 1;
                        return Ok(s);
                    }
                    b'\\' => {
                        self.i += 1;
                        if self.i >= self.b.len() {
                            return Err("unterminated escape in JSON string".to_string());
                        }
                        match self.b[self.i] {
                            b'"' => s.push('"'),
                            b'\\' => s.push('\\'),
                            b'/' => s.push('/'),
                            b'b' => s.push('\u{0008}'),
                            b'f' => s.push('\u{000C}'),
                            b'n' => s.push('\n'),
                            b'r' => s.push('\r'),
                            b't' => s.push('\t'),
                            b'u' => {
                                let cp = self.hex4()?;
                                if (0xD800..=0xDBFF).contains(&cp) {
                                    // High surrogate; expect a low surrogate.
                                    if self.i + 2 <= self.b.len()
                                        && self.b[self.i + 1] == b'\\'
                                        && self.i + 2 < self.b.len()
                                        && self.b[self.i + 2] == b'u'
                                    {
                                        self.i += 2;
                                        let lo = self.hex4()?;
                                        let combined = 0x10000
                                            + (((cp - 0xD800) as u32) << 10)
                                            + (lo - 0xDC00) as u32;
                                        s.push(char::from_u32(combined).unwrap_or('\u{FFFD}'));
                                    } else {
                                        s.push('\u{FFFD}');
                                    }
                                } else {
                                    s.push(char::from_u32(cp as u32).unwrap_or('\u{FFFD}'));
                                }
                            }
                            other => {
                                return Err(format!(
                                    "invalid escape \\{} in JSON string",
                                    other as char
                                ));
                            }
                        }
                        self.i += 1;
                    }
                    _ => {
                        // Copy a full UTF-8 sequence.
                        let len = utf8_len(c);
                        let end = (self.i + len).min(self.b.len());
                        match std::str::from_utf8(&self.b[self.i..end]) {
                            Ok(chunk) => s.push_str(chunk),
                            Err(_) => return Err("invalid UTF-8 in JSON string".to_string()),
                        }
                        self.i = end;
                    }
                }
            }
            Err("unterminated JSON string".to_string())
        }

        fn hex4(&mut self) -> Result<u16, String> {
            // self.i points at 'u'
            let start = self.i + 1;
            let end = start + 4;
            if end > self.b.len() {
                return Err("invalid \\u escape in JSON string".to_string());
            }
            let hex = std::str::from_utf8(&self.b[start..end])
                .map_err(|_| "invalid \\u escape in JSON string".to_string())?;
            let cp = u16::from_str_radix(hex, 16)
                .map_err(|_| "invalid \\u escape in JSON string".to_string())?;
            self.i = end - 1; // leave i on the last hex digit; caller does i += 1
            Ok(cp)
        }

        fn object(&mut self) -> Result<Value, String> {
            self.i += 1; // consume '{'
            let mut obj = Vec::new();
            self.ws();
            if self.i < self.b.len() && self.b[self.i] == b'}' {
                self.i += 1;
                return Ok(Value::Obj(obj));
            }
            loop {
                self.ws();
                if self.i >= self.b.len() || self.b[self.i] != b'"' {
                    return Err("expected string key in JSON object".to_string());
                }
                let key = self.string()?;
                self.ws();
                if self.i >= self.b.len() || self.b[self.i] != b':' {
                    return Err("expected ':' in JSON object".to_string());
                }
                self.i += 1;
                self.ws();
                let val = self.value()?;
                obj.push((key, val));
                self.ws();
                if self.i >= self.b.len() {
                    return Err("unterminated JSON object".to_string());
                }
                match self.b[self.i] {
                    b',' => self.i += 1,
                    b'}' => {
                        self.i += 1;
                        return Ok(Value::Obj(obj));
                    }
                    _ => return Err("expected ',' or '}' in JSON object".to_string()),
                }
            }
        }

        fn array(&mut self) -> Result<Value, String> {
            self.i += 1; // consume '['
            let mut arr = Vec::new();
            self.ws();
            if self.i < self.b.len() && self.b[self.i] == b']' {
                self.i += 1;
                return Ok(Value::Arr(arr));
            }
            loop {
                self.ws();
                let val = self.value()?;
                arr.push(val);
                self.ws();
                if self.i >= self.b.len() {
                    return Err("unterminated JSON array".to_string());
                }
                match self.b[self.i] {
                    b',' => self.i += 1,
                    b']' => {
                        self.i += 1;
                        return Ok(Value::Arr(arr));
                    }
                    _ => return Err("expected ',' or ']' in JSON array".to_string()),
                }
            }
        }
    }

    fn utf8_len(b: u8) -> usize {
        if b < 0x80 {
            1
        } else if b >> 5 == 0b110 {
            2
        } else if b >> 4 == 0b1110 {
            3
        } else if b >> 3 == 0b11110 {
            4
        } else {
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::common_params::TestLogMessageProcessor;

    #[test]
    fn test_read_logs_request_failure() {
        fn f(data: &str) {
            let ts = now_unix_nanos();

            let mut lmp = TestLogMessageProcessor::default();
            assert!(
                read_logs_request(ts, data.as_bytes(), &mut lmp).is_err(),
                "expecting non-empty error for data {data:?}"
            );
            if let Err(err) = lmp.verify(&[], "") {
                panic!("unexpected error: {err}");
            }
        }
        f("foobar");
        f("{}");
        f(r#"["create":{}]"#);
        f("{\"create\":{}}\nfoobar");
    }

    #[test]
    fn test_read_logs_request_success() {
        fn f(data: &str, rows_expected: usize, result_expected: &str) {
            let ts = now_unix_nanos();
            let timestamps_expected = vec![ts; rows_expected];
            let mut lmp = TestLogMessageProcessor::default();
            if let Err(err) = read_logs_request(ts, data.as_bytes(), &mut lmp) {
                panic!("unexpected error: {err}");
            }
            if let Err(err) = lmp.verify(&timestamps_expected, result_expected) {
                panic!("unexpected error: {err}");
            }
        }

        // Verify non-empty data
        let data = r#"[
		{
			"ddsource":"nginx",
			"ddtags":"tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":"bar",
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":"tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":{"message": "nested"},
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":"tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":"foobar",
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":"tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":"baz",
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":"tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":"xyz",
			"service":"test"
		}, {
			"ddsource": "nginx",
			"ddtags":"tag1:value1,tag2:value2,",
			"hostname":"127.0.0.1",
			"message":"xyz",
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":",tag1:value1,tag2:value2",
			"hostname":"127.0.0.1",
			"message":"xyz",
			"service":"test"
		}, {
			"ddsource":"nginx",
			"ddtags":"env:prod,foo",
			"hostname":"127.0.0.1",
			"message":"qux",
			"service":"test"
		}
	]"#;
        let rows_expected = 8;
        let result_expected = r#"{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"bar","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"nested","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"foobar","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"baz","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"xyz","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"xyz","service":"test"}
{"ddsource":"nginx","tag1":"value1","tag2":"value2","hostname":"127.0.0.1","_msg":"xyz","service":"test"}
{"ddsource":"nginx","env":"prod","foo":"no_label_value","hostname":"127.0.0.1","_msg":"qux","service":"test"}"#;
        f(data, rows_expected, result_expected);
    }
}

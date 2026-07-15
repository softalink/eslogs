//! Port of EsLogs `app/eslinsert/loki/{loki.go,loki_json.go}`.
//!
//! Handles the Loki push protocol dispatch and its JSON variant
//! (`/insert/loki/api/v1/push` with a JSON content-type). The protobuf+snappy
//! variant (`loki_protobuf.go`, `pb.go`, `pb_marshal.go`) is ported in
//! [`crate::loki_protobuf`] and non-JSON content-types are dispatched to it,
//! mirroring Go `loki.go`.
//!
//! PORT NOTE: Go parses JSON via the vendored `valyala/fastjson`. esl-insert
//! only depends on `esl-common`/`esl-logstorage`, so a small dependency-free JSON
//! value parser lives in the [`json`] submodule below.

use std::sync::Arc;

use esl_common::flagutil::{Bytes, Flag};
use esl_common::httpserver::{Request, ResponseWriter};
use esl_common::httputil::get_request_value;
use esl_common::writeconcurrencylimiter;

use esl_logstorage::json_parser::{JSONParser, get_json_parser, put_json_parser};
use esl_logstorage::rows::{Field, rename_field};
use esl_logstorage::tenant_id::parse_tenant_id;

use esl_common::timeutil::try_parse_unix_timestamp;

use crate::common_params::{
    CommonParams, LogMessageProcessor, LogRowsStorage, errorf_with_status, get_common_params,
    is_json_content_type, now_unix_nanos,
};

/// The maximum size of a single Loki request (shared by the JSON and protobuf
/// variants, like Go's package-level `maxRequestSize` in `loki_json.go`).
pub(crate) static MAX_REQUEST_SIZE: Flag<Bytes> = Flag::new(
    "loki.maxRequestSize",
    "The maximum size in bytes of a single Loki request",
    || Bytes::with_default(64 * 1024 * 1024),
);
esl_common::register_flag!(MAX_REQUEST_SIZE);

/// RequestHandler processes Loki insert requests. Returns true if the path was
/// handled.
pub fn request_handler<S: LogRowsStorage>(
    storage: &Arc<S>,
    path: &str,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    match path {
        "/insert/loki/api/v1/push" => {
            handle_insert(storage, req, w);
            true
        }
        "/insert/loki/ready" => {
            // See https://grafana.com/docs/loki/latest/api/#identify-ready-loki-instance
            w.set_status(200);
            w.write_str("ready");
            true
        }
        _ => false,
    }
}

// See https://grafana.com/docs/loki/latest/api/#push-log-entries-to-loki
fn handle_insert<S: LogRowsStorage>(storage: &Arc<S>, req: &mut Request, w: &mut ResponseWriter) {
    let ct = req.content_type().to_string();
    if is_json_content_type(&ct) {
        handle_json(storage, req, w);
    } else {
        // Protobuf request body should be handled by default according to
        // https://grafana.com/docs/loki/latest/api/#push-log-entries-to-loki
        crate::loki_protobuf::handle_protobuf(storage, req, w);
    }
}

pub(crate) struct LokiCommonParams {
    pub(crate) cp: CommonParams,
    /// Whether to parse JSON inside a plaintext log message.
    pub(crate) parse_message: bool,
    /// Optional prefix to add to parsed message fields when `parse_message`.
    pub(crate) msg_fields_prefix: String,
}

pub(crate) fn get_loki_common_params(req: &Request) -> Result<LokiCommonParams, String> {
    let mut cp = get_common_params(req)?;

    // If the parsed tenant is (0,0) it is likely the default tenant; try the
    // Loki X-Scope-OrgID header.
    if cp.tenant_id.account_id == 0 && cp.tenant_id.project_id == 0 {
        let org = req.header("X-Scope-OrgID");
        if !org.is_empty() {
            cp.tenant_id = parse_tenant_id(org)?;
        }
    }

    // Go defaults `parseMessage` to `!*disableMessageParsing`; the flag default
    // is false, so parsing is enabled by default.
    let mut parse_message = true;
    let rv = get_request_value(
        req,
        "disable_message_parsing",
        "ESL-Loki-Disable-Message-Parsing",
    );
    if !rv.is_empty() {
        let bv = parse_bool(&rv)
            .map_err(|err| format!("cannot parse disable_message_parsing={rv:?}: {err}"))?;
        parse_message = !bv;
    }

    let mut msg_fields_prefix = String::new();
    let rv2 = get_request_value(
        req,
        "message_fields_prefix",
        "ESL-Loki-Message-Fields-Prefix",
    );
    if !rv2.is_empty() {
        msg_fields_prefix = rv2;
    }

    Ok(LokiCommonParams {
        cp,
        parse_message,
        msg_fields_prefix,
    })
}

fn handle_json<S: LogRowsStorage>(storage: &Arc<S>, req: &mut Request, w: &mut ResponseWriter) {
    let cp = match get_loki_common_params(req) {
        Ok(cp) => cp,
        Err(err) => {
            w.errorf(
                req,
                &format!("cannot parse common params from request: {err}"),
            );
            return;
        }
    };

    if let Err((msg, status)) = storage.can_write_data() {
        errorf_with_status(w, req, &msg, status);
        return;
    }

    // Go streams the body via protoparserutil.ReadUncompressedData, which
    // waits for the first body byte, takes a writeconcurrencylimiter token
    // for the read+parse and enforces -loki.maxRequestSize while reading;
    // the port takes the token here and caps the decompressed size during the
    // read via read_full_body_limited.
    let _concurrency_guard = match writeconcurrencylimiter::inc_concurrency_guard() {
        Ok(g) => g,
        Err(err) => {
            errorf_with_status(
                w,
                req,
                &format!("cannot read Loki json data: {}", err.err),
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
            w.errorf(req, &format!("cannot read Loki json data: {err}"));
            return;
        }
    };

    let use_default_stream_fields = cp.cp.stream_fields.is_empty();
    let msg_fields: Vec<&str> = cp.cp.msg_fields.iter().map(String::as_str).collect();
    let preserve_keys: Vec<&[u8]> = cp
        .cp
        .preserve_json_keys
        .iter()
        .map(|s| s.as_bytes())
        .collect();

    let mut lmp = cp.cp.new_log_message_processor(storage, "loki_json");
    let res = parse_json_request(
        &data,
        &mut lmp,
        &msg_fields,
        &preserve_keys,
        &cp.msg_fields_prefix,
        use_default_stream_fields,
        cp.parse_message,
    );
    lmp.close();

    if let Err(err) = res {
        w.errorf(req, &format!("cannot read Loki json data: {err}"));
        return;
    }

    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8505
    w.set_status(204);
}

#[allow(clippy::too_many_arguments)]
fn parse_json_request<S: LogRowsStorage>(
    data: &[u8],
    lmp: &mut LogMessageProcessor<'_, S>,
    msg_fields: &[&str],
    preserve_keys: &[&[u8]],
    msg_fields_prefix: &str,
    use_default_stream_fields: bool,
    parse_message: bool,
) -> Result<(), String> {
    let v = json::parse(data).map_err(|err| format!("cannot parse JSON request body: {err}"))?;

    let streams_v = v
        .get("streams")
        .ok_or_else(|| "missing `streams` item in the parsed JSON".to_string())?;
    let streams = streams_v
        .as_array()
        .ok_or_else(|| "`streams` item in the parsed JSON must contain an array".to_string())?;

    let mut msg_parser: Option<JSONParser> = if parse_message {
        Some(get_json_parser())
    } else {
        None
    };

    let mut fields_tmp: Vec<Field> = Vec::new();

    for stream in streams {
        // Populate common labels from the `stream` dict.
        fields_tmp.clear();
        if let Some(labels_v) = stream.get("stream") {
            let labels = labels_v.as_object().ok_or_else(|| {
                "`stream` item in the parsed JSON must contain an object".to_string()
            })?;
            for (k, val) in labels {
                fields_tmp.push(Field {
                    name: k.clone().into_bytes(),
                    value: val.marshaled().into_bytes(),
                });
            }
        }

        // Populate messages from the `values` array.
        let lines_v = stream
            .get("values")
            .ok_or_else(|| "missing `values` item in the parsed `stream` object".to_string())?;
        let lines = lines_v
            .as_array()
            .ok_or_else(|| "`values` item in the parsed JSON must contain an array".to_string())?;

        let common_fields_len = fields_tmp.len();
        for line in lines {
            fields_tmp.truncate(common_fields_len);

            let line_a = line
                .as_array()
                .ok_or_else(|| "unexpected contents of `values` item; want array".to_string())?;
            if line_a.len() < 2 || line_a.len() > 3 {
                return Err(format!(
                    "unexpected number of values in `values` item array; got {} want 2 or 3",
                    line_a.len()
                ));
            }

            // Parse timestamp.
            let timestamp = line_a[0]
                .as_str()
                .ok_or_else(|| "unexpected log timestamp type; want string".to_string())?;
            let mut ts = parse_loki_timestamp(timestamp)
                .map_err(|err| format!("cannot parse log timestamp {timestamp:?}: {err}"))?;
            if ts == 0 {
                ts = now_unix_nanos();
            }

            // Parse structured metadata.
            if line_a.len() > 2 {
                let metadata = line_a[2].as_object().ok_or_else(|| {
                    "unexpected structured metadata type; want JSON object".to_string()
                })?;
                for (k, val) in metadata {
                    fields_tmp.push(Field {
                        name: k.clone().into_bytes(),
                        value: val.marshaled().into_bytes(),
                    });
                }
            }

            // Parse the log message.
            let msg = line_a[1]
                .as_str()
                .ok_or_else(|| "unexpected log message type; want string".to_string())?;
            let allow_msg_renaming = add_msg_field(
                &mut fields_tmp,
                msg_parser.as_mut(),
                msg.as_bytes(),
                preserve_keys,
                msg_fields_prefix,
            );
            if allow_msg_renaming {
                rename_field(&mut fields_tmp[common_fields_len..], msg_fields, "_msg");
            }

            let stream_fields_len = if use_default_stream_fields {
                common_fields_len as isize
            } else {
                -1
            };

            lmp.add_row(ts, &mut fields_tmp, stream_fields_len);
        }
    }

    if let Some(p) = msg_parser.take() {
        put_json_parser(p);
    }

    Ok(())
}

// Shared with the protobuf path in `loki_protobuf.rs`, mirroring Go where
// `addMsgField` in loki_json.go is used by both variants.
pub(crate) fn add_msg_field(
    fs: &mut Vec<Field>,
    msg_parser: Option<&mut JSONParser>,
    msg_orig: &[u8],
    preserve_keys: &[&[u8]],
    msg_fields_prefix: &str,
) -> bool {
    // Log collectors can leave trailing whitespace. Go TrimSpace trims
    // Unicode whitespace; mirror it when the bytes are valid UTF-8 and fall
    // back to ASCII trimming otherwise (an invalid sequence is not a space).
    let b: &[u8] = match std::str::from_utf8(msg_orig) {
        Ok(t) => t.trim().as_bytes(),
        Err(_) => msg_orig.trim_ascii(),
    };

    match msg_parser {
        Some(p) if b.len() >= 2 && b[0] == b'{' && b[b.len() - 1] == b'}' => {
            if p.parse_log_message(b, preserve_keys, msg_fields_prefix)
                .is_ok()
            {
                fs.extend_from_slice(p.fields());
                true
            } else {
                fs.push(Field {
                    name: b"_msg".to_vec(),
                    value: msg_orig.to_vec(),
                });
                false
            }
        }
        _ => {
            fs.push(Field {
                name: b"_msg".to_vec(),
                value: msg_orig.to_vec(),
            });
            false
        }
    }
}

fn parse_loki_timestamp(s: &str) -> Result<i64, String> {
    if s.is_empty() {
        // Empty timestamp is substituted with the current time by the caller.
        return Ok(0);
    }
    match try_parse_unix_timestamp(s) {
        Some(nsecs) => Ok(nsecs),
        None => Err(format!("cannot parse unix timestamp {s:?}")),
    }
}

/// Mirrors Go `strconv.ParseBool`.
fn parse_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!("strconv.ParseBool: parsing {s:?}: invalid syntax")),
    }
}

// ---------------------------------------------------------------------------
// Minimal JSON value parser
// ---------------------------------------------------------------------------

/// A small dependency-free JSON parser covering the Loki push request shape.
///
/// PORT NOTE: replaces the vendored `valyala/fastjson` used by the Go source.
/// `getMarshaledJSONValue` is modeled by [`Value::marshaled`]: strings return
/// their decoded contents, every other value returns its compact JSON text.
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

        pub fn get(&self, key: &str) -> Option<&Value> {
            match self {
                Value::Obj(o) => o.iter().find(|(k, _)| k == key).map(|(_, v)| v),
                _ => None,
            }
        }

        /// Mirrors Go `getMarshaledJSONValue`.
        pub fn marshaled(&self) -> String {
            match self {
                Value::Str(s) => s.clone(),
                Value::Raw(r) => r.clone(),
                _ => {
                    let mut out = String::new();
                    self.write_json(&mut out);
                    out
                }
            }
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
    use crate::common_params::CommonParams;
    use crate::testutil::{open_temp_storage, rows_count};

    #[test]
    fn test_json_value_parser() {
        let v = json::parse(br#"{"a":"b","n":42,"arr":[1,"x"],"t":true,"o":{"k":"v"}}"#).unwrap();
        assert_eq!(v.get("a").unwrap().as_str(), Some("b"));
        assert_eq!(v.get("n").unwrap().marshaled(), "42");
        assert_eq!(v.get("t").unwrap().marshaled(), "true");
        let arr = v.get("arr").unwrap().as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[1].as_str(), Some("x"));
        assert_eq!(v.get("o").unwrap().marshaled(), r#"{"k":"v"}"#);
    }

    #[test]
    fn test_json_string_escapes() {
        let v = json::parse(br#"{"k":"line1\nline2\t\"q\""}"#).unwrap();
        assert_eq!(v.get("k").unwrap().as_str(), Some("line1\nline2\t\"q\""));
    }

    #[test]
    fn test_parse_json_request_lands_rows() {
        let s = open_temp_storage("loki");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");

        // Empty timestamp strings force the current time (avoids retention).
        let body = br#"{"streams":[{"stream":{"app":"foo","level":"info"},"values":[["","hello"],["","world"]]}]}"#;
        let no_fields: [&str; 0] = [];
        let no_keys: [&[u8]; 0] = [];
        let res = parse_json_request(body, &mut lmp, &no_fields, &no_keys, "", true, true);
        assert!(res.is_ok(), "unexpected error: {res:?}");

        lmp.close();
        assert_eq!(rows_count(&s), 2, "expected 2 rows ingested");
        s.must_close();
    }

    #[test]
    fn test_parse_json_request_missing_streams() {
        let s = open_temp_storage("loki-bad");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");
        let no_fields: [&str; 0] = [];
        let no_keys: [&[u8]; 0] = [];
        let res = parse_json_request(b"{}", &mut lmp, &no_fields, &no_keys, "", true, true);
        assert!(res.is_err(), "expected error for missing streams");
        lmp.close();
        s.must_close();
    }
}

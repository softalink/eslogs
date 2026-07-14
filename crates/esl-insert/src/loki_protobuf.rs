//! Port of EsLogs `app/eslinsert/loki/{pb.go,pb_marshal.go,loki_protobuf.go}`.
//!
//! Handles the Loki push protocol protobuf variant
//! (`/insert/loki/api/v1/push` with a non-JSON content-type); the dispatch
//! lives in [`crate::loki`].
//!
//! PORT NOTE: Go keeps the three files separate in the `loki` package; the
//! port keeps them as sections of this single module (see the section
//! comments below), wired to `loki.rs` via `pub(crate)` items.

use std::sync::Arc;

use esl_common::easyproto;
use esl_common::httpserver::{Request, ResponseWriter};

use esl_logstorage::json_parser::{JSONParser, get_json_parser, put_json_parser};
use esl_logstorage::rows::{Field, rename_field};

use esl_common::writeconcurrencylimiter;

use crate::common_params::{
    LogMessageProcessor, LogRowsStorage, errorf_with_status, now_unix_nanos,
};
use crate::loki::{MAX_REQUEST_SIZE, add_msg_field, get_loki_common_params};

// ---------------------------------------------------------------------------
// loki_protobuf.go
// ---------------------------------------------------------------------------

pub(crate) fn handle_protobuf<S: LogRowsStorage>(
    storage: &Arc<S>,
    req: &mut Request,
    w: &mut ResponseWriter,
) {
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
    // the port takes the token here and caps the raw body during the read via
    // read_full_body_limited.
    let _concurrency_guard = match writeconcurrencylimiter::inc_concurrency_guard() {
        Ok(g) => g,
        Err(err) => {
            errorf_with_status(
                w,
                req,
                &format!("cannot read Loki protobuf data: {}", err.err),
                err.status_code,
            );
            return;
        }
    };

    // PORT NOTE: Go reads the body via `protoparserutil.ReadUncompressedData`
    // with the Content-Encoding value, defaulting to snappy. The Rust
    // httpserver already decompresses the body per Content-Encoding, so only
    // the "no Content-Encoding means snappy" Loki default is handled here.
    // -loki.maxRequestSize caps the raw body in both cases (Go's readFull),
    // and additionally the snappy-decompressed size (Go's limited
    // snappy.Decode); for the other encodings the body reaching this point
    // was already decompressed by the httpserver, so the raw cap applies to
    // the decompressed size like Go's readFull-after-GetUncompressedReader.
    // See https://grafana.com/docs/loki/latest/reference/loki-http-api/#ingest-logs
    let encoding = req.content_encoding();
    let body = match req.read_full_body_limited(
        MAX_REQUEST_SIZE.get().int_n() as i64,
        MAX_REQUEST_SIZE.name(),
    ) {
        Ok(b) => b,
        Err(err) => {
            w.errorf(req, &format!("cannot read Loki protobuf data: {err}"));
            return;
        }
    };
    let data = if encoding.is_empty() {
        match snappy_decode_block(&body) {
            Ok(d) => d,
            Err(err) => {
                w.errorf(req, &format!("cannot read Loki protobuf data: {err}"));
                return;
            }
        }
    } else {
        body
    };

    let use_default_stream_fields = cp.cp.stream_fields.is_empty();
    let msg_fields: Vec<&str> = cp.cp.msg_fields.iter().map(String::as_str).collect();
    let preserve_keys: Vec<&str> = cp
        .cp
        .preserve_json_keys
        .iter()
        .map(String::as_str)
        .collect();

    let mut lmp = cp.cp.new_log_message_processor(storage, "loki_protobuf");
    let res = parse_protobuf_request(
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
        w.errorf(req, &format!("cannot read Loki protobuf data: {err}"));
        return;
    }

    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8505
    w.set_status(204);
}

/// Decodes a block-mode snappy body, enforcing `-loki.maxRequestSize` on the
/// decompressed size (Go: `readUncompressedData` calling the limited
/// `snappy.Decode`; error texts match `lib/encoding/snappy` wrapped with
/// "cannot decompress data:").
fn snappy_decode_block(data: &[u8]) -> Result<Vec<u8>, String> {
    let decoded_len = snap::raw::decompress_len(data)
        .map_err(|err| format!("cannot decompress data: cannot read snappy header: {err}"))?;
    let max_request_size = MAX_REQUEST_SIZE.get().int_n().max(0) as usize;
    if decoded_len > max_request_size {
        return Err(format!(
            "cannot decompress data: too big data size {decoded_len} exceeding {max_request_size} bytes"
        ));
    }
    snap::raw::Decoder::new()
        .decompress_vec(data)
        .map_err(|err| format!("cannot decompress data: {err}"))
}

#[allow(clippy::too_many_arguments)]
fn parse_protobuf_request<S: LogRowsStorage>(
    data: &[u8],
    lmp: &mut LogMessageProcessor<'_, S>,
    msg_fields: &[&str],
    preserve_keys: &[&str],
    msg_fields_prefix: &str,
    use_default_stream_fields: bool,
    parse_message: bool,
) -> Result<(), String> {
    let mut msg_parser: Option<JSONParser> = if parse_message {
        Some(get_json_parser())
    } else {
        None
    };

    let mut push_logs =
        |timestamp: i64, line: &[u8], fs: &mut Vec<Field>, stream_fields_len: usize| {
            let ts = if timestamp == 0 {
                now_unix_nanos()
            } else {
                timestamp
            };

            let allow_msg_renaming = add_msg_field(
                fs,
                msg_parser.as_mut(),
                line,
                preserve_keys,
                msg_fields_prefix,
            );
            if allow_msg_renaming {
                rename_field(&mut fs[stream_fields_len..], msg_fields, "_msg");
            }

            let stream_fields_len = if use_default_stream_fields {
                stream_fields_len as isize
            } else {
                -1
            };

            lmp.add_row(ts, fs, stream_fields_len);
        };

    let res = decode_push_request(data, &mut push_logs)
        .map_err(|err| format!("cannot decode PushRequest: {err}"));

    if let Some(p) = msg_parser.take() {
        put_json_parser(p);
    }

    res
}

// ---------------------------------------------------------------------------
// pb.go
// ---------------------------------------------------------------------------

/// decodePushRequest parses a PushRequest protobuf message from src and calls
/// the provided push_logs for each decoded log record.
///
/// The handler receives `(timestamp, line, fields, stream_fields_len)` and may
/// append additional fields; they are truncated away before the next record.
///
/// See <https://github.com/grafana/loki/blob/ada4b7b8713385fbe9f5984a5a0aaaddf1a7b851/pkg/push/push.proto#L14>
fn decode_push_request<F>(src: &[u8], push_logs: &mut F) -> Result<(), String>
where
    F: FnMut(i64, &[u8], &mut Vec<Field>, usize),
{
    // message PushRequest {
    //   repeated Stream streams = 1;
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
                .ok_or_else(|| "cannot read Stream data".to_string())?;
            decode_stream(data, push_logs)
                .map_err(|err| format!("cannot unmarshal Stream: {err}"))?;
        }
    }
    Ok(())
}

fn decode_stream<F>(src: &[u8], push_logs: &mut F) -> Result<(), String>
where
    F: FnMut(i64, &[u8], &mut Vec<Field>, usize),
{
    // message Stream {
    //   string labels = 1;
    //   repeated Entry entries = 2;
    // }

    // PORT NOTE: Go pools the fields via logstorage.GetFields/PutFields; the
    // port uses a local Vec<Field> like the JSON path in loki.rs.
    let mut fs: Vec<Field> = Vec::new();

    // Go's GetString aliases the raw wire bytes; the byte-valued Field port
    // reads the labels as raw bytes so label values with invalid UTF-8 are
    // preserved verbatim.
    let labels = easyproto::get_bytes(src, 1)
        .map_err(|err| format!("cannot read labels: {err}"))?
        .ok_or_else(|| "missing labels".to_string())?;
    parse_prom_labels(&mut fs, labels).map_err(|err| {
        format!(
            "cannot parse labels {:?}: {err}",
            String::from_utf8_lossy(labels)
        )
    })?;
    let stream_fields_len = fs.len();

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        if fc.field_num == 2 {
            let data = fc
                .message_data()
                .ok_or_else(|| "cannot read Entry data".to_string())?;

            decode_entry(data, &mut fs, push_logs)
                .map_err(|err| format!("cannot unmarshal Entry: {err}"))?;

            fs.truncate(stream_fields_len);
        }
    }
    Ok(())
}

fn decode_entry<F>(src: &[u8], fs: &mut Vec<Field>, push_logs: &mut F) -> Result<(), String>
where
    F: FnMut(i64, &[u8], &mut Vec<Field>, usize),
{
    // message Entry {
    //   Timestamp timestamp = 1;
    //   string line = 2;
    //   repeated LabelPair structuredMetadata = 3;
    // }

    let mut timestamp: i64 = 0;
    let mut line: &[u8] = b"";

    let stream_fields_len = fs.len();

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        match fc.field_num {
            1 => {
                let data = fc
                    .message_data()
                    .ok_or_else(|| "cannot read Timestamp data".to_string())?;
                timestamp = decode_timestamp(data)
                    .map_err(|err| format!("cannot unmarshal Timestamp: {err}"))?;
            }
            2 => {
                // Go's String() aliases the raw wire bytes; read the log line
                // as raw bytes so invalid UTF-8 is ingested verbatim.
                line = fc.bytes().ok_or_else(|| "cannot read Line".to_string())?;
            }
            3 => {
                let data = fc
                    .message_data()
                    .ok_or_else(|| "cannot read StructuredMetadata".to_string())?;
                decode_label_pair(data, fs)
                    .map_err(|err| format!("cannot unmarshal StructuredMetadata: {err}"))?;
            }
            _ => {}
        }
    }

    push_logs(timestamp, line, fs, stream_fields_len);

    Ok(())
}

fn decode_label_pair(src: &[u8], fs: &mut Vec<Field>) -> Result<(), String> {
    // message LabelPair {
    //   string name = 1;
    //   string value = 2;
    // }

    // PORT NOTE: get_string_lossy mirrors Go's raw-byte GetString for the
    // field NAME (Field.name stays String, so an invalid-UTF-8 name is
    // U+FFFD-replaced instead of rejecting the request); the VALUE is read as
    // raw bytes and preserved verbatim like Go.
    let name = easyproto::get_string_lossy(src, 1)
        .map_err(|err| format!("cannot read name: {err}"))?
        .ok_or_else(|| "missing name".to_string())?;

    let value = easyproto::get_bytes(src, 2)
        .map_err(|err| format!("cannot read value: {err}"))?
        .ok_or_else(|| "missing value".to_string())?;

    if !name.is_empty() && !value.is_empty() {
        fs.push(Field {
            name: name.to_string(),
            value: value.to_vec(),
        });
    }

    Ok(())
}

fn decode_timestamp(src: &[u8]) -> Result<i64, String> {
    // message Timestamp {
    //   int64 seconds = 1;
    //   int32 nanos = 2;
    // }

    let mut seconds: i64 = 0;
    let mut nanos: i32 = 0;

    let mut fc = easyproto::FieldContext::default();
    let mut src = src;
    while !src.is_empty() {
        src = fc
            .next_field(src)
            .map_err(|err| format!("cannot read the next field: {err}"))?;
        match fc.field_num {
            1 => {
                seconds = fc
                    .int64()
                    .ok_or_else(|| "cannot read Seconds".to_string())?;
            }
            2 => {
                nanos = fc.int32().ok_or_else(|| "cannot read Nanos".to_string())?;
            }
            _ => {}
        }
    }

    // PORT NOTE: wrapping arithmetic mirrors Go's silent integer overflow for
    // out-of-range timestamps in untrusted input.
    let nsecs = seconds
        .wrapping_mul(1_000_000_000)
        .wrapping_add(i64::from(nanos));

    Ok(nsecs)
}

/// parsePromLabels parses log fields in Prometheus text exposition format from
/// s and appends them to fs.
///
/// See test data of promtail for examples:
/// <https://github.com/grafana/loki/blob/a24ef7b206e0ca63ee74ca6ecb0a09b745cd2258/pkg/push/types_test.go>
fn parse_prom_labels(fs: &mut Vec<Field>, s: &[u8]) -> Result<(), String> {
    // Go TrimSpace trims Unicode whitespace; mirror it when the bytes are
    // valid UTF-8 and fall back to ASCII trimming otherwise (Go stops
    // trimming at the first invalid sequence too, since it is not a space).
    let s = match std::str::from_utf8(s) {
        Ok(t) => t.trim().as_bytes(),
        Err(_) => s.trim_ascii(),
    };
    // Display-only lossy conversions below (R5): `s` may hold raw bytes; the
    // parsed values themselves are passed through verbatim.
    let d = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
    // Make sure s is wrapped into `{...}`
    if s.len() < 2 {
        return Err(format!("too short string to parse: {:?}", d(s)));
    }
    if !s.starts_with(b"{") {
        return Err(format!("missing `{{` at the beginning of {:?}", d(s)));
    }
    if !s.ends_with(b"}") {
        return Err(format!("missing `}}` at the end of {:?}", d(s)));
    }
    let mut s = &s[1..s.len() - 1];

    while !s.is_empty() {
        // Parse label name
        let n = s
            .iter()
            .position(|&c| c == b'=')
            .ok_or_else(|| format!("cannot find `=` char for label value at {}", d(s)))?;
        let name = &s[..n];
        s = &s[n + 1..];

        // Parse label value
        let (value, qs_len) = unquote_prefix(s).map_err(|err| {
            format!(
                "cannot parse value for label {:?} at {}: {err}",
                d(name),
                d(s)
            )
        })?;
        s = &s[qs_len..];

        // Append the found field to dst. The label NAME becomes Field.name
        // (a String), so an invalid-UTF-8 name is U+FFFD-replaced; the value
        // bytes are preserved verbatim.
        fs.push(Field {
            name: d(name),
            value,
        });

        // Check whether there are other labels remaining
        if s.is_empty() {
            break;
        }
        match s.strip_prefix(b",") {
            Some(tail) => {
                s = tail.strip_prefix(b" ").unwrap_or(tail);
            }
            None => return Err(format!("missing `,` char at {}", d(s))),
        }
    }
    Ok(())
}

/// Parses a Go double-quoted string prefix from s, returning the unquoted
/// value and the number of bytes consumed.
///
/// PORT NOTE: replaces Go `strconv.QuotedPrefix` + `strconv.Unquote`. Only
/// double-quoted strings are supported (Prometheus label values are always
/// double-quoted). Escapes decode to raw bytes exactly like Go's
/// `strconv.UnquoteChar` (`\xHH`/`\NNN` octal emit the single byte, `\u`/`\U`
/// emit the rune's UTF-8), so the decoded bytes match Go byte-for-byte —
/// including values that are NOT valid UTF-8 (e.g. `"\xff"` → raw `0xFF`),
/// which the byte-valued `Field` now stores verbatim.
fn unquote_prefix(s: &[u8]) -> Result<(Vec<u8>, usize), String> {
    const ERR: &str = "invalid syntax";
    let b = s;
    if b.first() != Some(&b'"') {
        return Err(ERR.to_string());
    }
    let mut out: Vec<u8> = Vec::new();
    let mut i = 1;
    while i < b.len() {
        match b[i] {
            b'"' => {
                return Ok((out, i + 1));
            }
            b'\n' => return Err(ERR.to_string()),
            b'\\' => {
                i += 1;
                if i >= b.len() {
                    return Err(ERR.to_string());
                }
                let e = b[i];
                i += 1;
                match e {
                    b'a' => out.push(0x07),
                    b'b' => out.push(0x08),
                    b'f' => out.push(0x0C),
                    b'n' => out.push(b'\n'),
                    b'r' => out.push(b'\r'),
                    b't' => out.push(b'\t'),
                    b'v' => out.push(0x0B),
                    b'\\' => out.push(b'\\'),
                    // Go's unquoteChar rejects \' inside double-quoted
                    // strings (the escape is only valid for the quote char).
                    b'"' => out.push(b'"'),
                    b'x' => {
                        // Go appends the raw byte, even if >= 0x80.
                        let v = unquote_hex_escape(b, &mut i, 2)?;
                        out.push(v as u8);
                    }
                    b'u' => {
                        let v = unquote_hex_escape(b, &mut i, 4)?;
                        let c = char::from_u32(v).ok_or_else(|| ERR.to_string())?;
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                    }
                    b'U' => {
                        let v = unquote_hex_escape(b, &mut i, 8)?;
                        let c = char::from_u32(v).ok_or_else(|| ERR.to_string())?;
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                    }
                    b'0'..=b'7' => {
                        // Octal escape: exactly 3 digits, raw byte, <= 255
                        // (Go errors on \400..\777).
                        let mut v = u32::from(e - b'0');
                        for _ in 0..2 {
                            if i >= b.len() || !b[i].is_ascii_digit() || b[i] > b'7' {
                                return Err(ERR.to_string());
                            }
                            v = v * 8 + u32::from(b[i] - b'0');
                            i += 1;
                        }
                        if v > 255 {
                            return Err(ERR.to_string());
                        }
                        out.push(v as u8);
                    }
                    _ => return Err(ERR.to_string()),
                }
            }
            c => {
                // Unescaped bytes are copied through unchanged (Go copies
                // the raw bytes the same way).
                out.push(c);
                i += 1;
            }
        }
    }
    Err(ERR.to_string())
}

fn unquote_hex_escape(b: &[u8], i: &mut usize, digits: usize) -> Result<u32, String> {
    let mut v: u32 = 0;
    for _ in 0..digits {
        if *i >= b.len() {
            return Err("invalid syntax".to_string());
        }
        let d = (b[*i] as char)
            .to_digit(16)
            .ok_or_else(|| "invalid syntax".to_string())?;
        v = v * 16 + d;
        *i += 1;
    }
    Ok(v)
}

// ---------------------------------------------------------------------------
// pb_marshal.go
//
// PORT NOTE: Go compiles the marshaling helpers into the package although
// they are only used by tests; the port gates them under cfg(test) to avoid
// dead code in release builds.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod pb_marshal {
    use esl_common::easyproto::{MarshalerPool, MessageMarshaler};

    static MP: MarshalerPool = MarshalerPool::new();

    /// pushRequest represents Loki PushRequest.
    ///
    /// See <https://github.com/grafana/loki/blob/ada4b7b8713385fbe9f5984a5a0aaaddf1a7b851/pkg/push/push.proto#L14>
    pub(crate) struct PushRequest {
        pub streams: Vec<Stream>,
    }

    impl PushRequest {
        /// MarshalProtobuf marshals pr to a protobuf message and appends it to
        /// dst.
        pub(crate) fn marshal_protobuf(&self, dst: &mut Vec<u8>) {
            let mut m = MP.get();
            self.marshal_protobuf_fields(&mut m.message_marshaler());
            m.marshal(dst);
            MP.put(m);
        }

        fn marshal_protobuf_fields(&self, mm: &mut MessageMarshaler<'_>) {
            for s in &self.streams {
                s.marshal_protobuf(&mut mm.append_message(1));
            }
        }
    }

    /// stream represents Loki stream.
    ///
    /// See <https://github.com/grafana/loki/blob/ada4b7b8713385fbe9f5984a5a0aaaddf1a7b851/pkg/push/push.proto#L23>
    pub(crate) struct Stream {
        pub labels: String,
        pub entries: Vec<Entry>,
    }

    impl Stream {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            mm.append_string(1, &self.labels);
            for e in &self.entries {
                e.marshal_protobuf(&mut mm.append_message(2));
            }
        }
    }

    /// entry represents Loki entry.
    ///
    /// PORT NOTE: Go stores the timestamp as `time.Time`; the port uses unix
    /// nanoseconds directly.
    ///
    /// See <https://github.com/grafana/loki/blob/ada4b7b8713385fbe9f5984a5a0aaaddf1a7b851/pkg/push/push.proto#L38>
    pub(crate) struct Entry {
        pub timestamp: i64,
        pub line: String,
        pub structured_metadata: Vec<LabelPair>,
    }

    impl Entry {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            marshal_time(mm, 1, self.timestamp);
            mm.append_string(2, &self.line);
            for lp in &self.structured_metadata {
                lp.marshal_protobuf(&mut mm.append_message(3));
            }
        }
    }

    /// labelPair represents Loki label pair.
    ///
    /// See <https://github.com/grafana/loki/blob/ada4b7b8713385fbe9f5984a5a0aaaddf1a7b851/pkg/push/push.proto#L33>
    pub(crate) struct LabelPair {
        pub name: String,
        pub value: String,
    }

    impl LabelPair {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            mm.append_string(1, &self.name);
            mm.append_string(2, &self.value);
        }
    }

    fn marshal_time(mm: &mut MessageMarshaler<'_>, field_num: u32, nsecs: i64) {
        let ts = Timestamp {
            seconds: nsecs / 1_000_000_000,
            nanos: (nsecs % 1_000_000_000) as i32,
        };
        ts.marshal_protobuf(&mut mm.append_message(field_num));
    }

    /// timestamp is the protobuf well-known timestamp type.
    struct Timestamp {
        seconds: i64,
        nanos: i32,
    }

    impl Timestamp {
        fn marshal_protobuf(&self, mm: &mut MessageMarshaler<'_>) {
            mm.append_int64(1, self.seconds);
            mm.append_int32(2, self.nanos);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests (pb_test.go + loki_protobuf_test.go)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::pb_marshal::{Entry, LabelPair, PushRequest, Stream};
    use super::*;
    use crate::common_params::CommonParams;
    use crate::testutil::{open_temp_storage, rows_count};

    /// Formats s like Go `fmt.Sprintf("%q", s)` for the escapes used in the
    /// ported test data.
    fn quote(s: &str) -> String {
        let mut out = String::from("\"");
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                _ => out.push(c),
            }
        }
        out.push('"');
        out
    }

    fn format_labels(fs: &[Field]) -> String {
        let a: Vec<String> = fs
            .iter()
            .map(|f| {
                let v = std::str::from_utf8(&f.value).expect("test label values are UTF-8");
                format!("{}={}", f.name, quote(v))
            })
            .collect();
        format!("{{{}}}", a.join(", "))
    }

    #[test]
    fn test_parse_prom_labels_success() {
        fn f(s: &str) {
            let mut fs: Vec<Field> = Vec::new();
            if let Err(err) = parse_prom_labels(&mut fs, s.as_bytes()) {
                panic!("unexpected error: {err}");
            }
            let result = format_labels(&fs);
            assert_eq!(result, s, "unexpected result");
        }

        f("{}");
        f(r#"{foo="bar"}"#);
        f(r#"{foo="bar", baz="x", y="z"}"#);
        f("{foo=\"ba\\\"r\\\\z\\n\", a=\"\", b=\"\\\"\\\\\"}");
    }

    #[test]
    fn test_parse_prom_labels_failure() {
        fn f(s: &str) {
            let mut fs: Vec<Field> = Vec::new();
            if parse_prom_labels(&mut fs, s.as_bytes()).is_ok() {
                panic!("expecting non-nil error for {s:?}");
            }
        }

        f("");
        f("{");
        f(r#"{foo}"#);
        f(r#"{foo=bar}"#);
        f(r#"{foo="bar}"#);
        f(r#"{foo="ba\",r}"#);
        f(r#"{foo="bar" baz="aa"}"#);
        f(r#"foobar"#);
        f(r#"foo{bar="baz"}"#);
    }

    // PORT-ONLY TEST: pins the strconv.Unquote escape semantics of
    // unquote_prefix. `\xHH`/octal escapes decode to raw bytes like Go —
    // including bytes that are NOT valid UTF-8, which are preserved verbatim
    // (see the PORT NOTE on unquote_prefix).
    #[test]
    fn test_unquote_prefix_escapes() {
        fn f(quoted: &str, want: &[u8]) {
            let (got, n) = unquote_prefix(quoted.as_bytes()).unwrap();
            assert_eq!(got, want, "unexpected unquoted value for {quoted:?}");
            assert_eq!(n, quoted.len(), "unexpected consumed length");
        }

        // \x escapes composing valid UTF-8 match Go exactly.
        f(r#""\xc3\xa9""#, "é".as_bytes());
        // Octal escapes composing valid UTF-8 match Go exactly.
        f(r#""\303\251""#, "é".as_bytes());
        // \u/\U escapes.
        f(r#""é \U0001F600""#, "é 😀".as_bytes());
        // Lone invalid byte: stored raw exactly like Go.
        f(r#""\xff""#, b"\xff");

        // Go errors on octal values > 255, on surrogate \u escapes, and on
        // \' inside double-quoted strings.
        assert!(unquote_prefix(br#""\400""#).is_err());
        assert!(unquote_prefix(br#""\ud800""#).is_err());
        assert!(unquote_prefix(br#""don\'t""#).is_err());
    }

    /// Mirrors Go `TestParseProtobufRequest_Success`: marshal a pushRequest
    /// to protobuf and verify each record produced by the decoder.
    ///
    /// PORT NOTE: Go builds the pushRequest by round-tripping through
    /// `parseJSONRequest` + `testLogMessageProcessor` and verifies via
    /// `insertutil.TestLogMessageProcessor`. The Rust `LogMessageProcessor` is
    /// concrete (writes to Storage), so the port constructs the pushRequest
    /// directly and verifies the decoded records at the `decode_push_request`
    /// level instead.
    #[test]
    fn test_decode_push_request_success() {
        let pr = PushRequest {
            streams: vec![
                Stream {
                    labels: r#"{label1="value1", label2="value2"}"#.to_string(),
                    entries: vec![
                        Entry {
                            timestamp: 1577836800000000001,
                            line: "foo bar".to_string(),
                            structured_metadata: Vec::new(),
                        },
                        Entry {
                            timestamp: 1477836900005000002,
                            line: "abc".to_string(),
                            structured_metadata: vec![
                                LabelPair {
                                    name: "foo".to_string(),
                                    value: "bar".to_string(),
                                },
                                LabelPair {
                                    name: "a".to_string(),
                                    value: "b".to_string(),
                                },
                            ],
                        },
                    ],
                },
                Stream {
                    labels: r#"{x="y"}"#.to_string(),
                    entries: vec![Entry {
                        timestamp: 1877836900005000002,
                        line: "yx".to_string(),
                        structured_metadata: Vec::new(),
                    }],
                },
            ],
        };
        let mut data = Vec::new();
        pr.marshal_protobuf(&mut data);

        let mut rows: Vec<(i64, String, String, usize)> = Vec::new();
        decode_push_request(
            &data,
            &mut |ts, line: &[u8], fs: &mut Vec<Field>, stream_fields_len| {
                let line = String::from_utf8(line.to_vec()).expect("test lines are UTF-8");
                rows.push((ts, line, format_labels(fs), stream_fields_len));
            },
        )
        .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0],
            (
                1577836800000000001,
                "foo bar".to_string(),
                r#"{label1="value1", label2="value2"}"#.to_string(),
                2
            )
        );
        assert_eq!(
            rows[1],
            (
                1477836900005000002,
                "abc".to_string(),
                r#"{label1="value1", label2="value2", foo="bar", a="b"}"#.to_string(),
                2
            )
        );
        assert_eq!(
            rows[2],
            (
                1877836900005000002,
                "yx".to_string(),
                r#"{x="y"}"#.to_string(),
                1
            )
        );
    }

    #[test]
    fn test_decode_push_request_empty_streams() {
        // Empty PushRequest.
        let pr = PushRequest {
            streams: Vec::new(),
        };
        let mut data = Vec::new();
        pr.marshal_protobuf(&mut data);
        let mut n = 0;
        decode_push_request(&data, &mut |_, _, _: &mut Vec<Field>, _| n += 1).unwrap();
        assert_eq!(n, 0);

        // Stream without entries.
        let pr = PushRequest {
            streams: vec![Stream {
                labels: r#"{foo="bar"}"#.to_string(),
                entries: Vec::new(),
            }],
        };
        let mut data = Vec::new();
        pr.marshal_protobuf(&mut data);
        let mut n = 0;
        decode_push_request(&data, &mut |_, _, _: &mut Vec<Field>, _| n += 1).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_decode_push_request_failure() {
        let mut noop = |_: i64, _: &[u8], _: &mut Vec<Field>, _: usize| {};
        // Garbage protobuf data.
        assert!(decode_push_request(&[0xff, 0xff, 0xff], &mut noop).is_err());
        // Stream with invalid labels.
        let pr = PushRequest {
            streams: vec![Stream {
                labels: "not-prom-labels".to_string(),
                entries: Vec::new(),
            }],
        };
        let mut data = Vec::new();
        pr.marshal_protobuf(&mut data);
        assert!(decode_push_request(&data, &mut noop).is_err());
    }

    #[test]
    fn test_parse_protobuf_request_lands_rows() {
        let s = open_temp_storage("loki-protobuf");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");

        // Zero timestamps force the current time (avoids retention drops).
        let pr = PushRequest {
            streams: vec![Stream {
                labels: r#"{app="foo", level="info"}"#.to_string(),
                entries: vec![
                    Entry {
                        timestamp: 0,
                        line: "hello".to_string(),
                        structured_metadata: Vec::new(),
                    },
                    Entry {
                        timestamp: 0,
                        line: "world".to_string(),
                        structured_metadata: vec![LabelPair {
                            name: "trace_id".to_string(),
                            value: "abc".to_string(),
                        }],
                    },
                ],
            }],
        };
        let mut data = Vec::new();
        pr.marshal_protobuf(&mut data);

        let no_fields: [&str; 0] = [];
        let res = parse_protobuf_request(&data, &mut lmp, &no_fields, &no_fields, "", true, true);
        assert!(res.is_ok(), "unexpected error: {res:?}");
        lmp.close();

        assert_eq!(rows_count(&s), 2, "expected 2 rows ingested");
        s.must_close();
    }

    #[test]
    fn test_parse_protobuf_request_snappy_roundtrip() {
        // Verifies the snappy block decoding used by handle_protobuf.
        let pr = PushRequest {
            streams: vec![Stream {
                labels: r#"{app="snappy"}"#.to_string(),
                entries: vec![Entry {
                    timestamp: 1577836800000000001,
                    line: "compressed".to_string(),
                    structured_metadata: Vec::new(),
                }],
            }],
        };
        let mut data = Vec::new();
        pr.marshal_protobuf(&mut data);

        let compressed = snap::raw::Encoder::new().compress_vec(&data).unwrap();
        let decoded = snappy_decode_block(&compressed).unwrap();
        assert_eq!(decoded, data);

        let mut lines = Vec::new();
        decode_push_request(&decoded, &mut |ts, line: &[u8], _: &mut Vec<Field>, _| {
            let line = String::from_utf8(line.to_vec()).expect("test lines are UTF-8");
            lines.push((ts, line));
        })
        .unwrap();
        assert_eq!(lines, vec![(1577836800000000001, "compressed".to_string())]);
    }

    #[test]
    fn test_decode_timestamp() {
        // Round-trip through the pb_marshal Timestamp encoding: an entry with
        // a known timestamp must decode to the same nanosecond value.
        let nsecs = 1577836800123456789i64;
        let pr = PushRequest {
            streams: vec![Stream {
                labels: "{}".to_string(),
                entries: vec![Entry {
                    timestamp: nsecs,
                    line: "x".to_string(),
                    structured_metadata: Vec::new(),
                }],
            }],
        };
        let mut data = Vec::new();
        pr.marshal_protobuf(&mut data);

        let mut got = Vec::new();
        decode_push_request(&data, &mut |ts, _, _: &mut Vec<Field>, _| got.push(ts)).unwrap();
        assert_eq!(got, vec![nsecs]);
    }
}

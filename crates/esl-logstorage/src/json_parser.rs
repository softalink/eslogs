//! Port of EsLogs `lib/logstorage/json_parser.go`.
//!
//! Also hosts the [`fastjson`] submodule — a port of the needed subset of the
//! vendored `github.com/valyala/fastjson` (Parser, Scanner, Value, Object) —
//! and the `commonJSON` helper shared with `json_scanner.rs`.

use crate::consts::MAX_FIELD_NAME_SIZE;
use crate::rows::Field;

/// JSONParser parses a single JSON log message into Fields.
///
/// See <https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model>
///
/// Use `parse_log_message()` for parsing the JSON log message.
///
/// Use `get_json_parser()` for obtaining the parser.
#[derive(Default)]
pub struct JSONParser {
    /// PORT NOTE: Go embeds `commonJSON`; the port holds it as a field and
    /// exposes the parsed fields via `fields()`.
    pub(crate) common: CommonJson,

    /// p is used for fast JSON parsing
    p: fastjson::Parser,
}

impl JSONParser {
    /// Fields contains the parsed JSON line after a `parse_log_message()` call.
    pub fn fields(&self) -> &[Field] {
        &self.common.fields
    }

    /// Mutable access to the parsed fields, so an ingestion caller can adjust
    /// them in place (extract `_time`, rename `_msg`) and hand them to the
    /// storage without cloning into a scratch buffer. Valid until the next
    /// `parse_log_message()` / `put_json_parser()`.
    pub fn fields_mut(&mut self) -> &mut Vec<Field> {
        &mut self.common.fields
    }

    fn reset(&mut self) {
        self.common.reset();
    }

    /// ParseLogMessage parses the given JSON log message msg into `fields()`.
    ///
    /// JSON values for keys from the preserve_keys list are preserved without flattening.
    ///
    /// The given field_prefix is added to all the parsed field names.
    ///
    /// The fields remain valid until the next call to `parse_log_message()` or `put_json_parser()`.
    pub fn parse_log_message(
        &mut self,
        msg: &[u8],
        preserve_keys: &[&[u8]],
        field_prefix: &str,
    ) -> Result<(), String> {
        self.parse_log_message_impl(msg, preserve_keys, field_prefix, MAX_FIELD_NAME_SIZE)
    }

    /// parseLogMessage parses the given JSON log message msg into `fields()`.
    ///
    /// Items in nested objects are flattened with `k1.k2. ... .kN` key until the key matches one of the preserve_keys
    /// or its length exceeds max_field_name_len.
    ///
    /// PORT NOTE: named `parse_log_message_impl` because Go overloads the
    /// name case-insensitively (`ParseLogMessage` / `parseLogMessage`).
    fn parse_log_message_impl(
        &mut self,
        msg: &[u8],
        preserve_keys: &[&[u8]],
        field_prefix: &str,
        max_field_name_len: usize,
    ) -> Result<(), String> {
        self.reset();

        let v = self.p.parse(msg)?;
        let o = self.p.doc.object(v)?;
        self.common
            .init(preserve_keys, field_prefix, max_field_name_len);
        self.common.append_log_fields(&mut self.p.doc, o);
        Ok(())
    }
}

/// GetJSONParser returns a JSONParser ready to parse JSON lines.
///
/// Return the parser to the pool when it is no longer needed by calling `put_json_parser()`.
pub fn get_json_parser() -> JSONParser {
    PARSER_POOL
        .with(|p| p.borrow_mut().pop())
        .unwrap_or_default()
}

/// PutJSONParser returns the parser to the pool.
///
/// The parser cannot be used after returning to the pool.
pub fn put_json_parser(mut p: JSONParser) {
    p.reset();
    PARSER_POOL.with(|pool| {
        let mut v = pool.borrow_mut();
        if v.len() < PARSER_POOL_CAP {
            v.push(p);
        }
    });
}

/// PORT NOTE: Go uses `sync.Pool` (which is per-P/thread-local internally). The
/// port uses a `thread_local!` free-list so the ingest hot path never contends
/// on a global lock (a `Mutex<Vec<..>>` pool showed up as lock contention under
/// concurrent ingest). Capped so idle threads don't retain unbounded parsers.
const PARSER_POOL_CAP: usize = 16;
thread_local! {
    static PARSER_POOL: std::cell::RefCell<Vec<JSONParser>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Port of Go's `commonJSON`, shared by `JSONParser` and `JSONScanner`.
#[derive(Default)]
pub(crate) struct CommonJson {
    /// Fields contains the parsed JSON line after append_log_fields() call.
    pub(crate) fields: Vec<Field>,

    /// PORT NOTE: Go backs `Fields` with an internal byte buffer (`c.buf`) to
    /// avoid per-field allocations; the Rust `Field` owns its strings, so the
    /// port recycles Field string capacities through `spare_fields` instead.
    spare_fields: Vec<Field>,

    /// buf is used as scratch space for marshaled JSON values.
    buf: Vec<u8>,

    /// prefix_buf is used for holding the current key prefix when it is composed from multiple keys.
    prefix_buf: Vec<u8>,

    /// PORT NOTE: Go stores the caller's `preserveKeys` slice and
    /// `fieldPrefix` string by reference; the port copies them, since
    /// `JSONScanner` keeps the settings alive across `next_log_message()`
    /// calls.
    preserve_keys: Vec<Vec<u8>>,
    field_prefix: String,
    max_field_name_len: usize,
}

impl CommonJson {
    pub(crate) fn reset(&mut self) {
        self.reset_keep_settings();
        self.preserve_keys.clear();
        self.field_prefix.clear();
        self.max_field_name_len = 0;
    }

    pub(crate) fn init(
        &mut self,
        preserve_keys: &[&[u8]],
        field_prefix: &str,
        max_field_name_len: usize,
    ) {
        self.preserve_keys.clear();
        self.preserve_keys
            .extend(preserve_keys.iter().map(|s| s.to_vec()));
        self.field_prefix.clear();
        self.field_prefix.push_str(field_prefix);
        self.max_field_name_len = max_field_name_len;
    }

    pub(crate) fn reset_keep_settings(&mut self) {
        // Recycle Field allocations instead of dropping them (see the
        // PORT NOTE on spare_fields).
        self.spare_fields.append(&mut self.fields);

        self.buf.clear();

        self.prefix_buf.clear();
    }

    pub(crate) fn append_log_fields(&mut self, doc: &mut fastjson::Doc, o: u32) {
        if self.is_too_long_key(doc, o) || self.should_preserve_key_prefix() {
            self.append_preserved_log_field(doc, o);
            return;
        }

        // Flatten JSON object o.
        // For example, {"foo":{"bar":"baz"}} is converted to {"foo.bar":"baz"}
        doc.unescape_keys(o); // Go's Object.Visit unescapes the keys up-front
        for i in 0..doc.object_len(o) {
            let (k, v) = doc.kv(o, i);
            let t = doc.value_type(v);
            match t {
                fastjson::JsonType::Null => {
                    // Skip nulls
                }
                fastjson::JsonType::Object => {
                    // Flatten nested JSON objects.
                    let prefix_len = self.prefix_buf.len();
                    self.prefix_buf.extend_from_slice(doc.str_bytes(k));
                    self.prefix_buf.push(b'.');
                    self.append_log_fields(doc, v);
                    self.prefix_buf.truncate(prefix_len);
                }
                fastjson::JsonType::Array
                | fastjson::JsonType::Number
                | fastjson::JsonType::True
                | fastjson::JsonType::False => {
                    // Convert JSON arrays, numbers, true and false values to their string representation
                    let buf_len = self.buf.len();
                    doc.marshal_value_to(v, &mut self.buf);
                    self.append_log_field_from_buf(doc, k, buf_len);
                }
                fastjson::JsonType::String => {
                    // Decode JSON strings
                    let value = doc.string_span(v);
                    self.append_log_field(doc, k, value);
                }
                fastjson::JsonType::RawString => {
                    // value_type() never returns RawString.
                    esl_common::panicf!("BUG: unexpected JSON type: rawString");
                    unreachable!()
                }
            }
        }
    }

    fn is_too_long_key(&self, doc: &mut fastjson::Doc, o: u32) -> bool {
        // Go computes the max key length via Object.Visit, which unescapes
        // the object keys as a side effect; mirror that.
        doc.unescape_keys(o);
        let mut max_key_len = 0;
        for i in 0..doc.object_len(o) {
            let (k, _) = doc.kv(o, i);
            if k.len() > max_key_len {
                max_key_len = k.len();
            }
        }
        self.prefix_buf.len() + max_key_len > self.max_field_name_len
    }

    fn should_preserve_key_prefix(&self) -> bool {
        if self.prefix_buf.is_empty() {
            return false;
        }

        // Drop trailing dot
        let key = &self.prefix_buf[..self.prefix_buf.len() - 1];

        self.preserve_keys.iter().any(|k| k == key)
    }

    fn append_preserved_log_field(&mut self, doc: &fastjson::Doc, o: u32) {
        let prefix_len = self.prefix_buf.len();
        if prefix_len > 0 {
            // Drop trailing dot
            self.prefix_buf.truncate(prefix_len - 1);
        }

        let buf_len = self.buf.len();
        doc.marshal_object_to(o, &mut self.buf);
        self.append_log_field_from_buf(doc, fastjson::StrSpan::default(), buf_len);
        if prefix_len > 0 {
            // Restore the trailing dot
            self.prefix_buf.push(b'.');
        }
    }

    /// Appends a field whose value was marshaled into `self.buf[buf_len..]`.
    fn append_log_field_from_buf(
        &mut self,
        doc: &fastjson::Doc,
        k: fastjson::StrSpan,
        buf_len: usize,
    ) {
        let mut f = self.take_spare_field();
        compose_field_name(
            &mut f.name,
            &self.field_prefix,
            &self.prefix_buf,
            doc.str_bytes(k),
        );
        f.value.extend_from_slice(&self.buf[buf_len..]);
        // PORT NOTE: Go keeps the marshaled value in c.buf because Fields
        // reference it; the port copies the value into the Field, so the
        // scratch space is reclaimed here.
        self.buf.truncate(buf_len);
        self.fields.push(f);
    }

    fn append_log_field(
        &mut self,
        doc: &fastjson::Doc,
        k: fastjson::StrSpan,
        value: fastjson::StrSpan,
    ) {
        let mut f = self.take_spare_field();
        compose_field_name(
            &mut f.name,
            &self.field_prefix,
            &self.prefix_buf,
            doc.str_bytes(k),
        );
        f.value.extend_from_slice(doc.str_bytes(value));
        self.fields.push(f);
    }

    fn take_spare_field(&mut self) -> Field {
        let mut f = self.spare_fields.pop().unwrap_or_default();
        f.name.clear();
        f.value.clear();
        f
    }
}

/// Composes `field_prefix + prefix_buf + k` into name; an empty name becomes
/// `_msg` (Go `commonJSON.appendLogField`).
///
/// Go views the raw key bytes as strings (`bytesutil.ToUnsafeString` in
/// `appendLogField`); `Field.name` is raw bytes, so JSON containing invalid
/// UTF-8 inside key literals keeps those bytes verbatim in the stored field
/// name, matching Go.
fn compose_field_name(name: &mut Vec<u8>, field_prefix: &str, prefix_buf: &[u8], k: &[u8]) {
    name.extend_from_slice(field_prefix.as_bytes());
    name.extend_from_slice(prefix_buf);
    name.extend_from_slice(k);
    if name.is_empty() {
        name.extend_from_slice(b"_msg");
    }
}

/// Minimal port of the vendored `github.com/valyala/fastjson` — only the
/// subset needed by `json_parser.rs` and `json_scanner.rs`.
///
/// PORT NOTE: Go's `*fastjson.Value` / `*fastjson.Object` pointers become
/// `u32` indexes into a per-parser value arena ([`Doc`]); string data is kept
/// as byte ranges ([`StrSpan`]) into the parser's working buffer, preserving
/// the zero-copy behavior (including the lazy in-place unescaping of JSON
/// strings) and the value/buffer reuse across parses.
pub(crate) mod fastjson {
    /// MaxDepth is the maximum depth for nested JSON.
    pub(crate) const MAX_DEPTH: usize = 300;

    /// Type represents JSON type (Go `fastjson.Type`).
    #[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
    pub(crate) enum JsonType {
        #[default]
        Null,
        Object,
        Array,
        String,
        Number,
        True,
        False,
        /// Internal type for not-yet-unescaped strings; never returned by
        /// `Doc::value_type()`.
        RawString,
    }

    impl std::fmt::Display for JsonType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let s = match self {
                JsonType::Object => "object",
                JsonType::Array => "array",
                JsonType::String => "string",
                JsonType::Number => "number",
                JsonType::True => "true",
                JsonType::False => "false",
                JsonType::Null => "null",
                // rawString is skipped intentionally,
                // since it shouldn't be visible to user.
                JsonType::RawString => panic!("BUG: unknown Value type: rawString"),
            };
            f.write_str(s)
        }
    }

    /// Byte range into `Doc::buf`.
    #[derive(Clone, Copy, Default)]
    pub(crate) struct StrSpan {
        start: usize,
        end: usize,
    }

    impl StrSpan {
        pub(crate) fn len(&self) -> usize {
            self.end - self.start
        }
    }

    #[derive(Default)]
    struct Kv {
        k: StrSpan,
        v: u32,
    }

    #[derive(Default)]
    struct ValueData {
        t: JsonType,
        s: StrSpan,
        a: Vec<u32>,
        kvs: Vec<Kv>,
        keys_unescaped: bool,
    }

    impl ValueData {
        fn reset(&mut self) {
            self.t = JsonType::Null;
            self.s = StrSpan::default();
            self.a.clear();
            self.kvs.clear();
            self.keys_unescaped = false;
        }
    }

    /// Port of fastjson's `cache`: value structs (and their inner Vec
    /// capacities) are reused across parses.
    #[derive(Default)]
    struct Cache {
        vs: Vec<ValueData>,
        len: usize,
    }

    impl Cache {
        fn reset(&mut self) {
            self.len = 0;
        }

        fn get_value(&mut self) -> u32 {
            if self.len < self.vs.len() {
                self.vs[self.len].reset();
            } else {
                self.vs.push(ValueData::default());
            }
            let idx = self.len as u32;
            self.len += 1;
            idx
        }
    }

    /// Doc combines Go's working buffer (`Parser.b` / `Scanner.b`) with the
    /// value cache; value/object accessors take arena indexes.
    #[derive(Default)]
    pub(crate) struct Doc {
        buf: Vec<u8>,
        c: Cache,
    }

    /// Internal parse error: (message, tail position).
    type PErr = (String, usize);

    impl Doc {
        fn vd(&self, v: u32) -> &ValueData {
            &self.c.vs[v as usize]
        }

        fn parse_value(&mut self, pos: usize, depth: usize) -> Result<(u32, usize), PErr> {
            if pos >= self.buf.len() {
                return Err(("cannot parse empty string".to_string(), pos));
            }
            let depth = depth + 1;
            if depth > MAX_DEPTH {
                return Err((
                    format!("too big depth for the nested JSON; it exceeds {MAX_DEPTH}"),
                    pos,
                ));
            }

            match self.buf[pos] {
                b'{' => self
                    .parse_object(pos + 1, depth)
                    .map_err(|(e, p)| (format!("cannot parse object: {e}"), p)),
                b'[' => self
                    .parse_array(pos + 1, depth)
                    .map_err(|(e, p)| (format!("cannot parse array: {e}"), p)),
                b'"' => {
                    let (ss, tail) = parse_raw_string(&self.buf, pos + 1)
                        .map_err(|(e, p)| (format!("cannot parse string: {e}"), p))?;
                    let v = self.c.get_value();
                    let vd = &mut self.c.vs[v as usize];
                    vd.t = JsonType::RawString;
                    vd.s = ss;
                    Ok((v, tail))
                }
                b't' => {
                    if !self.buf[pos..].starts_with(b"true") {
                        return Err((
                            format!("unexpected value found: {}", quote_bytes(&self.buf[pos..])),
                            pos,
                        ));
                    }
                    let v = self.c.get_value();
                    self.c.vs[v as usize].t = JsonType::True;
                    Ok((v, pos + "true".len()))
                }
                b'f' => {
                    if !self.buf[pos..].starts_with(b"false") {
                        return Err((
                            format!("unexpected value found: {}", quote_bytes(&self.buf[pos..])),
                            pos,
                        ));
                    }
                    let v = self.c.get_value();
                    self.c.vs[v as usize].t = JsonType::False;
                    Ok((v, pos + "false".len()))
                }
                b'n' => {
                    if !self.buf[pos..].starts_with(b"null") {
                        // Try parsing NaN
                        if self.buf.len() - pos >= 3
                            && self.buf[pos..pos + 3].eq_ignore_ascii_case(b"nan")
                        {
                            let v = self.c.get_value();
                            let vd = &mut self.c.vs[v as usize];
                            vd.t = JsonType::Number;
                            vd.s = StrSpan {
                                start: pos,
                                end: pos + 3,
                            };
                            return Ok((v, pos + 3));
                        }
                        return Err((
                            format!("unexpected value found: {}", quote_bytes(&self.buf[pos..])),
                            pos,
                        ));
                    }
                    let v = self.c.get_value();
                    self.c.vs[v as usize].t = JsonType::Null;
                    Ok((v, pos + "null".len()))
                }
                _ => {
                    let (ns, tail) = parse_raw_number(&self.buf, pos)
                        .map_err(|(e, p)| (format!("cannot parse number: {e}"), p))?;
                    let v = self.c.get_value();
                    let vd = &mut self.c.vs[v as usize];
                    vd.t = JsonType::Number;
                    vd.s = ns;
                    Ok((v, tail))
                }
            }
        }

        fn parse_array(&mut self, pos: usize, depth: usize) -> Result<(u32, usize), PErr> {
            let mut pos = skip_ws(&self.buf, pos);
            if pos >= self.buf.len() {
                return Err(("missing ']'".to_string(), pos));
            }

            if self.buf[pos] == b']' {
                let v = self.c.get_value();
                self.c.vs[v as usize].t = JsonType::Array;
                return Ok((v, pos + 1));
            }

            let a = self.c.get_value();
            self.c.vs[a as usize].t = JsonType::Array;
            loop {
                pos = skip_ws(&self.buf, pos);
                let (v, tail) = self
                    .parse_value(pos, depth)
                    .map_err(|(e, p)| (format!("cannot parse array value: {e}"), p))?;
                pos = tail;
                self.c.vs[a as usize].a.push(v);

                pos = skip_ws(&self.buf, pos);
                if pos >= self.buf.len() {
                    return Err(("unexpected end of array".to_string(), pos));
                }
                match self.buf[pos] {
                    b',' => {
                        pos += 1;
                    }
                    b']' => {
                        return Ok((a, pos + 1));
                    }
                    _ => return Err(("missing ',' after array value".to_string(), pos)),
                }
            }
        }

        fn parse_object(&mut self, pos: usize, depth: usize) -> Result<(u32, usize), PErr> {
            let mut pos = skip_ws(&self.buf, pos);
            if pos >= self.buf.len() {
                return Err(("missing '}'".to_string(), pos));
            }

            if self.buf[pos] == b'}' {
                let v = self.c.get_value();
                self.c.vs[v as usize].t = JsonType::Object;
                return Ok((v, pos + 1));
            }

            let o = self.c.get_value();
            self.c.vs[o as usize].t = JsonType::Object;
            loop {
                // Parse key.
                pos = skip_ws(&self.buf, pos);
                if pos >= self.buf.len() || self.buf[pos] != b'"' {
                    // Go keeps this odd quoting in the message; preserved as-is.
                    return Err(("cannot find opening '\"\" for object key".to_string(), pos));
                }
                let (k, tail) = parse_raw_key(&self.buf, pos + 1)
                    .map_err(|(e, p)| (format!("cannot parse object key: {e}"), p))?;
                pos = tail;
                pos = skip_ws(&self.buf, pos);
                if pos >= self.buf.len() || self.buf[pos] != b':' {
                    return Err(("missing ':' after object key".to_string(), pos));
                }
                pos += 1;

                // Parse value
                pos = skip_ws(&self.buf, pos);
                let (v, tail) = self
                    .parse_value(pos, depth)
                    .map_err(|(e, p)| (format!("cannot parse object value: {e}"), p))?;
                pos = tail;
                self.c.vs[o as usize].kvs.push(Kv { k, v });
                pos = skip_ws(&self.buf, pos);
                if pos >= self.buf.len() {
                    return Err(("unexpected end of object".to_string(), pos));
                }
                match self.buf[pos] {
                    b',' => {
                        pos += 1;
                    }
                    b'}' => {
                        return Ok((o, pos + 1));
                    }
                    _ => return Err(("missing ',' after object value".to_string(), pos)),
                }
            }
        }

        /// Go `Value.Type()`: lazily unescapes raw strings in-place.
        pub(crate) fn value_type(&mut self, v: u32) -> JsonType {
            if self.vd(v).t == JsonType::RawString {
                let span = self.vd(v).s;
                let ns = unescape_string_best_effort(&mut self.buf, span);
                let vd = &mut self.c.vs[v as usize];
                vd.s = ns;
                vd.t = JsonType::String;
            }
            self.vd(v).t
        }

        /// Go `Value.Object()`.
        pub(crate) fn object(&mut self, v: u32) -> Result<u32, String> {
            if self.vd(v).t != JsonType::Object {
                return Err(format!(
                    "value doesn't contain object; it contains {}",
                    self.value_type(v)
                ));
            }
            Ok(v)
        }

        /// Go `Object.Len()`.
        pub(crate) fn object_len(&self, o: u32) -> usize {
            self.vd(o).kvs.len()
        }

        /// Go `Object.unescapeKeys()` (called by `Object.Visit`).
        pub(crate) fn unescape_keys(&mut self, o: u32) {
            if self.vd(o).keys_unescaped {
                return;
            }
            for i in 0..self.object_len(o) {
                let span = self.c.vs[o as usize].kvs[i].k;
                let ns = unescape_string_best_effort(&mut self.buf, span);
                self.c.vs[o as usize].kvs[i].k = ns;
            }
            self.c.vs[o as usize].keys_unescaped = true;
        }

        /// Returns the i-th (key, value) pair of the object.
        pub(crate) fn kv(&self, o: u32, i: usize) -> (StrSpan, u32) {
            let kv = &self.vd(o).kvs[i];
            (kv.k, kv.v)
        }

        /// Number of elements in the JSON array `v`. Returns 0 if `v` is not
        /// an array. Mirrors the `object_len` accessor for arrays so
        /// `filter_json_array_contains_any` can iterate elements.
        pub(crate) fn array_len(&self, v: u32) -> usize {
            self.vd(v).a.len()
        }

        /// The i-th element handle of the JSON array `v` (Go `Array[i]`).
        pub(crate) fn array_element(&self, v: u32, i: usize) -> u32 {
            self.vd(v).a[i]
        }

        pub(crate) fn str_bytes(&self, span: StrSpan) -> &[u8] {
            &self.buf[span.start..span.end]
        }

        /// Go `Value.GetStringBytes()`: the (unescaped) string contents.
        pub(crate) fn string_span(&mut self, v: u32) -> StrSpan {
            let _ = self.value_type(v); // ensure the raw string is unescaped
            self.vd(v).s
        }

        /// Go `Value.MarshalTo`.
        pub(crate) fn marshal_value_to(&self, v: u32, dst: &mut Vec<u8>) {
            let vd = self.vd(v);
            match vd.t {
                JsonType::RawString => {
                    dst.push(b'"');
                    dst.extend_from_slice(self.str_bytes(vd.s));
                    dst.push(b'"');
                }
                JsonType::Object => self.marshal_object_to(v, dst),
                JsonType::Array => {
                    dst.push(b'[');
                    for (i, &vv) in vd.a.iter().enumerate() {
                        self.marshal_value_to(vv, dst);
                        if i != vd.a.len() - 1 {
                            dst.push(b',');
                        }
                    }
                    dst.push(b']');
                }
                JsonType::String => escape_string(dst, self.str_bytes(vd.s)),
                JsonType::Number => dst.extend_from_slice(self.str_bytes(vd.s)),
                JsonType::True => dst.extend_from_slice(b"true"),
                JsonType::False => dst.extend_from_slice(b"false"),
                JsonType::Null => dst.extend_from_slice(b"null"),
            }
        }

        /// Go `Object.MarshalTo`.
        pub(crate) fn marshal_object_to(&self, o: u32, dst: &mut Vec<u8>) {
            dst.push(b'{');
            let vd = self.vd(o);
            for (i, kv) in vd.kvs.iter().enumerate() {
                if vd.keys_unescaped {
                    escape_string(dst, self.str_bytes(kv.k));
                } else {
                    dst.push(b'"');
                    dst.extend_from_slice(self.str_bytes(kv.k));
                    dst.push(b'"');
                }
                dst.push(b':');
                self.marshal_value_to(kv.v, dst);
                if i != vd.kvs.len() - 1 {
                    dst.push(b',');
                }
            }
            dst.push(b'}');
        }
    }

    /// Port of Go `fastjson.Parser`.
    ///
    /// The Parser may be re-used for subsequent parsing.
    #[derive(Default)]
    pub(crate) struct Parser {
        pub(crate) doc: Doc,
    }

    impl Parser {
        /// Go `Parser.Parse` / `Parser.ParseBytes`; returns the root value index.
        ///
        /// The returned value is valid until the next call to `parse()`.
        pub(crate) fn parse(&mut self, s: &[u8]) -> Result<u32, String> {
            let s = &s[skip_ws(s, 0)..];
            self.doc.buf.clear();
            self.doc.buf.extend_from_slice(s);
            self.doc.c.reset();

            let (v, tail) = match self.doc.parse_value(0, 0) {
                Ok(x) => x,
                Err((e, pos)) => {
                    return Err(format!(
                        "cannot parse JSON: {e}; unparsed tail: {}",
                        quote_bytes(&start_end_bytes(&self.doc.buf[pos..]))
                    ));
                }
            };
            let tail = skip_ws(&self.doc.buf, tail);
            if tail < self.doc.buf.len() {
                return Err(format!(
                    "unexpected tail: {}",
                    quote_bytes(&start_end_bytes(&self.doc.buf[tail..]))
                ));
            }
            Ok(v)
        }
    }

    /// Port of Go `fastjson.Scanner`: scans a series of JSON values, which
    /// may be delimited by whitespace (JSON lines).
    #[derive(Default)]
    pub(crate) struct Scanner {
        pub(crate) doc: Doc,
        pos: usize,
        eof: bool,
        err: Option<String>,
        v: u32,
    }

    impl Scanner {
        /// Go `Scanner.Init` / `Scanner.InitBytes`.
        pub(crate) fn init_bytes(&mut self, b: &[u8]) {
            self.doc.buf.clear();
            self.doc.buf.extend_from_slice(b);
            self.pos = 0;
            self.eof = false;
            self.err = None;
            self.v = 0;
        }

        /// Go `Scanner.Next`: parses the next JSON value. Returns false
        /// either on error or on the end of input.
        pub(crate) fn next(&mut self) -> bool {
            if self.err.is_some() || self.eof {
                return false;
            }

            self.pos = skip_ws(&self.doc.buf, self.pos);
            if self.pos >= self.doc.buf.len() {
                // PORT NOTE: Go stores a sentinel errEOF; the port tracks
                // end-of-input with a separate flag.
                self.eof = true;
                return false;
            }

            self.doc.c.reset();
            match self.doc.parse_value(self.pos, 0) {
                Err((e, _)) => {
                    self.err = Some(e);
                    false
                }
                Ok((v, tail)) => {
                    self.pos = tail;
                    self.v = v;
                    true
                }
            }
        }

        /// Go `Scanner.Error`: `None` at normal end of input.
        pub(crate) fn error(&self) -> Option<&str> {
            self.err.as_deref()
        }

        /// Go `Scanner.Value`: the last parsed value, valid until `next()`.
        pub(crate) fn value(&self) -> u32 {
            self.v
        }
    }

    fn skip_ws(b: &[u8], mut pos: usize) -> usize {
        while pos < b.len()
            && (b[pos] == 0x20 || b[pos] == 0x0A || b[pos] == 0x09 || b[pos] == 0x0D)
        {
            pos += 1;
        }
        pos
    }

    fn find_byte(b: &[u8], x: u8) -> Option<usize> {
        b.iter().position(|&c| c == x)
    }

    /// parse_raw_key is similar to parse_raw_string, but is optimized
    /// for small-sized keys without escape sequences.
    fn parse_raw_key(b: &[u8], pos: usize) -> Result<(StrSpan, usize), PErr> {
        for i in pos..b.len() {
            if b[i] == b'"' {
                // Fast path.
                return Ok((StrSpan { start: pos, end: i }, i + 1));
            }
            if b[i] == b'\\' {
                // Slow path.
                return parse_raw_string(b, pos);
            }
        }
        Err(("missing closing '\"'".to_string(), b.len()))
    }

    fn parse_raw_string(b: &[u8], pos: usize) -> Result<(StrSpan, usize), PErr> {
        let Some(n) = find_byte(&b[pos..], b'"') else {
            return Err(("missing closing '\"'".to_string(), b.len()));
        };
        let mut n_abs = pos + n;
        if n == 0 || b[n_abs - 1] != b'\\' {
            // Fast path. No escaped ".
            return Ok((
                StrSpan {
                    start: pos,
                    end: n_abs,
                },
                n_abs + 1,
            ));
        }

        // Slow path - possible escaped " found.
        let mut cur = pos;
        loop {
            // Count the preceding backslashes: an even number means the
            // quote is not escaped.
            let mut i = n_abs - 1;
            while i > cur && b[i - 1] == b'\\' {
                i -= 1;
            }
            if (n_abs - i).is_multiple_of(2) {
                return Ok((
                    StrSpan {
                        start: pos,
                        end: n_abs,
                    },
                    n_abs + 1,
                ));
            }
            cur = n_abs + 1;

            let Some(n) = find_byte(&b[cur..], b'"') else {
                return Err(("missing closing '\"'".to_string(), b.len()));
            };
            n_abs = cur + n;
            if n == 0 || b[n_abs - 1] != b'\\' {
                return Ok((
                    StrSpan {
                        start: pos,
                        end: n_abs,
                    },
                    n_abs + 1,
                ));
            }
        }
    }

    fn parse_raw_number(b: &[u8], pos: usize) -> Result<(StrSpan, usize), PErr> {
        // The caller must ensure pos < b.len()

        // Find the end of the number.
        for i in 0..b.len() - pos {
            let ch = b[pos + i];
            if ch.is_ascii_digit()
                || ch == b'.'
                || ch == b'-'
                || ch == b'e'
                || ch == b'E'
                || ch == b'+'
            {
                continue;
            }
            if i == 0 || i == 1 && (b[pos] == b'-' || b[pos] == b'+') {
                if b.len() - (pos + i) >= 3 {
                    let xs = &b[pos + i..pos + i + 3];
                    if xs.eq_ignore_ascii_case(b"inf") || xs.eq_ignore_ascii_case(b"nan") {
                        return Ok((
                            StrSpan {
                                start: pos,
                                end: pos + i + 3,
                            },
                            pos + i + 3,
                        ));
                    }
                }
                return Err((
                    format!("unexpected char: {}", quote_bytes(&b[pos..pos + 1])),
                    pos,
                ));
            }
            return Ok((
                StrSpan {
                    start: pos,
                    end: pos + i,
                },
                pos + i,
            ));
        }
        Ok((
            StrSpan {
                start: pos,
                end: b.len(),
            },
            b.len(),
        ))
    }

    /// Port of fastjson's `unescapeStringBestEffort`: unescapes the string at
    /// `span` in place (the unescaped form never grows), returning the new
    /// (shorter or equal) span.
    fn unescape_string_best_effort(buf: &mut [u8], span: StrSpan) -> StrSpan {
        let (start, end) = (span.start, span.end);
        let Some(off) = find_byte(&buf[start..end], b'\\') else {
            // Fast path - nothing to unescape.
            return span;
        };

        // Slow path - unescape string.
        let mut w = start + off; // write position
        let mut r = w + 1; // read position (skip the backslash)
        while r < end {
            let ch = buf[r];
            r += 1;
            match ch {
                b'"' => {
                    buf[w] = b'"';
                    w += 1;
                }
                b'\\' => {
                    buf[w] = b'\\';
                    w += 1;
                }
                b'/' => {
                    buf[w] = b'/';
                    w += 1;
                }
                b'b' => {
                    buf[w] = 0x08;
                    w += 1;
                }
                b'f' => {
                    buf[w] = 0x0C;
                    w += 1;
                }
                b'n' => {
                    buf[w] = b'\n';
                    w += 1;
                }
                b'r' => {
                    buf[w] = b'\r';
                    w += 1;
                }
                b't' => {
                    buf[w] = b'\t';
                    w += 1;
                }
                b'u' => {
                    if end - r < 4 {
                        // Too short escape sequence. Just store it unchanged.
                        buf[w] = b'\\';
                        buf[w + 1] = b'u';
                        w += 2;
                    } else if let Some(x) = parse_hex4(&buf[r..r + 4]) {
                        let xs_start = r;
                        r += 4;
                        if !(0xD800..0xE000).contains(&x) {
                            let ch = char::from_u32(x).unwrap_or('\u{FFFD}');
                            w = write_utf8(buf, w, ch);
                        } else {
                            // Surrogate.
                            // See https://en.wikipedia.org/wiki/Universal_Character_Set_characters#Surrogates
                            if end - r < 6 || buf[r] != b'\\' || buf[r + 1] != b'u' {
                                buf[w] = b'\\';
                                buf[w + 1] = b'u';
                                w += 2;
                                for j in 0..4 {
                                    buf[w + j] = buf[xs_start + j];
                                }
                                w += 4;
                            } else if let Some(x1) = parse_hex4(&buf[r + 2..r + 6]) {
                                // utf16.DecodeRune: replacement char unless
                                // (x, x1) is a valid (high, low) pair.
                                let ch = if (0xD800..0xDC00).contains(&x)
                                    && (0xDC00..0xE000).contains(&x1)
                                {
                                    char::from_u32(0x10000 + ((x - 0xD800) << 10) + (x1 - 0xDC00))
                                        .unwrap_or('\u{FFFD}')
                                } else {
                                    '\u{FFFD}'
                                };
                                w = write_utf8(buf, w, ch);
                                r += 6;
                            } else {
                                buf[w] = b'\\';
                                buf[w + 1] = b'u';
                                w += 2;
                                for j in 0..4 {
                                    buf[w + j] = buf[xs_start + j];
                                }
                                w += 4;
                            }
                        }
                    } else {
                        // Invalid escape sequence. Just store it unchanged
                        // (the hex chars are copied by the chunk copy below).
                        buf[w] = b'\\';
                        buf[w + 1] = b'u';
                        w += 2;
                    }
                }
                _ => {
                    // Unknown escape sequence. Just store it unchanged.
                    buf[w] = b'\\';
                    buf[w + 1] = ch;
                    w += 2;
                }
            }

            // Copy the chunk up to the next backslash.
            match find_byte(&buf[r..end], b'\\') {
                None => {
                    buf.copy_within(r..end, w);
                    w += end - r;
                    break;
                }
                Some(n) => {
                    buf.copy_within(r..r + n, w);
                    w += n;
                    r += n + 1;
                }
            }
        }
        StrSpan { start, end: w }
    }

    fn parse_hex4(b: &[u8]) -> Option<u32> {
        let mut v: u32 = 0;
        for &ch in b {
            let x = match ch {
                b'0'..=b'9' => (ch - b'0') as u32,
                b'a'..=b'f' => (ch - b'a') as u32 + 10,
                b'A'..=b'F' => (ch - b'A') as u32 + 10,
                _ => return None,
            };
            v = v << 4 | x;
        }
        Some(v)
    }

    fn write_utf8(buf: &mut [u8], w: usize, ch: char) -> usize {
        let mut tmp = [0u8; 4];
        let enc = ch.encode_utf8(&mut tmp).as_bytes();
        buf[w..w + enc.len()].copy_from_slice(enc);
        w + enc.len()
    }

    fn escape_string(dst: &mut Vec<u8>, s: &[u8]) {
        if !has_special_chars(s) {
            // Fast path - nothing to escape.
            dst.push(b'"');
            dst.extend_from_slice(s);
            dst.push(b'"');
            return;
        }

        // Slow path — Go strconv.AppendQuote: ASCII control chars use the same
        // two-char escapes; a non-ASCII rune is kept raw when strconv.IsPrint,
        // else \uXXXX (BMP) / \UXXXXXXXX (astral); an invalid byte becomes \xHH.
        // Only reachable for JSON object keys containing escapes.
        dst.push(b'"');
        let mut i = 0;
        while i < s.len() {
            let ch = s[i];
            if ch < 0x80 {
                match ch {
                    b'"' => dst.extend_from_slice(b"\\\""),
                    b'\\' => dst.extend_from_slice(b"\\\\"),
                    0x07 => dst.extend_from_slice(b"\\a"),
                    0x08 => dst.extend_from_slice(b"\\b"),
                    0x0C => dst.extend_from_slice(b"\\f"),
                    b'\n' => dst.extend_from_slice(b"\\n"),
                    b'\r' => dst.extend_from_slice(b"\\r"),
                    b'\t' => dst.extend_from_slice(b"\\t"),
                    0x0B => dst.extend_from_slice(b"\\v"),
                    0x20..=0x7E => dst.push(ch),
                    _ => {
                        dst.extend_from_slice(format!("\\x{ch:02x}").as_bytes());
                    }
                }
                i += 1;
            } else {
                let n = (s.len() - i).min(4);
                let valid_char = match std::str::from_utf8(&s[i..i + n]) {
                    Ok(st) => st.chars().next(),
                    Err(e) if e.valid_up_to() > 0 => {
                        std::str::from_utf8(&s[i..i + e.valid_up_to()])
                            .expect("BUG: valid prefix")
                            .chars()
                            .next()
                    }
                    Err(_) => None,
                };
                match valid_char {
                    Some(c) => {
                        append_quoted_rune(dst, c);
                        i += c.len_utf8();
                    }
                    None => {
                        // Invalid byte -> \xHH (Go RuneError, width 1).
                        dst.extend_from_slice(format!("\\x{ch:02x}").as_bytes());
                        i += 1;
                    }
                }
            }
        }
        dst.push(b'"');
    }

    /// Appends a non-ASCII rune to `dst` the way Go `strconv.AppendQuote`
    /// does: raw when `strconv.IsPrint`, else `\uXXXX` (BMP) / `\UXXXXXXXX`
    /// (astral) with lowercase hex.
    fn append_quoted_rune(dst: &mut Vec<u8>, c: char) {
        if esl_common::strconv_isprint::is_print_char(c) {
            let mut tmp = [0u8; 4];
            dst.extend_from_slice(c.encode_utf8(&mut tmp).as_bytes());
            return;
        }
        let r = c as u32;
        if r < 0x10000 {
            dst.extend_from_slice(format!("\\u{r:04x}").as_bytes());
        } else {
            dst.extend_from_slice(format!("\\U{r:08x}").as_bytes());
        }
    }

    fn has_special_chars(s: &[u8]) -> bool {
        s.iter().any(|&c| c == b'"' || c == b'\\' || c < 0x20)
    }

    /// PORT NOTE: approximation of Go's `%q` formatting used in error
    /// messages (tests never assert on the quoted representation).
    fn quote_bytes(b: &[u8]) -> String {
        let mut dst = Vec::new();
        escape_string(&mut dst, b);
        String::from_utf8_lossy(&dst).into_owned()
    }

    const MAX_START_END_STRING_LEN: usize = 80;

    /// Port of fastjson's `startEndString`.
    fn start_end_bytes(b: &[u8]) -> Vec<u8> {
        if b.len() <= MAX_START_END_STRING_LEN {
            return b.to_vec();
        }
        let mut out = b[..40].to_vec();
        out.extend_from_slice(b"...");
        out.extend_from_slice(&b[b.len() - 40..]);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_json_parser_failure() {
        fn f(data: &str) {
            let mut p = get_json_parser();
            let err = p.parse_log_message(data.as_bytes(), &[], "");
            assert!(err.is_err(), "expecting non-nil error for {data:?}");
            put_json_parser(p);
        }
        f("");
        f("{foo");
        f("[1,2,3]");
        f("{\"foo\",}");
    }

    // PORT-ONLY TEST: upstream has no invalid-UTF-8 coverage. Pins that both
    // field NAMES and VALUES are raw bytes (Go strings are arbitrary bytes):
    // an invalid-UTF-8 JSON key and value are ingested byte-verbatim.
    #[test]
    fn test_json_parser_invalid_utf8_raw_bytes() {
        let mut p = get_json_parser();
        // {"a\xffb":"x\xffy"} with a raw 0xFF byte in both key and value.
        let data = b"{\"a\xffb\":\"x\xffy\"}";
        p.parse_log_message(data, &[], "")
            .unwrap_or_else(|e| panic!("unexpected error: {e}"));
        assert_eq!(
            p.fields(),
            &[Field {
                name: b"a\xffb".to_vec(),
                value: b"x\xffy".to_vec(),
            }],
            "unexpected fields: {:?}",
            p.fields()
        );
        put_json_parser(p);
    }

    #[test]
    fn test_json_parser_success() {
        fn f(data: &str, preserve_keys: &[&str], field_prefix: &str, fields_expected: &[Field]) {
            let mut p = get_json_parser();
            let preserve_keys: Vec<&[u8]> = preserve_keys.iter().map(|s| s.as_bytes()).collect();
            p.parse_log_message(data.as_bytes(), &preserve_keys, field_prefix)
                .unwrap_or_else(|e| panic!("unexpected error: {e}"));
            assert_eq!(
                p.fields(),
                fields_expected,
                "unexpected fields;\ngot\n{:?}\nwant\n{fields_expected:?}",
                p.fields()
            );
            put_json_parser(p);
        }

        f("{}", &[], "", &[]);
        f("{\"foo\":\"bar\"}", &[], "", &[field("foo", "bar")]);
        f(
            "{\"foo\":{\"bar\":{\"x\":\"y\",\"z\":[\"foo\"]}},\"a\":1,\"b\":true,\"c\":[1,2],\"d\":false,\"e\":null}",
            &[],
            "",
            &[
                field("foo.bar.x", "y"),
                field("foo.bar.z", "[\"foo\"]"),
                field("a", "1"),
                field("b", "true"),
                field("c", "[1,2]"),
                field("d", "false"),
            ],
        );

        // add prefix to the parsed field names
        f(
            "{\"foo\":{\"bar\":{\"x\":\"y\",\"z\":[\"foo\"]}},\"a\":1,\"b\":true,\"c\":[1,2],\"d\":false,\"e\":null}",
            &[],
            "qwe.",
            &[
                field("qwe.foo.bar.x", "y"),
                field("qwe.foo.bar.z", "[\"foo\"]"),
                field("qwe.a", "1"),
                field("qwe.b", "true"),
                field("qwe.c", "[1,2]"),
                field("qwe.d", "false"),
            ],
        );

        // preserve foo
        f(
            "{\"foo\":{\"bar\":{\"x\":\"y\",\"z\":[\"foo\"]}},\"a\":1,\"b\":true,\"c\":[1,2],\"d\":false,\"e\":null}",
            &["foo"],
            "",
            &[
                field("foo", "{\"bar\":{\"x\":\"y\",\"z\":[\"foo\"]}}"),
                field("a", "1"),
                field("b", "true"),
                field("c", "[1,2]"),
                field("d", "false"),
            ],
        );

        // preserve foo and add prefix to the parsed fields
        f(
            "{\"foo\":{\"bar\":{\"x\":\"y\",\"z\":[\"foo\"]}},\"a\":1,\"b\":true,\"c\":[1,2],\"d\":false,\"e\":null}",
            &["foo"],
            "qwe_",
            &[
                field("qwe_foo", "{\"bar\":{\"x\":\"y\",\"z\":[\"foo\"]}}"),
                field("qwe_a", "1"),
                field("qwe_b", "true"),
                field("qwe_c", "[1,2]"),
                field("qwe_d", "false"),
            ],
        );

        // preserve foo.bar
        f(
            "{\"foo\":{\"bar\":{\"x\":\"y\",\"z\":[\"foo\"]}},\"a\":1,\"b\":true,\"c\":[1,2],\"d\":false,\"e\":null}",
            &["foo.bar"],
            "",
            &[
                field("foo.bar", "{\"x\":\"y\",\"z\":[\"foo\"]}"),
                field("a", "1"),
                field("b", "true"),
                field("c", "[1,2]"),
                field("d", "false"),
            ],
        );
    }

    #[test]
    fn test_json_parser_too_long_field_name() {
        fn f(data: &str, max_field_len: usize, fields_expected: &[Field]) {
            let mut p = get_json_parser();
            p.parse_log_message_impl(data.as_bytes(), &[], "", max_field_len)
                .unwrap_or_else(|e| panic!("unexpected error: {e}"));
            assert_eq!(
                p.fields(),
                fields_expected,
                "unexpected fields;\ngot\n{:?}\nwant\n{fields_expected:?}",
                p.fields()
            );
            put_json_parser(p);
        }

        f(
            "{\"foo\":\"bar\",\"baz\":{\"abc\":\"y\"}}",
            7,
            &[field("foo", "bar"), field("baz.abc", "y")],
        );
        f(
            "{\"foo\":\"bar\",\"baz\":{\"abc\":\"y\"}}",
            6,
            &[field("foo", "bar"), field("baz", "{\"abc\":\"y\"}")],
        );
        f(
            "{\"foo\":\"bar\",\"baz\":{\"abc\":\"y\"}}",
            3,
            &[field("foo", "bar"), field("baz", "{\"abc\":\"y\"}")],
        );
        f(
            "{\"foo\":\"bar\",\"baz\":{\"abc\":\"y\"}}",
            2,
            &[field("_msg", "{\"foo\":\"bar\",\"baz\":{\"abc\":\"y\"}}")],
        );
    }
}

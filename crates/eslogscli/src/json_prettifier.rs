//! Port of `app/eslogscli/json_prettifier.go`.

use std::io::{self, BufRead, BufReader, Read};

use esl_logstorage::rows::{Field, marshal_fields_to_logfmt};

/// Port of Go `outputMode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputMode {
    JsonMultiline,
    JsonSingleline,
    Logfmt,
    Compact,
}

type Formatter = fn(&mut Vec<u8>, &[Field]) -> io::Result<()>;

fn get_output_formatter(output_mode: OutputMode) -> Formatter {
    match output_mode {
        OutputMode::JsonMultiline => |w, fields| write_json_object(w, fields, true),
        OutputMode::JsonSingleline => |w, fields| write_json_object(w, fields, false),
        OutputMode::Logfmt => write_logfmt_object,
        OutputMode::Compact => write_compact_object,
    }
}

/// Reads a stream of JSON objects (one per response line) from `r` and exposes
/// the formatted output via [`Read`].
///
/// PORT NOTE: Go pumps the formatted output through a goroutine plus
/// `io.Pipe`; the port formats the next JSON object lazily inside `Read`
/// instead, which preserves the streaming behavior without a thread.
pub struct JsonPrettifier {
    d: JsonDecoder<BufReader<Box<dyn Read>>>,
    formatter: Formatter,

    buf: Vec<u8>,
    buf_pos: usize,
}

impl JsonPrettifier {
    pub fn new(r: Box<dyn Read>, output_mode: OutputMode) -> JsonPrettifier {
        JsonPrettifier {
            d: JsonDecoder::new(BufReader::new(r)),
            formatter: get_output_formatter(output_mode),
            buf: Vec::new(),
            buf_pos: 0,
        }
    }
}

impl Read for JsonPrettifier {
    fn read(&mut self, p: &mut [u8]) -> io::Result<usize> {
        while self.buf_pos >= self.buf.len() {
            if !self.d.more()? {
                return Ok(0);
            }
            let fields = read_next_json_object(&mut self.d).map_err(io::Error::other)?;
            self.buf.clear();
            self.buf_pos = 0;
            (self.formatter)(&mut self.buf, &fields)?;
        }
        let n = (self.buf.len() - self.buf_pos).min(p.len());
        p[..n].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + n]);
        self.buf_pos += n;
        Ok(n)
    }
}

/// A JSON token, covering the subset produced by `encoding/json`'s
/// `Decoder.Token` that `readNextJSONObject` distinguishes.
#[derive(Debug, PartialEq, Eq)]
enum Token {
    Delim(char),
    Str(String),
    /// A non-string scalar (number, `true`, `false`, `null`); kept as its raw
    /// text purely for error messages, like Go's `%v` on the token.
    Other(String),
}

impl Token {
    fn describe(&self) -> String {
        match self {
            Token::Delim(c) => c.to_string(),
            Token::Str(s) => s.clone(),
            Token::Other(s) => s.clone(),
        }
    }
}

/// Minimal streaming JSON tokenizer mirroring the subset of
/// `encoding/json.Decoder` used by Go's `readNextJSONObject`: `More()` plus
/// `Token()` over a stream of flat objects with string values.
///
/// PORT NOTE: like Go's decoder, `,` and `:` separators are consumed
/// implicitly by `token()`.
struct JsonDecoder<R: BufRead> {
    r: R,
    peeked: Option<u8>,
}

impl<R: BufRead> JsonDecoder<R> {
    fn new(r: R) -> JsonDecoder<R> {
        JsonDecoder { r, peeked: None }
    }

    fn next_byte(&mut self) -> io::Result<Option<u8>> {
        if let Some(b) = self.peeked.take() {
            return Ok(Some(b));
        }
        let mut b = [0u8; 1];
        loop {
            match self.r.read(&mut b) {
                Ok(0) => return Ok(None),
                Ok(_) => return Ok(Some(b[0])),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
    }

    /// Returns the next non-whitespace byte without consuming it.
    fn peek_non_space(&mut self) -> io::Result<Option<u8>> {
        loop {
            match self.next_byte()? {
                None => return Ok(None),
                Some(b) if b.is_ascii_whitespace() => continue,
                Some(b) => {
                    self.peeked = Some(b);
                    return Ok(Some(b));
                }
            }
        }
    }

    /// Reports whether there is another value in the input stream
    /// (Go `Decoder.More`).
    fn more(&mut self) -> io::Result<bool> {
        Ok(self.peek_non_space()?.is_some())
    }

    /// Reads the next JSON token, skipping `,` and `:` separators like Go's
    /// `Decoder.Token`.
    fn token(&mut self) -> Result<Token, String> {
        loop {
            let b = match self.peek_non_space().map_err(|e| e.to_string())? {
                Some(b) => b,
                None => return Err("EOF".to_string()),
            };
            self.peeked = None;
            match b {
                b',' | b':' => continue,
                b'{' | b'}' | b'[' | b']' => return Ok(Token::Delim(b as char)),
                b'"' => return Ok(Token::Str(self.read_string_tail()?)),
                _ => return Ok(Token::Other(self.read_bare_tail(b)?)),
            }
        }
    }

    /// Reads the remainder of a JSON string after the opening `"`.
    fn read_string_tail(&mut self) -> Result<String, String> {
        let mut raw = Vec::new();
        loop {
            let b = self
                .next_byte()
                .map_err(|e| e.to_string())?
                .ok_or_else(|| "unexpected EOF inside JSON string".to_string())?;
            match b {
                b'"' => break,
                b'\\' => {
                    let esc = self
                        .next_byte()
                        .map_err(|e| e.to_string())?
                        .ok_or_else(|| "unexpected EOF inside JSON string escape".to_string())?;
                    match esc {
                        b'"' => raw.push(b'"'),
                        b'\\' => raw.push(b'\\'),
                        b'/' => raw.push(b'/'),
                        b'b' => raw.push(0x08),
                        b'f' => raw.push(0x0c),
                        b'n' => raw.push(b'\n'),
                        b'r' => raw.push(b'\r'),
                        b't' => raw.push(b'\t'),
                        b'u' => {
                            let cp = self.read_unicode_escape()?;
                            let c = if (0xd800..0xdc00).contains(&cp) {
                                // High surrogate; expect a low surrogate pair.
                                let (b1, b2) = (
                                    self.next_byte().map_err(|e| e.to_string())?,
                                    self.next_byte().map_err(|e| e.to_string())?,
                                );
                                if b1 != Some(b'\\') || b2 != Some(b'u') {
                                    return Err("invalid surrogate pair in JSON string".to_string());
                                }
                                let lo = self.read_unicode_escape()?;
                                if !(0xdc00..0xe000).contains(&lo) {
                                    return Err("invalid surrogate pair in JSON string".to_string());
                                }
                                let cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
                                char::from_u32(cp)
                                    .ok_or_else(|| "invalid unicode escape".to_string())?
                            } else if (0xdc00..0xe000).contains(&cp) {
                                // Lone low surrogate: mirror Go by replacing it
                                // with U+FFFD.
                                '\u{fffd}'
                            } else {
                                char::from_u32(cp)
                                    .ok_or_else(|| "invalid unicode escape".to_string())?
                            };
                            let mut cbuf = [0u8; 4];
                            raw.extend_from_slice(c.encode_utf8(&mut cbuf).as_bytes());
                        }
                        _ => {
                            return Err(format!(
                                "invalid escape char {:?} in JSON string",
                                esc as char
                            ));
                        }
                    }
                }
                _ => raw.push(b),
            }
        }
        String::from_utf8(raw).map_err(|_| "invalid UTF-8 in JSON string".to_string())
    }

    fn read_unicode_escape(&mut self) -> Result<u32, String> {
        let mut cp = 0u32;
        for _ in 0..4 {
            let b = self
                .next_byte()
                .map_err(|e| e.to_string())?
                .ok_or_else(|| "unexpected EOF inside \\u escape".to_string())?;
            let d = (b as char)
                .to_digit(16)
                .ok_or_else(|| format!("invalid hex digit {:?} in \\u escape", b as char))?;
            cp = cp * 16 + d;
        }
        Ok(cp)
    }

    /// Reads a bare scalar token (number, `true`, `false`, `null`) starting
    /// with `first`.
    fn read_bare_tail(&mut self, first: u8) -> Result<String, String> {
        let mut s = String::new();
        s.push(first as char);
        while let Some(b) = self.next_byte().map_err(|e| e.to_string())? {
            if b.is_ascii_whitespace() || matches!(b, b',' | b':' | b'}' | b']') {
                self.peeked = Some(b);
                break;
            }
            s.push(b as char);
        }
        Ok(s)
    }
}

fn read_next_json_object<R: BufRead>(d: &mut JsonDecoder<R>) -> Result<Vec<Field>, String> {
    let t = d
        .token()
        .map_err(|err| format!("cannot read '{{': {err}"))?;
    match t {
        Token::Delim('{') => {}
        t => {
            return Err(format!(
                "unexpected token read; got {:?}; want '{{'",
                t.describe()
            ));
        }
    }

    let mut fields = Vec::new();
    loop {
        // Read object key
        let t = d
            .token()
            .map_err(|err| format!("cannot read JSON object key or closing brace: {err}"))?;
        let key = match t {
            Token::Delim('}') => return Ok(fields),
            Token::Delim(delim) => {
                return Err(format!(
                    "unexpected delimiter read; got {delim:?}; want '}}'"
                ));
            }
            Token::Str(key) => key,
            t => {
                return Err(format!(
                    "unexpected token read for object key: {}; want string or '}}'",
                    t.describe()
                ));
            }
        };

        // read object value
        let t = d
            .token()
            .map_err(|err| format!("cannot read JSON object value: {err}"))?;
        let value = match t {
            Token::Str(value) => value,
            t => {
                return Err(format!(
                    "unexpected token read for object value: {}; want string",
                    t.describe()
                ));
            }
        };

        fields.push(Field { name: key, value });
    }
}

fn write_logfmt_object(w: &mut Vec<u8>, fields: &[Field]) -> io::Result<()> {
    marshal_fields_to_logfmt(w, fields);
    w.push(b'\n');
    Ok(())
}

fn write_compact_object(w: &mut Vec<u8>, fields: &[Field]) -> io::Result<()> {
    if fields.len() == 1 {
        // Just write field value as is without name
        w.extend_from_slice(fields[0].value.as_bytes());
        w.push(b'\n');
        return Ok(());
    }
    if fields.len() == 2 && (fields[0].name == "_time" || fields[1].name == "_time") {
        // Write _time\tfieldValue as is
        let (first, second) = if fields[0].name == "_time" {
            (&fields[0], &fields[1])
        } else {
            (&fields[1], &fields[0])
        };
        w.extend_from_slice(first.value.as_bytes());
        w.push(b'\t');
        w.extend_from_slice(second.value.as_bytes());
        w.push(b'\n');
        return Ok(());
    }

    // Fall back to logfmt
    write_logfmt_object(w, fields)
}

fn write_json_object(w: &mut Vec<u8>, fields: &[Field], is_multiline: bool) -> io::Result<()> {
    if fields.is_empty() {
        w.extend_from_slice(b"{}\n");
        return Ok(());
    }

    w.push(b'{');
    write_newline_if_needed(w, is_multiline);
    write_json_object_key_value(w, &fields[0], is_multiline);
    for f in &fields[1..] {
        w.push(b',');
        write_newline_if_needed(w, is_multiline);
        write_json_object_key_value(w, f, is_multiline);
    }
    write_newline_if_needed(w, is_multiline);
    w.extend_from_slice(b"}\n");
    Ok(())
}

fn write_newline_if_needed(w: &mut Vec<u8>, is_multiline: bool) {
    if is_multiline {
        w.push(b'\n');
    }
}

fn write_json_object_key_value(w: &mut Vec<u8>, f: &Field, is_multiline: bool) {
    let key = get_json_string(&f.name);
    let value = get_json_string(&f.value);
    if is_multiline {
        w.extend_from_slice(b"  ");
        w.extend_from_slice(key.as_bytes());
        w.extend_from_slice(b": ");
        w.extend_from_slice(value.as_bytes());
    } else {
        w.extend_from_slice(key.as_bytes());
        w.push(b':');
        w.extend_from_slice(value.as_bytes());
    }
}

/// Marshals `s` to a quoted JSON string.
///
/// PORT NOTE: Go marshals with `encoding/json` (which HTML-escapes `<`, `>`
/// and `&`) and then undoes the HTML escaping via `jsonHTMLReplacer`; the port
/// simply never HTML-escapes, producing the same final output.
fn get_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            // encoding/json escapes U+2028 and U+2029 for JS compatibility.
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(pairs: &[(&str, &str)]) -> Vec<Field> {
        pairs
            .iter()
            .map(|(name, value)| Field {
                name: name.to_string(),
                value: value.to_string(),
            })
            .collect()
    }

    fn decode_all(input: &str) -> Result<Vec<Vec<Field>>, String> {
        let mut d = JsonDecoder::new(BufReader::new(input.as_bytes()));
        let mut objects = Vec::new();
        while d.more().map_err(|e| e.to_string())? {
            objects.push(read_next_json_object(&mut d)?);
        }
        Ok(objects)
    }

    #[test]
    fn reads_stream_of_json_objects() {
        let objects = decode_all("{\"a\":\"b\"}\n{\"_time\":\"123\",\"_msg\":\"x y\"}\n").unwrap();
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0], fields(&[("a", "b")]));
        assert_eq!(objects[1], fields(&[("_time", "123"), ("_msg", "x y")]));
    }

    #[test]
    fn reads_empty_object_and_string_escapes() {
        let objects = decode_all(r#"{} {"k":"a\"b\\c\n\tAé"}"#).unwrap();
        assert_eq!(objects[0], Vec::new());
        assert_eq!(objects[1], fields(&[("k", "a\"b\\c\n\tA\u{e9}")]));
    }

    #[test]
    fn reads_surrogate_pair_escape() {
        let objects = decode_all(r#"{"k":"😀"}"#).unwrap();
        assert_eq!(objects[0], fields(&[("k", "\u{1f600}")]));
    }

    #[test]
    fn rejects_non_string_value() {
        let err = decode_all(r#"{"k":123}"#).unwrap_err();
        assert!(
            err.contains("unexpected token read for object value: 123; want string"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_non_object_input() {
        let err = decode_all(r#"["a"]"#).unwrap_err();
        assert!(
            err.contains("unexpected token read; got \"[\"; want '{'"),
            "unexpected error: {err}"
        );
    }

    fn format(mode: OutputMode, fields: &[Field]) -> String {
        let mut w = Vec::new();
        get_output_formatter(mode)(&mut w, fields).unwrap();
        String::from_utf8(w).unwrap()
    }

    #[test]
    fn formats_json_multiline() {
        let s = format(
            OutputMode::JsonMultiline,
            &fields(&[("_time", "t1"), ("_msg", "hello")]),
        );
        assert_eq!(s, "{\n  \"_time\": \"t1\",\n  \"_msg\": \"hello\"\n}\n");
    }

    #[test]
    fn formats_json_singleline() {
        let s = format(
            OutputMode::JsonSingleline,
            &fields(&[("_time", "t1"), ("_msg", "a<b>&c")]),
        );
        assert_eq!(s, "{\"_time\":\"t1\",\"_msg\":\"a<b>&c\"}\n");
    }

    #[test]
    fn formats_empty_json_object() {
        assert_eq!(format(OutputMode::JsonMultiline, &[]), "{}\n");
        assert_eq!(format(OutputMode::JsonSingleline, &[]), "{}\n");
    }

    #[test]
    fn formats_logfmt() {
        let s = format(
            OutputMode::Logfmt,
            &fields(&[("foo", "bar"), ("msg", "a b")]),
        );
        assert_eq!(s, "foo=bar msg=\"a b\"\n");
    }

    #[test]
    fn formats_compact_single_field() {
        let s = format(OutputMode::Compact, &fields(&[("_msg", "hello world")]));
        assert_eq!(s, "hello world\n");
    }

    #[test]
    fn formats_compact_time_pairs() {
        let s = format(
            OutputMode::Compact,
            &fields(&[("_time", "t1"), ("_msg", "hello")]),
        );
        assert_eq!(s, "t1\thello\n");
        let s = format(
            OutputMode::Compact,
            &fields(&[("_msg", "hello"), ("_time", "t1")]),
        );
        assert_eq!(s, "t1\thello\n");
    }

    #[test]
    fn formats_compact_fallback_to_logfmt() {
        let s = format(
            OutputMode::Compact,
            &fields(&[("a", "1"), ("b", "2"), ("c", "3")]),
        );
        assert_eq!(s, "a=1 b=2 c=3\n");
    }

    #[test]
    fn get_json_string_escapes() {
        assert_eq!(
            get_json_string("a\"b\\c\nd\u{1}"),
            "\"a\\\"b\\\\c\\nd\\u0001\""
        );
        assert_eq!(get_json_string("<>&"), "\"<>&\"");
    }

    #[test]
    fn prettifier_reads_formatted_stream() {
        let input: Box<dyn Read> = Box::new("{\"_msg\":\"a\"}\n{\"_msg\":\"b\"}\n".as_bytes());
        let mut jp = JsonPrettifier::new(input, OutputMode::Compact);
        let mut out = String::new();
        jp.read_to_string(&mut out).unwrap();
        assert_eq!(out, "a\nb\n");
    }

    #[test]
    fn prettifier_surfaces_parse_errors() {
        let input: Box<dyn Read> = Box::new("{\"k\":null}".as_bytes());
        let mut jp = JsonPrettifier::new(input, OutputMode::JsonMultiline);
        let mut out = String::new();
        let err = jp.read_to_string(&mut out).unwrap_err();
        assert!(err.to_string().contains("want string"));
    }
}

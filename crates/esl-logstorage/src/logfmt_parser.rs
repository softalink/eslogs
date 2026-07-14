//! Port of EsLogs `lib/logstorage/logfmt_parser.go`.

// TODO: remove once the upstream consumers of this module
// (pipe_unpack_logfmt.go, pipe_pack_logfmt.go, ...) are ported; until then
// the crate-private API is only exercised by the tests below.
#![allow(dead_code)]

use std::sync::Mutex;

use crate::filter_generic::decode_rune_at_end;
use crate::pattern::try_unquote_bytes;
use crate::pattern_matcher::decode_rune;
use crate::rows::Field;

/// Go `strings.TrimSpace` over raw bytes: trims leading/trailing Unicode
/// whitespace runes. Invalid UTF-8 bytes decode as `RuneError` (not
/// whitespace) and stop the trim, exactly like Go (whose `unicode.IsSpace`
/// never returns true for `RuneError`).
pub(crate) fn trim_space_bytes(mut b: &[u8]) -> &[u8] {
    while !b.is_empty() {
        let (r, size) = decode_rune(b);
        if size == 0 || !r.is_whitespace() {
            break;
        }
        b = &b[size..];
    }
    while !b.is_empty() {
        let r = decode_rune_at_end(b);
        if !r.is_whitespace() {
            break;
        }
        // A whitespace rune is always well-formed, so `len_utf8` is exact.
        b = &b[..b.len() - r.len_utf8()];
    }
    b
}

#[derive(Default)]
pub(crate) struct LogfmtParser {
    pub(crate) fields: Vec<Field>,

    /// PORT NOTE: recycles Field string capacities across parses (the Rust
    /// `Field` owns its strings, while Go's borrows from the input).
    spare_fields: Vec<Field>,
}

impl LogfmtParser {
    fn reset(&mut self) {
        self.spare_fields.append(&mut self.fields);
    }

    fn add_field(&mut self, name: &[u8], value: &[u8]) {
        let name = trim_space_bytes(name);
        if name.is_empty() && value.is_empty() {
            return;
        }
        let mut f = self.spare_fields.pop().unwrap_or_default();
        f.name.clear();
        f.name.extend_from_slice(name);
        f.value.clear();
        f.value.extend_from_slice(value);
        self.fields.push(f);
    }

    /// Parses raw logfmt bytes (Go strings are arbitrary bytes), so field
    /// values with invalid UTF-8 are preserved verbatim.
    pub(crate) fn parse(&mut self, s: &[u8]) {
        self.reset();
        let mut s = s;
        loop {
            // Search for field name
            let Some(n) = s.iter().position(|&c| c == b'=' || c == b' ') else {
                // empty value
                self.add_field(s, b"");
                return;
            };

            let name = &s[..n];
            let ch = s[n];
            s = &s[n + 1..];
            if ch == b' ' {
                // empty value
                self.add_field(name, b"");
                continue;
            }
            if s.is_empty() {
                self.add_field(name, b"");
                return;
            }

            // Search for field value
            match try_unquote_bytes(s, "") {
                Some((value, n_offset)) => {
                    self.add_field(name, &value);
                    s = &s[n_offset..];
                    if s.is_empty() {
                        return;
                    }
                    if s[0] != b' ' {
                        return;
                    }
                    s = &s[1..];
                }
                None => match s.iter().position(|&c| c == b' ') {
                    None => {
                        self.add_field(name, s);
                        return;
                    }
                    Some(n) => {
                        self.add_field(name, &s[..n]);
                        s = &s[n + 1..];
                    }
                },
            }
        }
    }
}

pub(crate) fn get_logfmt_parser() -> LogfmtParser {
    LOGFMT_PARSER_POOL.lock().unwrap().pop().unwrap_or_default()
}

pub(crate) fn put_logfmt_parser(mut p: LogfmtParser) {
    p.reset();
    LOGFMT_PARSER_POOL.lock().unwrap().push(p);
}

/// PORT NOTE: Go uses `sync.Pool`; the port uses a `Mutex<Vec<..>>` pool
/// handing parsers out by value (the established esl-common pattern).
static LOGFMT_PARSER_POOL: Mutex<Vec<LogfmtParser>> = Mutex::new(Vec::new());

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::marshal_fields_to_logfmt;

    #[test]
    fn test_logfmt_parser() {
        fn f(s: &str, result_expected: &str) {
            let mut p = get_logfmt_parser();

            p.parse(s.as_bytes());
            let mut result = Vec::new();
            marshal_fields_to_logfmt(&mut result, &p.fields);
            let result = String::from_utf8(result).unwrap();
            assert_eq!(
                result, result_expected,
                "unexpected result when parsing [{s}]; got\n{result}\nwant\n{result_expected}\n"
            );

            put_logfmt_parser(p);
        }

        f("", "");
        f("foo=bar", "foo=bar");
        f("foo=\"bar=baz x=y\"", "foo=\"bar=baz x=y\"");
        f("foo=", "foo=");
        f("foo", "foo=");
        f("foo bar", "foo= bar=");
        f("foo bar=baz", "foo= bar=baz");
        f("foo=bar baz=\"x y\" a=b", "foo=bar baz=\"x y\" a=b");
        f("  foo=bar  baz=x =z qwe", "foo=bar baz=x _msg=z qwe=");
    }

    #[test]
    fn test_logfmt_parser_invalid_utf8_value() {
        // A field value with invalid UTF-8 is preserved verbatim (Go parses
        // raw bytes); the byte-native parser does not lossily drop or replace.
        let mut p = get_logfmt_parser();
        p.parse(b"foo=a\xff\xfeb bar=baz");
        assert_eq!(p.fields.len(), 2);
        assert_eq!(p.fields[0].name, b"foo");
        assert_eq!(p.fields[0].value, b"a\xff\xfeb");
        assert_eq!(p.fields[1].name, b"bar");
        assert_eq!(p.fields[1].value, b"baz");
        put_logfmt_parser(p);

        // Quoted value carrying a raw-byte escape decodes to the raw byte.
        let mut p = get_logfmt_parser();
        p.parse(b"k=\"a\\xffb\"");
        assert_eq!(p.fields.len(), 1);
        assert_eq!(p.fields[0].value, b"a\xffb");
        put_logfmt_parser(p);
    }
}

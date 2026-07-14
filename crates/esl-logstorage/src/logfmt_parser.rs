//! Port of EsLogs `lib/logstorage/logfmt_parser.go`.

// TODO: remove once the upstream consumers of this module
// (pipe_unpack_logfmt.go, pipe_pack_logfmt.go, ...) are ported; until then
// the crate-private API is only exercised by the tests below.
#![allow(dead_code)]

use std::sync::Mutex;

use crate::pattern::try_unquote_string;
use crate::rows::Field;

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

    fn add_field(&mut self, name: &str, value: &str) {
        let name = name.trim();
        if name.is_empty() && value.is_empty() {
            return;
        }
        let mut f = self.spare_fields.pop().unwrap_or_default();
        f.name.clear();
        f.name.extend_from_slice(name.as_bytes());
        f.value.clear();
        f.value.extend_from_slice(value.as_bytes());
        self.fields.push(f);
    }

    pub(crate) fn parse(&mut self, s: &str) {
        self.reset();
        let mut s = s;
        loop {
            // Search for field name
            let Some(n) = s.find(['=', ' ']) else {
                // empty value
                self.add_field(s, "");
                return;
            };

            let name = &s[..n];
            let ch = s.as_bytes()[n];
            s = &s[n + 1..];
            if ch == b' ' {
                // empty value
                self.add_field(name, "");
                continue;
            }
            if s.is_empty() {
                self.add_field(name, "");
                return;
            }

            // Search for field value
            match try_unquote_string(s, "") {
                Some((value, n_offset)) => {
                    self.add_field(name, &value);
                    s = &s[n_offset..];
                    if s.is_empty() {
                        return;
                    }
                    if s.as_bytes()[0] != b' ' {
                        return;
                    }
                    s = &s[1..];
                }
                None => match s.find(' ') {
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

            p.parse(s);
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
}

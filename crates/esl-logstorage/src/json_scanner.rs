//! Port of EsLogs `lib/logstorage/json_scanner.go`.

use std::sync::Mutex;

use crate::consts::MAX_FIELD_NAME_SIZE;
use crate::json_parser::{CommonJson, fastjson};
use crate::rows::Field;

/// JSONScanner scans all JSON messages from a string in a streaming manner.
///
/// Call `init()` for initializing the scanner and then call `next_log_message()` for scanning
/// JSON messages one by one into the fields.
///
/// See <https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model>
///
/// Use `get_json_scanner()` for obtaining the scanner.
#[derive(Default)]
pub struct JSONScanner {
    /// PORT NOTE: Go embeds `commonJSON`; the port holds it as a field and
    /// exposes the parsed fields via `fields()`.
    pub(crate) common: CommonJson,

    /// s is used for JSON parsing
    s: fastjson::Scanner,

    /// err contains parsing error
    err: Option<String>,
}

impl JSONScanner {
    /// Fields contains the parsed JSON message after a `next_log_message()` call.
    pub fn fields(&self) -> &[Field] {
        &self.common.fields
    }

    /// Mutable access to the parsed fields.
    ///
    /// PORT NOTE: Go exposes `Fields` as a public struct field which callers
    /// (e.g. `app/eslinsert/splunk`) mutate via `ExtractTimestampFromFields` and
    /// `RenameField`; the port adds this accessor for the same purpose.
    pub fn fields_mut(&mut self) -> &mut [Field] {
        &mut self.common.fields
    }

    fn reset(&mut self) {
        self.common.reset();
        self.err = None;
    }

    /// Init initializes the scanner for scanning JSON messages from msg.
    ///
    /// Call `next_log_message()` for scanning the next JSON message into fields.
    pub fn init(&mut self, msg: &[u8], preserve_keys: &[&[u8]], field_prefix: &str) {
        self.reset();
        self.s.init_bytes(msg);
        self.common
            .init(preserve_keys, field_prefix, MAX_FIELD_NAME_SIZE);
    }

    /// NextLogMessage scans the next log message into fields.
    ///
    /// true is returned on success, false is returned on error or on the end of logs messages.
    /// Call `error()` after `next_log_message()` returns false in order to verify the last error.
    pub fn next_log_message(&mut self) -> bool {
        self.common.reset_keep_settings();

        if !self.s.next() {
            self.err = self.s.error().map(|e| e.to_string());
            return false;
        }
        let v = self.s.value();
        let o = match self.s.doc.object(v) {
            Ok(o) => o,
            Err(err) => {
                self.err = Some(err);
                return false;
            }
        };
        self.common.append_log_fields(&mut self.s.doc, o);
        true
    }

    /// Error returns the last error from a `next_log_message()` call.
    ///
    /// PORT NOTE: Go returns `error` (nil on success / end of input); the
    /// port returns `Option<&str>`.
    pub fn error(&self) -> Option<&str> {
        self.err.as_deref()
    }
}

/// GetJSONScanner returns a JSONScanner ready to parse JSON lines.
///
/// Return the scanner to the pool when it is no longer needed by calling `put_json_scanner()`.
pub fn get_json_scanner() -> JSONScanner {
    SCANNER_POOL.lock().unwrap().pop().unwrap_or_default()
}

/// PutJSONScanner returns the scanner to the pool.
///
/// The scanner cannot be used after returning to the pool.
pub fn put_json_scanner(mut s: JSONScanner) {
    s.reset();
    SCANNER_POOL.lock().unwrap().push(s);
}

/// PORT NOTE: Go uses `sync.Pool`; the port uses a `Mutex<Vec<..>>` pool
/// handing scanners out by value (the established esl-common pattern).
static SCANNER_POOL: Mutex<Vec<JSONScanner>> = Mutex::new(Vec::new());

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::marshal_fields_to_json;

    #[test]
    fn test_json_scanner_failure() {
        fn f(data: &str) {
            let mut s = get_json_scanner();
            s.init(data.as_bytes(), &[], "");
            while s.next_log_message() {}
            assert!(s.error().is_some(), "expecting non-nil error for {data:?}");
            put_json_scanner(s);
        }
        f("{foo");
        f("[1,2,3]");
        f("{\"foo\",}");
    }

    #[test]
    fn test_json_scanner_success() {
        fn f(data: &str, field_prefix: &str, preserve_keys: &[&str], output_expected: &str) {
            let mut s = get_json_scanner();
            let preserve_keys: Vec<&[u8]> = preserve_keys.iter().map(|s| s.as_bytes()).collect();
            s.init(data.as_bytes(), &preserve_keys, field_prefix);
            let mut output = Vec::new();
            while s.next_log_message() {
                marshal_fields_to_json(&mut output, s.fields());
            }
            assert!(s.error().is_none(), "unexpected error: {:?}", s.error());

            let output = String::from_utf8(output).unwrap();
            assert_eq!(
                output, output_expected,
                "unexpected fields;\ngot\n{output}\nwant\n{output_expected}"
            );
            put_json_scanner(s);
        }

        f("{}", "", &[], "{}");
        f(
            "{\"foo\":{\"bar\":\"baz\"}}{\"bar\":{\"baz\":\"bar\"}}",
            "",
            &[],
            "{\"foo.bar\":\"baz\"}{\"bar.baz\":\"bar\"}",
        );
        f(
            "{\"foo\":{\"bar\":{\"x\":\"y\",\"z\":[\"foo\"]}},\"a\":1,\"b\":true,\"c\":[1,2],\"d\":false,\"e\":null}",
            "",
            &[],
            "{\"foo.bar.x\":\"y\",\"foo.bar.z\":\"[\\\"foo\\\"]\",\"a\":\"1\",\"b\":\"true\",\"c\":\"[1,2]\",\"d\":\"false\"}",
        );

        // preserve foo
        f(
            "{\"foo\":{\"bar\":{\"x\":\"y\",\"z\":[\"foo\"]}},\"a\":1,\"b\":true,\"c\":[1,2],\"d\":false,\"e\":null}",
            "",
            &["foo"],
            "{\"foo\":\"{\\\"bar\\\":{\\\"x\\\":\\\"y\\\",\\\"z\\\":[\\\"foo\\\"]}}\",\"a\":\"1\",\"b\":\"true\",\"c\":\"[1,2]\",\"d\":\"false\"}",
        );

        // preserve foo.bar
        f(
            "{\"foo\":{\"bar\":{\"x\":\"y\",\"z\":[\"foo\"]}},\"a\":1,\"b\":true,\"c\":[1,2],\"d\":false,\"e\":null}",
            "",
            &["foo.bar"],
            "{\"foo.bar\":\"{\\\"x\\\":\\\"y\\\",\\\"z\\\":[\\\"foo\\\"]}\",\"a\":\"1\",\"b\":\"true\",\"c\":\"[1,2]\",\"d\":\"false\"}",
        );
    }
}

//! Port of EsLogs `app/eslinsert/insertutil/line_reader.go`.
//!
//! Reads newline-delimited lines from an underlying reader, skipping lines that
//! exceed `MaxLineSizeBytes` (replacing them with an empty line so protocols
//! like Elasticsearch bulk stay line-aligned).

use std::io::Read;

use esl_common::flagutil::{Bytes, Flag};
use esl_common::warnf;

/// MaxLineSizeBytes is the maximum length of a single line for `/insert/*`
/// handlers (Go `-insert.maxLineSizeBytes` from `insertutil/flags.go`).
pub static MAX_LINE_SIZE_BYTES: Flag<Bytes> = Flag::new(
    "insert.maxLineSizeBytes",
    "The maximum size of a single line that can be read by /insert/* handlers. \
     Regardless of this flag, entries above the 2 MB limit are ignored, \
     see https://docs.victoriametrics.com/victorialogs/faq/#what-length-a-log-record-is-expected-to-have",
    || Bytes::with_default(256 * 1024),
);

/// The resolved `-insert.maxLineSizeBytes` value (Go `MaxLineSizeBytes.IntN()`).
fn max_line_size_bytes() -> usize {
    MAX_LINE_SIZE_BYTES.get().int_n().max(0) as usize
}

/// LineReader reads newline-delimited lines from the underlying reader.
///
/// PORT NOTE: Go's `Line []byte` points into `buf`; the Rust reader exposes the
/// current line as a `(start, end)` slice of `buf` via [`LineReader::line`] to
/// keep the same zero-copy behavior.
pub struct LineReader<'a> {
    name: String,
    r: &'a mut dyn Read,
    buf: Vec<u8>,
    buf_offset: usize,
    line_start: usize,
    line_end: usize,
    err: Option<String>,
    eof_reached: bool,
}

impl<'a> LineReader<'a> {
    /// Returns a LineReader over r.
    pub fn new(name: &str, r: &'a mut dyn Read) -> Self {
        LineReader {
            name: name.to_string(),
            r,
            buf: Vec::new(),
            buf_offset: 0,
            line_start: 0,
            line_end: 0,
            err: None,
            eof_reached: false,
        }
    }

    /// Returns the line read by the last [`LineReader::next_line`] call.
    pub fn line(&self) -> &[u8] {
        &self.buf[self.line_start..self.line_end]
    }

    /// Returns the last error after a `next_line` call (Go `Err()`), prefixed
    /// with the reader name.
    pub fn err_string(&self) -> Option<String> {
        self.err.as_ref().map(|e| format!("{}: {e}", self.name))
    }

    /// Reads the next line from the underlying reader.
    ///
    /// Returns true if the next line was read into [`LineReader::line`]. If the
    /// line length exceeds `MAX_LINE_SIZE_BYTES` it is skipped and an empty line
    /// is returned instead. When false is returned, no more lines are left; call
    /// [`LineReader::err_string`] to check for errors.
    pub fn next_line(&mut self) -> bool {
        loop {
            self.line_start = 0;
            self.line_end = 0;
            if self.buf_offset >= self.buf.len() {
                if self.err.is_some() || self.eof_reached {
                    return false;
                }
                if !self.read_more_data() {
                    return false;
                }
                if self.buf_offset >= self.buf.len() && self.eof_reached {
                    return false;
                }
            }

            let start = self.buf_offset;
            if let Some(n) = self.buf[start..].iter().position(|&b| b == b'\n') {
                self.line_start = start;
                self.line_end = start + n;
                self.buf_offset += n + 1;
                return true;
            }
            if self.eof_reached {
                self.line_start = start;
                self.line_end = self.buf.len();
                self.buf_offset = self.buf.len();
                return true;
            }
            if !self.read_more_data() {
                return false;
            }
        }
    }

    fn read_more_data(&mut self) -> bool {
        if self.buf_offset > 0 {
            self.buf.drain(..self.buf_offset);
            self.buf_offset = 0;
        }

        let max_line_size = max_line_size_bytes();
        let buf_len = self.buf.len();
        if buf_len >= max_line_size {
            let snippet = limit_string_len(&String::from_utf8_lossy(&self.buf), 1024);
            let (ok, skipped_bytes) = self.skip_until_next_line();
            warnf!(
                "{}: the line length exceeds -insert.maxLineSizeBytes={}; skipping it; total skipped bytes={}; the line snippet={:?}",
                self.name,
                max_line_size,
                skipped_bytes,
                snippet
            );
            return ok;
        }

        self.buf.resize(max_line_size, 0);
        match self.r.read(&mut self.buf[buf_len..]) {
            Ok(0) => {
                self.buf.truncate(buf_len);
                self.eof_reached = true;
                true
            }
            Ok(n) => {
                self.buf.truncate(buf_len + n);
                n > 0
            }
            Err(e) => {
                self.buf.truncate(buf_len);
                self.err = Some(format!("cannot read the next line: {e}"));
                false
            }
        }
    }

    fn skip_until_next_line(&mut self) -> (bool, usize) {
        let max_line_size = max_line_size_bytes();

        // We've already read MaxLineSizeBytes without a newline.
        let mut skip_size_bytes: usize = max_line_size;

        loop {
            self.buf.resize(max_line_size, 0);
            let n = match self.r.read(&mut self.buf[..]) {
                Ok(n) => n,
                Err(e) => {
                    self.err = Some(format!("cannot skip the current line: {e}"));
                    self.buf.clear();
                    return (false, skip_size_bytes);
                }
            };
            skip_size_bytes += n;
            self.buf.truncate(n);
            if n == 0 {
                self.eof_reached = true;
                self.buf.clear();
                return (true, skip_size_bytes);
            }
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                // Count only bytes up to and including the newline.
                skip_size_bytes = (skip_size_bytes as isize + pos as isize + 1
                    - self.buf.len() as isize) as usize;
                // Keep the buffer starting at '\n' so the too-long line is
                // replaced by an empty line on the next next_line() call.
                self.buf.drain(..pos);
                return (true, skip_size_bytes);
            }
        }
    }
}

/// Mirrors Go `stringsutil.LimitStringLen`.
fn limit_string_len(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        return s.to_string();
    }
    if max_len <= 3 {
        return s.chars().take(max_len).collect();
    }
    let n = (max_len - 3) / 2;
    let head: String = s.chars().take(n).collect();
    let tail_start = s.chars().count().saturating_sub(n);
    let tail: String = s.chars().skip(tail_start).collect();
    format!("{head}...{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn collect_lines(lr: &mut LineReader) -> Vec<String> {
        let mut lines = Vec::new();
        while lr.next_line() {
            lines.push(String::from_utf8_lossy(lr.line()).into_owned());
        }
        lines
    }

    #[test]
    fn test_line_reader_success() {
        fn f(data: &str, lines_expected: &[&str]) {
            let mut r = Cursor::new(data.as_bytes().to_vec());
            let mut lr = LineReader::new("foo", &mut r);
            let lines = collect_lines(&mut lr);
            if let Some(err) = lr.err_string() {
                panic!("unexpected error: {err}");
            }
            assert!(
                !lr.next_line(),
                "expecting no more lines on the second call to next_line()"
            );
            assert!(
                lr.line().is_empty(),
                "unexpected non-empty line after failed next_line(): {:?}",
                String::from_utf8_lossy(lr.line())
            );
            assert_eq!(
                lines, lines_expected,
                "unexpected lines\ngot\n{lines:?}\nwant\n{lines_expected:?}"
            );
        }

        f("", &[]);
        f("\n", &[""]);
        f("\n\n", &["", ""]);
        f("foo", &["foo"]);
        f("foo\n", &["foo"]);
        f("\nfoo", &["", "foo"]);
        f("foo\n\n", &["foo", ""]);
        f("foo\nbar", &["foo", "bar"]);
        f("foo\nbar\n", &["foo", "bar"]);
        f("\nfoo\n\nbar\n\n", &["", "foo", "", "bar", ""]);
    }

    #[test]
    fn test_line_reader_skip_until_next_line() {
        fn f(data: &[u8], lines_expected: &[&str]) {
            let mut r = Cursor::new(data.to_vec());
            let mut lr = LineReader::new("foo", &mut r);
            let lines = collect_lines(&mut lr);
            if let Some(err) = lr.err_string() {
                panic!("unexpected error: {err}");
            }
            assert!(
                !lr.next_line(),
                "expecting no more lines on the second call to next_line()"
            );
            assert_eq!(
                lines, lines_expected,
                "unexpected lines\ngot\n{lines:?}\nwant\n{lines_expected:?}"
            );
        }

        let max = max_line_size_bytes();
        for overflow in [0usize, 100, max, max + 1, 2 * max] {
            let long_line = vec![b'a'; max + overflow];

            // Single long line
            let data = long_line.clone();
            f(&data, &[]);

            // Multiple long lines
            let mut data = long_line.clone();
            data.push(b'\n');
            data.extend_from_slice(&long_line);
            f(&data, &[""]);

            let mut data = long_line.clone();
            data.push(b'\n');
            data.extend_from_slice(&long_line);
            data.push(b'\n');
            f(&data, &["", ""]);

            // Long line in the middle
            let mut data = b"foo\n".to_vec();
            data.extend_from_slice(&long_line);
            data.extend_from_slice(b"\nbar");
            f(&data, &["foo", "", "bar"]);

            // Multiple long lines in the middle
            let mut data = b"foo\n".to_vec();
            data.extend_from_slice(&long_line);
            data.push(b'\n');
            data.extend_from_slice(&long_line);
            data.extend_from_slice(b"\nbar");
            f(&data, &["foo", "", "", "bar"]);

            // Long line in the end
            let mut data = b"foo\n".to_vec();
            data.extend_from_slice(&long_line);
            f(&data, &["foo"]);

            // Long line in the end
            let mut data = b"foo\n".to_vec();
            data.extend_from_slice(&long_line);
            data.push(b'\n');
            f(&data, &["foo", ""]);
        }
    }

    #[test]
    fn test_line_reader_failure() {
        fn f(data: &[u8], lines_expected: &[&str]) {
            let mut fr = FailureReader {
                r: Cursor::new(data.to_vec()),
            };
            let mut lr = LineReader::new("foo", &mut fr);
            let lines = collect_lines(&mut lr);
            assert!(lr.err_string().is_some(), "expecting non-nil error");
            assert!(
                !lr.next_line(),
                "expecting no more lines on the second call to next_line()"
            );
            assert!(
                lr.err_string().is_some(),
                "expecting non-nil error on the second call"
            );
            assert_eq!(
                lines, lines_expected,
                "unexpected lines\ngot\n{lines:?}\nwant\n{lines_expected:?}"
            );
        }

        f(b"", &[]);
        f(b"foo", &[]);
        f(b"foo\n", &["foo"]);
        f(b"\n", &[""]);
        f(b"foo\nbar", &["foo"]);
        f(b"foo\nbar\n", &["foo", "bar"]);
        f(b"\nfoo\nbar\n\n", &["", "foo", "bar", ""]);

        // long line
        let max = max_line_size_bytes();
        for overflow in [0usize, 100, max, max + 1, 2 * max] {
            let long_line = vec![b'a'; max + overflow];

            let data = long_line.clone();
            f(&data, &[]);

            let mut data = b"foo\n".to_vec();
            data.extend_from_slice(&long_line);
            f(&data, &["foo"]);

            let mut data = long_line.clone();
            data.extend_from_slice(b"\nfoo");
            f(&data, &[""]);

            let mut data = long_line.clone();
            data.extend_from_slice(b"\nfoo\n");
            f(&data, &["", "foo"]);
        }
    }

    /// Mirrors the Go test's `failureReader`: returns an error instead of EOF.
    struct FailureReader {
        r: Cursor<Vec<u8>>,
    }

    impl Read for FailureReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.r.read(buf).unwrap_or(0);
            if n > 0 {
                return Ok(n);
            }
            Err(std::io::Error::other("some error"))
        }
    }
}

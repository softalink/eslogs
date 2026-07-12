//! Port of `lib/logstorage/pipe_replace_regexp.go` — the
//! `| replace_regexp (re, replacement)` pipe, which rewrites a field by
//! replacing regexp matches with an expansion template.
//!
//! See <https://docs.victoriametrics.com/victorialogs/logsql/#replace_regexp-pipe>.
//!
//! PORT NOTE — `iff` (`ifFilter`): as in [`crate::pipe_replace`], the optional
//! `if (...)` clause is dropped because `ifFilter` is not ported; the processor
//! rewrites every row (matches Go when `iff == nil`).
//!
//! PORT NOTE — parser: `parsePipeReplaceRegexp` needs the query lexer; deferred.
//! The `pub(crate)` [`PipeReplaceRegexp::new`] constructor takes an already
//! compiled [`regex::Regex`] (build it with [`regexp_compile`]).
//!
//! PORT NOTE — regexp expansion: Go uses `regexp.Regexp.ExpandString` with
//! `$1` / `${1}` / `$0` templates. Rust's `regex` crate uses the identical
//! `$name` / `${name}` syntax (both stop a bare `$1` at the first non-word
//! char), so replacement templates carry over unchanged. Go's `regexpCompile`
//! wraps the pattern in `(?s)(?:...)` so `.` matches newlines; [`regexp_compile`]
//! reproduces that.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use regex::Regex;

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;

/// `| replace_regexp (re, replacement) [at field] [limit N]` pipe.
///
/// Port of Go's `pipeReplaceRegexp`.
#[derive(Clone, Debug)]
pub(crate) struct PipeReplaceRegexp {
    /// The field whose value is rewritten (defaults to `_msg`).
    pub(crate) field: String,
    /// The compiled regular expression.
    pub(crate) re: Regex,
    /// String representation of `re` (as written by the user, before wrapping).
    pub(crate) re_str: String,
    /// The replacement/expansion template.
    pub(crate) replacement: String,
    /// Maximum number of replacements per value (0 = unlimited).
    pub(crate) limit: u64,
}

impl PipeReplaceRegexp {
    /// Builds a `replace_regexp` pipe from parsed arguments.
    pub(crate) fn new(
        field: impl Into<String>,
        re: Regex,
        re_str: impl Into<String>,
        replacement: impl Into<String>,
        limit: u64,
    ) -> Self {
        Self {
            field: field.into(),
            re,
            re_str: re_str.into(),
            replacement: replacement.into(),
            limit,
        }
    }
}

impl Pipe for PipeReplaceRegexp {
    fn to_string(&self) -> String {
        let mut s = String::from("replace_regexp");
        s.push_str(&format!(
            " ({}, {})",
            quote_token_if_needed(&self.re_str),
            quote_token_if_needed(&self.replacement)
        ));
        if self.field != "_msg" {
            s.push_str(&format!(" at {}", quote_token_if_needed(&self.field)));
        }
        if self.limit > 0 {
            s.push_str(&format!(" limit {}", self.limit));
        }
        s
    }

    fn update_needed_fields(&self, _pf: &mut prefix_filter::Filter) {
        // PORT NOTE: no-op — iff deferred (see module docs).
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeReplaceRegexpProcessor {
            field: self.field.clone(),
            re: self.re.clone(),
            replacement: self.replacement.clone(),
            limit: self.limit,
            pp_next,
        })
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        true
    }
}

/// Execution half of [`PipeReplaceRegexp`]. Stateless across blocks.
struct PipeReplaceRegexpProcessor {
    field: String,
    re: Regex,
    replacement: String,
    limit: u64,
    pp_next: Arc<dyn PipeProcessor>,
}

impl PipeProcessor for PipeReplaceRegexpProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let c = br.get_column_by_name(&self.field);
        let values: Vec<Vec<u8>> = br.column_get_values(c).to_vec();

        let mut rc = ResultColumn {
            name: self.field.clone(),
            values: Vec::with_capacity(values.len()),
        };
        let mut v_new: Vec<u8> = Vec::new();
        for (i, v) in values.iter().enumerate() {
            if i == 0 || values[i - 1] != *v {
                v_new = Vec::new();
                let s = String::from_utf8_lossy(v);
                append_replace_regexp(&mut v_new, &s, &self.re, &self.replacement, self.limit);
            }
            rc.values.push(v_new.clone());
        }

        br.add_result_column(rc);
        self.pp_next.write_block(worker_id, br);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Compiles `s` the way Go's `regexpCompile` does: wraps it in `(?s)(?:...)` so
/// `.` matches newlines.
///
/// PORT NOTE: Go homes `regexpCompile` in `pipe_extract_regexp.go` (not ported);
/// duplicated here until that module lands.
pub(crate) fn regexp_compile(s: &str) -> Result<Regex, String> {
    let pattern = format!("(?s)(?:{s})");
    Regex::new(&pattern).map_err(|e| e.to_string())
}

/// Appends `s` to `dst` with up to `limit` regexp matches replaced by the
/// expanded `replacement` template (all matches when `limit == 0`).
///
/// Port of Go's `appendReplaceRegexp`.
pub(crate) fn append_replace_regexp(
    dst: &mut Vec<u8>,
    s: &str,
    re: &Regex,
    replacement: &str,
    limit: u64,
) {
    if s.is_empty() {
        return;
    }

    let bytes = s.as_bytes();
    let mut prev_end = 0usize;
    let mut count: u64 = 0;
    let mut tmp = String::new();
    // Loop shape mirrors the Go source.
    #[allow(clippy::explicit_counter_loop)]
    for caps in re.captures_iter(s) {
        if limit > 0 && count >= limit {
            break;
        }
        let m = caps.get(0).unwrap();
        dst.extend_from_slice(&bytes[prev_end..m.start()]);
        tmp.clear();
        caps.expand(replacement, &mut tmp);
        dst.extend_from_slice(tmp.as_bytes());
        prev_end = m.end();
        count += 1;
    }
    dst.extend_from_slice(&bytes[prev_end..]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;
    use std::sync::Mutex;

    // PORT NOTE: TestParsePipeReplaceRegexpSuccess/Failure and
    // TestPipeReplaceRegexpUpdateNeededFields are skipped — lexer / needed-fields
    // planner (parser + ifFilter), both deferred.

    fn arr(s: &str, re: &str, replacement: &str, limit: u64) -> String {
        let re = regexp_compile(re).unwrap();
        let mut dst = Vec::new();
        append_replace_regexp(&mut dst, s, &re, replacement, limit);
        String::from_utf8(dst).unwrap()
    }

    #[test]
    fn test_append_replace_regexp() {
        // capture-group expansion
        assert_eq!(
            arr("abc foo a bar foobar foo b bar", "foo(.+?)bar", "q-$1-x", 0),
            "abc q- a -x q-bar foo b -x"
        );
        // character class, no limit
        assert_eq!(arr("a_bc_d/ef", "[_/]", "-", 0), "a-bc-d-ef");
        // limit 1
        assert_eq!(arr("a_bc_d/ef", "[_/]", "-", 1), "a-bc_d/ef");
        // limit 100
        assert_eq!(arr("a_bc_d/ef", "[_/]", "-", 100), "a-bc-d-ef");
        // '.' matches newline via (?s) wrapper
        assert_eq!(
            arr("foo a\n aaa barabc", "foo(.+?)bar", "q-$1-x", 0),
            "q- a\n aaa -xabc"
        );
        // explicit (?-s) disables newline matching -> no match
        assert_eq!(
            arr("foo a\n aaa barabc", "(?-s)foo(.+?)bar", "q-$1-x", 0),
            "foo a\n aaa barabc"
        );
        // no match returns input unchanged
        assert_eq!(arr("1234", "[_/]", "-", 0), "1234");
    }

    struct Capture(Mutex<Vec<Vec<Field>>>);

    impl PipeProcessor for Capture {
        fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
            let cols = br.get_columns();
            let n = br.rows_len();
            let mut out = self.0.lock().unwrap();
            for r in 0..n {
                let mut row = Vec::new();
                for &c in &cols {
                    let name = br.column_name(c).to_string();
                    let value = br.column_get_value_at_row(c, r).to_string();
                    if !value.is_empty() {
                        row.push(Field { name, value });
                    }
                }
                out.push(row);
            }
        }
        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    fn canon(mut row: Vec<Field>) -> Vec<(String, String)> {
        row.sort_by(|a, b| a.name.cmp(&b.name));
        row.into_iter().map(|f| (f.name, f.value)).collect()
    }

    fn f(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn run(pipe: PipeReplaceRegexp, rows: Vec<Vec<Field>>) -> Vec<Vec<(String, String)>> {
        let cap = Arc::new(Capture(Mutex::new(Vec::new())));
        let stop = Arc::new(AtomicBool::new(false));
        let pp = pipe.new_pipe_processor(1, stop, cap.clone() as Arc<dyn PipeProcessor>);
        let mut br = BlockResult::default();
        br.must_init_from_rows(&rows);
        pp.write_block(0, &mut br);
        pp.flush().unwrap();
        let out = cap.0.lock().unwrap().clone();
        out.into_iter().map(canon).collect()
    }

    fn expected(rows: Vec<Vec<Field>>) -> Vec<Vec<(String, String)>> {
        rows.into_iter()
            .map(|r| canon(r.into_iter().filter(|f| !f.value.is_empty()).collect()))
            .collect()
    }

    #[test]
    fn test_pipe_replace_regexp_placeholders() {
        let re = regexp_compile("foo(.+?)bar").unwrap();
        let out = run(
            PipeReplaceRegexp::new("_msg", re, "foo(.+?)bar", "q-$1-x", 0),
            vec![
                vec![f("_msg", "abc foo a bar foobar foo b bar"), f("bar", "cde")],
                vec![f("_msg", "1234")],
            ],
        );
        assert_eq!(
            out,
            expected(vec![
                vec![f("_msg", "abc q- a -x q-bar foo b -x"), f("bar", "cde")],
                vec![f("_msg", "1234")],
            ])
        );
    }

    #[test]
    fn test_pipe_replace_regexp_limit() {
        let re = regexp_compile("[_/]").unwrap();
        let out = run(
            PipeReplaceRegexp::new("foo", re, "[_/]", "-", 1),
            vec![
                vec![f("foo", "a_bc_d/ef"), f("bar", "cde")],
                vec![f("foo", "1234")],
            ],
        );
        assert_eq!(
            out,
            expected(vec![
                vec![f("foo", "a-bc_d/ef"), f("bar", "cde")],
                vec![f("foo", "1234")],
            ])
        );
    }

    #[test]
    fn test_string() {
        let re = regexp_compile(" ").unwrap();
        assert_eq!(
            PipeReplaceRegexp::new("x", re, " ", "", 10).to_string(),
            r#"replace_regexp (" ", "") at x limit 10"#
        );
    }
}

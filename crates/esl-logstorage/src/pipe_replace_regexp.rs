//! Port of `lib/logstorage/pipe_replace_regexp.go` — the
//! `| replace_regexp (re, replacement)` pipe, which rewrites a field by
//! replacing regexp matches with an expansion template.
//!
//! See <https://docs.victoriametrics.com/victorialogs/logsql/#replace_regexp-pipe>.
//!
//! Like Go, row processing goes through the shared
//! [`crate::pipe_update::new_pipe_update_processor`] machinery, which honors
//! the optional `if (...)` clause (rows not matching the filter keep their
//! original value). The lexer-driven parse lives in
//! `parser::parse_pipe::parse_pipe_replace_regexp`.
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

use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_update::{
    IfFilter, UpdateFunc, new_pipe_update_processor, update_needed_fields_for_update_pipe,
};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;

/// `| replace_regexp [if (...)] (re, replacement) [at field] [limit N]` pipe.
///
/// Port of Go's `pipeReplaceRegexp`.
#[derive(Clone)]
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
    /// Optional `if (...)` filter for skipping the replace_regexp operation
    /// (Go `iff`).
    pub(crate) iff: Option<Arc<IfFilter>>,
}

impl PipeReplaceRegexp {
    /// Builds a `replace_regexp` pipe from parsed arguments.
    pub(crate) fn new(
        field: impl Into<String>,
        re: Regex,
        re_str: impl Into<String>,
        replacement: impl Into<String>,
        limit: u64,
        iff: Option<Arc<IfFilter>>,
    ) -> Self {
        Self {
            field: field.into(),
            re,
            re_str: re_str.into(),
            replacement: replacement.into(),
            limit,
            iff,
        }
    }
}

impl Pipe for PipeReplaceRegexp {
    /// Port of Go `pipeReplaceRegexp.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        let mut s = String::from("replace_regexp");
        if let Some(iff) = &self.iff {
            s.push(' ');
            s.push_str(&iff.to_string());
        }
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

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        update_needed_fields_for_update_pipe(pf, &self.field, self.iff.as_deref());
    }

    /// Go `hasFilterInWithQuery` for this pipe: checks the `if (...)` filter.
    fn has_filter_in_with_query(&self) -> bool {
        self.iff
            .as_ref()
            .is_some_and(|iff| iff.has_filter_in_with_query())
    }

    /// Go `initFilterInValues` for this pipe: rewrites the `if (...)` filter.
    fn init_filter_in_values(
        &mut self,
        get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
        timestamp: i64,
    ) -> Result<(), String> {
        if let Some(iff) = &self.iff
            && let Some(iff_new) = iff.init_filter_in_values(get_values, timestamp)?
        {
            self.iff = Some(Arc::new(iff_new));
        }
        Ok(())
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        // Port of Go's `updateFunc(a *arena, v string) string` which appends
        // `appendReplaceRegexp(a.b, v, ...)` to a pooled arena and returns the
        // new suffix. The Rust port drops the arena and returns an owned String
        // (see pipe_update.rs module docs).
        let re = self.re.clone();
        let replacement = self.replacement.clone();
        let limit = self.limit;
        let update_func: UpdateFunc = Arc::new(move |v: &str| {
            let mut buf: Vec<u8> = Vec::new();
            append_replace_regexp(&mut buf, v, &re, &replacement, limit);
            String::from_utf8_lossy(&buf).into_owned()
        });

        new_pipe_update_processor(
            update_func,
            pp_next,
            self.field.clone(),
            self.iff.clone(),
            concurrency,
        )
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        true
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
    use crate::block_result::BlockResult;
    use crate::filter::Filter;
    use crate::filter_phrase::new_filter_phrase;
    use crate::pipe_update::test_utils::assert_needed_fields;
    use crate::rows::Field;
    use std::sync::Mutex;

    fn arr(s: &str, re: &str, replacement: &str, limit: u64) -> String {
        let re = regexp_compile(re).unwrap();
        let mut dst = Vec::new();
        append_replace_regexp(&mut dst, s, &re, replacement, limit);
        String::from_utf8(dst).unwrap()
    }

    // Go TestAppendReplaceRegexp (pipe_replace_regexp_test.go), plus the '.'
    // vs newline cases from Go TestPipeReplaceRegexp.
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

    /// Builds the `if (field:phrase)` filter used by the conditional Go tests.
    fn phrase_iff(field: &str, phrase: &str) -> Arc<IfFilter> {
        let f: Arc<dyn Filter> = Arc::new(new_filter_phrase(field, phrase));
        Arc::new(IfFilter::new(f))
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

    // Go TestPipeReplaceRegexp "replace_regexp with placeholders".
    #[test]
    fn test_pipe_replace_regexp_placeholders() {
        let re = regexp_compile("foo(.+?)bar").unwrap();
        let out = run(
            PipeReplaceRegexp::new("_msg", re, "foo(.+?)bar", "q-$1-x", 0, None),
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

    // Go TestPipeReplaceRegexp "replace_regexp with limit 1 at foo".
    #[test]
    fn test_pipe_replace_regexp_limit() {
        let re = regexp_compile("[_/]").unwrap();
        let out = run(
            PipeReplaceRegexp::new("foo", re, "[_/]", "-", 1, None),
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

    // Go TestPipeReplaceRegexp "conditional replace_regexp at foo":
    // `replace_regexp if (bar:abc) ("[_/]", "") at foo`.
    #[test]
    fn test_pipe_replace_regexp_conditional() {
        let re = regexp_compile("[_/]").unwrap();
        let out = run(
            PipeReplaceRegexp::new("foo", re, "[_/]", "", 0, Some(phrase_iff("bar", "abc"))),
            vec![
                vec![f("foo", "a_bc_d/ef"), f("bar", "cde")],
                vec![f("foo", "123_45/6"), f("bar", "abc")],
            ],
        );
        assert_eq!(
            out,
            expected(vec![
                vec![f("foo", "a_bc_d/ef"), f("bar", "cde")],
                vec![f("foo", "123456"), f("bar", "abc")],
            ])
        );
    }

    // Go TestPipeReplaceRegexpUpdateNeededFields (pipe_replace_regexp_test.go).
    #[test]
    fn test_pipe_replace_regexp_update_needed_fields() {
        let p = |iff: Option<(&str, &str)>, at: &str| {
            let re = regexp_compile("a").unwrap();
            PipeReplaceRegexp::new(at, re, "a", "b", 0, iff.map(|(f, ph)| phrase_iff(f, ph)))
        };

        // all the needed fields
        assert_needed_fields(&p(None, "x"), "*", "", "*", "");
        assert_needed_fields(&p(Some(("f1", "q")), "x"), "*", "", "*", "");

        // unneeded fields do not intersect with at field
        assert_needed_fields(&p(None, "x"), "*", "f1,f2", "*", "f1,f2");
        assert_needed_fields(&p(Some(("f3", "q")), "x"), "*", "f1,f2", "*", "f1,f2");
        assert_needed_fields(&p(Some(("f2", "q")), "x"), "*", "f1,f2", "*", "f1");

        // unneeded fields intersect with at field
        assert_needed_fields(&p(None, "x"), "*", "x,y", "*", "x,y");
        assert_needed_fields(&p(Some(("f1", "q")), "x"), "*", "x,y", "*", "x,y");
        assert_needed_fields(&p(Some(("x", "q")), "x"), "*", "x,y", "*", "x,y");
        assert_needed_fields(&p(Some(("y", "q")), "x"), "*", "x,y", "*", "x,y");

        // needed fields do not intersect with at field
        assert_needed_fields(&p(None, "x"), "f2,y", "", "f2,y", "");
        assert_needed_fields(&p(Some(("f1", "q")), "x"), "f2,y", "", "f2,y", "");

        // needed fields intersect with at field
        assert_needed_fields(&p(None, "y"), "f2,y", "", "f2,y", "");
        assert_needed_fields(&p(Some(("f1", "q")), "y"), "f2,y", "", "f1,f2,y", "");
    }

    // Go TestParsePipeReplaceRegexpSuccess (pipe_replace_regexp_test.go):
    // parse + String() round-trip, matching Go's `expectParsePipeSuccess`.
    #[test]
    fn test_parse_pipe_replace_regexp_success() {
        fn p(pipe_str: &str) {
            let pipe = crate::pipe::must_parse_pipe(pipe_str, 0);
            assert_eq!(pipe.to_string(), pipe_str, "round-trip mismatch");
        }

        p(r#"replace_regexp (foo, bar)"#);
        p(r#"replace_regexp ("foo[^ ]+bar|baz", "bar${1}x$0")"#);
        p(r#"replace_regexp (" ", "") at x"#);
        p(r#"replace_regexp if (x:y) ("-", ":") at a"#);
        p(r#"replace_regexp (" ", "") at x limit 10"#);
        p(r#"replace_regexp if (x:y) (" ", "") at foo limit 10"#);
    }

    #[test]
    fn test_string() {
        let re = regexp_compile(" ").unwrap();
        assert_eq!(
            PipeReplaceRegexp::new("x", re, " ", "", 10, None).to_string(),
            r#"replace_regexp (" ", "") at x limit 10"#
        );
        let re = regexp_compile("-").unwrap();
        assert_eq!(
            PipeReplaceRegexp::new("a", re, "-", ":", 0, Some(phrase_iff("x", "y"))).to_string(),
            r#"replace_regexp if (x:y) ("-", ":") at a"#
        );
    }
}

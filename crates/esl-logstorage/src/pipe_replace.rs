//! Port of `lib/logstorage/pipe_replace.go` — the `| replace (old, new)` pipe,
//! which replaces occurrences of a literal substring inside a field.
//!
//! See <https://docs.victoriametrics.com/victorialogs/logsql/#replace-pipe>.
//!
//! Like Go, row processing goes through the shared
//! [`crate::pipe_update::new_pipe_update_processor`] machinery, which honors
//! the optional `if (...)` clause (rows not matching the filter keep their
//! original value). The lexer-driven parse lives in
//! `parser::parse_pipe::parse_pipe_replace`.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_update::{
    IfFilter, UpdateFunc, new_pipe_update_processor, update_needed_fields_for_update_pipe,
};
use crate::prefix_filter;

/// `| replace [if (...)] (old, new) [at field] [limit N]` pipe.
///
/// Port of Go's `pipeReplace`.
#[derive(Clone)]
pub(crate) struct PipeReplace {
    /// The field whose value is rewritten (defaults to `_msg`).
    pub(crate) field: Vec<u8>,
    /// The literal substring to replace. Raw bytes (Go strings are arbitrary
    /// bytes; raw `\xNN` escapes in the query text carry through byte-exact).
    pub(crate) old_substr: Vec<u8>,
    /// The replacement bytes.
    pub(crate) new_substr: Vec<u8>,
    /// Maximum number of replacements per value (0 = unlimited).
    pub(crate) limit: u64,
    /// Optional `if (...)` filter for skipping the replace operation (Go `iff`).
    pub(crate) iff: Option<Arc<IfFilter>>,
}

impl PipeReplace {
    /// Builds a `replace` pipe from parsed arguments.
    pub(crate) fn new(
        field: impl Into<Vec<u8>>,
        old_substr: impl Into<Vec<u8>>,
        new_substr: impl Into<Vec<u8>>,
        limit: u64,
        iff: Option<Arc<IfFilter>>,
    ) -> Self {
        Self {
            field: field.into(),
            old_substr: old_substr.into(),
            new_substr: new_substr.into(),
            limit,
            iff,
        }
    }
}

impl Pipe for PipeReplace {
    /// Port of Go `pipeReplace.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        let mut s = String::from("replace");
        if let Some(iff) = &self.iff {
            s.push(' ');
            s.push_str(&iff.to_string());
        }
        // Lossless render (Go quoteTokenIfNeeded, byte form): invalid UTF-8
        // re-quotes via Go strconv.Quote byte semantics (`\xNN`).
        s.push_str(&format!(
            " ({}, {})",
            crate::stream_filter::quote_value_bytes_if_needed(&self.old_substr),
            crate::stream_filter::quote_value_bytes_if_needed(&self.new_substr)
        ));
        if self.field != b"_msg" {
            s.push_str(&format!(
                " at {}",
                crate::parser::quote_token_bytes_if_needed(&self.field)
            ));
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

    /// Go `visitSubqueries` for this pipe: propagates into the `if (...)` filter.
    fn visit_subqueries_mut(
        &mut self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
        if let Some(iff) = &self.iff
            && let Some(iff_new) = iff.visit_subqueries_mut(timestamp, visit)
        {
            self.iff = Some(Arc::new(iff_new));
        }
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        // Port of Go's `updateFunc(a *arena, v string) string` which appends
        // `appendReplace(a.b, v, ...)` to a pooled arena and returns the new
        // suffix. The Rust port drops the arena and returns an owned String
        // (see pipe_update.rs module docs).
        let old_substr = self.old_substr.clone();
        let new_substr = self.new_substr.clone();
        let limit = self.limit;
        let update_func: UpdateFunc = Arc::new(move |v: &[u8]| {
            let mut buf: Vec<u8> = Vec::new();
            append_replace(&mut buf, v, &old_substr, &new_substr, limit);
            buf
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

/// Appends `s` to `dst` with up to `limit` occurrences of `old_substr` replaced
/// by `new_substr` (all of them when `limit == 0`).
///
/// Port of Go's `appendReplace`. Operates on bytes to match Go's
/// `strings.Index` byte semantics.
pub(crate) fn append_replace(
    dst: &mut Vec<u8>,
    s: &[u8],
    old_substr: &[u8],
    new_substr: &[u8],
    limit: u64,
) {
    if s.is_empty() {
        return;
    }
    if old_substr.is_empty() {
        dst.extend_from_slice(s);
        return;
    }

    let mut s = s;
    let mut replacements: u64 = 0;
    loop {
        match find_subslice(s, old_substr) {
            None => {
                dst.extend_from_slice(s);
                return;
            }
            Some(n) => {
                dst.extend_from_slice(&s[..n]);
                dst.extend_from_slice(new_substr);
                s = &s[n + old_substr.len()..];
                replacements += 1;
                if limit > 0 && replacements >= limit {
                    dst.extend_from_slice(s);
                    return;
                }
            }
        }
    }
}

/// Returns the byte index of the first occurrence of `needle` in `haystack`.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
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

    fn ar(s: &str, old_substr: &str, new_substr: &str, limit: u64) -> String {
        let mut dst = Vec::new();
        append_replace(
            &mut dst,
            s.as_bytes(),
            old_substr.as_bytes(),
            new_substr.as_bytes(),
            limit,
        );
        String::from_utf8(dst).unwrap()
    }

    // Go TestAppendReplace (pipe_replace_test.go).
    #[test]
    fn test_append_replace() {
        assert_eq!(ar("", "", "", 0), "");
        assert_eq!(ar("", "foo", "bar", 0), "");
        assert_eq!(ar("abc", "foo", "bar", 0), "abc");
        assert_eq!(ar("foo", "foo", "bar", 0), "bar");
        assert_eq!(ar("foox", "foo", "bar", 0), "barx");
        assert_eq!(ar("afoo", "foo", "bar", 0), "abar");
        assert_eq!(ar("afoox", "foo", "bar", 0), "abarx");
        assert_eq!(ar("foo-bar-baz", "-", "_", 0), "foo_bar_baz");
        assert_eq!(ar("foo bar baz  ", " ", "", 1), "foobar baz  ");
    }

    /// Builds the `if (field:phrase)` filter used by the conditional Go tests.
    fn phrase_iff(field: &str, phrase: &str) -> Arc<IfFilter> {
        let f: Arc<dyn Filter> = Arc::new(new_filter_phrase(field.as_bytes(), phrase));
        Arc::new(IfFilter::new(f))
    }

    /// Capturing downstream processor that reconstructs emitted rows, dropping
    /// empty-valued fields (matching Go's `expectPipeResults` canonicalization).
    struct Capture(Mutex<Vec<Vec<Field>>>);

    impl PipeProcessor for Capture {
        fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
            let cols = br.get_columns();
            let n = br.rows_len();
            let mut out = self.0.lock().unwrap();
            for r in 0..n {
                let mut row = Vec::new();
                for &c in &cols {
                    let name = br.column_name(c).to_vec();
                    let value = br.column_get_value_at_row(c, r).to_vec();
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

    fn canon(mut row: Vec<Field>) -> Vec<(Vec<u8>, Vec<u8>)> {
        row.sort_by(|a, b| a.name.cmp(&b.name));
        row.into_iter().map(|f| (f.name, f.value)).collect()
    }

    fn run(pipe: PipeReplace, rows: Vec<Vec<Field>>) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
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

    fn f(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn expected(rows: Vec<Vec<Field>>) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
        rows.into_iter()
            .map(|r| canon(r.into_iter().filter(|f| !f.value.is_empty()).collect()))
            .collect()
    }

    // Go TestPipeReplace "replace without limits at _msg".
    #[test]
    fn test_pipe_replace_msg() {
        // replace ("_", "-") at _msg, no limit
        let out = run(
            PipeReplace::new("_msg", "_", "-", 0, None),
            vec![
                vec![f("_msg", "a_bc_def"), f("bar", "cde")],
                vec![f("_msg", "a_bc_def")],
                vec![f("_msg", "1234")],
                vec![f("_msg", "1234")],
            ],
        );
        assert_eq!(
            out,
            expected(vec![
                vec![f("_msg", "a-bc-def"), f("bar", "cde")],
                vec![f("_msg", "a-bc-def")],
                vec![f("_msg", "1234")],
                vec![f("_msg", "1234")],
            ])
        );
    }

    // Go TestPipeReplace "replace with limit 1 at foo".
    #[test]
    fn test_pipe_replace_limit_1() {
        let out = run(
            PipeReplace::new("foo", "_", "-", 1, None),
            vec![
                vec![f("foo", "a_bc_def"), f("bar", "cde")],
                vec![f("foo", "1234")],
            ],
        );
        assert_eq!(
            out,
            expected(vec![
                vec![f("foo", "a-bc_def"), f("bar", "cde")],
                vec![f("foo", "1234")],
            ])
        );
    }

    // Go TestPipeReplace "replace with limit 100 at foo".
    #[test]
    fn test_pipe_replace_limit_100() {
        let out = run(
            PipeReplace::new("foo", "_", "-", 100, None),
            vec![
                vec![f("foo", "a_bc_def"), f("bar", "cde")],
                vec![f("foo", "1234")],
            ],
        );
        assert_eq!(
            out,
            expected(vec![
                vec![f("foo", "a-bc-def"), f("bar", "cde")],
                vec![f("foo", "1234")],
            ])
        );
    }

    // Go TestPipeReplace "conditional replace at foo":
    // `replace if (bar:abc) ("_", "") at foo`.
    #[test]
    fn test_pipe_replace_conditional() {
        let out = run(
            PipeReplace::new("foo", "_", "", 0, Some(phrase_iff("bar", "abc"))),
            vec![
                vec![f("foo", "a_bc_def"), f("bar", "cde")],
                vec![f("foo", "123_456"), f("bar", "abc")],
            ],
        );
        assert_eq!(
            out,
            expected(vec![
                vec![f("foo", "a_bc_def"), f("bar", "cde")],
                vec![f("foo", "123456"), f("bar", "abc")],
            ])
        );
    }

    // Go TestPipeReplaceUpdateNeededFields (pipe_replace_test.go).
    #[test]
    fn test_pipe_replace_update_needed_fields() {
        let p = |iff: Option<(&str, &str)>, at: &str| {
            PipeReplace::new(at, "a", "b", 0, iff.map(|(f, ph)| phrase_iff(f, ph)))
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

    // Go TestParsePipeReplaceSuccess (pipe_replace_test.go): parse + String()
    // round-trip, matching Go's `expectParsePipeSuccess`.
    #[test]
    fn test_parse_pipe_replace_success() {
        fn p(pipe_str: &str) {
            let pipe = crate::pipe::must_parse_pipe(pipe_str, 0);
            assert_eq!(pipe.to_string(), pipe_str, "round-trip mismatch");
        }

        p(r#"replace (foo, bar)"#);
        p(r#"replace (" ", "") at x"#);
        p(r#"replace if (x:y) ("-", ":") at a"#);
        p(r#"replace (" ", "") at x limit 10"#);
        p(r#"replace if (x:y) (" ", "") at foo limit 10"#);
    }

    #[test]
    fn test_string() {
        assert_eq!(
            PipeReplace::new("_msg", "a", "b", 0, None).to_string(),
            "replace (a, b)"
        );
        assert_eq!(
            PipeReplace::new("x", " ", "", 10, None).to_string(),
            r#"replace (" ", "") at x limit 10"#
        );
        assert_eq!(
            PipeReplace::new("a", "-", ":", 0, Some(phrase_iff("x", "y"))).to_string(),
            r#"replace if (x:y) ("-", ":") at a"#
        );
    }
}

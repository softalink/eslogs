//! Port of `lib/logstorage/pipe_replace.go` — the `| replace (old, new)` pipe,
//! which replaces occurrences of a literal substring inside a field.
//!
//! See <https://docs.victoriametrics.com/victorialogs/logsql/#replace-pipe>.
//!
//! PORT NOTE — `iff` (`ifFilter`): Go's `pipeReplace` carries an optional
//! `if (...)` filter that gates which rows get updated. `ifFilter` is not ported
//! yet, so the `if` clause is dropped: the processor applies the replacement to
//! every row (identical to Go when `iff == nil`), `String()` omits the `if`
//! part, and `update_needed_fields` is a no-op (Go's
//! `updateNeededFieldsForUpdatePipe` is also a no-op when `iff == nil`).
//!
//! PORT NOTE — parser: `parsePipeReplace` depends on the query lexer, which is
//! not ported. The `pub(crate)` [`PipeReplace::new`] constructor is exposed for
//! a future parser to call; the lexer-driven parse is deferred.
//!
//! PORT NOTE — shared update processor: Go routes replacement through
//! `newPipeUpdateProcessor` (`pipe_update.go`). That shared helper is not ported
//! here; its per-row logic (with consecutive-duplicate caching) is inlined into
//! [`PipeReplaceProcessor`].

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;

/// `| replace (old, new) [at field] [limit N]` pipe.
///
/// Port of Go's `pipeReplace`.
#[derive(Clone, Debug)]
pub(crate) struct PipeReplace {
    /// The field whose value is rewritten (defaults to `_msg`).
    pub(crate) field: String,
    /// The literal substring to replace.
    pub(crate) old_substr: String,
    /// The replacement string.
    pub(crate) new_substr: String,
    /// Maximum number of replacements per value (0 = unlimited).
    pub(crate) limit: u64,
}

impl PipeReplace {
    /// Builds a `replace` pipe from parsed arguments.
    pub(crate) fn new(
        field: impl Into<String>,
        old_substr: impl Into<String>,
        new_substr: impl Into<String>,
        limit: u64,
    ) -> Self {
        Self {
            field: field.into(),
            old_substr: old_substr.into(),
            new_substr: new_substr.into(),
            limit,
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
        s.push_str(&format!(
            " ({}, {})",
            quote_token_if_needed(&self.old_substr),
            quote_token_if_needed(&self.new_substr)
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
        // PORT NOTE: no-op — Go's updateNeededFieldsForUpdatePipe only mutates
        // pf when iff != nil, and iff is deferred (see module docs).
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeReplaceProcessor {
            field: self.field.clone(),
            old_substr: self.old_substr.clone().into_bytes(),
            new_substr: self.new_substr.clone().into_bytes(),
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

/// Execution half of [`PipeReplace`].
///
/// PORT NOTE: stateless across blocks (no sharding needed) — each block is
/// rewritten and forwarded immediately, and `flush` is a no-op.
struct PipeReplaceProcessor {
    field: String,
    old_substr: Vec<u8>,
    new_substr: Vec<u8>,
    limit: u64,
    pp_next: Arc<dyn PipeProcessor>,
}

impl PipeProcessor for PipeReplaceProcessor {
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
            // Consecutive-duplicate caching, mirroring Go's needUpdates/vPrev.
            if i == 0 || values[i - 1] != *v {
                v_new = Vec::new();
                append_replace(
                    &mut v_new,
                    v,
                    &self.old_substr,
                    &self.new_substr,
                    self.limit,
                );
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
    use crate::rows::Field;
    use std::sync::Mutex;

    // PORT NOTE: TestParsePipeReplaceSuccess/Failure and
    // TestPipeReplaceUpdateNeededFields are skipped — they exercise the query
    // lexer / needed-fields planner (parser + ifFilter), both deferred.

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

    fn run(pipe: PipeReplace, rows: Vec<Vec<Field>>) -> Vec<Vec<(String, String)>> {
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
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn expected(rows: Vec<Vec<Field>>) -> Vec<Vec<(String, String)>> {
        rows.into_iter()
            .map(|r| canon(r.into_iter().filter(|f| !f.value.is_empty()).collect()))
            .collect()
    }

    #[test]
    fn test_pipe_replace_msg() {
        // replace ("_", "-") at _msg, no limit
        let out = run(
            PipeReplace::new("_msg", "_", "-", 0),
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

    #[test]
    fn test_pipe_replace_limit_1() {
        let out = run(
            PipeReplace::new("foo", "_", "-", 1),
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

    #[test]
    fn test_pipe_replace_limit_100() {
        let out = run(
            PipeReplace::new("foo", "_", "-", 100),
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

    #[test]
    fn test_string() {
        assert_eq!(
            PipeReplace::new("_msg", "a", "b", 0).to_string(),
            "replace (a, b)"
        );
        assert_eq!(
            PipeReplace::new("x", " ", "", 10).to_string(),
            r#"replace (" ", "") at x limit 10"#
        );
    }
}

//! Port of `pipe_union.go` — the `| union (<subquery>|rows(...))` pipe, which
//! appends the rows produced by a subquery (or by inline `rows(...)`) after all
//! the input rows have been processed.
//!
//! PORT NOTE — subquery representation: Go's `pipeUnion` embeds `q *Query` and
//! a `runQuery` callback wired up by `initUnionQuery`; the port models the
//! subquery as its already-rendered query text ([`PipeUnion::query_text`]) —
//! the established subquery pattern — and `storage_search::init_subqueries`
//! wires the [`crate::pipe::RunUnionQueryFn`] callback via
//! [`Pipe::init_union_query`] before the search. `flush` then executes the
//! subquery and streams its blocks to `pp_next`, exactly like Go's lazy
//! single-node path (`eagerExecute == false`; the eager cluster path is
//! deferred with `net_query_runner`).

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::block_result::BlockResult;
use crate::pipe::{Pipe, PipeProcessor, RunUnionQueryFn};
use crate::prefix_filter;
use crate::rows::Field;

/// `pipeUnion` implements `| union ...`.
pub struct PipeUnion {
    /// Opaque rendered text of the union subquery (Go `q.String()`), present
    /// only when the union uses a subquery rather than inline rows.
    ///
    /// PORT NOTE: this stands in for Go's `q *Query`; it is executed by the
    /// processor's `flush` via `run_query`.
    pub(crate) query_text: Option<String>,

    /// Inline rows to append after the input (Go `rows`). `Some` iff the union
    /// was built with `rows(...)` instead of a subquery.
    pub(crate) rows: Option<Vec<Vec<Field>>>,

    /// Executes the union subquery; wired by
    /// `storage_search::init_subqueries` via [`Pipe::init_union_query`]
    /// before query execution (Go `runQuery runUnionQueryFunc`).
    pub(crate) run_query: Option<RunUnionQueryFn>,
}

/// Constructs a `union` pipe from already-parsed components (the tail of Go's
/// `parsePipeUnion`, whose lexer half lives in `parser::parse_pipe`).
pub(crate) fn new_pipe_union(
    rows: Option<Vec<Vec<Field>>>,
    query_text: Option<String>,
) -> PipeUnion {
    PipeUnion {
        query_text,
        rows,
        run_query: None,
    }
}

impl Pipe for PipeUnion {
    /// Port of Go `pipeUnion.visitSubqueries`: visits the union subquery.
    fn visit_subqueries_mut(
        &mut self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
        let Some(q_text) = self.query_text.as_mut() else {
            return;
        };
        let mut q = crate::parser::query::must_parse_query(q_text, timestamp);
        q.visit_subqueries(visit);
        *q_text = q.to_string();
    }

    /// Port of Go `pipeUnion.splitToRemoteAndLocal`: the pipe (and
    /// everything after it) runs locally only.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (None, vec![crate::pipe::clone_pipe(self, timestamp)])
    }

    /// Port of Go `pipeUnion.initUnionQuery` with `eagerExecute == true`
    /// (`NetQueryRunner`): executes the union subquery via `get_rows` and
    /// inlines the results as `union rows(...)`.
    fn init_union_query_eager(
        &mut self,
        get_rows: &mut crate::storage_search::GetJoinRowsFn<'_>,
    ) -> Result<(), String> {
        if self.rows.is_some() {
            // Go: rows already inlined — nothing to execute.
            return Ok(());
        }
        let Some(query_text) = self.query_text.clone() else {
            return Ok(());
        };
        let rows = get_rows(&query_text).map_err(|e| {
            format!(
                "cannot execute query at pipe [{}]: {e}",
                crate::pipe::Pipe::to_string(self)
            )
        })?;
        self.rows = Some(rows);
        // Go: `puNew.q = nil` once the rows are materialized.
        self.query_text = None;
        Ok(())
    }

    fn to_string(&self) -> String {
        let mut dst: Vec<u8> = Vec::new();
        dst.extend_from_slice(b"union ");

        if let Some(rows) = &self.rows {
            crate::pipe_join::marshal_rows(&mut dst, rows);
        } else {
            dst.push(b'(');
            // PORT NOTE: Go appends `pu.q.String()`; the port stores the
            // already-rendered subquery text and uses it verbatim.
            dst.extend_from_slice(self.query_text.as_deref().unwrap_or("").as_bytes());
            dst.push(b')');
        }

        String::from_utf8_lossy(&dst).into_owned()
    }

    fn can_live_tail(&self) -> bool {
        false
    }

    fn can_return_last_n_results(&self) -> bool {
        false
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        false
    }

    /// Port of Go `isPipeSafeForHits` for `*pipeUnion`: a union with a
    /// subquery is allowed (after dropping pipes unsafe for hits inside it —
    /// see <https://github.com/VictoriaMetrics/VictoriaLogs/issues/641>);
    /// `union rows(...)` is unsafe in the general case.
    ///
    /// PORT NOTE: Go mutates the parsed subquery in place
    /// (`t.q.dropPipesUnsafeForHits()`); the Rust pipe stores the subquery as
    /// rendered text, so it is re-parsed at `timestamp`, sanitized and
    /// re-rendered here.
    fn is_safe_for_hits(&mut self, timestamp: i64) -> bool {
        let Some(query_text) = &self.query_text else {
            // the union rows(...) is unsafe to use for hits in general case
            return false;
        };
        match crate::parser::ParseQueryAtTimestamp(query_text, timestamp) {
            Ok(mut q) => {
                q.drop_pipes_unsafe_for_hits();
                self.query_text = Some(q.to_string());
                true
            }
            Err(_) => false,
        }
    }

    fn subquery_is_fixed_output_fields_order(&self) -> Option<bool> {
        let query_text = self.query_text.as_deref()?;
        // PORT NOTE: Go reads the parsed subquery (`pu.q`); the Rust pipe
        // stores rendered text, so re-parse it (the timestamp does not affect
        // the output fields order).
        let q = crate::parser::ParseQuery(query_text).ok()?;
        Some(q.is_fixed_output_fields_order())
    }

    fn has_filter_in_with_query(&self) -> bool {
        // The pu.q query with possible in(...) filters is processed independently
        // at pu.flush(), so return false here.
        false
    }

    fn is_union_pipe(&self) -> bool {
        true
    }

    /// Port of Go `(*pipeUnion).initUnionQuery` (lazy single-node path:
    /// `eagerExecute == false`, so the subquery runs at the processor's
    /// `flush`).
    fn init_union_query(&mut self, run_query: &RunUnionQueryFn) -> Result<(), String> {
        self.run_query = Some(Arc::clone(run_query));
        Ok(())
    }

    fn update_needed_fields(&self, _pf: &mut prefix_filter::Filter) {
        // nothing to do
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeUnionProcessor {
            query_text: self.query_text.clone(),
            rows: self.rows.clone(),
            run_query: self.run_query.clone(),
            pp_next,
        })
    }
}

struct PipeUnionProcessor {
    query_text: Option<String>,
    rows: Option<Vec<Vec<Field>>>,
    run_query: Option<RunUnionQueryFn>,
    pp_next: Arc<dyn PipeProcessor>,
}

impl PipeProcessor for PipeUnionProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }
        self.pp_next.write_block(worker_id, br);
    }

    fn flush(&self) -> Result<(), String> {
        // execute the query to union
        //
        // PORT NOTE: Go wraps the pipeline stop channel into a context here
        // (`contextutil.NewStopChanContext`); the Rust `run_query` has no
        // cancellation (see its PORT NOTE), so nothing is threaded through.
        if let Some(rows) = &self.rows {
            let mut br = BlockResult::default();
            br.must_init_from_rows(rows);
            self.pp_next.write_block(0, &mut br);
            return Ok(());
        }

        match (&self.query_text, &self.run_query) {
            (Some(q_text), Some(run_query)) => run_query(q_text, Arc::clone(&self.pp_next)),
            // PORT NOTE: reachable only when the pipe runs outside
            // `Storage::run_query` (unit harnesses) — `init_union_query`
            // wires `run_query` before every real query execution (Go would
            // nil-panic here).
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::{assert_needed_fields, assert_rows_eq, rows, run_pipe};

    // PORT NOTE: the `TestParsePipeUnionSuccess` / `TestParsePipeUnionFailure`
    // cases are covered as query round-trips in
    // `parser::tests::test_parse_query_subqueries`; the subquery execution
    // path (`init_union_query` + processor `flush`) is covered by
    // `storage_search::tests::test_storage_run_query_subqueries`.

    fn subquery_union(query_text: &str) -> PipeUnion {
        new_pipe_union(None, Some(query_text.to_string()))
    }

    #[test]
    fn test_pipe_union_update_needed_fields() {
        // all the needed fields
        let p = subquery_union("abc");
        assert_needed_fields(&p, "*", "", "*", "");

        // all the needed fields, non-empty unneeded fields
        let p = subquery_union("abc");
        assert_needed_fields(&p, "*", "f1,f2", "*", "f1,f2");

        // non-empty needed fields
        let p = subquery_union("abc");
        assert_needed_fields(&p, "f1,f2", "", "f1,f2", "");
    }

    // The following tests exercise ONLY the inline-`rows` union path, which needs
    // no subquery execution (Go has no `TestPipeUnion` behavior test; these
    // verify the ported pass-through + inline-rows flush logic).

    #[test]
    fn test_to_string_inline_rows() {
        let p = new_pipe_union(Some(rows(&[])), None);
        assert_eq!(p.to_string(), "union rows()");

        let p = new_pipe_union(
            Some(rows(&[&[("foo", "bar"), ("baz", "123")], &[("q", "w")]])),
            None,
        );
        assert_eq!(
            p.to_string(),
            r#"union rows({"foo":"bar","baz":"123"},{"q":"w"})"#
        );
    }

    #[test]
    fn test_to_string_subquery() {
        let p = subquery_union("foo:bar");
        assert_eq!(p.to_string(), "union (foo:bar)");
    }

    #[test]
    fn test_pipe_union_inline_rows_appends() {
        // Input rows pass through; inline union rows are appended at flush.
        let p = new_pipe_union(Some(rows(&[&[("_msg", "u1"), ("k", "v")]])), None);
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "m1")], &[("_msg", "m2")]])),
            &rows(&[
                &[("_msg", "m1")],
                &[("_msg", "m2")],
                &[("_msg", "u1"), ("k", "v")],
            ]),
        );
    }
}

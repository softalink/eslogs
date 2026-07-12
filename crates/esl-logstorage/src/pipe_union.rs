//! Port of `pipe_union.go` — the `| union (<subquery>|rows(...))` pipe, which
//! appends the rows produced by a subquery (or by inline `rows(...)`) after all
//! the input rows have been processed.
//!
//! PORT NOTE — SUBQUERY DEFERRED: Go's `pipeUnion` embeds `q *Query`, the
//! subquery whose results are appended, and a `runQuery` callback wired up by
//! `initUnionQuery`. The `Query` type is unported and subquery execution
//! (`initUnionQuery`, `runQuery`, `visitSubqueries`/`initFilterInValues`) is
//! deferred per `crate::pipe` PORT NOTES. This port therefore:
//!   * models the subquery as an opaque already-rendered query string
//!     ([`PipeUnion::query_text`]) used only by `to_string`;
//!   * fully ports the local pass-through (`write_block`) and the INLINE-rows
//!     side of `flush` (Go's `pu.rows != nil` branch);
//!   * makes `flush` a no-op when only a subquery is present — that is exactly
//!     where Go would execute `runQuery`. See the PORT NOTE on
//!     [`PipeUnionProcessor::flush`].

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::block_result::BlockResult;
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::rows::Field;

/// `pipeUnion` implements `| union ...`.
pub struct PipeUnion {
    /// Opaque rendered text of the union subquery (Go `q.String()`), present
    /// only when the union uses a subquery rather than inline rows.
    ///
    /// PORT NOTE: this stands in for Go's `q *Query`; execution is deferred.
    pub(crate) query_text: Option<String>,

    /// Inline rows to append after the input (Go `rows`). `Some` iff the union
    /// was built with `rows(...)` instead of a subquery.
    pub(crate) rows: Option<Vec<Vec<Field>>>,
}

/// Constructs a `union` pipe from already-parsed components.
///
/// PORT NOTE: Go's `parsePipeUnion` (and `parseRows`) are lexer-dependent and
/// deferred; this constructor takes either the parsed inline `rows` or an
/// opaque `query_text` for the subquery.
pub(crate) fn new_pipe_union(
    rows: Option<Vec<Vec<Field>>>,
    query_text: Option<String>,
) -> PipeUnion {
    PipeUnion { query_text, rows }
}

impl Pipe for PipeUnion {
    fn to_string(&self) -> String {
        let mut dst: Vec<u8> = Vec::new();
        dst.extend_from_slice(b"union ");

        if let Some(rows) = &self.rows {
            crate::pipe_join::marshal_rows(&mut dst, rows);
        } else {
            dst.push(b'(');
            // PORT NOTE: Go appends `pu.q.String()`; the subquery is deferred, so
            // its already-rendered text is used verbatim.
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
            rows: self.rows.clone(),
            pp_next,
        })
    }
}

struct PipeUnionProcessor {
    rows: Option<Vec<Vec<Field>>>,
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
        match &self.rows {
            Some(rows) => {
                let mut br = BlockResult::default();
                br.must_init_from_rows(rows);
                self.pp_next.write_block(0, &mut br);
                Ok(())
            }
            None => {
                // PORT NOTE: subquery execution deferred (Query unported). Go
                // runs `pu.runQuery(ctx, pu.q, pu.ppNext.writeBlock)` here to
                // append the subquery results; that path needs the unported
                // `Query` engine and is intentionally a no-op until it lands.
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::{assert_needed_fields, assert_rows_eq, rows, run_pipe};

    // PORT NOTE: `TestParsePipeUnionSuccess` / `TestParsePipeUnionFailure`
    // exercise the lexer-based `parsePipeUnion`, which is deferred; they are
    // omitted until the LogsQL parser is ported.

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

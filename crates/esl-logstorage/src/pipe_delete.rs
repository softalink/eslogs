//! Port of `pipe_delete.go` from EsLogs v1.51.0.
//!
//! Implements the `| delete ...` (aka `del` / `rm` / `drop`) pipe, which drops
//! fields matching the configured field filters.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::block_result::BlockResult;
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stats_count::field_names_string;

use esl_common::panicf;

/// `PipeDelete` implements the `| delete ...` pipe.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#delete-pipe>
pub(crate) struct PipeDelete {
    /// List of field filters for the fields to delete.
    pub(crate) field_filters: Vec<String>,
}

/// Builds a `| delete ...` pipe.
///
/// PORT NOTE: `parsePipeDelete` is lexer-dependent and deferred; this
/// constructor exposes the parsed result for the future parser.
pub(crate) fn new_pipe_delete(field_filters: Vec<String>) -> PipeDelete {
    PipeDelete { field_filters }
}

impl Pipe for PipeDelete {
    /// Port of Go `pipeDelete.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn is_fields_or_delete_pipe(&self) -> bool {
        true
    }

    fn to_string(&self) -> String {
        if self.field_filters.is_empty() {
            panicf!("BUG: pipeDelete must contain at least a single field");
        }
        format!("delete {}", field_names_string(&self.field_filters))
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        !prefix_filter::match_filters(&self.field_filters, "_time")
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        Some(crate::pipe::StatsTailOp::Delete {
            field_filters: self.field_filters.clone(),
        })
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_deny_filters(&self.field_filters);
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeDeleteProcessor {
            field_filters: self.field_filters.clone(),
            pp_next,
        })
    }
}

struct PipeDeleteProcessor {
    field_filters: Vec<String>,
    pp_next: Arc<dyn PipeProcessor>,
}

impl PipeProcessor for PipeDeleteProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        br.delete_columns_by_filters(&self.field_filters);
        self.pp_next.write_block(worker_id, br);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::pipe_fields::pipe_test_util::*;
    use super::*;

    fn pd(filters: &[&str]) -> PipeDelete {
        new_pipe_delete(filters.iter().map(|s| s.to_string()).collect())
    }

    #[test]
    fn test_pipe_delete_string() {
        assert_eq!(pd(&["f1", "f2"]).to_string(), "delete f1, f2");
        assert_eq!(pd(&["*"]).to_string(), "delete *");
    }

    #[test]
    fn test_pipe_delete_existing_field() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&pd(&["_msg"]), &rows);
        assert_rows_eq(got, &[vec![field("a", "test")]]);
    }

    #[test]
    fn test_pipe_delete_all_fields() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&pd(&["a", "_msg"]), &rows);
        assert_rows_eq(got, &[vec![]]);
    }

    #[test]
    fn test_pipe_delete_wildcard_some() {
        let rows = vec![vec![
            field("a", "foo"),
            field("b", "bar"),
            field("bc", "1235"),
        ]];
        let got = run_pipe(&pd(&["b*"]), &rows);
        assert_rows_eq(got, &[vec![field("a", "foo")]]);
    }

    #[test]
    fn test_pipe_delete_multiple_rows() {
        let rows = vec![
            vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")],
            vec![field("a", "foobar")],
            vec![field("b", "baz"), field("c", "d"), field("e", "afdf")],
            vec![field("c", "dss"), field("b", "df")],
        ];
        let got = run_pipe(&pd(&["_msg", "a"]), &rows);
        assert_rows_eq(
            got,
            &[
                vec![],
                vec![],
                vec![field("b", "baz"), field("c", "d"), field("e", "afdf")],
                vec![field("c", "dss"), field("b", "df")],
            ],
        );
    }

    #[test]
    fn test_pipe_delete_update_needed_fields() {
        // all the needed fields
        expect_needed_fields(&pd(&["s1", "s2"]), "*", "", "*", "s1,s2");
        expect_needed_fields(&pd(&["s*", "s2", "x"]), "*", "", "*", "s*,x");
        expect_needed_fields(&pd(&["*"]), "*", "", "", "");

        // unneeded fields do not intersect with src
        expect_needed_fields(&pd(&["s1", "s2"]), "*", "f1,f2", "*", "s1,s2,f1,f2");
        expect_needed_fields(&pd(&["s1", "s2"]), "*", "f*", "*", "s1,s2,f*");
        expect_needed_fields(&pd(&["s*", "s2"]), "*", "f1,f2", "*", "s*,f1,f2");
        expect_needed_fields(&pd(&["s*", "s2"]), "*", "f*", "*", "s*,f*");

        // unneeded fields intersect with src
        expect_needed_fields(&pd(&["s1", "s2"]), "*", "s1,f1,f2", "*", "s1,s2,f1,f2");
        expect_needed_fields(&pd(&["s1", "s2"]), "*", "s*,f*", "*", "s*,f*");
        expect_needed_fields(&pd(&["s*"]), "*", "s1,f1,f2", "*", "s*,f1,f2");
        expect_needed_fields(&pd(&["s*"]), "*", "s*,f*", "*", "s*,f*");

        // needed fields do not intersect with src
        expect_needed_fields(&pd(&["s1", "s2"]), "f1,f2", "", "f1,f2", "");
        expect_needed_fields(&pd(&["s1", "s2"]), "f*", "", "f*", "");
        expect_needed_fields(&pd(&["s*"]), "f1,f2", "", "f1,f2", "");
        expect_needed_fields(&pd(&["s*"]), "f*", "", "f*", "");

        // needed fields intersect with src
        expect_needed_fields(&pd(&["s1", "s2"]), "s1,f1,f2", "", "f1,f2", "");
        expect_needed_fields(&pd(&["s1", "s2"]), "s*,f*", "", "f*,s*", "s1,s2");
        expect_needed_fields(&pd(&["s*"]), "s1,f1,f2", "", "f1,f2", "");
        expect_needed_fields(&pd(&["s*"]), "s*,f*", "", "f*", "s*");
    }
}

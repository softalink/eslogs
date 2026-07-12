//! Port of `pipe_copy.go` from EsLogs v1.51.0.
//!
//! Implements the `| copy ...` (aka `| cp ...`) pipe, which copies fields
//! matching source field filters to destination field filters.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::block_result::BlockResult;
use crate::filter_generic::quote_field_filter_if_needed;
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;

use esl_common::panicf;

/// `PipeCopy` implements the `| copy ...` pipe.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#copy-pipe>
pub(crate) struct PipeCopy {
    /// Source field filters to copy from.
    pub(crate) src_field_filters: Vec<String>,
    /// Destination field filters to copy to.
    pub(crate) dst_field_filters: Vec<String>,
}

/// Builds a `| copy ...` pipe.
///
/// PORT NOTE: `parsePipeCopy` is lexer-dependent and deferred; this constructor
/// exposes the parsed result for the future parser.
pub(crate) fn new_pipe_copy(
    src_field_filters: Vec<String>,
    dst_field_filters: Vec<String>,
) -> PipeCopy {
    PipeCopy {
        src_field_filters,
        dst_field_filters,
    }
}

impl Pipe for PipeCopy {
    /// Port of Go `pipeCopy.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        if self.src_field_filters.is_empty() {
            panicf!("BUG: pipeCopy must contain at least a single srcFieldFilter");
        }
        let a: Vec<String> = self
            .src_field_filters
            .iter()
            .zip(&self.dst_field_filters)
            .map(|(src, dst)| {
                format!(
                    "{} as {}",
                    quote_field_filter_if_needed(src),
                    quote_field_filter_if_needed(dst)
                )
            })
            .collect();
        format!("copy {}", a.join(", "))
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        !prefix_filter::match_filters(&self.dst_field_filters, "_time")
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        Some(crate::pipe::StatsTailOp::Copy {
            src: self.src_field_filters.clone(),
            dst: self.dst_field_filters.clone(),
        })
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        for i in (0..self.src_field_filters.len()).rev() {
            let src_field_filter = &self.src_field_filters[i];
            let dst_field_filter = &self.dst_field_filters[i];

            let need_src_field = pf.match_string_or_wildcard(dst_field_filter);
            if !prefix_filter::is_wildcard_filter(dst_field_filter) {
                pf.add_deny_filter(dst_field_filter);
            }
            if need_src_field {
                pf.add_allow_filter(src_field_filter);
            }
        }
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeCopyProcessor {
            src_field_filters: self.src_field_filters.clone(),
            dst_field_filters: self.dst_field_filters.clone(),
            pp_next,
        })
    }
}

struct PipeCopyProcessor {
    src_field_filters: Vec<String>,
    dst_field_filters: Vec<String>,
    pp_next: Arc<dyn PipeProcessor>,
}

impl PipeProcessor for PipeCopyProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        br.copy_columns_by_filters(&self.src_field_filters, &self.dst_field_filters);
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

    fn pc(pairs: &[(&str, &str)]) -> PipeCopy {
        new_pipe_copy(
            pairs.iter().map(|(s, _)| s.to_string()).collect(),
            pairs.iter().map(|(_, d)| d.to_string()).collect(),
        )
    }

    #[test]
    fn test_pipe_copy_string() {
        assert_eq!(pc(&[("foo", "bar")]).to_string(), "copy foo as bar");
        assert_eq!(
            pc(&[("foo", "bar"), ("a", "b")]).to_string(),
            "copy foo as bar, a as b"
        );
        assert_eq!(pc(&[("foo*", "bar*")]).to_string(), "copy foo* as bar*");
    }

    #[test]
    fn test_pipe_copy_existing_field() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&pc(&[("a", "b")]), &rows);
        assert_rows_eq(
            got,
            &[vec![
                field("_msg", r#"{"foo":"bar"}"#),
                field("a", "test"),
                field("b", "test"),
            ]],
        );
    }

    #[test]
    fn test_pipe_copy_non_existing_field() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&pc(&[("x", "b")]), &rows);
        assert_rows_eq(
            got,
            &[vec![
                field("_msg", r#"{"foo":"bar"}"#),
                field("a", "test"),
                field("b", ""),
            ]],
        );
    }

    #[test]
    fn test_pipe_copy_swap() {
        // copy a as b, _msg as a, b as _msg
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&pc(&[("a", "b"), ("_msg", "a"), ("b", "_msg")]), &rows);
        assert_rows_eq(
            got,
            &[vec![
                field("_msg", "test"),
                field("a", r#"{"foo":"bar"}"#),
                field("b", "test"),
            ]],
        );
    }

    #[test]
    fn test_pipe_copy_wildcard_prefix() {
        let rows = vec![vec![
            field("_msg", r#"{"foo":"bar"}"#),
            field("a", "test"),
            field("abc", "aaa"),
        ]];
        let got = run_pipe(&pc(&[("a*", "foo*")]), &rows);
        assert_rows_eq(
            got,
            &[vec![
                field("_msg", r#"{"foo":"bar"}"#),
                field("a", "test"),
                field("abc", "aaa"),
                field("foo", "test"),
                field("foobc", "aaa"),
            ]],
        );
    }

    #[test]
    fn test_pipe_copy_multiple_rows() {
        let rows = vec![
            vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")],
            vec![field("a", "foobar")],
            vec![field("b", "baz"), field("c", "d"), field("e", "afdf")],
            vec![field("c", "dss")],
        ];
        let got = run_pipe(&pc(&[("a", "b")]), &rows);
        assert_rows_eq(
            got,
            &[
                vec![
                    field("_msg", r#"{"foo":"bar"}"#),
                    field("a", "test"),
                    field("b", "test"),
                ],
                vec![field("a", "foobar"), field("b", "foobar")],
                vec![field("b", ""), field("c", "d"), field("e", "afdf")],
                vec![field("c", "dss"), field("b", "")],
            ],
        );
    }

    #[test]
    fn test_pipe_copy_update_needed_fields() {
        // all the needed fields
        expect_needed_fields(&pc(&[("s1", "d1"), ("s2", "d2")]), "*", "", "*", "d1,d2");
        expect_needed_fields(&pc(&[("a", "a")]), "*", "", "*", "");
        expect_needed_fields(&pc(&[("foo*", "bar*")]), "*", "", "*", "");
        expect_needed_fields(&pc(&[("foo", "bar*")]), "*", "", "*", "");
        expect_needed_fields(&pc(&[("foo*", "bar")]), "*", "", "*", "bar");
        expect_needed_fields(&pc(&[("*", "bar*")]), "*", "", "*", "");
        expect_needed_fields(&pc(&[("b*", "bar*")]), "*", "", "*", "");
        expect_needed_fields(&pc(&[("bar*", "b*")]), "*", "", "*", "");

        // unneeded fields do not intersect with src and dst
        expect_needed_fields(
            &pc(&[("s1", "d1"), ("s2", "d2")]),
            "*",
            "f1,f2",
            "*",
            "d1,d2,f1,f2",
        );
        expect_needed_fields(
            &pc(&[("s1", "d1"), ("s2", "d2")]),
            "*",
            "f*",
            "*",
            "d1,d2,f*",
        );
        expect_needed_fields(
            &pc(&[("s1*", "d1*"), ("s2", "d2")]),
            "*",
            "f1,f2",
            "*",
            "d2,f1,f2",
        );

        // unneeded fields intersect with src
        expect_needed_fields(
            &pc(&[("s1", "d1"), ("s2", "d2")]),
            "*",
            "s1,f1,f2",
            "*",
            "d1,d2,f1,f2",
        );

        // unneeded fields intersect with dst
        expect_needed_fields(
            &pc(&[("s1", "d1"), ("s2", "d2")]),
            "*",
            "d2,f1,f2",
            "*",
            "d1,d2,f1,f2",
        );
        expect_needed_fields(
            &pc(&[("s1", "d1"), ("s2", "d2")]),
            "*",
            "d*,f*",
            "*",
            "d*,f*",
        );

        // needed fields do not intersect with src and dst
        expect_needed_fields(&pc(&[("s1", "d1"), ("s2", "d2")]), "f1,f2", "", "f1,f2", "");

        // needed fields intersect with src
        expect_needed_fields(
            &pc(&[("s1", "d1"), ("s2", "d2")]),
            "s1,f1,f2",
            "",
            "s1,f1,f2",
            "",
        );

        // needed fields intersect with dst
        expect_needed_fields(
            &pc(&[("s1", "d1"), ("s2", "d2")]),
            "d1,f1,f2",
            "",
            "f1,f2,s1",
            "",
        );
        expect_needed_fields(
            &pc(&[("s1", "d1"), ("s2", "d2")]),
            "d*,f*",
            "",
            "d*,f*,s1,s2",
            "d1,d2",
        );

        // needed fields intersect with src and dst
        expect_needed_fields(
            &pc(&[("s1", "d1"), ("s2", "d2")]),
            "s1,d1,f1,f2",
            "",
            "s1,f1,f2",
            "",
        );
        expect_needed_fields(
            &pc(&[("s1", "d1"), ("s2", "d2")]),
            "s*,d*,f1,f2",
            "",
            "d*,f1,f2,s*",
            "d1,d2",
        );
    }
}

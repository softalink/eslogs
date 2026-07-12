//! Port of `pipe_first.go` from EsLogs v1.51.0.
//!
//! Implements the `| first ...` pipe, a thin wrapper over the `| sort` pipe
//! returning the first N results (ascending order).

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_last::pipe_last_first_string;
use crate::pipe_sort::PipeSort;
use crate::pipe_sort_topk::new_pipe_topk_processor;
use crate::prefix_filter;

/// `PipeFirst` implements the `| first ...` pipe.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#first-pipe>
pub(crate) struct PipeFirst {
    pub(crate) ps: PipeSort,
}

/// Builds a `| first ...` pipe from an already-configured sort description.
///
/// PORT NOTE: `parsePipeFirst` is lexer-dependent and deferred; this
/// constructor exposes the parsed result for the future parser.
pub(crate) fn new_pipe_first(ps: PipeSort) -> PipeFirst {
    PipeFirst { ps }
}

impl Pipe for PipeFirst {
    /// Port of Go `pipeFirst.splitToRemoteAndLocal` (delegates to the wrapped
    /// sort, so both sides run as plain `sort` pipes).
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        self.ps.split_to_remote_and_local(timestamp)
    }

    fn get_offset_limit(&self) -> Option<(u64, u64)> {
        crate::pipe_sort::get_offset_limit_from_pipe_sort(&self.ps)
    }

    /// Port of Go `pipeFirst.addPartitionByTime` (delegates to the wrapped sort).
    fn add_partition_by_time(&mut self, step: i64) {
        self.ps.add_partition_by_time(step);
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        // The first pipe does not change the set of fields.
        Some(crate::pipe::StatsTailOp::Keep)
    }

    fn to_string(&self) -> String {
        pipe_last_first_string(&self.ps)
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        self.ps.update_needed_fields(pf);
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        new_pipe_topk_processor(&self.ps, concurrency, stop, pp_next)
    }
}

#[cfg(test)]
mod tests {
    use super::super::pipe_fields::pipe_test_util::*;
    use super::*;
    use crate::pipe_sort::BySortField;

    fn ps(by: &[(&str, bool)], limit: u64, rank: &str, partition: &[&str]) -> PipeSort {
        PipeSort::new(
            by.iter().map(|(n, d)| BySortField::new(*n, *d)).collect(),
            false,
            0,
            limit,
            rank.to_string(),
            partition.iter().map(|s| s.to_string()).collect(),
        )
    }

    fn first(by: &[(&str, bool)], limit: u64, rank: &str, partition: &[&str]) -> PipeFirst {
        new_pipe_first(ps(by, limit, rank, partition))
    }

    #[test]
    fn test_pipe_first_string() {
        assert_eq!(first(&[], 1, "", &[]).to_string(), "first");
        assert_eq!(
            first(&[("x", false)], 1, "", &[]).to_string(),
            "first by (x)"
        );
        assert_eq!(
            first(&[("x", false)], 10, "", &[]).to_string(),
            "first 10 by (x)"
        );
    }

    #[test]
    fn test_pipe_first_all_fields() {
        let rows = vec![
            vec![field("_msg", "def"), field("a", "1")],
            vec![field("_msg", "abc"), field("a", "2")],
        ];
        let got = run_pipe(&first(&[], 1, "", &[]), &rows);
        assert_rows_eq(got, &[vec![field("_msg", "abc"), field("a", "2")]]);
    }

    #[test]
    fn test_pipe_first_by_single_field_asc() {
        let rows = vec![
            vec![field("_msg", "abc"), field("a", "2")],
            vec![field("_msg", "def"), field("a", "1")],
        ];
        let got = run_pipe(&first(&[("a", false)], 1, "", &[]), &rows);
        assert_rows_eq(got, &[vec![field("_msg", "def"), field("a", "1")]]);
    }

    #[test]
    fn test_pipe_first_by_single_field_desc() {
        let rows = vec![
            vec![field("_msg", "abc"), field("a", "2")],
            vec![field("_msg", "def"), field("a", "1")],
        ];
        let got = run_pipe(&first(&[("a", true)], 1, "", &[]), &rows);
        assert_rows_eq(got, &[vec![field("_msg", "abc"), field("a", "2")]]);
    }

    #[test]
    fn test_pipe_first_update_needed_fields() {
        // all the needed fields
        expect_needed_fields(&first(&[], 1, "", &[]), "*", "", "*", "");
        expect_needed_fields(&first(&[], 1, "x", &[]), "*", "", "*", "");
        expect_needed_fields(
            &first(&[("s1", false), ("s2", false)], 10, "", &[]),
            "*",
            "",
            "*",
            "",
        );
        expect_needed_fields(
            &first(&[("s1", false), ("s2", false)], 3, "x", &[]),
            "*",
            "",
            "*",
            "x",
        );
        expect_needed_fields(
            &first(&[("s1", false), ("s2", false)], 3, "x", &["x", "y"]),
            "*",
            "",
            "*",
            "",
        );
        expect_needed_fields(
            &first(&[("x", false), ("s2", false)], 3, "x", &[]),
            "*",
            "",
            "*",
            "",
        );

        // unneeded fields do not intersect with src
        expect_needed_fields(
            &first(&[("s1", false), ("s2", false)], 1, "", &[]),
            "*",
            "f1,f2",
            "*",
            "f1,f2",
        );
        expect_needed_fields(
            &first(&[("s1", false), ("s2", false)], 1, "x", &[]),
            "*",
            "f1,f2",
            "*",
            "f1,f2,x",
        );

        // needed fields do not intersect with src
        expect_needed_fields(
            &first(&[("s1", false), ("s2", false)], 1, "", &[]),
            "f1,f2",
            "",
            "s1,s2,f1,f2",
            "",
        );

        // needed fields intersect with src
        expect_needed_fields(
            &first(&[("s1", false), ("s2", false)], 1, "", &[]),
            "s1,f1,f2",
            "",
            "s1,s2,f1,f2",
            "",
        );
    }
}

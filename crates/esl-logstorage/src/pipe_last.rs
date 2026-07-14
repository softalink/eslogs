//! Port of `pipe_last.go` from EsLogs v1.51.0.
//!
//! Implements the `| last ...` pipe (the descending-order sibling of `first`),
//! a thin wrapper over the `| sort` pipe returning the last N results. This
//! module also hosts the shared `pipe_last_first_string` helper used by both
//! `pipe_last` and `pipe_first`.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_sort::PipeSort;
use crate::pipe_sort_topk::new_pipe_topk_processor;
use crate::prefix_filter;
use crate::stats_count::field_names_string;

/// Renders the shared string representation of the `first`/`last` pipes.
///
/// Port of Go's `pipeLastFirstString`.
pub(crate) fn pipe_last_first_string(ps: &PipeSort) -> String {
    let mut s = if ps.is_desc {
        String::from("last")
    } else {
        String::from("first")
    };
    if ps.limit != 1 {
        s += &format!(" {}", ps.limit);
    }
    if !ps.by_fields.is_empty() {
        let a: Vec<String> = ps.by_fields.iter().map(|bf| bf.to_string()).collect();
        s += &format!(" by ({})", a.join(", "));
    }
    if !ps.partition_by_fields.is_empty() {
        s += &format!(
            " partition by ({})",
            field_names_string(&ps.partition_by_fields)
        );
    }
    if !ps.rank_field_name.is_empty() {
        s += &rank_field_name_string(&ps.rank_field_name);
    }
    s
}

/// PORT NOTE: Go's `rankFieldNameString` lives in `pipe_top.go`, which is not
/// ported yet; local copy until that module lands (matching `pipe_sort.rs`).
fn rank_field_name_string(rank_field_name: &[u8]) -> String {
    let mut s = String::from(" rank");
    if rank_field_name != b"rank" {
        s += &format!(
            " as {}",
            crate::parser::quote_token_bytes_if_needed(rank_field_name)
        );
    }
    s
}

/// `PipeLast` implements the `| last ...` pipe.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#last-pipe>
pub(crate) struct PipeLast {
    pub(crate) ps: PipeSort,
}

/// Builds a `| last ...` pipe from an already-configured sort description.
///
/// PORT NOTE: `parsePipeLast` is lexer-dependent and deferred; this constructor
/// exposes the parsed result (with descending order forced on, mirroring
/// `parsePipeLast`) for the future parser.
pub(crate) fn new_pipe_last(mut ps: PipeSort) -> PipeLast {
    ps.is_desc = true;
    PipeLast { ps }
}

impl Pipe for PipeLast {
    /// Port of Go `pipeLast.splitToRemoteAndLocal` (delegates to the wrapped
    /// sort, so both sides run as plain `sort` pipes).
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        self.ps.split_to_remote_and_local(timestamp)
    }

    fn get_offset_limit(&self) -> Option<(u64, u64)> {
        crate::pipe_sort::get_offset_limit_from_pipe_sort(&self.ps)
    }

    /// Port of Go `pipeLast.addPartitionByTime` (delegates to the wrapped sort).
    fn add_partition_by_time(&mut self, step: i64) {
        self.ps.add_partition_by_time(step);
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        // The last pipe does not change the set of fields.
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

// PORT NOTE: `write_block`/`flush` for last/first live entirely in the shared
// topk processor (`pipe_sort_topk`); there is no dedicated processor type here.

#[cfg(test)]
mod tests {
    use super::super::pipe_fields::pipe_test_util::*;
    use super::*;
    use crate::pipe_sort::{BySortField, PipeSort};

    fn ps(by: &[(&str, bool)], limit: u64, rank: &str, partition: &[&str]) -> PipeSort {
        PipeSort::new(
            by.iter().map(|(n, d)| BySortField::new(*n, *d)).collect(),
            false,
            0,
            limit,
            rank.to_string(),
            partition.iter().map(|s| s.as_bytes().to_vec()).collect(),
        )
    }

    fn last(by: &[(&str, bool)], limit: u64, rank: &str, partition: &[&str]) -> PipeLast {
        new_pipe_last(ps(by, limit, rank, partition))
    }

    #[test]
    fn test_pipe_last_string() {
        assert_eq!(last(&[], 1, "", &[]).to_string(), "last");
        assert_eq!(last(&[("x", false)], 1, "", &[]).to_string(), "last by (x)");
        assert_eq!(
            last(&[("x", false)], 10, "", &[]).to_string(),
            "last 10 by (x)"
        );
    }

    #[test]
    fn test_pipe_last_all_fields() {
        let rows = vec![
            vec![field("_msg", "def"), field("a", "1")],
            vec![field("_msg", "abc"), field("a", "2")],
        ];
        let got = run_pipe(&last(&[], 1, "", &[]), &rows);
        assert_rows_eq(got, &[vec![field("_msg", "def"), field("a", "1")]]);
    }

    #[test]
    fn test_pipe_last_by_single_field_desc() {
        let rows = vec![
            vec![field("_msg", "abc"), field("a", "2")],
            vec![field("_msg", "def"), field("a", "1")],
        ];
        let got = run_pipe(&last(&[("a", true)], 1, "", &[]), &rows);
        assert_rows_eq(got, &[vec![field("_msg", "def"), field("a", "1")]]);
    }

    #[test]
    fn test_pipe_last_update_needed_fields() {
        // all the needed fields
        expect_needed_fields(&last(&[], 1, "", &[]), "*", "", "*", "");
        expect_needed_fields(&last(&[], 1, "x", &[]), "*", "", "*", "");
        expect_needed_fields(
            &last(&[("s1", false), ("s2", false)], 10, "", &[]),
            "*",
            "",
            "*",
            "",
        );
        expect_needed_fields(
            &last(&[("s1", false), ("s2", false)], 3, "x", &[]),
            "*",
            "",
            "*",
            "x",
        );
        expect_needed_fields(
            &last(&[("s1", false), ("s2", false)], 3, "x", &["x"]),
            "*",
            "",
            "*",
            "",
        );
        expect_needed_fields(
            &last(&[("x", false), ("s2", false)], 3, "x", &[]),
            "*",
            "",
            "*",
            "",
        );

        // unneeded fields do not intersect with src
        expect_needed_fields(
            &last(&[("s1", false), ("s2", false)], 1, "", &[]),
            "*",
            "f1,f2",
            "*",
            "f1,f2",
        );
        expect_needed_fields(
            &last(&[("s1", false), ("s2", false)], 1, "x", &[]),
            "*",
            "f1,f2",
            "*",
            "f1,f2,x",
        );

        // needed fields do not intersect with src
        expect_needed_fields(
            &last(&[("s1", false), ("s2", false)], 1, "", &[]),
            "f1,f2",
            "",
            "s1,s2,f1,f2",
            "",
        );
        expect_needed_fields(
            &last(&[("s1", false), ("s2", false)], 1, "x", &[]),
            "f1,f2",
            "",
            "s1,s2,f1,f2",
            "",
        );

        // needed fields intersect with src
        expect_needed_fields(
            &last(&[("s1", false), ("s2", false)], 1, "", &[]),
            "s1,f1,f2",
            "",
            "s1,s2,f1,f2",
            "",
        );
        expect_needed_fields(
            &last(&[("s1", false), ("s2", false)], 1, "x", &[]),
            "s1,f1,f2,x",
            "",
            "s1,s2,f1,f2",
            "",
        );
    }
}

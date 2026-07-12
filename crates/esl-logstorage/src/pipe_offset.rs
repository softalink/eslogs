//! Port of `pipe_offset.go` from EsLogs v1.51.0.
//!
//! Implements the `| offset N` (aka `| skip N`) pipe, which drops the first
//! `N` rows and passes the rest through.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::block_result::BlockResult;
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;

/// `PipeOffset` implements the `| offset N` pipe.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#offset-pipe>
pub(crate) struct PipeOffset {
    pub(crate) offset: u64,
}

/// Builds a `| offset N` pipe.
///
/// PORT NOTE: `parsePipeOffset` is lexer-dependent and deferred; this
/// constructor exposes the parsed result for the future parser.
pub(crate) fn new_pipe_offset(offset: u64) -> PipeOffset {
    PipeOffset { offset }
}

impl Pipe for PipeOffset {
    /// Port of Go `pipeOffset.splitToRemoteAndLocal`.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        if self.offset == 0 {
            // Special case - `offset 0` is safe to push to the remote side.
            return (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new());
        }
        (None, vec![crate::pipe::clone_pipe(self, timestamp)])
    }

    fn offset_pipe_value(&self) -> Option<u64> {
        Some(self.offset)
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        // Allowed in instant stats queries only (disallowed when step > 0).
        Some(crate::pipe::StatsTailOp::OffsetLimit)
    }

    fn fixed_fields_transparent(&self) -> bool {
        true
    }

    fn to_string(&self) -> String {
        format!("offset {}", self.offset)
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
        Arc::new(PipeOffsetProcessor {
            offset: self.offset,
            pp_next,
            rows_processed: AtomicU64::new(0),
        })
    }
}

struct PipeOffsetProcessor {
    offset: u64,
    pp_next: Arc<dyn PipeProcessor>,
    rows_processed: AtomicU64,
}

impl PipeProcessor for PipeOffsetProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let block_rows = br.rows_len() as u64;
        let rows_processed =
            self.rows_processed.fetch_add(block_rows, Ordering::SeqCst) + block_rows;
        if rows_processed <= self.offset {
            return;
        }

        let rows_before = rows_processed - block_rows;
        if rows_before >= self.offset {
            self.pp_next.write_block(worker_id, br);
            return;
        }

        let rows_skip = self.offset - rows_before;
        br.skip_rows(rows_skip as usize);
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

    #[test]
    fn test_pipe_offset_string() {
        assert_eq!(new_pipe_offset(10).to_string(), "offset 10");
        assert_eq!(new_pipe_offset(10000).to_string(), "offset 10000");
    }

    #[test]
    fn test_pipe_offset_all() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&new_pipe_offset(100), &rows);
        assert_rows_eq(got, &[]);
    }

    #[test]
    fn test_pipe_offset_zero() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&new_pipe_offset(0), &rows);
        assert_rows_eq(got, &rows);
    }

    #[test]
    fn test_pipe_offset_one() {
        let rows = vec![
            vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")],
            vec![field("_msg", "abc"), field("a", "aiewr")],
        ];
        let got = run_pipe(&new_pipe_offset(1), &rows);
        assert_rows_eq(got, &[vec![field("_msg", "abc"), field("a", "aiewr")]]);
    }

    #[test]
    fn test_pipe_offset_two() {
        let rows = vec![
            vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")],
            vec![
                field("_msg", "sdfsd"),
                field("adffd", "aiewr"),
                field("assdff", "fsf"),
            ],
            vec![
                field("_msg", "abc"),
                field("a", "aiewr"),
                field("asdf", "fsf"),
            ],
        ];
        let got = run_pipe(&new_pipe_offset(2), &rows);
        assert_rows_eq(
            got,
            &[vec![
                field("_msg", "abc"),
                field("a", "aiewr"),
                field("asdf", "fsf"),
            ]],
        );
    }

    #[test]
    fn test_pipe_offset_update_needed_fields() {
        expect_needed_fields(&new_pipe_offset(10), "*", "", "*", "");
        expect_needed_fields(&new_pipe_offset(10), "*", "f1,f2", "*", "f1,f2");
        expect_needed_fields(&new_pipe_offset(10), "f1,f2", "", "f1,f2", "");
    }
}

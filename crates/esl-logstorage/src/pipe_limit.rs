//! Port of `pipe_limit.go` from EsLogs v1.51.0.
//!
//! Implements the `| limit N` (aka `| head N`) pipe, which passes through only
//! the first `N` rows and then signals upstream to stop sending.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::block_result::BlockResult;
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;

/// `PipeLimit` implements the `| limit N` pipe.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#limit-pipe>
pub(crate) struct PipeLimit {
    pub(crate) limit: u64,
}

/// Builds a `| limit N` pipe.
///
/// PORT NOTE: `parsePipeLimit` is lexer-dependent and deferred; this
/// constructor exposes the parsed result for the future parser.
pub(crate) fn new_pipe_limit(limit: u64) -> PipeLimit {
    PipeLimit { limit }
}

impl Pipe for PipeLimit {
    /// Port of Go `pipeLimit.splitToRemoteAndLocal`: the pipe runs both
    /// remotely and locally, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (
            Some(crate::pipe::clone_pipe(self, timestamp)),
            vec![crate::pipe::clone_pipe(self, timestamp)],
        )
    }

    fn limit_pipe_value(&self) -> Option<u64> {
        Some(self.limit)
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        // Allowed in instant stats queries only (disallowed when step > 0).
        Some(crate::pipe::StatsTailOp::OffsetLimit)
    }

    fn fixed_fields_transparent(&self) -> bool {
        true
    }

    fn to_string(&self) -> String {
        format!("limit {}", self.limit)
    }

    fn update_needed_fields(&self, _pf: &mut prefix_filter::Filter) {
        // nothing to do
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        if self.limit == 0 {
            // Special case - notify the caller to stop writing data to the
            // returned processor.
            stop.store(true, Ordering::SeqCst);
        }
        Arc::new(PipeLimitProcessor {
            limit: self.limit,
            stop,
            pp_next,
            rows_processed: AtomicU64::new(0),
        })
    }
}

struct PipeLimitProcessor {
    limit: u64,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    rows_processed: AtomicU64,
}

impl PipeProcessor for PipeLimitProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let block_rows = br.rows_len() as u64;
        let rows_processed =
            self.rows_processed.fetch_add(block_rows, Ordering::SeqCst) + block_rows;
        let limit = self.limit;
        if rows_processed <= limit {
            // Fast path - write all the rows to ppNext.
            self.pp_next.write_block(worker_id, br);
            if rows_processed == limit {
                self.stop.store(true, Ordering::SeqCst);
            }
            return;
        }

        // Slow path - overflow. Write the remaining rows if needed.
        let rows_before = rows_processed - block_rows;
        if rows_before >= limit {
            // Nothing to write. There is no need for a stop() call, since it has
            // been done by another worker.
            return;
        }

        // Write remaining rows.
        let keep_rows = limit - rows_before;
        br.truncate_rows(keep_rows as usize);
        self.pp_next.write_block(worker_id, br);

        // Notify the caller that it should stop passing more data.
        self.stop.store(true, Ordering::SeqCst);
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
    fn test_pipe_limit_string() {
        assert_eq!(new_pipe_limit(10).to_string(), "limit 10");
        assert_eq!(new_pipe_limit(10000).to_string(), "limit 10000");
    }

    #[test]
    fn test_pipe_limit_pass_all() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&new_pipe_limit(100), &rows);
        assert_rows_eq(got, &rows);
    }

    #[test]
    fn test_pipe_limit_zero() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&new_pipe_limit(0), &rows);
        assert_rows_eq(got, &[]);
    }

    #[test]
    fn test_pipe_limit_one_of_two_blocks() {
        let rows = vec![
            vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")],
            vec![field("_msg", "abc"), field("a", "aiewr")],
        ];
        let got = run_pipe(&new_pipe_limit(1), &rows);
        assert_rows_eq(
            got,
            &[vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]],
        );
    }

    #[test]
    fn test_pipe_limit_truncate_within_block() {
        // A single block of two rows, limit 1 keeps only the first row.
        let rows = vec![vec![field("a", "1")], vec![field("a", "2")]];
        let got = run_pipe(&new_pipe_limit(1), &rows);
        assert_rows_eq(got, &[vec![field("a", "1")]]);
    }

    #[test]
    fn test_pipe_limit_update_needed_fields() {
        expect_needed_fields(&new_pipe_limit(10), "*", "", "*", "");
        expect_needed_fields(&new_pipe_limit(10), "*", "f1,f2", "*", "f1,f2");
        expect_needed_fields(&new_pipe_limit(10), "f1,f2", "", "f1,f2", "");
    }
}

//! Port of `pipe_generate_sequence.go` — the `| generate_sequence N` pipe, which
//! ignores its input and emits `N` rows whose `_msg` field is the decimal
//! sequence `0, 1, ..., N-1`.
//!
//! See <https://docs.victoriametrics.com/victorialogs/logsql/#generate_sequence-pipe>

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::values_encoder::marshal_uint64_string;

/// Max buffer size before the generated sequence is flushed downstream, matching
/// Go's `64*1024 - 20` threshold.
const FLUSH_BUF_THRESHOLD: usize = 64 * 1024 - 20;

/// `pipeGenerateSequence` implements `| generate_sequence N`.
pub(crate) struct PipeGenerateSequence {
    /// Number of rows to generate in the sequence.
    pub(crate) n: u64,
}

/// Constructs a `generate_sequence` pipe from the already-parsed row count.
///
/// PORT NOTE: Go's `parsePipeGenerateSequence` is lexer-dependent and deferred;
/// this constructor takes the parsed `n` directly.
pub(crate) fn new_pipe_generate_sequence(n: u64) -> PipeGenerateSequence {
    PipeGenerateSequence { n }
}

impl Pipe for PipeGenerateSequence {
    /// Port of Go `pipeGenerateSequence.splitToRemoteAndLocal`: the pipe (and
    /// everything after it) runs locally only.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (None, vec![crate::pipe::clone_pipe(self, timestamp)])
    }

    fn to_string(&self) -> String {
        format!("generate_sequence {}", self.n)
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.reset();
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        // PORT NOTE: Go keeps no per-worker shard state for this pipe — the whole
        // sequence is produced in flush() on worker 0 — so the port also keeps
        // none.
        Arc::new(PipeGenerateSequenceProcessor {
            n: self.n,
            stop,
            pp_next,
        })
    }
}

struct PipeGenerateSequenceProcessor {
    n: u64,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
}

impl PipeProcessor for PipeGenerateSequenceProcessor {
    fn write_block(&self, _worker_id: usize, _br: &mut BlockResult) {
        // The requested sequence is generated in full in flush(); incoming
        // blocks are ignored.
        //
        // PORT NOTE: Go calls the `cancel()` closure here to notify the caller it
        // may stop sending new data blocks (a pure optimization, since the input
        // is discarded). The single-node port has no independent `cancel` signal
        // — the shared `stop` token is consumed by flush() as the "needStop"
        // check — so this is left as a no-op. Observable output is unchanged.
    }

    fn flush(&self) -> Result<(), String> {
        let mut rc = ResultColumn {
            name: b"_msg".to_vec(),
            values: Vec::new(),
        };

        let mut br = BlockResult::default();
        let mut buf: Vec<u8> = Vec::new();

        for i in 0..self.n {
            if self.stop.load(Ordering::SeqCst) {
                return Ok(());
            }

            let buf_len = buf.len();
            marshal_uint64_string(&mut buf, i);
            rc.add_value(&buf[buf_len..]);

            if buf.len() >= FLUSH_BUF_THRESHOLD {
                // Flush the generated sequence to the next pipe.
                let rows_len = rc.values.len();
                br.set_result_columns(vec![std::mem::take(&mut rc)], rows_len);
                self.pp_next.write_block(0, &mut br);
                rc.name = b"_msg".to_vec();
                buf.clear();
            }
        }

        if !buf.is_empty() {
            let rows_len = rc.values.len();
            br.set_result_columns(vec![std::mem::take(&mut rc)], rows_len);
            self.pp_next.write_block(0, &mut br);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::{assert_needed_fields, assert_rows_eq, rows, run_pipe};

    // PORT NOTE: `TestParseGenerateSequenceSuccess` /
    // `TestParsePipeGenerateSequenceFailure` exercise the lexer-based
    // `parsePipeGenerateSequence`, which is deferred; they are omitted until the
    // LogsQL parser is ported.

    #[test]
    fn test_pipe_generate_sequence() {
        // non-empty input
        let p = new_pipe_generate_sequence(3);
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("foo", "bar"), ("bar", "abcde")]])),
            &rows(&[&[("_msg", "0")], &[("_msg", "1")], &[("_msg", "2")]]),
        );

        let p = new_pipe_generate_sequence(1);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[&[("foo", "bar"), ("bar", "abcde")], &[("x", "y")]]),
            ),
            &rows(&[&[("_msg", "0")]]),
        );

        // empty input
        let p = new_pipe_generate_sequence(3);
        assert_rows_eq(
            &run_pipe(&p, &rows(&[])),
            &rows(&[&[("_msg", "0")], &[("_msg", "1")], &[("_msg", "2")]]),
        );
    }

    #[test]
    fn test_pipe_generate_sequence_update_needed_fields() {
        // all the needed fields
        assert_needed_fields(&new_pipe_generate_sequence(12), "*", "", "", "");

        // all the needed fields, unneeded fields do not intersect with _msg
        assert_needed_fields(&new_pipe_generate_sequence(34), "*", "f1,f2", "", "");

        // all the needed fields, unneeded fields intersect with _msg
        assert_needed_fields(&new_pipe_generate_sequence(45), "*", "_msg,f1,f2", "", "");

        // needed fields do not intersect with _msg
        assert_needed_fields(&new_pipe_generate_sequence(1), "f1,f2", "", "", "");

        // needed fields intersect with _msg
        assert_needed_fields(&new_pipe_generate_sequence(2), "_msg,f1,f2", "", "", "");
    }
}

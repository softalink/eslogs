//! Port of `pipe_filter.go` from EsLogs v1.51.0.
//!
//! Implements the `| filter ...` (aka `| where ...`) pipe, which keeps only the
//! rows matching an embedded [`Filter`].

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::bitmap::Bitmap;
use crate::block_result::BlockResult;
use crate::filter::Filter;
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;

/// `PipeFilter` implements the `| filter ...` pipe.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#filter-pipe>
///
/// PORT NOTE: Go holds the filter as `filter` (an interface value). The Rust
/// port stores it as `Arc<dyn Filter>` so it can be shared cheaply into the
/// per-worker processor.
pub(crate) struct PipeFilter {
    /// Filter applied to the written rows.
    pub(crate) f: Arc<dyn Filter>,
}

/// Builds a `| filter ...` pipe wrapping the given filter.
///
/// PORT NOTE: `parsePipeFilter` is lexer-dependent and deferred; this
/// constructor exposes the parsed result for the future parser.
pub(crate) fn new_pipe_filter(f: Arc<dyn Filter>) -> PipeFilter {
    PipeFilter { f }
}

impl Pipe for PipeFilter {
    fn to_string(&self) -> String {
        format!("filter {}", self.f.to_string())
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        true
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        // The filter pipe does not change the set of fields.
        Some(crate::pipe::StatsTailOp::Keep)
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        self.f.update_needed_fields(pf);
    }

    /// Port of Go `pipeFilter.hasFilterInWithQuery`.
    fn has_filter_in_with_query(&self) -> bool {
        crate::storage_search::has_filter_in_with_query_for_filter(self.f.as_ref())
    }

    /// Port of Go `pipeFilter.initFilterInValues`.
    fn init_filter_in_values(
        &mut self,
        get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
        timestamp: i64,
    ) -> Result<(), String> {
        if let Some(f_new) = crate::storage_search::init_filter_in_values_for_shared_filter(
            &self.f, get_values, timestamp,
        )? {
            self.f = f_new;
        }
        Ok(())
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeFilterProcessorShard::default()))
            .collect();
        Arc::new(PipeFilterProcessor {
            f: Arc::clone(&self.f),
            pp_next,
            shards,
        })
    }
}

struct PipeFilterProcessor {
    f: Arc<dyn Filter>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeFilterProcessorShard>>,
}

#[derive(Default)]
struct PipeFilterProcessorShard {
    br: BlockResult,
    bm: Bitmap,
}

impl PipeProcessor for PipeFilterProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let mut guard = self.shards[worker_id].lock().unwrap();
        let shard = &mut *guard;

        shard.bm.init(br.rows_len());
        shard.bm.set_bits();
        self.f.apply_to_block_result(br, &mut shard.bm);
        if shard.bm.are_all_bits_set() {
            // Fast path - the filter didn't drop anything; forward br as is.
            drop(guard);
            self.pp_next.write_block(worker_id, br);
            return;
        }
        if shard.bm.is_zero() {
            // Nothing to send.
            return;
        }

        // Slow path - copy the remaining rows into shard.br before forwarding.
        shard.br.init_from_filter_all_columns(br, &shard.bm);
        self.pp_next.write_block(worker_id, &mut shard.br);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::pipe_fields::pipe_test_util::*;
    use super::*;
    use crate::filter_exact::new_filter_exact;
    use crate::filter_noop::new_filter_noop;

    // PORT NOTE: the Go result/needed-fields cases parse LogsQL filter
    // expressions (`filter foo`, `where x:foo y:bar`). The parser is deferred,
    // so these tests construct filters programmatically to exercise the pipe's
    // fast (all-match), zero (no-match) and slow (partial-match) paths and the
    // `update_needed_fields` delegation.

    #[test]
    fn test_pipe_filter_string() {
        let f = new_pipe_filter(Arc::new(new_filter_noop()));
        assert_eq!(f.to_string(), "filter *");
    }

    #[test]
    fn test_pipe_filter_match_all_fast_path() {
        let rows = vec![vec![field("a", "1")], vec![field("a", "2")]];
        let f = new_pipe_filter(Arc::new(new_filter_noop()));
        let got = run_pipe(&f, &rows);
        assert_rows_eq(got, &rows);
    }

    #[test]
    fn test_pipe_filter_no_match() {
        let rows = vec![vec![field("a", "1")], vec![field("a", "2")]];
        let f = new_pipe_filter(Arc::new(new_filter_exact("a", "zzz")));
        let got = run_pipe(&f, &rows);
        assert_rows_eq(got, &[]);
    }

    #[test]
    fn test_pipe_filter_partial_match_slow_path() {
        // One block of three rows; only the row with a=2 matches.
        let rows = vec![
            vec![field("a", "1"), field("b", "x")],
            vec![field("a", "2"), field("b", "y")],
            vec![field("a", "3"), field("b", "z")],
        ];
        let f = new_pipe_filter(Arc::new(new_filter_exact("a", "2")));
        let got = run_pipe(&f, &rows);
        assert_rows_eq(got, &[vec![field("a", "2"), field("b", "y")]]);
    }

    #[test]
    fn test_pipe_filter_update_needed_fields() {
        // A field-scoped filter contributes its field to the allow list.
        let f = new_pipe_filter(Arc::new(new_filter_exact("f1", "bar")));
        expect_needed_fields(&f, "*", "", "*", "");
        expect_needed_fields(&f, "f2,f3", "", "f1,f2,f3", "");
    }
}

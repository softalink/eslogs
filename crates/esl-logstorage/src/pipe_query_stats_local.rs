//! Port of `pipe_query_stats_local.go` — the cluster-local half of
//! `query_stats`. In a cluster the actual stats are gathered on the remote
//! storage nodes and passed to this pipe via a side channel; `write_block` is a
//! no-op and `flush` emits the single stats row.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::block_result::BlockResult;
use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_query_stats::write_query_stats_to_pipe_processor;
use crate::prefix_filter;
use crate::query_stats::QueryStats;

/// `pipeQueryStatsLocal` processes the local part of `pipeQueryStats` in a
/// cluster.
pub struct PipeQueryStatsLocal {}

/// Constructs a `query_stats_local` pipe.
///
/// This pipe is produced only by `pipeQueryStats.splitToRemoteAndLocal`
/// (the cluster split) and carries no parameters, so this constructor takes
/// none.
pub(crate) fn new_pipe_query_stats_local() -> PipeQueryStatsLocal {
    PipeQueryStatsLocal {}
}

impl Pipe for PipeQueryStatsLocal {
    /// Port of Go `pipeQueryStatsLocal.splitToRemoteAndLocal`: this pipe is only
    /// ever produced by a split, so splitting it again is a bug.
    fn split_to_remote_and_local(&self, _timestamp: i64) -> crate::pipe::SplitPipesResult {
        esl_common::panicf!("BUG: unexpected call for pipeQueryStatsLocal");
        unreachable!()
    }

    fn to_string(&self) -> String {
        "query_stats_local".to_string()
    }

    // Go: canLiveTail() == false, canReturnLastNResults() == false — both match
    // the trait defaults, so they are not overridden here.

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, _pf: &mut prefix_filter::Filter) {
        // Nothing to do.
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeQueryStatsLocalProcessor {
            pp_next,
            injection: Mutex::new(QueryStatsInjection::default()),
        })
    }
}

struct PipeQueryStatsLocalProcessor {
    pp_next: Arc<dyn PipeProcessor>,
    // `qs` / `query_duration_nsecs` must be set via `set_query_stats()` before
    // `flush()`.
    injection: Mutex<QueryStatsInjection>,
}

#[derive(Default)]
struct QueryStatsInjection {
    qs: Option<Arc<QueryStats>>,
    query_duration_nsecs: i64,
}

impl PipeQueryStatsLocalProcessor {
    /// Port of Go `(*pipeQueryStatsLocalProcessor).setQueryStats`.
    #[allow(dead_code)]
    pub(crate) fn set_query_stats(&self, qs: Arc<QueryStats>, query_duration_nsecs: i64) {
        let mut injection = self.injection.lock().unwrap();
        injection.qs = Some(qs);
        injection.query_duration_nsecs = query_duration_nsecs;
    }
}

impl PipeProcessor for PipeQueryStatsLocalProcessor {
    fn write_block(&self, _worker_id: usize, _br: &mut BlockResult) {
        // Nothing to do — query stats is passed from the remote storage nodes
        // via a side channel.
    }

    fn flush(&self) -> Result<(), String> {
        let injection = self.injection.lock().unwrap();
        let qs = injection
            .qs
            .as_ref()
            .expect("BUG: query stats must be set via set_query_stats() before flush()");
        write_query_stats_to_pipe_processor(
            qs,
            self.pp_next.as_ref(),
            injection.query_duration_nsecs,
        );
        Ok(())
    }
}

// PORT NOTE: upstream has no `pipe_query_stats_local_test.go`. The test below is
// port-added: it exercises the only real behaviour of this pipe — `flush`
// emitting the stats row via the injected `QueryStats` — mirroring the shared
// `writeToPipeProcessor` path validated for `pipe_query_stats`.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::{CollectProcessor, assert_rows_eq, rows};

    #[test]
    fn test_pipe_query_stats_local_flush_emits_zero_stats() {
        assert_eq!(
            new_pipe_query_stats_local().to_string(),
            "query_stats_local"
        );

        let collector = Arc::new(CollectProcessor::default());
        let processor = PipeQueryStatsLocalProcessor {
            pp_next: collector.clone(),
            injection: Mutex::new(QueryStatsInjection::default()),
        };
        processor.set_query_stats(Arc::new(QueryStats::default()), 0);
        // write_block is a no-op, so no input blocks are needed.
        processor.flush().unwrap();

        assert_rows_eq(
            &collector.rows(),
            &rows(&[&[
                ("BytesReadColumnsHeaders", "0"),
                ("BytesReadColumnsHeaderIndexes", "0"),
                ("BytesReadBloomFilters", "0"),
                ("BytesReadValues", "0"),
                ("BytesReadTimestamps", "0"),
                ("BytesReadBlockHeaders", "0"),
                ("BytesReadTotal", "0"),
                ("BlocksProcessed", "0"),
                ("RowsProcessed", "0"),
                ("RowsFound", "0"),
                ("ValuesRead", "0"),
                ("TimestampsRead", "0"),
                ("BytesProcessedUncompressedValues", "0"),
                ("QueryDurationNsecs", "0"),
            ]]),
        );
    }
}

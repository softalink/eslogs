//! Port of `pipe_query_stats.go` — the `| query_stats` pipe, which discards the
//! incoming rows and emits a single row describing query-execution statistics.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::block_result::{BlockResult, ResultColumn, append_result_column_with_name};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::query_stats::QueryStats;
use crate::values_encoder::marshal_uint64_string;

/// `pipeQueryStats` implements `| query_stats`.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#query_stats-pipe>.
pub struct PipeQueryStats {}

/// Constructs a `query_stats` pipe.
///
/// PORT NOTE: Go's `parsePipeQueryStats` is lexer-dependent and deferred; the
/// pipe carries no parameters, so this constructor takes none.
pub(crate) fn new_pipe_query_stats() -> PipeQueryStats {
    PipeQueryStats {}
}

impl Pipe for PipeQueryStats {
    /// Port of Go `pipeQueryStats.splitToRemoteAndLocal`: per-node query stats
    /// are merged locally by `query_stats_local`.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        let ps_local = crate::pipe_query_stats_local::new_pipe_query_stats_local();
        (
            Some(crate::pipe::clone_pipe(self, timestamp)),
            vec![Box::new(ps_local)],
        )
    }

    fn to_string(&self) -> String {
        "query_stats".to_string()
    }

    // Go: canLiveTail() == false, canReturnLastNResults() == false — both match
    // the trait defaults, so they are not overridden here.

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter("*");
    }

    // PORT NOTE: Go's `initFilterInValues` and `visitSubqueries` are no-ops
    // for this pipe; `splitToRemoteAndLocal` is implemented above.

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeQueryStatsProcessorShard::default()))
            .collect();
        Arc::new(PipeQueryStatsProcessor {
            pp_next,
            shards,
            injection: Mutex::new(QueryStatsInjection::default()),
        })
    }
}

struct PipeQueryStatsProcessor {
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeQueryStatsProcessorShard>>,
    // `qs` / `query_duration_nsecs` must be set via `set_query_stats()` before
    // `flush()`.
    injection: Mutex<QueryStatsInjection>,
}

#[derive(Default)]
struct QueryStatsInjection {
    qs: Option<Arc<QueryStats>>,
    query_duration_nsecs: i64,
}

#[derive(Default)]
struct PipeQueryStatsProcessorShard {
    // `sink` mirrors Go's field that prevents the compiler from eliminating the
    // value-reading loop inside `write_block`. It is written but never read.
    #[allow(dead_code)]
    sink: usize,
}

impl PipeQueryStatsProcessor {
    /// Port of Go `(*pipeQueryStatsProcessor).setQueryStats`.
    ///
    /// Must be called before [`flush`](PipeProcessor::flush); the shared
    /// `run_pipe` test harness does not call it, so behaviour is exercised via a
    /// dedicated test that constructs this processor directly.
    #[allow(dead_code)]
    pub(crate) fn set_query_stats(&self, qs: Arc<QueryStats>, query_duration_nsecs: i64) {
        let mut injection = self.injection.lock().unwrap();
        injection.qs = Some(qs);
        injection.query_duration_nsecs = query_duration_nsecs;
    }
}

impl PipeProcessor for PipeQueryStatsProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        // Read all the data from br in order to emulate the default behaviour
        // where this data would be returned to the client if there were no
        // query_stats pipe at the end of the query.
        let mut shard = self.shards[worker_id].lock().unwrap();
        let cs = br.get_columns();
        for c in cs {
            let values = br.column_get_values(c);
            shard.sink += values.len();
        }
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

/// Port of Go `(*QueryStats).writeToPipeProcessor`.
///
/// PORT NOTE: `query_stats.rs` defers `writeToPipeProcessor` (it needs the pipe
/// layer, which did not exist when that module was ported). Until it lands as a
/// method on `QueryStats`, this free function reproduces it using the
/// already-ported `QueryStats::add_entries`. It is shared with
/// `pipe_query_stats_local`.
pub(crate) fn write_query_stats_to_pipe_processor(
    qs: &QueryStats,
    pp_next: &dyn PipeProcessor,
    query_duration_nsecs: i64,
) {
    let mut rcs: Vec<ResultColumn> = Vec::new();
    qs.add_entries(
        |name, value| {
            let mut buf = Vec::new();
            marshal_uint64_string(&mut buf, value);
            append_result_column_with_name(&mut rcs, name);
            rcs.last_mut().unwrap().add_value(&buf);
        },
        query_duration_nsecs,
    );

    let mut br = BlockResult::default();
    br.set_result_columns(rcs, 1);
    pp_next.write_block(0, &mut br);
}

// PORT NOTE: `TestParseQueryStatsSuccess` / `TestParseQueryStatsFailure`
// exercise the deferred lexer-based `parsePipeQueryStats` and are omitted.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::{
        CollectProcessor, WORKERS_COUNT, assert_needed_fields, assert_rows_eq, rows, write_rows,
    };

    fn run_query_stats(input: &[Vec<crate::rows::Field>]) -> Vec<Vec<crate::rows::Field>> {
        let collector = Arc::new(CollectProcessor::default());
        // Construct the processor directly so we can call `set_query_stats`,
        // which the shared `run_pipe` harness does not do (matching the Go test
        // harness in pipe_utils_test.go, which calls `ps.setQueryStats`).
        let processor = PipeQueryStatsProcessor {
            pp_next: collector.clone(),
            shards: (0..WORKERS_COUNT)
                .map(|_| Mutex::new(PipeQueryStatsProcessorShard::default()))
                .collect(),
            injection: Mutex::new(QueryStatsInjection::default()),
        };
        processor.set_query_stats(Arc::new(QueryStats::default()), 0);
        write_rows(&processor, input);
        processor.flush().unwrap();
        collector.rows()
    }

    #[test]
    fn test_pipe_query_stats() {
        // The single stats row produced over an empty database, every counter at
        // zero. Field order is irrelevant — `assert_rows_eq` compares
        // order-independently, exactly like Go's `assertRowsEqual`.
        let expected = rows(&[&[
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
        ]]);

        // empty input
        assert_rows_eq(&run_query_stats(&rows(&[])), &expected);

        // non-empty input: the returned query stats is still empty because the
        // test harness does not store/read the rows from a database.
        let input = rows(&[
            &[("foo", "bar"), ("abc", "defaaa")],
            &[
                ("_msg", "qfdskj lj lkfdsjfds"),
                ("_time", "2025-08-30T10:20:30Z"),
            ],
        ]);
        assert_rows_eq(&run_query_stats(&input), &expected);
    }

    #[test]
    fn test_pipe_query_stats_update_needed_fields() {
        let p = new_pipe_query_stats();
        assert_needed_fields(&p, "*", "", "*", "");
        assert_needed_fields(&p, "*", "f1,f2", "*", "");
        assert_needed_fields(&p, "f1,f2", "", "*", "");
    }
}

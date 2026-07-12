//! Port of `pipe_blocks_count.go` — the `| blocks_count` pipe.
//!
//! Counts the number of blocks passed through the pipe and emits a single row
//! with the count in `result_name` (default `blocks_count`).
//!
//! PORT NOTE: the `splitToRemoteAndLocal` cluster path and the lexer-based
//! `parsePipeBlocksCount` are out of scope for the single-node port (see
//! `pipe.rs` module notes). A `pub(crate)` constructor is exposed for the parser
//! to build once the lexer lands.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::values_encoder::marshal_uint64_string;

/// The `| blocks_count [as name]` pipe.
pub struct PipeBlocksCount {
    /// Optional name of the column to write results to. Defaults to
    /// `blocks_count`.
    result_name: String,
}

/// Builds a [`PipeBlocksCount`] with the given result column name
/// (Go `parsePipeBlocksCount` result).
pub(crate) fn new_pipe_blocks_count(result_name: String) -> PipeBlocksCount {
    PipeBlocksCount { result_name }
}

impl Pipe for PipeBlocksCount {
    /// Port of Go `pipeBlocksCount.splitToRemoteAndLocal`: per-node block
    /// counts are summed locally.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        let result_name_quoted = crate::stream_filter::quote_token_if_needed(&self.result_name);

        let p_str = format!("stats sum({result_name_quoted}) as {result_name_quoted}");
        let p_local = crate::pipe::must_parse_pipe(&p_str, timestamp);

        (
            Some(crate::pipe::clone_pipe(self, timestamp)),
            vec![p_local],
        )
    }

    fn to_string(&self) -> String {
        let mut s = "blocks_count".to_string();
        if self.result_name != "blocks_count" {
            s += " as ";
            s += &self.result_name;
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.reset();
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeBlocksCountProcessor {
            result_name: self.result_name.clone(),
            stop,
            pp_next,
            shards: (0..concurrency.max(1)).map(|_| Mutex::new(0u64)).collect(),
        })
    }
}

struct PipeBlocksCountProcessor {
    result_name: String,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    // Per-worker block counters (Go's atomicutil.Slice[shard]). Each worker
    // touches only its own index, so the mutexes are uncontended.
    shards: Vec<Mutex<u64>>,
}

impl PipeProcessor for PipeBlocksCountProcessor {
    fn write_block(&self, worker_id: usize, _br: &mut BlockResult) {
        let idx = worker_id.min(self.shards.len() - 1);
        *self.shards[idx].lock().unwrap() += 1;
    }

    fn flush(&self) -> Result<(), String> {
        if self.stop.load(Ordering::SeqCst) {
            return Ok(());
        }

        let mut blocks_count: u64 = 0;
        for shard in &self.shards {
            blocks_count += *shard.lock().unwrap();
        }

        let mut value = Vec::new();
        marshal_uint64_string(&mut value, blocks_count);
        let rc = ResultColumn {
            name: self.result_name.clone(),
            values: vec![value],
        };

        let mut br = BlockResult::default();
        br.set_result_columns(vec![rc], 1);
        self.pp_next.write_block(0, &mut br);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;

    /// Test sink that records every row written to it, per Go's
    /// `expectPipeResults` collector.
    struct Collector {
        blocks: Mutex<Vec<Vec<Field>>>,
    }

    impl Collector {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                blocks: Mutex::new(Vec::new()),
            })
        }
    }

    impl PipeProcessor for Collector {
        fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
            let cols = br.get_columns();
            let names: Vec<String> = cols
                .iter()
                .map(|&c| br.column_name(c).to_string())
                .collect();
            let n = br.rows_len();
            let mut out = self.blocks.lock().unwrap();
            for i in 0..n {
                let mut fields = Vec::with_capacity(cols.len());
                for (ci, &c) in cols.iter().enumerate() {
                    let v = br.column_get_value_at_row(c, i).to_string();
                    fields.push(Field {
                        name: names[ci].clone(),
                        value: v,
                    });
                }
                out.push(fields);
            }
        }
        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    fn block_from_rows(rows: &[Vec<Field>]) -> BlockResult {
        let mut br = BlockResult::default();
        br.must_init_from_rows(rows);
        br
    }

    #[test]
    fn test_pipe_blocks_count_counts_blocks() {
        let pipe = new_pipe_blocks_count("blocks_count".to_string());
        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = pipe.new_pipe_processor(1, stop, sink.clone());

        // Push three blocks.
        for _ in 0..3 {
            let mut br = block_from_rows(&[vec![Field {
                name: "x".to_string(),
                value: "1".to_string(),
            }]]);
            pp.write_block(0, &mut br);
        }
        pp.flush().unwrap();

        let out = sink.blocks.lock().unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].len(), 1);
        assert_eq!(out[0][0].name, "blocks_count");
        assert_eq!(out[0][0].value, "3");
    }

    #[test]
    fn test_pipe_blocks_count_custom_name() {
        let pipe = new_pipe_blocks_count("cnt".to_string());
        assert_eq!(pipe.to_string(), "blocks_count as cnt");

        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = pipe.new_pipe_processor(2, stop, sink.clone());

        let mut br0 = block_from_rows(&[vec![Field {
            name: "x".to_string(),
            value: "1".to_string(),
        }]]);
        pp.write_block(0, &mut br0);
        let mut br1 = block_from_rows(&[vec![Field {
            name: "x".to_string(),
            value: "2".to_string(),
        }]]);
        pp.write_block(1, &mut br1);
        pp.flush().unwrap();

        let out = sink.blocks.lock().unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][0].name, "cnt");
        assert_eq!(out[0][0].value, "2");
    }
}

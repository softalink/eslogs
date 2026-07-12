//! Port of `pipe_block_stats.go` — the `| block_stats` pipe.
//!
//! For every column of every input block it emits one row describing that
//! column's on-disk footprint: `field`, `type`, `values_bytes`, `bloom_bytes`,
//! `dict_items`, `dict_bytes`, `rows`, `_stream`, `part_path`.
//!
//! # PORT NOTE — only the in-memory path is reachable
//! Go inspects `br.bs` (the block search: `getStreamStr`, `partPath`,
//! `getColumnHeader().valuesSize/bloomFilterSize/valuesDict`,
//! `timestampsHeader.blockSize`) to report the persisted-block statistics. In
//! this Rust port `BlockResult` keeps the block search behind a private,
//! type-erased pointer with no public accessor, and exposes no column-header /
//! dict / bloom internals. So only Go's `br.bs == nil` (in-memory) branch is
//! ported here: `_stream` is `"{}"`, `part_path` is `"inmemory"`, and per
//! column the type is `const` / `time` / `inmemory` with the size counters left
//! at zero (const rows report the const value's byte length, matching Go). The
//! on-disk column statistics are deferred until `BlockResult` exposes the
//! block-search internals.
//!
//! PORT NOTE: Go flushes the writer every 500 rows; here each input block emits
//! exactly one output block (column count per block is tiny), which is
//! behaviorally equivalent for the next pipe.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::values_encoder::marshal_uint64_string;

/// The `| block_stats` pipe.
pub struct PipeBlockStats {}

/// Builds a [`PipeBlockStats`] (Go `parsePipeBlockStats` result).
pub(crate) fn new_pipe_block_stats() -> PipeBlockStats {
    PipeBlockStats {}
}

impl Pipe for PipeBlockStats {
    /// Port of Go `pipeBlockStats.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        "block_stats".to_string()
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filter("*");
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeBlockStatsProcessor { pp_next })
    }
}

struct PipeBlockStatsProcessor {
    pp_next: Arc<dyn PipeProcessor>,
}

/// Column order emitted by `block_stats` (Go `pipeBlockStatsWriteContext`).
const COLUMN_NAMES: [&str; 9] = [
    "field",
    "type",
    "values_bytes",
    "bloom_bytes",
    "dict_items",
    "dict_bytes",
    "rows",
    "_stream",
    "part_path",
];

struct RowValues {
    column_name: String,
    column_type: &'static str,
    values_size: u64,
    bloom_size: u64,
    dict_items: u64,
    dict_size: u64,
}

impl PipeProcessor for PipeBlockStatsProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        // Only the in-memory path is reachable (see module PORT NOTE).
        let stream = "{}";
        let part_path = "inmemory";
        let rows_len = br.rows_len() as u64;

        let cols = br.get_columns();
        let mut rows: Vec<RowValues> = Vec::with_capacity(cols.len());
        for &c in &cols {
            let name = br.column_name(c).to_string();
            if br.column_is_const(c) {
                let values_size = br.column_get_value_at_row(c, 0).len() as u64;
                rows.push(RowValues {
                    column_name: name,
                    column_type: "const",
                    values_size,
                    bloom_size: 0,
                    dict_items: 0,
                    dict_size: 0,
                });
            } else if br.column_is_time(c) {
                rows.push(RowValues {
                    column_name: name,
                    column_type: "time",
                    values_size: 0,
                    bloom_size: 0,
                    dict_items: 0,
                    dict_size: 0,
                });
            } else {
                rows.push(RowValues {
                    column_name: name,
                    column_type: "inmemory",
                    values_size: 0,
                    bloom_size: 0,
                    dict_items: 0,
                    dict_size: 0,
                });
            }
        }

        let mut rcs: Vec<ResultColumn> = COLUMN_NAMES
            .iter()
            .map(|name| ResultColumn {
                name: name.to_string(),
                values: Vec::new(),
            })
            .collect();

        for r in &rows {
            rcs[0].add_value(r.column_name.as_bytes());
            rcs[1].add_value(r.column_type.as_bytes());
            add_uint64(&mut rcs[2], r.values_size);
            add_uint64(&mut rcs[3], r.bloom_size);
            add_uint64(&mut rcs[4], r.dict_items);
            add_uint64(&mut rcs[5], r.dict_size);
            add_uint64(&mut rcs[6], rows_len);
            rcs[7].add_value(stream.as_bytes());
            rcs[8].add_value(part_path.as_bytes());
        }

        let rows_count = rows.len();
        let mut out = BlockResult::default();
        out.set_result_columns(rcs, rows_count);
        self.pp_next.write_block(worker_id, &mut out);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

fn add_uint64(rc: &mut ResultColumn, n: u64) {
    let mut b = Vec::new();
    marshal_uint64_string(&mut b, n);
    rc.add_value(&b);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;
    use std::sync::Mutex;

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
                    fields.push(Field {
                        name: names[ci].clone(),
                        value: br.column_get_value_at_row(c, i).to_string(),
                    });
                }
                out.push(fields);
            }
        }
        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn get<'a>(row: &'a [Field], name: &str) -> &'a str {
        row.iter()
            .find(|f| f.name == name)
            .map(|f| f.value.as_str())
            .unwrap_or("")
    }

    #[test]
    fn test_block_stats_const_and_varying() {
        // Column "k" is constant across rows (→ const); column "v" varies
        // (→ inmemory).
        let pipe = new_pipe_block_stats();
        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = pipe.new_pipe_processor(1, stop, sink.clone());

        let mut br = BlockResult::default();
        br.must_init_from_rows(&[
            vec![field("k", "same"), field("v", "1")],
            vec![field("k", "same"), field("v", "2")],
            vec![field("k", "same"), field("v", "3")],
        ]);
        pp.write_block(0, &mut br);
        pp.flush().unwrap();

        let out = sink.blocks.lock().unwrap();
        // One output row per input column.
        assert_eq!(out.len(), 2);
        for r in out.iter() {
            assert_eq!(get(r, "rows"), "3");
            assert_eq!(get(r, "_stream"), "{}");
            assert_eq!(get(r, "part_path"), "inmemory");
            match get(r, "field") {
                "k" => {
                    assert_eq!(get(r, "type"), "const");
                    // const value "same" is 4 bytes.
                    assert_eq!(get(r, "values_bytes"), "4");
                }
                "v" => {
                    assert_eq!(get(r, "type"), "inmemory");
                    assert_eq!(get(r, "values_bytes"), "0");
                }
                other => panic!("unexpected field {other}"),
            }
            assert_eq!(get(r, "bloom_bytes"), "0");
            assert_eq!(get(r, "dict_items"), "0");
            assert_eq!(get(r, "dict_bytes"), "0");
        }
    }

    #[test]
    fn test_block_stats_empty_block_emits_nothing() {
        let pipe = new_pipe_block_stats();
        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = pipe.new_pipe_processor(1, stop, sink.clone());

        let mut br = BlockResult::default();
        br.must_init_from_rows(&[]);
        pp.write_block(0, &mut br);
        pp.flush().unwrap();

        assert!(sink.blocks.lock().unwrap().is_empty());
    }

    #[test]
    fn test_block_stats_to_string() {
        assert_eq!(new_pipe_block_stats().to_string(), "block_stats");
    }
}

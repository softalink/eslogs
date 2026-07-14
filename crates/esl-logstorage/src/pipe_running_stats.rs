//! Port of `pipe_running_stats.go` — the `| running_stats ...` /
//! `| total_stats ...` pipe, plus the `runningStatsFunc` /
//! `runningStatsProcessor` interfaces it defines.
//!
//! The two interfaces are extracted here as [`RunningStatsFunc`] and
//! [`RunningStatsProcessor`]; the six `running_stats_*` functions
//! (`count/sum/min/max/first/last`) `impl` them (their inherent
//! `update_running_stats`/`get_running_stats`/`new_running_stats_processor`
//! methods were built to this spec — the trait impls just forward to them).
//!
//! # PORT NOTES
//! * `splitToRemoteAndLocal` and cluster paths are omitted (single-node).
//! * `stateSizeBudget` accounting is dropped — the processor owns its collected
//!   rows directly (see `stats.rs` allocator PORT NOTE).
//! * A processor is created per stats func and needs the func's parameters. The
//!   frozen inherent methods take the concrete func type, so the trait method
//!   takes `&dyn RunningStatsFunc` and each impl downcasts via [`Any`]
//!   (mirroring the `StatsProcessor::merge_state` downcast pattern in
//!   `stats.rs`). This keeps the six function files unchanged apart from the
//!   forwarding trait impls.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use esl_common::encoding::marshal_bytes;

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::rows::{Field, get_field_value_by_name};

/// A running-stats function such as `count(...)`, `sum(x)`, `first(x)`.
///
/// Port of Go's unexported `runningStatsFunc` interface.
pub trait RunningStatsFunc: std::fmt::Display + Send + Sync {
    /// Updates `pf` with the fields needed to compute this function
    /// (Go `updateNeededFields`).
    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter);

    /// Creates a fresh processor for accumulating this function's running stats
    /// (Go `newRunningStatsProcessor`).
    fn new_running_stats_processor(&self) -> Box<dyn RunningStatsProcessor>;

    /// Returns `self` as `&dyn Any` so a processor can downcast the passed-in
    /// func to its concrete type. Every impl is `fn as_any(&self) -> &dyn Any { self }`.
    fn as_any(&self) -> &dyn Any;
}

/// Accumulates running state for one [`RunningStatsFunc`] over an ordered row
/// stream. Port of Go's unexported `runningStatsProcessor` interface.
pub trait RunningStatsProcessor: Send {
    /// Updates stats for the given row (Go `updateRunningStats`). `sf` is the
    /// same func that produced this processor; recover its type via
    /// `sf.as_any().downcast_ref::<...>()`.
    fn update_running_stats(&mut self, sf: &dyn RunningStatsFunc, row: &[Field]);

    /// Returns the current running-stats value (Go `getRunningStats`).
    fn get_running_stats(&self) -> Vec<u8>;
}

/// A running-stats function to execute and the name of its output field.
pub struct PipeRunningStatsFunc {
    f: Box<dyn RunningStatsFunc>,
    result_name: String,
}

/// Builds a [`PipeRunningStatsFunc`].
pub(crate) fn new_pipe_running_stats_func(
    f: Box<dyn RunningStatsFunc>,
    result_name: String,
) -> PipeRunningStatsFunc {
    PipeRunningStatsFunc { f, result_name }
}

/// The `| running_stats ...` / `| total_stats ...` pipe.
pub struct PipeRunningStats {
    /// When set, compute total stats (aka `total_stats`) rather than running
    /// stats.
    is_total: bool,
    by_fields: Arc<Vec<String>>,
    funcs: Arc<Vec<PipeRunningStatsFunc>>,
}

/// Builds a [`PipeRunningStats`] (Go `parsePipeRunningStatsExt` result).
pub(crate) fn new_pipe_running_stats(
    is_total: bool,
    by_fields: Vec<String>,
    funcs: Vec<PipeRunningStatsFunc>,
) -> PipeRunningStats {
    PipeRunningStats {
        is_total,
        by_fields: Arc::new(by_fields),
        funcs: Arc::new(funcs),
    }
}

impl Pipe for PipeRunningStats {
    /// Port of Go `pipeRunningStats.splitToRemoteAndLocal`: the pipe (and
    /// everything after it) runs locally only.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (None, vec![crate::pipe::clone_pipe(self, timestamp)])
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        Some(crate::pipe::StatsTailOp::RunningStats {
            by_fields: self.by_fields.as_ref().clone(),
            is_total: self.is_total,
            result_names: self.funcs.iter().map(|f| f.result_name.clone()).collect(),
        })
    }

    fn to_string(&self) -> String {
        let mut s = if self.is_total {
            "total_stats".to_string()
        } else {
            "running_stats".to_string()
        };
        if !self.by_fields.is_empty() {
            s += " by (";
            s += &self.by_fields.join(", ");
            s += ")";
        }
        let a: Vec<String> = self
            .funcs
            .iter()
            .map(|f| format!("{} as {}", f.f, f.result_name))
            .collect();
        s += " ";
        s += &a.join(", ");
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        let pf_orig = pf.clone();
        for f in self.funcs.iter() {
            pf.add_deny_filter(&f.result_name);
            if pf_orig.match_string(&f.result_name) {
                f.f.update_needed_fields(pf);
            }
        }
        for bf in self.by_fields.iter() {
            pf.add_allow_filter(bf);
        }
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let n = concurrency.max(1);
        Arc::new(PipeRunningStatsProcessor {
            is_total: self.is_total,
            by_fields: self.by_fields.clone(),
            funcs: self.funcs.clone(),
            stop,
            pp_next,
            shards: (0..n).map(|_| Mutex::new(Vec::new())).collect(),
        })
    }
}

struct PipeRunningStatsProcessor {
    is_total: bool,
    by_fields: Arc<Vec<String>>,
    funcs: Arc<Vec<PipeRunningStatsFunc>>,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    // Per-worker collected rows (Go's atomicutil.Slice[shard].rows).
    shards: Vec<Mutex<Vec<Vec<Field>>>>,
}

impl PipeProcessor for PipeRunningStatsProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }
        let cols = br.get_columns();
        let names: Vec<Vec<u8>> = cols.iter().map(|&c| br.column_name(c).to_vec()).collect();
        let rows_len = br.rows_len();

        let idx = worker_id.min(self.shards.len() - 1);
        let mut shard = self.shards[idx].lock().unwrap();
        for i in 0..rows_len {
            let mut fields = Vec::with_capacity(cols.len());
            for (ci, &c) in cols.iter().enumerate() {
                fields.push(Field {
                    name: names[ci].clone(),
                    value: br.column_get_value_at_row(c, i).to_vec(),
                });
            }
            shard.push(fields);
        }
    }

    fn flush(&self) -> Result<(), String> {
        let get_key = |row: &[Field]| -> Vec<u8> {
            let mut key = Vec::new();
            for bf in self.by_fields.iter() {
                let v = get_field_value_by_name(row, bf);
                marshal_bytes(&mut key, v);
            }
            key
        };

        // key -> Vec<(timestamp, fields)>
        type GroupedRows = Vec<(Vec<u8>, Vec<Field>)>;
        let mut m: HashMap<Vec<u8>, GroupedRows> = HashMap::new();
        for shard in &self.shards {
            let mut rows = shard.lock().unwrap();
            for row in rows.drain(..) {
                if self.stop.load(Ordering::SeqCst) {
                    return Ok(());
                }
                let key = get_key(&row);
                let timestamp = get_field_value_by_name(&row, "_time").to_vec();
                m.entry(key).or_default().push((timestamp, row));
            }
        }

        let mut keys: Vec<Vec<u8>> = m.keys().cloned().collect();
        keys.sort();

        let mut wctx = RunningStatsWriter::new(self.pp_next.clone());
        let funcs = &self.funcs;
        for key in keys {
            let mut rows = m.remove(&key).unwrap();
            rows.sort_by(|a, b| a.0.cmp(&b.0));

            if self.stop.load(Ordering::SeqCst) {
                return Ok(());
            }

            let mut sps: Vec<Box<dyn RunningStatsProcessor>> = funcs
                .iter()
                .map(|f| f.f.new_running_stats_processor())
                .collect();

            if self.is_total {
                for (_, fields) in &rows {
                    for (i, sp) in sps.iter_mut().enumerate() {
                        sp.update_running_stats(funcs[i].f.as_ref(), fields);
                    }
                }
            }

            for (_, fields) in &rows {
                let mut out_fields = fields.clone();
                for (i, sp) in sps.iter_mut().enumerate() {
                    if !self.is_total {
                        sp.update_running_stats(funcs[i].f.as_ref(), fields);
                    }
                    let result = sp.get_running_stats();
                    out_fields.push(Field {
                        name: funcs[i].result_name.clone().into_bytes(),
                        value: result,
                    });
                }
                wctx.write_row(&out_fields);
            }
        }
        wctx.flush();
        Ok(())
    }
}

/// Writes rows to the next pipe, rebuilding result columns whenever the row's
/// set of field names changes (Go `pipeRunningStatsWriter`).
struct RunningStatsWriter {
    pp_next: Arc<dyn PipeProcessor>,
    rcs: Vec<ResultColumn>,
    rows_count: usize,
    values_len: usize,
}

impl RunningStatsWriter {
    fn new(pp_next: Arc<dyn PipeProcessor>) -> Self {
        Self {
            pp_next,
            rcs: Vec::new(),
            rows_count: 0,
            values_len: 0,
        }
    }

    fn write_row(&mut self, row: &[Field]) {
        let equal = self.rcs.len() == row.len()
            && self.rcs.iter().zip(row).all(|(rc, f)| rc.name == f.name);
        if !equal {
            self.flush();
            self.rcs = row
                .iter()
                .map(|f| ResultColumn {
                    name: f.name.clone(),
                    values: Vec::new(),
                })
                .collect();
        }
        for (i, f) in row.iter().enumerate() {
            self.rcs[i].add_value(&f.value);
            self.values_len += f.value.len();
        }
        self.rows_count += 1;
        if self.values_len >= 64_000 {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.rows_count == 0 {
            return;
        }
        let mut br = BlockResult::default();
        br.set_result_columns(self.rcs.clone(), self.rows_count);
        self.values_len = 0;
        self.rows_count = 0;
        self.pp_next.write_block(0, &mut br);
        for rc in &mut self.rcs {
            rc.reset_values();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::running_stats_count::new_running_stats_count;
    use crate::running_stats_sum::new_running_stats_sum;

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
            let names: Vec<Vec<u8>> = cols.iter().map(|&c| br.column_name(c).to_vec()).collect();
            let n = br.rows_len();
            let mut out = self.blocks.lock().unwrap();
            for i in 0..n {
                let mut fields = Vec::with_capacity(cols.len());
                for (ci, &c) in cols.iter().enumerate() {
                    fields.push(Field {
                        name: names[ci].clone(),
                        value: br.column_get_value_at_row(c, i).to_vec(),
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
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn field_value<'a>(row: &'a [Field], name: &str) -> &'a str {
        std::str::from_utf8(get_field_value_by_name(row, name)).unwrap()
    }

    #[test]
    fn test_running_stats_count() {
        let f = new_pipe_running_stats_func(
            Box::new(new_running_stats_count(vec!["*".to_string()])),
            "rc".to_string(),
        );
        let ps = new_pipe_running_stats(false, vec![], vec![f]);
        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = ps.new_pipe_processor(1, stop, sink.clone());

        let mut br = BlockResult::default();
        br.must_init_from_rows(&[
            vec![field("_time", "1"), field("x", "a")],
            vec![field("_time", "2"), field("x", "b")],
            vec![field("_time", "3"), field("x", "c")],
        ]);
        pp.write_block(0, &mut br);
        pp.flush().unwrap();

        let out = sink.blocks.lock().unwrap();
        assert_eq!(out.len(), 3);
        // running count grows with each ordered row.
        let mut rcs: Vec<&str> = out.iter().map(|r| field_value(r, "rc")).collect();
        rcs.sort();
        assert_eq!(rcs, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_total_stats_sum_by_field() {
        let f = new_pipe_running_stats_func(
            Box::new(new_running_stats_sum(vec!["v".to_string()])),
            "tot".to_string(),
        );
        let ps = new_pipe_running_stats(true, vec!["g".to_string()], vec![f]);
        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = ps.new_pipe_processor(1, stop, sink.clone());

        let mut br = BlockResult::default();
        br.must_init_from_rows(&[
            vec![field("_time", "1"), field("g", "a"), field("v", "10")],
            vec![field("_time", "2"), field("g", "a"), field("v", "5")],
            vec![field("_time", "1"), field("g", "b"), field("v", "7")],
        ]);
        pp.write_block(0, &mut br);
        pp.flush().unwrap();

        let out = sink.blocks.lock().unwrap();
        // total_stats: every row in group "a" gets 15, group "b" gets 7.
        for r in out.iter() {
            let g = field_value(r, "g");
            let tot = field_value(r, "tot");
            if g == "a" {
                assert_eq!(tot, "15");
            } else {
                assert_eq!(tot, "7");
            }
        }
        assert_eq!(out.len(), 3);
    }
}

//! Port of `pipe_drop_empty_fields.go` — the `| drop_empty_fields` pipe, which
//! rebuilds every block dropping the fields whose value is empty for a given
//! row (and dropping rows that end up with no non-empty fields).

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::block_result::{BlockResult, ResultColumn, append_result_column_with_name};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::rows::Field;

/// `pipeDropEmptyFields` implements `| drop_empty_fields`.
pub struct PipeDropEmptyFields {}

/// Constructs a `drop_empty_fields` pipe.
///
/// PORT NOTE: Go's `parsePipeDropEmptyFields` is lexer-dependent and deferred;
/// the pipe carries no parameters, so this constructor takes none.
pub(crate) fn new_pipe_drop_empty_fields() -> PipeDropEmptyFields {
    PipeDropEmptyFields {}
}

impl Pipe for PipeDropEmptyFields {
    /// Port of Go `pipeDropEmptyFields.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        "drop_empty_fields".to_string()
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, _pf: &mut prefix_filter::Filter) {
        // nothing to do
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeDropEmptyFieldsProcessorShard::default()))
            .collect();
        Arc::new(PipeDropEmptyFieldsProcessor { pp_next, shards })
    }
}

struct PipeDropEmptyFieldsProcessor {
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeDropEmptyFieldsProcessorShard>>,
}

#[derive(Default)]
struct PipeDropEmptyFieldsProcessorShard {
    column_values: Vec<Vec<Vec<u8>>>,
    fields: Vec<Field>,
    wctx: PipeDropEmptyFieldsWriteContext,
}

impl PipeProcessor for PipeDropEmptyFieldsProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let mut shard = self.shards[worker_id].lock().unwrap();

        let cs = br.get_columns();
        let rows_len = br.rows_len();
        let names: Vec<String> = cs.iter().map(|&c| br.column_name(c).to_string()).collect();

        shard.column_values.clear();
        for &c in &cs {
            let values = br.column_get_values(c).to_vec();
            shard.column_values.push(values);
        }

        if !has_empty_values(&shard.column_values) {
            // Fast path - just write br to ppNext, since it has no empty values.
            drop(shard);
            self.pp_next.write_block(worker_id, br);
            return;
        }

        // Slow path - drop fields with empty values.
        let shard = &mut *shard;
        let PipeDropEmptyFieldsProcessorShard {
            column_values,
            fields,
            wctx,
        } = shard;

        wctx.init(worker_id, self.pp_next.clone());

        for row_idx in 0..rows_len {
            fields.clear();
            for (i, values) in column_values.iter().enumerate() {
                let v = &values[row_idx];
                if v.is_empty() {
                    continue;
                }
                fields.push(Field {
                    name: names[i].clone(),
                    value: v.clone(),
                });
            }
            wctx.write_row(fields.as_slice());
        }

        wctx.flush();
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Default)]
struct PipeDropEmptyFieldsWriteContext {
    worker_id: usize,
    pp_next: Option<Arc<dyn PipeProcessor>>,

    rcs: Vec<ResultColumn>,
    br: BlockResult,

    /// Number of rows in the current block.
    rows_count: usize,

    /// Total length of values in the current block.
    values_len: usize,
}

impl PipeDropEmptyFieldsWriteContext {
    fn reset(&mut self) {
        self.worker_id = 0;
        self.pp_next = None;

        for rc in &mut self.rcs {
            rc.reset();
        }
        self.rcs.clear();

        self.rows_count = 0;
        self.values_len = 0;
    }

    fn init(&mut self, worker_id: usize, pp_next: Arc<dyn PipeProcessor>) {
        self.reset();

        self.worker_id = worker_id;
        self.pp_next = Some(pp_next);
    }

    fn write_row(&mut self, fields: &[Field]) {
        if fields.is_empty() {
            // skip rows without non-empty fields
            return;
        }

        let mut are_equal_columns = self.rcs.len() == fields.len();
        if are_equal_columns {
            for (i, f) in fields.iter().enumerate() {
                if self.rcs[i].name != f.name {
                    are_equal_columns = false;
                    break;
                }
            }
        }
        if !are_equal_columns {
            // send the current block to ppNext and construct a block with new set of columns
            self.flush();

            self.rcs.clear();
            for f in fields {
                append_result_column_with_name(&mut self.rcs, &f.name);
            }
        }

        for (i, f) in fields.iter().enumerate() {
            let v = &f.value;
            self.rcs[i].add_value(v);
            self.values_len += v.len();
        }

        self.rows_count += 1;
        if self.values_len >= 1_000_000 {
            self.flush();
        }
    }

    fn flush(&mut self) {
        self.values_len = 0;

        // Flush rcs to ppNext.
        let rcs = std::mem::take(&mut self.rcs);
        // PORT NOTE: Go's setResultColumns reads rcs without consuming them, then
        // resetValues() reuses the same column buffers. Rust's set_result_columns
        // takes ownership, so we record the names and rebuild empty columns after
        // the flush — behaviorally identical to Go's resetValues().
        let names: Vec<String> = rcs.iter().map(|rc| rc.name.clone()).collect();
        let rows_count = self.rows_count;
        self.br.set_result_columns(rcs, rows_count);
        self.rows_count = 0;

        let pp_next = self
            .pp_next
            .clone()
            .expect("BUG: write context is not initialized");
        pp_next.write_block(self.worker_id, &mut self.br);
        self.br.reset();

        for name in &names {
            append_result_column_with_name(&mut self.rcs, name);
        }
    }
}

fn has_empty_values(column_values: &[Vec<Vec<u8>>]) -> bool {
    for values in column_values {
        if values.iter().any(|v| v.is_empty()) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::{assert_needed_fields, assert_rows_eq, rows, run_pipe};

    // PORT NOTE: `TestParsePipeDropEmptyFieldsSuccess` /
    // `TestParsePipeDropEmptyFieldsFailure` exercise the lexer-based
    // `parsePipeDropEmptyFields`, which is deferred; they are omitted until the
    // LogsQL parser is ported.

    #[test]
    fn test_pipe_drop_empty_fields() {
        let p = new_pipe_drop_empty_fields();
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("a", "foo"), ("b", "bar"), ("c", "baz")]])),
            &rows(&[&[("a", "foo"), ("b", "bar"), ("c", "baz")]]),
        );

        let p = new_pipe_drop_empty_fields();
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("a", "foo"), ("b", "bar"), ("c", "baz")],
                    &[("a", "foo1"), ("b", ""), ("c", "baz1")],
                    &[("a", ""), ("b", "bar2")],
                    &[("a", ""), ("b", ""), ("c", "")],
                ]),
            ),
            &rows(&[
                &[("a", "foo"), ("b", "bar"), ("c", "baz")],
                &[("a", "foo1"), ("c", "baz1")],
                &[("b", "bar2")],
            ]),
        );
    }

    #[test]
    fn test_pipe_drop_empty_fields_update_needed_fields() {
        // all the needed fields
        let p = new_pipe_drop_empty_fields();
        assert_needed_fields(&p, "*", "", "*", "");

        // non-empty unneeded fields
        let p = new_pipe_drop_empty_fields();
        assert_needed_fields(&p, "*", "f1,f2", "*", "f1,f2");

        // non-empty needed fields
        let p = new_pipe_drop_empty_fields();
        assert_needed_fields(&p, "x,y", "", "x,y", "");
    }
}

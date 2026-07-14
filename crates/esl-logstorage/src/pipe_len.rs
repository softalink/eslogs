//! Port of `pipe_len.go` from EsLogs v1.51.0.
//!
//! Implements the `| len(field) as result` pipe, which writes the byte length
//! of `field` into `result` for every row.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::block_result::{BlockResult, ResultColumn};
use crate::filter_generic::is_msg_field_name;
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;
use crate::values_encoder::{marshal_float64, marshal_uint64_string};

/// `PipeLen` implements the `| len(...)` pipe.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#len-pipe>
pub(crate) struct PipeLen {
    pub(crate) field_name: String,
    pub(crate) result_field: String,
}

/// Builds a `| len(field) as result` pipe.
///
/// PORT NOTE: `parsePipeLen` is lexer-dependent and deferred; this constructor
/// exposes the parsed result for the future parser.
pub(crate) fn new_pipe_len(field_name: String, result_field: String) -> PipeLen {
    PipeLen {
        field_name,
        result_field,
    }
}

impl Pipe for PipeLen {
    /// Port of Go `pipeLen.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        let mut s = format!("len({})", quote_token_if_needed(&self.field_name));
        if !is_msg_field_name(&self.result_field) {
            s += &format!(" as {}", quote_token_if_needed(&self.result_field));
        }
        s
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        self.result_field != "_time"
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        if pf.match_string(&self.result_field) {
            pf.add_deny_filter(&self.result_field);
            pf.add_allow_filter(&self.field_name);
        }
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeLenProcessorShard::new()))
            .collect();
        Arc::new(PipeLenProcessor {
            field_name: self.field_name.clone(),
            result_field: self.result_field.clone(),
            pp_next,
            shards,
        })
    }
}

struct PipeLenProcessor {
    field_name: String,
    result_field: String,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeLenProcessorShard>>,
}

// PORT NOTE: Go's shard threads a pooled `arena` for the encoded length bytes
// referenced by `resultColumn.addValue` (which stores an unsafe string). The
// Rust `ResultColumn::add_value` copies the bytes, so the arena is unnecessary
// and is dropped here (no observable behavior change per CONVENTIONS).
struct PipeLenProcessorShard {
    rc: ResultColumn,
    min_value: f64,
    max_value: f64,
}

impl PipeLenProcessorShard {
    fn new() -> Self {
        Self {
            rc: ResultColumn::default(),
            min_value: f64::NAN,
            max_value: f64::NAN,
        }
    }

    fn reset(&mut self) {
        self.rc.reset();
        self.min_value = f64::NAN;
        self.max_value = f64::NAN;
    }

    /// Returns the `marshalFloat64`-encoded byte length of `v` and updates the
    /// tracked min/max lengths.
    fn get_encoded_len(&mut self, v: &[u8]) -> Vec<u8> {
        let f = v.len() as f64;

        if self.min_value.is_nan() {
            self.min_value = f;
            self.max_value = f;
        } else if f < self.min_value {
            self.min_value = f;
        } else if f > self.max_value {
            self.max_value = f;
        }

        let mut buf = Vec::new();
        marshal_float64(&mut buf, f);
        buf
    }
}

impl PipeProcessor for PipeLenProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let mut shard = self.shards[worker_id].lock().unwrap();
        shard.rc.name = self.result_field.clone().into_bytes();

        let c = br.get_column_by_name(&self.field_name);
        if br.column_is_const(c) {
            // Fast path for const column.
            let v = br.column_get_value_at_row(c, 0).to_vec();
            let mut buf = Vec::new();
            marshal_uint64_string(&mut buf, v.len() as u64);
            br.add_const_column(&self.result_field, &buf);
        } else {
            // Slow path for other columns.
            let values = br.column_get_values(c).to_vec();
            let mut v_encoded: Vec<u8> = Vec::new();
            for (row_idx, val) in values.iter().enumerate() {
                if row_idx == 0 || values[row_idx] != values[row_idx - 1] {
                    v_encoded = shard.get_encoded_len(val);
                }
                shard.rc.add_value(&v_encoded);
            }
            let rc = std::mem::take(&mut shard.rc);
            br.add_result_column_float64(rc, shard.min_value, shard.max_value);
        }

        // Write the result to ppNext.
        self.pp_next.write_block(worker_id, br);

        shard.reset();
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::pipe_fields::pipe_test_util::*;
    use super::*;

    fn pl(field_name: &str, result_field: &str) -> PipeLen {
        new_pipe_len(field_name.to_string(), result_field.to_string())
    }

    #[test]
    fn test_pipe_len_string() {
        assert_eq!(pl("foo", "_msg").to_string(), "len(foo)");
        assert_eq!(pl("foo", "bar").to_string(), "len(foo) as bar");
    }

    #[test]
    fn test_pipe_len_const_columns() {
        let rows = vec![
            vec![field("foo", "abcde"), field("baz", "1234567890")],
            vec![field("foo", "abc"), field("bar", "de")],
            vec![field("baz", "xyz")],
        ];
        let got = run_pipe(&pl("foo", "x"), &rows);
        assert_rows_eq(
            got,
            &[
                vec![
                    field("foo", "abcde"),
                    field("baz", "1234567890"),
                    field("x", "5"),
                ],
                vec![field("foo", "abc"), field("bar", "de"), field("x", "3")],
                vec![field("baz", "xyz"), field("x", "0")],
            ],
        );
    }

    #[test]
    fn test_pipe_len_multi_row_block() {
        // A single block with multiple non-const values exercises the slow path.
        let rows = vec![
            vec![field("foo", "a")],
            vec![field("foo", "abcd")],
            vec![field("foo", "abcdefg")],
        ];
        let got = run_pipe(&pl("foo", "x"), &rows);
        assert_rows_eq(
            got,
            &[
                vec![field("foo", "a"), field("x", "1")],
                vec![field("foo", "abcd"), field("x", "4")],
                vec![field("foo", "abcdefg"), field("x", "7")],
            ],
        );
    }

    #[test]
    fn test_pipe_len_update_needed_fields() {
        // all the needed fields
        expect_needed_fields(&pl("y", "x"), "*", "", "*", "x");
        expect_needed_fields(&pl("x", "x"), "*", "", "*", "");

        // unneeded fields do not intersect with output field
        expect_needed_fields(&pl("y", "x"), "*", "f1,f2", "*", "f1,f2,x");
        expect_needed_fields(&pl("x", "x"), "*", "f1,f2", "*", "f1,f2");

        // unneeded fields intersect with output field
        expect_needed_fields(&pl("z", "x"), "*", "x,y", "*", "x,y");
        expect_needed_fields(&pl("y", "x"), "*", "x,y", "*", "x,y");
        expect_needed_fields(&pl("x", "x"), "*", "x,y", "*", "x,y");

        // needed fields do not intersect with output field
        expect_needed_fields(&pl("y", "z"), "x,y", "", "x,y", "");
        expect_needed_fields(&pl("z", "z"), "x,y", "", "x,y", "");

        // needed fields intersect with output field
        expect_needed_fields(&pl("z", "f2"), "f2,y", "", "y,z", "");
        expect_needed_fields(&pl("y", "f2"), "f2,y", "", "y", "");
        expect_needed_fields(&pl("y", "y"), "f2,y", "", "f2,y", "");
    }
}

//! Port of `pipe_hash.go` — the `| hash(field) [as result]` pipe, which writes a
//! float64-compatible xxhash of a source field into a result field.
//!
//! PORT NOTE: unlike `decolorize`, Go's `pipeHash` does NOT use
//! `newPipeUpdateProcessor`: it reads a *source* field (`fieldName`) and writes a
//! *different* result field (`resultField`), tracking float64 min/max for the
//! emitted column. The shared update processor rewrites a single field in place,
//! so it cannot model source ≠ result. This port therefore mirrors Go's bespoke
//! `pipeHashProcessor` directly (const fast path + float64 slow path), matching
//! `pipe_coalesce.rs`'s sharding style.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::block_result::{BlockResult, ResultColumn};
use crate::filter_generic::is_msg_field_name;
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;
use crate::values_encoder::{marshal_float64, marshal_float64_string};

/// `pipeHash` implements `| hash(field) [as result]`.
pub struct PipeHash {
    pub(crate) field_name: String,
    pub(crate) result_field: String,
}

/// Constructs a `hash` pipe from already-parsed components.
///
/// PORT NOTE: Go's `parsePipeHash` is lexer-dependent and deferred; this
/// constructor takes the source field and result field directly (the Go parser
/// defaults `result_field` to `_msg`).
pub(crate) fn new_pipe_hash(field_name: String, result_field: String) -> PipeHash {
    PipeHash {
        field_name,
        result_field,
    }
}

/// Port of Go `getFloat64CompatibleHash`: an xxhash64 (seed 0) masked to the
/// low 53 bits so the value round-trips exactly through an `f64`.
fn get_float64_compatible_hash(v: &[u8]) -> f64 {
    let h = xxhash_rust::xxh64::xxh64(v, 0) & ((1u64 << 53) - 1);
    h as f64
}

impl Pipe for PipeHash {
    fn to_string(&self) -> String {
        let mut s = format!("hash({})", quote_token_if_needed(&self.field_name));
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
            .map(|_| Mutex::new(PipeHashProcessorShard::default()))
            .collect();
        Arc::new(PipeHashProcessor {
            field_name: self.field_name.clone(),
            result_field: self.result_field.clone(),
            pp_next,
            shards,
        })
    }
}

struct PipeHashProcessor {
    field_name: String,
    result_field: String,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeHashProcessorShard>>,
}

#[derive(Default)]
struct PipeHashProcessorShard {
    rc: ResultColumn,
}

impl PipeProcessor for PipeHashProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let mut shard = self.shards[worker_id].lock().unwrap();
        shard.rc.name = self.result_field.clone();

        let c = br.get_column_by_name(&self.field_name);
        if br.column_is_const(c) {
            // Fast path for const column.
            let v = br.column_get_value_at_row(c, 0).to_string();
            let f = get_float64_compatible_hash(v.as_bytes());
            let mut b: Vec<u8> = Vec::new();
            marshal_float64_string(&mut b, f);
            shard.rc.reset();
            br.add_const_column(&self.result_field, &String::from_utf8_lossy(&b));
        } else {
            // Slow path for other columns.
            let values: Vec<Vec<u8>> = br.column_get_values(c).to_vec();
            let mut min_value = f64::NAN;
            let mut max_value = f64::NAN;
            let mut encoded: Vec<u8> = Vec::new();
            for (row_idx, v) in values.iter().enumerate() {
                if row_idx == 0 || values[row_idx] != values[row_idx - 1] {
                    let f = get_float64_compatible_hash(v);
                    if min_value.is_nan() {
                        min_value = f;
                        max_value = f;
                    } else if f < min_value {
                        min_value = f;
                    } else if f > max_value {
                        max_value = f;
                    }
                    encoded.clear();
                    marshal_float64(&mut encoded, f);
                }
                shard.rc.add_value(&encoded);
            }
            let rc = std::mem::take(&mut shard.rc);
            br.add_result_column_float64(rc, min_value, max_value);
        }

        drop(shard);
        self.pp_next.write_block(worker_id, br);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::{assert_needed_fields, assert_rows_eq, rows, run_pipe};

    // PORT NOTE: `TestParsePipeHashSuccess` / `TestParsePipeHashFailure` exercise
    // the lexer-based `parsePipeHash`, which is deferred; they are omitted until
    // the LogsQL parser is ported.

    fn hash(field_name: &str, result_field: &str) -> PipeHash {
        new_pipe_hash(field_name.to_string(), result_field.to_string())
    }

    #[test]
    fn test_pipe_hash() {
        let p = hash("foo", "x");
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("foo", "abcde"), ("baz", "1234567890")],
                    &[("foo", "abc"), ("bar", "de")],
                    &[("baz", "xyz")],
                ]),
            ),
            &rows(&[
                &[
                    ("foo", "abcde"),
                    ("baz", "1234567890"),
                    ("x", "957726378018795"),
                ],
                &[("foo", "abc"), ("bar", "de"), ("x", "7930733036767641")],
                &[("baz", "xyz"), ("x", "1929880503118233")],
            ]),
        );
    }

    #[test]
    fn test_pipe_hash_update_needed_fields() {
        // all the needed fields
        assert_needed_fields(&hash("y", "x"), "*", "", "*", "x");
        assert_needed_fields(&hash("x", "x"), "*", "", "*", "");

        // unneeded fields do not intersect with output field
        assert_needed_fields(&hash("y", "x"), "*", "f1,f2", "*", "f1,f2,x");
        assert_needed_fields(&hash("x", "x"), "*", "f1,f2", "*", "f1,f2");

        // unneeded fields intersect with output field
        assert_needed_fields(&hash("z", "x"), "*", "x,y", "*", "x,y");
        assert_needed_fields(&hash("y", "x"), "*", "x,y", "*", "x,y");
        assert_needed_fields(&hash("x", "x"), "*", "x,y", "*", "x,y");

        // needed fields do not intersect with output field
        assert_needed_fields(&hash("y", "z"), "x,y", "", "x,y", "");
        assert_needed_fields(&hash("z", "z"), "x,y", "", "x,y", "");

        // needed fields intersect with output field
        assert_needed_fields(&hash("z", "f2"), "f2,y", "", "y,z", "");
        assert_needed_fields(&hash("y", "f2"), "f2,y", "", "y", "");
        assert_needed_fields(&hash("y", "y"), "f2,y", "", "f2,y", "");
    }
}

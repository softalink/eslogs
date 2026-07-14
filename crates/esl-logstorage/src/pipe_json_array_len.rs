//! Port of EsLogs `lib/logstorage/pipe_json_array_len.go`.
//!
//! `| json_array_len(field) as result` stores the number of elements of the
//! JSON array in `field` into `result`.

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use esl_common::atomicutil::Slice;

use crate::block_result::{BlockResult, ResultColumn};
use crate::json_parser::fastjson;
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::values_encoder::{marshal_float64, marshal_uint64_string};

thread_local! {
    // PORT NOTE: Go pools `fastjson.Parser` via the package-level `jspp`; the
    // port keeps a thread-local pool so parse buffers are reused across calls.
    static JSON_PARSER_POOL: RefCell<Vec<fastjson::Parser>> = const { RefCell::new(Vec::new()) };
}

fn get_parser() -> fastjson::Parser {
    JSON_PARSER_POOL.with(|p| p.borrow_mut().pop().unwrap_or_default())
}

fn put_parser(p: fastjson::Parser) {
    JSON_PARSER_POOL.with(|pool| pool.borrow_mut().push(p));
}

/// `| json_array_len ...` pipe (Go `pipeJSONArrayLen`).
pub(crate) struct PipeJSONArrayLen {
    field_name: Vec<u8>,
    result_field: Vec<u8>,
}

/// Constructs a `PipeJSONArrayLen` (Go `parsePipeJSONArrayLen`; lexer parsing
/// is deferred).
pub(crate) fn new_pipe_json_array_len(
    field_name: impl Into<Vec<u8>>,
    result_field: impl Into<Vec<u8>>,
) -> PipeJSONArrayLen {
    PipeJSONArrayLen {
        field_name: field_name.into(),
        result_field: result_field.into(),
    }
}

impl Pipe for PipeJSONArrayLen {
    /// Port of Go `pipeJSONArrayLen.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        let mut s = format!(
            "json_array_len({})",
            crate::parser::quote_token_bytes_if_needed(&self.field_name)
        );
        if !crate::filter_generic::is_msg_field_name(&self.result_field) {
            s += &format!(
                " as {}",
                crate::parser::quote_token_bytes_if_needed(&self.result_field)
            );
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        if pf.match_string(&self.result_field) {
            pf.add_deny_filter(&self.result_field);
            pf.add_allow_filter(&self.field_name);
        }
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        self.result_field != b"_time"
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeJSONArrayLenProcessor {
            field_name: self.field_name.clone(),
            result_field: self.result_field.clone(),
            pp_next,
            shards: Slice::default(),
        })
    }
}

struct PipeJSONArrayLenProcessor {
    field_name: Vec<u8>,
    result_field: Vec<u8>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Slice<std::sync::Mutex<PipeJSONArrayLenProcessorShard>>,
}

#[derive(Default)]
struct PipeJSONArrayLenProcessorShard {
    rc: ResultColumn,
    min_value: f64,
    max_value: f64,
}

impl PipeProcessor for PipeJSONArrayLenProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let shard_arc = self.shards.get(worker_id);
        let mut guard = shard_arc.lock().unwrap();
        let shard = &mut *guard;

        shard.rc.reset();
        shard.rc.name = self.result_field.clone();
        shard.min_value = f64::NAN;
        shard.max_value = f64::NAN;

        let c = br.get_column_by_name(&self.field_name);
        if br.column_is_const(c) {
            // Fast path for const column.
            let v = br.column_get_value_at_row(c, 0).to_vec();
            let a_len = json_array_len(&v);
            let mut enc = Vec::new();
            marshal_uint64_string(&mut enc, a_len as u64);
            // PORT NOTE: Go calls `br.addResultColumnConst`; that method is
            // private in the Rust port, so a full const column is materialized
            // (all rows equal) and `add_result_column` recognizes it as const.
            let rows_len = br.rows_len();
            for _ in 0..rows_len {
                shard.rc.add_value(&enc);
            }
            br.add_result_column(std::mem::take(&mut shard.rc));
        } else {
            // Slow path for other columns.
            let values = br.column_get_values(c).to_vec();
            let mut v_encoded: Vec<u8> = Vec::new();
            for row_idx in 0..values.len() {
                if row_idx == 0 || values[row_idx] != values[row_idx - 1] {
                    v_encoded = get_encoded_len(
                        &values[row_idx],
                        &mut shard.min_value,
                        &mut shard.max_value,
                    );
                }
                shard.rc.add_value(&v_encoded);
            }
            let (min_value, max_value) = (shard.min_value, shard.max_value);
            br.add_result_column_float64(std::mem::take(&mut shard.rc), min_value, max_value);
        }

        self.pp_next.write_block(worker_id, br);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Port of Go's `shard.getEncodedLen`: encodes the array length as a float64
/// value and tracks the running min/max.
fn get_encoded_len(v: &[u8], min_value: &mut f64, max_value: &mut f64) -> Vec<u8> {
    let a_len = json_array_len(v);
    let f = a_len as f64;

    if min_value.is_nan() {
        *min_value = f;
        *max_value = f;
    } else if f < *min_value {
        *min_value = f;
    } else if f > *max_value {
        *max_value = f;
    }

    let mut buf = Vec::new();
    marshal_float64(&mut buf, f);
    buf
}

/// Returns the number of elements in the JSON array encoded in `v`, or 0 if `v`
/// is not a JSON array.
///
/// PORT NOTE: Go computes this via `len(unpackJSONArray(...))`, which lives in
/// `pipe_unroll.go` (not part of this batch). The length is computed directly
/// from the parsed `fastjson` document, which is behaviorally identical.
fn json_array_len(v: &[u8]) -> usize {
    let s = trim_json_whitespace(v);
    if s.is_empty() || !s.starts_with(b"[") {
        return 0;
    }
    let mut p = get_parser();
    let n = match p.parse(s) {
        Ok(root) => {
            if p.doc.value_type(root) == fastjson::JsonType::Array {
                p.doc.array_len(root)
            } else {
                0
            }
        }
        Err(_) => 0,
    };
    put_parser(p);
    n
}

/// Port of Go's `trimJSONWhitespace`.
fn trim_json_whitespace(mut s: &[u8]) -> &[u8] {
    let is_ws = |b: u8| b == b' ' || b == b'\t' || b == b'\n' || b == b'\r';
    while let Some(&b) = s.first() {
        if !is_ws(b) {
            break;
        }
        s = &s[1..];
    }
    while let Some(&b) = s.last() {
        if !is_ws(b) {
            break;
        }
        s = &s[..s.len() - 1];
    }
    s
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::pipe_unpack::test_utils::{rows, run_pipe};

    fn run(pipe: PipeJSONArrayLen, input: &[&[(&str, &str)]], expected: &[&[(&str, &str)]]) {
        run_pipe(Arc::new(pipe), &rows(input), &rows(expected));
    }

    #[test]
    fn test_pipe_json_array_len() {
        run(
            new_pipe_json_array_len("foo", "x"),
            &[
                &[
                    ("foo", r#"["abcde",2,{"bar":"x,y","z":[1,2]}]"#),
                    ("baz", "1234567890"),
                ],
                &[("foo", " \t\n\r[\"a\",\"b\"]")],
                &[("foo", "abc"), ("bar", "de")],
                &[("baz", "xyz")],
            ],
            &[
                &[
                    ("foo", r#"["abcde",2,{"bar":"x,y","z":[1,2]}]"#),
                    ("baz", "1234567890"),
                    ("x", "3"),
                ],
                &[("foo", " \t\n\r[\"a\",\"b\"]"), ("x", "2")],
                &[("foo", "abc"), ("bar", "de"), ("x", "0")],
                &[("baz", "xyz"), ("x", "0")],
            ],
        );
    }
}

//! Port of `pipe_time_add.go` — the `| time_add <offset> [at <field>]` pipe,
//! which shifts the timestamps stored in a field (default `_time`) by a fixed
//! offset.
//!
//! See <https://docs.victoriametrics.com/victorialogs/logsql/#time_add-pipe>

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::block_result::{BlockResult, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;
use crate::values_encoder::{
    marshal_timestamp_rfc3339_nano_string, sub_int64_no_overflow, try_parse_timestamp_rfc3339_nano,
};

/// `pipeTimeAdd` processes `| time_add ...`.
pub(crate) struct PipeTimeAdd {
    pub(crate) field: String,

    /// The offset (in nanoseconds) that is subtracted from each timestamp.
    ///
    /// Note that Go stores the *negated* parsed duration here (see the PORT NOTE
    /// on [`new_pipe_time_add`]), so subtracting it adds the parsed offset.
    pub(crate) offset: i64,
    pub(crate) offset_str: String,
}

/// Constructs a `time_add` pipe from already-parsed components.
///
/// PORT NOTE: Go's `parsePipeTimeAdd` is lexer-dependent and deferred. It parses
/// the offset duration and stores its negation in `pipeTimeAdd.offset`
/// (`offset: -offset`), so that `writeBlock` shifts timestamps forward by the
/// parsed duration via `SubInt64NoOverflow(ts, offset)`. This constructor takes
/// the field, the already-negated internal `offset`, and the original
/// `offset_str` directly; callers reproduce the parser's negation.
pub(crate) fn new_pipe_time_add(field: String, offset: i64, offset_str: String) -> PipeTimeAdd {
    PipeTimeAdd {
        field,
        offset,
        offset_str,
    }
}

impl Pipe for PipeTimeAdd {
    /// Port of Go `pipeTimeAdd.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        let mut s = format!("time_add {}", self.offset_str);
        if self.field != "_time" {
            s += &format!(" at {}", quote_token_if_needed(&self.field));
        }
        s
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, _pf: &mut prefix_filter::Filter) {
        // do nothing
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeTimeAddProcessorShard::default()))
            .collect();
        Arc::new(PipeTimeAddProcessor {
            field: self.field.clone(),
            offset: self.offset,
            pp_next,
            shards,
        })
    }
}

struct PipeTimeAddProcessor {
    field: String,
    offset: i64,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeTimeAddProcessorShard>>,
}

#[derive(Default)]
struct PipeTimeAddProcessorShard {
    rc: ResultColumn,
    buf: Vec<u8>,
}

impl PipeProcessor for PipeTimeAddProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let mut guard = self.shards[worker_id].lock().unwrap();

        {
            // Reborrow to a plain `&mut Shard` so `rc` and `buf` can be borrowed
            // as disjoint fields in the same expression.
            let shard = &mut *guard;
            shard.rc.name = self.field.clone();

            let c = br.get_column_by_name(&self.field);
            let rows_len = br.rows_len();
            for row_idx in 0..rows_len {
                // Own the value so the mutable borrow of `br` is released before
                // touching the locked shard.
                let v = br.column_get_value_at_row(c, row_idx).to_string();
                match try_parse_timestamp_rfc3339_nano(&v) {
                    Some(ts) => {
                        let ts = sub_int64_no_overflow(ts, self.offset);
                        let buf_len = shard.buf.len();
                        marshal_timestamp_rfc3339_nano_string(&mut shard.buf, ts);
                        shard.rc.add_value(&shard.buf[buf_len..]);
                    }
                    None => {
                        shard.rc.add_value(v.as_bytes());
                    }
                }
            }
        }

        let rc = std::mem::take(&mut guard.rc);
        guard.buf.clear();
        br.add_result_column(rc);
        drop(guard);
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

    // PORT NOTE: `TestParsePipeTimeAddSuccess` / `TestParsePipeTimeAddFailure`
    // exercise the lexer-based `parsePipeTimeAdd`, which is deferred; they are
    // omitted until the LogsQL parser is ported.

    /// Builds a `time_add` pipe the way `parsePipeTimeAdd` would: parse the
    /// offset duration and store its negation.
    fn time_add(offset_str: &str, field: &str) -> PipeTimeAdd {
        let offset = esl_common::timeutil::parse_duration(offset_str).unwrap();
        new_pipe_time_add(field.to_string(), -offset, offset_str.to_string())
    }

    #[test]
    fn test_pipe_time_add() {
        // time_add for _time field
        let p = time_add("1d", "_time");
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("_time", "2025-08-20T10:20:30Z"), ("bar", "abc")],
                    &[("_time", "2025-08-22T10:20:30Z"), ("x", "y")],
                ]),
            ),
            &rows(&[
                &[("_time", "2025-08-21T10:20:30Z"), ("bar", "abc")],
                &[("_time", "2025-08-23T10:20:30Z"), ("x", "y")],
            ]),
        );

        // time_add for non-_time field
        let p = time_add("-1d", "abc");
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("_time", "123"), ("abc", "2025-08-20T10:20:30Z")],
                    &[("_time", "2025-08-22T10:20:30Z"), ("abc", "foobar")],
                ]),
            ),
            &rows(&[
                &[("_time", "123"), ("abc", "2025-08-19T10:20:30Z")],
                &[("_time", "2025-08-22T10:20:30Z"), ("abc", "foobar")],
            ]),
        );
    }

    #[test]
    fn test_pipe_time_add_update_needed_fields() {
        // all the needed fields
        assert_needed_fields(&time_add("1h", "x"), "*", "", "*", "");

        // unneeded fields do not intersect with the field
        assert_needed_fields(&time_add("1h", "x"), "*", "f1,f2", "*", "f1,f2");

        // unneeded fields intersect with the field
        assert_needed_fields(&time_add("1h", "x"), "*", "x", "*", "x");

        // needed fields do not intersect with the field
        assert_needed_fields(&time_add("1h", "x"), "f1,f2", "", "f1,f2", "");

        // needed fields intersect with the field
        assert_needed_fields(&time_add("1h", "x"), "f1,f2,x", "", "f1,f2,x", "");
    }
}

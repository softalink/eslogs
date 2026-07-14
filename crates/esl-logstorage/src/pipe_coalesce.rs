//! Port of `pipe_coalesce.go` — the `| coalesce(...) [default v] [as f]` pipe,
//! which fills a destination field with the first non-empty source field value.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::block_result::{BlockResult, ColRef, ResultColumn};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter::{self, is_wildcard_filter, match_filter_bytes};
use crate::stats_count_uniq::field_names_string;
use crate::stream_filter::quote_token_if_needed;

/// `pipeCoalesce` implements `| coalesce (...) as ...`.
pub struct PipeCoalesce {
    pub(crate) src_field_filters: Vec<String>,
    pub(crate) dst_field: String,
    pub(crate) default_value: String,
}

/// Constructs a `coalesce` pipe from already-parsed components.
///
/// PORT NOTE: Go's `parsePipeCoalesce` is lexer-dependent and deferred; this
/// constructor takes the parsed source field filters, destination field and
/// default value directly.
pub(crate) fn new_pipe_coalesce(
    src_field_filters: Vec<String>,
    dst_field: String,
    default_value: String,
) -> PipeCoalesce {
    PipeCoalesce {
        src_field_filters,
        dst_field,
        default_value,
    }
}

impl Pipe for PipeCoalesce {
    /// Port of Go `pipeCoalesce.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        if self.src_field_filters.is_empty() {
            // Go logs a BUG panic here; keep the assertion text.
            panic!("BUG: pipeCoalesce must contain at least one srcField");
        }
        let mut s = format!("coalesce({})", field_names_string(&self.src_field_filters));
        if !self.default_value.is_empty() {
            s += &format!(" default {}", quote_token_if_needed(&self.default_value));
        }
        if self.dst_field != "_msg" {
            s += &format!(" as {}", quote_token_if_needed(&self.dst_field));
        }
        s
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        self.dst_field != "_time"
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        if pf.match_string(&self.dst_field) {
            pf.add_deny_filter(&self.dst_field);
            pf.add_allow_filters(&self.src_field_filters);
        }
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeCoalesceProcessorShard::default()))
            .collect();
        Arc::new(PipeCoalesceProcessor {
            src_field_filters: self.src_field_filters.clone(),
            dst_field: self.dst_field.clone(),
            default_value: self.default_value.clone(),
            pp_next,
            shards,
        })
    }
}

struct PipeCoalesceProcessor {
    src_field_filters: Vec<String>,
    dst_field: String,
    default_value: String,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeCoalesceProcessorShard>>,
}

#[derive(Default)]
struct PipeCoalesceProcessorShard {
    rc: ResultColumn,
}

impl PipeProcessor for PipeCoalesceProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let mut shard = self.shards[worker_id].lock().unwrap();

        // Determine the columns to coalesce, deduped by name (Go shard.cs).
        let cs = br.get_columns();
        let cs_names: Vec<Vec<u8>> = cs.iter().map(|&c| br.column_name(c).to_vec()).collect();

        let mut selected: Vec<ColRef> = Vec::new();
        let mut selected_names: Vec<Vec<u8>> = Vec::new();
        let add_col = |c: ColRef, name: &[u8], sel: &mut Vec<ColRef>, names: &mut Vec<Vec<u8>>| {
            if names.iter().any(|n| n == name) {
                return;
            }
            sel.push(c);
            names.push(name.to_vec());
        };

        for ff in &self.src_field_filters {
            if !is_wildcard_filter(ff) {
                let c = br.get_column_by_name(ff);
                let name = br.column_name(c).to_vec();
                add_col(c, &name, &mut selected, &mut selected_names);
                continue;
            }
            for (&c, name) in cs.iter().zip(cs_names.iter()) {
                if match_filter_bytes(ff, name) {
                    add_col(c, name, &mut selected, &mut selected_names);
                }
            }
        }

        // Fill the result column.
        for row_idx in 0..br.rows_len() {
            let mut value = Vec::new();
            for &c in &selected {
                let v = br.column_get_value_at_row(c, row_idx);
                if !v.is_empty() {
                    value = v.to_vec();
                    break;
                }
            }
            if value.is_empty() {
                value = self.default_value.clone().into_bytes();
            }
            shard.rc.add_value(&value);
        }

        shard.rc.name = self.dst_field.clone().into_bytes();
        let rc = std::mem::take(&mut shard.rc);
        br.add_result_column(rc);
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

    // PORT NOTE: `TestParsePipeCoalesceSuccess` / `TestParsePipeCoalesceFailure`
    // exercise the lexer-based `parsePipeCoalesce`, which is deferred; they are
    // omitted until the LogsQL parser is ported.

    fn coalesce(src: &[&str], default_value: &str, dst: &str) -> PipeCoalesce {
        new_pipe_coalesce(
            src.iter().map(|s| s.to_string()).collect(),
            dst.to_string(),
            default_value.to_string(),
        )
    }

    #[test]
    fn test_pipe_coalesce() {
        // a single value with default value
        let p = coalesce(&["a"], "foo", "_msg");
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[&[("_msg", "test"), ("b", "value_b")], &[("a", "value_a")]]),
            ),
            &rows(&[
                &[("_msg", "foo"), ("b", "value_b")],
                &[("_msg", "value_a"), ("a", "value_a")],
            ]),
        );

        // field prefix
        let p = coalesce(&["a*", "b"], "", "_msg");
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("_msg", "test"), ("abc", "value_a"), ("b", "value_b")],
                    &[("_msg", "test"), ("b", "value_b")],
                    &[("_msg", "test")],
                ]),
            ),
            &rows(&[
                &[("_msg", "value_a"), ("abc", "value_a"), ("b", "value_b")],
                &[("_msg", "value_b"), ("b", "value_b")],
                &[("_msg", "")],
            ]),
        );

        // multiple values
        let p = coalesce(&["a", "b"], "", "_msg");
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("_msg", "test"), ("a", "value_a"), ("b", "value_b")],
                    &[("_msg", "test"), ("b", "value_b")],
                    &[("_msg", "test"), ("a", "value_a")],
                    &[("_msg", "test")],
                ]),
            ),
            &rows(&[
                &[("_msg", "value_a"), ("a", "value_a"), ("b", "value_b")],
                &[("_msg", "value_b"), ("b", "value_b")],
                &[("_msg", "value_a"), ("a", "value_a")],
                &[("_msg", "")],
            ]),
        );

        // as result
        let p = coalesce(&["a", "b"], "", "result");
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "test"), ("b", "value_b")]])),
            &rows(&[&[("_msg", "test"), ("b", "value_b"), ("result", "value_b")]]),
        );

        // default value used
        let p = coalesce(&["a", "b"], "default_value", "result");
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "test")]])),
            &rows(&[&[("_msg", "test"), ("result", "default_value")]]),
        );

        let p = coalesce(&["x", "y", "z"], "unknown", "result");
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "test"), ("a", "value")]])),
            &rows(&[&[("_msg", "test"), ("a", "value"), ("result", "unknown")]]),
        );

        let p = coalesce(&["a", "b", "c"], "", "result");
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[&[("_msg", "test"), ("b", "value_b"), ("c", "value_c")]]),
            ),
            &rows(&[&[
                ("_msg", "test"),
                ("b", "value_b"),
                ("c", "value_c"),
                ("result", "value_b"),
            ]]),
        );

        // empty default
        let p = coalesce(&["a", "b"], "", "result");
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "test")]])),
            &rows(&[&[("_msg", "test"), ("result", "")]]),
        );
    }

    #[test]
    fn test_pipe_coalesce_update_needed_fields() {
        let p = coalesce(&["s1", "s2"], "", "d");
        assert_needed_fields(&p, "*", "", "*", "d");

        let p = coalesce(&["s1", "s2"], "", "s1");
        assert_needed_fields(&p, "*", "", "*", "");

        let p = coalesce(&["s1", "s2"], "", "d");
        assert_needed_fields(&p, "*", "f1,f2", "*", "d,f1,f2");

        let p = coalesce(&["s1", "s2"], "", "d");
        assert_needed_fields(&p, "*", "s1,f1,f2", "*", "d,f1,f2");

        let p = coalesce(&["s1", "s2"], "", "d");
        assert_needed_fields(&p, "*", "d,f1,f2", "*", "d,f1,f2");

        let p = coalesce(&["s1", "s2"], "", "s1");
        assert_needed_fields(&p, "*", "s1,f1,f2", "*", "f1,f2,s1");

        let p = coalesce(&["s1", "s2"], "", "d");
        assert_needed_fields(&p, "f1,f2", "", "f1,f2", "");

        let p = coalesce(&["s1", "s2"], "", "d");
        assert_needed_fields(&p, "s1,f1,f2", "", "f1,f2,s1", "");

        let p = coalesce(&["s1", "s2"], "", "d");
        assert_needed_fields(&p, "d,f1,f2", "", "f1,f2,s1,s2", "");

        let p = coalesce(&["s1", "s2*", "s3"], "", "d");
        assert_needed_fields(&p, "s1,d,f1,f2", "", "f1,f2,s1,s2*,s3", "");
    }
}

//! Port of EsLogs `lib/logstorage/pipe_unpack_words.go`.
//!
//! `| unpack_words ...` tokenizes a source field into words and stores them as
//! a JSON array in a destination field. It reuses [`crate::pipe_unpack`]'s
//! write context and [`crate::tokenizer`].

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use esl_common::atomicutil::Slice;

use crate::block_result::BlockResult;
use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_unpack::PipeUnpackWriteContext;
use crate::prefix_filter;
use crate::rows::Field;
use crate::stats_uniq_values::marshal_json_array;
use crate::stream_filter::quote_token_if_needed;
use crate::tokenizer::{get_tokenizer, put_tokenizer};

/// `| unpack_words ...` pipe (Go `pipeUnpackWords`).
pub(crate) struct PipeUnpackWords {
    /// Field to unpack words from.
    src_field: String,
    /// Field to put the unpacked words into.
    dst_field: String,
    /// Whether to drop duplicate words.
    drop_duplicates: bool,
}

/// Constructs a `PipeUnpackWords` (Go `parsePipeUnpackWords`; lexer parsing is
/// deferred).
pub(crate) fn new_pipe_unpack_words(
    src_field: impl Into<String>,
    dst_field: impl Into<String>,
    drop_duplicates: bool,
) -> PipeUnpackWords {
    PipeUnpackWords {
        src_field: src_field.into(),
        dst_field: dst_field.into(),
        drop_duplicates,
    }
}

impl Pipe for PipeUnpackWords {
    fn to_string(&self) -> String {
        let mut s = String::from("unpack_words");
        if self.src_field != "_msg" {
            s += &format!(" from {}", quote_token_if_needed(&self.src_field));
        }
        if self.dst_field != self.src_field {
            s += &format!(" as {}", quote_token_if_needed(&self.dst_field));
        }
        if self.drop_duplicates {
            s += " drop_duplicates";
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        if pf.match_string(&self.dst_field) {
            pf.add_deny_filter(&self.dst_field);
            pf.add_allow_filter(&self.src_field);
        }
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        self.dst_field != "_time"
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeUnpackWordsProcessor {
            src_field: self.src_field.clone(),
            dst_field: self.dst_field.clone(),
            drop_duplicates: self.drop_duplicates,
            pp_next,
            shards: Slice::default(),
        })
    }
}

struct PipeUnpackWordsProcessor {
    src_field: String,
    dst_field: String,
    drop_duplicates: bool,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Slice<std::sync::Mutex<PipeUnpackWordsProcessorShard>>,
}

#[derive(Default)]
struct PipeUnpackWordsProcessorShard {
    wctx: PipeUnpackWriteContext,
}

impl PipeProcessor for PipeUnpackWordsProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let shard_arc = self.shards.get(worker_id);
        let mut guard = shard_arc.lock().unwrap();
        let shard = &mut *guard;

        shard
            .wctx
            .init(worker_id, self.pp_next.clone(), false, false, br);

        let c = br.get_column_by_name(&self.src_field);
        let values: Vec<String> = br
            .column_get_values(c)
            .iter()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect();

        let mut t = get_tokenizer();
        let keep_duplicate_tokens = !self.drop_duplicates;
        let mut field = [Field {
            name: self.dst_field.clone(),
            value: String::new(),
        }];
        for row_idx in 0..values.len() {
            if row_idx == 0 || values[row_idx] != values[row_idx - 1] {
                t.reset();
                let mut words: Vec<&str> = Vec::new();
                t.tokenize_string(&mut words, &values[row_idx], keep_duplicate_tokens);
                let items: Vec<Vec<u8>> = words.iter().map(|w| w.as_bytes().to_vec()).collect();
                let mut buf = Vec::new();
                marshal_json_array(&mut buf, &items);
                field[0].value = String::from_utf8_lossy(&buf).into_owned();
            }
            shard.wctx.write_row(br, row_idx, &field);
        }
        put_tokenizer(t);

        shard.wctx.flush();
        shard.wctx.reset();
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::pipe_unpack::test_utils::{rows, run_pipe};

    fn run(pipe: PipeUnpackWords, input: &[&[(&str, &str)]], expected: &[&[(&str, &str)]]) {
        run_pipe(Arc::new(pipe), &rows(input), &rows(expected));
    }

    #[test]
    fn test_pipe_unpack_words_by_missing_field() {
        run(
            new_pipe_unpack_words("x", "x", false),
            &[&[("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#), ("q", "w")]],
            &[&[
                ("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#),
                ("q", "w"),
                ("x", "[]"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_words_field_without_words() {
        run(
            new_pipe_unpack_words("q", "q", false),
            &[&[
                ("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#),
                ("q", "!#$%,"),
            ]],
            &[&[
                ("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#),
                ("q", "[]"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_words_field_with_words() {
        run(
            new_pipe_unpack_words("a", "a", false),
            &[
                &[("a", "foo,bar baz"), ("q", "w")],
                &[("a", "b"), ("c", "d")],
            ],
            &[
                &[("a", r#"["foo","bar","baz"]"#), ("q", "w")],
                &[("a", r#"["b"]"#), ("c", "d")],
            ],
        );
    }

    #[test]
    fn test_pipe_unpack_words_into_another_field() {
        run(
            new_pipe_unpack_words("a", "b", false),
            &[
                &[("a", "foo,bar baz"), ("q", "w")],
                &[("a", "b"), ("c", "d")],
            ],
            &[
                &[
                    ("a", "foo,bar baz"),
                    ("b", r#"["foo","bar","baz"]"#),
                    ("q", "w"),
                ],
                &[("a", "b"), ("b", r#"["b"]"#), ("c", "d")],
            ],
        );
    }

    #[test]
    fn test_pipe_unpack_words_from_msg_inplace() {
        run(
            new_pipe_unpack_words("_msg", "_msg", false),
            &[
                &[("_msg", "foo,bar baz"), ("q", "w")],
                &[("_msg", "b"), ("c", "d")],
            ],
            &[
                &[("_msg", r#"["foo","bar","baz"]"#), ("q", "w")],
                &[("_msg", r#"["b"]"#), ("c", "d")],
            ],
        );
    }

    #[test]
    fn test_pipe_unpack_words_from_msg_into_other_field() {
        run(
            new_pipe_unpack_words("_msg", "b", false),
            &[
                &[("_msg", "  foo,bar foo  "), ("q", "w")],
                &[("_msg", "b"), ("c", "d")],
            ],
            &[
                &[
                    ("_msg", "  foo,bar foo  "),
                    ("b", r#"["foo","bar","foo"]"#),
                    ("q", "w"),
                ],
                &[("_msg", "b"), ("b", r#"["b"]"#), ("c", "d")],
            ],
        );
    }

    #[test]
    fn test_pipe_unpack_words_drop_duplicates() {
        run(
            new_pipe_unpack_words("_msg", "b", true),
            &[
                &[("_msg", "foo,bar foo"), ("q", "w")],
                &[("_msg", "b"), ("c", "d")],
            ],
            &[
                &[
                    ("_msg", "foo,bar foo"),
                    ("b", r#"["foo","bar"]"#),
                    ("q", "w"),
                ],
                &[("_msg", "b"), ("b", r#"["b"]"#), ("c", "d")],
            ],
        );
    }
}

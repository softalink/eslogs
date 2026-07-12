//! Port of `pipe_decolorize.go` — the `| decolorize [field]` pipe, which strips
//! ANSI color escape sequences from a single field value in place.
//!
//! This is an "update-family" pipe: it reuses the shared
//! [`crate::pipe_update::new_pipe_update_processor`] machinery, supplying an
//! `updateFunc` that runs [`crate::color_sequence::drop_color_sequences`].

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::color_sequence::drop_color_sequences;
use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_update::new_pipe_update_processor;
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;

/// `pipeDecolorize` implements `| decolorize [field]`.
pub struct PipeDecolorize {
    pub(crate) field: String,
}

/// Constructs a `decolorize` pipe from an already-parsed field name.
///
/// PORT NOTE: Go's `parsePipeDecolorize` is lexer-dependent and deferred; this
/// constructor takes the target field directly (defaulting to `_msg` at the
/// call site, matching the Go parser's default).
pub(crate) fn new_pipe_decolorize(field: String) -> PipeDecolorize {
    PipeDecolorize { field }
}

impl Pipe for PipeDecolorize {
    fn to_string(&self) -> String {
        let mut s = "decolorize".to_string();
        if self.field != "_msg" {
            s += &format!(" {}", quote_token_if_needed(&self.field));
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
        // nothing to do
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        // Port of Go's `updateFunc(a *arena, v string) string` which appends
        // `dropColorSequences(a.b, v)` to a pooled arena and returns the new
        // suffix. The Rust port drops the arena and returns an owned String.
        let update_func = Arc::new(|v: &str| {
            let mut buf: Vec<u8> = Vec::new();
            drop_color_sequences(&mut buf, v);
            String::from_utf8_lossy(&buf).into_owned()
        });

        new_pipe_update_processor(update_func, pp_next, self.field.clone(), None, concurrency)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::{assert_needed_fields, assert_rows_eq, rows, run_pipe};

    // PORT NOTE: `TestParsePipeDecolorizeSuccess` / `TestParsePipeDecolorizeFailure`
    // exercise the lexer-based `parsePipeDecolorize`, which is deferred; they are
    // omitted until the LogsQL parser is ported.

    fn decolorize(field: &str) -> PipeDecolorize {
        new_pipe_decolorize(field.to_string())
    }

    #[test]
    fn test_pipe_decolorize() {
        // decolorize _msg
        let p = decolorize("_msg");
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[
                        ("_msg", "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H"),
                        ("bar", "cde"),
                    ],
                    &[("_msg", "a_bc_def")],
                ]),
            ),
            &rows(&[
                &[("_msg", "fooERROR bar"), ("bar", "cde")],
                &[("_msg", "a_bc_def")],
            ]),
        );

        // decolorize non-_msg field
        let p = decolorize("bar");
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[
                        ("bar", "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H"),
                        ("_msg", "cde"),
                    ],
                    &[("bar", "a_bc_def")],
                ]),
            ),
            &rows(&[
                &[("bar", "fooERROR bar"), ("_msg", "cde")],
                &[("bar", "a_bc_def")],
            ]),
        );
    }

    #[test]
    fn test_pipe_decolorize_update_needed_fields() {
        // all the needed fields
        assert_needed_fields(&decolorize("x"), "*", "", "*", "");

        // unneeded fields do not intersect with field
        assert_needed_fields(&decolorize("x"), "*", "f1,f2", "*", "f1,f2");

        // unneeded fields intersect with field
        assert_needed_fields(&decolorize("x"), "*", "x,y", "*", "x,y");

        // needed fields do not intersect with field
        assert_needed_fields(&decolorize("x"), "f2,y", "", "f2,y", "");

        // needed fields intersect with field
        assert_needed_fields(&decolorize("y"), "f2,y", "", "f2,y", "");
    }
}

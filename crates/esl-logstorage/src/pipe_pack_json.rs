//! Port of EsLogs `lib/logstorage/pipe_pack_json.go`.
//!
//! `| pack_json ...` serializes a set of fields into a JSON object stored in a
//! result field, using the shared scaffolding in [`crate::pipe_pack`].

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_pack::{new_pipe_pack_processor, update_needed_fields_for_pipe_pack};
use crate::prefix_filter;
use crate::rows::marshal_fields_to_json;
use crate::stream_filter::quote_token_if_needed;

/// `| pack_json ...` pipe (Go `pipePackJSON`).
pub(crate) struct PipePackJSON {
    result_field: String,
    /// Field names and/or prefixes to put inside the packed JSON.
    field_filters: Vec<String>,
}

/// Constructs a `PipePackJSON` (Go `parsePipePackJSON`; lexer parsing is
/// deferred). Mirrors Go's normalization: a `fields` list containing `*`
/// becomes empty (pack everything).
pub(crate) fn new_pipe_pack_json(
    field_filters: Vec<String>,
    result_field: impl Into<String>,
) -> PipePackJSON {
    let field_filters = if field_filters.iter().any(|f| f == "*") {
        Vec::new()
    } else {
        field_filters
    };
    PipePackJSON {
        result_field: result_field.into(),
        field_filters,
    }
}

impl Pipe for PipePackJSON {
    /// Port of Go `pipePackJSON.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    fn to_string(&self) -> String {
        let mut s = String::from("pack_json");
        if !self.field_filters.is_empty() {
            s += &format!(" fields ({})", field_names_string(&self.field_filters));
        }
        if !crate::filter_generic::is_msg_field_name(&self.result_field) {
            s += &format!(" as {}", quote_token_if_needed(&self.result_field));
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        update_needed_fields_for_pipe_pack(pf, &self.result_field, &self.field_filters);
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        self.result_field != "_time"
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        new_pipe_pack_processor(
            pp_next,
            self.result_field.clone(),
            self.field_filters.clone(),
            marshal_fields_to_json,
        )
    }
}

/// Port of Go's `fieldNamesString`.
fn field_names_string(fields: &[String]) -> String {
    fields
        .iter()
        .map(|f| quote_token_if_needed(f))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::pipe_unpack::test_utils::{rows, run_pipe};

    fn run(pipe: PipePackJSON, input: &[&[(&str, &str)]], expected: &[&[(&str, &str)]]) {
        run_pipe(Arc::new(pipe), &rows(input), &rows(expected));
    }

    #[test]
    fn test_pipe_pack_json_into_msg() {
        run(
            new_pipe_pack_json(vec![], "_msg"),
            &[
                &[("_msg", "x"), ("foo", "abc"), ("bar", "cde")],
                &[("a", "b"), ("c", "d")],
            ],
            &[
                &[
                    ("_msg", r#"{"_msg":"x","foo":"abc","bar":"cde"}"#),
                    ("foo", "abc"),
                    ("bar", "cde"),
                ],
                &[("_msg", r#"{"a":"b","c":"d"}"#), ("a", "b"), ("c", "d")],
            ],
        );
    }

    #[test]
    fn test_pipe_pack_json_into_other_field() {
        run(
            new_pipe_pack_json(vec![], "a"),
            &[
                &[("_msg", "x"), ("foo", "abc"), ("bar", "cde")],
                &[("a", "b"), ("c", "d")],
            ],
            &[
                &[
                    ("_msg", "x"),
                    ("foo", "abc"),
                    ("bar", "cde"),
                    ("a", r#"{"_msg":"x","foo":"abc","bar":"cde"}"#),
                ],
                &[("a", r#"{"a":"b","c":"d"}"#), ("c", "d")],
            ],
        );
    }

    #[test]
    fn test_pipe_pack_json_only_needed_fields() {
        run(
            new_pipe_pack_json(vec!["foo".to_string(), "baz".to_string()], "a"),
            &[
                &[("_msg", "x"), ("foo", "abc"), ("bar", "cde")],
                &[("a", "b"), ("c", "d")],
            ],
            &[
                &[
                    ("_msg", "x"),
                    ("foo", "abc"),
                    ("bar", "cde"),
                    ("a", r#"{"foo":"abc"}"#),
                ],
                &[("a", "{}"), ("c", "d")],
            ],
        );
    }

    #[test]
    fn test_pipe_pack_json_wildcard_fields() {
        run(
            new_pipe_pack_json(vec!["x*".to_string(), "y".to_string()], "a"),
            &[&[("x", "abc"), ("xx", "xabc"), ("yy", "cde"), ("y", "xcde")]],
            &[&[
                ("x", "abc"),
                ("xx", "xabc"),
                ("yy", "cde"),
                ("y", "xcde"),
                ("a", r#"{"x":"abc","xx":"xabc","y":"xcde"}"#),
            ]],
        );
    }
}

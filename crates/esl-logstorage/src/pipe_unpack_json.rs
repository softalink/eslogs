//! Port of EsLogs `lib/logstorage/pipe_unpack_json.go`.
//!
//! `| unpack_json ...` unpacks JSON object fields from a source field into
//! separate log fields. It reuses the shared unpack scaffolding in
//! [`crate::pipe_unpack`] and [`crate::json_parser`] for parsing.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::json_parser::{get_json_parser, put_json_parser};
use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_unpack::{
    IfFilter, UnpackFunc, new_pipe_unpack_processor, update_needed_fields_for_unpack_pipe,
};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;

/// `| unpack_json ...` pipe (Go `pipeUnpackJSON`).
pub(crate) struct PipeUnpackJSON {
    /// Field to unpack JSON fields from.
    from_field: String,
    /// Field filters to extract from JSON.
    field_filters: Vec<String>,
    /// JSON keys whose values are preserved (not flattened).
    preserve_keys: Vec<String>,
    /// Prefix added to unpacked field names.
    result_prefix: String,
    keep_original_fields: bool,
    skip_empty_results: bool,
    /// Optional filter for skipping unpacking.
    iff: Option<IfFilter>,
}

/// Constructs a `PipeUnpackJSON` (Go `parsePipeUnpackJSON` builds the same
/// struct; lexer parsing is deferred).
#[allow(clippy::too_many_arguments)]
pub(crate) fn new_pipe_unpack_json(
    from_field: impl Into<String>,
    field_filters: Vec<String>,
    preserve_keys: Vec<String>,
    result_prefix: impl Into<String>,
    keep_original_fields: bool,
    skip_empty_results: bool,
    iff: Option<IfFilter>,
) -> PipeUnpackJSON {
    let field_filters = if field_filters.is_empty() {
        vec!["*".to_string()]
    } else {
        field_filters
    };
    PipeUnpackJSON {
        from_field: from_field.into(),
        field_filters,
        preserve_keys,
        result_prefix: result_prefix.into(),
        keep_original_fields,
        skip_empty_results,
        iff,
    }
}

impl Pipe for PipeUnpackJSON {
    /// Port of Go `pipeUnpackJSON.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

    /// Go `hasFilterInWithQuery` for this pipe: checks the `if (...)` filter.
    fn has_filter_in_with_query(&self) -> bool {
        self.iff
            .as_ref()
            .is_some_and(|iff| iff.has_filter_in_with_query())
    }

    /// Go `initFilterInValues` for this pipe: rewrites the `if (...)` filter.
    fn init_filter_in_values(
        &mut self,
        get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
        timestamp: i64,
    ) -> Result<(), String> {
        if let Some(iff) = &self.iff
            && let Some(iff_new) = iff.init_filter_in_values(get_values, timestamp)?
        {
            self.iff = Some(iff_new);
        }
        Ok(())
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        // The unpack_json pipe generates additional by(...) labels from
        // `fields (...)`. PORT NOTE: Go keeps `fieldFilters` empty when
        // `fields (...)` is missing; the Rust constructor normalizes that to
        // `["*"]`, which the stats-labels check maps back to "missing
        // fields(...)" via the wildcard validation.
        Some(crate::pipe::StatsTailOp::UnpackJson {
            field_filters: self.field_filters.clone(),
        })
    }

    fn to_string(&self) -> String {
        let mut s = String::from("unpack_json");
        if let Some(iff) = &self.iff {
            s += &format!(" {iff}");
        }
        if !crate::filter_generic::is_msg_field_name(&self.from_field) {
            s += &format!(" from {}", quote_token_if_needed(&self.from_field));
        }
        if !prefix_filter::match_all(&self.field_filters) {
            s += &format!(" fields ({})", field_names_string(&self.field_filters));
        }
        if !self.preserve_keys.is_empty() {
            s += &format!(
                " preserve_keys ({})",
                field_names_string(&self.preserve_keys)
            );
        }
        if !self.result_prefix.is_empty() {
            s += &format!(
                " result_prefix {}",
                quote_token_if_needed(&self.result_prefix)
            );
        }
        if self.keep_original_fields {
            s += " keep_original_fields";
        }
        if self.skip_empty_results {
            s += " skip_empty_results";
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        update_needed_fields_for_unpack_pipe(
            &self.from_field,
            &self.result_prefix,
            &self.field_filters,
            self.keep_original_fields,
            self.skip_empty_results,
            self.iff.as_ref(),
            pf,
        );
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        true
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let field_filters = self.field_filters.clone();
        let preserve_keys = self.preserve_keys.clone();
        let unpack_json: UnpackFunc = Box::new(move |uctx, s| {
            let s = trim_json_whitespace(s);
            if s.is_empty() || !s.starts_with('{') {
                // This isn't a JSON object.
                return;
            }
            let mut p = get_json_parser();
            let preserve: Vec<&str> = preserve_keys.iter().map(|s| s.as_str()).collect();
            // PORT NOTE: Go passes math.MaxInt for maxFieldNameLen; the public
            // Rust wrapper uses `consts::MAX_FIELD_NAME_SIZE` (128). This only
            // affects flattened keys longer than 128 bytes, which are otherwise
            // identical.
            match p.parse_log_message(s.as_bytes(), &preserve, "") {
                Err(_) => {
                    for filter in &field_filters {
                        if !prefix_filter::is_wildcard_filter(filter) {
                            uctx.add_field(filter, "");
                        }
                    }
                }
                Ok(()) => {
                    for f in p.fields() {
                        if !prefix_filter::match_filters(&field_filters, &f.name) {
                            continue;
                        }
                        uctx.add_field(&f.name, &f.value);
                    }
                    for filter in &field_filters {
                        if prefix_filter::is_wildcard_filter(filter) {
                            continue;
                        }
                        let add_empty_field = !p.fields().iter().any(|f| f.name == *filter);
                        if add_empty_field {
                            uctx.add_field(filter, "");
                        }
                    }
                }
            }
            put_json_parser(p);
        });

        new_pipe_unpack_processor(
            unpack_json,
            pp_next,
            self.from_field.clone(),
            self.result_prefix.clone(),
            self.keep_original_fields,
            self.skip_empty_results,
            self.iff.clone(),
        )
    }
}

/// Port of Go's `trimJSONWhitespace`.
fn trim_json_whitespace(s: &str) -> &str {
    s.trim_matches(|c| c == ' ' || c == '\t' || c == '\n' || c == '\r')
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

    fn run(pipe: PipeUnpackJSON, input: &[&[(&str, &str)]], expected: &[&[(&str, &str)]]) {
        run_pipe(Arc::new(pipe), &rows(input), &rows(expected));
    }

    // PORT NOTE: the `if (...)` runtime cases from Go's TestPipeUnpackJSON are
    // deferred — they need the lexer/filter parser to build the `if` filter.

    #[test]
    fn test_pipe_unpack_json_skip_empty_results() {
        run(
            new_pipe_unpack_json("_msg", vec![], vec![], "", false, true, None),
            &[&[
                ("_msg", r#"{"foo":"bar","z":"q","a":""}"#),
                ("foo", "x"),
                ("a", "foobar"),
            ]],
            &[&[
                ("_msg", r#"{"foo":"bar","z":"q","a":""}"#),
                ("foo", "bar"),
                ("z", "q"),
                ("a", "foobar"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_no_skip_empty_results() {
        run(
            new_pipe_unpack_json("_msg", vec![], vec![], "", false, false, None),
            &[&[
                ("_msg", r#"{"foo":"bar","z":"q","a":""}"#),
                ("foo", "x"),
                ("a", "foobar"),
            ]],
            &[&[
                ("_msg", r#"{"foo":"bar","z":"q","a":""}"#),
                ("foo", "bar"),
                ("z", "q"),
                ("a", ""),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_no_keep_original_fields() {
        run(
            new_pipe_unpack_json("_msg", vec![], vec![], "", false, false, None),
            &[&[
                ("_msg", r#"{"foo":"bar","z":"q","a":"b"}"#),
                ("foo", "x"),
                ("a", ""),
            ]],
            &[&[
                ("_msg", r#"{"foo":"bar","z":"q","a":"b"}"#),
                ("foo", "bar"),
                ("z", "q"),
                ("a", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_keep_original_fields() {
        run(
            new_pipe_unpack_json("_msg", vec![], vec![], "", true, false, None),
            &[&[
                ("_msg", r#"{"foo":"bar","z":"q","a":"b"}"#),
                ("foo", "x"),
                ("a", ""),
            ]],
            &[&[
                ("_msg", r#"{"foo":"bar","z":"q","a":"b"}"#),
                ("foo", "x"),
                ("z", "q"),
                ("a", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_only_requested_fields() {
        run(
            new_pipe_unpack_json(
                "_msg",
                vec!["foo".to_string(), "b".to_string()],
                vec![],
                "",
                false,
                false,
                None,
            ),
            &[&[("_msg", r#"{"foo":"bar","z":"q","a":"b"}"#)]],
            &[&[
                ("_msg", r#"{"foo":"bar","z":"q","a":"b"}"#),
                ("foo", "bar"),
                ("b", ""),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_preserve_keys() {
        run(
            new_pipe_unpack_json(
                "_msg",
                vec![],
                vec!["foo".to_string()],
                "",
                false,
                false,
                None,
            ),
            &[&[("_msg", r#"{"foo":{"bar":"baz"},"z":{"q":"y"},"a":"b"}"#)]],
            &[&[
                ("_msg", r#"{"foo":{"bar":"baz"},"z":{"q":"y"},"a":"b"}"#),
                ("foo", r#"{"bar":"baz"}"#),
                ("z.q", "y"),
                ("a", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_from_msg() {
        run(
            new_pipe_unpack_json("_msg", vec![], vec![], "", false, false, None),
            &[&[("_msg", r#"{"foo":"bar"}"#)]],
            &[&[("_msg", r#"{"foo":"bar"}"#), ("foo", "bar")]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_from_msg_whitespace() {
        run(
            new_pipe_unpack_json("_msg", vec![], vec![], "", false, false, None),
            &[&[("_msg", "\t \n {\"foo\":\"bar\"}\r\n")]],
            &[&[("_msg", "\t \n {\"foo\":\"bar\"}\r\n"), ("foo", "bar")]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_into_msg() {
        run(
            new_pipe_unpack_json("_msg", vec![], vec![], "", false, false, None),
            &[&[("_msg", r#"{"_msg":"bar"}"#)]],
            &[&[("_msg", "bar")]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_from_missing_field() {
        run(
            new_pipe_unpack_json("x", vec![], vec![], "", false, false, None),
            &[&[("_msg", r#"{"foo":"bar"}"#)]],
            &[&[("_msg", r#"{"foo":"bar"}"#)]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_from_non_json() {
        run(
            new_pipe_unpack_json("x", vec![], vec![], "", false, false, None),
            &[&[("x", "foobar")]],
            &[&[("x", "foobar")]],
        );
        run(
            new_pipe_unpack_json(
                "x",
                vec!["foo".to_string(), "bar".to_string()],
                vec![],
                "",
                false,
                false,
                None,
            ),
            &[&[("x", "foobar")]],
            &[&[("x", "foobar")]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_from_non_dict_json() {
        run(
            new_pipe_unpack_json("x", vec![], vec![], "", false, false, None),
            &[&[("x", r#"["foobar"]"#)]],
            &[&[("x", r#"["foobar"]"#)]],
        );
        run(
            new_pipe_unpack_json("x", vec![], vec![], "", false, false, None),
            &[&[("x", "1234")]],
            &[&[("x", "1234")]],
        );
        run(
            new_pipe_unpack_json("x", vec![], vec![], "", false, false, None),
            &[&[("x", r#""xxx""#)]],
            &[&[("x", r#""xxx""#)]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_from_named_field() {
        run(
            new_pipe_unpack_json("x", vec![], vec![], "", false, false, None),
            &[&[(
                "x",
                r#"{"foo":"bar","baz":"xyz","a":123,"b":["foo","bar"],"x":NaN,"y":{"z":{"a":"b"}}}"#,
            )]],
            &[&[
                ("x", "NaN"),
                ("foo", "bar"),
                ("baz", "xyz"),
                ("a", "123"),
                ("b", r#"["foo","bar"]"#),
                ("y.z.a", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_json_multiple_rows_distinct_fields() {
        run(
            new_pipe_unpack_json("x", vec![], vec![], "", false, false, None),
            &[
                &[("x", r#"{"foo":"bar","baz":"xyz"}"#), ("y", "abc")],
                &[("y", "abc")],
                &[("z", "foobar"), ("x", r#"{"z":["bar",123]}"#)],
            ],
            &[
                &[
                    ("x", r#"{"foo":"bar","baz":"xyz"}"#),
                    ("y", "abc"),
                    ("foo", "bar"),
                    ("baz", "xyz"),
                ],
                &[("y", "abc")],
                &[("z", r#"["bar",123]"#), ("x", r#"{"z":["bar",123]}"#)],
            ],
        );
    }

    #[test]
    fn test_pipe_unpack_json_wrapped_with_spaces() {
        run(
            new_pipe_unpack_json("_msg", vec![], vec![], "", false, false, None),
            &[&[("_msg", r#"  {  "foo" : "bar"  }  "#)]],
            &[&[("_msg", r#"  {  "foo" : "bar"  }  "#), ("foo", "bar")]],
        );
    }
}

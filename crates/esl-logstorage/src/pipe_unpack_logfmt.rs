//! Port of EsLogs `lib/logstorage/pipe_unpack_logfmt.go`.
//!
//! `| unpack_logfmt ...` unpacks logfmt `key=value` pairs from a source field
//! into separate log fields. It reuses the shared unpack scaffolding in
//! [`crate::pipe_unpack`] and [`crate::logfmt_parser`] for parsing.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::logfmt_parser::{get_logfmt_parser, put_logfmt_parser};
use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_unpack::{
    IfFilter, UnpackFunc, new_pipe_unpack_processor, update_needed_fields_for_unpack_pipe,
};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;

/// `| unpack_logfmt ...` pipe (Go `pipeUnpackLogfmt`).
pub(crate) struct PipeUnpackLogfmt {
    from_field: String,
    field_filters: Vec<String>,
    result_prefix: String,
    keep_original_fields: bool,
    skip_empty_results: bool,
    iff: Option<IfFilter>,
}

/// Constructs a `PipeUnpackLogfmt` (Go `parsePipeUnpackLogfmt`; lexer parsing
/// is deferred).
pub(crate) fn new_pipe_unpack_logfmt(
    from_field: impl Into<String>,
    field_filters: Vec<String>,
    result_prefix: impl Into<String>,
    keep_original_fields: bool,
    skip_empty_results: bool,
    iff: Option<IfFilter>,
) -> PipeUnpackLogfmt {
    let field_filters = if field_filters.is_empty() {
        vec!["*".to_string()]
    } else {
        field_filters
    };
    PipeUnpackLogfmt {
        from_field: from_field.into(),
        field_filters,
        result_prefix: result_prefix.into(),
        keep_original_fields,
        skip_empty_results,
        iff,
    }
}

impl Pipe for PipeUnpackLogfmt {
    fn to_string(&self) -> String {
        let mut s = String::from("unpack_logfmt");
        if let Some(iff) = &self.iff {
            s += &format!(" {iff}");
        }
        if !crate::filter_generic::is_msg_field_name(&self.from_field) {
            s += &format!(" from {}", quote_token_if_needed(&self.from_field));
        }
        if !prefix_filter::match_all(&self.field_filters) {
            s += &format!(" fields ({})", field_names_string(&self.field_filters));
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
        let unpack_logfmt: UnpackFunc = Box::new(move |uctx, s| {
            let mut p = get_logfmt_parser();
            p.parse(s);
            for f in &p.fields {
                if !prefix_filter::match_filters(&field_filters, &f.name) {
                    continue;
                }
                uctx.add_field(&f.name, &f.value);
            }
            for filter in &field_filters {
                if prefix_filter::is_wildcard_filter(filter) {
                    continue;
                }
                let add_empty_field = !p.fields.iter().any(|f| f.name == *filter);
                if add_empty_field {
                    uctx.add_field(filter, "");
                }
            }
            put_logfmt_parser(p);
        });

        new_pipe_unpack_processor(
            unpack_logfmt,
            pp_next,
            self.from_field.clone(),
            self.result_prefix.clone(),
            self.keep_original_fields,
            self.skip_empty_results,
            self.iff.clone(),
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

    fn run(pipe: PipeUnpackLogfmt, input: &[&[(&str, &str)]], expected: &[&[(&str, &str)]]) {
        run_pipe(Arc::new(pipe), &rows(input), &rows(expected));
    }

    // PORT NOTE: the `if (...)` runtime cases from Go's TestPipeUnpackLogfmt are
    // deferred — they need the lexer/filter parser to build the `if` filter.

    #[test]
    fn test_pipe_unpack_logfmt_subset_of_fields() {
        run(
            new_pipe_unpack_logfmt(
                "_msg",
                vec!["foo".to_string(), "a".to_string(), "b".to_string()],
                "",
                false,
                false,
                None,
            ),
            &[&[("_msg", r#"foo=bar baz="x y=z" a=b"#), ("a", "xxx")]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("foo", "bar"),
                ("a", "b"),
                ("b", ""),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_logfmt_no_skip_empty_results() {
        run(
            new_pipe_unpack_logfmt("_msg", vec![], "", false, false, None),
            &[&[
                ("_msg", r#"foo= baz="x y=z" a=b"#),
                ("foo", "321"),
                ("baz", "abcdef"),
            ]],
            &[&[
                ("_msg", r#"foo= baz="x y=z" a=b"#),
                ("foo", ""),
                ("baz", "x y=z"),
                ("a", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_logfmt_skip_empty_results() {
        run(
            new_pipe_unpack_logfmt("_msg", vec![], "", false, true, None),
            &[&[
                ("_msg", r#"foo= baz="x y=z" a=b"#),
                ("foo", "321"),
                ("baz", "abcdef"),
            ]],
            &[&[
                ("_msg", r#"foo= baz="x y=z" a=b"#),
                ("foo", "321"),
                ("baz", "x y=z"),
                ("a", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_logfmt_keep_original_fields() {
        run(
            new_pipe_unpack_logfmt("_msg", vec![], "", true, false, None),
            &[&[("_msg", r#"foo=bar baz="x y=z" a=b"#), ("baz", "abcdef")]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("foo", "bar"),
                ("baz", "abcdef"),
                ("a", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_logfmt_from_msg() {
        run(
            new_pipe_unpack_logfmt("_msg", vec![], "", false, false, None),
            &[&[("_msg", r#"foo=bar baz="x y=z" a=b"#), ("baz", "abcdef")]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("foo", "bar"),
                ("baz", "x y=z"),
                ("a", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_logfmt_into_msg() {
        run(
            new_pipe_unpack_logfmt("_msg", vec![], "", false, false, None),
            &[&[("_msg", "_msg=bar")]],
            &[&[("_msg", "bar")]],
        );
    }

    #[test]
    fn test_pipe_unpack_logfmt_from_missing_field() {
        run(
            new_pipe_unpack_logfmt("x", vec![], "", false, false, None),
            &[&[("_msg", "foo=bar")]],
            &[&[("_msg", "foo=bar")]],
        );
    }

    #[test]
    fn test_pipe_unpack_logfmt_from_non_logfmt() {
        run(
            new_pipe_unpack_logfmt("x", vec![], "", false, false, None),
            &[&[("x", "foobar")]],
            &[&[("x", "foobar"), ("foobar", "")]],
        );
    }

    #[test]
    fn test_pipe_unpack_logfmt_empty_value() {
        run(
            new_pipe_unpack_logfmt("x", vec![], "", false, false, None),
            &[&[("x", "foobar=")]],
            &[&[("x", "foobar="), ("foobar", "")]],
        );
        run(
            new_pipe_unpack_logfmt("x", vec![], "", false, false, None),
            &[&[("x", r#"foo="" bar= baz="#)]],
            &[&[
                ("x", r#"foo="" bar= baz="#),
                ("foo", ""),
                ("bar", ""),
                ("baz", ""),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_logfmt_multiple_rows_distinct_fields() {
        run(
            new_pipe_unpack_logfmt("x", vec![], "", false, false, None),
            &[
                &[("x", "foo=bar baz=xyz"), ("y", "abc")],
                &[("y", "abc")],
                &[("z", "foobar"), ("x", "z=bar")],
            ],
            &[
                &[
                    ("x", "foo=bar baz=xyz"),
                    ("y", "abc"),
                    ("foo", "bar"),
                    ("baz", "xyz"),
                ],
                &[("y", "abc")],
                &[("z", "bar"), ("x", "z=bar")],
            ],
        );
    }

    #[test]
    fn test_pipe_unpack_logfmt_surrounding_spaces() {
        run(
            new_pipe_unpack_logfmt("_msg", vec![], "", false, false, None),
            &[&[("_msg", "   foo=bar a=b   ")]],
            &[&[("_msg", "   foo=bar a=b   "), ("foo", "bar"), ("a", "b")]],
        );
    }
}

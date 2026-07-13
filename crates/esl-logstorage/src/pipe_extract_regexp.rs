//! Port of EsLogs `lib/logstorage/pipe_extract_regexp.go`.
//!
//! `| extract_regexp "re" ...` pulls named capture groups out of a source
//! field. Uses the `regex` crate (mirroring Go's stdlib `regexp`).

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use esl_common::atomicutil::Slice;
use regex::Regex;

use crate::bitmap::Bitmap;
use crate::block_result::{BlockResult, ColRef, ResultColumn, append_result_column_with_name};
use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_extract::should_deny_overwritten_field;
use crate::pipe_unpack::IfFilter;
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;

/// `| extract_regexp ...` pipe (Go `pipeExtractRegexp`).
pub(crate) struct PipeExtractRegexp {
    from_field: String,
    re: Regex,
    re_str: String,
    /// Named capture fields, indexed by capture-group number (index 0 and
    /// unnamed groups are `""`).
    re_fields: Vec<String>,
    keep_original_fields: bool,
    skip_empty_results: bool,
    iff: Option<IfFilter>,
}

/// Port of Go's `regexpCompile`: `.` matches newlines by default (issue #88).
fn regexp_compile(s: &str) -> Result<Regex, String> {
    Regex::new(&format!("(?s)(?:{s})")).map_err(|e| e.to_string())
}

/// Constructs a `PipeExtractRegexp`, compiling `pattern_str` (Go
/// `parsePipeExtractRegexp`; lexer parsing of the surrounding pipe is deferred).
pub(crate) fn new_pipe_extract_regexp(
    pattern_str: &str,
    from_field: impl Into<String>,
    keep_original_fields: bool,
    skip_empty_results: bool,
    iff: Option<IfFilter>,
) -> Result<PipeExtractRegexp, String> {
    let re = regexp_compile(pattern_str)
        .map_err(|e| format!("cannot parse 'pattern' {pattern_str:?}: {e}"))?;
    let re_fields: Vec<String> = re
        .capture_names()
        .map(|o| o.unwrap_or("").to_string())
        .collect();
    let has_named_fields = re_fields.iter().any(|f| !f.is_empty());
    if !has_named_fields {
        return Err(format!(
            "the 'pattern' {pattern_str:?} must contain at least a single named group in the form (?P<group_name>...)"
        ));
    }
    Ok(PipeExtractRegexp {
        from_field: from_field.into(),
        re,
        re_str: pattern_str.to_string(),
        re_fields,
        keep_original_fields,
        skip_empty_results,
        iff,
    })
}

impl Pipe for PipeExtractRegexp {
    /// Port of Go `pipeExtractRegexp.splitToRemoteAndLocal`: the pipe runs fully
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

    /// Go `visitSubqueries` for this pipe: propagates into the `if (...)` filter.
    fn visit_subqueries_mut(
        &mut self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
        if let Some(iff) = &self.iff
            && let Some(iff_new) = iff.visit_subqueries_mut(timestamp, visit)
        {
            self.iff = Some(iff_new);
        }
    }

    fn to_string(&self) -> String {
        let mut s = String::from("extract_regexp");
        if let Some(iff) = &self.iff {
            s += &format!(" {iff}");
        }
        s += &format!(" {}", quote_token_if_needed(&self.re_str));
        if !crate::filter_generic::is_msg_field_name(&self.from_field) {
            s += &format!(" from {}", quote_token_if_needed(&self.from_field));
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
        let pf_orig = pf.clone();
        let mut need_from_field = false;
        for f in &self.re_fields {
            if f.is_empty() {
                continue;
            }
            if pf_orig.match_string(f) {
                need_from_field = true;
                if should_deny_overwritten_field(
                    self.iff.as_ref(),
                    self.keep_original_fields,
                    self.skip_empty_results,
                ) {
                    pf.add_deny_filter(f);
                }
            }
        }
        if need_from_field {
            pf.add_allow_filter(&self.from_field);
            if let Some(iff) = &self.iff {
                pf.add_allow_filters(&iff.allow_filters);
            }
        }
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
        Arc::new(PipeExtractRegexpProcessor {
            from_field: self.from_field.clone(),
            re: self.re.clone(),
            re_fields: self.re_fields.clone(),
            keep_original_fields: self.keep_original_fields,
            skip_empty_results: self.skip_empty_results,
            has_iff: self.iff.is_some(),
            iff: self.iff.clone(),
            pp_next,
            shards: Slice::default(),
        })
    }
}

struct PipeExtractRegexpProcessor {
    from_field: String,
    re: Regex,
    re_fields: Vec<String>,
    keep_original_fields: bool,
    skip_empty_results: bool,
    has_iff: bool,
    iff: Option<IfFilter>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Slice<std::sync::Mutex<PipeExtractRegexpProcessorShard>>,
}

#[derive(Default)]
struct PipeExtractRegexpProcessorShard {
    bm: Bitmap,
    result_columns: Vec<Option<ColRef>>,
    result_values: Vec<String>,
    rcs: Vec<ResultColumn>,
    fields: Vec<String>,
}

impl PipeExtractRegexpProcessor {
    /// Port of Go's `pipeExtractRegexpProcessorShard.apply`.
    fn apply(&self, v: &str, fields: &mut Vec<String>) {
        let nfields = self.re_fields.len();
        fields.clear();
        fields.resize(nfields, String::new());
        if let Some(caps) = self.re.captures(v) {
            for (i, slot) in fields.iter_mut().enumerate() {
                if let Some(m) = caps.get(i) {
                    *slot = m.as_str().to_string();
                }
            }
        }
    }
}

impl PipeProcessor for PipeExtractRegexpProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let shard_arc = self.shards.get(worker_id);
        let mut guard = shard_arc.lock().unwrap();

        if let Some(iff) = &self.iff {
            guard.bm.init(br.rows_len());
            guard.bm.set_bits();
            iff.f.apply_to_block_result(br, &mut guard.bm);
            if guard.bm.is_zero() {
                self.pp_next.write_block(worker_id, br);
                return;
            }
        }

        let re_fields = &self.re_fields;
        let nfields = re_fields.len();

        let PipeExtractRegexpProcessorShard {
            bm,
            result_columns,
            result_values,
            rcs,
            fields,
        } = &mut *guard;

        rcs.clear();
        for f in re_fields {
            append_result_column_with_name(rcs, f);
        }

        let c = br.get_column_by_name(&self.from_field);
        let values: Vec<String> = br
            .column_get_values(c)
            .iter()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect();

        result_columns.clear();
        for f in re_fields {
            if f.is_empty() {
                result_columns.push(None);
            } else {
                result_columns.push(Some(br.get_column_by_name(f)));
            }
        }

        result_values.clear();
        result_values.resize(nfields, String::new());

        let mut need_updates = true;
        let mut v_prev = String::new();
        for (row_idx, v) in values.iter().enumerate() {
            if !self.has_iff || bm.is_set_bit(row_idx) {
                if need_updates || &v_prev != v {
                    v_prev = v.clone();
                    need_updates = false;

                    self.apply(v, fields);
                    for i in 0..nfields {
                        if re_fields[i].is_empty() {
                            continue;
                        }
                        let mut val = std::mem::take(&mut fields[i]);
                        let want_original = (val.is_empty() && self.skip_empty_results)
                            || self.keep_original_fields;
                        if let (true, Some(rc_col)) = (want_original, result_columns[i]) {
                            let v_orig = br.column_get_value_at_row(rc_col, row_idx).to_string();
                            if !v_orig.is_empty() {
                                val = v_orig;
                            }
                        }
                        result_values[i] = val;
                    }
                }
            } else {
                for i in 0..nfields {
                    if let Some(rc_col) = result_columns[i] {
                        result_values[i] = br.column_get_value_at_row(rc_col, row_idx).to_string();
                    }
                }
                need_updates = true;
            }

            for i in 0..nfields {
                if !re_fields[i].is_empty() {
                    rcs[i].add_value(result_values[i].as_bytes());
                }
            }
        }

        for (i, rc) in rcs.drain(..).enumerate() {
            if !re_fields[i].is_empty() {
                br.add_result_column(rc);
            }
        }
        self.pp_next.write_block(worker_id, br);
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

    fn run(pipe: PipeExtractRegexp, input: &[&[(&str, &str)]], expected: &[&[(&str, &str)]]) {
        run_pipe(Arc::new(pipe), &rows(input), &rows(expected));
    }

    fn build(pattern: &str, from_field: &str, keep: bool, skip: bool) -> PipeExtractRegexp {
        new_pipe_extract_regexp(pattern, from_field, keep, skip, None).unwrap()
    }

    // PORT NOTE: the `if (...)` runtime cases from Go's TestPipeExtractRegexp are
    // deferred — they need the lexer/filter parser to build the `if` filter.

    #[test]
    fn test_pipe_extract_regexp_skip_empty_results() {
        run(
            build("baz=(?P<abc>.*) a=(?P<aa>.*)", "_msg", false, true),
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a="#),
                ("aa", "foobar"),
                ("abc", "ippl"),
            ]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a="#),
                ("aa", "foobar"),
                ("abc", r#""x y=z""#),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_regexp_no_skip_empty_results() {
        run(
            build("baz=(?P<abc>.*) a=(?P<aa>.*)", "_msg", false, false),
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a="#),
                ("aa", "foobar"),
                ("abc", "ippl"),
            ]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a="#),
                ("aa", ""),
                ("abc", r#""x y=z""#),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_regexp_keep_original_fields() {
        run(
            build("baz=(?P<abc>.*) a=(?P<aa>.*)", "_msg", true, false),
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("aa", "foobar"),
                ("abc", ""),
            ]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("abc", r#""x y=z""#),
                ("aa", "foobar"),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_regexp_no_keep_original_fields() {
        run(
            build("baz=(?P<abc>.*) a=(?P<aa>.*)", "_msg", false, false),
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("aa", "foobar"),
                ("abc", ""),
            ]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("abc", r#""x y=z""#),
                ("aa", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_regexp_from_msg() {
        run(
            build("baz=(?P<abc>.*) a=(?P<aa>.*)", "_msg", false, false),
            &[&[("_msg", r#"foo=bar baz="x y=z" a=b"#)]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("abc", r#""x y=z""#),
                ("aa", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_regexp_into_msg() {
        run(
            build("msg=(?P<_msg>.*)", "_msg", false, false),
            &[&[("_msg", "msg=bar")]],
            &[&[("_msg", "bar")]],
        );
    }

    #[test]
    fn test_pipe_extract_regexp_from_non_existing_field() {
        run(
            build("foo=(?P<bar>.*)", "x", false, false),
            &[&[("_msg", "foo=bar")]],
            &[&[("_msg", "foo=bar"), ("bar", "")]],
        );
    }

    #[test]
    fn test_pipe_extract_regexp_pattern_mismatch() {
        run(
            build("foo=(?P<bar>.*)", "x", false, false),
            &[&[("x", "foobar")]],
            &[&[("x", "foobar"), ("bar", "")]],
        );
    }

    #[test]
    fn test_pipe_extract_regexp_partial_match() {
        run(
            build("foo=(?P<bar>.*) baz=(?P<xx>.*)", "x", false, false),
            &[&[("x", r#"a foo="a\"b\\c" cde baz=aa"#)]],
            &[&[
                ("x", r#"a foo="a\"b\\c" cde baz=aa"#),
                ("bar", r#""a\"b\\c" cde"#),
                ("xx", "aa"),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_regexp_overwrite_existing_column() {
        run(
            build("foo=(?P<bar>.*) baz=(?P<xx>.*)", "x", false, false),
            &[&[("x", "a foo=cc baz=aa b"), ("bar", "abc")]],
            &[&[("x", "a foo=cc baz=aa b"), ("bar", "cc"), ("xx", "aa b")]],
        );
    }

    #[test]
    fn test_pipe_extract_regexp_dot_matches_newline() {
        run(
            build("Query text: (?P<query>.+)", "_msg", false, false),
            &[&[(
                "_msg",
                "Query text: SELECT * FROM public.feed_posts\nORDER BY x",
            )]],
            &[&[
                (
                    "_msg",
                    "Query text: SELECT * FROM public.feed_posts\nORDER BY x",
                ),
                ("query", "SELECT * FROM public.feed_posts\nORDER BY x"),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_regexp_disable_dot_newline() {
        run(
            build("(?-s)Query text: (?P<query>.+)", "_msg", false, false),
            &[&[(
                "_msg",
                "Query text: SELECT * FROM public.feed_posts\nORDER BY x",
            )]],
            &[&[
                (
                    "_msg",
                    "Query text: SELECT * FROM public.feed_posts\nORDER BY x",
                ),
                ("query", "SELECT * FROM public.feed_posts"),
            ]],
        );
    }
}

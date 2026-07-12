//! Port of EsLogs `lib/logstorage/pipe_extract.go`.
//!
//! `| extract "pattern" ...` pulls sub-fields out of a source field using a
//! [`crate::pattern::Pattern`]. Also hosts [`should_deny_overwritten_field`],
//! shared with [`crate::pipe_extract_regexp`].

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use esl_common::atomicutil::Slice;

use crate::bitmap::Bitmap;
use crate::block_result::{BlockResult, ColRef, ResultColumn, append_result_column_with_name};
use crate::pattern::{Pattern, parse_pattern};
use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_unpack::IfFilter;
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;

/// Port of Go's `shouldDenyOverwrittenField` (from `pipe_update.go`).
pub(crate) fn should_deny_overwritten_field(
    iff: Option<&IfFilter>,
    keep_original_fields: bool,
    skip_empty_results: bool,
) -> bool {
    iff.is_none() && !keep_original_fields && !skip_empty_results
}

/// `| extract ...` pipe (Go `pipeExtract`).
pub(crate) struct PipeExtract {
    from_field: String,
    ptn: Pattern,
    pattern_str: String,
    keep_original_fields: bool,
    skip_empty_results: bool,
    iff: Option<IfFilter>,
}

/// Constructs a `PipeExtract`, parsing `pattern_str` via [`parse_pattern`] (Go
/// `parsePipeExtract`; lexer parsing of the surrounding pipe is deferred).
pub(crate) fn new_pipe_extract(
    pattern_str: &str,
    from_field: impl Into<String>,
    keep_original_fields: bool,
    skip_empty_results: bool,
    iff: Option<IfFilter>,
) -> Result<PipeExtract, String> {
    let ptn = parse_pattern(pattern_str)
        .map_err(|e| format!("cannot parse 'pattern' {pattern_str:?}: {e}"))?;
    Ok(PipeExtract {
        from_field: from_field.into(),
        ptn,
        pattern_str: pattern_str.to_string(),
        keep_original_fields,
        skip_empty_results,
        iff,
    })
}

impl Pipe for PipeExtract {
    /// Port of Go `pipeExtract.splitToRemoteAndLocal`: the pipe runs fully
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

    fn to_string(&self) -> String {
        let mut s = String::from("extract");
        if let Some(iff) = &self.iff {
            s += &format!(" {iff}");
        }
        s += &format!(" {}", quote_token_if_needed(&self.pattern_str));
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
        for step in &self.ptn.steps {
            if step.field.is_empty() {
                continue;
            }
            if pf_orig.match_string(&step.field) {
                need_from_field = true;
                if should_deny_overwritten_field(
                    self.iff.as_ref(),
                    self.keep_original_fields,
                    self.skip_empty_results,
                ) {
                    pf.add_deny_filter(&step.field);
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
        Arc::new(PipeExtractProcessor {
            from_field: self.from_field.clone(),
            ptn: self.ptn.clone(),
            keep_original_fields: self.keep_original_fields,
            skip_empty_results: self.skip_empty_results,
            has_iff: self.iff.is_some(),
            iff: self.iff.clone(),
            pp_next,
            shards: Slice::default(),
        })
    }
}

struct PipeExtractProcessor {
    from_field: String,
    ptn: Pattern,
    keep_original_fields: bool,
    skip_empty_results: bool,
    has_iff: bool,
    iff: Option<IfFilter>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Slice<std::sync::Mutex<PipeExtractProcessorShard>>,
}

#[derive(Default)]
struct PipeExtractProcessorShard {
    bm: Bitmap,
    ptn: Option<Pattern>,
    result_columns: Vec<ColRef>,
    result_values: Vec<String>,
    rcs: Vec<ResultColumn>,
}

impl PipeProcessor for PipeExtractProcessor {
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

        if guard.ptn.is_none() {
            guard.ptn = Some(self.ptn.clone());
        }
        let PipeExtractProcessorShard {
            bm,
            ptn,
            result_columns,
            result_values,
            rcs,
        } = &mut *guard;
        let ptn = ptn.as_mut().unwrap();

        let nfields = ptn.fields.len();
        rcs.clear();
        for f in &ptn.fields {
            append_result_column_with_name(rcs, &f.name);
        }

        let c = br.get_column_by_name(&self.from_field);
        let values: Vec<String> = br
            .column_get_values(c)
            .iter()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect();

        result_columns.clear();
        for f in &ptn.fields {
            result_columns.push(br.get_column_by_name(&f.name));
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

                    ptn.apply(v);
                    let field_vals: Vec<String> = ptn
                        .fields
                        .iter()
                        .map(|f| ptn.field_value(f).to_string())
                        .collect();
                    for (i, mut val) in field_vals.into_iter().enumerate() {
                        if (val.is_empty() && self.skip_empty_results) || self.keep_original_fields
                        {
                            let v_orig = br
                                .column_get_value_at_row(result_columns[i], row_idx)
                                .to_string();
                            if !v_orig.is_empty() {
                                val = v_orig;
                            }
                        }
                        result_values[i] = val;
                    }
                }
            } else {
                for (i, &rc_col) in result_columns.iter().enumerate() {
                    result_values[i] = br.column_get_value_at_row(rc_col, row_idx).to_string();
                }
                need_updates = true;
            }

            for (i, val) in result_values.iter().enumerate() {
                rcs[i].add_value(val.as_bytes());
            }
        }

        for rc in rcs.drain(..) {
            br.add_result_column(rc);
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

    fn run(pipe: PipeExtract, input: &[&[(&str, &str)]], expected: &[&[(&str, &str)]]) {
        run_pipe(Arc::new(pipe), &rows(input), &rows(expected));
    }

    fn build(pattern: &str, from_field: &str, keep: bool, skip: bool) -> PipeExtract {
        new_pipe_extract(pattern, from_field, keep, skip, None).unwrap()
    }

    // PORT NOTE: the `if (...)` runtime cases from Go's TestPipeExtract are
    // deferred — they need the lexer/filter parser to build the `if` filter.

    #[test]
    fn test_pipe_extract_skip_empty_results() {
        run(
            build("baz=<abc> a=<aa>", "_msg", false, true),
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" "#),
                ("aa", "foobar"),
                ("abc", "ippl"),
            ]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" "#),
                ("aa", "foobar"),
                ("abc", "x y=z"),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_no_skip_empty_results() {
        run(
            build("baz=<abc> a=<aa>", "_msg", false, false),
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" "#),
                ("aa", "foobar"),
                ("abc", "ippl"),
            ]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" "#),
                ("aa", ""),
                ("abc", "x y=z"),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_keep_original_fields() {
        run(
            build("baz=<abc> a=<aa>", "_msg", true, false),
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("aa", "foobar"),
                ("abc", ""),
            ]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("abc", "x y=z"),
                ("aa", "foobar"),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_no_keep_original_fields() {
        run(
            build("baz=<abc> a=<aa>", "_msg", false, false),
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("aa", "foobar"),
                ("abc", ""),
            ]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("abc", "x y=z"),
                ("aa", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_from_msg() {
        run(
            build("baz=<abc> a=<aa>", "_msg", false, false),
            &[&[("_msg", r#"foo=bar baz="x y=z" a=b"#)]],
            &[&[
                ("_msg", r#"foo=bar baz="x y=z" a=b"#),
                ("abc", "x y=z"),
                ("aa", "b"),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_into_msg() {
        run(
            build("msg=<_msg>", "_msg", false, false),
            &[&[("_msg", "msg=bar")]],
            &[&[("_msg", "bar")]],
        );
    }

    #[test]
    fn test_pipe_extract_from_non_existing_field() {
        run(
            build("foo=<bar>", "x", false, false),
            &[&[("_msg", "foo=bar")]],
            &[&[("_msg", "foo=bar"), ("bar", "")]],
        );
    }

    #[test]
    fn test_pipe_extract_pattern_mismatch() {
        run(
            build("foo=<bar>", "x", false, false),
            &[&[("x", "foobar")]],
            &[&[("x", "foobar"), ("bar", "")]],
        );
    }

    #[test]
    fn test_pipe_extract_partial_pattern_match() {
        run(
            build("foo=<bar> baz=<xx>", "x", false, false),
            &[&[("x", r#"a foo="a\"b\\c" cde baz=aa"#)]],
            &[&[
                ("x", r#"a foo="a\"b\\c" cde baz=aa"#),
                ("bar", r#"a"b\c"#),
                ("xx", ""),
            ]],
        );
    }

    #[test]
    fn test_pipe_extract_disable_unquoting() {
        run(
            build("foo=[< plain : bar >]", "x", false, false),
            &[&[("x", r#"a foo=["bc","de"]"#)]],
            &[&[("x", r#"a foo=["bc","de"]"#), ("bar", r#""bc","de""#)]],
        );
    }

    #[test]
    fn test_pipe_extract_default_unquoting() {
        run(
            build("foo=[< bar >]", "x", false, false),
            &[&[("x", r#"a foo=["bc","de"]"#)]],
            &[&[("x", r#"a foo=["bc","de"]"#), ("bar", "bc")]],
        );
    }

    #[test]
    fn test_pipe_extract_overwrite_existing_column() {
        run(
            build("foo=<bar> baz=<xx>", "x", false, false),
            &[&[("x", "a foo=cc baz=aa b"), ("bar", "abc")]],
            &[&[("x", "a foo=cc baz=aa b"), ("bar", "cc"), ("xx", "aa b")]],
        );
    }
}

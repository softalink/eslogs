//! Port of `pipe_fields.go` from EsLogs v1.51.0.
//!
//! Implements the `| fields ...` (aka `| keep ...`) pipe, which keeps only the
//! fields matching the configured field filters.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::block_result::BlockResult;
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stats_count::field_names_string;

use esl_common::panicf;

/// Returns true if any of the given filters is a wildcard filter.
///
/// PORT NOTE: mirrors Go's `hasWildcardFilters` (block_result.go), which is not
/// `pub` in the Rust `block_result` port; homed here for the field pipes.
pub(crate) fn has_wildcard_filters(filters: &[String]) -> bool {
    filters.iter().any(|f| prefix_filter::is_wildcard_filter(f))
}

/// `PipeFields` implements the `| fields ...` pipe.
///
/// See <https://docs.victoriametrics.com/victorialogs/logsql/#fields-pipe>
pub(crate) struct PipeFields {
    /// List of field filters for the fields to fetch.
    pub(crate) field_filters: Vec<String>,
}

/// Builds a `| fields ...` pipe keeping the given field filters.
///
/// PORT NOTE: the Go `parsePipeFields` reads the LogsQL lexer, which is not yet
/// ported. This constructor exposes the parsed result for the future parser.
pub(crate) fn new_pipe_fields(field_filters: Vec<String>) -> PipeFields {
    PipeFields { field_filters }
}

impl Pipe for PipeFields {
    fn is_fields_or_delete_pipe(&self) -> bool {
        true
    }

    fn to_string(&self) -> String {
        if self.field_filters.is_empty() {
            panicf!("BUG: pipeFields must contain at least a single field filter");
        }
        format!("fields {}", field_names_string(&self.field_filters))
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        prefix_filter::match_filters(&self.field_filters, "_time")
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        !has_wildcard_filters(&self.field_filters)
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        Some(crate::pipe::StatsTailOp::Fields {
            field_filters: self.field_filters.clone(),
        })
    }

    /// Port of Go `pipeFields.resultFields` (`None` for wildcard filters).
    fn fixed_result_fields(&self) -> Option<Vec<String>> {
        if has_wildcard_filters(&self.field_filters) {
            return None;
        }
        Some(self.field_filters.clone())
    }

    /// Go `getFieldNameFromPipes`' `*pipeFields` arm.
    fn in_query_field_name(&self) -> Option<Result<String, String>> {
        // Go isSingleField(t.fieldFilters).
        if self.field_filters.len() != 1
            || crate::prefix_filter::is_wildcard_filter(&self.field_filters[0])
        {
            return Some(Err(format!(
                "'{}' pipe must contain only a single field name",
                Pipe::to_string(self)
            )));
        }
        Some(Ok(self.field_filters[0].clone()))
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        let f_orig = pf.clone();
        pf.reset();

        for filter in &self.field_filters {
            if f_orig.match_string_or_wildcard(filter) {
                pf.add_allow_filter(filter);
            }
        }
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        Arc::new(PipeFieldsProcessor {
            field_filters: self.field_filters.clone(),
            pp_next,
        })
    }
}

struct PipeFieldsProcessor {
    field_filters: Vec<String>,
    pp_next: Arc<dyn PipeProcessor>,
}

impl PipeProcessor for PipeFieldsProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        br.set_column_filters(&self.field_filters);
        self.pp_next.write_block(worker_id, br);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared test harness for the field/transform pipe ports.
// ---------------------------------------------------------------------------

// PORT NOTE: the Go `expectParsePipeSuccess`/`expectPipeResults`/
// `expectPipeNeededFields` helpers drive the LogsQL lexer/parser. The parser is
// not yet ported, so the `TestParsePipe*` cases are deferred. The pipe behavior
// is covered by constructing pipes directly via `new_pipe_*` and driving them
// with the harness below (a faithful port of `pipe_utils_test.go` minus the
// parser).
#[cfg(test)]
pub(crate) mod pipe_test_util {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use crate::block_result::BlockResult;
    use crate::pipe::{Pipe, PipeProcessor};
    use crate::prefix_filter;
    use crate::rows::Field;

    pub(crate) fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    /// Terminal processor that records every row it receives.
    #[derive(Default)]
    pub(crate) struct CollectingProcessor {
        rows: Mutex<Vec<Vec<Field>>>,
    }

    impl CollectingProcessor {
        pub(crate) fn rows(&self) -> Vec<Vec<Field>> {
            self.rows.lock().unwrap().clone()
        }
    }

    impl PipeProcessor for CollectingProcessor {
        fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
            let cols = br.get_columns();
            let names: Vec<String> = cols
                .iter()
                .map(|&c| br.column_name(c).to_string())
                .collect();
            let mut col_values: Vec<Vec<String>> = Vec::with_capacity(cols.len());
            for &c in &cols {
                let vals = br.column_get_values(c);
                col_values.push(
                    vals.iter()
                        .map(|v| String::from_utf8_lossy(v).into_owned())
                        .collect(),
                );
            }
            let rows_len = br.rows_len();
            let mut out = self.rows.lock().unwrap();
            for i in 0..rows_len {
                let row: Vec<Field> = names
                    .iter()
                    .zip(&col_values)
                    .map(|(name, vals)| Field {
                        name: name.clone(),
                        value: vals[i].clone(),
                    })
                    .collect();
                out.push(row);
            }
        }

        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    /// Splits `rows` into blocks of consecutive rows sharing the same field
    /// names in the same order (mirrors `testBlockResultWriter.areSameFields`).
    fn split_blocks(rows: &[Vec<Field>]) -> Vec<Vec<Vec<Field>>> {
        let mut blocks: Vec<Vec<Vec<Field>>> = Vec::new();
        for row in rows {
            let same = blocks.last().is_some_and(|b| {
                let first = &b[0];
                first.len() == row.len() && first.iter().zip(row).all(|(a, c)| a.name == c.name)
            });
            if same {
                blocks.last_mut().unwrap().push(row.clone());
            } else {
                blocks.push(vec![row.clone()]);
            }
        }
        blocks
    }

    /// Runs `p` over `rows` (grouped into blocks) and returns the produced rows.
    pub(crate) fn run_pipe(p: &dyn Pipe, rows: &[Vec<Field>]) -> Vec<Vec<Field>> {
        let collector = Arc::new(CollectingProcessor::default());
        let stop = Arc::new(AtomicBool::new(false));
        let pp = p.new_pipe_processor(1, stop, collector.clone());
        for block in split_blocks(rows) {
            let mut br = BlockResult::default();
            br.must_init_from_rows(&block);
            // Prime timestamps so pipes that truncate/skip rows (limit/offset)
            // have a populated timestamp buffer, mirroring real query blocks
            // (which always carry `_time`). `get_timestamps` fills zeros when no
            // `_time` column is present, without adding a visible column.
            br.get_timestamps();
            pp.write_block(0, &mut br);
        }
        pp.flush().unwrap();
        collector.rows()
    }

    fn sorted_key(row: &[Field]) -> Vec<(String, String)> {
        let mut k: Vec<(String, String)> = row
            .iter()
            .map(|f| (f.name.clone(), f.value.clone()))
            .collect();
        k.sort();
        k
    }

    /// Asserts two row sets are equal ignoring row and field order (mirrors
    /// Go's `assertRowsEqual`).
    pub(crate) fn assert_rows_eq(got: Vec<Vec<Field>>, expected: &[Vec<Field>]) {
        let mut got_keys: Vec<Vec<(String, String)>> = got.iter().map(|r| sorted_key(r)).collect();
        let mut want_keys: Vec<Vec<(String, String)>> =
            expected.iter().map(|r| sorted_key(r)).collect();
        got_keys.sort();
        want_keys.sort();
        assert_eq!(got_keys, want_keys, "\n got: {got:?}\nwant: {expected:?}");
    }

    fn quote_strings(s: &str) -> String {
        if s.is_empty() {
            return String::new();
        }
        let mut a: Vec<String> = s.split(',').map(|v| format!("{v:?}")).collect();
        a.sort();
        a.join(",")
    }

    fn new_test_fields_filter(allow: &str, deny: &str) -> prefix_filter::Filter {
        let mut pf = prefix_filter::Filter::default();
        if !allow.is_empty() {
            let filters: Vec<&str> = allow.split(',').collect();
            pf.add_allow_filters(&filters);
        }
        if !deny.is_empty() {
            let filters: Vec<&str> = deny.split(',').collect();
            pf.add_deny_filters(&filters);
        }
        pf
    }

    /// Faithful port of `expectPipeNeededFields`, but builds the pipe directly
    /// instead of parsing it.
    pub(crate) fn expect_needed_fields(
        p: &dyn Pipe,
        allow: &str,
        deny: &str,
        allow_expected: &str,
        deny_expected: &str,
    ) {
        let mut pf = new_test_fields_filter(allow, deny);
        p.update_needed_fields(&mut pf);
        let got = pf.to_string();
        let want = format!(
            "allow=[{}], deny=[{}]",
            quote_strings(allow_expected),
            quote_strings(deny_expected)
        );
        assert_eq!(got, want);
    }
}

#[cfg(test)]
mod tests {
    use super::pipe_test_util::*;
    use super::*;

    fn pf(filters: &[&str]) -> PipeFields {
        new_pipe_fields(filters.iter().map(|s| s.to_string()).collect())
    }

    #[test]
    fn test_pipe_fields_string() {
        assert_eq!(pf(&["f1", "f2", "f3"]).to_string(), "fields f1, f2, f3");
        assert_eq!(pf(&["*"]).to_string(), "fields *");
    }

    #[test]
    fn test_pipe_fields_single_row_star() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&pf(&["*"]), &rows);
        assert_rows_eq(got, &rows);
    }

    #[test]
    fn test_pipe_fields_keep_existing() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&pf(&["a"]), &rows);
        assert_rows_eq(got, &[vec![field("a", "test")]]);
    }

    #[test]
    fn test_pipe_fields_non_existing() {
        let rows = vec![vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")]];
        let got = run_pipe(&pf(&["x", "y"]), &rows);
        assert_rows_eq(got, &[vec![field("x", ""), field("y", "")]]);
    }

    #[test]
    fn test_pipe_fields_wildcard_prefix() {
        let rows = vec![
            vec![
                field("de", "123234"),
                field("a", "qwe"),
                field("abc", "123"),
                field("bc", "12332423"),
                field("b", "sdfds"),
            ],
            vec![
                field("de", "ioi"),
                field("bc", "12332423"),
                field("aaa", "pdd"),
            ],
            vec![field("bc", "fd")],
        ];
        let got = run_pipe(&pf(&["a*", "b"]), &rows);
        assert_rows_eq(
            got,
            &[
                vec![field("a", "qwe"), field("abc", "123"), field("b", "sdfds")],
                vec![field("aaa", "pdd"), field("b", "")],
                vec![field("b", "")],
            ],
        );
    }

    #[test]
    fn test_pipe_fields_multiple_rows() {
        let rows = vec![
            vec![field("_msg", r#"{"foo":"bar"}"#), field("a", "test")],
            vec![field("a", "foobar")],
            vec![field("b", "baz"), field("c", "d"), field("e", "afdf")],
            vec![field("c", "dss"), field("d", "df")],
        ];
        let got = run_pipe(&pf(&["a", "b"]), &rows);
        assert_rows_eq(
            got,
            &[
                vec![field("a", "test"), field("b", "")],
                vec![field("a", "foobar"), field("b", "")],
                vec![field("a", ""), field("b", "baz")],
                vec![field("a", ""), field("b", "")],
            ],
        );
    }

    #[test]
    fn test_pipe_fields_update_needed_fields() {
        // all the needed fields
        expect_needed_fields(&pf(&["s1", "s2"]), "*", "", "s1,s2", "");
        expect_needed_fields(&pf(&["*"]), "*", "", "*", "");
        expect_needed_fields(&pf(&["a*"]), "*", "", "a*", "");
        expect_needed_fields(&pf(&["a*", "b"]), "*", "", "a*,b", "");

        // unneeded fields do not intersect with src
        expect_needed_fields(&pf(&["s1", "s2"]), "*", "f1,f2", "s1,s2", "");
        expect_needed_fields(&pf(&["s1", "s2"]), "*", "f*", "s1,s2", "");
        expect_needed_fields(&pf(&["*"]), "*", "f1,f2", "*", "");
        expect_needed_fields(&pf(&["a*"]), "*", "f1,f2", "a*", "");

        // unneeded fields intersect with src
        expect_needed_fields(&pf(&["s1", "s2"]), "*", "s1,f1,f2", "s2", "");
        expect_needed_fields(&pf(&["s1", "s2"]), "*", "s*,f*", "", "");
        expect_needed_fields(&pf(&["s1", "s2"]), "*", "s2*,f*", "s1", "");
        expect_needed_fields(&pf(&["*"]), "*", "s1,f1,f2", "*", "");
        expect_needed_fields(&pf(&["f*"]), "*", "s1,f1,f2", "f*", "");
        expect_needed_fields(&pf(&["f*"]), "*", "s*,f*", "", "");

        // needed fields do not intersect with src
        expect_needed_fields(&pf(&["s1", "s2"]), "f1,f2", "", "", "");
        expect_needed_fields(&pf(&["s1", "s2"]), "f*", "", "", "");
        expect_needed_fields(&pf(&["s*", "s2"]), "f1,f2", "", "", "");
        expect_needed_fields(&pf(&["s*", "s2"]), "f*", "", "", "");

        // needed fields intersect with src
        expect_needed_fields(&pf(&["s1", "s2"]), "s1,f1,f2", "", "s1", "");
        expect_needed_fields(&pf(&["s1", "s2"]), "s*,f*", "", "s1,s2", "");
        expect_needed_fields(&pf(&["*"]), "s1,f1,f2", "", "*", "");
        expect_needed_fields(
            &pf(&["s*", "s1*", "d", "f", "foo*", "bar*"]),
            "s1,f*",
            "",
            "f,foo*,s*",
            "",
        );
    }
}

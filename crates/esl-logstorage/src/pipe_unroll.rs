//! Port of `pipe_unroll.go` — the `| unroll ...` pipe, which expands one or more
//! fields holding JSON arrays into multiple output rows (one row per array
//! element), copying the remaining fields of the source row unchanged.
//!
//! See <https://docs.victoriametrics.com/victorialogs/logsql/#unroll-pipe>

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::bitmap::Bitmap;
use crate::block_result::BlockResult;
use crate::json_parser::fastjson;
use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_update::IfFilter;
use crate::prefix_filter;
use crate::rows::Field;
use crate::stats_count_uniq::field_names_string;

/// `pipeUnroll` implements `| unroll ...`.
pub struct PipeUnroll {
    /// fields to unroll
    pub(crate) fields: Vec<Vec<u8>>,

    /// optional filter for skipping the unroll
    pub(crate) iff: Option<Arc<IfFilter>>,
}

/// Constructs an `unroll` pipe from already-parsed components.
///
/// PORT NOTE: Go's `parsePipeUnroll` is lexer-dependent and deferred; this
/// constructor takes the parsed fields and optional `if` filter directly. The
/// parser guarantees `fields` is non-empty and free of `*`.
pub(crate) fn new_pipe_unroll(fields: Vec<Vec<u8>>, iff: Option<Arc<IfFilter>>) -> PipeUnroll {
    PipeUnroll { fields, iff }
}

impl Pipe for PipeUnroll {
    /// Port of Go `pipeUnroll.splitToRemoteAndLocal`: the pipe runs fully
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
            self.iff = Some(Arc::new(iff_new));
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
            self.iff = Some(Arc::new(iff_new));
        }
    }

    fn to_string(&self) -> String {
        let mut s = String::from("unroll");
        if let Some(iff) = &self.iff {
            s += " ";
            s += &iff.to_string();
        }
        s += " by (";
        s += &field_names_string(&self.fields);
        s += ")";
        s
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        true
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        if let Some(iff) = &self.iff {
            pf.add_allow_filters(&iff.allow_filters);
        }
        pf.add_allow_filters(&self.fields);
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeUnrollProcessorShard::default()))
            .collect();
        Arc::new(PipeUnrollProcessor {
            fields: self.fields.clone(),
            iff: self.iff.clone(),
            stop,
            pp_next,
            shards,
        })
    }
}

struct PipeUnrollProcessor {
    fields: Vec<Vec<u8>>,
    iff: Option<Arc<IfFilter>>,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeUnrollProcessorShard>>,
}

#[derive(Default)]
struct PipeUnrollProcessorShard {
    bm: Bitmap,
}

impl PipeProcessor for PipeUnrollProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let mut shard = self.shards[worker_id].lock().unwrap();

        let has_iff = self.iff.is_some();
        if let Some(iff) = &self.iff {
            shard.bm.init(br.rows_len());
            shard.bm.set_bits();
            iff.f.apply_to_block_result(br, &mut shard.bm);
            if shard.bm.is_zero() {
                drop(shard);
                self.pp_next.write_block(worker_id, br);
                return;
            }
        }

        // Snapshot the source columns (names first via the immutable accessor,
        // then their decoded values) so we can build output rows while the
        // block borrow is released. See pipe_coalesce.rs for the discipline.
        let cs = br.get_columns();
        let src_names: Vec<Vec<u8>> = cs.iter().map(|&c| br.column_name(c).to_vec()).collect();
        let src_values: Vec<Vec<Vec<u8>>> = cs
            .iter()
            .map(|&c| br.column_get_values(c).to_vec())
            .collect();

        // Snapshot the values of the fields to unroll.
        let field_values: Vec<Vec<Vec<u8>>> = self
            .fields
            .iter()
            .map(|f| {
                let c = br.get_column_by_name(f);
                br.column_get_values(c).to_vec()
            })
            .collect();

        let rows_len = br.rows_len();
        let mut out: Vec<Vec<Field>> = Vec::new();

        for row_idx in 0..rows_len {
            if self.stop.load(Ordering::Relaxed) {
                // Abandon the block without flushing, exactly like Go's
                // `needStop` early return.
                return;
            }

            if !has_iff || shard.bm.is_set_bit(row_idx) {
                self.write_unrolled_rows(&src_names, &src_values, &field_values, row_idx, &mut out);
            } else {
                // The row is not selected by the `if` filter — pass it through
                // with the unroll fields left intact.
                let extra: Vec<Field> = self
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| Field {
                        name: f.clone(),
                        value: field_values[i][row_idx].clone(),
                    })
                    .collect();
                out.push(build_row(&src_names, &src_values, row_idx, &extra));
            }
        }

        drop(shard);

        // PORT NOTE: Go's `pipeUnpackWriteContext` streams the unrolled rows to
        // the next pipe in ~64KB blocks, splitting whenever the column set
        // changes. The port buffers all unrolled rows of the input block and
        // emits them as a single block via `must_init_from_rows`, which handles
        // heterogeneous rows. Output rows (and their contents) are identical;
        // only the block boundaries differ, and results are compared
        // order-independently.
        let mut br_out = BlockResult::default();
        br_out.must_init_from_rows(&out);
        self.pp_next.write_block(worker_id, &mut br_out);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

impl PipeUnrollProcessor {
    /// Port of Go `(*pipeUnrollProcessorShard).writeUnrolledFields`: expands the
    /// unroll fields at `row_idx` into one or more output rows.
    fn write_unrolled_rows(
        &self,
        src_names: &[Vec<u8>],
        src_values: &[Vec<Vec<u8>>],
        field_values: &[Vec<Vec<u8>>],
        row_idx: usize,
        out: &mut Vec<Vec<Field>>,
    ) {
        // Unroll each field value into its own list of elements.
        let mut unrolled: Vec<Vec<Vec<u8>>> = Vec::with_capacity(self.fields.len());
        for fv in field_values {
            let mut arr = Vec::new();
            unpack_json_array(&mut arr, &fv[row_idx]);
            unrolled.push(arr);
        }

        // The number of output rows is the max element count across fields.
        let mut nrows = unrolled[0].len();
        for values in &unrolled[1..] {
            if values.len() > nrows {
                nrows = values.len();
            }
        }
        if nrows == 0 {
            // Unroll to a single row with empty unrolled values.
            nrows = 1;
        }

        for unroll_idx in 0..nrows {
            let extra: Vec<Field> = self
                .fields
                .iter()
                .enumerate()
                .map(|(i, name)| Field {
                    name: name.clone(),
                    value: unrolled[i].get(unroll_idx).cloned().unwrap_or_default(),
                })
                .collect();
            out.push(build_row(src_names, src_values, row_idx, &extra));
        }
    }
}

/// Builds a single output row: the source columns at `row_idx` overlaid with the
/// `extra` fields.
///
/// PORT NOTE: Go appends the extra fields as additional result columns and
/// relies on `blockResult`'s by-name column dedup (last write wins, kept at the
/// first column's position) to collapse an unroll field that also exists as a
/// source column. The port merges directly: an extra field replaces the value
/// of the same-named source column in place, otherwise it is appended.
fn build_row(
    src_names: &[Vec<u8>],
    src_values: &[Vec<Vec<u8>>],
    row_idx: usize,
    extra: &[Field],
) -> Vec<Field> {
    let mut row: Vec<Field> = src_names
        .iter()
        .enumerate()
        .map(|(i, name)| Field {
            name: name.clone(),
            value: src_values[i][row_idx].clone(),
        })
        .collect();

    for f in extra {
        if let Some(existing) = row.iter_mut().find(|r| r.name == f.name) {
            existing.value = f.value.clone();
        } else {
            row.push(f.clone());
        }
    }

    row
}

// ---------------------------------------------------------------------------
// JSON array unpacking (Go `unpackJSONArray`).
// ---------------------------------------------------------------------------

thread_local! {
    // PORT NOTE: Go pools `fastjson.Parser` via the package-level `jspp`; the
    // port keeps a thread-local pool so parse buffers are reused across calls.
    static JSON_PARSER_POOL: RefCell<Vec<fastjson::Parser>> = const { RefCell::new(Vec::new()) };
}

fn get_parser() -> fastjson::Parser {
    JSON_PARSER_POOL.with(|p| p.borrow_mut().pop().unwrap_or_default())
}

fn put_parser(p: fastjson::Parser) {
    JSON_PARSER_POOL.with(|pool| pool.borrow_mut().push(p));
}

/// Port of Go `unpackJSONArray`: appends the string representation of each
/// element of the JSON array encoded in `s` to `dst`. Non-array inputs and
/// parse errors append nothing.
fn unpack_json_array(dst: &mut Vec<Vec<u8>>, s: &[u8]) {
    let s = trim_json_whitespace(s);
    if s.is_empty() || !s.starts_with(b"[") {
        return;
    }

    let mut p = get_parser();
    if let Ok(v) = p.parse(s)
        && p.doc.value_type(v) == fastjson::JsonType::Array
    {
        let n = p.doc.array_len(v);
        for i in 0..n {
            let e = p.doc.array_element(v, i);
            if p.doc.value_type(e) == fastjson::JsonType::String {
                let span = p.doc.string_span(e);
                let sb = p.doc.str_bytes(span);
                dst.push(sb.to_vec());
            } else {
                let mut bb = Vec::new();
                p.doc.marshal_value_to(e, &mut bb);
                dst.push(bb);
            }
        }
    }
    put_parser(p);
}

/// Port of Go `trimJSONWhitespace`.
fn trim_json_whitespace(mut s: &[u8]) -> &[u8] {
    let is_ws = |b: u8| b == b' ' || b == b'\t' || b == b'\n' || b == b'\r';
    while let Some(&b) = s.first() {
        if !is_ws(b) {
            break;
        }
        s = &s[1..];
    }
    while let Some(&b) = s.last() {
        if !is_ws(b) {
            break;
        }
        s = &s[..s.len() - 1];
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::Filter;
    use crate::filter_phrase::new_filter_phrase;
    use crate::pipe_update::test_utils::{assert_needed_fields, assert_rows_eq, rows, run_pipe};

    // PORT NOTE: `TestParsePipeUnrollSuccess` / `TestParsePipeUrollFailure`
    // exercise the lexer-based `parsePipeUnroll`, which is deferred; they are
    // omitted until the LogsQL parser is ported.

    fn phrase_iff(field: &str, phrase: &str) -> Arc<IfFilter> {
        let f: Arc<dyn Filter> = Arc::new(new_filter_phrase(field.as_bytes(), phrase));
        Arc::new(IfFilter::new(f))
    }

    fn unroll(fields: &[&str], iff: Option<Arc<IfFilter>>) -> PipeUnroll {
        new_pipe_unroll(fields.iter().map(|s| s.as_bytes().to_vec()).collect(), iff)
    }

    #[test]
    fn test_pipe_unroll() {
        // unroll by missing field
        let p = unroll(&["x"], None);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[&[("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#), ("q", "w")]]),
            ),
            &rows(&[&[
                ("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#),
                ("q", "w"),
                ("x", ""),
            ]]),
        );

        // unroll by field without JSON array
        let p = unroll(&["q"], None);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[&[("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#), ("q", "w")]]),
            ),
            &rows(&[&[("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#), ("q", "")]]),
        );

        // unroll by a single field
        let p = unroll(&["a"], None);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("a", r#"["foo",1,{"baz":"x"},[1,2],null,NaN]"#), ("q", "w")],
                    &[("a", " \t\n\r[\"x\",\"y\"]"), ("q", "z")],
                    &[("a", "b"), ("c", "d")],
                ]),
            ),
            &rows(&[
                &[("a", "foo"), ("q", "w")],
                &[("a", "1"), ("q", "w")],
                &[("a", r#"{"baz":"x"}"#), ("q", "w")],
                &[("a", "[1,2]"), ("q", "w")],
                &[("a", "null"), ("q", "w")],
                &[("a", "NaN"), ("q", "w")],
                &[("a", "x"), ("q", "z")],
                &[("a", "y"), ("q", "z")],
                &[("a", ""), ("c", "d")],
            ]),
        );

        // unroll by multiple fields
        let p = unroll(&["timestamp", "value"], None);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[
                        ("timestamp", "[1,2,3]"),
                        ("value", r#"["foo","bar","baz"]"#),
                        ("other", "abc"),
                        ("x", "y"),
                    ],
                    &[("timestamp", "[1]"), ("value", r#"["foo","bar"]"#)],
                    &[("timestamp", "[1]"), ("value", "bar"), ("q", "w")],
                ]),
            ),
            &rows(&[
                &[
                    ("timestamp", "1"),
                    ("value", "foo"),
                    ("other", "abc"),
                    ("x", "y"),
                ],
                &[
                    ("timestamp", "2"),
                    ("value", "bar"),
                    ("other", "abc"),
                    ("x", "y"),
                ],
                &[
                    ("timestamp", "3"),
                    ("value", "baz"),
                    ("other", "abc"),
                    ("x", "y"),
                ],
                &[("timestamp", "1"), ("value", "foo")],
                &[("timestamp", ""), ("value", "bar")],
                &[("timestamp", "1"), ("value", ""), ("q", "w")],
            ]),
        );

        // conditional unroll by missing field
        let p = unroll(&["a"], Some(phrase_iff("q", "abc")));
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("a", "asd"), ("q", "w")],
                    &[("a", r#"["foo",123]"#), ("q", "abc")],
                ]),
            ),
            &rows(&[
                &[("a", "asd"), ("q", "w")],
                &[("a", "foo"), ("q", "abc")],
                &[("a", "123"), ("q", "abc")],
            ]),
        );

        // unroll by non-existing field
        let p = unroll(&["a"], None);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("a", "asd"), ("q", "w")],
                    &[("a", r#"["foo",123]"#), ("q", "abc")],
                ]),
            ),
            &rows(&[
                &[("a", ""), ("q", "w")],
                &[("a", "foo"), ("q", "abc")],
                &[("a", "123"), ("q", "abc")],
            ]),
        );
    }

    #[test]
    fn test_pipe_unroll_update_needed_fields() {
        // all the needed fields
        let p = unroll(&["x"], None);
        assert_needed_fields(&p, "*", "", "*", "");
        let p = unroll(&["x", "y"], None);
        assert_needed_fields(&p, "*", "", "*", "");
        let p = unroll(&["a", "b"], Some(phrase_iff("y", "z")));
        assert_needed_fields(&p, "*", "", "*", "");

        // all the needed fields, unneeded fields do not intersect with src
        let p = unroll(&["x"], None);
        assert_needed_fields(&p, "*", "f1,f2", "*", "f1,f2");
        let p = unroll(&["x"], Some(phrase_iff("a", "b")));
        assert_needed_fields(&p, "*", "f1,f2", "*", "f1,f2");
        let p = unroll(&["x"], Some(phrase_iff("f1", "b")));
        assert_needed_fields(&p, "*", "f1,f2", "*", "f2");

        // all the needed fields, unneeded fields intersect with src
        let p = unroll(&["x"], None);
        assert_needed_fields(&p, "*", "f2,x", "*", "f2");
        let p = unroll(&["x"], Some(phrase_iff("a", "b")));
        assert_needed_fields(&p, "*", "f2,x", "*", "f2");
        let p = unroll(&["x"], Some(phrase_iff("f2", "b")));
        assert_needed_fields(&p, "*", "f2,x", "*", "");

        // needed fields do not intersect with src
        let p = unroll(&["x"], None);
        assert_needed_fields(&p, "f1,f2", "", "f1,f2,x", "");
        let p = unroll(&["x"], Some(phrase_iff("a", "b")));
        assert_needed_fields(&p, "f1,f2", "", "a,f1,f2,x", "");

        // needed fields intersect with src
        let p = unroll(&["x"], None);
        assert_needed_fields(&p, "f2,x", "", "f2,x", "");
        let p = unroll(&["x"], Some(phrase_iff("a", "b")));
        assert_needed_fields(&p, "f2,x", "", "a,f2,x", "");
    }

    #[test]
    fn test_unpack_json_array() {
        fn u(s: &str) -> Vec<String> {
            let mut dst = Vec::new();
            unpack_json_array(&mut dst, s.as_bytes());
            dst.into_iter()
                .map(|v| String::from_utf8(v).unwrap())
                .collect()
        }

        assert_eq!(u(""), Vec::<String>::new());
        assert_eq!(u("123"), Vec::<String>::new());
        assert_eq!(u("foo"), Vec::<String>::new());
        assert_eq!(u(r#""foo""#), Vec::<String>::new());
        assert_eq!(u(r#"{"foo":"bar"}"#), Vec::<String>::new());
        assert_eq!(u("[foo"), Vec::<String>::new());
        assert_eq!(u("[]"), Vec::<String>::new());
        assert_eq!(u("[1]"), vec!["1".to_string()]);
        assert_eq!(
            u(" \t\n\r[1,2] \r\n\t"),
            vec!["1".to_string(), "2".to_string()]
        );
        assert_eq!(
            u(r#"[1,"foo",["bar",12],{"baz":"x"},NaN,null]"#),
            vec![
                "1".to_string(),
                "foo".to_string(),
                r#"["bar",12]"#.to_string(),
                r#"{"baz":"x"}"#.to_string(),
                "NaN".to_string(),
                "null".to_string(),
            ]
        );
    }
}

//! Port of `pipe_join.go` — the `| join by (...) (<subquery>|rows(...))` pipe,
//! which joins the input rows against the rows produced by a subquery (or by
//! inline `rows(...)`) on the `by` fields.
//!
//! PORT NOTE — subquery representation: Go's `pipeJoin` embeds `q *Query`;
//! the port models it as the already-rendered query text
//! ([`PipeJoin::query_text`]), the established subquery pattern. The join map:
//!   * INLINE-rows path (Go `rows`): built from the inline rows at
//!     construction time (the map-building half of Go `initJoinMap`);
//!   * SUBQUERY path: built by [`Pipe::init_join_map`] — the port of the
//!     subquery half of Go `initJoinMap` — which
//!     `storage_search::init_subqueries` drives before the search, executing
//!     the subquery via its `get_join_rows` callback (Go `getJoinRowsFunc`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use esl_common::encoding;

use crate::block_result::{BlockResult, ColRef, ResultColumn, append_result_column_with_name};
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::rows::{Field, marshal_fields_to_json};
use crate::stats_count_uniq::field_names_string;
use crate::stream_filter::quote_token_if_needed;

/// `pipeJoin` implements `| join by (...) ...`.
pub struct PipeJoin {
    /// Fields to join on (Go `byFields`).
    pub(crate) by_fields: Vec<String>,

    /// Opaque rendered text of the join subquery (Go `q.String()`), present only
    /// when the join uses a subquery rather than inline rows.
    ///
    /// PORT NOTE: this stands in for Go's `q *Query`; it is executed (and
    /// cleared, like Go's `pjNew.q = nil`) by [`Pipe::init_join_map`].
    pub(crate) query_text: Option<String>,

    /// Inline rows to join against (Go `rows`). `Some` iff the join was built
    /// with `rows(...)` instead of a subquery.
    pub(crate) rows: Option<Vec<Vec<Field>>>,

    /// INNER JOIN when true, LEFT JOIN otherwise (Go `isInner`).
    pub(crate) is_inner: bool,

    /// Prefix added to joined fields coming from the subquery/rows (Go `prefix`).
    pub(crate) prefix: String,

    /// Join map from marshaled `by` values to matching extra field sets (Go
    /// `m`). Built from [`PipeJoin::rows`]; empty when only a subquery is set.
    m: HashMap<Vec<u8>, Vec<Vec<Field>>>,
}

/// Constructs a `join` pipe from already-parsed components (the tail of Go's
/// `parsePipeJoin`, whose lexer half lives in `parser::parse_pipe`).
///
/// When `rows` is `Some`, the join map is built here — the map-building half
/// of Go `initJoinMap` (the path that runs when `pj.rows != nil`). When only
/// `query_text` is set, the map is built by [`Pipe::init_join_map`] once
/// `storage_search::init_subqueries` executes the subquery.
pub(crate) fn new_pipe_join(
    by_fields: Vec<String>,
    rows: Option<Vec<Vec<Field>>>,
    query_text: Option<String>,
    is_inner: bool,
    prefix: String,
) -> PipeJoin {
    let m = match &rows {
        Some(rows) => build_join_map(rows, &by_fields, &prefix),
        None => HashMap::new(),
    };
    PipeJoin {
        by_fields,
        query_text,
        rows,
        is_inner,
        prefix,
        m,
    }
}

/// Port of the map-building half of Go `(*pipeJoin).initJoinMap` (the `rows`
/// path, which needs no subquery execution).
fn build_join_map(
    rows: &[Vec<Field>],
    by_fields: &[String],
    prefix: &str,
) -> HashMap<Vec<u8>, Vec<Vec<Field>>> {
    let mut m: HashMap<Vec<u8>, Vec<Vec<Field>>> = HashMap::with_capacity(rows.len());
    let mut by_values: Vec<Vec<u8>> = Vec::new();
    let mut tmp_buf: Vec<u8> = Vec::new();
    for row in rows {
        by_values.clear();
        for bf in by_fields {
            let mut v: &[u8] = b"";
            for f in row {
                if f.name == bf.as_bytes() {
                    v = &f.value;
                    break;
                }
            }
            by_values.push(v.to_vec());
        }

        let mut fields: Vec<Field> = Vec::new();
        for f in row {
            if !by_fields.iter().any(|bf| bf.as_bytes() == f.name) {
                let name = if prefix.is_empty() {
                    f.name.clone()
                } else {
                    let mut name = Vec::with_capacity(prefix.len() + f.name.len());
                    name.extend_from_slice(prefix.as_bytes());
                    name.extend_from_slice(&f.name);
                    name
                };
                fields.push(Field {
                    name,
                    value: f.value.clone(),
                });
            }
        }

        tmp_buf.clear();
        marshal_strings(&mut tmp_buf, &by_values);
        m.entry(tmp_buf.clone()).or_default().push(fields);
    }
    m
}

/// Port of Go `marshalStrings` (defined in `storage_search.go`; inlined here as
/// it is not yet exported by a ported module). Appends length-prefixed strings.
fn marshal_strings(dst: &mut Vec<u8>, a: &[Vec<u8>]) {
    for s in a {
        encoding::marshal_bytes(dst, s);
    }
}

/// Port of Go `marshalRows` (defined in `pipe_join.go`; also used by
/// `pipe_union`). Renders inline rows as `rows({...},{...})`.
pub(crate) fn marshal_rows(dst: &mut Vec<u8>, rows: &[Vec<Field>]) {
    if rows.is_empty() {
        dst.extend_from_slice(b"rows()");
        return;
    }
    dst.extend_from_slice(b"rows(");
    for row in rows {
        marshal_fields_to_json(dst, row);
        dst.push(b',');
    }
    let last = dst.len() - 1;
    dst[last] = b')';
}

impl Pipe for PipeJoin {
    /// Port of Go `pipeJoin.visitSubqueries`: visits the join subquery.
    fn visit_subqueries_mut(
        &mut self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
        let Some(q_text) = self.query_text.as_mut() else {
            return;
        };
        let mut q = crate::parser::query::must_parse_query(q_text, timestamp);
        q.visit_subqueries(visit);
        *q_text = q.to_string();
    }

    /// Port of Go `pipeJoin.splitToRemoteAndLocal`: the pipe (and
    /// everything after it) runs locally only.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (None, vec![crate::pipe::clone_pipe(self, timestamp)])
    }

    fn to_string(&self) -> String {
        let mut dst: Vec<u8> = Vec::new();
        dst.extend_from_slice(b"join by (");
        dst.extend_from_slice(field_names_string(&self.by_fields).as_bytes());
        dst.extend_from_slice(b") ");

        if let Some(rows) = &self.rows {
            marshal_rows(&mut dst, rows);
        } else {
            dst.push(b'(');
            // PORT NOTE: Go appends `pj.q.String()`; the port stores the
            // already-rendered subquery text and uses it verbatim.
            dst.extend_from_slice(self.query_text.as_deref().unwrap_or("").as_bytes());
            dst.push(b')');
        }

        if self.is_inner {
            dst.extend_from_slice(b" inner");
        }
        if !self.prefix.is_empty() {
            dst.extend_from_slice(b" prefix ");
            dst.extend_from_slice(quote_token_if_needed(&self.prefix).as_bytes());
        }
        String::from_utf8_lossy(&dst).into_owned()
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        false
    }

    /// Port of Go `isPipeSafeForHits` for `*pipeJoin`: join pipes are allowed,
    /// since they do not drop the `_time` field.
    fn is_safe_for_hits(&mut self, _timestamp: i64) -> bool {
        true
    }

    fn subquery_is_fixed_output_fields_order(&self) -> Option<bool> {
        let query_text = self.query_text.as_deref()?;
        // PORT NOTE: Go reads the parsed subquery (`pj.q`); the Rust pipe
        // stores rendered text, so re-parse it (the timestamp does not affect
        // the output fields order).
        let q = crate::parser::ParseQuery(query_text).ok()?;
        Some(q.is_fixed_output_fields_order())
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        false
    }

    fn has_filter_in_with_query(&self) -> bool {
        // Do not check for in(...) filters at pj.q, since they are checked
        // separately during pj.q execution at initJoinMap.
        false
    }

    fn is_join_pipe(&self) -> bool {
        true
    }

    /// Port of Go `(*pipeJoin).initJoinMap` (the subquery half; the inline-rows
    /// half runs at construction — see [`new_pipe_join`]).
    fn init_join_map(
        &mut self,
        get_join_rows: &mut crate::storage_search::GetJoinRowsFn<'_>,
    ) -> Result<(), String> {
        if self.rows.is_some() {
            // The join map was already built from the inline rows at
            // construction (Go rebuilds it from pj.rows here; same map).
            return Ok(());
        }
        let Some(q_text) = self.query_text.as_deref() else {
            return Ok(());
        };
        let rows = get_join_rows(q_text).map_err(|e| {
            format!(
                "cannot execute query at pipe [{}]: {e}",
                Pipe::to_string(self)
            )
        })?;
        self.m = build_join_map(&rows, &self.by_fields, &self.prefix);
        // Go: pjNew.q = nil; pjNew.rows = rows — after init the pipe renders as
        // rows(...).
        // PORT NOTE: Go's map building strips the by-fields / applies the
        // prefix inside the shared row slices, so Go's post-init `String()`
        // shows the transformed rows; the port keeps the fetched rows as is
        // (they are only read by `to_string` after init).
        self.rows = Some(rows);
        self.query_text = None;
        Ok(())
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        pf.add_allow_filters(&self.by_fields);
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeJoinProcessorShard::default()))
            .collect();
        Arc::new(PipeJoinProcessor {
            by_fields: self.by_fields.clone(),
            is_inner: self.is_inner,
            m: self.m.clone(),
            stop,
            pp_next,
            shards,
        })
    }
}

struct PipeJoinProcessor {
    by_fields: Vec<String>,
    is_inner: bool,
    m: HashMap<Vec<u8>, Vec<Vec<Field>>>,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeJoinProcessorShard>>,
}

#[derive(Default)]
struct PipeJoinProcessorShard {
    wctx: PipeJoinWriteContext,
    by_values: Vec<Vec<u8>>,
    tmp_buf: Vec<u8>,
}

impl PipeProcessor for PipeJoinProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let mut shard = self.shards[worker_id].lock().unwrap();
        let shard = &mut *shard;

        shard
            .wctx
            .init(worker_id, self.pp_next.clone(), true, true, br);

        // Determine, for each block column, which by-field index it maps to
        // (Go `byValuesIdxs`).
        let cs = br.get_columns();
        let by_values_idxs: Vec<Option<usize>> = cs
            .iter()
            .map(|&c| {
                let name = br.column_name(c);
                self.by_fields.iter().position(|bf| bf.as_bytes() == name)
            })
            .collect();

        shard.by_values.clear();
        shard.by_values.resize(self.by_fields.len(), Vec::new());

        let rows_len = br.rows_len();
        for row_idx in 0..rows_len {
            for v in shard.by_values.iter_mut() {
                v.clear();
            }
            for (j, &c) in cs.iter().enumerate() {
                if let Some(c_idx) = by_values_idxs[j] {
                    shard.by_values[c_idx] = br.column_get_value_at_row(c, row_idx).to_vec();
                }
            }

            shard.tmp_buf.clear();
            marshal_strings(&mut shard.tmp_buf, &shard.by_values);
            let matching_rows = self.m.get(&shard.tmp_buf);

            match matching_rows {
                None => {
                    if !self.is_inner {
                        shard.wctx.write_row(br, row_idx, &[]);
                    }
                }
                Some(matching_rows) => {
                    for extra_fields in matching_rows {
                        if self.stop.load(Ordering::Relaxed) {
                            return;
                        }
                        shard.wctx.write_row(br, row_idx, extra_fields);
                    }
                }
            }
        }

        shard.wctx.flush();
        shard.wctx.reset();
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PipeJoinWriteContext
//
// PORT NOTE: Go reuses `pipeUnpackWriteContext` here. The Rust
// `PipeUnpackWriteContext` in `pipe_unpack.rs` has private methods, so a
// faithful local copy of the parts join needs is provided (join always passes
// keepOriginalFields=true, skipEmptyResults=true).
// ---------------------------------------------------------------------------

#[derive(Default)]
struct PipeJoinWriteContext {
    worker_id: usize,
    pp_next: Option<Arc<dyn PipeProcessor>>,
    keep_original_fields: bool,
    skip_empty_results: bool,

    cs_src: Vec<ColRef>,
    cs_src_names: Vec<Vec<u8>>,

    rcs: Vec<ResultColumn>,
    br: BlockResult,

    rows_count: usize,
    values_len: usize,
}

impl PipeJoinWriteContext {
    fn reset(&mut self) {
        self.worker_id = 0;
        self.pp_next = None;
        self.keep_original_fields = false;
        self.skip_empty_results = false;
        self.cs_src.clear();
        self.cs_src_names.clear();
        for rc in &mut self.rcs {
            rc.reset();
        }
        self.rcs.clear();
        self.rows_count = 0;
        self.values_len = 0;
    }

    fn init(
        &mut self,
        worker_id: usize,
        pp_next: Arc<dyn PipeProcessor>,
        keep_original_fields: bool,
        skip_empty_results: bool,
        br_src: &mut BlockResult,
    ) {
        self.reset();
        self.worker_id = worker_id;
        self.pp_next = Some(pp_next);
        self.keep_original_fields = keep_original_fields;
        self.skip_empty_results = skip_empty_results;
        self.cs_src = br_src.get_columns();
        self.cs_src_names = self
            .cs_src
            .iter()
            .map(|&r| br_src.column_name(r).to_vec())
            .collect();
    }

    fn write_row(&mut self, br_src: &mut BlockResult, row_idx: usize, extra_fields: &[Field]) {
        let cs_src_len = self.cs_src.len();

        let mut are_equal_columns = self.rcs.len() == cs_src_len + extra_fields.len();
        if are_equal_columns {
            for (i, f) in extra_fields.iter().enumerate() {
                if self.rcs[cs_src_len + i].name != f.name {
                    are_equal_columns = false;
                    break;
                }
            }
        }
        if !are_equal_columns {
            // send the current block to ppNext and construct a block with new
            // set of columns
            self.flush();

            self.rcs.clear();
            for name in &self.cs_src_names {
                append_result_column_with_name(&mut self.rcs, name);
            }
            for f in extra_fields {
                append_result_column_with_name(&mut self.rcs, &f.name);
            }
        }

        for i in 0..cs_src_len {
            let v = br_src
                .column_get_value_at_row(self.cs_src[i], row_idx)
                .to_vec();
            self.values_len += v.len();
            self.rcs[i].add_value(&v);
        }
        for (i, f) in extra_fields.iter().enumerate() {
            let mut v = f.value.clone();
            if ((v.is_empty() && self.skip_empty_results) || self.keep_original_fields)
                && let Some(idx) = self.cs_src_names.iter().position(|n| *n == f.name)
            {
                let v_orig = br_src
                    .column_get_value_at_row(self.cs_src[idx], row_idx)
                    .to_vec();
                if !v_orig.is_empty() {
                    v = v_orig;
                }
            }
            self.values_len += v.len();
            self.rcs[cs_src_len + i].add_value(&v);
        }

        self.rows_count += 1;
        // The 64_000 limit provides the best performance results.
        if self.values_len >= 64_000 {
            self.flush();
        }
    }

    fn flush(&mut self) {
        self.values_len = 0;

        let rows_count = self.rows_count;
        self.br.set_result_columns(self.rcs.clone(), rows_count);
        self.rows_count = 0;
        if let Some(pp) = self.pp_next.clone() {
            pp.write_block(self.worker_id, &mut self.br);
        }
        self.br.reset();
        for rc in &mut self.rcs {
            rc.reset_values();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::{assert_needed_fields, assert_rows_eq, rows, run_pipe};

    // PORT NOTE: the `TestParsePipeJoinSuccess` / `TestParsePipeJoinFailure`
    // cases (incl. `TestParseRows_*`) are covered as query round-trips in
    // `parser::tests::test_parse_query_subqueries`; the subquery execution
    // path (`init_join_map`) is covered by
    // `storage_search::tests::test_storage_run_query_subqueries`.

    fn subquery_join(by_fields: &[&str], is_inner: bool) -> PipeJoin {
        new_pipe_join(
            by_fields.iter().map(|s| s.to_string()).collect(),
            None,
            Some("abc".to_string()),
            is_inner,
            String::new(),
        )
    }

    #[test]
    fn test_pipe_join_update_needed_fields() {
        // all the needed fields
        let p = subquery_join(&["x", "y"], false);
        assert_needed_fields(&p, "*", "", "*", "");

        // all the needed fields, unneeded fields do not intersect with src
        let p = subquery_join(&["x", "y"], true);
        assert_needed_fields(&p, "*", "f1,f2", "*", "f1,f2");

        // all the needed fields, unneeded fields intersect with src
        let p = subquery_join(&["x", "y"], false);
        assert_needed_fields(&p, "*", "f2,x", "*", "f2");

        // needed fields do not intersect with src
        let p = subquery_join(&["x", "y"], false);
        assert_needed_fields(&p, "f1,f2", "", "f1,f2,x,y", "");

        // needed fields intersect with src
        let p = subquery_join(&["x", "y"], false);
        assert_needed_fields(&p, "f2,x", "", "f2,x,y", "");
    }

    // The following tests exercise ONLY the inline-`rows` join path, which needs
    // no subquery execution (Go has no `TestPipeJoin` behavior test; these
    // verify the ported map-building + write_block logic).

    fn rows_join(
        by_fields: &[&str],
        join_rows: &[Vec<Field>],
        is_inner: bool,
        prefix: &str,
    ) -> PipeJoin {
        new_pipe_join(
            by_fields.iter().map(|s| s.to_string()).collect(),
            Some(join_rows.to_vec()),
            None,
            is_inner,
            prefix.to_string(),
        )
    }

    #[test]
    fn test_to_string_inline_rows() {
        let p = rows_join(
            &["x"],
            &rows(&[&[("x", "y"), ("z", "qwe")], &[("x", "123"), ("z", "456")]]),
            false,
            "",
        );
        assert_eq!(
            p.to_string(),
            r#"join by (x) rows({"x":"y","z":"qwe"},{"x":"123","z":"456"})"#
        );

        let p = rows_join(&["x"], &rows(&[&[("x", "y"), ("z", "qwe")]]), true, "abc");
        assert_eq!(
            p.to_string(),
            r#"join by (x) rows({"x":"y","z":"qwe"}) inner prefix abc"#
        );
    }

    #[test]
    fn test_pipe_join_inline_rows_left() {
        // LEFT JOIN: rows without a match still pass through (no extra fields).
        let p = rows_join(
            &["x"],
            &rows(&[&[("x", "1"), ("y", "a")], &[("x", "2"), ("y", "b")]]),
            false,
            "",
        );
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[&[("_msg", "m1"), ("x", "1")], &[("_msg", "m2"), ("x", "3")]]),
            ),
            &rows(&[
                &[("_msg", "m1"), ("x", "1"), ("y", "a")],
                &[("_msg", "m2"), ("x", "3")],
            ]),
        );
    }

    #[test]
    fn test_pipe_join_inline_rows_inner() {
        // INNER JOIN: unmatched input rows are dropped.
        let p = rows_join(&["x"], &rows(&[&[("x", "1"), ("y", "a")]]), true, "");
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[&[("_msg", "m1"), ("x", "1")], &[("_msg", "m2"), ("x", "3")]]),
            ),
            &rows(&[&[("_msg", "m1"), ("x", "1"), ("y", "a")]]),
        );
    }

    #[test]
    fn test_pipe_join_inline_rows_multi_match() {
        // Multiple matching rows produce one output row each.
        let p = rows_join(
            &["x"],
            &rows(&[&[("x", "1"), ("y", "a")], &[("x", "1"), ("y", "b")]]),
            false,
            "",
        );
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "m1"), ("x", "1")]])),
            &rows(&[
                &[("_msg", "m1"), ("x", "1"), ("y", "a")],
                &[("_msg", "m1"), ("x", "1"), ("y", "b")],
            ]),
        );
    }

    #[test]
    fn test_pipe_join_inline_rows_prefix_keeps_original() {
        // prefix is applied to joined fields; a joined field colliding with a
        // non-empty source column keeps the source value (keepOriginalFields).
        let p = rows_join(&["x"], &rows(&[&[("x", "1"), ("y", "a")]]), false, "p_");
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "m1"), ("x", "1")]])),
            &rows(&[&[("_msg", "m1"), ("x", "1"), ("p_y", "a")]]),
        );
    }
}

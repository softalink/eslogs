//! Port of EsLogs `lib/logstorage/pipe_unpack.go`.
//!
//! Shared scaffolding for the `unpack_*` pipes (`unpack_json`, `unpack_logfmt`,
//! `unpack_syslog`). It exposes:
//!   * [`update_needed_fields_for_unpack_pipe`] — the shared
//!     `updateNeededFields` helper,
//!   * [`FieldsUnpackerContext`] — accumulates unpacked fields for one row,
//!   * [`PipeUnpackWriteContext`] — builds output blocks from source columns
//!     plus unpacked fields,
//!   * [`new_pipe_unpack_processor`] — the shared processor driving all of the
//!     above, parameterized by an `unpack_func`.
//!
//! PORT NOTE: Go's `fieldsUnpackerContext` and `pipeUnpackWriteContext` use an
//! `arena` for zero-copy field storage backed by the source strings. The Rust
//! `Field`/`ResultColumn` own their bytes, so the arena is dropped and values
//! are copied — behaviorally identical, with extra allocations.

use std::sync::Arc;

use esl_common::atomicutil::Slice;

use crate::bitmap::Bitmap;
use crate::block_result::{BlockResult, ColRef, ResultColumn, append_result_column_with_name};
use crate::filter::Filter;
use crate::pipe::PipeProcessor;
use crate::prefix_filter;
use crate::rows::Field;

// ---------------------------------------------------------------------------
// IfFilter
// ---------------------------------------------------------------------------

/// Optional `if (...)` filter attached to a pipe (Go `ifFilter`).
///
/// PORT NOTE: Go's `ifFilter` lives in `if_filter.go` and is constructed by the
/// lexer-based `parseIfFilter`. Parsing is deferred (no lexer yet), so only the
/// runtime-relevant struct plus [`new_if_filter`] (a faithful port of Go's
/// `newIfFilter`) are provided here. The extract/unpack pipes carry an
/// `Option<IfFilter>` and apply `f` to skip rows.
#[derive(Clone)]
pub(crate) struct IfFilter {
    /// The compiled filter.
    pub(crate) f: Arc<dyn Filter>,

    /// Fields the filter needs (Go `allowFilters`).
    pub(crate) allow_filters: Vec<Vec<u8>>,
}

impl std::fmt::Display for IfFilter {
    /// Go `ifFilter.String()`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "if ({})", self.f.to_string())
    }
}

/// Port of Go's `newIfFilter`.
pub(crate) fn new_if_filter(f: Arc<dyn Filter>) -> IfFilter {
    let mut pf = prefix_filter::Filter::default();
    f.update_needed_fields(&mut pf);
    let allow_filters = pf.get_allow_filters();
    IfFilter { f, allow_filters }
}

impl IfFilter {
    /// Port of Go `(iff *ifFilter).hasFilterInWithQuery`.
    pub(crate) fn has_filter_in_with_query(&self) -> bool {
        crate::storage_search::has_filter_in_with_query_for_filter(self.f.as_ref())
    }

    /// Port of Go `(iff *ifFilter).initFilterInValues`: returns a new
    /// `IfFilter` with the `in(<subquery>)` values resolved, or `None` when
    /// there is nothing to resolve (Go returns a copy either way; the callers
    /// keep the existing iff on `None`).
    pub(crate) fn init_filter_in_values(
        &self,
        get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
        timestamp: i64,
    ) -> Result<Option<IfFilter>, String> {
        match crate::storage_search::init_filter_in_values_for_shared_filter(
            &self.f, get_values, timestamp,
        )? {
            Some(f) => Ok(Some(IfFilter {
                f,
                allow_filters: self.allow_filters.clone(),
            })),
            None => Ok(None),
        }
    }

    /// Port of Go `(iff *ifFilter).visitSubqueries`.
    pub(crate) fn visit_subqueries_mut(
        &self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) -> Option<IfFilter> {
        crate::storage_search::visit_subqueries_in_shared_filter(&self.f, timestamp, visit).map(
            |f| IfFilter {
                f,
                allow_filters: self.allow_filters.clone(),
            },
        )
    }
}

// ---------------------------------------------------------------------------
// updateNeededFieldsForUnpackPipe
// ---------------------------------------------------------------------------

/// Port of Go's `updateNeededFieldsForUnpackPipe`.
pub(crate) fn update_needed_fields_for_unpack_pipe(
    from_field: &[u8],
    out_field_prefix: &str,
    out_field_filters: &[Vec<u8>],
    keep_original_fields: bool,
    skip_empty_results: bool,
    iff: Option<&IfFilter>,
    pf: &mut prefix_filter::Filter,
) {
    if pf.match_nothing() {
        // There is no need in fetching any fields, since the caller ignores all the fields.
        return;
    }

    let mut need_from_field = out_field_filters.is_empty();
    for f in out_field_filters {
        // Byte concat of prefix + filter (Go string concat over raw bytes).
        let mut prefixed = out_field_prefix.as_bytes().to_vec();
        prefixed.extend_from_slice(f);
        if pf.match_string_or_wildcard(&prefixed) {
            need_from_field = true;
            break;
        }
    }
    if !keep_original_fields && !skip_empty_results {
        for f in out_field_filters {
            if !prefix_filter::is_wildcard_filter(f) {
                let mut prefixed = out_field_prefix.as_bytes().to_vec();
                prefixed.extend_from_slice(f);
                pf.add_deny_filter(&prefixed);
            }
        }
    }
    if need_from_field {
        pf.add_allow_filter(from_field);
        if let Some(iff) = iff {
            pf.add_allow_filters(&iff.allow_filters);
        }
    }
}

// ---------------------------------------------------------------------------
// fieldsUnpackerContext
// ---------------------------------------------------------------------------

/// Accumulates unpacked fields for a single input row (Go
/// `fieldsUnpackerContext`).
#[derive(Default)]
pub(crate) struct FieldsUnpackerContext {
    field_prefix: String,
    pub(crate) fields: Vec<Field>,
}

impl FieldsUnpackerContext {
    fn reset(&mut self) {
        self.field_prefix.clear();
        self.reset_fields();
    }

    pub(crate) fn reset_fields(&mut self) {
        self.fields.clear();
    }

    fn init(&mut self, field_prefix: &str) {
        self.reset();
        self.field_prefix.push_str(field_prefix);
    }

    /// Adds a field, applying the configured field prefix to its name.
    pub(crate) fn add_field(&mut self, name: &[u8], value: impl AsRef<[u8]>) {
        let name_copy = if self.field_prefix.is_empty() {
            name.to_vec()
        } else {
            let mut name_copy = Vec::with_capacity(self.field_prefix.len() + name.len());
            name_copy.extend_from_slice(self.field_prefix.as_bytes());
            name_copy.extend_from_slice(name);
            name_copy
        };
        self.fields.push(Field {
            name: name_copy,
            value: value.as_ref().to_vec(),
        });
    }
}

// ---------------------------------------------------------------------------
// pipeUnpackWriteContext
// ---------------------------------------------------------------------------

/// Builds output blocks from source columns plus per-row unpacked fields (Go
/// `pipeUnpackWriteContext`).
#[derive(Default)]
pub(crate) struct PipeUnpackWriteContext {
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

impl PipeUnpackWriteContext {
    pub(crate) fn reset(&mut self) {
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

    pub(crate) fn init(
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

    pub(crate) fn write_row(
        &mut self,
        br_src: &mut BlockResult,
        row_idx: usize,
        extra_fields: &[Field],
    ) {
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
            // send the current block to ppNext and construct a block with new set of columns
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
            let want_original =
                (v.is_empty() && self.skip_empty_results) || self.keep_original_fields;
            let idx = if want_original {
                get_block_result_column_idx_by_name(&self.cs_src_names, &f.name)
            } else {
                None
            };
            if let Some(idx) = idx {
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

    pub(crate) fn flush(&mut self) {
        self.values_len = 0;

        // PORT NOTE: Go's `setResultColumns` borrows the shared `rcs` slice; the
        // Rust API takes ownership, so `rcs` is cloned to keep the column names
        // for reuse after the block is sent (values are reset below).
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

/// Port of Go's `getBlockResultColumnIdxByName`.
fn get_block_result_column_idx_by_name(names: &[Vec<u8>], name: &[u8]) -> Option<usize> {
    names.iter().position(|n| n == name)
}

// ---------------------------------------------------------------------------
// pipeUnpackProcessor
// ---------------------------------------------------------------------------

/// The unpack function applied to each source value (Go `unpackFunc`).
pub(crate) type UnpackFunc = Box<dyn Fn(&mut FieldsUnpackerContext, &[u8]) + Send + Sync>;

/// Port of Go's `pipeUnpackProcessor` plus `newPipeUnpackProcessor`.
pub(crate) struct PipeUnpackProcessor {
    unpack_func: UnpackFunc,
    pp_next: Arc<dyn PipeProcessor>,

    from_field: Vec<u8>,
    field_prefix: String,
    keep_original_fields: bool,
    skip_empty_results: bool,

    iff: Option<IfFilter>,

    shards: Slice<std::sync::Mutex<PipeUnpackProcessorShard>>,
}

#[derive(Default)]
struct PipeUnpackProcessorShard {
    bm: Bitmap,
    uctx: FieldsUnpackerContext,
    wctx: PipeUnpackWriteContext,
}

/// Port of Go's `newPipeUnpackProcessor`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn new_pipe_unpack_processor(
    unpack_func: UnpackFunc,
    pp_next: Arc<dyn PipeProcessor>,
    from_field: Vec<u8>,
    field_prefix: String,
    keep_original_fields: bool,
    skip_empty_results: bool,
    iff: Option<IfFilter>,
) -> Arc<dyn PipeProcessor> {
    Arc::new(PipeUnpackProcessor {
        unpack_func,
        pp_next,
        from_field,
        field_prefix,
        keep_original_fields,
        skip_empty_results,
        iff,
        shards: Slice::default(),
    })
}

impl PipeProcessor for PipeUnpackProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let shard_arc = self.shards.get(worker_id);
        let mut guard = shard_arc.lock().unwrap();
        let shard = &mut *guard;

        shard.wctx.init(
            worker_id,
            self.pp_next.clone(),
            self.keep_original_fields,
            self.skip_empty_results,
            br,
        );
        shard.uctx.init(&self.field_prefix);

        if let Some(iff) = &self.iff {
            shard.bm.init(br.rows_len());
            shard.bm.set_bits();
            iff.f.apply_to_block_result(br, &mut shard.bm);
            if shard.bm.is_zero() {
                self.pp_next.write_block(worker_id, br);
                return;
            }
        }

        let c = br.get_column_by_name(&self.from_field);
        let rows_len = br.rows_len();
        if br.column_is_const(c) {
            let v = br.column_get_value_at_row(c, 0).to_vec();
            shard.uctx.reset_fields();
            (self.unpack_func)(&mut shard.uctx, &v);
            let fields = std::mem::take(&mut shard.uctx.fields);
            for row_idx in 0..rows_len {
                if self.iff.is_none() || shard.bm.is_set_bit(row_idx) {
                    shard.wctx.write_row(br, row_idx, &fields);
                } else {
                    shard.wctx.write_row(br, row_idx, &[]);
                }
            }
        } else {
            let values = br.column_get_values(c).to_vec();
            let mut v_prev: Vec<u8> = Vec::new();
            let mut had_unpacks = false;
            let mut fields: Vec<Field> = Vec::new();
            for (row_idx, v) in values.iter().enumerate() {
                if self.iff.is_none() || shard.bm.is_set_bit(row_idx) {
                    if !had_unpacks || &v_prev != v {
                        v_prev = v.clone();
                        had_unpacks = true;
                        shard.uctx.reset_fields();
                        (self.unpack_func)(&mut shard.uctx, v);
                        fields = shard.uctx.fields.clone();
                    }
                    shard.wctx.write_row(br, row_idx, &fields);
                } else {
                    shard.wctx.write_row(br, row_idx, &[]);
                }
            }
        }

        shard.wctx.flush();
        shard.wctx.reset();
        shard.uctx.reset();
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared test harness (used by the extract/unpack/pack pipe test modules)
// ---------------------------------------------------------------------------

/// PORT NOTE: Port of the runtime half of `pipe_utils_test.go`
/// (`testBlockResultWriter`, `testPipeProcessor`, `assertRowsEqual`). The
/// `expectPipeResults`/`expectParsePipe*` helpers that parse a pipe string are
/// lexer-dependent and deferred; [`run_pipe`] instead accepts a pipe built via
/// its `pub(crate)` constructor. Block splitting uses a deterministic LCG
/// instead of Go's `math/rand`, so runs are reproducible.
#[cfg(test)]
pub(crate) mod test_utils {
    use std::cmp::Ordering;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use crate::block_result::{BlockResult, ResultColumn, append_result_column_with_name};
    use crate::pipe::{Pipe, PipeProcessor};
    use crate::rows::Field;

    pub(crate) fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    /// Convenience: build `[][]Field` from string tuples.
    pub(crate) fn rows(data: &[&[(&str, &str)]]) -> Vec<Vec<Field>> {
        data.iter()
            .map(|row| row.iter().map(|(n, v)| field(n, v)).collect())
            .collect()
    }

    struct TestPipeProcessor {
        result_rows: Mutex<Vec<Vec<Field>>>,
    }

    impl PipeProcessor for TestPipeProcessor {
        fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
            let cs = br.get_columns();
            let names: Vec<Vec<u8>> = cs.iter().map(|&c| br.column_name(c).to_vec()).collect();
            let mut column_values: Vec<Vec<Vec<u8>>> = Vec::with_capacity(cs.len());
            for &c in &cs {
                column_values.push(br.column_get_values(c).to_vec());
            }
            let mut out = self.result_rows.lock().unwrap();
            for i in 0..br.rows_len() {
                let mut row = Vec::with_capacity(column_values.len());
                for (j, values) in column_values.iter().enumerate() {
                    row.push(Field {
                        name: names[j].clone(),
                        value: values[i].clone(),
                    });
                }
                out.push(row);
            }
        }

        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    struct TestBlockResultWriter {
        workers_count: usize,
        pp_next: Arc<dyn PipeProcessor>,
        rcs: Vec<ResultColumn>,
        br: BlockResult,
        rows_count: usize,
        counter: u64,
    }

    impl TestBlockResultWriter {
        fn next_rand(&mut self) -> u64 {
            self.counter = self
                .counter
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.counter >> 33
        }

        fn are_same_fields(&self, row: &[Field]) -> bool {
            if self.rcs.len() != row.len() {
                return false;
            }
            self.rcs.iter().zip(row).all(|(rc, f)| rc.name == f.name)
        }

        fn write_row(&mut self, row: &[Field]) {
            if !self.are_same_fields(row) {
                self.flush();
                self.rcs.clear();
                for f in row {
                    append_result_column_with_name(&mut self.rcs, &f.name);
                }
            }
            for (i, f) in row.iter().enumerate() {
                self.rcs[i].add_value(&f.value);
            }
            self.rows_count += 1;
            if self.next_rand().is_multiple_of(5) {
                self.flush();
            }
        }

        fn flush(&mut self) {
            self.br
                .set_result_columns(self.rcs.clone(), self.rows_count);
            self.rows_count = 0;
            let worker_id = (self.next_rand() as usize) % self.workers_count.max(1);
            self.pp_next.write_block(worker_id, &mut self.br);
            self.br.reset();
            for rc in &mut self.rcs {
                rc.reset_values();
            }
        }
    }

    fn cmp_fields(a: &Field, b: &Field) -> Ordering {
        a.name.cmp(&b.name).then_with(|| a.value.cmp(&b.value))
    }

    fn row_key(row: &[Field]) -> Vec<u8> {
        let mut s = Vec::new();
        for f in row {
            s.extend_from_slice(&f.name);
            s.push(0);
            s.extend_from_slice(&f.value);
            s.push(1);
        }
        s
    }

    fn sort_rows(rows: &mut [Vec<Field>]) {
        for row in rows.iter_mut() {
            row.sort_by(cmp_fields);
        }
        rows.sort_by_key(|a| row_key(a));
    }

    /// Runs `rows` through the pipe built from `pipe` and asserts the produced
    /// rows equal `expected` (order-independent, like Go's `assertRowsEqual`).
    pub(crate) fn run_pipe(pipe: Arc<dyn Pipe>, rows: &[Vec<Field>], expected: &[Vec<Field>]) {
        let workers_count = 5;
        let stop = Arc::new(AtomicBool::new(false));
        let pp_test = Arc::new(TestPipeProcessor {
            result_rows: Mutex::new(Vec::new()),
        });
        let pp = pipe.new_pipe_processor(workers_count, stop, pp_test.clone());

        let mut brw = TestBlockResultWriter {
            workers_count,
            pp_next: pp.clone(),
            rcs: Vec::new(),
            br: BlockResult::default(),
            rows_count: 0,
            counter: 0x9e3779b97f4a7c15,
        };
        for row in rows {
            brw.write_row(row);
        }
        brw.flush();
        pp.flush().unwrap();

        let mut result_rows = pp_test.result_rows.lock().unwrap().clone();
        let mut expected_rows = expected.to_vec();

        assert_eq!(
            result_rows.len(),
            expected_rows.len(),
            "unexpected number of rows;\ngot {result_rows:?}\nwant {expected_rows:?}"
        );

        sort_rows(&mut result_rows);
        sort_rows(&mut expected_rows);

        for (i, (got, want)) in result_rows.iter().zip(expected_rows.iter()).enumerate() {
            assert_eq!(
                got, want,
                "unexpected row #{i};\ngot  {got:?}\nwant {want:?}"
            );
        }
    }
}

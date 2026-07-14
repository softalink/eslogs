//! Port of `pipe_update.go` — shared machinery for pipes that rewrite a single
//! field value in place (`decolorize`, `format`, `replace`, `replace_regexp`,
//! `hash`, ...).
//!
//! Unlike most `pipe_*.go` files this one defines no `Pipe`; it provides the
//! reusable [`PipeUpdateProcessor`], the [`IfFilter`] helper type (Go's
//! `ifFilter`, whose lexer-based `parseIfFilter` is deferred), and the
//! `updateNeededFieldsForUpdatePipe` / `shouldDenyOverwrittenField` helpers.
//!
//! PORT NOTE: Go's `updateFunc(a *arena, v string) string` threads a pooled
//! `arena` for the rewritten bytes. The Rust port drops the arena and returns
//! an owned `String`; correctness is identical, only the per-call allocation
//! pooling differs (acceptable per CONVENTIONS — no observable behavior change).

use std::sync::{Arc, Mutex};

use crate::bitmap::Bitmap;
use crate::block_result::{BlockResult, ResultColumn};
use crate::filter::Filter;
use crate::pipe::PipeProcessor;
use crate::prefix_filter;

/// Rewrites a field value. Port of Go's `updateFunc`.
pub(crate) type UpdateFunc = Arc<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>;

// ---------------------------------------------------------------------------
// IfFilter (Go `ifFilter`)
// ---------------------------------------------------------------------------

/// An optional `if (...)` clause attached to a pipe. Port of Go's `ifFilter`.
///
/// PORT NOTE: `parseIfFilter` is lexer-dependent and therefore deferred; build
/// an `IfFilter` from an already-parsed filter via [`IfFilter::new`].
pub(crate) struct IfFilter {
    pub f: Arc<dyn Filter>,
    pub allow_filters: Vec<Vec<u8>>,
}

impl IfFilter {
    /// Port of Go `newIfFilter`.
    pub(crate) fn new(f: Arc<dyn Filter>) -> Self {
        let mut pf = prefix_filter::Filter::default();
        f.update_needed_fields(&mut pf);
        let allow_filters = pf.get_allow_filters();
        Self { f, allow_filters }
    }

    /// Port of Go `(*ifFilter).String`.
    // PORT NOTE: named `to_string` for parity with Go's `String()`; this is an
    // internal helper, not a `Display` impl, so allow the inherent-method lint.
    #[allow(clippy::inherent_to_string)]
    pub(crate) fn to_string(&self) -> String {
        format!("if ({})", self.f.to_string())
    }

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

    /// Port of Go `(iff *ifFilter).visitSubqueries`: propagates into the
    /// subqueries embedded in the `if (...)` filter. Returns a replacement
    /// `IfFilter` when the filter held any subquery, else `None`.
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

/// Port of Go `updateNeededFieldsForUpdatePipe`.
pub(crate) fn update_needed_fields_for_update_pipe(
    pf: &mut prefix_filter::Filter,
    field: &[u8],
    iff: Option<&IfFilter>,
) {
    if let Some(iff) = iff
        && pf.match_string(field)
    {
        pf.add_allow_filters(&iff.allow_filters);
    }
}

/// Port of Go `shouldDenyOverwrittenField`.
pub(crate) fn should_deny_overwritten_field(
    iff: Option<&IfFilter>,
    keep_original_fields: bool,
    skip_empty_results: bool,
) -> bool {
    iff.is_none() && !keep_original_fields && !skip_empty_results
}

// ---------------------------------------------------------------------------
// PipeUpdateProcessor (Go `pipeUpdateProcessor`)
// ---------------------------------------------------------------------------

/// Port of Go `newPipeUpdateProcessor`.
pub(crate) fn new_pipe_update_processor(
    update_func: UpdateFunc,
    pp_next: Arc<dyn PipeProcessor>,
    field: Vec<u8>,
    iff: Option<Arc<IfFilter>>,
    concurrency: usize,
) -> Arc<dyn PipeProcessor> {
    let shards = (0..concurrency.max(1))
        .map(|_| Mutex::new(PipeUpdateProcessorShard::default()))
        .collect();
    Arc::new(PipeUpdateProcessor {
        update_func,
        field,
        iff,
        pp_next,
        shards,
    })
}

struct PipeUpdateProcessor {
    update_func: UpdateFunc,
    field: Vec<u8>,
    iff: Option<Arc<IfFilter>>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeUpdateProcessorShard>>,
}

#[derive(Default)]
struct PipeUpdateProcessorShard {
    bm: Bitmap,
    rc: ResultColumn,
}

impl PipeProcessor for PipeUpdateProcessor {
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

        shard.rc.name = self.field.clone();

        let c = br.get_column_by_name(&self.field);
        let values: Vec<Vec<u8>> = br.column_get_values(c).to_vec();

        let mut need_updates = true;
        let mut v_prev: Vec<u8> = Vec::new();
        let mut v_new: Vec<u8> = Vec::new();
        for (row_idx, v_bytes) in values.iter().enumerate() {
            if !has_iff || shard.bm.is_set_bit(row_idx) {
                if need_updates || &v_prev != v_bytes {
                    v_prev = v_bytes.clone();
                    need_updates = false;
                    v_new = (self.update_func)(v_bytes);
                }
                shard.rc.add_value(&v_new);
            } else {
                shard.rc.add_value(v_bytes);
            }
        }

        let rc = std::mem::take(&mut shard.rc);
        br.add_result_column(rc);
        drop(shard);
        self.pp_next.write_block(worker_id, br);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared test harness (reused by every `pipe_*` test module).
// ---------------------------------------------------------------------------

/// Port of `pipe_utils_test.go`'s `testBlockResultWriter` / `testPipeProcessor`
/// harness, minus the lexer: tests build a [`crate::pipe::Pipe`] via its
/// `new_pipe_*` constructor and drive it with [`test_utils::run_pipe`].
///
/// PORT NOTE: the Go harness randomizes block splits and worker assignment via
/// `math/rand`; the Rust port splits deterministically (runs of identical field
/// names, chunked, round-robined across worker slots) so results are
/// reproducible. Output rows are compared order-independently, exactly like Go.
#[cfg(test)]
pub(crate) mod test_utils {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use crate::block_result::BlockResult;
    use crate::pipe::{Pipe, PipeProcessor};
    use crate::rows::Field;

    pub(crate) const WORKERS_COUNT: usize = 5;

    #[derive(Default)]
    pub(crate) struct CollectProcessor {
        rows: Mutex<Vec<Vec<Field>>>,
    }

    impl PipeProcessor for CollectProcessor {
        fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
            let cs = br.get_columns();
            let names: Vec<Vec<u8>> = cs.iter().map(|&c| br.column_name(c).to_vec()).collect();
            let mut column_values: Vec<Vec<Vec<u8>>> = Vec::with_capacity(cs.len());
            for &c in &cs {
                column_values.push(br.column_get_values(c).to_vec());
            }
            let rows_len = br.rows_len();
            let mut out = self.rows.lock().unwrap();
            for i in 0..rows_len {
                let mut row = Vec::with_capacity(cs.len());
                for (name, col) in names.iter().zip(column_values.iter()) {
                    row.push(Field {
                        name: name.clone(),
                        value: col[i].clone(),
                    });
                }
                out.push(row);
            }
        }

        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    impl CollectProcessor {
        pub(crate) fn rows(&self) -> Vec<Vec<Field>> {
            self.rows.lock().unwrap().clone()
        }
    }

    fn same_field_names(a: &[Field], b: &[Field]) -> bool {
        a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.name == y.name)
    }

    /// Feeds `rows` to `pp` as blocks; used by [`run_pipe`].
    pub(crate) fn write_rows(pp: &dyn PipeProcessor, rows: &[Vec<Field>]) {
        let mut i = 0usize;
        let mut block_idx = 0usize;
        while i < rows.len() {
            let mut j = i + 1;
            // Group consecutive rows with identical field names, capped at 2 to
            // exercise multi-block behavior deterministically.
            while j < rows.len() && same_field_names(&rows[i], &rows[j]) && (j - i) < 2 {
                j += 1;
            }
            let mut br = BlockResult::default();
            br.must_init_from_rows(&rows[i..j]);
            let worker_id = block_idx % WORKERS_COUNT;
            pp.write_block(worker_id, &mut br);
            i = j;
            block_idx += 1;
        }
    }

    /// Builds a processor chain terminating in a [`CollectProcessor`], drives it
    /// with `rows`, flushes, and returns the collected output rows.
    pub(crate) fn run_pipe(p: &dyn Pipe, rows: &[Vec<Field>]) -> Vec<Vec<Field>> {
        let collector = Arc::new(CollectProcessor::default());
        let stop = Arc::new(AtomicBool::new(false));
        let pp = p.new_pipe_processor(WORKERS_COUNT, stop, collector.clone());
        write_rows(pp.as_ref(), rows);
        pp.flush().unwrap();
        collector.rows()
    }

    fn cmp_field(a: &Field, b: &Field) -> std::cmp::Ordering {
        a.name.cmp(&b.name).then_with(|| a.value.cmp(&b.value))
    }

    fn sort_rows(rows: &mut [Vec<Field>]) {
        for row in rows.iter_mut() {
            row.sort_by(cmp_field);
        }
        rows.sort_by(|a, b| {
            let (aa, bb, reverse) = if a.len() > b.len() {
                (b.as_slice(), a.as_slice(), true)
            } else {
                (a.as_slice(), b.as_slice(), false)
            };
            for (fa, fb) in aa.iter().zip(bb.iter()) {
                let mut r = cmp_field(fa, fb);
                if r != std::cmp::Ordering::Equal {
                    if reverse {
                        r = r.reverse();
                    }
                    return r;
                }
            }
            if aa.len() == bb.len() {
                std::cmp::Ordering::Equal
            } else if reverse {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        });
    }

    /// Port of Go `assertRowsEqual`: compares order-independently.
    pub(crate) fn assert_rows_eq(got: &[Vec<Field>], expected: &[Vec<Field>]) {
        let mut a = got.to_vec();
        let mut b = expected.to_vec();
        assert_eq!(
            a.len(),
            b.len(),
            "unexpected number of rows;\n got {got:?}\nwant {expected:?}"
        );
        sort_rows(&mut a);
        sort_rows(&mut b);
        assert_eq!(a, b, "rows differ;\n got {got:?}\nwant {expected:?}");
    }

    /// Convenience: build rows from `(name, value)` pairs.
    pub(crate) fn rows(spec: &[&[(&str, &str)]]) -> Vec<Vec<Field>> {
        spec.iter()
            .map(|row| {
                row.iter()
                    .map(|(n, v)| Field {
                        name: n.as_bytes().to_vec(),
                        value: v.as_bytes().to_vec(),
                    })
                    .collect()
            })
            .collect()
    }

    fn split_csv(s: &str) -> Vec<Vec<u8>> {
        if s.is_empty() {
            return Vec::new();
        }
        s.split(',').map(|x| x.as_bytes().to_vec()).collect()
    }

    /// Port of Go `expectPipeNeededFields`: seeds a prefix filter with
    /// `allow`/`deny` (comma-separated), applies the pipe, and asserts the
    /// resulting allow/deny sets (compared as sorted sets, like Go).
    pub(crate) fn assert_needed_fields(
        p: &dyn Pipe,
        allow: &str,
        deny: &str,
        allow_expected: &str,
        deny_expected: &str,
    ) {
        let mut pf = crate::prefix_filter::Filter::default();
        pf.add_allow_filters(&split_csv(allow));
        pf.add_deny_filters(&split_csv(deny));
        p.update_needed_fields(&mut pf);

        let mut got_allow = pf.get_allow_filters();
        let mut got_deny = pf.get_deny_filters();
        got_allow.sort();
        got_deny.sort();

        let mut want_allow = split_csv(allow_expected);
        let mut want_deny = split_csv(deny_expected);
        want_allow.sort();
        want_deny.sort();

        assert_eq!(got_allow, want_allow, "allow filters differ");
        assert_eq!(got_deny, want_deny, "deny filters differ");
    }
}

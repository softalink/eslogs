//! Port of `pipe_stats.go` — the `| stats by (fields) func() as name, ...` pipe.
//!
//! Groups input rows by the `by (...)` fields and runs one
//! [`crate::stats::StatsProcessor`] per stats function per group. The
//! `StatsFunc`/`StatsProcessor` interfaces already live in [`crate::stats`];
//! this module wires the grouping engine (`pipeStatsGroup` map) and the
//! parallel per-worker shards merged in `flush`.
//!
//! # Grouping engine
//! Each worker owns a shard. A shard tracks groups in three maps keyed by the
//! by-field value's canonical form, mirroring Go's `pipeStatsGroupMap`:
//!
//! - `groups_u64`   — the value parses as a `u64`.
//! - `groups_neg`   — the value parses as a negative `i64`.
//! - `groups_str`   — everything else (and the marshaled key for
//!   multi-field grouping / the empty global-group key).
//!
//! This partitioning reproduces Go's numeric canonicalization exactly, so the
//! reconstructed output values match (`marshal_uint64_string`,
//! `marshal_int64_string`, or the raw key bytes).
//!
//! # PORT NOTES — deliberate divergences from Go
//! * `pipeStatsSwitch` (parsed via the unported lexer) is omitted; the
//!   `pub(crate)` constructors let the parser build funcs directly once the
//!   lexer lands. `pipeStatsMode` (remote/local/proxy) and the
//!   `splitToRemoteAndLocal` / `import_state` cluster paths ARE ported — see
//!   [`PipeStatsMode`] and `net_query_runner.rs`.
//! * `chunkedAllocator` and the fine-grained `stateSizeBudget` accounting are
//!   dropped — each processor owns its state (`HashMap`, `Box<dyn ...>`), matching
//!   the `stats.rs` allocator PORT NOTE. `memory::allowed()` still bounds nothing
//!   here; OOM guarding is coarse.
//! * The consecutive-equal-value dedup, the all-const fast paths for the
//!   multi-column grouping, the encoded-numeric fast paths in
//!   `updateStatsSingleColumn`, and the 64_000-byte chunked writer flush are
//!   collapsed into simpler per-row map lookups / a single output block. Results
//!   are identical; only allocation churn differs (tracked in the Layer-7
//!   backlog).
//! * `initStatsConcurrency` (sharding `count_uniq`/`uniq_values` processors) is
//!   skipped — the frozen `StatsProcessor` trait exposes no concurrency setter.
//! * `iff` (per-func `if(...)` filter) is modeled as a plain
//!   `Option<Box<dyn Filter>>` instead of Go's `ifFilter` (which also carries
//!   precomputed `allowFilters`); `update_needed_fields` approximates the latter
//!   via the filter's own `update_needed_fields`.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use esl_common::encoding::{marshal_bytes, unmarshal_bytes};

use crate::bitmap::Bitmap;
use crate::block_result::{BlockResult, ByStatsField, ResultColumn};
use crate::filter::Filter;
use crate::pipe::{Pipe, PipeProcessor};
use crate::prefix_filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::values_encoder::{
    marshal_int64_string, marshal_uint64_string, try_parse_int64, try_parse_uint64,
};

/// A single stats function to execute, its optional `if(...)` filter, and the
/// output field name (Go `pipeStatsFunc`).
pub struct PipeStatsFunc {
    f: Box<dyn StatsFunc>,
    iff: Option<Box<dyn Filter>>,
    result_name: String,
}

/// Builds a [`PipeStatsFunc`] (Go `pipeStatsFunc` literal).
pub(crate) fn new_pipe_stats_func(
    f: Box<dyn StatsFunc>,
    iff: Option<Box<dyn Filter>>,
    result_name: String,
) -> PipeStatsFunc {
    PipeStatsFunc {
        f,
        iff,
        result_name,
    }
}

/// Execution mode of a `stats` pipe (Go `pipeStatsMode`).
///
/// The default mode computes final values. In a cluster split the remote side
/// runs in `Remote` mode (exporting serialized per-group states instead of
/// final values) and the local side runs in `Local` mode (importing those
/// states and finalizing them). `Proxy` both imports and re-exports (a
/// `stats_remote` pipe split again by an intermediate select frontend).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum PipeStatsMode {
    #[default]
    Default,
    Remote,
    Local,
    Proxy,
}

impl PipeStatsMode {
    /// Go `pipeStatsMode.needExportState`.
    fn need_export_state(self) -> bool {
        matches!(self, PipeStatsMode::Remote | PipeStatsMode::Proxy)
    }

    /// Go `pipeStatsMode.needImportState`.
    fn need_import_state(self) -> bool {
        matches!(self, PipeStatsMode::Local | PipeStatsMode::Proxy)
    }
}

/// The `| stats by (...) ...` pipe.
pub struct PipeStats {
    by_fields: Arc<Vec<ByStatsField>>,
    funcs: Arc<Vec<PipeStatsFunc>>,
    mode: PipeStatsMode,
}

impl PipeStats {
    /// Sets the execution mode. Used by the parser for the `stats_remote`
    /// keyword (Go `parsePipeStatsExt` sets `pipeStatsModeRemote`).
    pub(crate) fn set_mode(&mut self, mode: PipeStatsMode) {
        self.mode = mode;
    }

    /// Sets the per-second step (`step / 1e9`) on every `rate()`/`rate_sum()`
    /// func of this pipe (Go `pipeStats.initRateFuncs`).
    ///
    /// PORT NOTE: called from `Query::init_stats_rate_func_steps` right after
    /// parse, before the `funcs` Arc is shared into processor shards, so it is
    /// uniquely owned and `Arc::get_mut` succeeds — matching Go's in-place
    /// mutation of `ps.funcs`.
    fn init_rate_funcs(&mut self, step: i64) {
        if step <= 0 {
            return;
        }
        let step_seconds = step as f64 / 1e9;
        if let Some(funcs) = Arc::get_mut(&mut self.funcs) {
            for f in funcs.iter_mut() {
                f.f.set_rate_step_seconds(step_seconds);
            }
        }
    }

    /// If a `_time` by-field has an explicit bucket size, uses it as the rate
    /// step and returns true (Go `pipeStats.initRateFuncsFromTimeBucket`).
    fn init_rate_funcs_from_time_bucket(&mut self) -> bool {
        let bucket = self.by_fields.iter().find_map(|bf| {
            if bf.name == "_time" && bf.bucket_size > 0.0 {
                Some(bf.bucket_size as i64)
            } else {
                None
            }
        });
        match bucket {
            Some(b) => {
                self.init_rate_funcs(b);
                true
            }
            None => false,
        }
    }
}

/// Builds a [`ByStatsField`] with no bucket configuration (the common
/// `by (name)` case).
// Ported for Go parity; not yet wired into a caller (see PARITY.md).
#[allow(dead_code)]
pub(crate) fn new_by_stats_field(name: &str) -> ByStatsField {
    ByStatsField {
        name: name.to_string(),
        bucket_size_str: String::new(),
        bucket_size: 0.0,
        bucket_offset_str: String::new(),
        bucket_offset: 0.0,
    }
}

fn has_bucket_config(bf: &ByStatsField) -> bool {
    !bf.bucket_size_str.is_empty() || !bf.bucket_offset_str.is_empty()
}

fn by_stats_field_to_string(bf: &ByStatsField) -> String {
    let mut s = bf.name.clone();
    if !bf.bucket_size_str.is_empty() {
        s += ":";
        s += &bf.bucket_size_str;
        if !bf.bucket_offset_str.is_empty() {
            s += " offset ";
            s += &bf.bucket_offset_str;
        }
    }
    s
}

/// Builds a [`PipeStats`] and validates the by-field / result-name constraints
/// (Go `parsePipeStatsExt` tail checks).
pub(crate) fn new_pipe_stats(
    by_fields: Vec<ByStatsField>,
    funcs: Vec<PipeStatsFunc>,
) -> Result<PipeStats, String> {
    if funcs.is_empty() {
        return Err("'stats' pipe must contain at least a single entry".to_string());
    }
    let mut seen_result_names: HashMap<&str, ()> = HashMap::new();
    for f in &funcs {
        if let Some(bf) = by_fields.iter().find(|bf| bf.name == f.result_name) {
            return Err(format!(
                "the {:?} is used as 'by' field [{}], so it cannot be used as result name for [{}]",
                f.result_name,
                by_stats_field_to_string(bf),
                f.f.to_string()
            ));
        }
        if seen_result_names
            .insert(f.result_name.as_str(), ())
            .is_some()
        {
            return Err(format!(
                "cannot use identical result name {:?} for [{}]",
                f.result_name,
                f.f.to_string()
            ));
        }
    }
    Ok(PipeStats {
        by_fields: Arc::new(by_fields),
        funcs: Arc::new(funcs),
        mode: PipeStatsMode::Default,
    })
}

impl Pipe for PipeStats {
    fn to_string(&self) -> String {
        let mut s = match self.mode {
            PipeStatsMode::Default => "stats",
            PipeStatsMode::Remote => "stats_remote",
            PipeStatsMode::Local => "stats_local",
            PipeStatsMode::Proxy => "stats_proxy",
        }
        .to_string();
        if !self.by_fields.is_empty() {
            let a: Vec<String> = self
                .by_fields
                .iter()
                .map(by_stats_field_to_string)
                .collect();
            s += " by (";
            s += &a.join(", ");
            s += ")";
        }
        let a: Vec<String> = if self.mode.need_import_state() {
            // Go: import-state modes render `import_state(name) as name` per func.
            self.funcs
                .iter()
                .map(|f| {
                    let result_name_quoted =
                        crate::stream_filter::quote_token_if_needed(&f.result_name);
                    format!("import_state({result_name_quoted}) as {result_name_quoted}")
                })
                .collect()
        } else {
            self.funcs
                .iter()
                .map(|f| {
                    // Go pipeStatsFunc.String(): funcStr [ " " iffStr ] " as " quoted(resultName).
                    let mut fs = f.f.to_string();
                    if let Some(iff) = &f.iff {
                        fs += &format!(" if ({})", iff.to_string());
                    }
                    fs += " as ";
                    fs += &crate::stream_filter::quote_token_if_needed(&f.result_name);
                    fs
                })
                .collect()
        };
        s += " ";
        s += &a.join(", ");
        s
    }

    /// Port of Go `pipeStats.splitToRemoteAndLocal`: the remote side exports
    /// per-group states, the local side imports and finalizes them.
    fn split_to_remote_and_local(&self, _timestamp: i64) -> crate::pipe::SplitPipesResult {
        let ps_remote = PipeStats {
            by_fields: self.by_fields.clone(),
            funcs: self.funcs.clone(),
            mode: PipeStatsMode::Remote,
        };

        let local_mode = match self.mode {
            PipeStatsMode::Default => PipeStatsMode::Local,
            PipeStatsMode::Remote => PipeStatsMode::Proxy,
            PipeStatsMode::Local => {
                esl_common::panicf!("BUG: stats_local cannot be split");
                unreachable!()
            }
            PipeStatsMode::Proxy => {
                esl_common::panicf!("BUG: stats_proxy cannot be split");
                unreachable!()
            }
        };
        let ps_local = PipeStats {
            by_fields: self.by_fields.clone(),
            funcs: self.funcs.clone(),
            mode: local_mode,
        };

        (Some(Box::new(ps_remote)), vec![Box::new(ps_local)])
    }

    /// Port of Go `pipeStats.hasFilterInWithQuery`: checks the per-func
    /// `if (...)` filters.
    fn has_filter_in_with_query(&self) -> bool {
        self.funcs.iter().any(|f| {
            f.iff
                .as_deref()
                .is_some_and(crate::storage_search::has_filter_in_with_query_for_filter)
        })
    }

    /// Port of Go `pipeStats.initFilterInValues`: rewrites the per-func
    /// `if (...)` filters.
    fn init_filter_in_values(
        &mut self,
        get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
        _timestamp: i64,
    ) -> Result<(), String> {
        if !self.has_filter_in_with_query() {
            return Ok(());
        }
        // The funcs Arc is only shared with processors, which are created
        // after subquery initialization; get_mut cannot fail here.
        let Some(funcs) = Arc::get_mut(&mut self.funcs) else {
            return Err("BUG: PipeStats funcs are shared during init_filter_in_values".to_string());
        };
        for func in funcs.iter_mut() {
            if let Some(iff) = func.iff.take() {
                func.iff = Some(
                    if crate::storage_search::has_filter_in_with_query_for_filter(iff.as_ref()) {
                        crate::storage_search::init_filter_in_values_for_filter(iff, get_values)?
                    } else {
                        iff
                    },
                );
            }
        }
        Ok(())
    }

    /// Port of Go `pipeStats.visitSubqueries`: propagates into the per-func
    /// `if (...)` filters. The funcs' iff is an owned `Box<dyn Filter>`, so it
    /// is visited in place (no shared-filter re-parse needed).
    fn visit_subqueries_mut(
        &mut self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
        let Some(funcs) = Arc::get_mut(&mut self.funcs) else {
            return;
        };
        for func in funcs.iter_mut() {
            if let Some(iff) = func.iff.as_mut() {
                iff.visit_subqueries_mut(timestamp, visit);
            }
        }
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        if self.mode.need_import_state() {
            // Go `pipeStats.updateNeededFieldsLocal`: the input carries the
            // by-field values plus one serialized-state column per func.
            pf.reset();
            for bf in self.by_fields.iter() {
                pf.add_allow_filter(&bf.name);
            }
            for f in self.funcs.iter() {
                pf.add_allow_filter(&f.result_name);
            }
            return;
        }

        let pf_orig = pf.clone();
        pf.reset();

        for f in self.funcs.iter() {
            if pf_orig.match_string(&f.result_name) {
                f.f.update_needed_fields(pf);
                if let Some(iff) = &f.iff {
                    iff.update_needed_fields(pf);
                }
            }
        }

        // byFields are needed unconditionally, since the output number of rows
        // depends on them.
        for bf in self.by_fields.iter() {
            pf.add_allow_filter(&bf.name);
        }
    }

    fn is_fixed_output_fields_order(&self) -> bool {
        true
    }

    fn stats_pipe_fields(&self) -> Option<crate::pipe::StatsPipeFields> {
        Some(crate::pipe::StatsPipeFields {
            by_fields: self.by_fields.iter().map(|bf| bf.name.clone()).collect(),
            funcs: self
                .funcs
                .iter()
                .map(|f| (f.result_name.clone(), f.f.is_row_label()))
                .collect(),
        })
    }

    /// Port of Go `pipeStats.addByTimeField`.
    fn stats_add_by_time_field(&mut self, step: i64, offset: i64) {
        if step <= 0 {
            return;
        }

        // add step to byFields
        let mut bf = ByStatsField {
            name: "_time".to_string(),
            bucket_size_str: format!("{step}"),
            bucket_size: step as f64,
            ..ByStatsField::default()
        };
        if offset != 0 {
            bf.bucket_offset_str = format!("{offset}");
            bf.bucket_offset = offset as f64;
        }

        let mut dst_fields = Vec::with_capacity(self.by_fields.len() + 1);
        dst_fields.push(bf);
        for f in self.by_fields.iter() {
            if f.name != "_time" {
                dst_fields.push(f.clone());
            }
        }

        self.by_fields = Arc::new(dst_fields);
    }

    fn init_stats_rate_funcs(&mut self, step: i64) {
        if !self.init_rate_funcs_from_time_bucket() {
            self.init_rate_funcs(step);
        }
    }

    /// Port of Go `pipeStats.resultFields`.
    fn fixed_result_fields(&self) -> Option<Vec<String>> {
        let mut field_names = Vec::with_capacity(self.by_fields.len() + self.funcs.len());
        for bf in self.by_fields.iter() {
            field_names.push(bf.name.clone());
        }
        for sf in self.funcs.iter() {
            field_names.push(sf.result_name.clone());
        }
        Some(field_names)
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let n = concurrency.max(1);
        Arc::new(PipeStatsProcessor {
            by_fields: self.by_fields.clone(),
            funcs: self.funcs.clone(),
            mode: self.mode,
            stop,
            pp_next,
            shards: (0..n).map(|_| Mutex::new(Shard::default())).collect(),
            err: Mutex::new(None),
        })
    }
}

/// Accumulated stats for one group: one processor per stats function.
struct PipeStatsGroup {
    sfps: Vec<Box<dyn StatsProcessor>>,
}

fn new_group(funcs: &[PipeStatsFunc]) -> PipeStatsGroup {
    PipeStatsGroup {
        sfps: funcs.iter().map(|f| f.f.new_stats_processor()).collect(),
    }
}

impl PipeStatsGroup {
    fn update_all_rows(
        &mut self,
        funcs: &[PipeStatsFunc],
        bms: &[Bitmap],
        br: &mut BlockResult,
        br_tmp: &mut BlockResult,
    ) {
        for (i, sfp) in self.sfps.iter_mut().enumerate() {
            let f = &funcs[i];
            match &f.iff {
                None => {
                    sfp.update_stats_for_all_rows(f.f.as_ref(), br);
                }
                Some(_) => {
                    br_tmp.init_from_filter_all_columns(br, &bms[i]);
                    if br_tmp.rows_len() > 0 {
                        sfp.update_stats_for_all_rows(f.f.as_ref(), br_tmp);
                    }
                }
            }
        }
    }

    fn update_row(
        &mut self,
        funcs: &[PipeStatsFunc],
        bms: &[Bitmap],
        br: &mut BlockResult,
        row_idx: usize,
    ) {
        for (i, sfp) in self.sfps.iter_mut().enumerate() {
            let f = &funcs[i];
            if f.iff.is_none() || bms[i].is_set_bit(row_idx) {
                sfp.update_stats_for_row(f.f.as_ref(), br, row_idx);
            }
        }
    }

    fn merge(&mut self, funcs: &[PipeStatsFunc], src: &PipeStatsGroup) {
        for (i, sfp) in self.sfps.iter_mut().enumerate() {
            sfp.merge_state(funcs[i].f.as_ref(), src.sfps[i].as_ref());
        }
    }

    /// Port of Go `pipeStatsGroup.importStateFromRow`: imports one serialized
    /// state per stats function from the given row.
    fn import_state_from_row(
        &mut self,
        funcs: &[PipeStatsFunc],
        column_values: &[Vec<Vec<u8>>],
        row_idx: usize,
        stop: Option<&AtomicBool>,
    ) -> Result<(), String> {
        if column_values.len() != self.sfps.len() {
            return Err(format!(
                "unexpected number of columns; got {}; want {}",
                column_values.len(),
                self.sfps.len()
            ));
        }

        for (i, sfp) in self.sfps.iter_mut().enumerate() {
            let v = &column_values[i][row_idx];
            sfp.import_state(v, stop)
                .map_err(|e| format!("cannot import state for {}: {e}", funcs[i].f.to_string()))?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct Shard {
    groups_u64: HashMap<u64, PipeStatsGroup>,
    groups_neg: HashMap<i64, PipeStatsGroup>,
    groups_str: HashMap<Vec<u8>, PipeStatsGroup>,
    bms: Vec<Bitmap>,
    br_tmp: BlockResult,
    key_buf: Vec<u8>,
    col_values: Vec<Vec<Vec<u8>>>,
}

fn bytes_to_str(b: &[u8]) -> &str {
    std::str::from_utf8(b).unwrap_or("")
}

/// Resolves the group for a generic value, choosing the u64 / negative-i64 /
/// string bucket exactly like Go's `getPipeStatsGroupGeneric`.
fn get_group_generic<'a>(
    shard: &'a mut Shard,
    funcs: &[PipeStatsFunc],
    v: &[u8],
) -> &'a mut PipeStatsGroup {
    let s = bytes_to_str(v);
    if let Some(n) = try_parse_uint64(s) {
        return shard
            .groups_u64
            .entry(n)
            .or_insert_with(|| new_group(funcs));
    }
    if v.first() == Some(&b'-')
        && let Some(n) = try_parse_int64(s)
    {
        return shard
            .groups_neg
            .entry(n)
            .or_insert_with(|| new_group(funcs));
    }
    shard
        .groups_str
        .entry(v.to_vec())
        .or_insert_with(|| new_group(funcs))
}

struct PipeStatsProcessor {
    by_fields: Arc<Vec<ByStatsField>>,
    funcs: Arc<Vec<PipeStatsFunc>>,
    mode: PipeStatsMode,
    stop: Arc<AtomicBool>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<Shard>>,
    /// First error hit while importing states (Go `pipeStatsProcessor.err`).
    err: Mutex<Option<String>>,
}

impl PipeStatsProcessor {
    /// Records the first error and stops the pipeline (Go
    /// `pipeStatsProcessor.setError`; Go's `cancel()` is folded into the
    /// shared stop token, see pipe.rs).
    fn set_error(&self, err: String) {
        let mut e = self.err.lock().unwrap();
        if e.is_none() {
            *e = Some(err);
        }
        drop(e);
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Port of Go `pipeStatsProcessorShard.writeBlockLocal`: consumes blocks
    /// produced by an upstream `stats_remote` pipe — the leading columns carry
    /// the `by (...)` values, followed by one serialized-state column per
    /// stats function — and imports the states into the local groups.
    fn write_block_local(&self, shard: &mut Shard, br: &mut BlockResult) -> Result<(), String> {
        let funcs: &[PipeStatsFunc] = &self.funcs;
        let by_len = self.by_fields.len();
        let stop = Some(self.stop.as_ref());

        let cols = br.get_columns();
        let mut column_values: Vec<Vec<Vec<u8>>> = Vec::with_capacity(cols.len());
        for &c in &cols {
            column_values.push(br.column_get_values(c).to_vec());
        }
        if column_values.len() < by_len + 1 {
            return Err(format!(
                "at least {} columns must exist; got {} columns only",
                by_len + 1,
                column_values.len()
            ));
        }
        let state_values = column_values.split_off(by_len);
        let by_field_values = column_values;

        if by_len == 0 {
            if br.rows_len() != 1 {
                return Err(format!(
                    "global stats must have only a single row; got {} rows",
                    br.rows_len()
                ));
            }
            let group = shard
                .groups_str
                .entry(Vec::new())
                .or_insert_with(|| new_group(funcs));
            return group.import_state_from_row(funcs, &state_values, 0, stop);
        }

        if by_len == 1 {
            let rows_len = br.rows_len();
            for (row_idx, v) in by_field_values[0].iter().enumerate().take(rows_len) {
                let group = get_group_generic(shard, funcs, v);
                group.import_state_from_row(funcs, &state_values, row_idx, stop)?;
                if self.stop.load(Ordering::SeqCst) {
                    break;
                }
            }
            return Ok(());
        }

        let mut key_buf = std::mem::take(&mut shard.key_buf);
        for row_idx in 0..br.rows_len() {
            key_buf.clear();
            for values in &by_field_values {
                marshal_bytes(&mut key_buf, &values[row_idx]);
            }
            let group = match shard.groups_str.entry(key_buf.clone()) {
                Entry::Occupied(o) => o.into_mut(),
                Entry::Vacant(v) => v.insert(new_group(funcs)),
            };
            group.import_state_from_row(funcs, &state_values, row_idx, stop)?;
            if self.stop.load(Ordering::SeqCst) {
                break;
            }
        }
        shard.key_buf = key_buf;
        Ok(())
    }

    fn apply_per_function_filters(&self, bms: &mut Vec<Bitmap>, br: &mut BlockResult) {
        let funcs = &self.funcs;
        if bms.len() < funcs.len() {
            bms.resize_with(funcs.len(), Bitmap::default);
        }
        let rows_len = br.rows_len();
        for (i, f) in funcs.iter().enumerate() {
            if let Some(iff) = &f.iff {
                let bm = &mut bms[i];
                bm.init(rows_len);
                bm.set_bits();
                iff.apply_to_block_result(br, bm);
            }
        }
    }

    fn write_block_default(&self, shard: &mut Shard, bms: &mut Vec<Bitmap>, br: &mut BlockResult) {
        self.apply_per_function_filters(bms, br);
        let funcs = &self.funcs;

        let mut br_tmp = std::mem::take(&mut shard.br_tmp);

        if self.by_fields.is_empty() {
            // Fast path — all rows go to a single group with the empty key.
            let group = shard
                .groups_str
                .entry(Vec::new())
                .or_insert_with(|| new_group(funcs));
            group.update_all_rows(funcs, bms, br, &mut br_tmp);
            shard.br_tmp = br_tmp;
            return;
        }

        if self.by_fields.len() == 1 {
            self.update_stats_single_column(shard, bms, br, &mut br_tmp);
            shard.br_tmp = br_tmp;
            return;
        }

        // Multi-column grouping.
        let mut col_values = std::mem::take(&mut shard.col_values);
        col_values.clear();
        for bf in self.by_fields.iter() {
            let c = br.get_column_by_name(&bf.name);
            let vals = if has_bucket_config(bf) {
                br.column_get_values_bucketed(c, bf)
            } else {
                br.column_get_values(c).to_vec()
            };
            col_values.push(vals);
        }

        let mut key_buf = std::mem::take(&mut shard.key_buf);
        let rows_len = br.rows_len();
        for i in 0..rows_len {
            key_buf.clear();
            for vals in &col_values {
                marshal_bytes(&mut key_buf, &vals[i]);
            }
            let group = match shard.groups_str.entry(key_buf.clone()) {
                Entry::Occupied(o) => o.into_mut(),
                Entry::Vacant(v) => v.insert(new_group(funcs)),
            };
            group.update_row(funcs, bms, br, i);
        }
        shard.key_buf = key_buf;
        shard.col_values = col_values;
        shard.br_tmp = br_tmp;
    }

    fn update_stats_single_column(
        &self,
        shard: &mut Shard,
        bms: &[Bitmap],
        br: &mut BlockResult,
        br_tmp: &mut BlockResult,
    ) {
        let funcs = &self.funcs;
        let bf = &self.by_fields[0];
        let c = br.get_column_by_name(&bf.name);
        let bucketed = has_bucket_config(bf);

        if br.column_is_const(c) && !bucketed {
            // Fast path — a single constant value for the whole block.
            let v = br.column_get_value_at_row(c, 0).as_bytes().to_vec();
            let group = get_group_generic(shard, funcs, &v);
            group.update_all_rows(funcs, bms, br, br_tmp);
            return;
        }

        let values: Vec<Vec<u8>> = if bucketed {
            br.column_get_values_bucketed(c, bf)
        } else {
            br.column_get_values(c).to_vec()
        };
        for (i, value) in values.iter().enumerate() {
            let group = get_group_generic(shard, funcs, value);
            group.update_row(funcs, bms, br, i);
        }
    }
}

impl PipeProcessor for PipeStatsProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }
        let idx = worker_id.min(self.shards.len() - 1);
        let mut shard = self.shards[idx].lock().unwrap();
        if self.mode.need_import_state() {
            if let Err(err) = self.write_block_local(&mut shard, br) {
                self.set_error(err);
            }
            return;
        }
        let mut bms = std::mem::take(&mut shard.bms);
        self.write_block_default(&mut shard, &mut bms, br);
        shard.bms = bms;
    }

    fn flush(&self) -> Result<(), String> {
        // Go flush: return the error recorded via setError, if any.
        if let Some(err) = self.err.lock().unwrap().take() {
            return Err(err);
        }

        let funcs = &self.funcs;

        // Merge all shard states into one set of maps.
        let mut merged = Shard::default();
        for m in &self.shards {
            let mut shard = m.lock().unwrap();
            for (k, g) in shard.groups_u64.drain() {
                match merged.groups_u64.entry(k) {
                    Entry::Occupied(mut o) => o.get_mut().merge(funcs, &g),
                    Entry::Vacant(v) => {
                        v.insert(g);
                    }
                }
            }
            for (k, g) in shard.groups_neg.drain() {
                match merged.groups_neg.entry(k) {
                    Entry::Occupied(mut o) => o.get_mut().merge(funcs, &g),
                    Entry::Vacant(v) => {
                        v.insert(g);
                    }
                }
            }
            for (k, g) in shard.groups_str.drain() {
                match merged.groups_str.entry(k) {
                    Entry::Occupied(mut o) => o.get_mut().merge(funcs, &g),
                    Entry::Vacant(v) => {
                        v.insert(g);
                    }
                }
            }
        }

        if self.stop.load(Ordering::SeqCst) {
            return Ok(());
        }

        // Special case — zero matching rows for a global stats query.
        if self.by_fields.is_empty()
            && merged.groups_u64.is_empty()
            && merged.groups_neg.is_empty()
            && merged.groups_str.is_empty()
        {
            merged.groups_str.insert(Vec::new(), new_group(funcs));
        }

        self.write_merged(&merged)
    }
}

impl PipeStatsProcessor {
    fn write_merged(&self, merged: &Shard) -> Result<(), String> {
        let funcs = &self.funcs;
        let by_len = self.by_fields.len();
        let ncols = by_len + funcs.len();

        let mut rcs: Vec<ResultColumn> = Vec::with_capacity(ncols);
        for bf in self.by_fields.iter() {
            rcs.push(ResultColumn {
                name: bf.name.clone(),
                values: Vec::new(),
            });
        }
        for f in funcs.iter() {
            rcs.push(ResultColumn {
                name: f.result_name.clone(),
                values: Vec::new(),
            });
        }

        let mut rows_count = 0usize;
        let stop = Some(self.stop.as_ref());

        // groups_u64 → single by-field value reconstructed as a u64 string.
        for (n, group) in &merged.groups_u64 {
            if self.stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            let mut key = Vec::new();
            marshal_uint64_string(&mut key, *n);
            self.write_group(&mut rcs, &[key], group, stop);
            rows_count += 1;
        }
        for (n, group) in &merged.groups_neg {
            if self.stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            let mut key = Vec::new();
            marshal_int64_string(&mut key, *n);
            self.write_group(&mut rcs, &[key], group, stop);
            rows_count += 1;
        }
        for (key, group) in &merged.groups_str {
            if self.stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            let by_values = decode_by_values(key, by_len);
            self.write_group(&mut rcs, &by_values, group, stop);
            rows_count += 1;
        }

        if rows_count == 0 {
            return Ok(());
        }

        let mut br = BlockResult::default();
        br.set_result_columns(rcs, rows_count);
        self.pp_next.write_block(0, &mut br);
        Ok(())
    }

    fn write_group(
        &self,
        rcs: &mut [ResultColumn],
        by_values: &[Vec<u8>],
        group: &PipeStatsGroup,
        stop: Option<&AtomicBool>,
    ) {
        let by_len = self.by_fields.len();
        for (i, v) in by_values.iter().enumerate() {
            rcs[i].add_value(v);
        }
        // Go `pipeStatsWriter.writePipeStatsGroup`: remote/proxy modes export
        // the serialized state instead of the finalized value.
        let need_export_state = self.mode.need_export_state();
        for (i, sfp) in group.sfps.iter().enumerate() {
            let mut dst = Vec::new();
            if need_export_state {
                sfp.export_state(&mut dst, stop);
            } else {
                sfp.finalize_stats(self.funcs[i].f.as_ref(), &mut dst, stop);
            }
            rcs[by_len + i].add_value(&dst);
        }
    }
}

/// Reconstructs the by-field values from a group key.
///
/// For `by_len <= 1` the key is the raw single value (or empty for the global
/// group). For `by_len > 1` the key is the marshaled concatenation.
fn decode_by_values(key: &[u8], by_len: usize) -> Vec<Vec<u8>> {
    match by_len {
        0 => Vec::new(),
        1 => vec![key.to_vec()],
        _ => {
            let mut out = Vec::with_capacity(by_len);
            let mut rest = key;
            while !rest.is_empty() {
                let (v, n) = unmarshal_bytes(rest);
                match v {
                    Some(b) if n > 0 => {
                        out.push(b.to_vec());
                        rest = &rest[n as usize..];
                    }
                    _ => break,
                }
            }
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::Field;
    use crate::stats_count::new_stats_count;
    use crate::stats_sum::new_stats_sum;

    struct Collector {
        blocks: Mutex<Vec<Vec<Field>>>,
    }
    impl Collector {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                blocks: Mutex::new(Vec::new()),
            })
        }
    }
    impl PipeProcessor for Collector {
        fn write_block(&self, _worker_id: usize, br: &mut BlockResult) {
            let cols = br.get_columns();
            let names: Vec<String> = cols
                .iter()
                .map(|&c| br.column_name(c).to_string())
                .collect();
            let n = br.rows_len();
            let mut out = self.blocks.lock().unwrap();
            for i in 0..n {
                let mut fields = Vec::with_capacity(cols.len());
                for (ci, &c) in cols.iter().enumerate() {
                    fields.push(Field {
                        name: names[ci].clone(),
                        value: br.column_get_value_at_row(c, i).to_string(),
                    });
                }
                out.push(fields);
            }
        }
        fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn run(pipe: &PipeStats, blocks: Vec<Vec<Vec<Field>>>) -> Vec<Vec<Field>> {
        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = pipe.new_pipe_processor(1, stop, sink.clone());
        for rows in &blocks {
            let mut br = BlockResult::default();
            br.must_init_from_rows(rows);
            pp.write_block(0, &mut br);
        }
        pp.flush().unwrap();
        let out = sink.blocks.lock().unwrap();
        out.clone()
    }

    fn count_func(name: &str) -> PipeStatsFunc {
        new_pipe_stats_func(
            Box::new(new_stats_count(vec!["*".to_string()])),
            None,
            name.to_string(),
        )
    }

    // The benchmark-critical query: `* | stats count() rows`.
    #[test]
    fn test_stats_count_global() {
        let ps = new_pipe_stats(vec![], vec![count_func("rows")]).unwrap();
        let blocks = vec![
            vec![vec![field("a", "1")], vec![field("a", "2")]],
            vec![vec![field("a", "3")]],
        ];
        let out = run(&ps, blocks);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].len(), 1);
        assert_eq!(out[0][0].name, "rows");
        assert_eq!(out[0][0].value, "3");
    }

    // `init_stats_rate_funcs` propagates the per-second step to rate() funcs so
    // the processor normalizes the row count (Go `initRateFuncs`). step = 3s in
    // nanos over 3 rows -> rate = 3 / 3 = 1.
    #[test]
    fn test_stats_rate_step_applied() {
        let mut ps = new_pipe_stats(
            vec![],
            vec![new_pipe_stats_func(
                Box::new(crate::stats_rate::new_stats_rate()),
                None,
                "r".to_string(),
            )],
        )
        .unwrap();
        ps.init_stats_rate_funcs(3_000_000_000);
        let blocks = vec![vec![
            vec![field("a", "1")],
            vec![field("a", "2")],
            vec![field("a", "3")],
        ]];
        let out = run(&ps, blocks);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][0].name, "r");
        assert_eq!(out[0][0].value, "1");
    }

    // An explicit `_time` bucket wins over the query-range step
    // (Go `initRateFuncsFromTimeBucket`): a 2s `_time` bucket over 4 rows
    // yields rate = 4 / 2 = 2, ignoring the passed 4s step.
    #[test]
    fn test_stats_rate_step_from_time_bucket() {
        let mut time_bucket = new_by_stats_field("_time");
        time_bucket.bucket_size_str = "2000000000".to_string();
        time_bucket.bucket_size = 2_000_000_000.0;
        let mut ps = new_pipe_stats(
            vec![time_bucket],
            vec![new_pipe_stats_func(
                Box::new(crate::stats_rate::new_stats_rate()),
                None,
                "r".to_string(),
            )],
        )
        .unwrap();
        // Passed step is 4s, but the 2s _time bucket takes precedence.
        ps.init_stats_rate_funcs(4_000_000_000);
        let blocks = vec![vec![
            vec![field("_time", "1000000000")],
            vec![field("_time", "1000000001")],
            vec![field("_time", "1000000002")],
            vec![field("_time", "1000000003")],
        ]];
        let out = run(&ps, blocks);
        // One group (the single 2s bucket): rate = 4 / 2 = 2.
        let r = out
            .iter()
            .find_map(|row| row.iter().find(|f| f.name == "r"))
            .expect("rate column present");
        assert_eq!(r.value, "2");
    }

    // Global count over zero rows must still emit a single `0` row.
    #[test]
    fn test_stats_count_global_zero_rows() {
        let ps = new_pipe_stats(vec![], vec![count_func("rows")]).unwrap();
        let out = run(&ps, vec![]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][0].value, "0");
    }

    #[test]
    fn test_stats_count_by_single_field() {
        let ps = new_pipe_stats(vec![new_by_stats_field("host")], vec![count_func("cnt")]).unwrap();
        let blocks = vec![vec![
            vec![field("host", "a"), field("x", "1")],
            vec![field("host", "b"), field("x", "2")],
            vec![field("host", "a"), field("x", "3")],
        ]];
        let mut out = run(&ps, blocks);
        out.sort_by(|r1, r2| r1[0].value.cmp(&r2[0].value));
        assert_eq!(out.len(), 2);
        assert_eq!(out[0][0].name, "host");
        assert_eq!(out[0][0].value, "a");
        assert_eq!(out[0][1].value, "2");
        assert_eq!(out[1][0].value, "b");
        assert_eq!(out[1][1].value, "1");
    }

    #[test]
    fn test_stats_sum_by_two_fields() {
        let ps = new_pipe_stats(
            vec![new_by_stats_field("k1"), new_by_stats_field("k2")],
            vec![new_pipe_stats_func(
                Box::new(new_stats_sum(vec!["v".to_string()])),
                None,
                "s".to_string(),
            )],
        )
        .unwrap();
        let blocks = vec![vec![
            vec![field("k1", "a"), field("k2", "x"), field("v", "10")],
            vec![field("k1", "a"), field("k2", "x"), field("v", "5")],
            vec![field("k1", "b"), field("k2", "y"), field("v", "3")],
        ]];
        let mut out = run(&ps, blocks);
        out.sort_by(|r1, r2| {
            r1[0]
                .value
                .cmp(&r2[0].value)
                .then(r1[1].value.cmp(&r2[1].value))
        });
        assert_eq!(out.len(), 2);
        // a/x → 15
        assert_eq!(out[0][0].value, "a");
        assert_eq!(out[0][1].value, "x");
        assert_eq!(out[0][2].name, "s");
        assert_eq!(out[0][2].value, "15");
        // b/y → 3
        assert_eq!(out[1][0].value, "b");
        assert_eq!(out[1][1].value, "y");
        assert_eq!(out[1][2].value, "3");
    }

    #[test]
    fn test_stats_numeric_grouping_merges_across_workers() {
        // Two workers each see the same numeric key; flush must merge them.
        let ps = new_pipe_stats(vec![new_by_stats_field("n")], vec![count_func("c")]).unwrap();
        let sink = Collector::new();
        let stop = Arc::new(AtomicBool::new(false));
        let pp = pipe_new(&ps, 2, stop, sink.clone());
        let mut b0 = BlockResult::default();
        b0.must_init_from_rows(&[vec![field("n", "5")], vec![field("n", "5")]]);
        pp.write_block(0, &mut b0);
        let mut b1 = BlockResult::default();
        b1.must_init_from_rows(&[vec![field("n", "5")]]);
        pp.write_block(1, &mut b1);
        pp.flush().unwrap();
        let out = sink.blocks.lock().unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][0].value, "5");
        assert_eq!(out[0][1].value, "3");
    }

    fn pipe_new(
        ps: &PipeStats,
        c: usize,
        stop: Arc<AtomicBool>,
        sink: Arc<Collector>,
    ) -> Arc<dyn PipeProcessor> {
        ps.new_pipe_processor(c, stop, sink)
    }

    #[test]
    fn test_stats_to_string() {
        let ps = new_pipe_stats(vec![new_by_stats_field("host")], vec![count_func("cnt")]).unwrap();
        assert_eq!(ps.to_string(), "stats by (host) count(*) as cnt");
    }

    #[test]
    fn test_stats_duplicate_result_name_err() {
        let res = new_pipe_stats(vec![], vec![count_func("c"), count_func("c")]);
        match res {
            Ok(_) => panic!("expected duplicate result name error"),
            Err(err) => assert!(err.contains("identical result name")),
        }
    }
}

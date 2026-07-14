//! Port of EsLogs `lib/logstorage/stats_count_uniq.go`.
//!
//! `count_uniq(fields...)` counts the number of unique values across the given
//! fields. Small numbers of unique values are tracked in a single
//! [`StatsCountUniqSet`] (`uniq_values`); once it crosses
//! [`STATS_COUNT_UNIQ_VALUES_MAX_LEN`] the values are sharded across
//! `concurrency` sets so the final merge can be parallelized.
//!
//! This module also hosts the `pub(crate)` helpers shared with
//! [`crate::stats_count_uniq_hash`]: the `uint64`-set (de)serialization and
//! mutation helpers, [`fast_hash_uint64`], [`field_names_string`],
//! [`need_stop`] and the Go `unsafe.Sizeof` size constants.
//!
//! # PORT NOTES
//!
//! * **Allocator dropped.** Go threads a `*chunkedAllocator` for arena
//!   allocation of set state; per the frozen [`crate::stats`] contract the
//!   processors own their state directly, so the allocator is dropped.
//!
//! * **State-size deltas.** The `i64` state-size deltas that bound query memory
//!   are computed from the hardcoded Go `unsafe.Sizeof` values on 64-bit
//!   ([`SIZE_OF_MAP`]/[`SIZE_OF_UINT64`]/[`SIZE_OF_STRING`]), so the deltas are
//!   bit-identical to Go even though the Rust `HashSet`/`Vec` byte sizes differ.
//!   Go distinguishes a `nil` map (first insert costs a map header) from a
//!   populated one; the port uses `HashSet::is_empty()` as the `nil` proxy —
//!   these sets are only emptied by a full reset, so the proxy matches Go.
//!
//! * **`sf` unused / config captured.** The frozen `StatsFunc` trait exposes no
//!   downcast, so each processor captures its `fields`/`limit` at construction
//!   (via `new_stats_processor`); the `sf` parameter of the `update_*`/
//!   `finalize_stats` methods is therefore ignored.
//!
//! * **Sequential merges.** Go parallelizes the per-CPU shard merges with
//!   goroutines; the port performs the same reductions sequentially — the
//!   result is identical (a deterministic set union).
//!
//! * **Immutable `merge_state`.** Go's `mergeState` moves state out of `src`
//!   (mutating it). The frozen trait passes `other` immutably, so the port
//!   clones the needed state instead of moving it. The merged result is
//!   identical; only freeing `other`'s memory is deferred to its `Drop`.
//!
//! * **`export_state`/`finalize_stats` are `&self`.** Go's `exportState`
//!   mutates the processor (collapsing `shardss` into `shards` via
//!   `mergeShardssParallel`). The frozen trait methods take `&self`, so the port
//!   computes the merged view into a local instead. The `merge_shardss_parallel`
//!   inherent method is retained for the tests, which call it explicitly to
//!   reach the post-merge state that Go's `exportState` leaves behind.

use std::any::Any;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};

use esl_common::encoding::{
    marshal_bytes, marshal_uint64, marshal_var_uint64, unmarshal_bytes, unmarshal_uint64,
    unmarshal_var_uint64,
};
use xxhash_rust::xxh64::xxh64;

use crate::block_result::BlockResult;
use crate::filter_generic::quote_field_filter_if_needed;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::values_encoder::{
    ValueType, try_parse_int64_bytes, try_parse_uint64_bytes, unmarshal_int64, unmarshal_uint8,
    unmarshal_uint16, unmarshal_uint32, unmarshal_uint64 as decode_uint64,
};

// ---------------------------------------------------------------------------
// Shared constants and helpers (also used by stats_count_uniq_hash)
// ---------------------------------------------------------------------------

/// Maximum number of values tracked in `uniq_values` before switching to
/// sharded tracking (Go `statsCountUniqValuesMaxLen`).
pub(crate) const STATS_COUNT_UNIQ_VALUES_MAX_LEN: u64 = 4 << 10;

/// Go `unsafe.Sizeof(map)` on 64-bit (a map is a single pointer).
pub(crate) const SIZE_OF_MAP: i64 = 8;
/// Go `unsafe.Sizeof(uint64)`.
pub(crate) const SIZE_OF_UINT64: i64 = 8;
/// Go `unsafe.Sizeof(string)` on 64-bit (pointer + length).
pub(crate) const SIZE_OF_STRING: i64 = 16;
/// Go `unsafe.Sizeof(statsCountUniqSet)` (four map pointers), used to account
/// for the shard array allocation.
pub(crate) const SIZE_OF_SET: i64 = 32;

/// Returns true if the stop flag is set (Go `needStop`).
pub(crate) fn need_stop(stop: Option<&AtomicBool>) -> bool {
    stop.is_some_and(|s| s.load(Ordering::SeqCst))
}

/// Joins the given field names for the `String()` representation (Go
/// `fieldNamesString`).
pub(crate) fn field_names_string(fields: &[String]) -> String {
    fields
        .iter()
        .map(|f| quote_field_filter_if_needed(f))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Fast non-cryptographic hash of a `uint64` (Go `fastHashUint64`).
pub(crate) fn fast_hash_uint64(mut x: u64) -> u64 {
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    x.wrapping_mul(2685821657736338717)
}

// -- uint64 set helpers ------------------------------------------------------

/// Appends the marshaled `uint64` set to `dst` (Go `marshalUint64Set`).
pub(crate) fn marshal_uint64_set(dst: &mut Vec<u8>, m: &HashSet<u64>, stop: Option<&AtomicBool>) {
    marshal_var_uint64(dst, m.len() as u64);
    for &k in m {
        marshal_uint64(dst, k);
        if need_stop(stop) {
            return;
        }
    }
}

/// Unmarshals a `uint64` set from `src` (Go `unmarshalUint64Set`). Returns the
/// remaining input and the state-size increase.
pub(crate) fn unmarshal_uint64_set<'a>(
    dst: &mut HashSet<u64>,
    src: &'a [u8],
    stop: Option<&AtomicBool>,
) -> Result<(&'a [u8], i64), String> {
    if need_stop(stop) {
        return Ok((&[], 0));
    }
    let (entries_len, n) = unmarshal_var_uint64(src);
    if n <= 0 {
        return Err("cannot unmarshal the number of uint64 entries".to_string());
    }
    let mut src = &src[n as usize..];
    if (src.len() as u64) < 8 * entries_len {
        return Err(format!(
            "cannot unmarshal {} uint64 values from {} bytes; need {} bytes",
            entries_len,
            src.len(),
            8 * entries_len
        ));
    }
    let mut m = HashSet::with_capacity(entries_len as usize);
    for _ in 0..entries_len {
        let u = unmarshal_uint64(&src[..8]);
        src = &src[8..];
        m.insert(u);
        if need_stop(stop) {
            return Ok((&[], 0));
        }
    }
    *dst = m;
    Ok((src, 8 * entries_len as i64))
}

/// Inserts `n` into the set and returns the state-size increase (Go
/// `setUint64Set`).
pub(crate) fn set_uint64_set(set: &mut HashSet<u64>, n: u64) -> i64 {
    let was_empty = set.is_empty();
    set.insert(n);
    if was_empty {
        SIZE_OF_MAP + SIZE_OF_UINT64
    } else {
        SIZE_OF_UINT64
    }
}

/// Inserts `n` if absent and returns the state-size increase (Go
/// `updateUint64Set`).
pub(crate) fn update_uint64_set(set: &mut HashSet<u64>, n: u64) -> i64 {
    if set.contains(&n) {
        return 0;
    }
    set_uint64_set(set, n)
}

/// Merges `src` into `dst` (Go `mergeUint64Set`).
pub(crate) fn merge_uint64_set(
    dst: &mut HashSet<u64>,
    src: &HashSet<u64>,
    stop: Option<&AtomicBool>,
) {
    if src.is_empty() {
        return;
    }
    for &n in src {
        if need_stop(stop) {
            return;
        }
        dst.insert(n);
    }
}

// -- string set helpers (count_uniq only) ------------------------------------

fn marshal_string_set(dst: &mut Vec<u8>, m: &HashSet<Vec<u8>>, stop: Option<&AtomicBool>) {
    marshal_var_uint64(dst, m.len() as u64);
    for k in m {
        marshal_bytes(dst, k);
        if need_stop(stop) {
            return;
        }
    }
}

fn unmarshal_string_set<'a>(
    dst: &mut HashSet<Vec<u8>>,
    src: &'a [u8],
    stop: Option<&AtomicBool>,
) -> Result<(&'a [u8], i64), String> {
    if need_stop(stop) {
        return Ok((&[], 0));
    }
    let (entries_len, n) = unmarshal_var_uint64(src);
    if n <= 0 {
        return Err("cannot unmarshal the number of string entries".to_string());
    }
    let mut src = &src[n as usize..];
    let mut state_size = 0i64;
    let mut m = HashSet::with_capacity(entries_len as usize);
    for _ in 0..entries_len {
        let (v, n) = unmarshal_bytes(src);
        if n <= 0 {
            return Err("cannot unmarshal string entry".to_string());
        }
        let v = v.unwrap();
        src = &src[n as usize..];
        state_size += SIZE_OF_STRING + v.len() as i64;
        m.insert(v.to_vec());
        if need_stop(stop) {
            return Ok((&[], 0));
        }
    }
    *dst = m;
    Ok((src, state_size))
}

fn set_string_set(set: &mut HashSet<Vec<u8>>, v: Vec<u8>) -> i64 {
    let was_empty = set.is_empty();
    set.insert(v);
    if was_empty {
        SIZE_OF_MAP + SIZE_OF_STRING
    } else {
        SIZE_OF_STRING
    }
}

// ---------------------------------------------------------------------------
// StatsCountUniq (StatsFunc)
// ---------------------------------------------------------------------------

/// `count_uniq(fields...)` stats function (Go `statsCountUniq`).
#[derive(Debug, Default, Clone)]
pub struct StatsCountUniq {
    pub(crate) fields: Vec<String>,
    pub(crate) limit: u64,
}

impl StatsCountUniq {
    /// Constructs a `count_uniq` function over the given fields (exposed for the
    /// future stats parser).
    #[allow(dead_code)] // consumed by the not-yet-ported stats parser.
    pub(crate) fn new(fields: Vec<String>, limit: u64) -> Self {
        Self { fields, limit }
    }
}

impl StatsFunc for StatsCountUniq {
    fn to_string(&self) -> String {
        let mut s = format!("count_uniq({})", field_names_string(&self.fields));
        if self.limit > 0 {
            s += &format!(" limit {}", self.limit);
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.fields);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsCountUniqProcessor {
            fields: self.fields.clone(),
            limit: self.limit,
            // PORT NOTE: Go leaves concurrency 0 until the pipe sets it; the port
            // defaults to 1 (a non-zero shard count) so the processor is usable
            // before the pipe overrides it.
            concurrency: 1,
            ..Default::default()
        })
    }
}

// ---------------------------------------------------------------------------
// StatsCountUniqSet
// ---------------------------------------------------------------------------

/// Tracks unique values partitioned by kind (Go `statsCountUniqSet`).
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct StatsCountUniqSet {
    pub(crate) timestamps: HashSet<u64>,
    pub(crate) u64: HashSet<u64>,
    pub(crate) negative64: HashSet<u64>,
    pub(crate) strings: HashSet<Vec<u8>>,
}

impl StatsCountUniqSet {
    fn entries_count(&self) -> u64 {
        (self.timestamps.len() + self.u64.len() + self.negative64.len() + self.strings.len()) as u64
    }

    fn export_state(&self, dst: &mut Vec<u8>, stop: Option<&AtomicBool>) {
        marshal_uint64_set(dst, &self.timestamps, stop);
        marshal_uint64_set(dst, &self.u64, stop);
        marshal_uint64_set(dst, &self.negative64, stop);
        marshal_string_set(dst, &self.strings, stop);
    }

    fn import_state<'a>(
        &mut self,
        src: &'a [u8],
        stop: Option<&AtomicBool>,
    ) -> Result<(&'a [u8], i64), String> {
        let mut total = 0i64;
        let (src, s) = unmarshal_uint64_set(&mut self.timestamps, src, stop)
            .map_err(|e| format!("cannot unmarshal timestamps: {e}"))?;
        total += s;
        let (src, s) = unmarshal_uint64_set(&mut self.u64, src, stop)
            .map_err(|e| format!("cannot unmarshal uint64 values: {e}"))?;
        total += s;
        let (src, s) = unmarshal_uint64_set(&mut self.negative64, src, stop)
            .map_err(|e| format!("cannot unmarshal negative64 values: {e}"))?;
        total += s;
        let (src, s) = unmarshal_string_set(&mut self.strings, src, stop)
            .map_err(|e| format!("cannot unmarshal string values: {e}"))?;
        total += s;
        Ok((src, total))
    }

    fn update_state_timestamp(&mut self, ts: i64) -> i64 {
        update_uint64_set(&mut self.timestamps, ts as u64)
    }

    fn update_state_uint64(&mut self, n: u64) -> i64 {
        update_uint64_set(&mut self.u64, n)
    }

    fn update_state_negative_int64(&mut self, n: i64) -> i64 {
        update_uint64_set(&mut self.negative64, n as u64)
    }

    fn update_state_string(&mut self, v: &[u8]) -> i64 {
        if self.strings.contains(v) {
            return 0;
        }
        set_string_set(&mut self.strings, v.to_vec()) + v.len() as i64
    }

    fn merge_state(&mut self, src: &StatsCountUniqSet, stop: Option<&AtomicBool>) {
        merge_uint64_set(&mut self.timestamps, &src.timestamps, stop);
        merge_uint64_set(&mut self.u64, &src.u64, stop);
        merge_uint64_set(&mut self.negative64, &src.negative64, stop);
        if !src.strings.is_empty() {
            for k in &src.strings {
                if need_stop(stop) {
                    return;
                }
                self.strings.insert(k.clone());
            }
        }
    }
}

fn shard_index_uint64(n: u64, len: usize) -> usize {
    (fast_hash_uint64(n) % len as u64) as usize
}

fn shard_index_string(v: &[u8], len: usize) -> usize {
    (xxh64(v, 0) % len as u64) as usize
}

// ---------------------------------------------------------------------------
// StatsCountUniqProcessor
// ---------------------------------------------------------------------------

/// Accumulates `count_uniq` state for one group (Go `statsCountUniqProcessor`).
#[derive(Debug, Default)]
pub struct StatsCountUniqProcessor {
    pub(crate) fields: Vec<String>,
    pub(crate) limit: u64,
    /// Number of shards used once `uniq_values` overflows.
    pub(crate) concurrency: usize,
    pub(crate) uniq_values: StatsCountUniqSet,
    pub(crate) shards: Option<Vec<StatsCountUniqSet>>,
    pub(crate) shardss: Vec<Vec<StatsCountUniqSet>>,
    key_buf: Vec<u8>,
}

// PORT NOTE: `fields`/`limit`/`key_buf` are compared by `PartialEq` for the
// export/import round-trip tests; both sides construct with equal config and run
// no updates, so scratch state stays default and equal.
impl PartialEq for StatsCountUniqProcessor {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
            && self.limit == other.limit
            && self.concurrency == other.concurrency
            && self.uniq_values == other.uniq_values
            && self.shards == other.shards
            && self.shardss == other.shardss
            && self.key_buf == other.key_buf
    }
}

impl StatsCountUniqProcessor {
    fn entries_count(&self) -> u64 {
        match &self.shards {
            None => self.uniq_values.entries_count(),
            Some(sh) => sh.iter().map(|s| s.entries_count()).sum(),
        }
    }

    fn limit_reached(&self) -> bool {
        self.limit > 0 && self.entries_count() > self.limit
    }

    fn probably_move_uniq_values_to_shards(&mut self) -> i64 {
        if self.uniq_values.entries_count() < STATS_COUNT_UNIQ_VALUES_MAX_LEN {
            return 0;
        }
        self.move_uniq_values_to_shards()
    }

    fn move_uniq_values_to_shards(&mut self) -> i64 {
        let cpus = self.concurrency.max(1);
        let mut shards: Vec<StatsCountUniqSet> =
            (0..cpus).map(|_| StatsCountUniqSet::default()).collect();
        let state_size = cpus as i64 * SIZE_OF_SET;

        let uv = std::mem::take(&mut self.uniq_values);
        for &ts in &uv.timestamps {
            let idx = shard_index_uint64(ts, cpus);
            set_uint64_set(&mut shards[idx].timestamps, ts);
        }
        for &n in &uv.u64 {
            let idx = shard_index_uint64(n, cpus);
            set_uint64_set(&mut shards[idx].u64, n);
        }
        for &n in &uv.negative64 {
            let idx = shard_index_uint64(n, cpus);
            set_uint64_set(&mut shards[idx].negative64, n);
        }
        for s in &uv.strings {
            let idx = shard_index_string(s, cpus);
            set_string_set(&mut shards[idx].strings, s.clone());
        }
        self.shards = Some(shards);
        state_size
    }

    fn update_state_string(&mut self, v: &[u8]) -> i64 {
        if self.shards.is_none() {
            let inc = self.uniq_values.update_state_string(v);
            if inc > 0 {
                return inc + self.probably_move_uniq_values_to_shards();
            }
            return inc;
        }
        let cpus = self.shards.as_ref().unwrap().len();
        let idx = shard_index_string(v, cpus);
        self.shards.as_mut().unwrap()[idx].update_state_string(v)
    }

    fn update_state_timestamp(&mut self, ts: i64) -> i64 {
        if self.shards.is_none() {
            let inc = self.uniq_values.update_state_timestamp(ts);
            if inc > 0 {
                return inc + self.probably_move_uniq_values_to_shards();
            }
            return inc;
        }
        let cpus = self.shards.as_ref().unwrap().len();
        let idx = shard_index_uint64(ts as u64, cpus);
        self.shards.as_mut().unwrap()[idx].update_state_timestamp(ts)
    }

    fn update_state_uint64(&mut self, n: u64) -> i64 {
        if self.shards.is_none() {
            let inc = self.uniq_values.update_state_uint64(n);
            if inc > 0 {
                return inc + self.probably_move_uniq_values_to_shards();
            }
            return inc;
        }
        let cpus = self.shards.as_ref().unwrap().len();
        let idx = shard_index_uint64(n, cpus);
        self.shards.as_mut().unwrap()[idx].update_state_uint64(n)
    }

    fn update_state_negative_int64(&mut self, n: i64) -> i64 {
        if self.shards.is_none() {
            let inc = self.uniq_values.update_state_negative_int64(n);
            if inc > 0 {
                return inc + self.probably_move_uniq_values_to_shards();
            }
            return inc;
        }
        let cpus = self.shards.as_ref().unwrap().len();
        let idx = shard_index_uint64(n as u64, cpus);
        self.shards.as_mut().unwrap()[idx].update_state_negative_int64(n)
    }

    fn update_state_generic(&mut self, v: &[u8]) -> i64 {
        if let Some(n) = try_parse_uint64_bytes(v) {
            return self.update_state_uint64(n);
        }
        if v.first() == Some(&b'-')
            && let Some(n) = try_parse_int64_bytes(v)
        {
            return self.update_state_negative_int64(n);
        }
        self.update_state_string(v)
    }

    fn update_state_int64(&mut self, n: i64) -> i64 {
        if n >= 0 {
            self.update_state_uint64(n as u64)
        } else {
            self.update_state_negative_int64(n)
        }
    }

    /// Collapses `shardss` (and any current `shards`) into merged `shards`.
    /// Mirrors Go's `mergeShardssParallel`; the tests call this explicitly to
    /// mirror the mutation Go hides inside `exportState`.
    #[allow(dead_code)] // used by tests and the not-yet-ported pipe_stats.
    pub(crate) fn merge_shardss_parallel(&mut self) {
        if self.shardss.is_empty() {
            return;
        }
        let mut shardss = std::mem::take(&mut self.shardss);
        if let Some(sh) = self.shards.take() {
            shardss.push(sh);
        }
        let n = shardss[0].len();
        let mut result: Vec<StatsCountUniqSet> = Vec::with_capacity(n);
        for cpu in 0..n {
            let mut sus = std::mem::take(&mut shardss[0][cpu]);
            for grp in &mut shardss[1..] {
                let other = std::mem::take(&mut grp[cpu]);
                sus.merge_state(&other, None);
            }
            result.push(sus);
        }
        self.shards = Some(result);
    }

    /// Non-mutating merged view of `shardss` (+ current `shards`) for the
    /// `&self` `export_state`/`finalize_stats`.
    fn merged_shards_view(&self) -> Option<Vec<StatsCountUniqSet>> {
        if self.shardss.is_empty() {
            return self.shards.clone();
        }
        let mut groups: Vec<&Vec<StatsCountUniqSet>> = self.shardss.iter().collect();
        if let Some(sh) = &self.shards {
            groups.push(sh);
        }
        let n = groups[0].len();
        let mut result = Vec::with_capacity(n);
        for cpu in 0..n {
            let mut sus = groups[0][cpu].clone();
            for grp in &groups[1..] {
                sus.merge_state(&grp[cpu], None);
            }
            result.push(sus);
        }
        Some(result)
    }

    fn import_shards(&mut self, shards: Vec<StatsCountUniqSet>, state_size: i64) -> i64 {
        if shards.len() == self.concurrency {
            self.shards = Some(shards);
            return state_size;
        }
        // Reshard to align with self.concurrency.
        let mut inc = 0i64;
        for shard in &shards {
            inc += self.import_shard(shard);
        }
        inc
    }

    fn import_shard(&mut self, shard: &StatsCountUniqSet) -> i64 {
        let mut inc = 0i64;
        for &ts in &shard.timestamps {
            inc += self.update_state_timestamp(ts as i64);
        }
        for &n in &shard.u64 {
            inc += self.update_state_uint64(n);
        }
        for &n in &shard.negative64 {
            inc += self.update_state_negative_int64(n as i64);
        }
        for s in &shard.strings {
            inc += self.update_state_string(s);
        }
        inc
    }

    fn update_stats_for_all_rows_single_column(
        &mut self,
        br: &mut BlockResult,
        column_name: &str,
    ) -> i64 {
        let mut inc = 0i64;
        let r = br.get_column_by_name(column_name);
        if br.column_is_time(r) {
            let timestamps = br.get_timestamps();
            for i in 0..timestamps.len() {
                if i > 0 && timestamps[i - 1] == timestamps[i] {
                    continue;
                }
                let ts = timestamps[i];
                inc += self.update_state_timestamp(ts);
            }
            return inc;
        }
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0);
            if v.is_empty() {
                return 0;
            }
            // PORT NOTE: v borrows br; update_state_generic reads it but only
            // stores clones into self, so holding the borrow is sound.
            let v = v.to_vec();
            return self.update_state_generic(&v);
        }
        match br.column_value_type(r) {
            ValueType::UINT8 => {
                let ve = br.column_get_values_encoded(r).unwrap().to_vec();
                for i in 0..ve.len() {
                    if i > 0 && ve[i - 1] == ve[i] {
                        continue;
                    }
                    inc += self.update_state_uint64(unmarshal_uint8(&ve[i]) as u64);
                }
                inc
            }
            ValueType::UINT16 => {
                let ve = br.column_get_values_encoded(r).unwrap().to_vec();
                for i in 0..ve.len() {
                    if i > 0 && ve[i - 1] == ve[i] {
                        continue;
                    }
                    inc += self.update_state_uint64(unmarshal_uint16(&ve[i]) as u64);
                }
                inc
            }
            ValueType::UINT32 => {
                let ve = br.column_get_values_encoded(r).unwrap().to_vec();
                for i in 0..ve.len() {
                    if i > 0 && ve[i - 1] == ve[i] {
                        continue;
                    }
                    inc += self.update_state_uint64(unmarshal_uint32(&ve[i]) as u64);
                }
                inc
            }
            ValueType::UINT64 => {
                let ve = br.column_get_values_encoded(r).unwrap().to_vec();
                for i in 0..ve.len() {
                    if i > 0 && ve[i - 1] == ve[i] {
                        continue;
                    }
                    inc += self.update_state_uint64(decode_uint64(&ve[i]));
                }
                inc
            }
            ValueType::INT64 => {
                let ve = br.column_get_values_encoded(r).unwrap().to_vec();
                for i in 0..ve.len() {
                    if i > 0 && ve[i - 1] == ve[i] {
                        continue;
                    }
                    inc += self.update_state_int64(unmarshal_int64(&ve[i]));
                }
                inc
            }
            // PORT NOTE: Go has a valueTypeDict fast path via forEachDictValue;
            // block_result.rs does not expose dict internals, so DICT folds into
            // this decoded-values path (and IPv4/float64/iso8601/string). Both
            // feed update_state_generic, which dedups, so the counted set is
            // identical.
            _ => {
                let values = br.column_get_values(r).to_vec();
                for i in 0..values.len() {
                    if values[i].is_empty() {
                        continue;
                    }
                    if i > 0 && values[i - 1] == values[i] {
                        continue;
                    }
                    inc += self.update_state_generic(&values[i]);
                }
                inc
            }
        }
    }

    fn update_stats_for_row_single_column(
        &mut self,
        br: &mut BlockResult,
        column_name: &str,
        row_idx: usize,
    ) -> i64 {
        let r = br.get_column_by_name(column_name);
        if br.column_is_time(r) {
            let ts = br.get_timestamps()[row_idx];
            return self.update_state_timestamp(ts);
        }
        if br.column_is_const(r) {
            let v = br.column_get_value_at_row(r, 0);
            if v.is_empty() {
                return 0;
            }
            let v = v.to_vec();
            return self.update_state_generic(&v);
        }
        match br.column_value_type(r) {
            ValueType::UINT8 => {
                let n = unmarshal_uint8(&br.column_get_values_encoded(r).unwrap()[row_idx]);
                self.update_state_uint64(n as u64)
            }
            ValueType::UINT16 => {
                let n = unmarshal_uint16(&br.column_get_values_encoded(r).unwrap()[row_idx]);
                self.update_state_uint64(n as u64)
            }
            ValueType::UINT32 => {
                let n = unmarshal_uint32(&br.column_get_values_encoded(r).unwrap()[row_idx]);
                self.update_state_uint64(n as u64)
            }
            ValueType::UINT64 => {
                let n = decode_uint64(&br.column_get_values_encoded(r).unwrap()[row_idx]);
                self.update_state_uint64(n)
            }
            ValueType::INT64 => {
                let n = unmarshal_int64(&br.column_get_values_encoded(r).unwrap()[row_idx]);
                self.update_state_int64(n)
            }
            _ => {
                let v = br.column_get_value_at_row(r, row_idx);
                if v.is_empty() {
                    return 0;
                }
                let v = v.to_vec();
                self.update_state_generic(&v)
            }
        }
    }
}

impl StatsProcessor for StatsCountUniqProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        if self.limit_reached() {
            return 0;
        }
        if self.fields.len() == 1 {
            let f = self.fields[0].clone();
            return self.update_stats_for_all_rows_single_column(br, &f);
        }

        // Slow path for multiple columns.
        let fields = self.fields.clone();
        // PORT NOTE: each column's values are cloned because the block-result
        // accessor borrows `&mut BlockResult`, so multiple columns cannot be
        // borrowed simultaneously (Go shares the underlying string slices).
        let cols: Vec<Vec<Vec<u8>>> = fields
            .iter()
            .map(|f| {
                let r = br.get_column_by_name(f);
                br.column_get_values(r).to_vec()
            })
            .collect();
        let rows = br.rows_len();

        let mut inc = 0i64;
        let mut key_buf = std::mem::take(&mut self.key_buf);
        for i in 0..rows {
            let mut seen = true;
            for values in &cols {
                if i == 0 || values[i - 1] != values[i] {
                    seen = false;
                    break;
                }
            }
            if seen {
                continue;
            }
            let mut all_empty = true;
            key_buf.clear();
            for values in &cols {
                let v = &values[i];
                if !v.is_empty() {
                    all_empty = false;
                }
                marshal_bytes(&mut key_buf, v);
            }
            if all_empty {
                continue;
            }
            inc += self.update_state_string(&key_buf);
        }
        self.key_buf = key_buf;
        inc
    }

    fn update_stats_for_row(
        &mut self,
        _sf: &dyn StatsFunc,
        br: &mut BlockResult,
        row_idx: usize,
    ) -> i64 {
        if self.limit_reached() {
            return 0;
        }
        if self.fields.len() == 1 {
            let f = self.fields[0].clone();
            return self.update_stats_for_row_single_column(br, &f, row_idx);
        }

        let fields = self.fields.clone();
        let mut key_buf = std::mem::take(&mut self.key_buf);
        key_buf.clear();
        let mut all_empty = true;
        for f in &fields {
            let r = br.get_column_by_name(f);
            let v = br.column_get_value_at_row(r, row_idx);
            if !v.is_empty() {
                all_empty = false;
            }
            marshal_bytes(&mut key_buf, v);
        }
        let inc = if all_empty {
            0
        } else {
            self.update_state_string(&key_buf)
        };
        self.key_buf = key_buf;
        inc
    }

    fn merge_state(&mut self, _sf: &dyn StatsFunc, other: &dyn StatsProcessor) {
        if self.limit_reached() {
            return;
        }
        let src = other
            .as_any()
            .downcast_ref::<StatsCountUniqProcessor>()
            .expect("merge_state: other must be a StatsCountUniqProcessor");

        if self.shards.is_none() {
            if src.shards.is_none() {
                let src_uv = src.uniq_values.clone();
                self.uniq_values.merge_state(&src_uv, None);
                self.probably_move_uniq_values_to_shards();
                return;
            }
            self.move_uniq_values_to_shards();
        }

        // PORT NOTE: Go moves src.shards out of src; the immutable `other`
        // forces a clone here. If src has no shards yet, build them from its
        // uniq_values (sized to our concurrency) without mutating src.
        let src_shards = match &src.shards {
            Some(sh) => sh.clone(),
            None => build_shards_from_uniq_values(&src.uniq_values, self.concurrency.max(1)),
        };
        self.shardss.push(src_shards);
    }

    fn export_state(&self, dst: &mut Vec<u8>, stop: Option<&AtomicBool>) {
        match self.merged_shards_view() {
            None => {
                marshal_var_uint64(dst, 1);
                self.uniq_values.export_state(dst, stop);
            }
            Some(shards) => {
                marshal_var_uint64(dst, shards.len() as u64);
                for s in &shards {
                    s.export_state(dst, stop);
                }
            }
        }
    }

    fn import_state(&mut self, src: &[u8], stop: Option<&AtomicBool>) -> Result<i64, String> {
        let (shards_len, n) = unmarshal_var_uint64(src);
        if n <= 0 {
            return Err("cannot read the number of shards".to_string());
        }
        if shards_len < 1 {
            return Err("the number of shards must be at least 1".to_string());
        }
        let src = &src[n as usize..];

        if shards_len == 1 {
            let (tail, state_size) = self
                .uniq_values
                .import_state(src, stop)
                .map_err(|e| format!("cannot read uniqValues state: {e}"))?;
            if !tail.is_empty() {
                return Err(format!(
                    "unexpected tail left after importing uniqValues state; len(tail)={}",
                    tail.len()
                ));
            }
            return Ok(state_size);
        }

        let mut shards: Vec<StatsCountUniqSet> = (0..shards_len)
            .map(|_| StatsCountUniqSet::default())
            .collect();
        let mut state_size = SIZE_OF_SET * shards_len as i64;
        let mut src = src;
        for (i, shard) in shards.iter_mut().enumerate() {
            let (tail, s) = shard
                .import_state(src, stop)
                .map_err(|e| format!("cannot read state for shard[{i}]: {e}"))?;
            src = tail;
            state_size += s;
        }
        if !src.is_empty() {
            return Err(format!(
                "unexpected tail left after importing shards' state; len(tail)={}",
                src.len()
            ));
        }
        Ok(self.import_shards(shards, state_size))
    }

    fn finalize_stats(&self, _sf: &dyn StatsFunc, dst: &mut Vec<u8>, _stop: Option<&AtomicBool>) {
        let mut n = match self.merged_shards_view() {
            None => self.uniq_values.entries_count(),
            Some(sh) => sh.iter().map(|s| s.entries_count()).sum(),
        };
        if self.limit > 0 && n > self.limit {
            n = self.limit;
        }
        dst.extend_from_slice(n.to_string().as_bytes());
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Builds a `concurrency`-wide shard array from `uv` without mutating it
/// (used by `merge_state` since `other` is immutable).
fn build_shards_from_uniq_values(uv: &StatsCountUniqSet, cpus: usize) -> Vec<StatsCountUniqSet> {
    let mut shards: Vec<StatsCountUniqSet> =
        (0..cpus).map(|_| StatsCountUniqSet::default()).collect();
    for &ts in &uv.timestamps {
        set_uint64_set(&mut shards[shard_index_uint64(ts, cpus)].timestamps, ts);
    }
    for &n in &uv.u64 {
        set_uint64_set(&mut shards[shard_index_uint64(n, cpus)].u64, n);
    }
    for &n in &uv.negative64 {
        set_uint64_set(&mut shards[shard_index_uint64(n, cpus)].negative64, n);
    }
    for s in &uv.strings {
        set_string_set(&mut shards[shard_index_string(s, cpus)].strings, s.clone());
    }
    shards
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: TestParseStatsCountUniqSuccess/Failure (parser) and
    // TestStatsCountUniq (expectPipeResults) are deferred until the stats
    // parser and pipe_stats are ported. The state round-trip tests below are
    // ported faithfully.

    fn new_processor() -> StatsCountUniqProcessor {
        StatsCountUniqProcessor {
            concurrency: 2,
            ..Default::default()
        }
    }

    fn u64set(items: &[u64]) -> HashSet<u64> {
        items.iter().copied().collect()
    }

    fn strset(items: &[&str]) -> HashSet<Vec<u8>> {
        items.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn check(mut sup: StatsCountUniqProcessor, data_len_expected: usize, entries_expected: u64) {
        // Go's exportState mutates via mergeShardssParallel; hoisted here so sup
        // reaches the post-merge state that the DeepEqual below compares.
        sup.merge_shardss_parallel();
        let mut data = Vec::new();
        sup.export_state(&mut data, None);
        assert_eq!(data.len(), data_len_expected, "unexpected dataLen");
        assert_eq!(sup.entries_count(), entries_expected, "unexpected entries");

        let mut sup2 = new_processor();
        sup2.import_state(&data, None).unwrap();
        assert_eq!(
            sup2.entries_count(),
            entries_expected,
            "unexpected imported entries"
        );
        assert_eq!(sup, sup2, "unexpected state imported");
    }

    #[test]
    fn test_stats_count_uniq_export_import_state() {
        // Zero state.
        check(new_processor(), 5, 0);

        // uniqValues initialized.
        let mut sup = new_processor();
        sup.uniq_values = StatsCountUniqSet {
            timestamps: u64set(&[123, 0]),
            u64: u64set(&[43]),
            negative64: u64set(&[8234932]),
            strings: strset(&["foo", "bar"]),
        };
        check(sup, 45, 6);

        // shards initialized.
        let mut sup = new_processor();
        sup.shards = Some(vec![
            StatsCountUniqSet {
                timestamps: u64set(&[123, 0]),
                u64: u64set(&[43]),
                negative64: u64set(&[8234932]),
                strings: strset(&["foo", "bar"]),
            },
            StatsCountUniqSet {
                timestamps: u64set(&[10, 1123, 3234324]),
                u64: u64set(&[42]),
                ..Default::default()
            },
        ]);
        check(sup, 81, 10);

        // shardss initialized.
        let mut sup = new_processor();
        sup.shardss = vec![
            vec![
                StatsCountUniqSet {
                    strings: strset(&["afoo", "bar"]),
                    ..Default::default()
                },
                StatsCountUniqSet {
                    negative64: u64set(&[10, 1123, 3234324]),
                    ..Default::default()
                },
            ],
            vec![
                StatsCountUniqSet {
                    timestamps: u64set(&[123, 0]),
                    u64: u64set(&[43]),
                    strings: strset(&["foo", "bar", "baz"]),
                    ..Default::default()
                },
                StatsCountUniqSet {
                    timestamps: u64set(&[10]),
                    ..Default::default()
                },
            ],
        ];
        check(sup, 82, 11);

        // both shards and shardss initialized.
        let mut sup = new_processor();
        sup.shardss = vec![
            vec![
                StatsCountUniqSet {
                    strings: strset(&["afoo", "bar"]),
                    ..Default::default()
                },
                StatsCountUniqSet {
                    strings: strset(&["foo", "abar"]),
                    ..Default::default()
                },
            ],
            vec![
                StatsCountUniqSet {
                    strings: strset(&["afoo", "bar", "baz"]),
                    ..Default::default()
                },
                StatsCountUniqSet {
                    strings: strset(&["foo", "abar", "abaz"]),
                    ..Default::default()
                },
            ],
        ];
        sup.shards = Some(vec![
            StatsCountUniqSet {
                strings: strset(&["bar"]),
                ..Default::default()
            },
            StatsCountUniqSet {
                strings: strset(&["foo", "abarz"]),
                ..Default::default()
            },
        ]);
        check(sup, 42, 7);
    }

    #[test]
    fn test_stats_count_uniq_export_import_state_distinct_concurrency() {
        fn new_proc(concurrency: usize) -> StatsCountUniqProcessor {
            StatsCountUniqProcessor {
                concurrency,
                ..Default::default()
            }
        }

        fn f(remote_concurrency: usize, local_concurrency: usize) {
            let mut remote = new_proc(remote_concurrency);
            let mut shards = Vec::new();
            for i in 0..remote_concurrency {
                let mut shard = StatsCountUniqSet::default();
                shard.timestamps.insert(i as u64);
                shard.u64.insert(i as u64 + 1000);
                shard.negative64.insert((-(i as i64) - 1) as u64);
                shard.strings.insert(format!("string-{i}").into_bytes());
                shards.push(shard);
            }
            remote.shards = Some(shards);
            let remote_entries = remote.entries_count();

            let mut data = Vec::new();
            remote.merge_shardss_parallel();
            remote.export_state(&mut data, None);

            let mut local = new_proc(local_concurrency);
            local.import_state(&data, None).unwrap();
            assert_eq!(local.entries_count(), remote_entries);
        }

        f(2, 3);
        f(2, 1);
        f(2, 100);
        f(128, 3);
        f(128, 100);
    }
}

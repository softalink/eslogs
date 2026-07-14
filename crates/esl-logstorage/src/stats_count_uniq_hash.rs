//! Port of EsLogs `lib/logstorage/stats_count_uniq_hash.go`.
//!
//! `count_uniq_hash(fields...)` is like [`crate::stats_count_uniq`] but tracks
//! string values by their `xxhash.Sum64` hash instead of the full bytes, trading
//! a tiny collision probability for lower memory use. It reuses the shared
//! `uint64`-set helpers, [`fast_hash_uint64`], [`field_names_string`],
//! [`need_stop`] and the size constants from [`crate::stats_count_uniq`].
//!
//! # PORT NOTES
//!
//! * Same allocator / `sf` / sequential-merge / immutable-`merge_state` /
//!   `&self`-export notes as [`crate::stats_count_uniq`].
//!
//! * **`xxh64` bit-identical to Go.** String hashing uses
//!   `xxhash_rust::xxh64::xxh64(v, 0)`, matching Go's `cespare/xxhash`
//!   (`XXH64` with seed 0), so hashes round-trip identically.
//!
//! * **Faithful `update_state_uint64` → `timestamps`.** In the Go source, the
//!   set-level `updateStateUint64` writes into the `timestamps` map (not `u64`);
//!   `u64` is only populated by `moveUniqValuesToShards`. The port preserves
//!   this exactly — semantics are the spec.

use std::any::Any;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;

use esl_common::encoding::{marshal_bytes, marshal_var_uint64, unmarshal_var_uint64};
use xxhash_rust::xxh64::xxh64;

use crate::block_result::BlockResult;
use crate::prefix_filter::Filter;
use crate::stats::{StatsFunc, StatsProcessor};
use crate::stats_count_uniq::{
    SIZE_OF_SET, STATS_COUNT_UNIQ_VALUES_MAX_LEN, fast_hash_uint64, field_names_string,
    marshal_uint64_set, merge_uint64_set, set_uint64_set, unmarshal_uint64_set, update_uint64_set,
};
use crate::values_encoder::{
    ValueType, try_parse_int64_bytes, try_parse_uint64_bytes, unmarshal_int64, unmarshal_uint8,
    unmarshal_uint16, unmarshal_uint32, unmarshal_uint64 as decode_uint64,
};

// ---------------------------------------------------------------------------
// StatsCountUniqHash (StatsFunc)
// ---------------------------------------------------------------------------

/// `count_uniq_hash(fields...)` stats function (Go `statsCountUniqHash`).
#[derive(Debug, Default, Clone)]
pub struct StatsCountUniqHash {
    pub(crate) fields: Vec<Vec<u8>>,
    pub(crate) limit: u64,
}

impl StatsCountUniqHash {
    /// Constructs a `count_uniq_hash` function (exposed for the future parser).
    #[allow(dead_code)] // consumed by the not-yet-ported stats parser.
    pub(crate) fn new(fields: Vec<Vec<u8>>, limit: u64) -> Self {
        Self { fields, limit }
    }
}

impl StatsFunc for StatsCountUniqHash {
    fn to_string(&self) -> String {
        let mut s = format!("count_uniq_hash({})", field_names_string(&self.fields));
        if self.limit > 0 {
            s += &format!(" limit {}", self.limit);
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut Filter) {
        pf.add_allow_filters(&self.fields);
    }

    fn new_stats_processor(&self) -> Box<dyn StatsProcessor> {
        Box::new(StatsCountUniqHashProcessor {
            fields: self.fields.clone(),
            limit: self.limit,
            concurrency: 1,
            ..Default::default()
        })
    }
}

// ---------------------------------------------------------------------------
// StatsCountUniqHashSet
// ---------------------------------------------------------------------------

/// Tracks unique values by kind, with strings tracked by hash (Go
/// `statsCountUniqHashSet`).
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct StatsCountUniqHashSet {
    pub(crate) timestamps: HashSet<u64>,
    pub(crate) u64: HashSet<u64>,
    pub(crate) negative64: HashSet<u64>,
    pub(crate) strings: HashSet<u64>,
}

impl StatsCountUniqHashSet {
    fn entries_count(&self) -> u64 {
        (self.timestamps.len() + self.u64.len() + self.negative64.len() + self.strings.len()) as u64
    }

    fn export_state(&self, dst: &mut Vec<u8>, stop: Option<&AtomicBool>) {
        marshal_uint64_set(dst, &self.timestamps, stop);
        marshal_uint64_set(dst, &self.u64, stop);
        marshal_uint64_set(dst, &self.negative64, stop);
        marshal_uint64_set(dst, &self.strings, stop);
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
        let (src, s) = unmarshal_uint64_set(&mut self.strings, src, stop)
            .map_err(|e| format!("cannot unmarshal string values: {e}"))?;
        total += s;
        Ok((src, total))
    }

    fn update_state_timestamp(&mut self, ts: i64) -> i64 {
        update_uint64_set(&mut self.timestamps, ts as u64)
    }

    // PORT NOTE: faithful to the Go source — uint64 values go into `timestamps`,
    // not `u64`.
    fn update_state_uint64(&mut self, n: u64) -> i64 {
        update_uint64_set(&mut self.timestamps, n)
    }

    fn update_state_negative_int64(&mut self, n: i64) -> i64 {
        update_uint64_set(&mut self.negative64, n as u64)
    }

    fn update_state_string_hash(&mut self, h: u64) -> i64 {
        update_uint64_set(&mut self.strings, h)
    }

    fn merge_state(&mut self, src: &StatsCountUniqHashSet, stop: Option<&AtomicBool>) {
        merge_uint64_set(&mut self.timestamps, &src.timestamps, stop);
        merge_uint64_set(&mut self.u64, &src.u64, stop);
        merge_uint64_set(&mut self.negative64, &src.negative64, stop);
        merge_uint64_set(&mut self.strings, &src.strings, stop);
    }
}

fn shard_index_uint64(n: u64, len: usize) -> usize {
    (fast_hash_uint64(n) % len as u64) as usize
}

fn shard_index_string_hash(h: u64, len: usize) -> usize {
    (h % len as u64) as usize
}

// ---------------------------------------------------------------------------
// StatsCountUniqHashProcessor
// ---------------------------------------------------------------------------

/// Accumulates `count_uniq_hash` state for one group (Go
/// `statsCountUniqHashProcessor`).
#[derive(Debug, Default)]
pub struct StatsCountUniqHashProcessor {
    pub(crate) fields: Vec<Vec<u8>>,
    pub(crate) limit: u64,
    pub(crate) concurrency: usize,
    pub(crate) uniq_values: StatsCountUniqHashSet,
    pub(crate) shards: Option<Vec<StatsCountUniqHashSet>>,
    pub(crate) shardss: Vec<Vec<StatsCountUniqHashSet>>,
    key_buf: Vec<u8>,
}

impl PartialEq for StatsCountUniqHashProcessor {
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

impl StatsCountUniqHashProcessor {
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
        let mut shards: Vec<StatsCountUniqHashSet> = (0..cpus)
            .map(|_| StatsCountUniqHashSet::default())
            .collect();
        let state_size = cpus as i64 * SIZE_OF_SET;

        let uv = std::mem::take(&mut self.uniq_values);
        for &ts in &uv.timestamps {
            set_uint64_set(&mut shards[shard_index_uint64(ts, cpus)].timestamps, ts);
        }
        for &n in &uv.u64 {
            set_uint64_set(&mut shards[shard_index_uint64(n, cpus)].u64, n);
        }
        for &n in &uv.negative64 {
            set_uint64_set(&mut shards[shard_index_uint64(n, cpus)].negative64, n);
        }
        for &h in &uv.strings {
            set_uint64_set(&mut shards[shard_index_string_hash(h, cpus)].strings, h);
        }
        self.shards = Some(shards);
        state_size
    }

    fn update_state_string(&mut self, v: &[u8]) -> i64 {
        let h = xxh64(v, 0);
        self.update_state_string_hash(h)
    }

    fn update_state_string_hash(&mut self, h: u64) -> i64 {
        if self.shards.is_none() {
            let inc = self.uniq_values.update_state_string_hash(h);
            if inc > 0 {
                return inc + self.probably_move_uniq_values_to_shards();
            }
            return inc;
        }
        let cpus = self.shards.as_ref().unwrap().len();
        let idx = shard_index_string_hash(h, cpus);
        self.shards.as_mut().unwrap()[idx].update_state_string_hash(h)
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

    /// See [`crate::stats_count_uniq::StatsCountUniqProcessor::merge_shardss_parallel`].
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
        let mut result: Vec<StatsCountUniqHashSet> = Vec::with_capacity(n);
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

    fn merged_shards_view(&self) -> Option<Vec<StatsCountUniqHashSet>> {
        if self.shardss.is_empty() {
            return self.shards.clone();
        }
        let mut groups: Vec<&Vec<StatsCountUniqHashSet>> = self.shardss.iter().collect();
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

    fn import_shards(&mut self, shards: Vec<StatsCountUniqHashSet>, state_size: i64) -> i64 {
        if shards.len() == self.concurrency {
            self.shards = Some(shards);
            return state_size;
        }
        let mut inc = 0i64;
        for shard in &shards {
            inc += self.import_shard(shard);
        }
        inc
    }

    fn import_shard(&mut self, shard: &StatsCountUniqHashSet) -> i64 {
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
        for &h in &shard.strings {
            inc += self.update_state_string_hash(h);
        }
        inc
    }

    fn update_stats_for_all_rows_single_column(
        &mut self,
        br: &mut BlockResult,
        column_name: &[u8],
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
            // PORT NOTE: DICT folds into the decoded-values path — see
            // stats_count_uniq.rs for the rationale.
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
        column_name: &[u8],
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

impl StatsProcessor for StatsCountUniqHashProcessor {
    fn update_stats_for_all_rows(&mut self, _sf: &dyn StatsFunc, br: &mut BlockResult) -> i64 {
        if self.limit_reached() {
            return 0;
        }
        if self.fields.len() == 1 {
            let f = self.fields[0].clone();
            return self.update_stats_for_all_rows_single_column(br, &f);
        }

        let fields = self.fields.clone();
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
            .downcast_ref::<StatsCountUniqHashProcessor>()
            .expect("merge_state: other must be a StatsCountUniqHashProcessor");

        if self.shards.is_none() {
            if src.shards.is_none() {
                let src_uv = src.uniq_values.clone();
                self.uniq_values.merge_state(&src_uv, None);
                self.probably_move_uniq_values_to_shards();
                return;
            }
            self.move_uniq_values_to_shards();
        }

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

        let mut shards: Vec<StatsCountUniqHashSet> = (0..shards_len)
            .map(|_| StatsCountUniqHashSet::default())
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

fn build_shards_from_uniq_values(
    uv: &StatsCountUniqHashSet,
    cpus: usize,
) -> Vec<StatsCountUniqHashSet> {
    let mut shards: Vec<StatsCountUniqHashSet> = (0..cpus)
        .map(|_| StatsCountUniqHashSet::default())
        .collect();
    for &ts in &uv.timestamps {
        set_uint64_set(&mut shards[shard_index_uint64(ts, cpus)].timestamps, ts);
    }
    for &n in &uv.u64 {
        set_uint64_set(&mut shards[shard_index_uint64(n, cpus)].u64, n);
    }
    for &n in &uv.negative64 {
        set_uint64_set(&mut shards[shard_index_uint64(n, cpus)].negative64, n);
    }
    for &h in &uv.strings {
        set_uint64_set(&mut shards[shard_index_string_hash(h, cpus)].strings, h);
    }
    shards
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: parser + expectPipeResults tests deferred until the stats
    // parser and pipe_stats are ported.

    fn new_processor() -> StatsCountUniqHashProcessor {
        StatsCountUniqHashProcessor {
            concurrency: 2,
            ..Default::default()
        }
    }

    fn u64set(items: &[u64]) -> HashSet<u64> {
        items.iter().copied().collect()
    }

    fn check(
        mut sup: StatsCountUniqHashProcessor,
        data_len_expected: usize,
        entries_expected: u64,
    ) {
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
    fn test_stats_count_uniq_hash_export_import_state() {
        // Zero state.
        check(new_processor(), 5, 0);

        // uniqValues initialized.
        let mut sup = new_processor();
        sup.uniq_values = StatsCountUniqHashSet {
            timestamps: u64set(&[123, 0]),
            u64: u64set(&[43]),
            negative64: u64set(&[8234932]),
            strings: u64set(&[1111, 2222]),
        };
        check(sup, 53, 6);

        // shards initialized.
        let mut sup = new_processor();
        sup.shards = Some(vec![
            StatsCountUniqHashSet {
                timestamps: u64set(&[123, 0]),
                u64: u64set(&[43]),
                negative64: u64set(&[8234932]),
                strings: u64set(&[1111, 2222]),
            },
            StatsCountUniqHashSet {
                timestamps: u64set(&[10, 1123, 3234324]),
                u64: u64set(&[42]),
                ..Default::default()
            },
        ]);
        check(sup, 89, 10);

        // shardss initialized.
        let mut sup = new_processor();
        sup.shardss = vec![
            vec![
                StatsCountUniqHashSet {
                    strings: u64set(&[11111, 22222]),
                    ..Default::default()
                },
                StatsCountUniqHashSet {
                    negative64: u64set(&[10, 1123, 3234324]),
                    ..Default::default()
                },
            ],
            vec![
                StatsCountUniqHashSet {
                    timestamps: u64set(&[123, 0]),
                    u64: u64set(&[43]),
                    strings: u64set(&[111, 222, 3333]),
                    ..Default::default()
                },
                StatsCountUniqHashSet {
                    timestamps: u64set(&[10]),
                    ..Default::default()
                },
            ],
        ];
        check(sup, 105, 12);
    }

    #[test]
    fn test_stats_count_uniq_hash_export_import_state_distinct_concurrency() {
        fn new_proc(concurrency: usize) -> StatsCountUniqHashProcessor {
            StatsCountUniqHashProcessor {
                concurrency,
                ..Default::default()
            }
        }

        fn f(remote_concurrency: usize, local_concurrency: usize) {
            let mut remote = new_proc(remote_concurrency);
            let mut shards = Vec::new();
            for i in 0..remote_concurrency {
                let mut shard = StatsCountUniqHashSet::default();
                shard.timestamps.insert(i as u64);
                shard.u64.insert(i as u64 + 1000);
                shard.negative64.insert((-(i as i64) - 1) as u64);
                shard
                    .strings
                    .insert(xxh64(format!("string-{i}").as_bytes(), 0));
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

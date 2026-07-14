//! Port of EsLogs `lib/logstorage/hits_map.go`.
//!
//! `HitsMap` counts hits per unique value, bucketing values into three maps
//! (non-negative integers, negative integers, and raw strings) so that numeric
//! values are counted without string materialization. `HitsMapAdaptive` starts
//! with a single [`HitsMap`] and, once the number of unique values crosses
//! [`HITS_MAP_ADAPTIVE_MAX_LEN`], spreads them across per-CPU shards to keep the
//! parallel merge ([`hits_map_merge_parallel`]) scalable.
//!
//! # Reconciliation note (canonical vs. pipe-local copies)
//! `pipe_uniq.rs`, `pipe_top.rs` and `pipe_facets.rs` each carry their own
//! simplified `HitsMap` (single-map, no adaptive sharding / parallel merge)
//! ported earlier alongside those pipes. This module is the faithful, canonical
//! port of `hits_map.go`; a later cleanup should collapse the pipe-local copies
//! onto it. It is left standalone here to avoid disturbing the already-tested
//! Layer-5 pipe files.
//!
//! # PORT NOTES (whole-module design decisions)
//! * **Value maps instead of pooled `*uint64`.** Go stores `map[...]*uint64`
//!   whose pointers are allocated from a `chunkedAllocator` and shuffled between
//!   maps during shard moves / merges (the pointers outlive their source map,
//!   kept alive by the GC). A `chunkedAllocator` handle in the Rust port is
//!   bound to its owning allocator, so moving a handle into another allocator's
//!   map is unsound. The port therefore stores hit counts by value
//!   (`HashMap<_, u64>`); counting behavior is identical, only the allocation
//!   strategy differs.
//! * **String keys as bytes.** Go keys the string map with `string` values that
//!   may hold non-UTF-8 bytes; the port keys with `Vec<u8>`.
//! * **Shared state-size budget.** Go threads a shared `*int` budget across all
//!   `hitsMapAdaptive`s of a pipe; the port mirrors it with a `*mut i64` raw
//!   pointer under the same single-threaded-update contract.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use esl_common::bytesutil::to_unsafe_string;

use crate::stats_count_uniq::{fast_hash_uint64, need_stop};
use crate::values_encoder::{
    marshal_int64_string, marshal_uint64_string, try_parse_int64, try_parse_uint64,
};

/// The maximum number of values to track in `HitsMapAdaptive::hm` before
/// switching to `HitsMapAdaptive::hm_shards`.
///
/// Too big a value may slow down [`hits_map_merge_parallel`] across a big number
/// of CPU cores. Too small a value may significantly increase RAM usage when
/// hits for a big number of unique values are counted.
pub const HITS_MAP_ADAPTIVE_MAX_LEN: u64 = 4 << 10;

/// Adaptive hits counter that spreads unique values across per-CPU shards once
/// the single-map form grows past [`HITS_MAP_ADAPTIVE_MAX_LEN`].
pub struct HitsMapAdaptive {
    /// Shared state-size budget (Go `stateSizeBudget *int`).
    ///
    /// PORT NOTE: raw pointer mirroring Go's shared `*int`; only touched during
    /// single-threaded `update_state*` calls.
    state_size_budget: *mut i64,

    /// The number of parallel workers to use when merging `hm_shards`.
    ///
    /// Must be set by the caller via [`HitsMapAdaptive::init`] before use.
    concurrency: usize,

    /// Tracks hits until the number of unique values reaches
    /// [`HITS_MAP_ADAPTIVE_MAX_LEN`]; after that hits go to `hm_shards`.
    hm: HitsMap,

    /// Tracks hits for a big number of unique values; each shard holds a share.
    hm_shards: Option<Vec<HitsMapShard>>,
}

impl Default for HitsMapAdaptive {
    fn default() -> Self {
        HitsMapAdaptive {
            state_size_budget: std::ptr::null_mut(),
            concurrency: 0,
            hm: HitsMap::default(),
            hm_shards: None,
        }
    }
}

impl HitsMapAdaptive {
    fn reset(&mut self) {
        *self = HitsMapAdaptive::default();
    }

    /// Initializes the adaptive map for use.
    ///
    /// # Safety
    /// `state_size_budget` must remain valid for the lifetime of the map's use,
    /// mirroring Go's shared `*int` budget.
    pub fn init(&mut self, concurrency: usize, filter: &str, state_size_budget: *mut i64) {
        self.reset();
        self.state_size_budget = state_size_budget;
        self.concurrency = concurrency;
        self.hm.filter = filter.to_string();
    }

    /// Returns the shared state-size budget back and re-inits the map.
    pub fn clear(&mut self) {
        // SAFETY: state_size_budget is valid per init()'s contract.
        unsafe { *self.state_size_budget += self.state_size() };
        let concurrency = self.concurrency;
        let filter = std::mem::take(&mut self.hm.filter);
        let budget = self.state_size_budget;
        self.init(concurrency, &filter, budget);
    }

    /// Returns the total tracked state size in bytes.
    pub fn state_size(&self) -> i64 {
        let mut n = self.hm.state_size();
        if let Some(shards) = &self.hm_shards {
            for s in shards {
                n += s.hm.state_size();
            }
        }
        n
    }

    /// Returns the number of unique tracked values.
    pub fn entries_count(&self) -> u64 {
        match &self.hm_shards {
            None => self.hm.entries_count(),
            Some(shards) => shards.iter().map(|s| s.hm.entries_count()).sum(),
        }
    }

    /// Updates the state for the given string key, auto-detecting integer keys.
    pub fn update_state_generic(&mut self, key: &str, hits: u64) {
        if let Some(n) = try_parse_uint64(key) {
            self.update_state_uint64(n, hits);
            return;
        }
        if key.starts_with('-')
            && let Some(n) = try_parse_int64(key)
        {
            self.update_state_negative_int64(n, hits);
            return;
        }
        self.update_state_string(key.as_bytes(), hits);
    }

    /// Updates the state for the given signed integer key.
    pub fn update_state_int64(&mut self, n: i64, hits: u64) {
        if n >= 0 {
            self.update_state_uint64(n as u64, hits);
        } else {
            self.update_state_negative_int64(n, hits);
        }
    }

    /// Updates the state for the given non-negative integer key.
    pub fn update_state_uint64(&mut self, n: u64, hits: u64) {
        if self.hm_shards.is_none() {
            let state_size = self.hm.update_state_uint64(n, hits);
            if state_size > 0 {
                // SAFETY: state_size_budget is valid per init()'s contract.
                unsafe { *self.state_size_budget -= state_size };
                self.probably_move_to_shards();
            }
            return;
        }
        let idx = self.shard_idx_by_uint64(n);
        let state_size = self.hm_shards.as_mut().unwrap()[idx]
            .hm
            .update_state_uint64(n, hits);
        // SAFETY: state_size_budget is valid per init()'s contract.
        unsafe { *self.state_size_budget -= state_size };
    }

    /// Updates the state for the given negative integer key.
    pub fn update_state_negative_int64(&mut self, n: i64, hits: u64) {
        if self.hm_shards.is_none() {
            let state_size = self.hm.update_state_negative_int64(n, hits);
            if state_size > 0 {
                // SAFETY: state_size_budget is valid per init()'s contract.
                unsafe { *self.state_size_budget -= state_size };
                self.probably_move_to_shards();
            }
            return;
        }
        let idx = self.shard_idx_by_uint64(n as u64);
        let state_size = self.hm_shards.as_mut().unwrap()[idx]
            .hm
            .update_state_negative_int64(n, hits);
        // SAFETY: state_size_budget is valid per init()'s contract.
        unsafe { *self.state_size_budget -= state_size };
    }

    /// Updates the state for the given raw string key.
    pub fn update_state_string(&mut self, key: &[u8], hits: u64) {
        if self.hm_shards.is_none() {
            let state_size = self.hm.update_state_string(key, hits);
            if state_size > 0 {
                // SAFETY: state_size_budget is valid per init()'s contract.
                unsafe { *self.state_size_budget -= state_size };
                self.probably_move_to_shards();
            }
            return;
        }
        let idx = self.shard_idx_by_string(key);
        let state_size = self.hm_shards.as_mut().unwrap()[idx]
            .hm
            .update_state_string(key, hits);
        // SAFETY: state_size_budget is valid per init()'s contract.
        unsafe { *self.state_size_budget -= state_size };
    }

    fn probably_move_to_shards(&mut self) {
        if self.hm.entries_count() < HITS_MAP_ADAPTIVE_MAX_LEN {
            return;
        }
        self.move_to_shards();
    }

    fn move_to_shards(&mut self) {
        let concurrency = self.concurrency.max(1);
        let mut shards: Vec<HitsMapShard> =
            (0..concurrency).map(|_| HitsMapShard::default()).collect();
        for s in &mut shards {
            s.hm.filter = self.hm.filter.clone();
        }
        let n_shards = shards.len() as u64;

        // Redistribute existing entries into the shards.
        let hm = std::mem::take(&mut self.hm);
        for (n, hits) in hm.u64 {
            let idx = (fast_hash_uint64(n) % n_shards) as usize;
            shards[idx].hm.set_state_uint64(n, hits);
        }
        for (n, hits) in hm.negative64 {
            let idx = (fast_hash_uint64(n) % n_shards) as usize;
            shards[idx].hm.set_state_negative_int64(n as i64, hits);
        }
        for (s, hits) in hm.strings {
            let idx = (xxhash_rust::xxh64::xxh64(&s, 0) % n_shards) as usize;
            shards[idx].hm.set_state_string(s, hits);
        }
        self.hm_shards = Some(shards);
    }

    fn shard_idx_by_uint64(&self, n: u64) -> usize {
        let len = self.hm_shards.as_ref().unwrap().len() as u64;
        (fast_hash_uint64(n) % len) as usize
    }

    fn shard_idx_by_string(&self, v: &[u8]) -> usize {
        let len = self.hm_shards.as_ref().unwrap().len() as u64;
        (xxhash_rust::xxh64::xxh64(v, 0) % len) as usize
    }
}

// SAFETY: HitsMapAdaptive is used by a single worker at a time during updates;
// the raw budget pointer follows Go's single-threaded-update contract.
unsafe impl Send for HitsMapAdaptive {}

/// A per-CPU shard wrapping a [`HitsMap`].
///
/// PORT NOTE: Go pads the shard to a cache line to prevent false sharing. The
/// port aligns the shard to 128 bytes for the same reason.
#[repr(align(128))]
#[derive(Default)]
pub struct HitsMapShard {
    hm: HitsMap,
}

/// Counts hits per unique value, split into non-negative-integer,
/// negative-integer and raw-string maps.
#[derive(Default)]
pub struct HitsMap {
    /// If non-empty, only values containing this substring are counted.
    filter: String,

    /// Scratch buffer used by the `need_skip_key_*` helpers.
    tmp_buf: Vec<u8>,

    u64: HashMap<u64, u64>,
    negative64: HashMap<u64, u64>,
    strings: HashMap<Vec<u8>, u64>,
}

impl HitsMap {
    fn reset(&mut self) {
        self.filter = String::new();
        self.tmp_buf.clear();
        self.u64 = HashMap::new();
        self.negative64 = HashMap::new();
        self.strings = HashMap::new();
    }

    /// Returns the number of unique tracked values.
    pub fn entries_count(&self) -> u64 {
        (self.u64.len() + self.negative64.len() + self.strings.len()) as u64
    }

    /// Returns the tracked state size in bytes (Go `hitsMap.stateSize`).
    ///
    /// PORT NOTE: Go sums `unsafe.Sizeof` of the key, the `*uint64` pointer and
    /// the pointed-to `uint64`. The port uses equivalent value-map sizes
    /// (`8 + 8 + 8` per numeric entry; `len(k) + 8 + 8 + 8` per string entry).
    pub fn state_size(&self) -> i64 {
        let mut size: i64 = 0;
        size += self.u64.len() as i64 * 24;
        size += self.negative64.len() as i64 * 24;
        for k in self.strings.keys() {
            size += k.len() as i64 + 24;
        }
        size
    }

    fn need_skip_key_int64(&mut self, n: i64) -> bool {
        if self.filter.is_empty() {
            return false;
        }
        self.tmp_buf.clear();
        marshal_int64_string(&mut self.tmp_buf, n);
        let key = to_unsafe_string(&self.tmp_buf);
        !key.contains(&self.filter)
    }

    fn need_skip_key_uint64(&mut self, n: u64) -> bool {
        if self.filter.is_empty() {
            return false;
        }
        self.tmp_buf.clear();
        marshal_uint64_string(&mut self.tmp_buf, n);
        let key = to_unsafe_string(&self.tmp_buf);
        !key.contains(&self.filter)
    }

    fn need_skip_key_string(&self, key: &[u8]) -> bool {
        if self.filter.is_empty() {
            return false;
        }
        // Byte-wise substring check (Go strings.Contains is a byte search);
        // works for arbitrary value bytes.
        let f = self.filter.as_bytes();
        !key.windows(f.len()).any(|w| w == f)
    }

    /// Adds `hits` for the given non-negative integer, returning the added
    /// state size in bytes (0 when the value already existed or was filtered).
    pub fn update_state_uint64(&mut self, n: u64, hits: u64) -> i64 {
        if self.need_skip_key_uint64(n) {
            return 0;
        }
        if let Some(p) = self.u64.get_mut(&n) {
            *p += hits;
            return 0;
        }
        8 + self.set_state_uint64(n, hits)
    }

    fn set_state_uint64(&mut self, n: u64, hits: u64) -> i64 {
        let first = self.u64.is_empty();
        self.u64.insert(n, hits);
        if first { 24 } else { 16 }
    }

    /// Adds `hits` for the given negative integer, returning the added state
    /// size in bytes (0 when the value already existed or was filtered).
    pub fn update_state_negative_int64(&mut self, n: i64, hits: u64) -> i64 {
        if self.need_skip_key_int64(n) {
            return 0;
        }
        if let Some(p) = self.negative64.get_mut(&(n as u64)) {
            *p += hits;
            return 0;
        }
        8 + self.set_state_negative_int64(n, hits)
    }

    fn set_state_negative_int64(&mut self, n: i64, hits: u64) -> i64 {
        let first = self.negative64.is_empty();
        self.negative64.insert(n as u64, hits);
        if first { 24 } else { 16 }
    }

    /// Adds `hits` for the given raw string key, returning the added state size
    /// in bytes (0 when the value already existed or was filtered).
    pub fn update_state_string(&mut self, key: &[u8], hits: u64) -> i64 {
        if self.need_skip_key_string(key) {
            return 0;
        }
        if let Some(p) = self.strings.get_mut(key) {
            *p += hits;
            return 0;
        }
        let key_len = key.len() as i64;
        key_len + 8 + self.set_state_string(key.to_vec(), hits)
    }

    fn set_state_string(&mut self, v: Vec<u8>, hits: u64) -> i64 {
        let first = self.strings.is_empty();
        self.strings.insert(v, hits);
        if first { 24 } else { 16 }
    }

    /// Merges `src` into `self`, stopping early if `stop` is set.
    pub fn merge_state(&mut self, src: &HitsMap, stop: Option<&AtomicBool>) {
        for (&n, &hits_src) in &src.u64 {
            if need_stop(stop) {
                return;
            }
            match self.u64.get_mut(&n) {
                Some(p) => *p += hits_src,
                None => {
                    self.set_state_uint64(n, hits_src);
                }
            }
        }
        for (&n, &hits_src) in &src.negative64 {
            if need_stop(stop) {
                return;
            }
            match self.negative64.get_mut(&n) {
                Some(p) => *p += hits_src,
                None => {
                    self.set_state_negative_int64(n as i64, hits_src);
                }
            }
        }
        for (k, &hits_src) in &src.strings {
            if need_stop(stop) {
                return;
            }
            match self.strings.get_mut(k) {
                Some(p) => *p += hits_src,
                None => {
                    self.set_state_string(k.clone(), hits_src);
                }
            }
        }
    }

    /// Iterates over all tracked entries, calling `f` with each value's string
    /// form and its hits. Used by callers that consume merged results.
    pub fn for_each(&self, mut f: impl FnMut(&[u8], u64)) {
        let mut buf: Vec<u8> = Vec::new();
        for (&n, &hits) in &self.u64 {
            buf.clear();
            marshal_uint64_string(&mut buf, n);
            f(&buf, hits);
        }
        for (&n, &hits) in &self.negative64 {
            buf.clear();
            marshal_int64_string(&mut buf, n as i64);
            f(&buf, hits);
        }
        for (k, &hits) in &self.strings {
            f(k, hits);
        }
    }
}

/// Send/Sync wrapper for a per-shard `*mut HitsMap`, used to split disjoint
/// shard access across worker threads (see [`hits_map_merge_parallel`]).
struct ShardPtr(*mut HitsMap);
// SAFETY: each ShardPtr is handed to exactly one worker for a disjoint shard;
// no two workers ever dereference the same pointer (Go's contract).
unsafe impl Send for ShardPtr {}
unsafe impl Sync for ShardPtr {}

/// Merges `hmas` in parallel, passing the merged disjoint shards to `f`.
///
/// The merge may be interrupted by setting `stop`. The caller must check `stop`
/// after this returns.
///
/// PORT NOTE: Go spins goroutines via `sync.WaitGroup`; the port uses
/// `std::thread::scope`. `f` is called concurrently on disjoint shards, so it
/// must be `Sync`.
pub fn hits_map_merge_parallel(
    hmas: &mut [HitsMapAdaptive],
    stop: Option<&AtomicBool>,
    f: impl Fn(&mut HitsMap) + Sync,
) {
    if hmas.is_empty() {
        return;
    }

    // Move the still-unsharded adaptives to shards in parallel.
    std::thread::scope(|scope| {
        for hma in hmas.iter_mut() {
            if hma.hm_shards.is_some() {
                continue;
            }
            scope.spawn(move || {
                hma.move_to_shards();
            });
        }
    });
    if need_stop(stop) {
        return;
    }

    let cpus_count = hmas[0].hm_shards.as_ref().map(|s| s.len()).unwrap_or(0);
    let hmas_len = hmas.len();

    // Collect disjoint per-shard pointers: shard_ptrs[hma_idx][cpu_idx].
    let mut shard_ptrs: Vec<Vec<ShardPtr>> = Vec::with_capacity(hmas_len);
    for hma in hmas.iter_mut() {
        let shards = hma.hm_shards.as_mut().unwrap();
        shard_ptrs.push(shards.iter_mut().map(|s| ShardPtr(&mut s.hm)).collect());
    }

    let shard_ptrs = &shard_ptrs;
    let f_ref = &f;
    std::thread::scope(|scope| {
        for cpu_idx in 0..cpus_count {
            scope.spawn(move || {
                // SAFETY: `dst`/`src` are disjoint shard `cpu_idx` pointers —
                // distinct per task (different cpu_idx) and per hma index, so no
                // two workers touch the same HitsMap (Go's disjoint-shard merge).
                let dst_ptr: *mut HitsMap = shard_ptrs[0][cpu_idx].0;
                let dst = unsafe { &mut *dst_ptr };
                for row in shard_ptrs.iter().take(hmas_len).skip(1) {
                    let src_ptr: *mut HitsMap = row[cpu_idx].0;
                    let src = unsafe { &mut *src_ptr };
                    dst.merge_state(src, stop);
                    src.reset();
                }
                f_ref(dst);
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget() -> Box<i64> {
        Box::new(1 << 40)
    }

    #[test]
    fn test_hits_map_counts_by_type() {
        let mut hm = HitsMap::default();
        hm.update_state_uint64(5, 2);
        hm.update_state_uint64(5, 3);
        hm.update_state_negative_int64(-7, 4);
        hm.update_state_string(b"abc", 1);
        hm.update_state_string(b"abc", 1);

        assert_eq!(hm.entries_count(), 3);
        assert_eq!(*hm.u64.get(&5).unwrap(), 5);
        assert_eq!(*hm.negative64.get(&((-7i64) as u64)).unwrap(), 4);
        assert_eq!(*hm.strings.get(b"abc".as_slice()).unwrap(), 2);
    }

    #[test]
    fn test_hits_map_filter_skips_non_matching() {
        let mut hm = HitsMap {
            filter: "5".to_string(),
            ..Default::default()
        };
        // "5" contains "5" -> counted; 6 -> "6" doesn't contain "5" -> skipped.
        hm.update_state_uint64(5, 1);
        hm.update_state_uint64(6, 1);
        hm.update_state_string(b"x5y", 1);
        hm.update_state_string(b"xyz", 1);
        assert_eq!(hm.entries_count(), 2);
        assert!(hm.u64.contains_key(&5));
        assert!(!hm.u64.contains_key(&6));
    }

    #[test]
    fn test_hits_map_adaptive_generic_and_merge() {
        let mut b = budget();
        let ptr: *mut i64 = &mut *b;
        let mut hma = HitsMapAdaptive::default();
        hma.init(2, "", ptr);
        hma.update_state_generic("10", 1);
        hma.update_state_generic("10", 1);
        hma.update_state_generic("-3", 5);
        hma.update_state_generic("hello", 2);
        assert_eq!(hma.entries_count(), 3);

        // Merge two adaptives sharing the key "10".
        let mut b2 = budget();
        let ptr2: *mut i64 = &mut *b2;
        let mut hma2 = HitsMapAdaptive::default();
        hma2.init(2, "", ptr2);
        hma2.update_state_generic("10", 3);

        let mut hmas = vec![hma, hma2];
        let total: std::sync::Mutex<Vec<(Vec<u8>, u64)>> = std::sync::Mutex::new(Vec::new());
        let total_ref = &total;
        hits_map_merge_parallel(&mut hmas, None, move |hm| {
            hm.for_each(|k, hits| {
                total_ref.lock().unwrap().push((k.to_vec(), hits));
            });
        });
        let out = total.lock().unwrap();
        let ten = out.iter().find(|(k, _)| k == b"10").map(|(_, h)| *h);
        assert_eq!(ten, Some(5));
    }
}

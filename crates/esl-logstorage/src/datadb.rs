//! Port of EsLogs `lib/logstorage/datadb.go`.
//!
//! Concurrency mapping (see docs/LOGSTORAGE_PLAN.md cross-cutting notes):
//!
//! - Go goroutine merge workers → named `std::thread`s; the worker counts and
//!   the merge-idle semantics (workers exit when there is nothing to merge and
//!   are restarted under `partsLock` whenever a new part is registered) are
//!   preserved.
//! - `sync.WaitGroup` → the local [`WaitGroup`] (Mutex + Condvar), shared
//!   between the datadb workers and the rowsBuffer flush timers exactly like
//!   Go's `ddb.wg`.
//! - `stopCh` (closed under `partsLock`) → `PartsState::stopped` (checked
//!   under the parts lock before spawning workers, mirroring the Go comment
//!   about `wg.Add()` ordering) plus the `Datadb::stop` AtomicBool for the
//!   lock-free `needStop()` checks in worker loops.
//! - The `inmemoryPartsConcurrencyCh`/`smallPartsConcurrencyCh`/
//!   `bigPartsConcurrencyCh` channels used as counting semaphores →
//!   [`ConcurrencyCh`] (Mutex + Condvar) statics with the same
//!   `cgroup.AvailableCPUs()` capacity; acquiring in the spawning loop keeps
//!   Go's backpressure behavior.
//! - `wgPool` (pooled `sync.WaitGroup`s for merge fan-out) →
//!   `std::thread::scope`.
//!
//! PORT NOTE: `datadb.deleteRows` is NOT ported yet: it depends on
//! `partitionSearchOptions`, `ddb.getPartsForTimeRange` and
//! `part.hasMatchingRows` from the unported `storage_search.go`.
//! Consequently `mustMergePartsInternal` is ported without the `dropFilter`
//! parameter, and `must_merge_block_streams` is called without the Go `idb` /
//! `dropFilter` arguments (deferred by the block_stream_merger.rs port; `idb`
//! is only used by the dropFilter path). The search-layer porter must extend
//! both.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use esl_common::{cgroup, fs, infof, memory, panicf, warnf};

use crate::block_stream_merger::must_merge_block_streams;
use crate::block_stream_reader::{
    BlockStreamReader, get_block_stream_reader, put_block_stream_reader,
};
use crate::block_stream_writer::{get_block_stream_writer, put_block_stream_writer};
use crate::filenames::PARTS_FILENAME;
use crate::inmemory_part::{InmemoryPart, get_inmemory_part, put_inmemory_part};
use crate::log_rows::{LogRows, LogRowsInternal, get_log_rows_internal, put_log_rows_internal};
use crate::part::{Part, must_close_part, must_open_file_part, must_open_inmemory_part};
use crate::part_header::PartHeader;
use crate::partition::Partition;

/// The maximum size of big part.
///
/// This number limits the maximum time required for building big part.
/// This time shouldn't exceed a few days.
const MAX_BIG_PART_SIZE: u64 = 1_000_000_000_000;

/// The maximum number of inmemory parts in the partition.
///
/// The actual number of inmemory parts may exceed this value if in-memory mergers
/// cannot keep up with the rate of creating new in-memory parts.
const MAX_INMEMORY_PARTS_PER_PARTITION: u64 = 20;

/// Default number of parts to merge at once.
///
/// This number has been obtained empirically - it gives the lowest possible overhead.
/// See appendPartsToMerge tests for details.
const DEFAULT_PARTS_TO_MERGE: usize = 15;

/// minMergeMultiplier is the minimum multiplier for the size of the output part
/// compared to the size of the maximum input part for the merge.
///
/// Higher value reduces write amplification (disk write IO induced by the merge),
/// while increases the number of unmerged parts.
/// The 1.7 is good enough for production workloads.
const MIN_MERGE_MULTIPLIER: f64 = 1.7;

/// datadb represents a database with log data
pub struct Datadb {
    /// rb is an in-memory buffer for the added rows. It is periodically converted to parts.
    ///
    /// This buffer amortizes the overhead needed for converting the ingested logs into searchable parts.
    rb: RowsBuffer,

    /// mergeIdx is used for generating unique directory names for parts
    merge_idx: AtomicU64,

    inmemory_merges_total: AtomicU64,
    inmemory_active_merges: AtomicI64,
    inmemory_merge_rows_total: AtomicU64,

    small_part_merges_total: AtomicU64,
    small_part_active_merges: AtomicI64,
    small_part_merge_rows_total: AtomicU64,

    big_part_merges_total: AtomicU64,
    big_part_active_merges: AtomicI64,
    big_part_merge_rows_total: AtomicU64,

    /// pt is the partition the datadb belongs to
    ///
    /// PORT NOTE: Go stores a raw `*partition` and nils it in
    /// mustCloseDatadb(); the port stores a `Weak` so the partition → datadb
    /// reference cycle cannot leak.
    pt: Weak<Partition>,

    /// path is the path to the directory with log data
    path: PathBuf,

    /// flushInterval is interval for flushing the inmemory parts to disk
    flush_interval: Duration,

    /// parts contains the lists of inmemory parts, file-based small parts and
    /// file-based big parts, protected by the parts lock (Go: `partsLock`
    /// guarding `inmemoryParts`/`smallParts`/`bigParts`), plus the `stopped`
    /// flag standing in for the closed `stopCh`.
    parts: Mutex<PartsState>,

    /// stop mirrors `stopCh` for the lock-free `needStop()` checks.
    /// It is set under the parts lock together with `PartsState::stopped`.
    stop: AtomicBool,

    /// flusher_mu/flusher_cv wake the inmemoryPartsFlusher ticker early on close.
    flusher_mu: Mutex<()>,
    flusher_cv: Condvar,

    /// wg is used for determining when background workers stop
    ///
    /// wg.add() must be called under the parts lock after checking whether
    /// `stopped` isn't set. This should prevent from calling wg.add() after
    /// stop is set and wg.wait() is called.
    wg: Arc<WaitGroup>,
}

struct PartsState {
    /// inmemoryParts contains a list of inmemory parts
    inmemory_parts: Vec<Arc<PartWrapper>>,

    /// smallParts contains a list of file-based small parts
    small_parts: Vec<Arc<PartWrapper>>,

    /// bigParts contains a list of file-based big parts
    big_parts: Vec<Arc<PartWrapper>>,

    /// stopped is set when the datadb is being closed (Go: `stopCh` is closed
    /// under `partsLock`).
    stopped: bool,
}

/// partWrapper is a wrapper for opened part.
pub(crate) struct PartWrapper {
    /// refCount is the number of references to p.
    ///
    /// When the number of references reaches zero, then p is closed.
    ref_count: AtomicI32,

    /// The flag, which is set when the part must be deleted after refCount reaches zero.
    must_drop: AtomicBool,

    /// isInMerge is set to true if the part takes part in merge.
    ///
    /// PORT NOTE: a plain bool guarded by `partsLock` in Go; an AtomicBool
    /// here since PartWrapper is shared via Arc. It is still only mutated
    /// under the datadb parts lock.
    is_in_merge: AtomicBool,

    /// The deadline when in-memory part must be flushed to disk.
    ///
    /// PORT NOTE: `None` stands for Go's zero time.Time (file-based parts).
    flush_deadline: Option<Instant>,

    /// PORT NOTE: cached copy of `p.ph`. Go reads `pw.p.ph` directly all over
    /// datadb.go; the part header never changes after the part is opened, so
    /// caching it here lets the merge-selection code run without locking
    /// `inner`.
    ph: PartHeader,

    /// PORT NOTE: cached copy of `p.path` (empty for in-memory parts), for
    /// the same reason as `ph`.
    path: PathBuf,

    /// PORT NOTE: caches Go's `pw.mp != nil`.
    is_inmemory: bool,

    /// inner holds the opened part and the backing in-memory part; it is
    /// taken (and the resources are released) when refCount reaches zero.
    inner: Mutex<Option<PartWrapperInner>>,
}

struct PartWrapperInner {
    /// p is an opened part
    ///
    /// PORT NOTE: for in-memory parts `p` borrows the leaked `InmemoryPart`
    /// pointed to by `mp` (Go: `p` references `mp` buffers); the wrapper is
    /// the self-referential pair Go builds with garbage collection.
    p: Part<'static>,

    /// mp references inmemory part used for initializing p.
    ///
    /// It points to a `Box::leak`ed `InmemoryPart` reclaimed exactly once in
    /// `PartWrapper::dec_ref()` after `p` is closed.
    mp: Option<NonNull<InmemoryPart>>,
}

// SAFETY: `Part` only holds Send + Sync resources (file readers, plain data,
// `&'static chunkedbuffer::Buffer`), and `mp` is a uniquely-owned leaked Box
// only ever exposed as `&InmemoryPart`; the wrapper is shared across merge
// worker threads exactly like Go shares `*partWrapper` across goroutines.
unsafe impl Send for PartWrapperInner {}

impl PartWrapper {
    pub(crate) fn inc_ref(&self) {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn dec_ref(&self) {
        let n = self.ref_count.fetch_sub(1, Ordering::SeqCst) - 1;
        if n > 0 {
            return;
        }

        let inner = self.inner.lock().unwrap().take();
        let Some(mut inner) = inner else {
            // Test-only wrappers without an opened part.
            return;
        };

        let mut delete_path = None;
        if inner.mp.is_none() && self.must_drop.load(Ordering::SeqCst) {
            delete_path = Some(self.path.clone());
        }

        // PORT NOTE: Go returns pw.mp to the pool before mustClosePart(pw.p);
        // the port closes p first, since p borrows the in-memory part buffers
        // and put_inmemory_part resets them for reuse.
        must_close_part(&mut inner.p);
        if let Some(mp_ptr) = inner.mp.take() {
            // SAFETY: mp_ptr was created via Box::leak (see
            // open_created_part / must_flush_log_rows) and is reclaimed
            // exactly once, here, when the last reference to the part is
            // dropped; p (the only borrower) has been closed above.
            let mp = unsafe { Box::from_raw(mp_ptr.as_ptr()) };
            put_inmemory_part(*mp);
        }
        drop(inner);

        if let Some(delete_path) = delete_path {
            fs::must_remove_dir(&delete_path);
        }
    }

    /// Returns a stable pointer to the opened part backing pw (Go accesses
    /// `pw.p` directly).
    ///
    /// The pointer is valid for as long as the caller keeps pw referenced
    /// (refCount > 0): `inner` is only taken (freeing the part) in `dec_ref`
    /// once refCount reaches zero, and the `PartWrapper` is pinned behind the
    /// caller's `Arc`, so the inline `PartWrapperInner` — and the `Part` it
    /// stores — does not move. This mirrors Go's direct `pw.p` access under the
    /// same refCount discipline; the pointer lets the search borrow `&Part`
    /// across worker threads without holding `inner`'s lock for the whole search
    /// (which could otherwise contend with a concurrent merge calling `mp()`).
    pub(crate) fn part_ptr(&self) -> *const Part<'static> {
        let inner = self.inner.lock().unwrap();
        let p = &inner
            .as_ref()
            .expect("BUG: part must be open while it is referenced by a search")
            .p;
        p as *const Part<'static>
    }

    /// Returns the in-memory part backing pw (Go: `pw.mp`).
    ///
    /// The returned reference is valid for as long as the caller keeps pw
    /// referenced (refCount > 0), matching the Go access discipline.
    fn mp(&self) -> Option<&InmemoryPart> {
        let inner = self.inner.lock().unwrap();
        let mp_ptr = inner.as_ref().and_then(|inner| inner.mp)?;
        // SAFETY: the pointee is a leaked Box freed only in dec_ref() when
        // refCount reaches zero; the caller holds a reference, so the
        // in-memory part is alive.
        Some(unsafe { &*mp_ptr.as_ptr() })
    }
}

pub(crate) fn must_create_datadb(path: &Path) {
    fs::must_mkdir_fail_if_exist(path);
    must_write_part_names(path, None);
    fs::must_sync_path_and_parent_dir(path);
}

/// mustOpenDatadb opens datadb at the given path with the given flushInterval for in-memory data.
pub(crate) fn must_open_datadb(
    pt: Weak<Partition>,
    path: &Path,
    flush_interval: Duration,
) -> Arc<Datadb> {
    let part_names = must_read_part_names(path);
    must_remove_unused_dirs(path, &part_names);

    let mut small_parts: Vec<Arc<PartWrapper>> = Vec::new();
    let mut big_parts: Vec<Arc<PartWrapper>> = Vec::new();
    for part_name in &part_names {
        // Make sure the partName exists on disk.
        // If it is missing, then manual action from the user is needed,
        // since this is unexpected state, which cannot occur under normal operation,
        // including unclean shutdown.
        let part_path = path.join(part_name);
        if !fs::is_path_exist(&part_path) {
            let parts_file = path.join(PARTS_FILENAME);
            panicf!(
                "FATAL: part {:?} is listed in {:?}, but is missing on disk; ensure {:?} contents is not corrupted; remove {:?} from {:?} in order to fix this error",
                part_path,
                parts_file,
                parts_file,
                part_path,
                parts_file
            );
        }

        let p = must_open_file_part(pt.clone(), &part_path);
        let compressed_size_bytes = p.ph.compressed_size_bytes;
        let pw = new_part_wrapper(p, None, None);
        if compressed_size_bytes > get_max_inmemory_part_size() {
            big_parts.push(pw);
        } else {
            small_parts.push(pw);
        }
    }

    let wg = Arc::new(WaitGroup::new());
    let ddb = Arc::new_cyclic(|weak: &Weak<Datadb>| {
        let mut rb = RowsBuffer::default();
        let w = weak.clone();
        rb.init(
            Arc::clone(&wg),
            Arc::new(move |lr: &mut LogRowsInternal| {
                // The datadb outlives all rowsBuffer flushes: mustCloseDatadb
                // flushes rb and waits for the flush timers via wg while the
                // caller still holds the Arc.
                if let Some(ddb) = w.upgrade() {
                    ddb.must_flush_log_rows(lr);
                }
            }),
        );
        Datadb {
            rb,
            merge_idx: AtomicU64::new(0),
            inmemory_merges_total: AtomicU64::new(0),
            inmemory_active_merges: AtomicI64::new(0),
            inmemory_merge_rows_total: AtomicU64::new(0),
            small_part_merges_total: AtomicU64::new(0),
            small_part_active_merges: AtomicI64::new(0),
            small_part_merge_rows_total: AtomicU64::new(0),
            big_part_merges_total: AtomicU64::new(0),
            big_part_active_merges: AtomicI64::new(0),
            big_part_merge_rows_total: AtomicU64::new(0),
            pt,
            path: path.to_path_buf(),
            flush_interval,
            parts: Mutex::new(PartsState {
                inmemory_parts: Vec::new(),
                small_parts,
                big_parts,
                stopped: false,
            }),
            stop: AtomicBool::new(false),
            flusher_mu: Mutex::new(()),
            flusher_cv: Condvar::new(),
            wg,
        }
    });
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    ddb.merge_idx.store(now_nanos, Ordering::SeqCst);

    ddb.start_background_workers();

    ddb
}

static INMEMORY_PARTS_CONCURRENCY_CH: LazyLock<ConcurrencyCh> =
    LazyLock::new(|| ConcurrencyCh::new(cgroup::available_cpus()));
static SMALL_PARTS_CONCURRENCY_CH: LazyLock<ConcurrencyCh> =
    LazyLock::new(|| ConcurrencyCh::new(cgroup::available_cpus()));
static BIG_PARTS_CONCURRENCY_CH: LazyLock<ConcurrencyCh> =
    LazyLock::new(|| ConcurrencyCh::new(cgroup::available_cpus()));

impl Datadb {
    fn start_background_workers(self: &Arc<Self>) {
        // Start file parts mergers, so they could start merging unmerged parts if needed.
        // There is no need in starting in-memory parts mergers, since there are no in-memory parts yet.
        self.start_small_parts_mergers();
        self.start_big_parts_mergers();

        self.start_inmemory_parts_flusher();
    }

    fn start_small_parts_mergers(self: &Arc<Self>) {
        let ps = self.parts.lock().unwrap();
        for _ in 0..SMALL_PARTS_CONCURRENCY_CH.cap {
            self.start_small_parts_merger_locked(&ps);
        }
    }

    fn start_big_parts_mergers(self: &Arc<Self>) {
        let ps = self.parts.lock().unwrap();
        for _ in 0..BIG_PARTS_CONCURRENCY_CH.cap {
            self.start_big_parts_merger_locked(&ps);
        }
    }

    fn start_inmemory_parts_merger_locked(self: &Arc<Self>, ps: &PartsState) {
        if ps.stopped {
            return;
        }
        self.wg.add(1);
        let ddb = Arc::clone(self);
        std::thread::Builder::new()
            .name("inmemoryPartsMerger".to_string())
            .spawn(move || {
                ddb.inmemory_parts_merger();
                ddb.wg.done();
            })
            .unwrap();
    }

    fn start_small_parts_merger_locked(self: &Arc<Self>, ps: &PartsState) {
        if ps.stopped {
            return;
        }
        self.wg.add(1);
        let ddb = Arc::clone(self);
        std::thread::Builder::new()
            .name("smallPartsMerger".to_string())
            .spawn(move || {
                ddb.small_parts_merger();
                ddb.wg.done();
            })
            .unwrap();
    }

    fn start_big_parts_merger_locked(self: &Arc<Self>, ps: &PartsState) {
        if ps.stopped {
            return;
        }
        self.wg.add(1);
        let ddb = Arc::clone(self);
        std::thread::Builder::new()
            .name("bigPartsMerger".to_string())
            .spawn(move || {
                ddb.big_parts_merger();
                ddb.wg.done();
            })
            .unwrap();
    }

    fn start_inmemory_parts_flusher(self: &Arc<Self>) {
        self.wg.add(1);
        let ddb = Arc::clone(self);
        std::thread::Builder::new()
            .name("inmemoryPartsFlusher".to_string())
            .spawn(move || {
                ddb.inmemory_parts_flusher();
                ddb.wg.done();
            })
            .unwrap();
    }

    fn inmemory_parts_flusher(self: &Arc<Self>) {
        // Do not add jitter to d in order to guarantee the flush interval
        loop {
            let deadline = Instant::now() + self.flush_interval;
            let mut g = self.flusher_mu.lock().unwrap();
            loop {
                if self.stop.load(Ordering::SeqCst) {
                    return;
                }
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let (g2, _) = self.flusher_cv.wait_timeout(g, deadline - now).unwrap();
                g = g2;
            }
            drop(g);
            self.must_flush_inmemory_parts_to_files(false);
        }
    }

    fn must_flush_inmemory_parts_to_files(self: &Arc<Self>, is_final: bool) {
        let current_time = Instant::now();
        let mut pws: Vec<Arc<PartWrapper>> = Vec::new();

        {
            let ps = self.parts.lock().unwrap();
            for pw in &ps.inmemory_parts {
                if !pw.is_in_merge.load(Ordering::SeqCst)
                    && (is_final || pw.flush_deadline.is_some_and(|d| d < current_time))
                {
                    pw.is_in_merge.store(true, Ordering::SeqCst);
                    pws.push(Arc::clone(pw));
                }
            }
        }

        self.must_merge_parts_to_files(pws);
    }

    fn must_merge_parts_to_files(self: &Arc<Self>, pws: Vec<Arc<PartWrapper>>) {
        // PORT NOTE: Go fans the merges out on a pooled sync.WaitGroup
        // (wgPool); the port uses std::thread::scope. The semaphore is
        // acquired in the spawning loop like in Go, preserving backpressure.
        std::thread::scope(|s| {
            let mut pws = pws;
            while !pws.is_empty() {
                let (pws_to_merge, pws_remaining) = get_parts_for_optimal_merge(pws);
                let permit = INMEMORY_PARTS_CONCURRENCY_CH.acquire();

                let ddb = Arc::clone(self);
                std::thread::Builder::new()
                    .name("mustMergePartsToFiles".to_string())
                    .spawn_scoped(s, move || {
                        ddb.must_merge_parts(pws_to_merge, true);
                        drop(permit);
                    })
                    .unwrap();

                pws = pws_remaining;
            }
        });
    }

    fn inmemory_parts_merger(self: &Arc<Self>) {
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return;
            }
            let max_out_bytes = self.get_max_big_part_size();

            let pws = {
                let ps = self.parts.lock().unwrap();
                get_parts_to_merge_locked(&ps.inmemory_parts, max_out_bytes)
            };

            if pws.is_empty() {
                // Nothing to merge
                return;
            }

            let _permit = INMEMORY_PARTS_CONCURRENCY_CH.acquire();
            self.must_merge_parts(pws, false);
        }
    }

    fn small_parts_merger(self: &Arc<Self>) {
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return;
            }
            let max_out_bytes = self.get_max_big_part_size();

            let pws = {
                let ps = self.parts.lock().unwrap();
                get_parts_to_merge_locked(&ps.small_parts, max_out_bytes)
            };

            if pws.is_empty() {
                // Nothing to merge
                return;
            }

            let _permit = SMALL_PARTS_CONCURRENCY_CH.acquire();
            self.must_merge_parts(pws, false);
        }
    }

    fn big_parts_merger(self: &Arc<Self>) {
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return;
            }
            let max_out_bytes = self.get_max_big_part_size();

            let pws = {
                let ps = self.parts.lock().unwrap();
                get_parts_to_merge_locked(&ps.big_parts, max_out_bytes)
            };

            if pws.is_empty() {
                // Nothing to merge
                return;
            }

            let _permit = BIG_PARTS_CONCURRENCY_CH.acquire();
            self.must_merge_parts(pws, false);
        }
    }

    /// mustMergeParts merges pws to a single resulting part.
    ///
    /// if isFinal is set, then the resulting part is guaranteed to be saved to disk.
    /// if isFinal is set, then the merge process cannot be interrupted.
    ///
    /// The pws may remain unmerged after returning from the function in the following cases:
    /// - if the datadb is stopped
    /// - if there is no enough disk space
    ///
    /// All the parts inside pws must have isInMerge field set to true.
    /// The isInMerge field inside pws parts is set to false before returning from the function.
    fn must_merge_parts(self: &Arc<Self>, pws: Vec<Arc<PartWrapper>>, is_final: bool) {
        let _ = self.must_merge_parts_internal(pws, is_final, Some(&self.stop));
    }

    /// mustMergePartsInternal merges pws to a single resulting part.
    ///
    /// if isFinal is set, then the resulting part is guaranteed to be saved to disk.
    /// if isFinal is set, then the merge process cannot be interrupted.
    ///
    /// The pws may remain unmerged after returning from the function in the following cases:
    /// - if stop_ch is set
    /// - if there is no enough disk space
    ///
    /// If pws aren't merged, then false is returned from the function.
    ///
    /// All the parts inside pws must have isInMerge field set to true.
    /// The isInMerge field inside pws parts is set to false before returning from the function.
    ///
    /// PORT NOTE: the Go `dropFilter *partitionSearchOptions` parameter is
    /// deferred together with `deleteRows` (see the module PORT NOTE).
    fn must_merge_parts_internal(
        self: &Arc<Self>,
        pws: Vec<Arc<PartWrapper>>,
        is_final: bool,
        stop_ch: Option<&AtomicBool>,
    ) -> bool {
        if pws.is_empty() {
            // Nothing to merge.
            return true;
        }

        assert_is_in_merge(&pws);
        // Go: defer ddb.releasePartsToMerge(pws)
        let _release_guard = ReleasePartsGuard {
            ddb: self,
            pws: &pws,
        };

        let start_time = Instant::now();

        let dst_part_type = self.get_dst_part_type(&pws, is_final);
        let mut _disk_space_guard = None;
        if dst_part_type != PartType::Inmemory {
            // Make sure there is enough disk space for performing the merge
            let parts_size = get_compressed_size(&pws);
            if try_reserve_disk_space(&self.path, parts_size) {
                _disk_space_guard = Some(DiskSpaceGuard(parts_size));
            } else if !is_final {
                // There is no enough disk space for performing the non-final merge.
                return false;
            }
            // Try performing final merge even if there is no enough disk space
            // in order to persist in-memory data to disk.
            // It is better to crash on out of memory error in this case.
        }

        let _active_merges_guard = match dst_part_type {
            PartType::Inmemory => {
                self.inmemory_merges_total.fetch_add(1, Ordering::SeqCst);
                self.inmemory_active_merges.fetch_add(1, Ordering::SeqCst);
                ActiveMergesGuard(&self.inmemory_active_merges)
            }
            PartType::Small => {
                self.small_part_merges_total.fetch_add(1, Ordering::SeqCst);
                self.small_part_active_merges.fetch_add(1, Ordering::SeqCst);
                ActiveMergesGuard(&self.small_part_active_merges)
            }
            PartType::Big => {
                self.big_part_merges_total.fetch_add(1, Ordering::SeqCst);
                self.big_part_active_merges.fetch_add(1, Ordering::SeqCst);
                ActiveMergesGuard(&self.big_part_active_merges)
            }
        };

        // Initialize destination paths.
        let merge_idx = self.next_merge_idx();
        let dst_part_path = self.get_dst_part_path(dst_part_type, merge_idx);

        if is_final && pws.len() == 1 && pws[0].is_inmemory {
            // Fast path: flush a single in-memory part to disk.
            let mp = pws[0].mp().unwrap();
            mp.must_store_to_disk(&dst_part_path);
            let src_rows_count = mp.ph.rows_count;
            let dst_size = mp.ph.compressed_size_bytes;
            let ph = mp.ph.clone();

            let pw_new = self.open_created_part(&ph, &pws, None, &dst_part_path);
            self.swap_src_with_dst_parts(&pws, pw_new, dst_part_type);
            self.update_merge_metrics(dst_part_type, src_rows_count, start_time, dst_size);
            return true;
        }

        // Prepare blockStreamReaders for source parts.
        let mut bsrs = must_open_block_stream_readers(&pws);

        // Prepare BlockStreamWriter for destination part.
        let mut src_size = 0u64;
        let mut src_rows_count = 0u64;
        let mut src_blocks_count = 0u64;
        for pw in &pws {
            let ph = &pw.ph;
            src_size += ph.compressed_size_bytes;
            src_rows_count += ph.rows_count;
            src_blocks_count += ph.blocks_count;
        }
        let mut bsw = get_block_stream_writer();
        let mut mp_new = if dst_part_type == PartType::Inmemory {
            Some(get_inmemory_part())
        } else {
            None
        };
        match &mut mp_new {
            Some(mp) => bsw.must_init_for_inmemory_part(mp),
            None => {
                let nocache = dst_part_type == PartType::Big;
                bsw.must_init_for_file_part(&dst_part_path, nocache);
            }
        }

        // Merge source parts to destination part.
        let mut ph = PartHeader::default();
        // The final merge shouldn't be stopped even if stop_ch is set.
        let stop_ch = if is_final { None } else { stop_ch };
        must_merge_block_streams(&mut ph, &mut bsw, &mut bsrs, stop_ch);
        put_block_stream_writer(bsw);
        for bsr in bsrs.drain(..) {
            put_block_stream_reader(bsr);
        }

        // Persist partHeader for destination part after the merge.
        if let Some(mp) = &mut mp_new {
            mp.ph = ph.clone();
        } else {
            ph.must_write_metadata(&dst_part_path);
            // Make sure the created part directory contents is synced and visible in case of unclean shutdown.
            fs::must_sync_path_and_parent_dir(&dst_part_path);
        }
        if need_stop(stop_ch) {
            // Remove incomplete destination part
            if dst_part_type != PartType::Inmemory {
                fs::must_remove_dir(&dst_part_path);
            }
            return false;
        }

        // Atomically swap the source parts with the newly created part.
        let pw_new = self.open_created_part(&ph, &pws, mp_new, &dst_part_path);

        let mut dst_size = 0u64;
        let mut dst_rows_count = 0u64;
        let mut dst_blocks_count = 0u64;
        if let Some(pw_new) = &pw_new {
            dst_size = pw_new.ph.compressed_size_bytes;
            dst_rows_count = pw_new.ph.rows_count;
            dst_blocks_count = pw_new.ph.blocks_count;
        }

        self.swap_src_with_dst_parts(&pws, pw_new, dst_part_type);
        self.update_merge_metrics(dst_part_type, src_rows_count, start_time, dst_size);

        let d = start_time.elapsed();
        if d <= Duration::from_secs(60) {
            return true;
        }

        // Log stats for long merges.
        let duration_secs = d.as_secs_f64();
        let rows_per_sec = (src_rows_count as f64 / duration_secs) as i64;
        infof!(
            "merged ({} parts, {} rows, {} blocks, {} bytes) into (1 part, {} rows, {} blocks, {} bytes) in {:.3} seconds at {} rows/sec to {:?}",
            pws.len(),
            src_rows_count,
            src_blocks_count,
            src_size,
            dst_rows_count,
            dst_blocks_count,
            dst_size,
            duration_secs,
            rows_per_sec,
            dst_part_path
        );

        true
    }

    fn update_merge_metrics(
        &self,
        part_type: PartType,
        src_row_count: u64,
        start_time: Instant,
        dst_size: u64,
    ) {
        // PORT NOTE: Go keeps the per-type `esl_merge_duration_seconds` /
        // `esl_merge_bytes` summaries as datadb struct members created with
        // GetOrCreateSummary; the port uses equivalent process-wide statics.
        let (duration, bytes) = merge_summaries(part_type);
        duration.update_duration(start_time);
        bytes.update(dst_size as f64);
        match part_type {
            PartType::Inmemory => {
                self.inmemory_merge_rows_total
                    .fetch_add(src_row_count, Ordering::SeqCst);
            }
            PartType::Small => {
                self.small_part_merge_rows_total
                    .fetch_add(src_row_count, Ordering::SeqCst);
            }
            PartType::Big => {
                self.big_part_merge_rows_total
                    .fetch_add(src_row_count, Ordering::SeqCst);
            }
        }
    }

    fn next_merge_idx(&self) -> u64 {
        self.merge_idx.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn get_dst_part_type(&self, pws: &[Arc<PartWrapper>], is_final: bool) -> PartType {
        let dst_part_size = get_compressed_size(pws);
        if dst_part_size > self.get_max_small_part_size() {
            return PartType::Big;
        }
        if is_final || dst_part_size > get_max_inmemory_part_size() {
            return PartType::Small;
        }
        if !are_all_inmemory_parts(pws) {
            // If at least a single source part is located in file,
            // then the destination part must be in file for durability reasons.
            return PartType::Small;
        }
        PartType::Inmemory
    }

    fn get_dst_part_path(&self, dst_part_type: PartType, merge_idx: u64) -> PathBuf {
        let pt_path = &self.path;
        let mut dst_part_path = PathBuf::new();
        if dst_part_type != PartType::Inmemory {
            dst_part_path = pt_path.join(format!("{merge_idx:016X}"));
        }
        dst_part_path
    }

    fn open_created_part(
        &self,
        ph: &PartHeader,
        pws: &[Arc<PartWrapper>],
        mp_new: Option<InmemoryPart>,
        dst_part_path: &Path,
    ) -> Option<Arc<PartWrapper>> {
        // Open the created part.
        if ph.rows_count == 0 {
            // The created part is empty. Remove it
            if mp_new.is_none() {
                fs::must_remove_dir(dst_part_path);
            }
            return None;
        }
        match mp_new {
            Some(mp) => {
                // Open the created part from memory.
                let mp_ptr = NonNull::from(Box::leak(Box::new(mp)));
                // SAFETY: mp_ptr points to the Box leaked above; it stays
                // alive until PartWrapper::dec_ref() reclaims it.
                let mp_ref: &'static InmemoryPart = unsafe { mp_ptr.as_ref() };
                let p = must_open_inmemory_part(self.pt.clone(), mp_ref);
                let flush_deadline = self.get_flush_to_disk_deadline(pws);
                Some(new_part_wrapper(p, Some(mp_ptr), Some(flush_deadline)))
            }
            None => {
                // Open the created part from disk.
                let p = must_open_file_part(self.pt.clone(), dst_part_path);
                Some(new_part_wrapper(p, None, None))
            }
        }
    }

    pub(crate) fn must_add_rows(&self, lr: &LogRows) {
        self.rb.must_add_rows(lr);
    }

    fn must_flush_log_rows(self: &Arc<Self>, lr: &mut LogRowsInternal) {
        let (p, mp_ptr) = {
            let _permit = INMEMORY_PARTS_CONCURRENCY_CH.acquire();
            let mut mp = get_inmemory_part();
            mp.must_init_from_rows(lr);
            let mp_ptr = NonNull::from(Box::leak(Box::new(mp)));
            // SAFETY: mp_ptr points to the Box leaked above; it stays alive
            // until PartWrapper::dec_ref() reclaims it.
            let mp_ref: &'static InmemoryPart = unsafe { mp_ptr.as_ref() };
            let p = must_open_inmemory_part(self.pt.clone(), mp_ref);
            (p, mp_ptr)
        };

        let flush_deadline = Instant::now() + self.flush_interval;
        let pw = new_part_wrapper(p, Some(mp_ptr), Some(flush_deadline));

        let ps = self.parts.lock().unwrap();
        let mut ps = ps;
        ps.inmemory_parts.push(pw);
        self.start_inmemory_parts_merger_locked(&ps);
    }

    /// updateStats updates s with ddb stats.
    pub(crate) fn update_stats(&self, s: &mut DatadbStats) {
        s.inmemory_merges_count += self.inmemory_merges_total.load(Ordering::SeqCst);
        s.active_inmemory_merges += self.inmemory_active_merges.load(Ordering::SeqCst) as u64;
        s.inmemory_rows_merged += self.inmemory_merge_rows_total.load(Ordering::SeqCst);
        s.small_merges_count += self.small_part_merges_total.load(Ordering::SeqCst);
        s.active_small_merges += self.small_part_active_merges.load(Ordering::SeqCst) as u64;
        s.small_rows_merged += self.small_part_merge_rows_total.load(Ordering::SeqCst);
        s.big_merges_count += self.big_part_merges_total.load(Ordering::SeqCst);
        s.active_big_merges += self.big_part_active_merges.load(Ordering::SeqCst) as u64;
        s.big_rows_merged += self.big_part_merge_rows_total.load(Ordering::SeqCst);

        s.pending_rows = self.rb.len();

        let ps = self.parts.lock().unwrap();

        s.inmemory_rows_count += get_rows_count(&ps.inmemory_parts);
        s.small_part_rows_count += get_rows_count(&ps.small_parts);
        s.big_part_rows_count += get_rows_count(&ps.big_parts);

        s.inmemory_parts += ps.inmemory_parts.len() as u64;
        s.small_parts += ps.small_parts.len() as u64;
        s.big_parts += ps.big_parts.len() as u64;

        s.inmemory_blocks += get_blocks_count(&ps.inmemory_parts);
        s.small_part_blocks += get_blocks_count(&ps.small_parts);
        s.big_part_blocks += get_blocks_count(&ps.big_parts);

        s.compressed_inmemory_size += get_compressed_size(&ps.inmemory_parts);
        s.compressed_small_part_size += get_compressed_size(&ps.small_parts);
        s.compressed_big_part_size += get_compressed_size(&ps.big_parts);

        s.uncompressed_inmemory_size += get_uncompressed_size(&ps.inmemory_parts);
        s.uncompressed_small_part_size += get_uncompressed_size(&ps.small_parts);
        s.uncompressed_big_part_size += get_uncompressed_size(&ps.big_parts);
    }

    /// getMinMaxTimestamps returns min and max timestamps across parts in ddb.
    pub(crate) fn get_min_max_timestamps(&self) -> (i64, i64) {
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;

        let mut update_min_max_timestamps = |pws: &[Arc<PartWrapper>]| {
            for pw in pws {
                let ph = &pw.ph;
                if ph.min_timestamp < min_ts {
                    min_ts = ph.min_timestamp;
                }
                if ph.max_timestamp > max_ts {
                    max_ts = ph.max_timestamp;
                }
            }
        };

        let ps = self.parts.lock().unwrap();
        update_min_max_timestamps(&ps.inmemory_parts);
        update_min_max_timestamps(&ps.small_parts);
        update_min_max_timestamps(&ps.big_parts);
        drop(ps);

        (min_ts, max_ts)
    }

    /// debug_flush() makes sure that the recently ingested data is available for search.
    pub(crate) fn debug_flush(&self) {
        self.rb.flush();
    }

    pub(crate) fn must_create_snapshot_at(self: &Arc<Self>, dst_dir: &Path) {
        fs::must_mkdir_fail_if_exist(dst_dir);

        // flush in-memory parts before making a snapshot
        self.must_flush_inmemory_parts_to_files(true);

        // Get all the file-based parts
        let pws: Vec<Arc<PartWrapper>> = {
            let ps = self.parts.lock().unwrap();
            let mut pws = Vec::with_capacity(ps.small_parts.len() + ps.big_parts.len());
            pws.extend(ps.small_parts.iter().cloned());
            pws.extend(ps.big_parts.iter().cloned());
            for pw in &pws {
                pw.inc_ref();
            }
            pws
        };

        // Write parts.json file
        let part_names = get_part_names(&pws);
        must_write_part_names(dst_dir, Some(&part_names));

        // Make hardlinks for pws at dstDir
        for pw in &pws {
            let src_part_path = &pw.path;
            let dst_part_path = dst_dir.join(src_part_path.file_name().unwrap());
            fs::must_hard_link_files(src_part_path, &dst_part_path);
        }

        // Release all the file-based parts
        for pw in &pws {
            pw.dec_ref();
        }

        // Sync dstDir contents.
        // The parent dir for the dstDir must be synced by the caller.
        fs::must_sync_path(dst_dir);
    }

    fn swap_src_with_dst_parts(
        self: &Arc<Self>,
        pws: &[Arc<PartWrapper>],
        pw_new: Option<Arc<PartWrapper>>,
        dst_part_type: PartType,
    ) {
        // Atomically unregister old parts and add new part to pt.
        let parts_to_remove = parts_to_map(pws);

        let removed_inmemory_parts;
        let removed_small_parts;
        let removed_big_parts;

        {
            // Prevent from deadlock mentioned at https://github.com/VictoriaMetrics/VictoriaLogs/issues/1020#issuecomment-3763912067
            let mut ps = self.parts.lock().unwrap();

            removed_inmemory_parts = remove_parts(&mut ps.inmemory_parts, &parts_to_remove);
            removed_small_parts = remove_parts(&mut ps.small_parts, &parts_to_remove);
            removed_big_parts = remove_parts(&mut ps.big_parts, &parts_to_remove);

            if let Some(pw_new) = &pw_new {
                match dst_part_type {
                    PartType::Inmemory => {
                        ps.inmemory_parts.push(Arc::clone(pw_new));
                        self.start_inmemory_parts_merger_locked(&ps);
                    }
                    PartType::Small => {
                        ps.small_parts.push(Arc::clone(pw_new));
                        self.start_small_parts_merger_locked(&ps);
                    }
                    PartType::Big => {
                        ps.big_parts.push(Arc::clone(pw_new));
                        self.start_big_parts_merger_locked(&ps);
                    }
                }
            }

            // Atomically store the updated list of file-based parts on disk.
            // This must be performed under partsLock in order to prevent from races
            // when multiple concurrently running goroutines update the list.
            if removed_small_parts > 0
                || removed_big_parts > 0
                || (pw_new.is_some() && dst_part_type != PartType::Inmemory)
            {
                let mut part_names = get_part_names(&ps.small_parts);
                part_names.extend(get_part_names(&ps.big_parts));
                must_write_part_names(&self.path, Some(&part_names));
            }
        }

        let removed_parts = removed_inmemory_parts + removed_small_parts + removed_big_parts;
        if removed_parts != parts_to_remove.len() {
            panicf!(
                "BUG: unexpected number of parts removed; got {}, want {}",
                removed_parts,
                parts_to_remove.len()
            );
        }

        // Mark old parts as must be deleted and decrement reference count, so they are eventually closed and deleted.
        for pw in pws {
            pw.must_drop.store(true, Ordering::SeqCst);
            pw.dec_ref();
        }
    }

    fn get_flush_to_disk_deadline(&self, pws: &[Arc<PartWrapper>]) -> Instant {
        let mut d = Instant::now() + self.flush_interval;
        for pw in pws {
            if pw.is_inmemory
                && let Some(fd) = pw.flush_deadline
                && fd < d
            {
                d = fd;
            }
        }
        d
    }

    fn release_parts_to_merge(&self, pws: &[Arc<PartWrapper>]) {
        let _ps = self.parts.lock().unwrap();
        for pw in pws {
            if !pw.is_in_merge.load(Ordering::SeqCst) {
                panicf!("BUG: missing isInMerge flag on the part {:?}", pw.path);
            }
            pw.is_in_merge.store(false, Ordering::SeqCst);
        }
    }

    fn get_max_big_part_size(&self) -> u64 {
        get_max_out_bytes(&self.path)
    }

    fn get_max_small_part_size(&self) -> u64 {
        // Small parts are cached in the OS page cache,
        // so limit their size by the remaining free RAM.
        let mem = memory::remaining();
        let n = (mem as u64 / DEFAULT_PARTS_TO_MERGE as u64).max(10_000_000);
        // Make sure the output part fits available disk space for small parts.
        let size_limit = get_max_out_bytes(&self.path);
        n.min(size_limit)
    }

    /// Returns the parts holding data for `[min_timestamp, max_timestamp]`
    /// (Go `datadb.getPartsForTimeRange`). Each returned part has its refCount
    /// incremented; the caller must `dec_ref()` each when done.
    pub(crate) fn get_parts_for_time_range(
        &self,
        min_timestamp: i64,
        max_timestamp: i64,
    ) -> Vec<Arc<PartWrapper>> {
        let ps = self.parts.lock().unwrap();
        let mut pws: Vec<Arc<PartWrapper>> = Vec::new();
        append_parts_in_time_range(&mut pws, &ps.big_parts, min_timestamp, max_timestamp);
        append_parts_in_time_range(&mut pws, &ps.small_parts, min_timestamp, max_timestamp);
        append_parts_in_time_range(&mut pws, &ps.inmemory_parts, min_timestamp, max_timestamp);
        for pw in &pws {
            pw.inc_ref();
        }
        pws
    }

    pub(crate) fn must_force_merge_all_parts(self: &Arc<Self>) {
        // Flush inmemory parts to files before forced merge
        self.must_flush_inmemory_parts_to_files(true);

        let mut pws: Vec<Arc<PartWrapper>> = Vec::new();

        // Collect all the file parts for forced merge
        {
            let ps = self.parts.lock().unwrap();
            append_all_parts_for_merge_locked(&mut pws, &ps.small_parts);
            append_all_parts_for_merge_locked(&mut pws, &ps.big_parts);
        }

        // If len(pws) == 1, then the merge must run anyway.
        // This allows applying the configured retention, removing the deleted data, etc.

        // Merge pws optimally
        std::thread::scope(|s| {
            let mut pws = pws;
            while !pws.is_empty() {
                let (pws_to_merge, pws_remaining) = get_parts_for_optimal_merge(pws);
                let permit = BIG_PARTS_CONCURRENCY_CH.acquire();

                let ddb = Arc::clone(self);
                std::thread::Builder::new()
                    .name("mustForceMergeAllParts".to_string())
                    .spawn_scoped(s, move || {
                        ddb.must_merge_parts(pws_to_merge, false);
                        drop(permit);
                    })
                    .unwrap();

                pws = pws_remaining;
            }
        });
    }
}

/// getPartsToMergeLocked returns optimal parts to merge from pws.
///
/// The summary size of the returned parts must be smaller than maxOutBytes.
fn get_parts_to_merge_locked(
    pws: &[Arc<PartWrapper>],
    max_out_bytes: u64,
) -> Vec<Arc<PartWrapper>> {
    let mut pws_remaining: Vec<Arc<PartWrapper>> = Vec::with_capacity(pws.len());
    for pw in pws {
        if !pw.is_in_merge.load(Ordering::SeqCst) {
            pws_remaining.push(Arc::clone(pw));
        }
    }

    let pws_to_merge = append_parts_to_merge(Vec::new(), &pws_remaining, max_out_bytes);

    for pw in &pws_to_merge {
        if pw.is_in_merge.swap(true, Ordering::SeqCst) {
            panicf!("BUG: partWrapper.isInMerge cannot be set");
        }
    }

    pws_to_merge
}

fn assert_is_in_merge(pws: &[Arc<PartWrapper>]) {
    for pw in pws {
        if !pw.is_in_merge.load(Ordering::SeqCst) {
            panicf!("BUG: partWrapper.isInMerge unexpectedly set to false");
        }
    }
}

/// getPartsForOptimalMerge returns parts from pws for optimal merge, plus the remaining parts.
fn get_parts_for_optimal_merge(
    pws: Vec<Arc<PartWrapper>>,
) -> (Vec<Arc<PartWrapper>>, Vec<Arc<PartWrapper>>) {
    let pws_to_merge = append_parts_to_merge(Vec::new(), &pws, u64::MAX);
    if pws_to_merge.is_empty() {
        return (pws, Vec::new());
    }

    let m = parts_to_map(&pws_to_merge);
    let mut pws_remaining: Vec<Arc<PartWrapper>> =
        Vec::with_capacity(pws.len() - pws_to_merge.len());
    for pw in &pws {
        if !m.contains(&Arc::as_ptr(pw)) {
            pws_remaining.push(Arc::clone(pw));
        }
    }

    // PORT NOTE: Go nils the pws items so the GC can reclaim them faster;
    // dropping the owned Vec has the same effect here.

    (pws_to_merge, pws_remaining)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PartType {
    Inmemory,
    Small,
    Big,
}
// PORT NOTE: Go's partType is an int with `logger.Panicf("BUG: unknown
// partType=%d")` default branches; the Rust enum makes those unreachable, so
// the default branches are dropped.

pub(crate) type FlushFunc = Arc<dyn Fn(&mut LogRowsInternal) + Send + Sync>;

#[derive(Default)]
pub(crate) struct RowsBuffer {
    shards: Vec<Arc<RowsBufferShard>>,
    next_idx: AtomicU64,
}

impl RowsBuffer {
    pub(crate) fn len(&self) -> u64 {
        let mut n = 0u64;
        for shard in &self.shards {
            let state = shard.mu.lock().unwrap();
            if let Some(lr) = &state.lr {
                n += lr.len() as u64;
            }
        }
        n
    }

    pub(crate) fn init(&mut self, wg: Arc<WaitGroup>, flush_func: FlushFunc) {
        let mut shards = Vec::with_capacity(cgroup::available_cpus());
        for _ in 0..cgroup::available_cpus() {
            shards.push(Arc::new(RowsBufferShard {
                wg: Arc::clone(&wg),
                flush_func: Arc::clone(&flush_func),
                mu: Mutex::new(RowsBufferShardState {
                    lr: None,
                    flush_timer: None,
                }),
            }));
        }
        self.shards = shards;
    }

    pub(crate) fn flush(&self) {
        for shard in &self.shards {
            let mut state = shard.mu.lock().unwrap();
            shard.flush_locked(&mut state);
        }
    }

    pub(crate) fn must_add_rows(&self, lr: &LogRows) {
        if lr.stream_ids.is_empty() {
            return;
        }

        let shards = &self.shards;
        let idx = (self.next_idx.fetch_add(1, Ordering::SeqCst).wrapping_add(1)
            % shards.len() as u64) as usize;
        let shard = &shards[idx];

        let mut state = shard.mu.lock().unwrap();
        if state.flush_timer.is_none() {
            shard.wg.add(1);
            let shard2 = Arc::clone(shard);
            state.flush_timer = Some(FlushTimer::start(Duration::from_secs(1), move || {
                let mut state = shard2.mu.lock().unwrap();
                shard2.flush_locked(&mut state);
                drop(state);
                shard2.wg.done();
            }));
        }
        if state.lr.is_none() {
            state.lr = Some(get_log_rows_internal());
        }
        let shard_lr = state.lr.as_mut().unwrap();
        shard_lr.must_add_rows(lr);
        if shard_lr.need_flush() {
            shard.flush_locked(&mut state);
        }
    }
}

pub(crate) struct RowsBufferShard {
    /// wg is shared with datadb.
    wg: Arc<WaitGroup>,
    flush_func: FlushFunc,

    mu: Mutex<RowsBufferShardState>,
    // PORT NOTE: Go pads the shard to atomicutil.CacheLineSize to prevent
    // false sharing; the port allocates each shard in its own Arc, which
    // already keeps the shards on separate allocations.
}

struct RowsBufferShardState {
    lr: Option<LogRowsInternal>,
    flush_timer: Option<FlushTimer>,
}

impl RowsBufferShard {
    fn flush_locked(&self, state: &mut RowsBufferShardState) {
        if let Some(flush_timer) = state.flush_timer.take()
            && flush_timer.stop()
        {
            self.wg.done();
        }

        if let Some(mut lr) = state.lr.take() {
            (self.flush_func)(&mut lr);
            put_log_rows_internal(lr);
        }
    }
}

/// PORT NOTE: stands in for Go's `time.AfterFunc` one-shot timer: a named
/// thread waits on a Condvar for the duration or an earlier stop() and then
/// runs the callback unless it has been stopped. stop() returns whether the
/// timer was stopped before firing, matching `time.Timer.Stop()`.
struct FlushTimer {
    // 0 = pending, 1 = stopped, 2 = fired
    state: Arc<(Mutex<u8>, Condvar)>,
}

const FLUSH_TIMER_PENDING: u8 = 0;
const FLUSH_TIMER_STOPPED: u8 = 1;
const FLUSH_TIMER_FIRED: u8 = 2;

impl FlushTimer {
    fn start(d: Duration, f: impl FnOnce() + Send + 'static) -> FlushTimer {
        let state = Arc::new((Mutex::new(FLUSH_TIMER_PENDING), Condvar::new()));
        let thread_state = Arc::clone(&state);
        std::thread::Builder::new()
            .name("rowsBufferFlushTimer".to_string())
            .spawn(move || {
                let (mu, cv) = &*thread_state;
                let deadline = Instant::now() + d;
                let mut g = mu.lock().unwrap();
                while *g == FLUSH_TIMER_PENDING {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    g = cv.wait_timeout(g, deadline - now).unwrap().0;
                }
                let fire = *g == FLUSH_TIMER_PENDING;
                if fire {
                    *g = FLUSH_TIMER_FIRED;
                }
                drop(g);
                if fire {
                    f();
                }
            })
            .unwrap();
        FlushTimer { state }
    }

    /// Stops the timer. Returns true if the timer has been stopped before firing.
    fn stop(&self) -> bool {
        let (mu, cv) = &*self.state;
        let mut g = mu.lock().unwrap();
        if *g != FLUSH_TIMER_PENDING {
            return false;
        }
        *g = FLUSH_TIMER_STOPPED;
        cv.notify_all();
        true
    }
}

/// DatadbStats contains various stats for datadb.
#[derive(Debug, Default, Clone)]
pub struct DatadbStats {
    /// InmemoryMergesCount is the number of inmemory merges performed in the given datadb.
    pub inmemory_merges_count: u64,

    /// ActiveInmemoryMerges is the number of currently active inmemory merges performed by the given datadb.
    pub active_inmemory_merges: u64,

    /// InmemoryRowsMerged is the number of rows merged to inmemory parts.
    pub inmemory_rows_merged: u64,

    /// SmallMergesCount is the number of small file merges performed in the given datadb.
    pub small_merges_count: u64,

    /// ActiveSmallMerges is the number of currently active small file merges performed by the given datadb.
    pub active_small_merges: u64,

    /// SmallRowsMerged is the number of rows merged to small parts.
    pub small_rows_merged: u64,

    /// BigMergesCount is the number of big file merges performed in the given datadb.
    pub big_merges_count: u64,

    /// ActiveBigMerges is the number of currently active big file merges performed by the given datadb.
    pub active_big_merges: u64,

    /// BigRowsMerged is the number of rows merged to big parts.
    pub big_rows_merged: u64,

    /// PendingRows is the number of rows, which weren't flushed to searchable part yet.
    pub pending_rows: u64,

    /// InmemoryRowsCount is the number of rows, which weren't flushed to disk yet.
    pub inmemory_rows_count: u64,

    /// SmallPartRowsCount is the number of rows stored on disk in small parts.
    pub small_part_rows_count: u64,

    /// BigPartRowsCount is the number of rows stored on disk in big parts.
    pub big_part_rows_count: u64,

    /// InmemoryParts is the number of in-memory parts, which weren't flushed to disk yet.
    pub inmemory_parts: u64,

    /// SmallParts is the number of file-based small parts stored on disk.
    pub small_parts: u64,

    /// BigParts is the number of file-based big parts stored on disk.
    pub big_parts: u64,

    /// InmemoryBlocks is the number of in-memory blocks, which weren't flushed to disk yet.
    pub inmemory_blocks: u64,

    /// SmallPartBlocks is the number of file-based small blocks stored on disk.
    pub small_part_blocks: u64,

    /// BigPartBlocks is the number of file-based big blocks stored on disk.
    pub big_part_blocks: u64,

    /// CompressedInmemorySize is the size of compressed data stored in memory.
    pub compressed_inmemory_size: u64,

    /// CompressedSmallPartSize is the size of compressed small parts data stored on disk.
    pub compressed_small_part_size: u64,

    /// CompressedBigPartSize is the size of compressed big data stored on disk.
    pub compressed_big_part_size: u64,

    /// UncompressedInmemorySize is the size of uncompressed data stored in memory.
    pub uncompressed_inmemory_size: u64,

    /// UncompressedSmallPartSize is the size of uncompressed small data stored on disk.
    pub uncompressed_small_part_size: u64,

    /// UncompressedBigPartSize is the size of uncompressed big data stored on disk.
    pub uncompressed_big_part_size: u64,
}

impl DatadbStats {
    // Ported for Go parity; not yet wired into a caller (see PARITY.md).
    #[allow(dead_code)]
    pub(crate) fn reset(&mut self) {
        *self = DatadbStats::default();
    }

    /// RowsCount returns the number of rows stored in datadb.
    pub fn rows_count(&self) -> u64 {
        self.inmemory_rows_count + self.small_part_rows_count + self.big_part_rows_count
    }
}

fn parts_to_map(pws: &[Arc<PartWrapper>]) -> HashSet<*const PartWrapper> {
    let mut m: HashSet<*const PartWrapper> = HashSet::with_capacity(pws.len());
    for pw in pws {
        m.insert(Arc::as_ptr(pw));
    }
    if m.len() != pws.len() {
        panicf!(
            "BUG: {} duplicate parts found out of {} parts",
            pws.len() - m.len(),
            pws.len()
        );
    }
    m
}

fn remove_parts(
    pws: &mut Vec<Arc<PartWrapper>>,
    parts_to_remove: &HashSet<*const PartWrapper>,
) -> usize {
    // PORT NOTE: Go compacts the slice in place and nils the tail for the GC;
    // retain() is the equivalent.
    let n = pws.len();
    pws.retain(|pw| !parts_to_remove.contains(&Arc::as_ptr(pw)));
    n - pws.len()
}

fn must_open_block_stream_readers(pws: &[Arc<PartWrapper>]) -> Vec<BlockStreamReader<'_>> {
    let mut bsrs = Vec::with_capacity(pws.len());
    for pw in pws {
        let mut bsr = get_block_stream_reader();
        match pw.mp() {
            Some(mp) => bsr.must_init_from_inmemory_part(mp),
            None => bsr.must_init_from_file_part(&pw.path),
        }
        bsrs.push(bsr);
    }
    bsrs
}

fn new_part_wrapper(
    p: Part<'static>,
    mp: Option<NonNull<InmemoryPart>>,
    flush_deadline: Option<Instant>,
) -> Arc<PartWrapper> {
    let ph = p.ph.clone();
    let path = p.path.clone();
    let is_inmemory = mp.is_some();
    let pw = Arc::new(PartWrapper {
        ref_count: AtomicI32::new(0),
        must_drop: AtomicBool::new(false),
        is_in_merge: AtomicBool::new(false),
        flush_deadline,
        ph,
        path,
        is_inmemory,
        inner: Mutex::new(Some(PartWrapperInner { p, mp })),
    });

    // Increase reference counter for newly created part - it is decreased when the part
    // is removed from the list of open parts.
    pw.inc_ref();

    pw
}

fn get_max_inmemory_part_size() -> u64 {
    // Allocate 10% of allowed memory for in-memory parts.
    let n = (0.1 * memory::allowed() as f64 / MAX_INMEMORY_PARTS_PER_PARTITION as f64) as u64;
    n.max(1_000_000)
}

fn are_all_inmemory_parts(pws: &[Arc<PartWrapper>]) -> bool {
    for pw in pws {
        if !pw.is_inmemory {
            return false;
        }
    }
    true
}

fn get_max_out_bytes(path: &Path) -> u64 {
    available_disk_space(path).min(MAX_BIG_PART_SIZE)
}

fn available_disk_space(path: &Path) -> u64 {
    let available = fs::must_get_free_space(path);
    let reserved = RESERVED_DISK_SPACE.load(Ordering::SeqCst);
    if available < reserved {
        return 0;
    }
    available - reserved
}

fn try_reserve_disk_space(path: &Path, n: u64) -> bool {
    let available = fs::must_get_free_space(path);
    let reserved = reserve_disk_space(n);
    if available >= reserved {
        return true;
    }
    release_disk_space(n);
    false
}

fn reserve_disk_space(n: u64) -> u64 {
    RESERVED_DISK_SPACE.fetch_add(n, Ordering::SeqCst) + n
}

fn release_disk_space(n: u64) {
    RESERVED_DISK_SPACE.fetch_sub(n, Ordering::SeqCst);
}

/// reservedDiskSpace tracks global reserved disk space for currently executed
/// background merges across all the partitions.
///
/// It should allow avoiding background merges when there is no free disk space.
static RESERVED_DISK_SPACE: AtomicU64 = AtomicU64::new(0);

fn need_stop(stop_ch: Option<&AtomicBool>) -> bool {
    stop_ch.is_some_and(|s| s.load(Ordering::SeqCst))
}

/// mustCloseDatadb can be called only when nobody accesses ddb.
pub(crate) fn must_close_datadb(ddb: &Arc<Datadb>) {
    // Flush ddb.rb for the last time
    ddb.rb.flush();

    // Notify background workers to stop.
    // Make it under the parts lock in order to prevent from calling ddb.wg.add()
    // after stop is set and ddb.wg.wait() is called.
    {
        let mut ps = ddb.parts.lock().unwrap();
        ps.stopped = true;
        ddb.stop.store(true, Ordering::SeqCst);
    }
    // Wake up the inmemoryPartsFlusher ticker.
    {
        let _g = ddb.flusher_mu.lock().unwrap();
        ddb.flusher_cv.notify_all();
    }

    // Wait for background workers to stop.
    ddb.wg.wait();

    // flush in-memory data to disk
    ddb.must_flush_inmemory_parts_to_files(true);
    let mut ps = ddb.parts.lock().unwrap();
    if !ps.inmemory_parts.is_empty() {
        panicf!(
            "BUG: the number of in-memory parts must be zero after flushing them to disk; got {}",
            ps.inmemory_parts.len()
        );
    }
    ps.inmemory_parts = Vec::new();

    // close small parts
    for pw in ps.small_parts.drain(..) {
        pw.dec_ref();
        let n = pw.ref_count.load(Ordering::SeqCst);
        if n != 0 {
            panicf!("BUG: there are {} references to smallPart", n);
        }
    }

    // close big parts
    for pw in ps.big_parts.drain(..) {
        pw.dec_ref();
        let n = pw.ref_count.load(Ordering::SeqCst);
        if n != 0 {
            panicf!("BUG: there are {} references to bigPart", n);
        }
    }

    // PORT NOTE: Go sets ddb.path = "" and ddb.pt = nil here; the port keeps
    // the fields immutable — the datadb is dropped by the owning partition.
}

fn get_part_names(pws: &[Arc<PartWrapper>]) -> Vec<String> {
    let mut part_names = Vec::with_capacity(pws.len());
    for pw in pws {
        if pw.is_inmemory {
            // Skip in-memory parts
            continue;
        }
        let part_name = pw
            .path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        part_names.push(part_name);
    }
    part_names.sort_unstable();
    part_names
}

/// PORT NOTE: Go passes a nil or non-nil `[]string` to json.Marshal
/// (serializing to `null` or `[...]` respectively); the port mirrors nil with
/// `None`.
fn must_write_part_names(path: &Path, part_names: Option<&[String]>) {
    let data = marshal_part_names_json(part_names);
    let part_names_path = path.join(PARTS_FILENAME);
    fs::must_write_atomic(&part_names_path, &data, true);
}

fn must_read_part_names(path: &Path) -> Vec<String> {
    let part_names_path = path.join(PARTS_FILENAME);
    let data = match std::fs::read(&part_names_path) {
        Ok(data) => data,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // The parts.json file is missing. This can happen if EsLogs shuts down uncleanly
            // (via OOM crash, a panic, SIGKILL or hardware shutdown) in the middle of creating
            // new per-day partition inside the mustCreatePartition() function.
            // Check if there are any part directories in the datadb directory.
            let des = fs::must_read_dir(path);
            let mut part_dirs: Vec<String> = Vec::new();
            for de in &des {
                if !fs::is_dir_or_symlink(de) {
                    continue;
                }
                part_dirs.push(de.file_name().to_string_lossy().into_owned());
            }

            if part_dirs.is_empty() {
                warnf!(
                    "creating missing {} with empty parts list, since no part directories found in {}",
                    part_names_path.display(),
                    path.display()
                );
                must_write_part_names(path, None);
                return Vec::new();
            }

            // Parts exist but parts.json is missing - this is an unexpected state that requires manual intervention
            panicf!(
                "FATAL: cannot read {}: {}; found part directories [{}] in {}. This indicates corruption. Manually remove the {} partition directory to resolve the corruption (the partition data will be lost)",
                part_names_path.display(),
                err,
                part_dirs.join(" "),
                path.display(),
                path.display()
            );
            unreachable!()
        }
        Err(err) => {
            panicf!("FATAL: cannot read {}: {}", part_names_path.display(), err);
            unreachable!()
        }
    };
    match unmarshal_part_names_json(&data) {
        Ok(part_names) => part_names,
        Err(err) => {
            panicf!("FATAL: cannot parse {}: {}", part_names_path.display(), err);
            unreachable!()
        }
    }
}

// PORT NOTE: Go uses encoding/json for the parts.json content (a JSON array
// of strings, or `null` for a nil slice); the port hand-rolls the minimal
// codec instead of adding a JSON dependency. Go additionally escapes `<`,
// `>` and `&`; part names are hex directory names, so this is not mirrored.

fn marshal_part_names_json(part_names: Option<&[String]>) -> Vec<u8> {
    let Some(part_names) = part_names else {
        return b"null".to_vec();
    };
    let mut data = Vec::with_capacity(2 + part_names.len() * 20);
    data.push(b'[');
    for (i, name) in part_names.iter().enumerate() {
        if i > 0 {
            data.push(b',');
        }
        marshal_json_string(&mut data, name);
    }
    data.push(b']');
    data
}

fn marshal_json_string(dst: &mut Vec<u8>, s: &str) {
    dst.push(b'"');
    for c in s.chars() {
        match c {
            '"' => dst.extend_from_slice(b"\\\""),
            '\\' => dst.extend_from_slice(b"\\\\"),
            '\n' => dst.extend_from_slice(b"\\n"),
            '\r' => dst.extend_from_slice(b"\\r"),
            '\t' => dst.extend_from_slice(b"\\t"),
            c if (c as u32) < 0x20 => {
                dst.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes());
            }
            c => {
                let mut buf = [0u8; 4];
                dst.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    dst.push(b'"');
}

fn unmarshal_part_names_json(data: &[u8]) -> Result<Vec<String>, String> {
    let s = std::str::from_utf8(data).map_err(|err| format!("invalid UTF-8: {err}"))?;
    let s = s.trim();
    if s == "null" {
        return Ok(Vec::new());
    }
    let s = s
        .strip_prefix('[')
        .ok_or_else(|| "expected '[' at the beginning of JSON array".to_string())?;
    let s = s
        .strip_suffix(']')
        .ok_or_else(|| "expected ']' at the end of JSON array".to_string())?;
    let mut part_names = Vec::new();
    let mut rest = s.trim_start();
    if rest.is_empty() {
        return Ok(part_names);
    }
    loop {
        let (name, tail) = unmarshal_json_string(rest)?;
        part_names.push(name);
        rest = tail.trim_start();
        if rest.is_empty() {
            return Ok(part_names);
        }
        rest = rest
            .strip_prefix(',')
            .ok_or_else(|| "expected ',' between JSON array items".to_string())?
            .trim_start();
    }
}

fn unmarshal_json_string(s: &str) -> Result<(String, &str), String> {
    let s = s
        .strip_prefix('"')
        .ok_or_else(|| "expected '\"' at the beginning of JSON string".to_string())?;
    let mut result = String::new();
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Ok((result, &s[i + 1..])),
            '\\' => {
                let (_, esc) = chars
                    .next()
                    .ok_or_else(|| "truncated escape sequence in JSON string".to_string())?;
                match esc {
                    '"' => result.push('"'),
                    '\\' => result.push('\\'),
                    '/' => result.push('/'),
                    'b' => result.push('\u{0008}'),
                    'f' => result.push('\u{000C}'),
                    'n' => result.push('\n'),
                    'r' => result.push('\r'),
                    't' => result.push('\t'),
                    'u' => {
                        let mut code = 0u32;
                        for _ in 0..4 {
                            let (_, h) = chars
                                .next()
                                .ok_or_else(|| "truncated \\u escape in JSON string".to_string())?;
                            code = code * 16
                                + h.to_digit(16).ok_or_else(|| {
                                    format!("invalid hex digit {h:?} in \\u escape")
                                })?;
                        }
                        let c = char::from_u32(code)
                            .ok_or_else(|| format!("invalid \\u{code:04x} escape"))?;
                        result.push(c);
                    }
                    _ => return Err(format!("unsupported escape sequence \\{esc}")),
                }
            }
            c => result.push(c),
        }
    }
    Err("missing closing '\"' in JSON string".to_string())
}

/// mustRemoveUnusedDirs removes dirs at path, which are missing in partNames.
///
/// These dirs may be left after unclean shutdown.
fn must_remove_unused_dirs(path: &Path, part_names: &[String]) {
    let des = fs::must_read_dir(path);
    let m: HashSet<&str> = part_names.iter().map(|s| s.as_str()).collect();
    let mut removed_dirs = 0;
    for de in &des {
        if !fs::is_dir_or_symlink(de) {
            // Skip non-directories.
            continue;
        }
        let file_name = de.file_name();
        let fn_str = file_name.to_string_lossy();
        if !m.contains(fn_str.as_ref()) {
            let delete_path = path.join(&file_name);
            infof!(
                "removed unused directory {} (e.g. not listed in parts.json), which may have been left after an unclean shutdown",
                delete_path.display()
            );
            fs::must_remove_dir(&delete_path);
            removed_dirs += 1;
        }
    }
    if removed_dirs > 0 {
        fs::must_sync_path(path);
    }
}

/// appendPartsToMerge finds optimal parts to merge from src, appends them to dst and returns the result.
fn append_parts_to_merge(
    mut dst: Vec<Arc<PartWrapper>>,
    src: &[Arc<PartWrapper>],
    max_out_bytes: u64,
) -> Vec<Arc<PartWrapper>> {
    if src.len() < 2 {
        // There is no need in merging zero or one part :)
        return dst;
    }

    // Filter out too big parts.
    // This should reduce N for O(N^2) algorithm below.
    let max_in_part_bytes = (max_out_bytes as f64 / MIN_MERGE_MULTIPLIER) as u64;
    let mut src: Vec<Arc<PartWrapper>> = src
        .iter()
        .filter(|pw| pw.ph.compressed_size_bytes <= max_in_part_bytes)
        .cloned()
        .collect();

    sort_parts_for_optimal_merge(&mut src);

    let max_src_parts = DEFAULT_PARTS_TO_MERGE.min(src.len());
    let min_src_parts = max_src_parts.div_ceil(2).max(2);

    // Exhaustive search for parts giving the lowest write amplification when merged.
    let mut pws: Option<&[Arc<PartWrapper>]> = None;
    let mut max_m = 0f64;
    for i in min_src_parts..=max_src_parts {
        for j in 0..(src.len() - i + 1) {
            let a = &src[j..j + i];
            if a[0].ph.compressed_size_bytes * (a.len() as u64)
                < a[a.len() - 1].ph.compressed_size_bytes
            {
                // Do not merge parts with too big difference in size,
                // since this results in unbalanced merges.
                continue;
            }
            let out_size = get_compressed_size(a);
            if out_size > max_out_bytes {
                // There is no need in verifying remaining parts with bigger sizes.
                break;
            }
            let m = out_size as f64 / a[a.len() - 1].ph.compressed_size_bytes as f64;
            if m < max_m {
                continue;
            }
            max_m = m;
            pws = Some(a);
        }
    }

    let mut min_m = DEFAULT_PARTS_TO_MERGE as f64 / 2.0;
    if min_m < MIN_MERGE_MULTIPLIER {
        min_m = MIN_MERGE_MULTIPLIER;
    }
    if max_m < min_m {
        // There is no sense in merging parts with too small m,
        // since this leads to high disk write IO.
        return dst;
    }
    dst.extend(pws.unwrap_or_default().iter().cloned());
    dst
}

fn sort_parts_for_optimal_merge(pws: &mut [Arc<PartWrapper>]) {
    // Sort src parts by size and backwards timestamp.
    // This should improve adjanced points' locality in the merged parts.
    pws.sort_unstable_by(|x, y| {
        let a = &x.ph;
        let b = &y.ph;
        if a.compressed_size_bytes == b.compressed_size_bytes {
            b.min_timestamp.cmp(&a.min_timestamp)
        } else {
            a.compressed_size_bytes.cmp(&b.compressed_size_bytes)
        }
    });
}

fn get_compressed_size(pws: &[Arc<PartWrapper>]) -> u64 {
    let mut n = 0u64;
    for pw in pws {
        n += pw.ph.compressed_size_bytes;
    }
    n
}

fn get_uncompressed_size(pws: &[Arc<PartWrapper>]) -> u64 {
    let mut n = 0u64;
    for pw in pws {
        n += pw.ph.uncompressed_size_bytes;
    }
    n
}

fn get_rows_count(pws: &[Arc<PartWrapper>]) -> u64 {
    let mut n = 0u64;
    for pw in pws {
        n += pw.ph.rows_count;
    }
    n
}

fn get_blocks_count(pws: &[Arc<PartWrapper>]) -> u64 {
    let mut n = 0u64;
    for pw in pws {
        n += pw.ph.blocks_count;
    }
    n
}

fn append_all_parts_for_merge_locked(dst: &mut Vec<Arc<PartWrapper>>, src: &[Arc<PartWrapper>]) {
    for pw in src {
        if !pw.is_in_merge.load(Ordering::SeqCst) {
            pw.is_in_merge.store(true, Ordering::SeqCst);
            dst.push(Arc::clone(pw));
        }
    }
}

/// Appends parts from `src` overlapping `[min_timestamp, max_timestamp]` to
/// `dst` (Go `appendPartsInTimeRange`).
fn append_parts_in_time_range(
    dst: &mut Vec<Arc<PartWrapper>>,
    src: &[Arc<PartWrapper>],
    min_timestamp: i64,
    max_timestamp: i64,
) {
    for pw in src {
        if max_timestamp < pw.ph.min_timestamp || min_timestamp > pw.ph.max_timestamp {
            continue;
        }
        dst.push(Arc::clone(pw));
    }
}

/// PORT NOTE: minimal Go sync.WaitGroup equivalent (std has none); shared
/// between the datadb background workers and the rowsBuffer flush timers.
pub(crate) struct WaitGroup {
    count: Mutex<u64>,
    cv: Condvar,
}

impl WaitGroup {
    pub(crate) fn new() -> WaitGroup {
        WaitGroup {
            count: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    pub(crate) fn add(&self, n: u64) {
        let mut count = self.count.lock().unwrap();
        *count += n;
    }

    pub(crate) fn done(&self) {
        let mut count = self.count.lock().unwrap();
        if *count == 0 {
            panicf!("BUG: WaitGroup counter must be positive on done()");
        }
        *count -= 1;
        if *count == 0 {
            self.cv.notify_all();
        }
    }

    pub(crate) fn wait(&self) {
        let mut count = self.count.lock().unwrap();
        while *count > 0 {
            count = self.cv.wait(count).unwrap();
        }
    }
}

/// PORT NOTE: stands in for Go's buffered channels used as counting
/// semaphores (`inmemoryPartsConcurrencyCh` etc.).
struct ConcurrencyCh {
    cap: usize,
    used: Mutex<usize>,
    cv: Condvar,
}

impl ConcurrencyCh {
    fn new(cap: usize) -> ConcurrencyCh {
        ConcurrencyCh {
            cap,
            used: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    fn acquire(&self) -> ConcurrencyChPermit<'_> {
        let mut used = self.used.lock().unwrap();
        while *used >= self.cap {
            used = self.cv.wait(used).unwrap();
        }
        *used += 1;
        ConcurrencyChPermit { ch: self }
    }
}

struct ConcurrencyChPermit<'a> {
    ch: &'a ConcurrencyCh,
}

impl Drop for ConcurrencyChPermit<'_> {
    fn drop(&mut self) {
        let mut used = self.ch.used.lock().unwrap();
        *used -= 1;
        self.ch.cv.notify_one();
    }
}

/// Go: `defer ddb.releasePartsToMerge(pws)`.
struct ReleasePartsGuard<'a> {
    ddb: &'a Datadb,
    pws: &'a [Arc<PartWrapper>],
}

impl Drop for ReleasePartsGuard<'_> {
    fn drop(&mut self) {
        self.ddb.release_parts_to_merge(self.pws);
    }
}

/// Go: `defer releaseDiskSpace(partsSize)`.
struct DiskSpaceGuard(u64);

impl Drop for DiskSpaceGuard {
    fn drop(&mut self) {
        release_disk_space(self.0);
    }
}

/// Go: `defer ddb.inmemoryActiveMerges.Add(-1)` etc.
struct ActiveMergesGuard<'a>(&'a AtomicI64);

impl Drop for ActiveMergesGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(-1, Ordering::SeqCst);
    }
}

/// Returns the `(esl_merge_duration_seconds, esl_merge_bytes)` summaries for
/// the given part type (Go `datadb` members created via GetOrCreateSummary).
fn merge_summaries(
    part_type: PartType,
) -> (
    &'static std::sync::Arc<esl_common::metrics::Summary>,
    &'static std::sync::Arc<esl_common::metrics::Summary>,
) {
    use std::sync::{Arc, LazyLock};

    use esl_common::metrics::{Summary, get_or_create_summary};

    static INMEMORY: LazyLock<(Arc<Summary>, Arc<Summary>)> = LazyLock::new(|| {
        (
            get_or_create_summary(r#"esl_merge_duration_seconds{type="storage/inmemory"}"#),
            get_or_create_summary(r#"esl_merge_bytes{type="storage/inmemory"}"#),
        )
    });
    static SMALL: LazyLock<(Arc<Summary>, Arc<Summary>)> = LazyLock::new(|| {
        (
            get_or_create_summary(r#"esl_merge_duration_seconds{type="storage/small"}"#),
            get_or_create_summary(r#"esl_merge_bytes{type="storage/small"}"#),
        )
    });
    static BIG: LazyLock<(Arc<Summary>, Arc<Summary>)> = LazyLock::new(|| {
        (
            get_or_create_summary(r#"esl_merge_duration_seconds{type="storage/big"}"#),
            get_or_create_summary(r#"esl_merge_bytes{type="storage/big"}"#),
        )
    });
    let pair = match part_type {
        PartType::Inmemory => &*INMEMORY,
        PartType::Small => &*SMALL,
        PartType::Big => &*BIG,
    };
    (&pair.0, &pair.1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_rows::get_log_rows;
    use crate::rows::Field;
    use crate::tenant_id::TenantID;
    use std::sync::atomic::AtomicU64;

    // PORT NOTE: Go's seeded math/rand stream cannot be reproduced without
    // porting math/rand's internal tables, so a deterministic substitute is
    // used (same rationale as the inmemory_part.rs test module). Here the
    // core stepping is SplitMix64 rather than the xorshift used in
    // inmemory_part.rs: its NormFloat64 substitute (Box-Muller, below)
    // produces a size distribution whose merge convergence matches Go's
    // original test bounds (overhead <= 2.1 AND <= 18 leftover parts), so
    // test_append_parts_to_merge_many_parts keeps the exact Go assertions.
    struct GoRand {
        state: u64,
    }

    impl GoRand {
        fn new(seed: u64) -> GoRand {
            GoRand { state: seed }
        }

        fn next_u64(&mut self) -> u64 {
            self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn uint32(&mut self) -> u32 {
            (self.next_u64() >> 32) as u32
        }

        fn float64(&mut self) -> f64 {
            (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
        }

        fn intn(&mut self, n: u64) -> u64 {
            self.next_u64() % n
        }

        fn int63(&mut self) -> i64 {
            (self.next_u64() >> 1) as i64
        }

        fn shuffle<T>(&mut self, a: &mut [T]) {
            for i in (1..a.len()).rev() {
                let j = self.intn(i as u64 + 1) as usize;
                a.swap(i, j);
            }
        }

        /// PORT NOTE: Go's rand.NormFloat64 (Ziggurat algorithm) is replaced
        /// with Box-Muller over the deterministic substitute.
        fn norm_float64(&mut self) -> f64 {
            let mut u1 = self.float64();
            if u1 <= 0.0 {
                u1 = f64::MIN_POSITIVE;
            }
            let u2 = self.float64();
            (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
        }
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn new_test_log_rows(streams: usize, rows_per_stream: usize, seed: u64) -> LogRows {
        let stream_tags = ["some-stream-tag"];
        let mut lr = get_log_rows(&stream_tags, &[], &[], &[], "");
        let mut rng = GoRand::new(seed);
        let mut fields: Vec<Field> = Vec::new();
        for i in 0..streams {
            let tenant_id = TenantID {
                account_id: rng.uint32(),
                project_id: rng.uint32(),
            };
            for j in 0..rows_per_stream {
                // Add stream tags
                fields.clear();
                fields.push(field("some-stream-tag", &format!("some-stream-value-{i}")));
                // Add the remaining tags
                for k in 0..5 {
                    if rng.float64() < 0.5 {
                        fields.push(field(&format!("field_{k}"), &format!("value_{i}_{j}_{k}")));
                    }
                }
                // add a message field
                fields.push(field("", &format!("some row number {j} at stream {i}")));
                // add a field with constant value
                fields.push(field("job", "foobar"));
                // add a field with uint value
                fields.push(field("response_size_bytes", &format!("{}", rng.intn(1234))));
                // shuffle fields in order to check de-shuffling algorithm
                rng.shuffle(&mut fields);
                let timestamp = rng.int63();
                lr.must_add(tenant_id, timestamp, &mut fields, -1);
            }
        }
        lr
    }

    #[test]
    fn test_rows_buffer() {
        let rows_flushed = Arc::new(AtomicU64::new(0));
        let rows_flushed2 = Arc::clone(&rows_flushed);
        let flush_func: FlushFunc = Arc::new(move |lr: &mut LogRowsInternal| {
            rows_flushed2.fetch_add(lr.len() as u64, Ordering::SeqCst);
        });
        let wg_buffer = Arc::new(WaitGroup::new());

        let mut rb = RowsBuffer::default();
        rb.init(Arc::clone(&wg_buffer), flush_func);
        let rb = &rb;

        const CONCURRENCY: usize = 10;
        const ROWS_PER_INSERT: usize = 200;
        const INSERT_LOOPS: usize = 30;
        std::thread::scope(|s| {
            for _ in 0..CONCURRENCY {
                s.spawn(move || {
                    let lr = new_test_log_rows(1, ROWS_PER_INSERT, 1);
                    for _ in 0..INSERT_LOOPS {
                        rb.must_add_rows(&lr);
                    }
                });
            }
        });

        rb.flush();
        wg_buffer.wait();

        let rows_len = rows_flushed.load(Ordering::SeqCst);
        let rows_len_expected = (CONCURRENCY * ROWS_PER_INSERT * INSERT_LOOPS) as u64;
        assert_eq!(
            rows_len, rows_len_expected,
            "unexpected number of rows; got {rows_len}; want {rows_len_expected}"
        );
    }

    #[test]
    fn test_append_parts_to_merge_many_parts() {
        // Verify that big number of parts are merged into minimal number of parts
        // using minimum merges.
        let mut sizes: Vec<u64> = Vec::new();
        let mut max_out_size = 0u64;
        let mut r = GoRand::new(1);
        for _ in 0..1024 {
            // PORT NOTE: Go computes uint64(uint32(r.NormFloat64() * 1e9));
            // the float→uint32 conversion on amd64 truncates via int64 (so
            // negative values wrap to large uint32 values), reproduced here
            // with the explicit i64→u32 cast chain.
            let mut n = (r.norm_float64() * 1e9) as i64 as u32 as u64;
            n += 1;
            max_out_size += n;
            sizes.push(n);
        }
        let mut pws = new_test_part_wrappers_for_sizes(&sizes);

        let mut iterations_count = 0;
        let mut size_merged_total = 0u64;
        loop {
            let pms = append_parts_to_merge(Vec::new(), &pws, max_out_size);
            if pms.is_empty() {
                break;
            }
            let m: HashSet<*const PartWrapper> = pms.iter().map(Arc::as_ptr).collect();
            let mut pws_new: Vec<Arc<PartWrapper>> = Vec::new();
            let mut size = 0u64;
            for pw in &pws {
                if m.contains(&Arc::as_ptr(pw)) {
                    size += pw.ph.compressed_size_bytes;
                } else {
                    pws_new.push(Arc::clone(pw));
                }
            }
            let pw = new_test_part_wrapper_for_size(size);
            size_merged_total += size;
            pws_new.push(pw);
            pws = pws_new;
            iterations_count += 1;
        }
        let sizes = new_test_sizes_from_part_wrappers(&pws);
        let size_total: u64 = sizes.iter().sum();
        let overhead = size_merged_total as f64 / size_total as f64;
        assert!(
            overhead <= 2.1,
            "too big overhead; sizes={sizes:?}, iterationsCount={iterations_count}, sizeTotal={size_total}, sizeMergedTotal={size_merged_total}, overhead={overhead}"
        );
        assert!(
            sizes.len() <= 18,
            "too many sizes {}; sizes={sizes:?}, iterationsCount={iterations_count}, sizeTotal={size_total}, sizeMergedTotal={size_merged_total}, overhead={overhead}",
            sizes.len()
        );
    }

    fn new_test_sizes_from_part_wrappers(pws: &[Arc<PartWrapper>]) -> Vec<u64> {
        pws.iter().map(|pw| pw.ph.compressed_size_bytes).collect()
    }

    fn new_test_part_wrapper_for_size(size: u64) -> Arc<PartWrapper> {
        // PORT NOTE: Go builds a partWrapper around a bare `&part{ph: ...}`;
        // the port caches ph on the wrapper (see PartWrapper::ph), so the
        // test wrapper carries no opened part at all.
        Arc::new(PartWrapper {
            ref_count: AtomicI32::new(0),
            must_drop: AtomicBool::new(false),
            is_in_merge: AtomicBool::new(false),
            flush_deadline: None,
            ph: PartHeader {
                compressed_size_bytes: size,
                ..PartHeader::default()
            },
            path: PathBuf::new(),
            is_inmemory: false,
            inner: Mutex::new(None),
        })
    }

    fn new_test_part_wrappers_for_sizes(sizes: &[u64]) -> Vec<Arc<PartWrapper>> {
        sizes
            .iter()
            .map(|&size| new_test_part_wrapper_for_size(size))
            .collect()
    }
}

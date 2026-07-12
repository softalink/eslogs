//! Port of `lib/mergeset/table.go`.
//!
//! Concurrency mapping (mirrors the datadb.rs port of the same Go patterns):
//!
//! - Go goroutine merge workers → named `std::thread`s; worker counts and the
//!   merge-idle semantics (workers exit when there is nothing to merge and are
//!   restarted under the parts lock whenever a new part is registered) are
//!   preserved.
//! - `sync.WaitGroup` / `syncwg.WaitGroup` → the local [`WaitGroup`]
//!   (Mutex + Condvar).
//! - `stopCh` (closed under `partsLock`) → `PartsState::stopped` plus the
//!   `Table::stop` AtomicBool for lock-free checks.
//! - The `inmemoryPartsConcurrencyCh`/`filePartsConcurrencyCh` counting
//!   semaphores → [`ConcurrencyCh`] statics with the same capacities.
//! - `inmemoryPartsLimitCh` (buffered channel of size maxInmemoryParts used
//!   with `select` on stopCh) → [`InmemoryPartsLimit`].
//!
//! PORT NOTE: `partWrapper.refCount` is replaced by `Arc<PartWrapper>`: Go
//! needs the manual refcount because the GC provides no destruction point for
//! closing files / removing dropped part dirs; `Drop for PartWrapper` gives
//! the same semantics (close on last reference, then delete if must_drop).
//!
//! PORT NOTE: `isReadOnly` is not ported - the port's Storage has no
//! read-only mode, so the merger loops never park on it.
//!
//! PORT NOTE: the `WaitGroup`/`ConcurrencyCh` helpers are duplicated from
//! datadb.rs, which keeps them private (and is owned by the datadb port).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use esl_common::{cgroup, fs, infof, memory, panicf, warnf};

use super::PARTS_FILENAME;
use super::PrepareBlockCallback;
use super::block_stream_reader::BlockStreamReader;
use super::block_stream_writer::BlockStreamWriter;
use super::encoding::{InmemoryBlock, MAX_INMEMORY_BLOCK_SIZE};
use super::inmemory_part::InmemoryPart;
use super::merge::{MergeError, merge_block_streams};
use super::part::{Part, must_open_file_part, new_part_from_inmemory_part};
use super::part_header::PartHeader;

/// maxInmemoryParts is the maximum number of inmemory parts in the table.
///
/// This limit allows reducing CPU usage under high ingestion rate.
///
/// This number may be reached when the insertion pace outreaches merger pace.
/// If this number is reached, then the data ingestion is paused until
/// background mergers reduce the number of parts below this number.
const MAX_INMEMORY_PARTS: usize = 30;

/// Default number of parts to merge at once.
///
/// This number has been obtained empirically - it gives the lowest possible
/// overhead. See appendPartsToMerge tests for details.
const DEFAULT_PARTS_TO_MERGE: usize = 15;

/// maxPartSize is the maximum part size in bytes.
///
/// This number should be limited by the amount of time required to merge
/// parts of this summary size. The required time shouldn't exceed a day.
const MAX_PART_SIZE: u64 = 400_000_000_000;

/// The interval for flushing buffered data to parts, so it becomes visible to
/// search.
const PENDING_ITEMS_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// The default interval for calling flushCallback when there is pending data
/// to flush.
///
/// It is set relatively high in order to improve the effectiveness of caches
/// reset by flushCallback. It is used when the flush_callback_interval arg at
/// must_open is set to zero.
const DEFAULT_FLUSH_CALLBACK_INTERVAL: Duration = Duration::from_secs(10);

/// minMergeMultiplier is the minimum multiplier for the size of the output
/// part compared to the size of the maximum input part for the merge.
///
/// Higher value reduces write amplification (disk write IO induced by the
/// merge), while increases the number of unmerged parts. The 1.7 is good
/// enough for production workloads.
const MIN_MERGE_MULTIPLIER: f64 = 1.7;

/// maxItemsPerCachedPart is the maximum items per created part by the merge,
/// which must be cached in the OS page cache.
///
/// Such parts are usually frequently accessed, so it is good to cache their
/// contents in OS page cache.
fn max_items_per_cached_part() -> u64 {
    let mem = memory::remaining();
    // Production data shows that each item occupies ~4 bytes in the compressed
    // part. It is expected no more than defaultPartsToMerge/2 parts exist in
    // the OS page cache before they are merged into bigger part. Half of the
    // remaining RAM must be left for lib/storage parts, so the maxItems is
    // calculated using the below code:
    ((mem as u64) / (4 * DEFAULT_PARTS_TO_MERGE as u64)).max(1_000_000)
}

/// The number of shards for rawItems per table.
///
/// Higher number of shards reduces CPU contention and increases the max
/// bandwidth on multi-core systems.
fn raw_items_shards_per_table() -> usize {
    let cpus = cgroup::available_cpus();
    let multiplier = cpus.min(16);
    cpus * multiplier
}

const MAX_BLOCKS_PER_SHARD: usize = 256;

static TOO_LONG_ITEMS_TOTAL: AtomicU64 = AtomicU64::new(0);

static INMEMORY_PARTS_CONCURRENCY_CH: LazyLock<ConcurrencyCh> =
    LazyLock::new(|| ConcurrencyCh::new(get_inmemory_parts_concurrency()));
static FILE_PARTS_CONCURRENCY_CH: LazyLock<ConcurrencyCh> =
    LazyLock::new(|| ConcurrencyCh::new(get_file_parts_concurrency()));

fn get_inmemory_parts_concurrency() -> usize {
    // The concurrency for processing in-memory parts must equal to the number
    // of CPU cores, since these operations are CPU-bound.
    cgroup::available_cpus()
}

fn get_file_parts_concurrency() -> usize {
    let n = cgroup::available_cpus();
    if n < 4 {
        // Allow at least 4 concurrent workers for file parts on systems with
        // less than 4 CPU cores in order to be able to make small file merges
        // when big file merges are in progress.
        return 4;
    }
    n
}

/// partWrapper wraps an opened part (see the module PORT NOTE about
/// refCount → Arc).
pub(crate) struct PartWrapper {
    /// p is an opened part.
    pub(super) p: Part,

    /// mp references the inmemory part used for initializing p.
    pub(super) mp: Option<Arc<InmemoryPart>>,

    /// mustDrop marks the part for deletion once the last reference is gone.
    must_drop: AtomicBool,

    /// isInMerge is set to true if the part takes part in merge.
    ///
    /// PORT NOTE: a plain bool guarded by partsLock in Go; an AtomicBool here
    /// since PartWrapper is shared via Arc. It is still only mutated under
    /// the table parts lock.
    is_in_merge: AtomicBool,

    /// The deadline when the in-memory part must be flushed to disk
    /// (None for file-based parts, standing in for Go's zero time.Time).
    flush_to_disk_deadline: Option<Instant>,
}

#[cfg(test)]
impl PartWrapper {
    /// Builds a bare part wrapper for tests operating below the Table level
    /// (Go tests construct `&part{...}` directly).
    pub(super) fn new_for_test(p: Part, mp: Option<Arc<InmemoryPart>>) -> Arc<PartWrapper> {
        Arc::new(PartWrapper {
            p,
            mp,
            must_drop: AtomicBool::new(false),
            is_in_merge: AtomicBool::new(false),
            flush_to_disk_deadline: None,
        })
    }
}

impl Drop for PartWrapper {
    fn drop(&mut self) {
        let delete_path = if self.mp.is_none() && self.must_drop.load(Ordering::SeqCst) {
            Some(self.p.path.clone())
        } else {
            None
        };
        self.p.must_close();
        self.mp = None;
        if let Some(delete_path) = delete_path {
            fs::must_remove_dir(&delete_path);
        }
    }
}

fn new_part_wrapper_from_inmemory_part(
    mp: Arc<InmemoryPart>,
    flush_to_disk_deadline: Instant,
) -> Arc<PartWrapper> {
    let p = new_part_from_inmemory_part(&mp);
    Arc::new(PartWrapper {
        p,
        mp: Some(mp),
        must_drop: AtomicBool::new(false),
        is_in_merge: AtomicBool::new(false),
        flush_to_disk_deadline: Some(flush_to_disk_deadline),
    })
}

fn new_part_wrapper_from_file_part(p: Part) -> Arc<PartWrapper> {
    Arc::new(PartWrapper {
        p,
        mp: None,
        must_drop: AtomicBool::new(false),
        is_in_merge: AtomicBool::new(false),
        flush_to_disk_deadline: None,
    })
}

/// Table represents mergeset table.
pub(crate) struct Table {
    active_inmemory_merges: AtomicI64,
    active_file_merges: AtomicI64,

    inmemory_merges_count: AtomicU64,
    file_merges_count: AtomicU64,

    inmemory_items_merged: AtomicU64,
    file_items_merged: AtomicU64,

    items_added: AtomicU64,
    items_added_size_bytes: AtomicU64,

    inmemory_parts_limit_reached_count: AtomicU64,

    merge_idx: AtomicU64,

    path: PathBuf,

    /// The interval for guaranteed flush of recently ingested data from
    /// memory to on-disk parts so they survive process crash.
    flush_interval: Duration,

    flush_callback: Option<Box<dyn Fn() + Send + Sync>>,
    flush_callback_interval: Duration,
    need_flush_callback_call: AtomicBool,

    prepare_block: Option<PrepareBlockCallback>,

    /// rawItems contains recently added items that haven't been converted to
    /// parts yet. rawItems aren't visible for search due to performance
    /// reasons.
    raw_items: RawItemsShards,

    /// parts contains the in-memory and file-based parts, plus the `stopped`
    /// flag standing in for the closed `stopCh` (Go: `partsLock` guarding
    /// `inmemoryParts`/`fileParts`).
    parts: Mutex<PartsState>,

    /// stop mirrors `stopCh` for lock-free checks.
    /// It is set under the parts lock together with `PartsState::stopped`.
    stop: AtomicBool,

    /// inmemoryPartsLimitCh limits the number of inmemory parts to
    /// maxInmemoryParts in order to prevent from data ingestion slowdown.
    inmemory_parts_limit: InmemoryPartsLimit,

    /// ticker_mu/ticker_cv wake the flusher/callback tickers early on close.
    ticker_mu: Mutex<()>,
    ticker_cv: Condvar,

    /// wg is used for waiting for all the background workers to stop.
    ///
    /// wg.add() must be called under the parts lock after checking whether
    /// `stopped` isn't set.
    wg: Arc<WaitGroup>,

    /// Go: `flushPendingItemsWG syncwg.WaitGroup`.
    flush_pending_items_wg: WaitGroup,
}

struct PartsState {
    /// inmemoryParts contains inmemory parts, which are visible for search.
    inmemory_parts: Vec<Arc<PartWrapper>>,

    /// fileParts contains file-backed parts, which are visible for search.
    file_parts: Vec<Arc<PartWrapper>>,

    /// stopped is set when the table is being closed.
    stopped: bool,
}

struct RawItemsShards {
    flush_deadline_ms: AtomicI64,

    shard_idx: AtomicU32,

    /// shards reduce lock contention when adding rows on multi-CPU systems.
    shards: Vec<RawItemsShard>,

    ibs_to_flush: Mutex<Vec<InmemoryBlock>>,

    /// PORT NOTE: Go keeps `maxBlocksPerShard` as a package-level var so the
    /// stress test can lower it; the port stores it per table for the same
    /// purpose.
    max_blocks_per_shard: usize,
}

#[derive(Default)]
struct RawItemsShard {
    flush_deadline_ms: AtomicI64,
    ibs: Mutex<Vec<InmemoryBlock>>,
    // PORT NOTE: Go pads the shard to the cache-line size to prevent false
    // sharing; the shards here carry a Mutex + Vec spanning multiple words,
    // so the padding is omitted.
}

fn now_unix_milli() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

impl RawItemsShards {
    fn new(shards_count: usize, max_blocks_per_shard: usize) -> RawItemsShards {
        let mut shards = Vec::with_capacity(shards_count);
        shards.resize_with(shards_count, RawItemsShard::default);
        RawItemsShards {
            flush_deadline_ms: AtomicI64::new(0),
            shard_idx: AtomicU32::new(0),
            shards,
            ibs_to_flush: Mutex::new(Vec::new()),
            max_blocks_per_shard,
        }
    }

    /// Port of `rawItemsShards.addItems`.
    fn add_items(&self, tb: &Arc<Table>, items: &[Vec<u8>]) {
        let shards_len = self.shards.len() as u32;
        let mut items = items;
        while !items.is_empty() {
            let n = self.shard_idx.fetch_add(1, Ordering::SeqCst) + 1;
            let idx = (n % shards_len) as usize;
            let (tail_items, ibs_to_flush) =
                self.shards[idx].add_items(items, self.max_blocks_per_shard);
            self.add_ibs_to_flush(tb, ibs_to_flush);
            items = tail_items;
        }
    }

    /// Port of `rawItemsShards.addIbsToFlush`.
    fn add_ibs_to_flush(&self, tb: &Arc<Table>, ibs_to_flush: Vec<InmemoryBlock>) {
        if ibs_to_flush.is_empty() {
            return;
        }

        let mut ibs_to_merge = Vec::new();

        {
            let mut g = self.ibs_to_flush.lock().unwrap();
            if g.is_empty() {
                self.update_flush_deadline();
            }
            g.extend(ibs_to_flush);
            if g.len() >= self.max_blocks_per_shard * cgroup::available_cpus() {
                ibs_to_merge = std::mem::take(&mut *g);
            }
        }

        tb.flush_blocks_to_inmemory_parts(ibs_to_merge, false);
    }

    fn len(&self) -> usize {
        let mut n = 0;
        for shard in &self.shards {
            let ibs = shard.ibs.lock().unwrap();
            n += ibs.iter().map(|ib| ib.items.len()).sum::<usize>();
        }
        n
    }

    fn update_flush_deadline(&self) {
        self.flush_deadline_ms.store(
            now_unix_milli() + PENDING_ITEMS_FLUSH_INTERVAL.as_millis() as i64,
            Ordering::SeqCst,
        );
    }

    /// Port of `rawItemsShards.flush`.
    fn flush(&self, tb: &Arc<Table>, is_final: bool) {
        let mut dst: Vec<InmemoryBlock> = Vec::new();

        let current_time_ms = now_unix_milli();
        let flush_deadline_ms = self.flush_deadline_ms.load(Ordering::SeqCst);
        if is_final || current_time_ms >= flush_deadline_ms {
            let mut g = self.ibs_to_flush.lock().unwrap();
            dst = std::mem::take(&mut *g);
        }

        for shard in &self.shards {
            shard.append_blocks_to_flush(&mut dst, current_time_ms, is_final);
        }

        tb.flush_blocks_to_inmemory_parts(dst, is_final);
    }
}

impl RawItemsShard {
    /// Port of `rawItemsShard.addItems`. Returns the remaining tail items and
    /// the inmemory blocks that must be flushed.
    fn add_items<'a>(
        &self,
        items: &'a [Vec<u8>],
        max_blocks_per_shard: usize,
    ) -> (&'a [Vec<u8>], Vec<InmemoryBlock>) {
        let mut ibs_to_flush: Vec<InmemoryBlock> = Vec::new();
        let mut tail_items: &'a [Vec<u8>] = &[];

        let mut ibs = self.ibs.lock().unwrap();
        if ibs.is_empty() {
            ibs.push(InmemoryBlock::default());
            self.update_flush_deadline();
        }
        for (i, item) in items.iter().enumerate() {
            if ibs.last_mut().unwrap().add(item) {
                continue;
            }
            if ibs.len() >= max_blocks_per_shard {
                ibs_to_flush.append(&mut ibs);
                ibs.reserve(max_blocks_per_shard);
                tail_items = &items[i..];
                break;
            }
            let mut ib = InmemoryBlock::default();
            if ib.add(item) {
                ibs.push(ib);
                continue;
            }

            // Skip too long item
            let item_prefix = &item[..item.len().min(128)];
            TOO_LONG_ITEMS_TOTAL.fetch_add(1, Ordering::SeqCst);
            // PORT NOTE: Go throttles this log message to once per 5 seconds;
            // the port logs every occurrence.
            warnf!(
                "skipping adding too long item to indexdb: len(item)={}; it shouldn't exceed {} bytes; item prefix={:?}",
                item.len(),
                MAX_INMEMORY_BLOCK_SIZE,
                item_prefix
            );
        }
        drop(ibs);

        (tail_items, ibs_to_flush)
    }

    fn update_flush_deadline(&self) {
        self.flush_deadline_ms.store(
            now_unix_milli() + PENDING_ITEMS_FLUSH_INTERVAL.as_millis() as i64,
            Ordering::SeqCst,
        );
    }

    /// Port of `rawItemsShard.appendBlocksToFlush`.
    fn append_blocks_to_flush(
        &self,
        dst: &mut Vec<InmemoryBlock>,
        current_time_ms: i64,
        is_final: bool,
    ) {
        let flush_deadline_ms = self.flush_deadline_ms.load(Ordering::SeqCst);
        if !is_final && current_time_ms < flush_deadline_ms {
            // Fast path - nothing to flush
            return;
        }

        // Slow path - move ibs to dst
        let mut ibs = self.ibs.lock().unwrap();
        dst.append(&mut ibs);
    }
}

/// Opens a table on the given path (port of `MustOpenTable`).
///
/// The flush_interval is the interval for flushing pending in-memory data to
/// disk.
///
/// Optional flush_callback is called every time new data batch is flushed to
/// the underlying storage and becomes visible to search.
///
/// The flush_callback_interval is how often flush_callback is invoked when
/// there is pending data to flush. If it is zero, then
/// DEFAULT_FLUSH_CALLBACK_INTERVAL is used.
///
/// Optional prepare_block is called during merge before flushing the prepared
/// block to persistent storage.
///
/// The table is created if it doesn't exist yet.
pub(crate) fn must_open_table(
    path: &str,
    flush_interval: Duration,
    flush_callback: Option<Box<dyn Fn() + Send + Sync>>,
    flush_callback_interval: Duration,
    prepare_block: Option<PrepareBlockCallback>,
) -> Arc<Table> {
    must_open_table_ex(
        path,
        flush_interval,
        flush_callback,
        flush_callback_interval,
        prepare_block,
        raw_items_shards_per_table(),
        MAX_BLOCKS_PER_SHARD,
    )
}

/// See [`must_open_table`]; the shard parameters are exposed for tests only
/// (Go overrides the corresponding package-level vars there).
pub(super) fn must_open_table_ex(
    path: &str,
    mut flush_interval: Duration,
    flush_callback: Option<Box<dyn Fn() + Send + Sync>>,
    mut flush_callback_interval: Duration,
    prepare_block: Option<PrepareBlockCallback>,
    shards_per_table: usize,
    max_blocks_per_shard: usize,
) -> Arc<Table> {
    if flush_interval < PENDING_ITEMS_FLUSH_INTERVAL {
        // There is no sense in setting flushInterval to values smaller than
        // pendingItemsFlushInterval, since pending rows unconditionally remain
        // in memory for up to pendingItemsFlushInterval.
        flush_interval = PENDING_ITEMS_FLUSH_INTERVAL;
    }

    if flush_callback_interval.is_zero() {
        flush_callback_interval = DEFAULT_FLUSH_CALLBACK_INTERVAL;
    }

    let path = Path::new(path);

    // Create a directory at the path if it doesn't exist yet.
    fs::must_mkdir_if_not_exist(path);

    // Open table parts.
    let pws = must_open_parts(path);

    // Sync the path and the parent dir, so the path becomes visible in the
    // parent dir.
    fs::must_sync_path_and_parent_dir(path);

    let tb = Arc::new(Table {
        active_inmemory_merges: AtomicI64::new(0),
        active_file_merges: AtomicI64::new(0),
        inmemory_merges_count: AtomicU64::new(0),
        file_merges_count: AtomicU64::new(0),
        inmemory_items_merged: AtomicU64::new(0),
        file_items_merged: AtomicU64::new(0),
        items_added: AtomicU64::new(0),
        items_added_size_bytes: AtomicU64::new(0),
        inmemory_parts_limit_reached_count: AtomicU64::new(0),
        merge_idx: AtomicU64::new(0),
        path: path.to_path_buf(),
        flush_interval,
        flush_callback,
        flush_callback_interval,
        need_flush_callback_call: AtomicBool::new(false),
        prepare_block,
        raw_items: RawItemsShards::new(shards_per_table, max_blocks_per_shard),
        parts: Mutex::new(PartsState {
            inmemory_parts: Vec::new(),
            file_parts: pws,
            stopped: false,
        }),
        stop: AtomicBool::new(false),
        inmemory_parts_limit: InmemoryPartsLimit::new(MAX_INMEMORY_PARTS),
        ticker_mu: Mutex::new(()),
        ticker_cv: Condvar::new(),
        wg: Arc::new(WaitGroup::new()),
        flush_pending_items_wg: WaitGroup::new(),
    });
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    tb.merge_idx.store(now_nanos, Ordering::SeqCst);
    tb.start_background_workers();

    tb
}

impl Table {
    fn start_background_workers(self: &Arc<Self>) {
        // Start file parts mergers, so they could start merging unmerged parts
        // if needed. There is no need in starting in-memory parts mergers,
        // since there are no in-memory parts yet.
        self.start_file_parts_mergers();

        self.start_pending_items_flusher();
        self.start_inmemory_parts_flusher();
        self.start_flush_callback_worker();
    }

    fn start_file_parts_mergers(self: &Arc<Self>) {
        let ps = self.parts.lock().unwrap();
        for _ in 0..FILE_PARTS_CONCURRENCY_CH.cap {
            self.start_file_parts_merger_locked(&ps);
        }
    }

    fn start_inmemory_parts_merger_locked(self: &Arc<Self>, ps: &PartsState) {
        if ps.stopped {
            return;
        }
        self.wg.add(1);
        let tb = Arc::clone(self);
        std::thread::Builder::new()
            .name("mergesetInmemMerger".to_string())
            .spawn(move || {
                tb.inmemory_parts_merger();
                tb.wg.done();
            })
            .unwrap();
    }

    fn start_file_parts_merger_locked(self: &Arc<Self>, ps: &PartsState) {
        if ps.stopped {
            return;
        }
        self.wg.add(1);
        let tb = Arc::clone(self);
        std::thread::Builder::new()
            .name("mergesetFileMerger".to_string())
            .spawn(move || {
                tb.file_parts_merger();
                tb.wg.done();
            })
            .unwrap();
    }

    fn start_pending_items_flusher(self: &Arc<Self>) {
        self.wg.add(1);
        let tb = Arc::clone(self);
        std::thread::Builder::new()
            .name("mergesetPendingFlusher".to_string())
            .spawn(move || {
                tb.pending_items_flusher();
                tb.wg.done();
            })
            .unwrap();
    }

    fn start_inmemory_parts_flusher(self: &Arc<Self>) {
        self.wg.add(1);
        let tb = Arc::clone(self);
        std::thread::Builder::new()
            .name("mergesetInmemFlusher".to_string())
            .spawn(move || {
                tb.inmemory_parts_flusher();
                tb.wg.done();
            })
            .unwrap();
    }

    fn start_flush_callback_worker(self: &Arc<Self>) {
        if self.flush_callback.is_none() {
            return;
        }

        self.wg.add(1);
        let tb = Arc::clone(self);
        std::thread::Builder::new()
            .name("mergesetFlushCallback".to_string())
            .spawn(move || {
                // call flushCallback at flushCallbackInterval in order to
                // improve the effectiveness of caches, which are reset by the
                // flushCallback.
                loop {
                    if tb.sleep_or_stop(tb.flush_callback_interval) {
                        (tb.flush_callback.as_ref().unwrap())();
                        break;
                    }
                    if tb
                        .need_flush_callback_call
                        .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        (tb.flush_callback.as_ref().unwrap())();
                    }
                }
                tb.wg.done();
            })
            .unwrap();
    }

    /// Sleeps for d, waking early on close. Returns true if the table is
    /// being closed.
    fn sleep_or_stop(&self, d: Duration) -> bool {
        let deadline = Instant::now() + d;
        let mut g = self.ticker_mu.lock().unwrap();
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (g2, _) = self.ticker_cv.wait_timeout(g, deadline - now).unwrap();
            g = g2;
        }
    }

    fn pending_items_flusher(self: &Arc<Self>) {
        // do not add jitter in order to guarantee flush interval
        loop {
            if self.sleep_or_stop(PENDING_ITEMS_FLUSH_INTERVAL) {
                return;
            }
            self.flush_pending_items(false);
        }
    }

    fn inmemory_parts_flusher(self: &Arc<Self>) {
        // do not add jitter in order to guarantee flush interval
        loop {
            if self.sleep_or_stop(self.flush_interval) {
                return;
            }
            self.flush_inmemory_parts_to_files(false);
        }
    }

    fn flush_pending_items(self: &Arc<Self>, is_final: bool) {
        self.flush_pending_items_wg.add(1);
        self.raw_items.flush(self, is_final);
        self.flush_pending_items_wg.done();
    }

    fn flush_inmemory_items_to_files(self: &Arc<Self>) {
        self.flush_pending_items(true);
        self.flush_inmemory_parts_to_files(true);
    }

    fn flush_inmemory_parts_to_files(self: &Arc<Self>, is_final: bool) {
        let current_time = Instant::now();
        let mut pws: Vec<Arc<PartWrapper>> = Vec::new();

        {
            let ps = self.parts.lock().unwrap();
            for pw in &ps.inmemory_parts {
                if !pw.is_in_merge.load(Ordering::SeqCst)
                    && (is_final || pw.flush_to_disk_deadline.is_some_and(|d| d < current_time))
                {
                    pw.is_in_merge.store(true, Ordering::SeqCst);
                    pws.push(Arc::clone(pw));
                }
            }
        }

        if let Err(err) = self.merge_inmemory_parts_to_files(pws) {
            panicf!("FATAL: cannot merge in-memory parts to files: {}", err);
        }
    }

    /// Port of `Table.mergeInmemoryPartsToFiles`.
    fn merge_inmemory_parts_to_files(
        self: &Arc<Self>,
        pws: Vec<Arc<PartWrapper>>,
    ) -> Result<(), String> {
        let pws_len = pws.len();

        let err_global: Mutex<Option<String>> = Mutex::new(None);
        std::thread::scope(|s| {
            let mut pws = pws;
            while !pws.is_empty() {
                let (pws_to_merge, pws_remaining) = get_parts_for_optimal_merge(pws);
                let permit = INMEMORY_PARTS_CONCURRENCY_CH.acquire();

                let tb = Arc::clone(self);
                let err_global = &err_global;
                std::thread::Builder::new()
                    .name("mergesetInmemToFiles".to_string())
                    .spawn_scoped(s, move || {
                        if let Err(err) = tb.merge_parts(pws_to_merge, None, true) {
                            // There is no need for the errForciblyStopped
                            // check here, since stop_ch=None is passed.
                            let mut g = err_global.lock().unwrap();
                            if g.is_none() {
                                *g = Some(err.to_string());
                            }
                        }
                        drop(permit);
                    })
                    .unwrap();

                pws = pws_remaining;
            }
        });

        let err = err_global.lock().unwrap().take();
        match err {
            Some(err) => Err(format!("cannot optimally merge {pws_len} parts: {err}")),
            None => Ok(()),
        }
    }

    /// Makes sure all the recently added data is visible to search
    /// (port of `Table.DebugFlush`).
    ///
    /// Note: this function doesn't store all the in-memory data to disk - it
    /// just converts recently added items to searchable parts, which can be
    /// stored either in memory (if they are quite small) or to persistent
    /// disk.
    ///
    /// This function is for debugging and testing purposes only, since it may
    /// slow down data ingestion when used frequently.
    pub fn debug_flush(self: &Arc<Self>) {
        self.flush_pending_items(true);

        // Wait for background flushers to finish.
        self.flush_pending_items_wg.wait();
    }

    /// Adds the given items to the tb (port of `Table.AddItems`).
    ///
    /// The function ignores items with length exceeding
    /// MAX_INMEMORY_BLOCK_SIZE. It logs the ignored items, so users could
    /// notice and fix the issue.
    pub fn add_items(self: &Arc<Self>, items: &[Vec<u8>]) {
        self.raw_items.add_items(self, items);
        self.items_added
            .fetch_add(items.len() as u64, Ordering::SeqCst);
        let n: usize = items.iter().map(|item| item.len()).sum();
        self.items_added_size_bytes
            .fetch_add(n as u64, Ordering::SeqCst);
    }

    /// Returns a parts snapshot (port of `Table.getParts`; the parts are
    /// released by dropping the returned vec, Go's `putParts`).
    pub(super) fn get_parts(&self) -> Vec<Arc<PartWrapper>> {
        let ps = self.parts.lock().unwrap();
        let mut dst = Vec::with_capacity(ps.inmemory_parts.len() + ps.file_parts.len());
        dst.extend(ps.inmemory_parts.iter().cloned());
        dst.extend(ps.file_parts.iter().cloned());
        dst
    }

    /// Port of `Table.flushBlocksToInmemoryParts`.
    fn flush_blocks_to_inmemory_parts(self: &Arc<Self>, ibs: Vec<InmemoryBlock>, is_final: bool) {
        if ibs.is_empty() {
            return;
        }

        // Merge ibs into in-memory parts.
        let pws_lock: Mutex<Vec<Arc<PartWrapper>>> = Mutex::new(Vec::with_capacity(
            ibs.len().div_ceil(DEFAULT_PARTS_TO_MERGE),
        ));
        std::thread::scope(|s| {
            let mut ibs = ibs;
            while !ibs.is_empty() {
                let n = DEFAULT_PARTS_TO_MERGE.min(ibs.len());
                let permit = INMEMORY_PARTS_CONCURRENCY_CH.acquire();

                let ibs_tail = ibs.split_off(n);
                let ibs_chunk = std::mem::replace(&mut ibs, ibs_tail);

                let tb = Arc::clone(self);
                let pws_lock = &pws_lock;
                std::thread::Builder::new()
                    .name("mergesetFlushBlocks".to_string())
                    .spawn_scoped(s, move || {
                        if let Some(pw) = tb.create_inmemory_part(ibs_chunk) {
                            pws_lock.lock().unwrap().push(pw);
                        }
                        drop(permit);
                    })
                    .unwrap();
            }
        });
        let mut pws = pws_lock.into_inner().unwrap();

        // Merge pws into a single in-memory part.
        let max_part_size = get_max_inmemory_part_size();
        while pws.len() > 1 {
            pws = self.must_merge_inmemory_parts(pws);

            let mut pws_remaining = Vec::with_capacity(pws.len());
            for pw in pws {
                if pw.p.size >= max_part_size {
                    self.add_to_inmemory_parts(pw, is_final);
                } else {
                    pws_remaining.push(pw);
                }
            }
            pws = pws_remaining;
        }
        if pws.len() == 1 {
            self.add_to_inmemory_parts(pws.pop().unwrap(), is_final);
        }
    }

    /// Port of `Table.addToInmemoryParts`.
    fn add_to_inmemory_parts(self: &Arc<Self>, pw: Arc<PartWrapper>, is_final: bool) {
        // Wait until the number of in-memory parts goes below maxInmemoryParts.
        // This prevents from excess CPU usage during search in tb under high
        // ingestion rate to tb.
        if !self.inmemory_parts_limit.try_acquire() {
            self.inmemory_parts_limit_reached_count
                .fetch_add(1, Ordering::SeqCst);
            self.inmemory_parts_limit.acquire_or_stop(&self.stop);
        }

        {
            let mut ps = self.parts.lock().unwrap();
            ps.inmemory_parts.push(pw);
            self.start_inmemory_parts_merger_locked(&ps);
        }

        if let Some(flush_callback) = &self.flush_callback {
            if is_final {
                flush_callback();
            } else {
                // Use load in front of compare_exchange in order to avoid slow
                // inter-CPU synchronization when the flag is already set.
                if !self.need_flush_callback_call.load(Ordering::SeqCst) {
                    let _ = self.need_flush_callback_call.compare_exchange(
                        false,
                        true,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                }
            }
        }
    }

    /// Port of `Table.mustMergeInmemoryParts`.
    fn must_merge_inmemory_parts(
        self: &Arc<Self>,
        pws: Vec<Arc<PartWrapper>>,
    ) -> Vec<Arc<PartWrapper>> {
        let pws_result: Mutex<Vec<Arc<PartWrapper>>> = Mutex::new(Vec::new());
        std::thread::scope(|s| {
            let mut pws = pws;
            while !pws.is_empty() {
                let (pws_to_merge, pws_remaining) = get_parts_for_optimal_merge(pws);
                let permit = INMEMORY_PARTS_CONCURRENCY_CH.acquire();

                let tb = Arc::clone(self);
                let pws_result = &pws_result;
                std::thread::Builder::new()
                    .name("mergesetMergeInmem".to_string())
                    .spawn_scoped(s, move || {
                        let pw = tb.must_merge_inmemory_parts_final(pws_to_merge);
                        pws_result.lock().unwrap().push(pw);
                        drop(permit);
                    })
                    .unwrap();

                pws = pws_remaining;
            }
        });

        pws_result.into_inner().unwrap()
    }

    /// Merges the given in-memory part wrappers into a single new in-memory
    /// part wrapper (port of `Table.mustMergeInmemoryPartsFinal`).
    fn must_merge_inmemory_parts_final(
        self: &Arc<Self>,
        pws: Vec<Arc<PartWrapper>>,
    ) -> Arc<PartWrapper> {
        if pws.is_empty() {
            panicf!("BUG: pws must contain at least a single item");
        }
        if pws.len() == 1 {
            // Nothing to merge
            return pws.into_iter().next().unwrap();
        }

        let mut bsrs: Vec<BlockStreamReader<'_>> = Vec::with_capacity(pws.len());
        for pw in &pws {
            let Some(mp) = &pw.mp else {
                panicf!("BUG: unexpected file part");
                unreachable!()
            };
            let mut bsr = BlockStreamReader::default();
            bsr.must_init_from_inmemory_part(mp);
            bsrs.push(bsr);
        }

        let flush_to_disk_deadline = get_flush_to_disk_deadline(&pws, self.flush_interval);
        self.must_merge_into_inmemory_part(bsrs, flush_to_disk_deadline)
        // Go decrements the source pws refCounts here; dropping `pws` does it.
    }

    /// Port of `Table.createInmemoryPart`.
    fn create_inmemory_part(self: &Arc<Self>, ibs: Vec<InmemoryBlock>) -> Option<Arc<PartWrapper>> {
        // Prepare blockStreamReaders for source blocks.
        let mut bsrs: Vec<BlockStreamReader<'static>> = Vec::with_capacity(ibs.len());
        for ib in &ibs {
            if ib.items.is_empty() {
                continue;
            }
            let mut bsr = BlockStreamReader::default();
            bsr.must_init_from_inmemory_block(ib);
            bsrs.push(bsr);
        }
        drop(ibs);
        if bsrs.is_empty() {
            return None;
        }

        let flush_to_disk_deadline = Instant::now() + self.flush_interval;
        if bsrs.len() == 1 {
            // Nothing to merge. Just return a single inmemory part.
            let mut bsr = bsrs.pop().unwrap();
            let mut mp = InmemoryPart::default();
            mp.init(&mut bsr.block);
            return Some(new_part_wrapper_from_inmemory_part(
                Arc::new(mp),
                flush_to_disk_deadline,
            ));
        }

        Some(self.must_merge_into_inmemory_part(bsrs, flush_to_disk_deadline))
    }

    /// Port of `Table.mustMergeIntoInmemoryPart`.
    fn must_merge_into_inmemory_part(
        self: &Arc<Self>,
        mut bsrs: Vec<BlockStreamReader<'_>>,
        flush_to_disk_deadline: Instant,
    ) -> Arc<PartWrapper> {
        // Prepare blockStreamWriter for destination part.
        let mut out_items_count = 0u64;
        for bsr in &bsrs {
            out_items_count += bsr.ph.items_count;
        }
        let compress_level = get_compress_level(out_items_count);
        let mut mp_dst = InmemoryPart::default();
        let ph = {
            let mut bsw = BlockStreamWriter::default();
            bsw.must_init_from_inmemory_part(&mut mp_dst, compress_level);

            // Merge parts. The merge shouldn't be interrupted, so pass
            // stop_ch=None.
            match self.merge_parts_internal("", &mut bsw, &mut bsrs, PartType::Inmemory, None) {
                Ok(ph) => ph,
                Err(err) => {
                    panicf!("FATAL: cannot merge inmemoryBlocks: {}", err);
                    unreachable!()
                }
            }
        };
        for bsr in &mut bsrs {
            bsr.must_close();
        }
        mp_dst.ph = ph;

        new_part_wrapper_from_inmemory_part(Arc::new(mp_dst), flush_to_disk_deadline)
    }

    fn get_max_file_part_size(&self) -> u64 {
        let n = fs::must_get_free_space(&self.path);
        // Divide free space by the max number of concurrent merges for file
        // parts.
        (n / FILE_PARTS_CONCURRENCY_CH.cap as u64).min(MAX_PART_SIZE)
    }

    fn inmemory_parts_merger(self: &Arc<Self>) {
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return;
            }
            let max_out_bytes = self.get_max_file_part_size();

            let pws = {
                let ps = self.parts.lock().unwrap();
                get_parts_to_merge(&ps.inmemory_parts, max_out_bytes)
            };

            if pws.is_empty() {
                // Nothing to merge
                return;
            }

            let permit = INMEMORY_PARTS_CONCURRENCY_CH.acquire();
            let res = self.merge_parts(pws, Some(&self.stop), false);
            drop(permit);

            match res {
                Ok(()) => {
                    // Try merging additional parts.
                    continue;
                }
                Err(MergeError::ForciblyStopped) => {
                    // Nothing to do - finish the merger.
                    return;
                }
                Err(err) => {
                    // Unexpected error.
                    panicf!(
                        "FATAL: unrecoverable error when merging inmemory parts in {:?}: {}",
                        self.path,
                        err
                    );
                }
            }
        }
    }

    fn file_parts_merger(self: &Arc<Self>) {
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return;
            }
            let max_out_bytes = self.get_max_file_part_size();

            let pws = {
                let ps = self.parts.lock().unwrap();
                get_parts_to_merge(&ps.file_parts, max_out_bytes)
            };

            if pws.is_empty() {
                // Nothing to merge
                return;
            }

            let permit = FILE_PARTS_CONCURRENCY_CH.acquire();
            let res = self.merge_parts(pws, Some(&self.stop), false);
            drop(permit);

            match res {
                Ok(()) => {
                    // Try merging additional parts.
                    continue;
                }
                Err(MergeError::ForciblyStopped) => {
                    // The merger has been stopped.
                    return;
                }
                Err(err) => {
                    // Unexpected error.
                    panicf!(
                        "FATAL: unrecoverable error when merging file parts in {:?}: {}",
                        self.path,
                        err
                    );
                }
            }
        }
    }

    fn release_parts_to_merge(&self, pws: &[Arc<PartWrapper>]) {
        let _ps = self.parts.lock().unwrap();
        for pw in pws {
            if !pw.is_in_merge.load(Ordering::SeqCst) {
                panicf!("BUG: missing isInMerge flag on the part {:?}", pw.p.path);
            }
            pw.is_in_merge.store(false, Ordering::SeqCst);
        }
    }

    /// Merges pws to a single resulting part (port of `Table.mergeParts`).
    ///
    /// It is expected that pws contains at least a single part.
    ///
    /// Merging is immediately stopped if stop_ch is set.
    ///
    /// If is_final is set, then the resulting part will be stored to disk.
    /// If at least a single source part at pws is stored on disk, then the
    /// resulting part will be stored to disk.
    ///
    /// All the parts inside pws must have is_in_merge set to true.
    /// It is set to false before returning from the function.
    fn merge_parts(
        self: &Arc<Self>,
        pws: Vec<Arc<PartWrapper>>,
        stop_ch: Option<&AtomicBool>,
        is_final: bool,
    ) -> Result<(), MergeError> {
        if pws.is_empty() {
            panicf!("BUG: empty pws cannot be passed to mergeParts()");
        }

        assert_is_in_merge(&pws);
        // Go: defer tb.releasePartsToMerge(pws)
        let _release_guard = ReleasePartsGuard {
            tb: self,
            pws: &pws,
        };

        let start_time = Instant::now();

        // Initialize destination paths.
        let dst_part_type = get_dst_part_type(&pws, is_final);
        let merge_idx = self.next_merge_idx();
        let mut dst_part_path = PathBuf::new();
        if dst_part_type == PartType::File {
            dst_part_path = self.path.join(format!("{merge_idx:016X}"));
        }

        if is_final && pws.len() == 1 && pws[0].mp.is_some() {
            // Fast path: flush a single in-memory part to disk.
            let mp = pws[0].mp.as_ref().unwrap();
            mp.must_store_to_disk(&dst_part_path);
            let pw_new = self.open_created_part(&pws, None, &dst_part_path);
            self.swap_src_with_dst_parts(&pws, pw_new, dst_part_type);
            return Ok(());
        }

        // Prepare BlockStreamReaders for source parts.
        let mut bsrs = must_open_block_stream_readers(&pws);

        // Prepare BlockStreamWriter for destination part.
        let mut src_size = 0u64;
        let mut src_items_count = 0u64;
        let mut src_blocks_count = 0u64;
        for pw in &pws {
            src_size += pw.p.size;
            src_items_count += pw.p.ph.items_count;
            src_blocks_count += pw.p.ph.blocks_count;
        }
        let compress_level = get_compress_level(src_items_count);
        let mut mp_new: Option<InmemoryPart> = None;
        let ph = {
            let mut bsw = BlockStreamWriter::default();
            if dst_part_type == PartType::Inmemory {
                mp_new = Some(InmemoryPart::default());
                bsw.must_init_from_inmemory_part(mp_new.as_mut().unwrap(), compress_level);
            } else {
                let nocache = src_items_count > max_items_per_cached_part();
                bsw.must_init_from_file_part(&dst_part_path, nocache, compress_level);
            }

            // Merge source parts to destination part.
            self.merge_parts_internal(
                &dst_part_path.to_string_lossy(),
                &mut bsw,
                &mut bsrs,
                dst_part_type,
                stop_ch,
            )
        };
        for bsr in &mut bsrs {
            bsr.must_close();
        }
        drop(bsrs);
        let ph = match ph {
            Ok(ph) => ph,
            Err(err) => {
                if matches!(err, MergeError::ForciblyStopped) && dst_part_type == PartType::File {
                    // Remove the incomplete destination part.
                    fs::must_remove_dir(&dst_part_path);
                }
                return Err(err);
            }
        };
        if let Some(mp_new) = &mut mp_new {
            // Update partHeader for destination inmemory part after the merge.
            mp_new.ph = ph;
        } else {
            // Make sure the created part directory listing is synced.
            fs::must_sync_path_and_parent_dir(&dst_part_path);
        }

        // Atomically swap the source parts with the newly created part.
        let pw_new = self.open_created_part(&pws, mp_new, &dst_part_path);
        let p_dst = &pw_new.p;
        let dst_items_count = p_dst.ph.items_count;
        let dst_blocks_count = p_dst.ph.blocks_count;
        let dst_size = p_dst.size;

        self.swap_src_with_dst_parts(&pws, pw_new, dst_part_type);

        let d = start_time.elapsed();
        if d <= Duration::from_secs(30) {
            return Ok(());
        }

        // Log stats for long merges.
        let duration_secs = d.as_secs_f64();
        let items_per_sec = (src_items_count as f64 / duration_secs) as i64;
        infof!(
            "merged ({} parts, {} items, {} blocks, {} bytes) into (1 part, {} items, {} blocks, {} bytes) in {:.3} seconds at {} items/sec to {:?}",
            pws.len(),
            src_items_count,
            src_blocks_count,
            src_size,
            dst_items_count,
            dst_blocks_count,
            dst_size,
            duration_secs,
            items_per_sec,
            dst_part_path
        );

        Ok(())
    }

    /// Port of `Table.mergePartsInternal`.
    fn merge_parts_internal(
        &self,
        dst_part_path: &str,
        bsw: &mut BlockStreamWriter<'_>,
        bsrs: &mut [BlockStreamReader<'_>],
        dst_part_type: PartType,
        stop_ch: Option<&AtomicBool>,
    ) -> Result<PartHeader, MergeError> {
        let (items_merged, merges_count, active_merges) = match dst_part_type {
            PartType::Inmemory => (
                &self.inmemory_items_merged,
                &self.inmemory_merges_count,
                &self.active_inmemory_merges,
            ),
            PartType::File => (
                &self.file_items_merged,
                &self.file_merges_count,
                &self.active_file_merges,
            ),
        };
        active_merges.fetch_add(1, Ordering::SeqCst);
        let mut ph = PartHeader::default();
        let result = merge_block_streams(
            &mut ph,
            bsw,
            bsrs,
            self.prepare_block,
            stop_ch,
            items_merged,
        );
        active_merges.fetch_add(-1, Ordering::SeqCst);
        merges_count.fetch_add(1, Ordering::SeqCst);
        match result {
            Ok(()) => {}
            Err(MergeError::ForciblyStopped) => return Err(MergeError::ForciblyStopped),
            Err(err) => {
                return Err(MergeError::Other(format!(
                    "cannot merge {} parts to {dst_part_path}: {err}",
                    bsrs.len()
                )));
            }
        }
        if !dst_part_path.is_empty() {
            ph.must_write_metadata(Path::new(dst_part_path));
        }
        Ok(ph)
    }

    /// Port of `Table.openCreatedPart`.
    fn open_created_part(
        &self,
        pws: &[Arc<PartWrapper>],
        mp_new: Option<InmemoryPart>,
        dst_part_path: &Path,
    ) -> Arc<PartWrapper> {
        match mp_new {
            Some(mp) => {
                // Open the created part from memory.
                let flush_to_disk_deadline = get_flush_to_disk_deadline(pws, self.flush_interval);
                new_part_wrapper_from_inmemory_part(Arc::new(mp), flush_to_disk_deadline)
            }
            None => {
                // Open the created part from disk.
                let p_new = must_open_file_part(dst_part_path);
                new_part_wrapper_from_file_part(p_new)
            }
        }
    }

    /// Port of `Table.swapSrcWithDstParts`.
    fn swap_src_with_dst_parts(
        self: &Arc<Self>,
        pws: &[Arc<PartWrapper>],
        pw_new: Arc<PartWrapper>,
        dst_part_type: PartType,
    ) {
        // Atomically unregister old parts and add new part to tb.
        let m = parts_to_map(pws);

        let removed_inmemory_parts;
        let removed_file_parts;

        {
            let mut ps = self.parts.lock().unwrap();

            removed_inmemory_parts = remove_parts(&mut ps.inmemory_parts, &m);
            removed_file_parts = remove_parts(&mut ps.file_parts, &m);
            match dst_part_type {
                PartType::Inmemory => {
                    ps.inmemory_parts.push(pw_new);
                    self.start_inmemory_parts_merger_locked(&ps);
                }
                PartType::File => {
                    ps.file_parts.push(pw_new);
                    self.start_file_parts_merger_locked(&ps);
                }
            }

            // Atomically store the updated list of file-based parts on disk.
            // This must be performed under partsLock in order to prevent from
            // races when multiple concurrently running goroutines update the
            // list.
            if removed_file_parts > 0 || dst_part_type == PartType::File {
                must_write_part_names(&ps.file_parts, &self.path);
            }
        }

        // Update the in-memory parts limit accordingly to the number of the
        // removed in-memory parts.
        for _ in 0..removed_inmemory_parts {
            self.inmemory_parts_limit.release_or_stop(&self.stop);
        }
        if dst_part_type == PartType::Inmemory {
            self.inmemory_parts_limit.acquire_or_stop(&self.stop);
        }

        let removed_parts = removed_inmemory_parts + removed_file_parts;
        if removed_parts != m.len() {
            panicf!(
                "BUG: unexpected number of parts removed; got {}, want {}",
                removed_parts,
                m.len()
            );
        }

        // Mark old parts as must be deleted and drop the table's references,
        // so they are eventually closed and deleted.
        for pw in pws {
            pw.must_drop.store(true, Ordering::SeqCst);
        }
    }

    fn next_merge_idx(&self) -> u64 {
        self.merge_idx.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Updates m with metrics from tb (port of `Table.UpdateMetrics`).
    ///
    /// PORT NOTE: the Go DataBlocksCache*/IndexBlocksCache* metrics have no
    /// source here, since the port omits the global block caches (see
    /// part.rs).
    pub fn update_metrics(&self, m: &mut TableMetrics) {
        m.active_inmemory_merges += self.active_inmemory_merges.load(Ordering::SeqCst) as u64;
        m.active_file_merges += self.active_file_merges.load(Ordering::SeqCst) as u64;

        m.inmemory_merges_count += self.inmemory_merges_count.load(Ordering::SeqCst);
        m.file_merges_count += self.file_merges_count.load(Ordering::SeqCst);

        m.inmemory_items_merged += self.inmemory_items_merged.load(Ordering::SeqCst);
        m.file_items_merged += self.file_items_merged.load(Ordering::SeqCst);

        m.items_added += self.items_added.load(Ordering::SeqCst);
        m.items_added_size_bytes += self.items_added_size_bytes.load(Ordering::SeqCst);

        m.inmemory_parts_limit_reached_count += self
            .inmemory_parts_limit_reached_count
            .load(Ordering::SeqCst);

        m.pending_items += self.raw_items.len() as u64;

        let ps = self.parts.lock().unwrap();

        m.inmemory_parts_count += ps.inmemory_parts.len() as u64;
        for pw in &ps.inmemory_parts {
            m.inmemory_blocks_count += pw.p.ph.blocks_count;
            m.inmemory_items_count += pw.p.ph.items_count;
            m.inmemory_size_bytes += pw.p.size;
            m.parts_ref_count += Arc::strong_count(pw) as u64;
        }

        m.file_parts_count += ps.file_parts.len() as u64;
        for pw in &ps.file_parts {
            m.file_blocks_count += pw.p.ph.blocks_count;
            m.file_items_count += pw.p.ph.items_count;
            m.file_size_bytes += pw.p.size;
            m.parts_ref_count += Arc::strong_count(pw) as u64;
        }
        drop(ps);

        m.too_long_items_dropped_total += TOO_LONG_ITEMS_TOTAL.load(Ordering::SeqCst);
    }

    /// Creates tb snapshot in the given dst_dir
    /// (port of `Table.MustCreateSnapshotAt`).
    ///
    /// Snapshot is created using linux hard links, so it is usually created
    /// very quickly.
    pub fn must_create_snapshot_at(self: &Arc<Self>, dst_dir: &str) {
        let src_dir = match std::path::absolute(&self.path) {
            Ok(dir) => dir,
            Err(err) => {
                panicf!(
                    "FATAL: cannot obtain absolute dir for {:?}: {}",
                    self.path,
                    err
                );
                unreachable!()
            }
        };
        let dst_dir = match std::path::absolute(dst_dir) {
            Ok(dir) => dir,
            Err(err) => {
                panicf!("FATAL: cannot obtain absolute dir for {dst_dir:?}: {}", err);
                unreachable!()
            }
        };
        if dst_dir.starts_with(&src_dir) {
            panicf!(
                "BUG: cannot create snapshot {:?} inside the data dir {:?}",
                dst_dir,
                src_dir
            );
        }

        // Flush inmemory items to disk.
        self.flush_inmemory_items_to_files();

        fs::must_mkdir_fail_if_exist(&dst_dir);

        let pws = self.get_parts();

        // Create a file with part names at dstDir
        must_write_part_names(&pws, &dst_dir);

        // Make hardlinks for pws at dstDir
        for pw in &pws {
            if pw.mp.is_some() {
                // Skip in-memory parts
                continue;
            }
            let src_part_path = &pw.p.path;
            let dst_part_path = dst_dir.join(src_part_path.file_name().unwrap());
            fs::must_hard_link_files(src_part_path, &dst_part_path);
        }

        fs::must_sync_path_and_parent_dir(&dst_dir);
    }

    /// Closes the table (port of `Table.MustClose`).
    ///
    /// This func must be called only when there are no goroutines using the
    /// table, such as ones that ingest or retrieve index data.
    pub fn must_close(self: &Arc<Self>) {
        // Notify background workers to stop.
        // The parts lock is acquired in order to guarantee that wg.add()
        // isn't called after stop is set and wg.wait() is called below.
        {
            let mut ps = self.parts.lock().unwrap();
            ps.stopped = true;
            self.stop.store(true, Ordering::SeqCst);
        }
        // Wake up the tickers and any limit waiters.
        {
            let _g = self.ticker_mu.lock().unwrap();
            self.ticker_cv.notify_all();
        }
        self.inmemory_parts_limit.notify_stop();

        // Wait for background workers to stop.
        self.wg.wait();

        // Flush the remaining in-memory items to files.
        self.flush_inmemory_items_to_files();

        // Remove references to parts from the tb, so they may be eventually
        // closed after all the searches are done.
        let mut ps = self.parts.lock().unwrap();

        let n = self.raw_items.len();
        if n > 0 {
            panicf!(
                "BUG: raw items must be empty at this stage; got {} items",
                n
            );
        }

        if !ps.inmemory_parts.is_empty() {
            panicf!(
                "BUG: in-memory parts must be empty at this stage; got {} parts",
                ps.inmemory_parts.len()
            );
        }
        ps.inmemory_parts = Vec::new();

        for pw in ps.file_parts.drain(..) {
            // Go checks that refCount becomes 0 here; with Arc the remaining
            // strong count must be 1 (this reference).
            if Arc::strong_count(&pw) != 1 {
                panicf!(
                    "BUG: unexpected non-zero references to a part when closing the table: {}",
                    Arc::strong_count(&pw) - 1
                );
            }
        }
    }
}

/// TableMetrics contains essential metrics for the Table
/// (port of `mergeset.TableMetrics`; the block-cache fields are omitted, see
/// `update_metrics`).
#[derive(Default)]
pub(crate) struct TableMetrics {
    pub active_inmemory_merges: u64,
    pub active_file_merges: u64,

    pub inmemory_merges_count: u64,
    pub file_merges_count: u64,

    pub inmemory_items_merged: u64,
    pub file_items_merged: u64,

    pub items_added: u64,
    pub items_added_size_bytes: u64,

    pub inmemory_parts_limit_reached_count: u64,

    pub pending_items: u64,

    pub inmemory_parts_count: u64,
    pub file_parts_count: u64,

    pub inmemory_blocks_count: u64,
    pub file_blocks_count: u64,

    pub inmemory_items_count: u64,
    pub file_items_count: u64,

    pub inmemory_size_bytes: u64,
    pub file_size_bytes: u64,

    pub parts_ref_count: u64,

    pub too_long_items_dropped_total: u64,
}

impl TableMetrics {
    /// Returns the total number of items in the table
    /// (port of `TableMetrics.TotalItemsCount`).
    pub fn total_items_count(&self) -> u64 {
        self.inmemory_items_count + self.file_items_count
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PartType {
    Inmemory,
    File,
}
// PORT NOTE: Go's partType is an int with panic on unknown values; the enum
// makes those branches unreachable.

/// Port of `getDstPartType`.
fn get_dst_part_type(pws: &[Arc<PartWrapper>], is_final: bool) -> PartType {
    let dst_part_size = get_parts_size(pws);
    if is_final || dst_part_size > get_max_inmemory_part_size() {
        return PartType::File;
    }
    if !are_all_inmemory_parts(pws) {
        // If at least a single source part is located in file,
        // then the destination part must be in file for durability reasons.
        return PartType::File;
    }
    PartType::Inmemory
}

fn get_max_inmemory_part_size() -> u64 {
    // Allow up to 5% of memory for in-memory parts.
    ((0.05 * memory::allowed() as f64 / MAX_INMEMORY_PARTS as f64) as u64).max(1_000_000)
}

fn are_all_inmemory_parts(pws: &[Arc<PartWrapper>]) -> bool {
    pws.iter().all(|pw| pw.mp.is_some())
}

/// Port of `mustOpenBlockStreamReaders`.
fn must_open_block_stream_readers(pws: &[Arc<PartWrapper>]) -> Vec<BlockStreamReader<'_>> {
    let mut bsrs = Vec::with_capacity(pws.len());
    for pw in pws {
        let mut bsr = BlockStreamReader::default();
        match &pw.mp {
            Some(mp) => bsr.must_init_from_inmemory_part(mp),
            None => bsr.must_init_from_file_part(&pw.p.path),
        }
        bsrs.push(bsr);
    }
    bsrs
}

/// Port of `getFlushToDiskDeadline`.
fn get_flush_to_disk_deadline(pws: &[Arc<PartWrapper>], flush_interval: Duration) -> Instant {
    let mut d = Instant::now() + flush_interval;
    for pw in pws {
        if pw.mp.is_some()
            && let Some(fd) = pw.flush_to_disk_deadline
            && fd < d
        {
            d = fd;
        }
    }
    d
}

/// Port of `getCompressLevel`.
fn get_compress_level(items_count: u64) -> i32 {
    if items_count <= 1 << 16 {
        // -5 is the minimum supported compression for zstd.
        // See https://github.com/facebook/zstd/releases/tag/v1.3.4
        return -5;
    }
    if items_count <= 1 << 17 {
        return -4;
    }
    if items_count <= 1 << 18 {
        return -3;
    }
    if items_count <= 1 << 19 {
        return -2;
    }
    if items_count <= 1 << 20 {
        return -1;
    }
    if items_count <= 1 << 22 {
        return 1;
    }
    if items_count <= 1 << 25 {
        return 2;
    }
    3
}

fn assert_is_in_merge(pws: &[Arc<PartWrapper>]) {
    for pw in pws {
        if !pw.is_in_merge.load(Ordering::SeqCst) {
            panicf!("BUG: partWrapper.isInMerge unexpectedly set to false");
        }
    }
}

/// Port of `mustOpenParts`.
fn must_open_parts(path: &Path) -> Vec<Arc<PartWrapper>> {
    // Remove txn and tmp directories, which may be left after the upgrade
    // to v1.90.0 and newer versions.
    fs::must_remove_dir(path.join("txn"));
    fs::must_remove_dir(path.join("tmp"));

    let parts_file = path.join(PARTS_FILENAME);
    let part_names = must_read_part_names(&parts_file, path);

    // Remove dirs missing in partNames. These dirs may be left after unclean
    // shutdown or after the update from versions prior to v1.90.0.
    let des = fs::must_read_dir(path);
    let m: std::collections::HashSet<&str> = part_names.iter().map(|s| s.as_str()).collect();
    for part_name in &part_names {
        // Make sure the partName exists on disk.
        // If it is missing, then manual action from the user is needed, since
        // this is unexpected state, which cannot occur under normal
        // operation, including unclean shutdown.
        let part_path = path.join(part_name);
        if !fs::is_path_exist(&part_path) {
            panicf!(
                "FATAL: part {:?} is listed in {:?}, but is missing on disk; ensure {:?} contents is not corrupted; remove {:?} from {:?} in order to restore access to the remaining data",
                part_path,
                parts_file,
                parts_file,
                part_path,
                parts_file
            );
        }
    }
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
                "deleting {:?} because it isn't listed in {:?}; this is the expected case after unclean shutdown",
                delete_path,
                parts_file
            );
            fs::must_remove_dir(&delete_path);
        }
    }

    // Open parts
    let mut pws = Vec::with_capacity(part_names.len());
    for part_name in &part_names {
        let part_path = path.join(part_name);
        let p = must_open_file_part(&part_path);
        pws.push(new_part_wrapper_from_file_part(p));
    }
    if !fs::is_path_exist(&parts_file) {
        // Create parts.json file if it doesn't exist yet.
        // This should protect from possible crashloops just after the
        // migration from versions below v1.90.0.
        must_write_part_names(&pws, path);
    }

    pws
}

/// Port of `mustWritePartNames`.
fn must_write_part_names(pws: &[Arc<PartWrapper>], dst_dir: &Path) {
    let mut part_names: Vec<String> = Vec::with_capacity(pws.len());
    for pw in pws {
        if pw.mp.is_some() {
            // Skip in-memory parts
            continue;
        }
        let part_name =
            pw.p.path
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
        part_names.push(part_name);
    }
    part_names.sort_unstable();
    let data = marshal_part_names_json(&part_names);
    let parts_file = dst_dir.join(PARTS_FILENAME);
    fs::must_write_atomic(&parts_file, &data, true);
}

/// Port of `mustReadPartNames`.
fn must_read_part_names(parts_file: &Path, src_dir: &Path) -> Vec<String> {
    if fs::is_path_exist(parts_file) {
        let data = match std::fs::read(parts_file) {
            Ok(data) => data,
            Err(err) => {
                panicf!("FATAL: cannot read {:?}: {}", parts_file, err);
                unreachable!()
            }
        };
        match unmarshal_part_names_json(&data) {
            Ok(part_names) => return part_names,
            Err(err) => {
                panicf!("FATAL: cannot parse {:?}: {}", parts_file, err);
                unreachable!()
            }
        }
    }
    // The parts.json is missing. This is the upgrade from versions previous
    // to v1.90.0. Read part names from directories under srcDir.
    let des = fs::must_read_dir(src_dir);
    let mut part_names = Vec::new();
    for de in &des {
        if !fs::is_dir_or_symlink(de) {
            // Skip non-directories.
            continue;
        }
        let part_name = de.file_name().to_string_lossy().into_owned();
        if is_special_dir(&part_name) {
            // Skip special dirs.
            continue;
        }
        part_names.push(part_name);
    }
    part_names
}

// PORT NOTE: Go uses encoding/json for parts.json (a JSON array of strings);
// the port hand-rolls the minimal codec instead of adding a JSON dependency
// (part names are hex directory names, so no escaping is needed on the write
// path; the read path still handles standard escapes).

fn marshal_part_names_json(part_names: &[String]) -> Vec<u8> {
    let mut data = Vec::with_capacity(2 + part_names.len() * 20);
    data.push(b'[');
    for (i, name) in part_names.iter().enumerate() {
        if i > 0 {
            data.push(b',');
        }
        data.push(b'"');
        data.extend_from_slice(name.as_bytes());
        data.push(b'"');
    }
    data.push(b']');
    data
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
    for item in s.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let item = item
            .strip_prefix('"')
            .and_then(|it| it.strip_suffix('"'))
            .ok_or_else(|| format!("expected JSON string; got {item:?}"))?;
        if item.contains('\\') {
            return Err(format!("unsupported escape sequence in part name {item:?}"));
        }
        part_names.push(item.to_string());
    }
    Ok(part_names)
}

/// Port of `isSpecialDir`.
fn is_special_dir(name: &str) -> bool {
    // Snapshots and cache dirs aren't used anymore.
    // Keep them here for backwards compatibility.
    name == "tmp" || name == "txn" || name == "snapshots" || name == "cache"
}

/// Port of `getPartsToMerge`: returns optimal parts to merge from pws.
///
/// The summary size of the returned parts must be smaller than maxOutBytes.
fn get_parts_to_merge(pws: &[Arc<PartWrapper>], max_out_bytes: u64) -> Vec<Arc<PartWrapper>> {
    let mut pws_remaining: Vec<Arc<PartWrapper>> = Vec::with_capacity(pws.len());
    for pw in pws {
        if !pw.is_in_merge.load(Ordering::SeqCst) {
            pws_remaining.push(Arc::clone(pw));
        }
    }

    let pws_to_merge = append_parts_to_merge(
        Vec::new(),
        &pws_remaining,
        DEFAULT_PARTS_TO_MERGE,
        max_out_bytes,
    );

    for pw in &pws_to_merge {
        if pw.is_in_merge.swap(true, Ordering::SeqCst) {
            panicf!("BUG: partWrapper.isInMerge unexpectedly set to true");
        }
    }

    pws_to_merge
}

/// Port of `getPartsForOptimalMerge`: returns parts from pws for optimal
/// merge, plus the remaining parts.
fn get_parts_for_optimal_merge(
    pws: Vec<Arc<PartWrapper>>,
) -> (Vec<Arc<PartWrapper>>, Vec<Arc<PartWrapper>>) {
    let pws_to_merge = append_parts_to_merge(Vec::new(), &pws, DEFAULT_PARTS_TO_MERGE, u64::MAX);
    if pws_to_merge.is_empty() {
        return (pws, Vec::new());
    }

    let m = parts_to_map(&pws_to_merge);
    let mut pws_remaining: Vec<Arc<PartWrapper>> =
        Vec::with_capacity(pws.len() - pws_to_merge.len());
    for pw in &pws {
        if !m.contains(&Arc::as_ptr(pw).cast()) {
            pws_remaining.push(Arc::clone(pw));
        }
    }

    (pws_to_merge, pws_remaining)
}

/// Port of `appendPartsToMerge`: finds optimal parts to merge from src,
/// appends them to dst and returns the result.
fn append_parts_to_merge(
    mut dst: Vec<Arc<PartWrapper>>,
    src: &[Arc<PartWrapper>],
    max_parts_to_merge: usize,
    max_out_bytes: u64,
) -> Vec<Arc<PartWrapper>> {
    if src.len() < 2 {
        // There is no need in merging zero or one part :)
        return dst;
    }
    if max_parts_to_merge < 2 {
        panicf!(
            "BUG: maxPartsToMerge cannot be smaller than 2; got {}",
            max_parts_to_merge
        );
    }

    // Filter out too big parts.
    // This should reduce N for O(n^2) algorithm below.
    let max_in_part_bytes = (max_out_bytes as f64 / MIN_MERGE_MULTIPLIER) as u64;
    let mut src: Vec<Arc<PartWrapper>> = src
        .iter()
        .filter(|pw| pw.p.size <= max_in_part_bytes)
        .cloned()
        .collect();

    sort_parts_for_optimal_merge(&mut src);

    let max_src_parts = max_parts_to_merge.min(src.len());
    let min_src_parts = max_src_parts.div_ceil(2).max(2);

    // Exhaustive search for parts giving the lowest write amplification when
    // merged.
    let mut pws: Option<&[Arc<PartWrapper>]> = None;
    let mut max_m = 0f64;
    for i in min_src_parts..=max_src_parts {
        for j in 0..(src.len() - i + 1) {
            let a = &src[j..j + i];
            if a[0].p.size * (a.len() as u64) < a[a.len() - 1].p.size {
                // Do not merge parts with too big difference in size,
                // since this results in unbalanced merges.
                continue;
            }
            let out_bytes = get_parts_size(a);
            if out_bytes > max_out_bytes {
                // There is no sense in checking the remaining bigger parts.
                break;
            }
            let m = out_bytes as f64 / a[a.len() - 1].p.size as f64;
            if m < max_m {
                continue;
            }
            max_m = m;
            pws = Some(a);
        }
    }

    let mut min_m = max_parts_to_merge as f64 / 2.0;
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
    // Sort src parts by size.
    pws.sort_unstable_by_key(|pw| pw.p.size);
}

fn parts_to_map(pws: &[Arc<PartWrapper>]) -> std::collections::HashSet<*const PartWrapper> {
    let mut m: std::collections::HashSet<*const PartWrapper> =
        std::collections::HashSet::with_capacity(pws.len());
    for pw in pws {
        m.insert(Arc::as_ptr(pw));
    }
    if m.len() != pws.len() {
        panicf!(
            "BUG: {} duplicate parts found in {} source parts",
            pws.len() - m.len(),
            pws.len()
        );
    }
    m
}

fn remove_parts(
    pws: &mut Vec<Arc<PartWrapper>>,
    parts_to_remove: &std::collections::HashSet<*const PartWrapper>,
) -> usize {
    let n = pws.len();
    pws.retain(|pw| !parts_to_remove.contains(&Arc::as_ptr(pw)));
    n - pws.len()
}

fn get_parts_size(pws: &[Arc<PartWrapper>]) -> u64 {
    pws.iter().map(|pw| pw.p.size).sum()
}

/// PORT NOTE: minimal Go sync.WaitGroup equivalent (duplicated from
/// datadb.rs, which keeps it private).
pub(super) struct WaitGroup {
    count: Mutex<u64>,
    cv: Condvar,
}

impl WaitGroup {
    pub fn new() -> WaitGroup {
        WaitGroup {
            count: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    pub fn add(&self, n: u64) {
        let mut count = self.count.lock().unwrap();
        *count += n;
    }

    pub fn done(&self) {
        let mut count = self.count.lock().unwrap();
        if *count == 0 {
            panicf!("BUG: WaitGroup counter must be positive on done()");
        }
        *count -= 1;
        if *count == 0 {
            self.cv.notify_all();
        }
    }

    pub fn wait(&self) {
        let mut count = self.count.lock().unwrap();
        while *count > 0 {
            count = self.cv.wait(count).unwrap();
        }
    }
}

/// PORT NOTE: stands in for Go's buffered channels used as counting
/// semaphores (duplicated from datadb.rs, which keeps it private).
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

/// PORT NOTE: stands in for Go's `inmemoryPartsLimitCh` buffered channel plus
/// the `select` clauses on `stopCh`: acquiring blocks while the limit is
/// reached (unless the table is being closed), releasing blocks while no
/// token is held (unless the table is being closed).
struct InmemoryPartsLimit {
    cap: usize,
    used: Mutex<usize>,
    cv: Condvar,
}

impl InmemoryPartsLimit {
    fn new(cap: usize) -> InmemoryPartsLimit {
        InmemoryPartsLimit {
            cap,
            used: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    /// Go: `select { case ch <- struct{}{}: default: }`.
    fn try_acquire(&self) -> bool {
        let mut used = self.used.lock().unwrap();
        if *used < self.cap {
            *used += 1;
            return true;
        }
        false
    }

    /// Go: `select { case ch <- struct{}{}: case <-tb.stopCh: }`.
    fn acquire_or_stop(&self, stop: &AtomicBool) {
        let mut used = self.used.lock().unwrap();
        loop {
            if stop.load(Ordering::SeqCst) {
                return;
            }
            if *used < self.cap {
                *used += 1;
                return;
            }
            used = self.cv.wait(used).unwrap();
        }
    }

    /// Go: `select { case <-ch: case <-tb.stopCh: }`.
    fn release_or_stop(&self, stop: &AtomicBool) {
        let mut used = self.used.lock().unwrap();
        loop {
            if *used > 0 {
                *used -= 1;
                self.cv.notify_all();
                return;
            }
            if stop.load(Ordering::SeqCst) {
                return;
            }
            used = self.cv.wait(used).unwrap();
        }
    }

    fn notify_stop(&self) {
        let _used = self.used.lock().unwrap();
        self.cv.notify_all();
    }
}

/// Go: `defer tb.releasePartsToMerge(pws)`.
struct ReleasePartsGuard<'a> {
    tb: &'a Table,
    pws: &'a [Arc<PartWrapper>],
}

impl Drop for ReleasePartsGuard<'_> {
    fn drop(&mut self) {
        self.tb.release_parts_to_merge(self.pws);
    }
}

//! Minimal internal index store used by [`crate::indexdb`].
//!
//! # Mergeset decision (PORT NOTE)
//!
//! `lib/logstorage/indexdb.go` stores its rows in Softalink LLC'
//! `lib/mergeset.Table` — a full LSM index engine (~4500 LOC across
//! block_header, block_stream_{reader,writer}, encoding, inmemory_part,
//! merge, metaindex_row, part, part_header, part_search, table,
//! table_search). indexdb touches the entire Table surface:
//!
//!   - `MustOpenTable`, `MustClose`, `DebugFlush`, `AddItems`,
//!     `UpdateMetrics`, `MustCreateSnapshotAt`;
//!   - `TableSearch` with `Init`, `Seek`, `FirstItemWithPrefix`, `NextItem`,
//!     `Item`, `Error`, `MustClose`;
//!   - `Item{Start,End}` with `Bytes`/`String`;
//!   - the `PrepareBlockCallback` merge hook (`mergeTagToStreamIDsRows`).
//!
//! `lib/mergeset` is NOT ported and is out of scope here. This module is the
//! **API-compatible internal index store** option: it preserves every public
//! `indexdb` behavior and — crucially — the *item byte encoding* itself
//! (nsPrefix + tenantID + tag/streamID), which is defined in `indexdb.go`, not
//! in mergeset. Only mergeset's underlying sorted-items engine is replaced,
//! with a simpler sorted in-memory store persisted to a single
//! length-prefixed `items.bin` file.
//!
//! Rationale: porting mergeset's on-disk LSM format (block compression,
//! metaindex, part search heaps, background merges, inmemory parts) is
//! disproportionate to this task, and none of it changes the query semantics
//! indexdb relies on — which come from the item key bytes and the merge hook,
//! both of which are preserved exactly.
//!
//! Data-compat implication: the `indexdb/` directory written by this store is
//! NOT byte-compatible with upstream EsLogs/mergeset parts. An indexdb
//! dir produced by upstream cannot be read by this port and vice-versa. Every
//! other on-disk/wire format (streamID, stream tags canonical, tag encoding,
//! filterStream cache keys) is identical to upstream.

use std::path::Path;
use std::sync::{Arc, Mutex};

use esl_common::fs;

/// Item is an item stored in the store, addressed as a `[start, end)` range
/// into a shared data buffer (port of `mergeset.Item`).
#[derive(Clone, Copy)]
pub(crate) struct Item {
    pub start: u32,
    pub end: u32,
}

impl Item {
    /// Returns bytes for the given item from data (port of `Item.Bytes`).
    pub fn bytes<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        &data[self.start as usize..self.end as usize]
    }
}

/// PrepareBlockCallback prepares a block of items before it is stored.
///
/// PORT NOTE: Go passes `func(data []byte, items []Item) ([]byte, []Item)`.
/// Since indexdb's callback (`mergeTagToStreamIDsRows`) captures no state, a
/// plain `fn` pointer is used here.
pub(crate) type PrepareBlockCallback = fn(Vec<u8>, Vec<Item>) -> (Vec<u8>, Vec<Item>);

/// SearchError mirrors the two outcomes indexdb distinguishes from mergeset:
/// `io.EOF` (no item) and any other error.
pub(crate) enum SearchError {
    /// No matching item exists (Go `io.EOF`).
    Eof,
    /// Any other, unexpected error.
    #[allow(dead_code)]
    Other(String),
}

struct Inner {
    /// Sorted, flushed items shared with active [`TableSearch`] snapshots.
    items: Arc<Vec<Vec<u8>>>,

    /// Pending items added since the last flush.
    pending: Vec<Vec<u8>>,
}

/// Table is the internal index store (API-compatible replacement for
/// `mergeset.Table`).
pub(crate) struct Table {
    path: String,
    prepare_block: Option<PrepareBlockCallback>,
    // PORT NOTE: Go calls flushCallback periodically from a background
    // goroutine and on each flush; this store has no background timer, so the
    // callback fires synchronously whenever pending items are flushed.
    flush_callback: Option<Box<dyn Fn() + Send + Sync>>,
    // PORT NOTE: Go's flushInterval/flushCallbackInterval/isReadOnly drive
    // background merges; retained here only for signature fidelity.
    flush_interval: i64,
    inner: Mutex<Inner>,
}

impl Table {
    /// Opens the store at path, loading any persisted items
    /// (port of `mergeset.MustOpenTable`).
    pub fn must_open(
        path: &str,
        flush_interval: i64,
        flush_callback: Option<Box<dyn Fn() + Send + Sync>>,
        prepare_block: Option<PrepareBlockCallback>,
    ) -> Arc<Table> {
        fs::must_mkdir_if_not_exist(path);
        let items = load_items(path);
        fs::must_sync_path_and_parent_dir(path);
        Arc::new(Table {
            path: path.to_string(),
            prepare_block,
            flush_callback,
            flush_interval,
            inner: Mutex::new(Inner {
                items: Arc::new(items),
                pending: Vec::new(),
            }),
        })
    }

    /// Returns the configured flush interval in nanoseconds.
    pub fn flush_interval(&self) -> i64 {
        self.flush_interval
    }

    /// Adds items to the store (port of `Table.AddItems`).
    pub fn add_items(&self, items: &[Vec<u8>]) {
        let mut g = self.inner.lock().unwrap();
        g.pending.reserve(items.len());
        for it in items {
            g.pending.push(it.clone());
        }
    }

    /// Flushes pending items to disk (port of `Table.DebugFlush`).
    pub fn debug_flush(&self) {
        if self.flush() {
            self.persist();
            self.fire_flush_callback();
        }
    }

    /// Returns a consistent snapshot of the sorted items, flushing pending
    /// items first so they become visible to search (Go makes raw items
    /// visible only after they are converted to parts; the test calls
    /// `debugFlush` before searching — this keeps searches correct even
    /// without an explicit flush).
    pub fn snapshot(&self) -> Arc<Vec<Vec<u8>>> {
        let flushed = self.flush();
        let items = Arc::clone(&self.inner.lock().unwrap().items);
        if flushed {
            self.persist();
            self.fire_flush_callback();
        }
        items
    }

    /// Fills metrics from the current store contents (port of
    /// `Table.UpdateMetrics`; only the fields indexdb reads are populated).
    pub fn update_metrics(&self, m: &mut TableMetrics) {
        let g = self.inner.lock().unwrap();
        let items = &g.items;
        m.file_items_count += items.len() as u64;
        m.file_size_bytes += items.iter().map(|it| it.len() as u64).sum::<u64>();
        m.pending_items += g.pending.len() as u64;
        if !items.is_empty() {
            m.file_parts_count += 1;
            m.file_blocks_count += 1;
        }
    }

    /// Flushes and copies the persisted items file into dst_dir
    /// (port of `Table.MustCreateSnapshotAt`).
    pub fn must_create_snapshot_at(&self, dst_dir: &str) {
        if self.flush() {
            self.fire_flush_callback();
        }
        self.persist();
        fs::must_mkdir_fail_if_exist(dst_dir);
        let src = Path::new(&self.path).join(ITEMS_FILENAME);
        if fs::is_path_exist(&src) {
            let data = std::fs::read(&src).expect("FATAL: cannot read indexdb items for snapshot");
            fs::must_write_sync(Path::new(dst_dir).join(ITEMS_FILENAME), &data);
        }
        fs::must_sync_path_and_parent_dir(dst_dir);
    }

    /// Flushes pending items and persists the store (port of
    /// `Table.MustClose`).
    pub fn must_close(&self) {
        self.flush();
        self.persist();
    }

    /// Merges pending items into the sorted set, applying the prepare-block
    /// callback. Returns true if a flush happened.
    fn flush(&self) -> bool {
        let mut g = self.inner.lock().unwrap();
        if g.pending.is_empty() {
            return false;
        }
        let mut all: Vec<Vec<u8>> = (*g.items).clone();
        all.append(&mut g.pending);
        all.sort();

        let (data, ranges) = build_block(&all);
        let (data, ranges) = match self.prepare_block {
            Some(cb) => cb(data, ranges),
            None => (data, ranges),
        };
        let new_items: Vec<Vec<u8>> = ranges
            .iter()
            .map(|r| data[r.start as usize..r.end as usize].to_vec())
            .collect();
        g.items = Arc::new(new_items);
        true
    }

    fn fire_flush_callback(&self) {
        if let Some(cb) = &self.flush_callback {
            cb();
        }
    }

    fn persist(&self) {
        let items = Arc::clone(&self.inner.lock().unwrap().items);
        let mut buf = Vec::new();
        for it in items.iter() {
            buf.extend_from_slice(&(it.len() as u32).to_le_bytes());
            buf.extend_from_slice(it);
        }
        fs::must_write_sync(Path::new(&self.path).join(ITEMS_FILENAME), &buf);
    }
}

/// TableMetrics contains the subset of `mergeset.TableMetrics` read by indexdb.
#[derive(Default)]
pub(crate) struct TableMetrics {
    pub inmemory_size_bytes: u64,
    pub file_size_bytes: u64,
    pub inmemory_items_count: u64,
    pub file_items_count: u64,
    pub pending_items: u64,
    pub inmemory_parts_count: u64,
    pub file_parts_count: u64,
    pub inmemory_blocks_count: u64,
    pub file_blocks_count: u64,
    pub active_file_merges: u64,
    pub active_inmemory_merges: u64,
    pub file_merges_count: u64,
    pub inmemory_merges_count: u64,
    pub file_items_merged: u64,
    pub inmemory_items_merged: u64,
}

/// TableSearch is a reusable cursor over a [`Table`] snapshot
/// (port of `mergeset.TableSearch`).
#[derive(Default)]
pub(crate) struct TableSearch {
    items: Arc<Vec<Vec<u8>>>,
    idx: usize,
    reached_end: bool,
    next_item_noop: bool,
    need_closing: bool,
}

impl TableSearch {
    /// Initializes the cursor for searching in tb (port of `TableSearch.Init`).
    pub fn init(&mut self, tb: &Arc<Table>) {
        if self.need_closing {
            esl_common::panicf!("BUG: missing MustClose call before the next call to Init");
        }
        self.items = tb.snapshot();
        self.idx = 0;
        self.reached_end = false;
        self.next_item_noop = false;
        self.need_closing = true;
    }

    /// Closes the cursor (port of `TableSearch.MustClose`).
    pub fn must_close(&mut self) {
        self.items = Arc::new(Vec::new());
        self.idx = 0;
        self.reached_end = false;
        self.next_item_noop = false;
        self.need_closing = false;
    }

    /// Seeks to the first item greater or equal to k (port of `TableSearch.Seek`).
    pub fn seek(&mut self, k: &[u8]) {
        let idx = self.items.partition_point(|x| x.as_slice() < k);
        if idx >= self.items.len() {
            self.reached_end = true;
            self.next_item_noop = false;
            return;
        }
        self.idx = idx;
        self.reached_end = false;
        self.next_item_noop = true;
    }

    /// Seeks to the first item with the given prefix (port of
    /// `TableSearch.FirstItemWithPrefix`). Returns [`SearchError::Eof`] when no
    /// such item exists.
    pub fn first_item_with_prefix(&mut self, prefix: &[u8]) -> Result<(), SearchError> {
        self.seek(prefix);
        if !self.next_item() {
            return Err(SearchError::Eof);
        }
        if !self.item().starts_with(prefix) {
            return Err(SearchError::Eof);
        }
        Ok(())
    }

    /// Advances to the next item (port of `TableSearch.NextItem`).
    pub fn next_item(&mut self) -> bool {
        if self.reached_end {
            return false;
        }
        if self.next_item_noop {
            self.next_item_noop = false;
            return true;
        }
        if self.idx + 1 >= self.items.len() {
            self.reached_end = true;
            return false;
        }
        self.idx += 1;
        true
    }

    /// Returns the current item (port of the `TableSearch.Item` field).
    pub fn item(&self) -> &[u8] {
        &self.items[self.idx]
    }

    /// Returns the last error (port of `TableSearch.Error`).
    ///
    /// PORT NOTE: this store performs no IO during iteration, so — like Go,
    /// which maps `io.EOF` to nil — this always returns `None`.
    pub fn error(&self) -> Option<String> {
        None
    }
}

const ITEMS_FILENAME: &str = "items.bin";

fn build_block(items: &[Vec<u8>]) -> (Vec<u8>, Vec<Item>) {
    let total: usize = items.iter().map(|it| it.len()).sum();
    let mut data = Vec::with_capacity(total);
    let mut ranges = Vec::with_capacity(items.len());
    for it in items {
        let start = data.len() as u32;
        data.extend_from_slice(it);
        ranges.push(Item {
            start,
            end: data.len() as u32,
        });
    }
    (data, ranges)
}

fn load_items(path: &str) -> Vec<Vec<u8>> {
    let file = Path::new(path).join(ITEMS_FILENAME);
    if !fs::is_path_exist(&file) {
        return Vec::new();
    }
    let data = std::fs::read(&file).expect("FATAL: cannot read indexdb items file");
    let mut items = Vec::new();
    let mut i = 0;
    while i + 4 <= data.len() {
        let len = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as usize;
        i += 4;
        if i + len > data.len() {
            esl_common::panicf!("FATAL: corrupted indexdb items file {file:?}");
        }
        items.push(data[i..i + len].to_vec());
        i += len;
    }
    items
}

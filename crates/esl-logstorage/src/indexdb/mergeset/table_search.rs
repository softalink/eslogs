//! Port of `lib/mergeset/table_search.go`.

use std::sync::Arc;

use super::SearchError;
use super::part_search::PartSearch;
use super::table::{PartWrapper, Table};

enum TsError {
    Eof,
    Other(String),
}

/// TableSearch is a reusable cursor used for searching in the Table
/// (port of `mergeset.TableSearch`).
///
/// PORT NOTE: Go's psHeap holds `*partSearch`; the port keeps the part
/// searches in ps_pool and stores indices into it in the heap.
#[derive(Default)]
pub(crate) struct TableSearch {
    pws: Vec<Arc<PartWrapper>>,

    ps_pool: Vec<PartSearch>,
    ps_heap: Vec<usize>,

    err: Option<TsError>,

    next_item_noop: bool,
    need_closing: bool,
}

impl TableSearch {
    fn reset(&mut self) {
        self.pws.clear();

        for ps in &mut self.ps_pool {
            ps.reset();
        }
        self.ps_pool.clear();
        self.ps_heap.clear();

        self.err = None;

        self.next_item_noop = false;
        self.need_closing = false;
    }

    /// Initializes ts for searching in the tb (port of `TableSearch.Init`).
    ///
    /// must_close() must be called when the ts is no longer needed.
    pub fn init(&mut self, tb: &Arc<Table>, sparse: bool) {
        if self.need_closing {
            esl_common::panicf!("BUG: missing MustClose call before the next call to Init");
        }

        self.reset();

        self.need_closing = true;

        self.pws = tb.get_parts();

        // Initialize the ps_pool.
        self.ps_pool.clear();
        self.ps_pool
            .resize_with(self.pws.len(), PartSearch::default);
        for (i, pw) in self.pws.iter().enumerate() {
            self.ps_pool[i].init(Arc::clone(pw), sparse);
        }
    }

    /// Returns the current item (Go: the `ts.Item` field).
    ///
    /// The item contents breaks after the next call to next_item().
    pub fn item(&self) -> &[u8] {
        self.ps_pool[self.ps_heap[0]].item()
    }

    /// Seeks for the first item greater or equal to k in the ts
    /// (port of `TableSearch.Seek`).
    pub fn seek(&mut self, k: &[u8]) {
        if matches!(self.err, Some(TsError::Other(_))) {
            // Do nothing on unrecoverable error.
            return;
        }
        self.err = None;

        // Initialize the ps_heap.
        self.ps_heap.clear();
        for i in 0..self.ps_pool.len() {
            let ps = &mut self.ps_pool[i];
            ps.seek(k);
            if !ps.next_item() {
                if let Some(err) = ps.error() {
                    // Return only the first error, since it has no sense in
                    // returning all errors.
                    self.err = Some(TsError::Other(format!("cannot seek {k:X?}: {err}")));
                    return;
                }
                continue;
            }
            self.ps_heap.push(i);
        }
        if self.ps_heap.is_empty() {
            self.err = Some(TsError::Eof);
            return;
        }
        heap_init(&mut self.ps_heap, &self.ps_pool);
        self.next_item_noop = true;
    }

    /// Seeks for the first item with the given prefix in the ts
    /// (port of `TableSearch.FirstItemWithPrefix`).
    ///
    /// It returns [`SearchError::Eof`] if such an item doesn't exist.
    pub fn first_item_with_prefix(&mut self, prefix: &[u8]) -> Result<(), SearchError> {
        self.seek(prefix);
        if !self.next_item() {
            if let Some(err) = self.error() {
                return Err(SearchError::Other(err));
            }
            return Err(SearchError::Eof);
        }
        if let Some(err) = self.error() {
            return Err(SearchError::Other(err));
        }
        if !self.item().starts_with(prefix) {
            return Err(SearchError::Eof);
        }
        Ok(())
    }

    /// Advances to the next item (port of `TableSearch.NextItem`).
    pub fn next_item(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }
        if self.next_item_noop {
            self.next_item_noop = false;
            return true;
        }

        if let Err(err) = self.next_block() {
            let err = match err {
                TsError::Eof => TsError::Eof,
                TsError::Other(err) => TsError::Other(format!(
                    "cannot obtain the next block to search in the table: {err}"
                )),
            };
            self.err = Some(err);
            return false;
        }
        true
    }

    /// Port of `TableSearch.nextBlock`.
    fn next_block(&mut self) -> Result<(), TsError> {
        let ps_min_idx = self.ps_heap[0];
        let ps_min = &mut self.ps_pool[ps_min_idx];
        if ps_min.next_item() {
            heap_fix_root(&mut self.ps_heap, &self.ps_pool);
            return Ok(());
        }

        if let Some(err) = self.ps_pool[ps_min_idx].error() {
            return Err(TsError::Other(err));
        }

        heap_pop(&mut self.ps_heap, &self.ps_pool);

        if self.ps_heap.is_empty() {
            return Err(TsError::Eof);
        }

        Ok(())
    }

    /// Returns the last error in ts (port of `TableSearch.Error`; io.EOF maps
    /// to None).
    pub fn error(&self) -> Option<String> {
        match &self.err {
            None | Some(TsError::Eof) => None,
            Some(TsError::Other(err)) => Some(err.clone()),
        }
    }

    /// Closes the ts (port of `TableSearch.MustClose`).
    pub fn must_close(&mut self) {
        if !self.need_closing {
            esl_common::panicf!("BUG: missing Init call before MustClose call");
        }
        // Go returns the parts via tb.putParts(ts.pws); dropping the Arcs in
        // reset() does the same.
        self.reset();
    }
}

// The psHeap (Go: `container/heap` over `partSearchHeap`, ordered by Item).

fn heap_less(heap: &[usize], pool: &[PartSearch], i: usize, j: usize) -> bool {
    pool[heap[i]].item() < pool[heap[j]].item()
}

fn heap_init(heap: &mut [usize], pool: &[PartSearch]) {
    let n = heap.len();
    for i in (0..n / 2).rev() {
        sift_down(heap, pool, i);
    }
}

fn heap_fix_root(heap: &mut [usize], pool: &[PartSearch]) {
    sift_down(heap, pool, 0);
}

fn heap_pop(heap: &mut Vec<usize>, pool: &[PartSearch]) -> usize {
    let n = heap.len() - 1;
    heap.swap(0, n);
    let v = heap.pop().unwrap();
    if n > 0 {
        sift_down(heap, pool, 0);
    }
    v
}

fn sift_down(heap: &mut [usize], pool: &[PartSearch], mut i: usize) {
    let n = heap.len();
    loop {
        let j1 = 2 * i + 1;
        if j1 >= n {
            break;
        }
        let mut j = j1;
        let j2 = j1 + 1;
        if j2 < n && heap_less(heap, pool, j2, j1) {
            j = j2;
        }
        if !heap_less(heap, pool, j, i) {
            break;
        }
        heap.swap(i, j);
        i = j;
    }
}

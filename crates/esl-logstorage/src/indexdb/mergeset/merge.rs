//! Port of `lib/mergeset/merge.go`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::PrepareBlockCallback;
use super::block_stream_reader::BlockStreamReader;
use super::block_stream_writer::BlockStreamWriter;
use super::encoding::InmemoryBlock;
use super::part_header::PartHeader;

/// Merge error (Go: `errForciblyStopped` vs other errors).
#[derive(Debug)]
pub(crate) enum MergeError {
    ForciblyStopped,
    Other(String),
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeError::ForciblyStopped => write!(f, "forcibly stopped"),
            MergeError::Other(err) => write!(f, "{err}"),
        }
    }
}

/// Merges bsrs and writes result to bsw (port of `mergeBlockStreams`).
///
/// It also fills ph.
///
/// prepare_block is optional.
///
/// The function immediately returns when stop_ch is set.
///
/// It also atomically adds the number of items merged to items_merged.
pub(crate) fn merge_block_streams(
    ph: &mut PartHeader,
    bsw: &mut BlockStreamWriter<'_>,
    bsrs: &mut [BlockStreamReader<'_>],
    prepare_block: Option<PrepareBlockCallback>,
    stop_ch: Option<&AtomicBool>,
    items_merged: &AtomicU64,
) -> Result<(), MergeError> {
    let mut bsm = BlockStreamMerger {
        prepare_block,
        ..Default::default()
    };
    let result = (|| {
        bsm.init(bsrs).map_err(|err| {
            MergeError::Other(format!("cannot initialize blockStreamMerger: {err}"))
        })?;
        bsm.merge(bsw, bsrs, ph, stop_ch, items_merged)
    })();
    bsw.must_close();
    match result {
        Ok(()) => Ok(()),
        Err(MergeError::ForciblyStopped) => Err(MergeError::ForciblyStopped),
        Err(MergeError::Other(err)) => Err(MergeError::Other(format!(
            "cannot merge {} block streams: {err}",
            bsrs.len()
        ))),
    }
}

/// Port of `blockStreamMerger`.
///
/// PORT NOTE: Go's bsrHeap holds `*blockStreamReader`; the port keeps the
/// readers in the caller's slice and stores indices into it in the heap.
#[derive(Default)]
struct BlockStreamMerger {
    prepare_block: Option<PrepareBlockCallback>,

    /// Heap of indices into the bsrs slice, ordered by curr_item().
    bsr_heap: Vec<usize>,

    /// ib is a scratch block with pending items.
    ib: InmemoryBlock,

    ph_first_item_caught: bool,

    // These are auxiliary buffers used in flush_ib
    // for consistency checks after prepare_block call.
    first_item: Vec<u8>,
    last_item: Vec<u8>,
}

impl BlockStreamMerger {
    fn init(&mut self, bsrs: &mut [BlockStreamReader<'_>]) -> Result<(), String> {
        for (i, bsr) in bsrs.iter_mut().enumerate() {
            if bsr.next() {
                self.bsr_heap.push(i);
            }
            if let Some(err) = bsr.error() {
                return Err(format!(
                    "cannot obtain the next block from blockStreamReader {:?}: {err}",
                    bsr.path
                ));
            }
        }
        heap_init(&mut self.bsr_heap, bsrs);

        if self.bsr_heap.is_empty() {
            return Err("bsrHeap cannot be empty".to_string());
        }

        Ok(())
    }

    fn merge(
        &mut self,
        bsw: &mut BlockStreamWriter<'_>,
        bsrs: &mut [BlockStreamReader<'_>],
        ph: &mut PartHeader,
        stop_ch: Option<&AtomicBool>,
        items_merged: &AtomicU64,
    ) -> Result<(), MergeError> {
        // Use local variables for tracking the number of merged items and
        // periodically propagate the collected stats to the caller, so it
        // could be reflected in the exposed metrics.
        let mut update_stats_deadline = 0u64;
        let mut local_items_merged = 0u64;

        // PORT NOTE: Go writes the loop with `goto again`; the port uses
        // `loop` with explicit `continue`.
        loop {
            let ct = esl_common::fasttime::unix_timestamp();
            if ct > update_stats_deadline {
                items_merged.fetch_add(local_items_merged, Ordering::SeqCst);
                local_items_merged = 0;
                // Update the external stats once per second
                update_stats_deadline = ct + 1;
            }

            if self.bsr_heap.is_empty() {
                // Write the last (maybe incomplete) inmemoryBlock to bsw.
                self.flush_ib(bsw, ph, &mut local_items_merged);
                items_merged.fetch_add(local_items_merged, Ordering::SeqCst);
                return Ok(());
            }

            if stop_ch.is_some_and(|s| s.load(Ordering::SeqCst)) {
                items_merged.fetch_add(local_items_merged, Ordering::SeqCst);
                return Err(MergeError::ForciblyStopped);
            }

            let bsr_idx = self.bsr_heap[0];

            // PORT NOTE: Go aliases nextItem into the next reader's block; the
            // port copies it, since bsrs[bsr_idx] is mutated below.
            let mut next_item: Vec<u8> = Vec::new();
            let mut has_next_item = false;
            if self.bsr_heap.len() > 1 {
                let next_idx = heap_get_next_reader(&self.bsr_heap, bsrs);
                next_item.extend_from_slice(bsrs[next_idx].curr_item());
                has_next_item = true;
            }
            let bsr = &mut bsrs[bsr_idx];
            let mut compare_every_item = true;
            if bsr.curr_item_idx < bsr.block.items.len() {
                // An optimization, which allows skipping costly comparison for
                // every merged item in the loop below.
                let items = &bsr.block.items;
                let last_item = items[items.len() - 1].bytes(&bsr.block.data);
                compare_every_item = has_next_item && last_item > next_item.as_slice();
            }
            while bsr.curr_item_idx < bsr.block.items.len() {
                let item_range = bsr.block.items[bsr.curr_item_idx];
                let item = item_range.bytes(&bsr.block.data);
                if compare_every_item && item > next_item.as_slice() {
                    break;
                }
                if !self.ib.add(item) {
                    // The bsm.ib is full. Flush it to bsw and continue.
                    self.flush_ib(bsw, ph, &mut local_items_merged);
                    continue;
                }
                bsr.curr_item_idx += 1;
            }
            if bsr.curr_item_idx == bsr.block.items.len() {
                // bsr.Block is fully read. Proceed to the next block.
                if bsr.next() {
                    heap_fix(&mut self.bsr_heap, 0, bsrs);
                    continue;
                }
                if let Some(err) = bsr.error() {
                    items_merged.fetch_add(local_items_merged, Ordering::SeqCst);
                    return Err(MergeError::Other(format!(
                        "cannot read storageBlock: {err}"
                    )));
                }
                heap_pop(&mut self.bsr_heap, bsrs);
                continue;
            }

            // The next item in the bsr.Block exceeds nextItem.
            // Return bsr to heap.
            heap_fix(&mut self.bsr_heap, 0, bsrs);
        }
    }

    /// Port of `blockStreamMerger.flushIB`.
    fn flush_ib(
        &mut self,
        bsw: &mut BlockStreamWriter<'_>,
        ph: &mut PartHeader,
        items_merged: &mut u64,
    ) {
        if self.ib.items.is_empty() {
            // Nothing to flush.
            return;
        }
        *items_merged += self.ib.items.len() as u64;
        if let Some(prepare_block) = self.prepare_block {
            {
                let items = &self.ib.items;
                let data = &self.ib.data;
                self.first_item.clear();
                self.first_item.extend_from_slice(items[0].bytes(data));
                self.last_item.clear();
                self.last_item
                    .extend_from_slice(items[items.len() - 1].bytes(data));
            }
            let data = std::mem::take(&mut self.ib.data);
            let items = std::mem::take(&mut self.ib.items);
            let (data, items) = prepare_block(data, items);
            self.ib.data = data;
            self.ib.items = items;
            if self.ib.items.is_empty() {
                // Nothing to flush
                return;
            }
            // Consistency checks after prepareBlock call.
            let items = &self.ib.items;
            let data = &self.ib.data;
            let first_item = items[0].bytes(data);
            if first_item < self.first_item.as_slice() {
                esl_common::panicf!(
                    "BUG: prepareBlock must return the first item bigger or equal to the original first item;\ngot\n{first_item:X?}\nwant\n{:X?}",
                    self.first_item
                );
            }
            let last_item = items[items.len() - 1].bytes(data);
            if last_item > self.last_item.as_slice() {
                esl_common::panicf!(
                    "BUG: prepareBlock must return the last item smaller or equal to the original last item;\ngot\n{last_item:X?}\nwant\n{:X?}",
                    self.last_item
                );
            }
            // Verify whether the bsm.ib.items are sorted only in tests, since
            // this can be expensive check in prod for items with long common
            // prefix (Go: isInTest; the port checks under debug_assertions).
            if cfg!(debug_assertions) && !self.ib.is_sorted() {
                esl_common::panicf!(
                    "BUG: prepareBlock must return sorted items;\ngot\n{}",
                    self.ib.debug_items_string()
                );
            }
        }
        ph.items_count += self.ib.items.len() as u64;
        if !self.ph_first_item_caught {
            ph.first_item.clear();
            ph.first_item
                .extend_from_slice(self.ib.items[0].bytes(&self.ib.data));
            self.ph_first_item_caught = true;
        }
        ph.last_item.clear();
        ph.last_item
            .extend_from_slice(self.ib.items[self.ib.items.len() - 1].bytes(&self.ib.data));
        bsw.write_block(&mut self.ib);
        self.ib.reset();
        ph.blocks_count += 1;
    }
}

// The bsrHeap (Go: `container/heap` over `bsrHeap []*blockStreamReader`,
// ordered by CurrItem()).

fn heap_less(heap: &[usize], bsrs: &[BlockStreamReader<'_>], i: usize, j: usize) -> bool {
    bsrs[heap[i]].curr_item() < bsrs[heap[j]].curr_item()
}

fn heap_init(heap: &mut [usize], bsrs: &[BlockStreamReader<'_>]) {
    let n = heap.len();
    for i in (0..n / 2).rev() {
        sift_down(heap, bsrs, i);
    }
}

fn heap_fix(heap: &mut [usize], i: usize, bsrs: &[BlockStreamReader<'_>]) {
    if !sift_down(heap, bsrs, i) {
        sift_up(heap, bsrs, i);
    }
}

fn heap_pop(heap: &mut Vec<usize>, bsrs: &[BlockStreamReader<'_>]) -> usize {
    let n = heap.len() - 1;
    heap.swap(0, n);
    let v = heap.pop().unwrap();
    if n > 0 {
        sift_down(heap, bsrs, 0);
    }
    v
}

fn sift_down(heap: &mut [usize], bsrs: &[BlockStreamReader<'_>], mut i: usize) -> bool {
    let n = heap.len();
    let i0 = i;
    loop {
        let j1 = 2 * i + 1;
        if j1 >= n {
            break;
        }
        let mut j = j1;
        let j2 = j1 + 1;
        if j2 < n && heap_less(heap, bsrs, j2, j1) {
            j = j2;
        }
        if !heap_less(heap, bsrs, j, i) {
            break;
        }
        heap.swap(i, j);
        i = j;
    }
    i > i0
}

fn sift_up(heap: &mut [usize], bsrs: &[BlockStreamReader<'_>], mut i: usize) {
    while i > 0 {
        let parent = (i - 1) / 2;
        if !heap_less(heap, bsrs, i, parent) {
            break;
        }
        heap.swap(i, parent);
        i = parent;
    }
}

/// Port of `bsrHeap.getNextReader`: returns the index (into bsrs) of the
/// reader with the smallest current item among the heap's second and third
/// elements.
fn heap_get_next_reader(heap: &[usize], bsrs: &[BlockStreamReader<'_>]) -> usize {
    if heap.len() < 3 {
        return heap[1];
    }
    let a = heap[1];
    let b = heap[2];
    if bsrs[a].curr_item() <= bsrs[b].curr_item() {
        a
    } else {
        b
    }
}

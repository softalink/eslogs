//! Port of EsLogs `lib/logstorage/block_stream_merger.go`.
//!
//! PORT NOTE: the `idb *indexdb` and `dropFilter *partitionSearchOptions`
//! parameters (and the streamBuf/streamIDBuf/dropFilterFields state plus the
//! needDropRows/getStreamAndStreamID drop paths) are NOT ported yet: indexdb
//! and partitionSearchOptions belong to Layer 3 (indexdb.go /
//! storage_search.go), and `rows.skipRowsByDropFilter` was likewise left for
//! that layer by the rows.rs port. The Layer-3 porter must extend
//! `must_merge_block_streams` with these parameters. All the Go callers pass
//! them as nil in the currently-ported code (see inmemory_part_test.go).
//!
//! PORT NOTE: Go stores `bsw`/`bsrs` inside blockStreamMerger; the port
//! passes them into the methods that need them instead, so the merger state
//! can be pooled without carrying borrows.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use esl_common::panicf;

use crate::arena::Arena;
use crate::block::uncompressed_rows_size_bytes;
use crate::block_data::BlockData;
use crate::block_stream_reader::{BlockStreamReader, must_close_block_stream_readers};
use crate::block_stream_writer::BlockStreamWriter;
use crate::consts::MAX_UNCOMPRESSED_BLOCK_SIZE;
use crate::encoding::{
    StringsBlockUnmarshaler, get_strings_block_unmarshaler, put_strings_block_unmarshaler,
};
use crate::part_header::PartHeader;
use crate::rows::Rows;
use crate::stream_id::StreamID;
use crate::values_encoder::{ValuesDecoder, get_values_decoder, put_values_decoder};

/// Merges bsrs to bsw and updates ph accordingly.
///
/// finalize() is guaranteed to be called on bsw before returning from the func.
/// must_close() is guatanteed to be called on bsrs before returning from the func.
///
/// PORT NOTE: `stop_ch` stands in for Go's `stopCh <-chan struct{}` (a closed
/// channel signals stop); `needStop` lives in the unported datadb.go, so the
/// check is inlined here.
pub fn must_merge_block_streams(
    ph: &mut PartHeader,
    bsw: &mut BlockStreamWriter<'_>,
    bsrs: &mut [BlockStreamReader<'_>],
    stop_ch: Option<&AtomicBool>,
) {
    let mut bsm = get_block_stream_merger();
    bsm.must_init(bsrs);
    while !bsm.readers_heap.is_empty() {
        if need_stop(stop_ch) {
            break;
        }
        let idx = bsm.readers_heap[0];
        {
            let bd = &bsrs[idx].block_data;
            bsm.must_write_block(bd, bsw, bsrs);
        }
        if bsrs[idx].next_block() {
            heap_fix(&mut bsm.readers_heap, 0, bsrs);
        } else {
            heap_pop(&mut bsm.readers_heap, bsrs);
        }
    }
    bsm.must_flush_rows(bsw);
    put_block_stream_merger(bsm);

    bsw.finalize(ph);
    must_close_block_stream_readers(bsrs);
}

fn need_stop(stop_ch: Option<&AtomicBool>) -> bool {
    stop_ch.is_some_and(|s| s.load(Ordering::SeqCst))
}

/// blockStreamMerger merges block streams
#[derive(Default)]
pub struct BlockStreamMerger {
    /// readersHeap contains a heap of readers to read blocks to merge.
    ///
    /// PORT NOTE: Go stores `*blockStreamReader` values in the heap; the port
    /// stores indexes into the bsrs slice passed to must_init().
    readers_heap: Vec<usize>,

    /// streamID is the stream ID for the pending data.
    stream_id: StreamID,

    /// sbu is the unmarshaler for strings in rows and rowsTmp.
    sbu: Option<StringsBlockUnmarshaler>,

    /// vd is the decoder for unmarshaled strings.
    vd: Option<ValuesDecoder>,

    /// bd is the pending blockData.
    /// bd is unpacked into rows when needed.
    bd: BlockData,

    /// a holds bd data.
    a: Arena,

    /// rows is pending log entries.
    rows: Rows,

    /// rowsTmp is temporary storage for log entries during merge.
    rows_tmp: Rows,

    /// uncompressedRowsSizeBytes is the current size of uncompressed rows.
    ///
    /// It is used for flushing rows to blocks when their size reaches maxUncompressedBlockSize
    uncompressed_rows_size_bytes: u64,
}

impl BlockStreamMerger {
    fn reset(&mut self) {
        self.readers_heap.clear();

        self.stream_id.reset();
        self.reset_rows();
    }

    fn reset_rows(&mut self) {
        if let Some(sbu) = self.sbu.take() {
            put_strings_block_unmarshaler(sbu);
        }
        if let Some(vd) = self.vd.take() {
            put_values_decoder(vd);
        }
        self.bd.reset();
        self.a.reset();

        self.rows.reset();
        self.rows_tmp.reset();

        self.uncompressed_rows_size_bytes = 0;
    }

    fn assert_no_rows(&self) {
        if self.bd.rows_count > 0 {
            panicf!("BUG: bsm.bd must be empty; got {} rows", self.bd.rows_count);
        }
        if self.a.size_bytes() > 0 {
            panicf!(
                "BUG: bsm.a must be empty; got {} bytes",
                self.a.size_bytes()
            );
        }
        if !self.rows.timestamps.is_empty() {
            panicf!(
                "BUG: bsm.rows must be empty; got {} rows",
                self.rows.timestamps.len()
            );
        }
        if !self.rows_tmp.timestamps.is_empty() {
            panicf!(
                "BUG: bsm.rowsTmp must be empty; got {} rows",
                self.rows_tmp.timestamps.len()
            );
        }
        if self.uncompressed_rows_size_bytes != 0 {
            panicf!(
                "BUG: bsm.uncompressedRowsSizeBytes must be 0; got {}",
                self.uncompressed_rows_size_bytes
            );
        }
    }

    fn must_init(&mut self, bsrs: &mut [BlockStreamReader<'_>]) {
        self.reset();

        for (i, bsr) in bsrs.iter_mut().enumerate() {
            if bsr.next_block() {
                self.readers_heap.push(i);
            }
        }
        heap_init(&mut self.readers_heap, bsrs);
    }

    /// Writes bd to bsm.
    fn must_write_block(
        &mut self,
        bd: &BlockData,
        bsw: &mut BlockStreamWriter<'_>,
        bsrs: &[BlockStreamReader<'_>],
    ) {
        self.check_next_block(bd, bsrs);
        if !bd.stream_id.equal(&self.stream_id) {
            // The bd contains another streamID.
            // Write the bsm logs under the current streamID, then process the bd.
            self.must_flush_rows(bsw);
            self.set_stream_id(bd.stream_id);
            self.must_write_block_data(bd, bsw);
        } else if self.uncompressed_rows_size_bytes == 0
            && self.bd.rows_count == 0
            && bd.uncompressed_size_bytes >= MAX_UNCOMPRESSED_BLOCK_SIZE as u64
        {
            // The bsm is empty and the bd is full. Just write db to the output without spending CPU time on re-compression.
            self.must_write_block_data(bd, bsw);
        } else if self.uncompressed_rows_size_bytes
            + self.bd.uncompressed_size_bytes
            + bd.uncompressed_size_bytes
            >= 2 * MAX_UNCOMPRESSED_BLOCK_SIZE as u64
        {
            // The bd cannot be merged with bsm, since the final block size will be too big.
            // Write the bsm logs, then process the bd.
            self.must_flush_rows(bsw);
            self.must_write_block_data(bd, bsw);
        } else {
            // The bd contains the same streamID and the summary size of bsm logs and bd doesn't exceed the maximum allowed.
            // Merge them.
            self.must_merge_rows(bd, bsw, bsrs);
        }
    }

    /// Checks whether the bd can be written next after the current data.
    fn check_next_block(&self, bd: &BlockData, bsrs: &[BlockStreamReader<'_>]) {
        if !self.rows.timestamps.is_empty() && self.bd.rows_count > 0 {
            panicf!(
                "BUG: bsm.bd must be empty when bsm.rows isn't empty! got {} log entries in bsm.bd",
                self.bd.rows_count
            );
        }
        if bd.stream_id.less(&self.stream_id) {
            panicf!(
                "FATAL: cannot merge {}: the streamID={} for the next block is smaller than the streamID={} for the current block",
                readers_paths(bsrs),
                bd.stream_id,
                self.stream_id
            );
        }
        if !bd.stream_id.equal(&self.stream_id) {
            return;
        }
        // streamID at bd equals streamID at bsm. Check that minTimestamp in bd is bigger or equal to the minTimestmap at bsm.
        if bd.rows_count == 0 {
            return;
        }
        let next_min_timestamp = bd.timestamps_data.min_timestamp;
        if self.rows.timestamps.is_empty() {
            if self.bd.rows_count == 0 {
                return;
            }
            let min_timestamp = self.bd.timestamps_data.min_timestamp;
            if next_min_timestamp < min_timestamp {
                panicf!(
                    "FATAL: cannot merge {}: the next block's minTimestamp={} is smaller than the minTimestamp={} for the current block",
                    readers_paths(bsrs),
                    next_min_timestamp,
                    min_timestamp
                );
            }
            return;
        }
        let min_timestamp = self.rows.timestamps[0];
        if next_min_timestamp < min_timestamp {
            panicf!(
                "FATAL: cannot merge {}: the next block's minTimestamp={} is smaller than the minTimestamp={} for log entries for the current block",
                readers_paths(bsrs),
                next_min_timestamp,
                min_timestamp
            );
        }
    }

    /// Writes bd to bsm.
    fn must_write_block_data(&mut self, bd: &BlockData, bsw: &mut BlockStreamWriter<'_>) {
        self.assert_no_rows();

        if bd.uncompressed_size_bytes >= MAX_UNCOMPRESSED_BLOCK_SIZE as u64 {
            // Fast path - write full bd to the output without extracting log entries from it.
            bsw.must_write_block_data(bd);
            return;
        }

        // PORT NOTE: Go passes bsm.a to copyFrom; the Rust BlockData owns its
        // buffers, so copy_from takes no arena (see block_data.rs).
        self.bd.copy_from(bd);
    }

    /// Merges the current log entries inside bsm with bd log entries.
    fn must_merge_rows(
        &mut self,
        bd: &BlockData,
        bsw: &mut BlockStreamWriter<'_>,
        bsrs: &[BlockStreamReader<'_>],
    ) {
        if self.bd.rows_count > 0 {
            // Unmarshal log entries from bsm.bd
            // PORT NOTE: Go passes &bsm.bd to bsm.mustUnmarshalRows; the port
            // temporarily moves the pending bd out to satisfy the borrow
            // checker, then puts it back and resets it, keeping its buffers.
            let pending_bd = std::mem::take(&mut self.bd);
            self.must_unmarshal_rows(&pending_bd, bsrs);
            self.bd = pending_bd;
            self.bd.reset();
            self.a.reset();
        }

        // Unmarshal log entries from bd
        let rows_len = self.rows.timestamps.len();
        self.must_unmarshal_rows(bd, bsrs);

        // Merge unmarshaled log entries
        self.rows_tmp.merge_rows(
            &self.rows.timestamps[..rows_len],
            &self.rows.timestamps[rows_len..],
            &self.rows.rows[..rows_len],
            &self.rows.rows[rows_len..],
        );
        std::mem::swap(&mut self.rows, &mut self.rows_tmp);
        self.rows_tmp.reset();

        if self.uncompressed_rows_size_bytes >= MAX_UNCOMPRESSED_BLOCK_SIZE as u64 {
            self.must_flush_rows(bsw);
        }
    }

    fn must_unmarshal_rows(&mut self, bd: &BlockData, bsrs: &[BlockStreamReader<'_>]) {
        let rows_len = self.rows.timestamps.len();

        if self.sbu.is_none() {
            self.sbu = Some(get_strings_block_unmarshaler());
        }
        if self.vd.is_none() {
            self.vd = Some(get_values_decoder());
        }
        let sbu = self.sbu.as_mut().unwrap();
        let vd = self.vd.as_mut().unwrap();
        if let Err(err) = bd.unmarshal_rows(&mut self.rows, sbu, vd) {
            panicf!(
                "FATAL: cannot merge {}: cannot unmarshal log entries from blockData: {}",
                readers_paths(bsrs),
                err
            );
        }

        self.uncompressed_rows_size_bytes +=
            uncompressed_rows_size_bytes(&self.rows.rows[rows_len..]);
    }

    fn set_stream_id(&mut self, sid: StreamID) {
        self.stream_id = sid;
    }

    fn must_flush_rows(&mut self, bsw: &mut BlockStreamWriter<'_>) {
        if self.rows.timestamps.is_empty() {
            bsw.must_write_block_data(&self.bd);
        } else if self.rows.has_non_empty_rows() {
            bsw.must_write_rows(&self.stream_id, &self.rows.timestamps, &self.rows.rows);
        }
        self.reset_rows();
    }
}

/// Returns paths for input blockStreamReaders.
fn readers_paths(bsrs: &[BlockStreamReader<'_>]) -> String {
    let paths: Vec<String> = bsrs.iter().map(|bsr| bsr.path()).collect();
    format!("[{}]", paths.join(","))
}

static BLOCK_STREAM_MERGER_POOL: Mutex<Vec<BlockStreamMerger>> = Mutex::new(Vec::new());

pub fn get_block_stream_merger() -> BlockStreamMerger {
    BLOCK_STREAM_MERGER_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_default()
}

pub fn put_block_stream_merger(mut bsm: BlockStreamMerger) {
    bsm.reset();
    BLOCK_STREAM_MERGER_POOL.lock().unwrap().push(bsm);
}

// PORT NOTE: Go implements blockStreamReadersHeap via container/heap; the port
// implements the same sift-down algorithms over the index heap directly, since
// the comparison function needs access to the bsrs slice.

fn heap_less(bsrs: &[BlockStreamReader<'_>], i: usize, j: usize) -> bool {
    let a = &bsrs[i].block_data;
    let b = &bsrs[j].block_data;
    if !a.stream_id.equal(&b.stream_id) {
        return a.stream_id.less(&b.stream_id);
    }
    a.timestamps_data.min_timestamp < b.timestamps_data.min_timestamp
}

fn heap_init(h: &mut [usize], bsrs: &[BlockStreamReader<'_>]) {
    let n = h.len();
    for i in (0..n / 2).rev() {
        sift_down(h, i, bsrs);
    }
}

/// Equivalent of Go's heap.Fix(h, i) for i == 0 (the only usage here): a
/// sift-up at the root is a no-op, so only the sift-down is needed.
fn heap_fix(h: &mut [usize], i: usize, bsrs: &[BlockStreamReader<'_>]) {
    sift_down(h, i, bsrs);
}

fn heap_pop(h: &mut Vec<usize>, bsrs: &[BlockStreamReader<'_>]) {
    let n = h.len();
    h.swap(0, n - 1);
    h.pop();
    sift_down(h, 0, bsrs);
}

fn sift_down(h: &mut [usize], mut i: usize, bsrs: &[BlockStreamReader<'_>]) {
    let n = h.len();
    loop {
        let left = 2 * i + 1;
        if left >= n {
            break;
        }
        let mut j = left;
        let right = left + 1;
        if right < n && heap_less(bsrs, h[right], h[left]) {
            j = right;
        }
        if !heap_less(bsrs, h[j], h[i]) {
            break;
        }
        h.swap(i, j);
        i = j;
    }
}

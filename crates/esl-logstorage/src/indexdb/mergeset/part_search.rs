//! Port of `lib/mergeset/part_search.go`.
//!
//! PORT NOTE: upstream consults the global `idxbCache`/`ibCache`/
//! `ibSparseCache` block caches on every index/data block access; the port
//! reads and decompresses the blocks directly (see the note in part.rs). The
//! `sparse` Init parameter only selects the data-block cache upstream, so the
//! port accepts and ignores it.

use std::sync::Arc;

use super::block_header::{BlockHeader, unmarshal_block_headers};
use super::encoding::{InmemoryBlock, Item, common_prefix_len};
use super::table::PartWrapper;

/// The last error of a partSearch (Go stores `io.EOF` or another error).
enum PsError {
    Eof,
    Other(String),
}

#[derive(Default)]
pub(crate) struct PartSearch {
    /// The current item, as a range into ib.data
    /// (Go: the `Item []byte` field aliasing ib.data).
    ///
    /// The range is valid until the next call to next_item().
    item_start: usize,
    item_end: usize,

    /// p is a part to search.
    p: Option<Arc<PartWrapper>>,

    /// The index of the next metaindex row to scan in p.mrs
    /// (Go re-slices `ps.mrs`).
    mrs_idx: usize,

    /// The block headers of the current index block.
    bhs: Vec<BlockHeader>,

    /// The index of the next block header to scan in bhs
    /// (Go re-slices `ps.bhs`).
    bhs_idx: usize,

    /// err contains the last error.
    err: Option<PsError>,

    index_buf: Vec<u8>,
    compressed_index_buf: Vec<u8>,

    sb: super::encoding::StorageBlock,

    /// The current data block. Valid only when ib_valid is set
    /// (Go: `ps.ib != nil`).
    ib: InmemoryBlock,
    ib_valid: bool,
    ib_item_idx: usize,
}

impl PartSearch {
    pub fn reset(&mut self) {
        self.item_start = 0;
        self.item_end = 0;
        self.p = None;
        self.mrs_idx = 0;
        self.bhs.clear();
        self.bhs_idx = 0;
        self.err = None;

        self.index_buf.clear();
        self.compressed_index_buf.clear();

        self.sb.reset();

        self.ib.reset();
        self.ib_valid = false;
        self.ib_item_idx = 0;
    }

    /// Initializes ps for search in the p (port of `partSearch.Init`).
    ///
    /// Use seek() for search in p.
    pub fn init(&mut self, p: Arc<PartWrapper>, _sparse: bool) {
        self.reset();
        self.p = Some(p);
    }

    fn part(&self) -> &Arc<PartWrapper> {
        self.p
            .as_ref()
            .expect("BUG: partSearch must be initialized")
    }

    /// Returns the current item (Go: the `ps.Item` field).
    pub fn item(&self) -> &[u8] {
        &self.ib.data[self.item_start..self.item_end]
    }

    /// Seeks for the first item greater or equal to k in ps
    /// (port of `partSearch.Seek`).
    pub fn seek(&mut self, k: &[u8]) {
        if matches!(self.err, Some(PsError::Other(_))) {
            // Do nothing on unrecoverable error.
            return;
        }
        self.err = None;

        if k > self.part().p.ph.last_item.as_slice() {
            // Not matching items in the part.
            self.err = Some(PsError::Eof);
            return;
        }

        if self.try_fast_seek(k) {
            return;
        }

        self.item_start = 0;
        self.item_end = 0;
        self.mrs_idx = 0;
        self.bhs.clear();
        self.bhs_idx = 0;

        self.index_buf.clear();
        self.compressed_index_buf.clear();

        self.sb.reset();

        self.ib_valid = false;
        self.ib_item_idx = 0;

        if k <= self.part().p.ph.first_item.as_slice() {
            // The first item in the first block matches.
            if let Err(err) = self.next_block() {
                self.err = Some(err);
            }
            return;
        }

        // Locate the first metaindexRow to scan.
        let p = Arc::clone(self.part());
        let mrs = &p.p.mrs;
        if mrs.is_empty() {
            esl_common::panicf!("BUG: part without metaindex rows passed to partSearch");
        }
        let mut n = mrs.partition_point(|mr| mr.first_item.as_slice() < k);
        // The given k may be located in the previous metaindexRow, so go to it.
        n = n.saturating_sub(1);
        self.mrs_idx = n;

        // Read block headers for the found metaindexRow.
        if let Err(err) = self.next_bhs() {
            self.err = Some(err);
            return;
        }

        // Locate the first block to scan.
        let mut n = self.bhs.partition_point(|bh| bh.first_item.as_slice() < k);
        // The given k may be located in the previous block, so go to it.
        n = n.saturating_sub(1);
        self.bhs_idx = n;

        // Read the block.
        if let Err(err) = self.next_block() {
            self.err = Some(err);
            return;
        }

        // Locate the first item to scan in the block.
        let cp_len = common_prefix_len(&self.ib.common_prefix, k);
        self.ib_item_idx = binary_search_key(&self.ib.data, &self.ib.items, k, cp_len);
        if self.ib_item_idx < self.ib.items.len() {
            // The item has been found.
            return;
        }

        // Nothing found in the current block. Proceed to the next block.
        // The item to search must be the first in the next block.
        if let Err(err) = self.next_block() {
            self.err = Some(err);
        }
    }

    /// Port of `partSearch.tryFastSeek`.
    fn try_fast_seek(&mut self, k: &[u8]) -> bool {
        if !self.ib_valid {
            return false;
        }
        let data = &self.ib.data;
        let items = &self.ib.items;
        let mut idx = self.ib_item_idx;
        if idx >= items.len() {
            // The ib is exhausted.
            return false;
        }
        let cp_len = common_prefix_len(&self.ib.common_prefix, k);
        let suffix = &k[cp_len..];
        let it = items[items.len() - 1];
        let last_suffix = &data[it.start as usize + cp_len..it.end as usize];
        if suffix > last_suffix {
            // The item is located in next blocks.
            return false;
        }

        // The item is located either in the current block or in previous blocks.
        idx = idx.saturating_sub(1);
        let it = items[idx];
        let it_suffix = &data[it.start as usize + cp_len..it.end as usize];
        if suffix < it_suffix {
            let items_head = &items[..idx];
            if items_head.is_empty() {
                return false;
            }
            let it = items_head[0];
            let it_suffix = &data[it.start as usize + cp_len..it.end as usize];
            if suffix < it_suffix {
                // The item is located in previous blocks.
                return false;
            }
            idx = 0;
        }

        // The item is located in the current block
        self.ib_item_idx = idx + binary_search_key(data, &items[idx..], k, cp_len);
        true
    }

    /// Advances to the next item (port of `partSearch.NextItem`).
    ///
    /// Returns true on success.
    pub fn next_item(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }

        if self.ib_valid && self.ib_item_idx < self.ib.items.len() {
            // Fast path - the current block contains more items.
            // Proceed to the next item.
            let it = self.ib.items[self.ib_item_idx];
            self.item_start = it.start as usize;
            self.item_end = it.end as usize;
            self.ib_item_idx += 1;
            return true;
        }

        // The current block is over. Proceed to the next block.
        if let Err(err) = self.next_block() {
            let err = match err {
                PsError::Eof => PsError::Eof,
                PsError::Other(err) => {
                    PsError::Other(format!("error in {:?}: {err}", self.part().p.path))
                }
            };
            self.err = Some(err);
            return false;
        }

        // Invariant: !ib.items.is_empty() after next_block.
        let it = self.ib.items[0];
        self.item_start = it.start as usize;
        self.item_end = it.end as usize;
        self.ib_item_idx = 1;
        true
    }

    /// Returns the last error occurred in the ps
    /// (port of `partSearch.Error`; io.EOF maps to None).
    pub fn error(&self) -> Option<String> {
        match &self.err {
            None | Some(PsError::Eof) => None,
            Some(PsError::Other(err)) => Some(err.clone()),
        }
    }

    /// Port of `partSearch.nextBlock`.
    fn next_block(&mut self) -> Result<(), PsError> {
        if self.bhs_idx >= self.bhs.len() {
            // The current metaindexRow is over. Proceed to the next metaindexRow.
            self.next_bhs()?;
        }
        let bh_idx = self.bhs_idx;
        self.bhs_idx += 1;
        self.read_inmemory_block(bh_idx)?;
        self.ib_valid = true;
        self.ib_item_idx = 0;
        Ok(())
    }

    /// Port of `partSearch.nextBHS`.
    fn next_bhs(&mut self) -> Result<(), PsError> {
        let p = Arc::clone(self.part());
        if self.mrs_idx >= p.p.mrs.len() {
            return Err(PsError::Eof);
        }
        let mr = &p.p.mrs[self.mrs_idx];
        self.mrs_idx += 1;

        // PORT NOTE: Go first consults idxbCache here; the port always reads
        // the index block (see the module note).
        self.compressed_index_buf
            .resize(mr.index_block_size as usize, 0);
        p.p.index_file
            .must_read_at(&mut self.compressed_index_buf, mr.index_block_offset as i64);

        self.index_buf.clear();
        esl_common::encoding::decompress_zstd(&mut self.index_buf, &self.compressed_index_buf)
            .map_err(|err| {
                PsError::Other(format!(
                    "cannot read index block: cannot decompress index block: {err}"
                ))
            })?;

        self.bhs.clear();
        unmarshal_block_headers(
            &mut self.bhs,
            &self.index_buf,
            mr.block_headers_count as usize,
        )
        .map_err(|err| {
            PsError::Other(format!(
                "cannot read index block: cannot unmarshal block headers from index block (offset={}, size={}): {err}",
                mr.index_block_offset, mr.index_block_size
            ))
        })?;
        self.bhs_idx = 0;
        Ok(())
    }

    /// Reads and unmarshals the data block for bhs[bh_idx] into self.ib
    /// (port of `partSearch.getInmemoryBlock` + `readInmemoryBlock`, minus
    /// the caches).
    fn read_inmemory_block(&mut self, bh_idx: usize) -> Result<(), PsError> {
        let bh = &self.bhs[bh_idx];
        if bh.items_count == 1 {
            // special case for single item: there is no need in reading the
            // items/lens data, since firstItem is always stored in-memory.
            self.ib
                .unmarshal_single_item(&bh.common_prefix, &bh.first_item, bh.marshal_type);
            return Ok(());
        }

        let p = Arc::clone(self.part());
        self.sb.reset();
        self.sb.items_data.resize(bh.items_block_size as usize, 0);
        p.p.items_file
            .must_read_at(&mut self.sb.items_data, bh.items_block_offset as i64);
        self.sb.lens_data.resize(bh.lens_block_size as usize, 0);
        p.p.lens_file
            .must_read_at(&mut self.sb.lens_data, bh.lens_block_offset as i64);

        let bh = &self.bhs[bh_idx];
        self.ib
            .unmarshal_data(
                &self.sb,
                &bh.first_item,
                &bh.common_prefix,
                bh.items_count,
                bh.marshal_type,
            )
            .map_err(|err| {
                PsError::Other(format!(
                    "cannot unmarshal storage block with {} items: {err}",
                    bh.items_count
                ))
            })?;
        Ok(())
    }
}

/// Port of `binarySearchKey`.
fn binary_search_key(data: &[u8], items: &[Item], k: &[u8], cp_len: usize) -> usize {
    if items.is_empty() {
        return 0;
    }
    let suffix = &k[cp_len..];
    let it = items[0];
    let it_suffix = &data[it.start as usize + cp_len..it.end as usize];
    if suffix <= it_suffix {
        // Fast path - the item is the first.
        return 0;
    }
    let items = &items[1..];
    let offset = 1usize;

    items.partition_point(|it| {
        let it_suffix = &data[it.start as usize + cp_len..it.end as usize];
        suffix > it_suffix
    }) + offset
}

//! Port of `lib/mergeset/block_header.go`.

use esl_common::encoding;

use super::encoding::{
    MARSHAL_TYPE_PLAIN, MAX_INMEMORY_BLOCK_SIZE, MarshalType, check_marshal_type,
};

/// blockHeader describes a single block of items on disk.
///
/// PORT NOTE: Go keeps a `noCopy` flag and lets `UnmarshalNoCopy` alias the
/// source buffer; the port always owns `common_prefix`/`first_item` (the
/// index blocks are small and short-lived), trading a copy for safety.
#[derive(Default)]
pub(crate) struct BlockHeader {
    /// common prefix for all the items in the block.
    pub common_prefix: Vec<u8>,

    /// The first item.
    pub first_item: Vec<u8>,

    /// Marshal type used for block compression.
    pub marshal_type: MarshalType,

    /// The number of items in the block, including the first item.
    pub items_count: u32,

    /// The offset of the items block.
    pub items_block_offset: u64,

    /// The offset of the lens block.
    pub lens_block_offset: u64,

    /// The size of the items block.
    pub items_block_size: u32,

    /// The size of the lens block.
    pub lens_block_size: u32,
}

impl BlockHeader {
    pub fn reset(&mut self) {
        self.common_prefix.clear();
        self.first_item.clear();
        self.marshal_type = MARSHAL_TYPE_PLAIN;
        self.items_count = 0;
        self.items_block_offset = 0;
        self.lens_block_offset = 0;
        self.items_block_size = 0;
        self.lens_block_size = 0;
    }

    pub fn marshal(&self, dst: &mut Vec<u8>) {
        encoding::marshal_bytes(dst, &self.common_prefix);
        encoding::marshal_bytes(dst, &self.first_item);
        dst.push(self.marshal_type);
        encoding::marshal_uint32(dst, self.items_count);
        encoding::marshal_uint64(dst, self.items_block_offset);
        encoding::marshal_uint64(dst, self.lens_block_offset);
        encoding::marshal_uint32(dst, self.items_block_size);
        encoding::marshal_uint32(dst, self.lens_block_size);
    }

    /// Unmarshals bh from src (port of `blockHeader.UnmarshalNoCopy`; see the
    /// struct-level PORT NOTE about copying).
    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        // Unmarshal commonPrefix
        let (cp, n_size) = encoding::unmarshal_bytes(src);
        let Some(cp) = cp else {
            return Err("cannot unmarshal commonPrefix".to_string());
        };
        let src = &src[n_size as usize..];
        self.common_prefix.clear();
        self.common_prefix.extend_from_slice(cp);

        // Unmarshal firstItem
        let (fi, n_size) = encoding::unmarshal_bytes(src);
        let Some(fi) = fi else {
            return Err("cannot unmarshal firstItem".to_string());
        };
        let src = &src[n_size as usize..];
        self.first_item.clear();
        self.first_item.extend_from_slice(fi);

        // Unmarshal marshalType
        if src.is_empty() {
            return Err("cannot unmarshal marshalType from zero bytes".to_string());
        }
        self.marshal_type = src[0];
        let src = &src[1..];
        check_marshal_type(self.marshal_type)
            .map_err(|err| format!("unexpected marshalType: {err}"))?;

        // Unmarshal itemsCount
        if src.len() < 4 {
            return Err(format!(
                "cannot unmarshal itemsCount from {} bytes; need at least {} bytes",
                src.len(),
                4
            ));
        }
        self.items_count = encoding::unmarshal_uint32(src);
        let src = &src[4..];

        // Unmarshal itemsBlockOffset
        if src.len() < 8 {
            return Err(format!(
                "cannot unmarshal itemsBlockOffset from {} bytes; need at least {} bytes",
                src.len(),
                8
            ));
        }
        self.items_block_offset = encoding::unmarshal_uint64(src);
        let src = &src[8..];

        // Unmarshal lensBlockOffset
        if src.len() < 8 {
            return Err(format!(
                "cannot unmarshal lensBlockOffset from {} bytes; need at least {} bytes",
                src.len(),
                8
            ));
        }
        self.lens_block_offset = encoding::unmarshal_uint64(src);
        let src = &src[8..];

        // Unmarshal itemsBlockSize
        if src.len() < 4 {
            return Err(format!(
                "cannot unmarshal itemsBlockSize from {} bytes; need at least {} bytes",
                src.len(),
                4
            ));
        }
        self.items_block_size = encoding::unmarshal_uint32(src);
        let src = &src[4..];

        // Unmarshal lensBlockSize
        if src.len() < 4 {
            return Err(format!(
                "cannot unmarshal lensBlockSize from {} bytes; need at least {} bytes",
                src.len(),
                4
            ));
        }
        self.lens_block_size = encoding::unmarshal_uint32(src);
        let src = &src[4..];

        if self.items_count == 0 {
            return Err(format!(
                "itemsCount must be bigger than 0; got {}",
                self.items_count
            ));
        }
        if self.items_block_size > 2 * MAX_INMEMORY_BLOCK_SIZE as u32 {
            return Err(format!(
                "too big itemsBlockSize; got {}; cannot exceed {}",
                self.items_block_size,
                2 * MAX_INMEMORY_BLOCK_SIZE
            ));
        }
        if self.lens_block_size > 2 * 8 * MAX_INMEMORY_BLOCK_SIZE as u32 {
            return Err(format!(
                "too big lensBlockSize; got {}; cannot exceed {}",
                self.lens_block_size,
                2 * 8 * MAX_INMEMORY_BLOCK_SIZE
            ));
        }

        Ok(src)
    }
}

/// Unmarshals all the block headers from src and appends them to dst
/// (port of `unmarshalBlockHeadersNoCopy`; the port copies, see [`BlockHeader`]).
///
/// Block headers must be sorted by bh.firstItem.
pub(crate) fn unmarshal_block_headers(
    dst: &mut Vec<BlockHeader>,
    src: &[u8],
    block_headers_count: usize,
) -> Result<(), String> {
    if block_headers_count == 0 {
        esl_common::panicf!("BUG: blockHeadersCount must be greater than 0; got 0");
    }
    let dst_len = dst.len();
    let mut src = src;
    for i in 0..block_headers_count {
        let mut bh = BlockHeader::default();
        let tail = bh.unmarshal(src).map_err(|err| {
            format!("cannot unmarshal block header #{i} out of {block_headers_count}: {err}")
        })?;
        dst.push(bh);
        src = tail;
    }
    if !src.is_empty() {
        return Err(format!(
            "unexpected non-zero tail left after unmarshaling {block_headers_count} block headers; len(tail)={}",
            src.len()
        ));
    }
    let new_bhs = &dst[dst_len..];

    // Verify that block headers are sorted by firstItem.
    if !new_bhs
        .windows(2)
        .all(|w| w[0].first_item <= w[1].first_item)
    {
        return Err(format!(
            "block headers must be sorted by firstItem; unmarshaled {} unsorted block headers",
            new_bhs.len()
        ));
    }

    Ok(())
}

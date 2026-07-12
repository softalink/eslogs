//! Port of `lib/mergeset/metaindex_row.go`.

use esl_common::encoding;

/// The maximum size of index block with multiple blockHeaders
/// (Go keeps this const in block_stream_writer.go).
pub(crate) const MAX_INDEX_BLOCK_SIZE: usize = 64 * 1024;

/// metaindexRow describes a block of blockHeaders aka index block.
#[derive(Default)]
pub(crate) struct MetaindexRow {
    /// First item in the first block.
    /// It is used for fast lookup of the required index block.
    pub first_item: Vec<u8>,

    /// The number of blockHeaders the block contains.
    pub block_headers_count: u32,

    /// The offset of the block in the index file.
    pub index_block_offset: u64,

    /// The size of the block in the index file.
    pub index_block_size: u32,
}

impl MetaindexRow {
    pub fn reset(&mut self) {
        self.first_item.clear();
        self.block_headers_count = 0;
        self.index_block_offset = 0;
        self.index_block_size = 0;
    }

    pub fn marshal(&self, dst: &mut Vec<u8>) {
        encoding::marshal_bytes(dst, &self.first_item);
        encoding::marshal_uint32(dst, self.block_headers_count);
        encoding::marshal_uint64(dst, self.index_block_offset);
        encoding::marshal_uint32(dst, self.index_block_size);
    }

    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        // Unmarshal firstItem
        let (fi, n_size) = encoding::unmarshal_bytes(src);
        let Some(fi) = fi else {
            return Err("cannot unmarshal firstItem".to_string());
        };
        let src = &src[n_size as usize..];
        self.first_item.clear();
        self.first_item.extend_from_slice(fi);

        // Unmarshal blockHeadersCount
        if src.len() < 4 {
            return Err(format!(
                "cannot unmarshal blockHeadersCount from {} bytes; need at least {} bytes",
                src.len(),
                4
            ));
        }
        self.block_headers_count = encoding::unmarshal_uint32(src);
        let src = &src[4..];

        // Unmarshal indexBlockOffset
        if src.len() < 8 {
            return Err(format!(
                "cannot unmarshal indexBlockOffset from {} bytes; need at least {} bytes",
                src.len(),
                8
            ));
        }
        self.index_block_offset = encoding::unmarshal_uint64(src);
        let src = &src[8..];

        // Unmarshal indexBlockSize
        if src.len() < 4 {
            return Err(format!(
                "cannot unmarshal indexBlockSize from {} bytes; need at least {} bytes",
                src.len(),
                4
            ));
        }
        self.index_block_size = encoding::unmarshal_uint32(src);
        let src = &src[4..];

        if self.block_headers_count == 0 {
            return Err(format!(
                "blockHeadersCount must be bigger than 0; got {}",
                self.block_headers_count
            ));
        }
        if self.index_block_size > 4 * MAX_INDEX_BLOCK_SIZE as u32 {
            // The index block size can exceed maxIndexBlockSize by up to 4x,
            // since it can contain commonPrefix and firstItem at blockHeader
            // with the maximum length of maxIndexBlockSize per each field.
            return Err(format!(
                "too big indexBlockSize: {}; cannot exceed {}",
                self.index_block_size,
                4 * MAX_INDEX_BLOCK_SIZE
            ));
        }

        Ok(src)
    }
}

/// Reads and unmarshals all the metaindex rows from r
/// (port of `unmarshalMetaindexRows`).
pub(crate) fn unmarshal_metaindex_rows<R: std::io::Read + ?Sized>(
    dst: &mut Vec<MetaindexRow>,
    r: &mut R,
) -> Result<(), String> {
    // It is ok to read all the metaindex in memory,
    // since it is quite small.
    let mut compressed_data = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    loop {
        match r.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => compressed_data.extend_from_slice(&buf[..n]),
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) => return Err(format!("cannot read metaindex data: {err}")),
        }
    }
    let mut data = Vec::new();
    encoding::decompress_zstd(&mut data, &compressed_data)
        .map_err(|err| format!("cannot decompress metaindex data: {err}"))?;

    let dst_len = dst.len();
    let mut src: &[u8] = &data;
    while !src.is_empty() {
        let mut mr = MetaindexRow::default();
        let tail = mr.unmarshal(src).map_err(|err| {
            format!(
                "cannot unmarshal metaindexRow #{} from metaindex data: {err}",
                dst.len() - dst_len
            )
        })?;
        dst.push(mr);
        src = tail;
    }
    if dst_len == dst.len() {
        return Err("expecting non-zero metaindex rows; got zero".to_string());
    }

    // Make sure metaindexRows are sorted by firstItem.
    let tmp = &dst[dst_len..];
    if !tmp.windows(2).all(|w| w[0].first_item <= w[1].first_item) {
        return Err(format!(
            "metaindex {} rows aren't sorted by firstItem",
            tmp.len()
        ));
    }

    Ok(())
}

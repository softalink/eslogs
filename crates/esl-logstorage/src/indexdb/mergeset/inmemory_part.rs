//! Port of `lib/mergeset/inmemory_part.go`.

use std::path::{Path, PathBuf};

use esl_common::{chunkedbuffer, encoding, filestream, fs};

use super::block_header::BlockHeader;
use super::encoding::{InmemoryBlock, StorageBlock};
use super::metaindex_row::{MAX_INDEX_BLOCK_SIZE, MetaindexRow};
use super::part_header::PartHeader;
use super::{INDEX_FILENAME, ITEMS_FILENAME, LENS_FILENAME, METAINDEX_FILENAME};

#[derive(Default)]
pub(crate) struct InmemoryPart {
    pub ph: PartHeader,
    bh: BlockHeader,
    mr: MetaindexRow,

    pub metaindex_data: chunkedbuffer::Buffer,
    pub index_data: chunkedbuffer::Buffer,
    pub items_data: chunkedbuffer::Buffer,
    pub lens_data: chunkedbuffer::Buffer,
}

impl InmemoryPart {
    pub fn reset(&mut self) {
        self.ph.reset();
        self.bh.reset();
        self.mr.reset();

        self.metaindex_data.reset();
        self.index_data.reset();
        self.items_data.reset();
        self.lens_data.reset();
    }

    /// Stores mp to the given path on disk
    /// (port of `inmemoryPart.MustStoreToDisk`).
    pub fn must_store_to_disk(&self, path: &Path) {
        fs::must_mkdir_fail_if_exist(path);

        let metaindex_path = path.join(METAINDEX_FILENAME);
        let index_path = path.join(INDEX_FILENAME);
        let items_path = path.join(ITEMS_FILENAME);
        let lens_path = path.join(LENS_FILENAME);

        let mut psw = filestream::ParallelStreamWriter::new();
        fn add<'a>(
            psw: &mut filestream::ParallelStreamWriter<'a>,
            dst: PathBuf,
            cb: &'a chunkedbuffer::Buffer,
        ) {
            psw.add(
                dst,
                Box::new(move |w: &mut filestream::Writer| {
                    cb.write_to(w)
                        .map_err(|(_, err)| std::io::Error::other(err))
                }),
            );
        }
        add(&mut psw, metaindex_path, &self.metaindex_data);
        add(&mut psw, index_path, &self.index_data);
        add(&mut psw, items_path, &self.items_data);
        add(&mut psw, lens_path, &self.lens_data);
        psw.run();

        self.ph.must_write_metadata(path);

        fs::must_sync_path_and_parent_dir(path);
    }

    /// Initializes mp from ib (port of `inmemoryPart.Init`).
    pub fn init(&mut self, ib: &mut InmemoryBlock) {
        self.reset();

        let mut sb = StorageBlock::default();

        // Use the minimum possible compressLevel for compressing inmemoryPart,
        // since it will be merged into file part soon.
        // See https://github.com/facebook/zstd/releases/tag/v1.3.4 for details
        // about negative compression level
        let compress_level = -5;
        let (items_count, marshal_type) = ib.marshal_unsorted_data(
            &mut sb,
            &mut self.bh.first_item,
            &mut self.bh.common_prefix,
            compress_level,
        );
        self.bh.items_count = items_count;
        self.bh.marshal_type = marshal_type;

        self.ph.items_count = ib.items.len() as u64;
        self.ph.blocks_count = 1;
        self.ph.first_item.clear();
        self.ph
            .first_item
            .extend_from_slice(ib.items[0].bytes(&ib.data));
        self.ph.last_item.clear();
        self.ph
            .last_item
            .extend_from_slice(ib.items[ib.items.len() - 1].bytes(&ib.data));

        self.items_data.must_write(&sb.items_data);
        self.bh.items_block_offset = 0;
        self.bh.items_block_size = sb.items_data.len() as u32;

        self.lens_data.must_write(&sb.lens_data);
        self.bh.lens_block_offset = 0;
        self.bh.lens_block_size = sb.lens_data.len() as u32;

        let mut bb: Vec<u8> = Vec::new();
        self.bh.marshal(&mut bb);
        if bb.len() > 3 * MAX_INDEX_BLOCK_SIZE {
            // marshaled blockHeader can exceed indexBlockSize when firstItem
            // and commonPrefix sizes are close to indexBlockSize
            esl_common::panicf!(
                "BUG: too big index block: {} bytes; mustn't exceed {} bytes",
                bb.len(),
                3 * MAX_INDEX_BLOCK_SIZE
            );
        }
        let mut packed: Vec<u8> = Vec::new();
        encoding::compress_zstd_level(&mut packed, &bb, compress_level);
        self.index_data.must_write(&packed);

        self.mr.first_item.clear();
        self.mr.first_item.extend_from_slice(&self.bh.first_item);
        self.mr.block_headers_count = 1;
        self.mr.index_block_offset = 0;
        self.mr.index_block_size = packed.len() as u32;
        bb.clear();
        self.mr.marshal(&mut bb);
        packed.clear();
        encoding::compress_zstd_level(&mut packed, &bb, compress_level);
        self.metaindex_data.must_write(&packed);
    }

    pub fn size(&self) -> u64 {
        (self.metaindex_data.size_bytes()
            + self.index_data.size_bytes()
            + self.items_data.size_bytes()
            + self.lens_data.size_bytes()) as u64
    }
}

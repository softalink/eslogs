//! Port of `lib/mergeset/block_stream_writer.go`.

use std::path::Path;

use esl_common::{chunkedbuffer, encoding, filestream, fs};

use super::block_header::BlockHeader;
use super::encoding::{InmemoryBlock, StorageBlock};
use super::inmemory_part::InmemoryPart;
use super::metaindex_row::{MAX_INDEX_BLOCK_SIZE, MetaindexRow};
use super::{INDEX_FILENAME, ITEMS_FILENAME, LENS_FILENAME, METAINDEX_FILENAME};

/// The destination streams of a block stream writer
/// (Go: the four `filestream.WriteCloser` fields).
#[derive(Default)]
enum WriterDest<'a> {
    #[default]
    None,
    Mem {
        metaindex: &'a mut chunkedbuffer::Buffer,
        index: &'a mut chunkedbuffer::Buffer,
        items: &'a mut chunkedbuffer::Buffer,
        lens: &'a mut chunkedbuffer::Buffer,
    },
    // Boxed to keep the enum small (the four buffered writers are large).
    File(Box<FileWriters>),
}

struct FileWriters {
    metaindex: filestream::Writer,
    index: filestream::Writer,
    items: filestream::Writer,
    lens: filestream::Writer,
}

/// PORT NOTE: Go pools blockStreamWriters in a sync.Pool; the port creates
/// them per merge - the buffers are small and the merge itself dominates.
#[derive(Default)]
pub(crate) struct BlockStreamWriter<'a> {
    compress_level: i32,

    dest: WriterDest<'a>,

    sb: StorageBlock,
    bh: BlockHeader,
    mr: MetaindexRow,

    unpacked_index_block_buf: Vec<u8>,
    packed_index_block_buf: Vec<u8>,

    unpacked_metaindex_buf: Vec<u8>,
    packed_metaindex_buf: Vec<u8>,

    items_block_offset: u64,
    lens_block_offset: u64,
    index_block_offset: u64,

    /// whether the first item for mr has been caught.
    mr_first_item_caught: bool,
}

impl<'a> BlockStreamWriter<'a> {
    fn reset(&mut self) {
        self.compress_level = 0;

        self.dest = WriterDest::None;

        self.sb.reset();
        self.bh.reset();
        self.mr.reset();

        self.unpacked_index_block_buf.clear();
        self.packed_index_block_buf.clear();

        self.unpacked_metaindex_buf.clear();
        self.packed_metaindex_buf.clear();

        self.items_block_offset = 0;
        self.lens_block_offset = 0;
        self.index_block_offset = 0;

        self.mr_first_item_caught = false;
    }

    /// Port of `blockStreamWriter.MustInitFromInmemoryPart`.
    pub fn must_init_from_inmemory_part(&mut self, mp: &'a mut InmemoryPart, compress_level: i32) {
        self.reset();

        self.compress_level = compress_level;
        self.dest = WriterDest::Mem {
            metaindex: &mut mp.metaindex_data,
            index: &mut mp.index_data,
            items: &mut mp.items_data,
            lens: &mut mp.lens_data,
        };
    }

    /// Initializes bsw from a file-based part on the given path
    /// (port of `blockStreamWriter.MustInitFromFilePart`).
    ///
    /// The bsw doesn't pollute OS page cache if nocache is set.
    pub fn must_init_from_file_part(&mut self, path: &Path, nocache: bool, compress_level: i32) {
        self.reset();
        self.compress_level = compress_level;

        // Create the directory
        fs::must_mkdir_fail_if_exist(path);

        // Create part files in the directory in parallel in order to speedup
        // the process on high-latency storage systems such as NFS or Ceph.
        let index_path = path.join(INDEX_FILENAME);
        let items_path = path.join(ITEMS_FILENAME);
        let lens_path = path.join(LENS_FILENAME);
        let metaindex_path = path.join(METAINDEX_FILENAME);

        let mut index_writer: Option<filestream::Writer> = None;
        let mut items_writer: Option<filestream::Writer> = None;
        let mut lens_writer: Option<filestream::Writer> = None;
        let mut metaindex_writer: Option<filestream::Writer> = None;

        let mut pfc = filestream::ParallelFileCreator::new();
        pfc.add(index_path, &mut index_writer, nocache);
        pfc.add(items_path, &mut items_writer, nocache);
        pfc.add(lens_path, &mut lens_writer, nocache);
        // Always cache metaindex file in OS page cache, since it is
        // immediately read after the merge.
        pfc.add(metaindex_path, &mut metaindex_writer, false);
        pfc.run();

        self.dest = WriterDest::File(Box::new(FileWriters {
            metaindex: metaindex_writer.unwrap(),
            index: index_writer.unwrap(),
            items: items_writer.unwrap(),
            lens: lens_writer.unwrap(),
        }));
    }

    /// Closes the bsw and the underlying writers
    /// (port of `blockStreamWriter.MustClose`).
    pub fn must_close(&mut self) {
        // Flush the remaining data.
        self.flush_index_data();

        // Compress and write metaindex.
        self.packed_metaindex_buf.clear();
        encoding::compress_zstd_level(
            &mut self.packed_metaindex_buf,
            &self.unpacked_metaindex_buf,
            self.compress_level,
        );
        match &mut self.dest {
            WriterDest::None => {
                esl_common::panicf!("BUG: blockStreamWriter must be initialized before MustClose")
            }
            WriterDest::Mem { metaindex, .. } => metaindex.must_write(&self.packed_metaindex_buf),
            WriterDest::File(w) => {
                fs::must_write_data(&mut w.metaindex, &self.packed_metaindex_buf);
                w.metaindex.must_close();
                w.index.must_close();
                w.items.must_close();
                w.lens.must_close();
            }
        }

        self.reset();
    }

    /// Writes ib to bsw (port of `blockStreamWriter.WriteBlock`).
    ///
    /// ib must be sorted.
    pub fn write_block(&mut self, ib: &mut InmemoryBlock) {
        let (items_count, marshal_type) = ib.marshal_sorted_data(
            &mut self.sb,
            &mut self.bh.first_item,
            &mut self.bh.common_prefix,
            self.compress_level,
        );
        self.bh.items_count = items_count;
        self.bh.marshal_type = marshal_type;

        // Write itemsData
        match &mut self.dest {
            WriterDest::None => {
                esl_common::panicf!("BUG: blockStreamWriter must be initialized before WriteBlock")
            }
            WriterDest::Mem { items, lens, .. } => {
                items.must_write(&self.sb.items_data);
                lens.must_write(&self.sb.lens_data);
            }
            WriterDest::File(w) => {
                fs::must_write_data(&mut w.items, &self.sb.items_data);
                fs::must_write_data(&mut w.lens, &self.sb.lens_data);
            }
        }
        self.bh.items_block_size = self.sb.items_data.len() as u32;
        self.bh.items_block_offset = self.items_block_offset;
        self.items_block_offset += self.bh.items_block_size as u64;

        // Write lensData
        self.bh.lens_block_size = self.sb.lens_data.len() as u32;
        self.bh.lens_block_offset = self.lens_block_offset;
        self.lens_block_offset += self.bh.lens_block_size as u64;

        // Write blockHeader
        let unpacked_index_block_buf_len = self.unpacked_index_block_buf.len();
        self.bh.marshal(&mut self.unpacked_index_block_buf);
        if self.unpacked_index_block_buf.len() > MAX_INDEX_BLOCK_SIZE {
            self.unpacked_index_block_buf
                .truncate(unpacked_index_block_buf_len);
            self.flush_index_data();
            self.bh.marshal(&mut self.unpacked_index_block_buf);
        }

        if !self.mr_first_item_caught {
            self.mr.first_item.clear();
            self.mr.first_item.extend_from_slice(&self.bh.first_item);
            self.mr_first_item_caught = true;
        }
        self.bh.reset();
        self.mr.block_headers_count += 1;
    }

    /// Port of `blockStreamWriter.flushIndexData`.
    fn flush_index_data(&mut self) {
        if self.unpacked_index_block_buf.is_empty() {
            // Nothing to flush.
            return;
        }

        // Write indexBlock.
        self.packed_index_block_buf.clear();
        encoding::compress_zstd_level(
            &mut self.packed_index_block_buf,
            &self.unpacked_index_block_buf,
            self.compress_level,
        );
        match &mut self.dest {
            WriterDest::None => {
                esl_common::panicf!("BUG: blockStreamWriter must be initialized before use")
            }
            WriterDest::Mem { index, .. } => index.must_write(&self.packed_index_block_buf),
            WriterDest::File(w) => fs::must_write_data(&mut w.index, &self.packed_index_block_buf),
        }
        self.mr.index_block_size = self.packed_index_block_buf.len() as u32;
        self.mr.index_block_offset = self.index_block_offset;
        self.index_block_offset += self.mr.index_block_size as u64;
        self.unpacked_index_block_buf.clear();

        // Write metaindexRow.
        self.mr.marshal(&mut self.unpacked_metaindex_buf);
        self.mr.reset();

        // Notify that the next call to WriteBlock must catch the first item.
        self.mr_first_item_caught = false;
    }
}

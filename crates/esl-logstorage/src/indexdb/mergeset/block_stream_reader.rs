//! Port of `lib/mergeset/block_stream_reader.go`.

use std::path::{Path, PathBuf};

use esl_common::{chunkedbuffer, filestream, fs};

use super::block_header::{BlockHeader, unmarshal_block_headers};
use super::encoding::{InmemoryBlock, StorageBlock};
use super::inmemory_part::InmemoryPart;
use super::metaindex_row::{MetaindexRow, unmarshal_metaindex_rows};
use super::part_header::PartHeader;
use super::{INDEX_FILENAME, ITEMS_FILENAME, LENS_FILENAME, METAINDEX_FILENAME};

/// A sequential reader over one part file
/// (Go: the `filestream.ReadCloser` fields of blockStreamReader).
pub(super) enum StreamReader<'a> {
    File(filestream::Reader),
    Mem(chunkedbuffer::Reader<'a>),
}

impl StreamReader<'_> {
    /// io.ReadFull semantics (Go: `fs.MustReadData`).
    fn must_read_data(&mut self, data: &mut [u8]) {
        match self {
            StreamReader::File(r) => fs::must_read_data(r, data),
            StreamReader::Mem(r) => {
                use std::io::Read;
                let mut n = 0usize;
                while n < data.len() {
                    match r.read(&mut data[n..]) {
                        Ok(0) => esl_common::panicf!(
                            "FATAL: cannot read {} bytes from in-memory reader: unexpected EOF after {} bytes",
                            data.len(),
                            n
                        ),
                        Ok(m) => n += m,
                        Err(err) => esl_common::panicf!(
                            "FATAL: cannot read {} bytes from in-memory reader: {}",
                            data.len(),
                            err
                        ),
                    }
                }
            }
        }
    }

    fn must_close(&mut self) {
        match self {
            StreamReader::File(r) => r.must_close(),
            StreamReader::Mem(r) => r.must_close(),
        }
    }
}

/// The last error of a blockStreamReader (Go stores `io.EOF` or another error
/// in `bsr.err`).
pub(super) enum BsrError {
    Eof,
    Other(String),
}

#[derive(Default)]
pub(crate) struct BlockStreamReader<'a> {
    /// Block contains the current block if next() returned true.
    pub(super) block: InmemoryBlock,

    /// is_inmemory_block is set to true if bsr was initialized with
    /// must_init_from_inmemory_block().
    is_inmemory_block: bool,

    /// The index of the current item in the block, which is returned from
    /// curr_item().
    pub(super) curr_item_idx: usize,

    pub(super) path: PathBuf,

    /// ph contains partHeader for the read part.
    pub(super) ph: PartHeader,

    /// All the metaindexRows.
    mrs: Vec<MetaindexRow>,

    /// The index for the currently processed metaindexRow from mrs.
    mr_idx: usize,

    /// Currently processed blockHeaders.
    pub(super) bhs: Vec<BlockHeader>,

    /// The index of the currently processed blockHeader.
    pub(super) bh_idx: usize,

    readers: Option<Readers<'a>>,

    /// Contains the current storageBlock.
    sb: StorageBlock,

    /// The number of items read so far.
    items_read: u64,

    /// The number of blocks read so far.
    blocks_read: u64,

    /// Whether the first item in the reader checked with ph.firstItem.
    first_item_checked: bool,

    packed_buf: Vec<u8>,
    unpacked_buf: Vec<u8>,

    /// The last error.
    err: Option<BsrError>,
}

struct Readers<'a> {
    index: StreamReader<'a>,
    items: StreamReader<'a>,
    lens: StreamReader<'a>,
}

impl<'a> BlockStreamReader<'a> {
    fn reset(&mut self) {
        self.block.reset();
        self.is_inmemory_block = false;
        self.curr_item_idx = 0;
        self.path = PathBuf::new();
        self.ph.reset();
        self.mrs.clear();
        self.mr_idx = 0;
        self.bhs.clear();
        self.bh_idx = 0;

        self.readers = None;

        self.sb.reset();

        self.items_read = 0;
        self.blocks_read = 0;
        self.first_item_checked = false;

        self.packed_buf.clear();
        self.unpacked_buf.clear();

        self.err = None;
    }

    /// Initializes bsr from the given ib
    /// (port of `blockStreamReader.MustInitFromInmemoryBlock`).
    pub fn must_init_from_inmemory_block(&mut self, ib: &InmemoryBlock) {
        self.reset();
        self.block.copy_from(ib);
        self.block.sort_items();
        self.is_inmemory_block = true;
    }

    /// Initializes bsr from the given mp
    /// (port of `blockStreamReader.MustInitFromInmemoryPart`).
    pub fn must_init_from_inmemory_part(&mut self, mp: &'a InmemoryPart) {
        self.reset();

        let mut metaindex_reader = mp.metaindex_data.new_reader();
        if let Err(err) = unmarshal_metaindex_rows(&mut self.mrs, &mut metaindex_reader) {
            esl_common::panicf!(
                "BUG: cannot unmarshal metaindex rows from inmemory part: {}",
                err
            );
        }

        self.ph.copy_from(&mp.ph);
        self.readers = Some(Readers {
            index: StreamReader::Mem(mp.index_data.new_reader()),
            items: StreamReader::Mem(mp.items_data.new_reader()),
            lens: StreamReader::Mem(mp.lens_data.new_reader()),
        });

        if self.ph.items_count == 0 {
            esl_common::panicf!("BUG: source inmemoryPart must contain at least a single item");
        }
        if self.ph.blocks_count == 0 {
            esl_common::panicf!("BUG: source inmemoryPart must contain at least a single block");
        }
    }

    /// Initializes bsr from a file-based part on the given path
    /// (port of `blockStreamReader.MustInitFromFilePart`).
    ///
    /// Part files are read without OS cache pollution, since the part is
    /// usually deleted after the merge.
    pub fn must_init_from_file_part(&mut self, path: &Path) {
        self.reset();

        self.ph.must_read_metadata(path);

        let metaindex_path = path.join(METAINDEX_FILENAME);
        let mut metaindex_file = filestream::must_open(&metaindex_path, true);
        let res = {
            struct Adapter<'r>(&'r mut filestream::Reader);
            impl std::io::Read for Adapter<'_> {
                fn read(&mut self, p: &mut [u8]) -> std::io::Result<usize> {
                    esl_common::filestream::ReadCloser::read(self.0, p)
                }
            }
            unmarshal_metaindex_rows(&mut self.mrs, &mut Adapter(&mut metaindex_file))
        };
        metaindex_file.must_close();
        if let Err(err) = res {
            esl_common::panicf!(
                "FATAL: cannot unmarshal metaindex rows from file {:?}: {}",
                metaindex_path,
                err
            );
        }

        self.path = path.to_path_buf();

        // PORT NOTE: Go opens the three part files in parallel
        // (filestream.ParallelFileOpener) for high-latency network storage;
        // the port opens them sequentially.
        let index_path = path.join(INDEX_FILENAME);
        let items_path = path.join(ITEMS_FILENAME);
        let lens_path = path.join(LENS_FILENAME);
        self.readers = Some(Readers {
            index: StreamReader::File(filestream::must_open(&index_path, true)),
            items: StreamReader::File(filestream::must_open(&items_path, true)),
            lens: StreamReader::File(filestream::must_open(&lens_path, true)),
        });
    }

    /// Closes the bsr (port of `blockStreamReader.MustClose`).
    pub fn must_close(&mut self) {
        if let Some(readers) = &mut self.readers {
            readers.index.must_close();
            readers.items.must_close();
            readers.lens.must_close();
        }
        self.reset();
    }

    /// Port of `blockStreamReader.CurrItem`.
    pub(super) fn curr_item(&self) -> &[u8] {
        self.block.items[self.curr_item_idx].bytes(&self.block.data)
    }

    /// Port of `blockStreamReader.Next`.
    pub fn next(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }
        if self.is_inmemory_block {
            self.err = Some(BsrError::Eof);
            return true;
        }

        if self.bh_idx >= self.bhs.len() {
            // The current index block is over. Try reading the next index block.
            if let Err(err) = self.read_next_bhs() {
                let err = match err {
                    BsrError::Eof => {
                        // Check the last item.
                        let b = &self.block;
                        let last_item = b.items[b.items.len() - 1].bytes(&b.data);
                        if self.ph.last_item != last_item {
                            BsrError::Other(format!(
                                "unexpected last item; got {last_item:X?}; want {:X?}",
                                self.ph.last_item
                            ))
                        } else {
                            BsrError::Eof
                        }
                    }
                    BsrError::Other(err) => {
                        BsrError::Other(format!("cannot read the next index block: {err}"))
                    }
                };
                self.err = Some(err);
                return false;
            }
        }

        let bh = &self.bhs[self.bh_idx];
        self.bh_idx += 1;

        self.sb.items_data.resize(bh.items_block_size as usize, 0);
        self.sb.lens_data.resize(bh.lens_block_size as usize, 0);
        {
            let readers = self.readers.as_mut().expect("BUG: bsr must be initialized");
            readers.items.must_read_data(&mut self.sb.items_data);
            readers.lens.must_read_data(&mut self.sb.lens_data);
        }

        let bh = &self.bhs[self.bh_idx - 1];
        if let Err(err) = self.block.unmarshal_data(
            &self.sb,
            &bh.first_item,
            &bh.common_prefix,
            bh.items_count,
            bh.marshal_type,
        ) {
            self.err = Some(BsrError::Other(format!(
                "cannot unmarshal inmemoryBlock from storageBlock with firstItem={:X?}, commonPrefix={:X?}, itemsCount={}, marshalType={}: {err}",
                bh.first_item, bh.common_prefix, bh.items_count, bh.marshal_type
            )));
            return false;
        }
        self.blocks_read += 1;
        if self.blocks_read > self.ph.blocks_count {
            self.err = Some(BsrError::Other(format!(
                "too many blocks read: {}; must be smaller than partHeader.blocksCount {}",
                self.blocks_read, self.ph.blocks_count
            )));
            return false;
        }
        self.curr_item_idx = 0;
        self.items_read += self.block.items.len() as u64;
        if self.items_read > self.ph.items_count {
            self.err = Some(BsrError::Other(format!(
                "too many items read: {}; must be smaller than partHeader.itemsCount {}",
                self.items_read, self.ph.items_count
            )));
            return false;
        }
        if !self.first_item_checked {
            self.first_item_checked = true;
            let first_item = self.block.items[0].bytes(&self.block.data);
            if self.ph.first_item != first_item {
                self.err = Some(BsrError::Other(format!(
                    "unexpected first item; got {first_item:X?}; want {:X?}",
                    self.ph.first_item
                )));
                return false;
            }
        }
        true
    }

    /// Port of `blockStreamReader.readNextBHS`.
    fn read_next_bhs(&mut self) -> Result<(), BsrError> {
        if self.mr_idx >= self.mrs.len() {
            return Err(BsrError::Eof);
        }

        let mr = &self.mrs[self.mr_idx];
        self.mr_idx += 1;

        // Read compressed index block.
        self.packed_buf.resize(mr.index_block_size as usize, 0);
        self.readers
            .as_mut()
            .expect("BUG: bsr must be initialized")
            .index
            .must_read_data(&mut self.packed_buf);

        // Unpack the compressed index block.
        self.unpacked_buf.clear();
        esl_common::encoding::decompress_zstd(&mut self.unpacked_buf, &self.packed_buf)
            .map_err(|err| BsrError::Other(format!("cannot decompress index block: {err}")))?;

        // Unmarshal the unpacked index block into bsr.bhs.
        self.bhs.clear();
        let mr = &self.mrs[self.mr_idx - 1];
        unmarshal_block_headers(
            &mut self.bhs,
            &self.unpacked_buf,
            mr.block_headers_count as usize,
        )
        .map_err(|err| {
            BsrError::Other(format!(
                "cannot unmarshal blockHeaders in the index block #{}: {err}",
                self.mr_idx
            ))
        })?;
        self.bh_idx = 0;
        Ok(())
    }

    /// Port of `blockStreamReader.Error`.
    pub fn error(&self) -> Option<String> {
        match &self.err {
            None | Some(BsrError::Eof) => None,
            Some(BsrError::Other(err)) => Some(err.clone()),
        }
    }
}

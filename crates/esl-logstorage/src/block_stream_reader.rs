//! Port of EsLogs `lib/logstorage/block_stream_reader.go`.
//!
//! PORT NOTE: Go's `streamReaders` hold `filestream.ReadCloser` interface
//! values which are either `*filestream.Reader` (file parts) or in-memory
//! `chunkedbuffer` readers (in-memory parts). The port uses the
//! [`StreamReaderSource`] enum instead of a trait object, mirroring the two
//! concrete implementations Go actually uses; the `None` variant corresponds
//! to Go's nil interface value (readers absent for old part format versions).
//!
//! PORT NOTE: `lib/logstorage/index_block_header.go` (and its tests) is ported
//! into this module: there is no `index_block_header` module in the crate, and
//! `indexBlockHeader` is tightly coupled to `streamReaders`/`streamWriters`
//! defined by the block stream reader/writer pair. Move it to its own module
//! if one is added later.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};

use esl_common::fs::MustCloser;
use esl_common::{bytesutil, chunkedbuffer, encoding, filestream, fs, panicf};

use crate::arena::Arena;
use crate::block_data::BlockData;
use crate::block_header::{BlockHeader, reset_block_headers, unmarshal_block_headers};
use crate::block_stream_writer::{LONG_TERM_BUF_POOL, StreamWriters, WriterWithStats};
use crate::column_names::{must_read_column_idxs, must_read_column_names};
use crate::consts::MAX_INDEX_BLOCK_SIZE;
use crate::filenames::{
    COLUMN_IDXS_FILENAME, COLUMN_NAMES_FILENAME, COLUMNS_HEADER_FILENAME,
    COLUMNS_HEADER_INDEX_FILENAME, INDEX_FILENAME, MESSAGE_BLOOM_FILENAME, MESSAGE_VALUES_FILENAME,
    METAINDEX_FILENAME, OLD_BLOOM_FILENAME, OLD_VALUES_FILENAME, TIMESTAMPS_FILENAME,
};
use crate::inmemory_part::InmemoryPart;
use crate::part::{get_bloom_file_path, get_values_file_path};
use crate::part_header::PartHeader;
use crate::stream_id::StreamID;

/// The underlying source for a part input stream.
///
/// PORT NOTE: stands in for Go's `filestream.ReadCloser` interface; the `None`
/// variant corresponds to Go's nil interface value.
#[derive(Default)]
pub enum StreamReaderSource<'a> {
    #[default]
    None,
    File(filestream::Reader),
    Inmemory(chunkedbuffer::Reader<'a>),
}

/// readerWithStats reads data from r and tracks the total amount of data read at bytes_read.
#[derive(Default)]
pub struct ReaderWithStats<'a> {
    r: StreamReaderSource<'a>,
    pub bytes_read: u64,

    // PORT NOTE: the source path is cached here, since the in-memory reader
    // computes its path as an owned String while filestream::ReadCloser::path
    // must return a borrowed &str.
    path: String,
}

impl<'a> ReaderWithStats<'a> {
    pub fn reset(&mut self) {
        self.r = StreamReaderSource::None;
        self.bytes_read = 0;
        self.path.clear();
    }

    pub fn init(&mut self, rc: StreamReaderSource<'a>) {
        self.reset();

        self.path = match &rc {
            StreamReaderSource::None => String::new(),
            StreamReaderSource::File(r) => r.path().to_string(),
            StreamReaderSource::Inmemory(r) => r.path(),
        };
        self.r = rc;
    }

    /// Returns the path to r file.
    pub fn path(&self) -> &str {
        match &self.r {
            StreamReaderSource::None => {
                panicf!("BUG: readerWithStats must be initialized before Path() call");
                unreachable!()
            }
            _ => &self.path,
        }
    }

    /// Reads data.len() bytes to r.
    pub fn must_read_full(&mut self, data: &mut [u8]) {
        match &mut self.r {
            StreamReaderSource::None => {
                panicf!("BUG: readerWithStats must be initialized before MustReadFull() call")
            }
            StreamReaderSource::File(r) => fs::must_read_data(r, data),
            StreamReaderSource::Inmemory(r) => {
                // PORT NOTE: mirrors fs::must_read_data (io.ReadFull semantics)
                // for the in-memory reader, which doesn't implement the
                // filestream ReadCloser trait.
                let mut n = 0usize;
                while n < data.len() {
                    match r.read(&mut data[n..]) {
                        Ok(0) => {
                            if n == 0 {
                                break;
                            }
                            panicf!(
                                "FATAL: cannot read {} bytes from {}; read only {} bytes; error: unexpected EOF",
                                data.len(),
                                r.path(),
                                n
                            );
                        }
                        Ok(m) => n += m,
                        Err(err) => {
                            panicf!(
                                "FATAL: cannot read {} bytes from {}; read only {} bytes; error: {}",
                                data.len(),
                                r.path(),
                                n,
                                err
                            );
                        }
                    }
                }
            }
        }
        self.bytes_read += data.len() as u64;
    }

    pub fn must_close(&mut self) {
        match &mut self.r {
            StreamReaderSource::None => {}
            StreamReaderSource::File(r) => r.must_close(),
            StreamReaderSource::Inmemory(r) => r.must_close(),
        }
        self.r = StreamReaderSource::None;
    }
}

impl Read for ReaderWithStats<'_> {
    fn read(&mut self, p: &mut [u8]) -> std::io::Result<usize> {
        let n = match &mut self.r {
            StreamReaderSource::None => {
                panicf!("BUG: readerWithStats must be initialized before Read() call");
                unreachable!()
            }
            StreamReaderSource::File(r) => filestream::ReadCloser::read(r, p)?,
            StreamReaderSource::Inmemory(r) => r.read(p)?,
        };
        self.bytes_read += n as u64;
        Ok(n)
    }
}

impl MustCloser for ReaderWithStats<'_> {
    fn must_close(&mut self) {
        ReaderWithStats::must_close(self)
    }
}

// PORT NOTE: readerWithStats implements Go's filestream.ReadCloser interface
// (it is passed to mustReadColumnNames / mustReadColumnIdxs).
impl filestream::ReadCloser for ReaderWithStats<'_> {
    fn path(&self) -> &str {
        ReaderWithStats::path(self)
    }

    fn read(&mut self, p: &mut [u8]) -> std::io::Result<usize> {
        Read::read(self, p)
    }

    fn must_close(&mut self) {
        ReaderWithStats::must_close(self)
    }
}

#[derive(Default)]
pub struct BloomValuesReader<'a> {
    pub bloom: ReaderWithStats<'a>,
    pub values: ReaderWithStats<'a>,
}

impl<'a> BloomValuesReader<'a> {
    pub fn reset(&mut self) {
        self.bloom.reset();
        self.values.reset();
    }

    pub fn init(&mut self, sr: BloomValuesStreamReader<'a>) {
        self.bloom.init(sr.bloom);
        self.values.init(sr.values);
    }

    pub fn total_bytes_read(&self) -> u64 {
        self.bloom.bytes_read + self.values.bytes_read
    }

    fn append_closers<'b>(&'b mut self, dst: &mut Vec<&'b mut (dyn MustCloser + Send)>) {
        dst.push(&mut self.bloom);
        dst.push(&mut self.values);
    }
}

#[derive(Default)]
pub struct BloomValuesStreamReader<'a> {
    pub bloom: StreamReaderSource<'a>,
    pub values: StreamReaderSource<'a>,
}

/// streamReaders contains readers for blockStreamReader
#[derive(Default)]
pub struct StreamReaders<'a> {
    pub part_format_version: u64,

    pub column_names_reader: ReaderWithStats<'a>,
    pub column_idxs_reader: ReaderWithStats<'a>,
    pub metaindex_reader: ReaderWithStats<'a>,
    pub index_reader: ReaderWithStats<'a>,
    pub columns_header_index_reader: ReaderWithStats<'a>,
    pub columns_header_reader: ReaderWithStats<'a>,
    pub timestamps_reader: ReaderWithStats<'a>,

    pub message_bloom_values_reader: BloomValuesReader<'a>,
    pub old_bloom_values_reader: BloomValuesReader<'a>,
    pub bloom_values_shards: Vec<BloomValuesReader<'a>>,

    /// columnIdxs contains bloomValuesShards indexes for column names seen in the part
    pub column_idxs: HashMap<Arc<str>, u64>,

    /// columnNames contains id->columnName mapping for all the columns seen in the part
    pub column_names: Vec<Arc<str>>,
}

impl<'a> StreamReaders<'a> {
    pub fn reset(&mut self) {
        self.part_format_version = 0;

        self.column_names_reader.reset();
        self.column_idxs_reader.reset();
        self.metaindex_reader.reset();
        self.index_reader.reset();
        self.columns_header_index_reader.reset();
        self.columns_header_reader.reset();
        self.timestamps_reader.reset();

        self.message_bloom_values_reader.reset();
        self.old_bloom_values_reader.reset();
        self.bloom_values_shards.clear();

        // PORT NOTE: Go sets columnIdxs and columnNames to nil; dropping them matches that.
        self.column_idxs = HashMap::new();
        self.column_names = Vec::new();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn init(
        &mut self,
        part_format_version: u64,
        column_names_reader: StreamReaderSource<'a>,
        column_idxs_reader: StreamReaderSource<'a>,
        metaindex_reader: StreamReaderSource<'a>,
        index_reader: StreamReaderSource<'a>,
        columns_header_index_reader: StreamReaderSource<'a>,
        columns_header_reader: StreamReaderSource<'a>,
        timestamps_reader: StreamReaderSource<'a>,
        message_bloom_values_reader: BloomValuesStreamReader<'a>,
        old_bloom_values_reader: BloomValuesStreamReader<'a>,
        bloom_values_shards: Vec<BloomValuesStreamReader<'a>>,
    ) {
        self.part_format_version = part_format_version;

        self.column_names_reader.init(column_names_reader);
        self.column_idxs_reader.init(column_idxs_reader);
        self.metaindex_reader.init(metaindex_reader);
        self.index_reader.init(index_reader);
        self.columns_header_index_reader
            .init(columns_header_index_reader);
        self.columns_header_reader.init(columns_header_reader);
        self.timestamps_reader.init(timestamps_reader);

        self.message_bloom_values_reader
            .init(message_bloom_values_reader);
        self.old_bloom_values_reader.init(old_bloom_values_reader);

        let shards_count = bloom_values_shards.len() as u64;
        self.bloom_values_shards.clear();
        for sr in bloom_values_shards {
            let mut r = BloomValuesReader::default();
            r.init(sr);
            self.bloom_values_shards.push(r);
        }

        if part_format_version >= 1 {
            let (column_names, _) = must_read_column_names(&mut self.column_names_reader);
            self.column_names = column_names;
        }
        if part_format_version >= 3 {
            self.column_idxs = must_read_column_idxs(
                &mut self.column_idxs_reader,
                &self.column_names,
                shards_count,
            );
        }
    }

    pub fn total_bytes_read(&self) -> u64 {
        let mut n = 0u64;

        n += self.column_names_reader.bytes_read;
        n += self.column_idxs_reader.bytes_read;
        n += self.metaindex_reader.bytes_read;
        n += self.index_reader.bytes_read;
        n += self.columns_header_index_reader.bytes_read;
        n += self.columns_header_reader.bytes_read;
        n += self.timestamps_reader.bytes_read;

        n += self.message_bloom_values_reader.total_bytes_read();
        n += self.old_bloom_values_reader.total_bytes_read();
        for shard in &self.bloom_values_shards {
            n += shard.total_bytes_read();
        }

        n
    }

    pub fn must_close(&mut self) {
        // Close files in parallel in order to reduce the time needed for this operation
        // on high-latency storage systems such as NFS or Ceph.
        let mut cs: Vec<&mut (dyn MustCloser + Send)> = vec![
            &mut self.column_names_reader,
            &mut self.column_idxs_reader,
            &mut self.metaindex_reader,
            &mut self.index_reader,
            &mut self.columns_header_index_reader,
            &mut self.columns_header_reader,
            &mut self.timestamps_reader,
        ];

        self.message_bloom_values_reader.append_closers(&mut cs);
        self.old_bloom_values_reader.append_closers(&mut cs);
        for shard in &mut self.bloom_values_shards {
            shard.append_closers(&mut cs);
        }

        fs::must_close_parallel(&mut cs);
    }

    pub fn get_bloom_values_reader_for_column_name(
        &mut self,
        name: &str,
    ) -> &mut BloomValuesReader<'a> {
        if name.is_empty() {
            return &mut self.message_bloom_values_reader;
        }
        if self.part_format_version < 1 {
            return &mut self.old_bloom_values_reader;
        }
        let shard_idx = if self.part_format_version < 3 {
            let n = self.bloom_values_shards.len();
            let mut shard_idx = 0u64;
            if n > 1 {
                let h = xxhash_rust::xxh64::xxh64(name.as_bytes(), 0);
                shard_idx = h % n as u64;
            }
            shard_idx
        } else {
            match self.column_idxs.get(name) {
                Some(&shard_idx) => shard_idx,
                None => {
                    panicf!(
                        "BUG: missing column index for {name:?}; columnIdxs={:?}",
                        self.column_idxs
                    );
                    unreachable!()
                }
            }
        };
        &mut self.bloom_values_shards[shard_idx as usize]
    }
}

/// blockStreamReader is used for reading blocks in streaming manner from a part.
#[derive(Default)]
pub struct BlockStreamReader<'a> {
    /// blockData contains the data for the last read block
    pub block_data: BlockData,

    /// a contains data for blockData
    a: Arena,

    /// ph is the header for the part
    pub ph: PartHeader,

    /// streamReaders contains data readers in stream mode
    pub stream_readers: StreamReaders<'a>,

    /// indexBlockHeaders contains the list of all the indexBlockHeader entries for the part
    index_block_headers: Vec<IndexBlockHeader>,

    /// blockHeaders contains the list of blockHeader entries for the current indexBlockHeader pointed by nextIndexBlockIdx
    block_headers: Vec<BlockHeader>,

    /// nextIndexBlockIdx is the index of the next item to read from indexBlockHeaders
    next_index_block_idx: usize,

    /// nextBlockIdx is the index of the next item to read from blockHeaders
    next_block_idx: usize,

    /// globalUncompressedSizeBytes is the total size of log entries seen in the part
    global_uncompressed_size_bytes: u64,

    /// globalRowsCount is the number of log entries seen in the part
    global_rows_count: u64,

    /// globalBlocksCount is the number of blocks seen in the part
    global_blocks_count: u64,

    /// sidLast is the stream id for the previously read block
    sid_last: StreamID,

    /// minTimestampLast is the minimum timestamp for the previously read block
    min_timestamp_last: i64,
}

impl<'a> BlockStreamReader<'a> {
    /// Resets bsr, so it can be reused.
    pub fn reset(&mut self) {
        self.block_data.reset();
        self.a.reset();
        self.ph.reset();
        self.stream_readers.reset();

        if self.index_block_headers.len() > 10_000 {
            // The ihs len is unbound, so it is better to drop too long indexBlockHeaders in order to reduce memory usage
            self.index_block_headers = Vec::new();
        } else {
            self.index_block_headers.clear();
        }

        reset_block_headers(&mut self.block_headers);

        self.next_index_block_idx = 0;
        self.next_block_idx = 0;
        self.global_uncompressed_size_bytes = 0;
        self.global_rows_count = 0;
        self.global_blocks_count = 0;

        self.sid_last.reset();
        self.min_timestamp_last = 0;
    }

    /// Returns part path for bsr (e.g. file path, url or in-memory reference).
    pub fn path(&self) -> String {
        let path = self.stream_readers.metaindex_reader.path();
        let dir = Path::new(&path)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        if dir.is_empty() {
            // PORT NOTE: Go's filepath.Dir returns "." for paths without a directory.
            return ".".to_string();
        }
        dir
    }

    /// Initializes bsr from mp.
    pub fn must_init_from_inmemory_part(&mut self, mp: &'a InmemoryPart) {
        self.reset();

        self.ph = mp.ph.clone();

        // Initialize streamReaders
        let column_names_reader = StreamReaderSource::Inmemory(mp.column_names.new_reader());
        let column_idxs_reader = StreamReaderSource::Inmemory(mp.column_idxs.new_reader());
        let metaindex_reader = StreamReaderSource::Inmemory(mp.metaindex.new_reader());
        let index_reader = StreamReaderSource::Inmemory(mp.index.new_reader());
        let columns_header_index_reader =
            StreamReaderSource::Inmemory(mp.columns_header_index.new_reader());
        let columns_header_reader = StreamReaderSource::Inmemory(mp.columns_header.new_reader());
        let timestamps_reader = StreamReaderSource::Inmemory(mp.timestamps.new_reader());

        // PORT NOTE: Go's bloomValuesBuffer.NewStreamReader() (inmemory_part.go)
        // is constructed inline here, since the returned type is defined in
        // this module.
        let message_bloom_values_reader = BloomValuesStreamReader {
            bloom: StreamReaderSource::Inmemory(mp.message_bloom_values.bloom.new_reader()),
            values: StreamReaderSource::Inmemory(mp.message_bloom_values.values.new_reader()),
        };
        let old_bloom_values_reader = BloomValuesStreamReader::default();
        let bloom_values_shards = vec![BloomValuesStreamReader {
            bloom: StreamReaderSource::Inmemory(mp.field_bloom_values.bloom.new_reader()),
            values: StreamReaderSource::Inmemory(mp.field_bloom_values.values.new_reader()),
        }];

        self.stream_readers.init(
            self.ph.format_version,
            column_names_reader,
            column_idxs_reader,
            metaindex_reader,
            index_reader,
            columns_header_index_reader,
            columns_header_reader,
            timestamps_reader,
            message_bloom_values_reader,
            old_bloom_values_reader,
            bloom_values_shards,
        );

        // Read metaindex data
        self.index_block_headers.clear();
        must_read_index_block_headers(
            &mut self.index_block_headers,
            &mut self.stream_readers.metaindex_reader,
        );
    }

    /// Initializes bsr from file part at the given path.
    pub fn must_init_from_file_part(&mut self, path: &Path) {
        self.reset();

        // Files in the part are always read without OS cache pollution,
        // since they are usually deleted after the merge.
        const NOCACHE: bool = true;

        self.ph.must_read_metadata(path);

        let column_names_path = path.join(COLUMN_NAMES_FILENAME);
        let column_idxs_path = path.join(COLUMN_IDXS_FILENAME);
        let metaindex_path = path.join(METAINDEX_FILENAME);
        let index_path = path.join(INDEX_FILENAME);
        let columns_header_index_path = path.join(COLUMNS_HEADER_INDEX_FILENAME);
        let columns_header_path = path.join(COLUMNS_HEADER_FILENAME);
        let timestamps_path = path.join(TIMESTAMPS_FILENAME);

        // Open data readers in parallel in order to reduce the time for this operation
        // on high-latency storage systems such as NFS or Ceph.

        let mut column_names_reader = None;
        let mut column_idxs_reader = None;
        let mut metaindex_reader = None;
        let mut index_reader = None;
        let mut columns_header_index_reader = None;
        let mut columns_header_reader = None;
        let mut timestamps_reader = None;
        let mut message_bloom_reader = None;
        let mut message_values_reader = None;
        let mut old_bloom_reader = None;
        let mut old_values_reader = None;
        let mut shard_readers: Vec<(Option<filestream::Reader>, Option<filestream::Reader>)> =
            Vec::new();

        let mut pfo = filestream::ParallelFileOpener::new();

        if self.ph.format_version >= 1 {
            pfo.add(&column_names_path, &mut column_names_reader, NOCACHE);
        }
        if self.ph.format_version >= 3 {
            pfo.add(&column_idxs_path, &mut column_idxs_reader, NOCACHE);
        }

        pfo.add(&metaindex_path, &mut metaindex_reader, NOCACHE);
        pfo.add(&index_path, &mut index_reader, NOCACHE);

        if self.ph.format_version >= 1 {
            pfo.add(
                &columns_header_index_path,
                &mut columns_header_index_reader,
                NOCACHE,
            );
        }

        pfo.add(&columns_header_path, &mut columns_header_reader, NOCACHE);
        pfo.add(&timestamps_path, &mut timestamps_reader, NOCACHE);

        let message_bloom_filter_path = path.join(MESSAGE_BLOOM_FILENAME);
        let message_values_path = path.join(MESSAGE_VALUES_FILENAME);
        pfo.add(
            &message_bloom_filter_path,
            &mut message_bloom_reader,
            NOCACHE,
        );
        pfo.add(&message_values_path, &mut message_values_reader, NOCACHE);

        if self.ph.format_version < 1 {
            let bloom_path = path.join(OLD_BLOOM_FILENAME);
            pfo.add(&bloom_path, &mut old_bloom_reader, NOCACHE);

            let values_path = path.join(OLD_VALUES_FILENAME);
            pfo.add(&values_path, &mut old_values_reader, NOCACHE);
        } else {
            shard_readers.resize_with(self.ph.bloom_values_shards_count as usize, || (None, None));
            for (i, (bloom_reader, values_reader)) in shard_readers.iter_mut().enumerate() {
                let bloom_path = get_bloom_file_path(path, i as u64);
                pfo.add(bloom_path, bloom_reader, NOCACHE);

                let values_path = get_values_file_path(path, i as u64);
                pfo.add(values_path, values_reader, NOCACHE);
            }
        }

        pfo.run();

        let message_bloom_values_reader = BloomValuesStreamReader {
            bloom: file_reader_source(message_bloom_reader),
            values: file_reader_source(message_values_reader),
        };
        let old_bloom_values_reader = BloomValuesStreamReader {
            bloom: file_reader_source(old_bloom_reader),
            values: file_reader_source(old_values_reader),
        };
        let bloom_values_shards = shard_readers
            .into_iter()
            .map(|(bloom, values)| BloomValuesStreamReader {
                bloom: file_reader_source(bloom),
                values: file_reader_source(values),
            })
            .collect();

        // Initialize streamReaders
        self.stream_readers.init(
            self.ph.format_version,
            file_reader_source(column_names_reader),
            file_reader_source(column_idxs_reader),
            file_reader_source(metaindex_reader),
            file_reader_source(index_reader),
            file_reader_source(columns_header_index_reader),
            file_reader_source(columns_header_reader),
            file_reader_source(timestamps_reader),
            message_bloom_values_reader,
            old_bloom_values_reader,
            bloom_values_shards,
        );

        // Read metaindex data
        self.index_block_headers.clear();
        must_read_index_block_headers(
            &mut self.index_block_headers,
            &mut self.stream_readers.metaindex_reader,
        );
    }

    /// Reads the next block from bsr and puts it into bsr.block_data.
    ///
    /// false is returned if there are no other blocks.
    ///
    /// bsr.block_data is valid until the next call to next_block().
    pub fn next_block(&mut self) -> bool {
        while self.next_block_idx >= self.block_headers.len() {
            if !self.next_index_block() {
                return false;
            }
        }
        let ih = &self.index_block_headers[self.next_index_block_idx - 1];
        let bh = &self.block_headers[self.next_block_idx];
        let th = &bh.timestamps_header;

        // Validate bh
        if bh.stream_id.less(&self.sid_last) {
            panicf!(
                "FATAL: {}: blockHeader.streamID={} cannot be smaller than the streamID from the previously read block: {}",
                self.path(),
                bh.stream_id,
                self.sid_last
            );
        }
        if bh.stream_id.equal(&self.sid_last) && th.min_timestamp < self.min_timestamp_last {
            panicf!(
                "FATAL: {}: timestamps.minTimestamp={} cannot be smaller than the minTimestamp for the previously read block for the same streamID: {}",
                self.path(),
                th.min_timestamp,
                self.min_timestamp_last
            );
        }
        self.min_timestamp_last = th.min_timestamp;
        self.sid_last = bh.stream_id;
        if th.min_timestamp < ih.min_timestamp {
            panicf!(
                "FATAL: {}: timestampsHeader.minTimestamp={} cannot be smaller than indexBlockHeader.minTimestamp={}",
                self.path(),
                th.min_timestamp,
                ih.min_timestamp
            );
        }
        if th.max_timestamp > ih.max_timestamp {
            // PORT NOTE: Go prints ih.minTimestamp here (sic); the typo is
            // preserved for error message parity.
            panicf!(
                "FATAL: {}: timestampsHeader.maxTimestamp={} cannot be bigger than indexBlockHeader.maxTimestamp={}",
                self.path(),
                th.max_timestamp,
                ih.min_timestamp
            );
        }

        // Read bsr.blockData
        self.a.reset();
        let bh = &self.block_headers[self.next_block_idx];
        self.block_data
            .must_read_from(&mut self.a, bh, &mut self.stream_readers);

        self.global_uncompressed_size_bytes += bh.uncompressed_size_bytes;
        self.global_rows_count += bh.rows_count;
        self.global_blocks_count += 1;
        if self.global_uncompressed_size_bytes > self.ph.uncompressed_size_bytes {
            panicf!(
                "FATAL: {}: too big size of entries read: {}; mustn't exceed partHeader.UncompressedSizeBytes={}",
                self.path(),
                self.global_uncompressed_size_bytes,
                self.ph.uncompressed_size_bytes
            );
        }
        if self.global_rows_count > self.ph.rows_count {
            panicf!(
                "FATAL: {}: too many log entries read so far: {}; mustn't exceed partHeader.RowsCount={}",
                self.path(),
                self.global_rows_count,
                self.ph.rows_count
            );
        }
        if self.global_blocks_count > self.ph.blocks_count {
            panicf!(
                "FATAL: {}: too many blocks read so far: {}; mustn't exceed partHeader.BlocksCount={}",
                self.path(),
                self.global_blocks_count,
                self.ph.blocks_count
            );
        }

        // The block has been successfully read
        self.next_block_idx += 1;
        true
    }

    fn next_index_block(&mut self) -> bool {
        // Advance to the next indexBlockHeader
        if self.next_index_block_idx >= self.index_block_headers.len() {
            // No more blocks left
            // Validate bsr.ph
            let total_bytes_read = self.stream_readers.total_bytes_read();
            if self.ph.compressed_size_bytes != total_bytes_read {
                panicf!(
                    "FATAL: {}: partHeader.CompressedSizeBytes={} must match the size of data read: {}",
                    self.path(),
                    self.ph.compressed_size_bytes,
                    total_bytes_read
                );
            }
            if self.ph.uncompressed_size_bytes != self.global_uncompressed_size_bytes {
                panicf!(
                    "FATAL: {}: partHeader.UncompressedSizeBytes={} must match the size of entries read: {}",
                    self.path(),
                    self.ph.uncompressed_size_bytes,
                    self.global_uncompressed_size_bytes
                );
            }
            if self.ph.rows_count != self.global_rows_count {
                panicf!(
                    "FATAL: {}: partHeader.RowsCount={} must match the number of log entries read: {}",
                    self.path(),
                    self.ph.rows_count,
                    self.global_rows_count
                );
            }
            if self.ph.blocks_count != self.global_blocks_count {
                panicf!(
                    "FATAL: {}: partHeader.BlocksCount={} must match the number of blocks read: {}",
                    self.path(),
                    self.ph.blocks_count,
                    self.global_blocks_count
                );
            }
            return false;
        }
        let ih = &self.index_block_headers[self.next_index_block_idx];

        // Validate ih
        if ih.min_timestamp < self.ph.min_timestamp {
            panicf!(
                "FATAL: {}: indexBlockHeader.minTimestamp={} cannot be smaller than partHeader.MinTimestamp={}",
                self.stream_readers.metaindex_reader.path(),
                ih.min_timestamp,
                self.ph.min_timestamp
            );
        }
        if ih.max_timestamp > self.ph.max_timestamp {
            panicf!(
                "FATAL: {}: indexBlockHeader.maxTimestamp={} cannot be bigger than partHeader.MaxTimestamp={}",
                self.stream_readers.metaindex_reader.path(),
                ih.max_timestamp,
                self.ph.max_timestamp
            );
        }

        // Read indexBlock for the given ih
        let mut bb = LONG_TERM_BUF_POOL.get();
        ih.must_read_next_index_block(&mut bb.b, &mut self.stream_readers);
        reset_block_headers(&mut self.block_headers);
        let res = unmarshal_block_headers(&mut self.block_headers, &bb.b, self.ph.format_version);
        LONG_TERM_BUF_POOL.put(bb);
        if let Err(err) = res {
            panicf!(
                "FATAL: {}: cannot unmarshal blockHeader entries: {}",
                self.stream_readers.index_reader.path(),
                err
            );
        }

        self.next_index_block_idx += 1;
        self.next_block_idx = 0;
        true
    }

    /// Closes bsr.
    pub fn must_close(&mut self) {
        self.stream_readers.must_close();
        self.reset();
    }

    /// Converts bsr into a lifetime-erased value for pooling.
    ///
    /// PORT NOTE: Go pools `*blockStreamReader` directly via sync.Pool; the
    /// Rust type is parameterized by the borrow of the in-memory part, so the
    /// pool stores the `'static` instantiation. reset() drops all the borrows
    /// and the owned buffers are moved over, preserving Go's buffer reuse.
    fn into_static(mut self) -> BlockStreamReader<'static> {
        self.reset();
        BlockStreamReader {
            block_data: self.block_data,
            a: self.a,
            ph: self.ph,
            stream_readers: StreamReaders::default(),
            index_block_headers: self.index_block_headers,
            block_headers: self.block_headers,
            next_index_block_idx: self.next_index_block_idx,
            next_block_idx: self.next_block_idx,
            global_uncompressed_size_bytes: self.global_uncompressed_size_bytes,
            global_rows_count: self.global_rows_count,
            global_blocks_count: self.global_blocks_count,
            sid_last: self.sid_last,
            min_timestamp_last: self.min_timestamp_last,
        }
    }
}

fn file_reader_source(r: Option<filestream::Reader>) -> StreamReaderSource<'static> {
    match r {
        Some(r) => StreamReaderSource::File(r),
        None => StreamReaderSource::None,
    }
}

static BLOCK_STREAM_READER_POOL: Mutex<Vec<BlockStreamReader<'static>>> = Mutex::new(Vec::new());

/// Returns blockStreamReader.
///
/// The returned blockStreamReader must be initialized with must_init_from_*().
/// Call put_block_stream_reader() when the returned blockStreamReader is no longer needed.
pub fn get_block_stream_reader<'a>() -> BlockStreamReader<'a> {
    BLOCK_STREAM_READER_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_default()
}

/// Returns bsr to the pool.
///
/// bsr cannot be used after returning to the pool.
pub fn put_block_stream_reader(bsr: BlockStreamReader<'_>) {
    BLOCK_STREAM_READER_POOL
        .lock()
        .unwrap()
        .push(bsr.into_static());
}

/// Calls must_close() on the given bsrs.
pub fn must_close_block_stream_readers(bsrs: &mut [BlockStreamReader<'_>]) {
    for bsr in bsrs.iter_mut() {
        bsr.must_close();
    }
}

// ---------------------------------------------------------------------------
// Port of lib/logstorage/index_block_header.go
// ---------------------------------------------------------------------------

/// indexBlockHeader contains index information about multiple blocks.
///
/// It allows locating the block by streamID and/or by time range.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct IndexBlockHeader {
    /// streamID is the minimum streamID covered by the indexBlockHeader
    pub stream_id: StreamID,

    /// minTimestamp is the minimum timestamp seen across blocks covered by the indexBlockHeader
    pub min_timestamp: i64,

    /// maxTimestamp is the maximum timestamp seen across blocks covered by the indexBlockHeader
    pub max_timestamp: i64,

    /// indexBlockOffset is an offset of the linked index block at indexFilename
    pub index_block_offset: u64,

    /// indexBlockSize is the size of the linked index block at indexFilename
    pub index_block_size: u64,
}

impl IndexBlockHeader {
    /// Resets ih for subsequent reuse.
    pub fn reset(&mut self) {
        self.stream_id.reset();
        self.min_timestamp = 0;
        self.max_timestamp = 0;
        self.index_block_offset = 0;
        self.index_block_size = 0;
    }

    /// Writes data with the given additional args to sw and updates ih accordingly.
    pub fn must_write_index_block(
        &mut self,
        data: &[u8],
        sid_first: StreamID,
        min_timestamp: i64,
        max_timestamp: i64,
        sw: &mut StreamWriters,
    ) {
        self.stream_id = sid_first;
        self.min_timestamp = min_timestamp;
        self.max_timestamp = max_timestamp;

        let mut bb = LONG_TERM_BUF_POOL.get();
        encoding::compress_zstd_level(&mut bb.b, data, 1);
        self.index_block_offset = sw.index_writer.bytes_written;
        self.index_block_size = bb.b.len() as u64;
        sw.index_writer.must_write(&bb.b);
        LONG_TERM_BUF_POOL.put(bb);
    }

    /// Reads the next index block associated with ih from sr and appends it to dst.
    pub fn must_read_next_index_block(&self, dst: &mut Vec<u8>, sr: &mut StreamReaders) {
        let index_reader = &mut sr.index_reader;

        let index_block_size = self.index_block_size;
        if index_block_size > MAX_INDEX_BLOCK_SIZE as u64 {
            panicf!(
                "FATAL: {}: indexBlockHeader.indexBlockSize={} cannot exceed {} bytes",
                index_reader.path(),
                index_block_size,
                MAX_INDEX_BLOCK_SIZE
            );
        }
        if self.index_block_offset != index_reader.bytes_read {
            panicf!(
                "FATAL: {}: indexBlockHeader.indexBlockOffset={} must equal to {}",
                index_reader.path(),
                self.index_block_offset,
                index_reader.bytes_read
            );
        }
        let mut bb_compressed = LONG_TERM_BUF_POOL.get();
        bytesutil::resize_no_copy_may_overallocate(&mut bb_compressed.b, index_block_size as usize);
        index_reader.must_read_full(&mut bb_compressed.b);

        // Decompress bbCompressed to dst
        let res = encoding::decompress_zstd(dst, &bb_compressed.b);
        LONG_TERM_BUF_POOL.put(bb_compressed);
        if let Err(err) = res {
            panicf!(
                "FATAL: {}: cannot decompress indexBlock read at offset {} with size {}: {}",
                sr.index_reader.path(),
                self.index_block_offset,
                index_block_size,
                err
            );
        }
    }

    /// Appends marshaled ih to dst.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        self.stream_id.marshal(dst);
        encoding::marshal_uint64(dst, self.min_timestamp as u64);
        encoding::marshal_uint64(dst, self.max_timestamp as u64);
        encoding::marshal_uint64(dst, self.index_block_offset);
        encoding::marshal_uint64(dst, self.index_block_size);
    }

    /// Unmarshals ih from src and returns the tail left.
    pub fn unmarshal<'s>(&mut self, src: &'s [u8]) -> Result<&'s [u8], String> {
        // unmarshal ih.streamID
        let src = self
            .stream_id
            .unmarshal(src)
            .map_err(|err| format!("cannot unmarshal streamID: {err}"))?;

        // unmarshal the rest of indexBlockHeader fields
        if src.len() < 32 {
            return Err(format!(
                "cannot unmarshal indexBlockHeader from {} bytes; need at least 32 bytes",
                src.len()
            ));
        }
        self.min_timestamp = encoding::unmarshal_uint64(src) as i64;
        self.max_timestamp = encoding::unmarshal_uint64(&src[8..]) as i64;
        self.index_block_offset = encoding::unmarshal_uint64(&src[16..]);
        self.index_block_size = encoding::unmarshal_uint64(&src[24..]);

        Ok(&src[32..])
    }
}

/// Writes metaindexData to w.
pub fn must_write_index_block_headers(w: &mut WriterWithStats, metaindex_data: &[u8]) {
    let mut bb = LONG_TERM_BUF_POOL.get();
    encoding::compress_zstd_level(&mut bb.b, metaindex_data, 1);
    w.must_write(&bb.b);
    if bb.b.len() < 1024 * 1024 {
        LONG_TERM_BUF_POOL.put(bb);
    }
}

/// Reads indexBlockHeader entries from r and appends them to dst.
pub fn must_read_index_block_headers(dst: &mut Vec<IndexBlockHeader>, r: &mut ReaderWithStats) {
    let mut data = Vec::new();
    if let Err(err) = r.read_to_end(&mut data) {
        panicf!(
            "FATAL: {}: cannot read indexBlockHeader entries: {}",
            r.path(),
            err
        );
    }

    let mut bb = LONG_TERM_BUF_POOL.get();
    if let Err(err) = encoding::decompress_zstd(&mut bb.b, &data) {
        panicf!(
            "FATAL: {}: cannot decompress indexBlockHeader entries: {}",
            r.path(),
            err
        );
    }
    let res = unmarshal_index_block_headers(dst, &bb.b);
    if bb.b.len() < 1024 * 1024 {
        LONG_TERM_BUF_POOL.put(bb);
    }
    if let Err(err) = res {
        panicf!(
            "FATAL: {}: cannot parse indexBlockHeader entries: {}",
            r.path(),
            err
        );
    }
}

/// Appends unmarshaled from src indexBlockHeader entries to dst.
///
/// PORT NOTE: Go's signature is `(dst []indexBlockHeader, src []byte)
/// ([]indexBlockHeader, error)` with reuse of spare slice capacity; the port
/// appends to `&mut Vec` (which reuses its own capacity). Like in Go, entries
/// appended before an error are discarded (dst is truncated back).
pub fn unmarshal_index_block_headers(
    dst: &mut Vec<IndexBlockHeader>,
    src: &[u8],
) -> Result<(), String> {
    let dst_len = dst.len();
    let mut src = src;
    while !src.is_empty() {
        dst.push(IndexBlockHeader::default());
        let ih = dst.last_mut().unwrap();
        match ih.unmarshal(src) {
            Ok(tail) => src = tail,
            Err(err) => {
                let err = format!(
                    "cannot unmarshal indexBlockHeader {}: {}",
                    dst.len() - 1 - dst_len,
                    err
                );
                dst.truncate(dst_len);
                return Err(err);
            }
        }
    }
    if let Err(err) = validate_index_block_headers(&dst[dst_len..]) {
        dst.truncate(dst_len);
        return Err(err);
    }
    Ok(())
}

fn validate_index_block_headers(ihs: &[IndexBlockHeader]) -> Result<(), String> {
    for i in 1..ihs.len() {
        if ihs[i].stream_id.less(&ihs[i - 1].stream_id) {
            return Err(format!(
                "unexpected indexBlockHeader with smaller streamID={} after bigger streamID={}",
                ihs[i].stream_id,
                ihs[i - 1].stream_id
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant_id::TenantID;
    use crate::u128::U128;

    fn new_test_index_block_header() -> IndexBlockHeader {
        IndexBlockHeader {
            stream_id: StreamID {
                tenant_id: TenantID {
                    account_id: 123,
                    project_id: 456,
                },
                id: U128 { hi: 214, lo: 2111 },
            },
            min_timestamp: 1234,
            max_timestamp: 898943,
            index_block_offset: 234,
            index_block_size: 898,
        }
    }

    #[test]
    fn test_index_block_header_marshal_unmarshal() {
        fn f(ih: &IndexBlockHeader, marshaled_len: usize) {
            let mut data = Vec::new();
            ih.marshal(&mut data);
            assert_eq!(
                data.len(),
                marshaled_len,
                "unexpected marshaled length of indexBlockHeader"
            );
            let mut ih2 = IndexBlockHeader::default();
            let tail = ih2
                .unmarshal(&data)
                .expect("cannot unmarshal indexBlockHeader");
            assert!(
                tail.is_empty(),
                "unexpected non-empty tail left after unmarshaling indexBlockHeader: {tail:X?}"
            );
            assert_eq!(ih, &ih2, "unexpected unmarshaled indexBlockHeader");
        }
        f(&IndexBlockHeader::default(), 56);
        f(&new_test_index_block_header(), 56);
    }

    #[test]
    fn test_index_block_header_unmarshal_failure() {
        // PORT NOTE: the Go test also verifies that the returned tail equals
        // the original data on failure; the Rust unmarshal returns Err without
        // a tail, so only the error is checked.
        fn f(data: &[u8]) {
            let mut ih = IndexBlockHeader::default();
            assert!(ih.unmarshal(data).is_err(), "expecting non-nil error");
        }
        f(&[]);
        f(b"foo");

        let ih = new_test_index_block_header();
        let mut data = Vec::new();
        ih.marshal(&mut data);
        while !data.is_empty() {
            data.truncate(data.len() - 1);
            f(&data);
        }
    }

    #[test]
    fn test_index_block_header_reset() {
        let mut ih = new_test_index_block_header();
        ih.reset();
        let ih_zero = IndexBlockHeader::default();
        assert_eq!(
            ih, ih_zero,
            "unexpected non-zero indexBlockHeader after reset"
        );
    }

    #[test]
    fn test_marshal_unmarshal_index_block_headers() {
        fn f(ihs: &[IndexBlockHeader], marshaled_len: usize) {
            let mut data = Vec::new();
            for ih in ihs {
                ih.marshal(&mut data);
            }
            assert_eq!(
                data.len(),
                marshaled_len,
                "unexpected marshaled length for indexBlockHeader entries"
            );
            let mut ihs2 = Vec::new();
            unmarshal_index_block_headers(&mut ihs2, &data)
                .expect("cannot unmarshal indexBlockHeader entries");
            assert_eq!(
                ihs,
                &ihs2[..],
                "unexpected indexBlockHeader entries after unmarshaling"
            );
        }
        f(&[], 0);
        f(&[IndexBlockHeader::default()], 56);
        f(
            &[
                IndexBlockHeader {
                    index_block_offset: 234,
                    index_block_size: 5432,
                    ..Default::default()
                },
                IndexBlockHeader {
                    min_timestamp: -123,
                    ..Default::default()
                },
            ],
            112,
        );
    }
}

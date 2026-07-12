//! Port of EsLogs `lib/logstorage/block_stream_writer.go`.
//!
//! PORT NOTE: Go's `streamWriters` hold `filestream.WriteCloser` interface
//! values which are either `*filestream.Writer` (file parts) or
//! `*chunkedbuffer.Buffer` (in-memory parts). The port uses the
//! [`StreamWriterSource`] enum instead of a trait object, mirroring the two
//! concrete implementations Go actually uses.
//!
//! PORT NOTE: Go's `columnsHeader.mustWriteTo(bh, sw)` (block_header.go) is
//! ported here as [`must_write_columns_header`], since `streamWriters` and
//! `longTermBufPool` are defined in this module.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use esl_common::bytesutil::ByteBufferPool;
use esl_common::fs::MustCloser;
use esl_common::{chunkedbuffer, filestream, fs, panicf};

use crate::block::{Block, get_block, put_block};
use crate::block_data::BlockData;
use crate::block_header::{
    BlockHeader, ColumnsHeader, get_block_header, get_columns_header_index, put_block_header,
    put_columns_header_index,
};
use crate::block_stream_reader::{IndexBlockHeader, must_write_index_block_headers};
use crate::column_names::{ColumnNameIDGenerator, must_write_column_idxs, must_write_column_names};
use crate::consts::{
    BLOOM_VALUES_MAX_SHARDS_COUNT, MAX_COLUMNS_HEADER_INDEX_SIZE, MAX_COLUMNS_HEADER_SIZE,
    MAX_UNCOMPRESSED_INDEX_BLOCK_SIZE, PART_FORMAT_LATEST_VERSION,
};
use crate::filenames::{
    COLUMN_IDXS_FILENAME, COLUMN_NAMES_FILENAME, COLUMNS_HEADER_FILENAME,
    COLUMNS_HEADER_INDEX_FILENAME, INDEX_FILENAME, MESSAGE_BLOOM_FILENAME, MESSAGE_VALUES_FILENAME,
    METAINDEX_FILENAME, TIMESTAMPS_FILENAME,
};
use crate::inmemory_part::{BloomValuesBuffer, InmemoryPart};
use crate::part::{get_bloom_file_path, get_values_file_path};
use crate::part_header::PartHeader;
use crate::rows::Field;
use crate::stream_id::StreamID;

/// The Go package-level `longTermBufPool` (defined in block_stream_writer.go).
///
/// It is used by index block, columns header and block data readers/writers
/// across the whole part read/write pipeline.
pub static LONG_TERM_BUF_POOL: ByteBufferPool = ByteBufferPool::new();

/// The underlying destination for a part output stream.
///
/// PORT NOTE: stands in for Go's `filestream.WriteCloser` interface; the
/// `None` variant corresponds to Go's nil interface value.
#[derive(Default)]
pub enum StreamWriterSource<'a> {
    #[default]
    None,
    File(filestream::Writer),
    Inmemory(&'a mut chunkedbuffer::Buffer),
}

/// writerWithStats writes data to w and tracks the total amounts of data written at bytes_written.
#[derive(Default)]
pub struct WriterWithStats<'a> {
    w: StreamWriterSource<'a>,
    pub bytes_written: u64,
}

impl<'a> WriterWithStats<'a> {
    pub fn reset(&mut self) {
        self.w = StreamWriterSource::None;
        self.bytes_written = 0;
    }

    pub fn init(&mut self, wc: StreamWriterSource<'a>) {
        self.reset();

        self.w = wc;
    }

    pub fn path(&self) -> String {
        match &self.w {
            StreamWriterSource::None => {
                panicf!("BUG: writerWithStats must be initialized before Path() call");
                unreachable!()
            }
            StreamWriterSource::File(w) => w.path().to_string(),
            StreamWriterSource::Inmemory(cb) => cb.path(),
        }
    }

    pub fn must_write(&mut self, data: &[u8]) {
        match &mut self.w {
            StreamWriterSource::None => {
                panicf!("BUG: writerWithStats must be initialized before MustWrite() call")
            }
            StreamWriterSource::File(w) => fs::must_write_data(w, data),
            StreamWriterSource::Inmemory(cb) => cb.must_write(data),
        }
        self.bytes_written += data.len() as u64;
    }

    /// Closes the underlying w.
    pub fn must_close(&mut self) {
        match &mut self.w {
            StreamWriterSource::None => {
                panicf!("BUG: writerWithStats must be initialized before MustClose() call")
            }
            StreamWriterSource::File(w) => w.must_close(),
            StreamWriterSource::Inmemory(cb) => cb.must_close(),
        }
    }
}

impl MustCloser for WriterWithStats<'_> {
    fn must_close(&mut self) {
        WriterWithStats::must_close(self)
    }
}

#[derive(Default)]
pub struct BloomValuesWriter<'a> {
    pub bloom: WriterWithStats<'a>,
    pub values: WriterWithStats<'a>,
}

impl<'a> BloomValuesWriter<'a> {
    pub fn reset(&mut self) {
        self.bloom.reset();
        self.values.reset();
    }

    pub fn init(&mut self, sw: BloomValuesStreamWriter<'a>) {
        self.bloom.init(sw.bloom);
        self.values.init(sw.values);
    }

    pub fn total_bytes_written(&self) -> u64 {
        self.bloom.bytes_written + self.values.bytes_written
    }

    fn append_closers<'b>(&'b mut self, dst: &mut Vec<&'b mut (dyn MustCloser + Send)>) {
        dst.push(&mut self.bloom);
        dst.push(&mut self.values);
    }
}

pub struct BloomValuesStreamWriter<'a> {
    pub bloom: StreamWriterSource<'a>,
    pub values: StreamWriterSource<'a>,
}

/// Creates (bloom, values) writer pairs for new shards.
///
/// PORT NOTE: Go stores a `createBloomValuesWriter func(shardIdx)` closure; a
/// borrowing Rust closure cannot hand out its captured `&mut` more than once,
/// so the port uses this enum with the two factory kinds Go actually creates.
/// The Inmemory variant can only create a single shard, matching Go's usage
/// (`maxShards` is always 1 for in-memory parts).
#[derive(Default)]
pub enum BloomValuesWriterFactory<'a> {
    #[default]
    None,
    Inmemory(Option<&'a mut BloomValuesBuffer>),
    File {
        path: PathBuf,
        nocache: bool,
    },
}

impl<'a> BloomValuesWriterFactory<'a> {
    fn create(&mut self, shard_idx: u64) -> BloomValuesStreamWriter<'a> {
        match self {
            BloomValuesWriterFactory::None => {
                panicf!("BUG: createBloomValuesWriter must be set before use");
                unreachable!()
            }
            BloomValuesWriterFactory::Inmemory(bvb) => {
                let bvb = match bvb.take() {
                    Some(bvb) => bvb,
                    None => {
                        panicf!("BUG: the in-memory bloomValues writer can be created only once");
                        unreachable!()
                    }
                };
                BloomValuesStreamWriter {
                    bloom: StreamWriterSource::Inmemory(&mut bvb.bloom),
                    values: StreamWriterSource::Inmemory(&mut bvb.values),
                }
            }
            BloomValuesWriterFactory::File { path, nocache } => {
                let bloom_path = get_bloom_file_path(path, shard_idx);
                let values_path = get_values_file_path(path, shard_idx);

                BloomValuesStreamWriter {
                    bloom: StreamWriterSource::File(filestream::must_create(&bloom_path, *nocache)),
                    values: StreamWriterSource::File(filestream::must_create(
                        &values_path,
                        *nocache,
                    )),
                }
            }
        }
    }
}

/// streamWriters contain writers for blockStreamWriter
#[derive(Default)]
pub struct StreamWriters<'a> {
    pub column_names_writer: WriterWithStats<'a>,
    pub column_idxs_writer: WriterWithStats<'a>,
    pub metaindex_writer: WriterWithStats<'a>,
    pub index_writer: WriterWithStats<'a>,
    pub columns_header_index_writer: WriterWithStats<'a>,
    pub columns_header_writer: WriterWithStats<'a>,
    pub timestamps_writer: WriterWithStats<'a>,

    pub message_bloom_values_writer: BloomValuesWriter<'a>,

    pub bloom_values_shards: Vec<BloomValuesWriter<'a>>,
    create_bloom_values_writer: BloomValuesWriterFactory<'a>,
    max_shards: u64,

    /// columnNameIDGenerator is used for generating columnName->id mapping for all the columns seen in bsw
    pub column_name_id_generator: ColumnNameIDGenerator,

    pub column_idxs: HashMap<u64, u64>,
    next_column_idx: u64,
}

impl<'a> StreamWriters<'a> {
    pub fn reset(&mut self) {
        self.column_names_writer.reset();
        self.column_idxs_writer.reset();
        self.metaindex_writer.reset();
        self.index_writer.reset();
        self.columns_header_index_writer.reset();
        self.columns_header_writer.reset();
        self.timestamps_writer.reset();

        self.message_bloom_values_writer.reset();
        self.bloom_values_shards.clear();

        self.create_bloom_values_writer = BloomValuesWriterFactory::None;
        self.max_shards = 0;

        self.column_name_id_generator.reset();
        // PORT NOTE: Go sets columnIdxs to nil; dropping the map matches that.
        self.column_idxs = HashMap::new();
        self.next_column_idx = 0;
    }

    #[allow(clippy::too_many_arguments)]
    pub fn init(
        &mut self,
        column_names_writer: StreamWriterSource<'a>,
        column_idxs_writer: StreamWriterSource<'a>,
        metaindex_writer: StreamWriterSource<'a>,
        index_writer: StreamWriterSource<'a>,
        columns_header_index_writer: StreamWriterSource<'a>,
        columns_header_writer: StreamWriterSource<'a>,
        timestamps_writer: StreamWriterSource<'a>,
        message_bloom_values_writer: BloomValuesStreamWriter<'a>,
        create_bloom_values_writer: BloomValuesWriterFactory<'a>,
        max_shards: u64,
    ) {
        self.column_names_writer.init(column_names_writer);
        self.column_idxs_writer.init(column_idxs_writer);
        self.metaindex_writer.init(metaindex_writer);
        self.index_writer.init(index_writer);
        self.columns_header_index_writer
            .init(columns_header_index_writer);
        self.columns_header_writer.init(columns_header_writer);
        self.timestamps_writer.init(timestamps_writer);

        self.message_bloom_values_writer
            .init(message_bloom_values_writer);

        self.create_bloom_values_writer = create_bloom_values_writer;
        self.max_shards = max_shards;
    }

    pub fn total_bytes_written(&self) -> u64 {
        let mut n = 0u64;

        n += self.column_names_writer.bytes_written;
        n += self.column_idxs_writer.bytes_written;
        n += self.metaindex_writer.bytes_written;
        n += self.index_writer.bytes_written;
        n += self.columns_header_index_writer.bytes_written;
        n += self.columns_header_writer.bytes_written;
        n += self.timestamps_writer.bytes_written;

        n += self.message_bloom_values_writer.total_bytes_written();
        for shard in &self.bloom_values_shards {
            n += shard.total_bytes_written();
        }

        n
    }

    pub fn must_close(&mut self) {
        // Flush and close files in parallel in order to reduce the time needed for this operation
        // on high-latency storage systems such as NFS or Ceph.
        let mut cs: Vec<&mut (dyn MustCloser + Send)> = vec![
            &mut self.column_names_writer,
            &mut self.column_idxs_writer,
            &mut self.metaindex_writer,
            &mut self.index_writer,
            &mut self.columns_header_index_writer,
            &mut self.columns_header_writer,
            &mut self.timestamps_writer,
        ];

        self.message_bloom_values_writer.append_closers(&mut cs);
        for shard in &mut self.bloom_values_shards {
            shard.append_closers(&mut cs);
        }

        fs::must_close_parallel(&mut cs);
    }

    pub fn get_bloom_values_writer_for_column_name(
        &mut self,
        name: &str,
    ) -> &mut BloomValuesWriter<'a> {
        if name.is_empty() {
            return &mut self.message_bloom_values_writer;
        }

        let column_id = self.column_name_id_generator.get_column_name_id(name);
        let shard_idx = match self.column_idxs.get(&column_id) {
            Some(&shard_idx) => shard_idx,
            None => {
                let shard_idx = self.next_column_idx % self.max_shards;
                self.next_column_idx += 1;

                self.column_idxs.insert(column_id, shard_idx);

                if shard_idx >= self.bloom_values_shards.len() as u64 {
                    if shard_idx > self.bloom_values_shards.len() as u64 {
                        panicf!(
                            "BUG: shardIdx must equal {}; got {}; maxShards={}; columnIdxs={:?}",
                            self.bloom_values_shards.len(),
                            shard_idx,
                            self.max_shards,
                            self.column_idxs
                        );
                    }
                    let sws = self.create_bloom_values_writer.create(shard_idx);
                    let mut w = BloomValuesWriter::default();
                    w.init(sws);
                    self.bloom_values_shards.push(w);
                }
                shard_idx
            }
        };
        &mut self.bloom_values_shards[shard_idx as usize]
    }
}

/// Port of Go's `columnsHeader.mustWriteTo(bh, sw)` from block_header.go.
///
/// PORT NOTE: it lives here (as a free function) because `StreamWriters` and
/// `LONG_TERM_BUF_POOL` are defined in this module.
pub fn must_write_columns_header(
    csh: &ColumnsHeader,
    bh: &mut BlockHeader,
    sw: &mut StreamWriters,
) {
    let mut bb = LONG_TERM_BUF_POOL.get();

    let mut csh_index = get_columns_header_index();

    csh.marshal(&mut bb.b, &mut csh_index, &mut sw.column_name_id_generator);
    let columns_header_data_len = bb.b.len();

    csh_index.marshal(&mut bb.b);

    put_columns_header_index(csh_index);

    bh.columns_header_index_offset = sw.columns_header_index_writer.bytes_written;
    bh.columns_header_index_size = (bb.b.len() - columns_header_data_len) as u64;
    if bh.columns_header_index_size > MAX_COLUMNS_HEADER_INDEX_SIZE as u64 {
        panicf!(
            "BUG: too big columnsHeaderIndexSize: {} bytes; mustn't exceed {} bytes",
            bh.columns_header_index_size,
            MAX_COLUMNS_HEADER_INDEX_SIZE
        );
    }
    sw.columns_header_index_writer
        .must_write(&bb.b[columns_header_data_len..]);

    bh.columns_header_offset = sw.columns_header_writer.bytes_written;
    bh.columns_header_size = columns_header_data_len as u64;
    if bh.columns_header_size > MAX_COLUMNS_HEADER_SIZE as u64 {
        panicf!(
            "BUG: too big columnsHeaderSize: {} bytes; mustn't exceed {} bytes",
            bh.columns_header_size,
            MAX_COLUMNS_HEADER_SIZE
        );
    }
    sw.columns_header_writer
        .must_write(&bb.b[..columns_header_data_len]);

    LONG_TERM_BUF_POOL.put(bb);
}

/// blockStreamWriter is used for writing blocks into the underlying storage in streaming manner.
#[derive(Default)]
pub struct BlockStreamWriter<'a> {
    /// streamWriters contains writer for block data
    pub stream_writers: StreamWriters<'a>,

    /// sidLast is the streamID for the last written block
    sid_last: StreamID,

    /// sidFirst is the streamID for the first block in the current indexBlock
    sid_first: StreamID,

    /// minTimestampLast is the minimum timestamp seen for the last written block
    min_timestamp_last: i64,

    /// minTimestamp is the minimum timestamp seen across written blocks for the current indexBlock
    min_timestamp: i64,

    /// maxTimestamp is the maximum timestamp seen across written blocks for the current indexBlock
    max_timestamp: i64,

    /// hasWrittenBlocks is set to true if at least a single block is written to the current indexBlock
    has_written_blocks: bool,

    /// globalUncompressedSizeBytes is the total size of all the log entries written via bsw
    global_uncompressed_size_bytes: u64,

    /// globalRowsCount is the total number of log entries written via bsw
    global_rows_count: u64,

    /// globalBlocksCount is the total number of blocks written to bsw
    global_blocks_count: u64,

    /// globalMinTimestamp is the minimum timestamp seen across all the blocks written to bsw
    global_min_timestamp: i64,

    /// globalMaxTimestamp is the maximum timestamp seen across all the blocks written to bsw
    global_max_timestamp: i64,

    /// indexBlockData contains marshaled blockHeader data, which isn't written yet to indexFilename
    index_block_data: Vec<u8>,

    /// metaindexData contains marshaled indexBlockHeader data, which isn't written yet to metaindexFilename
    metaindex_data: Vec<u8>,

    /// indexBlockHeader is used for marshaling the data to metaindexData
    index_block_header: IndexBlockHeader,
}

impl<'a> BlockStreamWriter<'a> {
    /// Resets bsw for subsequent reuse.
    pub fn reset(&mut self) {
        self.stream_writers.reset();
        self.sid_last.reset();
        self.sid_first.reset();
        self.min_timestamp_last = 0;
        self.min_timestamp = 0;
        self.max_timestamp = 0;
        self.has_written_blocks = false;
        self.global_uncompressed_size_bytes = 0;
        self.global_rows_count = 0;
        self.global_blocks_count = 0;
        self.global_min_timestamp = 0;
        self.global_max_timestamp = 0;
        self.index_block_data.clear();

        if self.metaindex_data.len() > 1024 * 1024 {
            // The length of bsw.metaindexData is unbound, so drop too long buffer
            // in order to conserve memory.
            self.metaindex_data = Vec::new();
        } else {
            self.metaindex_data.clear();
        }

        self.index_block_header.reset();
    }

    /// Initializes bsw from mp
    pub fn must_init_for_inmemory_part(&mut self, mp: &'a mut InmemoryPart) {
        self.reset();

        let message_bloom_values = BloomValuesStreamWriter {
            bloom: StreamWriterSource::Inmemory(&mut mp.message_bloom_values.bloom),
            values: StreamWriterSource::Inmemory(&mut mp.message_bloom_values.values),
        };
        let create_bloom_values_writer =
            BloomValuesWriterFactory::Inmemory(Some(&mut mp.field_bloom_values));

        self.stream_writers.init(
            StreamWriterSource::Inmemory(&mut mp.column_names),
            StreamWriterSource::Inmemory(&mut mp.column_idxs),
            StreamWriterSource::Inmemory(&mut mp.metaindex),
            StreamWriterSource::Inmemory(&mut mp.index),
            StreamWriterSource::Inmemory(&mut mp.columns_header_index),
            StreamWriterSource::Inmemory(&mut mp.columns_header),
            StreamWriterSource::Inmemory(&mut mp.timestamps),
            message_bloom_values,
            create_bloom_values_writer,
            1,
        );
    }

    /// Initializes bsw for writing data to file part located at path.
    ///
    /// if nocache is true, then the written data doesn't go to OS page cache.
    pub fn must_init_for_file_part(&mut self, path: &Path, nocache: bool) {
        self.reset();

        fs::must_mkdir_fail_if_exist(path);

        // Open part files in parallel in order to minimze the time needed for this operation
        // on high-latency storage systems such as NFS and Ceph.

        let column_names_path = path.join(COLUMN_NAMES_FILENAME);
        let column_idxs_path = path.join(COLUMN_IDXS_FILENAME);
        let metaindex_path = path.join(METAINDEX_FILENAME);
        let index_path = path.join(INDEX_FILENAME);
        let columns_header_index_path = path.join(COLUMNS_HEADER_INDEX_FILENAME);
        let columns_header_path = path.join(COLUMNS_HEADER_FILENAME);
        let timestamps_path = path.join(TIMESTAMPS_FILENAME);

        let mut column_names_writer = None;
        let mut column_idxs_writer = None;
        let mut metaindex_writer = None;
        let mut index_writer = None;
        let mut columns_header_index_writer = None;
        let mut columns_header_writer = None;
        let mut timestamps_writer = None;
        let mut message_bloom_writer = None;
        let mut message_values_writer = None;

        let mut pfc = filestream::ParallelFileCreator::new();

        // Always cache columnNames file, since it is re-read immediately after part creation
        pfc.add(&column_names_path, &mut column_names_writer, false);

        // Always cache columnIdxs file, since it is re-read immediately after part creation
        pfc.add(&column_idxs_path, &mut column_idxs_writer, false);

        // Always cache metaindex file, since it is re-read immediately after part creation
        pfc.add(&metaindex_path, &mut metaindex_writer, false);

        pfc.add(&index_path, &mut index_writer, nocache);
        pfc.add(
            &columns_header_index_path,
            &mut columns_header_index_writer,
            nocache,
        );
        pfc.add(&columns_header_path, &mut columns_header_writer, nocache);
        pfc.add(&timestamps_path, &mut timestamps_writer, nocache);

        let message_bloom_filter_path = path.join(MESSAGE_BLOOM_FILENAME);
        let message_values_path = path.join(MESSAGE_VALUES_FILENAME);
        pfc.add(
            &message_bloom_filter_path,
            &mut message_bloom_writer,
            nocache,
        );
        pfc.add(&message_values_path, &mut message_values_writer, nocache);

        pfc.run();

        let message_bloom_values_writer = BloomValuesStreamWriter {
            bloom: file_writer_source(message_bloom_writer),
            values: file_writer_source(message_values_writer),
        };
        let create_bloom_values_writer = BloomValuesWriterFactory::File {
            path: path.to_path_buf(),
            nocache,
        };

        self.stream_writers.init(
            file_writer_source(column_names_writer),
            file_writer_source(column_idxs_writer),
            file_writer_source(metaindex_writer),
            file_writer_source(index_writer),
            file_writer_source(columns_header_index_writer),
            file_writer_source(columns_header_writer),
            file_writer_source(timestamps_writer),
            message_bloom_values_writer,
            create_bloom_values_writer,
            BLOOM_VALUES_MAX_SHARDS_COUNT as u64,
        );
    }

    /// Writes timestamps with rows under the given sid to bsw.
    ///
    /// timestamps must be sorted.
    /// sid must be bigger or equal to the sid for the previously written rs.
    pub fn must_write_rows(&mut self, sid: &StreamID, timestamps: &[i64], rows: &[Vec<Field>]) {
        if timestamps.is_empty() {
            return;
        }

        let mut b = get_block();
        b.must_init_from_rows(timestamps, rows);
        self.must_write_block(sid, &b);
        put_block(b);
    }

    /// Writes bd to bsw.
    ///
    /// The bd.streamID must be bigger or equal to the streamID for the previously written blocks.
    pub fn must_write_block_data(&mut self, bd: &BlockData) {
        if bd.rows_count == 0 {
            return;
        }
        self.must_write_block_internal(&bd.stream_id, None, Some(bd));
    }

    /// Writes b under the given sid to bsw.
    ///
    /// The sid must be bigger or equal to the sid for the previously written blocks.
    /// The minimum timestamp in b must be bigger or equal to the minimum timestamp written to the same sid.
    pub fn must_write_block(&mut self, sid: &StreamID, b: &Block) {
        let rows_count = b.len();
        if rows_count == 0 {
            return;
        }
        self.must_write_block_internal(sid, Some(b), None);
    }

    fn must_write_block_internal(
        &mut self,
        sid: &StreamID,
        b: Option<&Block>,
        bd: Option<&BlockData>,
    ) {
        if sid.less(&self.sid_last) {
            panicf!(
                "BUG: the sid={} cannot be smaller than the previously written sid={}",
                sid,
                self.sid_last
            );
        }
        let has_written_blocks = self.has_written_blocks;
        if !has_written_blocks {
            self.sid_first = *sid;
            self.has_written_blocks = true;
        }
        let is_seen_sid = sid.equal(&self.sid_last);
        self.sid_last = *sid;

        let mut bh = get_block_header();
        match b {
            Some(b) => b.must_write_to(sid, &mut bh, &mut self.stream_writers),
            None => bd
                .expect("BUG: either b or bd must be set")
                .must_write_to(&mut bh, &mut self.stream_writers),
        }

        let th = &bh.timestamps_header;
        if self.global_rows_count == 0 || th.min_timestamp < self.global_min_timestamp {
            self.global_min_timestamp = th.min_timestamp;
        }
        if self.global_rows_count == 0 || th.max_timestamp > self.global_max_timestamp {
            self.global_max_timestamp = th.max_timestamp;
        }
        if !has_written_blocks || th.min_timestamp < self.min_timestamp {
            self.min_timestamp = th.min_timestamp;
        }
        if !has_written_blocks || th.max_timestamp > self.max_timestamp {
            self.max_timestamp = th.max_timestamp;
        }
        if is_seen_sid && th.min_timestamp < self.min_timestamp_last {
            panicf!(
                "BUG: the block for sid={} cannot contain timestamp smaller than {}, but it contains timestamp {}",
                sid,
                self.min_timestamp_last,
                th.min_timestamp
            );
        }
        self.min_timestamp_last = th.min_timestamp;

        self.global_uncompressed_size_bytes += bh.uncompressed_size_bytes;
        self.global_rows_count += bh.rows_count;
        self.global_blocks_count += 1;

        // Marshal bh
        bh.marshal(&mut self.index_block_data);
        put_block_header(bh);
        if self.index_block_data.len() > MAX_UNCOMPRESSED_INDEX_BLOCK_SIZE {
            let mut data = std::mem::take(&mut self.index_block_data);
            self.must_flush_index_block(&data);
            data.clear();
            self.index_block_data = data;
        }
    }

    fn must_flush_index_block(&mut self, data: &[u8]) {
        if !data.is_empty() {
            self.index_block_header.must_write_index_block(
                data,
                self.sid_first,
                self.min_timestamp,
                self.max_timestamp,
                &mut self.stream_writers,
            );
            self.index_block_header.marshal(&mut self.metaindex_data);
        }
        self.has_written_blocks = false;
        self.min_timestamp = 0;
        self.max_timestamp = 0;
        self.sid_first.reset();
    }

    /// Finalizes the data write process and updates ph with the finalized stats.
    ///
    /// It closes the writers passed to must_init*().
    ///
    /// bsw can be reused after calling finalize().
    pub fn finalize(&mut self, ph: &mut PartHeader) {
        ph.format_version = PART_FORMAT_LATEST_VERSION;
        ph.uncompressed_size_bytes = self.global_uncompressed_size_bytes;
        ph.rows_count = self.global_rows_count;
        ph.blocks_count = self.global_blocks_count;
        ph.min_timestamp = self.global_min_timestamp;
        ph.max_timestamp = self.global_max_timestamp;
        ph.bloom_values_shards_count = self.stream_writers.bloom_values_shards.len() as u64;

        let data = std::mem::take(&mut self.index_block_data);
        self.must_flush_index_block(&data);
        self.index_block_data = data;

        // Write columnNames data
        must_write_column_names(
            &mut self.stream_writers.column_names_writer,
            &self.stream_writers.column_name_id_generator.column_names,
        );

        // Write columnIdxs data
        must_write_column_idxs(
            &mut self.stream_writers.column_idxs_writer,
            &self.stream_writers.column_idxs,
        );

        // Write metaindex data
        must_write_index_block_headers(
            &mut self.stream_writers.metaindex_writer,
            &self.metaindex_data,
        );

        ph.compressed_size_bytes = self.stream_writers.total_bytes_written();

        self.stream_writers.must_close();
        self.reset();
    }

    /// Converts bsw into a lifetime-erased value for pooling.
    ///
    /// PORT NOTE: Go pools `*blockStreamWriter` directly via sync.Pool; the
    /// Rust type is parameterized by the borrow of the in-memory part, so the
    /// pool stores the `'static` instantiation. reset() drops all the borrows
    /// and the owned buffers are moved over, preserving Go's buffer reuse.
    fn into_static(mut self) -> BlockStreamWriter<'static> {
        self.reset();
        BlockStreamWriter {
            stream_writers: StreamWriters::default(),
            sid_last: self.sid_last,
            sid_first: self.sid_first,
            min_timestamp_last: self.min_timestamp_last,
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
            has_written_blocks: self.has_written_blocks,
            global_uncompressed_size_bytes: self.global_uncompressed_size_bytes,
            global_rows_count: self.global_rows_count,
            global_blocks_count: self.global_blocks_count,
            global_min_timestamp: self.global_min_timestamp,
            global_max_timestamp: self.global_max_timestamp,
            index_block_data: self.index_block_data,
            metaindex_data: self.metaindex_data,
            index_block_header: self.index_block_header,
        }
    }
}

fn file_writer_source(w: Option<filestream::Writer>) -> StreamWriterSource<'static> {
    match w {
        Some(w) => StreamWriterSource::File(w),
        None => StreamWriterSource::None,
    }
}

static BLOCK_STREAM_WRITER_POOL: Mutex<Vec<BlockStreamWriter<'static>>> = Mutex::new(Vec::new());

/// Returns new blockStreamWriter from the pool.
///
/// Return back the blockStreamWriter to the pool when it is no longer needed by calling put_block_stream_writer.
pub fn get_block_stream_writer<'a>() -> BlockStreamWriter<'a> {
    BLOCK_STREAM_WRITER_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_default()
}

/// Returns bsw to the pool.
pub fn put_block_stream_writer(bsw: BlockStreamWriter<'_>) {
    BLOCK_STREAM_WRITER_POOL
        .lock()
        .unwrap()
        .push(bsw.into_static());
}

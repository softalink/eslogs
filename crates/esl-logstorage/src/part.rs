//! Port of EsLogs `lib/logstorage/part.go`.
//!
//! PORT NOTE: Go's part holds `fs.MustReadAtCloser` interface values which are
//! either `*fs.ReaderAt` (file parts) or `*chunkedbuffer.Buffer` (in-memory
//! parts). The port uses the [`PartReaderAt`] enum instead of a trait object,
//! mirroring the two concrete implementations Go actually uses; the `None`
//! variant corresponds to Go's nil interface value (files absent for some part
//! format versions).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use esl_common::fs::MustCloser;
use esl_common::{chunkedbuffer, filestream, fs, panicf};

use crate::block_stream_reader::{
    IndexBlockHeader, ReaderWithStats, StreamReaderSource, must_read_index_block_headers,
};
use crate::column_names::{must_read_column_idxs, must_read_column_names};
use crate::filenames::{
    BLOOM_FILENAME, COLUMN_IDXS_FILENAME, COLUMN_NAMES_FILENAME, COLUMNS_HEADER_FILENAME,
    COLUMNS_HEADER_INDEX_FILENAME, INDEX_FILENAME, MESSAGE_BLOOM_FILENAME, MESSAGE_VALUES_FILENAME,
    METAINDEX_FILENAME, OLD_BLOOM_FILENAME, OLD_VALUES_FILENAME, TIMESTAMPS_FILENAME,
    VALUES_FILENAME,
};
use crate::inmemory_part::InmemoryPart;
use crate::part_header::PartHeader;

/// PORT NOTE: Go's part holds a raw `pt *partition` back-pointer that is
/// nil'ed in mustClosePart(). The port uses `Weak<Partition>` so the
/// partition → datadb → partWrapper → part → partition reference cycle cannot
/// leak; the partition outlives all its open parts (they are closed via
/// mustCloseDatadb before the partition is dropped), so upgrading the Weak is
/// valid whenever Go would dereference `p.pt`.
pub type PartitionRef = std::sync::Weak<crate::partition::Partition>;

/// A random-access reader for a single part file.
#[derive(Default)]
pub enum PartReaderAt<'a> {
    #[default]
    None,
    File(fs::ReaderAt),
    Inmemory(&'a chunkedbuffer::Buffer),
}

impl PartReaderAt<'_> {
    /// Borrowed zero-copy slice for tiny random probes (bloom words). `None`
    /// for in-memory parts (chunked, non-contiguous) and non-mmapped files —
    /// callers fall back to `must_read_at`.
    pub fn mmap_slice(&self, off: i64, len: usize) -> Option<&[u8]> {
        match self {
            PartReaderAt::File(r) => r.mmap_slice(off, len),
            _ => None,
        }
    }

    pub fn path(&self) -> String {
        match self {
            PartReaderAt::None => {
                panicf!("BUG: the part file reader must be initialized before Path() call");
                unreachable!()
            }
            PartReaderAt::File(r) => r.path().to_string(),
            PartReaderAt::Inmemory(cb) => cb.path(),
        }
    }

    /// Reads p.len() bytes at the given offset.
    pub fn must_read_at(&self, p: &mut [u8], off: i64) {
        match self {
            PartReaderAt::None => {
                panicf!("BUG: the part file reader must be initialized before MustReadAt() call")
            }
            PartReaderAt::File(r) => r.must_read_at(p, off),
            PartReaderAt::Inmemory(cb) => cb.must_read_at(p, off),
        }
    }

    pub fn must_close(&mut self) {
        match self {
            PartReaderAt::None => {}
            PartReaderAt::File(r) => r.must_close(),
            PartReaderAt::Inmemory(cb) => cb.must_close(),
        }
        *self = PartReaderAt::None;
    }
}

impl MustCloser for PartReaderAt<'_> {
    fn must_close(&mut self) {
        PartReaderAt::must_close(self)
    }
}

/// part is an on-disk or in-memory part opened for reading.
pub struct Part<'a> {
    /// pt is the partition the part belongs to.
    ///
    /// PORT NOTE: see [`PartitionRef`]; Go nils it in mustClosePart().
    pub pt: Option<PartitionRef>,

    /// path is the path to the part on disk.
    ///
    /// If the part is in-memory then the path is empty.
    pub path: PathBuf,

    /// ph contains partHeader for the given part.
    pub ph: PartHeader,

    /// columnNameIDs is a mapping from column names seen in the given part to internal IDs.
    /// The internal IDs are used in columnHeaderRef.
    pub column_name_ids: HashMap<Arc<[u8]>, u64>,

    /// columnNames is a mapping from internal IDs to column names.
    /// The internal IDs are used in columnHeaderRef.
    pub column_names: Vec<Arc<[u8]>>,

    /// columnIdxs is a mapping from column name to the corresponding item at bloomValuesShards
    pub column_idxs: HashMap<Arc<[u8]>, u64>,

    /// indexBlockHeaders contains a list of indexBlockHeader entries for the given part.
    pub index_block_headers: Vec<IndexBlockHeader>,

    pub index_file: PartReaderAt<'a>,
    pub columns_header_index_file: PartReaderAt<'a>,
    pub columns_header_file: PartReaderAt<'a>,
    pub timestamps_file: PartReaderAt<'a>,

    pub message_bloom_values: BloomValuesReaderAt<'a>,
    pub old_bloom_values: BloomValuesReaderAt<'a>,

    pub bloom_values_shards: Vec<BloomValuesReaderAt<'a>>,
}

#[derive(Default)]
pub struct BloomValuesReaderAt<'a> {
    pub bloom: PartReaderAt<'a>,
    pub values: PartReaderAt<'a>,
}

impl BloomValuesReaderAt<'_> {
    fn append_closers<'b>(&'b mut self, dst: &mut Vec<&'b mut (dyn MustCloser + Send)>) {
        dst.push(&mut self.bloom);
        dst.push(&mut self.values);
    }
}

/// Opens the in-memory part mp for reading.
pub fn must_open_inmemory_part<'a>(pt: PartitionRef, mp: &'a InmemoryPart) -> Part<'a> {
    let mut p = Part {
        pt: Some(pt),
        path: PathBuf::new(),
        ph: mp.ph.clone(),
        column_name_ids: HashMap::new(),
        column_names: Vec::new(),
        column_idxs: HashMap::new(),
        index_block_headers: Vec::new(),
        index_file: PartReaderAt::None,
        columns_header_index_file: PartReaderAt::None,
        columns_header_file: PartReaderAt::None,
        timestamps_file: PartReaderAt::None,
        message_bloom_values: BloomValuesReaderAt::default(),
        old_bloom_values: BloomValuesReaderAt::default(),
        bloom_values_shards: Vec::new(),
    };

    // Read columnNames
    // PORT NOTE: Go passes the raw in-memory reader to mustReadColumnNames;
    // the port wraps it into ReaderWithStats, whose read stats are unused here.
    let mut column_names_reader = ReaderWithStats::default();
    column_names_reader.init(StreamReaderSource::Inmemory(mp.column_names.new_reader()));
    let (column_names, column_name_ids) = must_read_column_names(&mut column_names_reader);
    p.column_names = column_names;
    p.column_name_ids = column_name_ids;
    column_names_reader.must_close();

    // Read columnIdxs
    let mut column_idxs_reader = ReaderWithStats::default();
    column_idxs_reader.init(StreamReaderSource::Inmemory(mp.column_idxs.new_reader()));
    p.column_idxs = must_read_column_idxs(
        &mut column_idxs_reader,
        &p.column_names,
        p.ph.bloom_values_shards_count,
    );
    column_idxs_reader.must_close();

    // Read metaindex
    let mut mrs = ReaderWithStats::default();
    mrs.init(StreamReaderSource::Inmemory(mp.metaindex.new_reader()));
    must_read_index_block_headers(&mut p.index_block_headers, &mut mrs);
    mrs.must_close();

    // Open data files
    p.index_file = PartReaderAt::Inmemory(&mp.index);
    p.columns_header_index_file = PartReaderAt::Inmemory(&mp.columns_header_index);
    p.columns_header_file = PartReaderAt::Inmemory(&mp.columns_header);
    p.timestamps_file = PartReaderAt::Inmemory(&mp.timestamps);

    // Open files with bloom filters and column values
    p.message_bloom_values.bloom = PartReaderAt::Inmemory(&mp.message_bloom_values.bloom);
    p.message_bloom_values.values = PartReaderAt::Inmemory(&mp.message_bloom_values.values);

    p.bloom_values_shards = vec![BloomValuesReaderAt {
        bloom: PartReaderAt::Inmemory(&mp.field_bloom_values.bloom),
        values: PartReaderAt::Inmemory(&mp.field_bloom_values.values),
    }];

    p
}

/// Opens the file-based part at the given path for reading.
pub fn must_open_file_part(pt: PartitionRef, path: &Path) -> Part<'static> {
    let mut p = Part {
        pt: Some(pt),
        path: path.to_path_buf(),
        ph: PartHeader::default(),
        column_name_ids: HashMap::new(),
        column_names: Vec::new(),
        column_idxs: HashMap::new(),
        index_block_headers: Vec::new(),
        index_file: PartReaderAt::None,
        columns_header_index_file: PartReaderAt::None,
        columns_header_file: PartReaderAt::None,
        timestamps_file: PartReaderAt::None,
        message_bloom_values: BloomValuesReaderAt::default(),
        old_bloom_values: BloomValuesReaderAt::default(),
        bloom_values_shards: Vec::new(),
    };
    p.ph.must_read_metadata(path);

    let column_names_path = path.join(COLUMN_NAMES_FILENAME);
    let column_idxs_path = path.join(COLUMN_IDXS_FILENAME);
    let metaindex_path = path.join(METAINDEX_FILENAME);
    let index_path = path.join(INDEX_FILENAME);
    let columns_header_index_path = path.join(COLUMNS_HEADER_INDEX_FILENAME);
    let columns_header_path = path.join(COLUMNS_HEADER_FILENAME);
    let timestamps_path = path.join(TIMESTAMPS_FILENAME);

    // Read columnNames
    if p.ph.format_version >= 1 {
        let mut column_names_reader = ReaderWithStats::default();
        column_names_reader.init(StreamReaderSource::File(filestream::must_open(
            &column_names_path,
            true,
        )));
        let (column_names, column_name_ids) = must_read_column_names(&mut column_names_reader);
        p.column_names = column_names;
        p.column_name_ids = column_name_ids;
        column_names_reader.must_close();
    }
    if p.ph.format_version >= 3 {
        let mut column_idxs_reader = ReaderWithStats::default();
        column_idxs_reader.init(StreamReaderSource::File(filestream::must_open(
            &column_idxs_path,
            true,
        )));
        p.column_idxs = must_read_column_idxs(
            &mut column_idxs_reader,
            &p.column_names,
            p.ph.bloom_values_shards_count,
        );
        column_idxs_reader.must_close();
    }

    // Read metaindex
    let mut mrs = ReaderWithStats::default();
    mrs.init(StreamReaderSource::File(filestream::must_open(
        &metaindex_path,
        true,
    )));
    must_read_index_block_headers(&mut p.index_block_headers, &mut mrs);
    mrs.must_close();

    // Open data files
    p.index_file = PartReaderAt::File(fs::must_open_reader_at(&index_path));
    if p.ph.format_version >= 1 {
        p.columns_header_index_file =
            PartReaderAt::File(fs::must_open_reader_at(&columns_header_index_path));
    }
    p.columns_header_file = PartReaderAt::File(fs::must_open_reader_at(&columns_header_path));
    p.timestamps_file = PartReaderAt::File(fs::must_open_reader_at(&timestamps_path));

    // Open files with bloom filters and column values
    let message_bloom_filter_path = path.join(MESSAGE_BLOOM_FILENAME);
    p.message_bloom_values.bloom =
        PartReaderAt::File(fs::must_open_reader_at(&message_bloom_filter_path));

    let message_values_path = path.join(MESSAGE_VALUES_FILENAME);
    p.message_bloom_values.values =
        PartReaderAt::File(fs::must_open_reader_at(&message_values_path));

    if p.ph.format_version < 1 {
        let bloom_path = path.join(OLD_BLOOM_FILENAME);
        p.old_bloom_values.bloom = PartReaderAt::File(fs::must_open_reader_at(&bloom_path));

        let values_path = path.join(OLD_VALUES_FILENAME);
        p.old_bloom_values.values = PartReaderAt::File(fs::must_open_reader_at(&values_path));
    } else {
        p.bloom_values_shards = (0..p.ph.bloom_values_shards_count)
            .map(|i| {
                let bloom_path = get_bloom_file_path(path, i);
                let values_path = get_values_file_path(path, i);
                BloomValuesReaderAt {
                    bloom: PartReaderAt::File(fs::must_open_reader_at(&bloom_path)),
                    values: PartReaderAt::File(fs::must_open_reader_at(&values_path)),
                }
            })
            .collect();
    }

    p
}

/// Closes p.
pub fn must_close_part(p: &mut Part) {
    // Close files in parallel in order to speed up this operation
    // on high-latency storage systems such as NFS and Ceph.
    let mut cs: Vec<&mut (dyn MustCloser + Send)> = Vec::new();

    cs.push(&mut p.index_file);
    if p.ph.format_version >= 1 {
        cs.push(&mut p.columns_header_index_file);
    }
    cs.push(&mut p.columns_header_file);
    cs.push(&mut p.timestamps_file);
    p.message_bloom_values.append_closers(&mut cs);

    if p.ph.format_version < 1 {
        p.old_bloom_values.append_closers(&mut cs);
    } else {
        for shard in &mut p.bloom_values_shards {
            shard.append_closers(&mut cs);
        }
    }

    fs::must_close_parallel(&mut cs);

    p.pt = None;
}

impl Part<'_> {
    pub fn get_bloom_values_file_for_column_name(&self, name: &[u8]) -> &BloomValuesReaderAt<'_> {
        if name.is_empty() {
            return &self.message_bloom_values;
        }

        if self.ph.format_version < 1 {
            return &self.old_bloom_values;
        }
        if self.ph.format_version < 3 {
            let n = self.bloom_values_shards.len();
            let mut shard_idx = 0u64;
            if n > 1 {
                let h = xxhash_rust::xxh64::xxh64(name, 0);
                shard_idx = h % n as u64;
            }
            return &self.bloom_values_shards[shard_idx as usize];
        }

        match self.column_idxs.get(name) {
            Some(&shard_idx) => &self.bloom_values_shards[shard_idx as usize],
            None => {
                // Panic text only: lossy view of the raw name bytes.
                panicf!(
                    "BUG: unknown shard index for column {:?}; columnIdxs={:?}",
                    String::from_utf8_lossy(name),
                    self.column_idxs
                );
                unreachable!()
            }
        }
    }
}

/// Returns the path to the bloom filter file for the given shard.
pub fn get_bloom_file_path(part_path: &Path, shard_idx: u64) -> PathBuf {
    part_path.join(format!("{BLOOM_FILENAME}{shard_idx}"))
}

/// Returns the path to the values file for the given shard.
pub fn get_values_file_path(part_path: &Path, shard_idx: u64) -> PathBuf {
    part_path.join(format!("{VALUES_FILENAME}{shard_idx}"))
}

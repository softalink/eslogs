//! Port of `lib/mergeset/part.go`.
//!
//! PORT NOTE: upstream keeps three global block caches (`idxbCache`,
//! `ibCache`, `ibSparseCache` from lib/blockcache) for decompressed index and
//! data blocks. The port omits these caches: they affect performance only,
//! not the on-disk format, and the indexdb hot paths are already cached at
//! the Storage level (streamID cache, filterStream cache). Revisit if part
//! search shows up in profiles.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use esl_common::{filestream, fs};

use super::inmemory_part::InmemoryPart;
use super::metaindex_row::{MetaindexRow, unmarshal_metaindex_rows};
use super::part_header::PartHeader;
use super::{INDEX_FILENAME, ITEMS_FILENAME, LENS_FILENAME, METAINDEX_FILENAME};

/// Random-access reader over one part file
/// (Go: the `fs.MustReadAtCloser` values stored in `part`).
pub(crate) enum PartFile {
    File(fs::ReaderAt),
    Mem {
        mp: Arc<InmemoryPart>,
        sel: MemFileSel,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum MemFileSel {
    Index,
    Items,
    Lens,
}

impl PartFile {
    pub fn must_read_at(&self, p: &mut [u8], off: i64) {
        match self {
            PartFile::File(r) => r.must_read_at(p, off),
            PartFile::Mem { mp, sel } => match sel {
                MemFileSel::Index => mp.index_data.must_read_at(p, off),
                MemFileSel::Items => mp.items_data.must_read_at(p, off),
                MemFileSel::Lens => mp.lens_data.must_read_at(p, off),
            },
        }
    }

    fn must_close(&mut self) {
        if let PartFile::File(r) = self {
            r.must_close();
        }
    }
}

/// part represents a searchable part (in-memory or file-based).
pub(crate) struct Part {
    pub ph: PartHeader,

    /// path is empty for in-memory parts.
    pub path: PathBuf,

    pub size: u64,

    pub mrs: Vec<MetaindexRow>,

    pub index_file: PartFile,
    pub items_file: PartFile,
    pub lens_file: PartFile,
}

/// Port of `mustOpenFilePart`.
///
/// PORT NOTE: Go opens the part files in parallel (fs.ParallelReaderAtOpener)
/// for high-latency network storage; the port opens the three ReaderAt files
/// lazily (fs::must_open_reader_at defers the actual open to the first read),
/// which serves the same purpose without the fan-out.
pub(crate) fn must_open_file_part(path: &Path) -> Part {
    let mut ph = PartHeader::default();
    ph.must_read_metadata(path);

    let metaindex_path = path.join(METAINDEX_FILENAME);
    let mut metaindex_file = filestream::must_open(&metaindex_path, true);
    let metaindex_size = fs::must_file_size(&metaindex_path);

    let index_path = path.join(INDEX_FILENAME);
    let items_path = path.join(ITEMS_FILENAME);
    let lens_path = path.join(LENS_FILENAME);

    let index_size = fs::must_file_size(&index_path);
    let items_size = fs::must_file_size(&items_path);
    let lens_size = fs::must_file_size(&lens_path);

    let mut mrs = Vec::new();
    if let Err(err) = unmarshal_metaindex_rows_from_filestream(&mut mrs, &mut metaindex_file) {
        esl_common::panicf!(
            "FATAL: cannot unmarshal metaindexRows from {:?}: {}",
            metaindex_path,
            err
        );
    }
    metaindex_file.must_close();

    Part {
        ph,
        path: path.to_path_buf(),
        size: metaindex_size + index_size + items_size + lens_size,
        mrs,
        index_file: PartFile::File(fs::must_open_reader_at(&index_path)),
        items_file: PartFile::File(fs::must_open_reader_at(&items_path)),
        lens_file: PartFile::File(fs::must_open_reader_at(&lens_path)),
    }
}

fn unmarshal_metaindex_rows_from_filestream(
    dst: &mut Vec<MetaindexRow>,
    r: &mut filestream::Reader,
) -> Result<(), String> {
    struct Adapter<'a>(&'a mut filestream::Reader);
    impl std::io::Read for Adapter<'_> {
        fn read(&mut self, p: &mut [u8]) -> std::io::Result<usize> {
            esl_common::filestream::ReadCloser::read(self.0, p)
        }
    }
    unmarshal_metaindex_rows(dst, &mut Adapter(r))
}

/// Creates a part from the given in-memory part
/// (Go: `inmemoryPart.NewPart` + `newPart`).
pub(crate) fn new_part_from_inmemory_part(mp: &Arc<InmemoryPart>) -> Part {
    let mut mrs = Vec::new();
    let mut metaindex_reader = mp.metaindex_data.new_reader();
    if let Err(err) = unmarshal_metaindex_rows(&mut mrs, &mut metaindex_reader) {
        esl_common::panicf!(
            "FATAL: cannot unmarshal metaindexRows from inmemory part: {}",
            err
        );
    }

    Part {
        ph: mp.ph.clone(),
        path: PathBuf::new(),
        size: mp.size(),
        mrs,
        index_file: PartFile::Mem {
            mp: Arc::clone(mp),
            sel: MemFileSel::Index,
        },
        items_file: PartFile::Mem {
            mp: Arc::clone(mp),
            sel: MemFileSel::Items,
        },
        lens_file: PartFile::Mem {
            mp: Arc::clone(mp),
            sel: MemFileSel::Lens,
        },
    }
}

impl Part {
    /// Port of `part.MustClose`.
    ///
    /// PORT NOTE: Go also removes the part's blocks from the global block
    /// caches here; the port has no block caches (see the module note).
    pub fn must_close(&mut self) {
        self.index_file.must_close();
        self.items_file.must_close();
        self.lens_file.must_close();
    }
}

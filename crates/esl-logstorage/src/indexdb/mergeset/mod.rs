//! Port of Softalink LLC `lib/mergeset`: the LSM-style sorted-items store
//! backing [`crate::indexdb`].
//!
//! # Mergeset decision (PORT NOTE)
//!
//! Earlier iterations of the port used a simplified in-memory sorted-items
//! store here (a single length-prefixed items.bin file), which preserved the
//! `indexdb` API and query semantics but NOT upstream's on-disk indexdb
//! format. This module replaces it with a faithful port of `lib/mergeset`:
//!
//! - part layout: `metaindex.bin` / `index.bin` / `items.bin` / `lens.bin` +
//!   `metadata.json` per part dir, `parts.json` listing the part dirs
//!   ([`inmemory_part`], [`part_header`], [`table`]);
//! - block encodings: commonPrefix/firstItem block headers, plain and zstd
//!   items/lens encodings ([`encoding`], [`block_header`], [`metaindex_row`]);
//! - LSM machinery: rawItems shards → in-memory parts → file parts with
//!   background merges and the `PrepareBlockCallback` merge hook ([`table`],
//!   [`merge`]);
//! - search: metaindex/blockheader binary search with part/table cursors
//!   ([`part_search`], [`table_search`]).
//!
//! Data-compat implication: the `indexdb/` directory written by this store IS
//! byte-compatible with upstream VictoriaLogs/mergeset parts: an indexdb dir
//! produced by upstream can be opened by this port and vice-versa.
//!
//! Deliberate divergences (documented at their definitions):
//! - no global block caches (part.rs);
//! - `Arc<PartWrapper>` instead of the manual refCount (table.rs);
//! - no read-only mode (table.rs);
//! - object pools (bsr/bsw/lensBuffer/bytebuffers) are omitted.

mod block_header;
mod block_stream_reader;
mod block_stream_writer;
pub(crate) mod encoding;
mod inmemory_part;
mod merge;
mod metaindex_row;
mod part;
mod part_header;
mod part_search;
mod table;
mod table_search;

#[cfg(test)]
mod tests;

pub(crate) use encoding::Item;
pub(crate) use table::{Table, TableMetrics, must_open_table};
pub(crate) use table_search::TableSearch;

/// Part directory file names (port of `lib/mergeset/filenames.go`).
pub(crate) const METAINDEX_FILENAME: &str = "metaindex.bin";
pub(crate) const INDEX_FILENAME: &str = "index.bin";
pub(crate) const ITEMS_FILENAME: &str = "items.bin";
pub(crate) const LENS_FILENAME: &str = "lens.bin";
pub(crate) const METADATA_FILENAME: &str = "metadata.json";
pub(crate) const PARTS_FILENAME: &str = "parts.json";

/// PrepareBlockCallback can transform the passed items allocated at the given
/// data (port of `mergeset.PrepareBlockCallback`).
///
/// The callback is called during merge before flushing full block of the
/// given items to persistent storage.
///
/// The callback must return sorted items. The first and the last item must be
/// unchanged. The callback can reuse data and items for storing the result.
///
/// PORT NOTE: Go passes `func(data []byte, items []Item) ([]byte, []Item)`.
/// Since indexdb's callback (`mergeTagToStreamIDsRows`) captures no state, a
/// plain `fn` pointer is used here.
pub(crate) type PrepareBlockCallback = fn(Vec<u8>, Vec<Item>) -> (Vec<u8>, Vec<Item>);

/// SearchError mirrors the two outcomes indexdb distinguishes from mergeset:
/// `io.EOF` (no item) and any other error.
pub(crate) enum SearchError {
    /// No matching item exists (Go `io.EOF`).
    Eof,
    /// Any other, unexpected error.
    Other(String),
}

//! Port of EsLogs `lib/logstorage/block_search.go`.
//!
//! `BlockSearch` is the per-block search context. It owns the lazily populated
//! per-block caches (timestamps, bloom filters, column values, column headers)
//! and exposes the column accessors that `Filter::apply_to_block_search` reads.
//!
//! # Encoded-values representation (CONTRACT for filter ports and block_result)
//! [`BlockSearch::get_values_for_column`] returns the *encoded* column values,
//! i.e. the raw bytes stored in the values file before per-`ValueType`
//! decoding — matching Go's `getValuesForColumn`, which returns `[]string`.
//! Those bytes are frequently NOT valid UTF-8 (int64/float64/ipv4/timestamp
//! columns store fixed-width little-endian words; dict columns store a single
//! index byte), so the port returns `&[Vec<u8>]` rather than `&[String]`,
//! aligning with the already-ported [`crate::encoding::StringsBlockUnmarshaler`]
//! (which yields `Vec<Vec<u8>>`). Consumers reinterpret the bytes per
//! `columnHeader.value_type`, exactly as `block_result.go` does.
//!
//! # PORT NOTEs / deferrals
//! * `PartitionSearchOptions` originates in `storage_search.go`. It is homed
//!   here — `BlockSearch`'s primary consumer — until `storage_search.rs` is
//!   ported; that port should re-export it from here rather than redefine it.
//! * Go pools `blockSearch` via `sync.Pool` (`getBlockSearch`/`putBlockSearch`).
//!   The Rust `BlockSearch<'a>` borrows its inputs (`part`, `pso`, `qs`), so it
//!   cannot be pooled across searches with distinct lifetimes; the value pool is
//!   dropped. The buffer-reuse that mattered is preserved for the sub-resources
//!   (columns header, columns-header index, bloom filters, timestamps) which are
//!   returned to their own pools in [`BlockSearch`]'s `Drop`.
//! * `blockSearch.br`, `blockSearch.search()`, `initColumns` and the
//!   work-batch pool (`blockSearchWorkBatch`, `getBlockSearchWorkBatch`, …) are
//!   deferred: they orchestrate `blockResult` (ported in parallel in
//!   `block_result.rs`) and resolve a `bs`↔`br` aliasing that only the
//!   `BlockResult` design can settle. They land with `block_result.rs` /
//!   `storage_search.rs`.
//! * `getStreamStrSlow` needs `partition.idb` (`pt.idb.appendStreamString`),
//!   which is not yet attached to `Partition` (see the pending notes in
//!   `partition.rs`); it is a `todo!()` until indexdb is wired onto the
//!   partition.
//! * `indexBlockHeader.mustReadBlockHeaders` is deferred: its only caller is
//!   `storage_search.go` (unported) and it is not needed by the filter
//!   accessors.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use esl_common::bytesutil;
use esl_common::encoding as vc_encoding;
use esl_common::panicf;

use crate::bitmap::Bitmap;
use crate::block_header::{
    BlockHeader, ColumnHeader, ColumnsHeader, ColumnsHeaderIndex, get_columns_header,
    get_columns_header_index, put_columns_header, put_columns_header_index,
    unmarshal_block_headers,
};
use crate::block_result::BlockResult;
use crate::block_stream_reader::IndexBlockHeader;
use crate::block_stream_writer::LONG_TERM_BUF_POOL;
use crate::bloomfilter::{BloomFilter, get_bloom_filter, put_bloom_filter};
use crate::consts::{
    MAX_BLOOM_FILTER_BLOCK_SIZE, MAX_COLUMNS_HEADER_INDEX_SIZE, MAX_COLUMNS_HEADER_SIZE,
    MAX_INDEX_BLOCK_SIZE, MAX_TIMESTAMPS_BLOCK_SIZE, MAX_VALUES_BLOCK_SIZE,
    PART_FORMAT_LATEST_VERSION,
};
use crate::encoding::StringsBlockUnmarshaler;
use crate::filter::Filter;
use crate::log_rows::get_canonical_field_name;
use crate::part::Part;
use crate::prefix_filter;
use crate::query_stats::QueryStats;
use crate::rows::Field;
use crate::stream_id::StreamID;
use crate::tenant_id::TenantID;
use crate::u128::U128;
use crate::values_encoder::sub_int64_no_overflow;

/// The number of blocks to search at once by a single worker.
///
/// This number must be increased on systems with many CPU cores in order to
/// amortize the overhead for passing the blockSearchWork to worker goroutines.
pub const BLOCK_SEARCH_WORKS_PER_BATCH: usize = 64;

/// Search options for a partition search.
///
/// PORT NOTE: originates in `storage_search.go`; homed here until
/// `storage_search.rs` is ported (see module docs). Go holds `*prefixfilter.Filter`
/// pointers for the field filters; the port owns them.
///
/// PORT NOTE: Go's `pso.filter` is a `filter` interface value (a pointer shared
/// with the query); the port borrows the query's filter as `&'f dyn Filter`
/// instead of owning it, since the `Filter` trait has no clone hook and the
/// filter outlives every search that reads it.
pub struct PartitionSearchOptions<'f> {
    /// Optional sorted list of tenantIDs for the search.
    /// If it is empty, then the search is performed by streamIDs.
    pub tenant_ids: Vec<TenantID>,

    /// Optional sorted list of streamIDs for the search.
    /// If it is empty, then the search is performed by tenantIDs.
    pub stream_ids: Vec<StreamID>,

    /// The tenantIDs of the query, consumed by lazily-resolved `_stream`
    /// filters.
    ///
    /// PORT NOTE: Go's `initStreamFilters` binds `sso.tenantIDs` into
    /// per-partition `filterStream` copies before `tenant_ids` above is
    /// cleared for a streamID-keyed search; the port's shared `FilterStream`
    /// reads them from here instead (see filter_stream.rs). Unlike
    /// `tenant_ids`, this list is never cleared.
    pub stream_filter_tenant_ids: Vec<TenantID>,

    /// minTimestamp is the minimum timestamp for the search.
    pub min_timestamp: i64,

    /// maxTimestamp is the maximum timestamp for the search.
    pub max_timestamp: i64,

    /// filter is the filter to use for the search.
    pub filter: &'f dyn Filter,

    /// fieldsFilter is the filter of fields to return in the result.
    pub fields_filter: prefix_filter::Filter,

    /// hiddenFieldsFilter is the filter of fields which must be hidden during query.
    pub hidden_fields_filter: prefix_filter::Filter,
}

/// The actual work to perform on a single block.
///
/// PORT NOTE: Go's `blockSearchWork` is pooled and holds raw pointers; the port
/// borrows `p`/`pso` and owns the copied block header. The work-batch pool has
/// no consumer until `storage_search.rs` lands (see module docs).
pub struct BlockSearchWork<'a> {
    /// The part where the block belongs to.
    pub p: &'a Part<'a>,

    /// Search options for the block search.
    pub pso: &'a PartitionSearchOptions<'a>,

    /// The header of the block to search.
    pub bh: BlockHeader,
}

/// The per-block search context.
pub struct BlockSearch<'a> {
    /// qs is updated with various search stats.
    qs: &'a QueryStats,

    /// The part the searched block belongs to.
    ///
    /// PORT NOTE: Go reaches this via `bs.bsw.p`; the port stores the part,
    /// options and block header directly on `BlockSearch` instead of behind a
    /// separate pooled `blockSearchWork` pointer.
    p: &'a Part<'a>,

    /// Search options for the block search.
    pso: &'a PartitionSearchOptions<'a>,

    /// The header of the block being searched.
    bh: BlockHeader,

    /// Cached timestamps for the given block.
    timestamps_cache: Option<vc_encoding::Int64s>,

    /// Cached bloom filters for requested columns in the given block.
    bloom_filter_cache: HashMap<String, BloomFilter>,

    /// Cached encoded values for requested columns in the given block.
    ///
    /// PORT NOTE: Go caches `map[string]*stringBucket` (pooled `[]string`); the
    /// port owns `Vec<Vec<u8>>` per column because the encoded values are binary
    /// (see the module "Encoded-values representation" contract). The pooled
    /// `stringBucket` reuse is dropped for this cache.
    values_cache: HashMap<String, Vec<Vec<u8>>>,

    /// Used for unmarshaling local columns.
    sbu: StringsBlockUnmarshaler,

    /// Holds columnsHeaderIndex bytes for the given block (lazily filled).
    csh_index_block_cache: Vec<u8>,

    /// Holds columnsHeader bytes for the given block (lazily filled).
    csh_block_cache: Vec<u8>,
    csh_block_initialized: bool,

    /// Cache for accessed const columns.
    ccs_cache: Vec<Field>,

    /// Cache for accessed column headers.
    ///
    /// PORT NOTE: entries are boxed so the `*const ColumnHeader` handles that
    /// [`crate::block_result::BlockResult::add_column`] stores stay valid across
    /// `chs_cache` growth. Go relies on the GC keeping reallocated backing
    /// arrays alive; Rust frees them, so heap-stable `Box` storage is required
    /// to preserve the block-search-backed read path.
    // Boxed on purpose: raw pointers into these headers must stay valid across
    #[allow(clippy::vec_box)]
    chs_cache: Vec<Box<ColumnHeader>>,

    /// The columnsHeaderIndex for the given block (lazily filled).
    csh_index_cache: Option<ColumnsHeaderIndex>,

    /// The columnsHeader for the given block (lazily filled).
    csh_cache: Option<ColumnsHeader>,

    /// Seen streamIDs for the recent searches, used for speeding up fetching
    /// the `_stream` column.
    seen_streams: HashMap<U128, String>,
}

impl<'a> BlockSearch<'a> {
    /// Creates a `BlockSearch` for the given block header of the part.
    ///
    /// PORT NOTE: replaces Go's pooled `getBlockSearch` + `blockSearch.search`
    /// setup for the accessor surface; `search()` itself (which populates the
    /// embedded `blockResult`) is deferred to the `block_result.rs` port.
    pub fn new(
        qs: &'a QueryStats,
        p: &'a Part<'a>,
        pso: &'a PartitionSearchOptions<'a>,
        bh: BlockHeader,
    ) -> Self {
        BlockSearch {
            qs,
            p,
            pso,
            bh,
            timestamps_cache: None,
            bloom_filter_cache: HashMap::new(),
            values_cache: HashMap::new(),
            sbu: StringsBlockUnmarshaler::default(),
            csh_index_block_cache: Vec::new(),
            csh_block_cache: Vec::new(),
            csh_block_initialized: false,
            ccs_cache: Vec::new(),
            chs_cache: Vec::new(),
            csh_index_cache: None,
            csh_cache: None,
            seen_streams: HashMap::new(),
        }
    }

    /// Returns the query stats tracked by this block search.
    pub fn query_stats(&self) -> &'a QueryStats {
        self.qs
    }

    /// Returns the block header of the block being searched.
    /// The part the searched block belongs to (Go `bs.bsw.p`).
    pub fn part(&self) -> &Part<'a> {
        self.p
    }

    /// The search options for this block search (Go `bs.bsw.so`).
    pub fn search_options(&self) -> &PartitionSearchOptions<'a> {
        self.pso
    }

    pub fn block_header(&self) -> &BlockHeader {
        &self.bh
    }

    /// Returns the path of the part the block belongs to.
    pub fn part_path(&self) -> String {
        self.p.path.to_string_lossy().into_owned()
    }

    /// Returns the part format version.
    pub fn part_format_version(&self) -> u64 {
        self.p.ph.format_version
    }

    /// Returns true if the given field name must be hidden during the query.
    pub fn is_hidden_field(&self, name: &str) -> bool {
        self.pso.hidden_fields_filter.match_string(name)
    }

    /// Returns the value of the const column with the given name, or an empty
    /// string when the column is not a const column.
    ///
    /// PORT NOTE: Go returns a `string` view into `ccsCache`; the port returns
    /// an owned `String` so callers do not hold a borrow of `self` across other
    /// accessor calls.
    pub fn get_const_column_value(&mut self, name: &str) -> String {
        let name = get_canonical_field_name(name);
        if self.is_hidden_field(name) {
            return String::new();
        }

        if self.part_format_version() < 1 {
            let csh = self.get_columns_header();
            for cc in &csh.const_columns {
                if cc.name == name {
                    return cc.value.clone();
                }
            }
            return String::new();
        }

        let column_name_id = match self.get_column_name_id(name) {
            Some(id) => id,
            None => return String::new(),
        };

        for cc in &self.ccs_cache {
            if cc.name == name {
                return cc.value.clone();
            }
        }

        // Copy the matching const-column ref (Copy) so the columnsHeaderIndex
        // borrow ends before mutating the caches below.
        let cr = {
            let csh_index = self.get_columns_header_index();
            csh_index
                .const_columns_refs
                .iter()
                .find(|cr| cr.column_name_id == column_name_id)
                .copied()
        };
        let cr = match cr {
            Some(cr) => cr,
            None => return String::new(),
        };

        self.ensure_columns_header_block();
        let b_len = self.csh_block_cache.len();
        if cr.offset > b_len as u64 {
            panicf!(
                "FATAL: {}: header offset for const column {:?} cannot exceed {} bytes; got {} bytes",
                self.part_path(),
                name,
                b_len,
                cr.offset
            );
        }
        let name_owned = self.get_column_name_by_id(column_name_id).to_string();
        let mut cc = Field::default();
        if let Err(err) = cc.unmarshal_inplace(&self.csh_block_cache[cr.offset as usize..], false) {
            panicf!(
                "FATAL: {}: cannot unmarshal header for const column {:?}: {}",
                self.part_path(),
                name,
                err
            );
        }
        cc.name = name_owned;
        let value = cc.value.clone();
        self.ccs_cache.push(cc);
        value
    }

    /// Returns the column header for the given name, or `None` when the column
    /// is absent or hidden.
    pub fn get_column_header(&mut self, name: &str) -> Option<&ColumnHeader> {
        let name = get_canonical_field_name(name);
        if self.is_hidden_field(name) {
            return None;
        }

        if self.part_format_version() < 1 {
            let csh = self.get_columns_header();
            return csh.column_headers.iter().find(|ch| ch.name == name);
        }

        let column_name_id = self.get_column_name_id(name)?;

        if let Some(pos) = self.chs_cache.iter().position(|ch| ch.name == name) {
            return Some(&self.chs_cache[pos]);
        }

        // Copy the matching column-header ref (Copy) so the columnsHeaderIndex
        // borrow ends before mutating the caches below.
        let cr = {
            let csh_index = self.get_columns_header_index();
            csh_index
                .column_headers_refs
                .iter()
                .find(|cr| cr.column_name_id == column_name_id)
                .copied()
        };
        let cr = cr?;

        self.ensure_columns_header_block();
        let b_len = self.csh_block_cache.len();
        if cr.offset > b_len as u64 {
            panicf!(
                "FATAL: {}: header offset for column {:?} cannot exceed {} bytes; got {} bytes",
                self.part_path(),
                name,
                b_len,
                cr.offset
            );
        }
        let name_owned = self.get_column_name_by_id(column_name_id).to_string();
        let mut ch = ColumnHeader::default();
        if let Err(err) = ch.unmarshal_inplace(
            &self.csh_block_cache[cr.offset as usize..],
            PART_FORMAT_LATEST_VERSION,
        ) {
            panicf!(
                "FATAL: {}: cannot unmarshal header for column {:?}: {}",
                self.part_path(),
                name,
                err
            );
        }
        ch.name = name_owned;
        self.chs_cache.push(Box::new(ch));
        self.chs_cache.last().map(|b| b.as_ref())
    }

    /// Returns the internal id for the given column name.
    pub fn get_column_name_id(&self, name: &str) -> Option<u64> {
        self.p.column_name_ids.get(name).copied()
    }

    /// Returns the column name for the given internal id.
    pub fn get_column_name_by_id(&self, column_name_id: u64) -> &str {
        let column_names = &self.p.column_names;
        if column_name_id >= column_names.len() as u64 {
            panicf!(
                "FATAL: {}: too big columnNameID={}; it must be smaller than {}",
                self.part_path(),
                column_name_id,
                column_names.len()
            );
        }
        &column_names[column_name_id as usize]
    }

    /// Returns the columnsHeaderIndex for the block, reading it lazily.
    pub fn get_columns_header_index(&mut self) -> &ColumnsHeaderIndex {
        if self.part_format_version() < 1 {
            panicf!(
                "BUG: getColumnsHeaderIndex() can be called only for part encoding v1+, while it has been called for v{}",
                self.part_format_version()
            );
        }

        if self.csh_index_cache.is_none() {
            self.csh_index_block_cache.clear();
            read_columns_header_index_block(
                &mut self.csh_index_block_cache,
                self.p,
                &self.bh,
                self.qs,
            );

            let mut csh_index = get_columns_header_index();
            if let Err(err) = csh_index.unmarshal_inplace(&self.csh_index_block_cache) {
                panicf!(
                    "FATAL: {}: cannot unmarshal columns header index: {}",
                    self.p.path.to_string_lossy(),
                    err
                );
            }
            self.csh_index_cache = Some(csh_index);
        }
        self.csh_index_cache.as_ref().unwrap()
    }

    /// Returns the columnsHeader for the block, reading it lazily.
    pub fn get_columns_header(&mut self) -> &ColumnsHeader {
        if self.csh_cache.is_none() {
            self.ensure_columns_header_block();
            let part_format_version = self.part_format_version();

            let mut csh = get_columns_header();
            if let Err(err) = csh.unmarshal_inplace(&self.csh_block_cache, part_format_version) {
                panicf!(
                    "FATAL: {}: cannot unmarshal columns header: {}",
                    self.p.path.to_string_lossy(),
                    err
                );
            }
            if part_format_version >= 1 {
                // Capture the part reference (Copy) so `p.column_names` borrows
                // the part, not `self`, leaving `self` free for the `&mut self`
                // `get_columns_header_index()` call below.
                let p = self.p;
                let csh_index = self.get_columns_header_index();
                if let Err(err) = csh.set_column_names(csh_index, &p.column_names) {
                    panicf!("FATAL: {}: {}", p.path.to_string_lossy(), err);
                }
            }
            self.csh_cache = Some(csh);
        }
        self.csh_cache.as_ref().unwrap()
    }

    /// Returns the raw columnsHeader bytes for the block, reading them lazily.
    pub fn get_columns_header_block(&mut self) -> &[u8] {
        self.ensure_columns_header_block();
        &self.csh_block_cache
    }

    fn ensure_columns_header_block(&mut self) {
        if !self.csh_block_initialized {
            self.csh_block_cache.clear();
            read_columns_header_block(&mut self.csh_block_cache, self.p, &self.bh, self.qs);
            self.csh_block_initialized = true;
        }
    }

    /// Returns whether the column's bloom filter contains all `hashes`.
    ///
    /// Fast path (not in Go): probes the mmapped bloom block in place via
    /// [`BloomFilter::bytes_contain_all`] — a few word reads instead of
    /// copying + unmarshalling the whole filter. Falls back to the cached
    /// [`Self::get_bloom_filter_for_column`] for in-memory parts and
    /// non-mmapped files.
    pub fn bloom_contains_all(&mut self, ch: &ColumnHeader, hashes: &[u64]) -> bool {
        let bloom_filter_size = ch.bloom_filter_size;
        if bloom_filter_size > MAX_BLOOM_FILTER_BLOCK_SIZE as u64 {
            panicf!(
                "FATAL: {}: bloom filter block size cannot exceed {} bytes; got {} bytes",
                self.part_path(),
                MAX_BLOOM_FILTER_BLOCK_SIZE,
                bloom_filter_size
            );
        }
        let raw = {
            let bloom_values_file = self.p.get_bloom_values_file_for_column_name(&ch.name);
            bloom_values_file
                .bloom
                .mmap_slice(ch.bloom_filter_offset as i64, bloom_filter_size as usize)
        };
        match raw {
            Some(raw) => {
                self.qs
                    .bytes_read_bloom_filters
                    .fetch_add(ch.bloom_filter_size, Ordering::SeqCst);
                BloomFilter::bytes_contain_all(raw, hashes)
            }
            None => self.get_bloom_filter_for_column(ch).contains_all(hashes),
        }
    }

    /// Returns the bloom filter for the given column header.
    ///
    /// The returned bloom filter belongs to `self` and becomes invalid after
    /// `self` is dropped.
    pub fn get_bloom_filter_for_column(&mut self, ch: &ColumnHeader) -> &BloomFilter {
        if !self.bloom_filter_cache.contains_key(&ch.name) {
            let p = self.p;
            let mut bb = LONG_TERM_BUF_POOL.get();
            let bloom_filter_size = ch.bloom_filter_size;
            if bloom_filter_size > MAX_BLOOM_FILTER_BLOCK_SIZE as u64 {
                panicf!(
                    "FATAL: {}: bloom filter block size cannot exceed {} bytes; got {} bytes",
                    self.part_path(),
                    MAX_BLOOM_FILTER_BLOCK_SIZE,
                    bloom_filter_size
                );
            }
            {
                let bloom_values_file = p.get_bloom_values_file_for_column_name(&ch.name);
                bb.b.resize(bloom_filter_size as usize, 0);
                bloom_values_file
                    .bloom
                    .must_read_at(&mut bb.b, ch.bloom_filter_offset as i64);
            }

            self.qs
                .bytes_read_bloom_filters
                .fetch_add(ch.bloom_filter_size, Ordering::SeqCst);

            let mut bf = get_bloom_filter();
            if let Err(err) = bf.unmarshal(&bb.b) {
                panicf!(
                    "FATAL: {}: cannot unmarshal bloom filter: {}",
                    self.part_path(),
                    err
                );
            }
            LONG_TERM_BUF_POOL.put(bb);

            self.bloom_filter_cache.insert(ch.name.clone(), bf);
        }
        self.bloom_filter_cache.get(&ch.name).unwrap()
    }

    /// Returns the encoded block values for the given column header.
    ///
    /// The returned values belong to `self` and become invalid after `self` is
    /// dropped. See the module "Encoded-values representation" contract for the
    /// `&[Vec<u8>]` (raw bytes) representation.
    pub fn get_values_for_column(&mut self, ch: &ColumnHeader) -> &[Vec<u8>] {
        if !self.values_cache.contains_key(&ch.name) {
            let p = self.p;
            let mut bb = LONG_TERM_BUF_POOL.get();
            let values_size = ch.values_size;
            if values_size > MAX_VALUES_BLOCK_SIZE as u64 {
                panicf!(
                    "FATAL: {}: values block size cannot exceed {} bytes; got {} bytes",
                    self.part_path(),
                    MAX_VALUES_BLOCK_SIZE,
                    values_size
                );
            }
            {
                let bloom_values_file = p.get_bloom_values_file_for_column_name(&ch.name);
                bb.b.resize(values_size as usize, 0);
                bloom_values_file
                    .values
                    .must_read_at(&mut bb.b, ch.values_offset as i64);
            }

            self.qs
                .bytes_read_values
                .fetch_add(ch.values_size, Ordering::SeqCst);

            let mut values: Vec<Vec<u8>> = Vec::new();
            let rows_count = self.bh.rows_count;
            if let Err(err) = self.sbu.unmarshal(&mut values, &bb.b, rows_count) {
                panicf!(
                    "FATAL: {}: cannot unmarshal column {:?}: {}",
                    self.part_path(),
                    ch.name,
                    err
                );
            }
            LONG_TERM_BUF_POOL.put(bb);

            self.qs
                .values_read
                .fetch_add(values.len() as u64, Ordering::SeqCst);
            self.qs
                .bytes_processed_uncompressed_values
                .fetch_add(get_strings_len(&values), Ordering::SeqCst);

            self.values_cache.insert(ch.name.clone(), values);
        }
        self.values_cache.get(&ch.name).unwrap()
    }

    /// Subtracts the given time offset from the block timestamps and their header.
    pub fn sub_time_offset_to_timestamps(&mut self, time_offset: i64) {
        self.bh.timestamps_header.sub_time_offset(time_offset);
        if let Some(ts) = self.timestamps_cache.as_mut() {
            sub_time_offset(&mut ts.a, time_offset);
        }
    }

    /// Returns the timestamps for the block, reading them lazily.
    ///
    /// The returned timestamps belong to `self` and become invalid after `self`
    /// is dropped.
    pub fn get_timestamps(&mut self) -> &[i64] {
        if self.timestamps_cache.is_none() {
            let p = self.p;
            let mut bb = LONG_TERM_BUF_POOL.get();
            let block_size = self.bh.timestamps_header.block_size;
            if block_size > MAX_TIMESTAMPS_BLOCK_SIZE as u64 {
                panicf!(
                    "FATAL: {}: timestamps block size cannot exceed {} bytes; got {} bytes",
                    self.part_path(),
                    MAX_TIMESTAMPS_BLOCK_SIZE,
                    block_size
                );
            }
            bb.b.resize(block_size as usize, 0);
            p.timestamps_file
                .must_read_at(&mut bb.b, self.bh.timestamps_header.block_offset as i64);

            self.qs
                .bytes_read_timestamps
                .fetch_add(block_size, Ordering::SeqCst);
            self.qs
                .timestamps_read
                .fetch_add(self.bh.rows_count, Ordering::SeqCst);

            let rows_count = self.bh.rows_count as usize;
            let marshal_type = self.bh.timestamps_header.marshal_type;
            let min_timestamp = self.bh.timestamps_header.min_timestamp;
            let mut ts = vc_encoding::get_int64s(0);
            ts.a.clear();
            if let Err(err) = vc_encoding::unmarshal_timestamps(
                &mut ts.a,
                &bb.b,
                marshal_type,
                min_timestamp,
                rows_count,
            ) {
                panicf!(
                    "FATAL: {}: cannot unmarshal timestamps: {}",
                    self.part_path(),
                    err
                );
            }
            LONG_TERM_BUF_POOL.put(bb);

            self.timestamps_cache = Some(ts);
        }
        &self.timestamps_cache.as_ref().unwrap().a
    }

    /// Returns the `_stream` value for the block.
    pub fn get_stream_str(&mut self) -> String {
        let sid = self.bh.stream_id.id;
        if let Some(s) = self.seen_streams.get(&sid).filter(|s| !s.is_empty()) {
            // Fast path - streamStr is found in the seenStreams.
            return s.clone();
        }

        // Slow path - load streamStr from the storage.
        let stream_str = self.get_stream_str_slow();
        if !stream_str.is_empty() {
            // Store the found streamStr in seenStreams.
            if self.seen_streams.len() > 20_000 {
                self.seen_streams.clear();
            }
            self.seen_streams.insert(sid, stream_str.clone());
        }
        stream_str
    }

    fn get_stream_str_slow(&self) -> String {
        // Go: `bb.B = bs.bsw.p.pt.idb.appendStreamString(bb.B[:0], &bh.streamID)`.
        let pt = self
            .p
            .pt
            .as_ref()
            .expect("BUG: part.pt must be set during search")
            .upgrade()
            .expect("BUG: partition must outlive its parts during search");
        let mut bb: Vec<u8> = Vec::new();
        pt.idb.append_stream_string(&mut bb, &self.bh.stream_id);
        String::from_utf8_lossy(&bb).into_owned()
    }

    /// Searches the rows in this block that match the partition filter, then
    /// materializes the requested columns into `br`.
    ///
    /// PORT NOTE: Go's `blockSearch.search` populates the embedded `bs.br`. The
    /// Rust [`BlockResult`] is a separate value that only holds a `*const`
    /// back-reference to the block search (mirroring Go's `br.bs` pointer), so
    /// `search` takes the destination `br` explicitly instead. The block-search
    /// value pool (`getBlockSearch`/`putBlockSearch`) is dropped, since the
    /// borrowed `BlockSearch<'a>` cannot be reused across parts (see module
    /// docs); the caller creates a fresh `BlockSearch` per block.
    pub fn search(&mut self, bm: &mut Bitmap, br: &mut BlockResult) {
        br.reset();

        // search rows matching the given filter
        bm.init(self.bh.rows_count as usize);
        bm.set_bits();
        let pso = self.pso;
        pso.filter.apply_to_block_search(self, bm);

        if bm.is_zero() {
            // The filter doesn't match any logs in the current block.
            return;
        }

        br.must_init(&*self, bm);

        // fetch the requested columns to br.
        br.init_columns(&pso.fields_filter);
    }
}

/// A batch of block-search work items processed together by a single worker.
///
/// PORT NOTE: Go pools these via `sync.Pool` (`getBlockSearchWorkBatch`) and
/// reuses their backing slices. The Rust [`BlockSearchWork<'a>`] borrows `p`
/// and `pso`, so a batch cannot live in a `'static` pool; the batch is a plain
/// owned container. `storage_search.rs` scopes the worker threads so the
/// borrows outlive the workers. `append_block_search_work` mirrors Go's
/// `len < cap` "batch is full" signal used to flush a batch to the workers.
pub struct BlockSearchWorkBatch<'a> {
    pub bsws: Vec<BlockSearchWork<'a>>,
}

impl<'a> BlockSearchWorkBatch<'a> {
    /// Creates an empty batch preallocated to hold [`BLOCK_SEARCH_WORKS_PER_BATCH`].
    pub fn new() -> Self {
        Self {
            bsws: Vec::with_capacity(BLOCK_SEARCH_WORKS_PER_BATCH),
        }
    }

    /// Clears the batch for reuse.
    pub fn reset(&mut self) {
        self.bsws.clear();
    }

    /// Appends a block-search work item and returns `true` while the batch has
    /// spare capacity (Go returns `len(bsws) < cap(bsws)`).
    pub fn append_block_search_work(
        &mut self,
        p: &'a Part<'a>,
        pso: &'a PartitionSearchOptions<'a>,
        bh: &BlockHeader,
    ) -> bool {
        self.bsws.push(BlockSearchWork {
            p,
            pso,
            bh: bh.clone(),
        });
        self.bsws.len() < BLOCK_SEARCH_WORKS_PER_BATCH
    }
}

impl Default for BlockSearchWorkBatch<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for BlockSearch<'_> {
    fn drop(&mut self) {
        // Return pooled sub-resources, mirroring Go's blockSearch.reset().
        if let Some(ts) = self.timestamps_cache.take() {
            vc_encoding::put_int64s(ts);
        }
        for (_, bf) in self.bloom_filter_cache.drain() {
            put_bloom_filter(bf);
        }
        if let Some(csh_index) = self.csh_index_cache.take() {
            put_columns_header_index(csh_index);
        }
        if let Some(csh) = self.csh_cache.take() {
            put_columns_header(csh);
        }
    }
}

/// Sums the byte lengths of the given encoded values.
///
/// PORT NOTE: Go's `getStringsLen` sums `len(s)` over `[]string`; the port sums
/// over `&[Vec<u8>]` (encoded values are binary, see module docs).
pub fn get_strings_len(a: &[Vec<u8>]) -> u64 {
    a.iter().map(|s| s.len() as u64).sum()
}

/// Subtracts the given time offset from every timestamp in place.
pub fn sub_time_offset(timestamps: &mut [i64], time_offset: i64) {
    for t in timestamps.iter_mut() {
        *t = sub_int64_no_overflow(*t, time_offset);
    }
}

/// Reads and unmarshals the block headers referenced by `ih` from part `p`
/// into `dst` (Go `indexBlockHeader.mustReadBlockHeaders`).
///
/// PORT NOTE: Go's method returns the appended slice; the port clears `dst` and
/// fills it in place (mirroring the caller's `bhss.bhs[:0]` reuse). It lives as
/// a free function here rather than a method on [`IndexBlockHeader`] (which is
/// homed in `block_stream_reader.rs`) so the read path stays in the search
/// module without touching the reader module.
pub fn must_read_block_headers(
    dst: &mut Vec<BlockHeader>,
    ih: &IndexBlockHeader,
    p: &Part<'_>,
    qs: &QueryStats,
) {
    dst.clear();

    let index_block_size = ih.index_block_size;
    if index_block_size > MAX_INDEX_BLOCK_SIZE as u64 {
        panicf!(
            "FATAL: {}: index block size cannot exceed {} bytes; got {} bytes",
            p.index_file.path(),
            MAX_INDEX_BLOCK_SIZE,
            index_block_size
        );
    }

    let mut bb_compressed = LONG_TERM_BUF_POOL.get();
    bytesutil::resize_no_copy_may_overallocate(&mut bb_compressed.b, index_block_size as usize);
    p.index_file
        .must_read_at(&mut bb_compressed.b, ih.index_block_offset as i64);

    qs.bytes_read_block_headers
        .fetch_add(ih.index_block_size, Ordering::SeqCst);

    let mut bb = LONG_TERM_BUF_POOL.get();
    let res = vc_encoding::decompress_zstd(&mut bb.b, &bb_compressed.b);
    LONG_TERM_BUF_POOL.put(bb_compressed);
    if let Err(err) = res {
        panicf!(
            "FATAL: {}: cannot decompress indexBlock read at offset {} with size {}: {}",
            p.index_file.path(),
            ih.index_block_offset,
            index_block_size,
            err
        );
    }

    let res = unmarshal_block_headers(dst, &bb.b, p.ph.format_version);
    LONG_TERM_BUF_POOL.put(bb);
    if let Err(err) = res {
        panicf!(
            "FATAL: {}: cannot unmarshal block headers read at offset {} with size {}: {}",
            p.index_file.path(),
            ih.index_block_offset,
            index_block_size,
            err
        );
    }
}

/// Reads `columnsHeaderIndex` bytes for `bh` from `p`, appending them to `dst`.
fn read_columns_header_index_block(
    dst: &mut Vec<u8>,
    p: &Part<'_>,
    bh: &BlockHeader,
    qs: &QueryStats,
) {
    let n = bh.columns_header_index_size;
    if n > MAX_COLUMNS_HEADER_INDEX_SIZE as u64 {
        panicf!(
            "FATAL: {}: columns header index size cannot exceed {} bytes; got {} bytes",
            p.path.to_string_lossy(),
            MAX_COLUMNS_HEADER_INDEX_SIZE,
            n
        );
    }

    let dst_len = dst.len();
    dst.resize(dst_len + n as usize, 0);
    p.columns_header_index_file
        .must_read_at(&mut dst[dst_len..], bh.columns_header_index_offset as i64);

    qs.bytes_read_columns_header_indexes
        .fetch_add(bh.columns_header_index_size, Ordering::SeqCst);
}

/// Reads `columnsHeader` bytes for `bh` from `p`, appending them to `dst`.
fn read_columns_header_block(dst: &mut Vec<u8>, p: &Part<'_>, bh: &BlockHeader, qs: &QueryStats) {
    let n = bh.columns_header_size;
    if n > MAX_COLUMNS_HEADER_SIZE as u64 {
        panicf!(
            "FATAL: {}: columns header size cannot exceed {} bytes; got {} bytes",
            p.path.to_string_lossy(),
            MAX_COLUMNS_HEADER_SIZE,
            n
        );
    }

    let dst_len = dst.len();
    dst.resize(dst_len + n as usize, 0);
    p.columns_header_file
        .must_read_at(&mut dst[dst_len..], bh.columns_header_offset as i64);

    qs.bytes_read_columns_headers
        .fetch_add(bh.columns_header_size, Ordering::SeqCst);
}

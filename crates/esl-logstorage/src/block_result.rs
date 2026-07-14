//! Port of EsLogs `lib/logstorage/block_result.go`.
//!
//! `BlockResult` is the materialized query-result block: a set of named
//! columns ([`BlockResultColumn`]) plus per-row timestamps. Filters and pipes
//! (Layers 4-6) read and write query results through this type.
//!
//! # PORT NOTES (whole-module design decisions)
//!
//! * **Value representation.** Go stores every column value slice as
//!   `[]string`, relying on `string` being able to hold arbitrary bytes
//!   (encoded uint/int/float/ipv4/timestamp values are binary, not UTF-8).
//!   The port stores value slices as `Vec<Vec<u8>>`; accessors that hand out
//!   `&str` (const values, decoded human-readable values) reinterpret the
//!   bytes via [`esl_common::bytesutil::to_unsafe_string`], exactly matching the
//!   Go call sites.
//!
//! * **Arena / valuesBuf.** Go packs all column bytes into a shared
//!   `arena`/`valuesBuf` and hands out unsafe string views to minimize
//!   allocations. The Rust borrow checker forbids that self-referential
//!   pattern (see the same note on `arena.rs`), so each column owns its value
//!   `Vec`s. This diverges from Go's allocation strategy (a benchmark-relevant
//!   difference) but produces identical observable values.
//!
//! * **Column addressing.** Go returns `*blockResultColumn` from
//!   `getColumns`/`getColumnByName`. The port uses [`ColRef`] handles (indices
//!   into the column buffers) so value materialization can cache back into the
//!   owning column without aliasing violations. The Go free methods
//!   `br.getUint8Values(c)` etc. become [`BlockResultColumn`] methods, since
//!   with owned per-column storage they only need the column's own data plus
//!   `rows_len`/`timestamps`.
//!
//! * **Borrowed sources (`bs`, `br_src`, `bm`).** Go stores raw pointers
//!   (`*blockSearch`, `*blockResult`, `*bitmap`) that stay valid until the
//!   source changes. The port mirrors this with raw `*const` pointers and the
//!   same single-threaded, valid-until-source-changes contract; reads through
//!   them are wrapped in `unsafe` with `SAFETY` comments.
//!
//! * **`BlockSearch` coupling.** `blockSearch` (block_search.go) is ported in
//!   parallel in `block_search.rs`; this module references
//!   `crate::block_search::BlockSearch` but only ever stores a `*const` to it
//!   (never calls its methods). Result blocks touch a `BlockSearch` only when
//!   constructed from a block search (`bs` is `Some`), which never happens for
//!   pipe-constructed results or the tests here. Block-search-backed *reads*
//!   (`init_columns`, reading encoded values from a column header, the
//!   timestamp fast paths) are `unimplemented!()` until block_search.rs wires
//!   them up.

use std::collections::HashMap;

use esl_common::{decimal, encoding as vencoding, fastnum};

use crate::bitmap::Bitmap;
use crate::block_header::{ColumnHeader, ColumnsHeader};
use crate::block_search::BlockSearch;
use crate::log_rows::get_canonical_column_name_bytes;
use crate::prefix_filter::{
    self as prefixfilter, Filter, append_replace, is_wildcard_filter, match_filter, match_filters,
};
use crate::rows::Field;
use crate::values_encoder::{
    NSECS_PER_DAY, NSECS_PER_HOUR, NSECS_PER_MICROSECOND, NSECS_PER_MILLISECOND, NSECS_PER_MINUTE,
    NSECS_PER_SECOND, NSECS_PER_WEEK, ValueType, marshal_duration_string, marshal_float64_string,
    marshal_int64_string, marshal_ipv4_string, marshal_timestamp_iso8601_string,
    marshal_timestamp_rfc3339_nano_string, marshal_uint8_string, marshal_uint16_string,
    marshal_uint32_string, marshal_uint64_string, try_parse_bytes, try_parse_duration,
    try_parse_float64, try_parse_float64_bytes, try_parse_int64, try_parse_ipv4,
    try_parse_ipv4_mask, try_parse_timestamp_rfc3339_nano, unmarshal_float64, unmarshal_int64,
    unmarshal_ipv4, unmarshal_timestamp_iso8601, unmarshal_uint8, unmarshal_uint16,
    unmarshal_uint32, unmarshal_uint64,
};

/// `time.RFC3339Nano` length, used by `sumLenValues` for the `_time` column.
const RFC3339_NANO_LEN: usize = "2006-01-02T15:04:05.999999999Z07:00".len();

/// `iso8601Timestamp` const from values_encoder.go, used by `sumLenValues`.
const ISO8601_TIMESTAMP_LEN: usize = "2006-01-02T15:04:05.000Z".len();

// ---------------------------------------------------------------------------
// Placeholders for not-yet-ported types
// ---------------------------------------------------------------------------

/// PORT NOTE: minimal placeholder for `byStatsField` (pipe_stats.go, Layer 5).
/// Only the fields consulted by value bucketing are defined; the Layer-5
/// porter should replace this with the real type.
#[derive(Debug, Default, Clone)]
pub(crate) struct ByStatsField {
    // PORT NOTE: `name` is part of the Layer-5 `byStatsField` shape but is not
    // consulted by value bucketing; kept for parity with the Go struct.
    #[allow(dead_code)]
    pub name: Vec<u8>,
    pub bucket_size_str: String,
    pub bucket_size: f64,
    pub bucket_offset_str: String,
    pub bucket_offset: f64,
}

// ---------------------------------------------------------------------------
// ColRef
// ---------------------------------------------------------------------------

/// Handle to a column owned by a [`BlockResult`].
///
/// PORT NOTE: replaces Go's `*blockResultColumn`, distinguishing real columns
/// (`csBuf`) from lazily-created empty columns (`csEmpty`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColRef {
    /// Index into `cs_buf` (a requested column).
    Buf(usize),
    /// Index into `cs_empty` (a referenced-but-missing empty column).
    Empty(usize),
}

// ---------------------------------------------------------------------------
// BlockResult
// ---------------------------------------------------------------------------

/// Holds results for a single block of log entries.
///
/// It is expected that its contents are accessed only from a single thread at
/// a time.
#[derive(Default)]
pub struct BlockResult {
    /// Number of rows in the given block result.
    rows_len: usize,

    /// Associated block search. `None` for results constructed by pipes.
    ///
    /// PORT NOTE: `BlockSearch<'a>` carries a lifetime, but `BlockResult` must
    /// stay lifetime-free (filters take a bare `&BlockResult`). The pointer is
    /// therefore type-erased to `*const ()`, mirroring Go's raw `*blockSearch`.
    /// Block-search-backed reads recover a `&mut BlockSearch<'static>` via
    /// [`BlockResult::bs_ptr`] and the same shared→mut raw-pointer contract used
    /// for `br_src` above (single thread, valid until `bs`/`bm` change).
    bs: Option<*const ()>,

    /// Source block result, set when `bs` is `None` and `bm` is `Some`.
    br_src: Option<*const BlockResult>,

    /// Optional bitmap applied to `bs` or `br_src` to obtain the values.
    bm: Option<*const Bitmap>,

    /// Cached timestamps for the selected log entries in the block.
    timestamps_buf: Vec<i64>,

    /// Requested columns.
    cs_buf: Vec<BlockResultColumn>,

    /// Non-existing columns referenced via `get_column_by_name`.
    cs_empty: Vec<BlockResultColumn>,

    /// Cached column order (deduped indices into `cs_buf`) when initialized.
    cs: Vec<usize>,

    /// Whether `cs` is initialized and can be returned from `get_columns`.
    cs_initialized: bool,
}

impl BlockResult {
    /// Resets the block result for reuse.
    pub fn reset(&mut self) {
        self.rows_len = 0;
        self.bs = None;
        self.br_src = None;
        self.bm = None;
        self.timestamps_buf.clear();
        self.cs_buf.clear();
        self.cs_empty.clear();
        self.cs.clear();
        self.cs_initialized = false;
    }

    /// Returns the number of rows in the block result.
    pub fn rows_len(&self) -> usize {
        self.rows_len
    }

    /// Returns the size of the block result in bytes.
    pub fn size_bytes(&self) -> usize {
        let mut n = std::mem::size_of::<Self>();
        n += self.timestamps_buf.capacity() * std::mem::size_of::<i64>();
        for c in &self.cs_buf {
            n += c.size_bytes();
        }
        for c in &self.cs_empty {
            n += c.size_bytes();
        }
        n += self.cs.capacity() * std::mem::size_of::<usize>();
        n
    }

    // -- construction --------------------------------------------------------

    /// Returns a clone of `self` that owns its own data.
    pub fn clone_result(&mut self) -> BlockResult {
        let mut br_new = BlockResult {
            rows_len: self.rows_len,
            ..Default::default()
        };

        let cols = self.get_columns();

        // Pre-populate values in every column to materialize them before cloning.
        for &r in &cols {
            let _ = self.column_get_values(r);
        }

        let src_timestamps = self.get_timestamps().to_vec();
        br_new.timestamps_buf = src_timestamps;
        br_new.check_timestamps_len();

        let mut cs_new = Vec::with_capacity(cols.len());
        for &r in &cols {
            cs_new.push(self.col(r).clone_column());
        }
        br_new.cs_buf = cs_new;

        // csEmpty is not cloned - it is repopulated by the caller.
        br_new
    }

    /// Initializes `self` from `br_src` by copying rows identified by set bits
    /// in `bm`.
    ///
    /// The result is valid until `br_src` or `bm` is updated.
    pub fn init_from_filter_all_columns(&mut self, br_src: &BlockResult, bm: &Bitmap) {
        self.reset();

        self.rows_len = bm.ones_count();
        if self.rows_len == 0 {
            return;
        }

        self.br_src = Some(br_src as *const BlockResult);
        self.bm = Some(bm as *const Bitmap);

        // SAFETY: br_src is valid for the duration of this call (mirrors Go's
        // pointer usage); we only read column metadata here.
        let src = unsafe { &*(br_src as *const BlockResult) };
        let n = src.cs_buf.len();
        let order: Vec<usize> = if src.cs_initialized {
            src.cs.clone()
        } else {
            (0..n).collect()
        };
        for &ci in &order {
            let c_src = &src.cs_buf[ci];
            self.append_filtered_column(c_src, ci);
        }
    }

    fn append_filtered_column(&mut self, c_src: &BlockResultColumn, c_src_idx: usize) {
        if self.rows_len == 0 {
            esl_common::panicf!("BUG: br.rowsLen must be greater than 0");
        }

        let mut c_dst = BlockResultColumn {
            name: c_src.name.clone(),
            ..Default::default()
        };

        if c_src.is_const {
            c_dst.is_const = true;
            c_dst.values_encoded = c_src.values_encoded.clone();
        } else if c_src.is_time {
            c_dst.is_time = true;
        } else {
            c_dst.value_type = c_src.value_type;
            c_dst.min_value = c_src.min_value;
            c_dst.max_value = c_src.max_value;
            c_dst.dict_values = c_src.dict_values.clone();
            // PORT NOTE: Go stores cSrc = *blockResultColumn into the source
            // block result. We store the source column index; the read path
            // resolves it through br_src.
            c_dst.c_src = Some(c_src_idx);
        }

        self.cs_add(c_dst);
    }

    /// Initializes `self` from the given rows.
    pub fn must_init_from_rows(&mut self, rows: &[Vec<Field>]) {
        self.reset();
        self.rows_len = rows.len();

        if rows.is_empty() {
            return;
        }

        if are_same_fields_in_rows(rows) {
            // Fast path - all rows have the same fields.
            let fields = &rows[0];
            for (i, f0) in fields.iter().enumerate() {
                let name = f0.name.clone();
                let values: Vec<Vec<u8>> = rows.iter().map(|row| row[i].value.clone()).collect();
                self.add_result_column(ResultColumn { name, values });
            }
            return;
        }

        // Slow path - rows have different fields. Assign a column index to each
        // field name in first-appearance order (matches Go's columnIdxs).
        let mut column_idxs: HashMap<Vec<u8>, usize> = HashMap::new();
        for fields in rows {
            for f in fields {
                let next = column_idxs.len();
                column_idxs.entry(f.name.clone()).or_insert(next);
            }
        }

        // Initialize columns as string columns with empty values.
        let ncols = column_idxs.len();
        let mut names: Vec<Vec<u8>> = vec![Vec::new(); ncols];
        for (name, &idx) in &column_idxs {
            names[idx] = name.clone();
        }
        let mut values_per_col: Vec<Vec<Vec<u8>>> =
            (0..ncols).map(|_| vec![Vec::new(); rows.len()]).collect();

        for (i, fields) in rows.iter().enumerate() {
            for f in fields {
                let idx = column_idxs[&f.name];
                values_per_col[idx][i] = f.value.clone();
            }
        }

        for (idx, values) in values_per_col.into_iter().enumerate() {
            self.cs_add(BlockResultColumn {
                name: std::mem::take(&mut names[idx]),
                value_type: ValueType::STRING,
                values_encoded: Some(values),
                ..Default::default()
            });
        }
    }

    /// Sets the given result columns as the block result columns.
    pub fn set_result_columns(&mut self, rcs: Vec<ResultColumn>, rows_len: usize) {
        self.reset();
        self.rows_len = rows_len;
        for rc in rcs {
            self.add_result_column(rc);
        }
    }

    /// Adds a float64 result column with the given min/max values.
    pub fn add_result_column_float64(&mut self, rc: ResultColumn, min_value: f64, max_value: f64) {
        if rc.values.len() != self.rows_len {
            // Panic text only: lossy view of the raw name bytes.
            esl_common::panicf!(
                "BUG: column {:?} must contain {} rows, but it contains {} rows",
                String::from_utf8_lossy(&rc.name),
                self.rows_len,
                rc.values.len()
            );
        }
        self.cs_add(BlockResultColumn {
            name: rc.name,
            value_type: ValueType::FLOAT64,
            min_value: min_value.to_bits(),
            max_value: max_value.to_bits(),
            values_encoded: Some(rc.values),
            ..Default::default()
        });
    }

    /// Adds a result column, choosing const or string encoding.
    pub fn add_result_column(&mut self, rc: ResultColumn) {
        if rc.values.len() != self.rows_len {
            // Panic text only: lossy view of the raw name bytes.
            esl_common::panicf!(
                "BUG: column {:?} must contain {} rows, but it contains {} rows",
                String::from_utf8_lossy(&rc.name),
                self.rows_len,
                rc.values.len()
            );
        }
        if are_const_values(&rc.values) {
            self.add_result_column_const(rc);
        } else {
            self.cs_add(BlockResultColumn {
                name: rc.name,
                value_type: ValueType::STRING,
                values_encoded: Some(rc.values),
                ..Default::default()
            });
        }
    }

    fn add_result_column_const(&mut self, mut rc: ResultColumn) {
        let v = std::mem::take(&mut rc.values[0]);
        self.cs_add(BlockResultColumn {
            name: rc.name,
            is_const: true,
            values_encoded: Some(vec![v]),
            ..Default::default()
        });
    }

    /// Initializes columns in `self` from the given block search and bitmap.
    ///
    /// The result is valid until `bs` or `bm` changes.
    pub fn must_init(&mut self, bs: &BlockSearch<'_>, bm: &Bitmap) {
        self.reset();
        self.rows_len = bm.ones_count();
        if self.rows_len == 0 {
            return;
        }
        self.bs = Some(bs as *const BlockSearch<'_> as *const ());
        self.bm = Some(bm as *const Bitmap);
    }

    /// Recovers the associated block search as a mutable raw pointer.
    ///
    /// # Safety contract
    /// The pointer is valid until `bs`/`bm` change, and is only accessed from a
    /// single thread — mirroring Go's `br.bs` raw pointer and the `br_src`
    /// pattern used elsewhere in this module. Callers must not use the pointer
    /// after `reset()`.
    #[inline]
    fn bs_ptr(&self) -> *mut BlockSearch<'static> {
        self.bs
            .expect("bs must be set for block-search-backed reads")
            as *mut BlockSearch<'static>
    }

    /// Returns the block's field names straight from its columns-header index,
    /// without materializing the columns — the `pipeFieldNames.writeBlock`
    /// fast path (Go reads `br.bs.getColumnsHeaderIndex()` directly).
    ///
    /// Returns `None` when the block is not backed by a block search or the
    /// part format predates the header index
    /// (Go: `br.bs == nil || br.bs.partFormatVersion() < 1`); callers must
    /// fall back to `get_columns` then. The returned names cover the column
    /// headers and const columns only — the caller adds the generated
    /// `_time` / `_stream` / `_stream_id` columns itself, like Go.
    pub(crate) fn field_names_from_columns_header_index(&mut self) -> Option<Vec<Vec<u8>>> {
        self.bs?;
        let bs = self.bs_ptr();
        // SAFETY: bs points at the BlockSearch that initialized this
        // BlockResult (`must_init`) and outlives it; single-threaded access
        // per the `bs_ptr` contract.
        unsafe {
            if (*bs).part_format_version() < 1 {
                return None;
            }
            let csh_index =
                (*bs).get_columns_header_index() as *const crate::block_header::ColumnsHeaderIndex;
            // SAFETY: csh_index points into bs's stable csh_index_cache; the
            // `get_column_name_by_id` calls below only take shared borrows of
            // bs and do not invalidate that cache.
            let refs = (*csh_index)
                .column_headers_refs
                .iter()
                .chain((*csh_index).const_columns_refs.iter());
            let mut names = Vec::new();
            for cr in refs {
                names.push((*bs).get_column_name_by_id(cr.column_name_id).to_vec());
            }
            Some(names)
        }
    }

    /// Initializes columns in `self` according to the given prefix filter.
    pub fn init_columns(&mut self, pf: &Filter) {
        if let Some(fields) = pf.get_allow_strings() {
            // Fast path.
            // PORT NOTE: `fields` borrows `pf`; clone the names so the
            // `&mut self` calls below don't fight the borrow (the values are
            // small field names, matching Go's `[]string`).
            let fields: Vec<Vec<u8>> = fields.to_vec();
            self.init_columns_by_fields(&fields);
        } else {
            // Slow path.
            self.init_columns_by_filter(pf);
        }

        self.cs_init_fast();
    }

    fn init_columns_by_fields(&mut self, fields: &[Vec<u8>]) {
        let bs = self.bs_ptr();
        for f in fields {
            match f.as_slice() {
                b"_time" => self.add_time_column(),
                b"_stream_id" => self.add_stream_id_column(),
                b"_stream" => {
                    if !self.add_stream_column() {
                        // Skip the current block, since the associated stream
                        // tags are missing.
                        self.reset();
                        return;
                    }
                }
                _ => {
                    // SAFETY: bs is valid for the lifetime of this block result.
                    let v = unsafe { (*bs).get_const_column_value(f) };
                    if !v.is_empty() {
                        self.add_const_column(f, &v);
                    } else if let Some(ch) =
                        unsafe { (*bs).get_column_header(f).map(|c| c as *const ColumnHeader) }
                    {
                        // SAFETY: ch points into bs's heap-stable header caches
                        // (boxed chs_cache / csh_cache), valid while bs lives.
                        self.add_column(unsafe { &*ch });
                    } else {
                        self.add_const_column(f, "");
                    }
                }
            }
        }
    }

    fn init_columns_by_filter(&mut self, pf: &Filter) {
        let bs = self.bs_ptr();

        if pf.match_string("_time") {
            self.add_time_column();
        }
        if pf.match_string("_stream_id") {
            self.add_stream_id_column();
        }
        if pf.match_string("_stream") && !self.add_stream_column() {
            // Skip the current block, since the associated stream tags are missing.
            self.reset();
            return;
        }
        if pf.match_string("_msg") {
            // SAFETY: bs is valid for the lifetime of this block result.
            let v = unsafe { (*bs).get_const_column_value(b"_msg") };
            if !v.is_empty() {
                self.add_const_column("_msg", &v);
            } else if let Some(ch) = unsafe {
                (*bs)
                    .get_column_header(b"_msg")
                    .map(|c| c as *const ColumnHeader)
            } {
                self.add_column(unsafe { &*ch });
            } else {
                self.add_const_column("_msg", "");
            }
        }

        // Add other const columns and non-const columns from the columnsHeader.
        // SAFETY: csh is bs's stable, cached columnsHeader; the raw borrow ends
        // before the mutating `add_*` calls, which do not touch csh.
        let csh: &ColumnsHeader = unsafe { &*((*bs).get_columns_header() as *const ColumnsHeader) };

        let mut const_to_add: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for cc in &csh.const_columns {
            if is_special_column(&cc.name) {
                // Special columns have been added above.
                continue;
            }
            if pf.match_string(&cc.name) && unsafe { !(*bs).is_hidden_field(&cc.name) } {
                const_to_add.push((cc.name.clone(), cc.value.clone()));
            }
        }

        let mut cols_to_add: Vec<*const ColumnHeader> = Vec::new();
        for ch in &csh.column_headers {
            if is_special_column(&ch.name) {
                // Special columns have been added above.
                continue;
            }
            if pf.match_string(&ch.name) && unsafe { !(*bs).is_hidden_field(&ch.name) } {
                cols_to_add.push(ch as *const ColumnHeader);
            }
        }

        for (name, value) in const_to_add {
            self.add_const_column(&name, &value);
        }
        for ch in cols_to_add {
            // SAFETY: ch points into csh's stable column_headers, valid while bs lives.
            self.add_column(unsafe { &*ch });
        }
    }

    /// Adds the `_stream_id` const column from the block search.
    pub fn add_stream_id_column(&mut self) {
        // SAFETY: bs is valid for the lifetime of this block result.
        let bs = self.bs_ptr();
        let mut bb: Vec<u8> = Vec::new();
        unsafe { (*bs).block_header().stream_id.marshal_string(&mut bb) };
        self.add_const_column("_stream_id", &bb);
    }

    /// Adds the `_stream` const column from the block search, returning false
    /// when the stream tags are missing.
    pub fn add_stream_column(&mut self) -> bool {
        // SAFETY: bs is valid for the lifetime of this block result.
        let bs = self.bs_ptr();
        let stream_str = unsafe { (*bs).get_stream_str() };
        if stream_str.is_empty() {
            return false;
        }
        self.add_const_column("_stream", &stream_str);
        true
    }

    // -- block_stats source info (Go `pipeBlockStatsProcessor.writeBlock`) ---

    /// Returns `(_stream, part_path)` when this block is backed by a block
    /// search (Go `br.bs != nil`: `bs.getStreamStr()` / `bs.partPath()`), or
    /// `None` for in-memory blocks.
    pub(crate) fn block_stats_stream_and_part_path(&mut self) -> Option<(String, String)> {
        self.bs?;
        // SAFETY: bs is valid for the lifetime of this block result.
        let bs = self.bs_ptr();
        unsafe { Some(((*bs).get_stream_str(), (*bs).part_path())) }
    }

    /// Returns the on-disk size of the block's timestamps
    /// (Go `br.bs.bsw.bh.timestampsHeader.blockSize`), or 0 for in-memory
    /// blocks.
    pub(crate) fn block_stats_timestamps_block_size(&self) -> u64 {
        if self.bs.is_none() {
            return 0;
        }
        // SAFETY: bs is valid for the lifetime of this block result.
        let bs = self.bs_ptr();
        unsafe { (*bs).block_header().timestamps_header.block_size }
    }

    /// Returns `(values_size, bloom_filter_size, dict_items, dict_size)` for
    /// the named column from its column header (Go
    /// `br.bs.getColumnHeader(c.name)`); `is_dict` gates the dict-size sum
    /// like Go's `c.valueType == valueTypeDict` check. Returns `None` for
    /// in-memory blocks.
    pub(crate) fn block_stats_column_header(
        &mut self,
        name: &[u8],
        is_dict: bool,
    ) -> Option<(u64, u64, u64, u64)> {
        self.bs?;
        // SAFETY: bs is valid for the lifetime of this block result.
        let bs = self.bs_ptr();
        unsafe {
            let Some(ch) = (*bs).get_column_header(name) else {
                // Unreachable for columns materialized from this block search
                // (Go would nil-panic here); keep the row with zero sizes.
                return Some((0, 0, 0, 0));
            };
            let dict_items = ch.values_dict.values.len() as u64;
            let mut dict_size = 0u64;
            if is_dict {
                for v in &ch.values_dict.values {
                    dict_size += v.len() as u64;
                }
            }
            Some((ch.values_size, ch.bloom_filter_size, dict_items, dict_size))
        }
    }

    fn cs_init_fast(&mut self) {
        self.cs.clear();
        let n = self.cs_buf.len();
        self.cs = (0..n).collect();
        self.cs_initialized = true;
    }

    /// Adds a column for the given column header.
    pub fn add_column(&mut self, ch: &ColumnHeader) {
        self.cs_buf.push(BlockResultColumn {
            name: get_canonical_column_name_bytes(&ch.name).to_vec(),
            value_type: ch.value_type,
            min_value: ch.min_value,
            max_value: ch.max_value,
            dict_values: ch.values_dict.values.clone(),
            ch_src: Some(ch as *const ColumnHeader),
            ..Default::default()
        });
        self.cs_initialized = false;
    }

    /// Adds the `_time` column.
    pub fn add_time_column(&mut self) {
        self.cs_add(BlockResultColumn {
            name: b"_time".to_vec(),
            is_time: true,
            ..Default::default()
        });
    }

    /// Adds a const column with the given name and value (raw value bytes).
    pub fn add_const_column(&mut self, name: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.cs_add(BlockResultColumn {
            name: name.as_ref().to_vec(),
            is_const: true,
            values_encoded: Some(vec![value.as_ref().to_vec()]),
            ..Default::default()
        });
    }

    // -- timestamps ----------------------------------------------------------

    /// Returns true if the block timestamps intersect the given range.
    pub fn intersects_time_range(&mut self, min_timestamp: i64, max_timestamp: i64) -> bool {
        min_timestamp <= self.get_max_timestamp(min_timestamp)
            && max_timestamp >= self.get_min_timestamp(max_timestamp)
    }

    pub(crate) fn get_min_timestamp(&mut self, mut min_timestamp: i64) -> i64 {
        if self.bs.is_some() {
            // SAFETY: bs is valid for the lifetime of this block result.
            let bs = self.bs_ptr();
            let th_min = unsafe { (*bs).block_header().timestamps_header.min_timestamp };
            if self.is_full() {
                // Fast path - all the rows in the br are present, so return the
                // minTimestamp from blockHeader without reading the timestamps.
                return min_timestamp.min(th_min);
            }
            if min_timestamp <= th_min {
                return min_timestamp;
            }
        }

        let c = self.get_column_by_name(b"_time");
        let is_time = self.col(c).is_time;
        let timestamps = self.get_timestamps();
        if is_time {
            if !timestamps.is_empty() {
                return min_timestamp.min(timestamps[0]);
            }
            return min_timestamp;
        }
        for &timestamp in timestamps {
            if timestamp < min_timestamp {
                min_timestamp = timestamp;
            }
        }
        min_timestamp
    }

    pub(crate) fn get_max_timestamp(&mut self, mut max_timestamp: i64) -> i64 {
        if self.bs.is_some() {
            // SAFETY: bs is valid for the lifetime of this block result.
            let bs = self.bs_ptr();
            let th_max = unsafe { (*bs).block_header().timestamps_header.max_timestamp };
            if self.is_full() {
                // Fast path - all the rows in the br are present, so return the
                // maxTimestamp from blockHeader without reading the timestamps.
                return max_timestamp.max(th_max);
            }
            if max_timestamp >= th_max {
                return max_timestamp;
            }
        }

        let c = self.get_column_by_name(b"_time");
        let is_time = self.col(c).is_time;
        let timestamps = self.get_timestamps();
        if is_time {
            if !timestamps.is_empty() {
                return max_timestamp.max(timestamps[timestamps.len() - 1]);
            }
            return max_timestamp;
        }
        for &timestamp in timestamps.iter().rev() {
            if timestamp > max_timestamp {
                max_timestamp = timestamp;
            }
        }
        max_timestamp
    }

    /// Returns the timestamps for the selected log entries.
    pub fn get_timestamps(&mut self) -> &[i64] {
        if self.rows_len > 0 && self.timestamps_buf.is_empty() {
            self.init_timestamps();
        }
        &self.timestamps_buf
    }

    fn init_timestamps(&mut self) {
        if let Some(br_src) = self.br_src {
            // SAFETY: br_src is valid for the lifetime of this block result,
            // mirroring Go's pointer contract.
            let src = unsafe { &mut *(br_src as *mut BlockResult) };
            let src_timestamps = src.get_timestamps().to_vec();
            self.init_timestamps_internal(&src_timestamps);
            return;
        }
        if self.bs.is_some() {
            // SAFETY: bs is valid for the lifetime of this block result.
            let bs = self.bs_ptr();
            let src_timestamps = unsafe { (*bs).get_timestamps() }.to_vec();
            self.init_timestamps_internal(&src_timestamps);
            return;
        }

        // Try decoding timestamps from the _time field.
        let c = self.get_column_by_name(b"_time");
        let timestamp_values: Vec<Vec<u8>> = self.column_get_values(c).to_vec();
        self.timestamps_buf.clear();
        // Checked UTF-8 views: a non-UTF-8 value cannot be a valid timestamp,
        // so it fails the parse exactly like in Go.
        let strs: Option<Vec<&str>> = timestamp_values
            .iter()
            .map(|v| std::str::from_utf8(v).ok())
            .collect();
        if let Some(ts) = strs.as_deref().and_then(try_parse_timestamps) {
            self.timestamps_buf = ts;
        } else {
            self.timestamps_buf.clear();
            fastnum::append_int64_zeros(&mut self.timestamps_buf, self.rows_len);
        }
    }

    fn init_timestamps_internal(&mut self, src_timestamps: &[i64]) {
        // SAFETY: bm is valid for the lifetime of this block result.
        let bm = self.bm.map(|p| unsafe { &*p });
        self.timestamps_buf.clear();
        match bm {
            Some(bm) if bm.are_all_bits_set() => {
                self.timestamps_buf.extend_from_slice(src_timestamps);
            }
            Some(bm) => {
                let dst = &mut self.timestamps_buf;
                bm.for_each_set_bit_readonly(|idx| dst.push(src_timestamps[idx]));
            }
            None => {
                self.timestamps_buf.extend_from_slice(src_timestamps);
            }
        }
        self.check_timestamps_len();
    }

    fn check_timestamps_len(&self) {
        if self.timestamps_buf.len() != self.rows_len {
            esl_common::panicf!(
                "BUG: unexpected number of timestamps; got {}; want {}",
                self.timestamps_buf.len(),
                self.rows_len
            );
        }
    }

    fn is_full(&self) -> bool {
        match self.bs {
            None => false,
            Some(_) => {
                // SAFETY: bs is valid for the lifetime of this block result.
                let bs = self.bs_ptr();
                unsafe { (*bs).block_header().rows_count as usize == self.rows_len }
            }
        }
    }

    // -- column lookup -------------------------------------------------------

    /// Returns the columns of the block result (as [`ColRef`] handles).
    pub fn get_columns(&mut self) -> Vec<ColRef> {
        if !self.cs_initialized {
            self.cs_init();
        }
        self.cs.iter().map(|&i| ColRef::Buf(i)).collect()
    }

    /// Returns the column with the given name (raw bytes), creating an empty
    /// column if it is missing.
    pub fn get_column_by_name(&mut self, column_name: &[u8]) -> ColRef {
        if !self.cs_initialized {
            self.cs_init();
        }
        let column_name = get_canonical_column_name_bytes(column_name);
        for &ci in &self.cs {
            if self.cs_buf[ci].name == column_name {
                return ColRef::Buf(ci);
            }
        }
        let column_name = column_name.to_vec();
        self.get_empty_column_by_name(&column_name)
    }

    fn get_empty_column_by_name(&mut self, column_name: &[u8]) -> ColRef {
        for (i, c) in self.cs_empty.iter().enumerate() {
            if c.name == column_name {
                return ColRef::Empty(i);
            }
        }
        self.cs_empty.push(BlockResultColumn {
            name: column_name.to_vec(),
            is_const: true,
            values_encoded: Some(vec![Vec::new()]),
            ..Default::default()
        });
        ColRef::Empty(self.cs_empty.len() - 1)
    }

    fn cs_init(&mut self) {
        self.cs.clear();
        let n = self.cs_buf.len();
        for i in 0..n {
            self.cs_add_or_replace(i);
        }
        self.cs_initialized = true;
    }

    fn cs_add(&mut self, rc: BlockResultColumn) {
        self.cs_buf.push(rc);
        if !self.cs_initialized {
            return;
        }
        let idx = self.cs_buf.len() - 1;
        self.cs_add_or_replace(idx);
    }

    fn cs_add_or_replace(&mut self, ci: usize) {
        let name = &self.cs_buf[ci].name;
        let mut found = None;
        for (pos, &existing) in self.cs.iter().enumerate() {
            if &self.cs_buf[existing].name == name {
                found = Some(pos);
                break;
            }
        }
        match found {
            Some(pos) => self.cs[pos] = ci,
            None => self.cs.push(ci),
        }
    }

    /// Removes the first `skip_rows` rows.
    pub fn skip_rows(&mut self, skip_rows: usize) {
        let timestamps: Vec<i64> = self.get_timestamps()[skip_rows..].to_vec();
        self.timestamps_buf = timestamps;
        self.rows_len -= skip_rows;
        self.check_timestamps_len();

        let cols = self.get_columns();
        for &r in &cols {
            self.column_ensure_values_encoded(r);
            let c = self.col_mut(r);
            if let Some(values) = c.values.as_mut() {
                values.drain(..skip_rows);
            }
            if c.is_const {
                continue;
            }
            if let Some(ve) = c.values_encoded.as_mut() {
                ve.drain(..skip_rows);
            }
            if let Some(vb) = c.values_bucketed.as_mut() {
                vb.drain(..skip_rows);
            }
        }
    }

    /// Keeps only the first `keep_rows` rows.
    pub fn truncate_rows(&mut self, keep_rows: usize) {
        // Materialize the lazily-initialized timestamps before truncating them
        // (mirrors `skip_rows` above); otherwise a not-yet-materialized
        // `timestamps_buf` stays empty and `check_timestamps_len` panics.
        let _ = self.get_timestamps();
        self.timestamps_buf.truncate(keep_rows);
        self.rows_len = keep_rows;
        self.check_timestamps_len();

        let cols = self.get_columns();
        for &r in &cols {
            self.column_ensure_values_encoded(r);
            let c = self.col_mut(r);
            if let Some(values) = c.values.as_mut() {
                values.truncate(keep_rows);
            }
            if c.is_const {
                continue;
            }
            if let Some(ve) = c.values_encoded.as_mut() {
                ve.truncate(keep_rows);
            }
            if let Some(vb) = c.values_bucketed.as_mut() {
                vb.truncate(keep_rows);
            }
        }
    }

    // -- column filters ------------------------------------------------------

    /// Copies columns from `src_column_filters` to `dst_column_filters`.
    pub fn copy_columns_by_filters(
        &mut self,
        src_column_filters: &[Vec<u8>],
        dst_column_filters: &[Vec<u8>],
    ) {
        for (src_filter, dst_filter) in src_column_filters.iter().zip(dst_column_filters) {
            self.copy_columns_by_filter(src_filter, dst_filter);
        }
    }

    fn copy_columns_by_filter(&mut self, src_filter: &[u8], dst_filter: &[u8]) {
        let mut found = false;
        let cols = self.get_columns();
        let mut to_add: Vec<BlockResultColumn> = Vec::new();
        for &r in &cols {
            let c = self.col(r);
            if !match_filter(src_filter, &c.name) {
                continue;
            }
            let mut buf = Vec::new();
            append_replace(&mut buf, src_filter, dst_filter, &c.name);
            let mut c_copy = c.clone_shallow();
            c_copy.name = buf;
            to_add.push(c_copy);
            found = true;
        }
        for c in to_add {
            self.cs_add(c);
        }
        if !found && !is_wildcard_filter(src_filter) {
            self.add_const_column(dst_filter, "");
        }
    }

    /// Renames columns from `src_column_filters` to `dst_column_filters`.
    pub fn rename_columns_by_filters(
        &mut self,
        src_column_filters: &[Vec<u8>],
        dst_column_filters: &[Vec<u8>],
    ) {
        for (src_filter, dst_filter) in src_column_filters.iter().zip(dst_column_filters) {
            self.rename_columns_by_filter(src_filter, dst_filter);
        }
    }

    fn rename_columns_by_filter(&mut self, src_filter: &[u8], dst_filter: &[u8]) {
        let cols = self.get_columns();
        self.cs_initialized = false;

        let mut new_buf: Vec<BlockResultColumn> = Vec::new();
        let mut found = false;
        for &r in &cols {
            let c = self.col(r);
            if !match_filter(src_filter, &c.name) {
                new_buf.push(c.clone_shallow());
            }
        }
        for &r in &cols {
            let c = self.col(r);
            if !match_filter(src_filter, &c.name) {
                continue;
            }
            let mut buf = Vec::new();
            append_replace(&mut buf, src_filter, dst_filter, &c.name);
            let mut c_copy = c.clone_shallow();
            c_copy.name = buf;
            new_buf.push(c_copy);
            found = true;
        }

        self.cs_buf = new_buf;
        if !found && !is_wildcard_filter(src_filter) {
            self.add_const_column(dst_filter, "");
        }
    }

    /// Deletes columns matching the given column filters.
    pub fn delete_columns_by_filters(&mut self, column_filters: &[Vec<u8>]) {
        if column_filters.is_empty() {
            return;
        }
        let cols = self.get_columns();
        self.cs_initialized = false;

        let mut new_buf: Vec<BlockResultColumn> = Vec::new();
        for &r in &cols {
            let c = self.col(r);
            if !match_filters(column_filters, &c.name) {
                new_buf.push(c.clone_shallow());
            }
        }
        self.cs_buf = new_buf;
    }

    /// Sets the resulting columns according to the given column filters.
    pub fn set_column_filters(&mut self, column_filters: &[Vec<u8>]) {
        let cols = self.get_columns();

        if !has_wildcard_filters(column_filters) {
            if self.are_same_columns(&cols, column_filters) {
                return;
            }
            self.cs_initialized = false;
            let mut new_buf: Vec<BlockResultColumn> = Vec::new();
            for field in column_filters {
                if let Some(&r) = self.find_column_by_name(&cols, field) {
                    new_buf.push(self.col(r).clone_shallow());
                } else {
                    let mut c = BlockResultColumn {
                        name: field.clone(),
                        is_const: true,
                        values_encoded: Some(vec![Vec::new()]),
                        ..Default::default()
                    };
                    // add_const_column adds directly to cs_buf; here we collect.
                    c.value_type = ValueType::UNKNOWN;
                    new_buf.push(c);
                }
            }
            self.cs_buf = new_buf;
            return;
        }

        if self.are_same_wildcard_columns(&cols, column_filters) {
            return;
        }
        self.cs_initialized = false;
        let mut new_buf: Vec<BlockResultColumn> = Vec::new();
        for &r in &cols {
            let c = self.col(r);
            if match_filters(column_filters, &c.name) {
                new_buf.push(c.clone_shallow());
            }
        }
        for column_filter in column_filters {
            if is_wildcard_filter(column_filter) {
                continue;
            }
            if self.find_column_by_name(&cols, column_filter).is_none() {
                new_buf.push(BlockResultColumn {
                    name: column_filter.to_vec(),
                    is_const: true,
                    values_encoded: Some(vec![Vec::new()]),
                    ..Default::default()
                });
            }
        }
        self.cs_buf = new_buf;
    }

    fn are_same_columns(&self, cols: &[ColRef], column_filters: &[Vec<u8>]) -> bool {
        if cols.len() != column_filters.len() {
            return false;
        }
        cols.iter()
            .zip(column_filters)
            .all(|(&r, name)| &self.col(r).name == name)
    }

    fn are_same_wildcard_columns(&self, cols: &[ColRef], column_filters: &[Vec<u8>]) -> bool {
        for &r in cols {
            if !match_filters(column_filters, &self.col(r).name) {
                return false;
            }
        }
        for column_filter in column_filters {
            if is_wildcard_filter(column_filter) {
                continue;
            }
            if self.find_column_by_name(cols, column_filter).is_none() {
                return false;
            }
        }
        true
    }

    fn find_column_by_name<'a>(&self, cols: &'a [ColRef], name: &[u8]) -> Option<&'a ColRef> {
        cols.iter().find(|&&r| self.col(r).name == name)
    }

    // -- column accessors ----------------------------------------------------

    /// Returns the column referenced by `r`.
    fn col(&self, r: ColRef) -> &BlockResultColumn {
        match r {
            ColRef::Buf(i) => &self.cs_buf[i],
            ColRef::Empty(i) => &self.cs_empty[i],
        }
    }

    fn col_mut(&mut self, r: ColRef) -> &mut BlockResultColumn {
        match r {
            ColRef::Buf(i) => &mut self.cs_buf[i],
            ColRef::Empty(i) => &mut self.cs_empty[i],
        }
    }

    /// Returns the name of the column referenced by `r` (raw bytes; Go
    /// strings are arbitrary bytes).
    pub fn column_name(&self, r: ColRef) -> &[u8] {
        &self.col(r).name
    }

    /// Returns the value type of the column referenced by `r`.
    pub fn column_value_type(&self, r: ColRef) -> ValueType {
        self.col(r).value_type
    }

    /// Returns true if the column referenced by `r` is a const column.
    pub fn column_is_const(&self, r: ColRef) -> bool {
        self.col(r).is_const
    }

    /// Returns true if the column referenced by `r` is the `_time` column.
    pub fn column_is_time(&self, r: ColRef) -> bool {
        self.col(r).is_time
    }

    /// Materializes and returns the encoded values of the given column.
    fn column_ensure_values_encoded(&mut self, r: ColRef) {
        if self.col(r).is_time || self.col(r).values_encoded.is_some() {
            return;
        }
        let ve = self.read_values_encoded(r);
        self.col_mut(r).values_encoded = Some(ve);
    }

    /// Returns the encoded values of the given column.
    pub fn column_get_values_encoded(&mut self, r: ColRef) -> Option<&[Vec<u8>]> {
        if self.col(r).is_time {
            return None;
        }
        self.column_ensure_values_encoded(r);
        self.col(r).values_encoded.as_deref()
    }

    fn read_values_encoded(&mut self, r: ColRef) -> Vec<Vec<u8>> {
        if self.bs.is_some() {
            let ch = match self.col(r).ch_src {
                Some(ch) => ch,
                None => {
                    esl_common::panicf!("BUG: ch_src must be set for a block-search column");
                    unreachable!()
                }
            };
            // SAFETY: ch points into bs's heap-stable header caches, valid while
            // bs lives (mirrors Go's `c.chSrc` pointer).
            let ch: &ColumnHeader = unsafe { &*ch };
            return self.read_values_encoded_from_column_header(ch);
        }
        // brSrc path.
        let c_src_idx = match self.col(r).c_src {
            Some(idx) => idx,
            None => {
                esl_common::panicf!("BUG: c_src must be set for a non-block-search column");
                unreachable!()
            }
        };
        let br_src = self.br_src.expect("br_src must be set when c_src is set");
        // SAFETY: br_src is valid for the lifetime of this block result.
        let src = unsafe { &mut *(br_src as *mut BlockResult) };
        let src_encoded = src
            .column_get_values_encoded(ColRef::Buf(c_src_idx))
            .map(|s| s.to_vec())
            .unwrap_or_default();
        // SAFETY: bm is valid for the lifetime of this block result.
        let mut out = Vec::new();
        match self.bm.map(|p| unsafe { &*p }) {
            Some(bm) => bm.for_each_set_bit_readonly(|idx| out.push(src_encoded[idx].clone())),
            None => out = src_encoded,
        }
        out
    }

    /// Reads the encoded values for the given column header from the block
    /// search, applying the selection bitmap and validating fixed-width sizes.
    ///
    /// PORT NOTE: mirrors Go's `readValuesEncodedFromColumnHeader` +
    /// `visitValuesReadonly`. Go appends `string` views into a shared
    /// `valuesBuf`; the port copies the selected byte slices into an owned
    /// `Vec<Vec<u8>>` (the established arena divergence for this module).
    fn read_values_encoded_from_column_header(&mut self, ch: &ColumnHeader) -> Vec<Vec<u8>> {
        // SAFETY: bs is valid for the lifetime of this block result. `ch` points
        // into bs's own header caches; `get_values_for_column` only reads `ch`
        // and mutates unrelated bs caches, so the aliasing is benign (Go passes
        // the same `*columnHeader` into `bs.getValuesForColumn`).
        let bs = self.bs_ptr();
        let bm = self.bm.map(|p| unsafe { &*p });

        // Fast path - nothing to visit.
        if let Some(bm) = bm
            && bm.is_zero()
        {
            return Vec::new();
        }

        let values = unsafe { (*bs).get_values_for_column(ch) };
        let selected: Vec<Vec<u8>> = match bm {
            Some(bm) if bm.are_all_bits_set() => values.to_vec(),
            Some(bm) => {
                let mut out = Vec::new();
                bm.for_each_set_bit_readonly(|idx| out.push(values[idx].clone()));
                out
            }
            None => values.to_vec(),
        };

        // Validate fixed-width value sizes and dict indices, matching Go's
        // per-valueType checks in readValuesEncodedFromColumnHeader.
        let part_path = || unsafe { (*bs).part_path() };
        let check_size = |size_expected: usize, type_str: &str| {
            for v in &selected {
                if v.len() != size_expected {
                    esl_common::panicf!(
                        "FATAL: {}: {}: unexpected size for {} column {:?}; got {} bytes; want {} bytes",
                        type_str,
                        part_path(),
                        type_str,
                        ch.name,
                        v.len(),
                        size_expected
                    );
                }
            }
        };
        match ch.value_type {
            ValueType::STRING => {}
            ValueType::DICT => {
                check_size(1, "dict");
                let dict_len = ch.values_dict.values.len();
                for v in &selected {
                    let dict_idx = v[0] as usize;
                    if dict_idx >= dict_len {
                        esl_common::panicf!(
                            "FATAL: {}: too big dict index for column {:?}: {}; should be smaller than {}",
                            part_path(),
                            ch.name,
                            dict_idx,
                            dict_len
                        );
                    }
                }
            }
            ValueType::UINT8 => check_size(1, "uint8"),
            ValueType::UINT16 => check_size(2, "uint16"),
            ValueType::UINT32 => check_size(4, "uint32"),
            ValueType::UINT64 => check_size(8, "uint64"),
            ValueType::INT64 => check_size(8, "int64"),
            ValueType::FLOAT64 => check_size(8, "float64"),
            ValueType::IPV4 => check_size(4, "ipv4"),
            ValueType::TIMESTAMP_ISO8601 => check_size(8, "iso8601"),
            _ => {
                esl_common::panicf!(
                    "FATAL: {}: unknown valueType={} for column {:?}",
                    part_path(),
                    ch.value_type.0,
                    ch.name
                );
            }
        }

        selected
    }

    /// Returns the decoded values of the given column.
    pub fn column_get_values(&mut self, r: ColRef) -> &[Vec<u8>] {
        if self.col(r).values.is_some() {
            return self.col(r).values.as_deref().unwrap();
        }
        let values = self.new_values_for_column(r);
        self.col_mut(r).values = Some(values);
        self.col(r).values.as_deref().unwrap()
    }

    /// Returns the value of the given column at the given row (raw bytes).
    pub fn column_get_value_at_row(&mut self, r: ColRef, row_idx: usize) -> &[u8] {
        if self.col(r).is_const {
            return &self.col(r).values_encoded.as_ref().unwrap()[0];
        }
        if self.col(r).values.is_some() {
            return &self.col(r).values.as_ref().unwrap()[row_idx];
        }
        let values = self.new_values_for_column(r);
        self.col_mut(r).values = Some(values);
        &self.col(r).values.as_ref().unwrap()[row_idx]
    }

    fn new_values_for_column(&mut self, r: ColRef) -> Vec<Vec<u8>> {
        let rows_len = self.rows_len;
        if self.col(r).is_const {
            let v = self.col(r).values_encoded.as_ref().unwrap()[0].clone();
            return get_const_values(&v, rows_len);
        }
        if self.col(r).is_time {
            let timestamps = self.get_timestamps().to_vec();
            return get_timestamp_values(&timestamps);
        }
        self.column_ensure_values_encoded(r);
        self.col(r).compute_values(rows_len)
    }

    /// Returns the bucketed values of the given column.
    pub(crate) fn column_get_values_bucketed(
        &mut self,
        r: ColRef,
        bf: &ByStatsField,
    ) -> Vec<Vec<u8>> {
        {
            let c = self.col(r);
            let cached_matches = c.values_bucketed.is_some()
                && c.bucket_size_str == bf.bucket_size_str
                && c.bucket_offset_str == bf.bucket_offset_str;
            if cached_matches {
                return c.values_bucketed.clone().unwrap();
            }
        }
        let rows_len = self.rows_len;
        let vb = if self.col(r).is_const {
            let v = self.col(r).values_encoded.as_ref().unwrap()[0].clone();
            let s = get_bucketed_value(&v, bf);
            get_const_values(&s, rows_len)
        } else if self.col(r).is_time {
            let timestamps = self.get_timestamps().to_vec();
            get_bucketed_timestamp_values(&timestamps, bf)
        } else {
            self.column_ensure_values_encoded(r);
            self.col(r).compute_values_bucketed(rows_len, bf)
        };
        let c = self.col_mut(r);
        c.values_bucketed = Some(vb.clone());
        c.bucket_size_str = bf.bucket_size_str.clone();
        c.bucket_offset_str = bf.bucket_offset_str.clone();
        vb
    }

    /// Returns the float value of the given column at the given row.
    pub fn column_get_float_value_at_row(&mut self, r: ColRef, row_idx: usize) -> Option<f64> {
        if self.col(r).is_const {
            let v = self.col(r).values_encoded.as_ref().unwrap()[0].clone();
            return try_parse_float64_bytes(&v);
        }
        if self.col(r).is_time {
            return None;
        }
        self.column_ensure_values_encoded(r);
        self.col(r).float_value_at_row(row_idx)
    }

    /// Returns the sum of the lengths of the string representations of the
    /// column values.
    pub fn column_sum_len_values(&mut self, r: ColRef) -> u64 {
        let rows_len = self.rows_len as u64;
        if self.col(r).is_const {
            let v = &self.col(r).values_encoded.as_ref().unwrap()[0];
            return v.len() as u64 * rows_len;
        }
        if self.col(r).is_time {
            return RFC3339_NANO_LEN as u64 * rows_len;
        }
        if self.col(r).value_type == ValueType::TIMESTAMP_ISO8601 {
            return ISO8601_TIMESTAMP_LEN as u64 * rows_len;
        }
        if self.col(r).value_type == ValueType::DICT {
            self.column_ensure_values_encoded(r);
            let c = self.col(r);
            let mut n = 0u64;
            for v in c.values_encoded.as_ref().unwrap() {
                n += c.dict_values[v[0] as usize].len() as u64;
            }
            return n;
        }
        // string / uint* / int64 / float64 / ipv4 use the decoded lengths.
        let values = self.column_get_values(r);
        values.iter().map(|v| v.len() as u64).sum()
    }

    /// Returns the sum of the numeric column values and the count of parsed
    /// values.
    pub fn column_sum_values(&mut self, r: ColRef) -> (f64, usize) {
        let rows_len = self.rows_len;
        if self.col(r).is_const {
            let v = self.col(r).values_encoded.as_ref().unwrap()[0].clone();
            return match try_parse_float64_bytes(&v) {
                Some(f) => (f * rows_len as f64, rows_len),
                None => (0.0, 0),
            };
        }
        if self.col(r).is_time {
            return (0.0, 0);
        }
        self.column_ensure_values_encoded(r);
        self.col(r).sum_values(rows_len)
    }
}

// PORT NOTE: raw pointers are only read from a single thread (Go's contract);
// declaring the marker traits keeps `BlockResult` usable where the Go value is.
unsafe impl Send for BlockResult {}

// ---------------------------------------------------------------------------
// BlockResultColumn
// ---------------------------------------------------------------------------

/// A named column of a [`BlockResult`].
///
/// The column doesn't own the source-backed data (`ch_src`/`c_src`); those
/// resolve through the owning block result.
#[derive(Default, Clone)]
pub struct BlockResultColumn {
    /// Column name (raw bytes; Go strings are arbitrary bytes).
    name: Vec<u8>,
    /// True if the column is a const column (value in `values_encoded[0]`).
    is_const: bool,
    /// True if the column contains `_time` values.
    is_time: bool,
    /// Type of a non-const value.
    value_type: ValueType,
    /// Minimum encoded value for numeric/ipv4/timestamp columns.
    min_value: u64,
    /// Maximum encoded value for numeric/ipv4/timestamp columns.
    max_value: u64,
    /// Dict values for `ValueType::DICT` (raw value bytes).
    dict_values: Vec<Vec<u8>>,
    /// Encoded values (materialized on demand).
    values_encoded: Option<Vec<Vec<u8>>>,
    /// Decoded values (materialized on demand).
    values: Option<Vec<Vec<u8>>>,
    /// Bucketed values (materialized on demand).
    values_bucketed: Option<Vec<Vec<u8>>>,
    /// Source column header for block-search-backed reads.
    ///
    /// PORT NOTE: written by `add_column`, read by `read_values_encoded`'s
    /// block-search branch. Points into the owning block search's heap-stable
    /// header caches (boxed `chs_cache` / cached `csh_cache`), mirroring Go's
    /// `chSrc *columnHeader`.
    ch_src: Option<*const ColumnHeader>,
    /// Source column index in `br_src` for filtered reads.
    c_src: Option<usize>,
    /// Bucket size string for `values_bucketed`.
    bucket_size_str: String,
    /// Bucket offset string for `values_bucketed`.
    bucket_offset_str: String,
}

impl BlockResultColumn {
    fn size_bytes(&self) -> usize {
        let mut n = std::mem::size_of::<Self>() + self.name.len();
        for v in &self.dict_values {
            n += v.len();
        }
        for vs in [&self.values_encoded, &self.values, &self.values_bucketed]
            .into_iter()
            .flatten()
        {
            for v in vs {
                n += v.len();
            }
        }
        n
    }

    /// Returns a deep clone with materialized (owned) values, detached from any
    /// source. Mirrors Go's `blockResultColumn.clone`.
    fn clone_column(&self) -> BlockResultColumn {
        BlockResultColumn {
            name: self.name.clone(),
            is_const: self.is_const,
            is_time: self.is_time,
            value_type: self.value_type,
            min_value: self.min_value,
            max_value: self.max_value,
            dict_values: self.dict_values.clone(),
            values_encoded: self.values_encoded.clone(),
            values: if self.value_type != ValueType::STRING {
                self.values.clone()
            } else {
                None
            },
            values_bucketed: self.values_bucketed.clone(),
            ch_src: None,
            c_src: None,
            bucket_size_str: self.bucket_size_str.clone(),
            bucket_offset_str: self.bucket_offset_str.clone(),
        }
    }

    /// Shallow copy preserving source pointers (mirrors Go's `cCopy := *c`).
    fn clone_shallow(&self) -> BlockResultColumn {
        self.clone()
    }

    fn compute_values(&self, _rows_len: usize) -> Vec<Vec<u8>> {
        let ve = self
            .values_encoded
            .as_ref()
            .expect("valuesEncoded must be set");
        match self.value_type {
            ValueType::STRING => ve.clone(),
            ValueType::DICT => ve
                .iter()
                .map(|v| self.dict_values[v[0] as usize].clone())
                .collect(),
            ValueType::UINT8 => map_encoded(ve, |v| {
                let mut b = Vec::new();
                marshal_uint8_string(&mut b, unmarshal_uint8(v));
                b
            }),
            ValueType::UINT16 => map_encoded(ve, |v| {
                let mut b = Vec::new();
                marshal_uint16_string(&mut b, unmarshal_uint16(v));
                b
            }),
            ValueType::UINT32 => map_encoded(ve, |v| {
                let mut b = Vec::new();
                marshal_uint32_string(&mut b, unmarshal_uint32(v));
                b
            }),
            ValueType::UINT64 => map_encoded(ve, |v| {
                let mut b = Vec::new();
                marshal_uint64_string(&mut b, unmarshal_uint64(v));
                b
            }),
            ValueType::INT64 => map_encoded(ve, |v| {
                let mut b = Vec::new();
                marshal_int64_string(&mut b, unmarshal_int64(v));
                b
            }),
            ValueType::FLOAT64 => map_encoded(ve, |v| {
                let mut b = Vec::new();
                marshal_float64_string(&mut b, unmarshal_float64(v));
                b
            }),
            ValueType::IPV4 => map_encoded(ve, |v| {
                let mut b = Vec::new();
                marshal_ipv4_string(&mut b, unmarshal_ipv4(v));
                b
            }),
            ValueType::TIMESTAMP_ISO8601 => map_encoded(ve, |v| {
                let mut b = Vec::new();
                marshal_timestamp_iso8601_string(&mut b, unmarshal_timestamp_iso8601(v));
                b
            }),
            _ => {
                esl_common::panicf!("BUG: unknown valueType={}", self.value_type.0);
                unreachable!()
            }
        }
    }

    fn compute_values_bucketed(&self, rows_len: usize, bf: &ByStatsField) -> Vec<Vec<u8>> {
        let ve = self
            .values_encoded
            .as_ref()
            .expect("valuesEncoded must be set");
        match self.value_type {
            ValueType::STRING => get_bucketed_strings(ve, bf),
            ValueType::DICT => {
                let dict_bucketed = get_bucketed_strings(&self.dict_values, bf);
                if are_const_values(&dict_bucketed) {
                    return get_const_values(&dict_bucketed[0], rows_len);
                }
                ve.iter()
                    .map(|v| dict_bucketed[v[0] as usize].clone())
                    .collect()
            }
            ValueType::UINT8 => self.bucketed_uint(bf, rows_len, |v| unmarshal_uint8(v) as u64),
            ValueType::UINT16 => self.bucketed_uint(bf, rows_len, |v| unmarshal_uint16(v) as u64),
            ValueType::UINT32 => self.bucketed_uint(bf, rows_len, |v| unmarshal_uint32(v) as u64),
            ValueType::UINT64 => self.bucketed_uint(bf, rows_len, unmarshal_uint64),
            ValueType::INT64 => self.bucketed_int64(bf, rows_len),
            ValueType::FLOAT64 => self.bucketed_float64(bf, rows_len),
            ValueType::IPV4 => self.bucketed_ipv4(bf, rows_len),
            ValueType::TIMESTAMP_ISO8601 => self.bucketed_iso8601(bf, rows_len),
            _ => {
                esl_common::panicf!("BUG: unknown valueType={}", self.value_type.0);
                unreachable!()
            }
        }
    }

    fn bucketed_uint(
        &self,
        bf: &ByStatsField,
        rows_len: usize,
        decode: impl Fn(&[u8]) -> u64,
    ) -> Vec<Vec<u8>> {
        let mut bucket_size = bf.bucket_size as u64;
        if bucket_size == 0 {
            bucket_size = 1;
        }
        let bucket_offset = bf.bucket_offset as i64 as u64;
        let min_value = self.min_value;
        let max_value = self.max_value;
        let n_min = truncate_uint64(min_value, bucket_size, bucket_offset);
        let n_max = truncate_uint64(max_value, bucket_size, bucket_offset);
        if n_min == n_max {
            let mut b = Vec::new();
            marshal_uint64_string(&mut b, n_min);
            return get_const_values(&b, rows_len);
        }
        let ve = self.values_encoded.as_ref().unwrap();
        let mut out = Vec::with_capacity(ve.len());
        let mut s: Vec<u8> = Vec::new();
        let mut n_prev = 0u64;
        for (i, v) in ve.iter().enumerate() {
            if i > 0 && ve[i - 1] == *v {
                out.push(s.clone());
                continue;
            }
            let n = truncate_uint64(decode(v), bucket_size, bucket_offset);
            if i == 0 || n_prev != n {
                s = Vec::new();
                marshal_uint64_string(&mut s, n);
                n_prev = n;
            }
            out.push(s.clone());
        }
        out
    }

    fn bucketed_int64(&self, bf: &ByStatsField, rows_len: usize) -> Vec<Vec<u8>> {
        let mut bucket_size = bf.bucket_size as i64;
        if bucket_size == 0 {
            bucket_size = 1;
        }
        let bucket_offset = bf.bucket_offset as i64;
        let n_min = truncate_int64(self.min_value as i64, bucket_size, bucket_offset);
        let n_max = truncate_int64(self.max_value as i64, bucket_size, bucket_offset);
        if n_min == n_max {
            let mut b = Vec::new();
            marshal_int64_string(&mut b, n_min);
            return get_const_values(&b, rows_len);
        }
        let ve = self.values_encoded.as_ref().unwrap();
        let mut out = Vec::with_capacity(ve.len());
        let mut s: Vec<u8> = Vec::new();
        let mut n_prev = 0i64;
        for (i, v) in ve.iter().enumerate() {
            if i > 0 && ve[i - 1] == *v {
                out.push(s.clone());
                continue;
            }
            let n = truncate_int64(unmarshal_int64(v), bucket_size, bucket_offset);
            if i == 0 || n_prev != n {
                s = Vec::new();
                marshal_int64_string(&mut s, n);
                n_prev = n;
            }
            out.push(s.clone());
        }
        out
    }

    fn bucketed_float64(&self, bf: &ByStatsField, rows_len: usize) -> Vec<Vec<u8>> {
        let mut bucket_size = bf.bucket_size;
        if bucket_size <= 0.0 {
            bucket_size = 1.0;
        }
        let (_, e) = decimal::from_float(bucket_size);
        let p10 = pow10_exact(-(e as i32));
        let bucket_size_p10 = (bucket_size * p10) as i64;
        let min_value = f64::from_bits(self.min_value);
        let max_value = f64::from_bits(self.max_value);
        let f_min = truncate_float64(min_value, p10, bucket_size_p10, bf.bucket_offset);
        let f_max = truncate_float64(max_value, p10, bucket_size_p10, bf.bucket_offset);
        if f_min == f_max {
            let mut b = Vec::new();
            marshal_float64_string(&mut b, f_min);
            return get_const_values(&b, rows_len);
        }
        let ve = self.values_encoded.as_ref().unwrap();
        let mut out = Vec::with_capacity(ve.len());
        let mut s: Vec<u8> = Vec::new();
        let mut f_prev = 0f64;
        for (i, v) in ve.iter().enumerate() {
            if i > 0 && ve[i - 1] == *v {
                out.push(s.clone());
                continue;
            }
            let f = truncate_float64(unmarshal_float64(v), p10, bucket_size_p10, bf.bucket_offset);
            if i == 0 || f_prev != f {
                s = Vec::new();
                marshal_float64_string(&mut s, f);
                f_prev = f;
            }
            out.push(s.clone());
        }
        out
    }

    fn bucketed_ipv4(&self, bf: &ByStatsField, rows_len: usize) -> Vec<Vec<u8>> {
        let mut bucket_size = bf.bucket_size as u32;
        if bucket_size == 0 {
            bucket_size = 1;
        }
        let bucket_offset = bf.bucket_offset as i32 as u32;
        let ip_min = truncate_uint32(self.min_value as i32 as u32, bucket_size, bucket_offset);
        let ip_max = truncate_uint32(self.max_value as i32 as u32, bucket_size, bucket_offset);
        if ip_min == ip_max {
            let mut b = Vec::new();
            marshal_ipv4_string(&mut b, ip_min);
            return get_const_values(&b, rows_len);
        }
        let ve = self.values_encoded.as_ref().unwrap();
        let mut out = Vec::with_capacity(ve.len());
        let mut s: Vec<u8> = Vec::new();
        let mut n_prev = 0u32;
        for (i, v) in ve.iter().enumerate() {
            if i > 0 && ve[i - 1] == *v {
                out.push(s.clone());
                continue;
            }
            let n = truncate_uint32(unmarshal_ipv4(v), bucket_size, bucket_offset);
            if i == 0 || n_prev != n {
                s = Vec::new();
                marshal_ipv4_string(&mut s, n);
                n_prev = n;
            }
            out.push(s.clone());
        }
        out
    }

    fn bucketed_iso8601(&self, bf: &ByStatsField, rows_len: usize) -> Vec<Vec<u8>> {
        let mut bucket_size = bf.bucket_size as i64;
        if bucket_size <= 0 {
            bucket_size = 1;
        }
        let bucket_offset = bf.bucket_offset as i64;
        let ts_min = truncate_timestamp(
            self.min_value as i64,
            bucket_size,
            bucket_offset,
            &bf.bucket_size_str,
        );
        let ts_max = truncate_timestamp(
            self.max_value as i64,
            bucket_size,
            bucket_offset,
            &bf.bucket_size_str,
        );
        if ts_min == ts_max {
            let mut b = Vec::new();
            marshal_timestamp_iso8601_string(&mut b, ts_min);
            return get_const_values(&b, rows_len);
        }
        let ve = self.values_encoded.as_ref().unwrap();
        let mut out = Vec::with_capacity(ve.len());
        let mut s: Vec<u8> = Vec::new();
        let mut prev = 0i64;
        for (i, v) in ve.iter().enumerate() {
            if i > 0 && ve[i - 1] == *v {
                out.push(s.clone());
                continue;
            }
            let ts = unmarshal_timestamp_iso8601(v);
            let tt = truncate_timestamp(ts, bucket_size, bucket_offset, &bf.bucket_size_str);
            if i == 0 || prev != tt {
                s = Vec::new();
                marshal_timestamp_iso8601_string(&mut s, tt);
                prev = tt;
            }
            out.push(s.clone());
        }
        out
    }

    fn float_value_at_row(&self, row_idx: usize) -> Option<f64> {
        let ve = self.values_encoded.as_ref().unwrap();
        match self.value_type {
            ValueType::STRING => try_parse_float64_bytes(&ve[row_idx]),
            ValueType::DICT => {
                let v = &self.dict_values[ve[row_idx][0] as usize];
                try_parse_float64_bytes(v)
            }
            ValueType::UINT8 => Some(unmarshal_uint8(&ve[row_idx]) as f64),
            ValueType::UINT16 => Some(unmarshal_uint16(&ve[row_idx]) as f64),
            ValueType::UINT32 => Some(unmarshal_uint32(&ve[row_idx]) as f64),
            ValueType::UINT64 => Some(unmarshal_uint64(&ve[row_idx]) as f64),
            ValueType::INT64 => Some(unmarshal_int64(&ve[row_idx]) as f64),
            ValueType::FLOAT64 => {
                let f = unmarshal_float64(&ve[row_idx]);
                if f.is_nan() { None } else { Some(f) }
            }
            ValueType::IPV4 | ValueType::TIMESTAMP_ISO8601 => None,
            _ => {
                esl_common::panicf!("BUG: unknown valueType={}", self.value_type.0);
                unreachable!()
            }
        }
    }

    fn sum_values(&self, rows_len: usize) -> (f64, usize) {
        let ve = self.values_encoded.as_ref().unwrap();
        match self.value_type {
            ValueType::STRING => {
                let mut sum = 0.0;
                let mut count = 0;
                let mut f = 0.0;
                let mut ok = false;
                for (i, v) in ve.iter().enumerate() {
                    if i == 0 || ve[i - 1] != *v {
                        match try_parse_number_bytes(v) {
                            Some(x) => {
                                f = x;
                                ok = true;
                            }
                            None => ok = false,
                        }
                    }
                    if ok {
                        sum += f;
                        count += 1;
                    }
                }
                (sum, count)
            }
            ValueType::DICT => {
                let dict_floats: Vec<f64> = self
                    .dict_values
                    .iter()
                    .map(|v| try_parse_number_bytes(v).unwrap_or(f64::NAN))
                    .collect();
                let mut sum = 0.0;
                let mut count = 0;
                for v in ve {
                    let f = dict_floats[v[0] as usize];
                    if !f.is_nan() {
                        sum += f;
                        count += 1;
                    }
                }
                (sum, count)
            }
            ValueType::UINT8 => (
                ve.iter().map(|v| unmarshal_uint8(v) as u64).sum::<u64>() as f64,
                rows_len,
            ),
            ValueType::UINT16 => (
                ve.iter().map(|v| unmarshal_uint16(v) as u64).sum::<u64>() as f64,
                rows_len,
            ),
            ValueType::UINT32 => (
                ve.iter().map(|v| unmarshal_uint32(v) as u64).sum::<u64>() as f64,
                rows_len,
            ),
            ValueType::UINT64 => (
                ve.iter().map(|v| unmarshal_uint64(v) as f64).sum::<f64>(),
                rows_len,
            ),
            ValueType::INT64 => (
                ve.iter().map(|v| unmarshal_int64(v) as f64).sum::<f64>(),
                rows_len,
            ),
            ValueType::FLOAT64 => {
                let mut sum = 0.0;
                for v in ve {
                    let f = unmarshal_float64(v);
                    if !f.is_nan() {
                        sum += f;
                    }
                }
                (sum, rows_len)
            }
            ValueType::IPV4 | ValueType::TIMESTAMP_ISO8601 => (0.0, 0),
            _ => {
                esl_common::panicf!("BUG: unknown valueType={}", self.value_type.0);
                unreachable!()
            }
        }
    }
}

fn map_encoded(ve: &[Vec<u8>], f: impl Fn(&[u8]) -> Vec<u8>) -> Vec<Vec<u8>> {
    // PORT NOTE: Go dedups adjacent equal encoded values to reuse the marshaled
    // string; here we recompute per value for simplicity (same observable
    // output).
    ve.iter().map(|v| f(v)).collect()
}

// ---------------------------------------------------------------------------
// resultColumn
// ---------------------------------------------------------------------------

/// A column with result values, not owning them.
#[derive(Default, Clone)]
pub struct ResultColumn {
    /// Column name (raw bytes; Go strings are arbitrary bytes).
    pub name: Vec<u8>,
    /// Result values (as bytes; UTF-8 for human-readable values).
    pub values: Vec<Vec<u8>>,
}

impl ResultColumn {
    /// Resets the result column.
    pub fn reset(&mut self) {
        self.name.clear();
        self.reset_values();
    }

    /// Resets the values but keeps the name.
    pub fn reset_values(&mut self) {
        self.values.clear();
    }

    /// Adds a value to the column.
    pub fn add_value(&mut self, v: &[u8]) {
        self.values.push(v.to_vec());
    }
}

/// Appends a result column with the given name to `dst`.
pub fn append_result_column_with_name<S: AsRef<[u8]>>(dst: &mut Vec<ResultColumn>, name: S) {
    dst.push(ResultColumn {
        name: name.as_ref().to_vec(),
        values: Vec::new(),
    });
}

// ---------------------------------------------------------------------------
// const/timestamp value helpers
// ---------------------------------------------------------------------------

fn get_const_values(s: &[u8], rows_len: usize) -> Vec<Vec<u8>> {
    if s.is_empty() {
        return get_empty_strings(rows_len);
    }
    vec![s.to_vec(); rows_len]
}

/// PORT NOTE: Go caches an atomic slice of empty strings; here we allocate a
/// fresh vector of empty values, which the borrow model makes simpler.
fn get_empty_strings(rows_len: usize) -> Vec<Vec<u8>> {
    vec![Vec::new(); rows_len]
}

fn get_timestamp_values(timestamps: &[i64]) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(timestamps.len());
    let mut s: Vec<u8> = Vec::new();
    for (i, &ts) in timestamps.iter().enumerate() {
        if i == 0 || timestamps[i - 1] != ts {
            s = Vec::new();
            marshal_timestamp_rfc3339_nano_string(&mut s, ts);
        }
        out.push(s.clone());
    }
    out
}

fn get_bucketed_timestamp_values(timestamps: &[i64], bf: &ByStatsField) -> Vec<Vec<u8>> {
    let mut bucket_size = bf.bucket_size as i64;
    if bucket_size <= 0 {
        bucket_size = 1;
    }
    let bucket_offset = bf.bucket_offset as i64;

    let mut out = Vec::with_capacity(timestamps.len());
    let mut s: Vec<u8> = Vec::new();
    let mut prev = 0i64;
    for (i, &ts) in timestamps.iter().enumerate() {
        if i > 0 && timestamps[i - 1] == ts {
            out.push(s.clone());
            continue;
        }
        let tt = truncate_timestamp(ts, bucket_size, bucket_offset, &bf.bucket_size_str);
        if i == 0 || prev != tt {
            s = Vec::new();
            marshal_timestamp_rfc3339_nano_string(&mut s, tt);
            prev = tt;
        }
        out.push(s.clone());
    }
    out
}

fn get_bucketed_strings(values_orig: &[Vec<u8>], bf: &ByStatsField) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(values_orig.len());
    let mut s = Vec::new();
    for (i, v) in values_orig.iter().enumerate() {
        if i == 0 || values_orig[i - 1] != *v {
            s = get_bucketed_value(v, bf);
        }
        out.push(s.clone());
    }
    out
}

/// Returns `s` bucketed according to `bf`.
fn get_bucketed_value(s: &[u8], bf: &ByStatsField) -> Vec<u8> {
    if s.is_empty() {
        return Vec::new();
    }
    let c = s[0];
    if !c.is_ascii_digit() && c != b'-' {
        return s.to_vec();
    }
    // A non-UTF-8 value cannot parse as a number/timestamp/ipv4/duration, so
    // it is returned unchanged - exactly like in Go.
    let Ok(s_str) = std::str::from_utf8(s) else {
        return s.to_vec();
    };
    let s = s_str;

    if let Some(n) = try_parse_int64(s) {
        let mut bucket_size = bf.bucket_size as i64;
        if bucket_size <= 0 {
            bucket_size = 1;
        }
        let bucket_offset = bf.bucket_offset as i64;
        let n_truncated = truncate_int64(n, bucket_size, bucket_offset);
        let mut b = Vec::new();
        marshal_int64_string(&mut b, n_truncated);
        return b;
    }

    if let Some(f) = try_parse_float64(s) {
        let mut bucket_size = bf.bucket_size;
        if bucket_size <= 0.0 {
            bucket_size = 1.0;
        }
        let (_, e) = decimal::from_float(bucket_size);
        let p10 = pow10_exact(-(e as i32));
        let bucket_size_p10 = (bucket_size * p10) as i64;
        let f = truncate_float64(f, p10, bucket_size_p10, bf.bucket_offset);
        let mut b = Vec::new();
        marshal_float64_string(&mut b, f);
        return b;
    }

    if let Some(timestamp) = try_parse_timestamp_rfc3339_nano(s) {
        let mut bucket_size = bf.bucket_size as i64;
        if bucket_size <= 0 {
            bucket_size = 1;
        }
        let bucket_offset = bf.bucket_offset as i64;
        let tt = truncate_timestamp(timestamp, bucket_size, bucket_offset, &bf.bucket_size_str);
        let mut b = Vec::new();
        marshal_timestamp_rfc3339_nano_string(&mut b, tt);
        return b;
    }

    if let Some(n) = try_parse_ipv4(s) {
        let mut bucket_size = bf.bucket_size as u32;
        if bucket_size == 0 {
            bucket_size = 1;
        }
        let bucket_offset = bf.bucket_offset as i32 as u32;
        let n = truncate_uint32(n, bucket_size, bucket_offset);
        let mut b = Vec::new();
        marshal_ipv4_string(&mut b, n);
        return b;
    }

    if let Some(nsecs) = try_parse_duration(s) {
        let mut bucket_size = bf.bucket_size as i64;
        if bucket_size <= 0 {
            bucket_size = 1;
        }
        let bucket_offset = bf.bucket_offset as i64;
        let nsecs = truncate_int64(nsecs, bucket_size, bucket_offset);
        let mut b = Vec::new();
        marshal_duration_string(&mut b, nsecs);
        return b;
    }

    s.as_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// truncate helpers
// ---------------------------------------------------------------------------

/// Truncates `ts` to the given bucket.
pub(crate) fn truncate_timestamp(
    ts: i64,
    bucket_size_int: i64,
    bucket_offset_int: i64,
    bucket_size_str: &str,
) -> i64 {
    let mut bucket_offset_int = bucket_offset_int;
    if bucket_size_str == "week" {
        // Adjust the week to start from Monday.
        bucket_offset_int += 3 * NSECS_PER_DAY;
    }
    if bucket_offset_int == 0 && bucket_size_str != "month" && bucket_size_str != "year" {
        let mut r = ts % bucket_size_int;
        if r < 0 {
            r += bucket_size_int;
        }
        return ts - r;
    }

    let mut ts = ts + bucket_offset_int;
    match bucket_size_str {
        "month" => ts = truncate_timestamp_to_month(ts),
        "year" => ts = truncate_timestamp_to_year(ts),
        _ => {
            let mut r = ts % bucket_size_int;
            if r < 0 {
                r += bucket_size_int;
            }
            ts -= r;
        }
    }
    ts -= bucket_offset_int;
    ts
}

pub(crate) fn truncate_uint64(n: u64, bucket_size_int: u64, bucket_offset_int: u64) -> u64 {
    if bucket_offset_int == 0 {
        return n - n % bucket_size_int;
    }
    if bucket_offset_int > n {
        return 0;
    }
    let n = n + bucket_offset_int;
    let n = n - n % bucket_size_int;
    n - bucket_offset_int
}

pub(crate) fn truncate_int64(n: i64, bucket_size_int: i64, bucket_offset_int: i64) -> i64 {
    if bucket_offset_int == 0 {
        let mut r = n % bucket_size_int;
        if r < 0 {
            r += bucket_size_int;
        }
        return n - r;
    }
    let mut n = n + bucket_offset_int;
    let mut r = n % bucket_size_int;
    if r < 0 {
        r += bucket_size_int;
    }
    n -= r;
    n -= bucket_offset_int;
    n
}

pub(crate) fn truncate_uint32(n: u32, bucket_size_int: u32, bucket_offset_int: u32) -> u32 {
    if bucket_offset_int == 0 {
        return n - n % bucket_size_int;
    }
    if bucket_offset_int > n {
        return 0;
    }
    let n = n + bucket_offset_int;
    let n = n - n % bucket_size_int;
    n - bucket_offset_int
}

pub(crate) fn truncate_float64(f: f64, p10: f64, bucket_size_p10: i64, bucket_offset: f64) -> f64 {
    if bucket_offset == 0.0 {
        let mut f_p10 = (f * p10).floor() as i64;
        let r = f_p10 % bucket_size_p10;
        f_p10 -= r;
        return f_p10 as f64 / p10;
    }

    let f = f + bucket_offset;
    let mut f_p10 = (f * p10).floor() as i64;
    let r = f_p10 % bucket_size_p10;
    f_p10 -= r;
    let f = f_p10 as f64 / p10;
    f - bucket_offset
}

fn truncate_timestamp_to_month(timestamp: i64) -> i64 {
    let (year, month, _) = civil_from_days(timestamp.div_euclid(NSECS_PER_DAY));
    days_from_civil(year, month) * NSECS_PER_DAY
}

fn truncate_timestamp_to_year(timestamp: i64) -> i64 {
    let (year, _, _) = civil_from_days(timestamp.div_euclid(NSECS_PER_DAY));
    days_from_civil(year, 1) * NSECS_PER_DAY
}

/// PORT NOTE: duplicated from values_encoder.rs (private there). Howard
/// Hinnant's `days_from_civil`; matches Go's time package.
fn days_from_civil(y: i64, m: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// PORT NOTE: duplicated from values_encoder.rs (private there).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Computes `10^n` exactly for the exponent range used here.
fn pow10_exact(n: i32) -> f64 {
    const POW10_TAB: [f64; 32] = [
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
        1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31,
    ];
    if (0..=31).contains(&n) {
        return POW10_TAB[n as usize];
    }
    if (-31..0).contains(&n) {
        return 1.0 / POW10_TAB[(-n) as usize];
    }
    10f64.powi(n)
}

// ---------------------------------------------------------------------------
// parse/number helpers
// ---------------------------------------------------------------------------

fn try_parse_timestamps(src: &[&str]) -> Option<Vec<i64>> {
    let mut out = Vec::with_capacity(src.len());
    for v in src {
        out.push(try_parse_timestamp_rfc3339_nano(v)?);
    }
    Some(out)
}

fn are_const_values(values: &[Vec<u8>]) -> bool {
    if values.is_empty() {
        return false;
    }
    let v = &values[0];
    values[1..].iter().all(|x| x == v)
}

/// Returns true if `c` is one of the special columns (`_msg`, `_time`,
/// `_stream`, `_stream_id`). The canonical form of `_msg` is the empty string.
fn is_special_column(c: &[u8]) -> bool {
    if c.is_empty() {
        // This is a _msg column.
        return true;
    }
    if !c.starts_with(b"_") {
        return false;
    }
    c == b"_time" || c == b"_stream" || c == b"_stream_id"
}

/// PORT NOTE: reimplemented locally (Go's `areSameFieldsInRows` lives in
/// block.go and is not exported by the Rust `block` module).
fn are_same_fields_in_rows(rows: &[Vec<Field>]) -> bool {
    if rows.len() < 2 {
        return true;
    }
    let fields = &rows[0];
    let mut seen: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
    for f in fields {
        if !seen.insert(f.name.as_slice()) {
            return false;
        }
    }
    for row in &rows[1..] {
        if row.len() != fields.len() {
            return false;
        }
        for (a, b) in row.iter().zip(fields) {
            if a.name != b.name {
                return false;
            }
        }
    }
    true
}

fn has_wildcard_filters(column_filters: &[Vec<u8>]) -> bool {
    column_filters.iter().any(is_wildcard_filter)
}

// Go `tryParseNumber` (block_result.go): shared port lives in `pipe_math.rs`.
use crate::pipe_math::try_parse_number_bytes;

/// PORT NOTE: minimal port of `tryParseBucketSize` (pipe_stats.go); only the
/// named units used by `truncate_timestamp` tests plus the numeric/duration/
/// bytes/ipv4-mask fallbacks are provided.
pub(crate) fn try_parse_bucket_size(s: &str) -> Option<f64> {
    match s {
        "nanosecond" => return Some(1.0),
        "microsecond" => return Some(NSECS_PER_MICROSECOND as f64),
        "millisecond" => return Some(NSECS_PER_MILLISECOND as f64),
        "second" => return Some(NSECS_PER_SECOND as f64),
        "minute" => return Some(NSECS_PER_MINUTE as f64),
        "hour" => return Some(NSECS_PER_HOUR as f64),
        "day" => return Some(NSECS_PER_DAY as f64),
        "week" => return Some(NSECS_PER_WEEK as f64),
        _ => {}
    }
    if let Some(f) = try_parse_float64(s) {
        return if f > 0.0 { Some(f) } else { None };
    }
    if let Some(nsecs) = try_parse_duration(s) {
        return if nsecs > 0 { Some(nsecs as f64) } else { None };
    }
    if let Some(n) = try_parse_bytes(s) {
        return if n > 0 { Some(n as f64) } else { None };
    }
    if let Some(n) = try_parse_ipv4_mask(s) {
        return if n > 0 { Some(n as f64) } else { None };
    }
    None
}

// ---------------------------------------------------------------------------
// pool
// ---------------------------------------------------------------------------

use std::sync::Mutex;

static BR_POOL: Mutex<Vec<BlockResult>> = Mutex::new(Vec::new());

/// Obtains a block result from the pool.
pub fn get_block_result() -> BlockResult {
    BR_POOL.lock().unwrap().pop().unwrap_or_default()
}

/// Returns a block result to the pool.
pub fn put_block_result(mut br: BlockResult) {
    br.reset();
    BR_POOL.lock().unwrap().push(br);
}

// PORT NOTE: references surface that is faithfully ported but only consumed by
// not-yet-ported Layer-5/6 pipes (value bucketing / stats). Referencing the
// entry points keeps the whole reachable chain (ByStatsField, the bucketing
// helpers) from being flagged as dead code.
#[allow(dead_code)]
fn _keep_surface_alive() {
    let _ = BlockResult::is_full;
    let _ = BlockResult::column_get_values_bucketed;
    let _ = ResultColumn::reset;
    let _ = ResultColumn::reset_values;
    let _ = ResultColumn::add_value;
    let _ = append_result_column_with_name::<&str>;
    let _ = try_parse_bucket_size;
    let _ = vencoding::marshal_uint64 as fn(&mut Vec<u8>, u64);
    let _ = prefixfilter::match_all::<String>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_parser::{get_json_parser, put_json_parser};
    use crate::rows::marshal_fields_to_json;
    use esl_common::decimal;

    #[test]
    fn test_truncate_timestamp() {
        fn f(timestamp_str: &str, bucket_size_str: &str, offset_str: &str, result_expected: &str) {
            let ts = try_parse_timestamp_rfc3339_nano(timestamp_str)
                .unwrap_or_else(|| panic!("cannot parse timestamp {timestamp_str:?}"));

            let bucket_size = if bucket_size_str != "month" && bucket_size_str != "year" {
                try_parse_bucket_size(bucket_size_str)
                    .unwrap_or_else(|| panic!("cannot parse bucket {bucket_size_str:?}"))
                    as i64
            } else {
                0
            };

            let offset = if offset_str.is_empty() {
                0
            } else {
                try_parse_duration(offset_str)
                    .unwrap_or_else(|| panic!("cannot parse offset {offset_str:?}"))
            };

            let ts_bucketed = truncate_timestamp(ts, bucket_size, offset, bucket_size_str);
            let mut result = Vec::new();
            marshal_timestamp_rfc3339_nano_string(&mut result, ts_bucketed);
            let result = String::from_utf8(result).unwrap();
            assert_eq!(
                result, result_expected,
                "unexpected result; got {result:?}; want {result_expected:?}"
            );
        }

        f(
            "2025-01-20T10:20:30.12345Z",
            "10ms",
            "",
            "2025-01-20T10:20:30.12Z",
        );
        f(
            "2025-01-20T10:20:30.12345Z",
            "10m",
            "",
            "2025-01-20T10:20:00Z",
        );
        f(
            "2025-01-20T10:20:30.12345Z",
            "hour",
            "",
            "2025-01-20T10:00:00Z",
        );
        f(
            "2025-01-20T10:20:30.12345Z",
            "day",
            "",
            "2025-01-20T00:00:00Z",
        );
        f(
            "2025-01-19T23:59:59.999999999Z",
            "week",
            "",
            "2025-01-13T00:00:00Z",
        );
        f(
            "2025-01-20T10:20:30.12345Z",
            "week",
            "",
            "2025-01-20T00:00:00Z",
        );
        f(
            "2025-01-21T10:20:30.12345Z",
            "week",
            "",
            "2025-01-20T00:00:00Z",
        );
        f(
            "2025-03-20T10:20:30.12345Z",
            "month",
            "",
            "2025-03-01T00:00:00Z",
        );
        f(
            "2025-01-20T10:20:30.12345Z",
            "year",
            "",
            "2025-01-01T00:00:00Z",
        );

        // with offset
        f(
            "2025-01-20T10:20:30.1234Z",
            "1d",
            "",
            "2025-01-20T00:00:00Z",
        );
        f(
            "2025-01-20T10:20:30.1234Z",
            "1d",
            "2h",
            "2025-01-19T22:00:00Z",
        );
        f(
            "2025-01-20T10:20:30.1234Z",
            "1d",
            "-2h",
            "2025-01-20T02:00:00Z",
        );
        f(
            "2025-01-20T22:20:30.1234-05:00",
            "1d",
            "",
            "2025-01-21T00:00:00Z",
        );
        f(
            "2025-01-20T22:20:30.1234-05:00",
            "1d",
            "5h",
            "2025-01-20T19:00:00Z",
        );
        f(
            "2025-01-20T22:20:30.1234-05:00",
            "1d",
            "-5h",
            "2025-01-20T05:00:00Z",
        );
        f(
            "2025-01-19T23:59:59.999999999Z",
            "week",
            "3h",
            "2025-01-19T21:00:00Z",
        );
        f(
            "2025-01-19T23:59:59.999999999Z",
            "week",
            "-3h",
            "2025-01-13T03:00:00Z",
        );
        f(
            "2025-01-31T23:20:30-04:00",
            "month",
            "",
            "2025-02-01T00:00:00Z",
        );
        f(
            "2025-01-31T23:20:30+04:00",
            "month",
            "",
            "2025-01-01T00:00:00Z",
        );
        f(
            "2025-01-31T23:20:30Z",
            "month",
            "4h",
            "2025-01-31T20:00:00Z",
        );
        f(
            "2025-01-31T23:20:30Z",
            "month",
            "-4h",
            "2025-01-01T04:00:00Z",
        );
        f("2024-12-31T23:20:30Z", "year", "", "2024-01-01T00:00:00Z");
        f("2024-12-31T23:20:30Z", "year", "4h", "2024-12-31T20:00:00Z");
        f(
            "2024-12-31T23:20:30Z",
            "year",
            "-4h",
            "2024-01-01T04:00:00Z",
        );

        // negative timestamps
        f("1970-01-01T00:00:00Z", "week", "", "1969-12-29T00:00:00Z");
        f("1970-01-01T00:00:00Z", "week", "3d", "1969-12-26T00:00:00Z");
        f("1970-01-01T00:00:00Z", "week", "4d", "1970-01-01T00:00:00Z");
        f(
            "1970-01-01T00:00:00Z",
            "week",
            "-3d",
            "1970-01-01T00:00:00Z",
        );
        f(
            "1970-01-01T00:00:00Z",
            "week",
            "-4d",
            "1969-12-26T00:00:00Z",
        );
    }

    #[test]
    fn test_truncate_float64() {
        fn f(n: f64, bucket_size: f64, offset: f64, result_expected: f64) {
            let (_, e) = decimal::from_float(bucket_size);
            let p10 = pow10_exact(-(e as i32));
            let bucket_size_p10 = (bucket_size * p10) as i64;
            let result = truncate_float64(n, p10, bucket_size_p10, offset);
            assert_eq!(
                result, result_expected,
                "unexpected result; got {result}; want {result_expected}"
            );
        }

        f(0.0, 100.0, 0.0, 0.0);
        f(99.0, 100.0, 0.0, 0.0);
        f(-1.0, 100.0, 0.0, -100.0);
        f(-100.0, 100.0, 0.0, -100.0);
        f(-101.0, 100.0, 0.0, -200.0);

        f(1.0, 100.0, -10.0, -90.0);
        f(1.0, 100.0, 10.0, -10.0);
        f(0.0, 100.0, -30.0, -70.0);
        f(0.0, 100.0, 30.0, -30.0);
        f(120.0, 100.0, -30.0, 30.0);
        f(120.0, 100.0, 30.0, 70.0);
        f(130.0, 100.0, 30.3, 69.7);
        f(130.3, 100.0, -30.3, 130.3);
        f(130.3, 100.0, 30.3, 69.7);
        f(130.4, 100.0, -30.3, 130.3);
        f(130.4, 100.0, 30.3, 69.7);

        f(1.25, 0.1, 0.0, 1.2);
        f(1.3, 0.1, 0.0, 1.3);
        f(1.312, 0.1, 0.0, 1.3);
        f(-1.3, 0.1, 0.0, -1.3);
        f(-1.25, 0.1, 0.0, -1.3);
        f(-0.25, 0.1, 0.0, -0.3);
        f(-0.01, 0.1, 0.0, -0.1);
        f(-0.01, 0.1, 0.05, -0.05);

        f(123.0, 20.0, 0.0, 120.0);
        f(120.0, 20.0, 0.0, 120.0);
        f(119.0, 20.0, 0.0, 100.0);
        f(0.123, 0.02, 0.0, 0.12);
        f(0.1, 0.02, 0.0, 0.1);
    }

    #[test]
    fn test_truncate_int64() {
        fn f(n: i64, bucket_size: i64, offset: i64, result_expected: i64) {
            let result = truncate_int64(n, bucket_size, offset);
            assert_eq!(
                result, result_expected,
                "unexpected result; got {result}; want {result_expected}"
            );
        }

        f(0, 100, 0, 0);
        f(99, 100, 0, 0);
        f(-1, 100, 0, -100);
        f(-100, 100, 0, -100);
        f(-101, 100, 0, -200);

        f(1, 100, -10, -90);
        f(1, 100, 10, -10);
        f(0, 100, -30, -70);
        f(0, 100, 30, -30);
        f(120, 100, -30, 30);
        f(120, 100, 30, 70);
        f(130, 100, -30, 130);
        f(130, 100, 30, 70);
    }

    #[test]
    fn test_truncate_uint64() {
        fn f(n: u64, bucket_size: u64, offset: u64, result_expected: u64) {
            let result = truncate_uint64(n, bucket_size, offset);
            assert_eq!(
                result, result_expected,
                "unexpected result; got {result}; want {result_expected}"
            );
        }

        f(0, 100, 0, 0);
        f(99, 100, 0, 0);

        f(1, 100, 10, 0);
        f(0, 100, 30, 0);
        f(120, 100, 70, 30);
        f(120, 100, 30, 70);
        f(130, 100, 70, 130);
        f(130, 100, 30, 70);
    }

    #[test]
    fn test_truncate_uint32() {
        fn f(n: u32, bucket_size: u32, offset: u32, result_expected: u32) {
            let result = truncate_uint32(n, bucket_size, offset);
            assert_eq!(
                result, result_expected,
                "unexpected result; got {result}; want {result_expected}"
            );
        }

        f(0, 100, 0, 0);
        f(99, 100, 0, 0);

        f(1, 100, 10, 0);
        f(0, 100, 30, 0);
        f(120, 100, 30, 70);
        f(120, 100, 70, 30);
        f(130, 100, 30, 70);
        f(130, 100, 70, 130);
    }

    #[test]
    fn test_block_result_must_init_from_rows() {
        fn f(rows_str: &[&str]) {
            let mut rows: Vec<Vec<Field>> = Vec::new();
            let mut p = get_json_parser();
            for row_str in rows_str {
                p.parse_log_message(row_str.as_bytes(), &[], "")
                    .expect("cannot parse input row");
                let fields: Vec<Field> = p
                    .fields()
                    .iter()
                    .map(|f| Field {
                        name: f.name.clone(),
                        value: f.value.clone(),
                    })
                    .collect();
                rows.push(fields);
            }
            put_json_parser(p);

            let mut br = get_block_result();
            br.must_init_from_rows(&rows);

            let cols = br.get_columns();
            let mut result_rows_str: Vec<String> = Vec::new();
            for row_idx in 0..rows.len() {
                let mut fields: Vec<Field> = Vec::new();
                for &r in &cols {
                    let name = br.column_name(r).to_vec();
                    let value = br.column_get_value_at_row(r, row_idx).to_vec();
                    fields.push(Field { name, value });
                }
                let mut buf = Vec::new();
                marshal_fields_to_json(&mut buf, &fields);
                result_rows_str.push(String::from_utf8(buf).unwrap());
            }
            put_block_result(br);

            let want: Vec<String> = rows_str.iter().map(|s| s.to_string()).collect();
            assert_eq!(
                result_rows_str, want,
                "unexpected rows\ngot\n{result_rows_str:?}\nwant\n{want:?}"
            );
        }

        f(&[]);
        f(&["{}"]);

        // a single row
        f(&[r#"{"foo":"bar","a":"b"}"#]);

        // multiple rows with the same set of fields
        f(&[r#"{"a":"b","c":"d"}"#, r#"{"a":"x","c":"y"}"#]);
        f(&[
            r#"{"a":"b","c":"d"}"#,
            r#"{"a":"x","c":"y"}"#,
            r#"{"a":"qwewqr","c":"ieorer"}"#,
        ]);

        // multiple rows with different sets of fields
        f(&[
            r#"{"a":"b","c":"d"}"#,
            "{}",
            r#"{"a":"x","c":"y"}"#,
            r#"{"q":"z"}"#,
        ]);
    }
}

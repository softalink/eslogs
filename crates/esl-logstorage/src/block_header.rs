//! Port of `lib/logstorage/block_header.go`.

use std::sync::{Arc, Mutex};

use esl_common::encoding::MarshalType;
use esl_common::{bytesutil, encoding, panicf, slicesutil};

use crate::column_names::ColumnNameIDGenerator;
use crate::consts::{
    MAX_BLOOM_FILTER_BLOCK_SIZE, MAX_COLUMNS_HEADER_SIZE, MAX_COLUMNS_PER_BLOCK,
    MAX_ROWS_PER_BLOCK, MAX_VALUES_BLOCK_SIZE,
};
use crate::rows::Field;
use crate::stream_id::StreamID;
use crate::values_encoder::{ValueType, ValuesDict, sub_int64_no_overflow};

/// BlockHeader contains information about a single block.
///
/// blockHeader is stored in the indexFilename file.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BlockHeader {
    /// streamID is a stream id for entries in the block.
    pub stream_id: StreamID,

    /// uncompressedSizeBytes is the original (uncompressed) size of log entries stored in the block.
    pub uncompressed_size_bytes: u64,

    /// rowsCount is the number of log entries stored in the block.
    pub rows_count: u64,

    /// timestampsHeader contains information about timestamps for log entries in the block.
    pub timestamps_header: TimestampsHeader,

    /// columnsHeaderIndexOffset is the offset of columnsHeaderIndex at columnsHeaderIndexFilename.
    pub columns_header_index_offset: u64,

    /// columnsHeaderIndexSize is the size of columnsHeaderIndex at columnsHeaderIndexFilename.
    pub columns_header_index_size: u64,

    /// columnsHeaderOffset is the offset of columnsHeader at columnsHeaderFilename.
    pub columns_header_offset: u64,

    /// columnsHeaderSize is the size of columnsHeader at columnsHeaderFilename.
    pub columns_header_size: u64,
}

impl BlockHeader {
    /// Resets bh, so it can be reused.
    pub fn reset(&mut self) {
        self.stream_id.reset();
        self.uncompressed_size_bytes = 0;
        self.rows_count = 0;
        self.timestamps_header.reset();
        self.columns_header_index_offset = 0;
        self.columns_header_index_size = 0;
        self.columns_header_offset = 0;
        self.columns_header_size = 0;
    }

    pub fn copy_from(&mut self, src: &BlockHeader) {
        self.reset();

        self.stream_id = src.stream_id;
        self.uncompressed_size_bytes = src.uncompressed_size_bytes;
        self.rows_count = src.rows_count;
        self.timestamps_header.copy_from(&src.timestamps_header);
        self.columns_header_index_offset = src.columns_header_index_offset;
        self.columns_header_index_size = src.columns_header_index_size;
        self.columns_header_offset = src.columns_header_offset;
        self.columns_header_size = src.columns_header_size;
    }

    /// Appends the marshaled bh to dst.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        self.stream_id.marshal(dst);
        encoding::marshal_var_uint64(dst, self.uncompressed_size_bytes);
        encoding::marshal_var_uint64(dst, self.rows_count);
        self.timestamps_header.marshal(dst);
        encoding::marshal_var_uint64(dst, self.columns_header_index_offset);
        encoding::marshal_var_uint64(dst, self.columns_header_index_size);
        encoding::marshal_var_uint64(dst, self.columns_header_offset);
        encoding::marshal_var_uint64(dst, self.columns_header_size);
    }

    /// Unmarshals bh from src and returns the remaining tail.
    ///
    /// PORT NOTE: Go returns `(srcOrig, error)` on failure; the Rust port
    /// returns `Result<tail, String>` and leaves `src` untouched on error.
    pub fn unmarshal<'a>(
        &mut self,
        src: &'a [u8],
        part_format_version: u64,
    ) -> Result<&'a [u8], String> {
        self.reset();

        // unmarshal bh.streamID
        let mut src = self
            .stream_id
            .unmarshal(src)
            .map_err(|err| format!("cannot unmarshal streamID: {err}"))?;

        // unmarshal bh.uncompressedSizeBytes
        let (n, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err("cannot unmarshal uncompressedSizeBytes".to_string());
        }
        src = &src[n_size as usize..];
        self.uncompressed_size_bytes = n;

        // unmarshal bh.rowsCount
        let (n, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err("cannot unmarshal rowsCount".to_string());
        }
        src = &src[n_size as usize..];
        if n > MAX_ROWS_PER_BLOCK as u64 {
            return Err(format!(
                "too big value for rowsCount: {n}; mustn't exceed {MAX_ROWS_PER_BLOCK}"
            ));
        }
        self.rows_count = n;

        // unmarshal bh.timestampsHeader
        src = self
            .timestamps_header
            .unmarshal(src)
            .map_err(|err| format!("cannot unmarshal timestampsHeader: {err}"))?;

        if part_format_version >= 1 {
            // unmarshal columnsHeaderIndexOffset
            let (n, n_size) = encoding::unmarshal_var_uint64(src);
            if n_size <= 0 {
                return Err("cannot unmarshal columnsHeaderIndexOffset".to_string());
            }
            src = &src[n_size as usize..];
            self.columns_header_index_offset = n;

            // unmarshal columnsHeaderIndexSize
            let (n, n_size) = encoding::unmarshal_var_uint64(src);
            if n_size <= 0 {
                return Err("cannot unmarshal columnsHeaderIndexSize".to_string());
            }
            src = &src[n_size as usize..];
            self.columns_header_index_size = n;
        }

        // unmarshal columnsHeaderOffset
        let (n, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err("cannot unmarshal columnsHeaderOffset".to_string());
        }
        src = &src[n_size as usize..];
        self.columns_header_offset = n;

        // unmarshal columnsHeaderSize
        let (n, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err("cannot unmarshal columnsHeaderSize".to_string());
        }
        src = &src[n_size as usize..];
        if n > MAX_COLUMNS_HEADER_SIZE as u64 {
            return Err(format!(
                "too big value for columnsHeaderSize: {n}; mustn't exceed {MAX_COLUMNS_HEADER_SIZE}"
            ));
        }
        self.columns_header_size = n;

        Ok(src)
    }
}

/// Returns a BlockHeader from the pool.
pub fn get_block_header() -> BlockHeader {
    BLOCK_HEADER_POOL.lock().unwrap().pop().unwrap_or_default()
}

/// Returns bh to the pool.
pub fn put_block_header(mut bh: BlockHeader) {
    bh.reset();
    BLOCK_HEADER_POOL.lock().unwrap().push(bh);
}

static BLOCK_HEADER_POOL: Mutex<Vec<BlockHeader>> = Mutex::new(Vec::new());

/// Appends unmarshaled from src blockHeader entries to dst.
///
/// PORT NOTE: Go's signature is `(dst []blockHeader, src []byte, ...)
/// ([]blockHeader, error)` with reuse of spare slice capacity; the port
/// appends to `&mut Vec` (which reuses its own capacity) and returns
/// `Result<(), String>`. Like in Go, entries unmarshaled before an error are
/// left in dst.
pub fn unmarshal_block_headers(
    dst: &mut Vec<BlockHeader>,
    src: &[u8],
    part_format_version: u64,
) -> Result<(), String> {
    let dst_len = dst.len();
    let mut src = src;
    while !src.is_empty() {
        dst.push(BlockHeader::default());
        let bh = dst.last_mut().unwrap();
        let tail = bh
            .unmarshal(src, part_format_version)
            .map_err(|err| format!("cannot unmarshal blockHeader entries: {err}"))?;
        src = tail;
    }
    validate_block_headers(&dst[dst_len..])
}

fn validate_block_headers(bhs: &[BlockHeader]) -> Result<(), String> {
    for i in 1..bhs.len() {
        let bh_curr = &bhs[i];
        let bh_prev = &bhs[i - 1];
        if bh_curr.stream_id.less(&bh_prev.stream_id) {
            return Err(format!(
                "unexpected blockHeader with smaller streamID={} after bigger streamID={} at position {}",
                bh_curr.stream_id, bh_prev.stream_id, i
            ));
        }
        if !bh_curr.stream_id.equal(&bh_prev.stream_id) {
            continue;
        }
        let th_curr = &bh_curr.timestamps_header;
        let th_prev = &bh_prev.timestamps_header;
        if th_curr.min_timestamp < th_prev.min_timestamp {
            return Err(format!(
                "unexpected blockHeader with smaller timestamp={} after bigger timestamp={} at position {}",
                th_curr.min_timestamp, th_prev.min_timestamp, i
            ));
        }
    }
    Ok(())
}

/// Resets the given blockHeader entries and truncates bhs to zero length.
///
/// PORT NOTE: Go returns `bhs[:0]`; the port truncates in place.
pub fn reset_block_headers(bhs: &mut Vec<BlockHeader>) {
    for bh in bhs.iter_mut() {
        bh.reset();
    }
    bhs.clear();
}

/// ColumnHeaderRef references column header in the marshaled columnsHeader.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ColumnHeaderRef {
    /// columnNameID is the ID of the column name. The column name can be obtained from part.columnNames.
    pub column_name_id: u64,

    /// offset is the offset of the the corresponding columnHeader inside marshaled columnsHeader.
    pub offset: u64,
}

/// ColumnsHeaderIndex contains offsets for marshaled column headers.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ColumnsHeaderIndex {
    /// columnHeadersRefs contains references to columnHeaders.
    pub column_headers_refs: Vec<ColumnHeaderRef>,

    /// constColumnsRefs contains references to constColumns.
    pub const_columns_refs: Vec<ColumnHeaderRef>,
}

/// Returns a ColumnsHeaderIndex from the pool.
pub fn get_columns_header_index() -> ColumnsHeaderIndex {
    COLUMNS_HEADER_INDEX_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_default()
}

/// Returns cshIndex to the pool.
pub fn put_columns_header_index(mut csh_index: ColumnsHeaderIndex) {
    csh_index.reset();
    COLUMNS_HEADER_INDEX_POOL.lock().unwrap().push(csh_index);
}

static COLUMNS_HEADER_INDEX_POOL: Mutex<Vec<ColumnsHeaderIndex>> = Mutex::new(Vec::new());

impl ColumnsHeaderIndex {
    pub fn reset(&mut self) {
        self.column_headers_refs.clear();
        self.const_columns_refs.clear();
    }

    pub fn resize_const_columns_refs(&mut self, n: usize) -> &mut [ColumnHeaderRef] {
        slicesutil::set_length(&mut self.const_columns_refs, n);
        &mut self.const_columns_refs
    }

    pub fn resize_column_headers_refs(&mut self, n: usize) -> &mut [ColumnHeaderRef] {
        slicesutil::set_length(&mut self.column_headers_refs, n);
        &mut self.column_headers_refs
    }

    /// Appends the marshaled cshIndex to dst.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        marshal_column_headers_refs(dst, &self.column_headers_refs);
        marshal_column_headers_refs(dst, &self.const_columns_refs);
    }

    /// Unmarshals cshIndex from src.
    ///
    /// PORT NOTE: the Go name is kept for parity; the refs contain no views
    /// into src, so there is no in-place lifetime restriction in the port.
    pub fn unmarshal_inplace(&mut self, src: &[u8]) -> Result<(), String> {
        self.reset();

        let tail = unmarshal_column_headers_refs_inplace(&mut self.column_headers_refs, src)
            .map_err(|err| format!("cannot unmarshal columnHeadersRefs: {err}"))?;
        let src = tail;

        let tail = unmarshal_column_headers_refs_inplace(&mut self.const_columns_refs, src)
            .map_err(|err| format!("cannot unmarshal constColumnsRefs: {err}"))?;
        if !tail.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left after unmarshaling columnsHeaderIndex; len(tail)={}",
                tail.len()
            ));
        }

        Ok(())
    }
}

fn marshal_column_headers_refs(dst: &mut Vec<u8>, refs: &[ColumnHeaderRef]) {
    encoding::marshal_var_uint64(dst, refs.len() as u64);
    for r in refs {
        encoding::marshal_var_uint64(dst, r.column_name_id);
        encoding::marshal_var_uint64(dst, r.offset);
    }
}

/// Appends unmarshaled from src column headers to dst and returns the tail.
fn unmarshal_column_headers_refs_inplace<'a>(
    dst: &mut Vec<ColumnHeaderRef>,
    src: &'a [u8],
) -> Result<&'a [u8], String> {
    let (n, n_size) = encoding::unmarshal_var_uint64(src);
    if n_size <= 0 {
        return Err("cannot unmarshal the number of columnHeaderRef items".to_string());
    }
    let mut src = &src[n_size as usize..];

    for i in 0..n {
        let (column_name_id, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err(format!(
                "cannot unmarshal column name ID number {i} out of {n}"
            ));
        }
        src = &src[n_size as usize..];

        let (offset, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err(format!("cannot unmarshal offset number {i} out of {n}"));
        }
        src = &src[n_size as usize..];

        dst.push(ColumnHeaderRef {
            column_name_id,
            offset,
        });
    }

    Ok(src)
}

/// Returns a ColumnsHeader from the pool.
pub fn get_columns_header() -> ColumnsHeader {
    COLUMNS_HEADER_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_default()
}

/// Returns csh to the pool.
pub fn put_columns_header(mut csh: ColumnsHeader) {
    csh.reset();
    COLUMNS_HEADER_POOL.lock().unwrap().push(csh);
}

static COLUMNS_HEADER_POOL: Mutex<Vec<ColumnsHeader>> = Mutex::new(Vec::new());

/// ColumnsHeader contains information about columns in a single block.
///
/// columnsHeader is stored in the columnsHeaderFilename file.
#[derive(Debug, Default, PartialEq)]
pub struct ColumnsHeader {
    /// columnHeaders contains the information about every column seen in the block.
    pub column_headers: Vec<ColumnHeader>,

    /// constColumns contain fields with constant values across all the block entries.
    pub const_columns: Vec<Field>,
}

impl ColumnsHeader {
    pub fn reset(&mut self) {
        for ch in self.column_headers.iter_mut() {
            ch.reset();
        }
        self.column_headers.clear();

        for cc in self.const_columns.iter_mut() {
            cc.reset();
        }
        self.const_columns.clear();
    }

    pub fn resize_const_columns(&mut self, n: usize) -> &mut [Field] {
        slicesutil::set_length(&mut self.const_columns, n);
        &mut self.const_columns
    }

    pub fn resize_column_headers(&mut self, n: usize) -> &mut [ColumnHeader] {
        slicesutil::set_length(&mut self.column_headers, n);
        &mut self.column_headers
    }

    /// PORT NOTE: columnNames is `[]string` in Go; the port takes the interned
    /// `Arc<str>` names produced by the column_names module.
    pub fn set_column_names(
        &mut self,
        csh_index: &ColumnsHeaderIndex,
        column_names: &[Arc<str>],
    ) -> Result<(), String> {
        if csh_index.column_headers_refs.len() != self.column_headers.len() {
            return Err(format!(
                "unexpected number of column headers; got {}; want {}",
                csh_index.column_headers_refs.len(),
                self.column_headers.len()
            ));
        }
        for i in 0..self.column_headers.len() {
            let column_name_id = csh_index.column_headers_refs[i].column_name_id;
            if column_name_id >= column_names.len() as u64 {
                return Err(format!(
                    "unexpected columnNameID={} in columnHeadersRef; len(columnNames)={}; columnNames={}",
                    column_name_id,
                    column_names.len(),
                    format_string_slice(column_names)
                ));
            }
            self.column_headers[i].name = column_names[column_name_id as usize].to_string();
        }

        if csh_index.const_columns_refs.len() != self.const_columns.len() {
            return Err(format!(
                "unexpected number of const columns; got {}; want {}",
                csh_index.const_columns_refs.len(),
                self.const_columns.len()
            ));
        }
        for i in 0..self.const_columns.len() {
            let column_name_id = csh_index.const_columns_refs[i].column_name_id;
            if column_name_id >= column_names.len() as u64 {
                return Err(format!(
                    "unexpected columnNameID={} in constColumnsRefs; len(columnNames)={}; columnNames={}",
                    column_name_id,
                    column_names.len(),
                    format_string_slice(column_names)
                ));
            }
            self.const_columns[i].name = column_names[column_name_id as usize].to_string();
        }

        Ok(())
    }

    // PORT NOTE: Go's columnsHeader.mustWriteTo(bh, sw *streamWriters) is
    // ported as block_stream_writer::must_write_columns_header, since
    // StreamWriters and LONG_TERM_BUF_POOL are defined in that module.

    /// Appends the marshaled csh to dst, filling cshIndex with the refs.
    ///
    /// PORT NOTE: pub(crate) since ColumnNameIDGenerator is crate-private.
    pub(crate) fn marshal(
        &self,
        dst: &mut Vec<u8>,
        csh_index: &mut ColumnsHeaderIndex,
        g: &mut ColumnNameIDGenerator,
    ) {
        let dst_len = dst.len();

        let chs = &self.column_headers;
        csh_index.resize_column_headers_refs(chs.len());
        encoding::marshal_var_uint64(dst, chs.len() as u64);
        for (i, ch) in chs.iter().enumerate() {
            let column_name_id = g.get_column_name_id(&ch.name);
            let offset = dst.len() - dst_len;
            ch.marshal(dst);
            csh_index.column_headers_refs[i] = ColumnHeaderRef {
                column_name_id,
                offset: offset as u64,
            };
        }

        let ccs = &self.const_columns;
        csh_index.resize_const_columns_refs(ccs.len());
        encoding::marshal_var_uint64(dst, ccs.len() as u64);
        for (i, cc) in ccs.iter().enumerate() {
            let column_name_id = g.get_column_name_id(&cc.name);
            let offset = dst.len() - dst_len;
            cc.marshal(dst, false);
            csh_index.const_columns_refs[i] = ColumnHeaderRef {
                column_name_id,
                offset: offset as u64,
            };
        }
    }

    /// Unmarshals csh from src.
    ///
    /// PORT NOTE: the Go name is kept for parity; unlike Go, the port copies
    /// names and values into owned strings, so csh stays valid after src is
    /// changed.
    pub fn unmarshal_inplace(
        &mut self,
        src: &[u8],
        part_format_version: u64,
    ) -> Result<(), String> {
        self.reset();

        // unmarshal columnHeaders
        let (n, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err("cannot unmarshal columnHeaders len".to_string());
        }
        let mut src = &src[n_size as usize..];
        if n > 1_000_000 {
            return Err(format!("too big number of columnHeaders: {n}"));
        }

        let chs_len = n as usize;
        self.resize_column_headers(chs_len);
        for i in 0..chs_len {
            let tail = self.column_headers[i]
                .unmarshal_inplace(src, part_format_version)
                .map_err(|err| {
                    format!(
                        "cannot unmarshal columnHeader {i} out of {chs_len} columnHeaders: {err}"
                    )
                })?;
            src = tail;
        }

        if chs_len > MAX_COLUMNS_PER_BLOCK {
            let column_names = get_names_from_column_headers(&self.column_headers);
            return Err(format!(
                "too many column headers: {chs_len}; it mustn't exceed {MAX_COLUMNS_PER_BLOCK}; columns: {}",
                format_string_slice(&column_names)
            ));
        }

        // unmarshal constColumns
        let (n, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err("cannot unmarshal constColumns len".to_string());
        }
        src = &src[n_size as usize..];
        if n > 1_000_000 {
            return Err(format!("too big number of constColumns: {n}"));
        }

        let ccs_len = n as usize;
        self.resize_const_columns(ccs_len);
        for i in 0..ccs_len {
            let tail = self.const_columns[i]
                .unmarshal_inplace(src, part_format_version < 1)
                .map_err(|err| {
                    format!("cannot unmarshal constColumn {i} out of {ccs_len} columns: {err}")
                })?;
            src = tail;
        }

        if ccs_len + self.column_headers.len() > MAX_COLUMNS_PER_BLOCK {
            let mut column_names = get_names_from_column_headers(&self.column_headers);
            for cc in &self.const_columns {
                column_names.push(cc.name.clone());
            }
            return Err(format!(
                "too many columns: {}; mustn't exceed {MAX_COLUMNS_PER_BLOCK}; columns: {}",
                ccs_len + self.column_headers.len(),
                format_string_slice(&column_names)
            ));
        }

        // Verify that the src is empty
        if !src.is_empty() {
            return Err(format!(
                "unexpected non-empty tail left after unmarshaling columnsHeader: len(tail)={}",
                src.len()
            ));
        }

        Ok(())
    }
}

fn get_names_from_column_headers(chs: &[ColumnHeader]) -> Vec<String> {
    chs.iter().map(|ch| ch.name.clone()).collect()
}

/// Formats a slice of strings like Go's fmt "%s" / "%v" verbs do
/// (space-separated values in square brackets).
fn format_string_slice<S: AsRef<str>>(a: &[S]) -> String {
    let names: Vec<&str> = a.iter().map(|s| s.as_ref()).collect();
    format!("[{}]", names.join(" "))
}

/// ColumnHeader contains information for values, which belong to a single label in a single block.
///
/// The main column with an empty name is stored in messageValuesFilename,
/// while the rest of columns are stored in smallValuesFilename or bigValuesFilename depending
/// on the block size (see maxSmallValuesBlockSize).
/// This allows minimizing disk read IO when filtering by non-message columns.
///
/// Every block column contains also a bloom filter for all the tokens stored in the column.
/// This bloom filter is used for fast determining whether the given block may contain the given tokens.
///
/// Tokens in bloom filter depend on valueType:
///
///   - valueTypeString stores tokens seen in all the values
///   - valueTypeDict doesn't store anything in the bloom filter, since all the encoded values
///     are available directly in the valuesDict field
///   - valueTypeUint8, valueTypeUint16, valueTypeUint32 and valueTypeUint64 stores encoded uint values
///   - valueTypeInt64 stores encoded int64 values
///   - valueTypeFloat64 stores encoded float64 values
///   - valueTypeIPv4 stores encoded into uint32 ips
///   - valueTypeTimestampISO8601 stores encoded into uint64 timestamps
///
/// Bloom filters for main column with an empty name is stored in messageBloomFilename,
/// while the rest of columns are stored in smallBloomFilename or bigBloomFilename depending on their size
/// (see maxSmallBloomFilterBlockSize).
#[derive(Debug, Default)]
pub struct ColumnHeader {
    /// name contains column name aka label name.
    pub name: String,

    /// valueType is the type of values stored in the block.
    pub value_type: ValueType,

    /// minValue is the minimum encoded value for uint*, ipv4, timestamp and float64 value in the columnHeader.
    ///
    /// It is used for fast detection of whether the given columnHeader contains values in the given range.
    pub min_value: u64,

    /// maxValue is the maximum encoded value for uint*, ipv4, timestamp and float64 value in the columnHeader.
    ///
    /// It is used for fast detection of whether the given columnHeader contains values in the given range.
    pub max_value: u64,

    /// valuesDict contains unique values for valueType = valueTypeDict.
    pub values_dict: ValuesDict,

    /// valuesOffset contains the offset of the block in either messageValuesFilename, smallValuesFilename or bigValuesFilename.
    pub values_offset: u64,

    /// valuesSize contains the size of the block in either messageValuesFilename, smallValuesFilename or bigValuesFilename.
    pub values_size: u64,

    /// bloomFilterOffset contains the offset of the bloom filter in messageBloomFilename, smallBloomFilename or bigBloomFilename.
    pub bloom_filter_offset: u64,

    /// bloomFilterSize contains the size of the bloom filter in messageBloomFilename, smallBloomFilename or bigBloomFilename.
    pub bloom_filter_size: u64,
}

// PORT NOTE: covers Go's reflect.DeepEqual comparisons in tests; ValuesDict
// does not derive PartialEq, so the impl is written out.
impl PartialEq for ColumnHeader {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.value_type == other.value_type
            && self.min_value == other.min_value
            && self.max_value == other.max_value
            && self.values_dict.values == other.values_dict.values
            && self.values_offset == other.values_offset
            && self.values_size == other.values_size
            && self.bloom_filter_offset == other.bloom_filter_offset
            && self.bloom_filter_size == other.bloom_filter_size
    }
}

impl ColumnHeader {
    /// Resets ch.
    pub fn reset(&mut self) {
        self.name.clear();
        self.value_type = ValueType(0);

        self.min_value = 0;
        self.max_value = 0;
        self.values_dict.reset();

        self.values_offset = 0;
        self.values_size = 0;

        self.bloom_filter_offset = 0;
        self.bloom_filter_size = 0;
    }

    /// Appends marshaled ch to dst.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        // check minValue/maxValue
        match self.value_type {
            ValueType::INT64 => {
                let min_value = self.min_value as i64;
                let max_value = self.max_value as i64;
                if min_value > max_value {
                    panicf!(
                        "BUG: minValue={min_value} must be smaller than maxValue={max_value} for valueTypeInt64"
                    );
                }
            }
            ValueType::FLOAT64 => {
                let min_value = f64::from_bits(self.min_value);
                let max_value = f64::from_bits(self.max_value);
                if min_value > max_value {
                    // PORT NOTE: Go formats the values with %g; Rust's
                    // default f64 formatting differs for very big/small
                    // values in this BUG panic message.
                    panicf!(
                        "BUG: minValue={min_value} must be smaller than maxValue={max_value} for valueTypeFloat64"
                    );
                }
            }
            ValueType::TIMESTAMP_ISO8601 => {
                let min_value = self.min_value as i64;
                let max_value = self.max_value as i64;
                if min_value > max_value {
                    panicf!(
                        "BUG: minValue={min_value} must be smaller than maxValue={max_value} for valueTypeTimestampISO8601"
                    );
                }
            }
            _ => {
                if self.min_value > self.max_value {
                    panicf!(
                        "BUG: minValue={} must be smaller than maxValue={} for valueType={}",
                        self.min_value,
                        self.max_value,
                        self.value_type.0
                    );
                }
            }
        }

        // Do not encode ch.name, since it should be encoded at columnsHeaderIndex.columnHeadersRefs

        // Encode common field - ch.valueType
        dst.push(self.value_type.0);

        // Encode other fields depending on ch.valueType
        match self.value_type {
            ValueType::STRING => {
                self.marshal_values_and_bloom_filters(dst);
            }
            ValueType::DICT => {
                self.values_dict.marshal(dst);
                self.marshal_values(dst);
            }
            ValueType::UINT8 => {
                dst.push(self.min_value as u8);
                dst.push(self.max_value as u8);
                self.marshal_values_and_bloom_filters(dst);
            }
            ValueType::UINT16 => {
                encoding::marshal_uint16(dst, self.min_value as u16);
                encoding::marshal_uint16(dst, self.max_value as u16);
                self.marshal_values_and_bloom_filters(dst);
            }
            ValueType::UINT32 => {
                encoding::marshal_uint32(dst, self.min_value as u32);
                encoding::marshal_uint32(dst, self.max_value as u32);
                self.marshal_values_and_bloom_filters(dst);
            }
            ValueType::UINT64 => {
                encoding::marshal_uint64(dst, self.min_value);
                encoding::marshal_uint64(dst, self.max_value);
                self.marshal_values_and_bloom_filters(dst);
            }
            ValueType::INT64 => {
                encoding::marshal_int64(dst, self.min_value as i64);
                encoding::marshal_int64(dst, self.max_value as i64);
                self.marshal_values_and_bloom_filters(dst);
            }
            ValueType::FLOAT64 => {
                // float64 values are encoded as uint64 via math.Float64bits()
                encoding::marshal_uint64(dst, self.min_value);
                encoding::marshal_uint64(dst, self.max_value);
                self.marshal_values_and_bloom_filters(dst);
            }
            ValueType::IPV4 => {
                encoding::marshal_uint32(dst, self.min_value as u32);
                encoding::marshal_uint32(dst, self.max_value as u32);
                self.marshal_values_and_bloom_filters(dst);
            }
            ValueType::TIMESTAMP_ISO8601 => {
                // timestamps are encoded in nanoseconds
                encoding::marshal_uint64(dst, self.min_value);
                encoding::marshal_uint64(dst, self.max_value);
                self.marshal_values_and_bloom_filters(dst);
            }
            _ => {
                panicf!("BUG: unknown valueType={}", self.value_type.0);
            }
        }
    }

    fn marshal_values_and_bloom_filters(&self, dst: &mut Vec<u8>) {
        self.marshal_values(dst);
        self.marshal_bloom_filters(dst);
    }

    fn marshal_values(&self, dst: &mut Vec<u8>) {
        encoding::marshal_var_uint64(dst, self.values_offset);
        encoding::marshal_var_uint64(dst, self.values_size);
    }

    fn marshal_bloom_filters(&self, dst: &mut Vec<u8>) {
        encoding::marshal_var_uint64(dst, self.bloom_filter_offset);
        encoding::marshal_var_uint64(dst, self.bloom_filter_size);
    }

    /// Unmarshals ch from src and returns the tail left after unmarshaling.
    ///
    /// PORT NOTE: the Go name is kept for parity; unlike Go, the port copies
    /// the column name into an owned string, so ch stays valid after src is
    /// changed.
    pub fn unmarshal_inplace<'a>(
        &mut self,
        src: &'a [u8],
        part_format_version: u64,
    ) -> Result<&'a [u8], String> {
        self.reset();

        let mut src = src;

        // Unmarshal column name
        if part_format_version < 1 {
            let (data, n_size) = encoding::unmarshal_bytes(src);
            if n_size <= 0 {
                return Err("cannot unmarshal column name".to_string());
            }
            src = &src[n_size as usize..];
            self.name
                .push_str(bytesutil::to_unsafe_string(data.unwrap_or_default()));
        }

        // Unmarshal value type
        if src.is_empty() {
            return Err(format!(
                "cannot unmarshal valueType from 0 bytes for column {:?}; need at least 1 byte",
                self.name
            ));
        }
        self.value_type = ValueType(src[0]);
        src = &src[1..];

        // Unmarshal the rest of data depending on valueType
        match self.value_type {
            ValueType::STRING => {
                src = self.unmarshal_values_and_bloom_filters(src).map_err(|err| {
                    format!(
                        "cannot unmarshal values and bloom filters at valueTypeString for column {:?}: {err}",
                        self.name
                    )
                })?;
            }
            ValueType::DICT => {
                src = self.values_dict.unmarshal_inplace(src).map_err(|err| {
                    format!(
                        "cannot unmarshal dict at valueTypeDict for column {:?}: {err}",
                        self.name
                    )
                })?;

                src = self.unmarshal_values(src).map_err(|err| {
                    format!(
                        "cannot unmarshal values at valueTypeDict for column {:?}: {err}",
                        self.name
                    )
                })?;
            }
            ValueType::UINT8 => {
                if src.len() < 2 {
                    return Err(format!(
                        "cannot unmarshal min/max values at valueTypeUint8 from {} bytes for column {:?}; need at least 2 bytes",
                        src.len(),
                        self.name
                    ));
                }
                self.min_value = u64::from(src[0]);
                self.max_value = u64::from(src[1]);
                src = &src[2..];

                src = self.unmarshal_values_and_bloom_filters(src).map_err(|err| {
                    format!(
                        "cannot unmarshal values and bloom filters at valueTypeUint8 for column {:?}: {err}",
                        self.name
                    )
                })?;
            }
            ValueType::UINT16 => {
                if src.len() < 4 {
                    return Err(format!(
                        "cannot unmarshal min/max values at valueTypeUint16 from {} bytes for column {:?}; need at least 4 bytes",
                        src.len(),
                        self.name
                    ));
                }
                self.min_value = u64::from(encoding::unmarshal_uint16(src));
                self.max_value = u64::from(encoding::unmarshal_uint16(&src[2..]));
                src = &src[4..];

                src = self.unmarshal_values_and_bloom_filters(src).map_err(|err| {
                    format!(
                        "cannot unmarshal values and bloom filters at valueTypeUint16 for column {:?}: {err}",
                        self.name
                    )
                })?;
            }
            ValueType::UINT32 => {
                if src.len() < 8 {
                    return Err(format!(
                        "cannot unmarshal min/max values at valueTypeUint32 from {} bytes for column {:?}; need at least 8 bytes",
                        src.len(),
                        self.name
                    ));
                }
                self.min_value = u64::from(encoding::unmarshal_uint32(src));
                self.max_value = u64::from(encoding::unmarshal_uint32(&src[4..]));
                src = &src[8..];

                src = self.unmarshal_values_and_bloom_filters(src).map_err(|err| {
                    format!(
                        "cannot unmarshal values and bloom filters at valueTypeUint32 for column {:?}: {err}",
                        self.name
                    )
                })?;
            }
            ValueType::UINT64 => {
                if src.len() < 16 {
                    return Err(format!(
                        "cannot unmarshal min/max values at valueTypeUint64 from {} bytes for column {:?}; need at least 16 bytes",
                        src.len(),
                        self.name
                    ));
                }
                self.min_value = encoding::unmarshal_uint64(src);
                self.max_value = encoding::unmarshal_uint64(&src[8..]);
                src = &src[16..];

                src = self.unmarshal_values_and_bloom_filters(src).map_err(|err| {
                    format!(
                        "cannot unmarshal values and bloom filters at valueTypeUint64 for column {:?}: {err}",
                        self.name
                    )
                })?;
            }
            ValueType::INT64 => {
                if src.len() < 16 {
                    return Err(format!(
                        "cannot unmarshal min/max values at valueTypeInt64 from {} bytes for column {:?}; need at least 16 bytes",
                        src.len(),
                        self.name
                    ));
                }
                self.min_value = encoding::unmarshal_int64(src) as u64;
                self.max_value = encoding::unmarshal_int64(&src[8..]) as u64;
                src = &src[16..];

                src = self.unmarshal_values_and_bloom_filters(src).map_err(|err| {
                    format!(
                        "cannot unmarshal values and bloom filters at valueTypeInt64 for column {:?}: {err}",
                        self.name
                    )
                })?;
            }
            ValueType::FLOAT64 => {
                if src.len() < 16 {
                    return Err(format!(
                        "cannot unmarshal min/max values at valueTypeFloat64 from {} bytes for column {:?}; need at least 16 bytes",
                        src.len(),
                        self.name
                    ));
                }
                // min and max values must be converted to real values with math.Float64frombits() during querying.
                self.min_value = encoding::unmarshal_uint64(src);
                self.max_value = encoding::unmarshal_uint64(&src[8..]);
                src = &src[16..];

                src = self.unmarshal_values_and_bloom_filters(src).map_err(|err| {
                    format!(
                        "cannot unmarshal values and bloom filters at valueTypeFloat64 for column {:?}: {err}",
                        self.name
                    )
                })?;
            }
            ValueType::IPV4 => {
                if src.len() < 8 {
                    return Err(format!(
                        "cannot unmarshal min/max values at valueTypeIPv4 from {} bytes for column {:?}; need at least 8 bytes",
                        src.len(),
                        self.name
                    ));
                }
                self.min_value = u64::from(encoding::unmarshal_uint32(src));
                self.max_value = u64::from(encoding::unmarshal_uint32(&src[4..]));
                src = &src[8..];

                src = self.unmarshal_values_and_bloom_filters(src).map_err(|err| {
                    format!(
                        "cannot unmarshal values and bloom filters at valueTypeIPv4 for column {:?}: {err}",
                        self.name
                    )
                })?;
            }
            ValueType::TIMESTAMP_ISO8601 => {
                if src.len() < 16 {
                    return Err(format!(
                        "cannot unmarshal min/max values at valueTypeTimestampISO8601 from {} bytes for column {:?}; need at least 16 bytes",
                        src.len(),
                        self.name
                    ));
                }
                self.min_value = encoding::unmarshal_uint64(src);
                self.max_value = encoding::unmarshal_uint64(&src[8..]);
                src = &src[16..];

                src = self.unmarshal_values_and_bloom_filters(src).map_err(|err| {
                    format!(
                        "cannot unmarshal values and bloom filters at valueTypeTimestampISO8601 for column {:?}: {err}",
                        self.name
                    )
                })?;
            }
            _ => {
                return Err(format!(
                    "unexpected valueType={} for column {:?}",
                    self.value_type.0, self.name
                ));
            }
        }

        Ok(src)
    }

    fn unmarshal_values_and_bloom_filters<'a>(
        &mut self,
        src: &'a [u8],
    ) -> Result<&'a [u8], String> {
        let src = self
            .unmarshal_values(src)
            .map_err(|err| format!("cannot unmarshal values: {err}"))?;

        let src = self
            .unmarshal_bloom_filters(src)
            .map_err(|err| format!("cannot unmarshal bloom filters: {err}"))?;

        Ok(src)
    }

    fn unmarshal_values<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        let (n, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err("cannot unmarshal valuesOffset".to_string());
        }
        let mut src = &src[n_size as usize..];
        self.values_offset = n;

        let (n, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err("cannot unmarshal valuesSize".to_string());
        }
        src = &src[n_size as usize..];
        if n > MAX_VALUES_BLOCK_SIZE as u64 {
            return Err(format!(
                "too big valuesSize: {n} bytes; mustn't exceed {MAX_VALUES_BLOCK_SIZE} bytes"
            ));
        }
        self.values_size = n;

        Ok(src)
    }

    fn unmarshal_bloom_filters<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        let (n, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err("cannot unmarshal bloomFilterOffset".to_string());
        }
        let mut src = &src[n_size as usize..];
        self.bloom_filter_offset = n;

        let (n, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err("cannot unmarshal bloomFilterSize".to_string());
        }
        src = &src[n_size as usize..];
        if n > MAX_BLOOM_FILTER_BLOCK_SIZE as u64 {
            return Err(format!(
                "too big bloomFilterSize: {n} bytes; mustn't exceed {MAX_BLOOM_FILTER_BLOCK_SIZE} bytes"
            ));
        }
        self.bloom_filter_size = n;

        Ok(src)
    }
}

/// TimestampsHeader contains the information about timestamps block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampsHeader {
    /// blockOffset is an offset of timestamps block inside timestampsFilename file.
    pub block_offset: u64,

    /// blockSize is the size of the timestamps block inside timestampsFilename file.
    pub block_size: u64,

    /// minTimestamp is the minimum timestamp seen in the block in nanoseconds.
    pub min_timestamp: i64,

    /// maxTimestamp is the maximum timestamp seen in the block in nanoseconds.
    pub max_timestamp: i64,

    /// marshalType is the type used for encoding the timestamps block.
    pub marshal_type: MarshalType,
}

impl Default for TimestampsHeader {
    fn default() -> Self {
        TimestampsHeader {
            block_offset: 0,
            block_size: 0,
            min_timestamp: 0,
            max_timestamp: 0,
            marshal_type: MarshalType(0),
        }
    }
}

impl TimestampsHeader {
    /// Resets th, so it can be reused.
    pub fn reset(&mut self) {
        self.block_offset = 0;
        self.block_size = 0;
        self.min_timestamp = 0;
        self.max_timestamp = 0;
        self.marshal_type = MarshalType(0);
    }

    pub fn copy_from(&mut self, src: &TimestampsHeader) {
        self.block_offset = src.block_offset;
        self.block_size = src.block_size;
        self.min_timestamp = src.min_timestamp;
        self.max_timestamp = src.max_timestamp;
        self.marshal_type = src.marshal_type;
    }

    pub fn sub_time_offset(&mut self, time_offset: i64) {
        if time_offset != 0 {
            self.min_timestamp = sub_int64_no_overflow(self.min_timestamp, time_offset);
            self.max_timestamp = sub_int64_no_overflow(self.max_timestamp, time_offset);
        }
    }

    /// Appends marshaled th to dst.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        encoding::marshal_uint64(dst, self.block_offset);
        encoding::marshal_uint64(dst, self.block_size);
        encoding::marshal_uint64(dst, self.min_timestamp as u64);
        encoding::marshal_uint64(dst, self.max_timestamp as u64);
        dst.push(self.marshal_type.0);
    }

    /// Unmarshals th from src and returns the tail left after the unmarshaling.
    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        self.reset();

        if src.len() < 33 {
            return Err(format!(
                "cannot unmarshal timestampsHeader from {} bytes; need at least 33 bytes",
                src.len()
            ));
        }

        self.block_offset = encoding::unmarshal_uint64(src);
        self.block_size = encoding::unmarshal_uint64(&src[8..]);
        self.min_timestamp = encoding::unmarshal_uint64(&src[16..]) as i64;
        self.max_timestamp = encoding::unmarshal_uint64(&src[24..]) as i64;
        self.marshal_type = MarshalType(src[32]);

        Ok(&src[33..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::PART_FORMAT_LATEST_VERSION;
    use crate::tenant_id::TenantID;
    use crate::u128::U128;

    #[test]
    fn test_block_header_marshal_unmarshal() {
        fn f(bh: &BlockHeader, marshaled_len: usize) {
            let mut data = Vec::new();
            bh.marshal(&mut data);
            assert_eq!(
                data.len(),
                marshaled_len,
                "unexpected lengths of the marshaled blockHeader; got {}; want {}",
                data.len(),
                marshaled_len
            );
            let mut bh2 = BlockHeader::default();
            let tail = bh2
                .unmarshal(&data, PART_FORMAT_LATEST_VERSION)
                .unwrap_or_else(|err| panic!("unexpected error in unmarshal: {err}"));
            assert!(
                tail.is_empty(),
                "unexpected non-empty tail after unmarshal: {tail:X?}"
            );
            assert_eq!(
                bh, &bh2,
                "unexpected blockHeader unmarshaled\ngot\n{bh2:?}\nwant\n{bh:?}"
            );
        }
        f(&BlockHeader::default(), 63);
        f(
            &BlockHeader {
                stream_id: StreamID {
                    tenant_id: TenantID {
                        account_id: 123,
                        project_id: 456,
                    },
                    id: U128 {
                        lo: 3443,
                        hi: 23434,
                    },
                },
                uncompressed_size_bytes: 4344,
                rows_count: 1234,
                timestamps_header: TimestampsHeader {
                    block_offset: 13234,
                    block_size: 8843,
                    min_timestamp: -4334,
                    max_timestamp: 23434,
                    marshal_type: MarshalType::NEAREST_DELTA2,
                },
                columns_header_index_offset: 8923481,
                columns_header_index_size: 8989832,
                columns_header_offset: 4384,
                columns_header_size: 894,
            },
            73,
        );
    }

    #[test]
    fn test_columns_header_index_marshal_unmarshal() {
        fn f(csh_index: &ColumnsHeaderIndex, marshaled_len: usize) {
            let mut data = Vec::new();
            csh_index.marshal(&mut data);
            assert_eq!(
                data.len(),
                marshaled_len,
                "unexpected lengths of the marshaled columnsHeader; got {}; want {}",
                data.len(),
                marshaled_len
            );
            let mut csh_index2 = ColumnsHeaderIndex::default();
            csh_index2
                .unmarshal_inplace(&data)
                .unwrap_or_else(|err| panic!("unexpected error in unmarshal: {err}"));

            assert_eq!(
                csh_index, &csh_index2,
                "unexpected blockHeaderIndex unmarshaled\ngot\n{csh_index2:?}\nwant\n{csh_index:?}"
            );
        }

        f(&ColumnsHeaderIndex::default(), 2);
        f(
            &ColumnsHeaderIndex {
                column_headers_refs: vec![
                    ColumnHeaderRef {
                        column_name_id: 234,
                        offset: 123432,
                    },
                    ColumnHeaderRef {
                        column_name_id: 23898,
                        offset: 0,
                    },
                ],
                const_columns_refs: vec![ColumnHeaderRef {
                    column_name_id: 0,
                    offset: 8989,
                }],
            },
            14,
        );
    }

    #[test]
    fn test_columns_header_marshal_unmarshal() {
        fn f(csh: &ColumnsHeader, marshaled_len: usize) {
            let mut csh_index = get_columns_header_index();
            let mut g = ColumnNameIDGenerator::default();

            let mut data = Vec::new();
            csh.marshal(&mut data, &mut csh_index, &mut g);
            assert_eq!(
                data.len(),
                marshaled_len,
                "unexpected length of the marshaled columnsHeader; got {}; want {}",
                data.len(),
                marshaled_len
            );
            let mut csh2 = ColumnsHeader::default();
            csh2.unmarshal_inplace(&data, PART_FORMAT_LATEST_VERSION)
                .unwrap_or_else(|err| panic!("unexpected error in unmarshal: {err}"));
            csh2.set_column_names(&csh_index, &g.column_names)
                .unwrap_or_else(|err| panic!("cannot set column names: {err}"));

            assert_eq!(
                csh, &csh2,
                "unexpected blockHeader unmarshaled\ngot\n{csh2:?}\nwant\n{csh:?}"
            );
        }

        f(&ColumnsHeader::default(), 2);
        f(
            &ColumnsHeader {
                column_headers: vec![
                    ColumnHeader {
                        name: "foobar".to_string(),
                        value_type: ValueType::STRING,
                        values_offset: 12345,
                        values_size: 23434,
                        bloom_filter_offset: 89843,
                        bloom_filter_size: 8934,
                        ..Default::default()
                    },
                    ColumnHeader {
                        name: "message".to_string(),
                        value_type: ValueType::UINT16,
                        min_value: 123,
                        max_value: 456,
                        values_offset: 3412345,
                        values_size: 234434,
                        bloom_filter_offset: 83,
                        bloom_filter_size: 34,
                        ..Default::default()
                    },
                ],
                const_columns: vec![Field {
                    name: "foo".to_string(),
                    value: b"bar".to_vec(),
                }],
            },
            31,
        );
    }

    #[test]
    fn test_block_header_unmarshal_failure() {
        fn f(data: &[u8]) {
            let mut bh = get_block_header();
            let result = bh.unmarshal(data, PART_FORMAT_LATEST_VERSION);
            assert!(result.is_err(), "expecting non-nil error");
            // PORT NOTE: the Go test also verifies that the returned tail
            // equals the original data on error; the Rust port returns no
            // tail on error and data stays untouched by construction.
            put_block_header(bh);
        }
        f(&[]);
        f(b"foo");

        let bh = BlockHeader {
            stream_id: StreamID {
                tenant_id: TenantID {
                    account_id: 123,
                    project_id: 456,
                },
                id: U128 {
                    lo: 3443,
                    hi: 23434,
                },
            },
            uncompressed_size_bytes: 4344,
            rows_count: 1234,
            timestamps_header: TimestampsHeader {
                block_offset: 13234,
                block_size: 8843,
                min_timestamp: -4334,
                max_timestamp: 23434,
                marshal_type: MarshalType::NEAREST_DELTA2,
            },
            columns_header_index_offset: 89434,
            columns_header_index_size: 89123,
            columns_header_offset: 4384,
            columns_header_size: 894,
        };
        let mut data = Vec::new();
        bh.marshal(&mut data);
        while !data.is_empty() {
            data.truncate(data.len() - 1);
            f(&data);
        }
    }

    #[test]
    fn test_columns_header_index_unmarshal_failure() {
        fn f(data: &[u8]) {
            let mut csh_index = get_columns_header_index();
            assert!(
                csh_index.unmarshal_inplace(data).is_err(),
                "expecting non-nil error"
            );
            put_columns_header_index(csh_index);
        }

        f(&[]);
        f(b"foo");

        let csh_index = ColumnsHeaderIndex {
            column_headers_refs: vec![ColumnHeaderRef {
                column_name_id: 0,
                offset: 123,
            }],
            const_columns_refs: vec![
                ColumnHeaderRef {
                    column_name_id: 2,
                    offset: 89834,
                },
                ColumnHeaderRef {
                    column_name_id: 234,
                    offset: 8934,
                },
            ],
        };
        let mut data = Vec::new();
        csh_index.marshal(&mut data);
        while !data.is_empty() {
            data.truncate(data.len() - 1);
            f(&data);
        }
    }

    #[test]
    fn test_columns_header_unmarshal_failure() {
        fn f(data: &[u8]) {
            let mut csh = get_columns_header();
            assert!(
                csh.unmarshal_inplace(data, PART_FORMAT_LATEST_VERSION)
                    .is_err(),
                "expecting non-nil error"
            );
            put_columns_header(csh);
        }

        f(&[]);
        f(b"foo");

        let csh = ColumnsHeader {
            column_headers: vec![
                ColumnHeader {
                    name: "foobar".to_string(),
                    value_type: ValueType::STRING,
                    values_offset: 12345,
                    values_size: 23434,
                    bloom_filter_offset: 89843,
                    bloom_filter_size: 8934,
                    ..Default::default()
                },
                ColumnHeader {
                    name: "message".to_string(),
                    value_type: ValueType::UINT16,
                    min_value: 123,
                    max_value: 456,
                    values_offset: 3412345,
                    values_size: 234434,
                    bloom_filter_offset: 83,
                    bloom_filter_size: 34,
                    ..Default::default()
                },
            ],
            const_columns: vec![Field {
                name: "foo".to_string(),
                value: b"bar".to_vec(),
            }],
        };
        let mut csh_index = get_columns_header_index();
        let mut g = ColumnNameIDGenerator::default();
        let mut data = Vec::new();
        csh.marshal(&mut data, &mut csh_index, &mut g);
        while !data.is_empty() {
            data.truncate(data.len() - 1);
            f(&data);
        }
        put_columns_header_index(csh_index);
    }

    #[test]
    fn test_block_header_reset() {
        let mut bh = BlockHeader {
            stream_id: StreamID {
                tenant_id: TenantID {
                    account_id: 123,
                    project_id: 456,
                },
                id: U128 {
                    lo: 3443,
                    hi: 23434,
                },
            },
            uncompressed_size_bytes: 8984,
            rows_count: 1234,
            timestamps_header: TimestampsHeader {
                block_offset: 13234,
                block_size: 8843,
                min_timestamp: -4334,
                max_timestamp: 23434,
                marshal_type: MarshalType::NEAREST_DELTA2,
            },
            columns_header_index_offset: 18934,
            columns_header_index_size: 8912,
            columns_header_offset: 12332,
            columns_header_size: 234,
        };
        bh.reset();
        let bh_zero = BlockHeader::default();
        assert_eq!(
            bh, bh_zero,
            "unexpected non-zero blockHeader after reset: {bh:?}"
        );
    }

    #[test]
    fn test_columns_header_index_reset() {
        let mut csh_index = ColumnsHeaderIndex {
            column_headers_refs: vec![ColumnHeaderRef {
                column_name_id: 234,
                offset: 1234,
            }],
            const_columns_refs: vec![
                ColumnHeaderRef {
                    column_name_id: 328,
                    offset: 21344,
                },
                ColumnHeaderRef {
                    column_name_id: 1,
                    offset: 234,
                },
            ],
        };
        csh_index.reset();
        let csh_index_zero = ColumnsHeaderIndex::default();
        assert_eq!(
            csh_index, csh_index_zero,
            "unexpected non-zero columnsHeaderIndex after reset: {csh_index:?}"
        );
    }

    #[test]
    fn test_columns_header_reset() {
        let mut csh = ColumnsHeader {
            column_headers: vec![
                ColumnHeader {
                    name: "foobar".to_string(),
                    value_type: ValueType::STRING,
                    values_offset: 12345,
                    values_size: 23434,
                    bloom_filter_offset: 89843,
                    bloom_filter_size: 8934,
                    ..Default::default()
                },
                ColumnHeader {
                    name: "message".to_string(),
                    value_type: ValueType::UINT16,
                    min_value: 123,
                    max_value: 456,
                    values_offset: 3412345,
                    values_size: 234434,
                    bloom_filter_offset: 83,
                    bloom_filter_size: 34,
                    ..Default::default()
                },
            ],
            const_columns: vec![Field {
                name: "foo".to_string(),
                value: b"bar".to_vec(),
            }],
        };
        csh.reset();
        let csh_zero = ColumnsHeader::default();
        assert_eq!(
            csh, csh_zero,
            "unexpected non-zero columnsHeader after reset: {csh:?}"
        );
    }

    #[test]
    fn test_marshal_unmarshal_block_headers() {
        fn f(bhs: &[BlockHeader], marshaled_len: usize) {
            let mut data = Vec::new();
            for bh in bhs {
                bh.marshal(&mut data);
            }
            assert_eq!(
                data.len(),
                marshaled_len,
                "unexpected length for marshaled blockHeader entries; got {}; want {}",
                data.len(),
                marshaled_len
            );
            let mut bhs2 = Vec::new();
            unmarshal_block_headers(&mut bhs2, &data, PART_FORMAT_LATEST_VERSION).unwrap_or_else(
                |err| panic!("unexpected error when unmarshaling blockHeader entries: {err}"),
            );
            assert_eq!(
                bhs,
                &bhs2[..],
                "unexpected blockHeader entries unmarshaled\ngot\n{bhs2:?}\nwant\n{bhs:?}"
            );
        }
        f(&[], 0);
        f(&[BlockHeader::default()], 63);
        f(
            &[
                BlockHeader::default(),
                BlockHeader {
                    stream_id: StreamID {
                        tenant_id: TenantID {
                            account_id: 123,
                            project_id: 456,
                        },
                        id: U128 {
                            lo: 3443,
                            hi: 23434,
                        },
                    },
                    uncompressed_size_bytes: 89894,
                    rows_count: 1234,
                    timestamps_header: TimestampsHeader {
                        block_offset: 13234,
                        block_size: 8843,
                        min_timestamp: -4334,
                        max_timestamp: 23434,
                        marshal_type: MarshalType::NEAREST_DELTA2,
                    },
                    columns_header_index_offset: 1234,
                    columns_header_index_size: 89324,
                    columns_header_offset: 12332,
                    columns_header_size: 234,
                },
            ],
            134,
        );
    }

    #[test]
    fn test_column_header_marshal_unmarshal() {
        fn f(ch: &ColumnHeader, marshaled_len: usize) {
            let mut data = Vec::new();
            ch.marshal(&mut data);
            assert_eq!(
                data.len(),
                marshaled_len,
                "unexpected marshaled length of columnHeader; got {}; want {}",
                data.len(),
                marshaled_len
            );
            let mut ch2 = ColumnHeader::default();
            let tail = ch2
                .unmarshal_inplace(&data, PART_FORMAT_LATEST_VERSION)
                .unwrap_or_else(|err| panic!("unexpected error in umarshal({ch:?}): {err}"));
            assert!(
                tail.is_empty(),
                "unexpected non-empty tail after unmarshal({ch:?}): {tail:X?}"
            );

            // columnHeader.name isn't marshaled, since it is marshaled via columnsHeaderIndex starting from part format v1.
            ch2.name = ch.name.clone();

            assert_eq!(
                ch, &ch2,
                "unexpected columnHeader after unmarshal;\ngot\n{ch2:?}\nwant\n{ch:?}"
            );
        }

        f(
            &ColumnHeader {
                name: "foo".to_string(),
                value_type: ValueType::UINT8,
                ..Default::default()
            },
            7,
        );
        let mut ch = ColumnHeader {
            name: "foobar".to_string(),
            value_type: ValueType::DICT,

            values_offset: 12345,
            values_size: 254452,
            ..Default::default()
        };
        ch.values_dict.get_or_add(b"abc");
        f(&ch, 11);
    }

    #[test]
    fn test_column_header_unmarshal_failure() {
        fn f(data: &[u8]) {
            let mut ch = ColumnHeader::default();
            let result = ch.unmarshal_inplace(data, PART_FORMAT_LATEST_VERSION);
            assert!(result.is_err(), "expecting non-nil error");
            // PORT NOTE: the Go test also verifies that the returned tail
            // equals the original data on error; the Rust port returns no
            // tail on error and data stays untouched by construction.
        }

        f(&[]);
        f(b"foo");

        let ch = ColumnHeader {
            name: "abc".to_string(),
            value_type: ValueType::UINT16,
            bloom_filter_size: 3244,
            ..Default::default()
        };
        let mut data = Vec::new();
        ch.marshal(&mut data);
        f(&data[..data.len() - 1]);
    }

    #[test]
    fn test_column_header_reset() {
        let mut ch = ColumnHeader {
            name: "foobar".to_string(),
            value_type: ValueType::UINT16,

            values_offset: 12345,
            values_size: 254452,

            bloom_filter_offset: 34898234,
            bloom_filter_size: 873434,
            ..Default::default()
        };
        ch.values_dict.get_or_add(b"abc");
        ch.reset();
        let ch_zero = ColumnHeader::default();
        assert_eq!(
            ch, ch_zero,
            "unexpected non-zero columnHeader after reset: {ch:?}"
        );
    }

    #[test]
    fn test_timestamps_header_marshal_unmarshal() {
        fn f(th: &TimestampsHeader, marshaled_len: usize) {
            let mut data = Vec::new();
            th.marshal(&mut data);
            assert_eq!(
                data.len(),
                marshaled_len,
                "unexpected length of marshaled timestampsHeader; got {}; want {}",
                data.len(),
                marshaled_len
            );
            let mut th2 = TimestampsHeader::default();
            let tail = th2
                .unmarshal(&data)
                .unwrap_or_else(|err| panic!("unexpected error in unmarshal({th:?}): {err}"));
            assert!(
                tail.is_empty(),
                "unexpected non-nil tail after unmarshal({th:?}): {tail:X?}"
            );
            assert_eq!(
                th, &th2,
                "unexpected timestampsHeader after unmarshal; got\n{th2:?}\nwant\n{th:?}"
            );
        }
        f(&TimestampsHeader::default(), 33);

        f(
            &TimestampsHeader {
                block_offset: 12345,
                block_size: 3424834,
                min_timestamp: -123443,
                max_timestamp: 234343,
                marshal_type: MarshalType::ZSTD_NEAREST_DELTA,
            },
            33,
        );
    }

    #[test]
    fn test_timestamps_header_unmarshal_failure() {
        fn f(data: &[u8]) {
            let mut th = TimestampsHeader::default();
            let result = th.unmarshal(data);
            assert!(result.is_err(), "expecting non-nil error");
            // PORT NOTE: the Go test also verifies that the returned tail
            // equals the original data on error; the Rust port returns no
            // tail on error and data stays untouched by construction.
        }
        f(&[]);
        f(b"foo");
    }

    #[test]
    fn test_timestamps_header_reset() {
        let mut th = TimestampsHeader {
            block_offset: 12345,
            block_size: 3424834,
            min_timestamp: -123443,
            max_timestamp: 234343,
            marshal_type: MarshalType::ZSTD_NEAREST_DELTA,
        };
        th.reset();
        let th_zero = TimestampsHeader::default();
        assert_eq!(
            th, th_zero,
            "unexpected non-zero timestampsHeader after reset: {th:?}"
        );
    }
}

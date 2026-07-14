//! Port of EsLogs `lib/logstorage/block_data.go`.

use esl_common::{encoding as vc_encoding, panicf};

use crate::arena::Arena;
use crate::block::{get_block, put_block};
use crate::block_header::{
    BlockHeader, ColumnHeader, ColumnsHeader, TimestampsHeader, get_columns_header,
    get_columns_header_index, put_columns_header, put_columns_header_index,
};
use crate::block_stream_reader::StreamReaders;
use crate::block_stream_writer::{LONG_TERM_BUF_POOL, StreamWriters, must_write_columns_header};
use crate::consts::{
    MAX_BLOOM_FILTER_BLOCK_SIZE, MAX_COLUMNS_HEADER_INDEX_SIZE, MAX_COLUMNS_HEADER_SIZE,
    MAX_TIMESTAMPS_BLOCK_SIZE, MAX_VALUES_BLOCK_SIZE,
};
use crate::encoding::StringsBlockUnmarshaler;
use crate::rows::{Field, Rows, append_fields};
use crate::stream_id::StreamID;
use crate::values_encoder::{ValueType, ValuesDecoder, ValuesDict};

/// blockData contains packed data for a single block.
///
/// The main purpose of this struct is to reduce the work needed during background merge of parts.
/// If the block is full, then the blockData can be written to the destination part
/// without the need to unpack it.
#[derive(Debug, Default, PartialEq)]
pub struct BlockData {
    /// streamID is id of the stream for the data
    pub stream_id: StreamID,

    /// uncompressedSizeBytes is the original (uncompressed) size of log entries stored in the block
    pub uncompressed_size_bytes: u64,

    /// rowsCount is the number of log entries in the block
    pub rows_count: u64,

    /// timestampsData contains the encoded timestamps data for the block
    pub timestamps_data: TimestampsData,

    /// columnsData contains packed per-column data
    pub columns_data: Vec<ColumnData>,

    /// constColumns contains data for const columns across the block
    pub const_columns: Vec<Field>,
}

impl BlockData {
    /// reset resets bd for subsequent reuse
    ///
    /// PORT NOTE: Go resets every columnData / const column in place and
    /// truncates the slices, keeping the element buffers for reuse;
    /// `Vec::clear` drops the elements, so only the outer vector capacity is
    /// retained.
    pub fn reset(&mut self) {
        self.stream_id.reset();
        self.uncompressed_size_bytes = 0;
        self.rows_count = 0;
        self.timestamps_data.reset();

        self.columns_data.clear();
        self.const_columns.clear();
    }

    fn resize_columns_data(&mut self, columns_data_len: usize) -> &mut [ColumnData] {
        self.columns_data.clear();
        self.columns_data
            .resize_with(columns_data_len, ColumnData::default);
        &mut self.columns_data
    }

    /// copyFrom copies src to bd.
    ///
    /// PORT NOTE: Go copies the data into an arena (`copyFrom(a *arena, src)`)
    /// and bd stays valid until a.reset(); the port stores owned buffers, so
    /// the arena parameter is dropped and bd is always valid.
    pub fn copy_from(&mut self, src: &BlockData) {
        self.reset();

        self.stream_id = src.stream_id;
        self.uncompressed_size_bytes = src.uncompressed_size_bytes;
        self.rows_count = src.rows_count;
        self.timestamps_data.copy_from(&src.timestamps_data);

        let cds_src = &src.columns_data;
        self.resize_columns_data(cds_src.len());
        for (i, cd_src) in cds_src.iter().enumerate() {
            self.columns_data[i].copy_from(cd_src);
        }

        self.const_columns.clear();
        append_fields(&mut self.const_columns, &src.const_columns);
    }

    /// unmarshalRows appends unmarshaled from bd log entries to dst.
    ///
    /// PORT NOTE: Go's unmarshaled log entries are valid until sbu and vd are
    /// reset; the port produces owned Strings, so dst stays valid.
    pub fn unmarshal_rows(
        &self,
        dst: &mut Rows,
        sbu: &mut StringsBlockUnmarshaler,
        vd: &mut ValuesDecoder,
    ) -> Result<(), String> {
        let mut b = get_block();

        let res = b.init_from_block_data(self, sbu, vd);
        if res.is_ok() {
            b.append_rows_to(dst);
        }
        put_block(b);
        res
    }

    /// mustWriteTo writes bd to sw and updates bh accordingly
    pub fn must_write_to(&self, bh: &mut BlockHeader, sw: &mut StreamWriters<'_>) {
        bh.reset();

        bh.stream_id = self.stream_id;
        bh.uncompressed_size_bytes = self.uncompressed_size_bytes;
        bh.rows_count = self.rows_count;

        // Marshal timestamps
        self.timestamps_data
            .must_write_to(&mut bh.timestamps_header, sw);

        // Marshal columns
        let cds = &self.columns_data;

        let mut csh = get_columns_header();

        csh.resize_column_headers(cds.len());
        for (i, cd) in cds.iter().enumerate() {
            cd.must_write_to(&mut csh.column_headers[i], sw);
        }
        csh.const_columns.clear();
        csh.const_columns.extend_from_slice(&self.const_columns);

        // PORT NOTE: Go's csh.mustWriteTo(bh, sw) is ported as the free
        // must_write_columns_header() in block_stream_writer.rs.
        must_write_columns_header(&csh, bh, sw);

        put_columns_header(csh);
    }

    /// mustReadFrom reads block data associated with bh from sr to bd.
    ///
    /// PORT NOTE: Go reads the data into the arena and bd stays valid until
    /// a.reset(); the port stores owned buffers, so the arena parameter only
    /// keeps the call-site shape and is unused.
    pub fn must_read_from(&mut self, a: &mut Arena, bh: &BlockHeader, sr: &mut StreamReaders<'_>) {
        self.reset();

        self.stream_id = bh.stream_id;
        self.uncompressed_size_bytes = bh.uncompressed_size_bytes;
        self.rows_count = bh.rows_count;

        // Read timestamps
        self.timestamps_data
            .must_read_from(a, &bh.timestamps_header, sr);

        // Read columns
        if bh.columns_header_offset != sr.columns_header_reader.bytes_read {
            panicf!(
                "FATAL: {}: unexpected columnsHeaderOffset={}; must equal to the number of bytes read: {}",
                sr.columns_header_reader.path(),
                bh.columns_header_offset,
                sr.columns_header_reader.bytes_read
            );
        }
        let columns_header_size = bh.columns_header_size;
        if columns_header_size > MAX_COLUMNS_HEADER_SIZE as u64 {
            panicf!(
                "BUG: {}: too big columnsHeaderSize: {} bytes; mustn't exceed {} bytes",
                sr.columns_header_reader.path(),
                columns_header_size,
                MAX_COLUMNS_HEADER_SIZE
            );
        }
        let mut bb = LONG_TERM_BUF_POOL.get();
        bb.b.resize(columns_header_size as usize, 0);
        sr.columns_header_reader.must_read_full(&mut bb.b);

        let mut csh = get_columns_header();
        if let Err(err) = csh.unmarshal_inplace(&bb.b, sr.part_format_version) {
            panicf!(
                "FATAL: {}: cannot unmarshal columnsHeader: {}",
                sr.columns_header_reader.path(),
                err
            );
        }
        if sr.part_format_version >= 1 {
            read_column_names_from_columns_header_index(bh, sr, &mut csh);
        }

        let chs = &csh.column_headers;
        self.resize_columns_data(chs.len());
        for (i, ch) in chs.iter().enumerate() {
            self.columns_data[i].must_read_from(a, ch, sr);
        }
        self.const_columns.clear();
        append_fields(&mut self.const_columns, &csh.const_columns);
        put_columns_header(csh);
        LONG_TERM_BUF_POOL.put(bb);
    }
}

fn read_column_names_from_columns_header_index(
    bh: &BlockHeader,
    sr: &mut StreamReaders<'_>,
    csh: &mut ColumnsHeader,
) {
    let mut bb = LONG_TERM_BUF_POOL.get();

    let n = bh.columns_header_index_size;
    if n > MAX_COLUMNS_HEADER_INDEX_SIZE as u64 {
        panicf!(
            "BUG: {}: too big columnsHeaderIndexSize: {} bytes; mustn't exceed {} bytes",
            sr.columns_header_index_reader.path(),
            n,
            MAX_COLUMNS_HEADER_INDEX_SIZE
        );
    }

    bb.b.resize(n as usize, 0);
    sr.columns_header_index_reader.must_read_full(&mut bb.b);

    let mut csh_index = get_columns_header_index();
    if let Err(err) = csh_index.unmarshal_inplace(&bb.b) {
        panicf!(
            "FATAL: {}: cannot unmarshal columnsHeaderIndex: {}",
            sr.columns_header_index_reader.path(),
            err
        );
    }
    if let Err(err) = csh.set_column_names(&csh_index, &sr.column_names) {
        panicf!("FATAL: {}: {}", sr.columns_header_index_reader.path(), err);
    }

    put_columns_header_index(csh_index);
    LONG_TERM_BUF_POOL.put(bb);
}

/// timestampsData contains the encoded timestamps data.
#[derive(Debug, PartialEq, Eq)]
pub struct TimestampsData {
    /// data contains packed timestamps data.
    pub data: Vec<u8>,

    /// marshalType is the marshal type for timestamps
    pub marshal_type: vc_encoding::MarshalType,

    /// minTimestamp is the minimum timestamp in the timestamps data
    pub min_timestamp: i64,

    /// maxTimestamp is the maximum timestamp in the timestamps data
    pub max_timestamp: i64,
}

impl Default for TimestampsData {
    fn default() -> TimestampsData {
        TimestampsData {
            data: Vec::new(),
            marshal_type: vc_encoding::MarshalType(0),
            min_timestamp: 0,
            max_timestamp: 0,
        }
    }
}

impl TimestampsData {
    /// reset resets td for subsequent reuse
    pub fn reset(&mut self) {
        self.data.clear();
        self.marshal_type = vc_encoding::MarshalType(0);
        self.min_timestamp = 0;
        self.max_timestamp = 0;
    }

    /// copyFrom copies src to td.
    ///
    /// PORT NOTE: the Go arena parameter is dropped — see BlockData::copy_from.
    pub fn copy_from(&mut self, src: &TimestampsData) {
        self.reset();

        self.data.extend_from_slice(&src.data);
        self.marshal_type = src.marshal_type;
        self.min_timestamp = src.min_timestamp;
        self.max_timestamp = src.max_timestamp;
    }

    /// mustWriteTo writes td to sw and updates th accordingly
    pub fn must_write_to(&self, th: &mut TimestampsHeader, sw: &mut StreamWriters<'_>) {
        th.reset();

        th.marshal_type = self.marshal_type;
        th.min_timestamp = self.min_timestamp;
        th.max_timestamp = self.max_timestamp;
        th.block_offset = sw.timestamps_writer.bytes_written;
        th.block_size = self.data.len() as u64;
        if th.block_size > MAX_TIMESTAMPS_BLOCK_SIZE as u64 {
            panicf!(
                "BUG: too big timestampsHeader.blockSize: {} bytes; mustn't exceed {} bytes",
                th.block_size,
                MAX_TIMESTAMPS_BLOCK_SIZE
            );
        }
        sw.timestamps_writer.must_write(&self.data);
    }

    /// mustReadFrom reads timestamps data associated with th from sr to td.
    ///
    /// PORT NOTE: the arena parameter is unused — see BlockData::must_read_from.
    pub fn must_read_from(
        &mut self,
        _a: &mut Arena,
        th: &TimestampsHeader,
        sr: &mut StreamReaders<'_>,
    ) {
        self.reset();

        self.marshal_type = th.marshal_type;
        self.min_timestamp = th.min_timestamp;
        self.max_timestamp = th.max_timestamp;

        let timestamps_reader = &mut sr.timestamps_reader;
        if th.block_offset != timestamps_reader.bytes_read {
            panicf!(
                "FATAL: {}: unexpected timestampsHeader.blockOffset={}; must equal to the number of bytes read: {}",
                timestamps_reader.path(),
                th.block_offset,
                timestamps_reader.bytes_read
            );
        }
        let timestamps_block_size = th.block_size;
        if timestamps_block_size > MAX_TIMESTAMPS_BLOCK_SIZE as u64 {
            panicf!(
                "FATAL: {}: too big timestamps block with {} bytes; the maximum supported block size is {} bytes",
                timestamps_reader.path(),
                timestamps_block_size,
                MAX_TIMESTAMPS_BLOCK_SIZE
            );
        }
        self.data.resize(timestamps_block_size as usize, 0);
        timestamps_reader.must_read_full(&mut self.data);
    }
}

/// columnData contains packed data for a single column.
#[derive(Debug, Default)]
pub struct ColumnData {
    /// name is the column name
    /// PORT NOTE: raw bytes (Go strings are arbitrary bytes).
    pub name: Vec<u8>,

    /// valueType is the type of values stored in valuesData
    pub value_type: ValueType,

    /// minValue is the minimum encoded uint* or float64 value in the columnHeader
    ///
    /// It is used for fast detection of whether the given columnHeader contains values in the given range
    pub min_value: u64,

    /// maxValue is the maximum encoded uint* or float64 value in the columnHeader
    ///
    /// It is used for fast detection of whether the given columnHeader contains values in the given range
    pub max_value: u64,

    /// valuesDict contains unique values for valueType = valueTypeDict
    pub values_dict: ValuesDict,

    /// valuesData contains packed values data for the given column
    pub values_data: Vec<u8>,

    /// bloomFilterData contains packed bloomFilter data for the given column
    pub bloom_filter_data: Vec<u8>,
}

// PORT NOTE: covers Go's reflect.DeepEqual comparisons in tests; ValuesDict
// does not derive PartialEq, so the impl is written out.
impl PartialEq for ColumnData {
    fn eq(&self, other: &ColumnData) -> bool {
        self.name == other.name
            && self.value_type == other.value_type
            && self.min_value == other.min_value
            && self.max_value == other.max_value
            && self.values_dict.values == other.values_dict.values
            && self.values_data == other.values_data
            && self.bloom_filter_data == other.bloom_filter_data
    }
}

impl ColumnData {
    /// reset rests cd for subsequent reuse
    pub fn reset(&mut self) {
        self.name.clear();
        self.value_type = ValueType(0);

        self.min_value = 0;
        self.max_value = 0;
        self.values_dict.reset();

        self.values_data.clear();
        self.bloom_filter_data.clear();
    }

    /// copyFrom copies src to cd.
    ///
    /// PORT NOTE: the Go arena parameter is dropped — see BlockData::copy_from.
    pub fn copy_from(&mut self, src: &ColumnData) {
        self.reset();

        self.name.clone_from(&src.name);
        self.value_type = src.value_type;

        self.min_value = src.min_value;
        self.max_value = src.max_value;
        self.values_dict.copy_from_no_arena(&src.values_dict);

        self.values_data.extend_from_slice(&src.values_data);
        self.bloom_filter_data
            .extend_from_slice(&src.bloom_filter_data);
    }

    /// mustWriteTo writes cd to sw and updates ch accordingly.
    ///
    /// PORT NOTE: Go's ch is valid until cd is changed (ch.name shares
    /// cd.name); the port clones the name into ch.
    pub fn must_write_to(&self, ch: &mut ColumnHeader, sw: &mut StreamWriters<'_>) {
        ch.reset();

        ch.name.clone_from(&self.name);
        ch.value_type = self.value_type;

        ch.min_value = self.min_value;
        ch.max_value = self.max_value;
        ch.values_dict.copy_from_no_arena(&self.values_dict);

        let bloom_values_writer = sw.get_bloom_values_writer_for_column_name(&ch.name);

        // marshal values
        ch.values_size = self.values_data.len() as u64;
        if ch.values_size > MAX_VALUES_BLOCK_SIZE as u64 {
            panicf!(
                "BUG: too big valuesSize: {} bytes; mustn't exceed {} bytes",
                ch.values_size,
                MAX_VALUES_BLOCK_SIZE
            );
        }
        ch.values_offset = bloom_values_writer.values.bytes_written;
        bloom_values_writer.values.must_write(&self.values_data);

        // marshal bloom filter
        ch.bloom_filter_size = self.bloom_filter_data.len() as u64;
        if ch.bloom_filter_size > MAX_BLOOM_FILTER_BLOCK_SIZE as u64 {
            panicf!(
                "BUG: too big bloomFilterSize: {} bytes; mustn't exceed {} bytes",
                ch.bloom_filter_size,
                MAX_BLOOM_FILTER_BLOCK_SIZE
            );
        }
        ch.bloom_filter_offset = bloom_values_writer.bloom.bytes_written;
        bloom_values_writer
            .bloom
            .must_write(&self.bloom_filter_data);
    }

    /// mustReadFrom reads columns data associated with ch from sr to cd.
    ///
    /// PORT NOTE: the arena parameter is unused — see BlockData::must_read_from.
    pub fn must_read_from(
        &mut self,
        _a: &mut Arena,
        ch: &ColumnHeader,
        sr: &mut StreamReaders<'_>,
    ) {
        self.reset();

        self.name.clone_from(&ch.name);
        self.value_type = ch.value_type;

        self.min_value = ch.min_value;
        self.max_value = ch.max_value;
        self.values_dict.copy_from_no_arena(&ch.values_dict);

        let bloom_values_reader = sr.get_bloom_values_reader_for_column_name(&ch.name);

        // read values
        if ch.values_offset != bloom_values_reader.values.bytes_read {
            panicf!(
                "FATAL: {}: unexpected columnHeader.valuesOffset={}; must equal to the number of bytes read: {}",
                bloom_values_reader.values.path(),
                ch.values_offset,
                bloom_values_reader.values.bytes_read
            );
        }
        let values_size = ch.values_size;
        if values_size > MAX_VALUES_BLOCK_SIZE as u64 {
            panicf!(
                "FATAL: {}: values block size cannot exceed {} bytes; got {} bytes",
                bloom_values_reader.values.path(),
                MAX_VALUES_BLOCK_SIZE,
                values_size
            );
        }
        self.values_data.resize(values_size as usize, 0);
        bloom_values_reader
            .values
            .must_read_full(&mut self.values_data);

        // read bloom filter
        // bloom filter is missing in valueTypeDict.
        if ch.value_type != ValueType::DICT {
            if ch.bloom_filter_offset != bloom_values_reader.bloom.bytes_read {
                panicf!(
                    "FATAL: {}: unexpected columnHeader.bloomFilterOffset={}; must equal to the number of bytes read: {}",
                    bloom_values_reader.bloom.path(),
                    ch.bloom_filter_offset,
                    bloom_values_reader.bloom.bytes_read
                );
            }
            let bloom_filter_size = ch.bloom_filter_size;
            if bloom_filter_size > MAX_BLOOM_FILTER_BLOCK_SIZE as u64 {
                panicf!(
                    "FATAL: {}: bloom filter block size cannot exceed {} bytes; got {} bytes",
                    bloom_values_reader.bloom.path(),
                    MAX_BLOOM_FILTER_BLOCK_SIZE,
                    bloom_filter_size
                );
            }
            self.bloom_filter_data.resize(bloom_filter_size as usize, 0);
            bloom_values_reader
                .bloom
                .must_read_full(&mut self.bloom_filter_data);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant_id::TenantID;
    use esl_common::encoding::MarshalType;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_block_data_reset() {
        let mut bd = BlockData {
            stream_id: StreamID {
                tenant_id: TenantID {
                    account_id: 123,
                    project_id: 432,
                },
                ..Default::default()
            },
            uncompressed_size_bytes: 2344,
            rows_count: 134,
            timestamps_data: TimestampsData {
                data: b"foo".to_vec(),
                marshal_type: MarshalType::DELTA_CONST,
                min_timestamp: 1234,
                max_timestamp: 23443,
            },
            columns_data: vec![ColumnData {
                name: b"foo".to_vec(),
                value_type: ValueType::UINT16,
                values_data: b"aaa".to_vec(),
                bloom_filter_data: b"bsdf".to_vec(),
                ..Default::default()
            }],
            const_columns: vec![field("foo", "bar")],
        };
        bd.reset();
        let bd_zero = BlockData::default();
        assert_eq!(
            bd, bd_zero,
            "unexpected non-zero blockData after reset: {bd:?}"
        );
    }

    #[test]
    fn test_block_data_copy_from() {
        // PORT NOTE: the Go test obtains an arena via getArena()/putArena()
        // and passes it to copyFrom; the port stores owned buffers, so the
        // arena is dropped — see BlockData::copy_from.
        fn f(bd: &BlockData) {
            let mut bd2 = BlockData::default();
            bd2.copy_from(bd);
            assert_eq!(
                bd, &bd2,
                "unexpected blockData copy\ngot\n{bd2:?}\nwant\n{bd:?}"
            );

            // Try copying it again to the same destination
            bd2.copy_from(bd);
            assert_eq!(
                bd, &bd2,
                "unexpected blockData copy to the same destination\ngot\n{bd2:?}\nwant\n{bd:?}"
            );
        }

        f(&BlockData::default());

        let bd = BlockData {
            stream_id: StreamID {
                tenant_id: TenantID {
                    account_id: 123,
                    project_id: 432,
                },
                ..Default::default()
            },
            uncompressed_size_bytes: 8943,
            rows_count: 134,
            timestamps_data: TimestampsData {
                data: b"foo".to_vec(),
                marshal_type: MarshalType::DELTA_CONST,
                min_timestamp: 1234,
                max_timestamp: 23443,
            },
            columns_data: vec![
                ColumnData {
                    name: b"foo".to_vec(),
                    value_type: ValueType::UINT16,
                    values_data: b"aaa".to_vec(),
                    bloom_filter_data: b"bsdf".to_vec(),
                    ..Default::default()
                },
                ColumnData {
                    name: b"bar".to_vec(),
                    values_data: b"aaa".to_vec(),
                    bloom_filter_data: b"bsdf".to_vec(),
                    ..Default::default()
                },
            ],
            const_columns: vec![field("foobar", "baz")],
        };
        f(&bd);
    }
}

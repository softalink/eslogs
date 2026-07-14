//! Port of EsLogs `lib/logstorage/block.go`.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use esl_common::{encoding as vc_encoding, panicf, warnf};

use crate::block_data::BlockData;
use crate::block_header::{
    BlockHeader, ColumnHeader, TimestampsHeader, get_columns_header, put_columns_header,
};
use crate::block_stream_writer::{LONG_TERM_BUF_POOL, StreamWriters, must_write_columns_header};
use crate::bloomfilter::bloom_filter_marshal_hashes;
use crate::consts::{
    MAX_BLOOM_FILTER_BLOCK_SIZE, MAX_COLUMNS_PER_BLOCK, MAX_CONST_COLUMN_VALUE_SIZE,
    MAX_ROWS_PER_BLOCK, MAX_TIMESTAMPS_BLOCK_SIZE, MAX_VALUES_BLOCK_SIZE,
};
use crate::encoding::{StringsBlockUnmarshaler, marshal_strings_block};
use crate::hash_tokenizer::tokenize_hashes;
use crate::log_rows::{
    estimated_json_field_len, estimated_json_row_len, get_canonical_column_name_bytes,
};
use crate::rows::{Field, Rows, append_fields};
use crate::stream_id::StreamID;
use crate::values_encoder::{ValueType, ValuesDecoder, get_values_encoder, put_values_encoder};

/// The length of Go's `time.RFC3339Nano` layout string
/// ("2006-01-02T15:04:05.999999999Z07:00").
const TIME_RFC3339_NANO_LEN: usize = "2006-01-02T15:04:05.999999999Z07:00".len();

/// block represents a block of log entries.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Block {
    /// timestamps contains timestamps for log entries.
    pub timestamps: Vec<i64>,

    /// columns contains values for fields seen in log entries.
    pub columns: Vec<Column>,

    /// constColumns contains fields with constant values across all the block entries.
    pub const_columns: Vec<Field>,
}

impl Block {
    /// Resets b, so it can be reused.
    ///
    /// PORT NOTE: Go resets every column/const column in place and truncates
    /// the slices to zero length, so their backing buffers are reused on the
    /// next fill; Rust `Vec::clear` drops the elements, so only the outer
    /// vector capacity is retained.
    pub fn reset(&mut self) {
        self.timestamps.clear();
        self.columns.clear();
        self.const_columns.clear();
    }

    /// Returns the total size of the original log entries stored in b.
    ///
    /// It uses JSON format to calculate the size as if each log entry were represented as JSON.
    ///
    /// The calculation logic must stay in sync with estimated_json_row_len() in log_rows.rs.
    /// If you change logic here, update estimated_json_row_len() accordingly and vice versa.
    pub fn uncompressed_size_bytes(&self) -> usize {
        let rows_count = self.len();
        if rows_count == 0 {
            return 0;
        }

        let mut total_size =
            ("{}\n".len() + r#""_time":"""#.len() + TIME_RFC3339_NANO_LEN) * rows_count;

        // size of constant fields (included in every row)
        for cc in &self.const_columns {
            let name = get_canonical_column_name_bytes(&cc.name);
            total_size += estimated_json_field_len(name, &cc.value) * rows_count;
        }

        // add size of variable fields
        for c in &self.columns {
            let name = get_canonical_column_name_bytes(&c.name);

            for v in &c.values {
                // EsLogs data model (https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model)
                // treats empty values as non-existing values
                if v.is_empty() {
                    continue;
                }

                total_size += estimated_json_field_len(name, v);
            }
        }

        total_size
    }

    pub fn assert_valid(&self) {
        // Check that timestamps are in ascending order
        let timestamps = &self.timestamps;
        for i in 1..timestamps.len() {
            if timestamps[i - 1] > timestamps[i] {
                panicf!(
                    "BUG: log entries must be sorted by timestamp; got the previous entry with bigger timestamp {} than the current entry with timestamp {}",
                    timestamps[i - 1],
                    timestamps[i]
                );
            }
        }

        // Check that the number of items in each column matches the number of items in the block.
        let items_count = timestamps.len();
        for c in &self.columns {
            if c.values.len() != items_count {
                panicf!(
                    "BUG: unexpected number of values for column {:?}: got {}; want {}",
                    c.name,
                    c.values.len(),
                    items_count
                );
            }
        }
    }

    /// Initializes b from the given timestamps and rows.
    ///
    /// It is expected that timestamps are sorted.
    ///
    /// PORT NOTE: Go's `b` is valid until rows are changed (the columns share
    /// the row strings); the port clones field names and values into owned
    /// Strings, so b stays valid independently.
    pub fn must_init_from_rows(&mut self, timestamps: &[i64], rows: &[Vec<Field>]) {
        self.reset();

        assert_timestamps_sorted(timestamps);
        self.must_init_from_rows_internal(timestamps, rows);
        self.sort_columns_by_name();
    }

    /// Initializes b from the given timestamps and rows.
    ///
    /// PORT NOTE: Go has both the exported `MustInitFromRows` and the
    /// unexported `mustInitFromRows`; the latter is suffixed `_internal`.
    fn must_init_from_rows_internal(&mut self, timestamps: &[i64], rows: &[Vec<Field>]) {
        if timestamps.len() != rows.len() {
            panicf!(
                "BUG: len of timestamps {} and rows {} must be equal",
                timestamps.len(),
                rows.len()
            );
        }

        let rows_len = rows.len();
        if rows_len == 0 {
            // Nothing to do
            return;
        }

        if are_same_fields_in_rows(rows) {
            // Fast path - all the log entries have the same fields
            self.timestamps.extend_from_slice(timestamps);
            let fields = &rows[0];
            for (i, f) in fields.iter().enumerate() {
                if can_store_in_const_column(rows, i) {
                    let cc = self.extend_const_columns();
                    cc.name.clone_from(&f.name);
                    cc.value.clone_from(&f.value);
                } else {
                    let c = self.extend_columns();
                    c.name.clone_from(&f.name);
                    let values = c.resize_values(rows_len);
                    for (j, row) in rows.iter().enumerate() {
                        values[j].clone_from(&row[i].value);
                    }
                }
            }
            return;
        }

        // Slow path - log entries contain different set of fields

        // Determine indexes for columns
        //
        // PORT NOTE: Go pools the `map[string]int` via columnIdxsPool; the
        // port uses a local HashMap with keys borrowed from rows, which
        // cannot outlive this call, so the pool is dropped.
        let mut column_idxs: HashMap<&[u8], usize> = HashMap::new();
        let mut i = 0;
        while i < rows.len() {
            let fields = &rows[i];
            if column_idxs.len() + fields.len() > MAX_COLUMNS_PER_BLOCK {
                // User tries writing too many unique field names into a single log stream.
                // It is better ignoring rows with too many field names instead of trying to store them,
                // since the storage isn't designed to work with too big number of unique field names
                // per log stream - this leads to excess usage of RAM, CPU, disk IO and disk space.
                // It is better emitting a warning, so the user is aware of the problem and fixes it ASAP.
                // Log text only: raw name bytes are rendered via a lossy view.
                let field_names: Vec<String> = column_idxs
                    .keys()
                    .map(|name| String::from_utf8_lossy(name).into_owned())
                    .collect();
                warnf!(
                    "ignoring {} rows in the block, because they contain more than {} unique field names: [{}]",
                    rows.len() - i,
                    MAX_COLUMNS_PER_BLOCK,
                    field_names.join(" ")
                );
                break;
            }
            for f in fields {
                if !column_idxs.contains_key(f.name.as_slice()) {
                    column_idxs.insert(&f.name, column_idxs.len());
                }
            }
            i += 1;
        }
        let rows_processed = i;

        // keep only rows that fit maxColumnsPerBlock limit
        let rows = &rows[..rows_processed];
        let timestamps = &timestamps[..rows_processed];
        if rows.is_empty() {
            return;
        }

        self.timestamps.extend_from_slice(timestamps);

        // Initialize columns
        let cs = self.resize_columns(column_idxs.len());
        for (name, &idx) in &column_idxs {
            let c = &mut cs[idx];
            c.name.clear();
            c.name.extend_from_slice(name);
            c.resize_values(rows.len());
        }

        // Write rows to block
        for (i, row) in rows.iter().enumerate() {
            for f in row {
                let idx = column_idxs[f.name.as_slice()];
                cs[idx].values[i].clone_from(&f.value);
            }
        }
        drop(column_idxs);

        // Detect const columns
        let mut cs_len = self.columns.len();
        let mut i = cs_len;
        while i > 0 {
            i -= 1;
            if !self.columns[i].can_store_in_const_column() {
                continue;
            }
            let c = &mut self.columns[i];
            let name = std::mem::take(&mut c.name);
            let value = std::mem::take(&mut c.values[0]);
            c.reset();
            self.const_columns.push(Field { name, value });

            if i < cs_len - 1 {
                self.columns.swap(i, cs_len - 1);
            }
            cs_len -= 1;
        }
        self.columns.truncate(cs_len);
    }

    fn extend_const_columns(&mut self) -> &mut Field {
        self.const_columns.push(Field::default());
        self.const_columns.last_mut().unwrap()
    }

    fn extend_columns(&mut self) -> &mut Column {
        self.columns.push(Column::default());
        self.columns.last_mut().unwrap()
    }

    fn resize_columns(&mut self, columns_len: usize) -> &mut [Column] {
        self.columns.clear();
        self.columns.resize_with(columns_len, Column::default);
        &mut self.columns
    }

    fn sort_columns_by_name(&mut self) {
        if self.columns.len() + self.const_columns.len() > MAX_COLUMNS_PER_BLOCK {
            let column_names = self.get_column_names();
            panicf!(
                "BUG: too big number of columns detected in the block: {}; the number of columns mustn't exceed {}; columns: [{}]",
                self.columns.len() + self.const_columns.len(),
                MAX_COLUMNS_PER_BLOCK,
                column_names.join(" ")
            );
        }

        // PORT NOTE: Go sorts via pooled columnsSorter/constColumnsSorter
        // adapters for sort.Sort; slice sorting needs no adapters in Rust, so
        // the sorter types and their pools are dropped.
        self.columns.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        self.const_columns
            .sort_unstable_by(|a, b| a.name.cmp(&b.name));
    }

    /// Panic/log text only: raw name bytes are rendered via a lossy view.
    fn get_column_names(&self) -> Vec<String> {
        let mut a = Vec::with_capacity(self.columns.len() + self.const_columns.len());
        for c in &self.columns {
            a.push(String::from_utf8_lossy(&c.name).into_owned());
        }
        for cc in &self.const_columns {
            a.push(String::from_utf8_lossy(&cc.name).into_owned());
        }
        a
    }

    /// Returns the number of log entries in b.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    /// Unmarshals bd to b.
    ///
    /// sbu and vd are used as a temporary storage for unmarshaled column values.
    ///
    /// PORT NOTE: Go's `b` becomes outdated after sbu or vd is reset, since
    /// column values are unsafe views into sbu/vd buffers; the port stores
    /// owned Strings, so b stays valid independently.
    pub fn init_from_block_data(
        &mut self,
        bd: &BlockData,
        sbu: &mut StringsBlockUnmarshaler,
        vd: &mut ValuesDecoder,
    ) -> Result<(), String> {
        self.reset();

        if bd.rows_count > MAX_ROWS_PER_BLOCK as u64 {
            return Err(format!(
                "too many entries found in the block: {}; mustn't exceed {}",
                bd.rows_count, MAX_ROWS_PER_BLOCK
            ));
        }
        let rows_count = bd.rows_count as usize;

        // unmarshal timestamps
        let td = &bd.timestamps_data;
        vc_encoding::unmarshal_timestamps(
            &mut self.timestamps,
            &td.data,
            td.marshal_type,
            td.min_timestamp,
            rows_count,
        )
        .map_err(|err| format!("cannot unmarshal timestamps: {err}"))?;

        // unmarshal columns
        let cds = &bd.columns_data;
        self.resize_columns(cds.len());
        for (i, cd) in cds.iter().enumerate() {
            let c = &mut self.columns[i];
            c.name = cd.name.clone();
            // Column values are raw bytes, so the decoded buffers are moved
            // straight into c.values without any UTF-8 validation/allocation
            // pass (Go strings are arbitrary bytes).
            c.values.clear();
            sbu.unmarshal(&mut c.values, &cd.values_data, rows_count as u64)
                .map_err(|err| format!("cannot unmarshal column {i}: {err}"))?;
            vd.decode_inplace(&mut c.values, cd.value_type, &cd.values_dict.values)
                .map_err(|err| format!("cannot decode column values: {err}"))?;
        }

        // unmarshal constColumns
        //
        // PORT NOTE: Go uses sbu.appendFields to copy the fields into
        // sbu-owned memory; owned-String Fields make the plain append_fields
        // helper equivalent.
        self.const_columns.clear();
        append_fields(&mut self.const_columns, &bd.const_columns);

        Ok(())
    }

    /// Writes b with the given sid to sw and updates bh accordingly.
    pub fn must_write_to(&self, sid: &StreamID, bh: &mut BlockHeader, sw: &mut StreamWriters<'_>) {
        self.assert_valid();
        bh.reset();

        bh.stream_id = *sid;
        bh.uncompressed_size_bytes = self.uncompressed_size_bytes() as u64;
        bh.rows_count = self.len() as u64;

        // Marshal timestamps
        must_write_timestamps_to(&mut bh.timestamps_header, &self.timestamps, sw);

        // Marshal columns

        let mut csh = get_columns_header();

        let cs = &self.columns;
        csh.resize_column_headers(cs.len());
        for (i, c) in cs.iter().enumerate() {
            c.must_write_to(&mut csh.column_headers[i], sw);
        }

        csh.const_columns.clear();
        csh.const_columns.extend_from_slice(&self.const_columns);

        // PORT NOTE: Go's csh.mustWriteTo(bh, sw) is ported as the free
        // must_write_columns_header() in block_stream_writer.rs.
        must_write_columns_header(&csh, bh, sw);

        put_columns_header(csh);
    }

    /// Appends log entries from b to dst.
    ///
    /// PORT NOTE: Go pre-allocates dst.fieldsBuf for all the fields across
    /// rows and slices dst.rows out of it; the port's Rows stores each row as
    /// its own Vec<Field>, so rows are built directly.
    pub fn append_rows_to(&self, dst: &mut Rows) {
        // copy timestamps
        dst.timestamps.extend_from_slice(&self.timestamps);

        // copy columns
        let ccs = &self.const_columns;
        let cs = &self.columns;

        dst.rows.reserve(self.timestamps.len());
        for i in 0..self.timestamps.len() {
            let mut fields: Vec<Field> = Vec::with_capacity(ccs.len() + cs.len());
            // copy const columns
            fields.extend_from_slice(ccs);
            // copy other columns
            for c in cs {
                let value = &c.values[i];
                if value.is_empty() {
                    continue;
                }
                fields.push(Field {
                    name: c.name.clone(),
                    value: value.clone(),
                });
            }
            dst.rows.push(fields);
        }
    }
}

/// Returns the size of the uncompressed rows.
///
/// It is assumed that each row is in JSON format.
pub fn uncompressed_rows_size_bytes(rows: &[Vec<Field>]) -> u64 {
    let mut n = 0u64;
    for fields in rows {
        n += estimated_json_row_len(fields) as u64;
    }
    n
}

/// column contains values for the given field name seen in log entries.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Column {
    /// name is the field name
    ///
    /// PORT NOTE: raw bytes (Go strings are arbitrary bytes).
    pub name: Vec<u8>,

    /// values is the values seen for the given log entries.
    ///
    /// PORT NOTE: values are raw bytes (Go strings are arbitrary bytes).
    pub values: Vec<Vec<u8>>,
}

impl Column {
    pub fn reset(&mut self) {
        self.name.clear();
        self.values.clear();
    }

    fn can_store_in_const_column(&self) -> bool {
        let values = &self.values;
        if values.is_empty() {
            return true;
        }
        let value = &values[0];
        if value.len() > MAX_CONST_COLUMN_VALUE_SIZE {
            return false;
        }
        for v in &values[1..] {
            if value != v {
                return false;
            }
        }
        true
    }

    fn resize_values(&mut self, values_len: usize) -> &mut [Vec<u8>] {
        self.values.clear();
        self.values.resize_with(values_len, Vec::new);
        &mut self.values
    }

    /// Writes c to sw and updates ch accordingly.
    ///
    /// PORT NOTE: Go's ch is valid until c is changed (ch.name shares
    /// c.name); the port clones the name into ch.
    pub fn must_write_to(&self, ch: &mut ColumnHeader, sw: &mut StreamWriters<'_>) {
        ch.reset();

        ch.name.clone_from(&self.name);

        let bloom_values_writer = sw.get_bloom_values_writer_for_column_name(&ch.name);

        // encode values
        let mut ve = get_values_encoder();
        let (value_type, min_value, max_value) = ve.encode(&self.values, &mut ch.values_dict);
        ch.value_type = value_type;
        ch.min_value = min_value;
        ch.max_value = max_value;

        let mut bb = LONG_TERM_BUF_POOL.get();

        // marshal values
        //
        // PORT NOTE: ve.values() yields borrowed slices; they are collected
        // into a Vec for marshal_strings_block, which takes a slice.
        let encoded_values: Vec<&[u8]> = ve.values().collect();
        marshal_strings_block(&mut bb.b, &encoded_values);
        drop(encoded_values);
        put_values_encoder(ve);
        ch.values_size = bb.b.len() as u64;
        if ch.values_size > MAX_VALUES_BLOCK_SIZE as u64 {
            panicf!(
                "BUG: too big valuesSize: {} bytes; mustn't exceed {} bytes",
                ch.values_size,
                MAX_VALUES_BLOCK_SIZE
            );
        }
        ch.values_offset = bloom_values_writer.values.bytes_written;
        bloom_values_writer.values.must_write(&bb.b);

        // create and marshal bloom filter for c.values
        if ch.value_type != ValueType::DICT {
            let mut hashes_buf = vc_encoding::get_uint64s(0);
            hashes_buf.a.clear();
            tokenize_hashes(&mut hashes_buf.a, &self.values);
            bb.b.clear();
            bloom_filter_marshal_hashes(&mut bb.b, &hashes_buf.a);
            vc_encoding::put_uint64s(hashes_buf);
        } else {
            // there is no need in encoding bloom filter for dictionary type,
            // since it isn't used during querying - all the dictionary values are available in ch.valuesDict
            bb.b.clear();
        }
        ch.bloom_filter_size = bb.b.len() as u64;
        if ch.bloom_filter_size > MAX_BLOOM_FILTER_BLOCK_SIZE as u64 {
            panicf!(
                "BUG: too big bloomFilterSize: {} bytes; mustn't exceed {} bytes",
                ch.bloom_filter_size,
                MAX_BLOOM_FILTER_BLOCK_SIZE
            );
        }
        ch.bloom_filter_offset = bloom_values_writer.bloom.bytes_written;
        bloom_values_writer.bloom.must_write(&bb.b);

        LONG_TERM_BUF_POOL.put(bb);
    }
}

fn can_store_in_const_column(rows: &[Vec<Field>], col_idx: usize) -> bool {
    if rows.is_empty() {
        return true;
    }
    let value = &rows[0][col_idx].value;
    if value.len() > MAX_CONST_COLUMN_VALUE_SIZE {
        return false;
    }
    for row in &rows[1..] {
        if *value != row[col_idx].value {
            return false;
        }
    }
    true
}

fn assert_timestamps_sorted(timestamps: &[i64]) {
    for i in 0..timestamps.len() {
        if i > 0 && timestamps[i - 1] > timestamps[i] {
            panicf!(
                "BUG: log entries must be sorted by timestamp; got the previous entry with bigger timestamp {} than the current entry with timestamp {}",
                timestamps[i - 1],
                timestamps[i]
            );
        }
    }
}

fn are_same_fields_in_rows(rows: &[Vec<Field>]) -> bool {
    if rows.len() < 2 {
        return true;
    }
    let fields = &rows[0];

    // Verify that all the field names are unique
    //
    // PORT NOTE: Go pools the fields set map via fieldsSetPool; the port uses
    // a local set with keys borrowed from rows, which cannot outlive this
    // call, so the pool is dropped.
    let mut m: HashSet<&[u8]> = HashSet::new();
    for f in fields {
        if !m.insert(f.name.as_slice()) {
            // Field name isn't unique
            return false;
        }
    }
    drop(m);

    // Verify that all the fields are the same across rows
    for le_fields in &rows[1..] {
        if fields.len() != le_fields.len() {
            return false;
        }
        for (j, le_field) in le_fields.iter().enumerate() {
            if le_field.name != fields[j].name {
                return false;
            }
        }
    }
    true
}

/// Obtains a block from the pool.
pub fn get_block() -> Block {
    BLOCK_POOL.lock().unwrap().pop().unwrap_or_default()
}

/// Returns b to the pool.
pub fn put_block(mut b: Block) {
    b.reset();
    BLOCK_POOL.lock().unwrap().push(b);
}

// PORT NOTE: Go uses `sync.Pool` with `*block`; the port uses a
// `Mutex<Vec<Block>>` pool handing blocks out by value, preserving the buffer
// reuse pattern.
static BLOCK_POOL: Mutex<Vec<Block>> = Mutex::new(Vec::new());

/// Writes timestamps to sw and updates th accordingly
pub fn must_write_timestamps_to(
    th: &mut TimestampsHeader,
    timestamps: &[i64],
    sw: &mut StreamWriters<'_>,
) {
    th.reset();

    let mut bb = LONG_TERM_BUF_POOL.get();
    let (marshal_type, min_timestamp) = vc_encoding::marshal_timestamps(&mut bb.b, timestamps, 64);
    th.marshal_type = marshal_type;
    th.min_timestamp = min_timestamp;
    if bb.b.len() > MAX_TIMESTAMPS_BLOCK_SIZE {
        panicf!(
            "BUG: too big block with timestamps: {} bytes; the maximum supported size is {} bytes",
            bb.b.len(),
            MAX_TIMESTAMPS_BLOCK_SIZE
        );
    }
    th.max_timestamp = timestamps[timestamps.len() - 1];
    th.block_offset = sw.timestamps_writer.bytes_written;
    th.block_size = bb.b.len() as u64;
    sw.timestamps_writer.must_write(&bb.b);
    LONG_TERM_BUF_POOL.put(bb);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::MAX_UNCOMPRESSED_BLOCK_SIZE;
    use std::collections::BTreeMap;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_block_must_init_from_rows() {
        fn f(timestamps: &[i64], rows: &[Vec<Field>], b_expected: &Block) {
            let mut b = Block::default();
            b.must_init_from_rows(timestamps, rows);
            assert!(
                b.uncompressed_size_bytes() < MAX_UNCOMPRESSED_BLOCK_SIZE,
                "expecting non-full block"
            );
            assert_eq!(
                &b, b_expected,
                "unexpected block;\ngot\n{b:?}\nwant\n{b_expected:?}"
            );
            let n = b.len();
            assert_eq!(
                n,
                timestamps.len(),
                "unexpected block len; got {n}; want {}",
                timestamps.len()
            );
            b.assert_valid();
        }

        // An empty log entries
        f(&[], &[], &Block::default());
        f(&[], &[], &Block::default());

        // A single row
        let timestamps: &[i64] = &[1234];
        let rows = vec![vec![field("msg", "foo"), field("level", "error")]];
        let b_expected = Block {
            timestamps: vec![1234],
            columns: vec![],
            const_columns: vec![field("level", "error"), field("msg", "foo")],
        };
        f(timestamps, &rows, &b_expected);

        // Multiple log entries with the same set of fields
        let timestamps: &[i64] = &[3, 5];
        let rows = vec![
            vec![field("job", "foo"), field("instance", "host1")],
            vec![field("job", "foo"), field("instance", "host2")],
        ];
        let b_expected = Block {
            timestamps: vec![3, 5],
            columns: vec![Column {
                name: b"instance".to_vec(),
                values: vec![b"host1".to_vec(), b"host2".to_vec()],
            }],
            const_columns: vec![field("job", "foo")],
        };
        f(timestamps, &rows, &b_expected);

        // Multiple log entries with distinct set of fields
        let timestamps: &[i64] = &[3, 5, 10];
        let rows = vec![
            vec![field("msg", "foo"), field("b", "xyz")],
            vec![field("b", "xyz"), field("a", "aaa")],
            vec![field("b", "xyz")],
        ];
        let b_expected = Block {
            timestamps: vec![3, 5, 10],
            columns: vec![
                Column {
                    name: b"a".to_vec(),
                    values: vec![b"".to_vec(), b"aaa".to_vec(), b"".to_vec()],
                },
                Column {
                    name: b"msg".to_vec(),
                    values: vec![b"foo".to_vec(), b"".to_vec(), b"".to_vec()],
                },
            ],
            const_columns: vec![field("b", "xyz")],
        };
        f(timestamps, &rows, &b_expected);
    }

    #[test]
    fn test_block_must_init_from_rows_full_block() {
        const ROWS_COUNT: usize = 2000;
        let timestamps = vec![0i64; ROWS_COUNT];
        let mut rows = Vec::with_capacity(ROWS_COUNT);
        for _ in 0..ROWS_COUNT {
            let mut fields = Vec::with_capacity(10);
            for j in 0..10 {
                fields.push(field(
                    &format!("field_{j}"),
                    "very very looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong value",
                ));
            }
            rows.push(fields);
        }

        let mut b = get_block();
        b.must_init_from_rows(&timestamps, &rows);
        b.assert_valid();
        let n = b.len();
        assert_eq!(
            n,
            rows.len(),
            "unexpected total log entries; got {n}; want {}",
            rows.len()
        );
        let n = b.uncompressed_size_bytes();
        assert!(
            n >= MAX_UNCOMPRESSED_BLOCK_SIZE,
            "expecting full block with {MAX_UNCOMPRESSED_BLOCK_SIZE} bytes; got {n} bytes"
        );
        put_block(b);
    }

    #[test]
    fn test_block_must_init_from_rows_overflow() {
        fn f(rows_count: usize, fields_per_row: usize, expected_rows_processed: usize) {
            let timestamps = vec![0i64; rows_count];
            let mut rows = Vec::with_capacity(rows_count);
            for i in 0..rows_count {
                let mut fields = Vec::with_capacity(fields_per_row);
                for j in 0..fields_per_row {
                    fields.push(field(
                        &format!("field_{i}_{j}"),
                        "very very looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong value",
                    ));
                }
                rows.push(fields);
            }
            let mut b = get_block();
            b.must_init_from_rows(&timestamps, &rows);
            b.assert_valid();
            let n = b.len();
            assert_eq!(
                n, expected_rows_processed,
                "unexpected total log entries; got {n}; want {expected_rows_processed}"
            );
            put_block(b);
        }
        f(10, 300, 6);
        f(10, 10, 10);
        f(15, 30, 15);
        f(MAX_COLUMNS_PER_BLOCK + 1000, 1, MAX_COLUMNS_PER_BLOCK);
    }

    /// Returns the size of the Go `encoding/json` marshaling of m.
    ///
    /// PORT NOTE: the Go test marshals a map[string]string with
    /// encoding/json; the sizes match this direct computation, since none of
    /// the test strings need JSON escaping.
    fn json_object_size(m: &BTreeMap<String, String>) -> usize {
        let mut n = "{}".len();
        if !m.is_empty() {
            n += m.len() - 1; // commas
            for (k, v) in m {
                n += r#""":"""#.len() + k.len() + v.len();
            }
        }
        n
    }

    const TIME_RFC3339_NANO: &str = "2006-01-02T15:04:05.999999999Z07:00";

    #[test]
    fn test_block_uncompressed_size_bytes() {
        fn f(rows: &[Vec<Field>]) {
            // Build expected JSON and calculate actual serialized size
            let mut total_size = 0;
            for fields in rows {
                let mut m: BTreeMap<String, String> = BTreeMap::new();
                m.insert("_time".to_string(), TIME_RFC3339_NANO.to_string());

                for f in fields {
                    if f.value.is_empty() {
                        continue; // skip empty values
                    }
                    let key = get_canonical_column_name_bytes(&f.name);
                    m.insert(
                        String::from_utf8(key.to_vec()).unwrap(),
                        String::from_utf8(f.value.clone()).unwrap(),
                    );
                }

                total_size += json_object_size(&m) + 1; // +1 for newline if expected
            }

            let mut b = Block::default();
            let timestamps = vec![0i64; rows.len()]; // values don't matter for size estimation
            b.must_init_from_rows(&timestamps, rows);

            let actual_size = b.uncompressed_size_bytes();
            assert_eq!(
                actual_size, total_size,
                "unexpected uncompressed size;\n got  {actual_size}\n want {total_size}, testcase: {rows:?}"
            );
        }

        // Empty block
        f(&[]);

        // Single row with one field
        f(&[vec![field("msg", "hello")]]);

        // Multiple rows with constant columns
        f(&[vec![field("level", "info")], vec![field("level", "info")]]);

        // Multiple rows with variable columns
        f(&[vec![field("msg", "first")], vec![field("msg", "second")]]);

        // Mixed constant and variable columns
        f(&[
            vec![field("service", "api"), field("msg", "start")],
            vec![field("service", "api"), field("msg", "end")],
        ]);

        // Empty values ignored
        f(&[
            vec![field("msg", "hello"), field("empty", "")],
            vec![field("msg", ""), field("empty", "world")],
        ]);
    }

    // test_estimated_json_row_len_matches_block_uncompressed_size_bytes verifies that
    // estimated_json_row_len and Block::uncompressed_size_bytes stay in sync.
    // If this test fails, update the calculations in both functions so that they
    // produce identical results for the same set of log entries.
    #[test]
    fn test_estimated_json_row_len_matches_block_uncompressed_size_bytes() {
        fn f(rows: &[Vec<Field>]) {
            let mut b = Block::default();
            let timestamps = vec![0i64; rows.len()];
            b.must_init_from_rows(&timestamps, rows);

            let size_block = b.uncompressed_size_bytes();
            let mut size_rows = 0;
            for fields in rows {
                size_rows += estimated_json_row_len(fields);
            }

            assert_eq!(
                size_block, size_rows,
                "sizes mismatch: block={size_block} rows={size_rows} for rows: {rows:?}"
            );
        }

        // Test cases
        f(&[]);

        // Single row with one field
        f(&[vec![field("msg", "hello")]]);

        // Multiple rows with constant column
        f(&[vec![field("level", "info")], vec![field("level", "info")]]);

        // Multiple rows with variable columns
        f(&[vec![field("msg", "first")], vec![field("msg", "second")]]);

        // Mixed constant and variable columns
        f(&[
            vec![field("service", "api"), field("msg", "start")],
            vec![field("service", "api"), field("msg", "end")],
        ]);

        // Empty values ignored
        f(&[
            vec![field("msg", "hello"), field("empty", "")],
            vec![field("msg", ""), field("empty", "world")],
        ]);
    }
}

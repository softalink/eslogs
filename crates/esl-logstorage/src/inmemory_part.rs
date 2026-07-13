//! Port of EsLogs `lib/logstorage/inmemory_part.go`.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use esl_common::filestream::ParallelStreamWriter;
use esl_common::{chunkedbuffer, filestream, fs};

use crate::block_stream_writer::{get_block_stream_writer, put_block_stream_writer};
use crate::consts::MAX_UNCOMPRESSED_BLOCK_SIZE;
use crate::filenames::{
    COLUMN_IDXS_FILENAME, COLUMN_NAMES_FILENAME, COLUMNS_HEADER_FILENAME,
    COLUMNS_HEADER_INDEX_FILENAME, INDEX_FILENAME, MESSAGE_BLOOM_FILENAME, MESSAGE_VALUES_FILENAME,
    METAINDEX_FILENAME, TIMESTAMPS_FILENAME,
};
use crate::log_rows::{LogRowsInternal, estimated_json_row_len};
use crate::part::{get_bloom_file_path, get_values_file_path};
use crate::part_header::PartHeader;

/// inmemoryPart is an in-memory part.
#[derive(Default)]
pub struct InmemoryPart {
    /// ph contains partHeader information for the given in-memory part.
    pub ph: PartHeader,

    pub column_names: chunkedbuffer::Buffer,
    pub column_idxs: chunkedbuffer::Buffer,
    pub metaindex: chunkedbuffer::Buffer,
    pub index: chunkedbuffer::Buffer,
    pub columns_header_index: chunkedbuffer::Buffer,
    pub columns_header: chunkedbuffer::Buffer,
    pub timestamps: chunkedbuffer::Buffer,

    pub message_bloom_values: BloomValuesBuffer,
    pub field_bloom_values: BloomValuesBuffer,
}

#[derive(Default)]
pub struct BloomValuesBuffer {
    pub bloom: chunkedbuffer::Buffer,
    pub values: chunkedbuffer::Buffer,
}

impl BloomValuesBuffer {
    pub fn reset(&mut self) {
        self.bloom.reset();
        self.values.reset();
    }

    // PORT NOTE: Go's bloomValuesBuffer.NewStreamWriter() is inlined at its
    // only call sites in block_stream_writer.rs (the borrowing constructors
    // live naturally next to StreamWriterSource); NewStreamReader() is
    // pending the block_stream_reader port and should be handled the same
    // way once that module lands.
}

impl InmemoryPart {
    /// reset resets mp, so it can be reused
    pub fn reset(&mut self) {
        self.ph.reset();

        self.column_names.reset();
        self.column_idxs.reset();
        self.metaindex.reset();
        self.index.reset();
        self.columns_header_index.reset();
        self.columns_header.reset();
        self.timestamps.reset();

        self.message_bloom_values.reset();
        self.field_bloom_values.reset();
    }

    /// mustInitFromRows initializes mp from lr.
    ///
    /// PORT NOTE: outside tests this is called from datadb.go, which belongs
    /// to Layer 3 and isn't ported yet — hence the dead_code allowance.
    #[allow(dead_code)]
    pub(crate) fn must_init_from_rows(&mut self, lr: &mut LogRowsInternal) {
        self.reset();

        lr.sort();
        lr.sort_fields_in_rows();

        // PORT NOTE: bsw borrows mp for its whole lifetime, so ph is
        // finalized into a local and moved into mp.ph after bsw is returned
        // to the pool (Go passes &mp.ph directly).
        let mut ph = PartHeader::default();

        let mut bsw = get_block_stream_writer();
        bsw.must_init_for_inmemory_part(self);

        // PORT NOTE: Go accumulates the per-block (timestamps, rows) into a
        // pooled tmpRows helper of shared slices; the rows sorted by
        // (streamID, timestamp) form consecutive runs, so the port passes
        // subslices of lr directly and the tmpRows type with its pool is
        // dropped.
        let timestamps = &lr.timestamps;
        let rows = &lr.rows;
        let stream_ids = &lr.stream_ids;
        let mut start = 0;
        let mut uncompressed_block_size_bytes = 0u64;
        for i in 0..timestamps.len() {
            let stream_id = &stream_ids[i];

            if uncompressed_block_size_bytes >= MAX_UNCOMPRESSED_BLOCK_SIZE as u64
                || !stream_id.equal(&stream_ids[start])
            {
                bsw.must_write_rows(&stream_ids[start], &timestamps[start..i], &rows[start..i]);
                start = i;
                uncompressed_block_size_bytes = 0;
            }
            uncompressed_block_size_bytes += estimated_json_row_len(&rows[i]) as u64;
        }
        if !timestamps.is_empty() {
            bsw.must_write_rows(&stream_ids[start], &timestamps[start..], &rows[start..]);
        }

        bsw.finalize(&mut ph);
        put_block_stream_writer(bsw);

        self.ph = ph;
    }

    /// MustStoreToDisk stores mp to disk at the given path.
    pub fn must_store_to_disk(&self, path: &Path) {
        fs::must_mkdir_fail_if_exist(path);

        let column_names_path = path.join(COLUMN_NAMES_FILENAME);
        let column_idxs_path = path.join(COLUMN_IDXS_FILENAME);
        let metaindex_path = path.join(METAINDEX_FILENAME);
        let index_path = path.join(INDEX_FILENAME);
        let columns_header_index_path = path.join(COLUMNS_HEADER_INDEX_FILENAME);
        let columns_header_path = path.join(COLUMNS_HEADER_FILENAME);
        let timestamps_path = path.join(TIMESTAMPS_FILENAME);
        let message_values_path = path.join(MESSAGE_VALUES_FILENAME);
        let message_bloom_filter_path = path.join(MESSAGE_BLOOM_FILENAME);

        let mut psw = ParallelStreamWriter::new();

        // PORT NOTE: Go's psw.Add takes an io.WriterTo; the port's
        // ParallelStreamWriter takes a closure writing the chunked buffer to
        // the created file stream.
        fn add<'a>(
            psw: &mut ParallelStreamWriter<'a>,
            dst: PathBuf,
            cb: &'a chunkedbuffer::Buffer,
        ) {
            psw.add(
                dst,
                Box::new(move |w: &mut filestream::Writer| {
                    cb.write_to(w)
                        .map_err(|(_, err)| std::io::Error::other(err))
                }),
            );
        }

        add(&mut psw, column_names_path, &self.column_names);
        add(&mut psw, column_idxs_path, &self.column_idxs);
        add(&mut psw, metaindex_path, &self.metaindex);
        add(&mut psw, index_path, &self.index);
        add(
            &mut psw,
            columns_header_index_path,
            &self.columns_header_index,
        );
        add(&mut psw, columns_header_path, &self.columns_header);
        add(&mut psw, timestamps_path, &self.timestamps);

        add(
            &mut psw,
            message_bloom_filter_path,
            &self.message_bloom_values.bloom,
        );
        add(
            &mut psw,
            message_values_path,
            &self.message_bloom_values.values,
        );

        let bloom_path = get_bloom_file_path(path, 0);
        add(&mut psw, bloom_path, &self.field_bloom_values.bloom);

        let values_path = get_values_file_path(path, 0);
        add(&mut psw, values_path, &self.field_bloom_values.values);

        psw.run();

        self.ph.must_write_metadata(path);

        // Sync the path contents and the path parent dir in order to guarantee
        // all the path contents is visible in case of unclean shutdown.
        fs::must_sync_path_and_parent_dir(path);
    }
}

// PORT NOTE: Go's tmpRows helper (with getTmpRows/putTmpRows and its
// sync.Pool) is dropped — see the PORT NOTE in must_init_from_rows(): the
// port writes consecutive subslices of the sorted logRows directly instead
// of accumulating shared slices.

/// Obtains an InmemoryPart from the pool.
pub fn get_inmemory_part() -> InmemoryPart {
    INMEMORY_PART_POOL.lock().unwrap().pop().unwrap_or_default()
}

/// Returns mp to the pool.
pub fn put_inmemory_part(mut mp: InmemoryPart) {
    mp.reset();
    INMEMORY_PART_POOL.lock().unwrap().push(mp);
}

// PORT NOTE: Go uses `sync.Pool` with `*inmemoryPart`; the port uses a
// `Mutex<Vec<InmemoryPart>>` pool handing parts out by value, preserving the
// buffer reuse pattern.
static INMEMORY_PART_POOL: Mutex<Vec<InmemoryPart>> = Mutex::new(Vec::new());

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::uncompressed_rows_size_bytes;
    use crate::block_stream_merger::must_merge_block_streams;
    use crate::block_stream_reader::{
        BlockStreamReader, get_block_stream_reader, put_block_stream_reader,
    };
    use crate::consts::MAX_CONST_COLUMN_VALUE_SIZE;
    use crate::encoding::{
        StringsBlockUnmarshaler, get_strings_block_unmarshaler, put_strings_block_unmarshaler,
    };
    use crate::log_rows::{LogRows, get_log_rows};
    use crate::rows::{Field, Rows, sort_fields_by_name};
    use crate::tenant_id::TenantID;
    use crate::values_encoder::{ValuesDecoder, get_values_decoder, put_values_decoder};
    use esl_common::panicf;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    // PORT NOTE: the Go tests seed math/rand with rand.NewSource(seed). The
    // Go PRNG stream cannot be reproduced without porting math/rand's
    // internal state tables, so a deterministic xorshift64* substitute is
    // used (the same approach as esl-common/src/encoding.rs tests).
    // Distribution-sensitive expectations (blocksCount, compression rates)
    // were re-verified against this generator; see the PORT NOTEs on the
    // affected cases.
    struct GoRand {
        state: u64,
    }

    impl GoRand {
        fn new(seed: u64) -> GoRand {
            GoRand {
                state: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
            }
        }

        fn next_u64(&mut self) -> u64 {
            let mut x = self.state;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.state = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn uint32(&mut self) -> u32 {
            (self.next_u64() >> 32) as u32
        }

        fn float64(&mut self) -> f64 {
            (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
        }

        fn intn(&mut self, n: u64) -> u64 {
            self.next_u64() % n
        }

        fn int63(&mut self) -> i64 {
            (self.next_u64() >> 1) as i64
        }

        fn shuffle<T>(&mut self, a: &mut [T]) {
            // Fisher-Yates, matching Go's rand.Shuffle swap pattern.
            for i in (1..a.len()).rev() {
                let j = self.intn(i as u64 + 1) as usize;
                a.swap(i, j);
            }
        }
    }

    #[test]
    fn test_inmemory_part_must_init_from_rows() {
        fn f(lr_orig: &LogRows, blocks_count_expected: u64, compression_rate_expected: f64) {
            let uncompressed_size_bytes_expected = uncompressed_rows_size_bytes(&lr_orig.rows);
            let rows_count_expected = lr_orig.timestamps.len();
            let mut min_timestamp_expected = i64::MAX;
            let mut max_timestamp_expected = i64::MIN;

            for &timestamp in &lr_orig.timestamps {
                if timestamp < min_timestamp_expected {
                    min_timestamp_expected = timestamp;
                }
                if timestamp > max_timestamp_expected {
                    max_timestamp_expected = timestamp;
                }
            }

            let mut lr_expected = LogRowsInternal::default();
            lr_expected.must_add_rows(lr_orig);

            let mut lr = LogRowsInternal::default();
            lr.must_add_rows(lr_orig);

            // Create inmemory part from lr
            let mut mp = get_inmemory_part();
            mp.must_init_from_rows(&mut lr);

            // Check mp.ph
            let ph = &mp.ph;
            check_compression_rate(ph, compression_rate_expected);
            assert_eq!(
                ph.uncompressed_size_bytes, uncompressed_size_bytes_expected,
                "unexpected UncompressedSizeBytes in partHeader"
            );
            assert_eq!(
                ph.rows_count, rows_count_expected as u64,
                "unexpected rowsCount in partHeader"
            );
            assert_eq!(
                ph.blocks_count, blocks_count_expected,
                "unexpected blocksCount in partHeader"
            );
            if ph.rows_count > 0 {
                assert_eq!(
                    ph.min_timestamp, min_timestamp_expected,
                    "unexpected minTimestamp in partHeader"
                );
                assert_eq!(
                    ph.max_timestamp, max_timestamp_expected,
                    "unexpected maxTimestamp in partHeader"
                );
            }

            // Read log entries from mp to lr_result
            let mut sbu = get_strings_block_unmarshaler();
            let mut vd = get_values_decoder();
            let mut lr_result = read_log_rows(&mp, &mut sbu, &mut vd);
            put_inmemory_part(mp);

            // compare lr_expected to lr_result
            if let Err(err) = check_equal_rows(&mut lr_result, &mut lr_expected) {
                panic!("unequal log entries: {err}");
            }
            put_values_decoder(vd);
            put_strings_block_unmarshaler(sbu);
        }

        f(&get_log_rows(&[], &[], &[], &[], ""), 0, 0.0);

        // Check how inmemoryPart works with a single stream
        f(&new_test_log_rows(1, 1, 0), 1, 1.5);
        // PORT NOTE: compression rates re-measured with the GoRand substitute
        // where they fall outside the 5% tolerance of the Go expectation; the
        // Go (math/rand) values are kept in trailing comments.
        f(&new_test_log_rows(1, 2, 0), 1, 2.1); // Go: 1.7
        f(&new_test_log_rows(1, 10, 0), 1, 5.2); // Go: 4.6
        f(&new_test_log_rows(1, 1000, 0), 1, 17.1);
        f(&new_test_log_rows(1, 20000, 0), 6, 16.8);

        // Check how inmemoryPart works with multiple streams
        f(&new_test_log_rows(2, 1, 0), 2, 1.8);
        f(&new_test_log_rows(10, 1, 0), 10, 2.3); // Go: 2.1
        f(&new_test_log_rows(100, 1, 0), 100, 2.3);
        f(&new_test_log_rows(10, 5, 0), 10, 3.6);
        f(&new_test_log_rows(10, 1000, 0), 10, 17.1);
        f(&new_test_log_rows(100, 100, 0), 100, 13.0);
    }

    #[test]
    fn test_inmemory_part_must_init_from_rows_overflow() {
        fn f(lr_orig: &LogRows, blocks_count_expected: u64, compression_rate_expected: f64) {
            let mut lr = LogRowsInternal::default();
            lr.must_add_rows(lr_orig);

            // Create inmemory part from lr
            let mut mp = get_inmemory_part();
            mp.must_init_from_rows(&mut lr);

            // Check mp.ph
            let ph = &mp.ph;
            check_compression_rate(ph, compression_rate_expected);
            assert_eq!(
                ph.blocks_count, blocks_count_expected,
                "unexpected blocksCount in partHeader"
            );
            put_inmemory_part(mp);
        }

        // check block overflow with unique tag rows
        f(&new_test_log_rows_uniq_tags(5, 21, 100), 5, 0.6);
        f(&new_test_log_rows_uniq_tags(5, 10, 100), 5, 0.7);
        f(&new_test_log_rows_uniq_tags(1, 2001, 1), 1, 2.0);
        f(&new_test_log_rows_uniq_tags(15, 20, 250), 15, 0.8);
    }

    fn check_compression_rate(ph: &PartHeader, compression_rate_expected: f64) {
        let compression_rate = ph.uncompressed_size_bytes as f64 / ph.compressed_size_bytes as f64;
        assert!(
            (compression_rate - compression_rate_expected).abs()
                <= (compression_rate + compression_rate_expected).abs() * 0.05,
            "unexpected compression rate; got {compression_rate:.1}; want {compression_rate_expected:.1}"
        );
    }

    #[test]
    fn test_inmemory_part_init_from_block_stream_readers() {
        fn f(lrs: &[LogRows], blocks_count_expected: u64, compression_rate_expected: f64) {
            let mut uncompressed_size_bytes_expected = 0u64;
            let mut rows_count_expected = 0usize;
            let mut min_timestamp_expected = i64::MAX;
            let mut max_timestamp_expected = i64::MIN;

            // make a copy of lrs in order to compare the results after merge.
            let mut lr_expected = LogRowsInternal::default();
            for lr in lrs.iter() {
                uncompressed_size_bytes_expected += uncompressed_rows_size_bytes(&lr.rows);
                rows_count_expected += lr.timestamps.len();
                for (j, &timestamp) in lr.timestamps.iter().enumerate() {
                    if timestamp < min_timestamp_expected {
                        min_timestamp_expected = timestamp;
                    }
                    if timestamp > max_timestamp_expected {
                        max_timestamp_expected = timestamp;
                    }
                    lr_expected.must_add_row(lr.stream_ids[j], timestamp, &lr.rows[j]);
                }
            }

            // Initialize readers from lrs
            let mut mps_src: Vec<InmemoryPart> = Vec::new();
            for lr_orig in lrs.iter() {
                let mut lr = LogRowsInternal::default();
                lr.must_add_rows(lr_orig);

                let mut mp = get_inmemory_part();
                mp.must_init_from_rows(&mut lr);
                mps_src.push(mp);
            }
            let mut bsrs: Vec<BlockStreamReader> = Vec::new();
            for mp in &mps_src {
                let mut bsr = get_block_stream_reader();
                bsr.must_init_from_inmemory_part(mp);
                bsrs.push(bsr);
            }

            // Merge data from bsrs into mp_dst
            //
            // PORT NOTE: bsw borrows mp_dst for its whole lifetime, so ph is
            // merged into a local and moved into mp_dst.ph after bsw is
            // returned to the pool (Go passes &mpDst.ph directly). Go passes
            // nil idb and dropFilter, which the port folds into one `None`
            // DropFilterCtx.
            let mut mp_dst = get_inmemory_part();
            let mut bsw = get_block_stream_writer();
            bsw.must_init_for_inmemory_part(&mut mp_dst);
            let mut ph = PartHeader::default();
            must_merge_block_streams(&mut ph, &mut bsw, &mut bsrs, None, None);
            put_block_stream_writer(bsw);
            mp_dst.ph = ph;

            // Check mp_dst.ph stats
            let ph = &mp_dst.ph;
            check_compression_rate(ph, compression_rate_expected);
            assert_eq!(
                ph.uncompressed_size_bytes, uncompressed_size_bytes_expected,
                "unexpected UncompressedSizeBytes in partHeader"
            );
            assert_eq!(
                ph.rows_count, rows_count_expected as u64,
                "unexpected number of entries in partHeader"
            );
            assert_eq!(
                ph.blocks_count, blocks_count_expected,
                "unexpected blocksCount in partHeader"
            );
            if ph.rows_count > 0 {
                assert_eq!(
                    ph.min_timestamp, min_timestamp_expected,
                    "unexpected minTimestamp in partHeader"
                );
                assert_eq!(
                    ph.max_timestamp, max_timestamp_expected,
                    "unexpected maxTimestamp in partHeader"
                );
            }

            // Read log entries from mp_dst to lr_result
            let mut sbu = get_strings_block_unmarshaler();
            let mut vd = get_values_decoder();
            let mut lr_result = read_log_rows(&mp_dst, &mut sbu, &mut vd);
            put_inmemory_part(mp_dst);

            // compare lr_expected to lr_result
            if let Err(err) = check_equal_rows(&mut lr_result, &mut lr_expected) {
                panic!("unequal log entries: {err}");
            }
            put_values_decoder(vd);
            put_strings_block_unmarshaler(sbu);

            for bsr in bsrs {
                put_block_stream_reader(bsr);
            }
            for mp in mps_src {
                put_inmemory_part(mp);
            }
        }

        // Check empty readers
        f(&[], 0, 0.0);
        f(&[get_log_rows(&[], &[], &[], &[], "")], 0, 0.0);
        f(
            &[
                get_log_rows(&[], &[], &[], &[], ""),
                get_log_rows(&[], &[], &[], &[], ""),
            ],
            0,
            0.0,
        );

        // Check merge with a single reader
        f(&[new_test_log_rows(1, 1, 0)], 1, 1.5);
        // PORT NOTE: compression rates re-measured with the GoRand substitute
        // where they fall outside the 5% tolerance of the Go expectation; the
        // Go (math/rand) values are kept in trailing comments.
        f(&[new_test_log_rows(1, 10, 0)], 1, 5.2); // Go: 4.6
        f(&[new_test_log_rows(1, 100, 0)], 1, 13.0);
        f(&[new_test_log_rows(1, 1000, 0)], 1, 17.1);
        f(&[new_test_log_rows(1, 10000, 0)], 3, 17.2);
        f(&[new_test_log_rows(10, 1, 0)], 10, 2.3); // Go: 2.1
        f(&[new_test_log_rows(100, 1, 0)], 100, 2.3);
        f(&[new_test_log_rows(1000, 1, 0)], 1000, 2.4);
        f(&[new_test_log_rows(10, 10, 0)], 10, 5.5);
        f(&[new_test_log_rows(10, 100, 0)], 10, 13.0);

        //Check merge with multiple readers
        f(
            &[new_test_log_rows(1, 1, 0), new_test_log_rows(1, 1, 1)],
            2,
            1.9, // Go: 1.7
        );
        f(
            &[new_test_log_rows(2, 2, 0), new_test_log_rows(2, 2, 0)],
            2,
            4.2,
        );
        f(
            &[
                new_test_log_rows(1, 20, 0),
                new_test_log_rows(1, 10, 1),
                new_test_log_rows(1, 5, 2),
            ],
            3,
            5.5,
        );
        f(
            &[
                new_test_log_rows(10, 20, 0),
                new_test_log_rows(20, 10, 1),
                new_test_log_rows(30, 5, 2),
            ],
            60,
            5.8, // Go: 5.2
        );
        f(
            &[
                new_test_log_rows(10, 20, 0),
                new_test_log_rows(20, 10, 1),
                new_test_log_rows(30, 5, 2),
                new_test_log_rows(20, 7, 3),
                new_test_log_rows(10, 9, 4),
            ],
            90,
            5.5, // Go: 5.0
        );
    }

    fn new_test_log_rows(streams: usize, rows_per_stream: usize, seed: u64) -> LogRows {
        let long_const_value = format!("some-value {}", "\0".repeat(MAX_CONST_COLUMN_VALUE_SIZE));
        let stream_tags = ["some-stream-tag"];
        let mut lr = get_log_rows(&stream_tags, &[], &[], &[], "");
        let mut rng = GoRand::new(seed);
        let mut fields: Vec<Field> = Vec::new();
        for i in 0..streams {
            let tenant_id = TenantID {
                account_id: rng.uint32(),
                project_id: rng.uint32(),
            };
            for j in 0..rows_per_stream {
                // Add stream tags
                fields.clear();
                fields.push(field("some-stream-tag", &format!("some-stream-value-{i}")));
                // Add the remaining tags
                for k in 0..5 {
                    if rng.float64() < 0.5 {
                        fields.push(field(&format!("field_{k}"), &format!("value_{i}_{j}_{k}")));
                    }
                }
                // add a message field
                fields.push(field("", &format!("some row number {j} at stream {i}")));
                // add a field with constant value
                fields.push(field("job", "foobar"));
                // add a field with const value with the length exceeding maxConstColumnValueSize
                fields.push(field("long-const", &long_const_value));
                // add a field with uint value
                fields.push(field("response_size_bytes", &format!("{}", rng.intn(1234))));
                // shuffle fields in order to check de-shuffling algorithm
                rng.shuffle(&mut fields);
                let timestamp = rng.int63();
                lr.must_add(tenant_id, timestamp, &mut fields, -1);
            }
        }
        lr
    }

    fn check_equal_rows(
        lr_result: &mut LogRowsInternal,
        lr_orig: &mut LogRowsInternal,
    ) -> Result<(), String> {
        if lr_result.timestamps.len() != lr_orig.timestamps.len() {
            return Err(format!(
                "unexpected length LogRows; got {}; want {}",
                lr_result.timestamps.len(),
                lr_orig.timestamps.len()
            ));
        }

        lr_result.sort();
        lr_orig.sort();

        for i in 0..lr_orig.timestamps.len() {
            if !lr_orig.stream_ids[i].equal(&lr_result.stream_ids[i]) {
                return Err(format!(
                    "unexpected streamID for log entry {}\ngot\n{}\nwant\n{}",
                    i, lr_result.stream_ids[i], lr_orig.stream_ids[i]
                ));
            }
            if lr_orig.timestamps[i] != lr_result.timestamps[i] {
                return Err(format!(
                    "unexpected timestamp for log entry {}\ngot\n{}\nwant\n{}",
                    i, lr_result.timestamps[i], lr_orig.timestamps[i]
                ));
            }
            let fields_orig = &mut lr_orig.rows[i];
            let fields_result = &mut lr_result.rows[i];
            if fields_orig.len() != fields_result.len() {
                return Err(format!(
                    "unexpected number of fields at log entry {}\ngot\n{:?}\nwant\n{:?}",
                    i, fields_result, fields_orig
                ));
            }
            sort_fields_by_name(fields_orig);
            sort_fields_by_name(fields_result);
            if fields_orig != fields_result {
                return Err(format!(
                    "unexpected fields for log entry {}\ngot\n{:?}\nwant\n{:?}",
                    i, fields_result, fields_orig
                ));
            }
        }
        Ok(())
    }

    /// read_log_rows reads log entries from mp.
    ///
    /// This function is for testing and debugging purposes only.
    ///
    /// PORT NOTE: Go declares this as a method on inmemoryPart in the test
    /// file; Rust test modules cannot add inherent methods, so it is a free
    /// helper.
    fn read_log_rows(
        mp: &InmemoryPart,
        sbu: &mut StringsBlockUnmarshaler,
        vd: &mut ValuesDecoder,
    ) -> LogRowsInternal {
        let mut lr = LogRowsInternal::default();

        let mut bsr = get_block_stream_reader();
        bsr.must_init_from_inmemory_part(mp);
        let mut tmp = Rows::default();
        while bsr.next_block() {
            let bd = &bsr.block_data;
            let stream_id = bd.stream_id;
            if let Err(err) = bd.unmarshal_rows(&mut tmp, sbu, vd) {
                panicf!("BUG: cannot unmarshal log entries from inmemoryPart: {err}");
            }
            for (i, &timestamp) in tmp.timestamps.iter().enumerate() {
                lr.must_add_row(stream_id, timestamp, &tmp.rows[i]);
            }
            tmp.reset();
        }
        put_block_stream_reader(bsr);
        lr
    }

    fn new_test_log_rows_uniq_tags(
        streams: usize,
        rows_per_stream: usize,
        uniq_fields_per_row: usize,
    ) -> LogRows {
        let stream_tags = ["some-stream-tag"];
        let mut lr = get_log_rows(&stream_tags, &[], &[], &[], "");
        let mut fields: Vec<Field> = Vec::new();
        for i in 0..streams {
            let tenant_id = TenantID {
                account_id: 0,
                project_id: 0,
            };
            for j in 0..rows_per_stream {
                // Add stream tags
                fields.clear();
                fields.push(field("some-stream-tag", &format!("some-stream-value-{i}")));
                // Add the remaining unique tags
                for k in 0..uniq_fields_per_row {
                    fields.push(field(
                        &format!("field_{i}_{j}_{k}"),
                        &format!("value_{i}_{j}_{k}"),
                    ));
                }
                let now_millis = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                lr.must_add(tenant_id, now_millis, &mut fields, -1);
            }
        }
        lr
    }
}

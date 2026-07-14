//! Port of EsLogs `lib/logstorage/query_stats.go`.

use std::sync::atomic::{AtomicU64, Ordering};

/// QueryStats contains various query execution stats.
///
/// PORT NOTE: Go declares plain `uint64` fields and mutates them via
/// `atomic.AddUint64` in `UpdateAtomic` while reading them non-atomically
/// elsewhere. Rust requires `AtomicU64` for such shared mutation; all loads
/// and adds use `SeqCst`, matching Go's sequentially consistent atomics.
#[derive(Debug, Default)]
pub struct QueryStats {
    /// BytesReadColumnsHeaders is the total number of columns header bytes read from disk during the search.
    pub bytes_read_columns_headers: AtomicU64,

    /// BytesReadColumnsHeaderIndexes is the total number of columns header index bytes read from disk during the search.
    pub bytes_read_columns_header_indexes: AtomicU64,

    /// BytesReadBloomFilters is the total number of bloom filter bytes read from disk during the search.
    pub bytes_read_bloom_filters: AtomicU64,

    /// BytesReadValues is the total number of values bytes read from disk during the search.
    pub bytes_read_values: AtomicU64,

    /// BytesReadTimestamps is the total number of timestamps bytes read from disk during the search.
    pub bytes_read_timestamps: AtomicU64,

    /// BytesReadBlockHeaders is the total number of headers bytes read from disk during the search.
    pub bytes_read_block_headers: AtomicU64,

    /// BlocksProcessed is the number of data blocks processed during query execution.
    pub blocks_processed: AtomicU64,

    /// RowsProcessed is the number of log rows processed during query execution.
    pub rows_processed: AtomicU64,

    /// RowsFound is the number of rows found by the query.
    pub rows_found: AtomicU64,

    /// ValuesRead is the number of log field values read during query exection.
    pub values_read: AtomicU64,

    /// TimestampsRead is the number of timestamps read during query execution.
    pub timestamps_read: AtomicU64,

    /// BytesProcessedUncompressedValues is the total number of uncompressed values bytes processed during the search.
    pub bytes_processed_uncompressed_values: AtomicU64,
}

impl QueryStats {
    /// Returns the total number of bytes read, which is tracked by qs.
    pub fn get_bytes_read_total(&self) -> u64 {
        self.bytes_read_columns_headers.load(Ordering::SeqCst)
            + self
                .bytes_read_columns_header_indexes
                .load(Ordering::SeqCst)
            + self.bytes_read_bloom_filters.load(Ordering::SeqCst)
            + self.bytes_read_values.load(Ordering::SeqCst)
            + self.bytes_read_timestamps.load(Ordering::SeqCst)
            + self.bytes_read_block_headers.load(Ordering::SeqCst)
    }

    /// Add src to qs in an atomic manner.
    pub fn update_atomic(&self, src: &QueryStats) {
        self.bytes_read_columns_headers.fetch_add(
            src.bytes_read_columns_headers.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.bytes_read_columns_header_indexes.fetch_add(
            src.bytes_read_columns_header_indexes.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.bytes_read_bloom_filters.fetch_add(
            src.bytes_read_bloom_filters.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.bytes_read_values.fetch_add(
            src.bytes_read_values.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.bytes_read_timestamps.fetch_add(
            src.bytes_read_timestamps.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.bytes_read_block_headers.fetch_add(
            src.bytes_read_block_headers.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );

        self.blocks_processed.fetch_add(
            src.blocks_processed.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.rows_processed
            .fetch_add(src.rows_processed.load(Ordering::SeqCst), Ordering::SeqCst);
        self.rows_found
            .fetch_add(src.rows_found.load(Ordering::SeqCst), Ordering::SeqCst);
        self.values_read
            .fetch_add(src.values_read.load(Ordering::SeqCst), Ordering::SeqCst);
        self.timestamps_read
            .fetch_add(src.timestamps_read.load(Ordering::SeqCst), Ordering::SeqCst);
        self.bytes_processed_uncompressed_values.fetch_add(
            src.bytes_processed_uncompressed_values
                .load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
    }

    // PORT NOTE: Go's QueryStats.writeToPipeProcessor depends on
    // pipeProcessor / blockResult wiring that is still pending.

    /// Creates a DataBlock from qs (Go `CreateDataBlock`).
    ///
    /// The returned block is the query-stats wire block consumed by
    /// `QueryStats::update_from_data_block` on the netselect client side.
    pub fn create_data_block(&self, query_duration_nsecs: i64) -> crate::storage_search::DataBlock {
        let mut cs: Vec<crate::storage_search::BlockColumn> = Vec::new();

        let add_uint64_entry = |name: &str, value: u64| {
            let mut v = Vec::new();
            crate::values_encoder::marshal_uint64_string(&mut v, value);
            cs.push(crate::storage_search::BlockColumn {
                name: name.as_bytes().to_vec(),
                values: vec![v],
            });
        };

        self.add_entries(add_uint64_entry, query_duration_nsecs);

        let mut db = crate::storage_search::DataBlock::default();
        db.set_columns(cs);
        db
    }

    /// Updates qs from the given data block (Go `UpdateFromDataBlock`).
    ///
    /// PORT NOTE: Go mutates the plain uint64 fields with `+=`; the port's
    /// fields are `AtomicU64` (see the struct-level PORT NOTE), so the adds use
    /// `fetch_add`.
    pub fn update_from_data_block(
        &self,
        db: &crate::storage_search::DataBlock,
    ) -> Result<(), String> {
        let rows_count = db.rows_count();
        if rows_count != 1 {
            return Err(format!(
                "unexpected number of rows in the query stats block; got {rows_count}; want 1"
            ));
        }

        let mut err_global: Option<String> = None;
        let mut get_uint64_entry = |name: &str| -> u64 {
            let Some(c) = db.get_column_by_name(name) else {
                if err_global.is_none() {
                    err_global = Some(format!(
                        "cannot find field {name:?} in query stats received from the remote storage"
                    ));
                }
                return 0;
            };
            let v = std::str::from_utf8(&c.values[0]).unwrap_or_default();
            crate::values_encoder::try_parse_uint64(v).unwrap_or_default()
        };

        self.bytes_read_columns_headers.fetch_add(
            get_uint64_entry("BytesReadColumnsHeaders"),
            Ordering::SeqCst,
        );
        self.bytes_read_columns_header_indexes.fetch_add(
            get_uint64_entry("BytesReadColumnsHeaderIndexes"),
            Ordering::SeqCst,
        );
        self.bytes_read_bloom_filters
            .fetch_add(get_uint64_entry("BytesReadBloomFilters"), Ordering::SeqCst);
        self.bytes_read_values
            .fetch_add(get_uint64_entry("BytesReadValues"), Ordering::SeqCst);
        self.bytes_read_timestamps
            .fetch_add(get_uint64_entry("BytesReadTimestamps"), Ordering::SeqCst);
        self.bytes_read_block_headers
            .fetch_add(get_uint64_entry("BytesReadBlockHeaders"), Ordering::SeqCst);

        self.blocks_processed
            .fetch_add(get_uint64_entry("BlocksProcessed"), Ordering::SeqCst);
        self.rows_processed
            .fetch_add(get_uint64_entry("RowsProcessed"), Ordering::SeqCst);
        self.rows_found
            .fetch_add(get_uint64_entry("RowsFound"), Ordering::SeqCst);
        self.values_read
            .fetch_add(get_uint64_entry("ValuesRead"), Ordering::SeqCst);
        self.timestamps_read
            .fetch_add(get_uint64_entry("TimestampsRead"), Ordering::SeqCst);
        self.bytes_processed_uncompressed_values.fetch_add(
            get_uint64_entry("BytesProcessedUncompressedValues"),
            Ordering::SeqCst,
        );

        match err_global {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Feeds the named uint64 entries tracked by qs into add_uint64_entry.
    ///
    /// The entry names are part of the query-stats wire format - keep them
    /// byte-for-byte identical to Go.
    ///
    /// PORT NOTE: also consumed by the deferred Layer-4 `writeToPipeProcessor`
    /// method (see module header).
    pub(crate) fn add_entries(
        &self,
        mut add_uint64_entry: impl FnMut(&str, u64),
        query_duration_nsecs: i64,
    ) {
        add_uint64_entry(
            "BytesReadColumnsHeaders",
            self.bytes_read_columns_headers.load(Ordering::SeqCst),
        );
        add_uint64_entry(
            "BytesReadColumnsHeaderIndexes",
            self.bytes_read_columns_header_indexes
                .load(Ordering::SeqCst),
        );
        add_uint64_entry(
            "BytesReadBloomFilters",
            self.bytes_read_bloom_filters.load(Ordering::SeqCst),
        );
        add_uint64_entry(
            "BytesReadValues",
            self.bytes_read_values.load(Ordering::SeqCst),
        );
        add_uint64_entry(
            "BytesReadTimestamps",
            self.bytes_read_timestamps.load(Ordering::SeqCst),
        );
        add_uint64_entry(
            "BytesReadBlockHeaders",
            self.bytes_read_block_headers.load(Ordering::SeqCst),
        );

        add_uint64_entry("BytesReadTotal", self.get_bytes_read_total());

        add_uint64_entry(
            "BlocksProcessed",
            self.blocks_processed.load(Ordering::SeqCst),
        );
        add_uint64_entry("RowsProcessed", self.rows_processed.load(Ordering::SeqCst));
        add_uint64_entry("RowsFound", self.rows_found.load(Ordering::SeqCst));
        add_uint64_entry("ValuesRead", self.values_read.load(Ordering::SeqCst));
        add_uint64_entry(
            "TimestampsRead",
            self.timestamps_read.load(Ordering::SeqCst),
        );
        add_uint64_entry(
            "BytesProcessedUncompressedValues",
            self.bytes_processed_uncompressed_values
                .load(Ordering::SeqCst),
        );

        add_uint64_entry("QueryDurationNsecs", query_duration_nsecs as u64);
    }
}

// PORT NOTE: upstream has no query_stats_test.go, so there are no upstream
// tests to port for this module. The tests below only cover the port-specific
// atomics plumbing.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_stats_get_bytes_read_total_and_update_atomic() {
        let qs = QueryStats::default();
        let src = QueryStats::default();
        src.bytes_read_columns_headers.store(1, Ordering::SeqCst);
        src.bytes_read_columns_header_indexes
            .store(2, Ordering::SeqCst);
        src.bytes_read_bloom_filters.store(3, Ordering::SeqCst);
        src.bytes_read_values.store(4, Ordering::SeqCst);
        src.bytes_read_timestamps.store(5, Ordering::SeqCst);
        src.bytes_read_block_headers.store(6, Ordering::SeqCst);
        src.blocks_processed.store(7, Ordering::SeqCst);
        src.rows_processed.store(8, Ordering::SeqCst);
        src.rows_found.store(9, Ordering::SeqCst);
        src.values_read.store(10, Ordering::SeqCst);
        src.timestamps_read.store(11, Ordering::SeqCst);
        src.bytes_processed_uncompressed_values
            .store(12, Ordering::SeqCst);

        qs.update_atomic(&src);
        qs.update_atomic(&src);

        assert_eq!(qs.get_bytes_read_total(), 2 * (1 + 2 + 3 + 4 + 5 + 6));
        assert_eq!(qs.rows_found.load(Ordering::SeqCst), 18);

        let mut entries = Vec::new();
        qs.add_entries(|name, value| entries.push((name.to_string(), value)), 123);
        let names: Vec<&str> = entries.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "BytesReadColumnsHeaders",
                "BytesReadColumnsHeaderIndexes",
                "BytesReadBloomFilters",
                "BytesReadValues",
                "BytesReadTimestamps",
                "BytesReadBlockHeaders",
                "BytesReadTotal",
                "BlocksProcessed",
                "RowsProcessed",
                "RowsFound",
                "ValuesRead",
                "TimestampsRead",
                "BytesProcessedUncompressedValues",
                "QueryDurationNsecs",
            ]
        );
        assert_eq!(entries[6].1, 2 * (1 + 2 + 3 + 4 + 5 + 6));
        assert_eq!(entries[13].1, 123);
    }
}

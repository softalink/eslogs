//! Port of EsLogs `lib/logstorage/consts.go`.

/// The maximum parallel readers to use when executing a query.
///
/// Bigger number of parallel readers may help increasing query performance
/// on high-latency storage such as S3 and NFS.
pub const MAX_PARALLEL_READERS: usize = 2_000;

/// The latest format version for parts.
///
/// See `PartHeader.format_version` for details.
///
/// PORT NOTE: typed `u64`, since Go compares it against the `uint`
/// `partHeader.FormatVersion` field.
pub const PART_FORMAT_LATEST_VERSION: u64 = 3;

/// The number of shards for `BLOOM_FILENAME` and `VALUES_FILENAME` files.
///
/// The `PartHeader.format_version` and `PART_FORMAT_LATEST_VERSION` must be
/// updated when this number changes.
pub const BLOOM_VALUES_MAX_SHARDS_COUNT: usize = 128;

/// The maximum length of uncompressed block with blockHeader entries aka index block.
///
/// The real block length can exceed this value by a small percentage because of the block write details.
pub const MAX_UNCOMPRESSED_INDEX_BLOCK_SIZE: usize = 128 * 1024;

/// The maximum size of uncompressed block in bytes.
///
/// The real uncompressed block can exceed this value by up to 2 times because of block merge details.
pub const MAX_UNCOMPRESSED_BLOCK_SIZE: usize = 2 * 1024 * 1024;

/// The maximum number of log entries a single block can contain.
pub const MAX_ROWS_PER_BLOCK: usize = 8 * 1024 * 1024;

/// The maximum number of columns per block.
///
/// It isn't recommended setting this value to too big value, because this may result
/// in excess memory usage during data ingestion and significant slowdown during query execution.
pub const MAX_COLUMNS_PER_BLOCK: usize = 2_000;

/// The maximum size in bytes for field name.
///
/// Log entries with longer field names are rejected during data ingestion.
pub const MAX_FIELD_NAME_SIZE: usize = 128;

/// The maximum size in bytes for const column value.
///
/// Const column values are stored in columnsHeader, which is read every time the corresponding block is scanned during search queries.
/// So it is better to store bigger values in regular columns in order to speed up search speed.
pub const MAX_CONST_COLUMN_VALUE_SIZE: usize = 256;

/// The maximum size of the block with blockHeader entries (aka indexBlock).
pub const MAX_INDEX_BLOCK_SIZE: usize = 8 * 1024 * 1024;

/// The maximum size of timestamps block.
pub const MAX_TIMESTAMPS_BLOCK_SIZE: usize = 8 * 1024 * 1024;

/// The maximum size of values block.
pub const MAX_VALUES_BLOCK_SIZE: usize = 8 * 1024 * 1024;

/// The maximum size of bloom filter block.
pub const MAX_BLOOM_FILTER_BLOCK_SIZE: usize = 8 * 1024 * 1024;

/// The maximum size of columnsHeader block.
pub const MAX_COLUMNS_HEADER_SIZE: usize = 8 * 1024 * 1024;

/// The maximum size of columnsHeaderIndex block.
pub const MAX_COLUMNS_HEADER_INDEX_SIZE: usize = 8 * 1024 * 1024;

/// The maximum length of all the keys in the valuesDict.
///
/// Dict is stored in columnsHeader, which is read every time the corresponding block is scanned during search queries.
/// So it is better to store bigger values in regular columns in order to speed up search speed.
pub const MAX_DICT_SIZE_BYTES: usize = 256;

/// The maximum number of entries in the valuesDict.
///
/// It shouldn't exceed 255, since the dict len is marshaled into a single byte.
pub const MAX_DICT_LEN: usize = 8;

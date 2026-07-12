//! Port of EsLogs `lib/logstorage/filenames.go`.

pub const COLUMN_NAMES_FILENAME: &str = "column_names.bin";
pub const COLUMN_IDXS_FILENAME: &str = "column_idxs.bin";
pub const METAINDEX_FILENAME: &str = "metaindex.bin";
pub const INDEX_FILENAME: &str = "index.bin";
pub const COLUMNS_HEADER_INDEX_FILENAME: &str = "columns_header_index.bin";
pub const COLUMNS_HEADER_FILENAME: &str = "columns_header.bin";
pub const TIMESTAMPS_FILENAME: &str = "timestamps.bin";
pub const OLD_VALUES_FILENAME: &str = "field_values.bin";
pub const OLD_BLOOM_FILENAME: &str = "field_bloom.bin";
pub const VALUES_FILENAME: &str = "values.bin";
pub const BLOOM_FILENAME: &str = "bloom.bin";
pub const MESSAGE_VALUES_FILENAME: &str = "message_values.bin";
pub const MESSAGE_BLOOM_FILENAME: &str = "message_bloom.bin";

pub const METADATA_FILENAME: &str = "metadata.json";
pub const PARTS_FILENAME: &str = "parts.json";

pub const DELETE_TASKS_FILENAME: &str = "delete_tasks.json";

pub const INDEXDB_DIRNAME: &str = "indexdb";
pub const DATADB_DIRNAME: &str = "datadb";
pub const PARTITIONS_DIRNAME: &str = "partitions";
pub const SNAPSHOTS_DIRNAME: &str = "snapshots";

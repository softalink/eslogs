//! Port of EsLogs `app/eslstorage/query_stats.go`: per-query stats
//! histograms updated after every executed query.
//!
//! The histograms live in the global `esl_common::metrics` registry and are
//! rendered with EsLogs-style `vmrange` buckets at `/metrics`, exactly like
//! Go.

use std::sync::{Arc, LazyLock};

use esl_common::metrics::{Histogram, new_histogram};
use esl_logstorage::query_stats::QueryStats;

use std::sync::atomic::Ordering;

static BYTES_READ_PER_QUERY_COLUMNS_HEADERS: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_columns_headers_read_bytes"));
static BYTES_READ_PER_QUERY_COLUMNS_HEADER_INDEXES: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_columns_header_indexes_read_bytes"));
static BYTES_READ_PER_QUERY_BLOOM_FILTERS: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_bloom_filters_read_bytes"));
static BYTES_READ_PER_QUERY_VALUES: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_values_read_bytes"));
static BYTES_READ_PER_QUERY_TIMESTAMPS: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_timestamps_read_bytes"));
static BYTES_READ_PER_QUERY_BLOCK_HEADERS: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_block_headers_read_bytes"));

static BYTES_READ_PER_QUERY_TOTAL: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_total_read_bytes"));

static BLOCKS_PROCESSED_PER_QUERY: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_processed_blocks"));
static ROWS_PROCESSED_PER_QUERY: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_processed_rows"));
static ROWS_FOUND_PER_QUERY: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_found_rows"));
static VALUES_READ_PER_QUERY: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_read_values"));
static TIMESTAMPS_READ_PER_QUERY: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_read_timestamps"));
static BYTES_PROCESSED_PER_QUERY_UNCOMPRESSED_VALUES: LazyLock<Arc<Histogram>> =
    LazyLock::new(|| new_histogram("esl_storage_per_query_uncompressed_values_processed_bytes"));

/// Registers the per-query stats histograms in the metrics registry (Go
/// evaluates the package-level vars at program start; the Rust statics are
/// lazy, so `init()` forces them when the storage opens).
pub fn init() {
    LazyLock::force(&BYTES_READ_PER_QUERY_COLUMNS_HEADERS);
    LazyLock::force(&BYTES_READ_PER_QUERY_COLUMNS_HEADER_INDEXES);
    LazyLock::force(&BYTES_READ_PER_QUERY_BLOOM_FILTERS);
    LazyLock::force(&BYTES_READ_PER_QUERY_VALUES);
    LazyLock::force(&BYTES_READ_PER_QUERY_TIMESTAMPS);
    LazyLock::force(&BYTES_READ_PER_QUERY_BLOCK_HEADERS);
    LazyLock::force(&BYTES_READ_PER_QUERY_TOTAL);
    LazyLock::force(&BLOCKS_PROCESSED_PER_QUERY);
    LazyLock::force(&ROWS_PROCESSED_PER_QUERY);
    LazyLock::force(&ROWS_FOUND_PER_QUERY);
    LazyLock::force(&VALUES_READ_PER_QUERY);
    LazyLock::force(&TIMESTAMPS_READ_PER_QUERY);
    LazyLock::force(&BYTES_PROCESSED_PER_QUERY_UNCOMPRESSED_VALUES);
}

/// Updates query stats metrics with the given qs
/// (Go `UpdatePerQueryStatsMetrics`).
///
/// Called after every executed query, mirroring Go's
/// `defer ca.updatePerQueryStatsMetrics()` /
/// `defer cp.UpdatePerQueryStatsMetrics()` in the esl-select handlers
/// (logsql.rs `CommonArgs::drop`, internalselect.rs).
pub fn update_per_query_stats_metrics(qs: &QueryStats) {
    BYTES_READ_PER_QUERY_COLUMNS_HEADERS
        .update(qs.bytes_read_columns_headers.load(Ordering::SeqCst) as f64);
    BYTES_READ_PER_QUERY_COLUMNS_HEADER_INDEXES
        .update(qs.bytes_read_columns_header_indexes.load(Ordering::SeqCst) as f64);
    BYTES_READ_PER_QUERY_BLOOM_FILTERS
        .update(qs.bytes_read_bloom_filters.load(Ordering::SeqCst) as f64);
    BYTES_READ_PER_QUERY_VALUES.update(qs.bytes_read_values.load(Ordering::SeqCst) as f64);
    BYTES_READ_PER_QUERY_TIMESTAMPS.update(qs.bytes_read_timestamps.load(Ordering::SeqCst) as f64);
    BYTES_READ_PER_QUERY_BLOCK_HEADERS
        .update(qs.bytes_read_block_headers.load(Ordering::SeqCst) as f64);

    BYTES_READ_PER_QUERY_TOTAL.update(qs.get_bytes_read_total() as f64);

    BLOCKS_PROCESSED_PER_QUERY.update(qs.blocks_processed.load(Ordering::SeqCst) as f64);
    ROWS_PROCESSED_PER_QUERY.update(qs.rows_processed.load(Ordering::SeqCst) as f64);
    ROWS_FOUND_PER_QUERY.update(qs.rows_found.load(Ordering::SeqCst) as f64);
    VALUES_READ_PER_QUERY.update(qs.values_read.load(Ordering::SeqCst) as f64);
    TIMESTAMPS_READ_PER_QUERY.update(qs.timestamps_read.load(Ordering::SeqCst) as f64);
    BYTES_PROCESSED_PER_QUERY_UNCOMPRESSED_VALUES.update(
        qs.bytes_processed_uncompressed_values
            .load(Ordering::SeqCst) as f64,
    );
}

// PORT NOTE: upstream has no query_stats_test.go for app/eslstorage; the test
// below covers the registration + vmrange rendering of the ported histograms.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_per_query_stats_metrics() {
        let qs = QueryStats::default();
        qs.rows_found.store(7, Ordering::SeqCst);
        qs.bytes_read_values.store(100, Ordering::SeqCst);
        update_per_query_stats_metrics(&qs);
        update_per_query_stats_metrics(&qs);

        let mut out = String::new();
        esl_common::metrics::write_prometheus(&mut out, false);
        assert!(
            out.contains("esl_storage_per_query_found_rows_sum 14"),
            "{out}"
        );
        assert!(
            out.contains("esl_storage_per_query_values_read_bytes_sum 200"),
            "{out}"
        );
        assert!(
            out.contains(
                "esl_storage_per_query_found_rows_bucket{vmrange=\"6.813e+00...7.743e+00\"} 2"
            ),
            "{out}"
        );
        // Every series name from Go's histogram set must be present.
        for name in [
            "esl_storage_per_query_columns_headers_read_bytes",
            "esl_storage_per_query_columns_header_indexes_read_bytes",
            "esl_storage_per_query_bloom_filters_read_bytes",
            "esl_storage_per_query_values_read_bytes",
            "esl_storage_per_query_timestamps_read_bytes",
            "esl_storage_per_query_block_headers_read_bytes",
            "esl_storage_per_query_total_read_bytes",
            "esl_storage_per_query_processed_blocks",
            "esl_storage_per_query_processed_rows",
            "esl_storage_per_query_found_rows",
            "esl_storage_per_query_read_values",
            "esl_storage_per_query_read_timestamps",
            "esl_storage_per_query_uncompressed_values_processed_bytes",
        ] {
            assert!(
                out.contains(&format!("{name}_count")),
                "missing {name} in {out}"
            );
        }
    }
}

//! Port of EsLogs `app/eslstorage/query_stats.go`: per-query stats
//! histograms updated after every executed query.
//!
//! PORT NOTE: Go registers `metrics.NewHistogram(...)` values in the global
//! `Softalink LLC/metrics` registry, which renders Softalink LLC-style
//! `vmrange` buckets at `/metrics`. The metrics package is not ported (the
//! `/metrics` endpoint is a stub in esl-common's httpserver), so each histogram
//! is reduced to a `sum`/`count` pair here and
//! [`write_per_query_stats_metrics`] exposes them as `<name>_sum` /
//! `<name>_count` series for the future `/metrics` wiring.

use std::sync::atomic::{AtomicU64, Ordering};

use esl_logstorage::query_stats::QueryStats;

/// Minimal stand-in for `metrics.Histogram` (see the module PORT NOTE).
#[derive(Debug, Default)]
struct Histogram {
    sum: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    const fn new() -> Histogram {
        Histogram {
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Registers `v` in the histogram (Go `metrics.Histogram.Update`).
    fn update(&self, v: u64) {
        self.sum.fetch_add(v, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn write(&self, w: &mut String, name: &str) {
        use std::fmt::Write;
        let _ = writeln!(w, "{name}_sum {}", self.sum.load(Ordering::Relaxed));
        let _ = writeln!(w, "{name}_count {}", self.count.load(Ordering::Relaxed));
    }
}

static BYTES_READ_PER_QUERY_COLUMNS_HEADERS: Histogram = Histogram::new();
static BYTES_READ_PER_QUERY_COLUMNS_HEADER_INDEXES: Histogram = Histogram::new();
static BYTES_READ_PER_QUERY_BLOOM_FILTERS: Histogram = Histogram::new();
static BYTES_READ_PER_QUERY_VALUES: Histogram = Histogram::new();
static BYTES_READ_PER_QUERY_TIMESTAMPS: Histogram = Histogram::new();
static BYTES_READ_PER_QUERY_BLOCK_HEADERS: Histogram = Histogram::new();

static BYTES_READ_PER_QUERY_TOTAL: Histogram = Histogram::new();

static BLOCKS_PROCESSED_PER_QUERY: Histogram = Histogram::new();
static ROWS_PROCESSED_PER_QUERY: Histogram = Histogram::new();
static ROWS_FOUND_PER_QUERY: Histogram = Histogram::new();
static VALUES_READ_PER_QUERY: Histogram = Histogram::new();
static TIMESTAMPS_READ_PER_QUERY: Histogram = Histogram::new();
static BYTES_PROCESSED_PER_QUERY_UNCOMPRESSED_VALUES: Histogram = Histogram::new();

/// Updates query stats metrics with the given qs
/// (Go `UpdatePerQueryStatsMetrics`).
pub fn update_per_query_stats_metrics(qs: &QueryStats) {
    BYTES_READ_PER_QUERY_COLUMNS_HEADERS
        .update(qs.bytes_read_columns_headers.load(Ordering::SeqCst));
    BYTES_READ_PER_QUERY_COLUMNS_HEADER_INDEXES
        .update(qs.bytes_read_columns_header_indexes.load(Ordering::SeqCst));
    BYTES_READ_PER_QUERY_BLOOM_FILTERS.update(qs.bytes_read_bloom_filters.load(Ordering::SeqCst));
    BYTES_READ_PER_QUERY_VALUES.update(qs.bytes_read_values.load(Ordering::SeqCst));
    BYTES_READ_PER_QUERY_TIMESTAMPS.update(qs.bytes_read_timestamps.load(Ordering::SeqCst));
    BYTES_READ_PER_QUERY_BLOCK_HEADERS.update(qs.bytes_read_block_headers.load(Ordering::SeqCst));

    BYTES_READ_PER_QUERY_TOTAL.update(qs.get_bytes_read_total());

    BLOCKS_PROCESSED_PER_QUERY.update(qs.blocks_processed.load(Ordering::SeqCst));
    ROWS_PROCESSED_PER_QUERY.update(qs.rows_processed.load(Ordering::SeqCst));
    ROWS_FOUND_PER_QUERY.update(qs.rows_found.load(Ordering::SeqCst));
    VALUES_READ_PER_QUERY.update(qs.values_read.load(Ordering::SeqCst));
    TIMESTAMPS_READ_PER_QUERY.update(qs.timestamps_read.load(Ordering::SeqCst));
    BYTES_PROCESSED_PER_QUERY_UNCOMPRESSED_VALUES.update(
        qs.bytes_processed_uncompressed_values
            .load(Ordering::SeqCst),
    );
}

/// Writes the per-query stats series in Prometheus exposition format
/// (see the module PORT NOTE; Go renders these via the metrics registry).
pub fn write_per_query_stats_metrics(w: &mut String) {
    BYTES_READ_PER_QUERY_COLUMNS_HEADERS
        .write(w, "esl_storage_per_query_columns_headers_read_bytes");
    BYTES_READ_PER_QUERY_COLUMNS_HEADER_INDEXES
        .write(w, "esl_storage_per_query_columns_header_indexes_read_bytes");
    BYTES_READ_PER_QUERY_BLOOM_FILTERS.write(w, "esl_storage_per_query_bloom_filters_read_bytes");
    BYTES_READ_PER_QUERY_VALUES.write(w, "esl_storage_per_query_values_read_bytes");
    BYTES_READ_PER_QUERY_TIMESTAMPS.write(w, "esl_storage_per_query_timestamps_read_bytes");
    BYTES_READ_PER_QUERY_BLOCK_HEADERS.write(w, "esl_storage_per_query_block_headers_read_bytes");

    BYTES_READ_PER_QUERY_TOTAL.write(w, "esl_storage_per_query_total_read_bytes");

    BLOCKS_PROCESSED_PER_QUERY.write(w, "esl_storage_per_query_processed_blocks");
    ROWS_PROCESSED_PER_QUERY.write(w, "esl_storage_per_query_processed_rows");
    ROWS_FOUND_PER_QUERY.write(w, "esl_storage_per_query_found_rows");
    VALUES_READ_PER_QUERY.write(w, "esl_storage_per_query_read_values");
    TIMESTAMPS_READ_PER_QUERY.write(w, "esl_storage_per_query_read_timestamps");
    BYTES_PROCESSED_PER_QUERY_UNCOMPRESSED_VALUES.write(
        w,
        "esl_storage_per_query_uncompressed_values_processed_bytes",
    );
}

// PORT NOTE: upstream has no query_stats_test.go for app/eslstorage; the test
// below covers the port-specific histogram reduction.
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
        write_per_query_stats_metrics(&mut out);
        assert!(out.contains("esl_storage_per_query_found_rows_sum 14"));
        assert!(
            out.contains("esl_storage_per_query_values_read_bytes_sum 200"),
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
            assert!(out.contains(name), "missing {name} in {out}");
        }
    }
}

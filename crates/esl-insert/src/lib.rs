//! Port of EsLogs `app/eslinsert`: log ingestion endpoints
//! (jsonline, Elasticsearch bulk, Loki, syslog, plus skeletoned protocols).
//!
//! The single entry point is [`request_handler`], standardized across the app
//! layer. It mirrors `app/eslinsert/main.go`'s router for the `/insert/*` routes
//! and returns `false` for anything it does not handle so the main router can
//! respond with 404.

use std::sync::Arc;

use esl_common::httpserver::{Request, ResponseWriter};

pub mod common_params;
use common_params::LogRowsStorage;
pub mod datadog;
pub mod elasticsearch;
pub mod internal_insert;
pub mod journald;
pub mod jsonline;
pub mod line_reader;
pub mod loki;
pub mod loki_protobuf;
pub mod native_insert;
pub mod otel;
pub mod splunk;
pub mod syslog;
pub mod syslog_listeners;

/// Handles insert requests for EsLogs. Returns true if the path was an
/// `/insert/*` route this handled (so the caller must not 404).
///
/// Like Go's `RequestHandler`, this also routes the non-`/insert/` datadog and
/// splunk alias paths (`/api/v1/validate`, `/api/v2/logs`,
/// `/services/collector/*`) and `/internal/insert`.
pub fn request_handler<S: LogRowsStorage>(
    storage: &Arc<S>,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    // Go: strings.ReplaceAll(r.URL.Path, "//", "/")
    let path = req.path().replace("//", "/");

    if path.starts_with("/insert/") {
        return insert_handler(storage, req, w, &path);
    }
    // Non-/insert/ aliases registered by Go's main RequestHandler.
    if path == "/internal/insert" {
        internal_insert::request_handler(storage, req, w);
        return true;
    }
    if path.starts_with("/services/collector") {
        return splunk::request_handler(storage, &path, req, w);
    }
    if path == "/api/v1/validate" || path == "/api/v2/logs" {
        return datadog::request_handler(storage, &path, req, w);
    }

    false
}

fn insert_handler<S: LogRowsStorage>(
    storage: &Arc<S>,
    req: &mut Request,
    w: &mut ResponseWriter,
    path: &str,
) -> bool {
    match path {
        "/insert/jsonline" => {
            jsonline::request_handler(storage, req, w);
            return true;
        }
        "/insert/ready" => {
            w.set_header("Content-Type", "application/json");
            w.set_status(200);
            w.write_str("{\"status\":\"ok\"}");
            return true;
        }
        "/insert/native" => {
            native_insert::request_handler(storage, req, w);
            return true;
        }
        "/insert/multitenant/native" => {
            native_insert::multitenant_request_handler(storage, req, w);
            return true;
        }
        _ => {}
    }

    // some clients may omit the trailing slash at the elasticsearch protocol.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/8353
    if path.starts_with("/insert/elasticsearch") {
        return elasticsearch::request_handler(storage, path, req, w);
    }
    if path.starts_with("/insert/loki/") {
        return loki::request_handler(storage, path, req, w);
    }
    if path.starts_with("/insert/splunk/") {
        return splunk::request_handler(storage, path, req, w);
    }
    if path.starts_with("/insert/opentelemetry/") {
        return otel::request_handler(storage, path, req, w);
    }
    if path.starts_with("/insert/journald/") {
        return journald::request_handler(storage, path, req, w);
    }
    if path.starts_with("/insert/datadog/") {
        return datadog::request_handler(storage, path, req, w);
    }

    false
}

#[cfg(test)]
pub(crate) mod testutil {
    //! Shared helpers for the per-module ingestion round-trip tests.

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use esl_logstorage::storage::{Storage, StorageConfig, StorageStats};

    /// Opens a fresh temp Storage backed by a unique directory.
    pub(crate) fn open_temp_storage(name: &str) -> Arc<Storage> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!("esl-insert-test-{name}-{n}"));
        let _ = std::fs::remove_dir_all(&path);
        Storage::must_open_storage(&path, &StorageConfig::default())
    }

    /// Flushes and returns the number of rows stored.
    pub(crate) fn rows_count(s: &Arc<Storage>) -> u64 {
        s.debug_flush();
        let mut stats = StorageStats::default();
        s.update_stats(&mut stats);
        stats.rows_count()
    }
}

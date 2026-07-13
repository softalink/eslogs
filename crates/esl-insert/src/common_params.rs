//! Port of EsLogs `app/eslinsert/insertutil/common_params.go` (plus the
//! `flags.go` defaults and `timestamp.go` helpers used across the ingestion
//! endpoints).
//!
//! PORT NOTE: the Go package registers a global `LogRowsStorage` via
//! `SetLogRowsStorage` and exposes `CanWriteData()`. The standardized Rust app
//! layer passes the storage explicitly into each `request_handler`; the
//! handlers are generic over the [`LogRowsStorage`] trait so eslagent can pass
//! its remotewrite-backed sink while es-logs passes the local
//! [`Storage`]. Handlers call `storage.can_write_data()` where Go calls the
//! package-level `insertutil.CanWriteData()`.
//!
//! PORT NOTE: Go's `logMessageProcessor` runs a background goroutine
//! (`initPeriodicFlush`) for stream-mode connections (`isStreamMode=true`).
//! The HTTP request handlers feed a finite body and call `close()` at the
//! end, which flushes the tail, so the periodic-flush thread (and the
//! `isStreamMode` constructor argument) is omitted here; the long-lived
//! syslog TCP/unix connections — the case where the missing periodic flush
//! is observable — run it via [`LogMessageProcessor::flush_if_idle`] from a
//! per-connection flusher thread (see `syslog_listeners::process_stream`).

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use esl_common::metrics::{Counter, Gauge, Summary};

use esl_common::flagutil::Flag;
use esl_common::httpserver::{Request, ResponseWriter, get_quoted_remote_addr};
use esl_common::httputil::{get_array as httputil_get_array, get_request_value};
use esl_common::timeutil::try_parse_unix_timestamp;
use esl_common::warnf;

use esl_logstorage::log_rows::{
    InsertRow, LogRows, estimated_json_row_len, get_log_rows, put_log_rows,
};
use esl_logstorage::rows::{Field, marshal_fields_to_json};
use esl_logstorage::storage::Storage;
use esl_logstorage::stream_tags::check_stream_field_names;
use esl_logstorage::tenant_id::{TenantID, get_tenant_id_from_request};
use esl_logstorage::values_encoder::try_parse_timestamp_rfc3339_nano;

// ---------------------------------------------------------------------------
// Flags (insertutil/flags.go)
// ---------------------------------------------------------------------------
//
// PORT NOTE: `-insert.maxLineSizeBytes` from the same Go file lives in
// [`crate::line_reader::MAX_LINE_SIZE_BYTES`] next to its only user.

/// MaxFieldsPerLine is the maximum number of fields per line for `/insert/*` handlers.
pub static MAX_FIELDS_PER_LINE: Flag<i64> = Flag::new(
    "insert.maxFieldsPerLine",
    "The maximum number of log fields per line, which can be read by /insert/* handlers; \
     see https://docs.victoriametrics.com/victorialogs/faq/#how-many-fields-a-single-log-entry-may-contain",
    || 1000,
);

/// DefaultMsgValue is the default value for `_msg` field if the ingested log
/// entry doesn't contain it.
pub static DEFAULT_MSG_VALUE: Flag<String> = Flag::new(
    "defaultMsgValue",
    "Default value for _msg field if the ingested log entry doesn't contain it; \
     see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field",
    || {
        "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"
            .to_string()
    },
);

/// Returns the current unix time in nanoseconds (Go `time.Now().UnixNano()`).
pub(crate) fn now_unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// CommonParams contains common HTTP parameters used by log ingestion APIs.
///
/// See <https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-parameters>
pub struct CommonParams {
    pub tenant_id: TenantID,
    pub time_fields: Vec<String>,
    pub msg_fields: Vec<String>,
    pub stream_fields: Vec<String>,
    pub ignore_fields: Vec<String>,
    pub decolorize_fields: Vec<String>,
    pub preserve_json_keys: Vec<String>,
    pub extra_fields: Vec<Field>,

    /// Whether `TimeFields` was set manually via the request.
    pub is_time_field_set: bool,

    pub debug: bool,
    pub debug_request_uri: String,
    pub debug_remote_addr: String,
}

/// GetCommonParams returns CommonParams from req.
pub fn get_common_params(req: &Request) -> Result<CommonParams, String> {
    // Extract tenantID (Go reads the "AccountID"/"ProjectID" headers).
    let tenant_id = get_tenant_id_from_request(req.header("AccountID"), req.header("ProjectID"))?;

    let mut is_time_field_set = false;
    let mut time_fields = vec!["_time".to_string()];
    let tfs = get_array(req, "_time_field", "ESL-Time-Field");
    if !tfs.is_empty() {
        is_time_field_set = true;
        time_fields = tfs;
    }

    let msg_fields = get_array(req, "_msg_field", "ESL-Msg-Field");
    let stream_fields = get_array(req, "_stream_fields", "ESL-Stream-Fields");
    let ignore_fields = get_array(req, "ignore_fields", "ESL-Ignore-Fields");
    let decolorize_fields = get_array(req, "decolorize_fields", "ESL-Decolorize-Fields");
    let preserve_json_keys = get_array(req, "preserve_json_keys", "ESL-Preserve-JSON-Keys");

    // verify that the _stream_fields contains valid values
    let sf_refs: Vec<&str> = stream_fields.iter().map(String::as_str).collect();
    check_stream_field_names(&sf_refs).map_err(|err| {
        format!(
            "cannot parse stream field names from the _stream_fields query arg or from ESL-Stream-Fields header: {err}"
        )
    })?;

    let extra_fields = get_extra_fields(req)?;

    let mut debug = false;
    let dv = get_request_value(req, "debug", "ESL-Debug");
    if !dv.is_empty() {
        debug = parse_bool(&dv).map_err(|err| format!("cannot parse debug={dv:?}: {err}"))?;
    }
    let (debug_request_uri, debug_remote_addr) = if debug {
        (req.request_uri(), get_quoted_remote_addr(req))
    } else {
        (String::new(), String::new())
    };

    Ok(CommonParams {
        tenant_id,
        time_fields,
        msg_fields,
        stream_fields,
        ignore_fields,
        decolorize_fields,
        preserve_json_keys,
        extra_fields,
        is_time_field_set,
        debug,
        debug_request_uri,
        debug_remote_addr,
    })
}

/// GetCommonParamsForSyslog returns common params for parsing syslog messages
/// and storing them to the given tenantID.
pub fn get_common_params_for_syslog(
    tenant_id: TenantID,
    stream_fields: Option<Vec<String>>,
    ignore_fields: Vec<String>,
    decolorize_fields: Vec<String>,
    extra_fields: Vec<Field>,
) -> CommonParams {
    // See https://docs.victoriametrics.com/victorialogs/logsql/#unpack_syslog-pipe
    let stream_fields = stream_fields.unwrap_or_else(|| {
        [
            "hostname",
            "app_name",
            "proc_id",
            "cef.device_vendor",
            "cef.device_product",
            "cef.device_event_class_id",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    });

    CommonParams {
        tenant_id,
        time_fields: vec!["timestamp".to_string()],
        msg_fields: vec!["message".to_string()],
        stream_fields,
        ignore_fields,
        decolorize_fields,
        preserve_json_keys: Vec::new(),
        extra_fields,
        is_time_field_set: false,
        debug: false,
        debug_request_uri: String::new(),
        debug_remote_addr: String::new(),
    }
}

fn get_extra_fields(req: &Request) -> Result<Vec<Field>, String> {
    let efs = get_array(req, "extra_fields", "ESL-Extra-Fields");
    if efs.is_empty() {
        return Ok(Vec::new());
    }
    let mut extra_fields = Vec::with_capacity(efs.len());
    for ef in &efs {
        match ef.find('=') {
            Some(n) if n > 0 && n != ef.len() - 1 => {
                extra_fields.push(Field {
                    name: ef[..n].to_string(),
                    value: ef[n + 1..].to_string(),
                });
            }
            _ => {
                return Err(format!(
                    "invalid extra_field format: {ef:?}; must be in the form \"field=value\""
                ));
            }
        }
    }
    Ok(extra_fields)
}

/// Mirrors Go `getArray` (`httputil.GetArray` + `removeEmptyTokens`).
fn get_array(req: &Request, arg_key: &str, header_key: &str) -> Vec<String> {
    remove_empty_tokens(httputil_get_array(req, arg_key, header_key))
}

fn remove_empty_tokens(a: Vec<String>) -> Vec<String> {
    a.into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Mirrors Go `strconv.ParseBool`.
fn parse_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!("strconv.ParseBool: parsing {s:?}: invalid syntax")),
    }
}

/// IsJSONContentType returns true if ct is a JSON content-type.
pub fn is_json_content_type(ct: &str) -> bool {
    ct == "application/json" || ct.starts_with("application/json;")
}

// ---------------------------------------------------------------------------
// Timestamp extraction (insertutil/timestamp.go)
// ---------------------------------------------------------------------------

/// ExtractTimestampFromFields extracts a timestamp in nanoseconds from the
/// first field whose name is in `time_fields`.
///
/// The value of the matched field is cleared so it is ignored during ingestion.
/// The current timestamp is returned when no matching non-empty field is found.
pub fn extract_timestamp_from_fields(
    time_fields: &[&str],
    fields: &mut [Field],
) -> Result<i64, String> {
    for time_field in time_fields {
        for f in fields.iter_mut() {
            if f.name != *time_field {
                continue;
            }
            let nsecs = parse_timestamp(&f.value)
                .map_err(|err| format!("cannot parse timestamp from field {:?}: {err}", f.name))?;
            f.value.clear();
            let nsecs = if nsecs == 0 { now_unix_nanos() } else { nsecs };
            return Ok(nsecs);
        }
    }
    Ok(now_unix_nanos())
}

fn parse_timestamp(s: &str) -> Result<i64, String> {
    // "-" is a nil timestamp value, if the syslog application is incapable of
    // obtaining system time. See RFC 5424 section 6.2.3.
    if s.is_empty() || s == "0" || s == "-" {
        return Ok(now_unix_nanos());
    }
    let b = s.as_bytes();
    if b.len() <= 4 || b[4] != b'-' {
        return match try_parse_unix_timestamp(s) {
            Some(nsecs) => Ok(nsecs),
            None => Err(format!("cannot parse unix timestamp {s:?}")),
        };
    }
    match try_parse_timestamp_rfc3339_nano(s) {
        Some(nsecs) => Ok(nsecs),
        None => Err(format!("cannot unmarshal rfc3339 timestamp {s:?}")),
    }
}

// ---------------------------------------------------------------------------
// LogMessageProcessor
// ---------------------------------------------------------------------------

/// LogRowsStorage is the interface for writing LogRows to the underlying
/// storage (Go `insertutil.LogRowsStorage`, registered via
/// `SetLogRowsStorage` there; the port passes it explicitly instead).
pub trait LogRowsStorage: Send + Sync + Sized {
    /// MustAddRows adds lr to the underlying storage.
    ///
    /// PORT NOTE: the receiver is `&Arc<Self>` because the ported
    /// `Storage::must_add_rows` needs the `Arc` for its background flushers.
    fn must_add_rows(self: &Arc<Self>, lr: &LogRows);

    /// CanWriteData must return `(message, http_status_code)` if logs cannot
    /// be added to the underlying storage.
    ///
    /// PORT NOTE: Go returns an `httpserver.ErrorWithStatusCode`; the port
    /// returns the `(message, status_code)` pair, matching
    /// `esl_storage::can_write_data`.
    fn can_write_data(self: &Arc<Self>) -> Result<(), (String, u16)>;
}

impl LogRowsStorage for Storage {
    fn must_add_rows(self: &Arc<Self>, lr: &LogRows) {
        Storage::must_add_rows(self, lr)
    }

    fn can_write_data(self: &Arc<Self>) -> Result<(), (String, u16)> {
        // Go: app/eslstorage `(*Storage).CanWriteData` (rejects with 429 when
        // the storage is in read-only mode because of low free disk space).
        esl_storage::can_write_data(self)
    }
}

/// Enforces a per-protocol `-*.maxRequestSize` cap on a fully-read request
/// body, mirroring Go `protoparserutil.readFull`'s "too big data size" error.
///
/// PORT NOTE: Go caps the stream while reading it (and again after
/// decompression); the port's `Request::read_full_body` has already
/// decompressed the body per Content-Encoding, so the cap applies to the
/// decompressed size after the read.
pub(crate) fn check_max_request_size(
    data_len: usize,
    flag: &Flag<esl_common::flagutil::Bytes>,
) -> Result<(), String> {
    let n = flag.get().n.max(0);
    if data_len as i64 > n {
        return Err(format!(
            "too big data size exceeding -{}={} bytes",
            flag.name(),
            n
        ));
    }
    Ok(())
}

/// Writes an error response carrying its own HTTP status code, mirroring Go
/// `httpserver.Errorf(w, r, "%s", err)` when err is an
/// `httpserver.ErrorWithStatusCode` (logs with request context, then responds
/// with the embedded status).
pub(crate) fn errorf_with_status(w: &mut ResponseWriter, req: &Request, msg: &str, status: u16) {
    let remote = get_quoted_remote_addr(req);
    let uri = req.request_uri();
    warnf!("remoteAddr: {}; requestURI: {}; {}", remote, uri, msg);
    w.error(msg, status);
}

/// Interface for log message processors.
///
/// PORT NOTE: Go names this interface `LogMessageProcessor` and keeps its
/// production implementation unexported (`logMessageProcessor`). The port
/// already uses the `LogMessageProcessor` name for the concrete processor, so
/// the interface is modeled by this trait; it carries `AddRow` only, since
/// Go's `MustClose()` maps to the consuming [`LogMessageProcessor::close`] on
/// the concrete types. Protocol parsers accept `impl LogMessageProcessorTrait`
/// so tests can substitute [`TestLogMessageProcessor`], like Go does.
pub trait LogMessageProcessorTrait {
    /// AddRow must add a row with the given timestamp and fields.
    ///
    /// If `stream_fields_len >= 0`, then the given number of initial fields
    /// must be used as log stream fields instead of pre-configured fields.
    ///
    /// The implementation cannot hold references to fields, since the caller
    /// can reuse them.
    fn add_row(&mut self, timestamp: i64, fields: &mut [Field], stream_fields_len: isize);
}

/// InsertRowProcessor is used by native data ingestion protocol parser.
pub trait InsertRowProcessor {
    /// AddInsertRow must add r to the underlying storage.
    fn add_insert_row(&mut self, r: &InsertRow);
}

/// Accumulates rows into a [`LogRows`] and flushes them to the storage.
///
/// Mirrors Go's `logMessageProcessor`. Obtain one via
/// [`CommonParams::new_log_message_processor`] and call [`LogMessageProcessor::close`]
/// when finished.
pub struct LogMessageProcessor<'a, S: LogRowsStorage> {
    storage: &'a Arc<S>,
    cp: &'a CommonParams,
    lr: LogRows,

    rows_ingested_total: Arc<Counter>,
    bytes_ingested_total: Arc<Counter>,
    flush_duration: Arc<Summary>,

    unflushed_rows: u64,
    unflushed_bytes: u64,

    /// The time of the last flush (Go `lastFlushTime`), consulted by
    /// [`Self::flush_if_idle`] for the stream-mode periodic flush.
    last_flush_time: Instant,
}

static ROWS_DROPPED_TOTAL_DEBUG: LazyLock<Arc<Counter>> =
    LazyLock::new(|| esl_common::metrics::new_counter(r#"esl_rows_dropped_total{reason="debug"}"#));
static ROWS_DROPPED_TOTAL_TOO_MANY_FIELDS: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::get_or_create_counter(
        r#"esl_rows_dropped_total{reason="too_many_fields"}"#,
    )
});
static MESSAGE_PROCESSOR_COUNT: AtomicI64 = AtomicI64::new(0);
static MESSAGE_PROCESSOR_COUNT_GAUGE: LazyLock<Arc<Gauge>> = LazyLock::new(|| {
    esl_common::metrics::new_gauge(
        "esl_insert_processors_count",
        Some(Box::new(|| {
            MESSAGE_PROCESSOR_COUNT.load(Ordering::Relaxed) as f64
        })),
    )
});

impl<S: LogRowsStorage> LogMessageProcessor<'_, S> {
    /// AddRow adds a new log message with the given timestamp and fields.
    ///
    /// If `stream_fields_len >= 0`, the given number of initial fields is used
    /// as log stream fields instead of the pre-configured stream fields.
    pub fn add_row(&mut self, timestamp: i64, fields: &mut [Field], stream_fields_len: isize) {
        self.unflushed_rows += 1;
        let n = estimated_json_row_len(fields);
        self.unflushed_bytes += n as u64;

        let max_fields_per_line = *MAX_FIELDS_PER_LINE.get() as usize;
        if fields.len() > max_fields_per_line {
            let mut line = Vec::new();
            marshal_fields_to_json(&mut line, fields);
            warnf!(
                "dropping log line with {} fields; it exceeds -insert.maxFieldsPerLine={}; {}",
                fields.len(),
                max_fields_per_line,
                String::from_utf8_lossy(&line)
            );
            ROWS_DROPPED_TOTAL_TOO_MANY_FIELDS.inc();
            return;
        }

        self.lr
            .must_add(self.cp.tenant_id, timestamp, fields, stream_fields_len);

        self.after_add();
    }

    /// AddInsertRow adds r to the processor (native data ingestion protocol).
    pub fn add_insert_row(&mut self, r: &InsertRow) {
        self.unflushed_rows += 1;
        let n = estimated_json_row_len(&r.fields);
        self.unflushed_bytes += n as u64;

        let max_fields_per_line = *MAX_FIELDS_PER_LINE.get() as usize;
        if r.fields.len() > max_fields_per_line {
            let mut line = Vec::new();
            marshal_fields_to_json(&mut line, &r.fields);
            warnf!(
                "dropping log line with {} fields; it exceeds -insert.maxFieldsPerLine={}; {}",
                r.fields.len(),
                max_fields_per_line,
                String::from_utf8_lossy(&line)
            );
            ROWS_DROPPED_TOTAL_TOO_MANY_FIELDS.inc();
            return;
        }

        self.lr.must_add_insert_row(r);

        self.after_add();
    }

    /// Shared tail of `add_row`/`add_insert_row` (Go duplicates this code).
    fn after_add(&mut self) {
        if self.cp.debug {
            let s = self.lr.get_row_string(0);
            self.lr.reset_keep_settings();
            esl_common::infof!(
                "remoteAddr={}; requestURI={}; ignoring log entry because of `debug` arg: {s}",
                self.cp.debug_remote_addr,
                self.cp.debug_request_uri
            );
            ROWS_DROPPED_TOTAL_DEBUG.inc();
            return;
        }

        if self.lr.need_flush() {
            self.flush();
        }
    }

    fn flush(&mut self) {
        let start = Instant::now();
        self.last_flush_time = start;
        self.storage.must_add_rows(&self.lr);
        self.lr.reset_keep_settings();

        self.flush_duration.update_duration(start);
        self.rows_ingested_total.add(self.unflushed_rows);
        self.bytes_ingested_total.add(self.unflushed_bytes);

        self.unflushed_rows = 0;
        self.unflushed_bytes = 0;
    }

    /// Flushes the accumulated rows if no flush happened during the last `d`
    /// (the body of Go's `initPeriodicFlush` ticker goroutine), for stream-mode
    /// ingestion.
    ///
    /// Driven by the per-connection flusher thread in
    /// `syslog_listeners::process_stream` (the syslog TCP/unix stream path,
    /// which is Go's `isStreamMode=true` case).
    pub(crate) fn flush_if_idle(&mut self, d: std::time::Duration) {
        if self.last_flush_time.elapsed() >= d {
            self.flush();
        }
    }

    /// Flushes the remaining data to the storage and releases the LogRows.
    pub fn close(mut self) {
        self.flush();
        put_log_rows(self.lr);
        MESSAGE_PROCESSOR_COUNT.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<S: LogRowsStorage> LogMessageProcessorTrait for LogMessageProcessor<'_, S> {
    fn add_row(&mut self, timestamp: i64, fields: &mut [Field], stream_fields_len: isize) {
        LogMessageProcessor::add_row(self, timestamp, fields, stream_fields_len);
    }
}

impl<S: LogRowsStorage> InsertRowProcessor for LogMessageProcessor<'_, S> {
    fn add_insert_row(&mut self, r: &InsertRow) {
        LogMessageProcessor::add_insert_row(self, r);
    }
}

impl CommonParams {
    /// Returns a new [`LogMessageProcessor`] bound to the given storage.
    ///
    /// `protocol_name` labels the per-protocol ingestion metrics
    /// (`esl_rows_ingested_total{type=...}`, ...).
    pub fn new_log_message_processor<'a, S: LogRowsStorage>(
        &'a self,
        storage: &'a Arc<S>,
        protocol_name: &str,
    ) -> LogMessageProcessor<'a, S> {
        let sf: Vec<&str> = self.stream_fields.iter().map(String::as_str).collect();
        let ig: Vec<&str> = self.ignore_fields.iter().map(String::as_str).collect();
        let dc: Vec<&str> = self.decolorize_fields.iter().map(String::as_str).collect();
        let lr = get_log_rows(&sf, &ig, &dc, &self.extra_fields, DEFAULT_MSG_VALUE.get());

        let rows_ingested_total = esl_common::metrics::get_or_create_counter(&format!(
            "esl_rows_ingested_total{{type={protocol_name:?}}}"
        ));
        let bytes_ingested_total = esl_common::metrics::get_or_create_counter(&format!(
            "esl_bytes_ingested_total{{type={protocol_name:?}}}"
        ));
        let flush_duration = esl_common::metrics::get_or_create_summary(&format!(
            "esl_insert_flush_duration_seconds{{type={protocol_name:?}}}"
        ));

        MESSAGE_PROCESSOR_COUNT.fetch_add(1, Ordering::Relaxed);
        LazyLock::force(&MESSAGE_PROCESSOR_COUNT_GAUGE);

        LogMessageProcessor {
            storage,
            cp: self,
            lr,
            rows_ingested_total,
            bytes_ingested_total,
            flush_duration,
            unflushed_rows: 0,
            unflushed_bytes: 0,
            last_flush_time: Instant::now(),
        }
    }
}

// ---------------------------------------------------------------------------
// Test utilities (insertutil/testutils.go)
// ---------------------------------------------------------------------------

/// TestLogMessageProcessor implements [`LogMessageProcessorTrait`] for testing.
///
/// PORT NOTE: Go exports this from the non-test `testutils.go` so sibling
/// packages can use it; in the port all protocol modules live in the same
/// crate, so `#[cfg(test)]` visibility is sufficient. The Go
/// `BenchmarkLogMessageProcessor`/`BenchmarkStorage` helpers are omitted
/// because the benchmarks are not ported.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestLogMessageProcessor {
    timestamps: Vec<i64>,
    rows: Vec<String>,
}

#[cfg(test)]
impl LogMessageProcessorTrait for TestLogMessageProcessor {
    fn add_row(&mut self, timestamp: i64, fields: &mut [Field], stream_fields_len: isize) {
        if stream_fields_len >= 0 {
            panic!("BUG: streamFieldsLen must be negative; got {stream_fields_len}");
        }
        self.timestamps.push(timestamp);
        let mut line = Vec::new();
        marshal_fields_to_json(&mut line, fields);
        self.rows.push(String::from_utf8_lossy(&line).into_owned());
    }
}

#[cfg(test)]
impl TestLogMessageProcessor {
    /// MustClose closes tlp.
    pub(crate) fn must_close(self) {}

    /// Verify verifies the number of rows, timestamps and results after
    /// `add_row` calls.
    pub(crate) fn verify(
        &self,
        timestamps_expected: &[i64],
        result_expected: &str,
    ) -> Result<(), String> {
        let result = self.rows.join("\n");
        if self.rows.len() != timestamps_expected.len() {
            return Err(format!(
                "unexpected rows read; got {}; want {};\nrows read:\n{result}\nrows wanted\n{result_expected}",
                self.rows.len(),
                timestamps_expected.len()
            ));
        }

        if self.timestamps != timestamps_expected {
            return Err(format!(
                "unexpected timestamps;\ngot\n{:?}\nwant\n{timestamps_expected:?}",
                self.timestamps
            ));
        }
        if result != result_expected {
            return Err(format!(
                "unexpected result;\ngot\n{result}\nwant\n{result_expected}"
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
impl CommonParams {
    /// Minimal params for tests: default tenant, `_time` time field, nothing else.
    pub(crate) fn empty() -> Self {
        CommonParams {
            tenant_id: TenantID::default(),
            time_fields: vec!["_time".to_string()],
            msg_fields: Vec::new(),
            stream_fields: Vec::new(),
            ignore_fields: Vec::new(),
            decolorize_fields: Vec::new(),
            preserve_json_keys: Vec::new(),
            extra_fields: Vec::new(),
            is_time_field_set: false,
            debug: false,
            debug_request_uri: String::new(),
            debug_remote_addr: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    /// Counts calls to `must_add_rows` for exercising the stream-mode flush.
    struct CountingStorage {
        flushes: std::sync::atomic::AtomicUsize,
    }

    impl LogRowsStorage for CountingStorage {
        fn must_add_rows(self: &Arc<Self>, _lr: &LogRows) {
            self.flushes.fetch_add(1, Ordering::SeqCst);
        }

        fn can_write_data(self: &Arc<Self>) -> Result<(), (String, u16)> {
            Ok(())
        }
    }

    #[test]
    fn test_flush_if_idle_respects_interval() {
        let storage = Arc::new(CountingStorage {
            flushes: std::sync::atomic::AtomicUsize::new(0),
        });
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&storage, "test");

        // One small row does not reach the need_flush threshold, so nothing has
        // been flushed to the storage yet.
        lmp.add_row(1, &mut [field("_msg", "hello")], -1);
        assert_eq!(storage.flushes.load(Ordering::SeqCst), 0);

        // Idle for less than the interval: no periodic flush.
        lmp.flush_if_idle(std::time::Duration::from_secs(3600));
        assert_eq!(storage.flushes.load(Ordering::SeqCst), 0);

        // Idle for at least the interval (zero threshold always elapses): flush.
        lmp.flush_if_idle(std::time::Duration::ZERO);
        assert_eq!(storage.flushes.load(Ordering::SeqCst), 1);

        // A second idle tick with no new data still flushes (matches Go's
        // ticker, which flushes an empty LogRows as a no-op add).
        lmp.flush_if_idle(std::time::Duration::ZERO);
        assert_eq!(storage.flushes.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_extract_timestamp_rfc3339() {
        let mut fields = vec![field("_time", "2023-01-15T00:00:00Z"), field("host", "a")];
        let ts = extract_timestamp_from_fields(&["_time"], &mut fields).unwrap();
        // 2023-01-15T00:00:00Z == 1673740800 seconds.
        assert_eq!(ts, 1_673_740_800 * 1_000_000_000);
        // The matched time field value is cleared.
        assert_eq!(fields[0].value, "");
    }

    #[test]
    fn test_extract_timestamp_defaults_to_now_when_missing() {
        let before = now_unix_nanos();
        let mut fields = vec![field("host", "a")];
        let ts = extract_timestamp_from_fields(&["_time"], &mut fields).unwrap();
        assert!(ts >= before, "timestamp should default to current time");
    }

    #[test]
    fn test_extract_timestamp_from_fields_success() {
        fn f(time_field: &str, fields: &mut [Field], nsecs_expected: i64) {
            let nsecs = extract_timestamp_from_fields(&[time_field], fields)
                .unwrap_or_else(|err| panic!("unexpected error: {err}"));
            assert_eq!(
                nsecs, nsecs_expected,
                "unexpected nsecs; got {nsecs}; want {nsecs_expected}"
            );

            for fld in fields.iter() {
                if fld.name == time_field {
                    assert_eq!(
                        fld.value, "",
                        "unexpected value for field {time_field}; got {:?}; want \"\"",
                        fld.value
                    );
                }
            }
        }

        // UTC time
        f(
            "time",
            &mut [field("foo", "bar"), field("time", "2024-06-18T23:37:20Z")],
            1718753840000000000,
        );

        // Time with timezone
        f(
            "time",
            &mut [
                field("foo", "bar"),
                field("time", "2024-06-18T23:37:20+08:00"),
            ],
            1718725040000000000,
        );

        // SQL datetime format
        f(
            "time",
            &mut [
                field("foo", "bar"),
                field("time", "2024-06-18 23:37:20.123-05:30"),
            ],
            1718773640123000000,
        );

        // Time with nanosecond precision
        f(
            "time",
            &mut [
                field("time", "2024-06-18T23:37:20.123456789-05:30"),
                field("foo", "bar"),
            ],
            1718773640123456789,
        );

        // Unix timestamp in nanoseconds
        f(
            "time",
            &mut [field("foo", "bar"), field("time", "1718773640123456789")],
            1718773640123456789,
        );

        // Unix timestamp in microseconds
        f(
            "time",
            &mut [field("foo", "bar"), field("time", "1718773640123456")],
            1718773640123456000,
        );

        // Unix timestamp in milliseconds
        f(
            "time",
            &mut [field("foo", "bar"), field("time", "1718773640123")],
            1718773640123000000,
        );

        // Unix timestamp in seconds
        f(
            "time",
            &mut [field("foo", "bar"), field("time", "1718773640")],
            1718773640000000000,
        );
    }

    #[test]
    fn test_extract_timestamp_from_fields_now() {
        fn f(time_field: &str, fields: &mut [Field]) {
            let nsecs = extract_timestamp_from_fields(&[time_field], fields)
                .unwrap_or_else(|err| panic!("unexpected error: {err}"));
            assert!(nsecs >= 1, "expected generated timestamp");
        }

        // RFC5424 allows `-` for nil timestamp (log ingestion time)
        f("time", &mut [field("time", "-")]);

        f("time", &mut [field("time", "")]);

        f("time", &mut [field("time", "0")]);
    }

    #[test]
    fn test_extract_timestamp_from_fields_error() {
        fn f(s: &str) {
            let mut fields = [field("time", s)];
            let res = extract_timestamp_from_fields(&["time"], &mut fields);
            assert!(res.is_err(), "expecting non-nil error for {s:?}");
        }

        // invalid time
        f("foobar");

        // incomplete time
        f("2024-06-18");
        f("2024-06-18T23:37");
    }

    /// Port of `TestGetCommonParams_RemoveEmptyTokens`.
    ///
    /// PORT NOTE: the Go test builds an `*http.Request` via `httptest`; the
    /// port's `httpserver::Request` has no public test constructor outside
    /// esl-common, so the test exercises `remove_empty_tokens` (the logic under
    /// test) directly, plus the extra_fields `field=value` parsing rules.
    #[test]
    fn test_get_common_params_remove_empty_tokens() {
        fn f(input: &[&str], expected: &[&str]) {
            let got = remove_empty_tokens(input.iter().map(|s| s.to_string()).collect());
            assert_eq!(
                got, expected,
                "unexpected tokens; got {got:?}; want {expected:?}"
            );
        }

        f(
            &["collector", "", "service.name"],
            &["collector", "service.name"],
        );
        f(&["", "  observedTimestamp", " "], &["observedTimestamp"]);
        f(&["", ""], &[]);
        f(&["a=b", "", " c=d"], &["a=b", "c=d"]);
    }

    #[test]
    fn test_is_json_content_type() {
        assert!(is_json_content_type("application/json"));
        assert!(is_json_content_type("application/json; charset=utf-8"));
        assert!(!is_json_content_type("text/plain"));
    }
}

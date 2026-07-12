//! Port of EsLogs `app/eslselect/internalselect/internalselect.go`: the
//! server side of the cluster select protocol, serving `/internal/select/*`
//! and `/internal/delete/*` for the `esl-storage` netselect client.
//!
//! PORT NOTE — concurrency limiter: Go wraps `requestHandler` in a
//! `concurrencyLimitCh` gate sized by `-internalselect.maxConcurrentRequests`
//! (with `Init`/`Stop` lifecycle) and a `esl_concurrent_internalselect_requests_wait_duration`
//! summary. The limiter is dropped (requests are served directly, bounded by
//! the httpserver worker pool), so the wait-duration summary has nothing to
//! measure and is not registered. The per-path `esl_http_requests_total` /
//! `esl_http_errors_total` / `esl_http_request_duration_seconds` metrics are
//! ported.
//!
//! PORT NOTE — `QueryContext`: Go bundles ctx cancellation, `QueryStats`,
//! `AllowPartialResponse` and `HiddenFieldsFilters` into a
//! `logstorage.QueryContext` passed to `eslstorage.RunQuery`/`Get*`. The ported
//! `Storage::run_query` takes tenant_ids/query explicitly and does not
//! accumulate `QueryStats` (see the PORT NOTE on `Storage::run_query`), so the
//! trailing query-stats block carries zero counters plus the measured
//! `QueryDurationNsecs`. Ctx cancellation IS ported: each query/Get* handler
//! registers a disconnect-watcher token (`ResponseWriter::watch_disconnect`)
//! that cancels the running query when the netselect client goes away, and
//! canceled queries produce no response body (the dispatcher suppresses
//! `errorf` for them). `allow_partial_response` and `hidden_fields_filters`
//! are parsed and validated exactly like Go (their absence is an error) but are
//! not consumed by the local query execution: `allow_partial_response` only
//! affects multi-node fan-out (a client-side concern here) and
//! `hidden_fields_filters` is not carried by the ported query surface (the
//! netselect client always sends `null`).
//!
//! PORT NOTE — response streaming: Go streams length-prefixed frames to the
//! client as soon as a per-worker buffer exceeds 1MiB and aborts on write
//! errors via `errGlobal`. The ported `ResponseWriter` buffers the whole
//! response, so frames are accumulated in memory and flushed at the end;
//! mid-stream write errors (Go `errGlobal`/`sendBuf` failures) cannot occur.
//!
//! PORT NOTE — `UpdatePerQueryStatsMetrics` (Go `defer` in every handler)
//! is not called: the ported queries don't accumulate `QueryStats` (see
//! above), so the update would only record zeros. The
//! `esl_storage_per_query_*` histograms are still registered (at zero) by
//! `esl_storage::query_stats::init`, matching the series Go exposes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use esl_common::encoding as vlencoding;
use esl_common::encoding::zstd;
use esl_common::httpserver::{Request, ResponseWriter};

use esl_logstorage::delete_task::marshal_delete_tasks_to_json;
use esl_logstorage::parser::{ParseFilter, ParseQueryAtTimestamp, Query};
use esl_logstorage::query_stats::QueryStats;
use esl_logstorage::storage::Storage;
use esl_logstorage::storage_search::{DataBlock, ValueWithHits, WriteDataBlockFn};
use esl_logstorage::tenant_id::{TenantID, unmarshal_tenant_ids_from_json};

// The expected protocol versions are shared with the netselect client
// (Go internalselect also imports them from `app/eslstorage/netselect`).
use esl_storage::netselect::{
    DELETE_ACTIVE_TASKS_PROTOCOL_VERSION, DELETE_RUN_TASK_PROTOCOL_VERSION,
    DELETE_STOP_TASK_PROTOCOL_VERSION, FIELD_NAMES_PROTOCOL_VERSION, FIELD_VALUES_PROTOCOL_VERSION,
    QUERY_PROTOCOL_VERSION, STREAM_FIELD_NAMES_PROTOCOL_VERSION,
    STREAM_FIELD_VALUES_PROTOCOL_VERSION, STREAM_IDS_PROTOCOL_VERSION, STREAMS_PROTOCOL_VERSION,
};

/// The size threshold for flushing a per-worker buffer to the client
/// (Go: `len(bb.B) < 1024*1024` fast path in `writeBlock`).
const SEND_BUF_THRESHOLD: usize = 1024 * 1024;

/// Handles `/internal/select/*` and `/internal/delete/*` requests
/// (Go `internalselect.RequestHandler`).
///
/// Returns `true` if `req.path()` was routed here (even when the request
/// failed), and `false` when the path belongs to another handler.
pub fn request_handler(storage: &Arc<Storage>, req: &mut Request, w: &mut ResponseWriter) -> bool {
    let path = req.path().replace("//", "/");
    if !path.starts_with("/internal/select/") && !path.starts_with("/internal/delete/") {
        return false;
    }

    // Parse request before obtaining the request args from it in order to
    // catch parse errors, which are silently skipped at r.FormValue() calls
    // inside the request handlers executed below.
    //
    // See https://github.com/EsLogs/EsLogs/issues/1462
    //
    // PORT NOTE: Go reports the parse error via httpserver.Errorf and then
    // *continues* into the handler (there is no `return` upstream); the port
    // mirrors that control flow with an empty form, so the handler fails again
    // on the missing args and the last error wins in the buffered response.
    let form = match parse_request(req) {
        Ok(form) => form,
        Err(err) => {
            w.errorf(
                req,
                &format!("cannot parse request to {:?}: {err}", req.request_uri()),
            );
            Form::default()
        }
    };

    let start_time = Instant::now();
    let known_path = matches!(
        path.as_str(),
        "/internal/select/query"
            | "/internal/select/field_names"
            | "/internal/select/field_values"
            | "/internal/select/stream_field_names"
            | "/internal/select/stream_field_values"
            | "/internal/select/streams"
            | "/internal/select/stream_ids"
            | "/internal/select/tenant_ids"
            | "/internal/delete/run_task"
            | "/internal/delete/stop_task"
            | "/internal/delete/active_tasks"
    );
    if known_path {
        esl_common::metrics::get_or_create_counter(&format!(
            "esl_http_requests_total{{path={path:?}}}"
        ))
        .inc();
    }

    let result = match path.as_str() {
        "/internal/select/query" => process_query_request(storage, req, &form, w),
        "/internal/select/field_names" => process_field_names_request(storage, req, &form, w),
        "/internal/select/field_values" => process_field_values_request(storage, req, &form, w),
        "/internal/select/stream_field_names" => {
            process_stream_field_names_request(storage, req, &form, w)
        }
        "/internal/select/stream_field_values" => {
            process_stream_field_values_request(storage, req, &form, w)
        }
        "/internal/select/streams" => process_streams_request(storage, req, &form, w),
        "/internal/select/stream_ids" => process_stream_ids_request(storage, req, &form, w),
        "/internal/select/tenant_ids" => process_tenant_ids_request(storage, req, &form, w),

        "/internal/delete/run_task" => process_delete_run_task(storage, req, &form),
        "/internal/delete/stop_task" => process_delete_stop_task(storage, req, &form),
        "/internal/delete/active_tasks" => process_delete_active_tasks(storage, req, &form, w),

        _ => {
            w.errorf(req, &format!("unsupported endpoint requested: {path}"));
            return true;
        }
    };

    if let Err(err) = result {
        esl_common::metrics::get_or_create_counter(&format!(
            "esl_http_errors_total{{path={path:?}}}"
        ))
        .inc();
        if esl_logstorage::storage_search::is_query_canceled_error(&err) {
            // The client disconnected mid-query: there is nobody to respond
            // to (Go: writes to the closed conn are dropped).
        } else {
            w.errorf(req, &err);
        }
        // The return is skipped intentionally in order to track the duration
        // of failed queries.
    }
    esl_common::metrics::get_or_create_summary(&format!(
        "esl_http_request_duration_seconds{{path={path:?}}}"
    ))
    .update_duration(start_time);
    true
}

// ---------------------------------------------------------------------------
// Request parsing (Go parseRequest + r.FormValue)
// ---------------------------------------------------------------------------

/// The parsed multipart form args of a request (Go `r.ParseMultipartForm`).
///
/// The netselect client always sends the request args as
/// `multipart/form-data` in order to avoid the 10MB limit on
/// `application/x-www-form-urlencoded` request bodies.
/// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1462
#[derive(Default)]
struct Form {
    values: HashMap<String, String>,
}

impl Form {
    /// Returns the first form value for `key`, mirroring Go `r.FormValue`:
    /// multipart body values take precedence over urlencoded-body/query values
    /// (the latter are already parsed by the httpserver).
    fn value<'a>(&'a self, req: &'a Request, key: &str) -> &'a str {
        if let Some(v) = self.values.get(key) {
            return v;
        }
        req.form_value(key)
    }
}

/// Port of Go `parseRequest`.
///
/// PORT NOTE: Go bounds `ParseMultipartForm` by 10% of the allowed memory;
/// the ported httpserver already buffers request bodies without that knob, so
/// the limit is dropped.
fn parse_request(req: &mut Request) -> Result<Form, String> {
    let ct = req.content_type().to_string();
    if !ct.starts_with("multipart/form-data;") {
        // Non-multipart args (urlencoded bodies and the URL query string) are
        // already parsed by the ported httpserver (Go `r.ParseForm`).
        return Ok(Form::default());
    }

    let boundary = get_multipart_boundary(&ct)
        .ok_or("cannot parse multipart-encoded request args: missing boundary".to_string())?;
    let body = req
        .read_full_body()
        .map_err(|err| format!("cannot parse multipart-encoded request args: {err}"))?;
    let values = parse_multipart_form(&body, &boundary)
        .map_err(|err| format!("cannot parse multipart-encoded request args: {err}"))?;
    Ok(Form { values })
}

/// Extracts the `boundary` parameter from a `multipart/form-data` Content-Type.
fn get_multipart_boundary(content_type: &str) -> Option<String> {
    for param in content_type.split(';').skip(1) {
        let (k, v) = param.trim().split_once('=')?;
        if k.trim().eq_ignore_ascii_case("boundary") {
            let v = v.trim();
            let v = v
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(v);
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Minimal `multipart/form-data` parser covering the request bodies produced
/// by `mime/multipart.Writer` (and the ported
/// `esl_storage::http_client::new_multipart_request_body`): text parts with a
/// `Content-Disposition: form-data; name="..."` header.
fn parse_multipart_form(body: &[u8], boundary: &str) -> Result<HashMap<String, String>, String> {
    let dash_boundary = format!("--{boundary}").into_bytes();
    let mut values = HashMap::new();

    let mut pos = find_subslice(body, &dash_boundary).ok_or("missing opening boundary")?
        + dash_boundary.len();
    loop {
        let rest = &body[pos..];
        if rest.starts_with(b"--") {
            // The closing `--boundary--` delimiter.
            return Ok(values);
        }
        if !rest.starts_with(b"\r\n") {
            return Err("missing CRLF after boundary".to_string());
        }
        pos += 2;

        let hdr_len =
            find_subslice(&body[pos..], b"\r\n\r\n").ok_or("missing part header terminator")?;
        let name = parse_form_data_name(&body[pos..pos + hdr_len])?;
        pos += hdr_len + 4;

        let mut value_terminator = b"\r\n".to_vec();
        value_terminator.extend_from_slice(&dash_boundary);
        let value_len = find_subslice(&body[pos..], &value_terminator)
            .ok_or("missing closing boundary for part")?;
        let value = std::str::from_utf8(&body[pos..pos + value_len])
            .map_err(|_| format!("part {name:?} value is not valid UTF-8"))?;
        // Go FormValue returns the first value for duplicate keys.
        values.entry(name).or_insert_with(|| value.to_string());
        pos += value_len + value_terminator.len();
    }
}

/// Extracts the `name` from a part's `Content-Disposition: form-data` header,
/// unescaping the quoted-string `\"` and `\\` escapes produced by
/// `mime/multipart.Writer`.
fn parse_form_data_name(headers: &[u8]) -> Result<String, String> {
    let headers = std::str::from_utf8(headers).map_err(|_| "part headers are not valid UTF-8")?;
    for line in headers.split("\r\n") {
        let Some((key, rest)) = line.split_once(':') else {
            continue;
        };
        if !key.trim().eq_ignore_ascii_case("content-disposition") {
            continue;
        }
        let idx = rest
            .find("name=\"")
            .ok_or("missing name in Content-Disposition header")?;
        let mut out = String::new();
        let mut chars = rest[idx + 6..].chars();
        loop {
            match chars.next() {
                Some('\\') => match chars.next() {
                    Some(c) => out.push(c),
                    None => return Err("unterminated escape in part name".to_string()),
                },
                Some('"') => return Ok(out),
                Some(c) => out.push(c),
                None => return Err("unterminated quoted part name".to_string()),
            }
        }
    }
    Err("missing Content-Disposition form-data header in multipart part".to_string())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---------------------------------------------------------------------------
// /internal/select/* handlers
// ---------------------------------------------------------------------------

/// Port of Go `processQueryRequest`: streams the query result blocks in the
/// framed wire format decoded by `netselect::StorageNode::runQuery`
/// (`[8-byte len][optionally zstd-compressed frame]`, where each frame is a
/// sequence of `0`-marked data blocks and `1`-marked query-stats blocks).
fn process_query_request(
    storage: &Arc<Storage>,
    req: &Request,
    form: &Form,
    w: &mut ResponseWriter,
) -> Result<(), String> {
    let start_time = Instant::now();
    let cp = get_common_params(req, form, QUERY_PROTOCOL_VERSION)?;

    w.set_header("Content-Type", "application/octet-stream");

    // The framed output stream (Go: the locked writes to `w` in `sendBuf`).
    let out: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    // Per-worker buffers (Go: `atomicutil.Slice[bytesutil.ByteBuffer]`).
    let bufs: Arc<Mutex<HashMap<usize, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
    let disable_compression = cp.disable_compression;

    let out_wb = Arc::clone(&out);
    let bufs_wb = Arc::clone(&bufs);
    let write_block: WriteDataBlockFn = Arc::new(move |worker_id, db: &mut DataBlock| {
        let mut bufs = bufs_wb.lock().unwrap();
        let bb = bufs.entry(worker_id).or_default();

        // Write the marker of a regular data block.
        bb.push(0);

        // Marshal the data block.
        db.marshal(bb);

        if bb.len() < SEND_BUF_THRESHOLD {
            // Fast path - the bb is too small to be sent to the client yet.
            return;
        }

        // Slow path - the bb must be sent to the client.
        send_buf(bb, disable_compression, &out_wb);
    });

    // PORT NOTE: the ported `Storage::run_query` does not accumulate
    // QueryStats (see the module docs), so the query-stats block below carries
    // zero counters plus QueryDurationNsecs. Context cancellation from Go's
    // `*QueryContext` IS ported: the disconnect-watcher token below cancels
    // the query when the netselect client goes away, like ctx.Done().
    let qs = QueryStats::default();
    let cancel = w.watch_disconnect();
    storage.run_query_with_cancel(&cp.tenant_ids, &cp.query, write_block, cancel.as_deref())?;
    drop(cancel);

    // Send the remaining data.
    let mut bufs = bufs.lock().unwrap();
    for bb in bufs.values_mut() {
        send_buf(bb, disable_compression, &out);
    }

    // Send the query stats block.
    let mut bb = Vec::new();
    // Write the marker of query stats block.
    bb.push(1);
    // Marshal the block itself.
    marshal_query_stats_block(&mut bb, &qs, elapsed_nsecs(start_time));
    send_buf(&mut bb, disable_compression, &out);

    w.write_bytes(&out.lock().unwrap());
    Ok(())
}

/// Port of Go `sendBuf`: frames `bb` as `[8-byte len][data]` into `out`,
/// zstd-compressing the data unless compression is disabled, and resets `bb`.
fn send_buf(bb: &mut Vec<u8>, disable_compression: bool, out: &Mutex<Vec<u8>>) {
    if bb.is_empty() {
        return;
    }

    let mut compressed = Vec::new();
    let data: &[u8] = if !disable_compression {
        zstd::compress_level(&mut compressed, bb, 1);
        &compressed
    } else {
        bb
    };

    let mut out = out.lock().unwrap();
    vlencoding::marshal_uint64(&mut out, data.len() as u64);
    out.extend_from_slice(data);
    drop(out);

    // Reset the sent buf.
    bb.clear();
}

/// Port of Go `processFieldNamesRequest`.
fn process_field_names_request(
    storage: &Arc<Storage>,
    req: &Request,
    form: &Form,
    w: &mut ResponseWriter,
) -> Result<(), String> {
    let start_time = Instant::now();
    let cp = get_common_params(req, form, FIELD_NAMES_PROTOCOL_VERSION)?;

    let filter = form.value(req, "filter");

    let cancel = w.watch_disconnect();
    let field_names = storage
        .get_field_names(&cp.tenant_ids, &cp.query, filter, cancel.as_deref())
        .map_err(|err| format!("cannot obtain field names: {err}"))?;
    drop(cancel);

    write_values_with_hits(
        w,
        &QueryStats::default(),
        elapsed_nsecs(start_time),
        &field_names,
        cp.disable_compression,
    )
}

/// Port of Go `processFieldValuesRequest`.
fn process_field_values_request(
    storage: &Arc<Storage>,
    req: &Request,
    form: &Form,
    w: &mut ResponseWriter,
) -> Result<(), String> {
    let start_time = Instant::now();
    let cp = get_common_params(req, form, FIELD_VALUES_PROTOCOL_VERSION)?;

    let field_name = form.value(req, "field");
    let filter = form.value(req, "filter");

    let limit = get_int64_from_request(req, form, "limit")?;

    let cancel = w.watch_disconnect();
    let field_values = storage
        .get_field_values(
            &cp.tenant_ids,
            &cp.query,
            field_name,
            filter,
            limit as u64,
            cancel.as_deref(),
        )
        .map_err(|err| format!("cannot obtain field values: {err}"))?;
    drop(cancel);

    write_values_with_hits(
        w,
        &QueryStats::default(),
        elapsed_nsecs(start_time),
        &field_values,
        cp.disable_compression,
    )
}

/// Port of Go `processStreamFieldNamesRequest`.
fn process_stream_field_names_request(
    storage: &Arc<Storage>,
    req: &Request,
    form: &Form,
    w: &mut ResponseWriter,
) -> Result<(), String> {
    let start_time = Instant::now();
    let cp = get_common_params(req, form, STREAM_FIELD_NAMES_PROTOCOL_VERSION)?;

    let filter = form.value(req, "filter");

    let cancel = w.watch_disconnect();
    let field_names = storage
        .get_stream_field_names(&cp.tenant_ids, &cp.query, filter, cancel.as_deref())
        .map_err(|err| format!("cannot obtain stream field names: {err}"))?;
    drop(cancel);

    write_values_with_hits(
        w,
        &QueryStats::default(),
        elapsed_nsecs(start_time),
        &field_names,
        cp.disable_compression,
    )
}

/// Port of Go `processStreamFieldValuesRequest`.
fn process_stream_field_values_request(
    storage: &Arc<Storage>,
    req: &Request,
    form: &Form,
    w: &mut ResponseWriter,
) -> Result<(), String> {
    let start_time = Instant::now();
    let cp = get_common_params(req, form, STREAM_FIELD_VALUES_PROTOCOL_VERSION)?;

    let field_name = form.value(req, "field");
    let filter = form.value(req, "filter");

    let limit = get_int64_from_request(req, form, "limit")?;

    let cancel = w.watch_disconnect();
    let field_values = storage
        .get_stream_field_values(
            &cp.tenant_ids,
            &cp.query,
            field_name,
            filter,
            limit as u64,
            cancel.as_deref(),
        )
        .map_err(|err| format!("cannot obtain stream field values: {err}"))?;
    drop(cancel);

    write_values_with_hits(
        w,
        &QueryStats::default(),
        elapsed_nsecs(start_time),
        &field_values,
        cp.disable_compression,
    )
}

/// Port of Go `processStreamsRequest`.
fn process_streams_request(
    storage: &Arc<Storage>,
    req: &Request,
    form: &Form,
    w: &mut ResponseWriter,
) -> Result<(), String> {
    let start_time = Instant::now();
    let cp = get_common_params(req, form, STREAMS_PROTOCOL_VERSION)?;

    let limit = get_int64_from_request(req, form, "limit")?;

    let cancel = w.watch_disconnect();
    let streams = storage
        .get_streams(&cp.tenant_ids, &cp.query, limit as u64, cancel.as_deref())
        .map_err(|err| format!("cannot obtain streams: {err}"))?;
    drop(cancel);

    write_values_with_hits(
        w,
        &QueryStats::default(),
        elapsed_nsecs(start_time),
        &streams,
        cp.disable_compression,
    )
}

/// Port of Go `processStreamIDsRequest`.
fn process_stream_ids_request(
    storage: &Arc<Storage>,
    req: &Request,
    form: &Form,
    w: &mut ResponseWriter,
) -> Result<(), String> {
    let start_time = Instant::now();
    let cp = get_common_params(req, form, STREAM_IDS_PROTOCOL_VERSION)?;

    let limit = get_int64_from_request(req, form, "limit")?;

    let cancel = w.watch_disconnect();
    let stream_ids = storage
        .get_stream_ids(&cp.tenant_ids, &cp.query, limit as u64, cancel.as_deref())
        .map_err(|err| format!("cannot obtain streams: {err}"))?;
    drop(cancel);

    write_values_with_hits(
        w,
        &QueryStats::default(),
        elapsed_nsecs(start_time),
        &stream_ids,
        cp.disable_compression,
    )
}

/// Port of Go `processTenantIDsRequest`.
///
/// PORT NOTE: blocked on `Storage::get_tenant_ids` (Go `Storage.GetTenantIDs`
/// in `lib/logstorage/storage_search.go`, which scans the per-partition
/// IndexDB).
fn process_tenant_ids_request(
    storage: &Arc<Storage>,
    req: &Request,
    form: &Form,
    w: &mut ResponseWriter,
) -> Result<(), String> {
    let start = get_int64_from_request(req, form, "start")?;
    let end = get_int64_from_request(req, form, "end")?;
    let tenant_ids = storage
        .get_tenant_ids(start, end)
        .map_err(|e| format!("cannot obtain tenant IDs: {e}"))?;
    let data = esl_logstorage::tenant_id::marshal_tenant_ids_to_json(&tenant_ids);
    w.set_header("Content-Type", "application/json");
    w.write_bytes(&data);
    Ok(())
}

// ---------------------------------------------------------------------------
// /internal/delete/* handlers
// ---------------------------------------------------------------------------

/// Port of Go `processDeleteRunTask`.
fn process_delete_run_task(
    storage: &Arc<Storage>,
    req: &Request,
    form: &Form,
) -> Result<(), String> {
    check_protocol_version(req, form, DELETE_RUN_TASK_PROTOCOL_VERSION)?;

    // Parse query args
    let task_id = form.value(req, "task_id");
    if task_id.is_empty() {
        return Err("missing task_id arg".to_string());
    }

    let timestamp = get_int64_from_request(req, form, "timestamp")?;

    let tenant_ids_str = form.value(req, "tenant_ids");
    let tenant_ids = unmarshal_tenant_ids_from_json(tenant_ids_str.as_bytes())
        .map_err(|err| format!("cannot unmarshal tenant_ids={tenant_ids_str:?}: {err}"))?;

    let f_str = form.value(req, "filter");
    let f =
        ParseFilter(f_str).map_err(|err| format!("cannot unmarshal filter={f_str:?}: {err}"))?;

    // Execute the delete task.
    //
    // PORT NOTE: the ported `Storage::delete_run_task` takes the stringified
    // filter (Go `newDeleteTask` stores `f.String()`; see its PORT NOTE).
    storage.delete_run_task(task_id, timestamp, tenant_ids, &f.to_string())
}

/// Port of Go `processDeleteStopTask`.
fn process_delete_stop_task(
    storage: &Arc<Storage>,
    req: &Request,
    form: &Form,
) -> Result<(), String> {
    check_protocol_version(req, form, DELETE_STOP_TASK_PROTOCOL_VERSION)?;

    let task_id = form.value(req, "task_id");
    if task_id.is_empty() {
        return Err("missing task_id arg".to_string());
    }

    storage.delete_stop_task(task_id)
}

/// Port of Go `processDeleteActiveTasks`.
fn process_delete_active_tasks(
    storage: &Arc<Storage>,
    req: &Request,
    form: &Form,
    w: &mut ResponseWriter,
) -> Result<(), String> {
    check_protocol_version(req, form, DELETE_ACTIVE_TASKS_PROTOCOL_VERSION)?;

    let tasks = storage.delete_active_tasks();

    let data = marshal_delete_tasks_to_json(&tasks);

    w.set_header("Content-Type", "application/json");
    w.write_bytes(&data);

    Ok(())
}

// ---------------------------------------------------------------------------
// Common request params (Go commonParams)
// ---------------------------------------------------------------------------

/// Port of Go `commonParams`.
struct CommonParams {
    tenant_ids: Vec<TenantID>,
    query: Query,

    /// Whether to disable compression of the response sent to the eslselect.
    disable_compression: bool,

    /// Whether to allow partial response when some of eslstorage nodes are
    /// unavailable.
    ///
    /// PORT NOTE: parsed for protocol compatibility; unused locally (see the
    /// module docs).
    #[allow(dead_code)]
    allow_partial_response: bool,

    /// Optional list of log fields or log field prefixes ending with `*`,
    /// which must be hidden during query execution.
    ///
    /// PORT NOTE: parsed for protocol compatibility; unused locally (see the
    /// module docs).
    #[allow(dead_code)]
    hidden_fields_filters: Vec<String>,
}

/// Port of Go `getCommonParams`.
fn get_common_params(
    req: &Request,
    form: &Form,
    expected_protocol_version: &str,
) -> Result<CommonParams, String> {
    check_protocol_version(req, form, expected_protocol_version)?;

    let tenant_ids_str = form.value(req, "tenant_ids");
    let tenant_ids = unmarshal_tenant_ids_from_json(tenant_ids_str.as_bytes())
        .map_err(|err| format!("cannot unmarshal tenant_ids={tenant_ids_str:?}: {err}"))?;

    let timestamp = get_int64_from_request(req, form, "timestamp")?;

    let q_str = form.value(req, "query");
    let query = ParseQueryAtTimestamp(q_str, timestamp)
        .map_err(|err| format!("cannot unmarshal query={q_str:?}: {err}"))?;

    let disable_compression = get_bool_from_request(req, form, "disable_compression")?;

    let allow_partial_response = get_bool_from_request(req, form, "allow_partial_response")?;

    let hidden_fields_filters = get_string_slice_from_request(req, form, "hidden_fields_filters")?;

    Ok(CommonParams {
        tenant_ids,
        query,

        disable_compression,

        allow_partial_response,
        hidden_fields_filters,
    })
}

/// Port of Go `checkProtocolVersion`.
fn check_protocol_version(
    req: &Request,
    form: &Form,
    expected_protocol_version: &str,
) -> Result<(), String> {
    let version = form.value(req, "version");
    if version != expected_protocol_version {
        return Err(format!(
            "unexpected protocol version={version:?}; want {expected_protocol_version:?}; \
             the most likely cause of this error is different versions of EsLogs cluster components; \
             make sure EsLogs components have the same release version"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Response encoding (Go writeValuesWithHits / marshalQueryStatsBlock)
// ---------------------------------------------------------------------------

/// Port of Go `writeValuesWithHits`: `[8-byte count][entries][query-stats
/// block]`, optionally zstd-compressed as a whole.
fn write_values_with_hits(
    w: &mut ResponseWriter,
    qs: &QueryStats,
    query_duration_nsecs: i64,
    vhs: &[ValueWithHits],
    disable_compression: bool,
) -> Result<(), String> {
    let mut b = Vec::new();

    // Marshal vhs at first
    vlencoding::marshal_uint64(&mut b, vhs.len() as u64);
    for vh in vhs {
        vh.marshal(&mut b);
    }

    // Marshal query stats block after that
    marshal_query_stats_block(&mut b, qs, query_duration_nsecs);

    if !disable_compression {
        let mut compressed = Vec::new();
        zstd::compress_level(&mut compressed, &b, 1);
        b = compressed;
    }

    w.set_header("Content-Type", "application/octet-stream");
    w.write_bytes(&b);

    Ok(())
}

/// Port of Go `marshalQueryStatsBlock`.
///
/// PORT NOTE: Go derives the duration from `qctx.QueryDurationNsecs()`; the
/// port has no QueryContext, so the caller passes the measured duration.
fn marshal_query_stats_block(dst: &mut Vec<u8>, qs: &QueryStats, query_duration_nsecs: i64) {
    let db = qs.create_data_block(query_duration_nsecs);
    db.marshal(dst);
}

/// Nanoseconds elapsed since `start` (Go `qctx.QueryDurationNsecs()`).
fn elapsed_nsecs(start: Instant) -> i64 {
    i64::try_from(start.elapsed().as_nanos()).unwrap_or(i64::MAX)
}

// ---------------------------------------------------------------------------
// Typed request-arg helpers (Go get*FromRequest)
// ---------------------------------------------------------------------------

/// Port of Go `getInt64FromRequest`.
fn get_int64_from_request(req: &Request, form: &Form, arg_name: &str) -> Result<i64, String> {
    let s = form.value(req, arg_name);
    if s.is_empty() {
        return Err(format!("missing the required arg {arg_name}"));
    }
    s.parse::<i64>()
        .map_err(|err| format!("cannot parse {arg_name}={s:?}: {err}"))
}

/// Port of Go `getBoolFromRequest` (`strconv.ParseBool` semantics).
fn get_bool_from_request(req: &Request, form: &Form, arg_name: &str) -> Result<bool, String> {
    let s = form.value(req, arg_name);
    if s.is_empty() {
        return Err(format!("missing the required arg {arg_name}"));
    }
    match s {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
        _ => Err(format!(
            "cannot parse {arg_name}={s:?} as bool: invalid syntax"
        )),
    }
}

/// Port of Go `getStringSliceFromRequest` (a JSON array of strings; JSON
/// `null` yields an empty slice, mirroring `json.Unmarshal`).
fn get_string_slice_from_request(
    req: &Request,
    form: &Form,
    arg_name: &str,
) -> Result<Vec<String>, String> {
    let s = form.value(req, arg_name);
    if s.is_empty() {
        return Err(format!("missing the required arg {arg_name}"));
    }

    parse_json_string_array(s)
        .map_err(|err| format!("cannot unmarshal JSON array from {arg_name}={s:?}: {err}"))
}

/// Minimal parser for a JSON array of strings (or `null`), in the house style
/// of the hand-rolled JSON parsers in esl-logstorage (no external deps).
fn parse_json_string_array(s: &str) -> Result<Vec<String>, String> {
    let t = s.trim();
    if t == "null" {
        return Ok(Vec::new());
    }
    let b = t.as_bytes();
    if b.first() != Some(&b'[') || b.last() != Some(&b']') {
        return Err("expected a JSON array".to_string());
    }

    let mut a = Vec::new();
    let mut chars = t[1..t.len() - 1].trim().chars().peekable();
    if chars.peek().is_none() {
        return Ok(a);
    }
    loop {
        // Parse one JSON string literal.
        if chars.next() != Some('"') {
            return Err("expected a JSON string".to_string());
        }
        let mut out = String::new();
        loop {
            match chars.next() {
                Some('"') => break,
                Some('\\') => match chars.next() {
                    Some('"') => out.push('"'),
                    Some('\\') => out.push('\\'),
                    Some('/') => out.push('/'),
                    Some('b') => out.push('\u{0008}'),
                    Some('f') => out.push('\u{000c}'),
                    Some('n') => out.push('\n'),
                    Some('r') => out.push('\r'),
                    Some('t') => out.push('\t'),
                    Some('u') => {
                        let mut code = 0u32;
                        for _ in 0..4 {
                            let c = chars.next().ok_or("truncated \\u escape")?;
                            code = code * 16
                                + c.to_digit(16).ok_or("invalid hex digit in \\u escape")?;
                        }
                        out.push(char::from_u32(code).ok_or("invalid \\u escape code point")?);
                    }
                    _ => return Err("unsupported escape in JSON string".to_string()),
                },
                Some(c) => out.push(c),
                None => return Err("unterminated JSON string".to_string()),
            }
        }
        a.push(out);

        // Skip whitespace and expect `,` or the end of the array.
        loop {
            match chars.peek() {
                Some(c) if c.is_ascii_whitespace() => {
                    chars.next();
                }
                _ => break,
            }
        }
        match chars.next() {
            None => return Ok(a),
            Some(',') => {
                while matches!(chars.peek(), Some(c) if c.is_ascii_whitespace()) {
                    chars.next();
                }
            }
            _ => return Err("missing ',' between array items".to_string()),
        }
    }
}

// PORT NOTE: upstream has no internalselect test file; the tests below cover
// the port-specific request parsing plus an end-to-end round trip through the
// real netselect client (`esl_storage::netselect`) against a temp Storage.
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::path::PathBuf;

    use esl_common::httpserver::serve;
    use esl_logstorage::log_rows::get_log_rows;
    use esl_logstorage::rows::Field;
    use esl_logstorage::storage::StorageConfig;
    use esl_storage::http_client::AuthConfig;
    use esl_storage::netselect;

    fn unique_nsec() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }

    /// Opens a temp Storage, ingests `msgs` as rows (each with `_msg` and a
    /// `host` stream field), and flushes.
    fn open_storage_with_rows(name: &str, msgs: &[&str]) -> (Arc<Storage>, PathBuf) {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "esl-internalselect-{name}-{}-{}",
            std::process::id(),
            unique_nsec()
        ));
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);

        let tenant = TenantID::default();
        let mut lr = get_log_rows(&["host"], &[], &[], &[], "");
        let base = unique_nsec();
        for (i, msg) in msgs.iter().enumerate() {
            let mut fields = vec![
                Field {
                    name: "_msg".to_string(),
                    value: msg.to_string(),
                },
                Field {
                    name: "host".to_string(),
                    value: "node-1".to_string(),
                },
            ];
            lr.must_add(tenant, base + i as i64, &mut fields, -1);
        }
        s.must_add_rows(&lr);
        s.debug_flush();
        (s, path)
    }

    /// Serves `request_handler` over the given storage on an ephemeral port.
    fn serve_storage(storage: &Arc<Storage>) -> esl_common::httpserver::ServerHandle {
        let storage_h = Arc::clone(storage);
        serve("127.0.0.1:0", move |req, w| {
            if !request_handler(&storage_h, req, w) {
                w.error("not routed", 404);
            }
        })
        .expect("serve")
    }

    /// Connects the real netselect client to `addr`.
    fn new_client(addr: SocketAddr, disable_compression: bool) -> netselect::Storage {
        netselect::new_storage(
            &[format!("{addr}")],
            vec![AuthConfig::default()],
            disable_compression,
        )
    }

    /// Runs `query` through the netselect client and returns the values of the
    /// `_msg` column of every returned row.
    fn run_query_collect(
        client: &netselect::Storage,
        qs: &QueryStats,
        query: &str,
        column: &str,
    ) -> Result<Vec<String>, String> {
        let q = ParseQueryAtTimestamp(query, unique_nsec())?;
        let rows: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let rows_wb = Arc::clone(&rows);
        let column = column.to_string();
        let write_block: WriteDataBlockFn = Arc::new(move |_, db: &mut DataBlock| {
            if let Some(c) = db.get_column_by_name(&column) {
                let mut rows = rows_wb.lock().unwrap();
                for v in &c.values {
                    rows.push(String::from_utf8_lossy(v).into_owned());
                }
            }
        });
        client.run_query(qs, &[TenantID::default()], &q, false, write_block)?;
        let rows = rows.lock().unwrap().clone();
        Ok(rows)
    }

    /// End-to-end round trip: `/internal/select/query` served by this module,
    /// decoded by the real netselect client, with and without compression.
    #[test]
    fn test_process_query_request_roundtrip() {
        let msgs = [
            "connection error occurred",
            "all systems nominal",
            "disk error on node 3",
            "request completed ok",
            "cache warmed",
        ];
        let (storage, path) = open_storage_with_rows("query", &msgs);
        let handle = serve_storage(&storage);
        let addr = handle.local_addr();

        for disable_compression in [false, true] {
            let client = new_client(addr, disable_compression);
            let qs = QueryStats::default();

            // `*` returns all 5 rows.
            let mut got =
                run_query_collect(&client, &qs, "*", "_msg").expect("run_query `*` failed");
            got.sort();
            let mut want: Vec<String> = msgs.iter().map(|s| s.to_string()).collect();
            want.sort();
            assert_eq!(got, want, "disable_compression={disable_compression}");

            // `error` matches exactly the two rows containing the token.
            let got =
                run_query_collect(&client, &qs, "error", "_msg").expect("run_query `error` failed");
            assert_eq!(got.len(), 2, "disable_compression={disable_compression}");

            // `* | stats count() rows` returns a single row with rows=5.
            let got = run_query_collect(&client, &qs, "* | stats count() rows", "rows")
                .expect("run_query stats failed");
            assert_eq!(
                got,
                vec!["5".to_string()],
                "disable_compression={disable_compression}"
            );

            // The values-with-hits endpoints, decoded by the client through
            // unmarshal_values_with_hits + merge_values_with_hits.
            let q = ParseQueryAtTimestamp("*", unique_nsec()).expect("parse query");
            let tenants = [TenantID::default()];

            let vhs = client
                .get_field_names(&qs, &tenants, &q, false, "")
                .expect("get_field_names failed");
            let names: Vec<&str> = vhs.iter().map(|vh| vh.value.as_str()).collect();
            assert!(names.contains(&"_msg"), "field names: {names:?}");
            assert!(names.contains(&"host"), "field names: {names:?}");

            let vhs = client
                .get_field_values(&qs, &tenants, &q, false, "host", "", 10)
                .expect("get_field_values failed");
            assert_eq!(vhs.len(), 1, "host values: {vhs:?}");
            assert_eq!((vhs[0].value.as_str(), vhs[0].hits), ("node-1", 5));

            let vhs = client
                .get_stream_field_names(&qs, &tenants, &q, false, "")
                .expect("get_stream_field_names failed");
            assert_eq!(vhs.len(), 1, "stream field names: {vhs:?}");
            assert_eq!((vhs[0].value.as_str(), vhs[0].hits), ("host", 5));

            let vhs = client
                .get_stream_field_values(&qs, &tenants, &q, false, "host", "", 10)
                .expect("get_stream_field_values failed");
            assert_eq!(vhs.len(), 1, "stream field values: {vhs:?}");
            assert_eq!((vhs[0].value.as_str(), vhs[0].hits), ("node-1", 5));

            let vhs = client
                .get_streams(&qs, &tenants, &q, false, 10)
                .expect("get_streams failed");
            assert_eq!(vhs.len(), 1, "streams: {vhs:?}");
            assert!(
                vhs[0].value.contains("host=\"node-1\""),
                "stream: {}",
                vhs[0].value
            );
            assert_eq!(vhs[0].hits, 5);

            let vhs = client
                .get_stream_ids(&qs, &tenants, &q, false, 10)
                .expect("get_stream_ids failed");
            assert_eq!(vhs.len(), 1, "stream ids: {vhs:?}");
            assert_eq!(vhs[0].hits, 5);

            // tenant_ids: the fixture ingests under the default tenant only.
            let tids = client
                .get_tenant_ids(0, i64::MAX)
                .expect("get_tenant_ids failed");
            assert_eq!(tids, vec![TenantID::default()], "tenant ids: {tids:?}");

            client.must_stop();
        }

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    /// End-to-end round trip for the `/internal/delete/*` endpoints.
    #[test]
    fn test_process_delete_requests_roundtrip() {
        let (storage, path) = open_storage_with_rows("delete", &["some error", "ok"]);
        let handle = serve_storage(&storage);
        let client = new_client(handle.local_addr(), true);

        // No active tasks initially.
        let tasks = client.delete_active_tasks().expect("active_tasks failed");
        assert!(tasks.is_empty(), "unexpected initial tasks: {tasks:?}");

        // Register a delete task.
        let f = ParseFilter("error").expect("parse filter");
        client
            .delete_run_task("task-1", unique_nsec(), &[TenantID::default()], &f)
            .expect("delete_run_task failed");

        let tasks = client.delete_active_tasks().expect("active_tasks failed");
        assert_eq!(tasks.len(), 1, "tasks: {tasks:?}");
        assert_eq!(tasks[0].task_id, "task-1");

        // Registering the same taskID again fails on the server side.
        let err = client
            .delete_run_task("task-1", unique_nsec(), &[TenantID::default()], &f)
            .unwrap_err();
        assert!(err.contains("already registered"), "{err}");

        // Stop the task; the active list becomes empty.
        client
            .delete_stop_task("task-1")
            .expect("delete_stop_task failed");
        let tasks = client.delete_active_tasks().expect("active_tasks failed");
        assert!(tasks.is_empty(), "tasks must be empty: {tasks:?}");

        client.must_stop();
        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    /// Performs a raw HTTP/1.1 GET and returns (status_code, body); args are
    /// passed via the query string (Go `r.FormValue` falls back to it).
    fn http_get(addr: SocketAddr, target: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).expect("connect");
        write!(
            stream,
            "GET {target} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"
        )
        .expect("write request");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("read response");
        let text = String::from_utf8_lossy(&raw);
        let idx = text.find("\r\n\r\n").expect("headers/body separator");
        let head = &text[..idx];
        let body = text[idx + 4..].to_string();
        let status: u16 = head
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .expect("status code");
        (status, body)
    }

    #[test]
    fn test_request_handler_errors() {
        let (storage, path) = open_storage_with_rows("errors", &["x"]);
        let handle = serve_storage(&storage);
        let addr = handle.local_addr();

        // Unsupported endpoint under the /internal/ prefixes → 400.
        let (status, body) = http_get(addr, "/internal/select/unknown");
        assert_eq!(status, 400, "body={body}");
        assert!(body.contains("unsupported endpoint requested"), "{body}");

        // Non-internal paths are not routed here → 404 from the test handler.
        let (status, _) = http_get(addr, "/select/logsql/query?query=*");
        assert_eq!(status, 404);

        // Wrong protocol version → 400 with the version-mismatch message.
        let (status, body) = http_get(addr, "/internal/select/query?version=v0");
        assert_eq!(status, 400, "body={body}");
        assert!(body.contains("unexpected protocol version"), "{body}");

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_parse_multipart_form() {
        // Parse the exact body layout the netselect client produces.
        let args = vec![
            ("query".to_string(), "*".to_string()),
            ("na\"me".to_string(), "value1\r\nline2".to_string()),
            ("query".to_string(), "duplicate-ignored".to_string()),
        ];
        let (body, content_type) = esl_storage::http_client::new_multipart_request_body(&args);
        let boundary = get_multipart_boundary(&content_type).expect("boundary");
        let values = parse_multipart_form(&body, &boundary).expect("parse");
        assert_eq!(values.len(), 2);
        assert_eq!(values["query"], "*");
        assert_eq!(values["na\"me"], "value1\r\nline2");

        // Truncated body → error.
        assert!(parse_multipart_form(&body[..body.len() - 4], &boundary).is_err());
        // Wrong boundary → error.
        assert!(parse_multipart_form(&body, "nope").is_err());
    }

    #[test]
    fn test_parse_json_string_array() {
        assert_eq!(
            parse_json_string_array("null").unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(parse_json_string_array("[]").unwrap(), Vec::<String>::new());
        assert_eq!(
            parse_json_string_array(r#"["foo","b\"a\\r", "uA"]"#).unwrap(),
            vec!["foo".to_string(), "b\"a\\r".to_string(), "uA".to_string()]
        );
        assert!(parse_json_string_array("foo").is_err());
        assert!(parse_json_string_array(r#"["unterminated"#).is_err());
        assert!(parse_json_string_array(r#"["a" "b"]"#).is_err());
    }
}

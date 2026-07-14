//! Port of EsLogs `app/eslinsert/journald/journald.go`.
//!
//! Parses the systemd journal export format
//! (<https://systemd.io/JOURNAL_EXPORT_FORMATS/#journal-export-format>):
//! `key=value` lines plus binary-value framing
//! (`key\n<little_endian_size_64>value\n`), with `__REALTIME_TIMESTAMP`
//! handling and journald-specific stream fields.
//!
//! PORT NOTE: Go's `MustInit()` must run after `flag.Parse()`; the Rust flags
//! resolve lazily, so the tenant-id and stream-fields initialization happens on
//! first use via `OnceLock` instead of an explicit init call.
//!
//! PORT NOTE: the `Content-Encoding`s Go handles via
//! `protoparserutil.GetUncompressedReader` are decompressed transparently by
//! [`Request::body_reader`] in `esl_common::httpserver`, so the
//! `writeconcurrencylimiter` reader wraps the already-decompressed body here
//! (same bounded-concurrency effect as Go wrapping the raw body then
//! decompressing), matching the jsonline/elasticsearch siblings.

use std::io::Read;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Instant;

use esl_common::errorf;
use esl_common::flagutil::{ArrayString, Flag};
use esl_common::httpserver::{Request, ResponseWriter, get_quoted_remote_addr};
use esl_common::metrics::{Counter, Summary};
use esl_common::writeconcurrencylimiter;

use esl_logstorage::rows::Field;
use esl_logstorage::stream_tags::check_stream_field_names;
use esl_logstorage::tenant_id::{TenantID, parse_tenant_id};

use crate::common_params::{
    CommonParams, LogMessageProcessorTrait, LogRowsStorage, errorf_with_status,
    get_common_params as insertutil_get_common_params, now_unix_nanos,
};
use crate::line_reader::LineReader;

// See https://github.com/systemd/systemd/blob/main/src/libsystemd/sd-journal/journal-file.c#L1703
const MAX_FIELD_NAME_LEN: usize = 64;

static JOURNALD_STREAM_FIELDS: Flag<ArrayString> = Flag::new(
    "journald.streamFields",
    "Comma-separated list of fields to use as log stream fields for logs ingested over journald protocol. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/journald/#stream-fields",
    ArrayString::default,
);
static JOURNALD_IGNORE_FIELDS: Flag<ArrayString> = Flag::new(
    "journald.ignoreFields",
    "Comma-separated list of fields to ignore for logs ingested over journald protocol. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/journald/#dropping-fields",
    ArrayString::default,
);
static JOURNALD_TIME_FIELD: Flag<String> = Flag::new(
    "journald.timeField",
    "Field to use as a log timestamp for logs ingested via journald protocol. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/journald/#time-field",
    || "__REALTIME_TIMESTAMP".to_string(),
);
static JOURNALD_TENANT_ID: Flag<String> = Flag::new(
    "journald.tenantID",
    "TenantID for logs ingested via the Journald endpoint. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/journald/#multitenancy",
    || "0:0".to_string(),
);
static JOURNALD_INCLUDE_ENTRY_METADATA: Flag<bool> = Flag::new(
    "journald.includeEntryMetadata",
    "Include Journald fields with double underscore prefixes",
    || false,
);
static JOURNALD_USE_REMOTE_IP: Flag<bool> = Flag::new(
    "journald.useRemoteIP",
    "Whether to add the remote IP address as the remote_ip log field for ingested journald messages.",
    || false,
);

/// Lazily-initialized tenant id (Go `MustInit`'s `tenantID` global).
fn tenant_id() -> TenantID {
    static CELL: OnceLock<TenantID> = OnceLock::new();
    *CELL.get_or_init(|| match parse_tenant_id(JOURNALD_TENANT_ID.get()) {
        Ok(t) => t,
        Err(err) => {
            esl_common::fatalf!(
                "cannot parse -journald.tenantID={} command-line flag: {err}; see https://docs.victoriametrics.com/victorialogs/data-ingestion/journald/#multitenancy",
                JOURNALD_TENANT_ID.get()
            );
            unreachable!()
        }
    })
}

const DEFAULT_STREAM_FIELDS: [&str; 3] = ["_MACHINE_ID", "_HOSTNAME", "_SYSTEMD_UNIT"];

/// Lazily-initialized stream fields (Go `MustInit`'s `streamFields` global).
fn stream_fields() -> &'static [String] {
    static CELL: OnceLock<Vec<String>> = OnceLock::new();
    CELL.get_or_init(|| {
        let mut stream_fields: Vec<String> = DEFAULT_STREAM_FIELDS
            .iter()
            .map(|s| s.to_string())
            .collect();
        let flag = JOURNALD_STREAM_FIELDS.get();
        if !flag.is_empty() {
            stream_fields = flag.0.clone();
        }
        let refs: Vec<&str> = stream_fields.iter().map(String::as_str).collect();
        if let Err(err) = check_stream_field_names(&refs) {
            esl_common::fatalf!(
                "invalid stream field names in -journald.streamFields={stream_fields:?}: {err}; see https://docs.victoriametrics.com/victorialogs/data-ingestion/journald/#stream-fields"
            );
        }
        stream_fields
    })
}

fn get_common_params(req: &Request) -> Result<CommonParams, String> {
    let mut cp = insertutil_get_common_params(req)?;
    apply_journald_defaults(&mut cp);
    Ok(cp)
}

/// The journald-specific defaulting of Go's `getCommonParams`, split out so the
/// ported tests can drive it without an HTTP request (see PORT NOTE in tests).
fn apply_journald_defaults(cp: &mut CommonParams) {
    if cp.tenant_id.account_id == 0 && cp.tenant_id.project_id == 0 {
        cp.tenant_id = tenant_id();
    }
    if !cp.is_time_field_set {
        cp.time_fields = vec![JOURNALD_TIME_FIELD.get().clone()];
    }
    if cp.stream_fields.is_empty() {
        cp.stream_fields = stream_fields().to_vec();
    }
    if cp.ignore_fields.is_empty() {
        cp.ignore_fields = JOURNALD_IGNORE_FIELDS.get().0.clone();
    }
    cp.msg_fields = vec!["MESSAGE".to_string()];
}

/// RequestHandler processes Journald Export insert requests. Returns true if
/// the path was handled.
pub fn request_handler<S: LogRowsStorage>(
    storage: &Arc<S>,
    path: &str,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    match path {
        "/insert/journald/upload" => {
            if req.content_type() != "application/vnd.fdo.journal" {
                w.errorf(
                    req,
                    "only application/vnd.fdo.journal encoding is supported for Journald",
                );
                return true;
            }
            handle_journald(storage, req, w);
            true
        }
        _ => false,
    }
}

static REQUESTS_TOTAL: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::new_counter(r#"esl_http_requests_total{path="/insert/journald/upload"}"#)
});
static ERRORS_TOTAL: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::new_counter(r#"esl_http_errors_total{path="/insert/journald/upload"}"#)
});
static REQUEST_DURATION: LazyLock<Arc<Summary>> = LazyLock::new(|| {
    esl_common::metrics::new_summary(
        r#"esl_http_request_duration_seconds{path="/insert/journald/upload"}"#,
    )
});

/// handleJournald parses Journal binary entries
fn handle_journald<S: LogRowsStorage>(storage: &Arc<S>, req: &mut Request, w: &mut ResponseWriter) {
    let start_time = Instant::now();
    REQUESTS_TOTAL.inc();

    let cp = match get_common_params(req) {
        Ok(cp) => cp,
        Err(err) => {
            ERRORS_TOTAL.inc();
            w.errorf(
                req,
                &format!("cannot parse common params from request: {err}"),
            );
            return;
        }
    };

    if let Err((msg, status)) = storage.can_write_data() {
        ERRORS_TOTAL.inc();
        errorf_with_status(w, req, &msg, status);
        return;
    }

    let stream_name = format!(
        "remoteAddr={}, requestURI={:?}",
        get_quoted_remote_addr(req),
        req.request_uri()
    );

    let remote_ip = if *JOURNALD_USE_REMOTE_IP.get() {
        get_remote_ip(req)
    } else {
        String::new()
    };

    // Go wraps r.Body with writeconcurrencylimiter.GetReader for ingest
    // backpressure; on failure it logs and returns without an HTTP error.
    let res = {
        let mut wcr = match writeconcurrencylimiter::get_reader(req.body_reader()) {
            Ok(wcr) => wcr,
            Err(err) => {
                ERRORS_TOTAL.inc();
                errorf!("cannot start reading journald request: {}", err.err);
                return;
            }
        };
        let mut lmp = cp.new_log_message_processor(storage, "journald");
        let res = process_stream_internal(&stream_name, &mut wcr, &remote_ip, &mut lmp, &cp);
        lmp.close();
        res
    };
    if let Err(err) = res {
        ERRORS_TOTAL.inc();
        w.errorf(req, &format!("cannot read journald protocol data: {err}"));
        return;
    }

    // systemd starting release v258 will support compression, which starts working after negotiation: it expects supported compression
    // algorithms list in Accept-Encoding response header in a format "<algorithm_1>[:<priority_1>][;<algorithm_2>:<priority_2>]"
    // See https://github.com/systemd/systemd/pull/34822
    w.set_header("Accept-Encoding", "zstd");

    // Update REQUEST_DURATION only for successfully parsed requests.
    // There is no need in updating it for request errors, since their timings
    // are usually much smaller than the timing for successful request parsing.
    REQUEST_DURATION.update_duration(start_time);
}

fn process_stream_internal(
    stream_name: &str,
    r: &mut dyn Read,
    remote_ip: &str,
    lmp: &mut dyn LogMessageProcessorTrait,
    cp: &CommonParams,
) -> Result<(), String> {
    let mut lr = LineReader::new("journald", r);
    let mut fb = FieldsBuf::default();

    loop {
        match read_journald_log_entry(stream_name, &mut lr, remote_ip, lmp, cp, &mut fb) {
            // PORT NOTE: Go signals the clean end of the stream with the
            // `io.EOF` sentinel; the port returns `Ok(false)` instead.
            Ok(false) => return Ok(()),
            Ok(true) => {}
            Err(err) => return Err(format!("{stream_name}: {err}")),
        }
    }
}

/// Reusable per-entry buffers (Go's pooled `fieldsBuf`).
///
/// PORT NOTE: Go aliases `Field` name/value strings into `buf` via unsafe
/// casts and pools the struct with `sync.Pool`; the Rust `Field` owns its
/// strings, so `buf` and the pool are dropped and one `FieldsBuf` is reused
/// across all the entries of a stream instead.
#[derive(Default)]
struct FieldsBuf {
    fields: Vec<Field>,
    name: Vec<u8>,
    value: Vec<u8>,
}

impl FieldsBuf {
    fn reset(&mut self) {
        self.fields.clear();
        self.name.clear();
        self.value.clear();
    }
}

fn append_next_line_to_value(fb: &mut FieldsBuf, lr: &mut LineReader) -> Result<(), String> {
    if !lr.next_line() {
        if let Some(err) = lr.err_string() {
            return Err(err);
        }
        return Err("unexpected end of stream".to_string());
    }
    fb.value.extend_from_slice(lr.line());
    fb.value.push(b'\n');
    Ok(())
}

/// readJournaldLogEntry reads a single log entry in Journald format.
///
/// Returns `Ok(false)` on the clean end of the stream (Go's `io.EOF`).
///
/// See <https://systemd.io/JOURNAL_EXPORT_FORMATS/#journal-export-format>
fn read_journald_log_entry(
    stream_name: &str,
    lr: &mut LineReader,
    remote_ip: &str,
    lmp: &mut dyn LogMessageProcessorTrait,
    cp: &CommonParams,
    fb: &mut FieldsBuf,
) -> Result<bool, String> {
    let mut ts: i64 = 0;

    fb.reset();

    if !lr.next_line() {
        if let Some(err) = lr.err_string() {
            return Err(format!("cannot read the first field: {err}"));
        }
        return Ok(false);
    }

    loop {
        let line = lr.line();
        if line.is_empty() {
            // The end of a single log entry. Write it to the storage
            if !fb.fields.is_empty() {
                if ts == 0 {
                    ts = now_unix_nanos();
                }
                if !remote_ip.is_empty() {
                    fb.fields.push(Field {
                        name: "remote_ip".to_string(),
                        value: remote_ip.as_bytes().to_vec(),
                    });
                }
                lmp.add_row(ts, &mut fb.fields, -1);
            }
            return Ok(true);
        }

        // line could be either "key=value" or "key"
        // according to https://systemd.io/JOURNAL_EXPORT_FORMATS/#journal-export-format
        let is_binary_value;
        if let Some(n) = line.iter().position(|&b| b == b'=') {
            // line = "key=value"
            fb.name.clear();
            fb.name.extend_from_slice(&line[..n]);
            fb.value.clear();
            fb.value.extend_from_slice(&line[n + 1..]);
            is_binary_value = false;
        } else {
            // line = "key"
            // Parse the binary-encoded value from the next line according to "key\n<little_endian_size_64>value\n" format
            fb.name.clear();
            fb.name.extend_from_slice(line);

            fb.value.clear();
            while fb.value.len() < 8 {
                append_next_line_to_value(fb, lr)
                    .map_err(|err| format!("cannot read value size: {err}"))?;
            }
            let size = u64::from_le_bytes(fb.value[..8].try_into().expect("8-byte size prefix"));

            // Read the value until its length exceeds the given size - the last char in the read value will always be '\n'
            // because it is appended by append_next_line_to_value().
            while (fb.value.len() - 8) as u64 <= size {
                append_next_line_to_value(fb, lr).map_err(|err| {
                    format!(
                        "cannot read {:?} value with size {size} bytes; read only {} bytes: {err}",
                        String::from_utf8_lossy(&fb.name),
                        fb.value.len() - 8
                    )
                })?;
            }
            let value = &fb.value[8..fb.value.len() - 1];
            if value.len() as u64 != size {
                return Err(format!(
                    "unexpected {:?} value size; got {} bytes; want {size} bytes; value: {:?}",
                    String::from_utf8_lossy(&fb.name),
                    value.len(),
                    String::from_utf8_lossy(value)
                ));
            }
            is_binary_value = true;
        }

        if !lr.next_line()
            && let Some(err) = lr.err_string()
        {
            return Err(format!("cannot read the next log field: {err}"));
        }
        // When next_line() hit EOF without an error, the loop still adds the
        // last log field below before the return.

        if fb.name.len() > MAX_FIELD_NAME_LEN {
            errorf!(
                "{stream_name}: field name size should not exceed {MAX_FIELD_NAME_LEN} bytes; got {} bytes: {:?}; skipping this field",
                fb.name.len(),
                String::from_utf8_lossy(&fb.name)
            );
            continue;
        }
        if !is_valid_field_name(&fb.name) {
            errorf!(
                "{stream_name}: invalid field name {:?}; it must consist of `A-Z0-9_` chars and must start from non-digit char; skipping this field",
                String::from_utf8_lossy(&fb.name)
            );
            continue;
        }

        // is_valid_field_name guarantees the name is ASCII `A-Z0-9_`.
        let name = std::str::from_utf8(&fb.name).expect("validated ASCII field name");
        // Go aliases the raw value bytes as a string
        // (`bytesutil.ToUnsafeString`); with byte-valued `Field`s the port
        // now stores journald values — including binary-encoded ones with
        // invalid UTF-8 — verbatim, exactly like Go.
        let value: &[u8] = if is_binary_value {
            &fb.value[8..fb.value.len() - 1]
        } else {
            &fb.value
        };

        if cp.time_fields.iter().any(|tf| tf == name) {
            // R3: invalid UTF-8 fails the timestamp parse, matching Go's
            // parse semantics on arbitrary bytes.
            let parsed = std::str::from_utf8(value)
                .map_err(|e| e.to_string())
                .and_then(|s| s.parse::<i64>().map_err(|e| e.to_string()));
            match parsed {
                Ok(t) => {
                    // Convert journald microsecond timestamp to nanoseconds
                    ts = t * 1_000;
                }
                Err(err) => {
                    errorf!(
                        "{stream_name}: cannot parse timestamp from the field {name:?}: {err}; using the current timestamp"
                    );
                    ts = 0;
                }
            }
            continue;
        }

        let name = if cp.msg_fields.iter().any(|mf| mf == name) {
            "_msg"
        } else {
            name
        };

        if name == "PRIORITY" {
            let priority = journald_priority_to_level(value);
            fb.fields.push(Field {
                name: "level".to_string(),
                value: priority.to_vec(),
            });
        }

        if !name.starts_with("__") || *JOURNALD_INCLUDE_ENTRY_METADATA.get() {
            let name = name.to_string();
            let value = value.to_vec();
            fb.fields.push(Field { name, value });
        }
    }
}

fn journald_priority_to_level(priority: &[u8]) -> &[u8] {
    // See https://wiki.archlinux.org/title/Systemd/Journal#Priority_level
    // and https://grafana.com/docs/grafana/latest/explore/logs-integration/#log-level
    match priority {
        b"0" => b"emerg",
        b"1" => b"alert",
        b"2" => b"critical",
        b"3" => b"error",
        b"4" => b"warning",
        b"5" => b"notice",
        b"6" => b"info",
        b"7" => b"debug",
        _ => priority,
    }
}

fn is_valid_field_name(s: &[u8]) -> bool {
    if s.is_empty() {
        return false;
    }
    let c = s[0];
    if !(c.is_ascii_uppercase() || c == b'_') {
        return false;
    }

    for &c in &s[1..] {
        if !(c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_') {
            return false;
        }
    }
    true
}

fn get_remote_ip(req: &Request) -> String {
    get_remote_ip_from(req.remote_addr(), req.header("X-Forwarded-For"))
}

/// The core of Go's `getRemoteIP`, split out so the ported tests can drive it
/// without an HTTP request (see PORT NOTE in tests).
fn get_remote_ip_from(remote_addr: &str, xff: &str) -> String {
    let mut addr = remote_addr;

    // handle reverse proxies
    if !xff.is_empty() {
        addr = xff.split(',').next().unwrap_or("").trim();
    }

    // http server sets it to IP:port
    if let Some(host) = split_host_port(addr) {
        return host.to_string();
    }

    // strip brackets for IPv6 addresses
    let addr = addr.strip_prefix('[').unwrap_or(addr);
    let addr = addr.strip_suffix(']').unwrap_or(addr);

    addr.to_string()
}

/// The success cases of Go `net.SplitHostPort`, returning the host part of
/// `host:port` / `[host]:port`, or `None` when addr has no port.
fn split_host_port(addr: &str) -> Option<&str> {
    if let Some(rest) = addr.strip_prefix('[') {
        let end = rest.find(']')?;
        let after = &rest[end + 1..];
        after.strip_prefix(':')?;
        return Some(&rest[..end]);
    }
    let mut colons = addr.match_indices(':');
    match (colons.next(), colons.next()) {
        (Some((i, _)), None) => Some(&addr[..i]),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    use crate::common_params::TestLogMessageProcessor;

    /// PORT NOTE: the Go tests build `*http.Request` values via
    /// `http.NewRequest`; the Rust `Request` is tied to a live connection and
    /// has no test constructor, so the ported tests drive the request-free
    /// cores (`apply_journald_defaults`, `get_remote_ip_from`,
    /// `process_stream_internal`) directly.
    fn get_test_common_params(custom_time_field: Option<&str>) -> CommonParams {
        let mut cp = CommonParams::empty();
        if let Some(tf) = custom_time_field {
            cp.is_time_field_set = true;
            cp.time_fields = vec![tf.to_string()];
        }
        apply_journald_defaults(&mut cp);
        cp
    }

    #[test]
    fn test_is_valid_field_name() {
        fn f(name: &str, result_expected: bool) {
            let result = is_valid_field_name(name.as_bytes());
            assert_eq!(
                result, result_expected,
                "unexpected result for is_valid_field_name({name:?}); got {result}; want {result_expected}"
            );
        }

        f("", false);
        f("a", false);
        f("1", false);
        f("_", true);
        f("X", true);
        f("Xa", false);
        f("X_343", true);
        f("X_0123456789_AZ", true);
        f("SDDFD sdf", false);
    }

    #[test]
    fn test_get_common_params_time_field() {
        fn f(time_field_header: &str, expected_time_field: &str) {
            let custom = if time_field_header.is_empty() {
                None
            } else {
                Some(time_field_header)
            };
            let cp = get_test_common_params(custom);
            assert!(
                cp.time_fields.len() == 1 && cp.time_fields[0] == expected_time_field,
                "unexpected TimeFields; got {:?}; want [{expected_time_field}]",
                cp.time_fields
            );
        }

        // Test default behavior - when no custom time field is specified, journald uses __REALTIME_TIMESTAMP
        f("", "__REALTIME_TIMESTAMP");

        // Test custom time field - when a custom time field is specified via HTTP header, it's respected
        f("custom_time", "custom_time");
    }

    #[test]
    fn test_push_journald_success() {
        fn f(src: &[u8], remote_ip: &str, timestamps_expected: &[i64], result_expected: &str) {
            let mut tlp = TestLogMessageProcessor::default();
            let cp = get_test_common_params(None);

            let mut buf = Cursor::new(src);
            if let Err(err) = process_stream_internal("test", &mut buf, remote_ip, &mut tlp, &cp) {
                panic!("unexpected error: {err}");
            }

            if let Err(err) = tlp.verify(timestamps_expected, result_expected) {
                panic!("{err}");
            }
        }

        // Single event
        f(
            b"__REALTIME_TIMESTAMP=91723819283\nMESSAGE=Test message\n\n",
            "",
            &[91723819283000],
            "{\"_msg\":\"Test message\"}",
        );

        // Single event with remote_ip
        f(
            b"__REALTIME_TIMESTAMP=91723819283\nMESSAGE=Test message\n\n",
            "1.2.3.4",
            &[91723819283000],
            "{\"_msg\":\"Test message\",\"remote_ip\":\"1.2.3.4\"}",
        );

        // Multiple events
        f(
            b"__REALTIME_TIMESTAMP=91723819283\nPRIORITY=3\nMESSAGE=Test message\n\n__REALTIME_TIMESTAMP=91723819284\nMESSAGE=Test message2\n",
            "",
            &[91723819283000, 91723819284000],
            "{\"level\":\"error\",\"PRIORITY\":\"3\",\"_msg\":\"Test message\"}\n{\"_msg\":\"Test message2\"}",
        );

        // Parse binary data
        f(
            b"__CURSOR=s=e0afe8412a6a49d2bfcf66aa7927b588;i=1f06;b=f778b6e2f7584a77b991a2366612a7b5;m=300bdfd420;t=62526e1182354;x=930dc44b370963b7\nE=JobStateChanged\n__REALTIME_TIMESTAMP=1729698775704404\n__MONOTONIC_TIMESTAMP=206357648416\n__SEQNUM=7942\n__SEQNUM_ID=e0afe8412a6a49d2bfcf66aa7927b588\n_BOOT_ID=f778b6e2f7584a77b991a2366612a7b5\n_UID=0\n_GID=0\n_MACHINE_ID=a4a970370c30a925df02a13c67167847\n_HOSTNAME=ecd5e4555787\n_RUNTIME_SCOPE=system\n_TRANSPORT=journal\n_CAP_EFFECTIVE=1ffffffffff\n_SYSTEMD_CGROUP=/init.scope\n_SYSTEMD_UNIT=init.scope\n_SYSTEMD_SLICE=-.slice\nCODE_FILE=<stdin>\nCODE_LINE=1\nCODE_FUNC=<module>\nSYSLOG_IDENTIFIER=python3\n_COMM=python3\n_EXE=/usr/bin/python3.12\n_CMDLINE=python3\nMESSAGE\n\x13\x00\x00\x00\x00\x00\x00\x00foo\nbar\n\n\nasda\nasda\n_PID=2763\n_SOURCE_REALTIME_TIMESTAMP=1729698775704375\n\n",
            "",
            &[1729698775704404000],
            "{\"E\":\"JobStateChanged\",\"_BOOT_ID\":\"f778b6e2f7584a77b991a2366612a7b5\",\"_UID\":\"0\",\"_GID\":\"0\",\"_MACHINE_ID\":\"a4a970370c30a925df02a13c67167847\",\"_HOSTNAME\":\"ecd5e4555787\",\"_RUNTIME_SCOPE\":\"system\",\"_TRANSPORT\":\"journal\",\"_CAP_EFFECTIVE\":\"1ffffffffff\",\"_SYSTEMD_CGROUP\":\"/init.scope\",\"_SYSTEMD_UNIT\":\"init.scope\",\"_SYSTEMD_SLICE\":\"-.slice\",\"CODE_FILE\":\"\\u003cstdin>\",\"CODE_LINE\":\"1\",\"CODE_FUNC\":\"\\u003cmodule>\",\"SYSLOG_IDENTIFIER\":\"python3\",\"_COMM\":\"python3\",\"_EXE\":\"/usr/bin/python3.12\",\"_CMDLINE\":\"python3\",\"_msg\":\"foo\\nbar\\n\\n\\nasda\\nasda\",\"_PID\":\"2763\",\"_SOURCE_REALTIME_TIMESTAMP\":\"1729698775704375\"}",
        );

        // Parse binary data with trailing newline
        f(
            b"__REALTIME_TIMESTAMP=1729698775704404\n_CMDLINE=python3\nMESSAGE\n\x14\x00\x00\x00\x00\x00\x00\x00foo\nbar\n\n\nasda\nasda\n\n_PID=2763\n\n",
            "",
            &[1729698775704404000],
            r#"{"_CMDLINE":"python3","_msg":"foo\nbar\n\n\nasda\nasda\n","_PID":"2763"}"#,
        );
        f(
            b"__REALTIME_TIMESTAMP=1729698775704404\n_CMDLINE=python3\nMESSAGE\n\x00\x00\x00\x00\x00\x00\x00\x00\n_PID=2763\n\n",
            "",
            &[1729698775704404000],
            r#"{"_CMDLINE":"python3","_PID":"2763"}"#,
        );
        f(
            b"__REALTIME_TIMESTAMP=1729698775704404\n_CMDLINE=python3\nMESSAGE\n\x0A\x00\x00\x00\x00\x00\x00\x00123456789\n\n_PID=2763\n\n",
            "",
            &[1729698775704404000],
            r#"{"_CMDLINE":"python3","_msg":"123456789\n","_PID":"2763"}"#,
        );
        f(
            b"__REALTIME_TIMESTAMP=1729698775704404\n_CMDLINE=python3\nMESSAGE\n\x0A\x00\x00\x00\x00\x00\x00\x001234567890\n_PID=2763\n\n",
            "",
            &[1729698775704404000],
            r#"{"_CMDLINE":"python3","_msg":"1234567890","_PID":"2763"}"#,
        );

        // Empty field name must be ignored
        f(
            b"__REALTIME_TIMESTAMP=91723819283\na=b\n=Test message",
            "",
            &[],
            "",
        );
        f(
            b"__REALTIME_TIMESTAMP=91723819284\nMESSAGE=Test message2\n\n__REALTIME_TIMESTAMP=91723819283\n=Test message\n",
            "",
            &[91723819284000],
            r#"{"_msg":"Test message2"}"#,
        );

        // field name starting with number must be ignored
        f(
            b"__REALTIME_TIMESTAMP=91723819283\n1incorrect=Test message\n\n__REALTIME_TIMESTAMP=91723819284\nMESSAGE=Test message2\n\n",
            "",
            &[91723819284000],
            r#"{"_msg":"Test message2"}"#,
        );

        // field name exceeding 64 bytes limit must be ignored
        f(
            b"__REALTIME_TIMESTAMP=91723819283\ntoolooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongcorrecooooooooooooong=Test message\n",
            "",
            &[],
            "",
        );

        // field name with invalid chars must be ignored
        f(
            b"__REALTIME_TIMESTAMP=91723819283\nbadC!@$!@$as=Test message\n",
            "",
            &[],
            "",
        );

        // PORT-ONLY CASES: upstream has no invalid-UTF-8 coverage. Pin the
        // divergence documented at the Field construction site: Go stores
        // the raw value bytes; the port U+FFFD-replaces invalid sequences.
        // Plain `NAME=value` entry with a raw 0xFF byte in the value.
        f(
            b"__REALTIME_TIMESTAMP=91723819283\nMESSAGE=a\xffb\n\n",
            "",
            &[91723819283000],
            "{\"_msg\":\"a\u{FFFD}b\"}",
        );
        // Binary-encoded value (8-byte LE size prefix) with a raw 0xFF byte.
        f(
            b"__REALTIME_TIMESTAMP=91723819283\nMESSAGE\n\x03\x00\x00\x00\x00\x00\x00\x00a\xffb\n\n",
            "",
            &[91723819283000],
            "{\"_msg\":\"a\u{FFFD}b\"}",
        );
    }

    #[test]
    fn test_push_journald_failure() {
        fn f(data: &[u8]) {
            let mut tlp = TestLogMessageProcessor::default();
            let cp = get_test_common_params(None);

            let mut buf = Cursor::new(data);
            assert!(
                process_stream_internal("test", &mut buf, "", &mut tlp, &cp).is_err(),
                "expecting non-nil error"
            );
        }

        // too short binary encoded message
        f(b"__CURSOR=s=e0afe8412a6a49d2bfcf66aa7927b588;i=1f06;b=f778b6e2f7584a77b991a2366612a7b5;m=300bdfd420;t=62526e1182354;x=930dc44b370963b7\n__REALTIME_TIMESTAMP=1729698775704404\nMESSAGE\n\x13\x00\x00\x00\x00\x00\x00\x00foo\nbar\n\n\nasdaasd");
        f(b"__REALTIME_TIMESTAMP=1729698775704404\n_CMDLINE=python3\nMESSAGE\n\x00\x00\x00\x00\x00\x00\x00\x00_PID=2763\n\n");
        f(b"__REALTIME_TIMESTAMP=1729698775704404\n_CMDLINE=python3\nMESSAGE\n\x0A\x00\x00\x00\x00\x00\x00\x001234567890_PID=2763\n\n");
        f(b"__REALTIME_TIMESTAMP=1729698775704404\n_CMDLINE=python3\nMESSAGE\n\x0A\x00\x00\x00\x00\x00\x00\x00123456789\n_PID=2763\n\n");

        // too long binary encoded message
        f(b"__CURSOR=s=e0afe8412a6a49d2bfcf66aa7927b588;i=1f06;b=f778b6e2f7584a77b991a2366612a7b5;m=300bdfd420;t=62526e1182354;x=930dc44b370963b7\n__REALTIME_TIMESTAMP=1729698775704404\nMESSAGE\n\x13\x00\x00\x00\x00\x00\x00\x00foo\nbar\n\n\nasdaasdakljlsfd");
    }

    #[test]
    fn test_get_remote_ip() {
        fn f(remote_addr: &str, xff: &str, ip_expected: &str) {
            let ip = get_remote_ip_from(remote_addr, xff);
            assert_eq!(
                ip, ip_expected,
                "unexpected remote ip; got {ip:?}; want {ip_expected:?}"
            );
        }

        // remoteAddr
        f("1.2.3.4", "", "1.2.3.4");
        f("1.2.3.4:443", "", "1.2.3.4");
        f("[::1]", "", "::1");
        f("[::1]:80", "", "::1");

        // xff
        f("", "1.2.3.4", "1.2.3.4");
        f("", "1.2.3.4:443", "1.2.3.4");
        f("", "[::1]", "::1");
        f("", "[::1]:80", "::1");

        // multiple xff
        f("", "1.2.3.4,5.6.7.8", "1.2.3.4");
        f("", " 1.2.3.4 , 5.6.7.8", "1.2.3.4");
        f("", "[::1] , 5.6.7.8", "::1");
        f("", "[::1]:80 , 5.6.7.8", "::1");
    }
}

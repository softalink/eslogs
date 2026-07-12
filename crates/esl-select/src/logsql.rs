//! Port of the query handlers and shared arg parsing in
//! `app/eslselect/logsql/logsql.go`.
//!
//! This file owns Go `parseCommonArgs` (the [`CommonArgs`] ctx struct shared by
//! all `/select/logsql/*` endpoints), the `/select/logsql/query` and
//! `/select/logsql/query_time_range` handlers, and the shared helpers
//! (`getTimeNsec`, `getPositiveInt`, `parseDuration`, `parseExtraFilters`,
//! `parseExtraStreamFilters`, `appendJSONRow`, ...). The other endpoints live
//! in their dedicated `logsql_*.rs` sibling modules.

use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use esl_common::httpserver::{Request, ResponseWriter, get_quoted_remote_addr};
use esl_common::timeutil::parse_time_at;
use esl_logstorage::parser::{Filter, ParseFilter, Query};
use esl_logstorage::storage::Storage;
use esl_logstorage::storage_search::{BlockColumn, DataBlock, WriteDataBlockFn};
use esl_logstorage::tenant_id::{TenantID, get_tenant_id_from_request};
use esl_logstorage::values_encoder::{
    marshal_timestamp_rfc3339_nano_string, sub_int64_no_overflow, try_parse_duration,
};

pub(crate) use crate::logsql_hits::process_hits_request;

/// Maximum query length in bytes (Go `-search.maxQueryLen`, default 16*1024).
const MAX_QUERY_LEN: usize = 16 * 1024;

/// Current wall-clock time in nanoseconds since the Unix epoch.
pub(crate) fn now_nsec() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Go `getPositiveInt`: parses a non-negative integer form arg, defaulting to 0.
pub(crate) fn get_positive_int(req: &Request, name: &str) -> Result<i64, String> {
    let s = req.form_value(name);
    if s.is_empty() {
        return Ok(0);
    }
    match s.parse::<i64>() {
        Ok(n) if n >= 0 => Ok(n),
        Ok(n) => Err(format!("{name:?} cannot be smaller than 0; got {n}")),
        Err(_) => Err(format!("cannot parse integer {name:?}={s:?}")),
    }
}

/// Go `getTimeNsec`: parses an optional timestamp form arg to nanoseconds.
pub(crate) fn get_time_nsec(req: &Request, name: &str) -> Result<Option<i64>, String> {
    let s = req.form_value(name);
    if s.is_empty() {
        return Ok(None);
    }
    match parse_time_at(s, now_nsec()) {
        Ok(n) => Ok(Some(n)),
        Err(e) => Err(format!("cannot parse {name}={s}: {e}")),
    }
}

/// Go `getBoolFromRequest`: parses an optional bool form arg into `dst` with
/// `strconv.ParseBool` semantics.
pub(crate) fn get_bool_from_request(
    dst: &mut bool,
    req: &Request,
    name: &str,
) -> Result<(), String> {
    let s = req.form_value(name);
    if s.is_empty() {
        return Ok(());
    }
    match s {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => *dst = true,
        "0" | "f" | "F" | "false" | "FALSE" | "False" => *dst = false,
        _ => return Err(format!("cannot parse {name}={s:?} as bool")),
    }
    Ok(())
}

/// Go `httputil.GetBool`: returns false for "", "0", "f", "false" and "no"
/// (case-insensitive), true otherwise.
pub(crate) fn get_bool(req: &Request, name: &str) -> bool {
    !matches!(
        req.form_value(name).to_ascii_lowercase().as_str(),
        "" | "0" | "f" | "false" | "no"
    )
}

/// Go `getStringSliceFromRequest`: parses a form arg as either a JSON array of
/// strings or a comma-separated list of strings.
pub(crate) fn get_string_slice_from_request(
    req: &Request,
    name: &str,
) -> Result<Vec<String>, String> {
    let s = req.form_value(name);
    if s.is_empty() {
        return Ok(Vec::new());
    }
    if s.starts_with('[') {
        return parse_json_string_array(s)
            .map_err(|e| format!("cannot unmarshal JSON array from {name}={s:?}: {e}"));
    }
    Ok(s.split(',').map(str::to_string).collect())
}

/// Go `parseDuration`: parses a LogsQL duration form arg with a default value.
pub(crate) fn parse_duration(
    req: &Request,
    name: &str,
    default_value: &str,
) -> Result<i64, String> {
    let mut s = req.form_value(name);
    if s.is_empty() {
        s = default_value;
    }
    match try_parse_duration(s) {
        Some(nsecs) => Ok(nsecs),
        None => Err(format!("cannot parse duration from the arg '{name}={s}'")),
    }
}

/// Go `timestampToString` / `timestampToRFC3339Nano`: formats nanoseconds as
/// UTC RFC3339Nano.
pub(crate) fn timestamp_to_string(nsecs: i64) -> String {
    let mut buf = Vec::new();
    marshal_timestamp_rfc3339_nano_string(&mut buf, nsecs);
    String::from_utf8_lossy(&buf).into_owned()
}

/// Appends `s` as a JSON string literal (surrounded by double quotes) to `dst`.
///
/// Mirrors `quicktemplate.AppendJSONString(dst, s, true)` for the escapes that
/// matter for valid JSON. Valid UTF-8 multibyte sequences (bytes >= 0x80) are
/// passed through unchanged.
pub(crate) fn append_json_string(dst: &mut Vec<u8>, s: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    dst.push(b'"');
    for &b in s {
        match b {
            b'"' => dst.extend_from_slice(b"\\\""),
            b'\\' => dst.extend_from_slice(b"\\\\"),
            b'\n' => dst.extend_from_slice(b"\\n"),
            b'\r' => dst.extend_from_slice(b"\\r"),
            b'\t' => dst.extend_from_slice(b"\\t"),
            0x00..=0x1f => {
                dst.extend_from_slice(b"\\u00");
                dst.push(HEX[(b >> 4) as usize]);
                dst.push(HEX[(b & 0x0f) as usize]);
            }
            _ => dst.push(b),
        }
    }
    dst.push(b'"');
}

/// Go `appendJSONRow`: appends the JSON object for row `row_idx` plus a trailing
/// newline. Empty field values are skipped (EsLogs treats empty fields as
/// non-existing). Fully empty rows are skipped entirely.
fn append_json_row(dst: &mut Vec<u8>, columns: &[BlockColumn], row_idx: usize) {
    let start = dst.len();
    dst.push(b'{');
    for c in columns {
        let value = &c.values[row_idx];
        if value.is_empty() {
            continue;
        }
        append_json_string(dst, c.name.as_bytes());
        dst.push(b':');
        append_json_string(dst, value);
        dst.push(b',');
    }
    if dst.len() - start == 1 {
        // Empty row: drop the '{' we appended.
        dst.truncate(start);
        return;
    }
    // Replace the trailing comma with "}\n".
    dst.pop();
    dst.extend_from_slice(b"}\n");
}

/// Mirrors Go `httpserver.SendPrometheusError`: logs the error and writes a
/// `422 Unprocessable Entity` response in the Prometheus querying API format
/// (`{"status":"error","errorType":"422","error":"..."}`).
pub(crate) fn send_prometheus_error(w: &mut ResponseWriter, req: &Request, err: &str) {
    esl_common::warnf!(
        "remoteAddr: {}; requestURI: {}; {}",
        get_quoted_remote_addr(req),
        req.request_uri(),
        err
    );
    w.set_status(422);
    w.set_header("Content-Type", "application/json");
    let mut body = Vec::new();
    body.extend_from_slice(br#"{"status":"error","errorType":"422","error":"#);
    append_json_string(&mut body, err.as_bytes());
    body.push(b'}');
    w.write_bytes(&body);
}

// ---------------------------------------------------------------------------
// Go strconv.Quote / regexp.QuoteMeta.
//
// PORT NOTE: esl-logstorage hosts pub(crate) ports of both in
// `stream_filter.rs`, but they are not exported from the crate; the filter
// composition below needs them, so compact copies live here (same PORT NOTE as
// stream_filter.rs: lift into a shared location eventually).
// ---------------------------------------------------------------------------

/// Port of Go `strconv.Quote`.
fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            ' '..='~' => out.push(c),
            '\x07' => out.push_str("\\a"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x0b' => out.push_str("\\v"),
            _ => {
                let n = c as u32;
                if n < 0x20 || n == 0x7f {
                    out.push_str(&format!("\\x{n:02x}"));
                } else if !c.is_control() && !c.is_whitespace() {
                    // PORT NOTE: approximation of Go unicode.IsPrint (see
                    // stream_filter.rs).
                    out.push(c);
                } else if n < 0x10000 {
                    out.push_str(&format!("\\u{n:04x}"));
                } else {
                    out.push_str(&format!("\\U{n:08x}"));
                }
            }
        }
    }
    out.push('"');
    out
}

/// Port of Go `regexp.QuoteMeta`.
fn regexp_quote_meta(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(
            c,
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
        ) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

// ---------------------------------------------------------------------------
// Minimal JSON scanning (Go uses fastjson / encoding/json; the port has no
// external deps, so the small subset needed by the logsql args is hand-rolled).
// ---------------------------------------------------------------------------

/// Byte-level scanner over a JSON document.
pub(crate) struct JsonScanner<'a> {
    s: &'a [u8],
    pos: usize,
}

impl<'a> JsonScanner<'a> {
    pub(crate) fn new(s: &'a str) -> Self {
        JsonScanner {
            s: s.as_bytes(),
            pos: 0,
        }
    }

    pub(crate) fn skip_ws(&mut self) {
        while matches!(self.s.get(self.pos), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    pub(crate) fn peek(&self) -> Option<u8> {
        self.s.get(self.pos).copied()
    }

    pub(crate) fn advance(&mut self) {
        self.pos += 1;
    }

    pub(crate) fn expect(&mut self, b: u8) -> Result<(), String> {
        if self.peek() == Some(b) {
            self.pos += 1;
            return Ok(());
        }
        Err(format!(
            "expecting {:?} at position {} in JSON",
            b as char, self.pos
        ))
    }

    /// Returns true when only trailing whitespace remains.
    pub(crate) fn at_end(&mut self) -> bool {
        self.skip_ws();
        self.pos == self.s.len()
    }

    /// Parses a JSON string literal starting at the current position.
    pub(crate) fn parse_string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut out: Vec<u8> = Vec::new();
        loop {
            let Some(b) = self.peek() else {
                return Err("unexpected end of JSON string".to_string());
            };
            self.pos += 1;
            match b {
                b'"' => {
                    return String::from_utf8(out)
                        .map_err(|_| "invalid UTF-8 in JSON string".to_string());
                }
                b'\\' => {
                    let Some(e) = self.peek() else {
                        return Err("unexpected end of JSON escape".to_string());
                    };
                    self.pos += 1;
                    match e {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(0x08),
                        b'f' => out.push(0x0c),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let c = self.parse_unicode_escape()?;
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                        }
                        _ => return Err(format!("invalid JSON escape \\{}", e as char)),
                    }
                }
                _ => out.push(b),
            }
        }
    }

    /// Parses the 4 hex digits after `\u`, combining UTF-16 surrogate pairs.
    /// Invalid surrogates map to U+FFFD, like Go `encoding/json`.
    fn parse_unicode_escape(&mut self) -> Result<char, String> {
        let hi = self.parse_hex4()?;
        if (0xD800..=0xDBFF).contains(&hi) {
            // Expect a low surrogate `\uXXXX` right after.
            if self.peek() == Some(b'\\') && self.s.get(self.pos + 1) == Some(&b'u') {
                self.pos += 2;
                let lo = self.parse_hex4()?;
                if (0xDC00..=0xDFFF).contains(&lo) {
                    let n = 0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00);
                    return Ok(char::from_u32(n).unwrap_or('\u{FFFD}'));
                }
                return Ok('\u{FFFD}');
            }
            return Ok('\u{FFFD}');
        }
        Ok(char::from_u32(hi).unwrap_or('\u{FFFD}'))
    }

    fn parse_hex4(&mut self) -> Result<u32, String> {
        let mut n: u32 = 0;
        for _ in 0..4 {
            let Some(b) = self.peek() else {
                return Err("unexpected end of \\u escape".to_string());
            };
            self.pos += 1;
            let d = match b {
                b'0'..=b'9' => (b - b'0') as u32,
                b'a'..=b'f' => (b - b'a' + 10) as u32,
                b'A'..=b'F' => (b - b'A' + 10) as u32,
                _ => return Err(format!("invalid hex digit {:?} in \\u escape", b as char)),
            };
            n = n * 16 + d;
        }
        Ok(n)
    }

    /// Parses a non-negative JSON integer (the only number shape needed here).
    pub(crate) fn parse_u64(&mut self) -> Result<u64, String> {
        let start = self.pos;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(format!("expecting a number at position {start} in JSON"));
        }
        let digits = std::str::from_utf8(&self.s[start..self.pos]).unwrap_or("");
        digits
            .parse::<u64>()
            .map_err(|e| format!("cannot parse number {digits:?}: {e}"))
    }
}

/// Parses a strict JSON array of strings (Go `json.Unmarshal(&[]string{})`).
fn parse_json_string_array(s: &str) -> Result<Vec<String>, String> {
    let mut sc = JsonScanner::new(s);
    sc.skip_ws();
    sc.expect(b'[')?;
    let mut out = Vec::new();
    sc.skip_ws();
    if sc.peek() == Some(b']') {
        sc.advance();
    } else {
        loop {
            sc.skip_ws();
            out.push(sc.parse_string()?);
            sc.skip_ws();
            match sc.peek() {
                Some(b',') => sc.advance(),
                Some(b']') => {
                    sc.advance();
                    break;
                }
                _ => return Err("expecting ',' or ']' in JSON array".to_string()),
            }
        }
    }
    if !sc.at_end() {
        return Err("unexpected trailing data after JSON array".to_string());
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// extra_filters / extra_stream_filters parsing (Go parseExtraFilters /
// parseExtraStreamFilters / parseExtraFiltersJSON).
// ---------------------------------------------------------------------------

/// Go `extraFilter`.
struct ExtraFilter {
    key: String,
    values: Vec<String>,
}

/// Go `parseExtraFiltersJSON`: parses `{"field":"value"|["v1","v2"],...}`.
fn parse_extra_filters_json(s: &str) -> Result<Vec<ExtraFilter>, String> {
    let mut sc = JsonScanner::new(s);
    sc.skip_ws();
    sc.expect(b'{')?;
    let mut filters: Vec<ExtraFilter> = Vec::new();
    sc.skip_ws();
    if sc.peek() == Some(b'}') {
        sc.advance();
    } else {
        loop {
            sc.skip_ws();
            let key = sc.parse_string()?;
            sc.skip_ws();
            sc.expect(b':')?;
            sc.skip_ws();
            match sc.peek() {
                Some(b'"') => {
                    let v = sc.parse_string()?;
                    filters.push(ExtraFilter {
                        key,
                        values: vec![v],
                    });
                }
                Some(b'[') => {
                    sc.advance();
                    let mut or_values: Vec<String> = Vec::new();
                    sc.skip_ws();
                    if sc.peek() == Some(b']') {
                        sc.advance();
                    } else {
                        loop {
                            sc.skip_ws();
                            if sc.peek() != Some(b'"') {
                                return Err(format!(
                                    "cannot obtain string item at the array for key {key:?}"
                                ));
                            }
                            or_values.push(sc.parse_string()?);
                            sc.skip_ws();
                            match sc.peek() {
                                Some(b',') => sc.advance(),
                                Some(b']') => {
                                    sc.advance();
                                    break;
                                }
                                _ => {
                                    return Err(format!(
                                        "expecting ',' or ']' in the array for key {key:?}"
                                    ));
                                }
                            }
                        }
                    }
                    if !or_values.is_empty() {
                        filters.push(ExtraFilter {
                            key,
                            values: or_values,
                        });
                    }
                }
                _ => {
                    return Err(format!("unexpected type of value for key {key:?}"));
                }
            }
            sc.skip_ws();
            match sc.peek() {
                Some(b',') => sc.advance(),
                Some(b'}') => {
                    sc.advance();
                    break;
                }
                _ => return Err("expecting ',' or '}' in JSON object".to_string()),
            }
        }
    }
    if !sc.at_end() {
        return Err("unexpected trailing data after JSON object".to_string());
    }
    Ok(filters)
}

/// Go `parseExtraFilters`. Returns `None` for an empty arg (Go returns a nil
/// `*Filter`).
pub(crate) fn parse_extra_filters(s: &str) -> Result<Option<Filter>, String> {
    if s.is_empty() {
        return Ok(None);
    }
    if !s.starts_with(r#"{""#) {
        return ParseFilter(s).map(Some);
    }

    // Extra filters in the form {"field":"value",...}.
    let kvs = parse_extra_filters_json(s)?;

    let filters: Vec<String> = kvs
        .iter()
        .map(|kv| {
            if kv.values.len() == 1 {
                format!("{}:={}", go_quote(&kv.key), go_quote(&kv.values[0]))
            } else {
                let or_values: Vec<String> = kv.values.iter().map(|v| go_quote(v)).collect();
                format!("{}:in({})", go_quote(&kv.key), or_values.join(","))
            }
        })
        .collect();
    ParseFilter(&filters.join(" ")).map(Some)
}

/// Go `parseExtraStreamFilters`. Returns `None` for an empty arg.
pub(crate) fn parse_extra_stream_filters(s: &str) -> Result<Option<Filter>, String> {
    if s.is_empty() {
        return Ok(None);
    }
    if !s.starts_with(r#"{""#) {
        return ParseFilter(s).map(Some);
    }

    // Extra stream filters in the form {"field":"value",...}.
    let kvs = parse_extra_filters_json(s)?;

    let filters: Vec<String> = kvs
        .iter()
        .map(|kv| {
            if kv.values.len() == 1 {
                format!("{}={}", go_quote(&kv.key), go_quote(&kv.values[0]))
            } else {
                let or_values: Vec<String> =
                    kv.values.iter().map(|v| regexp_quote_meta(v)).collect();
                format!("{}=~{}", go_quote(&kv.key), go_quote(&or_values.join("|")))
            }
        })
        .collect();
    ParseFilter(&format!("{{{}}}", filters.join(","))).map(Some)
}

// ---------------------------------------------------------------------------
// commonArgs / parseCommonArgs.
// ---------------------------------------------------------------------------

/// Go `commonArgs`: the parsed args shared by all `/select/logsql/*` endpoints.
///
/// PORT NOTE: Go also carries `allowPartialResponse` (cluster-only),
/// `hiddenFieldsFilters` and the `qs logstorage.QueryStats` context used by
/// `newQueryContext`/`updatePerQueryStatsMetrics`. The single-node Rust engine
/// surface (`Storage::run_query(tenant_ids, q, write_fn)`) has no query-context
/// or per-query stats plumbing, so those args are parsed for validation (their
/// parse errors are user-visible in Go) and dropped.
pub(crate) struct CommonArgs {
    /// The parsed query. It includes optional extra_filters,
    /// extra_stream_filters and (start, end) time range filter.
    pub(crate) q: Query,

    /// The list of tenantIDs to query.
    pub(crate) tenant_ids: Vec<TenantID>,

    /// The start of the selected time range aligned to the given step.
    pub(crate) start_aligned: i64,

    /// The aligned end of the selected time range aligned to the given step.
    pub(crate) end_aligned: i64,
}

impl CommonArgs {
    /// Go `commonArgs.writeResponseHeaders`: emits the request duration and
    /// tenant headers plus `Access-Control-Expose-Headers`.
    pub(crate) fn write_response_headers(&self, w: &mut ResponseWriter, start_time: Instant) {
        let mut expose: Vec<String> = vec!["ESL-Request-Duration-Seconds".to_string()];
        w.set_header(
            "ESL-Request-Duration-Seconds",
            &format!("{:.3}", start_time.elapsed().as_secs_f64()),
        );

        if self.tenant_ids.len() == 1 {
            // Write the used AccountID and ProjectID, so the client could show
            // them properly.
            expose.push("AccountID".to_string());
            expose.push("ProjectID".to_string());
            let tenant_id = &self.tenant_ids[0];
            w.set_header("AccountID", &tenant_id.account_id.to_string());
            w.set_header("ProjectID", &tenant_id.project_id.to_string());
        }

        let canonical: Vec<String> = expose.iter().map(|v| canonical_header_key(v)).collect();
        w.set_header("Access-Control-Expose-Headers", &canonical.join(", "));
    }
}

/// Go `http.CanonicalHeaderKey`: upper-cases the first letter of each
/// dash-separated token and lower-cases the rest.
fn canonical_header_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper = true;
    for c in s.chars() {
        if upper {
            out.push(c.to_ascii_uppercase());
        } else {
            out.push(c.to_ascii_lowercase());
        }
        upper = c == '-';
    }
    out
}

/// Go `parseCommonArgs`.
pub(crate) fn parse_common_args(req: &Request) -> Result<CommonArgs, String> {
    parse_common_args_with_config(req, false)
}

/// Go `parseCommonArgsWithConfig`.
///
/// PORT NOTE: `-search.maxQueryTimeRange` defaults to `0` (check disabled) and
/// command-line flags are not ported, so `skip_max_range_check` is accepted for
/// signature parity (Go passes `true` for live tailing) but the check is a
/// no-op either way.
pub(crate) fn parse_common_args_with_config(
    req: &Request,
    _skip_max_range_check: bool,
) -> Result<CommonArgs, String> {
    // Extract tenantID
    let tenant_id = get_tenant_id_from_request(req.header("AccountID"), req.header("ProjectID"))
        .map_err(|e| format!("cannot obtain tenantID: {e}"))?;
    let tenant_ids = vec![tenant_id];

    // Parse optional start and end args
    let start_opt = get_time_nsec(req, "start")?;
    let mut end_opt = get_time_nsec(req, "end")?;
    if let Some(end) = end_opt.as_mut() {
        // Treat HTTP 'end' query arg as exclusive: [start, end)
        // Convert to inclusive bound for internal filter by subtracting 1ns.
        if *end != i64::MIN {
            *end -= 1;
        }
    }

    // Parse optional time arg
    let time_opt = get_time_nsec(req, "time")?;

    let curr_timestamp = now_nsec();
    // If time arg is missing, then evaluate query either at the end timestamp
    // (if it is set) or at the current timestamp (if end query arg isn't set)
    let timestamp = time_opt.unwrap_or_else(|| end_opt.unwrap_or(curr_timestamp));

    // Parse query
    let mut q = parse_query_from_request(req, timestamp)?;

    // Parse ignore_pipes arg
    let mut ignore_pipes = false;
    get_bool_from_request(&mut ignore_pipes, req, "ignore_pipes")?;
    if ignore_pipes {
        q.drop_all_pipes();
    }

    let mut start = start_opt.unwrap_or(i64::MIN);
    let mut end = end_opt.unwrap_or(i64::MAX);
    if start_opt.is_some() || end_opt.is_some() {
        // Add _time:[start, end] filter if start or end args were set.
        let step_str = req.form_value("step");
        if !step_str.is_empty()
            && let Some(step) = try_parse_duration(step_str)
        {
            let mut offset = 0i64;
            let offset_str = req.form_value("offset");
            if !offset_str.is_empty()
                && let Some(nsecs) = try_parse_duration(offset_str)
            {
                offset = nsecs;
            }
            (start, end) = align_start_end_to_step(start, end, step, offset);
        }

        q.add_time_filter(start, end);
    }

    // Initialize startAligned and endAligned
    let mut start_aligned = i64::MIN;
    if start_opt.is_some() {
        start_aligned = start;
    }
    let mut end_aligned = i64::MAX;
    if end_opt.is_some() {
        end_aligned = end;
    }

    // Parse optional extra_filters
    for extra_filters_str in req.form_values("extra_filters") {
        if let Some(extra_filters) = parse_extra_filters(extra_filters_str)? {
            q.add_extra_filters(extra_filters);
        }
    }

    // Parse optional extra_stream_filters
    for extra_stream_filters_str in req.form_values("extra_stream_filters") {
        if let Some(extra_stream_filters) = parse_extra_stream_filters(extra_stream_filters_str)? {
            q.add_extra_filters(extra_stream_filters);
        }
    }

    // PORT NOTE: Go checks q.GetFilterTimeRange() against
    // -search.maxQueryTimeRange here; the flag defaults to 0 (disabled) and
    // flags are not ported, so the check is omitted.

    // allow_partial_response / hidden_fields_filters: validate the args like Go
    // (invalid values are user-visible errors), then drop them (see the
    // CommonArgs PORT NOTE).
    let mut allow_partial_response = false;
    get_bool_from_request(&mut allow_partial_response, req, "allow_partial_response")?;
    let _hidden_fields_filters = get_string_slice_from_request(req, "hidden_fields_filters")?;

    Ok(CommonArgs {
        q,
        tenant_ids,
        start_aligned,
        end_aligned,
    })
}

/// Go `parseQueryFromRequest`.
fn parse_query_from_request(req: &Request, timestamp: i64) -> Result<Query, String> {
    let q_str = req.form_value("query");
    if q_str.is_empty() {
        return Err("`query` arg cannot be empty".to_string());
    }
    if q_str.len() > MAX_QUERY_LEN {
        return Err(format!(
            "the `query` arg length cannot exceed -search.maxQueryLen={MAX_QUERY_LEN} bytes; \
             the current query length is {} bytes; query={q_str}",
            q_str.len()
        ));
    }
    esl_logstorage::parser::ParseQueryAtTimestamp(q_str, timestamp)
        .map_err(|e| format!("cannot parse `query` arg: {e}; query={q_str}"))
}

/// Go `alignStartEndToStep`.
pub(crate) fn align_start_end_to_step(
    mut start: i64,
    mut end: i64,
    step: i64,
    offset: i64,
) -> (i64, i64) {
    if step <= 0 {
        return (start, end);
    }

    start = sub_int64_no_overflow(start, -offset);
    if start >= 0 {
        start -= start % step;
    } else {
        let d = step + start % step;
        start = sub_int64_no_overflow(start, d);
    }
    start = sub_int64_no_overflow(start, offset);

    end = sub_int64_no_overflow(end, -offset);
    if end <= 0 {
        end -= end % step;
    } else {
        let d = step - end % step;
        end = sub_int64_no_overflow(end, -d);
    }
    end = sub_int64_no_overflow(end, offset);

    end = end.saturating_sub(1);

    (start, end)
}

// ---------------------------------------------------------------------------
// /select/logsql/query_time_range (Go ProcessQueryTimeRangeRequest).
// ---------------------------------------------------------------------------

/// Handles `/select/logsql/query_time_range` (Go `ProcessQueryTimeRangeRequest`).
///
/// Returns a JSON object with the really selected time range of the provided
/// query in RFC3339Nano format:
/// `{"start":"...","end":"...","hasTimeFilter":true|false}`.
pub(crate) fn process_query_time_range_request(req: &Request, w: &mut ResponseWriter) {
    let (min_timestamp, max_timestamp, has_time_filter) = match parse_query_time_range_args(req) {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    w.set_header("Content-Type", "application/json");

    let start_str = timestamp_to_string(min_timestamp);
    let end_str = timestamp_to_string(max_timestamp);
    w.write_str(&format!(
        r#"{{"start":{start_str:?},"end":{end_str:?},"hasTimeFilter":{has_time_filter}}}"#
    ));
}

/// Go `parseQueryTimeRangeArgs`.
fn parse_query_time_range_args(req: &Request) -> Result<(i64, i64, bool), String> {
    let curr_timestamp = now_nsec();
    let q = parse_query_from_request(req, curr_timestamp)?;

    let (mut min_timestamp, mut max_timestamp) = q.get_filter_time_range();

    // hasTimeFilter is true if the query itself contains a _time filter
    let has_time_filter = min_timestamp != i64::MIN || max_timestamp != i64::MAX;

    if min_timestamp == i64::MIN
        && let Some(start) = get_time_nsec(req, "start")?
    {
        min_timestamp = start;
    }
    if max_timestamp == i64::MAX
        && let Some(end) = get_time_nsec(req, "end")?
    {
        max_timestamp = end;
    }

    Ok((min_timestamp, max_timestamp, has_time_filter))
}

// ---------------------------------------------------------------------------
// /select/logsql/query (Go ProcessQueryRequest).
// ---------------------------------------------------------------------------

/// Handles `/select/logsql/query` (Go `ProcessQueryRequest`).
///
/// Parses the common args (query/start/end/time/extra_filters/... via
/// [`parse_common_args`]) plus `offset`/`limit`, runs the query via
/// `Storage::run_query`, and streams each `DataBlock` as newline-delimited JSON
/// objects (`application/stream+json`).
pub(crate) fn process_query_request(storage: &Arc<Storage>, req: &Request, w: &mut ResponseWriter) {
    let start_time = Instant::now();

    let mut ca = match parse_common_args(req) {
        Ok(ca) => ca,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Parse offset query arg
    let offset = match get_positive_int(req, "offset") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    // Parse limit query arg
    let limit = match get_positive_int(req, "limit") {
        Ok(v) => v,
        Err(e) => {
            w.errorf(req, &e);
            return;
        }
    };

    let format = req.form_value("format");
    if !format.is_empty() && format != "csv" {
        w.errorf(
            req,
            &format!("unexpected format={format:?}; expecting 'csv' or ''"),
        );
        return;
    }
    if limit > 0 {
        // Add '| sort by (_time) desc | offset <offset> | limit <limit>' to the
        // end of the query. This pattern is automatically optimized during
        // query execution - see https://github.com/VictoriaMetrics/VictoriaLogs/issues/96 .
        if ca.q.can_return_last_n_results() {
            ca.q.add_pipe_sort_by_time_desc();
        }
        ca.q.add_pipe_offset_limit(offset as u64, limit as u64);
    }

    // Go ProcessQueryRequest format=csv branch: resolve the csv header fields.
    let mut csv_header: Vec<u8> = Vec::new();
    if format == "csv" {
        let fields = match ca.q.get_fixed_fields() {
            Some(fields) => fields,
            None => {
                // Slow path - detect the fields by scanning the logs for the
                // given query.
                let field_names = match storage.get_field_names(&ca.tenant_ids, &ca.q, "") {
                    Ok(v) => v,
                    Err(e) => {
                        w.errorf(
                            req,
                            &format!(
                                "cannot obtain field names for returning query results in csv format: {e}"
                            ),
                        );
                        return;
                    }
                };
                let mut fields: Vec<String> = field_names.into_iter().map(|vh| vh.value).collect();
                fields.sort();
                ca.q.add_pipe_fields(&fields);
                fields
            }
        };
        crate::csv::append_csv_line(&mut csv_header, &fields);
    }

    let need_sort_fields = !ca.q.is_fixed_output_fields_order();
    if format == "csv" && need_sort_fields {
        esl_common::panicf!("BUG: need_sort_fields must be false for format=csv");
    }
    let append_row: fn(&mut Vec<u8>, &[BlockColumn], usize) = if format == "csv" {
        crate::csv::append_csv_row
    } else {
        append_json_row
    };

    // Collect ndjson output. write_block_fn is invoked concurrently by parallel
    // query workers, so the shared sink is guarded by a Mutex. Each block builds
    // its rows into a local buffer (no lock held during JSON encoding) and the
    // lock is taken once per block to append.
    //
    // PORT NOTE: Go streams directly to the http.ResponseWriter via a
    // syncWriter. The Rust WriteDataBlockFn must be 'static (Arc<dyn Fn ...>),
    // so it cannot borrow the ResponseWriter; output is buffered into an owned
    // Vec and written to the response after run_query returns. Row order across
    // workers is not deterministic in either implementation.
    let sink: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let sink_cl = Arc::clone(&sink);
    let write_fn: WriteDataBlockFn = Arc::new(move |_worker_id, db: &mut DataBlock| {
        let rows_count = db.rows_count();
        if rows_count == 0 {
            return;
        }
        let columns = db.get_columns(need_sort_fields);
        let mut buf = Vec::new();
        for i in 0..rows_count {
            append_row(&mut buf, columns, i);
        }
        if !buf.is_empty() {
            sink_cl.lock().unwrap().extend_from_slice(&buf);
        }
    });

    // Perf diagnostic (ESL_QUERY_TIMING=1): splits handler time into run_query
    // vs response-write so platform-specific gaps can be attributed.
    let timing = std::env::var_os("ESL_QUERY_TIMING").is_some();
    let t0 = std::time::Instant::now();

    // PORT NOTE: Go routes this through eslstorage.RunQuery, whose last-N
    // optimization narrows the time range with several binary-search
    // subqueries before running the final query. That app-level workaround
    // exists because Go's engine scans every block for `sort by (_time) desc
    // limit N`; this port's engine already prunes those blocks natively
    // (newest-first scheduling + top-N heap feedback in search_parallel), and
    // measured end-to-end the Go dispatch is ~5x slower here (41ms vs 8ms on
    // the 500k benchmark corpus). The faithful port lives in
    // esl_storage::run_query / lastn_optimization (unit- and e2e-tested);
    // the direct engine path is used deliberately.
    if let Err(e) = storage.run_query(&ca.tenant_ids, &ca.q, write_fn) {
        w.errorf(req, &format!("cannot execute query [{}]: {e}", ca.q));
        return;
    }
    let t_run = t0.elapsed();

    if format == "csv" {
        w.set_header("Content-Type", "text/csv");
    } else {
        w.set_header("Content-Type", "application/stream+json");
    }
    ca.write_response_headers(w, start_time);
    if format == "csv" {
        // Header is written unconditionally even for empty results, matching
        // Go's final writeResponseHeadersOnce call.
        w.write_bytes(&csv_header);
    }
    let body = match Arc::try_unwrap(sink) {
        Ok(m) => m.into_inner().unwrap(),
        // All query workers have joined, so take the buffer instead of cloning.
        Err(arc) => std::mem::take(&mut *arc.lock().unwrap()),
    };
    w.write_bytes(&body);
    if timing {
        eprintln!(
            "ESL_QUERY_TIMING query=\"{}\" run_query_us={} write_us={} body_bytes={}",
            ca.q,
            t_run.as_micros(),
            (t0.elapsed() - t_run).as_micros(),
            body.len()
        );
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Shared test helpers for the `logsql_*` endpoint round-trip tests.

    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::path::PathBuf;
    use std::sync::Arc;

    use esl_logstorage::log_rows::get_log_rows;
    use esl_logstorage::rows::Field;
    use esl_logstorage::storage::{Storage, StorageConfig};
    use esl_logstorage::tenant_id::TenantID;

    pub(crate) fn unique_nsec() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }

    /// Opens a temp Storage, ingests `rows` as `(_msg, host)` pairs starting at
    /// timestamp `base` (1ns apart), and flushes. Returns the storage and its
    /// on-disk path (remove it with `esl_common::fs::must_remove_dir`).
    pub(crate) fn open_storage_with_rows(
        name: &str,
        base: i64,
        rows: &[(&str, &str)],
    ) -> (Arc<Storage>, PathBuf) {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "esl-select-{name}-{}-{}",
            std::process::id(),
            unique_nsec()
        ));
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);

        let tenant = TenantID::default();
        let mut lr = get_log_rows(&["host"], &[], &[], &[], "");
        for (i, (msg, host)) in rows.iter().enumerate() {
            let mut fields = vec![
                Field {
                    name: "_msg".to_string(),
                    value: msg.to_string(),
                },
                Field {
                    name: "host".to_string(),
                    value: host.to_string(),
                },
            ];
            lr.must_add(tenant, base + i as i64, &mut fields, -1);
        }
        s.must_add_rows(&lr);
        s.debug_flush();
        (s, path)
    }

    /// Performs a raw HTTP/1.1 GET and returns (status_code, body).
    pub(crate) fn http_get(addr: SocketAddr, target: &str) -> (u16, String) {
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

    /// Percent-encodes a query-string value.
    pub(crate) fn encode(q: &str) -> String {
        let mut out = String::new();
        for b in q.bytes() {
            match b {
                b'A'..=b'Z'
                | b'a'..=b'z'
                | b'0'..=b'9'
                | b'-'
                | b'_'
                | b'.'
                | b'~'
                | b'*'
                | b'('
                | b')' => out.push(b as char),
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Port of Go TestParseExtraFilters_Success.
    #[test]
    fn test_parse_extra_filters_success() {
        fn f(s: &str, result_expected: &str) {
            let filter = parse_extra_filters(s).expect("unexpected error in parse_extra_filters");
            let result = filter.map(|f| f.to_string()).unwrap_or_default();
            assert_eq!(
                result, result_expected,
                "unexpected result for {s:?}\ngot\n{result}\nwant\n{result_expected}"
            );
        }

        f("", "");

        // JSON string
        f(r#"{"foo":"bar"}"#, "foo:=bar");
        f(r#"{"foo":["bar","baz"]}"#, "foo:in(bar,baz)");
        f(
            r#"{"z":"=b ","c":["d","e,"],"a":[],"_msg":"x"}"#,
            r#"z:="=b " c:in(d,"e,") =x"#,
        );

        // LogsQL filter
        f("foobar", "foobar");
        f("foo:bar", "foo:bar");
        // PORT NOTE: Go expects `{foo="bar",baz="z"} (foo:bar or foo:baz)
        // error _time:5m` here. The parsed filter tree is identical and
        // FilterAnd now parenthesizes or-children like Go; the remaining
        // divergence is the deferred mergeFiltersStream optimize pass (stream
        // filter moved to front — parser/mod.rs PORT NOTE).
        f(
            r#"foo:(bar or baz) error _time:5m {"foo"=bar,baz="z"}"#,
            r#"(foo:bar or foo:baz) error _time:5m {foo="bar",baz="z"}"#,
        );
    }

    // Port of Go TestParseExtraFilters_Failure.
    #[test]
    fn test_parse_extra_filters_failure() {
        fn f(s: &str) {
            assert!(
                parse_extra_filters(s).is_err(),
                "expecting non-nil error for {s:?}"
            );
        }

        // Invalid JSON
        f(r#"{"foo"}"#);
        f("[1,2]");
        f(r#"{"foo":[1]}"#);

        // Invalid LogsQL filter
        f("foo:(bar");

        // excess pipe
        f("foo | count()");
    }

    // Port of Go TestParseExtraStreamFilters_Success.
    #[test]
    fn test_parse_extra_stream_filters_success() {
        fn f(s: &str, result_expected: &str) {
            let filter = parse_extra_stream_filters(s)
                .expect("unexpected error in parse_extra_stream_filters");
            let result = filter.map(|f| f.to_string()).unwrap_or_default();
            assert_eq!(
                result, result_expected,
                "unexpected result for {s:?};\ngot\n{result}\nwant\n{result_expected}"
            );
        }

        f("", "");

        // JSON string
        f(r#"{"foo":"bar"}"#, r#"{foo="bar"}"#);
        f(r#"{"foo":["bar","baz"]}"#, r#"{foo=~"bar|baz"}"#);
        f(
            r#"{"z":"b","c":["d","e|\""],"a":[],"_msg":"x"}"#,
            r#"{z="b",c=~"d|e\\|\"",_msg="x"}"#,
        );

        // LogsQL filter
        f("foobar", "foobar");
        f("foo:bar", "foo:bar");
        // PORT NOTE: Go expects `{foo="bar",baz="z"} (foo:bar or foo:baz)
        // error _time:5m` here; the or-grouping now matches Go, the deferred
        // mergeFiltersStream pass (stream filter moved to front) is the
        // remaining Display divergence.
        f(
            r#"foo:(bar or baz) error _time:5m {"foo"=bar,baz="z"}"#,
            r#"(foo:bar or foo:baz) error _time:5m {foo="bar",baz="z"}"#,
        );
    }

    // Port of Go TestParseExtraStreamFilters_Failure.
    #[test]
    fn test_parse_extra_stream_filters_failure() {
        fn f(s: &str) {
            assert!(
                parse_extra_stream_filters(s).is_err(),
                "expecting non-nil error for {s:?}"
            );
        }

        // Invalid JSON
        f(r#"{"foo"}"#);
        f("[1,2]");
        f(r#"{"foo":[1]}"#);

        // Invalid LogsQL filter
        f("foo:(bar");

        // excess pipe
        f("foo | count()");
    }

    /// Round-trip test: /select/logsql/query honors the `start`/`end` time
    /// filter and `extra_filters`/`extra_stream_filters` applied by
    /// parse_common_args (replacing the earlier PORT-NOTEd divergence where
    /// `start` was ignored).
    #[test]
    fn test_process_query_request_common_args_roundtrip() {
        use test_support::{encode, http_get, open_storage_with_rows, unique_nsec};

        let base = unique_nsec();
        let rows = [
            ("connection error occurred", "node-1"),
            ("all systems nominal", "node-1"),
            ("disk error on node 3", "node-2"),
            ("request completed ok", "node-2"),
            ("cache warmed", "node-2"),
        ];
        let (storage, path) = open_storage_with_rows("query-common", base, &rows);

        let storage_h = std::sync::Arc::clone(&storage);
        let handle = esl_common::httpserver::serve("127.0.0.1:0", move |req, w| match req.path() {
            "/select/logsql/query" => process_query_request(&storage_h, req, w),
            "/select/logsql/query_time_range" => process_query_time_range_request(req, w),
            _ => w.errorf(req, "unexpected path"),
        })
        .expect("serve");
        let addr = handle.local_addr();

        let count_rows = |target: &str| -> usize {
            let (status, body) = http_get(addr, target);
            assert_eq!(status, 200, "target={target} body={body}");
            body.lines().filter(|l| !l.is_empty()).count()
        };

        // No time filter: all 5 rows.
        assert_eq!(
            count_rows(&format!("/select/logsql/query?query={}", encode("*"))),
            5
        );

        // start in the far future: the _time filter excludes everything.
        assert_eq!(
            count_rows(&format!(
                "/select/logsql/query?query={}&start={}",
                encode("*"),
                encode("2100-01-01T00:00:00Z")
            )),
            0
        );

        // start in the far past: all rows still match.
        assert_eq!(
            count_rows(&format!(
                "/select/logsql/query?query={}&start={}",
                encode("*"),
                encode("2000-01-01T00:00:00Z")
            )),
            5
        );

        // end in the far past excludes everything.
        assert_eq!(
            count_rows(&format!(
                "/select/logsql/query?query={}&end={}",
                encode("*"),
                encode("2000-01-01T00:00:00Z")
            )),
            0
        );

        // extra_filters in JSON form.
        assert_eq!(
            count_rows(&format!(
                "/select/logsql/query?query={}&extra_filters={}",
                encode("*"),
                encode(r#"{"host":"node-1"}"#)
            )),
            2
        );

        // extra_filters in LogsQL form.
        assert_eq!(
            count_rows(&format!(
                "/select/logsql/query?query={}&extra_filters={}",
                encode("*"),
                encode("host:node-2")
            )),
            3
        );

        // extra_stream_filters execution: Go filters by `{host="node-2"}`.
        assert_eq!(
            count_rows(&format!(
                "/select/logsql/query?query={}&extra_stream_filters={}",
                encode("*"),
                encode(r#"{host="node-2"}"#)
            )),
            3
        );

        // limit trims the result set.
        assert_eq!(
            count_rows(&format!(
                "/select/logsql/query?query={}&limit=1",
                encode("*")
            )),
            1
        );

        // query_time_range echoes the start/end args when the query has no
        // _time filter ('end' is not made exclusive on this endpoint).
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/query_time_range?query={}&start={}&end={}",
                encode("*"),
                encode("2024-01-01T00:00:00Z"),
                encode("2024-01-01T01:00:00Z")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert_eq!(
            body,
            r#"{"start":"2024-01-01T00:00:00Z","end":"2024-01-01T01:00:00Z","hasTimeFilter":false}"#
        );

        // A query-side _time filter wins and sets hasTimeFilter.
        let (status, body) = http_get(
            addr,
            &format!(
                "/select/logsql/query_time_range?query={}",
                encode("_time:2024")
            ),
        );
        assert_eq!(status, 200, "body={body}");
        assert!(body.contains(r#""hasTimeFilter":true"#), "body={body}");

        handle.stop();
        storage.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_canonical_header_key() {
        assert_eq!(
            canonical_header_key("ESL-Request-Duration-Seconds"),
            "Esl-Request-Duration-Seconds"
        );
        assert_eq!(canonical_header_key("AccountID"), "Accountid");
    }
}

//! Port of `lib/logstorage/pipe_format.go` — the `| format "..."` pipe, which
//! builds a result field from a pattern string with `<field>` placeholders and
//! optional per-field transformations (`<q:foo>`, `<uc:foo>`, `<base64encode:x>`,
//! ...).
//!
//! PORT NOTE — parser: the top-level `parsePipeFormat(lex)` is lexer-dependent
//! and therefore deferred; callers build the pipe via the [`PipeFormat::new`]
//! constructor from already-parsed arguments. The self-contained pattern parser
//! (`parsePatternSteps`) is reused from [`crate::pattern`].
//!
//! PORT NOTE — arena: Go threads a pooled `arena`/`bytesutil` buffer through
//! `formatRow`; the Rust port returns owned `String`s. Behavior is identical;
//! only per-call allocation pooling differs (acceptable per CONVENTIONS).
//!
//! PORT NOTE — base64: Go uses `encoding/base64` `StdEncoding`; no base64 crate
//! is vendored, so [`append_base64_encode`]/[`append_base64_decode`] hand-roll
//! the standard (padded) alphabet with identical semantics.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use esl_common::stringsutil::json_string;
use esl_common::timeutil::try_parse_unix_timestamp;

use crate::bitmap::Bitmap;
use crate::block_result::{BlockResult, ResultColumn};
use crate::filter_generic::is_msg_field_name;
use crate::pattern::{PatternStep, parse_pattern_steps};
use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_update::{IfFilter, should_deny_overwritten_field};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;
use crate::values_encoder::{
    marshal_duration_string, marshal_float64_string, marshal_ipv4_string,
    marshal_timestamp_rfc3339_nano_string, marshal_uint64_string, try_parse_duration,
    try_parse_int64, try_parse_uint64,
};

// ---------------------------------------------------------------------------
// PipeFormat (Go `pipeFormat`)
// ---------------------------------------------------------------------------

/// The `| format ...` pipe.
pub(crate) struct PipeFormat {
    format_str: String,
    steps: Vec<PatternStep>,

    result_field: String,

    keep_original_fields: bool,
    skip_empty_results: bool,

    /// Optional `if (...)` filter for skipping the format func.
    iff: Option<Arc<IfFilter>>,
}

impl PipeFormat {
    /// Builds a `format` pipe from parsed arguments.
    ///
    /// PORT NOTE: replaces the lexer-based `parsePipeFormat`. `format_str` is the
    /// raw pattern; `result_field` is the `as ...` target (`"_msg"` by default).
    pub(crate) fn new(
        format_str: impl Into<String>,
        result_field: impl Into<String>,
        keep_original_fields: bool,
        skip_empty_results: bool,
        iff: Option<Arc<IfFilter>>,
    ) -> Result<Self, String> {
        let format_str = format_str.into();
        let steps = parse_pattern_steps(&format_str)
            .map_err(|e| format!("cannot parse 'pattern' {format_str:?}: {e}"))?;

        // Verify that all the fields mentioned in the format pattern do not end with '*'.
        for step in &steps {
            if prefix_filter::is_wildcard_filter(&step.field) {
                return Err(format!(
                    "'pattern' {:?} cannot contain wildcard fields like {:?}",
                    format_str, step.field
                ));
            }
        }

        Ok(Self {
            format_str,
            steps,
            result_field: result_field.into(),
            keep_original_fields,
            skip_empty_results,
            iff,
        })
    }
}

impl Pipe for PipeFormat {
    /// Go `hasFilterInWithQuery` for this pipe: checks the `if (...)` filter.
    fn has_filter_in_with_query(&self) -> bool {
        self.iff
            .as_ref()
            .is_some_and(|iff| iff.has_filter_in_with_query())
    }

    /// Go `initFilterInValues` for this pipe: rewrites the `if (...)` filter.
    fn init_filter_in_values(
        &mut self,
        get_values: &mut crate::storage_search::GetFieldValuesFn<'_>,
        timestamp: i64,
    ) -> Result<(), String> {
        if let Some(iff) = &self.iff
            && let Some(iff_new) = iff.init_filter_in_values(get_values, timestamp)?
        {
            self.iff = Some(Arc::new(iff_new));
        }
        Ok(())
    }

    fn to_string(&self) -> String {
        let mut s = String::from("format");
        if let Some(iff) = &self.iff {
            s.push(' ');
            s.push_str(&iff.to_string());
        }
        s.push(' ');
        s.push_str(&quote_token_if_needed(&self.format_str));
        if !is_msg_field_name(&self.result_field) {
            s.push_str(" as ");
            s.push_str(&quote_token_if_needed(&self.result_field));
        }
        if self.keep_original_fields {
            s.push_str(" keep_original_fields");
        }
        if self.skip_empty_results {
            s.push_str(" skip_empty_results");
        }
        s
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        self.result_field != "_time"
    }

    fn stats_labels_tail_op(&self) -> Option<crate::pipe::StatsTailOp> {
        // The format pipe generates an additional by(...) label.
        Some(crate::pipe::StatsTailOp::Format {
            result_field: self.result_field.clone(),
        })
    }

    fn update_needed_fields(&self, f: &mut prefix_filter::Filter) {
        if !f.match_string(&self.result_field) {
            return;
        }

        if let Some(iff) = &self.iff {
            f.add_allow_filters(&iff.allow_filters);
        } else if should_deny_overwritten_field(
            self.iff.as_deref(),
            self.keep_original_fields,
            self.skip_empty_results,
        ) {
            f.add_deny_filter(&self.result_field);
        }
        for step in &self.steps {
            if !step.field.is_empty() {
                f.add_allow_filter(&step.field);
            }
        }
    }

    fn new_pipe_processor(
        &self,
        concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let shards = (0..concurrency.max(1))
            .map(|_| Mutex::new(PipeFormatProcessorShard::default()))
            .collect();
        Arc::new(PipeFormatProcessor {
            steps: self.steps.clone(),
            result_field: self.result_field.clone(),
            keep_original_fields: self.keep_original_fields,
            skip_empty_results: self.skip_empty_results,
            iff: self.iff.clone(),
            pp_next,
            shards,
        })
    }
}

// ---------------------------------------------------------------------------
// PipeFormatProcessor (Go `pipeFormatProcessor`)
// ---------------------------------------------------------------------------

struct PipeFormatProcessor {
    steps: Vec<PatternStep>,
    result_field: String,
    keep_original_fields: bool,
    skip_empty_results: bool,
    iff: Option<Arc<IfFilter>>,
    pp_next: Arc<dyn PipeProcessor>,
    shards: Vec<Mutex<PipeFormatProcessorShard>>,
}

#[derive(Default)]
struct PipeFormatProcessorShard {
    bm: Bitmap,
    rc: ResultColumn,
}

impl PipeProcessor for PipeFormatProcessor {
    fn write_block(&self, worker_id: usize, br: &mut BlockResult) {
        if br.rows_len() == 0 {
            return;
        }

        let mut shard = self.shards[worker_id].lock().unwrap();

        let has_iff = self.iff.is_some();
        if let Some(iff) = &self.iff {
            shard.bm.init(br.rows_len());
            shard.bm.set_bits();
            iff.f.apply_to_block_result(br, &mut shard.bm);
            if shard.bm.is_zero() {
                drop(shard);
                self.pp_next.write_block(worker_id, br);
                return;
            }
        }

        shard.rc.name = self.result_field.clone();

        let result_column = br.get_column_by_name(&self.result_field);
        for row_idx in 0..br.rows_len() {
            let v = if !has_iff || shard.bm.is_set_bit(row_idx) {
                let mut v = format_row(&self.steps, br, row_idx);
                if (v.is_empty() && self.skip_empty_results) || self.keep_original_fields {
                    let v_orig = br.column_get_value_at_row(result_column, row_idx);
                    if !v_orig.is_empty() {
                        v = v_orig.to_string();
                    }
                }
                v
            } else {
                br.column_get_value_at_row(result_column, row_idx)
                    .to_string()
            };
            shard.rc.add_value(v.as_bytes());
        }

        let rc = std::mem::take(&mut shard.rc);
        br.add_result_column(rc);
        drop(shard);
        self.pp_next.write_block(worker_id, br);
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Port of Go `(*pipeFormatProcessorShard).formatRow`.
fn format_row(steps: &[PatternStep], br: &mut BlockResult, row_idx: usize) -> String {
    let mut b: Vec<u8> = Vec::new();
    for step in steps {
        b.extend_from_slice(step.prefix.as_bytes());
        if step.field.is_empty() {
            continue;
        }

        let c = br.get_column_by_name(&step.field);
        let v = br.column_get_value_at_row(c, row_idx).to_string();
        match step.field_opt.as_str() {
            "base64decode" => {
                if !append_base64_decode(&mut b, &v) {
                    b.extend_from_slice(v.as_bytes());
                }
            }
            "base64encode" => append_base64_encode(&mut b, &v),
            "duration" => match try_parse_int64(&v) {
                Some(nsecs) => marshal_duration_string(&mut b, nsecs),
                None => b.extend_from_slice(v.as_bytes()),
            },
            "duration_seconds" => match try_parse_duration(&v) {
                Some(nsecs) => {
                    let secs = nsecs as f64 / 1e9;
                    marshal_float64_string(&mut b, secs);
                }
                None => b.extend_from_slice(v.as_bytes()),
            },
            "hexdecode" => append_hex_decode(&mut b, &v),
            "hexencode" => append_hex_encode(&mut b, &v),
            "hexnumdecode" => append_hex_uint64_decode(&mut b, &v),
            "hexnumencode" => match try_parse_uint64(&v) {
                Some(n) => append_hex_uint64_encode(&mut b, n),
                None => b.extend_from_slice(v.as_bytes()),
            },
            "ipv4" => match try_parse_uint64(&v) {
                Some(ip_num) if ip_num <= u32::MAX as u64 => {
                    marshal_ipv4_string(&mut b, ip_num as u32);
                }
                _ => b.extend_from_slice(v.as_bytes()),
            },
            "lc" => append_lowercase(&mut b, &v),
            "time" => match try_parse_unix_timestamp(&v) {
                Some(nsecs) => marshal_timestamp_rfc3339_nano_string(&mut b, nsecs),
                None => b.extend_from_slice(v.as_bytes()),
            },
            "q" => b.extend_from_slice(json_string(&v).as_bytes()),
            "uc" => append_uppercase(&mut b, &v),
            "urldecode" => append_url_decode(&mut b, &v),
            "urlencode" => append_url_encode(&mut b, &v),
            _ => b.extend_from_slice(v.as_bytes()),
        }
    }

    String::from_utf8_lossy(&b).into_owned()
}

// ---------------------------------------------------------------------------
// String transformation helpers (ports of the `append*` funcs in Go).
// ---------------------------------------------------------------------------

/// PORT NOTE: Go uses `unicode.ToUpper`, a single-rune mapping; Rust's
/// `char::to_uppercase` applies full Unicode case mapping (a rune may expand,
/// e.g. `ß` -> `SS`). Identical for the ASCII/BMP inputs exercised here.
fn append_uppercase(dst: &mut Vec<u8>, s: &str) {
    for ch in s.chars() {
        for u in ch.to_uppercase() {
            let mut buf = [0u8; 4];
            dst.extend_from_slice(u.encode_utf8(&mut buf).as_bytes());
        }
    }
}

/// PORT NOTE: see [`append_uppercase`] for the `to_lowercase` caveat.
fn append_lowercase(dst: &mut Vec<u8>, s: &str) {
    for ch in s.chars() {
        for l in ch.to_lowercase() {
            let mut buf = [0u8; 4];
            dst.extend_from_slice(l.encode_utf8(&mut buf).as_bytes());
        }
    }
}

fn append_url_decode(dst: &mut Vec<u8>, s: &str) {
    let mut s = s.as_bytes();
    while !s.is_empty() {
        let Some(n) = s.iter().position(|&c| c == b'%' || c == b'+') else {
            dst.extend_from_slice(s);
            return;
        };
        dst.extend_from_slice(&s[..n]);
        let ch = s[n];
        s = &s[n + 1..];
        if ch == b'+' {
            dst.push(b' ');
            continue;
        }
        if s.len() < 2 {
            dst.push(b'%');
            continue;
        }
        match (unhex_char(s[0]), unhex_char(s[1])) {
            (Some(hi), Some(lo)) => {
                dst.push((hi << 4) | lo);
                s = &s[2..];
            }
            _ => dst.push(b'%'),
        }
    }
}

fn append_url_encode(dst: &mut Vec<u8>, s: &str) {
    for &c in s.as_bytes() {
        // See http://www.w3.org/TR/html5/forms.html#form-submission-algorithm
        if c.is_ascii_alphanumeric() || c == b'-' || c == b'.' || c == b'_' {
            dst.push(c);
        } else if c == b' ' {
            dst.push(b'+');
        } else {
            dst.push(b'%');
            dst.push(hex_char_upper(c >> 4));
            dst.push(hex_char_upper(c & 15));
        }
    }
}

fn hex_char_upper(c: u8) -> u8 {
    if c < 10 { b'0' + c } else { c - 10 + b'A' }
}

fn unhex_char(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'A'..=b'F' => Some(c - b'A' + 10),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

fn append_hex_uint64_encode(dst: &mut Vec<u8>, n: u64) {
    let mut shift: i32 = 60;
    while shift >= 0 {
        dst.push(hex_char_upper(((n >> shift) & 15) as u8));
        shift -= 4;
    }
}

fn append_hex_uint64_decode(dst: &mut Vec<u8>, s: &str) {
    if s.len() > 16 {
        dst.extend_from_slice(s.as_bytes());
        return;
    }
    let mut n: u64 = 0;
    for &c in s.as_bytes() {
        match unhex_char(c) {
            Some(x) => n = (n << 4) | u64::from(x),
            None => {
                dst.extend_from_slice(s.as_bytes());
                return;
            }
        }
    }
    marshal_uint64_string(dst, n);
}

fn append_hex_encode(dst: &mut Vec<u8>, s: &str) {
    for &c in s.as_bytes() {
        dst.push(hex_char_upper(c >> 4));
        dst.push(hex_char_upper(c & 15));
    }
}

fn append_hex_decode(dst: &mut Vec<u8>, s: &str) {
    let mut s = s.as_bytes();
    while s.len() >= 2 {
        match (unhex_char(s[0]), unhex_char(s[1])) {
            (Some(hi), Some(lo)) => dst.push((hi << 4) | lo),
            _ => {
                dst.push(s[0]);
                dst.push(s[1]);
            }
        }
        s = &s[2..];
    }
    dst.extend_from_slice(s);
}

const BASE64_STD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn append_base64_encode(dst: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    let mut i = 0;
    while i + 3 <= b.len() {
        let n = (u32::from(b[i]) << 16) | (u32::from(b[i + 1]) << 8) | u32::from(b[i + 2]);
        dst.push(BASE64_STD[(n >> 18) as usize & 63]);
        dst.push(BASE64_STD[(n >> 12) as usize & 63]);
        dst.push(BASE64_STD[(n >> 6) as usize & 63]);
        dst.push(BASE64_STD[n as usize & 63]);
        i += 3;
    }
    match b.len() - i {
        1 => {
            let n = u32::from(b[i]) << 16;
            dst.push(BASE64_STD[(n >> 18) as usize & 63]);
            dst.push(BASE64_STD[(n >> 12) as usize & 63]);
            dst.push(b'=');
            dst.push(b'=');
        }
        2 => {
            let n = (u32::from(b[i]) << 16) | (u32::from(b[i + 1]) << 8);
            dst.push(BASE64_STD[(n >> 18) as usize & 63]);
            dst.push(BASE64_STD[(n >> 12) as usize & 63]);
            dst.push(BASE64_STD[(n >> 6) as usize & 63]);
            dst.push(b'=');
        }
        _ => {}
    }
}

fn base64_val(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

/// Decodes standard (padded) base64 `s`, appending the bytes to `dst`. Returns
/// `false` (leaving `dst` unchanged) on any decode error, mirroring Go's
/// `StdEncoding.AppendDecode`.
fn append_base64_decode(dst: &mut Vec<u8>, s: &str) -> bool {
    let b = s.as_bytes();
    if !b.len().is_multiple_of(4) {
        return false;
    }
    let start = dst.len();
    let mut i = 0;
    while i < b.len() {
        let chunk = &b[i..i + 4];
        let mut vals = [0u8; 4];
        let mut pad = 0;
        for (k, &ch) in chunk.iter().enumerate() {
            if ch == b'=' {
                if i + 4 != b.len() || k < 2 {
                    dst.truncate(start);
                    return false;
                }
                pad += 1;
            } else {
                if pad > 0 {
                    dst.truncate(start);
                    return false;
                }
                match base64_val(ch) {
                    Some(v) => vals[k] = v,
                    None => {
                        dst.truncate(start);
                        return false;
                    }
                }
            }
        }
        let n = (u32::from(vals[0]) << 18)
            | (u32::from(vals[1]) << 12)
            | (u32::from(vals[2]) << 6)
            | u32::from(vals[3]);
        dst.push((n >> 16) as u8);
        if pad < 2 {
            dst.push((n >> 8) as u8);
        }
        if pad < 1 {
            dst.push(n as u8);
        }
        i += 4;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_update::test_utils::{assert_rows_eq, rows, run_pipe};

    fn enc(f: impl Fn(&mut Vec<u8>, &str)) -> impl Fn(&str) -> String {
        move |s| {
            let mut b = Vec::new();
            f(&mut b, s);
            String::from_utf8(b).unwrap()
        }
    }

    #[test]
    fn test_append_uppercase() {
        let up = enc(append_uppercase);
        assert_eq!(up(""), "");
        assert_eq!(up("foo"), "FOO");
        assert_eq!(up("лДЫ"), "ЛДЫ");
    }

    #[test]
    fn test_append_lowercase() {
        let lc = enc(append_lowercase);
        assert_eq!(lc(""), "");
        assert_eq!(lc("FoO"), "foo");
        assert_eq!(lc("ЛДЫ"), "лды");
    }

    #[test]
    fn test_append_url_encode() {
        let e = enc(append_url_encode);
        assert_eq!(e(""), "");
        assert_eq!(e("foo"), "foo");
        assert_eq!(e("a b+"), "a+b%2B");
        assert_eq!(e("йЫВ9&=/;"), "%D0%B9%D0%AB%D0%929%26%3D%2F%3B");
    }

    #[test]
    fn test_append_url_decode() {
        let d = enc(append_url_decode);
        assert_eq!(d(""), "");
        assert_eq!(d("foo"), "foo");
        assert_eq!(d("a+b%2Bs"), "a b+s");
        assert_eq!(d("%D0%B9%D0%AB%D0%929%26%3D%2F%3B"), "йЫВ9&=/;");
        assert_eq!(d("%qwer%3"), "%qwer%3");
    }

    #[test]
    fn test_append_hex_uint64_encode() {
        let e = |n: u64| {
            let mut b = Vec::new();
            append_hex_uint64_encode(&mut b, n);
            String::from_utf8(b).unwrap()
        };
        assert_eq!(e(0), "0000000000000000");
        assert_eq!(e(123456654), "00000000075BCC8E");
    }

    #[test]
    fn test_append_hex_uint64_decode() {
        let d = enc(append_hex_uint64_decode);
        assert_eq!(d("0"), "0");
        assert_eq!(d("5"), "5");
        assert_eq!(d("ff"), "255");
        assert_eq!(d("0000000000000000"), "0");
        assert_eq!(d("00000000075BCC8E"), "123456654");
    }

    #[test]
    fn test_append_hex_encode() {
        let e = enc(append_hex_encode);
        assert_eq!(e(""), "");
        assert_eq!(e("aA oqDF"), "6141206F714446");
        assert_eq!(e("ЙЦУК"), "D099D0A6D0A3D09A");
    }

    #[test]
    fn test_append_hex_decode() {
        let d = enc(append_hex_decode);
        assert_eq!(d(""), "");
        assert_eq!(d("6141206F714446"), "aA oqDF");
        assert_eq!(d("D099D0A6D0A3D09A"), "ЙЦУК");
        assert_eq!(d("1"), "1");
        assert_eq!(d("1t"), "1t");
        assert_eq!(d("t3"), "t3");
        assert_eq!(d("qwert"), "qwert");
        assert_eq!(d("qwerty"), "qwerty");
    }

    fn pf(
        format_str: &str,
        result_field: &str,
        keep_original_fields: bool,
        skip_empty_results: bool,
    ) -> PipeFormat {
        PipeFormat::new(
            format_str,
            result_field,
            keep_original_fields,
            skip_empty_results,
            None,
        )
        .unwrap()
    }

    // PORT NOTE: the `if (...)` conditional case of Go's `TestPipeFormat`
    // (`format if (!c:*) ...`) is omitted — it needs a lexer-parsed filter, and
    // the `if` processor path is exercised via `pipe_update` instead.
    #[test]
    fn test_pipe_format() {
        // format time, duration, duration_seconds and ipv4
        let p = pf(
            "time=<time:foo>, duration=<duration:bar>, duration_secs=<duration_seconds:d> ip=<ipv4:baz>",
            "x",
            false,
            false,
        );
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[
                        ("foo", "1717328141123456789"),
                        ("bar", "210123456789"),
                        ("baz", "1234567890"),
                        ("d", "1h5m35s"),
                    ],
                    &[
                        ("foo", "abc"),
                        ("bar", "de"),
                        ("baz", "ghkl"),
                        ("d", "foobar"),
                    ],
                ]),
            ),
            &rows(&[
                &[
                    ("foo", "1717328141123456789"),
                    ("bar", "210123456789"),
                    ("baz", "1234567890"),
                    ("d", "1h5m35s"),
                    (
                        "x",
                        "time=2024-06-02T11:35:41.123456789Z, duration=3m30.123456789s, duration_secs=3935 ip=73.150.2.210",
                    ),
                ],
                &[
                    ("foo", "abc"),
                    ("bar", "de"),
                    ("baz", "ghkl"),
                    ("d", "foobar"),
                    ("x", "time=abc, duration=de, duration_secs=foobar ip=ghkl"),
                ],
            ]),
        );

        // format Unix timestamp
        let p = pf("a=<time:foo>, b=<time:bar>", "x", false, false);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[
                        ("foo", "1717328141.123456789"),
                        ("bar", "1717328141.123456"),
                    ],
                    &[("foo", "-1717328141.123"), ("bar", "-1717328141")],
                ]),
            ),
            &rows(&[
                &[
                    ("foo", "1717328141.123456789"),
                    ("bar", "1717328141.123456"),
                    (
                        "x",
                        "a=2024-06-02T11:35:41.123456789Z, b=2024-06-02T11:35:41.123456Z",
                    ),
                ],
                &[
                    ("foo", "-1717328141.123"),
                    ("bar", "-1717328141"),
                    ("x", "a=1915-08-01T12:24:18.877Z, b=1915-08-01T12:24:19Z"),
                ],
            ]),
        );

        // skip_empty_results
        let p = pf("<foo><bar>", "x", false, true);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("foo", "abc"), ("bar", "cde"), ("x", "111")],
                    &[("xfoo", "ppp"), ("xbar", "123"), ("x", "222")],
                ]),
            ),
            &rows(&[
                &[("foo", "abc"), ("bar", "cde"), ("x", "abccde")],
                &[("xfoo", "ppp"), ("xbar", "123"), ("x", "222")],
            ]),
        );

        // no skip_empty_results
        let p = pf("<foo><bar>", "x", false, false);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("foo", "abc"), ("bar", "cde"), ("x", "111")],
                    &[("xfoo", "ppp"), ("xbar", "123"), ("x", "222")],
                ]),
            ),
            &rows(&[
                &[("foo", "abc"), ("bar", "cde"), ("x", "abccde")],
                &[("xfoo", "ppp"), ("xbar", "123"), ("x", "")],
            ]),
        );

        // no keep_original_fields
        let p = pf(r#"{"foo":<q:foo>,"bar":"<bar>"}"#, "x", false, false);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("foo", "abc"), ("bar", "cde"), ("x", "qwe")],
                    &[("foo", "ppp"), ("bar", "123")],
                ]),
            ),
            &rows(&[
                &[
                    ("foo", "abc"),
                    ("bar", "cde"),
                    ("x", r#"{"foo":"abc","bar":"cde"}"#),
                ],
                &[
                    ("foo", "ppp"),
                    ("bar", "123"),
                    ("x", r#"{"foo":"ppp","bar":"123"}"#),
                ],
            ]),
        );

        // keep_original_fields
        let p = pf(r#"{"foo":<q:foo>,"bar":"<bar>"}"#, "x", true, false);
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[
                    &[("foo", "abc"), ("bar", "cde"), ("x", "qwe")],
                    &[("foo", "ppp"), ("bar", "123")],
                ]),
            ),
            &rows(&[
                &[("foo", "abc"), ("bar", "cde"), ("x", "qwe")],
                &[
                    ("foo", "ppp"),
                    ("bar", "123"),
                    ("x", r#"{"foo":"ppp","bar":"123"}"#),
                ],
            ]),
        );

        // plain string with escaped quotes into a single field
        let p = pf(r#"{"foo":<q:foo>,"bar":"<bar>"}"#, "x", false, false);
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("foo", r#""abc""#), ("bar", "cde")]])),
            &rows(&[&[
                ("foo", r#""abc""#),
                ("bar", "cde"),
                ("x", r#"{"foo":"\"abc\"","bar":"cde"}"#),
            ]]),
        );

        // uc/lc/hex/url transforms
        let p = pf(
            "<uc:foo><lc:bar> <hexencode:foo> <hexdecode:baz> <hexnumencode:n> <hexnumdecode:hn> <urlencode:foo> <urldecode:y>",
            "x",
            false,
            false,
        );
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[&[
                    ("foo", "aцC"),
                    ("bar", "aBП"),
                    ("baz", "D099D0A6D0A3D09A"),
                    ("n", "1234"),
                    ("hn", "00000000000004D2"),
                    ("y", "a+b%20d"),
                ]]),
            ),
            &rows(&[&[
                ("foo", "aцC"),
                ("bar", "aBП"),
                ("baz", "D099D0A6D0A3D09A"),
                ("n", "1234"),
                ("hn", "00000000000004D2"),
                ("y", "a+b%20d"),
                (
                    "x",
                    "AЦCabп 61D18643 ЙЦУК 00000000000004D2 1234 a%D1%86C a b d",
                ),
            ]]),
        );

        // base64 transforms, default result field
        let p = pf(
            "<base64encode:foo>;<base64decode:bar>;<base64decode:baz>",
            "_msg",
            false,
            false,
        );
        assert_rows_eq(
            &run_pipe(
                &p,
                &rows(&[&[("foo", "aцC"), ("bar", "YdGGQw=="), ("baz", "al")]]),
            ),
            &rows(&[&[
                ("foo", "aцC"),
                ("bar", "YdGGQw=="),
                ("baz", "al"),
                ("_msg", "YdGGQw==;aцC;al"),
            ]]),
        );

        // plain string into a single field
        let p = pf("foo", "x", false, false);
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "foobar"), ("a", "x")]])),
            &rows(&[&[("_msg", "foobar"), ("a", "x"), ("x", "foo")]]),
        );

        // plain string with html escaping into a single field
        let p = pf("&lt;foo&gt;", "x", false, false);
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "foobar"), ("a", "x")]])),
            &rows(&[&[("_msg", "foobar"), ("a", "x"), ("x", "<foo>")]]),
        );

        // format with empty placeholders into existing field
        let p = pf("<_>foo<_>", "_msg", false, false);
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "foobar"), ("a", "x")]])),
            &rows(&[&[("_msg", "foo"), ("a", "x")]]),
        );

        // format with various placeholders into new field
        let p = pf("a<foo>aa<_msg>xx<a>x", "x", false, false);
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "foobar"), ("a", "b")]])),
            &rows(&[&[("_msg", "foobar"), ("a", "b"), ("x", "aaafoobarxxbx")]]),
        );

        // format into existing field
        let p = pf("a<foo>aa<_msg>xx<a>x", "_msg", false, false);
        assert_rows_eq(
            &run_pipe(&p, &rows(&[&[("_msg", "foobar"), ("a", "b")]])),
            &rows(&[&[("_msg", "aaafoobarxxbx"), ("a", "b")]]),
        );
    }

    // PORT NOTE: `if (...)` cases of Go's `TestPipeFormatUpdateNeededFields`
    // require lexer-parsed filters and are omitted; the non-`if` cases below are
    // ported in full via the `new(..)` constructor.
    #[test]
    fn test_pipe_format_update_needed_fields() {
        use crate::pipe_update::test_utils::assert_needed_fields;

        let f = |fmt: &str, result: &str, keep: bool, skip: bool, allow, deny, ea, ed| {
            let p = pf(fmt, result, keep, skip);
            assert_needed_fields(&p, allow, deny, ea, ed);
        };

        // all the needed fields
        f("foo", "x", false, false, "*", "", "*", "x");
        f("foo", "x", false, true, "*", "", "*", "");
        f("foo", "x", true, false, "*", "", "*", "");
        f("<f1>foo", "x", false, false, "*", "", "*", "x");

        // unneeded fields do not intersect with pattern and output field
        f("foo", "x", false, false, "*", "f1,f2", "*", "f1,f2,x");
        f("<f3>foo", "x", false, false, "*", "f1,f2", "*", "f1,f2,x");

        // unneeded fields intersect with pattern
        f("<f1>foo", "x", false, false, "*", "f1,f2", "*", "f2,x");
        f("<f1>foo", "x", false, true, "*", "f1,f2", "*", "f2");
        f("<f1>foo", "x", true, false, "*", "f1,f2", "*", "f2");

        // unneeded fields intersect with output field
        f("<f1>foo", "x", false, false, "*", "x,y", "*", "x,y");
        f("<f1>foo", "x", false, true, "*", "x,y", "*", "x,y");
        f("<f1>foo", "x", true, false, "*", "x,y", "*", "x,y");

        // needed fields do not intersect with pattern and output field
        f("<f1>foo", "f2", false, false, "x,y", "", "x,y", "");
        f("<f1>foo", "f2", true, false, "x,y", "", "x,y", "");
        f("<f1>foo", "f2", false, true, "x,y", "", "x,y", "");

        // needed fields intersect with pattern field
        f("<f1>foo", "f2", false, false, "f1,y", "", "f1,y", "");
        f("<f1>foo", "f2", false, true, "f1,y", "", "f1,y", "");
        f("<f1>foo", "f2", true, false, "f1,y", "", "f1,y", "");

        // needed fields intersect with output field
        f("<f1>foo", "f2", false, false, "f2,y", "", "f1,y", "");
        f("<f1>foo", "f2", false, true, "f2,y", "", "f1,f2,y", "");
        f("<f1>foo", "f2", true, false, "f2,y", "", "f1,f2,y", "");

        // needed fields intersect with pattern and output fields
        f("<f1>foo", "f2", false, false, "f1,f2,y", "", "f1,y", "");
        f("<f1>foo", "f2", false, true, "f1,f2,y", "", "f1,f2,y", "");
        f("<f1>foo", "f2", true, false, "f1,f2,y", "", "f1,f2,y", "");
    }
}

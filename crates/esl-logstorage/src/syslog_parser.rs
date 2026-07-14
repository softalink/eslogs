//! Port of `lib/logstorage/syslog_parser.go`.

use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use esl_common::fasttime;
use esl_common::tzdata::Location;

use crate::json_parser::{JSONParser, get_json_parser, put_json_parser};
use crate::logfmt_parser::LogfmtParser;
use crate::rows::Field;
use crate::values_encoder::{
    marshal_timestamp_rfc3339_nano_string, marshal_uint64_string, try_parse_uint64_bytes,
};

/// Returns syslog parser from the pool.
///
/// current_year must contain the current year. It is used for properly
/// setting the timestamp field for the rfc3164 format, which doesn't contain
/// a year.
///
/// The timezone is used for the rfc3164 format for setting the desired
/// timezone.
///
/// PORT NOTE: Go takes `*time.Location`. This entry point takes a fixed UTC
/// offset (UTC, `Etc/GMT±N`, `±HH:MM`, `Local`), the cheap common path. For a
/// DST-observing named IANA zone use [`get_syslog_parser_with_location`], which
/// resolves the offset per timestamp like Go's `time.Date`. (`Local` still
/// samples a single offset at startup.)
///
/// Return back the parser to the pool by calling put_syslog_parser when it is
/// no longer needed.
pub fn get_syslog_parser(current_year: i64, timezone_offset_secs: i64) -> SyslogParser {
    let mut p = SYSLOG_PARSER_POOL.lock().unwrap().pop().unwrap_or_default();
    p.current_year = current_year;
    p.timezone_offset_secs = timezone_offset_secs;
    p
}

/// Like [`get_syslog_parser`], but resolves the RFC 3164 timezone via a
/// DST-aware IANA [`Location`] (Go stores `*time.Location`) rather than a fixed
/// offset. The shared `Arc` keeps the per-message cost to a refcount bump plus a
/// binary search over the zone's transitions.
pub fn get_syslog_parser_with_location(current_year: i64, location: Arc<Location>) -> SyslogParser {
    let mut p = SYSLOG_PARSER_POOL.lock().unwrap().pop().unwrap_or_default();
    p.current_year = current_year;
    p.timezone = Some(location);
    p
}

/// Returns back syslog parser to the pool.
///
/// p cannot be used after returning to the pool.
pub fn put_syslog_parser(mut p: SyslogParser) {
    p.reset();
    SYSLOG_PARSER_POOL.lock().unwrap().push(p);
}

static SYSLOG_PARSER_POOL: Mutex<Vec<SyslogParser>> = Mutex::new(Vec::new());

/// SyslogParser is parser for syslog messages.
///
/// It understands the following syslog formats:
///
/// - <https://datatracker.ietf.org/doc/html/rfc5424>
/// - <https://datatracker.ietf.org/doc/html/rfc3164>
///
/// It extracts the following list of syslog message fields into fields -
/// <https://docs.victoriametrics.com/victorialogs/logsql/#unpack_syslog-pipe>
#[derive(Default)]
pub struct SyslogParser {
    /// fields contains parsed fields after parse() call.
    ///
    /// PORT NOTE: Go's Fields point into the input string and p.buf; the Rust
    /// `Field` owns its strings, so the p.buf backing buffer is dropped.
    pub fields: Vec<Field>,

    /// sd_parser is used for structured data parsing in rfc5424.
    /// See <https://datatracker.ietf.org/doc/html/rfc5424#section-6.3>
    sd_parser: LogfmtParser,

    /// json_parser is used for parsing CEE messages.
    ///
    /// See <https://cee.mitre.org/language/1.0-beta1/clt.html#syslog>
    json_parser: Option<JSONParser>,

    /// current_year is used as the current year for rfc3164 messages.
    current_year: i64,

    /// timezone_offset_secs is the fixed UTC offset for rfc3164 messages when
    /// `timezone` is `None` (UTC, `Etc/GMT±N`, `±HH:MM`, `Local`).
    timezone_offset_secs: i64,

    /// A DST-aware IANA zone for rfc3164 messages (named `-syslog.timezone`
    /// values); when set it takes precedence over `timezone_offset_secs` and
    /// the offset is resolved per timestamp.
    timezone: Option<Arc<Location>>,
    // PORT NOTE: Go's unescaper strings.Replacer (`\]` -> `]`) is replaced by
    // str::replace in parse_rfc5424_sd_line.
}

impl SyslogParser {
    fn reset(&mut self) {
        if let Some(jp) = self.json_parser.take() {
            put_json_parser(jp);
        }

        self.current_year = 0;
        self.timezone_offset_secs = 0;
        self.timezone = None;
        self.reset_fields();
    }

    fn reset_fields(&mut self) {
        self.fields.clear();
        // PORT NOTE: Go also resets p.sdParser and p.jsonParser here to
        // release references into input buffers early; the ported parsers
        // hold owned strings and reset themselves at the start of each parse
        // call (their reset methods are private), so nothing else is needed.
    }

    pub fn add_message_field(&mut self, s: &[u8]) {
        let fields_len = self.fields.len();
        if !self.parse_special_message(s) {
            self.fields.truncate(fields_len);
            self.add_field("message", s);
        }
    }

    fn parse_special_message(&mut self, s: &[u8]) -> bool {
        if let Some(cef_str) = s.strip_prefix(b"CEF:".as_slice()) {
            return self.parse_cef_message(cef_str);
        }
        if let Some(cee_str) = s.strip_prefix(b"@cee:".as_slice()) {
            return self.parse_cee_message(cee_str);
        }
        false
    }

    /// Parses CEE message. See <https://cee.mitre.org/language/1.0-beta1/clt.html#syslog>
    fn parse_cee_message(&mut self, s: &[u8]) -> bool {
        let mut jp = match self.json_parser.take() {
            Some(jp) => jp,
            None => get_json_parser(),
        };
        let ok = jp.parse_log_message(s, &[], "").is_ok();
        if ok {
            self.fields.extend_from_slice(jp.fields());
        }
        self.json_parser = Some(jp);
        ok
    }

    /// Adds name=value log field to p.fields.
    pub fn add_field(&mut self, name: &str, value: impl AsRef<[u8]>) {
        self.fields.push(Field {
            name: name.to_string(),
            value: value.as_ref().to_vec(),
        });
    }

    /// Parses syslog message from s into p.fields.
    pub fn parse(&mut self, s: &[u8]) {
        self.reset_fields();

        if s.is_empty() {
            // Cannot parse syslog message
            return;
        }

        if s[0] != b'<' {
            self.parse_no_header(s);
            return;
        }

        // parse priority
        let s = &s[1..];
        let n = match index_byte(s, b'>') {
            Some(n) => n,
            None => {
                // Cannot parse priority
                return;
            }
        };
        let priority_str = &s[..n];
        let s = &s[n + 1..];

        self.add_field("priority", priority_str);
        let priority = match try_parse_uint64_bytes(priority_str) {
            Some(priority) => priority,
            None => {
                // Cannot parse priority
                return;
            }
        };
        let facility = priority / 8;
        let severity = priority % 8;

        let facility_keyword = syslog_facility_to_level(facility);
        self.add_field("facility_keyword", facility_keyword);

        let level = syslog_severity_to_level(severity);
        self.add_field("level", level);

        let mut buf = Vec::new();
        marshal_uint64_string(&mut buf, facility);
        self.add_field("facility", &buf);

        buf.clear();
        marshal_uint64_string(&mut buf, severity);
        self.add_field("severity", &buf);

        self.parse_no_header(s);
    }

    fn parse_no_header(&mut self, s: &[u8]) {
        if s.is_empty() {
            return;
        }
        if let Some(tail) = s.strip_prefix(b"1 ".as_slice()) {
            self.parse_rfc5424(tail);
        } else {
            self.parse_rfc3164(s);
        }
    }

    fn parse_rfc5424(&mut self, s: &[u8]) {
        // See https://datatracker.ietf.org/doc/html/rfc5424

        self.add_field("format", "rfc5424");

        if s.is_empty() {
            return;
        }

        let mut s = s;

        // Parse timestamp
        let n = match index_byte(s, b' ') {
            Some(n) => n,
            None => {
                self.add_field("timestamp", s);
                return;
            }
        };
        self.add_field("timestamp", &s[..n]);
        s = &s[n + 1..];

        // Parse hostname
        let n = match index_byte(s, b' ') {
            Some(n) => n,
            None => {
                self.add_field("hostname", s);
                return;
            }
        };
        self.add_field("hostname", &s[..n]);
        s = &s[n + 1..];

        // Parse app-name
        let n = match index_byte(s, b' ') {
            Some(n) => n,
            None => {
                self.add_field("app_name", s);
                return;
            }
        };
        self.add_field("app_name", &s[..n]);
        s = &s[n + 1..];

        // Parse procid
        let n = match index_byte(s, b' ') {
            Some(n) => n,
            None => {
                self.add_field("proc_id", s);
                return;
            }
        };
        self.add_field("proc_id", &s[..n]);
        s = &s[n + 1..];

        // Parse msgID
        let n = match index_byte(s, b' ') {
            Some(n) => n,
            None => {
                self.add_field("msg_id", s);
                return;
            }
        };
        self.add_field("msg_id", &s[..n]);
        s = &s[n + 1..];

        // Parse structured data
        let (tail, ok) = self.parse_rfc5424_sd(s);
        if !ok {
            return;
        }
        s = tail;

        // Parse message
        self.add_message_field(s);
    }

    fn parse_rfc5424_sd<'a>(&mut self, s: &'a [u8]) -> (&'a [u8], bool) {
        if let Some(tail) = s.strip_prefix(b"- ".as_slice()) {
            return (tail, true);
        }
        if s.starts_with(b"@cee:") {
            return (s, true);
        }

        let mut s = s;
        loop {
            let (tail, ok) = self.parse_rfc5424_sd_line(s);
            if !ok {
                return (tail, false);
            }
            s = tail;
            if let Some(tail) = s.strip_prefix(b" ".as_slice()) {
                return (tail, true);
            }
        }
    }

    fn parse_rfc5424_sd_line<'a>(&mut self, s: &'a [u8]) -> (&'a [u8], bool) {
        if s.is_empty() || s[0] != b'[' {
            return (s, false);
        }
        let s = &s[1..];

        let n = match s.iter().position(|&b| b == b' ' || b == b']') {
            Some(n) => n,
            None => return (s, false),
        };
        let mut sd_id = &s[..n];
        let s = &s[n..];

        if let Some(n) = index_byte(sd_id, b'=') {
            // Special case when sdID contains `key=value`
            // PORT NOTE: the key becomes a field NAME; Go allows raw bytes in
            // names, but names stay String in this port — lossy-decoded here
            // (the VALUE keeps the raw bytes).
            self.add_field(&String::from_utf8_lossy(&sd_id[..n]), &sd_id[n + 1..]);
            sd_id = b"";
        }

        // Parse structured data
        let mut i = 0;
        while i < s.len() && (s[i] != b']' || (i > 0 && s[i - 1] == b'\\')) {
            // skip whitespace
            if s[i] == b' ' {
                i += 1;
                continue;
            }

            // Parse name
            let n = match index_byte(&s[i..], b'=') {
                Some(n) => n,
                None => return (s, false),
            };
            i += n + 1;

            // Parse value
            if s[i] == b'"' {
                let mut valid = false;
                i += 1;
                while i < s.len() {
                    if s[i] == b'"' && s[i - 1] != b'\\' {
                        valid = true;
                        break;
                    }
                    i += 1;
                }
                if !valid {
                    return (s, false);
                }
                i += 1;
            } else {
                let n = match s[i..].iter().position(|&b| b == b' ' || b == b']') {
                    Some(n) => n,
                    None => return (s, false),
                };
                i += n;
            }
        }
        if i == s.len() {
            return (s, false);
        }

        // PORT NOTE: Go unescapes `\]` (allowed in rfc5424, but breaks strings
        // unquoting) with a cached strings.Replacer; str::replace is used here.
        //
        // PORT NOTE: the SD block is handed to LogfmtParser, whose unquoting
        // helpers (shared with pattern/stream_tags/storage_search) operate on
        // &str; the SD block gets a checked &str view with a lossy fallback
        // for the SD block ONLY (SD is rarely non-ASCII). This is the single
        // residual non-byte-exact path in the syslog parse chain.
        let sd_block = String::from_utf8_lossy(&s[..i]);
        let sd_value = sd_block.trim().replace("\\]", "]");
        self.sd_parser.parse(&sd_value);
        if self.sd_parser.fields.is_empty() {
            // Special case when structured data doesn't contain any fields
            if !sd_id.is_empty() {
                self.add_field(&String::from_utf8_lossy(sd_id), "");
            }
        } else {
            let sd_fields = std::mem::take(&mut self.sd_parser.fields);
            // PORT NOTE: sd_id becomes part of field NAMES (String in this
            // port) — lossy-decoded; SD param values flow through sd_parser.
            let sd_id = String::from_utf8_lossy(sd_id);
            for f in &sd_fields {
                if sd_id.is_empty() {
                    self.add_field(&f.name, &f.value);
                    continue;
                }

                let field_name = format!("{sd_id}.{}", f.name);
                self.add_field(&field_name, &f.value);
            }
            self.sd_parser.fields = sd_fields;
        }

        (&s[i + 1..], true)
    }

    fn parse_rfc3164(&mut self, s: &[u8]) {
        // See https://datatracker.ietf.org/doc/html/rfc3164

        self.add_field("format", "rfc3164");

        // Parse timestamp: prefer classic RFC3164
        let mut n = TIME_STAMP_LEN;
        if s.len() < n {
            self.add_message_field(s);
            return;
        }

        let mut s = s;
        if s[10] != b'T' {
            // len("2006-01-02") == 10
            // Parse RFC3164 timestamp.
            if !self.try_parse_timestamp_rfc3164(&s[..n]) {
                self.add_message_field(s);
                return;
            }
        } else {
            // Parse RFC3339 timestamp.
            // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/303
            n = match index_byte(s, b' ') {
                Some(n) => n,
                None => {
                    self.add_message_field(s);
                    return;
                }
            };
            if !self.try_parse_timestamp_rfc3339_nano(&s[..n]) {
                self.add_message_field(s);
                return;
            }
        }
        s = &s[n..];

        if s.is_empty() || s[0] != b' ' {
            // Missing space after the time field
            if !s.is_empty() {
                self.add_message_field(s);
            }
            return;
        }
        s = &s[1..];

        // Parse hostname
        match index_byte(s, b' ') {
            None => {
                // If there is no space, the remainder could be either hostname or tag.
                // Detect common tag patterns (contains ':' or '['). If detected, skip hostname assignment
                // and let the tag parsing below handle it.
                let candidate = s;
                if candidate.iter().any(|&b| b == b':' || b == b'[') {
                    // no hostname; continue without consuming s
                } else {
                    self.add_field("hostname", s);
                    return;
                }
            }
            Some(n) => {
                let candidate = &s[..n];
                if candidate.iter().any(|&b| b == b':' || b == b'[') {
                    // The token after timestamp looks like a tag (e.g. "app[pid]:").
                    // Treat as missing hostname and do not consume it; proceed to tag parsing with s unchanged.
                } else {
                    self.add_field("hostname", candidate);
                    s = &s[n + 1..];
                }
            }
        }

        // Parse tag (aka app_name)
        let n = match s.iter().position(|&b| b == b'[' || b == b':' || b == b' ') {
            Some(n) => n,
            None => {
                self.add_field("app_name", s);
                return;
            }
        };
        let app_name = &s[..n];
        self.add_field("app_name", app_name);
        s = &s[n..];

        // Parse proc_id
        if s.is_empty() {
            return;
        }
        if s[0] == b'[' {
            s = &s[1..];
            let n = match index_byte(s, b']') {
                Some(n) => n,
                None => return,
            };
            self.add_field("proc_id", &s[..n]);
            s = &s[n + 1..];
        }

        // Skip optional ': ' in front of message
        s = s.strip_prefix(b":".as_slice()).unwrap_or(s);
        s = s.strip_prefix(b" ".as_slice()).unwrap_or(s);

        if !s.is_empty() {
            if app_name == b"CEF" {
                let fields_len = self.fields.len();
                if self.parse_cef_message(s) {
                    return;
                }
                self.fields.truncate(fields_len);
            }
            self.add_message_field(s);
        }
    }

    /// Parses CEF message. See <https://www.microfocus.com/documentation/arcsight/arcsight-smartconnectors-8.3/cef-implementation-standard/Content/CEF/Chapter%201%20What%20is%20CEF.htm>
    fn parse_cef_message(&mut self, s: &[u8]) -> bool {
        // PORT NOTE: Go unrolls the seven header fields; the loop below adds
        // them in the same order with identical semantics.
        let mut s = s;
        for name in [
            "cef.version",
            "cef.device_vendor",
            "cef.device_product",
            "cef.device_version",
            "cef.device_event_class_id",
            "cef.name",
            "cef.severity",
        ] {
            let n = match next_unescaped_char(s, b'|') {
                Some(n) => n,
                None => return false,
            };
            self.add_field(name, unescape_cef_value(&s[..n]));
            s = &s[n + 1..];
        }

        // Parse extension
        self.parse_cef_extension(s)
    }

    fn parse_cef_extension(&mut self, s: &[u8]) -> bool {
        if s.is_empty() {
            return true;
        }
        let mut s = s;
        loop {
            // Parse key name
            let n = match next_unescaped_char(s, b'=') {
                Some(n) => n,
                None => return false,
            };
            // PORT NOTE: CEF extension keys become field NAMES; Go allows raw
            // bytes in names, but names stay String in this port —
            // lossy-decoded (extension VALUES keep the raw bytes).
            let key_name = format!(
                "cef.extension.{}",
                String::from_utf8_lossy(&unescape_cef_value(&s[..n]))
            );
            s = &s[n + 1..];

            // Parse key value
            let n = match next_unescaped_char(s, b'=') {
                Some(n) => n,
                None => {
                    self.add_field(&key_name, s);
                    return true;
                }
            };

            let n = match s[..n].iter().rposition(|&b| b == b' ') {
                Some(n) => n,
                None => return false,
            };
            self.add_field(&key_name, unescape_cef_value(&s[..n]));
            s = &s[n + 1..];
        }
    }

    /// UTC offset (seconds) to subtract from a naive-UTC wall-clock instant to
    /// get the real instant: the fixed offset for `None`, or the DST-aware
    /// per-instant offset for a named zone. The `None` branch is the hot path
    /// for UTC/fixed zones and stays a single field read.
    fn wall_offset_secs(&self, naive_secs: i64) -> i64 {
        match &self.timezone {
            None => self.timezone_offset_secs,
            Some(loc) => loc.offset_for_wall_secs(naive_secs) as i64,
        }
    }

    fn try_parse_timestamp_rfc3164(&mut self, s: &[u8]) -> bool {
        let (month, day, hour, minute, second) = match parse_time_stamp(s) {
            Some(parts) => parts,
            None => return false,
        };

        // Go builds time.Date(currentYear, ...) in p.timezone, resolving the UTC
        // offset at that wall-clock date (DST-aware). The port does the same:
        // for a fixed-offset zone (`timezone` is None) it subtracts the fixed
        // offset; for a named IANA zone it resolves the offset for that
        // wall-clock instant via the loaded Location (see `wall_offset_secs`).
        // Out-of-range days overflow into the next month (unix_from_civil),
        // matching Go's time.Date normalization, and the uint64 wraparound below
        // matches Go's `uint64(t.Unix())-24*3600`.
        let naive = unix_from_civil(self.current_year, month, day, hour, minute, second);
        let mut unix = naive - self.wall_offset_secs(naive);
        if (unix as u64).wrapping_sub(24 * 3600) > fasttime::unix_timestamp() {
            // Adjust time to the previous year
            let naive_prev =
                unix_from_civil(self.current_year - 1, month, day, hour, minute, second);
            unix = naive_prev - self.wall_offset_secs(naive_prev);
        }
        let mut buf = Vec::new();
        marshal_timestamp_rfc3339_nano_string(&mut buf, unix * 1_000_000_000);
        self.add_field("timestamp", &buf);
        true
    }

    fn try_parse_timestamp_rfc3339_nano(&mut self, s: &[u8]) -> bool {
        // A valid RFC 3339 timestamp is ASCII: invalid UTF-8 fails the parse
        // exactly like Go's parse fails on non-ASCII bytes in a timestamp.
        let Ok(s) = std::str::from_utf8(s) else {
            return false;
        };
        let nsecs = match crate::values_encoder::try_parse_timestamp_rfc3339_nano(s) {
            Some(nsecs) => nsecs,
            None => return false,
        };

        let mut buf = Vec::new();
        marshal_timestamp_rfc3339_nano_string(&mut buf, nsecs);
        self.add_field("timestamp", &buf);
        true
    }
}

fn syslog_severity_to_level(severity: u64) -> &'static str {
    // See https://en.wikipedia.org/wiki/Syslog#Severity_level
    // and https://grafana.com/docs/grafana/latest/explore/logs-integration/#log-level
    match severity {
        0 => "emerg",
        1 => "alert",
        2 => "critical",
        3 => "error",
        4 => "warning",
        5 => "notice",
        6 => "info",
        7 => "debug",
        _ => "unknown",
    }
}

fn syslog_facility_to_level(facility: u64) -> &'static str {
    // See https://en.wikipedia.org/wiki/Syslog#Facility
    match facility {
        0 => "kern",
        1 => "user",
        2 => "mail",
        3 => "daemon",
        4 => "auth",
        5 => "syslog",
        6 => "lpr",
        7 => "news",
        8 => "uucp",
        9 => "cron",
        10 => "authpriv",
        11 => "ftp",
        12 => "ntp",
        13 => "security",
        14 => "console",
        15 => "solaris-cron",
        16 => "local0",
        17 => "local1",
        18 => "local2",
        19 => "local3",
        20 => "local4",
        21 => "local5",
        22 => "local6",
        23 => "local7",
        _ => "unknown",
    }
}

/// len(Go time.Stamp layout "Jan _2 15:04:05").
const TIME_STAMP_LEN: usize = 15;

/// Parses the Go time.Stamp layout "Jan _2 15:04:05".
///
/// PORT NOTE: replaces Go's time.Parse(time.Stamp, s): month names are matched
/// case-insensitively and the parsed components are range-checked the same way
/// (the day is validated against year 0, which is a leap year, like in Go).
fn parse_time_stamp(b: &[u8]) -> Option<(i64, i64, i64, i64, i64)> {
    if b.len() != TIME_STAMP_LEN {
        return None;
    }

    let month = match_month(&b[..3])?;
    if b[3] != b' ' {
        return None;
    }
    let day = if b[4] == b' ' {
        digit(b[5])?
    } else {
        digit(b[4])? * 10 + digit(b[5])?
    };
    if b[6] != b' ' || b[9] != b':' || b[12] != b':' {
        return None;
    }
    let hour = digit(b[7])? * 10 + digit(b[8])?;
    let minute = digit(b[10])? * 10 + digit(b[11])?;
    let second = digit(b[13])? * 10 + digit(b[14])?;

    if day < 1 || day > days_in_month_of_leap_year(month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some((month, day, hour, minute, second))
}

fn match_month(b: &[u8]) -> Option<i64> {
    const MONTHS: [&[u8; 3]; 12] = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec",
    ];
    for (i, m) in MONTHS.iter().enumerate() {
        if b.eq_ignore_ascii_case(*m) {
            return Some(i as i64 + 1);
        }
    }
    None
}

fn digit(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}

fn days_in_month_of_leap_year(month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => 29,
        _ => 0,
    }
}

fn unix_from_civil(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64) -> i64 {
    days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second
}

/// Returns the number of days since 1970-01-01 for the given civil date.
///
/// Out-of-range days overflow into the next month, matching Go's time.Date
/// normalization (e.g. Feb 29 of a non-leap year becomes Mar 1).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Go's `strings.IndexByte`.
fn index_byte(s: &[u8], c: u8) -> Option<usize> {
    s.iter().position(|&b| b == c)
}

fn next_unescaped_char(b: &[u8], c: u8) -> Option<usize> {
    let mut offset = 0;
    loop {
        let n = b[offset..].iter().position(|&x| x == c)?;
        offset += n;

        if prev_backslashes_count(b, offset).is_multiple_of(2) {
            return Some(offset);
        }
        offset += 1;
    }
}

fn unescape_cef_value(s: &[u8]) -> Cow<'_, [u8]> {
    let mut n = match s.iter().position(|&c| c == b'\\') {
        Some(n) => n,
        None => return Cow::Borrowed(s),
    };

    let mut b: Vec<u8> = Vec::with_capacity(s.len());
    let mut rest = s;
    loop {
        b.extend_from_slice(&rest[..n]);
        n += 1;
        if n >= rest.len() {
            b.push(b'\\');
            break;
        }
        match rest[n] {
            b'n' => b.push(b'\n'),
            b'r' => b.push(b'\r'),
            ch => b.push(ch),
        }
        rest = &rest[n + 1..];

        n = match rest.iter().position(|&c| c == b'\\') {
            Some(n) => n,
            None => {
                b.extend_from_slice(rest);
                break;
            }
        };
    }
    Cow::Owned(b)
}

fn prev_backslashes_count(b: &[u8], offset: usize) -> usize {
    let offset_orig = offset;
    let mut offset = offset;
    while offset > 0 && b[offset - 1] == b'\\' {
        offset -= 1;
    }
    offset_orig - offset
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::marshal_fields_to_logfmt;
    use esl_common::bytesutil;

    #[cfg(unix)]
    #[test]
    fn test_rfc3164_dst_aware_named_timezone() {
        // Skip gracefully where the system lacks the zone.
        let Ok(loc) = Location::load("America/New_York") else {
            return;
        };
        let loc = Arc::new(loc);
        let ts_of = |line: &str| -> Vec<u8> {
            // 2021 is in the past, so the "adjust to previous year" branch does
            // not fire (fasttime::unix_timestamp() is later).
            let mut p = get_syslog_parser_with_location(2021, Arc::clone(&loc));
            p.parse(line.as_bytes());
            let ts = p
                .fields
                .iter()
                .find(|f| f.name == "timestamp")
                .map(|f| f.value.clone())
                .unwrap_or_default();
            put_syslog_parser(p);
            ts
        };
        // Summer -> EDT (UTC-4): 12:00 New York == 16:00 UTC.
        assert!(
            ts_of("Jul 15 12:00:00 host app: msg").starts_with(b"2021-07-15T16:00:00"),
            "EDT offset not applied"
        );
        // Winter -> EST (UTC-5): 12:00 New York == 17:00 UTC.
        assert!(
            ts_of("Jan 15 12:00:00 host app: msg").starts_with(b"2021-01-15T17:00:00"),
            "EST offset not applied"
        );
    }

    #[test]
    fn test_syslog_parser() {
        // PORT NOTE: the Go test passes *time.Location; every case uses
        // time.UTC, which maps to a zero timezone offset here.
        fn f(s: &str, expected: &str) {
            const CURRENT_YEAR: i64 = 2024;
            let mut p = get_syslog_parser(CURRENT_YEAR, 0);

            p.parse(s.as_bytes());
            let mut result = Vec::new();
            marshal_fields_to_logfmt(&mut result, &p.fields);
            assert_eq!(
                bytesutil::to_unsafe_string(&result),
                expected,
                "unexpected result when parsing [{s}]"
            );

            put_syslog_parser(p);
        }

        // RFC 3164
        f(
            "Jun  3 12:08:33 abcd systemd[1]: Starting Update the local ESM caches...",
            r#"format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=systemd proc_id=1 message="Starting Update the local ESM caches...""#,
        );
        f(
            "<165>Jun  3 12:08:33 abcd systemd[1]: Starting Update the local ESM caches...",
            r#"priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=systemd proc_id=1 message="Starting Update the local ESM caches...""#,
        );
        f(
            "Mar 13 12:08:33 abcd systemd: Starting Update the local ESM caches...",
            r#"format=rfc3164 timestamp=2024-03-13T12:08:33Z hostname=abcd app_name=systemd message="Starting Update the local ESM caches...""#,
        );
        f(
            "Jun  3 12:08:33 abcd - Starting Update the local ESM caches...",
            r#"format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=- message="Starting Update the local ESM caches...""#,
        );
        f(
            "Jun  3 12:08:33 - - Starting Update the local ESM caches...",
            r#"format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=- app_name=- message="Starting Update the local ESM caches...""#,
        );

        // RFC 3164: missing hostname, first token is tag (FreeBSD syslogd over UDP)
        f(
            "Jun  3 12:08:33 sshd-session[14308]: Received disconnect from 192.168.0.1 port 22:11: disconnected by user",
            r#"format=rfc3164 timestamp=2024-06-03T12:08:33Z app_name=sshd-session proc_id=14308 message="Received disconnect from 192.168.0.1 port 22:11: disconnected by user""#,
        );
        f(
            "Jun  3 12:08:33 sshd-session: foo",
            "format=rfc3164 timestamp=2024-06-03T12:08:33Z app_name=sshd-session message=foo",
        );

        // RFC 5424
        f(
            r#"<134>1 2024-12-09T18:25:35.401631+00:00 ps999 account-server - - [sd@51059 project="secret" ] 1.2.3.4 - - [09/Dec/2024:18:25:35 +0000] "PUT someurl" 201 - "-" "-" "container-updater 1283500" 0.0010 "-" 1531 0"#,
            r#"priority=134 facility_keyword=local0 level=info facility=16 severity=6 format=rfc5424 timestamp=2024-12-09T18:25:35.401631+00:00 hostname=ps999 app_name=account-server proc_id=- msg_id=- sd@51059.project=secret message="1.2.3.4 - - [09/Dec/2024:18:25:35 +0000] \"PUT someurl\" 201 - \"-\" \"-\" \"container-updater 1283500\" 0.0010 \"-\" 1531 0""#,
        );
        f(
            "<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345 ID47 - This is a test message with structured data.",
            r#"priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47 message="This is a test message with structured data.""#,
        );
        f(
            "1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345 ID47 - This is a test message with structured data.",
            r#"format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47 message="This is a test message with structured data.""#,
        );
        f(
            r#"<165>1 2023-06-03T17:42:00Z mymachine.example.com appname 12345 ID47 [exampleSDID@32473 iut="3" eventSource="Application 123 = ] 56" eventID="11211"] This is a test message with structured data."#,
            r#"priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:00Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47 exampleSDID@32473.iut=3 exampleSDID@32473.eventSource="Application 123 = ] 56" exampleSDID@32473.eventID=11211 message="This is a test message with structured data.""#,
        );
        f(
            r#"<165>1 2023-06-03T17:42:00Z mymachine.example.com appname 12345 ID47 [foo@123 iut="3"][bar@456 eventID="11211"][abc=def][x=y z=a q="]= "] This is a test message with structured data."#,
            r#"priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:00Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47 foo@123.iut=3 bar@456.eventID=11211 abc=def x=y z=a q="]= " message="This is a test message with structured data.""#,
        );
        f(
            r#"<14>1 2025-02-11T12:31:28+01:00 synology Connection - - [synolog@6574 event_id="0x0001" synotype="Connection" username="synouser" luser="synouser" event="User [synouser\] from [192.168.0.10\] logged in successfully via [SSH\]." arg_1="synouser" arg_2="1027" arg_3="192.168.0.10" arg_4="SSH"][meta sequenceId="7"] User [synouser] from [192.168.0.10] logged in successfully via [SSH]."#,
            r#"priority=14 facility_keyword=user level=info facility=1 severity=6 format=rfc5424 timestamp=2025-02-11T12:31:28+01:00 hostname=synology app_name=Connection proc_id=- msg_id=- synolog@6574.event_id=0x0001 synolog@6574.synotype=Connection synolog@6574.username=synouser synolog@6574.luser=synouser synolog@6574.event="User [synouser] from [192.168.0.10] logged in successfully via [SSH]." synolog@6574.arg_1=synouser synolog@6574.arg_2=1027 synolog@6574.arg_3=192.168.0.10 synolog@6574.arg_4=SSH meta.sequenceId=7 message="User [synouser] from [192.168.0.10] logged in successfully via [SSH].""#,
        );
        f(
            r#"<14>1 2025-02-18T11:37:42+02:00 localhost Test - - [test event="quote \"test\""] Test message"#,
            r#"priority=14 facility_keyword=user level=info facility=1 severity=6 format=rfc5424 timestamp=2025-02-18T11:37:42+02:00 hostname=localhost app_name=Test proc_id=- msg_id=- test.event="quote \"test\"" message="Test message""#,
        );

        // Incomplete RFC 3164
        f("", "");
        f(
            "Jun  3 12:08:33",
            "format=rfc3164 timestamp=2024-06-03T12:08:33Z",
        );
        f(
            "Foo  3 12:08:33",
            r#"format=rfc3164 message="Foo  3 12:08:33""#,
        );
        f(
            "Foo  3 12:08:33bar",
            r#"format=rfc3164 message="Foo  3 12:08:33bar""#,
        );
        f(
            "Jun  3 12:08:33 abcd",
            "format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd",
        );
        f(
            "Jun  3 12:08:33 abcd sudo",
            "format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=sudo",
        );
        f(
            "Jun  3 12:08:33 abcd sudo[123]",
            "format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=sudo proc_id=123",
        );
        f(
            "Jun  3 12:08:33 abcd sudo foobar",
            "format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=sudo message=foobar",
        );
        f("foo bar baz", r#"format=rfc3164 message="foo bar baz""#);

        // Incomplete RFC 5424
        f(
            "<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345 ID47 [foo@123]",
            "priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47 foo@123=",
        );
        f(
            "<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345 ID47",
            "priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname proc_id=12345 msg_id=ID47",
        );
        f(
            "<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345",
            "priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname proc_id=12345",
        );
        f(
            "<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname",
            "priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com app_name=appname",
        );
        f(
            "<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com",
            "priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z hostname=mymachine.example.com",
        );
        f(
            "<165>1 2023-06-03T17:42:32.123456789Z",
            "priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2023-06-03T17:42:32.123456789Z",
        );
        f(
            "<165>1 ",
            "priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424",
        );

        // RFC 3164 with RFC3339/ISO8601 timestamp (rsyslog RSYSLOG_ForwardFormat)
        f(
            "2025-01-23T12:15:23.965512+01:00 example rsyslogd: start",
            "format=rfc3164 timestamp=2025-01-23T11:15:23.965512Z hostname=example app_name=rsyslogd message=start",
        );
        f(
            "<46>2025-01-23T12:15:23.965512+01:00 example rsyslogd: start",
            "priority=46 facility_keyword=syslog level=info facility=5 severity=6 format=rfc3164 timestamp=2025-01-23T11:15:23.965512Z hostname=example app_name=rsyslogd message=start",
        );
        f(
            "2025-01-23T11:15:23Z example rsyslogd: start",
            "format=rfc3164 timestamp=2025-01-23T11:15:23Z hostname=example app_name=rsyslogd message=start",
        );
        f(
            "<46>2025-01-23T11:15:23+00:00 example rsyslogd: start",
            "priority=46 facility_keyword=syslog level=info facility=5 severity=6 format=rfc3164 timestamp=2025-01-23T11:15:23Z hostname=example app_name=rsyslogd message=start",
        );
        f(
            "2025-06-15T10:15:23-07:00 example rsyslogd: start",
            "format=rfc3164 timestamp=2025-06-15T17:15:23Z hostname=example app_name=rsyslogd message=start",
        );
        f(
            "<46>2025-03-01T00:05:00+05:30 example rsyslogd: start",
            "priority=46 facility_keyword=syslog level=info facility=5 severity=6 format=rfc3164 timestamp=2025-02-28T18:35:00Z hostname=example app_name=rsyslogd message=start",
        );
        f(
            "2025-08-12T09:00:00+07:00 example rsyslogd: start",
            "format=rfc3164 timestamp=2025-08-12T02:00:00Z hostname=example app_name=rsyslogd message=start",
        );
        f(
            "2025-01-01T00:00:00.123+01:00 example rsyslogd: start",
            "format=rfc3164 timestamp=2024-12-31T23:00:00.123Z hostname=example app_name=rsyslogd message=start",
        );
        f(
            "<46>2025-04-05T22:10:59.500000-04:00 example rsyslogd: start",
            "priority=46 facility_keyword=syslog level=info facility=5 severity=6 format=rfc3164 timestamp=2025-04-06T02:10:59.5Z hostname=example app_name=rsyslogd message=start",
        );
        f(
            "2025-10-10T10:10:10.999000Z example rsyslogd: start",
            "format=rfc3164 timestamp=2025-10-10T10:10:10.999Z hostname=example app_name=rsyslogd message=start",
        );

        // CEF - see https://www.microfocus.com/documentation/arcsight/arcsight-smartconnectors-8.3/cef-implementation-standard/Content/CEF/Chapter%201%20What%20is%20CEF.htm
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232",
            r#"format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF cef.version=1 cef.device_vendor=Security cef.device_product=threatmanager cef.device_version=1.0 cef.device_event_class_id=100 cef.name="worm successfully stopped" cef.severity=10 cef.extension.src=10.0.0.1 cef.extension.dst=2.1.2.2 cef.extension.spt=1232"#,
        );
        f(
            r#"Sep 19 08:26:10 host CEF:0|Security|threatmanager|1.0|100|worm successfully\| \\stopped\n\r\=|10|s\=rc=10.0. \r\n\\\=  0.1  dst=2.1.2.2 spt=1232"#,
            r#"format=rfc3164 timestamp=2024-09-19T08:26:10Z hostname=host app_name=CEF cef.version=0 cef.device_vendor=Security cef.device_product=threatmanager cef.device_version=1.0 cef.device_event_class_id=100 cef.name="worm successfully| \\stopped\n\r=" cef.severity=10 cef.extension.s=rc="10.0. \r\n\\=  0.1 " cef.extension.dst=2.1.2.2 cef.extension.spt=1232"#,
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|10|",
            r#"format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF cef.version=1 cef.device_vendor=Security cef.device_product=threatmanager cef.device_version=1.0 cef.device_event_class_id=100 cef.name="worm successfully stopped" cef.severity=10"#,
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|10|foobar=baz ",
            r#"format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF cef.version=1 cef.device_vendor=Security cef.device_product=threatmanager cef.device_version=1.0 cef.device_event_class_id=100 cef.name="worm successfully stopped" cef.severity=10 cef.extension.foobar="baz ""#,
        );
        f(
            "<6>Sep 14 14:12:51 10.x.x.143 CEF:0|FORCEPOINT|Firewall|6.8.6|70018|Connection_Allowed|0|deviceExternalId=NGFW1 node 1 dvchost=10.x.x.143 dvc=10.x.x.143 src=10.x.x.142 dst=20.x.x.209 spt=59358 dpt=443 proto=6 deviceInboundInterface=0 deviceOutboundInterface=1 act=Allow sourceTranslatedAddress=10.x.x.143 destinationTranslatedAddress=20.x.x.209 sourceTranslatedPort=27237 destinationTranslatedPort=443 deviceFacility=Packet Filtering rt=Sep 14 2021 14:12:51 app=HTTPS cs1Label=RuleID cs1=2100123.2 cs2Label=NatRuleId cs2=2099555.1",
            r#"priority=6 facility_keyword=kern level=info facility=0 severity=6 format=rfc3164 timestamp=2024-09-14T14:12:51Z hostname=10.x.x.143 app_name=CEF cef.version=0 cef.device_vendor=FORCEPOINT cef.device_product=Firewall cef.device_version=6.8.6 cef.device_event_class_id=70018 cef.name=Connection_Allowed cef.severity=0 cef.extension.deviceExternalId="NGFW1 node 1" cef.extension.dvchost=10.x.x.143 cef.extension.dvc=10.x.x.143 cef.extension.src=10.x.x.142 cef.extension.dst=20.x.x.209 cef.extension.spt=59358 cef.extension.dpt=443 cef.extension.proto=6 cef.extension.deviceInboundInterface=0 cef.extension.deviceOutboundInterface=1 cef.extension.act=Allow cef.extension.sourceTranslatedAddress=10.x.x.143 cef.extension.destinationTranslatedAddress=20.x.x.209 cef.extension.sourceTranslatedPort=27237 cef.extension.destinationTranslatedPort=443 cef.extension.deviceFacility="Packet Filtering" cef.extension.rt="Sep 14 2021 14:12:51" cef.extension.app=HTTPS cef.extension.cs1Label=RuleID cef.extension.cs1=2100123.2 cef.extension.cs2Label=NatRuleId cef.extension.cs2=2099555.1"#,
        );
        f(
            "<6>1 2021-09-14T14:06:26-0500 10.x.x.147 - - - - CEF:0|FORCEPOINT|Firewall|6.10.0|76527|Sandbox_Unsupported-File-type|0|deviceExternalId=NGFW3 node 2 dvchost=10.x.x.147 dvc=10.x.x.147 deviceFacility=File Filtering rt=Sep 14 2021 14:06:26",
            r#"priority=6 facility_keyword=kern level=info facility=0 severity=6 format=rfc5424 timestamp=2021-09-14T14:06:26-0500 hostname=10.x.x.147 app_name=- proc_id=- msg_id=- cef.version=0 cef.device_vendor=FORCEPOINT cef.device_product=Firewall cef.device_version=6.10.0 cef.device_event_class_id=76527 cef.name=Sandbox_Unsupported-File-type cef.severity=0 cef.extension.deviceExternalId="NGFW3 node 2" cef.extension.dvchost=10.x.x.147 cef.extension.dvc=10.x.x.147 cef.extension.deviceFacility="File Filtering" cef.extension.rt="Sep 14 2021 14:06:26""#,
        );
        f(
            "<6>CEF:0|FORCEPOINT|Firewall|6.8.5|70019|Connection_Discarded|0|deviceExternalId=NGFW2 node 1 dvchost=10.x.x.149 dvc=10.x.x.149 src=10.x.x.4 dst=10.x.x.255 spt=138 dpt=138 proto=17 deviceInboundInterface=0 act=Discard msg=spoofed packet deviceFacility=Packet Filtering rt=Sep 14 2021 13:58:33 app=NetBIOS Datagram",
            r#"priority=6 facility_keyword=kern level=info facility=0 severity=6 format=rfc3164 cef.version=0 cef.device_vendor=FORCEPOINT cef.device_product=Firewall cef.device_version=6.8.5 cef.device_event_class_id=70019 cef.name=Connection_Discarded cef.severity=0 cef.extension.deviceExternalId="NGFW2 node 1" cef.extension.dvchost=10.x.x.149 cef.extension.dvc=10.x.x.149 cef.extension.src=10.x.x.4 cef.extension.dst=10.x.x.255 cef.extension.spt=138 cef.extension.dpt=138 cef.extension.proto=17 cef.extension.deviceInboundInterface=0 cef.extension.act=Discard cef.extension.msg="spoofed packet" cef.extension.deviceFacility="Packet Filtering" cef.extension.rt="Sep 14 2021 13:58:33" cef.extension.app="NetBIOS Datagram""#,
        );

        // Invalid CEF
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|10|foobar",
            r#"format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message="1|Security|threatmanager|1.0|100|worm successfully stopped|10|foobar""#,
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|10",
            r#"format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message="1|Security|threatmanager|1.0|100|worm successfully stopped|10""#,
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|",
            r#"format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message="1|Security|threatmanager|1.0|100|worm successfully stopped|""#,
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped",
            r#"format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message="1|Security|threatmanager|1.0|100|worm successfully stopped""#,
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager|1.0|100|",
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager|1.0|100",
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager|1.0|",
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager|1.0",
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager|",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager|",
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|threatmanager",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|threatmanager",
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security|",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security|",
        );
        f(
            "Sep 29 08:26:10 host CEF:1|Security",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|Security",
        );
        f(
            "Sep 29 08:26:10 host CEF:1|",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1|",
        );
        f(
            "Sep 29 08:26:10 host CEF:1",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF message=1",
        );
        f(
            "Sep 29 08:26:10 host CEF:",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF",
        );
        f(
            "Sep 29 08:26:10 host CEF",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host app_name=CEF",
        );
        f(
            "Sep 29 08:26:10 host",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z hostname=host",
        );
        f(
            "Sep 29 08:26:10",
            "format=rfc3164 timestamp=2024-09-29T08:26:10Z",
        );

        // @cee - https://cee.mitre.org/language/1.0-beta1/clt.html#syslog
        f(
            r#"Jun  3 12:08:33 abcd systemd[1]: @cee: {"k":"v","message":"test"}"#,
            "format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=systemd proc_id=1 k=v message=test",
        );
        f(
            r#"Jun  3 12:08:33 abcd systemd[1]: @cee: {"k":"v","message":"two words"}"#,
            r#"format=rfc3164 timestamp=2024-06-03T12:08:33Z hostname=abcd app_name=systemd proc_id=1 k=v message="two words""#,
        );
        f(
            r#"<0>Dec 20 12:42:20 syslog-relay process[35]: @cee: {"crit":123,"id":"abc","appname":"application","pname":"auth","pid":123,"host":"system.example.com","pri":10,"time":"2011-12-20T12:38:05.123456-05:00","action":"login","domain":"app","object":"account","service":"web","status":"success"}"#,
            "priority=0 facility_keyword=kern level=emerg facility=0 severity=0 format=rfc3164 timestamp=2024-12-20T12:42:20Z hostname=syslog-relay app_name=process proc_id=35 crit=123 id=abc appname=application pname=auth pid=123 host=system.example.com pri=10 time=2011-12-20T12:38:05.123456-05:00 action=login domain=app object=account service=web status=success",
        );
        f(
            r#"<165>1 2011-12-20T12:38:06Z 10.10.0.1 process - example-event-1 @cee:{"pname":"auth","host":"system.example.com","time":"2011-12-20T12:38:05.123456-05:00"}"#,
            "priority=165 facility_keyword=local4 level=notice facility=20 severity=5 format=rfc5424 timestamp=2011-12-20T12:38:06Z hostname=10.10.0.1 app_name=process proc_id=- msg_id=example-event-1 pname=auth host=system.example.com time=2011-12-20T12:38:05.123456-05:00",
        );
    }

    /// PORT-only test (no Go counterpart): Go strings are arbitrary bytes, so
    /// invalid UTF-8 in syslog message content is preserved verbatim. The
    /// byte-native parse chain must do the same instead of U+FFFD-replacing.
    #[test]
    fn test_syslog_parser_preserves_invalid_utf8_in_message() {
        fn message_of(line: &[u8]) -> Vec<u8> {
            let mut p = get_syslog_parser(2024, 0);
            p.parse(line);
            let msg = p
                .fields
                .iter()
                .find(|f| f.name == "message")
                .map(|f| f.value.clone())
                .expect("missing message field");
            put_syslog_parser(p);
            msg
        }

        // RFC 3164: the raw 0xFF byte must survive into the message value.
        assert_eq!(
            message_of(b"<13>Jan  2 15:04:05 host app: msg \xff raw"),
            b"msg \xff raw".to_vec()
        );

        // RFC 5424 variant.
        assert_eq!(
            message_of(b"<165>1 2023-06-03T17:42:32.123456789Z host app 123 ID47 - msg \xff raw"),
            b"msg \xff raw".to_vec()
        );
    }

    /// PORT-only test (no Go counterpart): TestSyslogParser passes
    /// *time.Location but only ever uses time.UTC. This exercises the
    /// fixed-offset timezone parameter, the previous-year adjustment for
    /// timestamps more than 24h in the future, and Go's time.Date
    /// normalization of out-of-range days.
    #[test]
    fn test_syslog_parser_rfc3164_timezone_offset_and_year_inference() {
        fn f(current_year: i64, timezone_offset_secs: i64, s: &str, expected: &str) {
            let mut p = get_syslog_parser(current_year, timezone_offset_secs);

            p.parse(s.as_bytes());
            let mut result = Vec::new();
            marshal_fields_to_logfmt(&mut result, &p.fields);
            assert_eq!(
                bytesutil::to_unsafe_string(&result),
                expected,
                "unexpected result when parsing [{s}]"
            );

            put_syslog_parser(p);
        }

        // Fixed +01:00 offset: wall-clock 12:08:33 is 11:08:33 UTC
        // (Go: time.Date(2024, June, 3, 12, 8, 33, 0, fixedZone(3600))).
        f(
            2024,
            3600,
            "Jun  3 12:08:33 abcd systemd[1]: foo",
            "format=rfc3164 timestamp=2024-06-03T11:08:33Z hostname=abcd app_name=systemd proc_id=1 message=foo",
        );
        // Fixed -04:30 offset.
        f(
            2024,
            -(4 * 3600 + 1800),
            "Jun  3 12:08:33 abcd systemd[1]: foo",
            "format=rfc3164 timestamp=2024-06-03T16:38:33Z hostname=abcd app_name=systemd proc_id=1 message=foo",
        );
        // A timestamp more than 24h in the future is moved to the previous
        // year (deterministic as long as the test runs before the year 2100).
        f(
            2100,
            0,
            "Jun  3 12:08:33 abcd systemd[1]: foo",
            "format=rfc3164 timestamp=2099-06-03T12:08:33Z hostname=abcd app_name=systemd proc_id=1 message=foo",
        );
        // Feb 29 passes time.Parse (year 0 is a leap year) and then
        // normalizes to Mar 1 in a non-leap currentYear, like Go's time.Date.
        f(
            2023,
            0,
            "Feb 29 12:00:00 abcd systemd[1]: foo",
            "format=rfc3164 timestamp=2023-03-01T12:00:00Z hostname=abcd app_name=systemd proc_id=1 message=foo",
        );
    }
}

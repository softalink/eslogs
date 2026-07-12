//! Port of EsLogs `lib/logstorage/delete_task.go`.

use std::fmt;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Condvar, Mutex};

use esl_common::{fs, panicf};

use crate::tenant_id::TenantID;
use crate::values_encoder::{
    marshal_timestamp_rfc3339_nano_string, try_parse_timestamp_rfc3339_nano,
};

/// DeleteTask describes a task for logs' deletion.
#[derive(Debug, Clone, Default)]
pub struct DeleteTask {
    /// TaskID is the id of the task
    pub task_id: String,

    /// TenantIDs are tenant ids for the task
    pub tenant_ids: Vec<TenantID>,

    /// Filter is the filter used for logs' deletion; Logs matching the given filter are deleted
    pub filter: String,

    /// StartTime is the time when the task has been created.
    ///
    /// PORT NOTE: Go stores a `time.Time`; the port stores unix nanoseconds in
    /// UTC. JSON marshaling always renders the UTC ("Z") form, while Go keeps
    /// the original time zone offset. The production path (`newDeleteTask`)
    /// calls `.UTC()` in Go too, so the on-disk format is identical.
    pub start_time: i64,

    /// cancel is set to non-nil during task execution. It is used for canceling the delete task.
    ///
    /// PORT NOTE: Go stores a `context.CancelFunc` (plus the paired `ctx`,
    /// which the port omits); the Layer-4 task executor observes this flag
    /// instead.
    pub(crate) cancel: Option<Arc<AtomicBool>>,

    /// done_ch is used for waiting until the delete task is complete.
    ///
    /// PORT NOTE: Go uses a `chan struct{}` closed on completion; the port
    /// uses a (Mutex<bool>, Condvar) pair so multiple waiters can wait.
    pub(crate) done_ch: Option<Arc<(Mutex<bool>, Condvar)>>,
}

impl DeleteTask {
    fn marshal_json(&self, dst: &mut Vec<u8>) {
        dst.extend_from_slice(b"{\"task_id\":");
        marshal_json_string(dst, &self.task_id);
        dst.extend_from_slice(b",\"tenant_ids\":[");
        for (i, tid) in self.tenant_ids.iter().enumerate() {
            if i > 0 {
                dst.push(b',');
            }
            dst.extend_from_slice(
                format!(
                    "{{\"account_id\":{},\"project_id\":{}}}",
                    tid.account_id, tid.project_id
                )
                .as_bytes(),
            );
        }
        dst.extend_from_slice(b"],\"filter\":");
        marshal_json_string(dst, &self.filter);
        dst.extend_from_slice(b",\"start_time\":\"");
        marshal_timestamp_rfc3339_nano_string(dst, self.start_time);
        dst.extend_from_slice(b"\"}");
    }
}

/// String returns string representation for the dt.
impl fmt::Display for DeleteTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut data = Vec::new();
        self.marshal_json(&mut data);
        f.write_str(std::str::from_utf8(&data).expect("BUG: DeleteTask JSON must be valid UTF-8"))
    }
}

pub(crate) fn new_delete_task(
    task_id: &str,
    start_time: i64,
    tenant_ids: Vec<TenantID>,
    filter: &str,
) -> DeleteTask {
    DeleteTask {
        task_id: task_id.to_string(),
        tenant_ids,
        filter: filter.to_string(),
        start_time,
        cancel: None,
        done_ch: None,
    }
}

/// MarshalDeleteTasksToJSON marshals tasks into a JSON array and returns the result.
///
/// PORT NOTE: Go uses encoding/json; the port renders the same output by hand
/// (serde is not a dependency), including encoding/json's HTML escaping of
/// `<`, `>` and `&` inside strings. Go marshals a nil slice as `null`; the
/// port always renders `[]` for an empty list, which Go's json.Unmarshal
/// accepts equally.
pub fn marshal_delete_tasks_to_json(tasks: &[DeleteTask]) -> Vec<u8> {
    let mut data = Vec::with_capacity(tasks.len() * 128 + 2);
    data.push(b'[');
    for (i, dt) in tasks.iter().enumerate() {
        if i > 0 {
            data.push(b',');
        }
        dt.marshal_json(&mut data);
    }
    data.push(b']');
    data
}

/// UnmarshalDeleteTasksFromJSON unmarshals DeleteTask slice from JSON array at data.
///
/// PORT NOTE: Go uses encoding/json; the port implements a minimal JSON parser
/// for arrays of DeleteTask objects (arbitrary key order, whitespace, string
/// escapes and `null` are supported).
pub fn unmarshal_delete_tasks_from_json(data: &[u8]) -> Result<Vec<DeleteTask>, String> {
    let mut p = JsonParser { src: data, pos: 0 };
    p.skip_ws();
    if p.consume_literal(b"null") {
        p.skip_ws();
        p.expect_eof()?;
        return Ok(Vec::new());
    }
    p.expect(b'[')?;
    let mut tasks = Vec::new();
    p.skip_ws();
    if p.peek() == Some(b']') {
        p.pos += 1;
    } else {
        loop {
            tasks.push(p.parse_delete_task()?);
            p.skip_ws();
            match p.next() {
                Some(b',') => p.skip_ws(),
                Some(b']') => break,
                _ => {
                    return Err(format!(
                        "unexpected data at position {}: want ',' or ']'",
                        p.pos
                    ));
                }
            }
        }
    }
    p.skip_ws();
    p.expect_eof()?;
    Ok(tasks)
}

pub(crate) fn must_read_delete_tasks_from_file(path: &Path) -> Vec<DeleteTask> {
    if !fs::is_path_exist(path) {
        return Vec::new();
    }
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(err) => {
            panicf!("FATAL: cannot read {}: {}", path.display(), err);
            unreachable!()
        }
    };
    match unmarshal_delete_tasks_from_json(&data) {
        Ok(dts) => dts,
        Err(err) => {
            panicf!(
                "FATAL: cannot parse delete tasks from {}: {}",
                path.display(),
                err
            );
            unreachable!()
        }
    }
}

pub(crate) fn must_write_delete_tasks_to_file(path: &Path, dts: &[DeleteTask]) {
    let data = marshal_delete_tasks_to_json(dts);
    fs::must_write_atomic(path, &data, true);
}

/// Appends the encoding/json representation of s to dst.
///
/// Matches Go's encoding/json string escaping, including the default HTML
/// escaping of `<`, `>` and `&`, and the U+2028 / U+2029 special cases.
fn marshal_json_string(dst: &mut Vec<u8>, s: &str) {
    dst.push(b'"');
    for c in s.chars() {
        match c {
            '"' => dst.extend_from_slice(b"\\\""),
            '\\' => dst.extend_from_slice(b"\\\\"),
            '\n' => dst.extend_from_slice(b"\\n"),
            '\r' => dst.extend_from_slice(b"\\r"),
            '\t' => dst.extend_from_slice(b"\\t"),
            '<' => dst.extend_from_slice(b"\\u003c"),
            '>' => dst.extend_from_slice(b"\\u003e"),
            '&' => dst.extend_from_slice(b"\\u0026"),
            '\u{2028}' => dst.extend_from_slice(b"\\u2028"),
            '\u{2029}' => dst.extend_from_slice(b"\\u2029"),
            c if (c as u32) < 0x20 => {
                dst.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes());
            }
            c => {
                let mut buf = [0u8; 4];
                dst.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    dst.push(b'"');
}

struct JsonParser<'a> {
    src: &'a [u8],
    pos: usize,
}

impl JsonParser<'_> {
    fn skip_ws(&mut self) {
        while let Some(&c) = self.src.get(self.pos) {
            if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let c = self.peek()?;
        self.pos += 1;
        Some(c)
    }

    fn expect(&mut self, c: u8) -> Result<(), String> {
        match self.next() {
            Some(got) if got == c => Ok(()),
            _ => Err(format!(
                "unexpected data at position {}: want {:?}",
                self.pos.saturating_sub(1),
                c as char
            )),
        }
    }

    fn expect_eof(&self) -> Result<(), String> {
        if self.pos == self.src.len() {
            Ok(())
        } else {
            Err(format!("unexpected trailing data at position {}", self.pos))
        }
    }

    fn consume_literal(&mut self, lit: &[u8]) -> bool {
        if self.src[self.pos..].starts_with(lit) {
            self.pos += lit.len();
            true
        } else {
            false
        }
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let c = self
                .next()
                .ok_or_else(|| "unexpected end of JSON string".to_string())?;
            match c {
                b'"' => return Ok(out),
                b'\\' => {
                    let esc = self
                        .next()
                        .ok_or_else(|| "unexpected end of JSON escape".to_string())?;
                    match esc {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let n = self.parse_hex4()?;
                            if (0xD800..0xDC00).contains(&n) {
                                // Surrogate pair.
                                if !self.consume_literal(b"\\u") {
                                    return Err("missing low surrogate in JSON string".to_string());
                                }
                                let n2 = self.parse_hex4()?;
                                if !(0xDC00..0xE000).contains(&n2) {
                                    return Err("invalid low surrogate in JSON string".to_string());
                                }
                                let cp = 0x10000 + ((n - 0xD800) << 10) + (n2 - 0xDC00);
                                out.push(
                                    char::from_u32(cp)
                                        .ok_or_else(|| "invalid surrogate pair".to_string())?,
                                );
                            } else {
                                out.push(
                                    char::from_u32(n)
                                        .ok_or_else(|| "invalid \\u escape".to_string())?,
                                );
                            }
                        }
                        _ => return Err(format!("unsupported JSON escape {:?}", esc as char)),
                    }
                }
                _ => {
                    // Collect the full UTF-8 sequence starting at c.
                    let start = self.pos - 1;
                    let len = utf8_len(c);
                    let end = start + len;
                    if end > self.src.len() {
                        return Err("invalid UTF-8 in JSON string".to_string());
                    }
                    let chunk = std::str::from_utf8(&self.src[start..end])
                        .map_err(|_| "invalid UTF-8 in JSON string".to_string())?;
                    out.push_str(chunk);
                    self.pos = end;
                }
            }
        }
    }

    fn parse_hex4(&mut self) -> Result<u32, String> {
        if self.pos + 4 > self.src.len() {
            return Err("unexpected end of \\u escape".to_string());
        }
        let s = std::str::from_utf8(&self.src[self.pos..self.pos + 4])
            .map_err(|_| "invalid \\u escape".to_string())?;
        let n = u32::from_str_radix(s, 16).map_err(|_| "invalid \\u escape".to_string())?;
        self.pos += 4;
        Ok(n)
    }

    fn parse_u32(&mut self) -> Result<u32, String> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.pos += 1;
            } else {
                break;
            }
        }
        if start == self.pos {
            return Err(format!("cannot parse number at position {start}"));
        }
        std::str::from_utf8(&self.src[start..self.pos])
            .unwrap()
            .parse::<u32>()
            .map_err(|err| format!("cannot parse number at position {start}: {err}"))
    }

    fn parse_tenant_ids(&mut self) -> Result<Vec<TenantID>, String> {
        self.skip_ws();
        if self.consume_literal(b"null") {
            return Ok(Vec::new());
        }
        self.expect(b'[')?;
        let mut tenant_ids = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(tenant_ids);
        }
        loop {
            self.skip_ws();
            self.expect(b'{')?;
            let mut tid = TenantID::default();
            self.skip_ws();
            if self.peek() == Some(b'}') {
                self.pos += 1;
            } else {
                loop {
                    self.skip_ws();
                    let key = self.parse_string()?;
                    self.skip_ws();
                    self.expect(b':')?;
                    self.skip_ws();
                    match key.as_str() {
                        "account_id" => tid.account_id = self.parse_u32()?,
                        "project_id" => tid.project_id = self.parse_u32()?,
                        _ => return Err(format!("unexpected key {key:?} in tenantID object")),
                    }
                    self.skip_ws();
                    match self.next() {
                        Some(b',') => {}
                        Some(b'}') => break,
                        _ => {
                            return Err(format!(
                                "unexpected data at position {}: want ',' or '}}'",
                                self.pos
                            ));
                        }
                    }
                }
            }
            tenant_ids.push(tid);
            self.skip_ws();
            match self.next() {
                Some(b',') => {}
                Some(b']') => return Ok(tenant_ids),
                _ => {
                    return Err(format!(
                        "unexpected data at position {}: want ',' or ']'",
                        self.pos
                    ));
                }
            }
        }
    }

    fn parse_delete_task(&mut self) -> Result<DeleteTask, String> {
        self.skip_ws();
        self.expect(b'{')?;
        let mut dt = DeleteTask::default();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(dt);
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect(b':')?;
            self.skip_ws();
            match key.as_str() {
                "task_id" => dt.task_id = self.parse_string()?,
                "tenant_ids" => dt.tenant_ids = self.parse_tenant_ids()?,
                "filter" => dt.filter = self.parse_string()?,
                "start_time" => {
                    let s = self.parse_string()?;
                    dt.start_time = try_parse_timestamp_rfc3339_nano(&s).ok_or_else(|| {
                        format!("cannot parse start_time {s:?} as RFC3339 timestamp")
                    })?;
                }
                _ => return Err(format!("unexpected key {key:?} in DeleteTask object")),
            }
            self.skip_ws();
            match self.next() {
                Some(b',') => {}
                Some(b'}') => return Ok(dt),
                _ => {
                    return Err(format!(
                        "unexpected data at position {}: want ',' or '}}'",
                        self.pos
                    ));
                }
            }
        }
    }
}

fn utf8_len(first_byte: u8) -> usize {
    match first_byte {
        b if b < 0x80 => 1,
        b if b & 0xE0 == 0xC0 => 2,
        b if b & 0xF0 == 0xE0 => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_task_marshal_unmarshal_as_json() {
        // PORT NOTE: the Go test uses time.Now(); the port uses fixed
        // timestamps with nanosecond precision, which exercises the same
        // marshal -> unmarshal -> marshal round-trip invariant.
        let dts = vec![
            DeleteTask {
                task_id: "task1".to_string(),
                tenant_ids: vec![
                    TenantID {
                        account_id: 0,
                        project_id: 0,
                    },
                    TenantID {
                        account_id: 12,
                        project_id: 456,
                    },
                ],
                filter: "app:=foo".to_string(),
                start_time: 1_234_567_890_123_456_789,
                ..Default::default()
            },
            DeleteTask {
                task_id: "task_2".to_string(),
                tenant_ids: vec![TenantID {
                    account_id: 0,
                    project_id: 0,
                }],
                filter: "app:=x".to_string(),
                start_time: 1_700_000_000_000_000_000,
                ..Default::default()
            },
        ];

        let data = marshal_delete_tasks_to_json(&dts);

        let dts_unmarshaled = match unmarshal_delete_tasks_from_json(&data) {
            Ok(dts) => dts,
            Err(err) => panic!("unexpected error: {err}"),
        };
        let data2 = marshal_delete_tasks_to_json(&dts_unmarshaled);
        assert_eq!(
            data2,
            data,
            "unexpected delete_task unmarshaled\ngot {}\nwant {}",
            String::from_utf8_lossy(&data2),
            String::from_utf8_lossy(&data)
        );
    }

    #[test]
    fn test_delete_task_json_format() {
        // Guards the exact on-disk format produced by Go's encoding/json,
        // asserted in TestStorageDeleteTaskOps upstream.
        let dt = new_delete_task(
            "task_id_1",
            1_234_567_890_123_456_789,
            vec![TenantID {
                account_id: 123,
                project_id: 456,
            }],
            "app:=foo SECRET",
        );
        let data = marshal_delete_tasks_to_json(&[dt]);
        let expected = r#"[{"task_id":"task_id_1","tenant_ids":[{"account_id":123,"project_id":456}],"filter":"app:=foo SECRET","start_time":"2009-02-13T23:31:30.123456789Z"}]"#;
        assert_eq!(String::from_utf8(data).unwrap(), expected);
    }
}

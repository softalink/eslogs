//! Port of EsLogs `lib/logstorage/pipe_unpack_syslog.go`.
//!
//! `| unpack_syslog ...` parses RFC 5424 / RFC 3164 syslog messages from a
//! source field into separate log fields. It reuses the shared unpack
//! scaffolding in [`crate::pipe_unpack`] and [`crate::syslog_parser`].

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::pipe::{Pipe, PipeProcessor};
use crate::pipe_unpack::{
    IfFilter, UnpackFunc, new_pipe_unpack_processor, update_needed_fields_for_unpack_pipe,
};
use crate::prefix_filter;
use crate::stream_filter::quote_token_if_needed;
use crate::syslog_parser::{get_syslog_parser, put_syslog_parser};

/// `| unpack_syslog ...` pipe (Go `pipeUnpackSyslog`).
pub(crate) struct PipeUnpackSyslog {
    from_field: Vec<u8>,

    /// Original `offset` token (for `to_string`).
    offset_str: String,
    /// Fixed UTC offset in seconds used for rfc3164 timestamps.
    ///
    /// PORT NOTE: Go stores `*time.Location`; the ported `SyslogParser` takes a
    /// fixed offset in seconds (std has no timezone database). The default is
    /// `0` (UTC) where Go uses `time.Local`.
    offset_secs: i64,

    result_prefix: String,
    keep_original_fields: bool,
    iff: Option<IfFilter>,
}

/// Constructs a `PipeUnpackSyslog` (Go `parsePipeUnpackSyslog`; lexer parsing —
/// including `offset` duration parsing — is deferred).
pub(crate) fn new_pipe_unpack_syslog(
    from_field: impl Into<Vec<u8>>,
    offset_str: impl Into<String>,
    offset_secs: i64,
    result_prefix: impl Into<String>,
    keep_original_fields: bool,
    iff: Option<IfFilter>,
) -> PipeUnpackSyslog {
    PipeUnpackSyslog {
        from_field: from_field.into(),
        offset_str: offset_str.into(),
        offset_secs,
        result_prefix: result_prefix.into(),
        keep_original_fields,
        iff,
    }
}

impl Pipe for PipeUnpackSyslog {
    /// Port of Go `pipeUnpackSyslog.splitToRemoteAndLocal`: the pipe runs fully
    /// remote, unchanged.
    fn split_to_remote_and_local(&self, timestamp: i64) -> crate::pipe::SplitPipesResult {
        (Some(crate::pipe::clone_pipe(self, timestamp)), Vec::new())
    }

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
            self.iff = Some(iff_new);
        }
        Ok(())
    }

    /// Go `visitSubqueries` for this pipe: propagates into the `if (...)` filter.
    fn visit_subqueries_mut(
        &mut self,
        timestamp: i64,
        visit: &mut dyn FnMut(&mut crate::parser::Query),
    ) {
        if let Some(iff) = &self.iff
            && let Some(iff_new) = iff.visit_subqueries_mut(timestamp, visit)
        {
            self.iff = Some(iff_new);
        }
    }

    fn to_string(&self) -> String {
        let mut s = String::from("unpack_syslog");
        if let Some(iff) = &self.iff {
            s += &format!(" {iff}");
        }
        if !crate::filter_generic::is_msg_field_name(&self.from_field) {
            s += &format!(
                " from {}",
                crate::parser::quote_token_bytes_if_needed(&self.from_field)
            );
        }
        if !self.offset_str.is_empty() {
            s += &format!(" offset {}", self.offset_str);
        }
        if !self.result_prefix.is_empty() {
            s += &format!(
                " result_prefix {}",
                quote_token_if_needed(&self.result_prefix)
            );
        }
        if self.keep_original_fields {
            s += " keep_original_fields";
        }
        s
    }

    fn update_needed_fields(&self, pf: &mut prefix_filter::Filter) {
        update_needed_fields_for_unpack_pipe(
            &self.from_field,
            &self.result_prefix,
            &[],
            self.keep_original_fields,
            false,
            self.iff.as_ref(),
            pf,
        );
    }

    fn can_live_tail(&self) -> bool {
        true
    }

    fn can_return_last_n_results(&self) -> bool {
        true
    }

    fn new_pipe_processor(
        &self,
        _concurrency: usize,
        _stop: Arc<AtomicBool>,
        pp_next: Arc<dyn PipeProcessor>,
    ) -> Arc<dyn PipeProcessor> {
        let offset_secs = self.offset_secs;
        let unpack_syslog: UnpackFunc = Box::new(move |uctx, s| {
            let year = current_year();
            let mut p = get_syslog_parser(year, offset_secs);
            let mut s = s;
            while let Some(&b) = s.first() {
                if !matches!(b, b' ' | b'\t' | b'\n' | b'\r') {
                    break;
                }
                s = &s[1..];
            }
            p.parse(s);
            for f in &p.fields {
                uctx.add_field(&f.name, &f.value);
            }
            put_syslog_parser(p);
        });

        new_pipe_unpack_processor(
            unpack_syslog,
            pp_next,
            self.from_field.clone(),
            self.result_prefix.clone(),
            self.keep_original_fields,
            false,
            self.iff.clone(),
        )
    }
}

/// Returns the current UTC year.
///
/// PORT NOTE: Go keeps `currentYear` refreshed by a background goroutine; the
/// port computes it on demand from the system clock (Howard Hinnant's
/// `civil_from_days`).
fn current_year() -> i64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 { y + 1 } else { y }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::pipe_unpack::test_utils::{rows, run_pipe};

    fn run(pipe: PipeUnpackSyslog, input: &[&[(&str, &str)]], expected: &[&[(&str, &str)]]) {
        run_pipe(Arc::new(pipe), &rows(input), &rows(expected));
    }

    const MSG: &str = "<165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345 ID47 - This is a test message with structured data";

    fn rfc5424_out() -> Vec<(&'static str, &'static str)> {
        vec![
            ("level", "notice"),
            ("priority", "165"),
            ("facility", "20"),
            ("facility_keyword", "local4"),
            ("severity", "5"),
            ("format", "rfc5424"),
            ("timestamp", "2023-06-03T17:42:32.123456789Z"),
            ("hostname", "mymachine.example.com"),
            ("app_name", "appname"),
            ("proc_id", "12345"),
            ("msg_id", "ID47"),
            ("message", "This is a test message with structured data"),
        ]
    }

    // PORT NOTE: the `if (...)` runtime cases from Go's TestPipeUnpackSyslog are
    // deferred — they need the lexer/filter parser to build the `if` filter.

    #[test]
    fn test_pipe_unpack_syslog_no_skip_empty_results() {
        let mut expected = vec![("_msg", MSG)];
        expected.push(("foo", "321"));
        expected.extend(rfc5424_out());
        run(
            new_pipe_unpack_syslog("_msg", "", 0, "", false, None),
            &[&[("_msg", MSG), ("foo", "321")]],
            &[&expected],
        );
    }

    #[test]
    fn test_pipe_unpack_syslog_surrounding_whitespace() {
        let msg = "\t\n\r  <165>1 2023-06-03T17:42:32.123456789Z mymachine.example.com appname 12345 ID47 - This is a test message with structured data   ";
        let mut expected: Vec<(&str, &str)> = vec![("_msg", msg)];
        expected.extend(rfc5424_out());
        // message includes trailing spaces from the source line.
        for f in expected.iter_mut() {
            if f.0 == "message" {
                f.1 = "This is a test message with structured data   ";
            }
        }
        run(
            new_pipe_unpack_syslog("_msg", "", 0, "", false, None),
            &[&[("_msg", msg)]],
            &[&expected],
        );
    }

    #[test]
    fn test_pipe_unpack_syslog_keep_original_fields() {
        let expected: Vec<(&str, &str)> = vec![
            ("_msg", MSG),
            ("level", "notice"),
            ("foo", "321"),
            ("priority", "165"),
            ("facility", "20"),
            ("facility_keyword", "local4"),
            ("severity", "5"),
            ("format", "rfc5424"),
            ("timestamp", "2023-06-03T17:42:32.123456789Z"),
            ("hostname", "mymachine.example.com"),
            ("app_name", "foobar"),
            ("proc_id", "12345"),
            ("msg_id", "baz"),
            ("message", "This is a test message with structured data"),
        ];
        run(
            new_pipe_unpack_syslog("_msg", "", 0, "", true, None),
            &[&[
                ("_msg", MSG),
                ("foo", "321"),
                ("app_name", "foobar"),
                ("msg_id", "baz"),
            ]],
            &[&expected],
        );
    }

    #[test]
    fn test_pipe_unpack_syslog_from_other_field() {
        let mut expected = vec![("x", MSG)];
        expected.extend(rfc5424_out());
        run(
            new_pipe_unpack_syslog("x", "", 0, "", false, None),
            &[&[("x", MSG)]],
            &[&expected],
        );
    }

    #[test]
    fn test_pipe_unpack_syslog_offset_ignored_for_rfc5424() {
        let mut expected = vec![("x", MSG)];
        expected.extend(rfc5424_out());
        run(
            new_pipe_unpack_syslog("x", "2h30m", 9000, "", false, None),
            &[&[("x", MSG)]],
            &[&expected],
        );
    }

    #[test]
    fn test_pipe_unpack_syslog_from_missing_field() {
        run(
            new_pipe_unpack_syslog("x", "", 0, "", false, None),
            &[&[("_msg", "foo=bar")]],
            &[&[("_msg", "foo=bar")]],
        );
    }

    #[test]
    fn test_pipe_unpack_syslog_from_non_syslog_field() {
        run(
            new_pipe_unpack_syslog("x", "", 0, "", false, None),
            &[&[("x", "foobar")]],
            &[&[
                ("x", "foobar"),
                ("format", "rfc3164"),
                ("message", "foobar"),
            ]],
        );
    }

    #[test]
    fn test_pipe_unpack_syslog_multiple_rows_result_prefix() {
        let msg2 = "<163>1 2024-12-13T18:21:43Z mymachine.example.com appname2 345 ID7 - foobar";
        run(
            new_pipe_unpack_syslog("x", "", 0, "qwe_", false, None),
            &[&[("x", MSG)], &[("x", msg2), ("y", "z=bar")]],
            &[
                &[
                    ("x", MSG),
                    ("qwe_level", "notice"),
                    ("qwe_priority", "165"),
                    ("qwe_facility", "20"),
                    ("qwe_facility_keyword", "local4"),
                    ("qwe_severity", "5"),
                    ("qwe_format", "rfc5424"),
                    ("qwe_timestamp", "2023-06-03T17:42:32.123456789Z"),
                    ("qwe_hostname", "mymachine.example.com"),
                    ("qwe_app_name", "appname"),
                    ("qwe_proc_id", "12345"),
                    ("qwe_msg_id", "ID47"),
                    ("qwe_message", "This is a test message with structured data"),
                ],
                &[
                    ("x", msg2),
                    ("y", "z=bar"),
                    ("qwe_level", "error"),
                    ("qwe_priority", "163"),
                    ("qwe_facility", "20"),
                    ("qwe_facility_keyword", "local4"),
                    ("qwe_severity", "3"),
                    ("qwe_format", "rfc5424"),
                    ("qwe_timestamp", "2024-12-13T18:21:43Z"),
                    ("qwe_hostname", "mymachine.example.com"),
                    ("qwe_app_name", "appname2"),
                    ("qwe_proc_id", "345"),
                    ("qwe_msg_id", "ID7"),
                    ("qwe_message", "foobar"),
                ],
            ],
        );
    }
}

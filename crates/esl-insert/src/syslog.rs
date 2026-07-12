//! Port of the message-parsing path of EsLogs
//! `app/eslinsert/syslog/syslog.go` (`processLine`).
//!
//! PORT NOTE: syslog is not wired into the HTTP router in the Go source either
//! — it is served by dedicated TCP/UDP/Unix listeners started from
//! `syslog.MustInit()`. Those listeners and the octet-counting/octet-stuffing
//! `syslogLineReader` framing are ported in
//! [`crate::syslog_listeners`]; this module ports `processLine`, which turns a
//! single syslog line into a row via
//! [`crate::common_params::LogMessageProcessor`]. The listeners call
//! [`process_line`] per framed message.

use esl_logstorage::rows::{Field, rename_field};
use esl_logstorage::syslog_parser::{get_syslog_parser, put_syslog_parser};

use crate::common_params::{
    LogMessageProcessor, LogRowsStorage, extract_timestamp_from_fields, now_unix_nanos,
};

const TIME_FIELDS: &[&str] = &["timestamp"];
const MSG_FIELDS: &[&str] = &["message"];

/// The sink [`process_line`] adds parsed rows to.
///
/// PORT NOTE: Go passes the `insertutil.LogMessageProcessor` *interface* here;
/// the Rust port of `common_params` collapsed that interface into the concrete
/// storage-bound [`LogMessageProcessor`] struct. This trait restores the seam
/// so the syslog listeners' stream functions can be exercised with a
/// `TestLogMessageProcessor` exactly like the Go tests.
pub trait SyslogLogMessageProcessor {
    /// Adds a new log message with the given timestamp and fields.
    fn add_row(&mut self, timestamp: i64, fields: &mut [Field], stream_fields_len: isize);
}

impl<S: LogRowsStorage> SyslogLogMessageProcessor for LogMessageProcessor<'_, S> {
    fn add_row(&mut self, timestamp: i64, fields: &mut [Field], stream_fields_len: isize) {
        LogMessageProcessor::add_row(self, timestamp, fields, stream_fields_len);
    }
}

/// Parses a single syslog `line` and adds the resulting row to `lmp`.
///
/// `current_year` is used for the RFC 3164 format (which omits the year).
/// `timezone_offset_secs` is the fixed UTC offset for RFC 3164 timestamps
/// (see the PORT NOTE on `syslog_parser::get_syslog_parser`).
///
/// When `use_local_timestamp` is true, the current time is used instead of the
/// timestamp parsed from the message. When `remote_ip` is non-empty it is added
/// as `remote_ip`, and as `hostname` when the message lacks one.
pub fn process_line<P: SyslogLogMessageProcessor + ?Sized>(
    line: &str,
    current_year: i64,
    timezone_offset_secs: i64,
    use_local_timestamp: bool,
    remote_ip: &str,
    lmp: &mut P,
) -> Result<(), String> {
    let mut p = get_syslog_parser(current_year, timezone_offset_secs);
    p.parse(line);

    let ts = if use_local_timestamp {
        now_unix_nanos()
    } else {
        match extract_timestamp_from_fields(TIME_FIELDS, &mut p.fields) {
            Ok(nsecs) => nsecs,
            Err(err) => {
                put_syslog_parser(p);
                return Err(format!(
                    "cannot get timestamp from syslog line {line:?}: {err}"
                ));
            }
        }
    };

    if !remote_ip.is_empty() {
        p.add_field("remote_ip", remote_ip);
        // Fallback: set hostname from remote_ip if RFC3164 message omitted it.
        let has_hostname = p
            .fields
            .iter()
            .any(|f| f.name == "hostname" && !f.value.is_empty());
        if !has_hostname {
            p.add_field("hostname", remote_ip);
        }
    }

    rename_field(&mut p.fields, MSG_FIELDS, "_msg");
    lmp.add_row(ts, &mut p.fields, -1);

    put_syslog_parser(p);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use esl_logstorage::tenant_id::TenantID;

    use crate::common_params::get_common_params_for_syslog;
    use crate::testutil::{open_temp_storage, rows_count};

    #[test]
    fn test_process_line_lands_row() {
        let s = open_temp_storage("syslog");
        let cp = get_common_params_for_syslog(TenantID::default(), None, vec![], vec![], vec![]);
        let mut lmp = cp.new_log_message_processor(&s);

        // RFC5424 line; use_local_timestamp=true forces the current time.
        let line = "<165>1 2023-01-01T00:00:00.000Z myhost myapp 1 - - hello world";
        let res = process_line(line, 2024, 0, true, "", &mut lmp);
        assert!(res.is_ok(), "unexpected error: {res:?}");

        lmp.close();
        assert_eq!(rows_count(&s), 1, "expected 1 row ingested");
        s.must_close();
    }
}

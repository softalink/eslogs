//! Port of EsLogs `app/eslinsert/elasticsearch/elasticsearch.go` and the
//! generated `bulk_response.qtpl.go` writer.
//!
//! Handles the Elasticsearch `_bulk` ingestion protocol (alternating
//! action/document ndjson) plus the fake control-plane responses various
//! clients probe before ingesting.

use std::io::Read;
use std::sync::Arc;
use std::time::Instant;

use esl_common::httpserver::{Request, ResponseWriter, get_quoted_remote_addr};

use esl_logstorage::json_parser::{get_json_parser, put_json_parser};
use esl_logstorage::rows::{Field, rename_field};
use esl_logstorage::values_encoder::try_parse_timestamp_rfc3339_nano;

use esl_common::timeutil::try_parse_unix_timestamp;

use crate::common_params::{
    LogMessageProcessor, LogRowsStorage, get_common_params, now_unix_nanos,
};
use crate::line_reader::LineReader;

/// Elasticsearch version reported to clients (Go `-elasticsearch.version`).
const ELASTICSEARCH_VERSION: &str = "8.9.0";

/// RequestHandler processes Elasticsearch insert requests. Returns true if the
/// path was handled.
pub fn request_handler<S: LogRowsStorage>(
    storage: &Arc<S>,
    path: &str,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    w.set_header("Content-Type", "application/json");
    // This header is needed for Logstash.
    w.set_header("X-Elastic-Product", "Elasticsearch");

    if path.starts_with("/insert/elasticsearch/_ilm/policy")
        || path.starts_with("/insert/elasticsearch/_index_template")
        || path.starts_with("/insert/elasticsearch/_ingest")
        || path.starts_with("/insert/elasticsearch/_nodes")
        || path.starts_with("/insert/elasticsearch/_rollup")
        || path.starts_with("/insert/elasticsearch/logstash")
        || path.starts_with("/insert/elasticsearch/_logstash")
    {
        // Fake responses for Elasticsearch control-plane requests.
        w.write_str("{}");
        return true;
    }

    match path {
        // some clients may omit the trailing slash.
        "/insert/elasticsearch/" | "/insert/elasticsearch" => {
            if req.method() == "GET" {
                // Fake response for the Elasticsearch ping request.
                w.write_str(&format!(
                    "{{\n\t\t\t\"version\": {{\n\t\t\t\t\"number\": {ELASTICSEARCH_VERSION:?}\n\t\t\t}}\n\t\t}}"
                ));
            }
            // HEAD: empty response for the Logstash ping request.
            true
        }
        "/insert/elasticsearch/_license" => {
            w.write_str(
                "{\n\t\t\t\"license\": {\n\t\t\t\t\"uid\": \"cbff45e7-c553-41f7-ae4f-9205eabd80xx\",\n\t\t\t\t\"type\": \"oss\",\n\t\t\t\t\"status\": \"active\",\n\t\t\t\t\"expiry_date_in_millis\" : 4000000000000\n\t\t\t}\n\t\t}",
            );
            true
        }
        "/insert/elasticsearch/_bulk" => {
            let start_time = Instant::now();

            let cp = match get_common_params(req) {
                Ok(cp) => cp,
                Err(err) => {
                    w.errorf(req, &err);
                    return true;
                }
            };
            let stream_name = format!(
                "remoteAddr={}, requestURI={:?}",
                get_quoted_remote_addr(req),
                req.request_uri()
            );
            let time_fields: Vec<&str> = cp.time_fields.iter().map(String::as_str).collect();
            let msg_fields: Vec<&str> = cp.msg_fields.iter().map(String::as_str).collect();
            let preserve_keys: Vec<&str> =
                cp.preserve_json_keys.iter().map(String::as_str).collect();

            let mut lmp = cp.new_log_message_processor(storage);
            let (n, res) = read_bulk_request(
                &stream_name,
                req.body_reader(),
                &time_fields,
                &msg_fields,
                &preserve_keys,
                &mut lmp,
            );
            lmp.close();

            if let Err(err) = res {
                w.errorf(
                    req,
                    &format!(
                        "cannot decode log message #{n} in /_bulk request: {err}, stream fields: {:?}",
                        cp.stream_fields
                    ),
                );
                return true;
            }

            let took_ms = start_time.elapsed().as_millis() as i64;
            write_bulk_response(w, n, took_ms);
            true
        }
        _ => false,
    }
}

/// Mirrors the generated `WriteBulkResponse`:
/// `{"took":N,"errors":false,"items":[{"create":{"status":201}},...]}`
fn write_bulk_response(w: &mut ResponseWriter, n: usize, took_ms: i64) {
    w.write_str(&bulk_response_string(n, took_ms));
}

fn bulk_response_string(n: usize, took_ms: i64) -> String {
    let mut s = format!("{{\"took\":{took_ms},\"errors\":false,\"items\":[");
    for i in 0..n {
        s.push_str("{\"create\":{\"status\":201}}");
        if i + 1 < n {
            s.push(',');
        }
    }
    s.push_str("]}");
    s
}

fn read_bulk_request<S: LogRowsStorage>(
    stream_name: &str,
    r: &mut dyn Read,
    time_fields: &[&str],
    msg_fields: &[&str],
    preserve_keys: &[&str],
    lmp: &mut LogMessageProcessor<'_, S>,
) -> (usize, Result<(), String>) {
    // See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
    let mut lr = LineReader::new(stream_name, r);
    let mut fields_buf: Vec<Field> = Vec::new();

    let mut n = 0usize;
    loop {
        let (has_more_lines, err) = read_bulk_line(
            &mut lr,
            time_fields,
            msg_fields,
            preserve_keys,
            lmp,
            &mut fields_buf,
        );
        if err.is_some() || !has_more_lines {
            return (n, err.map_or(Ok(()), Err));
        }
        n += 1;
    }
}

fn read_bulk_line<S: LogRowsStorage>(
    lr: &mut LineReader,
    time_fields: &[&str],
    msg_fields: &[&str],
    preserve_keys: &[&str],
    lmp: &mut LogMessageProcessor<'_, S>,
    fields_buf: &mut Vec<Field>,
) -> (bool, Option<String>) {
    // Read the command, must be "create" or "index".
    loop {
        if !lr.next_line() {
            return (false, lr.err_string());
        }
        if !lr.line().is_empty() {
            break;
        }
    }
    // Copy the command line out before the next next_line() reuses the buffer.
    let line_str = String::from_utf8_lossy(lr.line()).into_owned();
    if !line_str.contains("\"create\"") && !line_str.contains("\"index\"") {
        return (
            false,
            Some(format!(
                "unexpected command {line_str:?}; expecting \"create\" or \"index\""
            )),
        );
    }

    // Decode the log message line.
    if !lr.next_line() {
        if let Some(e) = lr.err_string() {
            return (false, Some(e));
        }
        return (
            false,
            Some("missing log message after the \"create\" or \"index\" command".to_string()),
        );
    }
    if lr.line().is_empty() {
        // Special case - the line was too long and got skipped. Continue.
        return (true, None);
    }

    let line = lr.line();
    let mut p = get_json_parser();
    if let Err(err) = p.parse_log_message(line, preserve_keys, "") {
        let tail = if line.len() > 128 {
            &line[line.len() - 128..]
        } else {
            line
        };
        let msg = format!(
            "cannot parse json-encoded log entry: {err}; last {} bytes: {:?}",
            tail.len(),
            String::from_utf8_lossy(tail)
        );
        put_json_parser(p);
        return (false, Some(msg));
    }

    fields_buf.clear();
    fields_buf.extend_from_slice(p.fields());
    put_json_parser(p);

    let ts = match extract_timestamp_from_fields(time_fields, fields_buf) {
        Ok(ts) => ts,
        Err(err) => return (false, Some(format!("cannot parse timestamp: {err}"))),
    };
    let ts = if ts == 0 { now_unix_nanos() } else { ts };
    rename_field(fields_buf, msg_fields, "_msg");
    lmp.add_row(ts, fields_buf, -1);

    (true, None)
}

/// Elasticsearch-specific timestamp extraction. Unlike the shared helper this
/// returns 0 (not "now") when no matching field is found; the caller
/// substitutes the current time.
fn extract_timestamp_from_fields(
    time_fields: &[&str],
    fields: &mut [Field],
) -> Result<i64, String> {
    for time_field in time_fields {
        for f in fields.iter_mut() {
            if f.name != *time_field {
                continue;
            }
            let timestamp = parse_elasticsearch_timestamp(&f.value)?;
            f.value.clear();
            return Ok(timestamp);
        }
    }
    Ok(0)
}

fn parse_elasticsearch_timestamp(s: &str) -> Result<i64, String> {
    if s == "0" || s.is_empty() {
        // Zero or empty timestamp is substituted with the current time by the caller.
        return Ok(0);
    }
    let b = s.as_bytes();
    if b.len() < "YYYY-MM-DD".len() || b[4] != b'-' {
        // Try parsing a unix timestamp in seconds or milliseconds.
        return match try_parse_unix_timestamp(s) {
            Some(nsecs) => Ok(nsecs),
            None => Err(format!("cannot parse unix timestamp {s:?}")),
        };
    }
    if b.len() == "YYYY-MM-DD".len() {
        // PORT NOTE: Go uses time.Parse("2006-01-02", s) (UTC midnight); the
        // port synthesizes the RFC3339 form and reuses the RFC3339 parser.
        let rfc = format!("{s}T00:00:00Z");
        return match try_parse_timestamp_rfc3339_nano(&rfc) {
            Some(nsecs) => Ok(nsecs),
            None => Err(format!("cannot parse date {s:?}")),
        };
    }
    match try_parse_timestamp_rfc3339_nano(s) {
        Some(nsecs) => Ok(nsecs),
        None => Err(format!("cannot parse timestamp {s:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    use crate::common_params::CommonParams;
    use crate::testutil::{open_temp_storage, rows_count};

    #[test]
    fn test_bulk_response_string_format() {
        assert_eq!(
            bulk_response_string(0, 7),
            r#"{"took":7,"errors":false,"items":[]}"#
        );
        assert_eq!(
            bulk_response_string(2, 5),
            r#"{"took":5,"errors":false,"items":[{"create":{"status":201}},{"create":{"status":201}}]}"#
        );
    }

    #[test]
    fn test_parse_elasticsearch_timestamp() {
        assert_eq!(parse_elasticsearch_timestamp("").unwrap(), 0);
        assert_eq!(parse_elasticsearch_timestamp("0").unwrap(), 0);
        // Date-only is interpreted as UTC midnight.
        assert_eq!(
            parse_elasticsearch_timestamp("2023-01-15").unwrap(),
            1_673_740_800 * 1_000_000_000
        );
        assert_eq!(
            parse_elasticsearch_timestamp("2023-01-15T00:00:00Z").unwrap(),
            1_673_740_800 * 1_000_000_000
        );
    }

    #[test]
    fn test_read_bulk_request_parses_actions_and_docs() {
        let s = open_temp_storage("esbulk");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s);

        let body = b"{\"create\":{\"_index\":\"logs\"}}\n{\"_msg\":\"hello\",\"host\":\"a\"}\n{\"index\":{}}\n{\"_msg\":\"world\"}\n".to_vec();
        let mut cur = Cursor::new(body);
        let (n, res) = read_bulk_request("test", &mut cur, &["@timestamp"], &[], &[], &mut lmp);
        assert!(res.is_ok(), "unexpected error: {res:?}");
        assert_eq!(n, 2, "expected 2 bulk entries");

        lmp.close();
        assert_eq!(rows_count(&s), 2, "expected 2 rows ingested");
        s.must_close();
    }

    #[test]
    fn test_read_bulk_request_rejects_bad_command() {
        let s = open_temp_storage("esbulk-bad");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s);

        let body = b"{\"delete\":{}}\n{\"_msg\":\"x\"}\n".to_vec();
        let mut cur = Cursor::new(body);
        let (_n, res) = read_bulk_request("test", &mut cur, &["@timestamp"], &[], &[], &mut lmp);
        assert!(res.is_err(), "expected error for unsupported command");

        lmp.close();
        s.must_close();
    }
}

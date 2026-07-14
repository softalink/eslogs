//! Port of EsLogs `app/eslinsert/jsonline/jsonline.go`.
//!
//! ndjson body -> per line: parse JSON, extract `_time`, rename `_msg`, and add
//! the row to a [`LogMessageProcessor`], which flushes to the storage.

use std::io::Read;
use std::sync::Arc;

use esl_common::httpserver::{Request, ResponseWriter, get_quoted_remote_addr};
use esl_common::{errorf, warnf, writeconcurrencylimiter};

use esl_logstorage::json_parser::{get_json_parser, put_json_parser};
use esl_logstorage::rows::rename_field;

use crate::common_params::{
    LogMessageProcessor, LogRowsStorage, errorf_with_status, extract_timestamp_from_fields,
    get_common_params,
};
use crate::line_reader::LineReader;

/// RequestHandler processes jsonline insert requests.
pub fn request_handler<S: LogRowsStorage>(
    storage: &Arc<S>,
    req: &mut Request,
    w: &mut ResponseWriter,
) {
    w.set_header("Content-Type", "application/json");

    if req.method() != "POST" {
        w.set_status(405);
        return;
    }

    let cp = match get_common_params(req) {
        Ok(cp) => cp,
        Err(err) => {
            w.errorf(req, &err);
            return;
        }
    };
    if let Err((msg, status)) = storage.can_write_data() {
        errorf_with_status(w, req, &msg, status);
        return;
    }

    let stream_name = format!(
        "remoteAddr={}, requestURI={:?}",
        get_quoted_remote_addr(req),
        req.request_uri()
    );
    let time_fields: Vec<&str> = cp.time_fields.iter().map(String::as_str).collect();
    let msg_fields: Vec<&str> = cp.msg_fields.iter().map(String::as_str).collect();
    let preserve_keys: Vec<&[u8]> = cp.preserve_json_keys.iter().map(|s| s.as_bytes()).collect();

    let res = {
        // Go wraps r.Body with writeconcurrencylimiter.GetReader; on failure
        // it logs the error and returns without an HTTP error response.
        let mut wcr = match writeconcurrencylimiter::get_reader(req.body_reader()) {
            Ok(wcr) => wcr,
            Err(err) => {
                errorf!("cannot start reading jsonline request: {}", err.err);
                return;
            }
        };
        let mut lmp = cp.new_log_message_processor(storage, "jsonline");
        let res = process_stream_internal(
            &stream_name,
            &mut wcr,
            &time_fields,
            &msg_fields,
            &preserve_keys,
            &mut lmp,
        );
        lmp.close();
        res
    };

    if let Err(err) = res {
        w.errorf(
            req,
            &format!("cannot process jsonline request; error: {err}"),
        );
    }
}

fn process_stream_internal<S: LogRowsStorage>(
    stream_name: &str,
    r: &mut dyn Read,
    time_fields: &[&str],
    msg_fields: &[&str],
    preserve_keys: &[&[u8]],
    lmp: &mut LogMessageProcessor<'_, S>,
) -> Result<(), String> {
    let mut lr = LineReader::new(stream_name, r);

    let mut n = 0usize;
    let mut errors = 0usize;
    let mut last_error: Option<String> = None;
    loop {
        let (ok, err) = read_line(&mut lr, time_fields, msg_fields, preserve_keys, lmp);
        if let Some(e) = err {
            warnf!("jsonline: cannot read line #{n} in /jsonline request: {e}");
            last_error = Some(e);
            errors += 1;
        }
        if !ok {
            break;
        }
        n += 1;
    }

    if errors > 0 && n == errors {
        // Return an error if no logs were processed and there were errors.
        return Err(last_error.unwrap_or_default());
    }

    Ok(())
}

fn read_line<S: LogRowsStorage>(
    lr: &mut LineReader,
    time_fields: &[&str],
    msg_fields: &[&str],
    preserve_keys: &[&[u8]],
    lmp: &mut LogMessageProcessor<'_, S>,
) -> (bool, Option<String>) {
    loop {
        if !lr.next_line() {
            return (false, lr.err_string());
        }
        if !lr.line().is_empty() {
            break;
        }
    }

    let line = lr.line();

    let mut p = get_json_parser();
    if let Err(err) = p.parse_log_message(line, preserve_keys, "") {
        let msg = format!("{err}; line contents: {:?}", String::from_utf8_lossy(line));
        put_json_parser(p);
        return (true, Some(msg));
    }

    // Operate on the parser's fields in place and hand them straight to the
    // storage (which copies them into the LogRows arena), avoiding a redundant
    // clone into a scratch buffer for every log line. The parser is kept alive
    // until after `add_row` and only then returned to the pool.
    let ts = match extract_timestamp_from_fields(time_fields, p.fields_mut()) {
        Ok(ts) => ts,
        Err(err) => {
            put_json_parser(p);
            return (
                true,
                Some(format!(
                    "{err}; line contents: {:?}",
                    String::from_utf8_lossy(line)
                )),
            );
        }
    };
    rename_field(p.fields_mut(), msg_fields, "_msg");
    lmp.add_row(ts, p.fields_mut(), -1);
    put_json_parser(p);

    (true, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    use crate::common_params::CommonParams;
    use crate::testutil::{open_temp_storage, rows_count};

    #[test]
    fn test_jsonline_to_storage_roundtrip() {
        let s = open_temp_storage("jsonline");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");

        // No _time field, so each row defaults to the current timestamp.
        let body =
            b"{\"_msg\":\"m1\",\"host\":\"a\"}\n{\"_msg\":\"m2\",\"host\":\"b\"}\n{\"_msg\":\"m3\"}\n"
                .to_vec();
        let mut cur = Cursor::new(body);
        let no_fields: [&str; 0] = [];
        let no_keys: [&[u8]; 0] = [];
        let res =
            process_stream_internal("test", &mut cur, &["_time"], &no_fields, &no_keys, &mut lmp);
        assert!(res.is_ok(), "unexpected error: {res:?}");

        lmp.close();
        assert_eq!(rows_count(&s), 3, "expected 3 rows ingested");
        s.must_close();
    }

    /// End-to-end raw-byte preservation: a jsonline field value containing
    /// invalid UTF-8 must round-trip through storage bit-identically (Go
    /// strings are arbitrary bytes; the port must not U+FFFD-replace them).
    #[test]
    fn test_jsonline_invalid_utf8_value_roundtrip() {
        use std::sync::{Arc, Mutex};

        use esl_logstorage::parser::ParseQuery;
        use esl_logstorage::storage_search::{DataBlock, WriteDataBlockFn};
        use esl_logstorage::tenant_id::TenantID;

        let s = open_temp_storage("jsonline-rawbytes");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");

        // The _msg value contains the invalid UTF-8 byte 0xFF (raw bytes, not
        // a str literal).
        let body = b"{\"_msg\":\"a\xff b\"}\n".to_vec();
        let mut cur = Cursor::new(body);
        let no_fields: [&str; 0] = [];
        let no_keys: [&[u8]; 0] = [];
        let res =
            process_stream_internal("test", &mut cur, &["_time"], &no_fields, &no_keys, &mut lmp);
        assert!(res.is_ok(), "unexpected error: {res:?}");
        lmp.close();
        assert_eq!(rows_count(&s), 1, "expected 1 row ingested");

        // Query the row back and verify the _msg bytes are exactly the
        // ingested raw bytes (no U+FFFD replacement anywhere in the path).
        let q = ParseQuery("*").expect("parse query");
        let captured: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let cap = Arc::clone(&captured);
        let write: WriteDataBlockFn = Arc::new(move |_wid, db: &mut DataBlock| {
            for c in db.get_columns(false) {
                if c.name == b"_msg" {
                    for v in &c.values {
                        cap.lock().unwrap().push(v.clone());
                    }
                }
            }
        });
        s.run_query(&[TenantID::default()], &q, write)
            .expect("run_query");

        let vals = captured.lock().unwrap();
        assert_eq!(vals.len(), 1, "expected exactly one _msg value");
        assert_eq!(
            vals[0],
            b"a\xff b",
            "raw bytes must round-trip verbatim; got {:?}",
            String::from_utf8_lossy(&vals[0])
        );
        drop(vals);
        s.must_close();
    }

    #[test]
    fn test_jsonline_skips_blank_lines() {
        let s = open_temp_storage("jsonline-blank");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");

        let body = b"\n{\"_msg\":\"only\"}\n\n".to_vec();
        let mut cur = Cursor::new(body);
        let no_fields: [&str; 0] = [];
        let no_keys: [&[u8]; 0] = [];
        let res =
            process_stream_internal("test", &mut cur, &["_time"], &no_fields, &no_keys, &mut lmp);
        assert!(res.is_ok(), "unexpected error: {res:?}");

        lmp.close();
        assert_eq!(rows_count(&s), 1, "expected 1 row ingested");
        s.must_close();
    }
}

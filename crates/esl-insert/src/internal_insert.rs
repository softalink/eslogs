//! Port of EsLogs `app/eslinsert/internalinsert/internalinsert.go`.
//!
//! `/internal/insert` — the endpoint eslagent's remotewrite pushes marshaled
//! [`esl_logstorage::log_rows::InsertRow`]s to. The wire format is identical to
//! `/insert/multitenant/native`: a concatenation of `InsertRow::marshal`
//! outputs, with each row carrying its own tenantID.
//!
//! PORT NOTE: upstream v1.51.0 ships no `internalinsert_test.go`; the tests
//! below pin the wire format via a marshal → parse → storage round-trip.

use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use esl_common::flagutil::{Bytes, Flag};
use esl_common::httpserver::{Request, ResponseWriter};
use esl_common::logger::{LogThrottler, with_throttler};

use esl_logstorage::tenant_id::TenantID;

use crate::common_params::{LogRowsStorage, errorf_with_status, get_common_params};
use crate::native_insert::{
    PROTOCOL_VERSION, parse_data_multitenant, reset_unsupported_common_params,
};

static MAX_REQUEST_SIZE: Flag<Bytes> = Flag::new(
    "internalinsert.maxRequestSize",
    "The maximum size in bytes of a single request, which can be accepted at /internal/insert HTTP endpoint",
    || Bytes::with_default(64 * 1024 * 1024),
);

static UNSUPPORTED_OPTIONS_LOGGER: LazyLock<&'static LogThrottler> =
    LazyLock::new(|| with_throttler("unsuppoted_options", Duration::from_secs(5)));

/// RequestHandler processes /internal/insert requests.
pub fn request_handler<S: LogRowsStorage>(
    storage: &Arc<S>,
    req: &mut Request,
    w: &mut ResponseWriter,
) {
    if req.method() != "POST" {
        w.set_status(405);
        return;
    }
    let version = req.form_value("version").to_string();
    if version != PROTOCOL_VERSION {
        w.errorf(
            req,
            &format!("unsupported protocol version={version:?}; want {PROTOCOL_VERSION:?}"),
        );
        return;
    }

    let mut cp = match get_common_params(req) {
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

    if cp.tenant_id.account_id != 0 || cp.tenant_id.project_id != 0 {
        UNSUPPORTED_OPTIONS_LOGGER.warnf(format_args!(
            "/internal/insert endpoint doesn't support setting tenantID via AccountID and ProjectID request headers; \
             ignoring it; tenantID=\"{}\"",
            cp.tenant_id
        ));
        cp.tenant_id = TenantID::default();
    }

    reset_unsupported_common_params(&mut cp, "/internal/insert");

    // PORT NOTE: Go streams the body through
    // `protoparserutil.ReadUncompressedData`, which caps the *decompressed*
    // size at -internalinsert.maxRequestSize; the port's
    // `Request::body_reader` already decompresses per Content-Encoding, so the
    // cap is checked after reading the body in full.
    let data = match req.read_full_body() {
        Ok(d) => d,
        Err(err) => {
            w.errorf(req, &format!("cannot read internal insert request: {err}"));
            return;
        }
    };
    let max_request_size = MAX_REQUEST_SIZE.get().int_n().max(0) as usize;
    if data.len() > max_request_size {
        w.errorf(
            req,
            &format!(
                "cannot read internal insert request: request size ({} bytes) exceeds -internalinsert.maxRequestSize={max_request_size}",
                data.len()
            ),
        );
        return;
    }

    let mut lmp = cp.new_log_message_processor(storage, "internalinsert");
    // PORT NOTE: Go duplicates `parseData` in each package; the port reuses
    // `native_insert::parse_data_multitenant`, which is byte-for-byte the same
    // parser (rows keep their own tenantID).
    let res = parse_data_multitenant(&mut lmp, &data);
    lmp.close();
    if let Err(err) = res {
        w.errorf(req, &format!("cannot parse internal insert request: {err}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use esl_logstorage::log_rows::InsertRow;
    use esl_logstorage::rows::Field;
    use esl_logstorage::stream_tags::{get_stream_tags, put_stream_tags};

    use crate::common_params::{CommonParams, now_unix_nanos};
    use crate::testutil::{open_temp_storage, rows_count};

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    fn marshal_test_row(dst: &mut Vec<u8>, tenant_id: TenantID, msg: &str) {
        let mut st = get_stream_tags();
        st.add("app", "foo");
        let mut stream_tags_canonical = Vec::new();
        st.marshal_canonical(&mut stream_tags_canonical);
        put_stream_tags(st);

        let r = InsertRow {
            tenant_id,
            stream_tags_canonical,
            timestamp: now_unix_nanos(),
            fields: vec![field("_msg", msg), field("app", "foo")],
        };
        r.marshal(dst);
    }

    /// The /internal/insert wire format: concatenated `InsertRow::marshal`
    /// outputs, rows keeping their own tenantIDs.
    #[test]
    fn test_internal_insert_wire_roundtrip() {
        let s = open_temp_storage("internal-insert");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");

        let mut data = Vec::new();
        marshal_test_row(&mut data, TenantID::default(), "m1");
        marshal_test_row(
            &mut data,
            TenantID {
                account_id: 7,
                project_id: 9,
            },
            "m2",
        );
        marshal_test_row(&mut data, TenantID::default(), "m3");

        let res = parse_data_multitenant(&mut lmp, &data);
        assert!(res.is_ok(), "unexpected error: {res:?}");

        lmp.close();
        assert_eq!(rows_count(&s), 3, "expected 3 rows ingested");
        s.must_close();
    }

    #[test]
    fn test_internal_insert_rejects_garbage() {
        let s = open_temp_storage("internal-insert-bad");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");

        let mut data = Vec::new();
        marshal_test_row(&mut data, TenantID::default(), "m1");
        data.extend_from_slice(b"\xff\xff\xff"); // trailing garbage

        let res = parse_data_multitenant(&mut lmp, &data);
        assert!(res.is_err(), "expected error for trailing garbage");
        assert!(
            res.unwrap_err().starts_with("cannot parse row #1"),
            "error must reference the failing row"
        );

        lmp.close();
        s.must_close();
    }
}

//! Port of EsLogs `app/eslinsert/nativeinsert/nativeinsert.go` and
//! `app/eslinsert/nativeinsert/nativemultitenant/multitenant.go`.
//!
//! The native binary ingestion protocol: `/insert/native` (single-tenant, the
//! tenant comes from the AccountID/ProjectID request headers) and
//! `/insert/multitenant/native` (each marshaled row carries its own tenantID).
//!
//! PORT NOTE: Go keeps the multitenant handler in the nested
//! `nativemultitenant` package; the port merges both files into this module
//! ([`request_handler`] / [`multitenant_request_handler`]).

use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use esl_common::flagutil::{Bytes, Flag};
use esl_common::httpserver::{Request, ResponseWriter};
use esl_common::logger::{LogThrottler, with_throttler};

use esl_logstorage::log_rows::{get_insert_row, put_insert_row};
use esl_logstorage::rows::marshal_fields_to_json;
use esl_logstorage::tenant_id::TenantID;

use crate::common_params::{
    CommonParams, InsertRowProcessor, LogRowsStorage, errorf_with_status, get_common_params,
};

/// ProtocolVersion is the version of the data ingestion protocol.
///
/// It must be changed every time the data encoding at /internal/insert HTTP
/// endpoint is changed.
///
/// PORT NOTE: the canonical constant lives in
/// [`esl_storage::netinsert::PROTOCOL_VERSION`], mirroring Go's
/// `app/eslstorage/netinsert.ProtocolVersion`, which both the eslagent
/// remotewrite client and the server-side handlers reference. Re-exported
/// here for the server-side users ([`crate::internal_insert`] included).
pub use esl_storage::netinsert::PROTOCOL_VERSION;

/// MaxRequestSize is the maximum size for the request to /insert/native and
/// /insert/multitenant/native.
pub static MAX_REQUEST_SIZE: Flag<Bytes> = Flag::new(
    "nativeinsert.maxRequestSize",
    "The maximum size in bytes of a single request, which can be accepted \
     at /insert/native and /insert/multitenant/native HTTP endpoints",
    || Bytes::with_default(64 * 1024 * 1024),
);
esl_common::register_flag!(MAX_REQUEST_SIZE);

static UNSUPPORTED_OPTIONS_LOGGER: LazyLock<&'static LogThrottler> =
    LazyLock::new(|| with_throttler("unsuppoted_options", Duration::from_secs(5)));

static INVALID_TENANT_ID_LOGGER: LazyLock<&'static LogThrottler> =
    LazyLock::new(|| with_throttler("invalid_tenant_id", Duration::from_secs(5)));

/// RequestHandler processes /insert/native requests.
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

    reset_unsupported_common_params(&mut cp, "/insert/native");

    let data = match read_capped_body(req, w, "native insert") {
        Some(d) => d,
        None => return,
    };

    let mut lmp = cp.new_log_message_processor(storage, "nativeinsert");
    let res = parse_data(&mut lmp, &data, cp.tenant_id);
    lmp.close();
    if let Err(err) = res {
        w.errorf(req, &format!("cannot parse native insert request: {err}"));
    }
}

/// RequestHandler processes /insert/multitenant/native requests
/// (Go `nativemultitenant.RequestHandler`).
pub fn multitenant_request_handler<S: LogRowsStorage>(
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
            "/insert/multitenant/native endpoint doesn't support setting tenantID via AccountID and ProjectID request headers; \
             ignoring it; tenantID=\"{}\"; see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy",
            cp.tenant_id
        ));
        cp.tenant_id = TenantID::default();
    }

    reset_unsupported_common_params(&mut cp, "/insert/multitenant/native");

    let data = match read_capped_body(req, w, "request to /insert/multitenant/native") {
        Some(d) => d,
        None => return,
    };

    let mut lmp = cp.new_log_message_processor(storage, "nativemultitenant");
    let res = parse_data_multitenant(&mut lmp, &data);
    lmp.close();
    if let Err(err) = res {
        w.errorf(
            req,
            &format!("cannot parse request to /insert/multitenant/native: {err}"),
        );
    }
}

/// The native protocol carries timestamps, stream tags and fields inside the
/// marshaled rows, so the corresponding HTTP options are ignored with a
/// throttled warning (shared boilerplate of the Go native/multitenant/internal
/// handlers).
pub(crate) fn reset_unsupported_common_params(cp: &mut CommonParams, endpoint: &str) {
    if cp.is_time_field_set {
        UNSUPPORTED_OPTIONS_LOGGER.warnf(format_args!(
            "{endpoint} endpoint doesn't support setting time fields via _time_field query arg and via ESL-Time-Field request header; \
             ignoring them; timeFields={:?}; see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy",
            cp.time_fields
        ));
    }
    // Unconditionally reset cp.time_fields, since the code below shouldn't depend on this field.
    cp.time_fields = Vec::new();

    if !cp.msg_fields.is_empty() {
        UNSUPPORTED_OPTIONS_LOGGER.warnf(format_args!(
            "{endpoint} endpoint doesn't support setting msg fields via _msg_field query arg and via ESL-Msg-Field request header; \
             ignoring them; msgFields={:?}; see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy",
            cp.msg_fields
        ));
        cp.msg_fields = Vec::new();
    }
    if !cp.stream_fields.is_empty() {
        UNSUPPORTED_OPTIONS_LOGGER.warnf(format_args!(
            "{endpoint} endpoint doesn't support setting stream fields via _stream_fields query arg and via ESL-Stream-Fields request header; \
             ignoring them; streamFields={:?}; see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy",
            cp.stream_fields
        ));
        cp.stream_fields = Vec::new();
    }
    if !cp.decolorize_fields.is_empty() {
        UNSUPPORTED_OPTIONS_LOGGER.warnf(format_args!(
            "{endpoint} endpoint doesn't support setting decolorize_fields query arg and ESL-Decolorize-Fields request header; \
             ignoring them; decolorizeFields={:?}; see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy",
            cp.decolorize_fields
        ));
        cp.decolorize_fields = Vec::new();
    }
}

/// Reads the full (already-decompressed) request body and enforces
/// `-nativeinsert.maxRequestSize`.
///
/// Go streams the body through `protoparserutil.ReadUncompressedData`, which
/// caps the *decompressed* size at `-nativeinsert.maxRequestSize` during the
/// read; `read_full_body_limited` decompresses per Content-Encoding and applies
/// the cap while reading, so a decompression bomb cannot fully materialize.
fn read_capped_body(req: &mut Request, w: &mut ResponseWriter, what: &str) -> Option<Vec<u8>> {
    match req.read_full_body_limited(
        MAX_REQUEST_SIZE.get().int_n() as i64,
        MAX_REQUEST_SIZE.name(),
    ) {
        Ok(d) => Some(d),
        Err(err) => {
            w.errorf(req, &format!("cannot read {what}: {err}"));
            None
        }
    }
}

/// Parses marshaled InsertRows from data, overriding each row's tenantID with
/// the given tenant_id (Go `nativeinsert.parseData`).
fn parse_data(
    irp: &mut impl InsertRowProcessor,
    data: &[u8],
    tenant_id: TenantID,
) -> Result<(), String> {
    let zero_tenant_id = TenantID::default();

    let mut r = get_insert_row();

    let mut src = data;
    let mut i = 0usize;
    while !src.is_empty() {
        match r.unmarshal_inplace(src) {
            Ok(tail) => src = tail,
            Err(err) => {
                put_insert_row(r);
                return Err(format!("cannot parse row #{i}: {err}"));
            }
        }
        i += 1;

        if !r.tenant_id.equal(&zero_tenant_id) && !r.tenant_id.equal(&tenant_id) {
            let mut line = Vec::new();
            marshal_fields_to_json(&mut line, &r.fields);
            INVALID_TENANT_ID_LOGGER.warnf(format_args!(
                "use \"{tenant_id}\" from AccountID and ProjectID request headers as tenantID for the log entry instead of \"{}\"; \
                 see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy ; \
                 log entry: {}",
                r.tenant_id,
                String::from_utf8_lossy(&line)
            ));
        }

        r.tenant_id = tenant_id;

        irp.add_insert_row(&r);
    }

    put_insert_row(r);
    Ok(())
}

/// Parses marshaled InsertRows from data, keeping each row's own tenantID
/// (Go `nativemultitenant.parseData`; also used by `/internal/insert`).
pub(crate) fn parse_data_multitenant(
    irp: &mut impl InsertRowProcessor,
    data: &[u8],
) -> Result<(), String> {
    let mut r = get_insert_row();

    let mut src = data;
    let mut i = 0usize;
    while !src.is_empty() {
        match r.unmarshal_inplace(src) {
            Ok(tail) => src = tail,
            Err(err) => {
                put_insert_row(r);
                return Err(format!("cannot parse row #{i}: {err}"));
            }
        }
        i += 1;

        irp.add_insert_row(&r);
    }

    put_insert_row(r);
    Ok(())
}

#[cfg(test)]
mod tests {
    //! PORT NOTE: upstream v1.51.0 ships no `nativeinsert` tests; these
    //! round-trip tests exercise the marshal → parse_data → storage path to
    //! pin the wire format.

    use super::*;

    use esl_logstorage::log_rows::InsertRow;
    use esl_logstorage::rows::Field;
    use esl_logstorage::stream_tags::{get_stream_tags, put_stream_tags};

    use crate::common_params::{CommonParams, now_unix_nanos};
    use crate::testutil::{open_temp_storage, rows_count};

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    /// Returns a marshaled InsertRow for the given tenant with an `app=foo`
    /// stream tag and a matching `app` field.
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

    #[test]
    fn test_parse_data_roundtrip() {
        let s = open_temp_storage("native");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");

        let mut data = Vec::new();
        marshal_test_row(&mut data, TenantID::default(), "m1");
        marshal_test_row(&mut data, TenantID::default(), "m2");

        let res = parse_data(&mut lmp, &data, TenantID::default());
        assert!(res.is_ok(), "unexpected error: {res:?}");

        lmp.close();
        assert_eq!(rows_count(&s), 2, "expected 2 rows ingested");
        s.must_close();
    }

    #[test]
    fn test_parse_data_overrides_tenant_id() {
        let s = open_temp_storage("native-tenant");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");

        // The row carries tenant 1:2, while the request tenant is the default;
        // parse_data must override it with the request tenant.
        let row_tenant = TenantID {
            account_id: 1,
            project_id: 2,
        };
        let mut data = Vec::new();
        marshal_test_row(&mut data, row_tenant, "m1");

        let res = parse_data(&mut lmp, &data, TenantID::default());
        assert!(res.is_ok(), "unexpected error: {res:?}");

        lmp.close();
        assert_eq!(rows_count(&s), 1, "expected 1 row ingested");
        s.must_close();
    }

    #[test]
    fn test_parse_data_multitenant_roundtrip() {
        let s = open_temp_storage("native-multitenant");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");

        let mut data = Vec::new();
        marshal_test_row(&mut data, TenantID::default(), "m1");
        marshal_test_row(
            &mut data,
            TenantID {
                account_id: 3,
                project_id: 4,
            },
            "m2",
        );

        let res = parse_data_multitenant(&mut lmp, &data);
        assert!(res.is_ok(), "unexpected error: {res:?}");

        lmp.close();
        assert_eq!(rows_count(&s), 2, "expected 2 rows ingested");
        s.must_close();
    }

    #[test]
    fn test_parse_data_error() {
        let s = open_temp_storage("native-bad");
        let cp = CommonParams::empty();
        let mut lmp = cp.new_log_message_processor(&s, "test");

        let res = parse_data(&mut lmp, b"\x01\x02\x03", TenantID::default());
        assert!(res.is_err(), "expected error for truncated data");
        assert!(
            res.unwrap_err().starts_with("cannot parse row #0"),
            "error must reference the failing row"
        );

        lmp.close();
        s.must_close();
    }
}

//! Port of EsLogs `lib/logstorage/log_rows.go`.

// TODO: remove once the upstream consumers of this module (datadb.go,
// inmemory_part.go, partition.go, storage.go and the eslinsert/eslstorage app
// layer) are ported; until then parts of the crate-private API are only
// exercised by the tests below.
#![allow(dead_code)]

use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use esl_common::logger::{LogThrottler, with_throttler};
use esl_common::{encoding, slicesutil};

use crate::color_sequence::{drop_color_sequences, has_color_sequences};
use crate::consts::{MAX_COLUMNS_PER_BLOCK, MAX_FIELD_NAME_SIZE, MAX_UNCOMPRESSED_BLOCK_SIZE};
use crate::hash128::hash128;
use crate::prefix_filter::Filter;
use crate::rows::{
    Field, marshal_fields_to_json, skip_leading_fields_without_values, sort_fields_by_name,
};
use crate::stream_id::StreamID;
use crate::stream_tags::{
    check_stream_field_name_bytes, get_stream_tags, get_stream_tags_string, put_stream_tags,
};
use crate::tenant_id::TenantID;
use crate::values_encoder::marshal_timestamp_rfc3339_nano_string;

/// The length of the Go `time.RFC3339Nano` layout string
/// (`"2006-01-02T15:04:05.999999999Z07:00"`), used by
/// [`estimated_json_row_len`].
const RFC3339_NANO_LAYOUT_LEN: usize = "2006-01-02T15:04:05.999999999Z07:00".len();

// ---------------------------------------------------------------------------
// LogRows
// ---------------------------------------------------------------------------

/// LogRows holds a set of rows needed for Storage.MustAddRows
///
/// LogRows must be obtained via [`get_log_rows`].
///
/// PORT NOTE: Go backs all the row strings with an arena (`lr.a`) and packs
/// all the fields into a shared `fieldsBuf`, slicing the rows out of it. The
/// Rust `Field` owns its strings, so the rows are stored as `Vec<Vec<Field>>`
/// (the same shape as `rows::Rows`) and the arena is replaced with
/// `a_size_bytes`, which tracks the number of bytes the Go arena would hold,
/// so [`LogRows::need_flush`] fires at the same points as in Go.
#[derive(Default)]
pub struct LogRows {
    /// The number of bytes the Go arena would hold for the current rows.
    a_size_bytes: usize,

    /// stream_ids holds streamIDs for rows added to LogRows
    pub(crate) stream_ids: Vec<StreamID>,

    /// timestamps holds timestamps for rows added to LogRows
    pub(crate) timestamps: Vec<i64>,

    /// rows holds fields for rows added to LogRows.
    pub(crate) rows: Vec<Vec<Field>>,

    /// stream_tags_canonicals holds streamTagsCanonical entries for rows added to LogRows
    ///
    /// PORT NOTE: Go stores the canonical representation in binary Go
    /// strings; the port uses `Vec<u8>`.
    pub(crate) stream_tags_canonicals: Vec<Vec<u8>>,

    /// stream_fields contains names for stream fields
    stream_fields: Vec<String>,

    /// ignore_fields is a filter for fields, which must be ignored during data ingestion
    ignore_fields: Filter,

    /// decolorize_fields is a filter for fields, which must be cleared from ANSI color escape sequences
    decolorize_fields: Filter,

    /// extra_fields contains extra fields to add to all the logs at must_add().
    extra_fields: Vec<Field>,

    /// extra_stream_fields contains extra_fields, which must be treated as stream fields.
    extra_stream_fields: Vec<Field>,

    /// default_msg_value contains default value for missing _msg field
    default_msg_value: String,
}

impl LogRows {
    /// Calls callback for every row stored in the lr.
    pub fn for_each_row(&self, mut callback: impl FnMut(u64, &InsertRow)) {
        let mut r = get_insert_row();
        for (i, &timestamp) in self.timestamps.iter().enumerate() {
            let sid = &self.stream_ids[i];

            let stream_hash = sid.id.lo ^ sid.id.hi;

            r.tenant_id = sid.tenant_id;
            r.stream_tags_canonical.clear();
            r.stream_tags_canonical
                .extend_from_slice(&self.stream_tags_canonicals[i]);
            r.timestamp = timestamp;
            // PORT NOTE: Go points r.Fields at lr.rows[i] without copying and
            // drops the reference afterwards; owned Fields require a copy.
            r.fields.clear();
            r.fields.extend_from_slice(&self.rows[i]);

            callback(stream_hash, &r);
        }
        // remove reference to logRows fields
        // since reset of r can modify actual LogRows
        r.fields.clear();
        put_insert_row(r);
    }

    /// Resets lr with all its settings.
    ///
    /// Call [`LogRows::reset_keep_settings`] for resetting lr without resetting its settings.
    pub fn reset(&mut self) {
        self.reset_keep_settings();

        self.stream_fields.clear();

        self.ignore_fields.reset();
        self.decolorize_fields.reset();

        self.extra_fields.clear();

        self.extra_stream_fields.clear();

        self.default_msg_value.clear();
    }

    /// Returns current log rows count
    pub fn rows_count(&self) -> usize {
        self.rows.len()
    }

    /// Resets rows stored in lr, while keeping its settings passed to [`get_log_rows`].
    pub fn reset_keep_settings(&mut self) {
        self.a_size_bytes = 0;

        self.stream_ids.clear();

        self.stream_tags_canonicals.clear();

        self.timestamps.clear();

        self.rows.clear();
    }

    /// Returns true if lr contains too much data, so it must be flushed to the storage.
    pub fn need_flush(&self) -> bool {
        self.a_size_bytes > (MAX_UNCOMPRESSED_BLOCK_SIZE / 8) * 7
            || self.rows.len() > MAX_UNCOMPRESSED_BLOCK_SIZE / 100
    }

    /// Adds r to lr.
    pub fn must_add_insert_row(&mut self, r: &InsertRow) {
        // verify r.stream_tags_canonical
        if let Err(err) = verify_stream_tags_canonical(&r.stream_tags_canonical, &r.fields) {
            let line = fields_to_json_string(&r.fields);
            INVALID_STREAM_TAGS_LOGGER.warnf(format_args!(
                "cannot unmarshal streamTagsCanonical: {err}; skipping the log entry; log entry: {line}"
            ));
            return;
        }

        // Calculate the id for the StreamTags
        let sid = StreamID {
            tenant_id: r.tenant_id,
            id: hash128(&r.stream_tags_canonical),
        };

        // Store the row
        self.must_add_internal(sid, r.timestamp, &r.fields, &r.stream_tags_canonical);
    }

    /// Adds a log entry with the given args to lr.
    ///
    /// If stream_fields_len >= 0, then the given number of initial fields are used as log stream fields
    /// instead of the pre-configured stream fields from [`get_log_rows`].
    ///
    /// It is OK to modify the args after returning from the function, since lr copies all the args to internal data.
    ///
    /// Log entries are dropped with the warning message in the following cases:
    /// - if there are too many log fields
    /// - if there are too long log field names
    /// - if the total length of log entries is too long
    /// - if the log entry contains _stream or _stream_id fields (these fields clash with the automatically generated fields by EsLogs)
    ///
    /// PORT NOTE: Go's unexported `mustAdd(tenantID, timestamp, fields)`
    /// wrapper (test-only) collides with this name in snake_case and is
    /// inlined at its call sites as `must_add(.., -1)`. `fields` is `&mut`
    /// because Go clears the values of `_stream`/`_stream_id` fields in the
    /// caller-provided slice.
    pub fn must_add(
        &mut self,
        tenant_id: TenantID,
        timestamp: i64,
        fields: &mut [Field],
        stream_fields_len: isize,
    ) {
        // Compose StreamTags from fields
        let mut st = get_stream_tags();
        if stream_fields_len >= 0 {
            // Compose StreamTags from fields[..stream_fields_len] and ignore lr.stream_fields with lr.extra_stream_fields.
            for f in &fields[..stream_fields_len as usize] {
                let field_name = get_canonical_field_name_bytes(&f.name);

                if let Err(err) = check_stream_field_name_bytes(field_name) {
                    let line = fields_to_json_string(fields);
                    // Log text only: lossy view of the raw name bytes.
                    INVALID_STREAM_TAGS_LOGGER.warnf(format_args!(
                        "invalid stream field name {:?}: {err}; skipping the log entry; log entry: {line}",
                        String::from_utf8_lossy(field_name)
                    ));
                    put_stream_tags(st);
                    return;
                }

                if !self.ignore_fields.match_string_bytes(field_name) {
                    st.add(field_name, &f.value);
                }
            }
        } else if !self.stream_fields.is_empty() || !self.extra_stream_fields.is_empty() {
            // Compose StreamTags from lr.stream_fields and lr.extra_stream_fields.
            for f in fields.iter() {
                let field_name = get_canonical_field_name_bytes(&f.name);
                if self
                    .stream_fields
                    .iter()
                    .any(|s| s.as_bytes() == field_name)
                {
                    st.add(field_name, &f.value);
                }
            }
            for f in &self.extra_stream_fields {
                let field_name = get_canonical_field_name_bytes(&f.name);
                st.add(field_name, &f.value);
            }
        } else {
            // Extract StreamTags from _stream field.
            // This can be used when importing the raw logs in JSON line format
            // received from /select/logsql/query endpoint.
            for i in 0..fields.len() {
                match fields[i].name.as_slice() {
                    b"_stream" => {
                        // The _stream value is a rendered `{k="v",...}` string;
                        // invalid UTF-8 is a malformed stream string, routed
                        // through the same parse-error path.
                        let parse_result = match std::str::from_utf8(&fields[i].value) {
                            Ok(s) => st.unmarshal_string_inplace(s),
                            Err(err) => Err(err.to_string()),
                        };
                        if let Err(err) = parse_result {
                            let line = fields_to_json_string(fields);
                            INVALID_STREAM_TAGS_LOGGER.warnf(format_args!(
                                "cannot parse _stream={}: {err}; skipping the log entry; log entry: {line}",
                                String::from_utf8_lossy(&fields[i].value)
                            ));
                            put_stream_tags(st);
                            return;
                        }
                        if let Err(err) = st.verify_canonical_field_values(fields) {
                            let line = fields_to_json_string(fields);
                            INVALID_STREAM_TAGS_LOGGER.warnf(format_args!(
                                "invalid _stream={}: {err}; skipping the log entry; log entry: {line}",
                                String::from_utf8_lossy(&fields[i].value)
                            ));
                            put_stream_tags(st);
                            return;
                        }
                        // Remove _stream field, since it is re-generated from st below.
                        fields[i].value.clear();
                    }
                    b"_stream_id" => {
                        // Remove _stream_id field, since it is re-generated from st below.
                        fields[i].value.clear();
                    }
                    _ => {}
                }
            }
        }

        // Marshal StreamTags
        //
        // PORT NOTE: Go takes the buffer from bytesutil's bbPool; a local Vec
        // is used instead.
        let mut bb = Vec::new();
        st.marshal_canonical(&mut bb);
        put_stream_tags(st);

        // Calculate the id for the StreamTags
        let sid = StreamID {
            tenant_id,
            id: hash128(&bb),
        };

        // Store the row
        self.must_add_internal(sid, timestamp, fields, &bb);
    }

    fn must_add_internal(
        &mut self,
        sid: StreamID,
        timestamp: i64,
        fields: &[Field],
        stream_tags_canonical: &[u8],
    ) {
        // Verify that the log entry doesn't exceed limits.
        if fields.len() > MAX_COLUMNS_PER_BLOCK {
            let line = fields_to_json_string(fields);
            TOO_MANY_COLUMNS_LOGGER.warnf(format_args!(
                "ignoring log entry with too big number of fields {}, since it exceeds the limit {MAX_COLUMNS_PER_BLOCK}; \
                see https://docs.victoriametrics.com/victorialogs/faq/#how-many-fields-a-single-log-entry-may-contain ; log entry: {line}",
                fields.len()
            ));
            return;
        }
        for f in fields {
            let field_name = &f.name;
            if field_name.len() > MAX_FIELD_NAME_SIZE {
                let line = fields_to_json_string(fields);
                TOO_LONG_FIELD_NAME_LOGGER.warnf(format_args!(
                    "ignoring log entry with too long field name {field_name:?}, since its length ({}) exceeds the limit {MAX_FIELD_NAME_SIZE} bytes; \
                    see https://docs.victoriametrics.com/victorialogs/faq/#what-is-the-maximum-supported-field-name-length ; log entry: {line}",
                    field_name.len()
                ));
                return;
            }
        }
        let row_len = estimated_json_row_len(fields);
        if row_len > MAX_UNCOMPRESSED_BLOCK_SIZE {
            let line = fields_to_json_string(fields);
            TOO_LONG_ENTRY_LOGGER.warnf(format_args!(
                "ignoring too long log entry with the estimated length of {row_len} bytes, since it exceeds the limit {MAX_UNCOMPRESSED_BLOCK_SIZE} bytes; \
                see https://docs.victoriametrics.com/victorialogs/faq/#what-length-a-log-record-is-expected-to-have ; log entry: {line}"
            ));
            return;
        }

        if self
            .stream_tags_canonicals
            .last()
            .is_some_and(|last| last == stream_tags_canonical)
        {
            // Go re-uses the previous string header without copying it into
            // the arena, so a_size_bytes doesn't grow here.
            let last = self.stream_tags_canonicals.last().unwrap().clone();
            self.stream_tags_canonicals.push(last);
        } else {
            self.a_size_bytes += stream_tags_canonical.len();
            self.stream_tags_canonicals
                .push(stream_tags_canonical.to_vec());
        }

        self.stream_ids.push(sid);
        self.timestamps.push(timestamp);

        let mut row: Vec<Field> = Vec::with_capacity(fields.len());
        let mut has_msg_field = self.add_fields_internal(&mut row, fields, true, true);
        // PORT NOTE: extra_fields is temporarily moved out to satisfy the
        // borrow checker (Go passes lr.extraFields with nil filters).
        let extra_fields = std::mem::take(&mut self.extra_fields);
        if self.add_fields_internal(&mut row, &extra_fields, false, false) {
            has_msg_field = true;
        }
        self.extra_fields = extra_fields;

        // Add optional default _msg field
        if !has_msg_field && !self.default_msg_value.is_empty() {
            row.push(Field {
                name: Vec::new(),
                value: self.default_msg_value.clone().into_bytes(),
            });
        }

        // Add log row fields to lr.rows
        self.rows.push(row);
    }

    /// PORT NOTE: Go passes `ignoreFields`/`decolorizeFields` pointers (nil
    /// for the extra-fields call); the port uses the `use_filters` flag and
    /// reads `self.ignore_fields`/`self.decolorize_fields` directly. The
    /// prev-row dedup avoids arena copies in Go; here it only drives the
    /// `a_size_bytes` accounting (a copy is made either way).
    fn add_fields_internal(
        &mut self,
        dst_row: &mut Vec<Field>,
        fields: &[Field],
        use_filters: bool,
        must_copy_fields: bool,
    ) -> bool {
        if fields.is_empty() {
            return false;
        }

        let mut prev_row: Option<&[Field]> = self.rows.last().map(|row| row.as_slice());

        let mut has_msg_field = false;
        for (i, f) in fields.iter().enumerate() {
            let field_name = get_canonical_field_name_bytes(&f.name);

            if use_filters && self.ignore_fields.match_string_bytes(field_name) {
                continue;
            }
            if f.value.is_empty() {
                // Skip fields without values
                continue;
            }
            if field_name == b"_time" {
                // Values for the _time field are stored in lr.timestamps
                // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1168
                let line = fields_to_json_string(fields);
                UNEXPECTED_TIME_FIELD_LOGGER.warnf(format_args!(
                    "skipping _time field with the value {:?} because the timestamp is parsed from another field \
                    according to https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-parameters ; log entry: {line}",
                    f.value
                ));
                continue;
            }
            if field_name == b"_stream" || field_name == b"_stream_id" {
                let line = fields_to_json_string(fields);
                // Log text only: lossy view of the raw name bytes.
                UNEXPECTED_STREAM_FIELD_LOGGER.warnf(format_args!(
                    "skipping {:?} field with the value {:?} since it clashes with the automatically generated field by EsLogs; \
                    see https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields; log entry: {line}",
                    String::from_utf8_lossy(field_name),
                    f.value
                ));
                continue;
            }

            let prev_field: Option<&Field> = prev_row.and_then(|row| row.get(i));

            let mut dst_field = Field::default();

            if field_name.is_empty() {
                has_msg_field = true;
            }

            match prev_field {
                Some(prev) if prev.name == field_name => {
                    dst_field.name.extend_from_slice(field_name);
                }
                _ => {
                    dst_field.name.extend_from_slice(field_name);
                    if must_copy_fields {
                        self.a_size_bytes += field_name.len();
                    }
                    prev_row = None;
                }
            }
            match prev_field {
                Some(prev) if prev.value == f.value => {
                    dst_field.value.extend_from_slice(&f.value);
                }
                _ => {
                    dst_field.value.extend_from_slice(&f.value);
                    if must_copy_fields {
                        self.a_size_bytes += f.value.len();
                    }

                    if use_filters
                        && self.decolorize_fields.match_string_bytes(field_name)
                        && has_color_sequences(&dst_field.value)
                    {
                        let mut b = Vec::new();
                        drop_color_sequences(&mut b, &dst_field.value);
                        // Go appends the decolorized value to the arena in
                        // addition to the original copy above.
                        self.a_size_bytes += b.len();
                        dst_field.value = b;
                    }
                }
            }
            dst_row.push(dst_field);
        }

        has_msg_field
    }

    /// Returns string representation of the row with the given idx.
    pub fn get_row_string(&self, idx: usize) -> String {
        // PORT NOTE: Go formats the timestamp via TimeFormatter (storage.go,
        // not yet ported); marshal_timestamp_rfc3339_nano_string produces the
        // identical RFC3339Nano representation.
        let mut time_buf = Vec::new();
        marshal_timestamp_rfc3339_nano_string(&mut time_buf, self.timestamps[idx]);
        let stream_tags = get_stream_tags_string(&self.stream_tags_canonicals[idx]);
        let mut fields: Vec<Field> = self.rows[idx].clone();
        fields.push(Field {
            name: b"_time".to_vec(),
            value: time_buf,
        });
        fields.push(Field {
            name: b"_stream".to_vec(),
            value: stream_tags.into_bytes(),
        });
        sort_fields_by_name(&mut fields);
        let mut line = Vec::new();
        marshal_fields_to_json(&mut line, &fields);
        // Display-only representation: values may hold arbitrary bytes, so a
        // lossy conversion is used instead of a panicking unsafe-string view.
        String::from_utf8_lossy(&line).into_owned()
    }
}

static INVALID_STREAM_TAGS_LOGGER: LazyLock<&'static LogThrottler> =
    LazyLock::new(|| with_throttler("invalid_stream_tags", Duration::from_secs(5)));

fn verify_stream_tags_canonical(
    stream_tags_canonical: &[u8],
    fields: &[Field],
) -> Result<(), String> {
    let mut st = get_stream_tags();

    let tail = match st.unmarshal_canonical_inplace(stream_tags_canonical) {
        Ok(tail) => tail,
        Err(err) => {
            put_stream_tags(st);
            return Err(format!("cannot unmarshal streamTagsCanonical: {err}"));
        }
    };
    if !tail.is_empty() {
        let msg = format!(
            "unexpected tail left after unmarshaling streamTagsCanonical; len(tail)={}; streamTags: {st}",
            tail.len()
        );
        put_stream_tags(st);
        return Err(msg);
    }
    let result = st.verify_canonical_field_values(fields);
    put_stream_tags(st);
    result
}

static TOO_MANY_COLUMNS_LOGGER: LazyLock<&'static LogThrottler> =
    LazyLock::new(|| with_throttler("too_many_columns", Duration::from_secs(5)));
// PORT NOTE: the "too_logn_field_name" typo is preserved from Go.
static TOO_LONG_FIELD_NAME_LOGGER: LazyLock<&'static LogThrottler> =
    LazyLock::new(|| with_throttler("too_logn_field_name", Duration::from_secs(5)));
static TOO_LONG_ENTRY_LOGGER: LazyLock<&'static LogThrottler> =
    LazyLock::new(|| with_throttler("too_long_entry", Duration::from_secs(5)));
static UNEXPECTED_TIME_FIELD_LOGGER: LazyLock<&'static LogThrottler> =
    LazyLock::new(|| with_throttler("unexpected_time_field", Duration::from_secs(5)));
static UNEXPECTED_STREAM_FIELD_LOGGER: LazyLock<&'static LogThrottler> =
    LazyLock::new(|| with_throttler("unexpected_stream_field", Duration::from_secs(5)));

pub(crate) fn get_canonical_field_name(field_name: &str) -> &str {
    if field_name == "_msg" {
        return "";
    }
    field_name
}

/// Byte-name variant of [`get_canonical_field_name`] for `Field.name`
/// (raw bytes) call sites.
pub(crate) fn get_canonical_field_name_bytes(field_name: &[u8]) -> &[u8] {
    if field_name == b"_msg" {
        return b"";
    }
    field_name
}

pub(crate) fn get_canonical_column_name(field_name: &str) -> &str {
    if field_name.is_empty() {
        return "_msg";
    }
    field_name
}

/// Byte-name variant of [`get_canonical_column_name`] for `Field.name`
/// (raw bytes) call sites.
pub(crate) fn get_canonical_column_name_bytes(field_name: &[u8]) -> &[u8] {
    if field_name.is_empty() {
        return b"_msg";
    }
    field_name
}

/// PORT NOTE: helper replacing Go's `MarshalFieldsToJSON(nil, fields)` +
/// implicit `[]byte` → `%s` conversion in the throttled warning messages.
fn fields_to_json_string(fields: &[Field]) -> String {
    let mut line = Vec::new();
    marshal_fields_to_json(&mut line, fields);
    // Display-only (throttled warning messages): values may hold arbitrary
    // bytes, so a lossy conversion is used.
    String::from_utf8_lossy(&line).into_owned()
}

/// Returns LogRows from the pool for the given stream_fields.
///
/// stream_fields is a set of fields, which must be associated with the stream.
///
/// ignore_fields is a set of fields, which must be ignored during data ingestion.
/// ignore_fields entries may end with '*'. In this case they match any fields with the prefix until '*'.
///
/// decolorize_fields is a set of fields, which must be cleared from ANSI color escape sequences.
/// decolorize_fields entries may end with '*'. In this case they match any fields with the prefix until '*'.
///
/// extra_fields is a set of fields, which must be added to all the logs passed to must_add().
///
/// default_msg_value is the default value to store in non-existing or empty _msg.
///
/// Return back it to the pool with [`put_log_rows`] when it is no longer needed.
pub fn get_log_rows(
    stream_fields: &[&str],
    ignore_fields: &[&str],
    decolorize_fields: &[&str],
    extra_fields: &[Field],
    default_msg_value: &str,
) -> LogRows {
    let mut lr = LOG_ROWS_POOL.lock().unwrap().pop().unwrap_or_default();

    // initialize ignore_fields
    for f in ignore_fields {
        let f = get_canonical_field_name(f);
        lr.ignore_fields.add_allow_filter(f);
    }
    for f in extra_fields {
        // Extra fields must override the existing fields for the sake of consistency and security,
        // so the client won't be able to override them.
        // Extra-field names come from query args (valid UTF-8); the lossy
        // view only feeds the filter set, the stored name stays raw.
        let field_name = String::from_utf8_lossy(&f.name);
        lr.ignore_fields
            .add_allow_filter(get_canonical_field_name(&field_name));
    }

    // initialize decolorize_fields
    for f in decolorize_fields {
        let f = get_canonical_field_name(f);
        lr.decolorize_fields.add_allow_filter(f);
    }

    // Initialize stream_fields
    for f in stream_fields {
        let f = get_canonical_field_name(f);
        if !lr.ignore_fields.match_string(f) {
            lr.stream_fields.push(f.to_string());
        }
    }

    // Initialize extra_stream_fields
    for f in extra_fields {
        let field_name = get_canonical_field_name_bytes(&f.name);
        if stream_fields.iter().any(|s| s.as_bytes() == field_name) {
            lr.extra_stream_fields.push(f.clone());
            lr.stream_fields.retain(|s| s.as_bytes() != field_name);
        }
    }

    // PORT NOTE: Go keeps a reference to the caller-provided extraFields
    // slice; owned Fields require a copy.
    lr.extra_fields = extra_fields.to_vec();
    lr.default_msg_value = default_msg_value.to_string();

    lr
}

/// Returns lr to the pool.
pub fn put_log_rows(mut lr: LogRows) {
    lr.reset();
    LOG_ROWS_POOL.lock().unwrap().push(lr);
}

// PORT NOTE: Go uses `sync.Pool`; the port uses a `Mutex<Vec<..>>` pool
// handing values out by value (the established esl-common pattern).
static LOG_ROWS_POOL: Mutex<Vec<LogRows>> = Mutex::new(Vec::new());

// ---------------------------------------------------------------------------
// LogRowsInternal — port of Go's unexported `logRows`
// ---------------------------------------------------------------------------

/// Port of Go's unexported `logRows` struct, used by datadb's rowsBuffer and
/// inmemory_part.
///
/// PORT NOTE: named `LogRowsInternal` since Go's `LogRows` and `logRows`
/// collide in Rust. Like [`LogRows`], the Go arena + shared `fieldsBuf` are
/// replaced with owned per-row `Vec<Field>`s plus `a_size_bytes` accounting.
#[derive(Default)]
pub(crate) struct LogRowsInternal {
    /// The number of bytes the Go arena would hold for the current rows.
    a_size_bytes: usize,

    /// stream_ids holds streamIDs for rows added to logRows
    pub(crate) stream_ids: Vec<StreamID>,

    /// timestamps holds timestamps for rows added to logRows
    pub(crate) timestamps: Vec<i64>,

    /// rows holds fields for rows added to logRows.
    pub(crate) rows: Vec<Vec<Field>>,
}

impl LogRowsInternal {
    pub(crate) fn reset(&mut self) {
        self.a_size_bytes = 0;

        self.stream_ids.clear();

        self.timestamps.clear();

        self.rows.clear();
    }

    /// Returns true if lr contains too much data, so it must be flushed to the storage.
    pub(crate) fn need_flush(&self) -> bool {
        self.a_size_bytes > (MAX_UNCOMPRESSED_BLOCK_SIZE / 8) * 7
    }

    pub(crate) fn must_add_rows(&mut self, src: &LogRows) {
        if src.rows.is_empty() {
            return;
        }

        for i in 0..src.rows.len() {
            self.must_add_row(src.stream_ids[i], src.timestamps[i], &src.rows[i]);
        }
    }

    pub(crate) fn must_add_row(&mut self, stream_id: StreamID, timestamp: i64, fields: &[Field]) {
        self.stream_ids.push(stream_id);
        self.timestamps.push(timestamp);

        let mut dst_fields: Vec<Field> = Vec::with_capacity(fields.len());
        for f in fields {
            let field_name = get_canonical_field_name_bytes(&f.name);

            // PORT NOTE: Go dedupes each field against
            // fieldsBuf[len(fieldsBuf)-len(fields)] (the first field of the
            // row being added) to avoid arena copies; the comparison is
            // mirrored here purely for the a_size_bytes accounting (at i=0 it
            // compares against a zero-value Field like a freshly grown Go
            // fieldsBuf).
            let (prev_name_eq, prev_value_eq) = match dst_fields.first() {
                Some(prev) => (prev.name == field_name, prev.value == f.value),
                None => (field_name.is_empty(), f.value.is_empty()),
            };
            if !prev_name_eq {
                self.a_size_bytes += field_name.len();
            }
            if !prev_value_eq {
                self.a_size_bytes += f.value.len();
            }

            dst_fields.push(Field {
                name: field_name.to_vec(),
                value: f.value.clone(),
            });
        }
        self.rows.push(dst_fields);
    }

    /// Returns the number of items in lr.
    pub(crate) fn len(&self) -> usize {
        self.stream_ids.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.stream_ids.is_empty()
    }

    /// Sorts the rows by (streamID, timestamp).
    ///
    /// PORT NOTE: replaces Go's sort.Interface impl (Len/Less/Swap) consumed
    /// via `sort.Sort(lr)` in inmemory_part.go; Go's sort.Sort is unstable,
    /// matched by sort_unstable_by on an index permutation.
    pub(crate) fn sort(&mut self) {
        let mut indexes: Vec<usize> = (0..self.len()).collect();
        indexes.sort_unstable_by(|&i, &j| {
            let a = &self.stream_ids[i];
            let b = &self.stream_ids[j];
            if !a.equal(b) {
                if a.less(b) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            } else {
                self.timestamps[i].cmp(&self.timestamps[j])
            }
        });

        self.stream_ids = indexes.iter().map(|&i| self.stream_ids[i]).collect();
        self.timestamps = indexes.iter().map(|&i| self.timestamps[i]).collect();
        let mut old_rows = std::mem::take(&mut self.rows);
        self.rows = indexes
            .iter()
            .map(|&i| std::mem::take(&mut old_rows[i]))
            .collect();
    }

    /// PORT NOTE: Go's `sortedFields` sort.Interface helper is replaced with
    /// `rows::sort_fields_by_name`.
    pub(crate) fn sort_fields_in_rows(&mut self) {
        for row in &mut self.rows {
            sort_fields_by_name(row);
        }
    }
}

/// PORT NOTE: Go's unexported `getLogRows`/`putLogRows` collide with the
/// exported `GetLogRows`/`PutLogRows` in snake_case, hence the `_internal`
/// suffix.
pub(crate) fn get_log_rows_internal() -> LogRowsInternal {
    LR_POOL.lock().unwrap().pop().unwrap_or_default()
}

pub(crate) fn put_log_rows_internal(mut lr: LogRowsInternal) {
    lr.reset();
    LR_POOL.lock().unwrap().push(lr);
}

static LR_POOL: Mutex<Vec<LogRowsInternal>> = Mutex::new(Vec::new());

// ---------------------------------------------------------------------------
// EstimatedJSONRowLen
// ---------------------------------------------------------------------------

/// Returns an approximate length of the log entry with the given fields if represented as JSON.
///
/// The calculation logic must stay in sync with block.uncompressed_size_bytes() in block.rs.
/// If you change logic here, update block.uncompressed_size_bytes() accordingly and vice versa.
pub fn estimated_json_row_len(fields: &[Field]) -> usize {
    let mut n = "{}\n".len();
    n += r#""_time":"""#.len() + RFC3339_NANO_LAYOUT_LEN;
    for f in fields {
        // EsLogs data model (https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model)
        // treats empty values as non-existing values
        if f.value.is_empty() {
            continue;
        }

        let name = get_canonical_column_name_bytes(&f.name);
        n += estimated_json_field_len(name, &f.value);
    }
    n
}

/// Returns an approximate length of the field with the given name and value if represented as JSON.
///
/// The field name must be in raw form (e.g., "" to "_msg") before passing.
pub(crate) fn estimated_json_field_len(name: &[u8], value: &[u8]) -> usize {
    r#","":"""#.len() + name.len() + value.len()
}

// ---------------------------------------------------------------------------
// InsertRow
// ---------------------------------------------------------------------------

/// Returns InsertRow from a pool.
///
/// Pass the returned row to [`put_insert_row`] when it is no longer needed, so it could be reused.
pub fn get_insert_row() -> InsertRow {
    INSERT_ROWS_POOL.lock().unwrap().pop().unwrap_or_default()
}

/// Returns r to the pool, so it could be reused via [`get_insert_row`].
pub fn put_insert_row(mut r: InsertRow) {
    r.reset();
    INSERT_ROWS_POOL.lock().unwrap().push(r);
}

static INSERT_ROWS_POOL: Mutex<Vec<InsertRow>> = Mutex::new(Vec::new());

/// InsertRow represents a row to insert into EsLogs via native protocol.
#[derive(Debug, Default)]
pub struct InsertRow {
    pub tenant_id: TenantID,
    /// PORT NOTE: Go stores the canonical stream tags in a binary Go string;
    /// the port uses `Vec<u8>`.
    pub stream_tags_canonical: Vec<u8>,
    pub timestamp: i64,
    pub fields: Vec<Field>,
}

impl InsertRow {
    /// Resets r to zero value.
    pub fn reset(&mut self) {
        self.tenant_id.reset();
        self.stream_tags_canonical.clear();
        self.timestamp = 0;

        self.fields.clear();
    }

    /// Appends marshaled r to dst.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        self.tenant_id.marshal(dst);
        encoding::marshal_bytes(dst, &self.stream_tags_canonical);
        encoding::marshal_uint64(dst, self.timestamp as u64);
        encoding::marshal_var_uint64(dst, self.fields.len() as u64);
        for field in &self.fields {
            field.marshal(dst, true);
        }
    }

    /// Appends marshaled r to dst in JSON format.
    pub fn append_json(&self, dst: &mut Vec<u8>) {
        let fields = skip_leading_fields_without_values(&self.fields);

        dst.extend_from_slice(br#"{"_time":""#);
        marshal_timestamp_rfc3339_nano_string(dst, self.timestamp);
        dst.push(b'"');

        for f in fields {
            if f.value.is_empty() {
                // Skip fields without values
                continue;
            }
            dst.push(b',');
            f.marshal_to_json(dst);
        }
        dst.push(b'}');
    }

    /// Unmarshals r from src and returns the remaining tail.
    ///
    /// PORT NOTE: Go keeps unsafe references into src (valid until src
    /// changes); the port copies the data into the owned fields. On error Go
    /// returns srcOrig as the tail; the port's `Result` carries no tail.
    pub fn unmarshal_inplace<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        let mut src = src;

        src = self
            .tenant_id
            .unmarshal(src)
            .map_err(|err| format!("cannot unmarshal tenantID: {err}"))?;

        let (stream_tags_canonical, n) = encoding::unmarshal_bytes(src);
        if n <= 0 {
            return Err("cannot unmarshal streamTagCanonical".to_string());
        }
        self.stream_tags_canonical.clear();
        self.stream_tags_canonical
            .extend_from_slice(stream_tags_canonical.unwrap_or_default());
        src = &src[n as usize..];

        if src.len() < 8 {
            return Err("cannot unmarshal timestamp".to_string());
        }
        self.timestamp = encoding::unmarshal_uint64(src) as i64;
        src = &src[8..];

        let (fields_len, n) = encoding::unmarshal_var_uint64(src);
        if n <= 0 {
            return Err("cannot unmarshal the number of fields".to_string());
        }
        if fields_len > MAX_COLUMNS_PER_BLOCK as u64 {
            return Err(format!(
                "too many fields in the log entry: {fields_len}; mustn't exceed {MAX_COLUMNS_PER_BLOCK}"
            ));
        }
        src = &src[n as usize..];

        slicesutil::set_length(&mut self.fields, fields_len as usize);
        for i in 0..self.fields.len() {
            src = self.fields[i]
                .unmarshal_inplace(src, true)
                .map_err(|err| format!("cannot unmarshal field #{i}: {err}"))?;
        }

        Ok(src)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_parser::{get_json_parser, put_json_parser};
    use crate::logfmt_parser::{get_logfmt_parser, put_logfmt_parser};
    use crate::stream_tags;

    #[derive(Default)]
    struct LogRowsTestOpts<'a> {
        rows: Vec<&'a str>,
        stream_fields: Vec<&'a str>,
        ignore_fields: Vec<&'a str>,
        decolorize_fields: Vec<&'a str>,
        extra_fields: Vec<Field>,
        default_msg_value: &'a str,
        stream_fields_len: isize,
        result_expected: Vec<&'a str>,
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    /// Shared body of the Go per-test `f(o opts)` helpers. When
    /// `use_stream_fields_len` is set, rows are added via
    /// `must_add(.., o.stream_fields_len)`; otherwise via the Go test-only
    /// `mustAdd` wrapper, i.e. `must_add(.., -1)`.
    fn check_log_rows(o: LogRowsTestOpts<'_>, use_stream_fields_len: bool) {
        let mut lr = get_log_rows(
            &o.stream_fields,
            &o.ignore_fields,
            &o.decolorize_fields,
            &o.extra_fields,
            o.default_msg_value,
        );

        let tid = TenantID {
            account_id: 123,
            project_id: 456,
        };

        let mut p = get_json_parser();
        for (i, r) in o.rows.iter().enumerate() {
            if let Err(err) = p.parse_log_message(r.as_bytes(), &[], "") {
                panic!("unexpected error when parsing {r:?}: {err}");
            }
            let timestamp = i as i64 * 1_000 + 1;
            // PORT NOTE: Go passes p.Fields directly; must_add takes &mut
            // like Go (it may clear _stream/_stream_id values), so the
            // parser's fields are copied out first.
            let mut fields = p.fields().to_vec();
            let stream_fields_len = if use_stream_fields_len {
                o.stream_fields_len
            } else {
                -1
            };
            lr.must_add(tid, timestamp, &mut fields, stream_fields_len);
        }
        put_json_parser(p);

        let mut result: Vec<String> = Vec::new();
        for i in 0..o.rows.len() {
            result.push(lr.get_row_string(i));
        }
        put_log_rows(lr);

        assert_eq!(
            result, o.result_expected,
            "unexpected result\ngot\n{result:?}\nwant\n{:?}",
            o.result_expected
        );
    }

    #[test]
    fn test_log_rows_wildcard_ignore_fields() {
        let o = LogRowsTestOpts {
            rows: vec![
                r#"{"foo.a":"bar","foo.b":"abc","z":"abc","x":"y","_msg":"aaa","foobar":"b"}"#,
                r#"{"_msg":"x"}"#,
            ],
            stream_fields: vec!["foo.a", "foo.b", "foobar"],
            ignore_fields: vec!["foo.*", "x"],
            extra_fields: vec![field("foo.a", "1234")],
            default_msg_value: "foobar",
            result_expected: vec![
                r#"{"_msg":"aaa","_stream":"{foo.a=\"1234\",foobar=\"b\"}","_time":"1970-01-01T00:00:00.000000001Z","foo.a":"1234","foobar":"b","z":"abc"}"#,
                r#"{"_msg":"x","_stream":"{foo.a=\"1234\"}","_time":"1970-01-01T00:00:00.000001001Z","foo.a":"1234"}"#,
            ],
            ..Default::default()
        };
        check_log_rows(o, false);
    }

    #[test]
    fn test_log_rows_stream_fields_override() {
        let o = LogRowsTestOpts {
            rows: vec![
                r#"{"xyz":"123","foo":"bar","_msg":"abc"}"#,
                r#"{"xyz":"bar","_msg":"abc"}"#,
                r#"{"xyz":"123","_msg":"abc"}"#,
            ],
            stream_fields_len: 1,
            default_msg_value: "foobar",
            result_expected: vec![
                r#"{"_msg":"abc","_stream":"{xyz=\"123\"}","_time":"1970-01-01T00:00:00.000000001Z","foo":"bar","xyz":"123"}"#,
                r#"{"_msg":"abc","_stream":"{xyz=\"bar\"}","_time":"1970-01-01T00:00:00.000001001Z","xyz":"bar"}"#,
                r#"{"_msg":"abc","_stream":"{xyz=\"123\"}","_time":"1970-01-01T00:00:00.000002001Z","xyz":"123"}"#,
            ],
            ..Default::default()
        };
        check_log_rows(o, true);

        let o = LogRowsTestOpts {
            rows: vec![
                r#"{"foo":"bar","_msg":"abc"}"#,
                r#"{"xyz":"bar","_msg":"abc"}"#,
                r#"{"xyz":"123","_msg":"abc"}"#,
            ],
            stream_fields_len: 0,
            ignore_fields: vec!["xyz", "qwert"],
            default_msg_value: "foobar",
            result_expected: vec![
                r#"{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000000001Z","foo":"bar"}"#,
                r#"{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000001001Z"}"#,
                r#"{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z"}"#,
            ],
            ..Default::default()
        };
        check_log_rows(o, true);
    }

    #[test]
    fn test_log_rows_default_msg_value() {
        let o = LogRowsTestOpts::default();
        check_log_rows(o, false);

        // default options
        let o = LogRowsTestOpts {
            rows: vec![r#"{"foo":"bar"}"#, r#"{}"#, r#"{"foo":"bar","a":"b"}"#],
            result_expected: vec![
                r#"{"_stream":"{}","_time":"1970-01-01T00:00:00.000000001Z","foo":"bar"}"#,
                r#"{"_stream":"{}","_time":"1970-01-01T00:00:00.000001001Z"}"#,
                r#"{"_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z","a":"b","foo":"bar"}"#,
            ],
            ..Default::default()
        };
        check_log_rows(o, false);

        // stream fields
        let o = LogRowsTestOpts {
            rows: vec![
                r#"{"x":"y","foo":"bar"}"#,
                r#"{"x":"y","foo":"bar","abc":"de"}"#,
                r#"{}"#,
            ],
            stream_fields: vec!["foo", "abc"],
            result_expected: vec![
                r#"{"_stream":"{foo=\"bar\"}","_time":"1970-01-01T00:00:00.000000001Z","foo":"bar","x":"y"}"#,
                r#"{"_stream":"{abc=\"de\",foo=\"bar\"}","_time":"1970-01-01T00:00:00.000001001Z","abc":"de","foo":"bar","x":"y"}"#,
                r#"{"_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z"}"#,
            ],
            ..Default::default()
        };
        check_log_rows(o, false);

        // ignore fields
        let o = LogRowsTestOpts {
            rows: vec![r#"{"x":"y","foo":"bar"}"#, r#"{"x":"y"}"#, r#"{}"#],
            stream_fields: vec!["foo", "abc", "x"],
            ignore_fields: vec!["foo"],
            result_expected: vec![
                r#"{"_stream":"{x=\"y\"}","_time":"1970-01-01T00:00:00.000000001Z","x":"y"}"#,
                r#"{"_stream":"{x=\"y\"}","_time":"1970-01-01T00:00:00.000001001Z","x":"y"}"#,
                r#"{"_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z"}"#,
            ],
            ..Default::default()
        };
        check_log_rows(o, false);

        // extra fields
        let o = LogRowsTestOpts {
            rows: vec![r#"{"x":"y","foo":"bar"}"#, r#"{}"#],
            stream_fields: vec!["foo", "abc", "x"],
            ignore_fields: vec!["foo"],
            extra_fields: vec![field("foo", "test"), field("abc", "1234")],
            result_expected: vec![
                r#"{"_stream":"{abc=\"1234\",foo=\"test\",x=\"y\"}","_time":"1970-01-01T00:00:00.000000001Z","abc":"1234","foo":"test","x":"y"}"#,
                r#"{"_stream":"{abc=\"1234\",foo=\"test\"}","_time":"1970-01-01T00:00:00.000001001Z","abc":"1234","foo":"test"}"#,
            ],
            ..Default::default()
        };
        check_log_rows(o, false);

        // default _msg value
        let o = LogRowsTestOpts {
            rows: vec![
                r#"{"x":"y","foo":"bar"}"#,
                r#"{"_msg":"ppp"}"#,
                r#"{"abc":"ppp"}"#,
            ],
            stream_fields: vec!["abc", "x"],
            default_msg_value: "qwert",
            result_expected: vec![
                r#"{"_msg":"qwert","_stream":"{x=\"y\"}","_time":"1970-01-01T00:00:00.000000001Z","foo":"bar","x":"y"}"#,
                r#"{"_msg":"ppp","_stream":"{}","_time":"1970-01-01T00:00:00.000001001Z"}"#,
                r#"{"_msg":"qwert","_stream":"{abc=\"ppp\"}","_time":"1970-01-01T00:00:00.000002001Z","abc":"ppp"}"#,
            ],
            ..Default::default()
        };
        check_log_rows(o, false);

        // decolorize with _msg field
        let colored = "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H";
        let rows: Vec<String> = vec![
            format!(r#"{{"_msg":"{colored}","abc":"de","bar":"baz"}}"#),
            format!(r#"{{"":"{colored}","abc":"de","bar":"baz"}}"#),
            format!(r#"{{"_msg":"abc","bar":"{colored}"}}"#),
            format!(r#"{{"_msg":"abc","bar":"baz","x":"{colored}"}}"#),
        ];
        let o = LogRowsTestOpts {
            rows: rows.iter().map(|s| s.as_str()).collect(),
            decolorize_fields: vec!["_msg", "bar"],
            result_expected: vec![
                r#"{"_msg":"fooERROR bar","_stream":"{}","_time":"1970-01-01T00:00:00.000000001Z","abc":"de","bar":"baz"}"#,
                r#"{"_msg":"fooERROR bar","_stream":"{}","_time":"1970-01-01T00:00:00.000001001Z","abc":"de","bar":"baz"}"#,
                r#"{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z","bar":"fooERROR bar"}"#,
                "{\"_msg\":\"abc\",\"_stream\":\"{}\",\"_time\":\"1970-01-01T00:00:00.000003001Z\",\"bar\":\"baz\",\"x\":\"\\u001b[mfoo\\u001b[1;31mERROR bar\\u001b[10;5H\"}",
            ],
            ..Default::default()
        };
        check_log_rows(o, false);

        // decolorize with "" field name (canonical _msg field)
        let o = LogRowsTestOpts {
            rows: rows.iter().map(|s| s.as_str()).collect(),
            decolorize_fields: vec!["", "bar"],
            result_expected: vec![
                r#"{"_msg":"fooERROR bar","_stream":"{}","_time":"1970-01-01T00:00:00.000000001Z","abc":"de","bar":"baz"}"#,
                r#"{"_msg":"fooERROR bar","_stream":"{}","_time":"1970-01-01T00:00:00.000001001Z","abc":"de","bar":"baz"}"#,
                r#"{"_msg":"abc","_stream":"{}","_time":"1970-01-01T00:00:00.000002001Z","bar":"fooERROR bar"}"#,
                "{\"_msg\":\"abc\",\"_stream\":\"{}\",\"_time\":\"1970-01-01T00:00:00.000003001Z\",\"bar\":\"baz\",\"x\":\"\\u001b[mfoo\\u001b[1;31mERROR bar\\u001b[10;5H\"}",
            ],
            ..Default::default()
        };
        check_log_rows(o, false);
    }

    #[test]
    fn test_insert_row_marshal_unmarshal() {
        let r = InsertRow {
            tenant_id: TenantID {
                account_id: 123,
                project_id: 456,
            },
            stream_tags_canonical: b"foobar".to_vec(),
            timestamp: 789,
            fields: vec![field("x", "y"), field("qwe", "rty")],
        };
        let mut data = Vec::new();
        r.marshal(&mut data);

        let mut r2 = InsertRow::default();
        let tail = match r2.unmarshal_inplace(&data) {
            Ok(tail) => tail,
            Err(err) => panic!("unexpected error when unmarshaling InsertRow: {err}"),
        };
        assert!(
            tail.is_empty(),
            "unexpected tail left after unmarshaling InsertRow; len(tail)={}; tail={tail:X?}",
            tail.len()
        );
    }

    #[test]
    fn test_insert_row_marshal_json() {
        let f = |ts: i64, fields: Vec<Field>, expected: &str| {
            let r = InsertRow {
                timestamp: ts,
                fields,
                ..Default::default()
            };
            let mut got = Vec::new();
            r.append_json(&mut got);
            let got = String::from_utf8(got).unwrap();

            assert_eq!(
                got, expected,
                "unexpected result\ngot\n{got:?}\nwant\n{expected:?}"
            );
        };

        // empty fields
        f(0, vec![], r#"{"_time":"1970-01-01T00:00:00Z"}"#);

        // non-empty fields
        f(
            123456789,
            vec![field("x", "y"), field("qwe", "rty")],
            r#"{"_time":"1970-01-01T00:00:00.123456789Z","x":"y","qwe":"rty"}"#,
        );

        // empty values
        f(
            123456789,
            vec![field("x", ""), field("qwe", "")],
            r#"{"_time":"1970-01-01T00:00:00.123456789Z"}"#,
        );

        // empty field name
        f(
            123456789,
            vec![field("", "y")],
            r#"{"_time":"1970-01-01T00:00:00.123456789Z","_msg":"y"}"#,
        );

        // escape quotes
        f(
            123456789,
            vec![field("x", "\"y\"")],
            r#"{"_time":"1970-01-01T00:00:00.123456789Z","x":"\"y\""}"#,
        );
    }

    #[test]
    fn test_verify_stream_tags_canonical_success() {
        let f = |stream_tags: &str, fields_str: &str| {
            let mut st = get_stream_tags();
            if let Err(err) = st.unmarshal_string_inplace(stream_tags) {
                panic!("cannot unmarshal stream tags: {err}");
            }
            let mut stream_tags_canonical = Vec::new();
            st.marshal_canonical(&mut stream_tags_canonical);
            put_stream_tags(st);

            let mut p = get_logfmt_parser();
            p.parse(fields_str);

            if let Err(err) = verify_stream_tags_canonical(&stream_tags_canonical, &p.fields) {
                panic!("cannot verify stream tags: {err}");
            }
            put_logfmt_parser(p);
        };

        f("{}", "");
        f("{}", "a=b c=d");
        f(r#"{a="b"}"#, "a=b");
        f(r#"{a="b"}"#, "x=y a=b q=w");
        f(r#"{a="b",c="d"}"#, "c=d x=y a=b");
        f(r#"{a="b"}"#, "a=b x=y a=b");
    }

    /// PORT NOTE: replaces Go's `st.marshalCanonicalInternal(nil)` (private
    /// to stream_tags.rs): marshals the parsed tags in their original order,
    /// without the sorting performed by marshal_canonical.
    fn marshal_canonical_no_sort(stream_tags: &str) -> Vec<u8> {
        let mut tags: Vec<Field> = Vec::new();
        stream_tags::parse_stream_fields(&mut tags, stream_tags)
            .unwrap_or_else(|err| panic!("cannot unmarshal stream tags: {err}"));
        let mut dst = Vec::new();
        encoding::marshal_var_uint64(&mut dst, tags.len() as u64);
        for tag in &tags {
            encoding::marshal_bytes(&mut dst, &tag.name);
            encoding::marshal_bytes(&mut dst, &tag.value);
        }
        dst
    }

    #[test]
    fn test_verify_stream_tags_canonical_failure() {
        let f = |stream_tags: &str, fields_str: &str| {
            let stream_tags_canonical = marshal_canonical_no_sort(stream_tags);

            let mut p = get_logfmt_parser();
            p.parse(fields_str);

            assert!(
                verify_stream_tags_canonical(&stream_tags_canonical, &p.fields).is_err(),
                "expecting non-nil error"
            );
            put_logfmt_parser(p);
        };

        // missing value
        f(r#"{a="b"}"#, "");
        f(r#"{a="b"}"#, "x=y");

        // value mismatch
        f(r#"{a="b"}"#, "a=c");

        // multiple fields with the same name
        f(r#"{a="b"}"#, "a=b x=y a=c");

        // tags are not sorted
        f(r#"{b="1",a="1"}"#, "a=1 b=1");
    }
}

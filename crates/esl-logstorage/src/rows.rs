//! Port of `lib/logstorage/rows.go`.

use std::fmt;
use std::fmt::Write as _;
use std::sync::Mutex;

use esl_common::{encoding, stringsutil};

use crate::stream_tags;

/// Field is a single field for the log entry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Field {
    /// Name is the name of the field.
    ///
    /// PORT NOTE: Go strings are arbitrary bytes; the name is stored as raw
    /// bytes so invalid UTF-8 survives ingestion/queries byte-identically.
    pub name: Vec<u8>,

    /// Value is the value of the field.
    ///
    /// PORT NOTE: Go strings are arbitrary bytes; the value is stored as raw
    /// bytes so invalid UTF-8 survives ingestion/queries byte-identically.
    pub value: Vec<u8>,
}

impl Field {
    /// Resets f for future reuse.
    pub fn reset(&mut self) {
        self.name.clear();
        self.value.clear();
    }

    // PORT NOTE: Go's Field.equal is covered by the derived PartialEq;
    // Field.String is covered by the Display impl below.

    /// Returns true if f is less than other (compare by name, then by value).
    pub fn less(&self, other: &Field) -> bool {
        if self.name != other.name {
            return self.name < other.name;
        }
        self.value < other.value
    }

    pub fn marshal(&self, dst: &mut Vec<u8>, marshal_field_name: bool) {
        if marshal_field_name {
            encoding::marshal_bytes(dst, &self.name);
        }
        encoding::marshal_bytes(dst, &self.value);
    }

    /// Unmarshals f from src and returns the remaining tail.
    ///
    /// PORT NOTE: Go's unmarshalInplace points f into src without copying;
    /// the Rust `Field` owns its strings, so the data is copied into them
    /// (reusing the existing capacity).
    pub fn unmarshal_inplace<'a>(
        &mut self,
        src: &'a [u8],
        unmarshal_field_name: bool,
    ) -> Result<&'a [u8], String> {
        let mut src = src;

        // Unmarshal field name
        if unmarshal_field_name {
            let (name, n_size) = encoding::unmarshal_bytes(src);
            if n_size <= 0 {
                return Err("cannot unmarshal field name".to_string());
            }
            src = &src[n_size as usize..];
            self.name.clear();
            self.name.extend_from_slice(name.unwrap_or_default());
        }

        // Unmarshal field value
        let (value, n_size) = encoding::unmarshal_bytes(src);
        if n_size <= 0 {
            return Err("cannot unmarshal field value".to_string());
        }
        src = &src[n_size as usize..];
        self.value.clear();
        self.value.extend_from_slice(value.unwrap_or_default());

        Ok(src)
    }

    pub fn marshal_to_json(&self, dst: &mut Vec<u8>) {
        let name: &[u8] = if self.name.is_empty() {
            b"_msg"
        } else {
            &self.name
        };
        stringsutil::json_string_bytes_append(dst, name);
        dst.push(b':');
        stringsutil::json_string_bytes_append(dst, &self.value);
    }

    pub fn marshal_to_logfmt(&self, dst: &mut Vec<u8>) {
        let name: &[u8] = if self.name.is_empty() {
            b"_msg"
        } else {
            &self.name
        };
        dst.extend_from_slice(name);
        dst.push(b'=');
        if needs_logfmt_quoting(&self.value) {
            stringsutil::json_string_bytes_append(dst, &self.value);
        } else {
            dst.extend_from_slice(&self.value);
        }
    }

    pub fn marshal_to_stream_tag(&self, dst: &mut Vec<u8>) {
        dst.extend_from_slice(&self.name);
        dst.push(b'=');
        append_quote(dst, &self.value);
    }

    pub fn indexdb_marshal(&self, dst: &mut Vec<u8>) {
        stream_tags::marshal_tag_value(dst, &self.name);
        stream_tags::marshal_tag_value(dst, &self.value);
    }

    /// PORT NOTE: Go appends the decoded name/value into a caller-provided
    /// buf and points f into it; the Rust `Field` owns its strings, so the
    /// buf parameter is dropped.
    pub fn indexdb_unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        let mut buf = Vec::new();

        let src = stream_tags::unmarshal_tag_value(&mut buf, src)
            .map_err(|err| format!("cannot unmarshal key: {err}"))?;
        self.name.clear();
        self.name.extend_from_slice(&buf);

        buf.clear();
        let src = stream_tags::unmarshal_tag_value(&mut buf, src)
            .map_err(|err| format!("cannot unmarshal value: {err}"))?;
        self.value.clear();
        self.value.extend_from_slice(&buf);

        Ok(src)
    }
}

impl fmt::Display for Field {
    /// Returns string representation of f.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut x = Vec::new();
        self.marshal_to_json(&mut x);
        // Display-only: the value may hold arbitrary bytes, so a lossy view
        // is used instead of a panicking unsafe-string view.
        f.write_str(&String::from_utf8_lossy(&x))
    }
}

pub fn get_field_value_by_name<'a>(fields: &'a [Field], name: &str) -> &'a [u8] {
    for f in fields {
        if f.name == name.as_bytes() {
            return &f.value;
        }
    }
    b""
}

fn needs_logfmt_quoting(s: &[u8]) -> bool {
    // A byte needs quoting iff it is an ASCII control/space, '"' or '\\';
    // bytes >= 0x80 never need quoting, so the byte scan matches the previous
    // char-based scan for valid UTF-8 and extends it to arbitrary bytes.
    s.iter().any(|&b| is_logfmt_special_byte(b))
}

fn is_logfmt_special_byte(b: u8) -> bool {
    b <= 0x20 || b == b'"' || b == b'\\'
}

/// Appends the Go `strconv.Quote` representation of s to dst.
///
/// PORT NOTE: replaces `strconv.AppendQuote`. Like esl-common's `go_quote`,
/// printable non-ASCII characters are kept as-is instead of `\u`-escaping
/// non-printable runes the way Go does; bytes >= 0x80 (including invalid
/// UTF-8, which Go would escape as `\xNN`) are passed through raw — stream
/// tag values are validated printable text before reaching here.
fn append_quote(dst: &mut Vec<u8>, s: &[u8]) {
    dst.push(b'"');
    for &b in s {
        match b {
            b'"' => dst.extend_from_slice(b"\\\""),
            b'\\' => dst.extend_from_slice(b"\\\\"),
            0x07 => dst.extend_from_slice(b"\\a"),
            0x08 => dst.extend_from_slice(b"\\b"),
            0x0c => dst.extend_from_slice(b"\\f"),
            b'\n' => dst.extend_from_slice(b"\\n"),
            b'\r' => dst.extend_from_slice(b"\\r"),
            b'\t' => dst.extend_from_slice(b"\\t"),
            0x0b => dst.extend_from_slice(b"\\v"),
            b if b < 0x20 || b == 0x7f => {
                let mut buf = String::new();
                write!(buf, "\\x{b:02x}").unwrap();
                dst.extend_from_slice(buf.as_bytes());
            }
            b => dst.push(b),
        }
    }
    dst.push(b'"');
}

/// Renames the first non-empty field with the name from old_names list to
/// new_name in fields.
pub fn rename_field(fields: &mut [Field], old_names: &[&str], new_name: &str) {
    if old_names.is_empty() {
        // Nothing to rename
        return;
    }
    for n in old_names {
        for f in fields.iter_mut() {
            if f.name == n.as_bytes() && !f.value.is_empty() {
                f.name.clear();
                f.name.extend_from_slice(new_name.as_bytes());
                return;
            }
        }
    }
}

/// Appends JSON-marshaled fields to dst.
pub fn marshal_fields_to_json(dst: &mut Vec<u8>, fields: &[Field]) {
    let mut fields = skip_leading_fields_without_values(fields);
    dst.push(b'{');
    if !fields.is_empty() {
        fields[0].marshal_to_json(dst);
        fields = &fields[1..];
        for f in fields {
            if f.value.is_empty() {
                // Skip fields without values
                continue;
            }
            dst.push(b',');
            f.marshal_to_json(dst);
        }
    }
    dst.push(b'}');
}

/// Appends logfmt-marshaled fields to dst.
pub fn marshal_fields_to_logfmt(dst: &mut Vec<u8>, fields: &[Field]) {
    if fields.is_empty() {
        return;
    }
    fields[0].marshal_to_logfmt(dst);
    for f in &fields[1..] {
        dst.push(b' ');
        f.marshal_to_logfmt(dst);
    }
}

/// Skips leading fields without values.
pub fn skip_leading_fields_without_values(fields: &[Field]) -> &[Field] {
    let mut i = 0;
    while i < fields.len() && fields[i].value.is_empty() {
        i += 1;
    }
    &fields[i..]
}

/// PORT NOTE: Go's appendFields copies the field strings into an arena so the
/// result doesn't reference external buffers; owned-String `Field`s give the
/// same guarantee with a plain clone, so the arena parameter is dropped.
pub fn append_fields(dst: &mut Vec<Field>, src: &[Field]) {
    dst.extend_from_slice(src);
}

/// rows is an aux structure used during rows merge.
///
/// PORT NOTE: Go packs all the fields into a shared fieldsBuf and slices the
/// rows out of it to minimize allocations; the Rust port stores each row as
/// its own Vec<Field>, so the fieldsBuf field is dropped.
#[derive(Debug, Default)]
pub struct Rows {
    pub timestamps: Vec<i64>,

    pub rows: Vec<Vec<Field>>,
}

impl Rows {
    /// Resets rs.
    pub fn reset(&mut self) {
        self.timestamps.clear();
        self.rows.clear();
    }

    pub fn has_non_empty_rows(&self) -> bool {
        self.rows.iter().any(|fields| !fields.is_empty())
    }

    /// Appends rows with the given timestamps to rs.
    pub fn append_rows(&mut self, timestamps: &[i64], rows: &[Vec<Field>]) {
        self.timestamps.extend_from_slice(timestamps);
        self.rows.extend_from_slice(rows);
    }

    /// Merges the args and appends them to rs.
    pub fn merge_rows(
        &mut self,
        timestamps_a: &[i64],
        timestamps_b: &[i64],
        fields_a: &[Vec<Field>],
        fields_b: &[Vec<Field>],
    ) {
        let mut timestamps_a = timestamps_a;
        let mut timestamps_b = timestamps_b;
        let mut fields_a = fields_a;
        let mut fields_b = fields_b;

        while !timestamps_a.is_empty() && !timestamps_b.is_empty() {
            let mut i = 0;
            let min_timestamp = timestamps_b[0];
            while i < timestamps_a.len() && timestamps_a[i] <= min_timestamp {
                i += 1;
            }
            self.append_rows(&timestamps_a[..i], &fields_a[..i]);
            fields_a = &fields_a[i..];
            timestamps_a = &timestamps_a[i..];

            std::mem::swap(&mut fields_a, &mut fields_b);
            std::mem::swap(&mut timestamps_a, &mut timestamps_b);
        }
        if timestamps_a.is_empty() {
            self.append_rows(timestamps_b, fields_b);
        } else {
            self.append_rows(timestamps_a, fields_a);
        }
    }

    /// Drops the rows at `[offset..]` matching `drop_filter` (Go
    /// `rows.skipRowsByDropFilter`). `stream`/`stream_id` carry the `_stream`
    /// and `_stream_id` values shared by all the rows.
    ///
    /// PORT NOTE: Go compacts the shared backing slices in place and nils the
    /// tail for the GC; the port compacts `timestamps`/`rows` in place with a
    /// write cursor and truncates. Go's pooled `bbPool` buffer for the
    /// rendered `_time` value is a plain local `Vec<u8>` here.
    pub(crate) fn skip_rows_by_drop_filter(
        &mut self,
        drop_filter: &crate::block_search::PartitionSearchOptions<'_>,
        drop_filter_fields: &crate::prefix_filter::Filter,
        offset: usize,
        stream: &str,
        stream_id: &str,
    ) {
        let mut tmp_fields = get_fields();

        add_field_if_needed(
            &mut tmp_fields.fields,
            drop_filter_fields,
            b"_stream",
            stream.as_bytes(),
        );
        add_field_if_needed(
            &mut tmp_fields.fields,
            drop_filter_fields,
            b"_stream_id",
            stream_id.as_bytes(),
        );
        let tmp_fields_base_len = tmp_fields.fields.len();

        let needs_time = drop_filter_fields.match_string("_time");
        let mut bb: Vec<u8> = Vec::new();
        let mut w = offset;
        for i in 0..self.timestamps.len() - offset {
            let src_timestamp = self.timestamps[offset + i];

            if src_timestamp < drop_filter.min_timestamp
                || src_timestamp > drop_filter.max_timestamp
            {
                // Fast path - keep row outsize the dropFilter time range
                self.timestamps[w] = src_timestamp;
                self.rows.swap(w, offset + i);
                w += 1;
                continue;
            }

            if needs_time {
                bb.clear();
                crate::values_encoder::marshal_timestamp_rfc3339_nano_string(
                    &mut bb,
                    src_timestamp,
                );
                tmp_fields.add("_time", &bb);
            }

            for f in &self.rows[offset + i] {
                add_field_if_needed(
                    &mut tmp_fields.fields,
                    drop_filter_fields,
                    &f.name,
                    &f.value,
                );
            }

            if !drop_filter.filter.match_row(&tmp_fields.fields) {
                self.timestamps[w] = src_timestamp;
                self.rows.swap(w, offset + i);
                w += 1;
            } else if i == 0 {
                // The first row with the minimum timestamp is deleted.
                // Replace it with an empty row with the original timestamp in order to keep valid the assumptions
                // that blocks for the same log stream are sorted by their first (minimum) timestamps.
                // Violating these assumptions leads to data loss during background merge
                // when obtaining the next block to merge via blockStreamReadersHeap.Less.
                //
                // It is safe to use an empty row here, since it is treated as non-existing row
                // during filtering because of VictoraLogs data model - https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model
                //
                // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/825
                self.timestamps[w] = src_timestamp;
                self.rows[w] = Vec::new();
                w += 1;
            }

            tmp_fields.fields.truncate(tmp_fields_base_len);
        }

        self.timestamps.truncate(w);
        self.rows.truncate(w);

        put_fields(tmp_fields);
    }
}

/// Appends `(name, value)` to dst when the canonicalized name matches pf
/// (Go `addFieldIfNeeded`).
fn add_field_if_needed(
    dst: &mut Vec<Field>,
    pf: &crate::prefix_filter::Filter,
    name: &[u8],
    value: &[u8],
) {
    let name = crate::log_rows::get_canonical_column_name_bytes(name);
    if pf.match_string_bytes(name) {
        dst.push(Field {
            name: name.to_vec(),
            value: value.to_vec(),
        });
    }
}

pub fn sort_fields_by_name(fields: &mut [Field]) {
    // PORT NOTE: Go uses the unstable sort.Slice; a stable sort is a valid
    // refinement of the unspecified ordering of equal names.
    fields.sort_by(|a, b| a.name.cmp(&b.name));
}

/// Fields holds a slice of Field items.
#[derive(Debug, Default)]
pub struct Fields {
    /// fields is a slice of fields.
    pub fields: Vec<Field>,
}

impl Fields {
    /// Resets f.
    pub fn reset(&mut self) {
        self.fields.clear();
    }

    /// Clears f.fields up to its capacity.
    ///
    /// PORT NOTE: Go clears the slice up to cap() so the underlying byte
    /// slices can be freed by GC; a Rust Vec has no live elements beyond its
    /// length, so this is equivalent to reset().
    pub fn clear_up_to_capacity(&mut self) {
        self.fields.clear();
    }

    /// Adds (name, value) field to f.
    pub fn add(&mut self, name: &str, value: impl AsRef<[u8]>) {
        self.fields.push(Field {
            name: name.as_bytes().to_vec(),
            value: value.as_ref().to_vec(),
        });
    }
}

/// Returns an empty Fields from the pool.
///
/// Pass the returned Fields to put_fields() when it is no longer needed.
pub fn get_fields() -> Fields {
    FIELDS_POOL.lock().unwrap().pop().unwrap_or_default()
}

/// Returns f to the pool.
///
/// f cannot be used after returning to the pool. Use get_fields() for
/// obtaining an empty Fields from the pool.
pub fn put_fields(mut f: Fields) {
    f.reset();
    FIELDS_POOL.lock().unwrap().push(f);
}

static FIELDS_POOL: Mutex<Vec<Fields>> = Mutex::new(Vec::new());

#[cfg(test)]
mod tests {
    use super::*;
    use esl_common::bytesutil;

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    // Helper from block_timing_test.go, needed by test_rows_append_rows.
    fn new_test_rows(rows_count: usize, fields_per_row: usize) -> (Vec<i64>, Vec<Vec<Field>>) {
        let mut timestamps = Vec::with_capacity(rows_count);
        let mut rows = Vec::with_capacity(rows_count);
        for i in 0..rows_count {
            timestamps.push(i as i64 * 1_000_000_000);
            let mut fields = Vec::with_capacity(fields_per_row);
            for j in 0..fields_per_row {
                fields.push(field(&format!("field_{j}"), &format!("value_{i}_{j}")));
            }
            rows.push(fields);
        }
        (timestamps, rows)
    }

    #[test]
    fn test_rename_field() {
        fn f(fields: &mut [Field], old_names: &[&str], result_expected: &str) {
            rename_field(fields, old_names, "_msg");
            let mut result = Vec::new();
            marshal_fields_to_json(&mut result, fields);
            assert_eq!(
                bytesutil::to_unsafe_string(&result),
                result_expected,
                "unexpected result"
            );
        }

        f(
            &mut [field("message", "test"), field("field.message", "foo")],
            &["field.message", "message"],
            r#"{"message":"test","_msg":"foo"}"#,
        );
    }

    #[test]
    fn test_marshal_fields_to_json() {
        fn f(fields: &[Field], result_expected: &str) {
            let mut result = Vec::new();
            marshal_fields_to_json(&mut result, fields);
            assert_eq!(
                bytesutil::to_unsafe_string(&result),
                result_expected,
                "unexpected result"
            );
        }

        f(&[], "{}");

        f(&[field("foo", "bar")], r#"{"foo":"bar"}"#);

        f(
            &[
                field("foo\nbar", "  \u{1b}[32m "),
                field("  \u{1b}[11m ", "АБв"),
            ],
            "{\"foo\\nbar\":\"  \\u001b[32m \",\"  \\u001b[11m \":\"АБв\"}",
        );
    }

    #[test]
    fn test_marshal_fields_to_json_invalid_utf8_passthrough() {
        // Values are raw bytes (Go strings are arbitrary bytes): invalid
        // UTF-8 must pass through JSON marshaling byte-identically.
        let fields = [Field {
            name: b"foo".to_vec(),
            value: b"a\xff\xfeb".to_vec(),
        }];
        let mut result = Vec::new();
        marshal_fields_to_json(&mut result, &fields);
        assert_eq!(result, b"{\"foo\":\"a\xff\xfeb\"}".to_vec());
    }

    #[test]
    fn test_field_marshal_unmarshal_invalid_utf8_roundtrip() {
        let f = Field {
            name: b"foo".to_vec(),
            value: b"a\xff\xfeb".to_vec(),
        };
        let mut data = Vec::new();
        f.marshal(&mut data, true);

        let mut f2 = Field::default();
        let tail = f2.unmarshal_inplace(&data, true).unwrap();
        assert!(tail.is_empty(), "unexpected tail after unmarshal");
        assert_eq!(
            f2, f,
            "invalid UTF-8 value must round-trip byte-identically"
        );
    }

    #[test]
    fn test_marshal_fields_to_logfmt() {
        fn f(fields: &[Field], result_expected: &str) {
            let mut result = Vec::new();
            marshal_fields_to_logfmt(&mut result, fields);
            assert_eq!(
                bytesutil::to_unsafe_string(&result),
                result_expected,
                "unexpected result"
            );
        }

        f(&[], "");

        f(&[field("foo", "bar")], "foo=bar");

        f(
            &[field("foo", "  \u{1b}[32m "), field("bar", "АБв")],
            "foo=\"  \\u001b[32m \" bar=АБв",
        );
    }

    // PORT NOTE: TestGetRowsSizeBytes is not ported here because
    // uncompressedRowsSizeBytes lives in log_rows.go; port it with log_rows.rs.

    #[test]
    fn test_rows_append_rows() {
        let mut rs = Rows::default();

        let timestamps = vec![1i64];
        let rows = vec![vec![field("foo", "bar")]];
        rs.append_rows(&timestamps, &rows);
        assert_eq!(
            rs.timestamps.len(),
            1,
            "unexpected number of row items; got {}; want 1",
            rs.timestamps.len()
        );
        rs.append_rows(&timestamps, &rows);
        assert_eq!(
            rs.timestamps.len(),
            2,
            "unexpected number of row items; got {}; want 2",
            rs.timestamps.len()
        );
        for i in 0..rs.timestamps.len() {
            assert_eq!(
                rs.timestamps[i], timestamps[0],
                "unexpected timestamps copied"
            );
            assert_eq!(rs.rows[i], rows[0], "unexpected fields copied");
        }

        // append multiple log entries
        let (timestamps, rows) = new_test_rows(100, 4);
        rs.append_rows(&timestamps, &rows);
        assert_eq!(
            rs.timestamps.len(),
            102,
            "unexpected number of row items; got {}; want 102",
            rs.timestamps.len()
        );
        for i in 0..timestamps.len() {
            assert_eq!(
                rs.timestamps[i + 2],
                timestamps[i],
                "unexpected timestamps copied"
            );
            assert_eq!(rs.rows[i + 2], rows[i], "unexpected log entry copied");
        }

        // reset rows
        rs.reset();
        assert_eq!(
            rs.timestamps.len(),
            0,
            "unexpected non-zero number of row items after reset: {}",
            rs.timestamps.len()
        );
    }

    #[test]
    fn test_merge_rows() {
        #[allow(clippy::too_many_arguments)]
        fn f(
            timestamps_a: &[i64],
            timestamps_b: &[i64],
            fields_a: &[Vec<Field>],
            fields_b: &[Vec<Field>],
            timestamps_expected: &[i64],
            rows_expected: &[Vec<Field>],
        ) {
            let mut rs = Rows::default();
            rs.merge_rows(timestamps_a, timestamps_b, fields_a, fields_b);
            assert_eq!(
                rs.timestamps, timestamps_expected,
                "unexpected timestamps after merge"
            );
            assert_eq!(rs.rows, rows_expected, "unexpected rows after merge");

            // check that the result doesn't change when merging in reverse order
            rs.reset();
            rs.merge_rows(timestamps_b, timestamps_a, fields_b, fields_a);
            assert_eq!(
                rs.timestamps, timestamps_expected,
                "unexpected timestamps after reverse merge"
            );
            assert_eq!(
                rs.rows, rows_expected,
                "unexpected rows after reverse merge"
            );
        }

        f(&[], &[], &[], &[], &[], &[]);

        // merge single entry with zero entries
        let timestamps_a = vec![123i64];
        let timestamps_b: Vec<i64> = vec![];
        let fields_a = vec![vec![field("foo", "bar")]];
        let fields_b: Vec<Vec<Field>> = vec![];
        let result_timestamps = vec![123i64];
        let result_fields = vec![vec![field("foo", "bar")]];
        f(
            &timestamps_a,
            &timestamps_b,
            &fields_a,
            &fields_b,
            &result_timestamps,
            &result_fields,
        );

        // merge two single entries
        let timestamps_a = vec![123i64];
        let timestamps_b = vec![43323i64];
        let fields_a = vec![vec![field("foo", "bar")]];
        let fields_b = vec![vec![field("asdfds", "asdfsa")]];
        let result_timestamps = vec![123i64, 43323];
        let result_fields = vec![vec![field("foo", "bar")], vec![field("asdfds", "asdfsa")]];
        f(
            &timestamps_a,
            &timestamps_b,
            &fields_a,
            &fields_b,
            &result_timestamps,
            &result_fields,
        );

        // merge identical entries
        let timestamps_a = vec![123i64, 456];
        let timestamps_b = vec![123i64, 456];
        let fields_a = vec![vec![field("foo", "bar")], vec![field("foo", "baz")]];
        let fields_b = vec![vec![field("foo", "bar")], vec![field("foo", "baz")]];
        let result_timestamps = vec![123i64, 123, 456, 456];
        let result_fields = vec![
            vec![field("foo", "bar")],
            vec![field("foo", "bar")],
            vec![field("foo", "baz")],
            vec![field("foo", "baz")],
        ];
        f(
            &timestamps_a,
            &timestamps_b,
            &fields_a,
            &fields_b,
            &result_timestamps,
            &result_fields,
        );

        // merge interleaved entries
        let timestamps_a = vec![12i64, 13432];
        let timestamps_b = vec![3i64, 43323];
        let fields_a = vec![vec![field("foo", "bar")], vec![field("xfoo", "xbar")]];
        let fields_b = vec![vec![field("asd", "assa")], vec![field("asdfds", "asdfsa")]];
        let result_timestamps = vec![3i64, 12, 13432, 43323];
        let result_fields = vec![
            vec![field("asd", "assa")],
            vec![field("foo", "bar")],
            vec![field("xfoo", "xbar")],
            vec![field("asdfds", "asdfsa")],
        ];
        f(
            &timestamps_a,
            &timestamps_b,
            &fields_a,
            &fields_b,
            &result_timestamps,
            &result_fields,
        );
    }
}

//! Port of `app/eslselect/logsql/csv.go` plus the csv row writer from
//! `logsql.go` (`appendCSVRow`), used by the `format=csv` branch of
//! `/select/logsql/query`.

use esl_logstorage::storage_search::BlockColumn;

/// Appends `fields` as a single RFC 4180 csv line to `dst`
/// (Go `appendCSVLine`).
pub fn append_csv_line(dst: &mut Vec<u8>, fields: &[Vec<u8>]) {
    for (i, field) in fields.iter().enumerate() {
        append_csv_field(dst, field);
        if i != fields.len() - 1 {
            dst.push(b',');
        }
    }
    dst.push(b'\n');
}

/// Appends `s` as a csv field to `dst`, quoting it when it contains `"`, `,`
/// or `\n` (Go `appendCSVField`).
///
/// PORT NOTE: Go takes a `string`; the port takes `&[u8]` since
/// [`BlockColumn`] values are byte-oriented. The quoting logic is byte-wise
/// identical.
pub fn append_csv_field(dst: &mut Vec<u8>, s: &[u8]) {
    let n = s.iter().position(|&b| matches!(b, b'"' | b',' | b'\n'));
    let Some(n) = n else {
        // fast path - nothing to quote
        dst.extend_from_slice(s);
        return;
    };

    // slow path - the s must be quoted
    dst.push(b'"');
    dst.extend_from_slice(&s[..n]);
    let mut s = &s[n..];

    loop {
        let Some(n) = s.iter().position(|&b| b == b'"') else {
            dst.extend_from_slice(s);
            break;
        };

        dst.extend_from_slice(&s[..n]);
        dst.extend_from_slice(b"\"\"");
        s = &s[n + 1..];
    }

    dst.push(b'"');
}

/// Appends the csv-encoded row `row_idx` of `columns` plus a trailing newline
/// to `dst` (Go `appendCSVRow` in `logsql.go`).
pub fn append_csv_row(dst: &mut Vec<u8>, columns: &[BlockColumn], row_idx: usize) {
    for (i, c) in columns.iter().enumerate() {
        let v = &c.values[row_idx];
        append_csv_field(dst, v);
        if i + 1 < columns.len() {
            dst.push(b',');
        }
    }
    dst.push(b'\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of Go `TestAppendCSVField` (csv_test.go).
    #[test]
    fn test_append_csv_field() {
        fn f(s: &str, result_expected: &str) {
            let mut result = Vec::new();
            append_csv_field(&mut result, s.as_bytes());
            assert_eq!(
                String::from_utf8(result).unwrap(),
                result_expected,
                "unexpected result for {s:?}"
            );
        }

        f("", "");
        f(" ", " ");
        f("\"", "\"\"\"\"");
        f(",", "\",\"");
        f("\n", "\"\n\"");
        f("\\\"", "\"\\\"\"\"");
        f("\\n", "\\n");
        f("\r", "\r");
        f("\t", "\t");

        f(" foo, bar\" baz", "\" foo, bar\"\" baz\"");
        f("foo bar\" \"baz", "\"foo bar\"\" \"\"baz\""); // test multiple quotes
    }

    /// Port of Go `TestAppendCSVLine` (csv_test.go).
    #[test]
    fn test_append_csv_line() {
        fn f(fields: &[&str], result_expected: &str) {
            let fields: Vec<Vec<u8>> = fields.iter().map(|s| s.as_bytes().to_vec()).collect();
            let mut result = Vec::new();
            append_csv_line(&mut result, &fields);
            assert_eq!(
                String::from_utf8(result).unwrap(),
                result_expected,
                "unexpected result for {fields:?}"
            );
        }

        f(&[], "\n");
        f(&["foo"], "foo\n");
        f(&["a", "", "b"], "a,,b\n");
        f(
            &["a,b", "\"cd\"", "a\nb,c\"d"],
            "\"a,b\",\"\"\"cd\"\"\",\"a\nb,c\"\"d\"\n",
        );
    }

    #[test]
    fn test_append_csv_row() {
        let columns = vec![
            BlockColumn {
                name: b"_msg".to_vec(),
                values: vec![b"plain".to_vec(), b"needs,quoting".to_vec()],
            },
            BlockColumn {
                name: b"host".to_vec(),
                values: vec![b"node-1".to_vec(), b"no\"de".to_vec()],
            },
        ];

        let mut dst = Vec::new();
        append_csv_row(&mut dst, &columns, 0);
        append_csv_row(&mut dst, &columns, 1);
        assert_eq!(
            String::from_utf8(dst).unwrap(),
            "plain,node-1\n\"needs,quoting\",\"no\"\"de\"\n"
        );
    }
}

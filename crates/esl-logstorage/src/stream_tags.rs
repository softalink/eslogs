//! Port of `lib/logstorage/stream_tags.go`.

use std::fmt;

use esl_common::{bytesutil, encoding};

use crate::rows::{Field, marshal_fields_to_json};

/// Returns a StreamTags from pool.
pub fn get_stream_tags() -> StreamTags {
    STREAM_TAGS_POOL
        .with(|p| p.borrow_mut().pop())
        .unwrap_or_default()
}

/// Returns st to the pool.
pub fn put_stream_tags(mut st: StreamTags) {
    st.reset();
    STREAM_TAGS_POOL.with(|pool| {
        let mut v = pool.borrow_mut();
        if v.len() < STREAM_TAGS_POOL_CAP {
            v.push(st);
        }
    });
}

// thread_local free-list (Go's sync.Pool is per-P thread-local too); avoids the
// global-lock contention a Mutex<Vec<..>> pool causes on the concurrent ingest
// path. Capped so idle threads don't retain unbounded StreamTags.
const STREAM_TAGS_POOL_CAP: usize = 16;
thread_local! {
    static STREAM_TAGS_POOL: std::cell::RefCell<Vec<StreamTags>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// StreamTags contains stream tags.
#[derive(Debug, Default)]
pub struct StreamTags {
    /// tags contains added tags.
    tags: Vec<Field>,
}

impl StreamTags {
    /// Resets st for reuse.
    pub fn reset(&mut self) {
        // PORT NOTE: Go clears the slice items so referenced external buffers
        // can be collected by GC; the owned-String Fields are simply dropped.
        self.tags.clear();
    }

    pub fn verify_canonical_field_values(&self, fields: &[Field]) -> Result<(), String> {
        // Verify that the unmarshaled stream tags match the corresponding fields' values.
        // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/38

        let mut prev_tag_name = "";
        for tag in &self.tags {
            let tag_name = tag.name.as_str();

            if check_stream_field_name(tag_name).is_err() {
                return Err(format!(
                    "invalid stream tag name: {tag_name}; streamTags: {self}"
                ));
            }

            if tag_name <= prev_tag_name {
                return Err(format!(
                    "stream tag names must be sorted; got {tag_name:?} after {prev_tag_name:?}; streamTags: {self}"
                ));
            }
            prev_tag_name = tag_name;

            let tag_value: &[u8] = &tag.value;
            let mut found = false;
            for f in fields {
                if f.name != tag_name {
                    continue;
                }
                if f.value != tag_value {
                    let mut line = Vec::new();
                    marshal_fields_to_json(&mut line, fields);
                    return Err(format!(
                        "unexpected value for the stream tag {tag_name:?}; got {:?}; want {:?}; streamTags: {self}; fields: {}",
                        String::from_utf8_lossy(&f.value),
                        String::from_utf8_lossy(tag_value),
                        bytesutil::to_unsafe_string(&line)
                    ));
                }
                found = true;
            }
            if !found {
                let mut line = Vec::new();
                marshal_fields_to_json(&mut line, fields);
                return Err(format!(
                    "cannot find value for the stream tag {tag_name:?} in fields; want {:?}; streamTags: {self}; fields: {}",
                    String::from_utf8_lossy(tag_value),
                    bytesutil::to_unsafe_string(&line)
                ));
            }
        }
        Ok(())
    }

    pub fn marshal_string(&self, dst: &mut Vec<u8>) {
        dst.push(b'{');

        let mut tags = self.tags.as_slice();
        if !tags.is_empty() {
            tags[0].marshal_to_stream_tag(dst);
            tags = &tags[1..];
            for tag in tags {
                dst.push(b',');
                tag.marshal_to_stream_tag(dst);
            }
        }

        dst.push(b'}');
    }

    /// Unmarshals st from the string representation stored at s received via
    /// marshal_string().
    ///
    /// PORT NOTE: Go's unmarshalStringInplace points st into s without
    /// copying; the Rust `Field` owns its strings, so the data is copied.
    pub fn unmarshal_string_inplace(&mut self, s: &str) -> Result<(), String> {
        self.reset();

        parse_stream_fields(&mut self.tags, s)
    }

    /// Adds (name:value) tag to st.
    pub fn add(&mut self, name: &str, value: impl AsRef<[u8]>) {
        let value = value.as_ref();
        if value.is_empty() {
            return;
        }

        let name = if name.is_empty() { "_msg" } else { name };

        self.tags.push(Field {
            name: name.to_string(),
            value: value.to_vec(),
        });
    }

    /// Marshals st in a canonical way.
    pub fn marshal_canonical(&mut self, dst: &mut Vec<u8>) {
        self.tags.sort_by(|a, b| {
            (a.name.as_str(), a.value.as_slice()).cmp(&(b.name.as_str(), b.value.as_slice()))
        });
        self.marshal_canonical_internal(dst);
    }

    fn marshal_canonical_internal(&self, dst: &mut Vec<u8>) {
        let tags = &self.tags;
        encoding::marshal_var_uint64(dst, tags.len() as u64);
        for tag in tags {
            encoding::marshal_bytes(dst, tag.name.as_bytes());
            encoding::marshal_bytes(dst, &tag.value);
        }
    }

    /// Unmarshals st from src marshaled with marshal_canonical and returns the
    /// remaining tail.
    ///
    /// PORT NOTE: Go's UnmarshalCanonicalInplace points st into src without
    /// copying; the Rust `Field` owns its strings, so the data is copied.
    pub fn unmarshal_canonical_inplace<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        self.reset();

        let mut src = src;

        let (n, n_size) = encoding::unmarshal_var_uint64(src);
        if n_size <= 0 {
            return Err("cannot unmarshal tags len".to_string());
        }
        src = &src[n_size as usize..];
        for _ in 0..n {
            let (name, n_size) = encoding::unmarshal_bytes(src);
            if n_size <= 0 {
                return Err("cannot unmarshal tag name".to_string());
            }
            src = &src[n_size as usize..];

            let (value, n_size) = encoding::unmarshal_bytes(src);
            if n_size <= 0 {
                return Err("cannot unmarshal tag value".to_string());
            }
            src = &src[n_size as usize..];

            let s_name = bytesutil::to_unsafe_string(name.unwrap_or_default());
            self.add(s_name, value.unwrap_or_default());
        }

        if !self.is_sorted() {
            return Err(format!(
                "stream tags must be sorted in alphabetical order; got unsorted: {self}"
            ));
        }

        Ok(src)
    }

    /// Returns the number of tags in st.
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// Returns true if st contains no tags.
    ///
    /// PORT NOTE: not present upstream; added to satisfy the
    /// `len_without_is_empty` lint.
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    // PORT NOTE: Go's sort.Interface methods (Len/Less/Swap) are replaced by
    // sort_by in marshal_canonical and the is_sorted check below.
    fn is_sorted(&self) -> bool {
        let tags = &self.tags;
        for i in 1..tags.len() {
            if tags[i].less(&tags[i - 1]) {
                return false;
            }
        }
        true
    }
}

impl fmt::Display for StreamTags {
    /// Returns string representation of st.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut b = Vec::new();
        self.marshal_string(&mut b);
        f.write_str(bytesutil::to_unsafe_string(&b))
    }
}

/// PORT NOTE: Go passes the canonical representation around as a binary Go
/// string; the Rust port uses `&[u8]`.
pub fn get_stream_tags_string(stream_tags_canonical: &[u8]) -> String {
    let mut st = get_stream_tags();
    must_unmarshal_stream_tags_inplace(&mut st, stream_tags_canonical);
    let s = st.to_string();
    put_stream_tags(st);

    s
}

pub fn must_unmarshal_stream_tags_inplace(dst: &mut StreamTags, stream_tags_canonical: &[u8]) {
    let tail = match dst.unmarshal_canonical_inplace(stream_tags_canonical) {
        Ok(tail) => tail,
        Err(err) => {
            esl_common::panicf!("FATAL: cannot unmarshal StreamTags: {}", err);
            unreachable!()
        }
    };
    if !tail.is_empty() {
        esl_common::panicf!(
            "FATAL: unexpected tail left after unmarshaling StreamTags; len(tail)={}; tail={:?}",
            tail.len(),
            tail
        );
    }
}

const ESCAPE_CHAR: u8 = 0;
const TAG_SEPARATOR_CHAR: u8 = 1;
const KV_SEPARATOR_CHAR: u8 = 2;

pub fn marshal_tag_value(dst: &mut Vec<u8>, src: &[u8]) {
    let b = src;
    let n1 = b.iter().position(|&c| c == ESCAPE_CHAR);
    let n2 = b.iter().position(|&c| c == TAG_SEPARATOR_CHAR);
    let n3 = b.iter().position(|&c| c == KV_SEPARATOR_CHAR);
    if n1.is_none() && n2.is_none() && n3.is_none() {
        // Fast path.
        dst.extend_from_slice(b);
        dst.push(TAG_SEPARATOR_CHAR);
        return;
    }

    // Slow path.
    for &ch in b {
        match ch {
            ESCAPE_CHAR => dst.extend_from_slice(&[ESCAPE_CHAR, b'0']),
            TAG_SEPARATOR_CHAR => dst.extend_from_slice(&[ESCAPE_CHAR, b'1']),
            KV_SEPARATOR_CHAR => dst.extend_from_slice(&[ESCAPE_CHAR, b'2']),
            _ => dst.push(ch),
        }
    }

    dst.push(TAG_SEPARATOR_CHAR);
}

/// Unmarshals a tag value from src into dst and returns the remaining src
/// tail.
///
/// PORT NOTE: Go returns (src, dst, err); the Rust port appends into the dst
/// Vec and returns the tail.
pub fn unmarshal_tag_value<'a>(dst: &mut Vec<u8>, src: &'a [u8]) -> Result<&'a [u8], String> {
    let n = match src.iter().position(|&c| c == TAG_SEPARATOR_CHAR) {
        Some(n) => n,
        None => return Err("cannot find the end of tag value".to_string()),
    };
    let mut b = &src[..n];
    let src = &src[n + 1..];
    loop {
        let n = match b.iter().position(|&c| c == ESCAPE_CHAR) {
            Some(n) => n,
            None => {
                dst.extend_from_slice(b);
                return Ok(src);
            }
        };
        dst.extend_from_slice(&b[..n]);
        b = &b[n + 1..];
        if b.is_empty() {
            return Err("missing escaped char".to_string());
        }
        match b[0] {
            b'0' => dst.push(ESCAPE_CHAR),
            b'1' => dst.push(TAG_SEPARATOR_CHAR),
            b'2' => dst.push(KV_SEPARATOR_CHAR),
            _ => return Err(format!("unsupported escaped char: {}", b[0] as char)),
        }
        b = &b[1..];
    }
}

/// PORT NOTE: parseStreamFields lives in storage_search.go upstream; it is
/// ported here because unmarshal_string_inplace (and its tests) need it —
/// storage_search.rs must reuse this definition instead of re-porting it.
pub(crate) fn parse_stream_fields(dst: &mut Vec<Field>, s: &str) -> Result<(), String> {
    if s.is_empty() || !s.starts_with('{') {
        return Err("missing '{' at the beginning of stream name".to_string());
    }
    let mut s = &s[1..];
    if s.is_empty() || !s.ends_with('}') {
        return Err("missing '}' at the end of stream name".to_string());
    }
    s = &s[..s.len() - 1];
    if s.is_empty() {
        return Ok(());
    }

    loop {
        let n = match s.find("=\"") {
            Some(n) => n,
            None => {
                return Err(format!("cannot find field value in double quotes at [{s}]"));
            }
        };
        let name = &s[..n];
        s = &s[n + 1..];

        let (value, n_offset) = match crate::pattern::try_unquote_string(s, "") {
            Some((value, n_offset)) => (value, n_offset),
            None => {
                return Err(format!(
                    "cannot find parse field value in double quotes at [{s}]"
                ));
            }
        };
        s = &s[n_offset..];

        dst.push(Field {
            name: name.to_string(),
            value: value.into_bytes(),
        });

        if s.is_empty() {
            return Ok(());
        }
        if !s.starts_with(',') {
            let f = dst.last().unwrap();
            return Err(format!(
                "missing ',' after {}={:?}",
                f.name,
                String::from_utf8_lossy(&f.value)
            ));
        }
        s = &s[1..];
    }
}

/// Returns non-nil error if names contain prohibited chars, which cannot be
/// used in stream field names.
pub fn check_stream_field_names(names: &[&str]) -> Result<(), String> {
    for name in names {
        check_stream_field_name(name)?;
    }
    Ok(())
}

/// Returns non-nil error if the name contains prohibited chars, which cannot
/// be used in stream field names.
pub fn check_stream_field_name(name: &str) -> Result<(), String> {
    if name.contains('=') {
        // The '=' cannot be located in stream field name, since it prevents from the proper parsing
        // when such a name is put inside _stream value.
        // For example:
        // - 'foo=bar' name cannot be parsed reliably in _stream={foo=bar="baz"}
        return Err(format!("the {name:?} cannot contain '=' char"));
    }
    if name.contains('}') {
        // The '}' cannot be located in stream field name, since it prevents from the proper parsing
        // when such a name is put inside _stream value.
        // For example:
        // - 'foo}bar' name cannot be parsed reliably in _stream={foo}bar="baz"}
        return Err(format!("the {name:?} cannot contain '}}' char"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_tags_unmarshal_string_inplace_success() {
        fn f(s: &str) {
            let mut st = StreamTags::default();
            st.unmarshal_string_inplace(s).unwrap_or_else(|err| {
                panic!("unexpected error in unmarshal_string_inplace({s}): {err}")
            });
            let result = st.to_string();
            assert_eq!(result, s, "unexpected result");
        }

        f(r#"{}"#);
        f(r#"{foo="bar"}"#);
        f(r#"{a="b",c="d"}"#);
    }

    #[test]
    fn test_stream_tags_unmarshal_string_inplace_failure() {
        fn f(s: &str) {
            let mut st = StreamTags::default();
            assert!(
                st.unmarshal_string_inplace(s).is_err(),
                "expecting non-nil error in unmarshal_string_inplace({s})"
            );
        }

        f("");
        f("{");
        f("{foo}");
        f(r#"{"foo":"bar"}"#);
        f("{foo=abc");
        f(r#"{foo="abc"#);
        f(r#"{foo="abc""#);
        f(r#"{foo="abc","#);
        f(r#"{foo="abc",bar}"#);
    }
}

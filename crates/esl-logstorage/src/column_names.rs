//! Port of EsLogs `lib/logstorage/column_names.go`.

// TODO: remove once the upstream consumers of this module
// (block_stream_reader.go, block_stream_writer.go, part.go) are ported; until
// then the crate-private API is only exercised by the tests below.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use esl_common::{bytesutil, encoding, filestream, panicf};

use crate::block_stream_writer::WriterWithStats;

/// Writes the column indexes to w.
pub fn must_write_column_idxs(w: &mut WriterWithStats<'_>, column_idxs: &HashMap<u64, u64>) {
    let mut data = Vec::new();
    marshal_column_idxs(&mut data, column_idxs);
    w.must_write(&data);
}

/// Reads the column indexes from r.
pub fn must_read_column_idxs(
    r: &mut dyn filestream::ReadCloser,
    column_names: &[Arc<str>],
    shards_count: u64,
) -> HashMap<Arc<str>, u64> {
    let src = match read_all(r) {
        Ok(src) => src,
        Err(err) => {
            panicf!("FATAL: {}: cannot read column indexes: {}", r.path(), err);
            unreachable!()
        }
    };

    match unmarshal_column_idxs(&src, column_names, shards_count) {
        Ok(column_idxs) => column_idxs,
        Err(err) => {
            panicf!("FATAL: {}: cannot parse column indexes: {}", r.path(), err);
            unreachable!()
        }
    }
}

pub fn marshal_column_idxs(dst: &mut Vec<u8>, column_idxs: &HashMap<u64, u64>) {
    encoding::marshal_var_uint64(dst, column_idxs.len() as u64);
    for (&column_id, &shard_idx) in column_idxs {
        encoding::marshal_var_uint64(dst, column_id);
        encoding::marshal_var_uint64(dst, shard_idx);
    }
}

pub fn unmarshal_column_idxs(
    src: &[u8],
    column_names: &[Arc<str>],
    shards_count: u64,
) -> Result<HashMap<Arc<str>, u64>, String> {
    let mut src = src;

    let (n, n_bytes) = encoding::unmarshal_var_uint64(src);
    if n_bytes <= 0 {
        return Err(format!(
            "cannot parse the number of entries from len(src)={}",
            src.len()
        ));
    }
    src = &src[n_bytes as usize..];
    if n > isize::MAX as u64 {
        return Err(format!(
            "too many entries: {n}; mustn't exceed {}",
            isize::MAX
        ));
    }

    let mut shard_idxs: HashMap<Arc<str>, u64> = HashMap::with_capacity(n as usize);
    for i in 0..n {
        let (column_id, n_bytes) = encoding::unmarshal_var_uint64(src);
        if n_bytes <= 0 {
            return Err(format!("cannot parse columnID #{i}"));
        }
        src = &src[n_bytes as usize..];

        let (shard_idx, n_bytes) = encoding::unmarshal_var_uint64(src);
        if n_bytes <= 0 {
            return Err(format!("cannot parse shardIdx #{i}"));
        }
        if shard_idx >= shards_count {
            return Err(format!(
                "too big shardIdx={shard_idx}; must be smaller than {shards_count}"
            ));
        }
        src = &src[n_bytes as usize..];

        if column_id >= column_names.len() as u64 {
            return Err(format!(
                "too big columnID; got {column_id}; must be smaller than {}",
                column_names.len()
            ));
        }
        let column_name = column_names[column_id as usize].clone();
        shard_idxs.insert(column_name, shard_idx);
    }
    if !src.is_empty() {
        return Err(format!(
            "unexpected tail left after reading column indexes; len(tail)={}",
            src.len()
        ));
    }

    Ok(shard_idxs)
}

/// Writes the column names dictionary to w.
pub fn must_write_column_names<S: AsRef<str>>(w: &mut WriterWithStats<'_>, column_names: &[S]) {
    let mut data = Vec::new();
    marshal_column_names(&mut data, column_names);
    w.must_write(&data);
}

/// Reads the column names dictionary from r.
pub fn must_read_column_names(
    r: &mut dyn filestream::ReadCloser,
) -> (Vec<Arc<str>>, HashMap<Arc<str>, u64>) {
    let src = match read_all(r) {
        Ok(src) => src,
        Err(err) => {
            panicf!("FATAL: {}: cannot read column names: {}", r.path(), err);
            unreachable!()
        }
    };

    match unmarshal_column_names(&src) {
        Ok((column_names, column_name_ids)) => (column_names, column_name_ids),
        Err(err) => {
            panicf!("FATAL: {}: {}", r.path(), err);
            unreachable!()
        }
    }
}

pub fn marshal_column_names<S: AsRef<str>>(dst: &mut Vec<u8>, column_names: &[S]) {
    let mut data = Vec::new();
    encoding::marshal_var_uint64(&mut data, column_names.len() as u64);
    // PORT NOTE: Go calls marshalStrings (defined in storage_search.go); it
    // is inlined here like in values_encoder.rs, since storage_search belongs
    // to a later layer.
    for name in column_names {
        encoding::marshal_bytes(&mut data, name.as_ref().as_bytes());
    }

    encoding::compress_zstd_level(dst, &data, 1);
}

/// The column names list and the column name → id mapping returned by
/// [`unmarshal_column_names`] (Go's `([]string, map[string]uint64)`).
pub type ColumnNamesWithIDs = (Vec<Arc<str>>, HashMap<Arc<str>, u64>);

/// PORT NOTE: Go returns `([]string, map[string]uint64)` with interned
/// strings; the port uses `Arc<str>` (the shape of
/// `esl_common::bytesutil::intern_bytes`). Like `bytesutil::to_unsafe_string`,
/// it panics on non-UTF-8 column names, which Go string headers would accept.
pub fn unmarshal_column_names(src: &[u8]) -> Result<ColumnNamesWithIDs, String> {
    let mut data = Vec::new();
    encoding::decompress_zstd(&mut data, src).map_err(|err| {
        format!(
            "cannot decompress column names from len(src)={}: {err}",
            src.len()
        )
    })?;
    let mut src = &data[..];

    let (n, n_bytes) = encoding::unmarshal_var_uint64(src);
    if n_bytes <= 0 {
        return Err(format!(
            "cannot parse the number of column names for len(src)={}",
            src.len()
        ));
    }
    src = &src[n_bytes as usize..];
    if n > isize::MAX as u64 {
        return Err(format!(
            "too many distinct column names: {n}; mustn't exceed {}",
            isize::MAX
        ));
    }

    let mut column_name_ids: HashMap<Arc<str>, u64> = HashMap::with_capacity(n as usize);
    let mut column_names: Vec<Arc<str>> = Vec::with_capacity(n as usize);

    for id in 0..n {
        let (name, n_bytes) = encoding::unmarshal_bytes(src);
        if n_bytes <= 0 {
            return Err(format!("cannot parse column name number {id} out of {n}"));
        }
        src = &src[n_bytes as usize..];
        let name = name.unwrap_or_default();

        // It should be good idea to intern column names, since usually the number of unique column names is quite small,
        // even for wide events (e.g. less than a few thousands). So, if the average length of the column name
        // exceeds 8 bytes (this is a typical case for Kubernetes with long column names), then interning saves some RAM.
        let name_str = bytesutil::intern_bytes(name);

        if let Some(&id_prev) = column_name_ids.get(name_str.as_ref()) {
            return Err(format!(
                "duplicate ids for column name {:?}: {id_prev} and {id}",
                bytesutil::to_unsafe_string(name)
            ));
        }

        column_name_ids.insert(name_str.clone(), id);
        column_names.push(name_str);
    }

    if !src.is_empty() {
        return Err(format!(
            "unexpected non-empty tail left after unmarshaling column name ids; len(tail)={}",
            src.len()
        ));
    }

    Ok((column_names, column_name_ids))
}

#[derive(Debug, Default)]
pub struct ColumnNameIDGenerator {
    /// column_name_ids contains columnName->id mapping for already seen columns
    pub column_name_ids: HashMap<Arc<str>, u64>,

    /// column_names contains id->columnName mapping for already seen columns
    pub column_names: Vec<Arc<str>>,
}

impl ColumnNameIDGenerator {
    pub fn reset(&mut self) {
        // PORT NOTE: Go sets the map and the slice to nil.
        self.column_name_ids = HashMap::new();
        self.column_names = Vec::new();
    }

    pub fn get_column_name_id(&mut self, name: &str) -> u64 {
        if let Some(&id) = self.column_name_ids.get(name) {
            return id;
        }
        let id = self.column_names.len() as u64;

        // it is better to intern the column name instead of cloning it with string.Clone,
        // since the number of column names is usually small (e.g. less than 10K).
        // This reduces memory allocations.
        let name_copy = bytesutil::intern_string(name);

        self.column_name_ids.insert(name_copy.clone(), id);
        self.column_names.push(name_copy);
        id
    }
}

/// PORT NOTE: replaces Go's `io.ReadAll` on the filestream reader.
fn read_all(r: &mut dyn filestream::ReadCloser) -> std::io::Result<Vec<u8>> {
    let mut data = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = r.read(&mut chunk)?;
        if n == 0 {
            return Ok(data);
        }
        data.extend_from_slice(&chunk[..n]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_marshal_unmarshal_column_names() {
        let f = |column_names: &[&str]| {
            let mut data = Vec::new();
            marshal_column_names(&mut data, column_names);
            let (result_column_names, result_column_name_ids) = match unmarshal_column_names(&data)
            {
                Ok(result) => result,
                Err(err) => panic!("unexpected error when unmarshaling columnNames: {err}"),
            };

            // Check column_names
            let result_column_names: Vec<&str> =
                result_column_names.iter().map(|s| s.as_ref()).collect();
            assert_eq!(
                result_column_names, column_names,
                "unexpected umarshaled columnNames\ngot\n{result_column_names:?}\nwant\n{column_names:?}"
            );

            // Check column_name_ids
            let mut expected_column_name_ids: HashMap<&str, u64> = HashMap::new();
            for (i, n) in column_names.iter().enumerate() {
                expected_column_name_ids.insert(n, i as u64);
            }
            let result_column_name_ids: HashMap<&str, u64> = result_column_name_ids
                .iter()
                .map(|(k, &v)| (k.as_ref(), v))
                .collect();
            assert_eq!(
                result_column_name_ids, expected_column_name_ids,
                "unexpected columnNameIDs\ngot\n{result_column_name_ids:?}\nwant\n{expected_column_name_ids:?}"
            );
        };

        f(&[]);

        f(&["", "foo", "bar"]);

        f(&[
            "asdf.sdf.dsfds.f fds. fds ",
            "foo",
            "bar.sdfsdf.fd",
            "",
            "aso apaa",
        ]);
    }

    #[test]
    fn test_column_name_id_generator() {
        let a = ["", "foo", "bar.baz", "asdf dsf dfs"];

        let mut g = ColumnNameIDGenerator::default();

        for (i, s) in a.iter().enumerate() {
            let id = g.get_column_name_id(s);
            assert_eq!(
                id, i as u64,
                "first run: unexpected id generated for s={s:?}; got {id}; want {i}; g={g:?}"
            );
        }

        // Repeat the loop
        for (i, s) in a.iter().enumerate() {
            let id = g.get_column_name_id(s);
            assert_eq!(
                id, i as u64,
                "second run: unexpected id generated for s={s:?}; got {id}; want {i}; g={g:?}"
            );
        }
    }
}

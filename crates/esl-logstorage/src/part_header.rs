//! Port of `lib/logstorage/part_header.go`.

use std::fmt;
use std::path::Path;

use esl_common::{fs, panicf};

use crate::consts::PART_FORMAT_LATEST_VERSION;
use crate::filenames::METADATA_FILENAME;

/// PartHeader contains the information about a single part.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PartHeader {
    /// FormatVersion is the version of the part format.
    pub format_version: u64,

    /// CompressedSizeBytes is physical size of the part.
    pub compressed_size_bytes: u64,

    /// UncompressedSizeBytes is the original size of log entries stored in the part.
    pub uncompressed_size_bytes: u64,

    /// RowsCount is the number of log entries in the part.
    pub rows_count: u64,

    /// BlocksCount is the number of blocks in the part.
    pub blocks_count: u64,

    /// MinTimestamp is the minimum timestamp seen in the part.
    pub min_timestamp: i64,

    /// MaxTimestamp is the maximum timestamp seen in the part.
    pub max_timestamp: i64,

    /// BloomValuesShardsCount is the number of (bloom, values) shards in the part.
    pub bloom_values_shards_count: u64,
}

impl PartHeader {
    /// Resets ph for subsequent reuse.
    pub fn reset(&mut self) {
        self.format_version = 0;
        self.compressed_size_bytes = 0;
        self.uncompressed_size_bytes = 0;
        self.rows_count = 0;
        self.blocks_count = 0;
        self.min_timestamp = 0;
        self.max_timestamp = 0;
        self.bloom_values_shards_count = 0;
    }

    /// Reads ph from `metadata.json` in the part directory at part_path.
    pub fn must_read_metadata(&mut self, part_path: &Path) {
        self.reset();

        let metadata_path = part_path.join(METADATA_FILENAME);
        let metadata = std::fs::read(&metadata_path).unwrap_or_else(|err| {
            panicf!("FATAL: cannot read {:?}: {}", metadata_path, err);
            unreachable!()
        });
        if let Err(err) = self.unmarshal_json(&metadata) {
            panicf!("FATAL: cannot parse {:?}: {}", metadata_path, err);
        }

        if self.format_version <= 1 {
            if self.bloom_values_shards_count != 0 {
                panicf!(
                    "FATAL: {:?}: unexpected BloomValuesShardsCount for FormatVersion<=1; got {}; want 0",
                    metadata_path,
                    self.bloom_values_shards_count
                );
            }
            if self.format_version == 1 {
                self.bloom_values_shards_count = 8;
            }
        }

        // Perform various checks
        if self.format_version > PART_FORMAT_LATEST_VERSION {
            panicf!(
                "FATAL: {:?}: unsupported part format version; got {}; mustn't exceed {}",
                metadata_path,
                self.format_version,
                PART_FORMAT_LATEST_VERSION
            );
        }
        if self.min_timestamp > self.max_timestamp {
            panicf!(
                "FATAL: {:?}: MinTimestamp cannot exceed MaxTimestamp; got {} vs {}",
                metadata_path,
                self.min_timestamp,
                self.max_timestamp
            );
        }
        if self.blocks_count > self.rows_count {
            panicf!(
                "FATAL: {:?}: BlocksCount={} cannot exceed RowsCount={}",
                metadata_path,
                self.blocks_count,
                self.rows_count
            );
        }
    }

    /// Writes ph to `metadata.json` in the part directory at part_path.
    ///
    /// PORT NOTE: Go's json.Marshal error branch (`BUG: cannot marshal
    /// partHeader`) is unreachable for this struct and is dropped.
    pub fn must_write_metadata(&self, part_path: &Path) {
        let metadata = self.marshal_json();
        let metadata_path = part_path.join(METADATA_FILENAME);
        fs::must_write_sync(&metadata_path, &metadata);
    }

    /// Returns the JSON representation of ph.
    ///
    /// PORT NOTE: Go uses encoding/json; the port renders the same output by
    /// hand (serde is not a dependency), byte-matching Go's field order and
    /// number formatting for this struct.
    fn marshal_json(&self) -> Vec<u8> {
        format!(
            "{{\"FormatVersion\":{},\"CompressedSizeBytes\":{},\"UncompressedSizeBytes\":{},\
             \"RowsCount\":{},\"BlocksCount\":{},\"MinTimestamp\":{},\"MaxTimestamp\":{},\
             \"BloomValuesShardsCount\":{}}}",
            self.format_version,
            self.compressed_size_bytes,
            self.uncompressed_size_bytes,
            self.rows_count,
            self.blocks_count,
            self.min_timestamp,
            self.max_timestamp,
            self.bloom_values_shards_count
        )
        .into_bytes()
    }

    /// Unmarshals ph from the JSON object at src.
    ///
    /// PORT NOTE: Go uses encoding/json; the port implements a minimal parser
    /// for JSON objects with integer members (any member order, missing
    /// members keep their current value like in Go). Unknown members are
    /// skipped when their value is a scalar; nested arrays/objects in unknown
    /// members are rejected. Error wording differs from Go's encoding/json.
    fn unmarshal_json(&mut self, src: &[u8]) -> Result<(), String> {
        let mut p = JsonParser { src, pos: 0 };
        p.skip_ws();
        p.expect(b'{')?;
        p.skip_ws();
        if p.peek() == Some(b'}') {
            p.pos += 1;
            p.skip_ws();
            return p.expect_eof();
        }
        loop {
            p.skip_ws();
            let key = p.parse_string()?;
            p.skip_ws();
            p.expect(b':')?;
            p.skip_ws();
            match key {
                "FormatVersion" => self.format_version = p.parse_uint64()?,
                "CompressedSizeBytes" => self.compressed_size_bytes = p.parse_uint64()?,
                "UncompressedSizeBytes" => self.uncompressed_size_bytes = p.parse_uint64()?,
                "RowsCount" => self.rows_count = p.parse_uint64()?,
                "BlocksCount" => self.blocks_count = p.parse_uint64()?,
                "MinTimestamp" => self.min_timestamp = p.parse_int64()?,
                "MaxTimestamp" => self.max_timestamp = p.parse_int64()?,
                "BloomValuesShardsCount" => self.bloom_values_shards_count = p.parse_uint64()?,
                _ => p.skip_scalar_value()?,
            }
            p.skip_ws();
            match p.next() {
                Some(b',') => continue,
                Some(b'}') => break,
                _ => return Err("missing ',' or '}' after object member".to_string()),
            }
        }
        p.skip_ws();
        p.expect_eof()
    }
}

impl fmt::Display for PartHeader {
    /// Returns string representation for ph.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{FormatVersion={}, CompressedSizeBytes={}, UncompressedSizeBytes={}, RowsCount={}, \
             BlocksCount={}, MinTimestamp={}, MaxTimestamp={}, BloomValuesShardsCount={}}}",
            self.format_version,
            self.compressed_size_bytes,
            self.uncompressed_size_bytes,
            self.rows_count,
            self.blocks_count,
            timestamp_to_string(self.min_timestamp),
            timestamp_to_string(self.max_timestamp),
            self.bloom_values_shards_count
        )
    }
}

/// Returns the pathname-friendly representation of the given timestamp in
/// nanoseconds, in the form YYYYMMDDhhmmss followed by 9 fractional digits.
///
/// Matches Go's `time.Unix(0, timestamp).UTC().Format("20060102150405.000000000")`
/// with the "." removed.
fn timestamp_to_string(timestamp: i64) -> String {
    const NSECS_PER_SECOND: i64 = 1_000_000_000;
    let secs = timestamp.div_euclid(NSECS_PER_SECOND);
    let nsecs = timestamp.rem_euclid(NSECS_PER_SECOND);

    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = secs_of_day % 3600 / 60;
    let second = secs_of_day % 60;

    format!("{year:04}{month:02}{day:02}{hour:02}{minute:02}{second:02}{nsecs:09}")
}

/// Returns (year, month, day) in the proleptic Gregorian calendar for the
/// given number of days since 1970-01-01.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (y + i64::from(m <= 2), m as u32, d as u32)
}

/// Minimal JSON parser for the partHeader metadata object.
struct JsonParser<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn skip_ws(&mut self) {
        while self.pos < self.src.len()
            && matches!(self.src[self.pos], b' ' | b'\t' | b'\n' | b'\r')
        {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn expect(&mut self, c: u8) -> Result<(), String> {
        if self.next() != Some(c) {
            return Err(format!("missing {:?}", c as char));
        }
        Ok(())
    }

    fn expect_eof(&self) -> Result<(), String> {
        if self.pos != self.src.len() {
            return Err("unexpected trailing data".to_string());
        }
        Ok(())
    }

    fn parse_string(&mut self) -> Result<&'a str, String> {
        self.expect(b'"')?;
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == b'"' {
                let s = std::str::from_utf8(&self.src[start..self.pos])
                    .map_err(|_| "invalid UTF-8 in string".to_string())?;
                self.pos += 1;
                return Ok(s);
            }
            if c == b'\\' {
                return Err("escape sequences in strings are not supported".to_string());
            }
            self.pos += 1;
        }
        Err("unterminated string".to_string())
    }

    fn parse_uint64(&mut self) -> Result<u64, String> {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.pos == start {
            return Err("missing number".to_string());
        }
        let s = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
        s.parse::<u64>()
            .map_err(|err| format!("cannot parse {s:?} as uint64: {err}"))
    }

    fn parse_int64(&mut self) -> Result<i64, String> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
        }
        let s = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
        if s.is_empty() || s == "-" {
            return Err("missing number".to_string());
        }
        s.parse::<i64>()
            .map_err(|err| format!("cannot parse {s:?} as int64: {err}"))
    }

    /// Skips a scalar JSON value (string, number, true, false or null) for
    /// unknown object members, which Go's encoding/json ignores.
    fn skip_scalar_value(&mut self) -> Result<(), String> {
        match self.peek() {
            Some(b'"') => {
                self.parse_string()?;
                Ok(())
            }
            Some(b't') => self.consume_literal(b"true"),
            Some(b'f') => self.consume_literal(b"false"),
            Some(b'n') => self.consume_literal(b"null"),
            Some(c) if c == b'-' || c.is_ascii_digit() => {
                self.pos += 1;
                while matches!(
                    self.peek(),
                    Some(c) if c.is_ascii_digit() || matches!(c, b'.' | b'e' | b'E' | b'+' | b'-')
                ) {
                    self.pos += 1;
                }
                Ok(())
            }
            _ => Err("unsupported value in unknown object member".to_string()),
        }
    }

    fn consume_literal(&mut self, lit: &[u8]) -> Result<(), String> {
        if self.src[self.pos..].starts_with(lit) {
            self.pos += lit.len();
            return Ok(());
        }
        Err(format!(
            "missing literal {:?}",
            std::str::from_utf8(lit).unwrap()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_part_header_reset() {
        let mut ph = PartHeader {
            compressed_size_bytes: 123,
            uncompressed_size_bytes: 234,
            rows_count: 1234,
            min_timestamp: 3434,
            max_timestamp: 32434,
            ..Default::default()
        };
        ph.reset();
        let ph_zero = PartHeader::default();
        assert_eq!(
            ph, ph_zero,
            "unexpected non-zero partHeader after reset: {ph}"
        );
    }

    // PORT NOTE: the Go package has no tests for the metadata JSON format;
    // the tests below are Rust-side golden checks pinning the byte-exact
    // encoding/json output and the timestamp formatting, since metadata.json
    // is part of the on-disk format.
    #[test]
    fn test_part_header_marshal_json_golden() {
        let ph = PartHeader {
            format_version: 3,
            compressed_size_bytes: 123,
            uncompressed_size_bytes: 456,
            rows_count: 10,
            blocks_count: 2,
            min_timestamp: -4334,
            max_timestamp: 23434,
            bloom_values_shards_count: 8,
        };
        let data = ph.marshal_json();
        assert_eq!(
            std::str::from_utf8(&data).unwrap(),
            "{\"FormatVersion\":3,\"CompressedSizeBytes\":123,\"UncompressedSizeBytes\":456,\
             \"RowsCount\":10,\"BlocksCount\":2,\"MinTimestamp\":-4334,\"MaxTimestamp\":23434,\
             \"BloomValuesShardsCount\":8}"
        );

        let mut ph2 = PartHeader::default();
        ph2.unmarshal_json(&data)
            .unwrap_or_else(|err| panic!("unexpected error when unmarshaling JSON: {err}"));
        assert_eq!(ph2, ph, "unexpected partHeader after JSON round-trip");
    }

    #[test]
    fn test_part_header_unmarshal_json() {
        // member order and whitespace must not matter; missing members keep
        // their zero value; unknown scalar members are ignored.
        let mut ph = PartHeader::default();
        ph.unmarshal_json(
            b" {\n\t\"RowsCount\": 42, \"MinTimestamp\": -7,\r\n \"Unknown\": \"x\" } ",
        )
        .unwrap_or_else(|err| panic!("unexpected error when unmarshaling JSON: {err}"));
        assert_eq!(
            ph,
            PartHeader {
                rows_count: 42,
                min_timestamp: -7,
                ..Default::default()
            }
        );

        let mut ph = PartHeader::default();
        for data in [
            &b""[..],
            b"foo",
            b"{",
            b"{\"RowsCount\":}",
            b"{\"RowsCount\":1",
            b"{\"MinTimestamp\":-}",
        ] {
            assert!(
                ph.unmarshal_json(data).is_err(),
                "expecting non-nil error for {data:?}"
            );
        }
    }

    #[test]
    fn test_timestamp_to_string() {
        assert_eq!(timestamp_to_string(0), "19700101000000000000000");
        assert_eq!(
            timestamp_to_string(1_000_000_000_000_000_000),
            "20010909014640000000000"
        );
        assert_eq!(timestamp_to_string(-1), "19691231235959999999999");
    }

    #[test]
    fn test_part_header_string() {
        let ph = PartHeader {
            format_version: 1,
            compressed_size_bytes: 2,
            uncompressed_size_bytes: 3,
            rows_count: 4,
            blocks_count: 5,
            min_timestamp: 0,
            max_timestamp: 1_000_000_000_000_000_000,
            bloom_values_shards_count: 8,
        };
        assert_eq!(
            ph.to_string(),
            "{FormatVersion=1, CompressedSizeBytes=2, UncompressedSizeBytes=3, RowsCount=4, \
             BlocksCount=5, MinTimestamp=19700101000000000000000, \
             MaxTimestamp=20010909014640000000000, BloomValuesShardsCount=8}"
        );
    }

    #[test]
    fn test_part_header_metadata_read_write() {
        let dir = std::env::temp_dir().join(format!(
            "esl-part-header-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let ph = PartHeader {
            format_version: 3,
            compressed_size_bytes: 123,
            uncompressed_size_bytes: 456,
            rows_count: 10,
            blocks_count: 2,
            min_timestamp: -4334,
            max_timestamp: 23434,
            bloom_values_shards_count: 4,
        };
        ph.must_write_metadata(&dir);

        let mut ph2 = PartHeader::default();
        ph2.must_read_metadata(&dir);
        assert_eq!(ph2, ph, "unexpected partHeader read from metadata");

        std::fs::remove_dir_all(&dir).unwrap();
    }
}

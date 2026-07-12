//! Port of `lib/mergeset/part_header.go`.
//!
//! The `metadata.json` file format must match Go's `json.Marshal` output for
//! `partHeaderJSON` exactly enough for cross-reading:
//! `{"ItemsCount":N,"BlocksCount":N,"FirstItem":"<hex>","LastItem":"<hex>"}`.

use std::path::Path;

use esl_common::fs;

use super::METADATA_FILENAME;

#[derive(Default, Clone)]
pub(crate) struct PartHeader {
    /// The number of items the part contains.
    pub items_count: u64,

    /// The number of blocks the part contains.
    pub blocks_count: u64,

    /// The first item in the part.
    pub first_item: Vec<u8>,

    /// The last item in the part.
    pub last_item: Vec<u8>,
}

impl PartHeader {
    pub fn reset(&mut self) {
        self.items_count = 0;
        self.blocks_count = 0;
        self.first_item.clear();
        self.last_item.clear();
    }

    pub fn copy_from(&mut self, src: &PartHeader) {
        self.items_count = src.items_count;
        self.blocks_count = src.blocks_count;
        self.first_item.clear();
        self.first_item.extend_from_slice(&src.first_item);
        self.last_item.clear();
        self.last_item.extend_from_slice(&src.last_item);
    }

    /// Port of `partHeader.MustReadMetadata`.
    pub fn must_read_metadata(&mut self, part_path: &Path) {
        self.reset();

        let metadata_path = part_path.join(METADATA_FILENAME);
        let metadata = match std::fs::read(&metadata_path) {
            Ok(data) => data,
            Err(err) => {
                esl_common::panicf!("FATAL: cannot read {}: {}", metadata_path.display(), err);
                unreachable!()
            }
        };

        match parse_part_header_json(&metadata) {
            Ok(phj) => {
                if phj.items_count == 0 {
                    esl_common::panicf!(
                        "FATAL: part {} cannot contain zero items",
                        part_path.display()
                    );
                }
                self.items_count = phj.items_count;

                if phj.blocks_count == 0 {
                    esl_common::panicf!(
                        "FATAL: part {} cannot contain zero blocks",
                        part_path.display()
                    );
                }
                if phj.blocks_count > phj.items_count {
                    esl_common::panicf!(
                        "FATAL: the number of blocks cannot exceed the number of items in the part {}; got blocksCount={}, itemsCount={}",
                        part_path.display(),
                        phj.blocks_count,
                        phj.items_count
                    );
                }
                self.blocks_count = phj.blocks_count;

                self.first_item = phj.first_item;
                self.last_item = phj.last_item;
            }
            Err(err) => {
                esl_common::panicf!("FATAL: cannot parse {}: {}", metadata_path.display(), err);
            }
        }
    }

    /// Port of `partHeader.MustWriteMetadata`.
    pub fn must_write_metadata(&self, part_path: &Path) {
        let metadata = marshal_part_header_json(self);
        let metadata_path = part_path.join(METADATA_FILENAME);
        // There is no need in calling fs.MustWriteAtomic() here,
        // since the file is created only once during part creation
        // and the part directory is synced afterward.
        fs::must_write_sync(metadata_path, &metadata);
    }
}

struct PartHeaderJson {
    items_count: u64,
    blocks_count: u64,
    first_item: Vec<u8>,
    last_item: Vec<u8>,
}

/// Serializes ph in the same field order as Go's `json.Marshal(partHeaderJSON)`.
fn marshal_part_header_json(ph: &PartHeader) -> Vec<u8> {
    let mut data = Vec::with_capacity(64 + 2 * (ph.first_item.len() + ph.last_item.len()));
    data.extend_from_slice(b"{\"ItemsCount\":");
    data.extend_from_slice(ph.items_count.to_string().as_bytes());
    data.extend_from_slice(b",\"BlocksCount\":");
    data.extend_from_slice(ph.blocks_count.to_string().as_bytes());
    data.extend_from_slice(b",\"FirstItem\":\"");
    append_hex(&mut data, &ph.first_item);
    data.extend_from_slice(b"\",\"LastItem\":\"");
    append_hex(&mut data, &ph.last_item);
    data.extend_from_slice(b"\"}");
    data
}

fn append_hex(dst: &mut Vec<u8>, src: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in src {
        dst.push(HEX[(b >> 4) as usize]);
        dst.push(HEX[(b & 0xF) as usize]);
    }
}

fn decode_hex(src: &str) -> Result<Vec<u8>, String> {
    if !src.len().is_multiple_of(2) {
        return Err(format!("odd-length hex string {src:?}"));
    }
    let src = src.as_bytes();
    let mut dst = Vec::with_capacity(src.len() / 2);
    for pair in src.chunks_exact(2) {
        let hi = (pair[0] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex char {:?}", pair[0] as char))?;
        let lo = (pair[1] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex char {:?}", pair[1] as char))?;
        dst.push(((hi << 4) | lo) as u8);
    }
    Ok(dst)
}

/// Minimal field-order-independent parser for the metadata.json object
/// written by Go's `json.Marshal(partHeaderJSON)`.
///
/// PORT NOTE: Go uses encoding/json; the port hand-rolls the codec instead of
/// adding a JSON dependency (same approach as datadb's parts.json codec).
fn parse_part_header_json(data: &[u8]) -> Result<PartHeaderJson, String> {
    let s = std::str::from_utf8(data).map_err(|err| format!("invalid UTF-8: {err}"))?;
    let s = s.trim();
    let s = s
        .strip_prefix('{')
        .ok_or_else(|| "expected '{' at the beginning of JSON object".to_string())?;
    let s = s
        .strip_suffix('}')
        .ok_or_else(|| "expected '}' at the end of JSON object".to_string())?;

    let mut phj = PartHeaderJson {
        items_count: 0,
        blocks_count: 0,
        first_item: Vec::new(),
        last_item: Vec::new(),
    };

    let mut rest = s.trim();
    while !rest.is_empty() {
        // Parse `"Key":`
        let r = rest.strip_prefix('"').ok_or_else(|| {
            format!("expected '\"' at the beginning of JSON key near {rest:.20?}")
        })?;
        let key_end = r
            .find('"')
            .ok_or_else(|| "missing closing '\"' in JSON key".to_string())?;
        let key = &r[..key_end];
        let r = r[key_end + 1..].trim_start();
        let r = r
            .strip_prefix(':')
            .ok_or_else(|| format!("expected ':' after JSON key {key:?}"))?;
        let r = r.trim_start();

        // Parse the value: either a number or a quoted hex string.
        let (value, tail) = if let Some(r2) = r.strip_prefix('"') {
            let value_end = r2
                .find('"')
                .ok_or_else(|| "missing closing '\"' in JSON string value".to_string())?;
            (&r2[..value_end], &r2[value_end + 1..])
        } else {
            let value_end = r.find([',', ' ', '\t', '\n']).unwrap_or(r.len());
            (&r[..value_end], &r[value_end..])
        };

        match key {
            "ItemsCount" => {
                phj.items_count = value
                    .parse()
                    .map_err(|err| format!("cannot parse ItemsCount from {value:?}: {err}"))?;
            }
            "BlocksCount" => {
                phj.blocks_count = value
                    .parse()
                    .map_err(|err| format!("cannot parse BlocksCount from {value:?}: {err}"))?;
            }
            "FirstItem" => {
                phj.first_item = decode_hex(value)
                    .map_err(|err| format!("cannot hex-decode FirstItem: {err}"))?;
            }
            "LastItem" => {
                phj.last_item = decode_hex(value)
                    .map_err(|err| format!("cannot hex-decode LastItem: {err}"))?;
            }
            _ => {
                // Ignore unknown keys, like encoding/json does.
            }
        }

        rest = tail.trim_start();
        if let Some(r) = rest.strip_prefix(',') {
            rest = r.trim_start();
        } else if !rest.is_empty() {
            return Err(format!(
                "unexpected trailing data in JSON object: {rest:.20?}"
            ));
        }
    }

    Ok(phj)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_part_header_json_round_trip() {
        let ph = PartHeader {
            items_count: 1234,
            blocks_count: 7,
            first_item: vec![0x00, 0x01, 0xAB, 0xFF],
            last_item: b"\x02last".to_vec(),
        };
        let data = marshal_part_header_json(&ph);
        let phj = parse_part_header_json(&data).unwrap();
        assert_eq!(phj.items_count, ph.items_count);
        assert_eq!(phj.blocks_count, ph.blocks_count);
        assert_eq!(phj.first_item, ph.first_item);
        assert_eq!(phj.last_item, ph.last_item);
    }

    #[test]
    fn test_part_header_json_go_output() {
        // Byte-exact sample of Go's json.Marshal(partHeaderJSON) output.
        let data =
            br#"{"ItemsCount":35,"BlocksCount":1,"FirstItem":"000000007b00000237","LastItem":"020000007b000002376a6f62"}"#;
        let phj = parse_part_header_json(data).unwrap();
        assert_eq!(phj.items_count, 35);
        assert_eq!(phj.blocks_count, 1);
        assert_eq!(
            phj.first_item,
            vec![0x00, 0x00, 0x00, 0x00, 0x7b, 0x00, 0x00, 0x02, 0x37]
        );
        assert_eq!(phj.last_item.len(), 12);

        // The writer must produce the exact same bytes for the same header.
        let ph = PartHeader {
            items_count: phj.items_count,
            blocks_count: phj.blocks_count,
            first_item: phj.first_item.clone(),
            last_item: phj.last_item.clone(),
        };
        assert_eq!(marshal_part_header_json(&ph), data.to_vec());
    }
}

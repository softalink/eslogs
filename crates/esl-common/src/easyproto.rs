//! Port of `github.com/VictoriaMetrics/easyproto` (`reader.go` + `writer.go`).
//!
//! easyproto is a dependency-free protobuf wire-format codec: the reader walks
//! protobuf-encoded messages via [`FieldContext`], while the writer constructs
//! messages via [`Marshaler`] / [`MessageMarshaler`].
//!
//! PORT NOTE: Go's `(value, bool)` accessor results become `Option<T>`, and
//! `(value, ok, err)` results become `Result<Option<T>, String>`.
//!
//! PORT NOTE: the packed repeated-field helpers (`Unpack*` / `Append*s`) are
//! not ported: the Loki and OpenTelemetry log ingestion protocols only use
//! scalar and message fields.
//!
//! PORT NOTE: Go `Bool()`/`Enum()` accessors are named [`FieldContext::bool_value`]
//! and [`FieldContext::enum_value`] since `bool` and `enum` are reserved words
//! in Rust.

use std::sync::Mutex;

// ---------------------------------------------------------------------------
// reader.go
// ---------------------------------------------------------------------------

/// wireType is the type of protobuf-encoded field.
///
/// See <https://protobuf.dev/programming-guides/encoding/#structure>
type WireType = u8;

/// VARINT type - one of int32, int64, uint32, uint64, sint32, sint64, bool, enum
const WIRE_TYPE_VARINT: WireType = 0;

/// I64 type
const WIRE_TYPE_I64: WireType = 1;

/// Len type
const WIRE_TYPE_LEN: WireType = 2;

/// I32 type
const WIRE_TYPE_I32: WireType = 5;

fn wire_type_string(wt: WireType) -> String {
    match wt {
        WIRE_TYPE_VARINT => "varint".to_string(),
        WIRE_TYPE_I64 => "i64".to_string(),
        WIRE_TYPE_LEN => "len".to_string(),
        WIRE_TYPE_I32 => "i32".to_string(),
        _ => format!("unknown ({wt})"),
    }
}

/// FieldContext represents a single protobuf-encoded field after
/// [`FieldContext::next_field`] or [`FieldContext::field_by_num`] call.
#[derive(Default)]
pub struct FieldContext<'a> {
    /// field_num is the number of the protobuf field read after `next_field()`
    /// or `field_by_num()` call.
    pub field_num: u32,

    /// wire_type is the wire type for the given field.
    wire_type: WireType,

    /// data is protobuf-encoded field data for wire_type=WIRE_TYPE_LEN.
    data: &'a [u8],

    /// int_value contains int value for wire_type!=WIRE_TYPE_LEN.
    int_value: u64,
}

impl<'a> FieldContext<'a> {
    /// FieldByNum sets fc to the field with the given field_num at
    /// protobuf-encoded src.
    ///
    /// `false` is returned if src doesn't contain a field with the given
    /// field_num.
    ///
    /// See also [`FieldContext::next_field`].
    pub fn field_by_num(&mut self, src: &'a [u8], field_num: u32) -> Result<bool, String> {
        let mut src = src;
        while !src.is_empty() {
            src = self.next_field(src).map_err(|err| {
                format!(
                    "cannot read the next field while searching for fieldNum={field_num}: {err}"
                )
            })?;
            if self.field_num != field_num {
                continue;
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// NextField reads the next field from protobuf-encoded src.
    ///
    /// It returns the tail left after reading the next field from src.
    ///
    /// See also [`FieldContext::field_by_num`].
    pub fn next_field(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        if src.len() >= 2 {
            let n = (u16::from(src[0]) << 8) | u16::from(src[1]);
            if (n & 0x8080) == 0 && (n & 0x0700) == (u16::from(WIRE_TYPE_LEN) << 8) {
                // Fast path - read message with the length smaller than 0x80 bytes.
                let msg_len = (n & 0xff) as usize;
                let src = &src[2..];
                if src.len() < msg_len {
                    return Err(format!(
                        "cannot read field from {} bytes; need at least {} bytes",
                        src.len(),
                        msg_len
                    ));
                }
                self.field_num = u32::from(n >> (8 + 3));
                self.wire_type = WIRE_TYPE_LEN;
                self.data = &src[..msg_len];
                return Ok(&src[msg_len..]);
            }
        }

        // Read field tag. See https://protobuf.dev/programming-guides/encoding/#structure
        if src.is_empty() {
            return Err("cannot unmarshal field from empty message".to_string());
        }

        let mut src = src;
        let field_num: u64;
        let tag: u64;
        if src[0] < 0x80 {
            tag = u64::from(src[0]);
            src = &src[1..];
            field_num = tag >> 3;
        } else {
            let (t, offset) = uvarint(src);
            if offset <= 0 {
                return Err("cannot unmarshal field tag from uvarint".to_string());
            }
            tag = t;
            src = &src[offset as usize..];
            field_num = tag >> 3;
            if field_num > u64::from(u32::MAX) {
                return Err(format!(
                    "fieldNum={field_num} is bigger than uint32max={}",
                    u32::MAX
                ));
            }
        }

        let wt = (tag & 0x07) as WireType;

        self.field_num = field_num as u32;
        self.wire_type = wt;

        // Read the remaining data
        if wt == WIRE_TYPE_LEN {
            let (u64v, offset) = uvarint(src);
            if offset <= 0 {
                return Err(format!("cannot read message length for field #{field_num}"));
            }
            let src = &src[offset as usize..];
            if (src.len() as u64) < u64v {
                return Err(format!(
                    "cannot read data for field #{field_num} from {} bytes; need at least {u64v} bytes",
                    src.len()
                ));
            }
            let msg_len = u64v as usize;
            self.data = &src[..msg_len];
            return Ok(&src[msg_len..]);
        }
        if wt == WIRE_TYPE_VARINT {
            let (u64v, offset) = uvarint(src);
            if offset <= 0 {
                return Err(format!(
                    "cannot read varint after field tag for field #{field_num}"
                ));
            }
            self.int_value = u64v;
            return Ok(&src[offset as usize..]);
        }
        if wt == WIRE_TYPE_I64 {
            if src.len() < 8 {
                return Err(format!("cannot read i64 for field #{field_num}"));
            }
            self.int_value = u64::from_le_bytes(src[..8].try_into().unwrap());
            return Ok(&src[8..]);
        }
        if wt == WIRE_TYPE_I32 {
            if src.len() < 4 {
                return Err(format!("cannot read i32 for field #{field_num}"));
            }
            let u32v = u32::from_le_bytes(src[..4].try_into().unwrap());
            self.int_value = u64::from(u32v);
            return Ok(&src[4..]);
        }
        Err(format!("unknown wireType={wt}"))
    }

    /// Int32 returns int32 value for fc.
    ///
    /// `None` is returned if fc doesn't contain int32 value.
    pub fn int32(&self) -> Option<i32> {
        if self.wire_type != WIRE_TYPE_VARINT {
            return None;
        }
        to_int32(self.int_value)
    }

    /// Int64 returns int64 value for fc.
    ///
    /// `None` is returned if fc doesn't contain int64 value.
    pub fn int64(&self) -> Option<i64> {
        if self.wire_type != WIRE_TYPE_VARINT {
            return None;
        }
        Some(self.int_value as i64)
    }

    /// Uint32 returns uint32 value for fc.
    ///
    /// `None` is returned if fc doesn't contain uint32 value.
    pub fn uint32(&self) -> Option<u32> {
        if self.wire_type != WIRE_TYPE_VARINT {
            return None;
        }
        to_uint32(self.int_value)
    }

    /// Uint64 returns uint64 value for fc.
    ///
    /// `None` is returned if fc doesn't contain uint64 value.
    pub fn uint64(&self) -> Option<u64> {
        if self.wire_type != WIRE_TYPE_VARINT {
            return None;
        }
        Some(self.int_value)
    }

    /// Sint32 returns sint32 value for fc.
    ///
    /// `None` is returned if fc doesn't contain sint32 value.
    pub fn sint32(&self) -> Option<i32> {
        if self.wire_type != WIRE_TYPE_VARINT {
            return None;
        }
        let u32v = to_uint32(self.int_value)?;
        Some(decode_zig_zag_int32(u32v))
    }

    /// Sint64 returns sint64 value for fc.
    ///
    /// `None` is returned if fc doesn't contain sint64 value.
    pub fn sint64(&self) -> Option<i64> {
        if self.wire_type != WIRE_TYPE_VARINT {
            return None;
        }
        Some(decode_zig_zag_int64(self.int_value))
    }

    /// Bool returns bool value for fc (Go `Bool()`).
    ///
    /// `None` is returned if fc doesn't contain bool value.
    pub fn bool_value(&self) -> Option<bool> {
        if self.wire_type != WIRE_TYPE_VARINT {
            return None;
        }
        to_bool(self.int_value)
    }

    /// Enum returns enum value for fc (Go `Enum()`).
    ///
    /// `None` is returned if fc doesn't contain enum value.
    pub fn enum_value(&self) -> Option<i32> {
        if self.wire_type != WIRE_TYPE_VARINT {
            return None;
        }
        to_int32(self.int_value)
    }

    /// Fixed64 returns fixed64 value for fc.
    ///
    /// `None` is returned if fc doesn't contain fixed64 value.
    pub fn fixed64(&self) -> Option<u64> {
        if self.wire_type != WIRE_TYPE_I64 {
            return None;
        }
        Some(self.int_value)
    }

    /// Sfixed64 returns sfixed64 value for fc.
    ///
    /// `None` is returned if fc doesn't contain sfixed64 value.
    pub fn sfixed64(&self) -> Option<i64> {
        if self.wire_type != WIRE_TYPE_I64 {
            return None;
        }
        Some(self.int_value as i64)
    }

    /// Double returns double value for fc.
    ///
    /// `None` is returned if fc doesn't contain double value.
    pub fn double(&self) -> Option<f64> {
        if self.wire_type != WIRE_TYPE_I64 {
            return None;
        }
        Some(f64::from_bits(self.int_value))
    }

    /// String returns string value for fc.
    ///
    /// The returned string is valid while the underlying buffer isn't changed.
    ///
    /// `None` is returned if fc doesn't contain string value.
    ///
    /// PORT NOTE: Go returns arbitrary bytes via an unsafe cast; Rust `&str`
    /// must be valid UTF-8, so invalid UTF-8 data yields `None`.
    pub fn string(&self) -> Option<&'a str> {
        if self.wire_type != WIRE_TYPE_LEN {
            return None;
        }
        std::str::from_utf8(self.data).ok()
    }

    /// Bytes returns bytes value for fc.
    ///
    /// The returned byte slice is valid while the underlying buffer isn't changed.
    ///
    /// `None` is returned if fc doesn't contain bytes value.
    pub fn bytes(&self) -> Option<&'a [u8]> {
        if self.wire_type != WIRE_TYPE_LEN {
            return None;
        }
        Some(self.data)
    }

    /// MessageData returns protobuf message data for fc.
    ///
    /// `None` is returned if fc doesn't contain message data.
    pub fn message_data(&self) -> Option<&'a [u8]> {
        if self.wire_type != WIRE_TYPE_LEN {
            return None;
        }
        Some(self.data)
    }

    /// Fixed32 returns fixed32 value for fc.
    ///
    /// `None` is returned if fc doesn't contain fixed32 value.
    pub fn fixed32(&self) -> Option<u32> {
        if self.wire_type != WIRE_TYPE_I32 {
            return None;
        }
        Some(must_get_uint32(self.int_value))
    }

    /// Sfixed32 returns sfixed32 value for fc.
    ///
    /// `None` is returned if fc doesn't contain sfixed32 value.
    pub fn sfixed32(&self) -> Option<i32> {
        if self.wire_type != WIRE_TYPE_I32 {
            return None;
        }
        Some(must_get_int32(self.int_value))
    }

    /// Float returns float value for fc.
    ///
    /// `None` is returned if fc doesn't contain float value.
    pub fn float(&self) -> Option<f32> {
        if self.wire_type != WIRE_TYPE_I32 {
            return None;
        }
        Some(f32::from_bits(must_get_uint32(self.int_value)))
    }

    fn get_field(
        &mut self,
        src: &'a [u8],
        field_num: u32,
        needed_wire_type: WireType,
    ) -> Result<bool, String> {
        let ok = self.field_by_num(src, field_num)?;
        if !ok {
            return Ok(false);
        }
        if self.wire_type != needed_wire_type {
            return Err(format!(
                "fieldNum={field_num} contains unexpected wireType; got {}; want {}",
                wire_type_string(self.wire_type),
                wire_type_string(needed_wire_type)
            ));
        }
        Ok(true)
    }
}

/// UnmarshalMessageLen unmarshals protobuf message length from src.
///
/// It returns the message length and the tail left after unmarshaling message
/// length from src.
///
/// It is expected that src is marshaled with [`Marshaler::marshal_with_len`].
///
/// `None` is returned if message length cannot be unmarshaled from src.
pub fn unmarshal_message_len(src: &[u8]) -> Option<(usize, &[u8])> {
    let (u64v, offset) = uvarint(src);
    if offset <= 0 {
        return None;
    }
    let src = &src[offset as usize..];
    if u64v > i32::MAX as u64 {
        return None;
    }
    Some((u64v as usize, src))
}

/// GetInt32 returns the int32 value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
///
/// This function is useful when only a single value with the given field_num
/// must be obtained from protobuf-encoded src. Otherwise use [`FieldContext`]
/// for obtaining multiple values from protobuf-encoded src.
pub fn get_int32(src: &[u8], field_num: u32) -> Result<Option<i32>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_VARINT)? {
        return Ok(None);
    }
    match to_int32(fc.int_value) {
        Some(n) => Ok(Some(n)),
        None => Err(format!(
            "fieldNum={field_num} contains too big integer {}, which cannot be converted to int32",
            fc.int_value
        )),
    }
}

/// GetInt64 returns the int64 value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_int64(src: &[u8], field_num: u32) -> Result<Option<i64>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_VARINT)? {
        return Ok(None);
    }
    Ok(Some(fc.int_value as i64))
}

/// GetUint32 returns the uint32 value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_uint32(src: &[u8], field_num: u32) -> Result<Option<u32>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_VARINT)? {
        return Ok(None);
    }
    match to_uint32(fc.int_value) {
        Some(n) => Ok(Some(n)),
        None => Err(format!(
            "fieldNum={field_num} contains too big integer {}, which cannot be converted to uint32",
            fc.int_value
        )),
    }
}

/// GetUint64 returns the uint64 value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_uint64(src: &[u8], field_num: u32) -> Result<Option<u64>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_VARINT)? {
        return Ok(None);
    }
    Ok(Some(fc.int_value))
}

/// GetSint32 returns sint32 value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_sint32(src: &[u8], field_num: u32) -> Result<Option<i32>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_VARINT)? {
        return Ok(None);
    }
    match to_uint32(fc.int_value) {
        Some(u32v) => Ok(Some(decode_zig_zag_int32(u32v))),
        None => Err(format!(
            "fieldNum={field_num} contains too big integer {}, which cannot be converted to uint32",
            fc.int_value
        )),
    }
}

/// GetSint64 returns sint64 value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_sint64(src: &[u8], field_num: u32) -> Result<Option<i64>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_VARINT)? {
        return Ok(None);
    }
    Ok(Some(decode_zig_zag_int64(fc.int_value)))
}

/// GetBool returns bool value for the given field_num from protobuf-encoded
/// message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_bool(src: &[u8], field_num: u32) -> Result<Option<bool>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_VARINT)? {
        return Ok(None);
    }
    match to_bool(fc.int_value) {
        Some(b) => Ok(Some(b)),
        None => Err(format!(
            "fieldNum={field_num} contains invalid integer {}, which cannot be converted to bool",
            fc.int_value
        )),
    }
}

/// GetEnum returns enum value for the given field_num from protobuf-encoded
/// message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_enum(src: &[u8], field_num: u32) -> Result<Option<i32>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_VARINT)? {
        return Ok(None);
    }
    match to_int32(fc.int_value) {
        Some(n) => Ok(Some(n)),
        None => Err(format!(
            "fieldNum={field_num} contains invalid integer {}, which cannot be converted to enum",
            fc.int_value
        )),
    }
}

/// GetFixed64 returns fixed64 value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_fixed64(src: &[u8], field_num: u32) -> Result<Option<u64>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_I64)? {
        return Ok(None);
    }
    Ok(Some(fc.int_value))
}

/// GetSfixed64 returns sfixed64 value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_sfixed64(src: &[u8], field_num: u32) -> Result<Option<i64>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_I64)? {
        return Ok(None);
    }
    Ok(Some(fc.int_value as i64))
}

/// GetDouble returns double value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_double(src: &[u8], field_num: u32) -> Result<Option<f64>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_I64)? {
        return Ok(None);
    }
    Ok(Some(f64::from_bits(fc.int_value)))
}

/// GetString returns string value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
/// The returned string is valid until src is changed.
///
/// PORT NOTE: invalid UTF-8 data yields an error (Go allows arbitrary bytes in
/// strings via an unsafe cast).
pub fn get_string(src: &[u8], field_num: u32) -> Result<Option<&str>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_LEN)? {
        return Ok(None);
    }
    match std::str::from_utf8(fc.data) {
        Ok(s) => Ok(Some(s)),
        Err(_) => Err(format!(
            "fieldNum={field_num} contains invalid UTF-8 string data"
        )),
    }
}

/// GetBytes returns bytes slice for the given field_num from protobuf-encoded
/// message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
/// The returned bytes slice is valid until src is changed.
pub fn get_bytes(src: &[u8], field_num: u32) -> Result<Option<&[u8]>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_LEN)? {
        return Ok(None);
    }
    Ok(Some(fc.data))
}

/// GetMessageData returns message data for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
/// The returned message data is valid until src is changed.
pub fn get_message_data(src: &[u8], field_num: u32) -> Result<Option<&[u8]>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_LEN)? {
        return Ok(None);
    }
    Ok(Some(fc.data))
}

/// GetFixed32 returns fixed32 value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_fixed32(src: &[u8], field_num: u32) -> Result<Option<u32>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_I32)? {
        return Ok(None);
    }
    Ok(Some(must_get_uint32(fc.int_value)))
}

/// GetSfixed32 returns sfixed32 value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_sfixed32(src: &[u8], field_num: u32) -> Result<Option<i32>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_I32)? {
        return Ok(None);
    }
    Ok(Some(must_get_int32(fc.int_value)))
}

/// GetFloat returns float32 value for the given field_num from
/// protobuf-encoded message at src.
///
/// `Ok(None)` is returned if src doesn't contain the given field_num.
pub fn get_float(src: &[u8], field_num: u32) -> Result<Option<f32>, String> {
    let mut fc = FieldContext::default();
    if !fc.get_field(src, field_num, WIRE_TYPE_I32)? {
        return Ok(None);
    }
    Ok(Some(f32::from_bits(must_get_uint32(fc.int_value))))
}

fn decode_zig_zag_int64(u64v: u64) -> i64 {
    ((u64v >> 1) as i64) ^ (((u64v << 63) as i64) >> 63)
}

fn decode_zig_zag_int32(u32v: u32) -> i32 {
    ((u32v >> 1) as i32) ^ (((u32v << 31) as i32) >> 31)
}

// PORT NOTE: Go's private getInt32/getUint32/getBool helpers are renamed to
// to_int32/to_uint32/to_bool in order to avoid clashing with the public
// get_int32/get_uint32/get_bool functions in the same module.
fn to_int32(u64v: u64) -> Option<i32> {
    to_uint32(u64v).map(|u32v| u32v as i32)
}

fn to_uint32(u64v: u64) -> Option<u32> {
    if u64v > u64::from(u32::MAX) {
        return None;
    }
    Some(u64v as u32)
}

fn must_get_int32(u64v: u64) -> i32 {
    must_get_uint32(u64v) as i32
}

fn must_get_uint32(u64v: u64) -> u32 {
    match to_uint32(u64v) {
        Some(u32v) => u32v,
        None => panic!("BUG: cannot get uint32 from {u64v}"),
    }
}

fn to_bool(u64v: u64) -> Option<bool> {
    match u64v {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    }
}

/// Mirrors Go `encoding/binary.Uvarint`: returns the decoded u64 and the
/// number of bytes read. The size is 0 when src is too small and negative on
/// 64-bit overflow.
fn uvarint(buf: &[u8]) -> (u64, isize) {
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    for (i, &b) in buf.iter().enumerate() {
        if i == 10 {
            // Overflow.
            return (0, -((i + 1) as isize));
        }
        if b < 0x80 {
            if i == 9 && b > 1 {
                // Overflow.
                return (0, -((i + 1) as isize));
            }
            return (x | (u64::from(b) << s), (i + 1) as isize);
        }
        x |= u64::from(b & 0x7f) << s;
        s += 7;
    }
    (0, 0)
}

// ---------------------------------------------------------------------------
// writer.go
// ---------------------------------------------------------------------------

/// MarshalerPool is a pool of [`Marshaler`] structs.
///
/// PORT NOTE: Go wraps `sync.Pool`; the port uses a `Mutex<Vec<Marshaler>>`,
/// matching the pooling style used elsewhere in the codebase.
pub struct MarshalerPool {
    p: Mutex<Vec<Marshaler>>,
}

impl MarshalerPool {
    /// Creates an empty pool.
    pub const fn new() -> Self {
        MarshalerPool {
            p: Mutex::new(Vec::new()),
        }
    }

    /// Get obtains a Marshaler from the pool.
    ///
    /// The returned Marshaler can be returned to the pool via
    /// [`MarshalerPool::put`] after it is no longer needed.
    pub fn get(&self) -> Marshaler {
        self.p.lock().unwrap().pop().unwrap_or_default()
    }

    /// Put returns the given m to the pool.
    pub fn put(&self, mut m: Marshaler) {
        m.reset();
        self.p.lock().unwrap().push(m);
    }
}

impl Default for MarshalerPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Marshaler helps marshaling arbitrary protobuf messages.
///
/// Construct the message with `append_*` functions at
/// [`Marshaler::message_marshaler`] and then call `marshal*` for marshaling
/// the constructed message.
///
/// It is recommended re-cycling Marshaler via [`MarshalerPool`] in order to
/// reduce memory allocations.
#[derive(Default)]
pub struct Marshaler {
    /// buf contains temporary data needed for marshaling the protobuf message.
    buf: Vec<u8>,

    /// fs contains fields for the currently marshaled message.
    fs: Vec<WireField>,

    /// mms contains MessageMarshaler state for the currently marshaled
    /// message. Entry 0 is the root (Go's `Marshaler.mm`).
    mms: Vec<MessageMarshalerState>,
}

/// MessageMarshaler helps constructing a protobuf message for marshaling.
///
/// MessageMarshaler must be obtained via [`Marshaler::message_marshaler`].
///
/// PORT NOTE: Go's `MessageMarshaler` holds a back-pointer to its parent
/// `Marshaler`; the Rust port makes it a borrowing handle (`&mut Marshaler` +
/// state index) instead.
pub struct MessageMarshaler<'a> {
    m: &'a mut Marshaler,
    idx: usize,
}

#[derive(Default)]
struct MessageMarshalerState {
    /// tag contains the protobuf message tag for the given MessageMarshaler.
    tag: u64,

    /// first_field_idx contains the index of the first field in
    /// `Marshaler.fs` which belongs to this MessageMarshaler, or -1.
    first_field_idx: isize,

    /// last_field_idx is the index of the last field in `Marshaler.fs` which
    /// belongs to this MessageMarshaler, or -1.
    last_field_idx: isize,
}

#[derive(Clone, Copy)]
struct WireField {
    /// message_size is the size of the marshaled protobuf message for the
    /// given field.
    message_size: u64,

    /// data_start is the start offset of field data at `Marshaler.buf`.
    data_start: usize,

    /// data_end is the end offset of field data at `Marshaler.buf`.
    data_end: usize,

    /// next_field_idx contains the index of the next field in `Marshaler.fs`,
    /// or -1.
    next_field_idx: isize,

    /// child_message_marshaler_idx contains the index of the child
    /// MessageMarshaler state in `Marshaler.mms`, or -1.
    child_message_marshaler_idx: isize,
}

impl Marshaler {
    /// Returns a fresh Marshaler.
    pub fn new() -> Self {
        Marshaler::default()
    }

    /// Reset resets m, so it can be re-used.
    pub fn reset(&mut self) {
        self.buf.clear();
        self.fs.clear();
        self.mms.clear();
    }

    /// MarshalWithLen marshals m and appends its length together with the
    /// marshaled m to dst.
    ///
    /// I.e. it appends a length-delimited protobuf message to dst. The length
    /// of the resulting message can be read via [`unmarshal_message_len`].
    ///
    /// See also [`Marshaler::marshal`].
    pub fn marshal_with_len(&mut self, dst: &mut Vec<u8>) {
        if self.mms.is_empty() {
            marshal_var_uint64(dst, 0);
            return;
        }
        let first_field_idx = self.mms[0].first_field_idx;
        if first_field_idx >= 0 {
            let message_size = self.init_message_size(first_field_idx as usize);
            marshal_var_uint64(dst, message_size);
            self.marshal_field(first_field_idx as usize, dst);
        }
    }

    /// Marshal appends marshaled protobuf m to dst.
    ///
    /// The marshaled message can be read via [`FieldContext::next_field`].
    ///
    /// See also [`Marshaler::marshal_with_len`].
    pub fn marshal(&mut self, dst: &mut Vec<u8>) {
        if self.mms.is_empty() {
            // Nothing to marshal
            return;
        }
        let first_field_idx = self.mms[0].first_field_idx;
        if first_field_idx >= 0 {
            self.init_message_size(first_field_idx as usize);
            self.marshal_field(first_field_idx as usize, dst);
        }
    }

    /// MessageMarshaler returns the root message marshaler for the given m.
    pub fn message_marshaler(&mut self) -> MessageMarshaler<'_> {
        if self.mms.is_empty() {
            self.new_message_marshaler_index();
        }
        MessageMarshaler { m: self, idx: 0 }
    }

    fn new_message_marshaler_index(&mut self) -> usize {
        self.mms.push(MessageMarshalerState {
            tag: 0,
            first_field_idx: -1,
            last_field_idx: -1,
        });
        self.mms.len() - 1
    }

    fn new_field_index(&mut self) -> usize {
        self.fs.push(WireField {
            message_size: 0,
            data_start: 0,
            data_end: 0,
            next_field_idx: -1,
            child_message_marshaler_idx: -1,
        });
        self.fs.len() - 1
    }

    fn init_message_size(&mut self, field_idx: usize) -> u64 {
        let mut field_idx = field_idx;
        let mut n = 0u64;
        loop {
            let f = self.fs[field_idx];
            if f.child_message_marshaler_idx < 0 {
                n += (f.data_end - f.data_start) as u64;
            } else {
                let child_idx = f.child_message_marshaler_idx as usize;
                let tag = self.mms[child_idx].tag;
                if tag < 0x80 {
                    n += 1;
                } else {
                    n += varuint_len(tag);
                }
                let mut message_size = 0u64;
                let first_field_idx = self.mms[child_idx].first_field_idx;
                if first_field_idx >= 0 {
                    message_size = self.init_message_size(first_field_idx as usize);
                }
                n += message_size;
                if message_size < 0x80 {
                    n += 1;
                } else {
                    n += varuint_len(message_size);
                }
                self.fs[field_idx].message_size = message_size;
            }
            let next_field_idx = self.fs[field_idx].next_field_idx;
            if next_field_idx < 0 {
                return n;
            }
            field_idx = next_field_idx as usize;
        }
    }

    fn marshal_field(&self, field_idx: usize, dst: &mut Vec<u8>) {
        let mut field_idx = field_idx;
        loop {
            let f = self.fs[field_idx];
            if f.child_message_marshaler_idx < 0 {
                dst.extend_from_slice(&self.buf[f.data_start..f.data_end]);
            } else {
                let child_idx = f.child_message_marshaler_idx as usize;
                let tag = self.mms[child_idx].tag;
                let message_size = f.message_size;
                if tag < 0x80 && message_size < 0x80 {
                    dst.push(tag as u8);
                    dst.push(message_size as u8);
                } else {
                    marshal_var_uint64(dst, tag);
                    marshal_var_uint64(dst, message_size);
                }
                let first_field_idx = self.mms[child_idx].first_field_idx;
                if first_field_idx >= 0 {
                    self.marshal_field(first_field_idx as usize, dst);
                }
            }
            let next_field_idx = f.next_field_idx;
            if next_field_idx < 0 {
                return;
            }
            field_idx = next_field_idx as usize;
        }
    }
}

impl MessageMarshaler<'_> {
    /// AppendInt32 appends the given int32 value under the given field_num to mm.
    pub fn append_int32(&mut self, field_num: u32, i32v: i32) {
        self.append_uint64(field_num, u64::from(i32v as u32));
    }

    /// AppendInt64 appends the given int64 value under the given field_num to mm.
    pub fn append_int64(&mut self, field_num: u32, i64v: i64) {
        self.append_uint64(field_num, i64v as u64);
    }

    /// AppendUint32 appends the given uint32 value under the given field_num to mm.
    pub fn append_uint32(&mut self, field_num: u32, u32v: u32) {
        self.append_uint64(field_num, u64::from(u32v));
    }

    /// AppendUint64 appends the given uint64 value under the given field_num to mm.
    pub fn append_uint64(&mut self, field_num: u32, u64v: u64) {
        let tag = make_tag(field_num, WIRE_TYPE_VARINT);

        let dst_len = self.m.buf.len();
        if tag < 0x80 {
            self.m.buf.push(tag as u8);
        } else {
            marshal_var_uint64(&mut self.m.buf, tag);
        }
        marshal_var_uint64(&mut self.m.buf, u64v);
        let dst_end = self.m.buf.len();

        self.append_field(dst_len, dst_end);
    }

    /// AppendSint32 appends the given sint32 value under the given field_num to mm.
    pub fn append_sint32(&mut self, field_num: u32, i32v: i32) {
        let u64v = u64::from(encode_zig_zag_int32(i32v));
        self.append_uint64(field_num, u64v);
    }

    /// AppendSint64 appends the given sint64 value under the given field_num to mm.
    pub fn append_sint64(&mut self, field_num: u32, i64v: i64) {
        let u64v = encode_zig_zag_int64(i64v);
        self.append_uint64(field_num, u64v);
    }

    /// AppendBool appends the given bool value under the given field_num to mm.
    pub fn append_bool(&mut self, field_num: u32, v: bool) {
        self.append_uint64(field_num, u64::from(v));
    }

    /// AppendFixed64 appends fixed64 value under the given field_num to mm.
    pub fn append_fixed64(&mut self, field_num: u32, u64v: u64) {
        let tag = make_tag(field_num, WIRE_TYPE_I64);

        let dst_len = self.m.buf.len();
        if tag < 0x80 {
            self.m.buf.push(tag as u8);
        } else {
            marshal_var_uint64(&mut self.m.buf, tag);
        }
        self.m.buf.extend_from_slice(&u64v.to_le_bytes());
        let dst_end = self.m.buf.len();

        self.append_field(dst_len, dst_end);
    }

    /// AppendSfixed64 appends sfixed64 value under the given field_num to mm.
    pub fn append_sfixed64(&mut self, field_num: u32, i64v: i64) {
        self.append_fixed64(field_num, i64v as u64);
    }

    /// AppendDouble appends double value under the given field_num to mm.
    pub fn append_double(&mut self, field_num: u32, f: f64) {
        self.append_fixed64(field_num, f.to_bits());
    }

    /// AppendString appends string value under the given field_num to mm.
    pub fn append_string(&mut self, field_num: u32, s: &str) {
        self.append_bytes(field_num, s.as_bytes());
    }

    /// AppendBytes appends bytes value under the given field_num to mm.
    ///
    /// PORT NOTE: Go implements `AppendBytes` on top of `AppendString` via an
    /// unsafe cast; the port inverts the delegation since arbitrary bytes are
    /// not a valid Rust `&str`.
    pub fn append_bytes(&mut self, field_num: u32, b: &[u8]) {
        let tag = make_tag(field_num, WIRE_TYPE_LEN);

        let dst_len = self.m.buf.len();
        let b_len = b.len();
        if tag < 0x80 && b_len < 0x80 {
            self.m.buf.push(tag as u8);
            self.m.buf.push(b_len as u8);
        } else {
            marshal_var_uint64(&mut self.m.buf, tag);
            marshal_var_uint64(&mut self.m.buf, b_len as u64);
        }
        self.m.buf.extend_from_slice(b);
        let dst_end = self.m.buf.len();

        self.append_field(dst_len, dst_end);
    }

    /// AppendMessage appends a protobuf message with the given field_num to mm.
    ///
    /// The function returns the MessageMarshaler for constructing the appended
    /// message.
    pub fn append_message(&mut self, field_num: u32) -> MessageMarshaler<'_> {
        let tag = make_tag(field_num, WIRE_TYPE_LEN);

        let f_idx = self.new_field();
        let child_idx = self.m.new_message_marshaler_index();
        self.m.fs[f_idx].child_message_marshaler_idx = child_idx as isize;
        self.m.mms[child_idx].tag = tag;
        MessageMarshaler {
            m: self.m,
            idx: child_idx,
        }
    }

    /// AppendFixed32 appends fixed32 value under the given field_num to mm.
    pub fn append_fixed32(&mut self, field_num: u32, u32v: u32) {
        let tag = make_tag(field_num, WIRE_TYPE_I32);

        let dst_len = self.m.buf.len();
        if tag < 0x80 {
            self.m.buf.push(tag as u8);
        } else {
            marshal_var_uint64(&mut self.m.buf, tag);
        }
        self.m.buf.extend_from_slice(&u32v.to_le_bytes());
        let dst_end = self.m.buf.len();

        self.append_field(dst_len, dst_end);
    }

    /// AppendSfixed32 appends sfixed32 value under the given field_num to mm.
    pub fn append_sfixed32(&mut self, field_num: u32, i32v: i32) {
        self.append_fixed32(field_num, i32v as u32);
    }

    /// AppendFloat appends float value under the given field_num to mm.
    pub fn append_float(&mut self, field_num: u32, f: f32) {
        self.append_fixed32(field_num, f.to_bits());
    }

    fn append_field(&mut self, data_start: usize, data_end: usize) {
        let last_field_idx = self.m.mms[self.idx].last_field_idx;
        if last_field_idx >= 0 {
            let f = &mut self.m.fs[last_field_idx as usize];
            if f.child_message_marshaler_idx == -1 && f.data_end == data_start {
                f.data_end = data_end;
                return;
            }
        }
        let f_idx = self.new_field();
        let f = &mut self.m.fs[f_idx];
        f.data_start = data_start;
        f.data_end = data_end;
    }

    fn new_field(&mut self) -> usize {
        let f_idx = self.m.new_field_index();
        let last_field_idx = self.m.mms[self.idx].last_field_idx;
        if last_field_idx >= 0 {
            self.m.fs[last_field_idx as usize].next_field_idx = f_idx as isize;
        } else {
            self.m.mms[self.idx].first_field_idx = f_idx as isize;
        }
        self.m.mms[self.idx].last_field_idx = f_idx as isize;
        f_idx
    }
}

fn marshal_var_uint64(dst: &mut Vec<u8>, u64v: u64) {
    let mut u64v = u64v;
    if u64v < 0x80 {
        // Fast path
        dst.push(u64v as u8);
        return;
    }
    while u64v > 0x7f {
        dst.push(0x80 | (u64v as u8));
        u64v >>= 7;
    }
    dst.push(u64v as u8);
}

fn encode_zig_zag_int64(i64v: i64) -> u64 {
    ((i64v << 1) ^ (i64v >> 63)) as u64
}

fn encode_zig_zag_int32(i32v: i32) -> u32 {
    ((i32v << 1) ^ (i32v >> 31)) as u32
}

fn make_tag(field_num: u32, wt: WireType) -> u64 {
    (u64::from(field_num) << 3) | u64::from(wt)
}

/// varuintLen returns the number of bytes needed for varuint-encoding of u64.
///
/// Note that it returns 0 for u64=0, so this case must be handled separately.
fn varuint_len(u64v: u64) -> u64 {
    u64::from((64 - u64v.leading_zeros()).div_ceil(7))
}

// ---------------------------------------------------------------------------
// Tests
//
// PORT NOTE: the vendored Go easyproto sources ship without *_test.go files,
// so these round-trip tests are written against the ported public API instead
// of being line-for-line ports.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_marshal_empty() {
        let mut m = Marshaler::new();
        let mut dst = Vec::new();
        m.marshal(&mut dst);
        assert!(dst.is_empty());

        let mut dst = Vec::new();
        m.marshal_with_len(&mut dst);
        assert_eq!(dst, vec![0]);
        let (msg_len, tail) = unmarshal_message_len(&dst).unwrap();
        assert_eq!(msg_len, 0);
        assert!(tail.is_empty());
    }

    #[test]
    fn test_marshal_golden_bytes() {
        // Classic protobuf example: field 1 = varint 150 -> [0x08, 0x96, 0x01].
        let mut m = Marshaler::new();
        m.message_marshaler().append_int64(1, 150);
        let mut dst = Vec::new();
        m.marshal(&mut dst);
        assert_eq!(dst, vec![0x08, 0x96, 0x01]);

        // field 2 = string "testing" -> [0x12, 0x07, b"testing"...].
        let mut m = Marshaler::new();
        m.message_marshaler().append_string(2, "testing");
        let mut dst = Vec::new();
        m.marshal(&mut dst);
        let mut expected = vec![0x12, 0x07];
        expected.extend_from_slice(b"testing");
        assert_eq!(dst, expected);
    }

    #[test]
    fn test_scalar_roundtrip() {
        let mut m = Marshaler::new();
        {
            let mut mm = m.message_marshaler();
            mm.append_int32(1, -12345);
            mm.append_int64(2, -1234567890123);
            mm.append_uint32(3, 12345);
            mm.append_uint64(4, 1234567890123);
            mm.append_sint32(5, -54321);
            mm.append_sint64(6, -9876543210);
            mm.append_bool(7, true);
            mm.append_fixed64(8, 0x0123456789abcdef);
            mm.append_sfixed64(9, -1);
            mm.append_double(10, 3.5);
            mm.append_string(11, "foo bar");
            mm.append_bytes(12, b"\x00\x01\x02");
            mm.append_fixed32(13, 0xdeadbeef);
            mm.append_sfixed32(14, -2);
            mm.append_float(15, -1.25);
        }
        let mut data = Vec::new();
        m.marshal(&mut data);

        let mut fc = FieldContext::default();
        let mut src: &[u8] = &data;
        let mut seen = 0;
        while !src.is_empty() {
            src = fc.next_field(src).unwrap();
            seen += 1;
            match fc.field_num {
                1 => assert_eq!(fc.int32(), Some(-12345)),
                2 => assert_eq!(fc.int64(), Some(-1234567890123)),
                3 => assert_eq!(fc.uint32(), Some(12345)),
                4 => assert_eq!(fc.uint64(), Some(1234567890123)),
                5 => assert_eq!(fc.sint32(), Some(-54321)),
                6 => assert_eq!(fc.sint64(), Some(-9876543210)),
                7 => assert_eq!(fc.bool_value(), Some(true)),
                8 => assert_eq!(fc.fixed64(), Some(0x0123456789abcdef)),
                9 => assert_eq!(fc.sfixed64(), Some(-1)),
                10 => assert_eq!(fc.double(), Some(3.5)),
                11 => assert_eq!(fc.string(), Some("foo bar")),
                12 => assert_eq!(fc.bytes(), Some(&b"\x00\x01\x02"[..])),
                13 => assert_eq!(fc.fixed32(), Some(0xdeadbeef)),
                14 => assert_eq!(fc.sfixed32(), Some(-2)),
                15 => assert_eq!(fc.float(), Some(-1.25)),
                n => panic!("unexpected fieldNum={n}"),
            }
        }
        assert_eq!(seen, 15);
    }

    #[test]
    fn test_wire_type_mismatch_returns_none() {
        let mut m = Marshaler::new();
        m.message_marshaler().append_string(1, "abc");
        let mut data = Vec::new();
        m.marshal(&mut data);

        let mut fc = FieldContext::default();
        let tail = fc.next_field(&data).unwrap();
        assert!(tail.is_empty());
        assert_eq!(fc.int64(), None);
        assert_eq!(fc.fixed64(), None);
        assert_eq!(fc.fixed32(), None);
        assert_eq!(fc.string(), Some("abc"));
    }

    #[test]
    fn test_nested_messages() {
        // message { child = 1 { string name = 1; uint64 n = 2 }; string tail = 2 }
        let mut m = Marshaler::new();
        {
            let mut mm = m.message_marshaler();
            {
                let mut child = mm.append_message(1);
                child.append_string(1, "child-name");
                child.append_uint64(2, 42);
            }
            mm.append_string(2, "tail");
        }
        let mut data = Vec::new();
        m.marshal(&mut data);

        let child_data = get_message_data(&data, 1).unwrap().unwrap();
        assert_eq!(get_string(child_data, 1).unwrap(), Some("child-name"));
        assert_eq!(get_uint64(child_data, 2).unwrap(), Some(42));
        assert_eq!(get_string(&data, 2).unwrap(), Some("tail"));
        // Missing field.
        assert_eq!(get_string(&data, 3).unwrap(), None);
    }

    #[test]
    fn test_repeated_messages() {
        let mut m = Marshaler::new();
        {
            let mut mm = m.message_marshaler();
            for i in 0..3u64 {
                let mut child = mm.append_message(1);
                child.append_uint64(1, i);
            }
        }
        let mut data = Vec::new();
        m.marshal(&mut data);

        let mut fc = FieldContext::default();
        let mut src: &[u8] = &data;
        let mut values = Vec::new();
        while !src.is_empty() {
            src = fc.next_field(src).unwrap();
            assert_eq!(fc.field_num, 1);
            let msg = fc.message_data().unwrap();
            values.push(get_uint64(msg, 1).unwrap().unwrap());
        }
        assert_eq!(values, vec![0, 1, 2]);
    }

    #[test]
    fn test_large_field_num_and_long_string() {
        // Exercise the multi-byte varint tag and length paths.
        let long = "x".repeat(300);
        let mut m = Marshaler::new();
        {
            let mut mm = m.message_marshaler();
            mm.append_string(12345, &long);
            mm.append_uint64(67890, u64::MAX);
        }
        let mut data = Vec::new();
        m.marshal(&mut data);

        assert_eq!(get_string(&data, 12345).unwrap(), Some(long.as_str()));
        assert_eq!(get_uint64(&data, 67890).unwrap(), Some(u64::MAX));
    }

    #[test]
    fn test_marshal_with_len_roundtrip() {
        let mut m = Marshaler::new();
        m.message_marshaler().append_string(1, "hello");
        let mut data = Vec::new();
        m.marshal_with_len(&mut data);

        let (msg_len, tail) = unmarshal_message_len(&data).unwrap();
        assert_eq!(msg_len, tail.len());
        assert_eq!(get_string(&tail[..msg_len], 1).unwrap(), Some("hello"));
    }

    #[test]
    fn test_next_field_errors() {
        let mut fc = FieldContext::default();
        // Empty message.
        assert!(fc.next_field(&[]).is_err());
        // Truncated len field: field 1, wireType=Len, claims 10 bytes.
        assert!(fc.next_field(&[0x0a, 0x0a, 0x01]).is_err());
        // Unknown wire type 3.
        assert!(fc.next_field(&[0x0b, 0x00]).is_err());
        // Truncated i64.
        assert!(fc.next_field(&[0x09, 0x01, 0x02]).is_err());
        // Truncated i32.
        assert!(fc.next_field(&[0x0d, 0x01]).is_err());
    }

    #[test]
    fn test_zig_zag() {
        for i in [-1000000i64, -1, 0, 1, 1000000, i64::MIN, i64::MAX] {
            assert_eq!(decode_zig_zag_int64(encode_zig_zag_int64(i)), i);
        }
        for i in [-1000000i32, -1, 0, 1, 1000000, i32::MIN, i32::MAX] {
            assert_eq!(decode_zig_zag_int32(encode_zig_zag_int32(i)), i);
        }
    }

    #[test]
    fn test_marshaler_pool_reuse() {
        static MP: MarshalerPool = MarshalerPool::new();

        let mut m = MP.get();
        m.message_marshaler().append_string(1, "first");
        let mut data = Vec::new();
        m.marshal(&mut data);
        assert_eq!(get_string(&data, 1).unwrap(), Some("first"));
        MP.put(m);

        // The pooled Marshaler must be fully reset.
        let mut m = MP.get();
        m.message_marshaler().append_string(2, "second");
        let mut data = Vec::new();
        m.marshal(&mut data);
        assert_eq!(get_string(&data, 1).unwrap(), None);
        assert_eq!(get_string(&data, 2).unwrap(), Some("second"));
        MP.put(m);
    }

    #[test]
    fn test_field_by_num() {
        let mut m = Marshaler::new();
        {
            let mut mm = m.message_marshaler();
            mm.append_uint64(1, 10);
            mm.append_uint64(2, 20);
            mm.append_uint64(3, 30);
        }
        let mut data = Vec::new();
        m.marshal(&mut data);

        let mut fc = FieldContext::default();
        assert!(fc.field_by_num(&data, 2).unwrap());
        assert_eq!(fc.uint64(), Some(20));
        assert!(!fc.field_by_num(&data, 5).unwrap());
    }
}

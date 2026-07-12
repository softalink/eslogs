//! Port of EsLogs `lib/logstorage/stream_id.go`.

use std::fmt;

use crate::tenant_id::TenantID;
use crate::u128::U128;

/// StreamID is an internal id of log stream.
///
/// Blocks are ordered by streamID inside parts.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamID {
    /// tenant_id is a tenant id for the given stream.
    /// It is located at the beginning of streamID in order
    /// to physically group blocks for the same tenants on the storage.
    pub tenant_id: TenantID,

    /// id is internal id, which uniquely identifies the stream in the tenant by its labels.
    /// It is calculated as a hash of canonically sorted stream labels.
    ///
    /// Streams with identical sets of labels, which belong to distinct tenants, have the same id.
    pub id: U128,
}

impl StreamID {
    /// Resets sid for subsequent reuse.
    pub fn reset(&mut self) {
        *self = StreamID::default();
    }

    /// Appends the `_stream_id` value for the given sid to dst.
    pub fn marshal_string(&self, dst: &mut Vec<u8>) {
        self.tenant_id.marshal_string(dst);
        self.id.marshal_string(dst);
    }

    /// Tries unmarshaling sid from the hex string s.
    pub fn try_unmarshal_from_string(&mut self, s: &str) -> bool {
        let Some(data) = decode_hex(s) else {
            return false;
        };
        match self.unmarshal(&data) {
            Ok(tail) => tail.is_empty(),
            Err(_) => false,
        }
    }

    /// Returns true if self is less than a.
    pub fn less(&self, a: &StreamID) -> bool {
        if !self.tenant_id.equal(&a.tenant_id) {
            return self.tenant_id.less(&a.tenant_id);
        }
        self.id.less(&a.id)
    }

    /// Returns true if self equals to a.
    pub fn equal(&self, a: &StreamID) -> bool {
        if !self.tenant_id.equal(&a.tenant_id) {
            return false;
        }
        self.id.equal(&a.id)
    }

    /// Appends the marshaled sid to dst.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        self.tenant_id.marshal(dst);
        self.id.marshal(dst);
    }

    /// Unmarshals sid from src and returns the tail from src.
    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        let tail = self.tenant_id.unmarshal(src)?;
        let tail = self.id.unmarshal(tail)?;
        Ok(tail)
    }
}

impl fmt::Display for StreamID {
    /// Returns human-readable representation for sid.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(tenant_id={}, id={})", self.tenant_id, self.id)
    }
}

/// PORT NOTE: replaces Go's `hex.DecodeString`; returns None on odd length or
/// non-hex characters like Go returns an error.
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    s.as_bytes()
        .chunks(2)
        .map(|pair| {
            let hi = (pair[0] as char).to_digit(16)?;
            let lo = (pair[1] as char).to_digit(16)?;
            Some((hi * 16 + lo) as u8)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id_marshal_unmarshal_string() {
        let f = |sid: &StreamID, result_expected: &str| {
            let mut result = Vec::new();
            sid.marshal_string(&mut result);
            let result = String::from_utf8(result).unwrap();

            assert_eq!(
                result, result_expected,
                "unexpected result\ngot\n{result:?}\nwant\n{result_expected:?}"
            );

            let mut sid2 = StreamID::default();
            assert!(
                sid2.try_unmarshal_from_string(&result),
                "cannot unmarshal streamID from {result:?}"
            );

            let mut result2 = Vec::new();
            sid2.marshal_string(&mut result2);
            let result2 = String::from_utf8(result2).unwrap();
            assert_eq!(
                result, result2,
                "unexpected marshaled streamID; got {result2}; want {result}"
            );
        };

        f(
            &StreamID::default(),
            "000000000000000000000000000000000000000000000000",
        );
        f(
            &StreamID {
                tenant_id: TenantID {
                    account_id: 123,
                    project_id: 456,
                },
                id: U128 { lo: 89, hi: 344334 },
            },
            "0000007b000001c8000000000005410e0000000000000059",
        );
    }

    #[test]
    fn test_stream_id_marshal_unmarshal() {
        let f = |sid: &StreamID, marshaled_len: usize| {
            let mut data = Vec::new();
            sid.marshal(&mut data);
            assert_eq!(
                data.len(),
                marshaled_len,
                "unexpected length of marshaled streamID; got {}; want {marshaled_len}",
                data.len()
            );
            let mut sid2 = StreamID::default();
            let tail = match sid2.unmarshal(&data) {
                Ok(tail) => tail,
                Err(err) => panic!("unexpected error on unmarshal({sid}): {err}"),
            };
            assert!(
                tail.is_empty(),
                "unexpected non-empty tail on unmarshal({sid}): {tail:X?}"
            );
            assert_eq!(
                *sid, sid2,
                "unexpected result on unmarshal; got {sid2}; want {sid}"
            );
            let s1 = sid.to_string();
            let s2 = sid2.to_string();
            assert_eq!(
                s1, s2,
                "unexpected string result on unmarshal; got {s2}; want {s1}"
            );
        };
        f(&StreamID::default(), 24);
        f(
            &StreamID {
                tenant_id: TenantID {
                    account_id: 123,
                    project_id: 456,
                },
                id: U128 { lo: 89, hi: 344334 },
            },
            24,
        );
    }

    #[test]
    fn test_stream_id_unmarshal_failure() {
        // PORT NOTE: Go additionally checks that the returned tail equals the
        // original data on error; the port's Result carries no tail.
        let f = |data: &[u8]| {
            let mut sid = StreamID::default();
            assert!(sid.unmarshal(data).is_err(), "expecting non-nil error");
        };
        f(&[]);
        f(b"foo");
        f(b"1234567890");
    }

    #[test]
    fn test_stream_id_less_equal() {
        // compare equal values
        let sid1 = StreamID::default();
        let sid2 = StreamID::default();
        assert!(!sid1.less(&sid2), "less for equal values must return false");
        assert!(!sid2.less(&sid1), "less for equal values must return false");
        assert!(
            sid1.equal(&sid2),
            "unexpected equal({sid1}, {sid2}) result; got false; want true"
        );
        assert!(
            sid2.equal(&sid1),
            "unexpected equal({sid2}, {sid1}) result; got false; want true"
        );

        let sid1 = StreamID {
            tenant_id: TenantID {
                account_id: 1,
                project_id: 2,
            },
            id: U128 { hi: 123, lo: 456 },
        };
        let sid2 = StreamID {
            tenant_id: TenantID {
                account_id: 1,
                project_id: 2,
            },
            id: U128 { hi: 123, lo: 456 },
        };
        assert!(!sid1.less(&sid2), "less for equal values must return false");
        assert!(!sid2.less(&sid1), "less for equal values must return false");
        assert!(
            sid1.equal(&sid2),
            "unexpected equal({sid1}, {sid2}) result; got false; want true"
        );
        assert!(
            sid2.equal(&sid1),
            "unexpected equal({sid2}, {sid1}) result; got false; want true"
        );

        // compare unequal values
        let sid1 = StreamID {
            id: U128 { lo: 456, hi: 0 },
            ..StreamID::default()
        };
        let sid2 = StreamID {
            id: U128 { hi: 123, lo: 0 },
            ..StreamID::default()
        };
        assert!(
            sid1.less(&sid2),
            "unexpected result for less({sid1}, {sid2}); got false; want true"
        );
        assert!(
            !sid2.less(&sid1),
            "unexpected result for less({sid2}, {sid1}); got true; want false"
        );
        assert!(
            !sid1.equal(&sid2),
            "unexpected result for equal({sid1}, {sid2}); got true; want false"
        );

        let sid1 = StreamID {
            id: U128 { hi: 123, lo: 456 },
            ..StreamID::default()
        };
        let sid2 = StreamID {
            tenant_id: TenantID {
                account_id: 123,
                project_id: 0,
            },
            ..StreamID::default()
        };
        assert!(
            sid1.less(&sid2),
            "unexpected result for less({sid1}, {sid2}); got false; want true"
        );
        assert!(
            !sid2.less(&sid1),
            "unexpected result for less({sid2}, {sid1}); got true; want false"
        );
        assert!(
            !sid1.equal(&sid2),
            "unexpected result for equal({sid1}, {sid2}); got true; want false"
        );
    }

    #[test]
    fn test_stream_id_reset() {
        let mut sid = StreamID {
            tenant_id: TenantID {
                account_id: 123,
                project_id: 456,
            },
            id: U128 { hi: 234, lo: 9843 },
        };
        sid.reset();
        let sid_zero = StreamID::default();
        assert_eq!(sid, sid_zero, "non-zero streamID after reset(): {sid}");
    }
}

//! Port of EsLogs `lib/logstorage/u128.go`.

use std::fmt;

use esl_common::encoding;

/// U128 is 128-bit uint number.
///
/// It is used as an unique id of stream.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct U128 {
    pub hi: u64,
    pub lo: u64,
}

impl fmt::Display for U128 {
    /// Returns human-readable representation of u.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{hi={},lo={}}}", self.hi, self.lo)
    }
}

impl U128 {
    /// Returns true if u is less than a.
    pub fn less(&self, a: &U128) -> bool {
        if self.hi != a.hi {
            return self.hi < a.hi;
        }
        self.lo < a.lo
    }

    /// Returns true if u equals to a.
    pub fn equal(&self, a: &U128) -> bool {
        self.hi == a.hi && self.lo == a.lo
    }

    /// Appends the hex string representation of u to dst.
    pub fn marshal_string(&self, dst: &mut Vec<u8>) {
        marshal_uint64_hex(dst, self.hi);
        marshal_uint64_hex(dst, self.lo);
    }

    /// Appends the marshaled u to dst.
    pub fn marshal(&self, dst: &mut Vec<u8>) {
        encoding::marshal_uint64(dst, self.hi);
        encoding::marshal_uint64(dst, self.lo);
    }

    /// Unmarshals u from src and returns the tail.
    ///
    /// PORT NOTE: Go returns `(tail, error)` where the tail equals `src` on
    /// error; the Rust port returns `Result<tail, String>` and leaves `src`
    /// untouched on error.
    pub fn unmarshal<'a>(&mut self, src: &'a [u8]) -> Result<&'a [u8], String> {
        if src.len() < 16 {
            return Err(format!(
                "cannot unmarshal u128 from {} bytes; need at least 16 bytes",
                src.len()
            ));
        }
        self.hi = encoding::unmarshal_uint64(&src[..8]);
        self.lo = encoding::unmarshal_uint64(&src[8..]);
        Ok(&src[16..])
    }
}

/// Appends the fixed-width hex representation of n to dst.
pub fn marshal_uint64_hex(dst: &mut Vec<u8>, n: u64) {
    marshal_byte_hex(dst, (n >> 56) as u8);
    marshal_byte_hex(dst, (n >> 48) as u8);
    marshal_byte_hex(dst, (n >> 40) as u8);
    marshal_byte_hex(dst, (n >> 32) as u8);
    marshal_byte_hex(dst, (n >> 24) as u8);
    marshal_byte_hex(dst, (n >> 16) as u8);
    marshal_byte_hex(dst, (n >> 8) as u8);
    marshal_byte_hex(dst, n as u8);
}

fn marshal_byte_hex(dst: &mut Vec<u8>, x: u8) {
    dst.push(HEX_BYTE_MAP[((x >> 4) & 15) as usize]);
    dst.push(HEX_BYTE_MAP[(x & 15) as usize]);
}

static HEX_BYTE_MAP: [u8; 16] = *b"0123456789abcdef";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u128_marshal_unmarshal() {
        fn f(u: &U128, marshaled_len: usize) {
            let mut data = Vec::new();
            u.marshal(&mut data);
            assert_eq!(
                data.len(),
                marshaled_len,
                "unexpected length of marshaled u128; got {}; want {}",
                data.len(),
                marshaled_len
            );
            let mut u2 = U128::default();
            let tail = u2
                .unmarshal(&data)
                .unwrap_or_else(|err| panic!("unexpected error at unmarshal({u}): {err}"));
            assert!(
                tail.is_empty(),
                "unexpected non-empty tail after unmarshal({u}): {tail:X?}"
            );
            assert_eq!(
                u, &u2,
                "unexpected value obtained from unmarshal({u}); got {u2}; want {u}"
            );
            let s1 = u.to_string();
            let s2 = u2.to_string();
            assert_eq!(
                s1, s2,
                "unexpected string representation after unmarshal; got {s2}; want {s1}"
            );
        }
        f(&U128::default(), 16);
        f(&U128 { hi: 123, lo: 456 }, 16);
    }

    #[test]
    fn test_u128_unmarshal_failure() {
        fn f(data: &[u8]) {
            let mut u = U128::default();
            let result = u.unmarshal(data);
            assert!(result.is_err(), "expecting non-nil error");
            // PORT NOTE: the Go test also verifies that the returned tail
            // equals the original data on error; the Rust port returns no
            // tail on error and `data` stays untouched by construction.
        }
        f(&[]);
        f(b"foo");
    }

    #[test]
    fn test_u128_less_equal() {
        // compare equal values
        let u1 = U128::default();
        let u2 = U128::default();
        assert!(!u1.less(&u2), "less for equal values must return false");
        assert!(!u2.less(&u1), "less for equal values must return false");
        assert!(
            u1.equal(&u2),
            "unexpected equal({u1}, {u2}) result; got false; want true"
        );
        assert!(
            u2.equal(&u1),
            "unexpected equal({u2}, {u1}) result; got false; want true"
        );

        let u1 = U128 { hi: 123, lo: 456 };
        let u2 = U128 { hi: 123, lo: 456 };
        assert!(!u1.less(&u2), "less for equal values must return false");
        assert!(!u2.less(&u1), "less for equal values must return false");
        assert!(
            u1.equal(&u2),
            "unexpected equal({u1}, {u2}) result; got false; want true"
        );
        assert!(
            u2.equal(&u1),
            "unexpected equal({u2}, {u1}) result; got false; want true"
        );

        // compare unequal values
        let u1 = U128 { hi: 0, lo: 456 };
        let u2 = U128 { hi: 123, lo: 0 };
        assert!(
            u1.less(&u2),
            "unexpected result for less({u1}, {u2}); got false; want true"
        );
        assert!(
            !u2.less(&u1),
            "unexpected result for less({u2}, {u1}); got true; want false"
        );
        assert!(
            !u1.equal(&u2),
            "unexpected result for equal({u1}, {u2}); got true; want false"
        );

        let u1 = U128 { hi: 123, lo: 0 };
        let u2 = U128 { hi: 123, lo: 456 };
        assert!(
            u1.less(&u2),
            "unexpected result for less({u1}, {u2}); got false; want true"
        );
        assert!(
            !u2.less(&u1),
            "unexpected result for less({u2}, {u1}); got true; want false"
        );
        assert!(
            !u1.equal(&u2),
            "unexpected result for equal({u1}, {u2}); got true; want false"
        );
    }

    // PORT NOTE: the Go package has no test for marshalString /
    // marshalUint64Hex; this is a Rust-side golden check to pin the hex
    // format, since stream ids marshaled this way are part of on-disk data.
    #[test]
    fn test_u128_marshal_string() {
        let u = U128 {
            hi: 0x0123456789abcdef,
            lo: 0xfedcba9876543210,
        };
        let mut dst = Vec::new();
        u.marshal_string(&mut dst);
        assert_eq!(dst, b"0123456789abcdeffedcba9876543210");
    }
}

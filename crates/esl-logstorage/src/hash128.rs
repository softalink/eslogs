//! Port of EsLogs `lib/logstorage/hash128.go`.

use xxhash_rust::xxh64::Xxh64;

use crate::u128::U128;

/// Returns 128-bit xxhash-based hash of data.
///
/// The output is bit-identical to the Go implementation based on
/// `github.com/cespare/xxhash/v2` (xxh64 with seed 0).
pub fn hash128(data: &[u8]) -> U128 {
    // PORT NOTE: Go pools `*xxhash.Digest` objects via `sync.Pool`; the Rust
    // `Xxh64` state is a small plain struct constructed on the stack, so no
    // pool is needed.
    let mut h = Xxh64::new(0);
    h.update(data);
    let hi = h.digest();
    h.update(MAGIC_SUFFIX_FOR_HASH);
    let lo = h.digest();

    U128 { hi, lo }
}

const MAGIC_SUFFIX_FOR_HASH: &[u8] = b"magic!";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash128() {
        fn f(data: &str, hash_expected: U128) {
            let h = hash128(data.as_bytes());
            assert!(
                h.equal(&hash_expected),
                "unexpected hash; got {h}; want {hash_expected}"
            );
        }
        f(
            "",
            U128 {
                hi: 17241709254077376921,
                lo: 13138662262368978769,
            },
        );

        f(
            "abc",
            U128 {
                hi: 4952883123889572249,
                lo: 3255951525518405514,
            },
        );
    }
}

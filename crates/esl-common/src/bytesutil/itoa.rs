//! Port of `lib/bytesutil/itoa.go`.

use std::sync::Arc;

use super::internstring::intern_string;

/// Returns string representation of `n`.
///
/// This function doesn't allocate memory on repeated calls for the same `n`.
///
/// PORT NOTE: Go formats into a pooled ByteBuffer before interning; the port
/// formats into a transient `String` — interning still deduplicates repeated
/// calls, which is the observable behavior. Go's `int` maps to `i64`.
pub fn itoa(n: i64) -> Arc<str> {
    intern_string(&n.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_itoa() {
        fn f(n: i64, result_expected: &str) {
            for _ in 0..5 {
                let result = itoa(n);
                assert_eq!(
                    &*result, result_expected,
                    "unexpected result for itoa({n}); got {result:?}; want {result_expected:?}"
                );
            }
        }
        f(0, "0");
        f(1, "1");
        f(-123, "-123");
        f(343432, "343432");
    }
}

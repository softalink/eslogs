//! Port of Softalink LLC `lib/fastnum`.

const DATA_LEN: usize = 8 * 1024;

static INT64_ZEROS: [i64; DATA_LEN] = [0; DATA_LEN];
static INT64_ONES: [i64; DATA_LEN] = [1; DATA_LEN];
static FLOAT64_ZEROS: [f64; DATA_LEN] = [0.0; DATA_LEN];
static FLOAT64_ONES: [f64; DATA_LEN] = [1.0; DATA_LEN];

/// AppendInt64Zeros appends items zeros to dst.
///
/// It is faster than the corresponding loop.
pub fn append_int64_zeros(dst: &mut Vec<i64>, items: usize) {
    append_int64_data(dst, items, &INT64_ZEROS)
}

/// AppendInt64Ones appends items ones to dst.
///
/// It is faster than the corresponding loop.
pub fn append_int64_ones(dst: &mut Vec<i64>, items: usize) {
    append_int64_data(dst, items, &INT64_ONES)
}

/// AppendFloat64Zeros appends items zeros to dst.
///
/// It is faster than the corresponding loop.
pub fn append_float64_zeros(dst: &mut Vec<f64>, items: usize) {
    append_float64_data(dst, items, &FLOAT64_ZEROS)
}

/// AppendFloat64Ones appends items ones to dst.
///
/// It is faster than the corresponding loop.
pub fn append_float64_ones(dst: &mut Vec<f64>, items: usize) {
    append_float64_data(dst, items, &FLOAT64_ONES)
}

/// IsInt64Zeros checks whether a contains only zeros.
pub fn is_int64_zeros(a: &[i64]) -> bool {
    is_int64_data(a, &INT64_ZEROS)
}

/// IsInt64Ones checks whether a contains only ones.
pub fn is_int64_ones(a: &[i64]) -> bool {
    is_int64_data(a, &INT64_ONES)
}

/// IsFloat64Zeros checks whether a contains only zeros.
pub fn is_float64_zeros(a: &[f64]) -> bool {
    is_float64_data(a, &FLOAT64_ZEROS)
}

/// IsFloat64Ones checks whether a contains only ones.
pub fn is_float64_ones(a: &[f64]) -> bool {
    is_float64_data(a, &FLOAT64_ONES)
}

fn append_int64_data(dst: &mut Vec<i64>, mut items: usize, src: &[i64; DATA_LEN]) {
    while items > 0 {
        let n = src.len().min(items);
        dst.extend_from_slice(&src[..n]);
        items -= n;
    }
}

fn append_float64_data(dst: &mut Vec<f64>, mut items: usize, src: &[f64; DATA_LEN]) {
    while items > 0 {
        let n = src.len().min(items);
        dst.extend_from_slice(&src[..n]);
        items -= n;
    }
}

// PORT NOTE: Go reinterprets the slices as raw bytes via `unsafe` and compares
// them with `bytes.Equal`; the `len(data) == 8*1024` runtime panic is
// enforced statically here by the `[_; DATA_LEN]` parameter type. Comparing
// i64 chunks directly compiles to the same memcmp without unsafe code.
fn is_int64_data(a: &[i64], data: &[i64; DATA_LEN]) -> bool {
    a.chunks(DATA_LEN).all(|x| x == &data[..x.len()])
}

// PORT NOTE: Go compares raw bytes, so -0.0 does not equal 0.0 and NaN equals
// an identical NaN. Comparing f64 bit patterns preserves this behavior, which
// `f64 ==` would not.
fn is_float64_data(a: &[f64], data: &[f64; DATA_LEN]) -> bool {
    a.chunks(DATA_LEN).all(|x| {
        x.iter()
            .zip(&data[..x.len()])
            .all(|(p, q)| p.to_bits() == q.to_bits())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_LENS: [usize; 8] = [0, 1, 10, 100, 1000, 10_000, 100_000, 8 * 1024 + 1];

    #[test]
    fn test_is_int64_zeros() {
        for &n in &TEST_LENS {
            let mut a = vec![0i64; n];
            assert!(
                is_int64_zeros(&a),
                "IsInt64Zeros must return true for {n} items"
            );
            if !a.is_empty() {
                let last = a.len() - 1;
                a[last] = 1;
                assert!(
                    !is_int64_zeros(&a),
                    "IsInt64Zeros must return false for {n} items"
                );
            }
        }
    }

    #[test]
    fn test_is_int64_ones() {
        for &n in &TEST_LENS {
            let mut a = vec![1i64; n];
            assert!(
                is_int64_ones(&a),
                "IsInt64Ones must return true for {n} items"
            );
            if !a.is_empty() {
                let last = a.len() - 1;
                a[last] = 0;
                assert!(
                    !is_int64_ones(&a),
                    "IsInt64Ones must return false for {n} items"
                );
            }
        }
    }

    #[test]
    fn test_is_float64_zeros() {
        for &n in &TEST_LENS {
            let mut a = vec![0f64; n];
            assert!(
                is_float64_zeros(&a),
                "IsFloat64Zeros must return true for {n} items"
            );
            if !a.is_empty() {
                let last = a.len() - 1;
                a[last] = 1.0;
                assert!(
                    !is_float64_zeros(&a),
                    "IsFloat64Zeros must return false for {n} items"
                );
            }
        }
    }

    #[test]
    fn test_is_float64_ones() {
        for &n in &TEST_LENS {
            let mut a = vec![1f64; n];
            assert!(
                is_float64_ones(&a),
                "IsFloat64Ones must return true for {n} items"
            );
            if !a.is_empty() {
                let last = a.len() - 1;
                a[last] = 0.0;
                assert!(
                    !is_float64_ones(&a),
                    "IsFloat64Ones must return false for {n} items"
                );
            }
        }
    }

    #[test]
    fn test_append_int64_zeros() {
        for &n in &TEST_LENS {
            let mut a = Vec::new();
            append_int64_zeros(&mut a, n);
            assert_eq!(a.len(), n, "unexpected len(a); got {}; want {n}", a.len());
            assert!(is_int64_zeros(&a), "IsInt64Zeros must return true");

            let prefix = vec![1i64, 2, 3];
            let mut a = prefix.clone();
            append_int64_zeros(&mut a, n);
            assert_eq!(
                a.len(),
                prefix.len() + n,
                "unexpected len(a) with prefix; got {}; want {}",
                a.len(),
                prefix.len() + n
            );
            assert_eq!(&a[..prefix.len()], &prefix[..], "unexpected prefix");
            assert!(
                is_int64_zeros(&a[prefix.len()..]),
                "IsInt64Zeros for prefixed a must return true"
            );
        }
    }

    #[test]
    fn test_append_int64_ones() {
        for &n in &TEST_LENS {
            let mut a = Vec::new();
            append_int64_ones(&mut a, n);
            assert_eq!(a.len(), n, "unexpected len(a); got {}; want {n}", a.len());
            assert!(is_int64_ones(&a), "IsInt64Ones must return true");

            let prefix = vec![1i64, 2, 3];
            let mut a = prefix.clone();
            append_int64_ones(&mut a, n);
            assert_eq!(
                a.len(),
                prefix.len() + n,
                "unexpected len(a) with prefix; got {}; want {}",
                a.len(),
                prefix.len() + n
            );
            assert_eq!(&a[..prefix.len()], &prefix[..], "unexpected prefix");
            assert!(
                is_int64_ones(&a[prefix.len()..]),
                "IsInt64Ones for prefixed a must return true"
            );
        }
    }

    #[test]
    fn test_append_float64_zeros() {
        for &n in &TEST_LENS {
            let mut a = Vec::new();
            append_float64_zeros(&mut a, n);
            assert_eq!(a.len(), n, "unexpected len(a); got {}; want {n}", a.len());
            assert!(is_float64_zeros(&a), "IsFloat64Zeros must return true");

            let prefix = vec![1f64, 2.0, 3.0];
            let mut a = prefix.clone();
            append_float64_zeros(&mut a, n);
            assert_eq!(
                a.len(),
                prefix.len() + n,
                "unexpected len(a) with prefix; got {}; want {}",
                a.len(),
                prefix.len() + n
            );
            assert_eq!(&a[..prefix.len()], &prefix[..], "unexpected prefix");
            assert!(
                is_float64_zeros(&a[prefix.len()..]),
                "IsFloat64Zeros for prefixed a must return true"
            );
        }
    }

    #[test]
    fn test_append_float64_ones() {
        for &n in &TEST_LENS {
            let mut a = Vec::new();
            append_float64_ones(&mut a, n);
            assert_eq!(a.len(), n, "unexpected len(a); got {}; want {n}", a.len());
            assert!(is_float64_ones(&a), "IsFloat64Ones must return true");

            let prefix = vec![1f64, 2.0, 3.0];
            let mut a = prefix.clone();
            append_float64_ones(&mut a, n);
            assert_eq!(
                a.len(),
                prefix.len() + n,
                "unexpected len(a) with prefix; got {}; want {}",
                a.len(),
                prefix.len() + n
            );
            assert_eq!(&a[..prefix.len()], &prefix[..], "unexpected prefix");
            assert!(
                is_float64_ones(&a[prefix.len()..]),
                "IsFloat64Ones for prefixed a must return true"
            );
        }
    }
}

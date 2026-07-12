//! Port of Softalink LLC `lib/decimal`.
//!
//! PORT NOTE: `lib/fastnum` and `lib/slicesutil` are not ported yet, so their
//! tiny helpers used here (`IsInt64Zeros`, `AppendFloat64Zeros`,
//! `ExtendCapacity`, ...) are inlined as private functions with identical
//! semantics (the Go versions are only unrolled-loop optimizations).

use std::sync::Mutex;

/// Calibrates `a` and `b` with the corresponding exponents `ae`, `be`
/// and returns the resulting exponent `e`.
pub fn calibrate_scale<'a>(
    mut a: &'a mut [i64],
    mut ae: i16,
    mut b: &'a mut [i64],
    mut be: i16,
) -> i16 {
    if ae == be {
        // Fast path - exponents are equal.
        return ae;
    }
    if a.is_empty() {
        return be;
    }
    if b.is_empty() {
        return ae;
    }

    if ae < be {
        std::mem::swap(&mut a, &mut b);
        std::mem::swap(&mut ae, &mut be);
    }

    let mut up_exp = ae - be;
    let mut down_exp: i16 = 0;
    for &v in a.iter() {
        let max_up_exp = max_up_exponent(v);
        if up_exp - max_up_exp > down_exp {
            down_exp = up_exp - max_up_exp;
        }
    }
    up_exp -= down_exp;

    if up_exp > 0 {
        let m = get_decimal_multiplier(up_exp as u16);
        for v in a.iter_mut() {
            if is_special_value(*v) {
                // Do not take into account special values.
                continue;
            }
            // PORT NOTE: Go integer overflow wraps silently; use wrapping_mul
            // to keep the same behavior in debug builds.
            *v = v.wrapping_mul(m);
        }
    }
    if down_exp > 0 {
        if down_exp > 18 {
            for v in b.iter_mut() {
                if is_special_value(*v) {
                    // Do not take into account special values.
                    continue;
                }
                *v = 0;
            }
        } else {
            let m = get_decimal_multiplier(down_exp as u16);
            for v in b.iter_mut() {
                if is_special_value(*v) {
                    // Do not take into account special values.
                    continue;
                }
                *v /= m;
            }
        }
    }
    be + down_exp
}

fn get_decimal_multiplier(exp: u16) -> i64 {
    if exp as usize >= DECIMAL_MULTIPLIERS.len() {
        return 1;
    }
    DECIMAL_MULTIPLIERS[exp as usize]
}

const DECIMAL_MULTIPLIERS: [i64; 19] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
];

/// Extends `dst` capacity to hold `additional_items` and returns the extended `dst`.
pub fn extend_float64s_capacity(mut dst: Vec<f64>, additional_items: usize) -> Vec<f64> {
    dst.reserve(additional_items);
    dst
}

/// Extends `dst` capacity to hold `additional_items` and returns the extended `dst`.
pub fn extend_int64s_capacity(mut dst: Vec<i64>, additional_items: usize) -> Vec<i64> {
    dst.reserve(additional_items);
    dst
}

/// Converts each item in `va` to `f = v * 10^e`, appends it to `dst`
/// and returns the resulting `dst`.
pub fn append_decimal_to_float(mut dst: Vec<f64>, va: &[i64], e: i16) -> Vec<f64> {
    // Extend dst capacity in order to eliminate memory allocations below.
    dst = extend_float64s_capacity(dst, va.len());

    if is_int64_zeros(va) {
        return append_float64_zeros(dst, va.len());
    }
    if e == 0 {
        if is_int64_ones(va) {
            return append_float64_ones(dst, va.len());
        }
        for &v in va {
            let f = if !is_special_value(v) {
                v as f64
            } else if v == V_INF_POS {
                INF_POS
            } else if v == V_INF_NEG {
                INF_NEG
            } else {
                STALE_NAN
            };
            dst.push(f);
        }
        return dst;
    }

    // increase conversion precision for negative exponents by dividing by e10
    if e < 0 {
        let e10 = pow10(-e as i32);
        for &v in va {
            let f = if !is_special_value(v) {
                v as f64 / e10
            } else if v == V_INF_POS {
                INF_POS
            } else if v == V_INF_NEG {
                INF_NEG
            } else {
                STALE_NAN
            };
            dst.push(f);
        }
        return dst;
    }
    let e10 = pow10(e as i32);
    for &v in va {
        let f = if !is_special_value(v) {
            v as f64 * e10
        } else if v == V_INF_POS {
            INF_POS
        } else if v == V_INF_NEG {
            INF_NEG
        } else {
            STALE_NAN
        };
        dst.push(f);
    }
    dst
}

/// Converts each item in `src` to `v * 10^e` and appends each `v` to `dst`
/// returning it as `va`.
///
/// It tries minimizing each item in `dst`.
pub fn append_float_to_decimal(mut dst: Vec<i64>, src: &[f64]) -> (Vec<i64>, i16) {
    if src.is_empty() {
        return (dst, 0);
    }
    if is_float64_zeros(src) {
        dst = append_int64_zeros(dst, src.len());
        return (dst, 0);
    }
    if is_float64_ones(src) {
        dst = append_int64_ones(dst, src.len());
        return (dst, 0);
    }

    let mut vae = get_vae_buf();
    vae.va.clear();
    vae.ea.clear();
    vae.va.reserve(src.len());
    vae.ea.reserve(src.len());

    // Determine the minimum exponent across all src items.
    let mut min_exp = i16::MAX;
    for &f in src {
        let (v, exp) = from_float(f);
        vae.va.push(v);
        vae.ea.push(exp);
        if exp < min_exp && !is_special_value(v) {
            min_exp = exp;
        }
    }

    // Determine whether all the src items may be upscaled to minExp.
    // If not, adjust minExp accordingly.
    let mut down_exp: i16 = 0;
    for (i, &v) in vae.va.iter().enumerate() {
        let exp = vae.ea[i];
        let up_exp = exp - min_exp;
        let max_up_exp = max_up_exponent(v);
        if up_exp - max_up_exp > down_exp {
            down_exp = up_exp - max_up_exp;
        }
    }
    min_exp += down_exp;

    // Extend dst capacity in order to eliminate memory allocations below.
    dst = extend_int64s_capacity(dst, src.len());

    // Scale each item in src to minExp and append it to dst.
    for (i, &v) in vae.va.iter().enumerate() {
        if is_special_value(v) {
            // There is no need in scaling special values.
            dst.push(v);
            continue;
        }
        let mut v = v;
        let mut adj_exp = vae.ea[i] - min_exp;
        while adj_exp > 0 {
            v *= 10;
            adj_exp -= 1;
        }
        while adj_exp < 0 {
            v /= 10;
            adj_exp += 1;
        }
        dst.push(v);
    }

    put_vae_buf(vae);

    (dst, min_exp)
}

struct VaeBuf {
    va: Vec<i64>,
    ea: Vec<i16>,
}

// PORT NOTE: Go uses sync.Pool; a Mutex<Vec<_>> pool preserves the buffer
// reuse pattern with std-only primitives.
static VAE_BUF_POOL: Mutex<Vec<VaeBuf>> = Mutex::new(Vec::new());

fn get_vae_buf() -> VaeBuf {
    VAE_BUF_POOL.lock().unwrap().pop().unwrap_or(VaeBuf {
        va: Vec::new(),
        ea: Vec::new(),
    })
}

fn put_vae_buf(vae: VaeBuf) {
    VAE_BUF_POOL.lock().unwrap().push(vae);
}

const INT64_MAX: i64 = i64::MAX;

fn max_up_exponent(v: i64) -> i16 {
    if v == 0 || is_special_value(v) {
        // Any exponent allowed for zeroes and special values.
        return 1024;
    }
    // PORT NOTE: Go negation wraps for v = -1<<63 (handled by the `v < 0`
    // corner case below); use wrapping_neg for the same behavior.
    let mut v = v;
    if v < 0 {
        v = v.wrapping_neg();
    }
    if v < 0 {
        // Handle corner case for v=-1<<63
        return 0;
    }
    if v <= INT64_MAX / DECIMAL_MULTIPLIERS[18] {
        18
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[17] {
        17
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[16] {
        16
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[15] {
        15
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[14] {
        14
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[13] {
        13
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[12] {
        12
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[11] {
        11
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[10] {
        10
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[9] {
        9
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[8] {
        8
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[7] {
        7
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[6] {
        6
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[5] {
        5
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[4] {
        4
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[3] {
        3
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[2] {
        2
    } else if v <= INT64_MAX / DECIMAL_MULTIPLIERS[1] {
        1
    } else {
        0
    }
}

/// Rounds `f` to the given number of decimal digits after the point.
///
/// See also [`round_to_significant_figures`].
pub fn round_to_decimal_digits(f: f64, digits: i32) -> f64 {
    if is_stale_nan(f) {
        // Do not modify stale nan mark value.
        return f;
    }
    if digits <= -100 || digits >= 100 {
        return f;
    }
    let m = pow10(digits);
    (f * m).round() / m
}

/// Rounds `f` to value with the given number of significant figures.
///
/// See also [`round_to_decimal_digits`].
pub fn round_to_significant_figures(f: f64, digits: i32) -> f64 {
    if is_stale_nan(f) {
        // Do not modify stale nan mark value.
        return f;
    }
    if digits <= 0 || digits >= 18 {
        return f;
    }
    if f.is_nan() || f.is_infinite() || f == 0.0 {
        return f;
    }
    let n = pow10(digits) as i64;
    let is_negative = f < 0.0;
    let f = if is_negative { -f } else { f };
    let (mut v, mut e) = positive_float_to_decimal(f);
    if v > V_MAX {
        v = V_MAX;
    }
    let mut rem: i64 = 0;
    while v > n {
        rem = v % 10;
        v /= 10;
        e += 1;
    }
    if rem >= 5 {
        v += 1;
    }
    if is_negative {
        v = -v;
    }
    to_float(v, e)
}

/// Returns `f = v * 10^e`.
pub fn to_float(v: i64, e: i16) -> f64 {
    if is_special_value(v) {
        if v == V_INF_POS {
            return INF_POS;
        }
        if v == V_INF_NEG {
            return INF_NEG;
        }
        return STALE_NAN;
    }
    let f = v as f64;
    // increase conversion precision for negative exponents by dividing by e10
    if e < 0 {
        return f / pow10(-e as i32);
    }
    f * pow10(e as i32)
}

pub(crate) const INF_POS: f64 = f64::INFINITY;
pub(crate) const INF_NEG: f64 = f64::NEG_INFINITY;

/// StaleNaN is a special NaN value, which is used as Prometheus staleness mark.
/// See <https://www.robustperception.io/staleness-and-promql>
pub const STALE_NAN: f64 = f64::from_bits(STALE_NAN_BITS);

pub(crate) const V_INF_POS: i64 = i64::MAX; // 1<<63 - 1
pub(crate) const V_INF_NEG: i64 = i64::MIN; // -1 << 63
pub(crate) const V_STALE_NAN: i64 = i64::MAX - 1; // 1<<63 - 2

pub(crate) const V_MAX: i64 = i64::MAX - 2; // 1<<63 - 3
pub(crate) const V_MIN: i64 = i64::MIN + 1; // -1<<63 + 1

/// staleNaNBits is bit representation of Prometheus staleness mark (aka stale NaN).
/// This mark is put by Prometheus at the end of time series for improving staleness detection.
/// See <https://www.robustperception.io/staleness-and-promql>
pub(crate) const STALE_NAN_BITS: u64 = 0x7ff0000000000002;

fn is_special_value(v: i64) -> bool {
    !(V_MIN..=V_MAX).contains(&v)
}

/// Returns true if `f` represents Prometheus staleness mark.
pub fn is_stale_nan(f: f64) -> bool {
    f.to_bits() == STALE_NAN_BITS
}

/// Returns true if `i` represents Prometheus staleness mark.
pub fn is_stale_nan_int64(i: i64) -> bool {
    i == V_STALE_NAN
}

/// Converts `f` to `v * 10^e`.
///
/// It tries minimizing `v`.
/// For instance, for f = -1.234 it returns v = -1234, e = -3.
///
/// FromFloat doesn't work properly with NaN values other than Prometheus
/// staleness mark, so don't pass them here.
pub fn from_float(f: f64) -> (i64, i16) {
    if f == 0.0 {
        return (0, 0);
    }
    if is_stale_nan(f) {
        return (V_STALE_NAN, 0);
    }
    if f.is_infinite() {
        return from_float_inf(f);
    }
    if f > 0.0 {
        let (mut v, e) = positive_float_to_decimal(f);
        if v > V_MAX {
            v = V_MAX;
        }
        return (v, e);
    }
    let (v, e) = positive_float_to_decimal(-f);
    let v = std::cmp::max(-v, V_MIN);
    (v, e)
}

fn from_float_inf(f: f64) -> (i64, i16) {
    // Limit infs by max and min values for int64
    if f == f64::INFINITY {
        return (V_INF_POS, 0);
    }
    (V_INF_NEG, 0)
}

fn positive_float_to_decimal(f: f64) -> (i64, i16) {
    // There is no need in checking for f == 0, since it should be already checked by the caller.
    // PORT NOTE: Rust `as u64` saturates on out-of-range floats while Go's
    // conversion is platform-defined; both cases fail the `u as f64 == f`
    // round-trip check below and take the slow path, so behavior matches.
    let u = f as u64;
    if u as f64 != f {
        return positive_float_to_decimal_slow(f);
    }
    // Fast path for integers.
    if u < (1 << 55) && !u.is_multiple_of(10) {
        return (u as i64, 0);
    }
    get_decimal_and_scale(u)
}

fn get_decimal_and_scale(mut u: u64) -> (i64, i16) {
    let mut scale: i16 = 0;
    while u >= (1 << 55) {
        // Remove trailing garbage bits left after float64->uint64 conversion,
        // since float64 contains only 53 significant bits.
        // See https://en.wikipedia.org/wiki/Double-precision_floating-point_format
        u /= 10;
        scale += 1;
    }
    if !u.is_multiple_of(10) {
        return (u as i64, scale);
    }
    // Minimize v by converting trailing zeros to scale.
    u /= 10;
    scale += 1;
    while u != 0 && u.is_multiple_of(10) {
        u /= 10;
        scale += 1;
    }
    (u as i64, scale)
}

fn positive_float_to_decimal_slow(mut f: f64) -> (i64, i16) {
    // Slow path for floating point numbers.
    let mut scale: i16 = 0;
    let mut prec = CONVERSION_PRECISION;
    if !(1e-6..=1e6).contains(&f) {
        // Normalize f, so it is in the small range suitable
        // for the next loop.
        if f > 1e6 {
            // Increase conversion precision for big numbers.
            // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/213
            prec = 1e15;
        }
        let mut exp = frexp_exp(f);
        // Bound the exponent according to https://en.wikipedia.org/wiki/Double-precision_floating-point_format
        // This fixes the issue https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1114
        exp = exp.clamp(-1022, 1023);
        scale = (exp as f64 * (std::f64::consts::LN_2 / std::f64::consts::LN_10)) as i16;
        f *= pow10(-(scale as i32));
    }

    // Multiply f by 100 until the fractional part becomes
    // too small comparing to integer part.
    while f < prec {
        let x = f.trunc();
        let frac = f - x;
        if frac * prec < x {
            f = x;
            break;
        }
        if (1.0 - frac) * prec < x {
            f = x + 1.0;
            break;
        }
        f *= 100.0;
        scale -= 2;
    }
    let u = f as u64;
    if !u.is_multiple_of(10) {
        return (u as i64, scale);
    }

    // Minimize u by converting trailing zero to scale.
    let u = u / 10;
    scale += 1;
    (u as i64, scale)
}

const CONVERSION_PRECISION: f64 = 1e12;

// PORT NOTE: the helpers below reproduce Go's math.Pow10 (exact table-based
// lookup) and the exponent part of math.Frexp, since Rust has no std
// equivalents with identical results. f64::powi is not guaranteed to return
// the same bits as the Go tables.
const POW10_TAB: [f64; 32] = [
    1e00, 1e01, 1e02, 1e03, 1e04, 1e05, 1e06, 1e07, 1e08, 1e09, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
    1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31,
];

const POW10_POS_TAB32: [f64; 10] = [
    1e0, 1e32, 1e64, 1e96, 1e128, 1e160, 1e192, 1e224, 1e256, 1e288,
];

const POW10_NEG_TAB32: [f64; 11] = [
    1e-0, 1e-32, 1e-64, 1e-96, 1e-128, 1e-160, 1e-192, 1e-224, 1e-256, 1e-288, 1e-320,
];

/// Port of Go's math.Pow10.
fn pow10(n: i32) -> f64 {
    if (0..=308).contains(&n) {
        return POW10_POS_TAB32[(n as usize) / 32] * POW10_TAB[(n as usize) % 32];
    }
    if (-323..0).contains(&n) {
        return POW10_NEG_TAB32[(-n as usize) / 32] / POW10_TAB[(-n as usize) % 32];
    }
    // n < -323 || 308 < n
    if n > 308 { f64::INFINITY } else { 0.0 }
}

/// Returns the binary exponent of `f` as Go's math.Frexp does
/// (frac is not needed by this module).
fn frexp_exp(f: f64) -> i32 {
    if f == 0.0 || f.is_infinite() || f.is_nan() {
        return 0;
    }
    let mut f = f;
    let mut exp: i32 = 0;
    if f.abs() < f64::MIN_POSITIVE {
        // Subnormal value: normalize like Go's math.normalize.
        f *= (1u64 << 52) as f64;
        exp = -52;
    }
    let x = f.to_bits();
    exp + (((x >> 52) & 0x7ff) as i32) - 1023 + 1
}

// Inlined fastnum helpers (see PORT NOTE at the top of the file).

fn is_int64_zeros(a: &[i64]) -> bool {
    a.iter().all(|&v| v == 0)
}

fn is_int64_ones(a: &[i64]) -> bool {
    a.iter().all(|&v| v == 1)
}

fn is_float64_zeros(a: &[f64]) -> bool {
    a.iter().all(|&v| v == 0.0)
}

fn is_float64_ones(a: &[f64]) -> bool {
    a.iter().all(|&v| v == 1.0)
}

fn append_float64_zeros(mut dst: Vec<f64>, items: usize) -> Vec<f64> {
    dst.resize(dst.len() + items, 0.0);
    dst
}

fn append_float64_ones(mut dst: Vec<f64>, items: usize) -> Vec<f64> {
    dst.resize(dst.len() + items, 1.0);
    dst
}

fn append_int64_zeros(mut dst: Vec<i64>, items: usize) -> Vec<i64> {
    dst.resize(dst.len() + items, 0);
    dst
}

fn append_int64_ones(mut dst: Vec<i64>, items: usize) -> Vec<i64> {
    dst.resize(dst.len() + items, 1);
    dst
}

#[cfg(test)]
mod tests {
    use super::*;

    const NAN: f64 = f64::NAN;

    #[test]
    fn test_round_to_decimal_digits() {
        fn f(v: f64, digits: i32, result_expected: f64) {
            let result = round_to_decimal_digits(v, digits);
            if result.is_nan() {
                if is_stale_nan(result_expected) {
                    assert!(
                        is_stale_nan(result),
                        "unexpected stale mark value; got {:016X}; want {:016X}",
                        result.to_bits(),
                        STALE_NAN_BITS
                    );
                    return;
                }
                assert!(
                    result_expected.is_nan(),
                    "unexpected result; got {result}; want {result_expected}"
                );
                return;
            }
            assert!(
                result == result_expected,
                "unexpected result; got {result}; want {result_expected}"
            );
        }
        f(12.34, 0, 12.0);
        f(12.57, 0, 13.0);
        f(-1.578, 2, -1.58);
        f(-1.578, 3, -1.578);
        f(1234.0, -2, 1200.0);
        f(1235.0, -1, 1240.0);
        f(1234.0, 0, 1234.0);
        f(1234.6, 0, 1235.0);
        f(123.4e-99, 99, 123e-99);
        f(NAN, 10, NAN);
        f(STALE_NAN, 10, STALE_NAN);
    }

    #[test]
    fn test_round_to_significant_figures() {
        fn f(v: f64, digits: i32, result_expected: f64) {
            let result = round_to_significant_figures(v, digits);
            if result.is_nan() {
                if is_stale_nan(result_expected) {
                    assert!(
                        is_stale_nan(result),
                        "unexpected stale mark value; got {:016X}; want {:016X}",
                        result.to_bits(),
                        STALE_NAN_BITS
                    );
                    return;
                }
                assert!(
                    result_expected.is_nan(),
                    "unexpected result; got {result}; want {result_expected}"
                );
                return;
            }
            assert!(
                result == result_expected,
                "unexpected result; got {result}; want {result_expected}"
            );
        }
        f(1234.0, 0, 1234.0);
        f(-12.34, 20, -12.34);
        f(12.0, 1, 10.0);
        f(25.0, 1, 30.0);
        f(2.5, 1, 3.0);
        f(-0.56, 1, -0.6);
        f(1234567.0, 3, 1230000.0);
        f(-1.234567, 4, -1.235);
        f(NAN, 10, NAN);
        f(STALE_NAN, 10, STALE_NAN);
    }

    #[test]
    // Float literals are kept verbatim from the Go test, including digits
    // beyond f64 precision.
    #[allow(clippy::excessive_precision)]
    fn test_positive_float_to_decimal() {
        fn f(v: f64, decimal_expected: i64, exponent_expected: i16) {
            let (decimal, exponent) = positive_float_to_decimal(v);
            assert!(
                decimal == decimal_expected,
                "unexpected decimal for positiveFloatToDecimal({v}); got {decimal}; want {decimal_expected}"
            );
            assert!(
                exponent == exponent_expected,
                "unexpected exponent for positiveFloatToDecimal({v}); got {exponent}; want {exponent_expected}"
            );
        }
        f(0.0, 0, 1); // The exponent is 1 is OK here. See comment in positiveFloatToDecimal.
        f(1.0, 1, 0);
        f(30.0, 3, 1);
        f(12345678900000000.0, 123456789, 8);
        f(12345678901234567.0, 12345678901234568, 0);
        f(1234567890123456789.0, 12345678901234567, 2);
        f(12345678901234567890.0, 12345678901234567, 3);
        f(18446744073670737131.0, 18446744073670737, 3);
        f(123456789012345678901.0, 12345678901234568, 4);
        f((1u64 << 53) as f64, 1 << 53, 0);
        f((1u64 << 54) as f64, 18014398509481984, 0);
        f((1u64 << 55) as f64, 3602879701896396, 1);
        f((1u64 << 62) as f64, 4611686018427387, 3);
        f((1u64 << 63) as f64, 9223372036854775, 3);
        // Skip this test, since M1 returns 18446744073709551 instead of 18446744073709548
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1653
        // f(1<<64, 18446744073709548, 3)
        f(36893488147419103232.0 /* 1<<65 */, 368934881474191, 5);
        f(73786976294838206464.0 /* 1<<66 */, 737869762948382, 5);
        f(
            147573952589676412928.0, /* 1<<67 */
            1475739525896764,
            5,
        );

        f(0.1, 1, -1);
        f(123456789012345678e-5, 12345678901234568, -4);
        f(1234567890123456789e-10, 12345678901234568, -8);
        f(1234567890123456789e-14, 1234567890123, -8);
        f(1234567890123456789e-17, 12345678901234, -12);
        f(1234567890123456789e-20, 1234567890123, -14);

        f(0.000874957, 874957, -9);
        f(0.001130435, 1130435, -9);
        f(V_INF_POS as f64, 9223372036854775, 3);
        f(V_MAX as f64, 9223372036854775, 3);

        // Extreme cases. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1114
        f(2.964393875e-100, 2964393875, -109);
        f(2.964393875e-309, 2964393875, -318);
        f(2.964393875e-314, 296439387505, -325);
        f(2.964393875e-315, 2964393875047, -327);
        f(2.964393875e-320, 296439387505, -331);
        f(2.964393875e-324, 494065645841, -335);
        f(2.964393875e-325, 0, 1);

        f(2.964393875e+307, 2964393875, 298);
        f(9.964393875e+307, 9964393875, 298);
        f(1.064393875e+308, 1064393875, 299);
        f(1.797393875e+308, 1797393875, 299);
    }

    #[test]
    fn test_append_decimal_to_float() {
        test_append_decimal_to_float_case(&[], 0, &[]);
        test_append_decimal_to_float_case(&[0], 0, &[0.0]);
        test_append_decimal_to_float_case(&[0], 10, &[0.0]);
        test_append_decimal_to_float_case(&[0], -10, &[0.0]);
        test_append_decimal_to_float_case(&[-1, -10, 0, 100], 2, &[-1e2, -1e3, 0.0, 1e4]);
        test_append_decimal_to_float_case(&[-1, -10, 0, 100], -2, &[-1e-2, -1e-1, 0.0, 1.0]);
        test_append_decimal_to_float_case(&[874957, 1130435], -5, &[8.74957, 1.130435e1]);
        test_append_decimal_to_float_case(&[874957, 1130435], -6, &[8.74957e-1, 1.130435]);
        test_append_decimal_to_float_case(&[874957, 1130435], -7, &[8.74957e-2, 1.130435e-1]);
        test_append_decimal_to_float_case(&[874957, 1130435], -8, &[8.74957e-3, 1.130435e-2]);
        test_append_decimal_to_float_case(&[874957, 1130435], -9, &[8.74957e-4, 1.130435e-3]);
        test_append_decimal_to_float_case(&[874957, 1130435], -10, &[8.74957e-5, 1.130435e-4]);
        test_append_decimal_to_float_case(&[874957, 1130435], -11, &[8.74957e-6, 1.130435e-5]);
        test_append_decimal_to_float_case(&[874957, 1130435], -12, &[8.74957e-7, 1.130435e-6]);
        test_append_decimal_to_float_case(&[874957, 1130435], -13, &[8.74957e-8, 1.130435e-7]);
        test_append_decimal_to_float_case(
            &[V_MAX, V_MIN, 1, 2],
            4,
            &[V_MAX as f64 * 1e4, V_MIN as f64 * 1e4, 1e4, 2e4],
        );
        test_append_decimal_to_float_case(
            &[V_MAX, V_MIN, 1, 2],
            -4,
            &[V_MAX as f64 * 1e-4, V_MIN as f64 * 1e-4, 1e-4, 2e-4],
        );
        test_append_decimal_to_float_case(
            &[V_INF_POS, V_INF_NEG, 1, 2],
            0,
            &[INF_POS, INF_NEG, 1.0, 2.0],
        );
        test_append_decimal_to_float_case(
            &[V_INF_POS, V_INF_NEG, 1, 2],
            4,
            &[INF_POS, INF_NEG, 1e4, 2e4],
        );
        test_append_decimal_to_float_case(
            &[V_INF_POS, V_INF_NEG, 1, 2],
            -4,
            &[INF_POS, INF_NEG, 1e-4, 2e-4],
        );
        test_append_decimal_to_float_case(
            &[1234, V_STALE_NAN, 1, 2],
            0,
            &[1234.0, STALE_NAN, 1.0, 2.0],
        );
        test_append_decimal_to_float_case(
            &[V_INF_POS, V_STALE_NAN, V_MIN, 2],
            4,
            &[INF_POS, STALE_NAN, V_MIN as f64 * 1e4, 2e4],
        );
        test_append_decimal_to_float_case(
            &[V_INF_POS, V_STALE_NAN, V_MIN, 2],
            -4,
            &[INF_POS, STALE_NAN, V_MIN as f64 * 1e-4, 2e-4],
        );
    }

    fn test_append_decimal_to_float_case(va: &[i64], e: i16, f_expected: &[f64]) {
        let f = append_decimal_to_float(Vec::new(), va, e);
        assert!(
            equal_values(&f, f_expected),
            "unexpected f for va={va:?}, e={e}: got\n{f:?}; expecting\n{f_expected:?}"
        );

        let prefix = [1.0f64, 2.0, 3.0, 4.0];
        let f = append_decimal_to_float(prefix.to_vec(), va, e);
        assert!(
            equal_values(&f[..prefix.len()], &prefix),
            "unexpected prefix for va={va:?}, e={e}; got\n{:?}; expecting\n{prefix:?}",
            &f[..prefix.len()]
        );
        assert!(
            equal_values(&f[prefix.len()..], f_expected),
            "unexpected prefixed f for va={va:?}, e={e}: got\n{:?}; expecting\n{f_expected:?}",
            &f[prefix.len()..]
        );
    }

    fn equal_values(a: &[f64], b: &[f64]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        a.iter()
            .zip(b.iter())
            .all(|(va, vb)| va.to_bits() == vb.to_bits())
    }

    #[test]
    fn test_calibrate_scale() {
        test_calibrate_scale_case(&[], &[], 0, 0, &[], &[], 0);
        test_calibrate_scale_case(&[0], &[0], 0, 0, &[0], &[0], 0);
        test_calibrate_scale_case(&[0], &[1], 0, 0, &[0], &[1], 0);
        test_calibrate_scale_case(&[1, 0, 2], &[5, -3], 0, 1, &[1, 0, 2], &[50, -30], 0);
        test_calibrate_scale_case(&[-1, 2], &[5, 6, 3], 2, -1, &[-1000, 2000], &[5, 6, 3], -1);
        test_calibrate_scale_case(
            &[123, -456, 94],
            &[-9, 4, -3, 45],
            -3,
            -3,
            &[123, -456, 94],
            &[-9, 4, -3, 45],
            -3,
        );
        test_calibrate_scale_case(
            &[1_000_000_000_000_000_000, 1, 0],
            &[3, 456],
            0,
            -2,
            &[1_000_000_000_000_000_000, 1, 0],
            &[0, 4],
            0,
        );
        test_calibrate_scale_case(
            &[12345, 678],
            &[12, -100_000_000_000_000_000, -3],
            -3,
            0,
            &[123, 6],
            &[120, -1_000_000_000_000_000_000, -30],
            -1,
        );
        test_calibrate_scale_case(&[1, 2], &[], 12, 34, &[1, 2], &[], 12);
        test_calibrate_scale_case(&[], &[3, 1], 12, 34, &[], &[3, 1], 34);
        test_calibrate_scale_case(
            &[923],
            &[2, 3],
            100,
            -100,
            &[923_000_000_000_000_000],
            &[0, 0],
            85,
        );
        test_calibrate_scale_case(
            &[923],
            &[2, 3],
            -100,
            100,
            &[0],
            &[2_000_000_000_000_000_000, 3_000_000_000_000_000_000],
            82,
        );
        test_calibrate_scale_case(
            &[123, 456, 789, 135],
            &[],
            -12,
            -10,
            &[123, 456, 789, 135],
            &[],
            -12,
        );
        test_calibrate_scale_case(
            &[123, 456, 789, 135],
            &[],
            -10,
            -12,
            &[123, 456, 789, 135],
            &[],
            -10,
        );

        test_calibrate_scale_case(
            &[V_INF_POS, 1200],
            &[500, 100],
            0,
            0,
            &[V_INF_POS, 1200],
            &[500, 100],
            0,
        );
        test_calibrate_scale_case(
            &[V_INF_POS, 1200],
            &[500, 100],
            0,
            2,
            &[V_INF_POS, 1200],
            &[50000, 10000],
            0,
        );
        test_calibrate_scale_case(
            &[V_INF_POS, 1200],
            &[500, 100],
            0,
            -2,
            &[V_INF_POS, 120_000],
            &[500, 100],
            -2,
        );
        test_calibrate_scale_case(
            &[V_INF_POS, 1200],
            &[3500, 100],
            0,
            -3,
            &[V_INF_POS, 1_200_000],
            &[3500, 100],
            -3,
        );
        test_calibrate_scale_case(
            &[V_INF_POS, 1200],
            &[35, 1],
            0,
            40,
            &[V_INF_POS, 0],
            &[3_500_000_000_000_000_000, 100_000_000_000_000_000],
            23,
        );
        test_calibrate_scale_case(
            &[V_INF_POS, 1200],
            &[35, 1],
            40,
            0,
            &[V_INF_POS, 1_200_000_000_000_000_000],
            &[0, 0],
            25,
        );
        test_calibrate_scale_case(
            &[V_INF_NEG, 1200],
            &[35, 1],
            35,
            -5,
            &[V_INF_NEG, 1_200_000_000_000_000_000],
            &[0, 0],
            20,
        );
        test_calibrate_scale_case(
            &[V_MAX, V_MIN, 123],
            &[100],
            0,
            3,
            &[V_MAX, V_MIN, 123],
            &[100_000],
            0,
        );
        test_calibrate_scale_case(
            &[V_MAX, V_MIN, 123],
            &[100],
            3,
            0,
            &[V_MAX, V_MIN, 123],
            &[0],
            3,
        );
        test_calibrate_scale_case(
            &[V_MAX, V_MIN, 123],
            &[100],
            0,
            30,
            &[92233, -92233, 0],
            &[1_000_000_000_000_000_000],
            14,
        );
        test_calibrate_scale_case(
            &[V_STALE_NAN, V_MIN, 123],
            &[100],
            0,
            30,
            &[V_STALE_NAN, -92233, 0],
            &[1_000_000_000_000_000_000],
            14,
        );

        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/805
        test_calibrate_scale_case(&[123], &[V_INF_POS], 0, 0, &[123], &[V_INF_POS], 0);
        test_calibrate_scale_case(
            &[123, V_INF_POS],
            &[V_INF_NEG],
            0,
            0,
            &[123, V_INF_POS],
            &[V_INF_NEG],
            0,
        );
        test_calibrate_scale_case(
            &[123, V_INF_POS, V_INF_NEG],
            &[456],
            0,
            0,
            &[123, V_INF_POS, V_INF_NEG],
            &[456],
            0,
        );
        test_calibrate_scale_case(
            &[123, V_INF_POS, V_INF_NEG, 456],
            &[],
            0,
            0,
            &[123, V_INF_POS, V_INF_NEG, 456],
            &[],
            0,
        );
        test_calibrate_scale_case(
            &[123, V_INF_POS],
            &[V_INF_NEG, 456],
            0,
            0,
            &[123, V_INF_POS],
            &[V_INF_NEG, 456],
            0,
        );
        test_calibrate_scale_case(
            &[123, V_INF_POS],
            &[V_INF_NEG, 456],
            0,
            10,
            &[123, V_INF_POS],
            &[V_INF_NEG, 4_560_000_000_000],
            0,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn test_calibrate_scale_case(
        a: &[i64],
        b: &[i64],
        ae: i16,
        be: i16,
        a_expected: &[i64],
        b_expected: &[i64],
        e_expected: i16,
    ) {
        let mut a_copy = a.to_vec();
        let mut b_copy = b.to_vec();
        let e = calibrate_scale(&mut a_copy, ae, &mut b_copy, be);
        assert!(
            e == e_expected,
            "unexpected e for a={a:?}, b={b:?}, ae={ae}, be={be}; got {e}; expecting {e_expected}"
        );
        assert!(
            a_copy == a_expected,
            "unexpected a for b={b:?}, ae={ae}, be={be}; got\n{a_copy:?}; expecting\n{a_expected:?}"
        );
        assert!(
            b_copy == b_expected,
            "unexpected b for a={a:?}, ae={ae}, be={be}; got\n{b_copy:?}; expecting\n{b_expected:?}"
        );

        // Try reverse args.
        let mut a_copy = a.to_vec();
        let mut b_copy = b.to_vec();
        let e = calibrate_scale(&mut b_copy, be, &mut a_copy, ae);
        assert!(
            e == e_expected,
            "reverse: unexpected e for a={a:?}, b={b:?}, ae={ae}, be={be}; got {e}; expecting {e_expected}"
        );
        assert!(
            a_copy == a_expected,
            "reverse: unexpected a for b={b:?}, ae={ae}, be={be}; got\n{a_copy:?}; expecting\n{a_expected:?}"
        );
        assert!(
            b_copy == b_expected,
            "reverse: unexpected b for a={a:?}, ae={ae}, be={be}; got\n{b_copy:?}; expecting\n{b_expected:?}"
        );
    }

    #[test]
    fn test_max_up_exponent() {
        fn f(v: i64, e_expected: i16) {
            let e = max_up_exponent(v);
            assert!(
                e == e_expected,
                "unexpected e for v={v}; got {e}; expecting {e_expected}"
            );
        }

        f(V_INF_POS, 1024);
        f(V_INF_NEG, 1024);
        f(V_STALE_NAN, 1024);
        f(V_MIN, 0);
        f(V_MAX, 0);
        f(0, 1024);
        f(1, 18);
        f(12, 17);
        f(123, 16);
        f(1234, 15);
        f(12345, 14);
        f(123456, 13);
        f(1234567, 12);
        f(12345678, 11);
        f(123456789, 10);
        f(1234567890, 9);
        f(12345678901, 8);
        f(123456789012, 7);
        f(1234567890123, 6);
        f(12345678901234, 5);
        f(123456789012345, 4);
        f(1234567890123456, 3);
        f(12345678901234567, 2);
        f(123456789012345678, 1);
        f(1234567890123456789, 0);
        f(923456789012345678, 0);
        f(92345678901234567, 1);
        f(9234567890123456, 2);
        f(923456789012345, 3);
        f(92345678901234, 4);
        f(9234567890123, 5);
        f(923456789012, 6);
        f(92345678901, 7);
        f(9234567890, 8);
        f(923456789, 9);
        f(92345678, 10);
        f(9234567, 11);
        f(923456, 12);
        f(92345, 13);
        f(9234, 14);
        f(923, 15);
        f(92, 17);
        f(9, 18);

        f(-1, 18);
        f(-12, 17);
        f(-123, 16);
        f(-1234, 15);
        f(-12345, 14);
        f(-123456, 13);
        f(-1234567, 12);
        f(-12345678, 11);
        f(-123456789, 10);
        f(-1234567890, 9);
        f(-12345678901, 8);
        f(-123456789012, 7);
        f(-1234567890123, 6);
        f(-12345678901234, 5);
        f(-123456789012345, 4);
        f(-1234567890123456, 3);
        f(-12345678901234567, 2);
        f(-123456789012345678, 1);
        f(-1234567890123456789, 0);
        f(-923456789012345678, 0);
        f(-92345678901234567, 1);
        f(-9234567890123456, 2);
        f(-923456789012345, 3);
        f(-92345678901234, 4);
        f(-9234567890123, 5);
        f(-923456789012, 6);
        f(-92345678901, 7);
        f(-9234567890, 8);
        f(-923456789, 9);
        f(-92345678, 10);
        f(-9234567, 11);
        f(-923456, 12);
        f(-92345, 13);
        f(-9234, 14);
        f(-923, 15);
        f(-92, 17);
        f(-9, 18);
    }

    #[test]
    fn test_append_float_to_decimal() {
        // no-op
        test_append_float_to_decimal_case(&[], &[], 0);
        test_append_float_to_decimal_case(&[0.0], &[0], 0);
        test_append_float_to_decimal_case(
            &[INF_POS, INF_NEG, 123.0],
            &[V_INF_POS, V_INF_NEG, 123],
            0,
        );
        test_append_float_to_decimal_case(
            &[INF_POS, INF_NEG, 123.0, 1e-4, 1e32],
            &[V_INF_POS, V_INF_NEG, 0, 0, 1000000000000000000],
            14,
        );
        test_append_float_to_decimal_case(
            &[STALE_NAN, INF_NEG, 123.0, 1e-4, 1e32],
            &[V_STALE_NAN, V_INF_NEG, 0, 0, 1000000000000000000],
            14,
        );
        test_append_float_to_decimal_case(
            &[0.0, -0.0, 1.0, -1.0, 12345678.0, -123456789.0],
            &[0, 0, 1, -1, 12345678, -123456789],
            0,
        );

        // upExp
        test_append_float_to_decimal_case(&[-24.0, 0.0, 4.123, 0.3], &[-24000, 0, 4123, 300], -3);
        test_append_float_to_decimal_case(
            &[0.0, 10.23456789, 1e2, 1e-3, 1e-4],
            &[0, 1023456789, 10_000_000_000, 100_000, 10_000],
            -8,
        );

        // downExp
        test_append_float_to_decimal_case(
            &[3e17, 7e-2, 5e-7, 45.0, 7e-1],
            &[3_000_000_000_000_000_000, 0, 0, 450, 7],
            -1,
        );
        test_append_float_to_decimal_case(
            &[3e18, 1.0, 0.1, 13.0],
            &[3_000_000_000_000_000_000, 1, 0, 13],
            0,
        );
    }

    fn test_append_float_to_decimal_case(fa: &[f64], da_expected: &[i64], e_expected: i16) {
        let (da, e) = append_float_to_decimal(Vec::new(), fa);
        assert!(
            e == e_expected,
            "unexpected e for fa={fa:?}; got {e}; expecting {e_expected}"
        );
        assert!(
            da == da_expected,
            "unexpected da for fa={fa:?}; got\n{da:?}; expecting\n{da_expected:?}"
        );

        let da_prefix = [1i64, 2, 3];
        let (da, e) = append_float_to_decimal(da_prefix.to_vec(), fa);
        assert!(
            e == e_expected,
            "unexpected e for fa={fa:?}; got {e}; expecting {e_expected}"
        );
        assert!(
            da[..da_prefix.len()] == da_prefix,
            "unexpected daPrefix for fa={fa:?}; got\n{:?}; expecting\n{da_prefix:?}",
            &da[..da_prefix.len()]
        );
        assert!(
            da[da_prefix.len()..] == *da_expected,
            "unexpected da for fa={fa:?}; got\n{:?}; expecting\n{da_expected:?}",
            &da[da_prefix.len()..]
        );
    }

    #[test]
    fn test_float_to_decimal() {
        fn f(v: f64, v_expected: i64, e_expected: i16) {
            let (v_got, e) = from_float(v);
            assert!(
                v_got == v_expected,
                "unexpected v for f={v:e}; got {v_got}; expecting {v_expected}"
            );
            assert!(
                e == e_expected,
                "unexpected e for f={v:e}; got {e}; expecting {e_expected}"
            );
        }

        f(0.0, 0, 0);
        f(1.0, 1, 0);
        f(-1.0, -1, 0);
        f(0.9, 9, -1);
        f(0.99, 99, -2);
        f(9.0, 9, 0);
        f(99.0, 99, 0);
        f(20.0, 2, 1);
        f(100.0, 1, 2);
        f(3000.0, 3, 3);

        f(0.123, 123, -3);
        f(-0.123, -123, -3);
        f(1.2345, 12345, -4);
        f(-1.2345, -12345, -4);
        f(12000.0, 12, 3);
        f(-12000.0, -12, 3);
        f(1e-30, 1, -30);
        f(-1e-30, -1, -30);
        f(1e-260, 1, -260);
        f(-1e-260, -1, -260);
        f(321e260, 321, 260);
        f(-321e260, -321, 260);
        f(1234567890123.0, 1234567890123, 0);
        f(-1234567890123.0, -1234567890123, 0);
        f(123e5, 123, 5);
        f(15e18, 15, 18);

        f(f64::INFINITY, V_INF_POS, 0);
        f(f64::NEG_INFINITY, V_INF_NEG, 0);
        f(STALE_NAN, V_STALE_NAN, 0);
        f(V_INF_POS as f64, 9223372036854775, 3);
        f(V_INF_NEG as f64, -9223372036854775, 3);
        f(V_MAX as f64, 9223372036854775, 3);
        f(V_MIN as f64, -9223372036854775, 3);
        f(i64::MAX as f64, 9223372036854775, 3);
        f(i64::MIN as f64, -9223372036854775, 3);

        // Test precision loss due to conversionPrecision.
        f(0.1234567890123456, 12345678901234, -14);
        f(-123456.7890123456, -12345678901234, -8);
    }

    #[test]
    fn test_float_to_decimal_roundtrip() {
        fn f(v: f64) {
            let (d, e) = from_float(v);
            let f_new = to_float(d, e);
            assert!(
                equal_float(v, f_new),
                "unexpected fNew for v={d}, e={e}; got {f_new}; expecting {v}"
            );

            let (d, e) = from_float(-v);
            let f_new = to_float(d, e);
            assert!(
                equal_float(-v, f_new),
                "unexpected fNew for v={d}, e={e}; got {f_new}; expecting {}",
                -v
            );
        }

        f(0.0);
        f(1.0);
        f(0.123);
        f(1.2345);
        f(12000.0);
        f(1e-30);
        f(1e-260);
        f(321e260);
        f(1234567890123.0);
        f(12.34567890125);
        f(1234567.8901256789);
        f(15e18);
        f(0.000874957);
        f(0.001130435);

        f(2933434554455e245);
        f(3439234258934e-245);
        f(V_INF_POS as f64);
        f(V_INF_NEG as f64);
        f(INF_POS);
        f(INF_NEG);
        f(V_MAX as f64);
        f(V_MIN as f64);
        f(V_STALE_NAN as f64);

        // PORT NOTE: Go uses rand.NormFloat64 with seed 1; Rust std has no
        // compatible RNG, so a deterministic xorshift-based approximation of
        // normally distributed values is used instead. The exact values
        // differ, but the roundtrip property being verified is the same.
        let mut state: u64 = 1;
        let mut uniform = move || {
            // xorshift64
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f64 / (1u64 << 53) as f64
        };
        for _ in 0..10_000 {
            // Irwin-Hall approximation of the normal distribution.
            let v: f64 = (0..12).map(|_| uniform()).sum::<f64>() - 6.0;
            f(v);
            f(v * 1e-6);
            f(v * 1e6);

            f(round_float(v, 20));
            f(round_float(v, 10));
            f(round_float(v, 5));
            f(round_float(v, 0));
            f(round_float(v, -5));
            f(round_float(v, -10));
            f(round_float(v, -20));
        }
    }

    fn round_float(f: f64, exp: i32) -> f64 {
        let f = f * pow10(-exp);
        f.trunc() * pow10(exp)
    }

    fn equal_float(f1: f64, f2: f64) -> bool {
        if f1 == f64::INFINITY {
            return f2 == f64::INFINITY;
        }
        // PORT NOTE: the Go original checks `IsInf(f2, -1)` twice here
        // (a benign quirk in the upstream test); kept verbatim.
        if f2 == f64::NEG_INFINITY {
            return f2 == f64::NEG_INFINITY;
        }
        let eps = (f1 - f2).abs();
        eps == 0.0 || eps * CONVERSION_PRECISION < f1.abs() + f2.abs()
    }
}

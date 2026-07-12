//! Port of Go's `unicode.SimpleFold` as used by `regexp/syntax`.
//!
//! PORT NOTE: Go computes fold orbits from the `unicode` package's
//! `caseOrbit` table plus simple case mappings. The `caseOrbit` exceptions
//! are ported verbatim below; the fallback uses Rust std's
//! `char::to_lowercase`/`to_uppercase` restricted to single-char results,
//! which coincides with Go's simple case mappings for all code points except
//! a handful whose full mapping is multi-char (e.g. `İ` U+0130); those fold
//! to themselves here.

/// Minimum rune involved in case folding (Go `minFold`).
pub const MIN_FOLD: i32 = 0x0041;
/// Maximum rune involved in case folding (Go `maxFold`).
pub const MAX_FOLD: i32 = 0x1e943;

const MAX_RUNE: i32 = 0x10FFFF;

/// Go `unicode.caseOrbit`: special fold orbits with more than two elements
/// or crossing simple case mappings. Sorted by the first element.
const CASE_ORBIT: [(u32, u32); 88] = [
    (0x004B, 0x006B),
    (0x0053, 0x0073),
    (0x006B, 0x212A),
    (0x0073, 0x017F),
    (0x00B5, 0x039C),
    (0x00C5, 0x00E5),
    (0x00DF, 0x1E9E),
    (0x00E5, 0x212B),
    (0x0130, 0x0130),
    (0x0131, 0x0131),
    (0x017F, 0x0053),
    (0x01C4, 0x01C5),
    (0x01C5, 0x01C6),
    (0x01C6, 0x01C4),
    (0x01C7, 0x01C8),
    (0x01C8, 0x01C9),
    (0x01C9, 0x01C7),
    (0x01CA, 0x01CB),
    (0x01CB, 0x01CC),
    (0x01CC, 0x01CA),
    (0x01F1, 0x01F2),
    (0x01F2, 0x01F3),
    (0x01F3, 0x01F1),
    (0x0345, 0x0399),
    (0x0392, 0x03B2),
    (0x0395, 0x03B5),
    (0x0398, 0x03B8),
    (0x0399, 0x03B9),
    (0x039A, 0x03BA),
    (0x039C, 0x03BC),
    (0x03A0, 0x03C0),
    (0x03A1, 0x03C1),
    (0x03A3, 0x03C2),
    (0x03A6, 0x03C6),
    (0x03A9, 0x03C9),
    (0x03B2, 0x03D0),
    (0x03B5, 0x03F5),
    (0x03B8, 0x03D1),
    (0x03B9, 0x1FBE),
    (0x03BA, 0x03F0),
    (0x03BC, 0x00B5),
    (0x03C0, 0x03D6),
    (0x03C1, 0x03F1),
    (0x03C2, 0x03C3),
    (0x03C3, 0x03A3),
    (0x03C6, 0x03D5),
    (0x03C9, 0x2126),
    (0x03D0, 0x0392),
    (0x03D1, 0x03F4),
    (0x03D5, 0x03A6),
    (0x03D6, 0x03A0),
    (0x03F0, 0x039A),
    (0x03F1, 0x03A1),
    (0x03F4, 0x0398),
    (0x03F5, 0x0395),
    (0x0412, 0x0432),
    (0x0414, 0x0434),
    (0x041E, 0x043E),
    (0x0421, 0x0441),
    (0x0422, 0x0442),
    (0x042A, 0x044A),
    (0x0432, 0x1C80),
    (0x0434, 0x1C81),
    (0x043E, 0x1C82),
    (0x0441, 0x1C83),
    (0x0442, 0x1C84),
    (0x044A, 0x1C86),
    (0x0462, 0x0463),
    (0x0463, 0x1C87),
    (0x1C80, 0x0412),
    (0x1C81, 0x0414),
    (0x1C82, 0x041E),
    (0x1C83, 0x0421),
    (0x1C84, 0x1C85),
    (0x1C85, 0x0422),
    (0x1C86, 0x042A),
    (0x1C87, 0x0462),
    (0x1C88, 0xA64A),
    (0x1E60, 0x1E61),
    (0x1E61, 0x1E9B),
    (0x1E9B, 0x1E60),
    (0x1E9E, 0x00DF),
    (0x1FBE, 0x0345),
    (0x2126, 0x03A9),
    (0x212A, 0x004B),
    (0x212B, 0x00C5),
    (0xA64A, 0xA64B),
    (0xA64B, 0x1C88),
];

fn to_lower_simple(c: char) -> char {
    let mut it = c.to_lowercase();
    match (it.next(), it.next()) {
        (Some(l), None) => l,
        _ => c,
    }
}

fn to_upper_simple(c: char) -> char {
    let mut it = c.to_uppercase();
    match (it.next(), it.next()) {
        (Some(u), None) => u,
        _ => c,
    }
}

/// Port of Go's `unicode.SimpleFold`: iterates over Unicode code points
/// equivalent under the Unicode-defined simple case folding.
pub fn simple_fold(r: i32) -> i32 {
    if !(0..=MAX_RUNE).contains(&r) {
        return r;
    }

    // Consult caseOrbit table for special cases.
    if let Ok(idx) = CASE_ORBIT.binary_search_by_key(&(r as u32), |&(from, _)| from) {
        return CASE_ORBIT[idx].1 as i32;
    }

    // No folding specified. This is a one- or two-element
    // equivalence class containing rune and ToLower(rune)
    // and ToUpper(rune) if they are different from rune.
    let Some(c) = char::from_u32(r as u32) else {
        return r;
    };
    let l = to_lower_simple(c);
    if l != c {
        return l as i32;
    }
    to_upper_simple(c) as i32
}

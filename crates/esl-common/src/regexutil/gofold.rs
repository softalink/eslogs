//! Port of Go's `unicode.SimpleFold` as used by `regexp/syntax`.
//!
//! PORT NOTE: Go computes fold orbits from the `unicode` package's
//! `caseOrbit` table plus simple case mappings. The `caseOrbit` exceptions
//! are ported verbatim below; the fallback uses Rust std's
//! `char::to_lowercase`/`to_uppercase` restricted to single-char results,
//! plus explicit fixups for the Greek iota-subscript letters, whose FULL
//! uppercase is multi-char (hiding the single-rune SIMPLE mapping Go uses,
//! e.g. `ᾀ` U+1F80 ↔ `ᾈ` U+1F88). The code points with multi-char full
//! mappings and no simple mapping (`ß`, `İ`, `ı`, `ſ`, ligatures, …) are
//! covered by the `caseOrbit` table or map to themselves in Go as well, so
//! [`simple_fold`] matches `unicode.SimpleFold` exactly, modulo
//! Unicode-version skew between the Rust std tables and the Go toolchain's
//! (case pairs added in a newer Unicode version fold on one side only).

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
    // Greek letters with iota subscript (ypogegrammeni): their full uppercase
    // is multi-char (e.g. ᾀ → ἈΙ), which would hide the single-rune simple
    // uppercase mapping Go's unicode.ToUpper applies (ᾀ → ᾈ). Restore it.
    match c as u32 {
        0x1F80..=0x1F87 | 0x1F90..=0x1F97 | 0x1FA0..=0x1FA7 => {
            return char::from_u32(c as u32 + 8).unwrap();
        }
        0x1FB3 => return '\u{1FBC}',
        0x1FC3 => return '\u{1FCC}',
        0x1FF3 => return '\u{1FFC}',
        _ => {}
    }
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

#[cfg(test)]
mod tests {
    use super::simple_fold;

    /// Pins `simple_fold` against Go's `unicode.SimpleFold` outputs for the
    /// code points where the Rust-std fallback is known to be tricky
    /// (multi-char full mappings, caseOrbit exceptions, high-plane pairs).
    #[test]
    fn test_simple_fold_matches_go() {
        fn f(r: char, want: char) {
            let got = simple_fold(r as i32);
            assert_eq!(
                got, want as i32,
                "simple_fold({r:?}) = U+{got:04X}; want {want:?}"
            );
        }

        // ASCII and non-cased runes.
        f('A', 'a');
        f('a', 'A');
        f('1', '1');
        f('日', '日');
        // k / K / KELVIN SIGN orbit.
        f('K', 'k');
        f('k', '\u{212A}');
        f('\u{212A}', 'K');
        // s / S / LATIN SMALL LETTER LONG S orbit.
        f('S', 's');
        f('s', '\u{17F}');
        f('\u{17F}', 'S');
        // Turkish dotted/dotless I fold only to themselves.
        f('\u{130}', '\u{130}');
        f('\u{131}', '\u{131}');
        // ß / ẞ orbit.
        f('\u{DF}', '\u{1E9E}');
        f('\u{1E9E}', '\u{DF}');
        // µ / Μ / μ orbit.
        f('\u{B5}', '\u{39C}');
        f('\u{39C}', '\u{3BC}');
        f('\u{3BC}', '\u{B5}');
        // Σ / ς / σ orbit.
        f('\u{3A3}', '\u{3C2}');
        f('\u{3C2}', '\u{3C3}');
        f('\u{3C3}', '\u{3A3}');
        // Greek iota-subscript letters: the simple uppercase mapping is
        // hidden behind a multi-char full uppercase in Rust std.
        f('\u{1F80}', '\u{1F88}');
        f('\u{1F88}', '\u{1F80}');
        f('\u{1F97}', '\u{1F9F}');
        f('\u{1F9F}', '\u{1F97}');
        f('\u{1FA0}', '\u{1FA8}');
        f('\u{1FB3}', '\u{1FBC}');
        f('\u{1FBC}', '\u{1FB3}');
        f('\u{1FC3}', '\u{1FCC}');
        f('\u{1FF3}', '\u{1FFC}');
        // Cherokee pair (fold crosses into the supplementary block).
        f('\u{13A0}', '\u{AB70}');
        f('\u{AB70}', '\u{13A0}');
        // Adlam — the top of Go's fold range (maxFold = U+1E943).
        f('\u{1E900}', '\u{1E922}');
        f('\u{1E922}', '\u{1E900}');
    }
}

#[cfg(test)]
mod dumpfold {
    // Temporary one-off dump for cross-checking against Go; removed after use.
    #[test]
    #[ignore]
    fn dump() {
        use std::io::Write as _;
        let path = std::env::var("FOLD_DUMP_PATH").unwrap();
        let mut w = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
        for r in 0i32..=0x10FFFF {
            let f = super::simple_fold(r);
            if f != r {
                writeln!(w, "{r:x} {f:x}").unwrap();
            }
        }
    }
}

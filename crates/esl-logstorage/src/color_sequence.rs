//! Port of EsLogs `lib/logstorage/color_sequence.go`.

/// Returns true if s contains ANSI color escape sequences.
///
/// PORT NOTE: byte-native (values are raw bytes); ANSI escape sequences are
/// pure ASCII byte patterns, so scanning bytes matches the Go behavior.
pub fn has_color_sequences(s: &[u8]) -> bool {
    find_csi(s).is_some()
}

/// Position of the first `ESC [` (CSI) introducer in s.
fn find_csi(s: &[u8]) -> Option<usize> {
    s.windows(2).position(|w| w == b"\x1b[")
}

/// Removes ANSI escape sequences from src and appends the result to dst.
///
/// See <https://en.wikipedia.org/wiki/ANSI_escape_code>
///
/// PORT NOTE: Go appends to and returns `dst []byte`; the port mutates
/// `dst: &mut Vec<u8>` in place. Bytes outside escape sequences (including
/// invalid UTF-8) pass through unchanged.
pub fn drop_color_sequences(dst: &mut Vec<u8>, src: &[u8]) {
    let mut src = src;
    loop {
        let Some(n) = find_csi(src) else {
            dst.extend_from_slice(src);
            return;
        };
        dst.extend_from_slice(&src[..n]);
        src = &src[n + 2..];

        src = skip_ansi_sequence(src);
    }
}

/// Skips non-ansi escape sequence at the beginning of s and returns the position of the first byte after it.
fn skip_ansi_sequence(s: &[u8]) -> &[u8] {
    let b = s;
    let mut n = 0;

    // Skip optional parameter bytes after CSI (control sequence introducer).
    // See https://gist.github.com/ConnerWill/d4b6c776b509add763e17f9f113fd25b
    while n < b.len() {
        let ch = b[n];
        if !(0x30..=0x3f).contains(&ch) {
            break;
        }
        n += 1;
    }

    // Scan ansi escape sequence according to the chapter 13.1
    // at https://www.ecma-international.org/wp-content/uploads/ECMA-35_6th_edition_december_1994.pdf

    // skip optional intermediate bytes
    while n < b.len() {
        let ch = b[n];
        if !(0x20..=0x2f).contains(&ch) {
            break;
        }
        n += 1;
    }

    // skip the final byte
    if n < b.len() {
        let ch = b[n];
        if (0x30..=0x7e).contains(&ch) {
            n += 1;
        }
    }

    &s[n..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_color_sequences() {
        fn f(s: &str, result_expected: bool) {
            let result = has_color_sequences(s.as_bytes());
            assert_eq!(
                result, result_expected,
                "unexpected result; got {result}; want {result_expected}"
            );
        }

        f("", false);
        f("foo", false);
        f("\x1babc", false);
        f("\x1b[abc", true);
        f("axxb\x1b[", true);
        f("axxb\x1b[abc", true);
    }

    #[test]
    fn test_drop_color_sequences() {
        fn f(s: &str, result_expected: &str) {
            let mut result = Vec::new();
            drop_color_sequences(&mut result, s.as_bytes());
            assert_eq!(
                result,
                result_expected.as_bytes(),
                "unexpected result\ngot\n{result:?}\nwant\n{result_expected:?}"
            );
        }

        // empty string
        f("", "");

        // zero color escape sequences
        f("a", "a");
        f("FooBar", "FooBar");

        // invalid color escape sequence
        f("foo\x1b[\x01", "foo\x01");

        // valid color escape sequence
        // See https://gist.github.com/ConnerWill/d4b6c776b509add763e17f9f113fd25b#colors--graphics-mode
        f("\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5H", "fooERROR bar");
        f(
            "\x1b[mfoo\x1b[1;31mERROR bar\x1b[10;5Hbaz",
            "fooERROR barbaz",
        );

        // valid erase escape sequence
        // See https://gist.github.com/ConnerWill/d4b6c776b509add763e17f9f113fd25b#erase-functions
        f("foo\x1b[2Jbar", "foobar");

        // valid cursor controls escape sequence
        // See https://gist.github.com/ConnerWill/d4b6c776b509add763e17f9f113fd25b#cursor-controls
        f("abc\x1b[65;81fdef", "abcdef");

        // valid operating system command sequence. It is left as is.
        f(
            "\x1b]0;My Terminal Title\x07",
            "\x1b]0;My Terminal Title\x07",
        );

        // valid device control string sequence. It is left as is.
        f("a\x1bP 1;2;3 qabc\x1b\\", "a\x1bP 1;2;3 qabc\x1b\\");
    }
}

//! Port of EsLogs `lib/logstorage/tokenizer.go`.

use std::collections::HashSet;
use std::sync::OnceLock;

use regex::Regex;

/// Extracts word tokens from a and appends them to dst.
///
/// The order of appended tokens equals the order of tokens seen in a.
///
/// PORT NOTE: Go appends to `dst []string` and returns it; the port mutates
/// `dst` in place. Tokens borrow from the input strings, exactly like the Go
/// substrings do.
pub fn tokenize_strings<'a, S: AsRef<str>>(dst: &mut Vec<&'a str>, a: &'a [S]) {
    let mut t = get_tokenizer();
    for (i, s) in a.iter().enumerate() {
        let s = s.as_ref();
        if i > 0 && s == a[i - 1].as_ref() {
            // This string has been already tokenized
            continue;
        }
        t.tokenize_string(dst, s, false);
    }
    put_tokenizer(t);
}

/// Tokenizer splits strings into word tokens, deduplicating them across calls.
///
/// PORT NOTE: the Go `tokenizer` holds a `map[string]struct{}` whose keys
/// borrow from the tokenized strings, which is fine because Go pools it only
/// after `reset()`. Rust must name that borrow: `Tokenizer<'a>` is tied to
/// the lifetime of the tokenized strings, so it cannot be stored in a global
/// pool. `get_tokenizer`/`put_tokenizer` construct/drop a fresh tokenizer
/// instead of pooling (one `HashSet` allocation per use).
#[derive(Default)]
pub struct Tokenizer<'a> {
    m: HashSet<&'a str>,
}

impl<'a> Tokenizer<'a> {
    /// Clears the set of seen tokens.
    pub fn reset(&mut self) {
        self.m.clear();
    }

    /// Appends word tokens from s to dst.
    ///
    /// Unless `keep_duplicate_tokens` is set, tokens already seen by this
    /// tokenizer are skipped.
    pub fn tokenize_string(
        &mut self,
        dst: &mut Vec<&'a str>,
        s: &'a str,
        keep_duplicate_tokens: bool,
    ) {
        if !is_ascii(s) {
            // Slow path - s contains unicode chars
            self.tokenize_string_unicode(dst, s, keep_duplicate_tokens);
            return;
        }

        // Fast path for ASCII s
        let b = s.as_bytes();
        let mut i = 0usize;
        while i < b.len() {
            // Search for the next token.
            let mut start = b.len();
            while i < b.len() {
                if !is_token_char(b[i]) {
                    i += 1;
                    continue;
                }
                start = i;
                i += 1;
                break;
            }
            // Search for the end of the token.
            let mut end = b.len();
            while i < b.len() {
                if is_token_char(b[i]) {
                    i += 1;
                    continue;
                }
                end = i;
                i += 1;
                break;
            }
            if end <= start {
                break;
            }

            // Register the token.
            let token = &s[start..end];
            if keep_duplicate_tokens {
                dst.push(token);
            } else if !self.m.contains(token) {
                self.m.insert(token);
                dst.push(token);
            }
        }
    }

    fn tokenize_string_unicode(
        &mut self,
        dst: &mut Vec<&'a str>,
        s: &'a str,
        keep_duplicate_tokens: bool,
    ) {
        let mut s = s;
        while !s.is_empty() {
            // Search for the next token.
            let mut n = s.len();
            for (offset, r) in s.char_indices() {
                if is_token_rune(r) {
                    n = offset;
                    break;
                }
            }
            s = &s[n..];
            // Search for the end of the token.
            let mut n = s.len();
            for (offset, r) in s.char_indices() {
                if !is_token_rune(r) {
                    n = offset;
                    break;
                }
            }
            if n == 0 {
                break;
            }

            // Register the token
            let token = &s[..n];
            s = &s[n..];
            if keep_duplicate_tokens {
                dst.push(token);
            } else if !self.m.contains(token) {
                self.m.insert(token);
                dst.push(token);
            }
        }
    }
}

pub(crate) fn is_ascii(s: &str) -> bool {
    s.is_ascii()
}

/// Returns true if c is a token char (`[a-zA-Z0-9_]`).
///
/// PORT NOTE: Go precomputes a 256-byte lookup table; the direct predicate
/// below is equivalent.
pub(crate) fn is_token_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

/// Returns true if c is a token rune (a letter, a digit or `_`).
///
/// PORT NOTE: Go uses `unicode.IsLetter` (general category L) and
/// `unicode.IsDigit` (general category Nd). Rust's `char::is_alphabetic` /
/// `char::is_numeric` cover broader Unicode properties, so the port matches
/// the exact Go categories via the regex classes `\p{L}` and `\p{Nd}`.
pub(crate) fn is_token_rune(c: char) -> bool {
    if c.is_ascii() {
        // Fast path - the char is ASCII
        return is_token_char(c as u8);
    }
    if c == '_' {
        return true;
    }
    static LETTER_OR_DIGIT_RE: OnceLock<Regex> = OnceLock::new();
    let re = LETTER_OR_DIGIT_RE.get_or_init(|| Regex::new(r"[\p{L}\p{Nd}]").unwrap());
    let mut buf = [0u8; 4];
    re.is_match(c.encode_utf8(&mut buf))
}

/// Returns a tokenizer ready for use.
///
/// PORT NOTE: no pooling - see the `Tokenizer` docs.
pub fn get_tokenizer<'a>() -> Tokenizer<'a> {
    Tokenizer::default()
}

/// Releases the tokenizer obtained via [`get_tokenizer`].
pub fn put_tokenizer(mut t: Tokenizer<'_>) {
    t.reset();
    drop(t);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_strings() {
        fn f(a: &[&str], tokens_expected: &[&str]) {
            let mut tokens: Vec<&str> = Vec::new();
            tokenize_strings(&mut tokens, a);
            assert_eq!(
                tokens, tokens_expected,
                "unexpected tokens;\ngot\n{tokens:?}\nwant\n{tokens_expected:?}"
            );
        }
        f(&[], &[]);
        f(&[""], &[]);
        f(&["foo"], &["foo"]);
        f(
            &["foo bar---.!!([baz]!!! %$# TaSte"],
            &["foo", "bar", "baz", "TaSte"],
        );
        f(
            &["теСТ 1234 f12.34", "34 f12 AS"],
            &["теСТ", "1234", "f12", "34", "AS"],
        );
        let log_lines = "
Apr 28 13:43:38 localhost whoopsie[2812]: [13:43:38] online
Apr 28 13:45:01 localhost CRON[12181]: (root) CMD (command -v debian-sa1 > /dev/null && debian-sa1 1 1)
Apr 28 13:48:01 localhost kernel: [36020.497806] CPU0: Core temperature above threshold, cpu clock throttled (total events = 22034)
";
        let a: Vec<&str> = log_lines.split('\n').collect();
        f(
            &a,
            &[
                "Apr",
                "28",
                "13",
                "43",
                "38",
                "localhost",
                "whoopsie",
                "2812",
                "online",
                "45",
                "01",
                "CRON",
                "12181",
                "root",
                "CMD",
                "command",
                "v",
                "debian",
                "sa1",
                "dev",
                "null",
                "1",
                "48",
                "kernel",
                "36020",
                "497806",
                "CPU0",
                "Core",
                "temperature",
                "above",
                "threshold",
                "cpu",
                "clock",
                "throttled",
                "total",
                "events",
                "22034",
            ],
        );
    }
}

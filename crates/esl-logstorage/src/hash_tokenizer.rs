//! Port of EsLogs `lib/logstorage/hash_tokenizer.go`.

use std::sync::Mutex;

use xxhash_rust::xxh64::xxh64;

use crate::bitmap::Bitmap;
use crate::pattern_matcher::decode_rune;
use crate::tokenizer::{is_token_char, is_token_rune};

fn is_ascii(b: &[u8]) -> bool {
    b.is_ascii()
}

/// Extracts word tokens from a, hashes them and appends hashes to dst.
///
/// The appended hashes must be passed to `bloom_filter_marshal_hashes` in order to build bloom filters.
/// The appended hashes must be passed to `append_hashes_hashes` before being passed to `BloomFilter::contains_all`.
///
/// PORT NOTE: Go appends to `dst []uint64` and returns it; the port mutates
/// `dst` in place.
pub fn tokenize_hashes<S: AsRef<[u8]>>(dst: &mut Vec<u64>, a: &[S]) {
    let mut t = get_hash_tokenizer();
    for (i, s) in a.iter().enumerate() {
        let s = s.as_ref();
        if i > 0 && s == a[i - 1].as_ref() {
            // This string has been already tokenized
            continue;
        }
        t.tokenize_string(dst, s);
    }
    put_hash_tokenizer(t);
}

const HASH_TOKENIZER_BUCKETS_COUNT: usize = 1024;

/// HashTokenizer extracts word tokens from strings and hashes them,
/// deduplicating hashes across calls.
pub struct HashTokenizer {
    buckets: [HashTokenizerBucket; HASH_TOKENIZER_BUCKETS_COUNT],
    bm: Bitmap,
}

#[derive(Default)]
struct HashTokenizerBucket {
    v: u64,
    overflow: Vec<u64>,
}

impl HashTokenizerBucket {
    fn reset(&mut self) {
        // do not spend CPU time on clearing v and overflow items,
        // since they'll be overwritten with new items.
        self.overflow.clear();
    }
}

fn new_hash_tokenizer() -> Box<HashTokenizer> {
    let mut t = Box::new(HashTokenizer {
        buckets: std::array::from_fn(|_| HashTokenizerBucket::default()),
        bm: Bitmap::default(),
    });
    let buckets_len = t.buckets.len();
    t.bm.init(buckets_len);
    t
}

impl HashTokenizer {
    /// Resets the tokenizer state, so it forgets all the registered tokens.
    pub fn reset(&mut self) {
        if self.bm.ones_count() <= self.buckets.len() / 4 {
            let buckets = &mut self.buckets;
            self.bm.for_each_set_bit(|idx| {
                buckets[idx].reset();
                false
            });
        } else {
            for b in self.buckets.iter_mut() {
                b.reset();
            }
            let buckets_len = self.buckets.len();
            self.bm.init(buckets_len);
        }
    }

    /// Appends hashes for word tokens from s to dst, skipping already seen
    /// tokens. `s` is raw value bytes (Go strings are arbitrary bytes).
    pub fn tokenize_string(&mut self, dst: &mut Vec<u64>, s: &[u8]) {
        if !is_ascii(s) {
            // Slow path - s contains unicode chars
            self.tokenize_string_unicode(dst, s);
            return;
        }

        // Fast path for ASCII s
        let b = s;
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
            if let (h, true) = self.add_token(token) {
                dst.push(h);
            }
        }
    }

    fn tokenize_string_unicode(&mut self, dst: &mut Vec<u64>, s: &[u8]) {
        // Byte-native rune iteration with Go `utf8.DecodeRune` semantics:
        // an invalid byte decodes as (U+FFFD, 1), which is not a token rune,
        // so invalid bytes act as token separators - exactly like in Go.
        let mut s = s;
        while !s.is_empty() {
            // Search for the next token.
            let mut n = s.len();
            let mut offset = 0usize;
            while offset < s.len() {
                let (r, size) = decode_rune(&s[offset..]);
                if is_token_rune(r) {
                    n = offset;
                    break;
                }
                offset += size;
            }
            s = &s[n..];
            // Search for the end of the token.
            let mut n = s.len();
            let mut offset = 0usize;
            while offset < s.len() {
                let (r, size) = decode_rune(&s[offset..]);
                if !is_token_rune(r) {
                    n = offset;
                    break;
                }
                offset += size;
            }
            if n == 0 {
                break;
            }

            // Register the token
            let token = &s[..n];
            s = &s[n..];
            if let (h, true) = self.add_token(token) {
                dst.push(h);
            }
        }
    }

    fn add_token(&mut self, token: &[u8]) -> (u64, bool) {
        let h = xxh64(token, 0);
        let idx = (h % (self.buckets.len() as u64)) as usize;

        let b = &mut self.buckets[idx];
        if !self.bm.is_set_bit(idx) {
            b.v = h;
            self.bm.set_bit(idx);
            return (h, true);
        }

        if b.v == h {
            return (h, false);
        }
        if b.overflow.contains(&h) {
            return (h, false);
        }
        b.overflow.push(h);
        (h, true)
    }
}

/// Returns a hash tokenizer from the pool.
///
/// PORT NOTE: Go uses `sync.Pool`; the port uses a `Mutex<Vec<Box<..>>>` pool.
pub fn get_hash_tokenizer() -> Box<HashTokenizer> {
    HASH_TOKENIZER_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_else(new_hash_tokenizer)
}

/// Returns t to the pool.
pub fn put_hash_tokenizer(mut t: Box<HashTokenizer>) {
    t.reset();
    HASH_TOKENIZER_POOL.lock().unwrap().push(t);
}

static HASH_TOKENIZER_POOL: Mutex<Vec<Box<HashTokenizer>>> = Mutex::new(Vec::new());

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_hashes() {
        fn f(a: &[&str], hashes_expected: &[u64]) {
            let mut hashes: Vec<u64> = Vec::new();
            tokenize_hashes(&mut hashes, a);
            assert_eq!(
                hashes, hashes_expected,
                "unexpected hashes\ngot\n{hashes:X?}\nwant\n{hashes_expected:X?}"
            );
        }

        f(&[], &[]);
        f(&[""], &[]);
        f(&["foo"], &[0x33BF00A859C4BA3F]);
        f(&["foo foo", "!!foo //"], &[0x33BF00A859C4BA3F]);
        f(
            &["foo bar---.!!([baz]!!! %$# TaSte"],
            &[
                0x33BF00A859C4BA3F,
                0x48A37C90AD27A659,
                0x42598CF26A247404,
                0x34709F40A3286E46,
            ],
        );
        f(
            &["foo bar---.!!([baz]!!! %$# baz foo TaSte"],
            &[
                0x33BF00A859C4BA3F,
                0x48A37C90AD27A659,
                0x42598CF26A247404,
                0x34709F40A3286E46,
            ],
        );
        f(
            &["теСТ 1234 f12.34", "34 f12 AS"],
            &[
                0xFE846FA145CEABD1,
                0xD8316E61D84F6BA4,
                0x6D67BA71C4E03D10,
                0x5E8D522CA93563ED,
                0xED80AED10E029FC8,
            ],
        );
    }
}

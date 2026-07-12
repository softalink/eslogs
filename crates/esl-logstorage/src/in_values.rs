//! Port of `lib/logstorage/in_values.go`.

use std::collections::HashSet;
use std::sync::OnceLock;

use esl_common::encoding;

use crate::bloomfilter::{append_hashes_hashes, append_tokens_hashes};
use crate::hash_tokenizer::tokenize_hashes;
use crate::tokenizer::tokenize_strings;
use crate::values_encoder::{
    marshal_float64, try_parse_float64_exact, try_parse_int64, try_parse_ipv4,
    try_parse_timestamp_iso8601, try_parse_uint64,
};

/// inValues keeps values for in(...), contains_any(...) and contains_all(...)
/// filters.
///
/// PORT NOTE: Go's inValues.q (*Query) and qFieldName fields — used for
/// populating values from a subquery before filter execution — are deferred
/// until parser.go lands (Layer 4); values must be populated by the caller.
/// The String() method is deferred for the same reason (it needs
/// quoteTokenIfNeeded from parser.go).
///
/// PORT NOTE: each Go sync.Once + field pair is merged into a single
/// OnceLock field.
#[derive(Debug, Default)]
pub struct InValues {
    pub values: Vec<String>,

    tokens_hashes_any: OnceLock<(Vec<u64>, Vec<Vec<u64>>)>,

    tokens_hashes_all: OnceLock<Vec<u64>>,

    string_values: OnceLock<HashSet<String>>,

    // PORT NOTE: Go's map[string]struct{} keys hold the binary value encoding
    // sliced out of a shared buf; the Rust port uses HashSet<Vec<u8>> keys.
    uint8_values: OnceLock<HashSet<Vec<u8>>>,

    uint16_values: OnceLock<HashSet<Vec<u8>>>,

    uint32_values: OnceLock<HashSet<Vec<u8>>>,

    uint64_values: OnceLock<HashSet<Vec<u8>>>,

    int64_values: OnceLock<HashSet<Vec<u8>>>,

    float64_values: OnceLock<HashSet<Vec<u8>>>,

    ipv4_values: OnceLock<HashSet<Vec<u8>>>,

    timestamp_iso8601_values: OnceLock<HashSet<Vec<u8>>>,
}

impl InValues {
    pub fn new(values: Vec<String>) -> InValues {
        InValues {
            values,
            ..Default::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn has_empty_value(&self) -> bool {
        let m = self.get_string_values();
        m.contains("")
    }

    pub fn get_non_empty_values_len(&self) -> usize {
        let m = self.get_string_values();
        let mut n = m.len();
        if m.contains("") {
            n -= 1;
        }
        n
    }

    pub fn is_only_empty_value(&self) -> bool {
        self.values.len() == 1 && self.values[0].is_empty()
    }

    pub fn get_tokens_hashes_all(&self) -> &[u64] {
        self.tokens_hashes_all.get_or_init(|| {
            let mut tokens = Vec::new();
            tokenize_hashes(&mut tokens, &self.values);
            let mut hashes = Vec::new();
            append_hashes_hashes(&mut hashes, &tokens);
            hashes
        })
    }

    pub fn get_tokens_hashes_any(&self) -> (&[u64], &[Vec<u64>]) {
        let (common_tokens_hashes, token_sets_hashes) = self.tokens_hashes_any.get_or_init(|| {
            let (common_tokens, token_sets) = get_common_tokens_and_token_sets(&self.values);

            let mut common_tokens_hashes = Vec::new();
            append_tokens_hashes(&mut common_tokens_hashes, &common_tokens);

            // PORT NOTE: Go slices all the token set hashes out of a shared
            // hashesBuf (recycled at 60KiB) to reduce allocations; the Rust
            // port allocates a Vec per token set.
            let token_sets_hashes = token_sets
                .iter()
                .map(|tokens| {
                    let mut hashes = Vec::new();
                    append_tokens_hashes(&mut hashes, tokens);
                    hashes
                })
                .collect();

            (common_tokens_hashes, token_sets_hashes)
        });
        (common_tokens_hashes, token_sets_hashes)
    }

    pub fn get_string_values(&self) -> &HashSet<String> {
        self.string_values.get_or_init(|| {
            let values = &self.values;
            let mut m = HashSet::with_capacity(values.len());
            for v in values {
                m.insert(v.clone());
            }
            m
        })
    }

    pub fn get_uint8_values(&self) -> &HashSet<Vec<u8>> {
        self.uint8_values.get_or_init(|| {
            let values = &self.values;
            let mut m = HashSet::with_capacity(values.len());
            for v in values {
                let n = match try_parse_uint64(v) {
                    Some(n) if n < (1 << 8) => n,
                    _ => continue,
                };
                m.insert(vec![n as u8]);
            }
            m
        })
    }

    pub fn get_uint16_values(&self) -> &HashSet<Vec<u8>> {
        self.uint16_values.get_or_init(|| {
            let values = &self.values;
            let mut m = HashSet::with_capacity(values.len());
            for v in values {
                let n = match try_parse_uint64(v) {
                    Some(n) if n < (1 << 16) => n,
                    _ => continue,
                };
                let mut buf = Vec::with_capacity(2);
                encoding::marshal_uint16(&mut buf, n as u16);
                m.insert(buf);
            }
            m
        })
    }

    pub fn get_uint32_values(&self) -> &HashSet<Vec<u8>> {
        self.uint32_values.get_or_init(|| {
            let values = &self.values;
            let mut m = HashSet::with_capacity(values.len());
            for v in values {
                let n = match try_parse_uint64(v) {
                    Some(n) if n < (1 << 32) => n,
                    _ => continue,
                };
                let mut buf = Vec::with_capacity(4);
                encoding::marshal_uint32(&mut buf, n as u32);
                m.insert(buf);
            }
            m
        })
    }

    pub fn get_uint64_values(&self) -> &HashSet<Vec<u8>> {
        self.uint64_values.get_or_init(|| {
            let values = &self.values;
            let mut m = HashSet::with_capacity(values.len());
            for v in values {
                let n = match try_parse_uint64(v) {
                    Some(n) => n,
                    None => continue,
                };
                let mut buf = Vec::with_capacity(8);
                encoding::marshal_uint64(&mut buf, n);
                m.insert(buf);
            }
            m
        })
    }

    pub fn get_int64_values(&self) -> &HashSet<Vec<u8>> {
        self.int64_values.get_or_init(|| {
            let values = &self.values;
            let mut m = HashSet::with_capacity(values.len());
            for v in values {
                let n = match try_parse_int64(v) {
                    Some(n) => n,
                    None => continue,
                };
                let mut buf = Vec::with_capacity(8);
                encoding::marshal_int64(&mut buf, n);
                m.insert(buf);
            }
            m
        })
    }

    pub fn get_float64_values(&self) -> &HashSet<Vec<u8>> {
        self.float64_values.get_or_init(|| {
            let values = &self.values;
            let mut m = HashSet::with_capacity(values.len());
            for v in values {
                let f = match try_parse_float64_exact(v) {
                    Some(f) => f,
                    None => continue,
                };
                let mut buf = Vec::with_capacity(8);
                marshal_float64(&mut buf, f);
                m.insert(buf);
            }
            m
        })
    }

    pub fn get_ipv4_values(&self) -> &HashSet<Vec<u8>> {
        self.ipv4_values.get_or_init(|| {
            let values = &self.values;
            let mut m = HashSet::with_capacity(values.len());
            for v in values {
                let n = match try_parse_ipv4(v) {
                    Some(n) => n,
                    None => continue,
                };
                let mut buf = Vec::with_capacity(4);
                encoding::marshal_uint32(&mut buf, n);
                m.insert(buf);
            }
            m
        })
    }

    pub fn get_timestamp_iso8601_values(&self) -> &HashSet<Vec<u8>> {
        self.timestamp_iso8601_values.get_or_init(|| {
            let values = &self.values;
            let mut m = HashSet::with_capacity(values.len());
            for v in values {
                let n = match try_parse_timestamp_iso8601(v) {
                    Some(n) => n,
                    None => continue,
                };
                let mut buf = Vec::with_capacity(8);
                encoding::marshal_uint64(&mut buf, n as u64);
                m.insert(buf);
            }
            m
        })
    }
}

/// PORT NOTE: Go slices all the token sets out of a shared tokensBuf
/// (recycled at 60KiB) to reduce allocations; the Rust port collects borrowed
/// tokens per value instead.
pub fn get_common_tokens_and_token_sets(values: &[String]) -> (Vec<&str>, Vec<Vec<&str>>) {
    let mut token_sets: Vec<Vec<&str>> = Vec::with_capacity(values.len());
    for v in values {
        let mut tokens = Vec::new();
        tokenize_strings(&mut tokens, std::slice::from_ref(v));
        token_sets.push(tokens);
    }

    let common_tokens = get_common_tokens(&token_sets);
    if common_tokens.is_empty() {
        return (common_tokens, token_sets);
    }

    // remove commonTokens from tokenSets
    for tokens in &mut token_sets {
        tokens.retain(|token| !common_tokens.contains(token));
    }

    (common_tokens, token_sets)
}

/// Returns common tokens seen at every set of tokens inside token_sets.
///
/// The returned common tokens preserve the original order seen in token_sets.
fn get_common_tokens<'a>(token_sets: &[Vec<&'a str>]) -> Vec<&'a str> {
    if token_sets.is_empty() {
        return Vec::new();
    }

    let mut common_tokens = token_sets[0].clone();

    for tokens in &token_sets[1..] {
        if common_tokens.is_empty() {
            return Vec::new();
        }
        common_tokens.retain(|token| tokens.contains(token));
    }
    common_tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_common_tokens_and_token_sets() {
        fn f(values: &[&str], common_tokens_expected: &[&str], token_sets_expected: &[&[&str]]) {
            let values: Vec<String> = values.iter().map(|s| s.to_string()).collect();
            let (mut common_tokens, token_sets) = get_common_tokens_and_token_sets(&values);
            common_tokens.sort_unstable();

            assert_eq!(
                common_tokens, common_tokens_expected,
                "unexpected commonTokens for values={values:?}"
            );

            for (i, mut tokens) in token_sets.into_iter().enumerate() {
                tokens.sort_unstable();
                let tokens_expected = token_sets_expected[i];
                assert_eq!(
                    tokens, tokens_expected,
                    "unexpected tokens for value={:?}",
                    values[i]
                );
            }
        }

        f(&[], &[], &[]);
        f(&["foo"], &["foo"], &[&[]]);
        f(&["foo", "foo"], &["foo"], &[&[], &[]]);
        f(
            &["foo", "bar", "bar", "foo"],
            &[],
            &[&["foo"], &["bar"], &["bar"], &["foo"]],
        );
        f(
            &["foo", "foo bar", "bar foo"],
            &["foo"],
            &[&[], &["bar"], &["bar"]],
        );
        f(
            &["a foo bar", "bar abc foo", "foo abc a bar"],
            &["bar", "foo"],
            &[&["a"], &["abc"], &["a", "abc"]],
        );
        f(
            &["a xfoo bar", "xbar abc foo", "foo abc a bar"],
            &[],
            &[
                &["a", "bar", "xfoo"],
                &["abc", "foo", "xbar"],
                &["a", "abc", "bar", "foo"],
            ],
        );
    }
}

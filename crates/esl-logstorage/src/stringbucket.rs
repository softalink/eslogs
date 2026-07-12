//! Port of EsLogs `lib/logstorage/stringbucket.go`.

use std::sync::Mutex;

/// StringBucket is a reusable bucket of strings.
#[derive(Default)]
pub struct StringBucket {
    pub a: Vec<String>,
}

impl StringBucket {
    /// Resets the bucket.
    ///
    /// PORT NOTE: Go `clear(sb.a)` zeroes the string headers before
    /// truncating, so the underlying strings can be garbage-collected;
    /// `Vec::clear` drops them while keeping the capacity.
    pub fn reset(&mut self) {
        self.a.clear();
    }
}

/// Obtains a string bucket from the pool.
pub fn get_string_bucket() -> StringBucket {
    STRING_BUCKET_POOL.lock().unwrap().pop().unwrap_or_default()
}

/// Returns sb to the pool.
pub fn put_string_bucket(mut sb: StringBucket) {
    sb.reset();
    STRING_BUCKET_POOL.lock().unwrap().push(sb);
}

// PORT NOTE: Go uses `sync.Pool` with `*stringBucket`; the port uses a
// `Mutex<Vec<StringBucket>>` pool and hands buckets out by value, preserving
// the reuse pattern.
static STRING_BUCKET_POOL: Mutex<Vec<StringBucket>> = Mutex::new(Vec::new());

// PORT NOTE: the Go package has no stringbucket_test.go; this is a minimal
// Rust-side sanity check for the pool/reset behavior.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_bucket_pool() {
        let mut sb = get_string_bucket();
        assert!(sb.a.is_empty());
        sb.a.push("foo".to_string());
        sb.a.push("bar".to_string());
        put_string_bucket(sb);

        let sb = get_string_bucket();
        assert!(sb.a.is_empty());
        put_string_bucket(sb);
    }
}

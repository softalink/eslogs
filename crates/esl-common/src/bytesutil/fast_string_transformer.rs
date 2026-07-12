//! Port of `lib/bytesutil/fast_string_transformer.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::fast_string_matcher::need_cleanup;
use super::internstring::{cache_expire_duration, is_skip_cache};
use super::unix_timestamp;

/// FastStringTransformer implements fast transformer for strings.
///
/// It caches transformed strings and returns them back on the next calls
/// without calling the transform_func, which may be expensive.
///
/// PORT NOTE: Go uses `sync.Map` and returns `string`; the port uses
/// `Mutex<HashMap>` and returns the shared `Arc<str>` so cache hits stay
/// allocation-free.
pub struct FastStringTransformer {
    last_cleanup_time: AtomicU64,

    m: Mutex<HashMap<String, Arc<FstEntry>>>,

    transform_func: Box<dyn Fn(&str) -> String + Send + Sync>,
}

struct FstEntry {
    last_access_time: AtomicU64,
    s: Arc<str>,
}

impl FastStringTransformer {
    /// Creates new transformer, which applies `transform_func` to strings
    /// passed to [`FastStringTransformer::transform`].
    ///
    /// `transform_func` must return the same result for the same input.
    pub fn new(transform_func: impl Fn(&str) -> String + Send + Sync + 'static) -> Self {
        FastStringTransformer {
            last_cleanup_time: AtomicU64::new(unix_timestamp()),
            m: Mutex::new(HashMap::new()),
            transform_func: Box::new(transform_func),
        }
    }

    /// Applies transform_func to `s` and returns the result.
    pub fn transform(&self, s: &str) -> Arc<str> {
        if is_skip_cache(s) {
            // PORT NOTE: Go clones the result when it equals s to drop
            // references to a possible bigger backing string; the Rust
            // transform_func already returns an owned String.
            return Arc::from((self.transform_func)(s));
        }

        let ct = unix_timestamp();
        let e = self.m.lock().unwrap().get(s).cloned();
        if let Some(e) = e {
            // Fast path - the transformed s is found in the cache.
            if e.last_access_time.load(Ordering::SeqCst) + 10 < ct {
                // Reduce the frequency of last_access_time update to once per 10 seconds
                // in order to improve the fast path speed on systems with many CPU cores.
                e.last_access_time.store(ct, Ordering::SeqCst);
            }
            return e.s.clone();
        }
        // Slow path - transform s and store it in the cache.
        let s_transformed: Arc<str> = Arc::from((self.transform_func)(s));
        let e = Arc::new(FstEntry {
            last_access_time: AtomicU64::new(ct),
            s: s_transformed.clone(),
        });
        // The insert copies s, which limits memory usage to the s length,
        // since s may point into a bigger string.
        self.m.lock().unwrap().insert(s.to_string(), e);

        if need_cleanup(&self.last_cleanup_time, ct) {
            // Perform a global cleanup for self.m by removing items, which
            // weren't accessed during the last 5 minutes.
            let deadline = ct - cache_expire_duration().as_secs();
            self.m
                .lock()
                .unwrap()
                .retain(|_, e| e.last_access_time.load(Ordering::SeqCst) >= deadline);
        }

        s_transformed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_string_transformer() {
        let fst = FastStringTransformer::new(|s: &str| s.to_uppercase());
        let f = |s: &str, result_expected: &str| {
            for i in 0..10 {
                let result = fst.transform(s);
                assert_eq!(
                    &*result, result_expected,
                    "unexpected result for transform({s:?}) at iteration {i}; got {result:?}; want {result_expected:?}"
                );
            }
        };
        f("", "");
        f("foo", "FOO");
        f("a_b-C", "A_B-C");
    }
}

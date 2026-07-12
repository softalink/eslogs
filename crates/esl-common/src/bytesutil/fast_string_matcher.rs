//! Port of `lib/bytesutil/fast_string_matcher.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::internstring::{cache_expire_duration, is_skip_cache};
use super::unix_timestamp;

/// FastStringMatcher implements fast matcher for strings.
///
/// It caches string match results and returns them back on the next calls
/// without calling the match_func, which may be expensive.
///
/// PORT NOTE: Go uses `sync.Map`; the port uses `Mutex<HashMap>` with
/// `Arc`-shared entries so the atomic last-access bookkeeping happens outside
/// the map lock, matching the Go access pattern.
pub struct FastStringMatcher {
    last_cleanup_time: AtomicU64,

    m: Mutex<HashMap<String, Arc<FsmEntry>>>,

    match_func: Box<dyn Fn(&str) -> bool + Send + Sync>,
}

struct FsmEntry {
    last_access_time: AtomicU64,
    ok: bool,
}

impl FastStringMatcher {
    /// Creates new matcher, which applies `match_func` to strings passed to
    /// [`FastStringMatcher::matches`].
    ///
    /// `match_func` must return the same result for the same input.
    pub fn new(match_func: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        FastStringMatcher {
            last_cleanup_time: AtomicU64::new(unix_timestamp()),
            m: Mutex::new(HashMap::new()),
            match_func: Box::new(match_func),
        }
    }

    /// Applies match_func to `s` and returns the result.
    ///
    /// PORT NOTE: named `matches` because `match` is a Rust keyword (Go:
    /// `Match`).
    pub fn matches(&self, s: &str) -> bool {
        if is_skip_cache(s) {
            return (self.match_func)(s);
        }

        let ct = unix_timestamp();
        let e = self.m.lock().unwrap().get(s).cloned();
        if let Some(e) = e {
            // Fast path - s match result is found in the cache.
            if e.last_access_time.load(Ordering::SeqCst) + 10 < ct {
                // Reduce the frequency of last_access_time update to once per 10 seconds
                // in order to improve the fast path speed on systems with many CPU cores.
                e.last_access_time.store(ct, Ordering::SeqCst);
            }
            return e.ok;
        }
        // Slow path - run match_func for s and store the result in the cache.
        let b = (self.match_func)(s);
        let e = Arc::new(FsmEntry {
            last_access_time: AtomicU64::new(ct),
            ok: b,
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

        b
    }
}

pub(super) fn need_cleanup(last_cleanup_time: &AtomicU64, current_time: u64) -> bool {
    let lct = last_cleanup_time.load(Ordering::SeqCst);
    if lct + 61 >= current_time {
        return false;
    }
    // Atomically compare and swap the current time with the last_cleanup_time
    // in order to guarantee that only a single thread out of multiple
    // concurrently executing threads gets true from the call.
    last_cleanup_time
        .compare_exchange(lct, current_time, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_string_matcher() {
        let fsm = FastStringMatcher::new(|s: &str| s.starts_with("foo"));
        let f = |s: &str, result_expected: bool| {
            for i in 0..10 {
                let result = fsm.matches(s);
                assert_eq!(
                    result, result_expected,
                    "unexpected result for matches({s:?}) at iteration {i}; got {result}; want {result_expected}"
                );
            }
        };
        f("", false);
        f("foo", true);
        f("a_b-C", false);
        f("foobar", true);
    }

    #[test]
    fn test_need_cleanup() {
        fn f(last_cleanup_time: u64, current_time: u64, result_expected: bool) {
            let lct = AtomicU64::new(last_cleanup_time);
            let result = need_cleanup(&lct, current_time);
            assert_eq!(
                result, result_expected,
                "unexpected result for need_cleanup({last_cleanup_time}, {current_time}); got {result}; want {result_expected}"
            );
            if result {
                let n = lct.load(Ordering::SeqCst);
                assert_eq!(
                    n, current_time,
                    "unexpected value for lct; got {n}; want current_time={current_time}"
                );
            } else {
                let n = lct.load(Ordering::SeqCst);
                assert_eq!(
                    n, last_cleanup_time,
                    "unexpected value for lct; got {n}; want last_cleanup_time={last_cleanup_time}"
                );
            }
        }
        f(0, 0, false);
        f(0, 61, false);
        f(0, 62, true);
        f(10, 100, true);
    }
}

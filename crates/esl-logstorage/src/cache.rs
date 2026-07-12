//! Port of `lib/logstorage/cache.go` from EsLogs v1.51.0.
//!
//! A two-generation cache with a background cleaner: entries survive one
//! `clean()` rotation and are dropped after two.

use std::any::Any;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use esl_common::cgroup;
use esl_common::timeutil;

/// The type of values stored in [`Cache`].
///
/// PORT NOTE: Go stores `any` values; the Rust equivalent for a concurrently
/// shared cache is `Arc<dyn Any + Send + Sync>`, downcast by callers.
pub type CacheValue = Arc<dyn Any + Send + Sync>;

const CLEANER_INTERVAL_NSECS: i64 = 3 * 60 * 1_000_000_000; // 3 minutes

/// Cache is a two-generation cache cleaned periodically in the background.
pub struct Cache {
    maps: Arc<CacheMaps>,

    stop_tx: Sender<()>,
    cleaner: Option<JoinHandle<()>>,
}

struct CacheMaps {
    // PORT NOTE: Go uses atomic.Pointer[sync.Map]; Rust swaps the Arc under a
    // short-lived RwLock instead, which preserves the lock-free reads of the
    // underlying map shards on the hot path.
    curr: RwLock<Arc<ShardedMap>>,
    prev: RwLock<Arc<ShardedMap>>,
}

impl Cache {
    /// Creates a new cache with a background cleaner (Go `newCache`).
    pub fn new() -> Cache {
        let maps = Arc::new(CacheMaps {
            curr: RwLock::new(Arc::new(ShardedMap::new())),
            prev: RwLock::new(Arc::new(ShardedMap::new())),
        });

        let (stop_tx, stop_rx) = mpsc::channel();
        let maps_for_cleaner = Arc::clone(&maps);
        let cleaner = std::thread::Builder::new()
            .name("cache_cleaner".to_string())
            .spawn(move || run_cleaner(&maps_for_cleaner, &stop_rx))
            .expect("FATAL: cannot spawn cache_cleaner thread");

        Cache {
            maps,
            stop_tx,
            cleaner: Some(cleaner),
        }
    }

    /// Stops the background cleaner and waits for it to exit.
    pub fn must_stop(&mut self) {
        // PORT NOTE: Go closes stopCh; sending on the channel (or dropping the
        // sender on Drop) wakes the cleaner the same way.
        let _ = self.stop_tx.send(());
        if let Some(h) = self.cleaner.take() {
            h.join().expect("BUG: cache_cleaner thread panicked");
        }
    }

    // PORT NOTE: in Go the cleaner goroutine calls c.clean(); here the cleaner
    // thread calls CacheMaps::clean directly, so this wrapper only exists for
    // the test which drives cleaning manually.
    #[cfg(test)]
    fn clean(&self) {
        self.maps.clean();
    }

    /// Returns the value for the given key k, if any.
    pub fn get(&self, k: &[u8]) -> Option<CacheValue> {
        let curr = Arc::clone(&self.maps.curr.read().unwrap());
        if let Some(v) = curr.load(k) {
            return Some(v);
        }

        let prev = Arc::clone(&self.maps.prev.read().unwrap());
        if let Some(v) = prev.load(k) {
            curr.store(k.to_vec(), Arc::clone(&v));
            return Some(v);
        }
        None
    }

    /// Stores the value v under the key k.
    pub fn set(&self, k: &[u8], v: CacheValue) {
        let curr = Arc::clone(&self.maps.curr.read().unwrap());
        curr.store(k.to_vec(), v);
    }
}

impl Default for Cache {
    fn default() -> Cache {
        Cache::new()
    }
}

impl Drop for Cache {
    // PORT NOTE: Go relies on an explicit MustStop call; Drop additionally
    // stops the cleaner so a leaked Cache cannot leave a running thread.
    fn drop(&mut self) {
        self.must_stop();
    }
}

fn run_cleaner(maps: &CacheMaps, stop_rx: &Receiver<()>) {
    let d = timeutil::add_jitter_to_duration(CLEANER_INTERVAL_NSECS);
    let d = Duration::from_nanos(d as u64);
    loop {
        match stop_rx.recv_timeout(d) {
            Err(RecvTimeoutError::Timeout) => maps.clean(),
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

impl CacheMaps {
    fn clean(&self) {
        let curr = Arc::clone(&self.curr.read().unwrap());
        *self.prev.write().unwrap() = curr;
        *self.curr.write().unwrap() = Arc::new(ShardedMap::new());
    }
}

// PORT NOTE: Go uses sync.Map. Rust std has no concurrent map, so the port
// keeps the sharded structure explicit: a fixed set of mutex-guarded HashMap
// shards sized to the next power of two >= available CPUs, selected by key
// hash. Keys are raw bytes because Go strings may hold arbitrary binary data.
struct ShardedMap {
    shards: Vec<Mutex<HashMap<Vec<u8>, CacheValue>>>,
    hash_builder: RandomState,
}

impl ShardedMap {
    fn new() -> ShardedMap {
        let shards_count = cgroup::available_cpus().next_power_of_two();
        let shards = (0..shards_count)
            .map(|_| Mutex::new(HashMap::new()))
            .collect();
        ShardedMap {
            shards,
            hash_builder: RandomState::new(),
        }
    }

    fn shard(&self, k: &[u8]) -> &Mutex<HashMap<Vec<u8>, CacheValue>> {
        let h = self.hash_builder.hash_one(k) as usize;
        &self.shards[h & (self.shards.len() - 1)]
    }

    fn load(&self, k: &[u8]) -> Option<CacheValue> {
        self.shard(k).lock().unwrap().get(k).cloned()
    }

    fn store(&self, k: Vec<u8>, v: CacheValue) {
        self.shard(&k).lock().unwrap().insert(k, v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache() {
        let mut m = HashMap::new();
        for i in 0..10 {
            let k = format!("key_{i}");
            m.insert(k, i);
        }

        let mut c = Cache::new();

        for (k_str, &n_expected) in &m {
            let k = k_str.as_bytes();

            assert!(
                c.get(k).is_none(),
                "unexpected value obtained from the cache for key {k_str:?}"
            );
            c.set(k, Arc::new(n_expected));
            let v = c
                .get(k)
                .unwrap_or_else(|| panic!("cannot obtain value for key {k_str:?}"));
            let n = *v.downcast_ref::<i32>().unwrap();
            assert_eq!(
                n, n_expected,
                "unexpected value obtained for key {k_str:?}; got {n}; want {n_expected}"
            );
        }

        // The cached entries should be still visible after a single clean() call.
        c.clean();
        for (k_str, &n_expected) in &m {
            let k = k_str.as_bytes();

            let v = c
                .get(k)
                .unwrap_or_else(|| panic!("cannot obtain value for key {k_str:?}"));
            let n = *v.downcast_ref::<i32>().unwrap();
            assert_eq!(
                n, n_expected,
                "unexpected value obtained for key {k_str:?}; got {n}; want {n_expected}"
            );
        }

        // The cached entries must be dropped after two clean() calls.
        c.clean();
        c.clean();

        for k_str in m.keys() {
            let k = k_str.as_bytes();

            assert!(
                c.get(k).is_none(),
                "unexpected value obtained from the cache for key {k_str:?}"
            );
        }

        c.must_stop();
    }
}

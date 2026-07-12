//! Port of Softalink LLC `lib/atomicutil`.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// CACHE_LINE_SIZE is the size of a CPU cache line.
// PORT NOTE: Go obtains this from `golang.org/x/sys/cpu.CacheLinePad`. Rust has
// no std equivalent, so the per-architecture values used by x/sys/cpu are
// inlined here (128 bytes on aarch64/powerpc64, 64 bytes elsewhere).
#[cfg(any(target_arch = "aarch64", target_arch = "powerpc64"))]
pub const CACHE_LINE_SIZE: usize = 128;
/// CACHE_LINE_SIZE is the size of a CPU cache line.
// PORT NOTE: see the aarch64/powerpc64 variant above.
#[cfg(not(any(target_arch = "aarch64", target_arch = "powerpc64")))]
pub const CACHE_LINE_SIZE: usize = 64;

/// Uint64 is like `AtomicU64`, but is protected from false sharing.
// PORT NOTE: Go pads the embedded `atomic.Uint64` with leading and trailing
// byte arrays. Rust achieves the same effect with `repr(align)`: the struct
// size is rounded up to its alignment, so a `Uint64` occupies whole cache
// lines and never shares one with a neighboring value.
#[cfg_attr(
    any(target_arch = "aarch64", target_arch = "powerpc64"),
    repr(align(128))
)]
#[cfg_attr(
    not(any(target_arch = "aarch64", target_arch = "powerpc64")),
    repr(align(64))
)]
#[derive(Debug, Default)]
pub struct Uint64 {
    v: AtomicU64,
}

impl Uint64 {
    /// Returns a new Uint64 initialized with the given value.
    pub const fn new(v: u64) -> Self {
        Self {
            v: AtomicU64::new(v),
        }
    }

    /// Atomically loads the value.
    pub fn load(&self) -> u64 {
        self.v.load(Ordering::SeqCst)
    }

    /// Atomically stores the given value.
    pub fn store(&self, v: u64) {
        self.v.store(v, Ordering::SeqCst)
    }

    /// Atomically adds delta and returns the new value (like Go `atomic.Uint64.Add`).
    pub fn add(&self, delta: u64) -> u64 {
        self.v
            .fetch_add(delta, Ordering::SeqCst)
            .wrapping_add(delta)
    }

    /// Atomically swaps in the given value and returns the previous one.
    pub fn swap(&self, v: u64) -> u64 {
        self.v.swap(v, Ordering::SeqCst)
    }

    /// Atomically replaces old with new if the current value equals old.
    /// Returns true on success (like Go `atomic.Uint64.CompareAndSwap`).
    pub fn compare_and_swap(&self, old: u64, new: u64) -> bool {
        self.v
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

/// Padded wraps a T so it occupies whole cache lines.
///
/// The padding prevents false sharing of neighboring items on multi-CPU systems.
// PORT NOTE: Go's private `itemPadded[T]` appends a `[CacheLineSize]byte` pad;
// Rust uses `repr(align)` (see Uint64 above), which is public here because
// `Slice::get` hands out `Arc<Padded<T>>` instead of Go's `*T`.
#[cfg_attr(
    any(target_arch = "aarch64", target_arch = "powerpc64"),
    repr(align(128))
)]
#[cfg_attr(
    not(any(target_arch = "aarch64", target_arch = "powerpc64")),
    repr(align(64))
)]
#[derive(Debug, Default)]
pub struct Padded<T> {
    x: T,
}

impl<T> Deref for Padded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.x
    }
}

impl<T> DerefMut for Padded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.x
    }
}

/// Optional callback for initializing items created by [`Slice`].
pub type InitFn<T> = Box<dyn Fn(&mut T) + Send + Sync>;

/// Slice allows thread-safe access to `T` items with automatic growth of the slice.
///
/// This is a replacement for `[workers_count]T` where `workers_count` isn't known beforehand.
///
/// It also prevents from false sharing of the created T items on multi-CPU systems.
// PORT NOTE: Go grows a `[]*itemPadded[T]` lock-free via CAS on an atomic
// pointer and returns `*T` under the contract that a single goroutine owns a
// given workerID. Safe Rust cannot express that aliasing contract, so the
// port uses an `RwLock` (reads on the fast path, writes only while growing)
// and returns `Arc<Padded<T>>`; callers needing mutation use interior
// mutability inside T, matching the Go usage pattern.
#[derive(Default)]
pub struct Slice<T> {
    /// Optional callback for initializing the created item x.
    pub init: Option<InitFn<T>>,

    p: RwLock<Vec<Arc<Padded<T>>>>,
}

impl<T: Default> Slice<T> {
    /// Returns the T item for the given worker_id in a thread-safe manner.
    ///
    /// The returned item is automatically created on the first access.
    pub fn get(&self, worker_id: usize) -> Arc<Padded<T>> {
        {
            let a = self.p.read().unwrap();
            if worker_id < a.len() {
                // Fast path - return already created item.
                return Arc::clone(&a[worker_id]);
            }
        }

        // Slow path - create the item, since it is missing.
        self.get_slow(worker_id)
    }

    fn get_slow(&self, worker_id: usize) -> Arc<Padded<T>> {
        let mut a = self.p.write().unwrap();
        while a.len() <= worker_id {
            let mut x = Padded::<T>::default();
            if let Some(init) = &self.init {
                init(&mut x.x);
            }
            a.push(Arc::new(x));
        }
        Arc::clone(&a[worker_id])
    }

    /// Returns the underlying items.
    ///
    /// The length of the returned vec equals to the max(worker_id)+1 passed to `get()`.
    ///
    /// `all()` is relatively slow, so it shouldn't be called in hot paths.
    pub fn all(&self) -> Vec<Arc<Padded<T>>> {
        self.p.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;
    use std::sync::Mutex;
    use std::thread;

    #[test]
    fn test_slice_no_init() {
        const WORKERS_COUNT: usize = 10;
        const LOOPS_PER_WORKER: usize = 100;

        let s: Slice<Mutex<String>> = Slice::default();
        let bbs = s.all();
        assert!(
            bbs.is_empty(),
            "unexpected length of slice: {}; want 0",
            bbs.len()
        );

        thread::scope(|scope| {
            for worker_id in 0..WORKERS_COUNT {
                let s = &s;
                scope.spawn(move || {
                    for i in 0..LOOPS_PER_WORKER {
                        let bb = s.get(worker_id);
                        writeln!(bb.lock().unwrap(), "item {i} at worker {worker_id}").unwrap();
                    }
                });
            }
        });

        let bbs = s.all();
        for (worker_id, bb) in bbs.iter().enumerate().take(WORKERS_COUNT) {
            let mut bb_expected = String::new();
            for i in 0..LOOPS_PER_WORKER {
                writeln!(bb_expected, "item {i} at worker {worker_id}").unwrap();
            }

            let result = bb.lock().unwrap().clone();
            assert_eq!(
                result, bb_expected,
                "unexpected result for worker {worker_id}\ngot\n{result:?}\nwant\n{bb_expected:?}"
            );
        }
    }

    #[test]
    fn test_slice_init() {
        const WORKERS_COUNT: usize = 10;
        const LOOPS_PER_WORKER: usize = 100;
        const PREFIX: &str = "foobar_prefix: ";

        let s: Slice<Mutex<String>> = Slice {
            init: Some(Box::new(|bb: &mut Mutex<String>| {
                bb.get_mut().unwrap().push_str(PREFIX);
            })),
            ..Default::default()
        };
        let bbs = s.all();
        assert!(
            bbs.is_empty(),
            "unexpected length of slice: {}; want 0",
            bbs.len()
        );

        thread::scope(|scope| {
            for worker_id in 0..WORKERS_COUNT {
                let s = &s;
                scope.spawn(move || {
                    for i in 0..LOOPS_PER_WORKER {
                        let bb = s.get(worker_id);
                        writeln!(bb.lock().unwrap(), "item {i} at worker {worker_id}").unwrap();
                    }
                });
            }
        });

        let bbs = s.all();
        for (worker_id, bb) in bbs.iter().enumerate().take(WORKERS_COUNT) {
            let mut bb_expected = String::from(PREFIX);
            for i in 0..LOOPS_PER_WORKER {
                writeln!(bb_expected, "item {i} at worker {worker_id}").unwrap();
            }

            let result = bb.lock().unwrap().clone();
            assert_eq!(
                result, bb_expected,
                "unexpected result for worker {worker_id}\ngot\n{result:?}\nwant\n{bb_expected:?}"
            );
        }
    }
}

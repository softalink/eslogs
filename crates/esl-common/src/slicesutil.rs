//! Port of Softalink LLC `lib/slicesutil`.
//!
//! PORT NOTE: the Go helpers take a slice and return a (possibly newly
//! allocated) slice sharing or replacing the original storage. Rust `Vec`
//! owns its storage, so the ported helpers mutate the `Vec` in place instead
//! of returning it.

use std::sync::Mutex;

/// Sets the length of `a` to `new_len`.
///
/// It may allocate when the capacity of `a` is smaller than `new_len`.
///
/// PORT NOTE: Go's `SetLength` re-exposes whatever bytes happen to live
/// between `len` and `cap` (callers treat that region as scratch to
/// overwrite). Rust cannot expose uninitialized memory, so the new region is
/// filled with `T::default()`. Shrinking drops the tail elements instead of
/// keeping them hidden within the capacity.
pub fn set_length<T: Default>(a: &mut Vec<T>, new_len: usize) {
    a.resize_with(new_len, T::default);
}

/// Extends the capacity of `a`, so `items_to_add` items can be appended
/// without an additional allocation.
///
/// PORT NOTE: Go grows to exactly `len(a)+itemsToAdd` via `append`; Rust
/// `Vec::reserve` guarantees at least that capacity.
pub fn extend_capacity<T>(a: &mut Vec<T>, items_to_add: usize) {
    a.reserve(items_to_add);
}

/// Buffer implements a simple buffer for `T`.
pub struct Buffer<T> {
    /// The underlying `T` vector.
    pub b: Vec<T>,
}

impl<T> Buffer<T> {
    /// Resets the buffer.
    pub fn reset(&mut self) {
        self.b.clear();
    }
}

impl<T> Default for Buffer<T> {
    fn default() -> Self {
        Buffer { b: Vec::new() }
    }
}

/// BufferPool is a pool of `T` buffers.
///
/// PORT NOTE: Go uses `sync.Pool`; the port uses a `Mutex<Vec<..>>` pool and
/// hands buffers out by value, preserving the reuse pattern.
pub struct BufferPool<T> {
    p: Mutex<Vec<Buffer<T>>>,
}

impl<T> BufferPool<T> {
    /// Creates an empty pool.
    pub const fn new() -> Self {
        BufferPool {
            p: Mutex::new(Vec::new()),
        }
    }

    /// Obtains a `Buffer` from the pool.
    pub fn get(&self) -> Buffer<T> {
        self.p.lock().unwrap().pop().unwrap_or_default()
    }

    /// Puts `b` back into the pool.
    pub fn put(&self, mut b: Buffer<T>) {
        b.reset();
        self.p.lock().unwrap().push(b);
    }
}

impl<T> Default for BufferPool<T> {
    fn default() -> Self {
        Self::new()
    }
}

// PORT NOTE: the Go package has no *_test.go files; the tests below are
// minimal Rust-side sanity checks for the adapted Vec semantics.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_length() {
        let mut a: Vec<u8> = Vec::new();
        set_length(&mut a, 5);
        assert_eq!(a.len(), 5);
        assert_eq!(a, &[0, 0, 0, 0, 0]);

        a[0] = 42;
        set_length(&mut a, 2);
        assert_eq!(a, &[42, 0]);

        set_length(&mut a, 4);
        assert_eq!(a, &[42, 0, 0, 0]);
    }

    #[test]
    fn test_extend_capacity() {
        let mut a: Vec<u64> = vec![1, 2, 3];
        extend_capacity(&mut a, 10);
        assert!(a.capacity() >= 13);
        assert_eq!(a, &[1, 2, 3]);
        let ptr = a.as_ptr();
        for i in 0..10 {
            a.push(i);
        }
        assert_eq!(
            ptr,
            a.as_ptr(),
            "push within reserved capacity must not reallocate"
        );
    }

    #[test]
    fn test_buffer_pool() {
        static POOL: BufferPool<u32> = BufferPool::new();
        let mut b = POOL.get();
        assert!(b.b.is_empty());
        b.b.extend_from_slice(&[1, 2, 3]);
        POOL.put(b);
        let b = POOL.get();
        assert!(b.b.is_empty(), "pooled buffer must be reset");
        POOL.put(b);
    }
}

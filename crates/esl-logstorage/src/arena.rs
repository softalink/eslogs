//! Port of EsLogs `lib/logstorage/arena.go`.

use std::sync::Mutex;

use esl_common::{bytesutil, slicesutil};

/// Obtains an arena from the pool.
pub fn get_arena() -> Arena {
    ARENA_POOL.lock().unwrap().pop().unwrap_or_default()
}

/// Returns a to the pool.
pub fn put_arena(mut a: Arena) {
    a.reset();
    ARENA_POOL.lock().unwrap().push(a);
}

// PORT NOTE: Go uses `sync.Pool` with `*arena`; the port uses a
// `Mutex<Vec<Arena>>` pool and hands arenas out by value, preserving the
// buffer reuse pattern.
static ARENA_POOL: Mutex<Vec<Arena>> = Mutex::new(Vec::new());

/// Arena is a byte buffer that hands out slices of itself.
///
/// PORT NOTE: Go's `arena` returns subslices that alias the arena buffer and
/// stay valid until the arena is reset. The Rust port returns slices borrowed
/// from the arena instead, which are valid only until the next mutation of
/// the arena; callers that need to keep several allocations alive at once
/// must track `(offset, len)` ranges into `a.b`.
#[derive(Default)]
pub struct Arena {
    pub b: Vec<u8>,
}

impl Arena {
    /// Resets the arena.
    pub fn reset(&mut self) {
        self.b.clear();
    }

    /// Extends the arena capacity, so n bytes can be appended without an
    /// additional allocation.
    pub fn preallocate(&mut self, n: usize) {
        slicesutil::extend_capacity(&mut self.b, n);
    }

    /// Returns the size of the memory held by the arena.
    pub fn size_bytes(&self) -> usize {
        self.b.capacity()
    }

    /// Copies b into the arena and returns the copy.
    pub fn copy_bytes(&mut self, b: &[u8]) -> &[u8] {
        if b.is_empty() {
            // PORT NOTE: Go returns the input slice as-is without touching
            // the arena; an empty slice is returned here instead.
            return &[];
        }

        let ab_len = self.b.len();
        self.b.extend_from_slice(b);
        &self.b[ab_len..]
    }

    /// Copies b into the arena and returns the copy as a string.
    ///
    /// PORT NOTE: Go reinterprets the copied bytes via
    /// `bytesutil.ToUnsafeString`; the safe Rust equivalent panics when b is
    /// not valid UTF-8 (Go callers only pass UTF-8 data here).
    pub fn copy_bytes_to_string(&mut self, b: &[u8]) -> &str {
        bytesutil::to_unsafe_string(self.copy_bytes(b))
    }

    /// Copies s into the arena and returns the copy.
    pub fn copy_string(&mut self, s: &str) -> &str {
        let b = bytesutil::to_unsafe_bytes(s);
        self.copy_bytes_to_string(b)
    }

    /// Allocates size bytes in the arena and returns them for writing.
    ///
    /// PORT NOTE: Go returns nil for `size <= 0`; size is unsigned here, and
    /// `size == 0` yields an empty slice.
    pub fn new_bytes(&mut self, size: usize) -> &mut [u8] {
        let ab_len = self.b.len();
        bytesutil::resize_with_copy_may_overallocate(&mut self.b, ab_len + size);
        &mut self.b[ab_len..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: the Go test keeps the strings returned from the arena alive
    // while continuing to allocate from it, relying on Go aliasing semantics.
    // The Rust borrow checker forbids that, so the test stores owned copies
    // plus `(offset..end)` ranges and re-verifies the data through `a.b`
    // after the later allocations.
    #[test]
    fn test_arena() {
        let values = ["foo", "bar", "", "adsfjkljsdfdsf", "dsfsopq", "io234"];

        for _ in 0..10 {
            let mut a = get_arena();
            let n = a.b.len();
            assert_eq!(n, 0, "unexpected non-zero length of empty arena: {n}");

            // add values to arena
            let mut values_copy: Vec<String> = Vec::with_capacity(values.len());
            let mut value_ranges = Vec::with_capacity(values.len());
            let mut values_len = 0;
            for v in values {
                let start = a.b.len();
                let v_copy = a.copy_string(v);
                assert_eq!(v_copy, v, "unexpected value; got {v_copy:?}; want {v:?}");
                values_copy.push(v_copy.to_string());
                value_ranges.push(start..a.b.len());
                values_len += v.len();
            }

            // verify that the values returned from arena match the original values
            for (j, v) in values.iter().enumerate() {
                let v_copy = &values_copy[j];
                assert_eq!(v_copy, v, "unexpected value; got {v_copy:?}; want {v:?}");
            }

            let n = a.b.len();
            assert_eq!(
                n, values_len,
                "unexpected arena size; got {n}; want {values_len}"
            );
            let n = a.size_bytes();
            assert!(
                n >= values_len,
                "unexpected arena capacity; got {n}; want at least {values_len}"
            );

            // Try allocating slices with different lengths
            let mut bs_ranges = Vec::with_capacity(100);
            for j in 0..100 {
                let start = a.b.len();
                let b = a.new_bytes(j);
                assert_eq!(b.len(), j, "unexpected len(b); got {}; want {j}", b.len());
                for (k, x) in b.iter_mut().enumerate() {
                    *x = k as u8;
                }
                bs_ranges.push(start..start + j);
                values_len += j;
                let n = a.b.len();
                assert_eq!(
                    n, values_len,
                    "unexpected arena size; got {n}; want {values_len}"
                );
                let n = a.size_bytes();
                assert!(
                    n >= values_len,
                    "unexpected arena capacity; got {n}; want at least {values_len}"
                );
            }

            // verify that the allocated slices didn't change
            for (j, r) in bs_ranges.iter().enumerate() {
                let mut b = vec![0u8; j];
                for (k, x) in b.iter_mut().enumerate() {
                    *x = k as u8;
                }
                let v = &a.b[r.clone()];
                assert_eq!(
                    v,
                    &b[..],
                    "unexpected value at index {j}; got {v:X?}; want {b:X?}"
                );
            }

            // verify that the values returned from arena match the original values
            for (j, v) in values.iter().enumerate() {
                let v_copy = &values_copy[j];
                assert_eq!(v_copy, v, "unexpected value; got {v_copy:?}; want {v:?}");
                let arena_v = &a.b[value_ranges[j].clone()];
                assert_eq!(
                    arena_v,
                    v.as_bytes(),
                    "unexpected arena-backed value; got {arena_v:X?}; want {v:?}"
                );
            }

            put_arena(a);
        }
    }
}

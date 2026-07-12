//! Port of `lib/logstorage/chunked_allocator.go` from EsLogs v1.51.0.
//!
//! `ChunkedAllocator` reduces memory fragmentation when allocating pre-defined
//! structs in a scoped fashion.
//!
//! It also reduces the number of memory allocations by amortizing them into
//! 64Kb chunk allocations.
//!
//! `ChunkedAllocator` cannot be used from concurrently running threads.
//!
//! PORT NOTE: the Go allocator hands out sub-slices (`xs := dst[dstLen:...]`)
//! and pointers aliasing its internal chunks; the returned references stay
//! valid even after the allocator moves on to a new chunk because the Go GC
//! keeps the old chunk alive. Safe Rust cannot return long-lived `&mut`
//! references into `self`, so the port keeps ownership of every chunk and
//! hands out [`ChunkedRange`] handles instead, resolved through the
//! `items`/`items_mut` (and typed `get_*`) accessors.
//!
//! PORT NOTE: the Go struct also holds per-type pools for the stats-pipe
//! processors (`statsAvgProcessor`, `pipeStatsGroup`, `hitsMapShard`, ...).
//! Those types are not ported yet; add them as `ChunkedItems<T>` fields with
//! the corresponding `new_stats_*` methods when the stats modules land. The
//! generic [`ChunkedItems`] below reproduces Go's `addNewItem`/`addNewItems`
//! for them.

/// The chunk size used for amortizing allocations (64Kb in Go).
const CHUNK_SIZE_BYTES: usize = 64 * 1024;

/// A handle to items allocated from a [`ChunkedItems`] pool.
///
/// The handle is only meaningful for the pool it was obtained from.
#[derive(Clone, Copy, Debug)]
pub struct ChunkedRange {
    chunk: usize,
    start: usize,
    len: usize,
}

/// A pool of `T` items allocated in 64Kb chunks (Go `addNewItem`/`addNewItems`
/// backing storage).
#[derive(Debug, Default)]
pub struct ChunkedItems<T> {
    chunks: Vec<Vec<T>>,
}

impl<T: Default> ChunkedItems<T> {
    /// Allocates a single zero-initialized item (Go `addNewItem`).
    ///
    /// `bytes_allocated` is increased by the chunk size whenever a new chunk
    /// is allocated, mirroring Go's `a.bytesAllocated` accounting.
    pub fn add_new_item(&mut self, bytes_allocated: &mut usize) -> ChunkedRange {
        self.add_new_items(1, bytes_allocated)
    }

    /// Allocates `items_len` zero-initialized items (Go `addNewItems`).
    pub fn add_new_items(&mut self, items_len: usize, bytes_allocated: &mut usize) -> ChunkedRange {
        let max_items = CHUNK_SIZE_BYTES / std::mem::size_of::<T>();
        if items_len > max_items {
            // Oversized allocations get a dedicated chunk, like Go's
            // `make([]T, itemsLen)`. Go doesn't count them in bytesAllocated.
            let mut chunk = Vec::new();
            chunk.resize_with(items_len, T::default);
            self.chunks.push(chunk);
            return ChunkedRange {
                chunk: self.chunks.len() - 1,
                start: 0,
                len: items_len,
            };
        }

        // PORT NOTE: when the current chunk cannot hold items_len more items,
        // Go drops it (`dst = nil`) and the abandoned tail capacity is kept
        // alive only by previously returned sub-slices; here the full chunk
        // simply stays owned by the pool, which retains the same memory.
        let need_new_chunk = match self.chunks.last() {
            None => true,
            Some(chunk) => chunk.len() + items_len > max_items,
        };
        if need_new_chunk {
            self.chunks.push(Vec::with_capacity(max_items));
            *bytes_allocated += max_items * std::mem::size_of::<T>();
        }

        let chunk_idx = self.chunks.len() - 1;
        let chunk = &mut self.chunks[chunk_idx];
        let start = chunk.len();
        chunk.resize_with(start + items_len, T::default);
        ChunkedRange {
            chunk: chunk_idx,
            start,
            len: items_len,
        }
    }
}

impl<T> ChunkedItems<T> {
    /// Returns the items referenced by r.
    pub fn items(&self, r: ChunkedRange) -> &[T] {
        &self.chunks[r.chunk][r.start..r.start + r.len]
    }

    /// Returns the items referenced by r for modification.
    pub fn items_mut(&mut self, r: ChunkedRange) -> &mut [T] {
        &mut self.chunks[r.chunk][r.start..r.start + r.len]
    }
}

/// ChunkedAllocator reduces memory fragmentation when allocating pre-defined
/// structs in a scoped fashion (Go `chunkedAllocator`).
#[derive(Debug, Default)]
pub struct ChunkedAllocator {
    u64_buf: ChunkedItems<u64>,

    strings_buf: ChunkedItems<u8>,

    bytes_allocated: usize,
}

impl ChunkedAllocator {
    /// Creates an empty allocator (Go zero-value `chunkedAllocator`).
    pub fn new() -> ChunkedAllocator {
        ChunkedAllocator::default()
    }

    /// Allocates a new zero-initialized u64 (Go `newUint64`).
    pub fn new_uint64(&mut self) -> ChunkedRange {
        self.u64_buf.add_new_item(&mut self.bytes_allocated)
    }

    /// Returns the u64 referenced by r.
    pub fn get_uint64(&self, r: ChunkedRange) -> u64 {
        self.u64_buf.items(r)[0]
    }

    /// Returns the u64 referenced by r for modification.
    pub fn get_uint64_mut(&mut self, r: ChunkedRange) -> &mut u64 {
        &mut self.u64_buf.items_mut(r)[0]
    }

    /// Copies b into the strings buffer (Go `cloneBytesToString`).
    pub fn clone_bytes_to_string(&mut self, b: &[u8]) -> ChunkedRange {
        let r = self
            .strings_buf
            .add_new_items(b.len(), &mut self.bytes_allocated);
        self.strings_buf.items_mut(r).copy_from_slice(b);
        r
    }

    /// Copies s into the strings buffer (Go `cloneString`).
    pub fn clone_string(&mut self, s: &str) -> ChunkedRange {
        self.clone_bytes_to_string(s.as_bytes())
    }

    /// Returns the string referenced by r.
    ///
    /// Panics if r references bytes which are not valid UTF-8; use
    /// [`ChunkedAllocator::get_string_bytes`] for binary data.
    ///
    /// PORT NOTE: Go strings may hold arbitrary bytes, so `cloneBytesToString`
    /// never fails there; Rust `&str` must be valid UTF-8.
    pub fn get_string(&self, r: ChunkedRange) -> &str {
        std::str::from_utf8(self.strings_buf.items(r))
            .expect("BUG: non-UTF-8 bytes accessed via get_string; use get_string_bytes")
    }

    /// Returns the raw bytes referenced by r.
    pub fn get_string_bytes(&self, r: ChunkedRange) -> &[u8] {
        self.strings_buf.items(r)
    }

    /// Returns the number of bytes allocated in 64Kb chunks so far
    /// (Go `bytesAllocated`).
    pub fn bytes_allocated(&self) -> usize {
        self.bytes_allocated
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // PORT NOTE: chunked_allocator.go has no tests in Go; these Rust-only
    // tests verify the chunking and accounting semantics described above.

    #[test]
    fn test_new_uint64() {
        let mut a = ChunkedAllocator::new();

        let r1 = a.new_uint64();
        let r2 = a.new_uint64();
        assert_eq!(a.get_uint64(r1), 0);
        assert_eq!(a.get_uint64(r2), 0);

        *a.get_uint64_mut(r1) = 42;
        *a.get_uint64_mut(r2) += 7;
        assert_eq!(a.get_uint64(r1), 42);
        assert_eq!(a.get_uint64(r2), 7);

        // A single 64Kb chunk holds 8192 u64 items.
        assert_eq!(a.bytes_allocated(), 64 * 1024);
        for _ in 0..8190 {
            a.new_uint64();
        }
        assert_eq!(a.bytes_allocated(), 64 * 1024);
        a.new_uint64();
        assert_eq!(a.bytes_allocated(), 2 * 64 * 1024);

        // Earlier allocations stay valid after a new chunk is started.
        assert_eq!(a.get_uint64(r1), 42);
        assert_eq!(a.get_uint64(r2), 7);
    }

    #[test]
    fn test_clone_string() {
        let mut a = ChunkedAllocator::new();

        let r1 = a.clone_string("foo");
        let r2 = a.clone_bytes_to_string(b"bar\xff");
        assert_eq!(a.get_string(r1), "foo");
        assert_eq!(a.get_string_bytes(r2), b"bar\xff");
        assert_eq!(a.bytes_allocated(), 64 * 1024);
    }

    #[test]
    fn test_oversized_allocation() {
        let mut a = ChunkedAllocator::new();

        // Allocations bigger than the chunk size get a dedicated allocation
        // which isn't counted in bytes_allocated, like Go's make([]T, itemsLen).
        let s = "x".repeat(70_000);
        let r = a.clone_string(&s);
        assert_eq!(a.get_string(r), s);
        assert_eq!(a.bytes_allocated(), 0);

        // Subsequent small allocations start a fresh chunk.
        let r2 = a.clone_string("abc");
        assert_eq!(a.get_string(r2), "abc");
        assert_eq!(a.get_string(r), s);
        assert_eq!(a.bytes_allocated(), 64 * 1024);
    }
}

//! Port of Softalink LLC `lib/bytesutil`.
//!
//! Layout mirrors the Go package files:
//! - `bytesutil.go` + `bytebuffer.go` → this file
//! - `internstring.go` → [`internstring`]
//! - `fast_string_matcher.go` → [`fast_string_matcher`]
//! - `fast_string_transformer.go` → [`fast_string_transformer`]
//! - `itoa.go` → [`itoa`]

use std::io;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

mod fast_string_matcher;
mod fast_string_transformer;
mod internstring;
mod itoa;

pub use fast_string_matcher::FastStringMatcher;
pub use fast_string_transformer::FastStringTransformer;
pub use internstring::{intern_bytes, intern_string};
pub use itoa::itoa;

// PORT NOTE: the Go slice-resize helpers take a slice and return a (possibly
// newly allocated) one; the ports mutate the `Vec<u8>` in place. Go re-exposes
// uninitialized bytes between `len` and `cap` (callers treat them as scratch);
// Rust cannot expose uninitialized memory, so grown regions are zero-filled.

/// Resizes `b` to minimum `n` bytes; the capacity is rounded up to the
/// nearest power of 2.
///
/// If a new buffer is allocated, `b` contents are copied to it.
pub fn resize_with_copy_may_overallocate(b: &mut Vec<u8>, n: usize) {
    if n <= b.capacity() {
        b.resize(n, 0);
        return;
    }
    let n_new = round_to_nearest_pow2(n);
    let mut b_new = Vec::with_capacity(n_new);
    b_new.extend_from_slice(b);
    b_new.resize(n, 0);
    *b = b_new;
}

/// Resizes `b` to exactly `n` bytes.
///
/// If a new buffer is allocated, `b` contents are copied to it.
pub fn resize_with_copy_no_overallocate(b: &mut Vec<u8>, n: usize) {
    if n <= b.capacity() {
        b.resize(n, 0);
        return;
    }
    let mut b_new = Vec::with_capacity(n);
    b_new.extend_from_slice(b);
    b_new.resize(n, 0);
    *b = b_new;
}

/// Resizes `b` to minimum `n` bytes; the capacity is rounded up to the
/// nearest power of 2.
///
/// If a new buffer is allocated, `b` contents aren't copied to it (the new
/// buffer is zeroed).
pub fn resize_no_copy_may_overallocate(b: &mut Vec<u8>, n: usize) {
    if n <= b.capacity() {
        b.resize(n, 0);
        return;
    }
    let n_new = round_to_nearest_pow2(n);
    let mut b_new = Vec::with_capacity(n_new);
    b_new.resize(n, 0);
    *b = b_new;
}

/// Resizes `b` to exactly `n` bytes.
///
/// If a new buffer is allocated, `b` contents aren't copied to it (the new
/// buffer is zeroed).
pub fn resize_no_copy_no_overallocate(b: &mut Vec<u8>, n: usize) {
    if n <= b.capacity() {
        b.resize(n, 0);
        return;
    }
    *b = vec![0u8; n];
}

/// Rounds `n` to the nearest power of 2.
///
/// It is expected that `n > 0`.
fn round_to_nearest_pow2(n: usize) -> usize {
    let pow2 = usize::BITS - (n - 1).leading_zeros();
    1usize << pow2
}

/// Converts `b` to a string without copying.
///
/// PORT NOTE: Go's `ToUnsafeString` reinterprets the bytes with no checks and
/// the result is valid only while `b` is unmodified. The Rust equivalent is a
/// safe zero-copy `&str` view tied to the `&[u8]` borrow; since Rust strings
/// must be UTF-8, it panics when `b` is not valid UTF-8 (Go callers only pass
/// UTF-8 data here).
pub fn to_unsafe_string(b: &[u8]) -> &str {
    std::str::from_utf8(b).expect("BUG: to_unsafe_string called with invalid UTF-8 data")
}

/// Converts `s` to a byte slice without copying.
///
/// PORT NOTE: Go's `ToUnsafeBytes` is unsafe pointer casting; in Rust this is
/// the safe zero-copy `str::as_bytes` view.
pub fn to_unsafe_bytes(s: &str) -> &[u8] {
    s.as_bytes()
}

// PORT NOTE: `lib/fasttime` is being ported in parallel; this private helper
// mirrors `fasttime.UnixTimestamp()` (current unix time in seconds) via
// `SystemTime` until that module lands.
pub(crate) fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// ByteBuffer implements a simple byte buffer.
#[derive(Default)]
pub struct ByteBuffer {
    /// The underlying byte vector.
    pub b: Vec<u8>,
}

impl ByteBuffer {
    /// Returns an unique id for the buffer.
    pub fn path(&self) -> String {
        format!("ByteBuffer/{:p}/mem", self as *const ByteBuffer)
    }

    /// Resets the buffer.
    pub fn reset(&mut self) {
        self.b.clear();
    }

    /// Returns the length of the data stored in the buffer.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.b.len()
    }

    /// Writes `p` to the buffer.
    pub fn must_write(&mut self, p: &[u8]) {
        self.b.extend_from_slice(p);
    }

    /// Grows the buffer capacity, so it can accept `n` bytes without
    /// additional allocations.
    pub fn grow(&mut self, n: usize) {
        // PORT NOTE: Go grows via slicesutil.SetLength + reslice; Vec::reserve
        // gives the same "cap >= len+n" guarantee.
        self.b.reserve(n);
    }

    /// Writes the buffer contents to `w`.
    pub fn write_to<W: io::Write>(&self, w: &mut W) -> io::Result<u64> {
        let n = w.write(&self.b)?;
        Ok(n as u64)
    }

    /// Reads all the data from `r` to the buffer until EOF.
    ///
    /// Returns the number of bytes read.
    ///
    /// PORT NOTE: Go returns `(n, err)`; on error the port returns only the
    /// error, with the buffer still holding everything read so far.
    pub fn read_from<R: io::Read>(&mut self, r: &mut R) -> io::Result<u64> {
        let b_len = self.b.len();
        if self.b.capacity() < 4 * 1024 {
            // Pre-allocate at least 4KiB
            self.b.reserve(4 * 1024 - b_len);
        }
        let mut cap = self.b.capacity();
        self.b.resize(cap, 0);
        let mut offset = b_len;
        loop {
            let free = cap - offset;
            if free < cap / 16 {
                // grow the buffer by 30% similar to how Go does this
                // https://go.googlesource.com/go/+/2dda92ff6f9f07eeb110ecbf0fc2d7a0ddd27f9d
                // higher growth rates could consume excessive memory when reading big amounts of data.
                let n = (1.3 * cap as f64) as usize;
                self.b.reserve_exact(n - cap);
                cap = self.b.capacity();
                self.b.resize(cap, 0);
            }
            match r.read(&mut self.b[offset..]) {
                Ok(0) => {
                    self.b.truncate(offset);
                    return Ok((offset - b_len) as u64);
                }
                Ok(n) => {
                    offset += n;
                }
                Err(err) => {
                    self.b.truncate(offset);
                    return Err(err);
                }
            }
        }
    }

    /// Returns a new reader for the buffer contents.
    ///
    /// PORT NOTE: Go returns a `filestream.ReadCloser`; `lib/filestream` is
    /// being ported in parallel, so the port returns the concrete reader with
    /// the same method surface (`read`/`path`/`must_close`).
    pub fn new_reader(&self) -> ByteBufferReader<'_> {
        ByteBufferReader {
            bb: self,
            read_offset: 0,
        }
    }
}

impl io::Write for ByteBuffer {
    /// Appends `p` to the buffer.
    fn write(&mut self, p: &[u8]) -> io::Result<usize> {
        self.must_write(p);
        Ok(p.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Reader over a [`ByteBuffer`], the port of the Go package-private `reader`.
pub struct ByteBufferReader<'a> {
    // The buffer to read from; also used for path() call.
    bb: &'a ByteBuffer,

    // read_offset is the offset in bb.b for read.
    read_offset: usize,
}

impl ByteBufferReader<'_> {
    /// Returns an unique id for the underlying ByteBuffer.
    pub fn path(&self) -> String {
        self.bb.path()
    }

    /// Closes the reader for subsequent reuse.
    pub fn must_close(&mut self) {
        self.read_offset = 0;
    }
}

impl io::Read for ByteBufferReader<'_> {
    /// Reads up to `p.len()` bytes from the buffer.
    ///
    /// PORT NOTE: Go returns `io.EOF` together with the final bytes when
    /// `n < len(p)`; Rust's `io::Read` signals EOF via `Ok(0)` on the next
    /// call instead.
    fn read(&mut self, p: &mut [u8]) -> io::Result<usize> {
        let data = &self.bb.b[self.read_offset..];
        let n = p.len().min(data.len());
        p[..n].copy_from_slice(&data[..n]);
        self.read_offset += n;
        Ok(n)
    }
}

/// ByteBufferPool is a pool of ByteBuffers.
///
/// PORT NOTE: Go uses `sync.Pool` with `*ByteBuffer`; the port uses a
/// `Mutex<Vec<..>>` pool handing buffers out by value, preserving the reuse
/// pattern.
pub struct ByteBufferPool {
    p: Mutex<Vec<ByteBuffer>>,
}

impl ByteBufferPool {
    /// Creates an empty pool.
    pub const fn new() -> Self {
        ByteBufferPool {
            p: Mutex::new(Vec::new()),
        }
    }

    /// Obtains a ByteBuffer from the pool.
    pub fn get(&self) -> ByteBuffer {
        self.p.lock().unwrap().pop().unwrap_or_default()
    }

    /// Puts `bb` back into the pool.
    pub fn put(&self, mut bb: ByteBuffer) {
        bb.reset();
        self.p.lock().unwrap().push(bb);
    }
}

impl Default for ByteBufferPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn test_round_to_nearest_pow2() {
        fn f(n: usize, result_expected: usize) {
            let result = round_to_nearest_pow2(n);
            assert_eq!(
                result, result_expected,
                "unexpected round_to_nearest_pow2({n}); got {result}; want {result_expected}"
            );
        }
        f(1, 1);
        f(2, 2);
        f(3, 4);
        f(4, 4);
        f(5, 8);
        f(6, 8);
        f(7, 8);
        f(8, 8);
        f(9, 16);
        f(10, 16);
        f(16, 16);
        f(17, 32);
        f(32, 32);
        f(33, 64);
        f(64, 64);
    }

    // PORT NOTE: the Go resize tests compare `&b[0]` pointers across the
    // functional calls; the ports compare `Vec::as_ptr()` around the in-place
    // calls instead. The "newly allocated buffer" checks observe the same
    // reallocation/zeroing/copying behavior on the mutated Vec.

    #[test]
    fn test_resize_no_copy_no_overallocate() {
        for i in 0..1000 {
            let mut b = Vec::new();
            resize_no_copy_no_overallocate(&mut b, i);
            assert_eq!(b.len(), i, "invalid b size; got {}; want {i}", b.len());
            assert_eq!(
                b.capacity(),
                i,
                "invalid cap(b); got {}; want {i}",
                b.capacity()
            );
            let ptr = b.as_ptr();

            resize_no_copy_no_overallocate(&mut b, i);
            assert_eq!(b.len(), i);
            assert_eq!(ptr, b.as_ptr(), "b1 must reuse the same buffer");
            assert_eq!(b.capacity(), i, "invalid cap(b1)");

            b.clear();
            resize_no_copy_no_overallocate(&mut b, i);
            assert_eq!(b.len(), i);
            assert_eq!(ptr, b.as_ptr(), "b2 must reuse the same buffer");
            assert_eq!(b.capacity(), i, "invalid cap(b2)");

            if i > 0 {
                b[0] = 123;
                resize_no_copy_no_overallocate(&mut b, i + 1);
                assert_eq!(
                    b.len(),
                    i + 1,
                    "invalid b3 len; got {}; want {}",
                    b.len(),
                    i + 1
                );
                assert_eq!(b.capacity(), i + 1, "invalid cap(b3)");
                assert_ne!(ptr, b.as_ptr(), "b3 must be newly allocated");
                assert_eq!(b[0], 0, "b3[0] must be zeroed; got {}", b[0]);
            }
        }
    }

    #[test]
    fn test_resize_no_copy_may_overallocate() {
        for i in 0..1000 {
            let mut b = Vec::new();
            resize_no_copy_may_overallocate(&mut b, i);
            assert_eq!(b.len(), i, "invalid b size; got {}; want {i}", b.len());
            // PORT NOTE: Go computes roundToNearestPow2(0) == 0 through shift
            // overflow; the port special-cases i == 0.
            let mut cap_expected = if i == 0 { 0 } else { round_to_nearest_pow2(i) };
            assert_eq!(b.capacity(), cap_expected, "invalid cap(b)");
            let ptr = b.as_ptr();

            resize_no_copy_may_overallocate(&mut b, i);
            assert_eq!(b.len(), i);
            assert_eq!(ptr, b.as_ptr(), "b1 must reuse the same buffer");
            assert_eq!(b.capacity(), cap_expected, "invalid cap(b1)");

            b.clear();
            resize_no_copy_may_overallocate(&mut b, i);
            assert_eq!(b.len(), i);
            assert_eq!(ptr, b.as_ptr(), "b2 must reuse the same buffer");
            assert_eq!(b.capacity(), cap_expected, "invalid cap(b2)");

            if i > 0 {
                resize_no_copy_may_overallocate(&mut b, i + 1);
                assert_eq!(b.len(), i + 1, "invalid b3 len");
                cap_expected = round_to_nearest_pow2(i + 1);
                assert_eq!(b.capacity(), cap_expected, "invalid cap(b3)");
            }
        }
    }

    #[test]
    fn test_resize_with_copy_no_overallocate() {
        for i in 0..1000 {
            let mut b = Vec::new();
            resize_with_copy_no_overallocate(&mut b, i);
            assert_eq!(b.len(), i, "invalid b size; got {}; want {i}", b.len());
            assert_eq!(b.capacity(), i, "invalid cap(b)");
            let ptr = b.as_ptr();

            resize_with_copy_no_overallocate(&mut b, i);
            assert_eq!(b.len(), i);
            assert_eq!(ptr, b.as_ptr(), "b1 must reuse the same buffer");
            assert_eq!(b.capacity(), i, "invalid cap(b1)");

            b.clear();
            resize_with_copy_no_overallocate(&mut b, i);
            assert_eq!(b.len(), i);
            assert_eq!(ptr, b.as_ptr(), "b2 must reuse the same buffer");
            assert_eq!(b.capacity(), i, "invalid cap(b2)");

            if i > 0 {
                b[0] = 123;
                resize_with_copy_no_overallocate(&mut b, i + 1);
                assert_eq!(b.len(), i + 1, "invalid b3 len");
                assert_eq!(b.capacity(), i + 1, "invalid cap(b3)");
                assert_ne!(ptr, b.as_ptr(), "b3 must be newly allocated for i={i}");
                assert_eq!(b[0], 123, "b3[0] must equal b[0]");
            }
        }
    }

    #[test]
    fn test_resize_with_copy_may_overallocate() {
        for i in 0..1000 {
            let mut b = Vec::new();
            resize_with_copy_may_overallocate(&mut b, i);
            assert_eq!(b.len(), i, "invalid b size; got {}; want {i}", b.len());
            let mut cap_expected = if i == 0 { 0 } else { round_to_nearest_pow2(i) };
            assert_eq!(b.capacity(), cap_expected, "invalid cap(b)");
            let ptr = b.as_ptr();

            resize_with_copy_may_overallocate(&mut b, i);
            assert_eq!(b.len(), i);
            assert_eq!(ptr, b.as_ptr(), "b1 must reuse the same buffer");
            assert_eq!(b.capacity(), cap_expected, "invalid cap(b1)");

            b.clear();
            resize_with_copy_may_overallocate(&mut b, i);
            assert_eq!(b.len(), i);
            assert_eq!(ptr, b.as_ptr(), "b2 must reuse the same buffer");
            assert_eq!(b.capacity(), cap_expected, "invalid cap(b2)");

            if i > 0 {
                b[0] = 123;
                resize_with_copy_may_overallocate(&mut b, i + 1);
                assert_eq!(b.len(), i + 1, "invalid b3 len");
                cap_expected = round_to_nearest_pow2(i + 1);
                assert_eq!(b.capacity(), cap_expected, "invalid cap(b3)");
                assert_eq!(b[0], 123, "b3[0] must equal b[0]");
            }
        }
    }

    #[test]
    fn test_to_unsafe_string() {
        let s = "str";
        assert_eq!(
            b"str",
            to_unsafe_bytes(s),
            "to_unsafe_bytes({s}) is not equal to {s}"
        );
        assert_eq!("str", to_unsafe_string(b"str"));
    }

    #[test]
    fn test_byte_buffer() {
        let mut bb = ByteBuffer::default();

        let n = bb.write(&[]).expect("cannot write empty slice");
        assert_eq!(
            n, 0,
            "unexpected n when writing empty slice; got {n}; want 0"
        );
        assert_eq!(
            bb.b.len(),
            0,
            "unexpected len(bb.b) after writing empty slice"
        );

        let data1 = b"123";
        let n = bb.write(data1).expect("cannot write data1");
        assert_eq!(n, data1.len(), "unexpected n when writing {data1:?}");
        assert_eq!(&bb.b, data1, "unexpected bb.b");

        bb.grow(10);

        let data2 = b"1";
        let n = bb.write(data2).expect("cannot write data2");
        assert_eq!(n, data2.len(), "unexpected n when writing {data2:?}");
        assert_eq!(bb.b, b"1231", "unexpected bb.b");

        bb.reset();
        assert_eq!(bb.b, b"", "unexpected bb.b after reset");
        let r = bb.new_reader();
        assert_eq!(r.read_offset, 0, "unexpected r.read_offset after reset");
    }

    fn test_byte_buffer_read_from_helper(bb_pool: &ByteBufferPool, test: impl Fn(&mut ByteBuffer)) {
        let mut bb = bb_pool.get();
        test(&mut bb);
        bb_pool.put(bb);
    }

    #[test]
    fn test_byte_buffer_read_from_zero_bytes() {
        let bb_pool = ByteBufferPool::new();
        test_byte_buffer_read_from_helper(&bb_pool, |bb| {
            let mut src: &[u8] = b"";
            let n = bb
                .read_from(&mut src)
                .expect("error when reading empty string");
            assert_eq!(n, 0, "unexpected number of bytes read; got {n}; want 0");
            assert_eq!(
                bb.b.len(),
                0,
                "unexpected len(bb.b); got {}; want 0",
                bb.b.len()
            );
        });
    }

    #[test]
    fn test_byte_buffer_read_from_non_zero_bytes() {
        let bb_pool = ByteBufferPool::new();
        test_byte_buffer_read_from_helper(&bb_pool, |bb| {
            let s = "foobarbaz";
            let mut src = s.as_bytes();
            let n = bb
                .read_from(&mut src)
                .expect("error when reading non-empty string");
            assert_eq!(n, s.len() as u64, "unexpected number of bytes read");
            assert_eq!(bb.b, s.as_bytes(), "unexpected value read");
        });
    }

    #[test]
    fn test_byte_buffer_read_from_big_number_of_bytes() {
        let bb_pool = ByteBufferPool::new();
        test_byte_buffer_read_from_helper(&bb_pool, |bb| {
            let mut b = vec![0u8; 1024 * 1024 + 234];
            for (i, x) in b.iter_mut().enumerate() {
                *x = i as u8;
            }
            let mut src: &[u8] = &b;
            let n = bb.read_from(&mut src).expect("cannot read big value");
            assert_eq!(n, b.len() as u64, "unexpected number of bytes read");
            assert_eq!(bb.b, b, "unexpected value read");
        });
    }

    #[test]
    fn test_byte_buffer_read_from_non_empty_bb() {
        let bb_pool = ByteBufferPool::new();
        test_byte_buffer_read_from_helper(&bb_pool, |bb| {
            let prefix = b"prefix";
            bb.b.clear();
            bb.b.extend_from_slice(prefix);
            let s = "aosdfdsafdjsf";
            let mut src = s.as_bytes();
            let n = bb.read_from(&mut src).expect("cannot read to non-empty bb");
            assert_eq!(n, s.len() as u64, "unexpected number of bytes read");
            assert_eq!(bb.b.len(), prefix.len() + s.len(), "unexpected bb.b len");
            assert_eq!(&bb.b[..prefix.len()], prefix, "unexpected prefix");
            assert_eq!(&bb.b[prefix.len()..], s.as_bytes(), "unexpected data read");
        });
    }

    #[test]
    fn test_byte_buffer_read() {
        let mut bb = ByteBuffer::default();

        let arg = "bar";
        write!(bb, "foo, {arg}, baz").expect("unexpected error after write!");
        let n = bb.b.len();
        assert_eq!(&bb.b, b"foo, bar, baz", "unexpected bb.b");
        let mut r = bb.new_reader();
        assert_eq!(
            r.read_offset, 0,
            "unexpected r.read_offset; got {}; want 0",
            r.read_offset
        );

        let mut r_copy = bb.new_reader();

        let mut bb1 = ByteBuffer::default();
        let n1 = io::copy(&mut r, &mut bb1).expect("unexpected error after io::copy");
        assert_eq!(
            r.read_offset as u64, n1,
            "unexpected r.read_offset after io::copy"
        );
        assert_eq!(n1, n as u64, "unexpected number of bytes copied");
        assert_eq!(&bb1.b, b"foo, bar, baz", "unexpected bb1.b");

        // Make read return EOF.
        // PORT NOTE: Go asserts `(0, io.EOF)`; Rust io::Read signals EOF as Ok(0).
        let mut buf = vec![0u8; n];
        let n2 = r.read(&mut buf).expect("unexpected error");
        assert_eq!(n2, 0, "unexpected n2 returned; got {n2}; want 0");

        // Read data from r_copy
        assert_eq!(r_copy.read_offset, 0, "unexpected r_copy.read_offset");
        let mut buf = vec![0u8; n + 13];
        let n2 = r_copy
            .read(&mut buf)
            .expect("unexpected error when reading from r_copy");
        assert_eq!(n2, n, "unexpected number of bytes read from r_copy");
        assert_eq!(&buf[..n2], b"foo, bar, baz", "unexpected data read");
        assert_eq!(r_copy.read_offset, n2, "unexpected r_copy.read_offset");
        // The next read hits EOF (Ok(0) in Rust).
        let n3 = r_copy.read(&mut buf).expect("unexpected error");
        assert_eq!(n3, 0);
    }
}

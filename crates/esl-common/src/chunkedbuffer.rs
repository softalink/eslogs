//! Port of Softalink LLC `lib/chunkedbuffer`.

use std::io;
use std::sync::Mutex;

const CHUNK_SIZE: usize = 4 * 1024;

const COPY_BUF_SIZE: usize = 16 * 1024;

/// Returns a [`Buffer`] from the pool.
///
/// Return back the Buffer to the pool via [`put`] call when it is no longer
/// needed.
///
/// PORT NOTE: Go's `sync.Pool` returns `*Buffer`; the port uses a
/// `Mutex<Vec<..>>` pool handing buffers out by value, preserving the chunk
/// reuse pattern.
pub fn get() -> Buffer {
    CB_POOL.lock().unwrap().pop().unwrap_or_default()
}

/// Returns `cb` to the pool, so it could be reused via [`get`] call.
pub fn put(mut cb: Buffer) {
    cb.reset();
    CB_POOL.lock().unwrap().push(cb);
}

static CB_POOL: Mutex<Vec<Buffer>> = Mutex::new(Vec::new());

/// Buffer provides in-memory buffer optimized for storing big bytes volumes.
///
/// It stores the data in chunks of fixed size. This reduces memory
/// fragmentation and memory waste comparing to the contiguous slices of bytes.
#[derive(Default)]
pub struct Buffer {
    chunks: Vec<Box<[u8; CHUNK_SIZE]>>,

    // offset is the offset in the last chunk to write data to.
    offset: usize,
}

impl Buffer {
    /// Resets the buffer, so it can be reused for writing new data into it.
    ///
    /// Reset frees up memory chunks allocated for the buffer, so they could be
    /// reused by other Buffer instances.
    pub fn reset(&mut self) {
        for chunk in self.chunks.drain(..) {
            put_chunk(chunk);
        }
        self.offset = 0;
    }

    /// Returns the number of bytes occupied by the buffer.
    pub fn size_bytes(&self) -> usize {
        self.chunks.len() * CHUNK_SIZE
    }

    /// Returns the length of the data stored in the buffer.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        if self.chunks.is_empty() {
            return 0;
        }
        (self.chunks.len() - 1) * CHUNK_SIZE + self.offset
    }

    /// Writes `p` to the buffer.
    pub fn must_write(&mut self, p: &[u8]) {
        let mut p = p;
        while !p.is_empty() {
            if self.chunks.is_empty() || self.offset == CHUNK_SIZE {
                let chunk = get_chunk();
                self.chunks.push(chunk);
                self.offset = 0;
            }
            let dst = self.chunks.last_mut().unwrap();
            let n = p.len().min(CHUNK_SIZE - self.offset);
            dst[self.offset..self.offset + n].copy_from_slice(&p[..n]);
            self.offset += n;
            p = &p[n..];
        }
    }

    /// Reads `p.len()` bytes from the buffer at the offset `off`.
    ///
    /// Panics when the requested range is out of the stored data, like the Go
    /// version.
    pub fn must_read_at(&self, p: &mut [u8], off: i64) {
        if p.is_empty() {
            return;
        }
        let mut p = p;

        let mut chunk_idx = (off / CHUNK_SIZE as i64) as usize;
        let offset = (off % CHUNK_SIZE as i64) as usize;

        let chunk = &self.chunks[chunk_idx];
        let n = p.len().min(CHUNK_SIZE - offset);
        p[..n].copy_from_slice(&chunk[offset..offset + n]);
        p = &mut p[n..];

        while !p.is_empty() {
            chunk_idx += 1;
            let chunk = &self.chunks[chunk_idx];
            let n = p.len().min(CHUNK_SIZE);
            p[..n].copy_from_slice(&chunk[..n]);
            p = &mut p[n..];
        }
    }

    /// Reads all the data from `r` and appends it to the buffer.
    ///
    /// Returns the number of bytes read.
    pub fn read_from<R: io::Read>(&mut self, r: &mut R) -> io::Result<u64> {
        let mut b = COPY_BUF_POOL
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_else(|| Box::new([0u8; COPY_BUF_SIZE]));

        let mut bytes_read = 0u64;
        loop {
            match r.read(&mut b[..]) {
                Ok(0) => {
                    COPY_BUF_POOL.lock().unwrap().push(b);
                    return Ok(bytes_read);
                }
                Ok(n) => {
                    self.must_write(&b[..n]);
                    bytes_read += n as u64;
                }
                Err(err) => {
                    COPY_BUF_POOL.lock().unwrap().push(b);
                    return Err(err);
                }
            }
        }
    }

    /// Writes the buffer data to `w`.
    ///
    /// PORT NOTE: Go returns `(n, err)`; the error variant carries
    /// `(bytes_written, message)` since callers (and the ported tests) need
    /// the byte count on failure. The Go type-switch calling `Grow` on
    /// `*bytesutil.ByteBuffer` / `*bytes.Buffer` targets is dropped — it is an
    /// allocation hint only, not observable behavior.
    pub fn write_to<W: io::Write>(&self, w: &mut W) -> Result<u64, (u64, String)> {
        let b_len = self.len();
        if b_len == 0 {
            return Ok(0);
        }

        let mut n_total = 0u64;

        // Write all the chunks except the last one, which may be incomplete.
        for chunk in &self.chunks[..self.chunks.len() - 1] {
            let n = w
                .write(&chunk[..])
                .map_err(|err| (n_total, err.to_string()))?;
            n_total += n as u64;
            if n != CHUNK_SIZE {
                return Err((
                    n_total,
                    format!("unexpected number of bytes written; got {n}; want {CHUNK_SIZE}"),
                ));
            }
        }

        // Write the last chunk
        let chunk = self.chunks.last().unwrap();
        let n = w
            .write(&chunk[..self.offset])
            .map_err(|err| (n_total, err.to_string()))?;
        n_total += n as u64;
        if n != self.offset {
            return Err((
                n_total,
                format!(
                    "unexpected number of bytes written; got {n}; want {}",
                    self.offset
                ),
            ));
        }

        Ok(n_total)
    }

    /// Writes the buffer contents to `w`.
    ///
    /// Use this function only if `w` cannot return errors. For example, if `w`
    /// is a `Vec<u8>` or a `bytesutil::ByteBuffer`. If `w` can return errors,
    /// then use [`Buffer::write_to`] function instead.
    pub fn must_write_to<W: io::Write>(&self, w: &mut W) {
        if let Err((_, err)) = self.write_to(w) {
            crate::panicf!(
                "BUG: unexpected error writing Buffer data to the provided writer: {err}"
            );
        }
    }

    /// Returns the buffer path.
    pub fn path(&self) -> String {
        format!("Buffer/{:p}/mem", self as *const Buffer)
    }

    /// Closes the buffer for subsequent reuse.
    pub fn must_close(&self) {
        // Do nothing, since certain code rely on the buffer reading after
        // must_close call.
    }

    /// Returns a reader for reading the data stored in the buffer.
    ///
    /// PORT NOTE: Go returns a `filestream.ReadCloser`; `lib/filestream` is
    /// being ported in parallel, so the port returns the concrete reader with
    /// the same method surface (`read`/`path`/`must_close`).
    pub fn new_reader(&self) -> Reader<'_> {
        Reader {
            cb: self,
            offset: 0,
        }
    }
}

impl io::Write for Buffer {
    fn write(&mut self, p: &[u8]) -> io::Result<usize> {
        self.must_write(p);
        Ok(p.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

static COPY_BUF_POOL: Mutex<Vec<Box<[u8; COPY_BUF_SIZE]>>> = Mutex::new(Vec::new());

/// Reader over a chunked [`Buffer`], the port of the Go package-private
/// `reader`.
pub struct Reader<'a> {
    cb: &'a Buffer,

    // offset is the offset at cb to read the next data at read call.
    offset: usize,
}

impl Reader<'_> {
    /// Returns an unique id for the underlying Buffer.
    pub fn path(&self) -> String {
        self.cb.path()
    }

    /// Closes the reader for subsequent reuse.
    pub fn must_close(&mut self) {
        self.offset = 0;
    }
}

impl io::Read for Reader<'_> {
    /// PORT NOTE: Go returns `io.EOF` together with the final bytes; Rust's
    /// `io::Read` signals EOF via `Ok(0)` on the next call instead.
    fn read(&mut self, p: &mut [u8]) -> io::Result<usize> {
        let chunk_idx = self.offset / CHUNK_SIZE;
        let offset = self.offset % CHUNK_SIZE;

        if chunk_idx == self.cb.chunks.len() {
            if offset != 0 {
                panic!("BUG: offset must be 0; got {offset}");
            }
            return Ok(0);
        }

        let chunk = &self.cb.chunks[chunk_idx];
        if chunk_idx == self.cb.chunks.len() - 1 {
            // read the last chunk
            let data = &chunk[offset..self.cb.offset];
            let n = p.len().min(data.len());
            p[..n].copy_from_slice(&data[..n]);
            self.offset += n;
            return Ok(n);
        }
        let data = &chunk[offset..];
        let n = p.len().min(data.len());
        p[..n].copy_from_slice(&data[..n]);
        self.offset += n;
        Ok(n)
    }
}

fn get_chunk() -> Box<[u8; CHUNK_SIZE]> {
    CHUNK_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_else(|| Box::new([0u8; CHUNK_SIZE]))
}

fn put_chunk(chunk: Box<[u8; CHUNK_SIZE]>) {
    CHUNK_POOL.lock().unwrap().push(chunk);
}

static CHUNK_POOL: Mutex<Vec<Box<[u8; CHUNK_SIZE]>>> = Mutex::new(Vec::new());

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn test_buffer() {
        let mut cb = get();

        for _ in 0..10 {
            cb.reset();

            // Write data to chunked buffer
            let mut total_size = 0;
            for j in 1..1000 {
                let mut b = vec![0u8; j];
                for (k, x) in b.iter_mut().enumerate() {
                    *x = k as u8;
                }
                cb.must_write(&b);
                total_size += b.len();
            }

            let cb_len = cb.len();
            assert_eq!(cb_len, total_size, "unexpected Buffer.len value");

            let size = cb.size_bytes();
            assert!(
                size >= total_size,
                "too small size_bytes; got {size}; want at least {total_size}"
            );

            // Read the data from chunked buffer via must_read_at.
            let mut off = 0usize;
            for j in 1..1000 {
                let mut b = vec![0u8; j];
                cb.must_read_at(&mut b, off as i64);
                off += j;

                // Verify the data is read correctly
                for (k, &x) in b.iter().enumerate() {
                    assert_eq!(
                        x, k as u8,
                        "unexpected byte read; got {x}; want {}",
                        k as u8
                    );
                }
            }

            // Read the data from chunked buffer via new_reader.
            let mut r = cb.new_reader();
            let mut bb = Vec::new();
            let n = r
                .read_to_end(&mut bb)
                .expect("error when reading data from chunked buffer");
            assert_eq!(
                n, off,
                "unexpected amounts of data read from chunked buffer"
            );

            // Verify that reader path is equivalent to cb path
            let cb_path = cb.path();
            let r_path = r.path();
            assert_eq!(r_path, cb_path, "unexpected path");

            r.must_close();

            // Verify the read data
            let mut off = 0usize;
            let data = &bb;
            for j in 1..1000 {
                let b = &data[off..off + j];
                off += j;

                // Verify the data is read correctly
                for (k, &x) in b.iter().enumerate() {
                    assert_eq!(
                        x, k as u8,
                        "unexpected byte read; got {x}; want {}",
                        k as u8
                    );
                }
            }

            // Copy the data to another chunked buffer via write_to.
            let mut cb2 = get();
            let n = cb
                .write_to(&mut cb2)
                .expect("error when writing data to another chunked buffer");
            assert_eq!(
                n, off as u64,
                "unexpected amounts of data written to chunked buffer"
            );

            // Verify that the data at cb is equivalent to the data at cb2
            let mut bb2 = Vec::new();
            let mut r2 = cb2.new_reader();
            let n = r2
                .read_to_end(&mut bb2)
                .expect("cannot read data from chunked buffer");
            assert_eq!(
                n, off,
                "unexpected amounts of data read from the chunked buffer"
            );

            assert_eq!(bb2, *data, "unexpected data at the second chunked buffer");

            // Verify must_close at chunked buffer
            cb2.must_close();

            put(cb2);
        }

        put(cb);
    }

    #[test]
    fn test_buffer_read_from() {
        let mut cb = get();

        let mut bb: &[u8] = b"foo";
        let n = cb.read_from(&mut bb).expect("unexpected error");
        assert_eq!(n, 3, "unexpected number of bytes written: {n}; want 3");

        let mut bb: &[u8] = b"bar";
        let n = cb.read_from(&mut bb).expect("unexpected error");
        assert_eq!(n, 3, "unexpected number of bytes written: {n}; want 3");

        let mut bb_result = Vec::new();
        cb.must_write_to(&mut bb_result);

        let result_expected = b"foobar";
        assert_eq!(
            bb_result, result_expected,
            "unexpected result; got {bb_result:?}; want {result_expected:?}"
        );

        put(cb);
    }

    #[test]
    fn test_buffer_must_read_at_zero_data() {
        let cb = Buffer::default();
        cb.must_read_at(&mut [], 0);
    }

    #[test]
    fn test_buffer_reader_zero_data() {
        let cb = Buffer::default();
        let mut r = cb.new_reader();
        let mut data = Vec::new();
        r.read_to_end(&mut data).expect("unexpected error");
        assert_eq!(
            data.len(),
            0,
            "unexpected data read with len={}; data={data:?}",
            data.len()
        );
    }

    #[test]
    fn test_buffer_reader_single_chunk() {
        let mut cb = Buffer::default();

        write!(cb, "foo bar baz").unwrap();
        let mut r = cb.new_reader();
        let mut b = [0u8; 4];

        r.read_exact(&mut b).expect("unexpected error");
        assert_eq!(&b, b"foo ", "unexpected data read");

        r.read_exact(&mut b).expect("unexpected error");
        assert_eq!(&b, b"bar ", "unexpected data read");

        let mut data = Vec::new();
        r.read_to_end(&mut data).expect("unexpected error");
        assert_eq!(data, b"baz", "unexpected data read");
    }

    #[test]
    fn test_buffer_write_to_zero_data() {
        let cb = Buffer::default();
        let mut bb = Vec::new();
        cb.must_write_to(&mut bb);
        assert_eq!(
            bb.len(),
            0,
            "unexpected data written to bb with len={}; data={bb:?}",
            bb.len()
        );
    }

    #[test]
    fn test_buffer_write_to_broken_writer() {
        let mut cb = Buffer::default();

        write!(cb, "foo bar baz").unwrap();

        let mut w = FaultyWriter::default();
        let (n, err) = split_write_to(cb.write_to(&mut w));
        assert!(err.is_some(), "expecting non-nil error");
        assert_eq!(n, 0, "expecting zero bytes written; got {n} bytes");

        let mut w = FaultyWriter {
            bytes_to_accept: 5,
            ..Default::default()
        };
        let (n, err) = split_write_to(cb.write_to(&mut w));
        assert!(err.is_some(), "expecting non-nil error");
        assert_eq!(
            n, w.bytes_to_accept as u64,
            "unexpected number of bytes written"
        );

        let mut w = FaultyWriter {
            return_invalid_bytes_read: true,
            ..Default::default()
        };
        let (n, err) = split_write_to(cb.write_to(&mut w));
        assert!(err.is_some(), "expecting non-nil error");
        assert_eq!(n, 0, "unexpected number of bytes written; got {n}; want 0");
    }

    fn split_write_to(result: Result<u64, (u64, String)>) -> (u64, Option<String>) {
        match result {
            Ok(n) => (n, None),
            Err((n, err)) => (n, Some(err)),
        }
    }

    // PORT NOTE: Go's faultyWriter returns `(n, err)` from a single Write
    // call; Rust's io::Write returns either Ok(n) or Err, so the partial
    // acceptance surfaces as a short write, which write_to flags with an
    // error while still reporting the accepted byte count.
    #[derive(Default)]
    struct FaultyWriter {
        bytes_to_accept: usize,
        return_invalid_bytes_read: bool,

        bytes_read: usize,
    }

    impl io::Write for FaultyWriter {
        fn write(&mut self, p: &[u8]) -> io::Result<usize> {
            if self.return_invalid_bytes_read {
                return Ok(0);
            }

            if self.bytes_read + p.len() > self.bytes_to_accept {
                let n = self.bytes_to_accept - self.bytes_read;
                self.bytes_read = self.bytes_to_accept;
                if n == 0 {
                    return Err(io::Error::other("some error"));
                }
                return Ok(n);
            }
            self.bytes_read += p.len();
            Ok(p.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}

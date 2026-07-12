//! Port of Softalink LLC `lib/persistentqueue`
//! (persistentqueue.go, fastqueue.go, filenames.go).
//!
//! A disk-backed FIFO queue of byte blocks ([`Queue`]) plus a fast wrapper
//! ([`FastQueue`]) which prefers passing blocks through memory and falls back
//! to the file-based queue when readers don't keep up with writers.
//!
//! PORT NOTE: Go pools block buffers via `bytesutil.ByteBufferPool`
//! (`blockBufPool`, `headerBufPool`); the port uses plain `Vec<u8>` buffers.
//!
//! PORT NOTE: `metainfo` is marshaled with `encoding/json` in Go; the port
//! hand-rolls the (tiny) JSON encoder/decoder to keep esl-agent dependency-free.
//! The on-disk format is identical for the values the queue writes.

use std::collections::VecDeque;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::Instant;

use esl_common::encoding::{marshal_uint64, unmarshal_uint64};
use esl_common::filestream;
use esl_common::fs as vlfs;
use esl_common::metrics::{Counter, FloatCounter};
use esl_common::{errorf, fasttime, infof, panicf, warnf};

/// MaxBlockSize is the maximum size of the block persistent queue can work with.
pub const MAX_BLOCK_SIZE: u64 = 32 * 1024 * 1024;

/// DefaultChunkFileSize represents default chunk file size.
pub const DEFAULT_CHUNK_FILE_SIZE: u64 = (MAX_BLOCK_SIZE + 8) * 16;

const METAINFO_FILENAME: &str = "metainfo.json";

/// Mirrors Go's `chunkFileNameRegex` (`^[0-9A-F]{16}$`) without a regex dep.
fn is_chunk_file_name(name: &str) -> bool {
    name.len() == 16
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b))
}

/// Error returned by the block-reading path of [`Queue`].
enum ReadError {
    /// Go `errEmptyQueue`.
    EmptyQueue,
    Other(String),
}

/// queue represents a persistent queue.
///
/// It is unsafe to call queue methods from concurrent threads
/// (Go: "concurrent goroutines"); [`FastQueue`] wraps it in a mutex.
struct Queue {
    chunk_file_size: u64,
    max_block_size: u64,
    max_pending_bytes: u64,

    dir: PathBuf,
    name: String,

    flock_f: Option<File>,

    reader: Option<filestream::Reader>,
    reader_path: PathBuf,
    reader_offset: u64,
    reader_local_offset: u64,

    writer: Option<filestream::Writer>,
    writer_path: PathBuf,
    writer_offset: u64,
    writer_local_offset: u64,
    writer_flushed_offset: u64,

    last_metainfo_flush_time: u64,

    blocks_dropped: Arc<Counter>,
    bytes_dropped: Arc<Counter>,
    blocks_written: Arc<Counter>,
    bytes_written: Arc<Counter>,
    blocks_read: Arc<Counter>,
    bytes_read: Arc<Counter>,
}

impl Queue {
    fn reader_mut(&mut self) -> &mut filestream::Reader {
        self.reader
            .as_mut()
            .expect("BUG: queue reader is used after MustClose")
    }

    fn writer_mut(&mut self) -> &mut filestream::Writer {
        self.writer
            .as_mut()
            .expect("BUG: queue writer is used after MustClose")
    }

    /// ResetIfEmpty resets q if it is empty.
    ///
    /// This is needed in order to remove chunk file associated with empty q.
    fn reset_if_empty(&mut self) {
        if self.reader_offset != self.writer_offset {
            // The queue isn't empty.
            return;
        }
        if self.reader_offset < 16 * 1024 * 1024 {
            // The file is too small to drop. Leave it as is in order to reduce filesystem load.
            return;
        }
        self.must_reset_files();
    }

    fn must_reset_files(&mut self) {
        if self.reader_path != self.writer_path {
            panicf!(
                "BUG: readerPath={:?} doesn't match writerPath={:?}",
                self.reader_path,
                self.writer_path
            );
        }
        if let Some(mut r) = self.reader.take() {
            r.must_close();
        }
        if let Some(mut w) = self.writer.take() {
            w.must_close();
        }
        vlfs::must_remove_path(&self.reader_path);

        self.writer_offset = 0;
        self.writer_local_offset = 0;
        self.writer_flushed_offset = 0;

        self.reader_offset = 0;
        self.reader_local_offset = 0;

        self.writer_path = self.chunk_file_path(self.writer_offset);
        self.writer = Some(filestream::must_create(&self.writer_path, false));

        self.reader_path = self.writer_path.clone();
        self.reader = Some(filestream::must_open(&self.reader_path, true));

        self.flush_metainfo();
    }

    /// GetPendingBytes returns the number of pending bytes in the queue.
    fn get_pending_bytes(&self) -> u64 {
        if self.reader_offset > self.writer_offset {
            panicf!(
                "BUG: readerOffset={} cannot exceed writerOffset={}",
                self.reader_offset,
                self.writer_offset
            );
        }
        self.writer_offset - self.reader_offset
    }

    /// MustClose closes q.
    ///
    /// must_write_block mustn't be called during and after the call to must_close.
    fn must_close(&mut self) {
        // Close writer.
        if let Some(mut w) = self.writer.take() {
            w.must_close();
        }

        // Close reader.
        if let Some(mut r) = self.reader.take() {
            r.must_close();
        }

        // Store metainfo.
        self.flush_metainfo();

        // Close flockF.
        self.flock_f = None;
    }

    fn chunk_file_path(&self, offset: u64) -> PathBuf {
        self.dir.join(format!("{offset:016X}"))
    }

    fn metainfo_path(&self) -> PathBuf {
        self.dir.join(METAINFO_FILENAME)
    }

    /// MustWriteBlock writes block to q.
    ///
    /// The block size cannot exceed MaxBlockSize.
    fn must_write_block(&mut self, block: &[u8]) {
        if block.len() as u64 > self.max_block_size {
            panicf!(
                "BUG: too big block to send: {} bytes; it mustn't exceed {} bytes",
                block.len(),
                self.max_block_size
            );
        }
        if self.reader_offset > self.writer_offset {
            panicf!(
                "BUG: readerOffset={} shouldn't exceed writerOffset={}",
                self.reader_offset,
                self.writer_offset
            );
        }
        if self.max_pending_bytes > 0 {
            // Drain the oldest blocks until the number of pending bytes becomes enough for the block.
            let block_size = block.len() as u64 + 8;
            let mut max_pending_bytes = self.max_pending_bytes;
            if block_size < max_pending_bytes {
                max_pending_bytes -= block_size;
            } else {
                max_pending_bytes = 0;
            }
            let mut bb = Vec::new();
            while self.writer_offset - self.reader_offset > max_pending_bytes {
                bb.clear();
                match self.read_block(&mut bb) {
                    Ok(()) => {}
                    Err(ReadError::EmptyQueue) => break,
                    Err(ReadError::Other(err)) => {
                        panicf!("FATAL: cannot read the oldest block {err}");
                    }
                }
                self.blocks_dropped.inc();
                self.bytes_dropped.add(bb.len() as u64);
            }
            if block_size > self.max_pending_bytes {
                // The block is too big to put it into the queue. Drop it.
                return;
            }
        }
        if let Err(err) = self.write_block(block) {
            panicf!("FATAL: {err}");
        }
    }

    fn write_block(&mut self, block: &[u8]) -> Result<(), String> {
        let start_time = Instant::now();
        let res = self.write_block_internal(block);
        write_duration_seconds().add(start_time.elapsed().as_secs_f64());
        res
    }

    fn write_block_internal(&mut self, block: &[u8]) -> Result<(), String> {
        if self.writer_local_offset + self.max_block_size + 8 > self.chunk_file_size {
            self.next_chunk_file_for_write()
                .map_err(|err| format!("cannot create next chunk file: {err}"))?;
        }

        // Write block len.
        let mut header = Vec::with_capacity(8);
        marshal_uint64(&mut header, block.len() as u64);
        self.write(&header).map_err(|err| {
            format!(
                "cannot write header with size 8 bytes to {:?}: {err}",
                self.writer_path
            )
        })?;

        // Write block contents.
        self.write(block).map_err(|err| {
            format!(
                "cannot write block contents with size {} bytes to {:?}: {err}",
                block.len(),
                self.writer_path
            )
        })?;
        self.blocks_written.inc();
        self.bytes_written.add(block.len() as u64);
        self.flush_writer_metainfo_if_needed();
        Ok(())
    }

    fn next_chunk_file_for_write(&mut self) -> Result<(), String> {
        // Finalize the current chunk and start new one.
        if let Some(mut w) = self.writer.take() {
            w.must_close();
        }
        // There is no need to sync writer_path here, since must_close already
        // does this.
        let n = self.writer_offset % self.chunk_file_size;
        if n > 0 {
            self.writer_offset += self.chunk_file_size - n;
        }
        self.writer_flushed_offset = self.writer_offset;
        self.writer_local_offset = 0;
        self.writer_path = self.chunk_file_path(self.writer_offset);
        self.writer = Some(filestream::must_create(&self.writer_path, false));
        self.flush_metainfo();
        vlfs::must_sync_path(&self.dir);
        Ok(())
    }

    /// MustReadBlockNonblocking appends the next block from q to dst.
    ///
    /// false is returned if q is empty.
    fn must_read_block_nonblocking(&mut self, dst: &mut Vec<u8>) -> bool {
        if self.reader_offset > self.writer_offset {
            panicf!(
                "BUG: readerOffset={} cannot exceed writerOffset={}",
                self.reader_offset,
                self.writer_offset
            );
        }
        if self.reader_offset == self.writer_offset {
            return false;
        }
        match self.read_block(dst) {
            Ok(()) => true,
            Err(ReadError::EmptyQueue) => false,
            Err(ReadError::Other(err)) => {
                panicf!("FATAL: {err}");
                unreachable!()
            }
        }
    }

    fn read_block(&mut self, dst: &mut Vec<u8>) -> Result<(), ReadError> {
        let start_time = Instant::now();
        let res = self.read_block_internal(dst);
        read_duration_seconds().add(start_time.elapsed().as_secs_f64());
        res
    }

    fn read_block_internal(&mut self, dst: &mut Vec<u8>) -> Result<(), ReadError> {
        if self.reader_local_offset + self.max_block_size + 8 > self.chunk_file_size {
            self.next_chunk_file_for_read()
                .map_err(|err| ReadError::Other(format!("cannot open next chunk file: {err}")))?;
        }

        // Go: `again:` label + goto.
        loop {
            // Read block len.
            let mut header = [0u8; 8];
            if let Err(err) = self.read_full(&mut header) {
                errorf!(
                    "skipping corrupted {:?}, since header with size 8 bytes cannot be read from it: {err}",
                    self.reader_path
                );
                self.skip_broken_chunk_file()?;
                continue;
            }
            let block_len = unmarshal_uint64(&header);
            // see https://github.com/VictoriaMetrics/VictoriaMetrics/pull/6241
            if block_len == 0 {
                errorf!(
                    "skipping corrupted {:?}, since zero block size is read from it",
                    self.reader_path
                );
                self.skip_broken_chunk_file()?;
                continue;
            }
            if block_len > self.max_block_size {
                errorf!(
                    "skipping corrupted {:?}, since too big block size is read from it: {block_len} bytes; cannot exceed {} bytes",
                    self.reader_path,
                    self.max_block_size
                );
                self.skip_broken_chunk_file()?;
                continue;
            }

            // Read block contents.
            let dst_len = dst.len();
            dst.resize(dst_len + block_len as usize, 0);
            let read_res = {
                let tail = &mut dst[dst_len..];
                self.read_full_slice(tail)
            };
            if let Err(err) = read_res {
                dst.truncate(dst_len);
                errorf!(
                    "skipping corrupted {:?}, since contents with size {block_len} bytes cannot be read from it: {err}",
                    self.reader_path
                );
                self.skip_broken_chunk_file()?;
                continue;
            }
            self.blocks_read.inc();
            self.bytes_read.add(block_len);
            self.flush_reader_metainfo_if_needed();
            return Ok(());
        }
    }

    fn skip_broken_chunk_file(&mut self) -> Result<(), ReadError> {
        // Try to recover from broken chunk file by skipping it.
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1030
        self.reader_offset += self.chunk_file_size - self.reader_offset % self.chunk_file_size;
        if self.reader_offset >= self.writer_offset {
            self.must_reset_files();
            return Err(ReadError::EmptyQueue);
        }
        self.next_chunk_file_for_read().map_err(ReadError::Other)
    }

    fn next_chunk_file_for_read(&mut self) -> Result<(), String> {
        // Remove the current chunk and go to the next chunk.
        if let Some(mut r) = self.reader.take() {
            r.must_close();
        }
        vlfs::must_remove_path(&self.reader_path);
        let n = self.reader_offset % self.chunk_file_size;
        if n > 0 {
            self.reader_offset += self.chunk_file_size - n;
        }
        self.check_reader_writer_offsets()?;
        self.reader_local_offset = 0;
        self.reader_path = self.chunk_file_path(self.reader_offset);
        self.reader = Some(filestream::must_open(&self.reader_path, true));
        self.flush_metainfo();
        vlfs::must_sync_path(&self.dir);
        Ok(())
    }

    fn write(&mut self, buf: &[u8]) -> Result<(), String> {
        let buf_len = buf.len() as u64;
        let n = filestream::WriteCloser::write(self.writer_mut(), buf)
            .map_err(|err| err.to_string())?;
        if n as u64 != buf_len {
            return Err(format!(
                "unexpected number of bytes written; got {n} bytes; want {buf_len} bytes"
            ));
        }
        self.writer_local_offset += buf_len;
        self.writer_offset += buf_len;
        Ok(())
    }

    fn read_full(&mut self, buf: &mut [u8]) -> Result<(), String> {
        self.read_full_slice(buf)
    }

    fn read_full_slice(&mut self, buf: &mut [u8]) -> Result<(), String> {
        let buf_len = buf.len() as u64;
        if self.reader_offset + buf_len > self.writer_flushed_offset {
            self.writer_mut().must_flush(false);
            self.writer_flushed_offset = self.writer_offset;
        }
        // PORT NOTE: Go uses io.ReadFull; std's read_exact has the same
        // "fill the whole buffer or fail" semantics.
        std::io::Read::read_exact(self.reader_mut(), buf).map_err(|err| err.to_string())?;
        self.reader_local_offset += buf_len;
        self.reader_offset += buf_len;
        self.check_reader_writer_offsets()
    }

    fn check_reader_writer_offsets(&self) -> Result<(), String> {
        if self.reader_offset > self.writer_offset {
            return Err(format!(
                "readerOffset={} cannot exceed writerOffset={}; it is likely persistent queue files were corrupted on unclean shutdown",
                self.reader_offset, self.writer_offset
            ));
        }
        Ok(())
    }

    fn flush_reader_metainfo_if_needed(&mut self) {
        let t = fasttime::unix_timestamp();
        if t == self.last_metainfo_flush_time {
            return;
        }
        self.flush_metainfo();
        self.last_metainfo_flush_time = t;
    }

    fn flush_writer_metainfo_if_needed(&mut self) {
        let t = fasttime::unix_timestamp();
        if t == self.last_metainfo_flush_time {
            return;
        }
        self.writer_mut().must_flush(true);
        self.flush_metainfo();
        self.last_metainfo_flush_time = t;
    }

    // PORT NOTE: Go's flushMetainfo returns an error, but the only fallible
    // step is json.Marshal of a plain struct (the file write panics via
    // fs.MustWriteSync in both languages), so the port is infallible.
    fn flush_metainfo(&self) {
        let mi = Metainfo {
            name: self.name.clone(),
            reader_offset: self.reader_offset,
            writer_offset: self.writer_offset,
        };
        mi.write_to_file(&self.metainfo_path());
    }
}

/// mustOpen opens persistent queue from the given path.
///
/// If max_pending_bytes is greater than 0, then the max queue size is limited
/// by this value. The oldest data is deleted when queue size exceeds
/// max_pending_bytes.
fn must_open(path: &Path, name: &str, max_pending_bytes: i64) -> Queue {
    let max_pending_bytes = max_pending_bytes.max(0);
    must_open_internal(
        path,
        name,
        DEFAULT_CHUNK_FILE_SIZE,
        MAX_BLOCK_SIZE,
        max_pending_bytes as u64,
    )
}

fn must_open_internal(
    path: &Path,
    name: &str,
    chunk_file_size: u64,
    max_block_size: u64,
    max_pending_bytes: u64,
) -> Queue {
    if chunk_file_size < 8 || chunk_file_size - 8 < max_block_size {
        panicf!(
            "BUG: too small chunkFileSize={chunk_file_size} for maxBlockSize={max_block_size}; chunkFileSize must fit at least one block"
        );
    }
    if max_block_size == 0 {
        panicf!("BUG: maxBlockSize must be greater than 0; got {max_block_size}");
    }
    match try_opening_queue(
        path,
        name,
        chunk_file_size,
        max_block_size,
        max_pending_bytes,
    ) {
        Ok(q) => q,
        Err(err) => {
            errorf!(
                "cannot open persistent queue at {path:?}: {err}; cleaning it up and trying again"
            );
            vlfs::must_remove_dir_contents(path);
            match try_opening_queue(
                path,
                name,
                chunk_file_size,
                max_block_size,
                max_pending_bytes,
            ) {
                Ok(q) => q,
                Err(err) => {
                    panicf!("FATAL: {err}");
                    unreachable!()
                }
            }
        }
    }
}

/// Closes reader, writer and flock on the error paths (Go: cleanOnError plus
/// the deferred mustCloseFlockF).
fn clean_on_error(q: &mut Queue) {
    if let Some(mut r) = q.reader.take() {
        r.must_close();
    }
    if let Some(mut w) = q.writer.take() {
        w.must_close();
    }
    q.flock_f = None;
}

/// The `esm_persistentqueue_<name>{path=...}` registry counter (Go
/// `vm_persistentqueue_*`).
fn pq_counter(name: &str, path: &Path) -> Arc<Counter> {
    esl_common::metrics::get_or_create_counter(&format!(
        "esm_persistentqueue_{name}{{path={:?}}}",
        path.to_string_lossy()
    ))
}

fn write_duration_seconds() -> &'static Arc<FloatCounter> {
    static C: LazyLock<Arc<FloatCounter>> = LazyLock::new(|| {
        esl_common::metrics::new_float_counter("esm_persistentqueue_write_duration_seconds_total")
    });
    &C
}

fn read_duration_seconds() -> &'static Arc<FloatCounter> {
    static C: LazyLock<Arc<FloatCounter>> = LazyLock::new(|| {
        esl_common::metrics::new_float_counter("esm_persistentqueue_read_duration_seconds_total")
    });
    &C
}

fn try_opening_queue(
    path: &Path,
    name: &str,
    chunk_file_size: u64,
    max_block_size: u64,
    max_pending_bytes: u64,
) -> Result<Queue, String> {
    let mut q = Queue {
        chunk_file_size,
        max_block_size,
        max_pending_bytes,
        dir: path.to_path_buf(),
        name: name.to_string(),
        flock_f: None,
        reader: None,
        reader_path: PathBuf::new(),
        reader_offset: 0,
        reader_local_offset: 0,
        writer: None,
        writer_path: PathBuf::new(),
        writer_offset: 0,
        writer_local_offset: 0,
        writer_flushed_offset: 0,
        last_metainfo_flush_time: 0,
        blocks_dropped: pq_counter("blocks_dropped_total", path),
        bytes_dropped: pq_counter("bytes_dropped_total", path),
        blocks_written: pq_counter("blocks_written_total", path),
        bytes_written: pq_counter("bytes_written_total", path),
        blocks_read: pq_counter("blocks_read_total", path),
        bytes_read: pq_counter("bytes_read_total", path),
    };

    // Protect from concurrent opens.
    vlfs::must_mkdir_if_not_exist(path);
    q.flock_f = Some(vlfs::must_create_flock_file(path));
    vlfs::must_sync_path_and_parent_dir(path);

    // Read metainfo.
    let mut mi = Metainfo::default();
    let metainfo_path = q.metainfo_path();
    if let Err(err) = mi.read_from_file(&metainfo_path) {
        if let MetainfoReadError::Other(msg) = &err {
            errorf!(
                "cannot read metainfo for persistent queue from {metainfo_path:?}: {msg}; re-creating {path:?}"
            );
        }

        // path contents is broken or missing. Re-create it from scratch.
        q.flock_f = None;
        vlfs::must_remove_dir_contents(path);
        q.flock_f = Some(vlfs::must_create_flock_file(path));
        mi.reset();
        mi.name = q.name.clone();
        mi.write_to_file(&metainfo_path);

        // Create initial chunk file.
        let filepath = q.chunk_file_path(0);
        vlfs::must_write_atomic(&filepath, &[], false);
    }

    // Locate reader and writer chunks in the path.
    for de in vlfs::must_read_dir(path) {
        let fname = de.file_name().to_string_lossy().into_owned();
        let filepath = path.join(&fname);
        if de.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            errorf!("skipping unknown directory {filepath:?}");
            continue;
        }
        if fname == METAINFO_FILENAME {
            // skip metainfo file
            continue;
        }
        if fname == vlfs::FLOCK_FILENAME {
            // skip flock file
            continue;
        }
        if !is_chunk_file_name(&fname) {
            errorf!("skipping unknown file {filepath:?}");
            continue;
        }
        let offset = match u64::from_str_radix(&fname, 16) {
            Ok(offset) => offset,
            Err(err) => {
                panicf!("BUG: cannot parse hex {fname:?}: {err}");
                unreachable!()
            }
        };
        if offset % q.chunk_file_size != 0 {
            errorf!(
                "unexpected offset for chunk file {filepath:?}: {offset}; it must be multiple of {}; removing the file",
                q.chunk_file_size
            );
            vlfs::must_remove_path(&filepath);
            continue;
        }
        if mi.reader_offset >= offset + q.chunk_file_size {
            errorf!("unexpected chunk file found from the past: {filepath:?}; removing it");
            vlfs::must_remove_path(&filepath);
            continue;
        }
        if mi.writer_offset < offset {
            errorf!("unexpected chunk file found from the future: {filepath:?}; removing it");
            vlfs::must_remove_path(&filepath);
            continue;
        }
        if mi.reader_offset >= offset && mi.reader_offset < offset + q.chunk_file_size {
            // Found the chunk for reading
            if q.reader.is_some() {
                panicf!(
                    "BUG: reader is already initialized with readerPath={:?}, readerOffset={}, readerLocalOffset={}",
                    q.reader_path,
                    q.reader_offset,
                    q.reader_local_offset
                );
            }
            q.reader_path = filepath.clone();
            q.reader_offset = mi.reader_offset;
            q.reader_local_offset = mi.reader_offset % q.chunk_file_size;
            let file_size = vlfs::must_file_size(&q.reader_path);
            if file_size < q.reader_local_offset {
                errorf!(
                    "chunk file {:?} size is too small for the given reader offset; file size {file_size} bytes; reader offset: {} bytes; removing the file",
                    q.reader_path,
                    q.reader_local_offset
                );
                vlfs::must_remove_path(&q.reader_path);
                continue;
            }
            match filestream::open_reader_at(&q.reader_path, q.reader_local_offset, true) {
                Ok(r) => q.reader = Some(r),
                Err(err) => {
                    errorf!(
                        "cannot open {:?} for reading at offset {}: {err}; removing this file",
                        q.reader_path,
                        q.reader_local_offset
                    );
                    vlfs::must_remove_path(&filepath);
                    continue;
                }
            }
        }
        if mi.writer_offset >= offset && mi.writer_offset < offset + q.chunk_file_size {
            // Found the chunk file for writing
            if q.writer.is_some() {
                panicf!(
                    "BUG: writer is already initialized with writerPath={:?}, writerOffset={}, writerLocalOffset={}",
                    q.writer_path,
                    q.writer_offset,
                    q.writer_local_offset
                );
            }
            q.writer_path = filepath.clone();
            q.writer_offset = mi.writer_offset;
            q.writer_local_offset = mi.writer_offset % q.chunk_file_size;
            q.writer_flushed_offset = mi.writer_offset;
            let file_size = vlfs::must_file_size(&q.writer_path);
            if file_size != q.writer_local_offset {
                if file_size < q.writer_local_offset {
                    errorf!(
                        "{:?} size ({file_size} bytes) is smaller than the writer offset ({} bytes); removing the file",
                        q.writer_path,
                        q.writer_local_offset
                    );
                    vlfs::must_remove_path(&q.writer_path);
                    continue;
                }
                warnf!(
                    "{:?} size ({file_size} bytes) is bigger than writer offset ({} bytes); \
                     this may be the case on unclean shutdown (OOM, `kill -9`, hardware reset); trying to fix it by adjusting fileSize to {}",
                    q.writer_path,
                    q.writer_local_offset,
                    q.writer_local_offset
                );
            }
            match filestream::open_writer_at(&q.writer_path, q.writer_local_offset, false) {
                Ok(w) => q.writer = Some(w),
                Err(err) => {
                    errorf!(
                        "cannot open {:?} for writing at offset {}: {err}; removing this file",
                        q.writer_path,
                        q.writer_local_offset
                    );
                    vlfs::must_remove_path(&filepath);
                    continue;
                }
            }
        }
    }
    if q.reader.is_none() {
        clean_on_error(&mut q);
        return Err(format!(
            "couldn't find chunk file for reading in {:?}",
            q.dir
        ));
    }
    if q.writer.is_none() {
        clean_on_error(&mut q);
        return Err(format!(
            "couldn't find chunk file for writing in {:?}",
            q.dir
        ));
    }
    if q.reader_offset > q.writer_offset {
        let err = format!(
            "readerOffset={} cannot exceed writerOffset={}",
            q.reader_offset, q.writer_offset
        );
        clean_on_error(&mut q);
        return Err(err);
    }
    Ok(q)
}

// ---------------------------------------------------------------------------
// metainfo
// ---------------------------------------------------------------------------

#[derive(Debug, Default, PartialEq)]
struct Metainfo {
    name: String,
    reader_offset: u64,
    writer_offset: u64,
}

enum MetainfoReadError {
    /// The metainfo file does not exist (Go: os.IsNotExist).
    NotExist,
    Other(String),
}

impl Metainfo {
    fn reset(&mut self) {
        self.reader_offset = 0;
        self.writer_offset = 0;
    }

    fn write_to_file(&self, path: &Path) {
        let mut data = String::with_capacity(64 + self.name.len());
        data.push_str("{\"Name\":");
        append_json_string(&mut data, &self.name);
        data.push_str(&format!(
            ",\"ReaderOffset\":{},\"WriterOffset\":{}}}",
            self.reader_offset, self.writer_offset
        ));
        vlfs::must_write_sync(path, data.as_bytes());
    }

    fn read_from_file(&mut self, path: &Path) -> Result<(), MetainfoReadError> {
        self.reset();
        let data = match std::fs::read(path) {
            Ok(data) => data,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(MetainfoReadError::NotExist);
            }
            Err(err) => {
                return Err(MetainfoReadError::Other(format!(
                    "cannot read {path:?}: {err}"
                )));
            }
        };
        let parsed = parse_metainfo_json(&data).map_err(|err| {
            MetainfoReadError::Other(format!(
                "cannot unmarshal persistent queue metainfo from {path:?}: {err}"
            ))
        })?;
        *self = parsed;
        if self.reader_offset > self.writer_offset {
            return Err(MetainfoReadError::Other(format!(
                "invalid data read from {path:?}: readerOffset={} cannot exceed writerOffset={}",
                self.reader_offset, self.writer_offset
            )));
        }
        Ok(())
    }
}

/// Appends s to dst as a JSON string (Go `encoding/json` string encoding,
/// minus the HTML escaping, which is irrelevant for queue names).
fn append_json_string(dst: &mut String, s: &str) {
    dst.push('"');
    for c in s.chars() {
        match c {
            '"' => dst.push_str("\\\""),
            '\\' => dst.push_str("\\\\"),
            '\n' => dst.push_str("\\n"),
            '\r' => dst.push_str("\\r"),
            '\t' => dst.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                dst.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => dst.push(c),
        }
    }
    dst.push('"');
}

/// Minimal JSON object parser for the metainfo file
/// (`{"Name":string,"ReaderOffset":number,"WriterOffset":number}`).
/// Unknown keys are ignored, like Go `json.Unmarshal`.
fn parse_metainfo_json(data: &[u8]) -> Result<Metainfo, String> {
    let s = std::str::from_utf8(data).map_err(|err| format!("invalid utf-8: {err}"))?;
    let mut p = JsonParser {
        s: s.as_bytes(),
        pos: 0,
    };
    let mut mi = Metainfo::default();
    p.skip_ws();
    p.expect(b'{')?;
    p.skip_ws();
    if p.peek() == Some(b'}') {
        return Ok(mi);
    }
    loop {
        p.skip_ws();
        let key = p.parse_string()?;
        p.skip_ws();
        p.expect(b':')?;
        p.skip_ws();
        match p.peek() {
            Some(b'"') => {
                let v = p.parse_string()?;
                if key == "Name" {
                    mi.name = v;
                }
            }
            Some(c) if c.is_ascii_digit() => {
                let v = p.parse_u64()?;
                match key.as_str() {
                    "ReaderOffset" => mi.reader_offset = v,
                    "WriterOffset" => mi.writer_offset = v,
                    _ => {}
                }
            }
            _ => return Err(format!("unexpected value for key {key:?}")),
        }
        p.skip_ws();
        match p.next() {
            Some(b',') => continue,
            Some(b'}') => return Ok(mi),
            _ => return Err("expected ',' or '}'".to_string()),
        }
    }
}

struct JsonParser<'a> {
    s: &'a [u8],
    pos: usize,
}

impl JsonParser<'_> {
    fn skip_ws(&mut self) {
        while self
            .peek()
            .map(|c| c.is_ascii_whitespace())
            .unwrap_or(false)
        {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.s.get(self.pos).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn expect(&mut self, c: u8) -> Result<(), String> {
        if self.next() != Some(c) {
            return Err(format!("expected {:?}", c as char));
        }
        Ok(())
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            match self.next() {
                None => return Err("unterminated string".to_string()),
                Some(b'"') => return Ok(out),
                Some(b'\\') => match self.next() {
                    Some(b'"') => out.push('"'),
                    Some(b'\\') => out.push('\\'),
                    Some(b'/') => out.push('/'),
                    Some(b'b') => out.push('\u{8}'),
                    Some(b'f') => out.push('\u{c}'),
                    Some(b'n') => out.push('\n'),
                    Some(b'r') => out.push('\r'),
                    Some(b't') => out.push('\t'),
                    Some(b'u') => {
                        if self.pos + 4 > self.s.len() {
                            return Err("truncated \\u escape".to_string());
                        }
                        let hex = std::str::from_utf8(&self.s[self.pos..self.pos + 4])
                            .map_err(|_| "invalid \\u escape".to_string())?;
                        let n = u32::from_str_radix(hex, 16)
                            .map_err(|_| "invalid \\u escape".to_string())?;
                        self.pos += 4;
                        out.push(char::from_u32(n).unwrap_or('\u{fffd}'));
                    }
                    _ => return Err("invalid escape".to_string()),
                },
                Some(c) if c < 0x80 => out.push(c as char),
                Some(c) => {
                    // Multi-byte utf-8 sequence: copy it verbatim.
                    let start = self.pos - 1;
                    let width = match c {
                        0xC0..=0xDF => 2,
                        0xE0..=0xEF => 3,
                        _ => 4,
                    };
                    if start + width > self.s.len() {
                        return Err("truncated utf-8 sequence".to_string());
                    }
                    let chunk = std::str::from_utf8(&self.s[start..start + width])
                        .map_err(|_| "invalid utf-8 in string".to_string())?;
                    out.push_str(chunk);
                    self.pos = start + width;
                }
            }
        }
    }

    fn parse_u64(&mut self) -> Result<u64, String> {
        let start = self.pos;
        while self.peek().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            self.pos += 1;
        }
        if self.pos == start {
            return Err("expected number".to_string());
        }
        std::str::from_utf8(&self.s[start..self.pos])
            .unwrap()
            .parse::<u64>()
            .map_err(|err| format!("invalid number: {err}"))
    }
}

// ---------------------------------------------------------------------------
// FastQueue (fastqueue.go)
// ---------------------------------------------------------------------------

/// FastQueue is fast persistent queue, which prefers sending data via memory.
///
/// It falls back to sending data via file when readers don't catch up with writers.
pub struct FastQueue {
    /// mu protects the state of FastQueue.
    mu: Mutex<FastQueueInner>,

    /// cond is used for notifying blocked readers when new data has been added
    /// or when must_close is called.
    cond: Condvar,

    /// is_pq_disabled is set to true when pq is disabled.
    is_pq_disabled: bool,
}

struct FastQueueInner {
    /// pq is the file-based queue.
    pq: Queue,

    /// ch is the in-memory queue (Go: buffered channel of pooled byte buffers).
    ch: VecDeque<Vec<u8>>,
    ch_cap: usize,

    pending_inmemory_bytes: u64,

    last_inmemory_block_read_time: u64,

    stop_deadline: u64,
}

impl FastQueue {
    /// MustOpenFastQueue opens persistent queue at the given path.
    ///
    /// It holds up to max_inmemory_blocks in memory before falling back to
    /// file-based persistence.
    ///
    /// if max_pending_bytes is 0, then the queue size is unlimited. Otherwise
    /// its size is limited by max_pending_bytes. The oldest data is dropped
    /// when the queue reaches max_pending_size.
    /// if is_pq_disabled is set to true, then write requests that exceed
    /// in-memory buffer capacity are rejected. The in-memory queue part can be
    /// stored on disk during graceful shutdown.
    ///
    /// PORT NOTE: returns `Arc<FastQueue>` (Go returns `*FastQueue`) so the
    /// `esm_persistentqueue_bytes_pending` / `esm_persistentqueue_free_disk_space_bytes`
    /// gauge callbacks can hold the queue, like the Go closures do.
    pub fn must_open_fast_queue(
        path: &Path,
        name: &str,
        max_inmemory_blocks: usize,
        max_pending_bytes: i64,
        is_pq_disabled: bool,
    ) -> Arc<FastQueue> {
        let pq = must_open(path, name, max_pending_bytes);
        let fq = Arc::new(FastQueue {
            mu: Mutex::new(FastQueueInner {
                pq,
                ch: VecDeque::with_capacity(max_inmemory_blocks),
                ch_cap: max_inmemory_blocks,
                pending_inmemory_bytes: 0,
                last_inmemory_block_read_time: fasttime::unix_timestamp(),
                stop_deadline: 0,
            }),
            cond: Condvar::new(),
            is_pq_disabled,
        });

        let path_label = path.to_string_lossy().into_owned();
        let fq_gauge = Arc::clone(&fq);
        let _ = esl_common::metrics::get_or_create_gauge(
            &format!("esm_persistentqueue_bytes_pending{{path={path_label:?}}}"),
            Some(Box::new(move || fq_gauge.get_pending_bytes() as f64)),
        );
        let free_space_path = path.to_path_buf();
        let _ = esl_common::metrics::get_or_create_gauge(
            &format!("esm_persistentqueue_free_disk_space_bytes{{path={path_label:?}}}"),
            Some(Box::new(move || {
                vlfs::must_get_free_space(&free_space_path) as f64
            })),
        );

        let pending_bytes = fq.get_pending_bytes();
        let persistence_status = if is_pq_disabled {
            "disabled"
        } else {
            "enabled"
        };
        infof!(
            "opened fast queue at {path:?} with maxInmemoryBlocks={max_inmemory_blocks}, it contains {pending_bytes} pending bytes, persistence is {persistence_status}"
        );
        fq
    }

    /// IsPersistentQueueDisabled returns true if persistent queue at fq is disabled.
    pub fn is_persistent_queue_disabled(&self) -> bool {
        self.is_pq_disabled
    }

    /// IsWriteBlocked checks if data can be pushed into fq.
    pub fn is_write_blocked(&self) -> bool {
        if !self.is_pq_disabled {
            return false;
        }
        let inner = self.mu.lock().unwrap();
        inner.ch.len() == inner.ch_cap || inner.pq.get_pending_bytes() > 0
    }

    /// UnblockAllReaders unblocks all the readers.
    pub fn unblock_all_readers(&self) {
        let mut inner = self.mu.lock().unwrap();
        // Unblock blocked readers
        // Allow for up to 5 seconds for sending Prometheus stale markers.
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1526
        inner.stop_deadline = fasttime::unix_timestamp() + 5;
        self.cond.notify_all();
    }

    /// MustClose unblocks all the readers.
    ///
    /// It is expected no new writers during and after the call.
    pub fn must_close(&self) {
        self.unblock_all_readers();

        let mut inner = self.mu.lock().unwrap();

        // flush blocks from fq.ch to fq.pq, so they can be persisted
        self.flush_inmemory_blocks_to_file_locked(&mut inner);

        // Close fq.pq
        inner.pq.must_close();

        infof!("closed fast persistent queue at {:?}", inner.pq.dir);
    }

    fn flush_inmemory_blocks_to_file_if_needed_locked(&self, inner: &mut FastQueueInner) {
        if inner.ch.is_empty() || self.is_pq_disabled {
            return;
        }
        if fasttime::unix_timestamp() < inner.last_inmemory_block_read_time + 5 {
            return;
        }
        self.flush_inmemory_blocks_to_file_locked(inner);
    }

    fn flush_inmemory_blocks_to_file_locked(&self, inner: &mut FastQueueInner) {
        // fq.mu must be locked by the caller.
        while let Some(bb) = inner.ch.pop_front() {
            inner.pq.must_write_block(&bb);
            inner.pending_inmemory_bytes -= bb.len() as u64;
            inner.last_inmemory_block_read_time = fasttime::unix_timestamp();
        }
        // Unblock all the potentially blocked readers, so they could proceed
        // with reading file-based queue.
        self.cond.notify_all();
    }

    /// GetPendingBytes returns the number of pending bytes in the fq.
    pub fn get_pending_bytes(&self) -> u64 {
        let inner = self.mu.lock().unwrap();
        inner.pending_inmemory_bytes + inner.pq.get_pending_bytes()
    }

    /// GetInmemoryQueueLen returns the length of inmemory queue.
    pub fn get_inmemory_queue_len(&self) -> usize {
        let inner = self.mu.lock().unwrap();
        inner.ch.len()
    }

    /// MustWriteBlockIgnoreDisabledPQ unconditionally writes block to fq.
    ///
    /// This method allows persisting in-memory blocks during graceful
    /// shutdown, even if persistence is disabled.
    pub fn must_write_block_ignore_disabled_pq(&self, block: &[u8]) {
        if !self.try_write_block_internal(block, true) {
            panicf!("BUG: tryWriteBlock must always write data even if persistence is disabled");
        }
    }

    /// TryWriteBlock tries writing block to fq.
    ///
    /// false is returned if the block couldn't be written to fq when the
    /// in-memory queue is full and the persistent queue is disabled.
    pub fn try_write_block(&self, block: &[u8]) -> bool {
        self.try_write_block_internal(block, false)
    }

    fn try_write_block_internal(&self, block: &[u8], ignore_disabled_pq: bool) -> bool {
        let mut inner = self.mu.lock().unwrap();

        let is_pq_write_allowed = !self.is_pq_disabled || ignore_disabled_pq;

        self.flush_inmemory_blocks_to_file_if_needed_locked(&mut inner);
        let n = inner.pq.get_pending_bytes();
        if n > 0 {
            // The file-based queue isn't drained yet. This means that
            // in-memory queue cannot be used yet. So put the block to
            // file-based queue.
            if !inner.ch.is_empty() {
                panicf!(
                    "BUG: the in-memory queue must be empty when the file-based queue is non-empty; it contains {n} pending bytes"
                );
            }
            if !is_pq_write_allowed {
                return false;
            }
            inner.pq.must_write_block(block);
            return true;
        }
        if inner.ch.len() == inner.ch_cap {
            // There is no space left in the in-memory queue. Put the data to
            // file-based queue.
            if !is_pq_write_allowed {
                return false;
            }
            self.flush_inmemory_blocks_to_file_locked(&mut inner);
            inner.pq.must_write_block(block);
            return true;
        }
        // Fast path - put the block to in-memory queue.
        inner.ch.push_back(block.to_vec());
        inner.pending_inmemory_bytes += block.len() as u64;

        // Notify potentially blocked reader.
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/pull/484 for the context.
        self.cond.notify_one();
        true
    }

    /// MustReadBlock reads the next block from fq, appending it to dst.
    /// It first reads from the in-memory queue, then checks file-based queue.
    /// It blocks until a block is available or the stop deadline is exceeded,
    /// in which case it returns false.
    pub fn must_read_block(&self, dst: &mut Vec<u8>) -> bool {
        let mut inner = self.mu.lock().unwrap();

        loop {
            if inner.stop_deadline > 0 && fasttime::unix_timestamp() > inner.stop_deadline {
                return false;
            }
            if !inner.ch.is_empty() {
                Self::must_read_in_memory_block_locked(&mut inner, dst);
                return true;
            }
            if inner.pq.get_pending_bytes() > 0 {
                if inner.pq.must_read_block_nonblocking(dst) {
                    return true;
                }
                continue;
            }
            if inner.stop_deadline > 0 {
                return false;
            }
            // There are no blocks. Wait for new block.
            inner.pq.reset_if_empty();
            inner = self.cond.wait(inner).unwrap();
        }
    }

    /// MustReadInMemoryBlock reads the next block from the in-memory queue,
    /// appending it to dst. It returns true if a block was available, or false
    /// if the in-memory queue is empty. It does not block waiting for new
    /// blocks.
    pub fn must_read_in_memory_block(&self, dst: &mut Vec<u8>) -> bool {
        let mut inner = self.mu.lock().unwrap();

        if !inner.ch.is_empty() {
            Self::must_read_in_memory_block_locked(&mut inner, dst);
            return true;
        }

        false
    }

    fn must_read_in_memory_block_locked(inner: &mut FastQueueInner, dst: &mut Vec<u8>) {
        if inner.ch.is_empty() {
            panicf!(
                "BUG: the function must not be called when in-memory queue is empty. Caller should verify the queue len upfront"
            );
        }
        let n = inner.pq.get_pending_bytes();
        if n > 0 {
            panicf!(
                "BUG: the file-based queue must be empty when the in-memory queue is non-empty; it contains {n} pending bytes"
            );
        }
        let bb = inner.ch.pop_front().unwrap();
        inner.pending_inmemory_bytes -= bb.len() as u64;
        inner.last_inmemory_block_read_time = fasttime::unix_timestamp();
        dst.extend_from_slice(&bb);
    }

    /// Dirname returns the directory name for persistent queue.
    pub fn dirname(&self) -> String {
        let inner = self.mu.lock().unwrap();
        inner
            .pq
            .dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Tests (persistentqueue_test.go + fastqueue_test.go)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    // PORT NOTE: the Go tests use relative paths in the package directory;
    // the port uses unique directories under the system temp dir so parallel
    // tests don't collide.
    fn test_dir(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!(
            "esl-agent-persistentqueue-{}-{n}-{name}",
            std::process::id()
        ))
    }

    fn must_create_file(path: &Path, contents: &str) {
        vlfs::must_write_sync(path, contents.as_bytes());
    }

    fn must_create_dir(path: &Path) {
        vlfs::must_remove_dir(path);
        std::fs::create_dir_all(path)
            .unwrap_or_else(|err| panic!("cannot create dir {path:?}: {err}"));
    }

    fn must_create_empty_metainfo(path: &Path, name: &str) {
        let mi = Metainfo {
            name: name.to_string(),
            ..Default::default()
        };
        mi.write_to_file(&path.join(METAINFO_FILENAME));
    }

    #[test]
    fn test_queue_open_close() {
        let path = test_dir("queue-open-close");
        vlfs::must_remove_dir(&path);
        for _ in 0..3 {
            let mut q = must_open(&path, "foobar", 0);
            let n = q.get_pending_bytes();
            assert_eq!(n, 0, "pending bytes must be 0; got {n}");
            q.must_close();
        }
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_open_invalid_metainfo() {
        let path = test_dir("queue-open-invalid-metainfo");
        must_create_dir(&path);
        must_create_file(&path.join(METAINFO_FILENAME), "foobarbaz");
        let mut q = must_open(&path, "foobar", 0);
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_open_junk_files_and_dirs() {
        let path = test_dir("queue-open-junk-files-and-dir");
        must_create_dir(&path);
        must_create_empty_metainfo(&path, "foobar");
        must_create_file(&path.join("junk-file"), "foobar");
        must_create_dir(&path.join("junk-dir"));
        let mut q = must_open(&path, "foobar", 0);
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_open_invalid_chunk_offset() {
        let path = test_dir("queue-open-invalid-chunk-offset");
        must_create_dir(&path);
        must_create_empty_metainfo(&path, "foobar");
        must_create_file(&path.join(format!("{:016X}", 1234)), "qwere");
        let mut q = must_open(&path, "foobar", 0);
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_open_too_new_chunk() {
        let path = test_dir("queue-open-too-new-chunk");
        must_create_dir(&path);
        must_create_empty_metainfo(&path, "foobar");
        must_create_file(
            &path.join(format!("{:016X}", 100 * DEFAULT_CHUNK_FILE_SIZE)),
            "asdf",
        );
        let mut q = must_open(&path, "foobar", 0);
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_open_too_old_chunk() {
        let path = test_dir("queue-open-too-old-chunk");
        must_create_dir(&path);
        let mi = Metainfo {
            name: "foobar".to_string(),
            reader_offset: DEFAULT_CHUNK_FILE_SIZE,
            writer_offset: DEFAULT_CHUNK_FILE_SIZE,
        };
        mi.write_to_file(&path.join(METAINFO_FILENAME));
        must_create_file(&path.join(format!("{:016X}", 0)), "adfsfd");
        let mut q = must_open(&path, &mi.name, 0);
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_open_too_big_reader_offset() {
        let path = test_dir("queue-open-too-big-reader-offset");
        must_create_dir(&path);
        // PORT NOTE: Go's mi.WriteToFile happily writes ReaderOffset >
        // WriterOffset (validation happens on read); write the raw JSON here.
        let data = format!(
            "{{\"Name\":\"foobar\",\"ReaderOffset\":{},\"WriterOffset\":0}}",
            DEFAULT_CHUNK_FILE_SIZE + 123
        );
        vlfs::must_write_sync(path.join(METAINFO_FILENAME), data.as_bytes());
        let mut q = must_open(&path, "foobar", 0);
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_open_metainfo_dir() {
        let path = test_dir("queue-open-metainfo-dir");
        must_create_dir(&path);
        must_create_dir(&path.join(METAINFO_FILENAME));
        let mut q = must_open(&path, "foobar", 0);
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_open_too_small_reader_file() {
        let path = test_dir("too-small-reader-file");
        must_create_dir(&path);
        let mi = Metainfo {
            name: "foobar".to_string(),
            reader_offset: 123,
            writer_offset: 123,
        };
        mi.write_to_file(&path.join(METAINFO_FILENAME));
        must_create_file(&path.join(format!("{:016X}", 0)), "sdf");
        let mut q = must_open(&path, &mi.name, 0);
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_open_invalid_writer_file_size() {
        let path = test_dir("invalid-writer-file-size");
        must_create_dir(&path);
        must_create_empty_metainfo(&path, "foobar");
        must_create_file(&path.join(format!("{:016X}", 0)), "sdfdsf");
        let mut q = must_open(&path, "foobar", 0);
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_open_invalid_queue_name() {
        let path = test_dir("invalid-queue-name");
        must_create_dir(&path);
        let mi = Metainfo {
            name: "foobar".to_string(),
            ..Default::default()
        };
        mi.write_to_file(&path.join(METAINFO_FILENAME));
        must_create_file(&path.join(format!("{:016X}", 0)), "sdf");
        let mut q = must_open(&path, "baz", 0);
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_reset_if_empty() {
        let path = test_dir("queue-reset-if-empty");
        vlfs::must_remove_dir(&path);
        let mut q = must_open(&path, "foobar", 0);

        let block = vec![0u8; 1024 * 1024];
        let mut buf = Vec::new();
        for _ in 0..10 {
            for _ in 0..10 {
                q.must_write_block(&block);
                buf.clear();
                let ok = q.must_read_block_nonblocking(&mut buf);
                assert!(
                    ok,
                    "unexpected ok=false returned from must_read_block_nonblocking"
                );
            }
            q.reset_if_empty();
            let n = q.get_pending_bytes();
            assert_eq!(
                n, 0,
                "unexpected non-zero pending bytes after queue reset: {n}"
            );
            q.reset_if_empty();
            let n = q.get_pending_bytes();
            assert_eq!(
                n, 0,
                "unexpected non-zero pending bytes after queue reset: {n}"
            );
        }

        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_write_read() {
        let path = test_dir("queue-write-read");
        vlfs::must_remove_dir(&path);
        let mut q = must_open(&path, "foobar", 0);

        for j in 0..5 {
            let mut blocks: Vec<Vec<u8>> = Vec::new();
            for i in 0..10 {
                let block = format!("block {j}+{i}").into_bytes();
                q.must_write_block(&block);
                blocks.push(block);
            }
            assert!(
                q.get_pending_bytes() > 0,
                "pending bytes must be greater than 0"
            );
            let mut buf = Vec::new();
            for block in &blocks {
                buf.clear();
                let ok = q.must_read_block_nonblocking(&mut buf);
                assert!(ok, "unexpected ok=false returned; want true");
                assert_eq!(
                    &buf, block,
                    "unexpected block read; got {buf:?}; want {block:?}"
                );
            }
            let n = q.get_pending_bytes();
            assert_eq!(n, 0, "pending bytes must be 0; got {n}");
        }

        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_write_close_read() {
        let path = test_dir("queue-write-close-read");
        vlfs::must_remove_dir(&path);
        let mut q = must_open(&path, "foobar", 0);

        for j in 0..5 {
            let mut blocks: Vec<Vec<u8>> = Vec::new();
            for i in 0..10 {
                let block = format!("block {j}+{i}").into_bytes();
                q.must_write_block(&block);
                blocks.push(block);
            }
            assert!(
                q.get_pending_bytes() > 0,
                "pending bytes must be greater than 0"
            );
            q.must_close();
            q = must_open(&path, "foobar", 0);
            assert!(
                q.get_pending_bytes() > 0,
                "pending bytes must be greater than 0"
            );
            let mut buf = Vec::new();
            for block in &blocks {
                buf.clear();
                let ok = q.must_read_block_nonblocking(&mut buf);
                assert!(ok, "unexpected ok=false returned; want true");
                assert_eq!(
                    &buf, block,
                    "unexpected block read; got {buf:?}; want {block:?}"
                );
            }
            let n = q.get_pending_bytes();
            assert_eq!(n, 0, "pending bytes must be 0; got {n}");
        }

        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_chunk_management_simple() {
        let path = test_dir("queue-chunk-management-simple");
        vlfs::must_remove_dir(&path);
        const CHUNK_FILE_SIZE: u64 = 100;
        const BLOCK_SIZE_MAX: u64 = 20;
        let mut q = must_open_internal(&path, "foobar", CHUNK_FILE_SIZE, BLOCK_SIZE_MAX, 0);
        let mut blocks = Vec::new();
        for i in 0..100 {
            let block = format!("block {i}");
            q.must_write_block(block.as_bytes());
            blocks.push(block);
        }
        assert_ne!(
            q.get_pending_bytes(),
            0,
            "unexpected zero number of bytes pending"
        );
        for block in &blocks {
            let mut data = Vec::new();
            let ok = q.must_read_block_nonblocking(&mut data);
            assert!(ok, "unexpected ok=false");
            assert_eq!(
                block.as_bytes(),
                &data[..],
                "unexpected block read; got {data:?}; want {block:?}"
            );
        }
        let n = q.get_pending_bytes();
        assert_eq!(n, 0, "unexpected non-zero number of pending bytes: {n}");
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_chunk_management_periodic_close() {
        let path = test_dir("queue-chunk-management-periodic-close");
        vlfs::must_remove_dir(&path);
        const CHUNK_FILE_SIZE: u64 = 100;
        const BLOCK_SIZE_MAX: u64 = 20;
        let mut q = must_open_internal(&path, "foobar", CHUNK_FILE_SIZE, BLOCK_SIZE_MAX, 0);
        let mut blocks = Vec::new();
        for i in 0..100 {
            let block = format!("block {i}");
            q.must_write_block(block.as_bytes());
            blocks.push(block);
            q.must_close();
            q = must_open_internal(&path, "foobar", CHUNK_FILE_SIZE, BLOCK_SIZE_MAX, 0);
        }
        assert_ne!(
            q.get_pending_bytes(),
            0,
            "unexpected zero number of bytes pending"
        );
        for block in &blocks {
            let mut data = Vec::new();
            let ok = q.must_read_block_nonblocking(&mut data);
            assert!(ok, "unexpected ok=false");
            assert_eq!(
                block.as_bytes(),
                &data[..],
                "unexpected block read; got {data:?}; want {block:?}"
            );
            q.must_close();
            q = must_open_internal(&path, "foobar", CHUNK_FILE_SIZE, BLOCK_SIZE_MAX, 0);
        }
        let n = q.get_pending_bytes();
        assert_eq!(n, 0, "unexpected non-zero number of pending bytes: {n}");
        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_queue_limited_size() {
        const MAX_PENDING_BYTES: i64 = 1000;
        let path = test_dir("queue-limited-size");
        vlfs::must_remove_dir(&path);
        let mut q = must_open(&path, "foobar", MAX_PENDING_BYTES);

        // Check that small blocks are successfully buffered and read
        let mut blocks = Vec::new();
        for i in 0..10 {
            let block = format!("block_{i}");
            q.must_write_block(block.as_bytes());
            blocks.push(block);
        }
        let mut buf = Vec::new();
        for block in &blocks {
            buf.clear();
            let ok = q.must_read_block_nonblocking(&mut buf);
            assert!(ok, "unexpected ok=false");
            assert_eq!(
                block.as_bytes(),
                &buf[..],
                "unexpected block read; got {buf:?}; want {block:?}"
            );
        }

        // Make sure that old blocks are dropped on queue size overflow
        for i in 0..MAX_PENDING_BYTES {
            let block = format!("{i}");
            q.must_write_block(block.as_bytes());
        }
        let n = q.get_pending_bytes();
        assert!(
            n <= MAX_PENDING_BYTES as u64,
            "too many pending bytes; got {n}; mustn't exceed {MAX_PENDING_BYTES}"
        );
        buf.clear();
        let ok = q.must_read_block_nonblocking(&mut buf);
        assert!(ok, "unexpected ok=false");
        let block_num: i64 = std::str::from_utf8(&buf)
            .unwrap()
            .parse()
            .unwrap_or_else(|err| panic!("cannot parse block contents: {err}"));
        assert!(
            block_num >= 20,
            "too small block number: {block_num}; it looks like it wasn't dropped"
        );

        // Try writing a block with too big size
        let block = vec![0u8; MAX_PENDING_BYTES as usize + 1];
        q.must_write_block(&block);
        let n = q.get_pending_bytes();
        assert_eq!(
            n, 0,
            "unexpected non-empty queue after writing a block with too big size; queue size: {n} bytes"
        );

        q.must_close();
        vlfs::must_remove_dir(&path);
    }

    // -----------------------------------------------------------------------
    // fastqueue_test.go
    // -----------------------------------------------------------------------

    #[test]
    fn test_fast_queue_open_close() {
        let path = test_dir("fast-queue-open-close");
        vlfs::must_remove_dir(&path);
        for _ in 0..10 {
            let fq = FastQueue::must_open_fast_queue(&path, "foobar", 100, 0, false);
            fq.must_close();
        }
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_fast_queue_write_read_inmemory() {
        let path = test_dir("fast-queue-write-read-inmemory");
        vlfs::must_remove_dir(&path);

        let capacity = 100;
        let fq = FastQueue::must_open_fast_queue(&path, "foobar", capacity, 0, false);
        let n = fq.get_inmemory_queue_len();
        assert_eq!(n, 0, "unexpected non-zero inmemory queue size: {n}");
        let mut blocks = Vec::new();
        for i in 0..capacity {
            let block = format!("block {i}");
            assert!(
                fq.try_write_block(block.as_bytes()),
                "try_write_block must return true in this context"
            );
            blocks.push(block);
        }
        let n = fq.get_inmemory_queue_len();
        assert_eq!(
            n, capacity,
            "unexpected size of inmemory queue; got {n}; want {capacity}"
        );
        for block in &blocks {
            let mut buf = Vec::new();
            let ok = fq.must_read_block(&mut buf);
            assert!(ok, "unexpected ok=false");
            assert_eq!(
                block.as_bytes(),
                &buf[..],
                "unexpected block read; got {buf:?}; want {block:?}"
            );
        }
        fq.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_fast_queue_write_read_mixed() {
        let path = test_dir("fast-queue-write-read-mixed");
        vlfs::must_remove_dir(&path);

        let capacity = 100;
        let fq = FastQueue::must_open_fast_queue(&path, "foobar", capacity, 0, false);
        let n = fq.get_pending_bytes();
        assert_eq!(n, 0, "the number of pending bytes must be 0; got {n}");
        let mut blocks = Vec::new();
        for i in 0..2 * capacity {
            let block = format!("block {i}");
            assert!(
                fq.try_write_block(block.as_bytes()),
                "try_write_block must return true in this context"
            );
            blocks.push(block);
        }
        assert_ne!(
            fq.get_pending_bytes(),
            0,
            "the number of pending bytes must be greater than 0"
        );
        for block in &blocks {
            let mut buf = Vec::new();
            let ok = fq.must_read_block(&mut buf);
            assert!(ok, "unexpected ok=false");
            assert_eq!(
                block.as_bytes(),
                &buf[..],
                "unexpected block read; got {buf:?}; want {block:?}"
            );
        }
        let n = fq.get_pending_bytes();
        assert_eq!(n, 0, "the number of pending bytes must be 0; got {n}");
        fq.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_fast_queue_write_read_with_closes() {
        let path = test_dir("fast-queue-write-read-with-closes");
        vlfs::must_remove_dir(&path);

        let capacity = 100;
        let mut fq = FastQueue::must_open_fast_queue(&path, "foobar", capacity, 0, false);
        let n = fq.get_pending_bytes();
        assert_eq!(n, 0, "the number of pending bytes must be 0; got {n}");
        let mut blocks = Vec::new();
        for i in 0..2 * capacity {
            let block = format!("block {i}");
            assert!(
                fq.try_write_block(block.as_bytes()),
                "try_write_block must return true in this context"
            );
            blocks.push(block);
            fq.must_close();
            fq = FastQueue::must_open_fast_queue(&path, "foobar", capacity, 0, false);
        }
        assert_ne!(
            fq.get_pending_bytes(),
            0,
            "the number of pending bytes must be greater than 0"
        );
        for block in &blocks {
            let mut buf = Vec::new();
            let ok = fq.must_read_block(&mut buf);
            assert!(ok, "unexpected ok=false");
            assert_eq!(
                block.as_bytes(),
                &buf[..],
                "unexpected block read; got {buf:?}; want {block:?}"
            );
            fq.must_close();
            fq = FastQueue::must_open_fast_queue(&path, "foobar", capacity, 0, false);
        }
        let n = fq.get_pending_bytes();
        assert_eq!(n, 0, "the number of pending bytes must be 0; got {n}");
        fq.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_fast_queue_read_unblock_by_close() {
        let path = test_dir("fast-queue-read-unblock-by-close");
        vlfs::must_remove_dir(&path);

        let fq = Arc::new(FastQueue::must_open_fast_queue(
            &path, "foorbar", 123, 0, false,
        ));
        let (result_tx, result_rx) = std::sync::mpsc::channel::<Result<(), String>>();
        {
            let fq = Arc::clone(&fq);
            std::thread::spawn(move || {
                let mut data = Vec::new();
                let ok = fq.must_read_block(&mut data);
                if ok {
                    let _ = result_tx.send(Err("unexpected ok=true".to_string()));
                    return;
                }
                if !data.is_empty() {
                    let _ = result_tx.send(Err(format!("unexpected non-empty data={data:?}")));
                    return;
                }
                let _ = result_tx.send(Ok(()));
            });
        }
        fq.must_close();
        match result_rx.recv_timeout(Duration::from_secs(1)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("unexpected error: {err}"),
            Err(_) => panic!("timeout"),
        }
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_fast_queue_read_unblock_by_write() {
        let path = test_dir("fast-queue-read-unblock-by-write");
        vlfs::must_remove_dir(&path);

        let fq = Arc::new(FastQueue::must_open_fast_queue(
            &path, "foobar", 13, 0, false,
        ));
        let block = "foodsafdsaf sdf";
        let (result_tx, result_rx) = std::sync::mpsc::channel::<Result<(), String>>();
        {
            let fq = Arc::clone(&fq);
            std::thread::spawn(move || {
                let mut data = Vec::new();
                let ok = fq.must_read_block(&mut data);
                if !ok {
                    let _ = result_tx.send(Err("unexpected ok=false".to_string()));
                    return;
                }
                if data != block.as_bytes() {
                    let _ = result_tx.send(Err(format!(
                        "unexpected block read; got {data:?}; want {block:?}"
                    )));
                    return;
                }
                let _ = result_tx.send(Ok(()));
            });
        }
        assert!(
            fq.try_write_block(block.as_bytes()),
            "try_write_block must return true in this context"
        );
        match result_rx.recv_timeout(Duration::from_secs(1)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("unexpected error: {err}"),
            Err(_) => panic!("timeout"),
        }
        fq.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_fast_queue_read_write_concurrent() {
        let path = test_dir("fast-queue-read-write-concurrent");
        vlfs::must_remove_dir(&path);

        let fq = Arc::new(FastQueue::must_open_fast_queue(
            &path, "foobar", 5, 0, false,
        ));

        let mut blocks = Vec::new();
        let blocks_map: Arc<Mutex<std::collections::HashSet<String>>> =
            Arc::new(Mutex::new(std::collections::HashSet::new()));
        for i in 0..1000 {
            let block = format!("block {i}");
            blocks.push(block.clone());
            blocks_map.lock().unwrap().insert(block);
        }

        // Start readers
        let mut readers = Vec::new();
        for _ in 0..10 {
            let fq = Arc::clone(&fq);
            let blocks_map = Arc::clone(&blocks_map);
            readers.push(std::thread::spawn(move || {
                loop {
                    let mut data = Vec::new();
                    if !fq.must_read_block(&mut data) {
                        return;
                    }
                    let s = String::from_utf8(data).unwrap();
                    let mut m = blocks_map.lock().unwrap();
                    assert!(m.remove(&s), "unexpected data read from the queue: {s:?}");
                }
            }));
        }

        // Start writers
        let (blocks_tx, blocks_rx) = std::sync::mpsc::channel::<String>();
        let blocks_rx = Arc::new(Mutex::new(blocks_rx));
        let mut writers = Vec::new();
        for _ in 0..10 {
            let fq = Arc::clone(&fq);
            let blocks_rx = Arc::clone(&blocks_rx);
            writers.push(std::thread::spawn(move || {
                loop {
                    let block = match blocks_rx.lock().unwrap().recv() {
                        Ok(b) => b,
                        Err(_) => return,
                    };
                    assert!(
                        fq.try_write_block(block.as_bytes()),
                        "try_write_block must return true in this context"
                    );
                }
            }));
        }

        // feed writers
        for block in &blocks {
            blocks_tx.send(block.clone()).unwrap();
        }
        drop(blocks_tx);

        // Wait for writers to finish
        for w in writers {
            w.join().unwrap();
        }

        // wait for a while, so readers could catch up
        std::thread::sleep(Duration::from_millis(100));

        // Close fq
        fq.must_close();

        // Wait for readers to finish
        for r in readers {
            r.join().unwrap();
        }

        // Collect the remaining data
        let fq = Arc::new(FastQueue::must_open_fast_queue(
            &path, "foobar", 5, 0, false,
        ));
        let (result_tx, result_rx) = std::sync::mpsc::channel::<Result<(), String>>();
        {
            let fq = Arc::clone(&fq);
            let blocks_map = Arc::clone(&blocks_map);
            std::thread::spawn(move || {
                while !blocks_map.lock().unwrap().is_empty() {
                    let mut data = Vec::new();
                    if !fq.must_read_block(&mut data) {
                        let _ = result_tx.send(Err("unexpected ok=false".to_string()));
                        return;
                    }
                    let s = String::from_utf8(data).unwrap();
                    if !blocks_map.lock().unwrap().remove(&s) {
                        let _ = result_tx.send(Err(format!("unexpected data read from fq: {s:?}")));
                        return;
                    }
                }
                let _ = result_tx.send(Ok(()));
            });
        }
        match result_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("unexpected error: {err}"),
            Err(_) => panic!("timeout"),
        }
        fq.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_fast_queue_write_read_with_disabled_pq() {
        let path = test_dir("fast-queue-write-read-inmemory-disabled-pq");
        vlfs::must_remove_dir(&path);

        let capacity = 20;
        let fq = FastQueue::must_open_fast_queue(&path, "foobar", capacity, 0, true);
        let n = fq.get_inmemory_queue_len();
        assert_eq!(n, 0, "unexpected non-zero inmemory queue size: {n}");
        let mut blocks = Vec::new();
        for i in 0..capacity {
            let block = format!("block {i}");
            assert!(
                fq.try_write_block(block.as_bytes()),
                "try_write_block must return true in this context"
            );
            blocks.push(block);
        }
        assert!(
            !fq.try_write_block(b"error-block"),
            "expect false due to full queue"
        );

        fq.must_close();
        let fq = FastQueue::must_open_fast_queue(&path, "foobar", capacity, 0, true);
        for block in &blocks {
            let mut buf = Vec::new();
            let ok = fq.must_read_block(&mut buf);
            assert!(ok, "unexpected ok=false");
            assert_eq!(
                block.as_bytes(),
                &buf[..],
                "unexpected block read; got {buf:?}; want {block:?}"
            );
        }
        fq.must_close();
        vlfs::must_remove_dir(&path);
    }

    #[test]
    fn test_fast_queue_write_read_with_ignore_disabled_pq() {
        let path = test_dir("fast-queue-write-read-inmemory-disabled-pq-force-write");
        vlfs::must_remove_dir(&path);

        let capacity = 20;
        let fq = FastQueue::must_open_fast_queue(&path, "foobar", capacity, 0, true);
        let n = fq.get_inmemory_queue_len();
        assert_eq!(n, 0, "unexpected non-zero inmemory queue size: {n}");
        let mut blocks = Vec::new();
        for i in 0..capacity {
            let block = format!("block {i}");
            assert!(
                fq.try_write_block(block.as_bytes()),
                "try_write_block must return true in this context"
            );
            blocks.push(block);
        }
        assert!(
            !fq.try_write_block(b"error-block"),
            "expect false due to full queue"
        );
        for i in 0..capacity {
            let block = format!("block {i}-{i}");
            fq.must_write_block_ignore_disabled_pq(block.as_bytes());
            blocks.push(block);
        }

        fq.must_close();
        let fq = FastQueue::must_open_fast_queue(&path, "foobar", capacity, 0, true);
        for block in &blocks {
            let mut buf = Vec::new();
            let ok = fq.must_read_block(&mut buf);
            assert!(ok, "unexpected ok=false");
            assert_eq!(
                block.as_bytes(),
                &buf[..],
                "unexpected block read; got {buf:?}; want {block:?}"
            );
        }
        fq.must_close();
        vlfs::must_remove_dir(&path);
    }

    // PORT NOTE: extra port-only coverage for the hand-rolled metainfo JSON
    // codec (Go relies on encoding/json, which needs no tests there).
    #[test]
    fn test_metainfo_json_roundtrip() {
        let dir = test_dir("metainfo-json-roundtrip");
        must_create_dir(&dir);
        let path = dir.join(METAINFO_FILENAME);
        let mi = Metainfo {
            name: "1:secret-url with \"quotes\" & \\slashes\\".to_string(),
            reader_offset: 123,
            writer_offset: 456,
        };
        mi.write_to_file(&path);
        let mut got = Metainfo::default();
        assert!(got.read_from_file(&path).is_ok());
        assert_eq!(got, mi);
        vlfs::must_remove_dir(&dir);
    }
}

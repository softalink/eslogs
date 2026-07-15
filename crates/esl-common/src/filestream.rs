//! Port of Softalink LLC `lib/filestream` (filestream.go, parallel.go and
//! the filestream_linux.go / filestream_windows.go stream trackers).
//!
//! PORT NOTE: the `vm_filestream_*` series are exported as
//! `esm_filestream_*`. Go counts "real" reads/writes via statReader/
//! statWriter shims around the underlying `os.File`; the port counts at the
//! equivalent underlying-file call sites. One difference: Rust `write_all`
//! retries short writes inside a single counted "real" write call, where Go
//! would count each `File.Write` — byte totals match either way.
//!
//! PORT NOTE: Go registers the metrics at package init; the port registers
//! them from `appmetrics::init_start_time` (or lazily on first file use).
//!
//! PORT NOTE: Go pools `bufio.Reader`/`bufio.Writer` objects via sync.Pool;
//! the Rust port pools the raw buffers behind [`Reader`]/[`Writer`], which
//! preserves the buffer-reuse behavior.

use std::fs::File;
use std::io::{Read as _, Seek, SeekFrom, Write as _};
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::Instant;

use crate::flagutil::Flag;
use crate::fs::fsutil;
use crate::metrics::{Counter, FloatCounter};
use crate::panicf;

static DISABLE_FADVISE: Flag<bool> = Flag::new(
    "filestream.disableFadvise",
    "Whether to disable fadvise() syscall when reading large data files. \
The fadvise() syscall prevents from eviction of recently accessed data from OS page cache during background merges and backups. \
In some rare cases it is better to disable the syscall if it uses too much CPU",
    || false,
);
crate::register_flag!(DISABLE_FADVISE);

// Only the Linux stream tracker consumes this in non-test builds.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const DONT_NEED_BLOCK_SIZE: u64 = 16 * 1024 * 1024;

/// ReadCloser is a standard interface for filestream Reader.
pub trait ReadCloser {
    fn path(&self) -> &str;
    fn read(&mut self, p: &mut [u8]) -> std::io::Result<usize>;
    fn must_close(&mut self);
}

/// WriteCloser is a standard interface for filestream Writer.
pub trait WriteCloser {
    fn path(&self) -> &str;
    fn write(&mut self, p: &[u8]) -> std::io::Result<usize>;
    fn must_close(&mut self);
}

fn get_read_buffer_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| (memory_allowed() / 1024 / 64).clamp(4 * 1024, 64 * 1024))
}

fn get_write_buffer_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| (memory_allowed() / 1024 / 8).clamp(4 * 1024, 128 * 1024))
}

fn memory_allowed() -> usize {
    crate::memory::allowed()
}

// The `vm_filestream_*` package-level metric vars from filestream.go,
// rebranded `esm_filestream_*` and grouped in one lazily-registered struct.
struct FilestreamMetrics {
    read_duration: Arc<FloatCounter>,
    read_calls_buffered: Arc<Counter>,
    read_calls_real: Arc<Counter>,
    read_bytes_buffered: Arc<Counter>,
    read_bytes_real: Arc<Counter>,
    readers_count: Arc<Counter>,

    write_duration: Arc<FloatCounter>,
    write_calls_buffered: Arc<Counter>,
    write_calls_real: Arc<Counter>,
    written_bytes_buffered: Arc<Counter>,
    written_bytes_real: Arc<Counter>,
    writers_count: Arc<Counter>,

    fsync_duration: Arc<FloatCounter>,
    fsync_calls: Arc<Counter>,
}

static METRICS: LazyLock<FilestreamMetrics> = LazyLock::new(|| FilestreamMetrics {
    read_duration: crate::metrics::new_float_counter("esm_filestream_read_duration_seconds_total"),
    read_calls_buffered: crate::metrics::new_counter("esm_filestream_buffered_read_calls_total"),
    read_calls_real: crate::metrics::new_counter("esm_filestream_real_read_calls_total"),
    read_bytes_buffered: crate::metrics::new_counter("esm_filestream_buffered_read_bytes_total"),
    read_bytes_real: crate::metrics::new_counter("esm_filestream_real_read_bytes_total"),
    readers_count: crate::metrics::new_counter("esm_filestream_readers"),

    write_duration: crate::metrics::new_float_counter(
        "esm_filestream_write_duration_seconds_total",
    ),
    write_calls_buffered: crate::metrics::new_counter("esm_filestream_buffered_write_calls_total"),
    write_calls_real: crate::metrics::new_counter("esm_filestream_real_write_calls_total"),
    written_bytes_buffered: crate::metrics::new_counter(
        "esm_filestream_buffered_written_bytes_total",
    ),
    written_bytes_real: crate::metrics::new_counter("esm_filestream_real_written_bytes_total"),
    writers_count: crate::metrics::new_counter("esm_filestream_writers"),

    fsync_duration: crate::metrics::new_float_counter(
        "esm_filestream_fsync_duration_seconds_total",
    ),
    fsync_calls: crate::metrics::new_counter("esm_filestream_fsync_calls_total"),
});

fn fs_metrics() -> &'static FilestreamMetrics {
    &METRICS
}

/// Registers the `esm_filestream_*` series in the default metrics set (Go
/// registers them at package init).
pub(crate) fn register_metrics() {
    LazyLock::force(&METRICS);
}

// statReader.Read equivalent: one timed, counted read from the underlying file.
fn read_real(f: &mut File, p: &mut [u8]) -> std::io::Result<usize> {
    let m = fs_metrics();
    let start_time = Instant::now();
    m.read_calls_real.inc();
    let res = f.read(p);
    m.read_duration.add(start_time.elapsed().as_secs_f64());
    if let Ok(n) = res {
        m.read_bytes_real.add(n as u64);
    }
    res
}

// statWriter.Write equivalent: one timed, counted write to the underlying file.
fn write_real(f: &mut File, data: &[u8]) -> std::io::Result<()> {
    let m = fs_metrics();
    let start_time = Instant::now();
    m.write_calls_real.inc();
    let res = f.write_all(data);
    m.write_duration.add(start_time.elapsed().as_secs_f64());
    if res.is_ok() {
        m.written_bytes_real.add(data.len() as u64);
    }
    res
}

static READ_BUF_POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());
static WRITE_BUF_POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

fn get_read_buf() -> Vec<u8> {
    READ_BUF_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_else(|| vec![0u8; get_read_buffer_size()])
}

fn put_read_buf(buf: Vec<u8>) {
    if !buf.is_empty() {
        READ_BUF_POOL.lock().unwrap().push(buf);
    }
}

fn get_write_buf() -> Vec<u8> {
    WRITE_BUF_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_else(|| Vec::with_capacity(get_write_buffer_size()))
}

fn put_write_buf(mut buf: Vec<u8>) {
    if buf.capacity() > 0 {
        buf.clear();
        WRITE_BUF_POOL.lock().unwrap().push(buf);
    }
}

/// Reader implements buffered file reader.
pub struct Reader {
    f: Option<File>,
    path: String,
    // buf always has len() == read buffer size; buf[pos..filled] holds
    // the buffered data.
    buf: Vec<u8>,
    pos: usize,
    filled: usize,
    st: StreamTracker,
}

impl Reader {
    /// Returns the path to the reader file.
    pub fn path(&self) -> &str {
        &self.path
    }

    fn file(&mut self) -> &mut File {
        self.f
            .as_mut()
            .expect("BUG: Reader is used after MustClose")
    }

    /// Closes the underlying file passed to must_open.
    pub fn must_close(&mut self) {
        if let Err(err) = self.st.close() {
            panicf!(
                "FATAL: cannot close streamTracker for file {:?}: {err}",
                self.path
            );
        }
        // PORT NOTE: Go panics when f.Close() fails; Rust closes the file on
        // drop without error reporting.
        self.f = None;

        put_read_buf(std::mem::take(&mut self.buf));
        self.pos = 0;
        self.filled = 0;

        fs_metrics().readers_count.dec();
    }

    // bufio.Reader.Read semantics: at most one read from the underlying file.
    fn read_buffered(&mut self, p: &mut [u8]) -> std::io::Result<usize> {
        if p.is_empty() {
            return Ok(0);
        }
        if self.pos == self.filled {
            if p.len() >= self.buf.len() {
                // Large read: bypass the buffer.
                return read_real(self.file(), p);
            }
            let n = {
                let (f, buf) = (
                    self.f
                        .as_mut()
                        .expect("BUG: Reader is used after MustClose"),
                    &mut self.buf,
                );
                read_real(f, buf)?
            };
            self.pos = 0;
            self.filled = n;
            if n == 0 {
                return Ok(0);
            }
        }
        let n = (self.filled - self.pos).min(p.len());
        p[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl ReadCloser for Reader {
    fn path(&self) -> &str {
        Reader::path(self)
    }

    /// Reads file contents to p.
    fn read(&mut self, p: &mut [u8]) -> std::io::Result<usize> {
        fs_metrics().read_calls_buffered.inc();
        let n = self.read_buffered(p)?;
        fs_metrics().read_bytes_buffered.add(n as u64);
        if let Err(err) = self.st.advise_dont_need(n, false) {
            return Err(std::io::Error::other(format!(
                "advise error for {:?}: {err}",
                self.path
            )));
        }
        Ok(n)
    }

    fn must_close(&mut self) {
        Reader::must_close(self)
    }
}

impl std::io::Read for Reader {
    fn read(&mut self, p: &mut [u8]) -> std::io::Result<usize> {
        ReadCloser::read(self, p)
    }
}

impl crate::fs::MustCloser for Reader {
    fn must_close(&mut self) {
        Reader::must_close(self)
    }
}

/// Opens the file at the given path in nocache mode at the given offset.
///
/// If nocache is set, then the reader doesn't pollute OS page cache.
///
/// PORT NOTE: offset is u64 (Go uses int64; negative offsets fail there).
pub fn open_reader_at(
    path: impl AsRef<Path>,
    offset: u64,
    nocache: bool,
) -> Result<Reader, String> {
    let path = path.as_ref();
    let mut r = must_open(path, nocache);
    match r.file().seek(SeekFrom::Start(offset)) {
        Err(err) => {
            r.must_close();
            Err(format!(
                "cannot seek to offset={offset} for {path:?}: {err}"
            ))
        }
        Ok(n) if n != offset => {
            r.must_close();
            Err(format!(
                "invalid seek offset for {path:?}; got {n}; want {offset}"
            ))
        }
        Ok(_) => Ok(r),
    }
}

/// Opens the file from the given path in nocache mode.
///
/// If nocache is set, then the reader doesn't pollute OS page cache.
pub fn must_open(path: impl AsRef<Path>, nocache: bool) -> Reader {
    let path = path.as_ref();
    let f = match File::open(path) {
        Ok(f) => f,
        Err(err) => {
            panicf!("FATAL: cannot open file: open {}: {err}", path.display());
            unreachable!()
        }
    };
    let mut nocache = nocache;
    if *DISABLE_FADVISE.get() {
        // Unconditionally disable fadvise() syscall
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/pull/5120 for details on why this is needed
        nocache = false;
    }
    let mut st = StreamTracker::default();
    if nocache {
        st.set_fd(&f);
    }
    fs_metrics().readers_count.inc();
    Reader {
        f: Some(f),
        path: path.to_string_lossy().into_owned(),
        buf: get_read_buf(),
        pos: 0,
        filled: 0,
        st,
    }
}

/// Writer implements buffered file writer.
pub struct Writer {
    f: Option<File>,
    path: String,
    buf: Vec<u8>,
    st: StreamTracker,
}

impl Writer {
    /// Returns the path to the writer file.
    pub fn path(&self) -> &str {
        &self.path
    }

    fn file(&mut self) -> &mut File {
        self.f
            .as_mut()
            .expect("BUG: Writer is used after MustClose")
    }

    /// Syncs the underlying file to storage and then closes it.
    pub fn must_close(&mut self) {
        self.flush();

        put_write_buf(std::mem::take(&mut self.buf));

        self.sync();
        if let Err(err) = self.st.close() {
            panicf!(
                "FATAL: cannot close streamTracker for file {:?}: {err}",
                self.path
            );
        }
        // PORT NOTE: Go panics when f.Close() fails; Rust closes the file on
        // drop without error reporting.
        self.f = None;

        fs_metrics().writers_count.dec();
    }

    fn flush(&mut self) {
        if let Err(err) = self.flush_buf() {
            panicf!(
                "FATAL: cannot flush buffered data to file {:?}: {err}",
                self.path
            );
        }
    }

    fn flush_buf(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            let (f, buf) = (
                self.f
                    .as_mut()
                    .expect("BUG: Writer is used after MustClose"),
                &self.buf,
            );
            write_real(f, buf)?;
            self.buf.clear();
        }
        Ok(())
    }

    fn sync(&mut self) {
        if !fsutil::is_fsync_disabled() {
            let start_time = Instant::now();
            if let Err(err) = self.file().sync_all() {
                panicf!("FATAL: cannot sync file {:?}: {err}", self.path);
            }
            let m = fs_metrics();
            m.fsync_duration.add(start_time.elapsed().as_secs_f64());
            m.fsync_calls.inc();
        }
    }

    // bufio.Writer.Write semantics: buffer small writes, flush on full buffer,
    // bypass the buffer for large writes when the buffer is empty.
    fn write_buffered(&mut self, p: &[u8]) -> std::io::Result<usize> {
        let cap = self.buf.capacity();
        let mut rest = p;
        while !rest.is_empty() {
            if self.buf.is_empty() && rest.len() >= cap {
                let f = self
                    .f
                    .as_mut()
                    .expect("BUG: Writer is used after MustClose");
                write_real(f, rest)?;
                break;
            }
            let avail = cap - self.buf.len();
            let n = avail.min(rest.len());
            self.buf.extend_from_slice(&rest[..n]);
            rest = &rest[n..];
            if self.buf.len() == cap {
                self.flush_buf()?;
            }
        }
        Ok(p.len())
    }

    /// Flushes all the buffered data to file.
    ///
    /// If is_sync is true, then the flushed data is fsynced to the underlying storage.
    pub fn must_flush(&mut self, is_sync: bool) {
        self.flush();
        if is_sync {
            self.sync();
        }
    }
}

impl WriteCloser for Writer {
    fn path(&self) -> &str {
        Writer::path(self)
    }

    /// Writes p to the underlying file.
    fn write(&mut self, p: &[u8]) -> std::io::Result<usize> {
        fs_metrics().write_calls_buffered.inc();
        let n = self.write_buffered(p)?;
        fs_metrics().written_bytes_buffered.add(n as u64);
        if let Err(err) = self.st.advise_dont_need(n, true) {
            return Err(std::io::Error::other(format!(
                "advise error for {:?}: {err}",
                self.path
            )));
        }
        Ok(n)
    }

    fn must_close(&mut self) {
        Writer::must_close(self)
    }
}

impl std::io::Write for Writer {
    fn write(&mut self, p: &[u8]) -> std::io::Result<usize> {
        WriteCloser::write(self, p)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_buf()
    }
}

impl crate::fs::MustCloser for Writer {
    fn must_close(&mut self) {
        Writer::must_close(self)
    }
}

/// Opens the file at path in nocache mode for writing at the given offset.
///
/// The file at path is created if it is missing.
///
/// If nocache is set, the writer doesn't pollute OS page cache.
///
/// PORT NOTE: offset is u64 (Go uses int64; negative offsets fail there).
pub fn open_writer_at(
    path: impl AsRef<Path>,
    offset: u64,
    nocache: bool,
) -> Result<Writer, String> {
    let path = path.as_ref();
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut f = options
        .open(path)
        .map_err(|err| format!("open {}: {err}", path.display()))?;
    match f.seek(SeekFrom::Start(offset)) {
        Err(err) => Err(format!("cannot seek to offset={offset} in {path:?}: {err}")),
        Ok(n) if n != offset => Err(format!(
            "invalid seek offset for {path:?}; got {n}; want {offset}"
        )),
        Ok(_) => Ok(new_writer(f, path, nocache)),
    }
}

/// Creates the file for the given path in nocache mode.
///
/// If nocache is set, the writer doesn't pollute OS page cache.
pub fn must_create(path: impl AsRef<Path>, nocache: bool) -> Writer {
    let path = path.as_ref();
    let f = match File::create(path) {
        Ok(f) => f,
        Err(err) => {
            panicf!("FATAL: cannot create file {path:?}: {err}");
            unreachable!()
        }
    };
    new_writer(f, path, nocache)
}

fn new_writer(f: File, path: &Path, nocache: bool) -> Writer {
    let mut st = StreamTracker::default();
    if nocache {
        st.set_fd(&f);
    }
    fs_metrics().writers_count.inc();
    Writer {
        f: Some(f),
        path: path.to_string_lossy().into_owned(),
        buf: get_write_buf(),
        st,
    }
}

#[cfg(unix)]
type TrackedFd = std::os::fd::RawFd;
// PORT NOTE: the raw HANDLE is stored as isize so the tracker stays Send.
#[cfg(windows)]
type TrackedFd = isize;

#[derive(Default)]
struct StreamTracker {
    fd: Option<TrackedFd>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    offset: u64,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    length: u64,
}

impl StreamTracker {
    #[cfg(unix)]
    fn set_fd(&mut self, f: &File) {
        self.fd = Some(std::os::fd::AsRawFd::as_raw_fd(f));
    }

    #[cfg(windows)]
    fn set_fd(&mut self, f: &File) {
        self.fd = Some(std::os::windows::io::AsRawHandle::as_raw_handle(f) as isize);
    }
}

#[cfg(target_os = "linux")]
impl StreamTracker {
    fn advise_dont_need(&mut self, n: usize, fdatasync: bool) -> Result<(), String> {
        self.length += n as u64;
        let Some(fd) = self.fd else {
            return Ok(());
        };
        if self.length < DONT_NEED_BLOCK_SIZE {
            return Ok(());
        }
        let block_size = self.length - (self.length % DONT_NEED_BLOCK_SIZE);
        if fdatasync {
            let rc = unsafe { libc::fdatasync(fd) };
            if rc != 0 {
                return Err(format!(
                    "unix.Fdatasync error: {}",
                    std::io::Error::last_os_error()
                ));
            }
        }
        let rc = unsafe {
            libc::posix_fadvise(
                fd,
                self.offset as i64,
                block_size as i64,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        if rc != 0 {
            return Err(format!(
                "unix.Fadvise(FADV_DONTNEEDED, {}, {}) error: {}",
                self.offset,
                block_size,
                std::io::Error::from_raw_os_error(rc)
            ));
        }
        self.offset += block_size;
        self.length -= block_size;
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        let Some(fd) = self.fd else {
            return Ok(());
        };
        // Advise the whole file as it shouldn't be cached.
        let rc = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
        if rc != 0 {
            return Err(format!(
                "unix.Fadvise(FADV_DONTNEEDED, 0, 0) error: {}",
                std::io::Error::from_raw_os_error(rc)
            ));
        }
        Ok(())
    }
}

// PORT NOTE: only Linux and Windows are supported targets; the
// darwin/BSD/solaris fadvise variants are not ported and these are no-ops.
#[cfg(all(unix, not(target_os = "linux")))]
impl StreamTracker {
    fn advise_dont_need(&mut self, n: usize, _fdatasync: bool) -> Result<(), String> {
        self.length += n as u64;
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(windows)]
impl StreamTracker {
    fn advise_dont_need(&mut self, _n: usize, fdatasync: bool) -> Result<(), String> {
        if fdatasync && let Some(fd) = self.fd {
            let rc = unsafe {
                windows_sys::Win32::Storage::FileSystem::FlushFileBuffers(
                    fd as *mut core::ffi::c_void,
                )
            };
            if rc == 0 {
                return Err(format!(
                    "windows.Fsync error: {}",
                    std::io::Error::last_os_error()
                ));
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// ParallelFileCreator is used for parallel creating of files.
///
/// ParallelFileCreator is needed for speeding up creating many files on high-latency
/// storage systems such as NFS or Ceph.
///
/// PORT NOTE: Go writes the results through caller-provided pointers; the
/// Rust port stores `&mut` output slots, which are filled on run().
#[derive(Default)]
pub struct ParallelFileCreator<'a> {
    tasks: Vec<ParallelFileCreatorTask<'a>>,
}

struct ParallelFileCreatorTask<'a> {
    dst_path: std::path::PathBuf,
    wc: &'a mut Option<Writer>,
    nocache: bool,
}

impl<'a> ParallelFileCreator<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a task for creating the file at dst_path and assigning it to `*wc`.
    ///
    /// Tasks are executed in parallel on run() call.
    pub fn add(
        &mut self,
        dst_path: impl Into<std::path::PathBuf>,
        wc: &'a mut Option<Writer>,
        nocache: bool,
    ) {
        self.tasks.push(ParallelFileCreatorTask {
            dst_path: dst_path.into(),
            wc,
            nocache,
        });
    }

    /// Runs all the registered tasks for creating files in parallel.
    pub fn run(self) {
        let concurrency_ch = fsutil::get_concurrency_ch();
        std::thread::scope(|s| {
            for task in self.tasks {
                let permit = concurrency_ch.acquire();
                s.spawn(move || {
                    *task.wc = Some(must_create(&task.dst_path, task.nocache));
                    drop(permit);
                });
            }
        });
    }
}

/// ParallelFileOpener is used for parallel opening of files.
///
/// ParallelFileOpener is needed for speeding up opening many files on high-latency
/// storage systems such as NFS or Ceph.
///
/// PORT NOTE: Go writes the results through caller-provided pointers; the
/// Rust port stores `&mut` output slots, which are filled on run().
#[derive(Default)]
pub struct ParallelFileOpener<'a> {
    tasks: Vec<ParallelFileOpenerTask<'a>>,
}

struct ParallelFileOpenerTask<'a> {
    path: std::path::PathBuf,
    rc: &'a mut Option<Reader>,
    nocache: bool,
}

impl<'a> ParallelFileOpener<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a task for opening the file at the given path and assigning it to `*rc`.
    ///
    /// Tasks are executed in parallel on run() call.
    pub fn add(
        &mut self,
        path: impl Into<std::path::PathBuf>,
        rc: &'a mut Option<Reader>,
        nocache: bool,
    ) {
        self.tasks.push(ParallelFileOpenerTask {
            path: path.into(),
            rc,
            nocache,
        });
    }

    /// Runs all the registered tasks for opening files in parallel.
    pub fn run(self) {
        let concurrency_ch = fsutil::get_concurrency_ch();
        std::thread::scope(|s| {
            for task in self.tasks {
                let permit = concurrency_ch.acquire();
                s.spawn(move || {
                    *task.rc = Some(must_open(&task.path, task.nocache));
                    drop(permit);
                });
            }
        });
    }
}

/// ParallelStreamWriter is used for parallel writing of data to the given dst_path files.
///
/// ParallelStreamWriter is needed for speeding up writing data to many files on high-latency
/// storage systems such as NFS or Ceph.
///
/// PORT NOTE: Go accepts an io.WriterTo; the Rust port accepts a closure that
/// writes its data to the given Writer and returns the number of written bytes.
#[derive(Default)]
pub struct ParallelStreamWriter<'a> {
    tasks: Vec<ParallelStreamWriterTask<'a>>,
}

type WriterToFn<'a> = Box<dyn FnOnce(&mut Writer) -> std::io::Result<u64> + Send + 'a>;

struct ParallelStreamWriterTask<'a> {
    dst_path: std::path::PathBuf,
    src: WriterToFn<'a>,
}

impl<'a> ParallelStreamWriter<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a task to execute in parallel - to write the data from src to the dst_path.
    ///
    /// Tasks are executed in parallel on run() call.
    pub fn add(&mut self, dst_path: impl Into<std::path::PathBuf>, src: WriterToFn<'a>) {
        self.tasks.push(ParallelStreamWriterTask {
            dst_path: dst_path.into(),
            src,
        });
    }

    /// Executes all the tasks added via add() call in parallel.
    pub fn run(self) {
        let concurrency_ch = fsutil::get_concurrency_ch();
        std::thread::scope(|s| {
            for task in self.tasks {
                let permit = concurrency_ch.acquire();

                s.spawn(move || {
                    let mut f = must_create(&task.dst_path, false);
                    if let Err(err) = (task.src)(&mut f) {
                        f.must_close();
                        // Do not call must_remove_path(path), so the user could inspect
                        // the file contents during investigation of the issue.
                        panicf!("FATAL: cannot write data to {:?}: {err}", task.dst_path);
                    }
                    f.must_close();

                    drop(permit);
                });
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn test_write_read() {
        test_write_read_with(false, "");
        test_write_read_with(true, "");
        test_write_read_with(false, "foobar");
        test_write_read_with(true, "foobar");
        test_write_read_with(false, "a\nb\nc\n");
        test_write_read_with(true, "a\nb\nc\n");

        let mut bb = String::new();
        while bb.len() < 3 * DONT_NEED_BLOCK_SIZE as usize {
            bb.push_str(&format!("line {}\n", bb.len()));
        }
        let test_str = bb;

        test_write_read_with(false, &test_str);
        test_write_read_with(true, &test_str);
    }

    // Port-only test: the Go package registers its metrics at init and has no
    // test for them; this pins that the esm_filestream_* counters move with
    // reader/writer traffic.
    #[test]
    fn test_filestream_metrics_move() {
        let m = fs_metrics();
        let read_calls0 = m.read_calls_buffered.get();
        let written_real0 = m.written_bytes_real.get();
        test_write_read_with(false, "metrics probe payload");
        assert!(
            m.read_calls_buffered.get() > read_calls0,
            "esm_filestream_buffered_read_calls_total must grow"
        );
        assert!(
            m.written_bytes_real.get() > written_real0,
            "esm_filestream_real_written_bytes_total must grow"
        );
    }

    fn test_write_read_with(nocache: bool, test_str: &str) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "esl-common-filestream-nocache_test-{}-{}.txt",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));

        let mut w = must_create(&path, nocache);
        if let Err(err) = WriteCloser::write(&mut w, test_str.as_bytes()) {
            panic!("unexpected error when writing testStr: {err}");
        }
        w.must_close();

        let mut r = must_open(&path, nocache);
        let mut buf = vec![0u8; test_str.len()];
        if let Err(err) = std::io::Read::read_exact(&mut r, &mut buf) {
            panic!("unexpected error when reading: {err}");
        }
        assert_eq!(
            buf,
            test_str.as_bytes(),
            "unexpected data read: got and want differ"
        );
        r.must_close();

        std::fs::remove_file(&path).ok();
    }
}

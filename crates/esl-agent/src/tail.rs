//! Port of EsLogs `app/eslagent/tail` — tailing of log files with
//! rotation/truncation detection and persistent read-position checkpoints.
//!
//! Sections below mirror the Go files: `logfile.go` (+ the
//! `logfile_other.go`/`logfile_windows.go` platform split), `checkpoints_db.go`
//! and `tailer.go`.
//!
//! PORT NOTE: Go broadcasts shutdown by closing `stopCh` channels. The port
//! uses one `std::sync::mpsc` channel per tailing thread; dropping the
//! `Sender` side is the "close" broadcast (`needs_stop` observes
//! `TryRecvError::Disconnected`, `BackoffTimer::wait` observes
//! `RecvTimeoutError::Disconnected`).

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, Metadata};
use std::io::Read;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError, channel};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use esl_common::fs::fsutil;
use esl_common::metrics::Counter;
use esl_common::timeutil::BackoffTimer;
use esl_common::{cgroup, infof, panicf, warnf};

// ---------------------------------------------------------------------------
// logfile.go
// ---------------------------------------------------------------------------

/// The maximum log line size that EsLogs can accept.
/// See <https://docs.victoriametrics.com/victorialogs/faq/#what-length-a-log-record-is-expected-to-have>
const MAX_LOG_LINE_SIZE: usize = 2 * 1024 * 1024;

struct LogFile {
    path: String,
    file: Option<File>,

    /// inode tracks the inode of the underlying file.
    /// It is used to detect file rotations.
    ///
    /// It is unexpected for multiple log files in the same mount point to have the same inode
    /// while eslagent is running, because eslagent keeps the current file open until Kubernetes
    /// creates a new log file to handle rotation.
    /// See fingerprint to distinguish files with the same inode.
    inode: u64,

    /// fingerprint contains the file fingerprint.
    /// It helps distinguish files with the same inode,
    /// which can happen if an inode is reused while eslagent is down.
    fingerprint: u64,

    /// offset tracks the current read offset in the file.
    offset: i64,

    /// commit_inode tracks the inode of the last committed log entry.
    commit_inode: u64,
    /// commit_fingerprint contains the last committed fingerprint.
    commit_fingerprint: u64,
    /// commit_offset tracks the offset of the last committed log entry.
    commit_offset: i64,

    /// tail contains the last incomplete line read from the file.
    /// Can be truncated if it exceeds MAX_LOG_LINE_SIZE.
    ///
    /// PORT NOTE: Go pools the tail buffer via `tailByteBufferPool` and uses
    /// `tail == nil` as the "no incomplete line" marker. The port keeps the
    /// allocation inline in the struct; "incomplete line pending" is
    /// `tail_size > 0`.
    tail: Vec<u8>,
    /// tail_size tracks the actual tail size.
    tail_size: usize,
}

fn new_log_file(file_path: &str) -> LogFile {
    LogFile {
        path: file_path.to_string(),
        file: None,
        inode: 0,
        fingerprint: 0,
        offset: 0,
        commit_inode: 0,
        commit_fingerprint: 0,
        commit_offset: 0,
        tail: Vec::new(),
        tail_size: 0,
    }
}

fn new_log_file_from_file(f: File, fingerprint: u64, file_path: &str) -> Result<LogFile, String> {
    let fi = f
        .metadata()
        .map_err(|err| format!("cannot get file info of {file_path:?}: {err}"))?;
    let inode = get_inode(&fi);

    let mut lf = new_log_file(file_path);
    lf.file = Some(f);
    lf.inode = inode;
    lf.commit_inode = inode;
    lf.fingerprint = fingerprint;
    lf.commit_fingerprint = fingerprint;

    Ok(lf)
}

const READ_BUFFER_SIZE: usize = 256 * 1024;

// PORT NOTE: Go uses `sync.Pool` for the 256KiB read buffers; the port uses a
// `Mutex<Vec<..>>` pool handing buffers out by value (the established
// esl-common pattern).
static READ_BYTE_BUFFER_POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

fn get_read_buffer() -> Vec<u8> {
    READ_BYTE_BUFFER_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_else(|| vec![0u8; READ_BUFFER_SIZE])
}

fn put_read_buffer(buf: Vec<u8>) {
    READ_BYTE_BUFFER_POOL.lock().unwrap().push(buf);
}

/// Counting semaphore, port of Go's buffered-channel concurrency limiters.
struct Semaphore {
    cap: usize,
    state: Mutex<usize>,
    cv: Condvar,
}

struct SemaphorePermit<'a> {
    sem: &'a Semaphore,
}

impl Semaphore {
    fn new(cap: usize) -> Semaphore {
        Semaphore {
            cap: cap.max(1),
            state: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    fn acquire(&self) -> SemaphorePermit<'_> {
        let mut n = self.state.lock().unwrap();
        while *n >= self.cap {
            n = self.cv.wait(n).unwrap();
        }
        *n += 1;
        SemaphorePermit { sem: self }
    }
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        let mut n = self.sem.state.lock().unwrap();
        *n -= 1;
        self.sem.cv.notify_one();
    }
}

// readConcurrencyCh in Go is fsutil.GetConcurrencyCh(); reused directly below.
static PROCESS_CONCURRENCY_CH: LazyLock<Semaphore> =
    LazyLock::new(|| Semaphore::new(cgroup::available_cpus()));

/// Result of [`LogFile::try_complete_tail`].
enum TailCompletion<'a> {
    /// The pending line is still not completed; all input data was consumed.
    Incomplete,
    /// The pending line (if any) was handled; `line` holds the completed line
    /// to add, `rest` is the remaining unprocessed data.
    Done {
        rest: &'a [u8],
        line: Option<Vec<u8>>,
    },
}

impl LogFile {
    /// Reads all the available complete lines from the file and passes them to `proc`.
    ///
    /// `stop_ch = None` corresponds to Go's nil stop channel (never stops).
    fn read_lines(&mut self, stop_ch: Option<&Receiver<()>>, proc: &mut dyn Processor) -> bool {
        if self.file.is_none() {
            // This happens on the first read attempt.
            // File may not exist in the case of races with Container Runtime or OS.
            if !self.try_reopen() {
                return false;
            }
        }

        let mut read_buf = get_read_buffer();
        let mut any_read = false;

        loop {
            if needs_stop_opt(stop_ch) {
                put_read_buffer(read_buf);
                return any_read;
            }

            let n = {
                let _permit = fsutil::get_concurrency_ch().acquire();
                match self.file.as_mut().unwrap().read(&mut read_buf) {
                    Ok(n) => n,
                    Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(err) => {
                        panicf!("FATAL: cannot read from file {:?}: {}", self.path, err);
                        unreachable!()
                    }
                }
            };
            if n == 0 {
                // PORT NOTE: Go gets `io.EOF`; Rust reads return `Ok(0)` at EOF.
                put_read_buffer(read_buf);
                return any_read;
            }

            any_read = true;

            {
                let _permit = PROCESS_CONCURRENCY_CH.acquire();
                // The borrow checker forbids passing a slice of read_buf while
                // mutating self; take the buffer out for the call.
                let data = &read_buf[..n];
                self.process_lines(data, proc);
            }

            if n < read_buf.len() {
                // Read less than the buffer size.
                // Stop reading for now.
                put_read_buffer(read_buf);
                return any_read;
            }
        }
    }

    fn process_lines(&mut self, data: &[u8], p: &mut dyn Processor) {
        if data.is_empty() {
            return;
        }

        // Handle incomplete line from the previous read.
        let (mut data, tail) = match self.try_complete_tail(data) {
            TailCompletion::Incomplete => {
                // Line is not completed yet.
                return;
            }
            TailCompletion::Done { rest, line } => (rest, line),
        };

        if let Some(tail_line) = tail {
            if !tail_line.is_empty() {
                self.add_line(p, &tail_line);
            }
            // Give the buffer allocation back to self.tail for reuse.
            let mut buf = tail_line;
            buf.clear();
            self.tail = buf;
        }

        // Process complete lines.
        while let Some(n) = data.iter().position(|&b| b == b'\n') {
            let line = &data[..n];
            data = &data[n + 1..];

            self.add_line(p, line);
        }

        // Save the new incomplete line for the next read.
        self.set_tail(data);
    }

    fn try_complete_tail<'a>(&mut self, data: &'a [u8]) -> TailCompletion<'a> {
        if self.tail_size == 0 {
            // Nothing to complete.
            return TailCompletion::Done {
                rest: data,
                line: None,
            };
        }

        let Some(n) = data.iter().position(|&b| b == b'\n') else {
            // Tail is not finished yet.
            self.tail_size += data.len();
            if self.tail_size <= MAX_LOG_LINE_SIZE {
                self.tail.extend_from_slice(data);
            }
            return TailCompletion::Incomplete;
        };

        let tail_end = &data[..n];
        let rest = &data[n + 1..];

        self.tail_size += tail_end.len();
        if self.tail_size > MAX_LOG_LINE_SIZE {
            TOO_LONG_LINES_SKIPPED.inc();
            warnf!(
                "log line from file {:?} with size {} bytes exceeds maximum allowed size of {} MiB",
                self.path,
                self.tail_size,
                MAX_LOG_LINE_SIZE / 1024 / 1024
            );

            if self.offset == 0 {
                // This is the first line of the current file.
                self.fingerprint = calc_fingerprint(&self.tail);
            }
            self.offset += (self.tail_size + "\n".len()) as i64;

            self.tail_size = 0;
            self.tail.clear();

            return TailCompletion::Done { rest, line: None };
        }

        self.tail.extend_from_slice(tail_end);
        let line = std::mem::take(&mut self.tail);

        self.tail_size = 0;

        TailCompletion::Done {
            rest,
            line: Some(line),
        }
    }

    fn set_tail(&mut self, tail: &[u8]) {
        if self.tail_size > 0 {
            panicf!("BUG: cannot set tail when previous tail is not empty");
        }

        if tail.is_empty() {
            // PORT NOTE: Go returns the buffer to tailByteBufferPool and sets
            // `tail = nil`; the port keeps the allocation in place.
            self.tail.clear();
            self.tail_size = 0;
            return;
        }

        self.tail_size = tail.len();
        self.tail.clear();
        self.tail.extend_from_slice(tail);
    }

    fn add_line(&mut self, p: &mut dyn Processor, line: &[u8]) {
        if self.offset == 0 {
            // This is the first line of the current file.
            self.fingerprint = calc_fingerprint(line);
        }
        self.offset += (line.len() + "\n".len()) as i64;

        let ok = p.try_add_line(line);
        if ok {
            self.commit_inode = self.inode;
            self.commit_fingerprint = self.fingerprint;
            self.commit_offset = self.offset;
        }
    }

    /// status reports the current status of the log file.
    fn status(&self) -> LogFileStatus {
        if !symlink_exists(&self.path) {
            // The symlink itself does not exist.
            return LogFileStatus::Deleted;
        }

        let Some(stat) = must_stat(&self.path) else {
            // The symlink exists, but the target file does not.
            // Treat the file as not rotated because it can be appended to during rotation.
            return LogFileStatus::NotRotated;
        };

        let size = stat.len() as i64;
        if size == 0 {
            // The new log file has been created, but an application hasn't switched to it yet.
            // Consider the file is not rotated, as it may be appended to during the rotation process.
            return LogFileStatus::NotRotated;
        }
        if size < self.offset {
            // The file was truncated.
            return LogFileStatus::Rotated;
        }

        let new_inode = get_inode(&stat);
        if self.inode != new_inode {
            return LogFileStatus::Rotated;
        }

        LogFileStatus::NotRotated
    }

    fn set_offset(&mut self, offset: i64) {
        use std::io::Seek;

        if self.fingerprint == 0 {
            panicf!("BUG: cannot set offset when no fingerprint is set");
        }

        self.offset = offset;
        let f = self.file.as_mut().unwrap();
        if let Err(err) = f.seek(std::io::SeekFrom::Start(offset as u64)) {
            panicf!(
                "FATAL: cannot seek to offset {} in file {:?}: {}",
                offset,
                self.path,
                err
            );
        }

        self.commit_inode = self.inode;
        self.commit_fingerprint = self.fingerprint;
        self.commit_offset = offset;
    }

    fn try_reopen(&mut self) -> bool {
        let Some((new_file, new_inode)) = open_file_with_inode(&self.path) else {
            return false;
        };

        self.close();

        self.file = Some(new_file);
        self.fingerprint = 0;
        self.inode = new_inode;
        self.offset = 0;

        true
    }

    fn close(&mut self) {
        self.file = None;
    }

    fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            path: self.path.clone(),
            inode: self.commit_inode,
            offset: self.commit_offset,
            fingerprint: self.commit_fingerprint,
        }
    }
}

/// max_fingerprint_data_len is the maximum length of the first line used to calculate the fingerprint.
/// 64 bytes is enough because Container Runtime log lines start with a timestamp with nanosecond
/// precision, so different files have unique prefixes.
const MAX_FINGERPRINT_DATA_LEN: usize = 64;

fn calc_fingerprint(data: &[u8]) -> u64 {
    let data = if data.len() > MAX_FINGERPRINT_DATA_LEN {
        &data[..MAX_FINGERPRINT_DATA_LEN]
    } else {
        data
    };
    let h = xxh64(data, 0);
    if h == 0 {
        // 0 hash is reserved to indicate no hash calculated.
        return 1;
    }
    h
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogFileStatus {
    NotRotated,
    Rotated,
    Deleted,
}

fn must_stat(path: &str) -> Option<Metadata> {
    match fs::metadata(path) {
        Ok(fi) => Some(fi),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            panicf!("FATAL: cannot get file info of {path:?}: {err}");
            unreachable!()
        }
    }
}

fn symlink_exists(path: &str) -> bool {
    match fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(err) => {
            panicf!("FATAL: cannot get symlink info of {path:?}: {err}");
            unreachable!()
        }
    }
}

// ---------------------------------------------------------------------------
// logfile_other.go / logfile_windows.go
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn get_inode(fi: &Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    fi.ino()
}

#[cfg(windows)]
fn get_inode(_fi: &Metadata) -> u64 {
    panicf!("eslagent does not support collecting logs from files on Windows");
    unreachable!()
}

// ---------------------------------------------------------------------------
// xxhash (internal)
// ---------------------------------------------------------------------------

// PORT NOTE: Go uses github.com/cespare/xxhash/v2. The esl-agent crate has no
// xxhash dependency, so the XXH64 algorithm is implemented here (verified
// against xxhash-rust/cespare vectors in the tests below). The fingerprints
// are only compared against previously persisted fingerprints, but keeping
// the hash bit-identical to Go preserves checkpoint-file compatibility.
fn xxh64(data: &[u8], seed: u64) -> u64 {
    const P1: u64 = 0x9E3779B185EBCA87;
    const P2: u64 = 0xC2B2AE3D27D4EB4F;
    const P3: u64 = 0x165667B19E3779F9;
    const P4: u64 = 0x85EBCA77C2B2AE63;
    const P5: u64 = 0x27D4EB2F165667C5;

    #[inline]
    fn round(acc: u64, input: u64) -> u64 {
        acc.wrapping_add(input.wrapping_mul(P2))
            .rotate_left(31)
            .wrapping_mul(P1)
    }

    #[inline]
    fn merge_round(acc: u64, val: u64) -> u64 {
        (acc ^ round(0, val)).wrapping_mul(P1).wrapping_add(P4)
    }

    #[inline]
    fn read_u64(b: &[u8]) -> u64 {
        u64::from_le_bytes(b[..8].try_into().unwrap())
    }

    #[inline]
    fn read_u32(b: &[u8]) -> u64 {
        u32::from_le_bytes(b[..4].try_into().unwrap()) as u64
    }

    let len = data.len() as u64;
    let mut p = data;

    let mut h = if data.len() >= 32 {
        let mut v1 = seed.wrapping_add(P1).wrapping_add(P2);
        let mut v2 = seed.wrapping_add(P2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(P1);
        while p.len() >= 32 {
            v1 = round(v1, read_u64(p));
            v2 = round(v2, read_u64(&p[8..]));
            v3 = round(v3, read_u64(&p[16..]));
            v4 = round(v4, read_u64(&p[24..]));
            p = &p[32..];
        }
        let mut h = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h = merge_round(h, v1);
        h = merge_round(h, v2);
        h = merge_round(h, v3);
        merge_round(h, v4)
    } else {
        seed.wrapping_add(P5)
    };

    h = h.wrapping_add(len);

    while p.len() >= 8 {
        h ^= round(0, read_u64(p));
        h = h.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        p = &p[8..];
    }
    if p.len() >= 4 {
        h ^= read_u32(p).wrapping_mul(P1);
        h = h.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        p = &p[4..];
    }
    for &b in p {
        h ^= (b as u64).wrapping_mul(P5);
        h = h.rotate_left(11).wrapping_mul(P1);
    }

    h ^= h >> 33;
    h = h.wrapping_mul(P2);
    h ^= h >> 29;
    h = h.wrapping_mul(P3);
    h ^= h >> 32;
    h
}

// ---------------------------------------------------------------------------
// checkpoints_db.go
// ---------------------------------------------------------------------------

/// Checkpoint represents a persistent snapshot of a log file reading state.
///
/// The checkpoint is saved to disk to enable resuming log collection from the exact
/// position after eslagent restarts, preventing:
///  1. Log duplication when logs are re-read from the beginning.
///  2. Log loss when a log file was rotated while eslagent was down.
///     In this case we should find the rotated file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    pub(crate) path: String,
    pub(crate) inode: u64,
    pub(crate) fingerprint: u64,
    pub(crate) offset: i64,
}

/// CheckpointsDB manages persistent log file reading state checkpoints.
/// It saves reading positions to disk to enable resuming log collection
/// after eslagent restarts without data loss or duplication.
///
/// The caller is responsible for closing CheckpointsDB via the stop() method
/// when it's no longer needed.
pub(crate) struct CheckpointsDB {
    checkpoints_path: String,

    checkpoints: Mutex<HashMap<String, Checkpoint>>,

    stop_tx: Mutex<Option<Sender<()>>>,
    sync_thread: Mutex<Option<JoinHandle<()>>>,
}

/// start_checkpoints_db starts a CheckpointsDB instance.
/// The caller must call stop() when the CheckpointsDB is no longer needed.
fn start_checkpoints_db(path: &str) -> Result<std::sync::Arc<CheckpointsDB>, String> {
    let checkpoints = read_checkpoints(path)?;

    let mut checkpoints_map = HashMap::new();
    for cp in checkpoints {
        checkpoints_map.insert(cp.path.clone(), cp);
    }

    let db = std::sync::Arc::new(CheckpointsDB {
        checkpoints_path: path.to_string(),
        checkpoints: Mutex::new(checkpoints_map),
        stop_tx: Mutex::new(None),
        sync_thread: Mutex::new(None),
    });

    db.start_periodic_sync_checkpoints();

    Ok(db)
}

impl CheckpointsDB {
    fn set(&self, cp: Checkpoint) {
        let mut checkpoints = self.checkpoints.lock().unwrap();
        checkpoints.insert(cp.path.clone(), cp);
    }

    pub(crate) fn get(&self, path: &str) -> Option<Checkpoint> {
        let checkpoints = self.checkpoints.lock().unwrap();
        checkpoints.get(path).cloned()
    }

    fn get_all(&self) -> Vec<Checkpoint> {
        let checkpoints = self.checkpoints.lock().unwrap();
        checkpoints.values().cloned().collect()
    }

    fn delete(&self, path: &str) {
        let mut checkpoints = self.checkpoints.lock().unwrap();
        checkpoints.remove(path);
    }

    fn must_sync(&self) {
        let mut cps = self.get_all();

        cps.sort_by(|a, b| a.path.cmp(&b.path));

        let data = marshal_checkpoints(&cps);

        esl_common::fs::must_write_atomic(&self.checkpoints_path, &data, true);
    }

    /// start_periodic_sync_checkpoints periodically persists in-memory checkpoints to disk.
    ///
    /// It complements the explicit sync performed on graceful stop,
    /// ensuring regular persistence even when the process is killed.
    fn start_periodic_sync_checkpoints(self: &std::sync::Arc<Self>) {
        let (tx, rx) = channel::<()>();
        *self.stop_tx.lock().unwrap() = Some(tx);

        let db = std::sync::Arc::clone(self);
        let handle = std::thread::spawn(move || {
            loop {
                match rx.recv_timeout(Duration::from_secs(60)) {
                    Err(RecvTimeoutError::Timeout) => db.must_sync(),
                    _ => {
                        db.must_sync();
                        return;
                    }
                }
            }
        });
        *self.sync_thread.lock().unwrap() = Some(handle);
    }

    fn stop(&self) {
        drop(self.stop_tx.lock().unwrap().take());
        if let Some(handle) = self.sync_thread.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

fn read_checkpoints(path: &str) -> Result<Vec<Checkpoint>, String> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            infof!(
                "no checkpoints file found at {path:?}; eslagent will read log files from the beginning"
            );
            return Ok(Vec::new());
        }
        Err(err) => {
            return Err(format!("cannot read file checkpoints: {err}"));
        }
    };

    if data.is_empty() {
        return Ok(Vec::new());
    }

    parse_checkpoints_json(&data)
        .map_err(|err| format!("cannot unmarshal file checkpoints from {path:?}: {err}"))
}

// PORT NOTE: Go serializes checkpoints with `encoding/json`
// (`json.MarshalIndent(cps, "", "\t")` / `json.Unmarshal`). The esl-agent crate
// has no serde dependency, so the writer and a minimal parser for the
// array-of-flat-objects format are implemented by hand below; the output
// matches Go's field names and indentation.

fn marshal_checkpoints(cps: &[Checkpoint]) -> Vec<u8> {
    if cps.is_empty() {
        return b"[]".to_vec();
    }

    let mut out = String::from("[");
    for (i, cp) in cps.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("\n\t{\n\t\t\"path\": ");
        json_quote(&mut out, &cp.path);
        out.push_str(&format!(
            ",\n\t\t\"inode\": {},\n\t\t\"fingerprint\": {},\n\t\t\"offset\": {}\n\t}}",
            cp.inode, cp.fingerprint, cp.offset
        ));
    }
    out.push_str("\n]");
    out.into_bytes()
}

fn json_quote(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn parse_checkpoints_json(data: &[u8]) -> Result<Vec<Checkpoint>, String> {
    let mut p = JsonCursor { data, pos: 0 };
    p.skip_ws();
    if p.eat_literal("null") {
        return Ok(Vec::new());
    }
    p.expect(b'[')?;
    p.skip_ws();
    if p.peek() == Some(b']') {
        return Ok(Vec::new());
    }
    let mut cps = Vec::new();
    loop {
        p.skip_ws();
        cps.push(p.parse_checkpoint()?);
        p.skip_ws();
        match p.next() {
            Some(b',') => continue,
            Some(b']') => return Ok(cps),
            _ => return Err("expected ',' or ']' in checkpoints array".to_string()),
        }
    }
}

struct JsonCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl JsonCursor<'_> {
    fn skip_ws(&mut self) {
        while let Some(&b) = self.data.get(self.pos) {
            if b == b' ' || b == b'\t' || b == b'\n' || b == b'\r' {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.data.get(self.pos).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Some(b)
    }

    fn expect(&mut self, b: u8) -> Result<(), String> {
        if self.next() != Some(b) {
            return Err(format!("expected {:?}", b as char));
        }
        Ok(())
    }

    fn eat_literal(&mut self, lit: &str) -> bool {
        if self.data[self.pos..].starts_with(lit.as_bytes()) {
            self.pos += lit.len();
            return true;
        }
        false
    }

    fn parse_checkpoint(&mut self) -> Result<Checkpoint, String> {
        self.expect(b'{')?;
        let mut cp = Checkpoint::default();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(cp);
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect(b':')?;
            self.skip_ws();
            match key.as_str() {
                "path" => cp.path = self.parse_string()?,
                "inode" => cp.inode = self.parse_u64()?,
                "fingerprint" => cp.fingerprint = self.parse_u64()?,
                "offset" => cp.offset = self.parse_i64()?,
                _ => self.skip_value()?,
            }
            self.skip_ws();
            match self.next() {
                Some(b',') => continue,
                Some(b'}') => return Ok(cp),
                _ => return Err("expected ',' or '}' in checkpoint object".to_string()),
            }
        }
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
                    Some(b'b') => out.push('\u{0008}'),
                    Some(b'f') => out.push('\u{000c}'),
                    Some(b'n') => out.push('\n'),
                    Some(b'r') => out.push('\r'),
                    Some(b't') => out.push('\t'),
                    Some(b'u') => {
                        let cp = self.parse_hex4()?;
                        if (0xD800..0xDC00).contains(&cp) {
                            // Surrogate pair.
                            if self.next() != Some(b'\\') || self.next() != Some(b'u') {
                                return Err("invalid surrogate pair".to_string());
                            }
                            let low = self.parse_hex4()?;
                            if !(0xDC00..0xE000).contains(&low) {
                                return Err("invalid surrogate pair".to_string());
                            }
                            let c = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                            out.push(
                                char::from_u32(c).ok_or_else(|| "invalid codepoint".to_string())?,
                            );
                        } else {
                            out.push(
                                char::from_u32(cp)
                                    .ok_or_else(|| "invalid codepoint".to_string())?,
                            );
                        }
                    }
                    _ => return Err("invalid escape sequence".to_string()),
                },
                Some(b) if b < 0x80 => out.push(b as char),
                Some(b) => {
                    // Multi-byte UTF-8 sequence: copy the raw bytes through.
                    let start = self.pos - 1;
                    let len = match b {
                        0xC0..=0xDF => 2,
                        0xE0..=0xEF => 3,
                        _ => 4,
                    };
                    if start + len > self.data.len() {
                        return Err("truncated UTF-8 sequence".to_string());
                    }
                    let s = std::str::from_utf8(&self.data[start..start + len])
                        .map_err(|_| "invalid UTF-8 in string".to_string())?;
                    out.push_str(s);
                    self.pos = start + len;
                }
            }
        }
    }

    fn parse_hex4(&mut self) -> Result<u32, String> {
        let mut v = 0u32;
        for _ in 0..4 {
            let b = self
                .next()
                .ok_or_else(|| "truncated \\u escape".to_string())?;
            let d = (b as char)
                .to_digit(16)
                .ok_or_else(|| "invalid \\u escape".to_string())?;
            v = v * 16 + d;
        }
        Ok(v)
    }

    fn parse_number_str(&mut self) -> Result<String, String> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while let Some(b) = self.peek() {
            if b.is_ascii_digit() {
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err("expected number".to_string());
        }
        Ok(std::str::from_utf8(&self.data[start..self.pos])
            .unwrap()
            .to_string())
    }

    fn parse_u64(&mut self) -> Result<u64, String> {
        let s = self.parse_number_str()?;
        s.parse::<u64>()
            .map_err(|err| format!("invalid number {s:?}: {err}"))
    }

    fn parse_i64(&mut self) -> Result<i64, String> {
        let s = self.parse_number_str()?;
        s.parse::<i64>()
            .map_err(|err| format!("invalid number {s:?}: {err}"))
    }

    fn skip_value(&mut self) -> Result<(), String> {
        self.skip_ws();
        match self.peek() {
            Some(b'"') => {
                self.parse_string()?;
            }
            Some(b'-') | Some(b'0'..=b'9') => {
                self.parse_number_str()?;
            }
            Some(b't') if self.eat_literal("true") => {}
            Some(b'f') if self.eat_literal("false") => {}
            Some(b'n') if self.eat_literal("null") => {}
            _ => return Err("unsupported JSON value".to_string()),
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// tailer.go
// ---------------------------------------------------------------------------

/// Processor processes log lines from a single file.
/// Log lines can be accumulated within a single file without committing them to the CheckpointsDB.
pub trait Processor: Send {
    /// try_add_line processes a log line and returns true if it should be committed
    /// to the CheckpointsDB.
    /// Returns true if the current line should be committed to CheckpointsDB, false otherwise.
    ///
    /// This allows accumulating multiple lines within a file before committing, which is useful for:
    /// - Multi-line log entries that span across lines.
    /// - Batching multiple log lines for efficiency.
    /// - Custom log parsing that needs context from multiple lines.
    ///
    /// Note: when a log file is rotated, no checkpoint will be written until try_add_line
    /// returns true, ensuring log entries spanning multiple files are handled correctly.
    fn try_add_line(&mut self, line: &[u8]) -> bool;

    /// Flush flushes any internally accumulated state.
    /// The caller is responsible for invoking flush when no new log lines are expected
    /// for a while, ensuring the accumulated state is propagated without waiting for
    /// the next line.
    fn flush(&mut self);

    /// must_close releases all resources associated with the Processor and ensures
    /// proper cleanup of internal states.
    /// It must be called after the target log file is deleted or eslagent is shutting down.
    fn must_close(&mut self);
}

struct TailerInner {
    log_files: Mutex<HashSet<String>>,
    checkpoints_db: std::sync::Arc<CheckpointsDB>,
}

pub struct Tailer {
    inner: std::sync::Arc<TailerInner>,

    // PORT NOTE: Go uses `sync.WaitGroup` + a closable `stopCh`; the port
    // keeps one JoinHandle and one stop Sender per tailing thread. Dropping
    // the senders is the close broadcast.
    threads: Mutex<Vec<JoinHandle<()>>>,
    stop_txs: Mutex<Vec<Sender<()>>>,
}

/// Start initializes a new Tailer with the given checkpoints storage path.
/// The caller must call stop() when the Tailer is no longer needed.
///
/// The Tailer maintains a checkpoint file as persistent state,
/// allowing log reading to resume from the last position after eslagent restart.
pub fn start(checkpoints_path: &str) -> Tailer {
    let checkpoints_db = match start_checkpoints_db(checkpoints_path) {
        Ok(db) => db,
        Err(err) => {
            panicf!("FATAL: cannot start checkpoints DB: {err}");
            unreachable!()
        }
    };

    Tailer {
        inner: std::sync::Arc::new(TailerInner {
            log_files: Mutex::new(HashSet::new()),
            checkpoints_db,
        }),
        threads: Mutex::new(Vec::new()),
        stop_txs: Mutex::new(Vec::new()),
    }
}

impl Tailer {
    pub fn start_read(&self, rel_path: &str, mut proc: Box<dyn Processor>) {
        // Use absolute paths to prevent duplicate logs in case the eslagent working
        // directory changes.
        let abs_path = match std::path::absolute(rel_path) {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(err) => {
                panicf!("FATAL: cannot get absolute path of {rel_path:?}: {err}");
                unreachable!()
            }
        };

        {
            let mut log_files = self.inner.log_files.lock().unwrap();
            if !log_files.insert(abs_path.clone()) {
                // Already reading from the file.
                drop(log_files);
                proc.must_close();
                return;
            }
        }

        let (stop_tx, stop_rx) = channel::<()>();
        self.stop_txs.lock().unwrap().push(stop_tx);

        let inner = std::sync::Arc::clone(&self.inner);
        let handle = std::thread::spawn(move || {
            let mut lf = open_log_file(&inner, &abs_path);
            process(&inner, &mut lf, proc.as_mut(), &stop_rx);
            // Go defers in StartRead's goroutine (via fc.process).
            lf.close();
            proc.must_close();
        });
        self.threads.lock().unwrap().push(handle);
    }

    /// cleanup_checkpoints removes all checkpoints for files that are no longer
    /// being processed.
    pub fn cleanup_checkpoints(&self) {
        let unused_checkpoints = self.get_unused_checkpoints();
        if unused_checkpoints.is_empty() {
            return;
        }

        for cp in &unused_checkpoints {
            self.inner.checkpoints_db.delete(&cp.path);
        }

        warnf!(
            "{} log files were deleted before being fully read; \
             this is expected if files were deleted while eslagent was restarting; \
             an example of such file: {:?}",
            unused_checkpoints.len(),
            unused_checkpoints[0].path
        );
    }

    fn get_unused_checkpoints(&self) -> Vec<Checkpoint> {
        let cps = self.inner.checkpoints_db.get_all();

        let log_files = self.inner.log_files.lock().unwrap();

        let mut unused = Vec::new();
        for cp in cps {
            if log_files.contains(&cp.path) {
                continue;
            }
            unused.push(cp);
        }
        unused
    }

    pub fn is_tailing(&self, rel_path: &str) -> bool {
        // Use absolute paths to prevent duplicate logs in case the eslagent working
        // directory changes.
        let abs_path = match std::path::absolute(rel_path) {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(err) => {
                panicf!("FATAL: cannot get absolute path of {rel_path:?}: {err}");
                unreachable!()
            }
        };

        let log_files = self.inner.log_files.lock().unwrap();
        log_files.contains(&abs_path)
    }

    pub fn stop(&self) {
        self.stop_txs.lock().unwrap().clear();
        let threads: Vec<JoinHandle<()>> = self.threads.lock().unwrap().drain(..).collect();
        for handle in threads {
            let _ = handle.join();
        }
        self.inner.checkpoints_db.stop();
    }

    #[cfg(test)]
    pub(crate) fn checkpoints_db(&self) -> &CheckpointsDB {
        &self.inner.checkpoints_db
    }
}

fn open_log_file(inner: &TailerInner, filepath: &str) -> LogFile {
    let Some(cp) = inner.checkpoints_db.get(filepath) else {
        // No checkpoint found - start reading from the beginning of the file.
        return new_log_file(filepath);
    };

    match try_resume_from_checkpoint(filepath, &cp) {
        Some(lf) => lf,
        None => {
            inner.checkpoints_db.delete(filepath);
            new_log_file(filepath)
        }
    }
}

fn try_resume_from_checkpoint(filepath: &str, cp: &Checkpoint) -> Option<LogFile> {
    let f = match open_file_with_inode(&cp.path) {
        None => {
            // The file was deleted just after start_read was called.
            warnf!(
                "log file {filepath:?} was deleted before being fully read; \
                 this is expected if the file was deleted while eslagent was starting"
            );
            return None;
        }
        Some((f, inode)) => {
            if inode == cp.inode {
                f
            } else {
                drop(f);

                // When kubelet or logrotate rotates log files, it typically keeps the previous
                // log file uncompressed in the same directory with a different name (typically
                // with a timestamp suffix).
                // We attempt to find this renamed file to continue reading from our last offset.
                // See https://github.com/kubernetes/kubernetes/blob/f794aa12d78f5b1f9579ce8a991a116a99a2c43c/pkg/kubelet/logs/container_log_manager.go#L416
                match find_renamed_file(&cp.path, cp.inode) {
                    Some(f) => f,
                    None => {
                        // Could not find the rotated file with matching inode.
                        // This means the file was rotated and potentially removed before we
                        // could process it.
                        warnf!(
                            "skipping log file {filepath:?}: rotated log file not found (inode={}); \
                             some log lines may have been lost; \
                             this typically happens when logs rotate faster than eslagent can process them during startup or downtime; \
                             consider increasing kubelet's --container-log-max-size to reduce log rotation frequency",
                            cp.inode
                        );
                        return None;
                    }
                }
            }
        }
    };

    let fp = get_file_fingerprint(&f, &cp.path);
    if fp == 0 || cp.fingerprint != 0 && cp.fingerprint != fp {
        warnf!(
            "skipping log file {filepath:?}: file content changed unexpectedly (expected fingerprint={}, got={fp}); \
             log file was likely rotated and truncated before eslagent could finish reading; \
             some log lines may have been lost; \
             this typically happens when logs rotate faster than eslagent can process them during startup or downtime; \
             consider reducing log rotation frequency",
            cp.fingerprint
        );
        return None;
    }

    let mut logfile = match new_log_file_from_file(f, fp, &cp.path) {
        Ok(lf) => lf,
        Err(err) => {
            panicf!("FATAL: cannot create log file: {err}");
            unreachable!()
        }
    };
    logfile.set_offset(cp.offset);

    Some(logfile)
}

/// get_file_fingerprint returns a fingerprint of the file.
/// This function returns 0 if the file does not contain any valid log lines.
fn get_file_fingerprint(f: &File, path: &str) -> u64 {
    let mut buf = [0u8; MAX_FINGERPRINT_DATA_LEN];
    let mut n = 0usize;
    while n < buf.len() {
        match read_at(f, &mut buf[n..], n as u64) {
            Ok(0) => break, // EOF
            Ok(m) => n += m,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => {
                panicf!("FATAL: cannot read file {path:?}: {err}");
                unreachable!()
            }
        }
    }

    let nl = buf[..n].iter().position(|&b| b == b'\n');
    let data = match nl {
        None if n < buf.len() => {
            // Line is not yet fully written - cannot calculate fingerprint.
            return 0;
        }
        None => &buf[..n],
        Some(nl) => &buf[..nl],
    };

    calc_fingerprint(data)
}

#[cfg(unix)]
fn read_at(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;
    f.read_at(buf, offset)
}

#[cfg(windows)]
fn read_at(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::windows::fs::FileExt;
    // PORT NOTE: unlike Go's ReadAt, seek_read moves the file cursor. The only
    // caller (get_file_fingerprint) is followed by an explicit seek in
    // set_offset, so the cursor state does not leak.
    f.seek_read(buf, offset)
}

fn process(
    inner: &TailerInner,
    lf: &mut LogFile,
    proc: &mut dyn Processor,
    stop_rx: &Receiver<()>,
) {
    let mut bt = BackoffTimer::new(100_000_000, 10_000_000_000); // 100ms .. 10s

    // Never-closed channel standing in for Go's nil channel; `_never_tx` must
    // stay alive so the receiver never disconnects.
    let (_never_tx, never_rx) = channel::<()>();

    loop {
        if needs_stop(stop_rx) {
            return;
        }

        let ok = lf.read_lines(Some(stop_rx), proc);
        if ok {
            // Some lines were read - update checkpoint and wait before checking again.
            inner.checkpoints_db.set(lf.checkpoint());
            bt.reset();
            bt.wait(stop_rx);
            continue;
        }

        // No lines read - check the log file status.
        match lf.status() {
            LogFileStatus::NotRotated => {
                // No more lines to read and file hasn't rotated - wait before checking again.
                proc.flush();
                bt.wait(stop_rx);
                continue;
            }
            LogFileStatus::Rotated => {
                // Ensure all remaining lines are flushed to the rotated file and read from it.
                // Do not use stop_rx here to finish reading from the rotated file even if
                // eslagent is shutting down.
                bt.reset();
                bt.wait(&never_rx);
                if lf.read_lines(Some(&never_rx), proc) {
                    // Double-check: if there are still new lines, it's an unexpected situation.
                    bt.wait(&never_rx);
                    if lf.read_lines(Some(&never_rx), proc) {
                        panicf!("BUG: log file {:?} was appended after rotation", lf.path);
                    }
                }

                if lf.try_reopen() {
                    inner.checkpoints_db.set(lf.checkpoint());
                } else {
                    // Cannot reopen the file right now - wait before retrying.
                    bt.wait(stop_rx);
                }
                continue;
            }
            LogFileStatus::Deleted => {
                forget_file(inner, &lf.path);

                if lf.tail_size > 0 {
                    panicf!(
                        "BUG: tail must be empty when the log file no longer exists; got: {:?}",
                        String::from_utf8_lossy(&lf.tail)
                    );
                }
                return;
            }
        }
    }
}

/// forget_file removes the given file from the tracking list and deletes its checkpoint.
/// It is called when the file is not expected to reappear, so its state no longer needs
/// to be stored.
fn forget_file(inner: &TailerInner, file_path: &str) {
    inner.checkpoints_db.delete(file_path);

    let mut log_files = inner.log_files.lock().unwrap();
    log_files.remove(file_path);
}

/// find_renamed_file looks for a file with the given inode in the same directory as log_path.
fn find_renamed_file(log_path: &str, inode: u64) -> Option<File> {
    let actual_path = try_resolve_symlink(log_path);

    let dir = match std::path::Path::new(&actual_path).parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        Err(err) => {
            panicf!("FATAL: cannot read dir {:?}: {}", dir.display(), err);
            unreachable!()
        }
    };

    // PORT NOTE: Go's os.ReadDir returns entries sorted by name; sort for the
    // same deterministic scan order.
    let mut entries: Vec<_> = entries.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());

    for de in entries {
        if de.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
            continue;
        }

        let file_name = de.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name.ends_with(".gz") {
            continue;
        }

        let file_path = dir.join(file_name.as_ref());
        let Some((file, file_inode)) = open_file_with_inode(&file_path.to_string_lossy()) else {
            continue;
        };

        if file_inode == inode {
            return Some(file);
        }

        drop(file);
    }

    None
}

fn needs_stop(rx: &Receiver<()>) -> bool {
    match rx.try_recv() {
        Ok(()) | Err(TryRecvError::Disconnected) => true,
        Err(TryRecvError::Empty) => false,
    }
}

fn needs_stop_opt(rx: Option<&Receiver<()>>) -> bool {
    match rx {
        Some(rx) => needs_stop(rx),
        None => false,
    }
}

fn open_file_with_inode(p: &str) -> Option<(File, u64)> {
    let f = match File::open(p) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        Err(err) => {
            panicf!("FATAL: cannot open file: {err}");
            unreachable!()
        }
    };

    let fi = match f.metadata() {
        Ok(fi) => fi,
        Err(err) => {
            panicf!("FATAL: cannot stat file: {err}");
            unreachable!()
        }
    };
    let inode = get_inode(&fi);

    Some((f, inode))
}

/// try_resolve_symlink resolves symlink to its target path.
/// If symlink cannot be resolved (e.g., symlink is not valid), returns the original path.
fn try_resolve_symlink(symlink: &str) -> String {
    match fs::read_link(symlink) {
        Ok(resolved_path) => resolved_path.to_string_lossy().into_owned(),
        Err(_) => symlink.to_string(),
    }
}

static TOO_LONG_LINES_SKIPPED: LazyLock<Arc<Counter>> = LazyLock::new(|| {
    esl_common::metrics::get_or_create_counter("esl_too_long_lines_skipped_total")
});

// ---------------------------------------------------------------------------
// Tests: logfile_test.go, logfile_timing_test.go, tailer_test.go
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // xxh64 vectors verified against xxhash-rust (cespare/xxhash-compatible).
    #[test]
    fn test_xxh64_vectors() {
        let f = |data: &[u8], expected: u64| {
            let h = xxh64(data, 0);
            assert_eq!(h, expected, "unexpected xxh64 for {data:?}");
        };

        f(b"", 0xef46db3751d8e999);
        f(b"a", 0xd24ec4f1a98c6e5b);
        f(b"abc", 0x44bc2cf5ad770999);
        f(
            b"Nobody inspects the spammish repetition",
            0xfbcea83c8a378bf1,
        );
        f(
            b"2025-10-16T15:37:36.1Z stderr F full line",
            0x8d7e73617af77b44,
        );
        f(
            b"0123456789012345678901234567890123456789012345678901234567890123",
            0xd502f0d566ce31d4,
        );
    }

    #[test]
    fn test_calc_fingerprint() {
        // The fingerprint uses at most the first MAX_FINGERPRINT_DATA_LEN bytes.
        let data: Vec<u8> = (0u8..=127).collect();
        assert_eq!(
            calc_fingerprint(&data),
            calc_fingerprint(&data[..MAX_FINGERPRINT_DATA_LEN])
        );
        // 0 is never returned.
        assert_ne!(calc_fingerprint(b""), 0);
    }

    #[test]
    fn test_checkpoints_json_roundtrip() {
        let f = |cps: Vec<Checkpoint>| {
            let data = marshal_checkpoints(&cps);
            let got = parse_checkpoints_json(&data).unwrap();
            assert_eq!(got, cps, "roundtrip mismatch for {:?}", cps);
        };

        f(Vec::new());
        f(vec![Checkpoint {
            path: "/var/log/pods/app.log".to_string(),
            inode: 12345,
            fingerprint: u64::MAX,
            offset: -7,
        }]);
        f(vec![
            Checkpoint {
                path: "a \"quoted\"\npath\\with\tescapes".to_string(),
                inode: 1,
                fingerprint: 2,
                offset: 3,
            },
            Checkpoint {
                path: "C:\\logs\\päth-щ.log".to_string(),
                inode: u64::MAX,
                fingerprint: 0,
                offset: i64::MAX,
            },
        ]);

        // null (Go marshals a nil slice as null).
        assert_eq!(parse_checkpoints_json(b"null").unwrap(), Vec::new());
        // Unknown keys and arbitrary whitespace are tolerated.
        let got = parse_checkpoints_json(
            b" [ { \"unknown\": \"x\", \"path\": \"p\", \"inode\": 4, \"extra\": null,\n \"offset\": 5, \"fingerprint\": 6 } ] ",
        )
        .unwrap();
        assert_eq!(
            got,
            vec![Checkpoint {
                path: "p".to_string(),
                inode: 4,
                fingerprint: 6,
                offset: 5,
            }]
        );
    }

    // ------------------------------------------------------------------
    // Shared helpers (ports of the Go test helpers). The tail runtime
    // requires inode support, so everything below is unix-only, like the
    // Go tests (getInode panics on Windows).
    // ------------------------------------------------------------------

    #[cfg(unix)]
    mod unix_tests {
        use super::super::*;
        use std::io::Write;
        use std::path::PathBuf;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicI64, Ordering};

        /// Port of Go's `t.TempDir()`: a fresh unique dir removed on drop.
        struct TempDir {
            path: PathBuf,
        }

        static NEXT_TEMP_DIR_ID: AtomicI64 = AtomicI64::new(0);

        fn temp_dir() -> TempDir {
            let n = NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("esl-agent-tail-test-{}-{n}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            TempDir { path }
        }

        impl Drop for TempDir {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.path);
            }
        }

        static NEXT_FILE_ID: AtomicI64 = AtomicI64::new(0);

        /// Port of Go createTestLogFile: creates the log file in one temp dir
        /// and a symlink to it in another; returns (symlink_path, inode).
        fn create_test_log_file(dirs: &mut Vec<TempDir>) -> (String, u64) {
            let id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed) + 1;
            let name = format!("logfile-{id}.log");

            let d1 = temp_dir();
            let d2 = temp_dir();
            let log_file_path = d1.path.join(&name);
            let symlink_path = d2.path.join(&name);
            dirs.push(d1);
            dirs.push(d2);

            File::create(&log_file_path).expect("failed to create log file");

            std::os::unix::fs::symlink(&log_file_path, &symlink_path)
                .expect("failed to create symlink");

            let stat = must_stat(log_file_path.to_str().unwrap()).expect("failed to stat log file");
            let inode = get_inode(&stat);

            (symlink_path.to_string_lossy().into_owned(), inode)
        }

        fn write_lines_to_file(file_path: &str, lines: &[&str]) {
            let mut f = fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(file_path)
                .expect("failed to open file");

            for s in lines {
                let s = s.trim_end_matches('\n');
                write_to_file(&mut f, &format!("{s}\n"));
            }
            f.sync_all().expect("failed to sync file");
        }

        fn write_to_file(f: &mut File, data: &str) {
            if data.is_empty() {
                return;
            }
            f.write_all(data.as_bytes())
                .expect("failed to write to file");
        }

        // Port of Go's sync.WaitGroup usage in testProcessor.
        struct WaitGroup {
            n: Mutex<i64>,
            cv: Condvar,
        }

        impl WaitGroup {
            fn new() -> WaitGroup {
                WaitGroup {
                    n: Mutex::new(0),
                    cv: Condvar::new(),
                }
            }

            fn add(&self, delta: i64) {
                let mut n = self.n.lock().unwrap();
                *n += delta;
                if *n <= 0 {
                    self.cv.notify_all();
                }
            }

            fn done(&self) {
                self.add(-1);
            }

            fn wait(&self) {
                let mut n = self.n.lock().unwrap();
                while *n > 0 {
                    n = self.cv.wait(n).unwrap();
                }
            }
        }

        type CommitFn = Box<dyn Fn(&[u8]) -> bool + Send + Sync>;

        struct TestProcessorInner {
            lines: Mutex<Vec<String>>,
            commit_fn: Option<CommitFn>,
            wg: WaitGroup,
        }

        /// Port of Go testProcessor. Cloneable handle so the test keeps
        /// access while the tailer owns a boxed copy.
        #[derive(Clone)]
        struct TestProcessor(Arc<TestProcessorInner>);

        fn new_test_processor(commit_fn: Option<CommitFn>) -> TestProcessor {
            TestProcessor(Arc::new(TestProcessorInner {
                lines: Mutex::new(Vec::new()),
                commit_fn,
                wg: WaitGroup::new(),
            }))
        }

        impl TestProcessor {
            fn expect(&self, n: i64) {
                self.0.wg.add(n);
            }

            fn wait(&self) {
                self.0.wg.wait();
            }

            fn verify(&self, expected: &str) -> Result<(), String> {
                let lines = self.0.lines.lock().unwrap();
                let mut got = String::new();
                if !lines.is_empty() {
                    got = format!("{}\n", lines.join("\n"));
                }
                if got != expected {
                    return Err(format!(
                        "unexpected log lines;\ngot:\n{got:?}\nwant:\n{expected:?}"
                    ));
                }
                Ok(())
            }
        }

        impl Processor for TestProcessor {
            fn try_add_line(&mut self, line: &[u8]) -> bool {
                self.0
                    .lines
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(line).into_owned());
                let commit = match &self.0.commit_fn {
                    None => true,
                    Some(f) => f(line),
                };
                self.0.wg.done();
                commit
            }

            fn flush(&mut self) {}

            fn must_close(&mut self) {}
        }

        // Port of Go TestReadLines.
        #[test]
        fn test_read_lines() {
            let f = |input: &[&str], expected: &str, expected_offset: i64| {
                // Open stop channel (Go: t.Context().Done()).
                let (_stop_tx, stop_rx) = channel::<()>();
                let mut dirs = Vec::new();
                let (file_path, _) = create_test_log_file(&mut dirs);

                write_lines_to_file(&file_path, input);
                let mut lf = new_log_file(&file_path);

                let mut proc = new_test_processor(None);
                proc.expect(input.len() as i64);
                lf.read_lines(Some(&stop_rx), &mut proc);

                proc.verify(expected)
                    .unwrap_or_else(|err| panic!("unexpected log lines: {err}"));

                assert_eq!(
                    lf.offset, expected_offset,
                    "unexpected offset; got {}; want {}",
                    lf.offset, expected_offset
                );
                assert_eq!(
                    lf.commit_offset, expected_offset,
                    "unexpected commit_offset; got {}; want {}",
                    lf.commit_offset, expected_offset
                );
            };

            // Empty file
            f(&[], "", 0);

            // Empty lines
            let input = ["foo", "", "", "", "bar"];
            let expected = format!("{}\n", input.join("\n"));
            f(&input, &expected, expected.len() as i64);

            let input = ["foo"];
            f(&input, "foo\n", 4);

            let input = ["one", "two", "three"];
            let expected = format!("{}\n", input.join("\n"));
            f(&input, &expected, expected.len() as i64);

            // Lines with max line size
            let long_a = "a".repeat(MAX_LOG_LINE_SIZE);
            let input = [long_a.as_str()];
            let expected = format!("{}\n", input.join("\n"));
            f(&input, &expected, (MAX_LOG_LINE_SIZE + 1) as i64);

            // Lines with max line size in the middle
            let long_b = "b".repeat(MAX_LOG_LINE_SIZE);
            let input = ["foo", long_b.as_str(), "bar"];
            let expected = format!("{}\n", input.join("\n"));
            let offset = ("foo\n".len() + MAX_LOG_LINE_SIZE + 1 + "bar\n".len()) as i64;
            f(&input, &expected, offset);

            // Line exceeding max line size
            let long_b1 = "b".repeat(MAX_LOG_LINE_SIZE + 1);
            let input = ["foo", long_b1.as_str(), "bar"];
            let expected = "foo\nbar\n";
            let offset = ("foo\n".len() + MAX_LOG_LINE_SIZE + 1 + 1 + "bar\n".len()) as i64;
            f(&input, expected, offset);

            // Multiple lines exceeding max line size
            let long_c = "c".repeat(MAX_LOG_LINE_SIZE + 10);
            let long_d = "d".repeat(MAX_LOG_LINE_SIZE + 20);
            let input = ["foo", long_c.as_str(), long_d.as_str(), "bar"];
            let expected = "foo\nbar\n";
            let offset = ("foo\n".len()
                + MAX_LOG_LINE_SIZE
                + 10
                + 1
                + MAX_LOG_LINE_SIZE
                + 20
                + 1
                + "bar\n".len()) as i64;
            f(&input, expected, offset);

            // Very long line
            let long_e = "e".repeat(MAX_LOG_LINE_SIZE * 3);
            let input = [long_e.as_str(), "end"];
            let expected = "end\n";
            let offset = (MAX_LOG_LINE_SIZE * 3 + 1 + "end\n".len()) as i64;
            f(&input, expected, offset);
        }

        // Port of Go benchmarkReadLines (logfile_timing_test.go), as plain
        // single-pass tests instead of benchmarks.
        fn run_read_lines_timing(line_len: usize, count: usize) {
            let td = temp_dir();
            let log_file_path = td.path.join("test.log");
            let log_file_path = log_file_path.to_str().unwrap();
            let line = "a".repeat(line_len);
            let lines: Vec<&str> = (0..count).map(|_| line.as_str()).collect();
            write_lines_to_file(log_file_path, &lines);

            // Total bytes processed per iteration (includes newline).
            let total_bytes = ((line_len + 1) * count) as i64;

            struct NoopProcessor;
            impl Processor for NoopProcessor {
                fn try_add_line(&mut self, _line: &[u8]) -> bool {
                    true
                }
                fn flush(&mut self) {}
                fn must_close(&mut self) {}
            }
            let mut proc = NoopProcessor;

            let mut lf = new_log_file(log_file_path);
            for _ in 0..2 {
                lf.read_lines(None, &mut proc);
                assert_eq!(
                    lf.offset, total_bytes,
                    "unexpected offset; got {}; want {total_bytes}",
                    lf.offset
                );
                // Reset state to re-read the file from the beginning in the next iteration.
                lf.set_offset(0);
            }
            lf.close();
        }

        #[test]
        fn test_read_lines_big_size_lines() {
            // 10 MiB per pass: 1024 bytes per line (including newline), 10_240 lines.
            run_read_lines_timing(1023, 10_240);
        }

        #[test]
        fn test_read_lines_medium_size_lines() {
            // 10 MiB per pass: 512 bytes per line (including newline), 20_480 lines.
            run_read_lines_timing(511, 20_480);
        }

        #[test]
        fn test_read_lines_short_size_lines() {
            // 10 MiB per pass: 32 bytes per line (including newline), 327_680 lines.
            run_read_lines_timing(31, 327_680);
        }

        // rotate_rename_create rotates the current_file using "rename-create"
        // (aka "create" in logrotate) rotation method, and returns the new name
        // of the rotated log file.
        fn rotate_rename_create(current_file: &str) -> String {
            let current_file = try_resolve_symlink(current_file);
            let rotated_file = format!("{current_file}-{}", unique_suffix());

            fs::rename(&current_file, &rotated_file).expect("failed to rename log file");
            File::create(&current_file).expect("failed to create new log file");

            rotated_file
        }

        fn rotate_copy_truncate(current_file: &str) {
            let current_file = try_resolve_symlink(current_file);
            let rotated_file = format!("{current_file}-{}", unique_suffix());

            fs::copy(&current_file, &rotated_file).expect("failed to copy log file");

            let f = fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&current_file)
                .expect("failed to truncate log file");
            drop(f);
        }

        fn unique_suffix() -> String {
            static NEXT: AtomicI64 = AtomicI64::new(0);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            format!("{nanos}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
        }

        fn update_inode(filename: &str, old_inode: u64) -> u64 {
            let stat =
                must_stat(filename).unwrap_or_else(|| panic!("file {filename:?} does not exist"));
            let inode = get_inode(&stat);
            assert_ne!(
                old_inode, inode,
                "file {filename:?} already has inode {inode}"
            );
            inode
        }

        // Port of Go TestTailer.
        #[test]
        fn test_tailer() {
            let td = temp_dir();
            let checkpoints_path = td.path.join("checkpoints.json");
            let checkpoints_path = checkpoints_path.to_str().unwrap();
            let mut dirs = Vec::new();
            let (log_file_path, mut inode) = create_test_log_file(&mut dirs);

            let f = |result_expected: &str,
                     lines_expected: i64,
                     inode_expected: u64,
                     offset_expected: i64| {
                let tailer = start(checkpoints_path);

                let proc = new_test_processor(None);
                proc.expect(lines_expected);
                tailer.start_read(&log_file_path, Box::new(proc.clone()));
                proc.wait();

                tailer.stop();

                proc.verify(result_expected)
                    .unwrap_or_else(|err| panic!("unexpected error: {err}"));

                let cp_got = tailer
                    .checkpoints_db()
                    .get(&log_file_path)
                    .unwrap_or_else(|| panic!("checkpoint for {log_file_path:?} is missing"));

                assert_eq!(
                    cp_got.inode, inode_expected,
                    "unexpected inode in checkpoint; got {}; want {inode_expected}",
                    cp_got.inode
                );
                assert_eq!(
                    cp_got.offset, offset_expected,
                    "unexpected offset in checkpoint; got {}; want {offset_expected}",
                    cp_got.offset
                );
            };

            // Test that the tailer reads all log lines from the given log file.
            let result_expected = "line1\nline2\nline3\nline4\nline5\n";
            let lines_expected = 5;
            let mut offset_expected = result_expected.len() as i64;
            write_lines_to_file(&log_file_path, &[result_expected]);
            f(result_expected, lines_expected, inode, offset_expected);

            // Test that the tailer continues reading from the last read offset after restart.
            let result_expected = "line6\nline7\n";
            let lines_expected = 2;
            offset_expected += result_expected.len() as i64;
            write_lines_to_file(&log_file_path, &[result_expected]);
            f(result_expected, lines_expected, inode, offset_expected);

            // Verify 'rename-create' rotation: the tailer should detect the new log file
            // and successfully resume reading after a restart.
            write_lines_to_file(&log_file_path, &["1", "22"]);
            rotate_rename_create(&log_file_path);
            inode = update_inode(&log_file_path, inode);

            write_lines_to_file(&log_file_path, &["333"]);
            let result_expected = "1\n22\n333\n";
            let lines_expected = 3;
            offset_expected = "333\n".len() as i64;
            f(result_expected, lines_expected, inode, offset_expected);

            // Verify 'copy-truncate' rotation: the tailer should detect the truncation
            // and start reading the file from the beginning after a restart.
            write_lines_to_file(&log_file_path, &["foo", "bar"]);
            rotate_copy_truncate(&log_file_path);
            write_lines_to_file(&log_file_path, &["buz"]);
            // It's expected that 'foo' and 'bar' are lost by eslagent due to truncation.
            let result_expected = "buz\n";
            let lines_expected = 1;
            offset_expected = "buz\n".len() as i64;
            f(result_expected, lines_expected, inode, offset_expected);
        }

        // Port of Go TestHandleRotationRenameCreate: verifies that eslagent switches
        // to the new log file by tracking inode changes.
        #[test]
        fn test_handle_rotation_rename_create() {
            let td = temp_dir();
            let checkpoints_path = td.path.join("checkpoints.json");
            let mut dirs = Vec::new();
            let (log_file_path, _) = create_test_log_file(&mut dirs);

            let tailer = start(checkpoints_path.to_str().unwrap());

            let proc = new_test_processor(None);
            tailer.start_read(&log_file_path, Box::new(proc.clone()));

            for s in ["foo", "bar", "buz"] {
                proc.expect(1);
                write_lines_to_file(&log_file_path, &[s]);
                proc.wait();
            }

            let old_path = rotate_rename_create(&log_file_path);

            // Simulate a scenario where the log file was rotated, but the old file was
            // still appended to.
            for s in ["1", "2", "3"] {
                proc.expect(1);
                write_lines_to_file(&old_path, &[s]);
                proc.wait();
            }

            for s in ["Softalink LLC", "EsLogs", "EsTraces"] {
                proc.expect(1);
                write_lines_to_file(&log_file_path, &[s]);
                proc.wait();
            }

            let expected = "foo\nbar\nbuz\n1\n2\n3\nSoftalink LLC\nEsLogs\nEsTraces\n";
            proc.verify(expected)
                .unwrap_or_else(|err| panic!("unexpected error: {err}"));

            tailer.stop();
        }

        // Port of Go TestHandleRotationCopyTruncate: verifies that eslagent detects
        // log truncation by tracking file size reduction.
        #[test]
        fn test_handle_rotation_copy_truncate() {
            let td = temp_dir();
            let checkpoints_path = td.path.join("checkpoints.json");
            let mut dirs = Vec::new();
            let (log_file_path, _) = create_test_log_file(&mut dirs);

            let tailer = start(checkpoints_path.to_str().unwrap());

            let proc = new_test_processor(None);
            tailer.start_read(&log_file_path, Box::new(proc.clone()));

            for s in ["foo", "bar", "buz"] {
                proc.expect(1);
                write_lines_to_file(&log_file_path, &[s]);
                proc.wait();
            }

            rotate_copy_truncate(&log_file_path);

            for s in ["ping", "pong"] {
                proc.expect(1);
                write_lines_to_file(&log_file_path, &[s]);
                proc.wait();
            }

            let expected = "foo\nbar\nbuz\nping\npong\n";
            proc.verify(expected)
                .unwrap_or_else(|err| panic!("unexpected error: {err}"));

            tailer.stop();
        }

        // Port of Go TestCommitPartialLines.
        #[test]
        fn test_commit_partial_lines() {
            let td = temp_dir();
            let checkpoints_path = td.path.join("checkpoints.json");
            let checkpoints_path = checkpoints_path.to_str().unwrap();
            let mut dirs = Vec::new();
            let (log_file_path, inode) = create_test_log_file(&mut dirs);

            let f = |is_full: &[bool],
                     read_lines_expected: i64,
                     inode_expected: u64,
                     offset_expected: i64| {
                let is_full: Vec<bool> = is_full.to_vec();
                let i = Arc::new(Mutex::new(0usize));
                let commit_fn: CommitFn = Box::new(move |_line: &[u8]| {
                    let mut i = i.lock().unwrap();
                    let full = is_full[*i];
                    *i += 1;
                    full
                });

                let tailer = start(checkpoints_path);

                let proc = new_test_processor(Some(commit_fn));
                proc.expect(read_lines_expected);
                tailer.start_read(&log_file_path, Box::new(proc.clone()));
                proc.wait();

                tailer.stop();

                let cp_got = tailer
                    .checkpoints_db()
                    .get(&log_file_path)
                    .unwrap_or_else(|| panic!("checkpoint for {log_file_path:?} is missing"));

                assert_eq!(
                    cp_got.inode, inode_expected,
                    "unexpected inode in checkpoint; got {}; want {inode_expected}",
                    cp_got.inode
                );
                assert_eq!(
                    cp_got.offset, offset_expected,
                    "unexpected offset in checkpoint; got {}; want {offset_expected}",
                    cp_got.offset
                );
            };

            // Verify that the tailer commits only the full line to the CheckpointsDB.
            write_lines_to_file(
                &log_file_path,
                &[
                    "2025-10-16T15:37:36.1Z stderr F full line",
                    "2025-10-16T15:37:36.1Z stderr P foo",
                ],
            );
            let is_full = [true, false];
            let read_lines_expected = 2;
            let offset_expected = "2025-10-16T15:37:36.1Z stderr F full line\n".len() as i64;
            f(&is_full, read_lines_expected, inode, offset_expected);

            // Write another partial line to the rotated log file to ensure that the
            // tailer switches to the new file.
            rotate_rename_create(&log_file_path);
            let new_inode = update_inode(&log_file_path, inode);
            write_lines_to_file(&log_file_path, &["2025-10-16T15:37:36.1Z stderr P bar"]);
            let is_full = [false, false];
            let read_lines_expected = 2;
            f(&is_full, read_lines_expected, inode, offset_expected);

            // Write a final line to the rotated log file and verify that the tailer
            // commits the full line to the CheckpointsDB.
            write_lines_to_file(&log_file_path, &["2025-10-16T15:37:36.1Z stderr F buz"]);
            let read_lines_expected = 3;
            let is_full = [false, false, true];
            let offset_expected = ("2025-10-16T15:37:36.1Z stderr P bar\n".len()
                + "2025-10-16T15:37:36.1Z stderr F buz\n".len())
                as i64;
            f(&is_full, read_lines_expected, new_inode, offset_expected);
        }

        // Port of Go TestRestoringFromFingerprint.
        #[test]
        fn test_restoring_from_fingerprint() {
            let f = |file1: &str, file2: &str, out_expected: &str| {
                let td = temp_dir();
                let checkpoints_path = td.path.join("checkpoints.json");
                let checkpoints_path = checkpoints_path.to_str().unwrap();
                let mut dirs = Vec::new();
                let (log_file_path, _) = create_test_log_file(&mut dirs);

                let proc = new_test_processor(None);

                for s in [file1, file2] {
                    proc.expect(1);

                    let actual_path = try_resolve_symlink(&log_file_path);
                    let mut file = File::create(&actual_path).expect("failed to create log file");
                    write_to_file(&mut file, s);
                    file.sync_all().unwrap();
                    drop(file);

                    let tailer = start(checkpoints_path);

                    tailer.start_read(&log_file_path, Box::new(proc.clone()));
                    proc.wait();

                    tailer.stop();
                }

                proc.verify(out_expected)
                    .unwrap_or_else(|err| panic!("unexpected error: {err}"));
            };

            // The same fingerprints.
            let file1 = "2025-10-16T15:37:36.1Z stderr F foo\n";
            let file2 = format!("{file1}2025-10-16T15:37:36.2Z stderr F bar\n");
            f(file1, &file2, &file2);

            // The same fingerprints with empty lines.
            let file1 = "\n";
            let file2 = format!("{file1}\n");
            f(file1, &file2, &file2);

            // Different fingerprints.
            let file1 = "2025-10-16T15:37:36.3Z stderr F foo\n";
            let file2 = "2025-10-16T15:37:36.4Z stderr F bar\n";
            let expected = format!("{file1}{file2}");
            f(file1, file2, &expected);

            // Different fingerprints with empty lines.
            let file1 = "2025-10-16T15:37:36.5Z stderr F foo\n";
            let file2 = "\n";
            let expected = format!("{file1}{file2}");
            f(file1, file2, &expected);

            // Content length more than MAX_FINGERPRINT_DATA_LEN.
            let file1 =
                "2025-10-16T15:37:36.6Z stderr F foo bar buz 01234567890123456789001234567890\n";
            let file2 = "2025-10-16T15:37:36.7Z stderr F bar\n";
            let expected = format!("{file1}{file2}");
            f(file1, file2, &expected);

            // Content length exceeds MAX_LOG_LINE_SIZE.
            let file1 = format!(
                "2025-10-16T15:37:36.1Z stderr F {}\n2025-10-16T15:37:35.8Z stderr F foo\n",
                "a".repeat(MAX_LOG_LINE_SIZE)
            );
            let file2 = "2025-10-16T15:37:36.9Z stderr F bar\n";
            let expected =
                "2025-10-16T15:37:35.8Z stderr F foo\n2025-10-16T15:37:36.9Z stderr F bar\n";
            f(&file1, file2, expected);
        }
    }
}

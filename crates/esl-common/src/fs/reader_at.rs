//! Port of Softalink LLC `lib/fs/reader_at.go` (+ mincore_linux.go and
//! mincore_other.go).

use std::fs::File;
use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use crate::flagutil::Flag;
use crate::panicf;

static DISABLE_MMAP: Flag<bool> = Flag::new(
    "fs.disableMmap",
    "Whether to use pread() instead of mmap() for reading data files. \
By default, mmap() is used for 64-bit arches and pread() is used for 32-bit arches, since they cannot read data files bigger than 2^32 bytes in memory. \
mmap() is usually faster for reading small data chunks than pread()",
    || IS_32BIT_PTR,
);

static DISABLE_MINCORE: Flag<bool> = Flag::new(
    "fs.disableMincore",
    "Whether to disable the mincore() syscall for checking mmap()ed files. \
By default, mincore() is used to detect whether mmap()ed file pages are resident in memory. \
Disabling mincore() may be needed on older ZFS filesystems (below 2.1.5), since it may trigger ZFS bug. \
See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10327 for details.",
    || false,
);

// Disable mmap for architectures with 32-bit pointers in order to be able to work with files exceeding 2^32 bytes.
const IS_32BIT_PTR: bool = cfg!(target_pointer_width = "32");

/// MustReadAtCloser is rand-access read interface.
pub trait MustReadAtCloser {
    /// Must return path for the reader (e.g. file path, url or in-memory reference).
    fn path(&self) -> &str;

    /// Must read `p.len()` bytes from offset `off` to `p`.
    fn must_read_at(&self, p: &mut [u8], off: i64);

    /// Must close the reader.
    fn must_close(&mut self);
}

/// ReaderAt implements rand-access reader.
pub struct ReaderAt {
    read_calls: AtomicI64,
    read_bytes: AtomicI64,

    // path contains the path to the file for reading
    path: String,

    // mr is used for lazy opening of the file at path on the first access.
    mr: OnceLock<MmapReader>,

    use_local_stats: AtomicBool,
}

impl ReaderAt {
    /// Returns path to the reader.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Reads `p.len()` bytes at `off` from the reader.
    pub fn must_read_at(&self, p: &mut [u8], off: i64) {
        if p.is_empty() {
            return;
        }
        if off < 0 {
            panicf!("BUG: off={off} cannot be negative");
        }

        // Lazily open the file at self.path on the first access
        let mr = self.get_mmap_reader();

        // Read p.len() bytes at offset off to p.
        match &mr.mmap {
            None => mr.must_read_at_via_syscall(p, off),
            Some(mmap) => {
                let mmap_data: &[u8] = &mmap[..];
                if off > mmap_data.len() as i64 - p.len() as i64 {
                    panicf!(
                        "BUG: off={off} is out of allowed range [0...{}] for len(p)={} in file {:?}",
                        mmap_data.len() as i64 - p.len() as i64,
                        p.len(),
                        self.path
                    );
                }
                if mr.can_fast_read_via_mmap(off, p.len()) {
                    let off = off as usize;
                    p.copy_from_slice(&mmap_data[off..off + p.len()]);
                } else {
                    // Fall back for reading the data via syscall in order to avoid thread stalls
                    // described at https://valyala.medium.com/mmap-in-go-considered-harmful-d92a25cb161d
                    mr.must_read_at_via_syscall(p, off);
                }
            }
        }
        if self.use_local_stats.load(Ordering::SeqCst) {
            self.read_calls.fetch_add(1, Ordering::SeqCst);
            self.read_bytes.fetch_add(p.len() as i64, Ordering::SeqCst);
        } else {
            READ_CALLS.fetch_add(1, Ordering::SeqCst);
            READ_BYTES.fetch_add(p.len() as i64, Ordering::SeqCst);
        }
    }

    /// Returns a borrowed slice of the mmapped file at `[off, off+len)`, or
    /// `None` when the file is not mmapped (32-bit arches / -fs.disableMmap).
    ///
    /// Unlike `must_read_at` this does not copy and does not consult the
    /// mincore fast-read heuristic; it is meant for tiny random probes (e.g.
    /// bloom-filter word lookups) where copying the whole region to inspect a
    /// few bytes dominates the cost.
    pub fn mmap_slice(&self, off: i64, len: usize) -> Option<&[u8]> {
        if off < 0 {
            panicf!("BUG: off={off} cannot be negative");
        }
        let mr = self.get_mmap_reader();
        let mmap = mr.mmap.as_ref()?;
        let data: &[u8] = &mmap[..];
        let off = off as usize;
        if off + len > data.len() {
            panicf!(
                "BUG: off={off}+len={len} is out of allowed range [0...{}] in file {:?}",
                data.len(),
                self.path
            );
        }
        Some(&data[off..off + len])
    }

    fn get_mmap_reader(&self) -> &MmapReader {
        // OnceLock provides the same double-checked locking as the Go
        // atomic.Pointer + mutex combination.
        self.mr
            .get_or_init(|| MmapReader::new_from_path(&self.path))
    }

    /// Closes the reader.
    pub fn must_close(&mut self) {
        if let Some(mr) = self.mr.take() {
            mr.must_close();
        }

        if self.use_local_stats.load(Ordering::SeqCst) {
            READ_CALLS.fetch_add(self.read_calls.load(Ordering::SeqCst), Ordering::SeqCst);
            READ_BYTES.fetch_add(self.read_bytes.load(Ordering::SeqCst), Ordering::SeqCst);
            self.read_calls.store(0, Ordering::SeqCst);
            self.read_bytes.store(0, Ordering::SeqCst);
            self.use_local_stats.store(false, Ordering::SeqCst);
        }
    }

    /// Switches to local stats collection instead of global stats collection.
    ///
    /// This function must be called before the first call to must_read_at().
    ///
    /// Collecting local stats may improve performance on systems with big number of CPU cores,
    /// since the locally collected stats is pushed to global stats only at must_close() call
    /// instead of pushing it at every must_read_at call.
    pub fn set_use_local_stats(&self) {
        self.use_local_stats.store(true, Ordering::SeqCst);
    }

    /// Hints the OS that the underlying file is read mostly sequentially.
    ///
    /// If `prefetch` is set, then the OS is hinted to prefetch the file data.
    pub fn must_fadvise_sequential_read(&self, prefetch: bool) {
        let mr = self.get_mmap_reader();
        if let Err(err) = super::sys::fadvise_sequential_read(&mr.f, prefetch) {
            panicf!(
                "FATAL: error in fadviseSequentialRead({:?}, {}): {}",
                self.path,
                prefetch,
                err
            );
        }
    }
}

impl MustReadAtCloser for ReaderAt {
    fn path(&self) -> &str {
        ReaderAt::path(self)
    }
    fn must_read_at(&self, p: &mut [u8], off: i64) {
        ReaderAt::must_read_at(self, p, off)
    }
    fn must_close(&mut self) {
        ReaderAt::must_close(self)
    }
}

static READ_CALLS: AtomicI64 = AtomicI64::new(0);
static READ_BYTES: AtomicI64 = AtomicI64::new(0);
static READERS_COUNT: AtomicI64 = AtomicI64::new(0);

/// Opens ReaderAt for reading from the file located at path.
///
/// must_close() must be called on the returned ReaderAt when it is no longer needed.
pub fn must_open_reader_at(path: impl AsRef<Path>) -> ReaderAt {
    ReaderAt {
        read_calls: AtomicI64::new(0),
        read_bytes: AtomicI64::new(0),
        path: path.as_ref().to_string_lossy().into_owned(),
        mr: OnceLock::new(),
        use_local_stats: AtomicBool::new(false),
    }
}

/// Returns ReaderAt for reading from `f`.
///
/// new_reader_at takes ownership for `f`, so it shouldn't be closed by the caller.
///
/// must_close() must be called on the returned ReaderAt when it is no longer needed.
///
/// PORT NOTE: Go derives the path from f.Name(); Rust files don't carry their
/// name, so the caller passes the path explicitly.
pub fn new_reader_at(f: File, path: &str) -> ReaderAt {
    let mr = MmapReader::new_from_file(f, path);
    let r = ReaderAt {
        read_calls: AtomicI64::new(0),
        read_bytes: AtomicI64::new(0),
        path: path.to_string(),
        mr: OnceLock::new(),
        use_local_stats: AtomicBool::new(false),
    };
    let _ = r.mr.set(mr);
    r
}

struct MmapReader {
    f: File,
    mmap: Option<memmap2::Mmap>,

    // PORT NOTE: Go uses mr.f.Name() in error messages; the path is kept here
    // instead, since Rust files don't carry their name.
    path: String,

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    mincore_bits: Vec<AtomicU64>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    mincore_next_cleanup_timestamp: AtomicU64,
}

impl MmapReader {
    fn new_from_path(path: &str) -> MmapReader {
        let f = match File::open(path) {
            Ok(f) => f,
            Err(err) => {
                panicf!(
                    "FATAL: cannot open file for reading: open {path}: {err}; try increasing the limit on the number of open files via 'ulimit -n'"
                );
                unreachable!()
            }
        };
        MmapReader::new_from_file(f, path)
    }

    fn new_from_file(f: File, path: &str) -> MmapReader {
        let mut mmap = None;
        let mut mincore_bits = Vec::new();

        if !*DISABLE_MMAP.get() {
            let size = match f.metadata() {
                Ok(m) => m.len(),
                Err(err) => {
                    panicf!("FATAL: error in fstat({path:?}): {err}");
                    unreachable!()
                }
            };
            match mmap_file(&f, size) {
                Ok(m) => mmap = m,
                Err(err) => {
                    panicf!("FATAL: cannot mmap {path:?}: {err}");
                }
            }

            mincore_bits = make_mincore_bits(size);
        }

        READERS_COUNT.fetch_add(1, Ordering::SeqCst);
        MmapReader {
            f,
            mmap,
            path: path.to_string(),
            mincore_bits,
            mincore_next_cleanup_timestamp: AtomicU64::new(super::unix_timestamp() + 60),
        }
    }

    fn must_close(self) {
        // PORT NOTE: memmap2 unmaps on drop without an error channel; Go's
        // panic-on-munmap-failure cannot be reproduced.
        if self.mmap.is_some() {
            MMAPPED_FILES.fetch_sub(1, Ordering::SeqCst);
        }
        READERS_COUNT.fetch_sub(1, Ordering::SeqCst);
    }

    fn must_read_at_via_syscall(&self, p: &mut [u8], off: i64) {
        let mut n = 0usize;
        while n < p.len() {
            let pos = off as u64 + n as u64;
            let res = {
                #[cfg(unix)]
                {
                    std::os::unix::fs::FileExt::read_at(&self.f, &mut p[n..], pos)
                }
                #[cfg(windows)]
                {
                    std::os::windows::fs::FileExt::seek_read(&self.f, &mut p[n..], pos)
                }
            };
            match res {
                Ok(0) => {
                    panicf!(
                        "FATAL: cannot read {} bytes at offset {} of file {:?}: EOF",
                        p.len(),
                        off,
                        self.path
                    );
                }
                Ok(m) => n += m,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
                Err(err) => {
                    panicf!(
                        "FATAL: cannot read {} bytes at offset {} of file {:?}: {}",
                        p.len(),
                        off,
                        self.path,
                        err
                    );
                }
            }
        }

        if self.mmap.is_none() || !has_mincore() {
            return;
        }

        // Mark the just read data as available for fast read via mmap
        #[cfg(target_os = "linux")]
        {
            let page_size = page_size_bytes();

            let end = off + n as i64;
            let mut off = off - (off as u64 % page_size) as i64;
            let mut page_idx = off as u64 / page_size;
            while off < end {
                let word_idx = (page_idx / 64) as usize;
                let bit_idx = page_idx % 64;
                let mask = 1u64 << bit_idx;
                let word_ptr = &self.mincore_bits[word_idx];
                let mut word = word_ptr.load(Ordering::SeqCst);
                while (word & mask) == 0 {
                    match word_ptr.compare_exchange(
                        word,
                        word | mask,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(w) => word = w,
                    }
                }

                off += page_size as i64;
                page_idx += 1;
            }
        }
    }

    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
    fn can_fast_read_via_mmap(&self, off: i64, n: usize) -> bool {
        if !has_mincore() {
            return true;
        }

        #[cfg(target_os = "linux")]
        {
            let page_size = page_size_bytes();

            let ct = super::unix_timestamp();
            let next_cleanup = self.mincore_next_cleanup_timestamp.load(Ordering::SeqCst);
            if ct > next_cleanup
                && self
                    .mincore_next_cleanup_timestamp
                    .compare_exchange(next_cleanup, ct + 60, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                for word in &self.mincore_bits {
                    word.store(0, Ordering::SeqCst);
                }
            }

            let mmap_data: &[u8] = self.mmap.as_ref().expect("BUG: mmap must be set");

            let end = off + n as i64;
            let mut off = off - (off as u64 % page_size) as i64;
            let mut page_idx = off as u64 / page_size;
            while off < end {
                let word_idx = (page_idx / 64) as usize;
                let bit_idx = page_idx % 64;
                let mask = 1u64 << bit_idx;
                let word_ptr = &self.mincore_bits[word_idx];
                let mut word = word_ptr.load(Ordering::SeqCst);
                if (word & mask) == 0 {
                    if !mincore(&mmap_data[off as usize]) {
                        return false;
                    }
                    while (word & mask) == 0 {
                        match word_ptr.compare_exchange(
                            word,
                            word | mask,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => break,
                            Err(w) => word = w,
                        }
                    }
                }

                off += page_size as i64;
                page_idx += 1;
            }

            true
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Mirrors the mincore_other.go stub, which must never be reached
            // since supports_mincore() is false on non-Linux systems.
            panic!("BUG: unexpected call");
        }
    }
}

fn make_mincore_bits(size: u64) -> Vec<AtomicU64> {
    #[cfg(target_os = "linux")]
    if has_mincore() {
        let page_size = page_size_bytes();
        let mincore_bits_size = size.div_ceil(page_size).div_ceil(64);
        return (0..mincore_bits_size).map(|_| AtomicU64::new(0)).collect();
    }
    #[cfg(not(target_os = "linux"))]
    let _ = size;
    Vec::new()
}

#[cfg(target_os = "linux")]
fn page_size_bytes() -> u64 {
    use std::sync::LazyLock;
    static PAGE_SIZE: LazyLock<u64> = LazyLock::new(|| {
        let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if ps > 0 { ps as u64 } else { 4096 }
    });
    *PAGE_SIZE
}

fn mmap_file(f: &File, size: u64) -> Result<Option<memmap2::Mmap>, String> {
    if size == 0 {
        return Ok(None);
    }
    if size > isize::MAX as u64 {
        return Err(format!("file is too big to be memory mapped: {size} bytes"));
    }
    // PORT NOTE: Go rounds the mapping up to a 4KiB multiple to protect
    // against Go's copy() reading beyond src bounds (SIGBUS); Rust slice
    // copies never read out of bounds, so memmap2 maps the exact file size.
    match unsafe { memmap2::MmapOptions::new().len(size as usize).map(f) } {
        Ok(m) => {
            MMAPPED_FILES.fetch_add(1, Ordering::SeqCst);
            Ok(Some(m))
        }
        Err(err) => Err(format!(
            "cannot mmap file with size {size} bytes; already memory mapped files: {}: {err}; \
try increasing /proc/sys/vm/max_map_count or passing -fs.disableMmap command-line flag to the application",
            MMAPPED_FILES.load(Ordering::SeqCst)
        )),
    }
}

static MMAPPED_FILES: AtomicI64 = AtomicI64::new(0);

fn has_mincore() -> bool {
    supports_mincore() && !*DISABLE_MINCORE.get()
}

fn supports_mincore() -> bool {
    cfg!(target_os = "linux")
}

#[cfg(target_os = "linux")]
fn mincore(ptr: &u8) -> bool {
    let mut result = [0u8; 1];
    let rc = unsafe {
        libc::mincore(
            ptr as *const u8 as *mut libc::c_void,
            1,
            result.as_mut_ptr(),
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        panicf!(
            "FATAL: cannot call mincore(ptr={:p}, 1): {err}",
            ptr as *const u8
        );
    }
    (result[0] & 1) == 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reader_at() {
        for buf_size in [1usize, 10, 100, 1_000, 10_000, 100_000] {
            test_reader_at_with_buf_size(buf_size);
        }
    }

    fn test_reader_at_with_buf_size(buf_size: usize) {
        let dir = crate::fs::test_temp_dir("reader_at");
        let path = dir.join("TestReaderAt");
        const FILE_SIZE: usize = 8 * 1024 * 1024;
        let data = vec![0u8; FILE_SIZE];
        crate::fs::must_write_sync(&path, &data);
        let mut r = must_open_reader_at(&path);

        let mut buf = vec![0u8; buf_size];
        let mut i = 0usize;
        while i < FILE_SIZE - buf_size {
            let offset = i as i64;
            r.must_read_at(&mut buf[..0], offset);
            r.must_read_at(&mut buf, offset);
            i += buf_size;
        }

        r.must_close();
        crate::fs::must_remove_path(&path);
        std::fs::remove_dir_all(&dir).ok();
    }
}

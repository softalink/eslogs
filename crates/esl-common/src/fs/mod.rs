//! Port of Softalink LLC `lib/fs` (with the `lib/fs/fsutil` subpackage in
//! [`fsutil`]).
//!
//! PORT NOTE: metrics registration (`RegisterPathFsMetrics`, `getFsType` and
//! the `vm_fs_*`/`vm_nfs_*`/`vm_mmapped_files` metrics) is not wired to the
//! `esl_common::metrics` registry: the equivalent disk-space series are
//! emitted by `esl-storage`'s `write_storage_metrics`, and the remaining
//! series are Go-runtime/NFS-workaround diagnostics. Counters whose values
//! appear in user-visible error messages are kept as private atomics.
//!
//! PORT NOTE: Go panics when `File.Close()` fails; Rust closes files on drop
//! without reporting errors, so those panics cannot be reproduced.

pub mod fsutil;

mod dir_remover;
mod parallel;
mod reader_at;
mod sys;

pub use dir_remover::{
    is_partially_removed_dir, must_remove_dir, must_remove_dir_contents, must_remove_path,
    must_stop_dir_remover,
};
pub use parallel::{MustCloser, ParallelReaderAtOpener, must_close_parallel};
pub use reader_at::{MustReadAtCloser, ReaderAt, must_open_reader_at, new_reader_at};

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

use crate::{filestream, panicf};

static TMP_FILE_NUM: AtomicU64 = AtomicU64::new(0);

// PORT NOTE: lib/fasttime is ported separately; this private helper computes
// the unix timestamp directly instead of using fasttime's cached clock.
pub(crate) fn unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Fsyncs the path and the parent dir.
///
/// This guarantees that the path is visible and readable after unclean shutdown.
pub fn must_sync_path_and_parent_dir(path: impl AsRef<Path>) {
    let path = path.as_ref();
    must_sync_path(path);
    // filepath.Dir semantics: the parent of a bare filename is ".".
    let parent_dir_path = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    must_sync_path(parent_dir_path);
}

/// Syncs contents of the given path.
pub fn must_sync_path(path: impl AsRef<Path>) {
    let path = path.as_ref();
    if fsutil::is_fsync_disabled() {
        // Just check that the path exists
        if !is_path_exist(path) {
            panicf!("FATAL: cannot fsync missing {path:?}");
        }
        return;
    }
    sys::must_sync_path_os(path);
}

/// Writes data to the file at path and then calls fsync on the created file.
///
/// The fsync guarantees that the written data survives hardware reset after successful call.
///
/// This function may leave the file at the path in inconsistent state on app crash
/// in the middle of the write.
/// Use must_write_atomic if the file at the path must be either written in full
/// or not written at all on app crash in the middle of the write.
pub fn must_write_sync(path: impl AsRef<Path>, data: &[u8]) {
    let path = path.as_ref();
    let mut f = filestream::must_create(path, false);
    if let Err(err) = filestream::WriteCloser::write(&mut f, data) {
        f.must_close();
        // Do not call must_remove_path(path), so the user could inspect
        // the file contents during investigation of the issue.
        panicf!(
            "FATAL: cannot write {} bytes to {path:?}: {err}",
            data.len()
        );
    }
    f.must_close();
}

/// Atomically writes data to the given file path.
///
/// This function returns only after the file is fully written and synced
/// to the underlying storage.
///
/// This function guarantees that the file at path either fully written or not written at all on app crash
/// in the middle of the write.
///
/// If the file at path already exists, then the file is overwritten atomically if can_overwrite is true.
/// Otherwise, this function panics.
pub fn must_write_atomic(path: impl AsRef<Path>, data: &[u8], can_overwrite: bool) {
    let path = path.as_ref();
    // Check for the existing file. It is expected that
    // the must_write_atomic function cannot be called concurrently
    // with the same `path`.
    if is_path_exist(path) && !can_overwrite {
        panicf!("FATAL: cannot create file {path:?}, since it already exists");
    }

    // Write data to a temporary file.
    let n = TMP_FILE_NUM.fetch_add(1, Ordering::SeqCst) + 1;
    let tmp_path = PathBuf::from(format!("{}.tmp.{n}", path.display()));
    must_write_sync(&tmp_path, data);

    // Atomically move the temporary file from tmp_path to path.
    if let Err(err) = std::fs::rename(&tmp_path, path) {
        // do not call must_remove_path(tmp_path) here, so the user could inspect
        // the file contents during investigation of the issue.
        panicf!("FATAL: cannot move temporary file {tmp_path:?} to {path:?}: {err}");
    }

    // Sync the containing directory, so the file is guaranteed to appear in the directory.
    // See https://www.quora.com/When-should-you-fsync-the-containing-directory-in-addition-to-the-file-itself
    let abs_path = match std::path::absolute(path) {
        Ok(p) => p,
        Err(err) => {
            panicf!("FATAL: cannot obtain absolute path to {path:?}: {err}");
            unreachable!()
        }
    };
    let parent_dir_path = abs_path.parent().unwrap_or(&abs_path);
    must_sync_path(parent_dir_path);
}

/// Returns true if `filename` matches the temporary file name pattern
/// from must_write_atomic.
pub fn is_temporary_file_name(filename: &str) -> bool {
    TMP_FILE_NAME_RE.is_match(filename)
}

// TMP_FILE_NAME_RE is regexp for temporary file name - see must_write_atomic for details.
static TMP_FILE_NAME_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"\.tmp\.\d+$").unwrap());

/// Creates the given path dir if it isn't exist.
///
/// The caller is responsible for must_sync_path() call for the parent directory for the path.
pub fn must_mkdir_if_not_exist(path: impl AsRef<Path>) {
    let path = path.as_ref();
    if is_path_exist(path) {
        return;
    }
    must_mkdir(path);
}

/// Creates the given path dir if it isn't exist.
///
/// If the directory at the given path already exists, then the function logs the fatal error and exits the process.
///
/// The caller is responsible for must_sync_path() call for the parent directory for the path.
pub fn must_mkdir_fail_if_exist(path: impl AsRef<Path>) {
    let path = path.as_ref();
    if is_path_exist(path) {
        panicf!("FATAL: the {path:?} already exists");
    }
    must_mkdir(path);
}

fn must_mkdir(path: &Path) {
    // PORT NOTE: Go creates the dirs with mode 0755; std::fs::create_dir_all
    // uses 0777 & umask, which yields 0755 under the default umask 022.
    if let Err(err) = std::fs::create_dir_all(path) {
        panicf!(
            "FATAL: cannot create directory: mkdir {}: {err}",
            path.display()
        );
    }
    // Do not sync the parent directory - this is the responsibility of the caller.
}

/// Returns file size for the given path.
pub fn must_file_size(path: impl AsRef<Path>) -> u64 {
    let path = path.as_ref();
    let fi = match std::fs::metadata(path) {
        Ok(fi) => fi,
        Err(err) => {
            panicf!("FATAL: cannot stat {path:?}: {err}");
            unreachable!()
        }
    };
    if fi.is_dir() {
        panicf!("FATAL: {path:?} must be a file, not a directory");
    }
    fi.len()
}

/// Returns whether the given path exists.
pub fn is_path_exist(path: impl AsRef<Path>) -> bool {
    let path = path.as_ref();
    match std::fs::metadata(path) {
        Ok(_) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(err) => {
            panicf!("FATAL: cannot stat {path:?}: {err}");
            unreachable!()
        }
    }
}

/// Reads directory entries at the given dir.
///
/// PORT NOTE: like Go's os.ReadDir, the entries are sorted by filename
/// (std::fs::read_dir returns them in arbitrary order).
pub fn must_read_dir(dir: impl AsRef<Path>) -> Vec<std::fs::DirEntry> {
    let dir = dir.as_ref();
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(err) => {
            panicf!(
                "FATAL: cannot read directory contents: open {}: {err}",
                dir.display()
            );
            unreachable!()
        }
    };
    let mut des = Vec::new();
    for de in rd {
        match de {
            Ok(de) => des.push(de),
            Err(err) => {
                panicf!(
                    "FATAL: cannot read directory contents: open {}: {err}",
                    dir.display()
                );
            }
        }
    }
    des.sort_by_key(|de| de.file_name());
    des
}

/// Creates dst_dir and makes hard links for all the files from src_dir in dst_dir.
///
/// The caller is responsible for calling must_sync_path for the parent directory of dst_dir.
pub fn must_hard_link_files(src_dir: impl AsRef<Path>, dst_dir: impl AsRef<Path>) {
    let src_dir = src_dir.as_ref();
    let dst_dir = dst_dir.as_ref();
    must_mkdir(dst_dir);

    let des = must_read_dir(src_dir);
    for de in &des {
        if is_dir_or_symlink(de) {
            // Skip directories.
            continue;
        }
        let name = de.file_name();
        let src_path = src_dir.join(&name);
        let dst_path = dst_dir.join(&name);
        if let Err(err) = std::fs::hard_link(&src_path, &dst_path) {
            panicf!("FATAL: cannot link files: {err}");
        }
    }

    must_sync_path(dst_dir);
}

/// Creates relative symlink for src_path in dst_path.
///
/// The caller is responsible for calling must_sync_path() for the parent directory of dst_path.
pub fn must_symlink_relative(src_path: impl AsRef<Path>, dst_path: impl AsRef<Path>) {
    let src_path = src_path.as_ref();
    let dst_path = dst_path.as_ref();
    let base_dir = match dst_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let src_path_rel = match rel_path(base_dir, src_path) {
        Ok(p) => p,
        Err(err) => {
            panicf!("FATAL: cannot make relative path for srcPath={src_path:?}: {err}");
            unreachable!()
        }
    };
    let res = {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&src_path_rel, dst_path)
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_file(&src_path_rel, dst_path)
        }
    };
    if let Err(err) = res {
        panicf!("FATAL: cannot make a symlink: {err}");
    }
}

// filepath.Rel equivalent for the cases used by must_symlink_relative.
fn rel_path(base: &Path, target: &Path) -> Result<PathBuf, String> {
    use std::path::Component;
    if base.is_absolute() != target.is_absolute() {
        return Err(format!(
            "Rel: can't make {} relative to {}",
            target.display(),
            base.display()
        ));
    }
    let b: Vec<Component<'_>> = base
        .components()
        .filter(|c| !matches!(c, Component::CurDir))
        .collect();
    let t: Vec<Component<'_>> = target
        .components()
        .filter(|c| !matches!(c, Component::CurDir))
        .collect();
    let mut i = 0;
    while i < b.len() && i < t.len() && b[i] == t[i] {
        i += 1;
    }
    let mut out = PathBuf::new();
    for c in &b[i..] {
        if matches!(c, Component::ParentDir) {
            return Err(format!(
                "Rel: can't make {} relative to {}",
                target.display(),
                base.display()
            ));
        }
        out.push("..");
    }
    for c in &t[i..] {
        out.push(c);
    }
    if out.as_os_str().is_empty() {
        out.push(".");
    }
    Ok(out)
}

/// Creates dst_path and copies all the files in src_path to dst_path.
///
/// The caller is responsible for calling must_sync_path() for the parent directory of dst_path.
pub fn must_copy_directory(src_path: impl AsRef<Path>, dst_path: impl AsRef<Path>) {
    let src_path = src_path.as_ref();
    let dst_path = dst_path.as_ref();
    must_mkdir(dst_path);

    let des = must_read_dir(src_path);
    for de in &des {
        if !de.file_type().is_ok_and(|ft| ft.is_file()) {
            // Skip non-files
            continue;
        }
        let src = src_path.join(de.file_name());
        let dst = dst_path.join(de.file_name());
        must_copy_file(&src, &dst);
    }

    must_sync_path(dst_path);
}

/// Copies the file from src_path to dst_path.
pub fn must_copy_file(src_path: impl AsRef<Path>, dst_path: impl AsRef<Path>) {
    let src_path = src_path.as_ref();
    let dst_path = dst_path.as_ref();
    let mut src = match File::open(src_path) {
        Ok(f) => f,
        Err(err) => {
            panicf!(
                "FATAL: cannot open srcPath: open {}: {err}",
                src_path.display()
            );
            unreachable!()
        }
    };
    let mut dst = match File::create(dst_path) {
        Ok(f) => f,
        Err(err) => {
            panicf!(
                "FATAL: cannot create dstPath: open {}: {err}",
                dst_path.display()
            );
            unreachable!()
        }
    };
    if let Err(err) = std::io::copy(&mut src, &mut dst) {
        panicf!("FATAL: cannot copy {src_path:?} to {dst_path:?}: {err}");
    }
    must_sync_path(dst_path);
}

/// Reads `data.len()` bytes from r.
pub fn must_read_data<R: filestream::ReadCloser + ?Sized>(r: &mut R, data: &mut [u8]) {
    // io.ReadFull semantics: returns io.EOF (and MustReadData returns silently)
    // only if nothing was read; a partial read panics.
    let mut n = 0usize;
    while n < data.len() {
        match r.read(&mut data[n..]) {
            Ok(0) => {
                if n == 0 {
                    return;
                }
                panicf!(
                    "FATAL: cannot read {} bytes from {}; read only {} bytes; error: unexpected EOF",
                    data.len(),
                    r.path(),
                    n
                );
            }
            Ok(m) => n += m,
            Err(err) => {
                panicf!(
                    "FATAL: cannot read {} bytes from {}; read only {} bytes; error: {}",
                    data.len(),
                    r.path(),
                    n,
                    err
                );
            }
        }
    }
}

/// Writes data to w.
pub fn must_write_data<W: filestream::WriteCloser + ?Sized>(w: &mut W, data: &[u8]) {
    if data.is_empty() {
        return;
    }
    match w.write(data) {
        Err(err) => {
            panicf!(
                "FATAL: cannot write {} bytes to {}: {}",
                data.len(),
                w.path(),
                err
            );
        }
        Ok(n) if n != data.len() => {
            panicf!(
                "BUG: writer wrote {} bytes instead of {} bytes to {}",
                n,
                data.len(),
                w.path()
            );
        }
        Ok(_) => {}
    }
}

/// Creates FLOCK_FILENAME file in the directory dir
/// and returns the handle to the file.
pub fn must_create_flock_file(dir: impl AsRef<Path>) -> File {
    let dir = dir.as_ref();
    let flock_filepath = dir.join(FLOCK_FILENAME);
    match sys::create_flock_file(&flock_filepath) {
        Ok(f) => f,
        Err(err) => {
            panicf!(
                "FATAL: cannot create lock file: {err}; make sure a single process has exclusive access to {dir:?}"
            );
            unreachable!()
        }
    }
}

/// The filename for the file created by must_create_flock_file().
pub const FLOCK_FILENAME: &str = "flock.lock";

/// Returns free space for the given directory path.
pub fn must_get_free_space(path: impl AsRef<Path>) -> u64 {
    update_disk_space_cached(path.as_ref()).free
}

/// Returns the total disk space for the given directory path.
pub fn must_get_total_space(path: impl AsRef<Path>) -> u64 {
    update_disk_space_cached(path.as_ref()).total
}

fn update_disk_space_cached(path: &Path) -> DiskSpaceEntry {
    // Try obtaining cached value at first.
    let mut map = DISK_SPACE_MAP.lock().unwrap();
    if let Some(e) = map.get(path)
        && unix_timestamp().wrapping_sub(e.update_time) < 2
    {
        // Fast path - the entry is fresh.
        return *e;
    }

    // Slow path.
    // Determine the amount of disk space at path.
    let (total, free) = sys::must_get_disk_space(path);
    let e = DiskSpaceEntry {
        update_time: unix_timestamp(),
        free,
        total,
    };
    map.insert(path.to_path_buf(), e);
    e
}

static DISK_SPACE_MAP: LazyLock<Mutex<HashMap<PathBuf, DiskSpaceEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Copy)]
struct DiskSpaceEntry {
    update_time: u64,
    free: u64,
    total: u64,
}

/// Returns true if de is directory or symlink.
pub fn is_dir_or_symlink(de: &std::fs::DirEntry) -> bool {
    let ft = match de.file_type() {
        Ok(ft) => ft,
        Err(err) => {
            panicf!("FATAL: cannot stat {:?}: {err}", de.path());
            unreachable!()
        }
    };
    ft.is_dir() || ft.is_symlink()
}

#[cfg(test)]
pub(crate) fn test_temp_dir(name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "esl-common-test-{name}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_temporary_file_name() {
        let f = |s: &str, result_expected: bool| {
            let result = is_temporary_file_name(s);
            assert_eq!(
                result, result_expected,
                "unexpected IsTemporaryFileName({s:?}); got {result}; want {result_expected}"
            );
        };
        f("", false);
        f(".", false);
        f(".tmp", false);
        f("tmp.123", false);
        f(".tmp.123.xx", false);
        f(".tmp.1", true);
        f("asdf.dff.tmp.123", true);
        f("asdf.sdfds.tmp.dfd", false);
        f("dfd.sdfds.dfds.1232", false);
    }
}

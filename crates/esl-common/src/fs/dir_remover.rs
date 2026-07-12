//! Port of Softalink LLC `lib/fs/dir_remover.go`.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use super::{is_path_exist, must_read_dir, must_sync_path};
use crate::{errorf, fatalf, panicf};

// directories with this filename are scheduled to be removed by must_remove_dir().
pub(crate) const DELETE_DIR_FILENAME: &str = ".delete-this-dir";

/// Removes the dir_path with all its contents.
///
/// The dir_path contents may be partially deleted if unclean shutdown happens during the removal.
/// The caller must verify whether the given directory is partially removed via is_partially_removed_dir() call
/// on the startup before using it. If the directory is partially removed, it must be removed again
/// via must_remove_dir() call.
pub fn must_remove_dir(dir_path: impl AsRef<Path>) {
    let dir_path = dir_path.as_ref();
    if !is_path_exist(dir_path) {
        // Nothing do delete.
        return;
    }

    // The code below is written in the way that partially deleted directories could be deleted
    // on the next start after unclean shutdown, by verifying them with is_partially_removed_dir() call.
    //
    // The code below doesn't depend on atomic renaming of directories, since it isn't supported
    // by NFS and object storage.

    // Create a DELETE_DIR_FILENAME file, which indicates that the dir_path must be removed.
    let delete_file_path = dir_path.join(DELETE_DIR_FILENAME);
    match std::fs::File::create(&delete_file_path) {
        Ok(f) => drop(f),
        Err(err) => {
            panicf!("FATAL: cannot create {delete_file_path:?} while deleting {dir_path:?}: {err}");
        }
    }

    // Make sure the DELETE_DIR_FILENAME file is visible in the dir_path.
    must_sync_path(dir_path);

    // Remove the contents of the dir_path except of the DELETE_DIR_FILENAME file.
    //
    // Make this in parallel in order to reduce the time needed for the removal of big number of items
    // on high-latency storage systems such as NFS.
    // Directories for VitoriaLogs parts may contain big number of items when wide events are stored there.
    // Also the number of parts in a partition may be quite big.

    if try_remove_dir(dir_path) {
        return;
    }

    // schedule NFS background dir removal.
    // NFS may perform "silly rename" before deletion, if client detects more than 1 file reference.
    // Silly rename is async operation and client may take an additional time before
    // unlink operation will succeed and could be actually deleted.
    {
        let mut pending = DIR_REMOVER.pending.lock().unwrap();
        if *pending >= REMOVE_DIR_QUEUE_LIMIT {
            panicf!(
                "FATAL: cannot schedule {} for removal, since the removal queue is full ({REMOVE_DIR_QUEUE_LIMIT} entries)",
                dir_path.display()
            );
        }
        *pending += 1;
    }
    let dir_path = dir_path.to_path_buf();
    let res = std::thread::Builder::new()
        .name("dirRemover".to_string())
        .spawn(move || {
            loop {
                if try_remove_dir(&dir_path) {
                    break;
                }
                std::thread::sleep(Duration::from_secs(1));
            }
            let mut pending = DIR_REMOVER.pending.lock().unwrap();
            *pending -= 1;
            DIR_REMOVER.cv.notify_all();
        });
    if let Err(err) = res {
        panicf!("FATAL: cannot start dirRemover thread: {err}");
    }
}

/// Returns true if dir_path is partially removed because of unclean shutdown during the must_remove_dir() call.
///
/// The caller must call must_remove_dir(dir_path) on partially removed dir_path.
pub fn is_partially_removed_dir(dir_path: impl AsRef<Path>) -> bool {
    let des = must_read_dir(dir_path.as_ref());
    if des.is_empty() {
        // Delete empty dirs too, since they may appear when the unclean shutdown happens after the DELETE_DIR_FILENAME is deleted,
        // but before the directory is deleted itself.
        return true;
    }

    for de in &des {
        if de.file_type().is_ok_and(|ft| ft.is_dir()) {
            continue;
        }
        let name = de.file_name();
        if name == DELETE_DIR_FILENAME {
            // The directory contains the DELETE_DIR_FILENAME. This means it is partially deleted.
            return true;
        }
    }
    false
}

/// Removes the given path. It must be either a file or an empty directory.
///
/// Use must_remove_dir for removing non-empty directories.
pub fn must_remove_path(path: impl AsRef<Path>) {
    let path = path.as_ref();
    // PORT NOTE: Go's os.Remove tries unlink() and falls back to rmdir();
    // Rust splits these into remove_file/remove_dir.
    if std::fs::remove_file(path).is_ok() {
        return;
    }
    if let Err(err) = std::fs::remove_dir(path) {
        panicf!("FATAL: cannot remove {path:?}: {err}");
    }
}

/// Removes all the contents of the given dir if it exists.
///
/// It doesn't remove the dir itself, so the dir may be mounted to a separate partition.
pub fn must_remove_dir_contents(dir: impl AsRef<Path>) {
    let dir = dir.as_ref();
    if !is_path_exist(dir) {
        // The path doesn't exist, so nothing to remove.
        return;
    }

    let des = must_read_dir(dir);
    for de in des {
        let full_path = dir.join(de.file_name());
        if let Err(err) = remove_all(&full_path) {
            panicf!("FATAL: cannot remove {}: {err}", full_path.display());
        }
    }
    must_sync_path(dir);
}

// os.RemoveAll equivalent: removes path and any children it contains;
// returns Ok(()) if the path doesn't exist.
fn remove_all(path: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
        Ok(m) if m.is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
    }
}

/// Attempts to remove a directory and returns true if the removal succeeded.
///
/// The function returns false if:
///
///  1. it detects .nfsXXXX files in inside dir_path. Since the creation of such
///     files is a valid NFS behavior, the function does not attempt to delete
///     them and lets the NFS do it gracefully (i.e. remove them when they are no
///     longer needed). See
///     <https://wiki.linux-nfs.org/wiki/index.php/Server-side_silly_rename>
///  2. OR there has been a temprorary NFS error while deleting a dir entry.
///
/// Returning false indicates that the caller should retry.
///
/// Finally, the function panics in case of any other fs operation failure.
pub(crate) fn try_remove_dir(dir_path: &Path) -> bool {
    let des = must_read_dir(dir_path);
    let must_retry = AtomicBool::new(false);
    // PORT NOTE: Go spawns one goroutine per entry gated by a channel with
    // workerCount slots; a fixed pool of workerCount threads consuming a
    // shared index gives the same bounded concurrency.
    let worker_count = des.len().clamp(1, 32);
    let mut entry_paths = Vec::with_capacity(des.len());
    for de in &des {
        let name = de.file_name();
        if name == DELETE_DIR_FILENAME {
            continue;
        }
        if name.to_string_lossy().starts_with(".nfs") {
            must_retry.store(true, Ordering::SeqCst);
            continue;
        }
        entry_paths.push(dir_path.join(name));
    }
    if !entry_paths.is_empty() {
        let next_idx = AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..worker_count.min(entry_paths.len()) {
                s.spawn(|| {
                    loop {
                        let i = next_idx.fetch_add(1, Ordering::SeqCst);
                        let Some(dir_entry_path) = entry_paths.get(i) else {
                            break;
                        };
                        // remove_all may create stale NFS files with .nfs prefix after
                        // dirEntry removal. So it's required to perform an additional check
                        // later with has_stale_nfs_files(). It could happen if NFS client
                        // detects multiple file descriptors that point to the same inode.
                        //
                        // While it's expected for storage process to open a file multiple
                        // times simultaneously and properly close it, fs caching may still
                        // confuse NFS client.
                        if let Err(err) = remove_all(dir_entry_path) {
                            if !is_temporary_nfs_error(&err) {
                                fatalf!("FATAL: cannot remove {dir_entry_path:?}: {err}");
                            }
                            must_retry.store(true, Ordering::SeqCst);
                        }
                    }
                });
            }
        });
    }
    if must_retry.load(Ordering::SeqCst) {
        NFS_DIR_REMOVE_FAILED_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        return false;
    }
    // Make sure the deleted names are properly synced to the dir_path,
    // so they are no longer visible after unclean shutdown.
    must_sync_path(dir_path);

    // New stale NFS files may have appeared since the loop
    if has_stale_nfs_files(dir_path) {
        NFS_DIR_REMOVE_FAILED_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        return false;
    }

    let delete_file_path = dir_path.join(DELETE_DIR_FILENAME);
    // Remove the DELETE_DIR_FILENAME file, since there are no other entries left in the directory.
    must_remove_path(&delete_file_path);

    // Sync the directory after the removing deletDirFilename file in order to make sure
    // all the metadata files are removed at some exotic filesystems such as OSSFS2.
    // See https://github.com/VictoriaMetrics/VictoriaLogs/issues/649
    // and https://github.com/VictoriaMetrics/VictoriaMetrics/pull/9709
    must_sync_path(dir_path);

    // Remove the dir_path itself
    must_remove_path(dir_path);

    // Do not sync the parent directory for the dir_path - the caller can do this if needed.
    // It is OK if the dir_path will remain undeleted after unclean shutdown - it will be deleted
    // on the next startup.

    true
}

struct DirRemoverState {
    pending: Mutex<usize>,
    cv: Condvar,
}

static DIR_REMOVER: DirRemoverState = DirRemoverState {
    pending: Mutex::new(0),
    cv: Condvar::new(),
};

static NFS_DIR_REMOVE_FAILED_ATTEMPTS: AtomicU64 = AtomicU64::new(0);

const REMOVE_DIR_QUEUE_LIMIT: usize = 1024;

/// Must be called in the end of graceful shutdown
/// in order to wait for removing the remaining directories scheduled for background removal.
///
/// It is expected that nobody calls must_remove_dir when must_stop_dir_remover is called.
pub fn must_stop_dir_remover() {
    const MAX_WAIT_TIME: Duration = Duration::from_secs(10);
    let deadline = Instant::now() + MAX_WAIT_TIME;
    let mut pending = DIR_REMOVER.pending.lock().unwrap();
    while *pending > 0 {
        let now = Instant::now();
        if now >= deadline {
            errorf!(
                "cannot stop dirRemover in 10s; the remaining partially deleted directories should be automatically removed on the next startup"
            );
            return;
        }
        let (guard, _) = DIR_REMOVER
            .cv
            .wait_timeout(pending, deadline - now)
            .unwrap();
        pending = guard;
    }
}

fn is_temporary_nfs_error(err: &std::io::Error) -> bool {
    // Some NFS implementations return EEXIST instead of ENOTEMPTY
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6398
    #[cfg(unix)]
    if let Some(code) = err.raw_os_error()
        && (code == libc::EEXIST || code == libc::ENOTEMPTY)
    {
        return true;
    }
    #[cfg(windows)]
    {
        // PORT NOTE: syscall.EEXIST/ENOTEMPTY don't map to Windows error
        // codes; ERROR_FILE_EXISTS(80), ERROR_ALREADY_EXISTS(183) and
        // ERROR_DIR_NOT_EMPTY(145) are the equivalents.
        if let Some(code) = err.raw_os_error()
            && (code == 80 || code == 183 || code == 145)
        {
            return true;
        }
    }
    // Do not check for NFS file handle error, usually it means that other client has opened file without proper lock
    // in this scenario it's better to panic.
    // User must configure proper locking options for the NFS client to prevent such error.
    // It must never have "nolock" or "local_lock=all" options to be set.

    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/61 for details.
    let err_str = err.to_string();
    err_str.contains("directory not empty") || err_str.contains("device or resource busy")
}

fn has_stale_nfs_files(dir_path: &Path) -> bool {
    let des = must_read_dir(dir_path);
    for de in &des {
        if de.file_name().to_string_lossy().starts_with(".nfs") {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_empty_file(file_path: &Path) {
        std::fs::write(file_path, b"empty")
            .unwrap_or_else(|err| panic!("cannot write file: {file_path:?}: {err}"));
    }

    #[test]
    fn test_is_partially_removed_dir() {
        let f = |dir_name: &str, filename: &str, want: bool| {
            let tmp = crate::fs::test_temp_dir("is_partially_removed_dir");
            let dir_path = tmp.join(dir_name);
            std::fs::create_dir(&dir_path)
                .unwrap_or_else(|err| panic!("cannot create directory={dir_path:?}: {err}"));
            if !filename.is_empty() {
                write_empty_file(&dir_path.join(filename));
            }
            let got = is_partially_removed_dir(&dir_path);
            assert_eq!(got, want, "unexpected result: got {got}, want {want}");
            std::fs::remove_dir_all(&tmp).ok();
        };
        f("partially_deleted", DELETE_DIR_FILENAME, true);
        f("empty_dir", "", true);
        f("regular_dir", "index.bin", false);
    }

    #[test]
    fn test_try_remove_dir() {
        let f = |setup: &dyn Fn(&Path), want: bool| {
            let d = crate::fs::test_temp_dir("try_remove_dir");
            setup(&d);
            let got = try_remove_dir(&d);
            assert_eq!(got, want, "unexpected error: (-{want};+{got})");
            std::fs::remove_dir_all(&d).ok();
        };

        // regular delete
        f(
            &|wd| {
                write_empty_file(&wd.join("metadata.bin"));
                write_empty_file(&wd.join(DELETE_DIR_FILENAME));
            },
            true,
        );

        // has stale nfs file
        f(
            &|wd| {
                write_empty_file(&wd.join(".nfs0000"));
                write_empty_file(&wd.join(DELETE_DIR_FILENAME));
            },
            false,
        );

        // empty dir
        f(
            &|wd| {
                write_empty_file(&wd.join(DELETE_DIR_FILENAME));
            },
            true,
        );

        // delete many files concurrent
        f(
            &|wd: &Path| {
                for i in 0..60 {
                    write_empty_file(&wd.join(format!("metadata_{i}.bin")));
                }
                write_empty_file(&wd.join(DELETE_DIR_FILENAME));
            },
            true,
        );
    }
}

//! Platform-specific pieces of `lib/fs`: fs_unix.go / fs_windows.go /
//! fadvise_unix.go.

#[cfg(unix)]
pub(crate) use unix_impl::*;
#[cfg(windows)]
pub(crate) use windows_impl::*;

#[cfg(unix)]
mod unix_impl {
    use std::ffi::CString;
    use std::fs::File;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::AsRawFd;
    use std::path::Path;

    pub(crate) fn must_sync_path_os(path: &Path) {
        let d = match File::open(path) {
            Ok(d) => d,
            Err(err) => {
                crate::panicf!(
                    "FATAL: cannot open file for fsync: open {}: {}",
                    path.display(),
                    err
                );
                unreachable!()
            }
        };
        if let Err(err) = d.sync_all() {
            crate::panicf!("FATAL: cannot flush {path:?} to storage: {err}");
        }
        // PORT NOTE: Go also panics when Close() fails; Rust closes the file
        // on drop without error reporting.
    }

    pub(crate) fn create_flock_file(flock_file: &Path) -> Result<File, String> {
        // os.Create semantics: O_RDWR|O_CREATE|O_TRUNC.
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(flock_file)
            .map_err(|err| format!("cannot create lock file {flock_file:?}: {err}"))?;
        let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!("cannot acquire lock on file {flock_file:?}: {err}"));
        }
        Ok(f)
    }

    pub(crate) fn must_get_disk_space(path: &Path) -> (u64, u64) {
        // PORT NOTE: Go uses statfs(); statvfs() is the portable libc
        // equivalent — f_frsize/f_bavail/f_blocks yield the same byte counts.
        let cpath = CString::new(path.as_os_str().as_bytes()).unwrap_or_default();
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(cpath.as_ptr(), &mut stat) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            crate::panicf!("FATAL: cannot determine free disk space on {path:?}: {err}");
        }
        let total = (stat.f_blocks as u64) * (stat.f_frsize as u64);
        let free = (stat.f_bavail as u64) * (stat.f_frsize as u64);
        (total, free)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn fadvise_sequential_read(f: &File, prefetch: bool) -> Result<(), String> {
        let fd = f.as_raw_fd();
        let mut mode = libc::POSIX_FADV_SEQUENTIAL;
        if prefetch {
            mode |= libc::POSIX_FADV_WILLNEED;
        }
        let rc = unsafe { libc::posix_fadvise(fd, 0, 0, mode) };
        if rc != 0 {
            let err = std::io::Error::from_raw_os_error(rc);
            return Err(format!("error returned from unix.Fadvise({mode}): {err}"));
        }
        Ok(())
    }

    // PORT NOTE: only Linux and Windows are supported targets; the
    // darwin/BSD/solaris fadvise variants are not ported and behave like the
    // Windows stub.
    #[cfg(not(target_os = "linux"))]
    pub(crate) fn fadvise_sequential_read(_f: &File, _prefetch: bool) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(windows)]
mod windows_impl {
    use std::fs::File;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use std::path::Path;

    use windows_sys::Win32::Foundation::{GENERIC_READ, HANDLE};
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_OVERLAPPED, FILE_SHARE_DELETE, FILE_SHARE_READ,
        GetDiskFreeSpaceExW,
    };

    // at windows only files could be synced
    // Sync for directories is not supported.
    pub(crate) fn must_sync_path_os(_path: &Path) {}

    const LOCKFILE_EXCLUSIVE_LOCK: u32 = 2;

    // PORT NOTE: windows-sys gates LockFileEx behind the `Win32_System_IO`
    // feature and CreateEventW behind `Win32_Security`; neither is enabled in
    // Cargo.toml (which must not be modified here). CreateFileW is reached
    // through std::fs::OpenOptions instead, while LockFileEx/CreateEventW and
    // a layout-compatible OVERLAPPED are declared manually below with the same
    // signatures windows-sys generates.
    #[repr(C)]
    struct Overlapped {
        internal: usize,
        internal_high: usize,
        offset: u32,
        offset_high: u32,
        h_event: HANDLE,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LockFileEx(
            hfile: HANDLE,
            dwflags: u32,
            dwreserved: u32,
            nnumberofbytestolocklow: u32,
            nnumberofbytestolockhigh: u32,
            lpoverlapped: *mut Overlapped,
        ) -> i32;
        fn CreateEventW(
            lpeventattributes: *const core::ffi::c_void,
            bmanualreset: i32,
            binitialstate: i32,
            lpname: *const u16,
        ) -> HANDLE;
    }

    // https://docs.microsoft.com/en-us/windows/win32/api/minwinbase/ns-minwinbase-overlapped
    fn new_overlapped() -> Result<Overlapped, String> {
        let event = unsafe { CreateEventW(std::ptr::null(), 1, 1, std::ptr::null()) };
        if event.is_null() {
            return Err(format!(
                "cannot create event: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(Overlapped {
            internal: 0,
            internal_high: 0,
            offset: 0,
            offset_high: 0,
            h_event: event,
        })
    }

    // https://github.com/juju/fslock/blob/master/fslock_windows.go
    pub(crate) fn create_flock_file(flock_file: &Path) -> Result<File, String> {
        let f = std::fs::OpenOptions::new()
            .write(true) // required by std for create(); actual access comes from access_mode()
            .create(true)
            .access_mode(GENERIC_READ | DELETE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OVERLAPPED)
            .attributes(FILE_ATTRIBUTE_NORMAL)
            .open(flock_file)
            .map_err(|err| format!("cannot create lock file {flock_file:?}: {err}"))?;
        let mut ol =
            new_overlapped().map_err(|err| format!("cannot create Overlapped handler: {err}"))?;
        // https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex
        let rc =
            unsafe { LockFileEx(f.as_raw_handle(), LOCKFILE_EXCLUSIVE_LOCK, 0, 0, 0, &mut ol) };
        if rc == 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        // PORT NOTE: like the Go code, the event handle stays open for the
        // lifetime of the lock file (i.e. the process).
        Ok(f)
    }

    pub(crate) fn must_get_disk_space(path: &Path) -> (u64, u64) {
        // https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getdiskfreespaceexw
        let wide: Vec<u16> = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let mut free = 0u64;
        let mut total = 0u64;
        let rc = unsafe {
            GetDiskFreeSpaceExW(wide.as_ptr(), &mut free, &mut total, std::ptr::null_mut())
        };
        if rc == 0 {
            let err = std::io::Error::last_os_error();
            crate::panicf!("FATAL: cannot get free space for {path:?} : {err}");
        }
        (total, free)
    }

    // stub
    pub(crate) fn fadvise_sequential_read(_f: &File, _prefetch: bool) -> Result<(), String> {
        Ok(())
    }
}

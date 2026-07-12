//! Port of Softalink LLC `lib/fs/parallel.go`.

use std::path::PathBuf;

use super::fsutil;
use super::reader_at::{ReaderAt, must_open_reader_at};

/// Opens ReaderAt files in parallel.
///
/// ParallelReaderAtOpener speeds up opening multiple ReaderAt files on high-latency
/// storage systems such as NFS or Ceph.
///
/// PORT NOTE: Go writes the results through caller-provided pointers; the
/// Rust port stores `&mut` output slots, which are filled on run().
#[derive(Default)]
pub struct ParallelReaderAtOpener<'a> {
    tasks: Vec<ParallelReaderAtOpenerTask<'a>>,
}

struct ParallelReaderAtOpenerTask<'a> {
    path: PathBuf,
    rc: &'a mut Option<ReaderAt>,
    file_size: &'a mut u64,
}

impl<'a> ParallelReaderAtOpener<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a task for opening the file at the given path and storing it to `*rc`,
    /// while storing the file size into `*file_size`.
    ///
    /// Call run() for running all the registered tasks in parallel.
    pub fn add(
        &mut self,
        path: impl Into<PathBuf>,
        rc: &'a mut Option<ReaderAt>,
        file_size: &'a mut u64,
    ) {
        self.tasks.push(ParallelReaderAtOpenerTask {
            path: path.into(),
            rc,
            file_size,
        });
    }

    /// Executes all the registered tasks in parallel.
    pub fn run(self) {
        let concurrency_ch = fsutil::get_concurrency_ch();
        std::thread::scope(|s| {
            for task in self.tasks {
                let permit = concurrency_ch.acquire();

                s.spawn(move || {
                    *task.rc = Some(must_open_reader_at(&task.path));
                    *task.file_size = super::must_file_size(&task.path);

                    drop(permit);
                });
            }
        });
    }
}

/// MustCloser must implement must_close() function.
pub trait MustCloser {
    fn must_close(&mut self);
}

impl MustCloser for ReaderAt {
    fn must_close(&mut self) {
        ReaderAt::must_close(self)
    }
}

/// Closes all the `cs` in parallel.
///
/// Parallel closing reduces the time needed to flush the data to the underlying files on close
/// on high-latency storage systems such as NFS or Ceph.
pub fn must_close_parallel(cs: &mut [&mut (dyn MustCloser + Send)]) {
    let concurrency_ch = fsutil::get_concurrency_ch();
    std::thread::scope(|s| {
        for c in cs.iter_mut() {
            let permit = concurrency_ch.acquire();
            s.spawn(move || {
                c.must_close();
                drop(permit);
            });
        }
    });
}

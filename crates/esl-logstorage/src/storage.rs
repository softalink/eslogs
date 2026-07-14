//! Port of EsLogs `lib/logstorage/storage.go`.
//!
//! # Cross-module dependency status
//!
//! The Storage type and its per-day partition management, `MustAddRows`
//! routing, stats and background watchers are wired against the sibling
//! `partition.rs` (Layer 3).
//!
//! Delete-task execution (`processDeleteTask`, `deleteRows`, the
//! `runDeleteTasksWatcher` background loop) is ported below on top of the
//! `storage_search.rs` search spine, alongside the delete-task *registry* ops
//! (`DeleteRunTask` / `DeleteStopTask` / `DeleteActiveTasks`).
//!
//! The Storage-keyed streamID cache half of ingestion is now wired: partitions
//! hold a `Weak<Storage>` and `partition.rs::must_add_rows` reads
//! `stream_id_cache` / `partition_cache_generation` / `log_new_streams` /
//! `log_ingested_rows` from Storage and registers new streams in the indexdb.
//! Consequently `must_add_rows`, `partition_attach` and `get_partition_for_writing`
//! take `self: &Arc<Storage>` (so the partition can be given a `Weak<Storage>`
//! back-reference). `IndexdbStats` is now part of `PartitionStats`, so the
//! disk-usage watcher accounts for indexdb size too.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use esl_common::{errorf, fs, infof, logger, panicf, timeutil, warnf};

use crate::cache::Cache;
use crate::delete_task::{
    DeleteTask, must_read_delete_tasks_from_file, must_write_delete_tasks_to_file, new_delete_task,
};
use crate::filenames::{DELETE_TASKS_FILENAME, PARTITIONS_DIRNAME, SNAPSHOTS_DIRNAME};
use crate::log_rows::{InsertRow, LogRows, get_log_rows, put_log_rows};
use crate::partition::{
    Partition, PartitionStats, get_partition_day_from_name, get_partition_name_from_day,
    must_close_partition, must_create_partition, must_delete_partition, must_open_partition,
};
use crate::rows::{Field, marshal_fields_to_json};
use crate::tenant_id::TenantID;
use crate::values_encoder::{NSECS_PER_DAY, marshal_timestamp_rfc3339_nano_string};

const NSECS_PER_SECOND: i64 = 1_000_000_000;
const NSECS_PER_MINUTE: i64 = 60 * NSECS_PER_SECOND;
const NSECS_PER_HOUR: i64 = 60 * NSECS_PER_MINUTE;

/// StorageStats represents stats for the storage. It may be obtained by calling Storage::update_stats().
#[derive(Debug, Default, Clone)]
pub struct StorageStats {
    /// RowsDroppedTooBigTimestamp is the number of rows dropped during data ingestion because their timestamp is bigger than the maximum allowed.
    pub rows_dropped_too_big_timestamp: u64,

    /// RowsDroppedTooSmallTimestamp is the number of rows dropped during data ingestion because their timestamp is smaller than the minimum allowed.
    pub rows_dropped_too_small_timestamp: u64,

    /// PartitionsCount is the number of partitions in the storage.
    pub partitions_count: u64,

    /// MaxDiskSpaceUsageBytes is the maximum disk space logs can use.
    pub max_disk_space_usage_bytes: i64,

    /// IsReadOnly indicates whether the storage is read-only.
    pub is_read_only: bool,

    /// PartitionStats contains partition stats.
    ///
    /// PORT NOTE: Go embeds `PartitionStats`; the port names the field so the
    /// promoted `RowsCount()` accessor becomes `rows_count()` below. `PartitionStats`
    /// carries only `DatadbStats` until `IndexdbStats` lands (see partition.rs).
    pub partition_stats: PartitionStats,

    /// MinTimestamp is the minimum event timestamp across the entire storage (in nanoseconds).
    /// It is set to i64::MIN if there is no data.
    pub min_timestamp: i64,

    /// MaxTimestamp is the maximum event timestamp across the entire storage (in nanoseconds).
    /// It is set to i64::MAX if there is no data.
    pub max_timestamp: i64,
}

impl StorageStats {
    /// Resets s.
    pub fn reset(&mut self) {
        *self = StorageStats::default();
    }

    /// Returns the number of rows stored in the storage (Go: promoted `DatadbStats.RowsCount()`).
    pub fn rows_count(&self) -> u64 {
        self.partition_stats.datadb_stats.rows_count()
    }
}

/// StorageConfig is the config for the Storage.
///
/// PORT NOTE: Go `time.Duration` fields are represented as i64 nanoseconds
/// (the established esl-common convention).
#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    /// Retention is the retention for the ingested data.
    ///
    /// Older data is automatically deleted.
    pub retention: i64,

    /// DefaultParallelReaders is the default number of parallel readers to use per each query execution.
    ///
    /// Higher value can help improving query performance on storage with high disk read latency such as S3.
    pub default_parallel_readers: usize,

    /// MaxDiskSpaceUsageBytes is an optional maximum disk space logs can use.
    ///
    /// The oldest per-day partitions are automatically dropped if the total disk space usage exceeds this limit.
    pub max_disk_space_usage_bytes: i64,

    /// MaxDiskUsagePercent is an optional threshold in percentage (1-100) for disk usage of the filesystem holding the storage path.
    /// When the current disk usage exceeds this percentage, the oldest per-day partitions are automatically dropped.
    pub max_disk_usage_percent: i64,

    /// FlushInterval is the interval for flushing the in-memory data to disk at the Storage.
    pub flush_interval: i64,

    /// FutureRetention is the allowed retention from the current time to future for the ingested data.
    ///
    /// Log entries with timestamps bigger than now+FutureRetention are ignored.
    pub future_retention: i64,

    /// MaxBackfillAge is the maximum allowed age for the backfilled logs.
    ///
    /// Log entries with timestamps older than now-MaxBackfillAge are ignored.
    pub max_backfill_age: i64,

    /// SnapshotsMaxAge is the maximum age for the created partition snapshots.
    ///
    /// Snapshots are automatically dropped after that duration.
    /// See https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle
    pub snapshots_max_age: i64,

    /// MinFreeDiskSpaceBytes is the minimum free disk space at storage path after which the storage stops accepting new data
    /// and enters read-only mode.
    pub min_free_disk_space_bytes: i64,

    /// LogNewStreams indicates whether to log newly created log streams.
    ///
    /// This can be useful for debugging of high cardinality issues.
    /// https://docs.victoriametrics.com/victorialogs/keyconcepts/#high-cardinality
    pub log_new_streams: bool,

    /// LogIngestedRows indicates whether to log the ingested log entries.
    ///
    /// This can be useful for debugging of data ingestion.
    pub log_ingested_rows: bool,
}

/// Groups the partitions bookkeeping guarded by Go's single `partitionsLock`.
///
/// PORT NOTE: Go protects `partitions`, `ptwHot` and `deletedPartitions` with
/// one `sync.Mutex`; the port groups them into one struct behind one Mutex so
/// the same invariants hold.
#[derive(Default)]
struct PartitionsState {
    /// partitions is a list of partitions for the Storage, sorted by time,
    /// e.g. partitions[0] has the smallest time.
    partitions: Vec<Arc<PartitionWrapper>>,

    /// ptwHot is the "hot" partition, where the last rows were ingested.
    ptw_hot: Option<Arc<PartitionWrapper>>,

    /// deletedPartitions contains days for the deleted partitions.
    /// It prevents from re-creating already deleted partitions.
    deleted_partitions: Vec<i64>,
}

/// Storage is the storage for log entries.
///
/// PORT NOTE: some fields are only read by the deferred delete-task executor
/// and stream-registration paths (see module header); `#[allow(dead_code)]`
/// keeps them documented and populated without warnings until those land.
#[allow(dead_code)]
pub struct Storage {
    rows_dropped_too_big_timestamp: AtomicU64,
    rows_dropped_too_small_timestamp: AtomicU64,

    /// path is the path to the Storage directory
    pub(crate) path: PathBuf,

    /// retention is the retention for the stored data (older data is automatically deleted).
    retention: i64,

    /// defaultParallelReaders is the default number of parallel IO-bound readers to use for query execution.
    pub(crate) default_parallel_readers: usize,

    /// maxDiskSpaceUsageBytes is an optional maximum disk space logs can use.
    max_disk_space_usage_bytes: i64,

    /// maxDiskUsagePercent is an optional threshold for disk usage percentage at which the oldest partitions are automatically dropped.
    max_disk_usage_percent: i64,

    /// flushInterval is the interval for flushing in-memory data to disk
    pub(crate) flush_interval: Duration,

    /// futureRetention is the maximum allowed interval to write data into the future
    future_retention: i64,

    /// maxBackfillAge is the maximum age of logs with historical timestamps to accept
    max_backfill_age: i64,

    /// snapshotsMaxAge is the maximum age for the created partition snapshots.
    snapshots_max_age: i64,

    /// minFreeDiskSpaceBytes is the minimum free disk space at path after which the storage stops accepting new data
    min_free_disk_space_bytes: u64,

    /// logNewStreams instructs to log new streams if it is set to true
    pub(crate) log_new_streams: AtomicBool,

    /// logIngestedRows instructs to log all the ingested log entries if it is set to true
    pub(crate) log_ingested_rows: bool,

    /// flockF makes sure that the Storage is opened by a single process.
    ///
    /// PORT NOTE: Go stores the `*os.File` and closes it via `fs.MustClose` in
    /// MustClose(); dropping the File releases the OS lock the same way.
    flock_f: Mutex<Option<std::fs::File>>,

    /// partitions/ptwHot/deletedPartitions, protected by one lock (Go: partitionsLock).
    partitions_state: Mutex<PartitionsState>,

    /// stop is closed when the Storage must be stopped (Go: stopCh).
    stop: Arc<StopSignal>,

    /// wg is used for waiting for background workers at MustClose() (Go: wg).
    workers: Mutex<Vec<JoinHandle<()>>>,

    /// streamIDCache caches (partition, streamIDs) seen during data ingestion.
    ///
    /// PORT NOTE: stored as `Mutex<Option<Cache>>` so MustClose can call
    /// `MustStop()` explicitly (Go semantics); the deferred stream-registration
    /// path is the reader (see module header).
    pub(crate) stream_id_cache: Mutex<Option<Cache>>,

    /// filterStreamCache caches streamIDs keyed by (partition, []TenantID, StreamFilter).
    pub(crate) filter_stream_cache: Mutex<Option<Cache>>,

    /// partitionCacheGeneration is incremented on partition attach and detach.
    pub(crate) partition_cache_generation: AtomicU64,

    /// deleteTasks contains a list of active and pending delete tasks (Go: deleteTasksLock + deleteTasks).
    delete_tasks: Mutex<Vec<DeleteTask>>,
}

/// A close-only stop signal standing in for Go's `stopCh chan struct{}`.
///
/// PORT NOTE: Go's watchers `select` on `stopCh`; the port uses a
/// (Mutex<bool>, Condvar) pair so a watcher can wait on its ticker interval
/// and wake immediately on close.
struct StopSignal {
    closed: Mutex<bool>,
    cv: Condvar,
}

impl StopSignal {
    fn new() -> StopSignal {
        StopSignal {
            closed: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    fn close(&self) {
        let mut c = self.closed.lock().unwrap();
        *c = true;
        self.cv.notify_all();
    }

    /// Waits up to `d`. Returns true if the signal was closed (Go: the
    /// `<-s.stopCh` branch of the select fired).
    fn wait_timeout(&self, d: Duration) -> bool {
        let guard = self.closed.lock().unwrap();
        if *guard {
            return true;
        }
        let (guard, _) = self.cv.wait_timeout_while(guard, d, |c| !*c).unwrap();
        *guard
    }

    /// Returns true if the signal was closed (Go `needStop(s.stopCh)`).
    fn is_closed(&self) -> bool {
        *self.closed.lock().unwrap()
    }
}

/// partitionWrapper wraps a partition with a reference count and deletion flag.
pub(crate) struct PartitionWrapper {
    /// refCount is the number of active references to partition.
    /// When it reaches zero, then the partition is closed.
    ref_count: AtomicI32,

    /// mustDrop is set when the partition must be deleted after refCount reaches zero.
    must_drop: AtomicBool,

    /// day is the day for the partition (unix nanoseconds divided by nsecsPerDay).
    pub(crate) day: i64,

    /// pt is the wrapped partition.
    ///
    /// PORT NOTE: Go nils `pt` inside decRef() once it is closed; the port
    /// tears down the inner state via `must_close_partition` and then lets the
    /// `Arc<Partition>` be dropped by its owner.
    pub(crate) pt: Arc<Partition>,

    /// doneCh is closed when refCount reaches zero (Go: doneCh chan struct{}).
    done: (Mutex<bool>, Condvar),
}

impl PartitionWrapper {
    fn new(pt: Arc<Partition>, day: i64) -> Arc<PartitionWrapper> {
        let ptw = Arc::new(PartitionWrapper {
            ref_count: AtomicI32::new(0),
            must_drop: AtomicBool::new(false),
            day,
            pt,
            done: (Mutex::new(false), Condvar::new()),
        });
        ptw.inc_ref();
        ptw
    }

    pub(crate) fn inc_ref(&self) {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn dec_ref(&self) {
        let n = self.ref_count.fetch_sub(1, Ordering::SeqCst) - 1;
        if n > 0 {
            return;
        }

        let delete_path = if self.must_drop.load(Ordering::SeqCst) {
            Some(self.pt.path.clone())
        } else {
            None
        };

        // Close pt, since nobody refers to it.
        must_close_partition(&self.pt);

        // Delete partition if needed.
        if let Some(delete_path) = delete_path {
            must_delete_partition(&delete_path);
        }

        // Signal that the ptw is no longer accessed.
        self.signal_done();
    }

    /// Signals that the wrapper is no longer accessed (Go: close(doneCh)).
    fn signal_done(&self) {
        let (m, cv) = &self.done;
        *m.lock().unwrap() = true;
        cv.notify_all();
    }

    /// Waits until the wrapper is no longer accessed (Go: `<-ptw.doneCh`).
    fn wait_done(&self) {
        let (m, cv) = &self.done;
        let mut g = m.lock().unwrap();
        while !*g {
            g = cv.wait(g).unwrap();
        }
    }

    fn can_add_all_rows(&self, lr: &LogRows) -> bool {
        let min_timestamp = self.day * NSECS_PER_DAY;
        let max_timestamp = min_timestamp + NSECS_PER_DAY - 1;
        for &ts in &lr.timestamps {
            if ts < min_timestamp || ts > max_timestamp {
                return false;
            }
        }
        true
    }
}

fn sort_partitions(ptws: &mut [Arc<PartitionWrapper>]) {
    ptws.sort_by_key(|ptw| ptw.day);
}

/// mustCreateStorage creates Storage at the given path.
fn must_create_storage(path: &Path) {
    fs::must_mkdir_fail_if_exist(path);

    let partitions_path = path.join(PARTITIONS_DIRNAME);
    fs::must_mkdir_fail_if_exist(&partitions_path);

    fs::must_sync_path_and_parent_dir(path);
}

impl Storage {
    /// MustOpenStorage opens Storage at the given path.
    ///
    /// MustClose must be called on the returned Storage when it is no longer needed.
    pub fn must_open_storage(path: &Path, cfg: &StorageConfig) -> Arc<Storage> {
        let flush_interval = cfg.flush_interval.max(NSECS_PER_SECOND);
        let retention = cfg.retention.max(24 * NSECS_PER_HOUR);
        let future_retention = cfg.future_retention.max(24 * NSECS_PER_HOUR);

        let mut max_backfill_age = cfg.max_backfill_age;
        if max_backfill_age <= 0 || max_backfill_age > retention {
            max_backfill_age = retention;
        }

        let min_free_disk_space_bytes = if cfg.min_free_disk_space_bytes >= 0 {
            cfg.min_free_disk_space_bytes as u64
        } else {
            0
        };

        if !fs::is_path_exist(path) {
            must_create_storage(path);
        }

        let flock_f = fs::must_create_flock_file(path);

        // Load caches
        let stream_id_cache = Cache::new();
        let filter_stream_cache = Cache::new();

        // Load delete tasks which may be left since the previous restart
        let delete_tasks_path = path.join(DELETE_TASKS_FILENAME);
        let delete_tasks = must_read_delete_tasks_from_file(&delete_tasks_path);

        let s = Arc::new(Storage {
            rows_dropped_too_big_timestamp: AtomicU64::new(0),
            rows_dropped_too_small_timestamp: AtomicU64::new(0),
            path: path.to_path_buf(),
            retention,
            default_parallel_readers: cfg.default_parallel_readers,
            max_disk_space_usage_bytes: cfg.max_disk_space_usage_bytes,
            max_disk_usage_percent: cfg.max_disk_usage_percent,
            flush_interval: Duration::from_nanos(flush_interval as u64),
            future_retention,
            max_backfill_age,
            snapshots_max_age: cfg.snapshots_max_age,
            min_free_disk_space_bytes,
            log_new_streams: AtomicBool::new(cfg.log_new_streams),
            log_ingested_rows: cfg.log_ingested_rows,
            flock_f: Mutex::new(Some(flock_f)),
            partitions_state: Mutex::new(PartitionsState::default()),
            stop: Arc::new(StopSignal::new()),
            workers: Mutex::new(Vec::new()),
            stream_id_cache: Mutex::new(Some(stream_id_cache)),
            filter_stream_cache: Mutex::new(Some(filter_stream_cache)),
            partition_cache_generation: AtomicU64::new(0),
            delete_tasks: Mutex::new(delete_tasks),
        });

        let partitions_path = path.join(PARTITIONS_DIRNAME);
        fs::must_mkdir_if_not_exist(&partitions_path);
        fs::must_sync_path(path);

        let des = fs::must_read_dir(&partitions_path);
        let mut partition_names = Vec::new();
        for de in des {
            let fname = de.file_name().to_string_lossy().into_owned();
            if fname.starts_with('.') {
                // Ignore "hidden" entries, which can be automatically created by MacOS (such as .DS_Store).
                continue;
            }
            let partition_dir = partitions_path.join(&fname);
            if fs::is_partially_removed_dir(&partition_dir) {
                // Drop partially removed partition directory. This may happen when unclean shutdown happens during partition deletion.
                fs::must_remove_dir(&partition_dir);
                continue;
            }
            partition_names.push(fname);
        }

        // PORT NOTE: Go opens partitions in parallel (bounded by
        // cgroup.AvailableCPUs()) to speed up startup; the port opens them
        // sequentially — this is a startup-perf optimization, not a semantic
        // difference.
        let mut ptws: Vec<Arc<PartitionWrapper>> = Vec::with_capacity(partition_names.len());
        for fname in &partition_names {
            let day = match get_partition_day_from_name(fname) {
                Ok(day) => day,
                Err(err) => {
                    panicf!(
                        "FATAL: cannot parse partition filename {:?} at {:?}: {}",
                        fname,
                        partitions_path,
                        err
                    );
                    unreachable!()
                }
            };
            let partition_path = partitions_path.join(fname);
            let pt = must_open_partition(&partition_path, &s);
            ptws.push(PartitionWrapper::new(pt, day));
        }

        sort_partitions(&mut ptws);

        // Delete partitions from the future if needed
        let now = now_unix_nanos();
        let max_allowed_day = s.get_max_allowed_day(now);
        while let Some(last) = ptws.last() {
            if last.day <= max_allowed_day {
                break;
            }
            let ptw = ptws.pop().unwrap();
            infof!(
                "the partition {} is scheduled to be deleted because it is outside the -futureRetention={}d",
                ptw.pt.path.display(),
                duration_to_days(s.future_retention)
            );
            ptw.must_drop.store(true, Ordering::SeqCst);
            ptw.dec_ref();
        }

        s.partitions_state.lock().unwrap().partitions = ptws;

        s.run_retention_watcher();
        s.run_max_disk_space_usage_watcher();
        s.run_delete_tasks_watcher();
        s.run_snapshots_max_age_watcher();

        s
    }

    /// PartitionAttach attaches the partition with the given name to s.
    ///
    /// The name must have the YYYYMMDD format.
    pub fn partition_attach(self: &Arc<Storage>, name: &str) -> Result<(), String> {
        let day = get_partition_day_from_name(name)?;

        let mut st = self.partitions_state.lock().unwrap();

        if st.deleted_partitions.contains(&day) {
            return Err(format!(
                "cannot attach the partition {name:?}, since it is automatically deleted because of retention; see https://docs.victoriametrics.com/victorialogs/#retention"
            ));
        }

        // Verify whether the given partition already exists in the attached partitions list.
        for ptw in &st.partitions {
            if ptw.pt.name == name {
                return Err(format!(
                    "cannot attach the partition {name:?}, because it is already attached"
                ));
            }
        }

        // Open the partition and add it to the s.partitions.
        let partitions_path = self.path.join(PARTITIONS_DIRNAME);
        let partition_path = partitions_path.join(name);
        if !fs::is_path_exist(&partition_path) {
            return Err(format!(
                "cannot attach the partition {name:?}, because there is no the corresponding directory {partition_path:?}"
            ));
        }

        let pt = must_open_partition(&partition_path, self);
        let ptw = PartitionWrapper::new(pt, day);

        st.partitions.push(ptw);
        sort_partitions(&mut st.partitions);
        drop(st);
        self.partition_cache_generation
            .fetch_add(1, Ordering::SeqCst);

        infof!(
            "successfully attached partition {:?} from {:?}",
            name,
            partition_path
        );

        Ok(())
    }

    /// PartitionDetach detaches the partition with the given name from s.
    ///
    /// The name must have the YYYYMMDD format.
    pub fn partition_detach(&self, name: &str) -> Result<(), String> {
        let ptw = {
            let mut st = self.partitions_state.lock().unwrap();
            let mut found = None;
            for i in 0..st.partitions.len() {
                if st.partitions[i].pt.name != name {
                    continue;
                }
                // Found the partition to detach. Detach it.
                let ptw = st.partitions.remove(i);
                if let Some(hot) = &st.ptw_hot
                    && Arc::ptr_eq(hot, &ptw)
                {
                    st.ptw_hot = None;
                }
                found = Some(ptw);
                break;
            }
            found
        };

        let Some(ptw) = ptw else {
            return Err(format!(
                "cannot detach the partition {name:?}, because it isn't attached"
            ));
        };

        let partition_path = ptw.pt.path.clone();
        ptw.dec_ref();

        infof!("waiting until the partition {:?} isn't accessed", name);
        ptw.wait_done();

        // Invalidate partition-related caches after partition detach.
        self.partition_cache_generation
            .fetch_add(1, Ordering::SeqCst);

        infof!(
            "successfully detached partition {:?} from {:?}",
            name,
            partition_path
        );

        Ok(())
    }

    /// PartitionList returns the list of names for the currently attached partitions.
    ///
    /// Every partition name has YYYYMMDD format.
    pub fn partition_list(&self) -> Vec<String> {
        let st = self.partitions_state.lock().unwrap();
        st.partitions
            .iter()
            .map(|ptw| ptw.pt.name.clone())
            .collect()
    }

    /// PartitionSnapshotMustCreate creates snapshots for partitions with the given prefix.
    ///
    /// The function returns paths to created snapshots.
    pub fn partition_snapshot_must_create(&self, partition_prefix: &str) -> Vec<PathBuf> {
        let ptws = self.get_partitions();

        let mut snapshot_paths = Vec::new();
        for ptw in &ptws {
            if ptw.pt.name.starts_with(partition_prefix) {
                snapshot_paths.push(ptw.pt.must_create_snapshot());
            }
        }

        self.put_partitions(&ptws);
        snapshot_paths
    }

    /// PartitionSnapshotList returns a list of paths to all snapshots across active partitions.
    pub fn partition_snapshot_list(&self) -> Vec<String> {
        let ptws = self.get_partitions();
        let mut snapshot_paths = get_snapshot_paths(&ptws);
        self.put_partitions(&ptws);
        snapshot_paths.sort();
        snapshot_paths
    }

    /// PartitionSnapshotDelete removes the snapshot at snapshotPath if it belongs to an active partition.
    pub fn partition_snapshot_delete(&self, snapshot_path: &Path) -> Result<(), String> {
        let snapshot_name = snapshot_path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        if let Err(err) = snapshotutil_validate(&snapshot_name) {
            return Err(format!(
                "unsupported snapshot name {snapshot_name:?} at {snapshot_path:?}: {err}"
            ));
        }

        let snapshot_dir = snapshot_path.parent().unwrap_or(Path::new(""));
        if snapshot_dir.file_name().and_then(|s| s.to_str()) != Some(SNAPSHOTS_DIRNAME) {
            return Err(format!(
                "snapshot path {snapshot_path:?} must point to a directory inside {SNAPSHOTS_DIRNAME:?}"
            ));
        }
        let partition_path = snapshot_dir.parent().unwrap_or(Path::new(""));

        let ptws = self.get_partitions();
        let ptw = ptws
            .iter()
            .find(|ptw| ptw.pt.path == partition_path)
            .cloned();
        let result = match &ptw {
            Some(ptw) => ptw.pt.delete_snapshot(&snapshot_name),
            None => Err(format!(
                "partition path {partition_path:?} cannot be found across active partitions"
            )),
        };
        self.put_partitions(&ptws);
        result
    }

    /// MustDeleteStalePartitionSnapshots deletes snapshots older than maxAge.
    ///
    /// The list of paths to deleted snapshots is returned.
    pub fn must_delete_stale_partition_snapshots(&self, max_age: Duration) -> Vec<String> {
        let mut deleted_snapshot_paths = Vec::new();

        let current_time = std::time::SystemTime::now();

        let ptws = self.get_partitions();
        let snapshot_paths = get_snapshot_paths(&ptws);
        self.put_partitions(&ptws);

        for snapshot_path in snapshot_paths {
            let meta = match std::fs::metadata(&snapshot_path) {
                Ok(m) => m,
                Err(err) => {
                    warnf!("skipping snapshot at {snapshot_path} since cannot access it: {err}");
                    continue;
                }
            };
            let creation_time = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
            let age = current_time
                .duration_since(creation_time)
                .unwrap_or(Duration::ZERO);
            if age > max_age {
                infof!(
                    "deleting snapshot at {snapshot_path} because it became older than maxAge={max_age:?}"
                );
                fs::must_remove_dir(&snapshot_path);
                deleted_snapshot_paths.push(snapshot_path.clone());
                infof!("deleted snapshot at {snapshot_path}");
            }
        }

        deleted_snapshot_paths
    }

    // -- Delete task registry ops (partition-independent; fully ported) --

    /// DeleteRunTask starts deletion of logs according to filter for the given tenantIDs.
    ///
    /// The taskID must be unique. PORT NOTE: Go takes a `*Filter` and stores
    /// `f.String()`; the caller passes the already-stringified filter here
    /// because `Filter` is Layer-4. The timestamp is in nanoseconds (as in
    /// Go's `newDeleteTask`).
    pub fn delete_run_task(
        &self,
        task_id: &str,
        timestamp: i64,
        tenant_ids: Vec<TenantID>,
        filter: &str,
    ) -> Result<(), String> {
        let dt = new_delete_task(task_id, timestamp, tenant_ids, filter);

        let mut tasks = self.delete_tasks.lock().unwrap();

        // Verify that the task with the given taskID doesn't exist yet
        for existing in tasks.iter() {
            if existing.task_id == task_id {
                return Err(format!(
                    "the delete task with task_id={task_id:?} is already registered"
                ));
            }
        }

        tasks.push(dt);
        self.must_save_delete_tasks_locked(&tasks);

        Ok(())
    }

    /// mustSaveDeleteTasksLocked saves s.deleteTasks to file.
    fn must_save_delete_tasks_locked(&self, tasks: &[DeleteTask]) {
        let delete_tasks_path = self.path.join(DELETE_TASKS_FILENAME);
        must_write_delete_tasks_to_file(&delete_tasks_path, tasks);
    }

    /// DeleteActiveTasks returns currently running active delete tasks.
    pub fn delete_active_tasks(&self) -> Vec<DeleteTask> {
        self.delete_tasks.lock().unwrap().clone()
    }

    /// DeleteStopTask stops the delete task with the given taskID.
    ///
    /// It waits until the task is stopped before returning. If there is no task
    /// with the given taskID, it returns immediately.
    pub fn delete_stop_task(&self, task_id: &str) -> Result<(), String> {
        let mut done_ch: Option<Arc<(Mutex<bool>, Condvar)>> = None;

        {
            let mut tasks = self.delete_tasks.lock().unwrap();
            for i in 0..tasks.len() {
                if tasks[i].task_id != task_id {
                    continue;
                }
                if let Some(cancel) = tasks[i].cancel.clone() {
                    // Cancel the currently executed task. The executor removes it from the list.
                    cancel.store(true, Ordering::SeqCst);
                    done_ch = tasks[i].done_ch.clone();
                } else {
                    // The task is waiting to be executed. Drop it.
                    tasks.remove(i);
                    self.must_save_delete_tasks_locked(&tasks);
                }
                break;
            }
        }

        let Some(done_ch) = done_ch else {
            return Ok(());
        };

        // Wait until the task is canceled (Go: `<-doneCh`).
        let (m, cv) = &*done_ch;
        let mut g = m.lock().unwrap();
        while !*g {
            g = cv.wait(g).unwrap();
        }
        Ok(())
    }

    /// EnableLogNewStreams enables logging newly ingested streams for the given number of seconds.
    pub fn enable_log_new_streams(self: &Arc<Storage>, seconds: i64) {
        if seconds <= 0 {
            return;
        }

        let v_prev = self.log_new_streams.swap(true, Ordering::SeqCst);
        if v_prev {
            infof!("logging of new streams is already enabled");
            return;
        }

        infof!("enabled logging of new streams for {seconds} seconds");

        // PORT NOTE: Go uses time.AfterFunc; the port spawns a detached timer
        // thread that flips the flag back after the delay.
        let s = Arc::clone(self);
        let d = Duration::from_secs(seconds as u64);
        std::thread::Builder::new()
            .name("log_new_streams_timer".to_string())
            .spawn(move || {
                std::thread::sleep(d);
                s.log_new_streams.store(false, Ordering::SeqCst);
                infof!("disabled logging of new streams");
            })
            .expect("FATAL: cannot spawn log_new_streams_timer thread");
    }

    /// MustClose closes s.
    ///
    /// It is expected that nobody uses the storage at the close time.
    pub fn must_close(&self) {
        // Stop background workers
        self.stop.close();
        // PORT NOTE: Go's delete-task executor derives its context from
        // s.stopCh (contextutil.NewStopChanContext); the port's stand-in is
        // the per-task cancel token, so closing the storage must set the
        // token of the in-flight task for the executor to abort promptly.
        // processDeleteTask checks is_stopped() first, so this is treated as
        // a storage stop (task postponed), not an explicit cancellation.
        for dt in self.delete_tasks.lock().unwrap().iter() {
            if let Some(cancel) = &dt.cancel {
                cancel.store(true, Ordering::SeqCst);
            }
        }
        let handles = std::mem::take(&mut *self.workers.lock().unwrap());
        for h in handles {
            let _ = h.join();
        }

        // Close partitions
        let partitions = {
            let mut st = self.partitions_state.lock().unwrap();
            st.ptw_hot = None;
            std::mem::take(&mut st.partitions)
        };
        for pw in partitions {
            pw.dec_ref();
            let n = pw.ref_count.load(Ordering::SeqCst);
            if n != 0 {
                panicf!("BUG: there are {} users of partition", n);
            }
        }

        // Stop caches.
        //
        // Do not persist caches, since they may become out of sync with
        // partitions if partitions are deleted, restored from backups or copied
        // from other sources between EsLogs restarts.
        if let Some(mut c) = self.stream_id_cache.lock().unwrap().take() {
            c.must_stop();
        }
        if let Some(mut c) = self.filter_stream_cache.lock().unwrap().take() {
            c.must_stop();
        }

        // Release the lock file (Go: fs.MustClose(s.flockF)).
        *self.flock_f.lock().unwrap() = None;
    }

    /// MustForceMerge force-merges parts in partitions with names starting with the given prefix.
    ///
    /// Partitions are merged sequentially in order to reduce load on the system.
    pub fn must_force_merge(&self, partition_prefix: &str) {
        let ptws = self.get_partitions();

        for ptw in &ptws {
            if !ptw.pt.name.starts_with(partition_prefix) {
                continue;
            }
            infof!("started force merge for partition {}", ptw.pt.name);
            let start_time = std::time::Instant::now();
            ptw.pt.must_force_merge();
            infof!(
                "finished force merge for partition {} in {:.3}s",
                ptw.pt.name,
                start_time.elapsed().as_secs_f64()
            );
        }

        self.put_partitions(&ptws);
    }

    /// MustAddRows adds lr to s.
    ///
    /// It is recommended to check read-only mode via is_read_only() before calling.
    ///
    /// The added rows become visible for search after a small duration of time.
    /// Call debug_flush if the added rows must be queried immediately.
    pub fn must_add_rows(self: &Arc<Storage>, lr: &LogRows) {
        // Fast path - try adding all the rows to the hot partition
        let ptw_hot = {
            let st = self.partitions_state.lock().unwrap();
            match &st.ptw_hot {
                Some(ptw) => {
                    ptw.inc_ref();
                    Some(ptw.clone())
                }
                None => None,
            }
        };

        if let Some(ptw_hot) = ptw_hot {
            if ptw_hot.can_add_all_rows(lr) {
                ptw_hot.pt.must_add_rows(lr);
                ptw_hot.dec_ref();
                return;
            }
            ptw_hot.dec_ref();
        }

        // Slow path - rows cannot be added to the hot partition, so split rows among available partitions.
        let now = now_unix_nanos();
        let min_allowed_day = self.get_min_allowed_day(now);
        let max_allowed_day = self.get_max_allowed_day(now);
        let min_allowed_timestamp = now - self.max_backfill_age;

        let mut m: HashMap<i64, LogRows> = HashMap::new();
        for i in 0..lr.timestamps.len() {
            let ts = lr.timestamps[i];
            let day = ts / NSECS_PER_DAY;
            if day < min_allowed_day {
                let line = fields_to_json(&lr.rows[i]);
                too_small_timestamp_logger().warnf(format_args!(
                    "skipping log entry with too small timestamp={}; it must be bigger than {} according \
                    to the configured -retentionPeriod={}d. See https://docs.victoriametrics.com/victorialogs/#retention ; \
                    log entry: {}",
                    TimeFormatter(ts),
                    TimeFormatter(min_allowed_day * NSECS_PER_DAY),
                    duration_to_days(self.retention),
                    line
                ));
                self.rows_dropped_too_small_timestamp
                    .fetch_add(1, Ordering::SeqCst);
                continue;
            }
            if day > max_allowed_day {
                let line = fields_to_json(&lr.rows[i]);
                too_big_timestamp_logger().warnf(format_args!(
                    "skipping log entry with too big timestamp={}; it must be smaller than {} according \
                    to the configured -futureRetention={}d; see https://docs.victoriametrics.com/victorialogs/#retention ; \
                    log entry: {}",
                    TimeFormatter(ts),
                    TimeFormatter(max_allowed_day * NSECS_PER_DAY),
                    duration_to_days(self.future_retention),
                    line
                ));
                self.rows_dropped_too_big_timestamp
                    .fetch_add(1, Ordering::SeqCst);
                continue;
            }
            if ts < min_allowed_timestamp {
                let line = fields_to_json(&lr.rows[i]);
                too_small_timestamp_logger().warnf(format_args!(
                    "skipping log entry with too small timestamp={}; it must be bigger than {} according \
                    to the configured -maxBackfillAge={}. See https://docs.victoriametrics.com/victorialogs/#backfilling ; \
                    log entry: {}",
                    TimeFormatter(ts),
                    TimeFormatter(min_allowed_timestamp),
                    esl_common::flagutil::duration::format_go_duration(self.max_backfill_age),
                    line
                ));
                self.rows_dropped_too_small_timestamp
                    .fetch_add(1, Ordering::SeqCst);
                continue;
            }

            let lr_part = m
                .entry(day)
                .or_insert_with(|| get_log_rows(&[], &[], &[], &[], ""));
            // PORT NOTE: Go calls lrPart.mustAddInternal(streamID, ts, fields,
            // canonical) to reuse the already-computed streamID. That method is
            // private to log_rows; the port routes through must_add_insert_row,
            // which recomputes the identical streamID = hash128(canonical) from
            // the same tenant/canonical/fields — behaviorally equivalent.
            let r = InsertRow {
                tenant_id: lr.stream_ids[i].tenant_id,
                stream_tags_canonical: lr.stream_tags_canonicals[i].clone(),
                timestamp: ts,
                fields: lr.rows[i].clone(),
            };
            lr_part.must_add_insert_row(&r);
        }

        for (day, lr_part) in m.drain() {
            if let Some(ptw) = self.get_partition_for_writing(day) {
                ptw.pt.must_add_rows(&lr_part);
                ptw.dec_ref();
            } else {
                // the lrPart must contain at least a single row, so log it.
                let line = fields_to_json(&lr_part.rows[0]);
                inactive_partition_logger().warnf(format_args!(
                    "skipping log entry because it cannot be saved into inactive per-day partition; \
                    see https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle; log entry {line}"
                ));
            }
            put_log_rows(lr_part);
        }
    }

    /// getPartitionForWriting returns the partition for the given day for writing.
    ///
    /// The partition is automatically created if it didn't exist. None is
    /// returned when the partition is outside the retention, has been detached,
    /// or its directory was manually added but not yet attached.
    fn get_partition_for_writing(self: &Arc<Storage>, day: i64) -> Option<Arc<PartitionWrapper>> {
        let mut st = self.partitions_state.lock().unwrap();

        // Search for the partition using binary search.
        let n = st.partitions.partition_point(|ptw| ptw.day < day);
        let mut ptw = if n < st.partitions.len() && st.partitions[n].day == day {
            Some(st.partitions[n].clone())
        } else {
            None
        };

        if ptw.is_none() {
            // Missing partition for the given day.
            if st.deleted_partitions.contains(&day) {
                // The partition has been already deleted.
                return None;
            }

            let fname = get_partition_name_from_day(day);
            let partition_path = self.path.join(PARTITIONS_DIRNAME).join(&fname);
            if fs::is_path_exist(&partition_path) {
                // The partition directory exists but isn't attached.
                return None;
            }

            // Create missing partition.
            must_create_partition(&partition_path);
            let pt = must_open_partition(&partition_path, self);
            let new_ptw = PartitionWrapper::new(pt, day);
            st.partitions.insert(n, new_ptw.clone());
            ptw = Some(new_ptw);
        }

        let ptw = ptw.unwrap();
        st.ptw_hot = Some(ptw.clone());
        ptw.inc_ref();

        Some(ptw)
    }

    /// UpdateStats updates ss for the given s.
    pub fn update_stats(&self, ss: &mut StorageStats) {
        ss.rows_dropped_too_big_timestamp +=
            self.rows_dropped_too_big_timestamp.load(Ordering::SeqCst);
        ss.rows_dropped_too_small_timestamp +=
            self.rows_dropped_too_small_timestamp.load(Ordering::SeqCst);
        if self.max_disk_space_usage_bytes > 0 {
            ss.max_disk_space_usage_bytes = self.max_disk_space_usage_bytes;
        } else {
            ss.max_disk_space_usage_bytes = (fs::must_get_total_space(&self.path)
                * self.max_disk_usage_percent as u64
                / 100) as i64;
        }
        // Use sentinel values to indicate unbounded / no data for consistency.
        ss.min_timestamp = i64::MIN;
        ss.max_timestamp = i64::MAX;

        let st = self.partitions_state.lock().unwrap();
        ss.partitions_count += st.partitions.len() as u64;
        for ptw in &st.partitions {
            ptw.pt.update_stats(&mut ss.partition_stats);
        }

        if !st.partitions.is_empty() {
            let p0 = &st.partitions[0];
            let p_last = &st.partitions[st.partitions.len() - 1];

            ss.min_timestamp = p0.pt.ddb().get_min_max_timestamps().0;
            ss.max_timestamp = p_last.pt.ddb().get_min_max_timestamps().1;
        }
        drop(st);

        ss.is_read_only = self.is_read_only();
    }

    /// IsReadOnly returns true if s is in read-only mode.
    pub fn is_read_only(&self) -> bool {
        let available = fs::must_get_free_space(&self.path);
        available < self.min_free_disk_space_bytes
    }

    /// DebugFlush flushes all buffered rows so they become visible for search.
    ///
    /// This function is for debugging and testing purposes only, since it is slow.
    pub fn debug_flush(&self) {
        let ptws = self.get_partitions();
        for ptw in &ptws {
            ptw.pt.debug_flush();
        }
        self.put_partitions(&ptws);
    }

    /// dropStalePartitions drops partitions that fell outside the retention.
    pub(crate) fn drop_stale_partitions(&self) {
        let now = now_unix_nanos();
        let min_allowed_day = self.get_min_allowed_day(now);

        let ptws_to_delete = {
            let mut st = self.partitions_state.lock().unwrap();

            // s.partitions are sorted by day; find the first non-expired partition.
            let n = st
                .partitions
                .partition_point(|ptw| ptw.day < min_allowed_day);
            let ptws_to_delete: Vec<Arc<PartitionWrapper>> = st.partitions.drain(..n).collect();
            update_deleted_partitions_locked(&mut st, &ptws_to_delete);

            // Remove reference to deleted partitions from ptwHot.
            if let Some(hot) = &st.ptw_hot
                && ptws_to_delete.iter().any(|p| Arc::ptr_eq(p, hot))
            {
                st.ptw_hot = None;
            }
            ptws_to_delete
        };

        for ptw in ptws_to_delete {
            infof!(
                "the partition {} is scheduled to be deleted because it is outside the -retentionPeriod={}d",
                ptw.pt.path.display(),
                duration_to_days(self.retention)
            );
            ptw.must_drop.store(true, Ordering::SeqCst);
            ptw.dec_ref();
        }
    }

    fn get_min_allowed_day(&self, now: i64) -> i64 {
        (now - self.retention) / NSECS_PER_DAY
    }

    fn get_max_allowed_day(&self, now: i64) -> i64 {
        (now + self.future_retention) / NSECS_PER_DAY
    }

    pub(crate) fn get_partitions(&self) -> Vec<Arc<PartitionWrapper>> {
        let st = self.partitions_state.lock().unwrap();
        let ptws: Vec<Arc<PartitionWrapper>> = st.partitions.clone();
        for ptw in &ptws {
            ptw.inc_ref();
        }
        ptws
    }

    pub(crate) fn put_partitions(&self, ptws: &[Arc<PartitionWrapper>]) {
        for ptw in ptws {
            ptw.dec_ref();
        }
    }

    /// Returns the partitions covering `[min_timestamp, max_timestamp]`
    /// (Go `getPartitionsForTimeRange`). Each returned partition has its refCount
    /// incremented; the caller must `put_partitions()` (dec_ref) when done.
    ///
    /// PORT NOTE: Go binary-searches `s.partitions` (sorted by day); the port
    /// filters linearly. The result is identical; only the selection cost
    /// differs (partitions per storage are few — one per day).
    pub(crate) fn get_partitions_for_time_range(
        &self,
        min_timestamp: i64,
        max_timestamp: i64,
    ) -> Vec<Arc<PartitionWrapper>> {
        let min_day = min_timestamp / NSECS_PER_DAY;
        let max_day = max_timestamp / NSECS_PER_DAY;
        let st = self.partitions_state.lock().unwrap();
        let ptws: Vec<Arc<PartitionWrapper>> = st
            .partitions
            .iter()
            .filter(|ptw| ptw.day >= min_day && ptw.day <= max_day)
            .cloned()
            .collect();
        for ptw in &ptws {
            ptw.inc_ref();
        }
        ptws
    }

    // -- Background watchers --

    fn run_retention_watcher(self: &Arc<Storage>) {
        self.spawn_worker("retention_watcher", |s| s.watch_retention());
    }

    fn run_max_disk_space_usage_watcher(self: &Arc<Storage>) {
        if self.max_disk_space_usage_bytes <= 0 && self.max_disk_usage_percent <= 0 {
            return; // nothing to watch
        }
        self.spawn_worker("max_disk_space_usage_watcher", |s| {
            s.watch_max_disk_space_usage()
        });
    }

    fn run_snapshots_max_age_watcher(self: &Arc<Storage>) {
        self.spawn_worker("snapshots_max_age_watcher", |s| s.watch_snapshots_max_age());
    }

    fn run_delete_tasks_watcher(self: &Arc<Storage>) {
        self.spawn_worker("delete_tasks_watcher", |s| s.watch_delete_tasks());
    }

    /// watchDeleteTasks executes pending delete tasks on a per-second
    /// (jittered) tick.
    fn watch_delete_tasks(self: Arc<Storage>) {
        let d = Duration::from_nanos(timeutil::add_jitter_to_duration(NSECS_PER_SECOND) as u64);
        loop {
            if self.stop.wait_timeout(d) {
                return;
            }

            let dt = {
                let mut tasks = self.delete_tasks.lock().unwrap();
                match tasks.first_mut() {
                    Some(dt) => {
                        // initialize dt.ctx and dt.cancel under the lock in order to avoid races
                        // with canceling the task at Storage.DeleteStopTask()
                        //
                        // PORT NOTE: Go pairs a contextutil.NewStopChanContext(s.stopCh)
                        // with the cancel func; the port's cancel token is also set by
                        // must_close() (see there), and processDeleteTask distinguishes
                        // the two via is_closed().
                        dt.cancel = Some(Arc::new(AtomicBool::new(false)));
                        dt.done_ch = Some(Arc::new((Mutex::new(false), Condvar::new())));
                        Some(dt.clone())
                    }
                    None => None,
                }
            };

            let Some(dt) = dt else {
                // There are no delete tasks.
                continue;
            };

            // Process delete tasks sequentially in order to limit resource usage needed for the logs' deletion.

            let cancel = dt.cancel.clone().unwrap();
            let ok = self.process_delete_task(&cancel, &dt);

            // close(dt.doneCh)
            let done_ch = dt.done_ch.clone().unwrap();
            {
                let (m, cv) = &*done_ch;
                *m.lock().unwrap() = true;
                cv.notify_all();
            }

            let mut tasks = self.delete_tasks.lock().unwrap();

            // Set dt.ctx and dt.cancel to nil under the lock in order to avoid races
            // with canceling the task at Storage.DeleteStopTask().
            let mut dt = tasks.remove(0);
            dt.cancel = None;
            dt.done_ch = None;

            if !ok {
                // The delete task couldn't be completed now. Try it later.
                tasks.push(dt);
            }
            self.must_save_delete_tasks_locked(&tasks);
        }
    }

    /// processDeleteTask processes dt.
    ///
    /// true is returned on successfully processed dt or on explicitly canceled dt.
    /// false is returned if dt couldn't be processed at the moment, so it must be processed later.
    fn process_delete_task(self: &Arc<Storage>, cancel: &Arc<AtomicBool>, dt: &DeleteTask) -> bool {
        infof!("started processing delete task {dt}");
        let start_time = std::time::Instant::now();
        let now = dt.start_time;

        let f = match crate::parser::ParseFilterAtTimestamp(&dt.filter, now) {
            Ok(f) => f,
            Err(_) => {
                panicf!("BUG: cannot parse filter from delete task: [{}]", dt.filter);
                unreachable!()
            }
        };

        let mut q = crate::parser::Query::from_filter_at_timestamp(f, now);

        // Add time filter ending at now in order to avoid deleting logs from the future.
        q.add_time_filter(i64::MIN, now);

        let qs = Arc::new(crate::query_stats::QueryStats::default());

        // Initialize subqueries
        let q_new = match crate::storage_search::init_subqueries(
            self,
            &dt.tenant_ids,
            &q,
            &[],
            Some(cancel),
            &qs,
        ) {
            Ok(q_new) => q_new,
            Err(err) => {
                errorf!(
                    "cannot process delete task with task_id={:?} while initializing subqueries: {err}; retrying later",
                    dt.task_id
                );
                return false;
            }
        };
        let q = q_new.unwrap_or(q);

        let mut sso = crate::storage_search::get_search_options(&dt.tenant_ids, &q, &[]);

        // reset fieldsFilter in order to avoid loading all the log fields
        // during search for parts which contain rows to delete, since these fields aren't needed.
        sso.fields_filter.reset();

        // delete rows matching q.f
        if !self.delete_rows(&sso, cancel) {
            if self.stop.is_closed() {
                infof!(
                    "the storage is stopped while executing the delete task with task_id={:?}; postponing the task for later execution",
                    dt.task_id
                );
                return false;
            }

            if cancel.load(Ordering::SeqCst) {
                // The task has been canceled explicitly. Return true, so it isn't re-scheduled for later execution.
                infof!(
                    "the delete task with task_id={:?} is explicitly canceled after {:.3} seconds",
                    dt.task_id,
                    start_time.elapsed().as_secs_f64()
                );
                return true;
            }

            // The task couldn't be processed at the moment
            warnf!(
                "cannot proceed with the delete task with task_id={:?} in {:.3} seconds; retrying it later",
                dt.task_id,
                start_time.elapsed().as_secs_f64()
            );
            return false;
        }

        infof!(
            "finished processing delete task {dt} in {:.3} seconds",
            start_time.elapsed().as_secs_f64()
        );
        true
    }

    /// Go `Storage.deleteRows`.
    fn delete_rows(
        &self,
        sso: &crate::storage_search::StorageSearchOptions<'_>,
        stop_ch: &AtomicBool,
    ) -> bool {
        let ptws = self.get_partitions_for_time_range(sso.min_timestamp, sso.max_timestamp);

        // Delete rows sequentially in every partition in order to limit resource usage needed for the logs' deletion.
        let mut ok = true;
        for ptw in &ptws {
            if !ptw.pt.delete_rows(sso, stop_ch) {
                // Return false if at least a single deletion was unsuccessful.
                // Continue deletion of rows at other partitions, since they may be successful.
                ok = false;
            }
        }

        self.put_partitions(&ptws);

        ok
    }

    fn spawn_worker(self: &Arc<Storage>, name: &str, f: fn(Arc<Storage>)) {
        let s = Arc::clone(self);
        let h = match std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || f(s))
        {
            Ok(h) => h,
            Err(err) => {
                panicf!("FATAL: cannot spawn {name} thread: {err}");
                unreachable!()
            }
        };
        self.workers.lock().unwrap().push(h);
    }

    /// watchRetention drops stale partitions on an hourly (jittered) tick.
    fn watch_retention(self: Arc<Storage>) {
        let d = Duration::from_nanos(timeutil::add_jitter_to_duration(NSECS_PER_HOUR) as u64);
        loop {
            self.drop_stale_partitions();
            if self.stop.wait_timeout(d) {
                return;
            }
        }
    }

    /// watchSnapshotsMaxAge deletes stale snapshots on a per-minute (jittered) tick.
    fn watch_snapshots_max_age(self: Arc<Storage>) {
        if self.snapshots_max_age <= 0 {
            return;
        }
        let d = Duration::from_nanos(timeutil::add_jitter_to_duration(NSECS_PER_MINUTE) as u64);
        loop {
            if self.stop.wait_timeout(d) {
                return;
            }
            self.must_delete_stale_partition_snapshots(Duration::from_nanos(
                self.snapshots_max_age as u64,
            ));
        }
    }

    /// watchMaxDiskSpaceUsage drops the oldest partitions once the configured limit is exceeded.
    fn watch_max_disk_space_usage(self: Arc<Storage>) {
        let d =
            Duration::from_nanos(timeutil::add_jitter_to_duration(10 * NSECS_PER_SECOND) as u64);
        loop {
            // Determine dynamic limit in bytes.
            let mut limit_bytes: u64 = 0;
            if self.max_disk_space_usage_bytes > 0 {
                limit_bytes = self.max_disk_space_usage_bytes as u64;
            } else if self.max_disk_usage_percent > 0 {
                let total = fs::must_get_total_space(&self.path);
                if total > 0 {
                    limit_bytes = total * self.max_disk_usage_percent as u64 / 100;
                }
            }
            if limit_bytes == 0 {
                // Nothing to enforce.
                if self.stop.wait_timeout(d) {
                    return;
                }
                continue;
            }

            let ptws_to_delete = {
                let mut st = self.partitions_state.lock().unwrap();
                let mut n: u64 = 0;
                let len = st.partitions.len();
                let mut split: Option<usize> = None;
                for i in (0..len).rev() {
                    let mut ps = PartitionStats::default();
                    st.partitions[i].pt.update_stats(&mut ps);
                    n += ps.indexdb_stats.indexdb_size_bytes
                        + ps.datadb_stats.compressed_small_part_size
                        + ps.datadb_stats.compressed_big_part_size;
                    if n <= limit_bytes {
                        continue;
                    }
                    if i >= len.saturating_sub(2) {
                        // Keep the last two per-day partitions, so logs could be queried for one day time range.
                        continue;
                    }
                    split = Some(i + 1);
                    break;
                }

                match split {
                    Some(k) => {
                        let ptws_to_delete: Vec<Arc<PartitionWrapper>> =
                            st.partitions.drain(..k).collect();
                        update_deleted_partitions_locked(&mut st, &ptws_to_delete);
                        if let Some(hot) = &st.ptw_hot
                            && ptws_to_delete.iter().any(|p| Arc::ptr_eq(p, hot))
                        {
                            st.ptw_hot = None;
                        }
                        ptws_to_delete
                    }
                    None => Vec::new(),
                }
            };

            for ptw in ptws_to_delete {
                let reason = if self.max_disk_space_usage_bytes > 0 {
                    format!(
                        "-retention.maxDiskSpaceUsageBytes={}",
                        self.max_disk_space_usage_bytes
                    )
                } else {
                    format!(
                        "-retention.maxDiskUsagePercent={}%",
                        self.max_disk_usage_percent
                    )
                };
                infof!(
                    "the partition {} is scheduled to be deleted because the total size of partitions exceeds {}",
                    ptw.pt.path.display(),
                    reason
                );
                ptw.must_drop.store(true, Ordering::SeqCst);
                ptw.dec_ref();
            }

            if self.stop.wait_timeout(d) {
                return;
            }
        }
    }

    /// Runs the parsed query `q` for the given `tenant_ids` and streams each
    /// result [`DataBlock`] to `write_block_fn` (Go `Storage.RunQuery`).
    ///
    /// PORT NOTE: Go takes a `*QueryContext` carrying the tenantIDs, context
    /// and query stats; the port passes `tenant_ids` explicitly. Context
    /// cancellation and per-query stats accumulation (Go `qctx.QueryStats`)
    /// are available via [`Storage::run_query_with_stats`]; this convenience
    /// wrapper discards the stats. The search spine lives in
    /// `storage_search.rs` (`run_query` / `search_parallel`).
    pub fn run_query(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &crate::parser::Query,
        write_block_fn: crate::storage_search::WriteDataBlockFn,
    ) -> Result<(), String> {
        let qs = Arc::new(crate::query_stats::QueryStats::default());
        crate::storage_search::run_query(self, tenant_ids, q, &[], write_block_fn, None, &qs)
    }

    /// Like [`Storage::run_query`], but aborts early when `cancel` is set,
    /// returning [`crate::storage_search::QUERY_CANCELED_ERROR`] — the port of
    /// Go's request-context cancellation (`ctx.Done()` -> `context.Canceled`).
    ///
    /// `cancel` must only be set by the external caller (e.g. the HTTP
    /// client-disconnect watcher); it is checked alongside the query's internal
    /// per-run stop flag, mirroring Go's parent-ctx/derived-cancel split (see
    /// the `storage_search::run_query` PORT NOTE). The same token may safely
    /// cancel several sequential `run_query_with_cancel` calls serving one
    /// request.
    pub fn run_query_with_cancel(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &crate::parser::Query,
        write_block_fn: crate::storage_search::WriteDataBlockFn,
        cancel: Option<&Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<(), String> {
        let qs = Arc::new(crate::query_stats::QueryStats::default());
        crate::storage_search::run_query(self, tenant_ids, q, &[], write_block_fn, cancel, &qs)
    }

    /// Like [`Storage::run_query_with_cancel`], but additionally accumulates
    /// the query execution stats into `qs` — the port of Go's
    /// `qctx.QueryStats` threading — and hides the fields matching
    /// `hidden_fields_filters` from query execution (Go
    /// `qctx.HiddenFieldsFilters`; full field names and `*`-suffixed
    /// prefixes). The same `qs` may accumulate several
    /// sequential queries serving one request (Go shares one
    /// `commonArgs.qs`/`commonParams.qs` across them).
    pub fn run_query_with_stats(
        self: &Arc<Storage>,
        tenant_ids: &[TenantID],
        q: &crate::parser::Query,
        hidden_fields_filters: &[String],
        write_block_fn: crate::storage_search::WriteDataBlockFn,
        cancel: Option<&Arc<std::sync::atomic::AtomicBool>>,
        qs: &Arc<crate::query_stats::QueryStats>,
    ) -> Result<(), String> {
        crate::storage_search::run_query(
            self,
            tenant_ids,
            q,
            hidden_fields_filters,
            write_block_fn,
            cancel,
            qs,
        )
    }
}

fn update_deleted_partitions_locked(
    st: &mut PartitionsState,
    ptws_to_delete: &[Arc<PartitionWrapper>],
) {
    for ptw in ptws_to_delete {
        if !st.deleted_partitions.contains(&ptw.day) {
            st.deleted_partitions.push(ptw.day);
        }
    }
}

/// Returns paths to all snapshots inside the given partitions.
fn get_snapshot_paths(ptws: &[Arc<PartitionWrapper>]) -> Vec<String> {
    let mut snapshot_paths = Vec::new();

    for ptw in ptws {
        let snapshots_path = ptw.pt.path.join(SNAPSHOTS_DIRNAME);
        if !fs::is_path_exist(&snapshots_path) {
            continue;
        }

        let des = fs::must_read_dir(&snapshots_path);
        for de in des {
            let name = de.file_name().to_string_lossy().into_owned();
            if let Err(err) = snapshotutil_validate(&name) {
                warnf!(
                    "unsupported snapshot name {name:?} at {}: {err}",
                    snapshots_path.display()
                );
                continue;
            }
            let snapshot_path = snapshots_path.join(&name);
            snapshot_paths.push(snapshot_path.to_string_lossy().into_owned());
        }
    }

    snapshot_paths
}

// -- Throttled loggers for dropped rows (Go: package-level WithThrottler vars) --

fn too_small_timestamp_logger() -> &'static logger::LogThrottler {
    logger::with_throttler("too_small_timestamp", Duration::from_secs(5))
}

fn too_big_timestamp_logger() -> &'static logger::LogThrottler {
    logger::with_throttler("too_big_timestamp", Duration::from_secs(5))
}

fn inactive_partition_logger() -> &'static logger::LogThrottler {
    logger::with_throttler("inactive_partition", Duration::from_secs(5))
}

fn fields_to_json(fields: &[Field]) -> String {
    let mut buf = Vec::new();
    marshal_fields_to_json(&mut buf, fields);
    String::from_utf8_lossy(&buf).into_owned()
}

/// TimeFormatter implements Display for a timestamp in nanoseconds.
pub struct TimeFormatter(pub i64);

impl std::fmt::Display for TimeFormatter {
    /// Returns human-readable representation for tf (Go: time.RFC3339Nano).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf = Vec::new();
        marshal_timestamp_rfc3339_nano_string(&mut buf, self.0);
        f.write_str(std::str::from_utf8(&buf).expect("BUG: RFC3339 timestamp must be UTF-8"))
    }
}

fn duration_to_days(d: i64) -> i64 {
    d / NSECS_PER_DAY
}

fn now_unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Validates the snapshotName.
///
/// PORT NOTE: this is a port of the vendored
/// `lib/snapshot/snapshotutil.Validate` (not available in esl-common yet);
/// it is private to storage.rs and preserves the Go error messages.
fn snapshotutil_validate(snapshot_name: &str) -> Result<(), String> {
    const SNAPSHOT_NAME_REGEXP: &str = "^[0-9]{14}-[0-9A-Fa-f]+$";
    let is_match = snapshot_name.len() > 15
        && snapshot_name.as_bytes()[..14]
            .iter()
            .all(u8::is_ascii_digit)
        && snapshot_name.as_bytes()[14] == b'-'
        && snapshot_name.as_bytes()[15..]
            .iter()
            .all(u8::is_ascii_hexdigit);
    if !is_match {
        return Err(format!(
            "unexpected snapshot name={snapshot_name:?}; it must match {SNAPSHOT_NAME_REGEXP:?} regexp"
        ));
    }
    // Go additionally parses the leading timestamp with the "20060102150405"
    // layout; validate the date-time components the same way.
    let s = &snapshot_name[..14];
    let num = |r: std::ops::Range<usize>| s[r].parse::<u32>().unwrap();
    let (month, day, hour, minute, second) =
        (num(4..6), num(6..8), num(8..10), num(10..12), num(12..14));
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(format!(
            "unexpected timestamp={s:?} in snapshot name; it must match YYYYMMDDhhmmss pattern"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_rows::get_log_rows;
    use crate::rows::Field;
    use crate::tenant_id::TenantID;
    use std::sync::atomic::{AtomicU64, Ordering};

    // PORT NOTE: storage_test.go's TestStoragePartitionDetachRecreate* cases
    // are not ported yet (they drive the attach/detach + stream-filter-query
    // combination). The remaining cases (Lifecycle, MustAddRows,
    // DeleteTaskOps, ProcessDeleteTask,
    // ProcessDeleteTaskRelativeTimeUsesTaskStartTime, DropStalePartitions)
    // are ported below.

    const NSECS_PER_SECOND_T: i64 = 1_000_000_000;

    fn unique_path(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("esl-logstorage-storage-test-{name}-{n}"))
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.as_bytes().to_vec(),
        }
    }

    /// Mirrors the shared Go `newTestLogRows` helper closely enough for the
    /// count-based assertions in the ported tests: it produces exactly
    /// `streams * rows_per_stream` rows across the requested number of streams.
    /// Timestamps are overwritten by the callers, as in Go.
    fn new_test_log_rows(streams: usize, rows_per_stream: usize, seed: i64) -> LogRows {
        let stream_tags = ["some-stream-tag"];
        let mut lr = get_log_rows(&stream_tags, &[], &[], &[], "");
        let mut fields: Vec<Field> = Vec::new();
        for i in 0..streams {
            let tenant_id = TenantID {
                account_id: 0,
                project_id: 0,
            };
            for j in 0..rows_per_stream {
                fields.clear();
                fields.push(field("some-stream-tag", &format!("some-stream-value-{i}")));
                fields.push(field("", &format!("some row number {j} at stream {i}")));
                fields.push(field("job", "foobar"));
                let timestamp = seed * 1_000_000 + (i * rows_per_stream + j) as i64;
                lr.must_add(tenant_id, timestamp, &mut fields, -1);
            }
        }
        lr
    }

    fn now_nanos() -> i64 {
        now_unix_nanos()
    }

    #[test]
    fn test_storage_lifecycle() {
        let path = unique_path("lifecycle");

        for _ in 0..3 {
            let cfg = StorageConfig::default();
            let s = Storage::must_open_storage(&path, &cfg);
            s.must_close();
        }
        fs::must_remove_dir(&path);
    }

    #[test]
    fn test_storage_must_add_rows() {
        let path = unique_path("must-add-rows");

        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);

        // Try adding the same entry multiple times.
        let mut total_rows_count: u64 = 0;
        for _ in 0..100 {
            let mut lr = new_test_log_rows(1, 1, 0);
            lr.timestamps[0] = now_nanos();
            total_rows_count += lr.timestamps.len() as u64;
            s.must_add_rows(&lr);
        }
        s.debug_flush();

        let mut s_stats = StorageStats::default();
        s.update_stats(&mut s_stats);
        assert_eq!(
            s_stats.rows_count(),
            total_rows_count,
            "unexpected number of entries in storage"
        );

        s.must_close();

        // Re-open the storage and verify data survives.
        let s = Storage::must_open_storage(&path, &cfg);
        s_stats.reset();
        s.update_stats(&mut s_stats);
        assert_eq!(
            s_stats.rows_count(),
            total_rows_count,
            "unexpected number of entries in storage after reopen"
        );

        let mut lr = new_test_log_rows(3, 10, 0);
        for i in 0..lr.timestamps.len() {
            lr.timestamps[i] = now_nanos();
        }
        total_rows_count += lr.timestamps.len() as u64;
        s.must_add_rows(&lr);
        s.debug_flush();
        s_stats.reset();
        s.update_stats(&mut s_stats);
        assert_eq!(
            s_stats.rows_count(),
            total_rows_count,
            "unexpected number of entries in storage"
        );

        s.must_close();

        // Re-open with big retention and write data across days (past + future).
        let cfg = StorageConfig {
            retention: 365 * 24 * NSECS_PER_HOUR,
            future_retention: 365 * 24 * NSECS_PER_HOUR,
            ..Default::default()
        };
        let s = Storage::must_open_storage(&path, &cfg);

        let mut lr = new_test_log_rows(3, 10, 0);
        let mut now = now_nanos() - (lr.timestamps.len() as i64 / 2) * NSECS_PER_DAY;
        for i in 0..lr.timestamps.len() {
            lr.timestamps[i] = now;
            now += NSECS_PER_DAY;
        }
        total_rows_count += lr.timestamps.len() as u64;
        s.must_add_rows(&lr);
        s.debug_flush();
        s_stats.reset();
        s.update_stats(&mut s_stats);
        assert_eq!(
            s_stats.rows_count(),
            total_rows_count,
            "unexpected number of entries in storage across days"
        );

        s.must_close();

        // Make sure the stats is valid after re-opening the storage.
        let s = Storage::must_open_storage(&path, &cfg);
        s_stats.reset();
        s.update_stats(&mut s_stats);
        assert_eq!(
            s_stats.rows_count(),
            total_rows_count,
            "unexpected number of entries in storage after final reopen"
        );
        s.must_close();

        fs::must_remove_dir(&path);
    }

    #[test]
    fn test_storage_delete_task_ops() {
        let path = unique_path("delete-task-ops");
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);

        let task_id = "task_id_1";
        let timestamp: i64 = 1_234_567_890_123_456_789;
        let tenant_ids = vec![TenantID {
            account_id: 123,
            project_id: 456,
        }];

        // PORT NOTE: Go parses `app:=foo _msg:SECRET` into a *Filter and stores
        // f.String() == "app:=foo SECRET". Filter is Layer-4, so the port
        // passes that canonical string directly.
        let filter = "app:=foo SECRET";

        // Register delete task.
        s.delete_run_task(task_id, timestamp, tenant_ids, filter)
            .expect("unexpected error in delete_run_task");

        // Verify that the delete task is registered.
        let dts = s.delete_active_tasks();
        let result = crate::delete_task::marshal_delete_tasks_to_json(&dts);
        let result_expected = r#"[{"task_id":"task_id_1","tenant_ids":[{"account_id":123,"project_id":456}],"filter":"app:=foo SECRET","start_time":"2009-02-13T23:31:30.123456789Z"}]"#;
        assert_eq!(String::from_utf8(result).unwrap(), result_expected);

        // Stop the registered delete task.
        s.delete_stop_task(task_id)
            .expect("cannot stop the delete task");

        // Verify that the list of delete tasks is empty.
        let dts = s.delete_active_tasks();
        assert_eq!(dts.len(), 0, "unexpected number of deleted tasks");

        s.must_close();
        fs::must_remove_dir(&path);
    }

    /// Go `checkQueryResults` from storage_test.go.
    fn check_query_results(
        s: &Arc<Storage>,
        now: i64,
        tenant_ids: &[TenantID],
        q_str: &str,
        results_expected: &[&str],
    ) {
        let q = match crate::parser::ParseQueryAtTimestamp(q_str, now) {
            Ok(q) => q,
            Err(err) => panic!("cannot parse query {q_str:?}: {err}"),
        };

        let qs = Arc::new(crate::query_stats::QueryStats::default());
        let buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

        let buf_w = Arc::clone(&buf);
        let callback: crate::storage_search::WriteDataBlockFn = Arc::new(
            move |_worker_id, db: &mut crate::storage_search::DataBlock| {
                let mut rows: Vec<Vec<Field>> = vec![Vec::new(); db.rows_count()];

                for c in db.get_columns(false) {
                    for (row_id, v) in c.values.iter().enumerate() {
                        rows[row_id].push(Field {
                            name: c.name.clone(),
                            value: v.clone(),
                        });
                    }
                }

                let mut buf = buf_w.lock().unwrap();
                for r in &rows {
                    marshal_fields_to_json(&mut buf, r);
                    buf.push(b'\n');
                }
            },
        );

        if let Err(err) =
            crate::storage_search::run_query(s, tenant_ids, &q, &[], callback, None, &qs)
        {
            panic!(
                "unexpected error while running query {q_str:?} for tenants {tenant_ids:?}: {err}"
            );
        }

        let mut buf = buf.lock().unwrap().clone();
        if !buf.is_empty() {
            // Drop the last \n
            buf.pop();
        }
        let results_str = String::from_utf8(buf).unwrap();
        let results_str_expected = results_expected.join("\n");
        assert_eq!(
            results_str, results_str_expected,
            "unexpected results for query {q_str:?} at tenants {tenant_ids:?}"
        );
    }

    /// Go `storeRowsForProcessDeleteTaskTest`.
    fn store_rows_for_process_delete_task_test(
        s: &Arc<Storage>,
        tenant_ids: &[TenantID],
        now: i64,
    ) {
        // Generate rows and put them in the storage
        let stream_tags = ["host", "app"];
        let mut lr = get_log_rows(&stream_tags, &[], &[], &[], "");
        let mut fields: Vec<Field> = Vec::new();

        const DAYS: i64 = 7;
        const STREAMS_PER_TENANT: usize = 5;
        const ROWS_PER_DAY_PER_STREAM: usize = 100;

        for row_id in 0..ROWS_PER_DAY_PER_STREAM {
            for stream_id in 0..STREAMS_PER_TENANT {
                // NB: like Go, the _msg/row_id/tenant_id fields accumulate
                // across the (tenantID, dayID) iterations without truncation
                // (`fields = append(fields, ...)` after `fields[:0]` per
                // streamID): later rows deliberately carry duplicate fields.
                fields.clear();
                fields.push(field("host", &format!("host-{stream_id}")));
                fields.push(field("app", &format!("app-{}", 200 + stream_id)));
                for tenant_id in tenant_ids {
                    for day_id in 0..DAYS {
                        fields.push(field(
                            "_msg",
                            &format!(
                                "value #{row_id} at the day {day_id} for the tenantID={tenant_id} and streamID={stream_id}"
                            ),
                        ));
                        fields.push(field("row_id", &format!("{row_id}")));
                        fields.push(field("tenant_id", &tenant_id.to_string()));
                        let timestamp = now - day_id * NSECS_PER_DAY;
                        lr.must_add(*tenant_id, timestamp, &mut fields, -1);
                        if lr.need_flush() {
                            s.must_add_rows(&lr);
                            lr.reset_keep_settings();
                        }
                    }
                }
            }
        }
        s.must_add_rows(&lr);

        s.debug_flush();
    }

    // Full port of Go's TestStorageRunQueryProcessDeleteTask: delete execution
    // through the block_stream_merger drop-filter path, including deletes whose
    // filter contains a stream filter (`{host=~...}`) — the merge materializes
    // `_stream` from the partition indexdb and evaluates the filter per row.
    #[test]
    fn test_storage_process_delete_task() {
        let path = unique_path("process-delete-task");

        let cfg = StorageConfig {
            retention: 30 * 24 * NSECS_PER_HOUR,
            ..Default::default()
        };
        let s = Storage::must_open_storage(&path, &cfg);

        let now = now_nanos();

        let check = |tenant_ids: &[TenantID], filters: &str, rows_expected: &[&str]| {
            check_query_results(&s, now, tenant_ids, filters, rows_expected);
        };

        let delete_rows = |tenant_ids: &[TenantID], filters: &str| {
            let dt =
                crate::delete_task::new_delete_task("task_id_x", now, tenant_ids.to_vec(), filters);
            let cancel = Arc::new(AtomicBool::new(false));
            while !s.process_delete_task(&cancel, &dt) {
                // Unsuccessful attempt because of concurrently executed background merges.
                // Wait for a bit and try again.
                std::thread::sleep(Duration::from_millis(10));
            }
        };

        let all_tenant_ids = [
            TenantID {
                account_id: 0,
                project_id: 100,
            },
            TenantID {
                account_id: 123,
                project_id: 0,
            },
            TenantID {
                account_id: 123,
                project_id: 456,
            },
        ];

        store_rows_for_process_delete_task_test(&s, &all_tenant_ids, now);

        // Verify that all the rows are properly stored across all the tenants
        check(
            &all_tenant_ids,
            "* | count(host) rows",
            &[r#"{"rows":"10500"}"#],
        );
        for tenant_id in &all_tenant_ids {
            check_query_results(
                &s,
                now,
                std::slice::from_ref(tenant_id),
                "* | count(host) rows",
                &[r#"{"rows":"3500"}"#],
            );
        }
        check(
            &[all_tenant_ids[0], all_tenant_ids[2]],
            "* | count(host) rows",
            &[r#"{"rows":"7000"}"#],
        );

        // Try deleting non-existing logs
        check(
            &all_tenant_ids,
            "row_id:=foobar | count(host) rows",
            &[r#"{"rows":"0"}"#],
        );
        delete_rows(&all_tenant_ids, "row_id:=foobar");
        check(
            &all_tenant_ids,
            "* | count(host) rows",
            &[r#"{"rows":"10500"}"#],
        );

        // Delete logs with the given row_id across all the tenants
        check(
            &all_tenant_ids,
            "row_id:=42 | count(host) rows",
            &[r#"{"rows":"105"}"#],
        );
        delete_rows(&all_tenant_ids, "row_id:=42");
        check(
            &all_tenant_ids,
            "row_id:=42 | count(host) rows",
            &[r#"{"rows":"0"}"#],
        );
        check(
            &all_tenant_ids,
            "row_id:!=42 | count(host) rows",
            &[r#"{"rows":"10395"}"#],
        );
        check(
            &all_tenant_ids,
            "* | count(host) rows",
            &[r#"{"rows":"10395"}"#],
        );

        // Delete logs for the given row_id at two tenants
        let tenant_ids = [all_tenant_ids[0], all_tenant_ids[2]];
        check(
            &all_tenant_ids,
            "row_id:=10 | count(host) rows",
            &[r#"{"rows":"105"}"#],
        );
        check(
            &tenant_ids,
            "row_id:=10 | count(host) rows",
            &[r#"{"rows":"70"}"#],
        );
        delete_rows(&tenant_ids, "row_id:=10");
        check(
            &tenant_ids,
            "row_id:=10 | count(host) rows",
            &[r#"{"rows":"0"}"#],
        );
        check(
            &all_tenant_ids,
            "row_id:=10 | count(host) rows",
            &[r#"{"rows":"35"}"#],
        );
        check(
            &all_tenant_ids,
            "* | count(host) rows",
            &[r#"{"rows":"10325"}"#],
        );

        // Delete all the logs for the particular tenant
        let tenant_ids = [all_tenant_ids[1]];
        check(&tenant_ids, "* | count(host) rows", &[r#"{"rows":"3465"}"#]);
        delete_rows(&tenant_ids, "*");
        check(&tenant_ids, "* | count(host) rows", &[r#"{"rows":"0"}"#]);
        check(
            &all_tenant_ids,
            "* | count(host) rows",
            &[r#"{"rows":"6860"}"#],
        );

        // Delete all the logs for the particular day
        let filter = "_time:1d offset 2d";
        check(
            &all_tenant_ids,
            &format!("{filter} | count(host) rows"),
            &[r#"{"rows":"980"}"#],
        );
        delete_rows(&all_tenant_ids, filter);
        check(
            &all_tenant_ids,
            &format!("{filter} | count(host) rows"),
            &[r#"{"rows":"0"}"#],
        );
        check(
            &all_tenant_ids,
            "* | count(host) rows",
            &[r#"{"rows":"5880"}"#],
        );

        // Delete logs by _stream filter at the particular tenant
        let tenant_ids = [all_tenant_ids[0]];
        let filter = r#"{host="host-4",app=~"app-.+"}"#;
        check(
            &tenant_ids,
            &format!("{filter} | count(host) rows"),
            &[r#"{"rows":"588"}"#],
        );
        delete_rows(&tenant_ids, filter);
        check(
            &tenant_ids,
            &format!("{filter} | count(host) rows"),
            &[r#"{"rows":"0"}"#],
        );
        check(
            &all_tenant_ids,
            "* | count(host) rows",
            &[r#"{"rows":"5292"}"#],
        );

        // Delete logs by composite filter at the particular tenant
        let tenant_ids = [all_tenant_ids[2]];
        let filter = r#"(_msg:3 row_id:23 _time:2d) or (row_id:56 {host=~"host-[23]"} app:*02* tenant_id:~"56")"#;
        check(
            &tenant_ids,
            &format!("{filter} | count(host) rows"),
            &[r#"{"rows":"8"}"#],
        );
        delete_rows(&tenant_ids, filter);
        check(
            &tenant_ids,
            &format!("{filter} | count(host) rows"),
            &[r#"{"rows":"0"}"#],
        );
        check(
            &all_tenant_ids,
            "* | count(host) rows",
            &[r#"{"rows":"5284"}"#],
        );

        s.must_close();

        fs::must_remove_dir(&path);
    }

    #[test]
    fn test_storage_process_delete_task_relative_time_uses_task_start_time() {
        let path = unique_path("process-delete-task-relative-time");

        let cfg = StorageConfig {
            retention: 30 * 24 * NSECS_PER_HOUR,
            future_retention: 30 * 24 * NSECS_PER_HOUR,
            ..Default::default()
        };
        let s = Storage::must_open_storage(&path, &cfg);

        let tenant_ids = [TenantID {
            account_id: 123,
            project_id: 456,
        }];

        let now = now_nanos() - 2 * NSECS_PER_SECOND_T;
        let row_timestamp = now - 500_000_000;

        let mut lr = get_log_rows(&["host"], &[], &[], &[], "");
        let mut fields = vec![field("host", "host-1"), field("row_id", "1")];
        lr.must_add(tenant_ids[0], row_timestamp, &mut fields, -1);
        s.must_add_rows(&lr);
        s.debug_flush();

        let check = |q_str: &str, results_expected: &[&str]| {
            check_query_results(&s, now, &tenant_ids, q_str, results_expected);
        };

        check("row_id:=1 | stats count(*) as rows", &[r#"{"rows":"1"}"#]);

        let dt = crate::delete_task::new_delete_task(
            "task_id_relative",
            now,
            tenant_ids.to_vec(),
            "_time:1s row_id:=1",
        );
        let cancel = Arc::new(AtomicBool::new(false));
        while !s.process_delete_task(&cancel, &dt) {
            std::thread::sleep(Duration::from_millis(10));
        }

        check("row_id:=1 | stats count(*) as rows", &[r#"{"rows":"0"}"#]);

        s.must_close();
        fs::must_remove_dir(&path);
    }

    #[test]
    fn test_storage_drop_stale_partitions() {
        let path = unique_path("drop-stale-partitions");

        let cfg = StorageConfig {
            retention: 30 * 24 * NSECS_PER_HOUR,
            ..Default::default()
        };
        let s = Storage::must_open_storage(&path, &cfg);

        let expect_partitions_number = |s: &Arc<Storage>, want: usize| {
            let pws = s.get_partitions();
            let got = pws.len();
            s.put_partitions(&pws);
            assert_eq!(got, want, "unexpected number of partitions");
        };

        let tenant_id = TenantID::default();
        let mut timestamp = now_nanos() - 10 * NSECS_PER_DAY;
        timestamp -= timestamp % NSECS_PER_DAY;
        let mut lr = get_log_rows(&[], &[], &[], &[], "");
        let mut fields: Vec<Field> = Vec::new();
        for i in 0..100 {
            fields.clear();
            fields.push(field("_msg", &format!("message #{i}")));
            timestamp += NSECS_PER_SECOND_T;
            lr.must_add(tenant_id, timestamp, &mut fields, -1);
        }

        s.drop_stale_partitions();
        expect_partitions_number(&s, 0);
        s.must_add_rows(&lr);
        s.debug_flush();
        s.drop_stale_partitions();
        expect_partitions_number(&s, 1);
        s.must_close();

        // Open the storage with the same retention and verify partitions still exist.
        let s = Storage::must_open_storage(&path, &cfg);
        expect_partitions_number(&s, 1);
        s.must_close();

        // Open the storage with smaller retention and drop stale partitions.
        let cfg = StorageConfig {
            retention: 24 * NSECS_PER_HOUR,
            ..Default::default()
        };
        let s = Storage::must_open_storage(&path, &cfg);
        s.drop_stale_partitions();
        expect_partitions_number(&s, 0);
        s.must_close();

        fs::must_remove_dir(&path);
    }

    #[test]
    fn test_time_formatter() {
        let tf = TimeFormatter(1_234_567_890_123_456_789);
        assert_eq!(tf.to_string(), "2009-02-13T23:31:30.123456789Z");
    }

    #[test]
    fn test_snapshotutil_validate() {
        assert!(snapshotutil_validate("20240102030405-0A1b2C3d").is_ok());
        assert!(snapshotutil_validate("2024010203040-0A1b2C3d").is_err());
        assert!(snapshotutil_validate("20241302030405-0A1b2C3d").is_err());
        assert!(snapshotutil_validate("20240102030405-").is_err());
        assert!(snapshotutil_validate("20240102030405-zz").is_err());
    }

    #[test]
    fn test_duration_to_days() {
        assert_eq!(duration_to_days(NSECS_PER_DAY), 1);
        assert_eq!(duration_to_days(30 * NSECS_PER_DAY), 30);
        assert_eq!(duration_to_days(NSECS_PER_HOUR), 0);
    }
}

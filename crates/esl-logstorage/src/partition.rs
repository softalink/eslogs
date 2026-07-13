//! Port of EsLogs `lib/logstorage/partition.go`.
//!
//! PORT NOTE — ownership: Go's `pt *partition` stores `s *Storage` and
//! `idb *indexdb` back-references and is shared by raw pointer among the datadb
//! back-reference and background merge workers. The port uses:
//!   - `Arc<Partition>`; the datadb holds a `Weak<Partition>` (see datadb.rs) to
//!     break the partition → datadb → part → partition cycle.
//!   - `s: Weak<Storage>` (instead of `s *Storage`) to break the
//!     Storage → partition → Storage strong cycle. Storage always outlives its
//!     partitions (its `must_close` closes partitions before the caches/Storage
//!     are torn down), so `upgrade()` always succeeds while the partition is in
//!     use.
//!   - `idb: Arc<Indexdb>` opened in `must_open_partition` and closed in
//!     `must_close_partition`. The indexdb does not reference the partition, so
//!     (unlike the datadb) it is a plain field constructed before the
//!     `Arc<Partition>`.
//!
//! PORT NOTE — the `logIngestedRows` / `logNewStream` debug paths are gated by
//! `Storage.log_ingested_rows` / `Storage.log_new_streams` (both default off)
//! and are ported faithfully.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use esl_common::{encoding, fs, infof, panicf, warnf};

use crate::datadb::{Datadb, DatadbStats, must_close_datadb, must_create_datadb, must_open_datadb};
use crate::filenames::{DATADB_DIRNAME, INDEXDB_DIRNAME, SNAPSHOTS_DIRNAME};
use crate::indexdb::{
    Indexdb, IndexdbStats, must_close_indexdb, must_create_indexdb, must_open_indexdb,
};
use crate::log_rows::LogRows;
use crate::rows::{Field, marshal_fields_to_json};
use crate::storage::Storage;
use crate::stream_id::StreamID;
use crate::stream_tags::get_stream_tags_string;
use crate::values_encoder::NSECS_PER_DAY;

/// PartitionStats contains stats for the partition.
#[derive(Debug, Default, Clone)]
pub struct PartitionStats {
    pub datadb_stats: DatadbStats,
    pub indexdb_stats: IndexdbStats,
}

/// partition is a partition (basically, a per-day directory) for the log data.
pub struct Partition {
    /// s is the parent storage for the partition.
    ///
    /// PORT NOTE: `Weak<Storage>` instead of Go's `s *Storage`; see the module
    /// header for the cycle-breaking rationale.
    pub(crate) s: Weak<Storage>,

    /// path is the path to the partition directory
    pub(crate) path: PathBuf,

    /// name is the partition name. It is basically the directory name obtained from path.
    /// It is used for creating keys for partition caches.
    pub(crate) name: String,

    /// idb is the indexdb used for the given partition.
    pub(crate) idb: Arc<Indexdb>,

    /// ddb is the datadb used for the given partition
    ///
    /// PORT NOTE: Go initializes `pt.ddb` after constructing pt (datadb keeps
    /// a `Weak` back-reference to pt) and nils it in mustClosePartition(); the
    /// port fills this `OnceLock` right after the `Arc<Partition>` is created
    /// and leaves it set until the partition is dropped.
    pub(crate) ddb: OnceLock<Arc<Datadb>>,

    /// The snapshotLock prevents from concurrent creation of snapshots,
    /// since this may result in snapshots without recently added data,
    /// which may be in the process of flushing to disk by concurrently running
    /// snapshot process.
    snapshot_lock: Mutex<()>,
}

impl Partition {
    /// Returns the datadb for the partition (Go: `pt.ddb`).
    pub(crate) fn ddb(&self) -> &Arc<Datadb> {
        self.ddb
            .get()
            .expect("BUG: partition datadb must be initialized")
    }
}

/// mustCreatePartition creates a partition at the given path.
///
/// The created partition can be opened with mustOpenPartition() after is has been created.
///
/// The created partition can be deleted with mustDeletePartition() when it is no longer needed.
pub(crate) fn must_create_partition(path: &Path) {
    fs::must_mkdir_fail_if_exist(path);

    let indexdb_path = path.join(INDEXDB_DIRNAME);
    must_create_indexdb(&path_to_str(&indexdb_path));

    let datadb_path = path.join(DATADB_DIRNAME);
    must_create_datadb(&datadb_path);

    fs::must_sync_path_and_parent_dir(path);
}

/// mustDeletePartition deletes partition at the given path.
///
/// The partition must be closed with MustClose before deleting it.
pub(crate) fn must_delete_partition(path: &Path) {
    fs::must_remove_dir(path);
}

/// Converts a filesystem path to the `&str` the indexdb API expects.
///
/// PORT NOTE: Go passes paths as `string` everywhere; the port keeps `PathBuf`
/// for the datadb side but the indexdb port takes `&str`, so partition paths are
/// converted at the boundary. Non-UTF-8 paths are unexpected here (partition
/// names are ASCII `YYYYMMDD`), so a lossy conversion is safe and matches Go.
fn path_to_str(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// mustOpenPartition opens partition at the given path for the given Storage.
///
/// The returned partition must be closed when no longer needed with
/// mustClosePartition() call.
///
/// PORT NOTE: Go takes `s *Storage`; the port takes `&Arc<Storage>` and stores
/// a `Weak<Storage>` (see the module header). The datadb flush interval is read
/// from `s.flush_interval`, matching Go's `mustOpenDatadb(pt, datadbPath,
/// s.flushInterval)`.
pub(crate) fn must_open_partition(path: &Path, s: &Arc<Storage>) -> Arc<Partition> {
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();

    let indexdb_path = path.join(INDEXDB_DIRNAME);
    let is_indexdb_exist = fs::is_path_exist(&indexdb_path);

    let datadb_path = path.join(DATADB_DIRNAME);
    let is_datadb_exist = fs::is_path_exist(&datadb_path);

    if !is_indexdb_exist {
        if is_datadb_exist {
            panicf!(
                "FATAL: indexdb directory {} is missing, but datadb directory {} exists. This indicates corruption. Manually remove the {} partition to resolve it (partition data will be lost)",
                indexdb_path.display(),
                datadb_path.display(),
                path.display()
            );
        }

        warnf!(
            "creating missing indexdb directory {}, this could happen if EsLogs shuts down uncleanly (via OOM crash, a panic, SIGKILL or hardware shutdown) while creating new per-day partition",
            indexdb_path.display()
        );
        must_create_indexdb(&path_to_str(&indexdb_path));
    }

    // Open indexdb before constructing the partition (Go does `idb :=
    // mustOpenIndexdb(...)` then stores it on pt). The indexdb keeps only a
    // Weak<Storage>, not a partition back-reference, so it needs no OnceLock.
    let idb = must_open_indexdb(&path_to_str(&indexdb_path), &name, s);

    // Start initializing the partition.
    let pt = Arc::new(Partition {
        s: Arc::downgrade(s),
        path: path.to_path_buf(),
        name,
        idb,
        ddb: OnceLock::new(),
        snapshot_lock: Mutex::new(()),
    });

    if !is_datadb_exist {
        warnf!(
            "creating missing datadb directory {}, this could happen if EsLogs shuts down uncleanly (via OOM crash, a panic, SIGKILL or hardware shutdown) while creating new per-day partition",
            datadb_path.display()
        );
        must_create_datadb(&datadb_path);
    }

    let ddb = must_open_datadb(Arc::downgrade(&pt), &datadb_path, s.flush_interval);
    if pt.ddb.set(ddb).is_err() {
        panicf!("BUG: partition datadb was already initialized");
    }

    pt
}

/// mustClosePartition closes pt.
///
/// The caller must ensure that pt is no longer used before the call to mustClosePartition().
///
/// The partition can be deleted if needed after it is closed via mustDeletePartition() call.
pub(crate) fn must_close_partition(pt: &Arc<Partition>) {
    // Close indexdb
    must_close_indexdb(&pt.idb);

    // Close datadb
    must_close_datadb(pt.ddb());

    // PORT NOTE: Go nils pt.idb/pt.ddb/pt.name/pt.path/pt.s here; the port
    // leaves the fields immutable — the partition is dropped by its owner once
    // the last Arc reference goes away.
}

impl Partition {
    /// Returns the parent Storage (Go: `pt.s`). Panics if Storage was dropped,
    /// which cannot happen while the partition is in use (see the module note).
    fn storage(&self) -> Arc<Storage> {
        self.s
            .upgrade()
            .expect("BUG: Storage dropped while partition is still alive")
    }

    pub(crate) fn must_add_rows(&self, lr: &LogRows) {
        let s = self.storage();

        // Register rows in indexdb.
        let stream_ids = &lr.stream_ids;
        let mut pending_rows: Vec<usize> = Vec::new();
        for i in 0..lr.timestamps.len() {
            let stream_id = &stream_ids[i];
            if self.has_stream_id_in_cache(&s, stream_id) {
                continue;
            }
            if pending_rows.is_empty()
                || !stream_ids[pending_rows[pending_rows.len() - 1]].equal(stream_id)
            {
                pending_rows.push(i);
            }
        }
        if !pending_rows.is_empty() {
            let log_new_streams = s.log_new_streams.load(Ordering::SeqCst);
            let stream_tags_canonicals = &lr.stream_tags_canonicals;
            pending_rows.sort_by(|&a, &b| {
                if stream_ids[a].less(&stream_ids[b]) {
                    std::cmp::Ordering::Less
                } else if stream_ids[b].less(&stream_ids[a]) {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            });
            for i in 0..pending_rows.len() {
                let row_idx = pending_rows[i];
                let stream_id = &stream_ids[row_idx];
                if i > 0 && stream_ids[pending_rows[i - 1]].equal(stream_id) {
                    continue;
                }
                if self.has_stream_id_in_cache(&s, stream_id) {
                    continue;
                }
                if !self.idb.has_stream_id(stream_id) {
                    let stream_tags_canonical = &stream_tags_canonicals[row_idx];
                    self.idb
                        .must_register_stream(stream_id, stream_tags_canonical);
                    if log_new_streams {
                        self.log_new_stream(stream_tags_canonical, &lr.rows[row_idx]);
                    }
                }
                self.put_stream_id_to_cache(&s, stream_id);
            }
        }

        // Add rows to datadb
        self.ddb().must_add_rows(lr);
        if s.log_ingested_rows {
            self.log_ingested_rows(lr);
        }
    }

    fn log_new_stream(&self, stream_tags_canonical: &[u8], fields: &[Field]) {
        let stream_tags = get_stream_tags_string(stream_tags_canonical);
        let mut line = Vec::new();
        marshal_fields_to_json(&mut line, fields);
        infof!(
            "partition {}: new stream {} for log entry {}",
            self.path.display(),
            stream_tags,
            String::from_utf8_lossy(&line)
        );
    }

    fn log_ingested_rows(&self, lr: &LogRows) {
        for i in 0..lr.rows.len() {
            let s = lr.get_row_string(i);
            infof!("partition {}: new log entry {}", self.path.display(), s);
        }
    }

    fn has_stream_id_in_cache(&self, s: &Storage, sid: &StreamID) -> bool {
        let mut key = Vec::new();
        self.marshal_stream_id_cache_key(s, &mut key, sid);
        // PORT NOTE: Go's `s.streamIDCache` is a plain *Cache; the port's Storage
        // keeps it in a `Mutex<Option<Cache>>` (so `must_close` can stop the
        // cleaner). The lock is held only for the O(1) lookup. The Cache is
        // always present while the partition is alive (partitions close first).
        let guard = s.stream_id_cache.lock().unwrap();
        let cache = guard
            .as_ref()
            .expect("BUG: streamIDCache stopped while partition is alive");
        cache.get(&key).is_some()
    }

    fn put_stream_id_to_cache(&self, s: &Storage, sid: &StreamID) {
        let mut key = Vec::new();
        self.marshal_stream_id_cache_key(s, &mut key, sid);
        // PORT NOTE: Go stores a nil value (only the key's presence matters); the
        // Rust cache requires a value, so an empty marker `Arc::new(())` is used.
        let value: crate::cache::CacheValue = Arc::new(());
        let guard = s.stream_id_cache.lock().unwrap();
        let cache = guard
            .as_ref()
            .expect("BUG: streamIDCache stopped while partition is alive");
        cache.set(&key, value);
    }

    fn marshal_stream_id_cache_key(&self, s: &Storage, dst: &mut Vec<u8>, sid: &StreamID) {
        encoding::marshal_uint64(dst, s.partition_cache_generation.load(Ordering::SeqCst));
        encoding::marshal_bytes(dst, self.name.as_bytes());
        sid.marshal(dst);
    }

    /// debugFlush makes sure that all the recently ingested data becomes searchable.
    pub(crate) fn debug_flush(&self) {
        self.ddb().debug_flush();
        self.idb.debug_flush();
    }

    /// mustCreateSnapshot creates a snapshot for the given pt and returns the
    /// full path to the created snapshot.
    pub(crate) fn must_create_snapshot(&self) -> PathBuf {
        infof!("creating a snapshot for partition {:?}", self.name);
        let start_time = std::time::Instant::now();

        let _snapshot_guard = self.snapshot_lock.lock().unwrap();

        let snapshot_name = new_snapshot_name();
        let dst_dir = self.path.join(SNAPSHOTS_DIRNAME).join(&snapshot_name);
        fs::must_mkdir_fail_if_exist(&dst_dir);

        let dst_indexdb_dir = dst_dir.join(INDEXDB_DIRNAME);
        self.idb
            .must_create_snapshot_at(&path_to_str(&dst_indexdb_dir));

        let dst_datadb_dir = dst_dir.join(DATADB_DIRNAME);
        self.ddb().must_create_snapshot_at(&dst_datadb_dir);

        fs::must_sync_path_and_parent_dir(&dst_dir);

        infof!(
            "created a snapshot for partition {:?} at {:?} in {:.3} seconds",
            self.name,
            dst_dir,
            start_time.elapsed().as_secs_f64()
        );

        dst_dir
    }

    /// deleteSnapshot removes the snapshot with the given snapshotName from the pt.
    pub(crate) fn delete_snapshot(&self, snapshot_name: &str) -> Result<(), String> {
        infof!(
            "deleting snapshot {:?} for partition {:?}",
            snapshot_name,
            self.name
        );

        let _snapshot_guard = self.snapshot_lock.lock().unwrap();

        let snapshot_path = self.path.join(SNAPSHOTS_DIRNAME).join(snapshot_name);
        if !fs::is_path_exist(&snapshot_path) {
            return Err(format!(
                "snapshot {:?} doesn't exist at {:?}",
                snapshot_name, self.path
            ));
        }

        fs::must_remove_dir(&snapshot_path);

        infof!(
            "deleted snapshot {:?} for partition {:?} at {:?}",
            snapshot_name,
            self.name,
            snapshot_path
        );

        Ok(())
    }

    pub(crate) fn update_stats(&self, ps: &mut PartitionStats) {
        self.ddb().update_stats(&mut ps.datadb_stats);
        self.idb.update_stats(&mut ps.indexdb_stats);
    }

    /// mustForceMerge runs forced merge for all the parts in pt.
    pub(crate) fn must_force_merge(&self) {
        self.ddb().must_force_merge_all_parts();
    }

    /// Deletes the rows matching sso (Go `partition.deleteRows`).
    ///
    /// Returns false if the deletion couldn't be fully performed at the
    /// moment, so it must be repeated later.
    pub(crate) fn delete_rows(
        &self,
        sso: &crate::storage_search::StorageSearchOptions<'_>,
        stop_ch: &std::sync::atomic::AtomicBool,
    ) -> bool {
        // make recently ingested rows visible for search, so they could be deleted.
        self.debug_flush();

        let pso = crate::storage_search::partition_search_options(sso, self);
        self.ddb().delete_rows(&pso, stop_ch)
    }
}

/// Returns the day for the given partition name.
pub(crate) fn get_partition_day_from_name(name: &str) -> Result<i64, String> {
    let t = parse_partition_name(name).ok_or_else(|| {
        format!("cannot parse partition name {name:?}; it must have the format YYYYMMDD")
    })?;
    Ok(t / NSECS_PER_DAY)
}

/// Returns the partition name for the given day.
pub(crate) fn get_partition_name_from_day(day: i64) -> String {
    let nsecs = day * NSECS_PER_DAY;
    let days = nsecs / NSECS_PER_DAY;
    let (year, month, mday) = civil_from_days(days);
    format!("{year:04}{month:02}{mday:02}")
}

const PARTITION_NAME_FORMAT_LEN: usize = 8; // "20060102"

/// Parses a partition name in the YYYYMMDD format into UTC nanoseconds.
fn parse_partition_name(name: &str) -> Option<i64> {
    if name.len() != PARTITION_NAME_FORMAT_LEN || !name.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let year: i64 = name[0..4].parse().ok()?;
    let month: i64 = name[4..6].parse().ok()?;
    let day: i64 = name[6..8].parse().ok()?;
    if !(1..=12).contains(&month) {
        return None;
    }
    if day < 1 || day > days_in_month(year, month) {
        return None;
    }
    Some(days_from_civil(year, month, day) * NSECS_PER_DAY)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

// PORT NOTE: Go uses time.Parse/time.Format with the "20060102" layout; the
// port replaces this with Howard Hinnant's branchless civil-date algorithms
// (there is no chrono dependency and only the UTC day boundary matters).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// PORT NOTE: stands in for snapshotutil.NewName()
/// (`YYYYMMDDhhmmss-%08X`); pending a snapshotutil port, the port uses a
/// monotically increasing index seeded from the current time, matching Go's
/// nextSnapshotIdx() semantics.
fn new_snapshot_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static SNAPSHOT_IDX: OnceLock<AtomicU64> = OnceLock::new();
    let idx = SNAPSHOT_IDX.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        AtomicU64::new(nanos)
    });
    let n = idx.fetch_add(1, Ordering::SeqCst) + 1;

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    let (year, month, mday) = civil_from_days(days);
    let (hh, mm, ss) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    format!("{year:04}{month:02}{mday:02}{hh:02}{mm:02}{ss:02}-{n:08X}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_rows::{LogRows, get_log_rows};
    use crate::rows::Field;
    use crate::storage::{Storage, StorageConfig};
    use crate::tenant_id::TenantID;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    // PORT NOTE: Go's partition_test.go uses newTestStorage()/closeTestStorage()
    // to build a *Storage carrying the flushInterval and streamID caches. Now
    // that partition↔indexdb↔storage is wired, the port opens a real Storage in
    // a throwaway temp dir and passes it to must_open_partition; the caller
    // closes it at the end of the test. flush_interval is 1s (Go's value).
    fn new_test_storage() -> Arc<Storage> {
        let dir = unique_path("storage");
        esl_common::fs::must_remove_dir(&dir);
        let cfg = StorageConfig {
            flush_interval: 1_000_000_000, // time.Second
            ..Default::default()
        };
        Storage::must_open_storage(&dir, &cfg)
    }

    fn close_test_storage(s: &Arc<Storage>) {
        let storage_path = s.path.clone();
        s.must_close();
        esl_common::fs::must_remove_dir(&storage_path);
    }

    fn unique_path(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("esl-logstorage-partition-test-{name}-{n}"))
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    // PORT NOTE: newTestLogRows mirrors the shared Go test helper; a
    // deterministic offset feeds distinct timestamps/streams so rows land in
    // the intended number of streams.
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

    #[test]
    fn test_partition_lifecycle() {
        let path = unique_path("lifecycle");
        let s = new_test_storage();

        for _ in 0..3 {
            must_create_partition(&path);
            for _ in 0..2 {
                let pt = must_open_partition(&path, &s);
                let mut ddb_stats = DatadbStats::default();
                pt.ddb().update_stats(&mut ddb_stats);
                assert_eq!(
                    ddb_stats.rows_count(),
                    0,
                    "unexpected non-zero number of entries in empty partition"
                );
                assert_eq!(
                    ddb_stats.inmemory_parts, 0,
                    "unexpected non-zero number of in-memory parts in empty partition"
                );
                assert_eq!(
                    ddb_stats.small_parts, 0,
                    "unexpected non-zero number of small file parts in empty partition"
                );
                assert_eq!(
                    ddb_stats.big_parts, 0,
                    "unexpected non-zero number of big file parts in empty partition"
                );
                assert_eq!(
                    ddb_stats.compressed_inmemory_size, 0,
                    "unexpected non-zero size of inmemory parts for empty partition"
                );
                assert_eq!(
                    ddb_stats.compressed_small_part_size, 0,
                    "unexpected non-zero size of small file parts for empty partition"
                );
                assert_eq!(
                    ddb_stats.compressed_big_part_size, 0,
                    "unexpected non-zero size of big file parts for empty partition"
                );
                std::thread::sleep(Duration::from_millis(10));
                must_close_partition(&pt);
            }
            must_delete_partition(&path);
        }

        close_test_storage(&s);
    }

    #[test]
    fn test_partition_must_add_rows_serial() {
        let path = unique_path("add-rows-serial");
        let s = new_test_storage();

        must_create_partition(&path);
        let mut pt = must_open_partition(&path, &s);

        // Try adding the same entry at a time.
        let mut total_rows_count = 0u64;
        for _ in 0..100 {
            let lr = new_test_log_rows(1, 1, 0);
            total_rows_count += lr.rows_count() as u64;
            pt.must_add_rows(&lr);
        }
        pt.debug_flush();

        let mut ddb_stats = DatadbStats::default();
        pt.ddb().update_stats(&mut ddb_stats);
        assert_eq!(
            ddb_stats.rows_count(),
            total_rows_count,
            "unexpected number of entries in partition"
        );

        // Try adding different entry at a time.
        for i in 0..100 {
            let lr = new_test_log_rows(1, 1, i);
            total_rows_count += lr.rows_count() as u64;
            pt.must_add_rows(&lr);
        }
        pt.debug_flush();

        ddb_stats.reset();
        pt.ddb().update_stats(&mut ddb_stats);
        assert_eq!(
            ddb_stats.rows_count(),
            total_rows_count,
            "unexpected number of entries in partition"
        );

        // Re-open the partition and verify the number of entries remains the same.
        must_close_partition(&pt);
        pt = must_open_partition(&path, &s);
        ddb_stats.reset();
        pt.ddb().update_stats(&mut ddb_stats);
        assert_eq!(
            ddb_stats.rows_count(),
            total_rows_count,
            "unexpected number of entries after re-opening the partition"
        );
        assert_eq!(
            ddb_stats.inmemory_parts, 0,
            "unexpected non-zero number of in-memory parts after re-opening the partition"
        );
        assert!(
            ddb_stats.small_parts + ddb_stats.big_parts > 0,
            "the number of small parts must be greater than 0 after re-opening the partition"
        );

        // Try adding entries for multiple streams at a time.
        for _ in 0..5 {
            let lr = new_test_log_rows(3, 7, 0);
            total_rows_count += lr.rows_count() as u64;
            pt.must_add_rows(&lr);
            std::thread::sleep(Duration::from_millis(1));
        }
        pt.debug_flush();

        ddb_stats.reset();
        pt.ddb().update_stats(&mut ddb_stats);
        assert_eq!(
            ddb_stats.rows_count(),
            total_rows_count,
            "unexpected number of entries in partition"
        );

        // Re-open the partition and verify the number of entries remains the same.
        must_close_partition(&pt);
        pt = must_open_partition(&path, &s);
        ddb_stats.reset();
        pt.ddb().update_stats(&mut ddb_stats);
        assert_eq!(
            ddb_stats.rows_count(),
            total_rows_count,
            "unexpected number of entries after re-opening the partition"
        );
        assert_eq!(
            ddb_stats.inmemory_parts, 0,
            "unexpected non-zero number of in-memory parts after re-opening the partition"
        );
        assert!(
            ddb_stats.small_parts + ddb_stats.big_parts > 0,
            "the number of file parts must be greater than 0 after re-opening the partition"
        );

        must_close_partition(&pt);
        must_delete_partition(&path);
        close_test_storage(&s);
    }

    #[test]
    fn test_partition_must_add_rows_concurrent() {
        let path = unique_path("add-rows-concurrent");
        let s = new_test_storage();

        must_create_partition(&path);
        let pt = must_open_partition(&path, &s);

        const WORKERS_COUNT: usize = 3;
        let total_rows_count = AtomicU64::new(0);
        std::thread::scope(|s| {
            for _ in 0..WORKERS_COUNT {
                let pt = &pt;
                let total_rows_count = &total_rows_count;
                s.spawn(move || {
                    for j in 0..7 {
                        let lr = new_test_log_rows(5, 10, j);
                        let n = lr.rows_count() as u64;
                        pt.must_add_rows(&lr);
                        total_rows_count.fetch_add(n, Ordering::SeqCst);
                    }
                });
            }
        });
        pt.debug_flush();

        let mut ddb_stats = DatadbStats::default();
        pt.ddb().update_stats(&mut ddb_stats);
        assert_eq!(
            ddb_stats.rows_count(),
            total_rows_count.load(Ordering::SeqCst),
            "unexpected number of entries"
        );

        must_close_partition(&pt);
        must_delete_partition(&path);
        close_test_storage(&s);
    }
}

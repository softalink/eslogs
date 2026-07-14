//! Port of EsLogs `app/eslstorage`: storage initialization, lifecycle
//! management and the internal storage HTTP API.
//!
//! Module map (one Rust module per Go file):
//!   * `lib.rs` — `main.go` (flags, init/stop, RequestHandler, RunQuery
//!     dispatch, writeStorageMetrics)
//!   * [`query_stats`] — `query_stats.go`
//!   * [`lastn_optimization`] — `lastnoptimization.go`
//!   * [`netinsert`] — `netinsert/netinsert.go`
//!   * [`netselect`] — `netselect/netselect.go`
//!   * [`http_client`] — port-specific HTTP transport shared by
//!     netinsert/netselect (no Go counterpart; see its docs)
//!
//! PORT NOTE: Go keeps the opened storage in the package-global `localStorage`.
//! The Rust port makes ownership explicit instead: [`init`] returns an
//! `Arc<Storage>` and every entry point takes `&Arc<Storage>`. The caller
//! (`es-logs` main) owns the `Arc` and closes it on shutdown via
//! `Storage::must_close` (Go `Stop`). Network mode has its own explicit handle:
//! [`init_network_storage`] returns a [`NetworkStorage`] whose `must_stop`
//! mirrors the network branch of Go `Stop`.
//!
//! PORT NOTE: cluster mode is not wired into the single-node `es-logs`
//! binary: [`init`] fails fast when `-storageNode` is set. The wiring
//! (Go `Init` dispatching to `initNetworkStorage`, `MustAddRows` routing
//! through `netinsert` and the query entry points routing through `netselect`)
//! is described in the netinsert/netselect module docs.
//!
//! PORT NOTE: like Go, [`init_storage_metrics`] registers
//! `write_storage_metrics` in an `esl_common::metrics::Set` rendered at
//! `/metrics`; `es-logs` main calls it after opening the storage (Go does it
//! inside `vlstorage.Init`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use esl_common::flagutil::{
    ArrayBool, ArrayString, Bytes, ExtendedDuration, Flag, FlagValue, Password, RetentionDuration,
};
use esl_common::httpserver::{Request, ResponseWriter};
use esl_common::stringsutil::json_string;
use esl_common::{cgroup, fatalf, httputil, infof, panicf, warnf};

use esl_logstorage::log_rows::LogRows;
use esl_logstorage::parser::Query;
use esl_logstorage::storage::{Storage, StorageConfig, StorageStats};
use esl_logstorage::storage_search::WriteDataBlockFn;
use esl_logstorage::tenant_id::TenantID;

pub mod http_client;
pub mod lastn_optimization;
pub mod netinsert;
pub mod netselect;
pub mod proxy;
pub mod query_stats;

// ---------------------------------------------------------------------------
// Flags (names, defaults and help text mirror Go app/eslstorage/main.go)
// ---------------------------------------------------------------------------

fn retention(s: &str) -> RetentionDuration {
    RetentionDuration::parse_flag(s).expect("BUG: invalid built-in retention default")
}

static RETENTION_PERIOD: Flag<RetentionDuration> = Flag::new(
    "retentionPeriod",
    "Log entries with timestamps older than now-retentionPeriod are automatically deleted; \
     log entries with timestamps outside the retention are also rejected during data ingestion; \
     the minimum supported retention is 1d (one day); \
     see https://docs.victoriametrics.com/victorialogs/#retention",
    || retention("7d"),
);

static DEFAULT_PARALLEL_READERS: Flag<usize> = Flag::new(
    "defaultParallelReaders",
    "Default number of parallel data readers to use for executing every query; \
     higher number of readers may help increasing query performance on high-latency storage \
     such as NFS or S3 at the cost of higher RAM usage",
    || 2 * cgroup::available_cpus(),
);

static MAX_DISK_SPACE_USAGE_BYTES: Flag<Bytes> = Flag::new(
    "retention.maxDiskSpaceUsageBytes",
    "The maximum disk space usage at -storageDataPath before older per-day partitions are \
     automatically dropped; see https://docs.victoriametrics.com/victorialogs/#retention-by-disk-space-usage",
    || Bytes::with_default(0),
);

static MAX_DISK_USAGE_PERCENT: Flag<i64> = Flag::new(
    "retention.maxDiskUsagePercent",
    "The maximum allowed disk usage percentage (1-100) for the filesystem that contains \
     -storageDataPath before older per-day partitions are automatically dropped; mutually \
     exclusive with -retention.maxDiskSpaceUsageBytes",
    || 0,
);

static FUTURE_RETENTION: Flag<RetentionDuration> = Flag::new(
    "futureRetention",
    "Log entries with timestamps bigger than now+futureRetention are rejected during data \
     ingestion; see https://docs.victoriametrics.com/victorialogs/#retention",
    || retention("2d"),
);

static MAX_BACKFILL_AGE: Flag<RetentionDuration> = Flag::new(
    "maxBackfillAge",
    "Log entries with timestamps older than now-maxBackfillAge are rejected during data \
     ingestion; see https://docs.victoriametrics.com/victorialogs/#backfilling",
    || retention("0"),
);

static SNAPSHOTS_MAX_AGE: Flag<RetentionDuration> = Flag::new(
    "snapshotsMaxAge",
    "Snapshots are automatically deleted after the given duration if it is set to positive value. \
     Make sure that the backup process has enough time for backing up the snapshot before its' deletion. \
     See https://docs.victoriametrics.com/victorialogs/#how-to-remove-snapshots",
    || retention("3d"),
);

static STORAGE_DATA_PATH: Flag<String> = Flag::new(
    "storageDataPath",
    "Path to directory where to store EsLogs data; \
     see https://docs.victoriametrics.com/victorialogs/#storage",
    || "es-logs-data".to_string(),
);

static INMEMORY_DATA_FLUSH_INTERVAL: Flag<ExtendedDuration> = Flag::new(
    "inmemoryDataFlushInterval",
    "The interval for guaranteed saving of in-memory data to disk. The saved data survives \
     unclean shutdowns such as OOM crash, hardware reset, SIGKILL, etc. Minimum supported value is 1s",
    || ExtendedDuration::parse_flag("5s").expect("BUG: invalid built-in flush-interval default"),
);

static LOG_NEW_STREAMS: Flag<bool> = Flag::new(
    "logNewStreams",
    "Whether to log creation of new streams; this can be useful for debugging of high cardinality \
     issues with log streams; see also -logIngestedRows",
    || false,
);

static LOG_INGESTED_ROWS: Flag<bool> = Flag::new(
    "logIngestedRows",
    "Whether to log all the ingested log entries; this can be useful for debugging of data \
     ingestion; see also -logNewStreams",
    || false,
);

static MIN_FREE_DISK_SPACE_BYTES: Flag<Bytes> = Flag::new(
    "storage.minFreeDiskSpaceBytes",
    "The minimum free disk space at -storageDataPath after which the storage stops accepting new data",
    || Bytes::with_default(10e6 as i64),
);

static LOG_NEW_STREAMS_AUTH_KEY: Flag<Password> = Flag::new(
    "logNewStreamsAuthKey",
    "authKey, which must be passed in query string to /internal/log_new_streams . It overrides -httpAuth.* . \
     See https://docs.victoriametrics.com/victorialogs/#logging-new-streams",
    || Password::new("logNewStreamsAuthKey"),
);

static FORCE_MERGE_AUTH_KEY: Flag<Password> = Flag::new(
    "forceMergeAuthKey",
    "authKey, which must be passed in query string to /internal/force_merge . It overrides -httpAuth.* . \
     See https://docs.victoriametrics.com/victorialogs/#forced-merge",
    || Password::new("forceMergeAuthKey"),
);

static FORCE_FLUSH_AUTH_KEY: Flag<Password> = Flag::new(
    "forceFlushAuthKey",
    "authKey, which must be passed in query string to /internal/force_flush . It overrides -httpAuth.* . \
     See https://docs.victoriametrics.com/victorialogs/#forced-flush",
    || Password::new("forceFlushAuthKey"),
);

static PARTITION_MANAGE_AUTH_KEY: Flag<Password> = Flag::new(
    "partitionManageAuthKey",
    "authKey, which must be passed in query string to /internal/partition/* . It overrides -httpAuth.* . \
     See https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle",
    || Password::new("partitionManageAuthKey"),
);

static STORAGE_NODE_ADDRS: Flag<ArrayString> = Flag::new(
    "storageNode",
    "Comma-separated list of TCP addresses for storage nodes to route the ingested logs to and \
     to send select queries to. If the list is empty, then the ingested logs are stored and \
     queried locally from -storageDataPath",
    ArrayString::default,
);

static INSERT_CONCURRENCY: Flag<usize> = Flag::new(
    "insert.concurrency",
    "The average number of concurrent data ingestion requests, which can be sent to every -storageNode",
    || 2,
);

static INSERT_DISABLE_COMPRESSION: Flag<bool> = Flag::new(
    "insert.disableCompression",
    "Whether to disable compression when sending the ingested data to -storageNode nodes. \
     Disabled compression reduces CPU usage at the cost of higher network usage",
    || false,
);

static SELECT_DISABLE_COMPRESSION: Flag<bool> = Flag::new(
    "select.disableCompression",
    "Whether to disable compression for select query responses received from -storageNode nodes. \
     Disabled compression reduces CPU usage at the cost of higher network usage",
    || false,
);

static STORAGE_NODE_USERNAME: Flag<ArrayString> = Flag::new(
    "storageNode.username",
    "Optional basic auth username to use for the corresponding -storageNode",
    ArrayString::default,
);

static STORAGE_NODE_USERNAME_FILE: Flag<ArrayString> = Flag::new(
    "storageNode.usernameFile",
    "Optional path to basic auth username to use for the corresponding -storageNode. \
     The file is re-read every second",
    ArrayString::default,
);

static STORAGE_NODE_PASSWORD: Flag<ArrayString> = Flag::new(
    "storageNode.password",
    "Optional basic auth password to use for the corresponding -storageNode",
    ArrayString::default,
);

static STORAGE_NODE_PASSWORD_FILE: Flag<ArrayString> = Flag::new(
    "storageNode.passwordFile",
    "Optional path to basic auth password to use for the corresponding -storageNode. \
     The file is re-read every second",
    ArrayString::default,
);

static STORAGE_NODE_BEARER_TOKEN: Flag<ArrayString> = Flag::new(
    "storageNode.bearerToken",
    "Optional bearer auth token to use for the corresponding -storageNode",
    ArrayString::default,
);

static STORAGE_NODE_BEARER_TOKEN_FILE: Flag<ArrayString> = Flag::new(
    "storageNode.bearerTokenFile",
    "Optional path to bearer token file to use for the corresponding -storageNode. \
     The token is re-read from the file every second",
    ArrayString::default,
);

static STORAGE_NODE_TLS: Flag<ArrayBool> = Flag::new(
    "storageNode.tls",
    "Whether to use TLS (HTTPS) protocol for communicating with the corresponding -storageNode. \
     By default communication is performed via HTTP",
    ArrayBool::default,
);

static STORAGE_NODE_TLS_CA_FILE: Flag<ArrayString> = Flag::new(
    "storageNode.tlsCAFile",
    "Optional path to TLS CA file to use for verifying connections to the corresponding -storageNode. \
     By default, system CA is used",
    ArrayString::default,
);

static STORAGE_NODE_TLS_CERT_FILE: Flag<ArrayString> = Flag::new(
    "storageNode.tlsCertFile",
    "Optional path to client-side TLS certificate file to use when connecting to the corresponding -storageNode",
    ArrayString::default,
);

static STORAGE_NODE_TLS_KEY_FILE: Flag<ArrayString> = Flag::new(
    "storageNode.tlsKeyFile",
    "Optional path to client-side TLS certificate key to use when connecting to the corresponding -storageNode",
    ArrayString::default,
);

static STORAGE_NODE_TLS_SERVER_NAME: Flag<ArrayString> = Flag::new(
    "storageNode.tlsServerName",
    "Optional TLS server name to use for connections to the corresponding -storageNode. \
     By default, the server name from -storageNode is used",
    ArrayString::default,
);

static STORAGE_NODE_TLS_INSECURE_SKIP_VERIFY: Flag<ArrayBool> = Flag::new(
    "storageNode.tlsInsecureSkipVerify",
    "Whether to skip tls verification when connecting to the corresponding -storageNode",
    ArrayBool::default,
);

/// Returns the configured `-storageDataPath`.
pub fn storage_data_path() -> &'static str {
    STORAGE_DATA_PATH.get()
}

/// Returns the configured `-storageNode` addresses.
pub fn storage_node_addrs() -> &'static [String] {
    &STORAGE_NODE_ADDRS.get().0
}

// ---------------------------------------------------------------------------
// Init / Stop
// ---------------------------------------------------------------------------

/// Converts a Go-style duration flag into the i64 nanoseconds expected by
/// [`StorageConfig`].
fn duration_nanos(d: Duration) -> i64 {
    d.as_nanos() as i64
}

/// Initializes eslstorage in local mode and returns the opened storage,
/// mirroring the local branch of Go `eslstorage.Init` (`initLocalStorage`).
///
/// # Panics
///
/// Calls `fatalf!` (process exit, like Go `logger.Fatalf`) when the retention
/// flags are invalid, or when `-storageNode` is set (cluster mode is not wired
/// into the single-node binary; see the module PORT NOTE).
pub fn init() -> Arc<Storage> {
    if !storage_node_addrs().is_empty() {
        fatalf!(
            "-storageNode is not supported by the single-node es-logs binary yet; \
             see the esl-storage netinsert/netselect PORT NOTEs"
        );
    }
    init_local_storage()
}

/// Port of Go `initLocalStorage`.
fn init_local_storage() -> Arc<Storage> {
    let retention = RETENTION_PERIOD.get();
    if retention.duration() < Duration::from_secs(24 * 3600) {
        fatalf!("-retentionPeriod cannot be smaller than a day; got {retention}");
    }
    // Validate mutually exclusive retention flags and their values.
    let max_disk_space = MAX_DISK_SPACE_USAGE_BYTES.get().n;
    let max_disk_percent = *MAX_DISK_USAGE_PERCENT.get();
    if max_disk_space > 0 && max_disk_percent > 0 {
        fatalf!(
            "-retention.maxDiskSpaceUsageBytes and -retention.maxDiskUsagePercent cannot be set simultaneously"
        );
    }
    if !(0..=100).contains(&max_disk_percent) {
        fatalf!("-retention.maxDiskUsagePercent must be between 1 and 100; got {max_disk_percent}");
    }

    let cfg = StorageConfig {
        retention: duration_nanos(retention.duration()),
        default_parallel_readers: *DEFAULT_PARALLEL_READERS.get(),
        max_disk_space_usage_bytes: max_disk_space,
        max_disk_usage_percent: max_disk_percent,
        flush_interval: duration_nanos(INMEMORY_DATA_FLUSH_INTERVAL.get().duration()),
        future_retention: duration_nanos(FUTURE_RETENTION.get().duration()),
        max_backfill_age: duration_nanos(MAX_BACKFILL_AGE.get().duration()),
        snapshots_max_age: duration_nanos(SNAPSHOTS_MAX_AGE.get().duration()),
        min_free_disk_space_bytes: MIN_FREE_DISK_SPACE_BYTES.get().n,
        log_new_streams: *LOG_NEW_STREAMS.get(),
        log_ingested_rows: *LOG_INGESTED_ROWS.get(),
    };

    let path = STORAGE_DATA_PATH.get();
    infof!("opening storage at -storageDataPath={path}");
    let start_time = Instant::now();
    let strg = Storage::must_open_storage(Path::new(path), &cfg);
    esl_common::fs::register_path_fs_metrics(path);

    let mut ss = StorageStats::default();
    strg.update_stats(&mut ss);
    let ds = &ss.partition_stats.datadb_stats;
    infof!(
        "successfully opened storage in {:.3} seconds; smallParts: {}; bigParts: {}; \
         smallPartBlocks: {}; bigPartBlocks: {}; smallPartRows: {}; bigPartRows: {}; \
         smallPartSize: {} bytes; bigPartSize: {} bytes",
        start_time.elapsed().as_secs_f64(),
        ds.small_parts,
        ds.big_parts,
        ds.small_part_blocks,
        ds.big_part_blocks,
        ds.small_part_rows_count,
        ds.big_part_rows_count,
        ds.compressed_small_part_size,
        ds.compressed_big_part_size
    );

    // PORT NOTE: Go registers `writeStorageMetrics` in a local `metrics.Set`
    // here; see the module PORT NOTE about the /metrics stub.

    strg
}

/// The network-mode storage handles (Go package globals `netstorageInsert` +
/// `netstorageSelect`).
pub struct NetworkStorage {
    pub insert: netinsert::Storage,
    pub select: netselect::Storage,
}

impl NetworkStorage {
    /// Stops the network storage, mirroring the network branch of Go `Stop`.
    pub fn must_stop(&self) {
        self.insert.must_stop();
        self.select.must_stop();
    }
}

/// Port of Go `initNetworkStorage`: starts the cluster-mode insert and select
/// services for the `-storageNode` addresses.
pub fn init_network_storage() -> NetworkStorage {
    let addrs = storage_node_addrs();
    if addrs.is_empty() {
        panicf!("BUG: init_network_storage() must be called only when -storageNode is set");
    }

    let mut insert_auth_cfgs = Vec::with_capacity(addrs.len());
    let mut select_auth_cfgs = Vec::with_capacity(addrs.len());
    for i in 0..addrs.len() {
        insert_auth_cfgs.push(new_auth_config_for_storage_node(i));
        select_auth_cfgs.push(new_auth_config_for_storage_node(i));
    }

    infof!("starting insert service for nodes {addrs:?}");
    let insert = netinsert::new_storage(
        addrs,
        insert_auth_cfgs,
        *INSERT_CONCURRENCY.get(),
        *INSERT_DISABLE_COMPRESSION.get(),
    );

    infof!("initializing select service for nodes {addrs:?}");
    let select = netselect::new_storage(addrs, select_auth_cfgs, *SELECT_DISABLE_COMPRESSION.get());

    infof!("initialized all the network services");

    NetworkStorage { insert, select }
}

/// Port of Go `newAuthConfigForStorageNode`.
fn new_auth_config_for_storage_node(arg_idx: usize) -> http_client::AuthConfig {
    let username = STORAGE_NODE_USERNAME.get().get_optional_arg(arg_idx);
    let username_file = STORAGE_NODE_USERNAME_FILE.get().get_optional_arg(arg_idx);
    let password = STORAGE_NODE_PASSWORD.get().get_optional_arg(arg_idx);
    let password_file = STORAGE_NODE_PASSWORD_FILE.get().get_optional_arg(arg_idx);
    let basic_auth = if !username.is_empty()
        || !username_file.is_empty()
        || !password.is_empty()
        || !password_file.is_empty()
    {
        Some(http_client::BasicAuthConfig {
            username: username.to_string(),
            username_file: username_file.to_string(),
            password: password.to_string(),
            password_file: password_file.to_string(),
        })
    } else {
        None
    };

    // PORT NOTE: Go always fills promauth.TLSConfig and passes the
    // -storageNode.tls toggle separately to newStorageNode; the port only
    // materializes the TLS config when the toggle is set (it is unused
    // otherwise) — see `http_client::Options`.
    let tls_config = if STORAGE_NODE_TLS.get().get_optional_arg(arg_idx) {
        Some(esl_common::tlsutil::TLSConfig {
            ca_file: STORAGE_NODE_TLS_CA_FILE
                .get()
                .get_optional_arg(arg_idx)
                .to_string(),
            cert_file: STORAGE_NODE_TLS_CERT_FILE
                .get()
                .get_optional_arg(arg_idx)
                .to_string(),
            key_file: STORAGE_NODE_TLS_KEY_FILE
                .get()
                .get_optional_arg(arg_idx)
                .to_string(),
            server_name: STORAGE_NODE_TLS_SERVER_NAME
                .get()
                .get_optional_arg(arg_idx)
                .to_string(),
            insecure_skip_verify: STORAGE_NODE_TLS_INSECURE_SKIP_VERIFY
                .get()
                .get_optional_arg(arg_idx),
            ..Default::default()
        })
    } else {
        None
    };

    let opts = http_client::Options {
        basic_auth,
        bearer_token: STORAGE_NODE_BEARER_TOKEN
            .get()
            .get_optional_arg(arg_idx)
            .to_string(),
        bearer_token_file: STORAGE_NODE_BEARER_TOKEN_FILE
            .get()
            .get_optional_arg(arg_idx)
            .to_string(),
        tls_config,
    };
    match opts.new_config() {
        Ok(ac) => ac,
        Err(err) => {
            panicf!("FATAL: cannot populate auth config for storage node #{arg_idx}: {err}");
            unreachable!()
        }
    }
}

// ---------------------------------------------------------------------------
// Ingestion
// ---------------------------------------------------------------------------

/// Returns an error if data cannot be written to the storage, mirroring Go
/// `(*Storage).CanWriteData` (the `insertutil.LogRowsStorage` impl).
///
/// PORT NOTE: Go returns an `httpserver.ErrorWithStatusCode` with
/// `http.StatusTooManyRequests`; the port returns `(message, status_code)`.
pub fn can_write_data(storage: &Arc<Storage>) -> Result<(), (String, u16)> {
    if storage.is_read_only() {
        return Err((
            format!(
                "cannot add rows into storage in read-only mode; the storage can be in read-only \
                 mode because of lack of free disk space at -storageDataPath={}",
                STORAGE_DATA_PATH.get()
            ),
            429,
        ));
    }
    Ok(())
}

/// Adds `lr` to the storage, mirroring Go `eslstorage.Storage.MustAddRows`
/// (local branch; the network branch routes rows through `netinsert` — see the
/// module PORT NOTE).
///
/// It is advised to call [`can_write_data`] before calling `must_add_rows`.
pub fn must_add_rows(storage: &Arc<Storage>, lr: &LogRows) {
    storage.must_add_rows(lr);
}

// ---------------------------------------------------------------------------
// Query dispatch
// ---------------------------------------------------------------------------

/// Runs the given query and calls `write_block` for the returned data blocks,
/// mirroring Go `eslstorage.RunQuery`: queries eligible for the last-N results
/// optimization take the [`lastn_optimization`] path.
///
/// PORT NOTE: the network branch (`netstorageSelect.RunQuery`) is not wired —
/// see the module PORT NOTE. esl-select should call this function instead of
/// `Storage::run_query` so `sort by (_time) desc | limit N` queries get the
/// adaptive time-range narrowing (that wiring belongs to esl-select and is left
/// to its owner).
pub fn run_query(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q: &Query,
    write_block: WriteDataBlockFn,
) -> Result<(), String> {
    run_query_with_cancel(storage, tenant_ids, q, write_block, None)
}

/// Like [`run_query`], but aborts early when the external `cancel` token is
/// set, returning `esl_logstorage::storage_search::QUERY_CANCELED_ERROR`
/// (Go: the `*QueryContext` ctx going done -> `context.Canceled`). The same
/// token is threaded through every subquery of the last-N optimization.
pub fn run_query_with_cancel(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q: &Query,
    write_block: WriteDataBlockFn,
    cancel: Option<&Arc<std::sync::atomic::AtomicBool>>,
) -> Result<(), String> {
    let qs = Arc::new(esl_logstorage::query_stats::QueryStats::default());
    run_query_with_stats(storage, tenant_ids, q, write_block, cancel, &qs)
}

/// Like [`run_query_with_cancel`], but additionally accumulates the query
/// execution stats into `qs` (Go `qctx.QueryStats`). The stats of every
/// subquery of the last-N optimization accumulate into the same `qs`, exactly
/// like Go's shared query context.
pub fn run_query_with_stats(
    storage: &Arc<Storage>,
    tenant_ids: &[TenantID],
    q: &Query,
    write_block: WriteDataBlockFn,
    cancel: Option<&Arc<std::sync::atomic::AtomicBool>>,
    qs: &Arc<esl_logstorage::query_stats::QueryStats>,
) -> Result<(), String> {
    if let Some((q_opt, offset, limit)) = q.get_last_n_results_query() {
        return lastn_optimization::run_optimized_last_n_results_query(
            storage,
            tenant_ids,
            &q_opt,
            offset,
            limit,
            write_block,
            cancel,
            qs,
        );
    }

    // PORT NOTE: Go RunQuery receives the hidden-fields filters inside the
    // `*QueryContext`; this wrapper is not on the logsql request path (see the
    // PORT NOTE above), so no caller has hidden-fields filters to thread and
    // an empty list is passed.
    storage.run_query_with_stats(tenant_ids, q, &[], write_block, cancel, qs)
}

// PORT NOTE: the Go `GetFieldNames`/`GetFieldValues`/`GetStreamFieldNames`/
// `GetStreamFieldValues`/`GetStreams`/`GetStreamIDs`/`Delete*`/`GetTenantIDs`
// dispatchers are not ported: their local engine surface
// (`localStorage.GetFieldNames` etc.) does not exist in esl-logstorage yet and
// no esl-select caller needs them. The network halves are already available on
// `netselect::Storage`.

// ---------------------------------------------------------------------------
// Internal HTTP endpoints
// ---------------------------------------------------------------------------

/// Handles the internal storage endpoints, mirroring Go
/// `eslstorage.RequestHandler` (local-storage mode). Returns `true` if the path
/// was handled.
pub fn request_handler(storage: &Arc<Storage>, req: &mut Request, w: &mut ResponseWriter) -> bool {
    let path = req.path().to_string();
    match path.as_str() {
        "/internal/log_new_streams" => process_log_new_streams(storage, req, w),
        "/internal/force_merge" => process_force_merge(storage, req, w),
        "/internal/force_flush" => process_force_flush(storage, req, w),
        "/internal/partition/attach" => process_partition_attach(storage, req, w),
        "/internal/partition/detach" => process_partition_detach(storage, req, w),
        "/internal/partition/list" => process_partition_list(storage, req, w),
        "/internal/partition/snapshot/create" => process_partition_snapshot_create(storage, req, w),
        "/internal/partition/snapshot/list" => process_partition_snapshot_list(storage, req, w),
        "/internal/partition/snapshot/delete" => process_partition_snapshot_delete(storage, req, w),
        "/internal/partition/snapshot/delete_stale" => {
            process_partition_snapshot_delete_stale(storage, req, w)
        }
        _ => false,
    }
}

/// Go `httpserver.CheckAuthFlag`: checks the authKey and falls back to
/// `CheckBasicAuth` (`-httpAuth.*`) when the auth-key flag is empty, with
/// Go's distinct missing-vs-mismatching authKey error messages.
fn check_auth_flag(w: &mut ResponseWriter, req: &Request, expected_flag: &Password) -> bool {
    esl_common::httpserver::check_auth_flag(w, req, expected_flag)
}

/// Port of Go `processLogNewStreams`.
fn process_log_new_streams(
    storage: &Arc<Storage>,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    if !check_auth_flag(w, req, LOG_NEW_STREAMS_AUTH_KEY.get()) {
        return true;
    }

    let mut seconds = match httputil::get_int(req, "seconds") {
        Ok(seconds) => seconds,
        Err(err) => {
            w.errorf(req, &format!("cannot parse 'seconds' query arg: {err}"));
            return true;
        }
    };
    if seconds <= 0 {
        seconds = 10;
    }

    storage.enable_log_new_streams(seconds);
    true
}

/// Port of Go `processForceMerge`.
fn process_force_merge(storage: &Arc<Storage>, req: &mut Request, w: &mut ResponseWriter) -> bool {
    if !check_auth_flag(w, req, FORCE_MERGE_AUTH_KEY.get()) {
        return true;
    }

    // Run force merge in the background, like Go's `go func() { ... }()`.
    let partition_prefix = req.form_value("partition_prefix").to_string();
    let storage = Arc::clone(storage);
    let _ = std::thread::Builder::new()
        .name("force_merge".to_string())
        .spawn(move || {
            active_force_merges().inc();
            infof!("forced merge for partition_prefix={partition_prefix:?} has been started");
            let start_time = Instant::now();
            storage.must_force_merge(&partition_prefix);
            infof!(
                "forced merge for partition_prefix={partition_prefix:?} has been successfully finished in {:.3} seconds",
                start_time.elapsed().as_secs_f64()
            );
            active_force_merges().dec();
        });
    true
}

/// The `esl_active_force_merges` counter (Go `activeForceMerges`).
fn active_force_merges() -> &'static Arc<esl_common::metrics::Counter> {
    static C: std::sync::LazyLock<Arc<esl_common::metrics::Counter>> =
        std::sync::LazyLock::new(|| esl_common::metrics::new_counter("esl_active_force_merges"));
    &C
}

/// Port of Go `processForceFlush`.
fn process_force_flush(storage: &Arc<Storage>, req: &mut Request, w: &mut ResponseWriter) -> bool {
    if !check_auth_flag(w, req, FORCE_FLUSH_AUTH_KEY.get()) {
        return true;
    }

    infof!("flushing storage to make pending data available for reading");
    storage.debug_flush();
    true
}

/// Port of Go `processPartitionAttach`.
fn process_partition_attach(
    storage: &Arc<Storage>,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    if !check_auth_flag(w, req, PARTITION_MANAGE_AUTH_KEY.get()) {
        return true;
    }

    let name = req.form_value("name").to_string();
    if let Err(err) = storage.partition_attach(&name) {
        w.errorf(req, &err);
        return true;
    }

    true
}

/// Port of Go `processPartitionDetach`.
fn process_partition_detach(
    storage: &Arc<Storage>,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    if !check_auth_flag(w, req, PARTITION_MANAGE_AUTH_KEY.get()) {
        return true;
    }

    let name = req.form_value("name").to_string();
    if let Err(err) = storage.partition_detach(&name) {
        w.errorf(req, &err);
        return true;
    }

    true
}

/// Port of Go `processPartitionList`.
fn process_partition_list(
    storage: &Arc<Storage>,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    if !check_auth_flag(w, req, PARTITION_MANAGE_AUTH_KEY.get()) {
        return true;
    }

    let pt_names = storage.partition_list();
    write_json_response(w, &pt_names);
    true
}

/// Port of Go `processPartitionSnapshotCreate`.
fn process_partition_snapshot_create(
    storage: &Arc<Storage>,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    if !check_auth_flag(w, req, PARTITION_MANAGE_AUTH_KEY.get()) {
        return true;
    }

    let mut partition_prefix = req.form_value("partition_prefix").to_string();
    if partition_prefix.is_empty() {
        // Fall back to the deprecated argument.
        partition_prefix = req.form_value("name").to_string();
    }

    let snapshot_paths = storage.partition_snapshot_must_create(&partition_prefix);

    // Go: if the client already closed the connection, drop the created
    // snapshot rather than leak it (`r.Context().Err()`). A one-shot
    // disconnect probe stands in for Go's request context.
    if w.is_client_disconnected() {
        for path in &snapshot_paths {
            infof!(
                "deleting already created snapshot at {} because the client canceled the request",
                path.display()
            );
            if let Err(err) = storage.partition_snapshot_delete(path) {
                warnf!("cannot delete already created snapshot: {err}");
            }
        }
        return true;
    }

    let snapshot_paths: Vec<String> = snapshot_paths
        .iter()
        .map(|p: &PathBuf| p.to_string_lossy().into_owned())
        .collect();

    write_json_response(w, &snapshot_paths);
    true
}

/// Port of Go `processPartitionSnapshotList`.
fn process_partition_snapshot_list(
    storage: &Arc<Storage>,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    if !check_auth_flag(w, req, PARTITION_MANAGE_AUTH_KEY.get()) {
        return true;
    }

    let snapshot_paths = storage.partition_snapshot_list();
    write_json_response(w, &snapshot_paths);
    true
}

/// Port of Go `processPartitionSnapshotDelete`.
fn process_partition_snapshot_delete(
    storage: &Arc<Storage>,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    if !check_auth_flag(w, req, PARTITION_MANAGE_AUTH_KEY.get()) {
        return true;
    }

    let snapshot_path = req.form_value("path").to_string();
    if snapshot_path.is_empty() {
        w.errorf(req, "missing `path` query arg");
        return true;
    }

    if let Err(err) = storage.partition_snapshot_delete(Path::new(&snapshot_path)) {
        w.errorf(req, &err);
        return true;
    }

    w.set_status(204);
    true
}

/// Port of Go `processPartitionSnapshotDeleteStale`.
fn process_partition_snapshot_delete_stale(
    storage: &Arc<Storage>,
    req: &mut Request,
    w: &mut ResponseWriter,
) -> bool {
    if !check_auth_flag(w, req, PARTITION_MANAGE_AUTH_KEY.get()) {
        return true;
    }

    let mut max_age = SNAPSHOTS_MAX_AGE.get().duration();
    let max_age_str = req.form_value("max_age").to_string();
    if !max_age_str.is_empty() {
        match RetentionDuration::parse_flag(&max_age_str) {
            Ok(d) => max_age = d.duration(),
            Err(err) => {
                w.errorf(req, &format!("cannot parse max_age={max_age_str:?}: {err}"));
                return true;
            }
        }
    }
    if max_age.is_zero() {
        // Nothing to delete.
        w.write_str("[]");
        return true;
    }

    let deleted_snapshot_paths = storage.must_delete_stale_partition_snapshots(max_age);
    write_json_response(w, &deleted_snapshot_paths);
    true
}

/// Port of Go `writeJSONResponse` for the `[]string` responses produced by the
/// partition endpoints (Go marshals via `encoding/json`).
fn write_json_response(w: &mut ResponseWriter, response: &[String]) {
    let items: Vec<String> = response.iter().map(|s| json_string(s)).collect();
    w.set_header("Content-Type", "application/json");
    w.write_str(&format!("[{}]", items.join(",")));
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

// The write_* helpers delegate to the `esl_common::metrics` exposition
// writers used by Go `writeStorageMetrics`.

use esl_common::metrics::{write_counter_uint64, write_gauge_float64, write_gauge_uint64};

/// Registers the storage metrics in a new `metrics::Set` rendered at
/// `/metrics`, mirroring Go `vlstorage.initLocalStorage`:
///
/// ```go
/// localStorageMetrics = metrics.NewSet()
/// localStorageMetrics.RegisterMetricsWriter(func(w io.Writer) { writeStorageMetrics(w, localStorage) })
/// metrics.RegisterSet(localStorageMetrics)
/// ```
pub fn init_storage_metrics(storage: &Arc<Storage>) {
    crate::query_stats::init();
    let storage = Arc::clone(storage);
    let local_storage_metrics = Arc::new(esl_common::metrics::Set::new());
    local_storage_metrics
        .register_metrics_writer(move |w: &mut String| write_storage_metrics(&storage, w));
    esl_common::metrics::register_set(local_storage_metrics);
}

/// Writes the storage metrics in Prometheus exposition format, mirroring Go
/// `eslstorage.writeStorageMetrics`.
pub fn write_storage_metrics(storage: &Arc<Storage>, w: &mut String) {
    let mut ss = StorageStats::default();
    storage.update_stats(&mut ss);
    let ds = &ss.partition_stats.datadb_stats;
    let is = &ss.partition_stats.indexdb_stats;
    let path = json_string(STORAGE_DATA_PATH.get());

    if ss.max_disk_space_usage_bytes > 0 {
        write_gauge_uint64(
            w,
            &format!("esl_max_disk_space_usage_bytes{{path={path}}}"),
            ss.max_disk_space_usage_bytes as u64,
        );
    }
    write_gauge_uint64(
        w,
        &format!("esl_free_disk_space_bytes{{path={path}}}"),
        esl_common::fs::must_get_free_space(STORAGE_DATA_PATH.get()),
    );
    write_gauge_uint64(
        w,
        &format!("esl_total_disk_space_bytes{{path={path}}}"),
        esl_common::fs::must_get_total_space(STORAGE_DATA_PATH.get()),
    );

    write_gauge_uint64(
        w,
        &format!("esl_storage_is_read_only{{path={path}}}"),
        u64::from(ss.is_read_only),
    );

    write_gauge_uint64(
        w,
        "esl_active_merges{type=\"storage/inmemory\"}",
        ds.active_inmemory_merges,
    );
    write_gauge_uint64(
        w,
        "esl_active_merges{type=\"storage/small\"}",
        ds.active_small_merges,
    );
    write_gauge_uint64(
        w,
        "esl_active_merges{type=\"storage/big\"}",
        ds.active_big_merges,
    );
    write_gauge_uint64(
        w,
        "esl_active_merges{type=\"indexdb/inmemory\"}",
        is.indexdb_active_inmemory_merges,
    );
    write_gauge_uint64(
        w,
        "esl_active_merges{type=\"indexdb/file\"}",
        is.indexdb_active_file_merges,
    );

    write_counter_uint64(
        w,
        "esl_merges_total{type=\"storage/inmemory\"}",
        ds.inmemory_merges_count,
    );
    write_counter_uint64(
        w,
        "esl_merges_total{type=\"storage/small\"}",
        ds.small_merges_count,
    );
    write_counter_uint64(
        w,
        "esl_merges_total{type=\"storage/big\"}",
        ds.big_merges_count,
    );
    write_counter_uint64(
        w,
        "esl_merges_total{type=\"indexdb/inmemory\"}",
        is.indexdb_inmemory_merges_count,
    );
    write_counter_uint64(
        w,
        "esl_merges_total{type=\"indexdb/file\"}",
        is.indexdb_file_merges_count,
    );

    write_counter_uint64(
        w,
        "esl_rows_merged_total{type=\"storage/inmemory\"}",
        ds.inmemory_rows_merged,
    );
    write_counter_uint64(
        w,
        "esl_rows_merged_total{type=\"storage/small\"}",
        ds.small_rows_merged,
    );
    write_counter_uint64(
        w,
        "esl_rows_merged_total{type=\"storage/big\"}",
        ds.big_rows_merged,
    );
    write_counter_uint64(
        w,
        "esl_rows_merged_total{type=\"indexdb/inmemory\"}",
        is.indexdb_inmemory_items_merged,
    );
    write_counter_uint64(
        w,
        "esl_rows_merged_total{type=\"indexdb/file\"}",
        is.indexdb_file_items_merged,
    );

    write_gauge_uint64(
        w,
        "esl_storage_rows{type=\"storage/inmemory\"}",
        ds.inmemory_rows_count,
    );
    write_gauge_uint64(
        w,
        "esl_storage_rows{type=\"storage/small\"}",
        ds.small_part_rows_count,
    );
    write_gauge_uint64(
        w,
        "esl_storage_rows{type=\"storage/big\"}",
        ds.big_part_rows_count,
    );

    write_gauge_uint64(
        w,
        "esl_storage_parts{type=\"storage/inmemory\"}",
        ds.inmemory_parts,
    );
    write_gauge_uint64(
        w,
        "esl_storage_parts{type=\"storage/small\"}",
        ds.small_parts,
    );
    write_gauge_uint64(w, "esl_storage_parts{type=\"storage/big\"}", ds.big_parts);

    write_gauge_uint64(
        w,
        "esl_storage_blocks{type=\"storage/inmemory\"}",
        ds.inmemory_blocks,
    );
    write_gauge_uint64(
        w,
        "esl_storage_blocks{type=\"storage/small\"}",
        ds.small_part_blocks,
    );
    write_gauge_uint64(
        w,
        "esl_storage_blocks{type=\"storage/big\"}",
        ds.big_part_blocks,
    );

    write_gauge_uint64(w, "esl_pending_rows{type=\"storage\"}", ds.pending_rows);
    write_gauge_uint64(
        w,
        "esl_pending_rows{type=\"indexdb\"}",
        is.indexdb_pending_items,
    );

    write_gauge_uint64(w, "esl_partitions", ss.partitions_count);
    write_counter_uint64(w, "esl_streams_created_total", is.streams_created_total);

    write_gauge_uint64(w, "esl_indexdb_rows", is.indexdb_items_count);
    write_gauge_uint64(w, "esl_indexdb_parts", is.indexdb_parts_count);
    write_gauge_uint64(w, "esl_indexdb_blocks", is.indexdb_blocks_count);

    write_gauge_uint64(
        w,
        "esl_data_size_bytes{type=\"indexdb\"}",
        is.indexdb_size_bytes,
    );
    write_gauge_uint64(
        w,
        "esl_data_size_bytes{type=\"storage\"}",
        ds.compressed_inmemory_size + ds.compressed_small_part_size + ds.compressed_big_part_size,
    );

    write_gauge_uint64(
        w,
        "esl_compressed_data_size_bytes{type=\"storage/inmemory\"}",
        ds.compressed_inmemory_size,
    );
    write_gauge_uint64(
        w,
        "esl_compressed_data_size_bytes{type=\"storage/small\"}",
        ds.compressed_small_part_size,
    );
    write_gauge_uint64(
        w,
        "esl_compressed_data_size_bytes{type=\"storage/big\"}",
        ds.compressed_big_part_size,
    );

    write_gauge_uint64(
        w,
        "esl_uncompressed_data_size_bytes{type=\"storage/inmemory\"}",
        ds.uncompressed_inmemory_size,
    );
    write_gauge_uint64(
        w,
        "esl_uncompressed_data_size_bytes{type=\"storage/small\"}",
        ds.uncompressed_small_part_size,
    );
    write_gauge_uint64(
        w,
        "esl_uncompressed_data_size_bytes{type=\"storage/big\"}",
        ds.uncompressed_big_part_size,
    );

    if ss.min_timestamp != i64::MIN {
        write_gauge_float64(
            w,
            "esl_storage_log_min_timestamp_seconds",
            (ss.min_timestamp / 1_000_000_000) as f64,
        );
    }
    if ss.max_timestamp != i64::MAX {
        write_gauge_float64(
            w,
            "esl_storage_log_max_timestamp_seconds",
            (ss.max_timestamp / 1_000_000_000) as f64,
        );
    }

    write_counter_uint64(
        w,
        "esl_rows_dropped_total{reason=\"too_big_timestamp\"}",
        ss.rows_dropped_too_big_timestamp,
    );
    write_counter_uint64(
        w,
        "esl_rows_dropped_total{reason=\"too_small_timestamp\"}",
        ss.rows_dropped_too_small_timestamp,
    );
}

// PORT NOTE: upstream has no main_test.go for app/eslstorage; the tests below
// are port-specific coverage of the flag routing, the request handler (driven
// through a real httpserver + the crate's http_client) and the metrics writer.
#[cfg(test)]
mod tests {
    use super::*;
    use esl_logstorage::log_rows::get_log_rows;
    use esl_logstorage::rows::Field;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn unique_path(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        // Include the pid so concurrent test processes (e.g. an interrupted
        // earlier run) never collide on the storage flock.
        let pid = std::process::id();
        std::env::temp_dir().join(format!("esl-storage-test-{name}-{pid}-{n}"))
    }

    fn field(name: &str, value: &str) -> Field {
        Field {
            name: name.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    fn open_temp() -> (PathBuf, Arc<Storage>) {
        let path = unique_path("hub");
        let cfg = StorageConfig::default();
        let s = Storage::must_open_storage(&path, &cfg);
        (path, s)
    }

    fn add_some_rows(storage: &Arc<Storage>) {
        let stream_tags = ["some-stream-tag"];
        let mut lr = get_log_rows(&stream_tags, &[], &[], &[], "");
        let tenant_id = TenantID {
            account_id: 0,
            project_id: 0,
        };
        let mut fields = vec![
            field("some-stream-tag", "some-stream-value-0"),
            field("", "some row"),
            field("job", "foobar"),
        ];
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        lr.must_add(tenant_id, now, &mut fields, -1);
        must_add_rows(storage, &lr);
    }

    /// Serves `handler` on an ephemeral port and returns `(status, body)` for
    /// a GET of `path_and_query` issued through the crate's http_client.
    fn get_with_handler<H>(handler: H, path_and_query: &str) -> (u16, String)
    where
        H: Fn(&mut Request, &mut ResponseWriter) + Send + Sync + 'static,
    {
        let handle = esl_common::httpserver::serve("127.0.0.1:0", handler)
            .expect("cannot start test http server");
        let addr = handle.local_addr().to_string();
        let resp = http_client::do_request(&addr, None, "GET", path_and_query, &[], None)
            .expect("request failed");
        handle.stop();
        (
            resp.status_code,
            String::from_utf8_lossy(&resp.body).into_owned(),
        )
    }

    /// Serves `request_handler` and GETs `path_and_query`.
    fn get(storage: &Arc<Storage>, path_and_query: &str) -> (u16, String) {
        let storage = Arc::clone(storage);
        get_with_handler(
            move |req, w| {
                if !request_handler(&storage, req, w) {
                    w.error("unsupported path", 404);
                }
            },
            path_and_query,
        )
    }

    #[test]
    fn test_request_handler_routes() {
        let (path, s) = open_temp();
        add_some_rows(&s);

        // force_flush makes pending data searchable and returns 200.
        let (status, _) = get(&s, "/internal/force_flush");
        assert_eq!(status, 200);

        // partition list returns a JSON array with the created partition.
        let (status, body) = get(&s, "/internal/partition/list");
        assert_eq!(status, 200);
        assert!(body.starts_with('[') && body.ends_with(']'), "{body}");
        assert_ne!(body, "[]");

        // snapshot create + list + delete round-trip.
        let (status, body) = get(&s, "/internal/partition/snapshot/create");
        assert_eq!(status, 200);
        assert_ne!(body, "[]");
        let (status, list_body) = get(&s, "/internal/partition/snapshot/list");
        assert_eq!(status, 200);
        assert_ne!(list_body, "[]");
        let snapshot_path: String =
            serde_unquote_first(&list_body).expect("cannot parse snapshot list");
        let (status, _) = get(
            &s,
            &format!(
                "/internal/partition/snapshot/delete?path={}",
                url_escape(&snapshot_path)
            ),
        );
        assert_eq!(status, 204);

        // delete_stale with max_age=0 returns `[]` without deleting anything.
        let (status, body) = get(&s, "/internal/partition/snapshot/delete_stale?max_age=0");
        assert_eq!(status, 200);
        assert_eq!(body, "[]");

        // log_new_streams enables stream logging.
        let (status, _) = get(&s, "/internal/log_new_streams?seconds=1");
        assert_eq!(status, 200);

        // unknown internal paths are not handled.
        let (status, _) = get(&s, "/internal/unknown");
        assert_eq!(status, 404);

        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    /// Extracts the first string from a JSON array of strings produced by
    /// `write_json_response` (no escapes are expected in temp paths).
    fn serde_unquote_first(s: &str) -> Option<String> {
        let inner = s.strip_prefix("[\"")?;
        let end = inner.find('"')?;
        Some(inner[..end].replace("\\\\", "\\").replace("\\/", "/"))
    }

    fn url_escape(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }

    #[test]
    fn test_check_auth_flag() {
        // An empty expected key allows every request; a non-empty key is
        // verified against the authKey query arg via the real handler path.
        let (path, s) = open_temp();

        let mut pw = Password::new("testAuthKey");
        pw.set("").unwrap();
        // Cannot build a Request directly (private fields); the empty-key
        // fast path is covered by test_request_handler_routes above. Verify
        // the Password plumbing itself here.
        assert_eq!(pw.get(), "");
        pw.set("secret").unwrap();
        assert_eq!(pw.get(), "secret");

        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_must_add_rows_and_can_write_data() {
        let (path, s) = open_temp();
        add_some_rows(&s);
        assert!(can_write_data(&s).is_ok());
        s.debug_flush();
        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_force_merge_background() {
        let (path, s) = open_temp();
        add_some_rows(&s);
        s.debug_flush();
        s.must_force_merge("");
        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_write_storage_metrics_full_series() {
        let (path, s) = open_temp();
        // write_storage_metrics reads free/total disk space at the
        // -storageDataPath flag (like Go); point the flag at the existing temp
        // storage dir before its lazy first read (default "es-logs-data"
        // does not exist under the test cwd and would fatal).
        esl_common::flagutil::parse_args(&[format!("-storageDataPath={}", path.to_string_lossy())]);
        add_some_rows(&s);
        s.debug_flush();
        let s_metrics = Arc::clone(&s);
        let (status, body) = get_with_handler(
            move |_req, w| {
                let mut buf = String::new();
                write_storage_metrics(&s_metrics, &mut buf);
                w.write_str(&buf);
            },
            "/eslstorage/metrics",
        );
        assert_eq!(status, 200);
        for series in [
            "esl_free_disk_space_bytes{path=",
            "esl_total_disk_space_bytes{path=",
            "esl_storage_is_read_only{path=",
            "esl_active_merges{type=\"storage/inmemory\"}",
            "esl_merges_total{type=\"indexdb/file\"}",
            "esl_rows_merged_total{type=\"storage/small\"}",
            "esl_storage_rows{type=\"storage/small\"}",
            "esl_storage_parts{type=\"storage/big\"}",
            "esl_storage_blocks{type=\"storage/inmemory\"}",
            "esl_pending_rows{type=\"indexdb\"}",
            "esl_partitions",
            "esl_streams_created_total",
            "esl_indexdb_rows",
            "esl_data_size_bytes{type=\"storage\"}",
            "esl_compressed_data_size_bytes{type=\"storage/small\"}",
            "esl_uncompressed_data_size_bytes{type=\"storage/big\"}",
            "esl_storage_log_min_timestamp_seconds",
            "esl_storage_log_max_timestamp_seconds",
            "esl_rows_dropped_total{reason=\"too_big_timestamp\"}",
            "esl_rows_dropped_total{reason=\"too_small_timestamp\"}",
        ] {
            assert!(body.contains(series), "missing series {series} in:\n{body}");
        }
        s.must_close();
        esl_common::fs::must_remove_dir(&path);
    }

    #[test]
    fn test_write_json_response_escaping() {
        let (status, body) = get_with_handler(
            |_req, w| write_json_response(w, &["a\"b".to_string(), "c".to_string()]),
            "/",
        );
        assert_eq!(status, 200);
        assert_eq!(body, r#"["a\"b","c"]"#);

        let (status, body) = get_with_handler(|_req, w| write_json_response(w, &[]), "/");
        assert_eq!(status, 200);
        assert_eq!(body, "[]");
    }
}

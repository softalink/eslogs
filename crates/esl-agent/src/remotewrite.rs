//! Port of EsLogs `app/eslagent/remotewrite`
//! (remotewrite.go, client.go, pendinglogrows.go), plus an inline port of
//! Softalink LLC `lib/ratelimiter/ratelimiter.go`.
//!
//! Collected log rows are buffered per `-remoteWrite.url` ([`PendingLogs`]),
//! compressed into blocks, pushed through a [`FastQueue`] and drained by
//! per-URL [`Client`] workers which POST them to the remote EsLogs
//! with retries and exponential backoff.
//!
//! PORT NOTE: the `eslagent_remotewrite_*` metrics (counters, gauges,
//! histograms) are not ported — there is no metrics crate in this workspace.
//!
//! PORT NOTE: the HTTP transport (Go `net/http` + `promauth` +
//! `httputil.NewTransport`) is replaced by `esl_storage::http_client` (the
//! house std-TCP client; one connection per request, no keep-alive). As a
//! result:
//!   * `https` remote write URLs and the `-remoteWrite.tls*` flags are not
//!     supported — init fails with a clear error when they are used;
//!   * `-remoteWrite.proxyURL` and the `-remoteWrite.oauth2.*` flags are not
//!     supported — init fails with a clear error when they are set;
//!   * Go's one-shot retry on `io.EOF` (stale keep-alive connection) is
//!     unnecessary and dropped, since every request uses a fresh connection.
//!
//! PORT NOTE: Go's client worker sends each block in a helper goroutine so a
//! stopping client can abandon an in-flight request after a 5s grace period.
//! The port sends synchronously: shutdown waits for the in-flight attempt,
//! which is bounded by `-remoteWrite.sendTimeout`; an unsent block is
//! returned to the queue exactly like in Go.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use esl_common::flagutil::duration::format_go_duration;
use esl_common::flagutil::{
    ArrayBool, ArrayBytes, ArrayDuration, ArrayInt, ArrayString, Bytes, ExtendedDuration, Flag,
};
use esl_common::fs as vlfs;
use esl_common::timeutil::{BackoffTimer, add_jitter_to_duration};
use esl_common::{
    cgroup, errorf, fasttime, fatalf, flagutil, infof, logger, memory, panicf, warnf,
};

use esl_insert::common_params::LogRowsStorage;
use esl_logstorage::log_rows::{InsertRow, LogRows};
use esl_storage::http_client::{
    AuthConfig, BasicAuthConfig, HttpResponse, Options, do_request_with_timeout,
};
use esl_storage::netinsert::PROTOCOL_VERSION;

use crate::persistentqueue::{DEFAULT_CHUNK_FILE_SIZE, FastQueue};

// ---------------------------------------------------------------------------
// Flags (remotewrite.go)
// ---------------------------------------------------------------------------

static REMOTE_WRITE_URLS: Flag<ArrayString> = Flag::new(
    "remoteWrite.url",
    "Remote storage URL to write data to. Example url: http://<eslogs-host>:9428/insert/native. \
     Pass multiple -remoteWrite.url options in order to replicate the collected data to multiple remote storage systems. \
     See also -remoteWrite.maxDiskUsagePerURL and -remoteWrite.format",
    ArrayString::default,
);

static MAX_PENDING_BYTES_PER_URL: Flag<ArrayBytes> = Flag::new(
    "remoteWrite.maxDiskUsagePerURL",
    "The maximum file-based buffer size in bytes at -remoteWrite.tmpDataPath for each -remoteWrite.url. \
     When buffer size reaches the configured maximum, then old data is dropped when adding new data to the buffer. \
     Buffered data is stored in ~500MB chunks. It is recommended to set the value for this flag to a multiple of the block size 500MB. \
     Disk usage is unlimited if the value is set to 0",
    || ArrayBytes::with_default(0),
);

static FORMAT: Flag<ArrayString> = Flag::new(
    "remoteWrite.format",
    "The data format to use for sending data to the corresponding -remoteWrite.url. \
     Available formats: native, jsonline. Default is native. \
     See https://docs.victoriametrics.com/victorialogs/vlagent/#remote-write-format",
    ArrayString::default,
);

static REMOTE_WRITE_TMP_DATA_PATH: Flag<String> = Flag::new(
    "remoteWrite.tmpDataPath",
    "Path to directory for storing pending data, which isn't sent to the configured -remoteWrite.url . \
     if this flag isn't set, then pending data is stored in the eslagent-remotewrite-data subdirectory under the -tmpDataPath directory; \
     see also -remoteWrite.maxDiskUsagePerURL",
    String::new,
);

static QUEUES: Flag<i64> = Flag::new(
    "remoteWrite.queues",
    "The number of concurrent queues to each -remoteWrite.url. Set more queues if default number of queues \
     isn't enough for sending high volume of collected data to remote storage. \
     Default value depends on the number of available CPU cores. It should work fine in most cases since it minimizes resource usage",
    || (cgroup::available_cpus() * 2) as i64,
);

static SHOW_REMOTE_WRITE_URL: Flag<bool> = Flag::new(
    "remoteWrite.showURL",
    "Whether to show -remoteWrite.url in the exported metrics. \
     It is hidden by default, since it can contain sensitive info such as auth key",
    || false,
);

// ---------------------------------------------------------------------------
// Flags (client.go)
// ---------------------------------------------------------------------------

static RATE_LIMIT: Flag<ArrayInt> = Flag::new(
    "remoteWrite.rateLimit",
    "Optional rate limit in bytes per second for data sent to the corresponding -remoteWrite.url. \
     By default, the rate limit is disabled. It can be useful for limiting load on remote storage when big amounts of buffered data ",
    || ArrayInt::with_default(0),
);

static SEND_TIMEOUT: Flag<ArrayDuration> = Flag::new(
    "remoteWrite.sendTimeout",
    "Timeout for sending a single block of data to the corresponding -remoteWrite.url",
    || ArrayDuration::with_default(Duration::from_secs(60)),
);

static RETRY_MIN_INTERVAL: Flag<ArrayDuration> = Flag::new(
    "remoteWrite.retryMinInterval",
    "The minimum delay between retry attempts to send a block of data to the corresponding -remoteWrite.url. \
     Every next retry attempt will double the delay to prevent hammering of remote database. See also -remoteWrite.retryMaxTime",
    || ArrayDuration::with_default(Duration::from_secs(1)),
);

static RETRY_MAX_TIME: Flag<ArrayDuration> = Flag::new(
    "remoteWrite.retryMaxTime",
    "The max time spent on retry attempts to send a block of data to the corresponding -remoteWrite.url. \
     Change this value if it is expected for -remoteWrite.url to be unreachable for more than -remoteWrite.retryMaxTime. \
     See also -remoteWrite.retryMinInterval",
    || ArrayDuration::with_default(Duration::from_secs(60)),
);

static PROXY_URL: Flag<ArrayString> = Flag::new(
    "remoteWrite.proxyURL",
    "Optional proxy URL for writing data to the corresponding -remoteWrite.url. \
     Supported proxies: http, https, socks5. Example: -remoteWrite.proxyURL=socks5://proxy:1234",
    ArrayString::default,
);

// PORT NOTE: -remoteWrite.tlsHandshakeTimeout is not ported: it only matters
// for the (unsupported) TLS transport.

static TLS_INSECURE_SKIP_VERIFY: Flag<ArrayBool> = Flag::new(
    "remoteWrite.tlsInsecureSkipVerify",
    "Whether to skip tls verification when connecting to the corresponding -remoteWrite.url",
    ArrayBool::default,
);

static TLS_CERT_FILE: Flag<ArrayString> = Flag::new(
    "remoteWrite.tlsCertFile",
    "Optional path to client-side TLS certificate file to use when connecting to the corresponding -remoteWrite.url",
    ArrayString::default,
);

static TLS_KEY_FILE: Flag<ArrayString> = Flag::new(
    "remoteWrite.tlsKeyFile",
    "Optional path to client-side TLS certificate key to use when connecting to the corresponding -remoteWrite.url",
    ArrayString::default,
);

static TLS_CA_FILE: Flag<ArrayString> = Flag::new(
    "remoteWrite.tlsCAFile",
    "Optional path to TLS CA file to use for verifying connections to the corresponding -remoteWrite.url. \
     By default, system CA is used",
    ArrayString::default,
);

static TLS_SERVER_NAME: Flag<ArrayString> = Flag::new(
    "remoteWrite.tlsServerName",
    "Optional TLS server name to use for connections to the corresponding -remoteWrite.url. \
     By default, the server name from -remoteWrite.url is used",
    ArrayString::default,
);

static HEADERS: Flag<ArrayString> = Flag::new(
    "remoteWrite.headers",
    "Optional HTTP headers to send with each request to the corresponding -remoteWrite.url. \
     For example, -remoteWrite.headers='My-Auth:foobar' would send 'My-Auth: foobar' HTTP header with every request to the corresponding -remoteWrite.url. \
     Multiple headers must be delimited by '^^': -remoteWrite.headers='header1:value1^^header2:value2'",
    ArrayString::default,
);

static BASIC_AUTH_USERNAME: Flag<ArrayString> = Flag::new(
    "remoteWrite.basicAuth.username",
    "Optional basic auth username to use for the corresponding -remoteWrite.url",
    ArrayString::default,
);

static BASIC_AUTH_PASSWORD: Flag<ArrayString> = Flag::new(
    "remoteWrite.basicAuth.password",
    "Optional basic auth password to use for the corresponding -remoteWrite.url",
    ArrayString::default,
);

static BASIC_AUTH_PASSWORD_FILE: Flag<ArrayString> = Flag::new(
    "remoteWrite.basicAuth.passwordFile",
    "Optional path to basic auth password to use for the corresponding -remoteWrite.url. \
     The file is re-read every second",
    ArrayString::default,
);

static BEARER_TOKEN: Flag<ArrayString> = Flag::new(
    "remoteWrite.bearerToken",
    "Optional bearer auth token to use for the corresponding -remoteWrite.url",
    ArrayString::default,
);

static BEARER_TOKEN_FILE: Flag<ArrayString> = Flag::new(
    "remoteWrite.bearerTokenFile",
    "Optional path to bearer token file to use for the corresponding -remoteWrite.url. \
     The token is re-read from the file every second",
    ArrayString::default,
);

// PORT NOTE: the -remoteWrite.oauth2.* flags are registered for CLI
// compatibility, but OAuth2 is not supported by this port (no TLS/token
// endpoint client); init fails with a clear error when they are set.
static OAUTH2_CLIENT_ID: Flag<ArrayString> = Flag::new(
    "remoteWrite.oauth2.clientID",
    "Optional OAuth2 clientID to use for the corresponding -remoteWrite.url",
    ArrayString::default,
);

static OAUTH2_CLIENT_SECRET: Flag<ArrayString> = Flag::new(
    "remoteWrite.oauth2.clientSecret",
    "Optional OAuth2 clientSecret to use for the corresponding -remoteWrite.url",
    ArrayString::default,
);

static OAUTH2_CLIENT_SECRET_FILE: Flag<ArrayString> = Flag::new(
    "remoteWrite.oauth2.clientSecretFile",
    "Optional OAuth2 clientSecretFile to use for the corresponding -remoteWrite.url",
    ArrayString::default,
);

// ---------------------------------------------------------------------------
// Flags (pendinglogrows.go)
// ---------------------------------------------------------------------------

static MAX_UNPACKED_BLOCK_SIZE: Flag<Bytes> = Flag::new(
    "remoteWrite.maxBlockSize",
    "The maximum block size to send to remote storage. Bigger blocks may improve performance at the cost of the increased memory usage.",
    || Bytes::with_default(8 * 1024 * 1024),
);

static FLUSH_INTERVAL: Flag<ExtendedDuration> = Flag::new(
    "remoteWrite.flushInterval",
    "Interval for flushing the data to remote storage. \
     This option takes effect only when less than 2MB of data per second are pushed to -remoteWrite.url",
    || {
        let mut d = ExtendedDuration::default();
        d.set("1s")
            .expect("BUG: cannot parse default flushInterval");
        d
    },
);

// ---------------------------------------------------------------------------
// remotewrite.go
// ---------------------------------------------------------------------------

/// rwctxsGlobal contains statically populated entries when -remoteWrite.url is
/// specified.
static RWCTXS_GLOBAL: RwLock<Vec<Arc<RemoteWriteCtx>>> = RwLock::new(Vec::new());

/// Storage implements the `esl_insert::common_params::LogRowsStorage` interface
/// (Go: `insertutil.LogRowsStorage`), routing ingested rows to the configured
/// remote storages.
pub struct Storage;

impl LogRowsStorage for Storage {
    /// MustAddRows implements the LogRowsStorage interface.
    fn must_add_rows(self: &Arc<Self>, lr: &LogRows) {
        push_to_remote_storages(lr);
    }
}

// PORT NOTE: the ported kubernetescollector carries its own Go-shaped
// `LogRowsStorage` trait (including `CanWriteData`, which the esl-insert port
// dropped); implement it too, so main can register this Storage as the
// collector sink (Go: `var storage = &remotewrite.Storage{}` there).
impl crate::kubernetescollector::LogRowsStorage for Storage {
    fn must_add_rows(&self, lr: &LogRows) {
        push_to_remote_storages(lr);
    }

    /// CanWriteData implements the LogRowsStorage interface.
    fn can_write_data(&self) -> Result<(), String> {
        Ok(())
    }
}

/// maxQueues limits the maximum value for `-remoteWrite.queues`. There is no
/// sense in setting too high value, since it may lead to high memory usage due
/// to big number of buffers.
fn max_queues() -> usize {
    cgroup::available_cpus() * 16
}

const PERSISTENT_QUEUE_DIRNAME: &str = "persistent-queue";

/// InitSecretFlags must be called after flag parsing and before any logging.
pub fn init_secret_flags() {
    init_secret_flags_internal(*SHOW_REMOTE_WRITE_URL.get());
}

fn init_secret_flags_internal(show_remote_write_url: bool) {
    if !show_remote_write_url {
        // remoteWrite.url can contain authentication codes, so hide it at `/metrics` output.
        flagutil::register_secret_flag("remoteWrite.url");
    }
    // remoteWrite.proxyURL can contain credentials in the proxy URL, so hide it too.
    flagutil::register_secret_flag("remoteWrite.proxyURL");
    // remoteWrite.headers can contain auth headers such as Authorization and API keys.
    flagutil::register_secret_flag("remoteWrite.headers");
}

/// Init initializes remotewrite.
///
/// It must be called after flag parsing.
///
/// [`stop`] must be called for graceful shutdown.
pub fn init(tmp_data_path: &str) {
    let urls = REMOTE_WRITE_URLS.get();
    if urls.is_empty() {
        fatalf!("at least one `-remoteWrite.url` command-line flag must be set");
    }
    // PORT NOTE: Go clamps the flag value in place; the port computes the
    // effective queue count instead (flag values are read-only here).
    let mut queues = *QUEUES.get() as isize;
    if queues > max_queues() as isize {
        queues = max_queues() as isize;
    }
    if queues <= 0 {
        queues = 1;
    }
    let path = {
        let p = REMOTE_WRITE_TMP_DATA_PATH.get();
        if p.is_empty() {
            Path::new(tmp_data_path).join("eslagent-remotewrite-data")
        } else {
            PathBuf::from(p)
        }
    };
    init_remote_write_ctxs(&path, urls, queues as usize);
    drop_dangling_queues(&path);
}

/// Stop stops remotewrite.
///
/// It is expected that nobody pushes data during and after the call to this
/// function.
pub fn stop() {
    let rwctxs = std::mem::take(&mut *RWCTXS_GLOBAL.write().unwrap());
    for rwctx in &rwctxs {
        rwctx.must_stop();
    }
}

fn drop_dangling_queues(tmp_data_path: &Path) {
    // Remove dangling persistent queues, if any.
    // This is required for the case when the number of queues has been changed or URL have been changed.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4014
    //
    // In case if there were many persistent queues with identical *remoteWriteURLs
    // the queue with the last index will be dropped.
    // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6140
    let rwctxs = RWCTXS_GLOBAL.read().unwrap();
    let existing_queues: std::collections::HashSet<String> =
        rwctxs.iter().map(|rwctx| rwctx.fq.dirname()).collect();

    let queues_dir = tmp_data_path.join(PERSISTENT_QUEUE_DIRNAME);
    let files = vlfs::must_read_dir(&queues_dir);
    let mut removed = 0;
    for f in files {
        let dirname = f.file_name().to_string_lossy().into_owned();
        if !existing_queues.contains(&dirname) {
            infof!("removing dangling queue {dirname:?}");
            let full_path = queues_dir.join(&dirname);
            vlfs::must_remove_dir(&full_path);
            removed += 1;
        }
    }
    if removed > 0 {
        infof!(
            "removed {removed} dangling queues from {tmp_data_path:?}, active queues: {}",
            rwctxs.len()
        );
    }
}

fn init_remote_write_ctxs(tmp_data_path: &Path, urls: &[String], queues: usize) {
    if urls.is_empty() {
        panicf!("BUG: urls must be non-empty");
    }

    let mut max_inmemory_blocks = memory::allowed() / urls.len() / 10000;
    if max_inmemory_blocks / queues > 100 {
        // There is no much sense in keeping higher number of blocks in memory,
        // since this means that the producer outperforms consumer and the queue
        // will continue growing. It is better storing the queue to file.
        max_inmemory_blocks = 100 * queues;
    }
    if max_inmemory_blocks < 2 {
        max_inmemory_blocks = 2;
    }
    let mut rwctxs = Vec::with_capacity(urls.len());
    for (i, remote_write_url_raw) in urls.iter().enumerate() {
        let remote_write_url = match RemoteUrl::parse(remote_write_url_raw) {
            Ok(u) => u,
            Err(err) => {
                fatalf!("invalid -remoteWrite.url={remote_write_url_raw:?}: {err}");
                unreachable!()
            }
        };
        let sanitized_url = if *SHOW_REMOTE_WRITE_URL.get() {
            format!("{}:{}", i + 1, remote_write_url_raw)
        } else {
            format!("{}:secret-url", i + 1)
        };
        rwctxs.push(new_remote_write_ctx(
            i,
            remote_write_url,
            max_inmemory_blocks,
            &sanitized_url,
            tmp_data_path,
            queues,
        ));
    }
    // PORT NOTE: fs.RegisterPathFsMetrics(tmpDataPath) is not ported (no
    // metrics crate).

    *RWCTXS_GLOBAL.write().unwrap() = rwctxs;
}

fn push_to_remote_storages(lr: &LogRows) {
    let rwctxs = RWCTXS_GLOBAL.read().unwrap();
    if rwctxs.len() == 1 {
        // fast path
        rwctxs[0].push(lr);
        return;
    }
    // Push samples to remote storage systems in parallel in order to reduce
    // the time needed for sending the data to multiple remote storage systems.
    std::thread::scope(|s| {
        for rwctx in rwctxs.iter() {
            s.spawn(move || {
                rwctx.push(lr);
            });
        }
    });
}

struct RemoteWriteCtx {
    fq: Arc<FastQueue>,
    c: Arc<Client>,

    pls: Vec<Arc<PendingLogs>>,
    pss_next_idx: AtomicU64,
}

fn new_remote_write_ctx(
    arg_idx: usize,
    mut remote_write_url: RemoteUrl,
    max_inmemory_blocks: usize,
    sanitized_url: &str,
    tmp_data_path: &Path,
    queues: usize,
) -> Arc<RemoteWriteCtx> {
    let mut data_format = FORMAT.get().get_optional_arg(arg_idx).to_string();
    if data_format.is_empty() {
        data_format = "native".to_string();
    }
    let data_format = match data_format.as_str() {
        "native" => DataFormat::Native,
        "jsonline" => DataFormat::Jsonline,
        _ => {
            fatalf!(
                "unsupported -remoteWrite.format={data_format:?}; see https://docs.victoriametrics.com/victorialogs/vlagent/#remote-write-format"
            );
            unreachable!()
        }
    };

    if data_format == DataFormat::Native {
        // Protocol version is required by EsLogs for native data ingestion protocol.
        //
        // PORT NOTE: Go re-encodes the whole query string via url.Values
        // (which also sorts the params); the port appends/replaces only the
        // `version` param, keeping the rest of the query untouched.
        remote_write_url.set_query_param("version", PROTOCOL_VERSION);
    }

    // strip query params, otherwise changing params resets pq
    let pq_url = remote_write_url.without_query();
    let h = xxhash_rust::xxh64::xxh64(pq_url.as_bytes(), 0);
    let queue_path = tmp_data_path
        .join(PERSISTENT_QUEUE_DIRNAME)
        .join(format!("{}_{h:016X}", arg_idx + 1));
    let mut max_pending_bytes = MAX_PENDING_BYTES_PER_URL.get().get_optional_arg(arg_idx);
    if max_pending_bytes != 0 && max_pending_bytes < DEFAULT_CHUNK_FILE_SIZE as i64 {
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4195
        warnf!(
            "rounding the -remoteWrite.maxDiskUsagePerURL={max_pending_bytes} to the minimum supported value: {DEFAULT_CHUNK_FILE_SIZE}"
        );
        max_pending_bytes = DEFAULT_CHUNK_FILE_SIZE as i64;
    }

    let fq = Arc::new(FastQueue::must_open_fast_queue(
        &queue_path,
        sanitized_url,
        max_inmemory_blocks,
        max_pending_bytes,
        false,
    ));

    match remote_write_url.scheme.as_str() {
        "http" => {}
        // PORT NOTE: Go supports https; the std-TCP house client has no TLS.
        "https" => {
            fatalf!(
                "https -remoteWrite.url is not supported by this port (no TLS client): {sanitized_url}"
            );
        }
        scheme => {
            fatalf!(
                "unsupported scheme: {scheme} for remoteWriteURL: {sanitized_url}, want `http`, `https`"
            );
        }
    }
    let c = new_http_client(arg_idx, remote_write_url, sanitized_url, Arc::clone(&fq));
    c.init(arg_idx, queues, sanitized_url);

    // Initialize pss
    let mut pls_len = queues;
    let n = cgroup::available_cpus();
    if pls_len > n {
        // There is no sense in running more than availableCPUs concurrent
        // pendingLogs, since every pendingLogs can saturate up to a single CPU.
        pls_len = n;
    }
    let pls: Vec<Arc<PendingLogs>> = (0..pls_len)
        .map(|_| PendingLogs::new(Arc::clone(&fq), data_format))
        .collect();

    Arc::new(RemoteWriteCtx {
        fq,
        c,
        pls,
        pss_next_idx: AtomicU64::new(0),
    })
}

impl RemoteWriteCtx {
    fn push(&self, lr: &LogRows) {
        let pls = &self.pls;
        let idx = self.pss_next_idx.fetch_add(1, Ordering::Relaxed) + 1;
        pls[(idx % pls.len() as u64) as usize].add(lr);
    }

    fn must_stop(&self) {
        for pl in &self.pls {
            pl.must_stop();
        }
        self.fq.unblock_all_readers();
        self.c.must_stop();

        self.fq.must_close();
    }
}

// ---------------------------------------------------------------------------
// URL handling
// ---------------------------------------------------------------------------

/// Minimal parsed form of a remote write URL
/// (stand-in for Go `net/url.URL`; only what the client needs).
#[derive(Debug, Clone, PartialEq)]
struct RemoteUrl {
    scheme: String,
    /// `host[:port]` exactly as written in the URL.
    host: String,
    /// Path without query/fragment; "/" when empty.
    path: String,
    /// Raw query string without the leading '?'.
    query: String,
}

impl RemoteUrl {
    fn parse(s: &str) -> Result<RemoteUrl, String> {
        let (scheme, rest) = s
            .split_once("://")
            .ok_or_else(|| "missing scheme".to_string())?;
        if scheme.is_empty() {
            return Err("missing scheme".to_string());
        }
        // Strip the fragment first, like url.Parse.
        let rest = rest.split('#').next().unwrap_or("");
        let (host, path_and_query) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        if host.is_empty() {
            return Err("missing host".to_string());
        }
        let (path, query) = match path_and_query.split_once('?') {
            Some((p, q)) => (p, q),
            None => (path_and_query, ""),
        };
        let path = if path.is_empty() { "/" } else { path };
        Ok(RemoteUrl {
            scheme: scheme.to_string(),
            host: host.to_string(),
            path: path.to_string(),
            query: query.to_string(),
        })
    }

    /// The TCP address to dial (`host:port`, default port filled in).
    fn addr(&self) -> String {
        if self.host.contains(':') {
            self.host.clone()
        } else {
            format!("{}:80", self.host)
        }
    }

    /// Sets (or replaces) the query parameter `key`.
    fn set_query_param(&mut self, key: &str, value: &str) {
        let mut params: Vec<String> = self
            .query
            .split('&')
            .filter(|p| !p.is_empty() && *p != key && !p.starts_with(&format!("{key}=")))
            .map(|p| p.to_string())
            .collect();
        params.push(format!("{key}={value}"));
        self.query = params.join("&");
    }

    /// The URL string without query and fragment
    /// (Go: `pqURL.RawQuery = ""; pqURL.Fragment = ""; pqURL.String()`).
    fn without_query(&self) -> String {
        format!("{}://{}{}", self.scheme, self.host, self.path)
    }

    /// Path plus query, as sent on the HTTP request line.
    fn path_and_query(&self) -> String {
        if self.query.is_empty() {
            self.path.clone()
        } else {
            format!("{}?{}", self.path, self.query)
        }
    }
}

// ---------------------------------------------------------------------------
// client.go
// ---------------------------------------------------------------------------

struct Client {
    sanitized_url: String,
    remote_write_url: RemoteUrl,

    fq: Arc<FastQueue>,

    send_timeout: Duration,
    /// Nanoseconds (Go time.Duration).
    retry_min_interval: i64,
    retry_max_time: i64,

    auth_cfg: AuthConfig,
    /// Extra headers from -remoteWrite.headers.
    headers: Vec<(String, String)>,

    /// Set by [`Client::init`] when -remoteWrite.rateLimit is configured.
    rl: Mutex<Option<Arc<RateLimiter>>>,

    /// Per-worker stop channels; dropping the senders unblocks the workers
    /// (Go: `close(c.stopCh)`).
    stop_senders: Mutex<Vec<SyncSender<()>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

fn new_http_client(
    arg_idx: usize,
    remote_write_url: RemoteUrl,
    sanitized_url: &str,
    fq: Arc<FastQueue>,
) -> Arc<Client> {
    let (auth_cfg, headers) = match get_auth_config(arg_idx) {
        Ok(v) => v,
        Err(err) => {
            fatalf!(
                "cannot initialize auth config for -remoteWrite.url={}: {err}",
                sanitized_url
            );
            unreachable!()
        }
    };

    let p_url = PROXY_URL.get().get_optional_arg(arg_idx);
    if !p_url.is_empty() {
        // PORT NOTE: proxies are unsupported by the std-TCP house client.
        fatalf!("-remoteWrite.proxyURL is not supported by this port; got {p_url:?}");
    }

    let mut send_timeout = SEND_TIMEOUT.get().get_optional_arg(arg_idx);
    if send_timeout.is_zero() {
        // Go's http.Client treats a zero timeout as "no timeout"; the blocking
        // std-TCP client needs a finite cap.
        send_timeout = Duration::from_secs(60);
    }

    Arc::new(Client {
        sanitized_url: sanitized_url.to_string(),
        remote_write_url,
        fq,
        send_timeout,
        retry_min_interval: RETRY_MIN_INTERVAL
            .get()
            .get_optional_arg(arg_idx)
            .as_nanos() as i64,
        retry_max_time: RETRY_MAX_TIME.get().get_optional_arg(arg_idx).as_nanos() as i64,
        auth_cfg,
        headers,
        rl: Mutex::new(None),
        stop_senders: Mutex::new(Vec::new()),
        workers: Mutex::new(Vec::new()),
    })
}

impl Client {
    fn init(self: &Arc<Self>, arg_idx: usize, concurrency: usize, sanitized_url: &str) {
        let bytes_per_sec = RATE_LIMIT.get().get_optional_arg(arg_idx);
        let rl = if bytes_per_sec > 0 {
            infof!(
                "applying {bytes_per_sec} bytes per second rate limit for -remoteWrite.url={sanitized_url}"
            );
            Some(Arc::new(RateLimiter::new(bytes_per_sec)))
        } else {
            None
        };
        *self.rl.lock().unwrap() = rl.clone();

        let mut senders = self.stop_senders.lock().unwrap();
        let mut workers = self.workers.lock().unwrap();
        for _ in 0..concurrency {
            let (tx, rx) = sync_channel::<()>(0);
            senders.push(tx);
            let c = Arc::clone(self);
            let rl = rl.clone();
            workers.push(std::thread::spawn(move || {
                c.run_worker(&rx, rl.as_deref());
            }));
        }
        infof!("initialized client for -remoteWrite.url={sanitized_url}");
    }

    fn must_stop(&self) {
        // Dropping the senders closes every worker's stop channel.
        self.stop_senders.lock().unwrap().clear();
        if let Some(rl) = self.rl.lock().unwrap().as_ref() {
            rl.stop();
        }
        let workers = std::mem::take(&mut *self.workers.lock().unwrap());
        for w in workers {
            let _ = w.join();
        }
        infof!("stopped client for -remoteWrite.url={}", self.sanitized_url);
    }

    fn run_worker(&self, stop_rx: &Receiver<()>, rl: Option<&RateLimiter>) {
        let mut block = Vec::new();
        loop {
            block.clear();
            if !self.fq.must_read_block(&mut block) {
                return;
            }
            if block.is_empty() {
                // skip empty data blocks from sending
                continue;
            }
            if !self.send_block_http(&block, stop_rx, rl) {
                // Return unsent block to the queue.
                self.fq.must_write_block_ignore_disabled_pq(&block);
                return;
            }
        }
    }

    fn do_request(&self, body: &[u8]) -> Result<HttpResponse, String> {
        let mut headers: Vec<(String, String)> = Vec::with_capacity(4 + self.headers.len());
        headers.push(("User-Agent".to_string(), "eslagent".to_string()));
        headers.push(("Content-Encoding".to_string(), "zstd".to_string()));
        headers.push((
            "Content-Type".to_string(),
            "application/octet-stream".to_string(),
        ));
        let auth_header = self.auth_cfg.get_auth_header()?;
        if !auth_header.is_empty() {
            headers.push(("Authorization".to_string(), auth_header));
        }
        headers.extend(self.headers.iter().cloned());
        do_request_with_timeout(
            &self.remote_write_url.addr(),
            "POST",
            &self.remote_write_url.path_and_query(),
            &headers,
            Some(body),
            self.send_timeout,
        )
    }

    /// sendBlockHTTP sends the given block to the remote write URL.
    ///
    /// The function returns false only if the client is stopped.
    /// Otherwise, it tries sending the block to remote storage indefinitely.
    fn send_block_http(
        &self,
        block: &[u8],
        stop_rx: &Receiver<()>,
        rl: Option<&RateLimiter>,
    ) -> bool {
        if let Some(rl) = rl {
            rl.register(block.len());
        }
        let mut bt = BackoffTimer::new(self.retry_min_interval, self.retry_max_time);
        let mut retries_count = 0;

        loop {
            let resp = match self.do_request(block) {
                Ok(resp) => resp,
                Err(err) => {
                    remote_write_retry_logger().warnf(format_args!(
                        "couldn't send a block with size {} bytes to {:?}: {err}; re-sending the block in {}",
                        block.len(),
                        self.sanitized_url,
                        format_go_duration(bt.current_delay())
                    ));
                    if !bt.wait(stop_rx) {
                        return false;
                    }
                    continue;
                }
            };

            let status_code = resp.status_code;
            if status_code / 100 == 2 {
                return true;
            }

            if status_code == 400 || status_code == 404 {
                log_block_rejected(block, &self.sanitized_url, &resp);
                return true;
            }
            // Unexpected status code returned
            retries_count += 1;
            let retry_after_header = parse_retry_after_header(resp.header("Retry-After"));
            // retryAfterDuration has the highest priority duration
            if retry_after_header > 0 {
                bt.set_delay(retry_after_header);
            }

            errorf!(
                "unexpected status code received after sending a block with size {} bytes to {:?} during retry #{retries_count}: {status_code}; response body={:?}; re-sending the block in {}",
                block.len(),
                self.sanitized_url,
                String::from_utf8_lossy(&resp.body),
                format_go_duration(bt.current_delay())
            );
            if !bt.wait(stop_rx) {
                return false;
            }
        }
    }
}

fn remote_write_rejected_logger() -> &'static logger::LogThrottler {
    logger::with_throttler("remoteWriteRejected", Duration::from_secs(5))
}

fn remote_write_retry_logger() -> &'static logger::LogThrottler {
    logger::with_throttler("remoteWriteRetry", Duration::from_secs(5))
}

fn log_block_rejected(block: &[u8], sanitized_url: &str, resp: &HttpResponse) {
    remote_write_rejected_logger().errorf(format_args!(
        "sending a block with size {} bytes to {sanitized_url:?} was rejected (skipping the block): status code {}; response body: {}",
        block.len(),
        resp.status_code,
        String::from_utf8_lossy(&resp.body)
    ));
}

/// Builds the promauth-equivalent config for the given -remoteWrite.url index.
///
/// Returns the auth config plus the parsed -remoteWrite.headers entries.
fn get_auth_config(arg_idx: usize) -> Result<(AuthConfig, Vec<(String, String)>), String> {
    let headers_value = HEADERS.get().get_optional_arg(arg_idx);
    let mut hdrs = Vec::new();
    if !headers_value.is_empty() {
        for h in headers_value.split("^^") {
            let (name, value) = h.split_once(':').ok_or_else(|| {
                format!(
                    "invalid header {h:?} in -remoteWrite.headers; must be in `Name:value` format"
                )
            })?;
            hdrs.push((name.trim().to_string(), value.trim().to_string()));
        }
    }

    let username = BASIC_AUTH_USERNAME.get().get_optional_arg(arg_idx);
    let password = BASIC_AUTH_PASSWORD.get().get_optional_arg(arg_idx);
    let password_file = BASIC_AUTH_PASSWORD_FILE.get().get_optional_arg(arg_idx);
    let basic_auth = if !username.is_empty() || !password.is_empty() || !password_file.is_empty() {
        Some(BasicAuthConfig {
            username: username.to_string(),
            username_file: String::new(),
            password: password.to_string(),
            password_file: password_file.to_string(),
        })
    } else {
        None
    };

    let token = BEARER_TOKEN.get().get_optional_arg(arg_idx);
    let token_file = BEARER_TOKEN_FILE.get().get_optional_arg(arg_idx);

    // PORT NOTE: OAuth2 is not supported by this port; fail clearly like the
    // TLS/proxy cases instead of silently ignoring credentials.
    let client_secret = OAUTH2_CLIENT_SECRET.get().get_optional_arg(arg_idx);
    let client_secret_file = OAUTH2_CLIENT_SECRET_FILE.get().get_optional_arg(arg_idx);
    let client_id = OAUTH2_CLIENT_ID.get().get_optional_arg(arg_idx);
    if !client_secret.is_empty() || !client_secret_file.is_empty() || !client_id.is_empty() {
        return Err("-remoteWrite.oauth2.* flags are not supported by this port".to_string());
    }

    // PORT NOTE: TLS is not supported by this port (no TLS client); reject
    // the TLS flags instead of silently ignoring them.
    if !TLS_CERT_FILE.get().get_optional_arg(arg_idx).is_empty()
        || !TLS_KEY_FILE.get().get_optional_arg(arg_idx).is_empty()
        || !TLS_CA_FILE.get().get_optional_arg(arg_idx).is_empty()
        || !TLS_SERVER_NAME.get().get_optional_arg(arg_idx).is_empty()
        || TLS_INSECURE_SKIP_VERIFY.get().get_optional_arg(arg_idx)
    {
        return Err("-remoteWrite.tls* flags are not supported by this port".to_string());
    }

    let opts = Options {
        basic_auth,
        bearer_token: token.to_string(),
        bearer_token_file: token_file.to_string(),
        needs_tls: false,
    };
    let auth_cfg = opts.new_config().map_err(|err| {
        format!("cannot populate auth config for remoteWrite idx: {arg_idx}, err: {err}")
    })?;
    Ok((auth_cfg, hdrs))
}

/// parseRetryAfterHeader parses `Retry-After` value retrieved from HTTP
/// response header, returning the delay in nanoseconds.
///
/// s should be in either HTTP-date or a number of seconds.
/// It returns 0 if s does not follow RFC 7231.
fn parse_retry_after_header(s: &str) -> i64 {
    if s.is_empty() {
        return 0;
    }

    // Retry-After could be in "Mon, 02 Jan 2006 15:04:05 GMT" format.
    if let Some(target_secs) = parse_http_date(s) {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        // Go: time.Duration(time.Until(t).Seconds()) * time.Second
        // (truncates towards zero to whole seconds).
        let until_secs = (target_secs * 1_000_000_000 - now_ns) / 1_000_000_000;
        return until_secs * 1_000_000_000;
    }
    // Retry-After could be in seconds.
    if let Ok(seconds) = s.parse::<i64>() {
        return seconds * 1_000_000_000;
    }

    0
}

/// Parses an HTTP-date in Go `http.TimeFormat`
/// ("Mon, 02 Jan 2006 15:04:05 GMT"), returning unix seconds.
fn parse_http_date(s: &str) -> Option<i64> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 6 || parts[5] != "GMT" {
        return None;
    }
    if !parts[0].ends_with(',') {
        return None;
    }
    let day: i64 = parts[1].parse().ok()?;
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let month = MONTHS.iter().position(|m| *m == parts[2])? as i64 + 1;
    let year: i64 = parts[3].parse().ok()?;
    let hms: Vec<&str> = parts[4].split(':').collect();
    if hms.len() != 3 {
        return None;
    }
    let (h, m, sec): (i64, i64, i64) = (
        hms[0].parse().ok()?,
        hms[1].parse().ok()?,
        hms[2].parse().ok()?,
    );
    Some(days_from_civil(year, month, day) * 86400 + h * 3600 + m * 60 + sec)
}

/// Days since 1970-01-01 (Howard Hinnant's `days_from_civil` algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

// ---------------------------------------------------------------------------
// lib/ratelimiter/ratelimiter.go (inline port)
// ---------------------------------------------------------------------------

/// RateLimiter limits per-second rate of arbitrary resources.
///
/// Call [`RateLimiter::register`] for registering the given amounts of
/// resources.
///
/// PORT NOTE: Go blocks on a pooled timer vs the stop channel; the port uses
/// a Condvar with a deadline plus a stop flag ([`RateLimiter::stop`] replaces
/// closing the Go stopCh). The `limitReached` counter is not ported.
struct RateLimiter {
    /// The per-second limit of resources.
    per_second_limit: i64,

    state: Mutex<RateLimiterState>,
    cond: Condvar,
    stopped: AtomicBool,
}

struct RateLimiterState {
    /// The current budget. It is increased by per_second_limit every second.
    budget: i64,
    /// The next deadline for increasing the budget by per_second_limit.
    deadline: Option<Instant>,
}

impl RateLimiter {
    fn new(per_second_limit: i64) -> RateLimiter {
        RateLimiter {
            per_second_limit,
            state: Mutex::new(RateLimiterState {
                budget: 0,
                deadline: None,
            }),
            cond: Condvar::new(),
            stopped: AtomicBool::new(false),
        }
    }

    /// Register registers count resources.
    ///
    /// Register blocks if the given per-second rate limit is exceeded.
    /// It may be forcibly unblocked by calling [`RateLimiter::stop`].
    fn register(&self, count: usize) {
        let limit = self.per_second_limit;
        if limit <= 0 {
            return;
        }

        let mut st = self.state.lock().unwrap();
        while st.budget <= 0 {
            if self.stopped.load(Ordering::Relaxed) {
                return;
            }
            if let Some(deadline) = st.deadline {
                let now = Instant::now();
                if deadline > now {
                    let (guard, _) = self.cond.wait_timeout(st, deadline - now).unwrap();
                    st = guard;
                    if self.stopped.load(Ordering::Relaxed) {
                        return;
                    }
                    if st.deadline.map(|d| d > Instant::now()).unwrap_or(false) {
                        // Spurious/early wakeup: keep waiting.
                        continue;
                    }
                }
            }
            st.budget += limit;
            st.deadline = Some(Instant::now() + Duration::from_secs(1));
        }
        st.budget -= count as i64;
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.cond.notify_all();
    }
}

// ---------------------------------------------------------------------------
// pendinglogrows.go
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
enum DataFormat {
    Native,
    Jsonline,
}

struct PendingLogs {
    last_flush_time: AtomicU64,

    /// The queue to send blocks to.
    fq: Arc<FastQueue>,

    /// format is the format of the data to send to the remote storage.
    format: DataFormat,

    /// wr holds the pending data (Go: `mu sync.Mutex` + `wr writeRequest`).
    wr: Mutex<WriteRequest>,

    /// Dropping the sender stops the periodic flusher (Go: `close(stopCh)`).
    stop_tx: Mutex<Option<SyncSender<()>>>,
    periodic_flusher: Mutex<Option<JoinHandle<()>>>,
}

impl PendingLogs {
    fn new(fq: Arc<FastQueue>, format: DataFormat) -> Arc<PendingLogs> {
        let (tx, rx) = sync_channel::<()>(0);
        let pl = Arc::new(PendingLogs {
            last_flush_time: AtomicU64::new(0),
            fq,
            format,
            wr: Mutex::new(WriteRequest::default()),
            stop_tx: Mutex::new(Some(tx)),
            periodic_flusher: Mutex::new(None),
        });
        let flusher_pl = Arc::clone(&pl);
        let handle = std::thread::spawn(move || {
            flusher_pl.periodic_flusher(&rx);
        });
        *pl.periodic_flusher.lock().unwrap() = Some(handle);
        pl
    }

    fn add(&self, lr: &LogRows) {
        lr.for_each_row(|_, r| {
            self.add_log_row(r);
        });
    }

    fn add_log_row(&self, r: &InsertRow) {
        let mut bb = Vec::new();
        match self.format {
            DataFormat::Native => r.marshal(&mut bb),
            DataFormat::Jsonline => {
                r.append_json(&mut bb);
                bb.push(b'\n');
            }
        }

        let mut wr = self.wr.lock().unwrap();
        wr.pending_data.extend_from_slice(&bb);
        wr.pending_log_rows_count += 1;
        if wr.pending_data.len() > MAX_UNPACKED_BLOCK_SIZE.get().int_n() as usize {
            self.must_flush_locked(&mut wr);
        }
    }

    fn must_flush_locked(&self, wr: &mut WriteRequest) {
        self.last_flush_time
            .store(fasttime::unix_timestamp(), Ordering::Relaxed);
        wr.push(|b| {
            if !self.fq.try_write_block(b) {
                fatalf!("BUG: TryWriteBlock cannot return false");
            }
        });
    }

    fn periodic_flusher(&self, stop_rx: &Receiver<()>) {
        let flush_interval = FLUSH_INTERVAL.get().duration();
        let mut flush_seconds = flush_interval.as_secs() as i64;
        if flush_seconds <= 0 {
            flush_seconds = 1;
        }
        let d = add_jitter_to_duration(flush_interval.as_nanos() as i64).max(1);
        loop {
            match stop_rx.recv_timeout(Duration::from_nanos(d as u64)) {
                Err(RecvTimeoutError::Timeout) => {
                    if fasttime::unix_timestamp() - self.last_flush_time.load(Ordering::Relaxed)
                        < flush_seconds as u64
                    {
                        continue;
                    }
                    let mut wr = self.wr.lock().unwrap();
                    self.must_flush_locked(&mut wr);
                }
                _ => {
                    // Stop signal (sender dropped).
                    let mut wr = self.wr.lock().unwrap();
                    self.must_flush_on_stop(&mut wr);
                    return;
                }
            }
        }
    }

    /// mustFlushOnStop force pushes wr data.
    ///
    /// This is needed in order to properly save in-memory data to persistent
    /// queue on graceful shutdown.
    fn must_flush_on_stop(&self, wr: &mut WriteRequest) {
        wr.push(|b| self.fq.must_write_block_ignore_disabled_pq(b));
    }

    fn must_stop(&self) {
        self.stop_tx.lock().unwrap().take();
        if let Some(h) = self.periodic_flusher.lock().unwrap().take() {
            let _ = h.join();
        }
    }
}

#[derive(Default)]
struct WriteRequest {
    pending_data: Vec<u8>,
    pending_log_rows_count: i64,
}

impl WriteRequest {
    fn push(&mut self, push_block: impl FnOnce(&[u8])) {
        if self.pending_data.is_empty() {
            return;
        }

        let mut zb = Vec::new();
        esl_common::encoding::zstd::compress_level(&mut zb, &self.pending_data, 1);
        push_block(&zb);

        // PORT NOTE: the eslagent_remotewrite_block_size_{bytes,rows}
        // histograms are not ported.

        self.pending_data.clear();
        self.pending_log_rows_count = 0;
    }
}

// ---------------------------------------------------------------------------
// Tests (client_test.go)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_retry_after_header() {
        fn f(retry_after_string: &str, expect_result: i64) {
            let result = parse_retry_after_header(retry_after_string);
            // expect `expect_result == result` when retry_after_string is in
            // seconds or invalid; expect the difference between result and
            // expect_result to be lower than 10%
            let ok = expect_result == result
                || ((expect_result - result).abs() as f64) / (expect_result as f64) < 0.10;
            assert!(
                ok,
                "incorrect retry after duration, want (ms): {}, got (ms): {}",
                expect_result / 1_000_000,
                result / 1_000_000
            );
        }

        const SECOND: i64 = 1_000_000_000;

        // retry after header in seconds
        f("10", 10 * SECOND);
        // retry after header in date time
        f(&http_time_format(now_unix_secs() + 30), 30 * SECOND);
        // retry after header invalid
        f("invalid-retry-after", 0);
        // retry after header not in GMT
        f(
            &http_time_format(now_unix_secs() + 10).replace("GMT", "FAKETZ"),
            0,
        );
    }

    fn now_unix_secs() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// Formats unix seconds in Go `http.TimeFormat`
    /// ("Mon, 02 Jan 2006 15:04:05 GMT").
    fn http_time_format(secs: i64) -> String {
        const MONTHS: [&str; 12] = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ];
        const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
        let days = secs.div_euclid(86400);
        let rem = secs.rem_euclid(86400);
        let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
        let (y, mo, d) = civil_from_days(days);
        let weekday = WEEKDAYS[days.rem_euclid(7) as usize];
        format!(
            "{weekday}, {d:02} {} {y} {h:02}:{m:02}:{s:02} GMT",
            MONTHS[(mo - 1) as usize]
        )
    }

    /// Inverse of days_from_civil (Howard Hinnant's `civil_from_days`).
    fn civil_from_days(z: i64) -> (i64, i64, i64) {
        let z = z + 719_468;
        let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        (if m <= 2 { y + 1 } else { y }, m, d)
    }

    #[test]
    fn test_init_secret_flags() {
        // PORT NOTE: the Go test flips the *showRemoteWriteURL flag value in
        // place; ported flag values are read-only after parsing, so the test
        // exercises the internal helper that takes the value explicitly.
        flagutil::unregister_all_secret_flags();
        init_secret_flags_internal(false);
        assert!(
            flagutil::is_secret_flag("remotewrite.url"),
            "expecting remoteWrite.url to be secret"
        );
        assert!(
            flagutil::is_secret_flag("remotewrite.proxyurl"),
            "expecting remoteWrite.proxyURL to be secret"
        );
        assert!(
            flagutil::is_secret_flag("remotewrite.headers"),
            "expecting remoteWrite.headers to be secret"
        );

        flagutil::unregister_all_secret_flags();
        init_secret_flags_internal(true);
        assert!(
            !flagutil::is_secret_flag("remotewrite.url"),
            "remoteWrite.url must remain visible when -remoteWrite.showURL is set"
        );
        assert!(
            flagutil::is_secret_flag("remotewrite.proxyurl"),
            "expecting remoteWrite.proxyURL to remain secret"
        );
        assert!(
            flagutil::is_secret_flag("remotewrite.headers"),
            "expecting remoteWrite.headers to remain secret"
        );
        flagutil::unregister_all_secret_flags();
    }

    // PORT NOTE: port-only coverage for the URL helper, which replaces Go's
    // net/url usage in newRemoteWriteCtx.
    #[test]
    fn test_remote_url_parse() {
        let mut u = RemoteUrl::parse("http://localhost:9428/insert/native").unwrap();
        assert_eq!(u.scheme, "http");
        assert_eq!(u.host, "localhost:9428");
        assert_eq!(u.path, "/insert/native");
        assert_eq!(u.query, "");
        assert_eq!(u.addr(), "localhost:9428");
        assert_eq!(u.path_and_query(), "/insert/native");
        assert_eq!(u.without_query(), "http://localhost:9428/insert/native");

        u.set_query_param("version", "v1");
        assert_eq!(u.path_and_query(), "/insert/native?version=v1");
        u.set_query_param("version", "v2");
        assert_eq!(u.path_and_query(), "/insert/native?version=v2");

        let u = RemoteUrl::parse("http://host/insert/jsonline?foo=bar#frag").unwrap();
        assert_eq!(u.addr(), "host:80");
        assert_eq!(u.query, "foo=bar");
        assert_eq!(u.path_and_query(), "/insert/jsonline?foo=bar");

        let u = RemoteUrl::parse("https://host:8443").unwrap();
        assert_eq!(u.scheme, "https");
        assert_eq!(u.path, "/");

        assert!(RemoteUrl::parse("localhost:9428").is_err());
        assert!(RemoteUrl::parse("http://").is_err());
    }
}

//! Port of EsLogs `app/eslstorage/netinsert/netinsert.go`: the
//! cluster-mode insert client, which distributes the ingested logs among the
//! remote `-storageNode` nodes over `/internal/insert` using stream-hash based
//! routing.
//!
//! PORT NOTE: the HTTP transport (Go `net/http` + `promauth` +
//! `httputil.NewTransport`) is replaced by [`crate::http_client`]; see the PORT
//! NOTES there.
//!
//! PORT NOTE: Go's `pendingDataBuffers` buffered channel doubles as a buffer
//! pool with backpressure; the port models it as a `Mutex<Vec<ByteBuffer>>` +
//! `Condvar` ([`BufferPool`]) with identical blocking semantics.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use esl_common::bytesutil::ByteBuffer;
use esl_common::encoding::zstd;
use esl_common::metrics::Counter;
use esl_common::{errorf, fasttime, warnf};

use esl_logstorage::log_rows::InsertRow;

use crate::http_client::{AuthConfig, do_request};

/// The maximum size of a single data block sent to storage node.
const MAX_INSERT_BLOCK_SIZE: usize = 2 * 1024 * 1024;

/// ProtocolVersion is the version of the data ingestion protocol.
///
/// It must be changed every time the data encoding at /internal/insert HTTP
/// endpoint is changed.
pub const PROTOCOL_VERSION: &str = "v1";

/// Storage is a network storage for sending data to remote storage nodes in
/// the cluster.
pub struct Storage {
    sns: Vec<Arc<StorageNode>>,

    srt: Arc<StreamRowsTracker>,

    shared: Arc<StorageShared>,

    /// Background flusher threads (Go `s.wg`).
    handles: Mutex<Vec<JoinHandle<()>>>,
}

/// The parts of [`Storage`] shared with every [`StorageNode`]
/// (Go: `storageNode.s *Storage` back-reference).
struct StorageShared {
    disable_compression: bool,

    pending_data_buffers: BufferPool,

    stop: StopCh,

    /// Weak back-references to the nodes for `send_insert_request_to_any_node`
    /// (weak to break the `Storage → node → shared → node` cycle).
    sns: OnceLock<Vec<Weak<StorageNode>>>,
}

struct StorageNode {
    /// scheme is the http scheme to communicate with addr ("http" or "https";
    /// Go derives it from the `isTLS` argument, the port from the presence of
    /// a TLS config on `ac`).
    #[allow(dead_code, reason = "parity with Go; requests derive TLS from ac")]
    scheme: &'static str,

    /// addr is TCP address of storage node to send the ingested data to.
    addr: String,

    /// shared is the shared state of the Storage which holds the given
    /// storageNode.
    shared: Arc<StorageShared>,

    /// ac is auth config used for setting request headers such as
    /// Authorization.
    ac: AuthConfig,

    /// pendingData contains pending data, which must be sent to the storage
    /// node at the addr (guards Go's `pendingDataMu` fields).
    pending: Mutex<PendingData>,

    /// sendErrors counts failed send attempts for this storage node
    /// (the `esl_insert_remote_send_errors_total{addr=...}` registry counter).
    send_errors: Arc<Counter>,

    /// disabledUntil contains unix timestamp until the storageNode is disabled
    /// for data writing.
    disabled_until: AtomicU64,

    /// isReachable is set to true if the given storageNode is available for
    /// data writing (Go: `esl_insert_remote_is_reachable{addr=...}` gauge).
    is_reachable: AtomicBool,
}

struct PendingData {
    data: ByteBuffer,
    last_flush: Instant,
}

/// Sentinel error mirroring Go's `errTemporarilyDisabled`.
enum SendError {
    TemporarilyDisabled,
    Other(String),
}

fn new_storage_node(shared: Arc<StorageShared>, addr: String, ac: AuthConfig) -> Arc<StorageNode> {
    let scheme = if ac.tls().is_some() { "https" } else { "http" };
    let addr_for_metrics = addr.clone();
    let sn = Arc::new(StorageNode {
        scheme,
        addr,
        shared,
        ac,
        pending: Mutex::new(PendingData {
            data: ByteBuffer::default(),
            last_flush: Instant::now(),
        }),
        send_errors: esl_common::metrics::get_or_create_counter(&format!(
            "esl_insert_remote_send_errors_total{{addr={addr_label:?}}}",
            addr_label = addr_for_metrics
        )),
        disabled_until: AtomicU64::new(0),
        is_reachable: AtomicBool::new(true),
    });
    sn.is_reachable.store(true, Ordering::SeqCst);

    // Go registers the reachability gauge with a callback reading
    // sn.isReachable; the gauge keeps a strong reference to the node, exactly
    // like the Go closure does.
    let sn_gauge = Arc::clone(&sn);
    let _ = esl_common::metrics::get_or_create_gauge(
        &format!(
            "esl_insert_remote_is_reachable{{addr={addr_label:?}}}",
            addr_label = sn.addr
        ),
        Some(Box::new(move || {
            if sn_gauge.is_reachable.load(Ordering::SeqCst) {
                1.0
            } else {
                0.0
            }
        })),
    );

    sn
}

impl StorageNode {
    /// Port of Go `storageNode.backgroundFlusher`.
    fn background_flusher(&self) {
        loop {
            if self.shared.stop.wait_timeout(Duration::from_secs(1)) {
                self.flush_pending_data(true);
                return;
            }
            self.flush_pending_data(false);
        }
    }

    /// Port of Go `storageNode.flushPendingData`.
    fn flush_pending_data(&self, force: bool) {
        let pending_data = {
            let mut pending = self.pending.lock().unwrap();
            if !force && pending.last_flush.elapsed() < Duration::from_secs(1) {
                // nothing to flush
                return;
            }
            self.grab_pending_data_for_flush_locked(&mut pending)
        };

        self.must_send_insert_request(pending_data);
    }

    /// Port of Go `storageNode.debugFlush`.
    fn debug_flush(&self) {
        // Send pending samples to sn.
        self.flush_pending_data(true);

        // Instruct sn to convert the received samples into searchable parts.
        if let Err(err) = self.do_request("/internal/force_flush", None) {
            errorf!("cannot convert pending samples into searchable parts: {err}");
        }
    }

    /// Port of Go `storageNode.addRow`.
    fn add_row(&self, r: &InsertRow) {
        let mut b = get_bb();
        r.marshal(&mut b);

        if b.len() > MAX_INSERT_BLOCK_SIZE {
            warnf!(
                "skipping too long log entry, since its length exceeds {MAX_INSERT_BLOCK_SIZE} bytes; the actual log entry length is {} bytes; log entry contents: {}",
                b.len(),
                String::from_utf8_lossy(&b)
            );
            put_bb(b);
            return;
        }

        let pending_data = {
            let mut pending = self.pending.lock().unwrap();
            let pending_data = if pending.data.len() + b.len() > MAX_INSERT_BLOCK_SIZE {
                Some(self.grab_pending_data_for_flush_locked(&mut pending))
            } else {
                None
            };
            pending.data.must_write(&b);
            pending_data
        };

        put_bb(b);

        if let Some(pending_data) = pending_data {
            self.must_send_insert_request(pending_data);
        }
    }

    /// Port of Go `storageNode.grabPendingDataForFlushLocked`.
    fn grab_pending_data_for_flush_locked(&self, pending: &mut PendingData) -> ByteBuffer {
        pending.last_flush = Instant::now();
        let fresh = self.shared.pending_data_buffers.get();
        std::mem::replace(&mut pending.data, fresh)
    }

    /// Port of Go `storageNode.mustSendInsertRequest`.
    fn must_send_insert_request(&self, mut pending_data: ByteBuffer) {
        let err = self.send_insert_request(&pending_data);
        match err {
            Ok(()) => {
                pending_data.reset();
                self.shared.pending_data_buffers.put(pending_data);
                return;
            }
            Err(SendError::TemporarilyDisabled) => {}
            Err(SendError::Other(err)) => {
                warnf!("{err}; re-routing the data block to the remaining nodes");
            }
        }

        while !self.shared.send_insert_request_to_any_node(&pending_data) {
            errorf!(
                "cannot send pending data to storage nodes, since all of them are unavailable; re-trying to send the data in a second"
            );

            if self.shared.stop.wait_timeout(Duration::from_secs(1)) {
                errorf!(
                    "dropping {} bytes of data, since there are no available storage nodes",
                    pending_data.len()
                );
                break;
            }
        }

        pending_data.reset();
        self.shared.pending_data_buffers.put(pending_data);
    }

    /// Port of Go `storageNode.sendInsertRequest`.
    fn send_insert_request(&self, pending_data: &ByteBuffer) -> Result<(), SendError> {
        let data_len = pending_data.len();
        if data_len == 0 {
            // Nothing to send.
            return Ok(());
        }

        if self.disabled_until.load(Ordering::SeqCst) > fasttime::unix_timestamp() {
            self.send_errors.inc();
            return Err(SendError::TemporarilyDisabled);
        }

        let mut compressed = Vec::new();
        let body: &[u8] = if !self.shared.disable_compression {
            zstd::compress_level(&mut compressed, &pending_data.b, 1);
            &compressed
        } else {
            &pending_data.b
        };

        if let Err(err) = self.do_request("/internal/insert", Some(body)) {
            return Err(SendError::Other(format!(
                "cannot send data block with the length {}: {err}",
                pending_data.len()
            )));
        }

        Ok(())
    }

    /// Port of Go `storageNode.doRequest`.
    ///
    /// PORT NOTE: `body` is the (possibly compressed) request payload; Go
    /// passes an `io.Reader`, the port passes buffered bytes. Cancellation via
    /// `contextutil.NewStopChanContext` is not supported (see `http_client`).
    fn do_request(&self, path: &str, body: Option<&[u8]>) -> Result<(), String> {
        let method = if body.is_some() { "POST" } else { "GET" };

        let req_url = self.get_request_url(path);
        let mut headers: Vec<(String, String)> = Vec::new();
        if body.is_some() {
            headers.push((
                "Content-Type".to_string(),
                "application/octet-stream".to_string(),
            ));
            if !self.shared.disable_compression {
                headers.push(("Content-Encoding".to_string(), "zstd".to_string()));
            }
        }
        match self.ac.get_auth_header() {
            Ok(auth) => {
                if !auth.is_empty() {
                    headers.push(("Authorization".to_string(), auth));
                }
            }
            Err(err) => {
                self.send_errors.inc();
                return Err(format!("cannot set auth headers for {req_url}: {err}"));
            }
        }

        let resp = match do_request(&self.addr, self.ac.tls(), method, &req_url, &headers, body) {
            Ok(resp) => resp,
            Err(err) => {
                self.set_disable_temporarily();
                return Err(format!("cannot send http request to {req_url}: {err}"));
            }
        };

        if resp.status_code / 100 == 2 {
            self.is_reachable.store(true, Ordering::SeqCst);
            return Ok(());
        }

        self.set_disable_temporarily();

        Err(format!(
            "unexpected response status code for request to {req_url}: {}; want 2xx; response body: {:?}",
            resp.status_code,
            String::from_utf8_lossy(&resp.body)
        ))
    }

    /// Port of Go `storageNode.getRequestURL`.
    ///
    /// PORT NOTE: returns the path-and-query part only; the `http://addr`
    /// prefix is implied by `http_client::do_request(addr, ...)`.
    fn get_request_url(&self, path: &str) -> String {
        format!("{path}?version={PROTOCOL_VERSION}")
    }

    /// Port of Go `storageNode.setDisableTemporarily`.
    fn set_disable_temporarily(&self) {
        // Disable sending data to this sn for 10 seconds.
        self.disabled_until
            .store(fasttime::unix_timestamp() + 10, Ordering::SeqCst);

        self.send_errors.inc();
        self.is_reachable.store(false, Ordering::SeqCst);
    }
}

// -- small ByteBuffer pool (Go `bbPool bytesutil.ByteBufferPool`) --

static BB_POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

fn get_bb() -> Vec<u8> {
    BB_POOL.lock().unwrap().pop().unwrap_or_default()
}

fn put_bb(mut b: Vec<u8>) {
    b.clear();
    BB_POOL.lock().unwrap().push(b);
}

impl StorageShared {
    /// Port of Go `Storage.sendInsertRequestToAnyNode`.
    fn send_insert_request_to_any_node(&self, pending_data: &ByteBuffer) -> bool {
        let sns = self.sns.get().map(Vec::as_slice).unwrap_or_default();
        if sns.is_empty() {
            return false;
        }
        let start_idx = fastrand_uint32n(sns.len() as u32) as usize;
        for i in 0..sns.len() {
            let idx = (start_idx + i) % sns.len();
            let Some(sn) = sns[idx].upgrade() else {
                continue;
            };
            match sn.send_insert_request(pending_data) {
                Ok(()) => return true,
                Err(SendError::TemporarilyDisabled) => {}
                Err(SendError::Other(err)) => {
                    warnf!(
                        "cannot send pending data to the storage node {:?}: {err}; trying to send it to another storage node",
                        sn.addr
                    );
                }
            }
        }
        false
    }
}

/// Returns new Storage for the given addrs with the given auth_cfgs
/// (Go `NewStorage`).
///
/// The concurrency is the average number of concurrent connections per every
/// addr.
///
/// If disable_compression is set, then the data is sent uncompressed to the
/// remote storage.
///
/// Call [`Storage::must_stop`] on the returned storage when it is no longer
/// needed.
pub fn new_storage(
    addrs: &[String],
    auth_cfgs: Vec<AuthConfig>,
    concurrency: usize,
    disable_compression: bool,
) -> Storage {
    let cap = concurrency * addrs.len();
    let mut buffers = Vec::with_capacity(cap);
    for _ in 0..cap {
        buffers.push(ByteBuffer::default());
    }

    let shared = Arc::new(StorageShared {
        disable_compression,
        pending_data_buffers: BufferPool::new(buffers),
        stop: StopCh::default(),
        sns: OnceLock::new(),
    });

    let mut sns = Vec::with_capacity(addrs.len());
    for (addr, ac) in addrs.iter().zip(auth_cfgs) {
        sns.push(new_storage_node(Arc::clone(&shared), addr.clone(), ac));
    }
    shared
        .sns
        .set(sns.iter().map(Arc::downgrade).collect())
        .unwrap_or_else(|_| unreachable!("BUG: sns set twice"));

    // Start the background flushers (Go: `s.wg.Go(sn.backgroundFlusher)` inside
    // `newStorageNode`).
    let mut handles = Vec::with_capacity(sns.len());
    for sn in &sns {
        let sn = Arc::clone(sn);
        handles.push(
            std::thread::Builder::new()
                .name("netinsert_flusher".to_string())
                .spawn(move || sn.background_flusher())
                .expect("cannot spawn netinsert background flusher"),
        );
    }

    // Active streams tracker.
    let srt = Arc::new(new_stream_rows_tracker(addrs.len()));
    let srt_gauge = Arc::clone(&srt);
    let _ = esl_common::metrics::get_or_create_gauge(
        "esl_insert_active_streams",
        Some(Box::new(move || {
            srt_gauge.rows_per_stream.lock().unwrap().len() as f64
        })),
    );

    Storage {
        sns,
        srt,
        shared,
        handles: Mutex::new(handles),
    }
}

impl Storage {
    /// Returns the number of log streams being tracked since the Storage start
    /// (Go `getActiveStreams`; exported by the `esl_insert_active_streams`
    /// gauge).
    pub fn get_active_streams(&self) -> usize {
        self.srt.rows_per_stream.lock().unwrap().len()
    }

    /// Stops the s (Go `MustStop`).
    pub fn must_stop(&self) {
        self.shared.stop.close();
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        for h in handles {
            let _ = h.join();
        }
    }

    /// Flushes pending samples to s, so they become visible for querying
    /// (Go `DebugFlush`).
    pub fn debug_flush(&self) {
        std::thread::scope(|scope| {
            for sn in &self.sns {
                scope.spawn(|| sn.debug_flush());
            }
        });
    }

    /// Adds the given log row into s (Go `AddRow`).
    pub fn add_row(&self, stream_hash: u64, r: &InsertRow) {
        let idx = self.srt.get_node_idx(stream_hash);
        let sn = &self.sns[idx as usize];
        sn.add_row(r);
    }

    /// Returns per-node `(addr, send_errors, is_reachable)` stats
    /// (upstream exposes these as the `esl_insert_remote_send_errors_total` /
    /// `esl_insert_remote_is_reachable` metrics; see the module PORT NOTE).
    pub fn node_stats(&self) -> Vec<(String, u64, bool)> {
        self.sns
            .iter()
            .map(|sn| {
                (
                    sn.addr.clone(),
                    sn.send_errors.get(),
                    sn.is_reachable.load(Ordering::SeqCst),
                )
            })
            .collect()
    }
}

/// Port of Go `streamRowsTracker`.
struct StreamRowsTracker {
    nodes_count: u64,
    rows_per_stream: Mutex<HashMap<u64, u64>>,
}

/// Port of Go `newStreamRowsTracker`.
fn new_stream_rows_tracker(nodes_count: usize) -> StreamRowsTracker {
    StreamRowsTracker {
        nodes_count: nodes_count as u64,
        rows_per_stream: Mutex::new(HashMap::new()),
    }
}

impl StreamRowsTracker {
    /// Port of Go `streamRowsTracker.getNodeIdx`.
    fn get_node_idx(&self, stream_hash: u64) -> u64 {
        if self.nodes_count == 1 {
            // Fast path for a single node.
            return 0;
        }

        let mut rows_per_stream = self.rows_per_stream.lock().unwrap();
        let stream_rows = rows_per_stream.entry(stream_hash).or_insert(0);
        *stream_rows += 1;

        if *stream_rows <= 1000 {
            // Write the initial rows for the stream to a single storage node for better locality.
            // This should work great for log streams containing small number of logs, since will be distributed
            // evenly among available storage nodes because they have different streamHash.
            return stream_hash % self.nodes_count;
        }

        // The log stream contains more than 1000 rows. Distribute them among storage nodes at random
        // in order to improve query performance over this stream (the data for the log stream
        // can be processed in parallel on all the storage nodes).
        //
        // The random distribution is preferred over round-robin distribution in order to avoid possible
        // dependency between the order of the ingested logs and the number of storage nodes,
        // which may lead to non-uniform distribution of logs among storage nodes.
        fastrand_uint32n(self.nodes_count as u32) as u64
    }
}

/// Uniform random `u32` in `[0, n)` (replaces Go's `valyala/fastrand.Uint32n`).
///
/// PORT NOTE: dependency-free splitmix64 over an atomic counter; distribution
/// quality matches the use case (spreading rows across nodes).
fn fastrand_uint32n(n: u32) -> u32 {
    static COUNTER: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let x = COUNTER.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    (z % n as u64) as u32
}

// -- stop channel + buffer pool plumbing --

/// Close-once stop signal with timed waits (Go `stopCh chan struct{}` +
/// `timerpool` select loops).
#[derive(Default)]
struct StopCh {
    stopped: Mutex<bool>,
    cv: Condvar,
}

impl StopCh {
    fn close(&self) {
        *self.stopped.lock().unwrap() = true;
        self.cv.notify_all();
    }

    /// Waits up to `d`; returns true when the stop signal fired.
    fn wait_timeout(&self, d: Duration) -> bool {
        let guard = self.stopped.lock().unwrap();
        if *guard {
            return true;
        }
        let (guard, _timeout) = self.cv.wait_timeout(guard, d).unwrap();
        *guard
    }
}

/// Blocking pool of reusable [`ByteBuffer`]s (Go: the `pendingDataBuffers`
/// buffered channel; see the module PORT NOTE).
struct BufferPool {
    buffers: Mutex<Vec<ByteBuffer>>,
    cv: Condvar,
}

impl BufferPool {
    fn new(buffers: Vec<ByteBuffer>) -> BufferPool {
        BufferPool {
            buffers: Mutex::new(buffers),
            cv: Condvar::new(),
        }
    }

    /// Takes a buffer from the pool, blocking until one is available
    /// (Go `<-s.pendingDataBuffers`).
    fn get(&self) -> ByteBuffer {
        let mut buffers = self.buffers.lock().unwrap();
        loop {
            if let Some(b) = buffers.pop() {
                return b;
            }
            buffers = self.cv.wait(buffers).unwrap();
        }
    }

    /// Returns a buffer to the pool (Go `s.pendingDataBuffers <- pendingData`).
    fn put(&self, b: ByteBuffer) {
        self.buffers.lock().unwrap().push(b);
        self.cv.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of Go `TestStreamRowsTracker` (netinsert_test.go).
    ///
    /// PORT NOTE: Go seeds `rand.NewSource(0)` for the row→stream assignment;
    /// the exact Go PRNG sequence is not reproduced — a deterministic LCG picks
    /// the streams instead. The distribution assertion (max 15% deviation from
    /// the uniform per-node row count) is unchanged, and the stream hashes are
    /// byte-identical to Go's `xxhash.Sum64`.
    #[test]
    fn test_stream_rows_tracker() {
        fn f(rows_count: usize, streams_count: usize, nodes_count: usize) {
            // generate stream hashes
            let stream_hashes: Vec<u64> = (0..streams_count)
                .map(|i| xxhash_rust::xxh64::xxh64(format!("stream {i}.").as_bytes(), 0))
                .collect();

            let srt = new_stream_rows_tracker(nodes_count);

            // Deterministic LCG (see the PORT NOTE above).
            let mut rng_state: u64 = 0;
            let mut rng = move || {
                rng_state = rng_state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (rng_state >> 33) as usize
            };

            let mut rows_per_node = vec![0u64; nodes_count];
            for _ in 0..rows_count {
                let stream_idx = rng() % streams_count;
                let h = stream_hashes[stream_idx];
                let node_idx = srt.get_node_idx(h);
                rows_per_node[node_idx as usize] += 1;
            }

            // Verify that rows are uniformly distributed among nodes.
            let expected_rows_per_node = rows_count as f64 / nodes_count as f64;
            for (node_idx, &node_rows) in rows_per_node.iter().enumerate() {
                let deviation =
                    (node_rows as f64 - expected_rows_per_node).abs() / expected_rows_per_node;
                assert!(
                    deviation <= 0.15,
                    "non-uniform distribution of rows among nodes; node {node_idx} has {node_rows} rows, \
                     while it must have {expected_rows_per_node} rows; rowsPerNode={rows_per_node:?}"
                );
            }
        }

        f(10000, 9, 2);
        f(10000, 100, 2);
        f(100000, 1000, 9);
    }

    #[test]
    fn test_new_storage_stop_without_nodes_activity() {
        // The storage must start its background flushers and stop cleanly even
        // when the nodes are unreachable and no data was ingested.
        let addrs = vec!["127.0.0.1:1".to_string()];
        let auth_cfgs = vec![crate::http_client::Options::default().new_config().unwrap()];
        let s = new_storage(&addrs, auth_cfgs, 2, false);
        assert_eq!(s.get_active_streams(), 0);
        assert_eq!(s.node_stats().len(), 1);
        s.must_stop();
    }
}

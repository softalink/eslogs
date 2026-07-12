//! Port of the network-listener path of EsLogs
//! `app/eslinsert/syslog/syslog.go`: `MustInit`/`MustStop`, the
//! `-syslog.listenAddr.{tcp,udp,unix}` flag families, the per-listener
//! goroutines (threads here), the octet-counting and octet-stuffing
//! (non-transparent, newline-delimited) `syslogLineReader` framing, per-listener
//! compression/stream-fields/extra-fields options and graceful shutdown.
//!
//! The message-level processing (`processLine`) lives in [`crate::syslog`];
//! this module feeds it framed lines.
//!
//! PORT NOTE: Go accesses the storage through the global `eslstorage` package;
//! the standardized Rust app layer passes `&Arc<Storage>` explicitly, so
//! [`must_init`] takes the storage handle (`insertutil.CanWriteData()` is
//! dropped for the same reason — see `common_params.rs`).
//!
//! PORT NOTE: TLS (`-syslog.tls*`) is ported on top of
//! `esl_common::tlsutil::get_server_tls_config` (the `netutil.GetServerTLSConfig`
//! port). Go wraps the accepted conn with `tls.Server` inside
//! `netutil.TCPListener.Accept`, so the handshake happens lazily on the first
//! `Read` in the connection goroutine; the port mirrors this with
//! [`SyslogTcpConn`], which completes the handshake on the first read in the
//! per-connection worker. A failed handshake therefore surfaces as a
//! `process_stream` error in the worker and never kills the accept loop.
//!
//! PORT NOTE: unix-socket listeners (`-syslog.listenAddr.unix`, including the
//! `unixgram:` prefix) are only available behind `cfg(unix)`; on other
//! platforms setting the flag is a fatal startup error (Go's `net.ListenUnix`
//! would fail there at runtime as well).
//!
//! PORT NOTE: the `esl_errors_total`/`esl_udp_reqests_total`/`esl_udp_errors_total`
//! metrics and the `writeconcurrencylimiter` wrapper are not ported, matching
//! the metrics omissions documented in `common_params.rs` and `httpserver.rs`.
//!
//! PORT NOTE: shutdown follows the house pattern from
//! `esl-common/src/httpserver.rs`: an `AtomicBool`-style stop signal, blocking
//! `accept()` woken by a throwaway self-connect, and read-timeout polling for
//! datagram sockets (Go closes the listeners/sockets instead).

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read};
use std::net::{Shutdown, TcpListener, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use esl_common::cgroup::available_cpus;
use esl_common::flagutil::{ArrayBool, ArrayString, Flag};
use esl_common::timeutil::get_local_timezone_offset_nsecs;
use esl_common::tlsutil::{TlsServerStream, get_server_tls_config, rustls, server_accept};
use esl_common::{errorf, fatalf, infof, panicf};

use esl_logstorage::rows::Field;

use crate::common_params::LogRowsStorage;
use esl_logstorage::stream_tags::check_stream_field_names;
use esl_logstorage::tenant_id::{TenantID, parse_tenant_id};

use crate::common_params::{CommonParams, get_common_params_for_syslog, now_unix_nanos};
use crate::line_reader::MAX_LINE_SIZE_BYTES;
use crate::syslog::{SyslogLogMessageProcessor, process_line};

// How often datagram readers wake from a blocking read to re-check the stop
// signal (Go unblocks them by closing the socket instead).
const PACKET_POLL: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

static SYSLOG_TIMEZONE: Flag<String> = Flag::new(
    "syslog.timezone",
    "Timezone to use when parsing timestamps in RFC3164 syslog messages. Timezone must be a valid IANA Time Zone. \
     For example: America/New_York, Europe/Berlin, Etc/GMT+3 . See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/",
    || "Local".to_string(),
);

static LISTEN_ADDR_TCP: Flag<ArrayString> = Flag::new(
    "syslog.listenAddr.tcp",
    "Comma-separated list of TCP addresses to listen to for Syslog messages. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/",
    ArrayString::default,
);
static LISTEN_ADDR_UDP: Flag<ArrayString> = Flag::new(
    "syslog.listenAddr.udp",
    "Comma-separated list of UDP addresses to listen to for Syslog messages. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/",
    ArrayString::default,
);
static LISTEN_ADDR_UNIX: Flag<ArrayString> = Flag::new(
    "syslog.listenAddr.unix",
    "Comma-separated list of Unix socket filepaths to listen to for Syslog messages. \
     Filepaths may be prepended with 'unixgram:'  for listening for SOCK_DGRAM sockets. By default SOCK_STREAM sockets are used. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/",
    ArrayString::default,
);

static TLS_ENABLE: Flag<ArrayBool> = Flag::new(
    "syslog.tls",
    "Whether to enable TLS for receiving syslog messages at the corresponding -syslog.listenAddr.tcp. \
     The corresponding -syslog.tlsCertFile and -syslog.tlsKeyFile must be set if -syslog.tls is set. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#security",
    ArrayBool::default,
);
static TLS_CERT_FILE: Flag<ArrayString> = Flag::new(
    "syslog.tlsCertFile",
    "Path to file with TLS certificate for the corresponding -syslog.listenAddr.tcp if the corresponding -syslog.tls is set. \
     Prefer ECDSA certs instead of RSA certs as RSA certs are slower. The provided certificate file is automatically re-read every second, so it can be dynamically updated. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#security",
    ArrayString::default,
);
static TLS_KEY_FILE: Flag<ArrayString> = Flag::new(
    "syslog.tlsKeyFile",
    "Path to file with TLS key for the corresponding -syslog.listenAddr.tcp if the corresponding -syslog.tls is set. \
     The provided key file is automatically re-read every second, so it can be dynamically updated. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#security",
    ArrayString::default,
);
static TLS_CIPHER_SUITES: Flag<ArrayString> = Flag::new(
    "syslog.tlsCipherSuites",
    "Optional list of TLS cipher suites for -syslog.listenAddr.tcp if -syslog.tls is set. \
     See the list of supported cipher suites at https://pkg.go.dev/crypto/tls#pkg-constants . \
     See also https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#security",
    ArrayString::default,
);
static TLS_MIN_VERSION: Flag<String> = Flag::new(
    "syslog.tlsMinVersion",
    "The minimum TLS version to use for -syslog.listenAddr.tcp if -syslog.tls is set. \
     Supported values: TLS10, TLS11, TLS12, TLS13. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#security",
    || "TLS13".to_string(),
);

static STREAM_FIELDS_TCP: Flag<ArrayString> = Flag::new(
    "syslog.streamFields.tcp",
    "Fields to use as log stream labels for logs ingested via the corresponding -syslog.listenAddr.tcp. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#stream-fields",
    ArrayString::default,
);
static STREAM_FIELDS_UDP: Flag<ArrayString> = Flag::new(
    "syslog.streamFields.udp",
    "Fields to use as log stream labels for logs ingested via the corresponding -syslog.listenAddr.udp. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#stream-fields",
    ArrayString::default,
);
static STREAM_FIELDS_UNIX: Flag<ArrayString> = Flag::new(
    "syslog.streamFields.unix",
    "Fields to use as log stream labels for logs ingested via the corresponding -syslog.listenAddr.unix. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#stream-fields",
    ArrayString::default,
);

static IGNORE_FIELDS_TCP: Flag<ArrayString> = Flag::new(
    "syslog.ignoreFields.tcp",
    "Fields to ignore at logs ingested via the corresponding -syslog.listenAddr.tcp. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#dropping-fields",
    ArrayString::default,
);
static IGNORE_FIELDS_UDP: Flag<ArrayString> = Flag::new(
    "syslog.ignoreFields.udp",
    "Fields to ignore at logs ingested via the corresponding -syslog.listenAddr.udp. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#dropping-fields",
    ArrayString::default,
);
static IGNORE_FIELDS_UNIX: Flag<ArrayString> = Flag::new(
    "syslog.ignoreFields.unix",
    "Fields to ignore at logs ingested via the corresponding -syslog.listenAddr.unix. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#dropping-fields",
    ArrayString::default,
);

static DECOLORIZE_FIELDS_TCP: Flag<ArrayString> = Flag::new(
    "syslog.decolorizeFields.tcp",
    "Fields to remove ANSI color codes across logs ingested via the corresponding -syslog.listenAddr.tcp. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#decolorizing-fields",
    ArrayString::default,
);
static DECOLORIZE_FIELDS_UDP: Flag<ArrayString> = Flag::new(
    "syslog.decolorizeFields.udp",
    "Fields to remove ANSI color codes across logs ingested via the corresponding -syslog.listenAddr.udp. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#decolorizing-fields",
    ArrayString::default,
);
static DECOLORIZE_FIELDS_UNIX: Flag<ArrayString> = Flag::new(
    "syslog.decolorizeFields.unix",
    "Fields to remove ANSI color codes across logs ingested via the corresponding -syslog.listenAddr.unix. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#decolorizing-fields",
    ArrayString::default,
);

static EXTRA_FIELDS_TCP: Flag<ArrayString> = Flag::new(
    "syslog.extraFields.tcp",
    "Fields to add to logs ingested via the corresponding -syslog.listenAddr.tcp. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#adding-extra-fields",
    ArrayString::default,
);
static EXTRA_FIELDS_UDP: Flag<ArrayString> = Flag::new(
    "syslog.extraFields.udp",
    "Fields to add to logs ingested via the corresponding -syslog.listenAddr.udp. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#adding-extra-fields",
    ArrayString::default,
);
static EXTRA_FIELDS_UNIX: Flag<ArrayString> = Flag::new(
    "syslog.extraFields.unix",
    "Fields to add to logs ingested via the corresponding -syslog.listenAddr.unix. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#adding-extra-fields",
    ArrayString::default,
);

static TENANT_ID_TCP: Flag<ArrayString> = Flag::new(
    "syslog.tenantID.tcp",
    "TenantID for logs ingested via the corresponding -syslog.listenAddr.tcp. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#multitenancy",
    ArrayString::default,
);
static TENANT_ID_UDP: Flag<ArrayString> = Flag::new(
    "syslog.tenantID.udp",
    "TenantID for logs ingested via the corresponding -syslog.listenAddr.udp. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#multitenancy",
    ArrayString::default,
);
static TENANT_ID_UNIX: Flag<ArrayString> = Flag::new(
    "syslog.tenantID.unix",
    "TenantID for logs ingested via the corresponding -syslog.listenAddr.unix. \
     See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#multitenancy",
    ArrayString::default,
);

static COMPRESS_METHOD_TCP: Flag<ArrayString> = Flag::new(
    "syslog.compressMethod.tcp",
    "Compression method for syslog messages received at the corresponding -syslog.listenAddr.tcp. \
     Supported values: none, gzip, deflate. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#compression",
    ArrayString::default,
);
static COMPRESS_METHOD_UDP: Flag<ArrayString> = Flag::new(
    "syslog.compressMethod.udp",
    "Compression method for syslog messages received at the corresponding -syslog.listenAddr.udp. \
     Supported values: none, gzip, deflate. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#compression",
    ArrayString::default,
);
static COMPRESS_METHOD_UNIX: Flag<ArrayString> = Flag::new(
    "syslog.compressMethod.unix",
    "Compression method for syslog messages received at the corresponding -syslog.listenAddr.unix. \
     Supported values: none, gzip, deflate. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#compression",
    ArrayString::default,
);

static USE_LOCAL_TIMESTAMP_TCP: Flag<ArrayBool> = Flag::new(
    "syslog.useLocalTimestamp.tcp",
    "Whether to use local timestamp instead of the original timestamp for the ingested syslog messages \
     at the corresponding -syslog.listenAddr.tcp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#log-timestamps",
    ArrayBool::default,
);
static USE_LOCAL_TIMESTAMP_UDP: Flag<ArrayBool> = Flag::new(
    "syslog.useLocalTimestamp.udp",
    "Whether to use local timestamp instead of the original timestamp for the ingested syslog messages \
     at the corresponding -syslog.listenAddr.udp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#log-timestamps",
    ArrayBool::default,
);
static USE_LOCAL_TIMESTAMP_UNIX: Flag<ArrayBool> = Flag::new(
    "syslog.useLocalTimestamp.unix",
    "Whether to use local timestamp instead of the original timestamp for the ingested syslog messages \
     at the corresponding -syslog.listenAddr.unix. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#log-timestamps",
    ArrayBool::default,
);

static USE_REMOTE_IP_TCP: Flag<ArrayBool> = Flag::new(
    "syslog.useRemoteIP.tcp",
    "Whether to add remote ip address as 'remote_ip' log field for syslog messages ingested \
     via the corresponding -syslog.listenAddr.tcp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#capturing-remote-ip-address",
    ArrayBool::default,
);
static USE_REMOTE_IP_UDP: Flag<ArrayBool> = Flag::new(
    "syslog.useRemoteIP.udp",
    "Whether to add remote ip address as 'remote_ip' log field for syslog messages ingested \
     via the corresponding -syslog.listenAddr.udp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#capturing-remote-ip-address",
    ArrayBool::default,
);
static USE_REMOTE_IP_UNIX: Flag<ArrayBool> = Flag::new(
    "syslog.useRemoteIP.unix",
    "Whether to add remote ip address as 'remote_ip' log field for syslog messages ingested \
     via the corresponding -syslog.listenAddr.unix. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#capturing-remote-ip-address",
    ArrayBool::default,
);

// ---------------------------------------------------------------------------
// MustInit / MustStop
// ---------------------------------------------------------------------------

/// MustInit initializes the syslog listeners at the given
/// `-syslog.listenAddr.tcp` and `-syslog.listenAddr.udp` ports.
///
/// This function must be called after the flags are parsed.
///
/// [`must_stop`] must be called in order to free up resources occupied by the
/// initialized syslog listeners.
///
/// PORT NOTE: Go reads the storage through the global `eslstorage` package;
/// the port takes it explicitly.
pub fn must_init<S: LogRowsStorage + 'static>(storage: &Arc<S>) {
    let mut workers = WORKERS.lock().unwrap();
    if workers.is_some() {
        panicf!("BUG: must_init() called twice without must_stop() call");
    }
    let stop = Arc::new(StopSignal::new());
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for (arg_idx, addr) in LISTEN_ADDR_TCP.get().iter().enumerate() {
        let addr = addr.clone();
        let storage = Arc::clone(storage);
        let stop = Arc::clone(&stop);
        handles.push(spawn_worker(format!("syslog-tcp-{arg_idx}"), move || {
            run_tcp_listener(&addr, arg_idx, &storage, &stop);
        }));
    }

    for (arg_idx, addr) in LISTEN_ADDR_UDP.get().iter().enumerate() {
        let addr = addr.clone();
        let storage = Arc::clone(storage);
        let stop = Arc::clone(&stop);
        handles.push(spawn_worker(format!("syslog-udp-{arg_idx}"), move || {
            run_udp_listener(&addr, arg_idx, &storage, &stop);
        }));
    }

    for (arg_idx, addr) in LISTEN_ADDR_UNIX.get().iter().enumerate() {
        let addr = addr.clone();
        let storage = Arc::clone(storage);
        let stop = Arc::clone(&stop);
        handles.push(spawn_worker(format!("syslog-unix-{arg_idx}"), move || {
            run_unix_listener(&addr, arg_idx, &storage, &stop);
        }));
    }

    GLOBAL_CURRENT_YEAR.store(current_year_local(), Ordering::SeqCst);
    {
        let stop = Arc::clone(&stop);
        handles.push(spawn_worker("syslog-current-year".to_string(), move || {
            // Go uses a minute time.Ticker plus a select on workersStopCh; the
            // port waits on the stop signal with a one-minute timeout.
            while !stop.wait_timeout(Duration::from_secs(60)) {
                GLOBAL_CURRENT_YEAR.store(current_year_local(), Ordering::SeqCst);
            }
        }));
    }

    let tz = SYSLOG_TIMEZONE.get();
    match parse_timezone_offset_secs(tz) {
        Ok(offset_secs) => GLOBAL_TIMEZONE_OFFSET_SECS.store(offset_secs, Ordering::SeqCst),
        Err(err) => {
            fatalf!("cannot parse -syslog.timezone={tz:?}: {err}");
        }
    }

    *workers = Some(Workers { stop, handles });
}

fn spawn_worker(name: String, f: impl FnOnce() + Send + 'static) -> JoinHandle<()> {
    thread::Builder::new()
        .name(name)
        .spawn(f)
        .expect("FATAL: cannot spawn syslog worker thread")
}

// Go: globalCurrentYear atomic.Int64 / globalTimezone *time.Location.
//
// PORT NOTE: std Rust has no IANA timezone database, so the timezone is kept
// as a fixed UTC offset in seconds (see `syslog_parser::get_syslog_parser`).
static GLOBAL_CURRENT_YEAR: AtomicI64 = AtomicI64::new(0);
static GLOBAL_TIMEZONE_OFFSET_SECS: AtomicI64 = AtomicI64::new(0);

// Go: workersWG sync.WaitGroup / workersStopCh chan struct{}.
struct Workers {
    stop: Arc<StopSignal>,
    handles: Vec<JoinHandle<()>>,
}

static WORKERS: Mutex<Option<Workers>> = Mutex::new(None);

/// MustStop stops the syslog listeners initialized via [`must_init`].
pub fn must_stop() {
    let workers = WORKERS.lock().unwrap().take();
    let Some(w) = workers else {
        // Go panics on `close(nil)` here.
        panicf!("BUG: must_stop() called without must_init() call");
        unreachable!()
    };
    w.stop.stop();
    for h in w.handles {
        let _ = h.join();
    }
}

// ---------------------------------------------------------------------------
// Unix listeners
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn run_unix_listener<S: LogRowsStorage + 'static>(
    addr: &str,
    arg_idx: usize,
    storage: &Arc<S>,
    stop: &Arc<StopSignal>,
) {
    let cfg = match get_configs(
        "unix",
        arg_idx,
        STREAM_FIELDS_UNIX.get(),
        IGNORE_FIELDS_UNIX.get(),
        DECOLORIZE_FIELDS_UNIX.get(),
        EXTRA_FIELDS_UNIX.get(),
        TENANT_ID_UNIX.get(),
        COMPRESS_METHOD_UNIX.get(),
        USE_LOCAL_TIMESTAMP_UNIX.get(),
        USE_REMOTE_IP_UNIX.get(),
    ) {
        Ok(cfg) => Arc::new(cfg),
        Err(err) => {
            fatalf!("cannot parse configs for -syslog.listenAddr.unix={addr:?}: {err}");
            unreachable!()
        }
    };

    let (net, path) = get_unix_socket_network_and_path(addr);
    match net.as_str() {
        "unix" => run_unix_stream_listener(&path, &cfg, storage, stop),
        "unixgram" => run_unix_packet_listener(&path, &cfg, storage, stop),
        _ => {
            // Go passes laddr.Net to net.ListenUnix, which fails there.
            fatalf!(
                "cannot start Unix socket syslog server at {addr:?}: unsupported network {net:?}"
            );
        }
    }
}

#[cfg(not(unix))]
fn run_unix_listener<S: LogRowsStorage + 'static>(
    addr: &str,
    _arg_idx: usize,
    _storage: &Arc<S>,
    _stop: &Arc<StopSignal>,
) {
    // PORT NOTE: unix sockets only exist behind cfg(unix); Go's net.ListenUnix
    // fails at runtime on unsupported platforms as well.
    fatalf!("-syslog.listenAddr.unix={addr:?} is not supported on this platform");
}

#[cfg(unix)]
fn run_unix_stream_listener<S: LogRowsStorage + 'static>(
    path: &str,
    cfg: &Arc<Configs>,
    storage: &Arc<S>,
    stop: &Arc<StopSignal>,
) {
    let ln = match std::os::unix::net::UnixListener::bind(path) {
        Ok(ln) => Arc::new(ln),
        Err(err) => {
            fatalf!("cannot start Unix socket syslog server at {path:?}: {err}");
            unreachable!()
        }
    };

    let done = {
        let ln = Arc::clone(&ln);
        let cfg = Arc::clone(cfg);
        let storage = Arc::clone(storage);
        let stop = Arc::clone(stop);
        spawn_worker(format!("syslog-unix-serve-{path}"), move || {
            serve_stream_listener(&*ln, &cfg, &storage, &stop);
        })
    };

    infof!("started accepting syslog messages at {path:?}");
    stop.wait();
    // Wake the blocked accept() with a throwaway self-connect (Go closes the
    // listener instead).
    let _ = std::os::unix::net::UnixStream::connect(path);
    let _ = done.join();
    // PORT NOTE: Go's net package unlinks the socket file on Close; std's
    // UnixListener does not, so remove it explicitly.
    let _ = std::fs::remove_file(path);
    infof!("finished accepting syslog messages at -syslog.listenAddr.unix={path:?}");
}

#[cfg(unix)]
fn run_unix_packet_listener<S: LogRowsStorage + 'static>(
    path: &str,
    cfg: &Arc<Configs>,
    storage: &Arc<S>,
    stop: &Arc<StopSignal>,
) {
    let ln = match std::os::unix::net::UnixDatagram::bind(path) {
        Ok(ln) => ln,
        Err(err) => {
            fatalf!("cannot start Unix socket syslog server at {path:?}: {err}");
            unreachable!()
        }
    };

    let done = {
        let cfg = Arc::clone(cfg);
        let storage = Arc::clone(storage);
        let stop = Arc::clone(stop);
        spawn_worker(format!("syslog-unixgram-serve-{path}"), move || {
            serve_packet_listener(&ln, &cfg, &storage, &stop);
        })
    };

    infof!("started accepting syslog messages at {path:?}");
    stop.wait();
    let _ = done.join();
    // PORT NOTE: Go's net package unlinks the socket file on Close.
    let _ = std::fs::remove_file(path);
    infof!("finished accepting syslog messages at {path:?}");
}

/// An optional network such as unix or unixgram can be specified in front of
/// addr and followed by ':'.
#[cfg(unix)]
fn get_unix_socket_network_and_path(addr: &str) -> (String, String) {
    match addr.split_once(':') {
        None => ("unix".to_string(), addr.to_string()),
        Some((before, after)) => (before.to_string(), after.to_string()),
    }
}

// ---------------------------------------------------------------------------
// UDP listener
// ---------------------------------------------------------------------------

fn run_udp_listener<S: LogRowsStorage + 'static>(
    addr: &str,
    arg_idx: usize,
    storage: &Arc<S>,
    stop: &Arc<StopSignal>,
) {
    // PORT NOTE: Go picks "udp"/"udp4" via netutil.GetUDPNetwork()
    // (-enableTCP6); std's UdpSocket::bind derives the stack from the address.
    let ln = match UdpSocket::bind(normalize_listen_addr(addr)) {
        Ok(ln) => ln,
        Err(err) => {
            fatalf!("cannot start UDP syslog server at {addr:?}: {err}");
            unreachable!()
        }
    };

    let cfg = match get_configs(
        "udp",
        arg_idx,
        STREAM_FIELDS_UDP.get(),
        IGNORE_FIELDS_UDP.get(),
        DECOLORIZE_FIELDS_UDP.get(),
        EXTRA_FIELDS_UDP.get(),
        TENANT_ID_UDP.get(),
        COMPRESS_METHOD_UDP.get(),
        USE_LOCAL_TIMESTAMP_UDP.get(),
        USE_REMOTE_IP_UDP.get(),
    ) {
        Ok(cfg) => Arc::new(cfg),
        Err(err) => {
            fatalf!("cannot parse configs for -syslog.listenAddr.udp={addr:?}: {err}");
            unreachable!()
        }
    };

    let done = {
        let cfg = Arc::clone(&cfg);
        let storage = Arc::clone(storage);
        let stop = Arc::clone(stop);
        spawn_worker(format!("syslog-udp-serve-{arg_idx}"), move || {
            serve_packet_listener(&ln, &cfg, &storage, &stop);
        })
    };

    infof!("started accepting syslog messages at -syslog.listenAddr.udp={addr:?}");
    stop.wait();
    // PORT NOTE: Go unblocks the readers by closing the socket; the port's
    // readers poll with a read timeout instead (see PACKET_POLL).
    let _ = done.join();
    infof!("finished accepting syslog messages at -syslog.listenAddr.udp={addr:?}");
}

// ---------------------------------------------------------------------------
// TCP listener
// ---------------------------------------------------------------------------

fn run_tcp_listener<S: LogRowsStorage + 'static>(
    addr: &str,
    arg_idx: usize,
    storage: &Arc<S>,
    stop: &Arc<StopSignal>,
) {
    // Go: tlsEnable/tlsCertFile/tlsKeyFile are per-listener via
    // GetOptionalArg(argIdx); tlsMinVersion is a scalar flag and
    // tlsCipherSuites is passed whole (not indexed).
    let mut tls_config: Option<Arc<rustls::ServerConfig>> = None;
    if TLS_ENABLE.get().get_optional_arg(arg_idx) {
        let cert_file = TLS_CERT_FILE.get().get_optional_arg(arg_idx);
        let key_file = TLS_KEY_FILE.get().get_optional_arg(arg_idx);
        let tls_min_version = TLS_MIN_VERSION.get();
        let tls_cipher_suites: &[String] = TLS_CIPHER_SUITES.get();
        match get_server_tls_config(cert_file, key_file, tls_min_version, tls_cipher_suites) {
            Ok(tc) => tls_config = Some(tc),
            Err(err) => {
                fatalf!(
                    "cannot load TLS cert from -syslog.tlsCertFile={cert_file:?}, -syslog.tlsKeyFile={key_file:?}, \
                     -syslog.tlsMinVersion={tls_min_version:?}, -syslog.tlsCipherSuites={tls_cipher_suites:?}: {err}"
                );
                unreachable!()
            }
        }
    }
    // Go: netutil.NewTCPListener("syslog", addr, false, tlsConfig).
    let ln = match TcpListener::bind(normalize_listen_addr(addr)) {
        Ok(ln) => Arc::new(SyslogTcpListener { ln, tls_config }),
        Err(err) => {
            fatalf!("syslog: cannot start TCP listener at {addr}: {err}");
            unreachable!()
        }
    };

    let cfg = match get_configs(
        "tcp",
        arg_idx,
        STREAM_FIELDS_TCP.get(),
        IGNORE_FIELDS_TCP.get(),
        DECOLORIZE_FIELDS_TCP.get(),
        EXTRA_FIELDS_TCP.get(),
        TENANT_ID_TCP.get(),
        COMPRESS_METHOD_TCP.get(),
        USE_LOCAL_TIMESTAMP_TCP.get(),
        USE_REMOTE_IP_TCP.get(),
    ) {
        Ok(cfg) => Arc::new(cfg),
        Err(err) => {
            fatalf!("cannot parse configs for -syslog.listenAddr.tcp={addr:?}: {err}");
            unreachable!()
        }
    };

    let local_addr = ln.ln.local_addr().ok();
    let done = {
        let ln = Arc::clone(&ln);
        let cfg = Arc::clone(&cfg);
        let storage = Arc::clone(storage);
        let stop = Arc::clone(stop);
        spawn_worker(format!("syslog-tcp-serve-{arg_idx}"), move || {
            serve_stream_listener(&*ln, &cfg, &storage, &stop);
        })
    };

    infof!("started accepting syslog messages at -syslog.listenAddr.tcp={addr:?}");
    stop.wait();
    // Wake the blocked accept() with a throwaway self-connect (house shutdown
    // pattern; Go closes the listener instead). The wakeup arrives as plain
    // TCP even when TLS is enabled; that is fine because the TLS handshake is
    // deferred to the first read, so the accept loop hits the stop-check and
    // exits before any TLS work happens.
    if let Some(a) = local_addr {
        let _ = TcpStream::connect(a);
    }
    let _ = done.join();
    infof!("finished accepting syslog messages at -syslog.listenAddr.tcp={addr:?}");
}

// ---------------------------------------------------------------------------
// Packet (datagram) serving
// ---------------------------------------------------------------------------

fn serve_packet_listener<C: PacketConn, S: LogRowsStorage + 'static>(
    ln: &C,
    cfg: &Arc<Configs>,
    storage: &Arc<S>,
    stop: &Arc<StopSignal>,
) {
    let gomaxprocs = available_cpus().max(1);
    let local_addr = ln.local_addr_string();
    let mut handles = Vec::with_capacity(gomaxprocs);
    for i in 0..gomaxprocs {
        let sock = match ln.try_clone_pc() {
            Ok(s) => s,
            Err(err) => {
                errorf!(
                    "syslog: cannot clone {} socket at {local_addr:?}: {err}",
                    cfg.typ
                );
                continue;
            }
        };
        let cfg = Arc::clone(cfg);
        let storage = Arc::clone(storage);
        let stop = Arc::clone(stop);
        let local_addr = local_addr.clone();
        handles.push(spawn_worker(
            format!("syslog-{}-reader-{i}", cfg.typ),
            move || {
                packet_reader_loop(&sock, &cfg, &storage, &stop, &local_addr);
            },
        ));
    }
    for h in handles {
        let _ = h.join();
    }
}

fn packet_reader_loop<C: PacketConn, S: LogRowsStorage + 'static>(
    sock: &C,
    cfg: &Configs,
    storage: &Arc<S>,
    stop: &StopSignal,
    local_addr: &str,
) {
    // PORT NOTE: poll with a read timeout so the stop signal is observed;
    // Go unblocks ReadFrom by closing the socket instead.
    if let Err(err) = sock.set_read_timeout_pc(Some(PACKET_POLL)) {
        errorf!(
            "syslog: cannot set read timeout on {} socket at {local_addr:?}: {err}",
            cfg.typ
        );
        return;
    }
    let cp = get_common_params_for_syslog(
        cfg.tenant_id,
        cfg.stream_fields.clone(),
        cfg.ignore_fields.clone(),
        cfg.decolorize_fields.clone(),
        cfg.extra_fields.clone(),
    );
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        if stop.is_stopped() {
            break;
        }
        let (n, remote_addr) = match sock.recv_from_pc(&mut buf) {
            Ok(v) => v,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(err) => {
                if stop.is_stopped() {
                    break;
                }
                // udpErrorsTotal metric omitted (see module PORT NOTE).
                errorf!(
                    "syslog: cannot read {} data at {local_addr}: {err}",
                    cfg.typ
                );
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        // udpRequestsTotal metric omitted (see module PORT NOTE).

        let remote_ip = get_remote_ip(&remote_addr, cfg.use_remote_ip);

        let mut r: &[u8] = &buf[..n];
        if let Err(err) = process_stream(
            cfg.typ,
            &mut r,
            &cfg.compress_method,
            cfg.use_local_timestamp,
            &remote_ip,
            &cp,
            storage,
        ) {
            errorf!(
                "syslog: cannot process {} data from {remote_addr} at {local_addr}: {err}",
                cfg.typ
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Stream (connection) serving
// ---------------------------------------------------------------------------

fn serve_stream_listener<L: StreamListener, S: LogRowsStorage + 'static>(
    ln: &L,
    cfg: &Arc<Configs>,
    storage: &Arc<S>,
    stop: &Arc<StopSignal>,
) {
    // PORT NOTE: Go tracks live connections in ingestserver.ConnsMap so
    // MustStop can close in-flight connections; the port keeps a map of
    // connection clones and shuts them down once the accept loop exits.
    let cm: Arc<Mutex<HashMap<u64, L::Conn>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut next_conn_id: u64 = 0;
    let mut conn_handles: Vec<JoinHandle<()>> = Vec::new();

    let addr = ln.addr_string();
    loop {
        let c = match ln.accept_conn() {
            Ok(c) => c,
            Err(err) => {
                if stop.is_stopped() {
                    break;
                }
                // PORT NOTE: Go distinguishes temporary net errors (1s
                // backoff) from closed listeners (break) and unrecoverable
                // errors (Fatalf); std::net does not expose net.Error, so all
                // accept errors get the temporary-error treatment.
                errorf!(
                    "syslog: temporary error when listening for {} addr {addr:?}: {err}",
                    cfg.typ
                );
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        if stop.is_stopped() {
            // Mirrors Go's `if !cm.Add(c) { c.Close(); break }`: the shutdown
            // wakeup self-connect (or a connection racing the shutdown) lands
            // here.
            c.shutdown_conn();
            break;
        }

        let conn_id = next_conn_id;
        next_conn_id += 1;
        if let Ok(clone) = c.try_clone_conn() {
            cm.lock().unwrap().insert(conn_id, clone);
        }

        let cfg = Arc::clone(cfg);
        let storage = Arc::clone(storage);
        let cm = Arc::clone(&cm);
        let addr = addr.clone();
        conn_handles.push(spawn_worker(
            format!("syslog-{}-conn-{conn_id}", cfg.typ),
            move || {
                let mut c = c;
                let cp = get_common_params_for_syslog(
                    cfg.tenant_id,
                    cfg.stream_fields.clone(),
                    cfg.ignore_fields.clone(),
                    cfg.decolorize_fields.clone(),
                    cfg.extra_fields.clone(),
                );

                let remote_addr = c.remote_addr_string();
                let remote_ip = get_remote_ip(&remote_addr, cfg.use_remote_ip);
                if let Err(err) = process_stream(
                    cfg.typ,
                    &mut c,
                    &cfg.compress_method,
                    cfg.use_local_timestamp,
                    &remote_ip,
                    &cp,
                    &storage,
                ) {
                    errorf!("syslog: cannot process {} data at {addr:?}: {err}", cfg.typ);
                }

                cm.lock().unwrap().remove(&conn_id);
                c.shutdown_conn();
            },
        ));
    }

    // Go: cm.CloseAll(0).
    for (_, c) in cm.lock().unwrap().drain() {
        c.shutdown_conn();
    }
    for h in conn_handles {
        let _ = h.join();
    }
}

// ---------------------------------------------------------------------------
// net.Listener / net.Conn / net.PacketConn seams
// ---------------------------------------------------------------------------

/// Minimal port of the `net.Listener` surface the syslog server uses.
trait StreamListener: Send + Sync + 'static {
    type Conn: StreamConn;
    fn accept_conn(&self) -> io::Result<Self::Conn>;
    fn addr_string(&self) -> String;
}

/// Minimal port of the `net.Conn` surface the syslog server uses.
trait StreamConn: Read + Send + 'static {
    fn try_clone_conn(&self) -> io::Result<Self>
    where
        Self: Sized;
    fn shutdown_conn(&self);
    fn remote_addr_string(&self) -> String;
}

/// Port of `netutil.NewTCPListener("syslog", addr, false, tlsConfig)`: a TCP
/// listener that optionally wraps accepted connections in server-side TLS.
///
/// PORT NOTE: Go's `netutil.TCPListener.Accept` returns `tls.Server(conn,
/// tlsConfig)` when TLS is enabled, which defers the handshake to the first
/// `Read` inside the connection goroutine. The port mirrors this by returning
/// a [`SyslogTcpConn::TlsHandshake`] and completing the handshake (via
/// `tlsutil::server_accept`) on the first read in the per-connection worker.
struct SyslogTcpListener {
    ln: TcpListener,
    tls_config: Option<Arc<rustls::ServerConfig>>,
}

impl StreamListener for SyslogTcpListener {
    type Conn = SyslogTcpConn;
    fn accept_conn(&self) -> io::Result<SyslogTcpConn> {
        let (c, _) = self.ln.accept()?;
        Ok(match &self.tls_config {
            None => SyslogTcpConn::Plain(c),
            Some(cfg) => SyslogTcpConn::TlsHandshake(Some(c), Arc::clone(cfg)),
        })
    }
    fn addr_string(&self) -> String {
        self.ln
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_default()
    }
}

/// A connection accepted by [`SyslogTcpListener`]: plaintext, or TLS before /
/// after the deferred handshake (Go `tls.Conn` handshakes on first `Read`).
enum SyslogTcpConn {
    Plain(TcpStream),
    /// TLS connection whose handshake has not run yet. The socket is consumed
    /// by the handshake, hence the `Option`; `None` after a failed handshake.
    TlsHandshake(Option<TcpStream>, Arc<rustls::ServerConfig>),
    /// Boxed: a rustls stream is ~1KiB, dwarfing the other variants
    /// (clippy::large_enum_variant).
    Tls(Box<TlsServerStream>),
}

impl SyslogTcpConn {
    /// The underlying TCP socket (used for shutdown/peer-addr regardless of
    /// the TLS state), if it is still around.
    fn socket(&self) -> Option<&TcpStream> {
        match self {
            SyslogTcpConn::Plain(c) => Some(c),
            SyslogTcpConn::TlsHandshake(sock, _) => sock.as_ref(),
            SyslogTcpConn::Tls(c) => Some(&c.sock),
        }
    }
}

impl Read for SyslogTcpConn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if let SyslogTcpConn::TlsHandshake(sock, cfg) = self {
            // Complete the deferred handshake, like Go's tls.Conn.Read. A
            // handshake failure (e.g. a plaintext client hitting a TLS
            // listener) surfaces as a read error handled by the caller.
            let sock = sock
                .take()
                .ok_or_else(|| io::Error::other("TLS handshake already failed"))?;
            let tls = server_accept(cfg, sock).map_err(io::Error::other)?;
            *self = SyslogTcpConn::Tls(Box::new(tls));
        }
        match self {
            SyslogTcpConn::Plain(c) => c.read(buf),
            SyslogTcpConn::Tls(c) => c.read(buf),
            SyslogTcpConn::TlsHandshake(..) => unreachable!(),
        }
    }
}

impl StreamConn for SyslogTcpConn {
    fn try_clone_conn(&self) -> io::Result<Self> {
        // PORT NOTE: the clone only feeds the conn map for shutdown (Go
        // ingestserver.ConnsMap closes the net.Conn), so it clones the raw
        // socket as a Plain conn regardless of the TLS state.
        let sock = self.socket().ok_or_else(|| {
            io::Error::other("cannot clone connection after failed TLS handshake")
        })?;
        Ok(SyslogTcpConn::Plain(sock.try_clone()?))
    }
    fn shutdown_conn(&self) {
        // Go's cm.CloseAll(0)/c.Close() closes the socket without sending a
        // TLS close_notify; shutting down the raw socket matches that.
        if let Some(sock) = self.socket() {
            let _ = sock.shutdown(Shutdown::Both);
        }
    }
    fn remote_addr_string(&self) -> String {
        self.socket()
            .and_then(|s| s.peer_addr().ok())
            .map(|a| a.to_string())
            .unwrap_or_default()
    }
}

#[cfg(unix)]
impl StreamListener for std::os::unix::net::UnixListener {
    type Conn = std::os::unix::net::UnixStream;
    fn accept_conn(&self) -> io::Result<std::os::unix::net::UnixStream> {
        self.accept().map(|(c, _)| c)
    }
    fn addr_string(&self) -> String {
        self.local_addr()
            .ok()
            .and_then(|a| a.as_pathname().map(|p| p.display().to_string()))
            .unwrap_or_default()
    }
}

#[cfg(unix)]
impl StreamConn for std::os::unix::net::UnixStream {
    fn try_clone_conn(&self) -> io::Result<Self> {
        self.try_clone()
    }
    fn shutdown_conn(&self) {
        let _ = self.shutdown(Shutdown::Both);
    }
    fn remote_addr_string(&self) -> String {
        self.peer_addr()
            .ok()
            .and_then(|a| a.as_pathname().map(|p| p.display().to_string()))
            .unwrap_or_default()
    }
}

/// Minimal port of the `net.PacketConn` surface the syslog server uses.
trait PacketConn: Send + Sync + Sized + 'static {
    fn try_clone_pc(&self) -> io::Result<Self>;
    fn set_read_timeout_pc(&self, timeout: Option<Duration>) -> io::Result<()>;
    /// Receives one datagram, returning its length and the remote address
    /// formatted like Go's `net.Addr.String()`.
    fn recv_from_pc(&self, buf: &mut [u8]) -> io::Result<(usize, String)>;
    fn local_addr_string(&self) -> String;
}

impl PacketConn for UdpSocket {
    fn try_clone_pc(&self) -> io::Result<Self> {
        self.try_clone()
    }
    fn set_read_timeout_pc(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.set_read_timeout(timeout)
    }
    fn recv_from_pc(&self, buf: &mut [u8]) -> io::Result<(usize, String)> {
        self.recv_from(buf).map(|(n, a)| (n, a.to_string()))
    }
    fn local_addr_string(&self) -> String {
        self.local_addr().map(|a| a.to_string()).unwrap_or_default()
    }
}

#[cfg(unix)]
impl PacketConn for std::os::unix::net::UnixDatagram {
    fn try_clone_pc(&self) -> io::Result<Self> {
        self.try_clone()
    }
    fn set_read_timeout_pc(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.set_read_timeout(timeout)
    }
    fn recv_from_pc(&self, buf: &mut [u8]) -> io::Result<(usize, String)> {
        self.recv_from(buf).map(|(n, a)| {
            let addr = a
                .as_pathname()
                .map(|p| p.display().to_string())
                .unwrap_or_default();
            (n, addr)
        })
    }
    fn local_addr_string(&self) -> String {
        self.local_addr()
            .ok()
            .and_then(|a| a.as_pathname().map(|p| p.display().to_string()))
            .unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Stream processing
// ---------------------------------------------------------------------------

/// processStream parses a stream of syslog messages from r and ingests them
/// into the storage.
///
/// PORT NOTE: Go checks `insertutil.CanWriteData()` and creates the processor
/// via `cp.NewLogMessageProcessor("syslog_"+protocol, true)`, which starts a
/// periodic background flush for long-lived connections. The Rust
/// `common_params` port dropped both (see its module PORT NOTEs), so rows from
/// an idle long-lived connection become searchable when the in-memory buffer
/// fills or the connection closes. `_protocol` is kept for signature parity —
/// Go only uses it for the processor name and its metrics.
fn process_stream<S: LogRowsStorage + 'static>(
    _protocol: &str,
    r: &mut dyn Read,
    compress_method: &str,
    use_local_timestamp: bool,
    remote_ip: &str,
    cp: &CommonParams,
    storage: &Arc<S>,
) -> Result<(), String> {
    let mut lmp = cp.new_log_message_processor(storage);
    let res = process_stream_internal(r, compress_method, use_local_timestamp, remote_ip, &mut lmp);
    lmp.close();
    res
}

fn process_stream_internal<P: SyslogLogMessageProcessor>(
    r: &mut dyn Read,
    compress_method: &str,
    use_local_timestamp: bool,
    remote_ip: &str,
    lmp: &mut P,
) -> Result<(), String> {
    // PORT NOTE: Go wraps r with writeconcurrencylimiter.GetReader; the
    // concurrency limiter is not ported (ingestion parallelism is bounded by
    // the listener/reader thread counts instead).
    let mut reader = get_uncompressed_reader(r, compress_method)
        .map_err(|err| format!("cannot decode syslog data: {err}"))?;
    process_uncompressed_stream(&mut *reader, use_local_timestamp, remote_ip, lmp)
}

/// Mirrors `protoparserutil.GetUncompressedReader` for the syslog
/// `-syslog.compressMethod.*` values (validated in [`get_configs`]).
fn get_uncompressed_reader<'a>(
    r: &'a mut dyn Read,
    compress_method: &str,
) -> io::Result<Box<dyn Read + 'a>> {
    match compress_method {
        "" | "none" => Ok(Box::new(r)),
        "gzip" => Ok(Box::new(flate2::read::GzDecoder::new(r))),
        "deflate" => Ok(Box::new(flate2::read::ZlibDecoder::new(r))),
        "zstd" => Ok(Box::new(zstd::stream::read::Decoder::new(r)?)),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported compressMethod={other:?}"),
        )),
    }
}

fn process_uncompressed_stream<P: SyslogLogMessageProcessor>(
    r: &mut dyn Read,
    use_local_timestamp: bool,
    remote_ip: &str,
    lmp: &mut P,
) -> Result<(), String> {
    let mut slr = SyslogLineReader::new(r);

    let mut n: u64 = 0;
    while slr.next_line() {
        let current_year = GLOBAL_CURRENT_YEAR.load(Ordering::SeqCst);
        let timezone_offset_secs = GLOBAL_TIMEZONE_OFFSET_SECS.load(Ordering::SeqCst);
        let line = String::from_utf8_lossy(&slr.line);
        // errorsTotal metric omitted (see module PORT NOTE).
        process_line(
            &line,
            current_year,
            timezone_offset_secs,
            use_local_timestamp,
            remote_ip,
            lmp,
        )
        .map_err(|err| format!("cannot read line #{n}: {err}"))?;
        n += 1;
    }
    slr.error()
}

// ---------------------------------------------------------------------------
// syslogLineReader
// ---------------------------------------------------------------------------

enum SyslogLineReaderError {
    /// Clean end of stream (Go `io.EOF`), not reported by [`SyslogLineReader::error`].
    Eof,
    Other(String),
}

/// Port of Go `syslogLineReader`: splits a byte stream into syslog messages
/// framed with either the octet-counting method
/// (<https://www.ietf.org/archive/id/draft-gerhards-syslog-plain-tcp-07.html#msgxfer>)
/// or the octet-stuffing (non-transparent, newline-delimited) method
/// (<https://www.ietf.org/archive/id/draft-gerhards-syslog-plain-tcp-07.html#octet-stuffing-legacy>).
///
/// PORT NOTE: Go pools readers via `sync.Pool`
/// (`getSyslogLineReader`/`putSyslogLineReader`); the port allocates one per
/// stream instead.
struct SyslogLineReader<R: Read> {
    line: Vec<u8>,

    br: BufReader<R>,
    err: Option<SyslogLineReaderError>,
}

impl<R: Read> SyslogLineReader<R> {
    fn new(r: R) -> Self {
        SyslogLineReader {
            line: Vec::new(),
            br: BufReader::with_capacity(64 * 1024, r),
            err: None,
        }
    }

    /// Returns the last error occurred in the reader (a clean EOF is not an
    /// error, like Go `Error()`).
    fn error(&self) -> Result<(), String> {
        match &self.err {
            Some(SyslogLineReaderError::Other(msg)) => Err(msg.clone()),
            _ => Ok(()),
        }
    }

    /// Reads the next syslog line and stores it at `self.line`.
    ///
    /// false is returned if the next line cannot be read. [`Self::error`] must
    /// be called in this case in order to verify whether there is an error or
    /// the stream has just been finished.
    fn next_line(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }

        let mut prefix: Vec<u8> = Vec::new();
        // Go `again:` label — retry on empty prefixes.
        loop {
            prefix.clear();
            match self.br.read_until(b' ', &mut prefix) {
                Err(err) => {
                    self.err = Some(SyslogLineReaderError::Other(format!(
                        "cannot read message frame prefix: {err}"
                    )));
                    return false;
                }
                Ok(0) => {
                    self.err = Some(SyslogLineReaderError::Eof);
                    return false;
                }
                Ok(_) => {}
            }
            // skip empty lines
            let skip = prefix.iter().take_while(|&&b| b == b'\n').count();
            prefix.drain(..skip);
            if !prefix.is_empty() {
                break;
            }
            // An empty prefix or a prefix with empty lines - try reading yet
            // another prefix.
        }

        if prefix[0].is_ascii_digit() {
            // This is the octet-counting method.
            let msg_len_str = String::from_utf8_lossy(&prefix[..prefix.len() - 1]).into_owned();
            let msg_len = match msg_len_str.parse::<u64>() {
                Ok(n) => n,
                Err(err) => {
                    self.err = Some(SyslogLineReaderError::Other(format!(
                        "cannot parse message length from {msg_len_str:?}: {err}"
                    )));
                    return false;
                }
            };
            // Go: insertutil.MaxLineSizeBytes.IntN().
            let max_msg_len = MAX_LINE_SIZE_BYTES.get().int_n().max(0) as u64;
            if msg_len > max_msg_len {
                self.err = Some(SyslogLineReaderError::Other(format!(
                    "cannot read message longer than {max_msg_len} bytes; msgLen={msg_len}"
                )));
                return false;
            }
            self.line.clear();
            self.line.resize(msg_len as usize, 0);
            if let Err(err) = self.br.read_exact(&mut self.line) {
                self.err = Some(SyslogLineReaderError::Other(format!(
                    "cannot read message with size {msg_len} bytes: {err}"
                )));
                return false;
            }
            return true;
        }

        // This is the octet-stuffing method.
        self.line.clear();
        self.line.extend_from_slice(&prefix);
        match self.br.read_until(b'\n', &mut self.line) {
            Ok(n) => {
                // Go strips the trailing '\n' only when ReadSlice found the
                // delimiter; on io.EOF the partial tail is kept as-is.
                if n > 0 && self.line.last() == Some(&b'\n') {
                    self.line.pop();
                }
                true
            }
            Err(err) => {
                self.err = Some(SyslogLineReaderError::Other(format!(
                    "cannot read message in octet-stuffing method: {err}"
                )));
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Flag-value parsing helpers
// ---------------------------------------------------------------------------

/// Parses a JSON array of strings (Go `parseFieldsList`); returns `None` for
/// an empty/null input so callers can distinguish "unset" from `[]`.
fn parse_fields_list(s: &str) -> Result<Option<Vec<String>>, String> {
    if s.is_empty() {
        return Ok(None);
    }

    let mut p = JsonReader::new(s);
    p.skip_ws();
    if p.consume_literal("null") {
        p.skip_ws();
        p.expect_eof()?;
        return Ok(None);
    }
    p.expect(b'[')?;
    let mut a = Vec::new();
    p.skip_ws();
    if p.peek() == Some(b']') {
        p.advance();
        p.skip_ws();
        p.expect_eof()?;
        return Ok(Some(a));
    }
    loop {
        p.skip_ws();
        a.push(p.parse_string()?);
        p.skip_ws();
        match p.next_byte() {
            Some(b',') => {}
            Some(b']') => break,
            _ => return Err("missing ',' or ']' in JSON array".to_string()),
        }
    }
    p.skip_ws();
    p.expect_eof()?;
    Ok(Some(a))
}

fn get_remote_ip(remote_addr: &str, use_remote_ip: bool) -> String {
    if !use_remote_ip {
        return String::new();
    }
    match remote_addr.rfind(':') {
        Some(n) => remote_addr[..n].to_string(),
        None => String::new(),
    }
}

/// Parses a JSON object of string values into sorted fields
/// (Go `parseExtraFields`).
fn parse_extra_fields(s: &str) -> Result<Vec<Field>, String> {
    if s.is_empty() {
        return Ok(Vec::new());
    }

    let mut p = JsonReader::new(s);
    p.skip_ws();
    if p.consume_literal("null") {
        p.skip_ws();
        p.expect_eof()?;
        return Ok(Vec::new());
    }
    p.expect(b'{')?;
    let mut fields: Vec<Field> = Vec::new();
    p.skip_ws();
    if p.peek() == Some(b'}') {
        p.advance();
    } else {
        loop {
            p.skip_ws();
            let name = p.parse_string()?;
            p.skip_ws();
            p.expect(b':')?;
            p.skip_ws();
            let value = p.parse_string()?;
            // Go decodes into map[string]string: a duplicate key overwrites.
            if let Some(f) = fields.iter_mut().find(|f| f.name == name) {
                f.value = value;
            } else {
                fields.push(Field { name, value });
            }
            p.skip_ws();
            match p.next_byte() {
                Some(b',') => {}
                Some(b'}') => break,
                _ => return Err("missing ',' or '}' in JSON object".to_string()),
            }
        }
    }
    p.skip_ws();
    p.expect_eof()?;
    fields.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(fields)
}

/// Minimal strict JSON reader for the two shapes the `-syslog.*` flag values
/// use (`["a","b"]` and `{"k":"v"}`).
///
/// PORT NOTE: Go uses encoding/json; the port hand-rolls the parser following
/// the precedent of `tenant_id.rs` (serde is not a dependency).
struct JsonReader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> JsonReader<'a> {
    fn new(s: &'a str) -> Self {
        JsonReader {
            b: s.as_bytes(),
            pos: 0,
        }
    }

    fn skip_ws(&mut self) {
        while matches!(
            self.b.get(self.pos),
            Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r')
        ) {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.pos).copied()
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn next_byte(&mut self) -> Option<u8> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn expect(&mut self, ch: u8) -> Result<(), String> {
        match self.next_byte() {
            Some(c) if c == ch => Ok(()),
            _ => Err(format!("expected {:?}", ch as char)),
        }
    }

    fn expect_eof(&self) -> Result<(), String> {
        if self.pos == self.b.len() {
            Ok(())
        } else {
            Err("unexpected trailing data".to_string())
        }
    }

    fn consume_literal(&mut self, lit: &str) -> bool {
        if self.b[self.pos..].starts_with(lit.as_bytes()) {
            self.pos += lit.len();
            true
        } else {
            false
        }
    }

    fn parse_string(&mut self) -> Result<String, String> {
        if self.next_byte() != Some(b'"') {
            return Err("expected JSON string".to_string());
        }
        let mut out: Vec<u8> = Vec::new();
        loop {
            let Some(c) = self.next_byte() else {
                return Err("unterminated JSON string".to_string());
            };
            match c {
                b'"' => {
                    return String::from_utf8(out)
                        .map_err(|_| "invalid UTF-8 in JSON string".to_string());
                }
                b'\\' => {
                    let Some(e) = self.next_byte() else {
                        return Err("unterminated escape sequence".to_string());
                    };
                    let decoded: char = match e {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{0008}',
                        b'f' => '\u{000c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => {
                            let hi = self.parse_hex4()?;
                            if (0xD800..0xDC00).contains(&hi) {
                                // Surrogate pair.
                                if self.next_byte() != Some(b'\\') || self.next_byte() != Some(b'u')
                                {
                                    return Err("invalid surrogate pair".to_string());
                                }
                                let lo = self.parse_hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return Err("invalid surrogate pair".to_string());
                                }
                                let cp = 0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00);
                                char::from_u32(cp)
                                    .ok_or_else(|| "invalid code point".to_string())?
                            } else {
                                char::from_u32(hi)
                                    .ok_or_else(|| "invalid code point".to_string())?
                            }
                        }
                        _ => return Err(format!("invalid escape \\{}", e as char)),
                    };
                    let mut tmp = [0u8; 4];
                    out.extend_from_slice(decoded.encode_utf8(&mut tmp).as_bytes());
                }
                _ => out.push(c),
            }
        }
    }

    fn parse_hex4(&mut self) -> Result<u32, String> {
        if self.pos + 4 > self.b.len() {
            return Err("truncated \\u escape".to_string());
        }
        let hex = std::str::from_utf8(&self.b[self.pos..self.pos + 4])
            .map_err(|_| "invalid \\u escape".to_string())?;
        let v = u32::from_str_radix(hex, 16).map_err(|_| "invalid \\u escape".to_string())?;
        self.pos += 4;
        Ok(v)
    }
}

// ---------------------------------------------------------------------------
// Per-listener configs
// ---------------------------------------------------------------------------

struct Configs {
    typ: &'static str,

    stream_fields: Option<Vec<String>>,
    ignore_fields: Vec<String>,
    decolorize_fields: Vec<String>,
    extra_fields: Vec<Field>,
    tenant_id: TenantID,
    compress_method: String,
    use_local_timestamp: bool,
    use_remote_ip: bool,
}

// Mirrors Go `getConfigs`, which takes the per-protocol flag set explicitly.
#[allow(clippy::too_many_arguments)]
fn get_configs(
    typ: &'static str,
    arg_idx: usize,
    stream_fields_arg: &ArrayString,
    ignore_fields_arg: &ArrayString,
    decolorize_fields_arg: &ArrayString,
    extra_fields_arg: &ArrayString,
    tenant_id_arg: &ArrayString,
    compress_method_arg: &ArrayString,
    use_local_timestamp_arg: &ArrayBool,
    use_remote_ip_arg: &ArrayBool,
) -> Result<Configs, String> {
    let stream_fields_str = stream_fields_arg.get_optional_arg(arg_idx);
    let stream_fields = parse_fields_list(stream_fields_str).map_err(|err| {
        format!("cannot parse -syslog.streamFields.{typ}={stream_fields_str:?}: {err}")
    })?;
    if let Some(sf) = &stream_fields {
        let refs: Vec<&str> = sf.iter().map(String::as_str).collect();
        check_stream_field_names(&refs).map_err(|err| {
            format!(
                "invalid stream field names inside -syslog.streamFields.{typ}={stream_fields_str:?}: {err}"
            )
        })?;
    }

    let ignore_fields_str = ignore_fields_arg.get_optional_arg(arg_idx);
    let ignore_fields = parse_fields_list(ignore_fields_str)
        .map_err(|err| {
            format!("cannot parse -syslog.ignoreFields.{typ}={ignore_fields_str:?}: {err}")
        })?
        .unwrap_or_default();

    let decolorize_fields_str = decolorize_fields_arg.get_optional_arg(arg_idx);
    let decolorize_fields = parse_fields_list(decolorize_fields_str)
        .map_err(|err| {
            format!("cannot parse -syslog.decolorizeFields.{typ}={decolorize_fields_str:?}: {err}")
        })?
        .unwrap_or_default();

    let extra_fields_str = extra_fields_arg.get_optional_arg(arg_idx);
    let extra_fields = parse_extra_fields(extra_fields_str).map_err(|err| {
        format!("cannot parse -syslog.extraFields.{typ}={extra_fields_str:?}: {err}")
    })?;

    let tenant_id_str = tenant_id_arg.get_optional_arg(arg_idx);
    let tenant_id = parse_tenant_id(tenant_id_str)
        .map_err(|err| format!("cannot parse -syslog.tenantID.{typ}={tenant_id_str:?}: {err}"))?;

    let compress_method = compress_method_arg.get_optional_arg(arg_idx);
    match compress_method {
        "" | "none" | "zstd" | "gzip" | "deflate" => {
            // These methods are supported
        }
        _ => {
            return Err(format!(
                "unsupported -syslog.compressMethod.{typ}={compress_method:?}; supported values: 'none', 'zstd', 'gzip', 'deflate'"
            ));
        }
    }

    let use_local_timestamp = use_local_timestamp_arg.get_optional_arg(arg_idx);
    let use_remote_ip = use_remote_ip_arg.get_optional_arg(arg_idx);

    Ok(Configs {
        typ,
        stream_fields,
        ignore_fields,
        decolorize_fields,
        extra_fields,
        tenant_id,
        compress_method: compress_method.to_string(),
        use_local_timestamp,
        use_remote_ip,
    })
}

// ---------------------------------------------------------------------------
// Misc helpers
// ---------------------------------------------------------------------------

/// Go's net convention: an address of the form ":514" means "all interfaces on
/// that port" (same normalization as `httpserver::serve`).
fn normalize_listen_addr(addr: &str) -> String {
    if addr.is_empty() {
        "0.0.0.0:0".to_string()
    } else if let Some(port) = addr.strip_prefix(':') {
        format!("0.0.0.0:{port}")
    } else {
        addr.to_string()
    }
}

/// Resolves `-syslog.timezone` to a fixed UTC offset in seconds.
///
/// PORT NOTE: Go resolves any IANA name via `time.LoadLocation`; std Rust has
/// no timezone database, so the port supports "Local" (the default), "UTC",
/// "Etc/GMT±N" (POSIX-inverted sign, like IANA) and fixed "±HH:MM" offsets.
/// Anything else is a fatal startup error.
fn parse_timezone_offset_secs(tz: &str) -> Result<i64, String> {
    if tz.is_empty() || tz == "Local" {
        return Ok(get_local_timezone_offset_nsecs() / 1_000_000_000);
    }
    if tz == "UTC" || tz == "Etc/UTC" || tz == "Etc/GMT" {
        return Ok(0);
    }
    if let Some(rest) = tz.strip_prefix("Etc/GMT") {
        // IANA Etc/GMT+3 is UTC-3 (POSIX-inverted sign).
        if let Ok(hours) = rest.parse::<i64>()
            && (-14..=12).contains(&hours)
        {
            return Ok(-hours * 3600);
        }
        return Err(format!("unsupported Etc/GMT offset {rest:?}"));
    }
    let b = tz.as_bytes();
    if b.len() == 6 && (b[0] == b'+' || b[0] == b'-') && b[3] == b':' {
        let sign: i64 = if b[0] == b'-' { -1 } else { 1 };
        let hh: i64 = tz[1..3]
            .parse()
            .map_err(|_| format!("invalid fixed offset {tz:?}"))?;
        let mm: i64 = tz[4..6]
            .parse()
            .map_err(|_| format!("invalid fixed offset {tz:?}"))?;
        if hh > 23 || mm > 59 {
            return Err(format!("invalid fixed offset {tz:?}"));
        }
        return Ok(sign * (hh * 3600 + mm * 60));
    }
    Err(
        "unknown time zone; this port has no IANA timezone database, supported values: \
         Local, UTC, Etc/GMT±N and fixed ±HH:MM offsets"
            .to_string(),
    )
}

/// The current year in the local timezone (Go `time.Now().Year()`).
fn current_year_local() -> i64 {
    let secs = now_unix_nanos() / 1_000_000_000 + get_local_timezone_offset_nsecs() / 1_000_000_000;
    year_from_unix_days(secs.div_euclid(86_400))
}

/// Returns the civil year containing the given number of days since the unix
/// epoch (civil-from-days, Howard Hinnant's algorithm).
fn year_from_unix_days(days: i64) -> i64 {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 400; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    if m <= 2 { y + 1 } else { y }
}

// ---------------------------------------------------------------------------
// Stop signal (house shutdown pattern, see module doc)
// ---------------------------------------------------------------------------

/// Broadcast one-shot stop signal: a flag for hot-path polling plus a condvar
/// for timed/blocking waits. Stands in for Go's `close(workersStopCh)`.
struct StopSignal {
    stopped: Mutex<bool>,
    cv: Condvar,
}

impl StopSignal {
    fn new() -> Self {
        StopSignal {
            stopped: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    fn stop(&self) {
        let mut stopped = self.stopped.lock().unwrap();
        *stopped = true;
        self.cv.notify_all();
    }

    fn is_stopped(&self) -> bool {
        *self.stopped.lock().unwrap()
    }

    /// Blocks until [`StopSignal::stop`] is called.
    fn wait(&self) {
        let mut stopped = self.stopped.lock().unwrap();
        while !*stopped {
            stopped = self.cv.wait(stopped).unwrap();
        }
    }

    /// Waits up to `timeout` for the stop signal; returns true when stopped.
    fn wait_timeout(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut stopped = self.stopped.lock().unwrap();
        while !*stopped {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, _) = self.cv.wait_timeout(stopped, deadline - now).unwrap();
            stopped = guard;
        }
        true
    }
}

// ---------------------------------------------------------------------------
// Tests (port of app/eslinsert/syslog/syslog_test.go)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use esl_common::tlsutil;
    use esl_logstorage::rows::marshal_fields_to_json;
    use esl_logstorage::storage::Storage;

    use crate::testutil::{open_temp_storage, rows_count};

    /// Serializes tests that touch the module globals or the
    /// must_init/must_stop state (Go runs package tests sequentially; Rust
    /// runs `#[test]`s in parallel).
    static TEST_MUTEX: Mutex<()> = Mutex::new(());

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_MUTEX
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Port of `insertutil.TestLogMessageProcessor`.
    #[derive(Default)]
    struct TestLogMessageProcessor {
        timestamps: Vec<i64>,
        rows: Vec<String>,
    }

    impl SyslogLogMessageProcessor for TestLogMessageProcessor {
        fn add_row(&mut self, timestamp: i64, fields: &mut [Field], stream_fields_len: isize) {
            assert!(
                stream_fields_len < 0,
                "BUG: streamFieldsLen must be negative; got {stream_fields_len}"
            );
            self.timestamps.push(timestamp);
            let mut buf = Vec::new();
            marshal_fields_to_json(&mut buf, fields);
            self.rows.push(String::from_utf8(buf).unwrap());
        }
    }

    impl TestLogMessageProcessor {
        /// Verifies the number of rows, timestamps and results after add_row
        /// calls.
        fn verify(&self, timestamps_expected: &[i64], result_expected: &str) -> Result<(), String> {
            let result = self.rows.join("\n");
            if self.rows.len() != timestamps_expected.len() {
                return Err(format!(
                    "unexpected rows read; got {}; want {};\nrows read:\n{}\nrows wanted\n{}",
                    self.rows.len(),
                    timestamps_expected.len(),
                    result,
                    result_expected
                ));
            }
            if self.timestamps != timestamps_expected {
                return Err(format!(
                    "unexpected timestamps;\ngot\n{:?}\nwant\n{:?}",
                    self.timestamps, timestamps_expected
                ));
            }
            if result != result_expected {
                return Err(format!(
                    "unexpected result;\ngot\n{result}\nwant\n{result_expected}"
                ));
            }
            Ok(())
        }
    }

    #[test]
    fn test_syslog_line_reader_success() {
        fn f(data: &str, lines_expected: &[&str]) {
            let mut r = data.as_bytes();
            let mut slr = SyslogLineReader::new(&mut r);

            let mut lines: Vec<String> = Vec::new();
            while slr.next_line() {
                lines.push(String::from_utf8(slr.line.clone()).unwrap());
            }
            if let Err(err) = slr.error() {
                panic!("unexpected error: {err}");
            }
            assert_eq!(
                lines, lines_expected,
                "unexpected lines read;\ngot\n{lines:?}\nwant\n{lines_expected:?}"
            );
        }

        f("", &[]);
        f("\n", &[]);
        f("\n\n\n", &[]);

        f("foobar", &["foobar"]);
        f("foobar\n", &["foobar\n"]);
        f("\n\nfoo\n\nbar\n\n", &["foo\n\nbar\n\n"]);

        f(
            "Jun  3 12:08:33 abcd systemd: Starting Update the local ESM caches...",
            &["Jun  3 12:08:33 abcd systemd: Starting Update the local ESM caches..."],
        );

        f(
            "Jun  3 12:08:33 abcd systemd: Starting Update the local ESM caches...\n\
             \n\
             48 <165>Jun  4 12:08:33 abcd systemd[345]: abc defg<123>1 2023-06-03T17:42:12.345Z mymachine.example.com appname 12345 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application 123 = ] 56\" eventID=\"11211\"] This is a test message with structured data.\n\
             \n",
            &[
                "Jun  3 12:08:33 abcd systemd: Starting Update the local ESM caches...",
                "<165>Jun  4 12:08:33 abcd systemd[345]: abc defg",
                r#"<123>1 2023-06-03T17:42:12.345Z mymachine.example.com appname 12345 ID47 [exampleSDID@32473 iut="3" eventSource="Application 123 = ] 56" eventID="11211"] This is a test message with structured data."#,
            ],
        );
    }

    #[test]
    fn test_syslog_line_reader_failure() {
        fn f(data: &str) {
            let mut r = data.as_bytes();
            let mut slr = SyslogLineReader::new(&mut r);

            assert!(!slr.next_line(), "expecting failure to read the first line");
            assert!(slr.error().is_err(), "expecting non-nil error");
        }

        // invalid format for message size
        f("12foo bar");

        // too big message size
        f("123 aa");
        f("1233423432 abc");
    }

    #[test]
    fn test_process_stream_internal_success() {
        let _guard = test_lock();

        fn f<S: LogRowsStorage + 'static>(
            storage: &Arc<S>,
            data: &str,
            current_year: i64,
            timestamps_expected: &[i64],
            result_expected: &str,
        ) {
            must_init(storage);

            // Go sets `globalTimezone = time.UTC`; the port stores a fixed
            // UTC offset instead.
            GLOBAL_TIMEZONE_OFFSET_SECS.store(0, Ordering::SeqCst);
            GLOBAL_CURRENT_YEAR.store(current_year, Ordering::SeqCst);

            let mut tlp = TestLogMessageProcessor::default();
            let mut r = data.as_bytes();
            let res = process_stream_internal(&mut r, "", false, "1.2.3.4", &mut tlp);
            must_stop();
            if let Err(err) = res {
                panic!("unexpected error: {err}");
            }
            if let Err(err) = tlp.verify(timestamps_expected, result_expected) {
                panic!("{err}");
            }
        }

        let s = open_temp_storage("syslog-listeners-psi-ok");

        let data = "Jun  3 12:08:33 abcd systemd: Starting Update the local ESM caches...\n\
                    \n\
                    Sep 19 08:26:10 host CEF:0|Security|threatmanager|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232\n\
                    48 <165>Jun  4 12:08:33 abcd systemd[345]: abc defg<123>1 2023-06-03T17:42:12.345Z mymachine.example.com appname 12345 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application 123 = ] 56\" eventID=\"11211\"] This is a test message with structured data.\n";
        let current_year = 2023;
        let timestamps_expected: &[i64] = &[
            1685794113000000000,
            1695111970000000000,
            1685880513000000000,
            1685814132345000000,
        ];
        let result_expected = concat!(
            r#"{"format":"rfc3164","hostname":"abcd","app_name":"systemd","_msg":"Starting Update the local ESM caches...","remote_ip":"1.2.3.4"}"#,
            "\n",
            r#"{"format":"rfc3164","hostname":"host","app_name":"CEF","cef.version":"0","cef.device_vendor":"Security","cef.device_product":"threatmanager","cef.device_version":"1.0","cef.device_event_class_id":"100","cef.name":"worm successfully stopped","cef.severity":"10","cef.extension.src":"10.0.0.1","cef.extension.dst":"2.1.2.2","cef.extension.spt":"1232","remote_ip":"1.2.3.4"}"#,
            "\n",
            r#"{"priority":"165","facility_keyword":"local4","level":"notice","facility":"20","severity":"5","format":"rfc3164","hostname":"abcd","app_name":"systemd","proc_id":"345","_msg":"abc defg","remote_ip":"1.2.3.4"}"#,
            "\n",
            r#"{"priority":"123","facility_keyword":"solaris-cron","level":"error","facility":"15","severity":"3","format":"rfc5424","hostname":"mymachine.example.com","app_name":"appname","proc_id":"12345","msg_id":"ID47","exampleSDID@32473.iut":"3","exampleSDID@32473.eventSource":"Application 123 = ] 56","exampleSDID@32473.eventID":"11211","_msg":"This is a test message with structured data.","remote_ip":"1.2.3.4"}"#,
        );
        f(&s, data, current_year, timestamps_expected, result_expected);

        s.must_close();
    }

    #[test]
    fn test_process_stream_internal_failure() {
        let _guard = test_lock();

        fn f<S: LogRowsStorage + 'static>(storage: &Arc<S>, data: &str) {
            must_init(storage);

            let mut tlp = TestLogMessageProcessor::default();
            let mut r = data.as_bytes();
            let res = process_stream_internal(&mut r, "", false, "1.2.3.4", &mut tlp);
            must_stop();
            assert!(res.is_err(), "expecting non-nil error");
        }

        let s = open_temp_storage("syslog-listeners-psi-fail");

        // invalid format for message size
        f(&s, "12foo bar");

        // too big message size
        f(&s, "123 foo");
        f(&s, "123456789 bar");

        s.must_close();
    }

    // -- Listener integration tests -----------------------------------------
    //
    // PORT NOTE: no upstream equivalents — the Go tests never exercise the
    // network listeners. These validate the ported threading/framing/shutdown
    // over real loopback sockets.

    fn test_configs(typ: &'static str) -> Configs {
        Configs {
            typ,
            stream_fields: None,
            ignore_fields: Vec::new(),
            decolorize_fields: Vec::new(),
            extra_fields: Vec::new(),
            tenant_id: TenantID::default(),
            compress_method: String::new(),
            // Ingest with the current time so the temp storage's default
            // retention doesn't drop the fixed test timestamps.
            use_local_timestamp: true,
            use_remote_ip: false,
        }
    }

    fn wait_for_rows(s: &Arc<Storage>, want: u64) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while rows_count(s) < want {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {want} ingested rows; got {}",
                rows_count(s)
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn test_serve_stream_listener_tcp_roundtrip() {
        let _guard = test_lock();
        let s = open_temp_storage("syslog-listeners-tcp");
        GLOBAL_TIMEZONE_OFFSET_SECS.store(0, Ordering::SeqCst);
        GLOBAL_CURRENT_YEAR.store(2023, Ordering::SeqCst);

        let ln = Arc::new(SyslogTcpListener {
            ln: TcpListener::bind("127.0.0.1:0").unwrap(),
            tls_config: None,
        });
        let addr = ln.ln.local_addr().unwrap();
        let cfg = Arc::new(test_configs("tcp"));
        let stop = Arc::new(StopSignal::new());

        let done = {
            let ln = Arc::clone(&ln);
            let cfg = Arc::clone(&cfg);
            let storage = Arc::clone(&s);
            let stop = Arc::clone(&stop);
            thread::spawn(move || serve_stream_listener(&*ln, &cfg, &storage, &stop))
        };

        {
            // One octet-stuffed RFC5424 message and one RFC3164 message; the
            // connection drop flushes them via lmp.close().
            let mut c = TcpStream::connect(addr).unwrap();
            c.write_all(
                b"<165>1 2023-06-03T17:42:12.345Z host app 123 - - hello\n\
                  Jun  3 12:08:33 abcd systemd: starting\n",
            )
            .unwrap();
        }

        wait_for_rows(&s, 2);

        stop.stop();
        // Wake the blocked accept, mirroring run_tcp_listener's shutdown.
        let _ = TcpStream::connect(addr);
        done.join().unwrap();

        assert_eq!(rows_count(&s), 2);
        s.must_close();
    }

    // PORT NOTE: no upstream equivalent (the Go tests never exercise TLS);
    // validates the ported netutil.TCPListener TLS wrapping end to end:
    // deferred handshake, framing over TLS, plaintext-client handshake
    // failure not killing the accept loop, and the plain-TCP shutdown wakeup.
    //
    // The companion startup-error case ("-syslog.tls without
    // -syslog.tlsCertFile/-syslog.tlsKeyFile is fatal") is not testable
    // in-process: fatalf! calls std::process::exit. The underlying error is
    // covered by esl-common's tlsutil tests for get_server_tls_config.
    #[test]
    fn test_serve_stream_listener_tls_roundtrip() {
        let _guard = test_lock();
        let s = open_temp_storage("syslog-listeners-tls");
        GLOBAL_TIMEZONE_OFFSET_SECS.store(0, Ordering::SeqCst);
        GLOBAL_CURRENT_YEAR.store(2023, Ordering::SeqCst);

        // Self-signed server cert on disk, standing in for the per-listener
        // -syslog.tlsCertFile/-syslog.tlsKeyFile values.
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
            .unwrap();
        let dir =
            std::env::temp_dir().join(format!("esl-insert-test-syslog-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_file = dir.join("cert.pem");
        let key_file = dir.join("key.pem");
        std::fs::write(&cert_file, ck.cert.pem()).unwrap();
        std::fs::write(&key_file, ck.key_pair.serialize_pem()).unwrap();

        let tls_config = get_server_tls_config(
            cert_file.to_str().unwrap(),
            key_file.to_str().unwrap(),
            "TLS13",
            &[],
        )
        .unwrap();

        let ln = Arc::new(SyslogTcpListener {
            ln: TcpListener::bind("127.0.0.1:0").unwrap(),
            tls_config: Some(tls_config),
        });
        let addr = ln.ln.local_addr().unwrap();
        let cfg = Arc::new(test_configs("tcp"));
        let stop = Arc::new(StopSignal::new());

        let done = {
            let ln = Arc::clone(&ln);
            let cfg = Arc::clone(&cfg);
            let storage = Arc::clone(&s);
            let stop = Arc::clone(&stop);
            thread::spawn(move || serve_stream_listener(&*ln, &cfg, &storage, &stop))
        };

        {
            // A plaintext client must fail the deferred handshake inside the
            // connection worker without killing the accept loop; the TLS
            // client below proves the listener is still serving.
            let mut c = TcpStream::connect(addr).unwrap();
            let _ = c.write_all(b"not a tls client hello\n");
        }

        {
            // Same payloads as the plain-TCP roundtrip, over TLS.
            let client_cfg = tlsutil::new_tls_client_config(&tlsutil::TLSConfig {
                ca_file: cert_file.to_str().unwrap().to_string(),
                ..Default::default()
            })
            .unwrap();
            let tcp = TcpStream::connect(addr).unwrap();
            let mut c = tlsutil::client_connect(&client_cfg, "localhost", tcp).unwrap();
            c.write_all(
                b"<165>1 2023-06-03T17:42:12.345Z host app 123 - - hello\n\
                  Jun  3 12:08:33 abcd systemd: starting\n",
            )
            .unwrap();
            // Send close_notify so the server sees a clean EOF, and flush it
            // before shutting down the write half (close_notify is buffered).
            c.conn.send_close_notify();
            c.flush().unwrap();
            let _ = c.sock.shutdown(Shutdown::Write);
            // Keep the client socket alive until the rows are ingested:
            // dropping it here would close the fd, and on Windows the
            // resulting RST discards data still queued in the server's
            // receive buffer (Linux delivers queued data before the reset),
            // making the worker lose the not-yet-read frames.
            wait_for_rows(&s, 2);
        }

        stop.stop();
        // The shutdown wakeup arrives as plain TCP; the deferred handshake
        // keeps the accept loop's stop-check reachable (no handshake runs in
        // the accept loop).
        let _ = TcpStream::connect(addr);
        done.join().unwrap();

        assert_eq!(rows_count(&s), 2);
        s.must_close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_serve_packet_listener_udp_roundtrip() {
        let _guard = test_lock();
        let s = open_temp_storage("syslog-listeners-udp");
        GLOBAL_TIMEZONE_OFFSET_SECS.store(0, Ordering::SeqCst);
        GLOBAL_CURRENT_YEAR.store(2023, Ordering::SeqCst);

        let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = sock.local_addr().unwrap();
        let cfg = Arc::new(test_configs("udp"));
        let stop = Arc::new(StopSignal::new());

        let done = {
            let cfg = Arc::clone(&cfg);
            let storage = Arc::clone(&s);
            let stop = Arc::clone(&stop);
            thread::spawn(move || serve_packet_listener(&sock, &cfg, &storage, &stop))
        };

        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        client
            .send_to(
                b"<165>1 2023-06-03T17:42:12.345Z host app 123 - - hello",
                addr,
            )
            .unwrap();

        wait_for_rows(&s, 1);

        stop.stop();
        done.join().unwrap();

        assert_eq!(rows_count(&s), 1);
        s.must_close();
    }
}

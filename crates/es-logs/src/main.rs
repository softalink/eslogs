//! Rust port of the EsLogs single-node server binary
//! (`app/es-logs/main.go` in the Go source).
//!
//! This is the integration HUB: it opens the local storage and wires the
//! ingestion (`esl_insert`), query (`esl_select`) and internal storage
//! (`esl_storage`) HTTP handlers behind a single router.

use std::sync::Arc;
use std::time::Instant;

// Allocation-heavy workload; a better allocator than the platform default beats
// it on CPU/latency/RSS. jemalloc on unix (aggressive decay keeps RSS low via
// MALLOC_CONF); mimalloc on Windows (jemalloc doesn't build on MSVC).
#[cfg(unix)]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(unix)]
#[unsafe(no_mangle)]
pub static malloc_conf: &[u8] = b"background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0\0";

#[cfg(windows)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use esl_common::flagutil::{ArrayString, Flag};
use esl_common::httpserver::{Request, ResponseWriter};
use esl_common::{
    buildinfo, envflag, fatalf, flagutil, httpserver, infof, logger, procutil, pushmetrics,
};

use esl_logstorage::storage::Storage;

// `-httpListenAddr` is an `ArrayString` (Go), so several listeners can be
// started, each with its own indexed `-tls*` and
// `-httpListenAddr.useProxyProtocol` config (via `httpserver::serve_listener`).
static HTTP_LISTEN_ADDR: Flag<ArrayString> = Flag::new(
    "httpListenAddr",
    "TCP address to listen for incoming http requests. See also -httpListenAddr.useProxyProtocol",
    // No baked-in default (Go's flagutil.NewArrayString has none): the flag is
    // empty unless set, and `:9428` is applied as a fallback below. Baking
    // `:9428` into the default here would make it the base that CLI-provided
    // addresses append to (a double-bind on :9428).
    ArrayString::default,
);
esl_common::register_flag!(HTTP_LISTEN_ADDR);

// PORT NOTE: Windows' default system timer resolution is ~15.6ms, which
// quantizes thread wakeups (mpsc handoff to HTTP workers, condvar waits in the
// merge/flush threads) and inflated small-query latency and CPU. Go's runtime
// raises the resolution on Windows; mirror that with timeBeginPeriod(1) so
// wakeups are ~1ms-grained. No-op on unix.
#[cfg(windows)]
fn raise_timer_resolution() {
    #[link(name = "winmm")]
    unsafe extern "system" {
        fn timeBeginPeriod(uperiod: u32) -> u32;
    }
    // SAFETY: timeBeginPeriod is a simple stateless winmm call; 1ms is a valid
    // period. The matching timeEndPeriod is omitted — the process wants the
    // higher resolution for its whole lifetime.
    unsafe {
        timeBeginPeriod(1);
    }
}

#[cfg(not(windows))]
fn raise_timer_resolution() {}

fn main() {
    raise_timer_resolution();

    // Test/PGO aid: shut down gracefully after N seconds so main returns and
    // atexit hooks run (a forced kill loses PGO profile data; process::exit
    // on Windows is ExitProcess, which also skips them). No effect unless
    // the env var is set.
    if let Ok(s) = std::env::var("ESL_EXIT_AFTER_SECS")
        && let Ok(secs) = s.parse::<u64>()
    {
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(secs));
            procutil::self_sigterm();
        });
    }

    // Install signal handlers before any long-running setup so a SIGHUP
    // delivered during startup (e.g. when the launching shell exits after
    // backgrounding the server) does not terminate the process via the default
    // disposition. See procutil::init.
    procutil::init();

    // Parse flags (envflag first, mirroring Go `envflag.Parse()` which wraps
    // the std flag package and adds environment-variable expansion).
    envflag::parse();
    buildinfo::init();
    // Go's flag.Parse prints the usage banner (version-prefixed via buildinfo)
    // and exits for -h/-help; do the same before any further startup.
    if flagutil::help_requested() {
        usage();
        std::process::exit(0);
    }
    // Go registers `-pushmetrics.url` as secret in package init(), before
    // logger.Init logs the flags; mirror that ordering here.
    pushmetrics::init_secret_flags();
    logger::init();

    // Go `main.run`: fall back to `:9428` only when no address is configured.
    let mut listen_addrs: Vec<String> = HTTP_LISTEN_ADDR.get().0.clone();
    if listen_addrs.is_empty() {
        listen_addrs = vec![":9428".to_string()];
    }
    infof!("starting EsLogs at {listen_addrs:?}...");
    let start_time = Instant::now();

    let storage = esl_storage::init();
    // Register the storage metrics set for /metrics (Go does this inside
    // vlstorage.Init).
    esl_storage::init_storage_metrics(&storage);

    // Go eslinsert.Init(): start the syslog TCP/UDP/unix listeners (no-op
    // unless -syslog.listenAddr.* flags are set).
    esl_insert::syslog_listeners::must_init(&storage);

    // Start one HTTP server per `-httpListenAddr`; each router closure owns a
    // clone of the storage `Arc` (`Send + Sync + 'static`, as `serve` requires)
    // and each listener reads its own indexed `-tls*` config.
    let mut handles: Vec<httpserver::ServerHandle> = Vec::with_capacity(listen_addrs.len());
    for (i, addr) in listen_addrs.iter().enumerate() {
        let router_storage = Arc::clone(&storage);
        let handle = match httpserver::serve_listener(addr, i, move |req, w| {
            request_handler(&router_storage, req, w);
        }) {
            Ok(h) => h,
            Err(err) => {
                fatalf!("cannot start the http server at {addr:?}: {err}");
                unreachable!()
            }
        };
        handles.push(handle);
    }

    infof!(
        "started EsLogs in {:.3} seconds; see https://docs.victoriametrics.com/victorialogs/",
        start_time.elapsed().as_secs_f64()
    );

    pushmetrics::init();
    // Block until SIGTERM/SIGINT, then shut down gracefully.
    let sig = procutil::wait_for_sigterm();
    infof!("received signal {sig}");
    pushmetrics::stop();

    infof!("gracefully shutting down webservice at {listen_addrs:?}");
    let start_time = Instant::now();
    for handle in handles {
        handle.stop();
    }
    infof!(
        "successfully shut down the webservice in {:.3} seconds",
        start_time.elapsed().as_secs_f64()
    );

    // Go eslinsert.Stop(): stop the syslog listeners before closing storage.
    esl_insert::syslog_listeners::must_stop();

    storage.must_close();
    infof!("the EsLogs has been stopped");
}

/// Dispatches an incoming request across the ingestion, query and internal
/// storage handlers, mirroring Go `main.requestHandler`.
///
/// Common endpoints (`/health`, `/metrics`, `/flags`, `/ping`, `/-/ready`,
/// `/-/healthy`, favicon, robots) are served by `httpserver`'s built-in routes
/// before this handler is invoked.
fn request_handler(storage: &Arc<Storage>, req: &mut Request, w: &mut ResponseWriter) {
    let path = req.path().to_string();

    if path == "/" {
        if req.method() != "GET" {
            w.error("unsupported path requested: /", 404);
            return;
        }
        w.set_header("Content-Type", "text/html; charset=utf-8");
        w.write_str("<h2>EsLogs</h2></br>");
        w.write_str(&format!("Version {}<br>", buildinfo::version()));
        w.write_str(
            "See docs at <a href='https://docs.victoriametrics.com/victorialogs/'>\
             https://docs.victoriametrics.com/victorialogs/</a></br>",
        );
        return;
    }

    // No /insert/ prefix gate: esl_insert also serves the non-/insert aliases
    // Go registers at top level (/api/v2/logs, /services/collector*,
    // /internal/insert) and returns false for anything it doesn't own.
    if esl_insert::request_handler(storage, req, w) {
        return;
    }
    if path.starts_with("/select/") && esl_select::request_handler(storage, req, w) {
        return;
    }
    // Storage-node side of the cluster select protocol (/internal/select/*,
    // /internal/delete/*). Go mounts this in eslstorage's RequestHandler;
    // mounting it here avoids a esl-storage -> esl-select crate cycle.
    if esl_select::internalselect::request_handler(storage, req, w) {
        return;
    }
    if esl_storage::request_handler(storage, req, w) {
        return;
    }

    // Nothing handled the request: mirror Go `httpserver` unsupported-path
    // behavior (including the `reason="unsupported"` error counter).
    esl_common::httpserver::unsupported_request_errors().inc();
    w.errorf(req, &format!("unsupported path requested: {path:?}"));
}

/// Prints the CLI usage banner for `-h`/`-help`. Mirrors Go `main.usage` via
/// `flagutil.Usage`, with the version line prepended (Go's `buildinfo` wraps
/// `flag.Usage` to print the version first). Written to stderr, like Go's
/// `flag.CommandLine.Output()`.
fn usage() {
    let mut out = std::io::stderr();
    let _ = std::io::Write::write_all(&mut out, buildinfo::version().as_bytes());
    let s = "\nes-logs is a log management and analytics service.\n\n\
         See the docs at https://docs.victoriametrics.com/victorialogs/\n";
    let _ = std::io::Write::write_all(&mut out, s.as_bytes());
    flagutil::write_flags(&mut out);
}

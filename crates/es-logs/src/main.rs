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

use esl_common::flagutil::Flag;
use esl_common::httpserver::{Request, ResponseWriter};
use esl_common::{buildinfo, envflag, fatalf, flagutil, httpserver, infof, logger, procutil};

use esl_logstorage::storage::Storage;

// PORT NOTE: Go declares `-httpListenAddr` as an `ArrayString` (multiple
// listeners). The ported `httpserver::serve` binds a single address, so the
// flag is a single `String` here. The benchmark passes exactly one address.
static HTTP_LISTEN_ADDR: Flag<String> = Flag::new(
    "httpListenAddr",
    "TCP address to listen for incoming http requests. See also -httpListenAddr.useProxyProtocol",
    || ":9428".to_string(),
);

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
    logger::init();

    let listen_addr = HTTP_LISTEN_ADDR.get().clone();
    infof!("starting EsLogs at {listen_addr:?}...");
    let start_time = Instant::now();

    let storage = esl_storage::init();
    // Register the storage metrics set for /metrics (Go does this inside
    // vlstorage.Init).
    esl_storage::init_storage_metrics(&storage);

    // Go eslinsert.Init(): start the syslog TCP/UDP/unix listeners (no-op
    // unless -syslog.listenAddr.* flags are set).
    esl_insert::syslog_listeners::must_init(&storage);

    // Build the router closure. It owns a clone of the storage `Arc` and is
    // `Send + Sync + 'static`, as required by `httpserver::serve`.
    let router_storage = Arc::clone(&storage);
    let handle = match httpserver::serve(&listen_addr, move |req, w| {
        request_handler(&router_storage, req, w);
    }) {
        Ok(h) => h,
        Err(err) => {
            fatalf!("cannot start the http server at {listen_addr:?}: {err}");
            unreachable!()
        }
    };

    infof!(
        "started EsLogs in {:.3} seconds; see https://docs.victoriametrics.com/victorialogs/",
        start_time.elapsed().as_secs_f64()
    );

    // Block until SIGTERM/SIGINT, then shut down gracefully.
    let sig = procutil::wait_for_sigterm();
    infof!("received signal {sig}");

    infof!("gracefully shutting down webservice at {listen_addr:?}");
    let start_time = Instant::now();
    handle.stop();
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

/// Prints the CLI usage banner. Mirrors Go `main.usage`.
///
/// PORT NOTE: Go registers this via `flag.Usage`; there is no central usage
/// hook in the ported flag layer yet, so it is exposed for the `-help` path to
/// call once that lands.
#[allow(dead_code)]
fn usage() {
    let s = "\nes-logs is a log management and analytics service.\n\n\
         See the docs at https://docs.victoriametrics.com/victorialogs/\n";
    let mut out = std::io::stdout();
    let _ = std::io::Write::write_all(&mut out, s.as_bytes());
    flagutil::write_flags(&mut out);
}

//! Rust port of the EsLogs eslagent binary (`app/eslagent/main.go`).
//!
//! eslagent collects logs via popular data ingestion protocols and routes them
//! to EsLogs: the eslinsert-compatible HTTP endpoints (plus the file and
//! kubernetes collectors) feed `remotewrite`, which buffers the rows in
//! persistent queues and ships them to every `-remoteWrite.url`.
//!
//! PORT NOTE: `-httpListenAddr` is an ArrayString (Go), so several listeners
//! can be started, each with its own indexed `-tls*` and
//! `-httpListenAddr.useProxyProtocol` config.
//!
use std::sync::Arc;
use std::time::Instant;

use esl_common::flagutil::{ArrayString, Flag};
use esl_common::httpserver::{Request, ResponseWriter};
use esl_common::{
    buildinfo, envflag, fatalf, flagutil, httpserver, infof, logger, procutil, pushmetrics,
};

use esl_agent::{filecollector, kubernetescollector, remotewrite, tail};

static HTTP_LISTEN_ADDR: Flag<ArrayString> = Flag::new(
    "httpListenAddr",
    "TCP address to listen for incoming http requests. \
     Set this flag to empty value in order to disable listening on any port. \
     This mode may be useful for running multiple eslagent instances on the same server.",
    // No baked-in default (Go's flagutil.NewArrayString has none): `:9429` is
    // the fallback applied below, not a base that CLI addresses append to.
    ArrayString::default,
);
esl_common::register_flag!(HTTP_LISTEN_ADDR);

static TMP_DATA_PATH: Flag<String> = Flag::new(
    "tmpDataPath",
    "Base directory for storing eslagent data. \
     Used as default for -remoteWrite.tmpDataPath, -kubernetesCollector.checkpointsPath, \
     and -fileCollector.checkpointsPath unless those flags are set explicitly",
    String::new,
);
esl_common::register_flag!(TMP_DATA_PATH);

fn main() {
    // Install signal handlers before any long-running setup (see
    // es-logs/src/main.rs and procutil::init for the rationale).
    procutil::init();

    // Parse flags (envflag first, mirroring Go `envflag.Parse()`).
    envflag::parse();
    buildinfo::init();
    // Go's flag.Parse prints the version-prefixed usage and exits for -h/-help.
    if flagutil::help_requested() {
        usage();
        std::process::exit(0);
    }
    remotewrite::init_secret_flags();
    // Go registers `-pushmetrics.url` as secret in package init(), before
    // logger.Init logs the flags; mirror that ordering here.
    pushmetrics::init_secret_flags();
    logger::init();

    // Go `main.run`: fall back to `:9429` only when no address is configured,
    // then drop empty entries so `-httpListenAddr=''` disables listening.
    let mut listen_addrs: Vec<String> = HTTP_LISTEN_ADDR.get().0.clone();
    if listen_addrs.is_empty() {
        listen_addrs = vec![":9429".to_string()];
    }
    let listen_addrs: Vec<String> = listen_addrs.into_iter().filter(|a| !a.is_empty()).collect();
    infof!("starting eslagent at {listen_addrs:?}...");
    let start_time = Instant::now();

    // Go: insertutil.SetLogRowsStorage(&remotewrite.Storage{}). The ported
    // esl-insert handlers take the storage sink explicitly instead.
    let storage = Arc::new(remotewrite::Storage);
    remotewrite::init(TMP_DATA_PATH.get());

    filecollector::init(TMP_DATA_PATH.get(), Arc::clone(&storage));
    // The ported kubernetescollector receives its tailer and storage sink via
    // registration hooks (Go reaches both through package globals).
    kubernetescollector::set_tailer_factory(Box::new(|checkpoints_path| {
        Box::new(TailerAdapter(tail::start(checkpoints_path)))
    }));
    kubernetescollector::set_log_rows_storage(Arc::clone(&storage) as _);
    kubernetescollector::init(TMP_DATA_PATH.get());
    // Go eslinsert.Init(): start the syslog TCP/UDP/unix listeners (no-op
    // unless -syslog.listenAddr.* flags are set).
    esl_insert::syslog_listeners::must_init(&storage);

    // Go disables the HTTP server entirely when every -httpListenAddr is empty;
    // otherwise start one listener per address with its indexed `-tls*` config.
    let mut handles: Vec<httpserver::ServerHandle> = Vec::with_capacity(listen_addrs.len());
    for (i, addr) in listen_addrs.iter().enumerate() {
        let router_storage = Arc::clone(&storage);
        match httpserver::serve_listener(addr, i, move |req, w| {
            request_handler(&router_storage, req, w);
        }) {
            Ok(h) => handles.push(h),
            Err(err) => {
                fatalf!("cannot start the http server at {addr:?}: {err}");
                unreachable!()
            }
        }
    }
    infof!(
        "started eslagent in {:.3} seconds",
        start_time.elapsed().as_secs_f64()
    );

    pushmetrics::init();
    let sig = procutil::wait_for_sigterm();
    infof!("received signal {sig}");
    pushmetrics::stop();

    let start_time = Instant::now();
    infof!("gracefully shutting down webservice at {listen_addrs:?}");
    for handle in handles {
        handle.stop();
    }
    // Go eslinsert.Stop().
    esl_insert::syslog_listeners::must_stop();
    kubernetescollector::stop();
    filecollector::stop();
    remotewrite::stop();
    infof!(
        "successfully shut down the webservice in {:.3} seconds",
        start_time.elapsed().as_secs_f64()
    );
    infof!(
        "successfully stopped eslagent in {:.3} seconds",
        start_time.elapsed().as_secs_f64()
    );
}

/// RequestHandler handles insert requests for EsLogs
/// (Go `main.requestHandler`).
///
/// Common endpoints (`/health`, `/metrics`, `/flags`, ...) are served by
/// `httpserver`'s built-in routes before this handler is invoked.
fn request_handler(storage: &Arc<remotewrite::Storage>, req: &mut Request, w: &mut ResponseWriter) {
    let path = req.path().to_string();
    if path == "/" {
        if req.method() != "GET" {
            w.error("unsupported path requested: /", 404);
            return;
        }
        w.set_header("Content-Type", "text/html; charset=utf-8");
        w.write_str("<h2>eslagent</h2>");
        w.write_str(
            "See docs at <a href='https://docs.victoriametrics.com/victorialogs/vlagent/'>\
             https://docs.victoriametrics.com/victorialogs/vlagent/</a></br>",
        );
        w.write_str("Useful endpoints:</br>");
        w.write_str("<a href='metrics'>metrics</a> - available service metrics</br>");
        w.write_str("<a href='flags'>flags</a> - command-line flags</br>");
        return;
    }
    if esl_insert::request_handler(storage, req, w) {
        return;
    }
    esl_common::httpserver::unsupported_request_errors().inc();
    w.errorf(req, &format!("unsupported path requested: {path:?}"));
}

/// Adapts the ported `tail::Tailer` to the Go-shaped `Tailer` trait the
/// kubernetescollector codes against (Go calls `tail.Start` directly).
struct TailerAdapter(tail::Tailer);

impl kubernetescollector::Tailer for TailerAdapter {
    fn start_read(&self, file_path: &str, proc: Box<dyn kubernetescollector::TailProcessor>) {
        self.0
            .start_read(file_path, Box::new(TailProcessorAdapter(proc)));
    }

    fn is_tailing(&self, file_path: &str) -> bool {
        self.0.is_tailing(file_path)
    }

    fn cleanup_checkpoints(&self) {
        self.0.cleanup_checkpoints();
    }

    fn stop(&self) {
        self.0.stop();
    }
}

/// Adapts the collector's `TailProcessor` to the tail module's `Processor`
/// (the traits have identical Go shapes; see the kubernetescollector PORT
/// NOTE on siblings).
struct TailProcessorAdapter(Box<dyn kubernetescollector::TailProcessor>);

impl tail::Processor for TailProcessorAdapter {
    fn try_add_line(&mut self, line: &[u8]) -> bool {
        self.0.try_add_line(line)
    }

    fn flush(&mut self) {
        self.0.flush();
    }

    fn must_close(&mut self) {
        self.0.must_close();
    }
}

/// Prints the CLI usage banner. Mirrors Go `main.usage`.
///
/// PORT NOTE: Go registers this via `flag.Usage`; there is no central usage
/// hook in the ported flag layer yet, so it is exposed for the `-help` path to
/// call once that lands (same as the es-logs binary).
fn usage() {
    let mut out = std::io::stderr();
    let _ = std::io::Write::write_all(&mut out, buildinfo::version().as_bytes());
    let s = "\neslagent collects logs via popular data ingestion protocols and routes it to EsLogs.\n\n\
         See the docs at https://docs.victoriametrics.com/victorialogs/vlagent/ .\n";
    let _ = std::io::Write::write_all(&mut out, s.as_bytes());
    flagutil::write_flags(&mut out);
}

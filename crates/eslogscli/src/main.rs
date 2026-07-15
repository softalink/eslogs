//! Port of `app/eslogscli/main.go` — interactive command-line tool for
//! querying EsLogs.

mod json_prettifier;
mod less_wrapper;

use std::fmt::Write as _;
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Write};
use std::net::TcpStream;
use std::time::Instant;

use esl_common::flagutil::{ArrayString, Flag, Password};
use esl_common::tlsutil::{self, TLSConfig, TlsClientConfig, TlsClientStream};
use esl_common::{buildinfo, envflag, flagutil, fs, logger};

use esl_logstorage::parser::ParseQuery;

use crate::json_prettifier::{JsonPrettifier, OutputMode};
use crate::less_wrapper::{is_err_pipe, read_with_less};

static DATASOURCE_URL: Flag<String> = Flag::new(
    "datasource.url",
    "URL for querying EsLogs; \
     see https://docs.victoriametrics.com/victorialogs/querying/#querying-logs . See also -tail.url",
    || "http://localhost:9428/select/logsql/query".to_string(),
);
static TAIL_URL: Flag<String> = Flag::new(
    "tail.url",
    "URL for live tailing queries to EsLogs; see https://docs.victoriametrics.com/victorialogs/querying/#live-tailing .\
     The url is automatically detected from -datasource.url by replacing /query with /tail at the end if -tail.url is empty",
    String::new,
);
static HISTORY_FILE: Flag<String> =
    Flag::new("historyFile", "Path to file with command history", || {
        "eslogscli-history".to_string()
    });

static HEADER: Flag<ArrayString> = Flag::new(
    "header",
    "Optional header to pass in request -datasource.url in the form 'HeaderName: value'",
    || ArrayString(Vec::new()),
);
static ACCOUNT_ID: Flag<i64> = Flag::new(
    "accountID",
    "Account ID to query; see https://docs.victoriametrics.com/victorialogs/#multitenancy",
    || 0,
);
static PROJECT_ID: Flag<i64> = Flag::new(
    "projectID",
    "Project ID to query; see https://docs.victoriametrics.com/victorialogs/#multitenancy",
    || 0,
);

static USERNAME: Flag<String> = Flag::new(
    "username",
    "Optional basic auth username to use for the -datasource.url",
    String::new,
);
static PASSWORD: Flag<Password> = Flag::new(
    "password",
    "Optional basic auth password to use for the -datasource.url",
    || Password::new("password"),
);
static BEARER_TOKEN: Flag<Password> = Flag::new(
    "bearerToken",
    "Optional bearer auth token to use for the -datasource.url",
    || Password::new("bearerToken"),
);

// PORT NOTE: Go wires these flags through lib/promauth (promauth.TLSConfig);
// this port builds an `esl_common::tlsutil::TLSConfig` from them in
// `new_auth_config` and speaks TLS via rustls.
static TLS_CA_FILE: Flag<String> = Flag::new(
    "tlsCAFile",
    "Optional path to TLS CA file to use for verifying connections to the -datasource.url. By default, system CA is used",
    String::new,
);
static TLS_CERT_FILE: Flag<String> = Flag::new(
    "tlsCertFile",
    "Optional path to client-side TLS certificate file to use when connecting to the -datasource.url",
    String::new,
);
static TLS_KEY_FILE: Flag<String> = Flag::new(
    "tlsKeyFile",
    "Optional path to client-side TLS certificate key to use when connecting to the -datasource.url",
    String::new,
);
static TLS_SERVER_NAME: Flag<String> = Flag::new(
    "tlsServerName",
    "Optional TLS server name to use for connections to the -datasource.url. \
     By default, the server name from -datasource.url is used",
    String::new,
);
static TLS_INSECURE_SKIP_VERIFY: Flag<bool> = Flag::new(
    "tlsInsecureSkipVerify",
    "Whether to skip tls verification when connecting to the -datasource.url",
    || false,
);

const FIRST_LINE_PROMPT: &str = ";> ";
const NEXT_LINE_PROMPT: &str = "";
/// Max entries kept in the history file and in rustyline's recall buffer.
const HISTORY_MAX_ENTRIES: usize = 500;

fn main() {
    // PORT NOTE: Go writes flags and the help message to stdout via
    // flag.CommandLine.SetOutput; the ported flag layer has no usage hook yet
    // (see `usage`).
    envflag::parse();
    buildinfo::init();
    logger::init_no_log_flags();

    if std::env::args().any(|a| matches!(a.as_str(), "-h" | "-help" | "--help")) {
        usage();
        return;
    }

    let headers = match parse_headers(&HEADER.get().0) {
        Ok(hes) => hes,
        Err(err) => fatalf(&format!("cannot parse -header command-line flag: {err}")),
    };

    let auth_config = new_auth_config();

    interrupt::spawn_watcher();

    let mut rl = Readline::new();

    rl.writeln(&format!(
        "sending queries to -datasource.url={}",
        DATASOURCE_URL.get()
    ));
    rl.writeln("type ? and press enter to see available commands");
    run_readline_loop(&mut rl, &headers, &auth_config);
}

/// Extra headers passed with every request, incl. the auth header and the TLS
/// client config.
struct RequestContext<'a> {
    headers: &'a [HeaderEntry],
    auth_config: &'a AuthConfig,
}

fn run_readline_loop(rl: &mut Readline, headers: &[HeaderEntry], auth_config: &AuthConfig) {
    let mut history_lines = match load_from_history(HISTORY_FILE.get()) {
        Ok(lines) => lines,
        Err(err) => fatalf(&format!("cannot load query history: {err}")),
    };
    for line in &history_lines {
        rl.save_to_history(line);
    }

    let rctx = RequestContext {
        headers,
        auth_config,
    };

    let mut output_mode = OutputMode::JsonMultiline;
    let mut disable_colors = true;
    let mut wrap_long_lines = false;
    let mut s = String::new();
    loop {
        let line = match rl.read_line() {
            ReadResult::Line(line) => line,
            ReadResult::Eof => {
                // Ctrl+D at an empty prompt, or EOF on piped stdin.
                if !s.is_empty() {
                    // Execute a query left in the buffer (e.g. a piped query
                    // without a trailing empty line).
                    execute_query(rl, &s, output_mode, disable_colors, wrap_long_lines, &rctx);
                }
                return;
            }
            ReadResult::Interrupted => {
                // Ctrl+C during line editing (rustyline surfaces Go's
                // readline ErrInterrupt): drop the half-entered multiline
                // query and return to the primary prompt without exiting.
                s.clear();
                rl.set_prompt(FIRST_LINE_PROMPT);
                continue;
            }
        };

        s += &line;
        if s.is_empty() {
            // Skip empty lines
            continue;
        }

        if is_quit_command(&s) {
            rl.writeln("bye!");
            push_to_history(rl, &mut history_lines, &s);
            return;
        }
        if is_help_command(&s) {
            print_commands_help(rl);
            push_to_history(rl, &mut history_lines, &s);
            s.clear();
            continue;
        }
        if s == r"\s" {
            rl.writeln("singleline json output mode");
            output_mode = OutputMode::JsonSingleline;
            push_to_history(rl, &mut history_lines, &s);
            s.clear();
            continue;
        }
        if s == r"\m" {
            rl.writeln("multiline json output mode");
            output_mode = OutputMode::JsonMultiline;
            push_to_history(rl, &mut history_lines, &s);
            s.clear();
            continue;
        }
        if s == r"\c" {
            rl.writeln("compact output mode");
            output_mode = OutputMode::Compact;
            push_to_history(rl, &mut history_lines, &s);
            s.clear();
            continue;
        }
        if s == r"\logfmt" {
            rl.writeln("logfmt output mode");
            output_mode = OutputMode::Logfmt;
            push_to_history(rl, &mut history_lines, &s);
            s.clear();
            continue;
        }
        if s == r"\wrap_long_lines" {
            if wrap_long_lines {
                wrap_long_lines = false;
                rl.writeln("wrapping of long lines is disabled");
            } else {
                wrap_long_lines = true;
                rl.writeln("wrapping of long lines is enabled");
            }
            push_to_history(rl, &mut history_lines, &s);
            s.clear();
            continue;
        }
        if s == r"\disable_colors" {
            if !disable_colors {
                disable_colors = true;
                rl.writeln(
                    r"disabled colors in compact output mode; enter \enable_colors for enabling it",
                );
            }
            push_to_history(rl, &mut history_lines, &s);
            s.clear();
            continue;
        }
        if s == r"\enable_colors" {
            if disable_colors {
                disable_colors = false;
                rl.writeln(
                    r"enabled colors in compact output mode; type \disable_colors for disabling it",
                );
            }
            push_to_history(rl, &mut history_lines, &s);
            s.clear();
            continue;
        }
        if is_incomplete_query_line(&line) {
            // Assume the query is incomplete and allow the user finishing the query on the next line
            s.push('\n');
            rl.set_prompt(NEXT_LINE_PROMPT);
            continue;
        }

        // Execute the query. Ctrl+C cancels the in-flight query and returns
        // to the prompt: Go scopes a signal.NotifyContext around this call;
        // the port's process-wide interrupt watcher plus the per-request
        // `interrupt::watch_query` registration inside `http_post` cover the
        // same window (see `mod interrupt`).
        execute_query(rl, &s, output_mode, disable_colors, wrap_long_lines, &rctx);

        push_to_history(rl, &mut history_lines, &s);
        s.clear();
        rl.set_prompt(FIRST_LINE_PROMPT);
    }
}

/// Returns true when `line` should be treated as an incomplete query, so the
/// user can finish the query on the next line (the query wrapping rule from
/// Go's `runReadlineLoop`).
fn is_incomplete_query_line(line: &str) -> bool {
    !line.is_empty() && !line.ends_with(';')
}

fn push_to_history(rl: &mut Readline, history_lines: &mut Vec<String>, s: &str) {
    let s = s.trim();
    if history_lines.last().map(String::as_str) != Some(s) {
        history_lines.push(s.to_string());
        if history_lines.len() > HISTORY_MAX_ENTRIES {
            history_lines.drain(..history_lines.len() - HISTORY_MAX_ENTRIES);
        }
        must_save_to_history(HISTORY_FILE.get(), history_lines);
    }
    rl.save_to_history(s);
}

fn load_from_history(file_path: &str) -> Result<Vec<String>, String> {
    if !fs::is_path_exist(file_path) {
        return Ok(Vec::new());
    }
    let data = std::fs::read_to_string(file_path).map_err(|err| err.to_string())?;
    let mut lines = Vec::new();
    for (i, line_quoted) in data.split('\n').enumerate() {
        if line_quoted.is_empty() {
            continue;
        }
        let line = go_unquote(line_quoted).map_err(|err| {
            format!(
                "cannot parse line #{} at {}: {}; line: [{}]",
                i + 1,
                file_path,
                err,
                line_quoted
            )
        })?;
        lines.push(line);
    }
    Ok(lines)
}

fn must_save_to_history(file_path: &str, lines: &[String]) {
    let lines_quoted: Vec<String> = lines.iter().map(|line| go_quote(line)).collect();
    let data = lines_quoted.join("\n");
    fs::must_write_sync(file_path, data.as_bytes());
}

fn is_quit_command(s: &str) -> bool {
    matches!(s, r"\q" | "q" | "quit" | "exit")
}

fn is_help_command(s: &str) -> bool {
    matches!(s, r"\h" | "h" | "help" | "?")
}

fn print_commands_help(rl: &mut Readline) {
    rl.write(
        r"Available commands:
\q - quit
\h - show this help
\s - singleline json output mode
\m - multiline json output mode
\c - compact output mode
\logfmt - logfmt output mode
\wrap_long_lines - toggles wrapping long lines
\enable_colors - enable ANSI colors in compact output mode
\disable_colors - disable ANSI colors in compact output mode
\tail <query> - live tail <query> results

See https://docs.victoriametrics.com/victorialogs/querying/vlogscli/ for more details
",
    );
}

fn execute_query(
    output: &mut Readline,
    q_str: &str,
    output_mode: OutputMode,
    disable_colors: bool,
    wrap_long_lines: bool,
    rctx: &RequestContext<'_>,
) {
    if let Some(tail_q) = q_str.strip_prefix(r"\tail ") {
        tail_query(output, tail_q, output_mode, rctx);
        return;
    }

    let Some(mut resp_body) =
        get_query_response(output, q_str, output_mode, DATASOURCE_URL.get(), rctx)
    else {
        return;
    };

    if let Err(err) = read_with_less(&mut resp_body, disable_colors, wrap_long_lines) {
        output.writeln(&format!("error when reading query response: {err}"));
    }
}

fn tail_query(
    output: &mut Readline,
    q_str: &str,
    output_mode: OutputMode,
    rctx: &RequestContext<'_>,
) {
    let q_url = match get_tail_url() {
        Ok(u) => u,
        Err(err) => {
            output.writeln(&err);
            return;
        }
    };

    let Some(mut resp_body) = get_query_response(output, q_str, output_mode, &q_url, rctx) else {
        return;
    };

    // PORT NOTE: `output` wraps stdout; the copy below matches Go's
    // io.Copy(output, respBody). A Ctrl+C-interrupted tail is silent like
    // Go's context.Canceled check.
    let stdout = io::stdout();
    let mut w = stdout.lock();
    if let Err(err) = io::copy(&mut resp_body, &mut w) {
        if !is_err_pipe(&err) && !interrupt::interrupted() {
            drop(w);
            output.writeln(&format!("error when live tailing query response: {err}"));
        }
        output.writeln("");
    }
    let _ = io::stdout().flush();
}

fn get_tail_url() -> Result<String, String> {
    if !TAIL_URL.get().is_empty() {
        return Ok(TAIL_URL.get().clone());
    }

    let datasource_url = DATASOURCE_URL.get();
    let u = parse_url(datasource_url)
        .map_err(|err| format!("cannot parse -datasource.url={datasource_url:?}: {err}"))?;
    let Some(path_prefix) = u.path.strip_suffix("/query") else {
        return Err(format!(
            "cannot find /query suffix in -datasource.url={datasource_url:?}"
        ));
    };
    Ok(format!(
        "{}://{}{}{}",
        u.scheme, u.host_port, path_prefix, "/tail"
    ))
}

fn get_query_response(
    output: &mut Readline,
    q_str: &str,
    output_mode: OutputMode,
    q_url: &str,
    rctx: &RequestContext<'_>,
) -> Option<JsonPrettifier> {
    // Parse the query and convert it to canonical view.
    let q_str = q_str.strip_suffix(';').unwrap_or(q_str);
    let q = match ParseQuery(q_str) {
        Ok(q) => q,
        Err(err) => {
            output.writeln(&format!("cannot parse query: {err}"));
            return None;
        }
    };
    let q_str = q.to_string();
    output.write(&format!("executing [{q_str}]..."));

    // Prepare and execute HTTP request at q_url
    let body = format!("query={}", query_escape(&q_str));

    let start_time = Instant::now();
    let result = http_post(q_url, body.as_bytes(), rctx);

    let mut query_duration = format!("client {:.3}", start_time.elapsed().as_secs_f64());
    if let Ok(resp) = &result
        && let Some(qd) = resp.header("esl-request-duration-seconds")
    {
        query_duration = format!("server {qd}");
    }
    output.writeln(&format!("; duration: {query_duration}s"));
    let mut resp = match result {
        Ok(resp) => resp,
        Err(err) => {
            if interrupt::interrupted() {
                // Go prints a bare newline for context.Canceled.
                output.writeln("");
            } else {
                output.writeln(&format!("cannot execute query: {err}"));
            }
            return None;
        }
    };

    // Verify response code
    if resp.status_code != 200 {
        let mut body = String::new();
        if let Err(err) = resp.body.read_to_string(&mut body) {
            body = format!("cannot read response body: {err}");
        }
        output.writeln(&format!(
            "unexpected status code: {}; response body:\n{}",
            resp.status_code, body
        ));
        return None;
    }

    // Prettify the response body
    Some(JsonPrettifier::new(resp.body, output_mode))
}

// ---------------------------------------------------------------------------
// Ctrl+C handling.
//
// Go wraps each interactive query in `signal.NotifyContext(context.Background(),
// os.Interrupt)` (runReadlineLoop): SIGINT cancels the in-flight HTTP request
// and the CLI returns to the prompt; at the prompt readline surfaces
// ErrInterrupt and clears the current line; readWithLess additionally ignores
// SIGINT while `less` runs so `less` can handle Ctrl+C itself.
//
// Ctrl+C splits cleanly across two phases, because rustyline puts the terminal
// into raw mode (ISIG cleared) only while it reads a line:
//  - AT THE PROMPT, rustyline reads the Ctrl+C byte itself and returns
//    `Interrupted` (no SIGINT is generated); the loop clears the half-entered
//    multiline query and redraws the prompt (Go's ErrInterrupt branch).
//  - DURING QUERY EXECUTION / `less` PAGING, the terminal is back in cooked
//    mode, so Ctrl+C is delivered as SIGINT to the watcher below.
//
// The port keeps ONE process-wide watcher thread (procutil::new_term_chan)
// instead of Go's scoped registrations. On SIGINT:
//  - a query in flight → flag it interrupted and shut down its socket, which
//    aborts the blocking request/response I/O (Go: ctx cancellation);
//  - `less` paging without an in-flight query → do nothing (`less` receives
//    the terminal-delivered SIGINT itself, like under Go's ignoreSignals);
//  - otherwise → print "interrupted" and exit 130. With rustyline handling
//    at-prompt Ctrl+C this branch is now only a safety net for a stray SIGINT
//    arriving between readline calls with no query running.
//
// PORT NOTE — remaining divergences from Go, all inherent to the no-ctx port:
// (1) cancellation attaches when the TCP connection is established, so a
// hanging dial is not abortable (Go's ctx aborts the dial too); (2) the
// non-interactive EOF execution path is also cancellable, where Go runs it
// with context.Background() and dies on SIGINT; (3) SIGTERM/SIGHUP exit via
// `exit(128+sig)` instead of the default kill-by-signal disposition (procutil's
// handlers cover them process-wide).
// ---------------------------------------------------------------------------

mod interrupt {
    use std::net::{Shutdown, TcpStream};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use esl_common::procutil;

    /// The socket of the in-flight query, shut down on SIGINT.
    static QUERY_SOCK: Mutex<Option<TcpStream>> = Mutex::new(None);
    /// Whether the current/most recent query was Ctrl+C-interrupted.
    static INTERRUPTED: AtomicBool = AtomicBool::new(false);
    /// Non-zero while `less` runs (Go `ignoreSignals(os.Interrupt)`).
    static IGNORE_DEPTH: AtomicUsize = AtomicUsize::new(0);

    /// Spawns the signal watcher; see the section comment above for the
    /// SIGINT decision table.
    pub fn spawn_watcher() {
        let term_rx = procutil::new_term_chan();
        std::thread::Builder::new()
            .name("eslogscli-interrupt-watcher".to_string())
            .spawn(move || {
                for sig in term_rx {
                    if sig != procutil::SIGINT {
                        // Go leaves SIGTERM at its default disposition;
                        // mirror the shell-visible exit status.
                        std::process::exit(128 + sig);
                    }
                    let aborted = {
                        let sock = QUERY_SOCK.lock().unwrap();
                        if let Some(sock) = &*sock {
                            INTERRUPTED.store(true, Ordering::SeqCst);
                            let _ = sock.shutdown(Shutdown::Both);
                            true
                        } else {
                            false
                        }
                    };
                    if !aborted && IGNORE_DEPTH.load(Ordering::SeqCst) == 0 {
                        // Go: fmt.Fprintf(rl, "interrupted\n");
                        //     os.Exit(128 + int(syscall.SIGINT)).
                        println!("interrupted");
                        std::process::exit(128 + procutil::SIGINT);
                    }
                }
            })
            .expect("BUG: cannot spawn the interrupt watcher thread");

        // SIGHUP keeps terminating the CLI (procutil's process-wide handlers
        // would swallow it otherwise); Go leaves it at the default disposition.
        let hup_rx = procutil::new_sighup_chan();
        std::thread::Builder::new()
            .name("eslogscli-sighup-watcher".to_string())
            .spawn(move || {
                if hup_rx.recv().is_ok() {
                    std::process::exit(128 + procutil::SIGHUP);
                }
            })
            .expect("BUG: cannot spawn the sighup watcher thread");
    }

    /// Clears the interrupted flag before a new request (Go: a fresh ctx per
    /// query). Needed separately from [`watch_query`] because a request can
    /// fail before the connection (and thus the watch) exists.
    pub fn reset() {
        INTERRUPTED.store(false, Ordering::SeqCst);
    }

    /// Registers the just-connected query socket for SIGINT abort; the
    /// returned token deregisters on drop (Go: the per-query
    /// signal.NotifyContext scope, released by `cancel()`).
    pub struct QueryWatch(());

    pub fn watch_query(sock: &TcpStream) -> QueryWatch {
        *QUERY_SOCK.lock().unwrap() = sock.try_clone().ok();
        QueryWatch(())
    }

    impl Drop for QueryWatch {
        fn drop(&mut self) {
            *QUERY_SOCK.lock().unwrap() = None;
        }
    }

    /// Whether the current/most recent query was Ctrl+C-interrupted
    /// (Go: `errors.Is(err, context.Canceled)`).
    pub fn interrupted() -> bool {
        INTERRUPTED.load(Ordering::SeqCst)
    }

    /// Suppresses the exit-on-SIGINT branch while alive (Go `ignoreSignals`
    /// in readWithLess: `less` handles Ctrl+C itself).
    pub struct IgnoreGuard(());

    pub fn ignore_interrupts() -> IgnoreGuard {
        IGNORE_DEPTH.fetch_add(1, Ordering::SeqCst);
        IgnoreGuard(())
    }

    impl Drop for IgnoreGuard {
        fn drop(&mut self) {
            IGNORE_DEPTH.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

// ---------------------------------------------------------------------------
// Minimal HTTP client.
//
// PORT NOTE: Go uses net/http with a lib/promauth-configured transport. This
// port speaks plain HTTP/1.1 over TcpStream (one connection per request,
// `Connection: close`), supporting Content-Length, chunked and read-to-EOF
// response bodies. https:// URLs wrap the connection with
// `esl_common::tlsutil` (rustls), mirroring Go's promauth.NewTLSConfig-based
// transport.
// ---------------------------------------------------------------------------

struct HeaderEntry {
    name: String,
    value: String,
}

fn parse_headers(a: &[String]) -> Result<Vec<HeaderEntry>, String> {
    let mut hes = Vec::with_capacity(a.len());
    for s in a {
        let Some((name, value)) = s.split_once(':') else {
            return Err(format!(
                "cannot parse header={s:?}; it must contain at least one ':'; for example, 'Cookie: foo'"
            ));
        };
        hes.push(HeaderEntry {
            name: name.trim().to_string(),
            value: value.trim().to_string(),
        });
    }
    Ok(hes)
}

/// The subset of Go's promauth.Config this port supports: a single optional
/// `Authorization` header derived from -username/-password or -bearerToken,
/// plus the TLS client config built from the -tls* flags.
struct AuthConfig {
    authorization: Option<String>,
    tls: TlsClientConfig,
}

/// Builds the request auth/TLS config from the command-line flags
/// (Go `newAuthConfig`).
fn new_auth_config() -> AuthConfig {
    // Go builds the TLS config eagerly via promauth.Options.NewConfig and
    // panics on a broken config; bad -tls* flag values are fatal here too.
    let tls_cfg = TLSConfig {
        ca_file: TLS_CA_FILE.get().clone(),
        cert_file: TLS_CERT_FILE.get().clone(),
        key_file: TLS_KEY_FILE.get().clone(),
        server_name: TLS_SERVER_NAME.get().clone(),
        insecure_skip_verify: *TLS_INSECURE_SKIP_VERIFY.get(),
        ..Default::default()
    };
    let tls = match tlsutil::new_tls_client_config(&tls_cfg) {
        Ok(tls) => tls,
        Err(err) => fatalf(&format!("FATAL: cannot populate auth config: {err}")),
    };

    let authorization = new_authorization_header();
    AuthConfig { authorization, tls }
}

fn new_authorization_header() -> Option<String> {
    let username = USERNAME.get();
    let password = PASSWORD.get().get();
    if !username.is_empty() || !password.is_empty() {
        let creds = format!("{username}:{password}");
        return Some(format!("Basic {}", base64_encode(creds.as_bytes())));
    }
    let bearer_token = BEARER_TOKEN.get().get();
    if !bearer_token.is_empty() {
        return Some(format!("Bearer {bearer_token}"));
    }
    None
}

struct ParsedUrl {
    scheme: String,
    host_port: String,
    path: String,
    path_and_query: String,
}

fn parse_url(u: &str) -> Result<ParsedUrl, String> {
    let (scheme, rest) = u
        .split_once("://")
        .ok_or_else(|| format!("missing scheme in url {u:?}"))?;
    if scheme != "http" && scheme != "https" {
        return Err(format!("unsupported scheme {scheme:?} in url {u:?}"));
    }
    let (host_port, path_and_query) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if host_port.is_empty() {
        return Err(format!("missing host in url {u:?}"));
    }
    let path = path_and_query
        .split_once('?')
        .map_or(path_and_query, |(p, _)| p);
    Ok(ParsedUrl {
        scheme: scheme.to_string(),
        host_port: host_port.to_string(),
        path: path.to_string(),
        path_and_query: path_and_query.to_string(),
    })
}

struct HttpResponse {
    status_code: u32,
    headers: Vec<(String, String)>,
    body: Box<dyn Read>,
}

impl HttpResponse {
    /// Returns the value of the header with the given lowercase name.
    fn header(&self, lowercase_name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(name, _)| name == lowercase_name)
            .map(|(_, value)| value.as_str())
    }
}

fn http_post(u: &str, body: &[u8], rctx: &RequestContext<'_>) -> Result<HttpResponse, String> {
    interrupt::reset();
    let pu = parse_url(u)?;
    let is_tls = pu.scheme == "https";
    let addr = if pu.host_port.contains(':') {
        pu.host_port.clone()
    } else if is_tls {
        format!("{}:443", pu.host_port)
    } else {
        format!("{}:80", pu.host_port)
    };

    let stream =
        TcpStream::connect(&addr).map_err(|err| format!("Post {u:?}: dial tcp {addr}: {err}"))?;
    stream.set_nodelay(true).map_err(|err| err.to_string())?;

    // From here on Ctrl+C aborts the request by shutting this socket down
    // (Go: the query ctx from signal.NotifyContext); the watch lives as long
    // as the response body.
    let watch = interrupt::watch_query(&stream);

    // When the user overrode the TLS server name, use it as the `Host` header
    // too (Go: `req.Host = ac.tlsServerName` in promauth.Config.SetHeaders).
    let server_name = &rctx.auth_config.tls.server_name;
    let host_header = if is_tls && !server_name.is_empty() {
        server_name
    } else {
        &pu.host_port
    };

    let mut req = Vec::with_capacity(body.len() + 512);
    let _ = write!(
        req,
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n",
        pu.path_and_query,
        host_header,
        body.len()
    );
    for h in rctx.headers {
        let _ = write!(req, "{}: {}\r\n", h.name, h.value);
    }
    let _ = write!(req, "AccountID: {}\r\n", ACCOUNT_ID.get());
    let _ = write!(req, "ProjectID: {}\r\n", PROJECT_ID.get());
    if let Some(auth) = &rctx.auth_config.authorization {
        let _ = write!(req, "Authorization: {auth}\r\n");
    }
    req.extend_from_slice(b"\r\n");
    req.extend_from_slice(body);

    let mut stream = if is_tls {
        let host = host_without_port(&pu.host_port);
        Stream::Tls(Box::new(
            tlsutil::client_connect(&rctx.auth_config.tls, host, stream)
                .map_err(|err| format!("Post {u:?}: {err}"))?,
        ))
    } else {
        Stream::Plain(stream)
    };
    stream
        .write_all(&req)
        .map_err(|err| format!("cannot send request to {u:?}: {err}"))?;
    stream
        .flush()
        .map_err(|err| format!("cannot send request to {u:?}: {err}"))?;

    let mut r = BufReader::new(stream);

    // Read the status line.
    let mut line = String::new();
    r.read_line(&mut line)
        .map_err(|err| format!("cannot read response from {u:?}: {err}"))?;
    let status_code: u32 = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("malformed HTTP status line {line:?} from {u:?}"))?;

    // Read the headers.
    let mut headers = Vec::new();
    loop {
        line.clear();
        r.read_line(&mut line)
            .map_err(|err| format!("cannot read response headers from {u:?}: {err}"))?;
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            headers.push((name.trim().to_lowercase(), value.trim().to_string()));
        }
    }

    // Wrap the body according to the response framing.
    let body: Box<dyn Read> = if headers
        .iter()
        .any(|(n, v)| n == "transfer-encoding" && v.to_lowercase().contains("chunked"))
    {
        Box::new(ChunkedReader {
            r,
            chunk_remaining: 0,
            done: false,
        })
    } else if let Some(cl) = headers
        .iter()
        .find(|(n, _)| n == "content-length")
        .and_then(|(_, v)| v.parse::<u64>().ok())
    {
        Box::new(r.take(cl))
    } else {
        Box::new(r)
    };

    Ok(HttpResponse {
        status_code,
        headers,
        body: Box::new(WatchedBody {
            inner: body,
            _watch: watch,
        }),
    })
}

/// The response body plus its [`interrupt::QueryWatch`] registration: reads
/// after a Ctrl+C report Go's `context.Canceled` instead of the bare
/// EOF/reset produced by the socket shutdown.
struct WatchedBody {
    inner: Box<dyn Read>,
    _watch: interrupt::QueryWatch,
}

impl Read for WatchedBody {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // NOTE: not ErrorKind::Interrupted — io::copy would retry it forever.
        if interrupt::interrupted() {
            return Err(io::Error::other("context canceled"));
        }
        match self.inner.read(buf) {
            Ok(0) if interrupt::interrupted() => Err(io::Error::other("context canceled")),
            other => other,
        }
    }
}

/// The connection to the datasource: plain TCP or TLS over TCP.
enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsClientStream>),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf),
            // PORT NOTE: rustls reports `UnexpectedEof` when the peer closes
            // the connection without sending TLS close_notify. The CLI reads
            // streaming/read-to-EOF response bodies, so map it to a clean EOF
            // (same pattern as `TolerantEofReader` in esl-storage's
            // http_client); Go's crypto/tls + net/http tolerate the missing
            // close_notify the same way. A truncating attacker is still
            // detected on Content-Length/chunked-framed bodies by the framing
            // checks.
            Stream::Tls(s) => match s.read(buf) {
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(0),
                other => other,
            },
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(buf),
            Stream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            Stream::Tls(s) => s.flush(),
        }
    }
}

/// Strips the `:port` suffix from a `host:port` address (IPv6 literals keep
/// Go's `[host]:port` bracket form); the result feeds TLS SNI/verification.
fn host_without_port(addr: &str) -> &str {
    if let Some(rest) = addr.strip_prefix('[')
        && let Some(end) = rest.find(']')
    {
        return &rest[..end];
    }
    match addr.rsplit_once(':') {
        // An unbracketed IPv6 literal contains more colons; treat it as a
        // bare host without port.
        Some((host, _)) if !host.contains(':') => host,
        _ => addr,
    }
}

/// Streaming `Transfer-Encoding: chunked` body decoder.
struct ChunkedReader {
    r: BufReader<Stream>,
    chunk_remaining: u64,
    done: bool,
}

impl Read for ChunkedReader {
    fn read(&mut self, p: &mut [u8]) -> io::Result<usize> {
        if self.done {
            return Ok(0);
        }
        if self.chunk_remaining == 0 {
            let mut line = String::new();
            self.r.read_line(&mut line)?;
            if line.trim().is_empty() {
                // Tolerate the CRLF terminating the previous chunk.
                line.clear();
                self.r.read_line(&mut line)?;
            }
            let size_str = line.trim().split(';').next().unwrap_or("");
            let size = u64::from_str_radix(size_str, 16).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("malformed chunk size {size_str:?}"),
                )
            })?;
            if size == 0 {
                self.done = true;
                // Drain the trailing CRLF (and any trailers) best-effort.
                let mut line = String::new();
                let _ = self.r.read_line(&mut line);
                return Ok(0);
            }
            self.chunk_remaining = size;
        }
        let n = p.len().min(self.chunk_remaining as usize);
        let n = self.r.read(&mut p[..n])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF inside chunked body",
            ));
        }
        self.chunk_remaining -= n as u64;
        Ok(n)
    }
}

// ---------------------------------------------------------------------------
// Interactive line editor.
//
// PORT NOTE: Go uses github.com/ergochat/readline for the interactive prompt;
// the port uses `rustyline`, the equivalent Rust readline (Unix + Windows),
// which provides the same interactive features: in-line editing (arrow keys,
// Ctrl+A/E/K/U/W, mid-line backspace), arrow-key history recall, and Ctrl+C
// surfaced as `Interrupted` (Go `ErrInterrupt`) / Ctrl+D as `Eof`.
//
// rustyline drives the prompt only when stdin is a terminal. Piped /
// non-interactive input is read as plain lines with NO prompt, preserving the
// exact scripted-input behavior the tests rely on.
//
// History: the on-disk `-historyFile` keeps the port's strconv.Quote format
// (see load_from_history / must_save_to_history), which round-trips multiline
// entries and matches Go. rustyline is fed the same entries via
// `add_history_entry` purely for in-memory recall; its own FileHistory on-disk
// format is NOT used, so the history file stays byte-compatible with Go.
// ---------------------------------------------------------------------------

struct Readline {
    prompt: String,
    /// `Some` when stdin is an interactive terminal (rustyline raw-mode editor
    /// with history recall and Ctrl+C handling); `None` for piped input, which
    /// is read as plain lines with no prompt.
    editor: Option<rustyline::DefaultEditor>,
}

/// The outcome of reading one input line.
enum ReadResult {
    /// A line of input, with the trailing newline stripped.
    Line(String),
    /// Ctrl+C during line editing (rustyline `Interrupted` / Go `ErrInterrupt`).
    Interrupted,
    /// End of input: Ctrl+D at an empty prompt, or EOF on piped stdin.
    Eof,
}

impl Readline {
    fn new() -> Readline {
        let editor = if io::stdin().is_terminal() {
            match new_editor() {
                Ok(ed) => Some(ed),
                Err(err) => fatalf(&format!("cannot initialize line editor: {err}")),
            }
        } else {
            None
        };
        Readline {
            prompt: FIRST_LINE_PROMPT.to_string(),
            editor,
        }
    }

    fn set_prompt(&mut self, prompt: &str) {
        self.prompt = prompt.to_string();
    }

    /// Adds an entry to rustyline's in-memory recall history (no-op for piped
    /// input). The on-disk history file is written separately by
    /// [`must_save_to_history`] in the port's strconv.Quote format.
    fn save_to_history(&mut self, line: &str) {
        if let Some(ed) = &mut self.editor {
            let _ = ed.add_history_entry(line);
        }
    }

    /// Reads the next input line.
    fn read_line(&mut self) -> ReadResult {
        if let Some(ed) = &mut self.editor {
            return match ed.readline(&self.prompt) {
                Ok(line) => ReadResult::Line(line),
                Err(rustyline::error::ReadlineError::Interrupted) => ReadResult::Interrupted,
                Err(rustyline::error::ReadlineError::Eof) => ReadResult::Eof,
                Err(err) => fatalf(&format!("unexpected error in readline: {err}")),
            };
        }
        read_piped_line(&mut io::stdin().lock())
    }

    /// Writes `s` to the terminal (Go writes via the readline instance).
    fn write(&mut self, s: &str) {
        let mut stdout = io::stdout();
        let _ = stdout.write_all(s.as_bytes());
        let _ = stdout.flush();
    }

    fn writeln(&mut self, s: &str) {
        self.write(s);
        self.write("\n");
    }
}

/// Builds the rustyline editor, capping recall history at the same size as the
/// on-disk history file and managing history entries manually (via
/// [`Readline::save_to_history`]).
fn new_editor() -> rustyline::Result<rustyline::DefaultEditor> {
    let config = rustyline::Config::builder()
        .max_history_size(HISTORY_MAX_ENTRIES)?
        .auto_add_history(false)
        .build();
    rustyline::DefaultEditor::with_config(config)
}

/// Reads one line from a piped (non-terminal) input source, stripping the
/// trailing newline; [`ReadResult::Eof`] marks end of input.
fn read_piped_line<R: BufRead>(r: &mut R) -> ReadResult {
    let mut line = String::new();
    match r.read_line(&mut line) {
        Ok(0) => ReadResult::Eof,
        Ok(_) => {
            while line.ends_with('\n') || line.ends_with('\r') {
                line.pop();
            }
            ReadResult::Line(line)
        }
        Err(err) => fatalf(&format!("unexpected error reading input: {err}")),
    }
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

fn fatalf(msg: &str) -> ! {
    eprintln!("{msg}");
    std::process::exit(1);
}

fn usage() {
    let s = "\neslogscli is a command-line tool for querying EsLogs.\n\n\
             See the docs at https://docs.victoriametrics.com/victorialogs/querying/vlogscli/\n";
    // PORT NOTE: Go's flagutil.Usage prints all registered flags with their
    // defaults; the ported flag layer registers flags lazily, so only the
    // explicitly set flags are printed here. The version line is prepended, as
    // Go's buildinfo wraps flag.Usage to print it first.
    let mut out = io::stdout();
    let _ = writeln!(out, "{}", buildinfo::version());
    let _ = out.write_all(s.as_bytes());
    flagutil::write_flags(&mut out);
}

/// Percent-encodes `s` like Go's `url.QueryEscape` (used by url.Values.Encode).
fn query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b' ' => out.push('+'),
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// Encodes `data` with the standard base64 alphabet (RFC 4648, padded).
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Quotes `s` like Go's `strconv.Quote` (the format used by the history file).
fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\x07' => out.push_str("\\a"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x0b' => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c == '\x7f' => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Unquotes a `strconv.Quote`-style string (the inverse of [`go_quote`]).
fn go_unquote(s: &str) -> Result<String, String> {
    let s = s
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .ok_or_else(|| "invalid syntax".to_string())?;
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            if c == '"' {
                return Err("invalid syntax".to_string());
            }
            out.push(c);
            continue;
        }
        let esc = chars.next().ok_or_else(|| "invalid syntax".to_string())?;
        match esc {
            '"' => out.push('"'),
            '\\' => out.push('\\'),
            'a' => out.push('\x07'),
            'b' => out.push('\x08'),
            'f' => out.push('\x0c'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'v' => out.push('\x0b'),
            'x' | 'u' | 'U' => {
                let n = match esc {
                    'x' => 2,
                    'u' => 4,
                    _ => 8,
                };
                let mut cp = 0u32;
                for _ in 0..n {
                    let d = chars
                        .next()
                        .and_then(|c| c.to_digit(16))
                        .ok_or_else(|| "invalid syntax".to_string())?;
                    cp = cp * 16 + d;
                }
                out.push(char::from_u32(cp).ok_or_else(|| "invalid syntax".to_string())?);
            }
            _ => return Err("invalid syntax".to_string()),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quit_and_help_commands() {
        for s in [r"\q", "q", "quit", "exit"] {
            assert!(is_quit_command(s), "{s} must be a quit command");
        }
        assert!(!is_quit_command("quit;"));
        for s in [r"\h", "h", "help", "?"] {
            assert!(is_help_command(s), "{s} must be a help command");
        }
        assert!(!is_help_command("halp"));
    }

    #[test]
    fn incomplete_query_line_detection() {
        // An empty line terminates the accumulated multiline query.
        assert!(!is_incomplete_query_line(""));
        // A `;`-terminated line completes the query.
        assert!(!is_incomplete_query_line("_time:5m;"));
        // Anything else continues on the next line.
        assert!(is_incomplete_query_line("_time:5m | count()"));
    }

    #[test]
    fn read_piped_line_reads_lines_then_eof() {
        use std::io::Cursor;
        // A multiline query split across two physical lines, then a command.
        let mut r = Cursor::new(&b"_time:5m\n| count();\n\\q\n"[..]);
        assert!(matches!(read_piped_line(&mut r), ReadResult::Line(l) if l == "_time:5m"));
        assert!(matches!(read_piped_line(&mut r), ReadResult::Line(l) if l == "| count();"));
        assert!(matches!(read_piped_line(&mut r), ReadResult::Line(l) if l == r"\q"));
        assert!(matches!(read_piped_line(&mut r), ReadResult::Eof));
        // Reading past EOF keeps reporting EOF.
        assert!(matches!(read_piped_line(&mut r), ReadResult::Eof));
    }

    #[test]
    fn read_piped_line_without_trailing_newline() {
        use std::io::Cursor;
        // A final line lacking a trailing newline still yields the line, then EOF.
        let mut r = Cursor::new(&b"error;"[..]);
        assert!(matches!(read_piped_line(&mut r), ReadResult::Line(l) if l == "error;"));
        assert!(matches!(read_piped_line(&mut r), ReadResult::Eof));
    }

    /// Mirrors the multiline accumulation / interrupt / EOF glue of
    /// `run_readline_loop` (using the same [`is_incomplete_query_line`]
    /// predicate) so it can be exercised without a terminal or network.
    /// Returns the query strings that would be submitted for execution.
    fn collect_submissions(results: Vec<ReadResult>) -> Vec<String> {
        let mut s = String::new();
        let mut submitted = Vec::new();
        for result in results {
            match result {
                ReadResult::Line(line) => {
                    if s.is_empty() && line.is_empty() {
                        continue; // skip empty lines at the prompt
                    }
                    s.push_str(&line);
                    if is_incomplete_query_line(&line) {
                        s.push('\n');
                    } else {
                        submitted.push(std::mem::take(&mut s));
                    }
                }
                ReadResult::Interrupted => s.clear(), // Ctrl+C drops the query
                ReadResult::Eof => {
                    if !s.is_empty() {
                        submitted.push(std::mem::take(&mut s));
                    }
                    break;
                }
            }
        }
        submitted
    }

    #[test]
    fn piped_multiline_query_is_assembled_and_submitted() {
        let submitted = collect_submissions(vec![
            ReadResult::Line("_time:5m".into()), // incomplete: accumulates with '\n'
            ReadResult::Line("| count();".into()), // terminated: submit
            ReadResult::Eof,
        ]);
        assert_eq!(submitted, vec!["_time:5m\n| count();".to_string()]);
    }

    #[test]
    fn eof_submits_query_left_in_the_buffer() {
        // A `;`-less final line is left in the buffer (with the continuation
        // '\n' the loop appends) and executed on EOF — the non-interactive
        // path Go runs with context.Background().
        let submitted =
            collect_submissions(vec![ReadResult::Line("error".into()), ReadResult::Eof]);
        assert_eq!(submitted, vec!["error\n".to_string()]);
    }

    #[test]
    fn ctrl_c_discards_in_progress_multiline_query() {
        let submitted = collect_submissions(vec![
            ReadResult::Line("_time:5m".into()), // starts a multiline query
            ReadResult::Interrupted,             // Ctrl+C clears it
            ReadResult::Line("error;".into()),   // fresh, complete query
            ReadResult::Eof,
        ]);
        assert_eq!(submitted, vec!["error;".to_string()]);
    }

    #[test]
    fn parse_headers_valid_and_invalid() {
        let hes = parse_headers(&["Cookie: foo".to_string(), "X-A:b: c".to_string()]).unwrap();
        assert_eq!(hes.len(), 2);
        assert_eq!(hes[0].name, "Cookie");
        assert_eq!(hes[0].value, "foo");
        assert_eq!(hes[1].name, "X-A");
        assert_eq!(hes[1].value, "b: c");
        assert!(parse_headers(&["no-colon".to_string()]).is_err());
    }

    #[test]
    fn parse_url_forms() {
        let u = parse_url("http://localhost:9428/select/logsql/query").unwrap();
        assert_eq!(u.scheme, "http");
        assert_eq!(u.host_port, "localhost:9428");
        assert_eq!(u.path, "/select/logsql/query");
        assert_eq!(u.path_and_query, "/select/logsql/query");

        let u = parse_url("http://host/path?x=y").unwrap();
        assert_eq!(u.path, "/path");
        assert_eq!(u.path_and_query, "/path?x=y");

        let u = parse_url("https://host").unwrap();
        assert_eq!(u.path, "/");

        assert!(parse_url("localhost:9428/query").is_err());
        assert!(parse_url("ftp://host/query").is_err());
    }

    #[test]
    fn query_escape_matches_go() {
        assert_eq!(query_escape("a b"), "a+b");
        assert_eq!(
            query_escape("_time:5m | count()"),
            "_time%3A5m+%7C+count%28%29"
        );
        assert_eq!(query_escape("a-_.~z"), "a-_.~z");
    }

    #[test]
    fn base64_encode_matches_go() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"user:pass"), "dXNlcjpwYXNz");
    }

    #[test]
    fn host_without_port_forms() {
        assert_eq!(host_without_port("example.com:9428"), "example.com");
        assert_eq!(host_without_port("127.0.0.1:9428"), "127.0.0.1");
        assert_eq!(host_without_port("[::1]:9428"), "::1");
        assert_eq!(host_without_port("example.com"), "example.com");
        assert_eq!(host_without_port("::1"), "::1");
    }

    /// Builds an [`AuthConfig`] directly from a [`TLSConfig`], bypassing the
    /// global command-line flags.
    fn test_auth_config(tls_cfg: &TLSConfig) -> AuthConfig {
        AuthConfig {
            authorization: None,
            tls: tlsutil::new_tls_client_config(tls_cfg).unwrap(),
        }
    }

    /// Reads one HTTP/1.1 request (headers + `Content-Length` body) from `r`.
    fn read_request<R: Read>(r: &mut R) -> Vec<u8> {
        let mut req = vec![0u8; 8192];
        let mut n = 0;
        let header_end = loop {
            if let Some(pos) = req[..n].windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
            let m = r.read(&mut req[n..]).unwrap();
            assert!(m > 0, "request truncated");
            n += m;
        };
        let head = String::from_utf8_lossy(&req[..header_end]).to_string();
        let cl: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .map_or(0, |v| v.trim().parse().unwrap());
        while n < header_end + cl {
            let m = r.read(&mut req[n..]).unwrap();
            assert!(m > 0, "request body truncated");
            n += m;
        }
        req.truncate(n);
        req
    }

    /// Spawns a one-shot https server with a self-signed rcgen certificate.
    /// It reads one request and answers with `response`; returns the server
    /// address, the certificate PEM (usable as the client's custom CA) and a
    /// handle yielding the captured request bytes.
    fn spawn_https_server(
        response: &'static [u8],
        close_notify: bool,
    ) -> (String, String, std::thread::JoinHandle<Vec<u8>>) {
        static DIR_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

        let ck = rcgen::generate_simple_self_signed(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .unwrap();
        let cert_pem = ck.cert.pem();
        // get_server_tls_config loads the pair from files.
        let dir = std::env::temp_dir().join(format!(
            "eslogscli-tls-test-{}-{}",
            std::process::id(),
            DIR_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("cert.pem");
        std::fs::write(&cert_path, &cert_pem).unwrap();
        let key_path = dir.join("key.pem");
        std::fs::write(&key_path, ck.key_pair.serialize_pem()).unwrap();
        let server_cfg = tlsutil::get_server_tls_config(
            cert_path.to_str().unwrap(),
            key_path.to_str().unwrap(),
            "",
            &[],
        )
        .unwrap();

        let ln = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = ln.local_addr().unwrap().to_string();
        let handle = std::thread::spawn(move || {
            let (tcp, _) = ln.accept().unwrap();
            // A client aborting the handshake (untrusted-cert test) is fine.
            let Ok(mut stream) = tlsutil::server_accept(&server_cfg, tcp) else {
                return Vec::new();
            };
            let req = read_request(&mut stream);
            stream.write_all(response).unwrap();
            if close_notify {
                stream.conn.send_close_notify();
            }
            // flush() after send_close_notify() BEFORE the socket is dropped;
            // otherwise the client's reads fail with `Broken pipe`.
            let _ = stream.flush();
            req
        });
        (addr, cert_pem, handle)
    }

    #[test]
    fn https_round_trip_with_custom_ca() {
        // Once with a graceful TLS shutdown, once with a bare TCP close (the
        // missing close_notify must be treated as EOF since the
        // Content-Length framing is already satisfied).
        for close_notify in [true, false] {
            let (addr, ca_pem, handle) = spawn_https_server(
                b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nresponse",
                close_notify,
            );
            let ac = test_auth_config(&TLSConfig {
                ca: ca_pem,
                ..Default::default()
            });
            let rctx = RequestContext {
                headers: &[],
                auth_config: &ac,
            };
            let mut resp = http_post(
                &format!("https://{addr}/select/logsql/query"),
                b"query=%2A",
                &rctx,
            )
            .unwrap();
            assert_eq!(resp.status_code, 200);
            let mut body = String::new();
            resp.body.read_to_string(&mut body).unwrap();
            assert_eq!(body, "response");

            let req = handle.join().unwrap();
            let head = String::from_utf8_lossy(&req);
            assert!(
                head.starts_with("POST /select/logsql/query HTTP/1.1\r\n"),
                "{head}"
            );
            assert!(head.contains(&format!("Host: {addr}\r\n")), "{head}");
            assert!(head.ends_with("query=%2A"), "{head}");
        }
    }

    #[test]
    fn https_read_to_eof_body_tolerates_missing_close_notify() {
        // A response without Content-Length is read to EOF; the server closes
        // the TCP connection without sending TLS close_notify, which must be
        // treated as a clean EOF.
        let (addr, ca_pem, handle) = spawn_https_server(
            b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nstreamed",
            false,
        );
        let ac = test_auth_config(&TLSConfig {
            ca: ca_pem,
            ..Default::default()
        });
        let rctx = RequestContext {
            headers: &[],
            auth_config: &ac,
        };
        let mut resp = http_post(&format!("https://{addr}/query"), b"query=%2A", &rctx).unwrap();
        assert_eq!(resp.status_code, 200);
        let mut body = String::new();
        resp.body.read_to_string(&mut body).unwrap();
        assert_eq!(body, "streamed");
        let _ = handle.join().unwrap();
    }

    #[test]
    fn https_server_name_override_sets_host_header() {
        let (addr, ca_pem, handle) = spawn_https_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            true,
        );
        let ac = test_auth_config(&TLSConfig {
            ca: ca_pem,
            server_name: "localhost".to_string(),
            ..Default::default()
        });
        let rctx = RequestContext {
            headers: &[],
            auth_config: &ac,
        };
        let resp = http_post(&format!("https://{addr}/query"), b"query=%2A", &rctx).unwrap();
        assert_eq!(resp.status_code, 200);
        let req = handle.join().unwrap();
        let head = String::from_utf8_lossy(&req);
        // Go: req.Host = ac.tlsServerName when the server name is overridden.
        assert!(head.contains("Host: localhost\r\n"), "{head}");
        assert!(!head.contains(&format!("Host: {addr}\r\n")), "{head}");
    }

    #[test]
    fn https_untrusted_cert_rejected() {
        let (addr, _ca_pem, handle) = spawn_https_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            true,
        );
        // Default config verifies against the bundled root CAs, which do not
        // include the self-signed test certificate.
        let ac = test_auth_config(&TLSConfig::default());
        let rctx = RequestContext {
            headers: &[],
            auth_config: &ac,
        };
        let err = match http_post(&format!("https://{addr}/query"), b"query=%2A", &rctx) {
            Ok(_) => panic!("https request with an untrusted certificate must fail"),
            Err(err) => err,
        };
        assert!(err.contains("handshake"), "{err}");
        let _ = handle.join();
    }

    #[test]
    fn http_plain_round_trip() {
        let ln = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = ln.local_addr().unwrap().to_string();
        let handle = std::thread::spawn(move || {
            let (mut tcp, _) = ln.accept().unwrap();
            let req = read_request(&mut tcp);
            tcp.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
            req
        });
        let ac = test_auth_config(&TLSConfig::default());
        let rctx = RequestContext {
            headers: &[],
            auth_config: &ac,
        };
        let mut resp = http_post(&format!("http://{addr}/query"), b"query=%2A", &rctx).unwrap();
        assert_eq!(resp.status_code, 200);
        let mut body = String::new();
        resp.body.read_to_string(&mut body).unwrap();
        assert_eq!(body, "ok");
        let req = handle.join().unwrap();
        let head = String::from_utf8_lossy(&req);
        assert!(head.starts_with("POST /query HTTP/1.1\r\n"), "{head}");
        assert!(head.contains(&format!("Host: {addr}\r\n")), "{head}");
    }

    #[test]
    fn go_quote_unquote_roundtrip() {
        for s in ["", "plain", "multi\nline;", "tab\t\"quote\"\\back", "\x01"] {
            let quoted = go_quote(s);
            assert_eq!(go_unquote(&quoted).unwrap(), s, "roundtrip of {s:?}");
        }
        assert_eq!(go_quote("a\nb"), "\"a\\nb\"");
        assert!(go_unquote("no-quotes").is_err());
        assert!(go_unquote("\"bad\\e\"").is_err());
    }
}

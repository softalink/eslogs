//! Port of `app/eslogscli/main.go` — interactive command-line tool for
//! querying EsLogs.

mod json_prettifier;
mod less_wrapper;

use std::fmt::Write as _;
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Write};
use std::net::TcpStream;
use std::time::Instant;

use esl_common::flagutil::{ArrayString, Flag, Password};
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

// PORT NOTE: the Go client supports TLS via lib/promauth; this std-only port
// has no TLS stack, so the flags are accepted for CLI compatibility but any
// non-default value (or an https:// -datasource.url) is rejected at startup.
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

    let auth_headers = new_auth_headers();

    let mut rl = Readline::new();

    rl.writeln(&format!(
        "sending queries to -datasource.url={}",
        DATASOURCE_URL.get()
    ));
    rl.writeln("type ? and press enter to see available commands");
    run_readline_loop(&mut rl, &headers, &auth_headers);
}

/// Extra headers passed with every request, incl. the auth header.
struct RequestContext<'a> {
    headers: &'a [HeaderEntry],
    auth_headers: &'a AuthHeaders,
}

fn run_readline_loop(rl: &mut Readline, headers: &[HeaderEntry], auth_headers: &AuthHeaders) {
    let mut history_lines = match load_from_history(HISTORY_FILE.get()) {
        Ok(lines) => lines,
        Err(err) => fatalf(&format!("cannot load query history: {err}")),
    };
    for line in &history_lines {
        rl.save_to_history(line);
    }

    let rctx = RequestContext {
        headers,
        auth_headers,
    };

    let mut output_mode = OutputMode::JsonMultiline;
    let mut disable_colors = true;
    let mut wrap_long_lines = false;
    let mut s = String::new();
    loop {
        let line = match rl.read_line() {
            Ok(Some(line)) => line,
            Ok(None) => {
                // EOF
                if !s.is_empty() {
                    // This is non-interactive query execution.
                    execute_query(rl, &s, output_mode, disable_colors, wrap_long_lines, &rctx);
                }
                return;
            }
            // PORT NOTE: Go's readline additionally surfaces ErrInterrupt for
            // Ctrl+C and stores the incomplete line into history; without a
            // raw-mode line editor, Ctrl+C terminates the process via the
            // default signal disposition.
            Err(err) => fatalf(&format!("unexpected error in readline: {err}")),
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

        // Execute the query
        // PORT NOTE: Go cancels the in-flight query on Ctrl+C via
        // signal.NotifyContext; this port has no signal handling, so Ctrl+C
        // terminates the whole process instead.
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
        if history_lines.len() > 500 {
            history_lines.drain(..history_lines.len() - 500);
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
    // io.Copy(output, respBody).
    let stdout = io::stdout();
    let mut w = stdout.lock();
    if let Err(err) = io::copy(&mut resp_body, &mut w) {
        if !is_err_pipe(&err) {
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
            output.writeln(&format!("cannot execute query: {err}"));
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
// Minimal HTTP client.
//
// PORT NOTE: Go uses net/http with a lib/promauth-configured transport. This
// std-only port speaks plain HTTP/1.1 over TcpStream (one connection per
// request, `Connection: close`), supporting Content-Length, chunked and
// read-to-EOF response bodies. https:// URLs are rejected (no TLS stack).
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
/// `Authorization` header derived from -username/-password or -bearerToken.
struct AuthHeaders {
    authorization: Option<String>,
}

fn new_auth_headers() -> AuthHeaders {
    if !TLS_CA_FILE.get().is_empty()
        || !TLS_CERT_FILE.get().is_empty()
        || !TLS_KEY_FILE.get().is_empty()
        || !TLS_SERVER_NAME.get().is_empty()
        || *TLS_INSECURE_SKIP_VERIFY.get()
    {
        fatalf(
            "FATAL: cannot populate auth config: TLS is not supported by this port of eslogscli",
        );
    }

    let username = USERNAME.get();
    let password = PASSWORD.get().get();
    if !username.is_empty() || !password.is_empty() {
        let creds = format!("{username}:{password}");
        return AuthHeaders {
            authorization: Some(format!("Basic {}", base64_encode(creds.as_bytes()))),
        };
    }
    let bearer_token = BEARER_TOKEN.get().get();
    if !bearer_token.is_empty() {
        return AuthHeaders {
            authorization: Some(format!("Bearer {bearer_token}")),
        };
    }
    AuthHeaders {
        authorization: None,
    }
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
    let pu = parse_url(u)?;
    if pu.scheme != "http" {
        return Err(format!(
            "unsupported scheme {:?} in url {u:?}: TLS is not supported by this port of eslogscli",
            pu.scheme
        ));
    }
    let addr = if pu.host_port.contains(':') {
        pu.host_port.clone()
    } else {
        format!("{}:80", pu.host_port)
    };

    let stream =
        TcpStream::connect(&addr).map_err(|err| format!("Post {u:?}: dial tcp {addr}: {err}"))?;
    stream.set_nodelay(true).map_err(|err| err.to_string())?;

    let mut req = Vec::with_capacity(body.len() + 512);
    let _ = write!(
        req,
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n",
        pu.path_and_query,
        pu.host_port,
        body.len()
    );
    for h in rctx.headers {
        let _ = write!(req, "{}: {}\r\n", h.name, h.value);
    }
    let _ = write!(req, "AccountID: {}\r\n", ACCOUNT_ID.get());
    let _ = write!(req, "ProjectID: {}\r\n", PROJECT_ID.get());
    if let Some(auth) = &rctx.auth_headers.authorization {
        let _ = write!(req, "Authorization: {auth}\r\n");
    }
    req.extend_from_slice(b"\r\n");
    req.extend_from_slice(body);

    let mut stream = stream;
    stream
        .write_all(&req)
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
        body,
    })
}

/// Streaming `Transfer-Encoding: chunked` body decoder.
struct ChunkedReader {
    r: BufReader<TcpStream>,
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
// Minimal line editor.
//
// PORT NOTE: Go uses github.com/ergochat/readline for the interactive prompt.
// External dependencies aren't allowed in this port, so this is a minimal
// line reader over std stdin/stdout: prompts are printed only when stdin is a
// terminal, history is kept in memory (and persisted via -historyFile), but
// there is no raw-mode editing or arrow-key history recall.
// ---------------------------------------------------------------------------

struct Readline {
    interactive: bool,
    prompt: String,
    /// In-memory history, mirroring readline's SaveToHistory.
    history: Vec<String>,
}

impl Readline {
    fn new() -> Readline {
        Readline {
            interactive: io::stdin().is_terminal(),
            prompt: FIRST_LINE_PROMPT.to_string(),
            history: Vec::new(),
        }
    }

    fn set_prompt(&mut self, prompt: &str) {
        self.prompt = prompt.to_string();
    }

    fn save_to_history(&mut self, line: &str) {
        self.history.push(line.to_string());
    }

    /// Reads the next input line; `Ok(None)` means EOF.
    fn read_line(&mut self) -> io::Result<Option<String>> {
        if self.interactive {
            let mut stdout = io::stdout();
            stdout.write_all(self.prompt.as_bytes())?;
            stdout.flush()?;
        }
        let mut line = String::new();
        if io::stdin().lock().read_line(&mut line)? == 0 {
            return Ok(None);
        }
        while line.ends_with('\n') || line.ends_with('\r') {
            line.pop();
        }
        Ok(Some(line))
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
    // explicitly set flags are printed here.
    let mut out = io::stdout();
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

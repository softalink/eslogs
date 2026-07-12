//! Port of Softalink LLC `lib/httpserver` (plus the request-body
//! decompression from `lib/protoparser/protoparserutil`), providing the
//! HTTP/1.1 server used by the EsLogs `esl-insert` ingestion and
//! `esl-select` query paths.
//!
//! # Design decision: threaded, not async
//!
//! This is a **threaded** server built on `std::net::TcpListener` and a bounded
//! pool of worker threads — deliberately *not* async/tokio. Rationale:
//!
//! * It mirrors Go's `net/http` threaded model (the source being ported),
//!   keeping the port faithful and avoiding an async runtime dependency in a
//!   crate whose other modules are all synchronous.
//! * The benchmark's ingestion pattern is many `POST` bodies over keep-alive
//!   connections. This is CPU-bound (parse + decompress + index), so a thread
//!   pool sized to the CPU count saturates cores without the overhead of an
//!   async scheduler, and lets each request use simple blocking reads with
//!   reusable buffers.
//!
//! Ingestion throughput is a measured metric, so the request path is kept
//! lean: pooled worker threads (no per-connection spawn), keep-alive connection
//! reuse, buffered reads/writes, and transparent streaming decompression.
//!
//! ## Worker-pool sizing
//!
//! A bounded pool of `crate::cgroup::available_cpus()` worker threads all
//! block in `accept()` on the shared listener directly (the kernel wakes one
//! per connection), which keeps the fresh-connection path to a single thread
//! wakeup. A worker owns a connection for its whole keep-alive lifetime, so
//! the number of concurrently *served* connections is bounded by the pool
//! size (excess connections queue in the listener backlog). This matches the
//! CPU-bound workload: ~CPU-count parallelism is where ingestion saturates.
//!
//! ## Graceful shutdown
//!
//! [`serve`] returns a [`ServerHandle`] carrying an `Arc<AtomicBool>` stop flag.
//! Workers check it after each accept and between requests (idle keep-alive
//! reads are bounded by a read timeout). [`ServerHandle::stop`] sets the flag,
//! wakes each blocked accept with a self-connect, and joins all threads.
//!
//! PORT NOTE: basic-auth, `authKey`, pprof, path-prefix and per-connection
//! deadline jitter from the Go source are intentionally omitted — the
//! benchmark uses plain HTTP with no auth. Auth checks are effectively no-ops
//! (all requests allowed), so the `wrong_basic_auth`/`wrong_auth_key` error
//! counters and the pprof request counters have nothing to count and are not
//! registered. `esm_http_conn_timeout_closed_conns_total` is omitted with the
//! deadline jitter. Response compression (`gzhttp`) is also omitted;
//! responses are small.
//!
//! ## TLS serving
//!
//! Go's `-tls`/`-tlsCertFile`/`-tlsKeyFile`/`-tlsMinVersion`/`-tlsCipherSuites`
//! serving flags are ported on top of `crate::tlsutil` (rustls). The
//! per-connection plumbing differs by transport: a plain-TCP connection holds
//! three independent `TcpStream::try_clone` handles (the `BufReader` read
//! half, the `BufWriter` write half, and the handle that
//! `ResponseWriter::flush_chunk` writes to mid-handler for `/tail` streaming),
//! while a rustls session is a single-owner state machine that serializes
//! reads and writes, so a TLS connection instead shares one `StreamOwned`
//! behind a mutex (uncontended — every user runs on the connection's worker
//! thread) plus a raw-socket dup for socket options and disconnect probes; see
//! [`TlsConn`]. The transports meet in the two-variant
//! [`ConnReader`]/[`ConnWriter`] enums, keeping the plain-TCP hot path's
//! direct, lock-free reads and writes (one predictable branch per buffered
//! refill/flush, no dyn dispatch, no mutex).
//!
//! The TLS handshake runs only after the post-accept stop-flag check (so the
//! plain-TCP self-connect shutdown wakeups can never wedge a worker in a
//! handshake) and is bounded by [`TLS_HANDSHAKE_TIMEOUT`] (so a client that
//! connects and never speaks TLS releases its worker).
//!
//! PORT NOTE (TLS disconnect detection): the disconnect watcher
//! (`crate::disconnect_watcher`) and the `flush_chunk` probe operate on the
//! *raw* socket, so on a TLS connection any bytes they observe are encrypted
//! records. That is fine: only `Ok(0)` (EOF) or a hard error flips the
//! disconnect flag — queued records are treated exactly like pipelined
//! plaintext on a plain connection ("still connected"). See the PORT NOTEs at
//! [`ResponseWriter::watch_disconnect`] and in `flush_chunk`.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, BufWriter, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::cgroup::available_cpus;
use crate::flagutil::{ArrayBool, ArrayString, Flag};
use crate::tlsutil::{TlsServerStream, rustls::ServerConfig};

// Read/write buffer sizes tuned for the ingestion path (large POST bodies).
const READ_BUF_SIZE: usize = 64 * 1024;
const WRITE_BUF_SIZE: usize = 16 * 1024;

// Total time an idle keep-alive connection is kept open awaiting the next
// request before it is closed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
// Poll interval used while idly awaiting the next request, so the stop flag is
// observed promptly during graceful shutdown.
const IDLE_POLL: Duration = Duration::from_millis(500);
// Per-read timeout once a request has started arriving. Bounds how long a
// stuck mid-request connection can hold a worker during shutdown.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
// How often the acceptor wakes to re-check the stop flag when idle.
const ACCEPT_POLL: Duration = Duration::from_millis(20);
// Read timeout applied to the socket for the duration of the TLS handshake, so
// a client that connects but never speaks TLS cannot hold a worker for long.
// PORT NOTE: Go's net/http has no default handshake deadline; this bound is
// needed here because a stuck handshake would pin one of the fixed pool of
// worker threads.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

// Mirrors protoparserutil line-reader limits.
const MAX_LINE_SIZE: usize = 256 * 1024;
const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

// snappy has a default limit of ~2GB which is too high for insert requests;
// mirror the Go 56MB cap to prevent memory-allocation attacks.
const MAX_SNAPPY_BLOCK_SIZE: usize = 56_000_000;

// ---------------------------------------------------------------------------
// TLS serving flags (Go lib/httpserver package-level flag vars)
// ---------------------------------------------------------------------------

// Like Go, the flags live in the shared httpserver library, so every binary
// that calls [`serve`] (es-logs, eslagent) gets them. The ported server binds
// a single -httpListenAddr (see the PORT NOTE in the binaries), so only index
// 0 of each array flag is consulted.
static TLS_ENABLE: Flag<ArrayBool> = Flag::new(
    "tls",
    "Whether to enable TLS for incoming HTTP requests at the given -httpListenAddr (aka https). \
     -tlsCertFile and -tlsKeyFile must be set if -tls is set. See also -mtls",
    ArrayBool::default,
);
static TLS_CERT_FILE: Flag<ArrayString> = Flag::new(
    "tlsCertFile",
    "Path to file with TLS certificate for the corresponding -httpListenAddr if -tls is set. \
     Prefer ECDSA certs instead of RSA certs as RSA certs are slower. \
     The provided certificate file is automatically re-read every second, so it can be dynamically updated. \
     See also -tlsAutocertHosts",
    ArrayString::default,
);
static TLS_KEY_FILE: Flag<ArrayString> = Flag::new(
    "tlsKeyFile",
    "Path to file with TLS key for the corresponding -httpListenAddr if -tls is set. \
     The provided key file is automatically re-read every second, so it can be dynamically updated. \
     See also -tlsAutocertHosts",
    ArrayString::default,
);
static TLS_CIPHER_SUITES: Flag<ArrayString> = Flag::new(
    "tlsCipherSuites",
    "Optional list of TLS cipher suites for incoming requests over HTTPS if -tls is set. \
     See the list of supported cipher suites at https://pkg.go.dev/crypto/tls#pkg-constants",
    ArrayString::default,
);
static TLS_MIN_VERSION: Flag<ArrayString> = Flag::new(
    "tlsMinVersion",
    "Optional minimum TLS version to use for the corresponding -httpListenAddr if -tls is set. \
     Supported values: TLS10, TLS11, TLS12, TLS13",
    ArrayString::default,
);

// ---------------------------------------------------------------------------
// Connection transport (plain TCP vs TLS)
// ---------------------------------------------------------------------------

/// Shared handles for one TLS connection.
///
/// A rustls session ([`TlsServerStream`]) is a single-owner state machine
/// that serializes reads and writes, so — unlike the plain-TCP path with its
/// three independent `try_clone` handles — the session is shared behind a
/// mutex between the reader half, the writer half and the mid-handler
/// [`ResponseWriter::flush_chunk`] hook. All of them run on the connection's
/// worker thread, so the lock is always uncontended (the disconnect watcher
/// probes the raw socket dup, never the TLS session).
#[derive(Clone)]
struct TlsConn {
    tls: Arc<Mutex<TlsServerStream>>,
    /// Raw-socket dup used only for disconnect probes (non-blocking peek/read
    /// of the raw byte stream). Byte-level reads on it would observe encrypted
    /// TLS records; it is only ever used for EOF/error probes. Socket options
    /// must NOT be set through it — see [`ConnReader::set_read_timeout`].
    sock: Arc<TcpStream>,
}

/// The read half of a connection. A two-variant enum (not `dyn Read`) so the
/// plain-TCP hot path keeps its direct, lock-free reads: one predictable
/// branch per `BufReader` refill, no dispatch, no mutex.
enum ConnReader {
    Plain(TcpStream),
    Tls(TlsConn),
}

impl ConnReader {
    /// Applies a read timeout to the exact socket handle reads run on.
    ///
    /// PORT NOTE (Windows): for TLS the timeout must be set on the session's
    /// own handle (`StreamOwned.sock`), not the raw-socket dup. On unix the
    /// two `try_clone` handles share `SO_RCVTIMEO` via the file description,
    /// but on Windows duplicated socket handles do not share it — a timeout
    /// set on the dup never fires for reads on the original, which left the
    /// session stuck on the 10s handshake timeout and stalled graceful
    /// shutdown (stop-flag polls woke every 10s instead of every IDLE_POLL).
    /// The mutex lock is uncontended (worker-thread only), same as `read`.
    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            ConnReader::Plain(s) => s.set_read_timeout(dur),
            ConnReader::Tls(c) => c.tls.lock().unwrap().sock.set_read_timeout(dur),
        }
    }
}

impl Read for ConnReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            ConnReader::Plain(s) => s.read(buf),
            ConnReader::Tls(c) => c.tls.lock().unwrap().read(buf),
        }
    }
}

/// The write half of a connection (see [`ConnReader`] for the design).
enum ConnWriter {
    Plain(TcpStream),
    Tls(TlsConn),
}

impl Write for ConnWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            ConnWriter::Plain(s) => s.write(buf),
            ConnWriter::Tls(c) => c.tls.lock().unwrap().write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            ConnWriter::Plain(s) => s.flush(),
            ConnWriter::Tls(c) => c.tls.lock().unwrap().flush(),
        }
    }
}

/// The buffered connection reader all request parsing runs over.
type ConnBufReader = BufReader<ConnReader>;

/// Per-request handle installed on [`ResponseWriter`] for mid-handler
/// streaming (`flush_chunk`) and disconnect watching. Cloning is a refcount
/// bump on the underlying `Arc`s.
#[derive(Clone)]
enum ChunkStream {
    Plain(Arc<TcpStream>),
    Tls(TlsConn),
}

impl ChunkStream {
    fn write_all(&self, buf: &[u8]) -> io::Result<()> {
        match self {
            ChunkStream::Plain(s) => (&**s).write_all(buf),
            ChunkStream::Tls(c) => c.tls.lock().unwrap().write_all(buf),
        }
    }

    fn flush(&self) -> io::Result<()> {
        match self {
            ChunkStream::Plain(s) => (&**s).flush(),
            ChunkStream::Tls(c) => c.tls.lock().unwrap().flush(),
        }
    }

    /// The raw socket, for the disconnect watcher and disconnect probes.
    fn raw_sock(&self) -> &Arc<TcpStream> {
        match self {
            ChunkStream::Plain(s) => s,
            ChunkStream::Tls(c) => &c.sock,
        }
    }
}

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

/// Case-insensitive HTTP header collection preserving insertion order.
#[derive(Default)]
struct Headers {
    // (lowercased-name, value) pairs.
    entries: Vec<(String, String)>,
}

impl Headers {
    fn new() -> Self {
        Headers {
            entries: Vec::new(),
        }
    }

    fn add(&mut self, name: &str, value: String) {
        self.entries.push((name.to_ascii_lowercase(), value));
    }

    /// Returns the first value for `name` (case-insensitive), or `None`.
    fn get(&self, name: &str) -> Option<&str> {
        let lname = name.to_ascii_lowercase();
        self.entries
            .iter()
            .find(|(k, _)| *k == lname)
            .map(|(_, v)| v.as_str())
    }
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// An incoming HTTP request. Borrows the connection reader for streaming body
/// access, so it is tied to the connection lifetime `'a`.
pub struct Request<'a> {
    method: String,
    path: String,
    raw_query: String,
    query: HashMap<String, Vec<String>>,
    post_form: HashMap<String, Vec<String>>,
    headers: Headers,
    remote_addr: String,
    http_1_1: bool,
    body: Body<'a>,
}

impl<'a> Request<'a> {
    /// The request method (e.g. `GET`, `POST`).
    pub fn method(&self) -> &str {
        &self.method
    }

    /// The decoded request path (without the query string).
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The raw (un-decoded) query string, without the leading `?`.
    pub fn raw_query(&self) -> &str {
        &self.raw_query
    }

    /// The remote peer address as `ip:port`.
    pub fn remote_addr(&self) -> &str {
        &self.remote_addr
    }

    /// Returns the first value of the given header (case-insensitive), or `""`.
    pub fn header(&self, name: &str) -> &str {
        self.headers.get(name).unwrap_or("")
    }

    /// `Content-Type` header value, or `""`.
    pub fn content_type(&self) -> &str {
        self.header("content-type")
    }

    /// `Content-Encoding` header value (lowercased), or `""`.
    ///
    /// This is the encoding of the *raw* wire body; [`Request::body_reader`]
    /// already transparently decompresses it, so callers rarely need this.
    pub fn content_encoding(&self) -> String {
        self.header("content-encoding").to_ascii_lowercase()
    }

    /// Parsed `Content-Length`, if present and valid.
    pub fn content_length(&self) -> Option<u64> {
        self.header("content-length").trim().parse::<u64>().ok()
    }

    /// Returns the first form value for `key`: POST-body form values (only
    /// populated for `application/x-www-form-urlencoded` bodies) take
    /// precedence over URL query values, mirroring Go `Request.FormValue`.
    pub fn form_value(&self, key: &str) -> &str {
        if let Some(v) = self.post_form.get(key).and_then(|vs| vs.first()) {
            return v;
        }
        if let Some(v) = self.query.get(key).and_then(|vs| vs.first()) {
            return v;
        }
        ""
    }

    /// Returns all form values for `key`: POST-body form values followed by
    /// URL query values, mirroring Go `Request.Form[key]` after `ParseForm`
    /// (which appends POST body values before URL query values).
    pub fn form_values(&self, key: &str) -> Vec<&str> {
        let mut out: Vec<&str> = Vec::new();
        if let Some(vs) = self.post_form.get(key) {
            out.extend(vs.iter().map(|v| v.as_str()));
        }
        if let Some(vs) = self.query.get(key) {
            out.extend(vs.iter().map(|v| v.as_str()));
        }
        out
    }

    /// The request URI (`path` or `path?query`), used for error logging.
    pub fn request_uri(&self) -> String {
        if self.raw_query.is_empty() {
            self.path.clone()
        } else {
            format!("{}?{}", self.path, self.raw_query)
        }
    }

    /// Returns whether the connection should be kept alive after this request,
    /// per HTTP/1.x defaults and the `Connection` header.
    fn wants_keep_alive(&self) -> bool {
        let conn = self.header("connection").to_ascii_lowercase();
        if self.http_1_1 {
            !conn.split(',').any(|t| t.trim() == "close")
        } else {
            conn.split(',').any(|t| t.trim() == "keep-alive")
        }
    }

    /// A `Read`er over the fully-decompressed request body.
    pub fn body_reader(&mut self) -> &mut dyn Read {
        &mut self.body
    }

    /// Reads the entire decompressed body into a fresh buffer (bulk bytes).
    pub fn read_full_body(&mut self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.body.read_to_end(&mut buf)?;
        Ok(buf)
    }

    /// Reads one block of `\n`-delimited lines from the decompressed body.
    ///
    /// Returns `(dst, tail, eof)`: `dst` holds complete lines (delimiters
    /// stripped at the block boundary), `tail` holds trailing bytes after the
    /// last newline (feed it back on the next call), and `eof` is true once the
    /// body is exhausted. Mirrors `protoparserutil.ReadLinesBlock`.
    pub fn read_line_block(
        &mut self,
        dst_buf: Vec<u8>,
        tail_buf: Vec<u8>,
    ) -> Result<(Vec<u8>, Vec<u8>, bool), String> {
        read_lines_block(&mut self.body, dst_buf, tail_buf)
    }

    /// Consumes any unread body so a keep-alive connection stays byte-aligned.
    /// Returns false if draining failed (the caller should close the conn).
    fn drain_body(&mut self) -> bool {
        io::copy(&mut self.body, &mut io::sink()).is_ok()
    }
}

// ---------------------------------------------------------------------------
// Body / transfer decoding
// ---------------------------------------------------------------------------

/// The raw HTTP message body framing over the connection reader.
enum Transfer<'a> {
    /// `Content-Length`-framed body: exactly N bytes.
    Length(io::Take<&'a mut ConnBufReader>),
    /// `Transfer-Encoding: chunked` body.
    Chunked(ChunkedReader<'a>),
    /// No body.
    Empty,
}

impl Read for Transfer<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Transfer::Length(r) => r.read(buf),
            Transfer::Chunked(r) => r.read(buf),
            Transfer::Empty => Ok(0),
        }
    }
}

/// The decompressed body, layered over a [`Transfer`] per `Content-Encoding`.
/// Mirrors `protoparserutil.GetUncompressedReader`.
enum Body<'a> {
    Plain(Transfer<'a>),
    Gzip(flate2::read::GzDecoder<Transfer<'a>>),
    Deflate(flate2::read::ZlibDecoder<Transfer<'a>>),
    Zstd(::zstd::stream::read::Decoder<'static, BufReader<Transfer<'a>>>),
    /// Fully-buffered decompressed bytes (snappy block mode / consumed forms).
    Buffered(io::Cursor<Vec<u8>>),
}

impl Read for Body<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Body::Plain(r) => r.read(buf),
            Body::Gzip(r) => r.read(buf),
            Body::Deflate(r) => r.read(buf),
            Body::Zstd(r) => r.read(buf),
            Body::Buffered(r) => r.read(buf),
        }
    }
}

/// Streaming reader for `Transfer-Encoding: chunked` bodies.
struct ChunkedReader<'a> {
    inner: &'a mut ConnBufReader,
    remaining: u64,
    done: bool,
}

impl<'a> ChunkedReader<'a> {
    fn new(inner: &'a mut ConnBufReader) -> Self {
        ChunkedReader {
            inner,
            remaining: 0,
            done: false,
        }
    }

    fn read_size_line(&mut self) -> io::Result<u64> {
        let mut line = Vec::new();
        let n = self.inner.read_until(b'\n', &mut line)?;
        if n == 0 {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "eof reading chunk size",
            ));
        }
        let s = String::from_utf8_lossy(&line);
        // A chunk size may carry `;ext` extensions; ignore them.
        let hex = s.trim().split(';').next().unwrap_or("").trim();
        u64::from_str_radix(hex, 16).map_err(|_| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid chunk size: {hex:?}"),
            )
        })
    }

    fn consume_crlf(&mut self) -> io::Result<()> {
        let mut b = [0u8; 2];
        self.inner.read_exact(&mut b)
    }

    fn consume_trailer(&mut self) -> io::Result<()> {
        loop {
            let mut line = Vec::new();
            let n = self.inner.read_until(b'\n', &mut line)?;
            if n == 0 {
                return Ok(());
            }
            let trimmed: &[u8] = line
                .strip_suffix(b"\n")
                .and_then(|l| l.strip_suffix(b"\r").or(Some(l)))
                .unwrap_or(&line);
            if trimmed.is_empty() {
                return Ok(());
            }
        }
    }
}

impl Read for ChunkedReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.done {
            return Ok(0);
        }
        if self.remaining == 0 {
            let size = self.read_size_line()?;
            if size == 0 {
                self.consume_trailer()?;
                self.done = true;
                return Ok(0);
            }
            self.remaining = size;
        }
        let to_read = self.remaining.min(buf.len() as u64) as usize;
        let n = self.inner.read(&mut buf[..to_read])?;
        if n == 0 {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "eof mid chunk"));
        }
        self.remaining -= n as u64;
        if self.remaining == 0 {
            self.consume_crlf()?;
        }
        Ok(n)
    }
}

fn make_transfer(
    reader: &mut ConnBufReader,
    chunked: bool,
    content_length: Option<u64>,
) -> Transfer<'_> {
    if chunked {
        Transfer::Chunked(ChunkedReader::new(reader))
    } else {
        Transfer::Length(reader.take(content_length.unwrap_or(0)))
    }
}

fn wrap_body<'a>(t: Transfer<'a>, encoding: &str) -> io::Result<Body<'a>> {
    Ok(match encoding {
        "" | "none" | "identity" => Body::Plain(t),
        "gzip" => Body::Gzip(flate2::read::GzDecoder::new(t)),
        "deflate" => Body::Deflate(flate2::read::ZlibDecoder::new(t)),
        "zstd" => Body::Zstd(::zstd::stream::read::Decoder::new(t)?),
        "snappy" => {
            // Block-mode snappy must be read in full then decoded in one shot.
            let mut comp = Vec::new();
            let mut lim = t.take(MAX_SNAPPY_BLOCK_SIZE as u64 + 1);
            lim.read_to_end(&mut comp)?;
            if comp.len() > MAX_SNAPPY_BLOCK_SIZE {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "cannot read snappy-encoded data block because its' size exceeds {MAX_SNAPPY_BLOCK_SIZE} bytes"
                    ),
                ));
            }
            let decoded = snap::raw::Decoder::new()
                .decompress_vec(&comp)
                .map_err(|e| {
                    io::Error::new(
                        ErrorKind::InvalidData,
                        format!("cannot decode snappy-encoded data block: {e}"),
                    )
                })?;
            Body::Buffered(io::Cursor::new(decoded))
        }
        other => {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("unsupported contentType: {other}"),
            ));
        }
    })
}

// ---------------------------------------------------------------------------
// ResponseWriter
// ---------------------------------------------------------------------------

/// Buffers a response, flushed after the handler returns.
///
/// PORT NOTE: the Go `responseWriterWithAbort` streams and can hijack/abort the
/// connection after headers are sent. Here the whole response body is buffered
/// and written with an accurate `Content-Length` once, which keeps keep-alive
/// framing trivially correct. Responses in this workload are small.
///
/// The one exception is [`ResponseWriter::flush_chunk`], the minimal streaming
/// hook needed by long-lived endpoints (`/select/logsql/tail`): it switches the
/// response to `Transfer-Encoding: chunked` and pushes the buffered body to the
/// client mid-handler (mirroring Go's `http.Flusher`).
pub struct ResponseWriter {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,

    // Streaming (chunked transfer-encoding) support for flush_chunk. `stream`
    // and `stop` are installed by handle_connection; both stay `None` when the
    // ResponseWriter is constructed outside a server connection (tests).
    stream: Option<ChunkStream>,
    stop: Option<Arc<AtomicBool>>,
    streaming_started: bool,
}

impl Default for ResponseWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseWriter {
    fn new() -> Self {
        ResponseWriter {
            status: 200,
            headers: Vec::new(),
            body: Vec::new(),
            stream: None,
            stop: None,
            streaming_started: false,
        }
    }

    /// Sets the HTTP status code.
    pub fn set_status(&mut self, code: u16) {
        self.status = code;
    }

    /// The current status code.
    pub fn status(&self) -> u16 {
        self.status
    }

    /// Sets a header, replacing any existing value(s) with the same name.
    pub fn set_header(&mut self, name: &str, value: &str) {
        self.headers.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        self.headers.push((name.to_string(), value.to_string()));
    }

    /// Appends raw bytes to the response body.
    pub fn write_bytes(&mut self, data: &[u8]) {
        self.body.extend_from_slice(data);
    }

    /// Appends a string to the response body.
    pub fn write_str(&mut self, s: &str) {
        self.body.extend_from_slice(s.as_bytes());
    }

    /// Writes an error response with the given status, mirroring Go
    /// `http.Error` (plain text, `nosniff`, trailing newline). Replaces body.
    pub fn error(&mut self, msg: &str, status: u16) {
        self.status = status;
        self.set_header("Content-Type", "text/plain; charset=utf-8");
        self.set_header("X-Content-Type-Options", "nosniff");
        self.body.clear();
        self.body.extend_from_slice(msg.as_bytes());
        self.body.push(b'\n');
    }

    /// Mirrors Go `httpserver.Errorf`: logs the error with request context and
    /// writes a `400 Bad Request` error response.
    pub fn errorf(&mut self, req: &Request, msg: &str) {
        let remote = get_quoted_remote_addr(req);
        let uri = req.request_uri();
        crate::warnf!("remoteAddr: {}; requestURI: {}; {}", remote, uri, msg);
        self.error(msg, 400);
    }

    /// Returns true if this response can be streamed with [`Self::flush_chunk`]
    /// (i.e. it is attached to a live server connection).
    pub fn supports_streaming(&self) -> bool {
        self.stream.is_some()
    }

    /// Registers this request's connection with the global
    /// [`crate::disconnect_watcher`] and returns a token whose flag flips when
    /// the client disconnects, letting long-running handlers abort doomed work
    /// (Go's `r.Context().Done()`).
    ///
    /// Returns `None` when no live connection is attached (direct handler
    /// invocation in tests). The token deregisters on drop; buffered handlers
    /// may simply let it fall out of scope — the response bytes are only
    /// written to the socket after the handler returns. Handlers that stream
    /// mid-handler via [`Self::flush_chunk`] must NOT use this (the watcher's
    /// non-blocking probes would race the streaming writes); `flush_chunk`
    /// already probes for disconnects itself.
    ///
    /// PORT NOTE (TLS): the watcher peeks the *raw* socket, so on a TLS
    /// connection any queued bytes it observes are encrypted records (e.g. a
    /// pipelined request). That is fine: only `peek() == Ok(0)` (EOF) or a
    /// hard error flips the cancel flag — queued records are treated exactly
    /// like pipelined plaintext ("still connected"), and `peek` never consumes
    /// them, so the TLS session stays intact.
    pub fn watch_disconnect(&mut self) -> Option<crate::disconnect_watcher::CancelToken> {
        let sock = Arc::clone(self.stream.as_ref()?.raw_sock());
        Some(crate::disconnect_watcher::watch(sock))
    }

    /// Streams the currently buffered body to the client mid-handler as an
    /// HTTP/1.1 chunk (Go `http.Flusher.Flush`).
    ///
    /// The first call sends the status line and headers with
    /// `Transfer-Encoding: chunked` + `Connection: close`; headers set after
    /// that point are ignored. Every call probes the connection so a client
    /// disconnect (or server shutdown) is reported as an error even when there
    /// is no data to write, letting long-lived handlers terminate.
    ///
    /// When no streaming transport is installed (direct handler invocation in
    /// tests), this is a no-op and the body stays buffered for the regular
    /// buffered-response path.
    pub fn flush_chunk(&mut self) -> io::Result<()> {
        let Some(stream) = self.stream.clone() else {
            return Ok(());
        };
        if self.stop.as_ref().is_some_and(|s| s.load(Ordering::SeqCst)) {
            return Err(io::Error::new(ErrorKind::Interrupted, "server is stopping"));
        }

        if !self.streaming_started {
            self.streaming_started = true;
            let mut head = Vec::with_capacity(256);
            write!(
                head,
                "HTTP/1.1 {} {}\r\n",
                self.status,
                reason_phrase(self.status)
            )?;
            for (k, v) in &self.headers {
                if k.eq_ignore_ascii_case("content-length")
                    || k.eq_ignore_ascii_case("connection")
                    || k.eq_ignore_ascii_case("transfer-encoding")
                {
                    continue;
                }
                write!(head, "{k}: {v}\r\n")?;
            }
            head.extend_from_slice(b"Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
            stream.write_all(&head)?;
        }

        // Probe for a client disconnect with a non-blocking read on the raw
        // socket. The request was fully drained before the handler ran, so any
        // readable byte here is either EOF (client gone) or a pipelined
        // request that will never be answered anyway (a streamed response
        // closes the connection).
        //
        // PORT NOTE (TLS): on a TLS connection the consumed byte belongs to an
        // encrypted record (a pipelined request or a close_notify alert).
        // Corrupting the inbound record stream is harmless for the same
        // reason the plain-TCP read is: this connection never reads again and
        // closes when the handler returns; only EOF/error matter here.
        let sock = stream.raw_sock();
        sock.set_nonblocking(true)?;
        let mut probe = [0u8; 1];
        let probe_result = Read::read(&mut (&**sock), &mut probe);
        sock.set_nonblocking(false)?;
        let client_gone = match probe_result {
            Ok(0) => true,
            Ok(_) => false,
            Err(e) if e.kind() == ErrorKind::WouldBlock => false,
            Err(_) => true,
        };
        if client_gone {
            return Err(io::Error::new(ErrorKind::BrokenPipe, "client disconnected"));
        }

        if !self.body.is_empty() {
            let mut chunk = Vec::with_capacity(self.body.len() + 20);
            write!(chunk, "{:x}\r\n", self.body.len())?;
            chunk.extend_from_slice(&self.body);
            chunk.extend_from_slice(b"\r\n");
            stream.write_all(&chunk)?;
            stream.flush()?;
            self.body.clear();
        }
        Ok(())
    }

    fn finish(&mut self, w: &mut impl Write, keep_alive: bool) -> io::Result<()> {
        if self.streaming_started {
            // Streamed (chunked) response: emit any remaining body as a final
            // chunk plus the terminating zero-length chunk. The connection is
            // then closed by the caller (`Connection: close` was already sent
            // with the streamed headers).
            let stream = self
                .stream
                .clone()
                .expect("BUG: streaming_started without a stream");
            let mut tail = Vec::with_capacity(self.body.len() + 25);
            if !self.body.is_empty() {
                write!(tail, "{:x}\r\n", self.body.len())?;
                tail.extend_from_slice(&self.body);
                tail.extend_from_slice(b"\r\n");
            }
            tail.extend_from_slice(b"0\r\n\r\n");
            stream.write_all(&tail)?;
            return stream.flush();
        }
        write!(
            w,
            "HTTP/1.1 {} {}\r\n",
            self.status,
            reason_phrase(self.status)
        )?;
        for (k, v) in &self.headers {
            if k.eq_ignore_ascii_case("content-length") || k.eq_ignore_ascii_case("connection") {
                continue;
            }
            write!(w, "{k}: {v}\r\n")?;
        }
        write!(w, "Content-Length: {}\r\n", self.body.len())?;
        let conn = if keep_alive { "keep-alive" } else { "close" };
        write!(w, "Connection: {conn}\r\n")?;
        w.write_all(b"\r\n")?;
        w.write_all(&self.body)?;
        Ok(())
    }
}

impl Write for ResponseWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.body.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "",
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Handle to a running server, used to observe its address and shut it down.
pub struct ServerHandle {
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
    local_addr: SocketAddr,
}

impl ServerHandle {
    /// The address the server is bound to (useful with port `0`).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Signals shutdown and joins the acceptor and all worker threads.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake every worker's blocking accept() with throwaway self-connects
        // so they observe the stop flag and exit promptly. A worker that is
        // mid-connection notices `stop` in its request-wait loop instead.
        for _ in 0..self.workers.len() {
            let _ = TcpStream::connect(self.local_addr);
        }
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Starts an HTTP/1.1 server on `addr`, dispatching each request to `handler`
/// on a pooled worker thread. Returns once the listener is bound.
///
/// `handler` is invoked as `handler(&mut Request, &mut ResponseWriter)`. Built-in
/// routes (`/health`, `/metrics`, `/-/healthy`, ...) and `OPTIONS`/CORS are
/// handled before the handler; the handler is only called for other requests.
pub fn serve<H>(addr: &str, handler: H) -> io::Result<ServerHandle>
where
    H: Fn(&mut Request, &mut ResponseWriter) + Send + Sync + 'static,
{
    // Build the TLS server config from the -tls* flags (Go does this inside
    // Serve before netutil.NewTCPListener). Go logger.Fatalf's on a bad
    // config; here the error is returned and the binaries fatalf on it, with
    // the same message.
    let tls_cfg = if TLS_ENABLE.get().get_optional_arg(0) {
        let cert_file = TLS_CERT_FILE.get().get_optional_arg(0);
        let key_file = TLS_KEY_FILE.get().get_optional_arg(0);
        let min_version = TLS_MIN_VERSION.get().get_optional_arg(0);
        let cipher_suites: &[String] = TLS_CIPHER_SUITES.get();
        let cfg =
            crate::tlsutil::get_server_tls_config(cert_file, key_file, min_version, cipher_suites)
                .map_err(|err| {
                    io::Error::new(
                        ErrorKind::InvalidInput,
                        format!(
                            "cannot load TLS cert from -tlsCertFile={cert_file:?}, -tlsKeyFile={key_file:?}, -tlsMinVersion={min_version:?}, -tlsCipherSuites={cipher_suites:?}: {err}"
                        ),
                    )
                })?;
        Some(cfg)
    } else {
        None
    };
    serve_with_tls(addr, tls_cfg, handler)
}

/// [`serve`] with an explicit TLS server config (`None` = plain HTTP). Split
/// out so tests can drive the TLS path without touching the process-global
/// flags.
fn serve_with_tls<H>(
    addr: &str,
    tls_cfg: Option<Arc<ServerConfig>>,
    handler: H,
) -> io::Result<ServerHandle>
where
    H: Fn(&mut Request, &mut ResponseWriter) + Send + Sync + 'static,
{
    // Go's net convention: an address of the form ":9428" (or "" ) means "all
    // interfaces on that port". Rust's TcpListener::bind can't resolve a bare
    // ":port", so normalize it to "0.0.0.0:port" (and "" to "0.0.0.0:0").
    let bind_addr = if addr.is_empty() {
        "0.0.0.0:0".to_string()
    } else if let Some(port) = addr.strip_prefix(':') {
        format!("0.0.0.0:{port}")
    } else {
        addr.to_string()
    };
    // Capture the app start time for `esm_app_start_timestamp` /
    // `esm_app_uptime_seconds` (Go does it at package init).
    crate::appmetrics::init_start_time();

    let listener = TcpListener::bind(&bind_addr)?;
    let local_addr = listener.local_addr()?;
    // Blocking listener: accept() returns immediately when a connection arrives,
    // so per-connection latency is not gated by a poll interval. Graceful
    // shutdown wakes the blocked accepts via self-connects (see ServerHandle).
    listener.set_nonblocking(false)?;

    let stop = Arc::new(AtomicBool::new(false));
    let handler = Arc::new(handler);

    // Every worker blocks in accept() on the shared listener directly (the
    // kernel wakes one per connection). Compared to a dedicated acceptor
    // thread + channel handoff this removes one thread wakeup from the
    // fresh-connection latency path. While all workers are busy serving
    // connections, new connections simply queue in the listener backlog.
    let listener = Arc::new(listener);
    let num_workers = available_cpus().max(1);

    let mut workers = Vec::with_capacity(num_workers);
    for i in 0..num_workers {
        let listener = Arc::clone(&listener);
        let handler = Arc::clone(&handler);
        let stop_w = Arc::clone(&stop);
        let tls_cfg = tls_cfg.clone();
        let worker = thread::Builder::new()
            .name(format!("httpserver-worker-{i}"))
            .spawn(move || {
                loop {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            // A shutdown self-connect also lands here; bail
                            // before serving it (and before any TLS handshake,
                            // so the plain-TCP wakeup connects can never wedge
                            // a worker on a TLS listener).
                            if stop_w.load(Ordering::SeqCst) {
                                break;
                            }
                            let _ = stream.set_nonblocking(false);
                            handle_connection(stream, &*handler, &stop_w, tls_cfg.as_ref());
                        }
                        Err(_) => {
                            if stop_w.load(Ordering::SeqCst) {
                                break;
                            }
                            // Transient accept error; brief backoff to avoid a
                            // busy loop, then retry the blocking accept.
                            thread::sleep(ACCEPT_POLL);
                        }
                    }
                }
            })?;
        workers.push(worker);
    }

    Ok(ServerHandle {
        stop,
        workers,
        local_addr,
    })
}

fn handle_connection<H>(
    stream: TcpStream,
    handler: &H,
    stop: &Arc<AtomicBool>,
    tls_cfg: Option<&Arc<ServerConfig>>,
) where
    H: Fn(&mut Request, &mut ResponseWriter),
{
    let _ = stream.set_nodelay(true);

    let remote_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();

    let (conn_reader, conn_writer, chunk_stream) = match tls_cfg {
        None => {
            let read_stream = match stream.try_clone() {
                Ok(s) => s,
                Err(_) => return,
            };
            // Shared handle to the socket for ResponseWriter::flush_chunk
            // streaming (one dup per connection; per-request installation is a
            // refcount bump).
            let chunk = stream
                .try_clone()
                .ok()
                .map(Arc::new)
                .map(ChunkStream::Plain);
            (
                ConnReader::Plain(read_stream),
                ConnWriter::Plain(stream),
                chunk,
            )
        }
        Some(cfg) => {
            // Complete the TLS handshake, bounded by a read timeout so a
            // client that connects and never speaks TLS releases this worker
            // (wait_for_request re-arms the regular timeouts right after).
            let _ = stream.set_read_timeout(Some(TLS_HANDSHAKE_TIMEOUT));
            let sock = match stream.try_clone() {
                Ok(s) => Arc::new(s),
                Err(_) => return,
            };
            let tls = match crate::tlsutil::server_accept(cfg, stream) {
                Ok(s) => s,
                Err(err) => {
                    // Mirror Go's tlsErrorSkipLogger: handshake failures from
                    // peers that just disconnect (health checks, port scans)
                    // or never speak (timeout) are not worth logging. The
                    // os-error substrings cover the Windows spellings of
                    // reset (10054) / would-block (10035) / timeout (10060).
                    if !err.contains("unexpected end of file")
                        && !err.contains("Connection reset")
                        && !err.contains("Resource temporarily unavailable")
                        && !err.contains("timed out")
                        && !err.contains("os error 10054")
                        && !err.contains("os error 10035")
                        && !err.contains("os error 10060")
                    {
                        crate::warnf!("cannot complete TLS handshake with {remote_addr}: {err}");
                    }
                    return;
                }
            };
            let conn = TlsConn {
                tls: Arc::new(Mutex::new(tls)),
                sock,
            };
            (
                ConnReader::Tls(conn.clone()),
                ConnWriter::Tls(conn.clone()),
                Some(ChunkStream::Tls(conn)),
            )
        }
    };

    let mut reader = BufReader::with_capacity(READ_BUF_SIZE, conn_reader);
    let mut writer = BufWriter::with_capacity(WRITE_BUF_SIZE, conn_writer);

    // Perf diagnostic (ESL_HTTP_TIMING=1): per-request read/handle/flush split.
    let timing = std::env::var_os("ESL_HTTP_TIMING").is_some();

    // Idly await each request, polling so `stop` is honored promptly. A `Close`
    // result (EOF, idle timeout, error, or stop) ends the connection.
    while let WaitResult::Ready = wait_for_request(&mut reader, stop) {
        // A request is arriving: use the longer per-read timeout while parsing
        // and while the handler streams the body.
        let _ = reader.get_ref().set_read_timeout(Some(REQUEST_TIMEOUT));
        let t0 = std::time::Instant::now();

        let mut req = match read_request(&mut reader, &remote_addr) {
            Ok(Some(r)) => r,
            Ok(None) => break, // clean EOF: peer closed the connection.
            Err(_) => break,   // malformed request, timeout, or reset.
        };
        let t_read = t0.elapsed();
        let path_owned = if timing {
            req.path().to_string()
        } else {
            String::new()
        };

        requests_total_all().inc();

        let keep_alive_req = req.wants_keep_alive();
        let mut rw = ResponseWriter::new();
        if let Some(cs) = &chunk_stream {
            rw.stream = Some(cs.clone());
            rw.stop = Some(Arc::clone(stop));
        }

        if req.method() == "OPTIONS" {
            enable_cors(&mut rw);
            rw.set_status(204);
        } else if !builtin_routes(&mut req, &mut rw) {
            handler(&mut req, &mut rw);
        }

        // Drain any unread body so the next request is byte-aligned.
        let drained = req.drain_body();
        drop(req); // release the borrow of `reader` for the next iteration.

        // A streamed (chunked) response was sent with `Connection: close`.
        let keep_alive = keep_alive_req && drained && !rw.streaming_started;
        let t_handle = t0.elapsed();
        if rw.finish(&mut writer, keep_alive).is_err() {
            break;
        }
        if writer.flush().is_err() {
            break;
        }
        if timing {
            eprintln!(
                "ESL_HTTP_TIMING path={path_owned} read_us={} handle_us={} flush_us={}",
                t_read.as_micros(),
                (t_handle - t_read).as_micros(),
                (t0.elapsed() - t_handle).as_micros()
            );
        }
        if !keep_alive {
            break;
        }
    }

    // Mirror Go's crypto/tls Conn.Close(): send close_notify before dropping
    // the session so well-behaved clients observe a clean TLS EOF instead of
    // an unexpected-EOF error. Best-effort — the peer may already be gone.
    if let ConnReader::Tls(c) = reader.get_ref() {
        let mut tls = c.tls.lock().unwrap();
        tls.conn.send_close_notify();
        let _ = tls.flush();
    }
}

#[cfg(test)]
impl Request<'static> {
    /// Test-only constructor for a bodyless request with the given method,
    /// target (path + optional `?query`), remote address and headers.
    pub(crate) fn new_test(
        method: &str,
        target: &str,
        remote: &str,
        hdrs: &[(&str, &str)],
    ) -> Self {
        let (path, raw_query) = split_target(target);
        let query = parse_query(&raw_query);
        let mut headers = Headers::new();
        for (k, v) in hdrs {
            headers.add(k, v.to_string());
        }
        Request {
            method: method.to_string(),
            path,
            raw_query,
            query,
            post_form: HashMap::new(),
            headers,
            remote_addr: remote.to_string(),
            http_1_1: true,
            body: Body::Plain(Transfer::Empty),
        }
    }
}

enum WaitResult {
    /// Request bytes are buffered and ready to parse.
    Ready,
    /// The connection should be closed (EOF, idle timeout, error, or stop).
    Close,
}

/// Blocks until the next request begins arriving, polling on a short timeout so
/// the `stop` flag is observed within `IDLE_POLL` during graceful shutdown.
/// Gives up after `IDLE_TIMEOUT` of inactivity.
fn wait_for_request(reader: &mut ConnBufReader, stop: &AtomicBool) -> WaitResult {
    let _ = reader.get_ref().set_read_timeout(Some(IDLE_POLL));
    let deadline = std::time::Instant::now() + IDLE_TIMEOUT;
    loop {
        if stop.load(Ordering::SeqCst) {
            return WaitResult::Close;
        }
        match reader.fill_buf() {
            Ok(&[]) => return WaitResult::Close, // EOF
            Ok(_) => return WaitResult::Ready,
            Err(ref e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                if std::time::Instant::now() >= deadline {
                    return WaitResult::Close;
                }
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(_) => return WaitResult::Close,
        }
    }
}

/// Reads and parses one request head, then constructs a [`Request`] whose body
/// streams from `reader`. Returns `Ok(None)` on a clean EOF before any bytes.
fn read_request<'a>(
    reader: &'a mut ConnBufReader,
    remote_addr: &str,
) -> io::Result<Option<Request<'a>>> {
    let request_line = match read_head_line(reader)? {
        Some(l) if !l.is_empty() => l,
        Some(_) => return Err(io::Error::new(ErrorKind::InvalidData, "empty request line")),
        None => return Ok(None),
    };
    let (method, target, http_1_1) = parse_request_line(&request_line)?;

    let mut headers = Headers::new();
    loop {
        match read_head_line(reader)? {
            Some(l) if l.is_empty() => break,
            Some(l) => {
                if let Some(idx) = l.find(':') {
                    let name = l[..idx].trim();
                    let value = l[idx + 1..].trim().to_string();
                    headers.add(name, value);
                }
            }
            None => return Err(io::Error::new(ErrorKind::UnexpectedEof, "eof in headers")),
        }
    }

    let (path, raw_query) = split_target(&target);
    let query = parse_query(&raw_query);

    let chunked = headers
        .get("transfer-encoding")
        .map(|v| v.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false);
    let content_length = headers
        .get("content-length")
        .and_then(|v| v.trim().parse::<u64>().ok());
    let content_encoding = headers
        .get("content-encoding")
        .unwrap_or("")
        .to_ascii_lowercase();
    let content_type = headers
        .get("content-type")
        .unwrap_or("")
        .to_ascii_lowercase();
    let is_form = matches!(method.as_str(), "POST" | "PUT" | "PATCH")
        && content_type.starts_with("application/x-www-form-urlencoded");
    let has_body = chunked || content_length.map(|n| n > 0).unwrap_or(false);

    let mut post_form = HashMap::new();
    let body = if is_form && has_body {
        // Read the (uncompressed) form body in full and parse it, mirroring
        // Go's FormValue which merges query args with the POST form.
        let mut buf = Vec::new();
        make_transfer(reader, chunked, content_length).read_to_end(&mut buf)?;
        post_form = parse_query(&String::from_utf8_lossy(&buf));
        Body::Buffered(io::Cursor::new(Vec::new()))
    } else if !has_body {
        Body::Plain(Transfer::Empty)
    } else {
        wrap_body(
            make_transfer(reader, chunked, content_length),
            &content_encoding,
        )?
    };

    Ok(Some(Request {
        method,
        path,
        raw_query,
        query,
        post_form,
        headers,
        remote_addr: remote_addr.to_string(),
        http_1_1,
        body,
    }))
}

/// Handles built-in routes; returns true if the request was served.
///
/// PORT NOTE: pprof, favicon bytes and auth (`-metricsAuthKey`,
/// `-flagsAuthKey`, `-httpAuth.*`) are omitted.
fn builtin_routes(req: &mut Request, rw: &mut ResponseWriter) -> bool {
    let path = req.path();
    if path.ends_with("/favicon.ico") {
        favicon_requests().inc();
        rw.set_header("Cache-Control", "max-age=3600");
        return true;
    }
    match path {
        "/health" => {
            rw.set_header("Content-Type", "text/plain; charset=utf-8");
            rw.write_str("OK");
            true
        }
        "/ping" => {
            let status = if req.form_value("verbose") == "true" {
                200
            } else {
                204
            };
            rw.set_status(status);
            true
        }
        "/metrics" => {
            metrics_requests().inc();
            let start_time = std::time::Instant::now();
            rw.set_header("Content-Type", "text/plain; charset=utf-8");
            let mut body = String::new();
            crate::appmetrics::write_prometheus_metrics(&mut body);
            rw.write_str(&body);
            metrics_handler_duration().update_duration(start_time);
            true
        }
        "/flags" => {
            rw.set_header("Content-Type", "text/plain; charset=utf-8");
            let mut buf = Vec::new();
            crate::flagutil::write_flags(&mut buf);
            rw.write_str(&String::from_utf8_lossy(&buf));
            true
        }
        "/-/healthy" => {
            rw.write_str("Softalink LLC is Healthy.\n");
            true
        }
        "/-/ready" => {
            rw.write_str("Softalink LLC is Ready.\n");
            true
        }
        "/robots.txt" => {
            rw.write_str("User-agent: *\nDisallow: /\n");
            true
        }
        _ => false,
    }
}

// Request metrics, mirroring the Go `lib/httpserver` package-level vars
// (rebranded `vm_` -> `esm_`). Registered lazily on first use in the default
// registry set.
fn metrics_requests() -> &'static Arc<crate::metrics::Counter> {
    static C: LazyLock<Arc<crate::metrics::Counter>> = LazyLock::new(|| {
        crate::metrics::new_counter(r#"esm_http_requests_total{path="/metrics"}"#)
    });
    &C
}

fn metrics_handler_duration() -> &'static Arc<crate::metrics::Histogram> {
    static H: LazyLock<Arc<crate::metrics::Histogram>> = LazyLock::new(|| {
        crate::metrics::new_histogram(r#"esm_http_request_duration_seconds{path="/metrics"}"#)
    });
    &H
}

fn favicon_requests() -> &'static Arc<crate::metrics::Counter> {
    static C: LazyLock<Arc<crate::metrics::Counter>> = LazyLock::new(|| {
        crate::metrics::new_counter(r#"esm_http_requests_total{path="*/favicon.ico"}"#)
    });
    &C
}

fn requests_total_all() -> &'static Arc<crate::metrics::Counter> {
    static C: LazyLock<Arc<crate::metrics::Counter>> =
        LazyLock::new(|| crate::metrics::new_counter("esm_http_requests_all_total"));
    &C
}

/// The `esm_http_request_errors_total{path="*", reason="unsupported"}`
/// counter (Go `unsupportedRequestErrors`). Application request handlers
/// increment it when they reject an unrecognized path.
pub fn unsupported_request_errors() -> &'static Arc<crate::metrics::Counter> {
    static C: LazyLock<Arc<crate::metrics::Counter>> = LazyLock::new(|| {
        crate::metrics::new_counter(
            r#"esm_http_request_errors_total{path="*", reason="unsupported"}"#,
        )
    });
    &C
}

/// Enables permissive CORS on the response, mirroring Go `EnableCORS`.
fn enable_cors(rw: &mut ResponseWriter) {
    rw.set_header("Access-Control-Allow-Origin", "*");
    rw.set_header("Access-Control-Allow-Methods", "*");
    rw.set_header("Access-Control-Allow-Headers", "*");
}

/// Returns the quoted remote address, appending `X-Forwarded-For` when present,
/// as a JSON string. Mirrors Go `httpserver.GetQuotedRemoteAddr`.
pub fn get_quoted_remote_addr(req: &Request) -> String {
    let mut remote = req.remote_addr().to_string();
    let xff = req.header("x-forwarded-for");
    if !xff.is_empty() {
        remote.push_str(", X-Forwarded-For: ");
        remote.push_str(xff);
    }
    crate::stringsutil::json_string(&remote)
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Reads one CRLF/LF-terminated line, stripping the trailing newline. Returns
/// `None` if EOF is reached with no bytes read.
fn read_head_line(reader: &mut impl BufRead) -> io::Result<Option<String>> {
    let mut buf = Vec::new();
    let n = reader.read_until(b'\n', &mut buf)?;
    if n == 0 {
        return Ok(None);
    }
    while matches!(buf.last(), Some(b'\n') | Some(b'\r')) {
        buf.pop();
    }
    Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
}

fn parse_request_line(line: &str) -> io::Result<(String, String, bool)> {
    let mut parts = line.split(' ');
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("");
    let version = parts.next().unwrap_or("");
    if method.is_empty() || target.is_empty() || version.is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("malformed request line: {line:?}"),
        ));
    }
    Ok((
        method.to_string(),
        target.to_string(),
        version == "HTTP/1.1",
    ))
}

/// Splits a request target into (decoded path, raw query string).
fn split_target(target: &str) -> (String, String) {
    match target.split_once('?') {
        Some((p, q)) => (percent_decode(p, false), q.to_string()),
        None => (percent_decode(target, false), String::new()),
    }
}

/// Parses a `&`-separated query string into a multimap, percent-decoding keys
/// and values and treating `+` as space.
fn parse_query(raw: &str) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    if raw.is_empty() {
        return out;
    }
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (percent_decode(k, true), percent_decode(v, true)),
            None => (percent_decode(pair, true), String::new()),
        };
        out.entry(k).or_default().push(v);
    }
    out
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Percent-decodes `s`; when `plus_as_space` is set, `+` becomes a space.
fn percent_decode(s: &str, plus_as_space: bool) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => match (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                (Some(h), Some(l)) => {
                    out.push((h << 4) | l);
                    i += 3;
                }
                _ => {
                    out.push(b'%');
                    i += 1;
                }
            },
            b'+' if plus_as_space => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Reads a block of `\n`-delimited lines from `r`, buffering any partial
/// trailing line into `tail`. Mirrors `protoparserutil.ReadLinesBlock`.
///
/// PORT NOTE: Go signals end-of-stream via a returned `io.EOF` error; here EOF
/// is the third tuple element (`bool`). Any leftover bytes are returned in
/// `dst` on the EOF-carrying call first, and the next call reports `eof=true`.
pub fn read_lines_block(
    r: &mut dyn Read,
    mut dst: Vec<u8>,
    mut tail: Vec<u8>,
) -> Result<(Vec<u8>, Vec<u8>, bool), String> {
    if dst.capacity() < DEFAULT_BLOCK_SIZE {
        dst.reserve(DEFAULT_BLOCK_SIZE);
    }
    dst.clear();
    dst.extend_from_slice(&tail);
    tail.clear();

    loop {
        if dst.len() == dst.capacity() {
            let extra = dst.capacity().max(DEFAULT_BLOCK_SIZE);
            dst.reserve(extra);
        }
        let start = dst.len();
        let cap = dst.capacity();
        dst.resize(cap, 0);
        let n = match r.read(&mut dst[start..]) {
            Ok(n) => n,
            Err(e) => {
                dst.truncate(start);
                return Err(format!("cannot read a block of data: {e}"));
            }
        };
        dst.truncate(start + n);

        if n == 0 {
            // EOF. Emit any leftover as a final block; report EOF next call.
            if !dst.is_empty() {
                return Ok((dst, tail, false));
            }
            return Ok((dst, tail, true));
        }

        if let Some(pos) = dst[start..].iter().rposition(|&b| b == b'\n') {
            let nn = start + pos;
            tail.clear();
            tail.extend_from_slice(&dst[nn + 1..]);
            dst.truncate(nn);
            return Ok((dst, tail, false));
        }

        if dst.len() > MAX_LINE_SIZE {
            return Err(format!("too long line: more than {MAX_LINE_SIZE} bytes"));
        }
        // No newline yet: loop to grow the buffer and read more.
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::net::TcpStream;

    // --- httputil helpers over Request -------------------------------------

    #[test]
    fn test_get_request_value_from_query_arg() {
        let r = Request::new_test("POST", "/insert?_msg_field=foo", "1.2.3.4:5", &[]);
        assert_eq!(
            crate::httputil::get_request_value(&r, "_msg_field", "ESL-Msg-Field"),
            "foo"
        );
    }

    #[test]
    fn test_get_request_value_header_fallback() {
        let r = Request::new_test("POST", "/insert", "1.2.3.4:5", &[("ESL-Msg-Field", "bar")]);
        assert_eq!(
            crate::httputil::get_request_value(&r, "_msg_field", "ESL-Msg-Field"),
            "bar"
        );
    }

    #[test]
    fn test_get_array_splits_on_comma() {
        let r = Request::new_test(
            "POST",
            "/insert?_stream_fields=host,app,env",
            "1.2.3.4:5",
            &[],
        );
        let got = crate::httputil::get_array(&r, "_stream_fields", "ESL-Stream-Fields");
        assert_eq!(got, vec!["host", "app", "env"]);

        let r2 = Request::new_test("POST", "/insert", "1.2.3.4:5", &[]);
        assert!(crate::httputil::get_array(&r2, "_stream_fields", "ESL-Stream-Fields").is_empty());
    }

    #[test]
    fn test_get_bool() {
        let mk = |q: &str| Request::new_test("GET", q, "1.2.3.4:5", &[]);
        assert!(!crate::httputil::get_bool(&mk("/x"), "debug"));
        assert!(!crate::httputil::get_bool(&mk("/x?debug=false"), "debug"));
        assert!(!crate::httputil::get_bool(&mk("/x?debug=0"), "debug"));
        assert!(!crate::httputil::get_bool(&mk("/x?debug=NO"), "debug"));
        assert!(crate::httputil::get_bool(&mk("/x?debug=1"), "debug"));
        assert!(crate::httputil::get_bool(&mk("/x?debug=true"), "debug"));
    }

    #[test]
    fn test_get_int() {
        let r = Request::new_test("GET", "/x?limit=42", "1.2.3.4:5", &[]);
        assert_eq!(crate::httputil::get_int(&r, "limit").unwrap(), 42);
        let r0 = Request::new_test("GET", "/x", "1.2.3.4:5", &[]);
        assert_eq!(crate::httputil::get_int(&r0, "limit").unwrap(), 0);
        let rbad = Request::new_test("GET", "/x?limit=abc", "1.2.3.4:5", &[]);
        let err = crate::httputil::get_int(&rbad, "limit").unwrap_err();
        assert!(err.contains("cannot parse integer"), "got: {err}");
    }

    #[test]
    fn test_check_url() {
        assert!(crate::httputil::check_url("http://localhost:8428").is_ok());
        assert!(
            crate::httputil::check_url("")
                .unwrap_err()
                .contains("empty")
        );
    }

    #[test]
    fn test_get_quoted_remote_addr() {
        // Ported from Go TestGetQuotedRemoteAddr.
        let r = Request::new_test("GET", "/", "1.2.3.4", &[]);
        assert_eq!(get_quoted_remote_addr(&r), r#""1.2.3.4""#);

        let r = Request::new_test("GET", "/", "1.2.3.4", &[("X-Forwarded-For", "foo.bar")]);
        assert_eq!(
            get_quoted_remote_addr(&r),
            r#""1.2.3.4, X-Forwarded-For: foo.bar""#
        );
    }

    #[test]
    fn test_query_parsing_and_percent_decoding() {
        let r = Request::new_test("GET", "/q?a=1&a=2&b=hello%20world&c=x+y", "h:1", &[]);
        assert_eq!(r.form_value("a"), "1"); // first value wins
        assert_eq!(r.form_value("b"), "hello world");
        assert_eq!(r.form_value("c"), "x y");
        assert_eq!(r.form_value("missing"), "");
    }

    // --- line reader --------------------------------------------------------

    #[test]
    fn test_read_lines_block() {
        let data = b"line1\nline2\nline3\npartial".to_vec();
        let mut r = Cursor::new(data);
        // Read everything in one block (fits in default block size).
        let (dst, tail, eof) = read_lines_block(&mut r, Vec::new(), Vec::new()).unwrap();
        assert!(!eof);
        assert_eq!(dst, b"line1\nline2\nline3");
        assert_eq!(tail, b"partial");

        // Next call: feed tail back, hit EOF, get the leftover as final block.
        let (dst2, tail2, eof2) = read_lines_block(&mut r, dst, tail).unwrap();
        assert!(!eof2);
        assert_eq!(dst2, b"partial");
        assert!(tail2.is_empty());

        // Final call reports EOF with empty dst.
        let (dst3, _t, eof3) = read_lines_block(&mut r, dst2, tail2).unwrap();
        assert!(eof3);
        assert!(dst3.is_empty());
    }

    #[test]
    fn test_chunked_reader() {
        // "Wikipedia\r\n\r\nin\r\n\r\nchunks." split across chunks.
        let raw = "4\r\nWiki\r\n5\r\npedia\r\nE\r\n in\r\n\r\nchunks.\r\n0\r\n\r\n";
        let stream = tcp_pair_send(raw.as_bytes());
        let mut reader = BufReader::new(ConnReader::Plain(stream));
        let mut cr = ChunkedReader::new(&mut reader);
        let mut out = Vec::new();
        cr.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"Wikipedia in\r\n\r\nchunks.");
    }

    // --- integration over a real loopback socket ---------------------------

    fn start_echo_server() -> ServerHandle {
        serve("127.0.0.1:0", |req, rw| {
            // Echo back the decompressed body.
            let body = req.read_full_body().unwrap_or_default();
            rw.write_bytes(&body);
        })
        .unwrap()
    }

    /// Sends a raw request and returns one parsed response (status_code, body).
    fn request_once(addr: SocketAddr, raw: &[u8]) -> (u16, Vec<u8>) {
        let mut s = TcpStream::connect(addr).unwrap();
        s.write_all(raw).unwrap();
        s.flush().unwrap();
        let mut reader = BufReader::new(s);
        read_response(&mut reader)
    }

    fn read_response<R: Read>(reader: &mut BufReader<R>) -> (u16, Vec<u8>) {
        let status_line = read_head_line(reader).unwrap().unwrap();
        let status: u16 = status_line.split(' ').nth(1).unwrap().parse().unwrap();
        let mut content_length = 0usize;
        loop {
            let line = read_head_line(reader).unwrap().unwrap();
            if line.is_empty() {
                break;
            }
            if let Some((k, v)) = line.split_once(':')
                && k.trim().eq_ignore_ascii_case("content-length")
            {
                content_length = v.trim().parse().unwrap();
            }
        }
        let mut body = vec![0u8; content_length];
        reader.read_exact(&mut body).unwrap();
        (status, body)
    }

    #[test]
    fn test_health_builtin_route() {
        let srv = start_echo_server();
        let addr = srv.local_addr();
        let (status, body) = request_once(
            addr,
            b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(status, 200);
        assert_eq!(body, b"OK");
        srv.stop();
    }

    #[test]
    fn test_query_param_and_header_fallback_roundtrip() {
        let srv = serve("127.0.0.1:0", |req, rw| {
            let v = crate::httputil::get_request_value(req, "_msg_field", "ESL-Msg-Field");
            rw.write_str(&v);
        })
        .unwrap();
        let addr = srv.local_addr();

        let (_, body) = request_once(
            addr,
            b"GET /insert?_msg_field=fromquery HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(body, b"fromquery");

        let (_, body) = request_once(
            addr,
            b"GET /insert HTTP/1.1\r\nHost: x\r\nESL-Msg-Field: fromheader\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(body, b"fromheader");
        srv.stop();
    }

    #[test]
    fn test_post_content_length_body() {
        let srv = start_echo_server();
        let addr = srv.local_addr();
        let payload = b"{\"_msg\":\"hello\"}\n{\"_msg\":\"world\"}\n";
        let raw = format!(
            "POST /insert/jsonline HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-ndjson\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload.len()
        );
        let mut req = raw.into_bytes();
        req.extend_from_slice(payload);
        let (status, body) = request_once(addr, &req);
        assert_eq!(status, 200);
        assert_eq!(body, payload);
        srv.stop();
    }

    #[test]
    fn test_chunked_request_body() {
        let srv = start_echo_server();
        let addr = srv.local_addr();
        let raw = "POST /insert HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
                   4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n";
        let (status, body) = request_once(addr, raw.as_bytes());
        assert_eq!(status, 200);
        assert_eq!(body, b"Wikipedia");
        srv.stop();
    }

    #[test]
    fn test_flush_chunk_streaming_response() {
        let srv = serve("127.0.0.1:0", |_req, rw| {
            assert!(rw.supports_streaming());
            rw.set_header("Content-Type", "application/x-ndjson");
            rw.write_str("first\n");
            rw.flush_chunk().unwrap();
            rw.write_str("second\n");
            rw.flush_chunk().unwrap();
            // Left buffered: finish() must emit it as the final chunk.
            rw.write_str("tail\n");
        })
        .unwrap();
        let addr = srv.local_addr();

        let mut s = TcpStream::connect(addr).unwrap();
        s.write_all(b"GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        let mut raw = Vec::new();
        // The server closes the connection after a streamed response.
        s.read_to_end(&mut raw).unwrap();
        let text = String::from_utf8(raw).unwrap();
        let (head, body) = text.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("HTTP/1.1 200 OK"), "head={head}");
        assert!(head.contains("Transfer-Encoding: chunked"), "head={head}");
        assert!(head.contains("Connection: close"), "head={head}");
        assert!(
            head.contains("Content-Type: application/x-ndjson"),
            "head={head}"
        );
        assert!(
            !head.to_ascii_lowercase().contains("content-length"),
            "head={head}"
        );
        assert_eq!(
            body,
            "6\r\nfirst\n\r\n7\r\nsecond\n\r\n5\r\ntail\n\r\n0\r\n\r\n"
        );
        srv.stop();
    }

    #[test]
    fn test_flush_chunk_detects_client_disconnect() {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let tx = std::sync::Mutex::new(tx);
        let srv = serve("127.0.0.1:0", move |_req, rw| {
            loop {
                rw.write_str("tick\n");
                if rw.flush_chunk().is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(5));
            }
            let _ = tx.lock().unwrap().send(());
        })
        .unwrap();
        let addr = srv.local_addr();

        {
            let mut s = TcpStream::connect(addr).unwrap();
            s.write_all(b"GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")
                .unwrap();
            let mut first = [0u8; 16];
            let n = s.read(&mut first).unwrap();
            assert!(n > 0, "expected streamed bytes before disconnecting");
            // Dropping the stream closes the connection.
        }

        rx.recv_timeout(Duration::from_secs(10))
            .expect("handler must observe the client disconnect and return");
        srv.stop();
    }

    fn post_compressed(addr: SocketAddr, encoding: &str, compressed: &[u8]) -> Vec<u8> {
        let raw = format!(
            "POST /insert HTTP/1.1\r\nHost: x\r\nContent-Encoding: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            encoding,
            compressed.len()
        );
        let mut req = raw.into_bytes();
        req.extend_from_slice(compressed);
        request_once(addr, &req).1
    }

    #[test]
    fn test_gzip_decompression_roundtrip() {
        let srv = start_echo_server();
        let addr = srv.local_addr();
        let plain = b"the quick brown fox jumps over the lazy dog\n".repeat(50);
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(&plain).unwrap();
        let compressed = enc.finish().unwrap();
        assert_eq!(post_compressed(addr, "gzip", &compressed), plain);
        srv.stop();
    }

    #[test]
    fn test_zstd_decompression_roundtrip() {
        let srv = start_echo_server();
        let addr = srv.local_addr();
        let plain = b"zstandard payload line\n".repeat(100);
        let compressed = ::zstd::stream::encode_all(&plain[..], 3).unwrap();
        assert_eq!(post_compressed(addr, "zstd", &compressed), plain);
        srv.stop();
    }

    #[test]
    fn test_snappy_decompression_roundtrip() {
        let srv = start_echo_server();
        let addr = srv.local_addr();
        let plain = b"snappy block payload\n".repeat(80);
        let compressed = snap::raw::Encoder::new().compress_vec(&plain).unwrap();
        assert_eq!(post_compressed(addr, "snappy", &compressed), plain);
        srv.stop();
    }

    #[test]
    fn test_keep_alive_two_requests() {
        let srv = start_echo_server();
        let addr = srv.local_addr();
        let mut s = TcpStream::connect(addr).unwrap();

        // Two pipelined keep-alive requests over one connection.
        let mk = |body: &str| {
            format!(
                "POST /insert HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                body.len(),
                body
            )
        };
        s.write_all(mk("first").as_bytes()).unwrap();
        s.write_all(mk("second").as_bytes()).unwrap();
        s.flush().unwrap();

        let mut reader = BufReader::new(s);
        let (st1, b1) = read_response(&mut reader);
        let (st2, b2) = read_response(&mut reader);
        assert_eq!((st1, st2), (200, 200));
        assert_eq!(b1, b"first");
        assert_eq!(b2, b"second");
        srv.stop();
    }

    // Sends `data` over a loopback connection and returns the receiving stream,
    // so body-reader internals can be exercised against a real TcpStream.
    fn tcp_pair_send(data: &[u8]) -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let data = data.to_vec();
        let sender = thread::spawn(move || {
            let mut c = TcpStream::connect(addr).unwrap();
            c.write_all(&data).unwrap();
            // Keep the socket open briefly so the reader sees a clean EOF.
            c.shutdown(std::net::Shutdown::Write).unwrap();
        });
        let (stream, _) = listener.accept().unwrap();
        sender.join().unwrap();
        stream
    }

    // --- TLS serving ---------------------------------------------------------

    use crate::tlsutil;

    /// A self-signed cert/key pair for `localhost`/`127.0.0.1` written to temp
    /// PEM files (the server-side config takes file paths, like Go).
    struct TlsFixture {
        cert_path: String,
        key_path: String,
    }

    fn make_tls_fixture(tag: &str) -> TlsFixture {
        let ck = rcgen::generate_simple_self_signed(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .unwrap();
        let dir =
            std::env::temp_dir().join(format!("esl-httpserver-tls-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("cert.pem").to_string_lossy().into_owned();
        let key_path = dir.join("key.pem").to_string_lossy().into_owned();
        std::fs::write(&cert_path, ck.cert.pem()).unwrap();
        std::fs::write(&key_path, ck.key_pair.serialize_pem()).unwrap();
        TlsFixture {
            cert_path,
            key_path,
        }
    }

    /// Starts the real server with TLS enabled (the same code path `serve`
    /// takes when the -tls flags are set; only the flag lookup is bypassed —
    /// flags are process-global and cannot be set per-test).
    fn serve_tls<H>(fx: &TlsFixture, handler: H) -> ServerHandle
    where
        H: Fn(&mut Request, &mut ResponseWriter) + Send + Sync + 'static,
    {
        let cfg =
            crate::tlsutil::get_server_tls_config(&fx.cert_path, &fx.key_path, "", &[]).unwrap();
        serve_with_tls("127.0.0.1:0", Some(cfg), handler).unwrap()
    }

    /// Connects a TLS client (via tlsutil, trusting the fixture cert as CA)
    /// and completes the handshake.
    fn tls_connect(fx: &TlsFixture, addr: SocketAddr) -> tlsutil::TlsClientStream {
        let cfg = tlsutil::new_tls_client_config(&tlsutil::TLSConfig {
            ca_file: fx.cert_path.clone(),
            ..Default::default()
        })
        .unwrap();
        let tcp = TcpStream::connect(addr).unwrap();
        tlsutil::client_connect(&cfg, "localhost", tcp).unwrap()
    }

    #[test]
    fn test_tls_health_round_trip() {
        let fx = make_tls_fixture("health");
        let srv = serve_tls(&fx, |req, rw| {
            let body = req.read_full_body().unwrap_or_default();
            rw.write_bytes(&body);
        });
        let addr = srv.local_addr();

        let mut s = tls_connect(&fx, addr);
        s.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .unwrap();
        s.flush().unwrap();
        let mut reader = BufReader::new(s);
        let (status, body) = read_response(&mut reader);
        assert_eq!(status, 200);
        assert_eq!(body, b"OK");
        srv.stop();
    }

    #[test]
    fn test_tls_post_body_round_trip() {
        let fx = make_tls_fixture("post");
        let srv = serve_tls(&fx, |req, rw| {
            let body = req.read_full_body().unwrap_or_default();
            rw.write_bytes(&body);
        });
        let addr = srv.local_addr();

        let payload = b"{\"_msg\":\"hello\"}\n{\"_msg\":\"world\"}\n";
        let raw = format!(
            "POST /insert/jsonline HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload.len()
        );
        let mut s = tls_connect(&fx, addr);
        s.write_all(raw.as_bytes()).unwrap();
        s.write_all(payload).unwrap();
        s.flush().unwrap();
        let mut reader = BufReader::new(s);
        let (status, body) = read_response(&mut reader);
        assert_eq!(status, 200);
        assert_eq!(body, payload);
        srv.stop();
    }

    #[test]
    fn test_tls_keep_alive_two_requests() {
        let fx = make_tls_fixture("keepalive");
        let srv = serve_tls(&fx, |req, rw| {
            let body = req.read_full_body().unwrap_or_default();
            rw.write_bytes(&body);
        });
        let addr = srv.local_addr();

        let mut s = tls_connect(&fx, addr);
        let mk = |body: &str| {
            format!(
                "POST /insert HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                body.len(),
                body
            )
        };
        s.write_all(mk("first").as_bytes()).unwrap();
        s.write_all(mk("second").as_bytes()).unwrap();
        s.flush().unwrap();

        let mut reader = BufReader::new(s);
        let (st1, b1) = read_response(&mut reader);
        let (st2, b2) = read_response(&mut reader);
        assert_eq!((st1, st2), (200, 200));
        assert_eq!(b1, b"first");
        assert_eq!(b2, b"second");
        srv.stop();
    }

    #[test]
    fn test_tls_flush_chunk_streaming_response() {
        let fx = make_tls_fixture("stream");
        let srv = serve_tls(&fx, |_req, rw| {
            assert!(rw.supports_streaming());
            rw.set_header("Content-Type", "application/x-ndjson");
            rw.write_str("first\n");
            rw.flush_chunk().unwrap();
            rw.write_str("second\n");
            rw.flush_chunk().unwrap();
            // Left buffered: finish() must emit it as the final chunk.
            rw.write_str("tail\n");
        });
        let addr = srv.local_addr();

        let mut s = tls_connect(&fx, addr);
        s.write_all(b"GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        s.flush().unwrap();
        let mut raw = Vec::new();
        // The server sends close_notify after a streamed response, so the
        // TLS read loop terminates with a clean EOF.
        s.read_to_end(&mut raw).unwrap();
        let text = String::from_utf8(raw).unwrap();
        let (head, body) = text.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("HTTP/1.1 200 OK"), "head={head}");
        assert!(head.contains("Transfer-Encoding: chunked"), "head={head}");
        assert!(head.contains("Connection: close"), "head={head}");
        assert_eq!(
            body,
            "6\r\nfirst\n\r\n7\r\nsecond\n\r\n5\r\ntail\n\r\n0\r\n\r\n"
        );
        srv.stop();
    }

    #[test]
    fn test_tls_flush_chunk_detects_client_disconnect() {
        let fx = make_tls_fixture("stream-disconnect");
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let tx = std::sync::Mutex::new(tx);
        let srv = serve_tls(&fx, move |_req, rw| {
            loop {
                rw.write_str("tick\n");
                if rw.flush_chunk().is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(5));
            }
            let _ = tx.lock().unwrap().send(());
        });
        let addr = srv.local_addr();

        {
            let mut s = tls_connect(&fx, addr);
            s.write_all(b"GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")
                .unwrap();
            s.flush().unwrap();
            let mut first = [0u8; 16];
            let n = s.read(&mut first).unwrap();
            assert!(n > 0, "expected streamed bytes before disconnecting");
            // Dropping the stream closes the raw connection (no close_notify).
        }

        rx.recv_timeout(Duration::from_secs(10))
            .expect("handler must observe the client disconnect and return");
        srv.stop();
    }

    #[test]
    fn test_tls_disconnect_watcher_flips_on_client_close() {
        let fx = make_tls_fixture("watcher");
        let (tx, rx) = std::sync::mpsc::channel::<bool>();
        let tx = std::sync::Mutex::new(tx);
        let srv = serve_tls(&fx, move |_req, rw| {
            let token = rw.watch_disconnect().expect("live connection");
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while !token.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            let _ = tx.lock().unwrap().send(token.load(Ordering::SeqCst));
        });
        let addr = srv.local_addr();

        {
            let mut s = tls_connect(&fx, addr);
            s.write_all(b"GET /watch HTTP/1.1\r\nHost: x\r\n\r\n")
                .unwrap();
            s.flush().unwrap();
            // Give the request time to reach the handler, then disconnect
            // abruptly: the watcher's raw-socket peek must observe the EOF.
            thread::sleep(Duration::from_millis(200));
        }

        assert!(
            rx.recv_timeout(Duration::from_secs(10))
                .expect("handler must finish"),
            "cancel token must flip after the TLS client disconnects"
        );
        srv.stop();
    }

    #[test]
    fn test_tls_garbage_and_eof_do_not_kill_accept_loop() {
        let fx = make_tls_fixture("garbage");
        let srv = serve_tls(&fx, |req, rw| {
            let body = req.read_full_body().unwrap_or_default();
            rw.write_bytes(&body);
        });
        let addr = srv.local_addr();

        // A plain-TCP probe that connects and immediately closes (kubernetes
        // health-check style) must not take a worker down.
        drop(TcpStream::connect(addr).unwrap());

        // A plain-TCP client sending non-TLS bytes fails the handshake; the
        // worker must drop the connection and keep accepting.
        {
            let mut s = TcpStream::connect(addr).unwrap();
            s.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
            let mut buf = [0u8; 64];
            let _ = s.read(&mut buf); // server closes (possibly after a TLS alert)
        }

        // A real TLS request still succeeds afterwards.
        let mut s = tls_connect(&fx, addr);
        s.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .unwrap();
        s.flush().unwrap();
        let mut reader = BufReader::new(s);
        let (status, body) = read_response(&mut reader);
        assert_eq!(status, 200);
        assert_eq!(body, b"OK");
        srv.stop();
    }

    #[test]
    fn test_tls_graceful_shutdown() {
        let fx = make_tls_fixture("shutdown");
        let srv = serve_tls(&fx, |req, rw| {
            let body = req.read_full_body().unwrap_or_default();
            rw.write_bytes(&body);
        });
        let addr = srv.local_addr();

        // Park one worker on an idle keep-alive TLS connection so shutdown
        // exercises both the wait_for_request stop check and the plain-TCP
        // self-connect accept wakeups (which never handshake).
        let mut s = tls_connect(&fx, addr);
        s.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n")
            .unwrap();
        s.flush().unwrap();
        let mut reader = BufReader::new(s);
        let (status, _) = read_response(&mut reader);
        assert_eq!(status, 200);

        let t0 = std::time::Instant::now();
        srv.stop();
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "graceful shutdown with TLS enabled must not wedge"
        );
    }
}

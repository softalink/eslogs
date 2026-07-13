//! Rust port of the EsLogs benchmark log generator
//! (`app/eslogsgenerator/main.go` in the Go source, tag v1.51.0).
//!
//! Generates JSON log lines spread over `-start`..`-end` for `-totalStreams`
//! log streams (`-activeStreams` at a time) and either prints them to stdout
//! or streams them to the `-addr` ingestion URL from `-workers` parallel
//! workers, publishing throughput stats every `-statInterval`.

use std::fmt;
use std::io::{BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use esl_common::flagutil::Flag;
use esl_common::flagutil::FlagValue;
use esl_common::flagutil::duration::parse_go_duration;
use esl_common::{buildinfo, envflag, fatalf, infof, logger, panicf, timeutil};

static ADDR: Flag<String> = Flag::new(
    "addr",
    "HTTP address to push the generated logs to; if it is set to stdout, then logs are generated to stdout",
    || "stdout".to_string(),
);
static WORKERS: Flag<i64> = Flag::new(
    "workers",
    "The number of workers to use to push logs to -addr",
    || 1,
);

static START: Flag<TimeFlag> = Flag::new(
    "start",
    "Generated logs start from this time; see https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#timestamp-formats",
    || TimeFlag::must_parse("-1d"),
);
static END: Flag<TimeFlag> = Flag::new(
    "end",
    "Generated logs end at this time; see https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#timestamp-formats",
    || TimeFlag::must_parse("0s"),
);
static ACTIVE_STREAMS: Flag<i64> = Flag::new(
    "activeStreams",
    "The number of active log streams to generate; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields",
    || 100,
);
static TOTAL_STREAMS: Flag<i64> = Flag::new(
    "totalStreams",
    "The number of total log streams; if -totalStreams > -activeStreams, then some active streams are substituted with new streams during data generation",
    || 0,
);
static LOGS_PER_STREAM: Flag<i64> = Flag::new(
    "logsPerStream",
    "The number of log entries to generate per each log stream. Log entries are evenly distributed between -start and -end",
    || 1_000,
);
static CONST_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "constFieldsPerLog",
    "The number of fields with constant values to generate per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 3,
);
static VAR_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "varFieldsPerLog",
    "The number of fields with variable values to generate per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 1,
);
static DICT_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "dictFieldsPerLog",
    "The number of fields with up to 8 different values to generate per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 2,
);
static U8_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "u8FieldsPerLog",
    "The number of fields with uint8 values to generate per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 1,
);
static U16_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "u16FieldsPerLog",
    "The number of fields with uint16 values to generate per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 1,
);
static U32_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "u32FieldsPerLog",
    "The number of fields with uint32 values to generate per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 1,
);
static U64_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "u64FieldsPerLog",
    "The number of fields with uint64 values to generate per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 1,
);
static I64_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "i64FieldsPerLog",
    "The number of fields with int64 values to generate per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 1,
);
static FLOAT_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "floatFieldsPerLog",
    "The number of fields with float64 values to generate per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 1,
);
static IP_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "ipFieldsPerLog",
    "The number of fields with IPv4 values to generate per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 1,
);
static TIMESTAMP_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "timestampFieldsPerLog",
    "The number of fields with ISO8601 timestamps per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 1,
);
static JSON_FIELDS_PER_LOG: Flag<i64> = Flag::new(
    "jsonFieldsPerLog",
    "The number of JSON fields to generate per each log entry; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model",
    || 1,
);

static STAT_INTERVAL: Flag<DurationFlag> = Flag::new(
    "statInterval",
    "The interval between publishing the stats",
    || DurationFlag {
        nanos: 10_000_000_000,
    },
);

fn main() {
    // PORT NOTE: Go redirects the flag help output to stdout via
    // `flag.CommandLine.SetOutput(os.Stdout)`; the ported flag layer has no
    // central usage hook yet (see the es-logs binary).
    envflag::parse();
    buildinfo::init();
    logger::init();

    let addr = ADDR.get().clone();
    let remote_write_url = if addr != "stdout" {
        match RemoteWriteUrl::parse(&addr) {
            Ok(u) => Some(u),
            Err(err) => {
                fatalf!("cannot parse -addr={addr:?}: {err}");
                unreachable!()
            }
        }
    } else {
        None
    };

    let start = START.get();
    let end = END.get();
    if start.nsec >= end.nsec {
        fatalf!("-start={start} must be smaller than -end={end}");
    }
    let active_streams = *ACTIVE_STREAMS.get();
    if active_streams <= 0 {
        fatalf!("-activeStreams must be bigger than 0; got {active_streams}");
    }
    let logs_per_stream = *LOGS_PER_STREAM.get();
    if logs_per_stream <= 0 {
        fatalf!("-logsPerStream must be bigger than 0; got {logs_per_stream}");
    }
    let mut total_streams = *TOTAL_STREAMS.get();
    if total_streams < active_streams {
        total_streams = active_streams;
    }

    // divide total and active streams among workers
    let workers = *WORKERS.get();
    if workers <= 0 {
        fatalf!("-workers must be bigger than 0; got {workers}");
    }
    if workers > active_streams {
        fatalf!("-workers={workers} cannot exceed -activeStreams={active_streams}");
    }

    // PORT NOTE: Go reads the *FieldsPerLog flags directly from the generator
    // goroutines; this port snapshots them into a config struct so the pure
    // line-generation helpers can be unit-tested without global flag state.
    let mut seed_rng = Rng::new(seed_from_time());
    let cfg = Arc::new(WorkerConfig {
        url: remote_write_url,
        active_streams: active_streams / workers,
        total_streams: total_streams / workers,
        start_nsec: start.nsec,
        end_nsec: end.nsec,
        logs_per_stream,
        fields: FieldsConfig::from_flags(),
        run_id: to_uuid(seed_rng.uint64(), seed_rng.uint64()),
    });

    infof!(
        "start -workers={workers} workers for ingesting -logsPerStream={logs_per_stream} log entries per each -totalStreams={total_streams} (-activeStreams={active_streams}) on a time range -start={}, -end={} to -addr={addr}",
        to_rfc3339(start.nsec),
        to_rfc3339(end.nsec)
    );

    let start_time = Instant::now();
    let mut handles = Vec::new();
    for worker_id in 0..workers {
        let cfg = Arc::clone(&cfg);
        // PORT NOTE: Go workers share the locked `math/rand` global source;
        // this port gives every worker its own deterministic PRNG (seeded from
        // the time-based seed) to avoid cross-thread locking.
        let seed = seed_rng.uint64();
        handles.push(std::thread::spawn(move || {
            generate_and_push_logs(&cfg, worker_id, Rng::new(seed));
        }));
    }

    let stat_interval_nanos = STAT_INTERVAL.get().nanos;
    if stat_interval_nanos <= 0 {
        // Mirrors the Go `time.NewTicker` panic on non-positive intervals.
        panicf!("non-positive interval for -statInterval ticker");
    }
    let stat_interval_secs = stat_interval_nanos as f64 / 1e9;
    std::thread::spawn(move || {
        let mut prev_entries = 0u64;
        let mut prev_bytes = 0u64;
        loop {
            std::thread::sleep(Duration::from_nanos(stat_interval_nanos as u64));
            let curr_entries = LOG_ENTRIES_COUNT.load(Ordering::Relaxed);
            let delta_entries = curr_entries - prev_entries;
            let rate_entries = delta_entries as f64 / stat_interval_secs;

            let curr_bytes = BYTES_GENERATED.load(Ordering::Relaxed);
            let delta_bytes = curr_bytes - prev_bytes;
            let rate_bytes = delta_bytes as f64 / stat_interval_secs;
            infof!(
                "generated {}K log entries ({}K total) at {:.0}K entries/sec, {}MB ({}MB total) at {:.0}MB/sec",
                delta_entries / 1_000,
                curr_entries / 1_000,
                rate_entries / 1e3,
                delta_bytes / 1_000_000,
                curr_bytes / 1_000_000,
                rate_bytes / 1e6
            );

            prev_entries = curr_entries;
            prev_bytes = curr_bytes;
        }
    });

    for h in handles {
        h.join().expect("worker thread panicked");
    }

    let d_secs = start_time.elapsed().as_secs_f64();
    let curr_entries = LOG_ENTRIES_COUNT.load(Ordering::Relaxed);
    let curr_bytes = BYTES_GENERATED.load(Ordering::Relaxed);
    let rate_entries = curr_entries as f64 / d_secs;
    let rate_bytes = curr_bytes as f64 / d_secs;
    infof!(
        "ingested {}K log entries ({}MB) in {d_secs:.3} seconds; avg ingestion rate: {:.0}K entries/sec, {:.0}MB/sec",
        curr_entries / 1_000,
        curr_bytes / 1_000_000,
        rate_entries / 1e3,
        rate_bytes / 1e6
    );
}

static LOG_ENTRIES_COUNT: AtomicU64 = AtomicU64::new(0);

static BYTES_GENERATED: AtomicU64 = AtomicU64::new(0);

struct WorkerConfig {
    url: Option<RemoteWriteUrl>,
    active_streams: i64,
    total_streams: i64,
    start_nsec: i64,
    end_nsec: i64,
    logs_per_stream: i64,
    fields: FieldsConfig,
    run_id: String,
}

/// Per-log field counts, snapshotted from the `*FieldsPerLog` flags.
struct FieldsConfig {
    const_fields_per_log: i64,
    var_fields_per_log: i64,
    dict_fields_per_log: i64,
    u8_fields_per_log: i64,
    u16_fields_per_log: i64,
    u32_fields_per_log: i64,
    u64_fields_per_log: i64,
    i64_fields_per_log: i64,
    float_fields_per_log: i64,
    ip_fields_per_log: i64,
    timestamp_fields_per_log: i64,
    json_fields_per_log: i64,
}

impl FieldsConfig {
    fn from_flags() -> FieldsConfig {
        FieldsConfig {
            const_fields_per_log: *CONST_FIELDS_PER_LOG.get(),
            var_fields_per_log: *VAR_FIELDS_PER_LOG.get(),
            dict_fields_per_log: *DICT_FIELDS_PER_LOG.get(),
            u8_fields_per_log: *U8_FIELDS_PER_LOG.get(),
            u16_fields_per_log: *U16_FIELDS_PER_LOG.get(),
            u32_fields_per_log: *U32_FIELDS_PER_LOG.get(),
            u64_fields_per_log: *U64_FIELDS_PER_LOG.get(),
            i64_fields_per_log: *I64_FIELDS_PER_LOG.get(),
            float_fields_per_log: *FLOAT_FIELDS_PER_LOG.get(),
            ip_fields_per_log: *IP_FIELDS_PER_LOG.get(),
            timestamp_fields_per_log: *TIMESTAMP_FIELDS_PER_LOG.get(),
            json_fields_per_log: *JSON_FIELDS_PER_LOG.get(),
        }
    }
}

/// Counts the bytes passing through to the inner writer, like Go `statWriter`.
struct StatWriter<W: Write> {
    w: W,
}

impl<W: Write> Write for StatWriter<W> {
    fn write(&mut self, p: &[u8]) -> std::io::Result<usize> {
        BYTES_GENERATED.fetch_add(p.len() as u64, Ordering::Relaxed);
        self.w.write_all(p)?;
        Ok(p.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

// PORT NOTE: Go pipes a generator goroutine into `http.Client.Do` via
// `io.Pipe`, which the client sends with chunked transfer encoding. This
// std-only port streams the generated bytes directly into the request body of
// a raw HTTP/1.1 connection using the same chunked encoding, so no pipe or
// extra thread is needed. The 1MB write buffer is kept: it reduces the number
// of send() syscalls (and here also yields ~1MB chunks).
fn generate_and_push_logs(cfg: &WorkerConfig, worker_id: i64, mut rng: Rng) {
    let Some(url) = &cfg.url else {
        let sw = StatWriter {
            w: std::io::stdout(),
        };
        let mut bw = BufWriter::with_capacity(1024 * 1024, sw);
        let res = generate_logs(&mut bw, cfg, worker_id, &mut rng).and_then(|()| bw.flush());
        if let Err(err) = res {
            fatalf!("unexpected error when writing logs to stdout: {err}");
        }
        return;
    };

    let mut stream = match TcpStream::connect((url.host.as_str(), url.port)) {
        Ok(s) => s,
        Err(err) => {
            fatalf!("cannot perform request to \"{url}\": {err}");
            unreachable!()
        }
    };
    let res = (|| -> std::io::Result<()> {
        stream.set_nodelay(true)?;
        write!(
            stream,
            "POST {} HTTP/1.1\r\nHost: {}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            url.request_uri, url.host
        )?;
        {
            let sw = StatWriter {
                w: ChunkedWriter { stream: &stream },
            };
            let mut bw = BufWriter::with_capacity(1024 * 1024, sw);
            generate_logs(&mut bw, cfg, worker_id, &mut rng)?;
            bw.flush()?;
        }
        // Terminating zero-length chunk.
        stream.write_all(b"0\r\n\r\n")?;
        Ok(())
    })();
    if let Err(err) = res {
        fatalf!("cannot perform request to \"{url}\": {err}");
    }

    let status_code = match read_response_status(&mut stream) {
        Ok(c) => c,
        Err(err) => {
            fatalf!("cannot perform request to \"{url}\": {err}");
            unreachable!()
        }
    };
    if status_code / 100 != 2 {
        fatalf!("unexpected status code got from \"{url}\": {status_code}; want 2xx");
    }
}

/// Writes each buffer as one HTTP/1.1 chunk (`Transfer-Encoding: chunked`).
struct ChunkedWriter<'a> {
    stream: &'a TcpStream,
}

impl Write for ChunkedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut s = self.stream;
        write!(s, "{:x}\r\n", buf.len())?;
        s.write_all(buf)?;
        s.write_all(b"\r\n")?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut s = self.stream;
        s.flush()
    }
}

/// Reads the whole HTTP response (the request was sent with
/// `Connection: close`) and returns its status code.
fn read_response_status(stream: &mut TcpStream) -> Result<u16, String> {
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .map_err(|err| format!("cannot read response: {err}"))?;
    let line_end = buf
        .windows(2)
        .position(|w| w == b"\r\n")
        .ok_or("missing status line in response")?;
    let status_line = String::from_utf8_lossy(&buf[..line_end]).to_string();
    status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or(format!("cannot parse status line {status_line:?}"))
}

fn generate_logs<W: Write>(
    bw: &mut W,
    cfg: &WorkerConfig,
    worker_id: i64,
    rng: &mut Rng,
) -> std::io::Result<()> {
    let range = cfg.end_nsec - cfg.start_nsec;
    let stream_lifetime =
        (range as f64 * (cfg.active_streams as f64 / cfg.total_streams as f64)) as i64;
    let stream_step = (range as f64 / (cfg.total_streams - cfg.active_streams + 1) as f64) as i64;
    let step = stream_lifetime / (cfg.logs_per_stream - 1);

    let mut curr_nsec = cfg.start_nsec;
    while curr_nsec < cfg.end_nsec {
        let first_stream_id = (curr_nsec - cfg.start_nsec) / stream_step;
        generate_logs_at_timestamp(bw, cfg, worker_id, curr_nsec, first_stream_id, rng)?;
        curr_nsec += step;
    }
    Ok(())
}

fn generate_logs_at_timestamp<W: Write>(
    bw: &mut W,
    cfg: &WorkerConfig,
    worker_id: i64,
    ts: i64,
    first_stream_id: i64,
    rng: &mut Rng,
) -> std::io::Result<()> {
    let fields = &cfg.fields;
    let time_str = to_rfc3339(ts);
    for stream_id in first_stream_id..first_stream_id + cfg.active_streams {
        let ip = to_ipv4(rng.uint32());
        let uuid = to_uuid(rng.uint64(), rng.uint64());
        write!(
            bw,
            "{{\"_time\":\"{time_str}\",\"_msg\":\"message for the stream {stream_id} and worker {worker_id}; ip={ip}; uuid={uuid}; u64={}\",\"host\":\"host_{stream_id}\",\"worker_id\":\"{worker_id}\"",
            rng.uint64()
        )?;
        write!(bw, ",\"run_id\":\"{}\"", cfg.run_id)?;
        for j in 0..fields.const_fields_per_log {
            write!(bw, ",\"const_{j}\":\"some value {j} {stream_id}\"")?;
        }
        for j in 0..fields.var_fields_per_log {
            write!(bw, ",\"var_{j}\":\"some value {j} {}\"", rng.uint64())?;
        }
        for j in 0..fields.dict_fields_per_log {
            write!(
                bw,
                ",\"dict_{j}\":\"{}\"",
                DICT_VALUES[rng.intn(DICT_VALUES.len())]
            )?;
        }
        for j in 0..fields.u8_fields_per_log {
            write!(bw, ",\"u8_{j}\":\"{}\"", rng.uint32() as u8)?;
        }
        for j in 0..fields.u16_fields_per_log {
            write!(bw, ",\"u16_{j}\":\"{}\"", rng.uint32() as u16)?;
        }
        for j in 0..fields.u32_fields_per_log {
            write!(bw, ",\"u32_{j}\":\"{}\"", rng.uint32())?;
        }
        for j in 0..fields.u64_fields_per_log {
            write!(bw, ",\"u64_{j}\":\"{}\"", rng.uint64())?;
        }
        for j in 0..fields.i64_fields_per_log {
            write!(bw, ",\"i64_{j}\":\"{}\"", rng.uint64() as i64)?;
        }
        for j in 0..fields.float_fields_per_log {
            write!(
                bw,
                ",\"float_{j}\":\"{}\"",
                (10_000.0 * rng.float64()).round() / 1000.0
            )?;
        }
        for j in 0..fields.ip_fields_per_log {
            let ip = to_ipv4(rng.uint32());
            write!(bw, ",\"ip_{j}\":\"{ip}\"")?;
        }
        for j in 0..fields.timestamp_fields_per_log {
            let timestamp = to_iso8601(rng.uint64() as i64);
            write!(bw, ",\"timestamp_{j}\":\"{timestamp}\"")?;
        }
        for j in 0..fields.json_fields_per_log {
            write!(
                bw,
                ",\"json_{j}\":\"{{\\\"foo\\\":\\\"bar_{}\\\",\\\"baz\\\":{{\\\"a\\\":[\\\"x\\\",\\\"y\\\"]}},\\\"f3\\\":NaN,\\\"f4\\\":{}}}\"",
                rng.intn(10),
                rng.intn(100)
            )?;
        }
        writeln!(bw, "}}")?;

        LOG_ENTRIES_COUNT.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

static DICT_VALUES: [&str; 8] = [
    "debug", "info", "warn", "error", "fatal", "ERROR", "FATAL", "INFO",
];

/// Port of Go `timeFlag`: a timestamp in any of the formats accepted by
/// `timeutil.ParseTimeMsec`, stored as unix nanoseconds.
#[derive(Clone)]
struct TimeFlag {
    s: String,
    nsec: i64,
}

impl TimeFlag {
    fn must_parse(s: &str) -> TimeFlag {
        match TimeFlag::parse_flag(s) {
            Ok(tf) => tf,
            Err(err) => {
                panicf!("invalid defaultValue={s:?} for time flag: {err}");
                unreachable!()
            }
        }
    }
}

impl FlagValue for TimeFlag {
    fn parse_flag(s: &str) -> Result<Self, String> {
        let msec = timeutil::parse_time_msec(s)
            .map_err(|err| format!("cannot parse time from {s:?}: {err}"))?;
        Ok(TimeFlag {
            s: s.to_string(),
            nsec: msec * 1_000_000,
        })
    }
}

impl fmt::Display for TimeFlag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.s)
    }
}

/// Port of Go `flag.Duration` (for `-statInterval`), stored as nanoseconds.
struct DurationFlag {
    nanos: i64,
}

impl FlagValue for DurationFlag {
    fn parse_flag(s: &str) -> Result<Self, String> {
        let nanos = parse_go_duration(s)?;
        Ok(DurationFlag { nanos })
    }
}

impl fmt::Display for DurationFlag {
    /// Canonical value string for the flag registry (the `FlagValue` trait's
    /// `Display` bound, mirroring Go `flag.Value.String()`): the largest unit
    /// dividing the duration evenly, matching Go `time.Duration.String()` for
    /// the round values this tool uses (e.g. `10s`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = self.nanos;
        if n != 0 && n % 1_000_000_000 == 0 {
            write!(f, "{}s", n / 1_000_000_000)
        } else if n != 0 && n % 1_000_000 == 0 {
            write!(f, "{}ms", n / 1_000_000)
        } else {
            write!(f, "{n}ns")
        }
    }
}

/// The parsed `-addr` ingestion URL with `_stream_fields=host,worker_id`
/// injected into the query string, like Go does via `url.Values.Set`.
///
/// The query string is re-serialized like Go's `url.Values.Encode` (keys
/// sorted, keys and values percent-escaped), so the `_stream_fields` comma is
/// sent as `%2C`.
///
/// PORT NOTE: Go parses `-addr` with `net/url` and pushes via `http.Client`, so
/// any scheme supported by the client (including https) works. This std-only
/// port speaks plain HTTP/1.1 over TCP, so only `http://` URLs are accepted.
#[derive(Debug, PartialEq)]
struct RemoteWriteUrl {
    host: String,
    port: u16,
    request_uri: String,
}

impl RemoteWriteUrl {
    fn parse(addr: &str) -> Result<RemoteWriteUrl, String> {
        let rest = addr
            .strip_prefix("http://")
            .ok_or("unsupported scheme; only http:// is supported")?;
        let (hostport, path_query) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        if hostport.is_empty() {
            return Err("missing host".to_string());
        }
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => {
                let port: u16 = p
                    .parse()
                    .map_err(|_| format!("cannot parse port in {hostport:?}"))?;
                (h.to_string(), port)
            }
            None => (hostport.to_string(), 80),
        };
        let (path, query) = match path_query.split_once('?') {
            Some((p, q)) => (p, q),
            None => (path_query, ""),
        };
        // Mirror Go: `q := u.Query(); q.Set("_stream_fields", "host,worker_id");
        // u.RawQuery = q.Encode()`. `Query()` percent-decodes the existing
        // pairs, `Set` replaces every `_stream_fields`, and `Encode()` re-emits
        // them sorted by key and percent-escaped (so the comma becomes `%2C`).
        let mut values: Vec<(String, String)> = query
            .split('&')
            .filter(|s| !s.is_empty())
            .map(|pair| match pair.split_once('=') {
                Some((k, v)) => (query_unescape(k), query_unescape(v)),
                None => (query_unescape(pair), String::new()),
            })
            .collect();
        values.retain(|(k, _)| k != "_stream_fields");
        values.push(("_stream_fields".to_string(), "host,worker_id".to_string()));
        values.sort_by(|a, b| a.0.cmp(&b.0));
        let encoded = values
            .iter()
            .map(|(k, v)| format!("{}={}", query_escape(k), query_escape(v)))
            .collect::<Vec<_>>()
            .join("&");
        Ok(RemoteWriteUrl {
            host,
            port,
            request_uri: format!("{path}?{encoded}"),
        })
    }
}

/// Go `url.QueryEscape` (the `encodeQueryComponent` mode): unreserved bytes pass
/// through, space becomes `+`, everything else is `%XX` with uppercase hex.
fn query_escape(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b' ' => out.push('+'),
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0f) as usize] as char);
            }
        }
    }
    out
}

/// Go `url.QueryUnescape`: `+` becomes a space and `%XX` decodes to a byte;
/// malformed escapes are left verbatim (lenient, like the values fed here).
fn query_unescape(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                match (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2])) {
                    (Some(h), Some(l)) => {
                        out.push((h << 4) | l);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

impl fmt::Display for RemoteWriteUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "http://{}:{}{}", self.host, self.port, self.request_uri)
    }
}

/// A small deterministic PRNG (splitmix64).
///
/// PORT NOTE: Go uses the auto-seeded `math/rand` global source. The
/// generated values only feed benchmark payloads, so any well-mixed 64-bit
/// generator is equivalent; splitmix64 is ported inline to keep the crate
/// std-only.
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng { state: seed }
    }

    fn uint64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn uint32(&mut self) -> u32 {
        (self.uint64() >> 32) as u32
    }

    /// Returns a value in `[0, n)`, like Go `rand.Intn`.
    ///
    /// PORT NOTE: uses a plain modulo instead of Go's rejection sampling; the
    /// bias is negligible for the tiny `n` values used here (8, 10, 100).
    fn intn(&mut self, n: usize) -> usize {
        (self.uint64() % n as u64) as usize
    }

    /// Returns a value in `[0.0, 1.0)`, like Go `rand.Float64`.
    fn float64(&mut self) -> f64 {
        (self.uint64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn seed_from_time() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as u64,
        Err(_) => 0,
    }
}

/// Formats unix nanoseconds like Go `time.Format(time.RFC3339Nano)` in UTC:
/// trailing zeros are trimmed from the fractional seconds and the fraction is
/// omitted entirely when zero.
fn to_rfc3339(nsec: i64) -> String {
    let (y, mo, d, h, mi, s, nanos) = utc_from_nsec(nsec);
    let mut out = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}");
    if nanos != 0 {
        let mut frac = format!("{nanos:09}");
        while frac.ends_with('0') {
            frac.pop();
        }
        out.push('.');
        out.push_str(&frac);
    }
    out.push('Z');
    out
}

/// Formats unix nanoseconds like Go `time.Format("2006-01-02T15:04:05.000Z")`
/// in UTC (always exactly three fractional digits).
fn to_iso8601(nsec: i64) -> String {
    let (y, mo, d, h, mi, s, nanos) = utc_from_nsec(nsec);
    format!(
        "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{:03}Z",
        nanos / 1_000_000
    )
}

/// Converts unix nanoseconds to UTC civil time
/// (year, month, day, hour, minute, second, nanosecond).
fn utc_from_nsec(nsec: i64) -> (i64, i64, i64, i64, i64, i64, i64) {
    let secs = nsec.div_euclid(1_000_000_000);
    let nanos = nsec.rem_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Howard Hinnant's civil-from-days algorithm.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mth = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mth <= 2 { y + 1 } else { y };
    (y, mth, d, h, m, s, nanos)
}

fn to_ipv4(n: u32) -> String {
    format!(
        "{}.{}.{}.{}",
        n >> 24,
        (n >> 16) & 0xff,
        (n >> 8) & 0xff,
        n & 0xff
    )
}

fn to_uuid(a: u64, b: u64) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        a & ((1u64 << 32) - 1),
        (a >> 32) & ((1u64 << 16) - 1),
        a >> 48,
        b & ((1u64 << 16) - 1),
        b >> 16
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_ipv4() {
        assert_eq!(to_ipv4(0), "0.0.0.0");
        assert_eq!(to_ipv4(0xFFFF_FFFF), "255.255.255.255");
        assert_eq!(to_ipv4(0x0102_0304), "1.2.3.4");
        assert_eq!(to_ipv4(0x7F00_0001), "127.0.0.1");
    }

    #[test]
    fn test_to_uuid() {
        assert_eq!(to_uuid(0, 0), "00000000-0000-0000-0000-000000000000");
        assert_eq!(
            to_uuid(0x1122_3344_5566_7788, 0x99AA_BBCC_DDEE_FF00),
            "55667788-3344-1122-ff00-99aabbccddee"
        );
    }

    #[test]
    fn test_to_rfc3339() {
        assert_eq!(to_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(
            to_rfc3339(1_700_000_000_000_000_000),
            "2023-11-14T22:13:20Z"
        );
        // Trailing zeros in the fraction are trimmed, like Go RFC3339Nano.
        assert_eq!(to_rfc3339(1_500_000), "1970-01-01T00:00:00.0015Z");
        assert_eq!(to_rfc3339(123_456_789), "1970-01-01T00:00:00.123456789Z");
        // Negative timestamps (dates before the epoch).
        assert_eq!(to_rfc3339(-1_000_000_000), "1969-12-31T23:59:59Z");
    }

    #[test]
    fn test_to_iso8601() {
        assert_eq!(to_iso8601(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(to_iso8601(-1), "1969-12-31T23:59:59.999Z");
        assert_eq!(
            to_iso8601(1_700_000_000_123_000_000),
            "2023-11-14T22:13:20.123Z"
        );
    }

    #[test]
    fn test_rng_deterministic() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..16 {
            assert_eq!(a.uint64(), b.uint64());
        }
        let mut c = Rng::new(43);
        assert_ne!(Rng::new(42).uint64(), c.uint64());
        let mut d = Rng::new(7);
        for _ in 0..1000 {
            let f = d.float64();
            assert!((0.0..1.0).contains(&f));
            assert!(d.intn(8) < 8);
        }
    }

    #[test]
    fn test_remote_write_url_parse() {
        let u = RemoteWriteUrl::parse("http://localhost:9428/insert/jsonline").unwrap();
        assert_eq!(u.host, "localhost");
        assert_eq!(u.port, 9428);
        // Go's url.Values.Encode percent-escapes the comma.
        assert_eq!(
            u.request_uri,
            "/insert/jsonline?_stream_fields=host%2Cworker_id"
        );
        assert_eq!(
            u.to_string(),
            "http://localhost:9428/insert/jsonline?_stream_fields=host%2Cworker_id"
        );

        // Existing query params are preserved; _stream_fields is replaced; keys
        // are sorted (`_stream_fields` < `foo` because `_` < `f`).
        let u = RemoteWriteUrl::parse("http://127.0.0.1/insert/jsonline?foo=bar&_stream_fields=x")
            .unwrap();
        assert_eq!(u.port, 80);
        assert_eq!(
            u.request_uri,
            "/insert/jsonline?_stream_fields=host%2Cworker_id&foo=bar"
        );

        // Missing path defaults to "/".
        let u = RemoteWriteUrl::parse("http://host:1234").unwrap();
        assert_eq!(u.request_uri, "/?_stream_fields=host%2Cworker_id");

        assert!(RemoteWriteUrl::parse("stdout").is_err());
        assert!(RemoteWriteUrl::parse("https://host/insert").is_err());
        assert!(RemoteWriteUrl::parse("http://host:bad/insert").is_err());
    }

    fn test_config(
        start_nsec: i64,
        end_nsec: i64,
        active_streams: i64,
        total_streams: i64,
        logs_per_stream: i64,
    ) -> WorkerConfig {
        WorkerConfig {
            url: None,
            active_streams,
            total_streams,
            start_nsec,
            end_nsec,
            logs_per_stream,
            fields: FieldsConfig {
                const_fields_per_log: 3,
                var_fields_per_log: 1,
                dict_fields_per_log: 2,
                u8_fields_per_log: 1,
                u16_fields_per_log: 1,
                u32_fields_per_log: 1,
                u64_fields_per_log: 1,
                i64_fields_per_log: 1,
                float_fields_per_log: 1,
                ip_fields_per_log: 1,
                timestamp_fields_per_log: 1,
                json_fields_per_log: 1,
            },
            run_id: to_uuid(1, 2),
        }
    }

    #[test]
    fn test_generate_logs_at_timestamp_line_shape() {
        let cfg = test_config(0, 1_000, 2, 2, 5);
        let mut buf = Vec::new();
        let mut rng = Rng::new(123);
        generate_logs_at_timestamp(&mut buf, &cfg, 7, 1_500_000, 3, &mut rng).unwrap();
        let out = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2);

        // Stream IDs start at first_stream_id and increment per line.
        assert!(lines[0].starts_with(
            "{\"_time\":\"1970-01-01T00:00:00.0015Z\",\"_msg\":\"message for the stream 3 and worker 7; ip="
        ));
        assert!(lines[1].contains("\"host\":\"host_4\",\"worker_id\":\"7\""));

        for line in &lines {
            assert!(line.ends_with('}'));
            assert!(line.contains(&format!("\"run_id\":\"{}\"", to_uuid(1, 2))));
            for key in [
                "\"const_0\":\"some value 0 ",
                "\"const_1\":",
                "\"const_2\":",
                "\"var_0\":\"some value 0 ",
                "\"dict_0\":",
                "\"dict_1\":",
                "\"u8_0\":",
                "\"u16_0\":",
                "\"u32_0\":",
                "\"u64_0\":",
                "\"i64_0\":",
                "\"float_0\":",
                "\"ip_0\":",
                "\"timestamp_0\":",
                "\"json_0\":\"{\\\"foo\\\":\\\"bar_",
            ] {
                assert!(line.contains(key), "missing {key} in {line}");
            }
            // The json_* field payload matches the upstream literal.
            assert!(line.contains(
                "\\\",\\\"baz\\\":{\\\"a\\\":[\\\"x\\\",\\\"y\\\"]},\\\"f3\\\":NaN,\\\"f4\\\":"
            ));
        }
        // Dict values come from the fixed dictionary.
        let dict_start = lines[0].find("\"dict_0\":\"").unwrap() + "\"dict_0\":\"".len();
        let dict_val =
            &lines[0][dict_start..lines[0][dict_start..].find('"').unwrap() + dict_start];
        assert!(DICT_VALUES.contains(&dict_val), "bad dict value {dict_val}");
    }

    #[test]
    fn test_generate_logs_pacing() {
        // streamLifetime = 1000 * (2/2) = 1000; streamStep = 1000/1 = 1000;
        // step = 1000/(5-1) = 250 => timestamps 0, 250, 500, 750 (1000 is
        // excluded since currNsec < end), 2 streams each = 8 lines.
        let cfg = test_config(0, 1_000, 2, 2, 5);
        let mut buf = Vec::new();
        let mut rng = Rng::new(1);
        generate_logs(&mut buf, &cfg, 0, &mut rng).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out.lines().count(), 8);
        assert_eq!(out.matches("\"_time\":\"1970-01-01T00:00:00Z\"").count(), 2);
        assert_eq!(
            out.matches("\"_time\":\"1970-01-01T00:00:00.00000075Z\"")
                .count(),
            2
        );
        assert!(!out.contains("\"_time\":\"1970-01-01T00:00:00.000001Z\""));
    }

    #[test]
    fn test_generate_logs_stream_rotation() {
        // totalStreams > activeStreams: streamLifetime = 1000*(1/2) = 500;
        // streamStep = 1000/(2-1+1) = 500; step = 500/(2-1) = 500 =>
        // timestamps 0 (stream 0) and 500 (stream 1).
        let cfg = test_config(0, 1_000, 1, 2, 2);
        let mut buf = Vec::new();
        let mut rng = Rng::new(1);
        generate_logs(&mut buf, &cfg, 0, &mut rng).unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert_eq!(out.lines().count(), 2);
        assert!(out.contains("\"host\":\"host_0\""));
        assert!(out.contains("\"host\":\"host_1\""));
    }

    #[test]
    fn test_time_flag_parse() {
        let tf = TimeFlag::parse_flag("2023-11-14T22:13:20Z").unwrap();
        assert_eq!(tf.nsec, 1_700_000_000_000_000_000);
        assert_eq!(tf.to_string(), "2023-11-14T22:13:20Z");
        assert!(TimeFlag::parse_flag("not-a-time").is_err());
    }

    #[test]
    fn test_duration_flag_parse() {
        assert_eq!(
            DurationFlag::parse_flag("10s").unwrap().nanos,
            10_000_000_000
        );
        assert_eq!(DurationFlag::parse_flag("1.5ms").unwrap().nanos, 1_500_000);
        assert!(DurationFlag::parse_flag("10").is_err());
    }
}

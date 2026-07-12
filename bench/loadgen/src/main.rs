//! Deterministic ingestion load generator for the Go-vs-Rust EsLogs
//! benchmark. Dependency-free (std only): raw HTTP/1.1 with keep-alive over
//! `std::net::TcpStream`, so it builds and behaves identically on Linux and
//! Windows (MSVC).
//!
//! Two subcommands:
//!   * `corpus`  — build a byte-identical newline-delimited JSON corpus from
//!                 source `.log` files (so both servers ingest the same input).
//!   * `replay`  — POST the corpus to a target server's ingestion endpoint at a
//!                 fixed rate (or unbounded) using N keep-alive connections, and
//!                 report throughput.

use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("");
    match cmd {
        "corpus" => cmd_corpus(&args[2..]),
        "replay" => cmd_replay(&args[2..]),
        _ => {
            eprintln!(
                "usage:\n  esl-loadgen corpus --logs <dir> --out <corpus.jsonl> [--max-lines N]\n  \
                 esl-loadgen replay --corpus <corpus.jsonl> --host 127.0.0.1 --port 9428 \
                 --path '/insert/jsonline?_stream_fields=source&_msg_field=message&_time_field=@timestamp' \
                 [--rate N] [--conns N] [--batch N]"
            );
            std::process::exit(2);
        }
    }
}

// ---------------------------------------------------------------------------
// corpus: source .log files -> newline-delimited JSON records
// ---------------------------------------------------------------------------

fn cmd_corpus(args: &[String]) {
    let logs = flag(args, "--logs").expect("--logs <dir> required");
    let out = flag(args, "--out").expect("--out <file> required");
    let max_lines: usize = flag(args, "--max-lines")
        .map(|s| s.parse().expect("--max-lines must be an integer"))
        .unwrap_or(usize::MAX);

    let mut entries: Vec<_> = fs::read_dir(&logs)
        .unwrap_or_else(|e| panic!("cannot read logs dir {logs}: {e}"))
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "log").unwrap_or(false))
        .collect();
    entries.sort(); // deterministic order regardless of readdir ordering

    let mut w = std::io::BufWriter::new(File::create(&out).expect("create corpus"));
    let mut written = 0usize;
    // A fixed base timestamp keeps the corpus deterministic (no wall clock).
    // 2026-01-01T00:00:00Z in RFC3339; one millisecond per line.
    let base_ms: u64 = 1_767_225_600_000;
    let mut line_buf = String::new();

    'outer: for path in &entries {
        let source = path.file_name().unwrap().to_string_lossy().to_string();
        let f = File::open(path).unwrap_or_else(|e| panic!("open {path:?}: {e}"));
        let mut reader = BufReader::new(f);
        let mut raw = Vec::new();
        loop {
            raw.clear();
            let n = reader.read_until(b'\n', &mut raw).expect("read line");
            if n == 0 {
                break;
            }
            while matches!(raw.last(), Some(b'\n' | b'\r')) {
                raw.pop();
            }
            if raw.is_empty() {
                continue;
            }
            let msg = String::from_utf8_lossy(&raw);
            let ts = iso8601_millis(base_ms + written as u64);
            line_buf.clear();
            line_buf.push('{');
            write!(line_buf, "\"@timestamp\":\"{ts}\",").unwrap();
            line_buf.push_str("\"source\":\"");
            json_escape_into(&source, &mut line_buf);
            line_buf.push_str("\",\"message\":\"");
            json_escape_into(&msg, &mut line_buf);
            line_buf.push_str("\"}\n");
            w.write_all(line_buf.as_bytes()).expect("write corpus");
            written += 1;
            if written >= max_lines {
                break 'outer;
            }
        }
    }
    w.flush().unwrap();
    eprintln!("corpus: wrote {written} records to {out}");
}

// ---------------------------------------------------------------------------
// replay: POST the corpus to the target ingestion endpoint
// ---------------------------------------------------------------------------

fn cmd_replay(args: &[String]) {
    let corpus = flag(args, "--corpus").expect("--corpus <file> required");
    let host = flag(args, "--host").unwrap_or_else(|| "127.0.0.1".to_string());
    let port: u16 = flag(args, "--port")
        .map(|s| s.parse().expect("--port"))
        .unwrap_or(9428);
    let path = flag(args, "--path").unwrap_or_else(|| {
        "/insert/jsonline?_stream_fields=source&_msg_field=message&_time_field=@timestamp"
            .to_string()
    });
    let conns: usize = flag(args, "--conns")
        .map(|s| s.parse().expect("--conns"))
        .unwrap_or(4);
    let batch: usize = flag(args, "--batch")
        .map(|s| s.parse().expect("--batch"))
        .unwrap_or(1000);
    // Total records/second across all connections; 0 = unbounded.
    let rate: u64 = flag(args, "--rate")
        .map(|s| s.parse().expect("--rate"))
        .unwrap_or(0);

    // Load the whole corpus into memory once, split into batches. Both servers
    // replay the identical in-memory batches.
    let batches = Arc::new(load_batches(&corpus, batch));
    let total_records: usize = batches.iter().map(|b| b.lines).sum();
    eprintln!(
        "replay: {} records in {} batches -> {host}:{port}{path} ({conns} conns, rate={})",
        total_records,
        batches.len(),
        if rate == 0 { "unbounded".into() } else { rate.to_string() }
    );

    let next = Arc::new(AtomicUsize::new(0));
    let sent_records = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(conns + 1));
    // Token-bucket start time is shared; each worker paces against it.
    let start = Instant::now();

    let mut handles = Vec::new();
    for _ in 0..conns {
        let batches = Arc::clone(&batches);
        let next = Arc::clone(&next);
        let sent_records = Arc::clone(&sent_records);
        let errors = Arc::clone(&errors);
        let barrier = Arc::clone(&barrier);
        let host = host.clone();
        let path = path.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut conn = Conn::connect(&host, port).expect("connect");
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= batches.len() {
                    break;
                }
                let b = &batches[idx];
                // Fixed-rate pacing: don't send batch idx until its scheduled
                // time has arrived (records-per-second across all conns).
                if rate > 0 {
                    let records_before = (idx as u64) * (batch as u64);
                    let due = Duration::from_secs_f64(records_before as f64 / rate as f64);
                    let elapsed = start.elapsed();
                    if due > elapsed {
                        std::thread::sleep(due - elapsed);
                    }
                }
                match conn.post(&path, &host, &b.body) {
                    Ok(()) => {
                        sent_records.fetch_add(b.lines as u64, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        // Reconnect on error and retry once.
                        if let Ok(c) = Conn::connect(&host, port) {
                            conn = c;
                            let _ = conn.post(&path, &host, &b.body);
                        }
                    }
                }
            }
        }));
    }

    barrier.wait();
    let t0 = Instant::now();
    for h in handles {
        h.join().ok();
    }
    let elapsed = t0.elapsed();

    let sent = sent_records.load(Ordering::Relaxed);
    let errs = errors.load(Ordering::Relaxed);
    let throughput = sent as f64 / elapsed.as_secs_f64();
    // Machine-readable summary line (parsed by run_bench).
    println!(
        "{{\"records\":{sent},\"elapsed_s\":{:.3},\"throughput_rps\":{:.1},\"errors\":{errs}}}",
        elapsed.as_secs_f64(),
        throughput
    );
}

struct Batch {
    body: Vec<u8>,
    lines: usize,
}

fn load_batches(corpus: &str, batch: usize) -> Vec<Batch> {
    let f = File::open(corpus).unwrap_or_else(|e| panic!("open corpus {corpus}: {e}"));
    let reader = BufReader::new(f);
    let mut batches = Vec::new();
    let mut body = Vec::new();
    let mut lines = 0usize;
    for line in reader.lines() {
        let line = line.expect("read corpus line");
        if line.is_empty() {
            continue;
        }
        body.extend_from_slice(line.as_bytes());
        body.push(b'\n');
        lines += 1;
        if lines >= batch {
            batches.push(Batch {
                body: std::mem::take(&mut body),
                lines,
            });
            lines = 0;
        }
    }
    if lines > 0 {
        batches.push(Batch { body, lines });
    }
    batches
}

/// A single keep-alive HTTP/1.1 connection.
struct Conn {
    stream: TcpStream,
}

impl Conn {
    fn connect(host: &str, port: u16) -> std::io::Result<Conn> {
        let stream = TcpStream::connect((host, port))?;
        stream.set_nodelay(true)?;
        Ok(Conn { stream })
    }

    fn post(&mut self, path: &str, host: &str, body: &[u8]) -> std::io::Result<()> {
        let mut req = Vec::with_capacity(body.len() + 256);
        write!(
            &mut req,
            "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/x-ndjson\r\n\
             Content-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            body.len()
        )
        .unwrap();
        req.extend_from_slice(body);
        self.stream.write_all(&req)?;
        self.stream.flush()?;
        self.read_response()
    }

    /// Reads one HTTP response, honoring Content-Length or chunked encoding
    /// enough to drain the body so the connection can be reused.
    fn read_response(&mut self) -> std::io::Result<()> {
        let mut buf = Vec::with_capacity(1024);
        let mut tmp = [0u8; 1024];
        let header_end;
        loop {
            if let Some(pos) = find_subsequence(&buf, b"\r\n\r\n") {
                header_end = pos + 4;
                break;
            }
            let n = self.stream.read(&mut tmp)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "eof before headers",
                ));
            }
            buf.extend_from_slice(&tmp[..n]);
        }
        let headers = String::from_utf8_lossy(&buf[..header_end]).to_ascii_lowercase();
        // Non-2xx is surfaced as an error so the caller counts/retries it.
        let status_ok = headers.starts_with("http/1.1 2") || headers.starts_with("http/1.0 2");
        let mut body_have = buf.len() - header_end;

        if let Some(cl) = header_value(&headers, "content-length:") {
            let want: usize = cl.trim().parse().unwrap_or(0);
            while body_have < want {
                let n = self.stream.read(&mut tmp)?;
                if n == 0 {
                    break;
                }
                body_have += n;
            }
        } else if headers.contains("transfer-encoding:") && headers.contains("chunked") {
            // Drain until the terminating 0-length chunk.
            let mut acc = buf[header_end..].to_vec();
            loop {
                if find_subsequence(&acc, b"0\r\n\r\n").is_some() {
                    break;
                }
                let n = self.stream.read(&mut tmp)?;
                if n == 0 {
                    break;
                }
                acc.extend_from_slice(&tmp[..n]);
            }
        }
        if status_ok {
            Ok(())
        } else {
            Err(std::io::Error::other("non-2xx response"))
        }
    }
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

fn header_value<'a>(headers: &'a str, key: &str) -> Option<&'a str> {
    let start = headers.find(key)? + key.len();
    let rest = &headers[start..];
    let end = rest.find("\r\n").unwrap_or(rest.len());
    Some(&rest[..end])
}

fn json_escape_into(s: &str, out: &mut String) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// Formats milliseconds-since-epoch as an RFC3339 UTC timestamp with millis.
fn iso8601_millis(ms: u64) -> String {
    let secs = ms / 1000;
    let millis = ms % 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
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
    format!("{y:04}-{mth:02}-{d:02}T{h:02}:{m:02}:{s:02}.{millis:03}Z")
}

fn _unused(_: &Path) {}

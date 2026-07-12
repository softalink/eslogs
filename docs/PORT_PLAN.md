# Port Plan

Goal: port EsLogs single-node **v1.51.0** (Go) to Rust and beat the Go
binary on **all** metrics of the upstream `deployment/logs-benchmark` (CPU,
memory, disk usage, ingestion throughput) on Linux **and** Windows (MSVC).

Reference checkout: upstream VictoriaLogs (tag `v1.51.0`, Go 1.26.4) — see `UPSTREAM.lock`.

## Scope (from upstream source inventory)

| Upstream | LOC (non-test) | Rust home |
|----------|---------------|-----------|
| `lib/logstorage` | ~58,800 | `crates/esl-logstorage` |
| `lib/prefixfilter` | ~360 | `crates/esl-logstorage` |
| Softalink LLC `lib/*` (~30 helper packages: logger, bytesutil, encoding, fs, atomicutil, slicesutil, flagutil, httpserver, timeutil, memory, cgroup, ...) | ~15–20k | `crates/esl-common` |
| `app/eslinsert` | ~4,900 | `crates/esl-insert` |
| `app/eslselect` | ~4,000 | `crates/esl-select` |
| `app/eslstorage` | ~2,300 | `crates/esl-storage` |
| `app/es-logs` | ~110 | `crates/es-logs` |

Out of scope (separate binaries, not part of single-node server):
`app/eslagent`, `app/eslogscli`, `app/eslogsgenerator`. The `esmui` web UI assets
can be embedded from the upstream prebuilt assets.

## Phases

1. **Foundation** (`esl-common`): bytesutil, encoding (+zstd/snappy), slicesutil,
   stringsutil, fasttime, timeutil, atomicutil, memory, cgroup, logger,
   flagutil/envflag, fs, filestream, regexutil, httpserver/httputil,
   protoparserutil, netutil, chunkedbuffer, bufferedwriter, timerpool.
   Port Go tests alongside.
2. **Storage engine** (`esl-logstorage`): bottom-up —
   primitives (hash, tokenizer, bloom filters, value encoders) → block format →
   parts/merge → datadb/partitions → indexdb → Storage API →
   LogsQL lexer/parser → filters → pipes → stats functions → query engine.
   Port the ~47k LOC of Go tests as the correctness spec.
3. **App layer**: eslstorage → eslinsert (Elasticsearch bulk + jsonline + Loki +
   syslog first — these are what the benchmark exercises) → eslselect →
   es-logs main with CLI-flag parity.
4. **Benchmark harness** (`bench/`): run Go v1.51.0 vs the Rust port under the
   upstream logs-benchmark load (generator → filebeat/promtail → both servers),
   collect CPU / RSS / disk-usage / throughput; automated report.
5. **Windows MSVC**: cross-platform correctness (no mmap assumptions, path
   handling, file locking), build + bench on Windows.
6. **Optimization loop**: profile → optimize → re-bench until Rust wins every
   metric on both OSes.

## Design decisions

- **Concurrency**: threads + rayon-style worker pools mirroring Go's
  goroutine-per-shard patterns; no async runtime in the hot ingest path unless
  benchmarking says otherwise. HTTP layer may use a small threaded server
  (hyper) — decision recorded when `esl-common::httpserver` is ported.
- **Memory discipline**: Go version leans heavily on `sync.Pool` and arena-ish
  byte-slice reuse; Rust port mirrors this with object pools and reused
  `Vec<u8>` buffers to avoid allocator churn — this is where we beat Go's GC.
- **Compression**: `zstd` crate (libzstd, same codec as Go's
  `klauspost/compress` zstd usage), `snap` for snappy, `lz4_flex` where
  upstream uses lz4. Same on-disk format => same-or-better compression ratio.
- **Hashing**: xxhash (`twox-hash`/`xxhash-rust`) matching `cespare/xxhash/v2`.
- **On-disk format**: byte-compatible with upstream where practical, so
  correctness can be validated by pointing both binaries at identical data.
- **Cross-platform**: `memmap2` for reads where upstream mmaps; fall back to
  buffered reads on Windows exactly like upstream's `fs` package does.

## Verification strategy

- Port upstream Go unit tests package-by-package (they are the spec).
- Golden-data checks: identical ingest input must produce equivalent query
  results from both binaries (`/select/logsql/query`).
- Benchmark harness produces a metric-by-metric Go-vs-Rust table; goal gate is
  "Rust strictly better on every metric".

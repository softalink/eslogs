<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.svg">
    <img src="assets/logo.svg" alt="eslogs — no garbage, just logs" width="440">
  </picture>
</p>

# EsLogs

> **No garbage. Just logs.**

**EsLogs** by Softalink LLC — a Rust log database, ported from
[VictoriaLogs](https://github.com/VictoriaMetrics/VictoriaLogs) single-node
(upstream reference: **v1.51.0**) — **beats the original Go implementation on
every benchmark metric — ingest throughput, CPU, peak memory, disk footprint,
and query latency — on both Linux and Windows (MSVC)**.

This is a derivative work of VictoriaMetrics' VictoriaLogs, licensed under
[Apache-2.0](LICENSE) (see [NOTICE](NOTICE)).

| | Linux (Rust vs Go) | Windows/MSVC (Rust vs Go) |
|---|---|---|
| Ingest throughput | **+12–15%** | **+15.8%** |
| CPU time | **−8%** | **−13%** |
| Peak RSS | **−53%** (186 MB vs ~400 MB) | **−55%** (198 MB vs 438 MB) |
| Disk usage | **−2.2%** | **−2.5%** |
| Query: last-100 of `*` | **4.5× faster** (6.7 ms vs 37 ms) | **2.8× faster** (15.2 ms vs 43.3 ms) |
| Query: phrase filter | **1.7× faster** (0.40 ms vs 0.74 ms) | faster (client-floor-bound) |
| Query: `stats count()` | **1.3× faster** (0.26 ms vs 0.47 ms) | faster |

Full numbers, history, and methodology: [bench/BASELINE.md](bench/BASELINE.md).

---

## Why a Rust port?

EsLogs is already one of the most efficient log databases in existence —
that's exactly why it's the right benchmark. The point of the port is not that
Go is slow; it's that even against a best-in-class Go system there is real,
measurable headroom, and where that headroom lies matters for anyone running
log storage:

- **No garbage collector — the slogan is the engineering.** A log store is the
  always-on tenant on every node: it ingests continuously and is queried in
  bursts, which is the workload GC likes least. Removing the collector removes
  GC pauses from tail latency, removes heap headroom from the memory bill, and
  makes footprint *deterministic*. The receipt is on the scoreboard: **peak RSS
  cut in half (−53–55%)** for identical data and workload — that's mostly
  reclaimed GC headroom, arenas, and pooled buffers with exact lifetimes.
- **Fleet-scale economics.** Observability infrastructure runs 24/7 on every
  cluster you own. −8–13% CPU and half the memory translate directly into
  smaller instances or higher tenant density; on the ingest path it means the
  same hardware absorbs +12–16% more log volume before saturating.
- **Predictability under pressure.** The moment you need your logs most — an
  incident — is when ingest spikes and everyone queries at once. Fixed-size
  worker pools, pooled allocations, and no collector competing for CPU keep
  behavior boring precisely when the rest of the system isn't.
- **Memory safety without a runtime tax.** Rust delivers the same
  no-use-after-free, no-data-race guarantees Go's runtime provides, but at
  compile time. The port's storage engine handles untrusted input (JSON
  ingestion, LogsQL parsing) in safe Rust; the handful of `unsafe` blocks are
  localized, documented, and inherited from deliberate designs (mmap, pinned
  part lifetimes).
- **A smaller, more portable artifact.** One ~6 MB static binary, no runtime
  to ship, and Windows (MSVC) as a first-class, benchmarked target rather than
  an afterthought — including cross-compilation and PGO entirely from Linux.
- **An embeddable engine.** The storage engine and LogsQL are ordinary Rust
  crates (`esl-logstorage`, `esl-common`). Rust applications can embed a
  full-text log store and query language in-process — no sidecar, no FFI
  boundary, no separate deployment.
- **A reproducible method.** Beyond the artifact, the repo demonstrates a
  playbook for porting mature Go infrastructure to Rust: port faithfully with
  the upstream tests, benchmark honestly, profile before optimizing, and only
  then go beyond the original where the borrow checker and zero-copy access
  open doors Go keeps shut (see
  [How the performance was achieved](#how-the-performance-was-achieved)).

---

## For users

### What works

The port is **functionally complete** for single-node use: the full
`lib/logstorage` engine (storage, LogsQL filters/stats/pipes, parser), the app
layer, and the `es-logs` server binary run end-to-end on Linux and Windows,
returning results identical to upstream on the ported behavior (the known
residual divergences — e.g. HTTP auth flags, log-deletion execution, and a
set of parser/engine edge cases — are enumerated exhaustively in the parity
ledger, [docs/PARITY.md](docs/PARITY.md) § "Parity ledger (v1.51.0)").
1,123 tests — the upstream Go tests, ported alongside the code — pass across
the workspace.

- **Ingestion**: jsonline, Elasticsearch bulk, Loki (JSON + protobuf), OTLP,
  DataDog, journald, Splunk HEC, syslog (TCP/UDP/unix listeners), native.
- **Queries**: the full `/select/logsql/*` surface — `query` (json + csv),
  `hits`, `facets`, `stats_query`, `stats_query_range`, `query_time_range`,
  `field_names`, `field_values`, `streams`, `stream_ids`,
  `stream_field_names`, `stream_field_values`, live `tail` — plus the
  embedded esmui web UI and the internal cluster-select protocol.
- **On-disk format**: byte-compatible with upstream for both the data path
  (`partitions/*/datadb`) and the stream index (`partitions/*/indexdb`, a
  faithful `lib/mergeset` port). An existing Go `-storageDataPath` opens in
  place, and data dirs written by the Rust binary open with the Go binary —
  both directions are verified against the reference binary (see
  [docs/PARITY.md](docs/PARITY.md)).
- **The entire upstream `app/` tree is ported**: all ingestion protocols
  (Elasticsearch bulk, jsonline, Loki JSON+protobuf, OTLP, DataDog, journald,
  Splunk HEC, syslog listeners, native/internal), all 13 `/select/logsql/*`
  endpoints (hits, facets, stats_query(_range), streams, live tail, CSV, ...),
  the embedded esmui web UI, the `eslagent` log shipper (file tailing, k8s
  collector, remote write with a persistent disk queue — wire-compatible with
  Go), and the `eslogscli`/`eslogsgenerator` tools.
- TLS is supported end to end (rustls with the `ring` provider — keeps the
  MSVC cross-build clean): https for `-storageNode.tls*`, `-remoteWrite.tls*`,
  the Kubernetes collector and `eslogscli -tls*`, plus server-side TLS for
  `-syslog.tls*` and the HTTP server's `-tls`/`-tlsCertFile`/`-tlsKeyFile`/
  `-tlsMinVersion`/`-tlsCipherSuites` serving flags (es-logs and eslagent).
- Residual divergences are tracked exhaustively in the parity ledger:
  [docs/PARITY.md](docs/PARITY.md) § "Parity ledger (v1.51.0)".

### Quick start

```sh
cargo build --release
./target/release/es-logs \
    -storageDataPath=/var/lib/es-logs \
    -httpListenAddr=:9428 \
    -retentionPeriod=10y
```

Ingest a couple of log lines and query them back:

```sh
curl -X POST 'http://127.0.0.1:9428/insert/jsonline?_stream_fields=source&_msg_field=message&_time_field=ts' \
  -H 'Content-Type: application/x-ndjson' --data-binary \
'{"ts":"2026-07-08T12:00:00Z","source":"app","message":"user login ok"}
{"ts":"2026-07-08T12:00:01Z","source":"app","message":"payment failed err=timeout"}'

curl 'http://127.0.0.1:9428/select/logsql/query' --data-urlencode 'query=failed' --data-urlencode 'limit=10'
curl 'http://127.0.0.1:9428/select/logsql/query' --data-urlencode 'query=* | stats count() rows'
```

Flags mirror upstream (`-storageDataPath`, `-httpListenAddr`,
`-retentionPeriod`, `-fs.disableMmap`, ...). Note: ingest bodies must **not**
use `Content-Type: application/x-www-form-urlencoded` (that content type is
reserved for form-style endpoints like the query API).

### Building

```sh
cargo build --release                                       # Linux
cargo build --release --target x86_64-pc-windows-msvc      # Windows (native)

# Cross-compile Windows binaries from Linux (used by this repo's CI/bench):
XWIN_ACCEPT_LICENSE=1 cargo xwin build --release --target x86_64-pc-windows-msvc -p es-logs
```

For maximum performance on a known target machine, add
`RUSTFLAGS="-C target-cpu=x86-64-v3"` (any AVX2-era x86) and apply PGO — the
recipe used for the published Windows numbers is in
[Benchmark → Reproducing](#reproducing).

---

## The benchmark

### Setup

Everything lives in [`bench/`](bench/). Instead of reproducing the upstream
docker/filebeat/Grafana stack, the harness drives each server directly with an
identical corpus over the same native API and measures the **server process
itself** — deterministic, dependency-light, and portable to Windows.

| Component | Role |
|---|---|
| `loadgen/` | Dependency-free Rust load generator (raw HTTP/1.1 over std TCP, keep-alive, `TCP_NODELAY`). `corpus` builds a byte-identical JSONL corpus; `replay` POSTs it over N connections and reports throughput. |
| `run_bench.sh` | Linux runner: start server → replay → flush+merge → timed queries → emit result JSON. |
| `bench_win.ps1` | Windows runner: same protocol; reports the **median of 5 alternating Rust/Go runs** because a saturated 4-core box has ±3% session noise. |
| `compare.py` | Metric-by-metric win/loss table; exits 0 iff Rust wins everything. |

Both servers ingest the **same 500,000-record corpus** (66 MB JSONL, two log
streams, millisecond-spaced timestamps) via `/insert/jsonline` over 8
keep-alive connections at an unbounded rate, then are force-flushed and
force-merged before measurement.

### What is measured, and how

| Metric | Source | Better |
|---|---|---|
| Ingest throughput (records/s) | records ÷ replay wall-clock | higher |
| CPU seconds | `/proc/PID/stat` utime+stime (Linux), `GetProcessTimes` (Windows) | lower |
| Peak RSS | `/proc/PID/status` `VmHWM` (kernel high-water mark), `PeakWorkingSet64` | lower |
| Disk usage | data-dir size after flush + full merge | lower |
| Query latency | warm minimum of 5–6 runs of three representative LogsQL queries, each with `limit=100`: `*` (newest-100 of everything), `error` (phrase filter), `* \| stats count()` | lower |

Two methodology lessons are baked in (both produced wrong conclusions before
they were fixed): query latency uses **warm minimums**, never cold single
shots; and on small machines, single benchmark runs of near-tied metrics are
noise — the Windows runner alternates implementations and takes medians. The
Windows query numbers sit on a ~10.5 ms `curl.exe` process-spawn floor that
affects both servers equally; server-side latency (measured with the built-in
`ESL_QUERY_TIMING=1` instrumentation) is what the Linux numbers show.

### Reproducing

Linux:

```sh
(cd bench/loadgen && cargo build --release)
./bench/loadgen/target/release/esl-loadgen corpus --logs <logs-dir> --out corpus.jsonl
./bench/run_bench.sh --bin <go-binary>  --label go   --corpus corpus.jsonl --out go.json
./bench/run_bench.sh --bin <rust-binary> --label rust --corpus corpus.jsonl --out rust.json
python3 bench/compare.py go.json rust.json
```

Windows: copy both binaries, `esl-loadgen.exe`, the corpus, and
`bench/bench_win.ps1` to the target machine and run the script.

The published Windows binary is **PGO-built**. The recipe (all cross-compiled
from Linux with `cargo-xwin`):

1. Build instrumented: `RUSTFLAGS="-C target-cpu=x86-64-v3 -C profile-generate=<dir>" cargo xwin build --release ...`
2. Train it on the bench workload on the target box. Set
   `ESL_EXIT_AFTER_SECS=60` so the server exits **gracefully** — a forced kill
   (or `process::exit`, which is `ExitProcess` on Windows) skips the atexit
   hook that writes the profile.
3. Merge: `llvm-profdata merge` (from `rustup component add llvm-tools`).
4. Rebuild with `-C profile-use=<profdata>` and deploy.

PGO alone was worth ~15% ingest throughput on Windows — it took that metric
from a per-run coin flip to a decisive win.

---

## How the performance was achieved

The port keeps EsLogs' architecture intact — LSM-style parts with
per-block columnar storage, zstd-compressed blocks, bloom-filter token indexes,
worker-per-connection HTTP — so none of the wins come from redesign. They came
in three layers:

### 1. A faithful port with Rust cost discipline

- **Same data structures, no GC**: arena-backed `LogRows`, pooled buffers and
  parsers (thread-local pools replacing Go's `sync.Pool`), and the ingest hot
  path hands parsed fields to storage **in place** — the JSON parser's fields
  go straight into the storage arena with no intermediate clone.
- **Allocators chosen per platform**: jemalloc on Linux (tuned decay keeps RSS
  low — the −53% peak-RSS win), mimalloc on Windows (jemalloc doesn't build on
  MSVC).
- **C libzstd instead of Go's pure-Go klauspost zstd**, with the compression
  level mapped to match klauspost's speed curve (libzstd level 2 where Go uses
  "SpeedDefault") — the result is *both* ~8% faster ingest/merge *and* smaller
  files than Go.
- SIMD-validated UTF-8 fast paths, `xxhash` tokenization, and mmap'd part
  reads, mirroring upstream behavior.

### 2. Query-engine parity that mattered (ported from Go)

Profiling exposed that the port initially ran queries *correct but
unoptimized*, and one stub dominated everything:

- **`Query::optimize` / `removeStarFilters`**: in Go, `*` is rewritten to a
  no-op filter. Without that rewrite, `*` executed as an empty-prefix match
  that zstd-decompressed **every `_msg` value in every block** — 21 ms for a
  simple `count()` over 500k rows. Porting the rewrite took it to 0.25 ms.
- **`sort | offset | limit` pipe merging**: Go's optimizer folds these into a
  single top-N sort. The query API now composes the merged form, engaging the
  ported top-N heap executor instead of buffering and sorting all rows.

### 3. Fast paths Go doesn't have

These are the reason the Rust port doesn't just match Go's query latency but
beats it by 2.8–4.5× on the "show me the newest logs" query every UI issues:

- **Search-side block pruning with heap feedback.** For
  `sort by (_time) desc limit N` queries, the block scheduler feeds blocks
  **newest-first**, and the top-N heap publishes a global threshold (the worst
  timestamp in any full heap). Every block whose max timestamp can't beat it is
  **skipped without being read** — no header decode, no decompression, no
  bitmap. A last-100 query touches ~2 of 30 blocks.
- **Monotone-timestamp early exit.** Within a block (timestamps are stored
  sorted), the heap iterates from the best end and abandons the whole block on
  the first losing row; losing rows cost zero allocations.
- **Zero-copy bloom probes.** Go reads and unmarshals the full ~300 KB bloom
  filter per block per query to test a handful of bits. The Rust port probes
  the mmap'd on-disk words in place — ~20 word reads instead of two 300 KB
  copies.
- **Adaptive serial start.** Cheap queries (bloom-pruned filters, header-only
  stats) complete within a ~300 µs inline budget and never wake the thread
  pool — thread wakeups are expensive, especially on Windows. Heavy queries
  fan out onto rayon after at most one warm-up block, which also seeds the
  pruning threshold before parallel workers start.
- **Lean HTTP path.** Worker threads `accept()` directly on the shared
  listener (no acceptor-thread handoff), blocking reads with keep-alive, one
  write + flush per response.
- **Build-level**: `-C target-cpu=x86-64-v3` and PGO for the deployed Windows
  binary.

Every optimization was driven by measurement, not intuition: `perf` on Linux,
env-gated server-side timing (`ESL_QUERY_TIMING=1`, `ESL_HTTP_TIMING=1`) to
split client, HTTP, and query-engine costs, and re-running the full benchmark
after each change. The dead ends are documented in
[bench/BASELINE.md](bench/BASELINE.md) too.

---

## For sponsors

**Why this project**: it demonstrates, with a reproducible harness, that a
memory-safe Rust implementation of a production log database can beat a mature,
heavily-optimized Go implementation on *every* axis at once — including the two
that dominate operating cost (CPU and peak memory: −8–13% and −53–55%) — while
staying wire- and format-compatible enough to be a drop-in single-node server.

**Where it stands**: functionally complete single-node port, 773 ported
upstream tests green, benchmark goal met on Linux and Windows.

**What support enables** (roughly in order):

1. Hardening for production use: crash-recovery soak testing, fuzzing the
   LogsQL parser and ingest paths, wider corpus/benchmark matrix (high-cardinality
   streams, multi-day retention, concurrent query load).
2. Feature completion: remaining select endpoints (`hits`, `field_names`,
   `field_values`), OTLP/journald/DataDog ingestion, auth. (The byte-compatible
   `indexdb` mergeset port has landed — existing Go data directories open in
   place.)
3. Upstream tracking beyond v1.51.0 and an upstreamable benchmark suite.

---

## For contributors

### Layout

| Path | Mirrors (Go upstream) | Purpose |
|---|---|---|
| `crates/esl-common` | Softalink LLC `lib/*` helpers | encoding, fs/mmap, pools, HTTP server, logger |
| `crates/esl-logstorage` | `lib/logstorage`, `lib/prefixfilter` | storage engine + LogsQL (filters, stats, pipes, parser) |
| `crates/esl-insert` | `app/eslinsert` | ingestion endpoints |
| `crates/esl-select` | `app/eslselect` | LogsQL query API |
| `crates/esl-storage` | `app/eslstorage` | storage lifecycle + internal HTTP API |
| `crates/es-logs` | `app/es-logs` | the server binary |
| `bench/` | `deployment/logs-benchmark` | Go-vs-Rust benchmark harness |
| `docs/` | — | port plan (`PORT_PLAN.md`), parity tracker (`PARITY.md`) |

### Conventions

- **This is a port, not a rewrite.** Files, functions, and tests map 1:1 to
  the Go source wherever practical; upstream Go tests are ported alongside the
  code they cover. Intentional divergences are marked with `PORT NOTE:`
  comments at the divergence site and tracked in
  [docs/PARITY.md](docs/PARITY.md).
- Where Go downcasts concrete types (`filter`, `pipe`), the port uses small
  object-safe trait hooks with conservative defaults instead of `Any`
  downcasting — see `Filter::is_match_all` / `Pipe::is_desc_time_topk` for the
  pattern.
- Performance claims require benchmark evidence; the history of what was
  tried, what worked, and what didn't lives in `bench/BASELINE.md`.

### Upstream sync

The port pins its upstream release in [`UPSTREAM.lock`](UPSTREAM.lock), and
**its semver follows upstream's**: the workspace version (and therefore every
crate and release tag) mirrors the pinned tag — currently `1.51.0`.
`upstream_sync.py bump` keeps them in lockstep, `check` and the release
pipeline enforce it.
When upstream ships a new release, `python3 scripts/upstream_sync.py report`
generates a file-level work plan (upstream diff → mapped Rust files) from the
`//! Port of ...` module headers, and `... check` gates that no upstream file
goes uncovered. Full workflow: [docs/UPSTREAM_SYNC.md](docs/UPSTREAM_SYNC.md).

### The gate

Every change must pass all three before commit:

```sh
cargo test --workspace                # 1,123 tests
cargo clippy --workspace              # clean
XWIN_ACCEPT_LICENSE=1 cargo xwin check --target x86_64-pc-windows-msvc --workspace
```

The MSVC check is not optional — Windows is a first-class benchmark target, and
platform-conditional code (`allocator`, mmap, timers, sockets) is where ports
rot first.

One-time clone setup: `git config core.hooksPath .githooks` — the tracked
`prepare-commit-msg` hook stamps every commit with the Claude co-author
trailer (this project is co-developed by Softalink LLC and Claude).

CI runs the same gate on every push/PR (`.github/workflows/ci.yml`: lint,
Linux + native-Windows test suites, an end-to-end smoke against a live
server, and the upstream coverage check). Tagging `v*` triggers the release
pipeline (`release.yml`: gated multi-platform binaries with checksums), and a
weekly `upstream-watch.yml` files an issue with a ready-made sync work plan
whenever upstream ships a new release.

### Debug/diagnostic aids

| Env var | Effect |
|---|---|
| `ESL_QUERY_TIMING=1` | per-query `run_query`/response-write timing + block-scan counters on stderr |
| `ESL_HTTP_TIMING=1` | per-request read/handle/flush split on stderr |
| `ESL_EXIT_AFTER_SECS=N` | graceful self-shutdown after N seconds (PGO training, soak scripts) |

## License

Apache-2.0. EsLogs is a Softalink LLC product and a derivative work of
[VictoriaLogs](https://github.com/VictoriaMetrics/VictoriaLogs) by
[VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics), whose
attribution is preserved per the license; see [LICENSE](LICENSE) and
[NOTICE](NOTICE).

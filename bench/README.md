# Benchmark harness — Go vs Rust EsLogs

Head-to-head benchmark comparing the Rust port against the upstream Go
EsLogs **v1.51.0** on the metrics that matter for the project goal:
ingestion throughput, CPU, peak memory, disk usage, and query latency.

Rather than reproduce the upstream docker/filebeat/Grafana stack, this harness
drives each server directly with an identical corpus over the same native
ingestion API and measures the server process itself. That makes it
deterministic, dependency-light, and portable to Windows (see `run_bench.ps1`).

## Components

| File | Role |
|------|------|
| `loadgen/` | Dependency-free Rust load generator (raw HTTP/1.1 over std TCP, keep-alive). Two subcommands: `corpus` (build a byte-identical JSONL corpus from `.log` files) and `replay` (POST it at a fixed or unbounded rate over N connections, report throughput). Detached from the main workspace so it never affects the `esl-*` build or MSVC checks. |
| `run_bench.sh` | Linux runner: starts one server, replays the corpus, measures CPU / peak-RSS / disk / query latency, emits a result JSON. |
| `run_bench.ps1` | Windows (MSVC) runner: same protocol, using `Get-Process`/`GetProcessTimes` + `PeakWorkingSet64` for CPU/RSS. |
| `compare.py` | Reads two result JSONs and prints the metric-by-metric win/loss table. Exit 0 **iff** Rust wins (within tolerance) on every metric — the project goal gate. |

## Metrics & win criteria

Per `docs/PARITY.md`, the goal is Rust strictly better on **every** metric:

| Metric | Source | Better |
|--------|--------|--------|
| Ingest throughput (rec/s) | loadgen: records ÷ wall-clock | higher |
| CPU seconds | `/proc/PID/stat` utime+stime (Linux) / `GetProcessTimes` (Win) | lower |
| Peak RSS (bytes) | `/proc/PID/status` `VmHWM` (Linux) / `PeakWorkingSet64` (Win) | lower |
| Disk usage (bytes) | `du -sb` of the data dir after flush+merge | lower |
| Query latency (ms) | wall-clock of representative LogsQL queries | lower |

`VmHWM` is a kernel-maintained high-water mark, so peak RSS is read once at the
end — no sampling loop needed.

## Running

```sh
# 1. Build the load generator (once):
(cd loadgen && cargo build --release)

# 2. Get logs. Either the upstream corpus (deployment/logs-benchmark/source_logs,
#    up to 49 GB) or any directory of *.log files. Then build a shared corpus:
./loadgen/target/release/esl-loadgen corpus --logs <logs-dir> --out corpus.jsonl [--max-lines N]

# 3. Run each server against the SAME corpus:
./run_bench.sh --bin <go-es-logs>   --label go   --corpus corpus.jsonl --out go.json
./run_bench.sh --bin <rust-es-logs> --label rust --corpus corpus.jsonl --out rust.json

# 4. Compare:
python3 compare.py go.json rust.json          # exit 0 iff Rust wins everything
```

Use `--rate N` on `run_bench.sh` (records/sec, 0 = unbounded) to measure
steady-state resource usage under the upstream default of 10 000 rec/s, or leave
it unbounded to measure peak throughput. `--conns` sets ingest concurrency.

## Fairness notes

- Both servers get **identical** flags: `-storageDataPath`, `-httpListenAddr`,
  `-retentionPeriod=10y`. The retention override is required because the corpus
  uses a fixed (deterministic) base timestamp; without it EsLogs silently
  drops out-of-retention rows (`esl_rows_dropped_total{reason="too_small_timestamp"}`)
  — which returns HTTP 200 and looks like a huge, fake throughput.
- Both replay the same in-memory batches from the same corpus file.
- Disk is measured only after `/internal/force_flush` + `/internal/force_merge`
  so in-memory parts are on disk and comparably compacted.
- Ingestion uses `/insert/jsonline` (native ndjson). The upstream benchmark uses
  the Elasticsearch bulk API via filebeat; both exercise the same
  parse→LogRows→storage path, which is what these metrics measure. Pass
  `--ingest-path` to switch endpoints.

## Status

The Go baseline path is validated end-to-end (server start, retention-correct
ingestion, query, disk, CPU, peak-RSS). The Rust side plugs in once the app
layer (`crates/es-logs`) implements the matching flags and
`/insert/jsonline` + `/select/logsql/query` endpoints. Windows measurement of
the "wins on Windows" criterion requires real Windows hardware — cross-compiled
binaries can be produced from Linux, but faithful CPU/RSS measurement cannot.

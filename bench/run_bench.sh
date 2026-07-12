#!/usr/bin/env bash
# Runs one EsLogs server (Go or Rust) under an identical ingestion load
# and records the benchmark metrics as JSON. Linux; measures the server process
# directly via /proc so no docker/cadvisor stack is needed. The Windows
# equivalent is run_bench.ps1.
#
# Usage:
#   run_bench.sh --bin <server-binary> --label <name> --corpus <corpus.jsonl> \
#                --out <result.json> [--port 9428] [--rate 0] [--conns 4] \
#                [--ingest-path '/insert/jsonline?...']
set -euo pipefail

BIN="" LABEL="" CORPUS="" OUT="" PORT=9428 RATE=0 CONNS=4
INGEST_PATH='/insert/jsonline?_stream_fields=source&_msg_field=message&_time_field=@timestamp'
while [ $# -gt 0 ]; do
  case "$1" in
    --bin) BIN="$2"; shift 2;;
    --label) LABEL="$2"; shift 2;;
    --corpus) CORPUS="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --port) PORT="$2"; shift 2;;
    --rate) RATE="$2"; shift 2;;
    --conns) CONNS="$2"; shift 2;;
    --ingest-path) INGEST_PATH="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done
[ -n "$BIN" ] && [ -n "$LABEL" ] && [ -n "$CORPUS" ] && [ -n "$OUT" ] || {
  echo "missing required arg (--bin --label --corpus --out)" >&2; exit 2; }

HERE="$(cd "$(dirname "$0")" && pwd)"
LOADGEN="$HERE/loadgen/target/release/esl-loadgen"
[ -x "$LOADGEN" ] || { echo "build loadgen first: (cd bench/loadgen && cargo build --release)" >&2; exit 1; }

DATA="$(mktemp -d "${TMPDIR:-/tmp}/vlbench-$LABEL.XXXXXX")"
LOG="$(mktemp "${TMPDIR:-/tmp}/vlbench-$LABEL-server.XXXXXX.log")"
CLK_TCK="$(getconf CLK_TCK)"
cleanup() { [ -n "${SRV_PID:-}" ] && kill "$SRV_PID" 2>/dev/null || true; }
trap cleanup EXIT

echo "[$LABEL] starting $BIN (data=$DATA)" >&2
# Free the port in case a prior run's server was orphaned (e.g. its cleanup
# trap was pre-empted by SIGKILL), otherwise the new server fails to bind.
if command -v fuser >/dev/null 2>&1; then fuser -k "$PORT/tcp" 2>/dev/null || true; sleep 0.5; fi
# -retentionPeriod=10y so the deterministic corpus (fixed 2026 base timestamp)
# is never dropped as out-of-retention. Both servers get identical flags.
"$BIN" -storageDataPath="$DATA" -httpListenAddr=":$PORT" -retentionPeriod=10y >"$LOG" 2>&1 &
SRV_PID=$!

# Wait for readiness (HTTP port answering), up to 30s.
ready=0
for _ in $(seq 1 300); do
  if curl -s -o /dev/null "http://127.0.0.1:$PORT/health" 2>/dev/null \
     || curl -s -o /dev/null "http://127.0.0.1:$PORT/" 2>/dev/null; then ready=1; break; fi
  kill -0 "$SRV_PID" 2>/dev/null || { echo "[$LABEL] server died on startup:" >&2; cat "$LOG" >&2; exit 1; }
  sleep 0.1
done
[ "$ready" = 1 ] || { echo "[$LABEL] server not ready" >&2; cat "$LOG" >&2; exit 1; }

echo "[$LABEL] replaying corpus" >&2
REPLAY_JSON="$("$LOADGEN" replay --corpus "$CORPUS" --host 127.0.0.1 --port "$PORT" \
  --path "$INGEST_PATH" --conns "$CONNS" --rate "$RATE" --batch 1000 | tail -1)"

# Let the server flush in-memory parts to disk before measuring disk usage.
curl -s -o /dev/null "http://127.0.0.1:$PORT/internal/force_flush" 2>/dev/null || true
sleep 3
curl -s -o /dev/null "http://127.0.0.1:$PORT/internal/force_merge" 2>/dev/null || true
sleep 2

# CPU seconds = (utime+stime) from /proc/PID/stat, in clock ticks (summed
# across all the server's threads by the kernel). Peak RSS = VmHWM, the
# kernel-maintained high-water mark over the process lifetime (read once).
utime="$(awk '{print $14}' "/proc/$SRV_PID/stat")"
stime="$(awk '{print $15}' "/proc/$SRV_PID/stat")"
cpu_seconds="$(awk "BEGIN{printf \"%.3f\", ($utime+$stime)/$CLK_TCK}")"
peak_rss_kb="$(awk '/^VmHWM:/{print $2}' "/proc/$SRV_PID/status")"; peak_rss_kb="${peak_rss_kb:-0}"
disk_bytes="$(du -sb "$DATA" | awk '{print $1}')"

# Query latency: run a few LogsQL queries, record wall-clock ms each.
query() {
  local q="$1" t0 t1
  t0="$(date +%s.%N)"
  curl -s -o /dev/null "http://127.0.0.1:$PORT/select/logsql/query" \
    --data-urlencode "query=$q" --data-urlencode "limit=100" 2>/dev/null || true
  t1="$(date +%s.%N)"
  awk "BEGIN{printf \"%.1f\", ($t1-$t0)*1000}"
}
q_all="$(query '*')"
q_phrase="$(query 'error')"
q_stats="$(query '* | stats count() rows')"

throughput="$(echo "$REPLAY_JSON" | sed -n 's/.*"throughput_rps":\([0-9.]*\).*/\1/p')"
records="$(echo "$REPLAY_JSON" | sed -n 's/.*"records":\([0-9]*\).*/\1/p')"
peak_rss_bytes=$(( peak_rss_kb * 1024 ))

cat > "$OUT" <<JSON
{
  "label": "$LABEL",
  "records": ${records:-0},
  "throughput_rps": ${throughput:-0},
  "cpu_seconds": $cpu_seconds,
  "peak_rss_bytes": $peak_rss_bytes,
  "disk_bytes": $disk_bytes,
  "query_ms": { "match_all": ${q_all:-0}, "phrase": ${q_phrase:-0}, "stats_count": ${q_stats:-0} }
}
JSON
echo "[$LABEL] done -> $OUT" >&2
cat "$OUT" >&2
kill "$SRV_PID" 2>/dev/null || true
# Wait for the server to actually exit so no child lingers for the caller.
for _ in $(seq 1 50); do kill -0 "$SRV_PID" 2>/dev/null || break; sleep 0.1; done
kill -9 "$SRV_PID" 2>/dev/null || true
rm -rf "$DATA" "$LOG" 2>/dev/null || true
trap - EXIT

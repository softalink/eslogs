#!/usr/bin/env python3
"""Compare two run_bench.sh result JSONs (Go vs Rust) and print a metric-by-
metric win/loss table. Exit code 0 iff Rust wins (or ties within tolerance) on
every metric — that is the goal gate for this project.

Usage: compare.py <go_result.json> <rust_result.json> [--tolerance 0.0]
"""
import json
import sys

# For each metric: (label, key, higher_is_better, path)
METRICS = [
    ("Ingest throughput (rec/s)", "throughput_rps", True, None),
    ("CPU seconds", "cpu_seconds", False, None),
    ("Peak RSS (bytes)", "peak_rss_bytes", False, None),
    ("Disk usage (bytes)", "disk_bytes", False, None),
    ("Query match_all (ms)", "match_all", False, ("query_ms",)),
    ("Query phrase (ms)", "phrase", False, ("query_ms",)),
    ("Query stats_count (ms)", "stats_count", False, ("query_ms",)),
]


def get(d, key, path):
    if path:
        for p in path:
            d = d[p]
    return d[key]


def fmt(v):
    if isinstance(v, float):
        return f"{v:,.1f}"
    return f"{v:,}"


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    tol = 0.0
    if "--tolerance" in sys.argv:
        tol = float(sys.argv[sys.argv.index("--tolerance") + 1])
    if len(args) != 2:
        print(__doc__)
        sys.exit(2)
    go = json.load(open(args[0]))
    rust = json.load(open(args[1]))

    rows = []
    all_win = True
    for label, key, higher_better, path in METRICS:
        g = float(get(go, key, path))
        r = float(get(rust, key, path))
        if higher_better:
            # Rust wins if r >= g*(1-tol)
            win = r >= g * (1 - tol)
            delta = (r - g) / g * 100 if g else 0.0
        else:
            win = r <= g * (1 + tol)
            delta = (r - g) / g * 100 if g else 0.0
        all_win = all_win and win
        rows.append((label, g, r, delta, win))

    w = max(len(r[0]) for r in rows)
    print(f"{'Metric':<{w}}  {'Go':>16}  {'Rust':>16}  {'Δ%':>8}  Result")
    print("-" * (w + 56))
    for label, g, r, delta, win in rows:
        mark = "RUST WINS" if win else "rust loses"
        print(f"{label:<{w}}  {fmt(g):>16}  {fmt(r):>16}  {delta:>+7.1f}%  {mark}")
    print("-" * (w + 56))
    print("GOAL GATE:", "PASS — Rust wins all metrics" if all_win
          else "FAIL — Rust does not yet win every metric")
    sys.exit(0 if all_win else 1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Create CSV and Markdown summaries from run_3fs_perf.py JSONL output."""

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path


def cv(values):
    return statistics.stdev(values) / statistics.mean(values) if len(values) > 1 and statistics.mean(values) else 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("results", type=Path)
    p.add_argument("--output", required=True, type=Path)
    args = p.parse_args()
    records = [json.loads(line) for line in args.results.read_text().splitlines() if line.strip()]
    groups = defaultdict(list)
    for record in records:
        if not record["warmup"]:
            for phase in record["phases"]:
                groups[(record["adapter"], record["mode"], record["value_size"],
                        record["threads"], phase["phase"])].append((record, phase))
    rows = []
    for key, samples in sorted(groups.items()):
        valid = [(r, s) for r, s in samples if r["valid"]]
        tp = [s["MiB/s"] for _, s in valid]
        p99 = [s["lat_us_p99"] for _, s in valid]
        rows.append({"adapter": key[0], "mode": key[1], "value_size": key[2],
                     "threads": key[3], "phase": key[4], "runs": len(samples),
                     "valid_runs": len(valid), "mib_s_median": statistics.median(tp) if tp else 0,
                     "mib_s_min": min(tp) if tp else 0, "mib_s_max": max(tp) if tp else 0,
                     "mib_s_cv": cv(tp), "p99_us_median": statistics.median(p99) if p99 else 0,
                     "stable": bool(tp) and cv(tp) <= .10 and len(valid) >= 3})
    args.output.mkdir(parents=True, exist_ok=True)
    csv_path = args.output / "micro-summary.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else [])
        if rows:
            writer.writeheader(); writer.writerows(rows)
    by_key = {(r["adapter"], r["mode"], r["value_size"], r["threads"], r["phase"]): r
              for r in rows}
    comparisons = []
    for r in rows:
        if r["adapter"] == "hf3fs":
            base = by_key.get(("posix", r["mode"], r["value_size"], r["threads"], r["phase"]))
            if base and base["mib_s_median"] and base["p99_us_median"]:
                comparisons.append({"comparison": "hf3fs/posix", "mode": r["mode"],
                                    "value_size": r["value_size"], "threads": r["threads"],
                                    "phase": r["phase"],
                                    "throughput_ratio": r["mib_s_median"] / base["mib_s_median"],
                                    "p99_ratio": r["p99_us_median"] / base["p99_us_median"]})
        if r["mode"] == "backend":
            base = by_key.get((r["adapter"], "adapter", r["value_size"], r["threads"], r["phase"]))
            if base and base["mib_s_median"] and base["p99_us_median"]:
                comparisons.append({"comparison": "backend/adapter", "mode": r["adapter"],
                                    "value_size": r["value_size"], "threads": r["threads"],
                                    "phase": r["phase"],
                                    "throughput_ratio": r["mib_s_median"] / base["mib_s_median"],
                                    "p99_ratio": r["p99_us_median"] / base["p99_us_median"]})
    comparison_path = args.output / "micro-comparisons.csv"
    with comparison_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(comparisons[0]) if comparisons else [])
        if comparisons:
            writer.writeheader(); writer.writerows(comparisons)
    md = ["# Mooncake 3FS micro-benchmark summary", "",
          "> Read results are write-then-read warm-cache observations, not raw-device or cold-cache measurements.", "",
          "| adapter | mode | size | threads | phase | median MiB/s | median P99 us | CV | valid | stable |",
          "|---|---|---:|---:|---|---:|---:|---:|---:|---|"]
    for r in rows:
        md.append(f"| {r['adapter']} | {r['mode']} | {r['value_size']} | {r['threads']} | {r['phase']} | {r['mib_s_median']:.2f} | {r['p99_us_median']:.2f} | {r['mib_s_cv']:.2%} | {r['valid_runs']}/{r['runs']} | {'yes' if r['stable'] else 'no'} |")
    if comparisons:
        md += ["", "## Comparisons", "",
               "| comparison | mode/adapter | size | threads | phase | throughput ratio | P99 ratio |",
               "|---|---|---:|---:|---|---:|---:|"]
        for r in comparisons:
            md.append(f"| {r['comparison']} | {r['mode']} | {r['value_size']} | {r['threads']} | {r['phase']} | {r['throughput_ratio']:.3f} | {r['p99_ratio']:.3f} |")
    (args.output / "micro-summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

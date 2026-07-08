#!/usr/bin/env python3
"""Reproducible Mooncake DFS micro-benchmark matrix runner.

Run this on the Open3FS client node.  It never removes its output root and
uses a fresh directory for every invocation of dfs_backend_bench.
"""

import argparse
import json
import math
import os
import re
import statistics
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


STAT_RE = re.compile(r"^(phase=\S+ .*)$", re.MULTILINE)


def parse_kv(line):
    result = {}
    for token in line.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        try:
            result[key] = float(value) if "." in value else int(value)
        except ValueError:
            result[key] = value
    return result


def run(command, timeout, log_path):
    started = time.monotonic()
    # CMake's RUNPATH points at the matching build tree. A developer shell may
    # otherwise inject an older installed libmooncake_store through
    # LD_LIBRARY_PATH, producing misleading symbol lookup failures.
    env = os.environ.copy()
    env.pop("LD_LIBRARY_PATH", None)
    proc = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=timeout, env=env)
    log_path.write_text(proc.stdout, encoding="utf-8")
    return proc.returncode, proc.stdout, time.monotonic() - started


def cv(values):
    if len(values) < 2 or statistics.mean(values) == 0:
        return 0.0
    return statistics.stdev(values) / statistics.mean(values)


def point_id(adapter, mode, size, threads):
    return f"{adapter}-{mode}-{size}-t{threads}"


def append_jsonl(path, record):
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, sort_keys=True) + "\n")


def matrix(args):
    sizes = [4096, 128 * 1024, 1024 * 1024]
    adapters = [args.adapter] if args.adapter != "all" else ["posix", "hf3fs"]
    for adapter in adapters:
        for mode, threads_set in (("adapter", [1, 8]),
                                  ("backend", [1, 4, 8])):
            for size in sizes:
                for threads in threads_set:
                    yield adapter, mode, size, threads


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bench-bin", required=True, type=Path)
    parser.add_argument("--posix-root", default="/tmp/mooncake_perf")
    parser.add_argument("--hf3fs-root", default="/mnt/3fs/mooncake_perf")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--adapter", choices=["all", "posix", "hf3fs"],
                        default="all")
    parser.add_argument("--bytes-per-phase", type=int, default=2 << 30)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--max-extra-repetitions", type=int, default=2)
    parser.add_argument("--cv-limit", type=float, default=0.10)
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()

    if not args.bench_bin.is_file():
        parser.error(f"benchmark binary not found: {args.bench_bin}")
    args.output.mkdir(parents=True, exist_ok=True)
    logs = args.output / "logs"
    logs.mkdir(exist_ok=True)
    results_path = args.output / "micro-results.jsonl"
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "run_id": uuid.uuid4().hex,
        "bench_bin": str(args.bench_bin.resolve()),
        "bytes_per_phase": args.bytes_per_phase,
        "repetitions": args.repetitions,
        "cv_limit": args.cv_limit,
        "cache_semantics": "write-then-read warm-cache path",
    }
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    failed = False
    for adapter, mode, size, threads in matrix(args):
        pid = point_id(adapter, mode, size, threads)
        root = args.posix_root if adapter == "posix" else args.hf3fs_root
        objects = max(32, math.ceil(args.bytes_per_phase / size / 32) * 32)
        # Four equally used shards, with 25% headroom, rounded to 4 KiB.
        capacity = math.ceil(objects * size * 1.25 / 4 / 4096) * 4096
        measured = []
        wanted = args.repetitions
        attempt = 0
        while attempt < args.warmups + wanted:
            warmup = attempt < args.warmups
            label = "warmup" if warmup else f"run{attempt - args.warmups + 1}"
            storage_path = str(Path(root) / manifest["run_id"] / pid / label)
            command = [str(args.bench_bin), f"--fs_adapter={adapter}",
                       f"--mode={mode}", "--test=all", f"--storage_path={storage_path}",
                       f"--value_size={size}", f"--num_objects={objects}",
                       "--batch_size=32", f"--num_threads={threads}",
                       "--shard_count=4", f"--shard_capacity={capacity}",
                       "--alignment=4096", "--warmup_batches=0",
                       "--verify=true", "--skip_cleanup=false"]
            log_path = logs / f"{pid}-{label}.log"
            try:
                rc, output, elapsed = run(command, args.timeout, log_path)
            except subprocess.TimeoutExpired as exc:
                output = (exc.stdout or "") + "\nTIMEOUT\n"
                log_path.write_text(output, encoding="utf-8")
                rc, elapsed = 124, args.timeout
            phases = [parse_kv(line) for line in STAT_RE.findall(output)]
            record = {"point": pid, "adapter": adapter, "mode": mode,
                      "value_size": size, "threads": threads, "warmup": warmup,
                      "attempt": attempt + 1, "returncode": rc,
                      "elapsed_s": elapsed, "command": command, "phases": phases,
                      "valid": rc == 0 and len(phases) == 2 and
                               all(p.get("errors", 1) == 0 for p in phases)}
            append_jsonl(results_path, record)
            print(f"{pid} {label}: rc={rc} valid={record['valid']}", flush=True)
            if not warmup:
                measured.append(record)
            if not record["valid"]:
                failed = True
            attempt += 1
            if attempt == args.warmups + wanted:
                throughputs = [p["MiB/s"] for r in measured if r["valid"]
                               for p in r["phases"] if p["phase"] == "write"]
                if (len(throughputs) == wanted and cv(throughputs) > args.cv_limit
                        and wanted < args.repetitions + args.max_extra_repetitions):
                    wanted += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Run the standard Mooncake Store DFS matrix against an already started master.

Start mooncake_master with the selected DFS adapter/root before invoking this
script. Run it once with ``--adapter=posix`` and once with ``--adapter=hf3fs``.
"""

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path


PHASE_RE = re.compile(r"=== phase ([^=]+) ===")
METRIC_RE = re.compile(r"([A-Za-z_/]+)=([0-9.]+)(?:ms|s)?")
DATASET_EXHAUSTED_RE = re.compile(r"dataset_exhausted=(True|False)")


def phases(output):
    parsed, current = [], None
    for line in output.splitlines():
        match = PHASE_RE.search(line)
        if match:
            current = {"phase": match.group(1).strip()}
            parsed.append(current)
        elif current is not None:
            for key, value in METRIC_RE.findall(line):
                current[key] = float(value)
            exhausted = DATASET_EXHAUSTED_RE.search(line)
            if exhausted:
                current["dataset_exhausted"] = exhausted.group(1) == "True"
    return parsed


def append(path, value):
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, sort_keys=True) + "\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bench", required=True, type=Path)
    p.add_argument("--adapter", required=True, choices=["posix", "hf3fs"])
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--python", default=sys.executable)
    p.add_argument("--local-hostname", default="127.0.0.1:50071")
    p.add_argument("--metadata-server", default="http://127.0.0.1:8080/metadata")
    p.add_argument("--master-server", default="127.0.0.1:50051")
    p.add_argument("--runtime", type=int, default=30)
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--timeout", type=int, default=900)
    p.add_argument("--global-segment-size", type=int, default=8 * 1024**3,
                   help="Bytes in the shared MEMORY segment used by the client")
    p.add_argument("--local-buffer-size", type=int, default=256 * 1024**2,
                   help="Bytes in the client-local buffer")
    p.add_argument("--nr-objects", type=int, default=4096,
                   help="Object count for each time-based benchmark")
    p.add_argument("--prepare-objects", type=int, default=4096,
                   help="Object count to create before read or mixed phases")
    p.add_argument("--write-objects", type=int, default=524288,
                   help="Maximum fresh objects for each timed write or mixed phase")
    p.add_argument("--quick", action="store_true",
                   help="Run five representative Store points once per repetition")
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    result_path = args.output / "store-results.jsonl"
    if args.quick:
        points = [("write_perf", 128 * 1024, 1),
                  ("read_perf", 128 * 1024, 4),
                  ("write_perf", 1024 * 1024, 4),
                  ("read_perf", 1024 * 1024, 4),
                  ("mixed_rw", 128 * 1024, 4)]
    else:
        points = [(scenario, size, jobs) for scenario in ("write_perf", "read_perf")
                  for size in (128 * 1024, 1024 * 1024) for jobs in (1, 4)]
        points.append(("mixed_rw", 128 * 1024, 4))
    failed = False
    for scenario, size, jobs in points:
        for repetition in range(1, args.repetitions + 1):
            prefix = f"{args.adapter[:3]}{scenario[:3]}{size // 1024}j{jobs}r{repetition}"
            command = [args.python, str(args.bench), "--scenario", scenario,
                       "--io-api", "plain", "--local-hostname", args.local_hostname,
                       "--metadata-server", args.metadata_server, "--master-server", args.master_server,
                       "--protocol", "tcp", "--numjobs", str(jobs), "--iodepth", "1",
                       "--batch-size", "32", "--runtime", str(args.runtime),
                       "--global-segment-size", str(args.global_segment_size),
                       "--local-buffer-size", str(args.local_buffer_size),
                       "--nr-objects", str(args.nr_objects),
                       "--prepare-objects", str(args.prepare_objects),
                       "--write-objects", str(args.write_objects),
                       "--key-prefix", prefix, "--key-size", "32", "--value-size", str(size),
                       "--memory-replica-num", "1", "--nof-replica-num", "0",
                       "--dfs-replica-num", "1", "--pattern", "0xee"]
            if scenario == "write_perf":
                command.append("--wait-dfs-complete")
            elif scenario == "read_perf":
                command += ["--prepare-mode", "auto", "--force-dfs-read",
                            "--dfs-clear-delay-sec", "1", "--verify"]
            else:
                command += ["--prepare-mode", "auto", "--rwmixread", "70", "--verify"]
            started = time.monotonic()
            try:
                proc = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, timeout=args.timeout)
                rc, output = proc.returncode, proc.stdout
            except subprocess.TimeoutExpired as exc:
                rc, output = 124, (exc.stdout or "") + "\nTIMEOUT\n"
            log = args.output / f"store-{prefix}.log"
            log.write_text(output, encoding="utf-8")
            parsed = phases(output)
            bad_metrics = any(x.get("failed_requests", 0) or x.get("failed_kvs", 0)
                              or x.get("misses", 0) or x.get("verify_failures", 0)
                              or x.get("dataset_exhausted", False)
                              for x in parsed)
            timed_phases = [x for x in parsed if x.get("phase") == scenario]
            short_runtime = (not timed_phases or
                             any(x.get("duration", 0) < args.runtime * 0.98
                                 for x in timed_phases))
            valid = rc == 0 and bool(parsed) and not bad_metrics and not short_runtime
            append(result_path, {"adapter": args.adapter, "scenario": scenario,
                                  "value_size": size, "numjobs": jobs,
                                  "repetition": repetition, "returncode": rc,
                                  "elapsed_s": time.monotonic() - started,
                                  "valid": valid, "short_runtime": short_runtime,
                                  "command": command, "phases": parsed})
            print(f"{prefix}: rc={rc} valid={valid}", flush=True)
            failed |= not valid
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

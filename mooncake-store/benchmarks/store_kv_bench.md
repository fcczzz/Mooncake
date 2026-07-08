# `store_kv_bench.py`

`store_kv_bench.py` is a Mooncake Store end-to-end KV benchmark tool. It talks
to a real Mooncake cluster through the Python `store` binding and can exercise
`put/get` as well as zero-copy `put_from/get_into` style APIs.

## Scope

This tool focuses on object-semantic benchmark scenarios:

- Functional verification with read-after-write validation
- Dataset fill for eviction / capacity tests
- Pure write performance
- Pure read performance
- Read/write mixed mode with "existing-object read + new-object write"

Fault injection, NoF register / unregister, heartbeat trigger, memory segment
unmount, and target-side operations are intentionally out of scope. The tool
supports phase gaps so external tools can finish those operations before the
next phase continues.

## Supported Scenarios

- `verify_write`
  - Fixed-count write followed by full readback verification
- `fill`
  - Fixed-count write used for filling a dataset / eviction watermark
- `write_perf`
  - Time-based or fixed-count write benchmark
- `read_perf`
  - Optional prepare-write phase, then read performance benchmark
- `mixed_rw`
  - Optional prepare-write phase, then mixed "read prepared objects + write new objects"

## APIs

- `--io-api=plain`
  - Single object:
    - `put`
    - `get`
  - Batch:
    - `put_batch`
    - `get_batch`
- `--io-api=zcopy`
  - Single object:
    - `put_from`
    - `get_into`
  - Batch:
    - `batch_put_from`
    - `batch_get_into`

`zcopy` mode automatically allocates temporary user buffers and registers them
with `register_buffer`.

## Key Rules

- Keys are generated deterministically:
  - `{prefix padded/truncated to fit}{16-digit object id}`
- The same `key-prefix`, `key-size`, and `object-id-start` produce the same key sequence
- `verify` currently requires `pattern`
- Any write-involved scenario requires `value-size` to be 512-byte aligned
- `memory-replica-num`, `nof-replica-num`, and `dfs-replica-num` cannot all be `0`
- `dfs-replica-num` currently supports `0` or `1`
- `dfs-replica-num=1` requires `memory-replica-num > 0`
- `--force-dfs-read` expects `memory-replica-num=1`, `nof-replica-num=0`, and `dfs-replica-num=1`
- `prepare-objects`
  - Controls how many objects are written by the prepare phase
  - `0` means reuse `nr-objects`

## DFS Modes

For DFS end-to-end runs, start `mooncake_master` with:

```bash
export MOONCAKE_ENABLE_DFS=1
export MOONCAKE_DFS_FS_ADAPTER=posix      # or hf3fs
export MOONCAKE_DFS_ROOT_DIR=/tmp/mooncake_posix_dfs_bench
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=distributed_storage_backend
```

When using 3FS, build with `USE_3FS=ON` and run on the node that has the HF3FS
FUSE mount:

```bash
export MOONCAKE_DFS_FS_ADAPTER=hf3fs
export MOONCAKE_DFS_ROOT_DIR=/mnt/3fs/mooncake_test/mooncake_hf3fs_bench
```

`--wait-dfs-complete` polls replica descriptors until every written key has a
`COMPLETE` DFS replica. `--force-dfs-read` additionally waits
`--dfs-clear-delay-sec`, clears the local MEMORY replica with
`batch_replica_clear(segment_name=store.get_hostname())`, and then starts the
read phase so the prepared objects are served from DFS.

## Phase Gap

Phase gaps are used when an external tool needs time to inject a fault or do an
unmount / remount operation.

- `--phase-gap-mode=none`
  - Continue immediately
- `--phase-gap-mode=sleep --phase-gap-sec=N`
  - Sleep before the next phase
- `--phase-gap-mode=manual`
  - Wait for Enter
- `--phase-gap-mode=file --phase-gap-file=/tmp/bench.ready`
  - Wait until the file exists

If `file` mode is used, make sure the marker file does not already exist before
starting the benchmark.

## Common Examples

### 1. Functional verification (`1+0`)

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario verify_write \
  --io-api plain \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 \
  --protocol tcp \
  --global-segment-size $((64*1024*1024)) \
  --local-buffer-size $((32*1024*1024)) \
  --nr-objects 16 \
  --batch-size 4 \
  --key-prefix verify \
  --key-size 20 \
  --value-size 4096 \
  --memory-replica-num 1 \
  --nof-replica-num 0 \
  --verify \
  --pattern 0xab
```

### 2. NoF-only functional verification (`0+1`)

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario verify_write \
  --io-api plain \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 \
  --protocol tcp \
  --global-segment-size 0 \
  --local-buffer-size $((8*1024*1024)) \
  --nr-objects 8 \
  --batch-size 2 \
  --key-prefix nofonly \
  --key-size 20 \
  --value-size 4096 \
  --memory-replica-num 0 \
  --nof-replica-num 1 \
  --verify \
  --pattern 0xcd
```

### 3. Read performance with automatic prepare phase

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario read_perf \
  --prepare-mode auto \
  --phase-gap-mode sleep \
  --phase-gap-sec 1 \
  --io-api plain \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 \
  --protocol tcp \
  --nr-objects 32 \
  --batch-size 4 \
  --runtime 5 \
  --key-prefix readperf \
  --key-size 20 \
  --value-size 4096 \
  --memory-replica-num 1 \
  --nof-replica-num 0 \
  --verify \
  --pattern 0xee
```

### 4. Mixed read/write with initial dataset

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario mixed_rw \
  --prepare-mode auto \
  --io-api zcopy \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 \
  --protocol tcp \
  --nr-objects 64 \
  --write-objects 4096 \
  --batch-size 4 \
  --runtime 10 \
  --rwmixread 70 \
  --key-prefix mixed \
  --key-size 20 \
  --value-size 4096 \
  --memory-replica-num 1 \
  --nof-replica-num 1 \
  --verify \
  --pattern 0x5a
```

In `mixed_rw`, reads are served from the prepared object set, while writes
always use fresh object ids. This keeps the workload as "existing-object read +
new-object write" and avoids key overlap between the read and write streams.

### 5. POSIX DFS write performance

Start the master with `MOONCAKE_DFS_FS_ADAPTER=posix` and
`MOONCAKE_DFS_ROOT_DIR=/tmp/mooncake_posix_dfs_bench`, then run:

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario write_perf \
  --io-api plain \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 \
  --protocol tcp \
  --nr-objects 4096 \
  --batch-size 32 \
  --runtime 30 \
  --key-prefix posixdfs \
  --key-size 24 \
  --value-size $((128*1024)) \
  --memory-replica-num 1 \
  --nof-replica-num 0 \
  --dfs-replica-num 1 \
  --wait-dfs-complete
```

### 6. 3FS DFS forced-read performance

Inside the 3FS runner container, use the HF3FS mount as the DFS root:

```bash
export MOONCAKE_DFS_FS_ADAPTER=hf3fs
export MOONCAKE_DFS_ROOT_DIR=/mnt/3fs/mooncake_test/mooncake_hf3fs_bench

python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario read_perf \
  --prepare-mode auto \
  --io-api plain \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 \
  --protocol tcp \
  --nr-objects 4096 \
  --batch-size 32 \
  --runtime 30 \
  --key-prefix hf3fsdfs \
  --key-size 24 \
  --value-size $((128*1024)) \
  --memory-replica-num 1 \
  --nof-replica-num 0 \
  --dfs-replica-num 1 \
  --force-dfs-read \
  --dfs-clear-delay-sec 1 \
  --verify \
  --pattern 0xee
```

## Output

Each phase prints:

- request counts
- KV counts
- miss / verify-failure counts
- bytes processed
- duration
- `req/s`
- `kv/s`
- `MiB/s`
- `lat_mean`
- `lat_p50`
- `lat_p95`
- `lat_p99`
- aggregated error counts
- DFS completion and local-replica-clear phases when requested

An overall summary is printed after all phases complete.

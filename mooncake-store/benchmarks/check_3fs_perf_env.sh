#!/usr/bin/env bash
set -uo pipefail

# Run on open3fs-node01. This script is read-only apart from one uniquely named
# health file under /mnt/3fs; it deliberately performs no cluster recovery.
output_dir=${1:?usage: check_3fs_perf_env.sh OUTPUT_DIR}
mkdir -p "$output_dir"
exec > >(tee "$output_dir/preflight.log") 2>&1

failed=0
check() {
  echo "== $1 =="
  shift
  "$@" || failed=1
}

date -Is
uname -a
check "ib0" ip -br -4 addr show ib0
check "mount" findmnt -R /mnt/3fs -o TARGET,SOURCE,FSTYPE,PROPAGATION,OPTIONS
check "containers" docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'

admin=(docker exec 3fs-mgmtd /opt/3fs/bin/admin_cli --cfg /opt/3fs/etc/admin_cli.toml)
check "nodes" "${admin[@]}" list-nodes
check "targets" bash -c 'docker exec 3fs-mgmtd /opt/3fs/bin/admin_cli --cfg /opt/3fs/etc/admin_cli.toml list-targets | awk '\''NR>1 {pub[$4]++; loc[$5]++} END {for (k in pub) print "PUBLIC",k,pub[k]; for (k in loc) print "LOCAL",k,loc[k]}'\'' | sort'
check "chains" bash -c 'docker exec 3fs-mgmtd /opt/3fs/bin/admin_cli --cfg /opt/3fs/etc/admin_cli.toml list-chains | awk '\''NR>1 {c[$4]++} END {for (k in c) print k,c[k]}'\'' | sort'

health_file="/mnt/3fs/mooncake-perf-health-$(date +%s)-$$.txt"
check "FUSE write/read" bash -c "printf 'mooncake-perf-health %s\n' '\$(date -Is)' > '$health_file' && cat '$health_file'"

for path in /sys/class/net/ib0/statistics/*; do
  [[ -r "$path" ]] && printf '%s=%s\n' "$(basename "$path")" "$(<"$path")"
done > "$output_dir/ib0-before.txt"
docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' > "$output_dir/container-stats.csv" || failed=1

if (( failed )); then
  echo "PREFLIGHT_FAILED"
  exit 1
fi
echo "PREFLIGHT_OK"

#!/usr/bin/env bash
# Disk + host baseline for HFresh search benchmarks (Phase 0 of
# docs/hfresh-search-optimization.md). Run once per rig BEFORE any benchmark
# and archive the output next to the benchmark results: every QPS number is
# only interpretable relative to what the disk can actually do.
#
# Usage:
#   ./tools/dev/hfresh_rig_baseline.sh /path/on/target/volume [out_dir]
#
# The first argument must be a directory ON THE VOLUME that holds the Weaviate
# data (fio needs to test the same device). Requires: fio, jq (optional).
set -euo pipefail

TARGET_DIR=${1:?"usage: $0 <dir-on-data-volume> [out-dir]"}
OUT_DIR=${2:-"hfresh_baseline_$(date +%Y%m%d_%H%M%S)"}
mkdir -p "$OUT_DIR"

echo "==> Host info"
{
  date -u
  uname -a
  nproc
  free -h || vm_stat
  lsblk -o NAME,SIZE,TYPE,MOUNTPOINT,ROTA 2>/dev/null || true
  # EBS/NVMe mapping and instance identity, when on EC2
  if command -v curl >/dev/null; then
    TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
      -H "X-aws-ec2-metadata-token-ttl-seconds: 60" || true)
    if [ -n "${TOKEN:-}" ]; then
      echo "instance-type: $(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-type)"
    fi
  fi
} | tee "$OUT_DIR/host_info.txt"

# fio matrix: the three block sizes that matter for the HFresh search path.
#   4k  -> segment index lookups / small metadata reads
#   16k -> rescore object reads (256d objects)
#   48k -> full posting reads (default maxPostingSizeKB)
# QD 1 approximates today's sequential reads; QD 32/64 approximate the
# parallel MultiGet planned in Phase 1.
echo "==> fio random-read matrix on $TARGET_DIR"
for bs in 4k 16k 48k; do
  for qd in 1 8 32 64; do
    name="randread_${bs}_qd${qd}"
    echo "--- $name"
    fio --name="$name" \
      --directory="$TARGET_DIR" \
      --rw=randread \
      --bs="$bs" \
      --iodepth="$qd" \
      --direct=1 \
      --ioengine=libaio \
      --size=4G \
      --runtime=30 \
      --time_based \
      --group_reporting \
      --output-format=json \
      --output="$OUT_DIR/fio_${name}.json"
    if command -v jq >/dev/null; then
      jq -r '.jobs[0].read | "iops=\(.iops | floor) bw_mb=\(.bw / 1024 | floor) lat_mean_us=\(.clat_ns.mean / 1000 | floor) lat_p99_us=\(.clat_ns.percentile["99.000000"] / 1000 | floor)"' \
        "$OUT_DIR/fio_${name}.json"
    fi
  done
done

# cleanup fio data files
rm -f "$TARGET_DIR"/randread_*.0.0 2>/dev/null || true

echo "==> Baseline written to $OUT_DIR"
echo "    Record alongside it: volume type/size, provisioned IOPS and MB/s,"
echo "    and the instance's EBS bandwidth cap (it can bind below the volume)."

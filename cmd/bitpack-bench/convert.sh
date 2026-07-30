#!/usr/bin/env bash
# One-time conversion of an ann-benchmarks HDF5 file to raw little-endian
# binary files readable by the pure-Go loader in this package.
#
# Usage: ./convert.sh <dataset.hdf5> [outdir]
#
# Produces in <outdir> (default: <dataset>.bin/):
#   train.f32     - base vectors, row-major float32 LE
#   test.f32      - query vectors, row-major float32 LE
#   neighbors.i32 - ground-truth neighbor ids, row-major int32 LE
#   meta.txt      - dims and row counts as reported by h5dump
set -euo pipefail

HDF5="$1"
OUT="${2:-${HDF5%.hdf5}.bin}"

command -v h5dump >/dev/null || { echo "h5dump not found (brew install hdf5)" >&2; exit 1; }

mkdir -p "$OUT"

h5dump -H "$HDF5" | awk '
  /DATASET/ { name=$2; gsub(/"/, "", name) }
  /DATASPACE  SIMPLE/ {
    line=$0; sub(/.*\{ \( /, "", line); sub(/ \) \/.*/, "", line)
    gsub(/,/, "", line)
    print name, line
  }' > "$OUT/meta.txt"
cat "$OUT/meta.txt"

for spec in "train:train.f32" "test:test.f32" "neighbors:neighbors.i32"; do
  ds="${spec%%:*}"; out="${spec##*:}"
  echo "dumping $ds -> $OUT/$out ..."
  h5dump -d "/$ds" -b LE -o "$OUT/$out" "$HDF5" > /dev/null
done

echo "done:"
ls -lh "$OUT"

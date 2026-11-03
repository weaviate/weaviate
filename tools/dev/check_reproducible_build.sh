#!/usr/bin/env bash
# Builds the weaviate binary twice with the Makefile and fails if the two builds differ.
# Run from the repository root on a clean checkout. See docs/reproducible-builds.md.
set -euo pipefail

if [[ -n "$(git status --porcelain)" ]]; then
  echo "working tree is dirty; vcs.modified would be stamped into the binary" >&2
  git status --short >&2
  exit 1
fi

out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

for i in 1 2; do
  make weaviate BUILD_USER=ci >/dev/null
  cp cmd/weaviate-server/weaviate "$out/weaviate.$i"
done

a=$(sha256sum "$out/weaviate.1" | cut -d' ' -f1)
b=$(sha256sum "$out/weaviate.2" | cut -d' ' -f1)
if [[ "$a" != "$b" ]]; then
  echo "builds differ: $a != $b" >&2
  echo "build info of the first binary:" >&2
  go version -m "$out/weaviate.1" >&2
  exit 1
fi
echo "reproducible: $a"

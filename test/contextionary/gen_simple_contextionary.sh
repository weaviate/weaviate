#!/usr/bin/env bash
set -e

if [ -f test/contextionary/en_test-vectors-small.txt ]; then
  echo "Already unpacked"
else
  echo "Unpacking fixture vectors"
  bunzip2 -k test/contextionary/en_test-vectors-small.txt.bz2
fi

if [ -f test/contextionary/example.knn ]; then
  echo "Fixture contextionary already generated"
else
  go run contextionary/generator/cmd/generator.go \
    -c test/contextionary/en_test-vectors-small.txt \
    -p test/contextionary/example
fi

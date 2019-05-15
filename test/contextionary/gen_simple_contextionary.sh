#!/usr/bin/env bash
set -e

if [ -f test/contextionary/en_test-vectors-small.txt ]; then
  echo "Already unpacked"
else
  echo "Unpacking fixture vectors"
  bunzip2 -k test/contextionary/en_test-vectors-small.txt.bz2
fi

# Fake stopword removal by removing the first 10 words. This will become
# obsolete once we have released a new minimal c11y

# build stopword.json
cat test/contextionary/en_test-vectors-small.txt | head | \
  while read -r word _; do echo "$word"; done | jq -nR '[inputs | select(length>0)] | { language: "en", words: . }'  > test/contextionary/stopwords.json

# remove stop words
sed -i.bak 1,10d test/contextionary/en_test-vectors-small.txt && rm test/contextionary/en_test-vectors-small.txt.bak

if [ -f test/contextionary/example.knn ]; then
  echo "Fixture contextionary already generated"
else
  go run contextionary/generator/cmd/generator.go \
    -c test/contextionary/en_test-vectors-small.txt \
    -p test/contextionary/example
fi

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest

package inverted

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/keydoccolumn"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
)

// POC benchmark for the resident key/doc column vs the production roaringset
// ContainsAny path, on the shared benchmark fixture (strictly-unique text
// property, 1 doc == 1 value). Build-on-startup only — no memtable layering,
// no updates — so the corpus is stable and we measure resolution alone.
//
// Run:
//
//	go test -tags integrationTest -run '^$' -bench 'KeyDocColumnPOC' \
//	    -benchmem -benchtime 20x -count 3 ./adapters/repos/db/inverted/

func keyDocColumnSortedKeys(values []string) [][]byte {
	keys := make([][]byte, len(values))
	for i, v := range values {
		keys[i] = []byte(v)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	return keys
}

func bucketKeyDocColumn(tb testing.TB, f *containsFixture) *keydoccolumn.Index {
	tb.Helper()
	idx := f.bucket.KeyDocColumn()
	require.NotNil(tb, idx)
	return idx
}

// TestKeyDocColumnPOC_Correctness pins the key/doc column against the sampled
// docIDs and against the production DocIDs path on the same corpus, so a
// benchmark win cannot come from a wrong result.
func TestKeyDocColumnPOC_Correctness(t *testing.T) {
	f := newContainsFixture(t, 20_000)
	ctx := context.Background()
	idx := bucketKeyDocColumn(t, f)

	for _, size := range []int{1, 100, 1_000, 10_000} {
		values, wantSampled := f.sampleValues(size)
		keys := keyDocColumnSortedKeys(values)

		got := idx.ResolvePerKey(keys).Bitmap().ToArray()

		// against the known sampled docIDs
		require.Equal(t, wantSampled, got, "key/doc column vs sampled docIDs at N=%d", size)

		// against the production roaringset path end-to-end
		al, err := f.searcher.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
			additional.Properties{}, className)
		require.NoError(t, err)
		prod := al.Slice()
		al.Close()
		sort.Slice(prod, func(i, j int) bool { return prod[i] < prod[j] })
		require.Equal(t, prod, got, "key/doc column vs production DocIDs at N=%d", size)
	}
}

// BenchmarkKeyDocColumnPOC measures resolution over the shared 300K corpus at
// N=1K/10K/100K (query-to-corpus ratios 1:300, 1:30, 1:3). Compare against the
// roaringset_point rows from BenchmarkFlatIndexPOC / BenchmarkDocIDs_ContainsAny.
func BenchmarkKeyDocColumnPOC(b *testing.B) {
	f := newContainsFixture(b, benchCorpusSize)
	idx := bucketKeyDocColumn(b, f)
	b.Logf("key/doc column built: %d keys", idx.Info().Keys)

	for _, size := range []int{1_000, 10_000, 100_000} {
		values, _ := f.sampleValues(size)
		keys := keyDocColumnSortedKeys(values)
		b.Run(fmt.Sprintf("N=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				idx.ResolvePerKey(keys).Bitmap()
			}
		})
	}
}

// BenchmarkKeyDocColumnPOC_Build measures the one-time startup build cost over
// the full corpus — the price paid once per property per shard on load. The
// index is built when the bucket opens, so each iteration reopens it; the
// "no_index" row is the same reopen without the index, and the gap between the
// two is what the index costs at load.
func BenchmarkKeyDocColumnPOC_Build(b *testing.B) {
	for _, withIndex := range []bool{false, true} {
		name := "no_index"
		if withIndex {
			name = "index"
		}
		b.Run(name, func(b *testing.B) {
			f := newContainsFixture(b, benchCorpusSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f.reopenBucket(b, withIndex)
			}
		})
	}
}

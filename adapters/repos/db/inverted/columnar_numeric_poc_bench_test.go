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
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/usecases/config"
)

// POC benchmark for the FIXED-WIDTH columnar key backing. The shared text
// fixture exercises blobKeyColumn (variable-length); this builds a standalone
// int64 roaringset corpus so BuildFromBucket selects fixedKeyColumn (8-byte
// LexicographicallySortableInt64 keys) and we can measure the fixed path and
// compare it against roaringset point lookups on the same numeric data.
//
// Run:
//
//	go test -tags integrationTest -run '^$' -bench 'ColumnarIndexNumericPOC' \
//	    -benchmem -benchtime 20x -count 3 ./adapters/repos/db/inverted/

type numericFixture struct {
	store   *lsmkv.Store
	bucket  *lsmkv.Bucket
	numDocs int
}

// newNumericFixture builds a roaringset bucket where int64 value i maps to
// docID i (strictly unique, 1:1), keyed with the analyzer's
// LexicographicallySortableInt64 encoding — so every key is exactly 8 bytes.
func newNumericFixture(tb testing.TB, numDocs int) *numericFixture {
	tb.Helper()
	dir := tb.TempDir()
	logger, _ := test.NewNullLogger()

	bufPool, bufPoolClose := roaringset.NewBitmapBufPoolDefault(logger, nil,
		config.DefaultQueryBitmapBufsMaxBufSize, config.DefaultQueryBitmapBufsMaxMemory)
	tb.Cleanup(bufPoolClose)

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(tb, err)
	tb.Cleanup(func() { store.Shutdown(context.Background()) })

	name := helpers.BucketFromPropNameLSM("numeric-int-roaringset")
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), name,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
	))
	bucket := store.Bucket(name)

	for i := 0; i < numDocs; i++ {
		key, err := entinverted.LexicographicallySortableInt64(int64(i))
		require.NoError(tb, err)
		require.NoError(tb, bucket.RoaringSetAddList(key, []uint64{uint64(i)}))
	}
	require.NoError(tb, bucket.FlushAndSwitch())

	return &numericFixture{store: store, bucket: bucket, numDocs: numDocs}
}

// sampleKeys picks `size` int values spread across the corpus, returns their
// encoded keys (sorted ascending, as ResolveContainsAny requires) and the
// docIDs they resolve to.
func (f *numericFixture) sampleKeys(tb testing.TB, size int) (keys [][]byte, docIDs []uint64) {
	tb.Helper()
	stride := f.numDocs / size
	if stride < 1 {
		stride = 1
	}
	for i := 0; i < f.numDocs && len(keys) < size; i += stride {
		key, err := entinverted.LexicographicallySortableInt64(int64(i))
		require.NoError(tb, err)
		keys = append(keys, key)
		docIDs = append(docIDs, uint64(i))
	}
	// encoding is order-preserving and i is increasing, so keys are already
	// ascending; sort defensively so the contract holds regardless.
	sort.Slice(keys, func(a, b int) bool { return string(keys[a]) < string(keys[b]) })
	return keys, docIDs
}

// numericPointLookup mirrors the production fold's fetch shape: one consistent
// view, N per-key RoaringSetGetFromView lookups, unioned via an accumulator.
func numericPointLookup(ctx context.Context, b *lsmkv.Bucket, keys [][]byte) *sroar.Bitmap {
	view := b.GetConsistentView()
	defer view.ReleaseView()
	acc := sroar.NewAccumulator()
	for _, k := range keys {
		bm, release, err := b.RoaringSetGetFromView(ctx, view, k)
		if err != nil {
			panic(err)
		}
		acc.Or(bm)
		release()
	}
	return acc.Bitmap()
}

// TestColumnarIndexNumericPOC_Correctness confirms the fixed-width backing is
// selected and that it resolves the same docIDs as point lookups.
func TestColumnarIndexNumericPOC_Correctness(t *testing.T) {
	f := newNumericFixture(t, 20_000)
	ctx := context.Background()

	idx, err := columnar.BuildFromBucket(f.bucket, uint64(f.numDocs+1), false, columnarTestLogger())
	require.NoError(t, err)
	require.Equal(t, 8, idx.Info().KeyWidth, "int64 corpus must select the fixed 8-byte key backing")

	for _, size := range []int{1, 100, 1_000, 10_000} {
		keys, want := f.sampleKeys(t, size)

		got := idx.ResolveContainsAny(keys).ToArray()
		require.Equal(t, want, got, "columnar vs sampled docIDs at N=%d", size)

		point := numericPointLookup(ctx, f.bucket, keys).ToArray()
		sort.Slice(point, func(i, j int) bool { return point[i] < point[j] })
		require.Equal(t, point, got, "columnar vs point lookups at N=%d", size)
	}
}

// BenchmarkColumnarIndexNumericPOC A/Bs the fixed-width columnar resolve against
// roaringset point lookups on the shared 300K int64 corpus.
func BenchmarkColumnarIndexNumericPOC(b *testing.B) {
	f := newNumericFixture(b, benchCorpusSize)
	ctx := context.Background()

	idx, err := columnar.BuildFromBucket(f.bucket, uint64(f.numDocs+1), false, columnarTestLogger())
	require.NoError(b, err)
	info := idx.Info()
	require.Equal(b, 8, info.KeyWidth)
	b.Logf("numeric columnar index built: %d keys, width=%d", info.Keys, info.KeyWidth)

	for _, size := range []int{1_000, 10_000, 100_000} {
		keys, _ := f.sampleKeys(b, size)

		b.Run(fmt.Sprintf("columnar/N=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				idx.ResolveContainsAny(keys)
			}
		})
		b.Run(fmt.Sprintf("roaringset_point/N=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				numericPointLookup(ctx, f.bucket, keys)
			}
		})
	}
}

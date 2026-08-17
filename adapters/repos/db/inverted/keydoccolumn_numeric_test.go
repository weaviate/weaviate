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
	"sort"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/usecases/config"
)

// Coverage for the fixed-width key backing. The shared text fixture exercises
// blobKeyColumn (variable-length); this builds a standalone int64 roaringset
// corpus so the build selects fixedKeyColumn instead — 8-byte
// LexicographicallySortableInt64 keys — which nothing else here reaches.

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

	// Reopen with the index option now that the corpus is in segments — a bucket
	// builds its index at open, over the segments it has then.
	require.NoError(tb, store.ShutdownBucket(context.Background(), name))
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), name,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
		lsmkv.WithKeyDocColumn(true),
		lsmkv.WithDocIDCountGetter(newFakeMaxIDGetter(uint64(numDocs+1))),
	))
	bucket = store.Bucket(name)

	return &numericFixture{store: store, bucket: bucket, numDocs: numDocs}
}

// sampleKeys picks `size` int values spread across the corpus, returns their
// encoded keys (sorted ascending, as Resolve requires) and the
// docIDs they resolve to.
func (f *numericFixture) sampleKeys(tb testing.TB, size int) (keys entinverted.SortedKeys, docIDs []uint64) {
	tb.Helper()
	stride := f.numDocs / size
	if stride < 1 {
		stride = 1
	}
	// the encoding is order-preserving and i increases, so appending in this
	// order already satisfies the ascending contract
	kb := entinverted.NewVarKeyBuilder(size, size*8)
	for i := 0; i < f.numDocs && len(docIDs) < size; i += stride {
		key, err := entinverted.LexicographicallySortableInt64(int64(i))
		require.NoError(tb, err)
		kb.AppendString(string(key))
		docIDs = append(docIDs, uint64(i))
	}
	keys, err := kb.Build()
	require.NoError(tb, err)
	return keys, docIDs
}

// numericPointLookup mirrors the production fold's fetch shape: one batch
// reader held across N per-key lookups, unioned via an accumulator.
func numericPointLookup(ctx context.Context, b *lsmkv.Bucket, keys entinverted.SortedKeys) *sroar.Bitmap {
	view := b.GetConsistentView()
	defer view.ReleaseView()
	reader, err := lsmkv.NewRoaringSetBatchReader(view.WithoutEmptyActiveMemtable(), keys)
	if err != nil {
		panic(err)
	}

	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)
	acc := sroar.NewAccumulator()
	for i := 0; i < reader.Len(); i++ {
		bm, release, err := reader.Next(mergeConc)
		if err != nil {
			panic(err)
		}
		acc.Or(bm)
		release()
	}
	return acc.Bitmap()
}

// TestKeyDocColumnNumericFixedWidthKeys confirms an int64 corpus selects the
// fixed-width backing, and that resolving through it answers what reading the
// same keys one at a time does.
func TestKeyDocColumnNumericFixedWidthKeys(t *testing.T) {
	f := newNumericFixture(t, 20_000)
	ctx := context.Background()

	require.NotNil(t, f.bucket.KeyDocColumn())
	require.Equal(t, 8, f.bucket.KeyDocColumn().Info().KeyWidth,
		"int64 corpus must select the fixed 8-byte key backing")

	for _, size := range []int{1, 100, 1_000, 10_000} {
		keys, want := f.sampleKeys(t, size)

		// through the view, as a query reads it: the state is pinned with the
		// memtables rather than read when the resolution gets round to it
		view := f.bucket.GetConsistentRoaringSetView()
		reader, err := view.WithoutEmptyActiveMemtable().NewRoaringSetKeyDocColumnReader(keys)
		require.NoError(t, err)
		res, err := reader.Resolve(ctx)
		require.NoError(t, err)
		require.NotNil(t, res, "the bucket carries a column")

		bm, put := res.Materialize(roaringset.NewBitmapBufPoolNoop())
		got := bm.ToArray()
		put()
		view.ReleaseView()
		require.Equal(t, want, got, "key/doc column vs sampled docIDs at N=%d", size)

		point := numericPointLookup(ctx, f.bucket, keys).ToArray()
		sort.Slice(point, func(i, j int) bool { return point[i] < point[j] })
		require.Equal(t, point, got, "key/doc column vs point lookups at N=%d", size)
	}
}

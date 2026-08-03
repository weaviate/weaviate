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
	"os"
	"sort"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// newUniqueTextSearcher builds a Searcher over a STRICTLY unique text roaringset
// property (val_%08d -> docID i, 1:1, no shared values), flushed to disk — the
// conditions under which the columnar accelerator is eligible to serve. Returns
// the searcher and its store.
func newUniqueTextSearcher(tb testing.TB, numDocs int) (*Searcher, *lsmkv.Store) {
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

	name := helpers.BucketFromPropNameLSM(benchPropName)
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), name,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
		lsmkv.WithContainsAcceleratorFactory(columnarTestFactory(numDocs)),
	))
	bucket := store.Bucket(name)
	for i := 0; i < numDocs; i++ {
		require.NoError(tb, bucket.RoaringSetAddList([]byte(benchValue(i)), []uint64{uint64(i)}))
	}
	require.NoError(tb, bucket.FlushAndSwitch())

	bitmapFactory := roaringset.NewBitmapFactory(bufPool, newFakeMaxIDGetter(uint64(numDocs+1)))
	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false },
		func(string) bool { return false }, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory).
		WithBatchedContainsEnabled(configRuntime.NewDynamicValue(true))
	return searcher, store
}

func sortedDocIDs(al helpers.AllowList) []uint64 {
	got := al.Slice()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	return got
}

// columnarTestFactory builds the accelerator at bucket open for tests. The base
// is empty at open (data is written+flushed afterwards, arriving via AbsorbFlush);
// non-unique corpora decline on the flush and detach.
func columnarTestFactory(numDocs int) lsmkv.ContainsAcceleratorFactory {
	return func(bkt *lsmkv.Bucket) lsmkv.ContainsAnyResolver {
		idx, err := columnar.BuildFromBucket(bkt, uint64(numDocs+1), true)
		if err != nil {
			return nil
		}
		return idx
	}
}

// sampleUniqueValues picks `size` unique values spread across [0,numDocs).
func sampleUniqueValues(numDocs, size int) []string {
	stride := numDocs / size
	if stride < 1 {
		stride = 1
	}
	vals := make([]string, 0, size)
	for i := 0; i < numDocs && len(vals) < size; i += stride {
		vals = append(vals, benchValue(i))
	}
	return vals
}

// BenchmarkColumnarWiring_DocIDs measures the full Searcher.DocIDs path for
// ContainsAny on a strictly-unique text property, comparing the standard fold
// (flag off) against the resident columnar accelerator (flag on). This is the
// end-to-end number: extraction + resolution + allowlist, through the real API.
func BenchmarkColumnarWiring_DocIDs(b *testing.B) {
	searcher, _ := newUniqueTextSearcher(b, benchCorpusSize)
	ctx := context.Background()

	modes := []struct{ name, flag string }{{"fold", ""}, {"columnar", "true"}}
	for _, size := range []int{1_000, 10_000, 100_000} {
		values := sampleUniqueValues(benchCorpusSize, size)
		filter := containsFilter(filters.ContainsAny, values)
		for _, m := range modes {
			b.Run(fmt.Sprintf("%s/N=%d", m.name, size), func(b *testing.B) {
				os.Setenv(entcfg.EnvEnableColumnarContains, m.flag)
				defer os.Unsetenv(entcfg.EnvEnableColumnarContains)
				// warm up: builds+caches the accelerator (columnar mode) so the
				// one-time build cost is excluded from the per-op measurement.
				al, err := searcher.DocIDs(ctx, filter, additional.Properties{}, className)
				require.NoError(b, err)
				al.Close()

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					al, err := searcher.DocIDs(ctx, filter, additional.Properties{}, className)
					if err != nil {
						b.Fatal(err)
					}
					al.Close()
				}
			})
		}
	}
}

// TestColumnarWiring_DocIDsMatchesFold drives the real Searcher.DocIDs path and
// pins that ContainsAny resolved via the columnar accelerator (flag on) returns
// exactly the same doc IDs as the standard fold (flag off), and that the
// accelerator actually served (was cached on the bucket), not silently fell
// back.
func TestColumnarWiring_DocIDsMatchesFold(t *testing.T) {
	const numDocs = 20_000
	searcher, store := newUniqueTextSearcher(t, numDocs)
	ctx := context.Background()

	sample := func(size int) []string {
		stride := numDocs / size
		if stride < 1 {
			stride = 1
		}
		vals := make([]string, 0, size)
		for i := 0; i < numDocs && len(vals) < size; i += stride {
			vals = append(vals, benchValue(i))
		}
		return vals
	}

	run := func(values []string) []uint64 {
		al, err := searcher.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
			additional.Properties{}, className)
		require.NoError(t, err)
		defer al.Close()
		return sortedDocIDs(al)
	}

	for _, size := range []int{1, 100, 1_000, 10_000} {
		values := sample(size)

		t.Setenv(entcfg.EnvEnableColumnarContains, "")
		fold := run(values)

		t.Setenv(entcfg.EnvEnableColumnarContains, "true")
		columnar := run(values)

		require.Equal(t, fold, columnar,
			"columnar accelerator must match the fold at N=%d", size)
	}

	// prove the accelerator actually served: it is attached at open and kept
	// live across the flush (via AbsorbFlush), so it is present on the bucket.
	bkt := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	require.NotNil(t, bkt.ContainsAnyAccelerator(),
		"columnar accelerator must be attached and serving")
}

// TestColumnarWiring_UnflushedWrites pins that the accelerator serves correctly
// when there are unflushed writes in the active memtable — new keys and a delete
// — via the memtable overlay, matching the fold, without falling back.
func TestColumnarWiring_UnflushedWrites(t *testing.T) {
	const numDocs = 5_000
	searcher, store := newUniqueTextSearcher(t, numDocs)
	ctx := context.Background()
	bucket := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))

	// unflushed writes into the active memtable (no FlushAndSwitch): two new
	// keys, and a delete of a doc that lives in the flushed run.
	require.NoError(t, bucket.RoaringSetAddList([]byte(benchValue(numDocs)), []uint64{uint64(numDocs)}))
	require.NoError(t, bucket.RoaringSetAddList([]byte(benchValue(numDocs+1)), []uint64{uint64(numDocs + 1)}))
	require.NoError(t, bucket.RoaringSetRemoveOne([]byte(benchValue(5)), 5))

	values := []string{
		benchValue(1),           // flushed, still present → 1
		benchValue(5),           // deleted in memtable → nothing
		benchValue(numDocs),     // new, unflushed → numDocs
		benchValue(numDocs + 1), // new, unflushed → numDocs+1
	}

	run := func(flag string) []uint64 {
		t.Setenv(entcfg.EnvEnableColumnarContains, flag)
		al, err := searcher.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
			additional.Properties{}, className)
		require.NoError(t, err)
		defer al.Close()
		return sortedDocIDs(al)
	}

	fold := run("")
	col := run("true")

	require.Equal(t, fold, col, "columnar + memtable overlay must match the fold with unflushed writes")
	require.Equal(t, []uint64{1, uint64(numDocs), uint64(numDocs + 1)}, col,
		"flushed(1) kept, deleted(5) gone, unflushed new keys present")
	require.NotNil(t, bucket.ContainsAnyAccelerator(),
		"unflushed writes must not detach the accelerator")
}

// TestColumnarWiring_DeclinesNonUnique pins that a non-unique property (a key
// with multiple docIDs) makes the accelerator decline, so the real path falls
// back to the fold and still returns correct (multi-doc) results.
func TestColumnarWiring_DeclinesNonUnique(t *testing.T) {
	// The shared benchmark fixture seeds shared_a/b/c -> {11,17}, so it is
	// non-unique — exactly the decline case.
	f := newContainsFixture(t, 5_000)
	ctx := context.Background()

	values := containsSharedValues // shared_a/b/c -> docs {11,17}

	t.Setenv(entcfg.EnvEnableColumnarContains, "true")
	al, err := f.searcher.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
		additional.Properties{}, className)
	require.NoError(t, err)
	got := sortedDocIDs(al)
	al.Close()

	require.Equal(t, []uint64{11, 17}, got,
		"non-unique key must fall back to the fold and keep BOTH docIDs")

	// the non-unique flush must have detached the accelerator, so the bucket has
	// none and ContainsAny falls back to the fold.
	bkt := f.store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	require.Nil(t, bkt.ContainsAnyAccelerator(),
		"non-unique property must detach the accelerator on flush")
}

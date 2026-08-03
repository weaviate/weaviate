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

	// prove the accelerator actually served: after the flag-on queries above it
	// must be cached on the bucket, so a get-or-build with a build that fails the
	// test returns a non-nil resolver WITHOUT rebuilding.
	bkt := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	view := bkt.GetConsistentView()
	defer view.ReleaseView()
	resolver := bkt.GetOrBuildContainsAnyAccelerator(view, func() lsmkv.ContainsAnyResolver {
		t.Fatal("accelerator should already be cached; build must not be called")
		return nil
	})
	require.NotNil(t, resolver, "columnar accelerator must be cached after flag-on queries")
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

	// the accelerator must be cached as declined (nil resolver) for this bucket.
	bkt := f.store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	view := bkt.GetConsistentView()
	defer view.ReleaseView()
	built := false
	resolver := bkt.GetOrBuildContainsAnyAccelerator(view, func() lsmkv.ContainsAnyResolver {
		built = true
		return nil
	})
	require.Nil(t, resolver, "non-unique property must resolve to a nil (declined) accelerator")
	require.False(t, built, "decline must be cached, not rebuilt")
}

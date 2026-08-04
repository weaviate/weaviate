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
	"math/rand"
	"os"
	"slices"
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

	name := helpers.BucketFromPropNameLSM(benchPropName)
	newStore := func() *lsmkv.Store {
		store, err := lsmkv.New(dir, dir, logger, nil, nil,
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop())
		require.NoError(tb, err)
		return store
	}

	// Phase 1: write + flush the corpus to disk, then close — so the data lives
	// in disk segments, not a memtable.
	{
		store := newStore()
		require.NoError(tb, store.CreateOrLoadBucket(context.Background(), name,
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
			lsmkv.WithBitmapBufPool(bufPool),
		))
		bucket := store.Bucket(name)
		for i := 0; i < numDocs; i++ {
			require.NoError(tb, bucket.RoaringSetAddList([]byte(benchValue(i)), []uint64{uint64(i)}))
		}
		require.NoError(tb, bucket.FlushAndSwitch())
		require.NoError(tb, store.Shutdown(context.Background()))
	}

	// Phase 2: reopen — the accelerator factory now builds its base from the
	// flushed disk segments (like a server restart), so the base carries the
	// corpus rather than it all arriving as a run. This is the realistic
	// always-on shape.
	store := newStore()
	tb.Cleanup(func() { store.Shutdown(context.Background()) })
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), name,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
		lsmkv.WithContainsAcceleratorFactory(columnarTestFactory(numDocs)),
	))

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
// covers whatever disk segments exist at open (all corpus on reopen; empty on a
// fresh bucket, where data then arrives via AbsorbFlush). Non-unique corpora
// decline on the flush and detach.
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

// BenchmarkColumnarLayers measures ContainsAny resolution cost as index tiers are
// added: base (on disk) → base + a populated active memtable → base + several
// flushed runs + a populated active memtable. Each variant runs the SAME query
// (drawn from the base), so the delta is the pure overhead of scanning/overlaying
// the extra tiers, not different match counts. Run:
//
//	go test -tags integrationTest -run '^$' -bench 'ColumnarLayers' \
//	    -benchmem -benchtime 20x -count 3 ./adapters/repos/db/inverted/
func BenchmarkColumnarLayers(b *testing.B) {
	const (
		baseDocs   = 300_000
		runDocs    = 10_000 // keys per flushed run
		activeDocs = 10_000 // unflushed keys in the active memtable
		numRuns    = 3      // below foldRunsThreshold, so they stay as runs
	)

	// populate adds `runs` flushed runs then an unflushed active memtable to the
	// searcher's bucket, using docIDs past the base so they don't collide.
	populate := func(store *lsmkv.Store, runs int) {
		bkt := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
		next := baseDocs
		for r := 0; r < runs; r++ {
			for i := 0; i < runDocs; i++ {
				require.NoError(b, bkt.RoaringSetAddList([]byte(benchValue(next)), []uint64{uint64(next)}))
				next++
			}
			require.NoError(b, bkt.FlushAndSwitch())
		}
		for i := 0; i < activeDocs; i++ {
			require.NoError(b, bkt.RoaringSetAddList([]byte(benchValue(next)), []uint64{uint64(next)}))
			next++
		}
	}

	baseOnly, _ := newUniqueTextSearcher(b, baseDocs)

	withActive, activeStore := newUniqueTextSearcher(b, baseDocs)
	populate(activeStore, 0) // active memtable only

	layered, layeredStore := newUniqueTextSearcher(b, baseDocs)
	populate(layeredStore, numRuns) // runs + active

	variants := []struct {
		name string
		s    *Searcher
	}{
		{"base", baseOnly},
		{"base+active", withActive},
		{"base+3runs+active", layered},
	}

	os.Setenv(entcfg.EnvEnableColumnarContains, "true")
	defer os.Unsetenv(entcfg.EnvEnableColumnarContains)
	ctx := context.Background()

	for _, size := range []int{1_000, 10_000, 100_000} {
		values := sampleUniqueValues(baseDocs, size)
		filter := containsFilter(filters.ContainsAny, values)
		for _, v := range variants {
			b.Run(fmt.Sprintf("%s/N=%d", v.name, size), func(b *testing.B) {
				al, err := v.s.DocIDs(ctx, filter, additional.Properties{}, className)
				require.NoError(b, err)
				al.Close()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					al, err := v.s.DocIDs(ctx, filter, additional.Properties{}, className)
					if err != nil {
						b.Fatal(err)
					}
					al.Close()
				}
			})
		}
	}
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

	requireMatchesFold := func(t *testing.T, values []string) {
		t.Helper()
		t.Setenv(entcfg.EnvEnableColumnarContains, "")
		fold := run(values)

		t.Setenv(entcfg.EnvEnableColumnarContains, "true")
		columnar := run(values)

		require.Equal(t, fold, columnar, "columnar accelerator must match the fold")
	}

	// The filter's value order is the user's, and the accelerator requires its
	// keys ascending — so every ordering must produce the same result.
	orderings := []struct {
		name    string
		reorder func([]string) []string
	}{
		{name: "ascending", reorder: func(v []string) []string { return v }},
		{
			name: "descending",
			reorder: func(v []string) []string {
				slices.Reverse(v)
				return v
			},
		},
		{
			name: "shuffled",
			reorder: func(v []string) []string {
				rnd := rand.New(rand.NewSource(42))
				rnd.Shuffle(len(v), func(i, j int) { v[i], v[j] = v[j], v[i] })
				return v
			},
		},
	}

	for _, size := range []int{1, 100, 1_000, 10_000} {
		for _, ord := range orderings {
			t.Run(fmt.Sprintf("N=%d/%s", size, ord.name), func(t *testing.T) {
				requireMatchesFold(t, ord.reorder(sample(size)))
			})
		}
	}

	// A value shorter than the corpus keys' shared prefix matches nothing, but it
	// still has to be compared against that prefix without running off its end.
	t.Run("value shorter than the shared key prefix", func(t *testing.T) {
		requireMatchesFold(t, append([]string{"val", "v"}, sample(100)...))
	})

	// Repeated values become repeated keys, which the merge-scan sees as two
	// query cursor positions against one corpus position.
	t.Run("repeated values", func(t *testing.T) {
		values := sample(1_000)
		requireMatchesFold(t, append(values, values...))
	})

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

// TestColumnarWiring_ManyFlushes drives more flushes than the fold threshold, so
// the accumulated runs are folded into the base (through real memtable cursors),
// and pins that ContainsAny still matches the fold across base + fold + recent
// runs, with adds and deletes.
func TestColumnarWiring_ManyFlushes(t *testing.T) {
	const numDocs = 1_000
	searcher, store := newUniqueTextSearcher(t, numDocs) // base holds 0..999
	ctx := context.Background()
	bucket := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))

	// 10 flushes (> the fold threshold): each adds one new key and deletes one
	// existing doc, exercising the fold's add and delete paths on real cursors.
	for r := 0; r < 10; r++ {
		newKey := numDocs + r
		require.NoError(t, bucket.RoaringSetAddList([]byte(benchValue(newKey)), []uint64{uint64(newKey)}))
		require.NoError(t, bucket.RoaringSetRemoveOne([]byte(benchValue(r)), uint64(r)))
		require.NoError(t, bucket.FlushAndSwitch())
	}

	values := []string{
		benchValue(500),         // base, never deleted → 500
		benchValue(3),           // deleted in round 3 → nothing
		benchValue(numDocs + 0), // added round 0 → numDocs
		benchValue(numDocs + 9), // added round 9 → numDocs+9
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
	require.Equal(t, fold, col, "columnar must match the fold across many flushes + a base fold")
	require.Equal(t, []uint64{500, uint64(numDocs), uint64(numDocs + 9)}, col)
	require.NotNil(t, bucket.ContainsAnyAccelerator())
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

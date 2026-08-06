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
	"slices"
	"sort"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// newUniqueTextSearcher builds a Searcher over a STRICTLY unique text roaringset
// property (val_%08d -> docID i, 1:1, no shared values), flushed to disk — the
// conditions under which the key/doc column is eligible to serve. Returns
// the searcher and its store.
// newUniqueTextSearcher builds a searcher whose property bucket carries the
// key/doc column. Use newUniqueTextSearcherNoIndex for the same corpus without
// one — configuration decides whether a bucket has an index, so comparing the
// two paths means comparing two buckets rather than toggling a switch.
func newUniqueTextSearcher(tb testing.TB, numDocs int) (*Searcher, *lsmkv.Store) {
	return newUniqueTextSearcherOpt(tb, numDocs, true)
}

func newUniqueTextSearcherNoIndex(tb testing.TB, numDocs int) (*Searcher, *lsmkv.Store) {
	return newUniqueTextSearcherOpt(tb, numDocs, false)
}

func newUniqueTextSearcherOpt(tb testing.TB, numDocs int, withIndex bool) (*Searcher, *lsmkv.Store) {
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

	// Phase 2: reopen — the segment group now builds its base from the
	// flushed disk segments (like a server restart), so the base carries the
	// corpus rather than it all arriving as a layer. This is the realistic
	// always-on shape.
	store := newStore()
	tb.Cleanup(func() { store.Shutdown(context.Background()) })
	reopenOpts := []lsmkv.BucketOption{
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
	}
	if withIndex {
		reopenOpts = append(reopenOpts,
			lsmkv.WithKeyDocColumn(true),
			lsmkv.WithMaxIdGetter(func() uint64 { return uint64(numDocs + 1) }))
	}
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), name, reopenOpts...))

	bitmapFactory := roaringset.NewBitmapFactory(bufPool, newFakeMaxIDGetter(uint64(numDocs+1)))
	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false },
		func(string) bool { return false }, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory).
		WithBatchedContainsEnabled(configRuntime.NewDynamicValue(true))
	return searcher, store
}

// pairedSearchers builds the same corpus twice, once with the key/doc column and
// once without, so a test can compare the two paths over identical data.
func pairedSearchers(tb testing.TB, numDocs int) (indexed, plain *Searcher,
	indexedBucket, plainBucket *lsmkv.Bucket,
) {
	tb.Helper()
	indexed, indexedStore := newUniqueTextSearcher(tb, numDocs)
	plain, plainStore := newUniqueTextSearcherNoIndex(tb, numDocs)
	name := helpers.BucketFromPropNameLSM(benchPropName)
	return indexed, plain, indexedStore.Bucket(name), plainStore.Bucket(name)
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

// BenchmarkKeyDocColumnLayers measures ContainsAny resolution cost as index
// layers are added: base (on disk) → base + a populated active memtable → base +
// several flushed layers + a populated active memtable. Each variant runs the
// SAME query (drawn from the base), so the delta is the pure overhead of
// scanning/overlaying the extra layers, not different match counts. Run:
//
//	go test -tags integrationTest -run '^$' -bench 'KeyDocColumnLayers' \
//	    -benchmem -benchtime 20x -count 3 ./adapters/repos/db/inverted/
func BenchmarkKeyDocColumnLayers(b *testing.B) {
	const (
		baseDocs   = 300_000
		layerDocs  = 10_000 // keys per flushed layer
		activeDocs = 10_000 // unflushed keys in the active memtable
		numLayers  = 3      // below flattenLayersThreshold, so they stay as layers
	)

	// populate adds `layers` flushed layers then an unflushed active memtable to
	// the searcher's bucket, using docIDs past the base so they don't collide.
	populate := func(store *lsmkv.Store, layers int) {
		bkt := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
		next := baseDocs
		for r := 0; r < layers; r++ {
			for i := 0; i < layerDocs; i++ {
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
	populate(layeredStore, numLayers) // layers + active

	variants := []struct {
		name string
		s    *Searcher
	}{
		{"base", baseOnly},
		{"base+active", withActive},
		{"base+3layers+active", layered},
	}

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

// BenchmarkKeyDocColumnWiring_DocIDs measures the full Searcher.DocIDs path for
// ContainsAny on a strictly-unique text property, comparing the standard fold
// (flag off) against the resident key/doc column (flag on). This is the
// end-to-end number: extraction + resolution + allowlist, through the real API.
func BenchmarkKeyDocColumnWiring_DocIDs(b *testing.B) {
	indexed, _ := newUniqueTextSearcher(b, benchCorpusSize)
	plain, _ := newUniqueTextSearcherNoIndex(b, benchCorpusSize)
	ctx := context.Background()

	modes := []struct {
		name     string
		searcher *Searcher
	}{{"fold", plain}, {"keydoccolumn", indexed}}
	for _, size := range []int{1_000, 10_000, 100_000} {
		values := sampleUniqueValues(benchCorpusSize, size)
		filter := containsFilter(filters.ContainsAny, values)
		for _, m := range modes {
			b.Run(fmt.Sprintf("%s/N=%d", m.name, size), func(b *testing.B) {
				// warm up so any one-time cost is excluded from the measurement
				al, err := m.searcher.DocIDs(ctx, filter, additional.Properties{}, className)
				require.NoError(b, err)
				al.Close()

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					al, err := m.searcher.DocIDs(ctx, filter, additional.Properties{}, className)
					if err != nil {
						b.Fatal(err)
					}
					al.Close()
				}
			})
		}
	}
}

// TestKeyDocColumnWiring_DocIDsMatchesFold drives the real Searcher.DocIDs path and
// pins that ContainsAny resolved via the key/doc column (flag on) returns
// exactly the same doc IDs as the standard fold (flag off), and that the
// index actually served (was cached on the bucket), not silently fell
// back.
func TestKeyDocColumnWiring_DocIDsMatchesFold(t *testing.T) {
	const numDocs = 20_000
	indexed, store := newUniqueTextSearcher(t, numDocs)
	plain, _ := newUniqueTextSearcherNoIndex(t, numDocs)
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

	run := func(s *Searcher, values []string) []uint64 {
		al, err := s.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
			additional.Properties{}, className)
		require.NoError(t, err)
		defer al.Close()
		return sortedDocIDs(al)
	}

	requireMatchesFold := func(t *testing.T, values []string) {
		t.Helper()
		require.Equal(t, run(plain, values), run(indexed, values),
			"the key/doc column must answer exactly as the standard fold does")
	}

	// The filter's value order is the user's, and the index requires its
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

	// prove the index actually served: it is attached at open and kept
	// live across the flush (via MergeMemtableByCursor), so it is present on the bucket.
	bkt := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	require.NotNil(t, bkt.KeyDocColumn(),
		"key/doc column must be attached and serving")
}

// TestKeyDocColumnWiring_MultiKeyDocDeletion covers a document that sits under more
// than one key losing one of them. roaringset records that as a deletion under
// that key alone, and the document must keep matching the keys it still holds.
//
// The eligibility gate keeps such properties off the index in production,
// so this exercises the resolution algorithm directly through the test factory:
// correctness here does not depend on the gate being right.
func TestKeyDocColumnWiring_MultiKeyDocDeletion(t *testing.T) {
	tests := []struct {
		name    string
		flush   bool // flush the removal, so it lands as a layer rather than in the memtable
		queried []string
	}{
		{name: "flushed removal, kept key queried", flush: true, queried: []string{"kept"}},
		{name: "flushed removal, both keys queried", flush: true, queried: []string{"dropped", "kept"}},
		{name: "unflushed removal, kept key queried", flush: false, queried: []string{"kept"}},
		{name: "unflushed removal, both keys queried", flush: false, queried: []string{"dropped", "kept"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const numDocs = 5_000
			indexed, store := newUniqueTextSearcher(t, numDocs)
			plain, plainStore := newUniqueTextSearcherNoIndex(t, numDocs)
			ctx := context.Background()
			bucket := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
			plainBucket := plainStore.Bucket(helpers.BucketFromPropNameLSM(benchPropName))

			// doc 5 gains a second value, so it sits under two keys
			for _, b := range []*lsmkv.Bucket{bucket, plainBucket} {
				require.NoError(t, b.RoaringSetAddList([]byte("extra_value"), []uint64{5}))
				require.NoError(t, b.FlushAndSwitch())

				// the object drops that second value but keeps its original one
				require.NoError(t, b.RoaringSetRemoveOne([]byte("extra_value"), 5))
				if tt.flush {
					require.NoError(t, b.FlushAndSwitch())
				}
			}

			values := make([]string, 0, len(tt.queried)+1)
			for _, q := range tt.queried {
				if q == "dropped" {
					values = append(values, "extra_value")
				} else {
					values = append(values, benchValue(5))
				}
			}
			values = append(values, benchValue(6))

			run := func(s *Searcher) []uint64 {
				al, err := s.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
					additional.Properties{}, className)
				require.NoError(t, err)
				defer al.Close()
				return sortedDocIDs(al)
			}

			fold := run(plain)
			require.Equal(t, []uint64{5, 6}, fold, "sanity: doc 5 still holds its original value")
			require.Equal(t, fold, run(indexed),
				"dropping one of a doc's keys must not remove it from the others")
		})
	}
}

// TestKeyDocColumnWiring_UnflushedNonUniqueKey covers an unflushed memtable putting
// a second document under a key the flushed layers already hold one for — a
// collision the build-time check cannot have seen. Both documents must come
// back: the memtable's addition adds to the key rather than replacing what it
// held, and the extra is carried alongside the scalar column.
func TestKeyDocColumnWiring_UnflushedNonUniqueKey(t *testing.T) {
	const numDocs = 2_000
	indexed, plain, bucket, plainBucket := pairedSearchers(t, numDocs)
	ctx := context.Background()

	// a second document takes a value another document already holds, and stays
	// unflushed — the build-time uniqueness check never sees it
	for _, b := range []*lsmkv.Bucket{bucket, plainBucket} {
		require.NoError(t, b.RoaringSetAddList([]byte(benchValue(7)), []uint64{uint64(numDocs + 1)}))
	}

	values := []string{benchValue(7), benchValue(8)}
	run := func(s *Searcher) []uint64 {
		al, err := s.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
			additional.Properties{}, className)
		require.NoError(t, err)
		defer al.Close()
		return sortedDocIDs(al)
	}

	fold := run(plain)
	require.Equal(t, []uint64{7, 8, numDocs + 1}, fold, "sanity: both documents hold value 7")
	require.Equal(t, fold, run(indexed),
		"a key holding two documents must resolve to both, not one")
}

// TestKeyDocColumnWiring_UnflushedWrites pins that the index serves correctly
// when there are unflushed writes in the active memtable — new keys and a delete
// — via the memtable overlay, matching the fold, without falling back.
func TestKeyDocColumnWiring_UnflushedWrites(t *testing.T) {
	const numDocs = 5_000
	indexed, plain, bucket, plainBucket := pairedSearchers(t, numDocs)
	ctx := context.Background()

	// unflushed writes into the active memtable (no FlushAndSwitch): two new
	// keys, and a delete of a doc that lives in the flushed run.
	for _, b := range []*lsmkv.Bucket{bucket, plainBucket} {
		require.NoError(t, b.RoaringSetAddList([]byte(benchValue(numDocs)), []uint64{uint64(numDocs)}))
		require.NoError(t, b.RoaringSetAddList([]byte(benchValue(numDocs+1)), []uint64{uint64(numDocs + 1)}))
		require.NoError(t, b.RoaringSetRemoveOne([]byte(benchValue(5)), 5))
	}

	values := []string{
		benchValue(1),           // flushed, still present → 1
		benchValue(5),           // deleted in memtable → nothing
		benchValue(numDocs),     // new, unflushed → numDocs
		benchValue(numDocs + 1), // new, unflushed → numDocs+1
	}

	run := func(s *Searcher) []uint64 {
		al, err := s.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
			additional.Properties{}, className)
		require.NoError(t, err)
		defer al.Close()
		return sortedDocIDs(al)
	}

	fold := run(plain)
	col := run(indexed)

	require.Equal(t, fold, col, "key/doc column + memtable overlay must match the fold with unflushed writes")
	require.Equal(t, []uint64{1, uint64(numDocs), uint64(numDocs + 1)}, col,
		"flushed(1) kept, deleted(5) gone, unflushed new keys present")
	require.NotNil(t, bucket.KeyDocColumn(),
		"unflushed writes must not detach the index")
}

// TestKeyDocColumnWiring_ManyFlushes drives more flushes than the fold threshold, so
// the accumulated runs are folded into the base (through real memtable cursors),
// and pins that ContainsAny still matches the fold across base + fold + recent
// runs, with adds and deletes.
func TestKeyDocColumnWiring_ManyFlushes(t *testing.T) {
	const numDocs = 1_000
	indexed, plain, bucket, plainBucket := pairedSearchers(t, numDocs) // base holds 0..999
	ctx := context.Background()

	// 10 flushes (> the fold threshold): each adds one new key and deletes one
	// existing doc, exercising the fold's add and delete paths on real cursors.
	for _, b := range []*lsmkv.Bucket{bucket, plainBucket} {
		for r := 0; r < 10; r++ {
			newKey := numDocs + r
			require.NoError(t, b.RoaringSetAddList([]byte(benchValue(newKey)), []uint64{uint64(newKey)}))
			require.NoError(t, b.RoaringSetRemoveOne([]byte(benchValue(r)), uint64(r)))
			require.NoError(t, b.FlushAndSwitch())
		}
	}

	values := []string{
		benchValue(500),         // base, never deleted → 500
		benchValue(3),           // deleted in round 3 → nothing
		benchValue(numDocs + 0), // added round 0 → numDocs
		benchValue(numDocs + 9), // added round 9 → numDocs+9
	}
	run := func(s *Searcher) []uint64 {
		al, err := s.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
			additional.Properties{}, className)
		require.NoError(t, err)
		defer al.Close()
		return sortedDocIDs(al)
	}

	fold := run(plain)
	col := run(indexed)
	require.Equal(t, fold, col, "key/doc column must match the fold across many flushes + a base fold")
	require.Equal(t, []uint64{500, uint64(numDocs), uint64(numDocs + 9)}, col)
	require.NotNil(t, bucket.KeyDocColumn())
}

// TestKeyDocColumnWiring_ServesNonUniqueValues pins that a property whose values are
// not unique — a key holding several documents — keeps being served and returns
// every one of them. The index is configured on the understanding that values
// are unique; when that turns out to be wrong the operator gets a warning, not
// missing documents.
func TestKeyDocColumnWiring_ServesNonUniqueValues(t *testing.T) {
	// The shared benchmark fixture seeds shared_a/b/c -> {11,17}.
	f := newContainsFixture(t, 5_000)
	plain := newContainsFixtureNoIndex(t, 5_000)
	ctx := context.Background()

	values := containsSharedValues // shared_a/b/c -> docs {11,17}

	run := func(s *Searcher) []uint64 {
		al, err := s.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
			additional.Properties{}, className)
		require.NoError(t, err)
		defer al.Close()
		return sortedDocIDs(al)
	}

	fold := run(plain.searcher)
	require.Equal(t, []uint64{11, 17}, fold, "sanity: both documents hold the shared values")
	require.Equal(t, fold, run(f.searcher),
		"a key holding several documents must resolve to all of them")

	bkt := f.store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	require.NotNil(t, bkt.KeyDocColumn(),
		"a handful of duplicates is warned about, not a reason to stop serving")
}

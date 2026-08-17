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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	entsInverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// newUniqueTextSearcher builds a Searcher over a text property with strictly
// unique values (val_%08d -> docID i), flushed to disk, in a bucket carrying a
// key/doc column — the conditions under which it is eligible to serve.
// newUniqueTextSearcherNoIndex is the same corpus without one, since which
// bucket carries an index is configuration rather than a switch to toggle.
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

	// Write and flush the corpus to disk, then close — so the data lives
	// in disk segments, not a memtable.
	{
		store := newStore()
		require.NoError(tb, store.CreateOrLoadBucket(context.Background(), name,
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
			lsmkv.WithBitmapBufPool(bufPool),
		))
		bucket := store.Bucket(name)
		writeUniqueTextCorpus(tb, bucket, numDocs)
		require.NoError(tb, bucket.FlushAndSwitch())
		require.NoError(tb, store.Shutdown(context.Background()))
	}

	// Reopen: the segment group now builds its base from the
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
			lsmkv.WithDocIDCountGetter(newFakeMaxIDGetter(uint64(numDocs+1))))
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

// containsDocIDs resolves a Contains filter through s and returns the documents
// it allows, in order. Every wiring test below is one searcher's answer against
// another's over the same values, so this is the whole of what they do.
func containsDocIDs(t *testing.T, ctx context.Context, s *Searcher,
	op filters.Operator, values []string,
) []uint64 {
	t.Helper()
	al, err := s.DocIDs(ctx, containsFilter(op, values), additional.Properties{}, className)
	require.NoError(t, err)
	defer al.Close()
	return sortedDocIDs(al)
}

// servedBy reports which read answered a Contains filter, taken from the
// annotation the slow-query log carries.
func servedBy(t *testing.T, s *Searcher, op filters.Operator, values []string) string {
	t.Helper()
	ctx := helpers.InitSlowQueryDetails(context.Background())
	containsDocIDs(t, ctx, s, op, values)
	served, _ := docBitmapEntry(t, ctx, "the batched read must annotate")["served_by"].(string)
	return served
}

// docBitmapEntry returns the single doc-bitmap annotation in ctx.
func docBitmapEntry(t *testing.T, ctx context.Context, msg string) map[string]any {
	t.Helper()
	summary, ok := helpers.ExtractSlowQueryDetails(ctx)[docBitmapSlowLogKey].(helpers.SlowestEntries)
	require.True(t, ok, msg)
	require.Equal(t, 1, summary.Count)
	require.Len(t, summary.Slowest, 1)
	return summary.Slowest[0]
}

// requireColumnAnswers fails unless the key/doc column is what answered. Every
// test comparing a column searcher against a fold searcher assumes this, and an
// attached column is not the same fact: a filter the dispatch declines is
// answered by the fold over the same data, so the two agree and the comparison
// passes having compared the fold with itself.
func requireColumnAnswers(t *testing.T, s *Searcher, op filters.Operator, values []string) {
	t.Helper()
	require.Equal(t, "key_doc_column", servedBy(t, s, op, values),
		"the column must be what answers, not merely be attached")
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
// a varying number of flushed layers + a populated active memtable. Each variant
// runs the SAME query (drawn from the base), so the delta is the pure overhead of
// scanning/overlaying the extra layers, not different match counts.
//
// This is the sweep behind flattenLayersThreshold, which trades what a query
// pays per layer against how often a whole base is rebuilt. It sweeps 1 to 7
// because 8 is the threshold: at that point the layers are folded in, so there
// is no steady state deeper than this to measure. What a deeper list costs is
// the deferred-rebuild case, which has tests rather than a benchmark — nothing
// reachable from here keeps a rebuild from running.
//
// Run:
//
//	go test -tags integrationTest -run '^$' -bench 'KeyDocColumnLayers' \
//	    -benchmem -benchtime 20x -count 3 ./adapters/repos/db/inverted/
func BenchmarkKeyDocColumnLayers(b *testing.B) {
	const (
		baseDocs   = 300_000
		layerDocs  = 10_000 // keys per flushed layer
		activeDocs = 10_000 // unflushed keys in the active memtable
	)
	// keydoccolumn's flattenLayersThreshold is unexported, so this top of range
	// is a copy of it rather than a reference; raise it with that constant.
	layerCounts := []int{1, 3, 7}

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

	variants := []struct {
		name string
		s    *Searcher
	}{
		{"base", baseOnly},
		{"base+active", withActive},
	}
	for _, layers := range layerCounts {
		layered, layeredStore := newUniqueTextSearcher(b, baseDocs)
		populate(layeredStore, layers) // layers + active
		variants = append(variants, struct {
			name string
			s    *Searcher
		}{fmt.Sprintf("base+%dlayers+active", layers), layered})
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
// against the same corpus in a bucket carrying a key/doc column. This is the
// end-to-end number: extraction + resolution + allowlist, through the real API.
//
// The values are shuffled before each filter is built. sampleUniqueValues walks
// the corpus in order, and keys arriving already sorted flatter both paths — the
// sort short-circuits and the row reads are sequential — which no real filter
// does.
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
		// one fixed seed, outside the timed loop: every row sees the same
		// shuffle, and shuffling is not what is being measured
		shuffle := rand.New(rand.NewSource(int64(size)))
		shuffle.Shuffle(len(values), func(i, j int) { values[i], values[j] = values[j], values[i] })
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
// pins that ContainsAny resolved through the key/doc column returns exactly the
// doc IDs the standard fold does over the same corpus, and that the index
// actually served — it is attached to the bucket — rather than silently falling
// back.
func TestKeyDocColumnWiring_DocIDsMatchesFold(t *testing.T) {
	const numDocs = 20_000
	indexed, store := newUniqueTextSearcher(t, numDocs)
	plain, _ := newUniqueTextSearcherNoIndex(t, numDocs)
	ctx := context.Background()

	sample := func(size int) []string { return sampleUniqueValues(numDocs, size) }

	run := func(s *Searcher, op filters.Operator, values []string) []uint64 {
		return containsDocIDs(t, ctx, s, op, values)
	}

	// ContainsNone is the same union denied, and the index computes the union
	// either way — so it has to agree with the fold on both, and the two answers
	// have to remain complements of each other. A deny flag left unset would
	// still match the fold's *union*, so only comparing against ContainsAny
	// would miss it.
	requireMatchesFold := func(t *testing.T, values []string) {
		t.Helper()
		any := run(indexed, filters.ContainsAny, values)
		none := run(indexed, filters.ContainsNone, values)
		require.Equal(t, run(plain, filters.ContainsAny, values), any,
			"the key/doc column must answer ContainsAny exactly as the standard fold does")
		require.Equal(t, run(plain, filters.ContainsNone, values), none,
			"the key/doc column must answer ContainsNone exactly as the standard fold does")
		require.Empty(t, intersect(any, none),
			"a document cannot both hold one of the values and hold none of them")
	}

	// the comparisons below say nothing unless the column is what answers
	requireColumnAnswers(t, indexed, filters.ContainsAny, sample(100))
	requireColumnAnswers(t, indexed, filters.ContainsNone, sample(100))

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

// TestKeyDocColumnWiring_ServesContainsNone pins that the index itself answers
// ContainsNone, which no black-box comparison can show: the fold returns the
// same documents, so a query that quietly fell back to it looks identical. It
// asks the index directly, and checks the deny flag is what the operator
// changes — the documents are the same union either way.
func TestKeyDocColumnWiring_ServesContainsNone(t *testing.T) {
	const numDocs = 2_000
	indexed, store := newUniqueTextSearcher(t, numDocs)

	bkt := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	require.NotNil(t, bkt.KeyDocColumn())

	values := []string{benchValue(3), benchValue(11), benchValue(1_500)}
	pv := &propValuePair{
		prop:         benchPropName,
		containsKeys: keyDocColumnSortedKeys(t, values),
	}

	view := bkt.GetConsistentRoaringSetView()
	defer view.ReleaseView()
	reader, err := view.WithoutEmptyActiveMemtable().NewRoaringSetKeyDocColumnReader(pv.containsKeys)
	require.NoError(t, err)

	allowed, ok, err := indexed.docBitmapContainsColumn(context.Background(), reader, false)
	require.NoError(t, err)
	require.True(t, ok, "the index must serve the query")
	defer allowed.release()

	denied, ok, err := indexed.docBitmapContainsColumn(context.Background(), reader, true)
	require.NoError(t, err)
	require.True(t, ok, "the index must serve the query when it is denied too")
	defer denied.release()

	require.False(t, allowed.IsDenyList())
	require.True(t, denied.IsDenyList(), "ContainsNone must come back denied")
	require.Equal(t, []uint64{3, 11, 1_500}, allowed.docIDs.ToArray())
	require.Equal(t, allowed.docIDs.ToArray(), denied.docIDs.ToArray(),
		"the operator changes the flag, not the documents")
}

// keyDocColumnSortedKeys is keysFrom over values a test states as strings.
// Sorting them first would be redundant: Build orders and dedups whatever it is
// given, which is what makes the query keys' order the user's business.
func keyDocColumnSortedKeys(tb testing.TB, values []string) entsInverted.SortedKeys {
	tb.Helper()
	keys := make([][]byte, 0, len(values))
	for _, v := range values {
		keys = append(keys, []byte(v))
	}
	return keysFrom(tb, keys...)
}

// intersect is the documents two results agree on, for assertions about results
// that must not overlap.
func intersect(a, b []uint64) []uint64 {
	inA := make(map[uint64]struct{}, len(a))
	for _, id := range a {
		inA[id] = struct{}{}
	}
	var both []uint64
	for _, id := range b {
		if _, ok := inA[id]; ok {
			both = append(both, id)
		}
	}
	return both
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
				return containsDocIDs(t, ctx, s, filters.ContainsAny, values)
			}

			requireColumnAnswers(t, indexed, filters.ContainsAny, values)

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
		return containsDocIDs(t, ctx, s, filters.ContainsAny, values)
	}

	requireColumnAnswers(t, indexed, filters.ContainsAny, values)

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
		return containsDocIDs(t, ctx, s, filters.ContainsAny, values)
	}

	fold := run(plain)
	col := run(indexed)

	require.Equal(t, fold, col, "key/doc column + memtable overlay must match the fold with unflushed writes")
	require.Equal(t, []uint64{1, uint64(numDocs), uint64(numDocs + 1)}, col,
		"flushed(1) kept, deleted(5) gone, unflushed new keys present")
	require.NotNil(t, bucket.KeyDocColumn(),
		"unflushed writes must not detach the index")
	requireColumnAnswers(t, indexed, filters.ContainsAny, values)
}

// TestKeyDocColumnWiring_UnflushedWritesAcrossWindows pins the unflushed layers
// being applied to the keys they belong to when the batch spans more than one
// window.
//
// The memtables are read a window at a time, each dense over its own range, so
// every row's query position is its offset plus where the window started.
// Getting that sum wrong applies a key's documents to a different key — the
// right number of documents, attributed to the wrong values, and no error
// anywhere. Nothing else here reaches past the first window: the tests with
// unflushed writes carry a handful of keys, and the ones with large batches are
// fully flushed, so the memtables have nothing to say.
func TestKeyDocColumnWiring_UnflushedWritesAcrossWindows(t *testing.T) {
	const (
		numDocs = 5_000
		// several windows wide, whatever the production window is
		batch = 3_000
	)
	indexed, plain, bucket, plainBucket := pairedSearchers(t, numDocs)
	ctx := context.Background()

	// One change per window, at a different offset within each, so an
	// off-by-window and an off-by-one land on different keys.
	changes := []struct {
		key   int
		add   uint64
		strip bool
	}{
		{key: 10, add: numDocs + 1},        // early in the first window
		{key: 1_500, strip: true},          // second window: its only document goes
		{key: 2_500, add: numDocs + 2},     // third window
		{key: batch - 1, add: numDocs + 3}, // the last key of the batch
	}
	for _, b := range []*lsmkv.Bucket{bucket, plainBucket} {
		for _, c := range changes {
			if c.strip {
				require.NoError(t, b.RoaringSetRemoveOne([]byte(benchValue(c.key)), uint64(c.key)))
				continue
			}
			require.NoError(t, b.RoaringSetAddOne([]byte(benchValue(c.key)), c.add))
		}
	}

	values := make([]string, 0, batch)
	for i := 0; i < batch; i++ {
		values = append(values, benchValue(i))
	}

	fold := containsDocIDs(t, ctx, plain, filters.ContainsAny, values)
	col := containsDocIDs(t, ctx, indexed, filters.ContainsAny, values)
	require.Equal(t, fold, col,
		"the column must answer a multi-window batch exactly as the fold does")

	// and the changes landed on the keys they were issued under
	assert.Contains(t, col, uint64(numDocs+1))
	assert.Contains(t, col, uint64(numDocs+2))
	assert.Contains(t, col, uint64(numDocs+3))
	assert.NotContains(t, col, uint64(1_500), "the document its key lost")
	require.NotNil(t, bucket.KeyDocColumn(), "unflushed writes must not detach the index")
	requireColumnAnswers(t, indexed, filters.ContainsAny, values)

	// each changed key on its own, so a misattribution that happens to keep the
	// union right still fails
	for _, c := range changes {
		got := containsDocIDs(t, ctx, indexed, filters.ContainsAny, []string{benchValue(c.key)})
		if c.strip {
			assert.Empty(t, got, "key %d alone: its only document was removed", c.key)
			continue
		}
		assert.Equal(t, []uint64{uint64(c.key), c.add}, got, "key %d alone", c.key)
	}
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
		return containsDocIDs(t, ctx, s, filters.ContainsAny, values)
	}

	fold := run(plain)
	col := run(indexed)
	require.Equal(t, fold, col, "key/doc column must match the fold across many flushes + a base fold")
	require.Equal(t, []uint64{500, uint64(numDocs), uint64(numDocs + 9)}, col)
	requireColumnAnswers(t, indexed, filters.ContainsAny, values)
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
		return containsDocIDs(t, ctx, s, filters.ContainsAny, values)
	}

	fold := run(plain.searcher)
	require.Equal(t, []uint64{11, 17}, fold, "sanity: both documents hold the shared values")
	require.Equal(t, fold, run(f.searcher),
		"a key holding several documents must resolve to all of them")

	bkt := f.store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	require.NotNil(t, bkt.KeyDocColumn(),
		"a handful of duplicates is warned about, not a reason to stop serving")
	requireColumnAnswers(t, f.searcher, filters.ContainsAny, values)
}

// TestKeyDocColumnWiring_AnnotatesWhichPathAnswered pins the slow-query log
// saying which read answered a batched Contains, and what that read spent.
//
// Both paths can answer the same filter, and their costs are not comparable, so
// the memtable counts say little without the name beside them. Without this an
// operator cannot tell whether the index they configured served the query they
// are looking at — the one question this feature keeps raising.
func TestKeyDocColumnWiring_AnnotatesWhichPathAnswered(t *testing.T) {
	const numDocs = 2_000
	indexed, plain, bucket, _ := pairedSearchers(t, numDocs)

	// unflushed, so the answering path has memtable work to report
	require.NoError(t, bucket.RoaringSetAddOne([]byte(benchValue(7)), numDocs+1))

	values := []string{benchValue(3), benchValue(7), benchValue(11)}

	annotationOf := func(t *testing.T, s *Searcher) map[string]any {
		t.Helper()
		ctx := helpers.InitSlowQueryDetails(context.Background())
		require.NotEmpty(t, containsDocIDs(t, ctx, s, filters.ContainsAny, values))
		return docBitmapEntry(t, ctx, "the batched read must annotate")
	}

	t.Run("the column answers, and says so", func(t *testing.T) {
		fields := annotationOf(t, indexed)
		assert.Equal(t, "key_doc_column", fields["served_by"])
		assert.Positive(t, fields["window_fills"], "it walked the unflushed layers")
		assert.Equal(t, 1, fields["memtables"], "one memtable contributed")
	})

	t.Run("the fold answers when there is no column, and says so", func(t *testing.T) {
		fields := annotationOf(t, plain)
		assert.Equal(t, "fold", fields["served_by"])
	})
}

// TestKeyDocColumnWiring_CancelledQuery pins the column path handing a
// cancelled query back rather than serving it. Both folds check cancellation per
// key; this path checked nothing.
//
// A hundred thousand keys resolve fast enough that this rarely costs anything,
// but "fast enough" is not a cancellation policy: a client that has gone away
// should not have its work finished on one path and abandoned on the other.
//
// Driven against the resolve directly. Through DocIDs a cancelled context is
// refused higher up, so that route would pass whether this path checked or not.
func TestKeyDocColumnWiring_CancelledQuery(t *testing.T) {
	const numDocs = 2_000
	indexed, store := newUniqueTextSearcher(t, numDocs)

	bkt := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	pv := &propValuePair{
		prop:         benchPropName,
		containsKeys: keyDocColumnSortedKeys(t, []string{benchValue(3), benchValue(11)}),
	}

	view := bkt.GetConsistentRoaringSetView()
	defer view.ReleaseView()
	reader, err := view.WithoutEmptyActiveMemtable().NewRoaringSetKeyDocColumnReader(pv.containsKeys)
	require.NoError(t, err)

	served, ok, err := indexed.docBitmapContainsColumn(context.Background(), reader, false)
	require.NoError(t, err)
	require.True(t, ok, "the query is one the column serves")
	served.release()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_, ok, err = indexed.docBitmapContainsColumn(cancelled, reader, false)
	assert.ErrorIs(t, err, context.Canceled, "a cancelled query is over, not handed on")
	assert.False(t, ok)
}

// TestKeyDocColumnWiring_FollowsTheBatchedContainsGate pins the column having no
// way past the batched-contains gate, and needing none.
//
// The column is served from the batched path and nowhere else, so it could have
// been given a bypass. It is not: the gate covers everything the batched path
// serves, which keeps it a whole kill-switch over the newest code rather than
// one with a hole in it exactly where that code runs.
//
// What makes that affordable is the two halves below. An unconfigured node
// leaves the gate unwired, which reads as on, so configuring a column is enough
// to be served by one — nobody has to know about a second variable. And a gate
// turned off does not detach anything: the column goes on being built and kept
// current, so flipping the gate back serves from it immediately rather than
// after a restart.
func TestKeyDocColumnWiring_FollowsTheBatchedContainsGate(t *testing.T) {
	const numDocs = 2_000
	indexed, store := newUniqueTextSearcher(t, numDocs)

	values := sampleUniqueValues(numDocs, 8)
	class := createSchema().GetClass(className)

	// Every operator the column answers, plus the one it does not: a bypass
	// reintroduced for either group would show up here.
	operators := []filters.Operator{
		filters.ContainsAny, filters.ContainsNone, filters.ContainsAll,
	}

	batches := func(t *testing.T, operator filters.Operator) bool {
		t.Helper()
		ctx := helpers.InitSlowQueryDetails(context.Background())
		pv, err := indexed.extractContains(ctx, containsPath(benchPropName),
			containsValue(schema.DataTypeText, values), operator, class)
		require.NoError(t, err)
		if pv.containsKeys.Len() > 0 {
			require.Empty(t, pv.children, "a batched filter must not also desugar")
			return true
		}
		require.NotEmpty(t, pv.children, "a filter that did not batch must desugar")
		require.Equal(t, containsDeclineNotEnabled, extractContainsDesugaredReason(t, ctx))
		return false
	}

	bkt := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	require.NotNil(t, bkt.KeyDocColumn(), "the property under test carries a column")

	t.Run("an unwired gate batches, so a configured column is read", func(t *testing.T) {
		indexed.batchedContainsEnabled = nil // as an unconfigured node leaves it
		for _, operator := range operators {
			assert.True(t, batches(t, operator), "%s", operator.Name())
		}
	})

	gate := configRuntime.NewDynamicValue(false)
	indexed.batchedContainsEnabled = gate

	t.Run("a gate turned off holds the column back too", func(t *testing.T) {
		for _, operator := range operators {
			assert.False(t, batches(t, operator), "%s", operator.Name())
		}
		require.NotNil(t, bkt.KeyDocColumn(),
			"the column is held back from queries, not detached from the bucket")
	})

	t.Run("turning it back on serves from the same column", func(t *testing.T) {
		require.NoError(t, gate.SetValue(true))
		for _, operator := range operators {
			assert.True(t, batches(t, operator), "%s", operator.Name())
		}
		require.NotNil(t, bkt.KeyDocColumn(),
			"the same column answers again, with no rebuild in between")
	})
}

// TestKeyDocColumnWiring_CancelledQueryNamesTheColumn pins the annotation
// naming the read that was running when a query gave up.
//
// The column's reader records every window it walked, including on a read that
// then fails, and the annotation falls back to those counts when no fold ran.
// Named afterwards, a cancelled column read reported its own window counters
// under "none" — a path that supposedly did nothing, with a bill attached.
func TestKeyDocColumnWiring_CancelledQueryNamesTheColumn(t *testing.T) {
	const numDocs = 2_000
	indexed, store := newUniqueTextSearcher(t, numDocs)

	bkt := store.Bucket(helpers.BucketFromPropNameLSM(benchPropName))
	require.NotNil(t, bkt.KeyDocColumn())

	pv := &propValuePair{
		prop:               benchPropName,
		operator:           filters.ContainsAny,
		hasFilterableIndex: true,
		containsKeys:       keyDocColumnSortedKeys(t, []string{benchValue(3), benchValue(11)}),
	}

	cancelled, cancel := context.WithCancel(helpers.InitSlowQueryDetails(context.Background()))
	cancel()

	_, err := pv.fetchContainsBatch(cancelled, indexed)
	require.ErrorIs(t, err, context.Canceled)

	entry := docBitmapEntry(t, cancelled, "a filter that failed partway is still timed")
	assert.Equal(t, "key_doc_column", entry["served_by"],
		"the column was the read that ran, failed or not")
	assert.Equal(t, true, entry["failed"])
}

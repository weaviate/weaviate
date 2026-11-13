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
	"os/exec"
	"sort"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	entsInverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// newUniqueTextSearcher builds a Searcher over flushed values val_%08d -> docID i, in a bucket with a key/doc column.
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

	// reopen, so the column's base is built from the disk segments as on a restart
	store := newStore()
	tb.Cleanup(func() { store.Shutdown(context.Background()) })
	reopenOpts := []lsmkv.BucketOption{
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
	}
	if withIndex {
		reopenOpts = append(reopenOpts,
			lsmkv.WithKeyDocColumn(tb.Name(), newFakeMaxIDGetter(uint64(numDocs+1))))
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

// pairedSearchers builds the same corpus with and without a key/doc column.
func pairedSearchers(tb testing.TB, numDocs int) (indexed, plain *Searcher,
	indexedBucket, plainBucket *lsmkv.Bucket,
) {
	tb.Helper()
	indexed, indexedStore := newUniqueTextSearcher(tb, numDocs)
	plain, plainStore := newUniqueTextSearcherNoIndex(tb, numDocs)
	name := helpers.BucketFromPropNameLSM(benchPropName)
	return indexed, plain, indexedStore.Bucket(name), plainStore.Bucket(name)
}

// containsDocIDs resolves a Contains filter through s and returns the allowed documents, sorted.
func containsDocIDs(t *testing.T, ctx context.Context, s *Searcher,
	op filters.Operator, values []string,
) []uint64 {
	t.Helper()
	al, err := s.DocIDs(ctx, containsFilter(op, values), additional.Properties{}, className)
	require.NoError(t, err)
	defer al.Close()
	return sortedDocIDs(al)
}

// servedBy returns the served_by field of the slow-query annotation for a Contains filter.
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

// requireColumnAnswers fails unless the slow-query annotation names the key/doc column as what answered.
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

// BenchmarkKeyDocColumnLayers measures ContainsAny as flushed layers stack on the
// base, one query drawn from the base; it sweeps flattenLayersThreshold's range.
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
	// 7 is one below flattenLayersThreshold (8, unexported), the deepest count left unflattened
	layerCounts := []int{1, 3, 7}

	// populate adds layers flushed layers, then an unflushed memtable, with docIDs past the base
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

// BenchmarkKeyDocColumnWiring_DocIDs measures Searcher.DocIDs for ContainsAny over shuffled values, fold against column.
func BenchmarkKeyDocColumnWiring_DocIDs(b *testing.B) {
	indexed, _ := newUniqueTextSearcher(b, benchCorpusSize)
	plain, _ := newUniqueTextSearcherNoIndex(b, benchCorpusSize)
	ctx := context.Background()

	modes := []struct {
		name     string
		searcher *Searcher
	}{{"fold", plain}, {"keydoccolumn", indexed}}
	for _, size := range []int{1_000, 3_000, 10_000, 20_000, 30_000, 60_000, 100_000} {
		values := sampleUniqueValues(benchCorpusSize, size)
		// sorted input would let both paths skip the sort; one fixed seed per size, outside the timed loop
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

// TestKeyDocColumnWiring_DocIDsMatchesFold pins ContainsAny and ContainsNone through the column, and ContainsAll past it, matching the fold.
func TestKeyDocColumnWiring_DocIDsMatchesFold(t *testing.T) {
	const numDocs = 20_000
	indexed, plain, bkt, _ := pairedSearchers(t, numDocs)
	ctx := context.Background()

	sample := func(size int) []string { return sampleUniqueValues(numDocs, size) }

	run := func(s *Searcher, op filters.Operator, values []string) []uint64 {
		return containsDocIDs(t, ctx, s, op, values)
	}

	// the comparisons below say nothing unless the column is what answers
	requireColumnAnswers(t, indexed, filters.ContainsAny, sample(100))
	requireColumnAnswers(t, indexed, filters.ContainsNone, sample(100))
	require.Equal(t, "fold", servedBy(t, indexed, filters.ContainsAll, sample(100)),
		"ContainsAll must stay on the fold: the column answers a union")

	// 100 takes scanInto's per-key search, 10 000 its merge scan
	for _, size := range []int{100, 10_000} {
		t.Run(fmt.Sprintf("N=%d", size), func(t *testing.T) {
			values := sample(size)
			any := run(indexed, filters.ContainsAny, values)
			// ContainsNone is compared too: a dropped deny flag still matches the fold's ContainsAny union
			none := run(indexed, filters.ContainsNone, values)
			require.Equal(t, run(plain, filters.ContainsAny, values), any,
				"the key/doc column must answer ContainsAny exactly as the standard fold does")
			require.Equal(t, run(plain, filters.ContainsNone, values), none,
				"the key/doc column must answer ContainsNone exactly as the standard fold does")
			require.Empty(t, intersect(any, none),
				"a document cannot both hold one of the values and hold none of them")
			require.Equal(t, run(plain, filters.ContainsAll, values), run(indexed, filters.ContainsAll, values),
				"ContainsAll is not the column's to answer, so it must still match the fold")
		})
	}

	require.NotNil(t, bkt.KeyDocColumn(),
		"key/doc column must be attached and serving")
}

// keyDocColumnSortedKeys is keysFrom over string values, which Build sorts and dedups.
func keyDocColumnSortedKeys(tb testing.TB, values []string) entsInverted.SortedKeys {
	tb.Helper()
	keys := make([][]byte, 0, len(values))
	for _, v := range values {
		keys = append(keys, []byte(v))
	}
	return keysFrom(tb, keys...)
}

// intersect returns the documents in both a and b.
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

// TestKeyDocColumnWiring_MultiKeyDocDeletion pins a document that loses one of its keys still matching the others.
func TestKeyDocColumnWiring_MultiKeyDocDeletion(t *testing.T) {
	tests := []struct {
		name  string
		flush bool // flush the removal, so it lands as a layer rather than in the memtable
	}{
		{name: "flushed removal", flush: true},
		{name: "unflushed removal", flush: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const numDocs = 5_000
			indexed, plain, bucket, plainBucket := pairedSearchers(t, numDocs)
			ctx := context.Background()

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

			values := []string{"extra_value", benchValue(5), benchValue(6)}

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

// TestKeyDocColumnWiring_UnflushedWritesAcrossWindows pins unflushed adds, new keys and deletions landing on their own keys in every window, as the fold applies them.
func TestKeyDocColumnWiring_UnflushedWritesAcrossWindows(t *testing.T) {
	const (
		numDocs = 5_000
		// three windows at BatchReaderWindowKeys (1024)
		batch = 3_000
	)
	indexed, plain, bucket, plainBucket := pairedSearchers(t, numDocs)
	ctx := context.Background()

	// changes in every window, each at a different offset, so off-by-window and off-by-one differ
	changes := []struct {
		key   int
		add   uint64
		strip bool
		want  []uint64 // the key queried alone
	}{
		// a second document under a flushed key, unseen by the build-time uniqueness check
		{key: 10, add: numDocs + 1, want: []uint64{10, numDocs + 1}},
		{key: 1_500, strip: true}, // second window: its only document goes
		{key: 2_500, add: numDocs + 2, want: []uint64{2_500, numDocs + 2}},
		{key: batch - 1, add: numDocs + 3, want: []uint64{batch - 1, numDocs + 3}},
		{key: numDocs, add: numDocs, want: []uint64{numDocs}}, // a key no flushed layer holds, last in the batch
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

	values := make([]string, 0, batch+1)
	for i := 0; i < batch; i++ {
		values = append(values, benchValue(i))
	}
	values = append(values, benchValue(numDocs))

	fold := containsDocIDs(t, ctx, plain, filters.ContainsAny, values)
	col := containsDocIDs(t, ctx, indexed, filters.ContainsAny, values)
	require.Equal(t, fold, col,
		"the column must answer a multi-window batch exactly as the fold does")

	// and the changes landed on the keys they were issued under
	assert.Contains(t, col, uint64(numDocs+1))
	assert.Contains(t, col, uint64(numDocs+2))
	assert.Contains(t, col, uint64(numDocs+3))
	assert.Contains(t, col, uint64(numDocs))
	assert.NotContains(t, col, uint64(1_500), "the document its key lost")
	require.NotNil(t, bucket.KeyDocColumn(), "unflushed writes must not detach the index")
	requireColumnAnswers(t, indexed, filters.ContainsAny, values)

	none := containsDocIDs(t, ctx, indexed, filters.ContainsNone, values)
	require.Equal(t, containsDocIDs(t, ctx, plain, filters.ContainsNone, values), none,
		"ContainsNone over unflushed writes must match the fold")
	assert.Contains(t, none, uint64(1_500), "the document its key lost matches none of the values")
	requireColumnAnswers(t, indexed, filters.ContainsNone, values)

	// each changed key alone, so a misattribution that keeps the union right still fails
	for _, c := range changes {
		got := containsDocIDs(t, ctx, indexed, filters.ContainsAny, []string{benchValue(c.key)})
		if c.strip {
			assert.Empty(t, got, "key %d alone: its only document was removed", c.key)
			continue
		}
		assert.Equal(t, c.want, got, "key %d alone", c.key)
	}
}

// TestKeyDocColumnWiring_ManyFlushes pins ContainsAny matching the fold after more flushes than flattenLayersThreshold, flattened or not.
func TestKeyDocColumnWiring_ManyFlushes(t *testing.T) {
	const numDocs = 1_000
	indexed, plain, bucket, plainBucket := pairedSearchers(t, numDocs) // base holds 0..999
	ctx := context.Background()

	// 10 flushes, past flattenLayersThreshold (8); each adds one key and deletes one document
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

// TestKeyDocColumnWiring_ServesNonUniqueValues pins a key holding several documents served by the column, all of them.
func TestKeyDocColumnWiring_ServesNonUniqueValues(t *testing.T) {
	f := newContainsFixtureWithColumn(t, 5_000)
	plain := newContainsFixture(t, 5_000)
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

// TestKeyDocColumnWiring_AnnotatesWhichPathAnswered pins served_by and the memtable counts in the slow-query annotation.
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

// TestKeyDocColumnWiring_CancelledQueryNamesTheColumn pins a cancelled column read annotated as key_doc_column, not none.
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

// A column read resolves on a budget of one, restored before a ContainsNone result
// is inverted and when the read fails, under DISABLE_SROAR_MERGE_BUDGET too.
func TestKeyDocColumnWiring_ResolvesOnShrunkBudget(t *testing.T) {
	const grant = 10
	indexed, _ := newUniqueTextSearcher(t, 2_000)
	values := []string{benchValue(3), benchValue(11)}
	wantCalls := [][2]int{{grant, 1}, {1, grant}}
	wantAnnotation := map[string]int{"held": grant, "shrunk": 1, "restored": grant}

	budgetCtx := func(parent context.Context) (*budgetProbeCtx, *recordingPool) {
		pool := &recordingPool{}
		ctx := &budgetProbeCtx{Context: concurrency.CtxWithResizableBudget(helpers.InitSlowQueryDetails(parent),
			concurrency.NewBudget(pool, grant))}
		pool.ctx = ctx
		return ctx, pool
	}
	requireRestored := func(t *testing.T, ctx *budgetProbeCtx, pool *recordingPool) {
		t.Helper()
		require.Equal(t, wantCalls, pool.calls)
		require.Equal(t, []string{"during resolve", "during resolve"}, pool.stages)
		require.Contains(t, ctx.seen, 1, "Resolve checks ctx between windows, so it must see the shrunk budget")
		require.Equal(t, grant, concurrency.BudgetFromCtx(ctx, -1))
		annotation, _ := helpers.ExtractSlowQueryDetails(ctx)["build_allow_list_budget"].(map[string]int)
		require.Equal(t, wantAnnotation, annotation)
		require.Equal(t, "key_doc_column", docBitmapEntry(t, ctx, "the column read must annotate")["served_by"])
	}

	for _, op := range []filters.Operator{filters.ContainsAny, filters.ContainsNone} {
		t.Run(op.Name(), func(t *testing.T) {
			ctx, pool := budgetCtx(context.Background())
			containsDocIDs(t, ctx, indexed, op, values)
			requireRestored(t, ctx, pool)
		})
	}

	t.Run("a failed resolve", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		ctx, pool := budgetCtx(cancelled)
		pv := &propValuePair{
			prop:               benchPropName,
			operator:           filters.ContainsAny,
			hasFilterableIndex: true,
			containsKeys:       keyDocColumnSortedKeys(t, values),
		}
		_, err := pv.fetchContainsBatch(ctx, indexed)
		require.ErrorIs(t, err, context.Canceled)
		requireRestored(t, ctx, pool)
	})

	// The kill switch is read at init, so this run repeats in a child process with it set.
	t.Run("DISABLE_SROAR_MERGE_BUDGET", func(t *testing.T) {
		if concurrency.BudgetCapDisabled() {
			return // this process already runs with it set
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestKeyDocColumnWiring_ResolvesOnShrunkBudget$",
			"-test.count=1", "-test.v", "-test.timeout=120s")
		cmd.Env = append(os.Environ(), "DISABLE_SROAR_MERGE_BUDGET=true")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		require.Contains(t, string(out), "--- PASS: TestKeyDocColumnWiring_ResolvesOnShrunkBudget")
	})
}

// budgetProbeCtx records the budget held at each Err call; Resolve calls Err between windows.
type budgetProbeCtx struct {
	context.Context
	mu   sync.Mutex
	seen []int
}

func (c *budgetProbeCtx) Err() error {
	c.mu.Lock()
	c.seen = append(c.seen, concurrency.BudgetFromCtx(c.Context, -1))
	c.mu.Unlock()
	return c.Context.Err()
}

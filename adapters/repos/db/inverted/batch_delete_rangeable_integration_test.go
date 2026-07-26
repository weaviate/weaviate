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
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

const rangeableInMemoProp = "inverted-roaringsetrange-in-memory"

// Delete resolves victims through Shard.FindUUIDs -> DocIDsLimited, not
// buildAllowList, reaching the memoised leaf on a path search-side tests don't
// cover. A stale leaf here is a wrong DELETE, not just a wrong search result.
func Test_BatchDelete_RangeableInMemory_HonoursSegmentMutations(t *testing.T) {
	// Which mutation exposes a stale leaf depends on the operator; get it wrong
	// and the test passes against a broken cache.
	//
	//   <= v resolves as plane0 \ leaf(>= v+1): deleting rows cannot expose a
	//   stale leaf, since they leave plane0 too. Only a row added *above* v
	//   does, because a stale leaf omits it and the subtraction hands it back.
	//
	//   >= v and == v return the leaf itself, so deleting a row inside the
	//   answer is enough.
	tests := []struct {
		name       string
		operator   filters.Operator
		value      int64
		wantBefore []uint64
		mutate     func(t *testing.T, bucket *lsmkv.Bucket)
		wantAfter  []uint64
	}{
		{
			name:       "less than or equal, the TTL sweep's shape",
			operator:   filters.OperatorLessThanEqual,
			value:      10,
			wantBefore: seq(1, 10),
			mutate: func(t *testing.T, bucket *lsmkv.Bucket) {
				require.NoError(t, addRangeable(bucket, 15, 30))
			},
			wantAfter: seq(1, 10),
		},
		{
			name:       "greater than or equal",
			operator:   filters.OperatorGreaterThanEqual,
			value:      10,
			wantBefore: seq(10, 20),
			mutate: func(t *testing.T, bucket *lsmkv.Bucket) {
				require.NoError(t, removeRangeable(bucket, 15, 15))
			},
			wantAfter: append(seq(10, 14), seq(16, 20)...),
		},
		{
			name:       "equal",
			operator:   filters.OperatorEqual,
			value:      7,
			wantBefore: []uint64{7},
			mutate: func(t *testing.T, bucket *lsmkv.Bucket) {
				require.NoError(t, removeRangeable(bucket, 7, 7))
			},
			wantAfter: []uint64{},
		},
		{
			name:       "not equal",
			operator:   filters.OperatorNotEqual,
			value:      7,
			wantBefore: append(seq(1, 6), seq(8, 20)...),
			mutate: func(t *testing.T, bucket *lsmkv.Bucket) {
				require.NoError(t, addRangeable(bucket, 7, 30))
			},
			wantAfter: append(seq(1, 6), seq(8, 20)...),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			searcher, bucket := newRangeableInMemoFixture(t)

			// three sightings: miss, admit-and-store, then a hit
			for round := 0; round < 3; round++ {
				require.ElementsMatchf(t, tt.wantBefore,
					findUUIDsShapedQuery(t, searcher, tt.operator, tt.value), "round=%d", round)
			}

			tt.mutate(t, bucket)
			require.NoError(t, bucket.FlushAndSwitch())

			requireOneStableAnswer(t, tt.wantAfter, func() []uint64 {
				return findUUIDsShapedQuery(t, searcher, tt.operator, tt.value)
			})
		})
	}
}

// Test_BatchDelete_RangeableInMemory_RespectsLimit pins the one argument the
// delete path passes that buildAllowList does not.
func Test_BatchDelete_RangeableInMemory_RespectsLimit(t *testing.T) {
	searcher, _ := newRangeableInMemoFixture(t)
	filter := rangeableFilter(filters.OperatorLessThanEqual, 20)

	for round := 0; round < 3; round++ {
		list, err := searcher.DocIDsLimited(context.Background(), filter,
			additional.Properties{}, className, 4)
		require.NoError(t, err)
		require.Equal(t, 4, list.LimitedIterator(4).Len(), "round=%d", round)
		list.Close()
	}
}

// newRangeableInMemoFixture holds docIDs 1..20 at value == docID, in a rangeable
// bucket kept in memory, so the memo and the seeded cascade are both live.
func newRangeableInMemoFixture(t *testing.T) (*Searcher, *lsmkv.Bucket) {
	t.Helper()

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	bucketName := helpers.BucketRangeableFromPropNameLSM(rangeableInMemoProp)
	require.NoError(t, store.CreateOrLoadBucket(context.Background(), bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSetRange),
		lsmkv.WithKeepSegmentsInMemory(true),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	))
	bucket := store.Bucket(bucketName)

	for docID := uint64(1); docID <= 20; docID++ {
		require.NoError(t, addRangeable(bucket, int64(docID), docID))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// above every docID the tests insert, or NOT filters under-report
	bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(64))
	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	requireInMemoryRangeablePathIsLive(t, searcher)
	return searcher, bucket
}

// requireInMemoryRangeablePathIsLive is the positive control the leaf-cache
// counter assertions need: with the in-memory path off the counters never
// move, so a broken cache and a fixture that never reached it read the same
// unless the answer is checked too.
func requireInMemoryRangeablePathIsLive(t *testing.T, searcher *Searcher) {
	t.Helper()

	requireRangeablePathIsLive(t, 20, func() []uint64 {
		return findUUIDsShapedQuery(t, searcher, filters.OperatorGreaterThanEqual, 1)
	})
}

// requireRangeablePathIsLive is shared by every fixture's control check, so
// they all get the same live-path guarantee instead of drifting copies.
func requireRangeablePathIsLive(t *testing.T, want int, ask func() []uint64) {
	t.Helper()

	before := leafCacheCounters(t)

	got := ask()
	require.Equalf(t, want, len(got),
		"the fixture query returned %d of %d docs, so any counter reading below is vacuous", len(got), want)

	// Only the memo-eligible ops count: a disabled cache still moves the
	// disabled counter, which would satisfy "something moved" while every memo
	// assertion downstream silently reads zero.
	after := leafCacheCounters(t)
	var moved bool
	for _, op := range []string{"hit", "miss", "store"} {
		if after[op] > before[op] {
			moved = true
		}
	}
	require.Truef(t, moved,
		"the query returned the right answer without touching the memo (disabled +%.0f): either it "+
			"did not go through the in-memory segment — check WithKeepSegmentsInMemory — or the memo "+
			"is switched off via %s",
		after["disabled"]-before["disabled"], roaringsetrange.LeafCacheMaxMemoryEnv)
}

// Pins the direction easy to miss: delete populates the memo, not just reads
// it, and a later search must be served from what a delete stored.
func Test_BatchDelete_RangeableInMemory_SharesTheLeafCacheWithSearch(t *testing.T) {
	const value = 13
	operator := filters.OperatorLessThanEqual
	want := seq(1, value)

	searcher, bucket := newRangeableInMemoFixture(t)
	settleSegment(t, searcher)

	before := leafCacheCounters(t)

	// only the destructive entry point runs here: first sight enters the
	// admission ring, second is admitted and stored
	for round := 0; round < 2; round++ {
		require.ElementsMatchf(t, want, findUUIDsShapedQuery(t, searcher, operator, value), "round=%d", round)
	}
	stored := leafCacheCounters(t)
	require.Greaterf(t, stored["store"], before["store"],
		"the delete path stored nothing, so excluding only its read half would be a no-op")
	require.Equal(t, before["hit"], stored["hit"], "nothing should have been served from the memo yet")

	// the search entry point must now be served the entry a delete built
	require.ElementsMatch(t, want, searchShapedQuery(t, searcher, operator, value))
	require.Greaterf(t, leafCacheCounters(t)["hit"], stored["hit"],
		"a search was not served from the entry the delete path stored")

	// one mutation must invalidate that entry for both readers; a row added
	// above the threshold exposes a stale leaf for <= (see the operator table above).
	require.NoError(t, addRangeable(bucket, 20, 30))
	require.NoError(t, bucket.FlushAndSwitch())

	requireOneStableAnswer(t, want, func() []uint64 {
		return searchShapedQuery(t, searcher, operator, value)
	})
	requireOneStableAnswer(t, want, func() []uint64 {
		return findUUIDsShapedQuery(t, searcher, operator, value)
	})
}

// Covers the producing half only; this package can't see Shard, so the sink
// here is the test's own. Shard's own sink is covered by
// Test_FindUUIDs_RecordsHowTheFilterResolved in package db.
func Test_BatchDelete_RangeableInMemory_AnnotatesTheRangeCascade(t *testing.T) {
	searcher, _ := newRangeableInMemoFixture(t)

	ctx := helpers.InitSlowQueryDetails(context.Background())
	list, err := searcher.DocIDsLimited(ctx, rangeableFilter(filters.OperatorLessThanEqual, 10),
		additional.Properties{}, className, 0)
	require.NoError(t, err)
	list.Close()

	details := helpers.ExtractSlowQueryDetails(ctx)
	require.Contains(t, details, roaringsetrange.DocBitmapAnnotation)
	require.NotEmpty(t, details[roaringsetrange.DocBitmapAnnotation])

	perProp, ok := details["build_allow_list_doc_bitmap"].([]map[string]any)
	require.True(t, ok, "the per-property annotation is missing or changed shape")
	require.Len(t, perProp, 1)
	require.Equal(t, lsmkv.StrategyRoaringSetRange, perProp[0]["strategy"])
}

// Polls rather than checking once: a flush's writes reach the segment
// asynchronously via MergeMemtableEventually, so a single post-flush sample
// could pass while a missed invalidation is still masked by the pending
// memtable.
func requireOneStableAnswer(t *testing.T, want []uint64, ask func() []uint64) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	rounds := 0
	for time.Now().Before(deadline) {
		require.ElementsMatchf(t, want, ask(),
			"answer changed after %d stable rounds; a pre-mutation leaf was served", rounds)
		rounds++
	}
	require.Greater(t, rounds, 1)
}

// settleSegment waits out the async MergeMemtableEventually behind the
// fixture's flush, so counter deltas below aren't perturbed by a generation
// bump mid-test.
func settleSegment(t *testing.T, searcher *Searcher) {
	t.Helper()

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		findUUIDsShapedQuery(t, searcher, filters.OperatorGreaterThanEqual, 19)
	}
}

// searchShapedQuery is the buildAllowList side: DocIDs, not DocIDsLimited.
func searchShapedQuery(t *testing.T, searcher *Searcher, operator filters.Operator, value int64) []uint64 {
	t.Helper()

	list, err := searcher.DocIDs(context.Background(), rangeableFilter(operator, value),
		additional.Properties{}, className)
	require.NoError(t, err)
	defer list.Close()

	out := []uint64{}
	it := list.LimitedIterator(0)
	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		out = append(out, docID)
	}
	return out
}

// leafCacheCounters reads the range-filter leaf cache's counters out of the
// process registry. On the delete path they are the only observable there is.
func leafCacheCounters(t *testing.T) map[string]float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	out := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "weaviate_lsm_roaringsetrange_leaf_cache_ops_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "operation" {
					out[label.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}
	require.NotEmpty(t, out, "leaf cache counters are absent; the memo is not in this binary")
	return out
}

// findUUIDsShapedQuery mirrors Shard.FindUUIDs: DocIDsLimited and the limited
// iterator it feeds, not the DocIDs the search path uses.
func findUUIDsShapedQuery(t *testing.T, searcher *Searcher, operator filters.Operator, value int64) []uint64 {
	t.Helper()

	const limit = 0 // what DB.BatchDeleteObjects passes: no per-shard cap

	list, err := searcher.DocIDsLimited(context.Background(), rangeableFilter(operator, value),
		additional.Properties{}, className, limit)
	require.NoError(t, err)
	defer list.Close()

	out := []uint64{}
	it := list.LimitedIterator(limit)
	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		out = append(out, docID)
	}
	return out
}

func rangeableFilter(operator filters.Operator, value int64) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			On: &filters.Path{
				Class:    className,
				Property: schema.PropertyName(rangeableInMemoProp),
			},
			Value: &filters.Value{
				Value: int(value),
				Type:  schema.DataTypeInt,
			},
		},
	}
}

func addRangeable(bucket *lsmkv.Bucket, value int64, docIDs ...uint64) error {
	key, err := entinverted.LexicographicallySortableInt64(value)
	if err != nil {
		return fmt.Errorf("encode %d: %w", value, err)
	}
	return bucket.RoaringSetRangeAdd(binary.BigEndian.Uint64(key), docIDs...)
}

func removeRangeable(bucket *lsmkv.Bucket, value int64, docIDs ...uint64) error {
	key, err := entinverted.LexicographicallySortableInt64(value)
	if err != nil {
		return fmt.Errorf("encode %d: %w", value, err)
	}
	return bucket.RoaringSetRangeRemove(binary.BigEndian.Uint64(key), docIDs...)
}

func seq(from, to uint64) []uint64 {
	out := make([]uint64, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// TTL sweep: index_objects_ttl.go re-resolves one unchanging threshold via
// FindUUIDs -> DocIDsLimited until exhausted, which stresses the admission
// memo harder than batch delete (same predicate every iteration, no cap).

const rangeableTTLProp = "inverted-roaringsetrange-ttl-date"

// ttlSweepOrigin is fixed rather than time.Now() so a failure reproduces.
var ttlSweepOrigin = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func ttlSweepAt(minutes int) time.Time {
	return ttlSweepOrigin.Add(time.Duration(minutes) * time.Minute)
}

// Nanoseconds, because that's the unit both the analyzer (asTime.UnixNano())
// and extractDateValue use; milliseconds would put the threshold a million
// times above every row, matching everything vacuously.
func ttlSweepKey(minutes int) int64 {
	return ttlSweepAt(minutes).UnixNano()
}

// A date property with a rangeable index and no filterable one, matching what
// index_objects_ttl.go actually filters on. getBucketName sends <= to the
// rangeable bucket before consulting hasFilterableIndex, so unlike
// Equal/NotEqual this routing can't be diverted by adding a filterable index.
func ttlSweepSchema() *schema.Schema {
	vFalse := false
	vTrue := true

	return &schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: className,
					Properties: []*models.Property{
						{
							Name:              rangeableTTLProp,
							DataType:          schema.DataTypeDate.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vTrue,
						},
					},
				},
			},
		},
	}
}

// docIDs 1..20 sit one minute apart, so "<= origin+N" selects exactly
// docIDs 1..N against a threshold that stays fixed while rows disappear.
func newTTLSweepFixture(t *testing.T) (*Searcher, *lsmkv.Bucket) {
	t.Helper()

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	bucketName := helpers.BucketRangeableFromPropNameLSM(rangeableTTLProp)
	require.NoError(t, store.CreateOrLoadBucket(context.Background(), bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSetRange),
		lsmkv.WithKeepSegmentsInMemory(true),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	))
	bucket := store.Bucket(bucketName)

	for docID := uint64(1); docID <= 20; docID++ {
		require.NoError(t, addRangeable(bucket, ttlSweepKey(int(docID)), docID))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(64))
	searcher := NewSearcher(logger, store, ttlSweepSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	requireRangeablePathIsLive(t, 20, func() []uint64 {
		return ttlSweepFind(t, searcher, ttlSweepAt(20), ttlSweepBatchSize)
	})
	return searcher, bucket
}

// OBJECTS_TTL_BATCH_SIZE's default. The rangeable branch of docBitmap ignores
// the limit and resolves the whole expired set; LimitedIterator applies the
// cap afterwards, so using the real default keeps that visible.
const ttlSweepBatchSize = 10_000

// Mirrors Shard.FindUUIDs's DocIDsLimited call, but with the sweep's
// per-shard batch limit instead of batch delete's uncapped 0.
func ttlSweepFind(t *testing.T, searcher *Searcher, threshold time.Time, limit int) []uint64 {
	t.Helper()

	list, err := searcher.DocIDsLimited(context.Background(), ttlFilter(threshold),
		additional.Properties{}, className, limit)
	require.NoError(t, err)
	defer list.Close()

	out := []uint64{}
	it := list.LimitedIterator(limit)
	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		out = append(out, docID)
	}
	return out
}

func ttlFilter(threshold time.Time) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorLessThanEqual,
			On: &filters.Path{
				Class:    className,
				Property: schema.PropertyName(rangeableTTLProp),
			},
			Value: &filters.Value{
				Value: threshold,
				Type:  schema.DataTypeDate,
			},
		},
	}
}

// Waits out the async MergeMemtableEventually behind the fixture's flush,
// using a predicate the tests never assert on so it doesn't perturb their
// counters or seed their admission entry.
func settleTTLSegment(t *testing.T, searcher *Searcher) {
	t.Helper()

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		ttlSweepFind(t, searcher, ttlSweepAt(19), ttlSweepBatchSize)
	}
}

// counterDelta reads one process-global registry, so every test below assumes
// no sibling rangeable test runs concurrently; none in this package calls
// t.Parallel(), and the exact per-iteration deltas would go flaky if one did.
func counterDelta(before, after map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for op, v := range after {
		out[op] = v - before[op]
	}
	return out
}

// Confirms the sweep's date filter is actually served by the range cascade
// over the in-memory segment, so the counter assertions in the other TTL
// tests aren't vacuous.
func Test_TTLSweep_RangeableInMemory_ReachesTheMemoisedLeaf(t *testing.T) {
	searcher, _ := newTTLSweepFixture(t)

	ctx := helpers.InitSlowQueryDetails(context.Background())
	list, err := searcher.DocIDsLimited(ctx, ttlFilter(ttlSweepAt(10)),
		additional.Properties{}, className, ttlSweepBatchSize)
	require.NoError(t, err)
	defer list.Close()

	details := helpers.ExtractSlowQueryDetails(ctx)
	require.Contains(t, details, roaringsetrange.DocBitmapAnnotation,
		"the sweep's date filter did not go through the range cascade")

	perProp, ok := details["build_allow_list_doc_bitmap"].([]map[string]any)
	require.True(t, ok, "the per-property annotation is missing or changed shape")
	require.Len(t, perProp, 1)
	require.Equal(t, lsmkv.StrategyRoaringSetRange, perProp[0]["strategy"],
		"a date property with IndexRangeFilters must route to the rangeable bucket")
}

// One unchanging predicate walks the admission filter alone. Pinned per
// iteration because it's off by one from the obvious reading: first sight
// only records the key, second sight is what stores and admits.
func Test_TTLSweep_RangeableInMemory_MemoisesFromTheThirdIteration(t *testing.T) {
	searcher, _ := newTTLSweepFixture(t)
	settleTTLSegment(t, searcher)

	threshold := ttlSweepAt(12)
	want := seq(1, 12)

	prev := leafCacheCounters(t)
	deltas := make([]map[string]float64, 0, 3)
	for round := 0; round < 3; round++ {
		require.ElementsMatchf(t, want,
			ttlSweepFind(t, searcher, threshold, ttlSweepBatchSize), "round=%d", round)
		now := leafCacheCounters(t)
		deltas = append(deltas, counterDelta(prev, now))
		prev = now
	}

	assert.Equal(t, float64(1), deltas[0]["miss"], "first sight only records the predicate")
	assert.Zero(t, deltas[0]["store"], "nothing may be admitted on first sight")
	assert.Zero(t, deltas[0]["hit"])

	assert.Equal(t, float64(1), deltas[1]["miss"])
	assert.Equal(t, float64(1), deltas[1]["store"], "second sight is what admits and stores")
	assert.Zero(t, deltas[1]["hit"])

	assert.Equal(t, float64(1), deltas[2]["hit"], "the third iteration must be served from the memo")
	assert.Zero(t, deltas[2]["store"])
	assert.Zero(t, deltas[2]["miss"])
}

// liveUpTo is the oracle's answer: the rows at or below maxDocID this test has
// inserted and not yet retired, derived from the test's own record rather than
// from the index it is checking.
func liveUpTo(live map[uint64]bool, maxDocID uint64) []uint64 {
	out := []uint64{}
	for docID := uint64(1); docID <= maxDocID; docID++ {
		if live[docID] {
			out = append(out, docID)
		}
	}
	return out
}

// requireSegmentAbsorbedTheFlush waits out the async MergeMemtableEventually
// behind FlushAndSwitch. invalidate rising is the only observable that the
// planes took the write, because the answer is identical either way — which is
// exactly what makes the two layers hard to tell apart from outside.
func requireSegmentAbsorbedTheFlush(t *testing.T, searcher *Searcher, threshold time.Time) {
	t.Helper()

	before := leafCacheCounters(t)["invalidate"]
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ttlSweepFind(t, searcher, threshold, 0)
		if leafCacheCounters(t)["invalidate"] > before {
			return
		}
	}
	t.Fatal("the flush never reached the planes: invalidate never rose, so the segment still " +
		"holds the pre-flush leaf and only one layer of the composition is populated")
}

// Test_TTLSweep_RangeableInMemory_EveryIterationIsExact is the invariant behind
// the sweep's answer being a composition rather than a single lookup: a
// memoised segment leaf that deliberately does not move, and a memtable layer
// subtracting the rows already retired from under it.
//
// The invariant is exactness at every instant, not convergence — an iteration
// that omits a live victim under-deletes, and nothing downstream would notice,
// because the sweep's own stop condition is an empty batch and an under-count
// looks the same as being finished.
//
// The sweep crosses a flush so both layers carry deletions at once: rows
// retired before it sit in the planes, rows retired after it sit in the
// memtable, and the memo is serving across both.
func Test_TTLSweep_RangeableInMemory_EveryIterationIsExact(t *testing.T) {
	searcher, bucket := newTTLSweepFixture(t)
	settleTTLSegment(t, searcher)

	const (
		batch = 2 // small, so the sweep runs well past third sight
		// A threshold below the fixture's top row, so 16..20 are present but
		// never expired: a leaf that over-returns is caught as well as one that
		// omits.
		thresholdMin = 15
		flushAfter   = 3
	)

	live := map[uint64]bool{}
	for docID := uint64(1); docID <= 20; docID++ {
		live[docID] = true
	}

	threshold := ttlSweepAt(thresholdMin)
	before := leafCacheCounters(t)

	var (
		retired       []uint64
		hitsPreFlush  float64
		hitsPostFlush float64
		flushed       bool
		iterations    int
	)

	for {
		require.Lessf(t, iterations, 20,
			"the sweep did not terminate; it kept resolving rows it had already retired")
		iterations++

		iterBefore := leafCacheCounters(t)

		// The untruncated answer is what the invariant is about; the limited
		// read below is what the sweep actually consumes.
		full := ttlSweepFind(t, searcher, threshold, 0)
		require.ElementsMatchf(t, liveUpTo(live, thresholdMin), full,
			"iteration %d returned a victim set that was already wrong when it was returned, not "+
				"merely wrong at the end: the memoised leaf and the layer beneath it no longer "+
				"compose to the current state", iterations)

		found := ttlSweepFind(t, searcher, threshold, batch)
		require.Subsetf(t, full, found, "iteration %d", iterations)
		require.LessOrEqualf(t, len(found), batch, "iteration %d", iterations)

		iterDelta := counterDelta(iterBefore, leafCacheCounters(t))
		if flushed {
			hitsPostFlush += iterDelta["hit"]
		} else {
			hitsPreFlush += iterDelta["hit"]
		}

		if len(found) == 0 {
			break // findAndDelete's stop condition
		}

		for _, docID := range found {
			require.NoError(t, removeRangeable(bucket, ttlSweepKey(int(docID)), docID))
			delete(live, docID)
		}
		retired = append(retired, found...)

		if iterations == flushAfter {
			require.NoError(t, bucket.FlushAndSwitch())
			requireSegmentAbsorbedTheFlush(t, searcher, threshold)
			flushed = true
		}
	}

	require.ElementsMatch(t, seq(1, thresholdMin), retired,
		"every expired row must be retired exactly once, and no unexpired row at all")
	require.True(t, flushed, "the sweep never crossed a flush, so only one layer was ever populated")

	assert.NotZero(t, hitsPreFlush,
		"no iteration before the flush was served from the memo, so a memoised leaf was never "+
			"held against memtable deletions")
	assert.NotZero(t, hitsPostFlush,
		"no iteration after the flush was served from the memo, so the two-layer composition — "+
			"rows retired earlier in the planes, later ones still in the memtable — was never "+
			"exercised against a memoised leaf")

	after := counterDelta(before, leafCacheCounters(t))
	assert.NotZero(t, after["invalidate"], "the mid-sweep flush must have dropped the memoised leaf")
}

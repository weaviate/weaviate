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

	return searcher, bucket
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

// Shard.FindUUIDs must install the slow-log sink so this annotation reaches an
// operator; otherwise a cascade-routed delete leaves only an inexpressive
// counter, no per-operation record.
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

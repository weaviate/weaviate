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
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// Regression test: resolveDocIDsAndOr/extractPropValuePairs chunked OR-merge
// with operand count above GOMAXPROCS, so >1 leaf lands in the same chunk (GH 12242).
func TestSearcher_ContainsAny_LargeValueSet(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	const propName = "inverted-text-roaringset"
	const numValues = 2000 // several multiples of GOMAXPROCS on any runner

	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	maxDocID := uint64(numValues*3 + 10)
	bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(maxDocID))
	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	bucketName := helpers.BucketFromPropNameLSM(propName)
	require.NoError(t, store.CreateOrLoadBucket(
		context.Background(), bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	))
	bucket := store.Bucket(bucketName)

	// Each value maps to 3 doc IDs: [3*i, 3*i+1, 3*i+2]. Every value's doc
	// IDs are disjoint from every other value's, and a handful of values
	// (every 7th) share one doc ID with the *next* value in the sequence so
	// the OR-merge has real overlap to dedupe, not just disjoint unions.
	values := make([]string, numValues)
	expected := map[uint64]struct{}{}
	for i := 0; i < numValues; i++ {
		val := fmt.Sprintf("val_%05d", i)
		values[i] = val
		ids := []uint64{uint64(3 * i), uint64(3*i + 1), uint64(3*i + 2)}
		if i%7 == 0 && i+1 < numValues {
			// overlap this value's last ID with the next value's first ID
			ids[2] = uint64(3 * (i + 1))
		}
		require.NoError(t, bucket.RoaringSetAddList([]byte(val), ids))
		for _, id := range ids {
			expected[id] = struct{}{}
		}
	}
	require.NoError(t, bucket.FlushAndSwitch())

	filter := &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.ContainsAny,
			On:       &filters.Path{Class: className, Property: schema.PropertyName(propName)},
			Value:    &filters.Value{Value: values, Type: schema.DataTypeText},
		},
	}

	require.Greater(t, numValues, runtime.GOMAXPROCS(0),
		"test fixture must exceed GOMAXPROCS so chunking exercises >1 leaf per goroutine")

	allowList, err := searcher.DocIDs(context.Background(), filter, additional.Properties{}, className)
	require.NoError(t, err)
	defer allowList.Close()

	got := allowList.Slice()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })

	wantSorted := make([]uint64, 0, len(expected))
	for id := range expected {
		wantSorted = append(wantSorted, id)
	}
	sort.Slice(wantSorted, func(i, j int) bool { return wantSorted[i] < wantSorted[j] })

	require.Equal(t, wantSorted, got,
		"ContainsAny over %d values must OR-merge to exactly the union of every value's doc IDs, "+
			"deduped, regardless of how leaves are chunked across goroutines", numValues)
}

// Regression test: ContainsAll chunked AND-fold must intersect every leaf
// inside a chunk, not just the last one resolved (GH 12242).
func TestSearcher_ContainsAll_LargeValueSet(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	const propName = "inverted-text-roaringset"
	const numValues = 200 // several multiples of GOMAXPROCS on any runner
	const bufferSize = 10 // doc IDs no value excludes; the true AND result

	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	maxDocID := uint64(numValues + bufferSize)
	bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(maxDocID))
	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	bucketName := helpers.BucketFromPropNameLSM(propName)
	require.NoError(t, store.CreateOrLoadBucket(
		context.Background(), bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	))
	bucket := store.Bucket(bucketName)

	values := make([]string, numValues)
	for i := 0; i < numValues; i++ {
		val := fmt.Sprintf("val_%05d", i)
		values[i] = val
		ids := make([]uint64, 0, maxDocID-1)
		for id := uint64(0); id < maxDocID; id++ {
			if id == uint64(i) {
				continue // value i excludes only its own index
			}
			ids = append(ids, id)
		}
		require.NoError(t, bucket.RoaringSetAddList([]byte(val), ids))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	filter := &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.ContainsAll,
			On:       &filters.Path{Class: className, Property: schema.PropertyName(propName)},
			Value:    &filters.Value{Value: values, Type: schema.DataTypeText},
		},
	}

	require.Greater(t, numValues, runtime.GOMAXPROCS(0),
		"test fixture must exceed GOMAXPROCS so chunking exercises >1 leaf per goroutine")

	allowList, err := searcher.DocIDs(context.Background(), filter, additional.Properties{}, className)
	require.NoError(t, err)
	defer allowList.Close()

	got := allowList.Slice()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })

	want := make([]uint64, 0, bufferSize)
	for id := uint64(numValues); id < maxDocID; id++ {
		want = append(want, id)
	}

	require.Equal(t, want, got,
		"ContainsAll over %d values must AND-fold to exactly the buffer region every value leaves "+
			"untouched, with every value's own excluded index correctly dropped regardless of how "+
			"leaves are chunked across goroutines",
		numValues)
}

// Regression test: chunk-local accumulation must correctly OR-fold a
// deny-list leaf with allow leaves that partially reclaim it, within a
// single chunk (GH 12242).
func TestSearcher_ContainsAny_DenyListLeafInChunk(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	const propName = "inverted-text-roaringset"
	const denyRangeSize = 150    // deny leaf excludes doc IDs [0, denyRangeSize)
	const numReclaimLeaves = 100 // allow leaves reclaiming [0, numReclaimLeaves) from the deny range
	const numPadLeaves = 100     // padding leaves outside the deny range, to push operand count above GOMAXPROCS

	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	maxDocID := uint64(denyRangeSize + numPadLeaves + 10)
	bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(maxDocID))
	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	bucketName := helpers.BucketFromPropNameLSM(propName)
	require.NoError(t, store.CreateOrLoadBucket(
		context.Background(), bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	))
	bucket := store.Bucket(bucketName)

	path := &filters.Path{Class: className, Property: schema.PropertyName(propName)}
	operands := make([]filters.Clause, 0, numReclaimLeaves+numPadLeaves+1)

	denyIDs := make([]uint64, denyRangeSize)
	for i := range denyIDs {
		denyIDs[i] = uint64(i)
	}
	const denyVal = "deny_value"
	require.NoError(t, bucket.RoaringSetAddList([]byte(denyVal), denyIDs))
	operands = append(operands, filters.Clause{
		Operator: filters.OperatorNot,
		Operands: []filters.Clause{
			{
				Operator: filters.OperatorEqual,
				On:       path,
				Value:    &filters.Value{Value: denyVal, Type: schema.DataTypeText},
			},
		},
	})

	for i := 0; i < numReclaimLeaves; i++ {
		val := fmt.Sprintf("reclaim_%05d", i)
		require.NoError(t, bucket.RoaringSetAddList([]byte(val), []uint64{uint64(i)}))
		operands = append(operands, filters.Clause{
			Operator: filters.OperatorEqual,
			On:       path,
			Value:    &filters.Value{Value: val, Type: schema.DataTypeText},
		})
	}
	for i := 0; i < numPadLeaves; i++ {
		id := uint64(denyRangeSize + i)
		val := fmt.Sprintf("pad_%05d", i)
		require.NoError(t, bucket.RoaringSetAddList([]byte(val), []uint64{id}))
		operands = append(operands, filters.Clause{
			Operator: filters.OperatorEqual,
			On:       path,
			Value:    &filters.Value{Value: val, Type: schema.DataTypeText},
		})
	}
	require.NoError(t, bucket.FlushAndSwitch())

	filter := &filters.LocalFilter{
		Root: &filters.Clause{Operator: filters.OperatorOr, Operands: operands},
	}

	require.Greater(t, len(operands), runtime.GOMAXPROCS(0),
		"test fixture must exceed GOMAXPROCS so chunking exercises >1 leaf per goroutine, "+
			"including at least one chunk that mixes a reclaiming allow leaf with the deny leaf")

	allowList, err := searcher.DocIDs(context.Background(), filter, additional.Properties{}, className)
	require.NoError(t, err)
	defer allowList.Close()

	got := allowList.Slice()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })

	stillExcluded := make([]uint64, 0, denyRangeSize-numReclaimLeaves)
	for id := uint64(numReclaimLeaves); id < denyRangeSize; id++ {
		stillExcluded = append(stillExcluded, id)
	}
	want := sroar.Prefill(maxDocID).AndNot(roaringset.NewBitmap(stillExcluded...)).ToArray()

	require.Equal(t, want, got,
		"OR of a deny-list leaf excluding [0,%d) with %d reclaiming allow leaves and %d padding "+
			"allow leaves must fold to the deny-list's complement with exactly [%d,%d) still "+
			"excluded, regardless of how leaves are chunked across goroutines",
		denyRangeSize, numReclaimLeaves, numPadLeaves, numReclaimLeaves, denyRangeSize)
}

// Regression test: a chunk goroutine must release its accumulated
// chunkResult (no bitmap-buffer leak) when it observes cancellation
// mid-chunk, before finishing its remaining leaves.
func TestSearcher_ContainsAny_CancellationMidChunk(t *testing.T) {
	const propName = "inverted-text-roaringset"
	const numValues = 5000 // wide enough that cancelDelay reliably lands mid-query
	const cancelDelay = 2 * time.Millisecond
	const attempts = 8

	sawContextError := false
	for attempt := 0; attempt < attempts; attempt++ {
		t.Run(fmt.Sprintf("attempt_%d", attempt), func(t *testing.T) {
			dirName := t.TempDir()
			logger, _ := test.NewNullLogger()

			store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop())
			require.NoError(t, err)
			t.Cleanup(func() { store.Shutdown(context.Background()) })

			maxDocID := uint64(numValues*3 + 10)
			bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(maxDocID))
			searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
				stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
				config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

			bucketName := helpers.BucketFromPropNameLSM(propName)
			trackingPool := roaringset.NewBitmapBufPoolTracking()
			require.NoError(t, store.CreateOrLoadBucket(
				context.Background(), bucketName,
				lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
				lsmkv.WithBitmapBufPool(trackingPool),
			))
			bucket := store.Bucket(bucketName)

			values := make([]string, numValues)
			for i := 0; i < numValues; i++ {
				val := fmt.Sprintf("val_%05d", i)
				values[i] = val
				ids := []uint64{uint64(3 * i), uint64(3*i + 1), uint64(3*i + 2)}
				require.NoError(t, bucket.RoaringSetAddList([]byte(val), ids))
			}
			require.NoError(t, bucket.FlushAndSwitch())

			filter := &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.ContainsAny,
					On:       &filters.Path{Class: className, Property: schema.PropertyName(propName)},
					Value:    &filters.Value{Value: values, Type: schema.DataTypeText},
				},
			}

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			cancelFired := make(chan struct{})
			go func() {
				time.Sleep(cancelDelay)
				cancel()
				close(cancelFired)
			}()

			allowList, err := searcher.DocIDs(ctx, filter, additional.Properties{}, className)
			<-cancelFired // ensure the cancel goroutine has fired before Outstanding() is read

			if err != nil {
				require.ErrorContains(t, err, "context canceled")
				sawContextError = true
			} else {
				allowList.Close()
			}

			require.Zero(t, trackingPool.Outstanding(),
				"cancellation mid-chunk must release every leaf bitmap buffer already folded "+
					"into a chunk's local accumulator, not just the ones a later leaf error "+
					"happens to unwind")
		})
	}

	require.True(t, sawContextError,
		"expected at least one of %d cancellation attempts to observe a context error "+
			"(locks in that the release-on-cancel/error-propagation path is reachable, not "+
			"merely theoretical); if this ever fails, re-check cancelDelay/numValues against "+
			"current hardware speed", attempts)
}

// TestSearcher_ContainsAny_DeleteThenReaddAcrossSegments guards oldest->newest
// fold order across segment boundaries: deletes for "tombstone-readd" and
// "deleted-forever" land in a segment newer than their additions, so a
// wrong-order fold would resurrect a deleted doc ID.
func TestSearcher_ContainsAny_DeleteThenReaddAcrossSegments(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	const propName = "inverted-text-roaringset"

	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	maxDocID := uint64(100)
	bitmapFactory := roaringset.NewBitmapFactory(roaringset.NewBitmapBufPoolNoop(), newFakeMaxIDGetter(maxDocID))
	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false }, nil, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	bucketName := helpers.BucketFromPropNameLSM(propName)
	require.NoError(t, store.CreateOrLoadBucket(
		context.Background(), bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	))
	bucket := store.Bucket(bucketName)

	// segment 1 (oldest)
	require.NoError(t, bucket.RoaringSetAddList([]byte("disk-seg1-only"), []uint64{1, 2, 3}))
	require.NoError(t, bucket.RoaringSetAddList([]byte("tombstone-readd"), []uint64{10, 11}))
	require.NoError(t, bucket.RoaringSetAddList([]byte("deleted-forever"), []uint64{20}))
	require.NoError(t, bucket.RoaringSetAddList([]byte("shared-across-segments"), []uint64{30}))
	require.NoError(t, bucket.FlushAndSwitch())

	// segment 2 (newer): deletes land here, newer than the additions
	require.NoError(t, bucket.RoaringSetRemoveOne([]byte("tombstone-readd"), 10))
	require.NoError(t, bucket.RoaringSetRemoveOne([]byte("deleted-forever"), 20))
	require.NoError(t, bucket.RoaringSetAddList([]byte("disk-seg2-only"), []uint64{40, 41}))
	require.NoError(t, bucket.RoaringSetAddList([]byte("shared-across-segments"), []uint64{31}))
	require.NoError(t, bucket.FlushAndSwitch())

	// active memtable (never flushed)
	require.NoError(t, bucket.RoaringSetAddList([]byte("tombstone-readd"), []uint64{12}))
	require.NoError(t, bucket.RoaringSetAddList([]byte("shared-across-segments"), []uint64{32}))

	values := []string{
		"disk-seg1-only", "disk-seg2-only", "tombstone-readd", "deleted-forever",
		"shared-across-segments", "never-existed",
	}

	filter := &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.ContainsAny,
			On:       &filters.Path{Class: className, Property: schema.PropertyName(propName)},
			Value:    &filters.Value{Value: values, Type: schema.DataTypeText},
		},
	}

	allowList, err := searcher.DocIDs(context.Background(), filter, additional.Properties{}, className)
	require.NoError(t, err)
	defer allowList.Close()

	got := allowList.Slice()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })

	// hand-computed oracle, independent of the code under test: tombstone-readd
	// nets to {11,12}, deleted-forever nets to {}, shared-across-segments
	// accumulates to {30,31,32}.
	want := []uint64{1, 2, 3, 11, 12, 30, 31, 32, 40, 41}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })

	require.Equal(t, want, got,
		"ContainsAny over a delete-then-readd-across-segments fixture must fold oldest->newest "+
			"through the floor's batch-acquired view + FastOr union: 'tombstone-readd' must resolve "+
			"to {11,12} (not resurrect 10), 'deleted-forever' must contribute zero doc IDs (not error, "+
			"not resurrect 20), and 'shared-across-segments' must accumulate across all three storage "+
			"layers to {30,31,32}")
}

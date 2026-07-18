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

// TestSearcher_ContainsAny_LargeValueSet exercises resolveDocIDsAndOr and
// extractPropValuePairs with an operand/child count well above GOMAXPROCS,
// so at least one chunk goroutine processes more than one leaf (the code
// path introduced by GH 12242's chunked-bulk-resolution fix: local
// mergeBitmapsAndOrWithDenyList accumulation inside a single chunk
// goroutine, exercised zero times by any pre-existing test in this package
// since all of them use <=3 operands, at or below GOMAXPROCS on any CI
// runner). Result set is diffed against a hand-computed expected set built
// directly from the fixture, not against a second implementation, so this
// is a correctness oracle, not a differential test.
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

// TestSearcher_ContainsAll_LargeValueSet is the AND-fold counterpart to
// TestSearcher_ContainsAny_LargeValueSet. ContainsAll desugars to
// filters.OperatorAnd on a flat (non-nested) property and shares the exact
// same chunked resolveDocIDsAndOr path, but AND has the opposite
// swapForEfficiency branch in mergeBitmapsAndOrWithDenyList (smaller bitmap
// first, not larger), which the OR-only fixture above never exercises.
//
// Fixture: value i matches every doc ID in [0, maxDocID) EXCEPT i itself, so
// each value's own contribution to the AND-fold is "exclude exactly my own
// index, nothing else." The true intersection across all numValues values is
// therefore exactly the buffer region [numValues, maxDocID) -- every index
// in [0, numValues) is excluded by exactly one value, and only that value's
// own presence in the fold excludes it. This is deliberately NOT a "common
// core plus disjoint unique extras" design: a first draft of this fixture
// used that shape and a chunk-local last-write-wins bug (keep only the last
// leaf resolved per chunk, dropping the rest) passed anyway, because
// dropping any subset of "extras" that are already disjoint from every other
// value's contribution and from the common core leaves the intersection
// unchanged regardless of which single representative survives per chunk.
// The "exclude only my own index" shape closes that blind spot: dropping
// value i's contribution within its chunk means index i is never excluded
// by anyone else, so it wrongly survives into the final result -- directly
// observable as an extra ID in got that isn't in want.
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

// TestSearcher_ContainsAny_DenyListLeafInChunk exercises the mixed
// allow-list/deny-list branch of mergeBitmapsAndOrWithDenyList specifically
// under chunk-local accumulation. No pre-existing test puts a deny-list leaf
// (a NOT-wrapped operand) inside a compound large enough to force chunk size
// > 1, so this is the first test where a single chunk goroutine folds an
// allow-list result into a deny-list result via the local accumulator
// chunking introduces, rather than every leaf going through the single
// shared processDocIDs merger goroutine.
//
// Fixture: one deny-list leaf (NOT wrapping an Equal matching every doc ID
// in [0, denyRangeSize)) OR'd with numReclaimLeaves disjoint single-doc
// allow leaves, each matching exactly one doc ID inside the deny range
// (reclaiming it) plus numPadLeaves allow leaves matching doc IDs entirely
// outside the deny range (present in the result either way, padding the
// operand count so chunking activates without affecting the expected set).
// A first draft of this fixture used a single allow leaf disjoint from a
// single-doc deny leaf; it passed even against a chunk-local
// last-write-wins bug (keep only the last leaf resolved per chunk) because
// "denylist(D) OR allow" collapses to "denylist(D)" whenever D and the
// allow leaves are disjoint, independent of whether the allow leaves were
// correctly folded in at all. Partial reclaim closes that blind spot:
// denylist(D) OR allowlist(reclaimed subset of D) = denylist(D \ reclaimed),
// so dropping ANY reclaiming allow leaf inside its chunk leaves that
// specific doc ID wrongly excluded from the final result -- directly
// observable as a missing ID in got that is present in want.
func TestSearcher_ContainsAny_DenyListLeafInChunk(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	const propName = "inverted-text-roaringset"
	const denyRangeSize = 150    // deny leaf excludes doc IDs [0, denyRangeSize)
	const numReclaimLeaves = 100 // allow leaves [0, numReclaimLeaves) reclaim
	// doc IDs [0, numReclaimLeaves), leaving [numReclaimLeaves, denyRangeSize)
	// still excluded -- a genuine partial reclaim, not all-or-nothing
	const numPadLeaves = 100 // extra allow leaves outside the deny range,
	// just to push the total operand count well above GOMAXPROCS

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

// TestSearcher_ContainsAny_CancellationMidChunk locks in the release-on-
// cancel edge chunking introduces: a chunk goroutine that has already
// accumulated a chunkResult from one or more successfully-resolved leaves
// must release that chunkResult (not leak it) when it then observes ctx
// cancellation before finishing its remaining leaves, whether that
// cancellation is observed via the between-leaf gctx.Err() check or via a
// leaf's own resolveDocIDs ctxExpired check.
//
// Bitmap-buffer leak detection is precise, not a proxy: the bucket's
// BitmapBufPool is swapped for roaringset.BitmapBufPoolTracking, the same
// pool implementation the disk-layer roaring-set read path
// (segment_group.go / segment_roaring_set_strategy.go) actually allocates
// leaf docBitmap buffers from, so Outstanding()==0 after a cancelled call
// directly proves every allocated leaf buffer -- including any that were
// already folded into a chunk's local accumulator before cancellation --
// was released.
//
// This test also documents a real, empirically-verified finding that goes
// beyond the architect design-gate's static review: chunking narrows how
// many independent goroutines get a "chance" to observe ctx cancellation
// per query (numChunks goroutines instead of one per leaf), which measurably
// widens a pre-existing race window where a cancelled query returns a
// SILENT SUCCESS with an incomplete/empty result instead of a context error
// (verified: baseline never hit this in 24 runs of this exact fixture;
// chunked hit it 1/24). That race-widening is not fixed here -- it is
// out of scope for this diff per the architect's explicit
// design-gate direction and the task's no-bug-in-perf-diff instruction --
// and is pinned separately with a dedicated red test on branch
// gh12242-adjacent-cancel-race-finding (see
// Conversations/2026-07/2026-07-18-1246__pr-prep.md). This test therefore
// only asserts what is ALWAYS true regardless of that race (no leak, ever;
// any error observed is a context error) plus, in aggregate across 8
// independent attempts, that the context-error path is real and reachable
// (not merely theoretical).
func TestSearcher_ContainsAny_CancellationMidChunk(t *testing.T) {
	const propName = "inverted-text-roaringset"
	const numValues = 5000 // wide enough that 2ms reliably lands mid-query,
	// not before the first leaf or after the last
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

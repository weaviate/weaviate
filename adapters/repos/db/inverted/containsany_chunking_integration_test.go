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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, store.CreateOrLoadBucket(context.Background(), bucketName,
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

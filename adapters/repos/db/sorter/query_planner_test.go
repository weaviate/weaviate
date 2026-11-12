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

package sorter

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestQueryPlanner(t *testing.T) {
	type testCase struct {
		name                 string
		objectCount          int
		matchCount           int
		nilBitmap            bool
		limit                int
		sort                 []filters.Sort
		shouldChooseInverted bool
		disabled             bool
		unsupportedType      bool
	}

	testCases := []testCase{
		{
			name:        "fewer matches than limit",
			objectCount: 1000,
			matchCount:  50,
			limit:       100,
			sort: []filters.Sort{
				{
					Path:  []string{"int"},
					Order: "asc",
				},
			},
			// with fewer than limit matches, we need to read every object from the
			// object store anyway. cheaper to sort using the object store
			shouldChooseInverted: false,
		},
		{
			name:        "high match ratio",
			objectCount: 1000,
			matchCount:  800,
			limit:       100,
			sort: []filters.Sort{
				{
					Path:  []string{"int"},
					Order: "asc",
				},
			},
			shouldChooseInverted: true,
		},
		{
			name:        "low match ratio, but high absolute count",
			objectCount: 10000,
			matchCount:  800,
			limit:       10,
			sort: []filters.Sort{
				{
					Path:  []string{"int"},
					Order: "asc",
				},
			},
			shouldChooseInverted: true,
		},
		{
			name:        "prop is not indexed",
			objectCount: 1000,
			matchCount:  800,
			limit:       100,
			sort: []filters.Sort{
				{
					Path:  []string{"int_not_indexed"},
					Order: "asc",
				},
			},
			shouldChooseInverted: false,
		},
		{
			name:        "prop is indexed, but missing",
			objectCount: 1000,
			matchCount:  800,
			limit:       100,
			sort: []filters.Sort{
				{
					Path:  []string{"int_corrupt_index"},
					Order: "asc",
				},
			},
			// possibly corrupt inverted index, fall back to objects bucket strategy
			shouldChooseInverted: false,
		},
		{
			name:                 "multiple sort args, first one eligible",
			objectCount:          1000,
			matchCount:           800,
			limit:                100,
			sort:                 sortsBy("int", "desc", "text", "asc"),
			shouldChooseInverted: true,
		},
		{
			name:                 "multiple sort args, first one is text",
			objectCount:          1000,
			matchCount:           800,
			limit:                100,
			sort:                 sortsBy("text", "asc", "int", "asc"),
			shouldChooseInverted: false,
			unsupportedType:      true,
		},
		{
			name:                 "multiple sort args, first one is not indexed",
			objectCount:          1000,
			matchCount:           800,
			limit:                100,
			sort:                 sortsBy("int_not_indexed", "asc", "int", "asc"),
			shouldChooseInverted: false,
		},
		{
			name:                 "multiple sort args, first one eligible, but globally disabled",
			objectCount:          1000,
			matchCount:           800,
			limit:                100,
			disabled:             true,
			sort:                 sortsBy("int", "asc", "text", "asc"),
			shouldChooseInverted: false,
		},
		{
			name:                 "single sort arg, cheaper than objects bucket",
			objectCount:          50,
			matchCount:           11,
			limit:                10,
			sort:                 sortsBy("int", "asc"),
			shouldChooseInverted: true,
		},
		{
			// the same query, but reading the candidates from the objects bucket
			// tips it over
			name:                 "multiple sort args, candidate reads make it costlier than objects bucket",
			objectCount:          50,
			matchCount:           11,
			limit:                10,
			sort:                 sortsBy("int", "asc", "number", "asc"),
			shouldChooseInverted: false,
		},
		{
			name:        "prop is not a supported type",
			objectCount: 1000,
			matchCount:  800,
			limit:       100,
			sort: []filters.Sort{
				{
					Path:  []string{"text"},
					Order: "asc",
				},
			},
			shouldChooseInverted: false,
			unsupportedType:      true,
		},
		{
			name:        "nil bitmap, i.e. unfiltered search",
			objectCount: 1000,
			nilBitmap:   true,
			limit:       100,
			sort: []filters.Sort{
				{
					Path:  []string{"int"},
					Order: "asc",
				},
			},
			shouldChooseInverted: true,
		},
		{
			name:        "nil bitmap, i.e. unfiltered search - unsupported prop",
			objectCount: 1000,
			nilBitmap:   true,
			limit:       100,
			sort: []filters.Sort{
				{
					Path:  []string{"text"},
					Order: "asc",
				},
			},
			shouldChooseInverted: false,
			unsupportedType:      true,
		},
		{
			name:        "high match ratio, but globally disabled",
			objectCount: 1000,
			matchCount:  800,
			limit:       100,
			disabled:    true,
			sort: []filters.Sort{
				{
					Path:  []string{"int"},
					Order: "asc",
				},
			},
			shouldChooseInverted: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				dirName   = t.TempDir()
				logger, _ = test.NewNullLogger()
				ctx       = helpers.InitSlowQueryDetails(context.Background())
			)
			store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop())
			require.Nil(t, err)
			defer store.Shutdown(ctx)

			err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM, lsmkv.WithCalcCountNetAdditions(true), lsmkv.WithStrategy(lsmkv.StrategyReplace))
			require.Nil(t, err)

			objectsB := store.Bucket(helpers.ObjectsBucketLSM)
			for i := 0; i < tc.objectCount; i++ {
				objBytes, docID := createDummyObject(t, i)
				objectsB.Put([]byte(fmt.Sprintf("%08d", docID)), objBytes)
				require.Nil(t, err)
			}

			for _, propName := range []string{"int", "number", "date"} {
				err = store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM(propName),
					lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
				require.Nil(t, err)
			}

			require.Nil(t, objectsB.FlushAndSwitch())

			var bm helpers.AllowList
			if !tc.nilBitmap {
				bm = allowlistWithExactMatchCount(t, tc.matchCount)
			}
			var disabled *runtime.DynamicValue[bool]
			if tc.disabled {
				disabled = runtime.NewDynamicValue[bool](true)
			}
			qp := NewQueryPlanner(store, newDataTypesHelper(dummyClass()), disabled)
			shouldUseInverted, err := qp.Do(ctx, bm, tc.limit, tc.sort)
			require.Nil(t, err)
			assert.Equal(t, tc.shouldChooseInverted, shouldUseInverted)
			plannerLog := fmt.Sprint(helpers.ExtractSlowQueryDetails(ctx)["sort_query_planner"])
			assert.Equal(t, !tc.unsupportedType, strings.Contains(plannerLog, "estimated costs"),
				"an unsupported type must skip the cost estimate: %s", plannerLog)
		})
	}
}

func allowlistWithExactMatchCount(t *testing.T, count int) helpers.AllowList {
	ids := make([]uint64, 0, count)
	for i := 0; i < count; i++ {
		ids = append(ids, uint64(i))
	}
	return helpers.NewAllowList(ids...)
}

func TestQueryPlannerSampledMatchPositions(t *testing.T) {
	const objectCount = 2000
	idsWhere := func(keep func(i int) bool) helpers.AllowList {
		var ids []uint64
		for i := 0; i < objectCount; i++ {
			if keep(i) {
				ids = append(ids, uint64(i))
			}
		}
		return helpers.NewAllowList(ids...)
	}
	// each set holds 4% of the objects, at the top or bottom of the index or
	// every 25th
	top := idsWhere(func(i int) bool { return i >= objectCount-80 })
	bottom := idsWhere(func(i int) bool { return i < 80 })
	spread := idsWhere(func(i int) bool { return i%25 == 0 })
	newestPerRow := idsWhere(func(i int) bool { return i%100 >= 96 })
	topThird := idsWhere(func(i int) bool { return i >= objectCount*2/3 })

	tests := []struct {
		name                 string
		ids                  helpers.AllowList
		idsPerRow            int
		sort                 []filters.Sort
		shouldChooseInverted bool
		skipsSampling        bool
	}{
		{
			name: "matches spread through the index", ids: spread, idsPerRow: 1,
			sort: sortsBy("int", "asc"), shouldChooseInverted: true,
		},
		{
			// the match ratio alone predicts a 250-entry walk
			name: "asc, matches at the far end of the walk", ids: top, idsPerRow: 1,
			sort: sortsBy("int", "asc"), shouldChooseInverted: false,
		},
		{
			name: "desc, matches at the far end of the walk", ids: bottom, idsPerRow: 1,
			sort: sortsBy("int", "desc"), shouldChooseInverted: false,
		},
		{
			name: "desc, matches at the start of the walk", ids: top, idsPerRow: 1,
			sort: sortsBy("int", "desc"), shouldChooseInverted: true,
		},
		{
			name: "multiple sort args, matches at the far end of the walk", ids: top, idsPerRow: 1,
			sort: sortsBy("int", "asc", "text", "asc"), shouldChooseInverted: false,
		},
		{
			// a row's oldest IDs miss, so only a whole-row check sees its matches
			name: "matches only among the newest IDs of each row", ids: newestPerRow, idsPerRow: 100,
			sort: sortsBy("int", "asc"), shouldChooseInverted: true,
		},
		{
			// a walk over the whole index still beats reading a third of the objects
			name: "asc, many matches at the far end of the walk", ids: topThird, idsPerRow: 1,
			sort: sortsBy("int", "asc"), shouldChooseInverted: true, skipsSampling: true,
		},
	}

	stores := map[int]*lsmkv.Store{}
	var s *lsmSorter
	for _, idsPerRow := range []int{1, 100} {
		var store *lsmkv.Store
		s, store = newCandidateTestSorter(t)
		for _, r := range sortRows(objectCount, groupedRow(idsPerRow)) {
			putSortRow(t, store, r)
		}
		flushSortBuckets(t, store)
		stores[idsPerRow] = store
	}

	for _, tc := range tests {
		allowLists := map[string]helpers.AllowList{
			"bitmap allow list": tc.ids,
			"other allow list":  tc.ids.WrapOnWrite(),
		}
		for alName, ids := range allowLists {
			t.Run(tc.name+", "+alName, func(t *testing.T) {
				ctx := helpers.InitSlowQueryDetails(context.Background())
				qp := NewQueryPlanner(stores[tc.idsPerRow], s.dataTypesHelper, nil)
				shouldUseInverted, err := qp.Do(ctx, ids, 10, tc.sort)
				require.NoError(t, err)
				assert.Equal(t, tc.shouldChooseInverted, shouldUseInverted)
				plannerLog := fmt.Sprint(helpers.ExtractSlowQueryDetails(ctx)["sort_query_planner"])
				assert.Equal(t, tc.skipsSampling, strings.Contains(plannerLog, "skipping sampling"), plannerLog)
			})
		}
	}

	t.Run("empty samples too few to trust", func(t *testing.T) {
		bucket := stores[1].Bucket(helpers.BucketFromPropNameLSM("int"))
		trusted := emptyWalkPrefix(context.Background(), bucket, top, 0.04, objectCount, false)
		assert.Greater(t, trusted, float64(objectCount)*0.9)
		// at this ratio ~1000 empty samples are expected to hold ~1 match
		assert.Zero(t, emptyWalkPrefix(context.Background(), bucket, top, 0.001, objectCount, false))
	})
}

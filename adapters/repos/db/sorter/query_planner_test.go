package sorter

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestQueryPlanner(t *testing.T) {
	type testCase struct {
		name                 string
		objectCount          int
		matchCount           int
		limit                int
		sort                 []filters.Sort
		shouldChooseInverted bool
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
			name:        "more than one sort arg – not supported yet",
			objectCount: 1000,
			matchCount:  800,
			limit:       100,
			sort: []filters.Sort{
				{
					Path:  []string{"int"},
					Order: "desc",
				},
				{
					Path:  []string{"number"},
					Order: "asc",
				},
			},
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
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				dirName   = t.TempDir()
				logger, _ = test.NewNullLogger()
				ctx       = context.Background()
			)
			store, err := lsmkv.New(dirName, dirName, logger, nil,
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop())
			require.Nil(t, err)
			defer store.Shutdown(ctx)

			err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM)
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

			bm := allowlistWithExactMatchCount(t, tc.matchCount)
			qp := NewQueryPlanner(store, newDataTypesHelper(dummyClass()))
			shouldUseInverted, err := qp.Do(ctx, bm, tc.limit, tc.sort)
			require.Nil(t, err)
			assert.Equal(t, tc.shouldChooseInverted, shouldUseInverted)
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

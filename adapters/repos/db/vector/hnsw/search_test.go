//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// prevents a regression of
// https://github.com/weaviate/weaviate/issues/2155
func TestNilCheckOnPartiallyCleanedNode(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{
		{100, 100}, // first to import makes this the EP, it is far from any query which means it will be replaced.
		{2, 2},     // a good potential entrypoint, but we will corrupt it later on
		{1, 1},     // the perfect search result
	}

	var vectorIndex *hnsw

	t.Run("import", func(*testing.T) {
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "bug-2155",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, ent.UserConfig{
			MaxConnections: 30,
			EFConstruction: 128,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)
		vectorIndex = index
	})

	t.Run("manually add the nodes", func(t *testing.T) {
		vectorIndex.entryPointID = 0
		vectorIndex.currentMaximumLayer = 1
		vectorIndex.nodes = []*vertex{
			{
				// must be on a non-zero layer for this bug to occur
				level: 1,
				connections: [][]uint64{
					{1, 2},
					{1},
				},
			},
			nil, // corrupt node
			{
				level: 0,
				connections: [][]uint64{
					{0, 1, 2},
				},
			},
		}
	})

	t.Run("run a search that would typically find the new ep", func(t *testing.T) {
		res, _, err := vectorIndex.SearchByVector(ctx, []float32{1.7, 1.7}, 20, nil)
		require.Nil(t, err)
		assert.Equal(t, []uint64{2, 0}, res, "right results are found")
	})

	t.Run("the corrupt node is now marked deleted", func(t *testing.T) {
		_, ok := vectorIndex.tombstones[1]
		assert.True(t, ok)
	})
}

func TestQueryVectorDistancer(t *testing.T) {
	vectors := [][]float32{
		{100, 100}, // first to import makes this the EP, it is far from any query which means it will be replaced.
		{2, 2},     // a good potential entrypoint, but we will corrupt it later on
		{1, 1},     // the perfect search result
	}

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "bug-2155",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 128,

		// The actual size does not matter for this test, but if it defaults to
		// zero it will constantly think it's full and needs to be deleted - even
		// after just being deleted, so make sure to use a positive number here.
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	index.Add(context.TODO(), uint64(0), []float32{-1, 0})

	dist := index.QueryVectorDistancer([]float32{0, 0})
	require.NotNil(t, dist)
	distance, err := dist.DistanceToNode(0)
	require.Nil(t, err)
	require.Equal(t, distance, float32(1.))

	// get distance for non-existing node above default cache size
	_, err = dist.DistanceToNode(1001)
	require.NotNil(t, err)
}

func TestAcornPercentage(t *testing.T) {
	vectors, _ := testinghelpers.RandomVecs(10, 1, 3)
	var vectorIndex *hnsw

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	t.Run("import test vectors", func(t *testing.T) {
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "delete-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			TempVectorForIDThunk: TempVectorForIDThunk(vectors),
		}, ent.UserConfig{
			MaxConnections:        16,
			EFConstruction:        16,
			VectorCacheMaxObjects: 1000,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), store)
		require.Nil(t, err)
		vectorIndex = index

		for i, vec := range vectors {
			err := vectorIndex.Add(context.TODO(), uint64(i), vec)
			require.Nil(t, err)
		}
	})

	t.Run("check acorn params on different filter percentags", func(t *testing.T) {
		vectorIndex.acornSearch.Store(false)
		allowList := helpers.NewAllowList(1, 2, 3)
		useAcorn, M := vectorIndex.acornParams(allowList)
		assert.False(t, useAcorn)
		assert.Equal(t, 0, M)

		vectorIndex.acornSearch.Store(true)

		useAcorn, M = vectorIndex.acornParams(allowList)
		assert.True(t, useAcorn)
		assert.Equal(t, 3, M)

		vectorIndex.acornSearch.Store(true)

		largerAllowList := helpers.NewAllowList(1, 2, 3, 4, 5)
		useAcorn, M = vectorIndex.acornParams(largerAllowList)
		// should be false as allow list percentage is 50%
		assert.False(t, useAcorn)
		assert.Equal(t, 2, M)
	})
}

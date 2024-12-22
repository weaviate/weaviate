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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/liutizhong/weaviate/adapters/repos/db/vector/cache"
	"github.com/liutizhong/weaviate/adapters/repos/db/vector/common"
	"github.com/liutizhong/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/liutizhong/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/liutizhong/weaviate/entities/cyclemanager"
	ent "github.com/liutizhong/weaviate/entities/vectorindex/hnsw"
)

func TestHnswIndex(t *testing.T) {
	ctx := context.Background()
	index := createEmptyHnswIndexForTests(t, testVectorForID)

	for i, vec := range testVectors {
		err := index.Add(ctx, uint64(i), vec)
		require.Nil(t, err)
	}

	t.Run("searching within cluster 1", func(t *testing.T) {
		position := 0
		res, _, err := index.knnSearchByVector(ctx, testVectors[position], 3, 36, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []uint64{0, 1, 2}, res)
	})

	t.Run("searching within cluster 2", func(t *testing.T) {
		position := 3
		res, _, err := index.knnSearchByVector(ctx, testVectors[position], 3, 36, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []uint64{3, 4, 5}, res)
	})

	t.Run("searching within cluster 3", func(t *testing.T) {
		position := 6
		res, _, err := index.knnSearchByVector(ctx, testVectors[position], 3, 36, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []uint64{6, 7, 8}, res)
	})

	t.Run("searching within cluster 2 with a scope larger than the cluster", func(t *testing.T) {
		position := 3
		res, _, err := index.knnSearchByVector(ctx, testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, []uint64{
			3, 5, 4, // cluster 2
			7, 8, 6, // cluster 3
			2, 1, 0, // cluster 1
		}, res)
	})

	t.Run("searching with negative value of k", func(t *testing.T) {
		position := 0
		_, _, err := index.knnSearchByVector(ctx, testVectors[position], -1, 36, nil)
		require.Error(t, err)
	})
}

func TestHnswIndexGrow(t *testing.T) {
	ctx := context.Background()
	vector := []float32{0.1, 0.2}
	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return vector, nil
	}
	index := createEmptyHnswIndexForTests(t, vecForIDFn)

	t.Run("should grow initial empty index", func(t *testing.T) {
		// when we invoke Add method suggesting a size bigger then the default
		// initial size, then if we don't grow an index at initial state
		// we get: panic: runtime error: index out of range [25001] with length 25000
		// in order to avoid this, insertInitialElement method is now able
		// to grow it's size at initial state
		err := index.Add(ctx, uint64(cache.InitialSize+1), vector)
		require.Nil(t, err)
	})

	t.Run("should grow index without panic", func(t *testing.T) {
		// This test shows that we had an edge case that was not covered
		// in growIndexToAccomodateNode method which was leading to panic:
		// panic: runtime error: index out of range [170001] with length 170001
		vector := []float32{0.11, 0.22}
		id := uint64(5*cache.InitialSize + 1)
		err := index.Add(ctx, id, vector)
		require.Nil(t, err)
		// index should grow to 5001
		assert.Equal(t, int(id)+cache.MinimumIndexGrowthDelta, len(index.nodes))
		assert.Equal(t, int32(id+2*cache.MinimumIndexGrowthDelta), index.cache.Len())
		// try to add a vector with id: 8001
		id = uint64(6*cache.InitialSize + cache.MinimumIndexGrowthDelta + 1)
		err = index.Add(ctx, id, vector)
		require.Nil(t, err)
		// index should grow to at least 8001
		assert.GreaterOrEqual(t, len(index.nodes), 8001)
		assert.GreaterOrEqual(t, index.cache.Len(), int32(8001))
	})

	t.Run("should grow index", func(t *testing.T) {
		// should not increase the nodes size
		sizeBefore := len(index.nodes)
		cacheBefore := index.cache.Len()
		idDontGrowIndex := uint64(6*cache.InitialSize - 1)
		err := index.Add(ctx, idDontGrowIndex, vector)
		require.Nil(t, err)
		assert.Equal(t, sizeBefore, len(index.nodes))
		assert.Equal(t, cacheBefore, index.cache.Len())
		// should increase nodes
		id := uint64(8*cache.InitialSize + 1)
		err = index.Add(ctx, id, vector)
		require.Nil(t, err)
		assert.GreaterOrEqual(t, len(index.nodes), int(id))
		assert.GreaterOrEqual(t, index.cache.Len(), int32(id))
		// should increase nodes when a much greater id is passed
		id = uint64(20*cache.InitialSize + 22)
		err = index.Add(ctx, id, vector)
		require.Nil(t, err)
		assert.Equal(t, int(id)+cache.MinimumIndexGrowthDelta, len(index.nodes))
		assert.Equal(t, int32(id+2*cache.MinimumIndexGrowthDelta), index.cache.Len())
	})
}

func TestHnswIndexGrowSafely(t *testing.T) {
	vector := []float32{0.1, 0.2}
	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return vector, nil
	}
	index := createEmptyHnswIndexForTests(t, vecForIDFn)

	t.Run("concurrently add nodes to grow index", func(t *testing.T) {
		growAttempts := 20
		var wg sync.WaitGroup
		offset := uint64(len(index.nodes))
		ctx := context.Background()

		addVectorPair := func(ids []uint64) {
			defer wg.Done()
			err := index.AddBatch(ctx, ids, [][]float32{vector, vector})
			require.Nil(t, err)
		}

		for i := 0; i < growAttempts; i++ {
			wg.Add(4)
			go addVectorPair([]uint64{offset - 4, offset - 5})
			go addVectorPair([]uint64{offset - 3, offset})
			go addVectorPair([]uint64{offset - 2, offset + 2})
			go addVectorPair([]uint64{offset - 1, offset + 3})
			wg.Wait()
			offset = uint64(len(index.nodes))
		}

		// Calculate non-nil nodes
		nonNilNodes := 0
		for _, node := range index.nodes {
			if node != nil {
				nonNilNodes++
			}
		}

		assert.Equal(t, growAttempts*8, nonNilNodes)
	})
}

func createEmptyHnswIndexForTests(t testing.TB, vecForIDFn common.VectorForID[float32]) *hnsw {
	// mock out commit logger before adding data so we don't leave a disk
	// footprint. Commit logging and deserializing from a (condensed) commit log
	// is tested in a separate integration test that takes care of providing and
	// cleaning up the correct place on disk to write test files
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForIDFn,
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	return index
}

func TestHnswIndexContainsNode(t *testing.T) {
	ctx := context.Background()
	t.Run("should return false if index is empty", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
			t.Fatalf("vecForID should not be called on empty index")
			return nil, nil
		}
		index := createEmptyHnswIndexForTests(t, vecForIDFn)
		require.False(t, index.ContainsNode(1))
	})

	t.Run("should return true if node is in the index", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
			return testVectors[id], nil
		}
		index := createEmptyHnswIndexForTests(t, vecForIDFn)
		for i, vec := range testVectors {
			err := index.Add(ctx, uint64(i), vec)
			require.Nil(t, err)
		}
		require.True(t, index.ContainsNode(5))
	})

	t.Run("should return false if node is not in the index", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
			return testVectors[id], nil
		}
		index := createEmptyHnswIndexForTests(t, vecForIDFn)
		for i, vec := range testVectors {
			err := index.Add(ctx, uint64(i), vec)
			require.Nil(t, err)
		}
		require.False(t, index.ContainsNode(100))
	})

	t.Run("should return false if node is deleted", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
			return testVectors[id], nil
		}
		index := createEmptyHnswIndexForTests(t, vecForIDFn)
		for i, vec := range testVectors {
			err := index.Add(ctx, uint64(i), vec)
			require.Nil(t, err)
		}
		err := index.Delete(5)
		require.Nil(t, err)
		require.False(t, index.ContainsNode(5))
	})
}

func TestHnswIndexIterate(t *testing.T) {
	ctx := context.Background()
	t.Run("should not run callback on empty index", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
			t.Fatalf("vecForID should not be called on empty index")
			return nil, nil
		}
		index := createEmptyHnswIndexForTests(t, vecForIDFn)
		index.Iterate(func(id uint64) bool {
			t.Fatalf("callback should not be called on empty index")
			return true
		})
	})

	t.Run("should iterate over all nodes", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
			return testVectors[id], nil
		}
		index := createEmptyHnswIndexForTests(t, vecForIDFn)
		for i, vec := range testVectors {
			err := index.Add(ctx, uint64(i), vec)
			require.Nil(t, err)
		}

		visited := make([]bool, len(testVectors))
		index.Iterate(func(id uint64) bool {
			visited[id] = true
			return true
		})
		for i, v := range visited {
			assert.True(t, v, "node %d was not visited", i)
		}
	})

	t.Run("should stop iteration when callback returns false", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
			return testVectors[id], nil
		}
		index := createEmptyHnswIndexForTests(t, vecForIDFn)
		for i, vec := range testVectors {
			err := index.Add(ctx, uint64(i), vec)
			require.Nil(t, err)
		}

		visited := make([]bool, len(testVectors))
		index.Iterate(func(id uint64) bool {
			visited[id] = true
			return id < 5
		})
		for i, v := range visited {
			if i <= 5 {
				assert.True(t, v, "node %d was not visited", i)
			} else {
				assert.False(t, v, "node %d was visited", i)
			}
		}
	})

	t.Run("should stop iteration when shutdownCtx is canceled", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
			return testVectors[id], nil
		}
		index := createEmptyHnswIndexForTests(t, vecForIDFn)
		for i, vec := range testVectors {
			err := index.Add(ctx, uint64(i), vec)
			require.Nil(t, err)
		}

		visited := make([]bool, len(testVectors))
		index.Iterate(func(id uint64) bool {
			visited[id] = true
			if id == 5 {
				err := index.Shutdown(context.Background())
				require.NoError(t, err)
			}
			return true
		})
		for i, v := range visited {
			if i <= 5 {
				assert.True(t, v, "node %d was not visited", i)
			} else {
				assert.False(t, v, "node %d was visited", i)
			}
		}
	})

	t.Run("should stop iteration when resetCtx is canceled", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
			return testVectors[id], nil
		}
		index := createEmptyHnswIndexForTests(t, vecForIDFn)
		for i, vec := range testVectors {
			err := index.Add(ctx, uint64(i), vec)
			require.Nil(t, err)
		}

		visited := make([]bool, len(testVectors))
		index.Iterate(func(id uint64) bool {
			visited[id] = true
			if id == 5 {
				index.resetCtxCancel()
			}
			return true
		})
		for i, v := range visited {
			if i <= 5 {
				assert.True(t, v, "node %d was not visited", i)
			} else {
				assert.False(t, v, "node %d was visited", i)
			}
		}
	})

	t.Run("should skip deleted nodes", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
			return testVectors[id], nil
		}
		index := createEmptyHnswIndexForTests(t, vecForIDFn)
		for i, vec := range testVectors {
			err := index.Add(ctx, uint64(i), vec)
			require.Nil(t, err)
		}

		err := index.Delete(uint64(5))
		require.Nil(t, err)

		visited := make([]bool, len(testVectors))
		index.Iterate(func(id uint64) bool {
			visited[id] = true
			return true
		})
		for i, v := range visited {
			if i == 5 {
				assert.False(t, v, "node %d was visited", i)
			} else {
				assert.True(t, v, "node %d was not visited", i)
			}
		}
	})
}

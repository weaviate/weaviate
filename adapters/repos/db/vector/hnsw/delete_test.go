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
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TempVectorForIDThunk(vectors [][]float32) func(context.Context, uint64, *common.VectorSlice) ([]float32, error) {
	return func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
		copy(container.Slice, vectors[int(id)])
		return vectors[int(id)], nil
	}
}

func TestDelete_WithoutCleaningUpTombstones(t *testing.T) {
	vectors := vectorsForDeleteTest()
	var vectorIndex *hnsw

	store := testinghelpers.NewDummyStore(t)
	t.Run("import the test vectors", func(t *testing.T) {
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
			MaxConnections: 30,
			EFConstruction: 128,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), store)
		require.Nil(t, err)
		vectorIndex = index

		for i, vec := range vectors {
			err := vectorIndex.Add(uint64(i), vec)
			require.Nil(t, err)
		}
	})

	var control []uint64

	t.Run("vectors are cached correctly", func(t *testing.T) {
		assert.Equal(t, len(vectors), int(vectorIndex.cache.CountVectors()))
	})

	t.Run("doing a control search before delete with the respective allow list", func(t *testing.T) {
		allowList := helpers.NewAllowList()
		for i := range vectors {
			if i%2 == 0 {
				continue
			}

			allowList.Insert(uint64(i))
		}

		res, _, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, allowList)
		require.Nil(t, err)
		require.True(t, len(res) > 0)
		control = res
	})

	t.Run("deleting every even element", func(t *testing.T) {
		for i := range vectors {
			if i%2 != 0 {
				continue
			}

			err := vectorIndex.Delete(uint64(i))
			require.Nil(t, err)
		}
	})

	t.Run("vector cache holds half the original vectors", func(t *testing.T) {
		vectorIndex.CleanUpTombstonedNodes(neverStop)
		assert.Equal(t, len(vectors)/2, int(vectorIndex.cache.CountVectors()))
	})

	t.Run("start a search that should only contain the remaining elements", func(t *testing.T) {
		res, _, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, nil)
		require.Nil(t, err)
		require.True(t, len(res) > 0)

		for _, elem := range res {
			if elem%2 == 0 {
				t.Errorf("search result contained an even element: %d", elem)
			}
		}

		assert.Equal(t, control, res)
	})

	t.Run("destroy the index", func(t *testing.T) {
		require.Nil(t, vectorIndex.Drop(context.Background()))
	})

	t.Run("vector cache holds no vectors", func(t *testing.T) {
		assert.Equal(t, 0, int(vectorIndex.cache.CountVectors()))
	})
}

func TestDelete_WithCleaningUpTombstonesOnce(t *testing.T) {
	// there is a single bulk clean event after all the deletes
	vectors := vectorsForDeleteTest()
	var vectorIndex *hnsw

	store := testinghelpers.NewDummyStore(t)

	t.Run("import the test vectors", func(t *testing.T) {
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
			MaxConnections: 30,
			EFConstruction: 128,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), store)
		require.Nil(t, err)
		vectorIndex = index

		for i, vec := range vectors {
			err := vectorIndex.Add(uint64(i), vec)
			require.Nil(t, err)
		}
	})

	var control []uint64
	var bfControl []uint64

	t.Run("doing a control search before delete with the respective allow list", func(t *testing.T) {
		allowList := helpers.NewAllowList()
		for i := range vectors {
			if i%2 == 0 {
				continue
			}

			allowList.Insert(uint64(i))
		}

		res, _, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, allowList)
		require.Nil(t, err)
		require.True(t, len(res) > 0)
		require.Len(t, res, 20)
		control = res
	})

	t.Run("brute force control", func(t *testing.T) {
		bf := bruteForceCosine(vectors, []float32{0.1, 0.1, 0.1}, 100)
		bfControl = make([]uint64, len(bf))
		i := 0
		for _, elem := range bf {
			if elem%2 == 0 {
				continue
			}

			bfControl[i] = elem
			i++
		}

		if i > 20 {
			i = 20
		}

		bfControl = bfControl[:i]
		assert.Equal(t, bfControl, control, "control should match bf control")
	})

	fmt.Printf("entrypoint before %d\n", vectorIndex.entryPointID)
	t.Run("deleting every even element", func(t *testing.T) {
		for i := range vectors {
			if i%2 != 0 {
				continue
			}

			err := vectorIndex.Delete(uint64(i))
			require.Nil(t, err)
		}
	})

	t.Run("running the cleanup", func(t *testing.T) {
		err := vectorIndex.CleanUpTombstonedNodes(neverStop)
		require.Nil(t, err)
	})

	t.Run("start a search that should only contain the remaining elements", func(t *testing.T) {
		res, _, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, nil)
		require.Nil(t, err)
		require.True(t, len(res) > 0)

		for _, elem := range res {
			if elem%2 == 0 {
				t.Errorf("search result contained an even element: %d", elem)
			}
		}

		assert.Equal(t, control, res)
	})

	t.Run("verify the graph no longer has any tombstones", func(t *testing.T) {
		assert.Len(t, vectorIndex.tombstones, 0)
	})

	t.Run("destroy the index", func(t *testing.T) {
		require.Nil(t, vectorIndex.Drop(context.Background()))
	})
}

func TestDelete_WithCleaningUpTombstonesInBetween(t *testing.T) {
	// there is a single bulk clean event after all the deletes
	vectors := vectorsForDeleteTest()
	var vectorIndex *hnsw
	store := testinghelpers.NewDummyStore(t)

	t.Run("import the test vectors", func(t *testing.T) {
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
			MaxConnections: 30,
			EFConstruction: 128,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), store)
		// makes sure index is build only with level 0. To be removed after fixing WEAVIATE-179
		index.randFunc = func() float64 { return 0.1 }

		require.Nil(t, err)
		vectorIndex = index

		for i, vec := range vectors {
			err := vectorIndex.Add(uint64(i), vec)
			require.Nil(t, err)
		}
	})

	var control []uint64

	t.Run("doing a control search before delete with the respective allow list", func(t *testing.T) {
		allowList := helpers.NewAllowList()
		for i := range vectors {
			if i%2 == 0 {
				continue
			}

			allowList.Insert(uint64(i))
		}

		res, _, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, allowList)
		require.Nil(t, err)
		require.True(t, len(res) > 0)

		control = res
	})

	t.Run("deleting every even element", func(t *testing.T) {
		for i := range vectors {
			if i%10 == 0 {
				// occasionally run clean up
				err := vectorIndex.CleanUpTombstonedNodes(neverStop)
				require.Nil(t, err)
			}

			if i%2 != 0 {
				continue
			}

			err := vectorIndex.Delete(uint64(i))
			require.Nil(t, err)
		}

		// finally run one final cleanup
		err := vectorIndex.CleanUpTombstonedNodes(neverStop)
		require.Nil(t, err)
	})

	t.Run("start a search that should only contain the remaining elements", func(t *testing.T) {
		res, _, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, nil)
		require.Nil(t, err)
		require.True(t, len(res) > 0)

		for _, elem := range res {
			if elem%2 == 0 {
				t.Errorf("search result contained an even element: %d", elem)
			}
		}

		assert.Equal(t, control, res)
	})

	t.Run("verify the graph no longer has any tombstones", func(t *testing.T) {
		assert.Len(t, vectorIndex.tombstones, 0)
	})

	t.Run("delete the remaining elements", func(t *testing.T) {
		for i := range vectors {
			if i%2 == 0 {
				continue
			}

			err := vectorIndex.Delete(uint64(i))
			require.Nil(t, err)
		}

		err := vectorIndex.CleanUpTombstonedNodes(neverStop)
		require.Nil(t, err)
	})

	t.Run("try to insert again and search", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			err := vectorIndex.Add(uint64(i), vectors[i])
			require.Nil(t, err)
		}

		res, _, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []uint64{0, 1, 2, 3, 4}, res)
	})

	t.Run("destroy the index", func(t *testing.T) {
		require.Nil(t, vectorIndex.Drop(context.Background()))
	})
}

func createIndexImportAllVectorsAndDeleteEven(t *testing.T, vectors [][]float32, store *lsmkv.Store) (index *hnsw, remainingResult []uint64) {
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
		MaxConnections: 30,
		EFConstruction: 128,

		// The actual size does not matter for this test, but if it defaults to
		// zero it will constantly think it's full and needs to be deleted - even
		// after just being deleted, so make sure to use a positive number here.
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), store)
	require.Nil(t, err)

	// makes sure index is build only with level 0. To be removed after fixing WEAVIATE-179
	index.randFunc = func() float64 { return 0.1 }

	// to speed up test execution, size of nodes array is decreased
	// from default 25k to little over number of vectors
	index.nodes = make([]*vertex, int(1.2*float64(len(vectors))))

	for i, vec := range vectors {
		err := index.Add(uint64(i), vec)
		require.Nil(t, err)
	}

	for i := range vectors {
		if i%2 != 0 {
			continue
		}
		err := index.Delete(uint64(i))
		require.Nil(t, err)
	}

	res, _, err := index.SearchByVector([]float32{0.1, 0.1, 0.1}, len(vectors), nil)
	require.Nil(t, err)
	require.True(t, len(res) > 0)

	for _, elem := range res {
		if elem%2 == 0 {
			t.Errorf("search result contained an even element: %d", elem)
		}
	}

	return index, res
}

func genStopAtFunc(i int) func() bool {
	counter := 0
	return func() bool {
		if counter < i {
			counter++
			return false
		}
		return true
	}
}

func TestDelete_WithCleaningUpTombstonesStopped(t *testing.T) {
	vectors := vectorsForDeleteTest()
	var index *hnsw
	var possibleStopsCount int
	// due to not yet resolved bug (https://semi-technology.atlassian.net/browse/WEAVIATE-179)
	// db can return less vectors than are actually stored after tombstones cleanup
	// controlRemainingResult contains all odd vectors (before cleanup was performed)
	// controlRemainingResultAfterCleanup contains most of odd vectors (after cleanup was performed)
	//
	// this test verifies if partial cleanup will not change search output, therefore depending on
	// where cleanup method was stopped, subset of controlRemainingResult is expected, though all
	// vectors from controlRemainingResultAfterCleanup should be returned
	// TODO to be simplified after fixing WEAVIATE-179, all results should be the same
	var controlRemainingResult []uint64
	var controlRemainingResultAfterCleanup []uint64
	store := testinghelpers.NewDummyStore(t)

	t.Run("create control index", func(t *testing.T) {
		index, controlRemainingResult = createIndexImportAllVectorsAndDeleteEven(t, vectors, store)
	})

	t.Run("count all cleanup tombstones stops", func(t *testing.T) {
		counter := 0
		countingStopFunc := func() bool {
			counter++
			return false
		}

		err := index.CleanUpTombstonedNodes(countingStopFunc)
		require.Nil(t, err)

		possibleStopsCount = counter
	})

	t.Run("search remaining elements after cleanup", func(t *testing.T) {
		res, _, err := index.SearchByVector([]float32{0.1, 0.1, 0.1}, len(vectors), nil)
		require.Nil(t, err)
		require.True(t, len(res) > 0)

		for _, elem := range res {
			if elem%2 == 0 {
				t.Errorf("search result contained an even element: %d", elem)
			}
		}
		controlRemainingResultAfterCleanup = res
	})

	t.Run("destroy the control index", func(t *testing.T) {
		require.Nil(t, index.Drop(context.Background()))
	})

	for i := 0; i < possibleStopsCount; i++ {
		index, _ = createIndexImportAllVectorsAndDeleteEven(t, vectors, store)

		t.Run("stop cleanup at place", func(t *testing.T) {
			require.Nil(t, index.CleanUpTombstonedNodes(genStopAtFunc(i)))
		})

		t.Run("search remaining elements after partial cleanup", func(t *testing.T) {
			res, _, err := index.SearchByVector([]float32{0.1, 0.1, 0.1}, len(vectors), nil)
			require.Nil(t, err)
			require.Subset(t, controlRemainingResult, res)
			require.Subset(t, res, controlRemainingResultAfterCleanup)
		})

		t.Run("run complete cleanup", func(t *testing.T) {
			require.Nil(t, index.CleanUpTombstonedNodes(neverStop))
		})

		t.Run("search remaining elements after complete cleanup", func(t *testing.T) {
			res, _, err := index.SearchByVector([]float32{0.1, 0.1, 0.1}, len(vectors), nil)
			require.Nil(t, err)
			require.Subset(t, controlRemainingResult, res)
			require.Subset(t, res, controlRemainingResultAfterCleanup)
		})

		t.Run("destroy the index", func(t *testing.T) {
			require.Nil(t, index.Drop(context.Background()))
		})
	}
}

func TestDelete_InCompressedIndex_WithCleaningUpTombstonesOnce(t *testing.T) {
	var (
		vectorIndex *hnsw
		// there is a single bulk clean event after all the deletes
		vectors    = vectorsForDeleteTest()
		rootPath   = t.TempDir()
		userConfig = ent.UserConfig{
			MaxConnections: 30,
			EFConstruction: 128,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 100000,
			PQ: ent.PQConfig{
				Enabled: true,
				Encoder: ent.PQEncoder{
					Type:         ent.PQEncoderTypeTile,
					Distribution: ent.PQEncoderDistributionNormal,
				},
			},
		}
	)
	store := testinghelpers.NewDummyStore(t)

	t.Run("import the test vectors", func(t *testing.T) {
		index, err := New(Config{
			RootPath:              rootPath,
			ID:                    "delete-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				if int(id) >= len(vectors) {
					return nil, storobj.NewErrNotFoundf(id, "out of range")
				}
				return vectors[int(id)], nil
			},
			TempVectorForIDThunk: TempVectorForIDThunk(vectors),
		}, userConfig, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), store)
		require.Nil(t, err)
		vectorIndex = index

		for i, vec := range vectors {
			err := vectorIndex.Add(uint64(i), vec)
			require.Nil(t, err)
		}
		cfg := ent.PQConfig{
			Enabled: true,
			Encoder: ent.PQEncoder{
				Type:         ent.PQEncoderTypeTile,
				Distribution: ent.PQEncoderDistributionLogNormal,
			},
			BitCompression: false,
			Segments:       3,
			Centroids:      256,
		}
		userConfig.PQ = cfg
		index.compress(userConfig)
	})

	var control []uint64
	var bfControl []uint64

	t.Run("doing a control search before delete with the respective allow list", func(t *testing.T) {
		allowList := helpers.NewAllowList()
		for i := range vectors {
			if i%2 == 0 {
				continue
			}

			allowList.Insert(uint64(i))
		}

		res, _, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, allowList)
		require.Nil(t, err)
		require.True(t, len(res) > 0)
		require.Len(t, res, 20)
		control = res
	})

	t.Run("brute force control", func(t *testing.T) {
		bf := bruteForceCosine(vectors, []float32{0.1, 0.1, 0.1}, 100)
		bfControl = make([]uint64, len(bf))
		i := 0
		for _, elem := range bf {
			if elem%2 == 0 {
				continue
			}

			bfControl[i] = elem
			i++
		}

		if i > 20 {
			i = 20
		}

		bfControl = bfControl[:i]
		recall := float32(testinghelpers.MatchesInLists(bfControl, control)) / float32(len(bfControl))
		fmt.Println(recall)
		assert.True(t, recall > 0.6, "control should match bf control")
	})

	fmt.Printf("entrypoint before %d\n", vectorIndex.entryPointID)
	t.Run("deleting every even element", func(t *testing.T) {
		for i := range vectors {
			if i%2 != 0 {
				continue
			}

			err := vectorIndex.Delete(uint64(i))
			require.Nil(t, err)
		}
	})

	t.Run("running the cleanup", func(t *testing.T) {
		err := vectorIndex.CleanUpTombstonedNodes(neverStop)
		require.Nil(t, err)
	})

	t.Run("start a search that should only contain the remaining elements", func(t *testing.T) {
		res, _, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, nil)
		require.Nil(t, err)
		require.True(t, len(res) > 0)

		for _, elem := range res {
			if elem%2 == 0 {
				t.Errorf("search result contained an even element: %d", elem)
			}
		}

		recall := float32(testinghelpers.MatchesInLists(res, control)) / float32(len(control))
		assert.True(t, recall > 0.6)
	})

	t.Run("verify the graph no longer has any tombstones", func(t *testing.T) {
		assert.Len(t, vectorIndex.tombstones, 0)
	})

	t.Run("destroy the index", func(t *testing.T) {
		require.Nil(t, vectorIndex.Drop(context.Background()))
	})
}

func TestDelete_InCompressedIndex_WithCleaningUpTombstonesOnce_DoesNotCrash(t *testing.T) {
	var (
		vectorIndex *hnsw
		// there is a single bulk clean event after all the deletes
		vectors    = vectorsForDeleteTest()
		rootPath   = t.TempDir()
		userConfig = ent.UserConfig{
			MaxConnections: 30,
			EFConstruction: 128,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 100000,
			PQ:                    ent.PQConfig{Enabled: true, Encoder: ent.PQEncoder{Type: "tile", Distribution: "normal"}},
		}
	)

	store := testinghelpers.NewDummyStore(t)

	t.Run("import the test vectors", func(t *testing.T) {
		index, err := New(Config{
			RootPath:              rootPath,
			ID:                    "delete-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id%uint64(len(vectors)))], nil
			},
			TempVectorForIDThunk: TempVectorForIDThunk(vectors),
		}, userConfig, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), store)
		require.Nil(t, err)
		vectorIndex = index

		for i, vec := range vectors {
			err := vectorIndex.Add(uint64(i), vec)
			require.Nil(t, err)
		}
		cfg := ent.PQConfig{
			Enabled: true,
			Encoder: ent.PQEncoder{
				Type:         ent.PQEncoderTypeTile,
				Distribution: ent.PQEncoderDistributionLogNormal,
			},
			BitCompression: false,
			Segments:       3,
			Centroids:      256,
		}
		userConfig.PQ = cfg
		index.compress(userConfig)
		for i := len(vectors); i < 1000; i++ {
			err := vectorIndex.Add(uint64(i), vectors[i%len(vectors)])
			require.Nil(t, err)
		}
	})

	t.Run("deleting every even element", func(t *testing.T) {
		for i := range vectors {
			if i%2 != 0 {
				continue
			}

			err := vectorIndex.Delete(uint64(i))
			require.Nil(t, err)
		}
	})

	t.Run("running the cleanup", func(t *testing.T) {
		err := vectorIndex.CleanUpTombstonedNodes(neverStop)
		require.Nil(t, err)
	})

	t.Run("verify the graph no longer has any tombstones", func(t *testing.T) {
		assert.Len(t, vectorIndex.tombstones, 0)
	})

	t.Run("destroy the index", func(t *testing.T) {
		require.Nil(t, vectorIndex.Drop(context.Background()))
	})
}

// we need a certain number of elements so that we can make sure that nodes
// from all layers will eventually be deleted, otherwise our test only tests
// edge cases which aren't very common in real life, but ignore the most common
// deletes
func vectorsForDeleteTest() [][]float32 {
	return [][]float32{
		{0.27335858, 0.42670676, 0.12599982},
		{0.34369454, 0.78510034, 0.78000546},
		{0.2342731, 0.076864816, 0.6405078},
		{0.07597838, 0.7752282, 0.87022865},
		{0.78632426, 0.06902865, 0.7423889},
		{0.3055758, 0.3901508, 0.9399572},
		{0.48687622, 0.26338226, 0.06495104},
		{0.5384028, 0.35410047, 0.8821815},
		{0.25123185, 0.62722564, 0.86443096},
		{0.58484185, 0.13103616, 0.4034975},
		{0.0019696166, 0.46822622, 0.42492124},
		{0.42401955, 0.8278863, 0.5952888},
		{0.15367928, 0.70778894, 0.0070928824},
		{0.95760256, 0.45898128, 0.1541115},
		{0.9125976, 0.9021616, 0.21607016},
		{0.9876307, 0.5243228, 0.37294936},
		{0.8194746, 0.56142205, 0.5130103},
		{0.805065, 0.62250346, 0.63715476},
		{0.9969276, 0.5115748, 0.18916714},
		{0.16419733, 0.15029702, 0.36020836},
		{0.9660323, 0.35887036, 0.6072966},
		{0.72765416, 0.27891788, 0.9094314},
		{0.8626208, 0.3540126, 0.3100354},
		{0.7153876, 0.17094712, 0.7801294},
		{0.23180388, 0.107446484, 0.69542855},
		{0.54731685, 0.8949827, 0.68316746},
		{0.15049729, 0.1293767, 0.0574729},
		{0.89379513, 0.67022973, 0.57360715},
		{0.725353, 0.25326362, 0.44264215},
		{0.2568602, 0.4986094, 0.9759933},
		{0.7300015, 0.70019704, 0.49546525},
		{0.54314494, 0.2004176, 0.63803226},
		{0.6180191, 0.5260845, 0.9373999},
		{0.63356537, 0.81430644, 0.78373694},
		{0.69995105, 0.84198904, 0.17851257},
		{0.5197941, 0.11502675, 0.95129955},
		{0.15791401, 0.07516741, 0.113447875},
		{0.06811827, 0.4450082, 0.98595786},
		{0.7153448, 0.41833848, 0.06332495},
		{0.6704102, 0.28931814, 0.031580303},
		{0.47773632, 0.73334247, 0.6925025},
		{0.7976896, 0.9499536, 0.6394833},
		{0.3074854, 0.14025249, 0.35961738},
		{0.49956197, 0.093575336, 0.790093},
		{0.4641653, 0.21276893, 0.528895},
		{0.1021849, 0.9416305, 0.46738508},
		{0.3790398, 0.50099677, 0.98233247},
		{0.39650732, 0.020929832, 0.53968865},
		{0.77604437, 0.8554197, 0.24056046},
		{0.07174444, 0.28758526, 0.67587185},
		{0.22292718, 0.66624546, 0.6077909},
		{0.22090498, 0.36197436, 0.40415043},
		{0.04838009, 0.120789215, 0.17928012},
		{0.55166364, 0.3400502, 0.43698996},
		{0.7638108, 0.47014108, 0.23208627},
		{0.9239513, 0.8418566, 0.23518613},
		{0.289589, 0.85010827, 0.055741556},
		{0.32436147, 0.18756394, 0.4217864},
		{0.041671168, 0.37824047, 0.66486764},
		{0.5052222, 0.07982704, 0.64345413},
		{0.62675995, 0.20138603, 0.8231867},
		{0.86306876, 0.9698708, 0.11398846},
		{0.68566775, 0.22026269, 0.13525572},
		{0.57706076, 0.32325208, 0.6122228},
		{0.80035216, 0.18560356, 0.6328281},
		{0.87145543, 0.19380389, 0.8863942},
		{0.33777508, 0.6056442, 0.9110077},
		{0.3961719, 0.49714503, 0.14191929},
		{0.5344362, 0.8166916, 0.75880384},
		{0.015749464, 0.63223976, 0.5470922},
		{0.10512444, 0.2212036, 0.24995685},
		{0.10831311, 0.27044898, 0.8668174},
		{0.3272971, 0.6659298, 0.87119603},
		{0.42913893, 0.14528985, 0.69957525},
		{0.33012474, 0.81964344, 0.092787445},
		{0.093618214, 0.90637344, 0.94406706},
		{0.12161567, 0.75131124, 0.40563175},
		{0.9154454, 0.75925833, 0.8406739},
		{0.81649286, 0.9025715, 0.3105051},
		{0.2927649, 0.22649862, 0.9708593},
		{0.30813727, 0.0079439245, 0.39662006},
		{0.94943213, 0.36778906, 0.217876},
		{0.716794, 0.3811725, 0.18448676},
		{0.66879725, 0.29722908, 0.0031202603},
		{0.11104216, 0.13094379, 0.0787222},
		{0.8508966, 0.86416596, 0.15885831},
		{0.2303136, 0.56660503, 0.17114973},
		{0.8632685, 0.4229249, 0.1936724},
		{0.03060897, 0.35226125, 0.8115969},
	}
}

func TestDelete_EntrypointIssues(t *testing.T) {
	// This test is motivated by flakyness of other tests. We seemed to have
	// experienced a failure with the following structure
	//
	// Entrypoint: 6
	// Max Level: 1
	// Tombstones map[]

	// Nodes and Connections:
	// Node 0
	// Level 0: Connections: [1 2 3 4 5 6 7 8]
	// Node 1
	// Level 0: Connections: [0 2 3 4 5 6 7 8]
	// Node 2
	// Level 0: Connections: [1 0 3 4 5 6 7 8]
	// Node 3
	// Level 0: Connections: [2 1 0 4 5 6 7 8]
	// Node 4
	// Level 0: Connections: [3 2 1 0 5 6 7 8]
	// Node 5
	// Level 0: Connections: [3 4 2 1 0 6 7 8]
	// Node 6
	// Level 0: Connections: [4 2 1 3 5 0 7 8]
	// Level 1: Connections: [7]
	// Node 7
	// Level 1: Connections: [6]
	// Level 0: Connections: [6 4 3 5 2 1 0 8]
	// Node 8
	// Level 0: Connections: [7 6 4 3 5 2 1 0]
	//
	// This test aims to rebuild this tree exactly (manually) and verifies that
	// deletion of the old entrypoint (element 6), works without issue
	//
	// The underlying test set can be found in vectors_for_test.go

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "delete-entrypoint-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
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

	// manually build the index
	index.entryPointID = 6
	index.currentMaximumLayer = 1
	index.nodes = make([]*vertex, 50)
	index.nodes[0] = &vertex{
		id: 0,
		connections: [][]uint64{
			{1, 2, 3, 4, 5, 6, 7, 8},
		},
	}
	index.nodes[1] = &vertex{
		id: 1,
		connections: [][]uint64{
			{0, 2, 3, 4, 5, 6, 7, 8},
		},
	}
	index.nodes[2] = &vertex{
		id: 2,
		connections: [][]uint64{
			{1, 0, 3, 4, 5, 6, 7, 8},
		},
	}
	index.nodes[3] = &vertex{
		id: 3,
		connections: [][]uint64{
			{2, 1, 0, 4, 5, 6, 7, 8},
		},
	}
	index.nodes[4] = &vertex{
		id: 4,
		connections: [][]uint64{
			{3, 2, 1, 0, 5, 6, 7, 8},
		},
	}
	index.nodes[5] = &vertex{
		id: 5,
		connections: [][]uint64{
			{3, 4, 2, 1, 0, 6, 7, 8},
		},
	}
	index.nodes[6] = &vertex{
		id: 6,
		connections: [][]uint64{
			{4, 3, 1, 3, 5, 0, 7, 8},
			{7},
		},
		level: 1,
	}
	index.nodes[7] = &vertex{
		id: 7,
		connections: [][]uint64{
			{6, 4, 3, 5, 2, 1, 0, 8},
			{6},
		},
		level: 1,
	}
	index.nodes[8] = &vertex{
		id: 8,
		connections: [][]uint64{
			8: {7, 6, 4, 3, 5, 2, 1, 0},
		},
	}

	dumpIndex(index, "before delete")

	t.Run("delete some elements and permanently delete tombstoned elements",
		func(t *testing.T) {
			err := index.Delete(6)
			require.Nil(t, err)
			err = index.Delete(8)
			require.Nil(t, err)

			err = index.CleanUpTombstonedNodes(neverStop)
			require.Nil(t, err)
		})

	dumpIndex(index, "after delete")

	expectedResults := []uint64{
		3, 5, 4, // cluster 2
		7,       // cluster 3 with element 6 and 8 deleted
		2, 1, 0, // cluster 1
	}

	t.Run("verify that the results are correct", func(t *testing.T) {
		position := 3
		res, _, err := index.knnSearchByVector(testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, expectedResults, res)
	})

	// t.Fail()
	t.Run("destroy the index", func(t *testing.T) {
		require.Nil(t, index.Drop(context.Background()))
	})
}

func TestDelete_MoreEntrypointIssues(t *testing.T) {
	vectors := [][]float32{
		{7, 1},
		{8, 2},
		{23, 14},
		{6.5, -1},
	}

	vecForID := func(ctx context.Context, id uint64) ([]float32, error) {
		return vectors[int(id)], nil
	}
	// This test is motivated by flakyness of other tests. We seemed to have
	// experienced a failure with the following structure
	//
	// ID: thing_geoupdatetestclass_single_location
	// Entrypoint: 2
	// Max Level: 1
	// Tombstones map[0:{} 1:{}]
	//
	// Nodes and Connections:
	//   Node 0
	//     Level 0: Connections: [1]
	//   Node 1
	//     Level 0: Connections: [0 2]
	//     Level 1: Connections: [2]
	//   Node 2
	//     Level 1: Connections: [1]
	//     Level 0: Connections: [1]

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "more-delete-entrypoint-flakyness-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewGeoProvider(),
		VectorForIDThunk:      vecForID,
		TempVectorForIDThunk:  TempVectorForIDThunk(vectors),
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

	// manually build the index
	index.entryPointID = 2
	index.currentMaximumLayer = 1
	index.tombstones = map[uint64]struct{}{
		0: {},
		1: {},
	}
	index.nodes = make([]*vertex, 50)
	index.nodes[0] = &vertex{
		id: 0,
		connections: [][]uint64{
			0: {1},
		},
	}
	index.nodes[1] = &vertex{
		id: 1,
		connections: [][]uint64{
			0: {0, 2},
			1: {2},
		},
	}
	index.nodes[2] = &vertex{
		id: 2,
		connections: [][]uint64{
			0: {1},
			1: {1},
		},
	}

	dumpIndex(index, "before adding another element")
	t.Run("adding a third element", func(t *testing.T) {
		vec, _ := testVectorForID(context.TODO(), 3)
		index.Add(3, vec)
	})

	expectedResults := []uint64{
		3, 2,
	}

	t.Run("verify that the results are correct", func(t *testing.T) {
		position := 3
		res, _, err := index.knnSearchByVector(testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, expectedResults, res)
	})

	t.Run("destroy the index", func(t *testing.T) {
		require.Nil(t, index.Drop(context.Background()))
	})
}

func TestDelete_TombstonedEntrypoint(t *testing.T) {
	vecForID := func(ctx context.Context, id uint64) ([]float32, error) {
		// always return same vec  for all elements
		return []float32{0.1, 0.2}, nil
	}
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "tombstoned-entrypoint-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForID,
		TempVectorForIDThunk:  TempVectorForIDThunk([][]float32{{0.1, 0.2}}),
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 128,
		// explicitly turn off, so we only focus on the tombstoned periods
		CleanupIntervalSeconds: 0,

		// The actual size does not matter for this test, but if it defaults to
		// zero it will constantly think it's full and needs to be deleted - even
		// after just being deleted, so make sure to use a positive number here.
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	objVec := []float32{0.1, 0.2}
	searchVec := []float32{0.05, 0.05}

	require.Nil(t, index.Add(0, objVec))
	require.Nil(t, index.Delete(0))
	require.Nil(t, index.Add(1, objVec))

	res, _, err := index.SearchByVector(searchVec, 100, nil)
	require.Nil(t, err)
	assert.Equal(t, []uint64{1}, res, "should contain the only result")

	t.Run("destroy the index", func(t *testing.T) {
		require.Nil(t, index.Drop(context.Background()))
	})
}

func TestDelete_Flakyness_gh_1369(t *testing.T) {
	// parse a snapshot form a flaky test
	snapshotBefore := []byte(`{"labels":["ran a cleanup cycle"],"id":"delete-test","entrypoint":3,"currentMaximumLayer":3,"tombstones":{},"nodes":[{"id":1,"level":0,"connections":{"0":[11,25,33,3,29,32,5,19,30,7,17,27,21,31,36,34,35,23,15,9,13]}},{"id":3,"level":3,"connections":{"0":[1,29,11,5,25,33,19,32,7,17,30,21,35,31,27,36,23,34,9,15,13],"1":[29,36,13],"2":[29,36],"3":[36]}},{"id":5,"level":0,"connections":{"0":[29,19,7,32,35,21,1,31,3,33,23,25,11,17,36,27,30,9,15,34,13]}},{"id":7,"level":0,"connections":{"0":[32,19,21,31,5,35,23,29,33,36,17,1,9,27,25,30,11,3,15,13,34]}},{"id":9,"level":0,"connections":{"0":[36,23,31,21,15,17,27,7,32,35,30,13,19,33,5,25,29,11,1,34,3]}},{"id":11,"level":0,"connections":{"0":[25,33,1,30,17,3,27,32,34,29,19,7,5,36,15,21,31,23,9,13,35]}},{"id":13,"level":1,"connections":{"0":[15,27,34,36,30,17,9,33,25,31,23,21,11,32,7,1,19,35,5,29,3],"1":[36,29,3]}},{"id":15,"level":0,"connections":{"0":[13,27,36,17,30,9,34,33,31,23,25,21,32,11,7,1,19,35,5,29,3]}},{"id":17,"level":0,"connections":{"0":[27,30,36,33,15,32,25,31,9,11,21,7,23,1,34,13,19,5,29,35,3]}},{"id":19,"level":0,"connections":{"0":[5,7,32,29,35,21,31,23,1,33,17,3,25,36,11,27,9,30,15,34,13]}},{"id":21,"level":0,"connections":{"0":[31,23,7,35,32,19,9,36,5,17,27,33,29,30,15,1,25,11,3,13,34]}},{"id":23,"level":0,"connections":{"0":[31,21,9,35,7,36,32,19,17,5,27,33,15,29,30,25,1,13,11,3,34]}},{"id":25,"level":0,"connections":{"0":[11,33,1,30,17,27,32,3,34,29,7,19,36,5,15,21,31,23,9,13,35]}},{"id":27,"level":0,"connections":{"0":[17,30,36,15,33,25,13,9,34,32,11,31,21,7,23,1,19,5,29,35,3]}},{"id":29,"level":2,"connections":{"0":[5,19,32,7,3,1,33,35,21,25,31,11,23,17,30,36,27,9,15,34,13],"1":[3,36,13],"2":[3,36]}},{"id":30,"level":0,"connections":{"0":[27,17,33,25,15,36,11,34,32,1,13,9,31,7,21,23,19,29,5,3,35]}},{"id":31,"level":0,"connections":{"0":[21,23,7,32,35,9,36,19,17,5,27,33,29,30,15,25,1,11,13,3,34]}},{"id":32,"level":0,"connections":{"0":[7,19,21,31,5,33,29,17,23,1,35,36,25,27,30,11,9,3,15,34,13]}},{"id":33,"level":0,"connections":{"0":[25,11,1,17,30,32,27,7,19,36,29,5,21,31,3,34,15,23,9,35,13]}},{"id":34,"level":0,"connections":{"0":[30,27,15,13,25,17,11,33,36,1,32,9,31,7,21,3,23,19,29,5,35]}},{"id":35,"level":0,"connections":{"0":[21,7,31,23,19,5,32,29,9,36,17,33,1,27,25,30,3,11,15,13,34]}},{"id":36,"level":3,"connections":{"0":[17,9,27,15,31,23,21,30,32,7,33,13,25,19,35,11,34,1,5,29,3],"1":[13,29,3],"2":[29,3],"3":[3]}}]}
`)

	vectors := vectorsForDeleteTest()
	vecForID := func(ctx context.Context, id uint64) ([]float32, error) {
		return vectors[int(id)], nil
	}

	index, err := NewFromJSONDumpMap(snapshotBefore, vecForID)
	require.Nil(t, err)
	index.forbidFlat = true

	var control []uint64
	t.Run("control search before delete with the respective allow list", func(t *testing.T) {
		allowList := helpers.NewAllowList()
		for i := range vectors {
			if i%2 == 0 {
				continue
			}

			allowList.Insert(uint64(i))
		}

		res, _, err := index.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, allowList)
		require.Nil(t, err)
		require.True(t, len(res) > 0)

		control = res
	})

	t.Run("delete the remaining even entries", func(t *testing.T) {
		require.Nil(t, index.Delete(30))
		require.Nil(t, index.Delete(32))
		require.Nil(t, index.Delete(34))
		require.Nil(t, index.Delete(36))
	})

	t.Run("verify against control BEFORE Tombstone Cleanup", func(t *testing.T) {
		res, _, err := index.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, nil)
		require.Nil(t, err)
		require.True(t, len(res) > 0)
		assert.Equal(t, control, res)
	})

	t.Run("clean up tombstoned nodes", func(t *testing.T) {
		require.Nil(t, index.CleanUpTombstonedNodes(neverStop))
	})

	t.Run("verify against control AFTER Tombstone Cleanup", func(t *testing.T) {
		res, _, err := index.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, nil)
		require.Nil(t, err)
		require.True(t, len(res) > 0)
		assert.Equal(t, control, res)
	})

	t.Run("now delete the entrypoint", func(t *testing.T) {
		require.Nil(t, index.Delete(index.entryPointID))
	})

	t.Run("clean up tombstoned nodes", func(t *testing.T) {
		require.Nil(t, index.CleanUpTombstonedNodes(neverStop))
	})

	t.Run("now delete the entrypoint", func(t *testing.T) {
		// this verifies that our findNewLocalEntrypoint also works when the global
		// entrypoint is affected
		require.Nil(t, index.Delete(index.entryPointID))
	})

	t.Run("clean up tombstoned nodes", func(t *testing.T) {
		require.Nil(t, index.CleanUpTombstonedNodes(neverStop))
	})

	t.Run("destroy the index", func(t *testing.T) {
		require.Nil(t, index.Drop(context.Background()))
	})
}

func bruteForceCosine(vectors [][]float32, query []float32, k int) []uint64 {
	type distanceAndIndex struct {
		distance float32
		index    uint64
	}

	distances := make([]distanceAndIndex, len(vectors))

	d := distancer.NewCosineDistanceProvider().New(distancer.Normalize(query))
	for i, vec := range vectors {
		dist, _, _ := d.Distance(distancer.Normalize(vec))
		distances[i] = distanceAndIndex{
			index:    uint64(i),
			distance: dist,
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	if len(distances) < k {
		k = len(distances)
	}

	out := make([]uint64, k)
	for i := 0; i < k; i++ {
		out[i] = distances[i].index
	}

	return out
}

func neverStop() bool {
	return false
}

// This test simulates what happens when the EP is removed from the
// VectorForID-serving store
func Test_DeleteEPVecInUnderlyingObjectStore(t *testing.T) {
	var vectorIndex *hnsw

	vectors := [][]float32{
		{1, 1},
		{2, 2},
		{3, 3},
	}

	vectorErrors := []error{
		nil,
		nil,
		nil,
	}
	store := testinghelpers.NewDummyStore(t)

	t.Run("import the test vectors", func(t *testing.T) {
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "delete-ep-in-underlying-store-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				fmt.Printf("vec for pos=%d is %v\n", id, vectors[int(id)])
				return vectors[int(id)], vectorErrors[int(id)]
			},
			TempVectorForIDThunk: TempVectorForIDThunk(vectors),
		}, ent.UserConfig{
			MaxConnections: 30,
			EFConstruction: 128,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), store)
		require.Nil(t, err)
		vectorIndex = index

		for i, vec := range vectors {
			err := vectorIndex.Add(uint64(i), vec)
			require.Nil(t, err)
		}

		fmt.Printf("ep is %d\n", vectorIndex.entryPointID)
	})

	t.Run("simulate ep vec deletion in object store", func(t *testing.T) {
		vectors[0] = nil
		vectorErrors[0] = storobj.NewErrNotFoundf(0, "deleted")
		vectorIndex.cache.Delete(context.Background(), 0)
	})

	t.Run("try to insert a fourth vector", func(t *testing.T) {
		vectors = append(vectors, []float32{4, 4})
		vectorErrors = append(vectorErrors, nil)

		pos := len(vectors) - 1
		err := vectorIndex.Add(uint64(pos), vectors[pos])
		require.Nil(t, err)
	})
}

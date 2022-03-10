//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHnswIndex(t *testing.T) {
	index := createEmptyHnswIndexForTests(t, testVectorForID)

	for i, vec := range testVectors {
		err := index.Add(uint64(i), vec)
		require.Nil(t, err)
	}

	t.Run("searching within cluster 1", func(t *testing.T) {
		position := 0
		res, _, err := index.knnSearchByVector(testVectors[position], 3, 36, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []uint64{0, 1, 2}, res)
	})

	t.Run("searching within cluster 2", func(t *testing.T) {
		position := 3
		res, _, err := index.knnSearchByVector(testVectors[position], 3, 36, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []uint64{3, 4, 5}, res)
	})

	t.Run("searching within cluster 3", func(t *testing.T) {
		position := 6
		res, _, err := index.knnSearchByVector(testVectors[position], 3, 36, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []uint64{6, 7, 8}, res)
	})

	t.Run("searching within cluster 2 with a scope larger than the cluster", func(t *testing.T) {
		position := 3
		res, _, err := index.knnSearchByVector(testVectors[position], 50, 36, nil)
		require.Nil(t, err)
		assert.Equal(t, []uint64{
			3, 5, 4, // cluster 2
			7, 8, 6, // cluster 3
			2, 1, 0, // cluster 1
		}, res)
	})
}

func TestHnswIndexGrow(t *testing.T) {
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
		err := index.Add(uint64(initialSize+1), vector)
		require.Nil(t, err)
	})
}

func createEmptyHnswIndexForTests(t *testing.T, vecForIDFn VectorForID) *hnsw {
	// mock out commit logger before adding data so we don't leave a disk
	// footprint. Commit logging and deserializing from a (condensed) commit log
	// is tested in a separate integration test that takes care of providing and
	// cleaning up the correct place on disk to write test files
	makeCL := MakeNoopCommitLogger
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: makeCL,
		DistanceProvider:      distancer.NewCosineProvider(),
		VectorForIDThunk:      vecForIDFn,
	}, UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	})
	require.Nil(t, err)
	return index
}

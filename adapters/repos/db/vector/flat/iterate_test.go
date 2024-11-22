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

package flat

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

func createTestIndex(t *testing.T) *flat {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	distancer := distancer.NewCosineDistanceProvider()

	store, err := lsmkv.New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	index, err := New(Config{
		ID:               uuid.New().String(),
		DistanceProvider: distancer,
		RootPath:         dirName,
	}, flatent.UserConfig{
		PQ: flatent.CompressionUserConfig{
			Enabled: false,
		},
		BQ: flatent.CompressionUserConfig{
			Enabled: false,
		},
	}, store)
	require.NoError(t, err)

	return index
}

func createTestVectors(n int) [][]float32 {
	dimensions := 256
	vectors, _ := testinghelpers.RandomVecs(n, 0, dimensions)
	testinghelpers.Normalize(vectors)

	return vectors
}

func TestFlatIndexIterate(t *testing.T) {
	ctx := context.Background()
	t.Run("should not run callback on empty index", func(t *testing.T) {
		index := createTestIndex(t)
		index.Iterate(func(id uint64) bool {
			t.Fatalf("callback should not be called on empty index")
			return true
		})
	})

	t.Run("should iterate over all nodes", func(t *testing.T) {
		testVectors := createTestVectors(10)
		index := createTestIndex(t)
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
			require.True(t, v, "node %d was not visited", i)
		}
	})

	t.Run("should stop iteration when callback returns false", func(t *testing.T) {
		testVectors := createTestVectors(10)
		index := createTestIndex(t)
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
				require.True(t, v, "node %d was not visited", i)
			} else {
				require.False(t, v, "node %d was visited", i)
			}
		}
	})
}

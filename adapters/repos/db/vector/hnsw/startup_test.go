//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestPrefill_CompressedDeletes(t *testing.T) {
	ctx := context.Background()
	vectors := vectorsForDeleteTest()

	dirName := t.TempDir()
	indexID := "backup-list-files-test"

	userConfig := enthnsw.NewDefaultUserConfig()
	idx, err := New(Config{
		RootPath:         dirName,
		ID:               indexID,
		Logger:           logrus.New(),
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		TempVectorForIDThunk: TempVectorForIDThunk(vectors),
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, logrus.New(), cyclemanager.NewCallbackGroupNoop())
		},
	}, userConfig, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	for i, vec := range vectors {
		err := idx.Add(uint64(i), vec)
		require.Nil(t, err)
	}

	userConfig.PQ = ent.PQConfig{
		Enabled: true,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionLogNormal,
		},
		BitCompression: false,
		Segments:       0,
		Centroids:      50,
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	idx.UpdateUserConfig(userConfig, func() {
		wg.Done()
	})
	wg.Wait()

	t.Run("all vectors should be in the compressed store", func(t *testing.T) {
		cursor := idx.compressedStore.Bucket(helpers.CompressedObjectsBucketLSM).Cursor()
		totalCount := 0
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			totalCount++
		}
		cursor.Close()
		assert.Equal(t, len(vectors), totalCount)
	})

	t.Run("deleting every even element", func(t *testing.T) {
		for i := range vectors {
			if i%2 != 0 {
				continue
			}

			err := idx.Delete(uint64(i))
			require.Nil(t, err)
		}
	})

	t.Run("runnign the cleanup", func(t *testing.T) {
		err := idx.CleanUpTombstonedNodes(neverStop)
		require.Nil(t, err)
	})

	t.Run("only half the vectors should be in the compressed store", func(t *testing.T) {
		cursor := idx.compressedStore.Bucket(helpers.CompressedObjectsBucketLSM).Cursor()
		totalCount := 0
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			totalCount++
		}
		cursor.Close()
		assert.Equal(t, len(vectors)/2, totalCount)
	})

	err = idx.Shutdown(ctx)
	require.Nil(t, err)
}

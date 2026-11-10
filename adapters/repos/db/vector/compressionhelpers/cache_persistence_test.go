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

package compressionhelpers_test

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// With cache persistence disabled the compressor must not touch the
// compressed bucket in any way, and PersistCache must then bring the bucket
// in line with the cache.
func TestCompressorCachePersistence(t *testing.T) {
	const (
		targetVector = "name"
		dims         = 32
	)
	logger, _ := test.NewNullLogger()
	dist := distancer.NewL2SquaredProvider()

	type newCompressor func(*lsmkv.Store, common.VectorForID[float32]) (compressionhelpers.VectorCompressor, error)
	tests := []struct {
		name string
		new  newCompressor
	}{
		{
			name: "bq",
			new: func(store *lsmkv.Store, vectorForID common.VectorForID[float32]) (compressionhelpers.VectorCompressor, error) {
				return compressionhelpers.NewBQCompressor(dist, 1e12, logger, store,
					lsmkv.MakeNoopBucketOptions, memwatch.NewDummyMonitor(), targetVector, vectorForID)
			},
		},
		{
			name: "rq8",
			new: func(store *lsmkv.Store, vectorForID common.VectorForID[float32]) (compressionhelpers.VectorCompressor, error) {
				return compressionhelpers.NewRQCompressor(dist, 1e12, logger, store,
					memwatch.NewDummyMonitor(), lsmkv.MakeNoopBucketOptions, 8, dims, targetVector, vectorForID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			vectors, _ := testinghelpers.RandomVecs(10, 0, dims)
			vectorForID := func(ctx context.Context, id uint64) ([]float32, error) {
				if id >= uint64(len(vectors)) {
					return nil, storobj.NewErrNotFoundf(id, "not found")
				}
				return vectors[id], nil
			}
			store := testinghelpers.NewDummyStore(t)
			bucketName := helpers.GetCompressedBucketName(targetVector)
			persisted := func() int {
				bucket := store.Bucket(bucketName)
				if bucket == nil {
					return 0
				}
				count := 0
				cursor := bucket.Cursor()
				defer cursor.Close()
				for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
					count++
				}
				return count
			}

			compressor, err := tt.new(store, vectorForID)
			require.NoError(t, err)
			compressor.SetCachePersistence(false)

			for id := uint64(0); id < 5; id++ {
				compressor.Preload(id, vectors[id])
			}
			require.Zero(t, persisted(), "preload must not write to the bucket")

			// id 7 is neither cached nor in the bucket
			_, err = compressor.DistanceBetweenCompressedVectorsFromIDs(ctx, 0, 7)
			require.NoError(t, err, "a cache miss must compress the raw vector")
			require.Zero(t, persisted(), "a cache miss must not write to the bucket")

			compressor.Delete(ctx, 4)
			_, err = compressor.DistanceBetweenCompressedVectorsFromIDs(ctx, 0, 1)
			require.NoError(t, err)

			// the bucket may be gone by the time the cache is persisted
			require.NoError(t, store.ShutdownBucket(ctx, bucketName))
			require.NoError(t, compressor.PersistCache(ctx))
			// ids 0-3 were preloaded and kept, 7 entered the cache on its miss
			require.Equal(t, 5, persisted(), "the bucket must hold what the cache holds")

			compressor.Preload(8, vectors[8])
			require.Equal(t, 6, persisted(), "preload must write through again")
			compressor.Delete(ctx, 8)
			require.Equal(t, 5, persisted(), "delete must reach the bucket again")
		})
	}
}

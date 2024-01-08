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

//go:build integrationTest
// +build integrationTest

package hnsw

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func Test_NoRaceCompressAdaptsSegments(t *testing.T) {
	ctx := context.Background()

	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	vectorsCount := 1000
	dimensions := 6
	expectedSegments := 3

	vectors, _ := testinghelpers.RandomVecs(vectorsCount, 0, dimensions)
	distancer := distancer.NewL2SquaredProvider()

	uc := ent.UserConfig{}
	uc.MaxConnections = maxNeighbors
	uc.EFConstruction = efConstruction
	uc.EF = ef
	uc.VectorCacheMaxObjects = 10e12
	uc.PQ = ent.PQConfig{
		Enabled: true,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionNormal,
		},
	}

	store := testinghelpers.NewDummyStore(t)
	defer func() {
		err := store.Shutdown(ctx)
		require.NoError(t, err)
	}()

	index, err := New(
		Config{
			RootPath:              t.TempDir(),
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			TempVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
				copy(container.Slice, vectors[int(id)])
				return container.Slice, nil
			},
		}, uc,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer func() {
		err := index.Shutdown(ctx)
		require.NoError(t, err)
	}()

	compressionhelpers.Concurrently(uint64(len(vectors)), func(id uint64) {
		index.Add(uint64(id), vectors[id])
	})
	uc.PQ = ent.PQConfig{
		Enabled: true,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionNormal,
		},
		Segments:  0,
		Centroids: 256,
	}
	index.compress(uc)
	assert.Equal(t, expectedSegments, int(index.compressor.ExposeFields().M))
	assert.Equal(t, expectedSegments, index.pqConfig.Segments)
}

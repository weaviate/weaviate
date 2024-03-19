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

	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestCompression_CalculateOptimalSegments(t *testing.T) {
	h := &hnsw{}

	type testCase struct {
		dimensions       int
		expectedSegments int
	}

	for _, tc := range []testCase{
		{
			dimensions:       2048,
			expectedSegments: 256,
		},
		{
			dimensions:       1536,
			expectedSegments: 256,
		},
		{
			dimensions:       768,
			expectedSegments: 128,
		},
		{
			dimensions:       512,
			expectedSegments: 128,
		},
		{
			dimensions:       256,
			expectedSegments: 64,
		},
		{
			dimensions:       125,
			expectedSegments: 125,
		},
		{
			dimensions:       64,
			expectedSegments: 32,
		},
		{
			dimensions:       27,
			expectedSegments: 27,
		},
		{
			dimensions:       19,
			expectedSegments: 19,
		},
		{
			dimensions:       2,
			expectedSegments: 1,
		},
	} {
		segments := h.calculateOptimalSegments(tc.dimensions)
		assert.Equal(t, tc.expectedSegments, segments)
	}
}

func Test_NoRaceCompressReturnsErrorWhenNotEnoughData(t *testing.T) {
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	dimensions := 200
	vectors_size := 10
	vectors, _ := testinghelpers.RandomVecs(vectors_size, 0, dimensions)
	distancer := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()

	uc := ent.UserConfig{}
	uc.MaxConnections = maxNeighbors
	uc.EFConstruction = efConstruction
	uc.EF = ef
	uc.VectorCacheMaxObjects = 10e12
	uc.PQ = ent.PQConfig{
		Enabled: false,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionLogNormal,
		},
		TrainingLimit: 5,
		Segments:      dimensions,
		Centroids:     256,
	}

	index, _ := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "recallbenchmark",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		TempVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}, uc, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	defer index.Shutdown(context.Background())
	assert.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(len(vectors)), func(id uint64) error {
		return index.Add(uint64(id), vectors[id])
	}))

	cfg := ent.PQConfig{
		Enabled: true,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionLogNormal,
		},
		Segments:  dimensions,
		Centroids: 256,
	}
	uc.PQ = cfg
	err := index.compress(uc)
	assert.NotNil(t, err)
}

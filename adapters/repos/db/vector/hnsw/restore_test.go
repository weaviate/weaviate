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
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func Test_RestartFromZeroSegments(t *testing.T) {
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	dimensions := 20
	vectors_size := 10000
	queries_size := 1
	vectors, _ := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	distancer := distancer.NewL2SquaredProvider()
	uc := ent.UserConfig{}
	uc.MaxConnections = maxNeighbors
	uc.EFConstruction = efConstruction
	uc.EF = ef
	uc.VectorCacheMaxObjects = 10e12
	uc.PQ = ent.PQConfig{Enabled: true, Encoder: ent.PQEncoder{Type: ent.PQEncoderTypeKMeans, Distribution: ent.PQEncoderDistributionNormal}}
	config := Config{
		RootPath:              path.Join(".", "compression_tests", "fixtures", "restart-from-zero-segments"),
		ID:                    "main",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		TempVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	_, err := New(
		config, uc,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())

	assert.Nil(t, err)
}

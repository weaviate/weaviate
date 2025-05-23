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
	"io"
	"os"
	"path"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var logger, _ = test.NewNullLogger()

func Test_RestartFromZeroSegments(t *testing.T) {
	testPath := t.TempDir()
	src := path.Join(".", "compression_tests", "fixtures", "restart-from-zero-segments", "1234567")
	source, err := os.Open(src)
	assert.Nil(t, err)
	dstPath := path.Join(testPath, "main.hnsw.commitlog.d")
	assert.Nil(t, os.Mkdir(dstPath, 0o777))
	destination, err := os.Create(path.Join(dstPath, "1234567"))
	assert.Nil(t, err)
	_, err = io.Copy(destination, source)
	assert.Nil(t, err)
	source.Close()
	destination.Close()

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
		RootPath:              testPath,
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

	_, err = New(
		config, uc,
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))

	assert.Nil(t, err)
}

func TestBackup_IntegrationHnsw(t *testing.T) {
	ctx := context.Background()
	dimensions := 20
	vectors_size := 10_000
	queries_size := 100
	k := 10

	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	truths := make([][]uint64, queries_size)
	distancer := distancer.NewL2SquaredProvider()
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, testinghelpers.DistanceWrapper(distancer))
	})

	dirName := t.TempDir()
	indexID := "restore-integration-test"
	noopCallback := cyclemanager.NewCallbackGroupNoop()
	hnswuc := ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 1_000_000,
	}

	config := Config{
		RootPath:         dirName,
		ID:               indexID,
		Logger:           logger,
		DistanceProvider: distancer,
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, logger, noopCallback)
		},
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			vec := vectors[int(id)]
			if vec == nil {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vec, nil
		},
		TempVectorForIDThunk: TempVectorForIDThunk(vectors),
	}

	store := testinghelpers.NewDummyStore(t)

	idx, err := New(config, hnswuc, cyclemanager.NewCallbackGroupNoop(), store)
	require.Nil(t, err)
	idx.PostStartup()

	compressionhelpers.Concurrently(logger, uint64(vectors_size), func(i uint64) {
		idx.Add(ctx, i, vectors[i])
	})
	recall1, _ := testinghelpers.RecallAndLatency(ctx, queries, k, idx, truths)
	assert.True(t, recall1 > 0.9)

	assert.Nil(t, idx.Flush())
	assert.Nil(t, idx.Shutdown(context.Background()))

	idx, err = New(config, hnswuc, cyclemanager.NewCallbackGroupNoop(), store)
	require.Nil(t, err)
	idx.PostStartup()

	recall2, _ := testinghelpers.RecallAndLatency(ctx, queries, k, idx, truths)
	assert.Equal(t, recall1, recall2)
}

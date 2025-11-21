//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"go.etcd.io/bbolt"
)

func TempVectorForIDThunk(vectors [][]float32) func(context.Context, uint64, *common.VectorSlice) ([]float32, error) {
	return func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
		copy(container.Slice, vectors[int(id)])
		return vectors[int(id)], nil
	}
}

func TestRestorBQ_Integration(t *testing.T) {
	ctx := context.Background()
	dimensions := 20
	vectors_size := 3_000
	queries_size := 100
	k := 10

	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	distancer := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()

	dirName := t.TempDir()
	indexID := "restore-bq-integration-test"
	noopCallback := cyclemanager.NewCallbackGroupNoop()
	uc := hnswent.UserConfig{}
	uc.SetDefaults()
	uc.MaxConnections = 30
	uc.EFConstruction = 64
	uc.EF = 32
	uc.VectorCacheMaxObjects = 1_000_000
	uc.BQ = hnswent.BQConfig{
		Enabled: true,
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "index.db")
	db, err := bbolt.Open(dbPath, 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	config := hnsw.Config{
		RootPath:         dirName,
		ID:               indexID,
		Logger:           logger,
		DistanceProvider: distancer,
		MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(dirName, indexID, logger, noopCallback)
		},
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			vec := vectors[int(id)]
			if vec == nil {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vec, nil
		},
		TempVectorForIDThunk: TempVectorForIDThunk(vectors),
		MakeBucketOptions:    lsmkv.MakeNoopBucketOptions,
	}

	idx, err := hnsw.New(config, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	idx.PostStartup(context.Background())

	compressionhelpers.Concurrently(logger, uint64(vectors_size), func(i uint64) {
		idx.Add(ctx, i, vectors[i])
	})

	assert.Nil(t, idx.Flush())
	assert.Nil(t, idx.Shutdown(context.Background()))

	idx, err = hnsw.New(config, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	idx.PostStartup(context.Background())

	for i := range queries {
		idx.SearchByVector(ctx, queries[i], k, nil)
	}
}

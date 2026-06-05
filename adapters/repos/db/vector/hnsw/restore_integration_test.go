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

package hnsw_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TempVectorForIDWithViewThunk(vectors [][]float32) func(context.Context, uint64, *common.VectorSlice, common.BucketView) ([]float32, error) {
	return func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		copy(container.Slice, vectors[int(id)])
		return vectors[int(id)], nil
	}
}

type noopBucketView struct{}

func (n *noopBucketView) ReleaseView() {}

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
		AllocChecker:     memwatch.NewDummyMonitor(),
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
		GetViewThunk:                 func() common.BucketView { return &noopBucketView{} },
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
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

func TestRestoreQuantization_Integration(t *testing.T) {
	ctx := context.Background()
	dimensions := 64
	vectorsSize := 290
	k := 10

	vectors, queries := testinghelpers.RandomVecs(vectorsSize, 1, dimensions)
	distancer := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()

	tests := []struct {
		name                string
		setupConfig         func(*hnswent.UserConfig)
		expectedDims        int32
		expectedCompression bool
		indexID             string
	}{
		{
			name: "uncompressed",
			setupConfig: func(uc *hnswent.UserConfig) {
				uc.RQ.Enabled = false
				uc.SkipDefaultQuantization = true
			},
			expectedDims:        int32(dimensions),
			expectedCompression: false,
			indexID:             "restore-bq-quantization-test",
		},
		{
			name: "BQ",
			setupConfig: func(uc *hnswent.UserConfig) {
				uc.BQ.Enabled = true
			},
			expectedDims:        int32(dimensions),
			expectedCompression: true,
			indexID:             "restore-quantization-test",
		},
		{
			name: "RQ_1bit",
			setupConfig: func(uc *hnswent.UserConfig) {
				uc.RQ = hnswent.RQConfig{
					Enabled: true,
					Bits:    1,
				}
			},
			expectedDims:        int32(dimensions),
			expectedCompression: true,
			indexID:             "restore-quantization-test",
		},
		{
			name: "RQ_8bit",
			setupConfig: func(uc *hnswent.UserConfig) {
				uc.RQ = hnswent.RQConfig{
					Enabled: true,
					Bits:    8,
				}
			},
			expectedDims:        int32(dimensions),
			expectedCompression: true,
			indexID:             "restore-quantization-test",
		},
		{
			name: "SQ",
			setupConfig: func(uc *hnswent.UserConfig) {
				uc.SQ.Enabled = true
			},
			expectedDims:        int32(dimensions),
			expectedCompression: true,
			indexID:             "restore-quantization-test",
		},
		{
			name: "PQ",
			setupConfig: func(uc *hnswent.UserConfig) {
				uc.PQ.Enabled = true
			},
			expectedDims:        int32(dimensions),
			expectedCompression: true,
			indexID:             "restore-quantization-test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dirName := t.TempDir()
			noopCallback := cyclemanager.NewCallbackGroupNoop()
			uc := hnswent.UserConfig{}
			uc.SetDefaults()
			tt.setupConfig(&uc)

			store := testinghelpers.NewDummyStore(t)
			defer store.Shutdown(context.Background())

			config := hnsw.Config{
				RootPath:         dirName,
				ID:               tt.indexID,
				Logger:           logger,
				DistanceProvider: distancer,
				MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
					return hnsw.NewCommitLogger(dirName, tt.indexID, logger, noopCallback)
				},
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					if int(id) >= len(vectors) {
						return nil, storobj.NewErrNotFoundf(id, "vector id out of range")
					}
					vec := vectors[int(id)]
					if vec == nil {
						return nil, storobj.NewErrNotFoundf(id, "nil vec")
					}
					return vec, nil
				},
				GetViewThunk: func() common.BucketView {
					return &noopBucketView{}
				},
				TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
				AllocChecker:                 memwatch.NewDummyMonitor(),
				MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
			}

			idx, err := hnsw.New(config, uc, cyclemanager.NewCallbackGroupNoop(), store)
			require.Nil(t, err)
			idx.PostStartup(context.Background())

			compressionhelpers.Concurrently(logger, uint64(vectorsSize), func(i uint64) {
				idx.Add(ctx, i, vectors[i])
			})

			// Explicitly set compression for non-automatic quantizers
			if tt.name == "PQ" || tt.name == "SQ" {
				var wg sync.WaitGroup
				wg.Add(1)
				err = idx.Upgrade(func() {
					wg.Done()
				})
				require.Nil(t, err, "Upgrade should start without error")
				wg.Wait()
				require.True(t, idx.Compressed(), "index should be compressed after Upgrade")
			}

			assert.Nil(t, idx.Flush())
			assert.Nil(t, idx.Shutdown(context.Background()))

			// Restore index using the same store
			idx, err = hnsw.New(config, uc, cyclemanager.NewCallbackGroupNoop(), store)
			require.Nil(t, err)
			idx.PostStartup(context.Background())

			// Check dimensions
			stats, err := idx.Stats()
			require.Nil(t, err)
			require.Equal(t, tt.expectedDims, stats.Dimensions, "dimensions should match expected value after restore")

			// Check compression
			require.Equal(t, tt.expectedCompression, idx.Compressed())

			// Perform a single search
			results, _, err := idx.SearchByVector(ctx, queries[0], k, nil)
			require.Nil(t, err)
			require.Greater(t, len(results), 0, "search should return results")
		})
	}
}

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

package dynamic

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var logger, _ = test.NewNullLogger()

func TestDynamic(t *testing.T) {
	ctx := context.Background()
	currentIndexing := os.Getenv("ASYNC_INDEXING")
	os.Setenv("ASYNC_INDEXING", "true")
	defer os.Setenv("ASYNC_INDEXING", currentIndexing)
	dimensions := 20
	vectors_size := 1_000
	queries_size := 10
	k := 10

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	rootPath := t.TempDir()
	distancer := distancer.NewL2SquaredProvider()
	truths := make([][]uint64, queries_size)
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, testinghelpers.DistanceWrapper(distancer))
	})
	noopCallback := cyclemanager.NewCallbackGroupNoop()
	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	hnswuc := hnswent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 1_000_000,
	}
	dynamic, err := New(Config{
		RootPath:              rootPath,
		ID:                    "nil-vector-test",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			vec := vectors[int(id)]
			if vec == nil {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vec, nil
		},
		TempVectorForIDThunk: TempVectorForIDThunk(vectors),
		TombstoneCallbacks:   noopCallback,
		SharedDB:             db,
	}, ent.UserConfig{
		Threshold: uint64(vectors_size),
		Distance:  distancer.Type(),
		HnswUC:    hnswuc,
		FlatUC:    fuc,
	}, testinghelpers.NewDummyStore(t))
	assert.Nil(t, err)

	compressionhelpers.Concurrently(logger, uint64(vectors_size), func(i uint64) {
		err := dynamic.Add(ctx, i, vectors[i])
		require.NoError(t, err)
	})
	shouldUpgrade, at := dynamic.ShouldUpgrade()
	assert.True(t, shouldUpgrade)
	assert.Equal(t, vectors_size, at)
	assert.False(t, dynamic.Upgraded())
	recall1, latency1 := testinghelpers.RecallAndLatency(ctx, queries, k, dynamic, truths)
	fmt.Println(recall1, latency1)
	assert.True(t, recall1 > 0.99)
	wg := sync.WaitGroup{}
	wg.Add(1)
	err = dynamic.Upgrade(func() {
		wg.Done()
	})
	require.NoError(t, err)
	wg.Wait()
	shouldUpgrade, _ = dynamic.ShouldUpgrade()
	assert.False(t, shouldUpgrade)
	recall2, latency2 := testinghelpers.RecallAndLatency(ctx, queries, k, dynamic, truths)
	fmt.Println(recall2, latency2)
	assert.True(t, recall2 > 0.9)
	assert.True(t, latency1 > latency2)
}

func TestDynamicReturnsErrorIfNoAsync(t *testing.T) {
	currentIndexing := os.Getenv("ASYNC_INDEXING")
	os.Unsetenv("ASYNC_INDEXING")
	defer os.Setenv("ASYNC_INDEXING", currentIndexing)
	rootPath := t.TempDir()
	noopCallback := cyclemanager.NewCallbackGroupNoop()
	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	hnswuc := hnswent.NewDefaultUserConfig()
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	distancer := distancer.NewL2SquaredProvider()
	_, err = New(Config{
		RootPath:              rootPath,
		ID:                    "nil-vector-test",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, nil
		},
		TempVectorForIDThunk: TempVectorForIDThunk(nil),
		TombstoneCallbacks:   noopCallback,
		SharedDB:             db,
	}, ent.UserConfig{
		Threshold: uint64(100),
		Distance:  distancer.Type(),
		HnswUC:    hnswuc,
		FlatUC:    fuc,
	}, testinghelpers.NewDummyStore(t))
	assert.NotNil(t, err)
}

func TempVectorForIDThunk(vectors [][]float32) func(context.Context, uint64, *common.VectorSlice) ([]float32, error) {
	return func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
		copy(container.Slice, vectors[int(id)])
		return vectors[int(id)], nil
	}
}

func TestDynamicWithTargetVectors(t *testing.T) {
	ctx := context.Background()
	currentIndexing := os.Getenv("ASYNC_INDEXING")
	os.Setenv("ASYNC_INDEXING", "true")
	defer os.Setenv("ASYNC_INDEXING", currentIndexing)
	dimensions := 20
	vectors_size := 1_000
	queries_size := 10
	k := 10

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	rootPath := t.TempDir()
	distancer := distancer.NewL2SquaredProvider()
	truths := make([][]uint64, queries_size)
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, testinghelpers.DistanceWrapper(distancer))
	})
	noopCallback := cyclemanager.NewCallbackGroupNoop()
	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	hnswuc := hnswent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 1_000_000,
	}

	var indexes []*dynamic

	for i := 0; i < 5; i++ {
		dynamic, err := New(Config{
			TargetVector:          "target_" + strconv.Itoa(i),
			RootPath:              rootPath,
			ID:                    "nil-vector-test_" + strconv.Itoa(i),
			MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
			DistanceProvider:      distancer,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				vec := vectors[int(id)]
				if vec == nil {
					return nil, storobj.NewErrNotFoundf(id, "nil vec")
				}
				return vec, nil
			},
			TempVectorForIDThunk: TempVectorForIDThunk(vectors),
			TombstoneCallbacks:   noopCallback,
			SharedDB:             db,
		}, ent.UserConfig{
			Threshold: uint64(vectors_size),
			Distance:  distancer.Type(),
			HnswUC:    hnswuc,
			FlatUC:    fuc,
		}, testinghelpers.NewDummyStore(t))
		require.NoError(t, err)

		indexes = append(indexes, dynamic)
	}

	for _, v := range indexes {
		v := v
		compressionhelpers.Concurrently(logger, uint64(vectors_size), func(i uint64) {
			v.Add(ctx, i, vectors[i])
		})
		shouldUpgrade, at := v.ShouldUpgrade()
		assert.True(t, shouldUpgrade)
		assert.Equal(t, vectors_size, at)
		assert.False(t, v.Upgraded())
		recall1, latency1 := testinghelpers.RecallAndLatency(ctx, queries, k, v, truths)
		fmt.Println(recall1, latency1)
		assert.True(t, recall1 > 0.99)
		wg := sync.WaitGroup{}
		wg.Add(1)
		v.Upgrade(func() {
			wg.Done()
		})
		wg.Wait()
		shouldUpgrade, _ = v.ShouldUpgrade()
		assert.False(t, shouldUpgrade)
		recall2, latency2 := testinghelpers.RecallAndLatency(ctx, queries, k, v, truths)
		fmt.Println(recall2, latency2)
		assert.True(t, recall2 > 0.9)
		assert.True(t, latency1 > latency2)
	}
}

func TestDynamicUpgradeCancelation(t *testing.T) {
	ctx := context.Background()
	t.Setenv("ASYNC_INDEXING", "true")
	dimensions := 20
	vectors_size := 1_000
	queries_size := 10
	k := 10

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	rootPath := t.TempDir()
	distancer := distancer.NewL2SquaredProvider()
	truths := make([][]uint64, queries_size)
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, testinghelpers.DistanceWrapper(distancer))
	})
	noopCallback := cyclemanager.NewCallbackGroupNoop()
	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	hnswuc := hnswent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 1_000_000,
	}

	dynamic, err := New(Config{
		RootPath:              rootPath,
		ID:                    "foo",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			vec := vectors[int(id)]
			if vec == nil {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vec, nil
		},
		TempVectorForIDThunk: TempVectorForIDThunk(vectors),
		TombstoneCallbacks:   noopCallback,
		SharedDB:             db,
	}, ent.UserConfig{
		Threshold: uint64(vectors_size),
		Distance:  distancer.Type(),
		HnswUC:    hnswuc,
		FlatUC:    fuc,
	}, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)

	compressionhelpers.Concurrently(logger, uint64(vectors_size), func(i uint64) {
		dynamic.Add(ctx, i, vectors[i])
	})

	shouldUpgrade, at := dynamic.ShouldUpgrade()
	require.True(t, shouldUpgrade)
	require.Equal(t, vectors_size, at)
	require.False(t, dynamic.Upgraded())

	called := make(chan struct{})
	dynamic.Upgrade(func() {
		close(called)
	})

	// close the index to cancel the upgrade
	err = dynamic.Shutdown(context.Background())
	require.NoError(t, err)

	require.False(t, dynamic.upgraded.Load())

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("upgrade callback was not called")
	}
}

func TestDynamicWithDifferentCompressionSchema(t *testing.T) {
	ctx := context.Background()
	t.Setenv("ASYNC_INDEXING", "true")
	dimensions := 20
	vectors_size := 1_000
	threshold := 600
	queries_size := 10
	k := 10

	tempDir := t.TempDir()

	db, err := bbolt.Open(filepath.Join(tempDir, "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	rootPath := tempDir
	distancer := distancer.NewL2SquaredProvider()
	truths := make([][]uint64, queries_size)
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, testinghelpers.DistanceWrapper(distancer))
	})
	noopCallback := cyclemanager.NewCallbackGroupNoop()
	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	fuc.BQ = flatent.CompressionUserConfig{
		Enabled: true,
		Cache:   true,
	}
	hnswuc := hnswent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 1_000_000,
		PQ: hnswent.PQConfig{
			Enabled:        true,
			BitCompression: false,
			Segments:       5,
			Centroids:      255,
			TrainingLimit:  threshold - 1,
			Encoder: hnswent.PQEncoder{
				Type:         hnswent.PQEncoderTypeKMeans,
				Distribution: hnswent.PQEncoderDistributionLogNormal,
			},
		},
	}

	config := Config{
		TargetVector: "",
		RootPath:     rootPath,
		ID:           "vector-test_0",
		MakeCommitLoggerThunk: func() (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(tempDir, "vector-test_0", logger, noopCallback)
		},
		DistanceProvider: distancer,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			vec := vectors[int(id)]
			if vec == nil {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vec, nil
		},
		TempVectorForIDThunk:    TempVectorForIDThunk(vectors),
		TombstoneCallbacks:      noopCallback,
		SharedDB:                db,
		HNSWWaitForCachePrefill: true,
	}
	uc := ent.UserConfig{
		Threshold: uint64(threshold),
		Distance:  distancer.Type(),
		HnswUC:    hnswuc,
		FlatUC:    fuc,
	}

	dummyStore := testinghelpers.NewDummyStore(t)
	dynamic, err := New(config, uc, dummyStore)
	require.NoError(t, err)

	compressionhelpers.Concurrently(logger, uint64(threshold), func(i uint64) {
		err := dynamic.Add(ctx, i, vectors[i])
		require.NoError(t, err)
	})
	shouldUpgrade, at := dynamic.ShouldUpgrade()
	assert.True(t, shouldUpgrade)
	assert.Equal(t, threshold, at)
	assert.False(t, dynamic.Upgraded())
	var wg sync.WaitGroup
	wg.Add(1)

	// flat -> hnsw
	err = dynamic.Upgrade(func() {
		wg.Done()
	})
	require.NoError(t, err)
	wg.Wait()
	wg.Add(1)

	// PQ
	err = dynamic.Upgrade(func() {
		wg.Done()
	})
	require.NoError(t, err)
	wg.Wait()
	compressionhelpers.Concurrently(logger, uint64(vectors_size-threshold), func(i uint64) {
		err := dynamic.Add(ctx, uint64(threshold)+i, vectors[threshold+int(i)])
		require.NoError(t, err)
	})

	recall, latency := testinghelpers.RecallAndLatency(ctx, queries, k, dynamic, truths)
	fmt.Println(recall, latency)

	err = dynamic.Flush()
	require.NoError(t, err)
	err = dynamic.Shutdown(t.Context())
	require.NoError(t, err)
	dummyStore.FlushMemtables(t.Context())

	// open the db again
	db, err = bbolt.Open(filepath.Join(tempDir, "index.db"), 0o666, nil)
	require.NoError(t, err)
	config.SharedDB = db

	dynamic, err = New(config, uc, dummyStore)
	require.NoError(t, err)
	dynamic.PostStartup()
	recall2, _ := testinghelpers.RecallAndLatency(ctx, queries, k, dynamic, truths)
	assert.Equal(t, recall, recall2)
}

func TestDynamicIndexUnderlyingIndexDetection(t *testing.T) {
	tests := []struct {
		name           string
		underlyingType common.IndexType
		expectedString string
		expectedType   common.IndexType
	}{
		{
			name:           "dynamic index with flat underlying",
			underlyingType: common.IndexTypeFlat,
			expectedString: "flat",
			expectedType:   common.IndexTypeFlat,
		},
		{
			name:           "dynamic index with hnsw underlying",
			underlyingType: common.IndexTypeHNSW,
			expectedString: "hnsw",
			expectedType:   common.IndexTypeHNSW,
		},
		{
			name:           "dynamic index with dynamic underlying",
			underlyingType: common.IndexTypeDynamic,
			expectedString: "dynamic",
			expectedType:   common.IndexTypeDynamic,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock that implements the UnderlyingIndex method
			mockDynamicIndex := NewMockIndex(t)
			mockDynamicIndex.EXPECT().UnderlyingIndex().Return(tt.underlyingType)

			// Test the method directly
			underlyingType := mockDynamicIndex.UnderlyingIndex()

			// Assert the returned type
			assert.Equal(t, tt.expectedType, underlyingType, "Should return correct underlying index type")

			// Assert the string conversion
			assert.Equal(t, tt.expectedString, underlyingType.String(), "Should convert to correct string")
		})
	}
}

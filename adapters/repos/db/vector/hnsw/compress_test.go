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

package hnsw

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func Test_NoRaceCompressReturnsErrorWhenNotEnoughData(t *testing.T) {
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	dimensions := 200
	vectors_size := 10
	vectors, _ := testinghelpers.RandomVecs(vectors_size, 0, dimensions)
	distancer := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

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
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
	}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	defer index.Shutdown(context.Background())
	assert.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(len(vectors)), func(id uint64) error {
		return index.Add(ctx, uint64(id), vectors[id])
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

func Test_CompressAndInsertDoNotRace(t *testing.T) {
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	dimensions := 200
	vectors_size := 100
	vectors, _ := testinghelpers.RandomVecs(vectors_size, 0, dimensions)
	distancer := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	uc := ent.UserConfig{}
	uc.MaxConnections = maxNeighbors
	uc.EFConstruction = efConstruction
	uc.EF = ef
	uc.VectorCacheMaxObjects = 10e12
	uc.RQ = ent.RQConfig{
		Enabled: false,
		Bits:    8,
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
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		AllocChecker:      memwatch.NewDummyMonitor(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	t.Cleanup(func() {
		_ = index.Shutdown(context.Background())
	})

	var wg sync.WaitGroup
	wg.Add(len(vectors))

	cfg := ent.RQConfig{
		Enabled: true,
		Bits:    8,
	}
	uc.RQ = cfg
	start := make(chan struct{})
	done := make(chan struct{})
	go func() {
		<-start
		index.UpdateUserConfig(uc, func() { close(done) })
	}()

	assert.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(len(vectors)), func(id uint64) error {
		defer wg.Done()
		if id == 0 {
			close(start)
		}
		return index.Add(ctx, uint64(id), vectors[id])
	}))

	wg.Wait()
	<-done
}

func userConfig(segments, centroids, maxConn, efC, ef, trainingLimit int) ent.UserConfig {
	uc := ent.UserConfig{}
	uc.MaxConnections = maxConn
	uc.EFConstruction = efC
	uc.EF = ef
	uc.VectorCacheMaxObjects = 10e12
	uc.PQ = ent.PQConfig{
		Enabled: true,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionLogNormal,
		},
		TrainingLimit: trainingLimit,
		Segments:      segments,
		Centroids:     centroids,
	}
	return uc
}

func indexConfig(vectorId, tempDir string, logger *logrus.Logger, vectors [][]float32, distancer distancer.Provider) Config {
	correctedID := fmt.Sprintf("vectors_%s", vectorId)
	return Config{
		RootPath: tempDir,
		ID:       correctedID,
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(tempDir, correctedID, logger, cyclemanager.NewCallbackGroupNoop())
		},
		DistanceProvider: distancer,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
	}
}

func Test_NoRaceCompressNamedVectorsDoNotCollide(t *testing.T) {
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	dimensions1 := 20
	dimensions2 := 30
	vectors_size := 50
	vectors1, queries1 := testinghelpers.RandomVecs(vectors_size, 1, dimensions1)
	vectors2, queries2 := testinghelpers.RandomVecs(vectors_size, 1, dimensions2)
	distancer := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	tempDir := t.TempDir()
	dummyStore := testinghelpers.NewDummyStoreFromFolder(tempDir, t)

	uc1 := userConfig(dimensions1/10, 5, maxNeighbors, efConstruction, ef, vectors_size)
	config1 := indexConfig("namedVec1", tempDir, logger, vectors1, distancer)

	index1, _ := New(config1, uc1, cyclemanager.NewCallbackGroupNoop(), dummyStore)

	wg := sync.WaitGroup{}
	wg.Add(len(vectors1))
	assert.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(len(vectors1)), func(id uint64) error {
		defer wg.Done()
		return index1.Add(ctx, uint64(id), vectors1[id])
	}))
	wg.Wait()

	err := index1.compress(uc1)
	assert.Nil(t, err)

	config2 := indexConfig("namedVec2", tempDir, logger, vectors2, distancer)
	uc2 := userConfig(dimensions2/10, 5, maxNeighbors, efConstruction, ef, vectors_size)

	index2, _ := New(config2, uc2, cyclemanager.NewCallbackGroupNoop(), dummyStore)

	wg = sync.WaitGroup{}
	wg.Add(len(vectors1))
	assert.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(len(vectors2)), func(id uint64) error {
		defer wg.Done()
		return index2.Add(ctx, uint64(id), vectors2[id])
	}))
	wg.Wait()

	err = index2.compress(uc2)
	assert.Nil(t, err)

	assert.Nil(t, index1.Flush())
	assert.Nil(t, index2.Flush())
	assert.Nil(t, index1.Shutdown(context.Background()))
	assert.Nil(t, index2.Shutdown(context.Background()))
	dummyStore.FlushMemtables(t.Context())

	index1, _ = New(config1, uc1, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	index2, _ = New(config2, uc2, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	index1.PostStartup(context.Background())
	index2.PostStartup(context.Background())

	_, _, err = index1.SearchByVector(t.Context(), queries1[0], 10, nil)
	assert.Nil(t, err)
	_, _, err = index2.SearchByVector(t.Context(), queries2[0], 10, nil)
	assert.Nil(t, err)
}

func Test_NoRaceCompressNamedVectorsDoNotMessEachOther(t *testing.T) {
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	dimensions := 20
	vectors_size := 50
	vectors1, queries1 := testinghelpers.RandomVecs(vectors_size, 1, dimensions)
	vectors2, queries2 := testinghelpers.RandomVecs(vectors_size, 1, dimensions)
	distancer := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	tempDir := t.TempDir()
	dummyStore := testinghelpers.NewDummyStoreFromFolder(tempDir, t)

	uc1 := userConfig(dimensions/10, 5, maxNeighbors, efConstruction, ef, vectors_size)
	config1 := indexConfig("namedVec1", tempDir, logger, vectors1, distancer)

	index1, _ := New(config1, uc1, cyclemanager.NewCallbackGroupNoop(), dummyStore)

	wg := sync.WaitGroup{}
	wg.Add(len(vectors1))
	assert.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(len(vectors1)), func(id uint64) error {
		defer wg.Done()
		return index1.Add(ctx, uint64(id), vectors1[id])
	}))
	wg.Wait()

	err := index1.compress(uc1)
	assert.Nil(t, err)

	config2 := indexConfig("namedVec2", tempDir, logger, vectors2, distancer)
	uc2 := userConfig(dimensions/10, 5, maxNeighbors, efConstruction, ef, vectors_size)

	index2, _ := New(config2, uc2, cyclemanager.NewCallbackGroupNoop(), dummyStore)

	wg = sync.WaitGroup{}
	wg.Add(len(vectors1))
	assert.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(len(vectors2)), func(id uint64) error {
		defer wg.Done()
		return index2.Add(ctx, uint64(id), vectors2[id])
	}))
	wg.Wait()

	err = index2.compress(uc2)
	assert.Nil(t, err)

	control1, _, err := index1.SearchByVector(t.Context(), queries1[0], 10, nil)
	assert.Nil(t, err)
	control2, _, err := index2.SearchByVector(t.Context(), queries2[0], 10, nil)
	assert.Nil(t, err)

	sample1, _, err := index1.SearchByVector(t.Context(), queries1[0], 10, nil)
	assert.Nil(t, err)
	sample2, _, err := index2.SearchByVector(t.Context(), queries2[0], 10, nil)
	assert.Nil(t, err)
	assert.ElementsMatch(t, control1, sample1)
	assert.ElementsMatch(t, control2, sample2)

	assert.Nil(t, index1.Flush())
	assert.Nil(t, index2.Flush())
	assert.Nil(t, index1.Shutdown(context.Background()))
	assert.Nil(t, index2.Shutdown(context.Background()))
	dummyStore.FlushMemtables(t.Context())

	index1, _ = New(config1, uc1, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	index2, _ = New(config2, uc2, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	index1.PostStartup(context.Background())
	index2.PostStartup(context.Background())

	sample1, _, err = index1.SearchByVector(t.Context(), queries1[0], 10, nil)
	assert.Nil(t, err)
	sample2, _, err = index2.SearchByVector(t.Context(), queries2[0], 10, nil)
	assert.Nil(t, err)
	assert.ElementsMatch(t, control1, sample1)
	assert.ElementsMatch(t, control2, sample2)
}

func Test_CompressRQWithSlowCachePrefill(t *testing.T) {
	dimensions := 64
	vectorsSize := 100
	vectors, _ := testinghelpers.RandomVecs(vectorsSize, 0, dimensions)
	dist := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	tempDir := t.TempDir()
	dummyStore := testinghelpers.NewDummyStoreFromFolder(tempDir, t)

	uc := ent.UserConfig{}
	uc.SetDefaults()

	// Phase 1: build the index with 100 vectors, then shut down.
	indexID := "slow_prefill_test"
	makeCommitLogger := func() (CommitLogger, error) {
		return NewCommitLogger(tempDir, indexID, logger, cyclemanager.NewCallbackGroupNoop())
	}
	cfg := Config{
		RootPath:              tempDir,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCommitLogger,
		DistanceProvider:      dist,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)

	require.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(vectorsSize), func(id uint64) error {
		return index.Add(ctx, uint64(id), vectors[id])
	}))

	require.Nil(t, index.Flush())
	require.Nil(t, index.Shutdown(ctx))
	dummyStore.FlushMemtables(ctx)

	// Phase 2: restart the index with a slow VectorForIDThunk to simulate
	// a very slow cache prefill and WaitForCachePrefill = false.
	slowCfg := Config{
		RootPath:              tempDir,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCommitLogger,
		DistanceProvider:      dist,
		WaitForCachePrefill:   false,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			time.Sleep(100 * time.Millisecond)
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	index, err = New(slowCfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)
	t.Cleanup(func() {
		_ = index.Shutdown(ctx)
	})

	// Start async cache prefill (will be very slow due to sleep).
	index.PostStartup(ctx)

	// Wait briefly so at least one vector is loaded into the cache.
	time.Sleep(300 * time.Millisecond)

	// Enable RQ compression before the cache finishes prefilling.
	ucRQ := uc
	ucRQ.RQ = ent.RQConfig{
		Enabled: true,
		Bits:    8,
	}

	done := make(chan struct{})
	err = index.UpdateUserConfig(ucRQ, func() { close(done) })
	require.Nil(t, err)
	<-done

	// Compression should be deferred because the cache is still prefilling.
	assert.False(t, index.compressed.Load(),
		"index should not be compressed while cache prefill is still in progress")
}

func Test_CompressRQAfterCachePrefillCompletes(t *testing.T) {
	dimensions := 64
	vectorsSize := 100
	vectors, _ := testinghelpers.RandomVecs(vectorsSize, 0, dimensions)
	dist := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	tempDir := t.TempDir()
	dummyStore := testinghelpers.NewDummyStoreFromFolder(tempDir, t)

	uc := ent.UserConfig{}
	uc.SetDefaults()

	// Phase 1: build the index with 100 vectors, then shut down.
	indexID := "prefill_then_compress"
	makeCommitLogger := func() (CommitLogger, error) {
		return NewCommitLogger(tempDir, indexID, logger, cyclemanager.NewCallbackGroupNoop())
	}
	cfg := Config{
		RootPath:              tempDir,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCommitLogger,
		DistanceProvider:      dist,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)

	require.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(vectorsSize), func(id uint64) error {
		return index.Add(ctx, uint64(id), vectors[id])
	}))

	require.Nil(t, index.Flush())
	require.Nil(t, index.Shutdown(ctx))
	dummyStore.FlushMemtables(ctx)

	// Phase 2: restart with WaitForCachePrefill = true so prefill completes
	// before we enable RQ. This verifies RQ works normally after a full prefill.
	restartCfg := Config{
		RootPath:              tempDir,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCommitLogger,
		DistanceProvider:      dist,
		WaitForCachePrefill:   true,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	index, err = New(restartCfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)
	t.Cleanup(func() {
		_ = index.Shutdown(ctx)
	})

	// Prefill completes synchronously.
	index.PostStartup(ctx)

	// Enable RQ compression after cache is fully prefilled.
	ucRQ := uc
	ucRQ.RQ = ent.RQConfig{
		Enabled: true,
		Bits:    8,
	}

	done := make(chan struct{})
	err = index.UpdateUserConfig(ucRQ, func() { close(done) })
	require.Nil(t, err)
	<-done

	require.True(t, index.compressed.Load(), "index should be compressed after prefill completes")

	compressed := index.compressor.CountVectors()
	t.Logf("compressor has %d of %d vectors", compressed, vectorsSize)
	assert.Equal(t, int64(vectorsSize), compressed,
		"expected compressor to have all vectors after full cache prefill")
}

func Test_CompressRQInsertDuringSlowPrefillDoesNotTrigger(t *testing.T) {
	dimensions := 64
	vectorsSize := 100
	vectors, _ := testinghelpers.RandomVecs(vectorsSize+1, 0, dimensions)
	dist := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	tempDir := t.TempDir()
	dummyStore := testinghelpers.NewDummyStoreFromFolder(tempDir, t)

	uc := ent.UserConfig{}
	uc.SetDefaults()

	indexID := "slow_prefill_insert"
	makeCommitLogger := func() (CommitLogger, error) {
		return NewCommitLogger(tempDir, indexID, logger, cyclemanager.NewCallbackGroupNoop())
	}

	// Phase 1: build the index with 100 vectors, then shut down.
	cfg := Config{
		RootPath:              tempDir,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCommitLogger,
		DistanceProvider:      dist,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)

	require.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(vectorsSize), func(id uint64) error {
		return index.Add(ctx, uint64(id), vectors[id])
	}))

	require.Nil(t, index.Flush())
	require.Nil(t, index.Shutdown(ctx))
	dummyStore.FlushMemtables(ctx)

	// Phase 2: restart with slow prefill and RQ enabled in the config.
	ucRQ := uc
	ucRQ.RQ = ent.RQConfig{Enabled: true, Bits: 8}

	slowCfg := Config{
		RootPath:              tempDir,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCommitLogger,
		DistanceProvider:      dist,
		WaitForCachePrefill:   false,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			time.Sleep(100 * time.Millisecond)
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	index, err = New(slowCfg, ucRQ, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)
	t.Cleanup(func() {
		_ = index.Shutdown(ctx)
	})

	// Start async cache prefill (slow).
	index.PostStartup(ctx)
	time.Sleep(300 * time.Millisecond)

	// Insert a new vector via AddBatch while cache is still prefilling.
	// This should NOT trigger RQ compression.
	newID := uint64(vectorsSize)
	err = index.AddBatch(ctx, []uint64{newID}, [][]float32{vectors[newID]})
	require.Nil(t, err)

	assert.False(t, index.compressed.Load(),
		"AddBatch should not trigger RQ compression while cache prefill is in progress")
}

func Test_CompressRQInsertAfterPrefillTriggers(t *testing.T) {
	dimensions := 64
	vectorsSize := 100
	vectors, _ := testinghelpers.RandomVecs(vectorsSize+1, 0, dimensions)
	dist := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	tempDir := t.TempDir()
	dummyStore := testinghelpers.NewDummyStoreFromFolder(tempDir, t)

	uc := ent.UserConfig{}
	uc.SetDefaults()

	indexID := "prefill_insert_compress"
	makeCommitLogger := func() (CommitLogger, error) {
		return NewCommitLogger(tempDir, indexID, logger, cyclemanager.NewCallbackGroupNoop())
	}

	// Phase 1: build the index with 100 vectors, then shut down.
	cfg := Config{
		RootPath:              tempDir,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCommitLogger,
		DistanceProvider:      dist,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)

	require.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(vectorsSize), func(id uint64) error {
		return index.Add(ctx, uint64(id), vectors[id])
	}))

	require.Nil(t, index.Flush())
	require.Nil(t, index.Shutdown(ctx))
	dummyStore.FlushMemtables(ctx)

	// Phase 2: restart with sync prefill and RQ enabled in the config.
	ucRQ := uc
	ucRQ.RQ = ent.RQConfig{Enabled: true, Bits: 8}

	restartCfg := Config{
		RootPath:              tempDir,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCommitLogger,
		DistanceProvider:      dist,
		WaitForCachePrefill:   true,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	index, err = New(restartCfg, ucRQ, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)
	t.Cleanup(func() {
		_ = index.Shutdown(ctx)
	})

	// Prefill completes synchronously.
	index.PostStartup(ctx)

	// Insert a new vector via AddBatch — this should trigger RQ compression
	// since the cache is fully prefilled and rqActive is true.
	newID := uint64(vectorsSize)
	err = index.AddBatch(ctx, []uint64{newID}, [][]float32{vectors[newID]})
	require.Nil(t, err)

	require.True(t, index.compressed.Load(),
		"AddBatch should trigger RQ compression after cache prefill completes")

	compressed := index.compressor.CountVectors()
	t.Logf("compressor has %d of %d vectors", compressed, vectorsSize+1)
	assert.Equal(t, int64(vectorsSize+1), compressed,
		"expected compressor to have all vectors including the newly inserted one")
}

func Test_CompressBQWithSlowCachePrefill(t *testing.T) {
	dimensions := 64
	vectorsSize := 100
	vectors, _ := testinghelpers.RandomVecs(vectorsSize, 0, dimensions)
	dist := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	tempDir := t.TempDir()
	dummyStore := testinghelpers.NewDummyStoreFromFolder(tempDir, t)

	uc := ent.UserConfig{}
	uc.SetDefaults()

	// Phase 1: build the index with 100 vectors, then shut down.
	indexID := "slow_prefill_bq"
	makeCommitLogger := func() (CommitLogger, error) {
		return NewCommitLogger(tempDir, indexID, logger, cyclemanager.NewCallbackGroupNoop())
	}
	cfg := Config{
		RootPath:              tempDir,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCommitLogger,
		DistanceProvider:      dist,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)

	require.Nil(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(vectorsSize), func(id uint64) error {
		return index.Add(ctx, uint64(id), vectors[id])
	}))

	require.Nil(t, index.Flush())
	require.Nil(t, index.Shutdown(ctx))
	dummyStore.FlushMemtables(ctx)

	// Phase 2: restart with slow prefill.
	slowCfg := Config{
		RootPath:              tempDir,
		ID:                    indexID,
		MakeCommitLoggerThunk: makeCommitLogger,
		DistanceProvider:      dist,
		WaitForCachePrefill:   false,
		AllocChecker:          memwatch.NewDummyMonitor(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			time.Sleep(100 * time.Millisecond)
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView {
			return &noopBucketView{}
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}

	index, err = New(slowCfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.Nil(t, err)
	t.Cleanup(func() {
		_ = index.Shutdown(ctx)
	})

	// Start async cache prefill (will be very slow due to sleep).
	index.PostStartup(ctx)
	time.Sleep(300 * time.Millisecond)

	// Enable BQ compression before the cache finishes prefilling.
	ucBQ := uc
	ucBQ.BQ = ent.BQConfig{Enabled: true}

	done := make(chan struct{})
	err = index.UpdateUserConfig(ucBQ, func() { close(done) })
	require.Nil(t, err)
	<-done

	// Compression should be deferred because the cache is still prefilling.
	assert.False(t, index.compressed.Load(),
		"index should not be compressed while cache prefill is still in progress")
}

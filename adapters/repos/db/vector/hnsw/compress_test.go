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
	"fmt"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
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
		TempVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
		AllocChecker: memwatch.NewDummyMonitor(),
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
		TempVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
		AllocChecker: memwatch.NewDummyMonitor(),
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

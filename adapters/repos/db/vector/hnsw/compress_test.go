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
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
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

func Test_CompressedEntrypointRecoveryOnRestart(t *testing.T) {
	dimensions := 20
	vectorsSize := 10
	vectors, queries := testinghelpers.RandomVecs(vectorsSize, 1, dimensions)
	dist := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	tempDir := t.TempDir()
	dummyStore := testinghelpers.NewDummyStoreFromFolder(tempDir, t)

	uc := userConfig(dimensions/10, 5, 32, 64, 32, vectorsSize)
	cfg := indexConfig("recovery_test", tempDir, logger, vectors, dist)

	// Phase 1: build index and compress.
	index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.NoError(t, err)

	require.NoError(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(vectorsSize), func(id uint64) error {
		return index.Add(ctx, uint64(id), vectors[id])
	}))
	require.NoError(t, index.compress(uc))

	// Record the entrypoint and search results before shutdown.
	entrypoint := index.entryPointID
	control, _, err := index.SearchByVector(ctx, queries[0], vectorsSize, nil)
	require.NoError(t, err)

	require.NoError(t, index.Flush())
	require.NoError(t, index.Shutdown(ctx))
	dummyStore.FlushMemtables(ctx)

	// Delete the entrypoint's compressed vector from the bucket to simulate
	// an interrupted compression preload.
	bucket := dummyStore.Bucket(helpers.GetCompressedBucketName("recovery_test"))
	require.NotNil(t, bucket)
	idBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(idBytes, entrypoint)
	require.NoError(t, bucket.Delete(idBytes))
	dummyStore.FlushMemtables(ctx)

	// Phase 2: restart — the entrypoint's compressed vector is missing.
	restartCfg := indexConfig("recovery_test", tempDir, logger, vectors, dist)
	restartCfg.WaitForCachePrefill = true

	index, err = New(restartCfg, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
	require.NoError(t, err)
	defer index.Shutdown(ctx)

	index.PostStartup(ctx)

	require.True(t, index.compressed.Load(), "index should be compressed")

	// Search should succeed — the entrypoint's compressed vector is recovered
	// on first access via the vectorForID fallback.
	results, _, err := index.SearchByVector(ctx, queries[0], vectorsSize, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, control, results, "search results should match after entrypoint recovery")
}

// Compression drops the float32 vector cache, and recovering a missing
// compressed vector reads through it. The read must serve the vector without
// regrowing the dropped cache.
func Test_CompressedVectorRecoveryBypassesFloatCache(t *testing.T) {
	dimensions := 20
	vectorsSize := 50
	vectors, _ := testinghelpers.RandomVecs(vectorsSize, 0, dimensions)
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	withCompression := func(enable func(*ent.UserConfig)) ent.UserConfig {
		uc := userConfig(dimensions/10, 5, 32, 64, 32, vectorsSize)
		uc.PQ.Enabled = false
		enable(&uc)
		return uc
	}
	tests := []struct {
		name string
		uc   ent.UserConfig
	}{
		{name: "PQ", uc: withCompression(func(uc *ent.UserConfig) { uc.PQ.Enabled = true })},
		{name: "SQ", uc: withCompression(func(uc *ent.UserConfig) { uc.SQ = ent.SQConfig{Enabled: true} })},
		{name: "BQ", uc: withCompression(func(uc *ent.UserConfig) { uc.BQ.Enabled = true })},
		{name: "RQ 8 bits", uc: withCompression(func(uc *ent.UserConfig) { uc.RQ = ent.RQConfig{Enabled: true, Bits: 8} })},
		{name: "RQ 1 bit", uc: withCompression(func(uc *ent.UserConfig) { uc.RQ = ent.RQConfig{Enabled: true, Bits: 1} })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			store := testinghelpers.NewDummyStoreFromFolder(tempDir, t)
			// readErrs holds the error the object store returns per id
			var readErrs sync.Map
			cfg := indexConfig("recovery", tempDir, logger, vectors, distancer.NewCosineDistanceProvider())
			cfg.WaitForCachePrefill = true
			cfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
				if err, ok := readErrs.Load(id); ok {
					return nil, err.(error)
				}
				if int(id) >= len(vectors) {
					return nil, storobj.NewErrNotFoundf(id, "deleted")
				}
				return vectors[id], nil
			}

			index, err := New(cfg, withCompression(func(*ent.UserConfig) {}), cyclemanager.NewCallbackGroupNoop(), store)
			require.NoError(t, err)
			floatCache := index.cache
			require.NoError(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(vectorsSize), func(id uint64) error {
				return index.Add(ctx, id, vectors[id])
			}))
			compressed := make(chan struct{})
			require.NoError(t, index.UpdateUserConfig(tt.uc, func() { close(compressed) }))
			<-compressed
			require.True(t, index.compressed.Load())
			require.NotNil(t, index.cache, "a reader racing the compressed flag must find a dropped cache, not nil")
			requireRecoveryBypassesFloatCache(t, index, floatCache, &readErrs, 1, 2, 3)

			require.NoError(t, index.Flush())
			require.NoError(t, index.Shutdown(ctx))
			store.FlushMemtables(ctx)

			index, err = New(cfg, tt.uc, cyclemanager.NewCallbackGroupNoop(), store)
			require.NoError(t, err)
			defer index.Shutdown(ctx)
			index.PostStartup(ctx)
			require.True(t, index.compressed.Load())
			require.Nil(t, index.cache, "a restored compressed index holds no float32 cache")
			requireRecoveryBypassesFloatCache(t, index, nil, &readErrs, 4, 5, 6)
		})
	}
}

// requireRecoveryBypassesFloatCache drops the compressed vectors of a live, a
// deleted and an unreadable node, then reads each back through the
// compressor's recovery.
func requireRecoveryBypassesFloatCache(t *testing.T, index *hnsw, floatCache cache.Cache[float32],
	readErrs *sync.Map, live, gone, unreadable uint64,
) {
	t.Helper()
	ctx := context.Background()
	errStore := errors.New("object store read failed")
	want, err := index.compressor.GetCompressed(live)
	require.NoError(t, err)
	readErrs.Store(gone, storobj.NewErrNotFoundf(gone, "deleted"))
	readErrs.Store(unreadable, errStore)
	for _, id := range []uint64{live, gone, unreadable} {
		index.compressor.Delete(ctx, id)
	}

	got, err := index.compressor.GetCompressed(live)
	require.NoError(t, err)
	require.Equal(t, want, got, "recovery must encode the normalized vector, as insert does")

	var notFound storobj.ErrNotFound
	_, err = index.compressor.GetCompressed(gone)
	require.ErrorAs(t, err, &notFound)
	_, err = index.compressor.GetCompressed(unreadable)
	require.ErrorIs(t, err, errStore, "a failed read must not look like a deleted object")

	// a recovery that went through a float32 cache would serve the vector again
	readErrs.Store(live, storobj.NewErrNotFoundf(live, "deleted"))
	index.compressor.Delete(ctx, live)
	_, err = index.compressor.GetCompressed(live)
	require.ErrorAs(t, err, &notFound)

	if floatCache != nil {
		require.Zero(t, floatCache.Len(), "the dropped float32 cache must stay empty")
	}
}

// A multivector or muvera BQ index compresses in New on every start.
// Recovering a node's missing compressed vector must read that node's own vector.
func Test_MultiVectorBQRecoveryOnRestart(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	// BQ keeps only signs, so doc 0's token encodes differently from doc 1's
	docs := [][][]float32{
		{{-1, -2, -3}},
		{{1, 2, 3}, {3, 2, 1}},
	}
	tests := []struct {
		name   string
		muvera ent.MuveraConfig
		// node returns the node holding doc 1's first token, or doc 1's
		// muvera encoding when muvera is on
		node func(*hnsw) uint64
	}{
		{
			name: "multivector",
			node: func(index *hnsw) uint64 { return index.docIDVectors[1][0] },
		},
		{
			name:   "muvera",
			muvera: ent.MuveraConfig{Enabled: true, KSim: 2, DProjections: 3, Repetitions: 5},
			node:   func(*hnsw) uint64 { return 1 },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			store := testinghelpers.NewDummyStoreFromFolder(tempDir, t)
			id := "vectors_multi_bq"
			cfg := Config{
				RootPath: tempDir,
				ID:       id,
				MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
					return NewCommitLogger(tempDir, id, logger, cyclemanager.NewCallbackGroupNoop(), opts...)
				},
				DistanceProvider: distancer.NewDotProductProvider(),
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return nil, errors.New("multivector index reads MultiVectorForIDThunk")
				},
				MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
					return docs[id], nil
				},
				GetViewThunk:        func() common.BucketView { return &noopBucketView{} },
				MakeBucketOptions:   lsmkv.MakeNoopBucketOptions,
				AllocChecker:        memwatch.NewDummyMonitor(),
				WaitForCachePrefill: true,
			}
			uc := ent.UserConfig{
				VectorCacheMaxObjects: 1e12,
				MaxConnections:        8,
				EFConstruction:        64,
				EF:                    64,
				Multivector:           ent.MultivectorConfig{Enabled: true, MuveraConfig: tt.muvera},
				BQ:                    ent.BQConfig{Enabled: true},
			}

			index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), store)
			require.NoError(t, err)
			for docID, vecs := range docs {
				require.NoError(t, index.AddMulti(ctx, uint64(docID), vecs))
			}
			node := tt.node(index)
			want, err := index.compressor.GetCompressed(node)
			require.NoError(t, err)
			require.NoError(t, index.Flush())
			require.NoError(t, index.Shutdown(ctx))
			store.FlushMemtables(ctx)

			bucket := store.Bucket(helpers.GetCompressedBucketName("multi_bq"))
			require.NotNil(t, bucket)
			idBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(idBytes, node)
			require.NoError(t, bucket.Delete(idBytes))
			store.FlushMemtables(ctx)

			index, err = New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), store)
			require.NoError(t, err)
			defer index.Shutdown(ctx)
			index.PostStartup(ctx)

			got, err := index.compressor.GetCompressed(node)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// A distancer built before compression keeps reading through the float32
// path. Compression drops that cache; the straggler must not bring it back.
func Test_StragglerReadAfterCompressionLeavesCacheDropped(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	t.Run("single vector reads through to the object store", func(t *testing.T) {
		dimensions, vectorsSize := 20, 50
		vectors, _ := testinghelpers.RandomVecs(vectorsSize, 0, dimensions)
		tempDir := t.TempDir()
		store := testinghelpers.NewDummyStoreFromFolder(tempDir, t)
		cfg := indexConfig("straggler", tempDir, logger, vectors, distancer.NewCosineDistanceProvider())
		cfg.WaitForCachePrefill = true
		uc := userConfig(dimensions/10, 5, 32, 64, 32, vectorsSize)
		uc.PQ.Enabled = false

		index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), store)
		require.NoError(t, err)
		defer index.Shutdown(ctx)
		floatCache := index.cache
		for id := range vectors {
			require.NoError(t, index.Add(ctx, uint64(id), vectors[id]))
		}
		straggler := index.QueryVectorDistancer(vectors[0])
		want, err := straggler.DistanceFunc(1)
		require.NoError(t, err)

		uc.BQ.Enabled = true
		compressed := make(chan struct{})
		require.NoError(t, index.UpdateUserConfig(uc, func() { close(compressed) }))
		<-compressed
		require.Zero(t, floatCache.Len())

		got, err := straggler.DistanceFunc(1)
		require.NoError(t, err)
		assert.InDelta(t, want, got, 1e-6, "a straggler still reads the right vector")
		assert.Zero(t, floatCache.Len(), "but must not bring the dropped cache back")
		assert.Zero(t, floatCache.CountVectors())
	})

	t.Run("multivector fails without looking deleted", func(t *testing.T) {
		docs := [][][]float32{
			{{-1, -2, -3}},
			{{1, 2, 3}, {3, 2, 1}},
		}
		tempDir := t.TempDir()
		store := testinghelpers.NewDummyStoreFromFolder(tempDir, t)
		id := "straggler_multi"
		cfg := Config{
			RootPath: tempDir,
			ID:       id,
			MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
				return NewCommitLogger(tempDir, id, logger, cyclemanager.NewCallbackGroupNoop(), opts...)
			},
			DistanceProvider: distancer.NewDotProductProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return nil, errors.New("multivector index reads MultiVectorForIDThunk")
			},
			MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
				return docs[id], nil
			},
			GetViewThunk:        func() common.BucketView { return &noopBucketView{} },
			MakeBucketOptions:   lsmkv.MakeNoopBucketOptions,
			AllocChecker:        memwatch.NewDummyMonitor(),
			WaitForCachePrefill: true,
		}
		uc := ent.UserConfig{
			VectorCacheMaxObjects: 1e12,
			MaxConnections:        8,
			EFConstruction:        64,
			EF:                    64,
			Multivector:           ent.MultivectorConfig{Enabled: true},
		}

		index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), store)
		require.NoError(t, err)
		defer index.Shutdown(ctx)
		floatCache := index.cache
		for docID, vecs := range docs {
			require.NoError(t, index.AddMulti(ctx, uint64(docID), vecs))
		}
		node := index.docIDVectors[1][0]
		straggler := index.QueryVectorDistancer(docs[1][0])
		_, err = straggler.DistanceFunc(node)
		require.NoError(t, err)

		uc.BQ.Enabled = true
		compressed := make(chan struct{})
		require.NoError(t, index.UpdateUserConfig(uc, func() { close(compressed) }))
		<-compressed
		require.Zero(t, floatCache.Len())

		// the keys a read needs went with the cache, so there is nothing left
		// to read through to; a not-found here would tombstone a live node
		_, err = straggler.DistanceFunc(node)
		require.ErrorIs(t, err, cache.ErrDropped)
		var notFound storobj.ErrNotFound
		assert.False(t, errors.As(err, &notFound))
		assert.Zero(t, floatCache.Len(), "the straggler must not bring the dropped cache back")
		assert.Zero(t, floatCache.CountVectors())
	})
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
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return NewCommitLogger(tempDir, correctedID, logger, cyclemanager.NewCallbackGroupNoop(), opts...)
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
	makeCommitLogger := func(opts ...CommitlogOption) (CommitLogger, error) {
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
	makeCommitLogger := func(opts ...CommitlogOption) (CommitLogger, error) {
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
	makeCommitLogger := func(opts ...CommitlogOption) (CommitLogger, error) {
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
	makeCommitLogger := func(opts ...CommitlogOption) (CommitLogger, error) {
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
	makeCommitLogger := func(opts ...CommitlogOption) (CommitLogger, error) {
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

func Test_NoRaceRQCompressAndPrefillCache(t *testing.T) {
	dimensions := 64
	vectorsSize := 100
	vectors, _ := testinghelpers.RandomVecs(vectorsSize, 0, dimensions)
	dist := distancer.NewL2SquaredProvider()
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.RQ = ent.RQConfig{Enabled: true, Bits: 1}
	uc.VectorCacheMaxObjects = 1e12

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "rq-cache-race",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
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
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Shutdown(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		compressionhelpers.Concurrently(logger, uint64(vectorsSize), func(i uint64) {
			index.Add(ctx, i, vectors[i])
		})
	}, logger)

	index.PostStartup(ctx)
	wg.Wait()
}

func Test_CompressSkipsTargetVectorNotFound(t *testing.T) {
	tests := []struct {
		name  string
		pqCfg ent.PQConfig
		sqCfg ent.SQConfig
	}{
		{
			name: "PQ",
			pqCfg: ent.PQConfig{
				Enabled: true,
				Encoder: ent.PQEncoder{
					Type:         ent.PQEncoderTypeKMeans,
					Distribution: ent.PQEncoderDistributionLogNormal,
				},
				TrainingLimit: 75,
				Segments:      10,
				Centroids:     5,
			},
		},
		{
			name: "SQ",
			sqCfg: ent.SQConfig{
				Enabled:       true,
				TrainingLimit: 75,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dimensions := 20
			vectorsSize := 100
			vectors, _ := testinghelpers.RandomVecs(vectorsSize, 0, dimensions)
			dist := distancer.NewL2SquaredProvider()
			logger, _ := test.NewNullLogger()
			ctx := context.Background()

			uc := ent.UserConfig{}
			uc.MaxConnections = 32
			uc.EFConstruction = 64
			uc.EF = 32
			uc.VectorCacheMaxObjects = 10e12
			uc.PQ = tc.pqCfg
			uc.SQ = tc.sqCfg

			// Half of the IDs return ErrNotFound to simulate objects where
			// the target vector is missing (the shard converts
			// ErrTargetVectorNotFound to ErrNotFound).
			index, err := New(Config{
				RootPath:              t.TempDir(),
				ID:                    "target_vector_not_found",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      dist,
				AllocChecker:          memwatch.NewDummyMonitor(),
				MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					if int(id) >= len(vectors) {
						return nil, storobj.NewErrNotFoundf(id, "out of range")
					}
					if id%2 == 0 {
						return nil, storobj.NewErrNotFoundf(id, "target vector %q not found", "test_vector")
					}
					return vectors[int(id)], nil
				},
				GetViewThunk: func() common.BucketView {
					return &noopBucketView{}
				},
				TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
					if id%2 == 0 {
						return nil, storobj.NewErrNotFoundf(id, "target vector %q not found", "test_vector")
					}
					copy(container.Slice, vectors[int(id)])
					return container.Slice, nil
				},
			}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
			require.NoError(t, err)
			defer index.Shutdown(context.Background())

			require.NoError(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(len(vectors)), func(id uint64) error {
				return index.Add(ctx, uint64(id), vectors[id])
			}))

			err = index.compress(uc)
			require.NoError(t, err, "compress should skip objects with missing target vectors")
			assert.True(t, index.compressed.Load(), "index should be compressed")
		})
	}
}

// TestChildLoggersCarryTheIndexID pins that the logger hnsw hands to the
// components it builds (here: the compressor) already carries hnsw's OWN
// index_id, overriding whatever id the caller's logger had. The shard hands
// hfresh's centroid graph a logger identified as the parent index; a
// compressor line still labeled with the parent id would misattribute
// centroid maintenance. The compressor's recovery path is the one child line
// that can be provoked deterministically: a compressed entry that is missing
// while the bucket is read-only.
func TestChildLoggersCarryTheIndexID(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	const parentID, ownID = "vectors_title", "vectors_title_centroids"
	vectors := [][]float32{{1, 0, 0, 0}}
	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.BQ = ent.BQConfig{Enabled: true}

	store := testinghelpers.NewDummyStore(t)
	index, err := New(Config{
		RootPath:         t.TempDir(),
		ID:               ownID,
		Logger:           logger.WithField("index_id", parentID), // what the shard hands a centroid graph
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		AllocChecker:     memwatch.NewDummyMonitor(),
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return MakeNoopCommitLogger()
		},
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vectors[id], nil
		},
		GetViewThunk:                 func() common.BucketView { return &noopBucketView{} },
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Shutdown(context.Background())

	require.NoError(t, index.Add(context.Background(), 0, vectors[0]))

	// provoke the compressor's recovery log line: drop the compressed vector
	// from the compressor's cache and bucket, then make the bucket read-only
	// so the recovery's write-back is skipped and logged
	index.compressor.Delete(context.Background(), 0)
	bucket := store.Bucket(helpers.GetCompressedBucketName("title_centroids"))
	require.NotNil(t, bucket)
	bucket.UpdateStatus(storagestate.StatusReadOnly)
	defer bucket.UpdateStatus(storagestate.StatusReady)

	_, err = index.compressor.NewDistancerFromID(0)
	require.NoError(t, err)

	var seen bool
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] != "recover_compressed_vector" {
			continue
		}
		seen = true
		require.Equalf(t, ownID, entry.Data["index_id"], "line %q", entry.Message)
	}
	require.True(t, seen, "the compressor's recovery line was not logged")
}

// TestCompressedBucketNameDerivesFromID pins the quantized bucket for every
// ID shape the shard builds: the legacy asymmetry (ID "main", bucket
// "vectors_compressed"), a named vector, and hfresh's centroid graph.
func TestCompressedBucketNameDerivesFromID(t *testing.T) {
	tests := []struct {
		id   string
		want string
	}{
		{"main", "vectors_compressed"},
		{"vectors_title", "vectors_compressed_title"},
		{"vectors_title_centroids", "vectors_compressed_title_centroids"},
	}
	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			vectors := [][]float32{{1, 0, 0, 0}}
			uc := ent.UserConfig{}
			uc.SetDefaults()
			uc.BQ = ent.BQConfig{Enabled: true}

			store := testinghelpers.NewDummyStore(t)
			index, err := New(Config{
				RootPath:         t.TempDir(),
				ID:               tt.id,
				DistanceProvider: distancer.NewCosineDistanceProvider(),
				AllocChecker:     memwatch.NewDummyMonitor(),
				MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
					return MakeNoopCommitLogger()
				},
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[id], nil
				},
				GetViewThunk:                 func() common.BucketView { return &noopBucketView{} },
				TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
				MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
			}, uc, cyclemanager.NewCallbackGroupNoop(), store)
			require.NoError(t, err)
			defer index.Shutdown(context.Background())

			require.NoError(t, index.Add(context.Background(), 0, vectors[0]))

			assert.Equal(t, tt.want, index.compressedBucketName())
			assert.NotNil(t, store.Bucket(tt.want), "the compressor must write to %q", tt.want)
		})
	}
}

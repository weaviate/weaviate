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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// mockCompressorDistancer returns ErrNotFound for deleted nodes.
type mockCompressorDistancer struct {
	deleted  map[uint64]bool
	vectors  [][]float32
	queryVec []float32
}

func (m *mockCompressorDistancer) DistanceToNode(id uint64) (float32, error) {
	if m.deleted[id] {
		return 0, storobj.NewErrNotFoundf(id, "deleted")
	}
	vec := m.vectors[id]
	var dist float32
	for i := range vec {
		d := vec[i] - m.queryVec[i]
		dist += d * d
	}
	return dist, nil
}

func (m *mockCompressorDistancer) DistanceToFloat(vec []float32) (float32, error) {
	var dist float32
	for i := range vec {
		d := vec[i] - m.queryVec[i]
		dist += d * d
	}
	return dist, nil
}

// mockVectorCompressor implements VectorCompressor for testing.
type mockVectorCompressor struct {
	deleted map[uint64]bool
	vectors [][]float32
}

func (m *mockVectorCompressor) NewDistancer(q []float32) (compressionhelpers.CompressorDistancer, compressionhelpers.ReturnDistancerFn) {
	return &mockCompressorDistancer{deleted: m.deleted, vectors: m.vectors, queryVec: q}, func() {}
}

func (m *mockVectorCompressor) Drop() error                                            { return nil }
func (m *mockVectorCompressor) GrowCache(uint64)                                       {}
func (m *mockVectorCompressor) SetCacheMaxSize(int64)                                  {}
func (m *mockVectorCompressor) GetCacheMaxSize() int64                                 { return 0 }
func (m *mockVectorCompressor) Delete(context.Context, uint64)                         {}
func (m *mockVectorCompressor) Preload(uint64, []float32)                              {}
func (m *mockVectorCompressor) PreloadMulti(uint64, []uint64, [][]float32)             {}
func (m *mockVectorCompressor) PreloadPassage(uint64, uint64, uint64, []float32)       {}
func (m *mockVectorCompressor) GetKeys(uint64) (uint64, uint64)                        { return 0, 0 }
func (m *mockVectorCompressor) SetKeys(uint64, uint64, uint64)                         {}
func (m *mockVectorCompressor) Prefetch(uint64)                                        {}
func (m *mockVectorCompressor) CountVectors() int64                                    { return int64(len(m.vectors)) }
func (m *mockVectorCompressor) MaxVectorID() uint64                                    { return uint64(len(m.vectors)) }
func (m *mockVectorCompressor) Len() int32                                             { return int32(len(m.vectors)) }
func (m *mockVectorCompressor) PrefillCache(context.Context)                           {}
func (m *mockVectorCompressor) PrefillMultiCache(context.Context, map[uint64][]uint64) {}
func (m *mockVectorCompressor) DistanceBetweenCompressedVectorsFromIDs(context.Context, uint64, uint64) (float32, error) {
	return 0, nil
}

func (m *mockVectorCompressor) NewDistancerFromID(uint64) (compressionhelpers.CompressorDistancer, error) {
	return nil, nil
}
func (m *mockVectorCompressor) NewBag() compressionhelpers.CompressionDistanceBag  { return nil }
func (m *mockVectorCompressor) PersistCompression(compressionhelpers.CommitLogger) {}
func (m *mockVectorCompressor) Stats() compressionhelpers.CompressionStats         { return nil }
func (m *mockVectorCompressor) Get(id uint64) ([]float32, error)                   { return m.vectors[id], nil }
func (m *mockVectorCompressor) GetCompressed(id uint64) (any, error)               { return nil, nil }

// TestSearchWithMissingEntrypoint is a behavioral regression test.
// Without fix: search returns error "entrypoint was deleted..."
// With fix: search finds fallback and returns valid results.
func TestSearchWithMissingEntrypoint(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{100, 100}, {1, 1}}

	logger, _ := test.NewNullLogger()
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		Logger:                logger,
		VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)

	// Setup: node 0 is entry point (deleted), node 1 is valid fallback
	index.entryPointID = 0
	index.currentMaximumLayer = 1
	conns0, _ := packedconn.NewWithElements([][]uint64{{1}, {1}})
	conns1, _ := packedconn.NewWithElements([][]uint64{{0}, {0}})
	index.nodes = []*vertex{
		{id: 0, level: 1, connections: conns0},
		{id: 1, level: 1, connections: conns1},
	}

	// Enable compressed mode with mock compressor that returns ErrNotFound for node 0
	index.compressed.Store(true)
	index.compressor = &mockVectorCompressor{
		deleted: map[uint64]bool{0: true},
		vectors: vectors,
	}

	results, err := index.KnnSearchByVectorMaxDist(ctx, []float32{1, 1}, 1000, 100, nil)

	require.NoError(t, err, "search should not fail when entrypoint is missing")
	require.NotEmpty(t, results, "should return results via fallback")
	assert.Equal(t, uint64(1), results[0])
	assert.True(t, index.hasTombstone(0), "deleted entrypoint should be tombstoned")
}

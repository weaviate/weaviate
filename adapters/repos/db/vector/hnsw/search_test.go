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
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// prevents a regression of
// https://github.com/weaviate/weaviate/issues/2155
func TestNilCheckOnPartiallyCleanedNode(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{
		{100, 100}, // first to import makes this the EP, it is far from any query which means it will be replaced.
		{2, 2},     // a good potential entrypoint, but we will corrupt it later on
		{1, 1},     // the perfect search result
	}

	var vectorIndex *hnsw

	t.Run("import", func(*testing.T) {
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "bug-2155",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, ent.UserConfig{
			MaxConnections: 30,
			EFConstruction: 128,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)
		vectorIndex = index
	})

	t.Run("manually add the nodes", func(t *testing.T) {
		vectorIndex.entryPointID = 0
		vectorIndex.currentMaximumLayer = 1
		vectorIndex.nodes = []*vertex{
			{
				// must be on a non-zero layer for this bug to occur
				level: 1,
				connections: [][]uint64{
					{1, 2},
					{1},
				},
			},
			nil, // corrupt node
			{
				level: 0,
				connections: [][]uint64{
					{0, 1, 2},
				},
			},
		}
	})

	t.Run("run a search that would typically find the new ep", func(t *testing.T) {
		res, _, err := vectorIndex.SearchByVector(ctx, []float32{1.7, 1.7}, 20, nil)
		require.Nil(t, err)
		assert.Equal(t, []uint64{2, 0}, res, "right results are found")
	})

	t.Run("the corrupt node is now marked deleted", func(t *testing.T) {
		_, ok := vectorIndex.tombstones[1]
		assert.True(t, ok)
	})
}

func TestRescore(t *testing.T) {
	type test struct {
		name        string
		concurrency int
		k           int
		objects     int
	}

	tests := []test{
		{
			name:        "single-threaded, limit < objects",
			concurrency: 1,
			k:           10,
			objects:     100,
		},
		{
			name:        "two threads, limit < objects",
			concurrency: 2,
			k:           10,
			objects:     50,
		},
		{
			name:        "more threads than objects",
			concurrency: 60,
			k:           10,
			objects:     50,
		},
		{
			name:        "result limit above objects with no concurrency",
			concurrency: 1,
			k:           60,
			objects:     50,
		},
		{
			name:        "result limit above objects with low concurrency",
			concurrency: 4,
			k:           60,
			objects:     50,
		},
		{
			name:        "result limit above objects with high concurrency",
			concurrency: 100,
			k:           60,
			objects:     50,
		},
	}

	logger := logrus.New()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vectors, queries := testinghelpers.RandomVecs(test.objects, 1, 128)

			d := distancer.NewDotProductProvider()
			distFn := func(a, b []float32) float32 {
				dist, _ := d.SingleDist(a, b)
				return dist
			}
			ids, _ := testinghelpers.BruteForce(logger, vectors, queries[0], test.k, distFn)

			res := priorityqueue.NewMax[any](test.k)
			// insert with random distances, so the result can't possibly be correct
			// without re-ranking
			for i := 0; i < test.objects; i++ {
				res.Insert(uint64(i), rand.Float32())
			}

			h := &hnsw{
				rescoreConcurrency: test.concurrency,
				logger:             logger,
				TempVectorForIDThunk: func(
					ctx context.Context, id uint64, container *common.VectorSlice,
				) ([]float32, error) {
					return vectors[id], nil
				},
				pools:             newPools(32),
				distancerProvider: d,
			}

			compDistancer := newFakeCompressionDistancer(queries[0], distFn)
			h.rescore(res, test.k, compDistancer)

			resultIDs := make([]uint64, res.Len())
			for res.Len() > 0 {
				item := res.Pop()
				resultIDs[res.Len()] = item.ID
			}

			assert.Equal(t, ids, resultIDs)
		})
	}
}

type fakeCompressionDistancer struct {
	queryVec []float32
	distFn   func(a, b []float32) float32
}

func newFakeCompressionDistancer(queryVec []float32, distFn func(a, b []float32) float32) *fakeCompressionDistancer {
	return &fakeCompressionDistancer{
		distFn:   distFn,
		queryVec: queryVec,
	}
}

func (f *fakeCompressionDistancer) DistanceToNode(id uint64) (float32, error) {
	panic("not implemented")
}

func (f *fakeCompressionDistancer) DistanceToFloat(vec []float32) (float32, error) {
	return f.distFn(f.queryVec, vec), nil
}

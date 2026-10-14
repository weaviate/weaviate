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
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
	"github.com/weaviate/weaviate/usecases/memwatch"
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
			AllocChecker:          memwatch.NewDummyMonitor(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			GetViewThunk: func() common.BucketView { return &noopBucketView{} },
		}, ent.UserConfig{
			MaxConnections: 30,
			EFConstruction: 128,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)
		vectorIndex = index
	})

	t.Run("manually add the nodes", func(t *testing.T) {
		vectorIndex.entryPointID = 0
		vectorIndex.currentMaximumLayer = 1
		conns1, _ := packedconn.NewWithElements([][]uint64{
			{1, 2},
			{1},
		})
		conns2, _ := packedconn.NewWithElements([][]uint64{
			{0, 1, 2},
		})
		vectorIndex.nodes = []*vertex{
			{
				// must be on a non-zero layer for this bug to occur
				level:       1,
				connections: *conns1,
			},
			nil, // corrupt node
			{
				level:       0,
				connections: *conns2,
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

func TestQueryVectorDistancer(t *testing.T) {
	vectors := [][]float32{
		{100, 100}, // first to import makes this the EP, it is far from any query which means it will be replaced.
		{2, 2},     // a good potential entrypoint, but we will corrupt it later on
		{1, 1},     // the perfect search result
	}

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "bug-2155",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 128,

		// The actual size does not matter for this test, but if it defaults to
		// zero it will constantly think it's full and needs to be deleted - even
		// after just being deleted, so make sure to use a positive number here.
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	index.Add(context.TODO(), uint64(0), []float32{-1, 0})

	dist := index.QueryVectorDistancer([]float32{0, 0})
	require.NotNil(t, dist)
	distance, err := dist.DistanceToNode(0)
	require.Nil(t, err)
	require.Equal(t, distance, float32(1.))

	// get distance for non-existing node above default cache size
	_, err = dist.DistanceToNode(1001)
	require.NotNil(t, err)
}

func TestQueryMultiVectorDistancer(t *testing.T) {
	vectors := [][][]float32{
		{{0.3, 0.1}, {1, 0}},
	}

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "bug-2155",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewDotProductProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[0][int(id)], nil
		},
		MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
			return vectors[int(id)], nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		GetViewThunk:      func() common.BucketView { return &noopBucketView{} },
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: 100000,
		Multivector: ent.MultivectorConfig{
			Enabled: true,
		},
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	index.AddMulti(context.TODO(), uint64(0), vectors[0])

	dist := index.QueryMultiVectorDistancer([][]float32{{0.2, 0}, {1, 0}})
	require.NotNil(t, dist)
	distance, err := dist.DistanceToNode(0)
	require.Nil(t, err)
	require.Equal(t, float32(-1.2), distance)

	// get distance for non-existing node
	_, err = dist.DistanceToNode(1032)
	require.NotNil(t, err)
}

func TestAcornPercentage(t *testing.T) {
	vectors, _ := testinghelpers.RandomVecs(10, 1, 3)
	var vectorIndex *hnsw

	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(context.Background())

	t.Run("import test vectors", func(t *testing.T) {
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "delete-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			AllocChecker:          memwatch.NewDummyMonitor(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			GetViewThunk:                 func() common.BucketView { return &noopBucketView{} },
			TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
			AcornFilterRatio:             0.4,
		}, ent.UserConfig{
			MaxConnections:        16,
			EFConstruction:        16,
			VectorCacheMaxObjects: 1000,
		}, cyclemanager.NewCallbackGroupNoop(), store)
		require.Nil(t, err)
		vectorIndex = index

		for i, vec := range vectors {
			err := vectorIndex.Add(context.TODO(), uint64(i), vec)
			require.Nil(t, err)
		}
	})

	t.Run("check acorn params on different filter percentags", func(t *testing.T) {
		vectorIndex.configuredFilterStrategy.Store(int32(SWEEPING))
		allowList := helpers.NewAllowList(1, 2, 3)
		useAcorn := vectorIndex.acornEnabled(allowList)
		assert.False(t, useAcorn)

		vectorIndex.configuredFilterStrategy.Store(int32(ACORN))

		useAcorn = vectorIndex.acornEnabled(allowList)
		assert.True(t, useAcorn)

		vectorIndex.configuredFilterStrategy.Store(int32(ACORN))

		largerAllowList := helpers.NewAllowList(1, 2, 3, 4, 5)
		useAcorn = vectorIndex.acornEnabled(largerAllowList)
		// should be false as allow list percentage is 50%
		assert.False(t, useAcorn)
	})
}

func TestRescore(t *testing.T) {
	for _, contextCancelled := range []bool{false, true} {
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
			name := fmt.Sprintf("%s, context cancelled: %v", test.name, contextCancelled)
			t.Run(name, func(t *testing.T) {
				vectors, queries := testinghelpers.RandomVecs(test.objects, 1, 128)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

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
					allocChecker:       memwatch.NewDummyMonitor(),
					GetViewThunk: func() common.BucketView {
						return &noopBucketView{}
					},
					TempVectorForIDWithViewThunk: func(
						ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView,
					) ([]float32, error) {
						return vectors[id], nil
					},
					pools:             newPools(32, 1),
					distancerProvider: d,
				}

				compDistancer := newFakeCompressionDistancer(queries[0], distFn)
				if contextCancelled {
					cancel()
				}
				err := h.rescore(ctx, res, test.k, compDistancer)

				if contextCancelled {
					assert.True(t, errors.Is(err, context.Canceled))
				} else {
					resultIDs := make([]uint64, res.Len())
					for res.Len() > 0 {
						item := res.Pop()
						resultIDs[res.Len()] = item.ID
					}

					assert.Equal(t, ids, resultIDs)
				}
			})
		}
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

func (f *fakeCompressionDistancer) DistancesToNodes(ids []uint64, dists []float32) []error {
	panic("not implemented")
}

func (f *fakeCompressionDistancer) DistanceToFloat(vec []float32) (float32, error) {
	return f.distFn(f.queryVec, vec), nil
}

// noopBucketView is a no-op implementation of common.BucketView for testing
type noopBucketView struct{}

func (n *noopBucketView) ReleaseView() {}

// TestShouldRescore pins that rescoring is gated the same way for every hnsw
// quantizer: a compressed index rescores unless the active quantizer's
// rescoreLimit is 0; automatic (-1) rescores with the quantizer's default.
func TestShouldRescore(t *testing.T) {
	type test struct {
		name       string
		compressed bool
		configure  func(h *hnsw)
		expected   bool
	}

	quantizers := map[string]func(h *hnsw, limit int){
		"pq": func(h *hnsw, limit int) { h.pqConfig = ent.PQConfig{Enabled: true, RescoreLimit: limit} },
		"bq": func(h *hnsw, limit int) { h.bqConfig = ent.BQConfig{Enabled: true, RescoreLimit: limit} },
		"sq": func(h *hnsw, limit int) { h.sqConfig = ent.SQConfig{Enabled: true, RescoreLimit: limit} },
		"rq": func(h *hnsw, limit int) { h.rqConfig = ent.RQConfig{Enabled: true, Bits: 8, RescoreLimit: limit} },
		"rq centered": func(h *hnsw, limit int) {
			h.rqConfig = ent.RQConfig{Enabled: true, Bits: 4, Centering: true, RescoreLimit: limit}
		},
	}

	var tests []test
	for name, configure := range quantizers {
		for _, limit := range []int{ent.RescoreLimitAuto, ent.RescoreLimitDisabled, 1, 20, 512} {
			tests = append(tests, test{
				name:       fmt.Sprintf("%s limit %d compressed", name, limit),
				compressed: true,
				configure:  func(h *hnsw) { configure(h, limit) },
				expected:   limit != ent.RescoreLimitDisabled,
			}, test{
				name:       fmt.Sprintf("%s limit %d uncompressed", name, limit),
				compressed: false,
				configure:  func(h *hnsw) { configure(h, limit) },
				expected:   false,
			})
		}
	}
	tests = append(tests, test{
		name:       "compressed without a quantizer in the config",
		compressed: true,
		configure:  func(h *hnsw) {},
		expected:   true,
	})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := &hnsw{}
			h.compressed.Store(test.compressed)
			test.configure(h)
			assert.Equal(t, test.expected, h.shouldRescore())
		})
	}
}

// TestRescoreLimitBoundsCandidates pins the rescore budget shared by every
// hnsw quantizer: of the candidates the graph search returned, exactly
// min(candidates, max(k, rescoreLimit)) are read back uncompressed, and the
// result is the exact top k of that set. A limit below k must not fall back to
// rescoring every candidate, and must not return fewer than k results either.
// Automatic resolves to the quantizer's own default before the bound applies.
func TestRescoreLimitBoundsCandidates(t *testing.T) {
	type test struct {
		name      string
		configure func(h *hnsw, limit int)
		// limit is what the config carries, effective what it resolves to
		limit      int
		effective  int
		k          int
		candidates int
		// keepAll marks the compressed-without-quantizer case, which rescores
		// every candidate regardless of limit
		keepAll bool
	}

	quantizers := []struct {
		name      string
		configure func(h *hnsw, limit int)
		auto      int
	}{
		{"pq", func(h *hnsw, limit int) { h.pqConfig = ent.PQConfig{Enabled: true, RescoreLimit: limit} }, ent.DefaultPQRescoreLimit},
		{"bq", func(h *hnsw, limit int) { h.bqConfig = ent.BQConfig{Enabled: true, RescoreLimit: limit} }, ent.DefaultBQRescoreLimit},
		{"sq", func(h *hnsw, limit int) { h.sqConfig = ent.SQConfig{Enabled: true, RescoreLimit: limit} }, ent.DefaultSQRescoreLimit},
		{"rq 8 bit", func(h *hnsw, limit int) { h.rqConfig = ent.RQConfig{Enabled: true, Bits: 8, RescoreLimit: limit} }, ent.DefaultRQRescoreLimit},
		{"rq 1 bit", func(h *hnsw, limit int) { h.rqConfig = ent.RQConfig{Enabled: true, Bits: 1, RescoreLimit: limit} }, ent.DefaultBRQRescoreLimit},
	}
	shapes := []struct {
		name       string
		limit      int
		k          int
		candidates int
	}{
		{name: "limit above k trims to limit", limit: 20, k: 10, candidates: 100},
		{name: "limit below k rescores exactly k", limit: 20, k: 30, candidates: 100},
		{name: "limit equal to k", limit: 10, k: 10, candidates: 100},
		{name: "limit above candidates keeps all", limit: 512, k: 10, candidates: 100},
		{name: "k above candidates keeps all", limit: 20, k: 150, candidates: 100},
		{name: "automatic resolves to the quantizer default", limit: ent.RescoreLimitAuto, k: 10, candidates: 100},
		{name: "automatic with k above the default rescores k", limit: ent.RescoreLimitAuto, k: 60, candidates: 100},
	}

	var tests []test
	for _, q := range quantizers {
		for _, shape := range shapes {
			limit := shape.limit
			if limit == ent.RescoreLimitAuto {
				limit = q.auto
			}
			tests = append(tests, test{
				name:       q.name + " " + shape.name,
				configure:  q.configure,
				limit:      shape.limit,
				effective:  limit,
				k:          shape.k,
				candidates: shape.candidates,
			})
		}
	}
	tests = append(tests, test{
		name:       "no quantizer in config keeps every candidate",
		configure:  func(h *hnsw, limit int) {},
		limit:      20,
		k:          10,
		candidates: 100,
		keepAll:    true,
	})

	logger := logrus.New()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vectors, queries := testinghelpers.RandomVecs(test.candidates, 1, 32)
			d := distancer.NewL2SquaredProvider()
			distFn := func(a, b []float32) float32 {
				dist, _ := d.SingleDist(a, b)
				return dist
			}

			// the compressed distance of candidate i is i, so the best
			// candidates by compressed distance are the lowest ids
			res := priorityqueue.NewMax[any](test.candidates)
			for i := 0; i < test.candidates; i++ {
				res.Insert(uint64(i), float32(i))
			}

			expectedReads := min(test.candidates, max(test.k, test.effective))
			if test.keepAll {
				expectedReads = test.candidates
			}
			expectedIDs, _ := testinghelpers.BruteForce(logger, vectors[:expectedReads], queries[0],
				min(test.k, expectedReads), distFn)

			var reads atomic.Int64
			h := &hnsw{
				rescoreConcurrency: 4,
				logger:             logger,
				allocChecker:       memwatch.NewDummyMonitor(),
				GetViewThunk:       func() common.BucketView { return &noopBucketView{} },
				TempVectorForIDWithViewThunk: func(
					ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView,
				) ([]float32, error) {
					reads.Add(1)
					return vectors[id], nil
				},
				pools:             newPools(32, 1),
				distancerProvider: d,
			}
			test.configure(h, test.limit)

			err := h.rescore(context.Background(), res, test.k, newFakeCompressionDistancer(queries[0], distFn))
			require.NoError(t, err)
			assert.Equal(t, int64(expectedReads), reads.Load(), "uncompressed vector reads")

			resultIDs := make([]uint64, res.Len())
			for res.Len() > 0 {
				item := res.Pop()
				resultIDs[res.Len()] = item.ID
			}
			assert.Equal(t, expectedIDs, resultIDs)
		})
	}
}

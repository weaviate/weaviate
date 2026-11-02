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
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"

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

type rescoreQuantizer struct {
	name       string
	configure  func(uc *ent.UserConfig, limit int, factor float64)
	autoLimit  int
	autoFactor float64
}

var rescoreQuantizers = []rescoreQuantizer{
	{"pq", func(uc *ent.UserConfig, limit int, factor float64) {
		uc.PQ.Enabled = true
		uc.PQ.Segments = rescoreTestDims / 2
		uc.PQ.Centroids = 16
		uc.PQ.RescoreLimit = limit
		uc.PQ.RescoreFactor = factor
	}, ent.DefaultPQRescoreLimit, ent.DefaultPQRescoreFactor},
	{"bq", func(uc *ent.UserConfig, limit int, factor float64) {
		uc.BQ.Enabled = true
		uc.BQ.RescoreLimit = limit
		uc.BQ.RescoreFactor = factor
	}, ent.DefaultBQRescoreLimit, ent.DefaultBQRescoreFactor},
	{"sq", func(uc *ent.UserConfig, limit int, factor float64) {
		uc.SQ.Enabled = true
		uc.SQ.RescoreLimit = limit
		uc.SQ.RescoreFactor = factor
	}, ent.DefaultSQRescoreLimit, ent.DefaultSQRescoreFactor},
	{"rq-8", func(uc *ent.UserConfig, limit int, factor float64) {
		uc.RQ.Enabled = true
		uc.RQ.Bits = 8
		uc.RQ.RescoreLimit = limit
		uc.RQ.RescoreFactor = factor
	}, ent.DefaultRQRescoreLimit, ent.DefaultRQRescoreFactor},
	{"rq-1", func(uc *ent.UserConfig, limit int, factor float64) {
		uc.RQ.Enabled = true
		uc.RQ.Bits = 1
		uc.RQ.RescoreLimit = limit
		uc.RQ.RescoreFactor = factor
	}, ent.DefaultBRQRescoreLimit, ent.DefaultBRQRescoreFactor},
}

const (
	rescoreTestEF   = 64
	rescoreTestDims = 32
)

// newRescoreCountingIndex builds an index compressed with q and counts how
// many uncompressed vectors searches read back. The search ef is fixed, so
// the graph search returns ef candidates for every k up to ef.
func newRescoreCountingIndex(t *testing.T, q rescoreQuantizer) (*hnsw, *ent.UserConfig, *atomic.Int64, [][]float32) {
	t.Helper()
	const vectorsNum = 2000

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	vectors, queries := testinghelpers.RandomVecs(vectorsNum, 1, rescoreTestDims)
	testinghelpers.Normalize(vectors)
	testinghelpers.Normalize(queries)

	uc := ent.NewDefaultUserConfig()
	uc.MaxConnections = 16
	uc.EFConstruction = 64
	uc.EF = rescoreTestEF
	uc.VectorCacheMaxObjects = 10e12

	reads := &atomic.Int64{}
	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "rescore-" + q.name,
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[id], nil
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			reads.Add(1)
			copy(container.Slice, vectors[id])
			return container.Slice, nil
		},
		GetViewThunk:      func() common.BucketView { return &noopBucketView{} },
		AllocChecker:      memwatch.NewDummyMonitor(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = index.Shutdown(context.Background()) })
	index.PostStartup(ctx)

	require.NoError(t, compressionhelpers.ConcurrentlyWithError(logger, vectorsNum, func(id uint64) error {
		return index.Add(ctx, id, vectors[id])
	}))

	q.configure(&uc, ent.RescoreLimitAuto, ent.RescoreFactorAuto)
	updateConfig(t, index, uc)
	require.True(t, index.Compressed(), "index must be compressed with %s", q.name)

	return index, &uc, reads, queries
}

func updateConfig(t *testing.T, index *hnsw, uc ent.UserConfig) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, index.UpdateUserConfig(uc, wg.Done))
	wg.Wait()
}

// TestRescorePerQuantizer counts how many uncompressed vectors a search reads
// back with each hnsw quantizer: it must be
// min(ef, max(k, rescoreLimit, ceil(k * rescoreFactor))), or none when the
// limit is 0. Automatic values resolve to the quantizer's defaults first.
func TestRescorePerQuantizer(t *testing.T) {
	const ef = rescoreTestEF
	auto := ent.RescoreFactorAuto

	shapes := []struct {
		name   string
		k      int
		limit  int
		factor float64
	}{
		{name: "limit above scaled k reads limit", k: 10, limit: 20, factor: 1.5},
		{name: "scaled k above limit reads scaled k", k: 16, limit: 20, factor: 3},
		{name: "fractional factor rounds up", k: 15, limit: 1, factor: 1.5},
		{name: "factor 1 with limit below k reads k", k: 30, limit: 20, factor: 1},
		{name: "automatic factor above limit", k: 30, limit: 20, factor: auto},
		{name: "automatic factor at small k", k: 10, limit: 20, factor: auto},
		{name: "limit above ef reads ef", k: 10, limit: 512, factor: auto},
		{name: "scaled k above ef reads ef", k: 40, limit: 1, factor: 4},
		{name: "k at ef reads ef", k: ef, limit: 20, factor: 1},
		{name: "limit 0 disables rescoring", k: 10, limit: ent.RescoreLimitDisabled, factor: 4},
		{name: "automatic limit and factor", k: 10, limit: ent.RescoreLimitAuto, factor: auto},
		{name: "automatic limit and factor at large k", k: 48, limit: ent.RescoreLimitAuto, factor: auto},
	}

	for _, q := range rescoreQuantizers {
		t.Run(q.name, func(t *testing.T) {
			index, uc, reads, queries := newRescoreCountingIndex(t, q)
			ctx := context.Background()

			for _, shape := range shapes {
				t.Run(shape.name, func(t *testing.T) {
					q.configure(uc, shape.limit, shape.factor)
					updateConfig(t, index, *uc)

					reads.Store(0)
					ids, _, err := index.SearchByVector(ctx, queries[0], shape.k, nil)
					require.NoError(t, err)
					require.Len(t, ids, shape.k)

					limit, factor := shape.limit, shape.factor
					if limit == ent.RescoreLimitAuto {
						limit = q.autoLimit
					}
					if factor == ent.RescoreFactorAuto {
						factor = q.autoFactor
					}
					expected := min(ef, max(shape.k, limit, int(math.Ceil(float64(shape.k)*factor))))
					if limit == ent.RescoreLimitDisabled {
						expected = 0
					}
					assert.Equal(t, int64(expected), reads.Load(),
						fmt.Sprintf("%s k=%d limit=%d factor=%v: uncompressed reads",
							q.name, shape.k, shape.limit, shape.factor))
				})
			}
		})
	}
}

// TestRescoreConfigUpdateDuringSearch changes the rescore settings while
// searches run. Searches read the settings on every query, so the update must
// publish them without a data race (run with -race).
func TestRescoreConfigUpdateDuringSearch(t *testing.T) {
	q := rescoreQuantizers[2] // sq
	index, uc, _, queries := newRescoreCountingIndex(t, q)
	ctx := context.Background()

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			ids, _, err := index.SearchByVector(ctx, queries[0], 10, nil)
			assert.NoError(t, err)
			assert.Len(t, ids, 10)
		}
	}()

	for i := 0; i < 200; i++ {
		q.configure(uc, 20+i%2, float64(1+i%3))
		updateConfig(t, index, *uc)
	}
	close(done)
	wg.Wait()
}

// TestRescoreLegacyConfigAtIndex builds an index from the config an upgraded
// collection carries: the exact JSON older versions stored. PQ and BQ have no
// rescoreLimit key, SQ and RQ keep the number older versions stored, and none
// has a rescoreFactor key. Missing keys must resolve to the quantizer's
// defaults rather than to disabled or zero.
func TestRescoreLegacyConfigAtIndex(t *testing.T) {
	tests := []struct {
		name     string
		legacy   string
		expected ent.RescorePolicy
	}{
		{
			name:     "pq",
			legacy:   `{"pq":{"enabled":true,"bitCompression":false,"segments":0,"centroids":256,"trainingLimit":100000,"encoder":{"type":"kmeans","distribution":"log-normal"}}}`,
			expected: ent.RescorePolicy{Limit: ent.DefaultPQRescoreLimit, Factor: ent.DefaultPQRescoreFactor},
		},
		{
			name:     "bq",
			legacy:   `{"bq":{"enabled":true}}`,
			expected: ent.RescorePolicy{Limit: ent.DefaultBQRescoreLimit, Factor: ent.DefaultBQRescoreFactor},
		},
		{
			name:     "sq",
			legacy:   `{"sq":{"enabled":true,"trainingLimit":100000,"rescoreLimit":20}}`,
			expected: ent.RescorePolicy{Limit: 20, Factor: ent.DefaultSQRescoreFactor},
		},
		{
			name:     "rq-8",
			legacy:   `{"rq":{"enabled":true,"bits":8,"rescoreLimit":20}}`,
			expected: ent.RescorePolicy{Limit: 20, Factor: ent.DefaultRQRescoreFactor},
		},
		{
			name:     "rq-1",
			legacy:   `{"rq":{"enabled":true,"bits":1,"rescoreLimit":512}}`,
			expected: ent.RescorePolicy{Limit: 512, Factor: ent.DefaultBRQRescoreFactor},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var asMap map[string]interface{}
			require.NoError(t, json.Unmarshal([]byte(test.legacy), &asMap))
			parsed, err := ent.ParseAndValidateConfig(asMap, false)
			require.NoError(t, err)
			uc := parsed.(ent.UserConfig)

			vectors, _ := testinghelpers.RandomVecs(10, 1, 8)
			index, err := New(Config{
				RootPath:              t.TempDir(),
				ID:                    "legacy-" + test.name,
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      distancer.NewCosineDistanceProvider(),
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[id], nil
				},
				GetViewThunk:      func() common.BucketView { return &noopBucketView{} },
				AllocChecker:      memwatch.NewDummyMonitor(),
				MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
			}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
			require.NoError(t, err)
			t.Cleanup(func() { _ = index.Shutdown(context.Background()) })

			assert.Equal(t, test.expected, index.rescorePolicy())
			if index.Compressed() {
				assert.True(t, index.shouldRescore(), "a compressed legacy index must still rescore")
			}
		})
	}
}

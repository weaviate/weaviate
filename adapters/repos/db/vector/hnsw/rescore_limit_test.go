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

// TestRescoreLimitPerQuantizer compresses a real index with each hnsw
// quantizer and counts how many uncompressed vectors a search reads back. The
// search ef is fixed and larger than every k, so the graph search always
// returns ef candidates and the rescore must read exactly
// min(ef, max(k, rescoreLimit)) of them, or none when the limit is 0.
// Automatic resolves to the quantizer's default before the bound applies.
func TestRescoreLimitPerQuantizer(t *testing.T) {
	const (
		ef         = 64
		dimensions = 32
		vectorsNum = 2000
	)

	quantizers := []struct {
		name      string
		configure func(uc *ent.UserConfig, limit int)
		auto      int
	}{
		{"pq", func(uc *ent.UserConfig, limit int) {
			uc.PQ.Enabled = true
			uc.PQ.Segments = dimensions / 2
			uc.PQ.Centroids = 16
			uc.PQ.RescoreLimit = limit
		}, ent.DefaultPQRescoreLimit},
		{"bq", func(uc *ent.UserConfig, limit int) {
			uc.BQ.Enabled = true
			uc.BQ.RescoreLimit = limit
		}, ent.DefaultBQRescoreLimit},
		{"sq", func(uc *ent.UserConfig, limit int) {
			uc.SQ.Enabled = true
			uc.SQ.RescoreLimit = limit
		}, ent.DefaultSQRescoreLimit},
		{"rq", func(uc *ent.UserConfig, limit int) {
			uc.RQ.Enabled = true
			uc.RQ.Bits = 8
			uc.RQ.RescoreLimit = limit
		}, ent.DefaultRQRescoreLimit},
	}

	shapes := []struct {
		name  string
		k     int
		limit int
	}{
		{name: "limit above k reads limit", k: 10, limit: 20},
		{name: "limit below k reads k not ef", k: 30, limit: 20},
		{name: "limit above ef reads ef", k: 10, limit: 512},
		{name: "k at ef reads ef", k: ef, limit: 20},
		{name: "limit 0 disables rescoring", k: 10, limit: ent.RescoreLimitDisabled},
		{name: "automatic reads the quantizer default", k: 10, limit: ent.RescoreLimitAuto},
	}

	for _, q := range quantizers {
		name, configure := q.name, q.configure
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			logger, _ := test.NewNullLogger()
			vectors, queries := testinghelpers.RandomVecs(vectorsNum, 1, dimensions)
			testinghelpers.Normalize(vectors)
			testinghelpers.Normalize(queries)

			uc := ent.NewDefaultUserConfig()
			uc.MaxConnections = 16
			uc.EFConstruction = 64
			uc.EF = ef
			uc.VectorCacheMaxObjects = 10e12

			var reads atomic.Int64
			index, err := New(Config{
				RootPath:              t.TempDir(),
				ID:                    "rescore-limit-" + name,
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

			configure(&uc, ent.DefaultSQRescoreLimit)
			var wg sync.WaitGroup
			wg.Add(1)
			require.NoError(t, index.UpdateUserConfig(uc, wg.Done))
			wg.Wait()
			require.True(t, index.Compressed(), "index must be compressed with %s", name)

			for _, shape := range shapes {
				t.Run(shape.name, func(t *testing.T) {
					configure(&uc, shape.limit)
					wg.Add(1)
					require.NoError(t, index.UpdateUserConfig(uc, wg.Done))
					wg.Wait()

					reads.Store(0)
					ids, _, err := index.SearchByVector(ctx, queries[0], shape.k, nil)
					require.NoError(t, err)
					require.Len(t, ids, shape.k)

					limit := shape.limit
					if limit == ent.RescoreLimitAuto {
						limit = q.auto
					}
					expected := min(ef, max(shape.k, limit))
					if limit == ent.RescoreLimitDisabled {
						expected = 0
					}
					assert.Equal(t, int64(expected), reads.Load(),
						fmt.Sprintf("%s k=%d limit=%d: uncompressed reads", name, shape.k, shape.limit))
				})
			}
		})
	}
}

// TestRescoreLimitLegacyConfigAtIndex builds an index from the config an
// upgraded PQ or BQ collection carries: the exact JSON older versions stored,
// with no rescoreLimit key. The index must resolve it to the quantizer's
// default rather than treat the missing key as disabled.
func TestRescoreLimitLegacyConfigAtIndex(t *testing.T) {
	tests := []struct {
		name     string
		legacy   string
		expected int
	}{
		{
			name:     "pq",
			legacy:   `{"pq":{"enabled":true,"bitCompression":false,"segments":0,"centroids":256,"trainingLimit":100000,"encoder":{"type":"kmeans","distribution":"log-normal"}}}`,
			expected: ent.DefaultPQRescoreLimit,
		},
		{
			name:     "bq",
			legacy:   `{"bq":{"enabled":true}}`,
			expected: ent.DefaultBQRescoreLimit,
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

			assert.Equal(t, test.expected, index.rescoreLimit())
			if index.Compressed() {
				assert.True(t, index.shouldRescore(), "a compressed legacy index must still rescore")
			}
		})
	}
}

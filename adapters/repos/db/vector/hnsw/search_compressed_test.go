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
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
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

type compressionCase struct {
	name     string
	compress func(uc *ent.UserConfig)
}

var compressionCases = []compressionCase{
	{name: "uncompressed"},
	{name: "rq-1", compress: func(uc *ent.UserConfig) { uc.RQ = ent.RQConfig{Enabled: true, Bits: 1, RescoreLimit: 20} }},
	{name: "rq-4", compress: func(uc *ent.UserConfig) { uc.RQ = ent.RQConfig{Enabled: true, Bits: 4, RescoreLimit: 20} }},
	{name: "rq-8", compress: func(uc *ent.UserConfig) { uc.RQ = ent.RQConfig{Enabled: true, Bits: 8, RescoreLimit: 20} }},
	{name: "bq", compress: func(uc *ent.UserConfig) { uc.BQ = ent.BQConfig{Enabled: true} }},
	{name: "sq", compress: func(uc *ent.UserConfig) {
		uc.SQ = ent.SQConfig{Enabled: true, TrainingLimit: 500, RescoreLimit: 20}
	}},
	{name: "pq", compress: func(uc *ent.UserConfig) {
		uc.PQ = ent.PQConfig{
			Enabled: true, Segments: 8, Centroids: 256, TrainingLimit: 500,
			Encoder: ent.PQEncoder{Type: ent.PQEncoderTypeKMeans, Distribution: ent.PQEncoderDistributionLogNormal},
		}
	}},
}

// newIndexForCompressionCase imports vectors and then compresses the index as tt asks.
func newIndexForCompressionCase(t *testing.T, tt compressionCase, vectors [][]float32) *hnsw {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.VectorCacheMaxObjects = 10e12

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "compression-case",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
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
		GetViewThunk:      func() common.BucketView { return &noopBucketView{} },
		AllocChecker:      memwatch.NewDummyMonitor(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = index.Shutdown(context.Background()) })
	index.PostStartup(ctx)

	require.NoError(t, compressionhelpers.ConcurrentlyWithError(logger, uint64(len(vectors)), func(id uint64) error {
		return index.Add(ctx, id, vectors[id])
	}))
	if tt.compress != nil {
		tt.compress(&uc)
		var wg sync.WaitGroup
		wg.Add(1)
		require.NoError(t, index.UpdateUserConfig(uc, wg.Done))
		wg.Wait()
		require.True(t, index.Compressed())
	}
	return index
}

// A query vector of the wrong length must fail with ErrVectorLength whether
// or not the index is compressed. Compressed indexes used to drop every
// candidate while rescoring and return no hits without an error.
func TestSearchRejectsQueryOfWrongDimensions(t *testing.T) {
	ctx := context.Background()
	dimensions := 64
	vectors, _ := testinghelpers.RandomVecs(600, 0, dimensions)
	testinghelpers.Normalize(vectors)

	for _, tt := range compressionCases {
		t.Run(tt.name, func(t *testing.T) {
			index := newIndexForCompressionCase(t, tt, vectors)

			allow := helpers.NewAllowList()
			for id := uint64(0); id < 50; id++ {
				allow.Insert(id)
			}
			for _, search := range []struct {
				name  string
				allow helpers.AllowList
			}{{"graph search", nil}, {"flat search", allow}} {
				t.Run(search.name, func(t *testing.T) {
					ids, _, err := index.SearchByVector(ctx, vectors[0], 10, search.allow)
					require.NoError(t, err)
					require.NotEmpty(t, ids)

					for _, wrong := range [][]float32{vectors[0][:dimensions-1], append(vectors[0][:dimensions:dimensions], 1)} {
						_, _, err := index.SearchByVector(ctx, wrong, 10, search.allow)
						require.ErrorIs(t, err, distancer.ErrVectorLength)
					}
				})
			}
		})
	}
}

// Multi-target search fills in the distance of a result one target did not
// return with QueryVectorDistancer. That distance must be the one a search
// reports for the same object; a compressed index used to hand out the
// quantized estimate, so a result's distance depended on which targets found it.
func TestQueryVectorDistancerMatchesSearchDistance(t *testing.T) {
	ctx := context.Background()
	vectors, queries := testinghelpers.RandomVecs(600, 1, 64)
	testinghelpers.Normalize(vectors)

	for _, tt := range compressionCases {
		t.Run(tt.name, func(t *testing.T) {
			index := newIndexForCompressionCase(t, tt, vectors)

			ids, dists, err := index.SearchByVector(ctx, queries[0], 10, nil)
			require.NoError(t, err)
			require.NotEmpty(t, ids)

			distancer := index.QueryVectorDistancer(queries[0])
			defer distancer.Close()
			for i, id := range ids {
				got, err := distancer.DistanceToNode(id)
				require.NoError(t, err)
				require.InDelta(t, dists[i], got, 1e-5, "id %d", id)
			}
		})
	}
}

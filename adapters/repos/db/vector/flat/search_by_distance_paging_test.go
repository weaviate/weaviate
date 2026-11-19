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

package flat

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

// A search by distance asks for a first page of results and, while the page
// comes back full and within the distance, for a larger one. The loop that
// asked for the larger page never searched again: a search by distance
// returned its first page (100 results) and no more, and with no maximum (the
// -1 an aggregation without an object limit passes) it never returned.
func TestFlatSearchByVectorDistance(t *testing.T) {
	const (
		vectors = 250
		// step is the angle between a vector and the next: the vector i is at
		// i steps of the query, so its cosine distance grows with i.
		step = 0.005
		page = common.DefaultSearchByDistInitialLimit
	)
	ctx := context.Background()
	index := createTestIndex(t)
	for i := range vectors {
		angle := float64(i) * step
		vector := []float32{float32(math.Cos(angle)), float32(math.Sin(angle))}
		require.NoError(t, index.Add(ctx, uint64(i), vector))
	}
	query := []float32{1, 0}
	// within is the distance that takes in the first n vectors and no other.
	within := func(n int) float32 {
		return float32(1 - math.Cos((float64(n)-0.5)*step))
	}

	tests := []struct {
		name     string
		distance float32
		maxLimit int64
		want     int
	}{
		{name: "less than a page", distance: within(50), maxLimit: 10_000, want: 50},
		{name: "a full page and no more", distance: within(page), maxLimit: 10_000, want: page},
		{name: "the distance ends in the second page", distance: within(180), maxLimit: 10_000, want: 180},
		{name: "every vector, with a maximum", distance: 2, maxLimit: 10_000, want: vectors},
		{name: "every vector, without a maximum", distance: 2, maxLimit: -1, want: vectors},
		{name: "the distance ends in the second page, without a maximum", distance: within(180), maxLimit: -1, want: 180},
		// The maximum is checked before a larger page is asked for: the
		// second page would be of 1100, over the maximum, so the first is
		// what comes back. HNSW does the same.
		{name: "the next page is over the maximum", distance: 2, maxLimit: 150, want: page},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				ids   []uint64
				dists []float32
				err   error
			)
			done := make(chan struct{})
			enterrors.GoWrapper(func() {
				defer close(done)
				ids, dists, err = index.SearchByVectorDistance(ctx, query, tt.distance, tt.maxLimit, nil)
			}, index.logger)
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatal("the search by distance did not return")
			}

			require.NoError(t, err)
			require.Len(t, ids, tt.want)
			require.Len(t, dists, tt.want)
			for i, id := range ids {
				require.Equal(t, uint64(i), id, "result %d", i)
				require.LessOrEqual(t, dists[i], tt.distance+1e-6, "result %d", i)
			}
		})
	}
}

// A search by distance kept the pages it had and added what the larger search
// returned past them. A compressed index rescores more candidates when it is
// asked for more results, so the first results of a larger page are not those
// of the page before it, and where the two disagreed an id came back twice
// and another was left out. The answer is now that of one search: the last
// one.
func TestFlatSearchByVectorDistanceCompressed(t *testing.T) {
	const (
		vectors = 3000
		dim     = 128
		queries = 10
		page    = common.DefaultSearchByDistInitialLimit
		// the first page and the one after it, the last under the maximum
		secondPage = page + page*common.DefaultSearchByDistLimitMultiplier
		// farther than any vector
		anyDistance = float32(2)
	)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	randomVector := func() []float32 {
		vector := make([]float32, dim)
		for i := range vector {
			vector[i] = rng.Float32()*2 - 1
		}
		return vector
	}
	stored := make([][]float32, vectors)
	for i := range stored {
		stored[i] = randomVector()
	}
	probes := make([][]float32, queries)
	for i := range probes {
		probes[i] = randomVector()
	}

	tests := []struct {
		name      string
		configure func(uc *flatent.UserConfig)
	}{
		{name: "bq", configure: func(uc *flatent.UserConfig) { uc.BQ.Enabled = true }},
		{name: "bq, cached", configure: func(uc *flatent.UserConfig) { uc.BQ.Enabled, uc.BQ.Cache = true, true }},
		{name: "rq, 1 bit", configure: func(uc *flatent.UserConfig) { uc.RQ.Enabled, uc.RQ.Bits = true, 1 }},
		{name: "rq, 8 bits", configure: func(uc *flatent.UserConfig) { uc.RQ.Enabled, uc.RQ.Bits = true, 8 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dirName := t.TempDir()
			logger, _ := test.NewNullLogger()
			store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Shutdown(context.Background())) })

			uc := flatent.NewDefaultUserConfig()
			tt.configure(&uc)
			index, err := New(Config{
				ID:                uuid.New().String(),
				DistanceProvider:  distancer.NewCosineDistanceProvider(),
				RootPath:          dirName,
				MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
			}, uc, store)
			require.NoError(t, err)
			for i, vector := range stored {
				require.NoError(t, index.Add(ctx, uint64(i), vector))
			}
			require.True(t, index.Compressed())

			for _, probe := range probes {
				// with a maximum, the answer is that of the search of the
				// second page
				want, wantDists, err := index.SearchByVector(ctx, probe, secondPage, nil)
				require.NoError(t, err)
				require.Len(t, want, secondPage)
				ids, dists, err := index.SearchByVectorDistance(ctx, probe, anyDistance, 10_000, nil)
				require.NoError(t, err)
				requireNoIDTwice(t, ids)
				require.Equal(t, want, ids)
				require.Equal(t, wantDists, dists)

				// without one, every vector, as a search that asks for all of
				// them returns them
				want, wantDists, err = index.SearchByVector(ctx, probe, vectors, nil)
				require.NoError(t, err)
				require.Len(t, want, vectors)
				ids, dists, err = index.SearchByVectorDistance(ctx, probe, anyDistance, -1, nil)
				require.NoError(t, err)
				requireNoIDTwice(t, ids)
				require.Equal(t, want, ids)
				require.Equal(t, wantDists, dists)
			}
		})
	}
}

func requireNoIDTwice(t *testing.T, ids []uint64) {
	t.Helper()
	seen := make(map[uint64]int, len(ids))
	for i, id := range ids {
		first, ok := seen[id]
		require.False(t, ok, "id %d is result %d and result %d", id, first, i)
		seen[id] = i
	}
}

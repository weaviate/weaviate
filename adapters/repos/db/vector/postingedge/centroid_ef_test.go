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

package postingedge

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// setCentroidEF changes the ef of the centroid graph search of the built
// index, which is otherwise the constant centroidEF. It is how the benchmark
// measures lower efs on one build (-bench.centroidefs); it is not a setting of
// the index.
func (ix *Index) setCentroidEF(ef int) error {
	st := ix.state.Load()
	if st == nil {
		return errors.New("posting-edge index is not built")
	}
	return st.centroids.graph.UpdateUserConfig(centroidUserConfig(ef), func() {})
}

// exactEntries returns the centroid nearest to each query, by brute force.
func exactEntries(st *state, queries [][]float32) []int {
	l2 := distancer.NewL2SquaredProvider()
	nearest := make([]int, len(queries))
	err := parallelChunks(context.Background(), testLogger(), len(queries), 4, func(_, start, end int) error {
		for i := start; i < end; i++ {
			best, bestDist := 0, float32(math.Inf(1))
			for c, centroid := range st.centroids.vectors {
				dist, err := l2.SingleDist(queries[i], centroid)
				if err != nil {
					return err
				}
				if dist < bestDist {
					best, bestDist = c, dist
				}
			}
			nearest[i] = best
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return nearest
}

// testLogger discards what it is given.
func testLogger() logrus.FieldLogger {
	logger, _ := logrustest.NewNullLogger()
	return logger
}

// entryAccuracy is the share of the queries for which the centroid graph
// search, as it is set now, returns the exact nearest centroid (exact, from
// exactEntries).
func entryAccuracy(st *state, queries [][]float32, exact []int) float64 {
	var hits atomic.Int64
	err := parallelChunks(context.Background(), testLogger(), len(queries), 4, func(_, start, end int) error {
		for i := start; i < end; i++ {
			got, err := st.centroids.nearest(context.Background(), queries[i], 1)
			if err != nil {
				return err
			}
			if len(got) == 1 && got[0] == uint64(exact[i]) {
				hits.Add(1)
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return float64(hits.Load()) / float64(len(queries))
}

// The ef of the centroid graph search can be changed on a built index: every
// ef still gives one entry and a full result, and an ef that covers every
// centroid finds the exact nearest one for every query.
func TestSetCentroidEF(t *testing.T) {
	ctx := context.Background()
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(3000, 24, 30, 911)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, provider, uc)
	st := index.state.Load()
	centroids := len(st.centroids.vectors)
	require.Greater(t, centroids, 20, "too few postings to search a graph over")

	queries := clusteredVectors(300, 24, 30, 912)
	exact := exactEntries(st, queries)

	tests := []struct {
		name  string
		ef    int
		exact bool // every entry must be the nearest centroid
	}{
		{name: "ef of one", ef: 1},
		{name: "ef of four", ef: 4},
		{name: "the default", ef: centroidEF},
		{name: "ef that covers every centroid", ef: centroids, exact: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, index.setCentroidEF(tt.ef))
			for _, q := range queries[:20] {
				entry, err := st.centroids.nearest(ctx, q, 1)
				require.NoError(t, err)
				require.Len(t, entry, 1)
				ids, _, err := index.SearchByVector(ctx, q, 10, nil)
				require.NoError(t, err)
				require.Len(t, ids, 10)
			}
			accuracy := entryAccuracy(st, queries, exact)
			assert.Greater(t, accuracy, 0.0)
			if tt.exact {
				assert.Equal(t, 1.0, accuracy)
			}
		})
	}

	t.Run("an index that is not built", func(t *testing.T) {
		empty, err := New(testConfig(t, vectors, 0, provider), uc)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, empty.Shutdown(context.Background())) })
		require.ErrorContains(t, empty.setCentroidEF(16), "not built")
	})
}

// Searches running while the ef changes see one ef or the other, never a
// broken search (run with -race).
func TestSetCentroidEFDuringSearches(t *testing.T) {
	ctx := context.Background()
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(3000, 24, 30, 913)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, provider, uc)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				ids, _, err := index.SearchByVector(ctx, vectors[(w*997+i)%len(vectors)], 10, nil)
				if err == nil && len(ids) != 10 {
					err = errors.Errorf("%d results", len(ids))
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	for round := 0; round < 200; round++ {
		require.NoError(t, index.setCentroidEF([]int{1, 8, 64}[round%3]))
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NoError(t, index.setCentroidEF(centroidEF))
}

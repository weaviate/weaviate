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
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/storobj"
)

// The rescore reads the exact vector of every candidate from wherever the
// owner of the index keeps them. A vector that is not found there is of an
// object that was deleted after the index took it in: the candidate is left
// out and the search goes on, as in HNSW and HFresh. It used to fail the
// whole search, so that one delete broke every query that came near it. Any
// other failure to read a vector still fails the search.
func TestRescoreLeavesOutVectorsThatAreGone(t *testing.T) {
	t.Parallel()
	const n, k = 80, 10
	all := clusteredVectors(n+5, 24, 3, 2718)
	vectors, queries := all[:n], all[n:]
	truth := normalizedCopy(vectors)
	provider := distancer.NewCosineDistanceProvider()
	diskFailure := errors.New("the disk is on fire")

	tests := []struct {
		name string
		// read tells what reading the vector of a doc gives: nil to read it.
		read    func(id uint64) error
		wantErr error
	}{
		{name: "every vector is there", read: func(uint64) error { return nil }},
		{name: "one of three is gone", read: func(id uint64) error {
			if id%3 == 0 {
				return storobj.NewErrNotFoundf(id, "deleted")
			}
			return nil
		}},
		{name: "a wrapped not found", read: func(id uint64) error {
			if id%2 == 1 {
				return errors.Wrap(storobj.NewErrNotFoundf(id, "deleted"), "read the object")
			}
			return nil
		}},
		{name: "all but a few are gone", read: func(id uint64) error {
			if id >= 4 {
				return storobj.NewErrNotFoundf(id, "deleted")
			}
			return nil
		}},
		{name: "every vector is gone", read: func(id uint64) error {
			return storobj.NewErrNotFoundf(id, "deleted")
		}},
		{name: "a read fails", wantErr: diskFailure, read: func(id uint64) error {
			if id%50 == 7 {
				return diskFailure
			}
			return nil
		}},
		{name: "a read fails among vectors that are gone", wantErr: diskFailure, read: func(id uint64) error {
			if id%50 == 7 {
				return diskFailure
			}
			return storobj.NewErrNotFoundf(id, "deleted")
		}},
	}
	// The rescore reads one by one or from several goroutines, by the budget
	// of the query.
	budgets := []struct {
		name string
		ctx  context.Context
	}{
		{"one reader", concurrency.CtxWithBudget(context.Background(), 1)},
		{"several readers", context.Background()},
	}

	// One index serves every case: its store reads through the case that
	// runs. The build reads no vector from the store, so every case searches
	// the same index. The cases run one after the other.
	var read atomic.Pointer[func(id uint64) error]
	cfg := testConfig(t, vectors, 0, provider)
	stored := cfg.TempVectorForIDWithViewThunk
	cfg.TempVectorForIDWithViewThunk = func(ctx context.Context, id uint64, container *common.VectorSlice,
		view common.BucketView,
	) ([]float32, error) {
		if err := (*read.Load())(id); err != nil {
			return nil, err
		}
		return stored(ctx, id, container, view)
	}
	// Every posting is an entry and every vector is rescored: the answer is
	// the exact one over the vectors that are there.
	uc := smallConfig()
	uc.Distance = "cosine"
	uc.EntryPostings = n
	uc.RescoreLimit = 2 * n
	index, err := New(cfg, uc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
	require.NoError(t, index.AddBatch(context.Background(), sequentialIDs(n, 0), vectors))
	require.NoError(t, index.Build(context.Background()))

	for _, tt := range tests {
		for _, budget := range budgets {
			t.Run(tt.name+"/"+budget.name, func(t *testing.T) {
				read.Store(&tt.read)

				there := make([][]float32, 0, n)
				ids := make([]uint64, 0, n)
				for id := range uint64(n) {
					if tt.read(id) == nil {
						there, ids = append(there, truth[id]), append(ids, id)
					}
				}
				for _, query := range queries {
					got, dists, err := index.SearchByVector(budget.ctx, query, k, nil)
					if tt.wantErr != nil {
						require.ErrorIs(t, err, tt.wantErr)
						continue
					}
					require.NoError(t, err)
					want := exactNearest(t, there, distancer.Normalize(query), k, -1, provider)
					require.Len(t, got, len(want))
					for i, w := range want {
						require.Equal(t, ids[w.id], got[i], "result %d", i)
						require.InDelta(t, w.dist, dists[i], 1e-5)
					}
				}
			})
		}
	}
}

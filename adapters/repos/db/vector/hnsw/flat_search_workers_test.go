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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

const testFlatSearchConcurrency = 4

func TestScoreCandidatesWorkersFollowBudget(t *testing.T) {
	logger, _ := test.NewNullLogger()
	h := &hnsw{logger: logger, flatSearchConcurrency: testFlatSearchConcurrency}

	tests := []struct {
		name         string
		ctx          context.Context
		limit        int
		wantMax      int
		wantExactMax bool
		wantIDs      []uint64
		hold         time.Duration
	}{
		{
			name: "no budget runs flatSearchConcurrency workers", ctx: context.Background(), limit: 8,
			wantMax: testFlatSearchConcurrency, wantExactMax: true, wantIDs: []uint64{1, 2, 4, 5, 7}, hold: 5 * time.Second,
		},
		{
			name: "budget above flatSearchConcurrency is capped", ctx: concurrency.CtxWithBudget(context.Background(), 100), limit: 8,
			wantMax: testFlatSearchConcurrency, wantExactMax: true, wantIDs: []uint64{1, 2, 4, 5, 7}, hold: 5 * time.Second,
		},
		{
			name: "budget of 2", ctx: concurrency.CtxWithBudget(context.Background(), 2), limit: 8,
			wantMax: 2, wantIDs: []uint64{1, 2, 4, 5, 7}, hold: 20 * time.Millisecond,
		},
		{
			name: "merging workers keeps the limit closest", ctx: context.Background(), limit: 3,
			wantMax: testFlatSearchConcurrency, wantExactMax: true, wantIDs: []uint64{1, 2, 4}, hold: 5 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const nCandidates = 8
			visits := make([]atomic.Int32, nCandidates)
			peak := &peakInFlight{all: testFlatSearchConcurrency, hold: tt.hold}

			results, err := h.scoreCandidates(tt.ctx, candidateRange(nCandidates), tt.limit, func(c uint64) (float32, bool, error) {
				defer peak.enter()()
				visits[c].Add(1)
				return float32(c), c%3 != 0, nil
			})
			require.NoError(t, err)

			peak.assertMax(t, tt.wantMax, tt.wantExactMax)
			for c := range visits {
				assert.Equal(t, int32(1), visits[c].Load(), "candidate %d", c)
			}
			var gotIDs []uint64
			for results.Len() > 0 {
				gotIDs = append(gotIDs, results.Pop().ID)
			}
			assert.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}

func TestFlatSearchWorkersFollowBudget(t *testing.T) {
	tests := []struct {
		name         string
		ctx          context.Context
		wantMax      int
		wantExactMax bool
		hold         time.Duration
	}{
		{name: "budget of 1", ctx: concurrency.CtxWithBudget(context.Background(), 1), wantMax: 1, wantExactMax: true, hold: 20 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const nVectors = 8
			vectors, queries := testinghelpers.RandomVecs(nVectors, 1, 4)
			provider := &peakDistanceProvider{Provider: distancer.NewL2SquaredProvider()}
			index := newFlatSearchIndex(t, provider, vectors, func(_ context.Context, id uint64) ([]float32, error) {
				return vectors[id], nil
			})

			provider.peak.Store(&peakInFlight{all: testFlatSearchConcurrency, hold: tt.hold})
			allowList := helpers.NewAllowList(candidateRange(nVectors)...)
			ids, _, err := index.flatSearch(tt.ctx, queries[0], nVectors, nVectors, allowList)
			require.NoError(t, err)

			provider.peak.Load().assertMax(t, tt.wantMax, tt.wantExactMax)
			assert.ElementsMatch(t, candidateRange(nVectors), ids)
		})
	}
}

func TestFlatSearchSkipsMissingCandidates(t *testing.T) {
	const nVectors, missing = 8, 5
	boom := errors.New("boom")
	tests := []struct {
		name        string
		beyondNodes bool
		lookupErr   error
		wantIDs     []uint64
		wantErr     error
	}{
		{name: "candidate beyond the node array is skipped", beyondNodes: true, wantIDs: candidateRange(nVectors)},
		{
			name: "vector deleted during the search is skipped", lookupErr: storobj.NewErrNotFoundf(missing, "deleted"),
			wantIDs: []uint64{0, 1, 2, 3, 4, 6, 7},
		},
		{name: "vector lookup error is returned", lookupErr: boom, wantErr: boom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vectors, queries := testinghelpers.RandomVecs(nVectors, 1, 4)
			var failMissing atomic.Bool
			index := newFlatSearchIndex(t, distancer.NewL2SquaredProvider(), vectors, func(_ context.Context, id uint64) ([]float32, error) {
				if id == missing && failMissing.Load() {
					return nil, tt.lookupErr
				}
				return vectors[id], nil
			})
			if tt.lookupErr != nil {
				index.cache.Delete(context.Background(), missing)
				failMissing.Store(true)
			}

			candidates := candidateRange(nVectors)
			if tt.beyondNodes {
				candidates = append(candidates, uint64(len(index.nodes)))
			}
			ids, _, err := index.flatSearch(context.Background(), queries[0], len(candidates), len(candidates),
				helpers.NewAllowList(candidates...))

			require.ErrorIs(t, err, tt.wantErr)
			assert.ElementsMatch(t, tt.wantIDs, ids)
		})
	}
}

func TestFlatMultiSearch(t *testing.T) {
	const nDocs, missing = 8, 5
	boom := errors.New("boom")
	tests := []struct {
		name         string
		ctx          context.Context
		lookupErr    error
		wantMax      int
		wantExactMax bool
		wantIDs      []uint64
		wantErr      error
	}{
		{
			name: "budget of 1", ctx: concurrency.CtxWithBudget(context.Background(), 1),
			wantMax: 1, wantExactMax: true, wantIDs: candidateRange(nDocs),
		},
		{
			name: "doc deleted during the search is skipped", ctx: context.Background(),
			lookupErr: storobj.NewErrNotFoundf(missing, "deleted"), wantMax: testFlatSearchConcurrency,
			wantIDs: []uint64{0, 1, 2, 3, 4, 6, 7},
		},
		{
			name: "doc lookup error is returned", ctx: context.Background(),
			lookupErr: boom, wantMax: testFlatSearchConcurrency, wantErr: boom,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vecs, queries := testinghelpers.RandomVecs(2*nDocs, 2, 4)
			docs := make([][][]float32, nDocs)
			for id := range docs {
				docs[id] = vecs[2*id : 2*id+2]
			}
			var peak atomic.Pointer[peakInFlight]
			index := newFlatMultiSearchIndex(t, docs, func(_ context.Context, id uint64) ([][]float32, error) {
				if p := peak.Load(); p != nil {
					defer p.enter()()
					if id == missing && tt.lookupErr != nil {
						return nil, tt.lookupErr
					}
				}
				return docs[id], nil
			})
			// empty the cached vectors but keep their doc keys, so every score reads
			// its doc through the lookup, where peak counts it
			for id := range uint64(nDocs) {
				vecIDs := index.docIDVectors[id]
				index.cache.PreloadMulti(id, vecIDs, make([][]float32, len(vecIDs)))
			}

			hold := 5 * time.Second
			if tt.wantMax < testFlatSearchConcurrency {
				hold = 20 * time.Millisecond
			}
			peak.Store(&peakInFlight{all: testFlatSearchConcurrency, hold: hold})
			ids, _, err := index.flatMultiSearch(tt.ctx, queries, nDocs, helpers.NewAllowList(candidateRange(nDocs)...))

			require.ErrorIs(t, err, tt.wantErr)
			peak.Load().assertMax(t, tt.wantMax, tt.wantExactMax)
			assert.ElementsMatch(t, tt.wantIDs, ids)
		})
	}
}

// newFlatMultiSearchIndex builds an uncompressed multi-vector index holding docs,
// which reads docs missing from its cache through lookup.
func newFlatMultiSearchIndex(t *testing.T, docs [][][]float32, lookup common.VectorForID[[]float32]) *hnsw {
	uc := ent.NewDefaultUserConfig()
	uc.Multivector = ent.MultivectorConfig{Enabled: true}
	index, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              t.TempDir(),
		ID:                    "flat-multi-search",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		ClassName:             "FlatMultiSearch",
		ShardName:             "shard",
		DistanceProvider:      distancer.NewDotProductProvider(),
		FlatSearchConcurrency: testFlatSearchConcurrency,
		VectorForIDThunk: func(context.Context, uint64) ([]float32, error) {
			return nil, errors.New("multi-vector index reads through MultiVectorForIDThunk")
		},
		MultiVectorForIDThunk: lookup,
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	for id := range uint64(len(docs)) {
		require.NoError(t, index.AddMulti(context.Background(), id, docs[id]))
	}
	return index
}

// newFlatSearchIndex builds an uncompressed index holding vectors, which reads
// vectors missing from its cache through lookup.
func newFlatSearchIndex(t *testing.T, provider distancer.Provider, vectors [][]float32,
	lookup common.VectorForID[float32],
) *hnsw {
	index, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              t.TempDir(),
		ID:                    "flat-search-workers",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		ClassName:             "FlatSearchWorkers",
		ShardName:             "shard",
		DistanceProvider:      provider,
		FlatSearchConcurrency: testFlatSearchConcurrency,
		VectorForIDThunk:      lookup,
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
	}, ent.NewDefaultUserConfig(), cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	for id := range uint64(len(vectors)) {
		require.NoError(t, index.Add(context.Background(), id, vectors[id]))
	}
	return index
}

// peakInFlight records the most calls in flight at once. Each call waits until all
// calls are in or hold passes, so an uncapped run shows every worker.
type peakInFlight struct {
	all        int32
	hold       time.Duration
	inFlight   atomic.Int32
	max        atomic.Int32
	allStarted atomic.Bool
}

func (p *peakInFlight) enter() (leave func()) {
	n := p.inFlight.Add(1)
	for m := p.max.Load(); n > m && !p.max.CompareAndSwap(m, n); m = p.max.Load() {
	}
	if n >= p.all {
		p.allStarted.Store(true)
	}
	for deadline := time.Now().Add(p.hold); !p.allStarted.Load() && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	return func() { p.inFlight.Add(-1) }
}

func (p *peakInFlight) assertMax(t *testing.T, want int, exact bool) {
	t.Helper()
	if exact {
		assert.Equal(t, int32(want), p.max.Load())
	} else {
		assert.LessOrEqual(t, p.max.Load(), int32(want))
	}
}

type peakDistanceProvider struct {
	distancer.Provider
	peak atomic.Pointer[peakInFlight]
}

func (p *peakDistanceProvider) SingleDist(a, b []float32) (float32, error) {
	if peak := p.peak.Load(); peak != nil {
		defer peak.enter()()
	}
	return p.Provider.SingleDist(a, b)
}

func candidateRange(n int) []uint64 {
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = uint64(i)
	}
	return ids
}

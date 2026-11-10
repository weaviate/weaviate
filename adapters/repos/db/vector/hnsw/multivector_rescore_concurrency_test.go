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
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type rescoreTestBucketView struct{}

func (rescoreTestBucketView) ReleaseView() {}

// TestMultiVectorRescoreConcurrentMatchesBruteForce: under concurrent searches
// and a rescore concurrency above 1, every returned distance is the exact
// MaxSim, with and without compression. The tokens of a compressed index's
// rescore are sub-slices of a pooled container, so a container returned to the
// pool too early would change a score.
func TestMultiVectorRescoreConcurrentMatchesBruteForce(t *testing.T) {
	const docs, tokens, dims = 80, 4, 32
	rng := rand.New(rand.NewSource(9))
	random := func() [][]float32 {
		doc := make([][]float32, tokens)
		for i := range doc {
			v := make([]float32, dims)
			for j := range v {
				v[j] = rng.Float32() - 0.5
			}
			doc[i] = distancer.Normalize(v)
		}
		return doc
	}
	corpus := make([][][]float32, docs)
	for i := range corpus {
		corpus[i] = random()
	}
	queries := make([][][]float32, 8)
	for i := range queries {
		queries[i] = random()
	}
	maxSim := func(query, doc [][]float32) float32 {
		var score float32
		for _, q := range query {
			var best float32
			for i, d := range doc {
				var dot float32
				for j := range q {
					dot += q[j] * d[j]
				}
				if i == 0 || -dot < best {
					best = -dot
				}
			}
			score += best
		}
		return score
	}
	// as a shard reads: into the container's memory, shared with no one
	intoContainer := func(id uint64, container *common.VectorSlice) ([][]float32, error) {
		doc := corpus[id]
		if cap(container.Slice) < tokens*dims {
			container.Slice = make([]float32, tokens*dims)
		}
		flat := container.Slice[:tokens*dims]
		// the pool re-slices Mem on Get, so the next read reuses this array
		container.Mem, container.Slice = flat, flat
		out := make([][]float32, tokens)
		for i, token := range doc {
			out[i] = flat[i*dims : (i+1)*dims]
			copy(out[i], token)
		}
		return out, nil
	}

	for _, tt := range []struct {
		name     string
		compress bool
	}{{"uncompressed", false}, {"compressed", true}} {
		t.Run(tt.name, func(t *testing.T) {
			var index *hnsw
			index, err := New(Config{
				RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
				ID:                    "rescore-concurrency",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      distancer.NewDotProductProvider(),
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					docID, relativeID := index.cache.GetKeys(id)
					return corpus[docID][relativeID], nil
				},
				MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
					return corpus[id], nil
				},
				TempMultiVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
					return intoContainer(id, container)
				},
				TempMultiVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([][]float32, error) {
					return intoContainer(id, container)
				},
				GetViewThunk:      func() common.BucketView { return rescoreTestBucketView{} },
				MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
			}, ent.UserConfig{
				VectorCacheMaxObjects: 1e12,
				MaxConnections:        16,
				EFConstruction:        128,
				EF:                    256,
				Multivector:           ent.MultivectorConfig{Enabled: true},
			}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
			require.NoError(t, err)
			t.Cleanup(func() { _ = index.Shutdown(context.Background()) })
			for i, doc := range corpus {
				require.NoError(t, index.AddMulti(context.Background(), uint64(i), doc))
			}
			if tt.compress {
				require.NoError(t, index.compress(ent.UserConfig{
					VectorCacheMaxObjects: 1e12,
					MaxConnections:        16,
					EFConstruction:        128,
					EF:                    256,
					Multivector:           ent.MultivectorConfig{Enabled: true},
					BQ:                    ent.BQConfig{Enabled: true},
				}))
			}
			require.Greater(t, index.rescoreConcurrency, 1)

			var wg sync.WaitGroup
			for range 4 {
				wg.Add(1)
				enterrors.GoWrapper(func() {
					defer wg.Done()
					for _, query := range queries {
						ids, dists, err := index.SearchByMultiVector(context.Background(), query, 10, nil)
						assert.NoError(t, err)
						assert.NotEmpty(t, ids)
						for i, id := range ids {
							assert.InDelta(t, maxSim(query, corpus[id]), dists[i], 1e-4, "doc %d", id)
						}
						// the scoring outside the search, one document at a time
						for id := range corpus {
							score, err := index.computeScore(query, uint64(id))
							assert.NoError(t, err)
							assert.InDelta(t, maxSim(query, corpus[id]), score, 1e-4, "doc %d", id)
						}
					}
				}, index.logger)
			}
			wg.Wait()
		})
	}
}

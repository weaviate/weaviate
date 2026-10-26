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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestFloatBatchDistancerMatchesDistanceToFloatNode(t *testing.T) {
	ctx := context.Background()
	const dim = 33 // odd on purpose to exercise SIMD remainder paths
	const n = 60
	rng := rand.New(rand.NewSource(20260726))
	vectors := make([][]float32, n)
	for i := range vectors {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		vectors[i] = vec
	}

	for _, provider := range []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewL2SquaredProvider(),
		distancer.NewDotProductProvider(),
	} {
		t.Run(provider.Type(), func(t *testing.T) {
			store := testinghelpers.NewDummyStore(t)
			defer store.Shutdown(context.Background())

			index, err := New(Config{
				RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
				ID:                    "float-batch-distancer-test",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      provider,
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					if id >= n {
						return nil, fmt.Errorf("no vector for id %d", id)
					}
					return vectors[id], nil
				},
				GetViewThunk:                 GetViewThunk,
				TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
				AllocChecker:                 memwatch.NewDummyMonitor(),
			}, ent.UserConfig{
				MaxConnections:        30,
				EFConstruction:        64,
				VectorCacheMaxObjects: 100000,
			}, cyclemanager.NewCallbackGroupNoop(), store)
			require.Nil(t, err)
			defer index.Shutdown(ctx)

			for i, vec := range vectors {
				require.Nil(t, index.Add(ctx, uint64(i), vec))
			}

			query := make([]float32, dim)
			for j := range query {
				query[j] = rng.Float32()*2 - 1
			}
			floatDistancer := index.distancerProvider.New(query)

			batch := index.pools.floatBatchDistancers.Get().(*floatBatchDistancer)
			batch.h, batch.distancer = index, floatDistancer
			defer func() {
				batch.distancer = nil
				index.pools.floatBatchDistancers.Put(batch)
			}()

			ids := make([]uint64, 0, n+1)
			for i := 0; i < n; i++ {
				ids = append(ids, uint64(i))
			}
			// One id the vectorForID thunk cannot resolve: both paths must
			// yield an error for it and the same results for everything else.
			missingID := uint64(n + 1000)
			ids = append(ids, missingID)

			// Evict one id from the cache so the batch has to take the
			// loading fallback for a resolvable vector.
			evictedID := uint64(n / 2)
			index.cache.Delete(ctx, evictedID)

			dists := make([]float32, len(ids))
			errs := batch.DistancesToNodes(ids, dists)
			require.NotNil(t, errs, "missing id must surface an error")

			for i, id := range ids {
				want, wantErr := index.distanceToFloatNode(floatDistancer, id)
				if id == missingID {
					assert.Error(t, wantErr)
					assert.Error(t, errs[i], "batch must report the same failure as distanceToFloatNode")
					continue
				}
				require.NoError(t, wantErr)
				require.NoError(t, errs[i])
				assert.Equal(t, want, dists[i], "distance mismatch for id %d", id)
			}

			// All-cached batch (the evicted id was re-cached by the loading
			// fallback above): no error slice at all.
			cached := ids[:n]
			errs = batch.DistancesToNodes(cached, dists[:n])
			assert.Nil(t, errs, "no errors expected when every id is cached")

			// A batch smaller than prefetchAhead must not read out of bounds.
			small := ids[:2]
			errs = batch.DistancesToNodes(small, dists[:2])
			assert.Nil(t, errs)

			// Reusing the same distancer with a larger batch than the last
			// call must regrow the scratch correctly.
			again := batch.DistancesToNodes(ids, dists)
			require.NotNil(t, again)
		})
	}
}

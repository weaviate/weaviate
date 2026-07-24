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

package compressionhelpers_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// DistancesToNodes must be indistinguishable from calling DistanceToNode per
// id: identical distances (bit-exact — both paths run the same kernel on the
// same codes) and identical error behavior for ids that cannot be resolved.
// The search layer switches between the two paths based on a type assertion,
// so any divergence would change search results depending on which path ran.
func TestBatchDistancerMatchesDistanceToNode(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dim := 96
	rng := newRNG(20260723)

	for _, bits := range []int{4, 8} {
		t.Run(fmt.Sprintf("rq%d", bits), func(t *testing.T) {
			for _, metric := range allMetrics() {
				t.Run(metric.Type(), func(t *testing.T) {
					store := testinghelpers.NewDummyStore(t)
					defer store.Shutdown(context.Background())

					compressor, err := compressionhelpers.NewRQCompressor(metric, 1e6, logger, store,
						memwatch.NewDummyMonitor(), lsmkv.MakeNoopBucketOptions, bits, dim, "", nil)
					require.NoError(t, err)
					defer compressor.Drop()

					const n = 50
					ids := make([]uint64, 0, n+1)
					for i := 0; i < n; i++ {
						id := uint64(i)
						compressor.Preload(id, randomUnitVector(dim, rng))
						ids = append(ids, id)
					}
					// One id that was never preloaded: both paths must yield an
					// error for it and the same results for everything else.
					missingID := uint64(n + 1000)
					ids = append(ids, missingID)

					distancer, returnFn := compressor.NewDistancer(randomUnitVector(dim, rng))
					defer returnFn()
					batch, ok := distancer.(compressionhelpers.BatchCompressorDistancer)
					require.True(t, ok, "expected %T to implement BatchCompressorDistancer", distancer)

					dists := make([]float32, len(ids))
					errs := batch.DistancesToNodes(ids, dists)
					require.NotNil(t, errs, "missing id must surface an error")

					for i, id := range ids {
						want, wantErr := distancer.DistanceToNode(id)
						if id == missingID {
							assert.Error(t, wantErr)
							assert.Error(t, errs[i], "batch must report the same failure as DistanceToNode")
							continue
						}
						require.NoError(t, wantErr)
						require.NoError(t, errs[i])
						assert.Equal(t, want, dists[i], "distance mismatch for id %d", id)
					}

					// All-cached batch: no error slice at all.
					cached := ids[:n]
					errs = batch.DistancesToNodes(cached, dists[:n])
					assert.Nil(t, errs, "no errors expected when every id is cached")

					// Reusing the same distancer with a larger batch than the
					// first call must regrow the scratch correctly.
					again := batch.DistancesToNodes(ids, dists)
					require.NotNil(t, again)
				})
			}
		})
	}
}

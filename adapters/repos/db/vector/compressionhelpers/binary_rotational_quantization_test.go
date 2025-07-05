//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compressionhelpers_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// Very basic test that distance estimates are in the same ballpark.
// TODO: Verify that we can handle non-unit vectors of different lengths as well.
// TODO: Verify that the estimator behaves according to the concentration bounds in the paper.
func TestBRQDistanceEstimates(t *testing.T) {
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	dimensions := []int{64, 128, 256, 512, 768, 1024, 1536}
	queryBits := []int{4, 5, 6, 7, 8}
	rng := newRNG(123)

	for _, dim := range dimensions {
		for _, bits := range queryBits {
			for _, m := range metrics {
				q, x := correlatedVectors(dim, 0.5)
				rq := compressionhelpers.NewBinaryRotationalQuantizer(dim, bits, rng.Uint64(), m)
				distancer := rq.NewDistancer(q)
				cx := rq.Encode(x)
				distancerEstimate, _ := distancer.Distance(cx)
				target, _ := m.SingleDist(q, x)
				eps := 0.3 * math.Abs(float64(target))
				assert.Less(t, absDiff(distancerEstimate, target), eps, "Metric: %s, Target: %.4f, Estimate: %.4f, eps: %.4f", m.Type(), target, distancerEstimate, eps)
				// t.Logf("cq: %v\n", distancer.QueryCode())
				// t.Logf("cx: %v\n", compressionhelpers.RQOneBitCode(cx))
			}
		}
	}
}

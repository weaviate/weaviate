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
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// TestRQ4SymmetricMatchesReconstruction verifies the algebraic identity behind
// DistanceBetweenCompressedVectors: the metadata-based dot estimate
// (D*lx*ly + lx*codeSumY + ly*codeSumX + sx*sy*<cx,cy>) must equal the dot
// product of the two reconstructions Restore(cx)·Restore(cy) up to float
// rounding, for every metric and across odd/even/pow2 dimensions.
func TestRQ4SymmetricMatchesReconstruction(t *testing.T) {
	rng := newRNG(20260723)
	dims := []int{2, 3, 37, 64, 100, 128, 384, 777, 1024, 1536, 2000}
	for _, d := range dims {
		for _, m := range allMetrics() {
			t.Run(fmt.Sprintf("d%d/%s", d, m.Type()), func(t *testing.T) {
				rq := compressionhelpers.NewFourBitRotationalQuantizer(d, rng.Uint64(), m)
				x, y := randomUnitVector(d, rng), randomUnitVector(d, rng)
				cx, cy := compressionhelpers.RQ4Code(rq.Encode(x)), compressionhelpers.RQ4Code(rq.Encode(y))

				got, err := rq.DistanceBetweenCompressedVectors(cx, cy)
				require.NoError(t, err)

				// Reference: reconstructions in rotated space (Restore returns
				// lower + step*code with the least-squares rescale folded in).
				hx, hy := rq.Restore(cx), rq.Restore(cy)
				var dot float64
				for i := range hx {
					dot += float64(hx[i]) * float64(hy[i])
				}
				var want float64
				switch m.Type() {
				case "dot":
					want = -dot
				case "cosine-dot":
					want = 1 - dot
				default: // l2-squared
					want = float64(cx.Norm2()) + float64(cy.Norm2()) - 2*dot
				}
				assert.InDelta(t, want, float64(got), 2e-3*(1+math.Abs(want)),
					"identity mismatch at d=%d metric=%s", d, m.Type())
			})
		}
	}
}

// TestRQ4AsymmetricMatchesReconstruction does the same for the query path:
// Distance(cx) must equal the dot of the 8-bit query reconstruction with the
// 4-bit data reconstruction.
func TestRQ4AsymmetricMatchesReconstruction(t *testing.T) {
	rng := newRNG(715)
	dims := []int{2, 37, 64, 384, 777, 1536}
	for _, d := range dims {
		for _, m := range allMetrics() {
			t.Run(fmt.Sprintf("d%d/%s", d, m.Type()), func(t *testing.T) {
				rq := compressionhelpers.NewFourBitRotationalQuantizer(d, rng.Uint64(), m)
				q, x := randomUnitVector(d, rng), randomUnitVector(d, rng)
				cx := compressionhelpers.RQ4Code(rq.Encode(x))
				dist := rq.NewDistancer(q)
				got, err := dist.Distance(cx)
				require.NoError(t, err)

				// Reconstruct the 8-bit query code from the rotated query the
				// same way encodeQuery does.
				rqx := rq.Rotate(q)
				lower := rqx[0]
				upper := rqx[0]
				for _, v := range rqx {
					if v < lower {
						lower = v
					}
					if v > upper {
						upper = v
					}
				}
				step := (upper - lower) / 255
				hq := make([]float64, len(rqx))
				if step > 0 {
					for i, v := range rqx {
						c := math.Trunc(float64((v-lower)/step + 0.5))
						hq[i] = float64(lower) + float64(step)*c
					}
				}
				hx := rq.Restore(cx)
				var dot float64
				for i := range hx {
					dot += hq[i] * float64(hx[i])
				}
				var qnorm2 float64
				for _, v := range q {
					qnorm2 += float64(v) * float64(v)
				}
				var want float64
				switch m.Type() {
				case "dot":
					want = -dot
				case "cosine-dot":
					want = 1 - dot
				default:
					want = float64(cx.Norm2()) + qnorm2 - 2*dot
				}
				assert.InDelta(t, want, float64(got), 2e-3*(1+math.Abs(want)),
					"identity mismatch at d=%d metric=%s", d, m.Type())
			})
		}
	}
}

// TestRQ4SymmetricEstimationQuality quantifies the extra noise of the
// symmetric (4-bit x 4-bit) estimate vs the asymmetric (8-bit query) one.
func TestRQ4SymmetricEstimationQuality(t *testing.T) {
	for _, dim := range []int{256, 768, 1536} {
		t.Run(fmt.Sprintf("d%d", dim), func(t *testing.T) {
			rng := newRNG(uint64(dim) * 7)
			rq := compressionhelpers.NewFourBitRotationalQuantizer(dim, rng.Uint64(), distancer.NewDotProductProvider())
			n := 100
			var asymErr, symErr float64
			for range n {
				q, x := randomUnitVector(dim, rng), randomUnitVector(dim, rng)
				trueDot := float64(dotFloat(q, x))
				cq, cx := rq.Encode(q), rq.Encode(x)
				est, err := rq.NewDistancer(q).Distance(cx)
				require.NoError(t, err)
				asymErr += math.Abs(-float64(est) - trueDot)
				est2, err := rq.DistanceBetweenCompressedVectors(cq, cx)
				require.NoError(t, err)
				symErr += math.Abs(-float64(est2) - trueDot)
			}
			t.Logf("d=%d avg |err| asymmetric=%.5f symmetric=%.5f ratio=%.2f",
				dim, asymErr/float64(n), symErr/float64(n), symErr/asymErr)
		})
	}
}

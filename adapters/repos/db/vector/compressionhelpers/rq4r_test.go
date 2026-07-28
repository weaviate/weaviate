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
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// The residual stage must strictly reduce the reconstruction error: for the
// least-squares sigma, SSE drops by exactly D*sigma^2.
func TestRQ4RResidualReducesReconstructionError(t *testing.T) {
	rng := newRNG(31415)
	for _, d := range []int{64, 384, 1024, 1536} {
		rq := compressionhelpers.NewFourBitResidualQuantizer(d, rng.Uint64(), distancer.NewCosineDistanceProvider())
		x := randomUnitVector(d, rng)
		rx := rq.Rotate(x)

		code := compressionhelpers.RQ4RCode(rq.Encode(x))
		withResidual := rq.Restore(code)
		baseOnly := rq.FourBitRotationalQuantizer.Restore(code.Base())

		var sseBase, sseResidual float64
		for i := range rx {
			db := float64(rx[i] - baseOnly[i])
			dr := float64(rx[i] - withResidual[i])
			sseBase += db * db
			sseResidual += dr * dr
		}
		assert.Less(t, sseResidual, sseBase, "dim %d", d)

		sigma := float64(code.Sigma())
		assert.InDelta(t, sseBase-float64(d)*sigma*sigma, sseResidual, 1e-4, "dim %d", d)
	}
}

// The residual-corrected estimator should be more accurate than the plain
// 4-bit estimator on average.
func TestRQ4RImprovesDistanceEstimates(t *testing.T) {
	rng := newRNG(2718)
	d := 1536
	n := 50
	seed := rng.Uint64()
	base := compressionhelpers.NewFourBitRotationalQuantizer(d, seed, distancer.NewDotProductProvider())
	residual := compressionhelpers.NewFourBitResidualQuantizer(d, seed, distancer.NewDotProductProvider())

	var baseErr, residualErr float64
	for range n {
		q, x := randomUnitVector(d, rng), randomUnitVector(d, rng)
		trueDot := float64(dotFloat(q, x))

		be, err := base.NewDistancer(q).Distance(base.Encode(x))
		require.NoError(t, err)
		re, err := residual.NewDistancer(q).Distance(residual.Encode(x))
		require.NoError(t, err)

		baseErr += math.Abs(-float64(be) - trueDot)
		residualErr += math.Abs(-float64(re) - trueDot)
	}
	assert.Less(t, residualErr, baseErr,
		"residual avg err %.6f should beat base avg err %.6f", residualErr/float64(n), baseErr/float64(n))
}

func TestRQ4RCodeLayout(t *testing.T) {
	rng := newRNG(99)
	for _, d := range []int{64, 384, 1536} {
		rq := compressionhelpers.NewFourBitResidualQuantizer(d, rng.Uint64(), distancer.NewL2SquaredProvider())
		x := randomUnitVector(d, rng)
		code := compressionhelpers.RQ4RCode(rq.Encode(x))
		outDim := rq.OutputDimension()

		require.Equal(t, outDim, code.Dimension())
		require.Equal(t, outDim/8, len(code.SignBits()))

		var popcount uint32
		for _, b := range code.SignBits() {
			popcount += uint32(bits.OnesCount8(b))
		}
		assert.Equal(t, popcount, code.SignPopcount())
		assert.GreaterOrEqual(t, code.Sigma(), float32(0))
	}
}

func TestRQ4RHandlesAbnormalVectorsGracefully(t *testing.T) {
	rng := newRNG(7)
	d := 128
	rq := compressionhelpers.NewFourBitResidualQuantizer(d, rng.Uint64(), distancer.NewL2SquaredProvider())
	x := randomUnitVector(d, rng)
	cx := rq.Encode(x)

	cases := [][]float32{nil, {}, make([]float32, d), make([]float32, 3), make([]float32, 10*d)}
	for i, q := range cases {
		code := rq.Encode(q)
		require.Equal(t, len(cx), len(code), "case %d", i)
		dist := rq.NewDistancer(q)
		_, err := dist.Distance(cx)
		assert.NoError(t, err, "case %d", i)
	}

	// Length mismatch must error.
	other := compressionhelpers.NewFourBitResidualQuantizer(1536, rng.Uint64(), distancer.NewL2SquaredProvider())
	_, err := rq.NewDistancer(x).Distance(other.Encode(randomUnitVector(1536, rng)))
	assert.Error(t, err)
}

func TestRQ4RZeroSigmaMatchesBaseDistance(t *testing.T) {
	rng := newRNG(55)
	d := 256
	seed := rng.Uint64()
	base := compressionhelpers.NewFourBitRotationalQuantizer(d, seed, distancer.NewDotProductProvider())
	residual := compressionhelpers.NewFourBitResidualQuantizer(d, seed, distancer.NewDotProductProvider())

	q, x := randomUnitVector(d, rng), randomUnitVector(d, rng)
	rcode := compressionhelpers.RQ4RCode(residual.Encode(x))
	// Force sigma to zero: the residual correction must vanish and the
	// estimate must equal the plain 4-bit one.
	zeroed := make([]byte, len(rcode))
	copy(zeroed, rcode)
	for i := len(rcode.Base()); i < len(rcode.Base())+4; i++ {
		zeroed[i] = 0
	}

	re, err := residual.NewDistancer(q).Distance(zeroed)
	require.NoError(t, err)
	be, err := base.NewDistancer(q).Distance(rcode.Base())
	require.NoError(t, err)
	assert.InDelta(t, be, re, 1e-6)
}

func BenchmarkRQ4RDistance(b *testing.B) {
	rng := newRNG(42)
	for _, dim := range []int{384, 1024, 1536} {
		rq := compressionhelpers.NewFourBitResidualQuantizer(dim, rng.Uint64(), distancer.NewCosineDistanceProvider())
		q, x := randomUnitVector(dim, rng), randomUnitVector(dim, rng)
		cx := rq.Encode(x)
		dist := rq.NewDistancer(q)
		b.Run(fmt.Sprintf("d%d", dim), func(b *testing.B) {
			for b.Loop() {
				dist.Distance(cx)
			}
			b.ReportMetric((float64(b.N)/1e6)/float64(b.Elapsed().Seconds()), "m.ops/sec")
		})
	}
}

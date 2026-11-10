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

package compressionhelpers

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// cappedTestVectors draws unit vectors that share a strong common component,
// the anisotropic shape centering exists for.
func cappedTestVectors(n, dims int, seed uint64) [][]float32 {
	rng := rand.New(rand.NewPCG(seed, 77))
	bias := make([]float64, dims)
	var biasNorm float64
	for i := range bias {
		bias[i] = rng.NormFloat64()
		biasNorm += bias[i] * bias[i]
	}
	biasNorm = math.Sqrt(biasNorm)

	out := make([][]float32, n)
	for i := range out {
		vec := make([]float32, dims)
		var norm float64
		for j := range vec {
			x := 0.5*bias[j]/biasNorm + 0.5*rng.NormFloat64()/math.Sqrt(float64(dims))
			vec[j] = float32(x)
			norm += x * x
		}
		norm = math.Sqrt(norm)
		for j := range vec {
			vec[j] = float32(float64(vec[j]) / norm)
		}
		out[i] = vec
	}
	return out
}

func cappedTestQuantizer(t *testing.T, vectors [][]float32, provider distancer.Provider) *CenteredBinaryRotationalQuantizer {
	t.Helper()
	dims := len(vectors[0])
	rq, err := NewCenteredBinaryRotationalQuantizer(dims, DefaultFastRotationSeed, MeanVector(vectors, dims), provider)
	require.NoError(t, err)
	return rq
}

func TestCappedCenteredRQ1Widths(t *testing.T) {
	vectors := cappedTestVectors(50, 1536, 1)
	rq := cappedTestQuantizer(t, vectors, distancer.NewCosineDistanceProvider())

	tests := []struct {
		name      string
		bits      int
		wantErr   bool
		wantWords int
	}{
		{name: "one word", bits: 64, wantWords: 2},
		{name: "256 bits", bits: 256, wantWords: 5},
		{name: "512 bits", bits: 512, wantWords: 9},
		{name: "768 bits", bits: 768, wantWords: 13},
		{name: "the full width", bits: 1536, wantWords: 25},
		{name: "zero", bits: 0, wantErr: true},
		{name: "negative", bits: -64, wantErr: true},
		{name: "not a multiple of 64", bits: 200, wantErr: true},
		{name: "wider than the rotation", bits: 1600, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capped, err := rq.Capped(tt.bits)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.bits, capped.Bits())
			assert.Len(t, capped.Encode(vectors[0]), tt.wantWords)
		})
	}
}

// Capped at the full width there is nothing left out: the codes and the
// distances must be exactly those of the quantizer.
func TestCappedCenteredRQ1AtFullWidthIsTheQuantizer(t *testing.T) {
	tests := []struct {
		name     string
		dims     int
		provider distancer.Provider
	}{
		{name: "cosine, 1536 dimensions", dims: 1536, provider: distancer.NewCosineDistanceProvider()},
		{name: "l2, 1536 dimensions", dims: 1536, provider: distancer.NewL2SquaredProvider()},
		{name: "dot, 768 dimensions", dims: 768, provider: distancer.NewDotProductProvider()},
		{name: "cosine, dimensions padded to the minimum width", dims: 100, provider: distancer.NewCosineDistanceProvider()},
		{name: "l2, dimensions padded to the minimum width", dims: 100, provider: distancer.NewL2SquaredProvider()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vectors := cappedTestVectors(60, tt.dims, 2)
			queries, vectors := vectors[:10], vectors[10:]
			rq := cappedTestQuantizer(t, vectors, tt.provider)
			capped, err := rq.Capped(int(rq.rotation.OutputDim))
			require.NoError(t, err)

			for _, vec := range vectors {
				require.Equal(t, rq.Encode(vec), capped.Encode(vec))
			}
			for _, query := range queries {
				full, short := rq.NewDistancer(query), capped.NewDistancer(query)
				for _, vec := range vectors {
					code := rq.Encode(vec)
					want, err := full.Distance(code)
					require.NoError(t, err)
					got, err := short.Distance(code)
					require.NoError(t, err)
					require.Equal(t, math.Float32bits(want), math.Float32bits(got))
				}
			}
		})
	}
}

// The header of a capped code describes the whole vector, except for the
// step, which scales the dot product of the kept bits. The kept bits are the
// first bits of the full code.
func TestCappedCenteredRQ1CodeIsAPrefixOfTheFullCode(t *testing.T) {
	vectors := cappedTestVectors(40, 1536, 3)
	rq := cappedTestQuantizer(t, vectors, distancer.NewCosineDistanceProvider())

	for _, bits := range []int{64, 256, 512, 768, 1024} {
		capped, err := rq.Capped(bits)
		require.NoError(t, err)
		for _, vec := range vectors {
			full, short := rq.Encode(vec), capped.Encode(vec)
			require.Equal(t, full[oneBitFieldWords:oneBitFieldWords+bits/64], short[oneBitFieldWords:])

			fullHeader, shortHeader := rq.header(full), rq.header(short)
			require.Equal(t, fullHeader.dmu, shortHeader.dmu)
			require.Equal(t, fullHeader.norm2, shortHeader.norm2)
			require.Positive(t, shortHeader.step)
			require.NotEqual(t, fullHeader.step, shortHeader.step, "the step is that of the kept bits")
		}
	}
}

// Fewer bits must cost precision, not correctness: the estimates stay
// centered on the true distance and tighten as bits are added.
func TestCappedCenteredRQ1EstimatesSharpenWithBits(t *testing.T) {
	tests := []struct {
		name     string
		provider distancer.Provider
	}{
		{name: "cosine", provider: distancer.NewCosineDistanceProvider()},
		{name: "l2", provider: distancer.NewL2SquaredProvider()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vectors := cappedTestVectors(420, 1536, 4)
			queries, vectors := vectors[:20], vectors[20:]
			rq := cappedTestQuantizer(t, vectors, tt.provider)

			var previous float64
			for i, bits := range []int{256, 512, 768, 1536} {
				capped, err := rq.Capped(bits)
				require.NoError(t, err)
				codes := make([][]uint64, len(vectors))
				for j, vec := range vectors {
					codes[j] = capped.Encode(vec)
				}

				var bias, spread, scale float64
				for _, query := range queries {
					estimator := capped.NewDistancer(query)
					for j, vec := range vectors {
						exact, err := tt.provider.SingleDist(query, vec)
						require.NoError(t, err)
						estimate, err := estimator.Distance(codes[j])
						require.NoError(t, err)
						bias += float64(estimate - exact)
						spread += math.Abs(float64(estimate - exact))
						scale += math.Abs(float64(exact))
					}
				}
				pairs := float64(len(queries) * len(vectors))
				bias, spread, scale = bias/pairs, spread/pairs, scale/pairs
				t.Logf("%d bits: mean distance %.4f, mean error %+.5f, mean absolute error %.5f", bits, scale, bias, spread)

				assert.Lessf(t, math.Abs(bias), 0.25*spread+0.002, "%d bits: the estimate must not be biased", bits)
				if i > 0 {
					assert.Lessf(t, spread, previous, "%d bits must estimate better than fewer bits", bits)
				}
				previous = spread
			}
		})
	}
}

func TestCappedCenteredRQ1DistancerEdgeCases(t *testing.T) {
	vectors := cappedTestVectors(30, 1536, 5)
	rq := cappedTestQuantizer(t, vectors, distancer.NewCosineDistanceProvider())
	capped, err := rq.Capped(512)
	require.NoError(t, err)

	t.Run("a code of another width", func(t *testing.T) {
		estimator := capped.NewDistancer(vectors[0])
		_, err := estimator.Distance(rq.Encode(vectors[1]))
		require.Error(t, err)
		wider, err := rq.Capped(768)
		require.NoError(t, err)
		_, err = estimator.Distance(wider.Encode(vectors[1]))
		require.Error(t, err)
	})

	t.Run("the zero query", func(t *testing.T) {
		// A query equal to the mean centers to zero and encodes to an
		// empty query code, which the distancer of the quantizer scores
		// from the header alone.
		query := make([]float32, 1536)
		copy(query, rq.mean)
		full, short := rq.NewDistancer(query), capped.NewDistancer(query)
		want, err := full.Distance(rq.Encode(vectors[1]))
		require.NoError(t, err)
		got, err := short.Distance(capped.Encode(vectors[1]))
		require.NoError(t, err)
		assert.False(t, math.IsNaN(float64(got)))
		assert.InDelta(t, want, got, 1e-3)
	})
}

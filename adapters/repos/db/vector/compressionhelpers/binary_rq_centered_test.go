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
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// crqTestData generates n unit vectors sharing a strong common component
// (embedding-model-like anisotropy: the case centering exists for), plus
// their float64-exact sample mean.
func crqTestData(n, dim int, seed uint64) ([][]float32, []float32) {
	rng := rand.New(rand.NewPCG(seed, 23))
	bias := make([]float64, dim)
	for i := range bias {
		bias[i] = rng.NormFloat64()
	}
	var bn float64
	for _, b := range bias {
		bn += b * b
	}
	bn = math.Sqrt(bn)
	for i := range bias {
		bias[i] /= bn
	}

	vecs := make([][]float32, n)
	meanAcc := make([]float64, dim)
	for i := range vecs {
		v := make([]float64, dim)
		var norm float64
		for j := range v {
			v[j] = 0.5*bias[j] + 0.5*rng.NormFloat64()/math.Sqrt(float64(dim))
			norm += v[j] * v[j]
		}
		norm = math.Sqrt(norm)
		f := make([]float32, dim)
		for j := range v {
			v[j] /= norm
			f[j] = float32(v[j])
			meanAcc[j] += v[j]
		}
		vecs[i] = f
	}
	mean := make([]float32, dim)
	for j := range meanAcc {
		mean[j] = float32(meanAcc[j] / float64(n))
	}
	return vecs, mean
}

func f64Dot(a, b []float32) float64 {
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

// TestCenteredRQ1HeaderAccessors pins the 8-byte header layout: step as
// float32 in the low 32 bits, squaredNorm as bfloat16 in bits 32..47, ⟨μ,x⟩
// as bfloat16 in bits 48..63. Step must round-trip exactly; the bf16 fields
// within RNE error.
func TestCenteredRQ1HeaderAccessors(t *testing.T) {
	vecs, mean := crqTestData(4, 128, 1)
	rq, err := NewCenteredBinaryRotationalQuantizer(128, 42, mean, distancer.NewCosineDistanceProvider())
	require.NoError(t, err)

	code := make([]uint64, 3)
	rq.putHeader(code, 0.12345678, 0.87654321, -0.31415927)
	h := rq.header(code)
	assert.Equal(t, float32(0.12345678), h.step, "step is float32-exact")
	assert.InDelta(t, 0.87654321, h.norm2, 0.87654321/256, "norm2 within bf16 RNE error")
	assert.InDelta(t, -0.31415927, h.muX, 0.31415927/256, "muX within bf16 RNE error")

	// the stored muX of a real code matches the float64 reference within
	// bf16 error
	x := vecs[0]
	c := rq.Encode(x)
	exactMuX := f64Dot(mean, x)
	assert.InDelta(t, exactMuX, float64(rq.header(c).muX), math.Abs(exactMuX)/256+1e-7)
}

// TestCenteredRQ1DispatchAndPlainUnchanged pins that the centered code has
// the same word count as the plain one (the header REPLACES the plain
// layout, it does not extend it), that the layouts differ as documented, and
// that the plain quantizer's layout is untouched by this change.
func TestCenteredRQ1DispatchAndPlainUnchanged(t *testing.T) {
	for _, dim := range []int{64, 256, 768, 1536} {
		t.Run(fmt.Sprintf("dim=%d", dim), func(t *testing.T) {
			vecs, mean := crqTestData(3, dim, 2)
			cos := distancer.NewCosineDistanceProvider()

			centered, err := NewCenteredBinaryRotationalQuantizer(dim, 42, mean, cos)
			require.NoError(t, err)
			plain, err := NewBinaryRotationalQuantizer(dim, 42, cos)
			require.NoError(t, err)

			x := vecs[0]
			cc := centered.Encode(x)
			pc := plain.Encode(x)
			require.Equal(t, len(pc), len(cc), "centered header must fit in the same single metadata word")

			// plain layout untouched: word0 = [squaredNorm f32 | step f32],
			// squaredNorm ≈ |x|² = 1 for unit vectors
			pcode := RQOneBitCode(pc)
			assert.InDelta(t, 1.0, float64(pcode.SquaredNorm()), 1e-3, "plain layout: f32 squaredNorm in upper bits")
			assert.Greater(t, pcode.Step(), float32(0), "plain layout: f32 step in lower bits")

			// centered layout: step f32 in low bits, centered norm ≪ 1 in
			// bf16, muX in bf16
			h := centered.header(cc)
			assert.Greater(t, h.step, float32(0))
			exactN2 := func() float64 {
				var s float64
				for i := range x {
					d := float64(x[i]) - float64(mean[i])
					s += d * d
				}
				return s
			}()
			assert.InDelta(t, exactN2, float64(h.norm2), exactN2/128+1e-6, "centered norm2 stores |x-μ|²")
			assert.InDelta(t, f64Dot(mean, x), float64(h.muX), math.Abs(f64Dot(mean, x))/128+1e-6)
		})
	}
}

// TestCenteredRQ1RoundTrip: Decode(Encode(x)) must point in x's direction
// across the dim range, including the padded (<256) case.
func TestCenteredRQ1RoundTrip(t *testing.T) {
	for _, dim := range []int{64, 128, 256, 768, 1536} {
		t.Run(fmt.Sprintf("dim=%d", dim), func(t *testing.T) {
			vecs, mean := crqTestData(10, dim, 3)
			rq, err := NewCenteredBinaryRotationalQuantizer(dim, 42, mean, distancer.NewCosineDistanceProvider())
			require.NoError(t, err)
			for _, x := range vecs {
				dec := rq.Decode(rq.Encode(x))
				require.Equal(t, dim, len(dec))
				var dot, dn float64
				for i := range x {
					dot += float64(x[i]) * float64(dec[i])
					dn += float64(dec[i]) * float64(dec[i])
				}
				cosine := dot / math.Sqrt(dn) // |x| = 1
				assert.Greater(t, cosine, 0.5, "decoded vector must correlate with the input")
			}
		})
	}
}

// TestCenteredRQ1EstimatorVsF32Arm is the bf16 gate: the distance estimator
// reading bf16 header fields must be statistically indistinguishable from
// the same estimator reading full-precision fields. The f32 arm is
// implemented here in the test (per-pair: bf16 distance corrected by the
// exact-vs-bf16 field difference) and is not shipped.
func TestCenteredRQ1EstimatorVsF32Arm(t *testing.T) {
	dim := 768
	n := 250
	vecs, mean := crqTestData(n, dim, 4)
	rq, err := NewCenteredBinaryRotationalQuantizer(dim, 42, mean, distancer.NewCosineDistanceProvider())
	require.NoError(t, err)

	var mu2 float64
	for _, m := range mean {
		mu2 += float64(m) * float64(m)
	}

	var sumErrBF, sumErrF32, sumAbsBF, sumAbsF32, maxPairDiff float64
	var count int
	for i := 0; i < 30; i++ {
		q := vecs[i]
		d := rq.NewDistancer(q)
		for j := 30; j < n; j++ {
			x := vecs[j]
			code := rq.Encode(x)
			est, err := d.Distance(code)
			require.NoError(t, err)

			// f32 arm: the only bf16 field in the query->code cosine path is
			// muX; substitute the float64-exact value.
			muXExact := f64Dot(mean, x)
			estF32 := float64(est) + float64(rq.header(code).muX) - muXExact

			exact := 1 - f64Dot(q, x)
			errBF := float64(est) - exact
			errF32 := estF32 - exact
			sumErrBF += errBF
			sumErrF32 += errF32
			sumAbsBF += math.Abs(errBF)
			sumAbsF32 += math.Abs(errF32)
			if diff := math.Abs(float64(est) - estF32); diff > maxPairDiff {
				maxPairDiff = diff
			}
			count++
		}
	}
	nF := float64(count)
	t.Logf("bias bf16=%.5f f32=%.5f | mean|err| bf16=%.5f f32=%.5f | max pair diff=%.5f",
		sumErrBF/nF, sumErrF32/nF, sumAbsBF/nF, sumAbsF32/nF, maxPairDiff)

	// estimator quality: 1-bit codes on hard anisotropic data
	assert.Less(t, math.Abs(sumErrBF/nF), 0.02, "estimator bias")
	assert.Less(t, sumAbsBF/nF, 0.08, "estimator mean abs error")
	// bf16 vs f32 arm: per-pair difference bounded by bf16 RNE on muX
	// (|muX| ≲ 1 → ≤ ~2^-8), and the aggregate error must not move
	assert.Less(t, maxPairDiff, 0.005, "bf16 muX may not move any distance more than RNE allows")
	assert.Less(t, math.Abs(sumErrBF/nF-sumErrF32/nF), 1e-3, "bf16 must not shift the estimator bias")
	assert.Less(t, math.Abs(sumAbsBF/nF-sumAbsF32/nF), 1e-3, "bf16 must not widen the estimator error")
}

// TestCenteredRQ1CompressedCompressed checks the code-to-code estimator
// (graph maintenance path), which reads both bf16 fields — norms via the
// angle estimate, muX via the correction — against the float64 reference.
// The constant term must come from the code's own Dimension(), so codes of
// one width computed by a quantizer of another width fail loudly rather
// than silently misscale (the OutputDim-assumption class of bug).
func TestCenteredRQ1CompressedCompressed(t *testing.T) {
	dim := 512
	n := 120
	vecs, mean := crqTestData(n, dim, 5)
	rq, err := NewCenteredBinaryRotationalQuantizer(dim, 42, mean, distancer.NewCosineDistanceProvider())
	require.NoError(t, err)

	codes := make([][]uint64, n)
	for i, v := range vecs {
		codes[i] = rq.Encode(v)
	}
	var sumErr, sumAbs float64
	var count int
	for i := 0; i < 20; i++ {
		for j := 20; j < n; j++ {
			got, err := rq.DistanceBetweenCompressedVectors(codes[i], codes[j])
			require.NoError(t, err)
			exact := 1 - f64Dot(vecs[i], vecs[j])
			sumErr += float64(got) - exact
			sumAbs += math.Abs(float64(got) - exact)
			count++
		}
	}
	t.Logf("cc bias=%.5f mean|err|=%.5f", sumErr/float64(count), sumAbs/float64(count))
	assert.Less(t, math.Abs(sumErr)/float64(count), 0.03)
	assert.Less(t, sumAbs/float64(count), 0.12)
}

// TestCenteredRQ1Validation pins constructor contracts.
func TestCenteredRQ1Validation(t *testing.T) {
	cos := distancer.NewCosineDistanceProvider()
	_, err := NewCenteredBinaryRotationalQuantizer(128, 42, nil, cos)
	assert.Error(t, err, "nil mean must be rejected — use the plain quantizer instead")
	_, err = NewCenteredBinaryRotationalQuantizer(128, 42, make([]float32, 5), cos)
	assert.Error(t, err, "mean dimension mismatch must be rejected")
}

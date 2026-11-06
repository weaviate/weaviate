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
// float32 in the low 32 bits, squaredNorm as bfloat16 in bits 32..47, the
// CENTERED dot ⟨μ,x-μ⟩ as bfloat16 in bits 48..63. Step must round-trip
// exactly; the bf16 fields within RNE error.
func TestCenteredRQ1HeaderAccessors(t *testing.T) {
	vecs, mean := crqTestData(4, 128, 1)
	rq, err := NewCenteredBinaryRotationalQuantizer(128, 42, mean, distancer.NewCosineDistanceProvider())
	require.NoError(t, err)

	code := make([]uint64, 3)
	rq.putHeader(code, 0.12345678, 0.87654321, -0.31415927)
	h := rq.header(code)
	assert.Equal(t, float32(0.12345678), h.step, "step is float32-exact")
	assert.InDelta(t, 0.87654321, h.norm2, 0.87654321/256, "norm2 within bf16 RNE error")
	assert.InDelta(t, -0.31415927, h.dmu, 0.31415927/256, "dmu within bf16 RNE error")

	// the stored dmu of a real code matches the float64 reference within
	// bf16 error
	x := vecs[0]
	c := rq.Encode(x)
	exactDmu := f64Dot(mean, x) - f64Dot(mean, mean)
	assert.InDelta(t, exactDmu, float64(rq.header(c).dmu), math.Abs(exactDmu)/256+1e-7)
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
			// bf16, centered dot ⟨μ,x-μ⟩ in bf16
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
			exactDmu := f64Dot(mean, x) - f64Dot(mean, mean)
			assert.InDelta(t, exactDmu, float64(h.dmu), math.Abs(exactDmu)/128+1e-6)
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
			// the centered dot; substitute the float64-exact value.
			dmuExact := f64Dot(mean, x) - mu2
			estF32 := float64(est) + float64(rq.header(code).dmu) - dmuExact

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
	// bf16 vs f32 arm: per-pair difference bounded by bf16 RNE on the
	// centered dot (|⟨μ,x-μ⟩| ≪ 1 → ≤ ~2^-8), and the aggregate error must
	// not move
	assert.Less(t, maxPairDiff, 0.005, "bf16 dmu may not move any distance more than RNE allows")
	assert.Less(t, math.Abs(sumErrBF/nF-sumErrF32/nF), 1e-3, "bf16 must not shift the estimator bias")
	assert.Less(t, math.Abs(sumAbsBF/nF-sumAbsF32/nF), 1e-3, "bf16 must not widen the estimator error")
}

// TestCenteredRQ1EstimatorVsF32ArmL2 is the bf16 gate for l2-squared. Under
// cosine the stored norm2 never enters the query-to-code estimate, so the
// test above cannot see its rounding; under l2 it is added to every
// distance. The f32 arm substitutes the float64-exact ‖x-μ‖².
func TestCenteredRQ1EstimatorVsF32ArmL2(t *testing.T) {
	dim := 768
	n := 250
	vecs, mean := crqTestData(n, dim, 4)
	rq, err := NewCenteredBinaryRotationalQuantizer(dim, 42, mean, distancer.NewL2SquaredProvider())
	require.NoError(t, err)

	var sumErrBF, sumErrF32, sumAbsBF, sumAbsF32, maxPairDiff, sumExact float64
	var count int
	for i := 0; i < 30; i++ {
		q := vecs[i]
		d := rq.NewDistancer(q)
		for j := 30; j < n; j++ {
			x := vecs[j]
			code := rq.Encode(x)
			est, err := d.Distance(code)
			require.NoError(t, err)

			var norm2Exact, exact float64
			for k := range x {
				c := float64(x[k]) - float64(mean[k])
				norm2Exact += c * c
				e := float64(q[k]) - float64(x[k])
				exact += e * e
			}
			estF32 := float64(est) - float64(rq.header(code).norm2) + norm2Exact

			errBF := float64(est) - exact
			errF32 := estF32 - exact
			sumErrBF += errBF
			sumErrF32 += errF32
			sumAbsBF += math.Abs(errBF)
			sumAbsF32 += math.Abs(errF32)
			sumExact += exact
			if diff := math.Abs(float64(est) - estF32); diff > maxPairDiff {
				maxPairDiff = diff
			}
			count++
		}
	}
	nF := float64(count)
	t.Logf("mean exact=%.5f | bias bf16=%.5f f32=%.5f | mean|err| bf16=%.5f f32=%.5f | max pair diff=%.6f",
		sumExact/nF, sumErrBF/nF, sumErrF32/nF, sumAbsBF/nF, sumAbsF32/nF, maxPairDiff)

	// estimator quality: 1-bit codes on hard anisotropic data, l2 distances
	// span twice the cosine range
	assert.Less(t, math.Abs(sumErrBF/nF), 0.04, "estimator bias")
	assert.Less(t, sumAbsBF/nF, 0.16, "estimator mean abs error")
	// bf16 vs f32 arm: bf16 RNE on norm2 (≤ 2⁻⁹ relative)
	// bounds the per-pair movement, and the aggregate error must not move
	assert.Less(t, maxPairDiff, 0.005, "bf16 norm2 may not move any distance more than RNE allows")
	assert.Less(t, math.Abs(sumErrBF/nF-sumErrF32/nF), 1e-3, "bf16 must not shift the estimator bias")
	assert.Less(t, math.Abs(sumAbsBF/nF-sumAbsF32/nF), 1e-3, "bf16 must not widen the estimator error")
}

// TestCenteredRQ1RejectsEmptyMean: an empty mean must fail construction even
// when inputDim is 0, as in RQ4. Accepted, every vector would encode alike,
// the persisted record would carry no mean, and the restart would restore
// the uncentered quantizer and panic on the first query.
func TestCenteredRQ1RejectsEmptyMean(t *testing.T) {
	for _, mean := range [][]float32{nil, {}} {
		_, err := NewCenteredBinaryRotationalQuantizer(0, 42, mean, distancer.NewCosineDistanceProvider())
		require.ErrorContains(t, err, "non-empty mean")
	}
}

// TestCenteredRQ1CompressedCompressed checks the code-to-code estimator
// (graph maintenance path), which reads both bf16 fields — norms via the
// angle estimate, the centered dots via the correction — against the
// float64 reference.
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

// TestRestoreCenteredRQ1Identity pins the restore boundary: the
// AddRQ/AddRQCentered record carries the rotation and the mean but not the
// query-encoder rounding, so restore regenerates the rounding from
// DefaultFastRotationSeed. This test proves the regeneration is exact — the
// restored quantizer must be observationally identical to the original:
// byte-identical rounding, byte-identical data codes, byte-identical query
// codes and bit-identical distances. Companion of
// compact.TestWALRoundTrip_AddRQCenteredBits1.
func TestRestoreCenteredRQ1Identity(t *testing.T) {
	for _, dim := range []int{64, 128, 1536} {
		t.Run(fmt.Sprintf("dim%d", dim), func(t *testing.T) {
			vecs, mean := crqTestData(8, dim, uint64(dim))
			cos := distancer.NewCosineDistanceProvider()
			rq, err := NewCenteredBinaryRotationalQuantizer(dim, DefaultFastRotationSeed, mean, cos)
			require.NoError(t, err)

			// Restore from exactly what the AddRQCentered record carries:
			// dims, rotation (output dim, rounds, swaps, signs) and mean.
			data := rq.Data()
			restored, err := RestoreCenteredBinaryRotationalQuantizer(
				int(data.InputDim), int(data.Rotation.OutputDim), int(data.Rotation.Rounds),
				data.Rotation.Swaps, data.Rotation.Signs, data.Mean, cos)
			require.NoError(t, err)

			require.Equal(t, rq.rounding, restored.rounding,
				"regenerated rounding must be byte-identical to the original")

			q := vecs[0]
			origQ, restQ := rq.NewDistancer(q).cq, restored.NewDistancer(q).cq
			assert.Equal(t, origQ, restQ, "query codes must be byte-identical after restore")

			for _, v := range vecs[1:] {
				origCode, restCode := rq.Encode(v), restored.Encode(v)
				require.Equal(t, origCode, restCode, "data codes must be byte-identical after restore")
				od, err := rq.NewDistancer(q).Distance(origCode)
				require.NoError(t, err)
				rd, err := restored.NewDistancer(q).Distance(restCode)
				require.NoError(t, err)
				require.Equal(t, od, rd, "distances must be bit-identical after restore")
			}
		})
	}

	// mean/dim mismatch stays rejected on the restore path
	_, err := RestoreCenteredBinaryRotationalQuantizer(4, 256, 3, nil, nil,
		[]float32{0.5}, distancer.NewCosineDistanceProvider())
	require.Error(t, err, "mean dimension mismatch must be rejected on restore")
}

// TestCenteredRQ1ShortInputCentersOmittedDims pins the padding semantics of
// the shared centerInto helper: a short input is zero-padded BEFORE
// centering, so an omitted coordinate centers to -mean[i] exactly as an
// explicit zero would. Encoding a short vector and its explicitly
// zero-padded twin must produce byte-identical codes, for data codes and
// query codes alike.
func TestCenteredRQ1ShortInputCentersOmittedDims(t *testing.T) {
	const dim = 128
	_, mean := crqTestData(16, dim, 3)
	rq, err := NewCenteredBinaryRotationalQuantizer(dim, 42, mean, distancer.NewCosineDistanceProvider())
	require.NoError(t, err)

	short := []float32{0.25, -0.5, 0.75}
	padded := make([]float32, dim)
	copy(padded, short)

	assert.Equal(t, rq.Encode(padded), rq.Encode(short),
		"short input must encode like its zero-padded twin")
	assert.Equal(t, rq.NewDistancer(padded).cq, rq.NewDistancer(short).cq,
		"short query must encode like its zero-padded twin")
}

// TestCenteredRQ1HighBiasDotRanking pins why the header stores the CENTERED
// dot ⟨μ,x-μ⟩ and not the absolute ⟨μ,x⟩: with μ = q = 1000·e₁ and data
// points straddling μ at ±0.25, the absolute dots 999750 and 1000250 both
// round to the same bf16 value, erasing the ranking before rescoring can see
// it. The centered dots are ∓250, bf16-exact, so the distances come out
// exact and ordered.
func TestCenteredRQ1HighBiasDotRanking(t *testing.T) {
	const dim = 128
	dot := distancer.NewDotProductProvider()
	mean := make([]float32, dim)
	mean[0] = 1000
	rq, err := NewCenteredBinaryRotationalQuantizer(dim, 42, mean, dot)
	require.NoError(t, err)

	q := make([]float32, dim)
	q[0] = 1000
	near := make([]float32, dim)
	near[0] = 999.75
	far := make([]float32, dim)
	far[0] = 1000.25

	d := rq.NewDistancer(q)
	dNear, err := d.Distance(rq.Encode(near))
	require.NoError(t, err)
	dFar, err := d.Distance(rq.Encode(far))
	require.NoError(t, err)

	// q-μ = 0, so the code estimate contributes nothing and the correction
	// carries the whole distance: -⟨x,q⟩ exactly.
	assert.InDelta(t, -999750, dNear, 1)
	assert.InDelta(t, -1000250, dFar, 1)
	assert.Greater(t, dNear, dFar,
		"dot ranking near μ must survive bf16; absolute ⟨μ,x⟩ storage collapses it to a tie")
}

// TestCenteredRQ1EmptyQuery pins the empty-query (zero-vector) semantics:
// the query must be centered unconditionally, so its centered form is -μ and
// the correction constant is ⟨μ,0⟩ = 0. Skipping the centering (the old
// len(q)>0 guard) made the L2 distance from an empty query to x=μ come out
// 0 instead of ‖μ‖², and the dot distance -‖μ‖² instead of 0.
func TestCenteredRQ1EmptyQuery(t *testing.T) {
	const dim = 128
	_, mean := crqTestData(16, dim, 6)
	var mu2 float64
	for _, m := range mean {
		mu2 += float64(m) * float64(m)
	}

	t.Run("dot", func(t *testing.T) {
		rq, err := NewCenteredBinaryRotationalQuantizer(dim, 42, mean, distancer.NewDotProductProvider())
		require.NoError(t, err)
		got, err := rq.NewDistancer(nil).Distance(rq.Encode(mean))
		require.NoError(t, err)
		assert.InDelta(t, 0, got, 0.02*mu2+1e-4, "dot distance to the zero query is -⟨x,0⟩ = 0")
	})

	t.Run("l2", func(t *testing.T) {
		rq, err := NewCenteredBinaryRotationalQuantizer(dim, 42, mean, distancer.NewL2SquaredProvider())
		require.NoError(t, err)
		got, err := rq.NewDistancer(nil).Distance(rq.Encode(mean))
		require.NoError(t, err)
		assert.InDelta(t, mu2, got, 0.05*mu2+1e-4, "l2 distance from the zero query to x=μ is ‖μ‖²")
	})
}

// TestCenteredRQ1L2SkipsNonFiniteCorrection pins that the L2 path branches
// around the centering correction instead of multiplying it by zero: with
// mean entries around 1e20 the mean products overflow float32, and
// 0·(±Inf) = NaN would poison a perfectly finite centered estimate. Both the
// query→code and the code→code estimators are covered.
func TestCenteredRQ1L2SkipsNonFiniteCorrection(t *testing.T) {
	const dim = 128
	l2 := distancer.NewL2SquaredProvider()
	mean := make([]float32, dim)
	mean[0] = 1e20 // meanNorm2 = 1e40 → +Inf in float32
	rq, err := NewCenteredBinaryRotationalQuantizer(dim, 42, mean, l2)
	require.NoError(t, err)

	x := make([]float32, dim)
	x[0] = 1e20
	code := rq.Encode(x)

	got, err := rq.NewDistancer(x).Distance(code)
	require.NoError(t, err)
	assert.False(t, math.IsNaN(float64(got)), "l2 distance must not be NaN")
	assert.InDelta(t, 0, got, 1e-3, "x and q coincide with μ, the centered l2 distance is 0")

	cc, err := rq.DistanceBetweenCompressedVectors(code, code)
	require.NoError(t, err)
	assert.False(t, math.IsNaN(float64(cc)), "code-to-code l2 distance must not be NaN")
	assert.InDelta(t, 0, cc, 1e-3)
}

// Centering remains restricted to bits 1 and 4 on the restore path.
func TestRestoreRQCompressorRejectsCenteredBits8(t *testing.T) {
	_, err := RestoreRQCompressor(distancer.NewCosineDistanceProvider(), 1e6, nil,
		4, 8, 256, 3, nil, nil, nil, []float32{0.5, 0.5, 0.5, 0.5},
		nil, nil, nil, "", nil)
	require.Error(t, err, "centered bits=8 restore must be rejected")
}

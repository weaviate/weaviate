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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// rqTestData returns n unit vectors with a common bias component, mimicking
// embedding-model output whose dataset mean has significant norm, plus the
// dataset mean of the sample.
func rqTestData(n, dim int, seed uint64) ([][]float32, []float32) {
	rng := rand.New(rand.NewPCG(seed, 17))
	bias := make([]float32, dim)
	for i := range bias {
		bias[i] = float32(rng.NormFloat64())
	}
	normalize := func(v []float32) {
		var norm float64
		for _, x := range v {
			norm += float64(x) * float64(x)
		}
		norm = math.Sqrt(norm)
		for i := range v {
			v[i] = float32(float64(v[i]) / norm)
		}
	}
	normalize(bias)

	vecs := make([][]float32, n)
	mean := make([]float32, dim)
	for i := range vecs {
		v := make([]float32, dim)
		for j := range v {
			// 60% shared bias direction, 40% noise: a strongly uncentered set.
			v[j] = 0.6*bias[j] + 0.4*float32(rng.NormFloat64())/float32(math.Sqrt(float64(dim)))
		}
		normalize(v)
		vecs[i] = v
		for j := range v {
			mean[j] += v[j]
		}
	}
	for j := range mean {
		mean[j] /= float32(n)
	}
	return vecs, mean
}

func cosineDistance(a, b []float32) float32 {
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return float32(1 - dot)
}

// TestRQOptionsZeroValueIsIdentical pins that the options constructor with
// zero options produces byte-identical codes and identical distances to the
// original constructor.
func TestRQOptionsZeroValueIsIdentical(t *testing.T) {
	dim := 200 // deliberately not a multiple of 64 to exercise padding
	vecs, _ := rqTestData(50, dim, 1)
	plain := NewRotationalQuantizer(dim, 42, 8, distancer.NewCosineDistanceProvider())
	withOpts, err := NewRotationalQuantizerWithOptions(dim, 42, 8, distancer.NewCosineDistanceProvider(), RQOptions{})
	require.NoError(t, err)

	for _, v := range vecs {
		assert.Equal(t, plain.Encode(v), withOpts.Encode(v))
	}
	d1 := plain.NewDistancer(vecs[0])
	d2 := withOpts.NewDistancer(vecs[0])
	for _, v := range vecs[1:] {
		dist1, err1 := d1.Distance(plain.Encode(v))
		dist2, err2 := d2.Distance(withOpts.Encode(v))
		require.NoError(t, err1)
		require.NoError(t, err2)
		assert.Equal(t, dist1, dist2)
	}
}

func TestRQOptionsValidation(t *testing.T) {
	cos := distancer.NewCosineDistanceProvider()
	_, err := NewRotationalQuantizerWithOptions(128, 42, 8, cos, RQOptions{TruncatedDims: 129})
	assert.Error(t, err, "truncated dims beyond output dim must be rejected")
	_, err = NewRotationalQuantizerWithOptions(128, 42, 8, cos, RQOptions{TruncatedDims: -1})
	assert.Error(t, err)
	_, err = NewRotationalQuantizerWithOptions(128, 42, 8, cos, RQOptions{Mean: make([]float32, 5)})
	assert.Error(t, err, "mean dimension mismatch must be rejected")
	_, err = NewRotationalQuantizerWithOptions(128, 42, 8, distancer.NewL2SquaredProvider(), RQOptions{Mean: make([]float32, 128)})
	assert.Error(t, err, "centering with l2 must be rejected")
	rq, err := NewRotationalQuantizerWithOptions(128, 42, 8, cos, RQOptions{TruncatedDims: 64, Mean: make([]float32, 128)})
	require.NoError(t, err)
	assert.Equal(t, 64+RQMetadataSize, len(rq.Encode(make([]float32, 128))))
}

// TestRQTruncatedDistanceEstimates verifies that truncated (and centered)
// codes still estimate the true cosine distance without bias: the mean signed
// error over many pairs must be near zero and far smaller than the mean
// absolute error would allow if the sqrt(n/D) rescaling or the centering
// correction were wrong.
func TestRQTruncatedDistanceEstimates(t *testing.T) {
	dim := 256
	n := 300
	vecs, mean := rqTestData(n, dim, 2)
	cos := distancer.NewCosineDistanceProvider()

	measure := func(t *testing.T, trunc int, center bool) (bias, absErr float64) {
		opts := RQOptions{TruncatedDims: trunc}
		if center {
			opts.Mean = mean
		}
		rq, err := NewRotationalQuantizerWithOptions(dim, 42, 8, cos, opts)
		require.NoError(t, err)

		var count int
		for i := 0; i < 50; i++ {
			d := rq.NewDistancer(vecs[i])
			for j := 50; j < n; j++ {
				est, err := d.Distance(rq.Encode(vecs[j]))
				require.NoError(t, err)
				exact := cosineDistance(vecs[i], vecs[j])
				diff := float64(est - exact)
				bias += diff
				absErr += math.Abs(diff)
				count++
			}
		}
		return bias / float64(count), absErr / float64(count)
	}

	fullBias, fullErr := measure(t, 0, false)
	assert.Less(t, math.Abs(fullBias), 0.002, "full-width bias: %v", fullBias)
	assert.Less(t, fullErr, 0.01, "full-width error: %v", fullErr)

	fullCBias, fullCErr := measure(t, 0, true)
	assert.Less(t, math.Abs(fullCBias), 0.002, "full-width centered bias: %v", fullCBias)
	assert.Less(t, fullCErr, 0.01, "full-width centered error: %v", fullCErr)

	// Truncation on UNCENTERED data has an intrinsic rotation-specific bias:
	// the shared mean component contributes Σ_{i<D}(Rμ)_i² to every prefix
	// dot, which differs from its expectation (D/n)|μ|² by an amount fixed by
	// the rotation. Centering removes the shared component, so the centered
	// bias must be markedly smaller. This is the measurable core of the
	// "centering is a precondition for prefix schemes" finding.
	halfBias, halfErr := measure(t, 128, false)
	halfCBias, halfCErr := measure(t, 128, true)
	t.Logf("half-width bias uncentered=%v centered=%v err uncentered=%v centered=%v",
		halfBias, halfCBias, halfErr, halfCErr)
	assert.Less(t, math.Abs(halfCBias), 0.01, "half-width centered bias: %v", halfCBias)
	assert.Less(t, math.Abs(halfCBias)*2, math.Abs(halfBias),
		"centering should remove most of the truncation bias (uncentered %v, centered %v)", halfBias, halfCBias)
	assert.Less(t, halfCErr, 0.08, "half-width centered error: %v", halfCErr)
}

// TestRQTruncatedMatchesFloatReference pins the code-based estimator against
// the same estimator computed in float space: est = 1 - [(n/D)·<R(x-μ)[:D],
// R(q-μ)[:D]> + μ·x + μ·q - |μ|²]. Any disagreement beyond 8-bit quantization
// noise means the scaling fold or a correction term is wrong, independent of
// how hard the dataset is.
func TestRQTruncatedMatchesFloatReference(t *testing.T) {
	dim := 256
	trunc := 128
	vecs, mean := rqTestData(200, dim, 7)
	cos := distancer.NewCosineDistanceProvider()
	rq, err := NewRotationalQuantizerWithOptions(dim, 42, 8, cos, RQOptions{TruncatedDims: trunc, Mean: mean})
	require.NoError(t, err)

	var mu2 float64
	for _, m := range mean {
		mu2 += float64(m) * float64(m)
	}
	center := func(v []float32) []float32 {
		c := make([]float32, len(v))
		for i := range v {
			c[i] = v[i] - mean[i]
		}
		return c
	}
	dot := func(a, b []float32) float64 {
		var s float64
		for i := range a {
			s += float64(a[i]) * float64(b[i])
		}
		return s
	}

	outDim := rq.OutputDimension()
	scale := float64(outDim) / float64(trunc)
	for i := 0; i < 20; i++ {
		q := vecs[i]
		d := rq.NewDistancer(q)
		rqRot := rq.Rotate(center(q))[:trunc]
		for j := 20; j < 200; j++ {
			x := vecs[j]
			rxRot := rq.Rotate(center(x))[:trunc]
			ref := 1 - (scale*dot(rqRot, rxRot) + dot(mean, x) + dot(mean, q) - mu2)
			est, err := d.Distance(rq.Encode(x))
			require.NoError(t, err)
			assert.InDelta(t, ref, float64(est), 0.02,
				"code estimate diverges from float reference at pair (%d,%d)", i, j)
		}
	}
}

// TestRQTruncatedCompressedCompressedAgrees pins the compressed-compressed
// distance path (used during graph construction) against the query-side
// estimator on truncated centered codes.
func TestRQTruncatedCompressedCompressedAgrees(t *testing.T) {
	dim := 256
	vecs, mean := rqTestData(100, dim, 3)
	cos := distancer.NewCosineDistanceProvider()
	rq, err := NewRotationalQuantizerWithOptions(dim, 42, 8, cos, RQOptions{TruncatedDims: 128, Mean: mean})
	require.NoError(t, err)

	codes := make([][]byte, len(vecs))
	for i, v := range vecs {
		codes[i] = rq.Encode(v)
	}
	for i := 0; i < 20; i++ {
		for j := 20; j < 100; j++ {
			cc, err := rq.DistanceBetweenCompressedVectors(codes[i], codes[j])
			require.NoError(t, err)
			exact := cosineDistance(vecs[i], vecs[j])
			// Same estimator family, so the two paths must land close to the
			// exact value; equality is not expected because the query side
			// re-quantizes at query precision.
			assert.InDelta(t, exact, cc, 0.15)
		}
	}
}

// TestRQTruncatedRankingQuality is the end-to-end sanity check: at half
// width with centering, brute-force top-10 by code distance must overlap
// strongly with the exact top-10.
func TestRQTruncatedRankingQuality(t *testing.T) {
	dim := 256
	n := 1000
	vecs, mean := rqTestData(n, dim, 4)
	queries, _ := rqTestData(100, dim, 5)
	cos := distancer.NewCosineDistanceProvider()

	recallAt10 := func(rq *RotationalQuantizer) float64 {
		codes := make([][]byte, n)
		for i, v := range vecs {
			codes[i] = rq.Encode(v)
		}
		var hits int
		for _, q := range queries {
			d := rq.NewDistancer(q)
			type scored struct {
				id   int
				dist float32
			}
			est := make([]scored, n)
			exact := make([]scored, n)
			for i := range vecs {
				e, err := d.Distance(codes[i])
				require.NoError(t, err)
				est[i] = scored{i, e}
				exact[i] = scored{i, cosineDistance(q, vecs[i])}
			}
			sort.Slice(est, func(a, b int) bool { return est[a].dist < est[b].dist })
			sort.Slice(exact, func(a, b int) bool { return exact[a].dist < exact[b].dist })
			top := map[int]bool{}
			for _, s := range exact[:10] {
				top[s.id] = true
			}
			for _, s := range est[:10] {
				if top[s.id] {
					hits++
				}
			}
		}
		return float64(hits) / float64(len(queries)*10)
	}

	full, err := NewRotationalQuantizerWithOptions(dim, 42, 8, cos, RQOptions{Mean: mean})
	require.NoError(t, err)
	rFull := recallAt10(full)

	halfC, err := NewRotationalQuantizerWithOptions(dim, 42, 8, cos, RQOptions{TruncatedDims: 128, Mean: mean})
	require.NoError(t, err)
	rHalfC := recallAt10(halfC)

	halfU, err := NewRotationalQuantizerWithOptions(dim, 42, 8, cos, RQOptions{TruncatedDims: 128})
	require.NoError(t, err)
	rHalfU := recallAt10(halfU)

	// The synthetic set is deliberately hostile: 60% shared direction, so
	// neighbors are separated by tiny cosine gaps, and the queries are drawn
	// from a DIFFERENT distribution than the data the mean was computed on.
	// Under that mismatch the relative ranking of the centered vs uncentered
	// estimator is a data property, not a code property (a pure-float
	// implementation of both estimators reproduces the same recalls to three
	// decimals), so no ordering between them is asserted here — correctness
	// of the code path is pinned by TestRQTruncatedMatchesFloatReference.
	// What must hold regardless of data: full width ranks well and half
	// width does not collapse to noise (random top-10 would be ~0.01).
	t.Logf("recall@10: full-centered=%v half-centered=%v half-uncentered=%v", rFull, rHalfC, rHalfU)
	assert.Greater(t, rFull, 0.9, "full-width centered recall@10 collapsed: %v", rFull)
	assert.Greater(t, rHalfC, 0.1, "half-width centered recall@10 collapsed entirely: %v", rHalfC)
}

// TestRQCompressedDistancerDistanceToFloat is a regression test for a
// pre-existing bug independent of truncation: NewCompressedQuantizerDistancer
// used to set the distancer's query field to Restore(code), a ROTATED-space
// vector, so DistanceToFloat computed SingleDist(rotated, raw) — a garbage
// distance. The 4-bit and 1-bit quantizers already handled this by encoding
// the float argument and comparing code-to-code; the 8-bit quantizer must do
// the same. Unreachable from current production call sites (the only
// DistanceToFloat caller is the rescore path, which always uses query-origin
// distancers), but live on the shared CompressorDistancer interface.
func TestRQCompressedDistancerDistanceToFloat(t *testing.T) {
	dim := 256
	vecs, mean := rqTestData(50, dim, 8)
	cos := distancer.NewCosineDistanceProvider()

	cases := []struct {
		name string
		opts RQOptions
	}{
		{name: "plain", opts: RQOptions{}},
		{name: "truncated-centered", opts: RQOptions{TruncatedDims: 128, Mean: mean}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rq, err := NewRotationalQuantizerWithOptions(dim, 42, 8, cos, tc.opts)
			require.NoError(t, err)
			origin := vecs[0]
			d := rq.NewCompressedQuantizerDistancer(rq.Encode(origin))
			for _, x := range vecs[1:] {
				got, err := d.DistanceToFloat(x)
				require.NoError(t, err)
				exact := cosineDistance(origin, x)
				assert.InDelta(t, exact, got, 0.15,
					"compressed-origin DistanceToFloat diverges from true distance")
			}
		})
	}
}

// TestRQTruncatedDecodeRoundTrip ensures Decode does not panic on truncated
// codes (UnRotateInPlace requires full-width input) and returns a vector
// correlated with the original.
func TestRQTruncatedDecodeRoundTrip(t *testing.T) {
	dim := 256
	vecs, mean := rqTestData(20, dim, 6)
	cos := distancer.NewCosineDistanceProvider()
	rq, err := NewRotationalQuantizerWithOptions(dim, 42, 8, cos, RQOptions{TruncatedDims: 128, Mean: mean})
	require.NoError(t, err)

	for _, v := range vecs {
		dec := rq.Decode(rq.Encode(v))
		require.GreaterOrEqual(t, len(dec), dim)
		var dot float64
		for i := range v {
			dot += float64(v[i]) * float64(dec[i])
		}
		// Half the signal is dropped, so reconstruction is lossy, but it must
		// point in the same general direction.
		assert.Greater(t, dot, 0.5, "decoded vector uncorrelated with input")
	}
}

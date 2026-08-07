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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// --- 4-bit quantizer ---

func TestRQ4OptionsZeroValueIsIdentical(t *testing.T) {
	dim := 200
	vecs, _ := rqTestData(50, dim, 11)
	cos := distancer.NewCosineDistanceProvider()
	plain := NewFourBitRotationalQuantizer(dim, 42, cos)
	withOpts, err := NewFourBitRotationalQuantizerWithOptions(dim, 42, cos, RQOptions{})
	require.NoError(t, err)
	for _, v := range vecs {
		assert.Equal(t, plain.Encode(v), withOpts.Encode(v))
	}
}

func TestRQ4OptionsValidation(t *testing.T) {
	cos := distancer.NewCosineDistanceProvider()
	_, err := NewFourBitRotationalQuantizerWithOptions(128, 42, cos, RQOptions{TruncatedDims: 129})
	assert.Error(t, err)
	_, err = NewFourBitRotationalQuantizerWithOptions(128, 42, cos, RQOptions{TruncatedDims: 63})
	assert.Error(t, err, "odd truncation must be rejected (plane layout)")
	_, err = NewFourBitRotationalQuantizerWithOptions(128, 42, distancer.NewL2SquaredProvider(), RQOptions{Mean: make([]float32, 128)})
	assert.Error(t, err)
	rq, err := NewFourBitRotationalQuantizerWithOptions(128, 42, cos, RQOptions{TruncatedDims: 64, Mean: make([]float32, 128)})
	require.NoError(t, err)
	assert.Equal(t, RQ4MetadataSize+32, len(rq.Encode(make([]float32, 128))),
		"64 retained dims must pack into 32 bytes plus metadata")
}

// TestRQ4TruncatedEstimates checks the asymmetric query-data estimator and
// the compressed-compressed estimator on truncated centered codes against
// exact cosine distances. 4-bit codes with interval clipping are coarser than
// 8-bit, so tolerances are wider; the bias check is the sharp part.
func TestRQ4TruncatedEstimates(t *testing.T) {
	dim := 256
	n := 300
	vecs, mean := rqTestData(n, dim, 12)
	cos := distancer.NewCosineDistanceProvider()

	for _, tc := range []struct {
		name  string
		trunc int
	}{
		{name: "full-centered", trunc: 0},
		{name: "half-centered", trunc: 128},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rq, err := NewFourBitRotationalQuantizerWithOptions(dim, 42, cos, RQOptions{TruncatedDims: tc.trunc, Mean: mean})
			require.NoError(t, err)

			var bias, absErr float64
			var count int
			for i := 0; i < 30; i++ {
				d := rq.NewDistancer(vecs[i])
				for j := 30; j < n; j++ {
					code := rq.Encode(vecs[j])
					est, err := d.Distance(code)
					require.NoError(t, err)
					exact := cosineDistance(vecs[i], vecs[j])
					bias += float64(est - exact)
					absErr += math.Abs(float64(est - exact))
					count++

					cc, err := rq.DistanceBetweenCompressedVectors(rq.Encode(vecs[i]), code)
					require.NoError(t, err)
					assert.InDelta(t, exact, cc, 0.2)
				}
			}
			bias /= float64(count)
			absErr /= float64(count)
			assert.Less(t, math.Abs(bias), 0.02, "4-bit centered bias: %v", bias)
			assert.Less(t, absErr, 0.1, "4-bit centered error: %v", absErr)
		})
	}
}

func TestRQ4TruncatedDecodeDoesNotPanic(t *testing.T) {
	dim := 256
	vecs, mean := rqTestData(10, dim, 13)
	cos := distancer.NewCosineDistanceProvider()
	rq, err := NewFourBitRotationalQuantizerWithOptions(dim, 42, cos, RQOptions{TruncatedDims: 128, Mean: mean})
	require.NoError(t, err)
	for _, v := range vecs {
		dec := rq.Decode(rq.Encode(v))
		require.Equal(t, dim, len(dec))
		var dot float64
		for i := range v {
			dot += float64(v[i]) * float64(dec[i])
		}
		assert.Greater(t, dot, 0.5, "decoded vector uncorrelated with input")
	}
}

// --- 1-bit quantizer ---

func TestBRQOptionsZeroValueIsIdentical(t *testing.T) {
	dim := 512
	vecs, _ := rqTestData(50, dim, 14)
	cos := distancer.NewCosineDistanceProvider()
	plain, err := NewBinaryRotationalQuantizer(dim, 42, cos)
	require.NoError(t, err)
	withOpts, err := NewBinaryRotationalQuantizerWithOptions(dim, 42, cos, RQOptions{})
	require.NoError(t, err)
	for _, v := range vecs {
		assert.Equal(t, plain.Encode(v), withOpts.Encode(v))
	}
}

func TestBRQOptionsValidation(t *testing.T) {
	cos := distancer.NewCosineDistanceProvider()
	_, err := NewBinaryRotationalQuantizerWithOptions(512, 42, cos, RQOptions{TruncatedDims: 256})
	assert.Error(t, err, "truncation is unsupported for 1-bit codes")
	_, err = NewBinaryRotationalQuantizerWithOptions(512, 42, distancer.NewL2SquaredProvider(), RQOptions{Mean: make([]float32, 512)})
	assert.Error(t, err)
}

// TestBRQCenteredLayout pins the centered code layout: one extra metadata
// word carrying <mean, x>, sign bits shifted by one word.
func TestBRQCenteredLayout(t *testing.T) {
	dim := 512
	vecs, mean := rqTestData(10, dim, 15)
	cos := distancer.NewCosineDistanceProvider()
	plain, err := NewBinaryRotationalQuantizer(dim, 42, cos)
	require.NoError(t, err)
	centered, err := NewBinaryRotationalQuantizerWithOptions(dim, 42, cos, RQOptions{Mean: mean})
	require.NoError(t, err)

	for _, v := range vecs {
		pc := plain.Encode(v)
		cc := centered.Encode(v)
		assert.Equal(t, len(pc)+1, len(cc), "centered code must carry one extra metadata word")
		var muX float64
		for i := range v {
			muX += float64(mean[i]) * float64(v[i])
		}
		assert.InDelta(t, muX, float64(centered.codeMuX(cc)), 1e-4)
	}
}

// TestBRQCenteredEstimates checks the centered 1-bit estimator against exact
// cosine distances. This is the configuration where centering matters most:
// the doc's measurements show uncentered sign quantization after rotation
// loses the most information on biased data.
func TestBRQCenteredEstimates(t *testing.T) {
	dim := 512
	n := 300
	vecs, mean := rqTestData(n, dim, 16)
	cos := distancer.NewCosineDistanceProvider()
	rq, err := NewBinaryRotationalQuantizerWithOptions(dim, 42, cos, RQOptions{Mean: mean})
	require.NoError(t, err)

	var bias, absErr float64
	var count int
	for i := 0; i < 30; i++ {
		d := rq.NewDistancer(vecs[i])
		for j := 30; j < n; j++ {
			code := rq.Encode(vecs[j])
			est, err := d.Distance(code)
			require.NoError(t, err)
			exact := cosineDistance(vecs[i], vecs[j])
			bias += float64(est - exact)
			absErr += math.Abs(float64(est - exact))
			count++
		}
	}
	bias /= float64(count)
	absErr /= float64(count)
	// 1-bit data codes are coarse; the centered estimator must still be
	// nearly unbiased, and its average error bounded.
	assert.Less(t, math.Abs(bias), 0.03, "1-bit centered bias: %v", bias)
	assert.Less(t, absErr, 0.1, "1-bit centered error: %v", absErr)
}

// TestBRQCenteredCompressedCompressed checks the code-code path (graph
// construction) with the centered layout and corrections.
func TestBRQCenteredCompressedCompressed(t *testing.T) {
	dim := 512
	vecs, mean := rqTestData(100, dim, 17)
	cos := distancer.NewCosineDistanceProvider()
	rq, err := NewBinaryRotationalQuantizerWithOptions(dim, 42, cos, RQOptions{Mean: mean})
	require.NoError(t, err)

	codes := make([][]uint64, len(vecs))
	for i, v := range vecs {
		codes[i] = rq.Encode(v)
	}
	for i := 0; i < 20; i++ {
		for j := 20; j < 100; j++ {
			cc, err := rq.DistanceBetweenCompressedVectors(codes[i], codes[j])
			require.NoError(t, err)
			exact := cosineDistance(vecs[i], vecs[j])
			// The angle-from-Hamming estimator is the coarsest path; a loose
			// tolerance still catches layout or correction-term mistakes,
			// which produce errors of O(|mean|²) ≈ 0.7 on this data.
			assert.InDelta(t, exact, cc, 0.25)
		}
	}
}

func TestBRQCenteredDecodeRoundTrip(t *testing.T) {
	dim := 512
	vecs, mean := rqTestData(10, dim, 18)
	cos := distancer.NewCosineDistanceProvider()
	rq, err := NewBinaryRotationalQuantizerWithOptions(dim, 42, cos, RQOptions{Mean: mean})
	require.NoError(t, err)
	for _, v := range vecs {
		dec := rq.Decode(rq.Encode(v))
		require.Equal(t, dim, len(dec))
		var dot, norm float64
		for i := range v {
			dot += float64(v[i]) * float64(dec[i])
			norm += float64(dec[i]) * float64(dec[i])
		}
		assert.Greater(t, dot/math.Sqrt(norm), 0.5, "decoded vector uncorrelated with input")
	}
}

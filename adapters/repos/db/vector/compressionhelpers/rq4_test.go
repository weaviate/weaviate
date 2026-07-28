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
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func allMetrics() []distancer.Provider {
	return []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
}

// The SIMD-assisted Distance (offset int8 codes) and the scalar packed-kernel
// path compute the same integer dot product, so their estimates must agree up
// to float rounding.
func TestRQ4SIMDAndScalarDistancesAreIdentical(t *testing.T) {
	rng := newRNG(64521467)
	n := 100
	for range n {
		d := 2 + rng.IntN(2000)
		for _, m := range allMetrics() {
			rq := compressionhelpers.NewFourBitRotationalQuantizer(d, rng.Uint64(), m)
			q, x := randomUnitVector(d, rng), randomUnitVector(d, rng)
			cx := rq.Encode(x)
			dist := rq.NewDistancer(q)
			simdEstimate, err := dist.Distance(cx)
			require.NoError(t, err)
			scalarEstimate, err := dist.DistanceScalar(cx)
			require.NoError(t, err)
			assert.Less(t, math.Abs(float64(simdEstimate-scalarEstimate)), 2e-5,
				"dim %d, metric %s", d, m.Type())
		}
	}
}

func TestRQ4DistanceEstimate(t *testing.T) {
	a := float32(1.0 / math.Sqrt2)
	q := []float32{1.0, 0.0}
	x := []float32{a, a}

	dim := 2
	var seed uint64 = 42

	for _, m := range allMetrics() {
		rq := compressionhelpers.NewFourBitRotationalQuantizer(dim, seed, m)
		cq, cx := rq.Encode(q), rq.Encode(x)
		dist := rq.NewDistancer(q)
		distancerEstimate, err := dist.Distance(cx)
		require.NoError(t, err)
		compressedEstimate, err := rq.DistanceBetweenCompressedVectors(cq, cx)
		require.NoError(t, err)

		target, _ := m.SingleDist(q, x)
		// 4-bit codes over a 64-dim padded rotation: the tolerance is looser
		// than for the 8-bit quantizer. The symmetric estimate quantizes both
		// sides at 4 bits and is noisier than the asymmetric one, which uses
		// an 8-bit query encoding.
		assert.Less(t, absDiff(distancerEstimate, target), 0.05, m.Type())
		assert.Less(t, absDiff(compressedEstimate, target), 0.1, m.Type())
	}
}

// The relative error of the dot product estimate between random unit vectors
// should decrease as roughly 1/sqrt(D). We use generous tolerances since this
// is a randomized test with a fixed seed.
func TestRQ4EstimationQuality(t *testing.T) {
	testCases := []struct {
		dim    int
		maxErr float64
	}{
		{dim: 256, maxErr: 0.05},
		{dim: 768, maxErr: 0.03},
		{dim: 1536, maxErr: 0.02},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("d%d", tc.dim), func(t *testing.T) {
			rng := newRNG(uint64(tc.dim))
			n := 100
			var avgAbsErr float64
			rq := compressionhelpers.NewFourBitRotationalQuantizer(tc.dim, rng.Uint64(), distancer.NewDotProductProvider())
			for range n {
				q, x := randomUnitVector(tc.dim, rng), randomUnitVector(tc.dim, rng)
				trueDot := float64(dotFloat(q, x))
				dist := rq.NewDistancer(q)
				est, err := dist.Distance(rq.Encode(x))
				require.NoError(t, err)
				avgAbsErr += math.Abs(-float64(est) - trueDot)
			}
			avgAbsErr /= float64(n)
			assert.Less(t, avgAbsErr, tc.maxErr)
		})
	}
}

func dotFloat(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

// Encoding and restoring should approximate the rotated vector within about
// a quantization step for entries inside the clip interval (the least-squares
// rescaling of the reconstruction slightly perturbs the half-step bound).
// Entries outside the interval are clamped and may have a larger error, but
// the per-vector clip search guarantees the total squared error is never
// worse than encoding over the full [min, max] range.
func TestRQ4EncodeRestore(t *testing.T) {
	rng := newRNG(7542)
	n := 10
	for range n {
		d := 2 + rng.IntN(1000)
		rq := compressionhelpers.NewFourBitRotationalQuantizer(d, rng.Uint64(), distancer.NewCosineDistanceProvider())
		x := randomUnitVector(d, rng)
		code := compressionhelpers.RQ4Code(rq.Encode(x))
		restored := rq.Restore(code)
		rx := rq.Rotate(x)
		lower, step := code.Lower(), code.Step()
		upper := lower + 15*step
		var clippedSSE float64
		for i := range rx {
			diff := math.Abs(float64(rx[i] - restored[i]))
			clippedSSE += diff * diff
			if rx[i] >= lower && rx[i] <= upper {
				assert.LessOrEqual(t, diff, float64(step)+1e-6)
			}
		}

		// Reference: quantization over the full [min, max] range.
		minV, maxV := slices.Min(rx), slices.Max(rx)
		fullStep := (maxV - minV) / 15
		var fullSSE float64
		for _, v := range rx {
			c := float32(int((v-minV)/fullStep + 0.5))
			diff := float64(v - (minV + fullStep*c))
			fullSSE += diff * diff
		}
		assert.LessOrEqual(t, clippedSSE, fullSSE+1e-6)
	}
}

// Decode should invert Encode up to quantization error and return a vector of
// the original dimensionality.
func TestRQ4Decode(t *testing.T) {
	rng := newRNG(11)
	dims := []int{2, 64, 100, 128, 700, 1536}
	for _, d := range dims {
		rq := compressionhelpers.NewFourBitRotationalQuantizer(d, rng.Uint64(), distancer.NewL2SquaredProvider())
		x := randomUnitVector(d, rng)
		decoded := rq.Decode(rq.Encode(x))
		require.Equal(t, d, len(decoded))
		var mse float64
		for i := range x {
			diff := float64(x[i] - decoded[i])
			mse += diff * diff
		}
		// ~4 bits of precision on a unit vector distributed over d dims.
		assert.Less(t, mse/float64(d), 0.01, "dim %d", d)
	}
}

func TestRQ4HandlesAbnormalVectorsGracefully(t *testing.T) {
	rng := newRNG(1234)
	d := 128
	rq := compressionhelpers.NewFourBitRotationalQuantizer(d, rng.Uint64(), distancer.NewL2SquaredProvider())

	cases := [][]float32{
		nil,
		{},
		make([]float32, d),       // zero vector
		make([]float32, 3),       // shorter than dim
		make([]float32, 10*d),    // longer than dim
		{float32(math.Inf(1))},   // not finite
		make([]float32, d-1)[:0], // empty again
	}
	x := randomUnitVector(d, rng)
	cx := rq.Encode(x)
	for i, q := range cases {
		code := rq.Encode(q)
		require.Equal(t, len(cx), len(code), "case %d", i)
		dist := rq.NewDistancer(q)
		_, err := dist.Distance(cx)
		assert.NoError(t, err, "case %d", i)
		_, err = rq.DistanceBetweenCompressedVectors(code, cx)
		assert.NoError(t, err, "case %d", i)
	}
}

// The zero vector should have zero dot product estimate against anything.
func TestRQ4ZeroVector(t *testing.T) {
	rng := newRNG(99)
	d := 256
	rq := compressionhelpers.NewFourBitRotationalQuantizer(d, rng.Uint64(), distancer.NewDotProductProvider())
	zero := make([]float32, d)
	x := randomUnitVector(d, rng)

	dist := rq.NewDistancer(zero)
	est, err := dist.Distance(rq.Encode(x))
	require.NoError(t, err)
	assert.Equal(t, float32(0), -est)

	distX := rq.NewDistancer(x)
	est, err = distX.Distance(rq.Encode(zero))
	require.NoError(t, err)
	assert.Equal(t, float32(0), -est)
}

func TestRQ4CompressedDistancerMatchesCompressedDistances(t *testing.T) {
	rng := newRNG(4321)
	d := 384
	for _, m := range allMetrics() {
		rq := compressionhelpers.NewFourBitRotationalQuantizer(d, rng.Uint64(), m)
		q, x := randomUnitVector(d, rng), randomUnitVector(d, rng)
		cq, cx := rq.Encode(q), rq.Encode(x)

		dist := rq.NewCompressedQuantizerDistancer(cq)
		got, err := dist.Distance(cx)
		require.NoError(t, err)
		want, err := rq.DistanceBetweenCompressedVectors(cq, cx)
		require.NoError(t, err)
		assert.Equal(t, want, got)

		gotFloat, err := dist.DistanceToFloat(x)
		require.NoError(t, err)
		assert.Equal(t, want, gotFloat)
	}
}

func TestRQ4DistanceCodeLengthMismatch(t *testing.T) {
	rng := newRNG(5)
	rq := compressionhelpers.NewFourBitRotationalQuantizer(128, rng.Uint64(), distancer.NewL2SquaredProvider())
	other := compressionhelpers.NewFourBitRotationalQuantizer(1536, rng.Uint64(), distancer.NewL2SquaredProvider())

	dist := rq.NewDistancer(randomUnitVector(128, rng))
	_, err := dist.Distance(other.Encode(randomUnitVector(1536, rng)))
	assert.Error(t, err)
	_, err = dist.DistanceScalar(other.Encode(randomUnitVector(1536, rng)))
	assert.Error(t, err)
	_, err = rq.DistanceBetweenCompressedVectors(
		rq.Encode(randomUnitVector(128, rng)), other.Encode(randomUnitVector(1536, rng)))
	assert.Error(t, err)
}

func TestRQ4KernelsAgree(t *testing.T) {
	rng := newRNG(77)
	for _, d := range []int{64, 128, 256, 1536} {
		q := make([]byte, d)
		packed := make([]byte, d/2)
		for i := range q {
			q[i] = byte(rng.UintN(256))
		}
		for i := range packed {
			packed[i] = byte(rng.UintN(256))
		}
		var want uint32
		half := len(packed)
		for i := range q {
			nib := packed[i%half] & 0x0F
			if i >= half {
				nib = packed[i%half] >> 4
			}
			want += uint32(q[i]) * uint32(nib)
		}
		assert.Equal(t, want, compressionhelpers.DotByteNibble(q, packed))

		var wantNN uint32
		other := make([]byte, d/2)
		for i := range other {
			other[i] = byte(rng.UintN(256))
		}
		for i := range packed {
			wantNN += uint32(packed[i]&0x0F)*uint32(other[i]&0x0F) +
				uint32(packed[i]>>4)*uint32(other[i]>>4)
		}
		assert.Equal(t, wantNN, compressionhelpers.DotNibbleNibble(packed, other))
	}
}

func TestRQ4CompressionRatio(t *testing.T) {
	rq := compressionhelpers.NewFourBitRotationalQuantizer(1536, 42, distancer.NewL2SquaredProvider())
	stats := rq.Stats()
	assert.Equal(t, "rq", stats.CompressionType())
	// 1536 float32 = 6144 bytes vs 16 + 768 = 784 bytes.
	assert.InDelta(t, 6144.0/784.0, stats.CompressionRatio(1536), 1e-9)
}

func BenchmarkRQ4Distance(b *testing.B) {
	rng := newRNG(42)
	dimensions := []int{128, 256, 384, 768, 1024, 1536, 2048}
	for _, dim := range dimensions {
		rq := compressionhelpers.NewFourBitRotationalQuantizer(dim, rng.Uint64(), distancer.NewCosineDistanceProvider())
		q, x := randomUnitVector(dim, rng), randomUnitVector(dim, rng)
		cx := rq.Encode(x)
		dist := rq.NewDistancer(q)
		b.Run(fmt.Sprintf("simd-d%d", dim), func(b *testing.B) {
			for b.Loop() {
				dist.Distance(cx)
			}
			b.ReportMetric((float64(b.N)/1e6)/float64(b.Elapsed().Seconds()), "m.ops/sec")
		})
		b.Run(fmt.Sprintf("scalar-d%d", dim), func(b *testing.B) {
			for b.Loop() {
				dist.DistanceScalar(cx)
			}
			b.ReportMetric((float64(b.N)/1e6)/float64(b.Elapsed().Seconds()), "m.ops/sec")
		})
	}
}

func BenchmarkRQ4Encode(b *testing.B) {
	rng := newRNG(42)
	dimensions := []int{256, 1024, 1536}
	for _, dim := range dimensions {
		rq := compressionhelpers.NewFourBitRotationalQuantizer(dim, rng.Uint64(), distancer.NewCosineDistanceProvider())
		x := randomUnitVector(dim, rng)
		b.Run(fmt.Sprintf("d%d", dim), func(b *testing.B) {
			for b.Loop() {
				rq.Encode(x)
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
		})
	}
}

// Compares the affine-grid encoder against the reference extended-RaBitQ
// encoder at equal candidate counts (10) and at the reference's default
// dense search (25).
func BenchmarkEncodeGridComparison(b *testing.B) {
	rng := newRNG(42)
	tenFactors := make([]float32, 10)
	for i := range tenFactors {
		tenFactors[i] = 0.55 + 0.05*float32(i)
	}
	dense := compressionhelpers.PureRaBitQ4ScaleFactors

	for _, dim := range []int{384, 1024, 1536} {
		rq := compressionhelpers.NewFourBitRotationalQuantizer(dim, rng.Uint64(), distancer.NewCosineDistanceProvider())
		x := randomUnitVector(dim, rng)
		b.Run(fmt.Sprintf("rq4-d%d", dim), func(b *testing.B) {
			for b.Loop() {
				rq.Encode(x)
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
		})
		b.Run(fmt.Sprintf("xrbq4-10cand-d%d", dim), func(b *testing.B) {
			compressionhelpers.PureRaBitQ4ScaleFactors = tenFactors
			defer func() { compressionhelpers.PureRaBitQ4ScaleFactors = dense }()
			for b.Loop() {
				rq.EncodePureRaBitQ4(x)
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
		})
		b.Run(fmt.Sprintf("xrbq4-25cand-d%d", dim), func(b *testing.B) {
			for b.Loop() {
				rq.EncodePureRaBitQ4(x)
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
		})
	}
}

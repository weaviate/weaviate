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
	"github.com/tphakala/simd/f32"
)

// rq4QuantCorrCases covers the vector body, the 16-element blocking, the
// scalar tail, and empty input.
var rq4QuantCorrCases = []int{0, 1, 7, 15, 16, 17, 31, 32, 48, 63, 64, 100, 512, 1536, 1537}

func randomQuantInput(n int, rng *rand.Rand) []float32 {
	xs := make([]float32, n)
	for i := range xs {
		xs[i] = float32(rng.NormFloat64())
	}
	return xs
}

// The active implementation (SIMD on arm64/amd64) must produce bit-identical
// codes and integer sums to the pure Go reference. sumXC differs only in
// float accumulation order, so it gets a relative epsilon.
func TestRQ4QuantCorrParity(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 0))
	for _, n := range rq4QuantCorrCases {
		for trial := range 5 {
			xs := randomQuantInput(n, rng)
			invStep := float32(rng.Float64()*20 + 0.1)
			offset := float32(rng.Float64()*10 - 5)

			ciImpl := make([]int32, n)
			ciGo := make([]int32, n)
			gotXC, gotC, gotC2 := rq4QuantCorrImpl(ciImpl, xs, invStep, offset)
			wantXC, wantC, wantC2 := rq4QuantCorrGo(ciGo, xs, invStep, offset)

			require.Equal(t, ciGo, ciImpl, "codes n=%d trial=%d", n, trial)
			assert.Equal(t, wantC, gotC, "sumC n=%d trial=%d", n, trial)
			assert.Equal(t, wantC2, gotC2, "sumC2 n=%d trial=%d", n, trial)
			assert.InDelta(t, wantXC, gotXC, 1e-4*(1+math.Abs(float64(wantXC))),
				"sumXC n=%d trial=%d", n, trial)
		}
	}
}

// The fused kernel replaces f32.Float32ToInt32ScaleClamp; the codes it writes
// must stay bit-identical to the library kernel so encodings do not change.
func TestRQ4QuantCorrMatchesLibraryClamp(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 0))
	for _, n := range rq4QuantCorrCases {
		xs := randomQuantInput(n, rng)
		invStep := float32(rng.Float64()*20 + 0.1)
		offset := float32(rng.Float64()*10 - 5)

		ciFused := make([]int32, n)
		ciLib := make([]int32, n)
		rq4QuantCorrImpl(ciFused, xs, invStep, offset)
		f32.Float32ToInt32ScaleClamp(ciLib, xs, invStep, offset, 0, rq4MaxCode)
		require.Equal(t, ciLib, ciFused, "n=%d", n)
	}
}

// Saturated and non-finite inputs: +Inf clamps to 15, -Inf to 0, NaN to 0,
// and the integer sums must stay exact.
func TestRQ4QuantCorrAbnormalInputs(t *testing.T) {
	n := 64
	xs := make([]float32, n)
	for i := range xs {
		switch i % 4 {
		case 0:
			xs[i] = float32(math.Inf(1))
		case 1:
			xs[i] = float32(math.Inf(-1))
		case 2:
			xs[i] = float32(math.NaN())
		default:
			xs[i] = 1e30
		}
	}
	ciImpl := make([]int32, n)
	ciGo := make([]int32, n)
	gotXC, gotC, gotC2 := rq4QuantCorrImpl(ciImpl, xs, 1.0, 0.5)
	_, wantC, wantC2 := rq4QuantCorrGo(ciGo, xs, 1.0, 0.5)
	require.Equal(t, ciGo, ciImpl)
	assert.Equal(t, wantC, gotC)
	assert.Equal(t, wantC2, gotC2)
	for i := range xs {
		switch i % 4 {
		case 0, 3:
			assert.Equal(t, int32(15), ciImpl[i], "i=%d", i)
		default:
			assert.Equal(t, int32(0), ciImpl[i], "i=%d", i)
		}
	}
	// sumXC over Inf*15 etc. is Inf/NaN territory; just require it not to
	// panic and to be deterministic per platform.
	_ = gotXC
}

// All-in-range integers hit every code value exactly.
func TestRQ4QuantCorrExactCodes(t *testing.T) {
	xs := make([]float32, 32)
	for i := range xs {
		xs[i] = float32(i % 16)
	}
	ci := make([]int32, len(xs))
	_, sumC, sumC2 := rq4QuantCorrImpl(ci, xs, 1.0, 0.0)
	var wantC, wantC2 int32
	for i := range xs {
		c := int32(i % 16)
		assert.Equal(t, c, ci[i], "i=%d", i)
		wantC += c
		wantC2 += c * c
	}
	assert.Equal(t, wantC, sumC)
	assert.Equal(t, wantC2, sumC2)
}

// min/max must be bit-exact (order-independent); the sum gets a relative
// epsilon for accumulation order.
func TestRQ4MinMaxSumParity(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 0))
	for _, n := range rq4QuantCorrCases {
		if n == 0 {
			continue // documented contract: xs must not be empty
		}
		for trial := range 5 {
			xs := randomQuantInput(n, rng)
			gotMin, gotMax, gotSum := rq4MinMaxSumImpl(xs)
			wantMin, wantMax, wantSum := rq4MinMaxSumGo(xs)
			require.Equal(t, wantMin, gotMin, "min n=%d trial=%d", n, trial)
			require.Equal(t, wantMax, gotMax, "max n=%d trial=%d", n, trial)
			assert.InDelta(t, wantSum, gotSum, 1e-4*(1+math.Abs(float64(wantSum))),
				"sum n=%d trial=%d", n, trial)
		}
	}
	// All-equal input: min == max == the value, sum exact.
	xs := make([]float32, 64)
	for i := range xs {
		xs[i] = -2.5
	}
	minV, maxV, sum := rq4MinMaxSumImpl(xs)
	assert.Equal(t, float32(-2.5), minV)
	assert.Equal(t, float32(-2.5), maxV)
	assert.Equal(t, float32(-160), sum)
	// Extremes at positions the seeding block would miss if it were wrong.
	xs[63] = 100
	xs[17] = -100
	minV, maxV, _ = rq4MinMaxSumImpl(xs)
	assert.Equal(t, float32(-100), minV)
	assert.Equal(t, float32(100), maxV)
}

func BenchmarkRQ4MinMaxSum(b *testing.B) {
	rng := rand.New(rand.NewPCG(42, 0))
	xs := randomQuantInput(1536, rng)
	b.Run("fused-n1536", func(b *testing.B) {
		for b.Loop() {
			rq4MinMaxSumImpl(xs)
		}
	})
	b.Run("legacy-3pass-n1536", func(b *testing.B) {
		for b.Loop() {
			_ = f32.Min(xs)
			_ = f32.Max(xs)
			_ = f32.Sum(xs)
		}
	})
}

func BenchmarkRQ4QuantCorr(b *testing.B) {
	rng := rand.New(rand.NewPCG(42, 0))
	for _, n := range []int{512, 1536} {
		xs := randomQuantInput(n, rng)
		ci := make([]int32, n)
		b.Run(fmt.Sprintf("fused-n%d", n), func(b *testing.B) {
			for b.Loop() {
				rq4QuantCorrImpl(ci, xs, 3.7, 0.4)
			}
		})
		b.Run(fmt.Sprintf("go-n%d", n), func(b *testing.B) {
			for b.Loop() {
				rq4QuantCorrGo(ci, xs, 3.7, 0.4)
			}
		})
		cf := make([]float32, n)
		b.Run(fmt.Sprintf("legacy-5pass-n%d", n), func(b *testing.B) {
			for b.Loop() {
				f32.Float32ToInt32ScaleClamp(ci, xs, 3.7, 0.4, 0, rq4MaxCode)
				f32.Int32ToFloat32Scale(cf, ci, 1)
				_ = f32.Sum(cf)
				_ = f32.SumOfSquares(cf)
				_ = f32.DotProduct(xs, cf)
			}
		})
	}
}

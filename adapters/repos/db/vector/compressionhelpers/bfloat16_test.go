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
)

// TestBF16RoundTripSemantics pins the round-to-nearest-even conversion the
// centered rq1 and rq4 headers share (float32ToBFloat16, introduced by the
// rq4c centering work): bias-add RNE, NaN canonicalized to 0x7FC0.
func TestBF16RoundTripSemantics(t *testing.T) {
	cases := []struct {
		name string
		in   float32
		want uint16
	}{
		// exact values: bf16 keeps sign + 8 exponent + 7 mantissa bits
		{name: "zero", in: 0, want: 0x0000},
		{name: "one", in: 1, want: 0x3F80},
		{name: "minus-one", in: -1, want: 0xBF80},
		{name: "two", in: 2, want: 0x4000},
		// RNE tie: 0x3F808000 is exactly halfway between 0x3F80 and 0x3F81;
		// even wins → 0x3F80
		{name: "tie-rounds-to-even-down", in: math.Float32frombits(0x3F808000), want: 0x3F80},
		// 0x3F818000 is halfway between 0x3F81 and 0x3F82; even wins → 0x3F82
		{name: "tie-rounds-to-even-up", in: math.Float32frombits(0x3F818000), want: 0x3F82},
		// just above the tie rounds up
		{name: "above-tie-rounds-up", in: math.Float32frombits(0x3F808001), want: 0x3F81},
		// just below the tie rounds down
		{name: "below-tie-rounds-down", in: math.Float32frombits(0x3F807FFF), want: 0x3F80},
		// NaN canonicalized
		{name: "nan", in: float32(math.NaN()), want: 0x7FC0},
		// f32 subnormals collapse to (signed) zero-ish bf16 subnormals via
		// truncation of the low 16 bits + rounding
		{name: "f32-subnormal", in: math.Float32frombits(0x00000001), want: 0x0000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, float32ToBFloat16(tc.in), "encode")
		})
	}

	// decode is exact: bf16 bits are the top 16 of a float32
	assert.Equal(t, float32(1), bfloat16ToFloat32(0x3F80))
	assert.Equal(t, float32(-2), bfloat16ToFloat32(0xC000))
	assert.True(t, math.IsNaN(float64(bfloat16ToFloat32(0x7FC0))))
}

// TestBF16RelativeError bounds the conversion error on the value ranges the
// centered rq1 header actually stores: squaredNorm of centered normalized
// embeddings (~0.1..2.0) and ⟨μ,x⟩ (~-1..1). bf16 keeps 8 significand bits,
// so relative error is at most 2^-8 for normal values.
func TestBF16RelativeError(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	maxRel := 0.0
	for i := 0; i < 100000; i++ {
		var v float32
		switch i % 3 {
		case 0: // squaredNorm range
			v = 0.05 + 2.0*rng.Float32()
		case 1: // ⟨μ,x⟩ range, both signs
			v = 2*rng.Float32() - 1
		case 2: // small magnitudes near zero
			v = (2*rng.Float32() - 1) * 1e-3
		}
		if v == 0 {
			continue
		}
		got := bfloat16ToFloat32(float32ToBFloat16(v))
		rel := math.Abs(float64(got-v)) / math.Abs(float64(v))
		if rel > maxRel {
			maxRel = rel
		}
	}
	require.LessOrEqual(t, maxRel, 1.0/256.0, "bf16 RNE relative error must stay within 2^-8")
}

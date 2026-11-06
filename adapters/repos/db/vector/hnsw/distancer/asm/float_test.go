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

//go:build amd64 || arm64

package asm

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// floatKernelCase is one Dot or L2 entry point under test; floatKernels (per
// architecture) lists every kernel the host CPU can run.

func dotRef(x, y []float32) float64 {
	var s float64
	for i := range x {
		s += float64(x[i]) * float64(y[i])
	}
	return s
}

func l2Ref(x, y []float32) float64 {
	var s float64
	for i := range x {
		d := float64(x[i]) - float64(y[i])
		s += d * d
	}
	return s
}

// floatTestLengths covers the straight-line paths (1..8), every tail
// combination of the 32/128-element blocks and the 16/8/4-wide and scalar
// tails, and the production dimensions.
var floatTestLengths = []int{
	1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 15, 16, 17, 23, 24, 31, 32, 33, 39, 47, 48,
	63, 64, 65, 95, 96, 100, 127, 128, 129, 130, 135, 143, 159, 255, 256, 257,
	300, 383, 384, 385, 511, 512, 513, 768, 1000, 1023, 1024, 1025, 1535, 1536, 1537, 3072,
}

func randVec(rng *rand.Rand, n int) []float32 {
	v := make([]float32, n)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return v
}

func intVec(rng *rand.Rand, n int) []float32 {
	v := make([]float32, n)
	for i := range v {
		v[i] = float32(rng.IntN(65) - 32)
	}
	return v
}

func TestFloatKernelsAgainstReference(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, k := range floatKernels(t) {
		ref := dotRef
		if k.l2 {
			ref = l2Ref
		}
		for _, n := range floatTestLengths {
			t.Run(fmt.Sprintf("%s/n=%d", k.name, n), func(t *testing.T) {
				// Exact on small integers: every product and partial sum is
				// representable, so association order cannot matter.
				xi, yi := intVec(rng, n), intVec(rng, n)
				require.Equal(t, float32(ref(xi, yi)), k.fn(xi, yi), "integer-valued")

				// Relative tolerance on real data (float32 lanes vs float64).
				x, y := randVec(rng, n), randVec(rng, n)
				want, got := ref(x, y), float64(k.fn(x, y))
				tol := 1e-5 * (math.Abs(want) + float64(n))
				require.InDelta(t, want, got, tol, "random")

				// y longer than x is allowed; the extra elements are ignored.
				yl := append(append([]float32(nil), y...), 1e9, 1e9, 1e9)
				require.Equal(t, k.fn(x, y), k.fn(x, yl), "longer y")

				// Unaligned slice starts (vector loads must not assume
				// alignment).
				xb, yb := make([]float32, n+3), make([]float32, n+5)
				copy(xb[3:], x)
				copy(yb[1:], y)
				require.Equal(t, k.fn(x, y), k.fn(xb[3:], yb[1:n+1]), "misaligned")
			})
		}
	}
}

func TestFloatKernelsSpecialValues(t *testing.T) {
	inf := float32(math.Inf(1))
	for _, k := range floatKernels(t) {
		for _, n := range []int{1, 2, 8, 9, 32, 33, 128, 129} {
			x := make([]float32, n)
			y := make([]float32, n)
			for i := range x {
				x[i] = float32(i%5) - 2
				y[i] = float32(i%3) - 1
			}
			// Infinity in the last lane must propagate, whichever tail
			// handles it.
			x[n-1] = inf
			y[n-1] = 2
			got := k.fn(x, y)
			require.True(t, math.IsInf(float64(got), 1), "%s n=%d inf: got %v", k.name, n, got)

			// Equal vectors give an exact zero L2 and a non-negative dot.
			z := make([]float32, n)
			for i := range z {
				z[i] = float32(i)*0.25 - 3
			}
			if k.l2 {
				require.Equal(t, float32(0), k.fn(z, z), "%s n=%d zero distance", k.name, n)
			} else {
				require.GreaterOrEqual(t, k.fn(z, z), float32(0), "%s n=%d self dot", k.name, n)
			}
		}
	}
}

// TestFloatKernelsMaskedOverlapInfinity puts an infinity into every element
// a masked step can reload but must discard (the last 16 positions cover the
// 4-, 8- and 16-lane overlaps of every kernel), in each operand separately.
// The element is counted once by an earlier block, so the result must be +Inf;
// a kernel that masked only one operand would compute 0*Inf there and return
// NaN. Lengths cover short paths and main-loop tails of every width.
func TestFloatKernelsMaskedOverlapInfinity(t *testing.T) {
	inf := float32(math.Inf(1))
	lengths := []int{
		4, 5, 6, 7, 9, 10, 11, 13, 15, 17, 19, 21, 23, 25, 29, 31, 33, 35, 39, 47, 49, 63, 65, 67,
		95, 97, 100, 127, 129, 130, 135, 143, 159, 257, 300, 385, 513, 777, 1025, 1537,
	}
	for _, k := range floatKernels(t) {
		for _, n := range lengths {
			for p := max(0, n-16); p < n; p++ {
				for _, side := range []string{"x", "y"} {
					x := make([]float32, n)
					y := make([]float32, n)
					for i := range x {
						x[i] = 1 + float32(i%7)*0.5
						y[i] = 2 + float32(i%5)*0.25
					}
					if side == "x" {
						x[p] = inf
					} else {
						y[p] = inf
					}
					got := k.fn(x, y)
					require.True(t, math.IsInf(float64(got), 1), "%s n=%d inf in %s[%d]: got %v", k.name, n, side, p, got)
				}
			}
		}
	}
}

var benchFloatSink float32

func BenchmarkFloatKernels(b *testing.B) {
	rng := rand.New(rand.NewPCG(3, 4))
	for _, n := range []int{2, 8, 30, 64, 128, 384, 768, 1024, 1536, 3072} {
		x, y := randVec(rng, n), randVec(rng, n)
		for _, k := range floatKernels(b) {
			b.Run(fmt.Sprintf("%s/n=%d", k.name, n), func(b *testing.B) {
				for b.Loop() {
					benchFloatSink = k.fn(x, y)
				}
			})
		}
	}
}

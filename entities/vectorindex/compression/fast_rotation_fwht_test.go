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

package compression

import (
	"math"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// randomVector returns test data with a mix of magnitudes and signs, plus a
// couple of exact zeros and denormal-adjacent values, to exercise the
// floating-point paths beyond well-behaved unit-scale inputs.
func randomVector(n int, rng *rand.Rand) []float32 {
	x := make([]float32, n)
	for i := range x {
		x[i] = float32(rng.NormFloat64()) * float32(math.Pow(10, float64(rng.IntN(7)-3)))
	}
	x[0] = 0
	if n > 1 {
		x[1] = math.SmallestNonzeroFloat32
	}
	return x
}

// TestFastWalshHadamardTransformMatchesGo pins the dispatched implementation
// (NEON assembly on arm64) to the pure Go reference. The assembly applies
// butterflies in the same order with the same normalization point, so the
// results must be bit-identical, not merely close.
func TestFastWalshHadamardTransformMatchesGo(t *testing.T) {
	cases := []struct {
		name       string
		n          int
		dispatched func([]float32)
		reference  func([]float32)
	}{
		{"64", 64, FastWalshHadamardTransform64, fastWalshHadamardTransform64Go},
		{"256", 256, FastWalshHadamardTransform256, fastWalshHadamardTransform256Go},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(42, uint64(tc.n)))
			for trial := range 100 {
				x := randomVector(tc.n, rng)
				want := make([]float32, tc.n)
				copy(want, x)
				tc.reference(want)
				tc.dispatched(x)
				for i := range x {
					require.Equal(t, math.Float32bits(want[i]), math.Float32bits(x[i]),
						"trial %d: mismatch at index %d: got %v want %v", trial, i, x[i], want[i])
				}
			}
		})
	}
}

// TestFastWalshHadamardTransformLongerSlice verifies that only the first
// block is transformed and trailing elements are untouched — Rotate passes
// sub-slices of the full vector.
func TestFastWalshHadamardTransformLongerSlice(t *testing.T) {
	cases := []struct {
		name      string
		n         int
		transform func([]float32)
	}{
		{"64", 64, FastWalshHadamardTransform64},
		{"256", 256, FastWalshHadamardTransform256},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(7, uint64(tc.n)))
			x := randomVector(tc.n+32, rng)
			tail := make([]float32, 32)
			copy(tail, x[tc.n:])
			want := make([]float32, tc.n)
			copy(want, x[:tc.n])
			if tc.n == 64 {
				fastWalshHadamardTransform64Go(want)
			} else {
				fastWalshHadamardTransform256Go(want)
			}
			tc.transform(x)
			assert.Equal(t, want, x[:tc.n])
			assert.Equal(t, tail, x[tc.n:], "elements past the block must not be modified")
		})
	}
}

// TestFastWalshHadamardTransformShortSlicePanics pins the bounds-check
// contract: a too-short slice must panic (as the Go version does via
// indexing) rather than read out of bounds in assembly.
func TestFastWalshHadamardTransformShortSlicePanics(t *testing.T) {
	cases := []struct {
		name      string
		n         int
		transform func([]float32)
	}{
		{"64", 63, FastWalshHadamardTransform64},
		{"256", 255, FastWalshHadamardTransform256},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Panics(t, func() {
				tc.transform(make([]float32, tc.n))
			})
		})
	}
}

// TestFastWalshHadamardTransformSelfInverse checks the normalized transform
// is its own inverse up to floating-point error, which is what UnRotate
// relies on.
func TestFastWalshHadamardTransformSelfInverse(t *testing.T) {
	cases := []struct {
		name      string
		n         int
		transform func([]float32)
	}{
		{"64", 64, FastWalshHadamardTransform64},
		{"256", 256, FastWalshHadamardTransform256},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(11, uint64(tc.n)))
			x := make([]float32, tc.n)
			for i := range x {
				x[i] = float32(rng.NormFloat64())
			}
			orig := make([]float32, tc.n)
			copy(orig, x)
			tc.transform(x)
			tc.transform(x)
			for i := range x {
				assert.InDelta(t, orig[i], x[i], 1e-4)
			}
		})
	}
}

func BenchmarkFastWalshHadamardTransform64(b *testing.B) {
	x := randomVector(64, rand.New(rand.NewPCG(1, 2)))
	b.Run("dispatched", func(b *testing.B) {
		for b.Loop() {
			FastWalshHadamardTransform64(x)
		}
	})
	b.Run("go", func(b *testing.B) {
		for b.Loop() {
			fastWalshHadamardTransform64Go(x)
		}
	})
}

func BenchmarkFastWalshHadamardTransform256(b *testing.B) {
	x := randomVector(256, rand.New(rand.NewPCG(1, 2)))
	b.Run("dispatched", func(b *testing.B) {
		for b.Loop() {
			FastWalshHadamardTransform256(x)
		}
	})
	b.Run("go", func(b *testing.B) {
		for b.Loop() {
			fastWalshHadamardTransform256Go(x)
		}
	})
}

func BenchmarkFastRotationRotate1536(b *testing.B) {
	r := NewFastRotation(1536, 3, DefaultFastRotationSeed)
	x := randomVector(1536, rand.New(rand.NewPCG(3, 4)))
	b.ResetTimer()
	for b.Loop() {
		r.Rotate(x)
	}
}

// TestRotateMatchesLegacySwapLoop pins the derived-permutation Rotate path
// to the original in-place swap loop, which is still reachable through
// hand-constructed FastRotation values (e.g. via RQData). Every element takes
// part in exactly one swap per round, so both must be bit-identical.
func TestRotateMatchesLegacySwapLoop(t *testing.T) {
	dims := []int{5, 64, 200, 256, 320, 1536}
	for _, dim := range dims {
		fast := NewFastRotation(dim, 3, DefaultFastRotationSeed)
		require.NotNil(t, fast.derived)
		legacy := &FastRotation{
			OutputDim: fast.OutputDim,
			Rounds:    fast.Rounds,
			Swaps:     fast.Swaps,
			Signs:     fast.Signs,
		}
		rng := rand.New(rand.NewPCG(13, uint64(dim)))
		for trial := range 20 {
			x := randomVector(dim, rng)
			got := fast.Rotate(x)
			want := legacy.Rotate(x)
			require.Equal(t, len(want), len(got))
			for i := range got {
				require.Equal(t, math.Float32bits(want[i]), math.Float32bits(got[i]),
					"dim %d trial %d: mismatch at index %d: got %v want %v",
					dim, trial, i, got[i], want[i])
			}
			// UnRotate must still invert the fast path.
			back := fast.UnRotate(got)
			for i := range x {
				assert.InDelta(t, x[i], back[i], 1e-3)
			}
		}
	}
}

// TestRotateIntoDirtyBuffer verifies RotateInto fully overwrites a polluted
// output buffer and matches Rotate, for both even and odd round counts (the
// two ping-pong parities).
func TestRotateIntoDirtyBuffer(t *testing.T) {
	for _, rounds := range []int{0, 1, 2, 3, 4} {
		r := NewFastRotation(300, rounds, 77)
		rng := rand.New(rand.NewPCG(5, uint64(rounds)))
		x := randomVector(300, rng)
		want := r.Rotate(x)
		out := make([]float32, r.OutputDim+7) // longer than needed
		for i := range out {
			out[i] = float32(math.NaN())
		}
		got := r.RotateInto(x, out)
		require.Equal(t, int(r.OutputDim), len(got), "rounds %d", rounds)
		for i := range got {
			require.Equal(t, math.Float32bits(want[i]), math.Float32bits(got[i]),
				"rounds %d: mismatch at index %d", rounds, i)
		}
	}
}

// TestRotateConcurrent exercises the internal scratch pool under the race
// detector.
func TestRotateConcurrent(t *testing.T) {
	r := NewFastRotation(1536, 3, DefaultFastRotationSeed)
	rng := rand.New(rand.NewPCG(9, 9))
	x := randomVector(1536, rng)
	want := r.Rotate(x)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				got := r.Rotate(x)
				for i := range got {
					if math.Float32bits(want[i]) != math.Float32bits(got[i]) {
						panic("concurrent Rotate mismatch")
					}
				}
			}
		}()
	}
	wg.Wait()
}

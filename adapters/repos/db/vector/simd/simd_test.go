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

package simd

import (
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sameFunc reports whether two function values point at the same code.
func sameFunc(a, b any) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

// testLengths covers empty, sub-lane, every lane boundary of every tier
// (4, 8, 16, 32, 64 lanes per iteration) and the production dimensions.
var testLengths = []int{
	0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65,
	127, 128, 129, 255, 256, 511, 512, 513, 768, 1023, 1024, 1536, 1537, 3072,
}

func randFloats(rng *rand.Rand, n int) []float32 {
	out := make([]float32, n)
	for i := range out {
		out[i] = float32(rng.NormFloat64() * 10)
	}
	return out
}

// randIntFloats returns small integer-valued floats, so that every sum and
// product of a reduction is exactly representable and all tiers must agree
// bit for bit regardless of association order.
func randIntFloats(rng *rand.Rand, n int) []float32 {
	out := make([]float32, n)
	for i := range out {
		out[i] = float32(rng.IntN(129) - 64)
	}
	return out
}

func randInt8s(rng *rand.Rand, n int) []int8 {
	out := make([]int8, n)
	for i := range out {
		out[i] = int8(rng.IntN(256) - 128)
	}
	return out
}

func randInt32s(rng *rand.Rand, n int) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = int32(rng.IntN(1<<20) - 1<<19)
	}
	return out
}

func TestElementwiseKernelsMatchGo(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, ks := range kernelSets() {
		for _, n := range testLengths {
			if n == 0 {
				continue // kernels receive non-empty slices; the wrappers guard empty
			}
			t.Run(fmt.Sprintf("%s/n=%d", ks.name, n), func(t *testing.T) {
				a, b := randFloats(rng, n), randFloats(rng, n)
				s := float32(rng.NormFloat64())
				want, got := make([]float32, n), make([]float32, n)

				addGo(want, a, b)
				ks.add(got, a, b)
				require.Equal(t, want, got, "add")

				subGo(want, a, b)
				ks.sub(got, a, b)
				require.Equal(t, want, got, "sub")

				scaleGo(want, a, s)
				ks.scale(got, a, s)
				require.Equal(t, want, got, "scale")

				addScalarGo(want, a, s)
				ks.addScalar(got, a, s)
				require.Equal(t, want, got, "addScalar")

				src := randInt32s(rng, n)
				int32ToFloat32ScaleGo(want, src, s)
				ks.int32ToFloat32Scale(got, src, s)
				require.Equal(t, want, got, "int32ToFloat32Scale")
			})
		}
	}
}

func TestReductionKernelsMatchGo(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for _, ks := range kernelSets() {
		for _, n := range testLengths {
			if n == 0 {
				continue
			}
			t.Run(fmt.Sprintf("%s/n=%d", ks.name, n), func(t *testing.T) {
				// Exact agreement on integer-valued inputs.
				ia, ib := randIntFloats(rng, n), randIntFloats(rng, n)
				require.Equal(t, sumGo(ia), ks.sum(ia), "sum exact")
				require.Equal(t, dotGo(ia, ib), ks.dot(ia, ib), "dot exact")

				// Reassociation-level agreement on real-valued inputs.
				a, b := randFloats(rng, n), randFloats(rng, n)
				tol := 1e-5 * float64(n)
				assert.InDelta(t, float64(sumGo(a)), float64(ks.sum(a)), tol*10, "sum")
				assert.InDelta(t, float64(dotGo(a, b)), float64(ks.dot(a, b)), tol*100, "dot")

				// Order-independent reductions are exact.
				require.Equal(t, minGo(a), ks.minF(a), "min")
				require.Equal(t, maxGo(a), ks.maxF(a), "max")
				require.Equal(t, maxAbsGo(a), ks.maxAbs(a), "maxAbs")

				x, y := randInt8s(rng, n), randInt8s(rng, n)
				require.Equal(t, dotInt8Go(x, y), ks.dotInt8(x, y), "dotInt8")
			})
		}
	}
}

// Extremes of the min/max family: the answer sits in every lane position and
// in the overlapping tail block, with infinities and signed zeros present.
func TestMinMaxExtremePositions(t *testing.T) {
	for _, ks := range kernelSets() {
		for _, n := range []int{1, 3, 4, 5, 8, 9, 16, 17, 20, 33, 64, 67} {
			for pos := 0; pos < n; pos++ {
				a := make([]float32, n)
				for i := range a {
					a[i] = float32(i%7) - 3 // -3..3
				}
				a[pos] = -1000
				require.Equal(t, float32(-1000), ks.minF(a), "%s min n=%d pos=%d", ks.name, n, pos)
				require.Equal(t, float32(1000), ks.maxAbs(a), "%s maxAbs n=%d pos=%d", ks.name, n, pos)
				a[pos] = 1000
				require.Equal(t, float32(1000), ks.maxF(a), "%s max n=%d pos=%d", ks.name, n, pos)
				a[pos] = float32(math.Inf(-1))
				require.Equal(t, float32(math.Inf(-1)), ks.minF(a), "%s -inf n=%d pos=%d", ks.name, n, pos)
				require.Equal(t, float32(math.Inf(1)), ks.maxAbs(a), "%s maxAbs inf n=%d pos=%d", ks.name, n, pos)
			}
		}
	}
}

func TestDotInt8Wraparound(t *testing.T) {
	// 2^31 / 16384 = 131072 products of (-128)*(-128) overflow int32; the
	// contract is two's-complement wraparound on every tier.
	n := 140000
	a := make([]int8, n)
	for i := range a {
		a[i] = -128
	}
	want := dotInt8Go(a, a)
	require.Less(t, want, int32(0), "test input must overflow")
	for _, ks := range kernelSets() {
		require.Equal(t, want, ks.dotInt8(a, a), ks.name)
	}
}

func TestFloat32ToInt32ScaleClampSpecialValues(t *testing.T) {
	nan := float32(math.NaN())
	pinf := float32(math.Inf(1))
	ninf := float32(math.Inf(-1))
	cases := []struct {
		name                      string
		src                       []float32
		scale, offset, minV, maxV float32
	}{
		{"nibble grid", []float32{-3.2, -0.001, 0, 0.001, 0.49, 0.5, 0.51, 1.5, 7.9999, 14.4, 15, 15.6, 16, 1e9, -1e9, 2.5, 3.5, 4.5}, 1, 0.5, 0, 15},
		{"scaled offset", []float32{-100, -1, -0.3, 0.3, 1, 2.9, 3.1, 50, 99.99, 100, 100.01, 1000}, 3.7, 0.4, 0, 15},
		{"non-finite", []float32{nan, pinf, ninf, nan, 1, pinf, ninf, 2, nan, 3, 4, 5, 6, 7, 8, 9, nan, pinf, ninf, 0}, 2, 1, -7, 7},
		{"nan scale", []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, nan, 0, 0, 15},
		{"negative range", []float32{-1e6, -200, -128.5, -128, -127.9, -1, -0.5, 0, 0.5, 1, 126.9, 127, 127.5, 128, 1e6, 3, 4}, 1, 0, -128, 127},
		{"truncation toward zero", []float32{-2.9, -2.5, -2.1, -1.9, -1.5, -1.1, -0.9, -0.5, -0.1, 0.1, 0.5, 0.9, 1.1, 1.5, 1.9, 2.1, 2.5, 2.9}, 1, 0, -10, 10},
		{"wide range", []float32{-3e9, -2147483648, -2147483520, -1e9, 0, 1e9, 2147483520, 2147483648, 3e9, 1, 2, 3, 4, 5, 6, 7, 8}, 1, 0, -2147483648, 2147483520},
	}
	for _, ks := range kernelSets() {
		for _, c := range cases {
			// Run at a length that exercises the vector body and the tail
			// for every lane width, plus the sub-lane lengths.
			for _, n := range []int{1, 3, 4, 7, 8, 15, 16, len(c.src)} {
				if n > len(c.src) {
					continue
				}
				want, got := make([]int32, n), make([]int32, n)
				float32ToInt32ScaleClampGo(want, c.src[:n], c.scale, c.offset, c.minV, c.maxV)
				ks.float32ToInt32ScaleClamp(got, c.src[:n], c.scale, c.offset, c.minV, c.maxV)
				require.Equal(t, want, got, "%s/%s n=%d", ks.name, c.name, n)
			}
		}
	}
}

// The quantizer path depends on exactly two roundings (product to float32,
// then the offset add); a fused multiply-add would move values across the
// truncation boundary. Random inputs dense around integer boundaries catch
// any tier that fuses.
func TestFloat32ToInt32ScaleClampRandomBitExact(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	for _, ks := range kernelSets() {
		for trial := 0; trial < 200; trial++ {
			n := testLengths[rng.IntN(len(testLengths))]
			if n == 0 {
				continue
			}
			src := make([]float32, n)
			for i := range src {
				// Values near k + 0.5 boundaries after scaling.
				src[i] = float32(rng.IntN(40)-10) + float32(rng.NormFloat64()*1e-3)
			}
			scale := float32(rng.Float64()*20 + 0.1)
			offset := float32(rng.Float64()*10 - 5)
			want, got := make([]int32, n), make([]int32, n)
			float32ToInt32ScaleClampGo(want, src, scale, offset, 0, 15)
			ks.float32ToInt32ScaleClamp(got, src, scale, offset, 0, 15)
			require.Equal(t, want, got, "%s trial=%d n=%d", ks.name, trial, n)
		}
	}
}

// In-place forms used by the quantizers: dst aliases a or b exactly.
func TestElementwiseAliasing(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 8))
	for _, ks := range kernelSets() {
		for _, n := range []int{1, 5, 16, 33, 64, 1536} {
			a, b := randFloats(rng, n), randFloats(rng, n)
			s := float32(1.75)

			want := make([]float32, n)
			addGo(want, a, b)
			got := append([]float32(nil), a...)
			ks.add(got, got, b)
			require.Equal(t, want, got, "%s add dst==a n=%d", ks.name, n)
			got = append([]float32(nil), b...)
			ks.add(got, a, got)
			require.Equal(t, want, got, "%s add dst==b n=%d", ks.name, n)

			subGo(want, a, b)
			got = append([]float32(nil), a...)
			ks.sub(got, got, b)
			require.Equal(t, want, got, "%s sub dst==a n=%d", ks.name, n)
			got = append([]float32(nil), b...)
			ks.sub(got, a, got)
			require.Equal(t, want, got, "%s sub dst==b n=%d", ks.name, n)

			scaleGo(want, a, s)
			got = append([]float32(nil), a...)
			ks.scale(got, got, s)
			require.Equal(t, want, got, "%s scale in place n=%d", ks.name, n)

			addScalarGo(want, a, s)
			got = append([]float32(nil), a...)
			ks.addScalar(got, got, s)
			require.Equal(t, want, got, "%s addScalar in place n=%d", ks.name, n)
		}
	}
}

// The exported wrappers clamp to the shortest slice, never touch elements
// beyond it, and define the empty-input results.
func TestExportedLengthContract(t *testing.T) {
	a := []float32{1, 2, 3, 4, 5}
	b := []float32{10, 20, 30}
	dst := []float32{-1, -1, -1, -1, -1, -1}

	Add(dst, a, b)
	require.Equal(t, []float32{11, 22, 33, -1, -1, -1}, dst)
	Sub(dst, a, b)
	require.Equal(t, []float32{-9, -18, -27, -1, -1, -1}, dst)
	Scale(dst[:2], a, 2)
	require.Equal(t, []float32{2, 4, -27, -1, -1, -1}, dst)
	AddScalar(dst[:4], a[:3], 1)
	require.Equal(t, []float32{2, 3, 4, -1, -1, -1}, dst)

	require.Equal(t, float32(15), Sum(a))
	require.Equal(t, float32(0), Sum(nil))
	require.Equal(t, float32(140), DotProduct(a, b))
	require.Equal(t, float32(0), DotProduct(a, nil))
	require.Equal(t, float32(55), SumOfSquares(a))
	require.Equal(t, float32(0), SumOfSquares(nil))
	require.Equal(t, float32(1), Min(a))
	require.Equal(t, float32(math.Inf(1)), Min(nil))
	require.Equal(t, float32(5), Max(a))
	require.Equal(t, float32(math.Inf(-1)), Max(nil))
	require.Equal(t, float32(30), MaxAbs([]float32{-30, 2}))
	require.Equal(t, float32(0), MaxAbs(nil))

	fdst := []float32{-1, -1, -1, -1}
	Int32ToFloat32Scale(fdst, []int32{1, 2, 3}, 0.5)
	require.Equal(t, []float32{0.5, 1, 1.5, -1}, fdst)
	Int32ToFloat32Scale(fdst, nil, 0.5)
	require.Equal(t, []float32{0.5, 1, 1.5, -1}, fdst)

	idst := []int32{-1, -1, -1, -1}
	Float32ToInt32ScaleClamp(idst, []float32{1.4, 99, -3}, 1, 0, 0, 15)
	require.Equal(t, []int32{1, 15, 0, -1}, idst)
	Float32ToInt32ScaleClamp(idst, nil, 1, 0, 0, 15)
	require.Equal(t, []int32{1, 15, 0, -1}, idst)

	require.Equal(t, int32(-2*3+4*5), DotProductInt8([]int8{-2, 4, 100}, []int8{3, 5}))
	require.Equal(t, int32(0), DotProductInt8(nil, []int8{1}))
}

// The exported functions dispatch through the kernel variables, so a sanity
// pass over the public API on production-sized inputs covers the wiring.
func TestExportedMatchGoReference(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 10))
	for _, n := range []int{64, 512, 1536, 1537} {
		a, b := randFloats(rng, n), randFloats(rng, n)
		want, got := make([]float32, n), make([]float32, n)
		addGo(want, a, b)
		Add(got, a, b)
		require.Equal(t, want, got)
		assert.InDelta(t, float64(dotGo(a, b)), float64(DotProduct(a, b)), 1e-2)
		assert.InDelta(t, float64(sumGo(a)), float64(Sum(a)), 1e-2)
		require.Equal(t, maxAbsGo(a), MaxAbs(a))
		x, y := randInt8s(rng, n), randInt8s(rng, n)
		require.Equal(t, dotInt8Go(x, y), DotProductInt8(x, y))
	}
}

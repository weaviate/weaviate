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

import "math"

// Add stores a[i] + b[i] into dst[i]. dst may alias a or b exactly.
func Add(dst, a, b []float32) {
	n := min(len(dst), len(a), len(b))
	if n == 0 {
		return
	}
	addImpl(dst[:n], a[:n], b[:n])
}

// Sub stores a[i] - b[i] into dst[i]. dst may alias a or b exactly.
func Sub(dst, a, b []float32) {
	n := min(len(dst), len(a), len(b))
	if n == 0 {
		return
	}
	subImpl(dst[:n], a[:n], b[:n])
}

// Scale stores a[i] * s into dst[i]. dst may alias a exactly.
func Scale(dst, a []float32, s float32) {
	n := min(len(dst), len(a))
	if n == 0 {
		return
	}
	scaleImpl(dst[:n], a[:n], s)
}

// AddScalar stores a[i] + s into dst[i]. dst may alias a exactly.
func AddScalar(dst, a []float32, s float32) {
	n := min(len(dst), len(a))
	if n == 0 {
		return
	}
	addScalarImpl(dst[:n], a[:n], s)
}

// Sum returns the sum of all elements, 0 for an empty slice.
func Sum(a []float32) float32 {
	if len(a) == 0 {
		return 0
	}
	return sumImpl(a)
}

// DotProduct returns sum(a[i] * b[i]) over min(len(a), len(b)) elements, 0
// when either slice is empty.
func DotProduct(a, b []float32) float32 {
	n := min(len(a), len(b))
	if n == 0 {
		return 0
	}
	if n <= SmallMax {
		if n == 2 { // geo coordinates: keep the hottest small case inline
			return a[0]*b[0] + a[1]*b[1]
		}
		return dotSmall(a[:n], b[:n])
	}
	return dotImpl(a[:n], b[:n])
}

// L2Squared returns sum((a[i] - b[i])^2) over min(len(a), len(b)) elements, 0
// when either slice is empty.
func L2Squared(a, b []float32) float32 {
	n := min(len(a), len(b))
	if n == 0 {
		return 0
	}
	if n <= SmallMax {
		if n == 2 { // geo coordinates: keep the hottest small case inline
			d0, d1 := a[0]-b[0], a[1]-b[1]
			return d0*d0 + d1*d1
		}
		return l2Small(a[:n], b[:n])
	}
	return l2Impl(a[:n], b[:n])
}

// SumOfSquares returns sum(a[i]^2), 0 for an empty slice. It shares the dot
// product kernel.
func SumOfSquares(a []float32) float32 {
	if len(a) == 0 {
		return 0
	}
	return dotImpl(a, a)
}

// Min returns the smallest element, +Inf for an empty slice.
func Min(a []float32) float32 {
	if len(a) == 0 {
		return float32(math.Inf(1))
	}
	return minImpl(a)
}

// Max returns the largest element, -Inf for an empty slice.
func Max(a []float32) float32 {
	if len(a) == 0 {
		return float32(math.Inf(-1))
	}
	return maxImpl(a)
}

// MaxAbs returns max |a[i]| (the infinity norm), 0 for an empty slice.
func MaxAbs(a []float32) float32 {
	if len(a) == 0 {
		return 0
	}
	return maxAbsImpl(a)
}

// Int32ToFloat32Scale stores float32(src[i]) * scale into dst[i].
func Int32ToFloat32Scale(dst []float32, src []int32, scale float32) {
	n := min(len(dst), len(src))
	if n == 0 {
		return
	}
	int32ToFloat32ScaleImpl(dst[:n], src[:n], scale)
}

// Float32ToInt32ScaleClamp quantizes src into dst as
//
//	dst[i] = int32(clamp(float32(src[i]*scale) + offset, minV, maxV))
//
// with the product rounded to float32 before the offset is added (two
// roundings, never a fused multiply-add), the clamp applied as max(minV) then
// min(maxV), and the final conversion truncating toward zero. A NaN input
// yields 0; +Inf clamps to maxV and -Inf to minV. minV and maxV must be
// finite, ordered, and within the float32-representable int32 range
// [-2147483648, 2147483520]; outside that contract the conversion of an
// out-of-range value is architecture dependent.
func Float32ToInt32ScaleClamp(dst []int32, src []float32, scale, offset, minV, maxV float32) {
	n := min(len(dst), len(src))
	if n == 0 {
		return
	}
	float32ToInt32ScaleClampImpl(dst[:n], src[:n], scale, offset, minV, maxV)
}

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

// Portable Go kernels. They are the correctness reference for the archsimd
// kernels and the production path when GOEXPERIMENT=simd is off. Inputs are
// pre-sliced to a common non-zero length; the re-slicing below lets the
// compiler drop the bounds checks inside the loops.

func addGo(dst, a, b []float32) {
	a, b = a[:len(dst)], b[:len(dst)]
	for i := range dst {
		dst[i] = a[i] + b[i]
	}
}

func subGo(dst, a, b []float32) {
	a, b = a[:len(dst)], b[:len(dst)]
	for i := range dst {
		dst[i] = a[i] - b[i]
	}
}

func scaleGo(dst, a []float32, s float32) {
	a = a[:len(dst)]
	for i := range dst {
		dst[i] = a[i] * s
	}
}

func addScalarGo(dst, a []float32, s float32) {
	a = a[:len(dst)]
	for i := range dst {
		dst[i] = a[i] + s
	}
}

// sumGo and dotGo keep four independent accumulators so the loop runs at
// load throughput rather than at floating-point add latency.
func sumGo(a []float32) float32 {
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		s0 += a[i]
		s1 += a[i+1]
		s2 += a[i+2]
		s3 += a[i+3]
	}
	s := (s0 + s1) + (s2 + s3)
	for ; i < len(a); i++ {
		s += a[i]
	}
	return s
}

func dotGo(a, b []float32) float32 {
	b = b[:len(a)]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		s0 += a[i] * b[i]
		s1 += a[i+1] * b[i+1]
		s2 += a[i+2] * b[i+2]
		s3 += a[i+3] * b[i+3]
	}
	s := (s0 + s1) + (s2 + s3)
	for ; i < len(a); i++ {
		s += a[i] * b[i]
	}
	return s
}

func l2Go(a, b []float32) float32 {
	b = b[:len(a)]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		d0, d1, d2, d3 := a[i]-b[i], a[i+1]-b[i+1], a[i+2]-b[i+2], a[i+3]-b[i+3]
		s0 += d0 * d0
		s1 += d1 * d1
		s2 += d2 * d2
		s3 += d3 * d3
	}
	s := (s0 + s1) + (s2 + s3)
	for ; i < len(a); i++ {
		d := a[i] - b[i]
		s += d * d
	}
	return s
}

func minGo(a []float32) float32 {
	m := a[0]
	for _, v := range a[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func maxGo(a []float32) float32 {
	m := a[0]
	for _, v := range a[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func maxAbsGo(a []float32) float32 {
	var m float32
	for _, v := range a {
		if v < 0 {
			v = -v
		}
		if v > m {
			m = v
		}
	}
	return m
}

func int32ToFloat32ScaleGo(dst []float32, src []int32, scale float32) {
	src = src[:len(dst)]
	for i := range dst {
		dst[i] = float32(src[i]) * scale
	}
}

func float32ToInt32ScaleClampGo(dst []int32, src []float32, scale, offset, minV, maxV float32) {
	src = src[:len(dst)]
	for i := range dst {
		// The explicit conversion forces the product to round to float32
		// before the add, which also stops the compiler fusing the two into
		// an FMA; the SIMD kernels issue a separate multiply and add.
		v := float32(src[i]*scale) + offset
		if v != v { // NaN
			dst[i] = 0
			continue
		}
		if v < minV {
			v = minV
		}
		if v > maxV {
			v = maxV
		}
		dst[i] = int32(v) // truncates toward zero
	}
}

func dotInt8Go(a, b []int8) int32 {
	b = b[:len(a)]
	var s int32
	for i := range a {
		s += int32(a[i]) * int32(b[i])
	}
	return s
}

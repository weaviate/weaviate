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

// rq4QuantCorrImpl is the fused quantize+correlate pass of the 4-bit encoder:
// one sweep over xs quantizes every entry to ci and accumulates the three
// reductions the interval search needs. It replaces five separate SIMD sweeps
// (Float32ToInt32ScaleClamp, Int32ToFloat32Scale, Sum, SumOfSquares,
// DotProduct); see rq4Correlation for how the sums combine into the
// correlation score.
//
// Semantics per element (identical to the previous library-composed path):
//
//	t     = float32(xs[i]*invStep) + offset  // two roundings, never FMA
//	c     = int32(clamp(t, 0, 15))           // truncation; NaN -> 0
//	ci[i] = c
//
// sumC and sumC2 are exact int32 sums of c and c*c. sumXC sums
// xs[i]*float32(c) in float32; its accumulation order is
// architecture-specific (16 SIMD lanes on arm64/amd64, sequential in the
// fallback), so it may differ from the scalar reference in the last ulps.
// Overridden with SIMD kernels in distance_arm64.go / distance_amd64.go.
var rq4QuantCorrImpl func(ci []int32, xs []float32, invStep, offset float32) (sumXC float32, sumC, sumC2 int32) = rq4QuantCorrGo

// rq4MinMaxSumImpl computes min, max and sum of xs in one sweep, replacing
// the separate f32.Min, f32.Max and f32.Sum passes of the interval search.
// min/max are exact (order-independent); the sum's accumulation order is
// architecture-specific like sumXC above. Overridden with SIMD kernels in
// distance_arm64.go / distance_amd64.go. xs must not be empty.
var rq4MinMaxSumImpl func(xs []float32) (minV, maxV, sum float32) = rq4MinMaxSumGo

// rq4MinMaxSumGo is the pure Go reference (and non-SIMD fallback).
func rq4MinMaxSumGo(xs []float32) (minV, maxV, sum float32) {
	minV, maxV = xs[0], xs[0]
	for _, x := range xs {
		if x < minV {
			minV = x
		}
		if x > maxV {
			maxV = x
		}
		sum += x
	}
	return minV, maxV, sum
}

// rq4QuantCorrGo is the pure Go reference (and non-SIMD fallback). The
// float32(xs[i]*invStep) conversion is a mandatory rounding barrier: without
// it the compiler may fuse the multiply-add into a single FMA on arm64 and
// diverge from the SIMD kernels.
func rq4QuantCorrGo(ci []int32, xs []float32, invStep, offset float32) (sumXC float32, sumC, sumC2 int32) {
	for i, x := range xs {
		t := float32(x*invStep) + offset
		if !(t > 0) { // also catches NaN, matching FCVTZS(NaN) = 0
			t = 0
		}
		if t > rq4MaxCode {
			t = rq4MaxCode
		}
		c := int32(t)
		ci[i] = c
		sumC += c
		sumC2 += c * c
		sumXC += x * float32(c)
	}
	return sumXC, sumC, sumC2
}

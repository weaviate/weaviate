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

//go:build goexperiment.simd

package simd

import (
	"simd/archsimd"
	"testing"
)

var avx2Kernels = kernelSet{
	name: "avx2", add: addAVX2, sub: subAVX2, scale: scaleAVX2, addScalar: addScalarAVX2,
	sum: sumAVX2, minF: minAVX2, maxF: maxAVX2, maxAbs: maxAbsAVX2, dot: dotAVX2, l2: l2AVX2,
	int32ToFloat32Scale: int32ToFloat32ScaleAVX2, float32ToInt32ScaleClamp: float32ToInt32ScaleClampAVX2,
	dotInt8: dotInt8AVX2,
}

var avx512Kernels = kernelSet{
	name: "avx512", add: addAVX512, sub: subAVX512, scale: scaleAVX512, addScalar: addScalarAVX512,
	sum: sumAVX512, minF: minAVX512, maxF: maxAVX512, maxAbs: maxAbsAVX512, dot: dotAVX512, l2: l2AVX512,
	int32ToFloat32Scale: int32ToFloat32ScaleAVX512, float32ToInt32ScaleClamp: float32ToInt32ScaleClampAVX512,
	dotInt8: dotInt8AVX512,
}

func kernelSets() []kernelSet {
	sets := []kernelSet{goKernels}
	if archsimd.X86.AVX2() && archsimd.X86.FMA() {
		sets = append(sets, avx2Kernels)
	}
	if archsimd.X86.AVX512() {
		// Benchmarked and tested wherever the CPU can run it, even if the
		// dispatch would not select it.
		sets = append(sets, avx512Kernels)
	}
	return sets
}

// TestSIMDActive guards against a silent fallback to the Go kernels in a
// GOEXPERIMENT=simd build on a CPU that supports the SIMD tiers.
func TestSIMDActive(t *testing.T) {
	switch {
	case useAVX512():
		if !sameFunc(dotImpl, dotAVX512) {
			t.Fatal("dotImpl is not the AVX-512 kernel")
		}
	case archsimd.X86.AVX2() && archsimd.X86.FMA():
		if !sameFunc(dotImpl, dotAVX2) {
			t.Fatal("dotImpl is not the AVX2 kernel")
		}
	default:
		t.Skip("CPU has neither AVX2+FMA nor AVX-512")
	}
}

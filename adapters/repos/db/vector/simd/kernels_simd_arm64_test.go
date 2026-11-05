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

import "testing"

var neonKernels = kernelSet{
	name: "neon", add: addNEON, sub: subNEON, scale: scaleNEON, addScalar: addScalarNEON,
	sum: sumNEON, minF: minNEON, maxF: maxNEON, maxAbs: maxAbsNEON, dot: dotNEON, l2: l2NEON,
	int32ToFloat32Scale: int32ToFloat32ScaleNEON, float32ToInt32ScaleClamp: float32ToInt32ScaleClampNEON,
	dotInt8: dotInt8NEON,
}

func kernelSets() []kernelSet { return []kernelSet{goKernels, neonKernels} }

// TestSIMDActive guards against a silent fallback to the Go kernels in a
// GOEXPERIMENT=simd build.
func TestSIMDActive(t *testing.T) {
	if !sameFunc(dotImpl, dotNEON) {
		t.Fatal("dotImpl is not the NEON kernel")
	}
}

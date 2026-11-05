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

// kernelSet names one implementation tier so every test and benchmark runs
// against each tier the build and CPU provide. goKernels is always present;
// the goexperiment.simd test files add the archsimd tiers.
type kernelSet struct {
	name                     string
	add, sub                 func(dst, a, b []float32)
	scale, addScalar         func(dst, a []float32, s float32)
	sum, minF, maxF, maxAbs  func(a []float32) float32
	dot, l2                  func(a, b []float32) float32
	int32ToFloat32Scale      func(dst []float32, src []int32, scale float32)
	float32ToInt32ScaleClamp func(dst []int32, src []float32, scale, offset, minV, maxV float32)
	dotInt8                  func(a, b []int8) int32
}

var goKernels = kernelSet{
	name: "go", add: addGo, sub: subGo, scale: scaleGo, addScalar: addScalarGo,
	sum: sumGo, minF: minGo, maxF: maxGo, maxAbs: maxAbsGo, dot: dotGo, l2: l2Go,
	int32ToFloat32Scale: int32ToFloat32ScaleGo, float32ToInt32ScaleClamp: float32ToInt32ScaleClampGo,
	dotInt8: dotInt8Go,
}

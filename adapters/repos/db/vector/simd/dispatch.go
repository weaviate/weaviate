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

// The kernel variables default to the portable Go implementations. The
// GOEXPERIMENT=simd builds (kernels_simd_*.go) replace them at init with the
// archsimd kernels matching the running CPU. All kernels receive slices that
// are already clamped to a common non-zero length by the exported wrappers.
var (
	addImpl                      = addGo
	subImpl                      = subGo
	scaleImpl                    = scaleGo
	addScalarImpl                = addScalarGo
	sumImpl                      = sumGo
	dotImpl                      = dotGo
	l2Impl                       = l2Go
	minImpl                      = minGo
	maxImpl                      = maxGo
	maxAbsImpl                   = maxAbsGo
	int32ToFloat32ScaleImpl      = int32ToFloat32ScaleGo
	float32ToInt32ScaleClampImpl = float32ToInt32ScaleClampGo
	dotInt8Impl                  = dotInt8Go
)

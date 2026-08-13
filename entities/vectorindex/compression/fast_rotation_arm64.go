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

//go:build arm64

package compression

// The NEON kernels operate on a raw pointer and require the full block to be
// addressable. ASIMD is a mandatory part of ARMv8-A, so no runtime feature
// detection is needed. Results are bit-identical to the pure Go
// implementation (same butterfly ordering, same normalization point).

//go:noescape
func fwht64NEON(x *float32)

//go:noescape
func fwht256NEON(x *float32)

// FastWalshHadamardTransform64 performs a normalized fast Walsh-Hadamard
// transform on the first 64 elements of x.
func FastWalshHadamardTransform64(x []float32) {
	x = x[:64:64] // bounds check, hoisted out of the assembly
	fwht64NEON(&x[0])
}

// FastWalshHadamardTransform256 performs a normalized fast Walsh-Hadamard
// transform on the first 256 elements of x.
func FastWalshHadamardTransform256(x []float32) {
	x = x[:256:256] // bounds check, hoisted out of the assembly
	fwht256NEON(&x[0])
}

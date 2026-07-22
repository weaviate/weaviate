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

//go:build amd64

package compression

import "golang.org/x/sys/cpu"

// The AVX kernels operate on a raw pointer and require the full block to be
// addressable. Unlike ASIMD on arm64, AVX is not part of the amd64 baseline,
// so CPUs without it (or hypervisors masking it) fall back to the pure Go
// implementation. Results are bit-identical either way (same butterfly
// ordering, same normalization point).
var fwhtHasAVX = cpu.X86.HasAVX

//go:noescape
func fwht64AVX(x *float32)

//go:noescape
func fwht256AVX(x *float32)

// FastWalshHadamardTransform64 performs a normalized fast Walsh-Hadamard
// transform on the first 64 elements of x.
func FastWalshHadamardTransform64(x []float32) {
	x = x[:64:64] // bounds check, hoisted out of the assembly
	if fwhtHasAVX {
		fwht64AVX(&x[0])
		return
	}
	fastWalshHadamardTransform64Go(x)
}

// FastWalshHadamardTransform256 performs a normalized fast Walsh-Hadamard
// transform on the first 256 elements of x.
func FastWalshHadamardTransform256(x []float32) {
	x = x[:256:256] // bounds check, hoisted out of the assembly
	if fwhtHasAVX {
		fwht256AVX(&x[0])
		return
	}
	fastWalshHadamardTransform256Go(x)
}

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

package distancer

import (
	"golang.org/x/sys/cpu"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/simd"
)

func init() {
	// The hand-written AVX2 and AVX-512 kernels still measure 2-14% faster than
	// archsimd from n=64 up on both Sapphire Rapids and Genoa (the Go compiler
	// does not fold the FMA memory operand), so they stay for long vectors.
	// archsimd's straight-line small-vector paths win by 1.2-1.4x up to n=8
	// (geo coordinates are n=2), so those lengths go to the simd package.
	// AMX-BF16 stands in for "Sapphire Rapids or newer", where 512-bit code
	// carries no frequency penalty.
	switch {
	case cpu.X86.HasAMXBF16 && cpu.X86.HasAVX512:
		dotProductImplementation = dotAVX512
	case cpu.X86.HasAVX2:
		dotProductImplementation = dotAVX256
	}
}

func dotAVX512(a, b []float32) float32 {
	if len(a) <= simd.SmallMax {
		return simd.DotProduct(a, b)
	}
	return asm.DotAVX512(a, b)
}

func dotAVX256(a, b []float32) float32 {
	if len(a) <= simd.SmallMax {
		return simd.DotProduct(a, b)
	}
	return asm.DotAVX256(a, b)
}

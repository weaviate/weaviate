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

import (
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
	"golang.org/x/sys/cpu"
)

func asmDotKernels() []asmKernel {
	ks := []asmKernel{{"asm-avx2", asm.DotAVX256}}
	if cpu.X86.HasAVX512 {
		ks = append(ks, asmKernel{"asm-avx512", asm.DotAVX512})
	}
	return ks
}

func asmL2Kernels() []asmKernel {
	ks := []asmKernel{{"asm-avx2", asm.L2AVX256}}
	if cpu.X86.HasAVX512 {
		ks = append(ks, asmKernel{"asm-avx512", asm.L2AVX512})
	}
	return ks
}

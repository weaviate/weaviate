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

package asm

import (
	"testing"

	"golang.org/x/sys/cpu"
)

type floatKernelCase struct {
	name string
	fn   func(x, y []float32) float32
	l2   bool
}

func floatKernels(tb testing.TB) []floatKernelCase {
	if !cpu.X86.HasAVX2 || !cpu.X86.HasFMA {
		tb.Skip("AVX2+FMA required")
	}
	ks := []floatKernelCase{
		{"DotAVX256", DotAVX256, false},
		{"L2AVX256", L2AVX256, true},
	}
	if cpu.X86.HasAVX512 {
		ks = append(ks, floatKernelCase{"DotAVX512", DotAVX512, false}, floatKernelCase{"L2AVX512", L2AVX512, true})
	}
	return ks
}

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
	"fmt"
	"math/rand/v2"
	"testing"
)

// BenchmarkDotVsAsm and BenchmarkL2VsAsm put the archsimd dot product and
// squared L2 beside the remaining hand-written kernels in hnsw/distancer/asm,
// which are the bar for any further migration into this package.
// asmDotKernels and asmL2Kernels are per-architecture.
func BenchmarkDotVsAsm(b *testing.B) {
	benchVsAsm(b, DotProduct, asmDotKernels())
}

func BenchmarkL2VsAsm(b *testing.B) {
	benchVsAsm(b, L2Squared, asmL2Kernels())
}

func benchVsAsm(b *testing.B, ours func(x, y []float32) float32, refs []asmKernel) {
	rng := rand.New(rand.NewPCG(21, 22))
	for _, n := range []int{2, 8, 64, 128, 384, 768, 1536, 3072} {
		x, y := randFloats(rng, n), randFloats(rng, n)
		b.Run(fmt.Sprintf("simd/n=%d", n), func(b *testing.B) {
			for b.Loop() {
				benchSink = ours(x, y)
			}
		})
		for _, k := range refs {
			b.Run(fmt.Sprintf("%s/n=%d", k.name, n), func(b *testing.B) {
				for b.Loop() {
					benchSink = k.fn(x, y)
				}
			})
		}
	}
}

type asmKernel struct {
	name string
	fn   func(x, y []float32) float32
}

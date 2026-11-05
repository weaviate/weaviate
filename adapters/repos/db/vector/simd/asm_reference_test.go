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

// BenchmarkDotVsAsm puts the archsimd dot product beside the hand-written
// kernels in hnsw/distancer/asm, which are the bar any migration of those
// kernels into this package has to clear. asmDotKernels is per-architecture.
func BenchmarkDotVsAsm(b *testing.B) {
	rng := rand.New(rand.NewPCG(21, 22))
	for _, n := range []int{8, 64, 128, 384, 768, 1536, 3072} {
		x, y := randFloats(rng, n), randFloats(rng, n)
		b.Run(fmt.Sprintf("simd/n=%d", n), func(b *testing.B) {
			for b.Loop() {
				benchSink = DotProduct(x, y)
			}
		})
		for _, k := range asmDotKernels() {
			b.Run(fmt.Sprintf("%s/n=%d", k.name, n), func(b *testing.B) {
				for b.Loop() {
					benchSink = k.fn(x, y)
				}
			})
		}
	}
}

type asmDotKernel struct {
	name string
	fn   func(x, y []float32) float32
}

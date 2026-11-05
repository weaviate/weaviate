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

var benchSink float32

func BenchmarkKernels(b *testing.B) {
	rng := rand.New(rand.NewPCG(11, 12))
	for _, n := range []int{64, 512, 1536} {
		x, y := randFloats(rng, n), randFloats(rng, n)
		dst := make([]float32, n)
		ci := make([]int32, n)
		xi, yi := randInt8s(rng, n), randInt8s(rng, n)
		for _, ks := range kernelSets() {
			pfx := fmt.Sprintf("%s/n=%d/", ks.name, n)
			b.Run(pfx+"Dot", func(b *testing.B) {
				for b.Loop() {
					benchSink = ks.dot(x, y)
				}
			})
			b.Run(pfx+"Sum", func(b *testing.B) {
				for b.Loop() {
					benchSink = ks.sum(x)
				}
			})
			b.Run(pfx+"MaxAbs", func(b *testing.B) {
				for b.Loop() {
					benchSink = ks.maxAbs(x)
				}
			})
			b.Run(pfx+"Add", func(b *testing.B) {
				for b.Loop() {
					ks.add(dst, x, y)
				}
			})
			b.Run(pfx+"Scale", func(b *testing.B) {
				for b.Loop() {
					ks.scale(dst, x, 1.01)
				}
			})
			b.Run(pfx+"Float32ToInt32ScaleClamp", func(b *testing.B) {
				for b.Loop() {
					ks.float32ToInt32ScaleClamp(ci, x, 3.7, 0.4, 0, 15)
				}
			})
			b.Run(pfx+"Int32ToFloat32Scale", func(b *testing.B) {
				for b.Loop() {
					ks.int32ToFloat32Scale(dst, ci, 0.25)
				}
			})
			b.Run(pfx+"DotInt8", func(b *testing.B) {
				for b.Loop() {
					benchSink = float32(ks.dotInt8(xi, yi))
				}
			})
		}
	}
}

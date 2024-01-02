//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package asm

import (
	"fmt"
	"math/rand"
	"testing"
)

func dotLoop(x, y []float32) float32 {
	var sum float32
	for i := range x {
		sum += x[i] * y[i]
	}

	return sum
}

func BenchmarkDotInlineVsLoop(b *testing.B) {
	lengths := []int{2, 4, 6, 8, 10, 12}
	for _, length := range lengths {
		x := make([]float32, length)
		y := make([]float32, length)

		for i := range x {
			x[i] = rand.Float32()
			y[i] = rand.Float32()
		}

		b.Run(fmt.Sprintf("vector dim=%d", length), func(b *testing.B) {
			b.Run("loop", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					dotLoop(x, y)
				}
			})

			b.Run("flat", func(b *testing.B) {
				// written to ensure that the compiler
				// inlines the function when possible
				switch length {
				case 2:
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						dot2(x, y)
					}
				case 4:
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						dot4(x, y)
					}
				case 6:
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						dot6(x, y)
					}
				case 8:
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						dot8(x, y)
					}
				case 10:
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						dot10(x, y)
					}
				case 12:
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						dot12(x, y)
					}
				default:
					panic("unsupported length")
				}
			})
		})
	}
}

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

package distancer

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

func L2PureGo(a, b []float32) float32 {
	var sum float32

	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}

	return sum
}

func Test_L2_DistanceImplementation(t *testing.T) {
	lengths := []int{1, 4, 16, 31, 32, 35, 64, 67, 128, 130, 256, 260, 384, 390, 768, 777}

	for _, length := range lengths {
		t.Run(fmt.Sprintf("with vector l=%d", length), func(t *testing.T) {
			x := make([]float32, length)
			y := make([]float32, length)
			for i := range x {
				x[i] = rand.Float32()
				y[i] = rand.Float32()
			}

			control := L2PureGo(x, y)
			asmResult := asm.L2(x, y)

			assert.InEpsilon(t, control, asmResult, 0.01)
		})
	}
}

func Test_L2_DistanceImplementation_OneNegativeValue(t *testing.T) {
	lengths := []int{1, 4, 16, 31, 32, 35, 64, 67, 128, 130, 256, 260, 384, 390, 768, 777}

	for _, length := range lengths {
		t.Run(fmt.Sprintf("with vector l=%d", length), func(t *testing.T) {
			x := make([]float32, length)
			y := make([]float32, length)
			for i := range x {
				x[i] = -rand.Float32()
				y[i] = rand.Float32()
			}

			control := L2PureGo(x, y)
			asmResult := asm.L2(x, y)

			assert.InEpsilon(t, control, asmResult, 0.01)
		})
	}
}

func Benchmark_L2_PureGo_VS_AVX(b *testing.B) {
	r := getRandomSeed()
	lengths := []int{30, 32, 128, 256, 300, 384, 600, 768, 1024}
	for _, length := range lengths {
		b.Run(fmt.Sprintf("vector dim=%d", length), func(b *testing.B) {
			x := make([]float32, length)
			y := make([]float32, length)
			for i := range x {
				x[i] = -r.Float32()
				y[i] = r.Float32()
			}

			b.Run("pure go", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					L2PureGo(x, y)
				}
			})

			b.Run("asm AVX", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					asm.L2(x, y)
				}
			})
		})
	}
}

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
	"golang.org/x/sys/cpu"
)

func L2PureGo(a, b []float32) float32 {
	var sum float32

	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}

	return sum
}

func L2PureGoByte(a, b []uint8) uint32 {
	var sum uint32

	for i := range a {
		diff := int32(a[i]) - int32(b[i])
		sum += uint32(diff * diff)
	}

	return sum
}

func Test_L2_DistanceImplementation(t *testing.T) {
	lengths := []int{1, 2, 3, 4, 5, 16, 31, 32, 35, 64, 67, 128, 130, 256, 260, 384, 390, 768, 777, 1000, 1536}

	for _, length := range lengths {
		t.Run(fmt.Sprintf("with vector l=%d", length), func(t *testing.T) {
			x := make([]float32, length)
			y := make([]float32, length)
			for i := range x {
				x[i] = rand.Float32()
				y[i] = rand.Float32()
			}

			control := L2PureGo(x, y)

			asmResult := asm.L2_Neon(x, y)
			assert.InEpsilon(t, control, asmResult, 0.01)

			if cpu.ARM64.HasSVE {
				asmResult := asm.L2_SVE(x, y)
				assert.InEpsilon(t, control, asmResult, 0.01)
			}
		})
	}
}

func Test_L2_DistanceImplementation_OneNegativeValue(t *testing.T) {
	lengths := []int{1, 2, 3, 4, 5, 16, 31, 32, 35, 64, 67, 128, 130, 256, 260, 384, 390, 768, 777, 1000, 1536}

	for _, length := range lengths {
		t.Run(fmt.Sprintf("with vector l=%d", length), func(t *testing.T) {
			x := make([]float32, length)
			y := make([]float32, length)
			for i := range x {
				x[i] = -rand.Float32()
				y[i] = rand.Float32()
			}

			control := L2PureGo(x, y)
			asmResult := asm.L2_Neon(x, y)
			assert.InEpsilon(t, control, asmResult, 0.01)

			if cpu.ARM64.HasSVE {
				asmResult := asm.L2_SVE(x, y)
				assert.InEpsilon(t, control, asmResult, 0.01)
			}
		})
	}
}

func Benchmark_L2_PureGo_VS_SIMD(b *testing.B) {
	r := getRandomSeed()
	lengths := []int{1, 2, 3, 4, 5, 16, 31, 32, 35, 64, 67, 128, 130, 256, 260, 384, 390, 768, 777, 1000, 1536}
	for _, length := range lengths {
		b.Run(fmt.Sprintf("vector dim=%d", length), func(b *testing.B) {
			x := make([]float32, length)
			y := make([]float32, length)
			for i := range x {
				x[i] = -r.Float32()
				y[i] = r.Float32()
			}

			b.ResetTimer()

			b.Run("pure go", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					L2PureGo(x, y)
				}
			})

			b.Run("asm Neon", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					asm.L2_Neon(x, y)
				}
			})

			b.Run("asm SVE", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					asm.L2_Neon(x, y)
				}
			})
		})
	}
}

func Test_L2_Byte_DistanceImplementation_RandomValues(t *testing.T) {
	lengths := []int{1, 2, 3, 4, 5, 16, 31, 32, 35, 64, 67, 128, 130, 256, 260, 384, 390, 768, 777, 1000, 1536}

	for _, length := range lengths {
		t.Run(fmt.Sprintf("with vector l=%d", length), func(t *testing.T) {
			x := make([]uint8, length)
			y := make([]uint8, length)
			for i := range x {
				x[i] = uint8(rand.Uint32() % 256)
				y[i] = uint8(rand.Uint32() % 256)
			}

			control := L2PureGoByte(x, y)

			asmResult := asm.L2ByteARM64(x, y)
			if uint32(control) != asmResult {
				t.Logf("for dim: %d -> want: %d, got: %d", length, control, asmResult)
				t.Fail()
			}
		})
	}
}

func Test_L2_Byte_DistanceImplementation_FixedValues(t *testing.T) {
	lengths := []int{1, 2, 3, 4, 5, 16, 31, 32, 35, 64, 67, 128, 130, 256, 260, 384, 390, 768, 777, 1000, 1536}

	for _, length := range lengths {
		t.Run(fmt.Sprintf("with vector l=%d", length), func(t *testing.T) {
			x := make([]uint8, length)
			y := make([]uint8, length)
			for i := range x {
				x[i] = uint8(251)
				y[i] = uint8(251)
			}

			control := L2PureGoByte(x, y)

			asmResult := asm.L2ByteARM64(x, y)
			if uint32(control) != asmResult {
				t.Logf("for dim: %d -> want: %d, got: %d", length, control, asmResult)
				t.Fail()
			}
		})
	}
}

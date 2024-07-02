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
	"github.com/stretchr/testify/require"
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

func L2BytePureGo(a, b []uint8) uint32 {
	var sum uint32

	for i := range a {
		diff := int32(a[i]) - int32(b[i])
		sum += uint32(diff * diff)
	}

	return sum
}

func L2FloatBytePureGo(a []float32, b []byte) float32 {
	var sum float32

	for i := range a {
		diff := a[i] - float32(b[i])
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

			asmResult := asm.L2AVX256(x, y)
			assert.InEpsilon(t, control, asmResult, 0.01)

			if cpu.X86.HasAVX512 {
				asmResult = asm.L2AVX512(x, y)
				assert.InEpsilon(t, control, asmResult, 0.01)
			}
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

			asmResult := asm.L2AVX256(x, y)
			assert.InEpsilon(t, control, asmResult, 0.01)

			if cpu.X86.HasAVX512 {
				asmResult = asm.L2AVX512(x, y)
				assert.InEpsilon(t, control, asmResult, 0.01)
			}
		})
	}
}

func Test_L2_Byte_DistanceImplementation(t *testing.T) {
	lengths := []int{1, 2, 3, 4, 5, 16, 31, 32, 35, 64, 67, 128, 130, 256, 260, 384, 390, 768, 777, 1000, 1536}

	for _, length := range lengths {
		t.Run(fmt.Sprintf("with vector l=%d", length), func(t *testing.T) {
			x := make([]uint8, length)
			y := make([]uint8, length)
			for i := range x {
				x[i] = uint8(rand.Uint32() % 256)
				y[i] = uint8(rand.Uint32() % 256)
			}

			control := L2BytePureGo(x, y)

			asmResult := asm.L2ByteAVX256(x, y)
			require.Equal(t, int(control), int(asmResult))
		})
	}
}

func Test_L2_FloatByte_DistanceImplementation(t *testing.T) {
	lengths := []int{1, 2, 3, 4, 5, 16, 31, 32, 35, 64, 67, 128, 130, 256, 260, 384, 390, 768, 777, 1000, 1536}

	for _, length := range lengths {
		t.Run(fmt.Sprintf("with vector l=%d", length), func(t *testing.T) {
			x := make([]float32, length)
			y := make([]uint8, length)
			for i := range x {
				x[i] = float32(rand.Uint32())
				y[i] = uint8(rand.Uint32() % 256)
			}

			control := L2FloatBytePureGo(x, y)

			asmResult := asm.L2FloatByteAVX256(x, y)
			assert.InEpsilon(t, control, asmResult, 0.01)
		})
	}
}

func Benchmark_L2(b *testing.B) {
	r := getRandomSeed()
	lengths := []int{
		1,
		2,
		3,
		4,
		5,
		6,
		8,
		10,
		12,
		16,
		24,
		30,
		31,
		32,
		64,
		67,
		128,
		256,
		260,
		299,
		300,
		384,
		390,
		600,
		768,
		777,
		784,
		1024,
		1536,
	}
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

			b.Run("asm AVX512", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					asm.L2AVX512(x, y)
				}
			})
		})
	}
}

func Benchmark_L2Byte(b *testing.B) {
	lengths := []int{
		1,
		2,
		3,
		4,
		5,
		6,
		8,
		10,
		12,
		16,
		24,
		30,
		31,
		32,
		64,
		67,
		128,
		256,
		260,
		299,
		300,
		384,
		390,
		600,
		768,
		777,
		784,
		1024,
		1536,
	}
	for _, length := range lengths {
		b.Run(fmt.Sprintf("vector dim=%d", length), func(b *testing.B) {
			x := make([]uint8, length)
			y := make([]uint8, length)
			for i := range x {
				x[i] = uint8(rand.Uint32() % 256)
				y[i] = uint8(rand.Uint32() % 256)
			}

			b.ResetTimer()

			b.Run("pure go", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					L2BytePureGo(x, y)
				}
			})

			b.Run("asm AVX", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					asm.L2ByteAVX256(x, y)
				}
			})
		})
	}
}

func Benchmark_L2FloatByte(b *testing.B) {
	r := getRandomSeed()
	lengths := []int{
		1,
		2,
		3,
		4,
		5,
		6,
		8,
		10,
		12,
		16,
		24,
		30,
		31,
		32,
		64,
		67,
		128,
		256,
		260,
		299,
		300,
		384,
		390,
		600,
		768,
		777,
		784,
		1024,
		1536,
	}
	for _, length := range lengths {
		b.Run(fmt.Sprintf("vector dim=%d", length), func(b *testing.B) {
			x := make([]float32, length)
			y := make([]byte, length)
			for i := range x {
				x[i] = -r.Float32()
				y[i] = uint8(rand.Uint32() % 256)
			}

			b.ResetTimer()

			b.Run("pure go", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					L2FloatBytePureGo(x, y)
				}
			})

			b.Run("asm AVX", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					asm.L2FloatByteAVX256(x, y)
				}
			})
		})
	}
}

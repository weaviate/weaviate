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
	"math/bits"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
	"golang.org/x/sys/cpu"
)

func HammingBitwiseGo(x, y []uint64) float32 {
	total := float32(0)
	for segment := range x {
		total += float32(bits.OnesCount64(x[segment] ^ y[segment]))
	}
	return total
}

func testHammingBitwiseFixedValue(t *testing.T, size uint, hammingBitwiseFn func(x []uint64, y []uint64) float32) {
	for num := 0; num < 255; num++ {
		vec1 := make([]uint64, size)
		vec2 := make([]uint64, size)
		for i := range vec1 {
			vec1[i] = uint64(num)
			vec2[i] = uint64(num + 1)
		}
		res := hammingBitwiseFn(vec1, vec2)

		resControl := HammingBitwiseGo(vec1, vec2)
		if resControl != res {
			t.Logf("for dim: %d -> want: %f, got: %f", size, resControl, res)
			t.Fail()
		}
	}
}

func testHammingBitwiseRandomValue(t *testing.T, size uint, hammingBitwiseFn func(x []uint64, y []uint64) float32) {
	r := getRandomSeed()
	count := 10000

	vec1s := make([][]uint64, count)
	vec2s := make([][]uint64, count)

	for i := 0; i < count; i++ {
		vec1 := make([]uint64, size)
		vec2 := make([]uint64, size)
		for j := range vec1 {
			vec1[j] = r.Uint64()
			vec2[j] = r.Uint64()

		}
		vec1s[i] = vec1
		vec2s[i] = vec2
	}

	for i := 0; i < count; i++ {
		res := hammingBitwiseFn(vec1s[i], vec2s[i])

		resControl := HammingBitwiseGo(vec1s[i], vec2s[i])
		if resControl != res {
			t.Logf("for dim: %d -> want: %f, got: %f", size, resControl, res)
			t.Fail()
		}
	}
}

func TestCompareHammingBitwise(t *testing.T) {
	sizes := []uint{
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

	for _, size := range sizes {
		t.Run(fmt.Sprintf("with size %d", size), func(t *testing.T) {
			if cpu.ARM64.HasASIMD {
				testHammingBitwiseFixedValue(t, size, asm.HammingBitwise)
				testHammingBitwiseRandomValue(t, size, asm.HammingBitwise)
			}
		})
	}
}

func benchmarkHammingBitwise(b *testing.B, dims int, hammingBitwiseFn func(x []uint64, y []uint64) float32) {
	r := getRandomSeed()

	vec1 := make([]uint64, dims)
	vec2 := make([]uint64, dims)
	for i := range vec1 {
		vec1[i] = r.Uint64()
		vec2[i] = r.Uint64()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		hammingBitwiseFn(vec1, vec2)
	}
}

func BenchmarkHammingBitwise(b *testing.B) {
	dims := []int{2, 4, 6, 8, 10, 12, 16, 24, 30, 32, 128, 256, 300, 384, 512, 768, 1024, 1536}
	for _, dim := range dims {
		b.Run(fmt.Sprintf("%d dimensions", dim), func(b *testing.B) {
			benchmarkHammingBitwise(b, dim, asm.HammingBitwise)

			b.Run("pure go", func(b *testing.B) { benchmarkHammingBitwise(b, dim, HammingBitwiseGo) })
			b.Run("neon", func(b *testing.B) { benchmarkHammingBitwise(b, dim, asm.HammingBitwise) })
		})
	}
}

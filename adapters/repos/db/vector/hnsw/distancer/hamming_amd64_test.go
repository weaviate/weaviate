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
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

func testHammingByteFixedValue(t *testing.T, size uint, hammingByteFn func(x []uint8, y []uint8) uint32) {
	for num := 0; num < 255; num++ {
		vec1 := make([]uint8, size)
		vec2 := make([]uint8, size)
		for i := range vec1 {
			vec1[i] = uint8(num)
			vec2[i] = uint8(num)
		}
		res := hammingByteFn(vec1, vec2)

		resControl := HammingDistanceByteGo(vec1, vec2)
		if uint32(resControl) != res {
			t.Logf("for dim: %d -> want: %d, got: %d", size, resControl, res)
			t.Fail()
		}
	}
}

func testHammingByteRandomValue(t *testing.T, size uint, hammingByteFn func(x []uint8, y []uint8) uint32) {
	r := getRandomSeed()
	count := 10000

	vec1s := make([][]byte, count)
	vec2s := make([][]byte, count)

	for i := 0; i < count; i++ {
		vec1 := make([]byte, size)
		vec2 := make([]byte, size)
		for j := range vec1 {
			rand1 := byte(r.Uint32() % 256)
			rand2 := byte(r.Uint32() % 256)

			vec1[j] = rand1
			vec2[j] = rand2
		}
	}

	for i := 0; i < count; i++ {
		res := hammingByteFn(vec1s[i], vec2s[i])

		resControl := HammingDistanceByteGo(vec1s[i], vec2s[i])
		if uint32(resControl) != res {
			t.Logf("for dim: %d -> want: %d, got: %d", size, resControl, res)
			t.Fail()
		}
	}
}

func TestCompareHammingByte(t *testing.T) {
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
			testHammingByteFixedValue(t, size, asm.HammingByteAVX256)
			testHammingByteRandomValue(t, size, asm.HammingByteAVX256)
		})
	}
}

func benchmarkHammingByte(b *testing.B, dims int, hammingByteFn func(a, b []uint8) uint32) {
	r := getRandomSeed()

	vec1 := make([]byte, dims)
	vec2 := make([]byte, dims)
	for i := range vec1 {
		vec1[i] = byte(r.Uint32() % 256)
		vec2[i] = byte(r.Uint32() % 256)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		hammingByteFn(vec1, vec2)
	}
}

func BenchmarkHammingByte(b *testing.B) {
	dims := []int{2, 4, 6, 8, 10, 12, 16, 24, 30, 32, 128, 256, 300, 384, 512, 768, 1024, 1536}
	for _, dim := range dims {
		b.Run(fmt.Sprintf("%d dimensions", dim), func(b *testing.B) {
			// benchmarkDotByte(b, dim, dotByteImpl)
			benchmarkHammingByte(b, dim, asm.HammingByteAVX256)

			b.Run("pure go", func(b *testing.B) { benchmarkHammingByte(b, dim, HammingDistanceByteGo) })
			b.Run("neon", func(b *testing.B) { benchmarkHammingByte(b, dim, asm.HammingByteAVX256) })
		})
	}
}

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
	"math"
	"testing"
	"unsafe"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
	"golang.org/x/sys/cpu"
)

var dotByteImpl func(a, b []byte) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		sum += uint32(a[i]) * uint32(b[i])
	}

	return uint32(sum)
}

func dotFloatByteImpl(a []float32, b []byte) float32 {
	var sum float32

	for i := range a {
		sum += a[i] * float32(b[i])
	}

	return sum
}

func testDotProductFixedValue(t *testing.T, size uint) {
	count := 10000
	countFailed := 0
	for i := 0; i < count; i++ {
		vec1 := make([]float32, size)
		vec2 := make([]float32, size)
		for i := range vec1 {
			vec1[i] = 129
			vec2[i] = 129
		}
		vec1 = Normalize(vec1)
		vec2 = Normalize(vec2)
		res := -asm.Dot_Neon(vec1, vec2)
		if cpu.ARM64.HasSVE {
			res = -asm.Dot_SVE(vec1, vec2)
		}
		if math.IsNaN(float64(res)) {
			panic("NaN")
		}

		resControl := DotProductFloatGo(vec1, vec2)
		delta := float64(0.01)
		diff := float64(resControl) - float64(res)
		if diff < -delta || diff > delta {
			countFailed++

			fmt.Printf("run %d: match: %f != %f\n", i, resControl, res)

			t.Fail()
		}
	}

	fmt.Printf("total failed: %d\n", countFailed)
}

func testDotProductRandomValue(t *testing.T, size uint) {
	r := getRandomSeed()
	count := 10000
	countFailed := 0

	vec1s := make([][]float32, count)
	vec2s := make([][]float32, count)

	for i := 0; i < count; i++ {
		vec1 := make([]float32, size)
		vec2 := make([]float32, size)
		for j := range vec1 {
			vec1[j] = r.Float32()
			vec2[j] = r.Float32()
		}
		vec1s[i] = Normalize(vec1)
		vec2s[i] = Normalize(vec2)
	}

	for i := 0; i < count; i++ {
		res := -asm.Dot_Neon(vec1s[i], vec2s[i])
		if cpu.ARM64.HasSVE {
			res = -asm.Dot_SVE(vec1s[i], vec2s[i])
		}
		if math.IsNaN(float64(res)) {
			panic("NaN")
		}

		resControl := DotProductFloatGo(vec1s[i], vec2s[i])
		delta := float64(0.01)
		diff := float64(resControl) - float64(res)
		if diff < -delta || diff > delta {
			countFailed++

			fmt.Printf("run %d: match: %f != %f, %d\n", i, resControl, res, (unsafe.Pointer(&vec1s[i][0])))

			t.Fail()
		}

	}
	fmt.Printf("total failed: %d\n", countFailed)
}

func TestCompareDotProductImplementations(t *testing.T) {
	sizes := []uint{
		1,
		4,
		8,
		16,
		31,
		32,
		35,
		64,
		67,
		128,
		130,
		256,
		260,
		384,
		390,
		768,
		777,
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("with size %d", size), func(t *testing.T) {
			testDotProductFixedValue(t, size)
			testDotProductRandomValue(t, size)
		})
	}
}

func testDotProductByteFixedValue(t *testing.T, size uint, dotFn func(x []uint8, y []uint8) uint32) {
	for num := 0; num < 255; num++ {
		vec1 := make([]uint8, size)
		vec2 := make([]uint8, size)
		for i := range vec1 {
			vec1[i] = uint8(num)
			vec2[i] = uint8(num)
		}
		res := dotFn(vec1, vec2)

		resControl := dotByteImpl(vec1, vec2)
		if uint32(resControl) != res {
			t.Logf("for dim: %d -> want: %d, got: %d", size, resControl, res)
			t.Fail()
		}
	}
}

func testDotProductByteRandomValue(t *testing.T, size uint, dotFn func(x []byte, y []byte) uint32) {
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
		res := dotFn(vec1s[i], vec2s[i])

		resControl := dotByteImpl(vec1s[i], vec2s[i])
		if uint32(resControl) != res {
			t.Logf("for dim: %d -> want: %d, got: %d", size, resControl, res)
			t.Fail()
		}
	}
}

func TestCompareDotProductByte(t *testing.T) {
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
			testDotProductByteFixedValue(t, size, asm.DotByteARM64)
			testDotProductByteRandomValue(t, size, asm.DotByteARM64)
		})
	}
}

func benchmarkDotByte(b *testing.B, dims int, dotFn func(a, b []byte) uint32) {
	r := getRandomSeed()

	vec1 := make([]byte, dims)
	vec2 := make([]byte, dims)
	for i := range vec1 {
		vec1[i] = byte(r.Uint32() % 256)
		vec2[i] = byte(r.Uint32() % 256)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dotFn(vec1, vec2)
	}
}

func BenchmarkDotByte(b *testing.B) {
	dims := []int{2, 4, 6, 8, 10, 12, 16, 24, 30, 32, 128, 256, 300, 384, 512, 768, 1024, 1536}
	for _, dim := range dims {
		b.Run(fmt.Sprintf("%d dimensions", dim), func(b *testing.B) {
			// benchmarkDotByte(b, dim, dotByteImpl)
			benchmarkDotByte(b, dim, asm.DotByteARM64)

			// b.Run("pure go", func(b *testing.B) { benchmarkDotByte(b, dim, dotByteImpl) })
			// b.Run("avx", func(b *testing.B) { benchmarkDotByte(b, dim, asm.DotByteAVX256) })
		})
	}
}

func testDotProductFloatByteFixedValue(t *testing.T, size uint, dotFn func(x []float32, y []uint8) float32) {
	for num := 0; num < 4; num++ {
		vec1 := make([]float32, size)
		vec2 := make([]uint8, size)
		for i := range vec1 {
			vec1[i] = float32(num)
			vec2[i] = uint8(num)
		}
		res := dotFn(vec1, vec2)

		resControl := dotFloatByteImpl(vec1, vec2)
		if resControl != res {
			t.Logf("for dim: %d and %d -> want: %f, got: %f", num, size, resControl, res)
			t.Fail()
		}
	}
}

func testDotProductFloatByteRandomValue(t *testing.T, size uint, dotFn func(x []float32, y []byte) float32) {
	r := getRandomSeed()
	count := 10000

	vec1s := make([][]float32, count)
	vec2s := make([][]byte, count)

	for i := 0; i < count; i++ {
		vec1 := make([]float32, size)
		vec2 := make([]byte, size)
		for j := range vec1 {
			rand1 := r.Float32()
			rand2 := byte(r.Uint32() % 256)

			vec1[j] = rand1
			vec2[j] = rand2
		}
	}

	for i := 0; i < count; i++ {
		res := dotFn(vec1s[i], vec2s[i])

		resControl := dotFloatByteImpl(vec1s[i], vec2s[i])
		if resControl != res {
			t.Logf("for dim: %d -> want: %f, got: %f", size, resControl, res)
			t.Fail()
		}
	}
}

func TestCompareDotProductFloatByte(t *testing.T) {
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
			testDotProductFloatByteFixedValue(t, size, asm.DotFloatByte_Neon)
			testDotProductFloatByteRandomValue(t, size, asm.DotFloatByte_Neon)
		})
	}
}

func benchmarkDotFloatByte(b *testing.B, dims int, dotFn func(a []float32, b []byte) float32) {
	r := getRandomSeed()

	vec1 := make([]float32, dims)
	vec2 := make([]byte, dims)
	for i := range vec1 {
		vec1[i] = r.Float32()
		vec2[i] = byte(r.Uint32() % 256)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dotFn(vec1, vec2)
	}
}

func BenchmarkDotFloatByte(b *testing.B) {
	dims := []int{2, 4, 6, 8, 10, 12, 16, 24, 30, 32, 128, 256, 300, 384, 512, 768, 1024, 1536}
	for _, dim := range dims {
		b.Run(fmt.Sprintf("%d dimensions", dim), func(b *testing.B) {
			b.Run("pure go", func(b *testing.B) { benchmarkDotFloatByte(b, dim, dotFloatByteImpl) })
			b.Run("neon", func(b *testing.B) { benchmarkDotFloatByte(b, dim, asm.DotFloatByte_Neon) })
		})
	}
}

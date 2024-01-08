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
)

func testDotProductFixedValue(t *testing.T, size uint) {
	count := 10000
	countFailed := 0
	for i := 0; i < count; i++ {
		vec1 := make([]float32, size)
		vec2 := make([]float32, size)
		for i := range vec1 {
			vec1[i] = 1
			vec2[i] = 1
		}
		vec1 = Normalize(vec1)
		vec2 = Normalize(vec2)
		res := -asm.Dot(vec1, vec2)
		if math.IsNaN(float64(res)) {
			panic("NaN")
		}

		resControl := DotProductGo(vec1, vec2)
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
		res := -asm.Dot(vec1s[i], vec2s[i])
		if math.IsNaN(float64(res)) {
			panic("NaN")
		}

		resControl := DotProductGo(vec1s[i], vec2s[i])
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
		8,
		16,
		32,
		64,
		128,
		256,
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("with size %d", size), func(t *testing.T) {
			testDotProductFixedValue(t, size)
			testDotProductRandomValue(t, size)
		})
	}
}

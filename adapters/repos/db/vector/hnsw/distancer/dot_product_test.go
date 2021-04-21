package distancer

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
	"unsafe"

	"github.com/mmcloughlin/avo/examples/dot"
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
		res := 1 - dot.Dot(vec1, vec2)
		if math.IsNaN(float64(res)) {
			panic("NaN")
		}

		// runtime.KeepAlive(&vec1)
		// runtime.KeepAlive(&vec2)

		// controlString := fmt.Sprintf("%f", res)
		resControl := DotProductGo(vec1, vec2)
		delta := float64(0.01)
		diff := float64(resControl) - float64(res)
		if diff < -delta || diff > delta {
			countFailed++

			// fmt.Printf("float 32 as string was %s\n", controlString)
			fmt.Printf("run %d: match: %f != %f\n", i, resControl, res)

			t.Fail()
		}
		// if resControl != res {
		// 	diff
		// 	fmt.Printf("run %d\n", i)
		// 	t.Fail()
		// } else {
		// 	fmt.Printf("match: %f == %f\n", resControl, res)
		// }
		// assert.InDelta(t, resControl, res, 0.01)
		// assert.GreaterOrEqual(t, res, float32(0))
		// assert.LessOrEqual(t, res, float32(1))
	}

	fmt.Printf("total failed: %d\n", countFailed)
}

func testDotProductRandomValue(t *testing.T, size uint) {
	count := 10000
	countFailed := 0

	vec1s := make([][]float32, count)
	vec2s := make([][]float32, count)

	for i := 0; i < count; i++ {
		vec1 := make([]float32, size)
		vec2 := make([]float32, size)
		for j := range vec1 {
			vec1[j] = rand.Float32()
			vec2[j] = rand.Float32()
		}
		vec1s[i] = Normalize(vec1)
		vec2s[i] = Normalize(vec2)
	}

	// for i := range vec1s {
	// 	fmt.Printf("vec 1: %v\n", vec1s[i])
	// 	fmt.Printf("vec 2: %v\n", vec2s[i])
	// }

	for i := 0; i < count; i++ {
		// fmt.Printf("run %d: before %d\n", i, (unsafe.Pointer(&vec1s[i][0])))
		res := 1 - dot.Dot(vec1s[i], vec2s[i])
		// res := DotProductGoNum(vec1s[i], vec2s[i])
		// el1 := LastElement(vec1s[i], vec2s[i])
		// res := DotProductGo(vec1s[i], vec2s[i])
		if math.IsNaN(float64(res)) {
			panic("NaN")
		}

		// idx := len(vec1s[i]) - 1
		// if el1 != vec1s[i][idx] {

		// 	fmt.Printf("Mismatch %f != %f (previous: %f)\n", el1, vec1s[i][idx], vec1s[i][idx-1])
		// 	t.Fail()

		// }
		// assert.InDelta(t, resControl, res, 0.01)
		// assert.GreaterOrEqual(t, res, float32(0))
		// assert.LessOrEqual(t, res, float32(1))
		resControl := DotProductGo(vec1s[i], vec2s[i])
		delta := float64(0.01)
		diff := float64(resControl) - float64(res)
		if diff < -delta || diff > delta {
			countFailed++

			// fmt.Printf("float 32 as string was %s\n", controlString)
			fmt.Printf("run %d: match: %f != %f, %d\n", i, resControl, res, (unsafe.Pointer(&vec1s[i][0])))

			t.Fail()
		} else {
			if i%50 == 0 {
				fmt.Printf("run %d successful\n", i)
			}
		}

	}
	fmt.Printf("total failed: %d\n", countFailed)
}

func TestCompareDotProductImplementations(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
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

// func TestLast(t *testing.T) {
// 	vec1 := []float32{0.1, 0.2, 0.3}
// 	vec2 := []float32{0.2, 0.3, 0.4}
// 	el := LastElement(vec1, vec2)
// 	fmt.Printf("%f\n", el)
// 	t.Fail()
// }

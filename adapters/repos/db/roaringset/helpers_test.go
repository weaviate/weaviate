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

package roaringset

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/sroar"
)

var logger, _ = test.NewNullLogger()

func TestBitmap_Condense(t *testing.T) {
	t.Run("And with itself (internal array)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 1000)...)
		for i := 0; i < 10; i++ {
			bm.And(bm)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And with itself (internal bitmap)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 3000)...)
		for i := 0; i < 10; i++ {
			bm.And(bm)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And (internal arrays)", func(t *testing.T) {
		bm1 := NewBitmap(slice(0, 1000)...)
		bm2 := NewBitmap(slice(500, 1500)...)
		bm := bm1.Clone()
		bm.And(bm2)
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And (internal bitmaps)", func(t *testing.T) {
		bm1 := NewBitmap(slice(0, 4000)...)
		bm2 := NewBitmap(slice(1000, 5000)...)
		bm := bm1.Clone()
		bm.And(bm2)
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And (internal bitmaps to bitmap with few elements)", func(t *testing.T) {
		// this is not optimal. Internally elements will be stored in bitmap,
		// though they would easily fit into array
		bm1 := NewBitmap(slice(0, 4000)...)
		bm2 := NewBitmap(slice(1000, 5000)...)
		bm := bm1.Clone()
		bm.And(bm2)
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("Remove (array)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 1000)...)
		for i := uint64(2); i < 1000; i++ {
			bm.Remove(i)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("Remove (bitmap)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 100_000)...)
		for i := uint64(10_000); i < 100_000; i++ {
			bm.Remove(i)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})
}

func TestBitmap_Prefill(t *testing.T) {
	t.Run("sequential", func(t *testing.T) {
		for _, maxVal := range []uint64{1_000, 10_000, 100_000, 1_000_000, uint64(prefillBufferSize)} {
			t.Run(fmt.Sprint(maxVal), func(t *testing.T) {
				bm := newBitmapPrefillSequential(maxVal)

				// +1, due to 0 included
				assert.Equal(t, int(maxVal)+1, bm.GetCardinality())

				// remove all except maxVal
				bm.RemoveRange(0, maxVal)

				assert.Equal(t, 1, bm.GetCardinality())
				assert.True(t, bm.Contains(maxVal))
			})
		}
	})

	t.Run("parallel", func(t *testing.T) {
		for _, maxVal := range []uint64{1_000, 10_000, 100_000, 1_000_000, uint64(prefillBufferSize)} {
			for _, routinesLimit := range []int{2, 3, 4, 5, 6, 7, 8} {
				t.Run(fmt.Sprint(maxVal), func(t *testing.T) {
					bm := newBitmapPrefillParallel(maxVal, routinesLimit, logger)

					// +1, due to 0 included
					assert.Equal(t, int(maxVal)+1, bm.GetCardinality())

					// remove all except maxVal
					bm.RemoveRange(0, maxVal)

					assert.Equal(t, 1, bm.GetCardinality())
					assert.True(t, bm.Contains(maxVal))
				})
			}
		}
	})

	t.Run("conditional - sequential or parallel", func(t *testing.T) {
		for _, maxVal := range []uint64{1_000, 10_000, 100_000, 1_000_000, uint64(prefillBufferSize)} {
			t.Run(fmt.Sprint(maxVal), func(t *testing.T) {
				bm := NewBitmapPrefill(maxVal, logger)

				// +1, due to 0 included
				assert.Equal(t, int(maxVal)+1, bm.GetCardinality())

				// remove all except maxVal
				bm.RemoveRange(0, maxVal)

				assert.Equal(t, 1, bm.GetCardinality())
				assert.True(t, bm.Contains(maxVal))
			})
		}
	})
}

func TestBitmap_Inverted(t *testing.T) {
	type test struct {
		name          string
		source        []uint64
		maxVal        uint64
		shouldContain []uint64
	}

	tests := []test{
		{
			name:          "just 0, no source",
			source:        nil,
			maxVal:        0,
			shouldContain: []uint64{0},
		},
		{
			name:          "no matches in source",
			source:        nil,
			maxVal:        7,
			shouldContain: []uint64{0, 1, 2, 3, 4, 5, 6, 7},
		},
		{
			name:          "some matches in source",
			source:        []uint64{3, 4, 5},
			maxVal:        7,
			shouldContain: []uint64{0, 1, 2, 6, 7},
		},
		{
			name:          "source has higher val than max val",
			source:        []uint64{3, 4, 5, 8},
			maxVal:        7,
			shouldContain: []uint64{0, 1, 2, 6, 7},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := sroar.NewBitmap()
			source.SetMany(test.source)
			out := NewInvertedBitmap(source, test.maxVal, logger)
			outSlice := out.ToArray()
			assert.Equal(t, test.shouldContain, outSlice)
		})
	}
}

func TestBitmapFactory(t *testing.T) {
	maxVal := uint64(10)
	maxValGetter := func() uint64 { return maxVal }
	bmf := NewBitmapFactory(maxValGetter, logger)
	t.Logf("card: %d", bmf.bitmap.GetCardinality())

	currMax := bmf.currentMaxVal
	t.Run("max val set correctly", func(t *testing.T) {
		assert.Equal(t, maxVal+DefaultBufferIncrement, currMax)
	})

	t.Run("max val increased to threshold does not change cardinality", func(t *testing.T) {
		maxVal += 100
		assert.NotNil(t, bmf.GetBitmap())
		assert.Equal(t, currMax, bmf.currentMaxVal)
		assert.Equal(t, currMax+1, uint64(bmf.bitmap.GetCardinality()))
		assert.Equal(t, maxVal, bmf.ActualMaxVal())
	})

	t.Run("max val surpasses threshold, cardinality increased", func(t *testing.T) {
		maxVal += 1
		assert.NotNil(t, bmf.GetBitmap())
		currMax += 1 + DefaultBufferIncrement
		assert.Equal(t, currMax, bmf.currentMaxVal)
		assert.Equal(t, currMax+1, uint64(bmf.bitmap.GetCardinality()))
		assert.Equal(t, maxVal, bmf.ActualMaxVal())
	})
}

func slice(from, to uint64) []uint64 {
	len := to - from
	s := make([]uint64, len)
	for i := uint64(0); i < len; i++ {
		s[i] = from + i
	}
	return s
}

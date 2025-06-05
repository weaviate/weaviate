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

package packedconn

import (
	"math/rand"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEncodeDecode(t *testing.T) {
	testValues := [][]uint64{
		{4477, 83, 6777, 13118, 12903},
		{1, 2, 3, 4, 5},
		{127, 128, 129, 255, 256},
		{65535, 65536, 16777215, 16777216},
		{0, 1, 127, 128, 255, 256, 65535, 65536},
	}

	buffer := make([]uint64, 2)
	for _, values := range testValues {
		encoder := NewPrefixEncoder()
		buffer = encoder.Decode(buffer)
		assert.Equal(t, 0, len(buffer))
		encoder.Init(values)
		buffer = encoder.Decode(buffer)
		assert.ElementsMatch(t, values, buffer)
	}
}

func TestEncodeGrowsSuccessfullyDecode(t *testing.T) {
	testValues := []uint64{1, 2, 3, 4, 5, 127, 128, 129, 255, 256, 65535, 65536, 16777215, 16777216, 0, 1, 127, 128, 255, 256, 65535, 65536}

	buffer := make([]uint64, 2)
	encoder := NewPrefixEncoder()
	buffer = encoder.Decode(buffer)
	assert.Equal(t, 0, len(buffer))
	encoder.Init(testValues)
	buffer = encoder.Decode(buffer)
	assert.ElementsMatch(t, testValues, buffer)
}

func TestInsertBefore(t *testing.T) {
	testValues := []uint64{
		2, 500,
	}

	buffer := make([]uint64, 2)
	encoder := NewPrefixEncoder()
	for _, item := range testValues {
		encoder.Add(item)
	}
	encoder.Add(1)
	testValues = append(testValues, 1)
	buffer = encoder.Decode(buffer)
	slices.Sort(testValues)
	assert.ElementsMatch(t, testValues, buffer)
}

func TestInsertBeforeMedium(t *testing.T) {
	testValues := []uint64{
		4477, 83, 6777, 13118,
	}

	buffer := make([]uint64, 2)
	encoder := NewPrefixEncoder()
	for _, item := range testValues {
		encoder.Add(item)
	}
	encoder.Add(12903)
	testValues = append(testValues, 12903)
	buffer = encoder.Decode(buffer)
	slices.Sort(testValues)
	assert.ElementsMatch(t, testValues, buffer)
}

func TestAdd(t *testing.T) {
	appendN := 100
	maxElements := 32

	for _, startSize := range []int{0, 8, 16, 24} {
		if startSize >= maxElements {
			continue
		}

		elementsToAdd := maxElements - startSize
		for i := 0; i < appendN; i++ {
			vs := NewValueSelector()
			initialValues := generateTestValuesFromGenerator(startSize, 1_000_000, vs)

			encoder := NewPrefixEncoder()
			if startSize > 0 {
				encoder.Init(initialValues)
			}

			for j := 0; j < elementsToAdd; j++ {
				nextElement := vs.GetNextValue()
				encoder.Add(nextElement)
				assert.Contains(t, encoder.Decode(nil), nextElement)
			}
		}
	}
}

type ValueSelector struct {
	generatedValues []uint64
	sortedValues    []uint64
	rng             *rand.Rand
}

func NewValueSelector() *ValueSelector {
	return &ValueSelector{
		generatedValues: make([]uint64, 0),
		sortedValues:    make([]uint64, 0),
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (vs *ValueSelector) GenerateInitialValue(maxValue uint64) uint64 {
	var value uint64
	switch len(vs.generatedValues) % 4 {
	case 0:
		value = uint64(vs.rng.Intn(256)) // Small delta
	case 1:
		value = uint64(vs.rng.Intn(65536)) // Medium delta
	case 2:
		value = uint64(vs.rng.Intn(16777216)) // Large delta
	case 3:
		value = vs.rng.Uint64() % maxValue // Full range
	}

	vs.addValue(value)
	return value
}

func (vs *ValueSelector) GetNextValue() uint64 {
	if len(vs.sortedValues) == 0 {
		return vs.GenerateInitialValue(100)
	}

	prob := vs.rng.Float64()
	var nextValue uint64

	switch {
	case prob < 0.95:
		nextValue = vs.sortedValues[len(vs.sortedValues)-1] + vs.rng.Uint64()%100
		vs.sortedValues = append(vs.sortedValues, nextValue)
	case prob < 0.99:
		if len(vs.sortedValues) >= 2 {
			nextValue = vs.sortedValues[len(vs.sortedValues)-2] + uint64(vs.rng.Float32()*float32(vs.sortedValues[len(vs.sortedValues)-1]-vs.sortedValues[len(vs.sortedValues)-2]))
			temp := vs.sortedValues[len(vs.sortedValues)-1]
			vs.sortedValues[len(vs.sortedValues)-1] = nextValue
			vs.sortedValues = append(vs.sortedValues, temp)
		}
		nextValue = vs.sortedValues[len(vs.sortedValues)-1]
		vs.sortedValues = append(vs.sortedValues, nextValue)
	default:
		if len(vs.sortedValues) >= 3 {
			nextValue = vs.sortedValues[len(vs.sortedValues)-3] + uint64(vs.rng.Float32()*float32(vs.sortedValues[len(vs.sortedValues)-2]-vs.sortedValues[len(vs.sortedValues)-3]))
			temp := vs.sortedValues[len(vs.sortedValues)-1]
			vs.sortedValues[len(vs.sortedValues)-2], vs.sortedValues[len(vs.sortedValues)-1] = nextValue, vs.sortedValues[len(vs.sortedValues)-2]
			vs.sortedValues = append(vs.sortedValues, temp)
		}
		nextValue = vs.sortedValues[len(vs.sortedValues)-1]
		vs.sortedValues = append(vs.sortedValues, nextValue)
	}
	return nextValue
}

func (vs *ValueSelector) addValue(value uint64) {
	vs.generatedValues = append(vs.generatedValues, value)

	insertPos := sort.Search(len(vs.sortedValues), func(i int) bool {
		return vs.sortedValues[i] >= value
	})

	vs.sortedValues = append(vs.sortedValues, 0)
	copy(vs.sortedValues[insertPos+1:], vs.sortedValues[insertPos:])
	vs.sortedValues[insertPos] = value
}

func (vs *ValueSelector) Reset() {
	vs.generatedValues = vs.generatedValues[:0]
	vs.sortedValues = vs.sortedValues[:0]
}

func generateTestValuesFromGenerator(count int, maxValue uint64, vs *ValueSelector) []uint64 {
	values := make([]uint64, count)

	if count > 0 {
		values[0] = vs.GenerateInitialValue(maxValue)
	}

	for i := 1; i < count; i++ {
		values[i] = vs.GenerateInitialValue(maxValue)
	}

	return values
}

func TestBits(t *testing.T) {
	data := []byte{0b11011000, 0b11000100}
	control := []byte{0b01001000, 0b00010011}
	shiftBitsRightIterative(data, 4)
	assert.Equal(t, control, data)

	data = []byte{0b11011000, 0b11000100}
	control = []byte{0b11011000, 0b00010000}
	shiftBitsRightIterative(data, 8)
	assert.Equal(t, control, data)

	data = []byte{0b11011000, 0b11000100}
	control = []byte{0b00011000, 0b00010011}
	shiftBitsRightIterative(data, 6)
	assert.Equal(t, control, data)

	data = []byte{0b11011000, 0b11000100, 0b10101010}
	control = []byte{0b01001000, 0b00010011, 0b10101011}
	shiftBitsRightIterative(data, 4)
	assert.Equal(t, control, data)
}

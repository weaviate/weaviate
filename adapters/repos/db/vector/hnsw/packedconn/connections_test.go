//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package packedconn

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	connsSlice1 = []uint64{
		4477, 83, 6777, 13118, 12903, 12873, 14397, 15034, 15127, 15162, 15219, 15599, 17627,
		18624, 18844, 19359, 22981, 23099, 36188, 37400, 39724, 39810, 47254, 58047, 59647, 61746,
		64635, 66528, 70470, 73936, 86283, 86697, 120033, 129098, 131345, 137609, 140937, 186468,
		191226, 199803, 206818, 223456, 271063, 278598, 288539, 395876, 396785, 452103, 487237,
		506431, 507230, 554813, 572566, 595572, 660562, 694477, 728865, 730031, 746368, 809331,
		949338,
	}
	connsSlice2 = []uint64{
		10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
	}
	connsSlice3 = []uint64{
		9999, 10000, 10001,
	}
)

func TestConnections_ReplaceLayers(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	// Initially all layers should have length==0 and return no results
	assert.Equal(t, 0, c.LenAtLayer(0))
	assert.Len(t, c.GetLayer(0), 0)
	assert.Equal(t, 0, c.LenAtLayer(1))
	assert.Len(t, c.GetLayer(1), 0)
	assert.Equal(t, 0, c.LenAtLayer(2))
	assert.Len(t, c.GetLayer(2), 0)

	// replace layer 0, it should return the correct results, all others should
	// still be empty
	c.ReplaceLayer(0, connsSlice1)
	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.Len(t, c.GetLayer(1), 0)
	assert.Len(t, c.GetLayer(2), 0)

	// replace layer 1+2, other layers should be unaffected
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)
	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))
	assert.ElementsMatch(t, connsSlice3, c.GetLayer(2))

	// replace a layer with a smaller list to trigger a shrinking operation
	c.ReplaceLayer(2, []uint64{768})
	assert.ElementsMatch(t, []uint64{768}, c.GetLayer(2))
	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))

	// replace the other layers with smaller lists
	c.ReplaceLayer(0, connsSlice1[:5])
	c.ReplaceLayer(1, connsSlice2[:5])
	assert.ElementsMatch(t, connsSlice1[:5], c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2[:5], c.GetLayer(1))

	// finally grow all layers back to their original sizes again, to verify what
	// previous shrinking does not hinder future growing
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)
	c.ReplaceLayer(0, connsSlice1)
	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))
	assert.ElementsMatch(t, connsSlice3, c.GetLayer(2))
}

func TestConnections_ConstructorWithData(t *testing.T) {
	c, err := NewWithElements([][]uint64{
		connsSlice1,
		connsSlice2,
		connsSlice3,
	})
	require.Nil(t, err)

	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))
	assert.ElementsMatch(t, connsSlice3, c.GetLayer(2))
}

func TestConnections_CopyLayers(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	conns := make([]uint64, 0, 100)

	// Initially all layers should have length==0 and return no results
	assert.Equal(t, 0, c.LenAtLayer(0))
	assert.Len(t, c.CopyLayer(conns, 0), 0)
	assert.Equal(t, 0, c.LenAtLayer(1))
	assert.Len(t, c.CopyLayer(conns, 1), 0)
	assert.Equal(t, 0, c.LenAtLayer(2))
	assert.Len(t, c.CopyLayer(conns, 2), 0)

	// replace layer 0, it should return the correct results, all others should
	// still be empty
	c.ReplaceLayer(0, connsSlice1)
	assert.ElementsMatch(t, connsSlice1, c.CopyLayer(conns, 0))
	assert.Len(t, c.CopyLayer(conns, 1), 0)
	assert.Len(t, c.CopyLayer(conns, 2), 0)

	// replace layer 1+2, other layers should be unaffected
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)
	assert.ElementsMatch(t, connsSlice1, c.CopyLayer(conns, 0))
	assert.ElementsMatch(t, connsSlice2, c.CopyLayer(conns, 1))
	assert.ElementsMatch(t, connsSlice3, c.CopyLayer(conns, 2))

	// replace a layer with a smaller list to trigger a shrinking operation
	c.ReplaceLayer(2, []uint64{768})
	assert.ElementsMatch(t, []uint64{768}, c.CopyLayer(conns, 2))
	assert.ElementsMatch(t, connsSlice1, c.CopyLayer(conns, 0))
	assert.ElementsMatch(t, connsSlice2, c.CopyLayer(conns, 1))

	// replace the other layers with smaller lists
	c.ReplaceLayer(0, connsSlice1[:5])
	c.ReplaceLayer(1, connsSlice2[:5])
	assert.ElementsMatch(t, connsSlice1[:5], c.CopyLayer(conns, 0))
	assert.ElementsMatch(t, connsSlice2[:5], c.CopyLayer(conns, 1))

	// finally grow all layers back to their original sizes again, to verify what
	// previous shrinking does not hinder future growing
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)
	c.ReplaceLayer(0, connsSlice1)
	assert.ElementsMatch(t, connsSlice1, c.CopyLayer(conns, 0))
	assert.ElementsMatch(t, connsSlice2, c.CopyLayer(conns, 1))
	assert.ElementsMatch(t, connsSlice3, c.CopyLayer(conns, 2))
}

func TestConnections_InsertLayers(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	assert.Equal(t, 0, c.LenAtLayer(0))
	assert.Len(t, c.GetLayer(0), 0)
	assert.Equal(t, 0, c.LenAtLayer(1))
	assert.Len(t, c.GetLayer(1), 0)
	assert.Equal(t, 0, c.LenAtLayer(2))
	assert.Len(t, c.GetLayer(2), 0)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)

	c.ReplaceLayer(1, []uint64{})
	shuffled := make([]uint64, len(connsSlice2))
	copy(shuffled, connsSlice2)
	shuffled = append(shuffled, 10000)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	for _, item := range shuffled {
		c.InsertAtLayer(item, 1)
	}

	conns2 := c.GetLayer(1)
	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, shuffled, conns2)
	assert.ElementsMatch(t, connsSlice3, c.GetLayer(2))
}

func TestConnections_InsertLayersAtEnd(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	assert.Equal(t, 0, c.LenAtLayer(0))
	assert.Len(t, c.GetLayer(0), 0)
	assert.Equal(t, 0, c.LenAtLayer(1))
	assert.Len(t, c.GetLayer(1), 0)
	assert.Equal(t, 0, c.LenAtLayer(2))
	assert.Len(t, c.GetLayer(2), 0)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)

	c.ReplaceLayer(0, []uint64{})
	shuffled := make([]uint64, len(connsSlice1))
	copy(shuffled, connsSlice1)
	shuffled = append(shuffled, 10000)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	for _, item := range shuffled {
		c.InsertAtLayer(item, 0)
	}

	conns1 := c.GetLayer(0)
	assert.ElementsMatch(t, shuffled, conns1)
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))
	assert.ElementsMatch(t, connsSlice3, c.GetLayer(2))
}

func TestConnections_InsertLayerAfterAddingLayer(t *testing.T) {
	c, err := NewWithMaxLayer(1)
	require.Nil(t, err)

	assert.Equal(t, 0, c.LenAtLayer(0))
	assert.Len(t, c.GetLayer(0), 0)
	assert.Equal(t, 0, c.LenAtLayer(1))
	assert.Len(t, c.GetLayer(1), 0)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)

	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))

	c.AddLayer()

	c.ReplaceLayer(0, []uint64{})
	shuffled := make([]uint64, len(connsSlice1))
	copy(shuffled, connsSlice1)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	for _, item := range shuffled {
		c.InsertAtLayer(item, 0)
	}

	c.ReplaceLayer(2, []uint64{})
	shuffled = make([]uint64, len(connsSlice3))
	copy(shuffled, connsSlice3)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	for _, item := range shuffled {
		c.InsertAtLayer(item, 2)
	}

	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))
	assert.ElementsMatch(t, connsSlice3, c.GetLayer(2))
}

func TestConnections_AccessHigherLayersDoesNotReturnData(t *testing.T) {
	c, err := NewWithMaxLayer(1)
	require.Nil(t, err)

	assert.Equal(t, 0, c.LenAtLayer(0))
	assert.Len(t, c.GetLayer(0), 0)
	assert.Equal(t, 0, c.LenAtLayer(1))
	assert.Len(t, c.GetLayer(1), 0)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)

	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))
	assert.ElementsMatch(t, nil, c.GetLayer(2))
}

func TestConnections_InsertLayersByNumber(t *testing.T) {
	c, err := NewWithMaxLayer(1)
	require.Nil(t, err)

	assert.Equal(t, 0, c.LenAtLayer(0))
	assert.Len(t, c.GetLayer(0), 0)
	assert.Equal(t, 0, c.LenAtLayer(1))
	assert.Len(t, c.GetLayer(1), 0)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)

	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))

	c.GrowLayersTo(4)

	c.ReplaceLayer(0, []uint64{})
	shuffled := make([]uint64, len(connsSlice1))
	copy(shuffled, connsSlice1)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	for _, item := range shuffled {
		c.InsertAtLayer(item, 0)
	}

	c.ReplaceLayer(2, []uint64{})
	shuffled = make([]uint64, len(connsSlice3))
	copy(shuffled, connsSlice3)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	for _, item := range shuffled {
		c.InsertAtLayer(item, 2)
	}

	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))
	assert.ElementsMatch(t, connsSlice3, c.GetLayer(2))
	assert.ElementsMatch(t, []uint64{}, c.GetLayer(3))
	assert.ElementsMatch(t, []uint64{}, c.GetLayer(4))
}

func randomArray(size int) []uint64 {
	res := make([]uint64, 0, size)
	for i := 0; i < size; i++ {
		res = append(res, uint64(rand.Uint32()/10000))
	}
	return res
}

func TestConnections_stress(t *testing.T) {
	layers := uint8(10)
	c, err := NewWithMaxLayer(layers)
	require.Nil(t, err)

	slices := make([][]uint64, 0, layers+1)
	for i := uint8(0); i <= layers; i++ {
		assert.Equal(t, 0, c.LenAtLayer(i))
		assert.Len(t, c.GetLayer(i), 0)
		slices = append(slices, randomArray(32))
	}

	for i := uint8(0); i <= layers; i++ {
		c.ReplaceLayer(i, slices[i])
	}

	randomArray(32)
	randomArray(32)

	for i := uint8(0); i <= layers; i++ {
		newNumbers := randomArray(5)
		slices[i] = append(slices[i], newNumbers...)
		for j := range newNumbers {
			c.InsertAtLayer(newNumbers[j], i)
		}
	}

	for i := uint8(0); int(i) < len(slices); i++ {
		sort.Slice(slices[i], func(i2, j int) bool {
			return slices[i][i2] < slices[i][j]
		})
		assert.Equal(t, len(slices[i]), c.LenAtLayer(i))
		if !assert.ElementsMatch(t, slices[i], c.GetLayer(i)) {
			return
		}
	}
}

func TestInitialSizeShouldAccommodateLayers(t *testing.T) {
	_, err := NewWithMaxLayer(50)
	require.Nil(t, err)
}

func TestConnections_LayerRange(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	layerCount := 0
	for layer := range c.LayerRange() {
		assert.Equal(t, uint8(layerCount), layer.Index)
		assert.Len(t, layer.Connections, 0)
		layerCount++
	}
	assert.Equal(t, 3, layerCount)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)

	expectedData := [][]uint64{connsSlice1, connsSlice2, connsSlice3}
	layerCount = 0

	for layer := range c.LayerRange() {
		assert.Equal(t, uint8(layerCount), layer.Index)
		assert.ElementsMatch(t, expectedData[layerCount], layer.Connections)
		layerCount++
	}
	assert.Equal(t, 3, layerCount)
}

func TestConnections_LayerRangeWithSingleLayer(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	c.ReplaceLayer(0, connsSlice1)

	layerCount := 0
	for layer := range c.LayerRange() {
		assert.Equal(t, uint8(0), layer.Index)
		assert.ElementsMatch(t, connsSlice1, layer.Connections)
		layerCount++
	}
	assert.Equal(t, 1, layerCount)
}

func TestConnections_LayerRangeAfterAddingLayers(t *testing.T) {
	c, err := NewWithMaxLayer(1)
	require.Nil(t, err)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)

	c.AddLayer()
	c.ReplaceLayer(2, connsSlice3)

	expectedData := [][]uint64{connsSlice1, connsSlice2, connsSlice3}
	layerCount := 0

	for layer := range c.LayerRange() {
		assert.Equal(t, uint8(layerCount), layer.Index)
		assert.ElementsMatch(t, expectedData[layerCount], layer.Connections)
		layerCount++
	}
	assert.Equal(t, 3, layerCount)
}

func TestConnections_LayerRangeAfterGrowingLayers(t *testing.T) {
	c, err := NewWithMaxLayer(1)
	require.Nil(t, err)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)

	c.GrowLayersTo(4)
	c.ReplaceLayer(2, connsSlice3)

	expectedLayers := 4
	layerCount := 0

	for layer := range c.LayerRange() {
		assert.Equal(t, uint8(layerCount), layer.Index)

		switch layerCount {
		case 0:
			assert.ElementsMatch(t, connsSlice1, layer.Connections)
		case 1:
			assert.ElementsMatch(t, connsSlice2, layer.Connections)
		case 2:
			assert.ElementsMatch(t, connsSlice3, layer.Connections)
		case 3, 4:
			assert.Len(t, layer.Connections, 0)
		}
		layerCount++
	}
	assert.Equal(t, expectedLayers, layerCount)
}

func TestConnections_LayerRangeWithDynamicModifications(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)

	layerCount := 0
	for layer := range c.LayerRange() {
		if layerCount == 1 {
			c.ReplaceLayer(2, []uint64{999, 1000, 1001})
		}

		assert.Equal(t, uint8(layerCount), layer.Index)
		assert.True(t, len(layer.Connections) >= 0)
		layerCount++
	}
	assert.Equal(t, 3, layerCount)
}

func TestConnections_LayerRangeCompareWithIterateOnLayers(t *testing.T) {
	c, err := NewWithMaxLayer(3)
	require.Nil(t, err)

	testData := [][]uint64{
		randomArray(10),
		randomArray(15),
		randomArray(8),
		randomArray(12),
	}

	for i, data := range testData {
		c.ReplaceLayer(uint8(i), data)
	}

	rangeResults := make(map[uint8][]uint64)
	for layer := range c.LayerRange() {
		rangeResults[layer.Index] = layer.Connections
	}

	iterateResults := make(map[uint8][]uint64)
	c.IterateOnLayers(func(layer uint8, conns []uint64) {
		iterateResults[layer] = conns
	})

	assert.Equal(t, len(iterateResults), len(rangeResults))
	for layer := uint8(0); layer < c.Layers(); layer++ {
		assert.ElementsMatch(t, iterateResults[layer], rangeResults[layer])
		assert.ElementsMatch(t, testData[layer], rangeResults[layer])
	}
}

func TestConnections_LayerRangeStress(t *testing.T) {
	layers := uint8(20)
	c, err := NewWithMaxLayer(layers)
	require.Nil(t, err)

	testSlices := make([][]uint64, layers+1)
	for i := uint8(0); i <= layers; i++ {
		testSlices[i] = randomArray(50)
		c.ReplaceLayer(i, testSlices[i])
	}

	layerCount := 0
	for layer := range c.LayerRange() {
		assert.Equal(t, uint8(layerCount), layer.Index)
		assert.ElementsMatch(t, testSlices[layerCount], layer.Connections)
		layerCount++
	}

	assert.Equal(t, int(layers)+1, layerCount)
}

func TestConnections_LayerRangeEmptyConnections(t *testing.T) {
	c, err := NewWithMaxLayer(5)
	require.Nil(t, err)

	layerCount := 0
	for layer := range c.LayerRange() {
		assert.Equal(t, uint8(layerCount), layer.Index)
		assert.Len(t, layer.Connections, 0)
		layerCount++
	}
	assert.Equal(t, 6, layerCount)
}

func TestConnections_ElementRange(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	elementCount := 0
	for range c.ElementRange(0) {
		elementCount++
	}
	assert.Equal(t, 0, elementCount)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)

	elementCount = 0
	expectedElements := make([]uint64, len(connsSlice1))
	copy(expectedElements, connsSlice1)
	sort.Slice(expectedElements, func(i, j int) bool { return expectedElements[i] < expectedElements[j] })

	actualElements := make([]uint64, 0)
	for element := range c.ElementRange(0) {
		assert.Equal(t, elementCount, element.Index)
		actualElements = append(actualElements, element.Value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice1), elementCount)
	assert.Equal(t, expectedElements, actualElements)

	elementCount = 0
	expectedElements = make([]uint64, len(connsSlice2))
	copy(expectedElements, connsSlice2)
	sort.Slice(expectedElements, func(i, j int) bool { return expectedElements[i] < expectedElements[j] })

	actualElements = make([]uint64, 0)
	for element := range c.ElementRange(1) {
		assert.Equal(t, elementCount, element.Index)
		actualElements = append(actualElements, element.Value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice2), elementCount)
	assert.Equal(t, expectedElements, actualElements)

	elementCount = 0
	expectedElements = make([]uint64, len(connsSlice3))
	copy(expectedElements, connsSlice3)
	sort.Slice(expectedElements, func(i, j int) bool { return expectedElements[i] < expectedElements[j] })

	actualElements = make([]uint64, 0)
	for element := range c.ElementRange(2) {
		assert.Equal(t, elementCount, element.Index)
		actualElements = append(actualElements, element.Value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice3), elementCount)
	assert.Equal(t, expectedElements, actualElements)
}

func TestConnections_ElementRangeWithSingleLayer(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	c.ReplaceLayer(0, connsSlice1)

	elementCount := 0
	expectedElements := make([]uint64, len(connsSlice1))
	copy(expectedElements, connsSlice1)
	sort.Slice(expectedElements, func(i, j int) bool { return expectedElements[i] < expectedElements[j] })

	actualElements := make([]uint64, 0)
	for element := range c.ElementRange(0) {
		assert.Equal(t, elementCount, element.Index)
		actualElements = append(actualElements, element.Value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice1), elementCount)
	assert.Equal(t, expectedElements, actualElements)
}

func TestConnections_ElementRangeAfterAddingLayers(t *testing.T) {
	c, err := NewWithMaxLayer(1)
	require.Nil(t, err)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)

	c.AddLayer()
	c.ReplaceLayer(2, connsSlice3)

	// Test the newly added layer
	elementCount := 0
	expectedElements := make([]uint64, len(connsSlice3))
	copy(expectedElements, connsSlice3)
	sort.Slice(expectedElements, func(i, j int) bool { return expectedElements[i] < expectedElements[j] })

	actualElements := make([]uint64, 0)
	for element := range c.ElementRange(2) {
		assert.Equal(t, elementCount, element.Index)
		actualElements = append(actualElements, element.Value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice3), elementCount)
	assert.Equal(t, expectedElements, actualElements)
}

func TestConnections_ElementRangeAfterGrowingLayers(t *testing.T) {
	c, err := NewWithMaxLayer(1)
	require.Nil(t, err)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)

	c.GrowLayersTo(4)
	c.ReplaceLayer(2, connsSlice3)

	elementCount := 0
	expectedElements := make([]uint64, len(connsSlice3))
	copy(expectedElements, connsSlice3)
	sort.Slice(expectedElements, func(i, j int) bool { return expectedElements[i] < expectedElements[j] })

	actualElements := make([]uint64, 0)
	for element := range c.ElementRange(2) {
		assert.Equal(t, elementCount, element.Index)
		actualElements = append(actualElements, element.Value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice3), elementCount)
	assert.Equal(t, expectedElements, actualElements)

	elementCount = 0
	for range c.ElementRange(3) {
		elementCount++
	}
	assert.Equal(t, 0, elementCount)

	elementCount = 0
	for range c.ElementRange(4) {
		elementCount++
	}
	assert.Equal(t, 0, elementCount)
}

func TestConnections_ElementRangeWithInvalidLayer(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)

	elementCount := 0
	for range c.ElementRange(5) {
		elementCount++
	}
	assert.Equal(t, 0, elementCount)
}

func TestConnections_ElementRangeCompareWithElementIterator(t *testing.T) {
	c, err := NewWithMaxLayer(3)
	require.Nil(t, err)

	testData := [][]uint64{
		randomArray(10),
		randomArray(15),
		randomArray(8),
		randomArray(12),
	}

	for i, data := range testData {
		c.ReplaceLayer(uint8(i), data)
	}

	for layer := uint8(0); layer < c.Layers(); layer++ {
		rangeResults := make([]struct {
			Index int
			Value uint64
		}, 0)

		for element := range c.ElementRange(layer) {
			rangeResults = append(rangeResults, struct {
				Index int
				Value uint64
			}{
				Index: element.Index,
				Value: element.Value,
			})
		}

		iteratorResults := make([]struct {
			Index int
			Value uint64
		}, 0)

		iter := c.ElementIterator(layer)
		for iter.Next() {
			index, value := iter.Current()
			iteratorResults = append(iteratorResults, struct {
				Index int
				Value uint64
			}{
				Index: index,
				Value: value,
			})
		}

		assert.Equal(t, len(iteratorResults), len(rangeResults))
		for i, rangeResult := range rangeResults {
			assert.Equal(t, iteratorResults[i].Index, rangeResult.Index)
			assert.Equal(t, iteratorResults[i].Value, rangeResult.Value)
		}
	}
}

func TestConnections_ElementRangeStress(t *testing.T) {
	layers := uint8(20)
	c, err := NewWithMaxLayer(layers)
	require.Nil(t, err)

	testSlices := make([][]uint64, layers+1)
	for i := uint8(0); i <= layers; i++ {
		testSlices[i] = randomArray(50)
		c.ReplaceLayer(i, testSlices[i])
	}

	for layer := uint8(0); layer <= layers; layer++ {
		elementCount := 0
		expectedElements := make([]uint64, len(testSlices[layer]))
		copy(expectedElements, testSlices[layer])
		sort.Slice(expectedElements, func(i, j int) bool { return expectedElements[i] < expectedElements[j] })

		actualElements := make([]uint64, 0)
		for element := range c.ElementRange(layer) {
			assert.Equal(t, elementCount, element.Index)
			actualElements = append(actualElements, element.Value)
			elementCount++
		}

		assert.Equal(t, len(testSlices[layer]), elementCount)
		assert.Equal(t, expectedElements, actualElements)
	}
}

func TestConnections_ElementRangeEmptyConnections(t *testing.T) {
	c, err := NewWithMaxLayer(5)
	require.Nil(t, err)

	for layer := uint8(0); layer <= 5; layer++ {
		elementCount := 0
		for range c.ElementRange(layer) {
			elementCount++
		}
		assert.Equal(t, 0, elementCount)
	}
}

func TestConnections_ElementRangeWithInsertions(t *testing.T) {
	c, err := NewWithMaxLayer(1)
	require.Nil(t, err)

	c.ReplaceLayer(0, []uint64{})

	testElements := []uint64{100, 50, 200, 25, 150}
	for _, elem := range testElements {
		c.InsertAtLayer(elem, 0)
	}

	expectedElements := make([]uint64, len(testElements))
	copy(expectedElements, testElements)
	sort.Slice(expectedElements, func(i, j int) bool { return expectedElements[i] < expectedElements[j] })

	elementCount := 0
	actualElements := make([]uint64, 0)
	for element := range c.ElementRange(0) {
		assert.Equal(t, elementCount, element.Index)
		actualElements = append(actualElements, element.Value)
		elementCount++
	}

	assert.Equal(t, len(testElements), elementCount)
	assert.Equal(t, expectedElements, actualElements)
}

func TestConnections_ElementRangeConsistentIndexing(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	testData := []uint64{10, 5, 20, 15, 25}
	c.ReplaceLayer(0, testData)

	expectedIndex := 0
	for element := range c.ElementRange(0) {
		assert.Equal(t, expectedIndex, element.Index)
		expectedIndex++
	}
	assert.Equal(t, len(testData), expectedIndex)
}

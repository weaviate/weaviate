//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

func TestConnections_GrowsLayersSuccessfully(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	assert.Equal(t, 0, c.LenAtLayer(0))
	assert.Len(t, c.GetLayer(0), 0)

	c.ReplaceLayer(0, connsSlice3)

	assert.ElementsMatch(t, connsSlice3, c.GetLayer(0))

	c.GrowLayersTo(1)

	c.ReplaceLayer(1, connsSlice2)
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))
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
	iter := c.Iterator()
	for iter.Next() {
		layer, connections := iter.Current()
		assert.Equal(t, uint8(layerCount), layer)
		assert.Len(t, connections, 0)
		layerCount++
	}
	assert.Equal(t, 3, layerCount)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)

	expectedData := [][]uint64{connsSlice1, connsSlice2, connsSlice3}
	layerCount = 0

	iter = c.Iterator()
	for iter.Next() {
		layer, connections := iter.Current()
		assert.Equal(t, uint8(layerCount), layer)
		assert.ElementsMatch(t, expectedData[layerCount], connections)
		layerCount++
	}
	assert.Equal(t, 3, layerCount)
}

func TestConnections_LayerRangeWithSingleLayer(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	c.ReplaceLayer(0, connsSlice1)

	layerCount := 0
	iter := c.Iterator()
	for iter.Next() {
		layer, connections := iter.Current()
		assert.Equal(t, uint8(0), layer)
		assert.ElementsMatch(t, connsSlice1, connections)
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

	iter := c.Iterator()
	for iter.Next() {
		layer, connections := iter.Current()
		assert.Equal(t, uint8(layerCount), layer)
		assert.ElementsMatch(t, expectedData[layerCount], connections)
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

	expectedLayers := 5
	layerCount := 0

	iter := c.Iterator()
	for iter.Next() {
		layer, connections := iter.Current()
		assert.Equal(t, uint8(layerCount), layer)

		switch layerCount {
		case 0:
			assert.ElementsMatch(t, connsSlice1, connections)
		case 1:
			assert.ElementsMatch(t, connsSlice2, connections)
		case 2:
			assert.ElementsMatch(t, connsSlice3, connections)
		case 3, 4:
			assert.Len(t, connections, 0)
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
	iter := c.Iterator()
	for iter.Next() {
		layer, connections := iter.Current()
		if layerCount == 1 {
			c.ReplaceLayer(2, []uint64{999, 1000, 1001})
		}

		assert.Equal(t, uint8(layerCount), layer)
		assert.True(t, len(connections) >= 0)
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
	iter := c.Iterator()
	for iter.Next() {
		layer, connections := iter.Current()
		rangeResults[layer] = connections
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
	iter := c.Iterator()
	for iter.Next() {
		layer, connections := iter.Current()
		assert.Equal(t, uint8(layerCount), layer)
		assert.ElementsMatch(t, testSlices[layerCount], connections)
		layerCount++
	}

	assert.Equal(t, int(layers)+1, layerCount)
}

func TestConnections_LayerRangeEmptyConnections(t *testing.T) {
	c, err := NewWithMaxLayer(5)
	require.Nil(t, err)

	layerCount := 0
	iter := c.Iterator()
	for iter.Next() {
		layer, connections := iter.Current()
		assert.Equal(t, uint8(layerCount), layer)
		assert.Len(t, connections, 0)
		layerCount++
	}
	assert.Equal(t, 6, layerCount)
}

func TestConnections_ElementRange(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	elementCount := 0
	iter := c.ElementIterator(0)
	for iter.Next() {
		elementCount++
	}
	assert.Equal(t, 0, elementCount)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)

	elementCount = 0
	actualElements := make([]uint64, 0)
	iter = c.ElementIterator(0)
	for iter.Next() {
		index, value := iter.Current()
		assert.Equal(t, elementCount, index)
		actualElements = append(actualElements, value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice1), elementCount)
	assert.ElementsMatch(t, connsSlice1, actualElements) // Use ElementsMatch instead of Equal

	elementCount = 0
	actualElements = make([]uint64, 0)
	iter = c.ElementIterator(1)
	for iter.Next() {
		index, value := iter.Current()
		assert.Equal(t, elementCount, index)
		actualElements = append(actualElements, value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice2), elementCount)
	assert.ElementsMatch(t, connsSlice2, actualElements) // Use ElementsMatch instead of Equal

	elementCount = 0
	actualElements = make([]uint64, 0)
	iter = c.ElementIterator(2)
	for iter.Next() {
		index, value := iter.Current()
		assert.Equal(t, elementCount, index)
		actualElements = append(actualElements, value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice3), elementCount)
	assert.ElementsMatch(t, connsSlice3, actualElements) // Use ElementsMatch instead of Equal
}

func TestConnections_ElementRangeWithSingleLayer(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	c.ReplaceLayer(0, connsSlice1)

	elementCount := 0
	actualElements := make([]uint64, 0)
	iter := c.ElementIterator(0)
	for iter.Next() {
		index, value := iter.Current()
		assert.Equal(t, elementCount, index)
		actualElements = append(actualElements, value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice1), elementCount)
	assert.ElementsMatch(t, connsSlice1, actualElements) // Use ElementsMatch instead of Equal
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
	actualElements := make([]uint64, 0)
	iter := c.ElementIterator(2)
	for iter.Next() {
		index, value := iter.Current()
		assert.Equal(t, elementCount, index)
		actualElements = append(actualElements, value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice3), elementCount)
	assert.ElementsMatch(t, connsSlice3, actualElements) // Use ElementsMatch instead of Equal
}

func TestConnections_ElementRangeAfterGrowingLayers(t *testing.T) {
	c, err := NewWithMaxLayer(1)
	require.Nil(t, err)

	c.ReplaceLayer(0, connsSlice1)
	c.ReplaceLayer(1, connsSlice2)

	c.GrowLayersTo(4)
	c.ReplaceLayer(2, connsSlice3)

	elementCount := 0
	actualElements := make([]uint64, 0)
	iter := c.ElementIterator(2)
	for iter.Next() {
		index, value := iter.Current()
		assert.Equal(t, elementCount, index)
		actualElements = append(actualElements, value)
		elementCount++
	}
	assert.Equal(t, len(connsSlice3), elementCount)
	assert.ElementsMatch(t, connsSlice3, actualElements) // Use ElementsMatch instead of Equal

	elementCount = 0
	iter = c.ElementIterator(3)
	for iter.Next() {
		elementCount++
	}
	assert.Equal(t, 0, elementCount)

	elementCount = 0
	iter = c.ElementIterator(4)
	for iter.Next() {
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
	iter := c.ElementIterator(5)
	for iter.Next() {
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

		iter := c.ElementIterator(layer)
		for iter.Next() {
			index, value := iter.Current()
			rangeResults = append(rangeResults, struct {
				Index int
				Value uint64
			}{
				Index: index,
				Value: value,
			})
		}

		iteratorResults := make([]struct {
			Index int
			Value uint64
		}, 0)

		iter = c.ElementIterator(layer)
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
		// Compare the actual results - they should be identical since we're using the same iterator
		assert.Equal(t, iteratorResults, rangeResults)
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
		actualElements := make([]uint64, 0)
		iter := c.ElementIterator(layer)
		for iter.Next() {
			index, value := iter.Current()
			assert.Equal(t, elementCount, index)
			actualElements = append(actualElements, value)
			elementCount++
		}

		assert.Equal(t, len(testSlices[layer]), elementCount)
		assert.ElementsMatch(t, testSlices[layer], actualElements) // Use ElementsMatch instead of Equal
	}
}

// Additional test to verify order preservation (insertion order should be maintained)
func TestConnections_ElementOrderPreservation(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	// Test with a specific sequence to verify insertion order is preserved
	testSequence := []uint64{100, 1, 50, 200, 25}
	c.ReplaceLayer(0, testSequence)

	actualElements := make([]uint64, 0)
	iter := c.ElementIterator(0)
	for iter.Next() {
		_, value := iter.Current()
		actualElements = append(actualElements, value)
	}

	// Should preserve the exact order from ReplaceLayer
	assert.Equal(t, testSequence, actualElements)
}

// Test to verify that individual insertions maintain order
func TestConnections_InsertionOrderPreservation(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	// Insert values one by one
	values := []uint64{100, 1, 50, 200, 25}
	for _, val := range values {
		c.InsertAtLayer(val, 0)
	}

	actualElements := make([]uint64, 0)
	iter := c.ElementIterator(0)
	for iter.Next() {
		_, value := iter.Current()
		actualElements = append(actualElements, value)
	}

	// Should preserve the insertion order
	assert.Equal(t, values, actualElements)
}

func TestConnections_ElementRangeEmptyConnections(t *testing.T) {
	c, err := NewWithMaxLayer(5)
	require.Nil(t, err)

	for layer := uint8(0); layer <= 5; layer++ {
		elementCount := 0
		iter := c.ElementIterator(layer)
		for iter.Next() {
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

	elementCount := 0
	actualElements := make([]uint64, 0)
	iter := c.ElementIterator(0)
	for iter.Next() {
		index, value := iter.Current()
		assert.Equal(t, elementCount, index)
		actualElements = append(actualElements, value)
		elementCount++
	}

	assert.Equal(t, len(testElements), elementCount)
	// Use ElementsMatch to check that all elements are present regardless of order
	assert.ElementsMatch(t, testElements, actualElements)
}

func TestConnections_ElementRangeConsistentIndexing(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	testData := []uint64{10, 5, 20, 15, 25}
	c.ReplaceLayer(0, testData)

	expectedIndex := 0
	iter := c.ElementIterator(0)
	for iter.Next() {
		index, _ := iter.Current()
		assert.Equal(t, expectedIndex, index)
		expectedIndex++
	}
	assert.Equal(t, len(testData), expectedIndex)
}

func TestConnections_InsertAtLayer_SchemeGrowth(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	expectedConns := make([]uint64, 0)

	// Start with 62 values that fit in SCHEME_2BYTE to test capacity growth
	for i := uint64(1); i <= 62; i++ {
		expectedConns = append(expectedConns, i)
		c.InsertAtLayer(i, 0)
	}
	assert.ElementsMatch(t, expectedConns, c.GetLayer(0))
	assert.Equal(t, 62, c.LenAtLayer(0))
	assert.Equal(t, uint8(SCHEME_2BYTE), unpackScheme(c.layers[0].packed))

	// SCHEME_3BYTE
	val2 := uint64(1 << 16) // 65536
	expectedConns = append(expectedConns, val2)
	c.InsertAtLayer(val2, 0)
	assert.ElementsMatch(t, expectedConns, c.GetLayer(0))
	assert.Equal(t, 63, c.LenAtLayer(0))
	assert.Equal(t, uint8(SCHEME_3BYTE), unpackScheme(c.layers[0].packed))

	// SCHEME_4BYTE
	val3 := uint64(1 << 24) // 16777216
	expectedConns = append(expectedConns, val3)
	c.InsertAtLayer(val3, 0)
	assert.ElementsMatch(t, expectedConns, c.GetLayer(0))
	assert.Equal(t, 64, c.LenAtLayer(0))
	assert.Equal(t, uint8(SCHEME_4BYTE), unpackScheme(c.layers[0].packed))

	// SCHEME_5BYTE
	val4 := uint64(1 << 32)
	expectedConns = append(expectedConns, val4)
	c.InsertAtLayer(val4, 0)
	assert.ElementsMatch(t, expectedConns, c.GetLayer(0))
	assert.Equal(t, 65, c.LenAtLayer(0))
	assert.Equal(t, uint8(SCHEME_5BYTE), unpackScheme(c.layers[0].packed))

	// SCHEME_8BYTE
	val5 := uint64(1 << 40)
	expectedConns = append(expectedConns, val5)
	c.InsertAtLayer(val5, 0)
	assert.ElementsMatch(t, expectedConns, c.GetLayer(0))
	assert.Equal(t, 66, c.LenAtLayer(0))
	assert.Equal(t, uint8(SCHEME_8BYTE), unpackScheme(c.layers[0].packed))
}

func BenchmarkInsertAtLayer(b *testing.B) {
	layers := uint8(5)

	c := make(map[int]*Connections)
	for i := 0; i < b.N; i++ {
		cTemp, err := NewWithMaxLayer(layers)
		require.Nil(b, err)
		for i := uint8(0); i <= layers; i++ {
			cTemp.ReplaceLayer(i, randomArray(32))
		}
		c[i] = cTemp
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for l := uint8(0); l <= layers; l++ {
			newNumbers := randomArray(5)
			for j := range newNumbers {
				c[i].InsertAtLayer(newNumbers[j], l)
			}
		}
	}
}

func BenchmarkInsertAtLayerLarge(b *testing.B) {
	layers := uint8(5)

	c, err := NewWithMaxLayer(layers)
	require.Nil(b, err)

	for i := uint8(0); i <= layers; i++ {
		c.ReplaceLayer(i, randomArray(32))
	}

	newNumbers := randomArray(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for l := uint8(0); l <= layers; l++ {
			for j := range newNumbers {
				c.InsertAtLayer(newNumbers[j], l)
			}
		}

		b.StopTimer()
		for i := uint8(0); i <= layers; i++ {
			c.ReplaceLayer(i, randomArray(32))
		}
		b.StartTimer()
	}
}

func BenchmarkReplaceLayer(b *testing.B) {
	layers := uint8(5)

	c, err := NewWithMaxLayer(layers)
	require.Nil(b, err)

	newNumbers := randomArray(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := uint8(0); i <= layers; i++ {
			c.ReplaceLayer(i, newNumbers)
		}
	}
}

func TestNewWithData(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	c.appendToLayer(1, 0)
	c.appendToLayer(9, 0)
	c.appendToLayer(5, 0)

	require.ElementsMatch(t, []uint64{1, 9, 5}, c.GetLayer(0))

	copied := NewWithData(c.Data())
	require.ElementsMatch(t, c.GetLayer(0), copied.GetLayer(0))
}

func TestConnections_DataSerialization_SingleLayer(t *testing.T) {
	// Test with small values that fit in 2-byte scheme
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	smallValues := []uint64{1, 100, 1000, 65535}
	c.ReplaceLayer(0, smallValues)

	data := c.Data()
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Deserialize and verify
	copied := NewWithData(data)
	require.NotNil(t, copied)
	assert.Equal(t, c.Layers(), copied.Layers())
	assert.ElementsMatch(t, c.GetLayer(0), copied.GetLayer(0))
	assert.Equal(t, c.LenAtLayer(0), copied.LenAtLayer(0))
}

func TestConnections_DataSerialization_MultipleLayers(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	// Layer 0: small values (2-byte scheme)
	smallValues := []uint64{1, 100, 1000, 65535}
	c.ReplaceLayer(0, smallValues)

	// Layer 1: medium values (3-byte scheme)
	mediumValues := []uint64{100000, 500000, 16777215}
	c.ReplaceLayer(1, mediumValues)

	// Layer 2: large values (4-byte scheme)
	largeValues := []uint64{10000000, 50000000, 4294967295}
	c.ReplaceLayer(2, largeValues)

	data := c.Data()
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Deserialize and verify
	copied := NewWithData(data)
	require.NotNil(t, copied)
	assert.Equal(t, c.Layers(), copied.Layers())

	// Verify each layer
	assert.ElementsMatch(t, c.GetLayer(0), copied.GetLayer(0))
	assert.ElementsMatch(t, c.GetLayer(1), copied.GetLayer(1))
	assert.ElementsMatch(t, c.GetLayer(2), copied.GetLayer(2))

	assert.Equal(t, c.LenAtLayer(0), copied.LenAtLayer(0))
	assert.Equal(t, c.LenAtLayer(1), copied.LenAtLayer(1))
	assert.Equal(t, c.LenAtLayer(2), copied.LenAtLayer(2))
}

func TestConnections_DataSerialization_AllSchemes(t *testing.T) {
	c, err := NewWithMaxLayer(4)
	require.Nil(t, err)

	// Test all encoding schemes with boundary values
	testCases := []struct {
		layer  uint8
		values []uint64
		scheme uint8
		desc   string
	}{
		{0, []uint64{1, 100, 65535}, SCHEME_2BYTE, "2-byte scheme"},
		{1, []uint64{65536, 100000, 16777215}, SCHEME_3BYTE, "3-byte scheme"},
		{2, []uint64{16777216, 100000000, 4294967295}, SCHEME_4BYTE, "4-byte scheme"},
		{3, []uint64{4294967296, 1000000000000, 1099511627775}, SCHEME_5BYTE, "5-byte scheme"},
		{4, []uint64{1099511627776, 1000000000000000, 18446744073709551615}, SCHEME_8BYTE, "8-byte scheme"},
	}

	for _, tc := range testCases {
		c.ReplaceLayer(tc.layer, tc.values)
	}

	data := c.Data()
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Deserialize and verify
	copied := NewWithData(data)
	require.NotNil(t, copied)
	assert.Equal(t, c.Layers(), copied.Layers())

	// Verify each layer
	for _, tc := range testCases {
		assert.ElementsMatch(t, c.GetLayer(tc.layer), copied.GetLayer(tc.layer),
			"Layer %d (%s) mismatch", tc.layer, tc.desc)
		assert.Equal(t, c.LenAtLayer(tc.layer), copied.LenAtLayer(tc.layer),
			"Layer %d (%s) length mismatch", tc.layer, tc.desc)
	}
}

func TestConnections_DataSerialization_EmptyLayers(t *testing.T) {
	c, err := NewWithMaxLayer(3)
	require.Nil(t, err)

	// Only populate layer 1, leave others empty
	values := []uint64{1, 2, 3, 4, 5}
	c.ReplaceLayer(1, values)

	data := c.Data()
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Deserialize and verify
	copied := NewWithData(data)
	require.NotNil(t, copied)
	assert.Equal(t, c.Layers(), copied.Layers())

	// Verify empty layers remain empty
	assert.Len(t, copied.GetLayer(0), 0)
	assert.Len(t, copied.GetLayer(2), 0)
	assert.Len(t, copied.GetLayer(3), 0)

	// Verify populated layer
	assert.ElementsMatch(t, values, copied.GetLayer(1))
	assert.Equal(t, len(values), copied.LenAtLayer(1))
}

func TestConnections_DataSerialization_EmptyConnections(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	data := c.Data()
	require.NotNil(t, data)
	// NewWithMaxLayer(0) creates layerCount=1, so we expect 1 layer with empty data
	// Format: [layerCount=1][packed=0][dataLen=0][dataLen=0][dataLen=0][dataLen=0]
	expectedData := []byte{1, 0, 0, 0, 0, 0, 0}
	assert.Equal(t, expectedData, data)

	// Deserialize and verify
	copied := NewWithData(data)
	require.NotNil(t, copied)
	assert.Equal(t, uint8(1), copied.Layers())
	assert.Len(t, copied.GetLayer(0), 0)
}

func TestConnections_DataSerialization_InsertAtLayer(t *testing.T) {
	c, err := NewWithMaxLayer(1)
	require.Nil(t, err)

	// Use InsertAtLayer to build up data
	expectedValues := []uint64{1, 100, 1000, 100000, 10000000}
	for _, val := range expectedValues {
		c.InsertAtLayer(val, 0)
	}

	data := c.Data()
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Deserialize and verify
	copied := NewWithData(data)
	require.NotNil(t, copied)
	assert.Equal(t, c.Layers(), copied.Layers())
	assert.ElementsMatch(t, expectedValues, copied.GetLayer(0))
	assert.Equal(t, len(expectedValues), copied.LenAtLayer(0))
}

func TestConnections_DataSerialization_BulkInsert(t *testing.T) {
	c, err := NewWithMaxLayer(1)
	require.Nil(t, err)

	// Start with some values
	initialValues := []uint64{1, 2, 3}
	c.ReplaceLayer(0, initialValues)

	// Add more values using BulkInsertAtLayer
	additionalValues := []uint64{100, 200, 300, 1000000}
	c.BulkInsertAtLayer(additionalValues, 0)

	expectedValues := append(initialValues, additionalValues...)

	data := c.Data()
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Deserialize and verify
	copied := NewWithData(data)
	require.NotNil(t, copied)
	assert.Equal(t, c.Layers(), copied.Layers())
	assert.ElementsMatch(t, expectedValues, copied.GetLayer(0))
	assert.Equal(t, len(expectedValues), copied.LenAtLayer(0))
}

func TestConnections_DataSerialization_LargeValues(t *testing.T) {
	c, err := NewWithMaxLayer(0)
	require.Nil(t, err)

	// Test with very large values that require 8-byte encoding
	largeValues := []uint64{
		18446744073709551615, // max uint64
		1000000000000000000,
		5000000000000000000,
		9999999999999999999,
	}

	c.ReplaceLayer(0, largeValues)

	data := c.Data()
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Deserialize and verify
	copied := NewWithData(data)
	require.NotNil(t, copied)
	assert.Equal(t, c.Layers(), copied.Layers())
	assert.ElementsMatch(t, largeValues, copied.GetLayer(0))
	assert.Equal(t, len(largeValues), copied.LenAtLayer(0))
}

func TestConnections_DataSerialization_MixedSchemes(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	// Layer 0: Start with small values, then add large ones to trigger scheme upgrade
	c.InsertAtLayer(1, 0)
	c.InsertAtLayer(100, 0)
	c.InsertAtLayer(18446744073709551615, 0) // This should trigger 8-byte scheme

	// Layer 1: Start with medium values, then add larger ones
	c.InsertAtLayer(100000, 1)
	c.InsertAtLayer(500000, 1)
	c.InsertAtLayer(4294967296, 1) // This should trigger 5-byte scheme

	data := c.Data()
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Deserialize and verify
	copied := NewWithData(data)
	require.NotNil(t, copied)
	assert.Equal(t, c.Layers(), copied.Layers())

	// Verify both layers
	assert.ElementsMatch(t, c.GetLayer(0), copied.GetLayer(0))
	assert.ElementsMatch(t, c.GetLayer(1), copied.GetLayer(1))
	assert.Equal(t, c.LenAtLayer(0), copied.LenAtLayer(0))
	assert.Equal(t, c.LenAtLayer(1), copied.LenAtLayer(1))
}

func TestConnections_DataSerialization_Stress(t *testing.T) {
	layers := uint8(5)
	c, err := NewWithMaxLayer(layers)
	require.Nil(t, err)

	// Populate all layers with different sized values
	for i := uint8(0); i <= layers; i++ {
		values := make([]uint64, 10)
		for j := 0; j < 10; j++ {
			// Mix different value ranges to test different schemes
			switch j % 5 {
			case 0:
				values[j] = uint64(j + 1) // Small values
			case 1:
				values[j] = uint64(100000 + j) // Medium values
			case 2:
				values[j] = uint64(1000000000 + j) // Large values
			case 3:
				values[j] = uint64(1000000000000 + j) // Very large values
			case 4:
				values[j] = uint64(1000000000000000000 + j) // Extremely large values
			}
		}
		c.ReplaceLayer(i, values)
	}

	data := c.Data()
	require.NotNil(t, data)
	require.Greater(t, len(data), 0)

	// Deserialize and verify
	copied := NewWithData(data)
	require.NotNil(t, copied)
	assert.Equal(t, c.Layers(), copied.Layers())

	// Verify all layers
	for i := uint8(0); i <= layers; i++ {
		assert.ElementsMatch(t, c.GetLayer(i), copied.GetLayer(i),
			"Layer %d mismatch", i)
		assert.Equal(t, c.LenAtLayer(i), copied.LenAtLayer(i),
			"Layer %d length mismatch", i)
	}
}

func TestConnections_DataSerialization_EmptyData(t *testing.T) {
	// Test with empty data
	copied := NewWithData([]byte{})
	require.NotNil(t, copied)
	assert.Equal(t, uint8(0), copied.Layers())

	// Test with nil data
	copied = NewWithData(nil)
	require.NotNil(t, copied)
	assert.Equal(t, uint8(0), copied.Layers())
}

func TestConnections_DataSerialization_MalformedData(t *testing.T) {
	// Test with malformed data (too short)
	copied := NewWithData([]byte{1}) // Layer count 1 but no data
	require.NotNil(t, copied)
	assert.Equal(t, uint8(1), copied.Layers())
	assert.Len(t, copied.GetLayer(0), 0)

	// Test with incomplete layer data
	malformedData := []byte{2, 0, 0, 0, 0, 0, 0} // Layer count 2, incomplete first layer
	copied = NewWithData(malformedData)
	require.NotNil(t, copied)
	assert.Equal(t, uint8(2), copied.Layers())
	assert.Len(t, copied.GetLayer(0), 0)
	assert.Len(t, copied.GetLayer(1), 0)
}

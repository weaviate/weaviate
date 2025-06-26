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
	"fmt"
	"math"
)

const (
	InitialCapacity    = 16
	DefaultMaxCapacity = 64
	// Simple encoding schemes - trade some compression for speed
	SCHEME_2BYTE = 0 // 2 bytes per value (0-65535)
	SCHEME_3BYTE = 1 // 3 bytes per value (0-16777215)
	SCHEME_4BYTE = 2 // 4 bytes per value (0-4294967295)
	SCHEME_5BYTE = 3 // 5 bytes per value (0-1099511627775)
	SCHEME_8BYTE = 4 // 8 bytes per value (full uint64)
)

type Connections struct {
	layers     []*CompressedUint64List
	layerCount uint8
}

func NewWithMaxLayer(maxLayer uint8) (*Connections, error) {
	if int(maxLayer)+1 > math.MaxUint8 {
		return nil, fmt.Errorf("max supported layer is %d", math.MaxUint8-1)
	}

	layerCount := maxLayer + 1
	c := &Connections{
		layers:     make([]*CompressedUint64List, layerCount),
		layerCount: layerCount,
	}
	for i := uint8(0); i < layerCount; i++ {
		c.layers[i] = NewCompressedUint64List()
	}
	return c, nil
}

func NewWithData(data []byte) *Connections {
	if len(data) == 0 {
		return &Connections{
			layers:     make([]*CompressedUint64List, 0),
			layerCount: 0,
		}
	}

	offset := 0
	layerCount := data[offset]
	offset++

	c := &Connections{
		layers:     make([]*CompressedUint64List, layerCount),
		layerCount: layerCount,
	}

	for i := uint8(0); i < layerCount; i++ {
		if offset+10 > len(data) {
			break
		}
		readThisRound := c.layers[i].Restore(data[offset:])
		offset += readThisRound
	}

	return c
}

func NewWithElements(elements [][]uint64) (*Connections, error) {
	c, err := NewWithMaxLayer(uint8(len(elements)) - 1)
	if err != nil {
		return nil, err
	}

	for index, conns := range elements {
		c.ReplaceLayer(uint8(index), conns)
	}
	return c, nil
}

func (c *Connections) AddLayer() {
	c.layerCount++
	c.layers = append(c.layers, &CompressedUint64List{})
}

func (c *Connections) GrowLayersTo(newLayers uint8) {
	targetCount := newLayers + 1
	if targetCount <= c.layerCount {
		return
	}

	// Optimize for the common case: 1 layer
	if c.layerCount == 0 && targetCount == 1 {
		c.layers = make([]*CompressedUint64List, 1)
		c.layerCount = 1
		return
	}

	for c.layerCount < targetCount {
		c.AddLayer()
	}
}

func (c *Connections) ReplaceLayer(layer uint8, conns []uint64) {
	if layer >= c.layerCount {
		c.GrowLayersTo(layer)
	}

	if len(conns) == 0 {
		c.layers[layer] = &CompressedUint64List{}
		return
	}

	c.layers[layer] = NewCompressedUint64List()
	c.layers[layer].EncodeValues(conns)
}

func (c *Connections) InsertAtLayer(conn uint64, layer uint8) {
	if layer >= c.layerCount {
		c.GrowLayersTo(layer)
	}
	c.layers[layer].Append(conn)
}

func (c *Connections) BulkInsertAtLayer(conns []uint64, layer uint8) {
	if layer >= c.layerCount {
		c.GrowLayersTo(layer)
	}

	if len(conns) == 0 {
		return
	}

	c.layers[layer].EncodeValues(conns)
}

func (c *Connections) Data() []byte {
	if c.layerCount == 0 {
		return []byte{0}
	}

	// Calculate total size
	totalSize := uint32(1) // layer count
	for i := uint8(0); i < c.layerCount; i++ {
		totalSize += c.layers[i].DataLen() // data
	}

	data := make([]byte, totalSize)
	offset := 0

	data[offset] = c.layerCount
	offset++

	for i := uint8(0); i < c.layerCount; i++ {
		layer := c.layers[i]

		layer.WriteData(data[offset:])
		offset += int(layer.DataLen())
	}

	return data
}

func (c *Connections) LenAtLayer(layer uint8) int {
	if layer >= c.layerCount {
		return 0
	}
	return c.layers[layer].Count()
}

func (c *Connections) GetLayer(layer uint8) []uint64 {
	if layer >= c.layerCount || c.layers[layer] == nil {
		return nil
	}

	return c.layers[layer].DecodeValues()
}

func (c *Connections) CopyLayer(conns []uint64, layer uint8) []uint64 {
	if layer >= c.layerCount || c.layers[layer] == nil {
		return nil
	}

	return c.layers[layer].CopyValues(conns)
}

func (c *Connections) Layers() uint8 {
	return c.layerCount
}

func (c *Connections) IterateOnLayers(f func(layer uint8, conns []uint64)) {
	for layer := uint8(0); layer < c.layerCount; layer++ {
		conns := c.GetLayer(layer)
		f(layer, conns)
	}
}

func (c *Connections) GetAllLayers() [][]uint64 {
	result := make([][]uint64, c.layerCount)
	for i := uint8(0); i < c.layerCount; i++ {
		result[i] = c.GetLayer(i)
	}
	return result
}

type LayerIterator struct {
	connections  *Connections
	currentLayer uint8
	maxLayers    uint8
}

func (c *Connections) Iterator() *LayerIterator {
	return &LayerIterator{
		connections:  c,
		currentLayer: 0,
		maxLayers:    c.layerCount,
	}
}

func (iter *LayerIterator) Next() bool {
	return iter.currentLayer < iter.maxLayers
}

func (iter *LayerIterator) Current() (uint8, []uint64) {
	if iter.currentLayer >= iter.maxLayers {
		return 0, nil
	}

	index := iter.currentLayer
	connections := iter.connections.GetLayer(index)
	iter.currentLayer++

	return index, connections
}

func (iter *LayerIterator) Reset() {
	iter.currentLayer = 0
}

type LayerElementIterator struct {
	connections *Connections
	layer       uint8
	index       int
	maxIndex    int
	values      []uint64
}

func (c *Connections) ElementIterator(layer uint8) *LayerElementIterator {
	maxIndex := 0
	var values []uint64

	if layer < c.layerCount && c.layers[layer] != nil {
		maxIndex = c.layers[layer].Count()
		values = c.layers[layer].DecodeValues()
	}

	return &LayerElementIterator{
		connections: c,
		layer:       layer,
		index:       0,
		maxIndex:    maxIndex,
		values:      values,
	}
}

func (iter *LayerElementIterator) Next() bool {
	if iter.index >= iter.maxIndex {
		return false
	}
	iter.index++
	return true
}

func (iter *LayerElementIterator) Current() (index int, value uint64) {
	if iter.index <= 0 || iter.index > iter.maxIndex {
		return -1, 0
	}

	currentIndex := iter.index - 1
	value = iter.values[currentIndex]

	return currentIndex, value
}

func (iter *LayerElementIterator) Value() uint64 {
	_, value := iter.Current()
	return value
}

func (iter *LayerElementIterator) Index() int {
	return iter.index - 1
}

func (iter *LayerElementIterator) Reset() {
	iter.index = 0
}

func (iter *LayerElementIterator) HasElements() bool {
	return iter.maxIndex > 0
}

func (iter *LayerElementIterator) Count() int {
	return iter.maxIndex
}

func (c *Connections) ClearLayer(layer uint8) {
	if layer < c.layerCount {
		c.layers[layer] = &CompressedUint64List{}
	}
}

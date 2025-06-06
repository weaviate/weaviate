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
	"fmt"
	"math"
)

type Connections struct {
	maxLayer  uint8
	topLayers [][]uint64
	layerZero *PrefixEncoder
}

func NewWithMaxLayer(maxLayer uint8) (*Connections, error) {
	if int(maxLayer)+1 > math.MaxUint8 {
		return nil, fmt.Errorf("max supported layer is %d",
			math.MaxUint8-1)
	}
	return &Connections{
		maxLayer:  maxLayer,
		topLayers: make([][]uint64, maxLayer),
		layerZero: NewPrefixEncoder(),
	}, nil
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
	c.topLayers = append(c.topLayers, nil)
	c.maxLayer++
}

func (c *Connections) GrowLayersTo(newLayers uint8) {
	if cap(c.topLayers) < int(newLayers) {
		tempLayers := c.topLayers
		c.topLayers = make([][]uint64, newLayers)
		copy(c.topLayers, tempLayers)
	} else {
		c.topLayers = c.topLayers[:newLayers]
	}
	c.maxLayer = newLayers
}

func (c *Connections) ReplaceLayerWithBuffer(layer uint8, conns []uint64, buff []byte) {
	if layer == 0 {
		c.layerZero.InitWithBuff(conns, buff)
	} else {
		c.topLayers[layer-1] = conns
	}
}

func (c *Connections) ReplaceLayer(layer uint8, conns []uint64) {
	if layer == 0 {
		c.layerZero.Init(conns)
	} else {
		c.topLayers[layer-1] = conns
	}
}

func (c Connections) LenAtLayer(layer uint8) int {
	if layer > c.maxLayer {
		panic(fmt.Sprintf("only has %d layers", c.Layers()))
	}

	if layer == 0 {
		return c.layerZero.Len()
	}
	return len(c.topLayers[layer-1])
}

// Returns the underlying data buffer. Do not modify the contents
// of the buffer or call this method concurrently.
func (c *Connections) Data() []byte {
	//return c.data
	return nil
}

func (c Connections) CopyLayer(conns []uint64, layer uint8) []uint64 {
	if layer >= c.Layers() {
		return nil
	}

	if layer == 0 {
		return c.layerZero.Decode(conns)
	}

	if cap(conns) > len(c.topLayers[layer-1]) {
		conns = conns[:len(c.topLayers[layer-1])]
	} else {
		conns = make([]uint64, len(c.topLayers[layer-1]))
	}
	copy(conns, c.topLayers[layer-1])
	return conns
}

func (c Connections) GetLayer(layer uint8) []uint64 {
	return c.CopyLayer(nil, layer)
}

func (c *Connections) InsertAtLayer(conn uint64, layer uint8) {
	if layer >= c.Layers() {
		panic(fmt.Sprintf("only has %d layers", c.Layers()))
	}

	if layer == 0 {
		c.layerZero.Add(conn)
	} else {
		c.topLayers[layer-1] = append(c.topLayers[layer-1], conn)
	}
}

func (c *Connections) Layers() uint8 {
	return c.maxLayer + 1
}

func (c *Connections) IterateOnLayers(f func(layer uint8, conns []uint64)) {
	for layer := uint8(0); layer < c.Layers(); layer++ {
		conns := c.GetLayer(layer)
		f(layer, conns)
	}
}

func (c *Connections) GetAllLayers() [][]uint64 {
	layers := c.Layers()
	result := make([][]uint64, layers)

	for i := uint8(0); i < layers; i++ {
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
		maxLayers:    c.Layers(),
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

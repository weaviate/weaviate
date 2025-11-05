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

type LayerData struct {
	data []byte
	// Packed scheme (4 bits) and count (12 bits)
	// scheme is in lower 4 bits, count in upper 12 bits
	packed uint16
}

type Connections struct {
	layers     []LayerData
	layerCount uint8
}

func NewWithMaxLayer(maxLayer uint8) (*Connections, error) {
	if int(maxLayer)+1 > math.MaxUint8 {
		return nil, fmt.Errorf("max supported layer is %d", math.MaxUint8-1)
	}

	layerCount := maxLayer + 1
	c := &Connections{
		layers:     make([]LayerData, layerCount),
		layerCount: layerCount,
	}
	for i := uint8(0); i < layerCount; i++ {
		c.layers[i].data = make([]byte, 0, InitialCapacity)
	}
	return c, nil
}

func NewWithData(data []byte) *Connections {
	if len(data) == 0 {
		return &Connections{
			layers:     make([]LayerData, 0),
			layerCount: 0,
		}
	}

	offset := 0
	layerCount := data[offset]
	offset++

	c := &Connections{
		layers:     make([]LayerData, layerCount),
		layerCount: layerCount,
	}

	for i := uint8(0); i < layerCount; i++ {
		if offset+6 > len(data) { // 2 for packed, 4 for dataLen
			break // Malformed data
		}

		// Read packed (2 bytes, little endian)
		packed := uint16(data[offset]) | uint16(data[offset+1])<<8
		offset += 2

		// Read data length (4 bytes, little endian)
		dataLen := uint32(data[offset]) |
			uint32(data[offset+1])<<8 |
			uint32(data[offset+2])<<16 |
			uint32(data[offset+3])<<24
		offset += 4

		if offset+int(dataLen) > len(data) {
			break // Malformed data
		}

		layerData := make([]byte, dataLen)
		copy(layerData, data[offset:offset+int(dataLen)])
		offset += int(dataLen)

		c.layers[i] = LayerData{
			data:   layerData,
			packed: packed,
		}
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
	c.layers = append(c.layers, LayerData{})
}

func (c *Connections) GrowLayersTo(newLayers uint8) {
	targetCount := newLayers + 1
	if targetCount <= c.layerCount {
		return
	}

	// Optimize for the common case: 1 layer
	if c.layerCount == 0 && targetCount == 1 {
		c.layers = make([]LayerData, 1)
		c.layerCount = 1
		return
	}

	for c.layerCount < targetCount {
		c.AddLayer()
	}
}

// determineOptimalScheme analyzes values to pick the most efficient encoding
func determineOptimalScheme(values []uint64) uint8 {
	if len(values) == 0 {
		return SCHEME_2BYTE
	}

	maxVal := uint64(0)
	for _, val := range values {
		if val > maxVal {
			maxVal = val
		}
	}

	if maxVal <= 65535 {
		return SCHEME_2BYTE
	} else if maxVal <= 16777215 {
		return SCHEME_3BYTE
	} else if maxVal <= 4294967295 {
		return SCHEME_4BYTE
	} else if maxVal <= 1099511627775 {
		return SCHEME_5BYTE
	}
	return SCHEME_8BYTE
}

// encodeValues encodes values using the specified scheme
func encodeValues(values []uint64, scheme uint8) []byte {
	switch scheme {
	case SCHEME_2BYTE:
		data := make([]byte, len(values)*2)
		for i, val := range values {
			data[i*2] = byte(val)
			data[i*2+1] = byte(val >> 8)
		}
		return data

	case SCHEME_3BYTE:
		data := make([]byte, len(values)*3)
		for i, val := range values {
			data[i*3] = byte(val)
			data[i*3+1] = byte(val >> 8)
			data[i*3+2] = byte(val >> 16)
		}
		return data

	case SCHEME_4BYTE:
		data := make([]byte, len(values)*4)
		for i, val := range values {
			data[i*4] = byte(val)
			data[i*4+1] = byte(val >> 8)
			data[i*4+2] = byte(val >> 16)
			data[i*4+3] = byte(val >> 24)
		}
		return data

	case SCHEME_5BYTE:
		data := make([]byte, len(values)*5)
		for i, val := range values {
			data[i*5] = byte(val)
			data[i*5+1] = byte(val >> 8)
			data[i*5+2] = byte(val >> 16)
			data[i*5+3] = byte(val >> 24)
			data[i*5+4] = byte(val >> 32)
		}
		return data

	case SCHEME_8BYTE:
		data := make([]byte, len(values)*8)
		for i, val := range values {
			for j := 0; j < 8; j++ {
				data[i*8+j] = byte(val >> (j * 8))
			}
		}
		return data

	default:
		return encodeValues(values, SCHEME_8BYTE)
	}
}

// decodeInto decodes values directly into the provided slice using the specified scheme
//
//go:inline
func decodeInto(data []byte, scheme uint8, count uint32, result []uint64) {
	switch scheme {
	case SCHEME_2BYTE:
		for i := uint32(0); i < count; i++ {
			result[i] = uint64(data[i*2]) | uint64(data[i*2+1])<<8
		}

	case SCHEME_3BYTE:
		for i := uint32(0); i < count; i++ {
			result[i] = uint64(data[i*3]) | uint64(data[i*3+1])<<8 | uint64(data[i*3+2])<<16
		}

	case SCHEME_4BYTE:
		for i := uint32(0); i < count; i++ {
			result[i] = uint64(data[i*4]) |
				uint64(data[i*4+1])<<8 |
				uint64(data[i*4+2])<<16 |
				uint64(data[i*4+3])<<24
		}

	case SCHEME_5BYTE:
		for i := uint32(0); i < count; i++ {
			result[i] = uint64(data[i*5]) |
				uint64(data[i*5+1])<<8 |
				uint64(data[i*5+2])<<16 |
				uint64(data[i*5+3])<<24 |
				uint64(data[i*5+4])<<32
		}

	case SCHEME_8BYTE:
		for i := uint32(0); i < count; i++ {
			val := uint64(0)
			for j := uint32(0); j < 8; j++ {
				val |= uint64(data[i*8+j]) << (j * 8)
			}
			result[i] = val
		}
	}
}

// decodeValues decodes values using the specified scheme
func decodeValues(data []byte, scheme uint8, count uint32) []uint64 {
	result := make([]uint64, count)
	decodeInto(data, scheme, count, result)
	return result
}

// Helper functions for packed scheme and count
func packSchemeAndCount(scheme uint8, count uint32) uint16 {
	if count > 4095 { // 2^12 - 1
		count = 4095
	}
	return uint16(scheme) | uint16(count)<<4
}

func unpackScheme(packed uint16) uint8 {
	return uint8(packed & 0xF)
}

func unpackCount(packed uint16) uint32 {
	return uint32(packed >> 4)
}

func (c *Connections) ReplaceLayer(layer uint8, conns []uint64) {
	if layer >= c.layerCount {
		c.GrowLayersTo(layer)
	}

	if len(conns) == 0 {
		c.layers[layer] = LayerData{}
		return
	}

	scheme := determineOptimalScheme(conns)
	data := encodeValues(conns, scheme)

	c.layers[layer] = LayerData{
		data:   data,
		packed: packSchemeAndCount(scheme, uint32(len(conns))),
	}
}

// Fast insertion optimized for append-only operations
func (c *Connections) InsertAtLayer(conn uint64, layer uint8) {
	if layer >= c.layerCount {
		c.GrowLayersTo(layer)
	}

	layerData := &c.layers[layer]

	// If layer is empty, start with optimal scheme for this value
	if layerData.packed == 0 {
		scheme := determineOptimalScheme([]uint64{conn})
		layerData.packed = packSchemeAndCount(scheme, 1)
		layerData.data = encodeValues([]uint64{conn}, scheme)
		return
	}

	// Check if current scheme can handle the new value
	requiredScheme := determineOptimalScheme([]uint64{conn})
	currentScheme := unpackScheme(layerData.packed)
	if requiredScheme > currentScheme {
		// Need to upgrade scheme - decode, append, re-encode
		values := decodeValues(layerData.data, currentScheme, unpackCount(layerData.packed))
		values = append(values, conn)
		layerData.packed = packSchemeAndCount(requiredScheme, uint32(len(values)))
		layerData.data = encodeValues(values, requiredScheme)
		return
	}

	// Can use current scheme - just append encoded bytes
	c.appendToLayer(conn, layer)
}

// appendToLayer appends a single value using the current scheme
func (c *Connections) appendToLayer(conn uint64, layer uint8) {
	layerData := &c.layers[layer]
	scheme := unpackScheme(layerData.packed)
	count := unpackCount(layerData.packed)

	var bytesNeeded int
	switch scheme {
	case SCHEME_2BYTE:
		bytesNeeded = 2
	case SCHEME_3BYTE:
		bytesNeeded = 3
	case SCHEME_4BYTE:
		bytesNeeded = 4
	case SCHEME_5BYTE:
		bytesNeeded = 5
	case SCHEME_8BYTE:
		bytesNeeded = 8
	default:
		bytesNeeded = 8 // Safe fallback
	}

	// Smart capacity management in limits - grow more conservatively than Go's default doubling
	if len(layerData.data)+bytesNeeded > cap(layerData.data) && len(layerData.data)+bytesNeeded <= DefaultMaxCapacity*bytesNeeded {

		currentLen := len(layerData.data)
		// We can assume this due to previous check
		maxCapacity := DefaultMaxCapacity * bytesNeeded

		// Use growth strategy based on quantile data from real world data
		// p25=0.39, p50=0.52, p75=0.69, p90=0.84, p95=0.92, p99=0.98
		ratio := float64(currentLen) / float64(maxCapacity)
		var target int

		switch {
		case ratio < 0.25:
			target = int(0.25 * float64(maxCapacity))
		case ratio < 0.52:
			target = int(0.52 * float64(maxCapacity))
		case ratio < 0.84:
			target = int(0.84 * float64(maxCapacity))
		default:
			target = maxCapacity
		}

		if target < currentLen+bytesNeeded {
			target = currentLen + bytesNeeded
		}

		// Cap at maximum capacity
		if target > maxCapacity {
			target = maxCapacity
		}

		newData := make([]byte, currentLen, target)
		copy(newData, layerData.data)
		layerData.data = newData
	}

	switch scheme {
	case SCHEME_2BYTE:
		layerData.data = append(layerData.data,
			byte(conn),
			byte(conn>>8))

	case SCHEME_3BYTE:
		layerData.data = append(layerData.data,
			byte(conn),
			byte(conn>>8),
			byte(conn>>16))

	case SCHEME_4BYTE:
		layerData.data = append(layerData.data,
			byte(conn),
			byte(conn>>8),
			byte(conn>>16),
			byte(conn>>24))

	case SCHEME_5BYTE:
		layerData.data = append(layerData.data,
			byte(conn),
			byte(conn>>8),
			byte(conn>>16),
			byte(conn>>24),
			byte(conn>>32))

	case SCHEME_8BYTE:
		for j := 0; j < 8; j++ {
			layerData.data = append(layerData.data, byte(conn>>(j*8)))
		}
	}

	layerData.packed = packSchemeAndCount(scheme, count+1)
}

func (c *Connections) BulkInsertAtLayer(conns []uint64, layer uint8) {
	if layer >= c.layerCount {
		c.GrowLayersTo(layer)
	}

	if len(conns) == 0 {
		return
	}

	layerData := &c.layers[layer]

	if layerData.packed == 0 {
		// Empty layer - just encode all values
		scheme := determineOptimalScheme(conns)
		layerData.packed = packSchemeAndCount(scheme, uint32(len(conns)))
		layerData.data = encodeValues(conns, scheme)
		return
	}

	// Check if current scheme can handle the new values
	currentScheme := unpackScheme(layerData.packed)
	requiredScheme := determineOptimalScheme(conns)

	if requiredScheme <= currentScheme {
		// Current scheme is sufficient - just append encoded bytes
		currentCount := unpackCount(layerData.packed)
		newCount := currentCount + uint32(len(conns))

		// Encode new values using current scheme and append
		newData := encodeValues(conns, currentScheme)
		layerData.data = append(layerData.data, newData...)
		layerData.packed = packSchemeAndCount(currentScheme, newCount)
		return
	}

	// Need to upgrade scheme - decode existing, merge, and re-encode
	existing := decodeValues(layerData.data, currentScheme, unpackCount(layerData.packed))
	all := append(existing, conns...)

	scheme := determineOptimalScheme(all)
	layerData.packed = packSchemeAndCount(scheme, uint32(len(all)))
	layerData.data = encodeValues(all, scheme)
}

func (c *Connections) Data() []byte {
	if c.layerCount == 0 {
		return []byte{0}
	}

	// Calculate total size
	totalSize := 1 // layer count
	for i := uint8(0); i < c.layerCount; i++ {
		totalSize += 2                     // packed scheme and count
		totalSize += 4                     // data length
		totalSize += len(c.layers[i].data) // data
	}

	data := make([]byte, totalSize)
	offset := 0

	data[offset] = c.layerCount
	offset++

	for i := uint8(0); i < c.layerCount; i++ {
		layer := &c.layers[i]

		// Write packed scheme and count (2 bytes, little endian)
		data[offset] = byte(layer.packed)
		data[offset+1] = byte(layer.packed >> 8)
		offset += 2

		// Write data length (4 bytes, little endian)
		dataLen := uint32(len(layer.data))
		data[offset] = byte(dataLen)
		data[offset+1] = byte(dataLen >> 8)
		data[offset+2] = byte(dataLen >> 16)
		data[offset+3] = byte(dataLen >> 24)
		offset += 4

		// Write data
		copy(data[offset:], layer.data)
		offset += len(layer.data)
	}

	return data
}

func (c *Connections) LenAtLayer(layer uint8) int {
	if layer >= c.layerCount {
		return 0
	}
	return int(unpackCount(c.layers[layer].packed))
}

func (c *Connections) GetLayer(layer uint8) []uint64 {
	if layer >= c.layerCount || c.layers[layer].packed == 0 {
		return nil
	}

	layerData := &c.layers[layer]
	return decodeValues(layerData.data, unpackScheme(layerData.packed), unpackCount(layerData.packed))
}

func (c *Connections) CopyLayer(conns []uint64, layer uint8) []uint64 {
	if layer >= c.layerCount || c.layers[layer].packed == 0 {
		return conns[:0]
	}

	layerData := &c.layers[layer]
	count := int(unpackCount(layerData.packed))

	if cap(conns) < count {
		conns = make([]uint64, count)
	} else {
		conns = conns[:count]
	}

	decodeInto(layerData.data, unpackScheme(layerData.packed), uint32(count), conns)
	return conns
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

// Iterator implementations remain similar but work with the new structure
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

// Element iterator for a specific layer
type LayerElementIterator struct {
	connections *Connections
	layer       uint8
	index       int
	maxIndex    int
	values      []uint64 // cached decoded values for performance
}

func (c *Connections) ElementIterator(layer uint8) *LayerElementIterator {
	maxIndex := 0
	var values []uint64

	if layer < c.layerCount && c.layers[layer].packed != 0 {
		maxIndex = int(unpackCount(c.layers[layer].packed))
		// Decode values once for the iterator's lifetime
		layerData := &c.layers[layer]
		values = decodeValues(layerData.data, unpackScheme(layerData.packed), uint32(maxIndex))
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
		c.layers[layer].data = c.layers[layer].data[:0]
		c.layers[layer].packed = 0
	}
}

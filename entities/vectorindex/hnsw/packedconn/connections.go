//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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

// Connections keeps layer 0 inline and the rare upper layers behind a
// pointer. 97% of HNSW nodes have a single layer, and that layer no longer
// needs a separate backing array: one heap object instead of two, in the
// 48 B size class.
type Connections struct {
	// layer 0 is plain fields rather than a nested LayerData so its header
	// shares the struct's padding: 35 B of payload, 40 B padded
	data       []byte
	upper      *[]LayerData
	packed     uint16
	layerCount uint8
}

// layer returns the storage of layer i: layer 0 lives in the struct, the
// others in upper.
func (c *Connections) layer(i uint8) (data *[]byte, packed *uint16) {
	if i == 0 {
		return &c.data, &c.packed
	}
	l := &(*c.upper)[i-1]
	return &l.data, &l.packed
}

// setLayerCount sizes the upper layers for n layers in total, keeping any
// existing layer data.
func (c *Connections) setLayerCount(n uint8) {
	if n > 1 {
		if c.upper == nil {
			s := make([]LayerData, n-1)
			c.upper = &s
		} else if int(n-1) > len(*c.upper) {
			*c.upper = append(*c.upper, make([]LayerData, int(n-1)-len(*c.upper))...)
		}
	}
	c.layerCount = n
}

func NewWithMaxLayer(maxLayer uint8) (*Connections, error) {
	if int(maxLayer)+1 > math.MaxUint8 {
		return nil, fmt.Errorf("max supported layer is %d", math.MaxUint8-1)
	}

	c := &Connections{}
	c.setLayerCount(maxLayer + 1)
	return c, nil
}

func NewWithData(data []byte) *Connections {
	c := &Connections{}
	if len(data) == 0 {
		return c
	}

	offset := 0
	layerCount := data[offset]
	offset++

	c.setLayerCount(layerCount)

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

		// an empty layer stays nil, as it is when built in memory
		var layerData []byte
		if dataLen > 0 {
			// View into the caller's blob rather than a copy. cap==len forces any
			// later append on this layer to reallocate instead of writing into the
			// blob and corrupting the neighboring layer's bytes.
			layerData = data[offset : offset+int(dataLen) : offset+int(dataLen)]
		}
		offset += int(dataLen)

		d, p := c.layer(i)
		*d, *p = layerData, packed
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
	c.setLayerCount(c.layerCount + 1)
}

func (c *Connections) GrowLayersTo(newLayers uint8) {
	targetCount := newLayers + 1
	if targetCount <= c.layerCount {
		return
	}
	c.setLayerCount(targetCount)
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

	data, packed := c.layer(layer)
	if len(conns) == 0 {
		*data, *packed = nil, 0
		return
	}

	scheme := determineOptimalScheme(conns)
	*data = encodeValues(conns, scheme)
	*packed = packSchemeAndCount(scheme, uint32(len(conns)))
}

// Fast insertion optimized for append-only operations
func (c *Connections) InsertAtLayer(conn uint64, layer uint8) {
	if layer >= c.layerCount {
		c.GrowLayersTo(layer)
	}

	data, packed := c.layer(layer)

	// If layer is empty, start with optimal scheme for this value
	if *packed == 0 {
		scheme := determineOptimalScheme([]uint64{conn})
		*packed = packSchemeAndCount(scheme, 1)
		*data = encodeValues([]uint64{conn}, scheme)
		return
	}

	// Check if current scheme can handle the new value
	requiredScheme := determineOptimalScheme([]uint64{conn})
	currentScheme := unpackScheme(*packed)
	if requiredScheme > currentScheme {
		// Need to upgrade scheme - decode, append, re-encode
		values := decodeValues(*data, currentScheme, unpackCount(*packed))
		values = append(values, conn)
		*packed = packSchemeAndCount(requiredScheme, uint32(len(values)))
		*data = encodeValues(values, requiredScheme)
		return
	}

	// Can use current scheme - just append encoded bytes
	c.appendToLayer(conn, layer)
}

// appendToLayer appends a single value using the current scheme
func (c *Connections) appendToLayer(conn uint64, layer uint8) {
	data, packed := c.layer(layer)
	scheme := unpackScheme(*packed)
	count := unpackCount(*packed)

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
	if len(*data)+bytesNeeded > cap(*data) && len(*data)+bytesNeeded <= DefaultMaxCapacity*bytesNeeded {

		currentLen := len(*data)
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
		copy(newData, *data)
		*data = newData
	}

	switch scheme {
	case SCHEME_2BYTE:
		*data = append(*data,
			byte(conn),
			byte(conn>>8))

	case SCHEME_3BYTE:
		*data = append(*data,
			byte(conn),
			byte(conn>>8),
			byte(conn>>16))

	case SCHEME_4BYTE:
		*data = append(*data,
			byte(conn),
			byte(conn>>8),
			byte(conn>>16),
			byte(conn>>24))

	case SCHEME_5BYTE:
		*data = append(*data,
			byte(conn),
			byte(conn>>8),
			byte(conn>>16),
			byte(conn>>24),
			byte(conn>>32))

	case SCHEME_8BYTE:
		for j := 0; j < 8; j++ {
			*data = append(*data, byte(conn>>(j*8)))
		}
	}

	*packed = packSchemeAndCount(scheme, count+1)
}

func (c *Connections) BulkInsertAtLayer(conns []uint64, layer uint8) {
	if layer >= c.layerCount {
		c.GrowLayersTo(layer)
	}

	if len(conns) == 0 {
		return
	}

	data, packed := c.layer(layer)

	if *packed == 0 {
		// Empty layer - just encode all values
		scheme := determineOptimalScheme(conns)
		*packed = packSchemeAndCount(scheme, uint32(len(conns)))
		*data = encodeValues(conns, scheme)
		return
	}

	// Check if current scheme can handle the new values
	currentScheme := unpackScheme(*packed)
	requiredScheme := determineOptimalScheme(conns)

	if requiredScheme <= currentScheme {
		// Current scheme is sufficient - just append encoded bytes
		currentCount := unpackCount(*packed)
		newCount := currentCount + uint32(len(conns))

		// Encode new values using current scheme and append
		newData := encodeValues(conns, currentScheme)
		*data = append(*data, newData...)
		*packed = packSchemeAndCount(currentScheme, newCount)
		return
	}

	// Need to upgrade scheme - decode existing, merge, and re-encode
	existing := decodeValues(*data, currentScheme, unpackCount(*packed))
	all := append(existing, conns...)

	scheme := determineOptimalScheme(all)
	*packed = packSchemeAndCount(scheme, uint32(len(all)))
	*data = encodeValues(all, scheme)
}

func (c *Connections) Data() []byte {
	if c.layerCount == 0 {
		return []byte{0}
	}

	// Calculate total size
	totalSize := 1 // layer count
	for i := uint8(0); i < c.layerCount; i++ {
		totalSize += 2 // packed scheme and count
		totalSize += 4 // data length
		d, _ := c.layer(i)
		totalSize += len(*d) // data
	}

	data := make([]byte, totalSize)
	offset := 0

	data[offset] = c.layerCount
	offset++

	for i := uint8(0); i < c.layerCount; i++ {
		d, p := c.layer(i)

		// Write packed scheme and count (2 bytes, little endian)
		data[offset] = byte(*p)
		data[offset+1] = byte(*p >> 8)
		offset += 2

		// Write data length (4 bytes, little endian)
		dataLen := uint32(len(*d))
		data[offset] = byte(dataLen)
		data[offset+1] = byte(dataLen >> 8)
		data[offset+2] = byte(dataLen >> 16)
		data[offset+3] = byte(dataLen >> 24)
		offset += 4

		// Write data
		copy(data[offset:], *d)
		offset += len(*d)
	}

	return data
}

func (c *Connections) LenAtLayer(layer uint8) int {
	if layer >= c.layerCount {
		return 0
	}
	_, packed := c.layer(layer)
	return int(unpackCount(*packed))
}

// emptyConnections is a shared empty slice to avoid nil returns
var emptyConnections = []uint64{}

func (c *Connections) GetLayer(layer uint8) []uint64 {
	if layer >= c.layerCount {
		return emptyConnections
	}
	data, packed := c.layer(layer)
	if *packed == 0 {
		return emptyConnections
	}
	return decodeValues(*data, unpackScheme(*packed), unpackCount(*packed))
}

func (c *Connections) CopyLayer(conns []uint64, layer uint8) []uint64 {
	if layer >= c.layerCount {
		return conns[:0]
	}
	data, packed := c.layer(layer)
	if *packed == 0 {
		return conns[:0]
	}
	count := int(unpackCount(*packed))

	if cap(conns) < count {
		conns = make([]uint64, count)
	} else {
		conns = conns[:count]
	}

	decodeInto(*data, unpackScheme(*packed), uint32(count), conns)
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

// LayerIterator provides iteration over connection layers
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

	if layer < c.layerCount {
		data, packed := c.layer(layer)
		if *packed != 0 {
			maxIndex = int(unpackCount(*packed))
			// Decode values once for the iterator's lifetime
			values = decodeValues(*data, unpackScheme(*packed), uint32(maxIndex))
		}
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
		data, packed := c.layer(layer)
		*data = (*data)[:0]
		*packed = 0
	}
}

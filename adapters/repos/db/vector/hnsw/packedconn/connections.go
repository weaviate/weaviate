package packedconn

import (
	"fmt"
	"math"
)

const (
	DefaultMaxCapacity = 64
	// Simple encoding schemes - trade some compression for speed
	SCHEME_1BYTE  = 0 // 1 byte per value (0-255)
	SCHEME_2BYTE  = 1 // 2 bytes per value (0-65535)
	SCHEME_3BYTE  = 2 // 3 bytes per value (0-16777215)
	SCHEME_4BYTE  = 3 // 4 bytes per value (0-4294967295)
	SCHEME_8BYTE  = 4 // 8 bytes per value (full uint64)
	SCHEME_VARINT = 5 // Variable length encoding for mixed sizes
)

type LayerData struct {
	data   []byte
	scheme uint8
	count  uint32 // number of elements
}

type Connections struct {
	layers      []LayerData
	maxCapacity int
	layerCount  uint8
}

func NewWithMaxLayer(maxLayer uint8) (*Connections, error) {
	if int(maxLayer)+1 > math.MaxUint8 {
		return nil, fmt.Errorf("max supported layer is %d", math.MaxUint8-1)
	}

	layerCount := maxLayer + 1
	return &Connections{
		layers:      make([]LayerData, layerCount),
		maxCapacity: DefaultMaxCapacity,
		layerCount:  layerCount,
	}, nil
}

func NewWithData(data []byte) *Connections {
	if len(data) == 0 {
		return &Connections{
			layers:      make([]LayerData, 0),
			maxCapacity: DefaultMaxCapacity,
			layerCount:  0,
		}
	}

	offset := 0
	layerCount := data[offset]
	offset++

	c := &Connections{
		layers:      make([]LayerData, layerCount),
		maxCapacity: DefaultMaxCapacity,
		layerCount:  layerCount,
	}

	for i := uint8(0); i < layerCount; i++ {
		if offset+9 > len(data) {
			break // Malformed data
		}

		// Read scheme (1 byte)
		scheme := data[offset]
		offset++

		// Read count (4 bytes, little endian)
		count := uint32(data[offset]) |
			uint32(data[offset+1])<<8 |
			uint32(data[offset+2])<<16 |
			uint32(data[offset+3])<<24
		offset += 4

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
			scheme: scheme,
			count:  count,
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

func (c *Connections) SetMaxCapacity(capacity int) {
	c.maxCapacity = capacity
}

func (c *Connections) GetMaxCapacity() int {
	return c.maxCapacity
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

	for c.layerCount < targetCount {
		c.AddLayer()
	}
}

// determineOptimalScheme analyzes values to pick the most efficient encoding
func determineOptimalScheme(values []uint64) uint8 {
	if len(values) == 0 {
		return SCHEME_1BYTE
	}

	maxVal := uint64(0)
	for _, val := range values {
		if val > maxVal {
			maxVal = val
		}
	}

	if maxVal <= 255 {
		return SCHEME_1BYTE
	} else if maxVal <= 65535 {
		return SCHEME_2BYTE
	} else if maxVal <= 16777215 {
		return SCHEME_3BYTE
	} else if maxVal <= 4294967295 {
		return SCHEME_4BYTE
	}
	return SCHEME_8BYTE
}

// encodeValues encodes values using the specified scheme
func encodeValues(values []uint64, scheme uint8) []byte {
	switch scheme {
	case SCHEME_1BYTE:
		data := make([]byte, len(values))
		for i, val := range values {
			data[i] = byte(val)
		}
		return data

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

// decodeValues decodes values using the specified scheme
func decodeValues(data []byte, scheme uint8, count uint32) []uint64 {
	result := make([]uint64, count)

	switch scheme {
	case SCHEME_1BYTE:
		for i := uint32(0); i < count; i++ {
			result[i] = uint64(data[i])
		}

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

	case SCHEME_8BYTE:
		for i := uint32(0); i < count; i++ {
			val := uint64(0)
			for j := uint32(0); j < 8; j++ {
				val |= uint64(data[i*8+j]) << (j * 8)
			}
			result[i] = val
		}
	}

	return result
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
		scheme: scheme,
		count:  uint32(len(conns)),
	}
}

// Fast insertion optimized for append-only operations
func (c *Connections) InsertAtLayer(conn uint64, layer uint8) {
	if layer >= c.layerCount {
		c.GrowLayersTo(layer)
	}

	layerData := &c.layers[layer]

	// If layer is empty, start with optimal scheme for this value
	if layerData.count == 0 {
		scheme := determineOptimalScheme([]uint64{conn})
		layerData.scheme = scheme
		layerData.data = encodeValues([]uint64{conn}, scheme)
		layerData.count = 1
		return
	}

	// Check if current scheme can handle the new value
	requiredScheme := determineOptimalScheme([]uint64{conn})
	if requiredScheme > layerData.scheme {
		// Need to upgrade scheme - decode, append, re-encode
		values := decodeValues(layerData.data, layerData.scheme, layerData.count)
		values = append(values, conn)
		layerData.scheme = requiredScheme
		layerData.data = encodeValues(values, requiredScheme)
		layerData.count++
		return
	}

	// Can use current scheme - just append encoded bytes
	c.appendToLayer(conn, layer)
}

// appendToLayer appends a single value using the current scheme
func (c *Connections) appendToLayer(conn uint64, layer uint8) {
	layerData := &c.layers[layer]

	switch layerData.scheme {
	case SCHEME_1BYTE:
		layerData.data = append(layerData.data, byte(conn))

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

	case SCHEME_8BYTE:
		for j := 0; j < 8; j++ {
			layerData.data = append(layerData.data, byte(conn>>(j*8)))
		}
	}

	layerData.count++
}

func (c *Connections) BulkInsertAtLayer(conns []uint64, layer uint8) {
	if layer >= c.layerCount {
		c.GrowLayersTo(layer)
	}

	if len(conns) == 0 {
		return
	}

	layerData := &c.layers[layer]

	if layerData.count == 0 {
		// Empty layer - just encode all values
		scheme := determineOptimalScheme(conns)
		layerData.scheme = scheme
		layerData.data = encodeValues(conns, scheme)
		layerData.count = uint32(len(conns))
		return
	}

	// Need to merge with existing data
	existing := decodeValues(layerData.data, layerData.scheme, layerData.count)
	all := append(existing, conns...)

	scheme := determineOptimalScheme(all)
	layerData.scheme = scheme
	layerData.data = encodeValues(all, scheme)
	layerData.count = uint32(len(all))
}

func (c *Connections) Data() []byte {
	if c.layerCount == 0 {
		return []byte{0}
	}

	// Calculate total size
	totalSize := 1 // layer count
	for i := uint8(0); i < c.layerCount; i++ {
		totalSize += 1                     // scheme
		totalSize += 4                     // count
		totalSize += 4                     // data length
		totalSize += len(c.layers[i].data) // data
	}

	data := make([]byte, totalSize)
	offset := 0

	data[offset] = c.layerCount
	offset++

	for i := uint8(0); i < c.layerCount; i++ {
		layer := &c.layers[i]

		// Write scheme
		data[offset] = layer.scheme
		offset++

		// Write count (4 bytes, little endian)
		data[offset] = byte(layer.count)
		data[offset+1] = byte(layer.count >> 8)
		data[offset+2] = byte(layer.count >> 16)
		data[offset+3] = byte(layer.count >> 24)
		offset += 4

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
	return int(c.layers[layer].count)
}

func (c *Connections) GetLayer(layer uint8) []uint64 {
	if layer >= c.layerCount || c.layers[layer].count == 0 {
		return nil
	}

	layerData := &c.layers[layer]
	return decodeValues(layerData.data, layerData.scheme, layerData.count)
}

func (c *Connections) CopyLayer(conns []uint64, layer uint8) []uint64 {
	if layer >= c.layerCount || c.layers[layer].count == 0 {
		return conns[:0]
	}

	layerData := &c.layers[layer]
	values := decodeValues(layerData.data, layerData.scheme, layerData.count)

	if cap(conns) < len(values) {
		conns = make([]uint64, len(values))
	} else {
		conns = conns[:len(values)]
	}

	copy(conns, values)
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

	if layer < c.layerCount && c.layers[layer].count > 0 {
		maxIndex = int(c.layers[layer].count)
		// Decode values once for the iterator's lifetime
		layerData := &c.layers[layer]
		values = decodeValues(layerData.data, layerData.scheme, layerData.count)
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

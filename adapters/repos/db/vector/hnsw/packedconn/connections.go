package packedconn

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// Data order
// | Start | Len        | Description |
// | ----- | ---------- | ------------|
// | 0     | 1          | uint8 indicating count of layers of object |
// | 1     | 3 * layers | uint8 len indicator, followed by uint16 offset indicator |
// | dyn.  | dyn        | all layers, starting with highest layer first

type Connections struct {
	data []byte
}

const layerPos = 0
const initialSize = 50

func NewWithMaxLayer(maxLayer uint8) (Connections, error) {
	if maxLayer+1 > math.MaxUint8 {
		return Connections{}, fmt.Errorf("max supported layer is %d",
			math.MaxUint8-1)
	}
	c := Connections{
		// TODO: low initial size and grow dynamically
		data: make([]byte, initialSize),
	}

	c.initLayers(maxLayer)

	return c, nil
}

func (c *Connections) ReplaceLayer(layer uint8, conns []uint64) {
	// create a temporary buffer that is guaranteed to fit everything. The
	// over-allocation does not matter, this buffer won't stick around, so the
	// only real downside is the overhead on GC. If this because noticeable this
	// buffer would be suitable to use pooling.
	buf := make([]byte, len(conns)*binary.MaxVarintLen64)

	sort.Slice(conns, func(a, b int) bool { return conns[a] < conns[b] })
	last := uint64(0)
	offset := 0
	for _, raw := range conns {
		delta := raw - last
		last = raw
		offset += binary.PutUvarint(buf[offset:], delta)
	}

	buf = buf[:offset]

	c.replaceLayer(layer, buf, uint8(len(conns)))
}

func (c Connections) LenAtLayer(layer uint8) int {
	if layer >= c.layers() {
		panic(fmt.Sprintf("only has %d layers", c.layers()))
	}

	return int(c.layerLength(layer))
}

func (c Connections) CopyLayer(conns []uint64, layer uint8) []uint64 {
	if cap(conns) < int(c.layerLength(layer)) {
		conns = make([]uint64, c.layerLength(layer))
	} else {
		conns = conns[:c.layerLength(layer)]
	}

	offset := c.layerOffset(layer)
	end := c.layerEndOffset(layer)
	last := uint64(0)
	i := 0
	for offset < end {
		val, n := binary.Uvarint(c.data[offset:])
		offset += uint16(n)

		// TODO: allocate exact size, don't rely on dynamic growing
		conns[i] = last + val
		last += val
		i++
	}

	return conns
}

func (c Connections) GetLayer(layer uint8) []uint64 {
	return c.CopyLayer(nil, layer)
}

func (c *Connections) InsertAtLayer(conn uint64, layer uint8) {
	offset := c.layerOffset(layer)
	end := c.layerEndOffset(layer)
	val := uint64(0)
	var n int
	val, n = binary.Uvarint(c.data[offset:])
	offset += uint16(n)
	for end > offset-uint16(n) && val < conn {
		conn -= val
		val, n = binary.Uvarint(c.data[offset:])
		offset += uint16(n)
	}

	len := c.replaceElement(layer, offset-uint16(n), 0, conn)
	offset = offset - uint16(n) + len
	c.setLayerLength(layer, c.layerLength(layer)+1)
	if end > offset-uint16(n) {
		val, n = binary.Uvarint(c.data[offset:])
		c.replaceElement(layer, offset, n, val-conn)
	}
}

func (c *Connections) replaceElement(layer uint8, pos uint16, formerLen int, value uint64) uint16 {
	buff := make([]byte, 8)
	len := binary.PutUvarint(buff, value)
	if len > formerLen {
		c.shiftRightByAndAdaptOffsets(pos, uint16(len-formerLen), layer)
	} else if formerLen > len {
		c.shiftLeftByAndAdaptOffsets(pos, uint16(formerLen-len), layer)
	}
	copy(c.data[pos:], buff[:len])
	return uint16(len)
}

func (c *Connections) initLayers(maxLayer uint8) {
	layers := maxLayer + 1
	c.data[layerPos] = layers

	// TODO: ensure correct minimum capacity
	c.data = c.data[:c.initialLayerOffset()]

	layer := maxLayer
	for {
		c.setLayerLength(layer, 0)
		c.setLayerOffset(layer, c.initialLayerOffset())

		if layer == 0 {
			break
		}
		layer--
	}
}

// number of layers, e.g. if the maxLayer is 7, the number of layers is 8, as 0
// is a valid layer
func (c *Connections) layers() uint8 {
	return c.data[layerPos]
}

func (c *Connections) layerLengthPos(layer uint8) int {
	return 1 + int(layer*3)
}

func (c *Connections) layerLength(layer uint8) uint8 {
	return c.data[c.layerLengthPos(layer)]
}

func (c *Connections) setLayerLength(layer, length uint8) {
	c.data[c.layerLengthPos(layer)] = length
}

func (c *Connections) layerOffsetPos(layer uint8) int {
	return c.layerLengthPos(layer) + 1
}

func (c *Connections) layerOffset(layer uint8) uint16 {
	return binary.LittleEndian.Uint16(c.data[c.layerOffsetPos(layer):])
}

func (c *Connections) layerEndOffset(layer uint8) uint16 {
	if layer == 0 {
		return uint16(len(c.data))
	}

	return c.layerOffset(layer - 1)
}

func (c *Connections) layerSize(layer uint8) uint16 {
	return c.layerEndOffset(layer) - c.layerOffset(layer)
}

func (c *Connections) setLayerOffset(layer uint8, offset uint16) {
	binary.LittleEndian.PutUint16(c.data[c.layerOffsetPos(layer):], offset)
}

func (c *Connections) initialLayerOffset() uint16 {
	// 1 byte for the uint8 indicating len
	return uint16(1 + c.layers()*3)
}

func (c *Connections) expandDataIfRequired(delta uint16) {
	newSize := len(c.data) + int(delta)
	if cap(c.data) < newSize {
		temp := c.data
		c.data = make([]byte, newSize, newSize+newSize/10)
		copy(c.data, temp)
	} else {
		c.data = c.data[:newSize]
	}
}

func (c *Connections) growLayerBy(layer uint8, delta uint16) {
	c.expandDataIfRequired(delta)

	if layer > 0 {
		// the backing array has the correct size now, next up we need to adapt the
		// offsets. Since layers are in reverse order, higher layers are not
		// affected, but any lower layer needs to be shifted right by the delta
		c.shiftRightBy(c.layerOffset(layer-1), delta)
	}

	for l := uint8(0); l < layer; l++ {
		c.setLayerOffset(l, c.layerOffset(l)+delta)
	}
}

func (c *Connections) shrinkLayerBy(layer uint8, delta uint16) {
	// TODO: check cap and shrink backing array if required

	// shrinking needs to happen in the reverse order of growing, we need to
	// first fix the offsets, otherwise our start position (the copy target) is
	// too high. We would only copy into the where the next layer previously
	// began instead of where it will begin in the future
	for l := uint8(0); l < layer; l++ {
		c.setLayerOffset(l, c.layerOffset(l)-delta)
	}

	if layer > 0 {
		// the backing array has the correct size now, next up we need to adapt the
		// offsets. Since layers are in reverse order, higher layers are not
		// affected, but any lower layer needs to be shifted left by the delta
		c.shiftLeftBy(c.layerOffset(layer-1), delta)
	}
	c.data = c.data[:len(c.data)-int(delta)]
}

func (c *Connections) shiftRightByAndAdaptOffsets(startPos, delta uint16, layer uint8) {
	c.growLayerBy(layer, delta)
	copy(c.data[startPos+delta:], c.data[startPos:c.layerEndOffset(layer)-delta])
}

func (c *Connections) shiftLeftByAndAdaptOffsets(startPos, delta uint16, layer uint8) {
	c.shrinkLayerBy(layer, delta)
	copy(c.data[startPos+delta:], c.data[startPos:c.layerEndOffset(layer)])
}

func (c *Connections) shiftRightBy(startPos, delta uint16) {
	copy(c.data[startPos+delta:], c.data[startPos:])
}

func (c *Connections) shiftLeftBy(startPos, delta uint16) {
	copy(c.data[startPos:], c.data[startPos+delta:])
}

func (c *Connections) replaceLayer(layer uint8, contents []byte,
	length uint8,
) {
	// resize
	oldLayerSize := c.layerSize(layer)
	newLayerSize := uint16(len(contents))

	if oldLayerSize > newLayerSize {
		c.shrinkLayerBy(layer, oldLayerSize-newLayerSize)
	} else if newLayerSize > oldLayerSize {
		c.growLayerBy(layer, newLayerSize-oldLayerSize)
	}
	copy(c.data[c.layerOffset(layer):], contents)
	c.setLayerLength(layer, length)
}

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
	"encoding/binary"
	"math/bits"
)

type CompressedUint64List struct {
	data      []byte
	positions []byte
	scheme    uint8
	count     int
	tempBuff  []byte
}

func NewCompressedUint64List() *CompressedUint64List {
	return &CompressedUint64List{
		data:      make([]byte, 0, 16),
		positions: make([]byte, 0, 4),
		tempBuff:  make([]byte, 5),
		scheme:    0,
		count:     0,
	}
}

func (c *CompressedUint64List) Count() int {
	return c.count
}

func (c *CompressedUint64List) EncodeValues(values []uint64) {
	if len(values) == 0 {
		return
	}

	maxBytes := 1

	if c.count > 0 {
		maxBytes = 1 << c.scheme
		if c.scheme == 0 {
			maxBytes = 1
		}
	}

	for _, value := range values {
		needed := bytesNeeded(value)
		if needed > maxBytes {
			maxBytes = needed
		}
	}

	newScheme := calculateScheme(maxBytes)
	if newScheme != c.scheme {
		c.reencodePositions(newScheme)
	}

	estimatedDataSize := len(c.data) + len(values)*maxBytes
	if cap(c.data) < estimatedDataSize {
		newData := make([]byte, len(c.data), estimatedDataSize)
		copy(newData, c.data)
		c.data = newData
	}

	for _, value := range values {
		needed := bytesNeeded(value)
		c.tempBuff = encodeValue(value, needed, c.tempBuff)
		if cap(c.data) >= len(c.data)+len(c.tempBuff) {
			c.data = c.data[:len(c.data)+len(c.tempBuff)]
		} else {
			tempBuff := make([]byte, len(c.data)+len(c.tempBuff))
			copy(tempBuff, c.data)
			c.data = tempBuff
		}
		copy(c.data[len(c.data)-len(c.tempBuff):], c.tempBuff)

		var posValue uint8
		if c.scheme == 0 {
			posValue = 0
		} else {
			posValue = uint8(needed - 1)
		}
		c.setBitsInPositions(c.count, posValue, c.scheme)
		c.count++
	}
}

func (c *CompressedUint64List) Append(value uint64) {
	c.EncodeValues([]uint64{value})
}

func (c *CompressedUint64List) DecodeValues() []uint64 {
	if c.count == 0 {
		return nil
	}

	result := make([]uint64, 0, c.count)
	return c.CopyValues(result)
}

func (c *CompressedUint64List) CopyValues(target []uint64) []uint64 {
	if c.count == 0 {
		target = target[:0]
		return target
	}

	if cap(target) >= c.count {
		target = target[:c.count]
	} else {
		target = make([]uint64, c.count)
	}

	dataOffset := 0

	for i := 0; i < c.count; i++ {
		var numBytes int
		if c.scheme == 0 {
			numBytes = 1
		} else {
			posValue := c.getBitsFromPositions(i, c.scheme)
			numBytes = int(posValue) + 1
		}

		if dataOffset+numBytes > len(c.data) {
			break
		}

		value := decodeValue(c.data, dataOffset, numBytes)
		target[i] = value
		dataOffset += numBytes
	}

	return target
}

func (c *CompressedUint64List) DataLen() uint32 {
	return uint32(len(c.data) + len(c.positions) + 10)
}

func (c *CompressedUint64List) WriteData(target []byte) {
	binary.BigEndian.PutUint32(target, uint32(len(c.positions)))
	binary.BigEndian.PutUint32(target[4:], uint32(len(c.data)))
	target[8] = c.scheme
	target[9] = uint8(c.count)
	copy(target[10:], c.positions)
	copy(target[10+len(c.positions):], c.data)
}

func (c *CompressedUint64List) Restore(from []byte) int {
	positionsLen := binary.BigEndian.Uint32(from)
	dataLen := binary.BigEndian.Uint32(from[4:])
	c.scheme = from[8]
	c.count = int(from[9])
	c.positions = make([]byte, positionsLen)
	copy(c.positions, from[10:10+positionsLen])
	c.data = make([]byte, dataLen)
	copy(c.data, from[10+positionsLen:10+positionsLen+dataLen])
	return int(10 + positionsLen + dataLen)
}

func bytesNeeded(value uint64) int {
	if value == 0 {
		return 1
	}
	return (bits.Len64(value) + 7) / 8
}

func calculateScheme(maxBytes int) uint8 {
	if maxBytes <= 1 {
		return 0
	}
	if maxBytes <= 2 {
		return 1
	}
	if maxBytes <= 4 {
		return 2
	}
	return 3
}

func encodeValue(value uint64, numBytes int, source []byte) []byte {
	if cap(source) < numBytes {
		source = make([]byte, numBytes)
	}
	source = source[:numBytes]
	for i := 0; i < numBytes; i++ {
		source[i] = byte(value >> (8 * i))
	}
	return source
}

func decodeValue(data []byte, offset, numBytes int) uint64 {
	var result uint64
	for i := 0; i < numBytes; i++ {
		result |= uint64(data[offset+i]) << (8 * i)
	}
	return result
}

func (c *CompressedUint64List) setBitsInPositions(index int, value uint8, numBits uint8) {
	if numBits == 0 {
		return
	}

	bitOffset := index * int(numBits)
	byteOffset := bitOffset / 8
	bitInByte := bitOffset % 8

	for len(c.positions) <= byteOffset {
		c.positions = append(c.positions, 0)
	}

	if bitInByte+int(numBits) <= 8 {
		mask := byte((1 << numBits) - 1)
		c.positions[byteOffset] &= ^(mask << bitInByte)
		c.positions[byteOffset] |= (value & mask) << bitInByte
	} else {
		bitsInFirstByte := 8 - bitInByte
		bitsInSecondByte := int(numBits) - bitsInFirstByte

		mask1 := byte((1 << bitsInFirstByte) - 1)
		c.positions[byteOffset] &= ^(mask1 << bitInByte)
		c.positions[byteOffset] |= (value & mask1) << bitInByte

		if len(c.positions) <= byteOffset+1 {
			c.positions = append(c.positions, 0)
		}
		mask2 := byte((1 << bitsInSecondByte) - 1)
		c.positions[byteOffset+1] &= ^mask2
		c.positions[byteOffset+1] |= (value >> bitsInFirstByte) & mask2
	}
}

func (c *CompressedUint64List) getBitsFromPositions(index int, numBits uint8) uint8 {
	if numBits == 0 {
		return 1
	}

	bitOffset := index * int(numBits)
	byteOffset := bitOffset / 8
	bitInByte := bitOffset % 8

	if byteOffset >= len(c.positions) {
		return 0
	}

	if bitInByte+int(numBits) <= 8 {
		mask := byte((1 << numBits) - 1)
		return (c.positions[byteOffset] >> bitInByte) & mask
	} else {
		bitsInFirstByte := 8 - bitInByte
		bitsInSecondByte := int(numBits) - bitsInFirstByte

		result := (c.positions[byteOffset] >> bitInByte)
		if byteOffset+1 < len(c.positions) {
			mask := byte((1 << bitsInSecondByte) - 1)
			result |= (c.positions[byteOffset+1] & mask) << bitsInFirstByte
		}
		return result
	}
}

func (c *CompressedUint64List) reencodePositions(newScheme uint8) {
	if c.count == 0 {
		c.scheme = newScheme
		return
	}

	var oldPositions []uint8

	if c.scheme == 0 {
		oldPositions = make([]uint8, c.count)
		for i := 0; i < c.count; i++ {
			oldPositions[i] = 0
		}
	} else {
		oldPositions = make([]uint8, c.count)
		for i := 0; i < c.count; i++ {
			oldPositions[i] = c.getBitsFromPositions(i, c.scheme)
		}
	}

	c.positions = c.positions[:0]

	c.scheme = newScheme
	for i, pos := range oldPositions {
		c.setBitsInPositions(i, pos, newScheme)
	}
}

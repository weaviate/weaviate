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
	"slices"
)

type PrefixEncoder struct {
	elementCount uint8
	lastValue    uint64
	buffer       []byte
}

func NewPrefixEncoder() *PrefixEncoder {
	return &PrefixEncoder{
		elementCount: 0,
		lastValue:    0,
		buffer:       make([]byte, 0, 16),
	}
}

func (encoder *PrefixEncoder) Len() int {
	return int(encoder.elementCount)
}

func (encoder *PrefixEncoder) InitWithBuff(values []uint64, buff []byte) {
	encoder.buffer = buff[:0]
	encoder.Init(values)
	temp := encoder.buffer
	encoder.buffer = make([]byte, len(temp))
	copy(encoder.buffer, temp)
}

func (encoder *PrefixEncoder) Init(values []uint64) {
	if len(values) == 0 {
		encoder.buffer = make([]byte, 0, 16)
		encoder.elementCount = 0
		encoder.lastValue = 0
		return
	}

	slices.Sort(values)

	count := len(values)
	prefixBytes := (count*2 + 7) / 8
	dataPos := prefixBytes
	lastValue := values[count-1]

	var prevValue uint64 = 0

	for groupStart := 0; groupStart < count; groupStart += 4 {
		var prefixByte byte
		groupEnd := groupStart + 4
		if groupEnd > count {
			groupEnd = count
		}

		for i := groupStart; i < groupEnd; i++ {
			delta := uint32(values[i] - prevValue)
			bitPos := (i - groupStart) * 2

			if delta < 256 {
				if dataPos >= len(encoder.buffer) {
					encoder.grow(dataPos + 1)
				}
				encoder.buffer[dataPos] = byte(delta)
				dataPos++
			} else if delta < 65536 {
				prefixByte |= 1 << bitPos
				if dataPos+1 >= len(encoder.buffer) {
					encoder.grow(dataPos + 2)
				}
				encoder.buffer[dataPos] = byte(delta)
				encoder.buffer[dataPos+1] = byte(delta >> 8)
				dataPos += 2
			} else if delta < 16777216 {
				prefixByte |= 2 << bitPos
				if dataPos+2 >= len(encoder.buffer) {
					encoder.grow(dataPos + 3)
				}
				encoder.buffer[dataPos] = byte(delta)
				encoder.buffer[dataPos+1] = byte(delta >> 8)
				encoder.buffer[dataPos+2] = byte(delta >> 16)
				dataPos += 3
			} else {
				prefixByte |= 3 << bitPos
				if dataPos+3 >= len(encoder.buffer) {
					encoder.grow(dataPos + 4)
				}
				encoder.buffer[dataPos] = byte(delta)
				encoder.buffer[dataPos+1] = byte(delta >> 8)
				encoder.buffer[dataPos+2] = byte(delta >> 16)
				encoder.buffer[dataPos+3] = byte(delta >> 24)
				dataPos += 4
			}

			prevValue = values[i]
		}

		encoder.buffer[groupStart/4] = prefixByte
	}

	encoder.lastValue = lastValue
	encoder.elementCount = uint8(count)
}

func (encoder *PrefixEncoder) Decode(values []uint64) []uint64 {
	if len(encoder.buffer) == 0 || encoder.elementCount == 0 {
		values = values[:0]
		return values
	}

	prefixBytes := int((encoder.elementCount)*2+7) / 8
	if len(encoder.buffer) < prefixBytes {
		return nil
	}

	if cap(values) < int(encoder.elementCount) {
		values = make([]uint64, encoder.elementCount)
	} else {
		values = values[:encoder.elementCount]
	}

	pos := prefixBytes
	var currentValue uint64 = 0

	for i := 0; i < int(encoder.elementCount) && pos < len(encoder.buffer); i++ {
		prefixByteIdx := i / 4
		bitOffset := (i % 4) * 2
		lengthCode := (encoder.buffer[prefixByteIdx] >> bitOffset) & 0x03
		length := lengthCode + 1

		var delta uint64
		switch length {
		case 1:
			if pos+1 <= len(encoder.buffer) {
				delta = uint64(encoder.buffer[pos])
			}
		case 2:
			if pos+2 <= len(encoder.buffer) {
				delta = uint64(encoder.buffer[pos]) | uint64(encoder.buffer[pos+1])<<8
			}
		case 3:
			if pos+3 <= len(encoder.buffer) {
				delta = uint64(encoder.buffer[pos]) | uint64(encoder.buffer[pos+1])<<8 | uint64(encoder.buffer[pos+2])<<16
			}
		case 4:
			if pos+4 <= len(encoder.buffer) {
				delta = uint64(encoder.buffer[pos]) | uint64(encoder.buffer[pos+1])<<8 | uint64(encoder.buffer[pos+2])<<16 | uint64(encoder.buffer[pos+3])<<24
			}
		}

		currentValue += delta
		values[i] = currentValue
		pos += int(length)
	}

	return values
}

func (encoder *PrefixEncoder) Add(element uint64) {
	if element >= encoder.lastValue {
		encoder.addAtEnd(element)
	} else {
		encoder.addInMiddle(element)
	}
	encoder.elementCount++
}

func (encoder *PrefixEncoder) addAtEnd(element uint64) {
	delta := uint32(element - encoder.lastValue)

	var newElementLength int
	if delta < 256 {
		newElementLength = 1
	} else if delta < 65536 {
		newElementLength = 2
	} else if delta < 16777216 {
		newElementLength = 3
	} else {
		newElementLength = 4
	}

	newElementCount := int(encoder.elementCount) + 1
	oldPrefixBytes := (int(encoder.elementCount)*2 + 7) / 8
	newPrefixBytes := (newElementCount*2 + 7) / 8

	oldDataSize := len(encoder.buffer) - oldPrefixBytes
	newTotalSize := newPrefixBytes + oldDataSize + newElementLength

	needsRealloc := cap(encoder.buffer) < newTotalSize
	prefixExpanded := newPrefixBytes != oldPrefixBytes

	var newBuffer []byte

	if needsRealloc {
		newBuffer = make([]byte, newTotalSize, newTotalSize*2)

		if prefixExpanded {
			copy(newBuffer[:oldPrefixBytes], encoder.buffer[:oldPrefixBytes])
			for i := oldPrefixBytes; i < newPrefixBytes; i++ {
				newBuffer[i] = 0
			}
			copy(newBuffer[newPrefixBytes:newPrefixBytes+oldDataSize], encoder.buffer[oldPrefixBytes:])
		} else {
			copy(newBuffer[:len(encoder.buffer)], encoder.buffer)
		}
	} else {
		newBuffer = encoder.buffer[:newTotalSize]

		if prefixExpanded {
			copy(newBuffer[newPrefixBytes:], encoder.buffer[oldPrefixBytes:len(encoder.buffer)])
			for i := oldPrefixBytes; i < newPrefixBytes; i++ {
				newBuffer[i] = 0
			}
		}
	}

	dataInsertPos := newPrefixBytes + oldDataSize
	switch newElementLength {
	case 1:
		newBuffer[dataInsertPos] = byte(delta)
	case 2:
		newBuffer[dataInsertPos] = byte(delta)
		newBuffer[dataInsertPos+1] = byte(delta >> 8)
	case 3:
		newBuffer[dataInsertPos] = byte(delta)
		newBuffer[dataInsertPos+1] = byte(delta >> 8)
		newBuffer[dataInsertPos+2] = byte(delta >> 16)
	case 4:
		newBuffer[dataInsertPos] = byte(delta)
		newBuffer[dataInsertPos+1] = byte(delta >> 8)
		newBuffer[dataInsertPos+2] = byte(delta >> 16)
		newBuffer[dataInsertPos+3] = byte(delta >> 24)
	}

	newElemPrefixByteIdx := int(encoder.elementCount) / 4
	newElemBitOffset := (int(encoder.elementCount) % 4) * 2
	newElemLengthCode := byte(newElementLength - 1)

	if newElemPrefixByteIdx < newPrefixBytes {
		newBuffer[newElemPrefixByteIdx] &= ^(0x03 << newElemBitOffset)
		newBuffer[newElemPrefixByteIdx] |= newElemLengthCode << newElemBitOffset
	}

	encoder.lastValue = element
	encoder.buffer = newBuffer
}

func (encoder *PrefixEncoder) addInMiddle(element uint64) {
	if encoder.elementCount == 0 {
		encoder.addAtEnd(element)
		return
	}

	oldPrefixBytes := (int(encoder.elementCount)*2 + 7) / 8
	newElementCount := int(encoder.elementCount) + 1
	newPrefixBytes := (newElementCount*2 + 7) / 8

	pos := oldPrefixBytes
	elementIndex := 0
	var currentValue uint64 = 0
	var insertBytePos int
	var prevValue uint64
	var nextElementIndex int = -1
	var nextElementBytePos int
	var nextElementLength int

	for elementIndex < int(encoder.elementCount) && pos < len(encoder.buffer) {
		prefixByteIdx := elementIndex / 4
		bitOffset := (elementIndex % 4) * 2
		lengthCode := (encoder.buffer[prefixByteIdx] >> bitOffset) & 0x03
		length := int(lengthCode) + 1

		var delta uint32
		switch length {
		case 1:
			delta = uint32(encoder.buffer[pos])
		case 2:
			delta = uint32(encoder.buffer[pos]) | uint32(encoder.buffer[pos+1])<<8
		case 3:
			delta = uint32(encoder.buffer[pos]) | uint32(encoder.buffer[pos+1])<<8 | uint32(encoder.buffer[pos+2])<<16
		case 4:
			delta = uint32(encoder.buffer[pos]) | uint32(encoder.buffer[pos+1])<<8 | uint32(encoder.buffer[pos+2])<<16 | uint32(encoder.buffer[pos+3])<<24
		}

		currentValue += uint64(delta)

		if currentValue < element {
			insertBytePos = pos + length
			prevValue = currentValue
		} else if nextElementIndex == -1 {
			nextElementIndex = elementIndex
			nextElementBytePos = pos
			nextElementLength = length
			insertBytePos = pos
			break
		}

		pos += length
		elementIndex++
	}

	if nextElementIndex == -1 {
		encoder.addAtEnd(element)
		return
	}

	var newElementDelta uint32
	if nextElementIndex == 0 {
		newElementDelta = uint32(element)
	} else {
		newElementDelta = uint32(element - prevValue)
	}

	adjustedNextDelta := uint32(currentValue - element)

	var newElementLength int
	if newElementDelta < 256 {
		newElementLength = 1
	} else if newElementDelta < 65536 {
		newElementLength = 2
	} else if newElementDelta < 16777216 {
		newElementLength = 3
	} else {
		newElementLength = 4
	}

	var adjustedNextElementLength int
	if adjustedNextDelta < 256 {
		adjustedNextElementLength = 1
	} else if adjustedNextDelta < 65536 {
		adjustedNextElementLength = 2
	} else if adjustedNextDelta < 16777216 {
		adjustedNextElementLength = 3
	} else {
		adjustedNextElementLength = 4
	}

	dataLengthChange := newElementLength + adjustedNextElementLength - nextElementLength
	prefixBytesChange := newPrefixBytes - oldPrefixBytes
	totalSizeChange := dataLengthChange + prefixBytesChange

	newTotalSize := len(encoder.buffer) + totalSizeChange
	needsRealloc := cap(encoder.buffer) < newTotalSize

	var newBuffer []byte
	if needsRealloc {
		newBuffer = make([]byte, newTotalSize, newTotalSize*2)
	} else {
		newBuffer = encoder.buffer[:newTotalSize]
	}

	if prefixBytesChange > 0 {
		if needsRealloc {
			copy(newBuffer[:oldPrefixBytes], encoder.buffer[:oldPrefixBytes])
			for i := oldPrefixBytes; i < newPrefixBytes; i++ {
				newBuffer[i] = 0
			}
			copy(newBuffer[newPrefixBytes:newPrefixBytes+insertBytePos-oldPrefixBytes],
				encoder.buffer[oldPrefixBytes:insertBytePos])
		} else {
			copy(newBuffer[newPrefixBytes+insertBytePos-oldPrefixBytes+newElementLength+adjustedNextElementLength:],
				encoder.buffer[insertBytePos+nextElementLength:])
			copy(newBuffer[newPrefixBytes:newPrefixBytes+insertBytePos-oldPrefixBytes],
				encoder.buffer[oldPrefixBytes:insertBytePos])
			for i := oldPrefixBytes; i < newPrefixBytes; i++ {
				newBuffer[i] = 0
			}
		}
	} else {
		if needsRealloc {
			copy(newBuffer[:insertBytePos], encoder.buffer[:insertBytePos])
		} else {
			copy(newBuffer[insertBytePos+newElementLength+adjustedNextElementLength:],
				encoder.buffer[insertBytePos+nextElementLength:])
		}
	}

	if !needsRealloc && prefixBytesChange == 0 {
		copy(newBuffer[:oldPrefixBytes], encoder.buffer[:oldPrefixBytes])
	} else if needsRealloc && prefixBytesChange == 0 {
		copy(newBuffer[:oldPrefixBytes], encoder.buffer[:oldPrefixBytes])
	}

	newElementInsertPos := newPrefixBytes + (insertBytePos - oldPrefixBytes)
	switch newElementLength {
	case 1:
		newBuffer[newElementInsertPos] = byte(newElementDelta)
	case 2:
		newBuffer[newElementInsertPos] = byte(newElementDelta)
		newBuffer[newElementInsertPos+1] = byte(newElementDelta >> 8)
	case 3:
		newBuffer[newElementInsertPos] = byte(newElementDelta)
		newBuffer[newElementInsertPos+1] = byte(newElementDelta >> 8)
		newBuffer[newElementInsertPos+2] = byte(newElementDelta >> 16)
	case 4:
		newBuffer[newElementInsertPos] = byte(newElementDelta)
		newBuffer[newElementInsertPos+1] = byte(newElementDelta >> 8)
		newBuffer[newElementInsertPos+2] = byte(newElementDelta >> 16)
		newBuffer[newElementInsertPos+3] = byte(newElementDelta >> 24)
	}

	adjustedNextInsertPos := newElementInsertPos + newElementLength
	switch adjustedNextElementLength {
	case 1:
		newBuffer[adjustedNextInsertPos] = byte(adjustedNextDelta)
	case 2:
		newBuffer[adjustedNextInsertPos] = byte(adjustedNextDelta)
		newBuffer[adjustedNextInsertPos+1] = byte(adjustedNextDelta >> 8)
	case 3:
		newBuffer[adjustedNextInsertPos] = byte(adjustedNextDelta)
		newBuffer[adjustedNextInsertPos+1] = byte(adjustedNextDelta >> 8)
		newBuffer[adjustedNextInsertPos+2] = byte(adjustedNextDelta >> 16)
	case 4:
		newBuffer[adjustedNextInsertPos] = byte(adjustedNextDelta)
		newBuffer[adjustedNextInsertPos+1] = byte(adjustedNextDelta >> 8)
		newBuffer[adjustedNextInsertPos+2] = byte(adjustedNextDelta >> 16)
		newBuffer[adjustedNextInsertPos+3] = byte(adjustedNextDelta >> 24)
	}

	if needsRealloc {
		copy(newBuffer[adjustedNextInsertPos+adjustedNextElementLength:],
			encoder.buffer[nextElementBytePos+nextElementLength:])
	}

	newElemPrefixByteIdx := nextElementIndex / 4
	newElemBitOffset := (nextElementIndex % 4) * 2
	newElemLengthCode := byte(newElementLength - 1)

	shiftBitsRightIterative(newBuffer[:newPrefixBytes], newElemPrefixByteIdx*8+newElemBitOffset)
	newBuffer[newElemPrefixByteIdx] |= newElemLengthCode << newElemBitOffset

	adjustedNextPrefixByteIdx := (nextElementIndex + 1) / 4
	adjustedNextBitOffset := ((nextElementIndex + 1) % 4) * 2
	adjustedNextLengthCode := byte(adjustedNextElementLength - 1)

	if adjustedNextPrefixByteIdx < newPrefixBytes {
		newBuffer[adjustedNextPrefixByteIdx] &= ^(0x03 << adjustedNextBitOffset)
		newBuffer[adjustedNextPrefixByteIdx] |= adjustedNextLengthCode << adjustedNextBitOffset
	}

	encoder.buffer = newBuffer
}

func (encoder *PrefixEncoder) grow(to int) {
	if cap(encoder.buffer) < to {
		buffer := encoder.buffer
		encoder.buffer = make([]byte, to)
		copy(encoder.buffer, buffer)
	} else {
		encoder.buffer = encoder.buffer[:to]
	}
}

func shiftBitsRightIterative(data []byte, startBit int) {
	if len(data) == 0 {
		return
	}

	totalBits := len(data) * 8
	if startBit >= totalBits {
		return
	}

	mask := byte(192)

	startByteIndex := startBit / 8

	for byteIndex := len(data) - 1; byteIndex >= startByteIndex; byteIndex-- {
		currentByte := data[byteIndex]

		if byteIndex == startByteIndex {
			bitsToPreserve := startBit % 8

			if bitsToPreserve > 0 {
				preserveMask := byte(0xFF >> (8 - bitsToPreserve))
				preservedBits := currentByte & preserveMask

				bitsToShift := currentByte & (0xFF << bitsToPreserve)
				tailBits := bitsToShift & mask
				shiftedBits := bitsToShift << 2

				data[byteIndex] = preservedBits | shiftedBits

				if byteIndex+1 < len(data) {
					data[byteIndex+1] = (data[byteIndex+1] | (tailBits >> 6))
				}
			} else {
				tailBits := currentByte & mask
				data[byteIndex] = currentByte << 2

				if byteIndex+1 < len(data) {
					data[byteIndex+1] = data[byteIndex+1] | (tailBits >> 6)
				}
			}
		} else {
			tailBits := currentByte & mask

			data[byteIndex] = currentByte << 2

			if byteIndex+1 < len(data) {
				data[byteIndex+1] = data[byteIndex+1] | (tailBits >> 6)
			}
		}
	}
}

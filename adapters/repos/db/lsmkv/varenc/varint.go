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

package varenc

import (
	"encoding/binary"
	"math/bits"
)

func decodeReusable(deltas []uint64, packed []byte, deltaDiff bool) {
	if len(packed) < 8 {
		return // Error handling: insufficient input or output space
	}

	// Read the first delta using BigEndian to handle byte order explicitly
	deltas[0] = binary.BigEndian.Uint64(packed[0:8])

	// Read bitsNeeded (6 bits starting from bit 2 of packed[8])
	bitsNeeded := int((packed[8] >> 2) & 0x3F)
	if bitsNeeded == 0 || bitsNeeded > 64 {
		// Handle invalid bitsNeeded
		return
	}

	// Starting bit position after reading bitsNeeded
	bitPos := 6
	bytePos := 8 // Start from packed[8]

	// Initialize the bit buffer with the remaining bits in packed[8], if any
	bitsLeft := 8 - bitPos
	bitBuffer := uint64(packed[bytePos] & ((1 << bitsLeft) - 1))

	bytePos++

	// Precompute the mask for bitsNeeded bits
	bitsMask := uint64((1 << bitsNeeded) - 1)

	// Read the deltas
	for i := 1; i < len(deltas); i++ {
		// Ensure we have enough bits in the buffer
		for bitsLeft < bitsNeeded {
			if bytePos >= len(packed) {
				// Handle insufficient data
				return
			}
			bitBuffer = (bitBuffer << 8) | uint64(packed[bytePos])
			bitsLeft += 8
			bytePos++
		}
		// Extract bitsNeeded bits from the buffer
		bitsLeft -= bitsNeeded
		deltas[i] = (bitBuffer >> bitsLeft) & bitsMask
		if deltaDiff {
			deltas[i] += deltas[i-1]
		}
	}
}

func encodeReusable(deltas []uint64, packed []byte, deltaDiff bool) int {
	var currentByte byte
	bitPos := 0 // Tracks the current bit position in the byte

	bitsNeeded := 0

	binary.BigEndian.PutUint64(packed, deltas[0])
	currentByteIndex := 8

	for i, delta := range deltas[1:] {
		if deltaDiff {
			delta -= deltas[i]
		}
		// Determine the number of bits needed to represent this delta
		if bitsNeeded < bits.Len64(delta) {
			bitsNeeded = bits.Len64(delta)
		}
	}
	if bitsNeeded == 0 {
		bitsNeeded = 1 // Ensure we use at least 1 bit for 0 values
	}

	bitsToStore := uint64(bitsNeeded)

	currentByte |= byte((bitsToStore>>(5-bitPos))&1) << (7 - bitPos)
	bitPos++
	currentByte |= byte((bitsToStore>>(5-bitPos))&1) << (7 - bitPos)
	bitPos++
	currentByte |= byte((bitsToStore>>(5-bitPos))&1) << (7 - bitPos)
	bitPos++
	currentByte |= byte((bitsToStore>>(5-bitPos))&1) << (7 - bitPos)
	bitPos++
	currentByte |= byte((bitsToStore>>(5-bitPos))&1) << (7 - bitPos)
	bitPos++
	currentByte |= byte((bitsToStore>>(5-bitPos))&1) << (7 - bitPos)
	bitPos++

	for i, delta := range deltas[1:] {
		if deltaDiff {
			delta -= deltas[i]
		}
		// Pack the number of bits (using 6 bits for the bit length)
		bitsNeededInteral := bitsNeeded
		// Pack the bits of this delta into the byte slice
		for bitsNeededInteral > 0 {
			if bitPos == 8 {
				// Move to a new byte when the current one is full
				packed[currentByteIndex] = currentByte
				currentByte = 0
				bitPos = 0
				currentByteIndex++
			}

			// Calculate how many bits can be written to the current byte
			bitsToWrite := 8 - bitPos
			if bitsNeededInteral < bitsToWrite {
				bitsToWrite = bitsNeededInteral
			}

			// Write bits from delta to current byte
			currentByte |= byte((delta>>(bitsNeededInteral-bitsToWrite))&((1<<bitsToWrite)-1)) << (8 - bitPos - bitsToWrite)
			bitPos += bitsToWrite
			bitsNeededInteral -= bitsToWrite
		}
	}

	// If there are remaining bits in the current byte, append it
	if bitPos > 0 {
		packed[currentByteIndex] = currentByte
	}

	return currentByteIndex + 1
}

type VarIntEncoder struct {
	values []uint64
	buf    []byte
}

func (e *VarIntEncoder) Init(expectedCount int) {
	if len(e.values) < expectedCount {
		e.values = make([]uint64, expectedCount)
	}
	if len(e.buf) < 8+8*expectedCount {
		e.buf = make([]byte, 8+8*expectedCount)
	}
}

func (e VarIntEncoder) EncodeReusable(values []uint64, buf []byte) {
	encodeReusable(values, buf, false)
}

func (e VarIntEncoder) DecodeReusable(data []byte, values []uint64) {
	decodeReusable(values, data, false)
}

func (e *VarIntEncoder) Encode(values []uint64) []byte {
	n := encodeReusable(values, e.buf, false)
	output := make([]byte, n)
	copy(output, e.buf[:n])
	return output
}

func (e *VarIntEncoder) Decode(data []byte) []uint64 {
	decodeReusable(e.values, data, false)
	return e.values
}

type VarIntDeltaEncoder struct {
	values []uint64
	buf    []byte
}

func (e *VarIntDeltaEncoder) Init(expectedCount int) {
	if len(e.values) < expectedCount {
		e.values = make([]uint64, expectedCount)
	}
	if len(e.buf) < 8+8*expectedCount {
		e.buf = make([]byte, 8+8*expectedCount)
	}
}

func (e VarIntDeltaEncoder) EncodeReusable(values []uint64, buf []byte) {
	encodeReusable(values, buf, true)
}

func (e VarIntDeltaEncoder) DecodeReusable(data []byte, values []uint64) {
	decodeReusable(values, data, true)
}

func (e *VarIntDeltaEncoder) Encode(values []uint64) []byte {
	n := encodeReusable(values, e.buf, true)
	output := make([]byte, n)
	copy(output, e.buf[:n])
	return output
}

func (e *VarIntDeltaEncoder) Decode(data []byte) []uint64 {
	decodeReusable(e.values, data, true)
	return e.values
}

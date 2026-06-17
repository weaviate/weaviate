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

package varenc

import (
	"encoding/binary"
	"math/bits"
)

func decodeReusable(deltas []uint64, packed []byte, deltaDiff bool) {
	if len(deltas) == 0 || len(packed) < 9 {
		return // Error handling: insufficient input or output space
	}

	// Read the first delta using BigEndian to handle byte order explicitly
	deltas[0] = binary.BigEndian.Uint64(packed[0:8])

	n := len(deltas)
	if n <= 1 {
		return
	}

	// Read bitsNeeded (6 bits starting from bit 2 of packed[8])
	bitsNeeded := int((packed[8] >> 2) & 0x3F)
	if bitsNeeded == 0 || bitsNeeded > 64 {
		// Handle invalid bitsNeeded
		return
	}

	bnu := uint(bitsNeeded)
	bitsMask := uint64(1)<<bnu - 1

	bits := packed[8:]
	bitsLen := len(bits)

	// High-aligned bit reservoir:
	//   - `buf` holds the next bits to extract in its MSB; the very next bit
	//     to read is at position 63.
	//   - `bitsAvail` counts valid bits, which occupy positions 63..64-bitsAvail.
	//   - Extract: value = buf >> (64 - bnu); buf <<= bnu; bitsAvail -= bnu.
	//   - Refill: buf |= newBits << (64 - bitsAvail - chunkBits).
	//
	// The 6-bit header was consumed via `packed[8]>>2`; the low 2 bits of
	// bits[0] are the start of the first delta and seed the reservoir.
	buf := uint64(bits[0]&0x3) << 62
	bitsAvail := 2
	bytePos := 1

	if bnu <= 32 {
		// Refill 32 bits per chunk. We refill only when bitsAvail < bnu, so
		// bitsAvail is at most 31 when we refill (bnu <= 32), keeping
		// bitsAvail+32 <= 63 — safe to store in the reservoir.
		if deltaDiff {
			prev := deltas[0]
			i := 1
			for ; i < n; i++ {
				if bitsAvail < int(bnu) {
					if bytePos+4 > bitsLen {
						break
					}
					buf |= uint64(binary.BigEndian.Uint32(bits[bytePos:])) << uint(32-bitsAvail)
					bytePos += 4
					bitsAvail += 32
				}
				prev += buf >> (64 - bnu)
				buf <<= bnu
				bitsAvail -= int(bnu)
				deltas[i] = prev
			}
			decodeTailDelta(deltas, bits, bytePos, bitsLen, buf, bitsAvail, bnu, i, n, prev)
		} else {
			i := 1
			for ; i < n; i++ {
				if bitsAvail < int(bnu) {
					if bytePos+4 > bitsLen {
						break
					}
					buf |= uint64(binary.BigEndian.Uint32(bits[bytePos:])) << uint(32-bitsAvail)
					bytePos += 4
					bitsAvail += 32
				}
				deltas[i] = buf >> (64 - bnu)
				buf <<= bnu
				bitsAvail -= int(bnu)
			}
			decodeTail(deltas, bits, bytePos, bitsLen, buf, bitsAvail, bnu, i, n)
		}
		return
	}

	// bnu > 32: per-iteration uint64 load. The reservoir state above is unused
	// here; we recompute from bitPos.
	//
	// Fast path: read 8 bytes at a time and extract bitsNeeded bits with a
	// single shift+mask. Safe only when bnu <= 57 (so that bitOffset+bnu <= 64
	// for any bitOffset in [0,7]) and when we have 8 bytes available.
	bitPos := uint(6)
	fastEnd := 1
	if bnu <= 57 && bitsLen >= 8 {
		room := 8*bitsLen - 63
		fastEnd = 2 + room/bitsNeeded
		if fastEnd > n {
			fastEnd = n
		}
	}

	if deltaDiff {
		prev := deltas[0]
		for i := 1; i < fastEnd; i++ {
			bytePos := bitPos >> 3
			bitOffset := bitPos & 7
			val := binary.BigEndian.Uint64(bits[bytePos:])
			d := (val >> (64 - bitOffset - bnu)) & bitsMask
			prev += d
			deltas[i] = prev
			bitPos += bnu
		}
	} else {
		for i := 1; i < fastEnd; i++ {
			bytePos := bitPos >> 3
			bitOffset := bitPos & 7
			val := binary.BigEndian.Uint64(bits[bytePos:])
			deltas[i] = (val >> (64 - bitOffset - bnu)) & bitsMask
			bitPos += bnu
		}
	}

	if fastEnd >= n {
		return
	}

	// Tail: byte-by-byte for the remaining values where a uint64 read would
	// run past the end of the input.
	bytePosT := int(bitPos >> 3)
	if bytePosT >= bitsLen {
		return
	}
	bitOffset := int(bitPos & 7)
	bitsLeft := 8 - bitOffset
	bitBuffer := uint64(bits[bytePosT]) & (uint64(1)<<uint(bitsLeft) - 1)
	bytePosT++

	for i := fastEnd; i < n; i++ {
		for bitsLeft < bitsNeeded {
			if bytePosT >= bitsLen {
				return
			}
			bitBuffer = (bitBuffer << 8) | uint64(bits[bytePosT])
			bitsLeft += 8
			bytePosT++
		}
		bitsLeft -= bitsNeeded
		deltas[i] = (bitBuffer >> uint(bitsLeft)) & bitsMask
		if deltaDiff {
			deltas[i] += deltas[i-1]
		}
	}
}

// decodeTail finishes the reservoir-style decode for the last few values, where
// a 32-bit refill would run past the end of the input. Refills one byte at a
// time. Called from the bnu <= 32 fast paths.
func decodeTail(deltas []uint64, bits []byte, bytePos, bitsLen int, buf uint64, bitsAvail int, bnu uint, i, n int) {
	for ; i < n; i++ {
		for bitsAvail < int(bnu) {
			if bytePos >= bitsLen {
				return
			}
			buf |= uint64(bits[bytePos]) << uint(56-bitsAvail)
			bytePos++
			bitsAvail += 8
		}
		deltas[i] = buf >> (64 - bnu)
		buf <<= bnu
		bitsAvail -= int(bnu)
	}
}

// decodeTailDelta is the delta variant of decodeTail; prev (the last decoded
// value) is consumed, not returned, since the caller has no values left after
// the tail.
func decodeTailDelta(deltas []uint64, bits []byte, bytePos, bitsLen int, buf uint64, bitsAvail int, bnu uint, i, n int, prev uint64) {
	p := prev
	for ; i < n; i++ {
		for bitsAvail < int(bnu) {
			if bytePos >= bitsLen {
				return
			}
			buf |= uint64(bits[bytePos]) << uint(56-bitsAvail)
			bytePos++
			bitsAvail += 8
		}
		p += buf >> (64 - bnu)
		buf <<= bnu
		bitsAvail -= int(bnu)
		deltas[i] = p
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

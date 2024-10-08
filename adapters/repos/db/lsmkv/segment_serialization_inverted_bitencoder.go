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

package lsmkv

import (
	"encoding/binary"
	"math/bits"
)

// Delta Encode: Compute the deltas of the input values
func deltaEncode(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	deltas := make([]uint64, len(values))
	deltas[0] = values[0] // First value is kept as is
	for i := 1; i < len(values); i++ {
		deltas[i] = values[i] - values[i-1] // Store difference
	}
	return deltas
}

// Delta Decode: Reverse the delta encoding to recover original values
func deltaDecode(deltas []uint64) []uint64 {
	if len(deltas) == 0 {
		return nil
	}
	for i := 1; i < len(deltas); i++ {
		deltas[i] = deltas[i-1] + deltas[i]
	}
	return deltas
}

// Pack Deltas: Pack delta values into a byte slice using the minimum number of bits
func packDeltas(deltas []uint64) []byte {
	var packed []byte
	var currentByte byte
	bitPos := 0 // Tracks the current bit position in the byte

	bitsNeeded := 0

	packed = append(packed, make([]byte, 8)...)

	binary.BigEndian.PutUint64(packed, deltas[0])

	for _, delta := range deltas[1:] {
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

	for _, delta := range deltas[1:] {
		// Pack the number of bits (using 6 bits for the bit length)
		bitsNeededInteral := bitsNeeded
		// Pack the bits of this delta into the byte slice
		for bitsNeededInteral > 0 {
			if bitPos == 8 {
				// Move to a new byte when the current one is full
				packed = append(packed, currentByte)
				currentByte = 0
				bitPos = 0
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
		packed = append(packed, currentByte)
	}

	return packed
}

func unpackDeltas(packed []byte, deltasCount int) []uint64 {
	deltas := make([]uint64, deltasCount)
	bitPos := 0
	currentByteIndex := 0
	bitsNeeded := 0

	deltas[0] = binary.BigEndian.Uint64(packed[:8])

	currentByteIndex += 8
	currentByte := packed[currentByteIndex]

	bitsNeeded = (bitsNeeded << 1) | int((currentByte>>(7-bitPos))&1)
	bitPos++
	bitsNeeded = (bitsNeeded << 1) | int((currentByte>>(7-bitPos))&1)
	bitPos++
	bitsNeeded = (bitsNeeded << 1) | int((currentByte>>(7-bitPos))&1)
	bitPos++
	bitsNeeded = (bitsNeeded << 1) | int((currentByte>>(7-bitPos))&1)
	bitPos++
	bitsNeeded = (bitsNeeded << 1) | int((currentByte>>(7-bitPos))&1)
	bitPos++
	bitsNeeded = (bitsNeeded << 1) | int((currentByte>>(7-bitPos))&1)
	bitPos++

	for i := 1; i < deltasCount; i++ {
		bitsNeededInternal := bitsNeeded
		// Read the delta bits based on the bitsNeeded
		for bitsNeededInternal > 0 {
			if currentByteIndex >= len(packed) {
				break
			}

			if bitPos == 8 {
				currentByteIndex++
				bitPos = 0
				if currentByteIndex >= len(packed) {
					break
				}
				currentByte = packed[currentByteIndex]

			}

			// Calculate how many bits to read from the current byte
			bitsToRead := 8 - bitPos
			if bitsToRead > bitsNeeded {
				bitsToRead = bitsNeeded
			}
			if bitsNeededInternal < bitsToRead {
				bitsToRead = bitsNeededInternal
			}

			// Extract bits from the packed byte
			shiftAmount := 8 - bitPos - bitsToRead
			deltas[i] |= (uint64(currentByte>>shiftAmount) & uint64((1<<bitsToRead)-1)) << (bitsNeededInternal - bitsToRead)

			bitPos += bitsToRead

			bitsNeededInternal -= bitsToRead
		}

	}

	return deltas
}

func packedEncode(docIds, termFreqs, propLengths []uint64) *blockData {
	docIdsDeltas := deltaEncode(docIds)
	docIdsPacked := packDeltas(docIdsDeltas)
	termFreqsPacked := packDeltas(termFreqs)
	propLengthsPacked := packDeltas(propLengths)

	return &blockData{
		docIds:      docIdsPacked,
		tfs:         termFreqsPacked,
		propLenghts: propLengthsPacked,
	}
}

func packedDecode(values *blockData, numValues int) ([]uint64, []uint64, []uint64) {
	docIds := deltaDecode(unpackDeltas(values.docIds, numValues))
	termFreqs := unpackDeltas(values.tfs, numValues)
	propLengths := unpackDeltas(values.propLenghts, numValues)
	return docIds, termFreqs, propLengths
}

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
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var BLOCK_SIZE_TEST = 128

func baselineEncode(docIds, termFreqs, propLengths []uint64) []byte {
	buffer := make([]byte, len(docIds)*20)
	offset := 0
	for i := range docIds {
		binary.LittleEndian.PutUint16(buffer[offset:], 8)
		offset += 2
		binary.BigEndian.PutUint64(buffer[offset:], docIds[i])
		offset += 8
		binary.LittleEndian.PutUint16(buffer[offset:], 4)
		offset += 2
		binary.LittleEndian.PutUint32(buffer[offset:], math.Float32bits(float32(termFreqs[i])))
		offset += 4
		binary.LittleEndian.PutUint32(buffer[offset:], math.Float32bits(float32(propLengths[i])))
		offset += 4
	}
	return buffer
}

func baselineDecode(values []byte) ([]uint64, []float32, []float32) {
	docIds := make([]uint64, BLOCK_SIZE_TEST)
	termFreqs := make([]float32, BLOCK_SIZE_TEST)
	propLengths := make([]float32, BLOCK_SIZE_TEST)
	offset := 0
	for offset < len(values) {
		docIds[offset/20] = binary.BigEndian.Uint64(values[offset+2:])
		termFreqs[offset/20] = math.Float32frombits(binary.LittleEndian.Uint32(values[offset+12:]))
		propLengths[offset/20] = math.Float32frombits(binary.LittleEndian.Uint32(values[offset+16:]))
		offset += 20
	}
	return docIds, termFreqs, propLengths
}

func BenchmarkBits(m *testing.B) {
	// Example input values
	docIds := make([]uint64, BLOCK_SIZE_TEST)
	termFreqs := make([]uint64, BLOCK_SIZE_TEST)
	propLengths := make([]uint64, BLOCK_SIZE_TEST)

	timeBaseline := 0
	timePacked := 0

	sizeBaseline := 0
	sizePacked := 0

	// do 1000 iterations
	for j := 0; j < 10000; j++ {

		for i := range docIds {
			docIds[i] = uint64(100 + i)
			// round to nearest integer
			termFreqs[i] = uint64(math.Round(rand.Float64()*10)) + 1
			propLengths[i] = uint64(math.Round(rand.Float64()*100)) + 1
		}

		// Baseline encoding
		encoded := baselineEncode(docIds, termFreqs, propLengths)
		startTime := time.Now()

		decodedDocIds, decodedTermFreqs, decodedPropLengths := baselineDecode(encoded)
		stopTime := time.Now()

		timeBaseline += int(stopTime.Sub(startTime))

		// Packed encoding
		encoded2 := packedEncode(docIds, termFreqs, propLengths)
		startTime = time.Now()

		decodedDocIds2, decodedTermFreqs2, decodedPropLengths2 := packedDecode(encoded2, len(docIds))
		stopTime = time.Now()

		timePacked += int(stopTime.Sub(startTime))

		sizeBaseline += len(encoded)
		sizePacked += len(encoded2)

		for i := range docIds {
			assert.Equal(m, docIds[i], decodedDocIds[i])
			assert.Equal(m, termFreqs[i], uint64(decodedTermFreqs[i]))
			assert.Equal(m, propLengths[i], uint64(decodedPropLengths[i]))

			assert.Equal(m, docIds[i], decodedDocIds2[i])
			assert.Equal(m, termFreqs[i], decodedTermFreqs2[i])
			assert.Equal(m, propLengths[i], decodedPropLengths2[i])
		}
	}

	// Print the results
	m.Logf("Time: %v, size: %v\n", float32(timePacked)/float32(timeBaseline), float32(sizeBaseline)/float32(sizePacked))
}

func BenchmarkDecoder(b *testing.B) {
	// Example input values
	docIds := make([]uint64, BLOCK_SIZE_TEST)
	termFreqs := make([]uint64, BLOCK_SIZE_TEST)
	propLengths := make([]uint64, BLOCK_SIZE_TEST)

	for i := range docIds {
		docIds[i] = uint64(100 + i)
		// round to nearest integer
		termFreqs[i] = uint64(math.Round(rand.Float64()*10)) + 1
		propLengths[i] = uint64(math.Round(rand.Float64()*10)) + 1
	}

	encoded2 := packedEncode(docIds, termFreqs, propLengths)
	for j := 0; j < 40000000; j++ {
		packedDecode(encoded2, len(docIds))
	}
}

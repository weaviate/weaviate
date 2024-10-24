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
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
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

func baselineDecodeReusable(values []byte, docIds []uint64, termFreqs, propLengths []float32) {
	offset := 0
	for offset < len(values) {
		docIds[offset/20] = binary.BigEndian.Uint64(values[offset+2:])
		termFreqs[offset/20] = math.Float32frombits(binary.LittleEndian.Uint32(values[offset+12:]))
		propLengths[offset/20] = math.Float32frombits(binary.LittleEndian.Uint32(values[offset+16:]))
		offset += 20
	}
}

func varintEncodeOne(numbers []uint64) []byte {
	buf := make([]byte, 0, len(numbers)*binary.MaxVarintLen64)
	for _, num := range numbers {
		// Create a temporary buffer for this number
		temp := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(temp, num)
		buf = append(buf, temp[:n]...)
	}
	return buf
}

func varintEncode(docIds, termFreqs, propLengths []uint64) []byte {
	docIdsBuf := varintEncodeOne(deltaEncode(docIds))
	termFreqsBuf := varintEncodeOne(termFreqs)
	propLengthsBuf := varintEncodeOne(propLengths)

	buffer := make([]byte, len(docIdsBuf)+len(termFreqsBuf)+len(propLengthsBuf)+6)
	offset := 0
	binary.LittleEndian.PutUint16(buffer[offset:], uint16(len(docIdsBuf)))
	offset += 2
	copy(buffer[offset:], docIdsBuf)
	offset += len(docIdsBuf)
	binary.LittleEndian.PutUint16(buffer[offset:], uint16(len(termFreqsBuf)))
	offset += 2
	copy(buffer[offset:], termFreqsBuf)
	offset += len(termFreqsBuf)
	binary.LittleEndian.PutUint16(buffer[offset:], uint16(len(propLengthsBuf)))
	offset += 2
	copy(buffer[offset:], propLengthsBuf)
	return buffer
}

func varintDecodeReusable(values []byte, docIds, termFreqs, propLengths []uint64) {
	offset := 0
	docIdsLen := binary.LittleEndian.Uint16(values)
	offset += 2
	for i := 0; i < int(docIdsLen); i++ {
		num, n := binary.Uvarint(values[offset:])
		if i == 0 {
			docIds[i] = num
		} else {
			docIds[i] = num + docIds[i-1]
		}
		offset += n
	}
	termFreqsLen := binary.LittleEndian.Uint16(values[offset:])
	offset += 2
	for i := 0; i < int(termFreqsLen); i++ {
		num, n := binary.Uvarint(values[offset:])
		termFreqs[i] = num
		offset += n
	}
	propLengthsLen := binary.LittleEndian.Uint16(values[offset:])
	offset += 2
	for i := 0; i < int(propLengthsLen); i++ {
		num, n := binary.Uvarint(values[offset:])
		propLengths[i] = num
		offset += n
	}
}

func TestBaseline(t *testing.T) {
	// Example input values
	docIds := make([]uint64, BLOCK_SIZE_TEST)
	termFreqs := make([]uint64, BLOCK_SIZE_TEST)
	propLengths := make([]uint64, BLOCK_SIZE_TEST)

	timeBaseline := 0
	timePacked := 0

	sizeBaseline := 0
	sizePacked := 0

	blockDataDecode := &terms.BlockDataDecoded{
		DocIds:      make([]uint64, BLOCK_SIZE_TEST),
		Tfs:         make([]uint64, BLOCK_SIZE_TEST),
		PropLenghts: make([]uint64, BLOCK_SIZE_TEST),
	}

	DocIds := make([]uint64, BLOCK_SIZE_TEST)
	Tfs := make([]float32, BLOCK_SIZE_TEST)
	PropLenghts := make([]float32, BLOCK_SIZE_TEST)
	// do 1000 iterations
	for j := 0; j < 100; j++ {

		for i := range docIds {
			docIds[i] = uint64(100 + i)
			// round to nearest integer
			termFreqs[i] = uint64(math.Round(rand.Float64()*10)) + 1
			propLengths[i] = uint64(math.Round(rand.Float64()*100)) + 1
		}

		// Baseline encoding
		encoded := baselineEncode(docIds, termFreqs, propLengths)
		startTime := time.Now()

		baselineDecodeReusable(encoded, DocIds, Tfs, PropLenghts)
		stopTime := time.Now()

		timeBaseline += int(stopTime.Sub(startTime))

		// Packed encoding
		encoded2 := packedEncode(docIds, termFreqs, propLengths)

		startTime = time.Now()

		packedDecodeReusable(encoded2, len(docIds), blockDataDecode)
		stopTime = time.Now()

		timePacked += int(stopTime.Sub(startTime))

		sizeBaseline += len(encoded)
		sizePacked += encoded2.Size()

		for i := range docIds {
			assert.Equal(t, docIds[i], DocIds[i])
			assert.Equal(t, termFreqs[i], uint64(Tfs[i]))
			assert.Equal(t, propLengths[i], uint64(PropLenghts[i]))

			assert.Equal(t, docIds[i], blockDataDecode.DocIds[i])
			assert.Equal(t, termFreqs[i], blockDataDecode.Tfs[i])
			assert.Equal(t, propLengths[i], blockDataDecode.PropLenghts[i])
		}
	}

	// Print the results
	t.Logf("Time: %v, size: %v\n", float32(timePacked)/float32(timeBaseline), float32(sizeBaseline)/float32(sizePacked))
}

func TestVarint(t *testing.T) {
	// Example input values
	docIds := make([]uint64, BLOCK_SIZE_TEST)
	termFreqs := make([]uint64, BLOCK_SIZE_TEST)
	propLengths := make([]uint64, BLOCK_SIZE_TEST)

	timeBaseline := 0
	timePacked := 0

	sizeBaseline := 0
	sizePacked := 0

	blockDataDecode := &terms.BlockDataDecoded{
		DocIds:      make([]uint64, BLOCK_SIZE_TEST),
		Tfs:         make([]uint64, BLOCK_SIZE_TEST),
		PropLenghts: make([]uint64, BLOCK_SIZE_TEST),
	}
	blockDataVarint := &terms.BlockDataDecoded{
		DocIds:      make([]uint64, BLOCK_SIZE_TEST),
		Tfs:         make([]uint64, BLOCK_SIZE_TEST),
		PropLenghts: make([]uint64, BLOCK_SIZE_TEST),
	}

	// do 1000 iterations
	for j := 0; j < 100; j++ {

		for i := range docIds {
			docIds[i] = uint64(100 + i)
			// round to nearest integer
			termFreqs[i] = uint64(math.Round(rand.Float64()*10)) + 1
			propLengths[i] = uint64(math.Round(rand.Float64()*100)) + 1
		}

		// Baseline encoding
		encoded := varintEncode(docIds, termFreqs, propLengths)
		startTime := time.Now()

		varintDecodeReusable(encoded, blockDataVarint.DocIds, blockDataVarint.Tfs, blockDataVarint.PropLenghts)
		stopTime := time.Now()

		timeBaseline += int(stopTime.Sub(startTime))

		// Packed encoding
		encoded2 := packedEncode(docIds, termFreqs, propLengths)

		startTime = time.Now()

		packedDecodeReusable(encoded2, len(docIds), blockDataDecode)
		stopTime = time.Now()

		timePacked += int(stopTime.Sub(startTime))

		sizeBaseline += len(encoded)
		sizePacked += encoded2.Size()

		for i := range docIds {
			assert.Equal(t, docIds[i], blockDataVarint.DocIds[i])
			assert.Equal(t, termFreqs[i], blockDataVarint.Tfs[i])
			assert.Equal(t, propLengths[i], blockDataVarint.PropLenghts[i])

			assert.Equal(t, docIds[i], blockDataDecode.DocIds[i])
			assert.Equal(t, termFreqs[i], blockDataDecode.Tfs[i])
			assert.Equal(t, propLengths[i], blockDataDecode.PropLenghts[i])
		}
	}

	// Print the results
	t.Logf("Time: %v, size: %v\n", float32(timePacked)/float32(timeBaseline), float32(sizeBaseline)/float32(sizePacked))
}

func BenchmarkBits(m *testing.B) {
	// Example input values
	docIds := make([]uint64, BLOCK_SIZE_TEST)
	termFreqs := make([]uint64, BLOCK_SIZE_TEST)
	propLengths := make([]uint64, BLOCK_SIZE_TEST)

	timeBaseline := 0.0
	timePacked := 0.0

	sizeBaseline := 0
	sizePacked := 0

	blockDataDecode := &terms.BlockDataDecoded{
		DocIds:      make([]uint64, BLOCK_SIZE_TEST),
		Tfs:         make([]uint64, BLOCK_SIZE_TEST),
		PropLenghts: make([]uint64, BLOCK_SIZE_TEST),
	}

	DocIds := make([]uint64, BLOCK_SIZE_TEST)
	Tfs := make([]float32, BLOCK_SIZE_TEST)
	PropLenghts := make([]float32, BLOCK_SIZE_TEST)
	iterations := 100000
	// do 1000 iterations
	for j := 0; j < iterations; j++ {

		for i := range docIds {
			docIds[i] = uint64(100 + i)
			// round to nearest integer
			termFreqs[i] = uint64(math.Round(rand.Float64()*10)) + 1
			propLengths[i] = uint64(math.Round(rand.Float64()*100)) + 1
		}

		// Baseline encoding
		encoded := baselineEncode(docIds, termFreqs, propLengths)
		startTime := time.Now()

		baselineDecodeReusable(encoded, DocIds, Tfs, PropLenghts)
		stopTime := time.Now()

		timeBaseline += stopTime.Sub(startTime).Seconds()

		// Packed encoding
		encoded2 := packedEncode(docIds, termFreqs, propLengths)

		startTime = time.Now()

		packedDecodeReusable(encoded2, len(docIds), blockDataDecode)
		stopTime = time.Now()

		timePacked += stopTime.Sub(startTime).Seconds()

		sizeBaseline += len(encoded)
		sizePacked += encoded2.Size()

		for i := range docIds {
			assert.Equal(m, docIds[i], DocIds[i])
			assert.Equal(m, termFreqs[i], uint64(Tfs[i]))
			assert.Equal(m, propLengths[i], uint64(PropLenghts[i]))

			assert.Equal(m, docIds[i], blockDataDecode.DocIds[i])
			assert.Equal(m, termFreqs[i], blockDataDecode.Tfs[i])
			assert.Equal(m, propLengths[i], blockDataDecode.PropLenghts[i])
		}
	}

	// Print the results
	m.Logf("Time: %v, size: %v speed: %v\n", float32(timePacked)/float32(timeBaseline), float32(sizeBaseline)/float32(sizePacked), float32((iterations*BLOCK_SIZE_TEST)/1000000)/float32(timePacked))
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

	buffer := &terms.BlockDataDecoded{
		DocIds:      make([]uint64, BLOCK_SIZE_TEST),
		Tfs:         make([]uint64, BLOCK_SIZE_TEST),
		PropLenghts: make([]uint64, BLOCK_SIZE_TEST),
	}
	encoded2 := packedEncode(docIds, termFreqs, propLengths)

	iterations := 40000000
	b.ResetTimer()
	for j := 0; j < iterations; j++ {
		packedDecodeReusable(encoded2, len(docIds), buffer)
	}

	b.StopTimer()
	b.Logf("Time: %v, speed: %v\n", b.Elapsed().Seconds(), float32((iterations*BLOCK_SIZE_TEST)/1000000)/float32(b.Elapsed().Seconds()))
}

func TestMapList(m *testing.T) {
	collectionSize := BLOCK_SIZE_TEST*7 + 15

	currentUncompressedSize := collectionSize * 29 // non-tombstone records have 29 bytes
	bestUncompressedSize := collectionSize * 16    // best possible uncompressed size for non-tombstone records 8 for key, 8 for value

	mapList := make([]MapPair, collectionSize)

	for i := range mapList {
		docId := uint64(100 + i*10)

		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, docId)

		tf := float32(math.Round(rand.Float64()*100)) + 1
		pl := float32(math.Round(rand.Float64()*1000)) + 1

		value := make([]byte, 8)
		binary.LittleEndian.PutUint32(value, math.Float32bits(tf))
		binary.LittleEndian.PutUint32(value[4:], math.Float32bits(pl))

		mapList[i] = MapPair{
			Key:   key,
			Value: value,
		}
	}

	blockEntries, blockDatas, _ := createBlocks(mapList)
	blocksEncoded := encodeBlocks(blockEntries, blockDatas, uint64(collectionSize))

	compressedSize := len(blocksEncoded)

	m.Logf("Compression ratios: %.2f %.2f\n", float32(currentUncompressedSize)/float32(compressedSize), float32(bestUncompressedSize)/float32(compressedSize))

	blockEntries2, blockDatas2, _ := decodeBlocks(blocksEncoded)

	assert.Equal(m, blockEntries, blockEntries2)
	assert.Equal(m, blockDatas, blockDatas2)

	mapList2 := convertFromBlocks(blockEntries2, blockDatas2, uint64(collectionSize))

	assert.Equal(m, mapList, mapList2)
}

func TestMultipleMapLists(m *testing.T) {
	postingListSizeRand := rand.NewZipf(rand.New(rand.NewSource(42)), 1.2, 1, 10000)
	termFreqRand := rand.NewZipf(rand.New(rand.NewSource(42)), 1.2, 1, 100)
	postlingListLenRand := rand.NewZipf(rand.New(rand.NewSource(42)), 1.2, 1, 1000)

	numberCollections := 10000

	currentUncompressedSize := uint64(0)
	bestUncompressedSize := uint64(0)
	compressedSize := uint64(0)

	encodeSingleSeparate := 5

	for i := 0; i < numberCollections; i++ {

		collectionSize := postingListSizeRand.Uint64() + 1

		currentUncompressedSize += collectionSize * 29 // non-tombstone records have 29 bytes
		bestUncompressedSize += collectionSize * 16    // best possible uncompressed size for non-tombstone records 8 for key, 8 for value

		mapList := make([]MapPair, collectionSize)

		for i := range mapList {
			docId := uint64(100 + i*10)

			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, docId)

			tf := float32(termFreqRand.Uint64()) + 1
			pl := float32(postlingListLenRand.Uint64()) + 1

			value := make([]byte, 8)
			binary.LittleEndian.PutUint32(value, math.Float32bits(tf))
			binary.LittleEndian.PutUint32(value[4:], math.Float32bits(pl))

			mapList[i] = MapPair{
				Key:   key,
				Value: value,
			}
		}

		blocksEncoded, _ := createAndEncodeBlocksTest(mapList, encodeSingleSeparate)

		compressedSize += uint64(len(blocksEncoded))

		mapList2, _ := decodeAndConvertFromBlocksTest(blocksEncoded, encodeSingleSeparate)

		assert.Equal(m, mapList, mapList2)
	}
	m.Logf("Compression ratios: %.2f %.2f\n", float32(currentUncompressedSize)/float32(compressedSize), float32(bestUncompressedSize)/float32(compressedSize))
}

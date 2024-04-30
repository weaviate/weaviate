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

package contentReader

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math/big"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/weaviate/weaviate/usecases/byteops"

	"github.com/stretchr/testify/require"
)

func mustRandIntn(max int64) int {
	randInt, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		panic(fmt.Sprintf("mustRandIntn error: %v", err))
	}
	return int(randInt.Int64())
}

func TestContentReader_ReadValue(t *testing.T) {
	bytes := []byte{0, 1, 2, 3, 4}
	tests := []struct{ mmap bool }{{mmap: true}, {mmap: false}}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			contReader := GetContentReaderFromBytes(t, tt.mmap, bytes)

			for i := 0; i < len(bytes); i++ {
				value, offset := contReader.ReadValue(uint64(i))
				require.Equal(t, byte(i), value)
				require.Equal(t, uint64(i+1), offset)
			}
			require.Equal(t, uint64(len(bytes)), contReader.Length())
		})
	}
}

func TestContentReader_ReadRange(t *testing.T) {
	bytes := []byte{0, 0, 0, 0, 1, 1, 1, 1, 2, 3, 4}
	tests := []struct{ mmap bool }{{mmap: true}, {mmap: false}}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			contReader := GetContentReaderFromBytes(t, tt.mmap, bytes)

			buf, offset := contReader.ReadRange(0, 4, nil)
			require.Equal(t, []byte{0, 0, 0, 0}, buf)
			require.Equal(t, uint64(4), offset)

			buf, offset = contReader.ReadRange(offset, 4, nil)
			require.Equal(t, []byte{1, 1, 1, 1}, buf)
			require.Equal(t, uint64(8), offset)

			buf, offset = contReader.ReadRange(offset, 3, nil)
			require.Equal(t, []byte{2, 3, 4}, buf)
			require.Equal(t, uint64(len(bytes)), offset)
			require.Equal(t, uint64(len(bytes)), contReader.Length())
		})
	}
}

func TestContentReader_OffsetsStartEnd(t *testing.T) {
	bytes := []byte{0, 0, 0, 0, 1, 1, 1, 1, 2, 3, 4}
	tests := []struct{ mmap bool }{{mmap: true}, {mmap: false}}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			contReader := GetContentReaderFromBytes(t, tt.mmap, bytes)

			contReader2, err := contReader.NewWithOffsetStartEnd(4, 10)
			require.Nil(t, err)
			require.Equal(t, uint64(6), contReader2.Length())

			buf, offset := contReader2.ReadRange(0, 4, nil)
			require.Equal(t, []byte{1, 1, 1, 1}, buf)
			require.Equal(t, uint64(4), offset)

			contReader3, err := contReader2.NewWithOffsetStartEnd(2, 6)
			require.Nil(t, err)
			require.Equal(t, uint64(4), contReader3.Length())

			buf, offset = contReader3.ReadRange(0, 4, nil)
			require.Equal(t, []byte{1, 1, 2, 3}, buf)
			require.Equal(t, uint64(4), offset)
		})
	}
}

func TestContentReader_OffsetsStart(t *testing.T) {
	bytes := []byte{0, 0, 0, 0, 1, 1, 1, 1, 2, 3, 4}
	tests := []struct{ mmap bool }{{mmap: true}, {mmap: false}}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			contReader := GetContentReaderFromBytes(t, tt.mmap, bytes)

			contReader2, err := contReader.NewWithOffsetStart(4)
			require.Nil(t, err)
			require.Equal(t, uint64(len(bytes)-4), contReader2.Length())

			buf, offset := contReader2.ReadRange(0, 4, nil)
			require.Equal(t, []byte{1, 1, 1, 1}, buf)
			require.Equal(t, uint64(4), offset)

			contReader3, err := contReader2.NewWithOffsetStart(2)
			require.Nil(t, err)
			require.Equal(t, uint64(len(bytes)-6), contReader3.Length())

			buf, offset = contReader3.ReadRange(0, 4, nil)
			require.Equal(t, []byte{1, 1, 2, 3}, buf)
			require.Equal(t, uint64(4), offset)

			buf, offset = contReader3.ReadRange(4, 1, nil)
			require.Equal(t, []byte{4}, buf)
			require.Equal(t, uint64(5), offset)
		})
	}
}

func TestContentReader_MixedOperations(t *testing.T) {
	valuesNumbers := []uint64{234, 78, 23, 66, 8, 9, 2, 346745, 1}
	valuesByteArray := make([]byte, mustRandIntn(500))
	rand.Read(valuesByteArray)

	writeBuffer := make([]byte, 1*uint64Len+2*uint32Len+len(valuesByteArray)+2*uint64Len+2*uint32Len)
	byteOpsWrite := byteops.NewReadWriter(writeBuffer)

	byteOpsWrite.WriteUint64(valuesNumbers[0])
	byteOpsWrite.WriteUint32(uint32(valuesNumbers[1]))
	byteOpsWrite.WriteUint32(uint32(valuesNumbers[2]))
	require.Equal(t, byteOpsWrite.CopyBytesToBuffer(valuesByteArray), nil)
	byteOpsWrite.WriteUint64(valuesNumbers[3])
	byteOpsWrite.WriteUint32(uint32(valuesNumbers[4]))
	byteOpsWrite.WriteUint64(valuesNumbers[5])
	byteOpsWrite.WriteUint32(uint32(valuesNumbers[6]))

	bufLength := uint64(len(writeBuffer))

	testsMmap := []struct{ enabled bool }{{enabled: true}, {enabled: false}}
	tests := []struct {
		startOffset uint64
		endOffset   uint64
	}{
		{startOffset: 0, endOffset: bufLength},
		{startOffset: uint64Len, endOffset: bufLength - uint32Len},
		{startOffset: uint64Len + uint32Len, endOffset: bufLength - uint32Len - uint64Len},
	}
	for _, mmap := range testsMmap {
		for _, tt := range tests {
			t.Run("", func(t *testing.T) {
				contReaderInit := GetContentReaderFromBytes(t, mmap.enabled, writeBuffer)
				contReader, err := contReaderInit.NewWithOffsetStartEnd(tt.startOffset, tt.endOffset)
				require.Nil(t, err)
				offset := uint64(0)
				var val64 uint64
				var val32 uint32

				if tt.startOffset < uint64Len {
					val64, offset = contReader.ReadUint64(offset)
					require.Equal(t, val64, valuesNumbers[0])
				}
				if tt.startOffset < uint64Len+uint32Len {
					val32, offset = contReader.ReadUint32(offset)
					require.Equal(t, uint64(val32), valuesNumbers[1])
				}
				val32, offset = contReader.ReadUint32(offset)
				require.Equal(t, uint64(val32), valuesNumbers[2])
				buf, offset := contReader.ReadRange(offset, uint64(len(valuesByteArray)), nil)
				require.Equal(t, buf, valuesByteArray)
				val64, offset = contReader.ReadUint64(offset)
				require.Equal(t, val64, valuesNumbers[3])
				val32, offset = contReader.ReadUint32(offset)
				require.Equal(t, uint64(val32), valuesNumbers[4])
				if tt.endOffset > bufLength-uint64Len {
					val64, offset = contReader.ReadUint64(offset)
					require.Equal(t, val64, valuesNumbers[5])
				}
				if tt.endOffset > bufLength-uint32Len {
					val32, _ = contReader.ReadUint32(offset)
					require.Equal(t, uint64(val32), valuesNumbers[6])
				}
			})
		}
	}
}

type testCase struct {
	name        string
	startOffset uint64
	endOffset   uint64
	outbuf      int
}

func TestContentReader_PreadWithCache(t *testing.T) {
	size := uint64(50)
	pageSize := int(size / 10)
	valuesByteArray := make([]byte, int64(size))
	rand.Read(valuesByteArray)
	fi := writeBytesToFile(t, valuesByteArray)

	l, _ := lru.New[int, []byte](5)
	contReader := Pread{contentFile: fi, size: size, startOffset: 0, endOffset: size, cache: l, pageSize: pageSize}
	readTests := []testCase{
		{name: "empty", startOffset: 0, endOffset: 0},
		{name: "full file", startOffset: 0, endOffset: size},
		{name: "one full page", startOffset: uint64(pageSize), endOffset: uint64(pageSize * 2)},
		{name: "one full page and a bit", startOffset: uint64(pageSize), endOffset: uint64(pageSize*2 + 2)},
		{name: "two partial pages", startOffset: 4, endOffset: 6},
		{name: "two partial pages", startOffset: 1, endOffset: 9},
		{name: "two partial pages with outbuf", startOffset: 1, endOffset: 9, outbuf: 11},
		{name: "two partial pages and one full page", startOffset: 1, endOffset: 14},
		{name: "partial first and last page", startOffset: 1, endOffset: size - 1},
		{name: "full first and partial last page", startOffset: 0, endOffset: size - 1},
		{name: "partial first and full last page", startOffset: 4, endOffset: 10},
	}
	for _, tt := range readTests {
		t.Run(tt.name, func(t *testing.T) {
			var outbuf []byte
			if tt.outbuf > 0 {
				outbuf = make([]byte, tt.outbuf)
			}
			// read data that overlaps with the first page
			buf, offset := contReader.ReadRange(tt.startOffset, tt.endOffset-tt.startOffset, outbuf)
			require.Equal(t, valuesByteArray[tt.startOffset:tt.endOffset], buf)
			require.Equal(t, tt.endOffset, offset)
		})
	}
}

func IntsToBytes(ints ...uint64) []byte {
	byteOps := byteops.NewReadWriter(make([]byte, len(ints)*uint64Len))
	for _, i := range ints {
		byteOps.WriteUint64(i)
	}
	return byteOps.Buffer
}

func FuzzContentReader(f *testing.F) {
	size := uint64(10)
	pageSize := int(size / 2)
	valuesByteArray := make([]byte, int64(size))
	rand.Read(valuesByteArray)
	fi := writeBytesToFile(f, valuesByteArray)

	readTests := []testCase{
		{name: "empty", startOffset: 0, endOffset: 0},
		{name: "full file", startOffset: 0, endOffset: size},
		{name: "one full page", startOffset: uint64(pageSize), endOffset: uint64(pageSize * 2)},
		{name: "one full page and a bit", startOffset: uint64(pageSize), endOffset: uint64(pageSize*2 + 2)},
		{name: "two partial pages", startOffset: 4, endOffset: 6},
		{name: "two partial pages", startOffset: 1, endOffset: 9},
		{name: "two partial pages and one full page", startOffset: 1, endOffset: 14},
		{name: "partial first and last page", startOffset: 1, endOffset: size - 1},
		{name: "full first and partial last page", startOffset: 0, endOffset: size - 1},
		{name: "partial first and full last page", startOffset: 2, endOffset: 3},
		{name: "partial first and full last page", startOffset: 4, endOffset: size - 1},
	}
	for _, tc := range readTests {
		f.Add(IntsToBytes(tc.startOffset, tc.endOffset)) // Use f.Add to provide a seed corpus
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) != 16 {
			return
		}
		startOffset := binary.LittleEndian.Uint64(data[0:8])
		endOffset := binary.LittleEndian.Uint64(data[8:16])

		if startOffset > endOffset {
			return
		}

		if endOffset > size {
			return
		}

		l, _ := lru.New[int, []byte](10)
		contReader := Pread{contentFile: fi, size: size, startOffset: 0, endOffset: size, cache: l, pageSize: pageSize}

		buf, offset := contReader.ReadRange(startOffset, endOffset-startOffset, nil)
		require.Equal(t, valuesByteArray[startOffset:endOffset], buf)
		require.Equal(t, endOffset, offset)
	})
}

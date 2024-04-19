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
	"fmt"
	"github.com/weaviate/weaviate/usecases/byteops"
	"math/big"
	"testing"

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

			buf, offset := contReader.ReadRange(0, 4)
			require.Equal(t, []byte{0, 0, 0, 0}, buf)
			require.Equal(t, uint64(4), offset)

			buf, offset = contReader.ReadRange(offset, 4)
			require.Equal(t, []byte{1, 1, 1, 1}, buf)
			require.Equal(t, uint64(8), offset)

			buf, offset = contReader.ReadRange(offset, 3)
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

			buf, offset := contReader2.ReadRange(0, 4)
			require.Equal(t, []byte{1, 1, 1, 1}, buf)
			require.Equal(t, uint64(4), offset)

			contReader3, err := contReader2.NewWithOffsetStartEnd(2, 6)
			require.Nil(t, err)
			require.Equal(t, uint64(4), contReader3.Length())

			buf, offset = contReader3.ReadRange(0, 4)
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

			buf, offset := contReader2.ReadRange(0, 4)
			require.Equal(t, []byte{1, 1, 1, 1}, buf)
			require.Equal(t, uint64(4), offset)

			contReader3, err := contReader2.NewWithOffsetStart(2)
			require.Nil(t, err)
			require.Equal(t, uint64(len(bytes)-6), contReader3.Length())

			buf, offset = contReader3.ReadRange(0, 4)
			require.Equal(t, []byte{1, 1, 2, 3}, buf)
			require.Equal(t, uint64(4), offset)

			buf, offset = contReader3.ReadRange(4, 1)
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
				buf, offset := contReader.ReadRange(offset, uint64(len(valuesByteArray)))
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
					val32, offset = contReader.ReadUint32(offset)
					require.Equal(t, uint64(val32), valuesNumbers[6])
				}

			})
		}
	}
}

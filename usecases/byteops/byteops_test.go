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

package byteops

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const MaxUint32 = ^uint32(0)

func mustRandIntn(max int64) int {
	randInt, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		panic(fmt.Sprintf("mustRandIntn error: %v", err))
	}
	return int(randInt.Int64())
}

// Create a buffer with space for several values and first write into it and then test that the values can be read again
func TestReadAnWrite(t *testing.T) {
	valuesNumbers := []uint64{234, 78, 23, 66, 8, 9, 2, 346745, 1}
	valuesByteArray := make([]byte, mustRandIntn(500))
	rand.Read(valuesByteArray)

	writeBuffer := make([]byte, 2*uint64Len+2*uint32Len+2*uint16Len+len(valuesByteArray))
	byteOpsWrite := NewReadWriter(writeBuffer)

	byteOpsWrite.WriteUint64(valuesNumbers[0])
	byteOpsWrite.WriteUint32(uint32(valuesNumbers[1]))
	byteOpsWrite.WriteUint32(uint32(valuesNumbers[2]))
	assert.Equal(t, byteOpsWrite.CopyBytesToBuffer(valuesByteArray), nil)
	byteOpsWrite.WriteUint16(uint16(valuesNumbers[3]))
	byteOpsWrite.WriteUint64(valuesNumbers[4])
	byteOpsWrite.WriteUint16(uint16(valuesNumbers[5]))

	byteOpsRead := NewReadWriter(writeBuffer)

	require.Equal(t, byteOpsRead.ReadUint64(), valuesNumbers[0])
	require.Equal(t, byteOpsRead.ReadUint32(), uint32(valuesNumbers[1]))
	require.Equal(t, byteOpsRead.ReadUint32(), uint32(valuesNumbers[2]))

	// we are going to do the next op twice (once with copying, once without)
	// to be able to rewind the buffer, let's cache the current position
	posBeforeByteArray := byteOpsRead.Position

	returnBuf, err := byteOpsRead.CopyBytesFromBuffer(uint64(len(valuesByteArray)), nil)
	assert.Equal(t, returnBuf, valuesByteArray)
	assert.Equal(t, err, nil)

	// rewind the buffer to where it was before the read
	byteOpsRead.MoveBufferToAbsolutePosition(posBeforeByteArray)

	subSlice := byteOpsRead.ReadBytesFromBuffer(uint64(len(valuesByteArray)))
	assert.Equal(t, subSlice, valuesByteArray)

	// now read again using the other method

	require.Equal(t, byteOpsRead.ReadUint16(), uint16(valuesNumbers[3]))
	require.Equal(t, byteOpsRead.ReadUint64(), valuesNumbers[4])
	require.Equal(t, byteOpsRead.ReadUint16(), uint16(valuesNumbers[5]))
}

// create buffer that is larger than uint32 and write to the end and then try to reread it
func TestReadAnWriteLargeBuffer(t *testing.T) {
	writeBuffer := make([]byte, uint64(MaxUint32)+4)
	byteOpsWrite := NewReadWriter(writeBuffer)
	byteOpsWrite.MoveBufferPositionForward(uint64(MaxUint32))
	byteOpsWrite.WriteUint16(uint16(10))

	byteOpsRead := NewReadWriter(writeBuffer)
	byteOpsRead.MoveBufferPositionForward(uint64(MaxUint32))
	require.Equal(t, byteOpsRead.ReadUint16(), uint16(10))
}

func TestWritingAndReadingBufferOfDynamicLength(t *testing.T) {
	t.Run("uint64 length indicator", func(t *testing.T) {
		bufLen := uint64(mustRandIntn(1024))
		buf := make([]byte, bufLen)
		rand.Read(buf)

		// uint64 length indicator + buffer + unrelated data at end of buffer
		totalBuf := make([]byte, bufLen+16)
		bo := NewReadWriter(totalBuf)

		assert.Nil(t, bo.CopyBytesToBufferWithUint64LengthIndicator(buf))
		bo.WriteUint64(17)
		assert.Equal(t, buf, totalBuf[8:8+bufLen])

		// read
		bo = NewReadWriter(totalBuf)
		bufRead := bo.ReadBytesFromBufferWithUint64LengthIndicator()
		assert.Len(t, bufRead, int(bufLen))
		assert.Equal(t, uint64(17), bo.ReadUint64())

		// discard
		bo = NewReadWriter(totalBuf)
		discarded := bo.DiscardBytesFromBufferWithUint64LengthIndicator()
		assert.Equal(t, bufLen, discarded)
		assert.Equal(t, uint64(17), bo.ReadUint64())
	})

	t.Run("uint32 length indicator", func(t *testing.T) {
		bufLen := uint32(mustRandIntn(1024))
		buf := make([]byte, bufLen)
		rand.Read(buf)

		// uint32 length indicator + buffer + unrelated data at end of buffer
		totalBuf := make([]byte, bufLen+8)
		bo := NewReadWriter(totalBuf)

		assert.Nil(t, bo.CopyBytesToBufferWithUint32LengthIndicator(buf))
		bo.WriteUint32(17)
		assert.Equal(t, buf, totalBuf[4:4+bufLen])

		// read
		bo = NewReadWriter(totalBuf)
		bufRead := bo.ReadBytesFromBufferWithUint32LengthIndicator()
		assert.Len(t, bufRead, int(bufLen))
		assert.Equal(t, uint32(17), bo.ReadUint32())

		// discard
		bo = NewReadWriter(totalBuf)
		discarded := bo.DiscardBytesFromBufferWithUint32LengthIndicator()
		assert.Equal(t, bufLen, discarded)
		assert.Equal(t, uint32(17), bo.ReadUint32())
	})
}

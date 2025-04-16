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

	writeBuffer := make([]byte, 2*Uint64Len+2*Uint32Len+2*Uint16Len+len(valuesByteArray))
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
	empty := []byte{}

	t.Run("uint64 length indicator", func(t *testing.T) {
		bufLen := uint64(mustRandIntn(1024))
		buf := make([]byte, bufLen)
		rand.Read(buf)

		// uint64 length indicator + buffer + unrelated data at end of buffer
		totalBuf := make([]byte, 8+bufLen+8+8)
		bo := NewReadWriter(totalBuf)

		// write
		assert.NoError(t, bo.CopyBytesToBufferWithUint64LengthIndicator(buf))
		assert.Equal(t, buf, totalBuf[8:8+bufLen])
		bo.WriteUint64(17)
		assert.NoError(t, bo.CopyBytesToBufferWithUint64LengthIndicator(empty))

		// read
		bo = NewReadWriter(totalBuf)
		bufRead := bo.ReadBytesFromBufferWithUint64LengthIndicator()
		assert.Len(t, bufRead, int(bufLen))
		assert.Equal(t, buf, bufRead)
		assert.Equal(t, uint64(17), bo.ReadUint64())
		bufRead = bo.ReadBytesFromBufferWithUint64LengthIndicator()
		assert.Len(t, bufRead, 0)
		assert.NotNil(t, bufRead)

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
		totalBuf := make([]byte, 4+bufLen+4+4)
		bo := NewReadWriter(totalBuf)

		// write
		assert.NoError(t, bo.CopyBytesToBufferWithUint32LengthIndicator(buf))
		assert.Equal(t, buf, totalBuf[4:4+bufLen])
		bo.WriteUint32(17)
		assert.NoError(t, bo.CopyBytesToBufferWithUint32LengthIndicator(empty))

		// read
		bo = NewReadWriter(totalBuf)
		bufRead := bo.ReadBytesFromBufferWithUint32LengthIndicator()
		assert.Len(t, bufRead, int(bufLen))
		assert.Equal(t, buf, bufRead)
		assert.Equal(t, uint32(17), bo.ReadUint32())
		bufRead = bo.ReadBytesFromBufferWithUint32LengthIndicator()
		assert.Len(t, bufRead, 0)
		assert.NotNil(t, bufRead)

		// discard
		bo = NewReadWriter(totalBuf)
		discarded := bo.DiscardBytesFromBufferWithUint32LengthIndicator()
		assert.Equal(t, bufLen, discarded)
		assert.Equal(t, uint32(17), bo.ReadUint32())
	})
}

func TestIntsToByteVector(t *testing.T) {
	t.Run("empty array", func(t *testing.T) {
		bytes := IntsToByteVector([]float64{})
		assert.Equal(t, []byte{}, bytes)
	})

	t.Run("non-empty array of u8s", func(t *testing.T) {
		bytes := IntsToByteVector([]float64{1, 2, 3})
		assert.Equal(t, []byte{
			0o1, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0,
			0o2, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0,
			0o3, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0,
		}, bytes)
	})

	t.Run("non-empty array of u64", func(t *testing.T) {
		bytes := IntsToByteVector([]float64{
			9007199254740992, // MaxFloat64
			9007199254740991,
			9007199254740990,
		})
		assert.Equal(t, []byte{
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1f, 0x00,
			0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1f, 0x00,
		}, bytes)
	})
}

func TestIntsFromByteVector(t *testing.T) {
	t.Run("empty array", func(t *testing.T) {
		ints := IntsFromByteVector([]byte{})
		assert.Equal(t, []int64{}, ints)
	})

	t.Run("non-empty array of u8s", func(t *testing.T) {
		ints := IntsFromByteVector([]byte{
			0o1, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0,
			0o2, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0,
			0o3, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0, 0o0,
		})
		assert.Equal(t, []int64{1, 2, 3}, ints)
	})

	t.Run("non-empty array of u64", func(t *testing.T) {
		ints := IntsFromByteVector([]byte{
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00,
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1f, 0x00,
			0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1f, 0x00,
		})
		assert.Equal(t, []int64{
			9007199254740992, // MaxFloat64
			9007199254740991,
			9007199254740990,
		}, ints)
	})
}

func TestFp32SliceToBytes(t *testing.T) {
	t.Run("empty array", func(t *testing.T) {
		bytes := Fp32SliceToBytes([]float32{})
		assert.Equal(t, []byte{}, bytes)
	})

	t.Run("non-empty array", func(t *testing.T) {
		bytes := Fp32SliceToBytes([]float32{1.1, 2.2, 3.3})
		assert.Equal(t, []byte{
			0xcd, 0xcc, 0x8c, 0x3f,
			0xcd, 0xcc, 0xc, 0x40,
			0x33, 0x33, 0x53, 0x40,
		}, bytes)
	})
}

func TestFp32SliceOfSlicesToBytes(t *testing.T) {
	t.Run("empty array", func(t *testing.T) {
		bytes := Fp32SliceOfSlicesToBytes([][]float32{})
		assert.Equal(t, []byte{}, bytes)
	})

	t.Run("empty subarrays", func(t *testing.T) {
		bytes := Fp32SliceOfSlicesToBytes([][]float32{{}, {}, {}})
		assert.Equal(t, []byte{}, bytes)
	})

	t.Run("non-empty array", func(t *testing.T) {
		bytes := Fp32SliceOfSlicesToBytes([][]float32{
			{1.1, 2.2, 3.3},
			{4.4, 5.5, 6.6},
			{7.7, 8.8, 9.9},
		})
		assert.Equal(t, []byte{
			0x3, 0x0,
			0xcd, 0xcc, 0x8c, 0x3f, 0xcd, 0xcc, 0xc, 0x40, 0x33, 0x33, 0x53, 0x40,
			0xcd, 0xcc, 0x8c, 0x40, 0x0, 0x0, 0xb0, 0x40, 0x33, 0x33, 0xd3, 0x40,
			0x66, 0x66, 0xf6, 0x40, 0xcd, 0xcc, 0xc, 0x41, 0x66, 0x66, 0x1e, 0x41,
		}, bytes)
	})
}

func TestFp32SliceFromBytes(t *testing.T) {
	t.Run("empty array", func(t *testing.T) {
		slice := Fp32SliceFromBytes([]byte{})
		assert.Equal(t, []float32{}, slice)
	})

	t.Run("non-empty array", func(t *testing.T) {
		slice := Fp32SliceFromBytes([]byte{
			0xcd, 0xcc, 0x8c, 0x3f,
			0xcd, 0xcc, 0xc, 0x40,
			0x33, 0x33, 0x53, 0x40,
		})
		assert.Equal(t, []float32{1.1, 2.2, 3.3}, slice)
	})
}

func TestFp32SliceOfSlicesFromBytes(t *testing.T) {
	t.Run("empty array", func(t *testing.T) {
		slices, err := Fp32SliceOfSlicesFromBytes([]byte{})
		assert.Nil(t, err)
		assert.Equal(t, [][]float32{}, slices)
	})

	t.Run("dimension is 0", func(t *testing.T) {
		_, err := Fp32SliceOfSlicesFromBytes([]byte{0x0, 0x0})
		assert.NotNil(t, err)
		assert.Equal(t, "dimension cannot be 0", err.Error())
	})

	t.Run("empty subarrays", func(t *testing.T) {
		slices, err := Fp32SliceOfSlicesFromBytes([]byte{0x3, 0x0})
		assert.Nil(t, err)
		assert.Equal(t, [][]float32{}, slices)
	})

	t.Run("non-empty array", func(t *testing.T) {
		slices, err := Fp32SliceOfSlicesFromBytes([]byte{
			0x3, 0x0,
			0xcd, 0xcc, 0x8c, 0x3f, 0xcd, 0xcc, 0xc, 0x40, 0x33, 0x33, 0x53, 0x40,
			0xcd, 0xcc, 0x8c, 0x40, 0x0, 0x0, 0xb0, 0x40, 0x33, 0x33, 0xd3, 0x40,
			0x66, 0x66, 0xf6, 0x40, 0xcd, 0xcc, 0xc, 0x41, 0x66, 0x66, 0x1e, 0x41,
		})
		assert.Nil(t, err)
		assert.Equal(t, [][]float32{
			{1.1, 2.2, 3.3},
			{4.4, 5.5, 6.6},
			{7.7, 8.8, 9.9},
		}, slices)
	})
}

func TestFp64SliceToBytes(t *testing.T) {
	t.Run("empty array", func(t *testing.T) {
		bytes := Fp32SliceToBytes([]float32{})
		assert.Equal(t, []byte{}, bytes)
	})

	t.Run("non-empty array", func(t *testing.T) {
		bytes := Fp64SliceToBytes([]float64{1.1, 2.2, 3.3})
		assert.Equal(t, []byte{
			0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xf1, 0x3f,
			0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0x1, 0x40,
			0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xa, 0x40,
		}, bytes)
	})
}

func TestFp64SliceFromBytes(t *testing.T) {
	t.Run("empty array", func(t *testing.T) {
		slice := Fp64SliceFromBytes([]byte{})
		assert.Equal(t, []float64{}, slice)
	})

	t.Run("non-empty array", func(t *testing.T) {
		slice := Fp64SliceFromBytes([]byte{
			0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xf1, 0x3f,
			0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0x1, 0x40,
			0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xa, 0x40,
		})
		assert.Equal(t, []float64{1.1, 2.2, 3.3}, slice)
	})
}

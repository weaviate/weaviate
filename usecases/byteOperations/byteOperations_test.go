package byteOperations

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Create a buffer with space for several values and first write into it and then test that the values can be read again
func TestReadAnWrite(t *testing.T) {
	valuesNumbers := []uint64{234, 78, 23, 66, 8, 9, 2, 346745, 1}
	valuesByteArray := make([]byte, rand.Intn(500))
	rand.Read(valuesByteArray)

	writeBuffer := make([]byte, 2*uint64Len+2*uint32Len+2*uint16Len+len(valuesByteArray))
	bufPosWrite := uint32(0)

	WriteUint64(writeBuffer, &bufPosWrite, valuesNumbers[0])
	WriteUint32(writeBuffer, &bufPosWrite, uint32(valuesNumbers[1]))
	WriteUint32(writeBuffer, &bufPosWrite, uint32(valuesNumbers[2]))
	assert.Equal(t, CopyBytesToBuffer(writeBuffer, &bufPosWrite, valuesByteArray), nil)
	WriteUint16(writeBuffer, &bufPosWrite, uint16(valuesNumbers[3]))
	WriteUint64(writeBuffer, &bufPosWrite, valuesNumbers[4])
	WriteUint16(writeBuffer, &bufPosWrite, uint16(valuesNumbers[5]))

	bufPosRead := uint32(0)

	require.Equal(t, ReadUint64(writeBuffer, &bufPosRead), valuesNumbers[0])
	require.Equal(t, ReadUint32(writeBuffer, &bufPosRead), uint32(valuesNumbers[1]))
	require.Equal(t, ReadUint32(writeBuffer, &bufPosRead), uint32(valuesNumbers[2]))
	returnBuf, err := CopyBytesFromBuffer(writeBuffer, &bufPosRead, uint32(len(valuesByteArray)))
	assert.Equal(t, returnBuf, valuesByteArray)
	assert.Equal(t, err, nil)
	require.Equal(t, ReadUint16(writeBuffer, &bufPosRead), uint16(valuesNumbers[3]))
	require.Equal(t, ReadUint64(writeBuffer, &bufPosRead), valuesNumbers[4])
	require.Equal(t, ReadUint16(writeBuffer, &bufPosRead), uint16(valuesNumbers[5]))
}

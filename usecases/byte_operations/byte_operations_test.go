//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package byte_operations

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
	byteOpsWrite := ByteOperations{Buffer: writeBuffer}

	byteOpsWrite.WriteUint64(valuesNumbers[0])
	byteOpsWrite.WriteUint32(uint32(valuesNumbers[1]))
	byteOpsWrite.WriteUint32(uint32(valuesNumbers[2]))
	assert.Equal(t, byteOpsWrite.CopyBytesToBuffer(valuesByteArray), nil)
	byteOpsWrite.WriteUint16(uint16(valuesNumbers[3]))
	byteOpsWrite.WriteUint64(valuesNumbers[4])
	byteOpsWrite.WriteUint16(uint16(valuesNumbers[5]))

	byteOpsRead := ByteOperations{Buffer: writeBuffer}

	require.Equal(t, byteOpsRead.ReadUint64(), valuesNumbers[0])
	require.Equal(t, byteOpsRead.ReadUint32(), uint32(valuesNumbers[1]))
	require.Equal(t, byteOpsRead.ReadUint32(), uint32(valuesNumbers[2]))
	returnBuf, err := byteOpsRead.CopyBytesFromBuffer(uint32(len(valuesByteArray)))
	assert.Equal(t, returnBuf, valuesByteArray)
	assert.Equal(t, err, nil)
	require.Equal(t, byteOpsRead.ReadUint16(), uint16(valuesNumbers[3]))
	require.Equal(t, byteOpsRead.ReadUint64(), valuesNumbers[4])
	require.Equal(t, byteOpsRead.ReadUint16(), uint16(valuesNumbers[5]))
}

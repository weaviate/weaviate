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

// Package byte_operations provides helper functions to (un-) marshal objects from or into a buffer
package byte_operations

import (
	"encoding/binary"
	"errors"
)

const (
	uint32Len = 4
	uint64Len = 8
	uint16Len = 2
)

type ByteOperations struct {
	Position uint64
	Buffer   []byte
}

func (bo *ByteOperations) ReadUint64() uint64 {
	bo.Position += uint64Len
	return binary.LittleEndian.Uint64(bo.Buffer[bo.Position-uint64Len : bo.Position])
}

func (bo *ByteOperations) ReadUint16() uint16 {
	bo.Position += uint16Len
	return binary.LittleEndian.Uint16(bo.Buffer[bo.Position-uint16Len : bo.Position])
}

func (bo *ByteOperations) ReadUint32() uint32 {
	bo.Position += uint32Len
	return binary.LittleEndian.Uint32(bo.Buffer[bo.Position-uint32Len : bo.Position])
}

func (bo *ByteOperations) CopyBytesFromBuffer(length uint64) ([]byte, error) {
	out := make([]byte, length)
	bo.Position += length
	numCopiedBytes := copy(out, bo.Buffer[bo.Position-length:bo.Position])
	if numCopiedBytes != int(length) {
		return nil, errors.New("could not copy data from buffer")
	}
	return out, nil
}

func (bo *ByteOperations) WriteUint64(value uint64) {
	bo.Position += uint64Len
	binary.LittleEndian.PutUint64(bo.Buffer[bo.Position-uint64Len:bo.Position], value)
}

func (bo *ByteOperations) WriteUint32(value uint32) {
	bo.Position += uint32Len
	binary.LittleEndian.PutUint32(bo.Buffer[bo.Position-uint32Len:bo.Position], value)
}

func (bo *ByteOperations) WriteUint16(value uint16) {
	bo.Position += uint16Len
	binary.LittleEndian.PutUint16(bo.Buffer[bo.Position-uint16Len:bo.Position], value)
}

func (bo *ByteOperations) CopyBytesToBuffer(copyBytes []byte) error {
	lenCopyBytes := uint64(len(copyBytes))
	bo.Position += lenCopyBytes
	numCopiedBytes := copy(bo.Buffer[bo.Position-lenCopyBytes:bo.Position], copyBytes)
	if numCopiedBytes != int(lenCopyBytes) {
		return errors.New("could not copy data into buffer")
	}
	return nil
}

func (bo *ByteOperations) MoveBufferPositionForward(length uint64) {
	bo.Position += length
}

func (bo *ByteOperations) WriteByte(b byte) {
	bo.Buffer[bo.Position] = b
	bo.Position += 1
}

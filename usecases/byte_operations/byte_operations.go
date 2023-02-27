//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
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

func (bo *ByteOperations) CopyBytesFromBuffer(length uint64, out []byte) ([]byte, error) {
	if out == nil {
		out = make([]byte, length)
	}
	bo.Position += length
	numCopiedBytes := copy(out, bo.Buffer[bo.Position-length:bo.Position])
	if numCopiedBytes != int(length) {
		return nil, errors.New("could not copy data from buffer")
	}
	return out, nil
}

func (bo *ByteOperations) ReadBytesFromBuffer(length uint64) []byte {
	subslice := bo.Buffer[bo.Position : bo.Position+length]
	bo.Position += length
	return subslice
}

func (bo *ByteOperations) ReadBytesFromBufferWithUint64LengthIndicator() []byte {
	bo.Position += uint64Len
	bufLen := binary.LittleEndian.Uint64(bo.Buffer[bo.Position-uint64Len : bo.Position])

	bo.Position += bufLen
	subslice := bo.Buffer[bo.Position-bufLen : bo.Position]
	return subslice
}

func (bo *ByteOperations) DiscardBytesFromBufferWithUint64LengthIndicator() uint64 {
	bo.Position += uint64Len
	bufLen := binary.LittleEndian.Uint64(bo.Buffer[bo.Position-uint64Len : bo.Position])

	bo.Position += bufLen
	return bufLen
}

func (bo *ByteOperations) ReadBytesFromBufferWithUint32LengthIndicator() []byte {
	bo.Position += uint32Len
	bufLen := uint64(binary.LittleEndian.Uint32(bo.Buffer[bo.Position-uint32Len : bo.Position]))

	bo.Position += bufLen
	subslice := bo.Buffer[bo.Position-bufLen : bo.Position]
	return subslice
}

func (bo *ByteOperations) DiscardBytesFromBufferWithUint32LengthIndicator() uint32 {
	bo.Position += uint32Len
	bufLen := binary.LittleEndian.Uint32(bo.Buffer[bo.Position-uint32Len : bo.Position])

	bo.Position += uint64(bufLen)
	return bufLen
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

// Writes a uint64 length indicator about the buffer that's about to follow,
// then writes the buffer itself
func (bo *ByteOperations) CopyBytesToBufferWithUint64LengthIndicator(copyBytes []byte) error {
	lenCopyBytes := uint64(len(copyBytes))
	bo.Position += uint64Len
	binary.LittleEndian.PutUint64(bo.Buffer[bo.Position-uint64Len:bo.Position], lenCopyBytes)
	bo.Position += lenCopyBytes
	numCopiedBytes := copy(bo.Buffer[bo.Position-lenCopyBytes:bo.Position], copyBytes)
	if numCopiedBytes != int(lenCopyBytes) {
		return errors.New("could not copy data into buffer")
	}
	return nil
}

// Writes a uint32 length indicator about the buffer that's about to follow,
// then writes the buffer itself
func (bo *ByteOperations) CopyBytesToBufferWithUint32LengthIndicator(copyBytes []byte) error {
	lenCopyBytes := uint32(len(copyBytes))
	bo.Position += uint32Len
	binary.LittleEndian.PutUint32(bo.Buffer[bo.Position-uint32Len:bo.Position], lenCopyBytes)
	bo.Position += uint64(lenCopyBytes)
	numCopiedBytes := copy(bo.Buffer[bo.Position-uint64(lenCopyBytes):bo.Position], copyBytes)
	if numCopiedBytes != int(lenCopyBytes) {
		return errors.New("could not copy data into buffer")
	}
	return nil
}

func (bo *ByteOperations) MoveBufferPositionForward(length uint64) {
	bo.Position += length
}

func (bo *ByteOperations) MoveBufferToAbsolutePosition(pos uint64) {
	bo.Position = pos
}

func (bo *ByteOperations) WriteByte(b byte) {
	bo.Buffer[bo.Position] = b
	bo.Position += 1
}

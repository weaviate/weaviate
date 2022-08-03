// Package byteOperations provides helper functions to (un-) marshal objects from or into a buffer
package byteOperations

import (
	"encoding/binary"
	"errors"
)

const (
	uint32Len = 4
	uint64Len = 8
	uint16Len = 2
)

func ReadUint64(buffer []byte, position *uint32) uint64 {
	*position += uint64Len
	return binary.LittleEndian.Uint64(buffer[*position-uint64Len : *position])
}

func ReadUint16(buffer []byte, position *uint32) uint16 {
	*position += uint16Len
	return binary.LittleEndian.Uint16(buffer[*position-uint16Len : *position])
}

func ReadUint32(buffer []byte, position *uint32) uint32 {
	*position += uint32Len
	return binary.LittleEndian.Uint32(buffer[*position-uint32Len : *position])
}

func CopyBytesFromBuffer(in []byte, position *uint32, length uint32) ([]byte, error) {
	out := make([]byte, length)
	numCopiedBytes := copy(out, in[*position:*position+length])
	if numCopiedBytes != int(length) {
		return nil, errors.New("could not copy data from buffer")
	}
	*position += length
	return out, nil
}

func WriteUint64(buffer []byte, position *uint32, value uint64) {
	binary.LittleEndian.PutUint64(buffer[*position:*position+uint64Len], value)
	*position += uint64Len
}

func WriteUint32(buffer []byte, position *uint32, value uint32) {
	binary.LittleEndian.PutUint32(buffer[*position:*position+uint32Len], value)
	*position += uint32Len
}

func WriteUint16(buffer []byte, position *uint32, value uint16) {
	binary.LittleEndian.PutUint16(buffer[*position:*position+uint16Len], value)
	*position += uint16Len
}

func CopyBytesToBuffer(buf []byte, position *uint32, copyBytes []byte) error {
	lenCopyBytes := uint32(len(copyBytes))
	numCopiedBytes := copy(buf[*position:*position+lenCopyBytes], copyBytes)
	if numCopiedBytes != int(lenCopyBytes) {
		return errors.New("could not copy data into buffer")
	}
	*position += lenCopyBytes
	return nil
}

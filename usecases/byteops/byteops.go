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

// Package byteops provides helper functions to (un-) marshal objects from or into a buffer
package byteops

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	uint64Len = 8
	uint32Len = 4
	uint16Len = 2
	uint8Len  = 1
)

type ReadWriter struct {
	Position uint64
	Buffer   []byte
}

func WithPosition(pos uint64) func(*ReadWriter) {
	return func(rw *ReadWriter) {
		rw.Position = pos
	}
}

func NewReadWriter(buf []byte, opts ...func(writer *ReadWriter)) ReadWriter {
	rw := ReadWriter{Buffer: buf}
	for _, opt := range opts {
		opt(&rw)
	}
	return rw
}

func (bo *ReadWriter) ResetBuffer(buf []byte) {
	bo.Buffer = buf
	bo.Position = 0
}

func (bo *ReadWriter) ReadUint64() uint64 {
	bo.Position += uint64Len
	return binary.LittleEndian.Uint64(bo.Buffer[bo.Position-uint64Len : bo.Position])
}

func (bo *ReadWriter) ReadUint16() uint16 {
	bo.Position += uint16Len
	return binary.LittleEndian.Uint16(bo.Buffer[bo.Position-uint16Len : bo.Position])
}

func (bo *ReadWriter) ReadUint32() uint32 {
	bo.Position += uint32Len
	return binary.LittleEndian.Uint32(bo.Buffer[bo.Position-uint32Len : bo.Position])
}

func (bo *ReadWriter) ReadUint8() uint8 {
	bo.Position += uint8Len
	return bo.Buffer[bo.Position-uint8Len]
}

func (bo *ReadWriter) CopyBytesFromBuffer(length uint64, out []byte) ([]byte, error) {
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

func (bo *ReadWriter) ReadBytesFromBuffer(length uint64) []byte {
	subslice := bo.Buffer[bo.Position : bo.Position+length]
	bo.Position += length
	return subslice
}

func (bo *ReadWriter) ReadBytesFromBufferWithUint64LengthIndicator() []byte {
	bo.Position += uint64Len
	bufLen := binary.LittleEndian.Uint64(bo.Buffer[bo.Position-uint64Len : bo.Position])

	bo.Position += bufLen
	subslice := bo.Buffer[bo.Position-bufLen : bo.Position]
	return subslice
}

func (bo *ReadWriter) DiscardBytesFromBufferWithUint64LengthIndicator() uint64 {
	bo.Position += uint64Len
	bufLen := binary.LittleEndian.Uint64(bo.Buffer[bo.Position-uint64Len : bo.Position])

	bo.Position += bufLen
	return bufLen
}

func (bo *ReadWriter) ReadBytesFromBufferWithUint32LengthIndicator() []byte {
	bo.Position += uint32Len
	bufLen := uint64(binary.LittleEndian.Uint32(bo.Buffer[bo.Position-uint32Len : bo.Position]))

	if bufLen == 0 {
		return nil
	}

	bo.Position += bufLen
	subslice := bo.Buffer[bo.Position-bufLen : bo.Position]
	return subslice
}

func (bo *ReadWriter) DiscardBytesFromBufferWithUint32LengthIndicator() uint32 {
	bo.Position += uint32Len
	bufLen := binary.LittleEndian.Uint32(bo.Buffer[bo.Position-uint32Len : bo.Position])

	bo.Position += uint64(bufLen)
	return bufLen
}

func (bo *ReadWriter) WriteUint64(value uint64) {
	bo.Position += uint64Len
	binary.LittleEndian.PutUint64(bo.Buffer[bo.Position-uint64Len:bo.Position], value)
}

func (bo *ReadWriter) WriteUint32(value uint32) {
	bo.Position += uint32Len
	binary.LittleEndian.PutUint32(bo.Buffer[bo.Position-uint32Len:bo.Position], value)
}

func (bo *ReadWriter) WriteUint16(value uint16) {
	bo.Position += uint16Len
	binary.LittleEndian.PutUint16(bo.Buffer[bo.Position-uint16Len:bo.Position], value)
}

func (bo *ReadWriter) CopyBytesToBuffer(copyBytes []byte) error {
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
func (bo *ReadWriter) CopyBytesToBufferWithUint64LengthIndicator(copyBytes []byte) error {
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
func (bo *ReadWriter) CopyBytesToBufferWithUint32LengthIndicator(copyBytes []byte) error {
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

func (bo *ReadWriter) MoveBufferPositionForward(length uint64) {
	bo.Position += length
}

func (bo *ReadWriter) MoveBufferToAbsolutePosition(pos uint64) {
	bo.Position = pos
}

func (bo *ReadWriter) WriteByte(b byte) {
	bo.Buffer[bo.Position] = b
	bo.Position += 1
}

func Float32ToByteVector(floats []float32) []byte {
	vector := make([]byte, len(floats)*uint32Len)
	for i := 0; i < len(floats); i++ {
		binary.LittleEndian.PutUint32(vector[i*uint32Len:(i+1)*uint32Len], math.Float32bits(floats[i]))
	}
	return vector
}

func Float64ToByteVector(floats []float64) []byte {
	vector := make([]byte, len(floats)*uint64Len)
	for i := 0; i < len(floats); i++ {
		binary.LittleEndian.PutUint64(vector[i*uint64Len:(i+1)*uint64Len], math.Float64bits(floats[i]))
	}
	return vector
}

func Float32FromByteVector(vector []byte) []float32 {
	floats := make([]float32, len(vector)/uint32Len)

	for i := 0; i < len(floats); i++ {
		asUint := binary.LittleEndian.Uint32(vector[i*uint32Len : (i+1)*uint32Len])
		floats[i] = math.Float32frombits(asUint)
	}
	return floats
}

func Float64FromByteVector(vector []byte) []float64 {
	floats := make([]float64, len(vector)/uint64Len)

	for i := 0; i < len(floats); i++ {
		asUint := binary.LittleEndian.Uint64(vector[i*uint64Len : (i+1)*uint64Len])
		floats[i] = math.Float64frombits(asUint)
	}
	return floats
}

func IntsToByteVector(ints []float64) []byte {
	vector := make([]byte, len(ints)*uint64Len)
	for i, val := range ints {
		intVal := int64(val)
		binary.LittleEndian.PutUint64(vector[i*uint64Len:(i+1)*uint64Len], uint64(intVal))
	}
	return vector
}

func IntsFromByteVector(vector []byte) []int64 {
	ints := make([]int64, len(vector)/uint64Len)
	for i := 0; i < len(ints); i++ {
		asUint := binary.LittleEndian.Uint64(vector[i*uint64Len : (i+1)*uint64Len])
		ints[i] = int64(asUint)
	}
	return ints
}

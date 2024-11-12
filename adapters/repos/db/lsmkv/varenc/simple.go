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

package varenc

import "encoding/binary"

type UintTypes interface {
	uint64 | uint32 | uint16 | uint8
}

type SimpleEncoder[T UintTypes] struct {
	values      []T
	buf         []byte
	elementSize int
}

func (e *SimpleEncoder[T]) Init(expectedCount int) {
	e.values = make([]T, expectedCount)

	switch any(*new(T)).(type) {
	case uint64:
		e.elementSize = 8
	case uint32:
		e.elementSize = 4
	case uint16:
		e.elementSize = 2
	case uint8:
		e.elementSize = 1
	default:
		panic("unsupported type")
	}

	e.buf = make([]byte, 8+e.elementSize*expectedCount)
}

func (e SimpleEncoder[T]) encode(value T, buf []byte) {
	switch v := any(value).(type) {
	case uint64:
		binary.LittleEndian.PutUint64(buf, v)
	case uint32:
		binary.LittleEndian.PutUint32(buf, v)
	case uint16:
		binary.LittleEndian.PutUint16(buf, v)
	case uint8:
		buf[0] = byte(v)
	}
}

func (e SimpleEncoder[T]) decode(buf []byte, value *T) {
	switch len(buf) {
	case 8:
		*value = any(binary.LittleEndian.Uint64(buf)).(T)
	case 4:
		*value = any(binary.LittleEndian.Uint32(buf)).(T)
	case 2:
		*value = any(binary.LittleEndian.Uint16(buf)).(T)
	case 1:
		*value = any(buf[0]).(T)
	}
}

func (e SimpleEncoder[T]) EncodeReusable(values []T, buf []byte) {
	binary.LittleEndian.PutUint64(buf, uint64(len(values)))
	len := e.elementSize
	for i, value := range values {
		e.encode(value, buf[8+i*len:8+(i+1)*len])
	}
}

func (e SimpleEncoder[T]) DecodeReusable(data []byte, values []T) {
	count := binary.LittleEndian.Uint64(data)
	len := e.elementSize
	for i := 0; i < int(count); i++ {
		e.decode(data[8+i*len:8+(i+1)*len], &values[i])
	}
}

func (e *SimpleEncoder[T]) Encode(values []T) []byte {
	e.EncodeReusable(values, e.buf)
	return e.buf
}

func (e *SimpleEncoder[T]) Decode(data []byte) []T {
	e.DecodeReusable(data, e.values)
	return e.values
}

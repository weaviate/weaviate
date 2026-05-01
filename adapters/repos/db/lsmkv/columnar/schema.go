//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package columnar

import (
	"encoding/binary"
	"fmt"
	"math"
)

// ColumnType identifies the fixed-width data type stored in a column.
type ColumnType uint8

const (
	ColumnTypeFloat32 ColumnType = 0
	ColumnTypeInt64   ColumnType = 1
)

func (ct ColumnType) Width() uint8 {
	switch ct {
	case ColumnTypeFloat32:
		return 4
	case ColumnTypeInt64:
		return 8
	default:
		panic(fmt.Sprintf("unknown column type %d", ct))
	}
}

func (ct ColumnType) String() string {
	switch ct {
	case ColumnTypeFloat32:
		return "float32"
	case ColumnTypeInt64:
		return "int64"
	default:
		return "unknown"
	}
}

// Column describes a single named column in the schema.
type Column struct {
	Name string
	Type ColumnType
}

// Schema defines the columns stored in a columnar bucket.
// The schema is fixed at bucket creation time and embedded in every segment.
type Schema struct {
	Columns []Column
}

// RowWidth returns the total byte width of all value columns (excluding docID and valid).
func (s *Schema) RowWidth() int {
	w := 0
	for _, c := range s.Columns {
		w += int(c.Type.Width())
	}
	return w
}

// ColOffset returns the byte offset within a row's value buffer for column colIdx.
func (s *Schema) ColOffset(colIdx int) int {
	off := 0
	for i := 0; i < colIdx; i++ {
		off += int(s.Columns[i].Type.Width())
	}
	return off
}

// EncodeFloat32 writes a float32 value into buf at the appropriate offset.
func EncodeFloat32(buf []byte, off int, v float32) {
	binary.LittleEndian.PutUint32(buf[off:], math.Float32bits(v))
}

// DecodeFloat32 reads a float32 value from buf at offset.
func DecodeFloat32(buf []byte, off int) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(buf[off:]))
}

// EncodeInt64 writes an int64 value into buf at the appropriate offset.
func EncodeInt64(buf []byte, off int, v int64) {
	binary.LittleEndian.PutUint64(buf[off:], uint64(v))
}

// DecodeInt64 reads an int64 value from buf at offset.
func DecodeInt64(buf []byte, off int) int64 {
	return int64(binary.LittleEndian.Uint64(buf[off:]))
}

// ColumnarHeader is the per-segment header written immediately after the
// standard LSM segment header. It describes the shape of the columnar data.
//
// Wire format:
//
//	numRows    uint64   (8 bytes)
//	numCols    uint16   (2 bytes)
//	for each column:
//	  nameLen  uint16   (2 bytes)
//	  name     []byte   (nameLen bytes)
//	  colType  uint8    (1 byte)
type ColumnarHeader struct {
	NumRows uint64
	Schema  Schema
}

// MarshalBinary serialises the columnar header.
func (h *ColumnarHeader) MarshalBinary() []byte {
	size := 8 + 2 // numRows + numCols
	for _, c := range h.Schema.Columns {
		size += 2 + len(c.Name) + 1 // nameLen + name + colType
	}
	buf := make([]byte, size)
	off := 0

	binary.LittleEndian.PutUint64(buf[off:], h.NumRows)
	off += 8
	binary.LittleEndian.PutUint16(buf[off:], uint16(len(h.Schema.Columns)))
	off += 2
	for _, c := range h.Schema.Columns {
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(c.Name)))
		off += 2
		copy(buf[off:], c.Name)
		off += len(c.Name)
		buf[off] = byte(c.Type)
		off++
	}
	return buf
}

// UnmarshalBinary reads a columnar header from buf, returning the header
// and the number of bytes consumed.
func UnmarshalColumnarHeader(buf []byte) (*ColumnarHeader, int, error) {
	if len(buf) < 10 {
		return nil, 0, fmt.Errorf("columnar header too short")
	}
	h := &ColumnarHeader{}
	off := 0

	h.NumRows = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	numCols := binary.LittleEndian.Uint16(buf[off:])
	off += 2

	h.Schema.Columns = make([]Column, numCols)
	for i := 0; i < int(numCols); i++ {
		if off+3 > len(buf) {
			return nil, 0, fmt.Errorf("columnar header truncated at column %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2
		if off+nameLen+1 > len(buf) {
			return nil, 0, fmt.Errorf("columnar header truncated at column %d name", i)
		}
		h.Schema.Columns[i].Name = string(buf[off : off+nameLen])
		off += nameLen
		h.Schema.Columns[i].Type = ColumnType(buf[off])
		off++
	}
	return h, off, nil
}

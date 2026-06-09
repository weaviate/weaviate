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
// Values are part of the on-disk format and must never be reused.
type ColumnType uint8

const (
	// ColumnTypeInt64 stores int64 values (weaviate types: int, date as
	// unix-nano timestamp).
	ColumnTypeInt64 ColumnType = 1
	// ColumnTypeFloat64 stores float64 values (weaviate type: number).
	// Storage is intentionally lossless: a float32 column would silently
	// truncate user data.
	ColumnTypeFloat64 ColumnType = 2
)

func (ct ColumnType) Width() int {
	switch ct {
	case ColumnTypeInt64, ColumnTypeFloat64:
		return 8
	default:
		panic(fmt.Sprintf("unknown column type %d", ct))
	}
}

func (ct ColumnType) String() string {
	switch ct {
	case ColumnTypeInt64:
		return "int64"
	case ColumnTypeFloat64:
		return "float64"
	default:
		return "unknown"
	}
}

// less compares two raw column values of this type. Used for min/max block
// statistics.
func (ct ColumnType) less(a, b uint64) bool {
	switch ct {
	case ColumnTypeInt64:
		return int64(a) < int64(b)
	case ColumnTypeFloat64:
		return math.Float64frombits(a) < math.Float64frombits(b)
	default:
		panic(fmt.Sprintf("unknown column type %d", ct))
	}
}

// Column describes a single named column in the schema.
type Column struct {
	Name string
	Type ColumnType
	// Encoding is the on-disk encoding of the column's data pages. Only
	// EncodingRawFixedWidth exists today; the field is in the wire format
	// so future encodings (delta, dict, FOR — and raw N-byte payloads for
	// vectors) don't require a format break.
	Encoding EncodingID
}

// Schema defines the columns stored in a columnar bucket. It is fixed at
// bucket creation time and embedded in every segment.
type Schema struct {
	Columns []Column
}

// RowWidth returns the total byte width of all value columns (excluding
// docID and the tombstone flag).
func (s *Schema) RowWidth() int {
	w := 0
	for _, c := range s.Columns {
		w += c.Type.Width()
	}
	return w
}

// ColOffset returns the byte offset within a row's value buffer for column
// colIdx.
func (s *Schema) ColOffset(colIdx int) int {
	off := 0
	for i := 0; i < colIdx; i++ {
		off += s.Columns[i].Type.Width()
	}
	return off
}

func (s *Schema) validate() error {
	if len(s.Columns) == 0 {
		return fmt.Errorf("columnar schema requires at least one column")
	}
	for i, c := range s.Columns {
		if c.Name == "" {
			return fmt.Errorf("column %d: empty name", i)
		}
		switch c.Type {
		case ColumnTypeInt64, ColumnTypeFloat64:
		default:
			return fmt.Errorf("column %q: unknown type %d", c.Name, c.Type)
		}
		if _, err := EncodingByID(c.Encoding); err != nil {
			return fmt.Errorf("column %q: %w", c.Name, err)
		}
	}
	return nil
}

func EncodeFloat64(buf []byte, off int, v float64) {
	binary.LittleEndian.PutUint64(buf[off:], math.Float64bits(v))
}

func DecodeFloat64(buf []byte, off int) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(buf[off:]))
}

func EncodeInt64(buf []byte, off int, v int64) {
	binary.LittleEndian.PutUint64(buf[off:], uint64(v))
}

func DecodeInt64(buf []byte, off int) int64 {
	return int64(binary.LittleEndian.Uint64(buf[off:]))
}

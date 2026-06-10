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
	// ColumnTypeVector stores one float32 vector of Column.Dims dimensions
	// per row (fixed width Dims×4). Stored verbatim: the column's purpose
	// is to absorb the quantization error of compressed vector indexes
	// during rescoring, so a lossy encoding here would defeat it. Block
	// min/max stats are meaningless for vectors and are not computed.
	ColumnTypeVector ColumnType = 3
	// ColumnTypeMultiVector stores a variable number of float32 vectors
	// (token matrix, Dims columns each) per row, encoded via
	// EncodingOffsetValues. Same losslessness contract as ColumnTypeVector.
	ColumnTypeMultiVector ColumnType = 4
)

// fixedWidth returns the per-row byte width for fixed-width types, or -1
// for variable-width types. Vector widths depend on Column.Dims; use
// Column.Width.
func (ct ColumnType) fixedWidth(dims int) int {
	switch ct {
	case ColumnTypeInt64, ColumnTypeFloat64:
		return 8
	case ColumnTypeVector:
		return dims * 4
	case ColumnTypeMultiVector:
		return -1
	default:
		panic(fmt.Sprintf("unknown column type %d", ct))
	}
}

// isVector reports whether the type holds float32 vector payloads.
func (ct ColumnType) isVector() bool {
	return ct == ColumnTypeVector || ct == ColumnTypeMultiVector
}

func (ct ColumnType) String() string {
	switch ct {
	case ColumnTypeInt64:
		return "int64"
	case ColumnTypeFloat64:
		return "float64"
	case ColumnTypeVector:
		return "vector"
	case ColumnTypeMultiVector:
		return "multivector"
	default:
		return "unknown"
	}
}

// less compares two raw column values of this type. Used for min/max block
// statistics. For float64, NaN compares false against everything (standard
// IEEE 754 semantics) — callers accumulating stats must skip NaN inputs, or
// a NaN that seeds min/max would freeze it (see BlockWriter.flushBlock).
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
	// Encoding is the on-disk encoding of the column's data pages:
	// EncodingRawFixedWidth for scalars and single vectors,
	// EncodingOffsetValues for multi-vectors.
	Encoding EncodingID
	// Dims is the vector dimensionality for vector column types; 0 for
	// scalars. Part of the wire format (header v2).
	Dims uint32
}

// Width returns the per-row byte width, or -1 for variable-width columns.
func (c *Column) Width() int {
	return c.Type.fixedWidth(int(c.Dims))
}

// IsVariable reports whether rows of this column have variable byte width.
func (c *Column) IsVariable() bool { return c.Width() < 0 }

// Schema defines the columns stored in a columnar bucket. It is fixed at
// bucket creation time and embedded in every segment.
type Schema struct {
	Columns []Column
}

// RowWidth returns the total byte width of all value columns (excluding
// docID and the tombstone flag). Only valid for all-fixed-width schemas;
// variable-width (multi-vector) schemas return -1.
func (s *Schema) RowWidth() int {
	w := 0
	for i := range s.Columns {
		cw := s.Columns[i].Width()
		if cw < 0 {
			return -1
		}
		w += cw
	}
	return w
}

// ColOffset returns the byte offset within a row's value buffer for column
// colIdx. Only valid for all-fixed-width schemas.
func (s *Schema) ColOffset(colIdx int) int {
	off := 0
	for i := 0; i < colIdx; i++ {
		off += s.Columns[i].Width()
	}
	return off
}

// IsVector reports whether this schema holds a single vector or
// multi-vector column.
func (s *Schema) IsVector() bool {
	return len(s.Columns) == 1 && s.Columns[0].Type.isVector()
}

// Equal reports whether two schemas describe the same column layout:
// same column count, and per column the same name, type, encoding, and dims.
// Used by the compactor to assert that two segments are mergeable —
// merging segments with diverging schemas would silently misinterpret
// column offsets.
func (s *Schema) Equal(other *Schema) bool {
	if other == nil || len(s.Columns) != len(other.Columns) {
		return false
	}
	for i := range s.Columns {
		if s.Columns[i] != other.Columns[i] {
			return false
		}
	}
	return true
}

func (s *Schema) validate() error {
	if len(s.Columns) == 0 {
		return fmt.Errorf("columnar schema requires at least one column")
	}
	for i := range s.Columns {
		c := &s.Columns[i]
		if c.Name == "" {
			return fmt.Errorf("column %d: empty name", i)
		}
		switch c.Type {
		case ColumnTypeInt64, ColumnTypeFloat64:
			if c.Dims != 0 {
				return fmt.Errorf("column %q: dims set on scalar column", c.Name)
			}
			if c.Encoding == EncodingOffsetValues {
				return fmt.Errorf("column %q: offset+values encoding requires a multi-vector column", c.Name)
			}
		case ColumnTypeVector:
			if c.Dims == 0 {
				return fmt.Errorf("column %q: vector column requires dims", c.Name)
			}
			// lossless always: the column absorbs index quantization error
			if c.Encoding != EncodingRawFixedWidth {
				return fmt.Errorf("column %q: vector columns only support raw fixed-width encoding", c.Name)
			}
			if len(s.Columns) != 1 {
				return fmt.Errorf("vector column %q must be the only column in its bucket", c.Name)
			}
		case ColumnTypeMultiVector:
			if c.Dims == 0 {
				return fmt.Errorf("column %q: multi-vector column requires dims", c.Name)
			}
			if c.Encoding != EncodingOffsetValues {
				return fmt.Errorf("column %q: multi-vector columns require offset+values encoding", c.Name)
			}
			if len(s.Columns) != 1 {
				return fmt.Errorf("multi-vector column %q must be the only column in its bucket", c.Name)
			}
		default:
			return fmt.Errorf("column %q: unknown type %d", c.Name, c.Type)
		}
		if !ValidEncoding(c.Encoding) {
			return fmt.Errorf("column %q: unknown encoding %d", c.Name, c.Encoding)
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

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// i64bits converts an int64 to its raw uint64 representation. A plain
// uint64(int64(-1)) on constants does not compile (constant overflow), so
// tests funnel negative values through this helper.
func i64bits(v int64) uint64 { return uint64(v) }

// marshalHeaderV1 hand-crafts a v1 columnar header (version byte 1, no
// per-column dims field) so tests can verify the v2 reader still accepts
// segments written before the vector-column format change.
func marshalHeaderV1(s *Schema) []byte {
	size := 1 + 2
	for _, c := range s.Columns {
		size += 2 + len(c.Name) + 1 + 1
	}
	buf := make([]byte, size)
	buf[0] = formatVersionV1
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(s.Columns)))
	off := 3
	for _, c := range s.Columns {
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(c.Name)))
		off += 2
		copy(buf[off:], c.Name)
		off += len(c.Name)
		buf[off] = byte(c.Type)
		off++
		buf[off] = byte(c.Encoding)
		off++
	}
	return buf
}

// marshalDirectoryV1 hand-crafts v1 directory entries (stats per column, no
// page table).
func marshalDirectoryV1(entries []DirectoryEntry, numCols int) []byte {
	entrySize := directoryEntrySize(numCols, formatVersionV1)
	buf := make([]byte, entrySize*len(entries))
	for i := range entries {
		e := &entries[i]
		off := i * entrySize
		binary.LittleEndian.PutUint64(buf[off:], e.StartDocID)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], e.EndDocID)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], e.Offset)
		off += 8
		binary.LittleEndian.PutUint32(buf[off:], e.RowCount)
		off += 4
		binary.LittleEndian.PutUint32(buf[off:], e.LiveCount)
		off += 4
		for c := 0; c < numCols; c++ {
			binary.LittleEndian.PutUint64(buf[off:], e.Stats[c].Min)
			off += 8
			binary.LittleEndian.PutUint64(buf[off:], e.Stats[c].Max)
			off += 8
		}
	}
	return buf
}

func TestColumn_Width(t *testing.T) {
	tests := []struct {
		name       string
		col        Column
		width      int
		isVariable bool
	}{
		{name: "int64", col: Column{Type: ColumnTypeInt64}, width: 8},
		{name: "float64", col: Column{Type: ColumnTypeFloat64}, width: 8},
		{name: "vector dims 8", col: Column{Type: ColumnTypeVector, Dims: 8}, width: 32},
		{name: "vector dims 1536", col: Column{Type: ColumnTypeVector, Dims: 1536}, width: 6144},
		{name: "multivector is variable", col: Column{Type: ColumnTypeMultiVector, Dims: 128}, width: -1, isVariable: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.width, tt.col.Width())
			assert.Equal(t, tt.isVariable, tt.col.IsVariable())
		})
	}

	t.Run("unknown type panics", func(t *testing.T) {
		// 3 and 4 became valid in v2 (vector, multivector); only genuinely
		// unknown ids panic.
		assert.Panics(t, func() { (&Column{Type: ColumnType(0)}).Width() })
		assert.Panics(t, func() { (&Column{Type: ColumnType(5)}).Width() })
		assert.Panics(t, func() { (&Column{Type: ColumnType(255)}).Width() })
	})
}

func TestColumnType_String(t *testing.T) {
	tests := []struct {
		ct   ColumnType
		want string
	}{
		{ct: ColumnTypeInt64, want: "int64"},
		{ct: ColumnTypeFloat64, want: "float64"},
		{ct: ColumnTypeVector, want: "vector"},
		{ct: ColumnTypeMultiVector, want: "multivector"},
		{ct: ColumnType(0), want: "unknown"},
		{ct: ColumnType(5), want: "unknown"},
		{ct: ColumnType(255), want: "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.ct.String())
		})
	}
}

func TestColumnType_less(t *testing.T) {
	i64 := func(v int64) uint64 { return uint64(v) }
	f64 := math.Float64bits

	tests := []struct {
		name string
		ct   ColumnType
		a, b uint64
		want bool
	}{
		// int64: must compare signed, not as raw uint64 bits
		{name: "int64 negative < positive", ct: ColumnTypeInt64, a: i64(-5), b: i64(3), want: true},
		{name: "int64 positive not < negative", ct: ColumnTypeInt64, a: i64(3), b: i64(-5), want: false},
		{name: "int64 more negative < less negative", ct: ColumnTypeInt64, a: i64(-100), b: i64(-1), want: true},
		{name: "int64 equal negatives", ct: ColumnTypeInt64, a: i64(-7), b: i64(-7), want: false},
		{name: "int64 MinInt64 < MaxInt64", ct: ColumnTypeInt64, a: i64(math.MinInt64), b: i64(math.MaxInt64), want: true},
		{name: "int64 MaxInt64 not < MinInt64", ct: ColumnTypeInt64, a: i64(math.MaxInt64), b: i64(math.MinInt64), want: false},
		{name: "int64 zero not < zero", ct: ColumnTypeInt64, a: i64(0), b: i64(0), want: false},
		{name: "int64 -1 < 0", ct: ColumnTypeInt64, a: i64(-1), b: i64(0), want: true},

		// float64: must compare as floats, not as raw uint64 bits
		{name: "float64 negative < positive", ct: ColumnTypeFloat64, a: f64(-1.5), b: f64(2.5), want: true},
		{name: "float64 positive not < negative", ct: ColumnTypeFloat64, a: f64(2.5), b: f64(-1.5), want: false},
		{name: "float64 -Inf < everything finite", ct: ColumnTypeFloat64, a: f64(math.Inf(-1)), b: f64(-math.MaxFloat64), want: true},
		{name: "float64 everything finite < +Inf", ct: ColumnTypeFloat64, a: f64(math.MaxFloat64), b: f64(math.Inf(1)), want: true},
		{name: "float64 -Inf < +Inf", ct: ColumnTypeFloat64, a: f64(math.Inf(-1)), b: f64(math.Inf(1)), want: true},
		{name: "float64 +Inf not < +Inf", ct: ColumnTypeFloat64, a: f64(math.Inf(1)), b: f64(math.Inf(1)), want: false},
		{name: "float64 negative zero not < positive zero", ct: ColumnTypeFloat64, a: f64(math.Copysign(0, -1)), b: f64(0), want: false},

		// NaN ordering caveat: less uses Go's < on float64, so ANY
		// comparison involving NaN yields false (IEEE 754 semantics).
		// Consequence for block stats: if the first live value in a block
		// is NaN, Min and Max stay NaN forever (nothing is ever "less"
		// than or "greater" than NaN); if NaN appears later it never
		// becomes Min or Max. Block-stat pruning on NaN-containing
		// columns is therefore unreliable — these cases pin down that
		// behavior so a future change to it is a conscious one.
		{name: "float64 NaN not < value", ct: ColumnTypeFloat64, a: f64(math.NaN()), b: f64(1), want: false},
		{name: "float64 value not < NaN", ct: ColumnTypeFloat64, a: f64(1), b: f64(math.NaN()), want: false},
		{name: "float64 NaN not < NaN", ct: ColumnTypeFloat64, a: f64(math.NaN()), b: f64(math.NaN()), want: false},
		{name: "float64 NaN not < -Inf", ct: ColumnTypeFloat64, a: f64(math.NaN()), b: f64(math.Inf(-1)), want: false},
		{name: "float64 -Inf not < NaN", ct: ColumnTypeFloat64, a: f64(math.Inf(-1)), b: f64(math.NaN()), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.ct.less(tt.a, tt.b))
		})
	}

	t.Run("non-scalar types panic", func(t *testing.T) {
		// vector columns carry no stats, so less is undefined for them
		assert.Panics(t, func() { ColumnType(0).less(1, 2) })
		assert.Panics(t, func() { ColumnTypeVector.less(1, 2) })
		assert.Panics(t, func() { ColumnTypeMultiVector.less(1, 2) })
		assert.Panics(t, func() { ColumnType(255).less(1, 2) })
	})
}

func TestSchema_RowWidthColOffset(t *testing.T) {
	tests := []struct {
		name        string
		schema      Schema
		rowWidth    int
		wantOffsets []int
	}{
		{
			name:        "empty schema",
			schema:      Schema{},
			rowWidth:    0,
			wantOffsets: nil,
		},
		{
			name: "single int64 column",
			schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnTypeInt64},
			}},
			rowWidth:    8,
			wantOffsets: []int{0},
		},
		{
			name: "single float64 column",
			schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnTypeFloat64},
			}},
			rowWidth:    8,
			wantOffsets: []int{0},
		},
		{
			name: "three mixed columns",
			schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnTypeInt64},
				{Name: "b", Type: ColumnTypeFloat64},
				{Name: "c", Type: ColumnTypeInt64},
			}},
			rowWidth:    24,
			wantOffsets: []int{0, 8, 16},
		},
		{
			name: "single vector column",
			schema: Schema{Columns: []Column{
				{Name: "vec", Type: ColumnTypeVector, Encoding: EncodingRawFixedWidth, Dims: 8},
			}},
			rowWidth:    32,
			wantOffsets: []int{0},
		},
		{
			name: "multi-vector schema is variable width",
			schema: Schema{Columns: []Column{
				{Name: "mv", Type: ColumnTypeMultiVector, Encoding: EncodingOffsetValues, Dims: 128},
			}},
			rowWidth:    -1,
			wantOffsets: nil, // ColOffset is undefined for variable schemas
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.rowWidth, tt.schema.RowWidth())
			for i, want := range tt.wantOffsets {
				assert.Equal(t, want, tt.schema.ColOffset(i), "column %d", i)
			}
		})
	}
}

func TestSchema_IsVector(t *testing.T) {
	tests := []struct {
		name   string
		schema Schema
		want   bool
	}{
		{
			name: "single vector column",
			schema: Schema{Columns: []Column{
				{Name: "v", Type: ColumnTypeVector, Encoding: EncodingRawFixedWidth, Dims: 4},
			}},
			want: true,
		},
		{
			name: "single multi-vector column",
			schema: Schema{Columns: []Column{
				{Name: "mv", Type: ColumnTypeMultiVector, Encoding: EncodingOffsetValues, Dims: 4},
			}},
			want: true,
		},
		{
			name: "single scalar column",
			schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnTypeInt64},
			}},
			want: false,
		},
		{
			name: "vector column not alone",
			schema: Schema{Columns: []Column{
				{Name: "v", Type: ColumnTypeVector, Dims: 4},
				{Name: "a", Type: ColumnTypeInt64},
			}},
			want: false, // invalid schema, but IsVector must not claim it
		},
		{name: "empty schema", schema: Schema{}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.schema.IsVector())
		})
	}
}

func TestSchema_Validate(t *testing.T) {
	tests := []struct {
		name    string
		schema  Schema
		errLike string // empty = valid
	}{
		{
			name: "single scalar",
			schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnTypeInt64, Encoding: EncodingRawFixedWidth},
			}},
		},
		{
			name: "multiple scalars",
			schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnTypeInt64},
				{Name: "b", Type: ColumnTypeFloat64},
			}},
		},
		{
			name: "vector with dims and raw encoding",
			schema: Schema{Columns: []Column{
				{Name: "v", Type: ColumnTypeVector, Encoding: EncodingRawFixedWidth, Dims: 1536},
			}},
		},
		{
			name: "multi-vector with dims and offset+values encoding",
			schema: Schema{Columns: []Column{
				{Name: "mv", Type: ColumnTypeMultiVector, Encoding: EncodingOffsetValues, Dims: 128},
			}},
		},
		{
			name:    "empty schema",
			schema:  Schema{},
			errLike: "at least one column",
		},
		{
			name: "empty column name",
			schema: Schema{Columns: []Column{
				{Name: "", Type: ColumnTypeInt64},
			}},
			errLike: "empty name",
		},
		{
			name: "scalar with dims set",
			schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnTypeInt64, Dims: 8},
			}},
			errLike: "dims set on scalar",
		},
		{
			name: "scalar with offset+values encoding",
			schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnTypeFloat64, Encoding: EncodingOffsetValues},
			}},
			errLike: "requires a multi-vector column",
		},
		{
			name: "vector without dims",
			schema: Schema{Columns: []Column{
				{Name: "v", Type: ColumnTypeVector, Encoding: EncodingRawFixedWidth},
			}},
			errLike: "requires dims",
		},
		{
			name: "vector with offset+values encoding",
			schema: Schema{Columns: []Column{
				{Name: "v", Type: ColumnTypeVector, Encoding: EncodingOffsetValues, Dims: 8},
			}},
			errLike: "raw fixed-width",
		},
		{
			name: "vector not the only column",
			schema: Schema{Columns: []Column{
				{Name: "v", Type: ColumnTypeVector, Encoding: EncodingRawFixedWidth, Dims: 8},
				{Name: "a", Type: ColumnTypeInt64},
			}},
			errLike: "must be the only column",
		},
		{
			name: "multi-vector without dims",
			schema: Schema{Columns: []Column{
				{Name: "mv", Type: ColumnTypeMultiVector, Encoding: EncodingOffsetValues},
			}},
			errLike: "requires dims",
		},
		{
			name: "multi-vector with raw encoding",
			schema: Schema{Columns: []Column{
				{Name: "mv", Type: ColumnTypeMultiVector, Encoding: EncodingRawFixedWidth, Dims: 8},
			}},
			errLike: "offset+values encoding",
		},
		{
			name: "multi-vector not the only column",
			schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnTypeInt64},
				{Name: "mv", Type: ColumnTypeMultiVector, Encoding: EncodingOffsetValues, Dims: 8},
			}},
			errLike: "must be the only column",
		},
		{
			name: "unknown column type",
			schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnType(99)},
			}},
			errLike: "unknown type",
		},
		{
			name: "unknown encoding on scalar",
			schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnTypeInt64, Encoding: EncodingID(7)},
			}},
			errLike: "unknown encoding",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.schema.validate()
			if tt.errLike == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.errLike)
		})
	}
}

func TestBlockSizeFor(t *testing.T) {
	scalar := &Schema{Columns: []Column{{Name: "a", Type: ColumnTypeInt64}}}
	vector := &Schema{Columns: []Column{
		{Name: "v", Type: ColumnTypeVector, Encoding: EncodingRawFixedWidth, Dims: 8},
	}}
	multiVector := &Schema{Columns: []Column{
		{Name: "mv", Type: ColumnTypeMultiVector, Encoding: EncodingOffsetValues, Dims: 8},
	}}

	assert.Equal(t, BlockSize, BlockSizeFor(scalar))
	assert.Equal(t, VectorBlockSize, BlockSizeFor(vector))
	assert.Equal(t, VectorBlockSize, BlockSizeFor(multiVector))
}

func TestHeader_MarshalUnmarshalRoundtrip(t *testing.T) {
	tests := []struct {
		name   string
		header Header
	}{
		{
			name: "single column",
			header: Header{Schema: Schema{Columns: []Column{
				{Name: "val", Type: ColumnTypeInt64, Encoding: EncodingRawFixedWidth},
			}}},
		},
		{
			name: "multiple mixed columns",
			header: Header{Schema: Schema{Columns: []Column{
				{Name: "a", Type: ColumnTypeInt64, Encoding: EncodingRawFixedWidth},
				{Name: "b", Type: ColumnTypeFloat64, Encoding: EncodingRawFixedWidth},
				{Name: "c", Type: ColumnTypeInt64, Encoding: EncodingRawFixedWidth},
			}}},
		},
		{
			name: "long and non-ascii column names",
			header: Header{Schema: Schema{Columns: []Column{
				{Name: "property_with_a_rather_long_name_to_exercise_the_length_prefix", Type: ColumnTypeFloat64},
				{Name: "ünïcödé", Type: ColumnTypeInt64},
			}}},
		},
		{
			name: "vector column preserves dims",
			header: Header{Schema: Schema{Columns: []Column{
				{Name: "vec", Type: ColumnTypeVector, Encoding: EncodingRawFixedWidth, Dims: 1536},
			}}},
		},
		{
			name: "multi-vector column preserves dims",
			header: Header{Schema: Schema{Columns: []Column{
				{Name: "colbert", Type: ColumnTypeMultiVector, Encoding: EncodingOffsetValues, Dims: 128},
			}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.header.MarshalBinary()

			got, n, version, err := UnmarshalHeader(buf)
			require.NoError(t, err)
			assert.Equal(t, len(buf), n, "must consume the entire marshaled header")
			assert.Equal(t, formatVersion, version, "writers always emit v2")
			assert.Equal(t, tt.header.Schema, got.Schema)

			t.Run("trailing data is not consumed", func(t *testing.T) {
				withTrailer := append(append([]byte{}, buf...), 0xde, 0xad, 0xbe, 0xef)
				got2, n2, _, err := UnmarshalHeader(withTrailer)
				require.NoError(t, err)
				assert.Equal(t, len(buf), n2)
				assert.Equal(t, tt.header.Schema, got2.Schema)
			})

			t.Run("every truncation errors", func(t *testing.T) {
				for cut := 0; cut < len(buf); cut++ {
					_, _, _, err := UnmarshalHeader(buf[:cut])
					assert.Error(t, err, "truncated to %d of %d bytes must error", cut, len(buf))
				}
			})
		})
	}
}

func TestUnmarshalHeader_V1(t *testing.T) {
	t.Run("scalar v1 header parses with zero dims", func(t *testing.T) {
		schema := Schema{Columns: []Column{
			{Name: "a", Type: ColumnTypeInt64, Encoding: EncodingRawFixedWidth},
			{Name: "b", Type: ColumnTypeFloat64, Encoding: EncodingRawFixedWidth},
		}}
		buf := marshalHeaderV1(&schema)

		got, n, version, err := UnmarshalHeader(buf)
		require.NoError(t, err)
		assert.Equal(t, len(buf), n, "must consume the entire v1 header")
		assert.Equal(t, formatVersionV1, version)
		assert.Equal(t, schema, got.Schema, "v1 columns have Dims 0")

		t.Run("trailing data is not consumed", func(t *testing.T) {
			withTrailer := append(append([]byte{}, buf...), 0xff, 0xff)
			_, n2, version2, err := UnmarshalHeader(withTrailer)
			require.NoError(t, err)
			assert.Equal(t, len(buf), n2)
			assert.Equal(t, formatVersionV1, version2)
		})

		t.Run("every truncation errors", func(t *testing.T) {
			for cut := 0; cut < len(buf); cut++ {
				_, _, _, err := UnmarshalHeader(buf[:cut])
				assert.Error(t, err, "truncated to %d of %d bytes must error", cut, len(buf))
			}
		})
	})

	t.Run("v1 header is shorter than v2 by 4 bytes per column", func(t *testing.T) {
		schema := Schema{Columns: []Column{
			{Name: "a", Type: ColumnTypeInt64},
			{Name: "bb", Type: ColumnTypeFloat64},
		}}
		v1 := marshalHeaderV1(&schema)
		v2 := (&Header{Schema: schema}).MarshalBinary()
		assert.Equal(t, len(v1)+4*len(schema.Columns), len(v2))
	})

	t.Run("v1 cannot carry vector columns", func(t *testing.T) {
		// a v1 header has no dims field, so a vector column read from it
		// always fails the dims requirement — vector segments cannot
		// masquerade as v1
		buf := marshalHeaderV1(&Schema{Columns: []Column{
			{Name: "v", Type: ColumnTypeVector, Encoding: EncodingRawFixedWidth},
		}})
		_, _, _, err := UnmarshalHeader(buf)
		require.Error(t, err)
		assert.ErrorContains(t, err, "requires dims")
	})
}

func TestUnmarshalHeader_ErrorCases(t *testing.T) {
	validHeader := func() []byte {
		h := Header{Schema: Schema{Columns: []Column{
			{Name: "a", Type: ColumnTypeInt64, Encoding: EncodingRawFixedWidth},
			{Name: "b", Type: ColumnTypeFloat64, Encoding: EncodingRawFixedWidth},
		}}}
		return h.MarshalBinary()
	}

	tests := []struct {
		name    string
		buf     func() []byte
		errLike string
	}{
		{
			name:    "empty buffer",
			buf:     func() []byte { return nil },
			errLike: "too short",
		},
		{
			name:    "one byte",
			buf:     func() []byte { return []byte{1} },
			errLike: "too short",
		},
		{
			name:    "two bytes",
			buf:     func() []byte { return []byte{1, 0} },
			errLike: "too short",
		},
		{
			name: "unknown version",
			buf: func() []byte {
				b := validHeader()
				b[0] = 99
				return b
			},
			errLike: "unsupported columnar format version",
		},
		{
			name: "version zero",
			buf: func() []byte {
				b := validHeader()
				b[0] = 0
				return b
			},
			errLike: "unsupported columnar format version",
		},
		{
			name: "zero columns fails validation",
			buf: func() []byte {
				return []byte{1, 0, 0} // version 1, numCols 0
			},
			errLike: "at least one column",
		},
		{
			name: "empty column name fails validation",
			buf: func() []byte {
				// version 1, numCols 1, nameLen 0, type int64, raw encoding
				return []byte{1, 1, 0, 0, 0, byte(ColumnTypeInt64), byte(EncodingRawFixedWidth)}
			},
			errLike: "empty name",
		},
		{
			name: "unknown column type",
			buf: func() []byte {
				b := validHeader()
				// column "a": version(1) + numCols(2) + nameLen(2) + name(1) = byte 6 is its type
				require.Equal(t, byte(ColumnTypeInt64), b[6], "fixture drifted: expected type byte at offset 6")
				b[6] = 200
				return b
			},
			errLike: "unknown type",
		},
		{
			name: "unknown encoding",
			buf: func() []byte {
				b := validHeader()
				require.Equal(t, byte(EncodingRawFixedWidth), b[7], "fixture drifted: expected encoding byte at offset 7")
				b[7] = 200
				return b
			},
			errLike: "unknown encoding",
		},
		{
			name: "numCols larger than data",
			buf: func() []byte {
				b := validHeader()
				binary.LittleEndian.PutUint16(b[1:], 50)
				return b
			},
			errLike: "truncated",
		},
		{
			name: "name length overflows buffer",
			buf: func() []byte {
				b := validHeader()
				binary.LittleEndian.PutUint16(b[3:], 60000) // first column's nameLen
				return b
			},
			errLike: "truncated",
		},
		// v2 schema-validation failures surfaced through the header reader:
		// MarshalBinary does not validate, so invalid schemas roundtrip into
		// the unmarshal-side validation
		{
			name: "vector column without dims",
			buf: func() []byte {
				h := Header{Schema: Schema{Columns: []Column{
					{Name: "v", Type: ColumnTypeVector, Encoding: EncodingRawFixedWidth},
				}}}
				return h.MarshalBinary()
			},
			errLike: "requires dims",
		},
		{
			name: "vector column with wrong encoding",
			buf: func() []byte {
				h := Header{Schema: Schema{Columns: []Column{
					{Name: "v", Type: ColumnTypeVector, Encoding: EncodingOffsetValues, Dims: 8},
				}}}
				return h.MarshalBinary()
			},
			errLike: "raw fixed-width",
		},
		{
			name: "vector column with siblings",
			buf: func() []byte {
				h := Header{Schema: Schema{Columns: []Column{
					{Name: "v", Type: ColumnTypeVector, Encoding: EncodingRawFixedWidth, Dims: 8},
					{Name: "a", Type: ColumnTypeInt64},
				}}}
				return h.MarshalBinary()
			},
			errLike: "must be the only column",
		},
		{
			name: "multi-vector column with raw encoding",
			buf: func() []byte {
				h := Header{Schema: Schema{Columns: []Column{
					{Name: "mv", Type: ColumnTypeMultiVector, Encoding: EncodingRawFixedWidth, Dims: 8},
				}}}
				return h.MarshalBinary()
			},
			errLike: "offset+values encoding",
		},
		{
			name: "scalar column with dims",
			buf: func() []byte {
				h := Header{Schema: Schema{Columns: []Column{
					{Name: "a", Type: ColumnTypeInt64, Encoding: EncodingRawFixedWidth, Dims: 8},
				}}}
				return h.MarshalBinary()
			},
			errLike: "dims set on scalar",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, n, _, err := UnmarshalHeader(tt.buf())
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.errLike)
			assert.Nil(t, h)
			assert.Equal(t, 0, n)
		})
	}
}

func TestDirectory_MarshalUnmarshal(t *testing.T) {
	threeScalars := &Schema{Columns: []Column{
		{Name: "a", Type: ColumnTypeInt64},
		{Name: "b", Type: ColumnTypeFloat64},
		{Name: "c", Type: ColumnTypeInt64},
	}}

	t.Run("entry size accounts for per-column stats and pages", func(t *testing.T) {
		// 8 (start) + 8 (end) + 8 (offset) + 4 (rows) + 4 (live) = 32 fixed;
		// v2 adds 16 (stats) + 8 (page offset+size) per column, v1 only 16
		assert.Equal(t, 32, directoryEntrySize(0, formatVersion))
		assert.Equal(t, 56, directoryEntrySize(1, formatVersion))
		assert.Equal(t, 104, directoryEntrySize(3, formatVersion))

		assert.Equal(t, 32, directoryEntrySize(0, formatVersionV1))
		assert.Equal(t, 48, directoryEntrySize(1, formatVersionV1))
		assert.Equal(t, 80, directoryEntrySize(3, formatVersionV1))
	})

	t.Run("roundtrip with extreme values", func(t *testing.T) {
		entries := []DirectoryEntry{
			{
				StartDocID: 0, EndDocID: math.MaxUint64, Offset: math.MaxUint64,
				RowCount: math.MaxUint32, LiveCount: 0,
				Stats: []ColStats{
					{Min: i64bits(math.MinInt64), Max: i64bits(math.MaxInt64)},
					{Min: math.Float64bits(math.Inf(-1)), Max: math.Float64bits(math.NaN())},
					{Min: 0, Max: 0},
				},
				Pages: []ColPage{
					{Offset: 0, Size: math.MaxUint32},
					{Offset: math.MaxUint32, Size: 0},
					{Offset: 64, Size: 4096},
				},
			},
			{
				StartDocID: 1, EndDocID: 2, Offset: 3, RowCount: 4, LiveCount: 4,
				Stats: []ColStats{{Min: 5, Max: 6}, {Min: 7, Max: 8}, {Min: 9, Max: 10}},
				Pages: []ColPage{{Offset: 36, Size: 32}, {Offset: 68, Size: 32}, {Offset: 100, Size: 32}},
			},
		}
		buf := MarshalDirectory(entries, 3)
		require.Len(t, buf, 2*directoryEntrySize(3, formatVersion))

		got, err := UnmarshalDirectory(buf, threeScalars, formatVersion)
		require.NoError(t, err)
		assert.Equal(t, entries, got)
	})

	t.Run("entries without page table panic", func(t *testing.T) {
		// v2 contract: MarshalDirectory requires entries[i].Pages populated
		// (one ColPage per column). Hand-built entries that only carry stats
		// are a programming error and panic rather than writing a corrupt
		// directory.
		assert.Panics(t, func() {
			MarshalDirectory([]DirectoryEntry{{
				Stats: []ColStats{{Min: 1, Max: 2}},
			}}, 1)
		})
	})

	t.Run("empty directory is valid", func(t *testing.T) {
		buf := MarshalDirectory(nil, 2)
		assert.Empty(t, buf)
		twoScalars := &Schema{Columns: threeScalars.Columns[:2]}
		got, err := UnmarshalDirectory(nil, twoScalars, formatVersion)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("every non-multiple length errors", func(t *testing.T) {
		oneScalar := &Schema{Columns: threeScalars.Columns[:1]}
		entries := []DirectoryEntry{{
			StartDocID: 1, EndDocID: 2, Offset: 3, RowCount: 4, LiveCount: 4,
			Stats: []ColStats{{Min: 5, Max: 6}},
			Pages: []ColPage{{Offset: 36, Size: 32}},
		}}
		buf := MarshalDirectory(entries, 1)
		entrySize := directoryEntrySize(1, formatVersion)
		require.Len(t, buf, entrySize)

		for cut := 1; cut < entrySize; cut++ {
			_, err := UnmarshalDirectory(buf[:cut], oneScalar, formatVersion)
			assert.Error(t, err, "length %d must not be a valid directory", cut)
			assert.ErrorContains(t, err, "not a multiple")
		}

		// one byte beyond a full entry is also invalid
		_, err := UnmarshalDirectory(append(append([]byte{}, buf...), 0), oneScalar, formatVersion)
		require.Error(t, err)
	})

	t.Run("schema mismatch that changes entry size errors", func(t *testing.T) {
		buf := MarshalDirectory([]DirectoryEntry{{
			Stats: []ColStats{{}, {}},
			Pages: []ColPage{{}, {}},
		}}, 2) // 80 bytes
		_, err := UnmarshalDirectory(buf, threeScalars, formatVersion) // entry size 104
		require.Error(t, err)
	})
}

func TestDirectory_V1DerivesPages(t *testing.T) {
	t.Run("two scalar columns", func(t *testing.T) {
		schema := &Schema{Columns: []Column{
			{Name: "a", Type: ColumnTypeInt64},
			{Name: "b", Type: ColumnTypeFloat64},
		}}
		entries := []DirectoryEntry{
			{
				StartDocID: 10, EndDocID: 13, Offset: 1000, RowCount: 4, LiveCount: 3,
				Stats: []ColStats{{Min: 1, Max: 9}, {Min: 2, Max: 8}},
			},
			{
				StartDocID: 20, EndDocID: 29, Offset: 2000, RowCount: 10, LiveCount: 10,
				Stats: []ColStats{{Min: 3, Max: 7}, {Min: 4, Max: 6}},
			},
		}
		buf := marshalDirectoryV1(entries, 2)
		require.Len(t, buf, 2*directoryEntrySize(2, formatVersionV1))

		got, err := UnmarshalDirectory(buf, schema, formatVersionV1)
		require.NoError(t, err)
		require.Len(t, got, 2)

		// v1 scalar block layout: [docIDs 8B×rows][live 1B×rows][col pages
		// contiguous, no padding] — pages derive deterministically
		assert.Equal(t, []ColPage{
			{Offset: 4*8 + 4, Size: 4 * 8},       // 36, 32
			{Offset: 4*8 + 4 + 4*8, Size: 4 * 8}, // 68, 32
		}, got[0].Pages)
		assert.Equal(t, []ColPage{
			{Offset: 10*8 + 10, Size: 10 * 8},        // 90, 80
			{Offset: 10*8 + 10 + 10*8, Size: 10 * 8}, // 170, 80
		}, got[1].Pages)

		// everything else roundtrips unchanged
		for i := range entries {
			assert.Equal(t, entries[i].StartDocID, got[i].StartDocID)
			assert.Equal(t, entries[i].EndDocID, got[i].EndDocID)
			assert.Equal(t, entries[i].Offset, got[i].Offset)
			assert.Equal(t, entries[i].RowCount, got[i].RowCount)
			assert.Equal(t, entries[i].LiveCount, got[i].LiveCount)
			assert.Equal(t, entries[i].Stats, got[i].Stats)
		}
	})

	t.Run("derivePagesV1 directly", func(t *testing.T) {
		schema := &Schema{Columns: []Column{
			{Name: "a", Type: ColumnTypeInt64},
			{Name: "b", Type: ColumnTypeFloat64},
			{Name: "c", Type: ColumnTypeInt64},
		}}
		e := DirectoryEntry{RowCount: 7, Pages: make([]ColPage, 3)}
		derivePagesV1(&e, schema)
		assert.Equal(t, []ColPage{
			{Offset: 7*8 + 7, Size: 56},
			{Offset: 7*8 + 7 + 56, Size: 56},
			{Offset: 7*8 + 7 + 112, Size: 56},
		}, e.Pages)
	})

	t.Run("v1 length validation uses v1 entry size", func(t *testing.T) {
		schema := &Schema{Columns: []Column{{Name: "a", Type: ColumnTypeInt64}}}
		entries := []DirectoryEntry{{
			RowCount: 1, Stats: []ColStats{{}},
		}}
		buf := marshalDirectoryV1(entries, 1)
		require.Len(t, buf, 48)

		// a v1-sized directory is not a multiple of the v2 entry size (56)
		_, err := UnmarshalDirectory(buf, schema, formatVersion)
		require.Error(t, err)

		got, err := UnmarshalDirectory(buf, schema, formatVersionV1)
		require.NoError(t, err)
		require.Len(t, got, 1)
	})
}

func TestBlockWriter_Stats(t *testing.T) {
	singleInt64 := &Schema{Columns: []Column{
		{Name: "val", Type: ColumnTypeInt64, Encoding: EncodingRawFixedWidth},
	}}

	collect := func(schema *Schema, startOffset uint64) (*BlockWriter, *[]DirectoryEntry, *[]int) {
		entries := &[]DirectoryEntry{}
		blockSizes := &[]int{}
		bw := NewBlockWriter(schema, startOffset, func(block []byte, e DirectoryEntry) error {
			*entries = append(*entries, e)
			*blockSizes = append(*blockSizes, len(block))
			return nil
		})
		return bw, entries, blockSizes
	}

	row := func(vals ...uint64) []byte {
		buf := make([]byte, 8*len(vals))
		for i, v := range vals {
			binary.LittleEndian.PutUint64(buf[i*8:], v)
		}
		return buf
	}

	t.Run("all-tombstone partial block has LiveCount 0 and zero stats", func(t *testing.T) {
		bw, entries, blockSizes := collect(singleInt64, 1000)

		for i := uint64(0); i < 5; i++ {
			require.NoError(t, bw.Append(10+i, false, row(i64bits(-100))))
		}
		require.NoError(t, bw.Flush())

		require.Len(t, *entries, 1)
		e := (*entries)[0]
		assert.Equal(t, uint64(10), e.StartDocID)
		assert.Equal(t, uint64(14), e.EndDocID)
		assert.Equal(t, uint64(1000), e.Offset)
		assert.Equal(t, uint32(5), e.RowCount)
		assert.Equal(t, uint32(0), e.LiveCount, "all rows are tombstones")
		require.Len(t, e.Stats, 1)
		assert.Equal(t, ColStats{Min: 0, Max: 0}, e.Stats[0],
			"stats must remain zero when no live row exists (\"stats are meaningless\")")

		// 5 rows × (8B docID + 1B live + 8B value); scalar layout has no padding
		assert.Equal(t, []int{5 * 17}, *blockSizes)
		assert.Equal(t, uint64(1000+5*17), bw.Offset())

		// v2 entries carry the page table: single scalar page follows
		// docIDs+live directly
		require.Len(t, e.Pages, 1)
		assert.Equal(t, ColPage{Offset: 5*8 + 5, Size: 5 * 8}, e.Pages[0])
	})

	t.Run("all-tombstone full block auto-emits with LiveCount 0", func(t *testing.T) {
		bw, entries, _ := collect(singleInt64, 0)

		for i := 0; i < BlockSize; i++ {
			require.NoError(t, bw.Append(uint64(i), false, row(42)))
		}
		// block emitted on reaching BlockSize, no Flush needed
		require.Len(t, *entries, 1)
		e := (*entries)[0]
		assert.Equal(t, uint32(BlockSize), e.RowCount)
		assert.Equal(t, uint32(0), e.LiveCount)
		assert.Equal(t, ColStats{Min: 0, Max: 0}, e.Stats[0])

		require.NoError(t, bw.Flush()) // nothing buffered, no extra block
		assert.Len(t, *entries, 1)
	})

	t.Run("tombstone values are excluded from min and max", func(t *testing.T) {
		bw, entries, _ := collect(singleInt64, 0)

		// dead rows carry the most extreme values; they must not leak into stats
		require.NoError(t, bw.Append(1, false, row(i64bits(math.MinInt64))))
		require.NoError(t, bw.Append(2, true, row(i64bits(-3))))
		require.NoError(t, bw.Append(3, true, row(i64bits(7))))
		require.NoError(t, bw.Append(4, false, row(i64bits(math.MaxInt64))))
		require.NoError(t, bw.Flush())

		require.Len(t, *entries, 1)
		e := (*entries)[0]
		assert.Equal(t, uint32(4), e.RowCount)
		assert.Equal(t, uint32(2), e.LiveCount)
		assert.Equal(t, int64(-3), int64(e.Stats[0].Min))
		assert.Equal(t, int64(7), int64(e.Stats[0].Max))
	})

	t.Run("single live row pins both min and max", func(t *testing.T) {
		bw, entries, _ := collect(singleInt64, 0)
		require.NoError(t, bw.Append(1, false, row(i64bits(999))))
		require.NoError(t, bw.Append(2, true, row(i64bits(-1))))
		require.NoError(t, bw.Flush())

		e := (*entries)[0]
		assert.Equal(t, uint32(1), e.LiveCount)
		assert.Equal(t, int64(-1), int64(e.Stats[0].Min))
		assert.Equal(t, int64(-1), int64(e.Stats[0].Max))
	})

	// Regression guard: the v1 flushBlock incremented LiveCount once per live
	// row PER COLUMN (the counting lived inside the per-column stats loop),
	// so multi-column schemas reported liveRows × numCols. The v2 rewrite
	// counts live rows in a dedicated loop; this test pins the documented
	// "rows with live=1" contract so the bug cannot return.
	t.Run("multi-column live count counts rows not cells", func(t *testing.T) {
		twoCols := &Schema{Columns: []Column{
			{Name: "a", Type: ColumnTypeInt64, Encoding: EncodingRawFixedWidth},
			{Name: "b", Type: ColumnTypeFloat64, Encoding: EncodingRawFixedWidth},
		}}
		bw, entries, _ := collect(twoCols, 0)

		require.NoError(t, bw.Append(1, true, row(1, math.Float64bits(1.5))))
		require.NoError(t, bw.Append(2, false, row(2, math.Float64bits(2.5))))
		require.NoError(t, bw.Append(3, true, row(3, math.Float64bits(3.5))))
		require.NoError(t, bw.Flush())

		require.Len(t, *entries, 1)
		e := (*entries)[0]
		assert.Equal(t, uint32(3), e.RowCount)
		assert.Equal(t, uint32(2), e.LiveCount,
			"LiveCount must count live ROWS (doc contract), not live row×column cells")
		// per-column stats themselves are correct
		assert.Equal(t, int64(1), int64(e.Stats[0].Min))
		assert.Equal(t, int64(3), int64(e.Stats[0].Max))
		assert.Equal(t, 1.5, math.Float64frombits(e.Stats[1].Min))
		assert.Equal(t, 3.5, math.Float64frombits(e.Stats[1].Max))
	})

	t.Run("emit error is propagated", func(t *testing.T) {
		bw := NewBlockWriter(singleInt64, 0, func([]byte, DirectoryEntry) error {
			return fmt.Errorf("disk full")
		})
		require.NoError(t, bw.Append(1, true, row(1)))
		err := bw.Flush()
		require.Error(t, err)
		assert.ErrorContains(t, err, "disk full")
	})

	t.Run("flush without rows is a no-op", func(t *testing.T) {
		bw, entries, _ := collect(singleInt64, 77)
		require.NoError(t, bw.Flush())
		assert.Empty(t, *entries)
		assert.Equal(t, uint64(77), bw.Offset())
	})

	t.Run("strictly decreasing docID is rejected", func(t *testing.T) {
		bw, _, _ := collect(singleInt64, 0)
		require.NoError(t, bw.Append(10, true, row(1)))
		err := bw.Append(9, true, row(1))
		require.Error(t, err)
		assert.ErrorContains(t, err, "out of order")
	})
}

func TestBlockWriterReader_VectorRoundtrip(t *testing.T) {
	const dims = 8
	schema := &Schema{Columns: []Column{
		{Name: "vec", Type: ColumnTypeVector, Encoding: EncodingRawFixedWidth, Dims: dims},
	}}
	require.NoError(t, schema.validate())
	require.Equal(t, VectorBlockSize, BlockSizeFor(schema))

	vecBytes := func(i int) []byte {
		buf := make([]byte, dims*4)
		for d := 0; d < dims; d++ {
			binary.LittleEndian.PutUint32(buf[d*4:],
				math.Float32bits(float32(i)+float32(d)*0.25))
		}
		return buf
	}

	const rows = 600 // 3 blocks of ≤256 rows
	var blocks [][]byte
	var entries []DirectoryEntry
	bw := NewBlockWriter(schema, 0, func(block []byte, e DirectoryEntry) error {
		blocks = append(blocks, append([]byte(nil), block...))
		entries = append(entries, e)
		return nil
	})

	for i := 0; i < rows; i++ {
		require.NoError(t, bw.Append(uint64(i*3), i%5 != 0, vecBytes(i)))
	}
	require.NoError(t, bw.Flush())

	require.Len(t, entries, 3)
	assert.Equal(t, uint32(VectorBlockSize), entries[0].RowCount)
	assert.Equal(t, uint32(VectorBlockSize), entries[1].RowCount)
	assert.Equal(t, uint32(rows-2*VectorBlockSize), entries[2].RowCount)

	t.Run("page layout and alignment", func(t *testing.T) {
		// flushBlock pads block starts to payloadAlign for every vector
		// schema, so the 64B-aligned in-block page offset
		// (alignUp(rows*9, 64)) yields absolutely aligned payloads too.
		for i := range entries {
			e := &entries[i]
			require.Len(t, e.Pages, 1)
			assert.Zero(t, int(e.Pages[0].Offset)%payloadAlign,
				"block %d: vector page offset must be 64B-aligned within the block", i)
			assert.Equal(t, alignUp(int(e.RowCount)*9, payloadAlign), int(e.Pages[0].Offset),
				"block %d: page follows docIDs+live, padded to alignment", i)
			assert.Equal(t, int(e.RowCount)*dims*4, int(e.Pages[0].Size), "block %d", i)
		}

		// directory offsets are consistent with the emitted block sizes
		wantOffset := uint64(0)
		for i := range entries {
			assert.Equal(t, wantOffset, entries[i].Offset, "block %d", i)
			wantOffset += uint64(len(blocks[i]))
		}
		assert.Equal(t, wantOffset, bw.Offset())
	})

	t.Run("stats are zeroed for vector columns", func(t *testing.T) {
		for i := range entries {
			assert.Equal(t, ColStats{}, entries[i].Stats[0], "block %d", i)
		}
	})

	t.Run("read back every row", func(t *testing.T) {
		var contents []byte
		for _, blk := range blocks {
			contents = append(contents, blk...)
		}

		globalRow := 0
		for i := range entries {
			br, err := NewBlockReader(schema, &entries[i], contents)
			require.NoError(t, err)
			for r := 0; r < br.Rows(); r++ {
				assert.Equal(t, uint64(globalRow*3), br.DocIDAt(r))
				assert.Equal(t, globalRow%5 != 0, br.IsLive(r))
				assert.Equal(t, vecBytes(globalRow), br.FixedValueAt(0, r),
					"row %d", globalRow)
				assert.Equal(t, vecBytes(globalRow), br.RowValues(nil, r),
					"row %d: RowValues must re-emit the original payload", globalRow)
				globalRow++
			}
		}
		assert.Equal(t, rows, globalRow)
	})

	t.Run("out of order append still errors", func(t *testing.T) {
		bw2 := NewBlockWriter(schema, 0, func([]byte, DirectoryEntry) error { return nil })
		require.NoError(t, bw2.Append(10, true, vecBytes(0)))
		err := bw2.Append(10, true, vecBytes(1))
		require.Error(t, err)
		assert.ErrorContains(t, err, "out of order")
	})
}

func TestBlockWriterReader_MultiVectorRoundtrip(t *testing.T) {
	const dims = 4
	schema := &Schema{Columns: []Column{
		{Name: "mv", Type: ColumnTypeMultiVector, Encoding: EncodingOffsetValues, Dims: dims},
	}}
	require.NoError(t, schema.validate())
	require.Equal(t, VectorBlockSize, BlockSizeFor(schema))
	require.Equal(t, -1, schema.RowWidth())

	// variable token counts, including zero-token rows
	tokenCount := func(i int) int { return i % 5 }
	payloadFor := func(i int) []byte {
		n := tokenCount(i)
		buf := make([]byte, n*dims*4)
		for tok := 0; tok < n; tok++ {
			for d := 0; d < dims; d++ {
				binary.LittleEndian.PutUint32(buf[(tok*dims+d)*4:],
					math.Float32bits(float32(i)+float32(tok)+float32(d)*0.5))
			}
		}
		return buf
	}

	const rows = 300 // 2 blocks: 256 + 44
	var blocks [][]byte
	var entries []DirectoryEntry
	bw := NewBlockWriter(schema, 0, func(block []byte, e DirectoryEntry) error {
		blocks = append(blocks, append([]byte(nil), block...))
		entries = append(entries, e)
		return nil
	})

	for i := 0; i < rows; i++ {
		require.NoError(t, bw.Append(uint64(i), i%7 != 0, payloadFor(i)))
	}
	require.NoError(t, bw.Flush())

	require.Len(t, entries, 2)
	assert.Equal(t, uint32(VectorBlockSize), entries[0].RowCount)
	assert.Equal(t, uint32(rows-VectorBlockSize), entries[1].RowCount)

	t.Run("page layout", func(t *testing.T) {
		// Multi-vector page layout per flushBlock:
		// [offsets (rows+1)×u32][pad to 64B][payload], offset table relative
		// to the payload start. The page start is aligned to payloadAlign
		// (relative to the block start) and the offsets header is a
		// payloadAlign multiple, so the payload itself is aligned whenever
		// the block start is — which the writer guarantees absolutely for
		// vector schemas by padding block starts (entry.Offset%64==0).
		globalRow := 0
		for i := range entries {
			e := &entries[i]
			blockRows := int(e.RowCount)
			require.Len(t, e.Pages, 1)
			assert.Equal(t, alignUp(blockRows*9, payloadAlign), int(e.Pages[0].Offset),
				"block %d: variable page start must be aligned", i)
			assert.Zero(t, int(e.Offset)%payloadAlign,
				"block %d: block start must be absolutely aligned", i)

			payloadBytes := 0
			for r := 0; r < blockRows; r++ {
				payloadBytes += len(payloadFor(globalRow + r))
			}
			wantSize := alignUp(4*(blockRows+1), payloadAlign) + payloadBytes
			assert.Equal(t, wantSize, int(e.Pages[0].Size), "block %d", i)
			globalRow += blockRows
		}
	})

	t.Run("stats are zeroed", func(t *testing.T) {
		for i := range entries {
			assert.Equal(t, ColStats{}, entries[i].Stats[0], "block %d", i)
		}
	})

	t.Run("read back every row", func(t *testing.T) {
		var contents []byte
		for _, blk := range blocks {
			contents = append(contents, blk...)
		}

		globalRow := 0
		zeroTokenRows := 0
		for i := range entries {
			br, err := NewBlockReader(schema, &entries[i], contents)
			require.NoError(t, err)
			for r := 0; r < br.Rows(); r++ {
				want := payloadFor(globalRow)
				assert.Equal(t, uint64(globalRow), br.DocIDAt(r))
				assert.Equal(t, globalRow%7 != 0, br.IsLive(r))
				got := br.VariableValueAt(r)
				assert.Equal(t, tokenCount(globalRow)*dims*4, len(got),
					"row %d: VariableValueAt boundaries", globalRow)
				reEmitted := br.RowValues(nil, r)
				if len(want) == 0 {
					zeroTokenRows++
					assert.Empty(t, got, "row %d", globalRow)
					assert.Empty(t, reEmitted, "row %d", globalRow)
				} else {
					assert.Equal(t, want, got, "row %d", globalRow)
					assert.Equal(t, want, reEmitted,
						"row %d: RowValues must re-emit the original payload", globalRow)
				}
				globalRow++
			}
		}
		assert.Equal(t, rows, globalRow)
		assert.Equal(t, rows/5, zeroTokenRows, "zero-token rows must roundtrip")
	})

	t.Run("out of order append still errors", func(t *testing.T) {
		bw2 := NewBlockWriter(schema, 0, func([]byte, DirectoryEntry) error { return nil })
		require.NoError(t, bw2.Append(5, true, payloadFor(1)))
		err := bw2.Append(4, true, payloadFor(2))
		require.Error(t, err)
		assert.ErrorContains(t, err, "out of order")
	})
}

// TestNewBlockReader_CorruptOffsetTable verifies that a corrupt variable-
// payload offset table (which a segment-level checksum mismatch may not
// catch) surfaces as an error from NewBlockReader instead of a panic on a
// later VariableValueAt call (e.g. in the compaction goroutine).
func TestNewBlockReader_CorruptOffsetTable(t *testing.T) {
	const dims = 4
	const tokens = 2
	const rows = 3
	schema := &Schema{Columns: []Column{
		{Name: "mv", Type: ColumnTypeMultiVector, Encoding: EncodingOffsetValues, Dims: dims},
	}}
	require.NoError(t, schema.validate())

	// one block, 3 rows × 2 tokens × 4 dims → 96B payload slab
	buildBlock := func(t *testing.T) ([]byte, DirectoryEntry) {
		var blocks [][]byte
		var entries []DirectoryEntry
		bw := NewBlockWriter(schema, 0, func(block []byte, e DirectoryEntry) error {
			blocks = append(blocks, append([]byte(nil), block...))
			entries = append(entries, e)
			return nil
		})
		payload := make([]byte, tokens*dims*4)
		for i := range payload {
			payload[i] = byte(i)
		}
		for i := 0; i < rows; i++ {
			require.NoError(t, bw.Append(uint64(i), true, payload))
		}
		require.NoError(t, bw.Flush())
		require.Len(t, entries, 1)
		return blocks[0], entries[0]
	}

	tests := []struct {
		name    string
		corrupt func(offsets []byte, payloadLen int)
		wantErr string
	}{
		{
			name:    "intact control",
			corrupt: func([]byte, int) {},
		},
		{
			name: "decreasing offsets",
			corrupt: func(offsets []byte, _ int) {
				// rows span [0:32][32:64][64:96]; make row 1 end before it starts
				binary.LittleEndian.PutUint32(offsets[4:], 30)
				binary.LittleEndian.PutUint32(offsets[8:], 10)
			},
			wantErr: "not monotonic",
		},
		{
			name: "final offset beyond payload slab",
			corrupt: func(offsets []byte, payloadLen int) {
				binary.LittleEndian.PutUint32(offsets[4*rows:], uint32(payloadLen+64))
			},
			wantErr: "exceeds payload size",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			contents, entry := buildBlock(t)
			pageStart := int(entry.Offset) + int(entry.Pages[0].Offset)
			payloadLen := int(entry.Pages[0].Size) - VariablePageHeaderSize(rows)
			tc.corrupt(contents[pageStart:pageStart+4*(rows+1)], payloadLen)

			br, err := NewBlockReader(schema, &entry, contents)
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.NotNil(t, br)
				return
			}
			require.Error(t, err)
			assert.ErrorContains(t, err, tc.wantErr)
			assert.Nil(t, br)
		})
	}
}

func TestEncodingRegistry(t *testing.T) {
	t.Run("raw fixed width is registered", func(t *testing.T) {
		enc, err := EncodingByID(EncodingRawFixedWidth)
		require.NoError(t, err)
		assert.Equal(t, EncodingRawFixedWidth, enc.ID())
	})

	t.Run("offset+values has no fixed-width codec", func(t *testing.T) {
		// EncodingOffsetValues is structural (handled by the block
		// writer/reader) — it is a valid schema encoding but resolves to no
		// codec
		_, err := EncodingByID(EncodingOffsetValues)
		require.Error(t, err)
		assert.ErrorContains(t, err, "variable-width")
	})

	t.Run("unknown ids error", func(t *testing.T) {
		for _, id := range []EncodingID{2, 100, 255} {
			_, err := EncodingByID(id)
			require.Error(t, err, "id %d", id)
			assert.ErrorContains(t, err, "unknown column encoding")
		}
	})

	t.Run("ValidEncoding covers fixed and variable", func(t *testing.T) {
		assert.True(t, ValidEncoding(EncodingRawFixedWidth))
		assert.True(t, ValidEncoding(EncodingOffsetValues))
		assert.False(t, ValidEncoding(EncodingID(2)))
		assert.False(t, ValidEncoding(EncodingID(255)))
	})

	t.Run("raw fixed width roundtrip", func(t *testing.T) {
		enc, err := EncodingByID(EncodingRawFixedWidth)
		require.NoError(t, err)

		page := make([]byte, 3*8)
		for i := range page {
			page[i] = byte(i)
		}
		assert.Equal(t, len(page), enc.EncodedSize(8, 3))

		encoded := enc.Encode(nil, page, 8, 3)
		assert.Equal(t, page, encoded)

		decoded, err := enc.Decode(encoded, 8, 3)
		require.NoError(t, err)
		assert.Equal(t, page, decoded)
	})

	t.Run("raw fixed width decode rejects short pages", func(t *testing.T) {
		enc, err := EncodingByID(EncodingRawFixedWidth)
		require.NoError(t, err)
		_, err = enc.Decode(make([]byte, 23), 8, 3)
		require.Error(t, err)
		assert.ErrorContains(t, err, "too short")
	})
}

// TestBlockWriter_Stats_NaN pins the NaN handling in the per-block min/max
// accumulation: NaN float64 values are stored as row data but excluded from
// the stats. Without the exclusion, a NaN that seeds Min/Max freezes them —
// less(v, NaN) and less(NaN, v) are both always false — poisoning the block
// stats for value-range pruning.
func TestBlockWriter_Stats_NaN(t *testing.T) {
	singleFloat64 := &Schema{Columns: []Column{
		{Name: "val", Type: ColumnTypeFloat64, Encoding: EncodingRawFixedWidth},
	}}

	f64 := func(v float64) []byte {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
		return buf
	}
	nan := math.NaN()

	tests := []struct {
		name string
		rows []struct {
			docID uint64
			live  bool
			val   float64
		}
		wantMin float64
		wantMax float64
		// allNaNOrEmpty: stats must remain zeroed
		allNaNOrEmpty bool
	}{
		{
			name: "NaN-first live value does not freeze stats",
			rows: []struct {
				docID uint64
				live  bool
				val   float64
			}{
				{1, true, nan},
				{2, true, 2.5},
				{3, true, -1.5},
			},
			wantMin: -1.5,
			wantMax: 2.5,
		},
		{
			name: "NaN in the middle is skipped",
			rows: []struct {
				docID uint64
				live  bool
				val   float64
			}{
				{1, true, 3.5},
				{2, true, nan},
				{3, true, 7.25},
			},
			wantMin: 3.5,
			wantMax: 7.25,
		},
		{
			name: "all-NaN live rows keep zeroed stats",
			rows: []struct {
				docID uint64
				live  bool
				val   float64
			}{
				{1, true, nan},
				{2, true, nan},
			},
			allNaNOrEmpty: true,
		},
		{
			name: "tombstoned non-NaN among live NaN keeps zeroed stats",
			rows: []struct {
				docID uint64
				live  bool
				val   float64
			}{
				{1, false, 99.0}, // dead, excluded
				{2, true, nan},   // live but NaN, excluded
			},
			allNaNOrEmpty: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var entries []DirectoryEntry
			bw := NewBlockWriter(singleFloat64, 0, func(_ []byte, e DirectoryEntry) error {
				entries = append(entries, e)
				return nil
			})
			for _, r := range tc.rows {
				require.NoError(t, bw.Append(r.docID, r.live, f64(r.val)))
			}
			require.NoError(t, bw.Flush())
			require.Len(t, entries, 1)
			stats := entries[0].Stats[0]

			if tc.allNaNOrEmpty {
				assert.Equal(t, ColStats{Min: 0, Max: 0}, stats,
					"stats must remain zeroed when no usable live value exists")
				return
			}
			assert.Equal(t, tc.wantMin, math.Float64frombits(stats.Min), "min")
			assert.Equal(t, tc.wantMax, math.Float64frombits(stats.Max), "max")
			assert.False(t, math.IsNaN(math.Float64frombits(stats.Min)), "min must not be NaN")
			assert.False(t, math.IsNaN(math.Float64frombits(stats.Max)), "max must not be NaN")
		})
	}

	t.Run("NaN rows are still stored as data", func(t *testing.T) {
		var entries []DirectoryEntry
		var blocks [][]byte
		bw := NewBlockWriter(singleFloat64, 0, func(block []byte, e DirectoryEntry) error {
			cp := make([]byte, len(block))
			copy(cp, block)
			blocks = append(blocks, cp)
			entries = append(entries, e)
			return nil
		})
		require.NoError(t, bw.Append(1, true, f64(nan)))
		require.NoError(t, bw.Append(2, true, f64(1.0)))
		require.NoError(t, bw.Flush())
		require.Len(t, entries, 1)
		assert.Equal(t, uint32(2), entries[0].LiveCount,
			"NaN rows count as live rows — only the stats exclude them")

		br, err := NewBlockReader(singleFloat64, &entries[0], blocks[0])
		require.NoError(t, err)
		require.Equal(t, 2, br.Rows())
		assert.True(t, math.IsNaN(math.Float64frombits(br.ValueBitsAt(0, 0))),
			"the NaN payload itself must survive the round trip")
		assert.Equal(t, 1.0, math.Float64frombits(br.ValueBitsAt(0, 1)))
	})
}

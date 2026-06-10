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

func TestColumnType_Width(t *testing.T) {
	tests := []struct {
		name  string
		ct    ColumnType
		width int
	}{
		{name: "int64", ct: ColumnTypeInt64, width: 8},
		{name: "float64", ct: ColumnTypeFloat64, width: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.width, tt.ct.Width())
		})
	}

	t.Run("unknown type panics", func(t *testing.T) {
		assert.Panics(t, func() { ColumnType(0).Width() })
		assert.Panics(t, func() { ColumnType(3).Width() })
		assert.Panics(t, func() { ColumnType(255).Width() })
	})
}

func TestColumnType_String(t *testing.T) {
	tests := []struct {
		ct   ColumnType
		want string
	}{
		{ct: ColumnTypeInt64, want: "int64"},
		{ct: ColumnTypeFloat64, want: "float64"},
		{ct: ColumnType(0), want: "unknown"},
		{ct: ColumnType(3), want: "unknown"},
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

	t.Run("unknown type panics", func(t *testing.T) {
		assert.Panics(t, func() { ColumnType(0).less(1, 2) })
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.header.MarshalBinary()

			got, n, err := UnmarshalHeader(buf)
			require.NoError(t, err)
			assert.Equal(t, len(buf), n, "must consume the entire marshaled header")
			assert.Equal(t, tt.header.Schema, got.Schema)

			t.Run("trailing data is not consumed", func(t *testing.T) {
				withTrailer := append(append([]byte{}, buf...), 0xde, 0xad, 0xbe, 0xef)
				got2, n2, err := UnmarshalHeader(withTrailer)
				require.NoError(t, err)
				assert.Equal(t, len(buf), n2)
				assert.Equal(t, tt.header.Schema, got2.Schema)
			})

			t.Run("every truncation errors", func(t *testing.T) {
				for cut := 0; cut < len(buf); cut++ {
					_, _, err := UnmarshalHeader(buf[:cut])
					assert.Error(t, err, "truncated to %d of %d bytes must error", cut, len(buf))
				}
			})
		})
	}
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
			errLike: "unknown column encoding",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, n, err := UnmarshalHeader(tt.buf())
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.errLike)
			assert.Nil(t, h)
			assert.Equal(t, 0, n)
		})
	}
}

func TestDirectory_MarshalUnmarshal(t *testing.T) {
	t.Run("entry size accounts for per-column stats", func(t *testing.T) {
		// 8 (start) + 8 (end) + 8 (offset) + 4 (rows) + 4 (live) = 32 fixed
		assert.Equal(t, 32, directoryEntrySize(0))
		assert.Equal(t, 48, directoryEntrySize(1))
		assert.Equal(t, 80, directoryEntrySize(3))
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
			},
			{
				StartDocID: 1, EndDocID: 2, Offset: 3, RowCount: 4, LiveCount: 4,
				Stats: []ColStats{{Min: 5, Max: 6}, {Min: 7, Max: 8}, {Min: 9, Max: 10}},
			},
		}
		buf := MarshalDirectory(entries, 3)
		require.Len(t, buf, 2*directoryEntrySize(3))

		got, err := UnmarshalDirectory(buf, 3)
		require.NoError(t, err)
		assert.Equal(t, entries, got)
	})

	t.Run("empty directory is valid", func(t *testing.T) {
		buf := MarshalDirectory(nil, 2)
		assert.Empty(t, buf)
		got, err := UnmarshalDirectory(nil, 2)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("every non-multiple length errors", func(t *testing.T) {
		entries := []DirectoryEntry{{
			StartDocID: 1, EndDocID: 2, Offset: 3, RowCount: 4, LiveCount: 4,
			Stats: []ColStats{{Min: 5, Max: 6}},
		}}
		buf := MarshalDirectory(entries, 1)
		entrySize := directoryEntrySize(1)
		require.Len(t, buf, entrySize)

		for cut := 1; cut < entrySize; cut++ {
			_, err := UnmarshalDirectory(buf[:cut], 1)
			assert.Error(t, err, "length %d must not be a valid directory", cut)
			assert.ErrorContains(t, err, "not a multiple")
		}

		// one byte beyond a full entry is also invalid
		_, err := UnmarshalDirectory(append(append([]byte{}, buf...), 0), 1)
		require.Error(t, err)
	})

	t.Run("numCols mismatch that changes entry size errors", func(t *testing.T) {
		buf := MarshalDirectory([]DirectoryEntry{{
			Stats: []ColStats{{}, {}},
		}}, 2) // 64 bytes
		_, err := UnmarshalDirectory(buf, 3) // entry size 80
		require.Error(t, err)
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

		// 5 rows × (8B docID + 1B live + 8B value)
		assert.Equal(t, []int{5 * 17}, *blockSizes)
		assert.Equal(t, uint64(1000+5*17), bw.Offset())
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

	// KNOWN BUG (do not delete this test): flushBlock in format.go
	// increments entry.LiveCount once per live row PER COLUMN (the
	// LiveCount++ statements live inside the per-column stats loop,
	// format.go lines ~315-337). For any schema with more than one column
	// LiveCount = liveRows × numCols, violating the documented contract
	// "LiveCount uint32 // rows with live=1" (format.go line 148). All
	// schemas produced today are single-column (columnarSchemaForProp), so
	// the bug is latent, but the wire format and BlockWriter explicitly
	// support multi-column schemas. This test asserts the documented
	// semantics and currently FAILS.
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

func TestEncodingRegistry(t *testing.T) {
	t.Run("raw fixed width is registered", func(t *testing.T) {
		enc, err := EncodingByID(EncodingRawFixedWidth)
		require.NoError(t, err)
		assert.Equal(t, EncodingRawFixedWidth, enc.ID())
	})

	t.Run("unknown ids error", func(t *testing.T) {
		for _, id := range []EncodingID{1, 2, 100, 255} {
			_, err := EncodingByID(id)
			require.Error(t, err, "id %d", id)
			assert.ErrorContains(t, err, "unknown column encoding")
		}
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

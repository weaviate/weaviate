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

package storobj

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weaviate/weaviate/entities/models"
)

func marshalledTestObject(t *testing.T, className string, legacyVec []float32,
	named map[string][]float32, payloadBytes int,
) []byte {
	t.Helper()
	obj := New(42)
	obj.Object = models.Object{
		ID:         strfmt.UUID("00000000-0000-4000-8000-00000000002a"),
		Class:      className,
		Properties: map[string]interface{}{"filler": strings.Repeat("x", payloadBytes)},
	}
	obj.Vector = legacyVec
	obj.Vectors = named
	data, err := obj.MarshalBinary()
	require.NoError(t, err)
	return data
}

// TestVectorTailOffsetAgainstFullDecode: for every layout variant, the tail computed
// from a bounded prefix plus VectorFromTail must yield exactly what the full-value
// VectorFromBinary yields.
func TestVectorTailOffsetAgainstFullDecode(t *testing.T) {
	named := map[string][]float32{
		"custom":  {1.5, -2.5, 3.25},
		"sibling": {9, 8, 7, 6},
	}

	cases := []struct {
		name      string
		className string
		legacyVec []float32
		payload   int
		peekSize  int
		wantOK    bool
	}{
		{"named only, short class, 512B peek", "Test", nil, 10_000, 512, true},
		{"long class name still within peek", strings.Repeat("C", 100), nil, 10_000, 512, true},
		{"legacy vector pushes schemaLen past small peek", "Test", make([]float32, 200), 10_000, 512, false},
		{"legacy vector within large peek", "Test", make([]float32, 100), 10_000, 512, true},
		{"tiny peek cannot reach schemaLen", "Test", nil, 10_000, 44, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := marshalledTestObject(t, tc.className, tc.legacyVec, named, tc.payload)
			peek := data[:min(tc.peekSize, len(data))]

			tailStart, schemaLen, ok, err := VectorTailOffsetFromPeek(peek)
			require.NoError(t, err)
			require.Equal(t, tc.wantOK, ok)
			if !ok {
				return
			}
			require.Greater(t, int(schemaLen), tc.payload, "schema JSON includes the payload")
			require.Less(t, tailStart, uint64(len(data)))

			for name, want := range named {
				got, err := VectorFromTail(data[tailStart:], name)
				require.NoError(t, err)
				require.Equal(t, want, got)

				full, err := VectorFromBinary(data, nil, name)
				require.NoError(t, err)
				require.Equal(t, full, got)
			}

			_, err = VectorFromTail(data[tailStart:], "does-not-exist")
			var notFound ErrTargetVectorNotFound
			require.ErrorAs(t, err, &notFound)
		})
	}
}

func TestVectorTailOffsetFromPeekErrors(t *testing.T) {
	_, _, _, err := VectorTailOffsetFromPeek(nil)
	assert.Error(t, err)

	_, _, _, err = VectorTailOffsetFromPeek([]byte{9, 0, 0})
	assert.Error(t, err, "unsupported version must error")

	// version byte alone is valid input but cannot resolve the offset
	_, _, ok, err := VectorTailOffsetFromPeek([]byte{1})
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestVectorFromTailErrors(t *testing.T) {
	_, err := VectorFromTail([]byte{1, 2, 3}, "custom")
	assert.Error(t, err, "truncated tail must error, not panic")

	_, err = VectorFromTail(nil, "")
	assert.Error(t, err, "legacy target is not served from the tail")
}

// TestVectorFromTailLegacyOnlyObject: objects written before target vectors existed
// have no trailing sections; the tail decode must mirror VectorFromBinary and report
// no vector rather than fail.
func TestVectorFromTailPreTargetVectorObject(t *testing.T) {
	data := marshalledTestObject(t, "Test", []float32{1, 2}, nil, 100)
	tailStart, _, ok, err := VectorTailOffsetFromPeek(data[:min(512, len(data))])
	require.NoError(t, err)
	require.True(t, ok)

	got, err := VectorFromTail(data[tailStart:], "custom")
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestVectorFromTailCorruptSections: a mislocated or truncated tail must fail as an
// error, never panic in the shared decoder.
func TestVectorFromTailCorruptSections(t *testing.T) {
	data := marshalledTestObject(t, "Test", nil, map[string][]float32{"custom": {1, 2}}, 200)
	tailStart, _, ok, err := VectorTailOffsetFromPeek(data[:min(512, len(data))])
	require.NoError(t, err)
	require.True(t, ok)
	tail := data[tailStart:]

	for cut := 1; cut < len(tail); cut++ {
		_, err := VectorFromTail(tail[:cut], "custom")
		_ = err // any outcome but a panic is acceptable for a truncated tail
	}
	// garbage bytes where the section lengths should be
	_, err = VectorFromTail([]byte{9, 9, 9, 9, 9}, "custom")
	require.Error(t, err)
}

// TestVectorFromTailOffsetOutOfBounds: the offsets map is on-disk data, so a
// well-formed tail can still carry an offset pointing past it. The bound check
// must reject it instead of letting the shared decoder slice out of range.
func TestVectorFromTailOffsetOutOfBounds(t *testing.T) {
	segment := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	for _, tc := range []struct {
		name   string
		offset uint32
	}{
		{"offset past the tail", 4096},
		{"vector length past the tail", uint32(len(segment)) - 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			offsets, err := msgpack.Marshal(map[string]uint32{"custom": tc.offset})
			require.NoError(t, err)

			var tail []byte
			appendU32 := func(v uint32) {
				tail = binary.LittleEndian.AppendUint32(tail, v)
			}
			appendU32(0) // meta section
			appendU32(0) // vectorWeights section
			appendU32(uint32(len(offsets)))
			tail = append(tail, offsets...)
			appendU32(uint32(len(segment)))
			tail = append(tail, segment...)

			_, err = VectorFromTail(tail, "custom")
			require.Error(t, err)
		})
	}
}

// TestVectorDecodersOnTruncatedValues sweeps every prefix of a real object through the
// vector decoders. Each walks the value's sections by reading their lengths out of the
// value itself, so a truncated row must produce an error rather than a panic: these
// serve the HNSW cache-miss path and the prefill scan, both of which read untrusted
// rows, and the prefill runs synchronously inside shard startup where a panic takes
// the process down.
//
// The backing array is longer than the value and filled with a sentinel, mirroring the
// mmapped segment a real truncated row is a subslice of: an unbounded read finds
// plausible lengths there and decodes the neighbouring row instead of failing, so a
// prefix that returns no error must still not return a vector built from those bytes.
func TestVectorDecodersOnTruncatedValues(t *testing.T) {
	named := map[string][]float32{"custom": {1, 2, 3}}
	multi := map[string][][]float32{"multi": {{1, 2}, {3, 4}}}

	cases := []struct {
		name   string
		full   []byte
		decode func(value []byte) (any, error)
		want   any
	}{
		{
			name: "named target vector",
			full: marshalledTestObject(t, "Test", nil, named, 4096),
			decode: func(v []byte) (any, error) {
				out, err := VectorFromBinary(v, nil, "custom")
				if out == nil {
					return nil, err
				}
				return out, err
			},
			want: []float32{1, 2, 3},
		},
		{
			name: "legacy vector",
			full: marshalledTestObject(t, "Test", []float32{4, 5, 6}, nil, 4096),
			decode: func(v []byte) (any, error) {
				out, err := VectorFromBinary(v, nil, "")
				if out == nil {
					return nil, err
				}
				return out, err
			},
			want: []float32{4, 5, 6},
		},
		{
			name: "multi vector",
			full: marshalledMultiVectorTestObject(t, multi),
			decode: func(v []byte) (any, error) {
				out, err := MultiVectorFromBinary(v, "multi")
				if out == nil {
					return nil, err
				}
				return out, err
			},
			want: [][]float32{{1, 2}, {3, 4}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for keep := 1; keep < len(tc.full); keep++ {
				backing := make([]byte, len(tc.full)+1024)
				for i := range backing {
					backing[i] = 0xAB
				}
				copy(backing, tc.full[:keep])
				value := backing[:keep:keep]

				require.NotPanicsf(t, func() {
					got, err := tc.decode(value)
					if err == nil && got != nil {
						require.Equalf(t, tc.want, got,
							"a %d byte prefix decoded to a vector that is not the real one", keep)
					}
				}, "decoder panicked on a %d byte prefix", keep)
			}

			got, err := tc.decode(tc.full)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func marshalledMultiVectorTestObject(t *testing.T, multi map[string][][]float32) []byte {
	t.Helper()
	obj := New(42)
	obj.Object = models.Object{
		ID:         strfmt.UUID("00000000-0000-4000-8000-00000000002a"),
		Class:      "Test",
		Properties: map[string]interface{}{"filler": strings.Repeat("x", 4096)},
	}
	obj.MultiVectors = multi
	data, err := obj.MarshalBinary()
	require.NoError(t, err)
	return data
}

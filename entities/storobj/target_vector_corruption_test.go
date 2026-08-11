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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/weaviate/weaviate/usecases/byteops"
)

func encodeTargetVec(vals ...float32) []byte {
	b := make([]byte, 2+len(vals)*4)
	binary.LittleEndian.PutUint16(b, uint16(len(vals)))
	for i, v := range vals {
		binary.LittleEndian.PutUint32(b[2+i*4:], math.Float32bits(v))
	}
	return b
}

func encodeMultiVec(vecs ...[]float32) []byte {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(vecs)))
	for _, v := range vecs {
		b = append(b, encodeTargetVec(v...)...)
	}
	return b
}

// buildVectorSection serializes one vector wire section
// ([uint32 offsetsLen][msgpack offsets][uint32 segLen][segment][trailing]);
// segLen is explicit so tests can declare a length that disagrees with the
// actual segment bytes.
func buildVectorSection(t *testing.T, offsets map[string]uint32, segLen uint32, segment, trailing []byte) []byte {
	t.Helper()
	blob, err := msgpack.Marshal(offsets)
	require.NoError(t, err)
	buf := binary.LittleEndian.AppendUint32(nil, uint32(len(blob)))
	buf = append(buf, blob...)
	buf = binary.LittleEndian.AppendUint32(buf, segLen)
	buf = append(buf, segment...)
	buf = append(buf, trailing...)
	return buf
}

// TestUnmarshalTargetVectorsCorruptOffsets pins that a corrupt offsets map or
// segment length fails with an error on both target-vector parsers, instead of
// silently decoding bytes of the following section (or panicking past the
// buffer, as unmarshalTargetVectors previously did).
func TestUnmarshalTargetVectorsCorruptOffsets(t *testing.T) {
	vecA := encodeTargetVec(1, 2, 3) // 14 bytes at offset 0
	vecB := encodeTargetVec(4, 5)    // 10 bytes at offset 14
	validSegment := append(append([]byte{}, vecA...), vecB...)
	// stands in for the multi-vector section that follows on disk: well-formed
	// vector bytes that a corrupt offset would decode cleanly
	trailing := encodeTargetVec(9, 9, 9, 9)

	// vecLen declares 10 floats but the segment ends after the prefix
	truncatedSegment := append(append([]byte{}, vecA...), 0x0A, 0x00)

	cases := []struct {
		name    string
		offsets map[string]uint32
		segLen  uint32
		segment []byte
		lookup  string
		want    []float32
		errLike string
	}{
		{
			name:    "valid",
			offsets: map[string]uint32{"vecA": 0, "vecB": 14},
			segLen:  uint32(len(validSegment)),
			segment: validSegment,
			lookup:  "vecB",
			want:    []float32{4, 5},
		},
		{
			name:    "offset into next section",
			offsets: map[string]uint32{"vecA": 0, "evil": uint32(len(validSegment))},
			segLen:  uint32(len(validSegment)),
			segment: validSegment,
			lookup:  "evil",
			errLike: "out of segment bounds",
		},
		{
			name:    "vector length crosses segment end",
			offsets: map[string]uint32{"vecA": 0, "evil": 14},
			segLen:  uint32(len(truncatedSegment)),
			segment: truncatedSegment,
			lookup:  "evil",
			errLike: "exceeds segment",
		},
		{
			name:    "segment length exceeds buffer",
			offsets: map[string]uint32{"vecA": 0},
			segLen:  10_000,
			segment: validSegment,
			lookup:  "vecA",
			errLike: "exceeds buffer",
		},
		{
			name:    "offset beyond buffer",
			offsets: map[string]uint32{"evil": 60_000},
			segLen:  uint32(len(validSegment)),
			segment: validSegment,
			lookup:  "evil",
			errLike: "out of segment bounds",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := buildVectorSection(t, tc.offsets, tc.segLen, tc.segment, trailing)

			rw := byteops.NewReadWriter(buf)
			all, err := unmarshalTargetVectors(&rw)
			rw2 := byteops.NewReadWriter(buf)
			single, serr := unmarshalSingleTargetVector(&rw2, tc.lookup, nil)

			if tc.errLike != "" {
				require.ErrorContains(t, err, tc.errLike)
				require.ErrorContains(t, serr, tc.errLike)
				return
			}
			require.NoError(t, err)
			require.NoError(t, serr)
			require.Equal(t, tc.want, single)
			require.Equal(t, tc.want, all[tc.lookup])
		})
	}

	t.Run("missing name still reports not-found", func(t *testing.T) {
		buf := buildVectorSection(t, map[string]uint32{"vecA": 0, "vecB": 14},
			uint32(len(validSegment)), validSegment, trailing)
		rw := byteops.NewReadWriter(buf)
		_, err := unmarshalSingleTargetVector(&rw, "missing", nil)
		require.ErrorAs(t, err, &ErrTargetVectorNotFound{})
	})
}

// TestVectorSectionFramingCorrupt pins the two fields that frame a section — the
// offsets blob length and the segment length. Both are read out of the value
// itself, one field earlier than any segment bound can apply, so they need their
// own check: a corrupt offsets length is as reachable as a corrupt offset.
func TestVectorSectionFramingCorrupt(t *testing.T) {
	cases := []struct {
		name string
		buf  []byte
	}{
		{"offsets length past buffer", binary.LittleEndian.AppendUint32(nil, 0xFFFFFFF0)},
		{"one byte, no offsets length", []byte{0x01}},
		{"offsets read, segment length truncated", append(binary.LittleEndian.AppendUint32(nil, 0), 0x00)},
		{"offsets blob truncated", binary.LittleEndian.AppendUint32(nil, 32)},
	}

	parsers := []struct {
		name  string
		parse func(*byteops.ReadWriter) error
	}{
		{"unmarshalTargetVectors", func(rw *byteops.ReadWriter) error { _, err := unmarshalTargetVectors(rw); return err }},
		{"unmarshalSingleTargetVector", func(rw *byteops.ReadWriter) error {
			_, err := unmarshalSingleTargetVector(rw, "any", nil)
			return err
		}},
		{"unmarshalMultiVectors", func(rw *byteops.ReadWriter) error { _, err := unmarshalMultiVectors(rw, nil); return err }},
		{"targetVectorsJSONFromBinary", func(rw *byteops.ReadWriter) error { _, err := targetVectorsJSONFromBinary(rw); return err }},
		{"multiVectorsJSONFromBinary", func(rw *byteops.ReadWriter) error { _, err := multiVectorsJSONFromBinary(rw); return err }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// backed by a larger array, so an unchecked read finds bytes to slice
			backing := make([]byte, len(tc.buf)+70_000)
			copy(backing, tc.buf)
			for i := len(tc.buf); i < len(backing); i++ {
				backing[i] = 0xAA
			}
			in := backing[:len(tc.buf)]

			for _, p := range parsers {
				t.Run(p.name, func(t *testing.T) {
					rw := byteops.NewReadWriter(in)
					require.Error(t, p.parse(&rw))
				})
			}
		})
	}
}

// TestEmptyOffsetsSegment pins the cursor hand-off between the two sections. A
// section that declares no offsets still declares a length, and the multi-vector
// parser starts wherever the target-vector parser stopped — so stopping at the
// segment start makes it read target-vector payload as multi-vector framing.
func TestEmptyOffsetsSegment(t *testing.T) {
	t.Run("advances past the declared segment", func(t *testing.T) {
		mvSegment := encodeMultiVec([]float32{7, 8})

		buf := binary.LittleEndian.AppendUint32(nil, 0) // no target vector offsets
		buf = binary.LittleEndian.AppendUint32(buf, 6)  // but six declared bytes
		buf = append(buf, 0xDE, 0xAD, 0xBE, 0xEF, 0xBA, 0xAD)
		buf = append(buf, buildVectorSection(t, map[string]uint32{"colbert": 0},
			uint32(len(mvSegment)), mvSegment, nil)...)

		rw := byteops.NewReadWriter(buf)
		tv, err := unmarshalTargetVectors(&rw)
		require.NoError(t, err)
		require.Nil(t, tv)

		mv, err := unmarshalMultiVectors(&rw, nil)
		require.NoError(t, err)
		require.Equal(t, [][]float32{{7, 8}}, mv["colbert"])
	})

	t.Run("corrupt segment length still errors", func(t *testing.T) {
		buf := binary.LittleEndian.AppendUint32(nil, 0)
		buf = binary.LittleEndian.AppendUint32(buf, 10_000)
		buf = append(buf, 0xDE, 0xAD)

		rw := byteops.NewReadWriter(buf)
		_, err := unmarshalTargetVectors(&rw)
		require.ErrorContains(t, err, "exceeds buffer")
	})
}

// TestExportVectorJSONCorruptOffsets: the export path decodes the same two
// sections straight to JSON, so it needs the same bounds as the object decoders.
func TestExportVectorJSONCorruptOffsets(t *testing.T) {
	trailing := encodeTargetVec(9, 9, 9, 9)

	t.Run("target vectors", func(t *testing.T) {
		segment := encodeTargetVec(1, 2, 3)
		buf := buildVectorSection(t, map[string]uint32{"evil": uint32(len(segment))},
			uint32(len(segment)), segment, trailing)

		rw := byteops.NewReadWriter(buf)
		_, err := targetVectorsJSONFromBinary(&rw)
		require.ErrorContains(t, err, "out of segment bounds")
	})

	t.Run("multi vectors", func(t *testing.T) {
		segment := encodeMultiVec([]float32{1, 2})
		buf := buildVectorSection(t, map[string]uint32{"evil": uint32(len(segment))},
			uint32(len(segment)), segment, trailing)

		rw := byteops.NewReadWriter(buf)
		_, err := multiVectorsJSONFromBinary(&rw)
		require.ErrorContains(t, err, "out of segment bounds")
	})

	t.Run("multi vector document count", func(t *testing.T) {
		segment := binary.LittleEndian.AppendUint32(nil, 1000)
		segment = append(segment, encodeTargetVec(1, 2)...)
		buf := buildVectorSection(t, map[string]uint32{"colbert": 0},
			uint32(len(segment)), segment, trailing)

		rw := byteops.NewReadWriter(buf)
		_, err := multiVectorsJSONFromBinary(&rw)
		require.ErrorContains(t, err, "truncated at document")
	})
}

// TestUnmarshalMultiVectorsCorruptOffsets: same guarantees for the multi-vector
// parser, whose per-document inner reads must also stay inside the declared
// segment.
func TestUnmarshalMultiVectorsCorruptOffsets(t *testing.T) {
	validSegment := encodeMultiVec([]float32{1, 2}, []float32{3, 4}) // 24 bytes
	trailing := encodeTargetVec(9, 9, 9, 9)

	// declares 1000 documents but holds one
	runawaySegment := binary.LittleEndian.AppendUint32(nil, 1000)
	runawaySegment = append(runawaySegment, encodeTargetVec(1, 2)...)

	// single document whose vector length reaches past the segment
	oversizedSegment := binary.LittleEndian.AppendUint32(nil, 1)
	oversizedSegment = append(oversizedSegment, 0x32, 0x00) // vecLen=50, no data

	cases := []struct {
		name    string
		offsets map[string]uint32
		segLen  uint32
		segment []byte
		want    [][]float32
		errLike string
	}{
		{
			name:    "valid",
			offsets: map[string]uint32{"colbert": 0},
			segLen:  uint32(len(validSegment)),
			segment: validSegment,
			want:    [][]float32{{1, 2}, {3, 4}},
		},
		{
			name:    "offset into next section",
			offsets: map[string]uint32{"colbert": uint32(len(validSegment))},
			segLen:  uint32(len(validSegment)),
			segment: validSegment,
			errLike: "out of segment bounds",
		},
		{
			name:    "document count crosses segment end",
			offsets: map[string]uint32{"colbert": 0},
			segLen:  uint32(len(runawaySegment)),
			segment: runawaySegment,
			errLike: "truncated at document",
		},
		{
			name:    "document length crosses segment end",
			offsets: map[string]uint32{"colbert": 0},
			segLen:  uint32(len(oversizedSegment)),
			segment: oversizedSegment,
			errLike: "exceeds segment",
		},
		{
			name:    "segment length exceeds buffer",
			offsets: map[string]uint32{"colbert": 0},
			segLen:  10_000,
			segment: validSegment,
			errLike: "exceeds buffer",
		},
		{
			name:    "offset beyond buffer",
			offsets: map[string]uint32{"colbert": 60_000},
			segLen:  uint32(len(validSegment)),
			segment: validSegment,
			errLike: "out of segment bounds",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := buildVectorSection(t, tc.offsets, tc.segLen, tc.segment, trailing)

			rw := byteops.NewReadWriter(buf)
			got, err := unmarshalMultiVectors(&rw, nil)

			if tc.errLike != "" {
				require.ErrorContains(t, err, tc.errLike)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got["colbert"])
		})
	}
}

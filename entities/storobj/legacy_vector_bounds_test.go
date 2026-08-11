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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// distinctiveVector returns a vector whose values are cheap to eyeball and
// unlikely to collide with zero-filled or garbage output.
func distinctiveVector(dims int) []float32 {
	vec := make([]float32, dims)
	for i := range vec {
		vec[i] = float32(i%100) + 0.5
	}
	return vec
}

func legacyVectorTestObject(docID uint64, vec []float32) *Object {
	obj := New(docID)
	obj.Object = models.Object{
		ID:         strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
		Class:      "LegacyVectorBoundsClass",
		Properties: map[string]interface{}{"name": "x"},
	}
	obj.Vector = vec
	return obj
}

// TestLegacyVectorRoundTrip covers the dimension counts that straddle the uint16
// scaling boundary: the on-disk length field is a uint16 and the writer permits up to
// maxVectorLength (65535) dims, so scaling it to bytes without widening first wraps
// from 16384 dims upwards — 16384 wraps to zero (all-zero vector), higher counts wrap
// to a partial length (silently truncated vector), both without an error.
func TestLegacyVectorRoundTrip(t *testing.T) {
	dimsList := []int{1, 1000, 16383, 16384, 20000, 65535}

	for _, dims := range dimsList {
		t.Run(fmt.Sprintf("%d dims", dims), func(t *testing.T) {
			vec := distinctiveVector(dims)
			obj := legacyVectorTestObject(1, vec)

			data, err := obj.MarshalBinary()
			require.NoError(t, err)

			t.Run("VectorFromBinary", func(t *testing.T) {
				got, err := VectorFromBinary(data, nil, "")
				require.NoError(t, err)
				require.Equal(t, vec, got)
			})

			t.Run("full object decode", func(t *testing.T) {
				full, err := FromBinaryDisk(data, "LegacyVectorBoundsClass")
				require.NoError(t, err)
				require.Equal(t, vec, full.Vector)
			})
		})
	}
}

// TestMultiVectorFromBinaryPastOversizedLegacyVector pins the same overflow in
// MultiVectorFromBinary, which uses the legacy vector's end offset as the start
// of a pos-cursor walk (classNameLength, schemaLength, ...) to reach the
// multivector segment. A wrong vecEnd misaligns that whole walk, not just the
// legacy vector itself.
func TestMultiVectorFromBinaryPastOversizedLegacyVector(t *testing.T) {
	dimsList := []int{16383, 16384, 20000, 65535}
	multiVec := [][]float32{{1, 2, 3}, {4, 5, 6}}

	for _, dims := range dimsList {
		t.Run(fmt.Sprintf("%d dims", dims), func(t *testing.T) {
			vec := distinctiveVector(dims)
			obj := legacyVectorTestObject(1, vec)
			obj.MultiVectors = map[string][][]float32{"mv": multiVec}

			data, err := obj.MarshalBinary()
			require.NoError(t, err)

			// Assert the multivector decode independently of VectorFromBinary (already
			// pinned in TestLegacyVectorRoundTrip) so a wrong pos-cursor walk here isn't
			// masked by the legacy vector's own bounds bug.
			gotMV, err := MultiVectorFromBinary(data, nil, "mv")
			require.NoError(t, err)
			require.Equal(t, multiVec, gotMV)
		})
	}
}

// TestVectorFromBinaryTruncatedInputErrors builds truncated views of a valid
// object as subslices of a larger backing array whose tail is filled with a
// 0xAA sentinel, mirroring how a value returned from an mmapped LSM segment
// can have capacity well past its declared length. Pre-fix, in[44:44+n] was an
// unchecked slice: Go's two-index slice bound check is against cap, not len, so
// the read silently succeeded and copied the sentinel (standing in for a
// neighbouring object's bytes) into the returned vector instead of erroring.
func TestVectorFromBinaryTruncatedInputErrors(t *testing.T) {
	const dims = 500
	vec := distinctiveVector(dims)
	obj := legacyVectorTestObject(1, vec)

	data, err := obj.MarshalBinary()
	require.NoError(t, err)

	vecStart := marshallerV1HeaderLen + 2
	vecEnd := vecStart + dims*4
	require.Less(t, vecEnd, len(data), "test object must have data trailing the vector")

	cases := []struct {
		name         string
		truncatedLen int
	}{
		{"single byte", 1},
		{"mid docID", 5},
		{"exactly header, no length field", marshallerV1HeaderLen},
		{"one byte into length field", marshallerV1HeaderLen + 1},
		{"length field present, zero vector bytes", vecStart},
		{"half the vector present", vecStart + dims*4/2},
		{"one byte short of full vector", vecEnd - 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backing := make([]byte, len(data)+70_000)
			copy(backing, data[:tc.truncatedLen])
			for i := tc.truncatedLen; i < len(backing); i++ {
				backing[i] = 0xAA
			}
			in := backing[:tc.truncatedLen]

			_, err := VectorFromBinary(in, nil, "")
			require.Error(t, err)
		})
	}
}

// TestZeroLengthNamedVectorDecodesNonNil pins readTargetVectorAt: a nil buffer
// must allocate even for a zero-length vector, because nil[:0] is nil and a
// caller holding the result in a map would see an absent vector rather than an
// explicitly empty one.
func TestZeroLengthNamedVectorDecodesNonNil(t *testing.T) {
	obj := legacyVectorTestObject(1, nil)
	obj.Vectors = map[string][]float32{
		"empty": {},
		"full":  {1, 2},
	}

	data, err := obj.MarshalBinary()
	require.NoError(t, err)

	full, err := FromBinaryDisk(data, "LegacyVectorBoundsClass")
	require.NoError(t, err)

	require.Contains(t, full.Vectors, "empty")
	require.NotNil(t, full.Vectors["empty"])
	require.Len(t, full.Vectors["empty"], 0)
	require.Equal(t, []float32{1, 2}, full.Vectors["full"])
}

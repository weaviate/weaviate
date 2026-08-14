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

	"github.com/weaviate/weaviate/entities/additional"
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
// scaling boundary: unwidened, 16384 dims scale to zero bytes and higher counts to
// a partial length, so the vector comes back empty or truncated without an error.
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

// TestMultiVectorFromBinaryPastOversizedLegacyVector pins the same widening in
// skipToVectorSections, which skips the legacy vector to reach the sections
// behind it. A wrong vector length misaligns every field after it, not just the
// vector.
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

			// asserted independently of VectorFromBinary, which walks to the sections
			// by a different route
			gotMV, err := MultiVectorFromBinary(data, "mv")
			require.NoError(t, err)
			require.Equal(t, multiVec, gotMV)
		})
	}
}

// truncatedView cuts data to n bytes but backs it with a larger array whose tail
// holds a 0xAA sentinel, the way an mmapped LSM segment hands back a value with
// capacity past its length. An unchecked read then yields the sentinel — standing
// in for a neighbouring object's bytes — rather than panicking.
func truncatedView(data []byte, n int) []byte {
	backing := make([]byte, len(data)+70_000)
	copy(backing, data[:n])
	for i := n; i < len(backing); i++ {
		backing[i] = 0xAA
	}
	return backing[:n]
}

// TestTruncatedObjectDecodersError runs every decoder that reads a vector out of
// an object value over one truncation matrix. Each reaches the vector sections by
// its own walk, so a bound on one says nothing about the others.
func TestTruncatedObjectDecodersError(t *testing.T) {
	const (
		dims      = 500
		className = "LegacyVectorBoundsClass"
	)
	vec := distinctiveVector(dims)
	obj := legacyVectorTestObject(1, vec)
	obj.Vectors = map[string][]float32{"nv": {1, 2, 3}}
	obj.MultiVectors = map[string][][]float32{"mv": {{1, 2}, {3, 4}}}

	data, err := obj.MarshalBinary()
	require.NoError(t, err)

	vecStart := marshallerV1HeaderLen + 2
	vecEnd := vecStart + dims*4
	require.Less(t, vecEnd, len(data), "test object must have data trailing the vector")

	decoders := []struct {
		name   string
		decode func([]byte) (any, error)
	}{
		{"VectorFromBinary legacy", func(in []byte) (any, error) { return VectorFromBinary(in, nil, "") }},
		{"VectorFromBinary named", func(in []byte) (any, error) { return VectorFromBinary(in, nil, "nv") }},
		{"MultiVectorFromBinary", func(in []byte) (any, error) { return MultiVectorFromBinary(in, "mv") }},
		{"FromBinaryDisk", func(in []byte) (any, error) { return FromBinaryDisk(in, className) }},
		{"FromBinaryOptionalDisk", func(in []byte) (any, error) {
			return FromBinaryOptionalDisk(in, className,
				additional.Properties{Vector: true, Vectors: []string{"nv", "mv"}}, nil)
		}},
		{"UnmarshalBinaryDisk", func(in []byte) (any, error) {
			decoded := &Object{}
			return decoded, decoded.UnmarshalBinaryDisk(in, className)
		}},
		{"ExportFieldsFromBinary", func(in []byte) (any, error) { return ExportFieldsFromBinary(in) }},
	}

	lengths := []struct {
		name string
		n    int
	}{
		{"single byte", 1},
		{"mid docID", 5},
		{"exactly header, no length field", marshallerV1HeaderLen},
		{"one byte into length field", marshallerV1HeaderLen + 1},
		{"length field present, zero vector bytes", vecStart},
		{"half the vector present", vecStart + dims*4/2},
		{"one byte short of full vector", vecEnd - 1},
		{"vector complete, nothing after", vecEnd},
		{"into the class name", vecEnd + 3},
		{"into the vector sections", vecEnd + (len(data)-vecEnd)/2},
		{"one byte short of complete", len(data) - 1},
	}

	for _, dec := range decoders {
		t.Run(dec.name, func(t *testing.T) {
			want, err := dec.decode(data)
			require.NoError(t, err, "the complete object must decode")

			for _, tc := range lengths {
				t.Run(tc.name, func(t *testing.T) {
					got, err := dec.decode(truncatedView(data, tc.n))

					// Truncations below the legacy vector's end cut a field every
					// decoder reads, so none of them can legitimately succeed.
					if tc.n < vecEnd {
						require.Error(t, err)
						return
					}

					// Past that point a decoder may stop before the truncation. What
					// it must never do is reconstruct a plausible-looking answer out
					// of the bytes that follow the value.
					if err == nil {
						require.Equal(t, want, got)
					}
				})
			}
		})
	}
}

// TestZeroLengthNamedVectorDecodesNonNil pins readVectorInto: a nil buffer must
// allocate even for a zero-length vector, because nil[:0] is nil and a caller
// holding the result in a map would see an absent vector rather than an empty one.
func TestZeroLengthNamedVectorDecodesNonNil(t *testing.T) {
	obj := legacyVectorTestObject(1, nil)
	obj.Vectors = map[string][]float32{
		"empty": {},
		"full":  {1, 2},
	}

	data, err := obj.MarshalBinary()
	require.NoError(t, err)

	t.Run("VectorFromBinary", func(t *testing.T) {
		empty, err := VectorFromBinary(data, nil, "empty")
		require.NoError(t, err)
		require.NotNil(t, empty)
		require.Len(t, empty, 0)

		full, err := VectorFromBinary(data, nil, "full")
		require.NoError(t, err)
		require.Equal(t, []float32{1, 2}, full)
	})

	t.Run("full object decode", func(t *testing.T) {
		full, err := FromBinaryDisk(data, "LegacyVectorBoundsClass")
		require.NoError(t, err)

		require.Contains(t, full.Vectors, "empty")
		require.NotNil(t, full.Vectors["empty"])
		require.Len(t, full.Vectors["empty"], 0)
		require.Equal(t, []float32{1, 2}, full.Vectors["full"])
	})
}

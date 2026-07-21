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
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

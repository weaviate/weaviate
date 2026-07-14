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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
)

func preparedMarshalTestObject() *Object {
	obj := New(7)
	obj.MarshallerVersion = 1
	obj.Object = models.Object{
		ID:                 strfmt.UUID("11111111-2222-3333-4444-555555555555"),
		Class:              "PreparedMarshalClass",
		CreationTimeUnix:   1700000000000,
		LastUpdateTimeUnix: 1700000001000,
		Properties: map[string]interface{}{
			"name":  "prepared",
			"count": float64(3),
		},
	}
	obj.Vector = []float32{0.5, -0.5, 1.5}
	obj.VectorLen = 3
	obj.Vectors = map[string][]float32{"named": {1, 2}}
	obj.MultiVectors = map[string][][]float32{"multi": {{3, 4}, {5}}}
	return obj
}

func TestPreparedMarshalMatchesMarshalBinaryOptional(t *testing.T) {
	obj := preparedMarshalTestObject()

	tests := []struct {
		name     string
		addProps additional.Properties
	}{
		{name: "everything", addProps: additional.Properties{Vector: true, IncludeAllTargetVectors: true}},
		{name: "no_vector", addProps: additional.Properties{}},
		{name: "no_props", addProps: additional.Properties{Vector: true, NoProps: true}},
		{name: "specific_target_vector", addProps: additional.Properties{Vector: true, Vectors: []string{"named"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected, err := obj.MarshalBinaryOptional(tt.addProps)
			require.NoError(t, err)

			pm, err := obj.PrepareMarshalOptional(tt.addProps)
			require.NoError(t, err)
			require.Equal(t, len(expected), pm.Len())

			buf := make([]byte, pm.Len())
			require.NoError(t, pm.MarshalTo(buf))
			assert.Equal(t, expected, buf)
		})
	}
}

func TestPreparedMarshalToBufferSizeMismatch(t *testing.T) {
	obj := preparedMarshalTestObject()

	pm, err := obj.PrepareMarshalOptional(additional.Properties{Vector: true, IncludeAllTargetVectors: true})
	require.NoError(t, err)

	tooSmall := make([]byte, pm.Len()-1)
	err = pm.MarshalTo(tooSmall)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a buffer of exactly")

	tooLarge := make([]byte, pm.Len()+1)
	err = pm.MarshalTo(tooLarge)
	require.Error(t, err)
}

func TestPreparedMarshalUnsupportedVersion(t *testing.T) {
	obj := preparedMarshalTestObject()
	obj.MarshallerVersion = 2

	_, err := obj.PrepareMarshalOptional(additional.Properties{Vector: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported marshaller version")
}

// TestPreparedMarshalSurvivesSourceMutation pins the documented contract that
// a PreparedMarshal is self-contained with respect to the source object's
// *structure*: replacing the object's fields (maps, slices, props) after the
// prepare pass must not change what MarshalTo writes.
func TestPreparedMarshalSurvivesSourceMutation(t *testing.T) {
	obj := preparedMarshalTestObject()
	addProps := additional.Properties{Vector: true, IncludeAllTargetVectors: true}

	expected, err := obj.MarshalBinaryOptional(addProps)
	require.NoError(t, err)

	pm, err := obj.PrepareMarshalOptional(addProps)
	require.NoError(t, err)

	// replace (not mutate in place) everything on the source object
	obj.Object.Properties = map[string]interface{}{"other": "value"}
	obj.Vector = []float32{9, 9, 9, 9}
	obj.Vectors = map[string][]float32{"different": {8}}
	obj.MultiVectors = nil
	obj.DocID = 999

	buf := make([]byte, pm.Len())
	require.NoError(t, pm.MarshalTo(buf))
	assert.Equal(t, expected, buf)
}

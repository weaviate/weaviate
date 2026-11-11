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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
)

// The token ids come back from the disk and the network forms, and an object
// without them has the bytes it had before the section existed.
func TestTokenIDsSection(t *testing.T) {
	newObject := func(ids models.TokenIds) *Object {
		obj := FromObject(&models.Object{
			Class:      "Doc",
			ID:         strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Properties: map[string]interface{}{"text": "x"},
			TokenIds:   ids,
		}, nil, map[string][]float32{"single": {1, 2}},
			map[string][][]float32{"a": {{1, 2, 3}, {4, 5, 6}}, "b": {{7, 8, 9}}})
		obj.DocID = 7
		return obj
	}
	ids := models.TokenIds{"a": {101, 2054}, "b": {7}}

	t.Run("disk", func(t *testing.T) {
		bin, err := newObject(ids).MarshalBinaryDisk(false)
		require.NoError(t, err)
		back, err := FromBinaryDisk(bin, "Doc")
		require.NoError(t, err)
		require.Equal(t, ids, back.Object.TokenIds)
		require.Equal(t, [][]float32{{1, 2, 3}, {4, 5, 6}}, back.MultiVectors["a"])

		got, err := TokenIDsFromBinary(bin, "a")
		require.NoError(t, err)
		require.Equal(t, []uint32{101, 2054}, got)
		got, err = TokenIDsFromBinary(bin, "missing")
		require.NoError(t, err)
		require.Nil(t, got)

		// the readers that stop at the multi-vectors still read them
		mv, _, err := MultiVectorFromBinaryInto(bin, nil, "b")
		require.NoError(t, err)
		require.Equal(t, [][]float32{{7, 8, 9}}, mv)
	})

	t.Run("network", func(t *testing.T) {
		bin, err := newObject(ids).MarshalBinaryOptional(additional.Properties{Vector: true, IncludeAllTargetVectors: true})
		require.NoError(t, err)
		back, err := FromBinaryNetwork(bin)
		require.NoError(t, err)
		require.Equal(t, ids, back.Object.TokenIds)
	})

	t.Run("an object without token ids keeps its bytes", func(t *testing.T) {
		// one multi-vector and one target vector: with several, their order
		// in the segment follows map iteration and differs between marshals
		single := func(ids models.TokenIds) *Object {
			obj := FromObject(&models.Object{
				Class:      "Doc",
				ID:         strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
				Properties: map[string]interface{}{"text": "x"},
				TokenIds:   ids,
			}, nil, map[string][]float32{"single": {1, 2}},
				map[string][][]float32{"a": {{1, 2, 3}, {4, 5, 6}}})
			obj.DocID = 7
			return obj
		}
		without, err := single(nil).MarshalBinaryDisk(false)
		require.NoError(t, err)
		with, err := single(models.TokenIds{"a": {101, 2054}}).MarshalBinaryDisk(false)
		require.NoError(t, err)
		// the section is appended, nothing before it moves
		require.Equal(t, without, with[:len(without)])
		got, err := TokenIDsFromBinary(without, "a")
		require.NoError(t, err)
		require.Nil(t, got)
		back, err := FromBinaryDisk(without, "Doc")
		require.NoError(t, err)
		require.Nil(t, back.Object.TokenIds)
	})

	t.Run("a deep copy keeps them", func(t *testing.T) {
		obj := newObject(ids)
		cp := obj.DeepCopyDangerous()
		require.Equal(t, ids, cp.Object.TokenIds)
		cp.Object.TokenIds["a"][0] = 1
		require.Equal(t, uint32(101), obj.Object.TokenIds["a"][0])
	})
}

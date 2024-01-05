//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package roaringset

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

func Test_BitmapLayers_Flatten(t *testing.T) {
	type inputSegment struct {
		additions []uint64
		deletions []uint64
	}

	type test struct {
		name                 string
		inputs               []inputSegment
		expectedContained    []uint64
		expectedNotContained []uint64
	}

	tests := []test{
		{
			name:                 "no inputs",
			inputs:               nil,
			expectedContained:    nil,
			expectedNotContained: nil,
		},
		{
			name: "single segment",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
			},
			expectedContained:    []uint64{4, 5},
			expectedNotContained: nil,
		},
		{
			name: "three segments, only additions",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
				{
					additions: []uint64{5, 6},
				},
				{
					additions: []uint64{6, 7, 8},
				},
			},
			expectedContained:    []uint64{4, 5, 6, 7, 8},
			expectedNotContained: nil,
		},
		{
			name: "two segments, including a delete",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
				{
					additions: []uint64{5, 6},
					deletions: []uint64{4},
				},
			},
			expectedContained:    []uint64{5, 6},
			expectedNotContained: []uint64{4},
		},
		{
			name: "three segments, including a delete, and a re-add",
			inputs: []inputSegment{
				{
					additions: []uint64{3, 4, 5},
				},
				{
					additions: []uint64{6},
					deletions: []uint64{4, 5},
				},
				{
					additions: []uint64{5},
				},
			},
			expectedContained:    []uint64{3, 5, 6},
			expectedNotContained: []uint64{4},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := make(BitmapLayers, len(test.inputs))
			for i, inp := range test.inputs {
				input[i].Additions = NewBitmap(inp.additions...)
				input[i].Deletions = NewBitmap(inp.deletions...)
			}

			res := input.Flatten()
			for _, x := range test.expectedContained {
				assert.True(t, res.Contains(x))
			}

			for _, x := range test.expectedNotContained {
				assert.False(t, res.Contains(x))
			}
		})
	}
}

func Test_BitmapLayers_Merge(t *testing.T) {
	type inputSegment struct {
		additions []uint64
		deletions []uint64
	}

	type test struct {
		name              string
		inputs            []inputSegment
		expectedAdditions []uint64
		expectedDeletions []uint64
		expectErr         bool
	}

	tests := []test{
		{
			name:              "no inputs - should error",
			inputs:            nil,
			expectedAdditions: nil,
			expectedDeletions: nil,
			expectErr:         true,
		},
		{
			name: "single layer - should error",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
			},
			expectedAdditions: nil,
			expectedDeletions: nil,
			expectErr:         true,
		},
		{
			name: "three layers - should error",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
				{
					additions: []uint64{4, 5},
				},
				{
					additions: []uint64{4, 5},
				},
			},
			expectedAdditions: nil,
			expectedDeletions: nil,
			expectErr:         true,
		},
		{
			name: "two layers, only additions",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
				{
					additions: []uint64{5, 6, 7},
				},
			},
			expectedAdditions: []uint64{4, 5, 6, 7},
			expectedDeletions: nil,
		},
		{
			name: "additions and deletions without overlap",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
					deletions: []uint64{1, 2},
				},
				{
					additions: []uint64{5, 6, 7},
					deletions: []uint64{2, 3},
				},
			},
			expectedAdditions: []uint64{4, 5, 6, 7},
			expectedDeletions: []uint64{1, 2, 3},
		},
		{
			name: "previously deleted element, re-added",
			inputs: []inputSegment{
				{
					additions: []uint64{},
					deletions: []uint64{1, 2},
				},
				{
					additions: []uint64{2},
					deletions: []uint64{},
				},
			},
			expectedAdditions: []uint64{2},
			expectedDeletions: []uint64{1},
		},
		{
			name: "previously added element deleted later",
			inputs: []inputSegment{
				{
					additions: []uint64{3, 4},
					deletions: []uint64{},
				},
				{
					additions: []uint64{},
					deletions: []uint64{3},
				},
			},
			expectedAdditions: []uint64{4},
			expectedDeletions: []uint64{3},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := make(BitmapLayers, len(test.inputs))
			for i, inp := range test.inputs {
				input[i].Additions = NewBitmap(inp.additions...)
				input[i].Deletions = NewBitmap(inp.deletions...)
			}

			res, err := input.Merge()
			if test.expectErr {
				require.NotNil(t, err)
				return
			} else {
				require.Nil(t, err)
			}
			for _, x := range test.expectedAdditions {
				assert.True(t, res.Additions.Contains(x))
			}

			for _, x := range test.expectedDeletions {
				assert.True(t, res.Deletions.Contains(x))
			}

			intersect := sroar.And(res.Additions, res.Deletions)
			assert.True(t, intersect.IsEmpty(),
				"verify that additions and deletions never intersect")
		})
	}
}

func Test_BitmapLayer_Clone(t *testing.T) {
	t.Run("cloning empty BitmapLayer", func(t *testing.T) {
		layerEmpty := BitmapLayer{}

		cloned := layerEmpty.Clone()

		assert.Nil(t, cloned.Additions)
		assert.Nil(t, cloned.Deletions)
	})

	t.Run("cloning partially inited BitmapLayer", func(t *testing.T) {
		additions := NewBitmap(1)
		deletions := NewBitmap(100)

		layerAdditions := BitmapLayer{Additions: additions}
		layerDeletions := BitmapLayer{Deletions: deletions}

		clonedLayerAdditions := layerAdditions.Clone()
		clonedLayerDeletions := layerDeletions.Clone()
		additions.Remove(1)
		deletions.Remove(100)

		assert.True(t, layerAdditions.Additions.IsEmpty())
		assert.ElementsMatch(t, []uint64{1}, clonedLayerAdditions.Additions.ToArray())
		assert.Nil(t, clonedLayerAdditions.Deletions)

		assert.True(t, layerDeletions.Deletions.IsEmpty())
		assert.Nil(t, clonedLayerDeletions.Additions)
		assert.ElementsMatch(t, []uint64{100}, clonedLayerDeletions.Deletions.ToArray())
	})

	t.Run("cloning fully inited BitmapLayer", func(t *testing.T) {
		additions := NewBitmap(1)
		deletions := NewBitmap(100)

		layer := BitmapLayer{Additions: additions, Deletions: deletions}

		clonedLayer := layer.Clone()
		additions.Remove(1)
		deletions.Remove(100)

		assert.True(t, layer.Additions.IsEmpty())
		assert.True(t, layer.Deletions.IsEmpty())
		assert.ElementsMatch(t, []uint64{1}, clonedLayer.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{100}, clonedLayer.Deletions.ToArray())
	})
}

// This test aims to prevent a regression on
// https://github.com/weaviate/sroar/issues/1
// found in Serialized Roaring Bitmaps library
func Test_BitmapLayers_Merge_PanicSliceBoundOutOfRange(t *testing.T) {
	genSlice := func(fromInc, toExc uint64) []uint64 {
		slice := []uint64{}
		for i := fromInc; i < toExc; i++ {
			slice = append(slice, i)
		}
		return slice
	}

	leftLayer := BitmapLayer{Deletions: NewBitmap(genSlice(289_800, 290_100)...)}
	rightLayer := BitmapLayer{Additions: NewBitmap(genSlice(290_000, 293_000)...)}

	failingDeletionsLayer, err := BitmapLayers{leftLayer, rightLayer}.Merge()
	assert.Nil(t, err)

	assert.ElementsMatch(t, genSlice(289_800, 290_000), failingDeletionsLayer.Deletions.ToArray())
}

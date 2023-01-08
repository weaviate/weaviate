package roaringset

import (
	"testing"

	"github.com/dgraph-io/sroar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				input[i].Additions = sroar.NewBitmap()
				input[i].Additions.SetMany(inp.additions)
				input[i].Deletions = sroar.NewBitmap()
				input[i].Deletions.SetMany(inp.deletions)
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
				input[i].Additions = sroar.NewBitmap()
				input[i].Additions.SetMany(inp.additions)
				input[i].Deletions = sroar.NewBitmap()
				input[i].Deletions.SetMany(inp.deletions)
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

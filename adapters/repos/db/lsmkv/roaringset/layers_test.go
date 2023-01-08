package roaringset

import (
	"testing"

	"github.com/dgraph-io/sroar"
	"github.com/stretchr/testify/assert"
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

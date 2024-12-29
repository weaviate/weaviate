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

package db

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestSearchDeduplication(t *testing.T) {
	type idAndDistPair struct {
		id   string
		dist float32
	}

	type shardResult struct {
		elems []idAndDistPair
	}

	type test struct {
		name           string
		input          []shardResult
		expectedOutput []idAndDistPair
	}

	tests := []test{
		{
			name: "single input",
			input: []shardResult{
				{
					elems: []idAndDistPair{
						{id: "1", dist: 0.1},
					},
				},
			},
			expectedOutput: []idAndDistPair{
				{id: "1", dist: 0.1},
			},
		},
		{
			name: "2 shards with full overlap",
			input: []shardResult{
				{
					elems: []idAndDistPair{
						{id: "1", dist: 0.1},
						{id: "2", dist: 0.2},
						{id: "3", dist: 0.3},
						{id: "4", dist: 0.4},
					},
				},
				{
					elems: []idAndDistPair{
						{id: "1", dist: 0.1},
						{id: "2", dist: 0.2},
						{id: "3", dist: 0.3},
						{id: "4", dist: 0.4},
					},
				},
			},
			expectedOutput: []idAndDistPair{
				{id: "1", dist: 0.1},
				{id: "2", dist: 0.2},
				{id: "3", dist: 0.3},
				{id: "4", dist: 0.4},
			},
		},
		{
			name: "2 shards with no overlap, best result in first shard",
			input: []shardResult{
				{
					elems: []idAndDistPair{
						{id: "001", dist: 0.1},
						{id: "003", dist: 0.3},
					},
				},
				{
					elems: []idAndDistPair{
						{id: "002", dist: 0.2},
						{id: "004", dist: 0.4},
					},
				},
			},
			expectedOutput: []idAndDistPair{
				{id: "001", dist: 0.1},
				{id: "002", dist: 0.2},
				{id: "003", dist: 0.3},
				{id: "004", dist: 0.4},
			},
		},
		{
			name: "2 shards with no overlap, best result in second shard",
			input: []shardResult{
				{
					elems: []idAndDistPair{
						{id: "002", dist: 0.2},
						{id: "004", dist: 0.4},
					},
				},
				{
					elems: []idAndDistPair{
						{id: "001", dist: 0.1},
						{id: "003", dist: 0.3},
					},
				},
			},
			expectedOutput: []idAndDistPair{
				{id: "001", dist: 0.1},
				{id: "002", dist: 0.2},
				{id: "003", dist: 0.3},
				{id: "004", dist: 0.4},
			},
		},
		{
			name: "2 shards with full overlap, but shard 1 has lower scores for some elements",
			input: []shardResult{
				{
					elems: []idAndDistPair{
						{id: "1", dist: 0.1},
						{id: "2", dist: 0.2},
						{id: "3", dist: 0.3},
						{id: "4", dist: 0.4},
					},
				},
				{
					elems: []idAndDistPair{
						{id: "1", dist: 0.15},
						{id: "2", dist: 0.2},
						{id: "3", dist: 0.35},
						{id: "4", dist: 0.4},
					},
				},
			},
			expectedOutput: []idAndDistPair{
				{id: "1", dist: 0.1},
				{id: "2", dist: 0.2},
				{id: "3", dist: 0.3},
				{id: "4", dist: 0.4},
			},
		},
		{
			name: "2 shards with full overlap, but shard 1 has higher scores for some elements",
			input: []shardResult{
				{
					elems: []idAndDistPair{
						{id: "1", dist: 0.15},
						{id: "2", dist: 0.2},
						{id: "3", dist: 0.35},
						{id: "4", dist: 0.4},
					},
				},
				{
					elems: []idAndDistPair{
						{id: "1", dist: 0.1},
						{id: "2", dist: 0.2},
						{id: "3", dist: 0.3},
						{id: "4", dist: 0.4},
					},
				},
			},
			expectedOutput: []idAndDistPair{
				{id: "1", dist: 0.1},
				{id: "2", dist: 0.2},
				{id: "3", dist: 0.3},
				{id: "4", dist: 0.4},
			},
		},
		{
			name: "choas with some overlap, some new results, some differing scores",
			input: []shardResult{
				{
					elems: []idAndDistPair{
						{id: "1", dist: 0.1},
						{id: "3", dist: 0.102},
						{id: "4", dist: 0.1},
					},
				},
				{
					elems: []idAndDistPair{
						{id: "2", dist: 0.099},
						{id: "3", dist: 0.101},
					},
				},
				{
					elems: []idAndDistPair{
						{id: "1", dist: 0.1},
						{id: "2", dist: 0.1},
						{id: "3", dist: 0.1},
						{id: "4", dist: 0.1},
						{id: "5", dist: 0.098},
					},
				},
			},
			expectedOutput: []idAndDistPair{
				{id: "1", dist: 0.1},
				{id: "2", dist: 0.099},
				{id: "3", dist: 0.1},
				{id: "4", dist: 0.1},
				{id: "5", dist: 0.098},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Build input
			input := []*storobj.Object{}
			inputDists := []float32{}
			docID := uint64(0)
			for _, shard := range test.input {
				for _, elem := range shard.elems {
					obj := storobj.New(docID)
					obj.Object.ID = strfmt.UUID(elem.id)
					input = append(input, obj)
					docID++ // the docIDs don't matter for this test, but we need them to create the storobjs
					inputDists = append(inputDists, elem.dist)
				}
			}

			// Run function
			out, outDists, err := searchResultDedup(input, inputDists)
			require.NoError(t, err)

			// turn results into idAndDistPair for easier comparison
			output := make([]idAndDistPair, len(out))
			for i, obj := range out {
				output[i] = idAndDistPair{obj.ID().String(), outDists[i]}
			}

			assert.ElementsMatch(t, test.expectedOutput, output)
		})
	}
}

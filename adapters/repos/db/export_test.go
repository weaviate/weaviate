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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssignShardsToNodes(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string][]string
		expected map[string][]string
	}{
		{
			name:     "empty input",
			input:    map[string][]string{},
			expected: map[string][]string{},
		},
		{
			name:  "single shard single node",
			input: map[string][]string{"s1": {"nodeA"}},
			expected: map[string][]string{
				"nodeA": {"s1"},
			},
		},
		{
			name:  "single shard multiple replicas picks alphabetically first",
			input: map[string][]string{"s1": {"nodeC", "nodeA", "nodeB"}},
			expected: map[string][]string{
				"nodeA": {"s1"},
			},
		},
		{
			name: "multiple shards same replica set distributes evenly",
			input: map[string][]string{
				"s1": {"nodeA", "nodeB"},
				"s2": {"nodeA", "nodeB"},
			},
			expected: map[string][]string{
				"nodeA": {"s1"},
				"nodeB": {"s2"},
			},
		},
		{
			name: "three shards two nodes",
			input: map[string][]string{
				"s1": {"nodeA", "nodeB"},
				"s2": {"nodeA", "nodeB"},
				"s3": {"nodeA", "nodeB"},
			},
			expected: map[string][]string{
				"nodeA": {"s1", "s3"},
				"nodeB": {"s2"},
			},
		},
		{
			name: "overlapping but different replica sets",
			input: map[string][]string{
				"s1": {"nodeA", "nodeB"},
				"s2": {"nodeB", "nodeC"},
				"s3": {"nodeA", "nodeC"},
			},
			expected: map[string][]string{
				"nodeA": {"s1"},
				"nodeB": {"s2"},
				"nodeC": {"s3"},
			},
		},
		{
			name: "uneven replica counts",
			input: map[string][]string{
				"s1": {"nodeA"},
				"s2": {"nodeB"},
				"s3": {"nodeA", "nodeB", "nodeC"},
			},
			expected: map[string][]string{
				"nodeA": {"s1"},
				"nodeB": {"s2"},
				"nodeC": {"s3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := assignShardsToNodes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

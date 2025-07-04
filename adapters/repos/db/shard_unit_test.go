//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardPathDimensionsLSM(t *testing.T) {
	tests := []struct {
		name      string
		indexPath string
		shardName string
		expected  string
	}{
		{
			name:      "basic path",
			indexPath: "/data/index",
			shardName: "shard1",
			expected:  "/data/index/shard1/lsm/dimensions",
		},
		{
			name:      "empty shard name",
			indexPath: "/data/index",
			shardName: "",
			expected:  "/data/index/lsm/dimensions",
		},
		{
			name:      "empty index path",
			indexPath: "",
			shardName: "shard1",
			expected:  "shard1/lsm/dimensions",
		},
		{
			name:      "both empty",
			indexPath: "",
			shardName: "",
			expected:  "lsm/dimensions",
		},
		{
			name:      "relative paths",
			indexPath: "data/index",
			shardName: "shard1",
			expected:  "data/index/shard1/lsm/dimensions",
		},
		{
			name:      "with special characters in shard name",
			indexPath: "/data/index",
			shardName: "shard-1_test",
			expected:  "/data/index/shard-1_test/lsm/dimensions",
		},
		{
			name:      "with spaces in shard name",
			indexPath: "/data/index",
			shardName: "shard 1",
			expected:  "/data/index/shard 1/lsm/dimensions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shardPathDimensionsLSM(tt.indexPath, tt.shardName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShardPathObjectsLSM(t *testing.T) {
	tests := []struct {
		name      string
		indexPath string
		shardName string
		expected  string
	}{
		{
			name:      "basic path",
			indexPath: "/data/index",
			shardName: "shard1",
			expected:  "/data/index/shard1/lsm/objects",
		},
		{
			name:      "empty shard name",
			indexPath: "/data/index",
			shardName: "",
			expected:  "/data/index/lsm/objects",
		},
		{
			name:      "empty index path",
			indexPath: "",
			shardName: "shard1",
			expected:  "shard1/lsm/objects",
		},
		{
			name:      "both empty",
			indexPath: "",
			shardName: "",
			expected:  "lsm/objects",
		},
		{
			name:      "relative paths",
			indexPath: "data/index",
			shardName: "shard1",
			expected:  "data/index/shard1/lsm/objects",
		},
		{
			name:      "with special characters in shard name",
			indexPath: "/data/index",
			shardName: "shard-1_test",
			expected:  "/data/index/shard-1_test/lsm/objects",
		},
		{
			name:      "with spaces in shard name",
			indexPath: "/data/index",
			shardName: "shard 1",
			expected:  "/data/index/shard 1/lsm/objects",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shardPathObjectsLSM(tt.indexPath, tt.shardName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

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

package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReport_OmitsEmptyFields(t *testing.T) {
	tests := []struct {
		name     string
		report   Report
		expected string
	}{
		{
			name:     "completely empty report",
			report:   Report{},
			expected: "{}",
		},
		{
			name: "report with only node name",
			report: Report{
				Node: "test-node",
			},
			expected: `{"node":"test-node"}`,
		},
		{
			name: "report with empty collections slice",
			report: Report{
				Node:        "test-node",
				Collections: []*CollectionUsage{},
			},
			expected: `{"node":"test-node"}`,
		},
		{
			name: "report with empty backups slice",
			report: Report{
				Node:    "test-node",
				Backups: []*BackupUsage{},
			},
			expected: `{"node":"test-node"}`,
		},
		{
			name: "complete report",
			report: Report{
				Version: "2025-01-01",
				Node:    "test-node",
				Collections: []*CollectionUsage{
					{Name: "test-collection"},
				},
				Backups: []*BackupUsage{
					{ID: "test-backup"},
				},
			},
			expected: `{"version":"2025-01-01","node":"test-node","collections":[{"name":"test-collection"}],"backups":[{"id":"test-backup"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.report)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(data))
		})
	}
}

func TestCollectionUsage_OmitsEmptyFields(t *testing.T) {
	tests := []struct {
		name     string
		usage    CollectionUsage
		expected string
	}{
		{
			name:     "completely empty collection usage",
			usage:    CollectionUsage{},
			expected: "{}",
		},
		{
			name: "collection usage with only name",
			usage: CollectionUsage{
				Name: "test-collection",
			},
			expected: `{"name":"test-collection"}`,
		},
		{
			name: "collection usage with empty shards slice",
			usage: CollectionUsage{
				Name:   "test-collection",
				Shards: []*ShardUsage{},
			},
			expected: `{"name":"test-collection"}`,
		},
		{
			name: "complete collection usage",
			usage: CollectionUsage{
				Name:              "test-collection",
				ReplicationFactor: 3,
				UniqueShardCount:  2,
				Shards: []*ShardUsage{
					{Name: "test-shard"},
				},
			},
			expected: `{"name":"test-collection","replication_factor":3,"unique_shard_count":2,"shards":[{"name":"test-shard","objects_count":0,"objects_storage_bytes":0,"vector_storage_bytes":0,"index_storage_bytes":0,"full_shard_storage_bytes":0}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.usage)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(data))
		})
	}
}

func TestShardUsage_OmitsEmptyFields(t *testing.T) {
	tests := []struct {
		name     string
		usage    ShardUsage
		expected string
	}{
		{
			name:     "completely empty shard usage",
			usage:    ShardUsage{},
			expected: `{"objects_count":0,"objects_storage_bytes":0,"vector_storage_bytes":0,"index_storage_bytes":0,"full_shard_storage_bytes":0}`,
		},
		{
			name: "shard usage with only name",
			usage: ShardUsage{
				Name: "test-shard",
			},
			expected: `{"name":"test-shard","objects_count":0,"objects_storage_bytes":0,"vector_storage_bytes":0,"index_storage_bytes":0,"full_shard_storage_bytes":0}`,
		},
		{
			name: "shard usage with empty named vectors slice",
			usage: ShardUsage{
				Name:         "test-shard",
				NamedVectors: []*VectorUsage{},
			},
			expected: `{"name":"test-shard","objects_count":0,"objects_storage_bytes":0,"vector_storage_bytes":0,"index_storage_bytes":0,"full_shard_storage_bytes":0}`,
		},
		{
			name: "shard usage with empty index storage",
			usage: ShardUsage{
				Name:                  "test-shard",
				Status:                "active",
				ObjectsCount:          1000,
				ObjectsStorageBytes:   1024,
				VectorStorageBytes:    2048,
				IndexStorageBytes:     0,
				FullShardStorageBytes: 8192,
				NamedVectors: []*VectorUsage{
					{Name: "default"},
				},
			},
			expected: `{"name":"test-shard","status":"active","objects_count":1000,"objects_storage_bytes":1024,"vector_storage_bytes":2048,"index_storage_bytes":0,"full_shard_storage_bytes":8192,"named_vectors":[{"name":"default"}]}`,
		},
		{
			name: "complete shard usage",
			usage: ShardUsage{
				Name:                  "test-shard",
				Status:                "active",
				ObjectsCount:          1000,
				ObjectsStorageBytes:   1024,
				VectorStorageBytes:    2048,
				IndexStorageBytes:     4096,
				FullShardStorageBytes: 8192,
				NamedVectors: []*VectorUsage{
					{Name: "default"},
				},
			},
			expected: `{"name":"test-shard","status":"active","objects_count":1000,"objects_storage_bytes":1024,"vector_storage_bytes":2048,"index_storage_bytes":4096,"full_shard_storage_bytes":8192,"named_vectors":[{"name":"default"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.usage)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(data))
		})
	}
}

func TestVectorUsage_OmitsEmptyFields(t *testing.T) {
	tests := []struct {
		name     string
		usage    VectorUsage
		expected string
	}{
		{
			name:     "completely empty vector usage",
			usage:    VectorUsage{},
			expected: `{"name":""}`,
		},
		{
			name: "vector usage with only name",
			usage: VectorUsage{
				Name: "default",
			},
			expected: `{"name":"default"}`,
		},
		{
			name: "vector usage with empty dimensionalities slice",
			usage: VectorUsage{
				Name:             "default",
				Dimensionalities: []*Dimensionality{},
			},
			expected: `{"name":"default"}`,
		},
		{
			name: "vector usage for legacy vector",
			usage: VectorUsage{
				Name:                   "",
				VectorIndexType:        "flat",
				IsDynamic:              false,
				Compression:            "standard",
				VectorCompressionRatio: 0.75,
				Dimensionalities: []*Dimensionality{
					{Dimensions: 1536, Count: 2000},
				},
			},
			expected: `{"name":"","vector_index_type":"flat","compression":"standard","vector_compression_ratio":0.75,"dimensionalities":[{"dimensionality":1536,"count":2000}]}`,
		},
		{
			name: "complete vector usage",
			usage: VectorUsage{
				Name:                   "default",
				VectorIndexType:        "hnsw",
				IsDynamic:              false,
				Compression:            "standard",
				VectorCompressionRatio: 0.75,
				Dimensionalities: []*Dimensionality{
					{Dimensions: 1536, Count: 1000},
				},
			},
			expected: `{"name":"default","vector_index_type":"hnsw","compression":"standard","vector_compression_ratio":0.75,"dimensionalities":[{"dimensionality":1536,"count":1000}]}`,
		},
		{
			name: "vector usage with is_dynamic true",
			usage: VectorUsage{
				Name:                   "default",
				VectorIndexType:        "hnsw",
				IsDynamic:              true,
				Compression:            "standard",
				VectorCompressionRatio: 0.75,
				Dimensionalities: []*Dimensionality{
					{Dimensions: 1536, Count: 1000},
				},
			},
			expected: `{"name":"default","vector_index_type":"hnsw","is_dynamic":true,"compression":"standard","vector_compression_ratio":0.75,"dimensionalities":[{"dimensionality":1536,"count":1000}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.usage)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(data))
		})
	}
}

func TestDimensionality_OmitsEmptyFields(t *testing.T) {
	tests := []struct {
		name     string
		dim      Dimensionality
		expected string
	}{
		{
			name:     "completely empty dimensionality",
			dim:      Dimensionality{},
			expected: "{}",
		},
		{
			name: "dimensionality with only dimensions",
			dim: Dimensionality{
				Dimensions: 1536,
			},
			expected: `{"dimensionality":1536}`,
		},
		{
			name: "dimensionality with only count",
			dim: Dimensionality{
				Count: 1000,
			},
			expected: `{"count":1000}`,
		},
		{
			name: "complete dimensionality",
			dim: Dimensionality{
				Dimensions: 1536,
				Count:      1000,
			},
			expected: `{"dimensionality":1536,"count":1000}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.dim)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(data))
		})
	}
}

func TestBackupUsage_OmitsEmptyFields(t *testing.T) {
	tests := []struct {
		name     string
		usage    BackupUsage
		expected string
	}{
		{
			name:     "completely empty backup usage",
			usage:    BackupUsage{},
			expected: "{}",
		},
		{
			name: "backup usage with only ID",
			usage: BackupUsage{
				ID: "test-backup",
			},
			expected: `{"id":"test-backup"}`,
		},
		{
			name: "backup usage with empty collections slice",
			usage: BackupUsage{
				ID:          "test-backup",
				Collections: []string{},
			},
			expected: `{"id":"test-backup"}`,
		},
		{
			name: "complete backup usage",
			usage: BackupUsage{
				ID:             "test-backup",
				CompletionTime: "2025-01-01T00:00:00Z",
				SizeInGib:      1.5,
				Type:           "success",
				Collections:    []string{"collection1", "collection2"},
			},
			expected: `{"id":"test-backup","completion_time":"2025-01-01T00:00:00Z","size_in_gib":1.5,"type":"success","collections":["collection1","collection2"]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.usage)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(data))
		})
	}
}

func TestZeroValues_AreOmitted(t *testing.T) {
	// Test that zero values are properly omitted
	report := Report{
		Node:        "",               // empty string
		Collections: nil,              // nil slice
		Backups:     []*BackupUsage{}, // empty slice
	}

	data, err := json.Marshal(report)
	require.NoError(t, err)
	assert.Equal(t, "{}", string(data))
}

func TestNilSlices_AreOmitted(t *testing.T) {
	// Test that nil slices are omitted
	report := Report{
		Node:        "test-node",
		Collections: nil,
		Backups:     nil,
	}

	data, err := json.Marshal(report)
	require.NoError(t, err)
	assert.Equal(t, `{"node":"test-node"}`, string(data))
}

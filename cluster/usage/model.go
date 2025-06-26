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

package usage

// Report represents the usage metrics report from the metrics endpoint
type Report struct {
	// The version of usage policy, date based versioning
	// e.g. 2025-06-01
	Version string `json:"version"`

	// The name of the node
	Node string `json:"node"`

	// List of collections and their metrics
	Collections []*CollectionUsage `json:"collections"`

	// List of backups and their metrics
	Backups []*BackupUsage `json:"backups"`
}

// CollectionUsage represents metrics for a single collection
type CollectionUsage struct {
	// The name of the collection
	Name string `json:"name"`

	// The replication factor of the collection
	ReplicationFactor int `json:"replication_factor"`

	// The number of unique shards in the collection
	UniqueShardCount int `json:"unique_shard_count"`

	// List of shards and their metrics
	Shards []*ShardUsage `json:"shards"`
}

// ShardUsage represents metrics for a single shard
type ShardUsage struct {
	// The name of the shard
	Name string `json:"name"`

	// The number of objects in the shard
	ObjectsCount int `json:"objects_count"`

	// The storage size in bytes
	ObjectsStorageBytes uint64 `json:"objects_storage_bytes"`

	// The actual memory storage bytes used by vectors
	VectorStorageBytes uint64 `json:"vector_storage_bytes"`

	// List of named vectors and their metrics
	NamedVectors []*VectorUsage `json:"named_vectors"`
}

// VectorUsage represents metrics for a single vector index
type VectorUsage struct {
	// The name of the vector
	Name string `json:"name"`

	// The type of vector index
	VectorIndexType string `json:"vectorIndexType"`

	// The compression type used
	Compression string `json:"compression"`

	// The compression ratio achieved
	VectorCompressionRatio float64 `json:"vector_compression_ratio"`

	// The actual memory storage bytes used by vectors
	VectorStorageBytes int64 `json:"vector_storage_bytes"`

	// List of dimensionalities and their metrics
	Dimensionalities []*DimensionalityUsage `json:"dimensionalities"`
}

// DimensionalityUsage represents metrics for a specific dimensionality
type DimensionalityUsage struct {
	// The dimensionality of the vectors
	Dimensionality int `json:"dimensionality"`

	// The number of objects with this dimensionality
	Count int `json:"count"`
}

// BackupUsage represents metrics for a single backup
type BackupUsage struct {
	// The ID of the backup
	ID string `json:"id"`

	// The completion time of the backup
	CompletionTime string `json:"completion_time"`

	// The size of the backup in GiB
	SizeInGib float64 `json:"size_in_gib"`

	// The type of backup
	Type string `json:"type"`

	// The list of collections included in the backup
	Collections []string `json:"collections"`
}

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

// Report represents the usage metrics report from the metrics endpoint
type Report struct {
	// The version of usage policy, date based versioning
	// e.g. 2025-06-01
	Version string `json:"version,omitempty"`

	// The name of the node
	Node string `json:"node,omitempty"`

	// List of collections and their metrics
	Collections []*CollectionUsage `json:"collections,omitempty"`

	// List of backups and their metrics
	Backups []*BackupUsage `json:"backups,omitempty"`

	// CollectingTIme The time of the collection of the metric
	CollectingTIme string `json:"-"`
}

// CollectionUsage represents metrics for a single collection
type CollectionUsage struct {
	// The name of the collection
	Name string `json:"name,omitempty"`

	// The replication factor of the collection
	ReplicationFactor int `json:"replication_factor,omitempty"`

	// The number of unique shards in the collection
	UniqueShardCount int `json:"unique_shard_count,omitempty"`

	// List of shards and their metrics
	Shards []*ShardUsage `json:"shards,omitempty"`
}

// ShardUsage represents metrics for a single shard
type ShardUsage struct {
	// The name of the shard
	Name string `json:"name,omitempty"`

	// The status of the shard (ACTIVE, INACTIVE)
	Status string `json:"status,omitempty"`

	// The number of objects in the shard
	ObjectsCount int64 `json:"objects_count,omitempty"`

	// The storage size in bytes
	ObjectsStorageBytes uint64 `json:"objects_storage_bytes,omitempty"`

	// The actual memory storage bytes used by vectors
	VectorStorageBytes uint64 `json:"vector_storage_bytes,omitempty"`

	// List of named vectors and their metrics
	NamedVectors []*VectorUsage `json:"named_vectors,omitempty"`
}

// VectorUsage represents metrics for a single vector index
type VectorUsage struct {
	// The name of the vector
	Name string `json:"name,omitempty"`

	// The type of vector index (for dynamic indexes, this shows the underlying type: flat/hnsw)
	VectorIndexType string `json:"vector_index_type,omitempty"`

	// Indicates if this index originated from a dynamic index configuration
	IsDynamic bool `json:"is_dynamic,omitempty"`

	// The compression type used
	Compression string `json:"compression,omitempty"`

	// The compression ratio achieved
	VectorCompressionRatio float64 `json:"vector_compression_ratio,omitempty"`

	// The bits parameter for RQ compression (only set when Compression="rq")
	Bits int16 `json:"bits,omitempty"`

	// List of dimensionalities and their metrics
	Dimensionalities []*Dimensionality `json:"dimensionalities,omitempty"`
}

// Dimensionality represents metrics for a specific dimensionality
type Dimensionality struct {
	// The dimensionality of the vectors
	Dimensions int `json:"dimensionality,omitempty"`

	// The number of objects with this dimensionality
	Count int `json:"count,omitempty"`
}

// BackupUsage represents metrics for a single backup
type BackupUsage struct {
	// The ID of the backup
	ID string `json:"id,omitempty"`

	// The completion time of the backup
	CompletionTime string `json:"completion_time,omitempty"`

	// The size of the backup in GiB
	SizeInGib float64 `json:"size_in_gib,omitempty"`

	// The type of backup
	Type string `json:"type,omitempty"`

	// The list of collections included in the backup
	Collections []string `json:"collections,omitempty"`
}

type ObjectUsage struct {
	Count        int64
	StorageBytes int64
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io

package models

// UsageResponse represents the response from the metrics endpoint
type UsageResponse struct {
	// The name of the node
	Node string `json:"node"`

	// List of single tenant collections and their metrics
	SingleTenantCollections []*CollectionUsage `json:"singleTenantCollections"`

	// List of backups and their metrics
	Backups []*BackupUsage `json:"backups"`
}

// CollectionUsage represents metrics for a single collection
type CollectionUsage struct {
	// The name of the collection
	Name string `json:"name"`

	// The replication factor of the collection
	ReplicationFactor int64 `json:"replicationFactor"`

	// The number of unique shards in the collection
	UniqueShardCount int64 `json:"uniqueShardCount"`

	// List of shards and their metrics
	Shards []*ShardUsage `json:"shards"`
}

// ShardUsage represents metrics for a single shard
type ShardUsage struct {
	// The name of the shard
	Name string `json:"name"`

	// The number of objects in the shard
	ObjectsCount int64 `json:"objectsCount"`

	// The storage size in bytes
	ObjectsStorageBytes int64 `json:"objectsStorageBytes"`

	// List of named vectors and their metrics
	NamedVectors []*VectorUsage `json:"namedVectors"`
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
	VectorCompressionRatio float64 `json:"vectorCompressionRatio"`

	// List of dimensionalities and their metrics
	Dimensionalities []*DimensionalityUsage `json:"dimensionalities"`
}

// DimensionalityUsage represents metrics for a specific dimensionality
type DimensionalityUsage struct {
	// The dimensionality of the vectors
	Dimensionality int64 `json:"dimensionality"`

	// The number of objects with this dimensionality
	Count int64 `json:"count"`
}

// BackupUsage represents metrics for a single backup
type BackupUsage struct {
	// The ID of the backup
	ID string `json:"id"`

	// The completion time of the backup
	CompletionTime string `json:"completionTime"`

	// The size of the backup in GiB
	SizeInGib float64 `json:"sizeInGib"`

	// The type of backup
	Type string `json:"type"`

	// The list of collections included in the backup
	Collections []string `json:"collections"`
}

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
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

// VectorIndex is anything that indexes vectors efficiently. For an example
// look at ./vector/hnsw/index.go
type VectorIndex interface {
	Dump(labels ...string)
	Add(ctx context.Context, id uint64, vector []float32) error
	AddMulti(ctx context.Context, docId uint64, vector [][]float32) error
	AddBatch(ctx context.Context, ids []uint64, vector [][]float32) error
	AddMultiBatch(ctx context.Context, docIds []uint64, vectors [][][]float32) error
	Delete(id ...uint64) error
	DeleteMulti(id ...uint64) error
	SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
	SearchByVectorDistance(ctx context.Context, vector []float32, dist float32,
		maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	SearchByMultiVector(ctx context.Context, vector [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
	SearchByMultiVectorDistance(ctx context.Context, vector [][]float32, dist float32,
		maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error
	GetKeys(id uint64) (uint64, uint64, error)
	Drop(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Flush() error
	SwitchCommitLogs(ctx context.Context) error
	ListFiles(ctx context.Context, basePath string) ([]string, error)
	PostStartup()
	Compressed() bool
	Multivector() bool
	ValidateBeforeInsert(vector []float32) error
	ValidateMultiBeforeInsert(vector [][]float32) error
	DistanceBetweenVectors(x, y []float32) (float32, error)
	// ContainsDoc returns true if the index has indexed document with a given id.
	// It must return false if the document does not exist, or has a tombstone.
	ContainsDoc(docID uint64) bool
	AlreadyIndexed() uint64
	// Iterate over all indexed document ids in the index.
	// Consistency or order is not guaranteed, as the index may be concurrently modified.
	// If the callback returns false, the iteration will stop.
	Iterate(fn func(docID uint64) bool)
	DistancerProvider() distancer.Provider
	QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer
	QueryMultiVectorDistancer(queryVector [][]float32) common.QueryVectorDistancer
	Stats() (common.IndexStats, error)
}

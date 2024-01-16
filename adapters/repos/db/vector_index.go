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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/schema"
)

// VectorIndex is anything that indexes vectors efficiently. For an example
// look at ./vector/hnsw/index.go
type VectorIndex interface {
	Dump(labels ...string)
	Add(id uint64, vector []float32) error
	AddBatch(ctx context.Context, id []uint64, vector [][]float32) error
	Delete(id ...uint64) error
	SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error)
	SearchByVectorDistance(vector []float32, dist float32,
		maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
	UpdateUserConfig(updated schema.VectorIndexConfig, callback func()) error
	Drop(ctx context.Context) error
	Shutdown(ctx context.Context) error
	Flush() error
	SwitchCommitLogs(ctx context.Context) error
	ListFiles(ctx context.Context, basePath string) ([]string, error)
	PostStartup()
	Compressed() bool
	ValidateBeforeInsert(vector []float32) error
	DistanceBetweenVectors(x, y []float32) (float32, bool, error)
	ContainsNode(id uint64) bool
	DistancerProvider() distancer.Provider
}

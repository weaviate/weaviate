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

//go:build !cuvs

package cuvs_index

import (
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

type cuvs_index struct{}

func (index *cuvs_index) Dump(labels ...string) {
}

func (index *cuvs_index) Add(ctx context.Context, id uint64, vector []float32) error {
	return nil
}

func (index *cuvs_index) AddMulti(ctx context.Context, docId uint64, vector [][]float32) error {
	return nil
}

func (index *cuvs_index) AddBatch(ctx context.Context, ids []uint64, vector [][]float32) error {
	return nil
}

func (index *cuvs_index) AddMultiBatch(ctx context.Context, docIds []uint64, vectors [][][]float32) error {
	return nil
}

func (index *cuvs_index) Delete(id ...uint64) error {
	return nil
}

func (index *cuvs_index) DeleteMulti(id ...uint64) error {
	return nil
}

func (index *cuvs_index) SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, nil
}

func (index *cuvs_index) SearchByVectorDistance(ctx context.Context, vector []float32, dist float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, nil
}

func (index *cuvs_index) SearchByMultiVector(ctx context.Context, vector [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, nil
}

func (index *cuvs_index) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	return nil
}

func (index *cuvs_index) GetKeys(id uint64) (uint64, uint64, error) {
	return 0, 0, nil
}

func (index *cuvs_index) Drop(ctx context.Context) error {
	return nil
}

func (index *cuvs_index) Shutdown(ctx context.Context) error {
	return nil
}

func (index *cuvs_index) Flush() error {
	return nil
}

func (index *cuvs_index) SwitchCommitLogs(ctx context.Context) error {
	return nil
}

func (index *cuvs_index) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return nil, nil
}

func (index *cuvs_index) PostStartup() {
}

func (index *cuvs_index) Compressed() bool {
	return false
}

func (index *cuvs_index) Multivector() bool {
	return false
}

func (index *cuvs_index) ValidateBeforeInsert(vector []float32) error {
	return nil
}

func (index *cuvs_index) ValidateMultiBeforeInsert(vector [][]float32) error {
	return nil
}

func (index *cuvs_index) DistanceBetweenVectors(x, y []float32) (float32, error) {
	return 0, nil
}

func (index *cuvs_index) ContainsNode(id uint64) bool {
	return false
}

func (index *cuvs_index) AlreadyIndexed() uint64 {
	return 0
}

func (index *cuvs_index) Iterate(fn func(id uint64) bool) {
}

func (index *cuvs_index) DistancerProvider() distancer.Provider {
	return nil
}

func (index *cuvs_index) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	return common.QueryVectorDistancer{}
}

func (index *cuvs_index) Stats() (common.IndexStats, error) {
	return nil, nil
}

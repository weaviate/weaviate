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

package ivfpq

import (
	"context"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

const (
	temporalVectorsBufferSize = 100_000
)

type ifvpq struct {
	distancer       distancer.Provider
	temporalVectors [][]float32
	indexed         atomic.Int32
	compressed      atomic.Bool
}

func New(distancer distancer.Provider) *ifvpq {
	return &ifvpq{
		distancer:       distancer,
		temporalVectors: make([][]float32, temporalVectorsBufferSize+10_000),
	}
}

func (index *ifvpq) Compressed() bool {
	return index.compressed.Load()
}

func (index *ifvpq) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ids) != len(vectors) {
		return errors.Errorf("ids and vectors sizes does not match")
	}
	if len(ids) == 0 {
		return errors.Errorf("insertBatch called with empty lists")
	}
	for i := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := index.Add(ctx, ids[i], vectors[i]); err != nil {
			return err
		}
	}
	return nil
}

func (index *ifvpq) Add(ctx context.Context, id uint64, vector []float32) error {
	if index.indexed.Load() < temporalVectorsBufferSize {
		index.temporalVectors[id] = vector
	}
	return nil
}

func (index *ifvpq) Delete(ids ...uint64) error {
	return nil
}

func (index *ifvpq) SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, nil
}

func (index *ifvpq) SearchByVectorDistance(ctx context.Context, vector []float32,
	targetDistance float32, maxLimit int64, allow helpers.AllowList,
) ([]uint64, []float32, error) {
	return nil, nil, nil
}

func (index *ifvpq) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	callback()
	return nil
}

func (index *ifvpq) Drop(ctx context.Context) error {
	return nil
}

func (index *ifvpq) Flush() error {
	return nil
}

func (index *ifvpq) Shutdown(ctx context.Context) error {
	return nil
}

func (index *ifvpq) SwitchCommitLogs(context.Context) error {
	return nil
}

func (index *ifvpq) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return nil, nil
}

func (i *ifvpq) ValidateBeforeInsert(vector []float32) error {
	return nil
}

func (index *ifvpq) PostStartup() {

}

func (index *ifvpq) Dump(labels ...string) {

}

func (index *ifvpq) DistanceBetweenVectors(x, y []float32) (float32, error) {
	return 0, nil
}

func (index *ifvpq) ContainsNode(id uint64) bool {
	return true
}

func (index *ifvpq) Iterate(fn func(id uint64) bool) {

}

func (index *ifvpq) DistancerProvider() distancer.Provider {
	return index.distancer
}

func (index *ifvpq) AlreadyIndexed() uint64 {
	return 0
}

func (index *ifvpq) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	return common.QueryVectorDistancer{}
}

func (index *ifvpq) Stats() (common.IndexStats, error) {
	return nil, nil
}

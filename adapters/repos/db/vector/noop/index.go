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

package noop

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	hnswconf "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type Index struct{}

func NewIndex() *Index {
	return &Index{}
}

func (i *Index) AddBatch(ctx context.Context, id []uint64, vector [][]float32) error {
	// silently ignore
	return nil
}

func (i *Index) AddMultiBatch(ctx context.Context, docIds []uint64, vectors [][][]float32) error {
	// silently ignore
	return nil
}

func (i *Index) Add(ctx context.Context, id uint64, vector []float32) error {
	// silently ignore
	return nil
}

func (i *Index) AddMulti(ctx context.Context, docId uint64, vector [][]float32) error {
	// silently ignore
	return nil
}

func (i *Index) Delete(id ...uint64) error {
	// silently ignore
	return nil
}

func (i *Index) DeleteMulti(id ...uint64) error {
	// silently ignore
	return nil
}

func (i *Index) SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
}

func (i *Index) SearchByMultiVector(ctx context.Context, vector [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
}

func (i *Index) SearchByVectorDistance(ctx context.Context, vector []float32, dist float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
}

func (i *Index) SearchByMultiVectorDistance(ctx context.Context, vector [][]float32, dist float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, errors.Errorf("cannot multi-vector-search on a class not vector-indexed")
}

func (i *Index) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	callback()
	switch t := updated.(type) {
	case hnswconf.UserConfig:
		// the fact that we are in the noop index means that 'skip' must have been
		// set to true before, so changing it now is not possible. But if it
		// stays, we don't mind.
		if t.Skip {
			return nil
		}
		return errors.Errorf("cannot update vector index config on a non-indexed class. Delete and re-create without skip property")

	default:
		return fmt.Errorf("unrecognized vector index config: %T", updated)

	}
}

func (i *Index) GetKeys(id uint64) (uint64, uint64, error) {
	return 0, 0, errors.Errorf("cannot get keys from a class not vector-indexed")
}

func (i *Index) Drop(context.Context) error {
	// silently ignore
	return nil
}

func (i *Index) Flush() error {
	return nil
}

func (i *Index) Shutdown(context.Context) error {
	return nil
}

func (i *Index) SwitchCommitLogs(context.Context) error {
	return nil
}

func (i *Index) ListFiles(context.Context, string) ([]string, error) {
	return nil, nil
}

func (i *Index) ValidateBeforeInsert(vector []float32) error {
	return nil
}

func (i *Index) ValidateMultiBeforeInsert(vector [][]float32) error {
	return nil
}

func (i *Index) PostStartup() {
}

func (i *Index) Dump(labels ...string) {
}

func (i *Index) DistanceBetweenVectors(x, y []float32) (float32, error) {
	return 0, nil
}

func (i *Index) ContainsDoc(docID uint64) bool {
	return false
}

func (i *Index) Iterate(fn func(id uint64) bool) {}

func (i *Index) DistancerProvider() distancer.Provider {
	return nil
}

func (i *Index) ShouldCompress() (bool, int) {
	return false, 0
}

func (i *Index) ShouldCompressFromConfig(config schemaConfig.VectorIndexConfig) (bool, int) {
	return false, 0
}

func (i *Index) Compressed() bool {
	return false
}

func (i *Index) Multivector() bool {
	return false
}

func (i *Index) AlreadyIndexed() uint64 {
	return 0
}

func (i *Index) TurnOnCompression(callback func()) error {
	return nil
}

func (i *Index) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	return common.QueryVectorDistancer{}
}

func (i *Index) QueryMultiVectorDistancer(queryVector [][]float32) common.QueryVectorDistancer {
	return common.QueryVectorDistancer{}
}

func (i *Index) Stats() (common.IndexStats, error) {
	return &NoopStats{}, errors.New("Stats() is not implemented for noop index")
}

type NoopStats struct{}

func (s *NoopStats) IndexType() common.IndexType {
	return common.IndexTypeNoop
}

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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type Index struct{}

func NewIndex() *Index {
	return &Index{}
}

func (i *Index) AddBatch(ctx context.Context, id []uint64, vector [][]float32) error {
	// silently ignore
	return nil
}

func (i *Index) Add(id uint64, vector []float32) error {
	// silently ignore
	return nil
}

func (i *Index) Delete(id ...uint64) error {
	// silently ignore
	return nil
}

func (i *Index) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
}

func (i *Index) SearchByVectorDistance(vector []float32, dist float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, errors.Errorf("cannot vector-search on a class not vector-indexed")
}

func (i *Index) UpdateUserConfig(updated schema.VectorIndexConfig, callback func()) error {
	callback()
	switch t := updated.(type) {
	case hnsw.UserConfig:
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

func (i *Index) PostStartup() {
}

func (i *Index) Dump(labels ...string) {
}

func (i *Index) DistanceBetweenVectors(x, y []float32) (float32, bool, error) {
	return 0, true, nil
}

func (i *Index) ContainsNode(id uint64) bool {
	return false
}

func (i *Index) DistancerProvider() distancer.Provider {
	return nil
}

func (i *Index) ShouldCompress() (bool, int) {
	return false, 0
}

func (i *Index) ShouldCompressFromConfig(config schema.VectorIndexConfig) (bool, int) {
	return false, 0
}

func (i *Index) Compressed() bool {
	return false
}

func (i *Index) AlreadyIndexed() uint64 {
	return 0
}

func (i *Index) TurnOnCompression(callback func()) error {
	return nil
}

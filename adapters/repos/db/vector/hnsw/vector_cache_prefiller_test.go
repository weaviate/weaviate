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

package hnsw

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

func TestVectorCachePrefilling(t *testing.T) {
	cache := newFakeCache()
	index := &hnsw{
		nodes:               generateDummyVertices(100),
		currentMaximumLayer: 3,
		shardedNodeLocks:    common.NewDefaultShardedLocks(),
	}

	logger, _ := test.NewNullLogger()

	pf := newVectorCachePrefiller[float32](cache, index, logger)

	t.Run("prefill with limit >= graph size", func(t *testing.T) {
		cache.Reset()
		pf.Prefill(context.Background(), 100)
		assert.Equal(t, allNumbersUpTo(100), cache.store)
	})

	t.Run("prefill with small limit so only the upper layer fits", func(t *testing.T) {
		cache.Reset()
		pf.Prefill(context.Background(), 7)
		assert.Equal(t, map[uint64]struct{}{
			0:  {},
			15: {},
			30: {},
			45: {},
			60: {},
			75: {},
			90: {},
		}, cache.store)
	})

	t.Run("limit where a layer partially fits", func(t *testing.T) {
		cache.Reset()
		pf.Prefill(context.Background(), 10)
		assert.Equal(t, map[uint64]struct{}{
			// layer 3
			0:  {},
			15: {},
			30: {},
			45: {},
			60: {},
			75: {},
			90: {},

			// additional layer 2
			5:  {},
			10: {},
			20: {},
		}, cache.store)
	})
}

func newFakeCache() *fakeCache {
	return &fakeCache{
		store: map[uint64]struct{}{},
	}
}

type fakeCache struct {
	store map[uint64]struct{}
}

func (f *fakeCache) MultiGet(ctx context.Context, id []uint64) ([][]float32, []error) {
	panic("not implemented")
}

func (f *fakeCache) Get(ctx context.Context, id uint64) ([]float32, error) {
	f.store[id] = struct{}{}
	return nil, nil
}

func (f *fakeCache) Delete(ctx context.Context, id uint64) {
	panic("not implemented")
}

func (f *fakeCache) Preload(id uint64, vec []float32) {
	panic("not implemented")
}

func (f *fakeCache) Prefetch(id uint64) {
	panic("not implemented")
}

func (f *fakeCache) Grow(id uint64) {
	panic("not implemented")
}

func (f *fakeCache) UpdateMaxSize(size int64) {
	panic("not implemented")
}

func (f *fakeCache) All() [][]float32 {
	panic("not implemented")
}

func (f *fakeCache) Drop() {
	panic("not implemented")
}

func (f *fakeCache) CopyMaxSize() int64 {
	return 1e6
}

func (f *fakeCache) Reset() {
	f.store = map[uint64]struct{}{}
}

func (f *fakeCache) Len() int32 {
	return int32(len(f.store))
}

func (f *fakeCache) CountVectors() int64 {
	panic("not implemented")
}

func generateDummyVertices(amount int) []*vertex {
	out := make([]*vertex, amount)
	for i := range out {
		out[i] = &vertex{
			id:    uint64(i),
			level: levelForDummyVertex(i),
		}
	}

	return out
}

// maximum of 3 layers
// if id % 15 == 0 -> layer 3
// if id % 5 == 0 -> layer 2
// if id % 3 == 0 -> layer 1
// remainder -> layer 0
func levelForDummyVertex(id int) int {
	if id%15 == 0 {
		return 3
	}

	if id%5 == 0 {
		return 2
	}

	if id%3 == 0 {
		return 1
	}

	return 0
}

func allNumbersUpTo(size int) map[uint64]struct{} {
	out := map[uint64]struct{}{}
	for i := 0; i < size; i++ {
		out[uint64(i)] = struct{}{}
	}

	return out
}

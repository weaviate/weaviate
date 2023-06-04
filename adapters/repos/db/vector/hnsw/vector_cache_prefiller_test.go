//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestVectorCachePrefilling(t *testing.T) {
	cache := newFakeCache()
	index := &hnsw{}

	logger, _ := test.NewNullLogger()

	pf := newVectorCachePrefiller[float32](cache, index, logger, fakeIteratorProvider())

	t.Run("prefill with limit >= graph size", func(t *testing.T) {
		cache.reset()
		pf.Prefill(context.Background(), 100)
		assert.Equal(t, allNumbersUpTo(100), cache.store)
	})

	pf = newVectorCachePrefiller[float32](cache, index, logger, fakeIteratorProvider())

	t.Run("prefill with limit < graph size", func(t *testing.T) {
		cache.reset()
		pf.Prefill(context.Background(), 50)
		assert.Equal(t, allNumbersUpTo(50), cache.store)
	})
}

func fakeIteratorProvider() VectorIterator[float32] {
	return &fakeIterator{limit: 100}
}

type fakeIterator struct {
	it    uint64
	limit uint64
}

func (ip *fakeIterator) Close() {}
func (ip *fakeIterator) Next() ([]float32, uint64, error) {
	if ip.it == ip.limit {
		return nil, 0, nil
	}

	id := ip.it
	ip.it++

	return []float32{1, 2, 3}, id, nil
}

func newFakeCache() *fakeCache {
	return &fakeCache{
		store: map[uint64]struct{}{},
	}
}

//nolint:unused
func (f *fakeCache) all() [][]float32 {
	return nil
}

type fakeCache struct {
	store map[uint64]struct{}
}

//nolint:unused
func (f *fakeCache) get(ctx context.Context, id uint64) ([]float32, error) {
	panic("not implemented")
}

//nolint:unused
func (f *fakeCache) delete(ctx context.Context, id uint64) {
	panic("not implemented")
}

//nolint:unused
func (f *fakeCache) preload(id uint64, vec []float32) {
	panic("not implemented")
}

//nolint:unused
func (f *fakeCache) load(id uint64, vec []float32) {
	f.store[id] = struct{}{}
}

//nolint:unused
func (f *fakeCache) prefetch(id uint64) {
	panic("not implemented")
}

//nolint:unused
func (f *fakeCache) grow(id uint64) {
	panic("not implemented")
}

//nolint:unused
func (f *fakeCache) updateMaxSize(size int64) {
	panic("not implemented")
}

//nolint:unused
func (f *fakeCache) drop() {
	panic("not implemented")
}

//nolint:unused
func (f *fakeCache) copyMaxSize() int64 {
	return 1e6
}

func (f *fakeCache) reset() {
	f.store = map[uint64]struct{}{}
}

//nolint:unused
func (f *fakeCache) len() int32 {
	return int32(len(f.store))
}

//nolint:unused
func (f *fakeCache) countVectors() int64 {
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

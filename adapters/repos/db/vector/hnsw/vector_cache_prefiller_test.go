package hnsw

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestVectorCachePrefilling(t *testing.T) {
	cache := newFakeCache()
	index := &hnsw{
		nodes:               generateDummyVertices(100),
		currentMaximumLayer: 3,
	}

	logger, _ := test.NewNullLogger()

	pf := newVectorCachePrefiller(cache, index, logger)

	t.Run("prefill with limit >= graph size", func(t *testing.T) {
		cache.reset()
		pf.Prefill(context.Background(), 100)
		assert.Equal(t, allNumbersUpTo(100), cache.store)
	})

	t.Run("prefill with small limit so only the upper layer fits", func(t *testing.T) {
		cache.reset()
		pf.Prefill(context.Background(), 7)
		assert.Equal(t, map[uint64]struct{}{
			0:  struct{}{},
			15: struct{}{},
			30: struct{}{},
			45: struct{}{},
			60: struct{}{},
			75: struct{}{},
			90: struct{}{},
		}, cache.store)
	})

	t.Run("limit where a layer partially fits", func(t *testing.T) {
		cache.reset()
		pf.Prefill(context.Background(), 10)
		assert.Equal(t, map[uint64]struct{}{
			// layer 3
			0:  struct{}{},
			15: struct{}{},
			30: struct{}{},
			45: struct{}{},
			60: struct{}{},
			75: struct{}{},
			90: struct{}{},

			// additional layer 2
			5:  struct{}{},
			10: struct{}{},
			20: struct{}{},
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

func (f *fakeCache) get(ctx context.Context, id uint64) ([]float32, error) {
	f.store[id] = struct{}{}
	return nil, nil
}

func (f *fakeCache) reset() {
	f.store = map[uint64]struct{}{}
}

func (f *fakeCache) len() int32 {
	return int32(len(f.store))
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

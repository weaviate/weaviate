package hnsw

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

type vectorCache struct {
	cache         sync.Map
	count         int32
	maxSize       int
	getFromSource vectorForID
}

func newCache(getFromSource vectorForID) *vectorCache {
	return &vectorCache{
		cache:         sync.Map{},
		count:         0,
		maxSize:       10000, // TODO: make configurable
		getFromSource: getFromSource,
	}

}

func (c *vectorCache) get(ctx context.Context, id int32) ([]float32, error) {
	// before := time.Now()
	vec, ok := c.cache.Load(id)
	// m.addCacheReadLocking(before)
	if !ok {
		vec, err := c.getFromSource(ctx, id)
		if err != nil {
			return nil, errors.Wrapf(err, "fill cache with id %d", id)
		}

		if c.count >= int32(c.maxSize) {
			c.cache.Range(func(key, value interface{}) bool {
				c.cache.Delete(key)
				atomic.AddInt32(&c.count, -1)

				return true
			})
		}

		c.cache.Store(id, vec)
		atomic.AddInt32(&c.count, 1)
		return vec, nil
	}

	return vec.([]float32), nil
}

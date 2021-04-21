//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type unlimitedCache struct {
	cache       [][]float32
	vectorForID VectorForID
}

func newUnlimitedCache(vecForID VectorForID, maxSize int,
	logger logrus.FieldLogger) *unlimitedCache {
	return &unlimitedCache{
		vectorForID: vecForID,
		cache:       make([][]float32, 1e6), // TODO: grow
	}
}

func (n *unlimitedCache) get(ctx context.Context, id uint64) ([]float32, error) {
	return n.cache[id], nil
}

func (n *unlimitedCache) preload(id uint64, vec []float32) {
	n.cache[id] = vec
}

func (n *unlimitedCache) len() int32 {
	return 0
}

type noopCache struct {
	vectorForID VectorForID
}

func newNoopCache(vecForID VectorForID, maxSize int,
	logger logrus.FieldLogger) *noopCache {
	return &noopCache{vectorForID: vecForID}
}

func (n *noopCache) get(ctx context.Context, id uint64) ([]float32, error) {
	return n.vectorForID(ctx, id)
}

func (n *noopCache) len() int32 {
	return 0
}

type vectorCache struct {
	cache         sync.Map
	count         int32
	maxSize       int
	getFromSource VectorForID
	logger        logrus.FieldLogger
	cancel        chan bool
	sync.RWMutex
}

func newCache(getFromSource VectorForID, maxSize int,
	logger logrus.FieldLogger) *vectorCache {
	vc := &vectorCache{
		cache:         sync.Map{},
		count:         0,
		maxSize:       maxSize,
		getFromSource: getFromSource,
		cancel:        make(chan bool),
		logger:        logger,
	}

	vc.watchForDeletion()
	return vc
}

func (c *vectorCache) watchForDeletion() {
	go func() {
		t := time.Tick(10 * time.Second)
		for {
			select {
			case <-c.cancel:
				return
			case <-t:
				c.replaceMapIfFull()
			}
		}
	}()
}

func (c *vectorCache) replaceMapIfFull() {
	if atomic.LoadInt32(&c.count) >= int32(c.maxSize) {
		c.Lock()
		c.logger.WithField("action", "hnsw_delete_vector_cache").
			Debug("deleting full vector cache")
		c.cache = sync.Map{}
		atomic.StoreInt32(&c.count, 0)
		c.Unlock()
	}
}

func (c *vectorCache) get(ctx context.Context, id uint64) ([]float32, error) {
	c.RLock()
	vec, ok := c.cache.Load(id)
	c.RUnlock()
	if !ok {
		vec, err := c.getFromSource(ctx, id)
		if err != nil {
			return nil, errors.Wrapf(err, "fill cache with id %d", id)
		}

		c.RLock()
		c.cache.Store(id, vec)
		c.RUnlock()
		atomic.AddInt32(&c.count, 1)
		return vec, nil
	}

	return vec.([]float32), nil
}

func (c *vectorCache) drop() {
	c.cancel <- true
}

func (c *vectorCache) len() int32 {
	return atomic.LoadInt32(&c.count)
}

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
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

type vectorCache struct {
	cache         sync.Map
	count         int32
	maxSize       int
	getFromSource VectorForID
	logger        logrus.FieldLogger
	cancel        chan bool
	sync.RWMutex
}

func newCache(getFromSource VectorForID, logger logrus.FieldLogger) *vectorCache {
	vc := &vectorCache{
		cache:         sync.Map{},
		count:         0,
		maxSize:       50000, // TODO: make configurable
		getFromSource: getFromSource,
		cancel:        make(chan bool),
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
	if c.count >= int32(c.maxSize) {
		c.Lock()
		c.logger.WithField("action", "hnsw_delete_vector_cache").
			Debug("deleting full vector cache")
		c.cache = sync.Map{}
		atomic.StoreInt32(&c.count, 0)
		c.Unlock()
	}
}

func (c *vectorCache) get(ctx context.Context, id int64) ([]float32, error) {
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

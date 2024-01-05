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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
)

type vectorCachePrefiller[T any] struct {
	cache  cache.Cache[T]
	index  *hnsw
	logger logrus.FieldLogger
}

func newVectorCachePrefiller[T any](cache cache.Cache[T], index *hnsw,
	logger logrus.FieldLogger,
) *vectorCachePrefiller[T] {
	return &vectorCachePrefiller[T]{
		cache:  cache,
		index:  index,
		logger: logger,
	}
}

func (pf *vectorCachePrefiller[T]) Prefill(ctx context.Context, limit int) error {
	before := time.Now()
	for level := pf.maxLevel(); level >= 0; level-- {
		ok, err := pf.prefillLevel(ctx, level, limit)
		if err != nil {
			return err
		}

		if !ok {
			break
		}
	}

	pf.logTotal(int(pf.cache.Len()), limit, before)
	return nil
}

// returns false if the max has been reached, true otherwise
func (pf *vectorCachePrefiller[T]) prefillLevel(ctx context.Context,
	level, limit int,
) (bool, error) {
	// TODO: this makes zero sense, just copy the lists, don't actually block
	//  !!!!

	before := time.Now()
	layerCount := 0

	pf.index.Lock()
	nodesLen := len(pf.index.nodes)
	pf.index.Unlock()

	for i := 0; i < nodesLen; i++ {
		if int(pf.cache.Len()) >= limit {
			break
		}

		if err := ctx.Err(); err != nil {
			return false, err
		}

		pf.index.shardedNodeLocks.RLock(uint64(i))
		node := pf.index.nodes[i]
		pf.index.shardedNodeLocks.RUnlock(uint64(i))

		if node == nil {
			continue
		}

		if levelOfNode(node) != level {
			continue
		}

		// we are not really interested in the result, we just want to populate the
		// cache
		pf.index.Lock()
		pf.cache.Get(ctx, uint64(i))
		layerCount++
		pf.index.Unlock()
	}

	pf.logLevel(level, layerCount, before)
	return true, nil
}

func (pf *vectorCachePrefiller[T]) logLevel(level, count int, before time.Time) {
	pf.logger.WithFields(logrus.Fields{
		"action":     "hnsw_vector_cache_prefill_level",
		"hnsw_level": level,
		"count":      count,
		"took":       time.Since(before),
		"index_id":   pf.index.id,
	}).Debug("prefilled level in vector cache")
}

func (pf *vectorCachePrefiller[T]) logTotal(count, limit int, before time.Time) {
	pf.logger.WithFields(logrus.Fields{
		"action":   "hnsw_vector_cache_prefill",
		"limit":    limit,
		"count":    count,
		"took":     time.Since(before),
		"index_id": pf.index.id,
	}).Info("prefilled vector cache")
}

func levelOfNode(node *vertex) int {
	node.Lock()
	defer node.Unlock()

	return node.level
}

func (pf *vectorCachePrefiller[T]) maxLevel() int {
	pf.index.Lock()
	defer pf.index.Unlock()

	return pf.index.currentMaximumLayer
}

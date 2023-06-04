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
	"time"

	"github.com/sirupsen/logrus"
)

type vectorCachePrefiller[T any] struct {
	cache    cache[T]
	index    *hnsw
	logger   logrus.FieldLogger
	iterator VectorIterator[T]
}

type cache[T any] interface {
	get(ctx context.Context, id uint64) ([]T, error)
	len() int32
	countVectors() int64
	delete(ctx context.Context, id uint64)
	preload(id uint64, vec []T)
	load(id uint64, vec []T)
	prefetch(id uint64)
	grow(size uint64)
	drop()
	updateMaxSize(size int64)
	copyMaxSize() int64
	all() [][]T
}

func newVectorCachePrefiller[T any](cache cache[T], index *hnsw,
	logger logrus.FieldLogger, iterator VectorIterator[T],
) *vectorCachePrefiller[T] {
	return &vectorCachePrefiller[T]{
		cache:    cache,
		index:    index,
		logger:   logger,
		iterator: iterator,
	}
}

func (pf *vectorCachePrefiller[T]) Prefill(ctx context.Context, limit int) error {
	before := time.Now()

	i := 0
	for v, id, err := pf.iterator.Next(); v != nil; v, id, err = pf.iterator.Next() {
		if err != nil {
			return err
		}

		if i >= limit {
			break
		}

		pf.cache.load(id, v)
		i++
	}

	pf.logTotal(int(pf.cache.len()), limit, before)
	return nil
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

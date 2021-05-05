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
	"time"

	"github.com/sirupsen/logrus"
)

type vectorCachePrefiller struct {
	cache  cache
	index  *hnsw
	logger logrus.FieldLogger
}

type cache interface {
	get(ctx context.Context, id uint64) ([]float32, error)
	len() int32
	preload(id uint64, vec []float32)
	prefetch(id uint64)
	grow(size uint64)
	drop()
	updateMaxSize(size int64)
}

func newVectorCachePrefiller(cache cache, index *hnsw,
	logger logrus.FieldLogger) *vectorCachePrefiller {
	return &vectorCachePrefiller{
		cache:  cache,
		index:  index,
		logger: logger,
	}
}

func (pf *vectorCachePrefiller) Prefill(ctx context.Context, limit int) error {
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

	pf.logTotal(int(pf.cache.len()), limit, before)
	return nil
}

// returns false if the max has been reached, true otherwise
func (pf *vectorCachePrefiller) prefillLevel(ctx context.Context,
	level, limit int) (bool, error) {
	// TODO: this makes zero sense, just copy the lists, don't actually block
	//  !!!!
	pf.index.Lock()
	defer pf.index.Unlock()

	before := time.Now()
	layerCount := 0
	for i, node := range pf.index.nodes {
		if int(pf.cache.len()) >= limit {
			break
		}

		if err := ctx.Err(); err != nil {
			return false, err
		}

		if node == nil {
			continue
		}

		if levelOfNode(node) != level {
			continue
		}

		// we are not really interested in the result, we just want to populate the
		// cache
		pf.cache.get(ctx, uint64(i))
		layerCount++
	}

	pf.logLevel(level, layerCount, before)
	return true, nil
}

func (pf *vectorCachePrefiller) logLevel(level, count int, before time.Time) {
	pf.logger.WithFields(logrus.Fields{
		"action":     "hnsw_vector_cache_prefill_level",
		"hnsw_level": level,
		"count":      count,
		"took":       time.Since(before),
		"index_id":   pf.index.id,
	}).Debug("prefilled level in vector cache")
}

func (pf *vectorCachePrefiller) logTotal(count, limit int, before time.Time) {
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

func (pf *vectorCachePrefiller) maxLevel() int {
	pf.index.Lock()
	defer pf.index.Unlock()

	return pf.index.currentMaximumLayer
}

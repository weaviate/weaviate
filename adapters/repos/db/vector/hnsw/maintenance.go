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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

// growIndexToAccomodateNode is a wrapper around the growIndexToAccomodateNode
// function growing the index of the hnsw struct. It does not do any locking on
// its own, make sure that this function is called from a single-thread or
// locked situation
func (h *hnsw) growIndexToAccomodateNode(id uint64, logger logrus.FieldLogger) error {
	size := h.nodes.Len()
	defer func() {
		h.metrics.SetSize(size)
	}()

	before := time.Now()

	// check whether h.nodes slice needs growing
	// not to unnecessarily lock h.shardedNodeLocks
	if id < uint64(size) {
		return nil
	}

	previousSize, newSize := h.nodes.Grow(uint64(id))

	took := time.Since(before)
	logger.WithField("action", "hnsw_grow_index").
		WithField("took", took).
		WithField("previous_size", previousSize).
		WithField("new_size", newSize).
		Debugf("index grown from %d to %d, took %s\n", previousSize, newSize, took)

	defer h.metrics.GrowDuration(before)

	if h.compressed.Load() {
		h.compressor.GrowCache(uint64(newSize))
	} else {
		h.cache.Grow(uint64(newSize))
	}

	h.pools.visitedListsLock.Lock()
	h.pools.visitedLists.Destroy()
	h.pools.visitedLists = nil
	h.pools.visitedLists = visited.NewPool(1, newSize+512, h.visitedListPoolMaxSize)
	h.pools.visitedListsLock.Unlock()

	return nil
}

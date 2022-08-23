//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/sirupsen/logrus"
)

const (
	initialSize             = 25000
	minimumIndexGrowthDelta = 25000
	indexGrowthRate         = 1.25
)

// growIndexToAccomodateNode is a wrapper around the growIndexToAccomodateNode
// function growing the index of the hnsw struct. It does not do any locking on
// its own, make sure that this function is called from a single-thread or
// locked situation
func (h *hnsw) growIndexToAccomodateNode(id uint64, logger logrus.FieldLogger) error {
	before := time.Now()
	newIndex, changed, err := growIndexToAccomodateNode(h.nodes, id, logger)
	if err != nil {
		return err
	}

	h.metrics.SetSize(len(h.nodes))

	if !changed {
		return nil
	}

	defer h.metrics.GrowDuration(before)

	h.cache.grow(uint64(len(newIndex)))

	h.pools.visitedListsLock.Lock()
	h.pools.visitedLists.Destroy()
	h.pools.visitedLists = nil
	h.pools.visitedLists = visited.NewPool(1, len(newIndex)+512)
	h.pools.visitedListsLock.Unlock()

	h.nodes = newIndex

	return nil
}

// growIndexToAccomodateNode does not lock the graph for writes as the
// assumption is that it is called as part of an operation that is already
// wrapped inside a lock, such as inserting a node into the graph. If
// growIndexToAccomodateNode is ever called outside of such an operation, the
// caller must make sure to lock the graph as concurrent reads/write would
// otherwise be possible
func growIndexToAccomodateNode(index []*vertex, id uint64,
	logger logrus.FieldLogger,
) ([]*vertex, bool, error) {
	before := time.Now()
	previousSize := uint64(len(index))
	if id < previousSize {
		// node will fit, nothing to do
		return nil, false, nil
	}

	var newSize uint64

	if (indexGrowthRate-1)*float64(previousSize) < float64(minimumIndexGrowthDelta) {
		// typically grow the index by the delta
		newSize = previousSize + minimumIndexGrowthDelta
	} else {
		newSize = uint64(float64(previousSize) * indexGrowthRate)
	}

	if uint64(newSize) <= id {
		// There are situations were docIDs are not in order. For example, if  the
		// default size is 10k and the default delta is 10k. Imagine the user
		// imports 21 objects, then deletes the first 20,500. When rebuilding the
		// index from disk the first id to be imported would be 20,501, however the
		// index default size and default delta would only reach up to 20,000.
		newSize = id + minimumIndexGrowthDelta
	}

	newIndex := make([]*vertex, newSize)
	copy(newIndex, index)

	took := time.Since(before)
	logger.WithField("action", "hnsw_grow_index").
		WithField("took", took).
		WithField("previous_size", previousSize).
		WithField("new_size", newSize).
		Debugf("index grown from %d to %d, took %s\n", previousSize, newSize, took)
	return newIndex, true, nil
}

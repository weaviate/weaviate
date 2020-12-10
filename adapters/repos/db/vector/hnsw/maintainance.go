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
	"time"

	"github.com/sirupsen/logrus"
)

const (
	initialSize             = 10000
	defaultIndexGrowthDelta = 10000
)

// growIndexToAccomodateNode is a wrapper around the growIndexToAccomodateNode
// function growing the index of the hnsw struct. It does not do any locking on
// its own, make sure that this function is called from a single-thread or
// locked situation
func (h *hnsw) growIndexToAccomodateNode(id uint64, logger logrus.FieldLogger) error {
	newIndex, err := growIndexToAccomodateNode(h.nodes, id, logger)
	if err != nil {
		return err
	}

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
	logger logrus.FieldLogger) ([]*vertex, error) {
	before := time.Now()
	previousSize := uint64(len(index))
	if id < previousSize {
		// node will fit, nothing to do
		return index, nil
	}

	// typically grow the index by the delta
	newSize := previousSize + defaultIndexGrowthDelta

	if uint64(newSize) < id {
		// There are situations were docIDs are not in order. For example, if  the
		// default size is 10k and the default delta is 10k. Imagine the user
		// imports 21 objects, then deletes the first 20,500. When rebuilding the
		// index from disk the first id to be imported would be 20,501, however the
		// index default size and default delta would only reach up to 20,000.
		newSize = id + defaultIndexGrowthDelta
	}

	newIndex := make([]*vertex, newSize)
	copy(newIndex, index)

	took := time.Since(before)
	logger.WithField("action", "hnsw_grow_index").
		WithField("took", took).
		WithField("previous_size", previousSize).
		WithField("new_size", newSize).
		Debugf("index grown from %d to %d, took %s\n", previousSize, newSize, took)
	return newIndex, nil
}

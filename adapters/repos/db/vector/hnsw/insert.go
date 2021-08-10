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
	"fmt"
	"math"
	"math/rand"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
)

func (h *hnsw) Add(id uint64, vector []float32) error {
	if len(vector) == 0 {
		return fmt.Errorf("insert called with nil-vector")
	}

	node := &vertex{
		id: id,
	}

	if h.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		vector = distancer.Normalize(vector)
	}

	return h.insert(node, vector)
}

func (h *hnsw) insertInitialElement(node *vertex, nodeVec []float32) error {
	h.Lock()
	defer h.Unlock()

	if err := h.commitLog.SetEntryPointWithMaxLayer(node.id, 0); err != nil {
		return err
	}

	h.entryPointID = node.id
	h.currentMaximumLayer = 0
	node.connections = map[int][]uint64{}
	node.level = 0
	if err := h.commitLog.AddNode(node); err != nil {
		return err
	}

	h.nodes[node.id] = node

	// go h.insertHook(node.id, 0, node.connections)
	return nil
}

func (h *hnsw) insert(node *vertex, nodeVec []float32) error {
	wasFirst := false
	var firstInsertError error
	h.initialInsertOnce.Do(func() {
		if h.isEmpty() {
			wasFirst = true
			firstInsertError = h.insertInitialElement(node, nodeVec)
		}
	})
	if wasFirst {
		return firstInsertError
	}

	node.markAsMaintenance()

	h.Lock()
	// initially use the "global" entrypoint which is guaranteed to be on the
	// currently highest layer
	entryPointID := h.entryPointID
	// initially use the level of the entrypoint which is the highest level of
	// the h-graph in the first iteration
	currentMaximumLayer := h.currentMaximumLayer
	h.Unlock()

	targetLevel := int(math.Floor(-math.Log(rand.Float64()) * h.levelNormalizer))

	// before = time.Now()
	// m.addBuildingItemLocking(before)
	node.level = targetLevel
	node.connections = map[int][]uint64{}

	if err := h.commitLog.AddNode(node); err != nil {
		return err
	}

	nodeId := node.id

	// before = time.Now()
	h.Lock()
	// m.addBuildingLocking(before)
	err := h.growIndexToAccomodateNode(node.id, h.logger)
	if err != nil {
		h.Unlock()
		return errors.Wrapf(err, "grow HNSW index to accommodate node %d", node.id)
	}
	h.nodes[nodeId] = node
	h.Unlock()

	// // make sure this new vec is immediately present in the cache, so we don't
	// // have to read it from disk again
	h.cache.preload(node.id, nodeVec)

	entryPointID, err = h.findBestEntrypointForNode(currentMaximumLayer, targetLevel,
		entryPointID, nodeVec)
	if err != nil {
		return errors.Wrap(err, "find best entrypoint")
	}

	if err := h.findAndConnectNeighbors(node, entryPointID, nodeVec,
		targetLevel, currentMaximumLayer, nil); err != nil {
		return errors.Wrap(err, "find and connect neighbors")
	}

	// go h.insertHook(nodeId, targetLevel, neighborsAtLevel)
	node.unmarkAsMaintenance()

	h.Lock()
	if targetLevel > h.currentMaximumLayer {
		// before = time.Now()
		// m.addBuildingLocking(before)
		if err := h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel); err != nil {
			h.Unlock()
			return err
		}

		h.entryPointID = nodeId
		h.currentMaximumLayer = targetLevel
	}
	h.Unlock()

	return nil
}

func (h *hnsw) selectNeighborsHeuristic(input *priorityqueue.Queue,
	max int, denyList helpers.AllowList) error {
	if input.Len() < max {
		return nil
	}

	closestFirst := priorityqueue.NewMin(input.Len())
	for input.Len() > 0 {
		elem := input.Pop()
		closestFirst.Insert(elem.ID, elem.Dist)
	}

	returnList := make([]*priorityqueue.Item, 0, max)

	for closestFirst.Len() > 0 && len(returnList) < max {
		curr := closestFirst.Pop()
		if denyList != nil && denyList.Contains(curr.ID) {
			continue
		}
		distToQuery := curr.Dist

		good := true
		for _, item := range returnList {

			peerDist, ok, err := h.distBetweenNodes(curr.ID, item.ID)
			if err != nil {
				return errors.Wrapf(err, "distance between %d and %d", curr.ID, item.ID)
			}

			if !ok {
				continue
			}

			if peerDist < distToQuery {
				good = false
				break
			}
		}

		if good {
			returnList = append(returnList, &curr)
		}

	}

	for _, retElem := range returnList {
		input.Insert(retElem.ID, retElem.Dist)
	}

	return nil
}

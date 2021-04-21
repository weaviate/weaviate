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
	"fmt"
	"math"
	"math/rand"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
)

func (h *hnsw) Add(id uint64, vector []float32) error {
	if len(vector) == 0 {
		return fmt.Errorf("insert called with nil-vector")
	}

	node := &vertex{
		id: id,
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
	// if err := h.commitLog.AddNode(node); err != nil {
	// 	return err
	// }

	h.nodes[node.id] = node

	// go h.insertHook(node.id, 0, node.connections)
	return nil
}

func (h *hnsw) insert(node *vertex, nodeVec []float32) error {
	// // make sure this new vec is immediately present in the cache, so we don't
	// // have to read it from disk again
	h.cache.preload(node.id, nodeVec)

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

	// node.markAsMaintenance()

	// initially use the "global" entrypoint which is guaranteed to be on the
	// currently highest layer
	entryPointID := h.entryPointID

	// initially use the level of the entrypoint which is the highest level of
	// the h-graph in the first iteration
	currentMaximumLayer := h.currentMaximumLayer

	targetLevel := int(math.Floor(-math.Log(rand.Float64()*h.levelNormalizer))) - 1

	// before = time.Now()
	// m.addBuildingItemLocking(before)
	node.level = targetLevel
	node.connections = map[int][]uint64{}

	// if err := h.commitLog.AddNode(node); err != nil {
	// 	h.Unlock()
	// 	return err
	// }

	nodeId := node.id

	// before = time.Now()
	// h.Lock()
	// m.addBuildingLocking(before)
	err := h.growIndexToAccomodateNode(node.id, h.logger)
	if err != nil {
		// h.Unlock()
		return errors.Wrapf(err, "grow HNSW index to accommodate node %d", node.id)
	}
	h.nodes[nodeId] = node
	// h.Unlock()

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
	// node.unmarkAsMaintenance()

	if targetLevel > h.currentMaximumLayer {
		// before = time.Now()
		// m.addBuildingLocking(before)
		// if err := h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel); err != nil {
		// 	h.Unlock()
		// 	return err
		// }

		h.Lock()
		h.entryPointID = nodeId
		h.currentMaximumLayer = targetLevel
		h.Unlock()
	}

	return nil
}

func (h *hnsw) selectNeighborsSimple(input *priorityqueue.Queue,
	max int, denyList helpers.AllowList) []uint64 {
	results := priorityqueue.NewMin(input.Len())
	for input.Len() > 0 {
		elem := input.Pop()
		results.Insert(elem.ID, elem.Dist)
	}

	// TODO: can we optimizie this by getting the last elem out one at a time?

	out := make([]uint64, max)
	actualSize := 0
	for results.Len() > 0 && actualSize < max {
		elem := results.Pop()
		if denyList != nil && denyList.Contains(elem.ID) {
			continue
		}

		out[actualSize] = elem.ID
		actualSize++
	}

	return out[:actualSize]
}

func (h *hnsw) selectNeighborsSimpleFromId(nodeId uint64, ids []uint64,
	max int, denyList helpers.AllowList) ([]uint64, error) {
	vec, err := h.vectorForID(context.Background(), nodeId)
	if err != nil {
		return nil, err
	}

	distancer := h.distancerProvider.New(vec)

	idQ := priorityqueue.NewMax(len(ids))
	for _, id := range ids {
		vecA, err := h.vectorForID(context.Background(), id)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID)
				continue
			} else {
				// not a typed error, we can recover from, return with err
				return nil, errors.Wrapf(err,
					"could not get vector of object at docID %d", id)
			}
		}
		dist, _, err := distancer.Distance(vecA)
		if err != nil {
			return nil, errors.Wrap(err, "select neighbors simple from id")
		}

		idQ.Insert(id, dist)
	}

	return h.selectNeighborsSimple(idQ, max, denyList), nil
}

func (h *hnsw) selectNeighborsHeuristic(input *priorityqueue.Queue,
	max int, denyList helpers.AllowList) {
	if input.Len() < max {
		return
	}

	// lenBegin := input.Len()

	// debug := []map[uint64]float32{}

	closestFirst := priorityqueue.NewMin(input.Len())
	for input.Len() > 0 {
		elem := input.Pop()
		// debug = append(debug, map[uint64]float32{elem.ID: elem.Dist})
		closestFirst.Insert(elem.ID, elem.Dist)
	}

	returnList := []priorityqueue.Item{}

	// i := 0
	for closestFirst.Len() > 0 && len(returnList) < max {
		curr := closestFirst.Pop()
		distToQuery := curr.Dist
		// fmt.Printf("round %d: cand %d with dist %f\n", i, curr.ID, curr.Dist)
		// i++

		good := true
		for _, item := range returnList {
			peerDist, ok, err := h.distBetweenNodes(curr.ID, item.ID)
			if err != nil {
				// TODO
				panic(err)
			}

			if !ok {
				continue
			}

			if peerDist < distToQuery {
				// fmt.Printf("discarding %d because %f < %f\n", curr.ID, peerDist, distToQuery)
				good = false
				break
			}
		}

		if good {
			returnList = append(returnList, curr)
		}

	}

	// TODO: respect deny list
	// if denyList != nil && denyList.Contains(elem.ID) {
	// 	continue
	// }
	if len(returnList) == 0 {
		panic("heuristic should never return zero neighbors")
	}

	// if len(returnList) == 1 {
	// 	fmt.Printf("we got just a single inputs were: %v\n", debug)
	// 	panic("just one")
	// }

	for _, retElem := range returnList {
		input.Insert(retElem.ID, retElem.Dist)
	}

	// fmt.Printf("started with %d elems, ended with %d elems\n", lenBegin, input.Len())
}

func (h *hnsw) selectNeighborsHeuristicFromId(nodeId uint64, ids []uint64,
	max int, denyList helpers.AllowList) (*priorityqueue.Queue, error) {
	vec, err := h.vectorForID(context.Background(), nodeId)
	if err != nil {
		return nil, err
	}

	distancer := h.distancerProvider.New(vec)

	// TODO: we're doing twice the work here, it will be reversed again in the
	// heurisitic
	idQ := priorityqueue.NewMax(len(ids))
	for _, id := range ids {
		vecA, err := h.vectorForID(context.Background(), id)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID)
				continue
			} else {
				// not a typed error, we can recover from, return with err
				return nil, errors.Wrapf(err,
					"could not get vector of object at docID %d", id)
			}
		}
		dist, _, err := distancer.Distance(vecA)
		if err != nil {
			return nil, errors.Wrap(err, "select neighbors simple from id")
		}

		idQ.Insert(id, dist)
	}

	h.selectNeighborsHeuristic(idQ, max, denyList)
	return idQ, nil
}

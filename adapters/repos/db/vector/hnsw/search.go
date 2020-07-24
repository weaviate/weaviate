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
	"github.com/pkg/errors"
)

func (h *hnsw) SearchByID(id int, k int) ([]int, error) {
	// TODO: make ef configurable
	return h.knnSearch(id, k, 36)
}

func (h *hnsw) SearchByVector(vector []float32, k int) ([]int, error) {
	// TODO: make ef configurable
	return h.knnSearchByVector(vector, k, 36)
}

func (h *hnsw) searchLayer(queryNode *hnswVertex, entrypoints binarySearchTreeGeneric, ef int, level int) (*binarySearchTreeGeneric, error) {

	// create 3 copies of the entrypoint bst
	visited := map[uint32]struct{}{}
	for _, elem := range entrypoints.flattenInOrder() {
		visited[uint32(elem.index)] = struct{}{}
	}
	candidates := &binarySearchTreeGeneric{}
	results := &binarySearchTreeGeneric{}

	for _, ep := range entrypoints.flattenInOrder() {
		candidates.insert(ep.index, ep.dist)
		results.insert(ep.index, ep.dist)
	}

	for candidates.root != nil { // efficient way to see if the len is > 0
		candidate := candidates.minimum()
		candidates.delete(candidate.index, candidate.dist)
		worstResultDistance, err := h.distBetweenNodes(results.maximum().index, queryNode.id)
		if err != nil {
			return nil, errors.Wrap(err, "calculated distance between worst result and query")
		}

		dist, err := h.distBetweenNodes(candidate.index, queryNode.id)
		if err != nil {
			return nil, errors.Wrap(err, "calculated distance between best candidate and query")
		}

		if dist > worstResultDistance {
			break
		}

		// before := time.Now()
		h.RLock()
		// m.addBuildingReadLocking(before)
		candidateNode := h.nodes[candidate.index]
		h.RUnlock()

		// before = time.Now()
		candidateNode.RLock()
		// m.addBuildingItemLocking(before)
		connections := candidateNode.connections[level]
		candidateNode.RUnlock()

		for _, neighborID := range connections {
			if _, ok := visited[neighborID]; ok {
				// skip if we've already visited this neighbor
				continue
			}

			// make sure we never visit this neighbor again
			visited[neighborID] = struct{}{}

			distance, err := h.distBetweenNodes(int(neighborID), queryNode.id)
			if err != nil {
				return nil, errors.Wrap(err, "calculate distance between neighbor and query")
			}

			resLenBefore := results.len() // calculating just once saves a bit of time
			if distance < worstResultDistance || resLenBefore < ef {
				results.insert(int(neighborID), distance)
				candidates.insert(int(neighborID), distance)

				if resLenBefore+1 > ef { // +1 because we have added one node size calculating the len
					max := results.maximum()
					results.delete(max.index, max.dist)
				}

			}

		}
	}

	return results, nil
}

func (h *hnsw) knnSearch(queryNodeID int, k int, ef int) ([]int, error) {
	h.RLock()
	queryNode := h.nodes[queryNodeID]
	h.RUnlock()

	entryPointID := h.entryPointID
	entryPointDistance, err := h.distBetweenNodes(entryPointID, queryNodeID)
	if err != nil {
		return nil, errors.Wrap(err, "knn search: distance between entrypint and query node")
	}

	for level := h.currentMaximumLayer; level >= 1; level-- { // stop at layer 1, not 0!
		eps := &binarySearchTreeGeneric{}
		eps.insert(entryPointID, entryPointDistance)
		res, err := h.searchLayer(queryNode, *eps, 1, level)
		if err != nil {
			return nil, errors.Wrapf(err, "knn search: search layer at level %d", level)
		}
		best := res.minimum()
		entryPointID = best.index
		entryPointDistance = best.dist
	}

	eps := &binarySearchTreeGeneric{}
	eps.insert(entryPointID, entryPointDistance)
	res, err := h.searchLayer(queryNode, *eps, ef, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "knn search: search layer at level %d", 0)
	}

	flat := res.flattenInOrder()
	size := min(len(flat), k)
	out := make([]int, size)
	for i, elem := range flat {
		if i >= size {
			break
		}
		out[i] = elem.index
	}

	return out, nil
}

func (h *hnsw) searchLayerByVector(queryVector []float32, entrypoints binarySearchTreeGeneric, ef int, level int) (*binarySearchTreeGeneric, error) {

	// create 3 copies of the entrypoint bst
	visited := map[uint32]struct{}{}
	for _, elem := range entrypoints.flattenInOrder() {
		visited[uint32(elem.index)] = struct{}{}
	}
	candidates := &binarySearchTreeGeneric{}
	results := &binarySearchTreeGeneric{}

	for _, ep := range entrypoints.flattenInOrder() {
		candidates.insert(ep.index, ep.dist)
		results.insert(ep.index, ep.dist)
	}

	for candidates.root != nil { // efficient way to see if the len is > 0
		candidate := candidates.minimum()
		candidates.delete(candidate.index, candidate.dist)
		worstResultDistance, err := h.distBetweenNodeAndVec(results.maximum().index, queryVector)
		if err != nil {
			return nil, errors.Wrap(err, "calculated distance between worst result and query")
		}

		dist, err := h.distBetweenNodeAndVec(candidate.index, queryVector)
		if err != nil {
			return nil, errors.Wrap(err, "calculated distance between best candidate and query")
		}

		if dist > worstResultDistance {
			break
		}

		// before := time.Now()
		h.RLock()
		// m.addBuildingReadLocking(before)
		candidateNode := h.nodes[candidate.index]
		h.RUnlock()

		// before = time.Now()
		candidateNode.RLock()
		// m.addBuildingItemLocking(before)
		connections := candidateNode.connections[level]
		candidateNode.RUnlock()

		for _, neighborID := range connections {
			if _, ok := visited[neighborID]; ok {
				// skip if we've already visited this neighbor
				continue
			}

			// make sure we never visit this neighbor again
			visited[neighborID] = struct{}{}

			distance, err := h.distBetweenNodeAndVec(int(neighborID), queryVector)
			if err != nil {
				return nil, errors.Wrap(err, "calculate distance between neighbor and query")
			}

			resLenBefore := results.len() // calculating just once saves a bit of time
			if distance < worstResultDistance || resLenBefore < ef {
				results.insert(int(neighborID), distance)
				candidates.insert(int(neighborID), distance)

				if resLenBefore+1 > ef { // +1 because we have added one node size calculating the len
					max := results.maximum()
					results.delete(max.index, max.dist)
				}

			}

		}
	}

	return results, nil
}

func (h *hnsw) knnSearchByVector(searchVec []float32, k int, ef int) ([]int, error) {

	entryPointID := h.entryPointID
	entryPointDistance, err := h.distBetweenNodeAndVec(entryPointID, searchVec)
	if err != nil {
		return nil, errors.Wrap(err, "knn search: distance between entrypint and query node")
	}

	for level := h.currentMaximumLayer; level >= 1; level-- { // stop at layer 1, not 0!
		eps := &binarySearchTreeGeneric{}
		eps.insert(entryPointID, entryPointDistance)
		res, err := h.searchLayerByVector(searchVec, *eps, 1, level)
		if err != nil {
			return nil, errors.Wrapf(err, "knn search: search layer at level %d", level)
		}
		best := res.minimum()
		entryPointID = best.index
		entryPointDistance = best.dist
	}

	eps := &binarySearchTreeGeneric{}
	eps.insert(entryPointID, entryPointDistance)
	res, err := h.searchLayerByVector(searchVec, *eps, ef, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "knn search: search layer at level %d", 0)
	}

	flat := res.flattenInOrder()
	size := min(len(flat), k)
	out := make([]int, size)
	for i, elem := range flat {
		if i >= size {
			break
		}
		out[i] = elem.index
	}

	return out, nil
}

func (h *hnsw) selectNeighborsSimple(nodeId int, input binarySearchTreeGeneric, max int) []uint32 {
	flat := input.flattenInOrder()
	size := min(len(flat), max)
	out := make([]uint32, size)
	for i, elem := range flat {
		if i >= size {
			break
		}
		out[i] = uint32(elem.index)
	}

	return out
}

func (h *hnsw) selectNeighborsSimpleFromId(nodeId int, ids []uint32, max int) ([]uint32, error) {
	bst := &binarySearchTreeGeneric{}
	for _, id := range ids {
		dist, err := h.distBetweenNodes(int(id), nodeId)
		if err != nil {
			return nil, errors.Wrap(err, "select neighbors simple from id")
		}
		bst.insert(int(id), dist)
	}

	return h.selectNeighborsSimple(nodeId, *bst, max), nil
}

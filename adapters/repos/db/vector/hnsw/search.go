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

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
)

func (h *hnsw) SearchByID(id int, k int) ([]int, error) {
	// TODO: make ef configurable
	return h.knnSearch(id, k, 8*k)
}

func (h *hnsw) SearchByVector(vector []float32, k int, allowList inverted.AllowList) ([]int, error) {
	// TODO: make ef configurable
	return h.knnSearchByVector(vector, k, k*8, allowList)
}

func (h *hnsw) knnSearch(queryNodeID int, k int, ef int) ([]int, error) {
	entryPointID := h.entryPointID
	entryPointDistance, err := h.distBetweenNodes(entryPointID, queryNodeID)
	if err != nil {
		return nil, errors.Wrap(err, "knn search: distance between entrypint and query node")
	}

	queryVector, err := h.vectorForID(context.Background(),
		int32(queryNodeID))
	if err != nil {
		return nil, errors.Wrapf(err, "could not get vector of object at docID %d", queryNodeID)
	}

	for level := h.currentMaximumLayer; level >= 1; level-- { // stop at layer 1, not 0!
		eps := &binarySearchTreeGeneric{}
		eps.insert(entryPointID, entryPointDistance)

		res, err := h.searchLayerByVector(queryVector, *eps, 1, level, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "knn search: search layer at level %d", level)
		}
		best := res.minimum()
		entryPointID = best.index
		entryPointDistance = best.dist
	}

	eps := &binarySearchTreeGeneric{}
	eps.insert(entryPointID, entryPointDistance)
	res, err := h.searchLayerByVector(queryVector, *eps, ef, 0, nil)
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

func (h *hnsw) searchLayerByVector(queryVector []float32,
	entrypoints binarySearchTreeGeneric, ef int, level int,
	allowList inverted.AllowList) (*binarySearchTreeGeneric, error) {

	// create 3 copies of the entrypoint bst
	visited := map[uint32]struct{}{}
	for _, elem := range entrypoints.flattenInOrder() {
		visited[uint32(elem.index)] = struct{}{}
	}
	candidates := &binarySearchTreeGeneric{}
	results := &binarySearchTreeGeneric{}

	distancer := newReusableDistancer(queryVector)

	for _, ep := range entrypoints.flattenInOrder() {
		candidates.insert(ep.index, ep.dist)
		if level == 0 && allowList != nil {
			// we are on the lowest level containing the actual candidates and we
			// have an allow list (i.e. the user has probably set some sort of a
			// filter restricting this search further. As a result we have to
			// ignore items not on the list
			if !allowList.Contains(uint32(ep.index)) {
				continue
			}
		}

		results.insert(ep.index, ep.dist)
	}

	for candidates.root != nil { // efficient way to see if the len is > 0
		candidate := candidates.minimum()
		candidates.delete(candidate.index, candidate.dist)

		var worstResultDistance float32

		if results.root != nil {
			nodeVec, err := h.vectorForID(context.Background(),
				int32(results.maximum().index))
			if err != nil {
				return nil, errors.Wrapf(err, "could not get vector of object at docID %d", results.maximum().index)
			}
			d, err := distancer.distance(nodeVec)
			if err != nil {
				return nil, errors.Wrap(err, "calculated distance between worst result and query")
			}
			worstResultDistance = d
		} else {
			// if the entrypoint (which we received from a higher layer doesn't match
			// the allow List the result list is empty. In this case we can just set
			// the worstDistance to an arbitrarily large number, so that any
			// (allowed) candidate will have a lower distance in comparison
			worstResultDistance = 9001
		}

		candidateVec, err := h.vectorForID(context.Background(),
			int32(candidate.index))
		if err != nil {
			return nil, errors.Wrapf(err, "could not get vector of object at docID %d", results.maximum().index)
		}
		dist, err := distancer.distance(candidateVec)
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

			neighborVec, err := h.vectorForID(context.Background(),
				int32(neighborID))
			if err != nil {
				return nil, errors.Wrapf(err, "could not get vector of object at docID %d", results.maximum().index)
			}
			distance, err := distancer.distance(neighborVec)
			if err != nil {
				return nil, errors.Wrap(err, "calculate distance between neighbor and query")
			}

			resLenBefore := results.len() // calculating just once saves a bit of time
			if distance < worstResultDistance || resLenBefore < ef {
				candidates.insert(int(neighborID), distance)
				if level == 0 && allowList != nil {
					// we are on the lowest level containing the actual candidates and we
					// have an allow list (i.e. the user has probably set some sort of a
					// filter restricting this search further. As a result we have to
					// ignore items not on the list
					if !allowList.Contains(neighborID) {
						continue
					}
				}
				results.insert(int(neighborID), distance)

				if resLenBefore+1 > ef { // +1 because we have added one node size calculating the len
					max := results.maximum()
					results.delete(max.index, max.dist)
				}

			}

		}
	}

	return results, nil
}

func (h *hnsw) knnSearchByVector(searchVec []float32, k int,
	ef int, allowList inverted.AllowList) ([]int, error) {

	entryPointID := h.entryPointID
	entryPointDistance, err := h.distBetweenNodeAndVec(entryPointID, searchVec)
	if err != nil {
		return nil, errors.Wrap(err, "knn search: distance between entrypint and query node")
	}

	for level := h.currentMaximumLayer; level >= 1; level-- { // stop at layer 1, not 0!
		eps := &binarySearchTreeGeneric{}
		eps.insert(entryPointID, entryPointDistance)
		res, err := h.searchLayerByVector(searchVec, *eps, 1, level, nil) // ignore allowList on layers > 0
		if err != nil {
			return nil, errors.Wrapf(err, "knn search: search layer at level %d", level)
		}
		best := res.minimum()
		entryPointID = best.index
		entryPointDistance = best.dist
	}

	eps := &binarySearchTreeGeneric{}
	eps.insert(entryPointID, entryPointDistance)
	res, err := h.searchLayerByVector(searchVec, *eps, ef, 0, allowList)
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

func (h *hnsw) selectNeighborsSimple(input binarySearchTreeGeneric,
	max int, denyList inverted.AllowList) []uint32 {
	flat := input.flattenInOrder()
	size := min(len(flat), max)
	out := make([]uint32, size)
	for i, elem := range flat {
		if denyList != nil && denyList.Contains(uint32(elem.index)) {
			continue
		}

		if i >= size {
			break
		}
		out[i] = uint32(elem.index)
	}

	return out
}

func (h *hnsw) selectNeighborsSimpleFromId(nodeId int, ids []uint32,
	max int, denyList inverted.AllowList) ([]uint32, error) {
	bst := &binarySearchTreeGeneric{}
	for _, id := range ids {
		dist, err := h.distBetweenNodes(int(id), nodeId)
		if err != nil {
			return nil, errors.Wrap(err, "select neighbors simple from id")
		}
		bst.insert(int(id), dist)
	}

	return h.selectNeighborsSimple(*bst, max, denyList), nil
}

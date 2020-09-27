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
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
)

func reasonableEfFromK(k int) int {
	ef := k * 8
	if ef > 100 {
		ef = 100
	}

	return ef
}

func (h *hnsw) SearchByID(id int, k int) ([]int, error) {
	return h.knnSearch(id, k, reasonableEfFromK(k))
}

func (h *hnsw) SearchByVector(vector []float32, k int, allowList inverted.AllowList) ([]int, error) {
	return h.knnSearchByVector(vector, k, reasonableEfFromK(k), allowList)
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

	visited := newVisitedList(entrypoints)
	candidates := &binarySearchTreeGeneric{}
	results := &binarySearchTreeGeneric{}
	distancer := newReusableDistancer(queryVector)

	h.insertViableEntrypointsAsCandidatesAndResults(entrypoints, candidates,
		results, level, allowList)

	for candidates.root != nil { // efficient way to see if the len is > 0
		candidate := candidates.minimum()
		candidates.delete(candidate.index, candidate.dist)

		worstResultDistance, err := h.currentWorstResultDistance(results, distancer)
		if err != nil {
			return nil, errors.Wrapf(err, "calculate distance of current last result")
		}

		dist, err := h.distanceToNode(distancer, int32(candidate.index))
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				// the underlying object seems to have been deleted, to recover from
				// this situation let's add a tombstone to the deleted object, so it
				// will be cleaned up and skip this candidate in the current search

				h.addTombstone(int(e.DocID))
				// TODO: Use structured logging, log level WARNING
				fmt.Printf("WARNING: skipping node %d as we couldn't find a vector for it. Original Error: %v\n",
					e.DocID, e.Error())
				continue
			} else {
				// not a typed error, we can recover from, return with err
				return nil, errors.Wrap(err, "calculate distance between candidate and query")
			}
		}

		if dist > worstResultDistance {
			break
		}

		// before := time.Now()
		h.RLock()
		// m.addBuildingReadLocking(before)
		candidateNode := h.nodes[candidate.index]
		h.RUnlock()

		if candidateNode == nil {
			// could have been a node that already had a tombstone attached and was
			// just cleaned up while we were waiting for a read lock
			continue
		}

		// before = time.Now()
		candidateNode.RLock()
		// m.addBuildingItemLocking(before)
		connections := candidateNode.connections[level]
		candidateNode.RUnlock()

		if err := h.extendCandidatesAndResultsFromNeighbors(candidates, results,
			connections, visited, distancer, ef, level, allowList,
			worstResultDistance); err != nil {
			return nil, errors.Wrap(err, "extend candidates and results from neighbors")
		}
	}

	return results, nil
}

func newVisitedList(entrypoints binarySearchTreeGeneric) map[uint32]struct{} {
	visited := map[uint32]struct{}{}
	for _, elem := range entrypoints.flattenInOrder() {
		visited[uint32(elem.index)] = struct{}{}
	}
	return visited
}

func (h *hnsw) insertViableEntrypointsAsCandidatesAndResults(
	entrypoints binarySearchTreeGeneric, candidates,
	results *binarySearchTreeGeneric, level int, allowList inverted.AllowList) {
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

		if level == 0 && h.hasTombstone(ep.index) {
			continue
		}

		results.insert(ep.index, ep.dist)
	}
}

func (h *hnsw) currentWorstResultDistance(results *binarySearchTreeGeneric,
	distancer *reusableDistancer) (float32, error) {
	if results.root != nil {
		id := int32(results.maximum().index)
		d, err := h.distanceToNode(distancer, id)
		if err != nil {
			return 0, errors.Wrap(err,
				"calculated distance between worst result and query")
		}
		return d, nil
	} else {
		// if the entrypoint (which we received from a higher layer doesn't match
		// the allow List the result list is empty. In this case we can just set
		// the worstDistance to an arbitrarily large number, so that any
		// (allowed) candidate will have a lower distance in comparison
		return 9001, nil
	}
}

func (h *hnsw) extendCandidatesAndResultsFromNeighbors(candidates,
	results *binarySearchTreeGeneric, connections []uint32,
	visited map[uint32]struct{}, distancer *reusableDistancer, ef int,
	level int, allowList inverted.AllowList, worstResultDistance float32) error {
	for _, neighborID := range connections {
		if _, ok := visited[neighborID]; ok {
			// skip if we've already visited this neighbor
			continue
		}

		// make sure we never visit this neighbor again
		visited[neighborID] = struct{}{}

		distance, err := h.distanceToNode(distancer, int32(neighborID))
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				// the underlying object seems to have been deleted, to recover from
				// this situation let's add a tombstone to the deleted object, so it
				// will be cleaned up and skip this candidate in the current search

				h.addTombstone(int(e.DocID))
				// TODO: Use structured logging, log level WARNING
				fmt.Printf("WARNING: skipping node %d as we couldn't find a vector for it. Original Error: %v\n",
					e.DocID, e.Error())
				continue
			} else {
				// not a typed error, we can recover from, return with err
				return errors.Wrap(err, "calculate distance between candidate and query")
			}
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

			if level == 0 && h.hasTombstone(int(neighborID)) {
				continue
			}

			results.insert(int(neighborID), distance)

			// +1 because we have added one node size calculating the len
			if resLenBefore+1 > ef {
				max := results.maximum()
				results.delete(max.index, max.dist)
			}
		}
	}

	return nil
}

func (h *hnsw) distanceToNode(distancer *reusableDistancer,
	nodeID int32) (float32, error) {

	candidateVec, err := h.vectorForID(context.Background(), nodeID)
	if err != nil {
		return 0, errors.Wrapf(err, "could not get vector of object at docID %d",
			nodeID)
	}
	dist, err := distancer.distance(candidateVec)
	if err != nil {
		return 0, err
	}

	return dist, nil
}

func (h *hnsw) knnSearchByVector(searchVec []float32, k int,
	ef int, allowList inverted.AllowList) ([]int, error) {

	entryPointID := h.entryPointID
	entryPointDistance, err := h.distBetweenNodeAndVec(entryPointID, searchVec)
	if err != nil {
		return nil, errors.Wrap(err, "knn search: distance between entrypint and query node")
	}

	// stop at layer 1, not 0!
	for level := h.currentMaximumLayer; level >= 1; level-- {
		eps := &binarySearchTreeGeneric{}
		eps.insert(entryPointID, entryPointDistance)
		// ignore allowList on layers > 0
		res, err := h.searchLayerByVector(searchVec, *eps, 1, level, nil)
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

	maxSize := min(len(flat), max)
	out := make([]uint32, maxSize)
	actualSize := 0
	for i, elem := range flat {
		if denyList != nil && denyList.Contains(uint32(elem.index)) {
			continue
		}

		if i >= maxSize {
			break
		}
		out[actualSize] = uint32(elem.index)
		actualSize++
	}

	return out[:actualSize]
}

func (h *hnsw) selectNeighborsSimpleFromId(nodeId int, ids []uint32,
	max int, denyList inverted.AllowList) ([]uint32, error) {
	bst := &binarySearchTreeGeneric{}
	for _, id := range ids {
		dist, err := h.distBetweenNodes(int(id), nodeId)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				// the underlying object seems to have been deleted, to recover from
				// this situation let's add a tombstone to the deleted object, so it
				// will be cleaned up and skip this candidate in the current search

				h.addTombstone(int(e.DocID))
				// TODO: Use structured logging, log level WARNING
				fmt.Printf("WARNING: skipping node %d as we couldn't find a vector for it. Original Error: %v\n",
					e.DocID, e.Error())
				continue
			} else {
				// not a typed error, we can recover from, return with err
				return nil, errors.Wrap(err, "select neighbors simple from id")
			}
		}
		bst.insert(int(id), dist)
	}

	return h.selectNeighborsSimple(*bst, max, denyList), nil
}

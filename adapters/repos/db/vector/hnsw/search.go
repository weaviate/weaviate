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
	"container/heap"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func reasonableEfFromK(k int) int {
	ef := k * 8
	if ef > 100 {
		ef = 100
	}
	if k > ef {
		ef = k // otherwise results will get cut off early
	}

	return ef
}

func (h *hnsw) SearchByID(id uint64, k int) ([]uint64, error) {
	return h.knnSearch(id, k, reasonableEfFromK(k))
}

func (h *hnsw) SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, error) {
	return h.knnSearchByVector(vector, k, reasonableEfFromK(k), allowList)
}

func (h *hnsw) knnSearch(queryNodeID uint64, k int, ef int) ([]uint64, error) {
	entryPointID := h.entryPointID
	entryPointDistance, ok, err := h.distBetweenNodes(entryPointID, queryNodeID)
	if err != nil || !ok {
		return nil, errors.Wrap(err, "knn search: distance between entrypint and query node")
	}

	queryVector, err := h.vectorForID(context.Background(), queryNodeID)
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
	out := make([]uint64, size)
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
	allowList helpers.AllowList) (*binarySearchTreeGeneric, error) {
	visited := h.newVisitedList(entrypoints)
	candidates := &binarySearchTreeGeneric{}
	results := &binarySearchTreeGeneric{}
	distancer := h.distancerProvider.New(queryVector)

	// var timeDistancing time.Duration
	// var timeGetMiminium time.Duration
	// var timeDelete time.Duration
	// var timeExtending time.Duration
	// var timeExtendingDistancing time.Duration
	// var timeExtendingOther otherTimes
	// var timeOther time.Duration

	h.insertViableEntrypointsAsCandidatesAndResults(entrypoints, candidates,
		results, level, allowList)

	// before = time.Now()
	worstResultDistance, err := h.currentWorstResultDistance(results, distancer)
	if err != nil {
		return nil, errors.Wrapf(err, "calculate distance of current last result")
	}
	// fmt.Printf("initial worst distance is %f\n", worstResultDistance)

	updateWorstResultDistance := func(dist float32) {
		worstResultDistance = dist
	}
	// timeOther += time.Since(before)

	for candidates.root != nil { // efficient way to see if the len is > 0
		// before := time.Now()
		candidate := candidates.minimum()
		// fmt.Printf("got a new candidate with dist %f\n", candidate.dist)
		// timeGetMiminium += time.Since(before)
		if candidate.dist > worstResultDistance {
			// fmt.Printf("stoppoing because cadidate %d dist is %f worse than worst result %f\n", candidate.index, candidate.dist,
			// worstResultDistance)
			break
		}

		// before = time.Now()
		candidates.delete(candidate.index, candidate.dist)
		// timeDelete += time.Since(before)

		// // before = time.Now()
		// dist, ok, err := h.distanceToNode(distancer, candidate.index)
		// if err != nil {
		// 	return nil, errors.Wrap(err, "calculate distance between candidate and query")
		// }
		// // timeDistancing += time.Since(before)

		// if !ok {
		// 	continue
		// }

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

		// before = time.Now()
		if _, _, err := h.extendCandidatesAndResultsFromNeighbors(candidates, results,
			connections, visited, distancer, ef, level, allowList,
			worstResultDistance, updateWorstResultDistance); err != nil {
			return nil, errors.Wrap(err, "extend candidates and results from neighbors")
		} else {
			// timeExtendingDistancing += td
			// timeExtendingOther.allowListChecking += to.allowListChecking
			// timeExtendingOther.inserting += to.inserting
			// timeExtendingOther.maxDeleting += to.maxDeleting
			// timeExtendingOther.tombstoneChecking += to.tombstoneChecking
			// timeExtendingOther.visitedListReading += to.visitedListReading
			// timeExtendingOther.visitedListWriting += to.visitedListWriting
			// timeExtendingOther.lenCalculating += to.lenCalculating
			// timeExtendingOther.total += to.total
			// timeExtendingOther.visitedListReadCount += to.visitedListReadCount
			// timeExtendingOther.visitedListWriteCount += to.visitedListWriteCount
		}
		// timeExtending += time.Since(before)
	}

	// beforePrint := time.Now()
	// fmt.Printf("distancing: %s\nminimum: %s\ndelete: %s\nextending: %s\nextending-distancing: %s\nother: %s\n\n", timeDistancing, timeGetMiminium, timeDelete, timeExtending, timeExtendingDistancing, timeOther)
	// timeExtendingOther.Print()
	// fmt.Printf("print took %s\n\n\n\n", time.Since(beforePrint))

	return results, nil
}

// func (h *hnsw) searchLayerByVectorPQ(queryVector []float32,
// 	entrypoint *pqItem, ef int, level int,
// 	allowList helpers.AllowList) (priorityQueue, error) {
// 	visited := newVisitedListPQ(entrypoint)

// 	candidates := priorityQueue{}
// 	results := priorityQueue{}
// 	distancer := h.distancerProvider.New(queryVector)

// 	var timeDistancing time.Duration
// 	var timeGetMiminium time.Duration
// 	var timeDelete time.Duration
// 	var timeExtending time.Duration
// 	var timeExtendingDistancing time.Duration
// 	var timeExtendingOther time.Duration
// 	var timeOther time.Duration

// 	h.insertViableEntrypointsAsCandidatesAndResultsPQ(entrypoint, candidates,
// 		results, level, allowList)

// 	for pq.Len() > 0 {
// 		before := time.Now()
// 		candidate := candidates.minimum()
// 		timeGetMiminium += time.Since(before)

// 		before = time.Now()
// 		candidates.delete(candidate.index, candidate.dist)
// 		timeDelete += time.Since(before)

// 		before = time.Now()
// 		worstResultDistance, err := h.currentWorstResultDistance(results, distancer)
// 		if err != nil {
// 			return nil, errors.Wrapf(err, "calculate distance of current last result")
// 		}
// 		timeOther += time.Since(before)

// 		before = time.Now()
// 		dist, ok, err := h.distanceToNode(distancer, candidate.index)
// 		if err != nil {
// 			return nil, errors.Wrap(err, "calculate distance between candidate and query")
// 		}
// 		timeDistancing += time.Since(before)

// 		if !ok {
// 			continue
// 		}

// 		if dist > worstResultDistance {
// 			break
// 		}

// 		// before := time.Now()
// 		h.RLock()
// 		// m.addBuildingReadLocking(before)
// 		candidateNode := h.nodes[candidate.index]
// 		h.RUnlock()

// 		if candidateNode == nil {
// 			// could have been a node that already had a tombstone attached and was
// 			// just cleaned up while we were waiting for a read lock
// 			continue
// 		}

// 		// before = time.Now()
// 		candidateNode.RLock()
// 		// m.addBuildingItemLocking(before)
// 		connections := candidateNode.connections[level]
// 		candidateNode.RUnlock()

// 		before = time.Now()
// 		if td, to, err := h.extendCandidatesAndResultsFromNeighbors(candidates, results,
// 			connections, visited, distancer, ef, level, allowList,
// 			worstResultDistance); err != nil {
// 			return nil, errors.Wrap(err, "extend candidates and results from neighbors")
// 		} else {
// 			timeExtendingDistancing += td
// 			timeExtendingOther += to
// 		}
// 		timeExtending += time.Since(before)
// 	}

// 	fmt.Printf("distancing: %s\nminimum: %s\ndelete: %s\nextending: %s\nextending-distancing: %s\nextending-other: %s\nother: %s\n\n", timeDistancing, timeGetMiminium, timeDelete, timeExtending, timeExtendingDistancing, timeExtendingOther, timeOther)

// 	return results, nil
// }

func (h *hnsw) newVisitedList(entrypoints binarySearchTreeGeneric) []bool {
	h.RLock()
	size := len(h.nodes) + defaultIndexGrowthDelta // add delta to be add some
	// buffer if a visited list is created shortly before a growth operation
	h.RUnlock()

	visited := make([]bool, size)
	for _, elem := range entrypoints.flattenInOrder() {
		visited[elem.index] = true
	}
	return visited
}

func newVisitedListPQ(entrypoint *pqItem) map[uint64]struct{} {
	visited := map[uint64]struct{}{
		entrypoint.hnswIndex: struct{}{},
	}
	return visited
}

func (h *hnsw) insertViableEntrypointsAsCandidatesAndResults(
	entrypoints binarySearchTreeGeneric, candidates,
	results *binarySearchTreeGeneric, level int, allowList helpers.AllowList) {
	for _, ep := range entrypoints.flattenInOrder() {
		candidates.insert(ep.index, ep.dist)
		if level == 0 && allowList != nil {
			// we are on the lowest level containing the actual candidates and we
			// have an allow list (i.e. the user has probably set some sort of a
			// filter restricting this search further. As a result we have to
			// ignore items not on the list
			if !allowList.Contains(ep.index) {
				continue
			}
		}

		if h.hasTombstone(ep.index) {
			continue
		}

		results.insert(ep.index, ep.dist)
	}
}

func (h *hnsw) insertViableEntrypointsAsCandidatesAndResultsPQ(
	entrypoint *pqItem, candidates,
	results *priorityQueue, level int, allowList helpers.AllowList) {
	heap.Push(candidates, entrypoint)
	if level == 0 && allowList != nil {
		// we are on the lowest level containing the actual candidates and we
		// have an allow list (i.e. the user has probably set some sort of a
		// filter restricting this search further. As a result we have to
		// ignore items not on the list
		if !allowList.Contains(entrypoint.hnswIndex) {
			return
		}

		if h.hasTombstone(entrypoint.hnswIndex) {
			return
		}

		heap.Push(results, entrypoint)
	}
}

func (h *hnsw) currentWorstResultDistance(results *binarySearchTreeGeneric,
	distancer distancer.Distancer) (float32, error) {
	if results.root != nil {
		return results.maximum().dist, nil
		// d, ok, err := h.distanceToNode(distancer, id)
		// if err != nil {
		// 	return 0, errors.Wrap(err,
		// 		"calculated distance between worst result and query")
		// }

		// if !ok {
		// 	return math.MaxFloat32, nil
		// }
		// return d, nil
	} else {
		// if the entrypoint (which we received from a higher layer doesn't match
		// the allow List the result list is empty. In this case we can just set
		// the worstDistance to an arbitrarily large number, so that any
		// (allowed) candidate will have a lower distance in comparison
		return math.MaxFloat32, nil
	}
}

type otherTimes struct {
	inserting             time.Duration
	allowListChecking     time.Duration
	maxDeleting           time.Duration
	tombstoneChecking     time.Duration
	visitedListReading    time.Duration
	visitedListWriting    time.Duration
	visitedListReadCount  int
	visitedListWriteCount int
	lenCalculating        time.Duration
	total                 time.Duration
}

func (t otherTimes) Print() {
	fmt.Printf("ex-other-inserting: %s\n", t.inserting)
	fmt.Printf("ex-other-allowlist: %s\n", t.allowListChecking)
	fmt.Printf("ex-other-maxDeleting: %s\n", t.maxDeleting)
	fmt.Printf("ex-other-tombstoneChecking: %s\n", t.tombstoneChecking)
	fmt.Printf("ex-other-visitedListWriting: %s\n", t.visitedListWriting)
	fmt.Printf("ex-other-visitedListReading: %s\n", t.visitedListReading)
	fmt.Printf("ex-other-visitedListReadCount: %d\n", t.visitedListReadCount)
	fmt.Printf("ex-other-visitedListWriteCount: %d\n", t.visitedListWriteCount)
	fmt.Printf("ex-other-lenCalculating: %s\n", t.lenCalculating)
	fmt.Printf("ex-other-total: %s\n", t.total)
}

func (h *hnsw) extendCandidatesAndResultsFromNeighbors(candidates,
	results *binarySearchTreeGeneric, connections []uint64,
	visited []bool, distancer distancer.Distancer, ef int,
	level int, allowList helpers.AllowList, worstResultDistance float32,
	updateWorstResultDistance func(dist float32),
) (time.Duration, otherTimes, error) {
	var timeDistancing time.Duration
	var timeOther otherTimes
	// var before time.Time
	total := time.Now()
	for _, neighborID := range connections {

		// before = time.Now()
		// timeOther.visitedListReadCount++
		if ok := visited[neighborID]; ok {
			// skip if we've already visited this neighbor
			// timeOther.visitedListReading += time.Since(before)
			continue
		}
		// timeOther.visitedListReading += time.Since(before)

		// make sure we never visit this neighbor again
		// before = time.Now()
		// timeOther.visitedListWriteCount++
		visited[neighborID] = true
		// timeOther.visitedListWriting += time.Since(before)

		// before = time.Now()
		distance, ok, err := h.distanceToNode(distancer, neighborID)
		if err != nil {
			return 0, timeOther, errors.Wrap(err, "calculate distance between candidate and query")
		}
		// timeDistancing += time.Since(before)

		if !ok {
			// node was deleted in the underlying object store
			continue
		}

		// before = time.Now()
		resLenBefore := results.len() // calculating just once saves a bit of time
		// timeOther.lenCalculating += time.Since(before)

		if resLenBefore < ef || distance < worstResultDistance {
			// before = time.Now()
			candidates.insert(neighborID, distance)
			// fmt.Printf("candidate total length is now %d\n", candidates.len())
			// fmt.Printf("inserting new cand with dist %f\n", distance)
			// timeOther.inserting += time.Since(before)

			// before = time.Now()
			if level == 0 && allowList != nil {
				// we are on the lowest level containing the actual candidates and we
				// have an allow list (i.e. the user has probably set some sort of a
				// filter restricting this search further. As a result we have to
				// ignore items not on the list
				if !allowList.Contains(neighborID) {
					// timeOther.allowListChecking += time.Since(before)
					continue
				}
			}
			// timeOther.allowListChecking += time.Since(before)

			// before = time.Now()
			if h.hasTombstone(neighborID) {
				// timeOther.tombstoneChecking += time.Since(before)
				continue
			}
			// timeOther.tombstoneChecking += time.Since(before)

			// before = time.Now()
			results.insert(neighborID, distance)
			// timeOther.inserting += time.Since(before)

			// before = time.Now()
			// +1 because we have added one node size calculating the len
			if resLenBefore+1 > ef {
				max := results.maximum()
				results.delete(max.index, max.dist)
			}

			if resLenBefore != 0 {
				worstResult := results.maximum()
				worstResultDistance = worstResult.dist
				updateWorstResultDistance(worstResultDistance)
				// fmt.Printf("updating worst distance to %f\n", worstResultDistance)
			}
			// timeOther.maxDeleting += time.Since(before)
		}
	}

	timeOther.total = time.Since(total)

	return timeDistancing, timeOther, nil
}

func (h *hnsw) distanceToNode(distancer distancer.Distancer,
	nodeID uint64) (float32, bool, error) {
	candidateVec, err := h.vectorForID(context.Background(), nodeID)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID)
			return 0, false, nil
		} else {
			// not a typed error, we can recover from, return with err
			return 0, false, errors.Wrapf(err, "get vector of docID %d", nodeID)
		}
	}

	dist, _, err := distancer.Distance(candidateVec)
	if err != nil {
		return 0, false, errors.Wrap(err, "calculate distance between candidate and query")
	}

	return dist, true, nil
}

// the underlying object seems to have been deleted, to recover from
// this situation let's add a tombstone to the deleted object, so it
// will be cleaned up and skip this candidate in the current search
func (h *hnsw) handleDeletedNode(docID uint64) {
	if h.hasTombstone(docID) {
		// nothing to do, this node already has a tombstone, it will be cleaned up
		// in the next deletion cycle
		return
	}

	h.addTombstone(docID)
	h.logger.WithField("action", "attach_tombstone_to_deleted_node").
		WithField("node_id", docID).
		Info("found a deleted node (%d) without a tombstone, "+
			"tombstone was added", docID)
}

func (h *hnsw) knnSearchByVector(searchVec []float32, k int,
	ef int, allowList helpers.AllowList) ([]uint64, error) {
	if h.isEmpty() {
		return nil, nil
	}

	entryPointID := h.entryPointID
	entryPointDistance, ok, err := h.distBetweenNodeAndVec(entryPointID, searchVec)
	if err != nil {
		return nil, errors.Wrap(err, "knn search: distance between entrypint and query node")
	}

	if !ok {
		return nil, fmt.Errorf("entrypoint was deleted in the object store, " +
			"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
	}

	// stop at layer 1, not 0!
	for level := h.currentMaximumLayer; level >= 1; level-- {
		eps := &binarySearchTreeGeneric{}
		eps.insert(entryPointID, entryPointDistance)
		res, err := h.searchLayerByVector(searchVec, *eps, 1, level, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "knn search: search layer at level %d", level)
		}

		// There might be situations where we did not find a better entrypoint at
		// that particular level, so instead we're keeping whatever entrypoint we
		// had before (i.e. either from a previous level or even the main
		// entrypoint)
		if res.root != nil {
			best := res.flattenInOrder()

			for _, cand := range best {
				if !h.nodeByID(cand.index).isUnderMaintenance() {
					entryPointID = cand.index
					entryPointDistance = cand.dist
					break
				}

				// if we managed to go through the loop without finding a single
				// suitable node, we simply stick with the original, i.e. the global
				// entrypoint
			}
		}
	}

	eps := &binarySearchTreeGeneric{}
	eps.insert(entryPointID, entryPointDistance)
	res, err := h.searchLayerByVector(searchVec, *eps, ef, 0, allowList)
	if err != nil {
		return nil, errors.Wrapf(err, "knn search: search layer at level %d", 0)
	}

	flat := res.flattenInOrder()
	size := min(len(flat), k)
	out := make([]uint64, size)
	for i, elem := range flat {
		if i >= size {
			break
		}
		out[i] = elem.index
	}

	return out, nil
}

func (h *hnsw) selectNeighborsSimple(input binarySearchTreeGeneric,
	max int, denyList helpers.AllowList) []uint64 {
	// before := time.Now()
	flat := input.flattenInOrder()

	maxSize := min(len(flat), max)
	out := make([]uint64, maxSize)
	actualSize := 0
	for i, elem := range flat {
		if denyList != nil && denyList.Contains(elem.index) {
			continue
		}

		if i >= maxSize {
			break
		}
		out[actualSize] = elem.index
		actualSize++
	}

	// fmt.Printf("SN - flatten&cut took %s\n", time.Since(before))
	return out[:actualSize]
}

func (h *hnsw) selectNeighborsSimpleFromId(nodeId uint64, ids []uint64,
	max int, denyList helpers.AllowList) ([]uint64, error) {
	// before := time.Now()

	vec, err := h.vectorForID(context.Background(), nodeId)
	if err != nil {
		return nil, err
	}

	distancer := h.distancerProvider.New(vec)

	bst := &binarySearchTreeGeneric{}
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

		bst.insert(id, dist)
	}

	// fmt.Printf("SN - distancing took %s\n", time.Since(before))

	return h.selectNeighborsSimple(*bst, max, denyList), nil
}

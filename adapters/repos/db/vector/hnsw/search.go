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
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/visited"
)

func (h *hnsw) searchTimeEF(k int) int {
	// load atomically, so we can get away with concurrent updates of the
	// userconfig without having to set a lock each time we try to read - which
	// can be so common that it would cause considerable overhead
	ef := int(atomic.LoadInt64(&h.ef))
	if ef < 1 {
		return autoEfFromK(k)
	}

	if ef < k {
		ef = k
	}

	return ef
}

func autoEfFromK(k int) int {
	ef := k * 8
	if ef > 100 {
		ef = 100
	}
	if k > ef {
		ef = k // otherwise results will get cut off early
	}

	return ef
}

func (h *hnsw) SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	if h.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		vector = distancer.Normalize(vector)
	}
	return h.knnSearchByVector(vector, k, h.searchTimeEF(k), allowList)
}

func (h *hnsw) searchLayerByVector(queryVector []float32,
	entrypoints *priorityqueue.Queue, ef int, level int,
	allowList helpers.AllowList) (*priorityqueue.Queue, error) {
	h.Lock()
	visited := h.visitedListPool.Borrow()
	h.Unlock()

	candidates := priorityqueue.NewMin(ef)
	results := priorityqueue.NewMax(ef)
	distancer := h.distancerProvider.New(queryVector)

	h.insertViableEntrypointsAsCandidatesAndResults(entrypoints, candidates,
		results, level, visited, allowList)

	worstResultDistance, err := h.currentWorstResultDistance(results, distancer)
	if err != nil {
		return nil, errors.Wrapf(err, "calculate distance of current last result")
	}

	for candidates.Len() > 0 {
		dist, ok, err := h.distanceToNode(distancer, candidates.Top().ID)
		if err != nil {
			return nil, errors.Wrap(err, "calculate distance between candidate and query")
		}

		if !ok {
			continue
		}

		if dist > worstResultDistance {
			break
		}
		candidate := candidates.Pop()
		candidateNode := h.nodes[candidate.ID]
		if candidateNode == nil {
			// could have been a node that already had a tombstone attached and was
			// just cleaned up while we were waiting for a read lock
			continue
		}

		candidateNode.Lock()
		connections := candidateNode.connections[level]
		candidateNode.Unlock()

		for _, neighborID := range connections {

			if ok := visited.Visited(neighborID); ok {
				// skip if we've already visited this neighbor
				continue
			}

			// make sure we never visit this neighbor again
			visited.Visit(neighborID)

			distance, ok, err := h.distanceToNode(distancer, neighborID)
			if err != nil {
				return nil, errors.Wrap(err, "calculate distance between candidate and query")
			}

			if !ok {
				// node was deleted in the underlying object store
				continue
			}

			if distance < worstResultDistance || results.Len() < ef {
				candidates.Insert(neighborID, distance)
				if level == 0 && allowList != nil {
					// we are on the lowest level containing the actual candidates and we
					// have an allow list (i.e. the user has probably set some sort of a
					// filter restricting this search further. As a result we have to
					// ignore items not on the list
					if !allowList.Contains(neighborID) {
						continue
					}
				}

				if h.hasTombstone(neighborID) {
					continue
				}

				results.Insert(neighborID, distance)

				h.cache.prefetch(candidates.Top().ID)

				// +1 because we have added one node size calculating the len
				if results.Len() > ef {
					results.Pop()
				}

				if results.Len() > 0 {
					worstResultDistance = results.Top().Dist
				}
			}
		}
	}

	h.Lock()
	h.visitedListPool.Return(visited)
	h.Unlock()

	return results, nil
}

func (h *hnsw) insertViableEntrypointsAsCandidatesAndResults(
	entrypoints, candidates, results *priorityqueue.Queue, level int,
	visitedList *visited.List, allowList helpers.AllowList) {
	for entrypoints.Len() > 0 {
		ep := entrypoints.Pop()
		visitedList.Visit(ep.ID)
		candidates.Insert(ep.ID, ep.Dist)
		if level == 0 && allowList != nil {
			// we are on the lowest level containing the actual candidates and we
			// have an allow list (i.e. the user has probably set some sort of a
			// filter restricting this search further. As a result we have to
			// ignore items not on the list
			if !allowList.Contains(ep.ID) {
				continue
			}
		}

		if h.hasTombstone(ep.ID) {
			continue
		}

		results.Insert(ep.ID, ep.Dist)
	}
}

func (h *hnsw) currentWorstResultDistance(results *priorityqueue.Queue,
	distancer distancer.Distancer) (float32, error) {
	if results.Len() > 0 {
		id := results.Top().ID
		d, ok, err := h.distanceToNode(distancer, id)
		if err != nil {
			return 0, errors.Wrap(err,
				"calculated distance between worst result and query")
		}

		if !ok {
			return math.MaxFloat32, nil
		}
		return d, nil
	} else {
		// if the entrypoint (which we received from a higher layer doesn't match
		// the allow List the result list is empty. In this case we can just set
		// the worstDistance to an arbitrarily large number, so that any
		// (allowed) candidate will have a lower distance in comparison
		return math.MaxFloat32, nil
	}
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
	ef int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	if h.isEmpty() {
		return nil, nil, nil
	}

	entryPointID := h.entryPointID
	entryPointDistance, ok, err := h.distBetweenNodeAndVec(entryPointID, searchVec)
	if err != nil {
		return nil, nil, errors.Wrap(err, "knn search: distance between entrypint and query node")
	}

	if !ok {
		return nil, nil, fmt.Errorf("entrypoint was deleted in the object store, " +
			"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
	}

	// stop at layer 1, not 0!
	for level := h.currentMaximumLayer; level >= 1; level-- {
		eps := priorityqueue.NewMin(10)
		eps.Insert(entryPointID, entryPointDistance)
		res, err := h.searchLayerByVector(searchVec, eps, 1, level, nil)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "knn search: search layer at level %d", level)
		}

		// There might be situations where we did not find a better entrypoint at
		// that particular level, so instead we're keeping whatever entrypoint we
		// had before (i.e. either from a previous level or even the main
		// entrypoint)
		for res.Len() > 0 {
			cand := res.Pop()
			if !h.nodeByID(cand.ID).isUnderMaintenance() {
				entryPointID = cand.ID
				entryPointDistance = cand.Dist
				break
			}

			// if we managed to go through the loop without finding a single
			// suitable node, we simply stick with the original, i.e. the global
			// entrypoint
		}
	}

	eps := priorityqueue.NewMin(10)
	eps.Insert(entryPointID, entryPointDistance)
	res, err := h.searchLayerByVector(searchVec, eps, ef, 0, allowList)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "knn search: search layer at level %d", 0)
	}

	for res.Len() > k {
		res.Pop()
	}

	ids := make([]uint64, res.Len())
	dists := make([]float32, res.Len())

	// results is ordered in reverse, we need to flip the order before presenting
	// to the user!
	i := len(ids) - 1
	for res.Len() > 0 {
		res := res.Pop()
		ids[i] = res.ID
		dists[i] = res.Dist
		i--
	}

	return ids, dists, nil
}

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
	"context"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

func (h *hnsw) searchTimeEF(k int) int {
	// load atomically, so we can get away with concurrent updates of the
	// userconfig without having to set a lock each time we try to read - which
	// can be so common that it would cause considerable overhead
	ef := int(atomic.LoadInt64(&h.ef))
	if ef < 1 {
		return h.autoEfFromK(k)
	}

	if ef < k {
		ef = k
	}

	return ef
}

func (h *hnsw) autoEfFromK(k int) int {
	factor := int(atomic.LoadInt64(&h.efFactor))
	min := int(atomic.LoadInt64(&h.efMin))
	max := int(atomic.LoadInt64(&h.efMax))

	ef := k * factor
	if ef > max {
		ef = max
	} else if ef < min {
		ef = min
	}
	if k > ef {
		ef = k // otherwise results will get cut off early
	}

	return ef
}

func (h *hnsw) SearchByVector(vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	h.compressActionLock.RLock()
	defer h.compressActionLock.RUnlock()

	vector = h.normalizeVec(vector)
	flatSearchCutoff := int(atomic.LoadInt64(&h.flatSearchCutoff))
	if allowList != nil && !h.forbidFlat && allowList.Len() < flatSearchCutoff {
		return h.flatSearch(vector, k, allowList)
	}
	return h.knnSearchByVector(vector, k, h.searchTimeEF(k), allowList)
}

// SearchByVectorDistance wraps SearchByVector, and calls it recursively until
// the search results contain all vector within the threshold specified by the
// target distance.
//
// The maxLimit param will place an upper bound on the number of search results
// returned. This is used in situations where the results of the method are all
// eventually turned into objects, for example, a Get query. If the caller just
// needs ids for sake of something like aggregation, a maxLimit of -1 can be
// passed in to truly obtain all results from the vector index.
func (h *hnsw) SearchByVectorDistance(vector []float32, targetDistance float32, maxLimit int64,
	allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	var (
		searchParams = newSearchByDistParams(maxLimit)

		resultIDs  []uint64
		resultDist []float32
	)

	recursiveSearch := func() (bool, error) {
		shouldContinue := false

		ids, dist, err := h.SearchByVector(vector, searchParams.totalLimit, allowList)
		if err != nil {
			return false, errors.Wrap(err, "vector search")
		}

		// ensures the indexers aren't out of range
		offsetCap := searchParams.offsetCapacity(ids)
		totalLimitCap := searchParams.totalLimitCapacity(ids)

		ids, dist = ids[offsetCap:totalLimitCap], dist[offsetCap:totalLimitCap]

		if len(ids) == 0 {
			return false, nil
		}

		lastFound := dist[len(dist)-1]
		shouldContinue = lastFound <= targetDistance

		for i := range ids {
			if aboveThresh := dist[i] <= targetDistance; aboveThresh ||
				floatcomp.InDelta(float64(dist[i]), float64(targetDistance), 1e-6) {
				resultIDs = append(resultIDs, ids[i])
				resultDist = append(resultDist, dist[i])
			} else {
				// as soon as we encounter a certainty which
				// is below threshold, we can stop searching
				break
			}
		}

		return shouldContinue, nil
	}

	shouldContinue, err := recursiveSearch()
	if err != nil {
		return nil, nil, err
	}

	for shouldContinue {
		searchParams.iterate()
		if searchParams.maxLimitReached() {
			h.logger.
				WithField("action", "unlimited_vector_search").
				Warnf("maximum search limit of %d results has been reached",
					searchParams.maximumSearchLimit)
			break
		}

		shouldContinue, err = recursiveSearch()
		if err != nil {
			return nil, nil, err
		}
	}

	return resultIDs, resultDist, nil
}

func (h *hnsw) shouldRescore() bool {
	return h.compressed.Load() && !h.doNotRescore
}

func (h *hnsw) searchLayerByVector(queryVector []float32,
	entrypoints *priorityqueue.Queue[any], ef int, level int,
	allowList helpers.AllowList,
) (*priorityqueue.Queue[any], error,
) {
	var compressorDistancer compressionhelpers.CompressorDistancer
	if h.compressed.Load() {
		var returnFn compressionhelpers.ReturnDistancerFn
		compressorDistancer, returnFn = h.compressor.NewDistancer(queryVector)
		defer returnFn()
	}
	return h.searchLayerByVectorWithDistancer(queryVector, entrypoints, ef, level, allowList, compressorDistancer)
}

func (h *hnsw) searchLayerByVectorWithDistancer(queryVector []float32,
	entrypoints *priorityqueue.Queue[any], ef int, level int,
	allowList helpers.AllowList, compressorDistancer compressionhelpers.CompressorDistancer) (*priorityqueue.Queue[any], error,
) {
	h.pools.visitedListsLock.Lock()
	visited := h.pools.visitedLists.Borrow()
	h.pools.visitedListsLock.Unlock()

	candidates := h.pools.pqCandidates.GetMin(ef)
	results := h.pools.pqResults.GetMax(ef)
	var floatDistancer distancer.Distancer
	if h.compressed.Load() {
		if compressorDistancer == nil {
			var returnFn compressionhelpers.ReturnDistancerFn
			compressorDistancer, returnFn = h.compressor.NewDistancer(queryVector)
			defer returnFn()
		}
	} else {
		floatDistancer = h.distancerProvider.New(queryVector)
	}

	h.insertViableEntrypointsAsCandidatesAndResults(entrypoints, candidates,
		results, level, visited, allowList)

	var worstResultDistance float32
	var err error
	if h.compressed.Load() {
		worstResultDistance, err = h.currentWorstResultDistanceToByte(results, compressorDistancer)
	} else {
		worstResultDistance, err = h.currentWorstResultDistanceToFloat(results, floatDistancer)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "calculate distance of current last result")
	}
	connectionsReusable := make([]uint64, h.maximumConnectionsLayerZero)

	for candidates.Len() > 0 {
		var dist float32
		candidate := candidates.Pop()
		dist = candidate.Dist

		if dist > worstResultDistance && results.Len() >= ef {
			break
		}

		h.shardedNodeLocks.RLock(candidate.ID)
		candidateNode := h.nodes[candidate.ID]
		h.shardedNodeLocks.RUnlock(candidate.ID)

		if candidateNode == nil {
			// could have been a node that already had a tombstone attached and was
			// just cleaned up while we were waiting for a read lock
			continue
		}

		candidateNode.Lock()
		if candidateNode.level < level {
			// a node level could have been downgraded as part of a delete-reassign,
			// but the connections pointing to it not yet cleaned up. In this case
			// the node doesn't have any outgoing connections at this level and we
			// must discard it.
			candidateNode.Unlock()
			continue
		}

		if len(candidateNode.connections[level]) > h.maximumConnectionsLayerZero {
			// How is it possible that we could ever have more connections than the
			// allowed maximum? It is not anymore, but there was a bug that allowed
			// this to happen in versions prior to v1.12.0:
			// https://github.com/weaviate/weaviate/issues/1868
			//
			// As a result the length of this slice is entirely unpredictable and we
			// can no longer retrieve it from the pool. Instead we need to fallback
			// to allocating a new slice.
			//
			// This was discovered as part of
			// https://github.com/weaviate/weaviate/issues/1897
			connectionsReusable = make([]uint64, len(candidateNode.connections[level]))
		} else {
			connectionsReusable = connectionsReusable[:len(candidateNode.connections[level])]
		}

		copy(connectionsReusable, candidateNode.connections[level])
		candidateNode.Unlock()

		for _, neighborID := range connectionsReusable {

			if ok := visited.Visited(neighborID); ok {
				// skip if we've already visited this neighbor
				continue
			}

			// make sure we never visit this neighbor again
			visited.Visit(neighborID)
			var distance float32
			var ok bool
			var err error
			if h.compressed.Load() {
				distance, ok, err = compressorDistancer.DistanceToNode(neighborID)
			} else {
				distance, ok, err = h.distanceToFloatNode(floatDistancer, neighborID)
			}
			if err != nil {
				var e storobj.ErrNotFound
				if errors.As(err, &e) {
					h.handleDeletedNode(e.DocID)
				} else {
					if err != nil {
						return nil, errors.Wrap(err, "calculate distance between candidate and query")
					}
				}
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

				if h.compressed.Load() {
					h.compressor.Prefetch(candidates.Top().ID)
				} else {
					h.cache.Prefetch(candidates.Top().ID)
				}

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

	h.pools.pqCandidates.Put(candidates)

	h.pools.visitedListsLock.Lock()
	h.pools.visitedLists.Return(visited)
	h.pools.visitedListsLock.Unlock()

	return results, nil
}

func (h *hnsw) insertViableEntrypointsAsCandidatesAndResults(
	entrypoints, candidates, results *priorityqueue.Queue[any], level int,
	visitedList visited.ListSet, allowList helpers.AllowList,
) {
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

func (h *hnsw) currentWorstResultDistanceToFloat(results *priorityqueue.Queue[any],
	distancer distancer.Distancer,
) (float32, error) {
	if results.Len() > 0 {
		id := results.Top().ID

		d, ok, err := h.distanceToFloatNode(distancer, id)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID)
			} else {
				if err != nil {
					return 0, errors.Wrap(err, "calculated distance between worst result and query")
				}
			}
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

func (h *hnsw) currentWorstResultDistanceToByte(results *priorityqueue.Queue[any],
	distancer compressionhelpers.CompressorDistancer,
) (float32, error) {
	if results.Len() > 0 {
		item := results.Top()
		if item.Dist != 0 {
			return item.Dist, nil
		}
		id := item.ID
		d, ok, err := distancer.DistanceToNode(id)
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

func (h *hnsw) distanceFromBytesToFloatNode(concreteDistancer compressionhelpers.CompressorDistancer, nodeID uint64) (float32, bool, error) {
	slice := h.pools.tempVectors.Get(int(h.dims))
	defer h.pools.tempVectors.Put(slice)
	vec, err := h.TempVectorForIDThunk(context.Background(), nodeID, slice)
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
	vec = h.normalizeVec(vec)
	return concreteDistancer.DistanceToFloat(vec)
}

func (h *hnsw) distanceToFloatNode(distancer distancer.Distancer,
	nodeID uint64,
) (float32, bool, error) {
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
		Infof("found a deleted node (%d) without a tombstone, "+
			"tombstone was added", docID)
}

func (h *hnsw) knnSearchByVector(searchVec []float32, k int,
	ef int, allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	if h.isEmpty() {
		return nil, nil, nil
	}

	if k < 0 {
		return nil, nil, fmt.Errorf("k must be greater than zero")
	}

	h.RLock()
	entryPointID := h.entryPointID
	maxLayer := h.currentMaximumLayer
	h.RUnlock()

	entryPointDistance, ok, err := h.distBetweenNodeAndVec(entryPointID, searchVec)
	if err != nil {
		return nil, nil, errors.Wrap(err, "knn search: distance between entrypoint and query node")
	}

	if !ok {
		return nil, nil, fmt.Errorf("entrypoint was deleted in the object store, " +
			"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
	}

	var compressorDistancer compressionhelpers.CompressorDistancer
	if h.compressed.Load() {
		var returnFn compressionhelpers.ReturnDistancerFn
		compressorDistancer, returnFn = h.compressor.NewDistancer(searchVec)
		defer returnFn()
	}
	// stop at layer 1, not 0!
	for level := maxLayer; level >= 1; level-- {
		eps := priorityqueue.NewMin[any](10)
		eps.Insert(entryPointID, entryPointDistance)

		res, err := h.searchLayerByVectorWithDistancer(searchVec, eps, 1, level, nil, compressorDistancer)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "knn search: search layer at level %d", level)
		}

		// There might be situations where we did not find a better entrypoint at
		// that particular level, so instead we're keeping whatever entrypoint we
		// had before (i.e. either from a previous level or even the main
		// entrypoint)
		//
		// If we do, however, have results, any candidate that's not nil (not
		// deleted), and not under maintenance is a viable candidate
		for res.Len() > 0 {
			cand := res.Pop()
			n := h.nodeByID(cand.ID)
			if n == nil {
				// we have found a node in results that is nil. This means it was
				// deleted, but not cleaned up properly. Make sure to add a tombstone to
				// this node, so it can be cleaned up in the next cycle.
				if err := h.addTombstone(cand.ID); err != nil {
					return nil, nil, err
				}

				// skip the nil node, as it does not make a valid entrypoint
				continue
			}

			if !n.isUnderMaintenance() {
				entryPointID = cand.ID
				entryPointDistance = cand.Dist
				break
			}

			// if we managed to go through the loop without finding a single
			// suitable node, we simply stick with the original, i.e. the global
			// entrypoint
		}

		h.pools.pqResults.Put(res)
	}

	eps := priorityqueue.NewMin[any](10)
	eps.Insert(entryPointID, entryPointDistance)
	res, err := h.searchLayerByVectorWithDistancer(searchVec, eps, ef, 0, allowList, compressorDistancer)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "knn search: search layer at level %d", 0)
	}

	if h.shouldRescore() {
		ids := make([]uint64, res.Len())
		i := len(ids) - 1
		for res.Len() > 0 {
			res := res.Pop()
			ids[i] = res.ID
			i--
		}
		res.Reset()
		for _, id := range ids {
			dist, _, _ := h.distanceFromBytesToFloatNode(compressorDistancer, id)
			res.Insert(id, dist)
			if res.Len() > ef {
				res.Pop()
			}
		}

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
	h.pools.pqResults.Put(res)
	return ids, dists, nil
}

func newSearchByDistParams(maxLimit int64) *searchByDistParams {
	initialOffset := 0
	initialLimit := DefaultSearchByDistInitialLimit

	return &searchByDistParams{
		offset:             initialOffset,
		limit:              initialLimit,
		totalLimit:         initialOffset + initialLimit,
		maximumSearchLimit: maxLimit,
	}
}

const (
	// DefaultSearchByDistInitialLimit :
	// the initial limit of 100 here is an
	// arbitrary decision, and can be tuned
	// as needed
	DefaultSearchByDistInitialLimit = 100

	// DefaultSearchByDistLimitMultiplier :
	// the decision to increase the limit in
	// multiples of 10 here is an arbitrary
	// decision, and can be tuned as needed
	DefaultSearchByDistLimitMultiplier = 10
)

type searchByDistParams struct {
	offset             int
	limit              int
	totalLimit         int
	maximumSearchLimit int64
}

func (params *searchByDistParams) offsetCapacity(ids []uint64) int {
	var offsetCap int
	if params.offset < len(ids) {
		offsetCap = params.offset
	} else {
		offsetCap = len(ids)
	}

	return offsetCap
}

func (params *searchByDistParams) totalLimitCapacity(ids []uint64) int {
	var totalLimitCap int
	if params.totalLimit < len(ids) {
		totalLimitCap = params.totalLimit
	} else {
		totalLimitCap = len(ids)
	}

	return totalLimitCap
}

func (params *searchByDistParams) iterate() {
	params.offset = params.totalLimit
	params.limit *= DefaultSearchByDistLimitMultiplier
	params.totalLimit = params.offset + params.limit
}

func (params *searchByDistParams) maxLimitReached() bool {
	if params.maximumSearchLimit < 0 {
		return false
	}

	return int64(params.totalLimit) > params.maximumSearchLimit
}

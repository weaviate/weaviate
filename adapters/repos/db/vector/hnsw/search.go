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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

type FilterStrategy int

const (
	SWEEPING FilterStrategy = iota
	ACORN
	RRE
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

func (h *hnsw) SearchByVector(ctx context.Context, vector []float32,
	k int, allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	h.compressActionLock.RLock()
	defer h.compressActionLock.RUnlock()

	vector = h.normalizeVec(vector)
	flatSearchCutoff := int(atomic.LoadInt64(&h.flatSearchCutoff))
	if allowList != nil && !h.forbidFlat && allowList.Len() < flatSearchCutoff {
		helpers.AnnotateSlowQueryLog(ctx, "hnsw_flat_search", true)
		return h.flatSearch(ctx, vector, k, h.searchTimeEF(k), allowList)
	}
	helpers.AnnotateSlowQueryLog(ctx, "hnsw_flat_search", false)
	return h.knnSearchByVector(ctx, vector, k, h.searchTimeEF(k), allowList)
}

func (h *hnsw) SearchByMultiVector(ctx context.Context, vectors [][]float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	if !h.multivector.Load() {
		return nil, nil, errors.New("multivector search is not enabled")
	}

	if h.muvera.Load() {
		muvera_query := h.muveraEncoder.EncodeQuery(vectors)
		overfetch := 2
		docIDs, _, err := h.SearchByVector(ctx, muvera_query, overfetch*k, allowList)
		if err != nil {
			return nil, nil, err
		}
		candidateSet := make(map[uint64]struct{})
		for _, docID := range docIDs {
			candidateSet[docID] = struct{}{}
		}
		return h.computeLateInteraction(vectors, k, candidateSet)
	}

	h.compressActionLock.RLock()
	defer h.compressActionLock.RUnlock()

	vectors = h.normalizeVecs(vectors)
	flatSearchCutoff := int(atomic.LoadInt64(&h.flatSearchCutoff))
	if allowList != nil && !h.forbidFlat && allowList.Len() < flatSearchCutoff {
		helpers.AnnotateSlowQueryLog(ctx, "hnsw_flat_search", true)
		return h.flatMultiSearch(ctx, vectors, k, allowList)
	}
	helpers.AnnotateSlowQueryLog(ctx, "hnsw_flat_search", false)
	return h.knnSearchByMultiVector(ctx, vectors, k, allowList)
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
func (h *hnsw) SearchByVectorDistance(ctx context.Context, vector []float32,
	targetDistance float32, maxLimit int64,
	allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	return searchByVectorDistance(ctx, vector, targetDistance, maxLimit, allowList,
		h.SearchByVector, h.logger)
}

// SearchByMultiVectorDistance wraps SearchByMultiVector, and calls it recursively until
// the search results contain all vector within the threshold specified by the
// target distance.
//
// The maxLimit param will place an upper bound on the number of search results
// returned. This is used in situations where the results of the method are all
// eventually turned into objects, for example, a Get query. If the caller just
// needs ids for sake of something like aggregation, a maxLimit of -1 can be
// passed in to truly obtain all results from the vector index.
func (h *hnsw) SearchByMultiVectorDistance(ctx context.Context, vector [][]float32,
	targetDistance float32, maxLimit int64,
	allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	return searchByVectorDistance(ctx, vector, targetDistance, maxLimit, allowList,
		h.SearchByMultiVector, h.logger)
}

func (h *hnsw) shouldRescore() bool {
	return h.compressed.Load() && !h.doNotRescore
}

func (h *hnsw) cacheSize() int64 {
	var size int64
	if h.compressed.Load() {
		size = h.compressor.CountVectors()
	} else {
		size = h.cache.CountVectors()
	}
	return size
}

func (h *hnsw) acornEnabled(allowList helpers.AllowList) bool {
	if allowList == nil || !h.acornSearch.Load() {
		return false
	}

	cacheSize := h.cacheSize()
	allowListSize := allowList.Len()
	if cacheSize != 0 && float32(allowListSize)/float32(cacheSize) > float32(h.acornFilterRatio) {
		return false
	}

	return true
}

func (h *hnsw) searchLayerByVectorWithDistancer(ctx context.Context,
	queryVector []float32,
	entrypoints *priorityqueue.Queue[any], ef int, level int,
	allowList helpers.AllowList, compressorDistancer compressionhelpers.CompressorDistancer,
) (*priorityqueue.Queue[any], error,
) {
	if h.acornEnabled(allowList) {
		return h.searchLayerByVectorWithDistancerWithStrategy(ctx, queryVector, entrypoints, ef, level, allowList, compressorDistancer, ACORN)
	}
	return h.searchLayerByVectorWithDistancerWithStrategy(ctx, queryVector, entrypoints, ef, level, allowList, compressorDistancer, SWEEPING)
}

func (h *hnsw) searchLayerByVectorWithDistancerWithStrategy(ctx context.Context,
	queryVector []float32,
	entrypoints *priorityqueue.Queue[any], ef int, level int,
	allowList helpers.AllowList, compressorDistancer compressionhelpers.CompressorDistancer,
	strategy FilterStrategy) (*priorityqueue.Queue[any], error,
) {
	start := time.Now()
	defer func() {
		took := time.Since(start)
		helpers.AnnotateSlowQueryLog(ctx, fmt.Sprintf("knn_search_layer_%d_took", level), took)
	}()
	h.pools.visitedListsLock.RLock()
	visited := h.pools.visitedLists.Borrow()
	visitedExp := h.pools.visitedLists.Borrow()
	h.pools.visitedListsLock.RUnlock()

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

	isMultivec := h.multivector.Load()
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
	var connectionsReusable []uint64
	var sliceConnectionsReusable *common.VectorUint64Slice
	var slicePendingNextRound *common.VectorUint64Slice
	var slicePendingThisRound *common.VectorUint64Slice

	if allowList == nil {
		strategy = SWEEPING
	}
	if strategy == ACORN {
		sliceConnectionsReusable = h.pools.tempVectorsUint64.Get(8 * h.maximumConnectionsLayerZero)
		slicePendingNextRound = h.pools.tempVectorsUint64.Get(h.maximumConnectionsLayerZero)
		slicePendingThisRound = h.pools.tempVectorsUint64.Get(h.maximumConnectionsLayerZero)
	} else {
		connectionsReusable = make([]uint64, h.maximumConnectionsLayerZero)
	}

	for candidates.Len() > 0 {
		if err := ctx.Err(); err != nil {
			h.pools.visitedListsLock.RLock()
			h.pools.visitedLists.Return(visited)
			h.pools.visitedListsLock.RUnlock()

			helpers.AnnotateSlowQueryLog(ctx, "context_error", "knn_search_layer")
			return nil, err
		}
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

		if strategy != ACORN {
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
		} else {
			connectionsReusable = sliceConnectionsReusable.Slice
			pendingNextRound := slicePendingNextRound.Slice
			pendingThisRound := slicePendingThisRound.Slice

			realLen := 0
			index := 0

			pendingNextRound = pendingNextRound[:len(candidateNode.connections[level])]
			copy(pendingNextRound, candidateNode.connections[level])
			hop := 1
			maxHops := 2
			for hop <= maxHops && realLen < 8*h.maximumConnectionsLayerZero && len(pendingNextRound) > 0 {
				if cap(pendingThisRound) >= len(pendingNextRound) {
					pendingThisRound = pendingThisRound[:len(pendingNextRound)]
				} else {
					pendingThisRound = make([]uint64, len(pendingNextRound))
					slicePendingThisRound.Slice = pendingThisRound
				}
				copy(pendingThisRound, pendingNextRound)
				pendingNextRound = pendingNextRound[:0]
				for index < len(pendingThisRound) && realLen < 8*h.maximumConnectionsLayerZero {
					nodeId := pendingThisRound[index]
					index++
					if ok := visited.Visited(nodeId); ok {
						// skip if we've already visited this neighbor
						continue
					}
					if !visitedExp.Visited(nodeId) {
						if !isMultivec {
							if allowList.Contains(nodeId) {
								connectionsReusable[realLen] = nodeId
								realLen++
								visitedExp.Visit(nodeId)
								continue
							}
						} else {
							docID, _ := h.cache.GetKeys(nodeId)
							if allowList.Contains(docID) {
								connectionsReusable[realLen] = nodeId
								realLen++
								visitedExp.Visit(nodeId)
								continue
							}
						}
					} else {
						continue
					}
					visitedExp.Visit(nodeId)

					h.RLock()
					h.shardedNodeLocks.RLock(nodeId)
					node := h.nodes[nodeId]
					h.shardedNodeLocks.RUnlock(nodeId)
					h.RUnlock()
					if node == nil {
						continue
					}
					for _, expId := range node.connections[level] {
						if visitedExp.Visited(expId) {
							continue
						}
						if visited.Visited(expId) {
							continue
						}

						if realLen >= 8*h.maximumConnectionsLayerZero {
							break
						}

						if !isMultivec {
							if allowList.Contains(expId) {
								visitedExp.Visit(expId)
								connectionsReusable[realLen] = expId
								realLen++
							} else if hop < maxHops {
								visitedExp.Visit(expId)
								pendingNextRound = append(pendingNextRound, expId)
							}
						} else {
							docID, _ := h.cache.GetKeys(expId)
							if allowList.Contains(docID) {
								visitedExp.Visit(expId)
								connectionsReusable[realLen] = expId
								realLen++
							} else if hop < maxHops {
								visitedExp.Visit(expId)
								pendingNextRound = append(pendingNextRound, expId)
							}
						}
					}
				}
				hop++
			}
			slicePendingNextRound.Slice = pendingNextRound
			connectionsReusable = connectionsReusable[:realLen]
		}
		candidateNode.Unlock()

		for _, neighborID := range connectionsReusable {
			if ok := visited.Visited(neighborID); ok {
				// skip if we've already visited this neighbor
				continue
			}

			// make sure we never visit this neighbor again
			visited.Visit(neighborID)

			if strategy == RRE && level == 0 {
				if isMultivec {
					docID, _ := h.cache.GetKeys(neighborID)
					if !allowList.Contains(docID) {
						continue
					}
				} else if !allowList.Contains(neighborID) {
					continue
				}
			}
			var distance float32
			var err error
			if h.compressed.Load() {
				distance, err = compressorDistancer.DistanceToNode(neighborID)
			} else {
				distance, err = h.distanceToFloatNode(floatDistancer, neighborID)
			}
			if err != nil {
				var e storobj.ErrNotFound
				if errors.As(err, &e) {
					h.handleDeletedNode(e.DocID, "searchLayerByVectorWithDistancer")
					continue
				} else {
					h.pools.visitedListsLock.RLock()
					h.pools.visitedLists.Return(visited)
					h.pools.visitedLists.Return(visitedExp)
					h.pools.visitedListsLock.RUnlock()
					return nil, errors.Wrap(err, "calculate distance between candidate and query")
				}
			}

			if distance < worstResultDistance || results.Len() < ef {
				candidates.Insert(neighborID, distance)
				if strategy == SWEEPING && level == 0 && allowList != nil {
					// we are on the lowest level containing the actual candidates and we
					// have an allow list (i.e. the user has probably set some sort of a
					// filter restricting this search further. As a result we have to
					// ignore items not on the list
					if isMultivec {
						docID, _ := h.cache.GetKeys(neighborID)
						if !allowList.Contains(docID) {
							continue
						}
					} else if !allowList.Contains(neighborID) {
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

	if strategy == ACORN {
		h.pools.tempVectorsUint64.Put(sliceConnectionsReusable)
		h.pools.tempVectorsUint64.Put(slicePendingNextRound)
		h.pools.tempVectorsUint64.Put(slicePendingThisRound)
	}

	h.pools.pqCandidates.Put(candidates)

	h.pools.visitedListsLock.RLock()
	h.pools.visitedLists.Return(visited)
	h.pools.visitedLists.Return(visitedExp)
	h.pools.visitedListsLock.RUnlock()

	return results, nil
}

func (h *hnsw) insertViableEntrypointsAsCandidatesAndResults(
	entrypoints, candidates, results *priorityqueue.Queue[any], level int,
	visitedList visited.ListSet, allowList helpers.AllowList,
) {
	isMultivec := h.multivector.Load()
	for entrypoints.Len() > 0 {
		ep := entrypoints.Pop()
		visitedList.Visit(ep.ID)
		candidates.Insert(ep.ID, ep.Dist)
		if level == 0 && allowList != nil {
			// we are on the lowest level containing the actual candidates and we
			// have an allow list (i.e. the user has probably set some sort of a
			// filter restricting this search further. As a result we have to
			// ignore items not on the list
			if isMultivec {
				docID, _ := h.cache.GetKeys(ep.ID)
				if !allowList.Contains(docID) {
					continue
				}
			} else if !allowList.Contains(ep.ID) {
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

		d, err := h.distanceToFloatNode(distancer, id)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID, "currentWorstResultDistanceToFloat")
				return math.MaxFloat32, nil
			}
			return 0, errors.Wrap(err, "calculated distance between worst result and query")
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
		d, err := distancer.DistanceToNode(id)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID, "currentWorstResultDistanceToByte")
				return math.MaxFloat32, nil
			}
			return 0, errors.Wrap(err,
				"calculated distance between worst result and query")
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

func (h *hnsw) distanceFromBytesToFloatNode(concreteDistancer compressionhelpers.CompressorDistancer, nodeID uint64) (float32, error) {
	slice := h.pools.tempVectors.Get(int(h.dims))
	defer h.pools.tempVectors.Put(slice)
	var vec []float32
	var err error
	if h.muvera.Load() || !h.multivector.Load() {
		vec, err = h.TempVectorForIDThunk(context.Background(), nodeID, slice)
	} else {
		docID, relativeID := h.cache.GetKeys(nodeID)
		vecs, err := h.TempMultiVectorForIDThunk(context.Background(), docID, slice)
		if err != nil {
			return 0, err
		} else if len(vecs) <= int(relativeID) {
			return 0, errors.Errorf("relativeID %d is out of bounds for docID %d", relativeID, docID)
		}
		vec = vecs[relativeID]
	}
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID, "distanceFromBytesToFloatNode")
			return 0, err
		}
		// not a typed error, we can recover from, return with err
		return 0, errors.Wrapf(err, "get vector of docID %d", nodeID)
	}
	vec = h.normalizeVec(vec)
	return concreteDistancer.DistanceToFloat(vec)
}

func (h *hnsw) distanceToFloatNode(distancer distancer.Distancer, nodeID uint64) (float32, error) {
	candidateVec, err := h.vectorForID(context.Background(), nodeID)
	if err != nil {
		return 0, err
	}

	dist, err := distancer.Distance(candidateVec)
	if err != nil {
		return 0, errors.Wrap(err, "calculate distance between candidate and query")
	}

	return dist, nil
}

// the underlying object seems to have been deleted, to recover from
// this situation let's add a tombstone to the deleted object, so it
// will be cleaned up and skip this candidate in the current search
func (h *hnsw) handleDeletedNode(docID uint64, operation string) {
	if h.hasTombstone(docID) {
		// nothing to do, this node already has a tombstone, it will be cleaned up
		// in the next deletion cycle
		return
	}

	h.addTombstone(docID)
	h.metrics.AddUnexpectedTombstone(operation)
	h.logger.WithField("action", "attach_tombstone_to_deleted_node").
		WithField("node_id", docID).
		Debugf("found a deleted node (%d) without a tombstone, "+
			"tombstone was added", docID)
}

func (h *hnsw) knnSearchByVector(ctx context.Context, searchVec []float32, k int,
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

	var compressorDistancer compressionhelpers.CompressorDistancer
	if h.compressed.Load() {
		var returnFn compressionhelpers.ReturnDistancerFn
		compressorDistancer, returnFn = h.compressor.NewDistancer(searchVec)
		defer returnFn()
	}
	entryPointDistance, err := h.distToNode(compressorDistancer, entryPointID, searchVec)
	var e storobj.ErrNotFound
	if err != nil && errors.As(err, &e) {
		h.handleDeletedNode(e.DocID, "knnSearchByVector")
		return nil, nil, fmt.Errorf("entrypoint was deleted in the object store, " +
			"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
	}
	if err != nil {
		return nil, nil, errors.Wrap(err, "knn search: distance between entrypoint and query node")
	}

	// stop at layer 1, not 0!
	for level := maxLayer; level >= 1; level-- {
		eps := priorityqueue.NewMin[any](10)
		eps.Insert(entryPointID, entryPointDistance)

		res, err := h.searchLayerByVectorWithDistancer(ctx, searchVec, eps, 1, level, nil, compressorDistancer)
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
	var strategy FilterStrategy
	h.shardedNodeLocks.RLock(entryPointID)
	entryPointNode := h.nodes[entryPointID]
	h.shardedNodeLocks.RUnlock(entryPointID)
	useAcorn := h.acornEnabled(allowList)
	isMultivec := h.multivector.Load()
	if useAcorn {
		if entryPointNode == nil {
			strategy = RRE
		} else {
			counter := float32(0)
			entryPointNode.Lock()
			if len(entryPointNode.connections) < 1 {
				strategy = ACORN
			} else {
				for _, id := range entryPointNode.connections[0] {
					if isMultivec {
						id, _ = h.cache.GetKeys(id)
					}
					if allowList.Contains(id) {
						counter++
					}
				}
				entryPointNode.Unlock()
				if counter/float32(len(h.nodes[entryPointID].connections[0])) > float32(h.acornFilterRatio) {
					strategy = RRE
				} else {
					strategy = ACORN
				}
			}
		}
	} else {
		strategy = SWEEPING
	}

	if allowList != nil && useAcorn {
		it := allowList.Iterator()
		idx, ok := it.Next()
		h.shardedNodeLocks.RLockAll()
		if !isMultivec {
			for ok && h.nodes[idx] == nil && h.hasTombstone(idx) {
				idx, ok = it.Next()
			}
		} else {
			_, exists := h.docIDVectors[idx]
			for ok && !exists {
				idx, ok = it.Next()
				_, exists = h.docIDVectors[idx]
			}
		}
		h.shardedNodeLocks.RUnlockAll()

		entryPointDistance, _ := h.distToNode(compressorDistancer, idx, searchVec)
		eps.Insert(idx, entryPointDistance)
	}
	res, err := h.searchLayerByVectorWithDistancerWithStrategy(ctx, searchVec, eps, ef, 0, allowList, compressorDistancer, strategy)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "knn search: search layer at level %d", 0)
	}

	beforeRescore := time.Now()
	if h.shouldRescore() && !h.multivector.Load() {
		if err := h.rescore(ctx, res, k, compressorDistancer); err != nil {
			helpers.AnnotateSlowQueryLog(ctx, "context_error", "knn_search_rescore")
			took := time.Since(beforeRescore)
			helpers.AnnotateSlowQueryLog(ctx, "knn_search_rescore_took", took)
			return nil, nil, fmt.Errorf("knn search:  %w", err)
		}
		took := time.Since(beforeRescore)
		helpers.AnnotateSlowQueryLog(ctx, "knn_search_rescore_took", took)
	}

	if !h.multivector.Load() {
		for res.Len() > k {
			res.Pop()
		}
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

func (h *hnsw) knnSearchByMultiVector(ctx context.Context, queryVectors [][]float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	kPrime := k
	candidateSet := make(map[uint64]struct{})
	for _, vec := range queryVectors {
		ids, _, err := h.knnSearchByVector(ctx, vec, kPrime, h.searchTimeEF(kPrime), allowList)
		if err != nil {
			return nil, nil, err
		}
		for _, id := range ids {
			var docId uint64
			if !h.compressed.Load() {
				docId, _ = h.cache.GetKeys(id)
			} else {
				docId, _ = h.compressor.GetKeys(id)
			}
			candidateSet[docId] = struct{}{}
		}
	}
	return h.computeLateInteraction(queryVectors, k, candidateSet)
}

func (h *hnsw) computeLateInteraction(queryVectors [][]float32, k int, candidateSet map[uint64]struct{}) ([]uint64, []float32, error) {
	resultsQueue := priorityqueue.NewMax[any](1)
	for docID := range candidateSet {
		sim, err := h.computeScore(queryVectors, docID)
		if err != nil {
			return nil, nil, err
		}
		resultsQueue.Insert(docID, sim)
		if resultsQueue.Len() > k {
			resultsQueue.Pop()
		}
	}

	distances := make([]float32, resultsQueue.Len())
	ids := make([]uint64, resultsQueue.Len())

	i := len(ids) - 1
	for resultsQueue.Len() > 0 {
		element := resultsQueue.Pop()
		ids[i] = element.ID
		distances[i] = element.Dist
		i--
	}

	return ids, distances, nil
}

func (h *hnsw) computeScore(searchVecs [][]float32, docID uint64) (float32, error) {
	h.RLock()
	vecIDs := h.docIDVectors[docID]
	h.RUnlock()
	var docVecs [][]float32
	if h.compressed.Load() {
		slice := h.pools.tempVectors.Get(int(h.dims))
		var err error
		docVecs, err = h.TempMultiVectorForIDThunk(context.Background(), docID, slice)
		if err != nil {
			return 0.0, errors.Wrap(err, "get vector for docID")
		}
		h.pools.tempVectors.Put(slice)
	} else {
		if !h.muvera.Load() {
			var errs []error
			docVecs, errs = h.multiVectorForID(context.Background(), vecIDs)
			for _, err := range errs {
				if err != nil {
					return 0.0, errors.Wrap(err, "get vector for docID")
				}
			}
		} else {
			var err error
			docVecs, err = h.cache.GetDoc(context.Background(), docID)
			if err != nil {
				return 0.0, errors.Wrap(err, "get muvera vector for docID")
			}
		}
	}

	similarity := float32(0.0)

	var distancer distancer.Distancer
	for _, searchVec := range searchVecs {
		maxSim := float32(math.MaxFloat32)
		distancer = h.multiDistancerProvider.New(searchVec)

		for _, docVec := range docVecs {
			dist, err := distancer.Distance(docVec)
			if err != nil {
				return 0.0, errors.Wrap(err, "calculate distance between candidate and query")
			}
			if dist < maxSim {
				maxSim = dist
			}
		}

		similarity += maxSim
	}

	return similarity, nil
}

func (h *hnsw) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	queryVector = h.normalizeVec(queryVector)
	if h.compressed.Load() {
		dist, returnFn := h.compressor.NewDistancer(queryVector)
		f := func(nodeID uint64) (float32, error) {
			if int(nodeID) > len(h.nodes) {
				return -1, fmt.Errorf("node %v is larger than the cache size %v", nodeID, len(h.nodes))
			}

			return dist.DistanceToNode(nodeID)
		}
		return common.QueryVectorDistancer{DistanceFunc: f, CloseFunc: returnFn}

	} else {
		distancer := h.distancerProvider.New(queryVector)
		f := func(nodeID uint64) (float32, error) {
			if int(nodeID) > len(h.nodes) {
				return -1, fmt.Errorf("node %v is larger than the cache size %v", nodeID, len(h.nodes))
			}
			return h.distanceToFloatNode(distancer, nodeID)
		}
		return common.QueryVectorDistancer{DistanceFunc: f}
	}
}

func (h *hnsw) QueryMultiVectorDistancer(queryVector [][]float32) common.QueryVectorDistancer {
	queryVector = h.normalizeVecs(queryVector)
	f := func(docID uint64) (float32, error) {
		h.RLock()
		_, ok := h.docIDVectors[docID]
		h.RUnlock()
		if !ok {
			return -1, fmt.Errorf("docID %v is not in the vector index", docID)
		}
		return h.computeScore(queryVector, docID)
	}
	return common.QueryVectorDistancer{DistanceFunc: f}
}

func (h *hnsw) rescore(ctx context.Context, res *priorityqueue.Queue[any], k int, compressorDistancer compressionhelpers.CompressorDistancer) error {
	if h.sqConfig.Enabled && h.sqConfig.RescoreLimit >= k {
		for res.Len() > h.sqConfig.RescoreLimit {
			res.Pop()
		}
	}
	ids := make([]uint64, res.Len())
	i := len(ids) - 1
	for res.Len() > 0 {
		res := res.Pop()
		ids[i] = res.ID
		i--
	}
	res.Reset()

	mu := sync.Mutex{} // protect res
	addID := func(id uint64, dist float32) {
		mu.Lock()
		defer mu.Unlock()

		res.Insert(id, dist)
		if res.Len() > k {
			res.Pop()
		}
	}

	eg := enterrors.NewErrorGroupWrapper(h.logger)
	for workerID := 0; workerID < h.rescoreConcurrency; workerID++ {
		workerID := workerID

		eg.Go(func() error {
			for idPos := workerID; idPos < len(ids); idPos += h.rescoreConcurrency {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("rescore: %w", err)
				}

				id := ids[idPos]
				dist, err := h.distanceFromBytesToFloatNode(compressorDistancer, id)
				if err == nil {
					addID(id, dist)
				} else {
					h.logger.
						WithField("action", "rescore").
						WithError(err).
						Warnf("could not rescore node %d", id)
				}
			}
			return nil
		}, h.logger)
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
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

func searchByVectorDistance[T dto.Embedding](ctx context.Context, vector T,
	targetDistance float32, maxLimit int64,
	allowList helpers.AllowList,
	searchByVector func(context.Context, T, int, helpers.AllowList) ([]uint64, []float32, error),
	logger logrus.FieldLogger,
) ([]uint64, []float32, error) {
	var (
		searchParams = newSearchByDistParams(maxLimit)

		resultIDs  []uint64
		resultDist []float32
	)

	recursiveSearch := func() (bool, error) {
		shouldContinue := false

		ids, dist, err := searchByVector(ctx, vector, searchParams.totalLimit, allowList)
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
			logger.
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

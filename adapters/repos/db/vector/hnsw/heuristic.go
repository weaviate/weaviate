//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (h *hnsw) selectNeighborsHeuristic(input *priorityqueue.Queue,
	max int, denyList helpers.AllowList,
) error {
	if input.Len() < max {
		return nil
	}

	// TODO, if this solution stays we might need something with fewer allocs
	ids := make([]uint64, input.Len())

	closestFirst := h.pools.pqHeuristic.GetMin(input.Len())
	i := uint64(0)
	for input.Len() > 0 {
		elem := input.Pop()
		closestFirst.Insert(elem.ID, i, elem.Dist)
		ids[i] = elem.ID
		i++
	}

	var returnList []priorityqueue.ItemWithIndex

	if h.compressed.Load() {
		vecs := make([][]byte, 0, len(ids))
		for _, id := range ids {
			v, err := h.compressedVectorsCache.get(context.Background(), id)
			if err != nil {
				return err
			}
			vecs = append(vecs, v)
		}

		returnList = h.pools.pqItemSlice.Get().([]priorityqueue.ItemWithIndex)

		for closestFirst.Len() > 0 && len(returnList) < max {
			curr := closestFirst.Pop()
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			distToQuery := curr.Dist

			currVec := vecs[curr.Index]
			good := true
			for _, item := range returnList {
				peerDist := h.pq.DistanceBetweenCompressedVectors(currVec, vecs[item.Index])

				if peerDist < distToQuery {
					good = false
					break
				}
			}

			if good {
				returnList = append(returnList, curr)
			}

		}
	} else {

		vecs, errs := h.multiVectorForID(context.TODO(), ids)

		returnList = h.pools.pqItemSlice.Get().([]priorityqueue.ItemWithIndex)

		for closestFirst.Len() > 0 && len(returnList) < max {
			curr := closestFirst.Pop()
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			distToQuery := curr.Dist

			currVec := vecs[curr.Index]
			if err := errs[curr.Index]; err != nil {
				var e storobj.ErrNotFound
				if errors.As(err, &e) {
					h.handleDeletedNode(e.DocID)
					continue
				} else {
					// not a typed error, we can recover from, return with err
					return errors.Wrapf(err,
						"unrecoverable error for docID %d", curr.ID)
				}
			}
			good := true
			for _, item := range returnList {
				peerDist, _, _ := h.distancerProvider.SingleDist(currVec,
					vecs[item.Index])

				if peerDist < distToQuery {
					good = false
					break
				}
			}

			if good {
				returnList = append(returnList, curr)
			}

		}
	}

	h.pools.pqHeuristic.Put(closestFirst)

	for _, retElem := range returnList {
		input.Insert(retElem.ID, retElem.Dist)
	}

	// rewind and return to pool
	returnList = returnList[:0]

	// nolint:staticcheck
	h.pools.pqItemSlice.Put(returnList)

	return nil
}
func (h *hnsw) filteredRobustPrune(input *priorityqueue.Queue,
	max int, nodeFilters map[int]int, denyList helpers.AllowList,
) error {
	if input.Len() < max {
		return nil
	}

	// TODO, if this solution stays we might need something with fewer allocs
	ids := make([]uint64, input.Len())

	closestFirst := h.pools.pqHeuristic.GetMin(input.Len())
	i := uint64(0)
	for input.Len() > 0 {
		elem := input.Pop()
		closestFirst.Insert(elem.ID, i, elem.Dist)
		ids[i] = elem.ID
		i++
	}

	var returnList []priorityqueue.ItemWithIndex

	if h.compressed.Load() {
		vecs := make([][]byte, 0, len(ids))
		for _, id := range ids {
			v, err := h.compressedVectorsCache.get(context.Background(), id)
			if err != nil {
				return err
			}
			vecs = append(vecs, v)
		}

		returnList = h.pools.pqItemSlice.Get().([]priorityqueue.ItemWithIndex)

		for closestFirst.Len() > 0 && len(returnList) < max {
			curr := closestFirst.Pop()
			currFilters := h.nodes[curr.ID].filters
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			distToQuery := curr.Dist

			currVec := vecs[curr.Index]
			good := true
			for _, item := range returnList {
				peerDist := h.pq.DistanceBetweenCompressedVectors(currVec, vecs[item.Index])
				peerFilters := h.nodes[item.ID].filters

				// populate intersection
				peer_query_intersection := computeIntersection(nodeFilters, currFilters)

				if !intersectIsNull(peer_query_intersection, peerFilters) {
					break // good remains true
				}

				if peerDist < distToQuery {
					good = false
					break
				}
			}

			if good {
				returnList = append(returnList, curr)
			}

		}
	} else {

		vecs, errs := h.multiVectorForID(context.TODO(), ids)

		// Future --> node_filters := make([]map[int]int, len(vecs))
		// peerFilters := make([]map[int]int, len(vecs))
		/*
			for _, id := range ids {
				filters = append(filters, h.nodes[id].filter)
			}
		*/

		returnList = h.pools.pqItemSlice.Get().([]priorityqueue.ItemWithIndex)

		for closestFirst.Len() > 0 && len(returnList) < max {
			curr := closestFirst.Pop()
			currFilters := h.nodes[curr.ID].filters
			if denyList != nil && denyList.Contains(curr.ID) {
				continue
			}
			distToQuery := curr.Dist

			currVec := vecs[curr.Index]
			if err := errs[curr.Index]; err != nil {
				var e storobj.ErrNotFound
				if errors.As(err, &e) {
					h.handleDeletedNode(e.DocID)
					continue
				} else {
					// not a typed error, we can recover from, return with err
					return errors.Wrapf(err,
						"unrecoverable error for docID %d", curr.ID)
				}
			}
			good := true
			// if currFilter == filter, good to add to returnList
			for _, item := range returnList {
				peerDist, _, _ := h.distancerProvider.SingleDist(currVec,
					vecs[item.Index])
				// do I need to lock this?
				peerFilters := h.nodes[item.ID].filters

				// populate intersection
				peer_query_intersection := computeIntersection(nodeFilters, currFilters)

				if !intersectIsNull(peer_query_intersection, peerFilters) {
					break // good remains true
				}

				if peerDist < distToQuery {
					good = false
					break
				}
			}

			if good {
				returnList = append(returnList, curr)
			}

		}
	}

	h.pools.pqHeuristic.Put(closestFirst)

	for _, retElem := range returnList {
		input.Insert(retElem.ID, retElem.Dist)
	}

	// rewind and return to pool
	returnList = returnList[:0]

	// nolint:staticcheck
	h.pools.pqItemSlice.Put(returnList)

	return nil
}

func computeIntersection(a, b map[int]int) map[int]int {
	intersection := make(map[int]int)
	for k := range a {
		if bVal, ok := b[k]; ok {
			if bVal == a[k] {
				intersection[k] = a[k]
			}
		}
	}
	return intersection
}

func intersectIsNull(a, b map[int]int) bool {
	for k := range a {
		if bVal, ok := b[k]; ok {
			if bVal == a[k] {
				return false
			}
		}
	}
	return true
}

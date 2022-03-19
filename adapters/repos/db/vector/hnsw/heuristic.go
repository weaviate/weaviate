//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
)

func (h *hnsw) selectNeighborsHeuristic(input *priorityqueue.Queue,
	max int, denyList helpers.AllowList) error {
	if input.Len() < max {
		return nil
	}

	closestFirst := h.pools.pqHeuristic.GetMin(input.Len())
	for input.Len() > 0 {
		elem := input.Pop()
		closestFirst.Insert(elem.ID, elem.Dist)
	}

	returnList := h.pools.pqItemSlice.Get().([]priorityqueue.Item)

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
			returnList = append(returnList, curr)
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

func (h *hnsw) selectNeighborsHeuristicWithStatistics(input *priorityqueue.Queue,
	max int, denyList helpers.AllowList, statistics map[uint64]int) error {
	if input.Len() < max {
		return nil
	}

	localVectorCache := map[uint64][]float32{}

	// TODO, if this solution stays we need somethign with fewer allocs
	ids := make([]uint64, 0, input.Len())

	closestFirst := h.pools.pqHeuristic.GetMin(input.Len())
	for input.Len() > 0 {
		elem := input.Pop()
		closestFirst.Insert(elem.ID, elem.Dist)
		ids = append(ids, elem.ID)
	}

	vecs, err := h.multiVectorForID(context.TODO(), ids)
	if err != nil {
		return err
	}

	for i, id := range ids {
		localVectorCache[id] = vecs[i]
	}

	returnList := h.pools.pqItemSlice.Get().([]priorityqueue.Item)

	for closestFirst.Len() > 0 && len(returnList) < max {
		curr := closestFirst.Pop()
		if denyList != nil && denyList.Contains(curr.ID) {
			continue
		}
		distToQuery := curr.Dist

		good := true
		for _, item := range returnList {

			// statistics[curr.ID]++
			// statistics[item.ID]++
			// peerDist, ok, err := h.distBetweenNodes(curr.ID, item.ID)
			// if err != nil {
			// 	return errors.Wrapf(err, "distance between %d and %d", curr.ID, item.ID)
			// }

			// current distance provider implementations cannot fail
			peerDist, _, _ := h.distancerProvider.SingleDist(localVectorCache[curr.ID],
				localVectorCache[item.ID])

			// if !ok {
			// 	continue
			// }

			if peerDist < distToQuery {
				good = false
				break
			}
		}

		if good {
			returnList = append(returnList, curr)
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

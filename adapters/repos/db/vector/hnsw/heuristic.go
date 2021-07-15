package hnsw

import (
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
	h.pools.pqItemSlice.Put(returnList)

	return nil
}

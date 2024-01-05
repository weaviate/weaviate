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
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
)

func (h *hnsw) flatSearch(queryVector []float32, limit int,
	allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	results := priorityqueue.NewMax[any](limit)

	it := allowList.Iterator()
	for candidate, ok := it.Next(); ok; candidate, ok = it.Next() {
		h.RLock()
		// Hot fix for https://github.com/weaviate/weaviate/issues/1937
		// this if statement mitigates the problem but it doesn't resolve the issue
		if candidate >= uint64(len(h.nodes)) {
			h.logger.WithField("action", "flatSearch").
				Warnf("trying to get candidate: %v but we only have: %v elements.",
					candidate, len(h.nodes))
			h.RUnlock()
			continue
		}
		if len(h.nodes) <= int(candidate) { // if index hasn't grown yet for a newly inserted node
			continue
		}

		h.shardedNodeLocks.RLock(candidate)
		c := h.nodes[candidate]
		h.shardedNodeLocks.RUnlock(candidate)

		if c == nil || h.hasTombstone(candidate) {
			h.RUnlock()
			continue
		}
		h.RUnlock()
		dist, ok, err := h.distBetweenNodeAndVec(candidate, queryVector)
		if err != nil {
			return nil, nil, err
		}

		if !ok {
			// deleted node, ignore
			continue
		}

		if results.Len() < limit {
			results.Insert(candidate, dist)
		} else if results.Top().Dist > dist {
			results.Pop()
			results.Insert(candidate, dist)
		}
	}

	ids := make([]uint64, results.Len())
	dists := make([]float32, results.Len())

	// results is ordered in reverse, we need to flip the order before presenting
	// to the user!
	i := len(ids) - 1
	for results.Len() > 0 {
		res := results.Pop()
		ids[i] = res.ID
		dists[i] = res.Dist
		i--
	}

	return ids, dists, nil
}

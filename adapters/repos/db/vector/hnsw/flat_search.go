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
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
)

func (h *hnsw) flatSearch(queryVector []float32, limit int,
	allowList helpers.AllowList) ([]uint64, []float32, error) {
	results := priorityqueue.NewMax(limit)

	for candidate := range allowList {
		h.Lock()
		c := h.nodes[candidate]
		if c == nil || h.hasTombstone(candidate) {
			h.Unlock()
			continue
		}
		h.Unlock()
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

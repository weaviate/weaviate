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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (h *hnsw) KnnSearchByVectorMaxDist(ctx context.Context, searchVec []float32,
	dist float32, ef int, allowList helpers.AllowList,
) ([]uint64, error) {
	entryPointID := h.entryPointID
	var compressorDistancer compressionhelpers.CompressorDistancer
	if h.compressed.Load() {
		var returnFn compressionhelpers.ReturnDistancerFn
		compressorDistancer, returnFn = h.compressor.NewDistancer(searchVec)
		defer returnFn()
	}
	entryPointDistance, err := h.distToNode(compressorDistancer, entryPointID, searchVec)
	var e storobj.ErrNotFound
	if err != nil && errors.As(err, &e) {
		h.handleDeletedNode(e.DocID, "KnnSearchByVectorMaxDist")
		return nil, fmt.Errorf("entrypoint was deleted in the object store, " +
			"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
	}
	if err != nil {
		return nil, errors.Wrap(err, "knn search: distance between entrypoint and query node")
	}

	// stop at layer 1, not 0!
	for level := h.currentMaximumLayer; level >= 1; level-- {
		eps := priorityqueue.NewMin[any](1)
		eps.Insert(entryPointID, entryPointDistance)
		// ignore allowList on layers > 0
		res, err := h.searchLayerByVectorWithDistancer(ctx, searchVec, eps, 1, level, nil, compressorDistancer)
		if err != nil {
			return nil, errors.Wrapf(err, "knn search: search layer at level %d", level)
		}
		if res.Len() > 0 {
			best := res.Pop()
			entryPointID = best.ID
			entryPointDistance = best.Dist
		}

		h.pools.pqResults.Put(res)
	}

	eps := priorityqueue.NewMin[any](1)
	eps.Insert(entryPointID, entryPointDistance)
	res, err := h.searchLayerByVectorWithDistancer(ctx, searchVec, eps, ef, 0, allowList, compressorDistancer)
	if err != nil {
		return nil, errors.Wrapf(err, "knn search: search layer at level %d", 0)
	}

	all := make([]priorityqueue.Item[any], res.Len())
	i := res.Len() - 1
	for res.Len() > 0 {
		all[i] = res.Pop()
		i--
	}

	out := make([]uint64, len(all))
	i = 0
	for _, elem := range all {
		if elem.Dist > dist {
			break
		}
		out[i] = elem.ID
		i++
	}

	h.pools.pqResults.Put(res)
	return out[:i], nil
}

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

package aggregator

import (
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
)

func (a *Aggregator) vectorSearch(allow helpers.AllowList) (ids []uint64, err error) {
	if a.params.ObjectLimit != nil {
		ids, err = a.searchByVector(a.params.SearchVector, a.params.ObjectLimit, allow)
		return
	}

	ids, err = a.searchByVectorDistance(a.params.SearchVector, allow)
	return
}

func (a *Aggregator) searchByVector(searchVector []float32, limit *int, ids helpers.AllowList) ([]uint64, error) {
	idsFound, dists, err := a.vectorIndex.SearchByVector(searchVector, *limit, ids)
	if err != nil {
		return idsFound, err
	}

	if a.params.Certainty > 0 {
		targetDist := float32(1-a.params.Certainty) * 2

		i := 0
		for _, dist := range dists {
			if dist > targetDist {
				break
			}
			i++
		}

		return idsFound[:i], nil

	}
	return idsFound, nil
}

func (a *Aggregator) searchByVectorDistance(searchVector []float32, ids helpers.AllowList) ([]uint64, error) {
	if a.params.Certainty <= 0 {
		return nil, errors.New("must provide certainty or objectLimit with vector search")
	}

	targetDist := float32(1-a.params.Certainty) * 2
	idsFound, _, err := a.vectorIndex.SearchByVectorDistance(searchVector, targetDist, -1, ids)
	if err != nil {
		return nil, errors.Wrap(err, "aggregate search by vector")
	}

	return idsFound, nil
}

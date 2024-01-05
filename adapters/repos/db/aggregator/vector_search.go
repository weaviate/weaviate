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

package aggregator

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (a *Aggregator) vectorSearch(allow helpers.AllowList, vec []float32) ([]uint64, []float32, error) {
	if a.params.ObjectLimit != nil {
		return a.searchByVector(vec, a.params.ObjectLimit, allow)
	}

	return a.searchByVectorDistance(vec, allow)
}

func (a *Aggregator) searchByVector(searchVector []float32, limit *int, ids helpers.AllowList) ([]uint64, []float32, error) {
	idsFound, dists, err := a.vectorIndex.SearchByVector(searchVector, *limit, ids)
	if err != nil {
		return idsFound, nil, err
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

		return idsFound[:i], dists, nil

	}
	return idsFound, dists, nil
}

func (a *Aggregator) searchByVectorDistance(searchVector []float32, ids helpers.AllowList) ([]uint64, []float32, error) {
	if a.params.Certainty <= 0 {
		return nil, nil, fmt.Errorf("must provide certainty or objectLimit with vector search")
	}

	targetDist := float32(1-a.params.Certainty) * 2
	idsFound, dists, err := a.vectorIndex.SearchByVectorDistance(searchVector, targetDist, -1, ids)
	if err != nil {
		return nil, nil, fmt.Errorf("aggregate search by vector: %w", err)
	}

	return idsFound, dists, nil
}

func (a *Aggregator) objectVectorSearch(searchVector []float32,
	allowList helpers.AllowList,
) ([]*storobj.Object, []float32, error) {
	ids, dists, err := a.vectorSearch(allowList, searchVector)
	if err != nil {
		return nil, nil, err
	}

	bucket := a.store.Bucket(helpers.ObjectsBucketLSM)
	objs, err := storobj.ObjectsByDocID(bucket, ids, additional.Properties{})
	if err != nil {
		return nil, nil, fmt.Errorf("get objects by doc id: %w", err)
	}
	return objs, dists, nil
}

func (a *Aggregator) buildAllowList(ctx context.Context) (helpers.AllowList, error) {
	var (
		allow helpers.AllowList
		err   error
	)

	if a.params.Filters != nil {
		s := a.getSchema.GetSchemaSkipAuth()
		allow, err = inverted.NewSearcher(a.logger, a.store, s, nil,
			a.classSearcher, a.stopwords, a.shardVersion, a.isFallbackToSearchable,
			a.tenant, a.nestedCrossRefLimit).
			DocIDs(ctx, a.params.Filters, additional.Properties{},
				a.params.ClassName)
		if err != nil {
			return nil, fmt.Errorf("retrieve doc IDs from searcher: %w", err)
		}
	}

	return allow, nil
}

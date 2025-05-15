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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (a *Aggregator) vectorSearch(ctx context.Context, allow helpers.AllowList, vec models.Vector) ([]uint64, []float32, error) {
	if a.params.ObjectLimit != nil {
		return a.searchByVector(ctx, vec, a.params.ObjectLimit, allow)
	}

	return a.searchByVectorDistance(ctx, vec, allow)
}

func (a *Aggregator) searchByVector(ctx context.Context, searchVector models.Vector, limit *int, ids helpers.AllowList) ([]uint64, []float32, error) {
	idsFound, dists, err := a.performVectorSearch(ctx, searchVector, *limit, ids)
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

func (a *Aggregator) searchByVectorDistance(ctx context.Context, searchVector models.Vector, ids helpers.AllowList) ([]uint64, []float32, error) {
	if a.params.Certainty <= 0 {
		return nil, nil, fmt.Errorf("must provide certainty or objectLimit with vector search")
	}

	targetDist := float32(1-a.params.Certainty) * 2
	idsFound, dists, err := a.performVectorDistanceSearch(ctx, searchVector, targetDist, -1, ids)
	if err != nil {
		return nil, nil, fmt.Errorf("aggregate search by vector: %w", err)
	}

	return idsFound, dists, nil
}

func (a *Aggregator) objectVectorSearch(ctx context.Context, searchVector models.Vector,
	allowList helpers.AllowList,
) ([]*storobj.Object, []float32, error) {
	ids, dists, err := a.vectorSearch(ctx, allowList, searchVector)
	if err != nil {
		return nil, nil, err
	}

	bucket := a.store.Bucket(helpers.ObjectsBucketLSM)
	objs, err := storobj.ObjectsByDocID(bucket, ids, additional.Properties{}, nil, a.logger)
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
		allow, err = inverted.NewSearcher(a.logger, a.store, a.getSchema.ReadOnlyClass, nil,
			a.classSearcher, a.stopwords, a.shardVersion, a.isFallbackToSearchable,
			a.tenant, a.nestedCrossRefLimit, a.bitmapFactory).
			DocIDs(ctx, a.params.Filters, additional.Properties{},
				a.params.ClassName)
		if err != nil {
			return nil, fmt.Errorf("retrieve doc IDs from searcher: %w", err)
		}
	}

	return allow, nil
}

func (a *Aggregator) performVectorSearch(ctx context.Context,
	searchVector models.Vector, limit int, ids helpers.AllowList,
) ([]uint64, []float32, error) {
	switch vec := searchVector.(type) {
	case []float32:
		idsFound, dists, err := a.vectorIndex.SearchByVector(ctx, vec, limit, ids)
		if err != nil {
			return idsFound, nil, err
		}
		return idsFound, dists, nil
	case [][]float32:
		idsFound, dists, err := a.vectorIndex.SearchByMultiVector(ctx, vec, limit, ids)
		if err != nil {
			return idsFound, nil, err
		}
		return idsFound, dists, nil
	default:
		return nil, nil, fmt.Errorf("perform vector search: unrecognized search vector type: %T", searchVector)
	}
}

func (a *Aggregator) performVectorDistanceSearch(ctx context.Context,
	searchVector models.Vector, targetDist float32, maxLimit int64, ids helpers.AllowList,
) ([]uint64, []float32, error) {
	switch vec := searchVector.(type) {
	case []float32:
		return a.vectorIndex.SearchByVectorDistance(ctx, vec, targetDist, maxLimit, ids)
	case [][]float32:
		return a.vectorIndex.SearchByMultiVectorDistance(ctx, vec, targetDist, maxLimit, ids)
	default:
		return nil, nil, fmt.Errorf("perform vector distance search: unrecognized search vector type: %T", searchVector)
	}
}

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

package db

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/aggregator"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/usecases/modules"
)

func (s *Shard) Aggregate(ctx context.Context, params aggregation.Params, modules *modules.Provider) (*aggregation.Result, error) {
	var vectorIndex VectorIndex

	// we only need the index queue for vector search
	if params.NearObject != nil || params.NearVector != nil || params.Hybrid != nil || params.SearchVector != nil {
		idx, ok := s.GetVectorIndex(params.TargetVector)
		if !ok {
			return nil, fmt.Errorf("no vector index for target vector %q", params.TargetVector)
		}
		vectorIndex = idx
	}

	return aggregator.New(s.store, params, s.index.getSchema, s.index.classSearcher,
		s.index.stopwords, s.versioner.Version(), vectorIndex, s.index.logger, s.GetPropertyLengthTracker(),
		s.isFallbackToSearchable, s.tenant(), s.index.Config.QueryNestedRefLimit, s.bitmapFactory, modules).
		Do(ctx)
}

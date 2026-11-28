//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/aggregator"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/usecases/modules"
)

// Aggregate holds a query admission grant until it returns: an aggregation can
// scan the whole shard, and its workers are sized from the grant.
func (s *Shard) Aggregate(ctx context.Context, params aggregation.Params, modules *modules.Provider) (*aggregation.Result, error) {
	ctx, release, err := s.index.Config.QueryAdmission.Admit(ctx, concurrency.TimesGOMAXPROCS(2))
	if err != nil {
		return nil, err
	}
	defer release()
	view := s.acquireView()
	defer view.release()
	return s.aggregate(ctx, view, params, modules)
}

func (s *Shard) aggregate(ctx context.Context, view *shardView, params aggregation.Params, modules *modules.Provider,
) (*aggregation.Result, error) {
	var vectorIndex VectorIndex

	// we only need the index queue for vector search
	if params.NearObject != nil || params.NearVector != nil || params.Hybrid != nil || params.SearchVector != nil {
		idx, ok := s.GetVectorIndex(params.TargetVector)
		if !ok {
			return nil, fmt.Errorf("no vector index for target vector %q", params.TargetVector)
		}
		vectorIndex = idx
	}

	return aggregator.New(view, params, s.index.getSchema, s.propertyIndicesSnapshot(), s.index.classSearcher,
		s.index.getStopwordProvider(), s.versioner.Version(), vectorIndex, s.index.logger, s.GetPropertyLengthTracker(),
		s.tenant(), s.index.Config.QueryNestedRefLimit, s.bitmapFactory, modules, s.index.Config.QueryHybridMaximumResults).
		WithBatchedContainsEnabled(s.index.Config.QueryBatchedContainsEnabled).
		Do(ctx)
}

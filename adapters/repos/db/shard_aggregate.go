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
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/aggregator"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/usecases/modules"
)

func (s *Shard) Aggregate(ctx context.Context, params aggregation.Params, modules *modules.Provider) (*aggregation.Result, error) {
	var vectorIndex VectorIndex

	start := time.Now()
	// Every filtered Aggregate resolves its filter through a Searcher of its
	// own, and the fold's annotations go nowhere without an accumulator on the
	// context, leaving which Contains path answered unrecoverable here. Gated
	// like the delete path's install, on the value LogIfSlow reads.
	if s.index.Config.QuerySlowLogEnabled.Get() {
		ctx = helpers.InitSlowQueryDetails(ctx)
	}
	defer func() {
		s.slowQueryReporter.LogIfSlow(ctx, start, map[string]any{
			"collection": s.index.Config.ClassName,
			"shard":      s.ID(),
			"tenant":     s.tenant(),
			"query":      "Aggregate",
			"filters":    params.Filters,
		})
	}()

	// we only need the index queue for vector search
	if params.NearObject != nil || params.NearVector != nil || params.Hybrid != nil || params.SearchVector != nil {
		idx, ok := s.GetVectorIndex(params.TargetVector)
		if !ok {
			return nil, fmt.Errorf("no vector index for target vector %q", params.TargetVector)
		}
		vectorIndex = idx
	}

	return aggregator.New(s.store, params, s.index.getSchema, s.propertyIndicesSnapshot(), s.index.classSearcher,
		s.index.getStopwordProvider(), s.versioner.Version(), vectorIndex, s.index.logger, s.GetPropertyLengthTracker(),
		s.isFallbackToSearchable, s.IsRangeableLocallyReady, s.tenant(), s.index.Config.QueryNestedRefLimit, s.bitmapFactory, modules, s.index.Config.QueryHybridMaximumResults,
		s.TokenizationFor).
		WithSearchableBucketPinningResolver(s.PinTokenizationAndSearchableBucket).
		WithBatchedContainsEnabled(s.index.Config.QueryBatchedContainsEnabled).
		Do(ctx)
}

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

	"github.com/weaviate/weaviate/adapters/repos/db/aggregator"
	"github.com/weaviate/weaviate/entities/aggregation"
)

func (s *Shard) Aggregate(ctx context.Context,
	params aggregation.Params,
) (*aggregation.Result, error) {
	return aggregator.New(s.store, params, s.index.getSchema, s.index.classSearcher,
		s.index.stopwords, s.versioner.Version(), s.queue, s.index.logger, s.GetPropertyLengthTracker(),
		s.isFallbackToSearchable, s.tenant(), s.index.Config.QueryNestedRefLimit).
		Do(ctx)
}

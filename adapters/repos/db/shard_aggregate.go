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

package db

import (
	"context"

	"github.com/semi-technologies/weaviate/adapters/repos/db/aggregator"
	"github.com/semi-technologies/weaviate/entities/aggregation"
)

func (s *Shard) aggregate(ctx context.Context,
	params aggregation.Params) (*aggregation.Result, error) {
	return aggregator.New(s.store, params, s.index.getSchema, s.invertedRowCache,
		s.index.classSearcher, s.deletedDocIDs).Do(ctx)
}

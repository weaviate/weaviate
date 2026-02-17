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

package selection

import (
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type Selector interface {
	Select(ctx context.Context, ids []uint64, queryDistances []float32, k int, lambda float64, view common.BucketView) ([]uint64, []float32, error)
}

type MMRSelector struct {
	provider distancer.Provider
	vecForID common.TempVectorForIDWithView[float32]
}

func (s *MMRSelector) Select(ctx context.Context, ids []uint64, queryDistances []float32, k int, lambda float64, view common.BucketView) ([]uint64, []float32, error) {
	return mmr(ctx, s.provider, s.vecForID, ids, queryDistances, k, lambda, view)
}

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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// Selector post-processes a candidate set of (ids, distances) into a final
// result. Implementations encode all algorithm-specific parameters at
// construction time so that the call site stays uniform regardless of which
// strategy is active.
type Selector interface {
	Select(ctx context.Context, ids []uint64, queryDistances []float32, view common.BucketView) ([]uint64, []float32, error)
}

// New returns the Selector described by sel, wired up with the index-level
// helpers it needs. Returns nil when sel is nil or no known strategy is set,
// meaning the caller should skip post-processing.
func New(sel *searchparams.Selection, provider distancer.Provider, vecForID common.TempVectorForIDWithView[float32], k int) (Selector, error) {
	if sel == nil {
		return nil, nil
	}
	if sel.MMR != nil {
		if sel.MMR.Balance < 0 || sel.MMR.Balance > 1 {
			return nil, fmt.Errorf("MMR balance must be between 0 and 1")
		}
		mmrLimit := int(sel.MMR.Limit)
		if mmrLimit < k {
			mmrLimit = k
		}
		return newMMRSelector(provider, vecForID, mmrLimit, sel.MMR.Balance), nil
	}
	return nil, nil
}

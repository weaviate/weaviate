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

package hnsw

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestHnswSearchByVectorDistanceCutoffs(t *testing.T) {
	testinghelpers.RunSearchByVectorDistanceCutoffTests(t, func(t *testing.T, provider distancer.Provider, vectors [][]float32) testinghelpers.DistanceSearcher {
		cfg := createVectorHnswIndexTestConfig()
		cfg.DistanceProvider = provider
		cfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		}
		index, err := New(cfg, ent.UserConfig{MaxConnections: 30, EFConstruction: 60, EF: 36},
			cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.NoError(t, err)
		for id, v := range vectors {
			require.NoError(t, index.Add(t.Context(), uint64(id), v))
		}
		return index
	})
}

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

package hfresh

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestSearchWithEmptyIndex(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	vectors, _ := testinghelpers.RandomVecs(1, 0, 32)

	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if indexID == 0 {
			return vectors[0], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})

	index := makeHFreshWithConfig(t, store, cfg, uc)

	// search on empty index returns 0 results and no error
	ids, dists, err := index.SearchByVector(t.Context(), vectors[0], 10, nil)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Empty(t, dists)

	err = index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	ids, dists, err = index.SearchByVector(t.Context(), vectors[0], 10, nil)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Len(t, dists, 1)
	require.Equal(t, uint64(0), ids[0])

	err = index.Delete(0)
	require.NoError(t, err)

	ids, dists, err = index.SearchByVector(t.Context(), vectors[0], 10, nil)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Empty(t, dists)
}

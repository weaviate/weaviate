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

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
)

// Regression test for issue #278 (query side): a single-vector near-vector
// search against a multi-vector index must fail with a clear error instead
// of silently searching the internal FDE space.
func TestSingleVectorQueryOnMultiVectorIndexRejected(t *testing.T) {
	ctx := context.Background()

	uc := enthfresh.NewDefaultUserConfig()
	uc.Multivector.Enabled = true
	uc.Multivector.MuveraConfig.Enabled = true

	shard, index := testShardWithSettings(t, ctx, &models.Class{Class: "MVQueryReject"}, uc, false, false, false)
	defer func() {
		require.NoError(t, index.drop())
	}()

	singleQuery := []models.Vector{[]float32{0.1, 0.2, 0.3}}

	t.Run("limit-based search rejected", func(t *testing.T) {
		_, _, err := shard.ObjectVectorSearch(ctx, singleQuery, []string{""},
			0, 10, nil, nil, nil, additional.Properties{}, nil, nil, nil)
		require.ErrorContains(t, err, "requires a multi-vector query")
	})

	t.Run("distance-based search rejected", func(t *testing.T) {
		_, _, err := shard.ObjectVectorSearch(ctx, singleQuery, []string{""},
			0.5, -1, nil, nil, nil, additional.Properties{}, nil, nil, nil)
		require.ErrorContains(t, err, "requires a multi-vector query")
	})

	t.Run("multi-vector search still works", func(t *testing.T) {
		// insert one document first: issue #275 (encoder not initialized
		// before the first AddMulti) makes querying an empty muvera
		// collection fail, which is a separate, known defect
		obj := testObject("MVQueryReject")
		obj.MultiVectors = map[string][][]float32{"": {{0.1, 0.2, 0.3}, {0.4, 0.5, 0.6}}}
		require.NoError(t, shard.PutObject(ctx, obj))

		found, _, err := shard.ObjectVectorSearch(ctx,
			[]models.Vector{[][]float32{{0.1, 0.2, 0.3}}}, []string{""},
			0, 10, nil, nil, nil, additional.Properties{}, nil, nil, nil)
		require.NoError(t, err)
		require.Len(t, found, 1)
	})
}

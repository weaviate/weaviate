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

//go:build !race

package flat

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

func TestFlatSearchByVectorDistanceCutoffs(t *testing.T) {
	testinghelpers.RunSearchByVectorDistanceCutoffTests(t, func(t *testing.T, provider distancer.Provider, vectors [][]float32) testinghelpers.DistanceSearcher {
		index, err := New(Config{
			ID:                "id",
			RootPath:          t.TempDir(),
			DistanceProvider:  provider,
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		}, flatent.UserConfig{}, loadTestStore(t, t.TempDir()))
		require.NoError(t, err)
		for id, v := range vectors {
			require.NoError(t, index.Add(t.Context(), uint64(id), v))
		}
		return index
	})
}

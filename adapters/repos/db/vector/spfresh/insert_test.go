//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	ent "github.com/weaviate/weaviate/entities/vectorindex/spfresh"
)

func TestSPFreshOptimizedPostingSize(t *testing.T) {
	cfg := DefaultConfig()
	uc := ent.NewDefaultUserConfig()
	uc.CentroidsIndexType = "bruteforce"
	store := testinghelpers.NewDummyStore(t)

	vector := make([]float32, 100)

	t.Run("max posting size computed by the index", func(t *testing.T) {
		index, err := New(cfg, uc, store)
		require.NoError(t, err)
		defer index.Shutdown(t.Context())

		err = index.Add(t.Context(), 0, vector)
		require.NoError(t, err)

		maxPostingSize := index.maxPostingSize
		require.Equal(t, 121, int(maxPostingSize))
	})

	t.Run("max posting size set by the user", func(t *testing.T) {
		uc.MaxPostingSize = 56
		index, err := New(cfg, uc, store)
		require.NoError(t, err)
		defer index.Shutdown(t.Context())

		err = index.Add(t.Context(), 0, vector)
		require.NoError(t, err)

		maxPostingSize := index.maxPostingSize
		require.Equal(t, 56, int(maxPostingSize))
	})

	t.Run("max posting size small", func(t *testing.T) {
		uc.MaxPostingSize = 2
		index, err := New(cfg, uc, store)
		require.NoError(t, err)
		defer index.Shutdown(t.Context())

		err = index.Add(t.Context(), 0, vector)
		require.NoError(t, err)

		maxPostingSize := index.maxPostingSize
		require.Equal(t, 2, int(maxPostingSize))
	})
}

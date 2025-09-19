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
)

func TestSPFreshOptimizedPostingSize(t *testing.T) {
	cfg := DefaultConfig()
	store := testinghelpers.NewDummyStore(t)

	vector := make([]float32, 100)

	t.Run("max posting size computed by the index", func(t *testing.T) {
		index, err := New(cfg, store)
		require.NoError(t, err)
		defer index.Shutdown(t.Context())

		err = index.Add(t.Context(), 0, vector)
		require.NoError(t, err)

		maxPostingSize := index.Config.MaxPostingSize
		require.Equal(t, uint32(101), maxPostingSize)
	})

	t.Run("max posting size set by the user", func(t *testing.T) {
		cfg.MaxPostingSize = 56
		index, err := New(cfg, store)
		require.NoError(t, err)
		defer index.Shutdown(t.Context())

		err = index.Add(t.Context(), 0, vector)
		require.NoError(t, err)

		maxPostingSize := index.Config.MaxPostingSize
		require.Equal(t, uint32(56), maxPostingSize)
	})

	t.Run("max posting size too small", func(t *testing.T) {
		cfg.MaxPostingSize = 2
		index, err := New(cfg, store)
		require.NoError(t, err)
		defer index.Shutdown(t.Context())

		err = index.Add(t.Context(), 0, vector)
		require.NoError(t, err)

		maxPostingSize := index.Config.MaxPostingSize
		require.Equal(t, uint32(10), maxPostingSize)
	})
}

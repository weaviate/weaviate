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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestBlockMaxInvertedIndexConfig(t *testing.T) {
	tests := []struct {
		name            string
		cfg             *models.InvertedIndexConfig
		wantNeedsUpdate bool
		wantFlag        bool
	}{
		{
			name:            "nil config needs the update and must not panic",
			cfg:             nil,
			wantNeedsUpdate: true,
			wantFlag:        true,
		},
		{
			name:            "flag already set is a no-op",
			cfg:             &models.InvertedIndexConfig{UsingBlockMaxWAND: true},
			wantNeedsUpdate: false,
		},
		{
			name:            "flag unset gets flipped",
			cfg:             &models.InvertedIndexConfig{UsingBlockMaxWAND: false},
			wantNeedsUpdate: true,
			wantFlag:        true,
		},
		{
			name:            "zero-value config gets flipped",
			cfg:             &models.InvertedIndexConfig{},
			wantNeedsUpdate: true,
			wantFlag:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, needsUpdate := blockMaxInvertedIndexConfig(tt.cfg)
			require.Equal(t, tt.wantNeedsUpdate, needsUpdate)
			if !tt.wantNeedsUpdate {
				return
			}
			require.NotNil(t, got)
			require.Equal(t, tt.wantFlag, got.UsingBlockMaxWAND)
		})
	}

	t.Run("sibling fields are carried over", func(t *testing.T) {
		cfg := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 77,
			Bm25:                   &models.BM25Config{},
		}
		got, needsUpdate := blockMaxInvertedIndexConfig(cfg)
		require.True(t, needsUpdate)
		require.EqualValues(t, 77, got.CleanupIntervalSeconds,
			"unrelated fields must survive the flip")
		require.Same(t, cfg.Bm25, got.Bm25)
	})

	t.Run("returned config never aliases the input", func(t *testing.T) {
		// The caller holds a shallow class copy whose InvertedIndexConfig still
		// points at the live schema. Flipping the flag through that pointer would
		// mutate the live schema outside RAFT.
		cfg := &models.InvertedIndexConfig{UsingBlockMaxWAND: false}
		got, needsUpdate := blockMaxInvertedIndexConfig(cfg)
		require.True(t, needsUpdate)
		require.NotSame(t, cfg, got, "must return a distinct pointer")
		require.False(t, cfg.UsingBlockMaxWAND, "input must not be mutated")
	})
}

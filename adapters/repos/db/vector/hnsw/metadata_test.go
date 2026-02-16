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
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_AdaptiveEFMetadata(t *testing.T) {
	rootPath := t.TempDir()
	h := &hnsw{rootPath: rootPath, id: "test"}

	t.Run("load from empty dir returns nil", func(t *testing.T) {
		cfg, err := h.LoadAdaptiveEFConfig()
		require.Nil(t, err)
		require.Nil(t, cfg)
	})

	original := &adaptiveEfConfig{
		MeanVec:      []float64{0.1, 0.2},
		VarianceVec:  []float64{0.01, 0.02},
		TargetRecall: 0.95,
		WAE:          64,
		Table: []efTableEntry{
			{Score: 0, EFRecalls: []efRecall{{EF: 32, Recall: 0.90}}},
			{Score: 50, EFRecalls: []efRecall{{EF: 64, Recall: 0.95}}},
		},
	}

	t.Run("store and load round-trip", func(t *testing.T) {
		require.Nil(t, h.StoreAdaptiveEFConfig(original))

		loaded, err := h.LoadAdaptiveEFConfig()
		require.Nil(t, err)
		require.NotNil(t, loaded)
		require.Equal(t, original.MeanVec, loaded.MeanVec)
		require.Equal(t, original.VarianceVec, loaded.VarianceVec)
		require.Equal(t, original.TargetRecall, loaded.TargetRecall)
		require.Equal(t, original.WAE, loaded.WAE)
		require.Equal(t, original.Table, loaded.Table)
	})

	t.Run("overwrite with new config", func(t *testing.T) {
		updated := &adaptiveEfConfig{TargetRecall: 0.99, WAE: 128}
		require.Nil(t, h.StoreAdaptiveEFConfig(updated))

		loaded, err := h.LoadAdaptiveEFConfig()
		require.Nil(t, err)
		require.InDelta(t, 0.99, loaded.TargetRecall, 0.001)
		require.Equal(t, 128, loaded.WAE)
	})

	t.Run("remove with keepFiles true preserves file", func(t *testing.T) {
		h.removeMetadataFile(true)
		cfg, err := h.LoadAdaptiveEFConfig()
		require.Nil(t, err)
		require.NotNil(t, cfg)
	})

	t.Run("remove with keepFiles false deletes file", func(t *testing.T) {
		h.removeMetadataFile(false)
		cfg, err := h.LoadAdaptiveEFConfig()
		require.Nil(t, err)
		require.Nil(t, cfg)
	})
}

func Test_AdaptiveEFMetadataTargetVector(t *testing.T) {
	h1 := &hnsw{id: "test"}
	h2 := &hnsw{id: "vectors_vec_a"}
	h3 := &hnsw{id: "vectors_vec_b"}

	t.Run("no target vector uses meta.db", func(t *testing.T) {
		require.Equal(t, "meta.db", h1.getMetadataFile())
	})

	t.Run("different target vectors use different files", func(t *testing.T) {
		require.NotEqual(t, h2.getMetadataFile(), h3.getMetadataFile())
		require.Contains(t, h2.getMetadataFile(), "vec_a")
		require.Contains(t, h3.getMetadataFile(), "vec_b")
	})

	t.Run("target vector file path traversal", func(t *testing.T) {
		h := &hnsw{id: "vectors_./../foo"}
		require.Equal(t, "meta_foo.db", h.getMetadataFile())
	})
}

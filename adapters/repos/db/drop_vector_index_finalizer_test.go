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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
)

type fakeClassUpdater struct {
	class       *models.Class
	updateErr   error // returned by every UpdateClassInternal
	updateCalls int
	updated     *models.Class
}

func (f *fakeClassUpdater) ReadOnlyClass(string) *models.Class {
	if f.class == nil {
		return nil
	}
	cp := *f.class
	cp.VectorConfig = make(map[string]models.VectorConfig, len(f.class.VectorConfig))
	for k, v := range f.class.VectorConfig {
		cp.VectorConfig[k] = v
	}
	return &cp
}

func (f *fakeClassUpdater) UpdateClassInternal(_ context.Context, _ string, updated *models.Class) error {
	f.updateCalls++
	if f.updateErr != nil {
		return f.updateErr
	}
	f.updated = updated
	return nil
}

func droppedCfg() models.VectorConfig {
	return models.VectorConfig{VectorIndexType: modelsext.VectorIndexTypeNone}
}

func TestRemoveDroppedVectorConfig(t *testing.T) {
	ctx := context.Background()

	t.Run("removes the dropped entry and keeps the rest", func(t *testing.T) {
		up := &fakeClassUpdater{class: &models.Class{Class: "C", VectorConfig: map[string]models.VectorConfig{
			"drop": droppedCfg(),
			"keep": {VectorIndexType: "hnsw"},
		}}}
		f := &schemaVectorConfigFinalizer{mgr: up}

		require.NoError(t, f.RemoveDroppedVectorConfig(ctx, "C", []string{"drop"}))
		require.Equal(t, 1, up.updateCalls)
		require.NotContains(t, up.updated.VectorConfig, "drop")
		require.Contains(t, up.updated.VectorConfig, "keep")
	})

	t.Run("case-differing sibling is a different vector; its marker stays", func(t *testing.T) {
		// Target vector names are case-sensitive identifiers: finalizing "drop"
		// must not erase the dropped marker of the distinct vector "Drop", whose
		// own cleanup has not run.
		up := &fakeClassUpdater{class: &models.Class{Class: "C", VectorConfig: map[string]models.VectorConfig{
			"drop": droppedCfg(),
			"Drop": droppedCfg(),
		}}}
		f := &schemaVectorConfigFinalizer{mgr: up}

		require.NoError(t, f.RemoveDroppedVectorConfig(ctx, "C", []string{"drop"}))
		require.Equal(t, 1, up.updateCalls)
		require.NotContains(t, up.updated.VectorConfig, "drop")
		require.Contains(t, up.updated.VectorConfig, "Drop", "the sibling's marker must survive")
	})

	t.Run("no-change is an idempotent no-op (entry already gone)", func(t *testing.T) {
		up := &fakeClassUpdater{class: &models.Class{Class: "C", VectorConfig: map[string]models.VectorConfig{
			"keep": {VectorIndexType: "hnsw"},
		}}}
		f := &schemaVectorConfigFinalizer{mgr: up}

		require.NoError(t, f.RemoveDroppedVectorConfig(ctx, "C", []string{"drop"}))
		require.Zero(t, up.updateCalls, "nothing to remove must not issue an update")
	})

	t.Run("keeps a live same-name re-creation (guard)", func(t *testing.T) {
		up := &fakeClassUpdater{class: &models.Class{Class: "C", VectorConfig: map[string]models.VectorConfig{
			"drop": {VectorIndexType: "hnsw"}, // same name, but a live index now
		}}}
		f := &schemaVectorConfigFinalizer{mgr: up}

		require.NoError(t, f.RemoveDroppedVectorConfig(ctx, "C", []string{"drop"}))
		require.Zero(t, up.updateCalls, "a live same-name entry must not be removed")
	})

	t.Run("class not found errors", func(t *testing.T) {
		f := &schemaVectorConfigFinalizer{mgr: &fakeClassUpdater{class: nil}}
		require.Error(t, f.RemoveDroppedVectorConfig(ctx, "missing", []string{"drop"}))
	})

	t.Run("retry exhaustion surfaces the last error", func(t *testing.T) {
		up := &fakeClassUpdater{
			class:     &models.Class{Class: "C", VectorConfig: map[string]models.VectorConfig{"drop": droppedCfg()}},
			updateErr: errors.New("raft busy"),
		}
		f := &schemaVectorConfigFinalizer{mgr: up}

		err := f.RemoveDroppedVectorConfig(ctx, "C", []string{"drop"})
		require.Error(t, err)
		require.ErrorContains(t, err, "raft busy")
		require.Equal(t, dropVectorFinalizeMaxAttempts, up.updateCalls, "must exhaust the bounded retry")
	})

	t.Run("ctx cancellation stops the retry loop", func(t *testing.T) {
		up := &fakeClassUpdater{
			class:     &models.Class{Class: "C", VectorConfig: map[string]models.VectorConfig{"drop": droppedCfg()}},
			updateErr: errors.New("transient"),
		}
		f := &schemaVectorConfigFinalizer{mgr: up}
		cctx, cancel := context.WithCancel(ctx)
		cancel()

		require.Error(t, f.RemoveDroppedVectorConfig(cctx, "C", []string{"drop"}))
		require.LessOrEqual(t, up.updateCalls, dropVectorFinalizeMaxAttempts)
	})
}

// TestDeepCopyClass pins the finalize-safety fix: ReadOnlyClass returns a
// shallow clone whose nested pointers are shared with the live FSM class, so the
// finalizer must deep-copy before the update path mutates anything through them.
func TestDeepCopyClass(t *testing.T) {
	orig := &models.Class{
		Class:               "C",
		VectorConfig:        map[string]models.VectorConfig{"v1": droppedCfg()},
		InvertedIndexConfig: &models.InvertedIndexConfig{Bm25: &models.BM25Config{K1: 1.2}},
	}

	cp, err := deepCopyClass(orig)
	require.NoError(t, err)

	cp.InvertedIndexConfig.Bm25.K1 = 9.9
	delete(cp.VectorConfig, "v1")

	require.Equal(t, float32(1.2), orig.InvertedIndexConfig.Bm25.K1,
		"mutating the copy's nested pointer must not touch the original")
	require.Contains(t, orig.VectorConfig, "v1")
}

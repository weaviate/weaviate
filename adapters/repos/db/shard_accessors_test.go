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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestShard_LeasedVectorIndexAccessors(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(idx *Index)

		wantLegacyExists bool
		wantNamedExists  bool
	}{
		{
			name: "only legacy initialized",
			setup: func(idx *Index) {
				idx.vectorIndexUserConfig = hnsw.NewDefaultUserConfig()
			},
			wantLegacyExists: true,
			wantNamedExists:  false,
		},
		{
			name: "only named initialized",
			setup: func(idx *Index) {
				idx.vectorIndexUserConfig = nil
				idx.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
					"named": hnsw.NewDefaultUserConfig(),
					"foo":   flat.NewDefaultUserConfig(),
				}
			},
			wantLegacyExists: false,
			wantNamedExists:  true,
		},
		{
			name: "mixed initialized",
			setup: func(idx *Index) {
				idx.vectorIndexUserConfig = hnsw.NewDefaultUserConfig()
				idx.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
					"named": hnsw.NewDefaultUserConfig(),
					"foo":   flat.NewDefaultUserConfig(),
				}
			},
			wantLegacyExists: true,
			wantNamedExists:  true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := testShardWithSettings(t, testCtx(), &models.Class{Class: "test"}, hnsw.UserConfig{}, false, false, tt.setup)

			for _, target := range []struct {
				name string
				want bool
			}{
				{"named", tt.wantNamedExists},
				{"", tt.wantLegacyExists},
				{modelsext.DefaultNamedVectorName, tt.wantLegacyExists},
			} {
				var called bool
				found, err := s.WithVectorIndex(target.name, func(index VectorIndex) error {
					called = true
					require.NotNil(t, index)
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, target.want, found, "WithVectorIndex(%q)", target.name)
				require.Equal(t, target.want, called)

				called = false
				found, err = s.WithVectorIndexQueue(target.name, func(queue *VectorIndexQueue) error {
					called = true
					require.NotNil(t, queue)
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, target.want, found, "WithVectorIndexQueue(%q)", target.name)
				require.Equal(t, target.want, called)

				index, release, ok := s.AcquireVectorIndex(target.name)
				require.Equal(t, target.want, ok, "AcquireVectorIndex(%q)", target.name)
				if ok {
					require.NotNil(t, index)
					release()
				}

				queue, release, ok := s.AcquireVectorIndexQueue(target.name)
				require.Equal(t, target.want, ok, "AcquireVectorIndexQueue(%q)", target.name)
				if ok {
					require.NotNil(t, queue)
					release()
				}
			}
		})
	}
}

func TestShard_WithVectorIndexReturnsTheCallbackError(t *testing.T) {
	s, _ := testShardWithSettings(t, testCtx(), &models.Class{Class: "test"}, hnsw.UserConfig{}, false, false, func(idx *Index) {
		idx.vectorIndexUserConfig = hnsw.NewDefaultUserConfig()
	})

	found, err := s.WithVectorIndex("", func(VectorIndex) error { return assert.AnError })
	require.True(t, found)
	require.ErrorIs(t, err, assert.AnError)

	found, err = s.WithVectorIndexQueue("", func(*VectorIndexQueue) error { return assert.AnError })
	require.True(t, found)
	require.ErrorIs(t, err, assert.AnError)
}

func TestShard_ForEachVectorIndexAndQueue(t *testing.T) {
	for _, tt := range []struct {
		name          string
		setConfigs    func(idx *Index)
		expectIndexes []string
	}{
		{
			name: "only legacy vector",
			setConfigs: func(idx *Index) {
				idx.vectorIndexUserConfig = hnsw.NewDefaultUserConfig()
			},
			expectIndexes: []string{""},
		},
		{
			name: "only named vector",
			setConfigs: func(idx *Index) {
				idx.vectorIndexUserConfig = nil
				idx.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
					"vector1": hnsw.NewDefaultUserConfig(),
					"vector2": flat.NewDefaultUserConfig(),
				}
			},
			expectIndexes: []string{"vector1", "vector2"},
		},
		{
			name: "mixed vectors",
			setConfigs: func(idx *Index) {
				idx.vectorIndexUserConfig = hnsw.NewDefaultUserConfig()
				idx.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
					"vector1": hnsw.NewDefaultUserConfig(),
					"vector2": flat.NewDefaultUserConfig(),
				}
			},
			expectIndexes: []string{"", "vector1", "vector2"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			shard, _ := testShardWithSettings(t, testCtx(), &models.Class{Class: "TestClass"}, hnsw.NewDefaultUserConfig(), false, false, tt.setConfigs)

			capturedIndexes := make(map[string]any)
			err := shard.ForEachVectorIndex(func(targetVector string, index VectorIndex) error {
				require.NotNil(t, index)
				capturedIndexes[targetVector] = index
				return nil
			})
			require.NoError(t, err)

			capturedQueues := make(map[string]any)
			err = shard.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
				require.NotNil(t, queue)
				capturedQueues[targetVector] = queue
				return nil
			})
			require.NoError(t, err)

			require.Len(t, capturedIndexes, len(tt.expectIndexes))
			for _, name := range tt.expectIndexes {
				_, ok := capturedIndexes[name]
				require.True(t, ok)
			}

			require.Len(t, capturedQueues, len(tt.expectIndexes))
			for _, name := range tt.expectIndexes {
				_, ok := capturedQueues[name]
				require.True(t, ok)
			}
		})
	}
}

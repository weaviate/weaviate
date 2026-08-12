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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestShared_GetVectorIndexAndQueue(t *testing.T) {
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
			s, _ := testShardWithSettings(t, testCtx(), &models.Class{Class: "test"}, hnsw.UserConfig{}, false, true, false, tt.setup)

			namedQueue, ok := s.GetVectorIndexQueue("named")
			require.Equal(t, tt.wantNamedExists, ok)

			namedIndex, ok := s.GetVectorIndex("named")
			require.Equal(t, tt.wantNamedExists, ok)

			if tt.wantNamedExists {
				require.NotNil(t, namedQueue)
				require.NotNil(t, namedIndex)
			}

			legacyQueue, ok := s.GetVectorIndexQueue("")
			require.Equal(t, tt.wantLegacyExists, ok)

			legacyIndex, ok := s.GetVectorIndex("")
			require.Equal(t, tt.wantLegacyExists, ok)

			defaultQueue, ok := s.GetVectorIndex(modelsext.DefaultNamedVectorName)
			require.Equal(t, tt.wantLegacyExists, ok)

			defaultIndex, ok := s.GetVectorIndex(modelsext.DefaultNamedVectorName)
			require.Equal(t, tt.wantLegacyExists, ok)

			if tt.wantLegacyExists {
				require.NotNil(t, legacyQueue)
				require.NotNil(t, legacyIndex)
				require.NotNil(t, defaultQueue)
				require.NotNil(t, defaultIndex)
			}
		})
	}
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
			shard, _ := testShardWithSettings(t, testCtx(), &models.Class{Class: "TestClass"}, hnsw.NewDefaultUserConfig(), false, true, false, tt.setConfigs)

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

func TestShard_PropertyIndicesSnapshot(t *testing.T) {
	tests := []struct {
		name    string
		props   []string
		wantNil bool
	}{
		{
			name:    "uninitialized",
			wantNil: true,
		},
		{
			name:    "no properties",
			props:   []string{},
			wantNil: true,
		},
		{
			name:  "single geo property",
			props: []string{"location"},
		},
		{
			name:  "several geo properties",
			props: []string{"location", "home", "work"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var live propertyspecific.Indices
			if test.props != nil {
				live = propertyspecific.Indices{}
				for _, propName := range test.props {
					live[propName] = propertyspecific.Index{
						Name: propName,
						Type: schema.DataTypeGeoCoordinates,
					}
				}
			}
			shard := &Shard{propertyIndices: live}

			snapshot := shard.propertyIndicesSnapshot()

			if test.wantNil {
				require.Nil(t, snapshot)
			}

			// a later initGeoProp / DropAll must not reach the handed-out copy
			if live != nil {
				shard.propertyIndicesLock.Lock()
				for _, propName := range test.props {
					delete(shard.propertyIndices, propName)
				}
				shard.propertyIndices["added"] = propertyspecific.Index{Name: "added"}
				shard.propertyIndicesLock.Unlock()
			}

			got := make([]string, 0, len(snapshot))
			for propName := range snapshot {
				got = append(got, propName)
			}
			require.ElementsMatch(t, test.props, got)
		})
	}
}

// A searcher reads the indices long after it was handed them, so the snapshot
// has to survive writers running the whole time.
func TestShard_PropertyIndicesSnapshotDuringConcurrentWrites(t *testing.T) {
	shard := &Shard{propertyIndices: propertyspecific.Indices{
		"location": {Name: "location", Type: schema.DataTypeGeoCoordinates},
	}}

	const rounds = 500
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			propName := fmt.Sprintf("prop%d", i)
			shard.propertyIndicesLock.Lock()
			shard.propertyIndices[propName] = propertyspecific.Index{
				Name: propName,
				Type: schema.DataTypeGeoCoordinates,
			}
			delete(shard.propertyIndices, propName)
			shard.propertyIndicesLock.Unlock()
		}
	}()

	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				snapshot := shard.propertyIndicesSnapshot()
				for range snapshot {
				}
				snapshot.ByProp("location")
			}
		}()
	}

	wg.Wait()
}

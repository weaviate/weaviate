//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
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
				idx.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
					"named": hnsw.NewDefaultUserConfig(),
					"foo":   flat.NewDefaultUserConfig(),
				}
			},
			wantLegacyExists: false,
			wantNamedExists:  true,
		},
		// TODO(faustas): uncomment this test once mixed vector support is added
		//{
		//	name: "mixed initialized",
		//	setup: func(idx *Index) {
		//		idx.vectorIndexUserConfig = hnsw.NewDefaultUserConfig()
		//		idx.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
		//			"named": hnsw.NewDefaultUserConfig(),
		//			"foo":   flat.NewDefaultUserConfig(),
		//		}
		//	},
		//	wantLegacyExists: true,
		//	wantNamedExists:  true,
		//},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := testShardWithSettings(t, testCtx(), &models.Class{Class: "test"}, hnsw.UserConfig{}, false, true, tt.setup)

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

			if tt.wantLegacyExists {
				require.NotNil(t, legacyQueue)
				require.NotNil(t, legacyIndex)
			}
		})
	}
}

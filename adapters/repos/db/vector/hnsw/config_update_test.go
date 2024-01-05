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

package hnsw

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestUserConfigUpdates(t *testing.T) {
	t.Run("various immutable and mutable fields", func(t *testing.T) {
		type test struct {
			name          string
			initial       schema.VectorIndexConfig
			update        schema.VectorIndexConfig
			expectedError error
		}

		tests := []test{
			{
				name:    "attempting to change ef construction",
				initial: ent.UserConfig{EFConstruction: 64},
				update:  ent.UserConfig{EFConstruction: 128},
				expectedError: errors.Errorf(
					"efConstruction is immutable: " +
						"attempted change from \"64\" to \"128\""),
			},
			{
				name:    "attempting to change ef construction",
				initial: ent.UserConfig{MaxConnections: 10},
				update:  ent.UserConfig{MaxConnections: 15},
				expectedError: errors.Errorf(
					"maxConnections is immutable: " +
						"attempted change from \"10\" to \"15\""),
			},
			{
				name:    "attempting to change cleanup interval seconds",
				initial: ent.UserConfig{CleanupIntervalSeconds: 60},
				update:  ent.UserConfig{CleanupIntervalSeconds: 90},
				expectedError: errors.Errorf(
					"cleanupIntervalSeconds is immutable: " +
						"attempted change from \"60\" to \"90\""),
			},
			{
				name:    "attempting to change distance",
				initial: ent.UserConfig{Distance: "cosine"},
				update:  ent.UserConfig{Distance: "l2-squared"},
				expectedError: errors.Errorf(
					"distance is immutable: " +
						"attempted change from \"cosine\" to \"l2-squared\""),
			},
			{
				name:          "changing ef",
				initial:       ent.UserConfig{EF: 100},
				update:        ent.UserConfig{EF: -1},
				expectedError: nil,
			},
			{
				name: "changing other mutable settings",
				initial: ent.UserConfig{
					VectorCacheMaxObjects: 700,
					FlatSearchCutoff:      800,
				},
				update: ent.UserConfig{
					VectorCacheMaxObjects: 730,
					FlatSearchCutoff:      830,
				},
				expectedError: nil,
			},
			{
				name: "attempting to change dynamic ef settings",
				initial: ent.UserConfig{
					DynamicEFMin:    100,
					DynamicEFMax:    200,
					DynamicEFFactor: 5,
				},
				update: ent.UserConfig{
					DynamicEFMin:    101,
					DynamicEFMax:    201,
					DynamicEFFactor: 6,
				},
				expectedError: nil,
			},
			{
				name: "setting bq compression on",
				initial: ent.UserConfig{
					BQ: ent.BQConfig{
						Enabled: false,
					},
				},
				update: ent.UserConfig{
					BQ: ent.BQConfig{
						Enabled: true,
					},
				},
				expectedError: nil,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				err := ValidateUserConfigUpdate(test.initial, test.update)
				if test.expectedError == nil {
					assert.Nil(t, err)
				} else {
					require.NotNil(t, err, "update validation must error")
					assert.Equal(t, test.expectedError.Error(), err.Error())
				}
			})
		}
	})
}

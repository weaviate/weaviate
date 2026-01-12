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

package hfresh

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema/config"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
)

func TestHFreshUserConfigUpdates(t *testing.T) {
	t.Run("various immutable and mutable fields", func(t *testing.T) {
		type test struct {
			name          string
			initial       config.VectorIndexConfig
			update        config.VectorIndexConfig
			expectedError error
		}

		tests := []test{
			{
				name:    "attempting to change distance",
				initial: ent.UserConfig{Distance: "cosine"},
				update:  ent.UserConfig{Distance: "l2-squared"},
				expectedError: errors.Errorf(
					"distance is immutable: " +
						"attempted change from \"cosine\" to \"l2-squared\""),
			},
			{
				name:    "attempting to change replicas",
				initial: ent.UserConfig{Replicas: 4},
				update:  ent.UserConfig{Replicas: 8},
				expectedError: errors.Errorf(
					"replicas is immutable: " +
						"attempted change from \"4\" to \"8\""),
			},
			{
				name:          "changing maxPostingSize",
				initial:       ent.UserConfig{MaxPostingSize: 50},
				update:        ent.UserConfig{MaxPostingSize: 100},
				expectedError: nil,
			},
			{
				name:          "changing minPostingSize",
				initial:       ent.UserConfig{MinPostingSize: 10},
				update:        ent.UserConfig{MinPostingSize: 20},
				expectedError: nil,
			},
			{
				name:          "changing rngFactor",
				initial:       ent.UserConfig{RNGFactor: 10.0},
				update:        ent.UserConfig{RNGFactor: 15.0},
				expectedError: nil,
			},
			{
				name:          "changing searchProbe",
				initial:       ent.UserConfig{SearchProbe: 64},
				update:        ent.UserConfig{SearchProbe: 128},
				expectedError: nil,
			},
			{
				name:          "changing rescoreLimit",
				initial:       ent.UserConfig{RescoreLimit: 350},
				update:        ent.UserConfig{RescoreLimit: 500},
				expectedError: nil,
			},
			{
				name:          "changing multiple mutable fields",
				initial:       ent.UserConfig{MaxPostingSize: 50, MinPostingSize: 10, RescoreLimit: 350},
				update:        ent.UserConfig{MaxPostingSize: 100, MinPostingSize: 20, RescoreLimit: 500},
				expectedError: nil,
			},
			{
				name:          "keeping immutable fields unchanged",
				initial:       ent.UserConfig{Distance: "cosine", Replicas: 4, MaxPostingSize: 50},
				update:        ent.UserConfig{Distance: "cosine", Replicas: 4, MaxPostingSize: 100},
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

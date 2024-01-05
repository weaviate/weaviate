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

package flat

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	ent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

func TestFlatUserConfigUpdates(t *testing.T) {
	t.Run("various immutable and mutable fields", func(t *testing.T) {
		type test struct {
			name          string
			initial       schema.VectorIndexConfig
			update        schema.VectorIndexConfig
			expectedError error
		}

		tests := []test{
			{
				name:    "attempting to change pq enabled",
				initial: ent.UserConfig{PQ: ent.CompressionUserConfig{Enabled: false}},
				update:  ent.UserConfig{PQ: ent.CompressionUserConfig{Enabled: true}},
				expectedError: errors.Errorf(
					"pq is immutable: " +
						"attempted change from \"false\" to \"true\""),
			},
			{
				name:    "attempting to change bq enabled",
				initial: ent.UserConfig{BQ: ent.CompressionUserConfig{Enabled: true}},
				update:  ent.UserConfig{BQ: ent.CompressionUserConfig{Enabled: false}},
				expectedError: errors.Errorf(
					"bq is immutable: " +
						"attempted change from \"true\" to \"false\""),
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
				name:          "changing rescoreLimit",
				initial:       ent.UserConfig{BQ: ent.CompressionUserConfig{RescoreLimit: 10}},
				update:        ent.UserConfig{BQ: ent.CompressionUserConfig{RescoreLimit: 100}},
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

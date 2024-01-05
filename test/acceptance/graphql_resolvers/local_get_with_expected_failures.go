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

package test

import (
	"testing"

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func getsWithExpectedFailures(t *testing.T) {
	t.Run("get with certainty on l2-squared distancer", func(t *testing.T) {
		className := "L2DistanceClass"
		defer deleteObjectClass(t, className)

		t.Run("create class configured with distance type l2-squared", func(t *testing.T) {
			createObjectClass(t, &models.Class{
				Class: className,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizeClassName": true,
					},
				},
				VectorIndexConfig: map[string]interface{}{
					"distance": "l2-squared",
				},
				Properties: []*models.Property{
					{
						Name:         "name",
						DataType:     schema.DataTypeText.PropString(),
						Tokenization: models.PropertyTokenizationWhitespace,
					},
				},
			})
		})

		t.Run("assert failure to get", func(t *testing.T) {
			query := `
				{
					Get {
						L2DistanceClass(nearVector: {vector:[1,1,1], certainty: 0.8}) {
							name
						}
					}
				}`

			result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, query)
			assert.Len(t, result, 1)

			errMsg := result[0].Message
			assert.Equal(t, "can't compute and return certainty when vector index is configured with l2-squared distance", errMsg)
		})
	})

	t.Run("get with certainty on dot distancer", func(t *testing.T) {
		className := "DotDistanceClass"
		defer deleteObjectClass(t, className)

		t.Run("create class configured with distance type dot", func(t *testing.T) {
			createObjectClass(t, &models.Class{
				Class: className,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizeClassName": true,
					},
				},
				VectorIndexConfig: map[string]interface{}{
					"distance": "dot",
				},
				Properties: []*models.Property{
					{
						Name:         "name",
						DataType:     schema.DataTypeText.PropString(),
						Tokenization: models.PropertyTokenizationWhitespace,
					},
				},
			})
		})

		t.Run("assert failure to get", func(t *testing.T) {
			query := `
				{
					Get {
						DotDistanceClass(nearVector: {vector:[1,1,1], certainty: 0.8}) {
							name
						}
					}
				}`

			result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, query)
			assert.Len(t, result, 1)

			errMsg := result[0].Message
			assert.Equal(t, "can't compute and return certainty when vector index is configured with dot distance", errMsg)
		})
	})
}

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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func aggregatesWithExpectedFailures(t *testing.T) {
	t.Run("with nearVector, no certainty", func(t *testing.T) {
		result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					CustomVectorClass(
						nearVector: {
							vector: [1,0,0]
						}
					){
						meta {
							count
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message,
			"must provide certainty or objectLimit with vector search"),
			"unexpected error message: %s", result[0].Message)
	})

	t.Run("with nearObject, no certainty", func(t *testing.T) {
		result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					City(
						nearObject: {
							id: "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
						}
					){
						meta {
							count
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message,
			"must provide certainty or objectLimit with vector search"),
			"unexpected error message: %s", result[0].Message)
	})

	t.Run("with nearText, no certainty", func(t *testing.T) {
		result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					City(
						nearText: {
							concepts: ["Amsterdam"]
						}
					){
						meta {
							count
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message,
			"must provide certainty or objectLimit with vector search"),
			"unexpected error message: %s", result[0].Message)
	})

	t.Run("with nearVector, where filter, no certainty", func(t *testing.T) {
		result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					CustomVectorClass(
						where: {
							valueText: "Mercedes",
							operator: Equal,
							path: ["name"]
						}
						nearVector: {
							vector: [1,0,0]
						}
					){
						meta {
							count
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message,
			"must provide certainty or objectLimit with vector search"),
			"unexpected error message: %s", result[0].Message)
	})

	t.Run("with nearObject, where filter, no certainty", func(t *testing.T) {
		result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					City (where: {
						valueBoolean: true,
						operator: Equal,
						path: ["isCapital"]
					}
					nearObject: {
						id: "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
					}
					){
						meta {
							count
						}
						isCapital {
							count
							percentageFalse
							percentageTrue
							totalFalse
							totalTrue
							type
						}
						population {
							mean
							count
							maximum
							minimum
							sum
							type
						}
						inCountry {
							pointingTo
							type
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message,
			"must provide certainty or objectLimit with vector search"),
			"unexpected error message: %s", result[0].Message)
	})

	t.Run("with nearText, where filter, no certainty", func(t *testing.T) {
		result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					City (where: {
						valueBoolean: true,
						operator: Equal,
						path: ["isCapital"]
					}
					nearText: {
						concepts: ["Amsterdam"]
					}
					){
						meta {
							count
						}
						isCapital {
							count
							percentageFalse
							percentageTrue
							totalFalse
							totalTrue
							type
						}
						population {
							mean
							count
							maximum
							minimum
							sum
							type
						}
						inCountry {
							pointingTo
							type
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message,
			"must provide certainty or objectLimit with vector search"),
			"unexpected error message: %s", result[0].Message)
	})

	t.Run("objectLimit passed with no nearMedia", func(t *testing.T) {
		result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					CustomVectorClass(objectLimit: 1){
						meta {
							count
						}
						name {
							topOccurrences {
								occurs
								value
							}
							type
							count
						}
					}
				}
			}
		`)

		require.NotEmpty(t, result)
		require.Len(t, result, 1)
		assert.True(t, strings.Contains(result[0].Message, "objectLimit can only be used with a near<Media> or hybrid filter"))
	})
}

func exploreWithExpectedFailures(t *testing.T) {
	t.Run("Explore called when classes have different distance configs", func(t *testing.T) {
		className := "L2DistanceClass"
		defer deleteObjectClass(t, className)

		t.Run("create class configured with non-default distance type", func(t *testing.T) {
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

		t.Run("assert failure to Explore with mismatched distance types", func(t *testing.T) {
			query := `
				{
					Explore(nearVector: {vector:[1,1,1]}) {
						distance
					}
				}`

			result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, query)
			assert.Len(t, result, 1)

			errMsg := result[0].Message
			assert.Contains(t, errMsg, "vector search across classes not possible")
			assert.Contains(t, errMsg, "found different distance metrics")
			assert.Contains(t, errMsg, "class 'L2DistanceClass' uses distance metric 'l2-squared'")
			assert.Contains(t, errMsg, "class 'Airport' uses distance metric 'cosine'")
			assert.Contains(t, errMsg, "class 'Person' uses distance metric 'cosine'")
			assert.Contains(t, errMsg, "class 'ArrayClass' uses distance metric 'cosine'")
			assert.Contains(t, errMsg, "class 'HasDateField' uses distance metric 'cosine'")
			assert.Contains(t, errMsg, "class 'CustomVectorClass' uses distance metric 'cosine'")
			assert.Contains(t, errMsg, "class 'RansomNote' uses distance metric 'cosine'")
			assert.Contains(t, errMsg, "class 'MultiShard' uses distance metric 'cosine'")
			assert.Contains(t, errMsg, "class 'Country' uses distance metric 'cosine'")
			assert.Contains(t, errMsg, "class 'City' uses distance metric 'cosine'")
			assert.Contains(t, errMsg, "class 'Company' uses distance metric 'cosine'")
			assert.Contains(t, errMsg, "class 'Pizza' uses distance metric 'cosine'")
		})
	})
}

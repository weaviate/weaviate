package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func TestQueryVectorExtension(t *testing.T) {
	t.Run("Get with nearVector", func(t *testing.T) {
		query := `
		{
			Get {
				City(nearVector: {vector: [0.9, 0.0, 0.0]}) {
					name
				}
			}
		}
		`
		result := graphqlhelper.QueryGraphQLOrFatal(t, helper.RootAuth, "", query, nil)

		extensions := result.Extensions
		require.NotNil(t, extensions)

		metadata, ok := extensions["metadata"].(map[string]interface{})
		require.True(t, ok)

		vectorizer, ok := metadata["vectorizer"].(map[string]interface{})
		require.True(t, ok)

		queryVectors, ok := vectorizer["queryVectors"].(map[string]interface{})
		require.True(t, ok)

		// Default target vector is empty string key if not named
		vec, ok := queryVectors[""].([]interface{})
		require.True(t, ok)
		assert.Equal(t, 3, len(vec))
		// We expect [0.9, 0.0, 0.0] approximately
		assert.InDelta(t, 0.9, vec[0].(float64), 0.0001)
	})

	t.Run("Explore with nearVector", func(t *testing.T) {
		query := `
		{
			Explore(nearVector: {vector: [0.0, 0.9, 0.0]}) {
				beacon
			}
		}
		`
		result := graphqlhelper.QueryGraphQLOrFatal(t, helper.RootAuth, "", query, nil)

		extensions := result.Extensions
		require.NotNil(t, extensions)

		metadata, ok := extensions["metadata"].(map[string]interface{})
		require.True(t, ok)

		vectorizer, ok := metadata["vectorizer"].(map[string]interface{})
		require.True(t, ok)

		queryVectors, ok := vectorizer["queryVectors"].(map[string]interface{})
		require.True(t, ok)

		vec, ok := queryVectors[""].([]interface{})
		require.True(t, ok)

		assert.Equal(t, 3, len(vec))
		assert.InDelta(t, 0.9, vec[1].(float64), 0.0001)
	})
}

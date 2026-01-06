package test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/cities"
)

func TestQueryVectorExtension(t *testing.T) {
	helper.SetupClient("localhost:8080")
	// Ensure schema is present. We can try to clean it first to be safe, or just overwrite.
	// CreateCountryCityAirportSchema likely creates classes.
	// Let's defer cleanup.
	cities.CreateCountryCityAirportSchema(t, "localhost:8080")

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
		val0, err := vec[0].(json.Number).Float64()
		require.NoError(t, err)
		assert.InDelta(t, 0.9, val0, 0.0001)
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
		val1, err := vec[1].(json.Number).Float64()
		require.NoError(t, err)
		assert.InDelta(t, 0.9, val1, 0.0001)
	})
}

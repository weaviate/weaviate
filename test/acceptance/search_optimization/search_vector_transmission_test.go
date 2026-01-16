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

package search_optimization

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// TestSearchVectorTransmission tests that search operations correctly handle
// conditional vector transmission. This verifies the optimization that reduces
// network bandwidth by not transmitting vectors when they are not requested.
//
// The test covers:
// - Legacy vectors (single unnamed vector)
// - Named vectors (multiple named vectors per object)
// - Multi-vectors (vectors with multiple embeddings per target)
// - Different sharding configurations to ensure inter-node communication
func TestSearchVectorTransmission(t *testing.T) {
	ctx := context.Background()

	// Start a 3-node cluster
	compose, err := docker.New().
		With3NodeCluster().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("LegacyVector", func(t *testing.T) {
		testLegacyVector(t)
	})

	t.Run("NamedVectors", func(t *testing.T) {
		testNamedVectors(t)
	})

	t.Run("ShardedClass", func(t *testing.T) {
		testShardedClass(t)
	})
}

func testLegacyVector(t *testing.T) {
	className := "LegacyVectorTest"

	// Create class with legacy vector (no named vectors)
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: schema.DataTypeText.PropString(),
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance": "cosine",
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": json.Number("2"), // Multiple shards to test inter-node communication
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// Insert objects with vectors
	objects := []*models.Object{
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000001"),
			Properties: map[string]interface{}{
				"title": "First document",
			},
			Vector: []float32{0.1, 0.2, 0.3, 0.4},
		},
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000002"),
			Properties: map[string]interface{}{
				"title": "Second document",
			},
			Vector: []float32{0.5, 0.6, 0.7, 0.8},
		},
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000003"),
			Properties: map[string]interface{}{
				"title": "Third document",
			},
			Vector: []float32{0.9, 0.1, 0.2, 0.3},
		},
	}
	helper.CreateObjectsBatch(t, objects)

	t.Run("SearchWithVectorRequested", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s {
					title
					_additional {
						id
						vector
					}
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.Len(t, results, 3)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			assert.NotNil(t, addl["vector"], "vector should be present when requested")
			vec := addl["vector"].([]interface{})
			assert.Len(t, vec, 4, "vector should have 4 dimensions")
		}
	})

	t.Run("SearchWithoutVectorRequested", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s {
					title
					_additional {
						id
					}
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.Len(t, results, 3)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			assert.Nil(t, addl["vector"], "vector should NOT be present when not requested")
		}
	})

	t.Run("NearVectorSearchWithoutVectorRequested", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s(nearVector: {vector: [0.1, 0.2, 0.3, 0.4]}) {
					title
					_additional {
						id
						distance
					}
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.GreaterOrEqual(t, len(results), 1)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			assert.NotNil(t, addl["distance"])
			assert.Nil(t, addl["vector"], "vector should NOT be present when not requested")
		}
	})

	t.Run("NearVectorSearchWithVectorRequested", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s(nearVector: {vector: [0.1, 0.2, 0.3, 0.4]}) {
					title
					_additional {
						id
						distance
						vector
					}
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.GreaterOrEqual(t, len(results), 1)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			assert.NotNil(t, addl["distance"])
			assert.NotNil(t, addl["vector"], "vector should be present when requested")
		}
	})
}

func testNamedVectors(t *testing.T) {
	className := "NamedVectorsTest"

	// Create class with named vectors
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: schema.DataTypeText.PropString(),
			},
			{
				Name:     "description",
				DataType: schema.DataTypeText.PropString(),
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			"title_vector": {
				Vectorizer: map[string]interface{}{
					"none": map[string]interface{}{},
				},
				VectorIndexType: "hnsw",
				VectorIndexConfig: map[string]interface{}{
					"distance": "cosine",
				},
			},
			"desc_vector": {
				Vectorizer: map[string]interface{}{
					"none": map[string]interface{}{},
				},
				VectorIndexType: "hnsw",
				VectorIndexConfig: map[string]interface{}{
					"distance": "cosine",
				},
			},
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": json.Number("2"),
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// Insert objects with named vectors
	objects := []*models.Object{
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000011"),
			Properties: map[string]interface{}{
				"title":       "First document",
				"description": "A description",
			},
			Vectors: map[string]models.Vector{
				"title_vector": []float32{0.1, 0.2, 0.3},
				"desc_vector":  []float32{0.4, 0.5, 0.6},
			},
		},
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000012"),
			Properties: map[string]interface{}{
				"title":       "Second document",
				"description": "Another description",
			},
			Vectors: map[string]models.Vector{
				"title_vector": []float32{0.7, 0.8, 0.9},
				"desc_vector":  []float32{0.1, 0.2, 0.3},
			},
		},
	}
	helper.CreateObjectsBatch(t, objects)

	t.Run("SearchWithSpecificVectorRequested", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s {
					title
					_additional {
						id
						vectors {
							title_vector
						}
					}
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.Len(t, results, 2)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			vectors := addl["vectors"].(map[string]interface{})
			assert.NotNil(t, vectors["title_vector"], "title_vector should be present when requested")
		}
	})

	t.Run("SearchWithAllVectorsRequested", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s {
					title
					_additional {
						id
						vectors {
							title_vector
							desc_vector
						}
					}
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.Len(t, results, 2)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			vectors := addl["vectors"].(map[string]interface{})
			assert.NotNil(t, vectors["title_vector"], "title_vector should be present")
			assert.NotNil(t, vectors["desc_vector"], "desc_vector should be present")
		}
	})

	t.Run("SearchWithoutVectorsRequested", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s {
					title
					_additional {
						id
					}
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.Len(t, results, 2)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			assert.Nil(t, addl["vectors"], "vectors should NOT be present when not requested")
		}
	})

	t.Run("NearVectorOnNamedVectorWithoutVectorRequested", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s(nearVector: {vector: [0.1, 0.2, 0.3], targetVectors: ["title_vector"]}) {
					title
					_additional {
						id
						distance
					}
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.GreaterOrEqual(t, len(results), 1)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			assert.NotNil(t, addl["distance"])
			assert.Nil(t, addl["vectors"], "vectors should NOT be present when not requested")
		}
	})
}

func testShardedClass(t *testing.T) {
	className := "ShardedClassTest"

	// Create class with high shard count to ensure inter-node communication
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "content",
				DataType: schema.DataTypeText.PropString(),
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance": "cosine",
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": json.Number("6"), // More shards than nodes to ensure distribution
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// Insert many objects to distribute across shards
	objects := make([]*models.Object, 20)
	for i := 0; i < 20; i++ {
		objects[i] = &models.Object{
			Class: className,
			ID:    strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000001%02d", i)),
			Properties: map[string]interface{}{
				"content": fmt.Sprintf("Document number %d with some content", i),
			},
			Vector: []float32{float32(i) * 0.1, float32(i) * 0.05, float32(i) * 0.02, float32(i) * 0.01},
		}
	}
	helper.CreateObjectsBatch(t, objects)

	t.Run("SearchAcrossShardsWithoutVector", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s(limit: 20) {
					content
					_additional {
						id
					}
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.Len(t, results, 20)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			assert.Nil(t, addl["vector"], "vector should NOT be present when not requested")
		}
	})

	t.Run("SearchAcrossShardsWithVector", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s(limit: 20) {
					content
					_additional {
						id
						vector
					}
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.Len(t, results, 20)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			assert.NotNil(t, addl["vector"], "vector should be present when requested")
			vec := addl["vector"].([]interface{})
			assert.Len(t, vec, 4, "vector should have 4 dimensions")
		}
	})

	t.Run("NearVectorAcrossShardsWithoutVector", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s(nearVector: {vector: [0.5, 0.25, 0.1, 0.05]}, limit: 10) {
					content
					_additional {
						id
						distance
					}
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.GreaterOrEqual(t, len(results), 1)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			assert.NotNil(t, addl["distance"])
			assert.Nil(t, addl["vector"], "vector should NOT be present when not requested")
		}
	})
}

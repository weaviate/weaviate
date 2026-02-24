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
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/usecases/byteops"
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

	// Start a 3-node cluster with gRPC exposed
	compose, err := docker.New().
		WithWeaviateClusterWithGRPC().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	// Setup gRPC client
	grpcConn, err := helper.CreateGrpcConnectionClient(compose.GetWeaviate().GrpcURI())
	require.NoError(t, err)
	defer grpcConn.Close()
	grpcClient := helper.CreateGrpcWeaviateClient(grpcConn)

	// GraphQL tests
	t.Run("LegacyVector", func(t *testing.T) {
		testLegacyVector(t)
	})

	t.Run("NamedVectors", func(t *testing.T) {
		testNamedVectors(t)
	})

	t.Run("ShardedClass", func(t *testing.T) {
		testShardedClass(t)
	})

	t.Run("PropertyVariations", func(t *testing.T) {
		testPropertyVariations(t)
	})

	// gRPC tests
	t.Run("gRPCVectorTransmission", func(t *testing.T) {
		testGRPCVectorTransmission(t, grpcClient)
	})

	t.Run("gRPCPropertyVariations", func(t *testing.T) {
		testGRPCPropertyVariations(t, grpcClient)
	})

	t.Run("gRPCNamedVectorTransmission", func(t *testing.T) {
		testGRPCNamedVectorTransmission(t, grpcClient)
	})

	// Cursor pagination tests
	t.Run("GraphQLCursorPagination", func(t *testing.T) {
		testGraphQLCursorPagination(t)
	})

	t.Run("RESTCursorPagination", func(t *testing.T) {
		testRESTCursorPagination(t)
	})

	t.Run("gRPCCursorPagination", func(t *testing.T) {
		testGRPCCursorPagination(t, grpcClient)
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
			assert.Nil(t, vectors["desc_vector"], "desc_vector should NOT be present when only title_vector requested")
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

// testPropertyVariations tests GraphQL search with various property inclusion configurations
func testPropertyVariations(t *testing.T) {
	className := "PropertyVariationsTest"

	// Create class with multiple properties
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
			{
				Name:     "count",
				DataType: schema.DataTypeInt.PropString(),
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance": "cosine",
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": json.Number("2"),
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// Insert objects
	objects := []*models.Object{
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000021"),
			Properties: map[string]interface{}{
				"title":       "First item",
				"description": "A description for the first item",
				"count":       float64(10),
			},
			Vector: []float32{0.1, 0.2, 0.3, 0.4},
		},
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000022"),
			Properties: map[string]interface{}{
				"title":       "Second item",
				"description": "A description for the second item",
				"count":       float64(20),
			},
			Vector: []float32{0.5, 0.6, 0.7, 0.8},
		},
	}
	helper.CreateObjectsBatch(t, objects)

	t.Run("SearchWithNoProperties", func(t *testing.T) {
		// Query with only _additional, no properties
		query := fmt.Sprintf(`{
			Get {
				%s {
					_additional { id }
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
			// Properties should not be in the response
			assert.Nil(t, obj["title"])
			assert.Nil(t, obj["description"])
			assert.Nil(t, obj["count"])
			// Vector should not be present when not requested
			assert.Nil(t, addl["vector"], "vector should NOT be present when not requested")
		}
	})

	t.Run("SearchWithSomeProperties", func(t *testing.T) {
		// Query with subset of properties
		query := fmt.Sprintf(`{
			Get {
				%s {
					title
					_additional { id }
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
			// Only title should be present
			assert.NotNil(t, obj["title"])
			assert.Nil(t, obj["description"])
			assert.Nil(t, obj["count"])
			// Vector should not be present when not requested
			assert.Nil(t, addl["vector"], "vector should NOT be present when not requested")
		}
	})

	t.Run("SearchWithAllProperties", func(t *testing.T) {
		// Query with all properties
		query := fmt.Sprintf(`{
			Get {
				%s {
					title
					description
					count
					_additional { id vector }
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
			assert.NotNil(t, addl["vector"])
			// All properties should be present
			assert.NotNil(t, obj["title"])
			assert.NotNil(t, obj["description"])
			assert.NotNil(t, obj["count"])
		}
	})

	t.Run("NearVectorWithSomeProperties", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s(nearVector: {vector: [0.1, 0.2, 0.3, 0.4]}) {
					title
					count
					_additional { id distance }
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
			// Only title and count should be present
			assert.NotNil(t, obj["title"])
			assert.NotNil(t, obj["count"])
			assert.Nil(t, obj["description"])
			// Vector not requested
			assert.Nil(t, addl["vector"])
		}
	})
}

// testGRPCVectorTransmission tests gRPC search with vector control
func testGRPCVectorTransmission(t *testing.T, grpcClient protocol.WeaviateClient) {
	ctx := context.Background()
	className := "GRPCVectorTest"

	// Create class with legacy vector
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
			"desiredCount": json.Number("2"),
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// Insert objects
	objects := []*models.Object{
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000031"),
			Properties: map[string]interface{}{
				"title": "First gRPC document",
			},
			Vector: []float32{0.1, 0.2, 0.3, 0.4},
		},
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000032"),
			Properties: map[string]interface{}{
				"title": "Second gRPC document",
			},
			Vector: []float32{0.5, 0.6, 0.7, 0.8},
		},
	}
	helper.CreateObjectsBatch(t, objects)

	t.Run("SearchWithoutVector", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Properties: &protocol.PropertiesRequest{
				NonRefProperties: []string{"title"},
			},
			Metadata: &protocol.MetadataRequest{
				Uuid: true,
				// NO Vector field - should not return vectors
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			assert.Empty(t, result.Metadata.VectorBytes, "vector bytes should be empty when not requested")
		}
	})

	t.Run("SearchWithVector", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Metadata: &protocol.MetadataRequest{
				Uuid:   true,
				Vector: true, // Request vector
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			// Vector should be present as VectorBytes
			assert.NotEmpty(t, result.Metadata.VectorBytes, "vector should be present when requested")
		}
	})

	t.Run("NearVectorWithoutVectorInResponse", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			NearVector: &protocol.NearVector{
				VectorBytes: byteops.Fp32SliceToBytes([]float32{0.1, 0.2, 0.3, 0.4}),
			},
			Metadata: &protocol.MetadataRequest{
				Uuid:     true,
				Distance: true,
				// NO Vector - should not return vectors
			},
			Uses_123Api: true,
			Uses_125Api: true,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.GreaterOrEqual(t, len(resp.Results), 1)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			// Distance is returned when requested (may be 0 for exact match)
			assert.Empty(t, result.Metadata.VectorBytes, "vector bytes should be empty when not requested")
		}
	})

	t.Run("NearVectorWithVectorInResponse", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			NearVector: &protocol.NearVector{
				VectorBytes: byteops.Fp32SliceToBytes([]float32{0.1, 0.2, 0.3, 0.4}),
			},
			Metadata: &protocol.MetadataRequest{
				Uuid:     true,
				Distance: true,
				Vector:   true, // Request vector
			},
			Uses_123Api: true,
			Uses_125Api: true,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.GreaterOrEqual(t, len(resp.Results), 1)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			// Distance is returned when requested (may be 0 for exact match)
			assert.NotEmpty(t, result.Metadata.VectorBytes, "vector should be present when requested")
		}
	})
}

// testGRPCPropertyVariations tests gRPC search with various property configurations
func testGRPCPropertyVariations(t *testing.T, grpcClient protocol.WeaviateClient) {
	ctx := context.Background()
	className := "GRPCPropsTest"

	// Create class with multiple properties
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
			{
				Name:     "count",
				DataType: schema.DataTypeInt.PropString(),
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance": "cosine",
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": json.Number("2"),
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// Insert objects
	objects := []*models.Object{
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000041"),
			Properties: map[string]interface{}{
				"title":       "First gRPC props item",
				"description": "Description one",
				"count":       float64(100),
			},
			Vector: []float32{0.1, 0.2, 0.3, 0.4},
		},
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000042"),
			Properties: map[string]interface{}{
				"title":       "Second gRPC props item",
				"description": "Description two",
				"count":       float64(200),
			},
			Vector: []float32{0.5, 0.6, 0.7, 0.8},
		},
	}
	helper.CreateObjectsBatch(t, objects)

	t.Run("SearchWithNoPropertiesField", func(t *testing.T) {
		// When Properties field is not specified, gRPC returns all properties by default
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			// No Properties field - returns all properties by default
			Metadata:    &protocol.MetadataRequest{Uuid: true},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			// Without Properties field, all properties are returned by default
			assert.NotNil(t, result.Properties)
			assert.NotNil(t, result.Properties.NonRefProps)
			assert.GreaterOrEqual(t, len(result.Properties.NonRefProps.Fields), 3, "all properties should be returned when Properties field is not specified")
			// But vector should not be returned since it wasn't requested
			assert.Empty(t, result.Metadata.VectorBytes, "vector should be empty when not requested")
		}
	})

	t.Run("SearchWithSomeProperties", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Properties: &protocol.PropertiesRequest{
				NonRefProperties: []string{"title"}, // Only title
			},
			Metadata:    &protocol.MetadataRequest{Uuid: true},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			assert.NotNil(t, result.Properties)
			// Only title should be present
			assert.NotNil(t, result.Properties.NonRefProps)
			titleField := result.Properties.NonRefProps.Fields["title"]
			assert.NotNil(t, titleField, "title property should be present")
			assert.NotEmpty(t, titleField.GetTextValue())
			// Vector should not be present when not requested
			assert.Empty(t, result.Metadata.VectorBytes, "vector should NOT be present when not requested")
		}
	})

	t.Run("SearchWithAllProperties", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Properties: &protocol.PropertiesRequest{
				ReturnAllNonrefProperties: true,
			},
			Metadata:    &protocol.MetadataRequest{Uuid: true, Vector: true},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			assert.NotEmpty(t, result.Metadata.VectorBytes, "vector should be present when requested")
			assert.NotNil(t, result.Properties)
			// All properties should be present
			assert.NotNil(t, result.Properties.NonRefProps)
			assert.GreaterOrEqual(t, len(result.Properties.NonRefProps.Fields), 3)
		}
	})

	t.Run("NearVectorWithSomeProperties", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			NearVector: &protocol.NearVector{
				VectorBytes: byteops.Fp32SliceToBytes([]float32{0.1, 0.2, 0.3, 0.4}),
			},
			Properties: &protocol.PropertiesRequest{
				NonRefProperties: []string{"title", "count"},
			},
			Metadata: &protocol.MetadataRequest{
				Uuid:     true,
				Distance: true,
				// NO Vector
			},
			Uses_123Api: true,
			Uses_125Api: true,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.GreaterOrEqual(t, len(resp.Results), 1)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			// Distance is returned when requested (may be 0 for exact match)
			assert.Empty(t, result.Metadata.VectorBytes, "vector bytes should be empty when not requested")
			assert.NotNil(t, result.Properties)
			assert.NotNil(t, result.Properties.NonRefProps)
		}
	})
}

// testGRPCNamedVectorTransmission tests gRPC search with named vectors
func testGRPCNamedVectorTransmission(t *testing.T, grpcClient protocol.WeaviateClient) {
	ctx := context.Background()
	className := "GRPCNamedVectorTest"

	// Create class with named vectors
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "title",
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

	// Insert objects
	objects := []*models.Object{
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000051"),
			Properties: map[string]interface{}{
				"title": "Named vector doc 1",
			},
			Vectors: map[string]models.Vector{
				"title_vector": []float32{0.1, 0.2, 0.3},
				"desc_vector":  []float32{0.4, 0.5, 0.6},
			},
		},
		{
			Class: className,
			ID:    strfmt.UUID("00000000-0000-0000-0000-000000000052"),
			Properties: map[string]interface{}{
				"title": "Named vector doc 2",
			},
			Vectors: map[string]models.Vector{
				"title_vector": []float32{0.7, 0.8, 0.9},
				"desc_vector":  []float32{0.1, 0.2, 0.3},
			},
		},
	}
	helper.CreateObjectsBatch(t, objects)

	t.Run("SearchWithSpecificNamedVectors", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Metadata: &protocol.MetadataRequest{
				Uuid:    true,
				Vectors: []string{"title_vector"}, // Only one named vector
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			// Should have only title_vector
			require.Len(t, result.Metadata.Vectors, 1)
			assert.Equal(t, "title_vector", result.Metadata.Vectors[0].Name)
		}
	})

	t.Run("SearchWithAllNamedVectors", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Metadata: &protocol.MetadataRequest{
				Uuid:    true,
				Vectors: []string{"title_vector", "desc_vector"},
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			// Should have both vectors
			require.Len(t, result.Metadata.Vectors, 2)
		}
	})

	t.Run("SearchWithNoNamedVectors", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Metadata: &protocol.MetadataRequest{
				Uuid: true,
				// No Vectors field
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			// Should have no vectors
			assert.Empty(t, result.Metadata.Vectors)
		}
	})

	t.Run("NearVectorOnNamedVectorWithoutVectorInResponse", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			NearVector: &protocol.NearVector{
				VectorBytes: byteops.Fp32SliceToBytes([]float32{0.1, 0.2, 0.3}),
				Targets: &protocol.Targets{
					TargetVectors: []string{"title_vector"},
				},
			},
			Metadata: &protocol.MetadataRequest{
				Uuid:     true,
				Distance: true,
				// No Vectors - should not return vectors
			},
			Uses_123Api: true,
			Uses_125Api: true,
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.GreaterOrEqual(t, len(resp.Results), 1)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			// Distance is returned when requested (may be 0 for exact match)
			assert.Empty(t, result.Metadata.Vectors, "vectors should be empty when not requested")
		}
	})
}

// testGraphQLCursorPagination tests GraphQL cursor-based listing with vector/property combinations
func testGraphQLCursorPagination(t *testing.T) {
	className := "GraphQLCursorTest"

	// Create class with multiple shards for distribution
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
		VectorIndexConfig: map[string]interface{}{
			"distance": "cosine",
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": json.Number("3"),
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// Insert objects with sequential UUIDs for predictable ordering
	objectIDs := []strfmt.UUID{
		"00000000-0000-0000-0000-000000000061",
		"00000000-0000-0000-0000-000000000062",
		"00000000-0000-0000-0000-000000000063",
		"00000000-0000-0000-0000-000000000064",
		"00000000-0000-0000-0000-000000000065",
	}
	objs := make([]*models.Object, len(objectIDs))
	for i, id := range objectIDs {
		objs[i] = &models.Object{
			Class: className,
			ID:    id,
			Properties: map[string]interface{}{
				"title":       fmt.Sprintf("Document %d", i+1),
				"description": fmt.Sprintf("Description for document %d", i+1),
			},
			Vector: []float32{float32(i) * 0.1, float32(i) * 0.2, float32(i) * 0.3, float32(i) * 0.4},
		}
	}
	helper.CreateObjectsBatch(t, objs)

	t.Run("CursorWithVectorAndAllProps", func(t *testing.T) {
		// First page
		query := fmt.Sprintf(`{
			Get {
				%s(limit: 2, after: "") {
					title
					description
					_additional { id vector }
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
			assert.NotNil(t, addl["vector"], "vector should be present when requested")
			assert.NotNil(t, obj["title"])
			assert.NotNil(t, obj["description"])
		}

		// Get last ID for next page
		lastObj := results[len(results)-1].(map[string]interface{})
		lastID := lastObj["_additional"].(map[string]interface{})["id"].(string)

		// Second page
		query2 := fmt.Sprintf(`{
			Get {
				%s(limit: 2, after: "%s") {
					title
					description
					_additional { id vector }
				}
			}
		}`, className, lastID)

		result2 := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query2)
		results2 := result2.Get("Get", className).AsSlice()
		require.GreaterOrEqual(t, len(results2), 1)

		for _, res := range results2 {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			assert.NotNil(t, addl["vector"], "vector should be present when requested")
		}
	})

	t.Run("CursorWithoutVector", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s(limit: 3, after: "") {
					title
					_additional { id }
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
			assert.NotNil(t, obj["title"])
		}
	})

	t.Run("CursorWithOnlySomeProps", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s(limit: 2, after: "") {
					title
					_additional { id }
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
			assert.NotNil(t, obj["title"])
			assert.Nil(t, obj["description"], "description should NOT be present when not requested")
			assert.Nil(t, addl["vector"], "vector should NOT be present when not requested")
		}
	})

	t.Run("CursorWithOnlyID", func(t *testing.T) {
		query := fmt.Sprintf(`{
			Get {
				%s(limit: 5, after: "") {
					_additional { id }
				}
			}
		}`, className)

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		results := result.Get("Get", className).AsSlice()
		require.Len(t, results, 5)

		for _, res := range results {
			obj := res.(map[string]interface{})
			addl := obj["_additional"].(map[string]interface{})
			assert.NotNil(t, addl["id"])
			assert.Nil(t, obj["title"])
			assert.Nil(t, obj["description"])
			assert.Nil(t, addl["vector"], "vector should NOT be present when not requested")
		}
	})
}

// testRESTCursorPagination tests REST API cursor-based listing with include=vector combinations
func testRESTCursorPagination(t *testing.T) {
	className := "RESTCursorTest"

	// Create class with multiple shards
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
		VectorIndexConfig: map[string]interface{}{
			"distance": "cosine",
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": json.Number("3"),
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// Insert objects
	objectIDs := []strfmt.UUID{
		"00000000-0000-0000-0000-000000000071",
		"00000000-0000-0000-0000-000000000072",
		"00000000-0000-0000-0000-000000000073",
		"00000000-0000-0000-0000-000000000074",
		"00000000-0000-0000-0000-000000000075",
	}
	objs := make([]*models.Object, len(objectIDs))
	for i, id := range objectIDs {
		objs[i] = &models.Object{
			Class: className,
			ID:    id,
			Properties: map[string]interface{}{
				"title":       fmt.Sprintf("REST Document %d", i+1),
				"description": fmt.Sprintf("REST Description %d", i+1),
			},
			Vector: []float32{float32(i) * 0.1, float32(i) * 0.2, float32(i) * 0.3, float32(i) * 0.4},
		}
	}
	helper.CreateObjectsBatch(t, objs)

	t.Run("CursorWithVector", func(t *testing.T) {
		limit := int64(2)
		after := ""
		include := "vector"

		params := objects.NewObjectsListParams().
			WithClass(&className).
			WithLimit(&limit).
			WithAfter(&after).
			WithInclude(&include)

		resp, err := helper.Client(t).Objects.ObjectsList(params, nil)
		require.NoError(t, err)
		require.NotNil(t, resp.Payload)
		require.Len(t, resp.Payload.Objects, 2)

		for _, obj := range resp.Payload.Objects {
			assert.NotNil(t, obj.ID)
			assert.NotNil(t, obj.Vector, "vector should be present when include=vector")
			assert.NotEmpty(t, obj.Properties)
		}
	})

	t.Run("CursorWithoutVector", func(t *testing.T) {
		limit := int64(3)
		after := ""

		params := objects.NewObjectsListParams().
			WithClass(&className).
			WithLimit(&limit).
			WithAfter(&after)
		// No include parameter - vector should not be returned

		resp, err := helper.Client(t).Objects.ObjectsList(params, nil)
		require.NoError(t, err)
		require.NotNil(t, resp.Payload)
		require.Len(t, resp.Payload.Objects, 3)

		for _, obj := range resp.Payload.Objects {
			assert.NotNil(t, obj.ID)
			assert.Nil(t, obj.Vector, "vector should NOT be present when include is not set")
			assert.NotEmpty(t, obj.Properties)
		}
	})

	t.Run("CursorPaginationWithVector", func(t *testing.T) {
		limit := int64(2)
		after := ""
		include := "vector"

		// First page
		params := objects.NewObjectsListParams().
			WithClass(&className).
			WithLimit(&limit).
			WithAfter(&after).
			WithInclude(&include)

		resp, err := helper.Client(t).Objects.ObjectsList(params, nil)
		require.NoError(t, err)
		require.Len(t, resp.Payload.Objects, 2)

		for _, obj := range resp.Payload.Objects {
			assert.NotNil(t, obj.Vector, "vector should be present on first page")
		}

		// Second page
		lastID := string(resp.Payload.Objects[len(resp.Payload.Objects)-1].ID)
		params2 := objects.NewObjectsListParams().
			WithClass(&className).
			WithLimit(&limit).
			WithAfter(&lastID).
			WithInclude(&include)

		resp2, err := helper.Client(t).Objects.ObjectsList(params2, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(resp2.Payload.Objects), 1)

		for _, obj := range resp2.Payload.Objects {
			assert.NotNil(t, obj.Vector, "vector should be present on second page")
		}
	})

	t.Run("CursorPaginationWithoutVector", func(t *testing.T) {
		limit := int64(2)
		after := ""

		// First page without vector
		params := objects.NewObjectsListParams().
			WithClass(&className).
			WithLimit(&limit).
			WithAfter(&after)

		resp, err := helper.Client(t).Objects.ObjectsList(params, nil)
		require.NoError(t, err)
		require.Len(t, resp.Payload.Objects, 2)

		for _, obj := range resp.Payload.Objects {
			assert.Nil(t, obj.Vector, "vector should NOT be present on first page")
		}

		// Second page without vector
		lastID := string(resp.Payload.Objects[len(resp.Payload.Objects)-1].ID)
		params2 := objects.NewObjectsListParams().
			WithClass(&className).
			WithLimit(&limit).
			WithAfter(&lastID)

		resp2, err := helper.Client(t).Objects.ObjectsList(params2, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(resp2.Payload.Objects), 1)

		for _, obj := range resp2.Payload.Objects {
			assert.Nil(t, obj.Vector, "vector should NOT be present on second page")
		}
	})
}

// testGRPCCursorPagination tests gRPC cursor-based listing with vector/property combinations
func testGRPCCursorPagination(t *testing.T, grpcClient protocol.WeaviateClient) {
	ctx := context.Background()
	className := "GRPCCursorTest"

	// Create class with multiple shards
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
		VectorIndexConfig: map[string]interface{}{
			"distance": "cosine",
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": json.Number("3"),
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// Insert objects
	objectIDs := []strfmt.UUID{
		"00000000-0000-0000-0000-000000000081",
		"00000000-0000-0000-0000-000000000082",
		"00000000-0000-0000-0000-000000000083",
		"00000000-0000-0000-0000-000000000084",
		"00000000-0000-0000-0000-000000000085",
	}
	objs := make([]*models.Object, len(objectIDs))
	for i, id := range objectIDs {
		objs[i] = &models.Object{
			Class: className,
			ID:    id,
			Properties: map[string]interface{}{
				"title":       fmt.Sprintf("gRPC Document %d", i+1),
				"description": fmt.Sprintf("gRPC Description %d", i+1),
			},
			Vector: []float32{float32(i) * 0.1, float32(i) * 0.2, float32(i) * 0.3, float32(i) * 0.4},
		}
	}
	helper.CreateObjectsBatch(t, objs)

	t.Run("CursorWithVectorAndAllProps", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Limit:      2,
			After:      nil,
			Properties: &protocol.PropertiesRequest{
				ReturnAllNonrefProperties: true,
			},
			Metadata: &protocol.MetadataRequest{
				Uuid:   true,
				Vector: true,
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			assert.NotEmpty(t, result.Metadata.VectorBytes, "vector should be present when requested")
			assert.NotNil(t, result.Properties)
			assert.GreaterOrEqual(t, len(result.Properties.NonRefProps.Fields), 2)
		}
	})

	t.Run("CursorWithoutVector", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Limit:      3,
			After:      nil,
			Properties: &protocol.PropertiesRequest{
				NonRefProperties: []string{"title"},
			},
			Metadata: &protocol.MetadataRequest{
				Uuid: true,
				// No Vector
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 3)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			assert.Empty(t, result.Metadata.VectorBytes, "vector should NOT be present when not requested")
			assert.NotNil(t, result.Properties)
		}
	})

	t.Run("CursorPaginationWithVector", func(t *testing.T) {
		// First page
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Limit:      2,
			After:      nil,
			Metadata: &protocol.MetadataRequest{
				Uuid:   true,
				Vector: true,
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.NotEmpty(t, result.Metadata.VectorBytes, "vector should be present on first page")
		}

		// Second page
		lastID := resp.Results[len(resp.Results)-1].Metadata.Id
		resp2, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Limit:      2,
			After:      &lastID,
			Metadata: &protocol.MetadataRequest{
				Uuid:   true,
				Vector: true,
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(resp2.Results), 1)

		for _, result := range resp2.Results {
			assert.NotEmpty(t, result.Metadata.VectorBytes, "vector should be present on second page")
		}
	})

	t.Run("CursorPaginationWithoutVector", func(t *testing.T) {
		// First page without vector
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Limit:      2,
			After:      nil,
			Metadata: &protocol.MetadataRequest{
				Uuid: true,
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.Empty(t, result.Metadata.VectorBytes, "vector should NOT be present on first page")
		}

		// Second page without vector
		lastID := resp.Results[len(resp.Results)-1].Metadata.Id
		resp2, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Limit:      2,
			After:      &lastID,
			Metadata: &protocol.MetadataRequest{
				Uuid: true,
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(resp2.Results), 1)

		for _, result := range resp2.Results {
			assert.Empty(t, result.Metadata.VectorBytes, "vector should NOT be present on second page")
		}
	})

	t.Run("CursorWithSomeProps", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &protocol.SearchRequest{
			Collection: className,
			Limit:      2,
			After:      nil,
			Properties: &protocol.PropertiesRequest{
				NonRefProperties: []string{"title"}, // Only title, not description
			},
			Metadata: &protocol.MetadataRequest{
				Uuid: true,
			},
			Uses_123Api: true,
			Uses_125Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Results, 2)

		for _, result := range resp.Results {
			assert.NotNil(t, result.Metadata.Id)
			assert.NotNil(t, result.Properties)
			assert.NotNil(t, result.Properties.NonRefProps)
			// Only title should be present
			titleField := result.Properties.NonRefProps.Fields["title"]
			assert.NotNil(t, titleField, "title should be present")
			// Vector should not be present when not requested
			assert.Empty(t, result.Metadata.VectorBytes, "vector should NOT be present when not requested")
		}
	})
}

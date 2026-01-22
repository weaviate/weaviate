//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright (c) 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func TestLocalGetQueryVectorExtensions(t *testing.T) {
	helper.SetupClient("localhost:8080")

	className := "QueryVectorExt" + sanitizeClassSuffix(t.Name())
	vector := []float32{0.1, 0.2, 0.3}

	createQueryVectorClass(t, className)
	t.Cleanup(func() {
		deleteQueryVectorClass(t, className)
	})

	createQueryVectorObject(t, className, vector)

	query := fmt.Sprintf(`{
		Get {
			%s(nearVector:{vector:%s}) {
				_additional { id }
			}
		}
	}`, className, graphqlhelper.Vec2String(vector))

	response := runGraphQLQuery(t, query)
	queryVector := extractDefaultQueryVector(t, response)
	require.Greater(t, len(queryVector), 0)
}

func sanitizeClassSuffix(name string) string {
	return strings.ReplaceAll(name, "/", "_")
}

func createQueryVectorClass(t *testing.T, className string) {
	t.Helper()

	createObjectClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	})
}

func deleteQueryVectorClass(t *testing.T, className string) {
	t.Helper()
	deleteObjectClass(t, className)
}

func createQueryVectorObject(t *testing.T, className string, vector []float32) {
	t.Helper()

	createObject(t, &models.Object{
		Class:      className,
		Vector:     vector,
		Properties: map[string]interface{}{"name": "Query vector test"},
	})
}

func runGraphQLQuery(t *testing.T, query string) *models.GraphQLResponse {
	t.Helper()
	return graphqlhelper.QueryGraphQLOrFatal(t, helper.RootAuth, "", query, nil)
}

func extractDefaultQueryVector(t *testing.T, response *models.GraphQLResponse) []float64 {
	t.Helper()

	if response == nil || response.Extensions == nil {
		t.Fatalf("expected extensions to be present")
	}

	extensions := asStringMap(t, response.Extensions)
	metadata := asStringMap(t, extensions["metadata"])
	vectorizer := asStringMap(t, metadata["vectorizer"])
	queryVectors := asStringMap(t, vectorizer["queryVectors"])

	rawDefault := queryVectors["default"]
	rawSlice, ok := rawDefault.([]interface{})
	require.True(t, ok, "expected default query vector to be a slice")

	return toFloat64Slice(t, rawSlice)
}

func asStringMap(t *testing.T, value interface{}) map[string]interface{} {
	t.Helper()
	typed, ok := value.(map[string]interface{})
	require.True(t, ok, "expected map[string]interface{}, got %T", value)
	return typed
}

func toFloat64Slice(t *testing.T, values []interface{}) []float64 {
	t.Helper()
	out := make([]float64, 0, len(values))
	for _, value := range values {
		switch typed := value.(type) {
		case json.Number:
			parsed, err := typed.Float64()
			require.NoError(t, err)
			out = append(out, parsed)
		case float64:
			out = append(out, typed)
		default:
			t.Fatalf("unexpected vector element type: %T", value)
		}
	}
	return out
}

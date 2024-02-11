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

package named_vectors_tests

import (
	acceptance_with_go_client "acceptance_tests_with_client"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var (
	className                  = "NamedVectors"
	text2vecContextionaryName1 = "c11y1"
	text2vecContextionaryName2 = "c11y2_flat"
	text2vecContextionaryName3 = "c11y2_pq"
	transformersName1          = "transformers1"
	transformersName2          = "transformers2_flat"
	transformersName3          = "transformers2_pq"
	text2vecContextionary      = "text2vec-contextionary"
	text2vecTransformers       = "text2vec-transformers"
	id1                        = "00000000-0000-0000-0000-000000000001"
	id2                        = "00000000-0000-0000-0000-000000000002"
)

func createNamedVectorsClass(t *testing.T, client *wvt.Client) {
	ctx := context.Background()
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name: "text", DataType: []string{schema.DataTypeText.String()},
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			text2vecContextionaryName1: {
				Vectorizer: map[string]interface{}{
					text2vecContextionary: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "hnsw",
			},
			text2vecContextionaryName2: {
				Vectorizer: map[string]interface{}{
					text2vecContextionary: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
			text2vecContextionaryName3: {
				Vectorizer: map[string]interface{}{
					text2vecContextionary: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: pqVectorIndexConfig(),
			},
			transformersName1: {
				Vectorizer: map[string]interface{}{
					text2vecTransformers: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "hnsw",
			},
			transformersName2: {
				Vectorizer: map[string]interface{}{
					text2vecTransformers: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
			transformersName3: {
				Vectorizer: map[string]interface{}{
					text2vecTransformers: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: pqVectorIndexConfig(),
			},
		},
		Vectorizer: text2vecContextionary,
	}

	err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
	require.NoError(t, err)

	cls, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
	require.NoError(t, err)
	assert.Equal(t, class.Class, cls.Class)
	require.NotEmpty(t, cls.VectorConfig)
	require.Len(t, cls.VectorConfig, 6)
	targetVectors := []string{
		text2vecContextionaryName1, text2vecContextionaryName2, text2vecContextionaryName3,
		transformersName1, transformersName2, transformersName3,
	}
	for _, name := range targetVectors {
		require.NotEmpty(t, cls.VectorConfig[name])
		assert.Equal(t, class.VectorConfig[name].VectorIndexType, cls.VectorConfig[name].VectorIndexType)
		vectorizerConfig, ok := cls.VectorConfig[name].Vectorizer.(map[string]interface{})
		require.True(t, ok)
		vectorizerName := text2vecContextionary
		if strings.HasPrefix(name, "transformers") {
			vectorizerName = text2vecTransformers
		}
		require.NotEmpty(t, vectorizerConfig[vectorizerName])
	}
}

func pqVectorIndexConfig() map[string]interface{} {
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	vectorCacheMaxObjects := 10e12

	return map[string]interface{}{
		"maxConnections":        maxNeighbors,
		"efConstruction":        efConstruction,
		"ef":                    ef,
		"vectorCacheMaxObjects": vectorCacheMaxObjects,
		"distance":              "l2-squared",
		"pq": map[string]interface{}{
			"enabled": true,
			"encoder": map[string]interface{}{
				"distribution": hnsw.PQEncoderDistributionNormal,
				"type":         hnsw.PQEncoderTypeKMeans,
			},
		},
	}
}

func getVectors(t *testing.T, client *wvt.Client, className, id string, targetVectors ...string) map[string][]float32 {
	where := filters.Where().
		WithPath([]string{"id"}).
		WithOperator(filters.Equal).
		WithValueText(id)
	field := graphql.Field{
		Name: "_additional",
		Fields: []graphql.Field{
			{Name: "id"},
			{Name: fmt.Sprintf("vectors{%s}", strings.Join(targetVectors, " "))},
		},
	}
	resp, err := client.GraphQL().Get().
		WithClassName(className).
		WithWhere(where).
		WithFields(field).
		Do(context.Background())
	require.NoError(t, err)

	ids := acceptance_with_go_client.GetIds(t, resp, className)
	require.ElementsMatch(t, ids, []string{id})

	return acceptance_with_go_client.GetVectors(t, resp, className, targetVectors...)
}

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
	"context"
	"fmt"
	"strings"
	"testing"

	acceptance_with_go_client "acceptance_tests_with_client"

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
	className                           = "NamedVectors"
	c11y                                = "c11y"
	c11y_pq                             = "c11y_pq"
	c11y_flat                           = "c11y_flat"
	c11y_bq                             = "c11y_bq"
	c11y_pq_very_long_230_chars         = "c11y_pq______bq_b9mgu3N7rCUWufddpfCqaVvr4IUjB9xpMBrmiQFIqyuUxKx5s8wCTD7iWb5gPkwNhECumphBMWXD67G9gvN4CQkylG3bDrR8p9sK02RLOGvE96jcaSKjpZrIRvjJuQliGf8BMNmzXEqH39UWGGt4zPNnZNvdPP6pIzxWG5zNpymGmJJLCHk6yP1eO3QgSdXMt0arzfcrAA1L9uZNIVT7tM"
	transformers                        = "transformers"
	transformers_flat                   = "transformers_flat"
	transformers_pq                     = "transformers_pq"
	transformers_bq                     = "transformers_bq"
	transformers_bq_very_long_230_chars = "transformers_bq_b9mgu3N7rCUWufddpfCqaVvr4IUjB9xpMBrmiQFIqyuUxKx5s8wCTD7iWb5gPkwNhECumphBMWXD67G9gvN4CQkylG3bDrR8p9sK02RLOGvE96jcaSKjpZrIRvjJuQliGf8BMNmzXEqH39UWGGt4zPNnZNvdPP6pIzxWG5zNpymGmJJLCHk6yP1eO3QgSdXMt0arzfcrAA1L9uZNIVT7tM"
	text2vecContextionary               = "text2vec-contextionary"
	text2vecTransformers                = "text2vec-transformers"
	id1                                 = "00000000-0000-0000-0000-000000000001"
	id2                                 = "00000000-0000-0000-0000-000000000002"
)

var targetVectors = []string{
	c11y, c11y_flat, c11y_pq, c11y_bq,
	transformers, transformers_flat, transformers_pq, transformers_bq,
	transformers_bq_very_long_230_chars, c11y_pq_very_long_230_chars,
}

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
			c11y: {
				Vectorizer: map[string]interface{}{
					text2vecContextionary: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "hnsw",
			},
			c11y_flat: {
				Vectorizer: map[string]interface{}{
					text2vecContextionary: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
			c11y_pq: {
				Vectorizer: map[string]interface{}{
					text2vecContextionary: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: pqVectorIndexConfig(),
			},
			c11y_bq: {
				Vectorizer: map[string]interface{}{
					text2vecContextionary: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType:   "flat",
				VectorIndexConfig: bqFlatIndexConfig(),
			},
			c11y_pq_very_long_230_chars: {
				Vectorizer: map[string]interface{}{
					text2vecContextionary: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: pqVectorIndexConfig(),
			},
			transformers: {
				Vectorizer: map[string]interface{}{
					text2vecTransformers: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "hnsw",
			},
			transformers_flat: {
				Vectorizer: map[string]interface{}{
					text2vecTransformers: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
			transformers_pq: {
				Vectorizer: map[string]interface{}{
					text2vecTransformers: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: pqVectorIndexConfig(),
			},
			transformers_bq: {
				Vectorizer: map[string]interface{}{
					text2vecTransformers: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType:   "flat",
				VectorIndexConfig: bqFlatIndexConfig(),
			},
			transformers_bq_very_long_230_chars: {
				Vectorizer: map[string]interface{}{
					text2vecTransformers: map[string]interface{}{
						"vectorizeClassName": false,
					},
				},
				VectorIndexType:   "flat",
				VectorIndexConfig: bqFlatIndexConfig(),
			},
		},
	}

	err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
	require.NoError(t, err)

	cls, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
	require.NoError(t, err)
	assert.Equal(t, class.Class, cls.Class)
	require.NotEmpty(t, cls.VectorConfig)
	require.Len(t, cls.VectorConfig, len(targetVectors))
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

func bqFlatIndexConfig() map[string]interface{} {
	return map[string]interface{}{
		"bq": map[string]interface{}{
			"enabled": true,
		},
	}
}

func getVectorsWithNearText(t *testing.T, client *wvt.Client,
	className, id string, nearText *graphql.NearTextArgumentBuilder, targetVectors ...string,
) map[string][]float32 {
	return getVectorsWithNearArgs(t, client, className, id, nearText, nil, nil, nil, false, targetVectors...)
}

func getVectorsWithNearTextWithCertainty(t *testing.T, client *wvt.Client,
	className, id string, nearText *graphql.NearTextArgumentBuilder, targetVectors ...string,
) map[string][]float32 {
	return getVectorsWithNearArgs(t, client, className, id, nearText, nil, nil, nil, true, targetVectors...)
}

func getVectorsWithNearVector(t *testing.T, client *wvt.Client,
	className, id string, nearVector *graphql.NearVectorArgumentBuilder, targetVectors ...string,
) map[string][]float32 {
	return getVectorsWithNearArgs(t, client, className, id, nil, nearVector, nil, nil, false, targetVectors...)
}

func getVectorsWithNearVectorWithCertainty(t *testing.T, client *wvt.Client,
	className, id string, nearVector *graphql.NearVectorArgumentBuilder, targetVectors ...string,
) map[string][]float32 {
	return getVectorsWithNearArgs(t, client, className, id, nil, nearVector, nil, nil, true, targetVectors...)
}

func getVectorsWithNearObjectWithCertainty(t *testing.T, client *wvt.Client,
	className, id string, nearObject *graphql.NearObjectArgumentBuilder, targetVectors ...string,
) map[string][]float32 {
	return getVectorsWithNearArgs(t, client, className, id, nil, nil, nearObject, nil, true, targetVectors...)
}

func getVectorsWithNearArgs(t *testing.T, client *wvt.Client,
	className, id string,
	nearText *graphql.NearTextArgumentBuilder,
	nearVector *graphql.NearVectorArgumentBuilder,
	nearObject *graphql.NearObjectArgumentBuilder,
	hybrid *graphql.HybridArgumentBuilder,
	withCertainty bool,
	targetVectors ...string,
) map[string][]float32 {
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
	if withCertainty {
		field.Fields = append(field.Fields, graphql.Field{Name: "certainty"})
	}
	get := client.GraphQL().Get().
		WithClassName(className).
		WithWhere(where).
		WithNearText(nearText).
		WithFields(field)

	if nearText != nil {
		get = get.WithNearText(nearText)
	}
	if nearVector != nil {
		get = get.WithNearVector(nearVector)
	}
	if nearObject != nil {
		get = get.WithNearObject(nearObject)
	}
	if hybrid != nil {
		get = get.WithHybrid(hybrid)
	}

	resp, err := get.Do(context.Background())
	require.NoError(t, err)

	ids := acceptance_with_go_client.GetIds(t, resp, className)
	require.ElementsMatch(t, ids, []string{id})

	return acceptance_with_go_client.GetVectors(t, resp, className, withCertainty, targetVectors...)
}

func getVectors(t *testing.T, client *wvt.Client,
	className, id string, targetVectors ...string,
) map[string][]float32 {
	return getVectorsWithNearText(t, client, className, id, nil, targetVectors...)
}

func checkTargetVectors(t *testing.T, resultVectors map[string][]float32) {
	require.NotEmpty(t, resultVectors[c11y])
	require.NotEmpty(t, resultVectors[c11y_flat])
	require.NotEmpty(t, resultVectors[c11y_pq])
	require.NotEmpty(t, resultVectors[c11y_bq])
	require.NotEmpty(t, resultVectors[transformers])
	require.NotEmpty(t, resultVectors[transformers_flat])
	require.NotEmpty(t, resultVectors[transformers_pq])
	require.NotEmpty(t, resultVectors[transformers_bq])
	assert.Equal(t, resultVectors[c11y], resultVectors[c11y_flat])
	assert.Equal(t, resultVectors[c11y_flat], resultVectors[c11y_pq])
	assert.Equal(t, resultVectors[c11y_pq], resultVectors[c11y_bq])
	assert.Equal(t, resultVectors[transformers], resultVectors[transformers_flat])
	assert.Equal(t, resultVectors[transformers_flat], resultVectors[transformers_pq])
	assert.Equal(t, resultVectors[transformers_pq], resultVectors[transformers_bq])
	assert.NotEqual(t, resultVectors[c11y], resultVectors[transformers])
	assert.NotEqual(t, resultVectors[c11y_flat], resultVectors[transformers_flat])
	assert.NotEqual(t, resultVectors[c11y_pq], resultVectors[transformers_pq])
	assert.NotEqual(t, resultVectors[c11y_bq], resultVectors[transformers_bq])
}

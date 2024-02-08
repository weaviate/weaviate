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

	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

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

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package acceptance_with_go_client

import (
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

var (
	vFalse = false
	vTrue  = true
)

func GetIds(t *testing.T, resp *models.GraphQLResponse, className string) []string {
	return ExtractGraphQLField[string](t, resp, "Get", className, "_additional", "id")
}

func ExtractGraphQLField[T any](t *testing.T, resp *models.GraphQLResponse, path ...string) []T {
	require.NotNil(t, resp)
	for _, err := range resp.Errors {
		t.Logf("ExtractGraphQLField error: %v", err)
	}
	require.Empty(t, resp.Errors)

	require.NotNil(t, resp.Data)
	classMap, ok := resp.Data[path[0]].(map[string]interface{})
	require.True(t, ok)

	objects, ok := classMap[path[1]].([]interface{})
	require.True(t, ok)

	results := make([]T, len(objects))
	for i := range objects {
		resultMap, ok := objects[i].(map[string]interface{})
		for j := 2; j < len(path)-1; j++ {
			resultMap, ok = resultMap[path[j]].(map[string]interface{})
			require.True(t, ok)
		}

		results[i], ok = resultMap[path[len(path)-1]].(T)
		require.True(t, ok, fmt.Sprintf("failed to extract %s from response: %s", path[len(path)-1], spew.Sdump(resp)))
	}
	return results
}

func GetVectors(t *testing.T,
	resp *models.GraphQLResponse,
	className string,
	withCertainty bool,
	targetVectors ...string,
) map[string]models.Vector {
	require.NotNil(t, resp)
	require.NotNil(t, resp.Data)
	require.Empty(t, resp.Errors)

	classMap, ok := resp.Data["Get"].(map[string]interface{})
	require.True(t, ok)

	class, ok := classMap[className].([]interface{})
	require.True(t, ok)

	targetVectorsMap := make(map[string]models.Vector)
	for i := range class {
		resultMap, ok := class[i].(map[string]interface{})
		require.True(t, ok)

		additional, ok := resultMap["_additional"].(map[string]interface{})
		require.True(t, ok)

		if withCertainty {
			certainty, ok := additional["certainty"].(float64)
			require.True(t, ok)
			require.True(t, certainty >= 0)
		}

		vectors, ok := additional["vectors"].(map[string]interface{})
		require.True(t, ok)

		for _, targetVector := range targetVectors {
			if targetVector == "" {
				targetVectorsMap[""] = parseVector(t, additional["vector"])
				continue
			}

			targetVectorsMap[targetVector] = parseVector(t, vectors[targetVector])
		}
	}

	return targetVectorsMap
}

func parseVector(t *testing.T, data interface{}) models.Vector {
	vector, ok := data.([]interface{})
	require.Truef(t, ok, "unexpected vector types in GraphQL response: %T", data)

	var multiVector [][]float32
	var vec []float32
	for i := range vector {
		switch v := vector[i].(type) {
		case float64:
			vec = append(vec, float32(v))
		case []interface{}:
			multiVectorVector := make([]float32, len(v))
			for j := range v {
				multiVectorVector[j] = float32(v[j].(float64))
			}
			multiVector = append(multiVector, multiVectorVector)
		}
	}

	if len(multiVector) > 0 {
		return multiVector
	}
	return vec
}

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

package acceptance_with_go_client

import (
	"testing"
	"fmt"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

var (
	vFalse = false
	vTrue  = true
)

func GetIds(t *testing.T, resp *models.GraphQLResponse, className string) []string {
	t.Logf("GetIds: %+v for class %s", resp, className)
	require.NotNil(t, resp)
	t.Logf("GetIds data: %v", resp.Data)
	require.NotNil(t, resp.Data)
	t.Logf("GetIds errors: %+v", resp.Errors)
	for _, err := range resp.Errors {
		t.Logf("GetIds error: %v", err)
		for _, loc := range err.Locations {
			t.Logf("GetIds error location: %v", loc)
		}
		t.Logf("GetIds error path: %v", err.Path)
	}
	if resp.Errors != nil {
		for _, err := range resp.Errors {
			fmt.Printf("GetIds error: %v", err)
			for _, loc := range err.Locations {
				fmt.Printf("GetIds error location: %v", loc)
			}
			fmt.Printf("GetIds error path: %v", err.Path)
		}
		require.Empty(t, resp.Errors)
	}
	require.Empty(t, resp.Errors)

	classMap, ok := resp.Data["Get"].(map[string]interface{})
	require.True(t, ok)

	class, ok := classMap[className].([]interface{})
	require.True(t, ok)

	ids := make([]string, len(class))
	for i := range class {
		resultMap, ok := class[i].(map[string]interface{})
		require.True(t, ok)

		additional, ok := resultMap["_additional"].(map[string]interface{})
		require.True(t, ok)

		ids[i] = additional["id"].(string)
	}

	return ids
}

func GetVectors(t *testing.T,
	resp *models.GraphQLResponse,
	className string,
	withCertainty bool,
	targetVectors ...string,
) map[string][]float32 {
	require.NotNil(t, resp)
	require.NotNil(t, resp.Data)
	require.Empty(t, resp.Errors)

	classMap, ok := resp.Data["Get"].(map[string]interface{})
	require.True(t, ok)

	class, ok := classMap[className].([]interface{})
	require.True(t, ok)

	targetVectorsMap := make(map[string][]float32)
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
			vector, ok := vectors[targetVector].([]interface{})
			require.True(t, ok)

			vec := make([]float32, len(vector))
			for i := range vector {
				vec[i] = float32(vector[i].(float64))
			}

			targetVectorsMap[targetVector] = vec
		}
	}

	return targetVectorsMap
}

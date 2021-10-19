//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testGetSchemaWithoutClient(t *testing.T) {
	res, err := http.Get(fmt.Sprintf("%s%s", helper.GetWeaviateURL(), "/v1/schema"))
	require.Nil(t, err)

	defer res.Body.Close()
	var body map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&body)
	require.Nil(t, err)

	expected := map[string]interface{}{
		"classes": []interface{}{
			map[string]interface{}{
				"class":           "YellowCars",
				"properties":      (interface{})(nil),
				"vectorIndexType": "hnsw", // from default
				"vectorIndexConfig": map[string]interface{}{ // from default
					"skip":                   false,
					"cleanupIntervalSeconds": float64(300),
					"efConstruction":         float64(128),
					"flatSearchCutoff":       float64(40000),
					"ef":                     float64(-1),
					"maxConnections":         float64(64),
					"vectorCacheMaxObjects":  float64(2e6),
				},
				"shardingConfig": map[string]interface{}{
					"actualCount":         float64(1),
					"actualVirtualCount":  float64(128),
					"desiredCount":        float64(1),
					"desiredVirtualCount": float64(128),
					"function":            "murmur3",
					"strategy":            "hash",
					"key":                 "_id",
					"virtualPerPhysical":  float64(128),
				},
				"vectorizer": "text2vec-contextionary", // global default from env var, see docker-compose-test.yml
				"invertedIndexConfig": map[string]interface{}{
					"cleanupIntervalSeconds": float64(60),
				},
				"moduleConfig": map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizeClassName": true,
					},
				},
			},
		},
	}

	assert.Equal(t, expected, body)
}

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

package test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/test/helper"
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
					"vectorCacheMaxObjects":  float64(1e12),
					"dynamicEfMin":           float64(100),
					"dynamicEfMax":           float64(500),
					"dynamicEfFactor":        float64(8),
					"distance":               "cosine",
					"bq": map[string]interface{}{
						"enabled": false,
					},
					"pq": map[string]interface{}{
						"bitCompression": false,
						"centroids":      float64(256),
						"enabled":        false,
						"encoder": map[string]interface{}{
							"distribution": "log-normal",
							"type":         hnsw.PQEncoderTypeKMeans,
						},
						"segments":      float64(0),
						"trainingLimit": float64(100000),
					},
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
				"replicationConfig": map[string]interface{}{
					"factor": float64(1),
				},
				"vectorizer": "text2vec-contextionary", // global default from env var, see docker-compose-test.yml
				"invertedIndexConfig": map[string]interface{}{
					"cleanupIntervalSeconds": float64(60),
					"bm25": map[string]interface{}{
						"k1": float64(1.2),
						"b":  float64(0.75),
					},
					"stopwords": map[string]interface{}{
						"preset":    "en",
						"additions": nil,
						"removals":  nil,
					},
				},
				"moduleConfig": map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizeClassName": true,
					},
				},
				"multiTenancyConfig": map[string]interface{}{"enabled": false},
			},
		},
	}

	assert.Equal(t, expected, body)
}

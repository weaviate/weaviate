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
	"bufio"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const metricClassPrefix = "MetricsClassPrefix"

func metricsCount(t *testing.T) {
	defer cleanupMetricsClasses(t, 0, 20)
	createImportQueryMetricsClasses(t, 0, 10)
	metricsLinesBefore := countMetricsLines(t)
	createImportQueryMetricsClasses(t, 10, 20)
	metricsLinesAfter := countMetricsLines(t)
	assert.Equal(t, metricsLinesBefore, metricsLinesAfter, "number of metrics should not have changed")
}

func createImportQueryMetricsClasses(t *testing.T, start, end int) {
	for i := start; i < end; i++ {
		createMetricsClass(t, i)
		importMetricsClass(t, i)
		queryMetricsClass(t, i)
	}
}

func createMetricsClass(t *testing.T, classIndex int) {
	createObjectClass(t, &models.Class{
		Class:      metricsClassName(classIndex),
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "some_text",
				DataType: schema.DataTypeText.PropString(),
			},
		},
		VectorIndexConfig: map[string]any{
			"efConstruction": 10,
			"maxConnextions": 2,
			"ef":             10,
		},
	})
}

func queryMetricsClass(t *testing.T, classIndex int) {
	// object by ID which exists
	resp, err := helper.Client(t).Objects.
		ObjectsClassGet(
			objects.NewObjectsClassGetParams().
				WithID(helper.IntToUUID(1)).
				WithClassName(metricsClassName(classIndex)),
			nil)

	require.Nil(t, err)
	assert.NotNil(t, resp.Payload)

	// object by ID which doesn't exist
	// ignore any return values
	helper.Client(t).Objects.
		ObjectsClassGet(
			objects.NewObjectsClassGetParams().
				WithID(helper.IntToUUID(math.MaxUint64)).
				WithClassName(metricsClassName(classIndex)),
			nil)

	// vector search
	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth,
		fmt.Sprintf(
			"{  Get { %s(nearVector:{vector: [0.3,0.3,0.7,0.7]}, limit:5) { some_text } } }",
			metricsClassName(classIndex),
		),
	)
	objs := result.Get("Get", metricsClassName(classIndex)).AsSlice()
	assert.Len(t, objs, 5)

	// filtered vector search (which has specific metrics)
	// vector search
	result = graphqlhelper.AssertGraphQL(t, helper.RootAuth,
		fmt.Sprintf(
			"{  Get { %s(nearVector:{vector:[0.3,0.3,0.7,0.7]}, limit:5, where: %s) { some_text } } }",
			metricsClassName(classIndex),
			`{operator:Equal, valueText: "individually", path:["some_text"]}`,
		),
	)
	objs = result.Get("Get", metricsClassName(classIndex)).AsSlice()
	assert.Len(t, objs, 1)
}

// make sure that we use both individual as well as batch imports, as they
// might produce different metrics
func importMetricsClass(t *testing.T, classIndex int) {
	// individual
	createObject(t, &models.Object{
		Class: metricsClassName(classIndex),
		Properties: map[string]interface{}{
			"some_text": "this object was created individually",
		},
		ID:     helper.IntToUUID(1),
		Vector: randomVector(4),
	})

	// with batches
	const (
		batchSize  = 100
		numBatches = 50
	)

	for i := 0; i < numBatches; i++ {
		batch := make([]*models.Object, batchSize)
		for j := 0; j < batchSize; j++ {
			batch[j] = &models.Object{
				Class: metricsClassName(classIndex),
				Properties: map[string]interface{}{
					"some_text": fmt.Sprintf("this is object %d of batch %d", j, i),
				},
				Vector: randomVector(4),
			}
		}

		createObjectsBatch(t, batch)
	}
}

func cleanupMetricsClasses(t *testing.T, start, end int) {
	for i := start; i < end; i++ {
		deleteObjectClass(t, metricsClassName(i))
	}
}

func randomVector(dims int) []float32 {
	out := make([]float32, dims)
	for i := range out {
		out[i] = rand.Float32()
	}
	return out
}

func countMetricsLines(t *testing.T) int {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost:2112/metrics", nil)
	require.Nil(t, err)

	c := &http.Client{}
	res, err := c.Do(req)
	require.Nil(t, err)

	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	scanner := bufio.NewScanner(res.Body)
	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "shards_loaded") || strings.Contains(line, "shards_loading") || strings.Contains(line, "shards_unloading") || strings.Contains(line, "shards_unloaded") {
			continue
		}
		require.NotContains(
			t,
			strings.ToLower(line),
			strings.ToLower(metricClassPrefix),
		)
		lineCount++
	}

	require.Nil(t, scanner.Err())

	return lineCount
}

func metricsClassName(classIndex int) string {
	return fmt.Sprintf("%s_%d", metricClassPrefix, classIndex)
}

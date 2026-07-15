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
	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const metricClassPrefix = "MetricsClassPrefix"

// metricsEndpoint is the host:port of the instance under test's Prometheus listener.
func metricsCount(t *testing.T, metricsEndpoint string) {
	defer cleanupMetricsClasses(t, 0, 20)
	createImportQueryMetricsClasses(t, 0, 10)
	backupID := startBackup(t, 0, 10)
	helper.ExpectBackupEventuallyCreated(t, backupID, "filesystem", nil, helper.WithPollInterval(time.Second), helper.WithDeadline(helper.MaxDeadline))
	metricsLinesBefore, linesBefore := countMetricsLines(t, metricsEndpoint)
	createImportQueryMetricsClasses(t, 10, 20)
	backupID = startBackup(t, 0, 20)
	helper.ExpectBackupEventuallyCreated(t, backupID, "filesystem", nil, helper.WithPollInterval(time.Second), helper.WithDeadline(helper.MaxDeadline))
	metricsLinesAfter, linesAfter := countMetricsLines(t, metricsEndpoint)
	if metricsLinesAfter != metricsLinesBefore {
		t.Logf("metric lines before:\n%s\n", strings.Join(linesBefore, "\n"))
		t.Logf("metric lines after:\n%s\n", strings.Join(linesAfter, "\n"))
	}
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
	assert.EventuallyWithT(t, func(collectT *assert.CollectT) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth,
			fmt.Sprintf(
				"{  Get { %s(nearVector:{vector: [0.3,0.3,0.7,0.7]}, limit:5) { some_text } } }",
				metricsClassName(classIndex),
			),
		)
		objs := result.Get("Get", metricsClassName(classIndex)).AsSlice()
		assert.Len(collectT, objs, 5)
	}, 15*time.Second, 500*time.Millisecond)

	// filtered vector search (which has specific metrics)
	// vector search
	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth,
		fmt.Sprintf(
			"{  Get { %s(nearVector:{vector:[0.3,0.3,0.7,0.7]}, limit:5, where: %s) { some_text } } }",
			metricsClassName(classIndex),
			`{operator:Equal, valueText: "individually", path:["some_text"]}`,
		),
	)
	objs := result.Get("Get", metricsClassName(classIndex)).AsSlice()
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

	waitForIndexing(t, metricsClassName(classIndex))
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

// lazilyMaterializedSeries only appear after first use, so their count depends on
// background LSM activity rather than class count; includes summary _sum/_count series.
var lazilyMaterializedSeries = map[string]bool{
	"file_io_writes_total_bytes":       true,
	"file_io_writes_total_bytes_sum":   true,
	"file_io_writes_total_bytes_count": true,
	"file_io_reads_total_bytes":        true,
	"file_io_reads_total_bytes_sum":    true,
	"file_io_reads_total_bytes_count":  true,
	"mmap_operations_total":            true,
}

// metricName returns the metric/series name from a Prometheus exposition line.
func metricName(line string) string {
	if strings.HasPrefix(line, "# HELP ") || strings.HasPrefix(line, "# TYPE ") {
		if fields := strings.Fields(line); len(fields) >= 3 {
			return fields[2]
		}
		return ""
	}
	if i := strings.IndexAny(line, "{ "); i != -1 {
		return line[:i]
	}
	return line
}

func countMetricsLines(t *testing.T, metricsEndpoint string) (int, []string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NotEmpty(t, metricsEndpoint, "no metrics endpoint configured for the instance under test")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("http://%s/metrics", metricsEndpoint), nil)
	require.Nil(t, err)

	c := &http.Client{}
	res, err := c.Do(req)
	require.Nil(t, err)

	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	scanner := bufio.NewScanner(res.Body)
	lineCount := 0
	var lines []string
	for scanner.Scan() {
		line := scanner.Text()
		// no line may leak a class name, including ones excluded below
		require.NotContains(
			t,
			strings.ToLower(line),
			strings.ToLower(metricClassPrefix),
		)
		if strings.Contains(line, "shards_loaded") || strings.Contains(line, "shards_loading") || strings.Contains(line, "shards_unloading") || strings.Contains(line, "shards_unloaded") {
			continue
		}
		if strings.Contains(line, "weaviate_lsm_bucket_cursor_duration_seconds") {
			continue
		}
		if strings.Contains(line, "weaviate_lsm_bucket_read_operation") {
			continue
		}
		if lazilyMaterializedSeries[metricName(line)] {
			continue
		}
		lineCount++
		lines = append(lines, line)
	}

	require.Nil(t, scanner.Err())

	return lineCount, lines
}

func metricsClassName(classIndex int) string {
	return fmt.Sprintf("%s_%d", metricClassPrefix, classIndex)
}

func startBackup(t *testing.T, start, end int) string {
	var includeClasses []string
	for i := start; i < end; i++ {
		includeClasses = append(includeClasses, metricsClassName(i))
	}

	backupID := fmt.Sprintf("metrics-test-backup-%d", rand.Intn(100000000))

	_, err := helper.Client(t).Backups.BackupsCreate(
		backups.NewBackupsCreateParams().
			WithBackend("filesystem").
			WithBody(&models.BackupCreateRequest{
				ID:      backupID,
				Include: includeClasses,
			}),
		nil)
	require.Nil(t, err)

	return backupID
}

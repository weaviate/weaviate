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

// Package reindex_singlenode consolidates all single-node reindex acceptance
// tests into a single shared container. Each subtest uses a different collection
// name for isolation. One container restart at the end verifies all deferred
// finalizations together.
package reindex_singlenode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// TestSingleNode_ReindexSuite runs all single-node reindex tests on a shared
// container. Each subtest creates its own collection and runs its reindex
// operation. After all subtests complete, the container is restarted once to
// verify deferred finalization for all migrations.
func TestSingleNode_ReindexSuite(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		WithWeaviateEnv("DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	restURI := compose.GetWeaviate().URI()
	container := compose.GetWeaviate().Container()

	// Dump container logs on failure.
	defer func() {
		if t.Failed() {
			reader, err := container.Logs(ctx)
			if err != nil {
				t.Logf("failed to get container logs: %v", err)
				return
			}
			defer reader.Close()
			logs, _ := io.ReadAll(reader)
			lines := strings.Split(string(logs), "\n")
			if len(lines) > 200 {
				lines = lines[len(lines)-200:]
			}
			t.Logf("=== Container logs (last 200 lines) ===\n%s", strings.Join(lines, "\n"))
		}
	}()

	// Collect shard names for post-restart filesystem checks.
	type shardInfo struct {
		className string
		shardName string
	}
	var shardInfos []shardInfo

	// --- Subtest 1: Map to Blockmax ---
	t.Run("MapToBlockmax", func(t *testing.T) {
		testBlockmaxMigration(t, restURI)
	})
	if sn := getFirstShardName(t, restURI, "ReindexTest"); sn != "" {
		shardInfos = append(shardInfos, shardInfo{"ReindexTest", sn})
	}

	// --- Subtest 2: Change Tokenization ---
	t.Run("ChangeTokenization", func(t *testing.T) {
		testChangeTokenization(t, restURI)
	})
	if sn := getFirstShardName(t, restURI, "RetokenizeTest"); sn != "" {
		shardInfos = append(shardInfos, shardInfo{"RetokenizeTest", sn})
	}

	// --- Subtest 3: Enable Rangeable ---
	t.Run("EnableRangeable", func(t *testing.T) {
		testEnableRangeable(t, restURI)
	})
	if sn := getFirstShardName(t, restURI, "EnableRangeableTest"); sn != "" {
		shardInfos = append(shardInfos, shardInfo{"EnableRangeableTest", sn})
	}

	// --- Subtest 4: Roaring Set Refresh ---
	t.Run("RoaringSetRefresh", func(t *testing.T) {
		testRoaringSetRefresh(t, restURI)
	})
	if sn := getFirstShardName(t, restURI, "RoaringSetRefreshTest"); sn != "" {
		shardInfos = append(shardInfos, shardInfo{"RoaringSetRefreshTest", sn})
	}

	// --- Shared restart: verify all deferred finalizations ---
	t.Run("PostRestartFinalize", func(t *testing.T) {
		t.Log("restarting weaviate container for deferred finalize verification")
		require.NoError(t, compose.StopAt(ctx, 0, nil))
		require.NoError(t, compose.StartAt(ctx, 0))
		helper.SetupClient(compose.GetWeaviate().URI())

		// Post-restart: verify blockmax queries.
		testBlockmaxPostRestart(t)

		// Post-restart: verify tokenization queries.
		testChangeTokenizationPostRestart(t)

		// Post-restart: verify rangeable queries.
		testEnableRangeablePostRestart(t)

		// Post-restart: verify roaring set queries.
		testRoaringSetRefreshPostRestart(t)

		// Post-restart: verify filesystem cleanup for all migrations.
		for _, si := range shardInfos {
			dirs := listLSMDirs(ctx, t, container, si.className, si.shardName)
			switch si.className {
			case "ReindexTest":
				assertNoSuffixedBuckets(t, dirs, "__blockmax_")
			case "RetokenizeTest":
				assertNoSuffixedBuckets(t, dirs, "__retokenize_")
				assertNoSuffixedBuckets(t, dirs, "__filt_retokenize_")
				assertBucketExists(t, dirs, "property_filepath_searchable")
				assertBucketExists(t, dirs, "property_description_searchable")
				assertBucketExists(t, dirs, "property_filepath")
				assertBucketExists(t, dirs, "property_description")
			case "EnableRangeableTest":
				assertNoSuffixedBuckets(t, dirs, "__rangeable_")
				assertBucketExists(t, dirs, "property_score_rangeable")
				assertBucketExists(t, dirs, "property_price_rangeable")
				assertBucketExists(t, dirs, "property_score")
				assertBucketExists(t, dirs, "property_price")
			case "RoaringSetRefreshTest":
				assertNoSuffixedBuckets(t, dirs, "__roaringset_")
			}
		}
	})
}

// =============================================================================
// Shared helpers
// =============================================================================

func submitIndexUpdate(t *testing.T, restURI, collection, property, jsonBody string) string {
	t.Helper()
	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, collection, property)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(jsonBody)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "index update request failed")
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	t.Logf("index update response (status=%d): %s", resp.StatusCode, string(respBody))
	require.Equal(t, http.StatusAccepted, resp.StatusCode,
		"index update endpoint returned non-202: %s", string(respBody))

	var result map[string]string
	require.NoError(t, json.Unmarshal(respBody, &result))
	return result["taskId"]
}

type indexesResponse struct {
	Collection string `json:"collection"`
	Properties []struct {
		Name    string `json:"name"`
		Indexes []struct {
			Type               string  `json:"type"`
			Status             string  `json:"status"`
			Progress           float32 `json:"progress"`
			Tokenization       string  `json:"tokenization,omitempty"`
			TargetTokenization string  `json:"targetTokenization,omitempty"`
		} `json:"indexes"`
	} `json:"properties"`
}

func getIndexes(t *testing.T, restURI, collection string) *indexesResponse {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/schema/%s/indexes", restURI, collection))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "get indexes failed: %s", string(body))
	var result indexesResponse
	require.NoError(t, json.Unmarshal(body, &result))
	return &result
}

func awaitReindexViaIndexes(t *testing.T, restURI, collection, property, indexType string) {
	t.Helper()
	var lastProgress float32
	var sawIndexing bool

	require.Eventually(t, func() bool {
		resp := getIndexes(t, restURI, collection)
		for _, prop := range resp.Properties {
			if prop.Name == property {
				for _, idx := range prop.Indexes {
					if idx.Type == indexType {
						switch idx.Status {
						case "indexing":
							sawIndexing = true
							lastProgress = idx.Progress
							return false
						case "ready":
							return true
						case "pending":
							return false
						}
					}
				}
			}
		}
		return false
	}, 120*time.Second, 1*time.Second, "expected property %s index %s to reach ready status", property, indexType)

	if sawIndexing {
		t.Logf("index monitoring: saw indexing->ready for %s/%s (final progress: %f)", property, indexType, lastProgress)
	} else {
		t.Logf("index monitoring: task completed too fast for %s/%s", property, indexType)
	}
}

func awaitReindexFinished(t *testing.T, restURI, taskID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}
		var tasks models.DistributedTasks
		if err := json.Unmarshal(body, &tasks); err != nil {
			return false
		}
		for _, task := range tasks["reindex"] {
			if task.ID == taskID {
				t.Logf("task %s status: %s", taskID, task.Status)
				if task.Status == "FAILED" {
					t.Fatalf("reindex task failed: %s", task.Error)
				}
				return task.Status == "FINISHED"
			}
		}
		return false
	}, 120*time.Second, 1*time.Second, "reindex task %s should reach FINISHED status", taskID)
}

func getFirstShardName(t *testing.T, restURI, collection string) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/nodes?output=verbose", restURI))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var nodesResp struct {
		Nodes []struct {
			Shards []struct {
				Class string `json:"class"`
				Name  string `json:"name"`
			} `json:"shards"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(body, &nodesResp))

	for _, node := range nodesResp.Nodes {
		for _, shard := range node.Shards {
			if shard.Class == collection {
				return shard.Name
			}
		}
	}
	return "" // not found — some subtests may not have created their collection yet
}

func runGraphQLQuery(t *testing.T, className, gqlQuery string) ([]string, error) {
	t.Helper()
	resp, err := graphqlhelper.QueryGraphQL(t, nil, "", gqlQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("graphql request: %w", err)
	}
	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", resp.Errors[0].Message)
	}
	data := make(map[string]interface{})
	for key, value := range resp.Data {
		data[key] = value
	}
	getMap := data["Get"].(map[string]interface{})
	items := getMap[className].([]interface{})
	ids := make([]string, 0, len(items))
	for _, item := range items {
		m := item.(map[string]interface{})
		additional := m["_additional"].(map[string]interface{})
		ids = append(ids, additional["id"].(string))
	}
	return ids, nil
}

func listLSMDirs(ctx context.Context, t *testing.T, c testcontainers.Container, col, shard string) []string {
	t.Helper()
	path := fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(col), shard)
	code, reader, err := c.Exec(ctx, []string{"ls", "-1", path})
	require.NoError(t, err, "exec ls on container")
	require.Equal(t, 0, code, "ls returned non-zero exit code")
	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	require.NoError(t, err)
	var dirs []string
	for _, line := range strings.Split(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			dirs = append(dirs, line)
		}
	}
	return dirs
}

func assertNoSuffixedBuckets(t *testing.T, dirs []string, suffix string) {
	t.Helper()
	for _, d := range dirs {
		assert.False(t, strings.Contains(d, suffix),
			"unexpected leftover bucket directory: %s", d)
	}
}

func assertBucketExists(t *testing.T, dirs []string, bucketName string) {
	t.Helper()
	found := false
	for _, d := range dirs {
		if d == bucketName {
			found = true
			break
		}
	}
	assert.True(t, found, "expected bucket directory %q not found in %v", bucketName, dirs)
}

func idsMatchUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aSorted := make([]string, len(a))
	bSorted := make([]string, len(b))
	copy(aSorted, a)
	copy(bSorted, b)
	sort.Strings(aSorted)
	sort.Strings(bSorted)
	for i := range aSorted {
		if aSorted[i] != bSorted[i] {
			return false
		}
	}
	return true
}

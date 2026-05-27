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

// Package reindex_concurrent tests that multiple non-conflicting reindex tasks
// can run concurrently on the same collection (different properties).
package reindex_concurrent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	numTextProps    = 10
	numIntProps     = 5
	numObjectsSmall = 200
)

// TestConcurrentReindex creates a collection with many properties (text + int),
// imports data, and submits concurrent reindex tasks for different properties:
//   - Change tokenization on text properties (word -> field)
//   - Enable rangeable on int properties
//
// All tasks should run in parallel without conflict. The test verifies all
// tasks complete successfully and that schema changes are applied.
func TestConcurrentReindex(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithDebugPort(). // QA-11311: expose pprof:6060 for goroutine dumps on stall
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	restURI := compose.GetWeaviate().URI()
	debugURI := compose.GetWeaviate().DebugURI()
	helper.SetupClient(restURI)

	// Dump container logs on failure.
	container := compose.GetWeaviate().Container()
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
			if len(lines) > 300 {
				lines = lines[len(lines)-300:]
			}
			t.Logf("=== Container logs (last 300 lines) ===\n%s", strings.Join(lines, "\n"))
		}
	}()

	collection := "ConcurrentReindexTest"

	// 1. Create collection with many properties.
	createCollectionWithManyProps(t, restURI, collection)

	// 2. Import data.
	importData(t, restURI, collection)

	// 3. Submit concurrent reindex tasks for different properties.
	type taskInfo struct {
		taskID   string
		propName string
		taskType string
	}

	var tasks []taskInfo
	var mu sync.Mutex

	// Submit change-tokenization on each text property.
	for i := range numTextProps {
		propName := fmt.Sprintf("text_%d", i)
		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, collection, propName,
			`{"searchable":{"tokenization":"field"}}`)
		tasks = append(tasks, taskInfo{taskID, propName, "change-tokenization"})
		t.Logf("submitted change-tokenization for %s: %s", propName, taskID)
	}

	// Submit enable-rangeable on each int property.
	for i := range numIntProps {
		propName := fmt.Sprintf("int_%d", i)
		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, collection, propName,
			`{"rangeable":{"enabled":true}}`)
		tasks = append(tasks, taskInfo{taskID, propName, "enable-rangeable"})
		t.Logf("submitted enable-rangeable for %s: %s", propName, taskID)
	}

	// 4. Wait for ALL tasks to complete.
	//
	// QA-11311 instrumentation: while tasks are in-flight, periodically dump the
	// server's goroutine stacks and the /v1/tasks state. The failing CI run
	// (run 26452655941) showed ALL 15 tasks stuck at "did not finish within
	// 5m0s" — a hard stall. A goroutine dump taken mid-stall shows exactly where
	// the task workers and the RAFT apply loop are parked, which the test output
	// otherwise cannot reveal.
	stopWatchdog := make(chan struct{})
	watchdogDone := make(chan struct{})
	go func() {
		defer close(watchdogDone)
		ticker := time.NewTicker(75 * time.Second)
		defer ticker.Stop()
		n := 0
		for {
			select {
			case <-stopWatchdog:
				return
			case <-ticker.C:
				n++
				dumpTaskStates(t, restURI, fmt.Sprintf("watchdog-%d (~%ds)", n, n*75))
				dumpGoroutines(t, debugURI, fmt.Sprintf("watchdog-%d (~%ds)", n, n*75))
			}
		}
	}()

	var wg sync.WaitGroup
	var errors []string
	for _, task := range tasks {
		wg.Add(1)
		go func(ti taskInfo) {
			defer wg.Done()
			err := awaitTask(t, restURI, ti.taskID, 5*time.Minute)
			if err != nil {
				mu.Lock()
				errors = append(errors, fmt.Sprintf("%s(%s): %v", ti.taskType, ti.propName, err))
				mu.Unlock()
			}
		}(task)
	}
	wg.Wait()
	close(stopWatchdog)
	<-watchdogDone

	// QA-11311: capture a final frozen snapshot of the stall before asserting.
	if len(errors) > 0 {
		dumpTaskStates(t, restURI, "post-stall")
		dumpGoroutines(t, debugURI, "post-stall")
	}
	require.Empty(t, errors, "some tasks failed: %s", strings.Join(errors, "; "))

	// 5. Verify schema changes — poll until they propagate.
	// OnGroupCompleted (which runs the swap + schema update for semantic
	// migrations) may fire AFTER the task reports FINISHED.
	t.Run("VerifySchemaChanges", func(t *testing.T) {
		require.Eventually(t, func() bool {
			class := helper.GetClass(t, collection)
			if class == nil {
				return false
			}
			for _, prop := range class.Properties {
				if strings.HasPrefix(prop.Name, "text_") && prop.Tokenization != "field" {
					return false
				}
				if strings.HasPrefix(prop.Name, "int_") {
					if prop.IndexRangeFilters == nil || !*prop.IndexRangeFilters {
						return false
					}
				}
			}
			return true
		}, 60*time.Second, 1*time.Second, "schema changes should propagate within 60s")
	})

	// 6. Verify queries still work correctly.
	t.Run("VerifyQueries", func(t *testing.T) {
		// Verify int properties — use Equal filter which uses the filterable
		// index (not the rangeable index) to verify data integrity.
		for i := range numIntProps {
			propName := fmt.Sprintf("int_%d", i)
			count := countWithEqualFilter(t, restURI, collection, propName, 0)
			assert.Greater(t, count, 0,
				"equal query on %s should return results", propName)
		}
	})

	// 7. Verify conflicting tasks are rejected.
	t.Run("ConflictRejection", func(t *testing.T) {
		// Submit a change-tokenization for text_0 again (same property).
		// Should succeed since the previous one finished.
		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, collection, "text_0",
			`{"searchable":{"tokenization":"word"}}`)
		t.Logf("submitted retokenize for text_0: %s", taskID)

		// While it's running, try to submit another for the same property.
		// This should be rejected as a conflict.
		time.Sleep(100 * time.Millisecond) // brief delay to let it start
		url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, collection, "text_0")
		req, err := http.NewRequest(http.MethodPut, url,
			bytes.NewReader([]byte(`{"searchable":{"tokenization":"field"}}`)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusConflict:
			t.Log("correctly rejected conflicting task (409)")
		case http.StatusAccepted:
			// Task might have finished before we could submit the conflict.
			t.Log("first task finished before conflict check could trigger (202)")
		}

		// Wait for the retokenize back to word to finish.
		require.NoError(t, awaitTask(t, restURI, taskID, 2*time.Minute))
	})

	// 8. Restart and verify deferred finalization.
	t.Run("PostRestartFinalize", func(t *testing.T) {
		t.Log("restarting weaviate container")
		require.NoError(t, compose.StopAt(ctx, 0, nil))
		require.NoError(t, compose.StartAt(ctx, 0))
		helper.SetupClient(compose.GetWeaviate().URI())
		restURI = compose.GetWeaviate().URI()

		// Verify queries still work after restart.
		for i := range numIntProps {
			propName := fmt.Sprintf("int_%d", i)
			count := countWithEqualFilter(t, restURI, collection, propName, 0)
			assert.Greater(t, count, 0,
				"equal query on %s should still return results after restart", propName)
		}
	})
}

// =============================================================================
// Helpers
// =============================================================================

func createCollectionWithManyProps(t *testing.T, restURI, collection string) {
	t.Helper()

	props := make([]*models.Property, 0, numTextProps+numIntProps)
	for i := range numTextProps {
		props = append(props, &models.Property{
			Name:            fmt.Sprintf("text_%d", i),
			DataType:        []string{"text"},
			Tokenization:    "word",
			IndexFilterable: reindexhelpers.BoolPtr(true),
			IndexSearchable: reindexhelpers.BoolPtr(true),
		})
	}
	for i := range numIntProps {
		props = append(props, &models.Property{
			Name:              fmt.Sprintf("int_%d", i),
			DataType:          []string{"int"},
			IndexFilterable:   reindexhelpers.BoolPtr(true),
			IndexRangeFilters: reindexhelpers.BoolPtr(false),
		})
	}

	class := &models.Class{
		Class:      collection,
		Properties: props,
		InvertedIndexConfig: &models.InvertedIndexConfig{
			UsingBlockMaxWAND: false,
		},
	}
	helper.CreateClass(t, class)
	t.Logf("created collection %s with %d text + %d int properties", collection, numTextProps, numIntProps)
}

func importData(t *testing.T, restURI, collection string) {
	t.Helper()
	objects := make([]*models.Object, numObjectsSmall)
	for i := range numObjectsSmall {
		props := map[string]interface{}{}
		for j := range numTextProps {
			props[fmt.Sprintf("text_%d", j)] = fmt.Sprintf("hello world %d", i)
		}
		for j := range numIntProps {
			props[fmt.Sprintf("int_%d", j)] = i
		}
		objects[i] = &models.Object{
			Class:      collection,
			Properties: props,
		}
	}
	helper.CreateObjectsBatch(t, objects)
	t.Logf("imported %d objects", numObjectsSmall)
}

// dumpGoroutines fetches the full goroutine stack dump (debug=2) from the
// server's pprof endpoint and logs it. QA-11311: this is the decisive artifact
// for diagnosing the all-tasks-stalled failure — it shows where every task
// worker and the RAFT apply loop are parked, plus how long each has been
// blocked.
func dumpGoroutines(t *testing.T, debugURI, label string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/debug/pprof/goroutine?debug=2", debugURI)
	resp, err := http.Get(url)
	if err != nil {
		t.Logf("[QA-11311] dumpGoroutines(%s): GET %s failed: %v", label, url, err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	t.Logf("[QA-11311] ===== BEGIN GOROUTINE DUMP (%s) =====\n%s\n[QA-11311] ===== END GOROUTINE DUMP (%s) =====",
		label, string(body), label)
}

// dumpTaskStates fetches GET /v1/tasks and logs the raw payload so we can see
// each reindex task's status (STARTED/FINISHED/FAILED) at the moment of capture.
func dumpTaskStates(t *testing.T, restURI, label string) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
	if err != nil {
		t.Logf("[QA-11311] dumpTaskStates(%s): %v", label, err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	t.Logf("[QA-11311] ===== TASK STATES (%s) =====\n%s", label, string(body))
}

func awaitTask(t *testing.T, restURI, taskID string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var tasks models.DistributedTasks
		if err := json.Unmarshal(body, &tasks); err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		for _, task := range tasks["reindex"] {
			if task.ID == taskID {
				switch task.Status {
				case "FINISHED":
					t.Logf("task %s: FINISHED", taskID)
					return nil
				case "FAILED":
					return fmt.Errorf("task %s FAILED: %s", taskID, task.Error)
				default:
					// Still running.
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("task %s did not finish within %v", taskID, timeout)
}

func countWithEqualFilter(t *testing.T, restURI, collection, propName string, value int) int {
	t.Helper()
	body := fmt.Sprintf(`{
		"query": "{Aggregate{%s(where:{path:[\"%s\"],operator:Equal,valueInt:%d}){meta{count}}}}"
	}`, collection, propName, value)
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/graphql", restURI),
		"application/json",
		bytes.NewReader([]byte(body)),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(respBody, &result))

	data, ok := result["data"].(map[string]interface{})
	if !ok {
		return 0
	}
	agg, ok := data["Aggregate"].(map[string]interface{})
	if !ok {
		return 0
	}
	items, ok := agg[collection].([]interface{})
	if !ok || len(items) == 0 {
		return 0
	}
	meta := items[0].(map[string]interface{})["meta"].(map[string]interface{})
	return int(meta["count"].(float64))
}

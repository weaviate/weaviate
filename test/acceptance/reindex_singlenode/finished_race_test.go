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

package reindex_singlenode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestSingleNode_FinishedStatusRaceWithSchemaFlag documents a race between the
// distributed-task FINISHED status and the schema-flag flip for a semantic
// migration (change-tokenization).
//
// Race window:
//
//  1. The last unit's `RecordDistributedTaskUnitCompletion` runs (Raft apply).
//     In cluster/distributedtask/manager.go:184, AllUnitsTerminal() → true and
//     task.Status flips to FINISHED. A poller against /v1/tasks now sees
//     FINISHED.
//  2. The scheduler tick (every DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS,
//     default 1s) later observes taskTerminal == true and fires
//     OnGroupCompleted (cluster/distributedtask/scheduler.go:318).
//  3. OnGroupCompleted (adapters/repos/db/reindex_provider.go:382) calls
//     RunSwapOnShard, which calls strategy.OnMigrationComplete (
//     adapters/repos/db/inverted_reindex_task_generic.go:876), which for
//     change-tokenization performs the schema update (
//     adapters/repos/db/inverted_reindex_strategy_filterable_retokenize.go:169).
//
// Between (1) and (3) the task is FINISHED but the schema still reports the
// old tokenization. A correct API contract would only flip FINISHED once the
// swap phase is done.
//
// This test reproduces the race by polling /v1/tasks and /v1/schema in a tight
// loop and asserts that the schema flag has flipped at the same moment FINISHED
// is first observed. It is expected to FAIL on the current implementation.
func TestSingleNode_FinishedStatusRaceWithSchemaFlag(t *testing.T) {
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

	const className = "FinishedRaceTest"

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "filepath", DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// A handful of small objects so the reindex is fast — we want the race
	// window to be narrow, the bug then is the FINISHED transition firing
	// before the swap.
	for i := 0; i < 5; i++ {
		obj := &models.Object{Class: className, Properties: map[string]interface{}{
			"filepath": fmt.Sprintf("/a/b/c/file_%d.go", i),
		}}
		require.NoError(t, helper.CreateObject(t, obj))
	}

	taskID := submitIndexUpdate(t, restURI, className, "filepath",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// Poll /v1/tasks every ~20ms. On the first observation of FINISHED,
	// immediately read the schema and assert the tokenization flipped.
	deadline := time.Now().Add(120 * time.Second)
	var sawFinishedAt time.Time
	for time.Now().Before(deadline) {
		status, err := fetchTaskStatus(restURI, taskID)
		require.NoError(t, err)
		if status == "FAILED" {
			t.Fatalf("task FAILED before reaching FINISHED")
		}
		if status == "FINISHED" {
			sawFinishedAt = time.Now()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.False(t, sawFinishedAt.IsZero(), "task never reached FINISHED")

	// At this exact moment, check the schema. If the swap-phase race exists,
	// the tokenization will still be "word" because OnGroupCompleted has not
	// yet run (it fires on the next scheduler tick, up to 1s later) or
	// RunSwapOnShard has not yet completed.
	cls := helper.GetClass(t, className)
	var tokenization string
	for _, prop := range cls.Properties {
		if prop.Name == "filepath" {
			tokenization = prop.Tokenization
		}
	}
	delay := time.Since(sawFinishedAt)
	t.Logf("schema check %v after FINISHED first observed: tokenization=%q", delay, tokenization)

	require.Equal(t, "field", tokenization,
		"FINISHED was observed but schema still reports the old tokenization — "+
			"schema flag flip lags task status by up to one scheduler tick + "+
			"swap-phase duration")
}

// fetchTaskStatus returns the status string for the given reindex task, or
// the empty string if not present.
func fetchTaskStatus(restURI, taskID string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var tasks models.DistributedTasks
	if err := json.Unmarshal(body, &tasks); err != nil {
		return "", err
	}
	for _, task := range tasks["reindex"] {
		if task.ID == taskID {
			return task.Status, nil
		}
	}
	return "", nil
}

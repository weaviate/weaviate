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
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// TestSingleNode_FinishedStatusRaceWithSchemaFlag pins the ordering between
// the distributed-task FINISHED status and the schema-flag flip for a semantic
// migration (change-tokenization) under the Journey 3 canonical wiring.
//
// The flip is not something FINISHED races: MarkTaskFinalized is only proposed
// after OnTaskCompleted has returned nil, and OnTaskCompleted's schema update
// is itself a RAFT apply. The flip therefore commits to the log strictly
// before FINISHED does, and on one node it is applied first.
//
// That ordering is what lets the index-status endpoint read both signals from
// the local FSM and treat FINISHED + flag-off as a stale task rather than a
// pending swap. This test pins it end to end: at the first observation of
// FINISHED the schema must ALREADY report the new tokenization.
//
// The window between the units stopping and the flip is reported by the
// PREPARING and SWAPPING statuses, not by FINISHED.
func TestSingleNode_FinishedStatusRaceWithSchemaFlag(t *testing.T) {
	ctx := context.Background()

	compose, err := reindexhelpers.StartSingleNode(ctx)
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

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "filepath",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// Poll /v1/tasks. On the first observation of FINISHED, immediately
	// capture the timestamp so the convergence-lag measurement below stays
	// accurate. Keep a tight 20ms tick — this test measures the sub-second
	// lag between FINISHED and the schema flip, so the cadence is
	// load-bearing.
	var sawFinishedAt time.Time
	require.Eventually(t, func() bool {
		status, err := fetchTaskStatus(restURI, taskID)
		require.NoError(t, err)
		if status == "FAILED" {
			t.Fatalf("task FAILED before reaching FINISHED")
		}
		if status == "FINISHED" {
			sawFinishedAt = time.Now()
			return true
		}
		return false
	}, 120*time.Second, 20*time.Millisecond)
	require.False(t, sawFinishedAt.IsZero(), "task never reached FINISHED")

	// No polling: the flip is ordered before FINISHED in the RAFT log, so it
	// is already applied by the time FINISHED is observable. A retry loop here
	// would hide exactly the regression this test exists to catch.
	var observedTokenization string
	for _, prop := range helper.GetClass(t, className).Properties {
		if prop.Name == "filepath" {
			observedTokenization = prop.Tokenization
		}
	}
	require.Equal(t, "field", observedTokenization,
		"the schema flip commits before FINISHED does, so a task reported FINISHED "+
			"must never render against the pre-migration schema. Either the Journey 3 "+
			"wiring (ReindexProvider.OnTaskCompleted -> applyPerPropertySchemaUpdate) "+
			"stopped preceding MarkTaskFinalized, or FINISHED is being reported early.")
	t.Logf("schema already at tokenization=%q when FINISHED first became visible (observed at %v)",
		observedTokenization, sawFinishedAt)
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

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

package reindex_concurrent

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestParallelCapBurst fires cap+8 enable-filterable submits for distinct
// properties of one collection as a truly parallel burst and pins the
// per-collection concurrency cap under the RAFT serialization point.
//
// Regression: the cap used to be enforced only as a handler-side pre-submit
// check (list tasks → count in-flight → submit). Every request in a
// simultaneous burst observed the task list before any of them had applied,
// so all of them passed the pre-check and all of them were admitted — a
// 40-parallel burst put 40 tasks in flight against a cap of 32. The fix
// re-enforces the cap inside the distributed-task FSM at apply time
// ([Manager.AddTask] under m.mu), so the cap holds no matter how parallel
// the submissions are.
//
// Pre-fix this test is RED: all cap+8 submits return 202. Post-fix exactly
// cap submits return 202 and the remaining 8 return 429 — rejected either
// by the handler pre-check (if it observed the filled task list) or by the
// apply-time backstop; both surface the same 429 contract.
func TestParallelCapBurst(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	collection := "ParallelCapBurst"
	// The cap is pinned to its externally observable value instead of
	// referencing distributedtask.MaxConcurrentActiveTasksPerCollection on
	// purpose: an acceptance test pins the user-facing contract, and
	// hardcoding keeps the test compilable against a pre-fix build so it
	// demonstrates the bug (every burst submit admitted) instead of
	// failing to compile.
	const (
		capLimit = 32
		overCap  = 8
	)
	numProps := capLimit + overCap

	helper.CreateClass(t, &models.Class{
		Class:      collection,
		Properties: reindexhelpers.CapBurstProps(numProps),
	})

	// A modest corpus so the admitted migrations outlive the submit-burst
	// window: the burst must be counted while all admitted tasks are still
	// in flight, otherwise a finished task frees a slot and more than cap
	// submits get admitted.
	const numObjects = 200
	objects := make([]*models.Object, numObjects)
	for i := range objects {
		objProps := map[string]interface{}{}
		for j := 0; j < numProps; j++ {
			objProps[fmt.Sprintf("prop_%02d", j)] = fmt.Sprintf("hello world %d", i)
		}
		objects[i] = &models.Object{
			Class:      collection,
			Properties: objProps,
		}
	}
	helper.CreateObjectsBatch(t, objects)

	// All submits leave as one truly parallel burst (start-barrier
	// orchestration and per-property result collection live in the shared
	// helper; no assertions inside the burst goroutines).
	client := &http.Client{Timeout: 60 * time.Second}
	results, err := reindexhelpers.FireIndexUpdateBurst(client, restURI, collection,
		`{"filterable":{"enabled":true}}`, numProps)
	require.NoError(t, err)

	accepted := reindexhelpers.AssertCapBurstOutcome(t, results, collection, capLimit, overCap)

	// Let every admitted task run to completion so container teardown happens
	// against a quiet node. Any FAILED/CANCELLED task aborts the poll early —
	// admitted tasks are expected to succeed.
	var terminalProblem string
	finishedSeen := 0
	require.Eventually(t, func() bool {
		tasks, ok := reindexhelpers.TryFetchTasks(restURI)
		if !ok {
			return false
		}
		finished := 0
		for _, task := range tasks["reindex"] {
			switch task.Status {
			case "STARTED", "PREPARING", "SWAPPING":
				return false
			case "FAILED", "CANCELLED":
				terminalProblem = fmt.Sprintf("task %s reached %s: %s", task.ID, task.Status, task.Error)
				return true
			case "FINISHED":
				finished++
			}
		}
		finishedSeen = finished
		return finished == accepted
	}, 5*time.Minute, time.Second, "all admitted reindex tasks should settle")
	require.Empty(t, terminalProblem)
	require.Equal(t, accepted, finishedSeen,
		"every admitted task must reach FINISHED")
}

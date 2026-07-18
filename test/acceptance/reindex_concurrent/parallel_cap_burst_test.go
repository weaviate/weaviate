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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

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

	// One text property per submit; IndexFilterable=false so
	// {"filterable":{"enabled":true}} passes validation for every property.
	props := make([]*models.Property, 0, numProps)
	for i := 0; i < numProps; i++ {
		props = append(props, &models.Property{
			Name:            fmt.Sprintf("prop_%02d", i),
			DataType:        []string{"text"},
			Tokenization:    "word",
			IndexFilterable: reindexhelpers.BoolPtr(false),
			IndexSearchable: reindexhelpers.BoolPtr(true),
		})
	}
	helper.CreateClass(t, &models.Class{
		Class:      collection,
		Properties: props,
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

	type submitResult struct {
		prop       string
		statusCode int
		body       string
		err        error
	}

	// Start barrier: every goroutine blocks on the gate until the main
	// goroutine closes it, so all requests leave as simultaneously as the
	// HTTP stack allows. Results are collected per index — no assertions
	// inside the goroutines (require.* must run on the test goroutine).
	gate := make(chan struct{})
	results := make([]submitResult, numProps)
	client := &http.Client{Timeout: 60 * time.Second}

	var g errgroup.Group
	for i := 0; i < numProps; i++ {
		g.Go(func() error {
			<-gate
			prop := fmt.Sprintf("prop_%02d", i)
			status, body, err := fireIndexUpdate(client, restURI, collection, prop,
				`{"filterable":{"enabled":true}}`)
			results[i] = submitResult{prop: prop, statusCode: status, body: body, err: err}
			return nil
		})
	}
	close(gate)
	require.NoError(t, g.Wait())

	var accepted, tooMany int
	for _, res := range results {
		require.NoError(t, res.err, "request for %s failed at transport level", res.prop)
		switch res.statusCode {
		case http.StatusAccepted:
			accepted++
		case http.StatusTooManyRequests:
			tooMany++
			var errResp models.ErrorResponse
			require.NoError(t, json.Unmarshal([]byte(res.body), &errResp),
				"429 body must decode to the standard ErrorResponse shape: %s", res.body)
			require.NotEmpty(t, errResp.Error)
			assert.Contains(t, errResp.Error[0].Message, collection,
				"429 message must name the capped collection")
		default:
			t.Fatalf("unexpected status %d for %s: %s", res.statusCode, res.prop, res.body)
		}
	}
	assert.Equal(t, capLimit, accepted,
		"exactly the cap may be admitted, no matter how parallel the burst")
	assert.Equal(t, overCap, tooMany,
		"the over-cap remainder must be rejected with 429")

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

// fireIndexUpdate fires PUT /v1/schema/{collection}/indexes/{property} and
// reports the raw outcome. It performs no assertions so it is safe to call
// from errgroup goroutines.
func fireIndexUpdate(client *http.Client, restURI, collection, property, jsonBody string) (int, string, error) {
	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, collection, property)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(jsonBody)))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(body), nil
}

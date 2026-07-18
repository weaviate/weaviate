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

package reindex_multinode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
)

// TestMultiNode_ParallelCapBurst_FollowerForwarded429 is the cross-node
// variant of the single-node TestParallelCapBurst (package
// reindex_concurrent): it fires the same cap+8 enable-filterable burst for
// distinct properties of one collection, but aims every request at a
// NON-LEADER node. The follower then forwards each submit to the RAFT
// leader's distributed-task FSM over gRPC, and the apply-time cap rejection
// ([distributedtask.Manager.AddTask]) travels back as
// (FailedPrecondition, "[dtm-perm/task-cap-exceeded] ..."). The submitting
// node must re-hydrate that wire error into the ErrTaskCapExceeded sentinel
// so the handler can map it to 429 — if the marker is lost on the
// round-trip, the client sees a generic 500 instead. This test pins the
// follower-forwarded contract: exactly cap×202 + over×429 and zero 5xx.
//
// Regression: pre-fix there is no apply-time cap, so all cap+8 submits are
// admitted (RED: 40×202, 0×429). The single-node variant never exercises the
// gRPC re-hydrate path at all; QA Claude verified this journey live on a
// 3-node cluster while reviewing weaviate/weaviate#12263, and this test is
// the permanent CI guard for it.
//
// The cap is pinned to its externally observable value instead of
// referencing distributedtask.MaxConcurrentActiveTasksPerCollection on
// purpose: an acceptance test pins the user-facing contract.
func TestMultiNode_ParallelCapBurst_FollowerForwarded429(t *testing.T) {
	ctx := context.Background()

	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	collection := "ParallelCapBurstFollower"
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
	createCollection(t, compose, restURIOf(compose, 1), collection, 3, 3, props)

	// A modest corpus so the admitted migrations outlive the submit-burst
	// window: the burst must be counted while all admitted tasks are still
	// in flight, otherwise a finished task frees a slot and more than cap
	// submits get admitted.
	const numObjects = 200
	batchImportMultiProp(t, restURIOf(compose, 1), collection, numObjects,
		func(i int) map[string]interface{} {
			objProps := map[string]interface{}{}
			for j := 0; j < numProps; j++ {
				objProps[fmt.Sprintf("prop_%02d", j)] = fmt.Sprintf("hello world %d", i)
			}
			return objProps
		})

	// Aim the burst at a NON-LEADER node: resolve the RAFT leader, pick the
	// next node, then re-resolve right before firing and re-pick if
	// leadership moved in between — a burst that accidentally lands on the
	// leader would silently skip the gRPC re-hydrate path under test.
	leaderIdx := raftLeaderIndex(t, compose)
	targetIdx := (leaderIdx + 1) % 3
	leaderIdx = raftLeaderIndex(t, compose)
	if targetIdx == leaderIdx {
		targetIdx = (leaderIdx + 1) % 3
	}
	require.NotEqual(t, leaderIdx, targetIdx,
		"burst target weaviate-%d must be a non-leader (leader is weaviate-%d)", targetIdx, leaderIdx)
	targetURI := compose.GetWeaviateNode(targetIdx + 1).URI()
	t.Logf("RAFT leader is weaviate-%d; firing burst at non-leader weaviate-%d (%s)",
		leaderIdx, targetIdx, targetURI)

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
			status, body, err := fireIndexUpdateFollower(client, targetURI, collection, prop,
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
		case http.StatusInternalServerError:
			t.Fatalf("500 for %s — the [dtm-perm/task-cap-exceeded] marker was lost on the "+
				"follower→leader gRPC round-trip, so a cap rejection surfaced as a generic "+
				"server error instead of 429: %s", res.prop, res.body)
		default:
			t.Fatalf("unexpected status %d for %s: %s", res.statusCode, res.prop, res.body)
		}
	}
	assert.Equal(t, capLimit, accepted,
		"exactly the cap may be admitted, no matter how parallel the burst")
	assert.Equal(t, overCap, tooMany,
		"the over-cap remainder must be rejected with 429, re-hydrated from the gRPC wire error")

	// Cluster-side state: the 8 rejected submits must never have become
	// tasks — exactly `accepted` reindex tasks exist for the collection.
	// Queried on node 1 (any node works; /v1/tasks reflects the RAFT FSM).
	// Then let every admitted task run to completion so container teardown
	// happens against a quiet cluster. Any FAILED/CANCELLED task — or a
	// task count above the admitted number at any sample — aborts the poll
	// early.
	var terminalProblem string
	finishedSeen, taskCountSeen := 0, 0
	require.Eventually(t, func() bool {
		tasks, ok := reindexhelpers.TryFetchTasks(restURIOf(compose, 1))
		if !ok {
			return false
		}
		finished := 0
		count := 0
		for _, task := range tasks["reindex"] {
			if !strings.EqualFold(reindexTaskCollection(task.Payload), collection) {
				continue
			}
			count++
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
		taskCountSeen = count
		if count > accepted {
			terminalProblem = fmt.Sprintf(
				"found %d reindex tasks for collection %s, but only %d submits were admitted — "+
					"an over-cap submit became a task", count, collection, accepted)
			return true
		}
		finishedSeen = finished
		return count == accepted && finished == accepted
	}, 5*time.Minute, time.Second, "all admitted reindex tasks should settle")
	require.Empty(t, terminalProblem)
	require.Equal(t, accepted, taskCountSeen,
		"the rejected over-cap submits must never have become tasks")
	require.Equal(t, accepted, finishedSeen,
		"every admitted task must reach FINISHED")
}

// reindexTaskCollection extracts the collection name from a reindex task's
// payload as served by GET /v1/tasks. Round-tripping through JSON keeps this
// tolerant of the payload's decoded shape (interface{} on
// models.DistributedTask); an unreadable payload yields "" and simply never
// matches the collection filter.
func reindexTaskCollection(payload interface{}) string {
	raw, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	var p struct {
		Collection string `json:"collection"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return ""
	}
	return p.Collection
}

// fireIndexUpdateFollower fires PUT /v1/schema/{collection}/indexes/{property}
// and reports the raw outcome. It performs no assertions so it is safe to
// call from errgroup goroutines.
func fireIndexUpdateFollower(client *http.Client, restURI, collection, property, jsonBody string) (int, string, error) {
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

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

// Package reindexhelpers provides HTTP-level helpers shared across
// the runtime-reindex acceptance test packages.
package reindexhelpers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// Option configures optional behavior of a helper call.
type Option func(*options)

type options struct {
	tenants []string
	timeout time.Duration
}

func applyOptions(opts []Option) options {
	o := options{timeout: 120 * time.Second}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// WithTenants appends `?tenants=t1,t2,…` to the URL. Empty/nil is a no-op.
func WithTenants(tenants []string) Option {
	return func(o *options) { o.tenants = tenants }
}

// WithTimeout overrides the default 120s require.Eventually timeout.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// SubmitIndexUpdate fires PUT /v1/schema/{collection}/indexes/{property}
// with the supplied JSON body, asserts 202, and returns the taskId.
func SubmitIndexUpdate(t *testing.T, restURI, collection, property, jsonBody string, opts ...Option) string {
	t.Helper()
	o := applyOptions(opts)

	url := indexUpdateURL(restURI, collection, property, o.tenants)
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

// IndexUpdateErrorResponse captures the status and body of a failing
// PUT /indexes/{prop} request.
type IndexUpdateErrorResponse struct {
	StatusCode int
	Body       string
}

// SubmitIndexUpdateExpect4xx submits a PUT /indexes request expected to
// fail at validation and returns the response status and body. The caller
// asserts the exact status code.
func SubmitIndexUpdateExpect4xx(t *testing.T, restURI, collection, property, jsonBody string, opts ...Option) IndexUpdateErrorResponse {
	t.Helper()
	o := applyOptions(opts)

	url := indexUpdateURL(restURI, collection, property, o.tenants)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(jsonBody)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	t.Logf("index update response (status=%d): %s", resp.StatusCode, string(body))
	return IndexUpdateErrorResponse{StatusCode: resp.StatusCode, Body: string(body)}
}

func indexUpdateURL(restURI, collection, property string, tenants []string) string {
	u := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, collection, property)
	if len(tenants) > 0 {
		u += "?tenants=" + strings.Join(tenants, ",")
	}
	return u
}

// IndexesResponse mirrors the GET /v1/schema/{class}/indexes response.
// Algorithm and TargetAlgorithm are populated only for BM25 (Map↔Blockmax).
type IndexesResponse struct {
	Collection string `json:"collection"`
	Properties []struct {
		Name    string `json:"name"`
		Indexes []struct {
			Type               string  `json:"type"`
			Status             string  `json:"status"`
			Progress           float32 `json:"progress"`
			Tokenization       string  `json:"tokenization,omitempty"`
			TargetTokenization string  `json:"targetTokenization,omitempty"`
			Algorithm          string  `json:"algorithm,omitempty"`
			TargetAlgorithm    string  `json:"targetAlgorithm,omitempty"`
		} `json:"indexes"`
	} `json:"properties"`
}

// GetIndexes fires GET /v1/schema/{collection}/indexes and returns the
// parsed response. Asserts 200.
func GetIndexes(t *testing.T, restURI, collection string) *IndexesResponse {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/schema/%s/indexes", restURI, collection))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "get indexes failed: %s", string(body))
	var result IndexesResponse
	require.NoError(t, json.Unmarshal(body, &result))
	return &result
}

// TryFetchTasks does one tolerant GET /v1/tasks: ok=false on any transport /
// non-200 / decode error so Eventually polls retry post-restart transients
// (gRPC reconnect). Deliberately takes no *testing.T — require inside an
// Eventually condition goroutine would runtime.Goexit the wrong goroutine.
func TryFetchTasks(restURI string) (models.DistributedTasks, bool) {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var tasks models.DistributedTasks
	if json.Unmarshal(body, &tasks) != nil {
		return nil, false
	}
	return tasks, true
}

// TryGetIndexes is the GET /v1/schema/{collection}/indexes counterpart of
// TryFetchTasks: tolerant and goroutine-safe, for Eventually polls that must
// ride out the same post-restart reconnect window.
func TryGetIndexes(restURI, collection string) (*IndexesResponse, bool) {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/schema/%s/indexes", restURI, collection))
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var out IndexesResponse
	if json.Unmarshal(body, &out) != nil {
		return nil, false
	}
	return &out, true
}

// fetchReindexTask does one tolerant GET /v1/tasks poll and returns the
// reindex task with the given ID. ok is false when the request, status,
// decode, or lookup hasn't yielded that task yet, so pollers keep retrying.
func fetchReindexTask(restURI, taskID string) (models.DistributedTask, bool) {
	tasks, ok := TryFetchTasks(restURI)
	if !ok {
		return models.DistributedTask{}, false
	}
	for _, task := range tasks["reindex"] {
		if task.ID == taskID {
			return task, true
		}
	}
	return models.DistributedTask{}, false
}

// AwaitReindexLive blocks until the task reaches a live status.
//
// Sync here, not on GET /v1/schema/<class>/indexes: the backup gate reads
// DTM liveness, and the two can lag ~1s, so index-status races the gate.
//
// A terminal status is captured inside the Eventually condition (which runs
// on its own goroutine, so it must never Fatalf) and reported from the test
// goroutine after Eventually returns.
func AwaitReindexLive(t *testing.T, restURI, taskID string, opts ...Option) {
	t.Helper()
	o := applyOptions(opts)

	// atomic.Pointer, not a plain var: correct today because
	// require.Eventually's cross-goroutine send/receive orders the write
	// before the read, but a future switch to assert.Eventually (returns on
	// timeout) would turn a plain capture into a live race.
	var terminalErr atomic.Pointer[error]
	require.Eventually(t, func() bool {
		task, ok := fetchReindexTask(restURI, taskID)
		if !ok {
			return false
		}
		t.Logf("task %s status: %s", taskID, task.Status)
		switch task.Status {
		case "FAILED", "CANCELLED":
			err := fmt.Errorf("reindex task %s reached terminal status %s before going live: %s",
				taskID, task.Status, task.Error)
			terminalErr.Store(&err)
			return true // exit Eventually; Fatalf below on the test goroutine
		case "FINISHED":
			// Drained before a live status was observed (fixture too small).
			err := fmt.Errorf("reindex task %s reached FINISHED before a live status was observed; "+
				"increase the import corpus so the migration outlives the gate-arming poll",
				taskID)
			terminalErr.Store(&err)
			return true
		case "STARTED", "PREPARING", "SWAPPING":
			return true
		}
		return false
	}, o.timeout, 200*time.Millisecond,
		"reindex task %s should reach a live (STARTED/PREPARING/SWAPPING) status", taskID)
	if errp := terminalErr.Load(); errp != nil {
		t.Fatal(*errp)
	}
}

// AwaitReindexFinished polls /v1/tasks until the named reindex task
// reaches FINISHED. Fails on FAILED or on timeout (default 120s). FAILED is
// captured inside the Eventually condition and reported from the test
// goroutine after — the condition goroutine must never Fatalf.
func AwaitReindexFinished(t *testing.T, restURI, taskID string, opts ...Option) {
	t.Helper()
	o := applyOptions(opts)

	// atomic.Pointer for the same future-proofing as AwaitReindexLive.
	var terminalErr atomic.Pointer[error]
	require.Eventually(t, func() bool {
		task, ok := fetchReindexTask(restURI, taskID)
		if !ok {
			return false
		}
		t.Logf("task %s status: %s", taskID, task.Status)
		if task.Status == "FAILED" {
			err := fmt.Errorf("reindex task failed: %s", task.Error)
			terminalErr.Store(&err)
			return true // exit Eventually; Fatalf below on the test goroutine
		}
		return task.Status == "FINISHED"
	}, o.timeout, 1*time.Second, "reindex task %s should reach FINISHED status", taskID)
	if errp := terminalErr.Load(); errp != nil {
		t.Fatal(*errp)
	}
}

// FetchClass: local=true sends consistency:false for the node's own FSM
// state; the default GET proxies to the leader, not a per-node visibility gate.
func FetchClass(restURI, className string, local bool) (*models.Class, bool) {
	req, err := http.NewRequest(http.MethodGet,
		fmt.Sprintf("http://%s/v1/schema/%s", restURI, className), nil)
	if err != nil {
		return nil, false
	}
	if local {
		req.Header.Set("consistency", "false")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var class models.Class
	if err := json.Unmarshal(body, &class); err != nil {
		return nil, false
	}
	return &class, true
}

func fetchLocalTokenization(restURI, className, propName string) (string, bool) {
	class, ok := FetchClass(restURI, className, true)
	if !ok {
		return "", false
	}
	for _, prop := range class.Properties {
		if prop.Name == propName {
			return prop.Tokenization, true
		}
	}
	return "", false
}

// AwaitTokenizationVisible gates on the node's local schema: /v1/tasks
// FINISHED is leader-forwarded, but the indexes PUT validates locally.
func AwaitTokenizationVisible(t *testing.T, restURI, className, propName, wantTok string, opts ...Option) {
	t.Helper()
	o := applyOptions(opts)

	lastSeen := "(no successful read)"
	deadline := time.Now().Add(o.timeout)
	for time.Now().Before(deadline) {
		if tok, ok := fetchLocalTokenization(restURI, className, propName); ok {
			lastSeen = tok
			if tok == wantTok {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("property %s.%s tokenization not %q in local schema of %s within %s (last seen: %q)",
		className, propName, wantTok, restURI, o.timeout, lastSeen)
}

// AwaitReindexViaIndexes polls GET /v1/schema/{collection}/indexes until
// the named (property, indexType) reports `ready`. Unlike
// AwaitReindexFinished, this polls the index-status surface — useful for
// verifying the index is queryable end-to-end. Default timeout 120s.
func AwaitReindexViaIndexes(t *testing.T, restURI, collection, property, indexType string, opts ...Option) {
	t.Helper()
	o := applyOptions(opts)

	// Atomics for the same future-proofing as AwaitReindexLive.
	var lastProgress atomic.Value // float32
	var sawIndexing atomic.Bool

	require.Eventually(t, func() bool {
		resp, ok := TryGetIndexes(restURI, collection)
		if !ok {
			return false // transient read (e.g. post-restart gRPC reconnect) — retry
		}
		for _, prop := range resp.Properties {
			if prop.Name == property {
				for _, idx := range prop.Indexes {
					if idx.Type == indexType {
						switch idx.Status {
						case "indexing":
							sawIndexing.Store(true)
							lastProgress.Store(idx.Progress)
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
	}, o.timeout, 1*time.Second, "expected property %s index %s to reach ready status", property, indexType)

	if sawIndexing.Load() {
		t.Logf("index monitoring: saw indexing->ready for %s/%s (final progress: %f)", property, indexType, lastProgress.Load())
	} else {
		t.Logf("index monitoring: task completed too fast for %s/%s", property, indexType)
	}
}

// GetFirstShardName resolves the first shard name for `collection` via
// GET /v1/nodes?output=verbose. Returns "" if the collection has no
// shards on any node.
func GetFirstShardName(t *testing.T, restURI, collection string) string {
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
	return ""
}

// BoolPtr returns a pointer to its bool argument.
func BoolPtr(b bool) *bool { return &b }

// IdsMatchUnordered reports whether two ID slices contain the same
// elements regardless of order.
func IdsMatchUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, v := range a {
		seen[v]++
	}
	for _, v := range b {
		if seen[v] == 0 {
			return false
		}
		seen[v]--
	}
	return true
}

// SetupClass creates a single-tenant class with the named properties and
// Vectorizer "none", and registers a t.Cleanup that deletes the class.
// Requires helper.SetupClient to have been called at the suite level.
func SetupClass(t *testing.T, class string, props []*models.Property) {
	t.Helper()
	helper.CreateClass(t, &models.Class{
		Class:      class,
		Properties: props,
		Vectorizer: "none",
	})
	t.Cleanup(func() { helper.DeleteClass(t, class) })
}

// SetupClassWithConfig is the SetupClass variant that accepts arbitrary
// class-level config.
func SetupClassWithConfig(t *testing.T, c *models.Class) {
	t.Helper()
	require.NotEmpty(t, c.Class, "Class name must be set on the models.Class")
	if c.Vectorizer == "" {
		c.Vectorizer = "none"
	}
	helper.CreateClass(t, c)
	t.Cleanup(func() { helper.DeleteClass(t, c.Class) })
}

// ImportObjects creates one object per entry in `objects` against the
// given class. Each entry is the Properties map. For large object counts
// callers should use helper.CreateObjectsBatch directly.
func ImportObjects(t *testing.T, class string, objects []map[string]interface{}) {
	t.Helper()
	for i, props := range objects {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      class,
			Properties: props,
		}), "create object %d", i)
	}
}

// WithEnv is the closure-style fixture for the "create class, import N
// objects, run body, delete class" pattern.
func WithEnv(
	t *testing.T,
	class string,
	props []*models.Property,
	objects []map[string]interface{},
	body func(),
) {
	t.Helper()
	SetupClass(t, class, props)
	ImportObjects(t, class, objects)
	body()
}

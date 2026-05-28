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
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// Retry budget for the WithRetryOnReadOnly mitigation. See AwaitReindexFinished.
const (
	readOnlyRetryInterval = 50 * time.Millisecond
	readOnlyRetryTimeout  = 2 * time.Second
	readOnlyMaxRetries    = 3
	readOnlyErrorMarker   = "store is read-only"
)

// Option configures optional behavior of a helper call.
type Option func(*options)

type options struct {
	tenants            []string
	timeout            time.Duration
	resubmitOnReadOnly func() string
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

// WithRetryOnReadOnly enables retry-on-read-only for AwaitReindexFinished. When
// the task transitions to FAILED with an error containing "store is read-only"
// (the known legacy PQ-compression schema-change code path that briefly marks a
// collection read-only on any schema change), AwaitReindexFinished polls for
// readOnlyRetryTimeout at readOnlyRetryInterval (with jitter) and calls
// `resubmit` to obtain a fresh taskID, retrying up to readOnlyMaxRetries times
// before failing.
//
// The `resubmit` closure must reproduce the SAME schema-change body as the
// original submit — typically:
//
//	submit := func() string {
//	    return reindexhelpers.SubmitIndexUpdate(t, restURI, class, prop, body)
//	}
//	taskID := submit()
//	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithRetryOnReadOnly(submit))
//
// Other FAILED reasons (anything not containing the marker substring) take
// the original fatal path.
//
// The 50ms/2s/3-retry budget composes with the default 120s AwaitReindexFinished
// timeout — pass WithTimeout(...) to shrink the per-attempt budget if needed.
//
// This mitigation will be removed once the server-side fix lands post-v1.38.
func WithRetryOnReadOnly(resubmit func() string) Option {
	return func(o *options) { o.resubmitOnReadOnly = resubmit }
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

// AwaitReindexFinished polls /v1/tasks until the named reindex task
// reaches FINISHED. Fails on FAILED or on timeout (default 120s).
//
// If WithRetryOnReadOnly is set and the task FAILS with an error containing
// "store is read-only" (the legacy PQ-compression schema-change race), the
// helper polls briefly for the read-only window to clear, then resubmits via
// the supplied closure (up to readOnlyMaxRetries times) before failing.
func AwaitReindexFinished(t *testing.T, restURI, taskID string, opts ...Option) {
	t.Helper()
	o := applyOptions(opts)

	deadline := time.Now().Add(o.timeout)
	retries := 0

	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		var tasks models.DistributedTasks
		if err := json.Unmarshal(body, &tasks); err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		for _, task := range tasks["reindex"] {
			if task.ID != taskID {
				continue
			}
			t.Logf("task %s status: %s", taskID, task.Status)
			if task.Status == "FINISHED" {
				return
			}
			if task.Status == "FAILED" {
				// Retry path: legacy PQ-compression schema-change race. The
				// collection is briefly read-only on any schema change; the
				// reindex writer collides and surfaces this error. We poll
				// briefly for the window to clear and resubmit.
				if o.resubmitOnReadOnly != nil && strings.Contains(task.Error, readOnlyErrorMarker) {
					if retries >= readOnlyMaxRetries {
						t.Fatalf("reindex task failed with read-only error after %d retries: %s", retries, task.Error)
					}
					retries++
					t.Logf("reindex task %s hit read-only race (attempt %d/%d); waiting for window to clear before resubmitting: %s",
						taskID, retries, readOnlyMaxRetries, task.Error)
					waitForReadOnlyWindow(readOnlyRetryTimeout, readOnlyRetryInterval)
					newTaskID := o.resubmitOnReadOnly()
					t.Logf("resubmitted reindex after read-only race; new task %s (was %s)", newTaskID, taskID)
					taskID = newTaskID
					// Extend the deadline so a late retry still has the full
					// per-attempt budget — the contract is documented on
					// WithRetryOnReadOnly.
					deadline = time.Now().Add(o.timeout)
					break
				}
				t.Fatalf("reindex task failed: %s", task.Error)
			}
			break
		}

		time.Sleep(1 * time.Second)
	}

	t.Fatalf("reindex task %s should reach FINISHED status within %s", taskID, o.timeout)
}

// waitForReadOnlyWindow sleeps for `total` in increments of `step` with 50%-150%
// jitter, to desynchronize concurrent retriers competing on the same window.
func waitForReadOnlyWindow(total, step time.Duration) {
	end := time.Now().Add(total)
	for time.Now().Before(end) {
		// Jitter the step between 50% and 150% of the nominal interval.
		factor := 0.5 + rand.Float64()
		sleep := time.Duration(float64(step) * factor)
		if remaining := time.Until(end); sleep > remaining {
			sleep = remaining
		}
		if sleep <= 0 {
			return
		}
		time.Sleep(sleep)
	}
}

// AwaitReindexViaIndexes polls GET /v1/schema/{collection}/indexes until
// the named (property, indexType) reports `ready`. Unlike
// AwaitReindexFinished, this polls the index-status surface — useful for
// verifying the index is queryable end-to-end. Default timeout 120s.
func AwaitReindexViaIndexes(t *testing.T, restURI, collection, property, indexType string, opts ...Option) {
	t.Helper()
	o := applyOptions(opts)

	var lastProgress float32
	var sawIndexing bool

	require.Eventually(t, func() bool {
		resp := GetIndexes(t, restURI, collection)
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
	}, o.timeout, 1*time.Second, "expected property %s index %s to reach ready status", property, indexType)

	if sawIndexing {
		t.Logf("index monitoring: saw indexing->ready for %s/%s (final progress: %f)", property, indexType, lastProgress)
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

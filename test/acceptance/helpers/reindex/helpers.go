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

// Package reindexhelpers provides the HTTP-level helpers shared across
// the runtime-reindex acceptance test packages (reindex_singlenode,
// reindex_multinode, reindex_concurrent, reindex_mt). Each helper
// previously existed in 2-3 near-identical copies inside individual
// test packages; consolidating them here ensures the four suites
// exercise the API the same way.
//
// All helpers take *testing.T as the first argument and call
// t.Helper() so failures report at the test callsite. None of them
// open long-lived resources — every HTTP response is closed before
// return.
//
// Variants captured via functional options:
//
//   - [WithTenants] adds `?tenants=t1,t2` to the URL on
//     [SubmitIndexUpdate] / [SubmitIndexUpdateExpect4xx] for the
//     multi-tenant suite.
//   - [WithTimeout] overrides the default [require.Eventually]
//     timeout (120s) for [AwaitReindexFinished] /
//     [AwaitReindexViaIndexes]. The multinode suite uses 180s to
//     absorb the slower 3-node startup.
package reindexhelpers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// -- Functional options --------------------------------------------------------

// Option configures the optional behaviour of a helper call. Use
// [WithTenants] and [WithTimeout]; the zero-options call gives the
// historical single-node / non-tenanted defaults.
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

// WithTenants appends `?tenants=t1,t2,…` to the URL on
// [SubmitIndexUpdate] and [SubmitIndexUpdateExpect4xx]. Empty / nil
// slice is a no-op.
func WithTenants(tenants []string) Option {
	return func(o *options) { o.tenants = tenants }
}

// WithTimeout overrides the default 120s [require.Eventually] timeout
// on [AwaitReindexFinished] and [AwaitReindexViaIndexes]. The
// multinode suite passes 180s for the slower 3-node startup.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// -- Submit ----------------------------------------------------------------

// SubmitIndexUpdate fires `PUT /v1/schema/{collection}/indexes/{property}`
// with the supplied JSON body and asserts the response is 202 Accepted.
// Returns the `taskId` field from the response body so the caller can
// poll the resulting reindex task.
//
// Use [WithTenants] to add a `?tenants=` query parameter for the
// multi-tenant suite. Without that option the URL is the same as
// every single-node caller's.
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

// IndexUpdateErrorResponse captures the status + body of an
// expected-to-fail PUT /indexes/{prop} request so the caller can
// assert both the status code AND that the error message names the
// right next step.
type IndexUpdateErrorResponse struct {
	StatusCode int
	Body       string
}

// SubmitIndexUpdateExpect4xx submits a PUT /indexes request that the
// test expects to fail at validation and returns the response status
// + body for assertion. Does NOT itself require 4xx (so the caller
// can pin the exact code).
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

// -- Indexes view ----------------------------------------------------------

// IndexesResponse mirrors the `/v1/schema/{class}/indexes` GET response
// shape. The Algorithm + TargetAlgorithm fields are populated for
// BM25 (Map↔Blockmax) and otherwise empty.
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

// GetIndexes fires `GET /v1/schema/{collection}/indexes` and asserts
// 200 OK. Returns the parsed response. Used by callers that need to
// inspect the per-index status / progress directly (instead of polling
// via /v1/tasks).
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

// -- Awaiters --------------------------------------------------------------

// AwaitReindexFinished polls `/v1/tasks` until the named reindex task
// reaches `FINISHED`. Fails the test if the task transitions to
// `FAILED` or doesn't reach `FINISHED` within the timeout.
//
// Default timeout is 120s; use [WithTimeout] to override (the
// multinode suite passes 180s to absorb the slower 3-node startup).
func AwaitReindexFinished(t *testing.T, restURI, taskID string, opts ...Option) {
	t.Helper()
	o := applyOptions(opts)

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
	}, o.timeout, 1*time.Second, "reindex task %s should reach FINISHED status", taskID)
}

// AwaitReindexViaIndexes polls `GET /v1/schema/{collection}/indexes`
// until the named (property, indexType) reports `ready` status.
// Distinguished from [AwaitReindexFinished]: the latter polls the
// task-orchestration surface, this one polls the index-status surface
// — useful for verifying the index is queryable end-to-end.
//
// Default timeout is 120s; use [WithTimeout] to override.
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

// -- Misc ------------------------------------------------------------------

// BoolPtr returns a pointer to its bool argument. Convenience for
// populating optional pointer-valued fields on schema requests.
func BoolPtr(b bool) *bool { return &b }

// IdsMatchUnordered reports whether two ID slices contain the same
// elements regardless of order. Used by tests that compare result-set
// IDs against an expected list when the storage/query path doesn't
// guarantee ordering.
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

// -- Test environment fixture -------------------------------------------------

// SetupClass creates a single-tenant class with the named properties and
// `Vectorizer: "none"` (the runtime-reindex tests all use BM25/filter
// paths, never a vectorizer), then registers a t.Cleanup that deletes
// the class at test end. Returns nothing — the class name passed in is
// the handle.
//
// Used by [WithEnv] but also useful standalone for tests that want to
// drive their own object creation (e.g. batch import, special data
// shapes) but still want the class lifecycle managed.
//
// Requires the test process to have already called
// [helper.SetupClient(restURI)] at the suite level — class creation
// goes through the package-level helper client, not a per-call HTTP
// hit.
func SetupClass(t *testing.T, class string, props []*models.Property) {
	t.Helper()
	helper.CreateClass(t, &models.Class{
		Class:      class,
		Properties: props,
		Vectorizer: "none",
	})
	t.Cleanup(func() { helper.DeleteClass(t, class) })
}

// SetupClassWithConfig is the [SetupClass] variant that lets the caller
// pass arbitrary class-level config (InvertedIndexConfig,
// MultiTenancyConfig, ReplicationConfig, etc.). The caller-supplied
// `class` value MUST match `c.Class`; this is enforced via require so
// a typo fails loudly.
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
// given class via [helper.CreateObject]. Each entry is the Properties
// map for that object — IDs and vectors are not set (the suites that
// need explicit IDs/vectors use the lower-level helper.CreateObject
// directly).
//
// Failures fail the test at the create-object call site (via
// require.NoError); the object index is included in the failure
// message so the offending row is identifiable.
//
// For large object counts use [helper.CreateObjectsBatch] directly;
// this loop is intentionally simple and not batched. The suites that
// need batch import (e.g. multinode round-trip) call the batch helper
// themselves.
func ImportObjects(t *testing.T, class string, objects []map[string]interface{}) {
	t.Helper()
	for i, props := range objects {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      class,
			Properties: props,
		}), "create object %d", i)
	}
}

// WithEnv is the closure-style fixture for the common "create class,
// import N objects, run test, delete class" pattern. Replaces ~12-15
// lines of boilerplate per test with one call.
//
// The fixture:
//
//  1. Calls [SetupClass] (creates the class + registers cleanup).
//  2. Calls [ImportObjects] (one-by-one create, no batching).
//  3. Invokes `body` — the per-test logic, which typically submits
//     a reindex via [SubmitIndexUpdate] then awaits via
//     [AwaitReindexFinished].
//
// Tests that need MT / custom InvertedIndexConfig / batch import / explicit
// IDs should use [SetupClass] / [SetupClassWithConfig] / [ImportObjects]
// directly — WithEnv targets the 80% case where the boilerplate is
// uniform.
//
// Example:
//
//	reindexhelpers.WithEnv(t, "MyClass",
//	    []*models.Property{
//	        {Name: "text", DataType: []string{"text"}, Tokenization: "word"},
//	    },
//	    []map[string]interface{}{
//	        {"text": "hello world"},
//	        {"text": "foo bar"},
//	    },
//	    func() {
//	        taskID := reindexhelpers.SubmitIndexUpdate(t, restURI,
//	            "MyClass", "text", `{"searchable":{"tokenization":"field"}}`)
//	        reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
//	        // assertions...
//	    })
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

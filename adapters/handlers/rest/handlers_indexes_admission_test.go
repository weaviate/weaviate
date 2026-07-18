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

package rest

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// admissionHandler builds an indexesHandlers with just enough state for the
// checkReindexAdmission gate (which only touches appState.Logger).
func admissionHandler() *indexesHandlers {
	return &indexesHandlers{appState: &state.State{Logger: logrus.New()}}
}

// statusOf renders a middleware.Responder to an httptest recorder and returns
// the HTTP status code + decoded ErrorResponse body (if any).
func statusOf(t *testing.T, resp middleware.Responder) (int, *models.ErrorResponse) {
	t.Helper()
	require.NotNil(t, resp, "admission gate must return a terminal responder")
	rec := httptest.NewRecorder()
	resp.WriteResponse(rec, runtime.JSONProducer())
	var body models.ErrorResponse
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
	}
	return rec.Code, &body
}

// Pins fail-closed: a list error must 503, never admit an unchecked submit.
func TestCheckReindexAdmission_FailsClosedOnListError(t *testing.T) {
	h := admissionHandler()

	resp := h.checkReindexAdmission(nil, "C", db.ReindexTypeChangeTokenization,
		[]string{"p"}, nil /* tasks */, errors.New("raft leader unavailable"))

	code, body := statusOf(t, resp)
	require.Equal(t, http.StatusServiceUnavailable, code,
		"list failure must fail closed with 503, not admit the submit")
	require.Len(t, body.Error, 1)
	assert.Contains(t, body.Error[0].Message, "listing in-flight tasks failed",
		"503 body must explain why the precondition check could not run")
}

// Pins that the cap is enforced by the real gate, not just the count helper.
func TestCheckReindexAdmission_CapExceededReturns429(t *testing.T) {
	h := admissionHandler()
	const collection = "C"

	// maxConcurrentReindexPerCollection active tasks, distinct props, all
	// enable-rangeable (no cross-type conflicts) so we reach the cap check.
	tasks := make([]*distributedtask.Task, 0, maxConcurrentReindexPerCollection)
	for i := 0; i < maxConcurrentReindexPerCollection; i++ {
		tasks = append(tasks, buildTask(t,
			"C:enable-rangeable:p"+string(rune('a'+i%26))+string(rune('a'+(i/26)%26)),
			distributedtask.TaskStatusStarted,
			db.ReindexTaskPayload{
				MigrationType: db.ReindexTypeEnableRangeable,
				Collection:    collection,
				Properties:    []string{"p" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))},
			}, nil))
	}
	require.Equal(t, maxConcurrentReindexPerCollection,
		countStartedTasksForCollection(collection, tasks),
		"fixture must have exactly the cap in flight")

	// The next submission (a distinct property) must be capped.
	resp := h.checkReindexAdmission(nil, collection, db.ReindexTypeEnableRangeable,
		[]string{"pZZ"}, tasks, nil)

	code, body := statusOf(t, resp)
	require.Equal(t, http.StatusTooManyRequests, code,
		"the (cap+1)th concurrent submit must return 429")
	require.Len(t, body.Error, 1)
	assert.Contains(t, body.Error[0].Message, "32")
}

// Pins the boundary: one below the cap must be admitted.
func TestCheckReindexAdmission_CapMinusOneAdmits(t *testing.T) {
	h := admissionHandler()
	const collection = "C"

	tasks := make([]*distributedtask.Task, 0, maxConcurrentReindexPerCollection-1)
	for i := 0; i < maxConcurrentReindexPerCollection-1; i++ {
		tasks = append(tasks, buildTask(t,
			"C:enable-rangeable:p"+string(rune('a'+i%26))+string(rune('a'+(i/26)%26)),
			distributedtask.TaskStatusStarted,
			db.ReindexTaskPayload{
				MigrationType: db.ReindexTypeEnableRangeable,
				Collection:    collection,
				Properties:    []string{"p" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))},
			}, nil))
	}

	resp := h.checkReindexAdmission(nil, collection, db.ReindexTypeEnableRangeable,
		[]string{"pZZ"}, tasks, nil)
	require.Nil(t, resp, "one below the cap must be admitted (proceed)")
}

// Pins that conflict (409, naming the task) is checked before the cap.
func TestCheckReindexAdmission_ConflictReturns409(t *testing.T) {
	h := admissionHandler()
	const collection = "C"

	inflight := buildTask(t, "C:change-tokenization:p:aaaa",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeChangeTokenization,
			Collection:    collection,
			Properties:    []string{"p"},
		}, nil)

	resp := h.checkReindexAdmission(nil, collection, db.ReindexTypeChangeTokenization,
		[]string{"p"}, []*distributedtask.Task{inflight}, nil)

	code, body := statusOf(t, resp)
	require.Equal(t, http.StatusConflict, code)
	require.Len(t, body.Error, 1)
	assert.Contains(t, body.Error[0].Message, "C:change-tokenization:p:aaaa",
		"409 body must name the offending in-flight task")
}

// Pins that an undecodable in-flight payload fails closed, not skipped.
func TestCheckReindexAdmission_UnparseablePayloadReturns503(t *testing.T) {
	h := admissionHandler()
	const collection = "C"

	bad := &distributedtask.Task{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "C:mystery:p:aaaa", Version: 1},
		Payload:        []byte(`{not valid json`),
		Status:         distributedtask.TaskStatusStarted,
	}

	resp := h.checkReindexAdmission(nil, collection, db.ReindexTypeChangeTokenization,
		[]string{"p"}, []*distributedtask.Task{bad}, nil)

	code, _ := statusOf(t, resp)
	require.Equal(t, http.StatusServiceUnavailable, code)
}

// Pins the happy path: no conflicts, no cap breach → proceed.
func TestCheckReindexAdmission_CleanProceeds(t *testing.T) {
	h := admissionHandler()
	resp := h.checkReindexAdmission(nil, "C", db.ReindexTypeChangeTokenization,
		[]string{"p"}, nil, nil)
	require.Nil(t, resp, "no conflict, no cap breach, no list error → proceed")
}

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

// TestCheckReindexAdmission_FailsClosedOnListError pins QA-2: when the
// in-flight task list cannot be fetched (RAFT/cluster blip), the pre-submit
// gate must FAIL CLOSED with 503 rather than admitting an unchecked submit.
// Admitting it would skip the 409-conflict and 429-cap checks and silently
// let a conflicting migration through — an availability blip turned into a
// correctness hole. Because the gate returns a non-nil responder, the caller
// (submitReindexTask) returns it before ever calling AddDistributedTask, so
// nothing is submitted.
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

// TestCheckReindexAdmission_CapExceededReturns429 pins QA-3: with the cap's
// worth of active tasks already in flight for the collection, the next
// (33rd) submission is rejected with 429 through the real gate — not merely
// counted by the count helper. Uses distinct properties so no conflict fires
// before the cap check.
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

// TestCheckReindexAdmission_CapMinusOneAdmits pins the lower boundary: one
// below the cap must be admitted (nil responder → proceed).
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

// TestCheckReindexAdmission_ConflictReturns409 pins that an overlapping
// in-flight migration on the same property yields 409 naming the task,
// checked BEFORE the cap.
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

// TestCheckReindexAdmission_UnparseablePayloadReturns503 pins that an
// in-flight task with an undecodable payload fails closed (cannot prove
// non-conflict) with 503 rather than being silently skipped.
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

// TestCheckReindexAdmission_CleanProceeds pins the happy path: no tasks, no
// list error → nil (proceed to submit).
func TestCheckReindexAdmission_CleanProceeds(t *testing.T) {
	h := admissionHandler()
	resp := h.checkReindexAdmission(nil, "C", db.ReindexTypeChangeTokenization,
		[]string{"p"}, nil, nil)
	require.Nil(t, resp, "no conflict, no cap breach, no list error → proceed")
}

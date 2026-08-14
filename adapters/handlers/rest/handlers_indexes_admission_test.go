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
	"context"
	"encoding/json"
	"errors"
	"io"
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

// TestCheckReindexAdmission pins admission ordering: conflict (409) before
// cap (429), fail-closed (503) without leaking the task ID, and proceed.
func TestCheckReindexAdmission(t *testing.T) {
	const collection = "C"

	// capTasks builds n distinct-property enable-rangeable tasks (no cross-type
	// conflicts) so the cap check — not the conflict check — is what fires.
	capTasks := func(n int) []*distributedtask.Task {
		tasks := make([]*distributedtask.Task, 0, n)
		for i := 0; i < n; i++ {
			p := "p" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
			tasks = append(tasks, buildTask(t, "C:enable-rangeable:"+p,
				distributedtask.TaskStatusStarted,
				db.ReindexTaskPayload{MigrationType: db.ReindexTypeEnableRangeable, Collection: collection, Properties: []string{p}}, nil))
		}
		return tasks
	}

	cases := []struct {
		name            string
		migrationType   db.ReindexMigrationType
		props           []string
		tasks           []*distributedtask.Task
		wantCode        int
		wantContains    string
		wantNotContains string
	}{
		{
			name:          "cap exceeded → 429",
			migrationType: db.ReindexTypeEnableRangeable,
			props:         []string{"pZZ"},
			tasks:         capTasks(maxConcurrentReindexPerCollection),
			wantCode:      http.StatusTooManyRequests,
			wantContains:  "32",
		},
		{
			name:          "one below cap → proceed",
			migrationType: db.ReindexTypeEnableRangeable,
			props:         []string{"pZZ"},
			tasks:         capTasks(maxConcurrentReindexPerCollection - 1),
		},
		{
			name:          "conflict checked before the cap → 409 naming the task",
			migrationType: db.ReindexTypeChangeTokenization,
			props:         []string{"p"},
			tasks: []*distributedtask.Task{buildTask(t, "C:change-tokenization:p:aaaa",
				distributedtask.TaskStatusStarted,
				db.ReindexTaskPayload{MigrationType: db.ReindexTypeChangeTokenization, Collection: collection, Properties: []string{"p"}}, nil)},
			wantCode:     http.StatusConflict,
			wantContains: "C:change-tokenization:p:aaaa",
		},
		{
			name:          "unparseable payload → 503 without leaking the task ID",
			migrationType: db.ReindexTypeChangeTokenization,
			props:         []string{"p"},
			tasks: []*distributedtask.Task{{
				Namespace:      db.ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "C:mystery:p:aaaa", Version: 1},
				Payload:        []byte(`{not valid json`),
				Status:         distributedtask.TaskStatusStarted,
			}},
			wantCode:        http.StatusServiceUnavailable,
			wantNotContains: "mystery",
		},
		{
			name:          "clean → proceed",
			migrationType: db.ReindexTypeChangeTokenization,
			props:         []string{"p"},
		},
	}

	h := admissionHandler()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := h.checkReindexAdmission(nil, collection, tc.migrationType, tc.props, tc.tasks)
			if tc.wantCode == 0 {
				require.Nil(t, resp, "expected proceed (nil responder)")
				return
			}
			code, body := statusOf(t, resp)
			require.Equal(t, tc.wantCode, code)
			require.Len(t, body.Error, 1)
			if tc.wantContains != "" {
				assert.Contains(t, body.Error[0].Message, tc.wantContains)
			}
			if tc.wantNotContains != "" {
				assert.NotContains(t, body.Error[0].Message, tc.wantNotContains)
			}
		})
	}
}

// fakeTaskLister injects a list error / result into reindexTasksOrFailClosed,
// which the concrete cluster service cannot be made to do in a unit test.
type fakeTaskLister struct {
	tasks map[string][]*distributedtask.Task
	err   error
}

func (f fakeTaskLister) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	return f.tasks, f.err
}

// Pins the live fail-closed 503 path: a list failure must reject rather than
// derive preconditions from a partial view.
func TestReindexTasksOrFailClosed_ListErrorReturns503(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	tasks, resp := reindexTasksOrFailClosed(context.Background(), nil,
		fakeTaskLister{err: errors.New("raft leader unavailable")}, logger)

	require.Nil(t, tasks, "no task list may be returned on failure")
	code, body := statusOf(t, resp)
	require.Equal(t, http.StatusServiceUnavailable, code,
		"a list failure must fail closed with 503, not admit an unchecked submit")
	require.Len(t, body.Error, 1)
	assert.Contains(t, body.Error[0].Message, "listing in-flight tasks failed",
		"503 body must explain why the precondition check could not run")
}

// Pins the success path: the reindex-namespace slice is returned, no responder.
func TestReindexTasksOrFailClosed_ReturnsReindexNamespace(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	want := []*distributedtask.Task{{TaskDescriptor: distributedtask.TaskDescriptor{ID: "x"}}}

	tasks, resp := reindexTasksOrFailClosed(context.Background(), nil,
		fakeTaskLister{tasks: map[string][]*distributedtask.Task{db.ReindexNamespace: want}}, logger)

	require.Nil(t, resp, "success path returns no responder")
	require.Equal(t, want, tasks)
}

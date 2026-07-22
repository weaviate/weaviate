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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestUpsertIndex_AuthorizesBeforeTaskListRead pins that an unprivileged caller
// is refused before the reindex task list is read — that list backs the join-202
// response, which discloses an in-flight task's ID.
func TestUpsertIndex_AuthorizesBeforeTaskListRead(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	authz := &denyAuthorizer{forbidden: authzerrors.NewForbidden(
		&models.Principal{Username: "u"}, authorization.UPDATE, "Movies")}
	// SchemaManager and ClusterService are left nil on purpose: the class read
	// would panic on the first and the task-list read would 503 on the second,
	// so a clean 403 is only reachable if authorization runs before both.
	h := &indexesHandlers{appState: &state.State{
		Authorizer:         authz,
		ReindexSubmitLocks: state.NewReindexSubmitLocks(),
		Logger:             logger,
		ServerConfig:       &config.WeaviateConfig{Config: config.Config{}},
	}}

	resp := h.upsertIndex(schema.SchemaObjectsIndexUpsertParams{
		HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
		ClassName:    "Movies",
		PropertyName: "title",
		IndexName:    "searchable",
		Body:         &models.IndexUpsertRequest{Algorithm: "blockmax"},
	}, &models.Principal{Username: "u"})

	code, _ := statusOf(t, resp)
	require.Equal(t, http.StatusForbidden, code, "an unprivileged caller must get 403")
	assert.Equal(t, authorization.UPDATE, authz.verb)
	assert.Equal(t, authorization.Collections("Movies"), authz.resources,
		"PUT per-property index must demand Collections (data+metadata), not metadata-only")
}

// TestNoopOrJoinResponder pins the two noop response shapes, and that a joined
// task ID is disclosed with the caller's own namespace stripped.
func TestNoopOrJoinResponder(t *testing.T) {
	cases := []struct {
		name       string
		principal  *models.Principal
		plan       upsertPlan
		wantCode   int
		wantStatus string
		wantTaskID string
	}{
		{
			name:       "no joined task -> 200 NO_OP without a task ID",
			principal:  &models.Principal{Username: "customer1:u1", Namespace: "customer1"},
			plan:       upsertPlan{noop: true},
			wantCode:   http.StatusOK,
			wantStatus: reindexNoOpStatus,
		},
		{
			name:       "joined task -> 202 STARTED with the caller's namespace stripped",
			principal:  &models.Principal{Username: "customer1:u1", Namespace: "customer1"},
			plan:       upsertPlan{noop: true, joinTaskID: "customer1:Movies:rebuild-searchable:title:ab3f"},
			wantCode:   http.StatusAccepted,
			wantStatus: reindexStartedStatus,
			wantTaskID: "Movies:rebuild-searchable:title:ab3f",
		},
		{
			name:       "joined task, un-namespaced caller -> ID unchanged",
			principal:  &models.Principal{Username: "u1"},
			plan:       upsertPlan{noop: true, joinTaskID: "Movies:rebuild-searchable:title:ab3f"},
			wantCode:   http.StatusAccepted,
			wantStatus: reindexStartedStatus,
			wantTaskID: "Movies:rebuild-searchable:title:ab3f",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			noopOrJoinResponder(tc.principal, tc.plan).WriteResponse(rec, runtime.JSONProducer())

			var body models.IndexUpdateResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tc.wantCode, rec.Code)
			assert.Equal(t, tc.wantStatus, body.Status)
			assert.Equal(t, tc.wantTaskID, body.TaskID)
		})
	}
}

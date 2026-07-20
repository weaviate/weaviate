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
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

// foreignLeakTaskID embeds a namespace/collection/property the caller must
// never see. The reindex conflict pre-flights emit task IDs BEFORE the
// collection filter, and StripErrorMessage strips only the caller's own
// namespace — so a foreign task ID would leak intact if returned to the
// caller.
const foreignLeakTaskID = "otherns:SecretCollection:change-tokenization:secretprop:beef"

// TestCheckReindexAdmission_MalformedForeignTask_NoTaskIDLeak pins leak
// site 1 (the indexes submit pre-flight): a malformed foreign in-flight
// task must fail the submit closed (503) WITHOUT surfacing the foreign
// task ID, while retaining it server-side for operators.
func TestCheckReindexAdmission_MalformedForeignTask_NoTaskIDLeak(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	h := &indexesHandlers{appState: &state.State{Logger: logger}}

	tasks := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: foreignLeakTaskID},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        []byte("not json"),
	}}

	principal := &models.Principal{Username: "customer1:u1", Namespace: "customer1"}
	resp := h.checkReindexAdmission(principal, "customer1:MyClass", db.ReindexTypeEnableFilterable, []string{"p"}, tasks)

	sua, ok := resp.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
	require.True(t, ok, "malformed in-flight payload must fail closed with 503")
	require.NotEmpty(t, sua.Payload.Error)
	require.NotContains(t, sua.Payload.Error[0].Message, foreignLeakTaskID,
		"foreign task ID must not leak to the caller")
	require.Contains(t, hook.LastEntry().Message, foreignLeakTaskID,
		"task ID must be retained server-side so operators can act")
}

// TestCheckReindexConflictForPropertyMutation_MalformedForeignTask_NoTaskIDLeak
// pins leak site 2 (the property-mutation pre-flight): a malformed foreign
// in-flight task must still refuse the mutation but WITHOUT surfacing the
// foreign task ID, while retaining it server-side.
func TestCheckReindexConflictForPropertyMutation_MalformedForeignTask_NoTaskIDLeak(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	h := &schemaHandlers{
		logger: logger,
		reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
			db.ReindexNamespace: {{
				Namespace:      db.ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: foreignLeakTaskID},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        []byte("not json"),
			}},
		}},
	}

	reason := h.checkReindexConflictForPropertyMutation(context.Background(), "customer1:MyClass", "title")
	require.NotEmpty(t, reason, "a malformed in-flight task must still refuse the mutation")
	require.NotContains(t, reason, foreignLeakTaskID, "foreign task ID must not leak to the caller")
	require.Equal(t, foreignLeakTaskID, hook.LastEntry().Data["task_id"],
		"task ID must be retained server-side so operators can act")
}

type recordingReindexTaskLister struct {
	called bool
	tasks  map[string][]*distributedtask.Task
}

func (r *recordingReindexTaskLister) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	r.called = true
	return r.tasks, nil
}

type denyingAuthorizer struct{}

func (denyingAuthorizer) Authorize(ctx context.Context, p *models.Principal, verb string, resources ...string) error {
	return authzerrors.NewForbidden(p, verb, resources...)
}

func (denyingAuthorizer) AuthorizeSilent(ctx context.Context, p *models.Principal, verb string, resources ...string) error {
	return authzerrors.NewForbidden(p, verb, resources...)
}

func (denyingAuthorizer) FilterAuthorizedResources(ctx context.Context, p *models.Principal, verb string, resources ...string) ([]string, error) {
	return nil, nil
}

// TestDeleteClassPropertyIndex_UnprivilegedGets403BeforePreflight pins that
// authorization runs at the top of the DELETE handler, before the reindex
// conflict pre-flight — so an authenticated-but-unprivileged caller gets a
// 403 and never reaches (and never leaks from) the pre-flight.
func TestDeleteClassPropertyIndex_UnprivilegedGets403BeforePreflight(t *testing.T) {
	// A real in-flight conflict on the caller's own (class, property): if
	// authorization did NOT run first, the pre-flight would reach this task
	// and return 422. Authorization must win with a 403 instead, and the
	// pre-flight must never be consulted (rec.called stays false).
	conflictPayload, err := json.Marshal(db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "MyClass",
		Properties:    []string{"title"},
	})
	require.NoError(t, err)

	rec := &recordingReindexTaskLister{tasks: map[string][]*distributedtask.Task{
		db.ReindexNamespace: {{
			Namespace:      db.ReindexNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1"},
			Status:         distributedtask.TaskStatusStarted,
			Payload:        conflictPayload,
		}},
	}}
	h := &schemaHandlers{
		authorizer:          denyingAuthorizer{},
		metricRequestsTotal: newSchemaRequestsTotal(nil, logrus.New()),
		reindexTaskLister:   rec,
	}

	resp := h.deleteClassPropertyIndex(schema.SchemaObjectsPropertiesDeleteParams{
		HTTPRequest:  httptest.NewRequest("DELETE", "/", nil),
		ClassName:    "MyClass",
		PropertyName: "title",
		IndexName:    "searchable",
	}, &models.Principal{Username: "u1"})

	_, ok := resp.(*schema.SchemaObjectsPropertiesDeleteForbidden)
	require.True(t, ok, "unprivileged caller must get 403")
	require.False(t, rec.called, "authorization must run before the reindex conflict pre-flight")
}

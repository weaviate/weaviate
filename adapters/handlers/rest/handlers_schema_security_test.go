//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

// denyAuthorizer denies every request with a Forbidden error.
type denyAuthorizer struct{ forbidden authzerrors.Forbidden }

func (d denyAuthorizer) Authorize(context.Context, *models.Principal, string, ...string) error {
	return d.forbidden
}

func (d denyAuthorizer) AuthorizeSilent(context.Context, *models.Principal, string, ...string) error {
	return d.forbidden
}

func (d denyAuthorizer) FilterAuthorizedResources(context.Context, *models.Principal, string, ...string) ([]string, error) {
	return nil, d.forbidden
}

// recordingReindexLister flags whether the conflict pre-flight (the leak site)
// was consulted.
type recordingReindexLister struct{ consulted bool }

func (r *recordingReindexLister) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	r.consulted = true
	return nil, nil
}

// TestCheckReindexConflictForPropertyMutation_RedactsForeignTaskID pins that an
// undecodable in-flight task's (possibly foreign-namespace) ID is NOT returned
// to the caller — StripErrorMessage only strips the caller's own namespace, so
// echoing task.ID leaked a cross-tenant identifier. The operator can still act:
// the ID is logged server-side.
func TestCheckReindexConflictForPropertyMutation_RedactsForeignTaskID(t *testing.T) {
	var logBuf bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&logBuf)

	const foreignID = "victimNamespace:SecretCollection:change-tokenization:secret:ffff"
	h := &schemaHandlers{
		logger:              logger,
		metricRequestsTotal: newSchemaRequestsTotal(nil, logrus.New()),
		reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
			db.ReindexNamespace: {{
				Namespace:      db.ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: foreignID},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        []byte("{not valid json"),
			}},
		}},
	}

	msg := h.checkReindexConflictForPropertyMutation(context.Background(), "Movies", "title")
	require.NotEmpty(t, msg, "an undecodable in-flight task must still refuse the mutation")
	require.NotContains(t, msg, foreignID, "the foreign task ID must not leak to the caller")
	require.NotContains(t, msg, "SecretCollection", "no fragment of the foreign namespace may leak")
	require.Contains(t, msg, "Movies", "the caller's own class name is safe to echo")
	require.Contains(t, logBuf.String(), foreignID,
		"the task ID must be logged server-side so an operator can find the task")
}

// TestDeleteClassPropertyIndex_AuthorizesBeforeConflictPreflight pins that an
// unprivileged caller gets 403 BEFORE the conflict pre-flight (the redaction
// site) runs — the authz that governs this operation must not be reached only
// deep inside manager.DeleteClassPropertyIndex.
func TestDeleteClassPropertyIndex_AuthorizesBeforeConflictPreflight(t *testing.T) {
	lister := &recordingReindexLister{}
	h := &schemaHandlers{
		metricRequestsTotal: newSchemaRequestsTotal(nil, logrus.New()),
		authorizer: denyAuthorizer{forbidden: authzerrors.NewForbidden(
			&models.Principal{Username: "u"}, authorization.UPDATE, "Movies")},
		reindexTaskLister: lister,
		logger:            logrus.New(),
	}

	resp := h.deleteClassPropertyIndex(schema.SchemaObjectsPropertiesDeleteParams{
		HTTPRequest:  httptest.NewRequest("DELETE", "/", nil),
		ClassName:    "Movies",
		PropertyName: "title",
	}, &models.Principal{Username: "u"})

	_, ok := resp.(*schema.SchemaObjectsPropertiesDeleteForbidden)
	require.True(t, ok, "an unprivileged caller must get 403")
	require.False(t, lister.consulted,
		"authz must run BEFORE the conflict pre-flight so the leak site is unreachable to unprivileged callers")
}

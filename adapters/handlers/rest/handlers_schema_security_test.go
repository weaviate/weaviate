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

// denyAuthorizer denies every request with a Forbidden error and records the
// last (verb, resources) it was asked to authorize, so tests can assert which
// permission an endpoint demands.
type denyAuthorizer struct {
	forbidden authzerrors.Forbidden
	verb      string
	resources []string
}

func (d *denyAuthorizer) Authorize(_ context.Context, _ *models.Principal, verb string, resources ...string) error {
	d.verb, d.resources = verb, resources
	return d.forbidden
}

func (d *denyAuthorizer) AuthorizeSilent(context.Context, *models.Principal, string, ...string) error {
	return d.forbidden
}

func (d *denyAuthorizer) FilterAuthorizedResources(context.Context, *models.Principal, string, ...string) ([]string, error) {
	return nil, d.forbidden
}

// recordingReindexLister flags whether the conflict pre-flight (the leak site)
// was consulted.
type recordingReindexLister struct{ consulted bool }

func (r *recordingReindexLister) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	r.consulted = true
	return nil, nil
}

// TestCheckReindexConflictForPropertyMutation_RedactsForeignTaskID pins that a
// foreign-namespace task ID is never returned to the caller (only logged
// server-side) — StripErrorMessage only strips the caller's own namespace.
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
// unprivileged caller gets 403 before the conflict pre-flight runs, and that
// the demanded permission is Collections (data+metadata), not metadata-only.
func TestDeleteClassPropertyIndex_AuthorizesBeforeConflictPreflight(t *testing.T) {
	lister := &recordingReindexLister{}
	authz := &denyAuthorizer{forbidden: authzerrors.NewForbidden(
		&models.Principal{Username: "u"}, authorization.UPDATE, "Movies")}
	h := &schemaHandlers{
		metricRequestsTotal: newSchemaRequestsTotal(nil, logrus.New()),
		authorizer:          authz,
		reindexTaskLister:   lister,
		logger:              logrus.New(),
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
	require.Equal(t, authorization.UPDATE, authz.verb)
	require.Equal(t, authorization.Collections("Movies"), authz.resources,
		"DELETE per-property index must demand Collections (data+metadata), not metadata-only")
}

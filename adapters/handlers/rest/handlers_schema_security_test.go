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
	"context"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
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

// TestDeleteClassPropertyIndex_AuthorizesBeforeConflictPreflight pins that an
// unprivileged caller gets 403 BEFORE the conflict pre-flight runs, so it
// cannot tell whether a reindex task is in flight.
func TestDeleteClassPropertyIndex_AuthorizesBeforeConflictPreflight(t *testing.T) {
	lister := &recordingReindexLister{}
	h := &schemaHandlers{
		metricRequestsTotal: newSchemaRequestsTotal(nil, logrus.New()),
		authorizer: denyAuthorizer{forbidden: authzerrors.NewForbidden(
			&models.Principal{Username: "u"}, authorization.UPDATE, "Movies")},
		reindexTaskLister: lister,
	}

	resp := h.deleteClassPropertyIndex(schema.SchemaObjectsPropertiesDeleteParams{
		HTTPRequest:  httptest.NewRequest("DELETE", "/", nil),
		ClassName:    "Movies",
		PropertyName: "title",
	}, &models.Principal{Username: "u"})

	_, ok := resp.(*schema.SchemaObjectsPropertiesDeleteForbidden)
	require.True(t, ok, "an unprivileged caller must get 403")
	require.False(t, lister.consulted,
		"authz must run BEFORE the conflict pre-flight so it is unreachable to unprivileged callers")
}

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
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/namespaces"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type fakeReindexTaskLister struct {
	tasks map[string][]*distributedtask.Task
}

func (f fakeReindexTaskLister) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return f.tasks, nil
}

// recordingLockProvider records the (collection, property) keys passed to
// SubmitLockFor so a test can assert which class-name form the handler locks on.
type recordingLockProvider struct {
	mu   sync.Mutex
	keys []string
}

func (r *recordingLockProvider) SubmitLockFor(collection, property string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, collection+"/"+property)
	return &sync.Mutex{}
}

// A shard status update refused by the namespace is the caller's problem, not a
// fault: the propose-time gate returns a lifecycle sentinel, and 500 would read
// as a broken server and hide it from anything retrying on 4xx.
func TestShardStatusErrResponder(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want middleware.Responder
	}{
		{
			name: "suspended namespace is unprocessable",
			err:  fmt.Errorf("propose: %w", namespaces.ErrNamespaceSuspended),
			want: schema.NewSchemaObjectsShardsUpdateUnprocessableEntity(),
		},
		{
			name: "deleting namespace is unprocessable",
			err:  namespaces.ErrNamespaceDeleting,
			want: schema.NewSchemaObjectsShardsUpdateUnprocessableEntity(),
		},
		{
			name: "missing namespace is unprocessable",
			err:  namespaces.ErrNamespaceGone,
			want: schema.NewSchemaObjectsShardsUpdateUnprocessableEntity(),
		},
		// Resuming asks for 503, which this operation has no responder for, so it
		// keeps the fallback rather than being mislabelled a client error.
		{
			name: "resuming namespace falls through",
			err:  namespaces.ErrNamespaceResuming,
			want: schema.NewSchemaObjectsShardsUpdateInternalServerError(),
		},
		{
			name: "a forbidden error still maps to forbidden",
			err:  authzerrors.Forbidden{},
			want: schema.NewSchemaObjectsShardsUpdateForbidden(),
		},
		{
			name: "an unknown shard still maps to not found",
			err:  fmt.Errorf("shard: %w", schemaUC.ErrNotFound),
			want: schema.NewSchemaObjectsShardsUpdateNotFound(),
		},
		{
			name: "anything else stays a server error",
			err:  errors.New("boom"),
			want: schema.NewSchemaObjectsShardsUpdateInternalServerError(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.IsType(t, tt.want, shardStatusErrResponder(nil, tt.err))
		})
	}
}

// TestDeleteClassPropertyIndex_NamespaceConflictPreflight: reindex tasks are keyed by
// the qualified class, so a namespaced caller deleting by short name must still match an
// in-flight task on customer1:Movies and get a 422 — i.e. the handler qualifies first.
func TestDeleteClassPropertyIndex_NamespaceConflictPreflight(t *testing.T) {
	payload, err := json.Marshal(db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "customer1:Movies",
		Properties:    []string{"title"},
	})
	require.NoError(t, err)

	h := &schemaHandlers{
		namespacesEnabled:   true,
		metricRequestsTotal: newSchemaRequestsTotal(nil, logrus.New()),
		// allow-all authorizer: this test is about class qualification, not authz
		authorizer: &authorization.DummyAuthorizer{},
		reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
			db.ReindexNamespace: {{
				Namespace:      db.ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1"},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        payload,
			}},
		}},
	}

	resp := h.deleteClassPropertyIndex(schema.SchemaObjectsPropertiesDeleteParams{
		HTTPRequest:  httptest.NewRequest("DELETE", "/", nil),
		ClassName:    "Movies", // short name; the handler qualifies to customer1:Movies
		PropertyName: "title",
	}, &models.Principal{Username: "customer1:u1", Namespace: "customer1"})

	_, ok := resp.(*schema.SchemaObjectsPropertiesDeleteUnprocessableEntity)
	require.True(t, ok, "expected 422 reindex-conflict — the handler must qualify the class before the conflict pre-flight")
}

// TestDeleteClassPropertyIndex_SubmitLockKeyedOnQualifiedClass: the submit lock
// must key on the qualified class. Otherwise a namespaced caller (short "Movies")
// and a global admin (qualified "customer1:Movies") take different locks for the
// same collection and stop serializing, reopening the PUT-vs-DELETE torn-bucket race.
func TestDeleteClassPropertyIndex_SubmitLockKeyedOnQualifiedClass(t *testing.T) {
	// A matching in-flight task makes the conflict pre-flight return 422 after
	// the lock is taken, so the handler never reaches the (nil) manager.
	payload, err := json.Marshal(db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "customer1:Movies",
		Properties:    []string{"title"},
	})
	require.NoError(t, err)

	rec := &recordingLockProvider{}
	h := &schemaHandlers{
		namespacesEnabled:   true,
		metricRequestsTotal: newSchemaRequestsTotal(nil, logrus.New()),
		// allow-all authorizer: this test is about the lock key, not authz
		authorizer:         &authorization.DummyAuthorizer{},
		reindexSubmitLocks: rec,
		reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
			db.ReindexNamespace: {{
				Namespace:      db.ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1"},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        payload,
			}},
		}},
	}

	resp := h.deleteClassPropertyIndex(schema.SchemaObjectsPropertiesDeleteParams{
		HTTPRequest:  httptest.NewRequest("DELETE", "/", nil),
		ClassName:    "Movies", // short name; the handler qualifies to customer1:Movies
		PropertyName: "title",
	}, &models.Principal{Username: "customer1:u1", Namespace: "customer1"})

	_, ok := resp.(*schema.SchemaObjectsPropertiesDeleteUnprocessableEntity)
	require.True(t, ok)
	require.Equal(t, []string{"customer1:Movies/title"}, rec.keys,
		"submit lock must be keyed on the qualified class so callers using the short vs qualified name share it")
}

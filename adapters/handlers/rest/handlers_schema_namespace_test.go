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
	"net/http"
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
	"github.com/weaviate/weaviate/usecases/config"
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

// A namespace-refused shard status update is a 4xx, not a 500 fault, and its
// body is the neutral namespaces.PublicMessage text.
func TestShardStatusErrResponder(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		want    middleware.Responder
		message string
	}{
		{
			name:    "suspended namespace is unprocessable",
			err:     fmt.Errorf("propose: %w", namespaces.ErrNamespaceSuspended),
			want:    schema.NewSchemaObjectsShardsUpdateUnprocessableEntity(),
			message: "instance suspended",
		},
		{
			name:    "deleting namespace is unprocessable",
			err:     namespaces.ErrNamespaceDeleting,
			want:    schema.NewSchemaObjectsShardsUpdateUnprocessableEntity(),
			message: "instance unavailable",
		},
		{
			name:    "missing namespace is unprocessable",
			err:     namespaces.ErrNamespaceGone,
			want:    schema.NewSchemaObjectsShardsUpdateUnprocessableEntity(),
			message: "instance unavailable",
		},
		// Resuming asks for 503, which this operation has no responder for, so it
		// keeps the fallback rather than being mislabelled a client error.
		{
			name:    "resuming namespace falls through",
			err:     namespaces.ErrNamespaceResuming,
			want:    schema.NewSchemaObjectsShardsUpdateInternalServerError(),
			message: "instance resuming, retry shortly",
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
			name: "an invalid class name is unprocessable",
			err:  fmt.Errorf("%w: %w", schemaUC.ErrValidation, errors.New("'a:Foo' is not a valid class name")),
			want: schema.NewSchemaObjectsShardsUpdateUnprocessableEntity(),
		},
		{
			name: "anything else stays a server error",
			err:  errors.New("boom"),
			want: schema.NewSchemaObjectsShardsUpdateInternalServerError(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := shardStatusErrResponder(nil, tt.err)
			require.IsType(t, tt.want, resp)
			if tt.message != "" {
				_, body := statusOf(t, resp)
				require.Len(t, body.Error, 1)
				require.Equal(t, tt.message, body.Error[0].Message)
			}
		})
	}
}

// aliasLookupFailure fails the alias lookup that alias update and delete start with.
type aliasLookupFailure struct {
	schemaUC.SchemaManager
	err error
}

func (f aliasLookupFailure) GetAliases(context.Context, string, *models.Class) ([]*models.Alias, error) {
	return nil, f.err
}

func (f aliasLookupFailure) GetAlias(context.Context, string) (*models.Alias, error) {
	return nil, f.err
}

// Alias update and delete render a namespace refusal as namespaces.PublicMessage text.
func TestAliasHandlersRenderNamespaceErrorsNeutrally(t *testing.T) {
	for _, tt := range []struct {
		name     string
		err      error
		wantCode int
		message  string
	}{
		{"suspended", fmt.Errorf("propose: %w", namespaces.ErrNamespaceSuspended), http.StatusUnprocessableEntity, "instance suspended"},
		{"deleting", namespaces.ErrNamespaceDeleting, http.StatusUnprocessableEntity, "instance unavailable"},
		{"resuming", namespaces.ErrNamespaceResuming, http.StatusInternalServerError, "instance resuming, retry shortly"},
		{"anything else keeps its detail", errors.New("boom"), http.StatusInternalServerError, "boom"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := schemaUC.NewHandler(nil, aliasLookupFailure{err: tt.err}, nil, logrus.New(),
				&authorization.DummyAuthorizer{}, nil, config.Config{}, nil, nil, nil, nil, nil, nil,
				schemaUC.Parser{}, nil, nil, nil)
			require.NoError(t, err)
			h := &aliasesHandlers{
				manager:             &schemaUC.Manager{Handler: handler},
				metricRequestsTotal: newSchemaRequestsTotal(nil, logrus.New()),
			}
			req := httptest.NewRequest("PUT", "/", nil)

			for op, resp := range map[string]middleware.Responder{
				"update": h.updateAlias(schema.AliasesUpdateParams{
					HTTPRequest: req, AliasName: "A", Body: schema.AliasesUpdateBody{Class: "C"},
				}, nil),
				"delete": h.deleteAlias(schema.AliasesDeleteParams{HTTPRequest: req, AliasName: "A"}, nil),
			} {
				code, body := statusOf(t, resp)
				require.Equal(t, tt.wantCode, code, op)
				require.Len(t, body.Error, 1, op)
				require.Equal(t, tt.message, body.Error[0].Message, op)
			}
		})
	}
}

// refusingAuthorizer fails every check with err, the first call each schema
// write makes, so the handler renders err without further dependencies.
type refusingAuthorizer struct {
	authorization.DummyAuthorizer
	err error
}

func (a *refusingAuthorizer) Authorize(context.Context, *models.Principal, string, ...string) error {
	return a.err
}

// Schema writes the namespace can refuse render that refusal as
// namespaces.PublicMessage text and keep their status code.
func TestSchemaWritesRenderNamespaceErrorsNeutrally(t *testing.T) {
	req := httptest.NewRequest("POST", "/", nil)
	ops := []struct {
		name     string
		wantCode int
		call     func(*schemaHandlers, *aliasesHandlers) middleware.Responder
	}{
		{"add class", http.StatusUnprocessableEntity, func(s *schemaHandlers, _ *aliasesHandlers) middleware.Responder {
			return s.addClass(schema.SchemaObjectsCreateParams{HTTPRequest: req, ObjectClass: &models.Class{Class: "C"}}, nil)
		}},
		{"delete class", http.StatusBadRequest, func(s *schemaHandlers, _ *aliasesHandlers) middleware.Responder {
			return s.deleteClass(schema.SchemaObjectsDeleteParams{HTTPRequest: req, ClassName: "C"}, nil)
		}},
		{"create tenants", http.StatusUnprocessableEntity, func(s *schemaHandlers, _ *aliasesHandlers) middleware.Responder {
			return s.createTenants(schema.TenantsCreateParams{HTTPRequest: req, ClassName: "C", Body: []*models.Tenant{{Name: "t"}}}, nil)
		}},
		{"update tenants", http.StatusUnprocessableEntity, func(s *schemaHandlers, _ *aliasesHandlers) middleware.Responder {
			return s.updateTenants(schema.TenantsUpdateParams{HTTPRequest: req, ClassName: "C", Body: []*models.Tenant{{Name: "t"}}}, nil)
		}},
		{"delete tenants", http.StatusUnprocessableEntity, func(s *schemaHandlers, _ *aliasesHandlers) middleware.Responder {
			return s.deleteTenants(schema.TenantsDeleteParams{HTTPRequest: req, ClassName: "C", Tenants: []string{"t"}}, nil)
		}},
		{"add alias", http.StatusInternalServerError, func(_ *schemaHandlers, a *aliasesHandlers) middleware.Responder {
			return a.addAlias(schema.AliasesCreateParams{HTTPRequest: req, Body: &models.Alias{Alias: "A", Class: "C"}}, nil)
		}},
	}
	errs := []struct {
		name    string
		err     error
		message string
	}{
		{"suspended", fmt.Errorf("propose: %w", namespaces.ErrNamespaceSuspended), "instance suspended"},
		{"resuming", namespaces.ErrNamespaceResuming, "instance resuming, retry shortly"},
		{"anything else keeps its detail", errors.New("boom"), "boom"},
	}

	for _, op := range ops {
		for _, tt := range errs {
			t.Run(op.name+"/"+tt.name, func(t *testing.T) {
				handler, err := schemaUC.NewHandler(nil, nil, nil, logrus.New(),
					&refusingAuthorizer{err: tt.err}, nil, config.Config{}, nil, nil, nil, nil, nil, nil,
					schemaUC.Parser{}, nil, nil, nil)
				require.NoError(t, err)
				manager := &schemaUC.Manager{Handler: handler}
				metrics := newSchemaRequestsTotal(nil, logrus.New())

				code, body := statusOf(t, op.call(
					&schemaHandlers{manager: manager, metricRequestsTotal: metrics},
					&aliasesHandlers{manager: manager, metricRequestsTotal: metrics},
				))
				require.Equal(t, op.wantCode, code)
				require.Len(t, body.Error, 1)
				require.Equal(t, tt.message, body.Error[0].Message)
			})
		}
	}
}

func TestShardsStorageStatusErrResponder(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want middleware.Responder
	}{
		{
			name: "a forbidden error maps to forbidden",
			err:  authzerrors.Forbidden{},
			want: schema.NewSchemaObjectsShardsGetForbidden(),
		},
		{
			name: "an unknown class maps to not found",
			err:  fmt.Errorf("class: %w", schemaUC.ErrNotFound),
			want: schema.NewSchemaObjectsShardsGetNotFound(),
		},
		{
			name: "an invalid class name maps to unprocessable entity",
			err:  fmt.Errorf("%w: %w", schemaUC.ErrValidation, errors.New("'a:Foo' is not a valid class name")),
			want: schema.NewSchemaObjectsShardsGetUnprocessableEntity(),
		},
		{
			name: "anything else stays a server error",
			err:  errors.New("boom"),
			want: schema.NewSchemaObjectsShardsGetInternalServerError(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.IsType(t, tt.want, shardsStorageStatusErrResponder(nil, tt.err))
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

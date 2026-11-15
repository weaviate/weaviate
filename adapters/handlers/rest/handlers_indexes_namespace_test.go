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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/namespaces"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// TestUpsertIndex_SubmitLockKeyedOnQualifiedClass pins that a namespaced
// caller locks on the qualified class (DeleteClassPropertyIndex's key),
// acquired before the class read (which panics on nil SchemaManager here).
func TestUpsertIndex_SubmitLockKeyedOnQualifiedClass(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	locks := state.NewReindexSubmitLocks()
	h := &indexesHandlers{appState: &state.State{
		Authorizer:         &authorization.DummyAuthorizer{},
		ReindexSubmitLocks: locks,
		Logger:             logger,
		ServerConfig: &config.WeaviateConfig{Config: config.Config{
			Namespaces:            config.Namespaces{Enabled: true},
			RuntimeReindexEnabled: true,
		}},
		// SchemaManager left nil: the correctly-keyed handler blocks before the
		// class read; the buggy path reaches it and panics, which the goroutine recovers.
	}}

	held := locks.SubmitLockFor("customer1:Movies", "title")
	held.Lock()

	params := schema.SchemaObjectsIndexUpsertParams{
		HTTPRequest:  httptest.NewRequest("PUT", "/", nil),
		ClassName:    "Movies", // short name; qualifies to customer1:Movies
		PropertyName: "title",
		IndexName:    "searchable",
	}
	principal := &models.Principal{Username: "customer1:u1", Namespace: "customer1"}

	finished := make(chan struct{})
	// recover swallows the nil-SchemaManager panic so the test binary survives;
	// the deferred close fires whether upsertIndex returns or panics.
	go func() {
		defer func() {
			_ = recover()
			close(finished)
		}()
		_ = h.upsertIndex(params, principal)
	}()

	select {
	case <-finished:
		t.Fatal("upsertIndex did not block on the qualified-class lock — submit lock is keyed on the raw class name")
	case <-time.After(200 * time.Millisecond):
	}

	held.Unlock()

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("upsertIndex did not proceed after the qualified-class lock was released")
	}
}

// A submit into a suspended namespace is refused before the stale-state sweep,
// which would otherwise load the namespace's shards on this node. ClusterService
// is nil, so getting past the check answers 503 instead.
func TestSubmitReindexTask_RefusesSuspendedNamespace(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	nsc := namespaces.NewController(logger)
	require.NoError(t, nsc.Create(api.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, 1))
	require.NoError(t, nsc.ChangeState("customer1", api.NamespaceStateSuspended, namespaces.StateChange{AppliedIndex: 2}))

	h := &indexesHandlers{appState: &state.State{NamespacesController: nsc, Logger: logger}}
	principal := &models.Principal{Username: "customer1:u1", Namespace: "customer1"}
	resp := h.submitReindexTask(context.Background(), principal, &models.Class{Class: "customer1:Movies"},
		"customer1:Movies", "title", upsertPlan{}, nil, nil)

	rec := httptest.NewRecorder()
	resp.WriteResponse(rec, runtime.JSONProducer())
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	require.Contains(t, rec.Body.String(), "instance suspended")
}

// An invalid class name is a malformed request, not a refusal, and it is
// rejected before any schema read.
func TestGetIndexes_InvalidClassNameIsUnprocessable(t *testing.T) {
	h := &indexesHandlers{appState: &state.State{
		Authorizer:    &authorization.DummyAuthorizer{},
		SchemaManager: &schemaUC.Manager{SchemaReader: schemaUC.NewMockSchemaReader(t)},
		ServerConfig:  &config.WeaviateConfig{},
		Logger:        logrus.New(),
	}}

	resp := h.getIndexes(schema.SchemaObjectsIndexesGetParams{
		HTTPRequest: httptest.NewRequest(http.MethodGet, "/", nil),
		ClassName:   "a:Foo",
	}, &models.Principal{Username: "u"})

	rec := httptest.NewRecorder()
	resp.WriteResponse(rec, runtime.JSONProducer())
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	require.Contains(t, rec.Body.String(), "is not a valid class name")
}

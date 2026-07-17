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
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestUpsertIndex_SubmitLockKeyedOnQualifiedClass pins the PUT side of the shared
// submit lock: a namespaced short-name caller must lock on the qualified class,
// the key DeleteClassPropertyIndex uses. The test pre-holds that lock — a
// correctly-keyed handler blocks; the buggy raw-keyed one takes a different lock
// and proceeds.
//
// The lock must be acquired BEFORE the class read (which panics on the nil
// SchemaManager here); that ordering is what closes the DELETE race.
func TestUpsertIndex_SubmitLockKeyedOnQualifiedClass(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	locks := state.NewReindexSubmitLocks()
	h := &indexesHandlers{appState: &state.State{
		Authorizer:         &authorization.DummyAuthorizer{},
		ReindexSubmitLocks: locks,
		Logger:             logger,
		ServerConfig: &config.WeaviateConfig{Config: config.Config{
			Namespaces: config.Namespaces{Enabled: true},
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

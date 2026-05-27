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
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
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
		reindexSubmitLocks:  rec,
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

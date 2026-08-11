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

// Pins that updateIndex actually consults each refusal-arm helper and answers
// with its status, not just that the helpers work standalone.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// startedRepairFilterable is an in-flight task shaped like the one
// submitReindex would create, targeting prop.
func startedRepairFilterable(t *testing.T, id, prop string) *distributedtask.Task {
	t.Helper()
	return buildTask(t, id, distributedtask.TaskStatusStarted, db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeRepairFilterable,
		Collection:    "Movies",
		Properties:    []string{prop},
	}, nil)
}

// ladderHandlers is submissionHandlers with an idle prober: every refusal
// below must land before the fan-out probe runs.
func ladderHandlers(t *testing.T, svc *raceTaskService) *indexesHandlers {
	t.Helper()
	var busy atomic.Bool
	return submissionHandlers(t, svc, togglingProber{busy: &busy})
}

// An unparseable in-flight payload could conflict — racing the destructive
// pre-submit sweep against it isn't an option.
func TestUpdateIndexRefusesOnAnUnparseableInFlightPayload(t *testing.T) {
	svc := &raceTaskService{tasks: []*distributedtask.Task{{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "t-unreadable", Version: 3},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        []byte("{not json"),
	}}}
	h := ladderHandlers(t, svc)

	responder := submitReindex(h)

	unavailable, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
	require.Truef(t, ok, "a conflict that cannot be ruled out must be refused with 503, got %T", responder)
	msg := errorMessage(t, unavailable.Payload)
	require.Contains(t, msg, "t-unreadable", "the operator needs to know which task to inspect")
	require.Contains(t, msg, "unparseable payload")
	require.Zerof(t, svc.adds, "a migration was committed while a conflict could not be ruled out")
}

// A live task on the same (collection, property) tuple must answer 409: the
// two migrations would race each other's on-disk state.
func TestUpdateIndexRefusesAConflictingInFlightTask(t *testing.T) {
	svc := &raceTaskService{tasks: []*distributedtask.Task{
		startedRepairFilterable(t, "t-conflict", "title"),
	}}
	h := ladderHandlers(t, svc)

	responder := submitReindex(h)

	conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
	require.Truef(t, ok, "a conflicting in-flight migration must be refused with 409, got %T", responder)
	msg := errorMessage(t, conflict.Payload)
	require.Contains(t, msg, "t-conflict", "the operator needs to know which task conflicts")
	require.Contains(t, msg, "conflicts")
	require.Zerof(t, svc.adds, "a migration was committed over a conflicting one")
}

// At the per-collection cap, a non-conflicting submit must still be refused,
// and with 429 rather than any error the cluster's health could be blamed for.
func TestUpdateIndexRefusesPastThePerCollectionCap(t *testing.T) {
	var tasks []*distributedtask.Task
	for i := range maxConcurrentReindexPerCollection {
		tasks = append(tasks, startedRepairFilterable(t, fmt.Sprintf("t-cap-%d", i), fmt.Sprintf("p%d", i)))
	}
	svc := &raceTaskService{tasks: tasks}
	h := ladderHandlers(t, svc)

	responder := submitReindex(h)

	rec := httptest.NewRecorder()
	responder.WriteResponse(rec, runtime.JSONProducer())
	require.Equalf(t, http.StatusTooManyRequests, rec.Code,
		"the cap must answer 429, got %d: %s", rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `\"Movies\"`)
	require.Zerof(t, svc.adds, "a migration was committed past the per-collection cap")
}

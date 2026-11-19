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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// gateWithTasks builds the schema handler with just enough wiring for
// checkReindexConflictForPropertyMutation, the REST pre-flight for the
// schema FSM's mutation guard.
func gateWithTasks(tasks ...*distributedtask.Task) *schemaHandlers {
	return &schemaHandlers{
		metricRequestsTotal: newSchemaRequestsTotal(nil, logrus.New()),
		// allow-all authorizer: these tests are about the conflict gate, not authz
		authorizer: &authorization.DummyAuthorizer{},
		reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
			db.ReindexNamespace: tasks,
		}},
	}
}

// Pins: every non-terminal status blocks the mutation, including an
// unrecognized one.
func TestCheckReindexConflictForPropertyMutation_BlocksEveryInFlightStatus(t *testing.T) {
	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"title"},
	}

	for _, tc := range []struct {
		status  distributedtask.TaskStatus
		blocked bool
	}{
		{distributedtask.TaskStatusStarted, true},
		{distributedtask.TaskStatusPreparing, true},
		{distributedtask.TaskStatusSwapping, true},
		{unknownFutureStatus, true},
		{distributedtask.TaskStatusFinished, false},
		{distributedtask.TaskStatusFailed, false},
		{distributedtask.TaskStatusCancelled, false},
	} {
		t.Run(string(tc.status), func(t *testing.T) {
			h := gateWithTasks(buildTask(t, "T1", tc.status, payload, nil))
			conflict := h.checkReindexConflictForPropertyMutation(context.Background(), "C", "title", true)
			if !tc.blocked {
				require.Empty(t, conflict, "a task this build knows is done must not block")
				return
			}
			require.Contains(t, conflict, "T1")
			require.Contains(t, conflict, string(tc.status))
		})
	}
}

// Pins: the pre-flight's refusal ends on the same remedy as the FSM guard,
// and never advises a cancel where the cancel verb itself would 409.
func TestCheckReindexConflictForPropertyMutation_EndsOnTheFSMGuardsRemedy(t *testing.T) {
	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"title"},
	}
	refusals := func(status distributedtask.TaskStatus) (string, error) {
		task := buildTask(t, "T1", status, payload, nil)
		return gateWithTasks(task).
				checkReindexConflictForPropertyMutation(context.Background(), "C", "title", true),
			db.ReindexDetector{}.
				CheckPropertyUpdate("C", "title", []*distributedtask.Task{task})
	}

	// The remedy for the cancellable statuses; elsewhere it must be gone,
	// not merely qualified.
	cancellable, _ := refusals(distributedtask.TaskStatusStarted)
	cancellableRemedy := remedyOf(t, cancellable)

	for _, tc := range []struct {
		status     distributedtask.TaskStatus
		wantCancel bool
	}{
		{distributedtask.TaskStatusStarted, true},
		{distributedtask.TaskStatusPreparing, true},
		{distributedtask.TaskStatusSwapping, false},
		{unknownFutureStatus, false},
	} {
		t.Run(string(tc.status), func(t *testing.T) {
			restReason, fsmErr := refusals(tc.status)
			require.NotEmpty(t, restReason)
			require.Error(t, fsmErr)

			remedy := remedyOf(t, restReason)
			require.Equal(t, remedyOf(t, fsmErr.Error()), remedy,
				"the two surfaces must end on the same sentence")

			if tc.wantCancel {
				require.Contains(t, remedy, "cancel")
				return
			}
			require.NotContains(t, remedy, "cancel",
				"the cancel verb does nothing for %q, so the closing advice must not "+
					"send the operator to it", tc.status)
			require.NotContains(t, restReason, cancellableRemedy,
				"the cancel advice has to be gone, not joined by a caveat")
		})
	}
}

// remedyOf returns the closing advice of a mutation refusal: everything from
// its last em-dash on. A non-cancellable remedy carries an em-dash of its own,
// so this is the clause the operator acts on, not the whole remedy.
func remedyOf(t *testing.T, refusal string) string {
	t.Helper()
	i := strings.LastIndex(refusal, "— ")
	require.NotEqual(t, -1, i, "refusal %q has no remedy", refusal)
	return refusal[i:]
}

// Pins: the pre-flight and the FSM guard reach the same verdict for
// every structural payload class. The two arms that never look at the
// mutated collection — a payload that does not decode, and one that
// decodes without a Collection — are the ones this side used to skip,
// which produced exactly the ok-then-FAILED two-step the pre-flight
// exists to prevent.
func TestCheckReindexConflictForPropertyMutation_AgreesWithTheFSMGuard(t *testing.T) {
	for _, tc := range []struct {
		name        string
		raw         string
		wantBlocked bool
	}{
		{
			name:        "well-formed, another collection",
			raw:         `{"collection":"Other","migrationType":"change-tokenization","properties":["title"]}`,
			wantBlocked: false,
		},
		{
			name:        "well-formed, this collection",
			raw:         `{"collection":"C","migrationType":"change-tokenization","properties":["title"]}`,
			wantBlocked: true,
		},
		{
			name:        "type error, names another collection",
			raw:         `{"collection":"Other","properties":[1,2]}`,
			wantBlocked: true,
		},
		{
			name:        "type error, names this collection",
			raw:         `{"collection":"C","properties":[1,2]}`,
			wantBlocked: true,
		},
		{name: "syntax error", raw: `{"collection":"C",`, wantBlocked: true},
		{name: "not json", raw: `not json`, wantBlocked: true},
		// Decodes with no error at all and leaves every field zero.
		{name: "literal null", raw: `null`, wantBlocked: true},
		{
			name:        "decodes without a collection",
			raw:         `{"migrationType":"change-tokenization","properties":["title"]}`,
			wantBlocked: true,
		},
		{
			// Decodes cleanly and names another collection, so every
			// later arm would wave it through — but half a payload is
			// not enough to prove the task is unrelated, so both guards
			// refuse on every collection.
			name:        "decodes without a migration type, names another collection",
			raw:         `{"collection":"Other","properties":["title"]}`,
			wantBlocked: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := &distributedtask.Task{
				Namespace:      db.ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_bad", Version: 1},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        []byte(tc.raw),
			}

			restReason := gateWithTasks(task).
				checkReindexConflictForPropertyMutation(context.Background(), "C", "title", true)
			fsmErr := db.ReindexDetector{}.
				CheckPropertyUpdate("C", "title", []*distributedtask.Task{task})

			require.Equal(t, tc.wantBlocked, restReason != "",
				"REST pre-flight verdict (reason=%q)", restReason)
			require.Equal(t, tc.wantBlocked, fsmErr != nil,
				"FSM guard verdict (err=%v)", fsmErr)
		})
	}
}

// Pins: an empty Properties list means "all properties".
func TestCheckReindexConflictForPropertyMutation_EmptyPropertiesMatchesAnyProperty(t *testing.T) {
	h := gateWithTasks(buildTask(t, "T_all", distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeChangeTokenization,
			Collection:    "C",
		}, nil))

	require.Contains(t,
		h.checkReindexConflictForPropertyMutation(context.Background(), "C", "any-property", true),
		"T_all")
}

// Pins: the refusal an operator reads on
// DELETE /v1/schema/{class}/properties/{prop}/index/{name} names a cancel
// only when the cancel verb would accept one for that task.
//
// Driven through the handler because this is where the two meet: a
// non-empty conflict short-circuits to 422 before the request reaches
// RAFT, so the FSM guard's wording never reaches this journey, and the
// cancel it recommends is the one the same operator issues next.
func TestDeleteClassPropertyIndex_NamesACancelOnlyWhereTheCancelVerbAcceptsOne(t *testing.T) {
	const cancelAdvice = "cancel it via the reindex REST API before retrying"

	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "C",
		Properties:    []string{"title"},
	}

	for _, tc := range []struct {
		status             distributedtask.TaskStatus
		wantCancelAccepted bool
		// cancelCode is what a cancel answers where it is not accepted.
		cancelCode int
	}{
		{status: distributedtask.TaskStatusStarted, wantCancelAccepted: true},
		{status: distributedtask.TaskStatusPreparing, wantCancelAccepted: true},
		{status: distributedtask.TaskStatusSwapping, cancelCode: http.StatusConflict},
		{status: unknownFutureStatus, cancelCode: http.StatusConflict},
	} {
		t.Run(string(tc.status), func(t *testing.T) {
			task := buildTask(t, "T1", tc.status, payload, nil)

			rec := httptest.NewRecorder()
			gateWithTasks(task).deleteClassPropertyIndex(schema.SchemaObjectsPropertiesDeleteParams{
				HTTPRequest: httptest.NewRequest(http.MethodDelete,
					"/v1/schema/C/properties/title/index/searchable", nil),
				ClassName:    "C",
				PropertyName: "title",
				IndexName:    "searchable",
			}, nil).WriteResponse(rec, runtime.JSONProducer())

			require.Equal(t, http.StatusUnprocessableEntity, rec.Code,
				"an in-flight task blocks the DELETE before it reaches RAFT")
			body := rec.Body.String()
			require.Contains(t, body, "T1", "the refusal must name the task that blocks it")

			// What the cancel this body may or may not name would answer.
			logger, _ := logrustest.NewNullLogger()
			target, _ := findCancelTarget(
				[]*distributedtask.Task{task}, "C", "title", "searchable", logger)
			cancelResp := (&indexesHandlers{appState: &state.State{Logger: logger}}).
				cancelPreflight(target, "C", "title", "searchable", nil)

			require.Equal(t, tc.wantCancelAccepted, cancelResp == nil,
				"cancel verb reachability for %q", tc.status)

			if tc.wantCancelAccepted {
				require.Contains(t, body, cancelAdvice)
				return
			}

			cancelRec := httptest.NewRecorder()
			cancelResp.WriteResponse(cancelRec, runtime.JSONProducer())
			require.Equal(t, tc.cancelCode, cancelRec.Code)
			require.NotContains(t, body, cancelAdvice,
				"a cancel cannot stop the task in %q, so the refusal must not recommend one", tc.status)
		})
	}
}

// Pins: a task on a different property does not block the mutation.
func TestCheckReindexConflictForPropertyMutation_DifferentPropertyAllows(t *testing.T) {
	h := gateWithTasks(buildTask(t, "T1", distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeChangeTokenization,
			Collection:    "C",
			Properties:    []string{"other"},
		}, nil))

	require.Empty(t, h.checkReindexConflictForPropertyMutation(context.Background(), "C", "title", true))
}

// Pins that an index DELETE is refused, naming the resubmit, while a partial run stands on the property.
func TestIndexDeleteRefusedWhileAPartialRunStands(t *testing.T) {
	run := func(id string, version uint64, status distributedtask.TaskStatus, mt db.ReindexMigrationType, decided bool) *distributedtask.Task {
		task := buildTask(t, id, status, db.ReindexTaskPayload{
			MigrationType: mt, Collection: "C", Properties: []string{"title"}, TargetTokenization: models.PropertyTokenizationField,
		}, map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}})
		task.Version = version
		if decided {
			task.SwapDecidedGroups = map[string]time.Time{"a": {}}
		}
		return task
	}
	cancelled, finished := distributedtask.TaskStatusCancelled, distributedtask.TaskStatusFinished
	for _, tc := range []struct {
		name     string
		property string
		tasks    []*distributedtask.Task
		resubmit string // empty: not refused
	}{
		{
			name: "a partial enable-rangeable on the property", property: "title",
			tasks:    []*distributedtask.Task{run("T1", 1, cancelled, db.ReindexTypeEnableRangeable, true)},
			resubmit: `PUT /v1/schema/C/properties/title/index/rangeFilters -d '{}'`,
		},
		{
			name: "an older partial run on another index type than a newer, covered one", property: "title",
			tasks: []*distributedtask.Task{
				run("P1", 10, cancelled, db.ReindexTypeChangeTokenizationFilterable, true),
				run("P2", 20, cancelled, db.ReindexTypeChangeAlgorithm, true),
				run("F", 30, finished, db.ReindexTypeChangeAlgorithm, false),
			},
			resubmit: `PUT /v1/schema/C/properties/title/index/filterable -d '{"tokenization":"field"}'`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			preflight := gateWithTasks(tc.tasks...).checkReindexConflictForPropertyMutation(context.Background(), "C", tc.property, true)
			withoutCounts := gateWithTasks(tc.tasks...).checkReindexConflictForPropertyMutation(context.Background(), "C", tc.property, false)
			applied := db.ReindexDetector{}.CheckPropertyUpdate("C", tc.property, tc.tasks)
			if tc.resubmit == "" {
				require.Empty(t, preflight)
				require.NoError(t, applied)
				return
			}
			require.Contains(t, preflight, "1 of 2 tenants")
			require.Contains(t, preflight, tc.resubmit)
			require.Error(t, applied)
			require.Contains(t, withoutCounts, tc.resubmit)
			require.Equal(t, withoutCounts, applied.Error(), "both checks give the one rule's answer")
		})
	}
}

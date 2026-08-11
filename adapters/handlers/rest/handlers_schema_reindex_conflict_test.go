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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// gateWithTasks builds the schema handler with just enough wiring for
// checkReindexConflictForPropertyMutation, the REST pre-flight for the
// schema FSM's mutation guard.
func gateWithTasks(tasks ...*distributedtask.Task) *schemaHandlers {
	return &schemaHandlers{
		metricRequestsTotal: newSchemaRequestsTotal(nil, logrus.New()),
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
			conflict := h.checkReindexConflictForPropertyMutation(context.Background(), "C", "title")
			if !tc.blocked {
				require.Empty(t, conflict, "a task this build knows is done must not block")
				return
			}
			require.Contains(t, conflict, "T1")
			require.Contains(t, conflict, string(tc.status))
		})
	}
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
				checkReindexConflictForPropertyMutation(context.Background(), "C", "title")
			fsmErr := (&db.ReindexProvider{}).
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
		h.checkReindexConflictForPropertyMutation(context.Background(), "C", "any-property"),
		"T_all")
}

// Pins: a task on a different property does not block the mutation.
func TestCheckReindexConflictForPropertyMutation_DifferentPropertyAllows(t *testing.T) {
	h := gateWithTasks(buildTask(t, "T1", distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeChangeTokenization,
			Collection:    "C",
			Properties:    []string{"other"},
		}, nil))

	require.Empty(t, h.checkReindexConflictForPropertyMutation(context.Background(), "C", "title"))
}

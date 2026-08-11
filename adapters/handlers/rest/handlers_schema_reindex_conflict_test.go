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

// Pins: an undecodable payload is a hard reject, but scoped to its own
// collection.
func TestCheckReindexConflictForPropertyMutation_UndecodablePayloadIsScopedToItsCollection(t *testing.T) {
	for _, tc := range []struct {
		name        string
		raw         string
		wantBlocked bool
	}{
		{
			name:        "names another collection before failing",
			raw:         `{"collection":"Other","properties":[1,2]}`,
			wantBlocked: false,
		},
		{
			name:        "names this collection before failing",
			raw:         `{"collection":"C","properties":[1,2]}`,
			wantBlocked: true,
		},
		{
			name:        "names no collection at all",
			raw:         `not json`,
			wantBlocked: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := gateWithTasks(&distributedtask.Task{
				Namespace:      db.ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_bad", Version: 1},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        []byte(tc.raw),
			})

			conflict := h.checkReindexConflictForPropertyMutation(context.Background(), "C", "title")
			if !tc.wantBlocked {
				require.Empty(t, conflict,
					"an undecodable task on another collection must not block mutations on C")
				return
			}
			require.Contains(t, conflict, "unparseable payload")
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

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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	api "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
)

// TestFindCancelTargetTask_IsActive pins that cancel matches every in-flight
// status (STARTED/PREPARING/SWAPPING), not just STARTED.
func TestFindCancelTargetTask_IsActive(t *testing.T) {
	mk := func(status distributedtask.TaskStatus) *distributedtask.Task {
		return activeReindexTask("C:enable-filterable:p:aaaa", "C",
			db.ReindexTypeEnableFilterable, "", status, "p")
	}

	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
	} {
		t.Run(status.String()+" is a cancel target", func(t *testing.T) {
			target, payload := findCancelTargetTask(
				[]*distributedtask.Task{mk(status)}, "C", "p", "filterable")
			require.NotNil(t, target, "%s task must be a cancel target", status)
			assert.Equal(t, "C:enable-filterable:p:aaaa", target.ID)
			assert.Equal(t, db.ReindexTypeEnableFilterable, payload.MigrationType)
		})
	}

	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusFinished,
		distributedtask.TaskStatusFailed,
		distributedtask.TaskStatusCancelled,
	} {
		t.Run(status.String()+" is NOT a cancel target", func(t *testing.T) {
			target, _ := findCancelTargetTask(
				[]*distributedtask.Task{mk(status)}, "C", "p", "filterable")
			require.Nil(t, target, "terminal %s task must not be a cancel target", status)
		})
	}

	t.Run("wrong index type is not a target", func(t *testing.T) {
		target, _ := findCancelTargetTask(
			[]*distributedtask.Task{mk(distributedtask.TaskStatusStarted)}, "C", "p", "searchable")
		require.Nil(t, target, "enable-filterable does not target the searchable index")
	})
}

// realFSMCanceller adapts a real distributedtask.Manager to
// reindexTaskCanceller so the cancel handler's error mapping runs against
// genuine FSM rejections — no RAFT stack, but the same ErrTaskNotRunning
// the wire path rehydrates.
type realFSMCanceller struct{ mgr *distributedtask.Manager }

func (c realFSMCanceller) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return c.mgr.ListDistributedTasks(ctx)
}

func (c realFSMCanceller) CancelDistributedTask(_ context.Context, namespace, taskID string, version uint64) error {
	sub, err := json.Marshal(&api.CancelDistributedTaskRequest{
		Namespace:             namespace,
		Id:                    taskID,
		Version:               version,
		CancelledAtUnixMillis: time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return c.mgr.CancelTask(&api.ApplyRequest{SubCommand: sub})
}

// seedReindexTask adds a reindex task to mgr and drives it to target through
// the real FSM: PREPARING via the barrier path, SWAPPING via the no-barrier
// path, STARTED left un-completed.
func seedReindexTask(t *testing.T, mgr *distributedtask.Manager, id, collection, property string,
	mt db.ReindexMigrationType, target distributedtask.TaskStatus,
) {
	t.Helper()
	payload, err := json.Marshal(db.ReindexTaskPayload{
		Collection: collection, MigrationType: mt, Properties: []string{property},
	})
	require.NoError(t, err)

	addSub, err := json.Marshal(&api.AddDistributedTaskRequest{
		Namespace:               db.ReindexNamespace,
		Id:                      id,
		Payload:                 payload,
		SubmittedAtUnixMillis:   time.Now().UnixMilli(),
		UnitIds:                 []string{"u-1"},
		NeedsPreparationBarrier: target == distributedtask.TaskStatusPreparing,
	})
	require.NoError(t, err)
	require.NoError(t, mgr.AddTask(&api.ApplyRequest{SubCommand: addSub}, 1))

	if target == distributedtask.TaskStatusStarted {
		return
	}

	compSub, err := json.Marshal(&api.RecordDistributedTaskUnitCompletionRequest{
		Namespace:            db.ReindexNamespace,
		Id:                   id,
		Version:              1,
		NodeId:               "node-1",
		UnitId:               "u-1",
		FinishedAtUnixMillis: time.Now().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, mgr.RecordUnitCompletion(&api.ApplyRequest{SubCommand: compSub}))

	tasks, err := mgr.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	require.Equal(t, target, tasks[db.ReindexNamespace][0].Status, "FSM must reach %s", target)
}

// cancelResponse renders a cancel responder and returns (code, Status).
func cancelResponse(t *testing.T, resp middleware.Responder) (int, string) {
	t.Helper()
	require.NotNil(t, resp)
	rec := httptest.NewRecorder()
	resp.WriteResponse(rec, runtime.JSONProducer())
	var body models.IndexUpdateResponse
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	}
	return rec.Code, body.Status
}

// TestCancelReindexTask_NonStartedTaskIsNoOp pins M1: cancelling a task that
// reached PREPARING/SWAPPING (units done, mid-swap) round-trips through the
// real FSM, which rejects with ErrTaskNotRunning, and the handler maps that
// to an idempotent 202 NO_OP — not a 500. Regression guard for SF-2, which
// widened findCancelTargetTask to select PREPARING/SWAPPING targets that the
// FSM's STARTED-only CancelTask then rejects.
func TestCancelReindexTask_NonStartedTaskIsNoOp(t *testing.T) {
	principal := &models.Principal{Username: "tester"}
	for _, tc := range []struct {
		name   string
		target distributedtask.TaskStatus
	}{
		{"SWAPPING is NO_OP", distributedtask.TaskStatusSwapping},
		{"PREPARING is NO_OP", distributedtask.TaskStatusPreparing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := distributedtask.NewManager(distributedtask.ManagerParameters{Logger: logrus.New()})
			seedReindexTask(t, mgr, "C:enable-filterable:p:aaaa", "C", "p",
				db.ReindexTypeEnableFilterable, tc.target)

			h := &indexesHandlers{appState: &state.State{Logger: logrus.New()}}
			resp := h.cancelReindexTask(context.Background(), realFSMCanceller{mgr},
				"C", "p", "filterable", principal)

			code, status := cancelResponse(t, resp)
			require.Equal(t, http.StatusAccepted, code, "non-STARTED cancel must be 202, not 500")
			assert.Equal(t, reindexCancelStatusNoOp, status)
		})
	}
}

// TestCancelReindexTask_StartedTaskCancels pins the happy path stays intact: a
// STARTED task cancels through the real FSM and returns 202 CANCELLED.
func TestCancelReindexTask_StartedTaskCancels(t *testing.T) {
	mgr := distributedtask.NewManager(distributedtask.ManagerParameters{Logger: logrus.New()})
	seedReindexTask(t, mgr, "C:enable-filterable:p:aaaa", "C", "p",
		db.ReindexTypeEnableFilterable, distributedtask.TaskStatusStarted)

	h := &indexesHandlers{appState: &state.State{Logger: logrus.New()}}
	resp := h.cancelReindexTask(context.Background(), realFSMCanceller{mgr},
		"C", "p", "filterable", &models.Principal{Username: "tester"})

	code, status := cancelResponse(t, resp)
	require.Equal(t, http.StatusAccepted, code)
	assert.Equal(t, "CANCELLED", status)
}

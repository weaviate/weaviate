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
	dbreindex "github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	api "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	reindexusecase "github.com/weaviate/weaviate/usecases/reindex"
)

// TestFindCancelTargetTask_IsActive pins that cancel matches every in-flight
// status (STARTED/PREPARING/SWAPPING), not just STARTED.
func TestFindCancelTargetTask_IsActive(t *testing.T) {
	mk := func(status distributedtask.TaskStatus) *distributedtask.Task {
		return activeReindexTask("C:enable-filterable:p:aaaa", "C",
			dbreindex.ReindexTypeEnableFilterable, "", status, "p")
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
			assert.Equal(t, dbreindex.ReindexTypeEnableFilterable, payload.MigrationType)
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
// [reindexusecase.ClusterService] so the cancel path's error mapping runs
// against genuine FSM rejections (ErrTaskNotRunning) without a RAFT stack.
type realFSMCanceller struct{ mgr *distributedtask.Manager }

// The submit half of the interface is unreachable from the cancel path.
func (c realFSMCanceller) AddDistributedTaskWithBarrier(context.Context, string, string, any, []string, bool) error {
	return nil
}

func (c realFSMCanceller) AddDistributedTaskWithGroupsBarrier(context.Context, string, string, any, []distributedtask.UnitSpec, bool) error {
	return nil
}

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

// seedReindexTask drives a task through the real FSM to target: PREPARING
// via the barrier path, SWAPPING via the no-barrier path, STARTED as-is.
func seedReindexTask(t *testing.T, mgr *distributedtask.Manager, id, collection, property string,
	mt dbreindex.ReindexMigrationType, target distributedtask.TaskStatus,
) {
	t.Helper()
	payload, err := json.Marshal(dbreindex.ReindexTaskPayload{
		Collection: collection, MigrationType: mt, Properties: []string{property},
	})
	require.NoError(t, err)

	addSub, err := json.Marshal(&api.AddDistributedTaskRequest{
		Namespace:               dbreindex.ReindexNamespace,
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
		Namespace:            dbreindex.ReindexNamespace,
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
	require.Equal(t, target, tasks[dbreindex.ReindexNamespace][0].Status, "FSM must reach %s", target)
}

// cancelHandler wires an indexesHandlers whose reindex service cancels
// against the real FSM in mgr.
func cancelHandler(mgr *distributedtask.Manager) *indexesHandlers {
	logger := logrus.New()
	return &indexesHandlers{
		appState: &state.State{
			Logger: logger,
			ReindexService: reindexusecase.New(reindexusecase.Deps{
				Cluster: realFSMCanceller{mgr},
			}, logger),
		},
		metricRequestsTotal: newIndexesRequestsTotal(nil, logger),
	}
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

// TestCancelReindexTask_NonStartedTaskIsNoOp pins that a PREPARING/SWAPPING
// target rejected by the FSM (ErrTaskNotRunning) maps to 202 NO_OP, not 500.
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
				dbreindex.ReindexTypeEnableFilterable, tc.target)

			h := cancelHandler(mgr)
			resp := h.cancelReindexTask(context.Background(),
				"C", "p", "filterable", principal)

			code, status := cancelResponse(t, resp)
			require.Equal(t, http.StatusAccepted, code, "non-STARTED cancel must be 202, not 500")
			assert.Equal(t, reindexusecase.StatusNoOp, status)
		})
	}
}

// TestCancelReindexTask_StartedTaskCancels pins the happy path: a STARTED
// task cancels through the real FSM and returns 202 CANCELLED.
func TestCancelReindexTask_StartedTaskCancels(t *testing.T) {
	mgr := distributedtask.NewManager(distributedtask.ManagerParameters{Logger: logrus.New()})
	seedReindexTask(t, mgr, "C:enable-filterable:p:aaaa", "C", "p",
		dbreindex.ReindexTypeEnableFilterable, distributedtask.TaskStatusStarted)

	h := cancelHandler(mgr)
	resp := h.cancelReindexTask(context.Background(),
		"C", "p", "filterable", &models.Principal{Username: "tester"})

	code, status := cancelResponse(t, resp)
	require.Equal(t, http.StatusAccepted, code)
	assert.Equal(t, "CANCELLED", status)
}

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

package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/runtime/middleware"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/replication"
	rebalance "github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

type authorizeCall struct {
	verb      string
	resources []string
}

// fakeRebalanceAuthorizer implements only Authorize. Any other method call
// panics on the embedded nil Authorizer.
type fakeRebalanceAuthorizer struct {
	authorization.Authorizer
	err   error
	calls []authorizeCall
}

func (a *fakeRebalanceAuthorizer) Authorize(_ context.Context, _ *models.Principal, verb string, resources ...string) error {
	a.calls = append(a.calls, authorizeCall{verb: verb, resources: resources})
	return a.err
}

// fakeRebalanceManager keeps the local and leader views of the plan apart, so a
// test can tell which read a handler used.
type fakeRebalanceManager struct {
	plan       *rebalance.Plan
	leaderPlan *rebalance.Plan
	draftTasks []rebalance.DraftTask
	leaderErr  error
	createErr  error
	stopErr    error
	deleteErr  error

	// calls counts every manager call. localReads counts ReadRebalancePlan
	// calls. leaderReads counts QueryRebalancePlan calls. draftReads counts
	// RebalanceDraftTasks calls.
	calls       int
	localReads  int
	leaderReads int
	draftReads  int
	created     *rebalance.CreatePlanRequest
	stopped     []string
	deleted     []string
}

func (m *fakeRebalanceManager) CreateRebalancePlan(_ context.Context, req *rebalance.CreatePlanRequest) error {
	m.calls++
	m.created = req
	return m.createErr
}

func (m *fakeRebalanceManager) StopRebalancePlan(_ context.Context, req *rebalance.StopPlanRequest) error {
	m.calls++
	m.stopped = append(m.stopped, req.PlanID)
	return m.stopErr
}

func (m *fakeRebalanceManager) DeleteRebalancePlan(_ context.Context, req *rebalance.DeletePlanRequest) error {
	m.calls++
	m.deleted = append(m.deleted, req.PlanID)
	return m.deleteErr
}

func (m *fakeRebalanceManager) ReadRebalancePlan() (rebalance.Plan, bool) {
	m.calls++
	m.localReads++
	if m.plan == nil {
		return rebalance.Plan{}, false
	}
	return *m.plan, true
}

func (m *fakeRebalanceManager) QueryRebalancePlan(context.Context) (*rebalance.Plan, error) {
	m.calls++
	m.leaderReads++
	if m.leaderErr != nil {
		return nil, m.leaderErr
	}
	return m.leaderPlan, nil
}

func (m *fakeRebalanceManager) RebalanceDraftTasks() []rebalance.DraftTask {
	m.calls++
	m.draftReads++
	return m.draftTasks
}

type fakeRebalanceNodes struct {
	live       []string
	nonStorage []string
}

func (n fakeRebalanceNodes) StorageCandidates() []string { return n.live }
func (n fakeRebalanceNodes) NonStorageNodes() []string   { return n.nonStorage }

func createRebalanceHandlerWithFakes(t *testing.T, movementEnabled, rebalanceEnabled bool, manager *fakeRebalanceManager, nodes fakeRebalanceNodes,
) (*replicationHandler, *fakeRebalanceAuthorizer, *logrustest.Hook) {
	t.Helper()
	logger, hook := logrustest.NewNullLogger()
	authorizer := &fakeRebalanceAuthorizer{}
	handler := &replicationHandler{
		authorizer:       authorizer,
		rebalanceManager: manager,
		nodes:            nodes,
		logger:           logger,
		enabled:          runtime.NewDynamicValue(movementEnabled),
		rebalanceEnabled: runtime.NewDynamicValue(rebalanceEnabled),
	}
	return handler, authorizer, hook
}

// wrapped wraps sentinel the same way cluster.Raft does.
func wrapped(sentinel error) error {
	return fmt.Errorf("execute rebalance command: %w", sentinel)
}

func requireRebalanceLog(t *testing.T, hook *logrustest.Hook, planID string) {
	t.Helper()
	entry := hook.LastEntry()
	require.NotNil(t, entry)
	assert.Equal(t, "replication_rebalance", entry.Data["action"])
	assert.Equal(t, planID, entry.Data["planId"])
}

func requireGetPlanID(planID string) func(t *testing.T, response middleware.Responder, _ *logrustest.Hook) {
	return func(t *testing.T, response middleware.Responder, _ *logrustest.Hook) {
		t.Helper()
		assert.Equal(t, planID, response.(*replication.GetRebalancePlanOK).Payload.ID)
	}
}

func TestRebalanceHandlers(t *testing.T) {
	principal := &models.Principal{}
	request := &http.Request{}
	allReplications := []string{authorization.Replications("*", "*")}

	type operation func(h *replicationHandler) middleware.Responder
	create := func(body *models.ReplicationRebalanceRequest) operation {
		return func(h *replicationHandler) middleware.Responder {
			return h.createRebalancePlan(replication.CreateRebalancePlanParams{HTTPRequest: request, Body: body}, principal)
		}
	}
	get := func(h *replicationHandler) middleware.Responder {
		return h.getRebalancePlan(replication.GetRebalancePlanParams{HTTPRequest: request}, principal)
	}
	getWithConsistency := func(consistency bool) operation {
		return func(h *replicationHandler) middleware.Responder {
			return h.getRebalancePlan(replication.GetRebalancePlanParams{HTTPRequest: request, Consistency: &consistency}, principal)
		}
	}
	stop := func(h *replicationHandler) middleware.Responder {
		return h.stopRebalancePlan(replication.StopRebalancePlanParams{HTTPRequest: request}, principal)
	}
	approve := func(h *replicationHandler) middleware.Responder {
		return h.approveRebalancePlan(replication.ApproveRebalancePlanParams{HTTPRequest: request}, principal)
	}
	del := func(h *replicationHandler) middleware.Responder {
		return h.deleteRebalancePlan(replication.DeleteRebalancePlanParams{HTTPRequest: request}, principal)
	}
	validBody := func() *models.ReplicationRebalanceRequest {
		return &models.ReplicationRebalanceRequest{
			SourceNodes: []string{"node-1"},
			TargetNodes: []string{"node-2"},
			Collections: []string{"Books"},
		}
	}
	liveNodes := fakeRebalanceNodes{live: []string{"node-1", "node-2", "node-3"}}

	t.Run("a denied caller gets 403 before any flag check or plan read", func(t *testing.T) {
		tests := []struct {
			name string
			call operation
			verb string
			want middleware.Responder
		}{
			{"create", create(validBody()), authorization.CREATE, &replication.CreateRebalancePlanForbidden{}},
			{"get", get, authorization.READ, &replication.GetRebalancePlanForbidden{}},
			{"get with consistency true", getWithConsistency(true), authorization.READ, &replication.GetRebalancePlanForbidden{}},
			{"stop", stop, authorization.UPDATE, &replication.StopRebalancePlanForbidden{}},
			{"approve", approve, authorization.CREATE, &replication.ApproveRebalancePlanForbidden{}},
			{"delete", del, authorization.DELETE, &replication.DeleteRebalancePlanForbidden{}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				manager := &fakeRebalanceManager{plan: &rebalance.Plan{ID: "plan-1", State: rebalance.PlanStatePlanning}}
				handler, authorizer, _ := createRebalanceHandlerWithFakes(t, false, false, manager, liveNodes)
				authorizer.err = errors.New("forbidden")

				response := tt.call(handler)

				assert.IsType(t, tt.want, response)
				assert.Equal(t, []authorizeCall{{verb: tt.verb, resources: allReplications}}, authorizer.calls)
				assert.Zero(t, manager.calls)
			})
		}
	})

	t.Run("create", func(t *testing.T) {
		tests := []struct {
			name              string
			movementDisabled  bool
			rebalanceDisabled bool
			body              *models.ReplicationRebalanceRequest
			nodes             *fakeRebalanceNodes
			createErr         error
			want              middleware.Responder
			wantErrors        []string
		}{
			{name: "rebalance flag off", rebalanceDisabled: true, body: validBody(), want: &replication.CreateRebalancePlanNotImplemented{}},
			{name: "movement flag off", movementDisabled: true, body: validBody(), want: &replication.CreateRebalancePlanNotImplemented{}},
			{name: "missing body", want: &replication.CreateRebalancePlanBadRequest{}},
			{
				name: "empty source nodes",
				body: &models.ReplicationRebalanceRequest{TargetNodes: []string{"node-2"}},
				want: &replication.CreateRebalancePlanBadRequest{},
			},
			{
				name: "empty target nodes",
				body: &models.ReplicationRebalanceRequest{SourceNodes: []string{"node-1"}},
				want: &replication.CreateRebalancePlanBadRequest{},
			},
			{
				name: "duplicate node",
				body: &models.ReplicationRebalanceRequest{SourceNodes: []string{"node-1", "node-1"}, TargetNodes: []string{"node-2"}},
				want: &replication.CreateRebalancePlanBadRequest{},
			},
			{
				name: "overlapping node groups",
				body: &models.ReplicationRebalanceRequest{SourceNodes: []string{"node-1"}, TargetNodes: []string{"node-2", "node-1"}},
				want: &replication.CreateRebalancePlanBadRequest{},
			},
			{
				name:       "nodes that are not live get one row each",
				body:       &models.ReplicationRebalanceRequest{SourceNodes: []string{"node-1", "gone-1"}, TargetNodes: []string{"gone-2"}},
				want:       &replication.CreateRebalancePlanUnprocessableEntity{},
				wantErrors: []string{`node "gone-1" is not a live storage node`, `node "gone-2" is not a live storage node`},
			},
			{
				name:       "non-storage node",
				body:       validBody(),
				nodes:      &fakeRebalanceNodes{live: []string{"node-1"}, nonStorage: []string{"node-2"}},
				want:       &replication.CreateRebalancePlanUnprocessableEntity{},
				wantErrors: []string{`node "node-2" is not a storage node`},
			},
			{
				name:      "unfinished plan exists",
				body:      validBody(),
				createErr: wrapped(rebalance.ErrPlanExists),
				want:      &replication.CreateRebalancePlanConflict{},
			},
			{
				name:      "unexpected proposal error",
				body:      validBody(),
				createErr: errors.New("raft unavailable"),
				want:      &replication.CreateRebalancePlanInternalServerError{},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				nodes := liveNodes
				if tt.nodes != nil {
					nodes = *tt.nodes
				}
				manager := &fakeRebalanceManager{createErr: tt.createErr}
				handler, _, _ := createRebalanceHandlerWithFakes(t, !tt.movementDisabled, !tt.rebalanceDisabled, manager, nodes)

				response := create(tt.body)(handler)

				require.IsType(t, tt.want, response)
				// Only the rows that set createErr pass validation and reach the manager.
				assert.Equal(t, tt.createErr != nil, manager.created != nil, "proposal made")
				if tt.wantErrors != nil {
					var got []string
					for _, item := range response.(*replication.CreateRebalancePlanUnprocessableEntity).Payload.Error {
						got = append(got, item.Message)
					}
					assert.Equal(t, tt.wantErrors, got)
				}
			})
		}
	})

	t.Run("create proposes a new plan and logs its id", func(t *testing.T) {
		manager := &fakeRebalanceManager{}
		handler, _, hook := createRebalanceHandlerWithFakes(t, true, true, manager, liveNodes)

		before := time.Now().UnixMilli()
		response := create(validBody())(handler)
		after := time.Now().UnixMilli()

		require.IsType(t, &replication.CreateRebalancePlanAccepted{}, response)
		require.NotNil(t, manager.created)
		_, err := uuid.Parse(manager.created.PlanID)
		require.NoError(t, err)
		assert.Equal(t, manager.created.PlanID, response.(*replication.CreateRebalancePlanAccepted).Payload.ID)
		assert.Equal(t, []string{"node-1"}, manager.created.SourceNodes)
		assert.Equal(t, []string{"node-2"}, manager.created.TargetNodes)
		assert.Equal(t, []string{"Books"}, manager.created.Collections)
		assert.GreaterOrEqual(t, manager.created.CreatedAtUnixMs, before)
		assert.LessOrEqual(t, manager.created.CreatedAtUnixMs, after)
		requireRebalanceLog(t, hook, manager.created.PlanID)
	})

	t.Run("approve answers 501 to a permitted caller", func(t *testing.T) {
		for _, enabled := range []bool{true, false} {
			t.Run(fmt.Sprintf("flags enabled %v", enabled), func(t *testing.T) {
				manager := &fakeRebalanceManager{}
				handler, authorizer, _ := createRebalanceHandlerWithFakes(t, enabled, enabled, manager, liveNodes)

				assert.IsType(t, &replication.ApproveRebalancePlanNotImplemented{}, approve(handler))
				assert.Equal(t, []authorizeCall{{verb: authorization.CREATE, resources: allReplications}}, authorizer.calls)
				assert.Zero(t, manager.calls)
			})
		}
	})

	t.Run("get, stop and delete ignore the flags", func(t *testing.T) {
		planning := &rebalance.Plan{ID: "plan-1", State: rebalance.PlanStatePlanning}
		proposed := &rebalance.Plan{ID: "plan-1", State: rebalance.PlanStateProposed}
		stale := &rebalance.Plan{ID: "plan-0", State: rebalance.PlanStateStopped}
		failed := &rebalance.Plan{
			ID:               "plan-1",
			State:            rebalance.PlanStatePlanningFailed,
			SourceNodes:      []string{"node-1"},
			TargetNodes:      []string{"node-2"},
			MeasuredAtUnixMs: 1700000000000,
			Reason:           "disk totals differ",
			Nodes: []types.RebalanceNodeMeasurement{{
				Node:                  "node-1",
				DiskTotalBytes:        1000,
				DiskAvailableBytes:    400,
				DiskWarningPercentage: 80,
				MemoryLimitBytes:      2048,
				ReplicationWorkers:    10,
			}, {Node: "node-2", Error: "measurement unavailable"}},
			Baseline: []rebalance.PlanNodeBalance{{Node: "node-1", DiskUtilisation: 0.75, ReplicaCount: 3}},
		}

		tests := []struct {
			name        string
			call        operation
			plan        *rebalance.Plan
			leaderPlan  *rebalance.Plan
			leaderErr   error
			stopErr     error
			deleteErr   error
			leaderRead  bool
			want        middleware.Responder
			wantStopped []string
			wantDeleted []string
			check       func(t *testing.T, response middleware.Responder, hook *logrustest.Hook)
		}{
			{name: "get reads the local state, not the leader's", call: get, leaderPlan: planning, want: &replication.GetRebalancePlanNotFound{}},
			{
				name: "get returns the plan with its measurements",
				call: get,
				plan: failed,
				want: &replication.GetRebalancePlanOK{},
				check: func(t *testing.T, response middleware.Responder, _ *logrustest.Hook) {
					assert.Equal(t, &models.ReplicationRebalancePlan{
						ID:               "plan-1",
						State:            models.ReplicationRebalancePlanStatePLANNINGFAILED,
						SourceNodes:      []string{"node-1"},
						TargetNodes:      []string{"node-2"},
						MeasuredAtUnixMs: 1700000000000,
						Reason:           "disk totals differ",
						Nodes: []*models.ReplicationRebalancePlanNodesItems0{{
							Node:                  "node-1",
							DiskTotalBytes:        1000,
							DiskAvailableBytes:    400,
							DiskWarningPercentage: 80,
							MemoryLimitBytes:      2048,
							ReplicationWorkers:    10,
						}, {Node: "node-2", Error: "measurement unavailable"}},
						Baseline: []*models.ReplicationRebalancePlanBaselineItems0{{Node: "node-1", DiskUtilisation: 0.75, ReplicaCount: 3}},
						Drafting: []*models.ReplicationRebalancePlanDraftingItems0{},
					}, response.(*replication.GetRebalancePlanOK).Payload)
				},
			},
			// In every consistency row, the local plan and the leader's plan differ.
			// Only a handler that reads the plan the consistency header selects gets
			// the expected response and plan ID.
			{
				name:       "get with consistency false reads the local state",
				call:       getWithConsistency(false),
				plan:       planning,
				leaderPlan: stale,
				want:       &replication.GetRebalancePlanOK{},
				check:      requireGetPlanID("plan-1"),
			},
			{
				name:       "get with consistency true reads the leader's state",
				call:       getWithConsistency(true),
				leaderRead: true,
				plan:       stale,
				leaderPlan: planning,
				want:       &replication.GetRebalancePlanOK{},
				check:      requireGetPlanID("plan-1"),
			},
			{name: "get with consistency true when the leader has no plan", call: getWithConsistency(true), plan: stale, leaderRead: true, want: &replication.GetRebalancePlanNotFound{}},
			{
				name:       "get with consistency true when the leader read fails",
				call:       getWithConsistency(true),
				leaderRead: true,
				plan:       planning,
				leaderErr:  errors.New("no leader"),
				want:       &replication.GetRebalancePlanInternalServerError{},
			},
			// In every stop and delete row, the local plan is missing or out of date.
			// Only a handler that reads the leader's plan gets the expected response
			// and plan ID.
			{name: "stop when the leader has no plan", call: stop, plan: planning, leaderRead: true, want: &replication.StopRebalancePlanNotFound{}},
			{name: "stop when the leader read fails", call: stop, plan: planning, leaderErr: errors.New("no leader"), leaderRead: true, want: &replication.StopRebalancePlanInternalServerError{}},
			{
				name:        "stop a planning plan",
				call:        stop,
				leaderRead:  true,
				plan:        stale,
				leaderPlan:  planning,
				want:        &replication.StopRebalancePlanAccepted{},
				wantStopped: []string{"plan-1"},
				check: func(t *testing.T, _ middleware.Responder, hook *logrustest.Hook) {
					requireRebalanceLog(t, hook, "plan-1")
				},
			},
			{
				name:        "stop a plan that is not planning",
				call:        stop,
				leaderRead:  true,
				leaderPlan:  proposed,
				stopErr:     wrapped(rebalance.ErrPlanTransition),
				want:        &replication.StopRebalancePlanConflict{},
				wantStopped: []string{"plan-1"},
			},
			{
				name:        "stop a plan deleted before the proposal applied",
				call:        stop,
				leaderRead:  true,
				leaderPlan:  planning,
				stopErr:     wrapped(rebalance.ErrPlanNotFound),
				want:        &replication.StopRebalancePlanNotFound{},
				wantStopped: []string{"plan-1"},
			},
			{
				name:        "stop with an unexpected proposal error",
				call:        stop,
				leaderRead:  true,
				leaderPlan:  planning,
				stopErr:     errors.New("raft unavailable"),
				want:        &replication.StopRebalancePlanInternalServerError{},
				wantStopped: []string{"plan-1"},
			},
			{name: "delete when the leader has no plan", call: del, plan: proposed, leaderRead: true, want: &replication.DeleteRebalancePlanNoContent{}},
			{name: "delete when the leader read fails", call: del, plan: proposed, leaderErr: errors.New("no leader"), leaderRead: true, want: &replication.DeleteRebalancePlanInternalServerError{}},
			{
				name:        "delete a planning plan",
				call:        del,
				leaderRead:  true,
				leaderPlan:  planning,
				deleteErr:   wrapped(rebalance.ErrPlanTransition),
				want:        &replication.DeleteRebalancePlanConflict{},
				wantDeleted: []string{"plan-1"},
			},
			{
				name:        "delete a proposed plan",
				call:        del,
				leaderRead:  true,
				plan:        stale,
				leaderPlan:  proposed,
				want:        &replication.DeleteRebalancePlanNoContent{},
				wantDeleted: []string{"plan-1"},
				check: func(t *testing.T, _ middleware.Responder, hook *logrustest.Hook) {
					requireRebalanceLog(t, hook, "plan-1")
				},
			},
			{
				name:        "delete a plan deleted before the proposal applied",
				call:        del,
				leaderRead:  true,
				leaderPlan:  proposed,
				deleteErr:   wrapped(rebalance.ErrPlanNotFound),
				want:        &replication.DeleteRebalancePlanNoContent{},
				wantDeleted: []string{"plan-1"},
			},
			{
				name:        "delete with an unexpected proposal error",
				call:        del,
				leaderRead:  true,
				leaderPlan:  proposed,
				deleteErr:   errors.New("raft unavailable"),
				want:        &replication.DeleteRebalancePlanInternalServerError{},
				wantDeleted: []string{"plan-1"},
			},
		}
		for _, enabled := range []bool{true, false} {
			for _, tt := range tests {
				t.Run(fmt.Sprintf("%s with flags enabled %v", tt.name, enabled), func(t *testing.T) {
					manager := &fakeRebalanceManager{
						plan:       tt.plan,
						leaderPlan: tt.leaderPlan,
						leaderErr:  tt.leaderErr,
						stopErr:    tt.stopErr,
						deleteErr:  tt.deleteErr,
					}
					handler, _, hook := createRebalanceHandlerWithFakes(t, enabled, enabled, manager, liveNodes)

					response := tt.call(handler)

					require.IsType(t, tt.want, response)
					if tt.leaderRead {
						assert.Equal(t, 1, manager.leaderReads, "leader reads")
						assert.Zero(t, manager.localReads, "local reads")
					} else {
						assert.Equal(t, 1, manager.localReads, "local reads")
						assert.Zero(t, manager.leaderReads, "leader reads")
					}
					assert.Equal(t, tt.wantStopped, manager.stopped)
					assert.Equal(t, tt.wantDeleted, manager.deleted)
					if tt.check != nil {
						tt.check(t, response, hook)
					}
				})
			}
		}
	})

	// Each read path puts the plan only in the store that path reads. A handler
	// that reads the wrong store finds no plan and returns 404.
	type readPath struct {
		name    string
		call    operation
		manager func(plan *rebalance.Plan, tasks []rebalance.DraftTask) *fakeRebalanceManager
	}
	readPaths := []readPath{
		{"local read", get, func(plan *rebalance.Plan, tasks []rebalance.DraftTask) *fakeRebalanceManager {
			return &fakeRebalanceManager{plan: plan, draftTasks: tasks}
		}},
		{"consistency true", getWithConsistency(true), func(plan *rebalance.Plan, tasks []rebalance.DraftTask) *fakeRebalanceManager {
			return &fakeRebalanceManager{leaderPlan: plan, draftTasks: tasks}
		}},
	}
	// Sources then targets give c, a, d, b, e: neither sorted nor reverse-sorted.
	listedPlan := func(state rebalance.PlanState) *rebalance.Plan {
		return &rebalance.Plan{
			ID:          "plan-1",
			State:       state,
			SourceNodes: []string{"node-c", "node-a"},
			TargetNodes: []string{"node-d", "node-b", "node-e"},
		}
	}
	allUnits := func(status rebalance.DraftUnitStatus) map[string]rebalance.DraftUnitStatus {
		return map[string]rebalance.DraftUnitStatus{
			"node-a": status, "node-b": status, "node-c": status, "node-d": status, "node-e": status,
		}
	}
	// node-e has no unit.
	current := rebalance.DraftTask{PlanID: "plan-1", Version: 2, Units: map[string]rebalance.DraftUnitStatus{
		"node-c": rebalance.DraftUnitCompleted,
		"node-a": rebalance.DraftUnitFailed,
		"node-d": rebalance.DraftUnitInProgress,
		"node-b": rebalance.DraftUnitPending,
	}}
	older := rebalance.DraftTask{PlanID: "plan-1", Version: 1, Terminal: true, Units: allUnits(rebalance.DraftUnitCompleted)}
	otherPlan := rebalance.DraftTask{PlanID: "plan-0", Version: 7, Units: allUnits(rebalance.DraftUnitCompleted)}

	t.Run("get renders per-node drafting status while planning", func(t *testing.T) {
		tests := []struct {
			name  string
			tasks []rebalance.DraftTask
		}{
			{"another plan's task is listed first", []rebalance.DraftTask{otherPlan, current}},
			{"the highest version wins over an older one listed after it", []rebalance.DraftTask{current, older}},
			{"the highest version wins over an older one listed before it", []rebalance.DraftTask{older, otherPlan, current}},
		}
		want := []*models.ReplicationRebalancePlanDraftingItems0{
			{Node: "node-c", Status: models.ReplicationRebalancePlanDraftingItems0StatusREPORTED},
			{Node: "node-a", Status: models.ReplicationRebalancePlanDraftingItems0StatusFAILED},
			{Node: "node-d", Status: models.ReplicationRebalancePlanDraftingItems0StatusDRAFTING},
			{Node: "node-b", Status: models.ReplicationRebalancePlanDraftingItems0StatusDRAFTING},
			{Node: "node-e", Status: models.ReplicationRebalancePlanDraftingItems0StatusDRAFTING},
		}
		for _, path := range readPaths {
			for _, tt := range tests {
				t.Run(fmt.Sprintf("%s on %s", tt.name, path.name), func(t *testing.T) {
					manager := path.manager(listedPlan(rebalance.PlanStatePlanning), tt.tasks)
					handler, _, _ := createRebalanceHandlerWithFakes(t, true, true, manager, liveNodes)

					response := path.call(handler)

					require.IsType(t, &replication.GetRebalancePlanOK{}, response)
					assert.Equal(t, want, response.(*replication.GetRebalancePlanOK).Payload.Drafting)
					assert.Equal(t, 1, manager.draftReads, "draft task reads")
				})
			}
		}
	})

	t.Run("get renders an empty drafting list outside drafting", func(t *testing.T) {
		populated := []rebalance.DraftTask{{PlanID: "plan-1", Version: 1, Units: allUnits(rebalance.DraftUnitCompleted)}}
		localRead := readPaths[:1]
		// Every non-PLANNING row holds the plan's draft task. Only the PLANNING
		// check in latestDraftTask keeps its draft reads at zero. One
		// non-PLANNING row also runs on consistency true, so both read paths
		// cover that check.
		tests := []struct {
			name       string
			state      rebalance.PlanState
			tasks      []rebalance.DraftTask
			paths      []readPath
			draftReads int
		}{
			{"proposed", rebalance.PlanStateProposed, populated, localRead, 0},
			{"not needed", rebalance.PlanStateNotNeeded, populated, localRead, 0},
			{"no acceptable plan", rebalance.PlanStateNoAcceptablePlan, populated, localRead, 0},
			{"planning failed", rebalance.PlanStatePlanningFailed, populated, localRead, 0},
			{"running", rebalance.PlanStateRunning, populated, localRead, 0},
			{"stopping", rebalance.PlanStateStopping, populated, localRead, 0},
			{"stopped", rebalance.PlanStateStopped, populated, localRead, 0},
			{"completed", rebalance.PlanStateCompleted, populated, readPaths, 0},
			{"failed", rebalance.PlanStateFailed, populated, localRead, 0},
			{"planning with no draft task", rebalance.PlanStatePlanning, nil, readPaths, 1},
			{"planning with only another plan's draft task", rebalance.PlanStatePlanning, []rebalance.DraftTask{otherPlan}, readPaths, 1},
		}
		for _, tt := range tests {
			for _, path := range tt.paths {
				t.Run(fmt.Sprintf("%s on %s", tt.name, path.name), func(t *testing.T) {
					manager := path.manager(listedPlan(tt.state), tt.tasks)
					handler, _, _ := createRebalanceHandlerWithFakes(t, true, true, manager, liveNodes)

					response := path.call(handler)

					require.IsType(t, &replication.GetRebalancePlanOK{}, response)
					body, err := json.Marshal(response.(*replication.GetRebalancePlanOK).Payload)
					require.NoError(t, err)
					var fields map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(body, &fields))
					assert.Equal(t, `[]`, string(fields["drafting"]))
					assert.Equal(t, tt.draftReads, manager.draftReads, "draft task reads")
				})
			}
		}
	})
}

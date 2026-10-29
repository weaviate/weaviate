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

package replication_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
)

const (
	planGaugeName = "weaviate_replication_rebalance_plans"
	seededPlanID  = "plan-1"
	// createdAtMs and the approval windows sit in 1970, so an apply that read the
	// local clock would see every PROPOSED plan as expired.
	createdAtMs int64 = 1_000
	// In the create matrix's PROPOSED row, the approval window is open by the
	// smallest margin.
	openApprovalMs int64  = createdAtMs + 1
	runTerm        uint64 = 5
)

var allPlanStates = []replication.PlanState{
	replication.PlanStatePlanning,
	replication.PlanStateProposed,
	replication.PlanStateNotNeeded,
	replication.PlanStateNoAcceptablePlan,
	replication.PlanStatePlanningFailed,
	replication.PlanStateRunning,
	replication.PlanStateStopping,
	replication.PlanStateStopped,
	replication.PlanStateCompleted,
	replication.PlanStateFailed,
}

var planningOutcomes = []replication.PlanState{
	replication.PlanStateProposed,
	replication.PlanStateNotNeeded,
	replication.PlanStateNoAcceptablePlan,
	replication.PlanStatePlanningFailed,
}

// planSnapshot mirrors the unexported snapshot struct's encoding.
type planSnapshot struct {
	Ops  map[replication.ShardReplicationOp]replication.ShardReplicationOpStatus
	Plan *replication.Plan `json:"plan,omitempty"`
}

func newPlanFSM() (*replication.ShardReplicationFSM, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	return replication.NewShardReplicationFSM(reg), reg
}

func seededPlan(state replication.PlanState) *replication.Plan {
	return &replication.Plan{
		ID:                  seededPlanID,
		State:               state,
		SourceNodes:         []string{"node-0"},
		TargetNodes:         []string{"node-1"},
		CreatedAtUnixMs:     createdAtMs - 500,
		ApproveBeforeUnixMs: openApprovalMs,
	}
}

// seedPlan restores a snapshot holding p and no ops; nil p seeds no plan.
func seedPlan(t *testing.T, fsm *replication.ShardReplicationFSM, p *replication.Plan) {
	t.Helper()
	b, err := json.Marshal(planSnapshot{Plan: p})
	require.NoError(t, err)
	require.NoError(t, fsm.Restore(b))
}

// requirePlanGauge asserts the gauge has one series, for state at value 1, or no
// series when state is empty.
func requirePlanGauge(t *testing.T, reg *prometheus.Registry, state replication.PlanState) {
	t.Helper()
	expected := ""
	if state != "" {
		expected = fmt.Sprintf(`
# HELP %[1]s Rebalance plan state: 1 for the current plan's state, no series when no plan exists
# TYPE %[1]s gauge
%[1]s{state=%[2]q} 1
`, planGaugeName, state)
	}
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), planGaugeName))
}

func createReq(id string) *replication.CreatePlanRequest {
	return &replication.CreatePlanRequest{
		PlanID:          id,
		SourceNodes:     []string{"node-2"},
		TargetNodes:     []string{"node-3"},
		Collections:     []string{"Articles"},
		CreatedAtUnixMs: createdAtMs,
	}
}

func completeReq(outcome replication.PlanState) *replication.CompletePlanningRequest {
	return &replication.CompletePlanningRequest{
		PlanID:              seededPlanID,
		RunID:               "run-a",
		RunTerm:             runTerm,
		Outcome:             outcome,
		Baseline:            []replication.PlanNodeBalance{{Node: "node-0", DiskUtilisation: 0.74, ReplicaCount: 412}},
		Goal:                []replication.PlanNodeBalance{{Node: "node-0", DiskUtilisation: 0.58, ReplicaCount: 318}},
		MoveCount:           94,
		PlanHash:            "sha256:9f1e",
		ApproveBeforeUnixMs: openApprovalMs,
	}
}

// matrixWant is the outcome of one command: an error, or the resulting plan
// state ("" for no plan).
type matrixWant struct {
	state replication.PlanState
	err   error
}

// runPlanCommand seeds start (nil for no plan), applies cmd and checks want. A
// refused command must leave the plan and gauge exactly as seeded.
func runPlanCommand(t *testing.T, start *replication.Plan, cmd func(*replication.ShardReplicationFSM) error, want matrixWant) {
	t.Helper()
	fsm, reg := newPlanFSM()
	seedPlan(t, fsm, start)
	before, hadBefore := fsm.GetPlan()

	err := cmd(fsm)

	got, ok := fsm.GetPlan()
	if want.err != nil {
		require.ErrorIs(t, err, want.err)
		require.Equal(t, hadBefore, ok)
		require.Equal(t, before, got)
		requirePlanGauge(t, reg, before.State)
		return
	}
	require.NoError(t, err)
	require.Equal(t, want.state != "", ok)
	require.Equal(t, want.state, got.State)
	requirePlanGauge(t, reg, want.state)
}

func startPlan(state replication.PlanState) *replication.Plan {
	if state == "" {
		return nil
	}
	return seededPlan(state)
}

func startName(state replication.PlanState) string {
	if state == "" {
		return "no plan"
	}
	return string(state)
}

func TestRebalancePlan_TransitionMatrix(t *testing.T) {
	starts := append([]replication.PlanState{""}, allPlanStates...)
	notFound := matrixWant{err: replication.ErrPlanNotFound}
	transition := matrixWant{err: replication.ErrPlanTransition}

	t.Run("create", func(t *testing.T) {
		exists := matrixWant{err: replication.ErrPlanExists}
		planning := matrixWant{state: replication.PlanStatePlanning}
		want := map[replication.PlanState]matrixWant{
			"":                                    planning,
			replication.PlanStatePlanning:         exists,
			replication.PlanStateProposed:         exists,
			replication.PlanStateNotNeeded:        planning,
			replication.PlanStateNoAcceptablePlan: planning,
			replication.PlanStatePlanningFailed:   planning,
			replication.PlanStateRunning:          exists,
			replication.PlanStateStopping:         exists,
			replication.PlanStateStopped:          planning,
			replication.PlanStateCompleted:        planning,
			replication.PlanStateFailed:           planning,
		}
		for _, start := range starts {
			t.Run(startName(start), func(t *testing.T) {
				runPlanCommand(t, startPlan(start), func(fsm *replication.ShardReplicationFSM) error {
					return fsm.CreatePlan(createReq("plan-2"))
				}, want[start])
			})
		}
	})

	t.Run("complete-planning", func(t *testing.T) {
		for _, outcome := range planningOutcomes {
			for _, start := range starts {
				var want matrixWant
				switch start {
				case "":
					want = notFound
				case replication.PlanStatePlanning:
					want = matrixWant{state: outcome}
				default:
					want = transition
				}
				t.Run(fmt.Sprintf("%s from %s", outcome, startName(start)), func(t *testing.T) {
					runPlanCommand(t, startPlan(start), func(fsm *replication.ShardReplicationFSM) error {
						return fsm.CompletePlanning(runTerm, completeReq(outcome))
					}, want)
				})
			}
		}
		for _, outcome := range allPlanStates {
			if slices.Contains(planningOutcomes, outcome) {
				continue
			}
			t.Run(fmt.Sprintf("invalid outcome %s from PLANNING", outcome), func(t *testing.T) {
				runPlanCommand(t, startPlan(replication.PlanStatePlanning), func(fsm *replication.ShardReplicationFSM) error {
					return fsm.CompletePlanning(runTerm, completeReq(outcome))
				}, transition)
			})
		}
	})

	t.Run("stop", func(t *testing.T) {
		for _, start := range starts {
			var want matrixWant
			switch start {
			case "":
				want = notFound
			case replication.PlanStatePlanning:
				want = matrixWant{state: replication.PlanStateStopped}
			default:
				want = transition
			}
			t.Run(startName(start), func(t *testing.T) {
				runPlanCommand(t, startPlan(start), func(fsm *replication.ShardReplicationFSM) error {
					return fsm.StopPlan(&replication.StopPlanRequest{PlanID: seededPlanID})
				}, want)
			})
		}
	})

	t.Run("delete", func(t *testing.T) {
		deleted := matrixWant{}
		want := map[replication.PlanState]matrixWant{
			"":                                    notFound,
			replication.PlanStatePlanning:         transition,
			replication.PlanStateProposed:         deleted,
			replication.PlanStateNotNeeded:        deleted,
			replication.PlanStateNoAcceptablePlan: deleted,
			replication.PlanStatePlanningFailed:   deleted,
			replication.PlanStateRunning:          transition,
			replication.PlanStateStopping:         transition,
			replication.PlanStateStopped:          deleted,
			replication.PlanStateCompleted:        deleted,
			replication.PlanStateFailed:           deleted,
		}
		for _, start := range starts {
			t.Run(startName(start), func(t *testing.T) {
				runPlanCommand(t, startPlan(start), func(fsm *replication.ShardReplicationFSM) error {
					return fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: seededPlanID})
				}, want[start])
			})
		}
	})

	t.Run("create replaces a finished plan, including PROPOSED past approveBefore at the command's timestamp", func(t *testing.T) {
		// The create matrix's PROPOSED row covers the refusal while the window is open.
		cases := []struct {
			name            string
			approveBeforeMs int64
		}{
			{name: "window closed before the command", approveBeforeMs: createdAtMs - 1},
			{name: "window closes at the command", approveBeforeMs: createdAtMs},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				fsm, reg := newPlanFSM()
				start := seededPlan(replication.PlanStateProposed)
				start.RunID, start.RunTerm, start.PlanHash = "run-a", runTerm, "sha256:9f1e"
				start.Baseline = []replication.PlanNodeBalance{{Node: "node-0", DiskUtilisation: 0.7, ReplicaCount: 4}}
				start.ApproveBeforeUnixMs = tc.approveBeforeMs
				seedPlan(t, fsm, start)

				require.NoError(t, fsm.CreatePlan(createReq("plan-2")))

				got, ok := fsm.GetPlan()
				require.True(t, ok)
				require.Equal(t, replication.Plan{
					ID:              "plan-2",
					State:           replication.PlanStatePlanning,
					SourceNodes:     []string{"node-2"},
					TargetNodes:     []string{"node-3"},
					Collections:     []string{"Articles"},
					CreatedAtUnixMs: createdAtMs,
				}, got)
				requirePlanGauge(t, reg, replication.PlanStatePlanning)
			})
		}
	})

	t.Run("no command reaches RUNNING, STOPPING, COMPLETED or FAILED", func(t *testing.T) {
		unreachable := []replication.PlanState{
			replication.PlanStateRunning,
			replication.PlanStateStopping,
			replication.PlanStateCompleted,
			replication.PlanStateFailed,
		}
		commands := map[string]func(*replication.ShardReplicationFSM) error{
			"create": func(fsm *replication.ShardReplicationFSM) error {
				return fsm.CreatePlan(createReq("plan-2"))
			},
			"stop": func(fsm *replication.ShardReplicationFSM) error {
				return fsm.StopPlan(&replication.StopPlanRequest{PlanID: seededPlanID})
			},
			"delete": func(fsm *replication.ShardReplicationFSM) error {
				return fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: seededPlanID})
			},
		}
		for _, outcome := range allPlanStates {
			commands["complete with "+string(outcome)] = func(fsm *replication.ShardReplicationFSM) error {
				return fsm.CompletePlanning(runTerm, completeReq(outcome))
			}
		}
		for _, start := range starts {
			for _, name := range slices.Sorted(maps.Keys(commands)) {
				t.Run(fmt.Sprintf("%s from %s", name, startName(start)), func(t *testing.T) {
					fsm, _ := newPlanFSM()
					seedPlan(t, fsm, startPlan(start))

					if err := commands[name](fsm); err != nil {
						got, _ := fsm.GetPlan()
						require.Equal(t, start, got.State, "a refused command changed the plan")
						return
					}
					got, _ := fsm.GetPlan()
					require.NotContains(t, unreachable, got.State)
				})
			}
		}
	})
}

func TestRebalancePlan_Fencing(t *testing.T) {
	planning := seededPlan(replication.PlanStatePlanning)
	recordedRun := seededPlan(replication.PlanStatePlanning)
	recordedRun.RunID, recordedRun.RunTerm = "run-a", runTerm

	withRun := func(runID string, term uint64) *replication.CompletePlanningRequest {
		req := completeReq(replication.PlanStateProposed)
		req.RunID, req.RunTerm = runID, term
		return req
	}
	landed := func(base *replication.Plan, runID string, term uint64) replication.Plan {
		p := *base
		req := withRun(runID, term)
		p.State = replication.PlanStateProposed
		p.RunID, p.RunTerm = req.RunID, req.RunTerm
		p.Baseline, p.Goal = req.Baseline, req.Goal
		p.MoveCount, p.PlanHash, p.ApproveBeforeUnixMs = req.MoveCount, req.PlanHash, req.ApproveBeforeUnixMs
		return p
	}
	stopped := *planning
	stopped.State = replication.PlanStateStopped
	proposed := seededPlan(replication.PlanStateProposed)
	proposedRecordedRun := seededPlan(replication.PlanStateProposed)
	proposedRecordedRun.RunID, proposedRecordedRun.RunTerm = "run-a", runTerm

	cases := []struct {
		name     string
		seed     *replication.Plan
		apply    func(*replication.ShardReplicationFSM) []error
		wantErrs []error
		want     replication.Plan
	}{
		{
			name: "complete whose run term differs from the entry's term is refused",
			seed: planning,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				return []error{fsm.CompletePlanning(runTerm+1, withRun("run-a", runTerm))}
			},
			wantErrs: []error{replication.ErrPlanStaleRun},
			want:     *planning,
		},
		{
			name: "complete from another run in the recorded term is refused",
			seed: recordedRun,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				return []error{fsm.CompletePlanning(runTerm, withRun("run-b", runTerm))}
			},
			wantErrs: []error{replication.ErrPlanStaleRun},
			want:     *recordedRun,
		},
		{
			name: "a stale term against a STOPPED plan is refused as stale before the state check",
			seed: &stopped,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				return []error{fsm.CompletePlanning(runTerm+1, withRun("run-a", runTerm))}
			},
			wantErrs: []error{replication.ErrPlanStaleRun},
			want:     stopped,
		},
		{
			name: "a stale term against a PROPOSED plan is refused as stale before the state check",
			seed: proposed,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				return []error{fsm.CompletePlanning(runTerm+1, withRun("run-a", runTerm))}
			},
			wantErrs: []error{replication.ErrPlanStaleRun},
			want:     *proposed,
		},
		{
			name: "another run in the recorded term against a PROPOSED plan is refused as stale",
			seed: proposedRecordedRun,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				return []error{fsm.CompletePlanning(runTerm, withRun("run-b", runTerm))}
			},
			wantErrs: []error{replication.ErrPlanStaleRun},
			want:     *proposedRecordedRun,
		},
		{
			name: "a stale term for another plan ID is refused as not found",
			seed: planning,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				req := withRun("run-a", runTerm)
				req.PlanID = "plan-other"
				return []error{fsm.CompletePlanning(runTerm+1, req)}
			},
			wantErrs: []error{replication.ErrPlanNotFound},
			want:     *planning,
		},
		{
			name: "a stale term with an invalid outcome is refused as stale before the outcome check",
			seed: planning,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				req := withRun("run-a", runTerm)
				req.Outcome = replication.PlanStateRunning
				return []error{fsm.CompletePlanning(runTerm+1, req)}
			},
			wantErrs: []error{replication.ErrPlanStaleRun},
			want:     *planning,
		},
		{
			name: "another plan ID with an invalid outcome is refused as not found before the outcome check",
			seed: planning,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				req := withRun("run-a", runTerm)
				req.PlanID = "plan-other"
				req.Outcome = replication.PlanStateRunning
				return []error{fsm.CompletePlanning(runTerm, req)}
			},
			wantErrs: []error{replication.ErrPlanNotFound},
			want:     *planning,
		},
		{
			name: "another run in the recorded term with an invalid outcome is refused as stale before the outcome check",
			seed: recordedRun,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				req := withRun("run-b", runTerm)
				req.Outcome = replication.PlanStateRunning
				return []error{fsm.CompletePlanning(runTerm, req)}
			},
			wantErrs: []error{replication.ErrPlanStaleRun},
			want:     *recordedRun,
		},
		{
			name: "another run in the recorded term for another plan ID is refused as not found",
			seed: recordedRun,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				req := withRun("run-b", runTerm)
				req.PlanID = "plan-other"
				return []error{fsm.CompletePlanning(runTerm, req)}
			},
			wantErrs: []error{replication.ErrPlanNotFound},
			want:     *recordedRun,
		},
		{
			name: "stop for another plan ID against a PROPOSED plan is refused as not found",
			seed: proposed,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				return []error{fsm.StopPlan(&replication.StopPlanRequest{PlanID: "plan-other"})}
			},
			wantErrs: []error{replication.ErrPlanNotFound},
			want:     *proposed,
		},
		{
			name: "a run from a later term replaces the recorded run",
			seed: recordedRun,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				return []error{fsm.CompletePlanning(runTerm+1, withRun("run-b", runTerm+1))}
			},
			wantErrs: []error{nil},
			want:     landed(recordedRun, "run-b", runTerm+1),
		},
		{
			name: "complete for another plan ID is refused",
			seed: planning,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				req := withRun("run-a", runTerm)
				req.PlanID = "plan-other"
				return []error{fsm.CompletePlanning(runTerm, req)}
			},
			wantErrs: []error{replication.ErrPlanNotFound},
			want:     *planning,
		},
		{
			name: "complete after stop is refused",
			seed: planning,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				return []error{
					fsm.StopPlan(&replication.StopPlanRequest{PlanID: seededPlanID}),
					fsm.CompletePlanning(runTerm, withRun("run-a", runTerm)),
				}
			},
			wantErrs: []error{nil, replication.ErrPlanTransition},
			want:     stopped,
		},
		{
			name: "stop and delete for another plan ID are refused",
			seed: planning,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				return []error{
					fsm.StopPlan(&replication.StopPlanRequest{PlanID: "plan-other"}),
					fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: "plan-other"}),
				}
			},
			wantErrs: []error{replication.ErrPlanNotFound, replication.ErrPlanNotFound},
			want:     *planning,
		},
		{
			name: "the matching run lands and records its run ID and term",
			seed: planning,
			apply: func(fsm *replication.ShardReplicationFSM) []error {
				return []error{fsm.CompletePlanning(runTerm, withRun("run-a", runTerm))}
			},
			wantErrs: []error{nil},
			want:     landed(planning, "run-a", runTerm),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm, reg := newPlanFSM()
			seedPlan(t, fsm, tc.seed)

			errs := tc.apply(fsm)

			require.Len(t, errs, len(tc.wantErrs))
			for i, wantErr := range tc.wantErrs {
				if wantErr == nil {
					require.NoError(t, errs[i])
					continue
				}
				require.ErrorIs(t, errs[i], wantErr)
			}
			got, ok := fsm.GetPlan()
			require.True(t, ok)
			require.Equal(t, tc.want, got)
			requirePlanGauge(t, reg, tc.want.State)
		})
	}
}

func TestRebalancePlan_SnapshotRoundTrip(t *testing.T) {
	seedOps := func(t *testing.T, fsm *replication.ShardReplicationFSM) {
		seedViaRestore(t, fsm,
			seedSpec{id: 1, state: api.HYDRATING, stateStartMs: 10},
			seedSpec{id: 2, state: api.READY, stateStartMs: 20, shard: "shard2"},
		)
	}

	t.Run("plan and ops survive Snapshot and Restore", func(t *testing.T) {
		src, _ := newPlanFSM()
		seedOps(t, src)
		require.NoError(t, src.CreatePlan(createReq(seededPlanID)))
		require.NoError(t, src.CompletePlanning(runTerm, completeReq(replication.PlanStateProposed)))
		wantPlan, ok := src.GetPlan()
		require.True(t, ok)

		b, err := src.Snapshot()
		require.NoError(t, err)

		dst, reg := newPlanFSM()
		require.NoError(t, dst.Restore(b))

		gotPlan, ok := dst.GetPlan()
		require.True(t, ok)
		require.Equal(t, wantPlan, gotPlan)
		require.Equal(t, src.GetStatusByOps(), dst.GetStatusByOps())
		requirePlanGauge(t, reg, replication.PlanStateProposed)
	})

	t.Run("a snapshot without a plan has only the Ops key", func(t *testing.T) {
		fsm, _ := newPlanFSM()
		seedOps(t, fsm)
		require.NoError(t, fsm.CreatePlan(createReq(seededPlanID)))
		require.NoError(t, fsm.StopPlan(&replication.StopPlanRequest{PlanID: seededPlanID}))
		require.NoError(t, fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: seededPlanID}))

		b, err := fsm.Snapshot()
		require.NoError(t, err)

		var keys map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(b, &keys))
		require.Equal(t, []string{"Ops"}, slices.Collect(maps.Keys(keys)))

		prePlan, err := json.Marshal(struct {
			Ops map[replication.ShardReplicationOp]replication.ShardReplicationOpStatus
		}{Ops: fsm.GetStatusByOps()})
		require.NoError(t, err)
		require.Equal(t, string(prePlan), string(b), "a snapshot without a plan must encode as before plans existed")
	})

	t.Run("restoring a snapshot without a plan clears the existing plan", func(t *testing.T) {
		fsm, reg := newPlanFSM()
		seedPlan(t, fsm, seededPlan(replication.PlanStateProposed))
		requirePlanGauge(t, reg, replication.PlanStateProposed)

		seedOps(t, fsm)

		_, ok := fsm.GetPlan()
		require.False(t, ok)
		requirePlanGauge(t, reg, "")
		require.Len(t, fsm.GetStatusByOps(), 2)
	})
}

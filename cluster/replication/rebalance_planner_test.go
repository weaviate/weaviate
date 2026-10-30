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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/types"
	clusterTypes "github.com/weaviate/weaviate/cluster/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	planningRunsMetric     = "weaviate_replication_rebalance_planning_runs_total"
	planningDurationMetric = "weaviate_replication_rebalance_planning_duration_seconds"

	plannerPlanID        = "plan-1"
	plannerTerm   uint64 = 9
)

// plannerStart is the fake clock's time when a run starts.
var plannerStart = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

type measureFunc func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error)

// fakeProposer applies each completion to the FSM at logTerm, as Raft applies
// the entry, unless err is set. It records every request and its result. The
// real proposer JSON-encodes each request, so a request that fails to encode
// fails the test.
type fakeProposer struct {
	t       *testing.T
	fsm     *replication.ShardReplicationFSM
	logTerm uint64
	err     error

	mu   sync.Mutex
	reqs []replication.CompletePlanningRequest
	errs []error
}

func (f *fakeProposer) CompleteRebalancePlanning(_ context.Context, req *replication.CompletePlanningRequest) error {
	_, err := json.Marshal(req)
	if err != nil {
		f.t.Errorf("completion request does not encode: %v", err)
	} else if err = f.err; err == nil {
		err = f.fsm.CompletePlanning(f.logTerm, req)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, *req)
	f.errs = append(f.errs, err)
	return err
}

func (f *fakeProposer) recorded() ([]replication.CompletePlanningRequest, []error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]replication.CompletePlanningRequest(nil), f.reqs...), append([]error(nil), f.errs...)
}

type plannerHarness struct {
	planner  *replication.RebalancePlanner
	fsm      *replication.ShardReplicationFSM
	proposer *fakeProposer
	clock    *clockwork.FakeClock
	registry *prometheus.Registry

	measureCalls atomic.Int32
	term         atomic.Uint64
	leader       atomic.Bool
	rebalanceOn  atomic.Bool
	movementOn   atomic.Bool
	// gate, when set before Run starts, receives once per Enabled evaluation.
	gate chan struct{}
	// afterReady, when set before a run, runs after each ReadyToPlan evaluation.
	afterReady func()
}

// newPlannerHarness returns a leader with both flags on and a PLANNING plan
// listing node-1 as source and node-2 as target. A nil measure leaves the
// planner's Measure nil.
func newPlannerHarness(t *testing.T, measure measureFunc, states map[string]clusterTypes.ClassState) *plannerHarness {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()

	h := &plannerHarness{
		registry: prometheus.NewPedanticRegistry(),
		clock:    clockwork.NewFakeClockAt(plannerStart),
	}
	h.fsm = replication.NewShardReplicationFSM(h.registry)
	h.proposer = &fakeProposer{t: t, fsm: h.fsm, logTerm: plannerTerm}
	h.term.Store(plannerTerm)
	h.leader.Store(true)
	h.rebalanceOn.Store(true)
	h.movementOn.Store(true)

	require.NoError(t, h.fsm.CreatePlan(&replication.CreatePlanRequest{
		PlanID:          plannerPlanID,
		SourceNodes:     []string{"node-1"},
		TargetNodes:     []string{"node-2"},
		CreatedAtUnixMs: plannerStart.UnixMilli(),
	}))

	params := replication.RebalancePlannerParams{
		Logger:      logger,
		FSM:         h.fsm,
		Proposer:    h.proposer,
		Clock:       h.clock,
		Registerer:  h.registry,
		States:      func() map[string]clusterTypes.ClassState { return states },
		CurrentTerm: h.term.Load,
		ReadyToPlan: func() bool {
			ready := h.leader.Load()
			if h.afterReady != nil {
				h.afterReady()
			}
			return ready
		},
		Enabled: func() bool {
			if h.gate != nil {
				h.gate <- struct{}{}
			}
			return h.rebalanceOn.Load() && h.movementOn.Load()
		},
	}
	if measure != nil {
		params.Measure = func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
			h.measureCalls.Add(1)
			return measure(ctx, node)
		}
	}
	planner, err := replication.NewRebalancePlanner(params)
	require.NoError(t, err)
	h.planner = planner
	return h
}

// equalNodes measures node-1 and node-2 within every tolerance but not
// identically: by (max−min)/max, disk totals differ by 3.8% and memory limits
// by 3.6%, which an absolute comparison would refuse.
func equalNodes() map[string]types.RebalanceNodeMeasurement {
	return map[string]types.RebalanceNodeMeasurement{
		"node-1": {
			DiskTotalBytes: 1_000_000_000, DiskAvailableBytes: 400_000_000,
			DiskWarningPercentage: 80, MemoryLimitBytes: 8_000_000_000, ReplicationWorkers: 10,
		},
		"node-2": {
			DiskTotalBytes: 1_040_000_000, DiskAvailableBytes: 400_000_000,
			DiskWarningPercentage: 80, MemoryLimitBytes: 8_300_000_000, ReplicationWorkers: 10,
		},
	}
}

// Disk utilisation of equalNodes: used / (total × 80%).
const (
	node1Utilisation = 600_000_000.0 / 800_000_000.0
	node2Utilisation = 640_000_000.0 / 832_000_000.0
)

// fixedMeasure answers from rows, leaving Node for the planner to set.
func fixedMeasure(rows map[string]types.RebalanceNodeMeasurement) measureFunc {
	return func(_ context.Context, node string) (types.RebalanceNodeMeasurement, error) {
		m, ok := rows[node]
		if !ok {
			return types.RebalanceNodeMeasurement{}, fmt.Errorf("unexpected node %q", node)
		}
		return m, nil
	}
}

// measuredRows is rows as the planner records them: node-1 then node-2, Node set.
func measuredRows(rows map[string]types.RebalanceNodeMeasurement) []types.RebalanceNodeMeasurement {
	out := make([]types.RebalanceNodeMeasurement, 0, 2)
	for _, node := range []string{"node-1", "node-2"} {
		m := rows[node]
		m.Node = node
		out = append(out, m)
	}
	return out
}

type shardGroup struct {
	count  int
	status string
	nodes  []string
}

// layout returns one collection holding count shards per group, each shard
// with a replica on every node of its group.
func layout(groups ...shardGroup) map[string]clusterTypes.ClassState {
	physical := map[string]sharding.Physical{}
	for g, group := range groups {
		for i := range group.count {
			name := fmt.Sprintf("g%d-shard%d", g, i)
			physical[name] = sharding.Physical{Name: name, BelongsToNodes: group.nodes, Status: group.status}
		}
	}
	return map[string]clusterTypes.ClassState{
		"Articles": {Class: models.Class{Class: "Articles"}, Shards: sharding.State{Physical: physical}},
	}
}

// balancedLayout puts 100 replicas on node-1 and 104 on node-2: count ratios
// 0.98 and 1.02 (stddev 0.0196, balanced), while the raw counts' stddev is 2.
// Counting the FROZEN shards would give node-1 130 (stddev 0.11), skipping the
// COLD ones would give node-2 100, and counting the unlisted node-9 in the
// mean would unbalance every ratio.
func balancedLayout() map[string]clusterTypes.ClassState {
	return layout(
		shardGroup{count: 100, nodes: []string{"node-1", "node-2"}},
		shardGroup{count: 4, status: models.TenantActivityStatusCOLD, nodes: []string{"node-2", "node-9"}},
		shardGroup{count: 30, status: models.TenantActivityStatusFROZEN, nodes: []string{"node-1"}},
	)
}

// requireRun asserts the run proposed exactly once with the run's identity and
// measurement time, and counted one run under result.
func requireRun(t *testing.T, h *plannerHarness, result string) replication.CompletePlanningRequest {
	t.Helper()
	reqs, _ := h.proposer.recorded()
	require.Len(t, reqs, 1)
	req := reqs[0]
	require.Equal(t, plannerPlanID, req.PlanID)
	require.Equal(t, plannerStart.UnixMilli(), req.MeasuredAtUnixMs)
	require.Equal(t, plannerTerm, req.RunTerm)
	require.NotEmpty(t, req.RunID)
	requireCounted(t, h, result)
	return req
}

// requireCounted asserts the planner counted and timed exactly one run, under
// result.
func requireCounted(t *testing.T, h *plannerHarness, result string) {
	t.Helper()
	require.NoError(t, testutil.GatherAndCompare(h.registry, strings.NewReader(fmt.Sprintf(`
# HELP %[1]s Total number of rebalance planning runs, by outcome or by why the outcome did not land
# TYPE %[1]s counter
%[1]s{result=%[2]q} 1
`, planningRunsMetric, result)), planningRunsMetric))
	require.Equal(t, uint64(1), histogramCount(t, h.registry, planningDurationMetric))
}

func histogramCount(t *testing.T, g prometheus.Gatherer, name string) uint64 {
	t.Helper()
	families, err := g.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == name {
			require.Len(t, family.GetMetric(), 1)
			return family.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	return 0
}

func requireBaseline(t *testing.T, want, got []replication.PlanNodeBalance) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].Node, got[i].Node)
		require.Equal(t, want[i].ReplicaCount, got[i].ReplicaCount, "replica count of %s", want[i].Node)
		require.InDelta(t, want[i].DiskUtilisation, got[i].DiskUtilisation, 1e-9, "disk utilisation of %s", want[i].Node)
	}
}

func TestRebalancePlanner_Verdicts(t *testing.T) {
	withNode2 := func(mutate func(*types.RebalanceNodeMeasurement)) map[string]types.RebalanceNodeMeasurement {
		rows := equalNodes()
		m := rows["node-2"]
		mutate(&m)
		rows["node-2"] = m
		return rows
	}
	withAll := func(mutate func(*types.RebalanceNodeMeasurement)) map[string]types.RebalanceNodeMeasurement {
		rows := equalNodes()
		for node, m := range rows {
			mutate(&m)
			rows[node] = m
		}
		return rows
	}

	cases := []struct {
		name   string
		rows   map[string]types.RebalanceNodeMeasurement
		states map[string]clusterTypes.ClassState
		// measure, when set, replaces fixedMeasure(rows).
		measure    func(h *plannerHarness) measureFunc
		nilMeasure bool
		setup      func(t *testing.T, h *plannerHarness)

		wantOutcome  replication.PlanState
		wantResult   string
		wantReason   string
		wantNodes    []types.RebalanceNodeMeasurement
		wantBaseline []replication.PlanNodeBalance
		check        func(t *testing.T, h *plannerHarness)
	}{
		{
			name:        "balanced",
			wantOutcome: replication.PlanStateNotNeeded,
			wantBaseline: []replication.PlanNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 100},
				{Node: "node-2", DiskUtilisation: node2Utilisation, ReplicaCount: 104},
			},
		},
		{
			name: "imbalanced count ratios",
			// Count ratios 1.33 and 0.67; disk utilisation as in balanced.
			states: layout(
				shardGroup{count: 40, nodes: []string{"node-1", "node-2"}},
				shardGroup{count: 40, nodes: []string{"node-1"}},
			),
			wantOutcome: replication.PlanStateProposed,
			wantBaseline: []replication.PlanNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 80},
				{Node: "node-2", DiskUtilisation: node2Utilisation, ReplicaCount: 40},
			},
			check: func(t *testing.T, h *plannerHarness) {
				create := func(id string) error {
					return h.fsm.CreatePlan(&replication.CreatePlanRequest{
						PlanID:          id,
						SourceNodes:     []string{"node-1"},
						TargetNodes:     []string{"node-2"},
						CreatedAtUnixMs: plannerStart.Add(365 * 24 * time.Hour).UnixMilli(),
					})
				}
				require.ErrorIs(t, create("plan-2"), replication.ErrPlanExists, "a PROPOSED plan with no moves never expires")
				require.NoError(t, h.fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: plannerPlanID}))
				_, ok := h.fsm.GetPlan()
				require.False(t, ok)
				require.NoError(t, create("plan-3"))
			},
		},
		{
			name: "imbalanced disk utilisation",
			rows: withNode2(func(m *types.RebalanceNodeMeasurement) { m.DiskAvailableBytes = 100_000_000 }),
			// Utilisation 0.75 and 1.13; count ratios as in balanced.
			wantOutcome: replication.PlanStateProposed,
			wantBaseline: []replication.PlanNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 100},
				{Node: "node-2", DiskUtilisation: 940_000_000.0 / 832_000_000.0, ReplicaCount: 104},
			},
		},
		{
			name: "no replicas on any listed node",
			// node-9 is not listed, so the mean replica count is 0.
			states:      layout(shardGroup{count: 4, nodes: []string{"node-9"}}),
			wantOutcome: replication.PlanStateNotNeeded,
			wantBaseline: []replication.PlanNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation},
				{Node: "node-2", DiskUtilisation: node2Utilisation},
			},
		},
		{
			name: "unreachable node",
			measure: func(h *plannerHarness) measureFunc {
				return func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
					if node != "node-2" {
						return equalNodes()[node], nil
					}
					deadline, ok := ctx.Deadline()
					if !ok || !deadline.Equal(plannerStart.Add(30*time.Second)) {
						return types.RebalanceNodeMeasurement{}, fmt.Errorf("deadline %v, %v: want start + 30s", deadline, ok)
					}
					h.clock.Advance(deadline.Sub(h.clock.Now()))
					<-ctx.Done()
					if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
						return types.RebalanceNodeMeasurement{}, ctx.Err()
					}
					return types.RebalanceNodeMeasurement{}, errors.New("node-2 did not answer before the deadline")
				}
			},
			wantOutcome: replication.PlanStatePlanningFailed,
			wantReason:  "could not measure nodes due to error: node-2: node-2 did not answer before the deadline",
			wantNodes: []types.RebalanceNodeMeasurement{
				measuredRows(equalNodes())[0],
				{Node: "node-2", Error: "node-2 did not answer before the deadline"},
			},
		},
		{
			name:        "nil Measure",
			nilMeasure:  true,
			wantOutcome: replication.PlanStatePlanningFailed,
			wantReason:  "measurement unavailable",
			wantNodes: []types.RebalanceNodeMeasurement{
				{Node: "node-1", Error: "measurement unavailable"},
				{Node: "node-2", Error: "measurement unavailable"},
			},
		},
		{
			name:        "no memory limit",
			rows:        withNode2(func(m *types.RebalanceNodeMeasurement) { m.MemoryLimitBytes = math.MaxInt64 }),
			wantOutcome: replication.PlanStatePlanningFailed,
			wantReason:  "no memory limit on nodes: node-2",
		},
		{
			name: "unequal disk capacity",
			// 5.7% apart.
			rows:        withNode2(func(m *types.RebalanceNodeMeasurement) { m.DiskTotalBytes = 1_060_000_000 }),
			wantOutcome: replication.PlanStatePlanningFailed,
			wantReason:  "disk capacity differs by more than 5% across nodes",
		},
		{
			name: "unequal memory limit",
			// 5.9% apart.
			rows:        withNode2(func(m *types.RebalanceNodeMeasurement) { m.MemoryLimitBytes = 8_500_000_000 }),
			wantOutcome: replication.PlanStatePlanningFailed,
			wantReason:  "memory limit differs by more than 5% across nodes",
		},
		{
			name:        "unequal warning percentage",
			rows:        withNode2(func(m *types.RebalanceNodeMeasurement) { m.DiskWarningPercentage = 85 }),
			wantOutcome: replication.PlanStatePlanningFailed,
			wantReason:  "disk warning percentage differs across nodes",
		},
		{
			// Equal on every node, so the agreement checks pass; utilisation
			// would divide by zero.
			name:        "zero warning percentage",
			rows:        withAll(func(m *types.RebalanceNodeMeasurement) { m.DiskWarningPercentage = 0 }),
			wantOutcome: replication.PlanStatePlanningFailed,
			wantReason:  "disk usage warning disabled on nodes: node-1, node-2",
		},
		{
			// Equal on every node, so the agreement checks pass; utilisation
			// would divide by zero.
			name: "zero disk total",
			rows: withAll(func(m *types.RebalanceNodeMeasurement) {
				m.DiskTotalBytes, m.DiskAvailableBytes = 0, 0
			}),
			wantOutcome: replication.PlanStatePlanningFailed,
			wantReason:  "no disk capacity on nodes: node-1, node-2",
		},
		{
			name: "empty node lists",
			setup: func(t *testing.T, h *plannerHarness) {
				require.NoError(t, h.fsm.StopPlan(&replication.StopPlanRequest{PlanID: plannerPlanID}))
				require.NoError(t, h.fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: plannerPlanID}))
				require.NoError(t, h.fsm.CreatePlan(&replication.CreatePlanRequest{
					PlanID:          plannerPlanID,
					CreatedAtUnixMs: plannerStart.UnixMilli(),
				}))
			},
			wantOutcome: replication.PlanStatePlanningFailed,
			wantReason:  "no nodes listed",
			wantNodes:   []types.RebalanceNodeMeasurement{},
		},
		{
			name: "stop during measurement",
			measure: func(h *plannerHarness) measureFunc {
				return func(_ context.Context, node string) (types.RebalanceNodeMeasurement, error) {
					if node == "node-1" {
						if err := h.fsm.StopPlan(&replication.StopPlanRequest{PlanID: plannerPlanID}); err != nil {
							return types.RebalanceNodeMeasurement{}, err
						}
					}
					return equalNodes()[node], nil
				}
			},
			wantOutcome: replication.PlanStateNotNeeded,
			wantResult:  "STOPPED",
			wantBaseline: []replication.PlanNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 100},
				{Node: "node-2", DiskUtilisation: node2Utilisation, ReplicaCount: 104},
			},
			check: func(t *testing.T, h *plannerHarness) {
				_, errs := h.proposer.recorded()
				require.ErrorIs(t, errs[0], replication.ErrPlanTransition)
				got, ok := h.fsm.GetPlan()
				require.True(t, ok)
				require.Equal(t, replication.PlanStateStopped, got.State)
				require.Empty(t, got.RunID)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := tc.rows
			if rows == nil {
				rows = equalNodes()
			}
			states := tc.states
			if states == nil {
				states = balancedLayout()
			}
			wantNodes := tc.wantNodes
			if wantNodes == nil {
				wantNodes = measuredRows(rows)
			}
			wantResult := tc.wantResult
			if wantResult == "" {
				wantResult = string(tc.wantOutcome)
			}
			var wantApproveBefore int64
			if tc.wantOutcome == replication.PlanStateProposed {
				wantApproveBefore = math.MaxInt64
			}

			var h *plannerHarness
			switch {
			case tc.nilMeasure:
				h = newPlannerHarness(t, nil, states)
			case tc.measure != nil:
				// The harness exists before the measure that refers to it.
				var measure measureFunc
				h = newPlannerHarness(t, func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
					return measure(ctx, node)
				}, states)
				measure = tc.measure(h)
			default:
				h = newPlannerHarness(t, fixedMeasure(rows), states)
			}
			if tc.setup != nil {
				tc.setup(t, h)
			}

			h.planner.RunOnce(context.Background())

			req := requireRun(t, h, wantResult)
			require.Equal(t, tc.wantOutcome, req.Outcome)
			require.Equal(t, tc.wantReason, req.Reason)
			require.Equal(t, wantNodes, req.Nodes)
			requireBaseline(t, tc.wantBaseline, req.Baseline)
			require.Zero(t, req.MoveCount)
			require.Equal(t, wantApproveBefore, req.ApproveBeforeUnixMs)

			if wantResult == string(tc.wantOutcome) {
				got, ok := h.fsm.GetPlan()
				require.True(t, ok)
				require.Equal(t, tc.wantOutcome, got.State)
				require.Equal(t, req.RunID, got.RunID)
				require.Equal(t, plannerTerm, got.RunTerm)
				require.Equal(t, plannerStart.UnixMilli(), got.MeasuredAtUnixMs)
				require.Equal(t, wantNodes, got.Nodes)
				require.Equal(t, tc.wantReason, got.Reason)
				requireBaseline(t, tc.wantBaseline, got.Baseline)
				require.Zero(t, got.MoveCount)
				require.Equal(t, wantApproveBefore, got.ApproveBeforeUnixMs)
			}
			if tc.check != nil {
				tc.check(t, h)
			}
		})
	}
}

func TestRebalancePlanner_ProposalErrors(t *testing.T) {
	cases := []struct {
		name      string
		setup     func(t *testing.T, h *plannerHarness)
		measure   func(h *plannerHarness) measureFunc
		wantErr   error
		wantPlan  string
		wantState replication.PlanState
		result    string
	}{
		{
			name: "a plan stopped, deleted and recreated during measurement ends STOPPED",
			measure: func(h *plannerHarness) measureFunc {
				return func(_ context.Context, node string) (types.RebalanceNodeMeasurement, error) {
					if node == "node-1" {
						err := errors.Join(
							h.fsm.StopPlan(&replication.StopPlanRequest{PlanID: plannerPlanID}),
							h.fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: plannerPlanID}),
							h.fsm.CreatePlan(&replication.CreatePlanRequest{
								PlanID:      "plan-2",
								SourceNodes: []string{"node-1"},
								TargetNodes: []string{"node-2"},
							}),
						)
						if err != nil {
							return types.RebalanceNodeMeasurement{}, err
						}
					}
					return equalNodes()[node], nil
				}
			},
			wantErr:   replication.ErrPlanNotFound,
			wantPlan:  "plan-2",
			wantState: replication.PlanStatePlanning,
			result:    "STOPPED",
		},
		{
			name: "a completion applied in a later term ends STALE",
			setup: func(t *testing.T, h *plannerHarness) {
				h.proposer.logTerm = plannerTerm + 1
			},
			wantErr:   replication.ErrPlanStaleRun,
			wantPlan:  plannerPlanID,
			wantState: replication.PlanStatePlanning,
			result:    "STALE",
		},
		{
			name: "a run whose leader is deposed after the ready gate keeps the old term and ends STALE",
			setup: func(t *testing.T, h *plannerHarness) {
				h.afterReady = func() { h.term.Store(plannerTerm + 1) }
				h.proposer.logTerm = plannerTerm + 1
			},
			wantErr:   replication.ErrPlanStaleRun,
			wantPlan:  plannerPlanID,
			wantState: replication.PlanStatePlanning,
			result:    "STALE",
		},
		{
			name: "any other proposal error ends ERROR",
			setup: func(t *testing.T, h *plannerHarness) {
				h.proposer.err = errors.New("no leader found")
			},
			wantPlan:  plannerPlanID,
			wantState: replication.PlanStatePlanning,
			result:    "ERROR",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			measure := fixedMeasure(equalNodes())
			h := newPlannerHarness(t, func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
				return measure(ctx, node)
			}, balancedLayout())
			if tc.measure != nil {
				measure = tc.measure(h)
			}
			if tc.setup != nil {
				tc.setup(t, h)
			}

			h.planner.RunOnce(context.Background())

			requireRun(t, h, tc.result)
			if tc.wantErr != nil {
				_, errs := h.proposer.recorded()
				require.ErrorIs(t, errs[0], tc.wantErr)
			}
			got, ok := h.fsm.GetPlan()
			require.True(t, ok)
			require.Equal(t, tc.wantPlan, got.ID)
			require.Equal(t, tc.wantState, got.State)
			require.Empty(t, got.RunID)
		})
	}
}

func TestRebalancePlanner_Cancelled(t *testing.T) {
	t.Run("a run cancelled during measurement proposes nothing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		h := newPlannerHarness(t, func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
			if node == "node-2" {
				cancel()
				<-ctx.Done()
				return types.RebalanceNodeMeasurement{}, ctx.Err()
			}
			return equalNodes()[node], nil
		}, balancedLayout())

		h.planner.RunOnce(ctx)

		require.Equal(t, int32(2), h.measureCalls.Load())
		reqs, _ := h.proposer.recorded()
		require.Empty(t, reqs)
		got, ok := h.fsm.GetPlan()
		require.True(t, ok)
		require.Equal(t, replication.PlanStatePlanning, got.State)
		require.Empty(t, got.RunID)
		requireCounted(t, h, "CANCELLED")
	})
}

func TestRebalancePlanner_Wake(t *testing.T) {
	t.Run("Wake does not block on a full channel when Run is not receiving", func(t *testing.T) {
		h := newPlannerHarness(t, fixedMeasure(equalNodes()), balancedLayout())

		done := make(chan struct{})
		logger, _ := logrustest.NewNullLogger()
		enterrors.GoWrapper(func() {
			defer close(done)
			for range 1000 {
				h.planner.Wake()
			}
		}, logger)

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("Wake blocked")
		}
	})
}

func TestRebalancePlanner_MeasuresInParallel(t *testing.T) {
	t.Run("each node's Measure completes only after the other's has started", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// If the timeout below fails the test, this releases the Measure call still waiting.
		defer cancel()

		rows := equalNodes()
		started := map[string]chan struct{}{"node-1": make(chan struct{}), "node-2": make(chan struct{})}
		other := map[string]string{"node-1": "node-2", "node-2": "node-1"}
		h := newPlannerHarness(t, func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
			close(started[node])
			select {
			case <-started[other[node]]:
				return rows[node], nil
			case <-ctx.Done():
				return types.RebalanceNodeMeasurement{}, ctx.Err()
			}
		}, balancedLayout())

		done := make(chan struct{})
		logger, _ := logrustest.NewNullLogger()
		enterrors.GoWrapper(func() {
			defer close(done)
			h.planner.RunOnce(ctx)
		}, logger)

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("a node's Measure did not start while another's was running")
		}
		req := requireRun(t, h, string(replication.PlanStateNotNeeded))
		require.Equal(t, measuredRows(rows), req.Nodes)
	})
}

func TestRebalancePlanner_NoRun(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, h *plannerHarness)
	}{
		{
			name:  "rebalance flag off",
			setup: func(t *testing.T, h *plannerHarness) { h.rebalanceOn.Store(false) },
		},
		{
			name:  "movement flag off",
			setup: func(t *testing.T, h *plannerHarness) { h.movementOn.Store(false) },
		},
		{
			name:  "follower node",
			setup: func(t *testing.T, h *plannerHarness) { h.leader.Store(false) },
		},
		{
			name: "plan not PLANNING",
			setup: func(t *testing.T, h *plannerHarness) {
				require.NoError(t, h.fsm.StopPlan(&replication.StopPlanRequest{PlanID: plannerPlanID}))
			},
		},
		{
			name: "no plan",
			setup: func(t *testing.T, h *plannerHarness) {
				require.NoError(t, h.fsm.StopPlan(&replication.StopPlanRequest{PlanID: plannerPlanID}))
				require.NoError(t, h.fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: plannerPlanID}))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newPlannerHarness(t, fixedMeasure(equalNodes()), balancedLayout())
			h.gate = make(chan struct{})
			tc.setup(t, h)
			before, hadBefore := h.fsm.GetPlan()

			ctx, cancel := context.WithCancel(context.Background())
			runDone := make(chan error, 1)
			logger, _ := logrustest.NewNullLogger()
			enterrors.GoWrapper(func() { runDone <- h.planner.Run(ctx) }, logger)

			// Enabled is the loop's first gate. Run handles one wake at a time,
			// so the second wake reaching the gate proves the first pass ended.
			awaitGate := func() {
				select {
				case <-h.gate:
				case <-time.After(10 * time.Second):
					t.Fatal("the planner loop did not take the wake")
				}
			}
			h.planner.Wake()
			awaitGate()
			h.planner.Wake()
			awaitGate()

			cancel()
			require.ErrorIs(t, <-runDone, context.Canceled)
			require.Zero(t, h.measureCalls.Load())
			reqs, _ := h.proposer.recorded()
			require.Empty(t, reqs)
			after, hasAfter := h.fsm.GetPlan()
			require.Equal(t, hadBefore, hasAfter)
			require.Equal(t, before, after)
			runSeries, err := testutil.GatherAndCount(h.registry, planningRunsMetric)
			require.NoError(t, err)
			require.Zero(t, runSeries)
		})
	}
}

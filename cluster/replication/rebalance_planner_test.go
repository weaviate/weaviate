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
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
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

type plannerHarness struct {
	t        *testing.T
	planner  *replication.RebalancePlanner
	fsm      *replication.ShardReplicationFSM
	proposer *replication.MockrebalanceProposer
	clock    *clockwork.FakeClock
	registry *prometheus.Registry

	// logTerm is the term at which a proposal is applied to the FSM.
	logTerm uint64
	// proposeErr, when set, is returned by the proposer, which then skips the FSM.
	proposeErr error

	mu   sync.Mutex
	reqs []replication.CompletePlanningRequest
	errs []error

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
// planner's Measure nil. The proposer mock has no expectations until
// expectProposals is called.
func newPlannerHarness(t *testing.T, measure measureFunc, schemaReader *replication.MockrebalanceSchemaReader) *plannerHarness {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()

	h := &plannerHarness{
		t:        t,
		registry: prometheus.NewPedanticRegistry(),
		clock:    clockwork.NewFakeClockAt(plannerStart),
		proposer: replication.NewMockrebalanceProposer(t),
		logTerm:  plannerTerm,
	}
	h.fsm = replication.NewShardReplicationFSM(h.registry)
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
		Schema:      schemaReader,
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

// expectProposals makes the proposer apply each completion to the FSM at
// logTerm, as Raft applies the entry, unless proposeErr is set. It records
// every request and its result. The real proposer JSON-encodes each request,
// so a request that fails to encode fails the test.
func (h *plannerHarness) expectProposals() *mock.Call {
	return h.proposer.EXPECT().CompleteRebalancePlanning(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *replication.CompletePlanningRequest) error {
			_, err := json.Marshal(req)
			if err != nil {
				h.t.Errorf("completion request does not encode: %v", err)
			} else if err = h.proposeErr; err == nil {
				err = h.fsm.CompletePlanning(h.logTerm, req)
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			h.reqs = append(h.reqs, *req)
			h.errs = append(h.errs, err)
			return err
		}).Call
}

func (h *plannerHarness) recorded() ([]replication.CompletePlanningRequest, []error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]replication.CompletePlanningRequest(nil), h.reqs...), append([]error(nil), h.errs...)
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

// schemaFixture holds the sharding states a schema mock serves. A collection
// in readErrs is listed, and reading it returns its error.
type schemaFixture struct {
	states   map[string]sharding.State
	readErrs map[string]error
}

// mock returns a schema mock that serves f and expects ReadSchema and a Read
// of every listed collection. ReadSchema lists collections in sorted order.
// Read is expected only with retryIfClassNotFound=false, because a real reader
// retries a missing class with backoff before failing.
func (f *schemaFixture) mock(t *testing.T) *replication.MockrebalanceSchemaReader {
	m := replication.NewMockrebalanceSchemaReader(t)
	classes := append(slices.Collect(maps.Keys(f.states)), slices.Collect(maps.Keys(f.readErrs))...)
	slices.Sort(classes)
	m.EXPECT().ReadSchema(mock.Anything).RunAndReturn(func(reader func(models.Class, uint64)) error {
		for _, class := range classes {
			reader(models.Class{Class: class}, 0)
		}
		return nil
	})
	for class, state := range f.states {
		m.EXPECT().Read(class, false, mock.Anything).
			RunAndReturn(func(_ string, _ bool, reader func(*models.Class, *sharding.State) error) error {
				return reader(&models.Class{Class: class}, &state)
			})
	}
	for class, err := range f.readErrs {
		m.EXPECT().Read(class, false, mock.Anything).Return(err)
	}
	return m
}

// failing adds a listed collection whose read returns err.
func (f *schemaFixture) failing(class string, err error) *schemaFixture {
	if f.readErrs == nil {
		f.readErrs = map[string]error{}
	}
	f.readErrs[class] = err
	return f
}

// layout returns one collection holding count shards per group, each shard
// with a replica on every node of its group.
func layout(groups ...shardGroup) *schemaFixture {
	physical := map[string]sharding.Physical{}
	for g, group := range groups {
		for i := range group.count {
			name := fmt.Sprintf("g%d-shard%d", g, i)
			physical[name] = sharding.Physical{Name: name, BelongsToNodes: group.nodes, Status: group.status}
		}
	}
	return &schemaFixture{states: map[string]sharding.State{"Articles": {Physical: physical}}}
}

// balancedLayout puts 100 replicas on node-1 and 104 on node-2: count ratios
// 0.98 and 1.02 (stddev 0.0196, balanced), while the raw counts' stddev is 2.
// Counting the FROZEN shards would give node-1 130 (stddev 0.11), skipping the
// COLD ones would give node-2 100, and counting the unlisted node-9 in the
// mean would unbalance every ratio.
func balancedLayout() *schemaFixture {
	return layout(
		shardGroup{count: 100, nodes: []string{"node-1", "node-2"}},
		shardGroup{count: 4, status: models.TenantActivityStatusCOLD, nodes: []string{"node-2", "node-9"}},
		shardGroup{count: 30, status: models.TenantActivityStatusFROZEN, nodes: []string{"node-1"}},
	)
}

func newSchemaManager() *schema.SchemaManager {
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	return schema.NewSchemaManager("node-1", nil, parser, nil, prometheus.NewPedanticRegistry(), logrus.New())
}

// missingClassErr returns the error a real SchemaReader returns for a
// collection that does not exist, read without retry.
func missingClassErr(t *testing.T) error {
	t.Helper()
	err := newSchemaManager().NewSchemaReader().Read("Deleted", false, func(*models.Class, *sharding.State) error { return nil })
	require.ErrorIs(t, err, schema.ErrClassNotFound)
	return err
}

// requireRun asserts the run proposed exactly once with the run's identity and
// measurement time, and counted one run under result.
func requireRun(t *testing.T, h *plannerHarness, result string) replication.CompletePlanningRequest {
	t.Helper()
	reqs, _ := h.recorded()
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
		states *schemaFixture
		// noSchemaRead marks a run that ends before counting replicas; its
		// schema mock fails the test on any call.
		noSchemaRead bool
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
			name:        "class read error",
			states:      balancedLayout().failing("Broken", errors.New("invalid sharding state: physical shards unavailable")),
			wantOutcome: replication.PlanStatePlanningFailed,
			wantReason:  `could not read collection "Broken": invalid sharding state: physical shards unavailable`,
		},
		{
			name:        "class deleted during the pass",
			states:      balancedLayout().failing("Deleted", missingClassErr(t)),
			wantOutcome: replication.PlanStateNotNeeded,
			wantBaseline: []replication.PlanNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 100},
				{Node: "node-2", DiskUtilisation: node2Utilisation, ReplicaCount: 104},
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
			noSchemaRead: true,
			wantOutcome:  replication.PlanStatePlanningFailed,
			wantReason:   "could not measure nodes due to error: node-2: node-2 did not answer before the deadline",
			wantNodes: []types.RebalanceNodeMeasurement{
				measuredRows(equalNodes())[0],
				{Node: "node-2", Error: "node-2 did not answer before the deadline"},
			},
		},
		{
			name:         "nil Measure",
			nilMeasure:   true,
			noSchemaRead: true,
			wantOutcome:  replication.PlanStatePlanningFailed,
			wantReason:   "measurement unavailable",
			wantNodes: []types.RebalanceNodeMeasurement{
				{Node: "node-1", Error: "measurement unavailable"},
				{Node: "node-2", Error: "measurement unavailable"},
			},
		},
		{
			name:         "no memory limit",
			rows:         withNode2(func(m *types.RebalanceNodeMeasurement) { m.MemoryLimitBytes = math.MaxInt64 }),
			noSchemaRead: true,
			wantOutcome:  replication.PlanStatePlanningFailed,
			wantReason:   "no memory limit on nodes: node-2",
		},
		{
			name: "unequal disk capacity",
			// 5.7% apart.
			rows:         withNode2(func(m *types.RebalanceNodeMeasurement) { m.DiskTotalBytes = 1_060_000_000 }),
			noSchemaRead: true,
			wantOutcome:  replication.PlanStatePlanningFailed,
			wantReason:   "disk capacity differs by more than 5% across nodes",
		},
		{
			name: "unequal memory limit",
			// 5.9% apart.
			rows:         withNode2(func(m *types.RebalanceNodeMeasurement) { m.MemoryLimitBytes = 8_500_000_000 }),
			noSchemaRead: true,
			wantOutcome:  replication.PlanStatePlanningFailed,
			wantReason:   "memory limit differs by more than 5% across nodes",
		},
		{
			name:         "unequal warning percentage",
			rows:         withNode2(func(m *types.RebalanceNodeMeasurement) { m.DiskWarningPercentage = 85 }),
			noSchemaRead: true,
			wantOutcome:  replication.PlanStatePlanningFailed,
			wantReason:   "disk warning percentage differs across nodes",
		},
		{
			// Equal on every node, so the agreement checks pass; utilisation
			// would divide by zero.
			name:         "zero warning percentage",
			rows:         withAll(func(m *types.RebalanceNodeMeasurement) { m.DiskWarningPercentage = 0 }),
			noSchemaRead: true,
			wantOutcome:  replication.PlanStatePlanningFailed,
			wantReason:   "disk usage warning disabled on nodes: node-1, node-2",
		},
		{
			// Equal on every node, so the agreement checks pass; utilisation
			// would divide by zero.
			name: "zero disk total",
			rows: withAll(func(m *types.RebalanceNodeMeasurement) {
				m.DiskTotalBytes, m.DiskAvailableBytes = 0, 0
			}),
			noSchemaRead: true,
			wantOutcome:  replication.PlanStatePlanningFailed,
			wantReason:   "no disk capacity on nodes: node-1, node-2",
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
			noSchemaRead: true,
			wantOutcome:  replication.PlanStatePlanningFailed,
			wantReason:   "no nodes listed",
			wantNodes:    []types.RebalanceNodeMeasurement{},
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
				_, errs := h.recorded()
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
			var states *replication.MockrebalanceSchemaReader
			switch {
			case tc.noSchemaRead:
				states = replication.NewMockrebalanceSchemaReader(t)
			case tc.states != nil:
				states = tc.states.mock(t)
			default:
				states = balancedLayout().mock(t)
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
			h.expectProposals()
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

func TestRebalancePlanner_ConcurrentTenantWrites(t *testing.T) {
	t.Run("planning passes count replicas while tenants are added", func(t *testing.T) {
		const class = "Tenants"
		sm := newSchemaManager()
		require.NoError(t, sm.AddClass(buildApplyRequest(class, api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{
				Class:              class,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				ReplicationConfig:  &models.ReplicationConfig{Factor: 2},
			},
			State: &sharding.State{PartitioningEnabled: true},
		}), "node-1", true, false))

		registry := prometheus.NewPedanticRegistry()
		fsm := replication.NewShardReplicationFSM(registry)
		proposer := replication.NewMockrebalanceProposer(t)
		proposer.EXPECT().CompleteRebalancePlanning(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, req *replication.CompletePlanningRequest) error {
				if _, err := json.Marshal(req); err != nil {
					t.Errorf("completion request does not encode: %v", err)
				}
				return fsm.CompletePlanning(plannerTerm, req)
			})
		logger, _ := logrustest.NewNullLogger()
		planner, err := replication.NewRebalancePlanner(replication.RebalancePlannerParams{
			Logger:      logger,
			FSM:         fsm,
			Proposer:    proposer,
			Clock:       clockwork.NewFakeClockAt(plannerStart),
			Registerer:  registry,
			Schema:      sm.NewSchemaReader(),
			CurrentTerm: func() uint64 { return plannerTerm },
			ReadyToPlan: func() bool { return true },
			Enabled:     func() bool { return true },
			Measure:     fixedMeasure(equalNodes()),
		})
		require.NoError(t, err)

		// The writer adds tenants until the planner has finished minPasses
		// passes, so at least minPasses passes run while tenant adds are in flight.
		const minTenants, minPasses = 200, 50
		var passes atomic.Int32
		type written struct {
			tenants int
			err     error
		}
		writerDone := make(chan written, 1)
		// If a failed assertion ends the subtest early, cancel stops the writer.
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		enterrors.GoWrapper(func() {
			n := 0
			for ; ctx.Err() == nil && (n < minTenants || passes.Load() < minPasses); n++ {
				sub, err := gproto.Marshal(&api.AddTenantsRequest{
					ClusterNodes: []string{"node-1", "node-2"},
					Tenants:      []*api.Tenant{{Name: fmt.Sprintf("tenant-%d", n), Status: models.TenantActivityStatusHOT}},
				})
				if err == nil {
					err = sm.AddTenants(&api.ApplyRequest{Type: api.ApplyRequest_TYPE_ADD_TENANT, Class: class, SubCommand: sub}, true)
				}
				if err != nil {
					writerDone <- written{tenants: n, err: err}
					return
				}
			}
			writerDone <- written{tenants: n}
		}, logger)

		// The pass after the writer reports runs with every tenant added.
		var w written
		var last replication.Plan
		for done := false; !done; {
			select {
			case w = <-writerDone:
				done = true
			default:
			}
			require.NoError(t, fsm.CreatePlan(&replication.CreatePlanRequest{
				PlanID:          plannerPlanID,
				SourceNodes:     []string{"node-1"},
				TargetNodes:     []string{"node-2"},
				CreatedAtUnixMs: plannerStart.UnixMilli(),
			}))
			planner.RunOnce(context.Background())
			passes.Add(1)

			plan, ok := fsm.GetPlan()
			require.True(t, ok)
			require.Equal(t, replication.PlanStateNotNeeded, plan.State, plan.Reason)
			require.Len(t, plan.Baseline, 2)
			// Every tenant has a replica on both nodes.
			require.Equal(t, plan.Baseline[0].ReplicaCount, plan.Baseline[1].ReplicaCount)
			last = plan
			require.NoError(t, fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: plannerPlanID}))
		}
		require.NoError(t, w.err)
		require.GreaterOrEqual(t, w.tenants, minTenants)
		require.Equal(t, w.tenants, last.Baseline[0].ReplicaCount)
	})
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
				h.logTerm = plannerTerm + 1
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
				h.logTerm = plannerTerm + 1
			},
			wantErr:   replication.ErrPlanStaleRun,
			wantPlan:  plannerPlanID,
			wantState: replication.PlanStatePlanning,
			result:    "STALE",
		},
		{
			name: "any other proposal error ends ERROR",
			setup: func(t *testing.T, h *plannerHarness) {
				h.proposeErr = errors.New("no leader found")
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
			}, balancedLayout().mock(t))
			h.expectProposals()
			if tc.measure != nil {
				measure = tc.measure(h)
			}
			if tc.setup != nil {
				tc.setup(t, h)
			}

			h.planner.RunOnce(context.Background())

			requireRun(t, h, tc.result)
			if tc.wantErr != nil {
				_, errs := h.recorded()
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
		}, replication.NewMockrebalanceSchemaReader(t))
		h.expectProposals().Maybe()

		h.planner.RunOnce(ctx)

		require.Equal(t, int32(2), h.measureCalls.Load())
		reqs, _ := h.recorded()
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
		h := newPlannerHarness(t, fixedMeasure(equalNodes()), replication.NewMockrebalanceSchemaReader(t))

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
		}, balancedLayout().mock(t))
		h.expectProposals()

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
			h := newPlannerHarness(t, fixedMeasure(equalNodes()), replication.NewMockrebalanceSchemaReader(t))
			h.expectProposals().Maybe()
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
			reqs, _ := h.recorded()
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

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
	// draftVersion is the version of every draft task the tests serve.
	draftVersion uint64 = 7
)

// plannerStart is the fake clock's time when a run starts.
var plannerStart = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

type measureFunc func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error)

type measureCall struct {
	node    string
	taskID  string
	version uint64
}

type plannerHarness struct {
	t        *testing.T
	planner  *replication.RebalancePlanner
	fsm      *replication.ShardReplicationFSM
	proposer *replication.MockrebalanceProposer
	drafting *replication.MockrebalanceDrafting
	clock    *clockwork.FakeClock
	registry *prometheus.Registry
	logs     *logrustest.Hook

	// logTerm is the term at which a proposal is applied to the FSM.
	logTerm uint64
	// proposeErr, when set, is returned by the proposer, which then skips the FSM.
	proposeErr error

	mu       sync.Mutex
	reqs     []replication.CompletePlanningRequest
	errs     []error
	tasks    []replication.DraftTask
	measured []measureCall

	measureCalls atomic.Int32
	term         atomic.Uint64
	leader       atomic.Bool
	rebalanceOn  atomic.Bool
	movementOn   atomic.Bool
	// gate, when set before Run starts, receives once per ReadyToPlan evaluation.
	gate chan struct{}
	// afterReady, when set before a run, runs after each ReadyToPlan evaluation.
	afterReady func()
}

// newPlannerHarness returns a leader with both flags on and a PLANNING plan
// listing node-1 as source and node-2 as target. A nil measure leaves the
// planner's Measure nil. The proposer mock has no expectations until
// expectProposals is called. The drafting mock serves the tasks set by
// setDraftTasks, starting with reportedDraftTask(plannerPlanID). It expects no
// add and no terminate.
func newPlannerHarness(t *testing.T, measure measureFunc, schemaReader *replication.MockrebalanceSchemaReader) *plannerHarness {
	t.Helper()
	logger, logs := logrustest.NewNullLogger()

	h := &plannerHarness{
		t:        t,
		logs:     logs,
		registry: prometheus.NewPedanticRegistry(),
		clock:    clockwork.NewFakeClockAt(plannerStart),
		proposer: replication.NewMockrebalanceProposer(t),
		drafting: replication.NewMockrebalanceDrafting(t),
		logTerm:  plannerTerm,
		tasks:    []replication.DraftTask{reportedDraftTask(plannerPlanID)},
	}
	h.drafting.EXPECT().RebalanceDraftTasks().RunAndReturn(h.draftTasks).Maybe()
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
		Drafting:    h.drafting,
		Clock:       h.clock,
		Registerer:  h.registry,
		Schema:      schemaReader,
		CurrentTerm: h.term.Load,
		ReadyToPlan: func() bool {
			if h.gate != nil {
				h.gate <- struct{}{}
			}
			ready := h.leader.Load()
			if h.afterReady != nil {
				h.afterReady()
			}
			return ready
		},
		Enabled: func() bool {
			return h.rebalanceOn.Load() && h.movementOn.Load()
		},
	}
	if measure != nil {
		params.Measure = func(ctx context.Context, node, taskID string, version uint64) (types.RebalanceNodeMeasurement, error) {
			h.measureCalls.Add(1)
			h.mu.Lock()
			h.measured = append(h.measured, measureCall{node: node, taskID: taskID, version: version})
			h.mu.Unlock()
			return measure(ctx, node)
		}
	}
	planner, err := replication.NewRebalancePlanner(params)
	require.NoError(t, err)
	h.planner = planner
	return h
}

// reportedDraftTask is planID's FINISHED draft task, with every unit COMPLETED.
func reportedDraftTask(planID string) replication.DraftTask {
	return draftTask(planID, true, "", replication.DraftUnitCompleted, replication.DraftUnitCompleted)
}

// draftTask is planID's draft task at draftVersion with node-1's and node-2's unit statuses.
func draftTask(planID string, terminal bool, taskErr string, node1, node2 replication.DraftUnitStatus) replication.DraftTask {
	return replication.DraftTask{
		PlanID:   planID,
		Version:  draftVersion,
		Terminal: terminal,
		Error:    taskErr,
		Units:    map[string]replication.DraftUnitStatus{"node-1": node1, "node-2": node2},
	}
}

func (h *plannerHarness) setDraftTasks(tasks ...replication.DraftTask) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tasks = tasks
}

func (h *plannerHarness) draftTasks() []replication.DraftTask {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.tasks)
}

func (h *plannerHarness) measuredCalls() []measureCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.measured)
}

// driveClock advances the fake clock by 30s each time the planner waits on it,
// until the test ends. Each wait is a 30s collection attempt or a backoff of
// at most 30s, so one advance ends each wait. A failing Measure must block
// until its ctx ends. One that returns early leaves its attempt's timer
// pending, and an advance then lands mid-round.
func (h *plannerHarness) driveClock() {
	ctx, cancel := context.WithCancel(context.Background())
	h.t.Cleanup(cancel)
	logger, _ := logrustest.NewNullLogger()
	enterrors.GoWrapper(func() {
		for h.clock.BlockUntilContext(ctx, 1) == nil {
			h.clock.Advance(30 * time.Second)
		}
	}, logger)
}

// nodeCalls counts Measure calls per node.
type nodeCalls struct {
	mu sync.Mutex
	n  map[string]int
}

// next counts a call for node and returns that node's call count.
func (c *nodeCalls) next(node string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[node]++
	return c.n[node]
}

// unreachable blocks until ctx ends, as a request to a node that never
// answers does, and returns an error naming node.
func unreachable(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
	<-ctx.Done()
	return types.RebalanceNodeMeasurement{}, fmt.Errorf("%s did not answer before the deadline", node)
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
	return schema.NewSchemaManager("node-1", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
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
// plannerStart as measurement time, and counted one run under result.
func requireRun(t *testing.T, h *plannerHarness, result string) replication.CompletePlanningRequest {
	t.Helper()
	return requireRunAt(t, h, result, plannerStart)
}

// requireRunAt is requireRun with measuredAt as measurement time.
func requireRunAt(t *testing.T, h *plannerHarness, result string, measuredAt time.Time) replication.CompletePlanningRequest {
	t.Helper()
	reqs, _ := h.recorded()
	require.Len(t, reqs, 1)
	req := reqs[0]
	require.Equal(t, plannerPlanID, req.PlanID)
	require.Equal(t, measuredAt.UnixMilli(), req.MeasuredAtUnixMs)
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

func warnings(h *plannerHarness) []*logrus.Entry {
	var entries []*logrus.Entry
	for _, e := range h.logs.AllEntries() {
		if e.Level == logrus.WarnLevel {
			entries = append(entries, e)
		}
	}
	return entries
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
		// wantMeasuredAt, when set, is the expected measurement time instead
		// of plannerStart.
		wantMeasuredAt time.Time
		check          func(t *testing.T, h *plannerHarness)
	}{
		{
			name:        "balanced",
			wantOutcome: replication.PlanStateNotNeeded,
			wantBaseline: []replication.PlanNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 100},
				{Node: "node-2", DiskUtilisation: node2Utilisation, ReplicaCount: 104},
			},
			check: func(t *testing.T, h *plannerHarness) { require.Empty(t, warnings(h)) },
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
					return unreachable(ctx, node)
				}
			},
			setup: func(t *testing.T, h *plannerHarness) { h.driveClock() },
			// Under driveClock, round k starts at 60s × (k−1). Round 11 is the
			// first to end at least 10 minutes after node-2's first failure.
			wantMeasuredAt: plannerStart.Add(10 * time.Minute),
			noSchemaRead:   true,
			wantOutcome:    replication.PlanStatePlanningFailed,
			wantReason:     "could not collect drafts from nodes: node-2",
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
			wantMeasuredAt := tc.wantMeasuredAt
			if wantMeasuredAt.IsZero() {
				wantMeasuredAt = plannerStart
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

			req := requireRunAt(t, h, wantResult, wantMeasuredAt)
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
				require.Equal(t, wantMeasuredAt.UnixMilli(), got.MeasuredAtUnixMs)
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
		drafting := replication.NewMockrebalanceDrafting(t)
		drafting.EXPECT().RebalanceDraftTasks().Return([]replication.DraftTask{reportedDraftTask(plannerPlanID)})
		measure := fixedMeasure(equalNodes())
		logger, _ := logrustest.NewNullLogger()
		planner, err := replication.NewRebalancePlanner(replication.RebalancePlannerParams{
			Logger:      logger,
			FSM:         fsm,
			Proposer:    proposer,
			Drafting:    drafting,
			Clock:       clockwork.NewFakeClockAt(plannerStart),
			Registerer:  registry,
			Schema:      sm.NewSchemaReader(),
			CurrentTerm: func() uint64 { return plannerTerm },
			ReadyToPlan: func() bool { return true },
			Enabled:     func() bool { return true },
			Measure: func(ctx context.Context, node, _ string, _ uint64) (types.RebalanceNodeMeasurement, error) {
				return measure(ctx, node)
			},
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

			// ReadyToPlan is the loop's first gate. Run handles one wake at a
			// time, so the second wake reaching the gate proves the first pass
			// ended.
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

func TestRebalancePlanner_Drafting(t *testing.T) {
	const (
		completed  = replication.DraftUnitCompleted
		inProgress = replication.DraftUnitInProgress
		pending    = replication.DraftUnitPending
	)
	createPlan2 := func(t *testing.T, h *plannerHarness) {
		require.NoError(t, h.fsm.CreatePlan(&replication.CreatePlanRequest{
			PlanID:          "plan-2",
			SourceNodes:     []string{"node-1"},
			TargetNodes:     []string{"node-2"},
			CreatedAtUnixMs: plannerStart.UnixMilli(),
		}))
	}
	stop := func(t *testing.T, h *plannerHarness) {
		require.NoError(t, h.fsm.StopPlan(&replication.StopPlanRequest{PlanID: plannerPlanID}))
	}
	deletePlan := func(t *testing.T, h *plannerHarness) {
		require.NoError(t, h.fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: plannerPlanID}))
	}
	// requireWaiting asserts the run measured nothing, proposed nothing and
	// counted no run, and the plan is still PLANNING.
	requireWaiting := func(t *testing.T, h *plannerHarness) {
		t.Helper()
		require.Zero(t, h.measureCalls.Load())
		reqs, _ := h.recorded()
		require.Empty(t, reqs)
		runSeries, err := testutil.GatherAndCount(h.registry, planningRunsMetric)
		require.NoError(t, err)
		require.Zero(t, runSeries)
		got, ok := h.fsm.GetPlan()
		require.True(t, ok)
		require.Equal(t, replication.PlanStatePlanning, got.State)
	}
	balancedBaseline := []replication.PlanNodeBalance{
		{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 100},
		{Node: "node-2", DiskUtilisation: node2Utilisation, ReplicaCount: 104},
	}

	t.Run("creates the task once", func(t *testing.T) {
		h := newPlannerHarness(t, fixedMeasure(equalNodes()), replication.NewMockrebalanceSchemaReader(t))
		h.setDraftTasks()
		h.drafting.EXPECT().AddRebalanceDraftTask(mock.Anything, plannerPlanID, []string{"node-1", "node-2"}).
			RunAndReturn(func(context.Context, string, []string) error {
				h.setDraftTasks(draftTask(plannerPlanID, false, "", pending, pending))
				return nil
			}).Once()

		h.planner.RunOnce(context.Background())
		h.planner.RunOnce(context.Background())

		requireWaiting(t, h)
	})

	for _, tc := range []struct {
		name    string
		setup   func(t *testing.T, h *plannerHarness)
		wantAdd string
	}{
		{
			name:  "stop ends drafting",
			setup: stop,
		},
		{
			name:  "delete after stop ends a still-running task",
			setup: func(t *testing.T, h *plannerHarness) { stop(t, h); deletePlan(t, h) },
		},
		{
			name: "replace after stop ends a still-running task",
			setup: func(t *testing.T, h *plannerHarness) {
				stop(t, h)
				createPlan2(t, h)
			},
			wantAdd: "plan-2",
		},
		{
			name: "sweep runs with the flag off",
			setup: func(t *testing.T, h *plannerHarness) {
				stop(t, h)
				h.rebalanceOn.Store(false)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newPlannerHarness(t, fixedMeasure(equalNodes()), replication.NewMockrebalanceSchemaReader(t))
			h.setDraftTasks(draftTask(plannerPlanID, false, "", completed, inProgress))
			tc.setup(t, h)
			var calls []string
			h.drafting.EXPECT().TerminateRebalanceDraftTask(mock.Anything, plannerPlanID, draftVersion).
				RunAndReturn(func(context.Context, string, uint64) error {
					calls = append(calls, "terminate "+plannerPlanID)
					h.setDraftTasks(draftTask(plannerPlanID, true, "", completed, inProgress))
					return nil
				}).Once()
			wantCalls := []string{"terminate " + plannerPlanID}
			if tc.wantAdd != "" {
				h.drafting.EXPECT().AddRebalanceDraftTask(mock.Anything, tc.wantAdd, []string{"node-1", "node-2"}).
					RunAndReturn(func(_ context.Context, planID string, _ []string) error {
						calls = append(calls, "add "+planID)
						h.setDraftTasks(append(h.draftTasks(), draftTask(planID, false, "", pending, pending))...)
						return nil
					}).Once()
				wantCalls = append(wantCalls, "add "+tc.wantAdd)
			}

			h.planner.RunOnce(context.Background())
			// The terminate marked the task ended, so the second run sweeps nothing.
			h.planner.RunOnce(context.Background())

			require.Equal(t, wantCalls, calls)
			require.Zero(t, h.measureCalls.Load())
			reqs, _ := h.recorded()
			require.Empty(t, reqs)
		})
	}

	t.Run("failed terminate adds no task", func(t *testing.T) {
		h := newPlannerHarness(t, fixedMeasure(equalNodes()), replication.NewMockrebalanceSchemaReader(t))
		stop(t, h)
		deletePlan(t, h)
		createPlan2(t, h)
		h.setDraftTasks(draftTask(plannerPlanID, false, "", completed, inProgress))
		h.drafting.EXPECT().TerminateRebalanceDraftTask(mock.Anything, plannerPlanID, draftVersion).
			Return(errors.New("no leader found")).Once()

		h.planner.RunOnce(context.Background())

		h.drafting.AssertNotCalled(t, "AddRebalanceDraftTask", mock.Anything, mock.Anything, mock.Anything)
		h.drafting.EXPECT().TerminateRebalanceDraftTask(mock.Anything, plannerPlanID, draftVersion).
			RunAndReturn(func(context.Context, string, uint64) error {
				h.setDraftTasks(draftTask(plannerPlanID, true, "", completed, inProgress))
				return nil
			}).Once()
		h.drafting.EXPECT().AddRebalanceDraftTask(mock.Anything, "plan-2", []string{"node-1", "node-2"}).Return(nil).Once()

		h.planner.RunOnce(context.Background())
	})

	t.Run("collects only after every report", func(t *testing.T) {
		rows := equalNodes()
		h := newPlannerHarness(t, func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
			m := rows[node]
			m.Shards = []types.RebalanceShardSize{{Collection: "Articles", Shard: node + "-shard", Bytes: 100}}
			return m, nil
		}, balancedLayout().mock(t))
		h.expectProposals()
		h.setDraftTasks(draftTask(plannerPlanID, false, "", completed, inProgress))

		h.planner.RunOnce(context.Background())
		requireWaiting(t, h)

		// A provider-only task stays SWAPPING after its last unit completes.
		h.clock.Advance(5 * time.Minute)
		h.setDraftTasks(draftTask(plannerPlanID, false, "", completed, completed))
		h.planner.RunOnce(context.Background())

		require.ElementsMatch(t, []measureCall{
			{node: "node-1", taskID: plannerPlanID, version: draftVersion},
			{node: "node-2", taskID: plannerPlanID, version: draftVersion},
		}, h.measuredCalls())
		req := requireRunAt(t, h, string(replication.PlanStateNotNeeded), plannerStart.Add(5*time.Minute))
		require.Equal(t, measuredRows(rows), req.Nodes, "draft sizes are stripped before proposing")
		requireBaseline(t, balancedBaseline, req.Baseline)
	})

	t.Run("reported despite task failure", func(t *testing.T) {
		h := newPlannerHarness(t, fixedMeasure(equalNodes()), balancedLayout().mock(t))
		h.expectProposals()
		h.setDraftTasks(draftTask(plannerPlanID, true, "post-completion callback failed", completed, completed))

		h.planner.RunOnce(context.Background())

		require.Equal(t, int32(2), h.measureCalls.Load())
		req := requireRun(t, h, string(replication.PlanStateNotNeeded))
		requireBaseline(t, balancedBaseline, req.Baseline)
	})

	t.Run("node restart while drafting", func(t *testing.T) {
		h := newPlannerHarness(t, fixedMeasure(equalNodes()), balancedLayout().mock(t))
		h.expectProposals()
		// A restart leaves node-2's unit IN_PROGRESS, and the node reports
		// nothing until it is back. Each wake meanwhile finds the task running.
		h.setDraftTasks(draftTask(plannerPlanID, false, "", completed, inProgress))
		for range 3 {
			h.planner.RunOnce(context.Background())
			requireWaiting(t, h)
			h.clock.Advance(3 * time.Minute)
		}

		h.setDraftTasks(reportedDraftTask(plannerPlanID))
		h.planner.RunOnce(context.Background())

		requireRunAt(t, h, string(replication.PlanStateNotNeeded), plannerStart.Add(9*time.Minute))
		got, ok := h.fsm.GetPlan()
		require.True(t, ok)
		require.Equal(t, replication.PlanStateNotNeeded, got.State)
	})

	t.Run("the highest task version decides", func(t *testing.T) {
		older := draftTask(plannerPlanID, true, "unit node-2 made no progress for 10m0s", completed, inProgress)
		older.Version = draftVersion - 1
		for _, tc := range []struct {
			name  string
			tasks []replication.DraftTask
		}{
			{name: "higher version listed last", tasks: []replication.DraftTask{older, reportedDraftTask(plannerPlanID)}},
			{name: "higher version listed first", tasks: []replication.DraftTask{reportedDraftTask(plannerPlanID), older}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				h := newPlannerHarness(t, fixedMeasure(equalNodes()), balancedLayout().mock(t))
				h.expectProposals()
				h.setDraftTasks(tc.tasks...)

				h.planner.RunOnce(context.Background())

				require.ElementsMatch(t, []measureCall{
					{node: "node-1", taskID: plannerPlanID, version: draftVersion},
					{node: "node-2", taskID: plannerPlanID, version: draftVersion},
				}, h.measuredCalls())
				requireRun(t, h, string(replication.PlanStateNotNeeded))
				got, ok := h.fsm.GetPlan()
				require.True(t, ok)
				require.Equal(t, replication.PlanStateNotNeeded, got.State)
			})
		}
	})

	t.Run("ended task fails the plan naming nodes", func(t *testing.T) {
		h := newPlannerHarness(t, fixedMeasure(equalNodes()), replication.NewMockrebalanceSchemaReader(t))
		h.expectProposals()
		// Units is a map, which has no iteration order. The Reason lists the
		// unfinished nodes sorted.
		units := map[string]replication.DraftUnitStatus{"node-01": completed}
		var unfinished []string
		for i := 2; i <= 25; i++ {
			node := fmt.Sprintf("node-%02d", i)
			units[node] = []replication.DraftUnitStatus{inProgress, pending}[i%2]
			unfinished = append(unfinished, node)
		}
		h.setDraftTasks(replication.DraftTask{
			PlanID:   plannerPlanID,
			Version:  draftVersion,
			Terminal: true,
			Error:    "unit node-02 made no progress for 10m0s",
			Units:    units,
		})

		h.planner.RunOnce(context.Background())

		require.Zero(t, h.measureCalls.Load())
		req := requireRun(t, h, string(replication.PlanStatePlanningFailed))
		require.Equal(t, replication.PlanStatePlanningFailed, req.Outcome)
		require.Equal(t, "drafting did not finish on nodes: "+strings.Join(unfinished, ", ")+
			": unit node-02 made no progress for 10m0s", req.Reason)
		got, ok := h.fsm.GetPlan()
		require.True(t, ok)
		require.Equal(t, replication.PlanStatePlanningFailed, got.State)
		require.Equal(t, req.Reason, got.Reason)
	})

	t.Run("collection retries", func(t *testing.T) {
		// In round 1 node-1 reports a fuller disk, which would unbalance the
		// baseline, and node-2 does not answer. Both answer from round 2 on.
		var calls nodeCalls
		h := newPlannerHarness(t, func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
			first := calls.next(node) == 1
			switch {
			case first && node == "node-2":
				return unreachable(ctx, node)
			case first:
				m := equalNodes()[node]
				m.DiskAvailableBytes = 100_000_000
				return m, nil
			}
			return equalNodes()[node], nil
		}, balancedLayout().mock(t))
		h.expectProposals()
		h.driveClock()

		h.planner.RunOnce(context.Background())

		require.Equal(t, int32(4), h.measureCalls.Load())
		// Round 1 ends on its 30s limit, and the backoff after it ends 30s later.
		req := requireRunAt(t, h, string(replication.PlanStateNotNeeded), plannerStart.Add(time.Minute))
		require.Equal(t, measuredRows(equalNodes()), req.Nodes)
		requireBaseline(t, balancedBaseline, req.Baseline)
	})

	t.Run("collection limit fails the plan naming nodes", func(t *testing.T) {
		// node-1 fails round 1 only, and node-2 fails every round.
		var calls nodeCalls
		h := newPlannerHarness(t, func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
			if calls.next(node) == 1 || node == "node-2" {
				return unreachable(ctx, node)
			}
			return equalNodes()[node], nil
		}, replication.NewMockrebalanceSchemaReader(t))
		h.expectProposals()
		h.driveClock()

		h.planner.RunOnce(context.Background())

		// Round k starts at 60s × (k−1). Round 11 is the first to end at
		// least 10 minutes after node-2's first failure.
		req := requireRunAt(t, h, string(replication.PlanStatePlanningFailed), plannerStart.Add(10*time.Minute))
		require.Equal(t, int32(22), h.measureCalls.Load())
		require.Equal(t, "could not collect drafts from nodes: node-2", req.Reason)
		require.Equal(t, []types.RebalanceNodeMeasurement{
			measuredRows(equalNodes())[0],
			{Node: "node-2", Error: "node-2 did not answer before the deadline"},
		}, req.Nodes)
	})

	t.Run("stop between collection rounds proposes nothing", func(t *testing.T) {
		stopPlan := func(h *plannerHarness) error {
			return h.fsm.StopPlan(&replication.StopPlanRequest{PlanID: plannerPlanID})
		}
		stopDelete := func(h *plannerHarness) error {
			return errors.Join(stopPlan(h), h.fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: plannerPlanID}))
		}
		for _, tc := range []struct {
			name string
			// change runs inside a Measure call, off the test goroutine.
			change func(h *plannerHarness) error
			// wantPlan is the plan's ID afterwards, or "" for no plan.
			wantPlan  string
			wantState replication.PlanState
		}{
			{
				name:      "stop",
				change:    stopPlan,
				wantPlan:  plannerPlanID,
				wantState: replication.PlanStateStopped,
			},
			{
				name:   "stop then delete",
				change: stopDelete,
			},
			{
				name: "stop, delete, then create of a second plan",
				change: func(h *plannerHarness) error {
					return errors.Join(stopDelete(h), h.fsm.CreatePlan(&replication.CreatePlanRequest{
						PlanID:          "plan-2",
						SourceNodes:     []string{"node-1"},
						TargetNodes:     []string{"node-2"},
						CreatedAtUnixMs: plannerStart.UnixMilli(),
					}))
				},
				wantPlan:  "plan-2",
				wantState: replication.PlanStatePlanning,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// node-2 makes the change in round 1 and then fails that round.
				// Every later call answers, so a run that misses the change
				// goes on to plan.
				var calls nodeCalls
				var h *plannerHarness
				h = newPlannerHarness(t, func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
					if calls.next(node) > 1 || node == "node-1" {
						return equalNodes()[node], nil
					}
					if err := tc.change(h); err != nil {
						return types.RebalanceNodeMeasurement{}, err
					}
					return unreachable(ctx, node)
				}, replication.NewMockrebalanceSchemaReader(t))
				h.driveClock()

				h.planner.RunOnce(context.Background())

				require.Equal(t, int32(2), h.measureCalls.Load())
				reqs, _ := h.recorded()
				require.Empty(t, reqs)
				requireCounted(t, h, "STOPPED")
				got, ok := h.fsm.GetPlan()
				require.Equal(t, tc.wantPlan != "", ok)
				require.Equal(t, tc.wantPlan, got.ID)
				require.Equal(t, tc.wantState, got.State)
			})
		}
	})

	t.Run("leadership lost between collection rounds proposes nothing", func(t *testing.T) {
		// node-2 fails round 1 and every later call answers. A run that misses
		// the lost leadership goes on to plan, and the schema mock, which
		// expects no calls, fails the test.
		var calls nodeCalls
		h := newPlannerHarness(t, func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
			if node == "node-2" && calls.next(node) == 1 {
				return unreachable(ctx, node)
			}
			return equalNodes()[node], nil
		}, replication.NewMockrebalanceSchemaReader(t))
		// The planner first waits on round 1's attempt limit, then on the
		// backoff. Leadership is lost while the backoff wait is pending.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		logger, _ := logrustest.NewNullLogger()
		enterrors.GoWrapper(func() {
			for wait := 1; h.clock.BlockUntilContext(ctx, 1) == nil; wait++ {
				if wait == 2 {
					h.leader.Store(false)
				}
				h.clock.Advance(30 * time.Second)
			}
		}, logger)

		h.planner.RunOnce(context.Background())

		require.Equal(t, int32(2), h.measureCalls.Load())
		reqs, _ := h.recorded()
		require.Empty(t, reqs)
		requireCounted(t, h, "STALE")
		got, ok := h.fsm.GetPlan()
		require.True(t, ok)
		require.Equal(t, replication.PlanStatePlanning, got.State)
	})

	t.Run("failed collection round is logged", func(t *testing.T) {
		rows := equalNodes()
		rows["node-3"] = rows["node-1"]
		// node-1 and node-3 fail round 1, and node-2 answers. All answer in round 2.
		errs := map[string]string{"node-1": "disk read failed", "node-3": "connection refused"}
		var calls nodeCalls
		h := newPlannerHarness(t, func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
			if calls.next(node) > 1 || node == "node-2" {
				return rows[node], nil
			}
			<-ctx.Done()
			return types.RebalanceNodeMeasurement{}, errors.New(errs[node])
		}, balancedLayout().mock(t))
		stop(t, h)
		deletePlan(t, h)
		require.NoError(t, h.fsm.CreatePlan(&replication.CreatePlanRequest{
			PlanID:          plannerPlanID,
			SourceNodes:     []string{"node-1"},
			TargetNodes:     []string{"node-2", "node-3"},
			CreatedAtUnixMs: plannerStart.UnixMilli(),
		}))
		task := reportedDraftTask(plannerPlanID)
		task.Units["node-3"] = completed
		h.setDraftTasks(task)
		h.expectProposals()
		h.driveClock()

		h.planner.RunOnce(context.Background())

		reqs, _ := h.recorded()
		require.Len(t, reqs, 1)
		warned := warnings(h)
		require.Len(t, warned, 1)
		for node, err := range errs {
			require.Contains(t, warned[0].Message, node)
			require.Contains(t, warned[0].Message, err)
		}
		require.NotContains(t, warned[0].Message, "node-2")
	})

	t.Run("collection backoff doubles from 1s to a 30s cap", func(t *testing.T) {
		const rounds = 8
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// starts[k] is when round k+1 began: its attempt deadline minus the 30s
		// measurementTimeout.
		var starts []time.Time
		var calls nodeCalls
		h := newPlannerHarness(t, func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error) {
			if node == "node-1" {
				deadline, _ := ctx.Deadline()
				starts = append(starts, deadline.Add(-30*time.Second))
				if calls.next(node) == rounds {
					cancel()
				}
				return equalNodes()[node], nil
			}
			return unreachable(ctx, node)
		}, replication.NewMockrebalanceSchemaReader(t))
		// The planner holds one timer at a time: a round's 30s attempt limit,
		// then its backoff. Each step advances 1s, so every wait ends on its
		// exact second.
		logger, _ := logrustest.NewNullLogger()
		enterrors.GoWrapper(func() {
			for h.clock.BlockUntilContext(ctx, 1) == nil {
				h.clock.Advance(time.Second)
			}
		}, logger)

		h.planner.RunOnce(ctx)

		require.Len(t, starts, rounds)
		require.Equal(t, plannerStart, starts[0])
		var backoffs []time.Duration
		for k := 1; k < rounds; k++ {
			backoffs = append(backoffs, starts[k].Sub(starts[k-1])-30*time.Second)
		}
		require.Equal(t, []time.Duration{
			time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
			16 * time.Second, 30 * time.Second, 30 * time.Second,
		}, backoffs)
		requireCounted(t, h, "CANCELLED")
	})
}

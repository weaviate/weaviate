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

package rebalance_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
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
	"github.com/weaviate/weaviate/wl/rebalance"
	"github.com/weaviate/weaviate/wl/rebalance/rebalanceplanner"
)

const (
	planningRunsMetric     = "weaviate_replication_rebalance_planning_runs_total"
	planningDurationMetric = "weaviate_replication_rebalance_planning_duration_seconds"

	plannerPlanID        = "plan-1"
	plannerTerm   uint64 = 9
	// draftVersion is the version of every draft task the tests serve.
	draftVersion uint64 = 7

	noAcceptablePlanReason = "disk utilisation is unbalanced, and no direct move from a source to a target that fits the target's capacity narrows its spread"
	inFlightBalancedReason = "in-flight replication operations will balance the cluster"
	baselineBalancedReason = "no source's disk utilisation exceeds any target's by more than the balance tolerance of 0.10"
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
	planner  *rebalance.RebalancePlanner
	fsm      *replication.ShardReplicationFSM
	proposer *rebalance.MockrebalanceProposer
	drafting *rebalance.MockrebalanceDrafting
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
func newPlannerHarness(t *testing.T, measure measureFunc, schemaReader *rebalance.MockrebalanceSchemaReader) *plannerHarness {
	t.Helper()
	logger, logs := logrustest.NewNullLogger()

	h := &plannerHarness{
		t:        t,
		logs:     logs,
		registry: prometheus.NewPedanticRegistry(),
		clock:    clockwork.NewFakeClockAt(plannerStart),
		proposer: rebalance.NewMockrebalanceProposer(t),
		drafting: rebalance.NewMockrebalanceDrafting(t),
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

	params := rebalance.RebalancePlannerParams{
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
	planner, err := rebalance.NewRebalancePlanner(params)
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

// measuredRows returns rows as the planner records them when the plan lists
// nodes in name order. Each row has Node set and its draft (Shards) removed.
func measuredRows(rows map[string]types.RebalanceNodeMeasurement) []types.RebalanceNodeMeasurement {
	out := make([]types.RebalanceNodeMeasurement, 0, len(rows))
	for _, node := range slices.Sorted(maps.Keys(rows)) {
		m := rows[node]
		m.Node, m.Shards = node, nil
		out = append(out, m)
	}
	return out
}

// diskUsable is the usable bytes of every node diskRows returns.
const diskUsable = 800_000_000.0

// diskRows returns rows for node-1, node-2 and so on. The nth value of used is
// the bytes used on node-n. Every node has a 1GB disk with an 80% warning, the
// same memory limit and one replication worker. So a target keeps free only
// the size of the largest replica moved onto it.
func diskRows(used ...int64) map[string]types.RebalanceNodeMeasurement {
	rows := make(map[string]types.RebalanceNodeMeasurement, len(used))
	for i, u := range used {
		rows[fmt.Sprintf("node-%d", i+1)] = types.RebalanceNodeMeasurement{
			DiskTotalBytes: 1_000_000_000, DiskAvailableBytes: 1_000_000_000 - u,
			DiskWarningPercentage: 80, MemoryLimitBytes: 8_000_000_000, ReplicationWorkers: 1,
		}
	}
	return rows
}

// drafted adds shards to node's draft in rows and returns rows.
func drafted(rows map[string]types.RebalanceNodeMeasurement, node string, shards ...types.RebalanceShardSize) map[string]types.RebalanceNodeMeasurement {
	m := rows[node]
	m.Shards = append(m.Shards, shards...)
	rows[node] = m
	return rows
}

// shardSizes returns draft sizes for class's shards in layout group group.
// Shard g<group>-shard<i> has bytes[i].
func shardSizes(class string, group int, bytes ...int64) []types.RebalanceShardSize {
	out := make([]types.RebalanceShardSize, len(bytes))
	for i, b := range bytes {
		out[i] = types.RebalanceShardSize{Collection: class, Shard: fmt.Sprintf("g%d-shard%d", group, i), Bytes: b}
	}
	return out
}

// spreadGroup is the layout group of the ten Articles shards spreadRows drafts.
var spreadGroup = shardGroup{count: 10, nodes: []string{"node-1"}}

// spreadRows has 700MB used on node-1 and 400MB on node-2, so disk utilisation
// is unbalanced. node-1 drafts the spreadGroup shards at 16MB to 25MB.
func spreadRows() map[string]types.RebalanceNodeMeasurement {
	return drafted(diskRows(700_000_000, 400_000_000), "node-1", shardSizes("Articles", 0,
		16_000_000, 17_000_000, 18_000_000, 19_000_000, 20_000_000,
		21_000_000, 22_000_000, 23_000_000, 24_000_000, 25_000_000)...)
}

// transfer carries bytes and one replica from one node to another. An empty
// from or to leaves that side unchanged.
type transfer struct {
	from, to string
	bytes    int64
}

func transfersOf(moves []replication.PlanMove) []transfer {
	out := make([]transfer, len(moves))
	for i, m := range moves {
		out[i] = transfer{from: m.SourceNode, to: m.TargetNode, bytes: m.EstimatedBytes}
	}
	return out
}

// goalAfter returns one row per baseline node. Each row holds the node's disk
// utilisation (from rows) and replica count (from baseline) after every
// transfer.
func goalAfter(rows map[string]types.RebalanceNodeMeasurement, baseline []types.RebalanceNodeBalance, transfers ...transfer) []types.RebalanceNodeBalance {
	used := make(map[string]int64, len(baseline))
	counts := make(map[string]int, len(baseline))
	for _, b := range baseline {
		used[b.Node] = rows[b.Node].DiskTotalBytes - rows[b.Node].DiskAvailableBytes
		counts[b.Node] = b.ReplicaCount
	}
	for _, tr := range transfers {
		if tr.to != "" {
			used[tr.to] += tr.bytes
			counts[tr.to]++
		}
		if tr.from != "" {
			used[tr.from] -= tr.bytes
			counts[tr.from]--
		}
	}
	out := make([]types.RebalanceNodeBalance, len(baseline))
	for i, b := range baseline {
		r := rows[b.Node]
		usable := float64(r.DiskTotalBytes) * float64(r.DiskWarningPercentage) / 100
		out[i] = types.RebalanceNodeBalance{Node: b.Node, DiskUtilisation: float64(used[b.Node]) / usable, ReplicaCount: counts[b.Node]}
	}
	return out
}

// withoutIDs returns moves with their IDs cleared. Move IDs are random, so
// moves of two runs compare without them.
func withoutIDs(moves []replication.PlanMove) []replication.PlanMove {
	moves = slices.Clone(moves)
	for i := range moves {
		moves[i].ID = ""
	}
	return moves
}

// advance takes op id through states, in order, as the replication engine
// reports them.
func (h *plannerHarness) advance(id uint64, states ...api.ShardReplicationState) {
	h.t.Helper()
	for _, state := range states {
		require.NoError(h.t, h.fsm.UpdateReplicationOpStatus(&api.ReplicationUpdateOpStateRequest{Id: id, State: state}))
	}
}

// cancel moves op id to CANCELLED by applying the cancel command and then the
// replication engine's cancellation-complete command.
func (h *plannerHarness) cancel(id uint64) {
	h.t.Helper()
	opUUID, err := h.fsm.GetReplicationOpUUIDFromId(id)
	require.NoError(h.t, err)
	require.NoError(h.t, h.fsm.CancelReplication(&api.ReplicationCancelRequest{Uuid: opUUID}))
	require.NoError(h.t, h.fsm.CancellationComplete(&api.ReplicationCancellationCompleteRequest{Id: id}))
}

// requireSameAsNoOpRun runs the planner on rows and states with no replication
// op. It asserts that req has the same moves (ignoring IDs) and the same goal
// as that run.
func requireSameAsNoOpRun(t *testing.T, req replication.CompletePlanningRequest, rows map[string]types.RebalanceNodeMeasurement, states *schemaFixture) {
	t.Helper()
	noOp := newPlannerHarness(t, fixedMeasure(rows), states.mock(t))
	noOp.expectProposals()
	noOp.planner.RunOnce(context.Background())
	want := noOp.proposal()
	require.Equal(t, replication.PlanStateProposed, want.Outcome, want.Reason)
	require.Equal(t, withoutIDs(want.Moves), withoutIDs(req.Moves))
	requireBaseline(t, want.Goal, req.Goal)
}

// replicate registers an in-flight op that transfers Articles shard from one
// node to another.
func (h *plannerHarness) replicate(id uint64, transferType api.ShardReplicationTransferType, shard, from, to string) {
	h.t.Helper()
	require.NoError(h.t, h.fsm.Replicate(id, &api.ReplicationReplicateShardRequest{
		Uuid:             strfmt.UUID(uuid.NewString()),
		SourceNode:       from,
		SourceCollection: "Articles",
		SourceShard:      shard,
		TargetNode:       to,
		TransferType:     string(transferType),
	}))
}

// replacePlan replaces the harness's plan with a PLANNING plan of the same ID
// listing sources, targets and collections, and serves a reported draft task
// for its nodes.
func (h *plannerHarness) replacePlan(sources, targets, collections []string) {
	h.t.Helper()
	require.NoError(h.t, h.fsm.StopPlan(&replication.StopPlanRequest{PlanID: plannerPlanID}))
	require.NoError(h.t, h.fsm.DeletePlan(&replication.DeletePlanRequest{PlanID: plannerPlanID}))
	require.NoError(h.t, h.fsm.CreatePlan(&replication.CreatePlanRequest{
		PlanID:          plannerPlanID,
		SourceNodes:     sources,
		TargetNodes:     targets,
		Collections:     collections,
		CreatedAtUnixMs: plannerStart.UnixMilli(),
	}))
	task := reportedDraftTask(plannerPlanID)
	for _, node := range slices.Concat(sources, targets) {
		task.Units[node] = replication.DraftUnitCompleted
	}
	h.setDraftTasks(task)
}

// proposal returns the run's only request.
func (h *plannerHarness) proposal() replication.CompletePlanningRequest {
	reqs, _ := h.recorded()
	require.Len(h.t, reqs, 1)
	return reqs[0]
}

func requireProposal(t *testing.T, plan replication.Plan, req replication.CompletePlanningRequest) {
	t.Helper()
	require.NotEmpty(t, req.Moves)
	require.Equal(t, len(req.Moves), req.MoveCount)
	ids := make(map[string]struct{}, len(req.Moves))
	for _, m := range req.Moves {
		require.NotEmpty(t, m.ID)
		ids[m.ID] = struct{}{}
	}
	require.Len(t, ids, len(req.Moves), "move IDs are unique")
	require.Len(t, req.Goal, len(req.Nodes))
	require.Equal(t, plannerStart.Add(time.Hour).UnixMilli(), req.ApproveBeforeUnixMs)

	hashed, err := json.Marshal(struct {
		SourceNodes []string               `json:"sourceNodes"`
		TargetNodes []string               `json:"targetNodes"`
		Collections []string               `json:"collections"`
		Namespaces  []string               `json:"namespaces"`
		Moves       []replication.PlanMove `json:"moves"`
	}{plan.SourceNodes, plan.TargetNodes, plan.Collections, plan.Namespaces, req.Moves})
	require.NoError(t, err)
	sum := sha256.Sum256(hashed)
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, req.PlanHash)
	require.Equal(t, "sha256:"+hex.EncodeToString(sum[:]), req.PlanHash)
}

// requireNoMoves asserts that a request or plan that is not PROPOSED holds no
// moves, goal, move count, hash or approval deadline.
func requireNoMoves(t *testing.T, moves []replication.PlanMove, goal []types.RebalanceNodeBalance, count int, hash string, approveBefore int64) {
	t.Helper()
	require.Empty(t, moves)
	require.Empty(t, goal)
	require.Zero(t, count)
	require.Empty(t, hash)
	require.Zero(t, approveBefore)
}

type shardGroup struct {
	count  int
	status string
	nodes  []string
}

// schemaFixture holds the sharding states a schema mock serves. A collection
// in readErrs is listed, and reading it returns its error.
type schemaFixture struct {
	states        map[string]sharding.State
	readErrs      map[string]error
	optionalReads bool
}

// mock returns a schema mock that serves f and expects ReadSchema and a Read
// of every listed collection. ReadSchema lists collections in sorted order.
// Read is expected only with retryIfClassNotFound=false, because a real reader
// retries a missing class with backoff before failing.
// With optionalReads set, the mock serves each Read but does not require it.
func (f *schemaFixture) mock(t *testing.T) *rebalance.MockrebalanceSchemaReader {
	m := rebalance.NewMockrebalanceSchemaReader(t)
	classes := append(slices.Collect(maps.Keys(f.states)), slices.Collect(maps.Keys(f.readErrs))...)
	slices.Sort(classes)
	m.EXPECT().ReadSchema(mock.Anything).RunAndReturn(func(reader func(models.Class, uint64)) error {
		for _, class := range classes {
			reader(models.Class{Class: class}, 0)
		}
		return nil
	})
	for class, state := range f.states {
		read := m.EXPECT().Read(class, false, mock.Anything).
			RunAndReturn(func(_ string, _ bool, reader func(*models.Class, *sharding.State) error) error {
				return reader(&models.Class{Class: class}, &state)
			})
		if f.optionalReads {
			read.Maybe()
		}
	}
	for class, err := range f.readErrs {
		read := m.EXPECT().Read(class, false, mock.Anything).Return(err)
		if f.optionalReads {
			read.Maybe()
		}
	}
	return m
}

// unread marks f's sharding reads as optional, for a run that may end before
// reading them.
func (f *schemaFixture) unread() *schemaFixture {
	f.optionalReads = true
	return f
}

// failing adds a listed collection whose read returns err.
func (f *schemaFixture) failing(class string, err error) *schemaFixture {
	if f.readErrs == nil {
		f.readErrs = map[string]error{}
	}
	f.readErrs[class] = err
	return f
}

// layout returns the collection Articles holding count shards per group, each
// shard with a replica on every node of its group.
func layout(groups ...shardGroup) *schemaFixture {
	return (&schemaFixture{}).withClass("Articles", groups...)
}

// withClass adds the collection class, laid out as layout lays out Articles.
func (f *schemaFixture) withClass(class string, groups ...shardGroup) *schemaFixture {
	physical := map[string]sharding.Physical{}
	for g, group := range groups {
		for i := range group.count {
			name := fmt.Sprintf("g%d-shard%d", g, i)
			physical[name] = sharding.Physical{Name: name, BelongsToNodes: group.nodes, Status: group.status}
		}
	}
	if f.states == nil {
		f.states = map[string]sharding.State{}
	}
	f.states[class] = sharding.State{Physical: physical}
	return f
}

// balancedLayout puts 100 replicas on node-1 and 104 on node-2. The replica
// counts in the wanted baseline catch a miscount. Counting the FROZEN shards
// would give node-1 130. Skipping the COLD ones would give node-2 100.
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

func requireBaseline(t *testing.T, want, got []types.RebalanceNodeBalance) {
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
	// The "finished op is not projected" rows share these fixtures, one row
	// per end state. Each row ends a MOVE of node-1's 90MB g1 shard. Projecting
	// the op would shift 90MB to node-2 and make g1 ineligible, so the run must
	// match a run with no op.
	finishedRows := func() map[string]types.RebalanceNodeMeasurement {
		return drafted(spreadRows(), "node-1", shardSizes("Articles", 1, 90_000_000)...)
	}
	finishedLayout := func() *schemaFixture {
		return layout(spreadGroup, shardGroup{count: 1, nodes: []string{"node-1"}})
	}
	finishedSetup := func(finish func(h *plannerHarness)) func(t *testing.T, h *plannerHarness) {
		return func(t *testing.T, h *plannerHarness) {
			h.replicate(1, api.MOVE, "g1-shard0", "node-1", "node-2")
			finish(h)
		}
	}
	finishedBaseline := []types.RebalanceNodeBalance{
		{Node: "node-1", DiskUtilisation: 700_000_000 / diskUsable, ReplicaCount: 11},
		{Node: "node-2", DiskUtilisation: 400_000_000 / diskUsable},
	}
	finishedCheck := func(t *testing.T, h *plannerHarness) {
		requireSameAsNoOpRun(t, h.proposal(), finishedRows(), finishedLayout())
	}

	t.Run("baseline reason states the tolerance", func(t *testing.T) {
		require.Contains(t, baselineBalancedReason, fmt.Sprintf("%.2f", rebalanceplanner.BalanceTolerance))
	})

	cases := []struct {
		name   string
		rows   map[string]types.RebalanceNodeMeasurement
		states *schemaFixture
		// noSchemaRead marks a run that ends before counting replicas; its
		// schema mock fails the test on any call.
		noSchemaRead bool
		// listsSchemaOnce, when set, requires exactly one ReadSchema call.
		listsSchemaOnce bool
		// measure, when set, replaces fixedMeasure(rows).
		measure    func(h *plannerHarness) measureFunc
		nilMeasure bool
		setup      func(t *testing.T, h *plannerHarness)

		wantOutcome replication.PlanState
		wantResult  string
		wantReason  string
		// wantReasonParts, when set, lists substrings the reason must
		// contain, and wantReason is not checked.
		wantReasonParts []string
		// wantReasonAbsent lists substrings the reason must not contain.
		wantReasonAbsent []string
		wantNodes        []types.RebalanceNodeMeasurement
		wantBaseline     []types.RebalanceNodeBalance
		// wantMeasuredAt, when set, is the expected measurement time instead
		// of plannerStart.
		wantMeasuredAt time.Time
		check          func(t *testing.T, h *plannerHarness)
	}{
		{
			name:        "balanced",
			wantOutcome: replication.PlanStateNotNeeded,
			wantReason:  baselineBalancedReason,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 100},
				{Node: "node-2", DiskUtilisation: node2Utilisation, ReplicaCount: 104},
			},
			check: func(t *testing.T, h *plannerHarness) { require.Empty(t, warnings(h)) },
		},
		{
			name: "skewed replica counts with level disk need no plan",
			// Replica counts 80 and 40; disk utilisation as in balanced.
			states: layout(
				shardGroup{count: 40, nodes: []string{"node-1", "node-2"}},
				shardGroup{count: 40, nodes: []string{"node-1"}},
			),
			wantOutcome: replication.PlanStateNotNeeded,
			wantReason:  baselineBalancedReason,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 80},
				{Node: "node-2", DiskUtilisation: node2Utilisation, ReplicaCount: 40},
			},
		},
		{
			name: "source emptier than its target needs no plan",
			// node-1 at 0.75 is emptier than node-2 at 0.95.
			rows: drafted(withNode2(func(m *types.RebalanceNodeMeasurement) { m.DiskAvailableBytes = 250_000_000 }),
				"node-1", shardSizes("Articles", 0, 1_000_000, 1_000_000)...),
			states:      layout(shardGroup{count: 2, nodes: []string{"node-1"}}),
			wantOutcome: replication.PlanStateNotNeeded,
			wantReason:  baselineBalancedReason,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 2},
				{Node: "node-2", DiskUtilisation: 790_000_000.0 / 832_000_000.0},
			},
		},
		{
			name: "no move narrows the spread",
			// Utilisation 0.875 and 0.625. Moving either node-1 replica needs
			// 500MB + 2 × its size on node-2. That is more than node-2's 800MB
			// usable.
			rows:        drafted(diskRows(700_000_000, 500_000_000), "node-1", shardSizes("Articles", 0, 160_000_000, 200_000_000)...),
			states:      layout(shardGroup{count: 2, nodes: []string{"node-1"}}),
			wantOutcome: replication.PlanStateNoAcceptablePlan,
			wantReason:  noAcceptablePlanReason,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: 700_000_000 / diskUsable, ReplicaCount: 2},
				{Node: "node-2", DiskUtilisation: 500_000_000 / diskUsable},
			},
		},
		{
			name:        "unbalanced cluster gets a plan",
			rows:        spreadRows(),
			states:      layout(spreadGroup),
			wantOutcome: replication.PlanStateProposed,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: 700_000_000 / diskUsable, ReplicaCount: 10},
				{Node: "node-2", DiskUtilisation: 400_000_000 / diskUsable},
			},
			check: func(t *testing.T, h *plannerHarness) {
				req := h.proposal()
				// Searching the same layout directly gives the expected moves,
				// sized from node-1's draft. The plan must keep their order.
				in := rebalanceplanner.Input{
					Nodes: []rebalanceplanner.Node{
						{Name: "node-1", Role: rebalanceplanner.Source, UsedBytes: 700_000_000, UsableBytes: diskUsable, Workers: 1},
						{Name: "node-2", Role: rebalanceplanner.Target, UsedBytes: 400_000_000, UsableBytes: diskUsable, Workers: 1},
					},
				}
				for _, s := range spreadRows()["node-1"].Shards {
					in.Replicas = append(in.Replicas, rebalanceplanner.Replica{
						Collection: s.Collection, Shard: s.Shard, Node: "node-1", Bytes: s.Bytes, Eligible: true,
					})
				}
				res, err := rebalanceplanner.Search(context.Background(), in)
				require.NoError(t, err)
				require.Equal(t, rebalanceplanner.MovesFound, res.Verdict)
				want := make([]replication.PlanMove, len(res.Moves))
				for i, m := range res.Moves {
					want[i] = replication.PlanMove{
						Collection: m.Collection, Shard: m.Shard, SourceNode: m.SourceNode, TargetNode: m.TargetNode, EstimatedBytes: m.Bytes,
					}
				}
				require.Greater(t, len(want), 1, "a single move has no order to check")
				require.Equal(t, want, withoutIDs(req.Moves))
				require.True(t, rebalanceplanner.Balanced(req.Goal, []string{"node-1"}))

				create := func(id string, at int64) error {
					return h.fsm.CreatePlan(&replication.CreatePlanRequest{
						PlanID:          id,
						SourceNodes:     []string{"node-1"},
						TargetNodes:     []string{"node-2"},
						CreatedAtUnixMs: at,
					})
				}
				require.ErrorIs(t, create("plan-2", req.ApproveBeforeUnixMs-1), replication.ErrPlanExists)
				require.NoError(t, create("plan-3", req.ApproveBeforeUnixMs))
				got, ok := h.fsm.GetPlan()
				require.True(t, ok)
				require.Equal(t, "plan-3", got.ID)
			},
		},
		{
			name: "in-flight MOVE projected",
			// node-1's 90MB g1 shard is moving to node-2. The projection,
			// 610MB and 490MB, is still unbalanced.
			rows:        drafted(spreadRows(), "node-1", shardSizes("Articles", 1, 90_000_000)...),
			states:      layout(spreadGroup, shardGroup{count: 1, nodes: []string{"node-1"}}),
			setup:       func(t *testing.T, h *plannerHarness) { h.replicate(1, api.MOVE, "g1-shard0", "node-1", "node-2") },
			wantOutcome: replication.PlanStateProposed,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: 700_000_000 / diskUsable, ReplicaCount: 11},
				{Node: "node-2", DiskUtilisation: 400_000_000 / diskUsable},
			},
			check: func(t *testing.T, h *plannerHarness) {
				req := h.proposal()
				rows := diskRows(700_000_000, 400_000_000)
				inFlight := transfer{from: "node-1", to: "node-2", bytes: 90_000_000}
				requireBaseline(t, goalAfter(rows, req.Baseline, append([]transfer{inFlight}, transfersOf(req.Moves)...)...), req.Goal)
				for _, m := range req.Moves {
					require.NotEqual(t, "g1-shard0", m.Shard)
				}
			},
		},
		{
			name: "in-flight COPY projected",
			// node-1's 100MB g1 shard is being copied to node-3, and stays on
			// node-1. Without the in-flight op, moving it to node-2 would be
			// the search's first pick.
			rows: drafted(drafted(diskRows(700_000_000, 400_000_000, 400_000_000), "node-1", spreadRows()["node-1"].Shards...),
				"node-1", shardSizes("Articles", 1, 100_000_000)...),
			states: layout(spreadGroup, shardGroup{count: 1, nodes: []string{"node-1"}}),
			setup: func(t *testing.T, h *plannerHarness) {
				h.replacePlan([]string{"node-1"}, []string{"node-2", "node-3"}, nil)
				h.replicate(1, api.COPY, "g1-shard0", "node-1", "node-3")
			},
			wantOutcome: replication.PlanStateProposed,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: 700_000_000 / diskUsable, ReplicaCount: 11},
				{Node: "node-2", DiskUtilisation: 400_000_000 / diskUsable},
				{Node: "node-3", DiskUtilisation: 400_000_000 / diskUsable},
			},
			check: func(t *testing.T, h *plannerHarness) {
				req := h.proposal()
				rows := diskRows(700_000_000, 400_000_000, 400_000_000)
				inFlight := transfer{to: "node-3", bytes: 100_000_000}
				requireBaseline(t, goalAfter(rows, req.Baseline, append([]transfer{inFlight}, transfersOf(req.Moves)...)...), req.Goal)
				for _, m := range req.Moves {
					require.NotEqual(t, "g1-shard0", m.Shard)
				}
			},
		},
		{
			name: "in-flight ops balance the cluster",
			// Measured 700MB and 400MB; the 150MB MOVE levels both at 550MB.
			rows:        drafted(diskRows(700_000_000, 400_000_000), "node-1", shardSizes("Articles", 0, 150_000_000)...),
			states:      layout(shardGroup{count: 1, nodes: []string{"node-1"}}),
			setup:       func(t *testing.T, h *plannerHarness) { h.replicate(1, api.MOVE, "g0-shard0", "node-1", "node-2") },
			wantOutcome: replication.PlanStateNotNeeded,
			wantReason:  inFlightBalancedReason,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: 700_000_000 / diskUsable, ReplicaCount: 1},
				{Node: "node-2", DiskUtilisation: 400_000_000 / diskUsable},
			},
		},
		{
			name: "in-flight MOVE onto a target that already holds the replica",
			// The schema already lists node-2 for the moving 90MB g1 shard, so
			// node-2's measured disk holds it. Adding 90MB on node-2 as well
			// would change its goal utilisation.
			rows:        drafted(spreadRows(), "node-1", shardSizes("Articles", 1, 90_000_000)...),
			states:      layout(spreadGroup, shardGroup{count: 1, nodes: []string{"node-1", "node-2"}}),
			setup:       func(t *testing.T, h *plannerHarness) { h.replicate(1, api.MOVE, "g1-shard0", "node-1", "node-2") },
			wantOutcome: replication.PlanStateProposed,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: 700_000_000 / diskUsable, ReplicaCount: 11},
				{Node: "node-2", DiskUtilisation: 400_000_000 / diskUsable, ReplicaCount: 1},
			},
			check: func(t *testing.T, h *plannerHarness) {
				req := h.proposal()
				inFlight := transfer{from: "node-1", bytes: 90_000_000}
				requireBaseline(t, goalAfter(diskRows(700_000_000, 400_000_000), req.Baseline,
					append([]transfer{inFlight}, transfersOf(req.Moves)...)...), req.Goal)
			},
		},
		{
			name: "in-flight MOVE whose source no longer holds the replica",
			// The schema lists the moving g1 shard on node-2 only, so both
			// measured disks already reflect the MOVE. node-1's draft still
			// sizes the shard at 90MB, which the op must not apply to either node.
			rows:        drafted(spreadRows(), "node-1", shardSizes("Articles", 1, 90_000_000)...),
			states:      layout(spreadGroup, shardGroup{count: 1, nodes: []string{"node-2"}}),
			setup:       func(t *testing.T, h *plannerHarness) { h.replicate(1, api.MOVE, "g1-shard0", "node-1", "node-2") },
			wantOutcome: replication.PlanStateProposed,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: 700_000_000 / diskUsable, ReplicaCount: 10},
				{Node: "node-2", DiskUtilisation: 400_000_000 / diskUsable, ReplicaCount: 1},
			},
			check: func(t *testing.T, h *plannerHarness) {
				req := h.proposal()
				requireBaseline(t, goalAfter(diskRows(700_000_000, 400_000_000), req.Baseline, transfersOf(req.Moves)...), req.Goal)
			},
		},
		{
			name:         "finished op is not projected/READY",
			rows:         finishedRows(),
			states:       finishedLayout(),
			setup:        finishedSetup(func(h *plannerHarness) { h.advance(1, api.HYDRATING, api.FINALIZING, api.READY) }),
			wantOutcome:  replication.PlanStateProposed,
			wantBaseline: finishedBaseline,
			check:        finishedCheck,
		},
		{
			name:         "finished op is not projected/CANCELLED",
			rows:         finishedRows(),
			states:       finishedLayout(),
			setup:        finishedSetup(func(h *plannerHarness) { h.advance(1, api.HYDRATING); h.cancel(1) }),
			wantOutcome:  replication.PlanStateProposed,
			wantBaseline: finishedBaseline,
			check:        finishedCheck,
		},
		{
			name: "in-flight MOVE onto a source",
			// node-2's 150MB g1 shard is moving to node-1, the plan's source:
			// 850MB and 250MB once projected. If the arriving replica were
			// eligible, moving it back would be the search's first pick.
			rows:        drafted(spreadRows(), "node-2", shardSizes("Articles", 1, 150_000_000)...),
			states:      layout(spreadGroup, shardGroup{count: 1, nodes: []string{"node-2"}}),
			setup:       func(t *testing.T, h *plannerHarness) { h.replicate(1, api.MOVE, "g1-shard0", "node-2", "node-1") },
			wantOutcome: replication.PlanStateProposed,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: 700_000_000 / diskUsable, ReplicaCount: 10},
				{Node: "node-2", DiskUtilisation: 400_000_000 / diskUsable, ReplicaCount: 1},
			},
			check: func(t *testing.T, h *plannerHarness) {
				req := h.proposal()
				for _, m := range req.Moves {
					require.NotEqual(t, "g1-shard0", m.Shard)
				}
				inFlight := transfer{from: "node-2", to: "node-1", bytes: 150_000_000}
				requireBaseline(t, goalAfter(diskRows(700_000_000, 400_000_000), req.Baseline,
					append([]transfer{inFlight}, transfersOf(req.Moves)...)...), req.Goal)
			},
		},
		{
			name: "in-flight MOVE larger than its source's used bytes",
			// node-1 drafted its g0 shard at 300MB but measures 100MB used. The
			// MOVE goes to node-9, which the plan does not list, so node-1
			// ends at 0, level with node-2.
			rows:        drafted(diskRows(100_000_000, 0), "node-1", shardSizes("Articles", 0, 300_000_000)...),
			states:      layout(shardGroup{count: 1, nodes: []string{"node-1"}}),
			setup:       func(t *testing.T, h *plannerHarness) { h.replicate(1, api.MOVE, "g0-shard0", "node-1", "node-9") },
			wantOutcome: replication.PlanStateNotNeeded,
			wantReason:  inFlightBalancedReason,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: 100_000_000 / diskUsable, ReplicaCount: 1},
				{Node: "node-2"},
			},
		},
		{
			name: "node without replication workers",
			rows: func() map[string]types.RebalanceNodeMeasurement {
				rows := spreadRows()
				m := rows["node-2"]
				m.ReplicationWorkers = 0
				rows["node-2"] = m
				return rows
			}(),
			states:          layout(spreadGroup),
			wantOutcome:     replication.PlanStatePlanningFailed,
			wantReasonParts: []string{"could not search for moves", "node-2", "workers"},
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: 700_000_000 / diskUsable, ReplicaCount: 10},
				{Node: "node-2", DiskUtilisation: 400_000_000 / diskUsable},
			},
		},
		{
			name: "no replicas on any listed node",
			// node-9 is not listed, so both listed nodes count 0 replicas.
			states:      layout(shardGroup{count: 4, nodes: []string{"node-9"}}),
			wantOutcome: replication.PlanStateNotNeeded,
			wantReason:  baselineBalancedReason,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation},
				{Node: "node-2", DiskUtilisation: node2Utilisation},
			},
		},
		{
			// The plan's collections list is empty and non-nil, as JSON
			// "collections": [] decodes. The run lists the schema once and
			// reaches the balance check.
			name:   "empty collections list names nothing",
			states: layout(shardGroup{count: 4, nodes: []string{"node-9"}}),
			setup: func(t *testing.T, h *plannerHarness) {
				h.replacePlan([]string{"node-1"}, []string{"node-2"}, []string{})
			},
			listsSchemaOnce: true,
			wantOutcome:     replication.PlanStateNotNeeded,
			wantReason:      baselineBalancedReason,
			wantBaseline: []types.RebalanceNodeBalance{
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
			wantReason:  baselineBalancedReason,
			wantBaseline: []types.RebalanceNodeBalance{
				{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 100},
				{Node: "node-2", DiskUtilisation: node2Utilisation, ReplicaCount: 104},
			},
		},
		{
			name:   "plan names a missing collection",
			rows:   spreadRows(),
			states: layout(spreadGroup).unread(),
			setup: func(t *testing.T, h *plannerHarness) {
				h.replacePlan([]string{"node-1"}, []string{"node-2"}, []string{"Ghost"})
			},
			wantOutcome:     replication.PlanStatePlanningFailed,
			wantReasonParts: []string{"Ghost"},
		},
		{
			// The cluster is balanced. If the check ran after the balance
			// check, the run would end NOT_NEEDED.
			name:   "plan names a missing collection on a balanced cluster",
			states: balancedLayout().unread(),
			setup: func(t *testing.T, h *plannerHarness) {
				h.replacePlan([]string{"node-1"}, []string{"node-2"}, []string{"Ghost"})
			},
			wantOutcome:     replication.PlanStatePlanningFailed,
			wantReasonParts: []string{"Ghost"},
		},
		{
			name:   "plan names an existing and a missing collection",
			rows:   spreadRows(),
			states: layout(spreadGroup).unread(),
			setup: func(t *testing.T, h *plannerHarness) {
				h.replacePlan([]string{"node-1"}, []string{"node-2"}, []string{"Articles", "Ghost"})
			},
			wantOutcome:      replication.PlanStatePlanningFailed,
			wantReasonParts:  []string{"Ghost"},
			wantReasonAbsent: []string{"Articles"},
		},
		{
			name:   "plan names two missing collections",
			rows:   spreadRows(),
			states: layout(spreadGroup).unread(),
			setup: func(t *testing.T, h *plannerHarness) {
				h.replacePlan([]string{"node-1"}, []string{"node-2"}, []string{"Phantom", "Ghost"})
			},
			wantOutcome:     replication.PlanStatePlanningFailed,
			wantReasonParts: []string{"Ghost, Phantom"},
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
			wantReason:  baselineBalancedReason,
			wantBaseline: []types.RebalanceNodeBalance{
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
			var states *rebalance.MockrebalanceSchemaReader
			switch {
			case tc.noSchemaRead:
				states = rebalance.NewMockrebalanceSchemaReader(t)
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

			if tc.listsSchemaOnce {
				states.AssertNumberOfCalls(t, "ReadSchema", 1)
			}
			req := requireRunAt(t, h, wantResult, wantMeasuredAt)
			require.Equal(t, tc.wantOutcome, req.Outcome)
			if tc.wantReasonParts != nil {
				for _, part := range tc.wantReasonParts {
					require.Contains(t, req.Reason, part)
				}
			} else {
				require.Equal(t, tc.wantReason, req.Reason)
			}
			for _, part := range tc.wantReasonAbsent {
				require.NotContains(t, req.Reason, part)
			}
			require.Equal(t, wantNodes, req.Nodes)
			requireBaseline(t, tc.wantBaseline, req.Baseline)
			proposed := tc.wantOutcome == replication.PlanStateProposed
			if proposed {
				plan, ok := h.fsm.GetPlan()
				require.True(t, ok)
				requireProposal(t, plan, req)
			} else {
				requireNoMoves(t, req.Moves, req.Goal, req.MoveCount, req.PlanHash, req.ApproveBeforeUnixMs)
			}

			if wantResult == string(tc.wantOutcome) {
				got, ok := h.fsm.GetPlan()
				require.True(t, ok)
				require.Equal(t, tc.wantOutcome, got.State)
				require.Equal(t, req.RunID, got.RunID)
				require.Equal(t, plannerTerm, got.RunTerm)
				require.Equal(t, wantMeasuredAt.UnixMilli(), got.MeasuredAtUnixMs)
				require.Equal(t, wantNodes, got.Nodes)
				require.Equal(t, req.Reason, got.Reason)
				requireBaseline(t, tc.wantBaseline, got.Baseline)
				if proposed {
					require.Equal(t, req.Moves, got.Moves)
					require.Equal(t, req.Goal, got.Goal)
					require.Equal(t, req.MoveCount, got.MoveCount)
					require.Equal(t, req.PlanHash, got.PlanHash)
					require.Equal(t, req.ApproveBeforeUnixMs, got.ApproveBeforeUnixMs)
				} else {
					requireNoMoves(t, got.Moves, got.Goal, got.MoveCount, got.PlanHash, got.ApproveBeforeUnixMs)
				}
			}
			if tc.check != nil {
				tc.check(t, h)
			}
		})
	}
}

func TestRebalancePlanner_Eligibility(t *testing.T) {
	// Each layout adds one protected replica, 150MB on node-1, to spreadRows.
	// Moving it alone levels both nodes at 550MB, so the search takes it first
	// unless a rule makes it ineligible.
	const protectedBytes = 150_000_000
	onNode1 := []string{"node-1"}
	cases := []struct {
		name        string
		states      *schemaFixture
		collections []string
		protected   types.RebalanceShardSize
		// draftNode is the node whose draft holds the protected shard.
		draftNode string
		wantMoved bool
	}{
		{
			name:      "shard neither HOT nor COLD does not move",
			states:    layout(spreadGroup, shardGroup{count: 1, status: models.TenantActivityStatusFREEZING, nodes: onNode1}),
			protected: types.RebalanceShardSize{Collection: "Articles", Shard: "g1-shard0"},
			draftNode: "node-1",
		},
		{
			name:      "COLD shard moves",
			states:    layout(spreadGroup, shardGroup{count: 1, status: models.TenantActivityStatusCOLD, nodes: onNode1}),
			protected: types.RebalanceShardSize{Collection: "Articles", Shard: "g1-shard0"},
			draftNode: "node-1",
			wantMoved: true,
		},
		{
			name:        "collection outside plan collections does not move",
			states:      layout(spreadGroup).withClass("Books", shardGroup{count: 1, nodes: onNode1}),
			collections: []string{"Articles"},
			protected:   types.RebalanceShardSize{Collection: "Books", Shard: "g0-shard0"},
			draftNode:   "node-1",
		},
		{
			// Only node-1 holds the shard, but node-2 drafted it. Another
			// node's draft does not give node-1's replica a size.
			name:      "replica without a drafted size does not move",
			states:    layout(spreadGroup, shardGroup{count: 1, nodes: onNode1}),
			protected: types.RebalanceShardSize{Collection: "Articles", Shard: "g1-shard0"},
			draftNode: "node-2",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			protected := tc.protected
			protected.Bytes = protectedBytes
			h := newPlannerHarness(t, fixedMeasure(drafted(spreadRows(), tc.draftNode, protected)), tc.states.mock(t))
			h.expectProposals()
			if tc.collections != nil {
				h.replacePlan([]string{"node-1"}, []string{"node-2"}, tc.collections)
			}

			h.planner.RunOnce(context.Background())

			req := requireRun(t, h, string(replication.PlanStateProposed))
			require.Equal(t, replication.PlanStateProposed, req.Outcome, req.Reason)
			moved := slices.ContainsFunc(req.Moves, func(m replication.PlanMove) bool {
				return m.Collection == protected.Collection && m.Shard == protected.Shard
			})
			require.Equal(t, tc.wantMoved, moved, "moves: %+v", req.Moves)
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
		proposer := rebalance.NewMockrebalanceProposer(t)
		proposer.EXPECT().CompleteRebalancePlanning(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, req *replication.CompletePlanningRequest) error {
				if _, err := json.Marshal(req); err != nil {
					t.Errorf("completion request does not encode: %v", err)
				}
				return fsm.CompletePlanning(plannerTerm, req)
			})
		drafting := rebalance.NewMockrebalanceDrafting(t)
		drafting.EXPECT().RebalanceDraftTasks().Return([]replication.DraftTask{reportedDraftTask(plannerPlanID)})
		measure := fixedMeasure(equalNodes())
		logger, _ := logrustest.NewNullLogger()
		planner, err := rebalance.NewRebalancePlanner(rebalance.RebalancePlannerParams{
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
		}, rebalance.NewMockrebalanceSchemaReader(t))
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
		h := newPlannerHarness(t, fixedMeasure(equalNodes()), rebalance.NewMockrebalanceSchemaReader(t))

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
			h := newPlannerHarness(t, fixedMeasure(equalNodes()), rebalance.NewMockrebalanceSchemaReader(t))
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
	balancedBaseline := []types.RebalanceNodeBalance{
		{Node: "node-1", DiskUtilisation: node1Utilisation, ReplicaCount: 100},
		{Node: "node-2", DiskUtilisation: node2Utilisation, ReplicaCount: 104},
	}

	t.Run("creates the task once", func(t *testing.T) {
		h := newPlannerHarness(t, fixedMeasure(equalNodes()), rebalance.NewMockrebalanceSchemaReader(t))
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
			h := newPlannerHarness(t, fixedMeasure(equalNodes()), rebalance.NewMockrebalanceSchemaReader(t))
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
		h := newPlannerHarness(t, fixedMeasure(equalNodes()), rebalance.NewMockrebalanceSchemaReader(t))
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
		h := newPlannerHarness(t, fixedMeasure(equalNodes()), rebalance.NewMockrebalanceSchemaReader(t))
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
		}, rebalance.NewMockrebalanceSchemaReader(t))
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
				}, rebalance.NewMockrebalanceSchemaReader(t))
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
		}, rebalance.NewMockrebalanceSchemaReader(t))
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
		}, rebalance.NewMockrebalanceSchemaReader(t))
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

func buildApplyRequest(class string, cmdType api.ApplyRequest_Type, jsonSubCmd any) *api.ApplyRequest {
	subData, err := json.Marshal(jsonSubCmd)
	if err != nil {
		panic("json.Marshal: " + err.Error())
	}
	return &api.ApplyRequest{Type: cmdType, Class: class, SubCommand: subData}
}

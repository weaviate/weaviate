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

package distributedtask

// This file builds a multi-scheduler harness that runs N scheduler
// instances against a single shared [Manager] (representing the
// RAFT-replicated FSM state every node sees in production). Each
// scheduler has its own LocalNode + provider, so per-node callback
// behavior — OnGroupCompleted, OnTaskCompleted, completion markers —
// can be asserted directly in unit tests.
//
// Why this exists: the FINALIZING regression (every node must fire
// OnTaskCompleted, even when a peer races ahead with
// MarkDistributedTaskFinalized) only surfaced in the docker-backed
// acceptance tests in test/acceptance/distributed_tasks, which take
// 20+ minutes per CI run. The single-scheduler tests in
// scheduler_test.go could not reproduce a cross-scheduler race
// because they only have one scheduler. This harness closes that
// gap: future bugs in the same class (per-node callback firing,
// per-node bootstrap suppression, cross-node finalize coordination)
// should fail loudly here at unit-test speed before they ever reach
// CI's acceptance tier.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// multiSchedulerHarness runs N scheduler instances pointing at a single
// shared [Manager], simulating a cluster of N nodes in one process. The
// fakeClock is shared across all schedulers (matching production's
// real-time clock), so a single h.advanceClock fires every scheduler's
// next tick.
type multiSchedulerHarness struct {
	t             *testing.T
	clock         *clockwork.FakeClock
	logger        logrus.FieldLogger
	namespace     string
	manager       *Manager
	taskTTL       time.Duration
	tickInterval  time.Duration
	completionRec *fanoutRecorder
	cleaner       *directCleaner
	finalizer     *directFinalizer
	notifier      *fanoutNotifier
	nodes         []*schedulerNode
}

// schedulerNode is a single node's slice of the cluster: a scheduler
// bound to a per-node provider that records every callback fire.
type schedulerNode struct {
	id        string
	scheduler *Scheduler
	provider  *recordingUnitAwareProvider
}

// recordingUnitAwareProvider satisfies [UnitAwareProvider] and tracks
// every OnGroupCompleted / OnTaskCompleted call. We embed the
// testTaskProvider so the harness can also assert startedCh /
// completedCh on a per-node basis if it ever needs to.
type recordingUnitAwareProvider struct {
	*testTaskProvider

	mu          sync.Mutex
	groupCalls  []string // taskID per OnGroupCompleted fire
	taskCalls   []string // taskID per OnTaskCompleted fire
	taskCallsBy map[string]int
}

func newRecordingUnitAwareProvider(t *testing.T) *recordingUnitAwareProvider {
	return &recordingUnitAwareProvider{
		testTaskProvider: newTestTaskProvider(t, nil),
		taskCallsBy:      map[string]int{},
	}
}

func (p *recordingUnitAwareProvider) OnGroupCompleted(task *Task, _ string, _ []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groupCalls = append(p.groupCalls, task.ID)
}

func (p *recordingUnitAwareProvider) OnTaskCompleted(task *Task) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.taskCalls = append(p.taskCalls, task.ID)
	p.taskCallsBy[task.ID]++
}

func (p *recordingUnitAwareProvider) taskCompletedCount(taskID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.taskCallsBy[taskID]
}

func (p *recordingUnitAwareProvider) groupCompletedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.groupCalls)
}

// fanoutNotifier dispatches Wake() to every registered scheduler. The
// production Manager has a single SchedulerNotifier slot; in a
// multi-node cluster every node's apply path calls its own scheduler's
// Wake. In-process we fan out one Manager-side Wake to every scheduler.
type fanoutNotifier struct {
	mu      sync.Mutex
	targets []*Scheduler
}

func (f *fanoutNotifier) Wake() {
	f.mu.Lock()
	targets := append([]*Scheduler(nil), f.targets...)
	f.mu.Unlock()
	for _, s := range targets {
		s.Wake()
	}
}

func (f *fanoutNotifier) add(s *Scheduler) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targets = append(f.targets, s)
}

// fanoutRecorder routes every recorder call through to the shared
// Manager. In a real cluster the recorder writes to RAFT and every
// node's FSM sees the result; in-process we just write to the shared
// Manager directly — the schedulers polling that Manager observe the
// same FSM state, which is what we want for these tests.
type fanoutRecorder struct {
	t       *testing.T
	manager *Manager
}

func (r *fanoutRecorder) RecordDistributedTaskUnitCompletion(_ context.Context, ns, id string, version uint64, node, unit string) error {
	return r.manager.RecordUnitCompletion(toCmd(r.t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace:            ns,
		Id:                   id,
		Version:              version,
		NodeId:               node,
		UnitId:               unit,
		FinishedAtUnixMillis: r.manager.clock.Now().UnixMilli(),
	}))
}

func (r *fanoutRecorder) RecordDistributedTaskUnitFailure(_ context.Context, ns, id string, version uint64, node, unit, errMsg string) error {
	return r.manager.RecordUnitCompletion(toCmd(r.t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace:            ns,
		Id:                   id,
		Version:              version,
		NodeId:               node,
		UnitId:               unit,
		Error:                errMsg,
		FinishedAtUnixMillis: r.manager.clock.Now().UnixMilli(),
	}))
}

func (r *fanoutRecorder) UpdateDistributedTaskUnitProgress(_ context.Context, ns, id string, version uint64, node, unit string, progress float32) error {
	return r.manager.UpdateUnitProgress(toCmd(r.t, &cmd.UpdateDistributedTaskUnitProgressRequest{
		Namespace:           ns,
		Id:                  id,
		Version:             version,
		NodeId:              node,
		UnitId:              unit,
		Progress:            progress,
		UpdatedAtUnixMillis: r.manager.clock.Now().UnixMilli(),
	}))
}

// directCleaner routes CleanUp calls into the shared Manager so cleanup
// of completed tasks happens in-process.
type directCleaner struct {
	t       *testing.T
	manager *Manager
}

func (c *directCleaner) CleanUpDistributedTask(_ context.Context, ns, id string, version uint64) error {
	return c.manager.CleanUpTask(toCmd(c.t, &cmd.CleanUpDistributedTaskRequest{
		Namespace: ns, Id: id, Version: version,
	}))
}

func newMultiSchedulerHarness(t *testing.T, nodeIDs []string) *multiSchedulerHarness {
	logger, _ := logrustest.NewNullLogger()
	clock := clockwork.NewFakeClock()
	mgr := NewManager(ManagerParameters{Clock: clock, CompletedTaskTTL: 24 * time.Hour})

	h := &multiSchedulerHarness{
		t:             t,
		clock:         clock,
		logger:        logger,
		namespace:     "tasks-namespace",
		manager:       mgr,
		taskTTL:       24 * time.Hour,
		tickInterval:  30 * time.Second,
		completionRec: &fanoutRecorder{t: t, manager: mgr},
		cleaner:       &directCleaner{t: t, manager: mgr},
		finalizer:     newDirectFinalizer(t, mgr),
		notifier:      &fanoutNotifier{},
	}

	for _, id := range nodeIDs {
		prov := newRecordingUnitAwareProvider(t)
		sched := NewScheduler(SchedulerParams{
			CompletionRecorder: h.completionRec,
			TasksLister:        mgr,
			TaskCleaner:        h.cleaner,
			TaskFinalizer:      h.finalizer,
			Providers:          map[string]Provider{h.namespace: prov},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          id,
			CompletedTaskTTL:   h.taskTTL,
			TickInterval:       h.tickInterval,
		})
		h.notifier.add(sched)
		h.nodes = append(h.nodes, &schedulerNode{id: id, scheduler: sched, provider: prov})
	}

	mgr.SetSchedulerNotifier(h.notifier)
	return h
}

// tick drives a single scheduler's tick body directly. We deliberately
// do NOT call [Scheduler.Start] in these tests — the loop goroutine
// would fire tick() on a FakeClock advance for every scheduler
// concurrently, which makes cross-scheduler ordering non-deterministic.
// By driving tick() manually we can force a specific order ("node-1's
// tick wins the FINALIZING→FINISHED race, then node-2 / node-3 must
// still fire OnTaskCompleted on the FINISHED state") and assert on
// the exact outcome.
//
// All ThrottledRecorder wrapping that Start() normally does is skipped
// — tests use the [fanoutRecorder] directly without throttling, which
// is what we want for deterministic assertions.
func (h *multiSchedulerHarness) tick(nodeIdx int) {
	h.nodes[nodeIdx].scheduler.tick()
}

func (h *multiSchedulerHarness) tickAll() {
	for i := range h.nodes {
		h.tick(i)
	}
}

func (h *multiSchedulerHarness) close() {
	for _, n := range h.nodes {
		// Close is safe even if Start was never called — it just signals
		// the stopCh and terminates the (empty) running-task map. We
		// call it for symmetry / leaktest hygiene.
		n.scheduler.Close()
	}
}

func (h *multiSchedulerHarness) listManagerTasks(t *testing.T) map[string][]*Task {
	t.Helper()
	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	return tasks
}

// TestMultiScheduler_OnTaskCompletedFiresOnEveryNode pins the
// cross-node finalize-race regression that the test/acceptance/
// distributed_tasks crash-recovery suite caught at 20+ min latency.
// With three schedulers sharing one Manager, every node MUST observe
// OnTaskCompleted exactly once — even when one scheduler's tick wins
// the race to issue MarkDistributedTaskFinalized and flips the FSM to
// FINISHED before the other two have observed FINALIZING.
//
// We force a deterministic ordering by driving tick() manually in a
// specific sequence: node-1 ticks while the FSM is at FINALIZING (it
// fires OnTaskCompleted + issues MarkDistributedTaskFinalized, flipping
// the FSM to FINISHED in that same tick), then node-2 and node-3 tick
// on a task that is already FINISHED. Without the fix in
// [Scheduler.tick] (firing readyForFinalize on FINISHED as well as
// FINALIZING / FAILED), node-2 and node-3 would silently skip
// OnTaskCompleted and this test would fail with counts of {node-1: 1,
// node-2: 0, node-3: 0}.
func TestMultiScheduler_OnTaskCompletedFiresOnEveryNode(t *testing.T) {
	defer leaktest.Check(t)()

	h := newMultiSchedulerHarness(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "multi-node-finalize"
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"u-node-1", "u-node-2", "u-node-3"},
	}), 1))

	// First round of ticks: each scheduler picks up its local unit.
	// We also assign the unit's NodeID by reporting progress before
	// ticking — the FSM uses the first progress update to bind a unit
	// to a node.
	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
			context.Background(), h.namespace, taskID, 1, n.id, "u-"+n.id, 0.1,
		))
	}
	h.tickAll()

	// Complete every node's unit. The last completion pushes the FSM
	// to FINALIZING. No scheduler has ticked yet on the FINALIZING
	// state.
	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
			context.Background(), h.namespace, taskID, 1, n.id, "u-"+n.id,
		))
	}
	tasks := h.listManagerTasks(t)[h.namespace]
	require.Len(t, tasks, 1)
	require.Equal(t, TaskStatusFinalizing, tasks[0].Status,
		"FSM must be FINALIZING after the last unit completes")

	// node-1 ticks first. Inside this tick it observes FINALIZING,
	// fires OnTaskCompleted, then issues MarkDistributedTaskFinalized
	// which transitions the FSM to FINISHED inline.
	h.tick(0)
	tasks = h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusFinished, tasks[0].Status,
		"node-1's tick must finalize the task FINALIZING→FINISHED")
	require.Equal(t, 1, h.nodes[0].provider.taskCompletedCount(taskID),
		"node-1 must have fired OnTaskCompleted in its tick")
	require.Equal(t, 0, h.nodes[1].provider.taskCompletedCount(taskID),
		"node-2 has not ticked yet; OnTaskCompleted must not have fired")
	require.Equal(t, 0, h.nodes[2].provider.taskCompletedCount(taskID),
		"node-3 has not ticked yet; OnTaskCompleted must not have fired")

	// node-2's first observation of the task is FINISHED, not
	// FINALIZING — node-1 won the race. With the fix, OnTaskCompleted
	// still fires here.
	h.tick(1)
	require.Equal(t, 1, h.nodes[1].provider.taskCompletedCount(taskID),
		"node-2 must fire OnTaskCompleted on FINISHED (peer finalized first); "+
			"a regression here breaks per-node post-completion work cluster-wide")

	// node-3: same scenario as node-2.
	h.tick(2)
	require.Equal(t, 1, h.nodes[2].provider.taskCompletedCount(taskID),
		"node-3 must fire OnTaskCompleted on FINISHED (peer finalized first)")

	// Idempotency: extra ticks must NOT re-fire on any node — the
	// per-scheduler fired-once flag carries exactly-once.
	h.tickAll()
	h.tickAll()
	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.taskCompletedCount(taskID),
			"node %s: OnTaskCompleted must fire exactly once across multiple ticks", n.id)
	}
}

// TestMultiScheduler_FailedTaskFiresOnTaskCompletedOnEveryNode mirrors
// the success-path test for the failure path: if one node reports a
// unit failure, the FSM transitions straight to FAILED (skipping
// FINALIZING). Every node's OnTaskCompleted must still fire so
// per-node cleanup (cache eviction, error logging) runs everywhere,
// not just on the node that recorded the failure.
func TestMultiScheduler_FailedTaskFiresOnTaskCompletedOnEveryNode(t *testing.T) {
	defer leaktest.Check(t)()

	h := newMultiSchedulerHarness(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "multi-node-failed"
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"u-node-1", "u-node-2", "u-node-3"},
	}), 1))

	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
			context.Background(), h.namespace, taskID, 1, n.id, "u-"+n.id, 0.1,
		))
	}
	h.tickAll()

	// One node reports failure — task transitions to FAILED. FAILED
	// is terminal at the FSM layer; no MarkDistributedTaskFinalized is
	// issued for FAILED tasks. Every node's tick must still fire
	// OnTaskCompleted.
	require.NoError(t, h.completionRec.RecordDistributedTaskUnitFailure(
		context.Background(), h.namespace, taskID, 1, "node-1", "u-node-1", "synthetic failure",
	))
	tasks := h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusFailed, tasks[0].Status,
		"first unit failure must transition FSM to FAILED")

	h.tickAll()

	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.taskCompletedCount(taskID),
			"node %s: OnTaskCompleted must fire on FAILED task", n.id)
	}

	// Idempotency across additional ticks.
	h.tickAll()
	h.tickAll()
	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.taskCompletedCount(taskID),
			"node %s: OnTaskCompleted on FAILED must fire exactly once", n.id)
	}
}

// TestMultiScheduler_OnGroupCompletedFiresPerNodeOnlyForLocalUnits
// pins per-node group-callback scoping: a group's OnGroupCompleted on
// node X must fire iff X owns at least one unit in that group. This is
// the contract OnGroupCompleted has historically relied on (and the
// reindex provider relies on: the swap step only runs for local
// shards). A future scheduler refactor that accidentally fires the
// callback cluster-wide would mis-run swap on nodes that don't own
// the shard.
func TestMultiScheduler_OnGroupCompletedFiresPerNodeOnlyForLocalUnits(t *testing.T) {
	defer leaktest.Check(t)()

	h := newMultiSchedulerHarness(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "multi-node-groups"
	// Two groups: group-a has units on node-1 + node-2, group-b only
	// on node-3. After all units complete, node-1/node-2 fire
	// OnGroupCompleted for group-a (one each); node-3 fires for group-b.
	// No node fires for a group it doesn't own units in.
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitSpecs: []*cmd.UnitSpec{
			{Id: "u-a-1", GroupId: "group-a"},
			{Id: "u-a-2", GroupId: "group-a"},
			{Id: "u-b-3", GroupId: "group-b"},
		},
	}), 1))

	require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
		context.Background(), h.namespace, taskID, 1, "node-1", "u-a-1", 0.1))
	require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
		context.Background(), h.namespace, taskID, 1, "node-2", "u-a-2", 0.1))
	require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
		context.Background(), h.namespace, taskID, 1, "node-3", "u-b-3", 0.1))
	h.tickAll()

	require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
		context.Background(), h.namespace, taskID, 1, "node-1", "u-a-1"))
	require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
		context.Background(), h.namespace, taskID, 1, "node-2", "u-a-2"))
	require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
		context.Background(), h.namespace, taskID, 1, "node-3", "u-b-3"))

	h.tickAll()

	// Each node must have fired OnGroupCompleted exactly once for its
	// own group.
	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.groupCompletedCount(),
			"node %s: OnGroupCompleted must fire once for its local group", n.id)
	}
}

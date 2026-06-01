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
	"fmt"
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
	ackRecorder   *fanoutAckRecorder
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

	mu                sync.Mutex
	groupCalls        []string // taskID per OnGroupCompleted fire
	taskCalls         []string // taskID per OnTaskCompleted fire
	taskCallsBy       map[string]int
	groupCompletedErr error // when non-nil, OnGroupCompleted returns this to drive Gap A ack-failure paths in tests
}

func newRecordingUnitAwareProvider(t *testing.T) *recordingUnitAwareProvider {
	return &recordingUnitAwareProvider{
		testTaskProvider: newTestTaskProvider(t, nil),
		taskCallsBy:      map[string]int{},
	}
}

func (p *recordingUnitAwareProvider) OnGroupCompleted(task *Task, _ string, _ []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groupCalls = append(p.groupCalls, task.ID)
	if p.groupCompletedErr != nil {
		return p.groupCompletedErr
	}
	return nil
}

func (p *recordingUnitAwareProvider) OnSwapRequested(_ *Task, _ string, _ []string) error {
	// Recording provider exercises the NeedsPreparationBarrier=false path;
	// scheduler never fires this for these tasks. Stub for interface
	// compliance.
	return nil
}

// SetGroupCompletedError makes subsequent OnGroupCompleted calls on
// this provider return the given error. Used to drive the
// https://github.com/weaviate/0-weaviate-issues/issues/214 Gap A ack-failure path in tests.
func (p *recordingUnitAwareProvider) SetGroupCompletedError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groupCompletedErr = err
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
	targets := func() []*Scheduler {
		f.mu.Lock()
		defer f.mu.Unlock()
		return append([]*Scheduler(nil), f.targets...)
	}()
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

// fanoutAckRecorder routes RecordDistributedTaskPostCompletionAck calls
// directly into the shared Manager so every scheduler observes the
// ack on its next tick. The production wiring uses RAFT-applied
// commits; in-process we just call into the FSM directly because the
// schedulers all poll the same Manager.
//
// See https://github.com/weaviate/0-weaviate-issues/issues/214 Gap A.
type fanoutAckRecorder struct {
	t       *testing.T
	manager *Manager
}

func (r *fanoutAckRecorder) RecordDistributedTaskPostCompletionAck(
	_ context.Context,
	ns, id string,
	version uint64,
	nodeID string,
	success bool,
	errMsg string,
) error {
	return r.manager.RecordPostCompletionAck(toCmd(r.t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
		Namespace:         ns,
		Id:                id,
		Version:           version,
		NodeId:            nodeID,
		Success:           success,
		Error:             errMsg,
		AckedAtUnixMillis: r.manager.clock.Now().UnixMilli(),
	}))
}

func (r *fanoutAckRecorder) RecordDistributedTaskPreparationCompleteAck(
	_ context.Context,
	ns, id string,
	version uint64,
	nodeID string,
	success bool,
	errMsg string,
) error {
	return r.manager.RecordPreparationCompleteAck(toCmd(r.t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace:         ns,
		Id:                id,
		Version:           version,
		NodeId:            nodeID,
		Success:           success,
		Error:             errMsg,
		AckedAtUnixMillis: r.manager.clock.Now().UnixMilli(),
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
	return newMultiSchedulerHarnessWithOptions(t, nodeIDs, false)
}

// newMultiSchedulerHarnessWithAckBarrier wires the post-completion ack
// recorder on every scheduler so tests can drive the
// https://github.com/weaviate/0-weaviate-issues/issues/214 Gap A barrier. Without an ack recorder the
// scheduler falls back to the legacy pre-#214 behavior (no barrier;
// schema flip fires as soon as OnTaskCompleted returns), which is the
// default for tests that don't care about the barrier.
func newMultiSchedulerHarnessWithAckBarrier(t *testing.T, nodeIDs []string) *multiSchedulerHarness {
	return newMultiSchedulerHarnessWithOptions(t, nodeIDs, true)
}

func newMultiSchedulerHarnessWithOptions(t *testing.T, nodeIDs []string, withAckBarrier bool) *multiSchedulerHarness {
	logger, _ := logrustest.NewNullLogger()
	clock := clockwork.NewFakeClock()
	mgr := NewManager(ManagerParameters{Clock: clock, CompletedTaskTTL: 24 * time.Hour, Logger: logger})

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
	if withAckBarrier {
		h.ackRecorder = &fanoutAckRecorder{t: t, manager: mgr}
	}

	for _, id := range nodeIDs {
		prov := newRecordingUnitAwareProvider(t)
		params := SchedulerParams{
			CompletionRecorder: h.completionRec,
			TaskLister:         mgr,
			TaskCleaner:        h.cleaner,
			TaskFinalizer:      h.finalizer,
			Providers:          map[string]Provider{h.namespace: prov},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          id,
			CompletedTaskTTL:   h.taskTTL,
			TickInterval:       h.tickInterval,
		}
		if h.ackRecorder != nil {
			params.AckRecorder = h.ackRecorder
		}
		sched := NewScheduler(params)
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
	require.Equal(t, TaskStatusSwapping, tasks[0].Status,
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

// TestMultiScheduler_CancelledTaskFiresOnTaskCompletedOnEveryNode pins
// the cluster-wide cancel-cleanup path: a CANCELLED RAFT transition must
// fire OnTaskCompleted on every node, not just the node that received the
// cancel REST call, so per-node post-cancellation cleanup runs cluster-wide.
func TestMultiScheduler_CancelledTaskFiresOnTaskCompletedOnEveryNode(t *testing.T) {
	defer leaktest.Check(t)()

	h := newMultiSchedulerHarness(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "multi-node-cancelled"
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"u-node-1", "u-node-2", "u-node-3"},
	}), 1))
	h.tickAll()

	require.NoError(t, h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
		Namespace:             h.namespace,
		Id:                    taskID,
		Version:               1,
		CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
	})))
	tasks := h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusCancelled, tasks[0].Status,
		"CancelTask must transition FSM to CANCELLED")

	h.tickAll()

	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.taskCompletedCount(taskID),
			"node %s: OnTaskCompleted must fire on CANCELLED task", n.id)
	}

	// Idempotency across additional ticks.
	h.tickAll()
	h.tickAll()
	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.taskCompletedCount(taskID),
			"node %s: OnTaskCompleted on CANCELLED must fire exactly once", n.id)
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

// TestMultiScheduler_AckBarrier_BlocksFinalizeUntilEveryNodeAcks pins
// https://github.com/weaviate/0-weaviate-issues/issues/214 Gap A: with the post-completion ack recorder
// wired, MarkDistributedTaskFinalized (and therefore OnTaskCompleted's
// cluster-wide schema flip) must NOT commit until every node with local
// units has recorded a success ack. A node that crashed between local
// swap completion and ack emission — or whose silent swap failure means
// it never emits a success ack — keeps the task at FINALIZING forever
// until the truth is recorded.
//
// Why this matters: before the fix, the FINALIZING → FINISHED
// transition fired as soon as ONE node's tick observed FINALIZING. The
// scheduler had no per-node barrier, so a node whose RunSwapOnShard
// silently failed would let the schema flip commit cluster-wide while
// that replica's bucket pointed at old data — the production failure
// shape on a rolling restart during FINALIZING.
func TestMultiScheduler_AckBarrier_BlocksFinalizeUntilEveryNodeAcks(t *testing.T) {
	defer leaktest.Check(t)()

	h := newMultiSchedulerHarnessWithAckBarrier(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "ack-barrier-success"
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"u-node-1", "u-node-2", "u-node-3"},
	}), 1))

	// Claim units + complete every unit so the FSM transitions to FINALIZING.
	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
			context.Background(), h.namespace, taskID, 1, n.id, "u-"+n.id, 0.1,
		))
	}
	h.tickAll()
	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
			context.Background(), h.namespace, taskID, 1, n.id, "u-"+n.id,
		))
	}
	tasks := h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusSwapping, tasks[0].Status)

	// Tick only node-1. Inside this tick:
	//   - OnGroupCompleted fires on node-1.
	//   - node-1 records its ack via the fanout recorder.
	//   - The OnTaskCompleted gate sees MissingPostCompletionAckNodes
	//     still contains node-2 + node-3 → DOES NOT fire OnTaskCompleted.
	//   - MarkDistributedTaskFinalized is therefore NOT issued.
	h.tick(0)
	require.Equal(t, 1, h.nodes[0].provider.groupCompletedCount(),
		"node-1: OnGroupCompleted must have fired")
	require.Equal(t, 0, h.nodes[0].provider.taskCompletedCount(taskID),
		"node-1: OnTaskCompleted must NOT fire while node-2/node-3 acks are missing")

	tasks = h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusSwapping, tasks[0].Status,
		"task must remain FINALIZING until every node has acked")
	require.Len(t, tasks[0].PostCompletionAcks, 1,
		"only node-1's ack must be recorded so far")
	require.Contains(t, tasks[0].PostCompletionAcks, "node-1")
	require.True(t, tasks[0].PostCompletionAcks["node-1"].Success)

	// Tick node-2. node-2 records its ack; still 1 missing.
	h.tick(1)
	require.Equal(t, 0, h.nodes[1].provider.taskCompletedCount(taskID),
		"node-2: OnTaskCompleted must NOT fire while node-3's ack is missing")
	tasks = h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusSwapping, tasks[0].Status)
	require.Len(t, tasks[0].PostCompletionAcks, 2)

	// Tick node-3. Records its ack — all three acks now present.
	// OnTaskCompleted fires on node-3 in the same tick.
	// MarkDistributedTaskFinalized is issued; FSM goes to FINISHED.
	h.tick(2)
	require.Equal(t, 1, h.nodes[2].provider.taskCompletedCount(taskID),
		"node-3: OnTaskCompleted must fire once all acks are present")
	tasks = h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusFinished, tasks[0].Status,
		"task must transition to FINISHED once every node has acked")

	// Re-tick node-1 and node-2: they now observe FINISHED and fire
	// their OnTaskCompleted once (idempotently).
	h.tick(0)
	h.tick(1)
	require.Equal(t, 1, h.nodes[0].provider.taskCompletedCount(taskID))
	require.Equal(t, 1, h.nodes[1].provider.taskCompletedCount(taskID))

	// Idempotency on extra ticks.
	h.tickAll()
	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.taskCompletedCount(taskID),
			"node %s: OnTaskCompleted must fire exactly once across all ticks", n.id)
	}
}

// TestMultiScheduler_AckBarrier_FailureAckTransitionsToFailed pins the
// failure-path branch of https://github.com/weaviate/0-weaviate-issues/issues/214 Gap A: when one node's
// OnGroupCompleted returns an error, the per-node ack records
// success=false, which transitions the FSM straight to FAILED. The
// cluster-wide schema flip (in the reindex provider's OnTaskCompleted)
// skips its work on FAILED status, so the replica that had the silent
// swap failure does not get left behind serving wrong-tokenization data
// under a new schema.
func TestMultiScheduler_AckBarrier_FailureAckTransitionsToFailed(t *testing.T) {
	defer leaktest.Check(t)()

	h := newMultiSchedulerHarnessWithAckBarrier(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "ack-barrier-failure"
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
	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
			context.Background(), h.namespace, taskID, 1, n.id, "u-"+n.id,
		))
	}
	tasks := h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusSwapping, tasks[0].Status)

	// Inject a failure in node-2's OnGroupCompleted. The production
	// equivalent is the reindex provider's RunSwapOnShard returning an
	// error (compaction-interrupted shutdown, ENOSPC mid-rename, etc.).
	h.nodes[1].provider.SetGroupCompletedError(fmt.Errorf("synthetic swap failure on node-2"))

	// Tick node-1 first. node-1's OnGroupCompleted succeeds; ack
	// success=true recorded. Schema flip gate sees missing acks; no
	// OnTaskCompleted.
	h.tick(0)
	tasks = h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusSwapping, tasks[0].Status)
	require.Equal(t, 0, h.nodes[0].provider.taskCompletedCount(taskID),
		"node-1: OnTaskCompleted must not fire until ack barrier opens")

	// Tick node-2. Its OnGroupCompleted returns the injected error,
	// the scheduler emits the ack with success=false, and the FSM
	// transitions the task to FAILED on the apply path.
	h.tick(1)
	tasks = h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusFailed, tasks[0].Status,
		"task must transition to FAILED on any node's failure ack")
	require.NotEmpty(t, tasks[0].Error,
		"FAILED task must carry the ack-failure message for forensics")
	require.Contains(t, tasks[0].PostCompletionAcks, "node-2")
	require.False(t, tasks[0].PostCompletionAcks["node-2"].Success)

	// node-2's tick observed FAILED inside the same tick, so
	// OnTaskCompleted fires there for per-node cleanup. Per-node
	// cleanup MUST fire on FAILED (not just FINISHED) so the reindex
	// provider clears its caches everywhere; the FAILED gate makes
	// OnTaskCompleted skip the schema flip but does NOT skip the
	// callback itself.
	require.GreaterOrEqual(t, h.nodes[1].provider.taskCompletedCount(taskID), 1,
		"node-2: OnTaskCompleted must fire on FAILED status for per-node cleanup")

	// node-1 + node-3: their next tick observes FAILED. OnTaskCompleted
	// fires for cleanup; FINALIZING is not visible anymore. The ack
	// recorder is still safe to call on FAILED — the Manager silently
	// discards stale acks (idempotent for late arrivals).
	h.tick(0)
	h.tick(2)
	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.taskCompletedCount(taskID),
			"node %s: OnTaskCompleted must fire exactly once on FAILED across ticks", n.id)
	}

	// MarkDistributedTaskFinalized must NOT have fired: FAILED is
	// terminal at the FSM layer; the FINALIZING → FINISHED transition
	// is precisely what the ack barrier blocked.
	tasks = h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusFailed, tasks[0].Status,
		"task must remain FAILED — Mark*Finalized must not promote it past FAILED")
}

// TestMultiScheduler_AckBarrier_LateSuccessAckSurvivesIdempotently pins
// the ack-replay behavior: a node whose ack apply errored on the wire
// retries on its next tick. The Manager keeps the first ack per (task,
// node) and silently drops duplicates; this test verifies the retry
// doesn't double-count or flip a recorded failure to success (or vice
// versa).
func TestMultiScheduler_AckBarrier_LateSuccessAckSurvivesIdempotently(t *testing.T) {
	defer leaktest.Check(t)()

	h := newMultiSchedulerHarnessWithAckBarrier(t, []string{"node-1"})
	defer h.close()

	taskID := "ack-barrier-idempotent"
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"u-node-1"},
	}), 1))
	require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
		context.Background(), h.namespace, taskID, 1, "node-1", "u-node-1", 0.1))
	h.tick(0)
	require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
		context.Background(), h.namespace, taskID, 1, "node-1", "u-node-1"))

	h.tick(0)
	tasks := h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusFinished, tasks[0].Status,
		"single-node task transitions to FINISHED in one tick (its own ack is the only one expected)")
	originalAck := tasks[0].PostCompletionAcks["node-1"]
	require.True(t, originalAck.Success)

	// Inject a manual stale ack — the kind that would arrive from a
	// retry that landed after the task transitioned past FINALIZING.
	// The Manager must silently drop it.
	require.NoError(t, h.ackRecorder.RecordDistributedTaskPostCompletionAck(
		context.Background(), h.namespace, taskID, 1, "node-1", false, "duplicate stale ack",
	))
	tasks = h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusFinished, tasks[0].Status,
		"FINISHED task must reject (silently) any late post-completion ack")
	require.Equal(t, originalAck.Success, tasks[0].PostCompletionAcks["node-1"].Success,
		"original ack must remain unchanged on late retry")
}

// TestMultiScheduler_AckBarrier_ContextCanceledIsTransient pins the
// graceful-shutdown recovery semantics for https://github.com/weaviate/0-weaviate-issues/issues/214 +
// #213: when OnGroupCompleted returns a context.Canceled-rooted error
// (the production graceful-shutdown shape produced by lsmkv's
// non-cancellable in-flight compaction during SIGTERM), the scheduler
// MUST NOT emit a failure ack. Instead it MUST drop the local
// "fired" mark so the post-restart tick re-fires OnGroupCompleted via
// the rehydrate path and emits a fresh success ack.
//
// Without this discipline the task would transition to FAILED on the
// first node whose graceful shutdown raced its swap (CI surfaced this
// exact failure in TestMultiNode_RollingRestartDuringFinalizing on
// PR https://github.com/weaviate/weaviate/pull/10694), short-circuiting the post-restart recovery and turning
// every rolling restart that lands in the FINALIZING window into a
// permanent migration failure.
func TestMultiScheduler_AckBarrier_ContextCanceledIsTransient(t *testing.T) {
	defer leaktest.Check(t)()

	h := newMultiSchedulerHarnessWithAckBarrier(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "ack-barrier-ctx-canceled"
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
	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
			context.Background(), h.namespace, taskID, 1, n.id, "u-"+n.id,
		))
	}

	// node-2's OnGroupCompleted returns context.Canceled (the
	// shutdown-during-FINALIZING shape from #213).
	h.nodes[1].provider.SetGroupCompletedError(fmt.Errorf("simulated swap error: %w", context.Canceled))

	// Tick all. node-1 + node-3 should succeed and emit success acks;
	// node-2 should DROP its fired mark (ctx.Canceled path) and NOT
	// emit an ack. The task must NOT transition to FAILED — it stays
	// at FINALIZING waiting for the recovery (= clearing the simulated
	// error and re-ticking) to emit a success ack.
	h.tickAll()

	tasks := h.listManagerTasks(t)[h.namespace]
	require.Equal(t, TaskStatusSwapping, tasks[0].Status,
		"ctx.Canceled from OnGroupCompleted MUST NOT flip task to FAILED")
	require.NotContains(t, tasks[0].PostCompletionAcks, "node-2",
		"ctx.Canceled path MUST NOT emit an ack — recovery is responsible")

	// node-1 and node-3 acked success (their OnGroupCompleted returned nil).
	require.Contains(t, tasks[0].PostCompletionAcks, "node-1")
	require.True(t, tasks[0].PostCompletionAcks["node-1"].Success)
	require.Contains(t, tasks[0].PostCompletionAcks, "node-3")
	require.True(t, tasks[0].PostCompletionAcks["node-3"].Success)

	// Simulate the recovery: clear the injected error (as if node-2
	// restarted and the second OnGroupCompleted call on a fresh process
	// has no compaction to cancel).
	h.nodes[1].provider.SetGroupCompletedError(nil)

	// node-2 re-ticks. The dropped fired mark causes a fresh
	// OnGroupCompleted call (now succeeding), then the ack-emission gate
	// fires success ack. The schema flip + MarkTaskFinalized can finally
	// land.
	h.tick(1)
	tasks = h.listManagerTasks(t)[h.namespace]
	require.Contains(t, tasks[0].PostCompletionAcks, "node-2",
		"after recovery re-fires OnGroupCompleted, node-2's ack must land")
	require.True(t, tasks[0].PostCompletionAcks["node-2"].Success,
		"recovery's re-fire should produce a success ack")
	require.Equal(t, TaskStatusFinished, tasks[0].Status,
		"with all three acks landed (1 via recovery), task must transition to FINISHED")
}

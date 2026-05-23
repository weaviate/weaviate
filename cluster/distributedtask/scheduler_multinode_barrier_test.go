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

// This file extends the multi-scheduler harness in scheduler_multinode_test.go
// with PHASE B coverage for the two-phase RAFT swap barrier introduced in
// PR #11328. The existing recording providers stub OnSwapRequested to a
// no-op, so the legacy test file only exercises PHASE A (PreparationCompleteAck)
// and the non-barrier PostCompletionAck path. The barrierRecordingProvider
// in this file actually records per-node OnSwapRequested invocations and
// allows per-node fault injection so the cross-replica failure modes
// G1-G8 enumerated in sub-report 04-raft-barrier.md can be pinned at
// unit-test latency rather than relying on the slow single-config
// acceptance test in test/acceptance/reindex_multinode/cross_replica_barrier_test.go.
//
// Helper-name discipline: every type/func/var introduced here is
// prefixed with `barrier*` so the file can land alongside other parallel
// agents' additions without collisions. The existing multiSchedulerHarness,
// fanoutAckRecorder, fanoutRecorder, directCleaner, directFinalizer, and
// fanoutNotifier types are REUSED — DO NOT redefine.

import (
	"context"
	"encoding/json"
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

// barrierRecordingProvider is the PHASE-B-aware sibling of
// recordingUnitAwareProvider. It actually IMPLEMENTS OnSwapRequested
// (the legacy recording provider stubs it to a no-op since it only
// exercises the NeedsPreparationBarrier=false path) and records every
// invocation per-task so PHASE B firing can be asserted directly.
//
// SetSwapRequestedError + SetGroupCompletedError let tests inject
// per-node faults symmetric to the existing scheduler_multinode_test.go
// failure-path coverage, but in the SWAPPING window rather than the
// FINALIZING / SWAPPING-legacy window.
type barrierRecordingProvider struct {
	*testTaskProvider

	mu             sync.Mutex
	groupCalls     []string // taskID per OnGroupCompleted fire (PHASE A in barrier mode)
	swapCalls      []string // taskID per OnSwapRequested fire (PHASE B in barrier mode)
	taskCalls      []string // taskID per OnTaskCompleted fire
	swapCallsBy    map[string]int
	groupCallsBy   map[string]int
	taskCallsBy    map[string]int
	groupCompErr   error
	swapRequestErr error
}

func newBarrierRecordingProvider(t *testing.T) *barrierRecordingProvider {
	return &barrierRecordingProvider{
		testTaskProvider: newTestTaskProvider(t, nil),
		swapCallsBy:      map[string]int{},
		groupCallsBy:     map[string]int{},
		taskCallsBy:      map[string]int{},
	}
}

func (p *barrierRecordingProvider) OnGroupCompleted(task *Task, _ string, _ []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groupCalls = append(p.groupCalls, task.ID)
	p.groupCallsBy[task.ID]++
	if p.groupCompErr != nil {
		return p.groupCompErr
	}
	return nil
}

func (p *barrierRecordingProvider) OnSwapRequested(task *Task, _ string, _ []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.swapCalls = append(p.swapCalls, task.ID)
	p.swapCallsBy[task.ID]++
	if p.swapRequestErr != nil {
		return p.swapRequestErr
	}
	return nil
}

func (p *barrierRecordingProvider) OnTaskCompleted(task *Task) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.taskCalls = append(p.taskCalls, task.ID)
	p.taskCallsBy[task.ID]++
}

func (p *barrierRecordingProvider) SetGroupCompletedError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groupCompErr = err
}

func (p *barrierRecordingProvider) SetSwapRequestedError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.swapRequestErr = err
}

func (p *barrierRecordingProvider) swapCount(taskID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.swapCallsBy[taskID]
}

func (p *barrierRecordingProvider) groupCount(taskID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.groupCallsBy[taskID]
}

func (p *barrierRecordingProvider) taskCount(taskID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.taskCallsBy[taskID]
}

// barrierHarness wraps multiSchedulerHarness with barrierRecordingProvider
// wiring on every scheduler. We can't reuse newMultiSchedulerHarnessWithAckBarrier
// directly because it hard-codes the recording provider type — so we build
// the scheduler stack here with the barrier-aware provider instead.
type barrierHarness struct {
	t             *testing.T
	clock         *clockwork.FakeClock
	logger        logrus.FieldLogger
	namespace     string
	manager       *Manager
	completionRec *fanoutRecorder
	cleaner       *directCleaner
	finalizer     *directFinalizer
	ackRecorder   *fanoutAckRecorder
	notifier      *fanoutNotifier
	nodes         []*barrierNode
}

type barrierNode struct {
	id        string
	scheduler *Scheduler
	provider  *barrierRecordingProvider
}

func newBarrierHarness(t *testing.T, nodeIDs []string) *barrierHarness {
	logger, _ := logrustest.NewNullLogger()
	clock := clockwork.NewFakeClock()
	mgr := NewManager(ManagerParameters{Clock: clock, CompletedTaskTTL: 24 * time.Hour, Logger: logger})

	h := &barrierHarness{
		t:             t,
		clock:         clock,
		logger:        logger,
		namespace:     "tasks-namespace",
		manager:       mgr,
		completionRec: &fanoutRecorder{t: t, manager: mgr},
		cleaner:       &directCleaner{t: t, manager: mgr},
		finalizer:     newDirectFinalizer(t, mgr),
		ackRecorder:   &fanoutAckRecorder{t: t, manager: mgr},
		notifier:      &fanoutNotifier{},
	}

	for _, id := range nodeIDs {
		prov := newBarrierRecordingProvider(t)
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
			CompletedTaskTTL:   24 * time.Hour,
			TickInterval:       30 * time.Second,
			AckRecorder:        h.ackRecorder,
		}
		sched := NewScheduler(params)
		h.notifier.add(sched)
		h.nodes = append(h.nodes, &barrierNode{id: id, scheduler: sched, provider: prov})
	}

	mgr.SetSchedulerNotifier(h.notifier)
	return h
}

func (h *barrierHarness) tick(nodeIdx int) {
	h.nodes[nodeIdx].scheduler.tick()
}

func (h *barrierHarness) tickAll() {
	for i := range h.nodes {
		h.tick(i)
	}
}

func (h *barrierHarness) close() {
	for _, n := range h.nodes {
		n.scheduler.Close()
	}
}

func (h *barrierHarness) listTasks(t *testing.T) []*Task {
	t.Helper()
	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	return tasks[h.namespace]
}

// getTask returns the live FSM task matching taskID. Fails the test if no
// such task exists. Used by the barrier helpers and per-test assertions to
// avoid index/order coupling when multiple tasks coexist in the harness
// (e.g. G4's parallel-prop test, G3's mid-test second-task injection).
func (h *barrierHarness) getTask(t *testing.T, taskID string) *Task {
	t.Helper()
	for _, task := range h.listTasks(t) {
		if task.ID == taskID {
			return task
		}
	}
	require.Failf(t, "task not found", "task %s missing from FSM", taskID)
	return nil
}

// barrierAddTaskOneUnitPerNode submits a barrier-mode task with one
// unit per node (no explicit group, so every unit shares the default
// "" group). Mirrors the body the existing TestMultiScheduler_AckBarrier_*
// tests use for the non-barrier path, but with NeedsPreparationBarrier=true.
func barrierAddTaskOneUnitPerNode(t *testing.T, h *barrierHarness, taskID string) {
	t.Helper()
	units := make([]string, 0, len(h.nodes))
	for _, n := range h.nodes {
		units = append(units, "u-"+n.id)
	}
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:               h.namespace,
		Id:                      taskID,
		SubmittedAtUnixMillis:   h.clock.Now().UnixMilli(),
		UnitIds:                 units,
		NeedsPreparationBarrier: true,
	}), 1))
}

// barrierDriveToPreparing claims every unit + completes every unit so
// the FSM transitions to PREPARING. After this returns, the next call
// to tick on any node fires PHASE A (OnGroupCompleted) and PHASE A.5
// (prep-complete ack emission).
func barrierDriveToPreparing(t *testing.T, h *barrierHarness, taskID string) {
	t.Helper()
	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
			context.Background(), h.namespace, taskID, 1, n.id, "u-"+n.id, 0.1,
		))
	}
	// One tick per node so each scheduler claims its unit (StartTask path).
	h.tickAll()
	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
			context.Background(), h.namespace, taskID, 1, n.id, "u-"+n.id,
		))
	}
	task := h.getTask(t, taskID)
	require.Equal(t, TaskStatusPreparing, task.Status,
		"barrier task with all units terminal MUST land in PREPARING")
}

// barrierDriveAllAcksAndAssertSwapping ticks every node so each fires
// PHASE A + emits PHASE A.5 ack, then asserts the FSM advanced to
// SWAPPING (barrier lifted) so PHASE B can start.
func barrierDriveAllAcksAndAssertSwapping(t *testing.T, h *barrierHarness, taskID string) {
	t.Helper()
	h.tickAll()
	task := h.getTask(t, taskID)
	require.Equal(t, TaskStatusSwapping, task.Status,
		"after every node's prep-ack lands successfully the barrier must lift to SWAPPING")
}

// TestBarrier_G1_LeaderLossBetweenPhaseAandPhaseB pins gap G1:
// after every node has committed its PreparationCompleteAck and the FSM
// has transitioned PREPARING → SWAPPING, every node's NEXT tick must
// fire OnSwapRequested. Whether or not a RAFT leader change happens
// between the apply that lifted the barrier and the next tick is
// invisible to the scheduler — the only state input is the
// FSM-replicated Task.Status. We model "leader loss" by ordering ticks
// so that a different scheduler observes SWAPPING in each tick and must
// nevertheless re-emit its own per-node PHASE B work.
//
// Important production-behavior subtlety: the scheduler ONLY reflects
// the FAILED transition on the local task clone (scheduler.go:574-576);
// it does NOT reflect a successful PREPARING → SWAPPING flip. So even
// on the node whose ack lifts the barrier, PHASE B does NOT fire in
// the SAME tick — the in-tick `postStarted` predicate evaluated from
// the stale local clone is still PREPARING. PHASE B fires on the NEXT
// tick when ListDistributedTasks returns the fresh SWAPPING status.
// This test pins that contract: no node fires OnSwapRequested in its
// own ack-emitting tick, but every node MUST fire on its first
// subsequent SWAPPING-observing tick.
func TestBarrier_G1_LeaderLossBetweenPhaseAandPhaseB(t *testing.T) {
	defer leaktest.Check(t)()

	h := newBarrierHarness(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "barrier-g1-leader-loss"
	barrierAddTaskOneUnitPerNode(t, h, taskID)
	barrierDriveToPreparing(t, h, taskID)

	// node-1 ticks first: PHASE A fires, PHASE A.5 ack lands. The FSM
	// stays PREPARING because node-2 + node-3 haven't acked yet. node-1
	// must NOT fire OnSwapRequested yet — that gate opens only on
	// task.Status == SWAPPING.
	h.tick(0)
	require.Equal(t, 1, h.nodes[0].provider.groupCount(taskID),
		"node-1: PHASE A (OnGroupCompleted) must fire on PREPARING")
	require.Equal(t, 0, h.nodes[0].provider.swapCount(taskID),
		"node-1: PHASE B (OnSwapRequested) must NOT fire while barrier holds")
	task := h.getTask(t, taskID)
	require.Equal(t, TaskStatusPreparing, task.Status,
		"FSM must stay PREPARING until every node has prep-acked")

	// node-2 ticks second: same story.
	h.tick(1)
	require.Equal(t, 1, h.nodes[1].provider.groupCount(taskID))
	require.Equal(t, 0, h.nodes[1].provider.swapCount(taskID))
	task = h.getTask(t, taskID)
	require.Equal(t, TaskStatusPreparing, task.Status)
	require.Len(t, task.PreparationCompletionAcks, 2)

	// node-3 ticks third: its prep-ack apply trips the
	// PREPARING → SWAPPING transition on the shared FSM. But the local
	// clone in node-3's tick body was captured BEFORE the ack-emission
	// landed; the scheduler only writes the FAILED flip back to that
	// clone (scheduler.go:574-576), not the SWAPPING flip. So node-3's
	// PHASE B does NOT fire in this tick — it observes the stale
	// PREPARING and skips. The post-tick FSM status IS SWAPPING.
	h.tick(2)
	require.Equal(t, 1, h.nodes[2].provider.groupCount(taskID))
	require.Equal(t, 0, h.nodes[2].provider.swapCount(taskID),
		"node-3 must NOT fire OnSwapRequested in its ack-emitting tick — the local clone reflects only FAILED, not SWAPPING (scheduler.go:574-576)")
	task = h.getTask(t, taskID)
	require.Equal(t, TaskStatusSwapping, task.Status,
		"after every node's prep-ack landed, FSM must be SWAPPING")

	// Now simulate the "leader loss between PHASE A.5 commit and PHASE
	// B firing" scenario: every node's next tick observes SWAPPING for
	// the first time (the previous tick saw PREPARING) and MUST fire
	// its own OnSwapRequested. A regression here would leave replicas
	// stuck in mixed-tokenization state forever — exactly the bug
	// PR #11328 is designed to prevent.
	h.tick(0)
	require.Equal(t, 1, h.nodes[0].provider.swapCount(taskID),
		"node-1: OnSwapRequested must fire on the SWAPPING tick following a barrier-lift it didn't itself trigger")
	h.tick(1)
	require.Equal(t, 1, h.nodes[1].provider.swapCount(taskID),
		"node-2: OnSwapRequested must fire on the SWAPPING tick following a barrier-lift it didn't itself trigger")
	h.tick(2)
	require.Equal(t, 1, h.nodes[2].provider.swapCount(taskID),
		"node-3: OnSwapRequested must fire on the SWAPPING tick after its own ack-emitting tick")

	// Idempotency on additional ticks across all nodes.
	h.tickAll()
	h.tickAll()
	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.swapCount(taskID),
			"node %s: OnSwapRequested must fire exactly once across additional ticks", n.id)
	}
}

// TestBarrier_G2_PartialPhaseBFailure pins gap G2 — the most important
// barrier-PHASE-B gap: some replicas SWAP successfully, one returns an
// error from OnSwapRequested. The post-completion ack with Success=false
// must flip the FSM to FAILED, MarkTaskFinalized must NOT issue
// SWAPPING → FINISHED, and the cluster-wide schema flip (the production
// side-effect in OnTaskCompleted) is skipped via the FAILED gate.
//
// This is the cross-replica failure shape that PR #11328 explicitly
// targets but never directly tests at the scheduler-multinode layer.
// The legacy TestMultiScheduler_AckBarrier_FailureAckTransitionsToFailed
// covers the same shape via OnGroupCompleted (PHASE A — the legacy
// pre-barrier path); none of the existing multinode tests exercise
// OnSwapRequested failure at all (the provider stubs OnSwapRequested
// to a no-op).
func TestBarrier_G2_PartialPhaseBFailure(t *testing.T) {
	defer leaktest.Check(t)()

	h := newBarrierHarness(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "barrier-g2-partial-swap-failure"
	barrierAddTaskOneUnitPerNode(t, h, taskID)
	barrierDriveToPreparing(t, h, taskID)
	barrierDriveAllAcksAndAssertSwapping(t, h, taskID)

	// After barrierDriveAllAcksAndAssertSwapping no node has fired SWAP
	// yet: each tick observed PREPARING on its local clone (PHASE A.5
	// only reflects FAILED back to the clone, not SWAPPING — see
	// scheduler.go:574-576 vs the missing PREPARING→SWAPPING reflection).
	// The FSM is SWAPPING after the third ack lands, but PHASE B fires
	// only on each scheduler's NEXT tick.
	for _, n := range h.nodes {
		require.Equal(t, 0, n.provider.swapCount(taskID),
			"prerequisite: node %s must NOT have fired SWAP in its ack-emitting tick", n.id)
	}

	h.nodes[1].provider.SetSwapRequestedError(fmt.Errorf("synthetic ENOSPC during swap on node-2"))

	// node-1 ticks: PHASE B fires, no error → success ack lands.
	// MissingPostCompletionAckNodes still contains node-2 + node-3, so
	// OnTaskCompleted does not fire on node-1.
	h.tick(0)
	require.Equal(t, 1, h.nodes[0].provider.swapCount(taskID),
		"node-1: OnSwapRequested must fire on its first SWAPPING-observing tick")
	require.Equal(t, 0, h.nodes[0].provider.taskCount(taskID),
		"node-1: OnTaskCompleted must NOT fire while node-2 / node-3 swap-acks are missing")
	task := h.getTask(t, taskID)
	require.Equal(t, TaskStatusSwapping, task.Status)

	// node-2 ticks: SetSwapRequestedError makes OnSwapRequested return
	// the synthetic error. The scheduler emits the swap-ack with
	// Success=false. The Manager's RecordPostCompletionAck apply flips
	// the FSM SWAPPING → FAILED. node-2's local-clone status reflects
	// FAILED in the same tick (scheduler reflects !success ack to
	// FAILED on the clone — scheduler.go:669-671), so OnTaskCompleted
	// fires on FAILED on node-2 in the same tick (per-node cleanup
	// must run on FAILED).
	h.tick(1)
	task = h.getTask(t, taskID)
	require.Equal(t, TaskStatusFailed, task.Status,
		"task must transition to FAILED on any node's swap-ack failure")
	require.NotEmpty(t, task.Error,
		"FAILED task must carry the per-node failure message for forensics")
	require.Contains(t, task.PostCompletionAcks, "node-2")
	require.False(t, task.PostCompletionAcks["node-2"].Success,
		"node-2's ack must record Success=false")
	require.Contains(t, task.PostCompletionAcks["node-2"].Error,
		"synthetic ENOSPC during swap on node-2",
		"node-2's swap error message must be preserved for forensics")
	require.GreaterOrEqual(t, h.nodes[1].provider.taskCount(taskID), 1,
		"node-2: OnTaskCompleted must fire on FAILED for per-node cleanup")

	// node-3 ticks: observes FAILED. PHASE B still fires on FAILED
	// (postStarted=true includes FAILED on scheduler.go:586-588) so the
	// per-shard SWAP runs on node-3 too — production reindex provider
	// guards against the FAILED gate at the schema-flip level (the
	// OnTaskCompleted call below), not at the per-shard swap level.
	// node-1 also re-ticks and observes FAILED; OnTaskCompleted fires
	// for per-node cleanup.
	h.tick(2)
	h.tick(0)
	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.taskCount(taskID),
			"node %s: OnTaskCompleted must fire exactly once on FAILED across ticks", n.id)
	}

	// MarkTaskFinalized must NOT have fired — FAILED is terminal at the
	// FSM layer; the barrier blocked SWAPPING → FINISHED precisely to
	// prevent a cluster-wide schema flip with one replica still on the
	// old tokenization.
	task = h.getTask(t, taskID)
	require.Equal(t, TaskStatusFailed, task.Status,
		"task must remain FAILED — MarkTaskFinalized must not promote past FAILED")

	// Total PHASE B fires across the cluster: exactly 3 (one per node).
	// node-1 fired SWAP successfully, node-2 fired SWAP and errored,
	// node-3 fired SWAP after observing FAILED (PHASE B post-FAILED
	// still runs for cleanup symmetry).
	total := 0
	for _, n := range h.nodes {
		total += n.provider.swapCount(taskID)
	}
	require.Equal(t, 3, total,
		"OnSwapRequested must fire exactly once per node across the cluster, regardless of success/failure")
}

// TestBarrier_G3_LatePrepAckArrivesInSwapping pins gap G3: a late
// PreparationCompleteAck that arrives AFTER the FSM has transitioned
// PREPARING → SWAPPING must be silently dropped — NOT errored, NOT
// state-flipped, NOT cause a panic. Specifically, a duplicate ack
// against a SWAPPING task must keep the task in SWAPPING with the
// original ack untouched.
//
// Production scenario: node-1's PreparationCompleteAck applies last
// and trips the SWAPPING transition. node-2's tick re-emits its ack
// (its preparationAckEmitted didn't reflect the first apply because
// the wire-side return errored and the in-memory map was not flipped
// at scheduler.go:561). node-2's ack now arrives in SWAPPING. The
// drop path is at manager.go:564-567.
func TestBarrier_G3_LatePrepAckArrivesInSwapping(t *testing.T) {
	defer leaktest.Check(t)()

	h := newBarrierHarness(t, []string{"node-1", "node-2"})
	defer h.close()

	taskID := "barrier-g3-late-prep-ack"
	barrierAddTaskOneUnitPerNode(t, h, taskID)
	barrierDriveToPreparing(t, h, taskID)
	barrierDriveAllAcksAndAssertSwapping(t, h, taskID)
	task := h.getTask(t, taskID)
	require.Equal(t, TaskStatusSwapping, task.Status)
	originalAcks := map[string]PostCompletionAck{}
	for k, v := range task.PreparationCompletionAcks {
		originalAcks[k] = v
	}
	require.Len(t, originalAcks, 2, "both nodes' prep acks must be recorded by SWAPPING")
	require.True(t, originalAcks["node-2"].Success,
		"prerequisite: node-2's original prep ack recorded Success=true")

	// Inject a duplicate prep ack on node-2 with Success=false — the
	// shape a retried wire-call would land in if the in-memory
	// preparationAckEmitted flag was never set. The Manager MUST drop
	// it: status stays SWAPPING, the original successful ack remains.
	require.NoError(t, h.ackRecorder.RecordDistributedTaskPreparationCompleteAck(
		context.Background(), h.namespace, taskID, 1, "node-2", false, "late duplicate prep ack with synthesized failure",
	))

	task = h.getTask(t, taskID)
	require.Equal(t, TaskStatusSwapping, task.Status,
		"SWAPPING task MUST silently drop a late prep-ack — no state flip back to FAILED")
	require.True(t, task.PreparationCompletionAcks["node-2"].Success,
		"original successful prep-ack MUST NOT be overwritten by the late duplicate")
	require.Equal(t, originalAcks["node-2"].AckedAt,
		task.PreparationCompletionAcks["node-2"].AckedAt,
		"original ack timestamp MUST survive a late duplicate")
	require.Empty(t, task.Error,
		"SWAPPING task MUST NOT pick up an error from a silently-dropped late prep-ack")

	// Repeat for the FAILED case: drive a separate barrier task to
	// FAILED via swap injection, then deliver a late prep ack — same
	// silent-drop expectation. The version on taskID2 has to be > the
	// first taskID's version so AddTask accepts it; both use version 1
	// because they have distinct IDs.
	taskID2 := "barrier-g3-late-prep-ack-into-failed"
	barrierAddTaskOneUnitPerNode(t, h, taskID2)
	barrierDriveToPreparing(t, h, taskID2)
	h.nodes[1].provider.SetSwapRequestedError(fmt.Errorf("force FAILED for late-prep-ack test"))
	barrierDriveAllAcksAndAssertSwapping(t, h, taskID2)
	h.tickAll()
	failedTask := h.getTask(t, taskID2)
	require.Equal(t, TaskStatusFailed, failedTask.Status,
		"prerequisite: task2 must be FAILED via the injected swap error")
	preFailedAcks := map[string]PostCompletionAck{}
	for k, v := range failedTask.PreparationCompletionAcks {
		preFailedAcks[k] = v
	}

	// Deliver a late prep-ack for an already-acked node (the manager's
	// 2-arm guard at manager.go:564-567 returns nil for terminal-state
	// tasks BEFORE the idempotency check fires, so the FAILED state
	// short-circuits whether or not the ack is a duplicate).
	require.NoError(t, h.ackRecorder.RecordDistributedTaskPreparationCompleteAck(
		context.Background(), h.namespace, taskID2, 1, "node-1", false, "late duplicate after task FAILED",
	))
	failedTask = h.getTask(t, taskID2)
	require.Equal(t, TaskStatusFailed, failedTask.Status,
		"FAILED task MUST silently drop a late prep-ack — no state flip")
	require.Equal(t, preFailedAcks, failedTask.PreparationCompletionAcks,
		"FAILED task's PreparationCompletionAcks MUST remain bit-for-bit identical")
}

// TestBarrier_G4_ParallelMigrationDifferentPropDuringBarrier pins gap G4:
// while one barrier task is in the PREPARING/SWAPPING window, a second
// barrier task on a DIFFERENT prop (different task ID, no payload
// overlap) must progress through its own barrier independently. The
// scheduler tick processes both tasks each iteration; each task has
// its own per-(task,group) callback-fired maps, so PHASE A/B firing on
// task-A must not be gated on task-B and vice versa.
//
// CheckConflict is per-payload (the conflict-detector reads payloads
// and decides) and we don't register one in this harness — so both
// tasks are accepted at AddTask time. This test asserts that beyond
// AddTask, the scheduler tick correctly multiplexes barrier callbacks
// across concurrent in-flight tasks.
func TestBarrier_G4_ParallelMigrationDifferentPropDuringBarrier(t *testing.T) {
	defer leaktest.Check(t)()

	h := newBarrierHarness(t, []string{"node-1", "node-2"})
	defer h.close()

	taskA := "barrier-g4-prop-a"
	taskB := "barrier-g4-prop-b"

	barrierAddTaskOneUnitPerNode(t, h, taskA)
	barrierAddTaskOneUnitPerNode(t, h, taskB)

	// Drive task-A halfway through: claim units, complete units → PREPARING.
	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
			context.Background(), h.namespace, taskA, 1, n.id, "u-"+n.id, 0.1,
		))
		require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
			context.Background(), h.namespace, taskB, 1, n.id, "u-"+n.id, 0.1,
		))
	}
	h.tickAll()
	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
			context.Background(), h.namespace, taskA, 1, n.id, "u-"+n.id,
		))
	}

	// Confirm task-A is in PREPARING, task-B still STARTED (units not
	// yet completed for B).
	tasks := h.listTasks(t)
	require.Len(t, tasks, 2)
	statuses := map[string]TaskStatus{}
	for _, task := range tasks {
		statuses[task.ID] = task.Status
	}
	require.Equal(t, TaskStatusPreparing, statuses[taskA])
	require.Equal(t, TaskStatusStarted, statuses[taskB])

	// Complete task-B's units mid-task-A's barrier window. task-B
	// transitions to PREPARING; task-A remains PREPARING.
	for _, n := range h.nodes {
		require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
			context.Background(), h.namespace, taskB, 1, n.id, "u-"+n.id,
		))
	}
	tasks = h.listTasks(t)
	statuses = map[string]TaskStatus{}
	for _, task := range tasks {
		statuses[task.ID] = task.Status
	}
	require.Equal(t, TaskStatusPreparing, statuses[taskA],
		"task-A must still be PREPARING — task-B's completion must not affect task-A")
	require.Equal(t, TaskStatusPreparing, statuses[taskB])

	// Tick all → both tasks' PHASE A + PHASE A.5 land for every node.
	// FSM transitions PREPARING → SWAPPING for both tasks.
	h.tickAll()
	tasks = h.listTasks(t)
	statuses = map[string]TaskStatus{}
	for _, task := range tasks {
		statuses[task.ID] = task.Status
	}
	require.Equal(t, TaskStatusSwapping, statuses[taskA],
		"task-A must advance to SWAPPING independently of task-B")
	require.Equal(t, TaskStatusSwapping, statuses[taskB],
		"task-B must advance to SWAPPING independently of task-A")

	// Tick all → PHASE B fires for every (node, task), post-completion
	// ack lands for every (node, task), MarkTaskFinalized issued for
	// both. End state: FINISHED for both.
	h.tickAll()
	h.tickAll()
	tasks = h.listTasks(t)
	statuses = map[string]TaskStatus{}
	for _, task := range tasks {
		statuses[task.ID] = task.Status
	}
	require.Equal(t, TaskStatusFinished, statuses[taskA],
		"task-A must reach FINISHED through its own barrier")
	require.Equal(t, TaskStatusFinished, statuses[taskB],
		"task-B must reach FINISHED through its own barrier")

	// Every node fired OnSwapRequested exactly once per task. Cross-task
	// callback contamination would show up as e.g. node-1 firing
	// OnSwapRequested for taskA twice.
	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.swapCount(taskA),
			"node %s: OnSwapRequested for taskA must fire exactly once", n.id)
		require.Equal(t, 1, n.provider.swapCount(taskB),
			"node %s: OnSwapRequested for taskB must fire exactly once", n.id)
		require.Equal(t, 1, n.provider.taskCount(taskA),
			"node %s: OnTaskCompleted for taskA must fire exactly once", n.id)
		require.Equal(t, 1, n.provider.taskCount(taskB),
			"node %s: OnTaskCompleted for taskB must fire exactly once", n.id)
	}
}

// TestBarrier_G5_SchemaVersionMismatchRollingUpgrade pins gap G5: a
// pre-PR-#11328 node deserializes a Task envelope that includes the
// new NeedsPreparationBarrier field as the field being silently
// missing — i.e. NeedsPreparationBarrier unmarshals as false. This is
// the backward-compat invariant: a v1.36 follower receiving a RAFT
// snapshot from a v1.37 leader (mid-rolling-upgrade) must not get the
// barrier silently degraded to the legacy path (the exact bug class
// PR #11328 closes).
//
// The deterministic check is a JSON round-trip: serialize a Task with
// NeedsPreparationBarrier=true via the live struct, then deserialize
// into a struct that doesn't know the field. The barrier field reads
// as zero-value (false) — which IS the legacy behavior. The test pins
// the contract that there is no defensive guard preventing this:
// rolling upgrades MUST go from pre-PR to post-PR (so a new leader's
// barrier-true tasks are seen by a still-old follower) and the
// backward-compat shape MUST be either explicit rejection OR the safe
// default. This test documents the actual current behavior so an
// upgrade-safety regression is caught.
func TestBarrier_G5_SchemaVersionMismatchRollingUpgrade(t *testing.T) {
	// Build a Task in the new (post-PR) shape with the barrier on.
	now := time.Now().UTC().Truncate(time.Millisecond)
	postPRTask := &Task{
		Namespace:               "ns",
		TaskDescriptor:          TaskDescriptor{ID: "barrier-task", Version: 7},
		NeedsPreparationBarrier: true,
		Status:                  TaskStatusPreparing,
		StartedAt:               now,
		Units: map[string]*Unit{
			"u-1": {ID: "u-1", NodeID: "node-1", Status: UnitStatusCompleted, FinishedAt: now},
		},
		PreparationCompletionAcks: map[string]PostCompletionAck{
			"node-1": {Success: true, AckedAt: now},
		},
	}

	wireBytes, err := json.Marshal(postPRTask)
	require.NoError(t, err)
	require.Contains(t, string(wireBytes), `"needsPreparationBarrier":true`,
		"post-PR serialization MUST emit the barrier field — without it pre-PR followers see only the legacy shape")

	// Simulate a pre-PR-#11328 node: struct with all the OLD fields but
	// NO NeedsPreparationBarrier. The barrierLegacyTask shape captures
	// the v1.36 layout exactly. Unmarshalling the post-PR wire bytes
	// into it MUST succeed (JSON unmarshal silently drops unknown fields
	// by default) and the legacy node sees the task without the barrier.
	type barrierLegacyTask struct {
		Namespace                 string                       `json:"namespace"`
		TaskDescriptor            TaskDescriptor               `json:",inline"`
		Payload                   []byte                       `json:"payload"`
		Status                    TaskStatus                   `json:"status"`
		StartedAt                 time.Time                    `json:"startedAt"`
		FinishedAt                time.Time                    `json:"finishedAt"`
		Error                     string                       `json:"error,omitempty"`
		Units                     map[string]*Unit             `json:"units,omitempty"`
		PostCompletionAcks        map[string]PostCompletionAck `json:"postCompletionAcks,omitempty"`
		PreparationCompletionAcks map[string]PostCompletionAck `json:"preparationCompletionAcks,omitempty"`
	}
	var legacy barrierLegacyTask
	require.NoError(t, json.Unmarshal(wireBytes, &legacy),
		"pre-PR follower MUST successfully deserialize a post-PR task — unknown fields are silently dropped")

	// Verify the legacy follower preserves the rest of the task shape —
	// the Units map, the existing acks, the status. Note: the legacy
	// follower sees Status=PREPARING which is a NEW value (PR also added
	// the TaskStatusPreparing constant). The follower's switch/case on
	// status would silently fall through to a default branch.
	require.Equal(t, postPRTask.Namespace, legacy.Namespace)
	require.Equal(t, postPRTask.Units["u-1"].ID, legacy.Units["u-1"].ID)
	require.Equal(t, postPRTask.PreparationCompletionAcks, legacy.PreparationCompletionAcks,
		"PreparationCompletionAcks must round-trip even on a legacy follower (the field name is the same, only NeedsPreparationBarrier is new)")
	require.Equal(t, TaskStatus("PREPARING"), legacy.Status,
		"the new PREPARING status string survives the round-trip — legacy code-paths must defensively handle unknown TaskStatus values to avoid silent state flips")

	// Round-trip the other way: a pre-PR Task envelope (no
	// NeedsPreparationBarrier field at all) must deserialize cleanly
	// into the post-PR Task struct with the barrier defaulting to
	// false. A legacy task sent by a v1.36 leader to a v1.37 follower
	// MUST be treated as a legacy task on the new follower — not
	// silently upgraded to barrier mode.
	preReleasedWire := []byte(`{
		"namespace": "ns",
		"id": "legacy-task",
		"version": 4,
		"status": "SWAPPING",
		"startedAt": "2026-05-22T00:00:00Z",
		"units": {"u-1": {"id": "u-1", "nodeId": "node-1", "status": "COMPLETED"}}
	}`)
	var postPR Task
	require.NoError(t, json.Unmarshal(preReleasedWire, &postPR),
		"post-PR follower MUST successfully deserialize a pre-PR task envelope")
	require.False(t, postPR.NeedsPreparationBarrier,
		"absent NeedsPreparationBarrier in the envelope MUST default to false — a pre-PR leader sending the envelope intends legacy semantics")
	require.Equal(t, TaskStatusSwapping, postPR.Status,
		"legacy task envelopes use the old SWAPPING status; the post-PR Task struct preserves that")
}

// TestBarrier_G6_RestartMidPhaseARecovery pins gap G6: a node that
// crashed mid-PREPARING (e.g. SIGKILL'd between PHASE A firing and
// PHASE A.5 ack emission) must, after restart, re-fire OnGroupCompleted
// for its local groups and emit a fresh prep-ack. The bootstrap
// pre-mark suppression test (scheduler_test.go:1474) already pins
// that PREPARING/SWAPPING tasks are NOT pre-marked — meaning the new
// scheduler's empty preparationCallbackFired map allows re-firing on
// the next tick. This test exercises the end-to-end recovery flow.
//
// We model the restart by spinning a brand-new scheduler with a brand
// new (empty) callback-fired maps, pointing at the same Manager and
// the same provider. The barrier-recording provider is per-node, so
// re-using it across the restart preserves "the same provider survives
// the restart" semantics; the in-memory callback-fired maps are per-
// scheduler and DON'T survive, which is exactly the production reality.
func TestBarrier_G6_RestartMidPhaseARecovery(t *testing.T) {
	defer leaktest.Check(t)()

	h := newBarrierHarness(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "barrier-g6-restart-mid-prep"
	barrierAddTaskOneUnitPerNode(t, h, taskID)
	barrierDriveToPreparing(t, h, taskID)

	// node-1 ticks: PHASE A fires + PHASE A.5 ack lands. node-2 and
	// node-3 have NOT ticked yet — they're "the restarted nodes that
	// crashed mid-PREPARING".
	h.tick(0)
	require.Equal(t, 1, h.nodes[0].provider.groupCount(taskID),
		"prerequisite: node-1 fired PHASE A")
	tasks := h.listTasks(t)
	require.Equal(t, TaskStatusPreparing, tasks[0].Status)
	require.Len(t, tasks[0].PreparationCompletionAcks, 1)

	// Simulate node-2 restart: replace its scheduler with a new one
	// bound to the same manager + same provider. The new scheduler's
	// in-memory maps (preparationCallbackFired, preparationAckEmitted,
	// etc.) start empty. The provider survives the restart (it's
	// per-node and lives outside the scheduler instance, like a
	// persistent on-disk reindex sentinel).
	freshLogger, _ := logrustest.NewNullLogger()
	freshScheduler := NewScheduler(SchedulerParams{
		CompletionRecorder: h.completionRec,
		TaskLister:         h.manager,
		TaskCleaner:        h.cleaner,
		TaskFinalizer:      h.finalizer,
		Providers:          map[string]Provider{h.namespace: h.nodes[1].provider},
		Clock:              h.clock,
		Logger:             freshLogger,
		MetricsRegisterer:  monitoring.NoopRegisterer,
		LocalNode:          "node-2",
		CompletedTaskTTL:   24 * time.Hour,
		TickInterval:       30 * time.Second,
		AckRecorder:        h.ackRecorder,
	})
	// Drop the old scheduler (no Start was ever called, no goroutines
	// to leak) and patch the harness's node slot.
	h.nodes[1].scheduler.Close()
	h.nodes[1].scheduler = freshScheduler

	// node-3 also restarted; rebuild it the same way for completeness.
	freshScheduler3 := NewScheduler(SchedulerParams{
		CompletionRecorder: h.completionRec,
		TaskLister:         h.manager,
		TaskCleaner:        h.cleaner,
		TaskFinalizer:      h.finalizer,
		Providers:          map[string]Provider{h.namespace: h.nodes[2].provider},
		Clock:              h.clock,
		Logger:             freshLogger,
		MetricsRegisterer:  monitoring.NoopRegisterer,
		LocalNode:          "node-3",
		CompletedTaskTTL:   24 * time.Hour,
		TickInterval:       30 * time.Second,
		AckRecorder:        h.ackRecorder,
	})
	h.nodes[2].scheduler.Close()
	h.nodes[2].scheduler = freshScheduler3

	// Fresh schedulers tick: their empty preparationCallbackFired maps
	// MUST re-fire OnGroupCompleted (production rehydrate path), and
	// the PHASE A.5 ack MUST be emitted. The Manager's idempotency on
	// duplicate acks (manager.go:580-583) means even if node-2 had
	// already acked pre-restart, the post-restart re-ack is harmless.
	freshScheduler.tick()
	freshScheduler3.tick()

	groupCountAfterRestartNode2 := h.nodes[1].provider.groupCount(taskID)
	groupCountAfterRestartNode3 := h.nodes[2].provider.groupCount(taskID)
	require.Equal(t, 1, groupCountAfterRestartNode2,
		"node-2 restart: OnGroupCompleted MUST re-fire on the fresh scheduler's first tick")
	require.Equal(t, 1, groupCountAfterRestartNode3,
		"node-3 restart: OnGroupCompleted MUST re-fire on the fresh scheduler's first tick")

	// All three prep-acks must now be present; FSM must have advanced
	// PREPARING → SWAPPING.
	tasks = h.listTasks(t)
	require.Len(t, tasks[0].PreparationCompletionAcks, 3,
		"every node's prep-ack must be present after restart recovery")
	require.Equal(t, TaskStatusSwapping, tasks[0].Status,
		"FSM must lift to SWAPPING after the restarted nodes re-emit their prep-acks")
}

// TestBarrier_G7_PhaseAAckEmissionForCrashedGroupNode: multi-group
// task where group-a is owned only by node-1 and group-b only by
// node-2; node-2 crashes pre-prep-ack. The FSM barrier-lift
// (allExpectedPreparationAcksLanded) only counts nodes with local
// units, so node-3's vacuous "all-local-groups-fired" ack is ignored
// and the barrier stays up until node-2 recovers.
func TestBarrier_G7_PhaseAAckEmissionForCrashedGroupNode(t *testing.T) {
	defer leaktest.Check(t)()

	h := newBarrierHarness(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "barrier-g7-multi-group-crash"

	// Multi-group task: group-a owned by node-1 only, group-b owned by
	// node-2 only. node-3 owns zero local units (it's a witness node
	// that holds no shards for this collection).
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitSpecs: []*cmd.UnitSpec{
			{Id: "u-a-1", GroupId: "group-a"},
			{Id: "u-b-2", GroupId: "group-b"},
		},
		NeedsPreparationBarrier: true,
	}), 1))

	// Claim units. node-3 never reports progress because it owns no units.
	require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
		context.Background(), h.namespace, taskID, 1, "node-1", "u-a-1", 0.1))
	require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
		context.Background(), h.namespace, taskID, 1, "node-2", "u-b-2", 0.1))

	h.tickAll()

	// Complete both units → FSM transitions PREPARING.
	require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
		context.Background(), h.namespace, taskID, 1, "node-1", "u-a-1"))
	require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
		context.Background(), h.namespace, taskID, 1, "node-2", "u-b-2"))

	task := h.getTask(t, taskID)
	require.Equal(t, TaskStatusPreparing, task.Status)

	// node-1 ticks: PHASE A fires for group-a; prep-ack emitted.
	h.tick(0)
	require.Equal(t, 1, h.nodes[0].provider.groupCount(taskID))
	task = h.getTask(t, taskID)
	require.Contains(t, task.PreparationCompletionAcks, "node-1",
		"node-1: prep-ack must land after PHASE A fires for its group")

	// node-3 ticks: NO local units in either group → no PHASE A fire.
	// The scheduler's allLocalGroupsPreparationFiredLocked vacuously
	// returns true (the for-loop body never runs), so the prep-ack
	// emission gate DOES fire — recording a vacuous Success=true ack
	// for node-3. This is benign: allExpectedPreparationAcksLanded
	// (the barrier-lift predicate at the FSM layer) only counts nodes
	// with local units, so node-3's ack does not contribute to the
	// barrier.
	h.tick(2)
	require.Equal(t, 0, h.nodes[2].provider.groupCount(taskID),
		"node-3: OnGroupCompleted MUST NOT fire — no local units in any group")
	task = h.getTask(t, taskID)
	require.Equal(t, TaskStatusPreparing, task.Status,
		"FSM must hold at PREPARING — node-2's ack is still missing despite node-3's vacuous ack")
	require.NotContains(t, task.PreparationCompletionAcks, "node-2",
		"node-2's prep-ack must NOT be present (it hasn't ticked yet)")
	require.False(t, allExpectedPreparationAcksLanded(task),
		"barrier predicate must be false — node-2 owns local units and has not acked")

	// Simulate node-2 crash: don't tick it, and rebuild its scheduler
	// with empty maps (as if the binary restarted). The provider
	// (which would be backed by an on-disk reindex sentinel in
	// production) survives the restart so OnGroupCompleted can re-fire
	// idempotently.
	freshLogger, _ := logrustest.NewNullLogger()
	freshScheduler := NewScheduler(SchedulerParams{
		CompletionRecorder: h.completionRec,
		TaskLister:         h.manager,
		TaskCleaner:        h.cleaner,
		TaskFinalizer:      h.finalizer,
		Providers:          map[string]Provider{h.namespace: h.nodes[1].provider},
		Clock:              h.clock,
		Logger:             freshLogger,
		MetricsRegisterer:  monitoring.NoopRegisterer,
		LocalNode:          "node-2",
		CompletedTaskTTL:   24 * time.Hour,
		TickInterval:       30 * time.Second,
		AckRecorder:        h.ackRecorder,
	})
	h.nodes[1].scheduler.Close()
	h.nodes[1].scheduler = freshScheduler

	// Restarted node-2 ticks: fresh map allows OnGroupCompleted to fire
	// for group-b, then PHASE A.5 emits node-2's prep-ack.
	// allExpectedPreparationAcksLanded now true → PREPARING → SWAPPING.
	freshScheduler.tick()

	task = h.getTask(t, taskID)
	require.Contains(t, task.PreparationCompletionAcks, "node-2",
		"node-2: prep-ack must land after recovery re-fires PHASE A for group-b")
	require.Equal(t, TaskStatusSwapping, task.Status,
		"FSM must lift to SWAPPING after the crashed node's group is recovered")
}

// TestBarrier_G8_SnapshotRestoreMidPreparing pins gap G8: the new
// PreparationCompletionAcks map on Task must survive a JSON snapshot/restore
// round-trip through Manager.Snapshot / Manager.Restore — a follower
// joining mid-PREPARING via snapshot install must see the partial ack
// map exactly as the leader committed it.
//
// The existing TestTask_Clone_DeepCopiesAckMaps test covers the
// in-memory Clone path; this test covers the JSON snapshot path used
// by RAFT install-snapshot.
func TestBarrier_G8_SnapshotRestoreMidPreparing(t *testing.T) {
	defer leaktest.Check(t)()

	h := newBarrierHarness(t, []string{"node-1", "node-2", "node-3"})
	defer h.close()

	taskID := "barrier-g8-snapshot-restore"
	barrierAddTaskOneUnitPerNode(t, h, taskID)
	barrierDriveToPreparing(t, h, taskID)

	// Tick node-1 + node-2 only: partial prep-acks land; FSM stays
	// PREPARING. node-3 is the new follower that hasn't ticked.
	h.tick(0)
	h.tick(1)
	tasks := h.listTasks(t)
	require.Equal(t, TaskStatusPreparing, tasks[0].Status)
	require.Len(t, tasks[0].PreparationCompletionAcks, 2,
		"only the two ticked nodes' prep-acks must be present pre-snapshot")
	require.Contains(t, tasks[0].PreparationCompletionAcks, "node-1")
	require.Contains(t, tasks[0].PreparationCompletionAcks, "node-2")

	// Take the snapshot, then build a fresh Manager and restore into
	// it. This is the install-snapshot path a follower joining
	// mid-PREPARING walks.
	snapshotBytes, err := h.manager.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snapshotBytes)

	freshLogger, _ := logrustest.NewNullLogger()
	freshManager := NewManager(ManagerParameters{
		Clock:            h.clock,
		CompletedTaskTTL: 24 * time.Hour,
		Logger:           freshLogger,
	})
	require.NoError(t, freshManager.Restore(snapshotBytes))

	// Read the restored task and assert the partial ack map round-tripped.
	restoredTasks, err := freshManager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	require.Len(t, restoredTasks[h.namespace], 1)
	restored := restoredTasks[h.namespace][0]

	require.Equal(t, TaskStatusPreparing, restored.Status,
		"restored task must preserve PREPARING status — a follower joining mid-barrier must see the same FSM state as the leader")
	require.True(t, restored.NeedsPreparationBarrier,
		"NeedsPreparationBarrier must survive snapshot/restore — otherwise the follower silently degrades to legacy SWAPPING semantics")
	require.Len(t, restored.PreparationCompletionAcks, 2,
		"partial PreparationCompletionAcks must survive snapshot/restore")
	require.Contains(t, restored.PreparationCompletionAcks, "node-1",
		"node-1's prep-ack must be restored")
	require.Contains(t, restored.PreparationCompletionAcks, "node-2",
		"node-2's prep-ack must be restored")
	require.NotContains(t, restored.PreparationCompletionAcks, "node-3",
		"node-3's missing prep-ack must remain missing after restore — restoration MUST be exact")
	require.True(t, restored.PreparationCompletionAcks["node-1"].Success,
		"node-1's success flag must survive restore")
	require.True(t, restored.PreparationCompletionAcks["node-2"].Success,
		"node-2's success flag must survive restore")

	// allExpectedPreparationAcksLanded on the restored task should be
	// false (node-3 missing). The barrier predicate is a pure transform
	// of the restored state, so applying node-3's ack against the
	// restored Manager must lift the barrier exactly as it would on the
	// leader.
	require.False(t, allExpectedPreparationAcksLanded(restored),
		"barrier predicate must be false on the restored partial state")

	require.NoError(t, freshManager.RecordPreparationCompleteAck(
		toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
			Namespace:         h.namespace,
			Id:                taskID,
			Version:           restored.Version,
			NodeId:            "node-3",
			Success:           true,
			AckedAtUnixMillis: h.clock.Now().UnixMilli(),
		}),
	))
	restoredTasks, err = freshManager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	require.Equal(t, TaskStatusSwapping, restoredTasks[h.namespace][0].Status,
		"after the restored Manager records node-3's prep-ack, the barrier must lift to SWAPPING — proving the restore preserved enough state for the FSM transition to fire correctly")
}

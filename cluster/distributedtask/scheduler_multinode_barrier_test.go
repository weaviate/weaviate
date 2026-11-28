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

// Helper-name discipline: every type/func/var introduced here is
// prefixed with `barrier*` so the file can land alongside other parallel
// agents' additions without collisions. The existing multiSchedulerHarness,
// fanoutAckRecorder, fanoutRecorder, directCleaner, directFinalizer, and
// fanoutNotifier types are REUSED — DO NOT redefine.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
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

type barrierRecordingProvider struct {
	*testTaskProvider

	mu           sync.Mutex
	groupCalls   []string // taskID per OnGroupCompleted fire (PHASE A in barrier mode)
	taskCalls    []string // taskID per OnTaskCompleted fire
	groupCallsBy map[string]int
	taskCallsBy  map[string]int
	groupCompErr error
	// groupErrs overrides groupCompErr for one group.
	groupErrs map[string]error
	// Status each callback was handed: the provider's only view of the task.
	taskStatuses []TaskStatus
	taskDecided  [][]string
}

func newBarrierRecordingProvider(t *testing.T) *barrierRecordingProvider {
	return &barrierRecordingProvider{
		testTaskProvider: newTestTaskProvider(t, nil),
		groupCallsBy:     map[string]int{},
		taskCallsBy:      map[string]int{},
	}
}

func (p *barrierRecordingProvider) OnGroupCompleted(task *Task, groupID string, _ []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groupCalls = append(p.groupCalls, task.ID)
	p.groupCallsBy[task.ID]++
	if err, ok := p.groupErrs[groupID]; ok {
		return err
	}
	if p.groupCompErr != nil {
		return p.groupCompErr
	}
	return nil
}

func (p *barrierRecordingProvider) OnTaskCompleted(task *Task) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.taskCalls = append(p.taskCalls, task.ID)
	p.taskCallsBy[task.ID]++
	p.taskStatuses = append(p.taskStatuses, task.Status)
	p.taskDecided = append(p.taskDecided, slices.Sorted(maps.Keys(task.SwapDecidedGroups)))
	return nil
}

func (p *barrierRecordingProvider) SetGroupCompletedError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groupCompErr = err
}

func (p *barrierRecordingProvider) SetGroupErrors(errs map[string]error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groupErrs = errs
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
	mgr := NewManager(ManagerParameters{Logger: logger})

	h := &barrierHarness{
		t:             t,
		clock:         clock,
		logger:        logger,
		namespace:     "tasks-namespace",
		manager:       mgr,
		completionRec: &fanoutRecorder{t: t, manager: mgr, clock: clock},
		cleaner:       &directCleaner{t: t, manager: mgr},
		finalizer:     newDirectFinalizer(t, mgr, clock),
		ackRecorder:   &fanoutAckRecorder{t: t, manager: mgr, clock: clock},
		notifier:      &fanoutNotifier{},
	}

	for _, id := range nodeIDs {
		prov := newBarrierRecordingProvider(t)
		params := SchedulerParams{
			CompletionRecorder: h.completionRec,
			TaskSource:         mgr,
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

// restart swaps in a fresh scheduler; the provider (on-disk state) survives.
func (h *barrierHarness) restart(nodeIdx int) *Scheduler {
	return h.restartWith(nodeIdx, h.nodes[nodeIdx].provider)
}

func (h *barrierHarness) restartWith(nodeIdx int, provider Provider) *Scheduler {
	node := h.nodes[nodeIdx]
	logger, _ := logrustest.NewNullLogger()
	node.scheduler.Close()
	node.scheduler = NewScheduler(SchedulerParams{
		CompletionRecorder: h.completionRec,
		TaskSource:         h.manager,
		TaskCleaner:        h.cleaner,
		TaskFinalizer:      h.finalizer,
		Providers:          map[string]Provider{h.namespace: provider},
		Clock:              h.clock,
		Logger:             logger,
		MetricsRegisterer:  monitoring.NoopRegisterer,
		LocalNode:          node.id,
		CompletedTaskTTL:   24 * time.Hour,
		TickInterval:       30 * time.Second,
		AckRecorder:        h.ackRecorder,
	})
	return node.scheduler
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
	addTaskOneUnitPerNode(t, h, taskID, true)
}

func addTaskOneUnitPerNode(t *testing.T, h *barrierHarness, taskID string, needsBarrier bool) {
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
		NeedsPreparationBarrier: needsBarrier,
	}), 1))
}

// barrierDriveToPreparing claims every unit + completes every unit so
// the FSM transitions to PREPARING. After this returns, the next call
// to tick on any node fires PHASE A (OnGroupCompleted) and PHASE A.5
// (prep-complete ack emission).
func barrierDriveToPreparing(t *testing.T, h *barrierHarness, taskID string) {
	t.Helper()
	completeAllUnits(t, h, taskID)
	task := h.getTask(t, taskID)
	require.Equal(t, TaskStatusPreparing, task.Status,
		"barrier task with all units terminal MUST land in PREPARING")
}

func completeAllUnits(t *testing.T, h *barrierHarness, taskID string) {
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

// A duplicate PreparationCompleteAck after PREPARING → SWAPPING is dropped.
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
	decidedAt, decided := task.SwapDecidedGroups[""]
	require.True(t, decided, "prerequisite: both nodes' prep acks decided the group by SWAPPING")

	h.clock.Advance(time.Minute)
	require.NoError(t, h.ackRecorder.RecordDistributedTaskPreparationCompleteAck(
		context.Background(), h.namespace, taskID, 1, "", "node-2", false, "late duplicate prep ack with synthesized failure",
	))

	task = h.getTask(t, taskID)
	require.Equal(t, TaskStatusSwapping, task.Status,
		"SWAPPING task MUST silently drop a late prep-ack — no state flip back to FAILED")
	require.Equal(t, decidedAt, task.SwapDecidedGroups[""],
		"the decision MUST survive a late duplicate")
	require.Empty(t, task.Error,
		"SWAPPING task MUST NOT pick up an error from a silently-dropped late prep-ack")

	// Repeat for the FAILED case: drive a separate barrier task to
	// FAILED via a failed prep ack, then deliver a late prep ack — same
	// silent-drop expectation. The version on taskID2 has to be > the
	// first taskID's version so AddTask accepts it; both use version 1
	// because they have distinct IDs.
	taskID2 := "barrier-g3-late-prep-ack-into-failed"
	barrierAddTaskOneUnitPerNode(t, h, taskID2)
	barrierDriveToPreparing(t, h, taskID2)
	require.NoError(t, h.ackRecorder.RecordDistributedTaskPreparationCompleteAck(
		context.Background(), h.namespace, taskID2, 1, "", "node-1", false, "force FAILED for late-prep-ack test",
	))
	failedTask := h.getTask(t, taskID2)
	require.Equal(t, TaskStatusFailed, failedTask.Status,
		"prerequisite: task2 must be FAILED via the failed prep ack")
	preFailedDecisions := failedTask.SwapDecidedGroups

	// Deliver a late prep-ack for an already-acked node (the manager's
	// 2-arm guard at manager.go:564-567 returns nil for terminal-state
	// tasks BEFORE the idempotency check fires, so the FAILED state
	// short-circuits whether or not the ack is a duplicate).
	require.NoError(t, h.ackRecorder.RecordDistributedTaskPreparationCompleteAck(
		context.Background(), h.namespace, taskID2, 1, "", "node-1", false, "late duplicate after task FAILED",
	))
	failedTask = h.getTask(t, taskID2)
	require.Equal(t, TaskStatusFailed, failedTask.Status,
		"FAILED task MUST silently drop a late prep-ack — no state flip")
	require.Equal(t, preFailedDecisions, failedTask.SwapDecidedGroups,
		"FAILED task's decisions MUST remain bit-for-bit identical")
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

	for _, n := range h.nodes {
		require.Equal(t, 1, n.provider.taskCount(taskA),
			"node %s: OnTaskCompleted for taskA must fire exactly once", n.id)
		require.Equal(t, 1, n.provider.taskCount(taskB),
			"node %s: OnTaskCompleted for taskB must fire exactly once", n.id)
	}
}

// TestBarrier_G6_RestartMidPhaseARecovery: node crashes between
// PHASE A and PHASE A.5; after restart its fresh scheduler must re-
// fire OnGroupCompleted and emit a prep-ack. Restart is modeled by
// spinning a new Scheduler against the same Manager + provider —
// in-memory maps reset (per-scheduler), provider state survives.
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
	require.Len(t, tasks[0].PreparedGroups[""], 1)

	freshScheduler := h.restart(1)

	// node-3 also restarted; rebuild it the same way for completeness.
	freshScheduler3 := h.restart(2)

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
	require.True(t, tasks[0].GroupSwapDecided(""),
		"every node's prep-ack must have landed after restart recovery")
	require.Equal(t, TaskStatusSwapping, tasks[0].Status,
		"FSM must lift to SWAPPING after the restarted nodes re-emit their prep-acks")
}

// group-a (node-1 only) decides alone; group-b (node-2 only, crashed pre-ack) waits for node-2.
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
	require.True(t, task.GroupSwapDecided("group-a"),
		"node-1 alone owns group-a, so its prep-ack decides it")

	h.tick(2)
	require.Equal(t, 0, h.nodes[2].provider.groupCount(taskID),
		"node-3: OnGroupCompleted MUST NOT fire — no local units in any group")
	task = h.getTask(t, taskID)
	require.Equal(t, TaskStatusPreparing, task.Status,
		"FSM must hold at PREPARING — node-2's ack for group-b is still missing")
	require.False(t, task.GroupSwapDecided("group-b"),
		"group-b must stay undecided until node-2 acks")

	// Simulate node-2 crash: don't tick it, and rebuild its scheduler
	// with empty maps (as if the binary restarted). The provider
	// (which would be backed by an on-disk reindex sentinel in
	// production) survives the restart so OnGroupCompleted can re-fire
	// idempotently.
	freshScheduler := h.restart(1)

	// Restarted node-2 ticks: fresh map allows OnGroupCompleted to fire
	// for group-b, then PHASE A.5 emits node-2's prep-ack.
	freshScheduler.tick()

	task = h.getTask(t, taskID)
	require.True(t, task.GroupSwapDecided("group-b"),
		"node-2: prep-ack must land after recovery re-fires PHASE A for group-b")
	require.Equal(t, TaskStatusSwapping, task.Status,
		"FSM must lift to SWAPPING after the crashed node's group is recovered")
}

// PreparedGroups survives a JSON snapshot/restore round-trip.
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
	require.Len(t, tasks[0].PreparedGroups[""], 2,
		"only the two ticked nodes' prep-acks must be present pre-snapshot")
	require.Contains(t, tasks[0].PreparedGroups[""], "node-1")
	require.Contains(t, tasks[0].PreparedGroups[""], "node-2")

	// Take the snapshot, then build a fresh Manager and restore into
	// it. This is the install-snapshot path a follower joining
	// mid-PREPARING walks.
	snapshotBytes, err := h.manager.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snapshotBytes)

	freshManager := NewManager(ManagerParameters{
		Logger: h.logger,
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
	// AckedAt is compared with Equal: the JSON round trip changes its Location.
	require.Len(t, restored.PreparedGroups, 1)
	require.Len(t, restored.PreparedGroups[""], 2)
	for node, want := range tasks[0].PreparedGroups[""] {
		got := restored.PreparedGroups[""][node]
		require.True(t, want.AckedAt.Equal(got.AckedAt), "%s: AckedAt", node)
		want.AckedAt, got.AckedAt = time.Time{}, time.Time{}
		require.Equal(t, want, got, "%s: partial PreparedGroups must survive snapshot/restore exactly", node)
	}
	require.False(t, restored.GroupSwapDecided(""),
		"the group must stay undecided on the restored partial state")

	require.NoError(t, freshManager.RecordPreparationCompleteAck(
		toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
			Namespace:         h.namespace,
			Id:                taskID,
			Version:           restored.Version,
			GroupId:           new(""),
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

// Pins that a cancel after every group's swap is decided leaves the migration to finish and flip.
func TestBarrier_G9_CancelAfterDecisionStillFinishes(t *testing.T) {
	defer leaktest.Check(t)()

	h := newBarrierHarness(t, []string{"node-1", "node-2"})
	defer h.close()

	barrierAddTaskOneUnitPerNode(t, h, "T9")
	barrierDriveToPreparing(t, h, "T9")
	barrierDriveAllAcksAndAssertSwapping(t, h, "T9")

	err := h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
		Namespace:             h.namespace,
		Id:                    "T9",
		Version:               1,
		CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
	}))
	require.ErrorIs(t, err, ErrNothingLeftToCancel)
	require.Equal(t, TaskStatusSwapping, h.getTask(t, "T9").Status)

	h.tickAll()
	h.tickAll()
	require.Equal(t, TaskStatusFinished, h.getTask(t, "T9").Status)
	require.ElementsMatch(t, []TaskStatus{TaskStatusSwapping, TaskStatusFinished},
		append(h.nodes[0].provider.taskStatuses, h.nodes[1].provider.taskStatuses...),
		"the first node to see every ack runs the SWAPPING schema flip; the other sees it finished")
}

// Regression: a node's own failed PREP ack must hand callbacks FAILED, not a stale PREPARING.
func TestBarrier_FailedPhaseHandsCallbacksFailedStatus(t *testing.T) {
	groupErr := func(p *barrierRecordingProvider) {
		p.SetGroupCompletedError(fmt.Errorf("synthetic EIO on node-2"))
	}
	tests := []struct {
		name         string
		needsBarrier bool
		// inject makes node-2's own phase fail.
		inject func(p *barrierRecordingProvider)
	}{
		{
			name:         "prep failure",
			needsBarrier: true,
			inject:       groupErr,
		},
		{
			name:   "non-barrier group failure",
			inject: groupErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer leaktest.Check(t)()

			h := newBarrierHarness(t, []string{"node-1", "node-2", "node-3"})
			defer h.close()

			taskID := "barrier-failed-phase-status"
			addTaskOneUnitPerNode(t, h, taskID, tt.needsBarrier)
			completeAllUnits(t, h, taskID)
			failing := h.nodes[1].provider
			tt.inject(failing)

			h.tick(0)
			h.tick(1)
			require.Equal(t, TaskStatusFailed, h.getTask(t, taskID).Status,
				"node-2's failed ack must fail the task")
			h.tick(1)
			require.Equal(t, []TaskStatus{TaskStatusFailed}, failing.taskStatuses,
				"node-2 must run FAILED cleanup, never the SWAPPING schema flip")

			h.tickAll()
			h.tickAll()
			for _, n := range h.nodes {
				require.Equal(t, []TaskStatus{TaskStatusFailed}, n.provider.taskStatuses,
					"node %s: OnTaskCompleted must fire once, on FAILED", n.id)
			}
		})
	}
}

// Pins that FAILED cleanup reads the status RAFT committed, never one a node assumed from its own failed PREP ack.
func TestBarrier_FailedPrepAckLeavesStatusToRaft(t *testing.T) {
	addTask := func(t *testing.T, h *barrierHarness, units map[string]string) {
		specs := []*cmd.UnitSpec{}
		for unit := range units {
			specs = append(specs, &cmd.UnitSpec{Id: unit, GroupId: "tenant-" + unit[:1]})
		}
		require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: h.namespace, Id: "T", SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
			UnitSpecs: specs, NeedsPreparationBarrier: true,
		}), 1))
		for unit, node := range units {
			require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
				context.Background(), h.namespace, "T", 1, node, unit, 0.1))
		}
		h.tickAll()
	}
	complete := func(t *testing.T, h *barrierHarness, unit, node string) {
		require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
			context.Background(), h.namespace, "T", 1, node, unit))
	}
	units := map[string]string{"a-1": "node-1", "a-2": "node-2", "b-1": "node-1", "b-2": "node-2"}

	for _, tc := range []struct {
		name string
		run  func(t *testing.T, h *barrierHarness)
		// wantDecided is what node-2's FAILED cleanup must see; nil means no cleanup runs.
		wantDecided []string
	}{
		{
			name: "a tenant decided in the same tick as another tenant's failure",
			run: func(t *testing.T, h *barrierHarness) {
				for _, unit := range []string{"a-1", "a-2", "b-1"} {
					complete(t, h, unit, units[unit])
				}
				h.tick(0)
				complete(t, h, "b-2", "node-2")
				h.nodes[1].provider.SetGroupErrors(map[string]error{"tenant-b": errors.New("synthetic EIO")})
				h.tick(1)
				h.tick(1)
			},
			wantDecided: []string{"tenant-a"},
		},
		{
			name: "a failed PREP re-run whose success RAFT already recorded",
			run: func(t *testing.T, h *barrierHarness) {
				complete(t, h, "a-1", "node-1")
				complete(t, h, "a-2", "node-2")
				h.tick(1)
				h.restart(1)
				h.nodes[1].provider.SetGroupCompletedError(errors.New("synthetic EIO"))
				h.tick(1)
				h.tick(1)
				require.Equal(t, TaskStatusStarted, h.getTask(t, "T").Status, "RAFT dropped the second ack")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer leaktest.Check(t)()
			h := newBarrierHarness(t, []string{"node-1", "node-2"})
			defer h.close()
			addTask(t, h, units)

			tc.run(t, h)

			if tc.wantDecided == nil {
				require.Empty(t, h.nodes[1].provider.taskStatuses, "no cleanup on a task that did not fail")
				return
			}
			require.Equal(t, []TaskStatus{TaskStatusFailed}, h.nodes[1].provider.taskStatuses)
			require.Equal(t, [][]string{tc.wantDecided}, h.nodes[1].provider.taskDecided,
				"FAILED cleanup must see every decision RAFT holds")
		})
	}
}

// A failed local prepare must fail the task even while another local group
// is still retryable (context.Canceled, for example a held shard).
func TestBarrier_FailedGroupAcksWhileOtherGroupRetryable(t *testing.T) {
	defer leaktest.Check(t)()

	h := newBarrierHarness(t, []string{"node-1"})
	defer h.close()

	taskID := "barrier-failed-group-acks"
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitSpecs: []*cmd.UnitSpec{
			{Id: "u-a", GroupId: "group-a"},
			{Id: "u-b", GroupId: "group-b"},
		},
		NeedsPreparationBarrier: true,
	}), 1))
	for _, u := range []string{"u-a", "u-b"} {
		require.NoError(t, h.completionRec.UpdateDistributedTaskUnitProgress(
			context.Background(), h.namespace, taskID, 1, "node-1", u, 0.1))
	}
	h.tick(0)
	for _, u := range []string{"u-a", "u-b"} {
		require.NoError(t, h.completionRec.RecordDistributedTaskUnitCompletion(
			context.Background(), h.namespace, taskID, 1, "node-1", u))
	}
	require.Equal(t, TaskStatusPreparing, h.getTask(t, taskID).Status)

	h.nodes[0].provider.SetGroupErrors(map[string]error{
		"group-a": fmt.Errorf("shard held: %w", context.Canceled),
		"group-b": fmt.Errorf("synthetic EIO"),
	})
	h.tick(0)

	task := h.getTask(t, taskID)
	require.Equal(t, TaskStatusFailed, task.Status, "group-b's failure must be acked without waiting for group-a")
	require.Contains(t, task.Error, `"group-b"`)
}

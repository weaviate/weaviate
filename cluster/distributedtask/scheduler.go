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

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/logrusext"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// taskSchedulerState bundles the per-task scheduler-side state for a
// single TaskDescriptor. It collapses what was previously seven parallel
// maps (each keyed by TaskDescriptor) into one struct so adding new
// per-task fields no longer requires touching a separate "register
// here too" cleanup helper.
//
// All access is gated by [Scheduler.mu]. Entries are created lazily on
// first write and deleted as a unit on local task cleanup; the lazy
// nature is preserved by [Scheduler.perTaskStateLockedOrInit] /
// [Scheduler.perTaskStateLocked].
//
//   - completedCallbackFired: per-task one-shot for [UnitAwareProvider.OnTaskCompleted].
//   - groupCallbackFired: per-group one-shot for [UnitAwareProvider.OnGroupCompleted]
//     (PHASE B / non-barrier path) and [UnitAwareProvider.OnSwapRequested] (barrier PHASE B).
//   - preparationCallbackFired: per-group one-shot for [UnitAwareProvider.OnGroupCompleted]
//     in barrier PHASE A.
//   - postCompletionAckEmitted: per-task one-shot for emitting the per-node
//     PostCompletionAck via [PostCompletionAckRecorder.RecordDistributedTaskPostCompletionAck].
//   - preparationAckEmitted: per-task one-shot for emitting the per-node
//     PreparationCompleteAck.
//   - postCompletionGroupErrors / preparationCompletionGroupErrors: capture
//     return values from the per-group callbacks (incl. nil) so the
//     ack-emission gate can distinguish "fired and succeeded" from "not
//     fired yet".
type taskSchedulerState struct {
	completedCallbackFired           bool
	groupCallbackFired               map[string]bool
	preparationCallbackFired         map[string]bool
	postCompletionAckEmitted         bool
	preparationAckEmitted            bool
	postCompletionGroupErrors        map[string]error
	preparationCompletionGroupErrors map[string]error
}

// Scheduler is the component which is responsible for polling the active tasks in the cluster (via the Manager)
// and making sure that the tasks are running on the local node.
//
// # Backup precheck scope (coordinator-local fast-fail)
//
// The scheduler keeps the local node's view of which tasks are in
// flight; consumers like the backup coordinator's Backupable precheck
// (adapters/repos/db/backup.go) consult that view to refuse a backup
// whose target class has a runtime-reindex still in flight.
//
// That precheck is intentionally coordinator-local: it only walks
// shards on the node that received the backup request. Remote refusals
// are NOT escalated through the scheduler; they happen later in the
// backup state machine on every replica's canCommit step. The
// asymmetry is by design — coordinator-local hit is a fast-fail
// optimization (most backup attempts will hit a remote shard's
// canCommit anyway, so paying a cross-node RAFT round-trip in the
// precheck path is wasted latency), and any reindex that exists on a
// remote shard but not locally still gets caught by the global
// canCommit barrier before the backup writes anything durable.
//
// If a future change pushes the precheck down into a cluster-wide
// query, this paragraph and the matching comment in
// db/backup.go:Backupable should be updated together.
//
// The general flow of a distributed task is as follows:
// 1. A Provider is registered with the Scheduler at startup to handle all tasks under a specific namespace.
// 2. A task is created and added to the cluster via the Manager.AddTask.
// 3. Scheduler regularly scans all available tasks in the cluster, picks up new ones and instructs the Provider to execute them locally.
// 4. A task is responsible for updating its status in the cluster via TaskCompletionRecorder.
// 5. Scheduler polls the cluster for the task status and checks if it is still running. It cancels the local task if it is not marked as STARTED anymore.
// 6. After completed task TTL has passed, the Scheduler issues the Manager.CleanUpDistributedTask request to remove the task from the cluster list.
// 7. After a task is removed from the cluster list, the Scheduler instructs the Provider to clean up the local task state.
type Scheduler struct {
	mu           sync.Mutex
	runningTasks map[string]map[TaskDescriptor]TaskHandle

	providers          map[string]Provider // namespace -> Provider
	completionRecorder TaskCompletionRecorder
	taskLister         TaskLister
	taskCleaner        TaskCleaner
	taskFinalizer      TaskFinalizer
	ackRecorder        PostCompletionAckRecorder
	clock              clockwork.Clock

	localNode        string
	completedTaskTTL time.Duration
	tickInterval     time.Duration

	logger        logrus.FieldLogger
	sampledLogger *logrusext.Sampler

	tasksRunning *prometheus.GaugeVec

	// perTaskState holds all per-scheduler-instance per-task state for
	// the two-phase callback firing and ack emission. Entries are
	// per-task by TaskDescriptor; preparation* fields are populated
	// only for barrier tasks. The whole struct is rebuilt on restart
	// and the recovery path re-fires callbacks so the cluster never
	// loses the barrier. See [taskSchedulerState] for the field
	// semantics; the single-struct replaces seven parallel maps that
	// previously required a manual "register here too" entry in
	// [Scheduler.deletePerTaskStateLocked] for every new field.
	perTaskState map[TaskDescriptor]*taskSchedulerState

	// bootstrapped flips to true once the scheduler has snapshotted the
	// RAFT-replicated task list and pre-marked every task that was
	// already terminal at that snapshot. Until this is true, the tick
	// loop's callback-firing path is gated on a deferred pre-mark so
	// post-restart callback replay (an old change-tokenization task
	// re-firing OnTaskCompleted after a newer one already committed the
	// schema flip) cannot happen. See [Scheduler.preMarkTerminalCallbacksLocked].
	//
	// Two paths set this:
	//  1. Successful [Scheduler.bootstrapProviders] during Start() (the
	//     happy path: RAFT is ready at Start() time).
	//  2. First successful tick after a failed Start()-time listing — if
	//     RAFT wasn't ready yet at Start(), the deferred bootstrap runs
	//     in tick() before any callbacks fire on this scheduler instance.
	bootstrapped bool

	stopCh chan struct{}

	// loopCtx is the context the tick path threads through every
	// cancellable call (listTasks, ackRecorder writes, finalizer,
	// cleaner, UnitAwareProvider callbacks). loopCancel is invoked by
	// [Scheduler.Close] BEFORE the loop-done barrier so a tick stuck
	// inside any of those calls unwinds before Close waits on loopDone.
	// Without this, a stalled RAFT round-trip (leader unavailable,
	// partition, slow apply) holds shutdown indefinitely.
	loopCtx    context.Context
	loopCancel context.CancelFunc

	// loopDone is closed by the run loop goroutine after the final tick
	// returns. [Scheduler.Close] waits on this so no tick can race
	// against teardown of the DB / schema / cluster services on the
	// caller side.
	loopDone chan struct{}

	// wakeCh signals the run loop to fire a scheduling cycle immediately
	// instead of waiting for the next periodic tick. Sized 1 so concurrent
	// callers coalesce — a pending wake-up is equivalent to any number of
	// queued wake-ups, because the next loop iteration reads cluster-wide
	// task state from scratch.
	//
	// Why this exists: with the default 1-minute tickInterval, a barrier
	// opening (last unit terminal) on the leader was followed by up to a
	// minute of "the followers haven't fired their OnGroupCompleted /
	// OnTaskCompleted yet" — the FSM apply path knows the barrier is open,
	// but the followers' scheduler loops are still asleep on their ticker.
	// Wake() (called from the Manager's RAFT-apply paths) lets us collapse
	// that gap to roughly one channel-send + one loop iteration.
	wakeCh chan struct{}
}

type SchedulerParams struct {
	CompletionRecorder TaskCompletionRecorder
	TaskLister         TaskLister
	TaskCleaner        TaskCleaner
	TaskFinalizer      TaskFinalizer
	// AckRecorder publishes per-node phase results via RAFT. nil in unit
	// tests; production wiring in configure_api.go always sets this.
	AckRecorder       PostCompletionAckRecorder
	Providers         map[string]Provider
	Clock             clockwork.Clock
	Logger            logrus.FieldLogger
	MetricsRegisterer prometheus.Registerer

	LocalNode        string
	CompletedTaskTTL time.Duration
	TickInterval     time.Duration
}

func NewScheduler(params SchedulerParams) *Scheduler {
	if params.Clock == nil {
		params.Clock = clockwork.NewRealClock()
	}

	if params.MetricsRegisterer == nil {
		params.MetricsRegisterer = monitoring.NoopRegisterer
	}

	return &Scheduler{
		runningTasks: map[string]map[TaskDescriptor]TaskHandle{},

		providers:          params.Providers,
		completionRecorder: params.CompletionRecorder,
		perTaskState:       map[TaskDescriptor]*taskSchedulerState{},
		taskLister:         params.TaskLister,
		taskCleaner:        params.TaskCleaner,
		taskFinalizer:      params.TaskFinalizer,
		ackRecorder:        params.AckRecorder,
		clock:              params.Clock,

		localNode:        params.LocalNode,
		completedTaskTTL: params.CompletedTaskTTL,
		tickInterval:     params.TickInterval,

		logger:        params.Logger,
		sampledLogger: logrusext.NewSampler(params.Logger, 5, 5*params.TickInterval),

		tasksRunning: promauto.With(params.MetricsRegisterer).NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_distributed_tasks_running",
			Help: "Number of active distributed tasks running per namespace",
		}, []string{"namespace"}),

		stopCh: make(chan struct{}),
		wakeCh: make(chan struct{}, 1),
	}
}

// Wake requests an immediate scheduling cycle. Non-blocking: a pending
// wake-up coalesces additional calls (the next loop iteration sees the
// latest cluster-wide task state regardless of how many wakes accumulated).
// Safe to call from any goroutine, including RAFT-apply paths.
//
// Wake is a no-op after [Scheduler.Close] returns — the run loop has
// already exited and won't observe the signal.
func (s *Scheduler) Wake() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
		// A wake-up is already queued; coalesce.
	}
}

// Start wires up providers with a [ThrottledRecorder], performs an initial task listing to
// bootstrap any already-active tasks, and spawns the background tick loop. It is safe to call
// exactly once. Use [Scheduler.Close] to stop the loop and terminate all running tasks.
func (s *Scheduler) Start(ctx context.Context) error {
	// [DefaultThrottleInterval] caps per-unit progress writes on the Raft hot
	// path without making the UI feel frozen on long-running migrations. A
	// coarser cap (the old 30s) is invisible to users watching a 60–90s
	// reindex — every other sample gets eaten and the progress bar appears
	// to jump in large increments. 3s gives ~20 samples per minute per unit,
	// well within Raft's write budget.
	throttledRecorder := NewThrottledRecorder(s.completionRecorder, DefaultThrottleInterval, s.clock)

	s.setCompletionRecorders(throttledRecorder)

	// Attempt an initial task listing to bootstrap running tasks. If it fails
	// (e.g. Raft not ready yet), log and continue — tick() will pick tasks up
	// once the cluster is ready, and will run the deferred bootstrap on the
	// first successful tick so post-restart callback replay is still
	// suppressed.
	tasksByNamespace, err := s.listTasks(ctx)
	if err != nil {
		s.logger.Warnf("initial distributed task listing failed; bootstrap deferred to first successful tick: %v", err)
	} else {
		s.bootstrapProviders(tasksByNamespace)
	}

	// Arm the cancel-on-shutdown context BEFORE spawning the loop. Every
	// cancellable site in the tick path (listTasks, ackRecorder writes,
	// finalizer, cleaner, UnitAwareProvider callbacks) threads s.loopCtx
	// so a stalled RAFT round-trip unwinds when Close() fires
	// loopCancel.
	s.loopDone = make(chan struct{})
	s.loopCtx, s.loopCancel = context.WithCancel(context.Background())
	enterrors.GoWrapper(s.loop, s.logger)

	return nil
}

// bootstrapProviders cleans up stale local tasks and starts tasks that are currently active,
// based on the initial task listing from the Raft log.
//
// Suppresses post-restart callback replay: tasks already at a terminal
// status (Finished / Failed / Cancelled) at scheduler-start time have
// either already had their OnGroupCompleted + OnTaskCompleted fire on
// this node before the restart, OR (rare) they will have fired on
// another node and the result is in the RAFT-replicated state we just
// re-loaded. Either way, re-firing them on this fresh scheduler can
// only revert work that newer tasks have already cluster-wide
// committed (the classic shape: an older change-tokenization task's
// schema flip replaying after a newer one has already moved the
// schema past it). Mark them as already-fired so the tick loop's
// "fire if not yet fired" guard skips them.
//
// The trade-off — a node that died in the exact window between
// OnGroupCompleted committing on it and OnTaskCompleted starting
// (sub-millisecond) would skip the schema flip on this node, but the
// flip is RAFT-replicated and any peer that fired it captures the
// state cluster-wide. The reindex provider's `OnTaskCompleted` is
// also idempotent at the mutator level, so a peer firing immediately
// before this one dying is the same outcome.
func (s *Scheduler) bootstrapProviders(tasksByNamespace map[string]map[TaskDescriptor]*Task) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for namespace, provider := range s.providers {
		startedTasks := s.filterStartedTasks(tasksByNamespace[namespace])

		s.cleanupStaleTasks(namespace, provider, startedTasks)
		s.startActiveTasks(namespace, provider, startedTasks)

		s.tasksRunning.
			WithLabelValues(namespace).
			Set(float64(len(startedTasks)))
	}

	// Pre-mark callbacks for already-terminal tasks as fired so the tick
	// loop's `!s.completedCallbackFired[desc]` and
	// `!s.groupCallbackFired[desc][groupID]` guards skip them. This MUST
	// happen for every provider's tasks, not just the providers we have
	// registered locally — a task in a namespace we don't host can still
	// appear in tasksByNamespace via RAFT replication.
	s.preMarkTerminalCallbacksLocked(tasksByNamespace)
	s.bootstrapped = true
}

// preMarkTerminalCallbacksLocked sets s.completedCallbackFired and
// s.groupCallbackFired for every task that is already terminal in the
// provided snapshot. The intent is to suppress post-restart callback
// replay: the in-memory `*CallbackFired` maps are empty on every fresh
// scheduler, and without this pre-mark the tick loop would re-fire
// OnGroupCompleted + OnTaskCompleted for every task still in the FSM
// (including tasks whose callbacks already fired before the restart).
// For semantic migrations that produce a cluster-wide schema flip
// (change-tokenization, enable-searchable), an older task replaying
// after a newer one has already committed its flip silently reverts
// the schema cluster-wide.
//
// Caller MUST hold s.mu.
func (s *Scheduler) preMarkTerminalCallbacksLocked(tasksByNamespace map[string]map[TaskDescriptor]*Task) {
	for namespace, tasks := range tasksByNamespace {
		provider := s.providers[namespace]
		for desc, task := range tasks {
			if task.Status != TaskStatusFinished &&
				task.Status != TaskStatusFailed &&
				task.Status != TaskStatusCancelled {
				continue
			}
			// Recovery hook: if the provider implements
			// [RecoveryAwareProvider] and reports local-side callback
			// work as NOT yet done, skip the pre-mark for this task so
			// the next tick re-fires OnGroupCompleted and the
			// provider's recovery path can complete (e.g. a swap that
			// got context-cancelled during a rolling restart). Failed /
			// cancelled tasks are NOT subject to this check — the
			// provider semantically owns the "should the schema-flip
			// callback be retried" decision only for the FINISHED case;
			// retrying a cancelled task's OnGroupCompleted could
			// re-apply a half-baked swap the user explicitly aborted.
			if task.Status == TaskStatusFinished {
				if r, ok := provider.(RecoveryAwareProvider); ok {
					if !r.LocalCallbacksDone(task, s.localNode) {
						s.logger.WithField("namespace", namespace).
							WithField("taskID", desc.ID).
							WithField("taskVersion", desc.Version).
							Info("scheduler bootstrap: skipping pre-mark for terminal task with pending local recovery (callbacks will re-fire on next tick)")
						continue
					}
				}
			}
			state := s.perTaskStateLockedOrInit(desc)
			state.completedCallbackFired = true
			if state.groupCallbackFired == nil {
				state.groupCallbackFired = map[string]bool{}
			}
			if state.preparationCallbackFired == nil {
				state.preparationCallbackFired = map[string]bool{}
			}
			for _, groupID := range task.Groups() {
				state.groupCallbackFired[groupID] = true
				// Pre-mark prep as fired too — a terminal task is past
				// both phases of the barrier so neither the prep nor
				// the swap callback should re-fire on this scheduler
				// instance.
				state.preparationCallbackFired[groupID] = true
			}
			// Tasks that were already terminal at bootstrap have, by
			// definition, already gone through the ack barrier (or
			// were FAILED/CANCELLED, which bypasses it). Mark BOTH the
			// per-node prep-ack and the swap-ack as emitted so the
			// next tick does not re-emit for a task that's already
			// past either gate.
			state.postCompletionAckEmitted = true
			state.preparationAckEmitted = true
		}
	}
}

// cleanupStaleTasks removes local state for tasks that the provider knows about
// but that are no longer active in the cluster.
func (s *Scheduler) cleanupStaleTasks(namespace string, provider Provider, startedTasks map[TaskDescriptor]*Task) {
	for _, taskDesc := range provider.GetLocalTasks() {
		if _, ok := startedTasks[taskDesc]; ok {
			continue
		}

		if err := provider.CleanupTask(taskDesc); err != nil {
			s.loggerWithTask(namespace, taskDesc).
				Errorf("failed to clean up local distributed task state: %v", err)
			continue
		}

		s.loggerWithTask(namespace, taskDesc).Info("cleaned up local distributed task state")
	}
}

// startActiveTasks launches tasks that are currently active and have pending work on this node.
func (s *Scheduler) startActiveTasks(namespace string, provider Provider, startedTasks map[TaskDescriptor]*Task) {
	for desc, task := range startedTasks {
		handle, err := provider.StartTask(task)
		if err != nil {
			s.loggerWithTask(namespace, desc).
				Errorf("failed to start distributed task during bootstrap: %v", err)
			continue
		}

		s.recordRunningTaskHandleLocked(namespace, desc, handle)
		s.loggerWithTask(namespace, desc).Info("started distributed task execution")
	}
}

func (s *Scheduler) filterStartedTasks(tasks map[TaskDescriptor]*Task) map[TaskDescriptor]*Task {
	return filterTasks(tasks, func(task *Task) bool {
		if task.Status != TaskStatusStarted {
			return false
		}
		return task.NodeHasNonTerminalUnits(s.localNode)
	})
}

func filterTasks(tasks map[TaskDescriptor]*Task, predicate func(task *Task) bool) map[TaskDescriptor]*Task {
	filtered := make(map[TaskDescriptor]*Task, len(tasks))
	for _, task := range tasks {
		if !predicate(task) {
			continue
		}

		filtered[TaskDescriptor{
			ID:      task.ID,
			Version: task.Version,
		}] = task
	}
	return filtered
}

func (s *Scheduler) loop() {
	// Close loopDone last so [Scheduler.Close]'s wait barrier can confirm
	// that no tick is still in flight. Without this, Close returns
	// concurrent with a mid-RAFT-apply tick and races the caller's
	// teardown of DB / schema services.
	defer close(s.loopDone)
	ticker := s.clock.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			s.tick()
		case <-s.wakeCh:
			// Reactive wake-up from a RAFT-apply path (typically the
			// Manager observing a unit transition that opens a group
			// barrier). Fire a tick immediately instead of waiting for
			// the next ticker. The periodic ticker keeps running as a
			// fallback — late wake-ups, missed signals, and any state
			// drift get cleaned up at the next periodic tick.
			s.tick()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Scheduler) tick() {
	// listTasks gets s.loopCtx so a stalled RAFT round-trip is cut by
	// [Scheduler.Close] cancelling loopCtx — same shutdown-cancel contract
	// as the per-phase RAFT writes below.
	tasksByNamespace, err := s.listTasks(s.loopCtx)
	if err != nil {
		s.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
			l.Errorf("failed to list distributed tasks: %v", err)
		})
		return
	}

	// Deferred bootstrap + providers-map snapshot in a single brief lock
	// scope. Closure so the Unlock is deferred — a panic in
	// preMarkTerminalCallbacksLocked would otherwise leak the mutex.
	var (
		needsBootstrap bool
		providers      map[string]Provider
	)
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		needsBootstrap = !s.bootstrapped
		if needsBootstrap {
			s.preMarkTerminalCallbacksLocked(tasksByNamespace)
			s.bootstrapped = true
		}
		providers = make(map[string]Provider, len(s.providers))
		for ns, p := range s.providers {
			providers[ns] = p
		}
	}()
	if needsBootstrap {
		s.logger.Info("distributed task scheduler: deferred bootstrap pre-mark complete on first successful tick")
	}

	for namespace, provider := range providers {
		tasks := tasksByNamespace[namespace]

		startedTasks := s.reconcileRunningTasks(namespace, provider, tasks)
		s.tasksRunning.
			WithLabelValues(namespace).
			Set(float64(len(startedTasks)))

		// Fire group-level and task-level callbacks for unit-aware providers.
		// OnGroupCompleted fires per-group as each group's units all reach terminal
		// state (can fire mid-flight while task is still STARTED).
		// OnTaskCompleted fires once when the task reaches the SWAPPING
		// (success path) or FAILED state. FINISHED tasks have already had
		// their callbacks fire — the FINISHED transition is committed by
		// [TaskFinalizer.MarkDistributedTaskFinalized] below only AFTER
		// OnTaskCompleted returns successfully, so a task in the FINISHED
		// state is by construction past this point. The callback-fired
		// maps' pre-mark from [Scheduler.bootstrapProviders] (and the
		// deferred-bootstrap path in this tick) also marks FINISHED tasks
		// as already-fired so a node restart cannot replay them.
		suProvider, providerIsUnitAware := provider.(UnitAwareProvider)
		if providerIsUnitAware {
			for desc, task := range tasks {
				// effectiveStatus carries the per-tick state-machine view: it
				// starts at task.Status (what ListDistributedTasks returned)
				// and the PREP-ack / SWAP-ack failure paths advance it to
				// FAILED so PHASE B and Phase 2 can react inside the same
				// tick without overwriting the FSM clone. The clone stays
				// equal to the RAFT-replicated state for the entire tick;
				// any downstream code that re-reads from `tasks` sees the
				// authoritative status, not a hidden in-tick mutation.
				effectiveStatus := task.Status

				// State-machine dispatch by effectiveStatus.
				//
				// CANCELLED is a first-class branch: today the Manager's
				// CancelTask only accepts cancel from STARTED (see
				// manager.go:CancelTask), so a CANCELLED task by
				// construction has not yet hit PREP / SWAP / any ack
				// barrier. Skipping those phases on this branch is the
				// FSM rule, not an in-scheduler optimization — making it
				// a named case (instead of a `if !cancelled` wrapper)
				// keeps the dispatch readable and surfaces the
				// dependency on the FSM rule for any future change.
				//
				// All other branches (STARTED, PREPARING, SWAPPING,
				// FAILED, FINISHED) fall through to the in-flight ack
				// pipeline and then Phase 2.
				switch effectiveStatus {
				case TaskStatusCancelled:
					// Skip PHASE A / PHASE B / ack barriers; Phase 2
					// below still fires OnTaskCompleted exactly once
					// so the provider can run its cancel-cleanup.

				default:

					// PHASE A: PREP-phase callback firing for barrier tasks
					// in PREPARING. SWAP is deferred until the cluster-wide
					// PreparationCompleteAck barrier lifts.
					//
					// Snapshot/process/commit: under s.mu we collect the
					// per-group work items into prepWorklist (and set the
					// fired marks pre-callback); s.mu is dropped exactly
					// once for the whole phase while the provider runs
					// real I/O (fs moves, fsyncs) and the ack RAFT-write;
					// then s.mu is re-acquired and we commit results into
					// per-task state with the load-bearing state re-fetch
					// (local cleanup may have deleted the entry while we
					// ran callbacks — a nil state means "nothing to record").
					if task.NeedsPreparationBarrier && effectiveStatus == TaskStatusPreparing {
						if newStatus := s.runPreparationPhase(namespace, desc, task, suProvider, effectiveStatus); newStatus != effectiveStatus {
							effectiveStatus = newStatus
						}
					}

					// PHASE B: SWAP-phase callback firing. Barrier tasks fire
					// OnSwapRequested only after postStarted (FSM gates SWAP
					// on the cluster-wide barrier); non-barrier tasks fire
					// OnGroupCompleted mid-flight via AllGroupUnitsTerminal.
					//
					// Snapshot/process/commit: same shape as PHASE A —
					// snapshot the per-group work items under s.mu (set
					// the fired marks pre-callback), drop s.mu for the
					// whole phase (provider callbacks + the per-node
					// post-completion ack RAFT-write), re-acquire and
					// commit with the load-bearing state re-fetch.
					if newStatus := s.runSwapPhase(namespace, desc, task, suProvider, effectiveStatus); newStatus != effectiveStatus {
						effectiveStatus = newStatus
					}
				} // end switch effectiveStatus

				// Phase 2: fire OnTaskCompleted on SWAPPING/FAILED/FINISHED/
				// CANCELLED. FINISHED is included because the first node to
				// see SWAPPING issues MarkFinalized in the same tick, so
				// other nodes' next tick may already see FINISHED. On the
				// SWAPPING path wait until every node has acked: the schema
				// flip can't commit while any replica's swap is undetermined.
				//
				// Snapshot/process/commit: snapshot the "should fire" decision
				// (and set completedCallbackFired pre-callback) under s.mu;
				// drop s.mu for the single OnTaskCompleted call; re-acquire
				// on the way out. There is no per-result commit beyond the
				// pre-set fired mark — OnTaskCompleted's failure mode is
				// handled by the MarkDistributedTaskFinalized rollback
				// (it clears completedCallbackFired so the next tick retries).
				s.runCompletedCallbackPhase(desc, task, suProvider, effectiveStatus)
			}
		}

		// MarkDistributedTaskFinalized issues SWAPPING → FINISHED. For
		// unit-aware providers, gate on OnTaskCompleted having fired so
		// FINISHED lines up with "every post-completion callback committed".
		//
		// Snapshot/process/commit: snapshot the eligible task identities
		// under s.mu, drop s.mu once for the whole batch of finalize
		// RAFT-writes, then re-acquire and apply per-result rollbacks
		// with the load-bearing state re-fetch (a parallel local-cleanup
		// tick may have deleted the entry; nil-state means nothing to
		// roll back).
		if s.taskFinalizer != nil {
			s.runFinalizePhase(namespace, tasks, providerIsUnitAware)
		}

		// TTL-cleanup of finished tasks. IsActive() excludes PREPARING and
		// SWAPPING explicitly — their FinishedAt is zero-time, so
		// clock.Since(zero) would otherwise mis-classify them as expired.
		cleanableTasks := filterTasks(tasks, func(task *Task) bool {
			if task.Status.IsActive() {
				return false
			}
			return s.completedTaskTTL <= s.clock.Since(task.FinishedAt)
		})
		for _, task := range cleanableTasks {
			err = s.taskCleaner.CleanUpDistributedTask(s.loopCtx, namespace, task.ID, task.Version)
			if err != nil {
				s.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
					s.loggerWithTask(namespace, task.TaskDescriptor).
						Errorf("failed to clean up distributed task: %v", err)
				})
				continue
			}

			s.loggerWithTask(namespace, task.TaskDescriptor).
				Info("successfully submitted request to clean up distributed task")
		}

		// Check that tasks that can be cleaned up locally
		localTasks := provider.GetLocalTasks()
		for _, desc := range localTasks {
			if _, ok := tasks[desc]; ok {
				// task still present in the list
				continue
			}

			func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.deletePerTaskStateLocked(desc)
			}()

			if err = provider.CleanupTask(desc); err != nil {
				s.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
					s.loggerWithTask(namespace, desc).
						Errorf("failed to clean up local distributed task state: %v", err)
				})
			}
		}
	}
}

// reconcileRunningTasks owns the start/terminate decisions for a single
// namespace per tick. Acquires s.mu briefly to drop dead handles, decide
// the started set, terminate handles for descriptors no longer in the
// started set, and record newly-started handles. Provider.StartTask is
// invoked without the lock — it can be slow (image pulls, fs setup) —
// and the resulting handle is then committed under a brief lock.
//
// Returns the started-set map so the caller can update the running-tasks
// metric without re-acquiring the lock.
func (s *Scheduler) reconcileRunningTasks(
	namespace string,
	provider Provider,
	tasks map[TaskDescriptor]*Task,
) map[TaskDescriptor]*Task {
	// Phase 1: drop dead handles and snapshot the started-set decision.
	// Inline closure so the Unlock is deferred — a panic inside
	// filterStartedTasks or a map operation would otherwise leak the lock.
	type terminateEntry struct {
		desc   TaskDescriptor
		handle TaskHandle
	}
	var (
		startedTasks map[TaskDescriptor]*Task
		toStart      []*Task
		toTerminate  []terminateEntry
	)
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for desc, taskHandle := range s.runningTasks[namespace] {
			select {
			case <-taskHandle.Done():
				delete(s.runningTasks[namespace], desc)
			default:
			}
		}
		startedTasks = s.filterStartedTasks(tasks)
		for _, activeTask := range startedTasks {
			if _, alreadyLaunched := s.runningTasks[namespace][activeTask.TaskDescriptor]; alreadyLaunched {
				continue
			}
			toStart = append(toStart, activeTask)
		}
		for desc, taskHandle := range s.runningTasks[namespace] {
			if _, ok := startedTasks[desc]; ok {
				continue
			}
			toTerminate = append(toTerminate, terminateEntry{desc, taskHandle})
			delete(s.runningTasks[namespace], desc)
		}
	}()

	// Phase 2: launch and terminate without the lock — both can block on
	// I/O. Each per-handle commit is its own deferred-Unlock closure.
	for _, activeTask := range toStart {
		handle, err := provider.StartTask(activeTask)
		if err != nil {
			s.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
				s.loggerWithTask(namespace, activeTask.TaskDescriptor).
					Errorf("failed to start distributed task: %v", err)
			})
			continue
		}
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.recordRunningTaskHandleLocked(namespace, activeTask.TaskDescriptor, handle)
		}()
		s.loggerWithTask(namespace, activeTask.TaskDescriptor).Info("started distributed task execution")
	}
	for _, kill := range toTerminate {
		kill.handle.Terminate()
		s.loggerWithTask(namespace, kill.desc).Info("terminated distributed task execution")
	}
	return startedTasks
}

// preparationGroupWork captures one PHASE-A per-group work item that must
// run with s.mu released. Populated under s.mu in the snapshot step;
// consumed without s.mu in the process step.
type preparationGroupWork struct {
	groupID  string
	localIDs []string
}

// runPreparationPhase executes PHASE A (preparation barrier) for a single
// task. Owns its own lock lifecycle:
//
//   - Lock to snapshot the per-group worklist and pre-set fired marks.
//   - Unlock for the OnGroupCompleted batch.
//   - Lock to commit per-result group errors (with the load-bearing
//     per-result state re-fetch — a concurrent local-cleanup tick may
//     have deleted the entry).
//   - Lock again to snapshot ack eligibility (success/joined identity),
//     unlock for the ack RAFT-write, lock to commit ack-emitted + the
//     effectiveStatus advance.
//
// Returns the new effectiveStatus (may advance to TaskStatusFailed if
// the ack RAFT-write reports success=false). No outer lock inherited.
func (s *Scheduler) runPreparationPhase(
	namespace string,
	desc TaskDescriptor,
	task *Task,
	suProvider UnitAwareProvider,
	effectiveStatus TaskStatus,
) TaskStatus {
	// Snapshot: collect per-group work + pre-set fired marks, under a
	// deferred-Unlock closure for panic safety.
	worklist := func() []preparationGroupWork {
		s.mu.Lock()
		defer s.mu.Unlock()
		var list []preparationGroupWork
		for _, groupID := range task.Groups() {
			state := s.perTaskStateLocked(desc)
			if state != nil && state.preparationCallbackFired[groupID] {
				continue
			}
			localIDs := task.LocalGroupUnitIDs(groupID, s.localNode)
			if len(localIDs) == 0 {
				continue
			}
			state = s.perTaskStateLockedOrInit(desc)
			if state.preparationCallbackFired == nil {
				state.preparationCallbackFired = map[string]bool{}
			}
			state.preparationCallbackFired[groupID] = true
			list = append(list, preparationGroupWork{groupID: groupID, localIDs: localIDs})
		}
		return list
	}()

	// Process without the lock. Provider does real I/O. ctx = s.loopCtx
	// so a stuck callback unwinds on Scheduler.Close.
	groupErrors := make([]error, len(worklist))
	for i, w := range worklist {
		groupErrors[i] = suProvider.OnGroupCompleted(s.loopCtx, task, w.groupID, w.localIDs)
	}

	// Commit group errors + ack eligibility snapshot under deferred-Unlock.
	// Returns the ack-snapshot result (eligible=false means caller skips
	// the RAFT-write).
	type prepAckSnapshot struct {
		eligible bool
		success  bool
		joined   string
	}
	ackSnap := func() prepAckSnapshot {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, w := range worklist {
			groupErr := groupErrors[i]
			state := s.perTaskStateLocked(desc)
			if state == nil {
				continue
			}
			if errors.Is(groupErr, context.Canceled) {
				delete(state.preparationCallbackFired, w.groupID)
				s.loggerWithTask(namespace, desc).
					WithField("groupID", w.groupID).
					Info("PREP phase aborted by graceful shutdown; recovery on next boot will re-fire and emit the prep-complete ack")
				continue
			}
			if state.preparationCompletionGroupErrors == nil {
				state.preparationCompletionGroupErrors = map[string]error{}
			}
			state.preparationCompletionGroupErrors[w.groupID] = groupErr
		}
		prepState := s.perTaskStateLocked(desc)
		if s.ackRecorder == nil ||
			(prepState != nil && prepState.preparationAckEmitted) ||
			!s.allLocalGroupsPreparationFiredLocked(task, desc) {
			return prepAckSnapshot{}
		}
		success, joined := s.aggregatePreparationAckErrorsLocked(task, desc)
		return prepAckSnapshot{eligible: true, success: success, joined: joined}
	}()
	if !ackSnap.eligible {
		return effectiveStatus
	}
	success, joined := ackSnap.success, ackSnap.joined

	// Ack RAFT-write without the lock. Threads s.loopCtx so a stuck RAFT
	// round-trip is cut by Scheduler.Close cancelling loopCtx.
	ackErr := s.ackRecorder.RecordDistributedTaskPreparationCompleteAck(
		s.loopCtx, namespace, task.ID, task.Version,
		s.localNode, success, joined,
	)

	// Commit ack outcome.
	s.mu.Lock()
	defer s.mu.Unlock()
	if ackErr != nil {
		s.loggerWithTask(namespace, desc).
			Warnf("failed to record distributed task prep-complete ack; will retry on next tick or wake: %v", ackErr)
		return effectiveStatus
	}
	if afterAckState := s.perTaskStateLocked(desc); afterAckState != nil {
		afterAckState.preparationAckEmitted = true
	}
	if task.PreparationCompletionAcks == nil {
		task.PreparationCompletionAcks = map[string]PostCompletionAck{}
	}
	task.PreparationCompletionAcks[s.localNode] = PostCompletionAck{
		Success: success,
		Error:   joined,
		AckedAt: s.clock.Now(),
	}
	// Advance effectiveStatus to FAILED (in lieu of overwriting task.Status)
	// so PHASE B and Phase 2 react inside this same tick.
	if !success && effectiveStatus == TaskStatusPreparing {
		effectiveStatus = TaskStatusFailed
	}
	return effectiveStatus
}

// swapGroupWork captures one PHASE-B per-group work item. Populated
// under s.mu in the snapshot step; consumed without s.mu in the
// process step.
type swapGroupWork struct {
	groupID  string
	localIDs []string
	// isSwap distinguishes barrier (OnSwapRequested) from non-barrier
	// (OnGroupCompleted) callbacks; captured at snapshot time so the
	// process loop has no branching dependency on task state.
	isSwap bool
}

// runSwapPhase executes PHASE B (swap callback firing) for a single task.
// Same lock lifecycle shape as runPreparationPhase. No outer lock inherited.
//
// Barrier tasks fire OnSwapRequested only after postStarted (FSM gates SWAP
// on the cluster-wide barrier); non-barrier tasks fire OnGroupCompleted
// mid-flight via AllGroupUnitsTerminal.
func (s *Scheduler) runSwapPhase(
	namespace string,
	desc TaskDescriptor,
	task *Task,
	suProvider UnitAwareProvider,
	effectiveStatus TaskStatus,
) TaskStatus {
	postStarted := effectiveStatus == TaskStatusSwapping ||
		effectiveStatus == TaskStatusFailed ||
		effectiveStatus == TaskStatusFinished

	// Snapshot: per-group eligibility + pre-set fired marks under a
	// deferred-Unlock closure.
	worklist := func() []swapGroupWork {
		s.mu.Lock()
		defer s.mu.Unlock()
		var list []swapGroupWork
		for _, groupID := range task.Groups() {
			state := s.perTaskStateLocked(desc)
			if state != nil && state.groupCallbackFired[groupID] {
				continue
			}
			if task.NeedsPreparationBarrier {
				if !postStarted {
					continue
				}
			} else {
				if !postStarted && !task.AllGroupUnitsTerminal(groupID) {
					continue
				}
			}
			localIDs := task.LocalGroupUnitIDs(groupID, s.localNode)
			if len(localIDs) == 0 {
				continue
			}
			state = s.perTaskStateLockedOrInit(desc)
			if state.groupCallbackFired == nil {
				state.groupCallbackFired = map[string]bool{}
			}
			state.groupCallbackFired[groupID] = true
			list = append(list, swapGroupWork{
				groupID:  groupID,
				localIDs: localIDs,
				isSwap:   task.NeedsPreparationBarrier,
			})
		}
		return list
	}()

	// Process without the lock. ctx = s.loopCtx so a stuck callback
	// unwinds on Scheduler.Close.
	groupErrors := make([]error, len(worklist))
	for i, w := range worklist {
		if w.isSwap {
			groupErrors[i] = suProvider.OnSwapRequested(s.loopCtx, task, w.groupID, w.localIDs)
		} else {
			groupErrors[i] = suProvider.OnGroupCompleted(s.loopCtx, task, w.groupID, w.localIDs)
		}
	}

	// Commit group errors + ack eligibility snapshot under deferred-Unlock.
	type swapAckSnapshot struct {
		eligible bool
		success  bool
		joined   string
	}
	ackSnap := func() swapAckSnapshot {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, w := range worklist {
			groupErr := groupErrors[i]
			state := s.perTaskStateLocked(desc)
			if state == nil {
				continue
			}
			// ctx.Canceled from a graceful SIGTERM is transient; drop the
			// fired mark so the post-restart tick re-fires SWAP. Treating
			// it as a permanent failure would flip the task to FAILED and
			// short-circuit recovery.
			if errors.Is(groupErr, context.Canceled) {
				delete(state.groupCallbackFired, w.groupID)
				s.loggerWithTask(namespace, desc).
					WithField("groupID", w.groupID).
					Info("SWAP callback aborted by graceful shutdown; recovery on next boot will re-fire and emit the post-completion ack")
				continue
			}
			// Record nil too so the ack-emission gate can tell "fired and
			// succeeded" from "not fired yet".
			if state.postCompletionGroupErrors == nil {
				state.postCompletionGroupErrors = map[string]error{}
			}
			state.postCompletionGroupErrors[w.groupID] = groupErr
		}
		ackState := s.perTaskStateLocked(desc)
		if s.ackRecorder == nil ||
			(ackState != nil && ackState.postCompletionAckEmitted) ||
			effectiveStatus == TaskStatusStarted ||
			!s.allLocalGroupsFiredLocked(task, desc) {
			return swapAckSnapshot{}
		}
		success, joined := s.aggregateAckErrorsLocked(task, desc)
		return swapAckSnapshot{eligible: true, success: success, joined: joined}
	}()
	if !ackSnap.eligible {
		return effectiveStatus
	}
	success, joined := ackSnap.success, ackSnap.joined

	// Ack RAFT-write without the lock. Threads s.loopCtx so a stuck RAFT
	// round-trip is cut by Scheduler.Close cancelling loopCtx.
	ackErr := s.ackRecorder.RecordDistributedTaskPostCompletionAck(
		s.loopCtx, namespace, task.ID, task.Version,
		s.localNode, success, joined,
	)

	// Commit ack outcome.
	s.mu.Lock()
	defer s.mu.Unlock()
	if ackErr != nil {
		// Leave postCompletionAckEmitted unset; FSM-side ack is
		// idempotent so retry is safe.
		s.loggerWithTask(namespace, desc).
			Warnf("failed to record distributed task post-completion ack; will retry on next tick or wake: %v", ackErr)
		return effectiveStatus
	}
	if afterAckState := s.perTaskStateLocked(desc); afterAckState != nil {
		afterAckState.postCompletionAckEmitted = true
	}
	// Reflect the ack on the per-tick local clone's ack map (mirror of
	// what the FSM apply path records) so the Phase 2 gate has the
	// up-to-date view without re-listing. Status itself stays on
	// effectiveStatus; the clone's Status field is never overwritten.
	if task.PostCompletionAcks == nil {
		task.PostCompletionAcks = map[string]PostCompletionAck{}
	}
	task.PostCompletionAcks[s.localNode] = PostCompletionAck{
		Success: success,
		Error:   joined,
		AckedAt: s.clock.Now(),
	}
	if !success && effectiveStatus == TaskStatusSwapping {
		effectiveStatus = TaskStatusFailed
	}
	return effectiveStatus
}

// runCompletedCallbackPhase fires OnTaskCompleted for a single task in
// Phase 2 (SWAPPING/FAILED/FINISHED/CANCELLED).
//
// Owns its own lock lifecycle: acquires s.mu to decide eligibility +
// set completedCallbackFired pre-callback, releases for the slow
// callback, returns. No commit step needed — the pre-set fired mark
// records the attempt; OnTaskCompleted's failure mode is handled by the
// MarkDistributedTaskFinalized rollback (it clears completedCallbackFired
// so the next tick retries). Providers implementing
// [UnitAwareProvider.OnTaskCompleted] MUST be safe to invoke more than
// once per task — see the "Idempotency contract" note at the rollback site.
func (s *Scheduler) runCompletedCallbackPhase(
	desc TaskDescriptor,
	task *Task,
	suProvider UnitAwareProvider,
	effectiveStatus TaskStatus,
) {
	readyForFinalize := effectiveStatus == TaskStatusSwapping ||
		effectiveStatus == TaskStatusFailed ||
		effectiveStatus == TaskStatusFinished ||
		effectiveStatus == TaskStatusCancelled
	if !readyForFinalize {
		return
	}
	if s.ackRecorder != nil && effectiveStatus == TaskStatusSwapping {
		if len(task.MissingPostCompletionAckNodes()) > 0 {
			return
		}
	}

	// Eligibility + pre-set fired mark under a deferred-Unlock closure.
	// Returns false when a concurrent tick already fired the callback.
	alreadyFired := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		if phase2State := s.perTaskStateLocked(desc); phase2State != nil && phase2State.completedCallbackFired {
			return true
		}
		s.perTaskStateLockedOrInit(desc).completedCallbackFired = true
		return false
	}()
	if alreadyFired {
		return
	}

	// Fire OnTaskCompleted without the lock. ctx = s.loopCtx so a stuck
	// schema flip unwinds on Scheduler.Close. See the "Idempotency
	// contract" note at the matching rollback site in runFinalizePhase.
	suProvider.OnTaskCompleted(s.loopCtx, task)
}

// finalizeWork captures one finalize-phase task identity. Populated under
// s.mu in the snapshot step; consumed without s.mu in the process step.
type finalizeWork struct {
	desc TaskDescriptor
	task *Task
}

// runFinalizePhase issues MarkDistributedTaskFinalized (SWAPPING → FINISHED)
// for every eligible task in `tasks`.
//
// Owns its own lock lifecycle: acquires s.mu to snapshot the eligible
// worklist, releases for the RAFT-write batch, re-acquires to commit
// per-result rollbacks. Holds no inherited lock.
func (s *Scheduler) runFinalizePhase(
	namespace string,
	tasks map[TaskDescriptor]*Task,
	providerIsUnitAware bool,
) {
	// Snapshot eligible task identities under a deferred-Unlock closure
	// so a panic inside perTaskStateLocked can't leak the lock.
	worklist := func() []finalizeWork {
		s.mu.Lock()
		defer s.mu.Unlock()
		var list []finalizeWork
		for desc, task := range tasks {
			if task.Status != TaskStatusSwapping {
				continue
			}
			finState := s.perTaskStateLocked(desc)
			if providerIsUnitAware && (finState == nil || !finState.completedCallbackFired) {
				continue
			}
			list = append(list, finalizeWork{desc: desc, task: task})
		}
		return list
	}()

	if len(worklist) == 0 {
		return
	}

	// Process without the lock. One RAFT-write per task, sequential —
	// the FSM accepts these idempotently, and the per-tick volume is
	// small (typically 1 task SWAPPING per tick).
	finErrors := make([]error, len(worklist))
	for i, w := range worklist {
		// Threads s.loopCtx so a stuck finalize RAFT-write is cut by
		// Scheduler.Close cancelling loopCtx.
		finErrors[i] = s.taskFinalizer.MarkDistributedTaskFinalized(
			s.loopCtx, namespace, w.task.ID, w.task.Version,
		)
	}

	// Commit under s.mu: apply per-result rollback. Re-fetch state per
	// result because the entry may have been cleaned up between phases.
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, w := range worklist {
		finErr := finErrors[i]
		if finErr == nil {
			continue
		}
		s.loggerWithTask(namespace, w.desc).
			Warnf("failed to mark distributed task finalized; will retry on next tick or wake: %v", finErr)
		// TODO(scheduler): clearing completedCallbackFired here causes
		// OnTaskCompleted to re-fire on the next tick. Safe today because
		// the reindex provider's OnTaskCompleted is idempotent, but
		// [UnitAwareProvider.OnTaskCompleted] (in types.go) doesn't yet
		// declare the requirement. See the matching "Idempotency contract"
		// note in [Scheduler.runCompletedCallbackPhase].
		if providerIsUnitAware {
			if rollbackState := s.perTaskStateLocked(w.desc); rollbackState != nil {
				rollbackState.completedCallbackFired = false
			}
		}
	}
}

func (s *Scheduler) listTasks(ctx context.Context) (map[string]map[TaskDescriptor]*Task, error) {
	tasksByNamespace, err := s.taskLister.ListDistributedTasks(ctx)
	if err != nil {
		return nil, fmt.Errorf("list distributed tasks: %w", err)
	}

	result := make(map[string]map[TaskDescriptor]*Task, len(tasksByNamespace))
	for namespace, tasks := range tasksByNamespace {
		result[namespace] = make(map[TaskDescriptor]*Task, len(tasks))
		for _, task := range tasks {
			result[namespace][task.TaskDescriptor] = task
		}
	}
	return result, nil
}

// recordRunningTaskHandleLocked stores a freshly-launched task handle in
// s.runningTasks. Caller MUST hold s.mu. Used from bootstrapProviders
// (which holds the lock across the entire bootstrap) and from
// reconcileRunningTasks's commit step (which re-acquires after the
// slow StartTask call).
func (s *Scheduler) recordRunningTaskHandleLocked(namespace string, desc TaskDescriptor, handle TaskHandle) {
	if _, ok := s.runningTasks[namespace]; !ok {
		s.runningTasks[namespace] = map[TaskDescriptor]TaskHandle{}
	}
	s.runningTasks[namespace][desc] = handle
}

// Close stops the background tick loop and terminates all running task
// handles. Contract:
//
//  1. CANCEL: loopCancel is fired BEFORE the wait barrier so any tick
//     stuck inside a RAFT round-trip (listTasks / ackRecorder writes /
//     finalizer / cleaner / UnitAwareProvider callbacks) unwinds on
//     ctx.Done() rather than holding shutdown indefinitely.
//  2. SYNC: waits on loopDone so no tick is mid-RAFT-apply when Close
//     returns. Without this, the caller's teardown of DB / schema /
//     cluster services races against an in-flight tick.
//
// After Close returns, no new ticks will fire AND no previously-spawned
// tick is still running.
func (s *Scheduler) Close() {
	close(s.stopCh)
	if s.loopCancel != nil {
		s.loopCancel()
	}
	if s.loopDone != nil {
		<-s.loopDone
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, tasks := range s.runningTasks {
		for _, task := range tasks {
			task.Terminate()
		}
	}
}

func (s *Scheduler) setCompletionRecorders(recorder TaskCompletionRecorder) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, provider := range s.providers {
		provider.SetCompletionRecorder(recorder)
	}
}

func (s *Scheduler) totalRunningTaskCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for _, tasks := range s.runningTasks {
		count += len(tasks)
	}
	return count
}

func (s *Scheduler) loggerWithTask(namespace string, taskDesc TaskDescriptor) *logrus.Entry {
	return s.logger.WithFields(logrus.Fields{
		"namespace":   namespace,
		"taskID":      taskDesc.ID,
		"taskVersion": taskDesc.Version,
	})
}

// allLocalGroupsFiredLocked returns true iff every group of the task
// for which this node has at least one local unit has had its
// OnGroupCompleted fire on THIS scheduler instance (i.e.
// s.groupCallbackFired[desc][groupID] == true). Caller must hold s.mu.
//
// Used as the gating predicate for emitting the per-node post-completion
// ack: we only ack after every relevant local group has fired, so the
// aggregated success/error reflects the full local picture for the task.
//
// Groups in which this node has zero local units are skipped — those
// groups' OnGroupCompleted only fires on the nodes that own units in
// them, so this node never has anything to ack for them.
func (s *Scheduler) allLocalGroupsFiredLocked(task *Task, desc TaskDescriptor) bool {
	state := s.perTaskStateLocked(desc)
	for _, groupID := range task.Groups() {
		localIDs := task.LocalGroupUnitIDs(groupID, s.localNode)
		if len(localIDs) == 0 {
			continue
		}
		if state == nil || !state.groupCallbackFired[groupID] {
			return false
		}
	}
	return true
}

// allLocalGroupsPreparationFiredLocked — PREP counterpart to allLocalGroupsFiredLocked.
// Gates per-node RecordPreparationCompleteAck emission. Caller must hold s.mu.
func (s *Scheduler) allLocalGroupsPreparationFiredLocked(task *Task, desc TaskDescriptor) bool {
	state := s.perTaskStateLocked(desc)
	for _, groupID := range task.Groups() {
		localIDs := task.LocalGroupUnitIDs(groupID, s.localNode)
		if len(localIDs) == 0 {
			continue
		}
		if state == nil || !state.preparationCallbackFired[groupID] {
			return false
		}
	}
	return true
}

// aggregateAckErrorsLocked returns (success, joined-error-message) for
// THIS node's OnGroupCompleted results captured in
// postCompletionGroupErrors[desc]. Caller must hold s.mu.
//
// success is true iff every group's OnGroupCompleted returned nil.
// joined is the semicolon-joined error messages from the failing groups,
// empty when success==true. Order is unspecified (map iteration); the
// FSM persists the joined string as-is on PostCompletionAck.Error for
// forensic visibility.
func (s *Scheduler) aggregateAckErrorsLocked(task *Task, desc TaskDescriptor) (bool, string) {
	state := s.perTaskStateLocked(desc)
	if state == nil {
		return true, ""
	}
	errs := state.postCompletionGroupErrors
	if len(errs) == 0 {
		return true, ""
	}
	var msgs []string
	for groupID, err := range errs {
		if err == nil {
			continue
		}
		msgs = append(msgs, fmt.Sprintf("group=%q: %v", groupID, err))
	}
	if len(msgs) == 0 {
		return true, ""
	}
	return false, strings.Join(msgs, "; ")
}

// aggregatePreparationAckErrorsLocked — PREP counterpart to aggregateAckErrorsLocked.
// Caller must hold s.mu.
func (s *Scheduler) aggregatePreparationAckErrorsLocked(task *Task, desc TaskDescriptor) (bool, string) {
	state := s.perTaskStateLocked(desc)
	if state == nil {
		return true, ""
	}
	errs := state.preparationCompletionGroupErrors
	if len(errs) == 0 {
		return true, ""
	}
	var msgs []string
	for groupID, err := range errs {
		if err == nil {
			continue
		}
		msgs = append(msgs, fmt.Sprintf("group=%q: %v", groupID, err))
	}
	if len(msgs) == 0 {
		return true, ""
	}
	return false, strings.Join(msgs, "; ")
}

// perTaskStateLocked returns the per-task scheduler state for desc, or
// nil if no entry exists. Caller must hold s.mu. Read-only callers
// should use this to avoid creating empty entries on map reads.
func (s *Scheduler) perTaskStateLocked(desc TaskDescriptor) *taskSchedulerState {
	return s.perTaskState[desc]
}

// perTaskStateLockedOrInit returns the per-task scheduler state for
// desc, creating an empty entry on demand. Caller must hold s.mu.
// Used at write sites where the absence of an entry should be treated
// the same as a zero-valued entry.
func (s *Scheduler) perTaskStateLockedOrInit(desc TaskDescriptor) *taskSchedulerState {
	state, ok := s.perTaskState[desc]
	if !ok {
		state = &taskSchedulerState{}
		s.perTaskState[desc] = state
	}
	return state
}

// deletePerTaskStateLocked removes the per-task scheduler state for
// desc. Caller must hold s.mu. After the collapse into
// [taskSchedulerState] a single delete on the outer map is enough;
// the previous version had to enumerate seven parallel maps and
// silently leaked any map a contributor forgot to register.
func (s *Scheduler) deletePerTaskStateLocked(desc TaskDescriptor) {
	delete(s.perTaskState, desc)
}

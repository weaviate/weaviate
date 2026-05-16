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

// Scheduler is the component which is responsible for polling the active tasks in the cluster (via the Manager)
// and making sure that the tasks are running on the local node.
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
	tasksLister        TasksLister
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

	completedCallbackFired map[TaskDescriptor]bool
	groupCallbackFired     map[TaskDescriptor]map[string]bool

	// postCompletionAckEmitted tracks per-task whether THIS node has
	// already published its post-completion ack via the ack recorder.
	// The ack is emitted once per (scheduler instance, task) after
	// OnGroupCompleted has fired for every local group of the task.
	// Survival on restart: false (the in-memory map is rebuilt on
	// startup); the recovery path repeats OnGroupCompleted via the
	// rehydrate branch (see ReindexProvider.OnGroupCompleted) so the
	// ack is re-emitted post-restart and the cluster never loses the
	// barrier.
	postCompletionAckEmitted map[TaskDescriptor]bool

	// postCompletionGroupErrors aggregates per-group OnGroupCompleted
	// errors for THIS node across a single task. The scheduler joins
	// the entries when emitting the per-node ack, so the cluster sees
	// every group's failure even if they arrived across multiple ticks
	// (e.g. tenant-grouped reindex tasks). Cleared once the ack has
	// been emitted (postCompletionAckEmitted[desc] == true) so a later
	// re-emit attempt (after a transient apply error) re-aggregates
	// from the still-fired groups in the descriptor list.
	postCompletionGroupErrors map[TaskDescriptor]map[string]error

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
	TasksLister        TasksLister
	TaskCleaner        TaskCleaner
	TaskFinalizer      TaskFinalizer
	// AckRecorder is the RAFT-apply hook used to publish this node's
	// OnGroupCompleted result for each task. May be nil in unit-test
	// constructions; when nil, the scheduler falls back to the legacy
	// pre-#214 behavior (no ack barrier — MarkTaskFinalized fires as
	// soon as OnTaskCompleted returns). Production wiring in
	// configure_api.go always sets this.
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

		providers:                 params.Providers,
		completionRecorder:        params.CompletionRecorder,
		completedCallbackFired:    map[TaskDescriptor]bool{},
		groupCallbackFired:        map[TaskDescriptor]map[string]bool{},
		postCompletionAckEmitted:  map[TaskDescriptor]bool{},
		postCompletionGroupErrors: map[TaskDescriptor]map[string]error{},
		tasksLister:               params.TasksLister,
		taskCleaner:               params.TaskCleaner,
		taskFinalizer:             params.TaskFinalizer,
		ackRecorder:               params.AckRecorder,
		clock:                     params.Clock,

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
	// 3s caps per-unit progress writes on the Raft hot path without making
	// the UI feel frozen on long-running migrations. A coarser cap (the
	// old 30s) is invisible to users watching a 60–90s reindex — every
	// other sample gets eaten and the progress bar appears to jump in
	// large increments. 3s gives ~20 samples per minute per unit, well
	// within Raft's write budget.
	throttledRecorder := NewThrottledRecorder(s.completionRecorder, 3*time.Second, s.clock)

	s.setCompletionRecorders(throttledRecorder)

	// Attempt an initial task listing to bootstrap running tasks. If it fails
	// (e.g. Raft not ready yet), log and continue — tick() will pick tasks up
	// once the cluster is ready, and will run the deferred bootstrap on the
	// first successful tick so post-restart callback replay is still
	// suppressed.
	tasksByNamespace, err := s.listTasks(ctx)
	if err != nil {
		s.logger.WithError(err).Warn("initial distributed task listing failed; bootstrap deferred to first successful tick")
	} else {
		s.bootstrapProviders(tasksByNamespace)
	}

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
			s.completedCallbackFired[desc] = true
			if s.groupCallbackFired[desc] == nil {
				s.groupCallbackFired[desc] = map[string]bool{}
			}
			for _, groupID := range task.Groups() {
				s.groupCallbackFired[desc][groupID] = true
			}
			// Tasks that were already terminal at bootstrap have, by
			// definition, already gone through the ack barrier (or
			// were FAILED/CANCELLED, which bypasses it). Mark the
			// per-node ack as emitted so the next tick does not
			// re-emit for a task that's already past the FINALIZING
			// gate. See 0-weaviate-issues#214 Gap A.
			s.postCompletionAckEmitted[desc] = true
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
			s.loggerWithTask(namespace, taskDesc).WithError(err).
				Error("failed to clean up local distributed task state")
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
			s.loggerWithTask(namespace, desc).WithError(err).
				Error("failed to start distributed task during bootstrap")
			continue
		}

		s.setRunningTaskHandleWithLock(namespace, desc, handle)
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
	tasksByNamespace, err := s.listTasks(context.Background())
	if err != nil {
		s.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
			l.WithError(err).Error("failed to list distributed tasks")
		})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Deferred bootstrap: if listTasks failed at Start() (typically RAFT
	// not ready yet), s.bootstrapped is still false. On this first
	// successful tick, pre-mark every already-terminal task so the
	// callback-firing loop below doesn't replay OnGroupCompleted /
	// OnTaskCompleted for tasks that finished before this scheduler
	// instance existed. Without this, a node that restarts and then
	// takes a few seconds to rejoin RAFT (so Start()'s listTasks
	// returned an error) will fire callbacks for every historical
	// task on its first tick — and the older change-tokenization
	// tasks' schema flips will revert state that newer tasks have
	// already committed.
	if !s.bootstrapped {
		s.preMarkTerminalCallbacksLocked(tasksByNamespace)
		s.bootstrapped = true
		s.logger.Info("distributed task scheduler: deferred bootstrap pre-mark complete on first successful tick")
	}

	for namespace, provider := range s.providers {
		tasks := tasksByNamespace[namespace]

		// Remove dead handles so tasks can be re-launched if they still have pending work.
		// A handle is "dead" when its goroutine has exited (Done() channel is closed).
		for desc, taskHandle := range s.runningTasks[namespace] {
			select {
			case <-taskHandle.Done():
				delete(s.runningTasks[namespace], desc)
			default:
			}
		}

		// Check that all tasks that are supposed to be running
		// and launch if they aren't.
		startedTasks := s.filterStartedTasks(tasks)
		for _, activeTask := range startedTasks {
			if _, alreadyLaunched := s.runningTasks[namespace][activeTask.TaskDescriptor]; alreadyLaunched {
				continue
			}

			handle, err := provider.StartTask(activeTask)
			if err != nil {
				s.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
					s.loggerWithTask(namespace, activeTask.TaskDescriptor).WithError(err).
						Error("failed to start distributed task")
				})
				continue
			}

			s.setRunningTaskHandleWithLock(namespace, activeTask.TaskDescriptor, handle)
			s.loggerWithTask(namespace, activeTask.TaskDescriptor).Info("started distributed task execution")
		}

		s.tasksRunning.
			WithLabelValues(namespace).
			Set(float64(len(startedTasks)))

		// Check that all tasks that are not supposed to be running are not running.
		for desc, taskHandle := range s.runningTasks[namespace] {
			if _, ok := startedTasks[desc]; ok {
				continue
			}

			taskHandle.Terminate()
			delete(s.runningTasks[namespace], desc)

			s.loggerWithTask(namespace, desc).Info("terminated distributed task execution")

		}

		// Fire group-level and task-level callbacks for unit-aware providers.
		// OnGroupCompleted fires per-group as each group's units all reach terminal
		// state (can fire mid-flight while task is still STARTED).
		// OnTaskCompleted fires once when the task reaches the FINALIZING
		// (success path) or FAILED state. FINISHED tasks have already had
		// their callbacks fire — the FINISHED transition is committed by
		// [TaskFinalizer.MarkDistributedTaskFinalized] below only AFTER
		// OnTaskCompleted returns successfully, so a task in the FINISHED
		// state is by construction past this point. The callback-fired
		// maps' pre-mark from [Scheduler.bootstrapProviders] (and the
		// deferred-bootstrap path in this tick) also marks FINISHED tasks
		// as already-fired so a node restart can't replay them.
		_, providerIsUnitAware := provider.(UnitAwareProvider)
		if suProvider, ok := provider.(UnitAwareProvider); ok {
			for desc, task := range tasks {
				if task.Status == TaskStatusCancelled {
					continue
				}

				// Phase 1: per-group finalization (fires mid-flight as groups complete).
				// A group is ready to finalize when either:
				//   - All units in the group are terminal (normal completion), OR
				//   - The task itself is past STARTED (fail-fast or all units
				//     terminal: remaining units won't complete)
				postStarted := task.Status == TaskStatusFinalizing ||
					task.Status == TaskStatusFailed ||
					task.Status == TaskStatusFinished
				for _, groupID := range task.Groups() {
					if s.groupCallbackFired[desc] != nil && s.groupCallbackFired[desc][groupID] {
						continue
					}
					if !postStarted && !task.AllGroupUnitsTerminal(groupID) {
						continue
					}
					localIDs := task.LocalGroupUnitIDs(groupID, s.localNode)
					if len(localIDs) > 0 {
						if s.groupCallbackFired[desc] == nil {
							s.groupCallbackFired[desc] = map[string]bool{}
						}
						s.groupCallbackFired[desc][groupID] = true
						groupErr := suProvider.OnGroupCompleted(task, groupID, localIDs)
						// Context-cancellation handling: a graceful
						// process shutdown (SIGTERM during a rolling
						// restart) propagates ctx.Canceled into the
						// per-shard RunSwapOnShard, which fails with
						// the lsmkv "long-running compaction in
						// progress: context canceled" shape (see
						// 0-weaviate-issues#213). That is NOT a
						// permanent failure — the post-restart
						// recovery path re-fires OnGroupCompleted via
						// the rehydrate branch on a fresh process
						// with no in-flight compaction to cancel, and
						// the swap completes cleanly. Treating
						// shutdown-cancellation as a failure ack
						// would prematurely flip the task to FAILED
						// and short-circuit the recovery — exactly
						// the symptom CI surfaced on
						// TestMultiNode_RollingRestartDuringFinalizing.
						//
						// Drop the in-memory "fired" mark for this
						// group so the next scheduler tick (on the
						// restarted process) re-fires
						// OnGroupCompleted and the ack-emission gate
						// below sees a fresh success/failure picture.
						// Skip the per-group error capture and the
						// ack emission — let the recovery path own
						// the resolution.
						if errors.Is(groupErr, context.Canceled) {
							delete(s.groupCallbackFired[desc], groupID)
							s.loggerWithTask(namespace, desc).
								WithField("groupID", groupID).
								Info("OnGroupCompleted aborted by graceful shutdown; recovery on next boot will re-fire and emit the post-completion ack (0-weaviate-issues#214 + #213)")
							continue
						}
						// Capture the per-group error so the
						// per-node post-completion ack can report the
						// aggregated picture (0-weaviate-issues#214
						// Gap A). Even success (nil) is captured so
						// the ack-emission predicate below can see
						// "this group fired and succeeded" vs "this
						// group hasn't fired yet on this node".
						if s.postCompletionGroupErrors[desc] == nil {
							s.postCompletionGroupErrors[desc] = map[string]error{}
						}
						s.postCompletionGroupErrors[desc][groupID] = groupErr
					}
				}

				// Phase 1.5: emit the per-node post-completion ack
				// once every group this node owns local units in has
				// fired OnGroupCompleted (success or failure). The
				// scheduler aggregates per-group errors into one
				// per-(node, task) ack so the cluster has a single
				// barrier per node.
				//
				// Gating conditions:
				//   - ack recorder configured (production wiring; nil
				//     in legacy unit-test setups, where we fall back
				//     to the pre-#214 behavior).
				//   - task is past STARTED (the post-completion barrier
				//     is only meaningful from FINALIZING onward).
				//   - ack not yet emitted this scheduler-instance.
				//   - every group this node has local units in has had
				//     OnGroupCompleted fire on this scheduler instance.
				//
				// Survives restart: postCompletionAckEmitted is empty
				// on a fresh scheduler; LocalCallbacksDone (in the
				// recovery-aware provider) gates whether the
				// bootstrap pre-mark skips the task. If recovery is
				// needed, the next tick re-fires OnGroupCompleted via
				// the provider's rehydrate path, which re-populates
				// postCompletionGroupErrors[desc] and we re-emit the
				// ack here.
				if s.ackRecorder != nil &&
					!s.postCompletionAckEmitted[desc] &&
					task.Status != TaskStatusStarted &&
					s.allLocalGroupsFiredLocked(task, desc) {
					success, joined := s.aggregateAckErrorsLocked(task, desc)
					if err := s.ackRecorder.RecordDistributedTaskPostCompletionAck(
						context.Background(), namespace, task.ID, task.Version,
						s.localNode, success, joined,
					); err != nil {
						// Apply error (leader unreachable, RAFT not
						// ready, etc.). Leave postCompletionAckEmitted
						// unset so the next tick / wake retries. The
						// Manager's RecordPostCompletionAck is
						// idempotent at the FSM layer, so a successful
						// retry after a partial commit is safe.
						s.loggerWithTask(namespace, desc).WithError(err).
							Warn("failed to record distributed task post-completion ack; will retry on next tick or wake")
					} else {
						s.postCompletionAckEmitted[desc] = true
						// Reflect the just-emitted ack on the local
						// task clone so the OnTaskCompleted gate
						// below in this same tick sees the updated
						// post-completion state without an extra
						// listTasks round-trip. The clone is
						// per-tick (from the listTasks() above) so
						// the mutation is process-local. If the ack
						// was a failure, also flip the local clone's
						// status to FAILED so the same tick can fire
						// OnTaskCompleted on FAILED (which skips the
						// schema flip but still runs per-node
						// cleanup); the FSM-side flip is the
						// authoritative one and will be observed by
						// every other node's next tick.
						if task.PostCompletionAcks == nil {
							task.PostCompletionAcks = map[string]PostCompletionAck{}
						}
						task.PostCompletionAcks[s.localNode] = PostCompletionAck{
							Success: success,
							Error:   joined,
							AckedAt: s.clock.Now(),
						}
						if !success && task.Status == TaskStatusFinalizing {
							task.Status = TaskStatusFailed
						}
					}
				}

				// Phase 2: global task completion. Fires on FINALIZING (success
				// path — every unit COMPLETED, no failures), FAILED, or
				// FINISHED. FINISHED is included so a node that observes
				// the task only after MarkDistributedTaskFinalized has
				// already flipped it past FINALIZING still gets to fire
				// OnTaskCompleted exactly once: the first node to see
				// FINALIZING will issue MarkFinalized inside the same
				// tick, so other nodes' next tick sees FINISHED, not
				// FINALIZING. Without FINISHED here, those other nodes
				// silently skip the callback, breaking idempotent
				// per-node post-completion work (reindex provider clears
				// caches and emits its completion marker from here).
				// The pre-mark in [Scheduler.bootstrapProviders] still
				// suppresses replay for tasks that were already FINISHED
				// at bootstrap; only "FINISHED first observed by this
				// scheduler instance" fires the callback.
				//
				// The MarkDistributedTaskFinalized RAFT command is issued
				// in the finalize-issuance block below only for FINALIZING
				// tasks — FINISHED is already terminal, so re-issuing the
				// finalize would be a wasted no-op RAFT round-trip.
				// CANCELLED tasks are skipped at the top of the outer
				// loop (line above) so they never reach here.
				//
				// Post-completion ack gate: on the FINALIZING path,
				// wait until every node with local units has recorded
				// an ack. This is the cluster-wide crash-safety
				// barrier from 0-weaviate-issues#214 Gap A — without
				// it, OnTaskCompleted's schema flip could commit
				// while one replica's RunSwapOnShard had silently
				// failed, leaving that replica permanently wrong-
				// tokenized. The FAILED / FINISHED paths bypass this
				// gate: FAILED already short-circuits the schema flip
				// inside OnTaskCompleted (see reindex_provider.go
				// ~L1045), and FINISHED has already committed past
				// the ack barrier on whichever node won the
				// MarkDistributedTaskFinalized race.
				readyForFinalize := task.Status == TaskStatusFinalizing ||
					task.Status == TaskStatusFailed ||
					task.Status == TaskStatusFinished
				if readyForFinalize && !s.completedCallbackFired[desc] {
					if s.ackRecorder != nil && task.Status == TaskStatusFinalizing {
						missing := task.MissingPostCompletionAckNodes()
						if len(missing) > 0 {
							// Not all nodes have acked yet. Wait —
							// schema flip + MarkFinalized must not
							// commit until the cluster has decided
							// the post-completion outcome.
							continue
						}
					}
					s.completedCallbackFired[desc] = true
					suProvider.OnTaskCompleted(task)
				}
			}
		}

		// Issue MarkDistributedTaskFinalized for FINALIZING tasks. For
		// unit-aware providers we wait until OnTaskCompleted has fired
		// (s.completedCallbackFired[desc] == true) so the FINISHED
		// transition lines up with "every post-completion callback
		// committed cluster-wide." For non-unit-aware providers there is
		// no OnTaskCompleted to gate on — the task transitions straight
		// from FINALIZING to FINISHED as soon as the scheduler sees the
		// FINALIZING status.
		if s.taskFinalizer != nil {
			for desc, task := range tasks {
				if task.Status != TaskStatusFinalizing {
					continue
				}
				if providerIsUnitAware && !s.completedCallbackFired[desc] {
					// OnTaskCompleted hasn't fired yet (e.g. provider's
					// callback returned an error so the fired flag was
					// reset, or the task only just transitioned to
					// FINALIZING). Wait until the next tick.
					continue
				}
				if err := s.taskFinalizer.MarkDistributedTaskFinalized(
					context.Background(), namespace, task.ID, task.Version,
				); err != nil {
					s.loggerWithTask(namespace, desc).WithError(err).
						Warn("failed to mark distributed task finalized; will retry on next tick or wake")
					// For unit-aware providers, reset the fired flag so a
					// subsequent tick or wake retries OnTaskCompleted +
					// finalize. OnTaskCompleted is idempotent at the
					// provider layer (the reindex schema flip is
					// RAFT-applied with apply=false on no-op), so
					// re-firing is safe.
					if providerIsUnitAware {
						s.completedCallbackFired[desc] = false
					}
				}
			}
		}

		// Check that all tasks that are already finished and if their TTL has passed, so we can clean them up.
		// FINALIZING is excluded explicitly: its FinishedAt is zero-time
		// (set by [Manager.MarkTaskFinalized] only on the FINISHED
		// transition), and `clock.Since(zero)` is enormous — without the
		// exclusion the predicate would mis-classify every FINALIZING task
		// as TTL-expired and request its cleanup before its post-completion
		// callbacks finish.
		cleanableTasks := filterTasks(tasks, func(task *Task) bool {
			if task.Status == TaskStatusStarted || task.Status == TaskStatusFinalizing {
				return false
			}
			return s.completedTaskTTL <= s.clock.Since(task.FinishedAt)
		})
		for _, task := range cleanableTasks {
			err = s.taskCleaner.CleanUpDistributedTask(context.Background(), namespace, task.ID, task.Version)
			if err != nil {
				s.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
					s.loggerWithTask(namespace, task.TaskDescriptor).WithError(err).
						Error("failed to clean up distributed task")
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

			delete(s.completedCallbackFired, desc)
			delete(s.groupCallbackFired, desc)
			delete(s.postCompletionAckEmitted, desc)
			delete(s.postCompletionGroupErrors, desc)

			if err = provider.CleanupTask(desc); err != nil {
				s.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
					s.loggerWithTask(namespace, desc).WithError(err).
						Error("failed to clean up local distributed task state")
				})
			}
		}
	}
}

func (s *Scheduler) listTasks(ctx context.Context) (map[string]map[TaskDescriptor]*Task, error) {
	tasksByNamespace, err := s.tasksLister.ListDistributedTasks(ctx)
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

func (s *Scheduler) setRunningTaskHandleWithLock(namespace string, desc TaskDescriptor, handle TaskHandle) {
	if _, ok := s.runningTasks[namespace]; !ok {
		s.runningTasks[namespace] = map[TaskDescriptor]TaskHandle{}
	}
	s.runningTasks[namespace][desc] = handle
}

// Close stops the background tick loop and terminates all running task handles. It blocks
// until all handles have been signalled. After Close returns, no new ticks will fire.
func (s *Scheduler) Close() {
	close(s.stopCh)

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
//
// See 0-weaviate-issues#214 Gap A.
func (s *Scheduler) allLocalGroupsFiredLocked(task *Task, desc TaskDescriptor) bool {
	for _, groupID := range task.Groups() {
		localIDs := task.LocalGroupUnitIDs(groupID, s.localNode)
		if len(localIDs) == 0 {
			continue
		}
		if s.groupCallbackFired[desc] == nil || !s.groupCallbackFired[desc][groupID] {
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
//
// See 0-weaviate-issues#214 Gap A.
func (s *Scheduler) aggregateAckErrorsLocked(task *Task, desc TaskDescriptor) (bool, string) {
	errs := s.postCompletionGroupErrors[desc]
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

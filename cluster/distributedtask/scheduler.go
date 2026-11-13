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
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/errorcompounder"
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
//   - swapPending / prepPending: groups PHASE B / PHASE A may still act on; nil until the first tick.
//   - postCompletionAckEmitted: per-task one-shot for emitting the per-node
//     PostCompletionAck via [PostCompletionAckRecorder.RecordDistributedTaskPostCompletionAck].
type taskSchedulerState struct {
	completedCallbackFired           bool
	swapPending                      map[string]struct{}
	prepPending                      map[string]struct{}
	postCompletionAckEmitted         bool
	postCompletionGroupErrors        map[string]error
	preparationCompletionGroupErrors map[string]error
	swapAcksPending                  map[string]error
	// completedCallbackAttempts counts transient OnTaskCompleted failures on
	// the SWAPPING path; at [maxCompletedCallbackAttempts] the scheduler stops
	// retrying and fails the task (weaviate/0-weaviate-issues#297).
	completedCallbackAttempts int
}

// maxCompletedCallbackAttempts bounds transient OnTaskCompleted retries on
// the SWAPPING path before the task is FAILED. [ErrTaskCompletionPermanent]
// failures bypass this and fail on the first attempt.
const maxCompletedCallbackAttempts = 5

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
	// startLogged holds the started tasks whose first start was logged; pruned each tick, since some providers never report tasks for cleanup.
	startLogged map[string]map[TaskDescriptor]struct{}

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

	// A steady-state reporter gets its own sampler: it re-reports every
	// tick until something outside the scheduler clears the condition, so
	// sharing sampledLogger's budget would crowd out its one-off errors.
	unrecognizedStatusLogger *logrusext.Sampler
	postCompletionAckLogger  *logrusext.Sampler
	relaunchLogger           *logrusext.Sampler
	swapRetryLogger          *logrusext.Sampler

	tasksRunning *prometheus.GaugeVec

	// The alertable form of the unrecognized-status warn, which is sampled
	// and so cannot carry a condition that outlives a node's data dir.
	tasksUnrecognizedStatus *prometheus.GaugeVec

	// The label set the gauge carried last tick, so a namespace that
	// stopped reporting can be dropped one series at a time. Tick loop only.
	unrecognizedStatusNamespaces map[string]struct{}

	localTaskInspector LocalTaskInspector

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
	AckRecorder PostCompletionAckRecorder
	// nil in unit tests that don't exercise the unrecognized-status warn.
	LocalTaskInspector LocalTaskInspector
	Providers          map[string]Provider
	Clock              clockwork.Clock
	Logger             logrus.FieldLogger
	MetricsRegisterer  prometheus.Registerer

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
		startLogged:  map[string]map[TaskDescriptor]struct{}{},

		providers:          params.Providers,
		completionRecorder: params.CompletionRecorder,
		perTaskState:       map[TaskDescriptor]*taskSchedulerState{},
		taskLister:         params.TaskLister,
		taskCleaner:        params.TaskCleaner,
		taskFinalizer:      params.TaskFinalizer,
		ackRecorder:        params.AckRecorder,
		localTaskInspector: params.LocalTaskInspector,
		clock:              params.Clock,

		localNode:        params.LocalNode,
		completedTaskTTL: params.CompletedTaskTTL,
		tickInterval:     params.TickInterval,

		logger:        params.Logger,
		sampledLogger: logrusext.NewSampler(params.Logger, 5, 5*params.TickInterval),

		unrecognizedStatusLogger: logrusext.NewSampler(params.Logger, 3, unrecognizedStatusWarnWindow),
		postCompletionAckLogger:  logrusext.NewSampler(params.Logger, 5, 5*params.TickInterval),
		relaunchLogger:           logrusext.NewSampler(params.Logger, 5, 5*params.TickInterval),
		swapRetryLogger:          logrusext.NewSampler(params.Logger, 5, 5*params.TickInterval),

		tasksRunning: promauto.With(params.MetricsRegisterer).NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_distributed_tasks_running",
			Help: "Number of active distributed tasks running per namespace",
		}, []string{"namespace"}),

		tasksUnrecognizedStatus: promauto.With(params.MetricsRegisterer).NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_distributed_tasks_unrecognized_status",
			Help: "Number of distributed tasks in a status this build does not recognize, per namespace",
		}, []string{"namespace"}),
		unrecognizedStatusNamespaces: map[string]struct{}{},

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

		s.cleanupStaleTasks(namespace, provider, tasksByNamespace[namespace])
		s.startActiveTasks(namespace, provider, startedTasks)

		s.tasksRunning.
			WithLabelValues(namespace).
			Set(float64(len(startedTasks)))
	}

	// Pre-mark already-terminal tasks so the tick skips their callbacks, for
	// every namespace, including ones this node hosts no provider for.
	s.preMarkTerminalCallbacksLocked(tasksByNamespace)
	s.bootstrapped = true
}

// preMarkTerminalCallbacksLocked marks terminal tasks fired so a restart does not replay their callbacks:
// an older task replaying after a newer one's flip would silently revert the schema cluster-wide.
//
// Caller MUST hold s.mu.
func (s *Scheduler) preMarkTerminalCallbacksLocked(tasksByNamespace map[string]map[TaskDescriptor]*Task) {
	for namespace, tasks := range tasksByNamespace {
		provider := s.providers[namespace]
		for desc, task := range tasks {
			if !task.Status.IsTerminal() {
				continue
			}
			// A [RecoveryAwareProvider] that reports local callback work
			// as not done skips the pre-mark, so the next tick
			// re-dispatches this task's callbacks. That recovers
			// nothing: every callback is a no-op at terminal status. The
			// one durable effect is a re-issued post-completion ack, once
			// per process start, until the completed-task TTL drops the
			// task (see [RecoveryAwareProvider.LocalCallbacksDone]). Only
			// FINISHED tasks are asked: a failed or cancelled task has no
			// completed swap that could be missing.
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
			state.prepPending = map[string]struct{}{}
			if !task.NeedsPreparationBarrier {
				state.swapPending = map[string]struct{}{}
			}
			state.postCompletionAckEmitted = true
		}
	}
}

// cleanupStaleTasks removes local state for tasks that the provider knows about
// but that are no longer in the task list.
func (s *Scheduler) cleanupStaleTasks(namespace string, provider Provider, tasks map[TaskDescriptor]*Task) {
	for _, taskDesc := range provider.GetLocalTasks() {
		if _, ok := tasks[taskDesc]; ok {
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

// Long, because the condition holds until an operator acts on it and
// every replicated progress update wakes the tick loop.
const unrecognizedStatusWarnWindow = time.Hour

// warnOnUnrecognizedStatuses reports the tasks in a status this build
// never declared.
//
// One aggregated sorted line per tick: naming the tasks one at a time let
// the sampler's budget pick an arbitrary three by map-iteration order,
// which is not a set an operator can act on.
//
// leaderListAvailable says whether tasksByNamespace is the cluster's task
// list or nothing at all. It gates the metric prune: only the cluster's
// list knows which namespaces exist.
func (s *Scheduler) warnOnUnrecognizedStatuses(
	tasksByNamespace map[string]map[TaskDescriptor]*Task,
	leaderListAvailable bool,
) {
	counts := make(map[string]int, len(tasksByNamespace))
	var named []string
	for namespace, tasks := range tasksByNamespace {
		counts[namespace] = 0
		for _, task := range tasks {
			if task.Status.IsRecognized() {
				continue
			}
			counts[namespace]++
			named = append(named, fmt.Sprintf("%s/%s@%d=%s",
				namespace, task.ID, task.Version, task.Status))
		}
	}
	if s.localTaskInspector != nil {
		for namespace, tasks := range s.localTaskInspector.LocalUnrecognizedDistributedTasks() {
			if _, ok := counts[namespace]; !ok {
				counts[namespace] = 0
			}
			for _, task := range tasks {
				// Skip only what the loop above already counted. Skipping on
				// the leader having any copy at all loses the task whose copy
				// the leader can name and this node cannot — counted nowhere,
				// while this node keeps refusing schema mutations for it.
				leaderCopy, onLeaderList := tasksByNamespace[namespace][task.TaskDescriptor]
				if onLeaderList && !leaderCopy.Status.IsRecognized() {
					continue
				}
				counts[namespace]++
				// Naming the cluster's status is what makes the line
				// actionable: it says this node has fallen behind the rest,
				// rather than that the task exists nowhere else.
				where := "this node only"
				if onLeaderList {
					where = fmt.Sprintf("this node only; cluster reports %s", leaderCopy.Status)
				}
				named = append(named, fmt.Sprintf("%s/%s@%d=%s (%s)",
					namespace, task.ID, task.Version, task.Status, where))
			}
		}
	}

	// Write every namespace's value before dropping the ones that stopped
	// reporting. Reset-then-Set drops every series first, and a scrape in
	// that window reads the metric the way it reads a node that is down.
	for namespace, count := range counts {
		s.tasksUnrecognizedStatus.WithLabelValues(namespace).Set(float64(count))
		s.unrecognizedStatusNamespaces[namespace] = struct{}{}
	}
	// A tick without the cluster's list saw only this node's own
	// unrecognized copies, which say nothing about the namespaces that
	// have none. Pruning off that view drops the series reporting zero
	// and the ones reporting a real count alike.
	if leaderListAvailable {
		for namespace := range s.unrecognizedStatusNamespaces {
			if _, stillReporting := counts[namespace]; !stillReporting {
				s.tasksUnrecognizedStatus.DeleteLabelValues(namespace)
				delete(s.unrecognizedStatusNamespaces, namespace)
			}
		}
	}

	if len(named) == 0 {
		return
	}
	sort.Strings(named)

	s.unrecognizedStatusLogger.WithSampling(func(l logrus.FieldLogger) {
		l.WithField("unrecognized_tasks", named).
			Warnf("%d distributed task(s) in an unrecognized status: %s. Each counts as in flight, "+
				"so schema mutations and backups stay refused until it reaches a terminal state on "+
				"the nodes that do recognize the status. A cancel will not help: the FSM refuses "+
				"one for a status this build cannot classify, on every node",
				len(named), strings.Join(named, ", "))
	})
}

func (s *Scheduler) tick() {
	tasksByNamespace, err := s.listTasks(context.Background())
	if err != nil {
		s.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
			l.Errorf("failed to list distributed tasks: %v", err)
		})
		// Warn on the error path rather than returning: returning would
		// freeze the gauge at its last value for as long as the failure
		// lasts, and this node's own copies need no leader anyway.
		s.warnOnUnrecognizedStatuses(nil, false)
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

	s.warnOnUnrecognizedStatuses(tasksByNamespace, true)

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
		// state is by construction past this point. The bootstrap pre-mark stops a restart replaying it.
		suProvider, providerIsUnitAware := provider.(UnitAwareProvider)
		if providerIsUnitAware {
			for desc, task := range tasks {
				if task.NeedsPreparationBarrier {
					s.runDecidedSwapPhase(namespace, desc, task, suProvider)
				} else if task.Status != TaskStatusCancelled {
					s.runSwapPhase(namespace, desc, task, suProvider)
				}

				if task.NeedsPreparationBarrier &&
					(task.Status == TaskStatusStarted || task.Status == TaskStatusPreparing) {
					s.runPreparationPhase(namespace, desc, task, suProvider)
				}

				// Phase 2: fire OnTaskCompleted on SWAPPING/FAILED/FINISHED/
				// CANCELLED. FINISHED is included because the first node to
				// see SWAPPING issues MarkFinalized in the same tick, so
				// other nodes' next tick may already see FINISHED. On the
				// SWAPPING path wait until every node has acked: the schema
				// flip can't commit while any replica's swap is undetermined.
				//
				// A non-nil OnTaskCompleted error on the SWAPPING path withholds
				// FINISHED and retries or fails the task; see
				// runCompletedCallbackPhase (weaviate/0-weaviate-issues#297).
				s.runCompletedCallbackPhase(namespace, desc, task, suProvider)
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

		// TTL-cleanup of finished tasks.
		retainer, providerRetains := provider.(CompletedTaskRetainer)
		cleanableTasks := filterTasks(tasks, func(task *Task) bool {
			if task.Status.IsActive() {
				return false
			}
			if s.completedTaskTTL > s.clock.Since(task.FinishedAt) {
				return false
			}
			// A provider may veto expiry of a record that is still
			// load-bearing (e.g. drop-vector coverage records while their
			// marker is pending — expiring the chain forces a periodic
			// full re-clean). Proposal-side only: the FSM stays untouched.
			return !providerRetains || !retainer.ShouldRetainCompletedTask(task, tasks)
		})
		for _, task := range cleanableTasks {
			err = s.taskCleaner.CleanUpDistributedTask(context.Background(), namespace, task.ID, task.Version)
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
		for desc := range s.startLogged[namespace] {
			if _, ok := startedTasks[desc]; !ok {
				delete(s.startLogged[namespace], desc)
			}
		}
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
		relaunch := func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.recordRunningTaskHandleLocked(namespace, activeTask.TaskDescriptor, handle)
			if s.startLogged[namespace] == nil {
				s.startLogged[namespace] = map[TaskDescriptor]struct{}{}
			}
			_, logged := s.startLogged[namespace][activeTask.TaskDescriptor]
			s.startLogged[namespace][activeTask.TaskDescriptor] = struct{}{}
			return logged
		}()
		if !relaunch {
			s.loggerWithTask(namespace, activeTask.TaskDescriptor).Info("started distributed task execution")
			continue
		}
		// A task whose unit waits on an operator is relaunched on every tick and wake.
		s.relaunchLogger.WithSampling(func(logrus.FieldLogger) {
			s.loggerWithTask(namespace, activeTask.TaskDescriptor).Info("started distributed task execution")
		})
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

// runPreparationPhase: s.mu is never held across provider callbacks or RAFT writes.
func (s *Scheduler) runPreparationPhase(
	namespace string,
	desc TaskDescriptor,
	task *Task,
	suProvider UnitAwareProvider,
) {
	worklist := func() []preparationGroupWork {
		s.mu.Lock()
		defer s.mu.Unlock()
		state := s.perTaskStateLockedOrInit(desc)
		if state.prepPending == nil {
			state.prepPending = task.groupSet()
		}
		var list []preparationGroupWork
		for groupID := range state.prepPending {
			if task.GroupSwapDecided(groupID) {
				delete(state.prepPending, groupID)
				continue
			}
			if !task.AllGroupUnitsTerminal(groupID) {
				continue
			}
			delete(state.prepPending, groupID)
			if ids := task.LocalGroupUnitIDs(groupID, s.localNode); len(ids) > 0 {
				list = append(list, preparationGroupWork{groupID: groupID, localIDs: ids})
			}
		}
		return list
	}()

	groupErrors := make([]error, len(worklist))
	for i, w := range worklist {
		groupErrors[i] = suProvider.OnGroupCompleted(task, w.groupID, w.localIDs)
	}

	acks := func() []groupAck {
		s.mu.Lock()
		defer s.mu.Unlock()
		state := s.perTaskStateLocked(desc)
		if state == nil {
			return nil
		}
		for i, w := range worklist {
			if errors.Is(groupErrors[i], context.Canceled) {
				state.prepPending[w.groupID] = struct{}{}
				// This line is at Debug because a step that waits on an operator re-fires every tick, and the provider logs that wait itself.
				s.loggerWithTask(namespace, desc).
					WithField("groupID", w.groupID).
					Debugf("PREP phase did not complete and is retryable; a later tick or the next boot re-fires it and emits the prep-complete ack: %v", groupErrors[i])
				continue
			}
			if state.preparationCompletionGroupErrors == nil {
				state.preparationCompletionGroupErrors = map[string]error{}
			}
			state.preparationCompletionGroupErrors[w.groupID] = groupErrors[i]
		}
		if s.ackRecorder == nil {
			return nil
		}
		var pending []groupAck
		for _, groupID := range slices.Sorted(maps.Keys(state.preparationCompletionGroupErrors)) {
			pending = append(pending, groupAck{groupID: groupID, err: state.preparationCompletionGroupErrors[groupID]})
		}
		return pending
	}()

	s.sendGroupAcks(namespace, desc, "prep-complete", acks,
		func(groupID string, success bool, errMsg string) error {
			return s.ackRecorder.RecordDistributedTaskPreparationCompleteAck(
				context.Background(), namespace, task.ID, task.Version, groupID, s.localNode, success, errMsg)
		},
		func(state *taskSchedulerState) map[string]error { return state.preparationCompletionGroupErrors })
}

type groupAck struct {
	groupID string
	err     error
}

func (s *Scheduler) sendGroupAcks(namespace string, desc TaskDescriptor, kind string, acks []groupAck,
	record func(groupID string, success bool, errMsg string) error,
	pendingOf func(*taskSchedulerState) map[string]error,
) {
	for i, ack := range acks {
		var errMsg string
		if ack.err != nil {
			errMsg = ack.err.Error()
		}
		if ackErr := record(ack.groupID, ack.err == nil, errMsg); ackErr != nil {
			s.postCompletionAckLogger.WithSampling(func(l logrus.FieldLogger) {
				s.loggerWithTask(namespace, desc).WithField("pending_group_count", len(acks)-i).
					Warnf("failed to record distributed task %s acks; will retry on next tick or wake: %v", kind, ackErr)
			})
			return
		}
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if state := s.perTaskStateLocked(desc); state != nil {
				delete(pendingOf(state), ack.groupID)
			}
		}()
	}
}

// swapGroupWork captures one PHASE-B per-group work item. Populated
// under s.mu in the snapshot step; consumed without s.mu in the
// process step.
type swapGroupWork struct {
	groupID  string
	localIDs []string
}

func (s *Scheduler) runSwapPhase(
	namespace string,
	desc TaskDescriptor,
	task *Task,
	suProvider UnitAwareProvider,
) {
	postStarted := task.Status == TaskStatusSwapping ||
		task.Status == TaskStatusFailed ||
		task.Status == TaskStatusFinished

	worklist := func() []swapGroupWork {
		s.mu.Lock()
		defer s.mu.Unlock()
		state := s.perTaskStateLockedOrInit(desc)
		if state.swapPending == nil {
			state.swapPending = task.groupSet()
		}
		var list []swapGroupWork
		for groupID := range state.swapPending {
			if !postStarted && !task.AllGroupUnitsTerminal(groupID) {
				continue
			}
			delete(state.swapPending, groupID)
			if localIDs := task.LocalGroupUnitIDs(groupID, s.localNode); len(localIDs) > 0 {
				list = append(list, swapGroupWork{groupID: groupID, localIDs: localIDs})
			}
		}
		return list
	}()

	groupErrors := make([]error, len(worklist))
	for i, w := range worklist {
		groupErrors[i] = suProvider.OnGroupCompleted(task, w.groupID, w.localIDs)
	}

	type swapAckSnapshot struct {
		eligible bool
		success  bool
		joined   string
	}
	ackSnap := func() swapAckSnapshot {
		s.mu.Lock()
		defer s.mu.Unlock()
		state := s.perTaskStateLocked(desc)
		if state == nil {
			return swapAckSnapshot{}
		}
		for i, w := range worklist {
			if errors.Is(groupErrors[i], context.Canceled) {
				state.swapPending[w.groupID] = struct{}{}
				s.loggerWithTask(namespace, desc).
					WithField("groupID", w.groupID).
					Debugf("SWAP callback did not complete and is retryable; a later tick or the next boot re-fires it and emits the post-completion ack: %v", groupErrors[i])
				continue
			}
			// Record nil too: the ack gate must tell "succeeded" from "not returned".
			if state.postCompletionGroupErrors == nil {
				state.postCompletionGroupErrors = map[string]error{}
			}
			state.postCompletionGroupErrors[w.groupID] = groupErrors[i]
		}
		if s.ackRecorder == nil || state.postCompletionAckEmitted || task.Status != TaskStatusSwapping {
			return swapAckSnapshot{}
		}
		// A failure is acked at once; only a success waits for every group.
		success, joined := s.joinGroupErrors(state.postCompletionGroupErrors)
		if success && !s.allLocalGroupsReturnedLocked(task, desc) {
			return swapAckSnapshot{}
		}
		return swapAckSnapshot{eligible: true, success: success, joined: joined}
	}()
	if !ackSnap.eligible {
		return
	}

	ackErr := s.ackRecorder.RecordDistributedTaskPostCompletionAck(
		context.Background(), namespace, task.ID, task.Version,
		"", s.localNode, ackSnap.success, ackSnap.joined,
	)

	s.mu.Lock()
	defer s.mu.Unlock()
	if ackErr != nil {
		// Leave postCompletionAckEmitted unset; FSM-side ack is
		// idempotent so retry is safe.
		s.postCompletionAckLogger.WithSampling(func(l logrus.FieldLogger) {
			s.loggerWithTask(namespace, desc).
				Warnf("failed to record distributed task post-completion ack; will retry on next tick or wake: %v", ackErr)
		})
		return
	}
	if afterAckState := s.perTaskStateLocked(desc); afterAckState != nil {
		afterAckState.postCompletionAckEmitted = true
	}
	// Mirrors the FSM's ack record onto the per-tick clone so later phases see it without re-listing.
	if task.PostCompletionAcks == nil {
		task.PostCompletionAcks = map[string]PostCompletionAck{}
	}
	task.PostCompletionAcks[s.localNode] = PostCompletionAck{
		Success: ackSnap.success,
		Error:   ackSnap.joined,
		AckedAt: s.clock.Now(),
	}
	if !ackSnap.success {
		task.markTerminal(TaskStatusFailed, s.clock.Now())
	}
}

func (s *Scheduler) runDecidedSwapPhase(
	namespace string,
	desc TaskDescriptor,
	task *Task,
	suProvider UnitAwareProvider,
) {
	worklist := func() []swapGroupWork {
		s.mu.Lock()
		defer s.mu.Unlock()
		state := s.perTaskStateLockedOrInit(desc)
		if state.swapPending == nil && task.Status.IsTerminal() {
			state.swapPending = make(map[string]struct{}, len(task.SwapsOwed))
			for groupID := range task.SwapsOwed {
				state.swapPending[groupID] = struct{}{}
			}
		} else if state.swapPending == nil {
			state.swapPending = task.groupSet()
		}
		var list []swapGroupWork
		for groupID := range state.swapPending {
			if !task.GroupSwapDecided(groupID) {
				if task.Status.IsTerminal() {
					delete(state.swapPending, groupID)
				}
				continue
			}
			delete(state.swapPending, groupID)
			if !task.SwapOwedBy(groupID, s.localNode) {
				continue
			}
			list = append(list, swapGroupWork{groupID: groupID, localIDs: task.LocalGroupUnitIDs(groupID, s.localNode)})
		}
		return list
	}()

	groupErrors := make([]error, len(worklist))
	for i, w := range worklist {
		groupErrors[i] = suProvider.OnSwapRequested(task, w.groupID, w.localIDs)
	}

	acks := func() []groupAck {
		s.mu.Lock()
		defer s.mu.Unlock()
		state := s.perTaskStateLocked(desc)
		if state == nil {
			return nil
		}
		for i, w := range worklist {
			err := groupErrors[i]
			if err != nil {
				state.swapPending[w.groupID] = struct{}{}
				if errors.Is(err, context.Canceled) {
					// Checked first so a discarded debug line spends no sampler budget.
					if logrusext.LevelEnabled(s.logger, logrus.DebugLevel) {
						s.relaunchLogger.WithSampling(func(logrus.FieldLogger) {
							s.loggerWithTask(namespace, desc).WithField("groupID", w.groupID).
								Debugf("swap of a decided group did not complete and is retryable; a later tick re-fires it: %v", err)
						})
					}
				} else {
					s.swapRetryLogger.WithSampling(func(l logrus.FieldLogger) {
						l.WithField("taskID", desc.ID).WithField("groupID", w.groupID).WithField("node", s.localNode).
							Errorf("swap of a decided group failed on this node; retrying it next tick: %v", err)
					})
				}
				continue
			}
			if state.swapAcksPending == nil {
				state.swapAcksPending = map[string]error{}
			}
			state.swapAcksPending[w.groupID] = nil
		}
		if s.ackRecorder == nil {
			return nil
		}
		pending := make([]groupAck, 0, len(state.swapAcksPending))
		for _, groupID := range slices.Sorted(maps.Keys(state.swapAcksPending)) {
			pending = append(pending, groupAck{groupID: groupID, err: state.swapAcksPending[groupID]})
		}
		return pending
	}()

	s.sendGroupAcks(namespace, desc, "swap", acks,
		func(groupID string, success bool, errMsg string) error {
			return s.ackRecorder.RecordDistributedTaskPostCompletionAck(
				context.Background(), namespace, task.ID, task.Version, groupID, s.localNode, success, errMsg)
		},
		func(state *taskSchedulerState) map[string]error { return state.swapAcksPending })
}

// runCompletedCallbackPhase fires OnTaskCompleted for a single task in
// Phase 2 (SWAPPING/FAILED/FINISHED/CANCELLED).
//
// Owns its own lock lifecycle: acquires s.mu to decide eligibility and set
// completedCallbackFired pre-callback, releases for the slow callback, then
// re-acquires to react to its error. [UnitAwareProvider.OnTaskCompleted]
// implementations MUST tolerate being invoked more than once per task —
// see the "Idempotency contract" note at the rollback site.
//
// SWAPPING-path error handling (weaviate/0-weaviate-issues#297): a non-nil
// error withholds FINISHED and retries next tick; [ErrTaskCompletionPermanent]
// or exhausting [maxCompletedCallbackAttempts] fails the task instead.
// Terminal-status errors are best-effort cleanup and never reopen the task.
func (s *Scheduler) runCompletedCallbackPhase(
	namespace string,
	desc TaskDescriptor,
	task *Task,
	suProvider UnitAwareProvider,
) {
	readyForFinalize := task.Status == TaskStatusSwapping ||
		task.Status == TaskStatusFailed ||
		task.Status == TaskStatusFinished ||
		task.Status == TaskStatusCancelled
	if !readyForFinalize {
		return
	}
	if s.ackRecorder != nil && task.Status == TaskStatusSwapping {
		if task.NeedsPreparationBarrier && len(task.SwapsOwed) > 0 {
			return
		}
		if !task.NeedsPreparationBarrier && len(task.MissingPostCompletionAckNodes()) > 0 {
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

	// Fire OnTaskCompleted without the lock. See the "Idempotency contract"
	// note at the matching rollback site in runFinalizePhase.
	cbErr := suProvider.OnTaskCompleted(task)
	if cbErr == nil {
		return
	}

	// Only the SWAPPING cutover retries/fails; a terminal-status error is
	// best-effort cleanup — leave the fired mark set so it doesn't re-fire.
	if task.Status != TaskStatusSwapping {
		s.loggerWithTask(namespace, desc).
			Warnf("OnTaskCompleted returned an error on terminal status %s; ignoring (best-effort cleanup): %v", task.Status, cbErr)
		return
	}

	// Clear the fired mark so the next tick re-fires; a permanent error fails
	// now, a transient one only once the retry budget is exhausted.
	failMsg := func() string {
		s.mu.Lock()
		defer s.mu.Unlock()
		state := s.perTaskStateLocked(desc)
		if state == nil {
			// Local cleanup deleted the entry between callback and commit —
			// nothing to record or retry.
			return ""
		}
		state.completedCallbackFired = false
		state.completedCallbackAttempts++
		permanent := errors.Is(cbErr, ErrTaskCompletionPermanent)
		if permanent || state.completedCallbackAttempts >= maxCompletedCallbackAttempts {
			return cbErr.Error()
		}
		s.loggerWithTask(namespace, desc).
			Warnf("OnTaskCompleted failed (attempt %d/%d); withholding finalize, will retry next tick: %v",
				state.completedCallbackAttempts, maxCompletedCallbackAttempts, cbErr)
		return ""
	}()
	if failMsg == "" {
		return
	}

	// Fired mark stays cleared so a failed MarkDistributedTaskFailed call
	// retries next tick (idempotent at the FSM layer).
	if s.taskFinalizer == nil {
		return
	}
	if err := s.taskFinalizer.MarkDistributedTaskFailed(
		context.Background(), namespace, task.ID, task.Version, failMsg,
	); err != nil {
		s.loggerWithTask(namespace, desc).
			Warnf("failed to mark distributed task failed after OnTaskCompleted error; will retry on next tick: %v", err)
		return
	}
	s.loggerWithTask(namespace, desc).
		Errorf("distributed task marked FAILED, or FINISHED if its cutover write landed: OnTaskCompleted did not succeed: %s", failMsg)
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
		finErrors[i] = s.taskFinalizer.MarkDistributedTaskFinalized(
			context.Background(), namespace, w.task.ID, w.task.Version,
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
		// Clearing completedCallbackFired re-fires OnTaskCompleted next tick;
		// safe now that [UnitAwareProvider.OnTaskCompleted] requires idempotency.
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

func (s *Scheduler) loggerWithTask(namespace string, taskDesc TaskDescriptor) *logrus.Entry {
	return s.logger.WithFields(logrus.Fields{
		"namespace":   namespace,
		"taskID":      taskDesc.ID,
		"taskVersion": taskDesc.Version,
	})
}

func (s *Scheduler) allLocalGroupsReturnedLocked(task *Task, desc TaskDescriptor) bool {
	state := s.perTaskStateLocked(desc)
	for _, groupID := range task.Groups() {
		if !task.groupHasUnitOn(groupID, s.localNode) {
			continue
		}
		if state == nil {
			return false
		}
		if _, returned := state.postCompletionGroupErrors[groupID]; !returned {
			return false
		}
	}
	return true
}

// maxReportedGroupErrors bounds the ack error (stored in the RAFT log and
// snapshots) so it can't grow with the tenant count.
const maxReportedGroupErrors = 10

// joinGroupErrors returns false and the failing groups' errors when any group
// failed. Sorting keeps the reported subset the same on every call.
func (s *Scheduler) joinGroupErrors(errs map[string]error) (bool, string) {
	ec := errorcompounder.New()
	for _, groupID := range slices.Sorted(maps.Keys(errs)) {
		if err := errs[groupID]; err != nil {
			ec.Addf("group=%q: {%v}", groupID, err)
		}
	}
	if ec.Empty() {
		return true, ""
	}
	return false, fmt.Sprintf("%d group(s) failed: %v", ec.Len(), ec.ToErrorLimited(maxReportedGroupErrors))
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

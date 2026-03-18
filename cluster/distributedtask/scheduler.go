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
	"fmt"
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
	clock              clockwork.Clock

	localNode        string
	completedTaskTTL time.Duration
	tickInterval     time.Duration

	logger        logrus.FieldLogger
	sampledLogger *logrusext.Sampler

	tasksRunning *prometheus.GaugeVec

	completedCallbackFired map[TaskDescriptor]bool
	groupCallbackFired     map[TaskDescriptor]map[string]bool

	stopCh chan struct{}
}

type SchedulerParams struct {
	CompletionRecorder TaskCompletionRecorder
	TasksLister        TasksLister
	TaskCleaner        TaskCleaner
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

		providers:              params.Providers,
		completionRecorder:     params.CompletionRecorder,
		completedCallbackFired: map[TaskDescriptor]bool{},
		groupCallbackFired:     map[TaskDescriptor]map[string]bool{},
		tasksLister:            params.TasksLister,
		taskCleaner:            params.TaskCleaner,
		clock:                  params.Clock,

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
	}
}

// Start wires up providers with a [ThrottledRecorder], performs an initial task listing to
// bootstrap any already-active tasks, and spawns the background tick loop. It is safe to call
// exactly once. Use [Scheduler.Close] to stop the loop and terminate all running tasks.
func (s *Scheduler) Start(ctx context.Context) error {
	throttledRecorder := NewThrottledRecorder(s.completionRecorder, 30*time.Second, s.clock)

	s.setCompletionRecorders(throttledRecorder)

	// Attempt an initial task listing to bootstrap running tasks. If it fails
	// (e.g. Raft not ready yet), log and continue — tick() will pick tasks up
	// once the cluster is ready.
	tasksByNamespace, err := s.listTasks(ctx)
	if err != nil {
		s.logger.WithError(err).Warn("initial distributed task listing failed, scheduler will retry on next tick")
	} else {
		s.bootstrapProviders(tasksByNamespace)
	}

	enterrors.GoWrapper(s.loop, s.logger)

	return nil
}

// bootstrapProviders cleans up stale local tasks and starts tasks that are currently active,
// based on the initial task listing from the Raft log.
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
		return task.NodeHasNonTerminalSubUnits(s.localNode)
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

		// Fire group-level and task-level callbacks for sub-unit-aware providers.
		// OnGroupCompleted fires per-group as each group's sub-units all reach terminal
		// state (can fire mid-flight while task is still STARTED).
		// OnTaskCompleted fires once when the task reaches terminal state.
		if suProvider, ok := provider.(SubUnitAwareProvider); ok {
			for desc, task := range tasks {
				if task.Status == TaskStatusCancelled {
					continue
				}

				// Phase 1: per-group finalization (fires mid-flight as groups complete).
				// A group is ready to finalize when either:
				//   - All sub-units in the group are terminal (normal completion), OR
				//   - The task itself is terminal (fail-fast: remaining sub-units won't complete)
				taskTerminal := task.Status == TaskStatusFinished || task.Status == TaskStatusFailed
				for _, groupID := range task.Groups() {
					if s.groupCallbackFired[desc] != nil && s.groupCallbackFired[desc][groupID] {
						continue
					}
					if !taskTerminal && !task.AllGroupSubUnitsTerminal(groupID) {
						continue
					}
					if s.groupCallbackFired[desc] == nil {
						s.groupCallbackFired[desc] = map[string]bool{}
					}
					s.groupCallbackFired[desc][groupID] = true
					localIDs := task.LocalGroupSubUnitIDs(groupID, s.localNode)
					if len(localIDs) > 0 {
						suProvider.OnGroupCompleted(task, groupID, localIDs)
					}
				}

				// Phase 2: global task completion (fires once when task is terminal)
				if taskTerminal && !s.completedCallbackFired[desc] {
					s.completedCallbackFired[desc] = true
					suProvider.OnTaskCompleted(task)
				}
			}
		}

		// Check that all tasks that are already finished and if their TTL has passed, so we can clean them up.
		cleanableTasks := filterTasks(tasks, func(task *Task) bool {
			return task.Status != TaskStatusStarted && s.completedTaskTTL <= s.clock.Since(task.FinishedAt)
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

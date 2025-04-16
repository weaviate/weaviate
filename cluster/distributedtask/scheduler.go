//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package distributedtask

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/logrusext"
)

// TODO: add an endpoint to purge a task if it causes problems?
// TODO: expose the task list
// TODO: add jitter to the scheduler
// TODO: add observability

// TODO: add comment on Scheduler and Manager
type Scheduler struct {
	// is accessed only from background goroutine, therefore, no synchronization, TODO: so why there is a mutex, m? :D
	mu           sync.Mutex // TODO: consider removing this once I add metrics, we can wait on those
	runningTasks map[string]map[string]TaskHandle

	providers          map[string]Provider // namespace -> Provider
	completionRecorder TaskStatusChanger
	tasksLister        TasksLister
	clock              clockwork.Clock

	localNode        string
	completedTaskTTL time.Duration
	tickDuration     time.Duration

	logger        logrus.FieldLogger
	sampledLogger *logrusext.Sampler

	stopCh chan struct{}
}

type SchedulerParams struct {
	CompletionRecorder TaskStatusChanger
	TasksLister        TasksLister
	Providers          map[string]Provider
	Clock              clockwork.Clock
	Logger             logrus.FieldLogger
	MetricsRegisterer  prometheus.Registerer

	LocalNode        string
	CompletedTaskTTL time.Duration
	TickDuration     time.Duration
}

func NewScheduler(params SchedulerParams) *Scheduler {
	if params.Clock == nil {
		params.Clock = clockwork.NewRealClock()
	}

	return &Scheduler{
		runningTasks: map[string]map[string]TaskHandle{},

		providers:          params.Providers,
		completionRecorder: params.CompletionRecorder,
		tasksLister:        params.TasksLister,
		clock:              params.Clock,

		localNode:        params.LocalNode,
		completedTaskTTL: params.CompletedTaskTTL,
		tickDuration:     params.TickDuration,

		logger:        params.Logger,
		sampledLogger: logrusext.NewSampler(params.Logger, 5, 5*params.TickDuration),

		stopCh: make(chan struct{}),
	}
}

func (s *Scheduler) Start() error {
	tasksByNamespace := s.tasksLister.ListTasks()

	s.mu.Lock()
	defer s.mu.Unlock()
	for namespace, provider := range s.providers {
		tasks := tasksByNamespace[namespace]

		provider.SetCompletionRecorder(s.completionRecorder)

		runnableTasks := s.filterRunnableTasks(tasks)

		localTaskDesc := provider.GetLocalTaskIDs()
		for _, taskDesc := range localTaskDesc {
			if _, ok := runnableTasks[taskDesc]; ok {
				continue
			}

			if err := provider.CleanupTask(taskDesc); err != nil {
				s.logger.WithFields(logrus.Fields{
					"namespace":   namespace,
					"taskID":      taskDesc.ID,
					"taskVersion": taskDesc.Version,
					"error":       err,
				}).Error("failed to clean up local distributed task state")
				continue
			}

			s.logger.WithFields(logrus.Fields{
				"namespace":   namespace,
				"taskID":      taskDesc.ID,
				"taskVersion": taskDesc.Version,
			}).Info("cleaned up local distributed task state")
		}

		for desc, task := range runnableTasks {
			handle, err := provider.StartTask(task)
			if err != nil {
				return errors.Wrapf(err, "provider %s start task %v", namespace, desc)
			}

			s.setRunningTaskHandleWithLock(namespace, desc.ID, handle)
			s.logger.WithFields(logrus.Fields{
				"namespace":   namespace,
				"taskID":      desc.ID,
				"taskVersion": desc.Version,
			}).Info("started distributed task execution")
		}
	}

	go s.loop()

	return nil
}

func (s *Scheduler) filterRunnableTasks(tasks []*Task) map[TaskDescriptor]*Task {
	return filterTasks(tasks, func(task *Task) bool {
		return task.Status == TaskStatusStarted && task.FinishedNodes[s.localNode] == false
	})
}

func filterTasks(tasks []*Task, predicate func(task *Task) bool) map[TaskDescriptor]*Task {
	filtered := map[TaskDescriptor]*Task{}
	for _, task := range tasks {
		if !predicate(task) {
			continue
		}
		filtered[TaskDescriptor{ID: task.ID, Version: task.Version}] = task
	}
	return filtered
}

func (s *Scheduler) loop() {
	ticker := s.clock.NewTicker(s.tickDuration)
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
	tasksByNamespace := s.tasksLister.ListTasks()

	s.mu.Lock()
	defer s.mu.Unlock()

	for namespace, provider := range s.providers {
		tasks := tasksByNamespace[namespace]

		// Phase #1: check all tasks that are supposed to be running
		// and launch if they aren't.
		activeTasks := s.filterRunnableTasks(tasks)
		for _, activeTask := range activeTasks {
			if _, alreadyLaunched := s.runningTasks[namespace][activeTask.ID]; alreadyLaunched {
				continue
			}

			handle, err := provider.StartTask(activeTask)
			if err != nil {
				s.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
					l.WithFields(logrus.Fields{
						"namespace":   namespace,
						"taskID":      activeTask.ID,
						"taskVersion": activeTask.Version,
						"error":       err,
					}).Error("failed to start distributed task")
				})
				continue
			}

			s.setRunningTaskHandleWithLock(namespace, activeTask.ID, handle)
			s.logger.WithFields(logrus.Fields{
				"namespace":   namespace,
				"taskID":      activeTask.ID,
				"taskVersion": activeTask.Version,
			}).Info("started distributed task execution")
		}

		// Phase #2: check all tasks that are not supposed to be running,
		// and terminate them if they are.
		finishedTasks := filterTasks(tasks, func(task *Task) bool {
			return task.Status != TaskStatusStarted || task.FinishedNodes[s.localNode] == true
		})
		for _, finishedTask := range finishedTasks {
			handle, ok := s.runningTasks[namespace][finishedTask.ID]
			if !ok {
				continue
			}
			handle.Terminate()
			delete(s.runningTasks[namespace], finishedTask.ID)

			s.logger.WithFields(logrus.Fields{
				"namespace":   namespace,
				"taskID":      finishedTask.ID,
				"taskVersion": finishedTask.Version,
			}).Info("terminated distributed task execution")
		}

		// Phase #3: check all tasks that are already finished and if their TTL has passed, so we can
		// clean them up.
		cleanableTasks := filterTasks(tasks, func(task *Task) bool {
			return task.Status != TaskStatusStarted && s.completedTaskTTL <= s.clock.Since(task.FinishedAt)
		})
		for _, task := range cleanableTasks {
			err := s.completionRecorder.CleanUpDistributedTask(context.Background(), namespace, task.ID, task.Version)
			if err != nil {
				s.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
					l.WithFields(logrus.Fields{
						"namespace":   namespace,
						"taskID":      task.ID,
						"taskVersion": task.Version,
						"error":       err,
					}).Error("failed to clean up distributed task")
				})
				continue
			}

			s.logger.WithFields(logrus.Fields{
				"namespace":   namespace,
				"taskID":      task.ID,
				"taskVersion": task.Version,
			}).Info("successfully submitted request to clean up distributed task")
		}
	}
}

func (s *Scheduler) setRunningTaskHandleWithLock(namespace string, taskID string, handle TaskHandle) {
	if _, ok := s.runningTasks[namespace]; !ok {
		s.runningTasks[namespace] = map[string]TaskHandle{}
	}
	s.runningTasks[namespace][taskID] = handle
}

func (s *Scheduler) Close() {
	close(s.stopCh)
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

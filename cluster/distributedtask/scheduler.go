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
)

type Scheduler struct {
	// is accessed only from background goroutine, therefore, no synchronization
	mu           sync.Mutex // TODO: consider removing this once I add metrics, we can wait on those
	runningTasks map[string]map[string]TaskHandle

	providers          map[string]Provider // namespace -> Provider
	completionRecorder TaskStatusChanger
	tasksLister        TasksLister
	clock              clockwork.Clock

	localNode        string
	completedTaskTTL time.Duration
	tickDuration     time.Duration

	stopCh chan struct{}
}

type SchedulerParams struct {
	CompletionRecorder TaskStatusChanger
	TasksLister        TasksLister
	Providers          map[string]Provider
	Clock              clockwork.Clock

	LocalNode        string
	CompletedTaskTTL time.Duration
	TickDuration     time.Duration
}

// TODO: add observability

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
				return errors.Wrapf(err, "provider %s cleanup task %v", namespace, taskDesc)
			}
		}

		for desc, task := range runnableTasks {
			handle, err := provider.StartTask(task)
			if err != nil {
				return errors.Wrapf(err, "provider %s start task %v", namespace, desc)
			}

			s.setRunningTaskHandle(namespace, desc.ID, handle)
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

		// 1. collect tasks that are supposed to be running
		activeTasks := s.filterRunnableTasks(tasks)
		// compare that to tasks that are already running and launch if anything is missing
		for _, activeTask := range activeTasks {
			if _, alreadyLaunched := s.runningTasks[namespace][activeTask.ID]; alreadyLaunched {
				continue
			}

			handle, err := provider.StartTask(activeTask)
			if err != nil {
				// TODO: think what to do here
				continue
			}

			s.setRunningTaskHandle(namespace, activeTask.ID, handle)
		}

		// 2. collect tasks that should not be running
		finishedTasks := filterTasks(tasks, func(task *Task) bool {
			return task.Status != TaskStatusStarted || task.FinishedNodes[s.localNode] == true
		})

		// compare that to tasks that are running, cancel them and remove from the list.
		for _, finishedTask := range finishedTasks {
			handle, ok := s.runningTasks[namespace][finishedTask.ID]
			if !ok {
				continue
			}
			handle.Terminate()
			delete(s.runningTasks[namespace], finishedTask.ID)
		}
		// Cancelled task has a responsibility to clean up after itself in the same way as it would clean up
		// if completed successfully

		// 3. for tasks are not running for a long time, send a cleanup request
		cleanableTasks := filterTasks(tasks, func(task *Task) bool {
			return task.Status != TaskStatusStarted && s.completedTaskTTL <= s.clock.Since(task.FinishedAt)
		})
		for _, task := range cleanableTasks {
			err := s.completionRecorder.CleanUpDistributedTask(context.Background(), namespace, task.ID, task.Version)
			if err != nil { // TODO: check the error
				// TODO: log?
				continue
			}
		}
	}
}

func (s *Scheduler) setRunningTaskHandle(namespace string, taskID string, handle TaskHandle) {
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

package distributedtask

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/pkg/errors"
)

type TaskStatusChanger interface {
	RecordDistributedTaskNodeCompletion(ctx context.Context, taskType, taskID string, version uint64) error
	CleanUpDistributedTask(ctx context.Context, taskType, taskID string, taskVersion uint64) error
}

type TaskListProvider interface {
	TaskList(taskType string) ([]*Task, error)
}

type TaskHandle interface {
	Terminate()
}

type Provider interface {
	GetLocalTaskIDs() []TaskDescriptor
	CleanupTask(desc TaskDescriptor) error
	StartTask(task *Task) (TaskHandle, error)
}

type Scheduler struct {
	SchedulerParams

	runningTasks map[string]map[string]TaskHandle

	stopCh chan struct{}
}

type SchedulerParams struct {
	CompletionRecorder TaskStatusChanger
	TasksManager       *Manager
	Providers          map[string]Provider
	Clock              clockwork.Clock

	LocalNode string
}

func NewScheduler(params SchedulerParams) *Scheduler {
	if params.Clock == nil {
		params.Clock = clockwork.NewRealClock()
	}

	return &Scheduler{
		SchedulerParams: params,
	}
}

func (s *Scheduler) Start() error {
	for namespace, provider := range s.Providers {
		activeTasks := s.tasksThatShouldBeRunning(namespace)

		localTaskDesc := provider.GetLocalTaskIDs()
		for _, taskDesc := range localTaskDesc {
			if _, ok := activeTasks[taskDesc]; ok {
				handle, err := provider.StartTask(activeTasks[taskDesc])
				if err != nil {
					return errors.Wrapf(err, "provider %s start task %v", namespace, taskDesc)
				}

				s.runningTasks[namespace][taskDesc.ID] = handle
				continue
			}

			if err := provider.CleanupTask(taskDesc); err != nil {
				return errors.Wrapf(err, "provider %s cleanup task %v", namespace, taskDesc)
			}
		}
	}

	go s.loop()

	return nil
}

func (s *Scheduler) tasksThatShouldBeRunning(namespace string) map[TaskDescriptor]*Task {
	activeTasks := map[TaskDescriptor]*Task{}
	for _, task := range s.TasksManager.tasks[namespace] {
		if task.Status == TaskStatusStarted && !task.FinishedNodes[s.LocalNode] {
			activeTasks[TaskDescriptor{
				ID:      task.ID,
				Version: task.Version,
			}] = task
		}
	}
	return activeTasks
}

func (s *Scheduler) finishedTasks(namespace string) map[TaskDescriptor]*Task {
	// TODO: add implementation
	//activeTasks := map[TaskDescriptor]*Task{}
	//for _, task := range s.TasksManager.tasks[namespace] {
	//	if task.Status == TaskStatusStarted && !task.FinishedNodes[s.LocalNode] {
	//		activeTasks[TaskDescriptor{
	//			ID:      task.ID,
	//			Version: task.Version,
	//		}] = task
	//	}
	//}
	//return activeTasks
	return nil
}

func (s *Scheduler) loop() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			s.process()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Scheduler) process() {
	for namespace, provider := range s.Providers {
		// 1. collect tasks that are supposed to be running
		activeTasks := s.tasksThatShouldBeRunning(namespace)
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
			s.runningTasks[namespace][activeTask.ID] = handle
		}

		// 2. collect tasks that should not be running
		finishedTasks := s.finishedTasks(namespace)
		// compare that to tasks that are running, cancel them and remove from the list.
		for _, finishedTask := range finishedTasks {
			handle, ok := s.runningTasks[namespace][finishedTask.ID]
			if !ok {
				continue
			}
			handle.Terminate()
		}
		// Cancelled task has a responsibility to clean up after itself in the same way as it would clean up
		// if completed successfully

		// 3. for tasks are not running for a long time, send a cleanup request
		for _, finishedTask := range finishedTasks {
			err := s.CompletionRecorder.CleanUpDistributedTask(context.Background(), namespace, finishedTask.ID, finishedTask.Version)
			if err != nil { // TODO: check the error

			}
		}
	}
}

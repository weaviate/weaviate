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

package queue

import (
	"context"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type Scheduler struct {
	SchedulerOptions

	queues struct {
		sync.Mutex

		m map[string]*queueState
	}

	// context used to close pending tasks
	ctx      context.Context
	cancelFn context.CancelFunc

	activeTasks *common.SharedGauge

	wg        sync.WaitGroup
	chans     []chan *Batch
	triggerCh chan chan struct{}
}

type SchedulerOptions struct {
	Logger logrus.FieldLogger
	// Number of workers to process tasks. Defaults to the number of CPUs - 1.
	Workers int
	// The interval at which the scheduler checks the queues for tasks. Defaults to 1 second.
	ScheduleInterval time.Duration
	// The interval between retries for failed tasks.
	RetryInterval time.Duration
	// MaxScheduleTime limits how long the scheduler drains queues per tick before
	// yielding back to allow concurrent read operations (e.g. vector searches) to
	// acquire index locks. Defaults to half of ScheduleInterval.
	// Can be overridden via the QUEUE_SCHEDULER_MAX_SCHEDULE_TIME environment variable.
	MaxScheduleTime time.Duration
	// Function to be called when the scheduler is closed
	OnClose func()
}

func NewScheduler(opts SchedulerOptions) *Scheduler {
	var err error

	if opts.Logger == nil {
		opts.Logger = logrus.New()
	}
	opts.Logger = opts.Logger.WithField("component", "queue-scheduler")

	if opts.Workers <= 0 {
		opts.Workers = max(1, runtime.GOMAXPROCS(0)-1)
	}

	if opts.ScheduleInterval == 0 {
		var it time.Duration
		v := os.Getenv("QUEUE_SCHEDULER_INTERVAL")

		if v != "" {
			it, err = time.ParseDuration(v)
			if err != nil {
				opts.Logger.WithError(err).WithField("value", v).Warn("failed to parse QUEUE_SCHEDULER_INTERVAL, using default")
			}
		}

		if it == 0 {
			it = 1 * time.Second
		}
		opts.ScheduleInterval = it
	}

	if opts.RetryInterval == 0 {
		var ri time.Duration
		v := os.Getenv("QUEUE_RETRY_INTERVAL")

		if v != "" {
			ri, err = time.ParseDuration(v)
			if err != nil {
				opts.Logger.WithError(err).WithField("value", v).Warn("failed to parse QUEUE_RETRY_INTERVAL, using default")
			}
		}

		if ri == 0 {
			ri = 5 * time.Second
		}
		opts.RetryInterval = ri
	}

	if opts.MaxScheduleTime <= 0 {
		var mt time.Duration

		if opts.MaxScheduleTime < 0 {
			opts.Logger.WithField("value", opts.MaxScheduleTime).Warn("MaxScheduleTime must be positive, using default")
		}

		v := os.Getenv("QUEUE_SCHEDULER_MAX_SCHEDULE_TIME")

		if v != "" {
			mt, err = time.ParseDuration(v)
			if err != nil {
				opts.Logger.WithError(err).WithField("value", v).Warn("failed to parse QUEUE_SCHEDULER_MAX_SCHEDULE_TIME, using default")
			} else if mt <= 0 {
				opts.Logger.WithField("value", v).Warn("QUEUE_SCHEDULER_MAX_SCHEDULE_TIME must be positive, using default")
				mt = 0
			}
		}

		if mt == 0 {
			mt = opts.ScheduleInterval / 2
		}
		opts.MaxScheduleTime = mt
	}

	s := Scheduler{
		SchedulerOptions: opts,
		activeTasks:      common.NewSharedGauge(),
	}
	s.queues.m = make(map[string]*queueState)
	s.triggerCh = make(chan chan struct{}, 1)

	return &s
}

func (s *Scheduler) RegisterQueue(q Queue) {
	if s.ctx == nil {
		// scheduler not started
		return
	}

	s.queues.Lock()
	defer s.queues.Unlock()

	s.queues.m[q.ID()] = newQueueState(s.ctx, q)

	q.Metrics().Registered(q.ID())
}

func (s *Scheduler) UnregisterQueue(id string) {
	if s.ctx == nil {
		// scheduler not started
		return
	}

	q := s.getQueue(id)
	if q == nil {
		return
	}

	s.PauseQueue(id)

	q.cancelFn()

	// wait for the workers to finish processing the queue's tasks
	s.Wait(id)

	// the queue is paused, so it's safe to remove it
	s.queues.Lock()
	delete(s.queues.m, id)
	s.queues.Unlock()

	q.q.Metrics().Unregistered(q.q.ID())
}

func (s *Scheduler) Start() {
	if s.ctx != nil {
		// scheduler already started
		return
	}

	s.ctx, s.cancelFn = context.WithCancel(context.Background())

	// run workers
	chans := make([]chan *Batch, s.Workers)

	for i := 0; i < s.Workers; i++ {
		worker, ch := NewWorker(s.Logger, s.RetryInterval)
		chans[i] = ch

		s.wg.Add(1)
		f := func() {
			defer s.wg.Done()

			worker.Run(s.ctx)
		}
		enterrors.GoWrapper(f, s.Logger)
	}

	s.chans = chans

	// run scheduler goroutine
	s.wg.Add(1)
	f := func() {
		defer s.wg.Done()

		s.runScheduler()
	}
	enterrors.GoWrapper(f, s.Logger)
}

func (s *Scheduler) Close() error {
	if s == nil || s.ctx == nil {
		// scheduler not initialized. No op.
		return nil
	}

	// check if the scheduler is already closed
	if s.ctx.Err() != nil {
		return nil
	}

	// stop scheduling
	s.cancelFn()

	// wait for the workers to finish processing tasks
	s.activeTasks.Wait()

	// wait for the spawned goroutines to stop
	s.wg.Wait()

	// close the channels
	for _, ch := range s.chans {
		close(ch)
	}

	s.Logger.Debug("scheduler closed")

	if s.OnClose != nil {
		s.OnClose()
	}

	return nil
}

func (s *Scheduler) PauseQueue(id string) {
	if s.ctx == nil {
		// scheduler not started
		return
	}

	q := s.getQueue(id)
	if q == nil {
		return
	}

	q.m.Lock()
	q.paused = true
	q.m.Unlock()

	s.Logger.WithField("id", id).Debug("queue paused")
}

// IsQueuePaused returns true if the queue is paused.
func (s *Scheduler) IsQueuePaused(id string) bool {
	if s.ctx == nil {
		// scheduler not started
		return false
	}

	q := s.getQueue(id)
	if q == nil {
		return false
	}

	return q.Paused()
}

func (s *Scheduler) ResumeQueue(id string) {
	if s.ctx == nil {
		// scheduler not started
		return
	}

	q := s.getQueue(id)
	if q == nil {
		return
	}

	q.m.Lock()
	q.paused = false
	q.m.Unlock()

	s.Logger.WithField("id", id).Debug("queue resumed")
}

func (s *Scheduler) Wait(id string) {
	if s.ctx == nil {
		// scheduler not started
		return
	}

	q := s.getQueue(id)
	if q == nil {
		return
	}

	q.scheduled.Wait()
	q.activeTasks.Wait()
}

func (s *Scheduler) WaitAll() {
	if s.ctx == nil {
		// scheduler not started
		return
	}

	s.activeTasks.Wait()
}

func (s *Scheduler) getQueue(id string) *queueState {
	s.queues.Lock()
	defer s.queues.Unlock()

	return s.queues.m[id]
}

func (s *Scheduler) runScheduler() {
	t := time.NewTicker(s.ScheduleInterval)

	for {
		select {
		case <-s.ctx.Done():
			// stop the ticker
			t.Stop()
			return
		case <-t.C:
			s.schedule()
		case ch := <-s.triggerCh:
			s.schedule()
			close(ch)
		}
	}
}

// Manually schedule the queues.
func (s *Scheduler) Schedule(ctx context.Context) {
	if s.ctx == nil {
		// scheduler not started
		return
	}

	ch := make(chan struct{})
	select {
	case s.triggerCh <- ch:
		select {
		case <-ch:
		case <-ctx.Done():
		}
	default:
	}
}

// triggerSchedule signals the scheduler to run schedule() immediately.
// It is non-blocking: if the scheduler is busy or a trigger is already pending,
// the signal is dropped (the current or pending run will cover the work).
func (s *Scheduler) triggerSchedule() {
	ch := make(chan struct{})
	select {
	case s.triggerCh <- ch:
	default:
		close(ch)
	}
}

func (s *Scheduler) schedule() {
	deadline := time.Now().Add(s.MaxScheduleTime)
	for {
		if s.ctx.Err() != nil {
			return
		}

		if nothingScheduled := s.scheduleQueues(); nothingScheduled {
			return
		}

		if time.Now().After(deadline) {
			return
		}
	}
}

func (s *Scheduler) scheduleQueues() (nothingScheduled bool) {
	// loop over the queues in random order
	s.queues.Lock()
	ids := make([]string, 0, len(s.queues.m))
	for id := range s.queues.m {
		ids = append(ids, id)
	}
	s.queues.Unlock()

	nothingScheduled = true

	for _, id := range ids {
		if s.ctx.Err() != nil {
			return nothingScheduled
		}

		q := s.getQueue(id)
		if q == nil {
			continue
		}

		// mark it as scheduled
		q.MarkAsScheduled()

		if q.Paused() {
			q.MarkAsUnscheduled()
			continue
		}

		// run the before-schedule hook if it is implemented
		if hook, ok := q.q.(BeforeScheduleHook); ok {
			if skip := hook.BeforeSchedule(); skip {
				q.MarkAsUnscheduled()
				continue
			}
		}

		if q.q.Size() == 0 {
			q.MarkAsUnscheduled()
			continue
		}

		count, err := s.dispatchQueue(q)
		if err != nil {
			s.Logger.WithError(err).WithField("id", id).Error("failed to schedule queue")
		}

		q.MarkAsUnscheduled()

		nothingScheduled = count <= 0
	}

	return nothingScheduled
}

func (s *Scheduler) dispatchQueue(q *queueState) (int64, error) {
	if q.ctx.Err() != nil {
		return 0, nil
	}

	batch, err := q.q.DequeueBatch()
	if err != nil {
		return 0, errors.Wrap(err, "failed to dequeue batch")
	}
	if batch == nil || len(batch.Tasks) == 0 {
		return 0, nil
	}

	partitions := make([][]Task, s.Workers)

	var taskCount int64
	for _, t := range batch.Tasks {
		// TODO: introduce other partitioning strategies if needed
		slot := t.Key() % uint64(s.Workers)
		partitions[slot] = append(partitions[slot], t)
		taskCount++
	}

	// compress the tasks before sending them to the workers
	// i.e. group consecutive tasks with the same operation as a single task
	for i := range partitions {
		partitions[i] = s.compressTasks(partitions[i])
	}

	// keep track of the number of active tasks
	// for this chunk to remove it when all tasks are done
	var counter int
	for i, partition := range partitions {
		if len(partition) == 0 {
			continue
		}
		q.m.Lock()
		counter++
		q.m.Unlock()

		// increment the global active tasks counter
		s.activeTasks.Incr()
		// increment the queue's active tasks counter
		q.activeTasks.Incr()

		start := time.Now()

		select {
		case <-s.ctx.Done():
			s.activeTasks.Decr()
			q.activeTasks.Decr()
			return taskCount, nil
		case <-q.ctx.Done():
			s.activeTasks.Decr()
			q.activeTasks.Decr()
			return taskCount, nil
		case s.chans[i] <- &Batch{
			Tasks: partitions[i],
			Ctx:   q.ctx,
			onDone: func() {
				defer q.q.Metrics().TasksProcessed(start, int(taskCount))
				defer q.activeTasks.Decr()
				defer s.activeTasks.Decr()

				q.m.Lock()
				counter--
				c := counter
				q.m.Unlock()
				if c == 0 {
					// It is important to unlock the queue here
					// to avoid a deadlock when the last worker calls Done.
					batch.Done()
					s.Logger.
						WithField("queue_id", q.q.ID()).
						WithField("queue_size", q.q.Size()).
						WithField("count", taskCount).
						Debug("tasks processed")
					// Signal the scheduler to pick up more work immediately
					// instead of waiting for the next tick. This restores
					// event-driven scheduling: workers stay continuously busy
					// while work is available, with MaxScheduleTime still
					// acting as the yield limit per scheduling burst.
					s.triggerSchedule()
				}
			},
			onCanceled: func() {
				q.activeTasks.Decr()
				s.activeTasks.Decr()
			},
		}:
		}
	}

	s.logQueueStats(q.q, taskCount)

	return taskCount, nil
}

func (s *Scheduler) logQueueStats(q Queue, tasksDequeued int64) {
	s.Logger.
		WithField("queue_id", q.ID()).
		WithField("queue_size", q.Size()).
		WithField("count", tasksDequeued).
		Debug("processing tasks")
}

func (s *Scheduler) compressTasks(tasks []Task) []Task {
	if len(tasks) == 0 {
		return tasks
	}
	grouper, ok := tasks[0].(TaskGrouper)
	if !ok {
		return tasks
	}

	var cur uint8
	var group []Task
	var compressed []Task

	for i, t := range tasks {
		if i == 0 {
			cur = t.Op()
			group = append(group, t)
			continue
		}

		if t.Op() == cur {
			group = append(group, t)
			continue
		}

		compressed = append(compressed, grouper.NewGroup(cur, group...))

		cur = t.Op()
		group = []Task{t}
	}

	compressed = append(compressed, grouper.NewGroup(cur, group...))

	return compressed
}

type queueState struct {
	m           sync.RWMutex
	q           Queue
	paused      bool
	scheduled   *common.SharedGauge
	activeTasks *common.SharedGauge
	ctx         context.Context
	cancelFn    context.CancelFunc
}

func newQueueState(ctx context.Context, q Queue) *queueState {
	qs := queueState{
		q:           q,
		scheduled:   common.NewSharedGauge(),
		activeTasks: common.NewSharedGauge(),
	}

	if ctx != nil {
		qs.ctx, qs.cancelFn = context.WithCancel(ctx)
	}

	return &qs
}

func (qs *queueState) Paused() bool {
	qs.m.RLock()
	defer qs.m.RUnlock()

	return qs.paused
}

func (qs *queueState) Scheduled() bool {
	return qs.scheduled.Count() > 0
}

func (qs *queueState) MarkAsScheduled() {
	qs.scheduled.Incr()
}

func (qs *queueState) MarkAsUnscheduled() {
	qs.scheduled.Decr()
}

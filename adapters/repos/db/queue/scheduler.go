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
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/usecases/monitoring"
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

	// closeOnce: OnClose fires exactly once, started or not (owners count it in a shutdown WaitGroup).
	closeOnce sync.Once
	// closeLock serialises concurrent Close calls (double close of chans panics and would skip OnClose).
	closeLock sync.Mutex
}

type SchedulerOptions struct {
	Logger logrus.FieldLogger
	// Number of workers to process tasks. Defaults to the number of CPUs - 1.
	Workers int
	// The interval at which the scheduler checks the queues for tasks. Defaults to 1 second.
	ScheduleInterval time.Duration
	// How long a queue waits before replaying a batch that failed. Defaults to 5 seconds.
	RetryInterval time.Duration
	// Function to be called when the scheduler is closed
	OnClose func()
	// Prometheus metrics. Optional.
	Metrics *monitoring.PrometheusMetrics
}

func NewScheduler(opts SchedulerOptions) *Scheduler {
	if opts.Logger == nil {
		opts.Logger = logrus.New()
	}
	opts.Logger = opts.Logger.WithField("component", "queue-scheduler")

	if opts.Workers <= 0 {
		opts.Workers = max(1, runtime.GOMAXPROCS(0)-1)
	}

	if opts.ScheduleInterval <= 0 {
		opts.ScheduleInterval = intervalFromEnv(opts.Logger, "QUEUE_SCHEDULER_INTERVAL", time.Second)
	}

	if opts.RetryInterval <= 0 {
		opts.RetryInterval = intervalFromEnv(opts.Logger, "QUEUE_RETRY_INTERVAL", 5*time.Second)
	}

	s := Scheduler{
		SchedulerOptions: opts,
		activeTasks:      common.NewSharedGauge(),
	}
	s.queues.m = make(map[string]*queueState)
	s.triggerCh = make(chan chan struct{}, 1)

	return &s
}

// intervalFromEnv returns the duration set in the given environment variable,
// or def if it is unset, invalid or not positive.
func intervalFromEnv(logger logrus.FieldLogger, name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}

	d, err := time.ParseDuration(v)
	if err != nil {
		logger.WithField("value", v).Warnf("failed to parse %s, using default: %v", name, err)
		return def
	}
	if d <= 0 {
		logger.WithField("value", v).Warnf("%s must be positive, using default", name)
		return def
	}

	return d
}

func (s *Scheduler) RegisterQueue(q Queue) {
	if s.ctx == nil {
		// scheduler not started
		return
	}

	s.queues.Lock()
	defer s.queues.Unlock()

	s.queues.m[q.ID()] = newQueueState(s.ctx, q)

	s.updateQueueCountMetric()
}

func (s *Scheduler) UnregisterQueue(ctx context.Context, id string) {
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
	_ = s.Wait(ctx, id)

	// the queue is paused, so it's safe to remove it
	s.queues.Lock()
	delete(s.queues.m, id)
	s.queues.Unlock()

	s.updateQueueCountMetric()
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
		worker, ch := NewWorker(s.Logger.WithField("worker_id", i))
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

func (s *Scheduler) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.closeLock.Lock()
	defer s.closeLock.Unlock()
	if s.ctx == nil {
		// Never started: still fire OnClose or the owner's shutdown WaitGroup pins forever.
		s.runOnClose()
		return nil
	}

	// check if the scheduler is already closed
	if s.ctx.Err() != nil {
		return nil
	}

	// stop scheduling
	s.cancelFn()

	// wait for the workers to finish processing tasks
	_ = s.activeTasks.Wait(ctx)

	// wait for the spawned goroutines to stop
	s.wg.Wait()

	// close the channels
	for _, ch := range s.chans {
		close(ch)
	}

	s.Logger.Debug("scheduler closed")

	s.runOnClose()

	return nil
}

func (s *Scheduler) runOnClose() {
	if s.OnClose != nil {
		s.closeOnce.Do(s.OnClose)
	}
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

	s.updatePausedMetric()

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

	s.updatePausedMetric()

	s.Logger.WithField("id", id).Debug("queue resumed")
}

func (s *Scheduler) updatePausedMetric() {
	if s.Metrics == nil {
		return
	}

	var count int
	s.queues.Lock()
	for _, q := range s.queues.m {
		if q.Paused() {
			count++
		}
	}
	s.queues.Unlock()

	s.Metrics.QueuePaused.Set(float64(count))
}

func (s *Scheduler) updateQueueCountMetric() {
	if s.Metrics == nil {
		return
	}

	s.Metrics.QueueCount.Set(float64(len(s.queues.m)))
}

func (s *Scheduler) QueueCount() int {
	s.queues.Lock()
	defer s.queues.Unlock()

	return len(s.queues.m)
}

func (s *Scheduler) Wait(ctx context.Context, id string) error {
	if s.ctx == nil {
		// scheduler not started
		return nil
	}

	q := s.getQueue(id)
	if q == nil {
		return nil
	}

	if err := q.scheduled.Wait(ctx); err != nil {
		return err
	}
	return q.activeTasks.Wait(ctx)
}

func (s *Scheduler) WaitAll(ctx context.Context) error {
	if s.ctx == nil {
		// scheduler not started
		return nil
	}

	return s.activeTasks.Wait(ctx)
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
			s.scheduleQueues()
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

func (s *Scheduler) triggerSchedule() {
	ch := make(chan struct{})
	select {
	case s.triggerCh <- ch:
	default:
		close(ch)
	}
}

func (s *Scheduler) schedule() {
	// as long as there are tasks to schedule, keep running
	// in a tight loop
	for {
		if s.ctx.Err() != nil {
			return
		}

		if nothingScheduled := s.scheduleQueues(); nothingScheduled {
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

		// skip if already scheduled
		if q.activeTasks.Count() > 0 {
			continue
		}

		// skip if its last batch failed recently
		if q.Retrying() {
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
			// e.g. an I/O error reading the queue: try again later, not on
			// every tick
			q.RetryAfter(s.RetryInterval)
		}

		q.MarkAsUnscheduled()

		if count > 0 {
			nothingScheduled = false
		}
	}

	return nothingScheduled
}

func (s *Scheduler) dispatchQueue(q *queueState) (taskCount int64, err error) {
	// a panic here (e.g. while dequeuing or decoding a corrupt chunk) would
	// kill the scheduler goroutine, which is shared by every queue and never
	// restarted. Contain it so the scheduler moves on to the other queues.
	defer func() {
		if r := recover(); r != nil {
			entsentry.Recover(r)
			enterrors.PrintStack(s.Logger.WithField("queue_id", q.q.ID()))
			err = errors.Errorf("recovered from panic while dispatching queue: %v", r)
		}
	}()

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

	for _, t := range batch.Tasks {
		// TODO: introduce other partitioning strategies if needed
		slot := t.Key() % uint64(s.Workers)
		partitions[slot] = append(partitions[slot], t)
		taskCount++
	}

	// compress the tasks before sending them to the workers
	// i.e. group consecutive tasks with the same operation as a single task
	// e.g. multiple index.Add into a single index.AddBatch
	for i := range partitions {
		partitions[i] = s.compressTasks(partitions[i])
	}

	// the batch is done once every partition has run, and canceled if any
	// of them failed: all are counted before the first one is sent, so an
	// early finisher cannot complete the batch alone.
	var pending atomic.Int32
	for _, partition := range partitions {
		if len(partition) > 0 {
			pending.Add(1)
		}
	}
	var failed atomic.Bool
	complete := func(ok bool) {
		if !ok {
			failed.Store(true)
		}
		if pending.Add(-1) > 0 {
			return
		}

		if failed.Load() {
			// the whole batch is replayed on the queue's next turn, which
			// must not come right away or a failing queue would spin
			q.RetryAfter(s.RetryInterval)
			batch.Cancel()
			return
		}

		batch.Done()
		s.Logger.
			WithField("queue_id", q.q.ID()).
			WithField("queue_size", q.q.Size()).
			WithField("count", taskCount).
			Debug("tasks processed")
	}

	for i, partition := range partitions {
		if len(partition) == 0 {
			continue
		}

		// increment the global active tasks counter
		s.activeTasks.Incr()
		// increment the queue's active tasks counter
		q.activeTasks.Incr()

		start := time.Now()

		// decrement the global and queue active tasks counters, after the
		// batch is completed so the queue is not dequeued before that
		release := func() {
			qTaskCount := q.activeTasks.Decr()
			s.activeTasks.Decr()

			// notify the scheduler to check for more tasks
			if qTaskCount == 0 {
				s.triggerSchedule()
			}
		}

		// prepare the batch for the worker
		wb := Batch{
			Tasks: partitions[i],
			Ctx:   q.ctx,
			OnDone: func() {
				complete(true)
				q.q.Metrics().TasksProcessed(start, int(taskCount))
				release()
			},
			OnCanceled: func() {
				complete(false)
				release()
			},
		}

		err = s.sendToAvailableWorker(q, &wb)
		if err != nil {
			// this partition and the ones not sent yet will never run
			for _, p := range partitions[i:] {
				if len(p) > 0 {
					complete(false)
				}
			}
			s.activeTasks.Decr()
			q.activeTasks.Decr()
			return taskCount, errors.Wrap(err, "failed to send batch to worker")
		}
	}

	s.logQueueStats(q.q, taskCount)

	return taskCount, nil
}

// sendToAvailableWorker tries to send the batch to an available worker channel.
// If no worker is available, it waits and retries until successful or until
// the scheduler or queue context is done.
func (s *Scheduler) sendToAvailableWorker(q *queueState, batch *Batch) error {
	// pick the first available worker channel
	for {
		for _, ch := range s.chans {
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-q.ctx.Done():
				return q.ctx.Err()
			case ch <- batch:
				return nil
			default:
			}
		}

		// wait a bit before retrying
		t := time.NewTimer(100 * time.Millisecond)
		select {
		case <-s.ctx.Done():
			t.Stop()
			return s.ctx.Err()
		case <-q.ctx.Done():
			t.Stop()
			return q.ctx.Err()
		case <-t.C:
		}
	}
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
	retryAfter  time.Time
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

// RetryAfter prevents the queue from being scheduled for the given duration.
func (qs *queueState) RetryAfter(d time.Duration) {
	qs.m.Lock()
	defer qs.m.Unlock()

	qs.retryAfter = time.Now().Add(d)
}

// Retrying returns true while the queue waits to replay a failed batch.
func (qs *queueState) Retrying() bool {
	qs.m.RLock()
	defer qs.m.RUnlock()

	return time.Now().Before(qs.retryAfter)
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

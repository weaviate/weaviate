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

package db

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

type ShardReindexerV3 interface {
	RunBeforeLsmInit(ctx context.Context, shard *Shard) error
	RunAfterLsmInit(ctx context.Context, shard *Shard) error
	RunAfterLsmInitAsync(ctx context.Context, shard *Shard) error
	Stop(shard *Shard, cause error)
}

type ShardReindexTaskV3 interface {
	Name() string
	OnBeforeLsmInit(ctx context.Context, shard *Shard) error
	OnAfterLsmInit(ctx context.Context, shard *Shard) error
	// TODO alisza:blockmax change to *Shard?
	OnAfterLsmInitAsync(ctx context.Context, shard ShardLike) (rerunAt time.Time, reloadShard bool, err error)
}

// -----------------------------------------------------------------------------

func NewShardReindexerV3Noop() *shardReindexerV3Noop {
	return &shardReindexerV3Noop{}
}

type shardReindexerV3Noop struct{}

func (r *shardReindexerV3Noop) RunBeforeLsmInit(ctx context.Context, shard *Shard) error {
	return nil
}

func (r *shardReindexerV3Noop) RunAfterLsmInit(ctx context.Context, shard *Shard) error {
	return nil
}

func (r *shardReindexerV3Noop) RunAfterLsmInitAsync(ctx context.Context, shard *Shard) error {
	return nil
}

func (r *shardReindexerV3Noop) Stop(shard *Shard, cause error) {}

// -----------------------------------------------------------------------------

func NewShardReindexerV3(ctx context.Context, logger logrus.FieldLogger,
	getIndex func(className schema.ClassName) *Index, concurrency int,
) *shardReindexerV3 {
	config := shardsReindexerV3Config{
		concurrency:          concurrency,
		retryOnErrorInterval: 15 * time.Minute,
	}

	logger.WithField("config", fmt.Sprintf("%+v", config)).Debug("reindexer created")

	return &shardReindexerV3{
		logger:                      logger,
		ctx:                         ctx,
		getIndex:                    getIndex,
		queue:                       newShardsQueue(),
		lock:                        new(sync.Mutex),
		taskNames:                   map[string]struct{}{},
		tasks:                       []ShardReindexTaskV3{},
		waitingTasksPerShard:        map[string][]ShardReindexTaskV3{},
		waitingCtxPerShard:          map[string]context.Context{},
		waitingCtxCancelPerShard:    map[string]context.CancelCauseFunc{},
		processingCtxPerShard:       map[string]context.Context{},
		processingCtxCancelPerShard: map[string]context.CancelCauseFunc{},
		config:                      config,
	}
}

type shardReindexerV3 struct {
	logger   logrus.FieldLogger
	ctx      context.Context
	getIndex func(className schema.ClassName) *Index
	queue    *shardsQueue
	lock     *sync.Mutex

	taskNames                   map[string]struct{}
	tasks                       []ShardReindexTaskV3
	waitingTasksPerShard        map[string][]ShardReindexTaskV3
	waitingCtxPerShard          map[string]context.Context
	waitingCtxCancelPerShard    map[string]context.CancelCauseFunc
	processingCtxPerShard       map[string]context.Context
	processingCtxCancelPerShard map[string]context.CancelCauseFunc

	config shardsReindexerV3Config
}

type shardsReindexerV3Config struct {
	concurrency          int
	retryOnErrorInterval time.Duration
}

func (r *shardReindexerV3) RegisterTask(task ShardReindexTaskV3) bool {
	name := task.Name()
	if _, ok := r.taskNames[name]; ok {
		return false
	}

	r.taskNames[name] = struct{}{}
	r.tasks = append(r.tasks, task)
	return true
}

func (r *shardReindexerV3) Init() {
	enterrors.GoWrapper(func() {
		eg := enterrors.NewErrorGroupWrapper(r.logger)
		eg.SetLimit(r.config.concurrency)

		for {
			key, tasks, err := r.queue.getWhenReady(r.ctx)
			if err != nil {
				r.logger.WithError(err).Errorf("failed getting shard key from queue")
				return
			}

			r.locked(func() {
				r.waitingTasksPerShard[key] = tasks
				if _, ok := r.waitingCtxPerShard[key]; !ok {
					r.waitingCtxPerShard[key], r.waitingCtxCancelPerShard[key] = context.WithCancelCause(r.ctx)
				}
			})

			eg.Go(func() error {
				var prevProcessingCtx context.Context
				var processingCtx context.Context
				var processingCtxCancel context.CancelCauseFunc
				var tasks []ShardReindexTaskV3

				r.locked(func() {
					tasks = r.waitingTasksPerShard[key]
					processingCtx = r.waitingCtxPerShard[key]
					processingCtxCancel = r.waitingCtxCancelPerShard[key]
					prevProcessingCtx = r.processingCtxPerShard[key]

					delete(r.waitingTasksPerShard, key)
					delete(r.waitingCtxPerShard, key)
					delete(r.waitingCtxCancelPerShard, key)

					r.processingCtxPerShard[key] = processingCtx
					r.processingCtxCancelPerShard[key] = processingCtxCancel
				})

				if processingCtx == nil {
					return nil
				}
				if prevProcessingCtx != nil {
					<-prevProcessingCtx.Done()
				}

				defer processingCtxCancel(fmt.Errorf("deferred, context cleanup"))
				return r.runScheduledTask(processingCtx, key, tasks)
			})
		}
	}, r.logger)
}

func (r *shardReindexerV3) RunBeforeLsmInit(_ context.Context, shard *Shard) (err error) {
	// TODO aliszka:blockmax merge contexts (reindex + incoming)?
	mergedCtx := r.ctx

	collectionName := shard.Index().Config.ClassName.String()
	logger := r.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shard.Name(),
		"method":     "RunBeforeLsmInit",
	})
	logger.Debug("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.WithError(err).Error("finished with error")
		} else {
			logger.Debug("finished")
		}
	}(time.Now())

	if len(r.tasks) == 0 {
		return nil
	}

	ec := errorcompounder.New()
	for i := range r.tasks {
		ec.Add(r.tasks[i].OnBeforeLsmInit(mergedCtx, shard))
	}
	err = ec.ToError()
	return
}

func (r *shardReindexerV3) RunAfterLsmInit(_ context.Context, shard *Shard) (err error) {
	// TODO aliszka:blockmax merge contexts (reindex + incoming)?
	mergedCtx := r.ctx

	collectionName := shard.Index().Config.ClassName.String()
	logger := r.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shard.Name(),
		"method":     "RunAfterLsmInit",
	})
	logger.Debug("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.WithError(err).Error("finished with error")
		} else {
			logger.Debug("finished")
		}
	}(time.Now())

	if len(r.tasks) == 0 {
		return nil
	}

	ec := errorcompounder.New()
	for i := range r.tasks {
		ec.Add(r.tasks[i].OnAfterLsmInit(mergedCtx, shard))
	}
	err = ec.ToError()
	return
}

func (r *shardReindexerV3) RunAfterLsmInitAsync(_ context.Context, shard *Shard) (err error) {
	collectionName := shard.Index().Config.ClassName.String()
	logger := r.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shard.Name(),
		"method":     "RunAfterLsmInitAsync",
	})
	logger.Debug("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.WithError(err).Error("finished with error")
		} else {
			logger.Debug("finished")
		}
	}(time.Now())

	if len(r.tasks) == 0 {
		return nil
	}

	key := toIndexShardKeyOfShard(shard)
	return r.scheduleTasks(key, r.tasks, time.Now())
}

func (r *shardReindexerV3) Stop(shard *Shard, cause error) {
	key := toIndexShardKeyOfShard(shard)
	r.locked(func() {
		if cancel, ok := r.processingCtxCancelPerShard[key]; ok {
			cancel(cause)
		}
		if cancel, ok := r.waitingCtxCancelPerShard[key]; ok {
			cancel(cause)
		}
		r.scheduleTasks(key, nil, time.Time{})
	})

	collectionName := shard.Index().Config.ClassName.String()
	r.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shard.Name(),
		"cause":      cause,
	}).Debug("stop reindex tasks requested")
}

func (r *shardReindexerV3) runScheduledTask(ctx context.Context, key string, tasks []ShardReindexTaskV3) (err error) {
	collectionName, shardName := fromIndexShardKey(key)
	logger := r.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shardName,
		"method":     "runScheduledTask",
	})
	logger.Debug("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.WithError(err).Error("finished with error")
		} else {
			logger.Debug("finished")
		}
	}(time.Now())

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (1): %w / %w", ctx.Err(), context.Cause(ctx))
		return
	}

	index := r.getIndex(schema.ClassName(collectionName))
	if index == nil {
		// try again later, as we have observed that index can be nil
		// for a short period of time after shard is created, but before it is loaded
		r.locked(func() {
			if ctx.Err() == nil {
				r.queue.insert(key, tasks, time.Now().Add(1*time.Minute))
			}
		})
		err = fmt.Errorf("index for shard '%s' of collection '%s' not found", shardName, collectionName)
		return
	}
	shard, release, err := index.GetShard(ctx, shardName)
	if err != nil {
		r.locked(func() {
			if ctx.Err() == nil {
				r.queue.insert(key, tasks, time.Now().Add(r.config.retryOnErrorInterval))
			}
		})
		err = fmt.Errorf("not loaded '%s' of collection '%s': %w", shardName, collectionName, err)
		return
	}

	rerunAt, reloadShard, err := func() (time.Time, bool, error) {
		defer release()
		if shard == nil {
			return time.Time{}, false, fmt.Errorf("shard '%s' of collection '%s' not found", shardName, collectionName)
		}

		if err = ctx.Err(); err != nil {
			return time.Time{}, false, fmt.Errorf("context check (2): %w / %w", ctx.Err(), context.Cause(ctx))
		}

		// at this point lazy shard should be loaded (there is no unloading), otherwise [RunAfterLsmInitAsync]
		// would not be called and tasks scheduled for shard
		return tasks[0].OnAfterLsmInitAsync(ctx, shard)
	}()

	scheduleNextTasks := func(ctx context.Context, lastErr error) (err error) {
		r.locked(func() {
			if lastErr != nil {
				// schedule tasks only if context not cancelled
				if ctx.Err() == nil {
					r.scheduleTasks(key, tasks[1:], time.Now())
				}
				err = fmt.Errorf("executing task '%s' on shard '%s' of collection '%s': %w",
					tasks[0].Name(), shardName, collectionName, lastErr)
				return
			}
			if err = ctx.Err(); err != nil {
				err = fmt.Errorf("executing task '%s' on shard '%s' of collection '%s': %w / %w",
					tasks[0].Name(), shardName, collectionName, err, context.Cause(ctx))
				return
			}
			if rerunAt.IsZero() {
				r.scheduleTasks(key, tasks[1:], time.Now())
				logger.WithField("task", tasks[0].Name()).Debug("task executed completely")
				return
			}
			r.scheduleTasks(key, tasks, rerunAt)
			logger.WithField("task", tasks[0].Name()).Debug("task executed partially, rerun scheduled")
		})
		return
	}

	// do not reload if error occurred. schedule tasks using shard's individual context
	if !reloadShard || err != nil || ctx.Err() != nil {
		err = scheduleNextTasks(ctx, err)
		return
	}

	// reload uninterrupted by context. shard's context will be cancelled by shutdown anyway
	if err = r.reloadShard(context.Background(), index, shardName); err != nil {
		err = fmt.Errorf("reloading shard '%s' of collection '%s': %w", shardName, collectionName, err)
	}
	// schedule tasks using global context
	err = scheduleNextTasks(r.ctx, err)
	return
}

func (r *shardReindexerV3) scheduleTasks(key string, tasks []ShardReindexTaskV3, runAt time.Time) error {
	if len(tasks) == 0 {
		r.queue.delete(key)
		return nil
	}

	return r.queue.insert(key, tasks, runAt)
}

func (r *shardReindexerV3) locked(callback func()) {
	r.lock.Lock()
	defer r.lock.Unlock()
	callback()
}

func (r *shardReindexerV3) reloadShard(ctx context.Context, index *Index, shardName string) error {
	if err := index.IncomingReinitShard(ctx, shardName); err != nil {
		return err
	}
	// force loading shard (if lazy) by getting store
	shard, release, err := index.GetShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()
	shard.Store()

	return nil
}

// -----------------------------------------------------------------------------

type shardsQueue struct {
	lock           *sync.Mutex
	runShardQueue  *priorityqueue.Queue[string]
	tasksPerShard  map[string][]ShardReindexTaskV3
	timerCtx       context.Context
	timerCtxCancel context.CancelFunc
}

func newShardsQueue() *shardsQueue {
	q := &shardsQueue{
		lock:          new(sync.Mutex),
		runShardQueue: priorityqueue.NewMinWithId[string](16),
		tasksPerShard: map[string][]ShardReindexTaskV3{},
	}
	q.timerCtx, q.timerCtxCancel = q.infiniteDeadlineCtx()

	return q
}

func (q *shardsQueue) insert(key string, tasks []ShardReindexTaskV3, runAt time.Time) error {
	id := q.timeToId(runAt)

	q.lock.Lock()
	defer q.lock.Unlock()

	if _, ok := q.tasksPerShard[key]; ok {
		return fmt.Errorf("tasks for shard already added")
	}
	q.tasksPerShard[key] = tasks

	q.runShardQueue.InsertWithValue(id, 0, key)
	// element added as top. update deadline context to new (and closest) runAt
	if top := q.runShardQueue.Top(); top.Value == key && top.ID == id {
		_, cancel := q.timerCtx, q.timerCtxCancel
		q.timerCtx, q.timerCtxCancel = q.deadlineCtx(runAt)
		cancel()
	}
	return nil
}

func (q *shardsQueue) delete(key string) bool {
	q.lock.Lock()
	defer q.lock.Unlock()

	if _, ok := q.tasksPerShard[key]; !ok {
		return false
	}
	delete(q.tasksPerShard, key)

	if q.runShardQueue.Len() > 0 && q.runShardQueue.Top().Value == key {
		_, cancel := q.timerCtx, q.timerCtxCancel
		q.runShardQueue.Pop()
		if q.runShardQueue.Len() > 0 {
			// set timer to next/top shard
			tm := q.idToTime(q.runShardQueue.Top().ID)
			q.timerCtx, q.timerCtxCancel = q.deadlineCtx(tm)
		} else {
			// set timer to "infinity"
			q.timerCtx, q.timerCtxCancel = q.infiniteDeadlineCtx()
		}
		cancel()
		return true
	}

	return q.runShardQueue.DeleteItem(func(item priorityqueue.Item[string]) bool {
		return item.Value == key
	})
}

func (q *shardsQueue) getWhenReady(ctx context.Context) (key string, tasks []ShardReindexTaskV3, err error) {
	q.lock.Lock()
	timerCtx, timerCtxCancel := q.timerCtx, q.timerCtxCancel
	q.lock.Unlock()

	for {
		select {
		case <-ctx.Done():
			timerCtxCancel()
			return "", nil, fmt.Errorf("context check (shardsQueue): %w / %w", ctx.Err(), context.Cause(ctx))

		case <-timerCtx.Done():
			q.lock.Lock()
			// check if this is latest ctx and deadline exceeded. if so then top key is to be returned
			if q.timerCtx == timerCtx && errors.Is(timerCtx.Err(), context.DeadlineExceeded) {
				defer q.lock.Unlock()

				if q.runShardQueue.Len() > 0 {
					key := q.runShardQueue.Pop().Value
					if q.runShardQueue.Len() > 0 {
						// set timer to next/top shard
						tm := q.idToTime(q.runShardQueue.Top().ID)
						q.timerCtx, q.timerCtxCancel = q.deadlineCtx(tm)
					} else {
						// set timer to "infinity"
						q.timerCtx, q.timerCtxCancel = q.infiniteDeadlineCtx()
					}
					tasks := q.tasksPerShard[key]
					delete(q.tasksPerShard, key)
					return key, tasks, nil
				}
				// should not happen
				return "", nil, fmt.Errorf("shards queue empty")
			}

			timerCtx, timerCtxCancel = q.timerCtx, q.timerCtxCancel
			q.lock.Unlock()
		}
	}
}

func (q *shardsQueue) timeToId(tm time.Time) uint64 {
	return uint64(-tm.UnixNano())
}

func (q *shardsQueue) idToTime(id uint64) time.Time {
	nsec := -int64(id)
	return time.Unix(0, nsec)
}

func (q *shardsQueue) deadlineCtx(deadline time.Time) (context.Context, context.CancelFunc) {
	return context.WithDeadline(context.Background(), deadline)
}

func (q *shardsQueue) infiniteDeadlineCtx() (context.Context, context.CancelFunc) {
	return q.deadlineCtx(time.Now().Add(10 * 365 * 24 * time.Hour))
}

// -----------------------------------------------------------------------------

func toIndexShardKeyOfShard(shard ShardLike) string {
	return toIndexShardKey(shard.Index().Config.ClassName.String(), shard.Name())
}

func toIndexShardKey(collectionName, shardName string) string {
	return collectionName + "//" + shardName
}

func fromIndexShardKey(key string) (string, string) {
	s := strings.Split(key, "//")
	return s[0], s[1]
}

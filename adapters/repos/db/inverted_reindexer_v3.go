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
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

type ShardReindexerV3 interface {
	RunBeforeLsmInit(ctx context.Context, shard *Shard) error
	RunAfterLsmInit(ctx context.Context, shard *Shard) error
	RunAfterLsmInitAsync(ctx context.Context, shard *Shard) error
}

type ShardReindexTaskV3 interface {
	Name() string
	OnBeforeLsmInit(ctx context.Context, shard *Shard) error
	OnAfterLsmInit(ctx context.Context, shard *Shard) error
	// TODO alisza:blockmax change to *Shard?
	OnAfterLsmInitAsync(ctx context.Context, shard ShardLike) (rerunAt time.Time, err error)
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

// -----------------------------------------------------------------------------

func NewShardReindexerV3(ctx context.Context, logger logrus.FieldLogger,
	getIndex func(className schema.ClassName) *Index,
) *shardReindexerV3 {
	return &shardReindexerV3{
		logger:   logger,
		ctx:      ctx,
		getIndex: getIndex,
		queue:    newShardsQueue(),
		lock:     new(sync.Mutex),

		taskNames:     map[string]struct{}{},
		tasks:         []ShardReindexTaskV3{},
		tasksPerShard: map[string][]ShardReindexTaskV3{},

		config: shardsReindexerV3Config{
			concurrency:          concurrency.NUMCPU_2,
			retryOnErrorInterval: 15 * time.Minute,
		},
	}
}

type shardReindexerV3 struct {
	logger   logrus.FieldLogger
	ctx      context.Context
	getIndex func(className schema.ClassName) *Index
	queue    *shardsQueue
	lock     *sync.Mutex

	taskNames     map[string]struct{}
	tasks         []ShardReindexTaskV3
	tasksPerShard map[string][]ShardReindexTaskV3

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
			key, err := r.queue.getWhenReady(r.ctx)
			if err != nil {
				r.logger.WithError(err).Errorf("failed getting shard key from queue")
				return
			}

			eg.Go(func() error {
				if err := r.runScheduledTask(r.ctx, key); err != nil {
					r.logger.WithError(err).Errorf("failed running scheduled task")
				}
				return nil
			})
		}
	}, r.logger)
}

func (r *shardReindexerV3) RunBeforeLsmInit(ctx context.Context, shard *Shard) (err error) {
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

func (r *shardReindexerV3) RunAfterLsmInit(ctx context.Context, shard *Shard) (err error) {
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

func (r *shardReindexerV3) RunAfterLsmInitAsync(ctx context.Context, shard *Shard) (err error) {
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

	key := toIndexShardKey(shard.Index().Config.ClassName.String(), shard.Name())
	r.scheduleTasks(key, r.tasks, time.Now())

	return nil
}

func (r *shardReindexerV3) runScheduledTask(ctx context.Context, key string) (err error) {
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

	var tasks []ShardReindexTaskV3
	r.locked(func() { tasks = r.tasksPerShard[key] })

	index := r.getIndex(schema.ClassName(collectionName))
	if index == nil {
		err = fmt.Errorf("index for shard %q of collection %q not found", shardName, collectionName)
		return
	}
	shard, release, err := index.GetShard(ctx, shardName)
	if err != nil {
		r.queue.insert(key, time.Now().Add(r.config.retryOnErrorInterval))
		err = fmt.Errorf("get shard %q of collection %q: %w", shardName, collectionName, err)
		return
	}
	defer release()

	if shard == nil {
		r.scheduleTasks(key, nil, time.Time{})
		err = fmt.Errorf("shard %q of collection %q not found", shardName, collectionName)
		return
	}

	// at this point lazy shard should be loaded (there is no unloading), otherwise [RunAfterLsmInitAsync]
	// would not be called and tasks scheduled for shard
	rerunAt, err := tasks[0].OnAfterLsmInitAsync(ctx, shard)
	if err != nil {
		r.scheduleTasks(key, tasks[1:], time.Now())
		err = fmt.Errorf("executing task %q on shard %q of collection %q: %w", tasks[0].Name(), shardName, collectionName, err)
		return
	}
	if rerunAt.IsZero() {
		r.scheduleTasks(key, tasks[1:], time.Now())
		logger.WithField("task", tasks[0].Name()).Debug("task executed completely")
		return nil
	}
	r.scheduleTasks(key, tasks, rerunAt)
	logger.WithField("task", tasks[0].Name()).Debug("task executed partially, rerun scheduled")
	return nil
}

func (r *shardReindexerV3) scheduleTasks(key string, tasks []ShardReindexTaskV3, runAt time.Time) {
	if len(tasks) == 0 {
		r.locked(func() { delete(r.tasksPerShard, key) })
		return
	}

	r.locked(func() { r.tasksPerShard[key] = tasks })
	r.queue.insert(key, runAt)
}

func (r *shardReindexerV3) locked(cb func()) {
	r.lock.Lock()
	defer r.lock.Unlock()
	cb()
}

// -----------------------------------------------------------------------------

type shardsQueue struct {
	lock           *sync.Mutex
	runShardQueue  *priorityqueue.Queue[string]
	timerCtx       context.Context
	timerCtxCancel context.CancelFunc
}

func newShardsQueue() *shardsQueue {
	q := &shardsQueue{
		lock:          new(sync.Mutex),
		runShardQueue: priorityqueue.NewMinWithId[string](16),
	}
	q.timerCtx, q.timerCtxCancel = q.infiniteDeadlineCtx()

	return q
}

func (q *shardsQueue) insert(key string, runAt time.Time) {
	id := q.timeToId(runAt)

	q.lock.Lock()
	defer q.lock.Unlock()

	q.runShardQueue.InsertWithValue(id, 0, key)
	// element added as top. update deadline context to new (and closest) runAt
	if top := q.runShardQueue.Top(); top.Value == key && top.ID == id {
		_, cancel := q.timerCtx, q.timerCtxCancel
		q.timerCtx, q.timerCtxCancel = q.deadlineCtx(runAt)
		cancel()
	}
}

func (q *shardsQueue) getWhenReady(ctx context.Context) (key string, err error) {
	q.lock.Lock()
	timerCtx, timerCtxCancel := q.timerCtx, q.timerCtxCancel
	q.lock.Unlock()

	for {
		select {
		case <-ctx.Done():
			timerCtxCancel()
			return "", fmt.Errorf("context check (shardsQueue): %w", ctx.Err())

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
					return key, nil
				}
				return "", fmt.Errorf("shards queue empty")
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

func toIndexShardKey(collectionName, shardName string) string {
	return collectionName + "//" + shardName
}

func fromIndexShardKey(key string) (string, string) {
	s := strings.Split(key, "//")
	return s[0], s[1]
}

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
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type ShardInvertedReindexTaskV2 interface {
	HasOnBefore() bool
	OnBefore(ctx context.Context) error
	OnBeforeByShard(ctx context.Context, shard ShardLike) error
	ReindexByShard(ctx context.Context, shard ShardLike) (rerunAt time.Time, err error)
}

type ReindexerV2 struct {
	logger    logrus.FieldLogger
	indexes   map[string]*Index
	indexIds  []string
	indexLock *sync.RWMutex

	taskNames []string
	tasks     map[string]ShardInvertedReindexTaskV2
	skipTasks map[string]struct{}

	config reindexerConfig
}

type reindexerConfig struct {
	concurrencyIndexes int
	concurrencyShards  int
	waitInterval       time.Duration
}

func NewReindexerV2(logger logrus.FieldLogger, indexes map[string]*Index, indexLock *sync.RWMutex) *ReindexerV2 {
	logger = logger.WithField("action", "reindexV2")

	return &ReindexerV2{
		logger:    logger,
		indexes:   indexes,
		indexLock: indexLock,
		tasks:     map[string]ShardInvertedReindexTaskV2{},
		skipTasks: map[string]struct{}{},
		config: reindexerConfig{
			concurrencyIndexes: concurrency.NUMCPU_2,
			concurrencyShards:  concurrency.NUMCPU,
			waitInterval:       5 * time.Second,
		},
	}
}

func (r *ReindexerV2) RegisterTask(name string, args any) error {
	if _, ok := r.tasks[name]; ok {
		return fmt.Errorf("task %q already registered", name)
	}

	switch name {
	case "ShardInvertedReindexTask_MapToBlockmax":
		swap := args.(map[string]bool)["ReindexMapToBlockmaxSwapBuckets"]
		tidy := args.(map[string]bool)["ReindexMapToBlockmaxTidyBuckets"]
		r.tasks[name] = NewShardInvertedReindexTaskMapToBlockmax(r.logger, swap, tidy)
		r.taskNames = append(r.taskNames, name)
	default:
		return fmt.Errorf("unknown/undefined task %q", name)
	}
	return nil
}

func (r *ReindexerV2) HasOnBefore() bool {
	for _, task := range r.tasks {
		if task.HasOnBefore() {
			return true
		}
	}
	return false
}

func (r *ReindexerV2) OnBefore(ctx context.Context) error {
	r.initIndexIds()
	errs := errorcompounder.NewSafe()

	for _, taskName := range r.taskNames {
		task := r.tasks[taskName]
		if !task.HasOnBefore() {
			continue
		}

		if err := task.OnBefore(ctx); err != nil {
			errs.Add(fmt.Errorf("OnBefore task %q: %w", taskName, err))
			r.skipTasks[taskName] = struct{}{}
			continue
		}

		for _, indexId := range r.indexIds {
			r.indexLock.RLock()
			index, ok := r.indexes[indexId]
			r.indexLock.RUnlock()
			if !ok {
				continue
			}

			collection := index.Config.ClassName.String()
			if err := index.ForEachShardConcurrently(func(_ string, shard ShardLike) error {
				if err := task.OnBeforeByShard(ctx, shard); err != nil {
					errs.Add(fmt.Errorf("OnBeforeByShard task %q, collection %q, shard %q: %w",
						taskName, collection, shard.Name(), err))
					return err
				}
				return nil
			}); err != nil {
				r.skipTasks[taskName] = struct{}{}
			}
		}
	}

	return errs.ToError()
}

func (r *ReindexerV2) Reindex(ctx context.Context) error {
	r.initIndexIds()
	errsAllTasks := errorcompounder.New()

	for _, taskName := range r.taskNames {
		logger := r.logger.WithField("task", taskName)

		if err := ctx.Err(); err != nil {
			errsAllTasks.Add(fmt.Errorf("task %q: context check (1): %w", taskName, err))
			break
		}

		if _, ok := r.skipTasks[taskName]; ok {
			logger.Debug("skipping due to previous error")
			continue
		}

		errsPerTask := errorcompounder.NewSafe()
		task := r.tasks[taskName]

		egIndexes, gctx := enterrors.NewErrorGroupWithContextWrapper(r.logger, ctx)
		egIndexes.SetLimit(r.config.concurrencyIndexes)
		egShards, gctx := enterrors.NewErrorGroupWithContextWrapper(r.logger, gctx)
		egShards.SetLimit(r.config.concurrencyShards)

		for _, indexId := range r.indexIds {
			r.indexLock.RLock()
			index, ok := r.indexes[indexId]
			r.indexLock.RUnlock()
			if !ok {
				continue
			}

			if err := gctx.Err(); err != nil {
				errsPerTask.Add(fmt.Errorf("index %q: context check(2): %w", indexId, err))
				break
			}

			egIndexes.Go(func() error {
				r.runForIndex(gctx, task, index, egShards, errsPerTask)
				return nil
			})
		}

		egIndexes.Wait()
		egShards.Wait()

		if err := errsPerTask.ToError(); err != nil {
			errsAllTasks.Add(fmt.Errorf("task %q: %w", taskName, err))
		}
	}

	return errsAllTasks.ToError()
}

func (r *ReindexerV2) initIndexIds() {
	if r.indexIds == nil {
		r.indexLock.RLock()
		r.indexIds = make([]string, 0, len(r.indexes))
		for name := range r.indexes {
			r.indexIds = append(r.indexIds, name)
		}
		r.indexLock.RUnlock()
	}
}

func (r *ReindexerV2) runForIndex(ctx context.Context, task ShardInvertedReindexTaskV2, index *Index,
	errgrp *enterrors.ErrorGroupWrapper, errs *errorcompounder.SafeErrorCompounder,
) {
	tracker := newProcessingTracker(r.config.waitInterval)

	index.ForEachShard(func(shardName string, _ ShardLike) error {
		tracker.increment()
		errgrp.Go(func() error {
			defer tracker.decrement()
			if err := r.runForShard(ctx, task, index, shardName, tracker); err != nil {
				errs.Add(fmt.Errorf("index %q / shard %q: %w", index.ID(), shardName, err))
			}
			return nil
		})
		return ctx.Err()
	})

	for {
		if err := ctx.Err(); err != nil {
			errs.Add(fmt.Errorf("index %q: context check (3): %w", index.ID(), err))
			return
		}

		if tracker.isFinished() {
			return
		}

		if shardName := tracker.getIfReady(); shardName != "" {
			tracker.increment()
			errgrp.Go(func() error {
				defer tracker.decrement()
				if err := r.runForShard(ctx, task, index, shardName, tracker); err != nil {
					errs.Add(fmt.Errorf("index %q / shard %q: %w", index.ID(), shardName, err))
				}
				return nil
			})
			continue
		}

		if err := tracker.wait(ctx); err != nil {
			errs.Add(fmt.Errorf("index %q: context check (4): %w", index.ID(), err))
			return
		}
	}
}

func (r *ReindexerV2) runForShard(ctx context.Context, task ShardInvertedReindexTaskV2,
	index *Index, shardName string, tracker *processingTracker,
) error {
	shard, release, err := index.GetShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	rerunAt, err := task.ReindexByShard(ctx, shard)
	if err != nil {
		return err
	}
	if !rerunAt.IsZero() {
		tracker.insert(shardName, rerunAt)
	}
	return nil
}

type processingTracker struct {
	lock              *sync.Mutex
	processingCounter int
	rerunShardQueue   *priorityqueue.Queue[string]
	config            processingTrackerConfig
}

type processingTrackerConfig struct {
	waitInterval time.Duration
}

func newProcessingTracker(waitInterval time.Duration) *processingTracker {
	return &processingTracker{
		lock:              new(sync.Mutex),
		processingCounter: 0,
		rerunShardQueue:   priorityqueue.NewMinWithId[string](16),
		config: processingTrackerConfig{
			waitInterval: waitInterval,
		},
	}
}

func (t *processingTracker) increment() {
	t.lock.Lock()
	t.processingCounter++
	t.lock.Unlock()
}

func (t *processingTracker) decrement() {
	t.lock.Lock()
	t.processingCounter--
	t.lock.Unlock()
}

func (t *processingTracker) insert(shardName string, rerunAt time.Time) {
	t.lock.Lock()
	t.rerunShardQueue.InsertWithValue(uint64(rerunAt.UnixMicro()), 0, shardName)
	t.lock.Unlock()
}

func (t *processingTracker) getIfReady() string {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.rerunShardQueue.Len() > 0 && int64(t.rerunShardQueue.Top().ID) < time.Now().UnixMicro() {
		return t.rerunShardQueue.Pop().Value
	}
	return ""
}

func (t *processingTracker) wait(ctx context.Context) error {
	timer := time.NewTimer(t.config.waitInterval)
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	}
}

func (t *processingTracker) isFinished() bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.processingCounter == 0 && t.rerunShardQueue.Len() == 0
}

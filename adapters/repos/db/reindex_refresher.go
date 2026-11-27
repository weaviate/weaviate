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

package db

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const (
	reindexRetryMin  = 50 * time.Millisecond
	reindexRetryMax  = 5 * time.Second
	reindexWarnEvery = time.Minute
)

// reindexRefresh is the node's one loop that publishes each loaded shard's
// view after a task change. Its queue is a work list that decides nothing,
// because every shard load computes its own view.
type reindexRefresh struct {
	db   *DB
	wake chan struct{}
	stop context.CancelFunc
	done chan struct{}

	mu       sync.Mutex
	pending  map[string]map[string]struct{} // collection -> shards; a nil set means all
	retry    map[shardKey]*retryEntry
	due      retryHeap
	lastWarn map[string]time.Time
}

type shardKey struct{ collection, shard string }

type retryEntry struct {
	key     shardKey
	at      time.Time
	backoff time.Duration
	err     error
	index   int // in due; -1 while not scheduled
}

func newReindexRefresh(db *DB) *reindexRefresh {
	return &reindexRefresh{
		db: db, wake: make(chan struct{}, 1),
		pending: map[string]map[string]struct{}{}, retry: map[shardKey]*retryEntry{}, lastWarn: map[string]time.Time{},
	}
}

func (db *DB) reindexRefresher() *reindexRefresh {
	if db == nil {
		return nil
	}
	return db.reindexRefresh
}

// notify queues shards of a collection, nil meaning all, and never waits.
func (r *reindexRefresh) notify(collection string, shards []string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	cur, seen := r.pending[collection]
	switch {
	case len(shards) == 0:
		r.pending[collection] = nil
	case !seen || cur != nil:
		if cur == nil {
			cur = make(map[string]struct{}, len(shards))
		}
		for _, s := range shards {
			cur[s] = struct{}{}
		}
		r.pending[collection] = cur
	}
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// start runs the loop until stop. Notices queued earlier are handled then.
func (r *reindexRefresh) start() {
	if r == nil || r.stop != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.stop, r.done = cancel, make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(r.done)
		timer := time.NewTimer(time.Hour)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.wake:
			case <-timer.C:
			}
			for coll, shards := range r.takeWork(time.Now()) {
				r.refreshCollection(ctx, coll, shards)
			}
			timer.Reset(r.nextWake())
		}
	}, r.db.logger)
}

func (r *reindexRefresh) shutdown() {
	if r == nil || r.stop == nil {
		return
	}
	r.stop()
	<-r.done
}

func (r *reindexRefresh) takeWork(now time.Time) map[string]map[string]struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	work := r.pending
	r.pending = map[string]map[string]struct{}{}
	for len(r.due) > 0 && !r.due[0].at.After(now) {
		e := heap.Pop(&r.due).(*retryEntry)
		shards, seen := work[e.key.collection]
		switch {
		case seen && shards == nil:
		case shards == nil:
			work[e.key.collection] = map[string]struct{}{e.key.shard: {}}
		default:
			shards[e.key.shard] = struct{}{}
		}
	}
	return work
}

func (r *reindexRefresh) nextWake() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.due) == 0 {
		return time.Hour
	}
	return max(time.Until(r.due[0].at), time.Millisecond)
}

func (r *reindexRefresh) refreshCollection(ctx context.Context, coll string, shards map[string]struct{}) {
	idx := r.db.registeredIndex(schema.ClassName(coll))
	if idx == nil {
		r.forgetCollection(coll)
		return
	}
	var failed *Shard
	var failErr error
	visit := func(name string, sl ShardLike) {
		sh, busy := loadedShardNoWait(sl)
		switch {
		case busy:
			r.scheduleRetry(shardKey{coll, name}, nil)
		case sh == nil:
			r.clearRetry(shardKey{coll, name})
		default:
			err := sh.refreshView(ctx)
			if err == nil {
				r.clearRetry(shardKey{coll, name})
				return
			}
			if !errors.Is(err, errViewNotReady) && failed == nil {
				failed, failErr = sh, err
			}
			r.scheduleRetry(shardKey{coll, name}, err)
		}
	}
	if shards == nil {
		idx.shards.Range(func(name string, sl ShardLike) error {
			visit(name, sl)
			return nil
		})
	} else {
		for name := range shards {
			if sl := idx.shards.Load(name); sl != nil {
				visit(name, sl)
			} else {
				r.clearRetry(shardKey{coll, name})
			}
		}
	}
	if failed != nil {
		r.warn(coll, failed, failErr)
	}
}

// loadedShardNoWait returns the shard only when it is loaded, and busy when a
// lazy shard's load lock is taken, so the loop never waits on a load.
func loadedShardNoWait(sl ShardLike) (sh *Shard, busy bool) {
	switch s := sl.(type) {
	case *Shard:
		return s, false
	case *LazyLoadShard:
		if !s.mutex.TryLock() {
			return nil, true
		}
		defer s.mutex.Unlock()
		if !s.loaded {
			return nil, false
		}
		return s.shard, false
	default:
		return nil, false
	}
}

func (r *reindexRefresh) scheduleRetry(key shardKey, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.retry[key]
	if e == nil {
		e = &retryEntry{key: key, index: -1}
		r.retry[key] = e
	}
	e.backoff = min(max(e.backoff*2, reindexRetryMin), reindexRetryMax)
	e.at, e.err = time.Now().Add(e.backoff), err
	if e.index >= 0 {
		heap.Fix(&r.due, e.index)
	} else {
		heap.Push(&r.due, e)
	}
	monitoring.GetMetrics().SetReindexViewRefreshPending(len(r.retry))
}

func (r *reindexRefresh) clearRetry(key shardKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeLocked(key)
}

func (r *reindexRefresh) forgetCollection(coll string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.retry {
		if key.collection == coll {
			r.removeLocked(key)
		}
	}
	delete(r.lastWarn, coll)
}

func (r *reindexRefresh) removeLocked(key shardKey) {
	e := r.retry[key]
	if e == nil {
		return
	}
	if e.index >= 0 {
		heap.Remove(&r.due, e.index)
	}
	delete(r.retry, key)
	monitoring.GetMetrics().SetReindexViewRefreshPending(len(r.retry))
}

func (r *reindexRefresh) warn(coll string, sh *Shard, err error) {
	r.mu.Lock()
	if time.Since(r.lastWarn[coll]) < reindexWarnEvery {
		r.mu.Unlock()
		return
	}
	r.lastWarn[coll] = time.Now()
	more := -1
	for key, e := range r.retry {
		if key.collection == coll && e.err != nil {
			more++
		}
	}
	r.mu.Unlock()
	r.db.logger.WithField("action", "reindex_refresh").WithField("collection", coll).WithField("shard", sh.name).
		WithField("more_pending", max(more, 0)).WithField("waiting_tasks", sh.view.wait.waitingTasks()).
		Warnf("cannot make shard %q serve what the cluster state says, retrying; cancel the task or restart the node "+
			"if this persists: %v", sh.name, err)
}

type retryHeap []*retryEntry

func (h retryHeap) Len() int           { return len(h) }
func (h retryHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h retryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *retryHeap) Push(x any) {
	e := x.(*retryEntry)
	e.index = len(*h)
	*h = append(*h, e)
}

func (h *retryHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	e.index = -1
	return e
}

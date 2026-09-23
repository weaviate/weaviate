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

package cluster

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// loadState is the local DB's readiness.
type loadState int32

const (
	loadIdle loadState = iota
	loadRunning
	loadDone
)

// dbLoader runs the startup shard load off the FSM goroutine and owns whether
// the local DB is ready.
//
// The load rebuilds indexes from the schema as it stood when it started, so it
// owns the DB until then: commands applied meanwhile queue their DB write, and
// the loader runs them in log order before publishing the DB.
type dbLoader struct {
	log logrus.FieldLogger

	state atomic.Int32 // read on every apply, outside mu

	mu     sync.Mutex
	queued []func()
	cancel context.CancelFunc

	wg sync.WaitGroup
}

func newDBLoader(log logrus.FieldLogger) *dbLoader {
	return &dbLoader{log: log}
}

func (l *dbLoader) done() bool {
	return l != nil && loadState(l.state.Load()) == loadDone
}

// markDone publishes a DB that needs no load, as an empty node's does.
func (l *dbLoader) markDone() {
	l.state.Store(int32(loadDone))
}

// run loads in the background unless a load has already run. Apply is raft's
// FSM goroutine: a load of minutes to hours there stalls every other command,
// bootstrap joins included.
func (l *dbLoader) run(load func(context.Context)) bool {
	l.mu.Lock()
	if loadState(l.state.Load()) != loadIdle {
		l.mu.Unlock()
		return false
	}
	ctx := l.beginLocked()
	l.mu.Unlock()

	enterrors.GoWrapper(func() {
		defer l.wg.Done()
		l.loadThenPublish(ctx, load)
	}, l.log)
	return true
}

// runInline loads on the caller's goroutine, superseding a running load: raft
// requires a restore not to overlap other commands.
func (l *dbLoader) runInline(load func(context.Context)) {
	l.stop()

	l.mu.Lock()
	ctx := l.beginLocked()
	l.mu.Unlock()
	defer l.wg.Done()

	// Writes the superseded load left queued predate the snapshot, and the
	// restored schema no longer names what they deleted: run them first, or
	// nothing ever removes that data.
	l.drain(ctx, false)
	l.loadThenPublish(ctx, load)
}

func (l *dbLoader) beginLocked() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.wg.Add(1)
	l.state.Store(int32(loadRunning))
	return ctx
}

// stop cancels a running load and waits for it, so it cannot hold shutdown open.
func (l *dbLoader) stop() {
	if l == nil {
		return
	}
	l.mu.Lock()
	cancel := l.cancel
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	l.wg.Wait()
}

// deferWrite queues write if a load is running. The check shares mu with the
// drain below, so nothing is queued once the DB is published.
func (l *dbLoader) deferWrite(write func()) bool {
	if loadState(l.state.Load()) != loadRunning {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if loadState(l.state.Load()) != loadRunning {
		return false
	}
	l.queued = append(l.queued, write)
	return true
}

// deferStoreWrite adapts deferWrite to [schema.StoreWriteDeferrer].
func (l *dbLoader) deferStoreWrite(op string, write func() error) bool {
	return l.deferWrite(func() {
		if err := write(); err != nil {
			l.log.WithField("op", op).Errorf("DB write deferred behind the local DB load: %v", err)
		}
	})
}

// loadThenPublish loads, then runs the writes queued behind it in order and
// publishes the DB.
func (l *dbLoader) loadThenPublish(ctx context.Context, load func(context.Context)) {
	load(ctx)
	l.drain(ctx, true)
}

// drain runs the queued writes in order until none are left, stopping early on
// cancellation and leaving the rest for whoever supersedes the load. With
// publish, the DB goes ready under the same lock as the last emptiness check,
// so nothing is queued once it is published.
func (l *dbLoader) drain(ctx context.Context, publish bool) {
	for {
		l.mu.Lock()
		if len(l.queued) == 0 || ctx.Err() != nil {
			if publish {
				l.state.Store(int32(loadDone))
			}
			l.mu.Unlock()
			return
		}
		batch := l.queued
		l.queued = nil
		l.mu.Unlock()

		for _, write := range batch {
			write()
		}
	}
}

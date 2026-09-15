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

// dbLoader runs the startup shard load off the FSM goroutine. DB writes applied
// meanwhile are queued and run in log order once the load is done.
type dbLoader struct {
	log logrus.FieldLogger

	inFlight atomic.Bool // read on every apply, outside mu

	mu      sync.Mutex
	started bool
	queued  []func()
	cancel  context.CancelFunc

	wg sync.WaitGroup
}

func newDBLoader(log logrus.FieldLogger) *dbLoader {
	return &dbLoader{log: log}
}

// begin reports whether this call owns the load; the caller must call l.wg.Done.
// One-shot: inFlight clears before dbLoaded is set, so Apply could otherwise
// start a second loader in between.
func (l *dbLoader) begin() (context.Context, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return nil, false
	}
	l.started = true
	l.inFlight.Store(true)
	l.wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	return ctx, true
}

// start runs load in the background unless a load has already run.
func (l *dbLoader) start(load func(context.Context)) bool {
	ctx, ok := l.begin()
	if !ok {
		return false
	}
	enterrors.GoWrapper(func() {
		defer l.wg.Done()
		load(ctx)
	}, l.log)
	return true
}

// stop cancels a running load and waits for it.
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

// deferWrite queues write if a load is running. The check shares mu with drain,
// so a write cannot be queued after drain found the queue empty.
func (l *dbLoader) deferWrite(write func()) bool {
	if !l.inFlight.Load() {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.inFlight.Load() {
		return false
	}
	l.queued = append(l.queued, write)
	return true
}

// deferStoreWrite adapts deferWrite to [schema.StoreWriteDeferrer].
func (l *dbLoader) deferStoreWrite(op string, write func() error) bool {
	if !l.inFlight.Load() {
		return false
	}
	return l.deferWrite(func() {
		if err := write(); err != nil {
			l.log.WithField("op", op).Errorf("DB write deferred behind the local DB load: %v", err)
		}
	})
}

// drain runs queued writes in order until none are left, then ends the load.
// On cancellation the queue is dropped.
func (l *dbLoader) drain(ctx context.Context) {
	for {
		l.mu.Lock()
		batch := l.queued
		l.queued = nil
		if len(batch) == 0 || ctx.Err() != nil {
			l.inFlight.Store(false)
			l.mu.Unlock()
			return
		}
		l.mu.Unlock()

		for _, write := range batch {
			write()
		}
	}
}

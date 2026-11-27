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

package idleshardcollector

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// shard is what the policy needs of a lazy shard wrapper; *db.LazyLoadShard in production.
type shard interface {
	comparable
	LastAccess() uint32
	UnloadCandidate() bool
	TryMarkUnloadPending() bool
	ClearUnloadPending()
	TryUnload(ctx context.Context, req db.UnloadRequest) bool
}

// registry is what the policy needs of *db.LoadedShardRegistry.
type registry[S shard] interface {
	Loaded() int64
	Unloading() int64
	AppendChunk(dst []S, from, limit int) ([]S, int)
}

// queueCapacity bounds the handoff channel; a full channel cuts the sweep short until the next tick.
const queueCapacity = 1024

type item[S shard] struct {
	shard S
}

type policyDeps struct {
	logger   logrus.FieldLogger
	metrics  *metrics
	clock    func() uint32
	queueCap int
	// snapshotRead runs in refresh between reading the knobs and publishing them; tests only.
	snapshotRead func()
}

// policy is the unloading policy over any shard type, so tests can drive it with fakes.
//
// Concurrency: the loop is the only goroutine that resizes workers and owns the loop* fields.
type policy[S shard] struct {
	policyDeps
	reg   registry[S]
	knobs Knobs

	snap atomic.Pointer[snapshot]
	// refreshMu keeps an older knob reading from publishing over a newer one.
	refreshMu sync.Mutex

	backgroundCh chan item[S]
	wake         chan struct{}
	pending      atomic.Int64
	// enqueuing counts enqueues past their closed check; close drains only once it is 0.
	enqueuing atomic.Int64

	ctx       context.Context
	cancel    context.CancelFunc
	closed    atomic.Bool
	closeOnce sync.Once
	wg        sync.WaitGroup

	workersMu      sync.Mutex
	workers        []chan struct{}
	workersClosed  bool
	workersRunning atomic.Int64

	loopBuf []S
	ranked  []ranked[S]
}

type ranked[S shard] struct {
	shard  S
	access uint32
}

func newPolicy[S shard](reg registry[S], knobs Knobs, deps policyDeps) *policy[S] {
	if deps.queueCap <= 0 {
		deps.queueCap = queueCapacity
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &policy[S]{
		policyDeps:   deps,
		reg:          reg,
		knobs:        knobs.withDefaults(),
		backgroundCh: make(chan item[S], deps.queueCap),
		wake:         make(chan struct{}, 1),
		ctx:          ctx,
		cancel:       cancel,
	}
	s := p.refresh()
	p.adjustWorkers(s.workers)
	p.wg.Add(1)
	enterrors.GoWrapper(func() {
		defer p.wg.Done()
		p.runLoop()
	}, p.logger)
	return p
}

func (p *policy[S]) refresh() *snapshot {
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	s := readSnapshot(p.knobs)
	if p.snapshotRead != nil {
		p.snapshotRead()
	}
	p.snap.Store(s)
	return s
}

// refreshConfig re-reads the knobs now and wakes the loop to apply them.
func (p *policy[S]) refreshConfig() {
	if p.closed.Load() {
		return
	}
	p.refresh()
	p.wakeLoop()
}

func (p *policy[S]) wakeLoop() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// collecting reports whether the current knobs unload idle shards.
func (p *policy[S]) collecting() bool {
	s := p.snap.Load()
	return !p.closed.Load() && s.idleTTL > 0 && !s.paused
}

// enqueue hands a claimed shard to the workers without blocking; on failure,
// a closed policy included, it releases the claim.
func (p *policy[S]) enqueue(ch chan item[S], it item[S]) bool {
	p.enqueuing.Add(1)
	defer p.enqueuing.Add(-1)
	if p.closed.Load() {
		it.shard.ClearUnloadPending()
		return false
	}
	p.pending.Add(1)
	select {
	case ch <- it:
		return true
	default:
		p.pending.Add(-1)
		it.shard.ClearUnloadPending()
		return false
	}
}

// close stops the loop and the workers. Queued handoffs are released before it
// waits, so a close that times out on an unload in flight strands no claim.
func (p *policy[S]) close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		p.cancel()
		p.workersMu.Lock()
		p.workersClosed = true
		for _, stop := range p.workers {
			close(stop)
		}
		p.workers = nil
		p.workersMu.Unlock()
		// An enqueue that read closed as false is past its check; once none
		// is, every later one refuses, so this drain is the last.
		for p.enqueuing.Load() > 0 {
			runtime.Gosched()
		}
		p.drainQueues()
	})
	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		p.wg.Wait()
		close(done)
	}, p.logger)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *policy[S]) drainQueues() {
	for {
		select {
		case it := <-p.backgroundCh:
			p.release(it)
		default:
			return
		}
	}
}

func (p *policy[S]) release(it item[S]) {
	it.shard.ClearUnloadPending()
	p.pending.Add(-1)
}

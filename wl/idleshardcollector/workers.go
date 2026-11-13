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
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// adjustWorkers grows or shrinks the pool to n; a stopped worker finishes its current unload first.
func (p *policy[S]) adjustWorkers(n int) {
	p.workersMu.Lock()
	defer p.workersMu.Unlock()
	if p.workersClosed {
		return
	}
	for len(p.workers) < n {
		stop := make(chan struct{})
		p.workers = append(p.workers, stop)
		p.wg.Add(1)
		enterrors.GoWrapper(func() {
			defer p.wg.Done()
			p.workersRunning.Add(1)
			defer p.workersRunning.Add(-1)
			p.work(stop)
		}, p.logger)
	}
	for len(p.workers) > n {
		last := len(p.workers) - 1
		close(p.workers[last])
		p.workers[last] = nil
		p.workers = p.workers[:last]
	}
}

func (p *policy[S]) work(stop <-chan struct{}) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-stop:
			return
		case it := <-p.backgroundCh:
			if p.retired(stop) {
				p.release(it)
				return
			}
			p.process(it)
		}
	}
}

// retired reports whether stop closed or the policy stopped; select can still pick a queued item after either.
func (p *policy[S]) retired(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return p.ctx.Err() != nil
	}
}

func (p *policy[S]) process(it item[S]) {
	defer p.pending.Add(-1)
	defer func() {
		if r := recover(); r != nil {
			it.shard.ClearUnloadPending()
			p.logger.WithField("action", "idle_shard_collector_unload").Errorf("unload panicked, the shard stays loaded: %v", r)
			enterrors.PrintStack(p.logger)
		}
	}()
	s := p.snap.Load()
	if p.ctx.Err() != nil || s.paused || !p.stillWanted(s, it) {
		it.shard.ClearUnloadPending()
		return
	}
	start := time.Now()
	unloaded := it.shard.TryUnload(p.ctx, db.UnloadRequest{IdleTTL: s.idleTTL})
	if unloaded {
		p.metrics.unloadDuration.Observe(time.Since(start).Seconds())
	}
	p.metrics.observeUnload(unloaded)
}

// stillWanted re-checks a handoff, which may have queued behind busy workers, against the current knobs.
func (p *policy[S]) stillWanted(s *snapshot, it item[S]) bool {
	return s.idleTTL > 0 && p.namespaceActive(it.shard.Namespace())
}

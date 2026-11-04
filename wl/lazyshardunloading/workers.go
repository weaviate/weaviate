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

package lazyshardunloading

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

// work prefers make-room handoffs, which a request is waiting to see land, over background ones.
func (p *policy[S]) work(stop <-chan struct{}) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-stop:
			return
		case it := <-p.makeRoomCh:
			p.process(it)
			continue
		default:
		}
		select {
		case <-p.ctx.Done():
			return
		case <-stop:
			return
		case it := <-p.makeRoomCh:
			p.process(it)
		case it := <-p.backgroundCh:
			p.process(it)
		}
	}
}

func (p *policy[S]) process(it item[S]) {
	defer p.pending.Add(-1)
	defer func() {
		if r := recover(); r != nil {
			it.shard.ClearUnloadPending()
			p.coolDown(p.snap.Load(), it.shard, refusalCorrectness)
			p.logger.WithField("action", "lazy_shard_unload").Errorf("unload panicked, the shard stays loaded: %v", r)
			enterrors.PrintStack(p.logger)
		}
	}()
	s := p.snap.Load()
	if p.ctx.Err() != nil || s.paused || (it.reason == reasonIdle && s.idleTTL == 0) {
		it.shard.ClearUnloadPending()
		return
	}
	start := time.Now()
	outcome := it.shard.TryUnload(p.ctx, it.req)
	if outcome == db.UnloadOutcomeOK {
		p.metrics.unloadDuration.Observe(time.Since(start).Seconds())
	}
	p.metrics.observeUnload(it.reason, string(outcome))
	p.coolDown(s, it.shard, refusalLevelOf(outcome))
}

// refusalLevel says which victim selection a refusal keeps off the shard.
type refusalLevel uint8

const (
	// refusalNone: nothing to wait out on this shard.
	refusalNone refusalLevel = iota
	// refusalSoft: a clause only the loop honours, so make-room may still take the shard.
	refusalSoft
	// refusalCorrectness: a clause every mode honours.
	refusalCorrectness
)

// refusalLevelOf has no default, so the exhaustive lint fails until a new outcome is listed.
func refusalLevelOf(outcome db.UnloadOutcome) refusalLevel {
	switch outcome {
	// Unloaded, or gone from the node or the registry: no attempt follows.
	case db.UnloadOutcomeOK, db.UnloadOutcomeAlreadyCold, db.UnloadOutcomeNotInMap,
		db.UnloadOutcomeIndexClosing, db.UnloadOutcomeTorn:
		return refusalNone
	// Its LastAccess moved, so ranking already puts it behind the others.
	case db.UnloadOutcomeReaccessed:
		return refusalNone
	// Node-wide answers every shard gets: skipping this one frees no other, and only delays the resume.
	case db.UnloadOutcomeReindexUnknown, db.UnloadOutcomeResourceReadOnly:
		return refusalNone
	case db.UnloadOutcomeMinResidency, db.UnloadOutcomeNotIdle,
		db.UnloadOutcomeQueueNotEmpty, db.UnloadOutcomeHashtreeInit:
		return refusalSoft
	case db.UnloadOutcomeLockBusy, db.UnloadOutcomeRecovering, db.UnloadOutcomeInUse, db.UnloadOutcomeHalted,
		db.UnloadOutcomePending2PC, db.UnloadOutcomeChangeLog, db.UnloadOutcomeAsyncOverride,
		db.UnloadOutcomeAsyncCheckpoint, db.UnloadOutcomeVectorOp, db.UnloadOutcomeReindex,
		db.UnloadOutcomeManualReadOnly:
		return refusalCorrectness
	}
	return refusalNone
}

// coolDown keeps a refused shard off victim selection for retry_refused_after, so a
// blocker UnloadCandidate does not see cannot starve the shards behind it.
func (p *policy[S]) coolDown(s *snapshot, sh S, level refusalLevel) {
	seconds := s.retryRefusedAfterSeconds()
	if seconds == 0 || level == refusalNone {
		return
	}
	until := p.clock() + seconds
	sh.SetBackgroundRetryAt(until)
	if level == refusalCorrectness {
		sh.SetMakeRoomRetryAt(until)
	}
}

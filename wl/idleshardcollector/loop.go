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
	"cmp"
	"slices"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// idleSweepChunk bounds the wrappers an idle sweep copies per registry lock.
const idleSweepChunk = 4096

func (p *policy[S]) runLoop() {
	timer := time.NewTimer(p.snap.Load().checkInterval)
	defer timer.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-timer.C:
		case <-p.wake:
		}
		s := p.safeTick()
		timer.Reset(s.checkInterval)
	}
}

// safeTick keeps the loop alive across a panicking tick.
func (p *policy[S]) safeTick() (s *snapshot) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.WithField("action", "idle_shard_collector_tick").Errorf("tick panicked: %v", r)
			enterrors.PrintStack(p.logger)
			s = p.snap.Load()
		}
	}()
	return p.tick()
}

func (p *policy[S]) tick() *snapshot {
	s := p.refresh()
	p.adjustWorkers(s.workers)
	p.publishGauges()
	if s.paused {
		return s
	}
	p.sweepIdle(s)
	return s
}

func (p *policy[S]) publishGauges() {
	if p.namespaces {
		p.nsCounts = p.reg.NamespaceCounts(p.nsCounts)
		for ns := range p.nsGauges {
			if _, ok := p.nsCounts[ns]; !ok {
				p.metrics.loaded.DeleteLabelValues(ns)
				delete(p.nsGauges, ns)
			}
		}
		for ns, n := range p.nsCounts {
			p.metrics.loaded.WithLabelValues(ns).Set(float64(n))
			p.nsGauges[ns] = struct{}{}
		}
	} else {
		p.metrics.loaded.WithLabelValues().Set(float64(p.reg.Loaded()))
	}
	p.metrics.unloading.Set(float64(p.reg.Unloading()))
}

// sweepIdle queues up to unload_rate shards idle for idle_ttl, least recently
// used first; what the budget or a full queue leaves waits for the next check.
func (p *policy[S]) sweepIdle(s *snapshot) {
	budget := min(s.rate, cap(p.backgroundCh)-len(p.backgroundCh))
	if s.idleTTL <= 0 || budget <= 0 {
		return
	}
	clear(p.nsActiveCache)
	p.enqueueRanked(p.rank(s), budget)
}

// rank walks every loaded shard in chunks and returns the idle ones, least recently used first.
func (p *policy[S]) rank(s *snapshot) []ranked[S] {
	p.loopBuf = p.loopBuf[:0]
	for from := 0; from >= 0; {
		p.loopBuf, from = p.reg.AppendChunk(p.loopBuf, from, idleSweepChunk)
	}
	clock := p.clock()
	idleSeconds := accessClockTicks(s.idleTTL)
	p.ranked = p.ranked[:0]
	for _, c := range p.loopBuf {
		if !c.UnloadCandidate() || !p.loopNamespaceActive(c.Namespace()) {
			continue
		}
		at := c.LastAccess()
		// Strictly more ticks than idle_ttl, the same rule as db.AccessClockIdle.
		if clock <= at || clock-at <= idleSeconds {
			continue
		}
		p.ranked = append(p.ranked, ranked[S]{shard: c, access: at})
	}
	clear(p.loopBuf)
	p.loopBuf = p.loopBuf[:0]
	slices.SortFunc(p.ranked, func(a, b ranked[S]) int { return cmp.Compare(a.access, b.access) })
	return p.ranked
}

func (p *policy[S]) loopNamespaceActive(namespace string) bool {
	if namespace == "" || p.nsActive == nil {
		return true
	}
	active, ok := p.nsActiveCache[namespace]
	if !ok {
		active = p.nsActive(namespace)
		p.nsActiveCache[namespace] = active
	}
	return active
}

func (p *policy[S]) enqueueRanked(eligible []ranked[S], n int) int {
	var zero S
	queued := 0
	for i := range eligible {
		if queued >= n {
			break
		}
		c := eligible[i].shard
		eligible[i].shard = zero
		if !c.TryMarkUnloadPending() {
			continue
		}
		if !p.enqueue(p.backgroundCh, item[S]{shard: c}) {
			break
		}
		queued++
	}
	for i := range eligible {
		eligible[i].shard = zero
	}
	return queued
}

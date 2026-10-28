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
	"cmp"
	"slices"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// maxWatermarkSample bounds the victims sampled per watermark tick.
const maxWatermarkSample = 4096

// idleSweepChunk bounds the wrappers an idle sweep copies per registry lock.
const idleSweepChunk = 4096

func (p *policy[S]) runLoop() {
	timer := time.NewTimer(p.snap.Load().interval)
	defer timer.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-timer.C:
		case <-p.wake:
		}
		s := p.safeTick()
		timer.Reset(s.interval)
	}
}

// safeTick keeps the loop alive across a panicking tick.
func (p *policy[S]) safeTick() (s *snapshot) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.WithField("action", "lazy_shard_unloading_tick").Errorf("tick panicked: %v", r)
			enterrors.PrintStack(p.logger)
			s = p.snap.Load()
		}
	}()
	return p.tick(time.Now())
}

func (p *policy[S]) tick(now time.Time) *snapshot {
	s := p.refresh()
	p.adjustWorkers(s.workers)
	p.storeRatio(p.memRatio())
	p.publishGauges(s)
	if s.paused {
		p.draining, p.idleBacklog = false, false
		return s
	}
	clear(p.nsActiveCache)
	if s.maxLoaded > 0 || s.high > 0 || s.idleTTL > 0 {
		p.refreshReindex()
	}
	budget := min(s.rate, cap(p.backgroundCh)-len(p.backgroundCh))
	budget -= p.drainWatermarks(s, now, budget)
	p.sweepIdle(s, now, budget)
	return s
}

func (p *policy[S]) publishGauges(s *snapshot) {
	loaded := p.reg.Loaded()
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
		p.metrics.loaded.WithLabelValues().Set(float64(loaded))
	}
	p.metrics.unloading.Set(float64(p.reg.Unloading()))
	p.metrics.limit.Set(float64(s.maxLoaded))
	over := int64(0)
	if s.maxLoaded > 0 {
		over = max(loaded-s.maxLoaded, 0)
	}
	p.metrics.overLimit.Set(float64(over))
}

// drainWatermarks unloads toward max_loaded_shards, and while the heap is
// draining (above high until below low) up to budget per tick.
func (p *policy[S]) drainWatermarks(s *snapshot, now time.Time, budget int) int {
	need := int64(0)
	if s.maxLoaded > 0 {
		need = max(p.reg.Loaded()-p.pending.Load()-s.maxLoaded, 0)
	}
	if s.high > 0 {
		switch ratio := p.cachedRatio(); {
		case ratio > s.high:
			p.draining = true
		case ratio < s.low:
			p.draining = false
		}
		if p.draining {
			need = int64(budget)
		}
	} else {
		p.draining = false
	}
	need = min(need, int64(budget))
	if need <= 0 {
		return 0
	}
	sample := int(min(max(int64(s.candidates), 4*need), maxWatermarkSample))
	eligible := p.rank(s, now, sample, false)
	return p.enqueueRanked(eligible, int(need), reasonWatermark,
		db.UnloadRequest{Mode: db.UnloadModeBackground, MinResidency: s.minResidency, Reindex: p.reindexSnapshot()})
}

// sweepIdle unloads every shard idle for idle_ttl, whatever the limits, every
// idle_check_interval; a sweep cut short by budget resumes on the next tick.
func (p *policy[S]) sweepIdle(s *snapshot, now time.Time, budget int) {
	if s.idleTTL <= 0 {
		p.idleBacklog = false
		return
	}
	if !p.idleBacklog && !p.lastIdleSweep.IsZero() && now.Sub(p.lastIdleSweep) < s.idleCheckInterval {
		return
	}
	p.lastIdleSweep = now
	if budget <= 0 {
		p.idleBacklog = true
		return
	}
	eligible := p.rank(s, now, 0, true)
	queued := p.enqueueRanked(eligible, budget, reasonIdle,
		db.UnloadRequest{Mode: db.UnloadModeBackground, MinResidency: s.minResidency, IdleTTL: s.idleTTL, Reindex: p.reindexSnapshot()})
	p.idleBacklog = queued >= budget && len(eligible) > queued
}

// rank samples k loaded shards, or for idle walks every one in chunks, and
// returns those the background may unload, over-share namespaces first, then
// least recently used.
func (p *policy[S]) rank(s *snapshot, now time.Time, k int, idle bool) []ranked[S] {
	if idle {
		p.loopBuf = p.loopBuf[:0]
		for from := 0; from >= 0; {
			p.loopBuf, from = p.reg.AppendChunk(p.loopBuf, from, idleSweepChunk)
		}
	} else {
		p.loopBuf = p.reg.Sample(p.loopBuf[:0], k)
	}
	if p.namespaces && s.nsShare > 0 {
		p.nsCounts = p.reg.NamespaceCounts(p.nsCounts)
	}
	clock := p.clock()
	idleSeconds := uint32(min(s.idleTTL/time.Second, 1<<31))
	p.ranked = p.ranked[:0]
	for _, c := range p.loopBuf {
		if !c.UnloadCandidate() || !p.loopNamespaceActive(c.Namespace()) {
			continue
		}
		loadedAt := c.LoadedAt()
		if loadedAt.IsZero() || now.Sub(loadedAt) < s.minResidency {
			continue
		}
		at := c.LastAccess()
		if idle && (clock < at || clock-at < idleSeconds) {
			continue
		}
		preferred := p.namespaces && s.nsShare > 0 && p.overShare(s, c.Namespace(), p.nsCounts[c.Namespace()])
		p.ranked = append(p.ranked, ranked[S]{shard: c, access: at, preferred: preferred})
	}
	clear(p.loopBuf)
	p.loopBuf = p.loopBuf[:0]
	slices.SortFunc(p.ranked, func(a, b ranked[S]) int {
		if a.preferred != b.preferred {
			if a.preferred {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.access, b.access)
	})
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

func (p *policy[S]) enqueueRanked(eligible []ranked[S], n int, r reason, req db.UnloadRequest) int {
	var zero S
	queued := 0
	for i := range eligible {
		if queued >= n {
			break
		}
		c, pickedAt := eligible[i].shard, eligible[i].access
		eligible[i].shard = zero
		if !c.TryMarkUnloadPending() {
			continue
		}
		req.PickedAt = pickedAt
		if !p.enqueue(p.backgroundCh, item[S]{shard: c, reason: r, req: req}) {
			break
		}
		queued++
	}
	for i := range eligible {
		eligible[i].shard = zero
	}
	return queued
}

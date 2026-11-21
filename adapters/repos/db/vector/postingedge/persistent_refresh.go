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

package postingedge

import (
	"context"

	"github.com/sirupsen/logrus"
)

// The refresh of the edges of the index a shard opens (grow_refresh.go):
// every time the index doubles, the vectors whose edges were built when it
// held half of what it holds now, or less, have them built again.
//
// A pass refreshes one vector at a time, as a vector is inserted: its search
// next to the searches, and then its writes with nobody else in. It is one
// writer with the inserts, and it goes in line with them: while a pass is on,
// every insert refreshes refreshPerInsert vectors after its own writes, so
// that the pass ends long before the index doubles again however the inserts
// come. Left to take its turns between two batches of inserts, a pass in the
// background gets a couple of vectors a batch, and never ends while the index
// grows. What runs in the background takes the turns nobody else wants: it
// ends the pass when the inserts stop.

// refreshPerInsert is how many vectors an insert refreshes while a pass is
// on. A pass takes half the vectors of the index, at most: with one an
// insert it ends by the time the index has grown by half.
const refreshPerInsert = 1

// refreshStep refreshes the next vector of the pass that is on, starting one
// if it is due, and ends the pass when no vector is left. It tells whether it
// refreshed one. The caller holds writeMu: it is the one writer, from the
// search of the vector to its writes.
func (p *Persistent) refreshStep(ctx context.Context) (bool, error) {
	g := p.g
	if g == nil {
		return false, nil
	}
	plan := g.own
	plan.work = nil

	p.mu.RLock()
	if p.closed || p.failed != nil {
		p.mu.RUnlock()
		return false, nil
	}
	var err error
	if p.refreshing == nil {
		if !g.refreshDue() {
			p.mu.RUnlock()
			return false, nil
		}
		if p.refreshing, err = g.beginRefresh(); err != nil {
			p.mu.RUnlock()
			return false, err
		}
	}
	pass := p.refreshing
	docID, found := pass.nextStale()
	if found {
		err = g.planRefresh(ctx, plan, docID)
	}
	p.mu.RUnlock()
	if err != nil {
		return false, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if !found {
		// The pass is done: the next is due when the index holds twice what
		// it held when this one started.
		pass.finish()
		p.refreshing = nil
		p.dirty = true
		p.refreshes.Add(1)
		p.logger.WithFields(logrus.Fields{
			"action":    "postingedge_refresh",
			"id":        p.cfg.ID,
			"refreshed": g.refreshed.Refreshed,
			"vectors":   pass.held,
		}).Info("built again the edges of the vectors that had them from half the index")
		return false, nil
	}
	if err := g.applyRefresh(plan); err != nil {
		p.failed = err
		return false, err
	}
	p.dirty = true
	return true, nil
}

// refreshInLine has an insert pay its share of the pass that is on. The
// caller holds writeMu.
func (p *Persistent) refreshInLine(ctx context.Context) error {
	for range refreshPerInsert {
		if p.g == nil || p.refreshing == nil && !p.g.refreshDue() {
			return nil
		}
		// What it writes is not left halfway because whoever asked for the
		// insert stopped waiting.
		if _, err := p.refreshStep(context.WithoutCancel(ctx)); err != nil {
			return err
		}
	}
	return nil
}

// refresh takes the pass that is on, or the one that is due, to its end,
// next to the searches and the inserts: a vector at a time, letting whoever
// else writes in between. If it is told to stop it stops between two
// vectors, and whoever comes next goes on from there. A write of it that
// fails halfway stops the index.
func (p *Persistent) refresh(ctx context.Context) error {
	for done := 0; ; done++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.writeMu.Lock()
		found, err := p.refreshStep(ctx)
		p.writeMu.Unlock()
		if err != nil {
			return err
		}
		if !found {
			// The pass ended, or there is none, or the index is shut down.
			break
		}
		// Nobody flushes after what the index writes by itself, as a shard
		// does after a request: the pass hands its writes to the system as
		// it goes.
		if done%maintenanceFlushEvery == maintenanceFlushEvery-1 {
			if err := p.Flush(); err != nil {
				return err
			}
		}
	}
	return p.Flush()
}

// refreshOn tells whether a refresh of the edges is on or due.
func (p *Persistent) refreshOn() bool {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.mu.RLock()
	defer p.mu.RUnlock()
	return !p.closed && p.failed == nil && p.g != nil && (p.refreshing != nil || p.g.refreshDue())
}

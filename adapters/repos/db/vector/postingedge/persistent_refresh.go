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

// refresh builds again the edges of the vectors whose edges were built when
// the index held half of what it holds now, or less (grow_refresh.go), next to
// the searches and the inserts. Every vector is refreshed as a vector is
// inserted: its search with the searches, and then its writes with nobody
// else in. One writer at a time, so an insert waits for the refresh of one
// vector, and the refresh of the next waits for an insert. If it is told to
// stop it stops between two vectors, and the next pass takes those that are
// left. A write of it that fails halfway stops the index.
func (p *Persistent) refresh(ctx context.Context) error {
	p.passMu.Lock()
	defer p.passMu.Unlock()
	p.mu.RLock()
	if p.closed || p.failed != nil || p.g == nil {
		p.mu.RUnlock()
		return nil
	}
	g := p.g
	pass, err := g.beginRefresh()
	p.mu.RUnlock()
	if err != nil {
		return err
	}
	for done := 0; ; done++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		found, stopped, err := p.refreshNext(ctx, pass)
		if err != nil || stopped {
			return err
		}
		if !found {
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
	p.writeMu.Lock()
	p.mu.Lock()
	if !p.closed && p.failed == nil {
		pass.finish()
		p.dirty = true
	}
	refreshed := g.refreshed.Refreshed
	p.mu.Unlock()
	p.writeMu.Unlock()
	if err := p.Flush(); err != nil {
		return err
	}
	p.refreshes.Add(1)
	p.logger.WithFields(logrus.Fields{
		"action":    "postingedge_refresh",
		"id":        p.cfg.ID,
		"refreshed": refreshed,
		"vectors":   pass.held,
	}).Info("built again the edges of the vectors that had them from half the index")
	return nil
}

// refreshNext refreshes the next vector of a pass. It tells whether there was
// one, and whether the index was shut down or had stopped.
func (p *Persistent) refreshNext(ctx context.Context, pass *growRefresh) (found, stopped bool, err error) {
	// The one writer, from the search of the vector to its writes: nothing
	// changes between the two.
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	g := pass.g
	plan := g.own
	plan.work = nil

	p.mu.RLock()
	if p.closed || p.failed != nil {
		p.mu.RUnlock()
		return false, true, nil
	}
	docID, found := pass.nextStale()
	if found {
		err = g.planRefresh(ctx, plan, docID)
	}
	p.mu.RUnlock()
	if !found || err != nil {
		return found, false, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := g.applyRefresh(plan); err != nil {
		p.failed = err
		return true, true, err
	}
	p.dirty = true
	return true, false, nil
}

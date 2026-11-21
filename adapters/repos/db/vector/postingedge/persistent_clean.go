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

// The cleanup of the deleted vectors of the index a shard opens
// (grow_clean.go): once enough vectors are marked deleted, a pass takes out
// those that are marked when it starts.
//
// A pass goes a step at a time: it reads the lists of the vectors that leave
// as a search reads, and then writes one posting at a time with nobody else
// in. It is one writer with the inserts and the deletes, and it goes in line
// with them, as the refresh of the edges does (persistent_refresh.go): while
// a pass is on, every insert and every delete takes it a step further, so
// that it ends however the writes come. What runs in the background takes
// the turns nobody else wants, and ends the pass when the writes stop.

// cleanGatherStep is how many rows of edges a pass over the deleted vectors
// reads in a step, while it only reads.
const cleanGatherStep = 16

// mergeUnder is the size under which a posting joins its nearest one after a
// cleanup, for a split threshold: 177 members for the 360 of the default,
// three quarters of the mean posting, as the experiments merged (v6.1).
func mergeUnder(threshold int) int {
	return threshold * 177 / DefaultSplitThreshold
}

// cleanStep takes the pass that is on a step further, starting one if it is
// due, or if force is set and a vector is marked, and tells whether a pass is
// still on. The caller holds writeMu: it is the one writer. A write that
// fails halfway stops the index, as that of an insert does.
func (p *Persistent) cleanStep(ctx context.Context, force bool) (on bool, err error) {
	g := p.g
	if g == nil {
		return false, nil
	}
	p.mu.RLock()
	if p.closed || p.failed != nil {
		p.mu.RUnlock()
		return false, nil
	}
	if p.cleaning == nil {
		if !g.cleanDue() && (!force || g.deletedCount == 0 || !g.consolidated()) {
			p.mu.RUnlock()
			return false, nil
		}
		if p.cleaning, err = g.beginClean(mergeUnder(g.meta.Threshold), g.meta.Threshold); err != nil {
			p.mu.RUnlock()
			return false, err
		}
	}
	pass := p.cleaning
	done := false
	if pass.gathers() {
		// It only reads: next to the searches.
		done, err = pass.step(ctx, cleanGatherStep)
		p.mu.RUnlock()
		if err != nil {
			return true, err
		}
	} else {
		p.mu.RUnlock()
		p.mu.Lock()
		// What it writes of a posting is not left halfway because whoever
		// asked stopped waiting.
		done, err = pass.step(context.WithoutCancel(ctx), 1)
		if err != nil {
			p.failed = err
		}
		p.dirty = true
		p.mu.Unlock()
		if err != nil {
			return false, err
		}
	}
	if !done {
		return true, nil
	}
	p.cleaning = nil
	p.cleans.Add(1)
	p.mu.RLock()
	removed, live := g.cleaned.Removed, g.meta.Live
	p.mu.RUnlock()
	p.logger.WithFields(logrus.Fields{
		"action":  "postingedge_clean",
		"id":      p.cfg.ID,
		"removed": removed,
		"vectors": live,
	}).Info("took the deleted vectors out of the index")
	return false, nil
}

// cleanInLine has a write pay its share of the pass that is on: the given
// number of steps. The caller holds writeMu.
func (p *Persistent) cleanInLine(ctx context.Context, steps int) error {
	if p.cfg.noMaintenance {
		return nil
	}
	for range steps {
		if p.g == nil || p.cleaning == nil && !p.g.cleanDue() {
			return nil
		}
		if _, err := p.cleanStep(ctx, false); err != nil {
			return err
		}
	}
	return nil
}

// cleanOn tells whether a pass over the deleted vectors is on or due.
func (p *Persistent) cleanOn() bool {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.mu.RLock()
	defer p.mu.RUnlock()
	return !p.closed && p.failed == nil && p.g != nil && (p.cleaning != nil || p.g.cleanDue())
}

// cleanPasses takes the pass that is on to its end, and the ones that follow
// while one is due, or, with force, while a vector is marked: a step at a
// time, letting whoever else writes in between. If it is told to stop it
// stops between two steps, where what there is is an index, and whoever comes
// next goes on from there.
func (p *Persistent) cleanPasses(ctx context.Context, force bool) error {
	for steps := 0; ; steps++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.writeMu.Lock()
		on, err := p.cleanStep(ctx, force)
		p.writeMu.Unlock()
		if err != nil {
			return err
		}
		if !on {
			// A pass ended, or there is none to start.
			again := p.cleanOn()
			if force && !again {
				p.mu.RLock()
				again = !p.closed && p.failed == nil && p.g != nil && p.g.consolidated() && p.g.deletedCount > 0
				p.mu.RUnlock()
			}
			if !again {
				break
			}
		}
		// Nobody flushes after what the index writes by itself, as a shard
		// does after a request: the pass hands its writes to the system as
		// it goes.
		if steps%maintenanceFlushEvery == maintenanceFlushEvery-1 {
			if err := p.Flush(); err != nil {
				return err
			}
		}
	}
	return p.Flush()
}

// clean takes out of the index every vector that is marked deleted, however
// few they are: what the tests and the experiments call. With an index whose
// edges are a value per list, which is not cleaned, it says so.
func (p *Persistent) clean(ctx context.Context) error {
	p.mu.RLock()
	g := p.g
	unsupported := !p.closed && p.failed == nil && g != nil && !g.consolidated() && g.deletedCount > 0
	p.mu.RUnlock()
	if unsupported {
		_, err := g.beginClean(0, 0)
		return err
	}
	return p.cleanPasses(ctx, true)
}

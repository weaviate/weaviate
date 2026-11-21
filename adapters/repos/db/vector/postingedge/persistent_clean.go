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

// cleanGatherStep is how many rows of edges a pass over the deleted vectors
// reads between two looks at whether anybody waits to write.
const cleanGatherStep = 16

// mergeUnder is the size under which a posting joins its nearest one after a
// cleanup, for a split threshold: 177 members for the 360 of the default,
// three quarters of the mean posting, as the experiments merged (v6.1).
func mergeUnder(threshold int) int {
	return threshold * 177 / DefaultSplitThreshold
}

// clean takes out of the index the vectors that are marked deleted
// (grow_clean.go), next to the searches and the inserts: it reads their lists
// as a search reads, and then writes one posting at a time with nobody else
// in, letting whoever waits in between. If it is told to stop it stops
// between two postings, where what is on disk is an index, and the next pass
// starts again. A write of it that fails halfway stops the index, as that of
// an insert does.
func (p *Persistent) clean(ctx context.Context) error {
	p.passMu.Lock()
	defer p.passMu.Unlock()
	p.mu.RLock()
	if p.closed || p.failed != nil || p.g == nil {
		p.mu.RUnlock()
		return nil
	}
	g := p.g
	pass, err := g.beginClean(mergeUnder(g.meta.Threshold), g.meta.Threshold)
	p.mu.RUnlock()
	if err != nil {
		return err
	}
	written := 0
	for done := false; !done; {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pass.gathers() {
			p.mu.RLock()
			if p.closed || p.failed != nil {
				p.mu.RUnlock()
				return nil
			}
			done, err = pass.step(ctx, cleanGatherStep)
			p.mu.RUnlock()
			if err != nil {
				return err
			}
			continue
		}
		var stopped bool
		if done, stopped, err = p.cleanPosting(ctx, pass); err != nil || stopped {
			return err
		}
		// Nobody flushes after what the index writes by itself, as a shard
		// does after a request: the pass hands its writes to the system as
		// it goes.
		if written++; written%maintenanceFlushEvery == 0 {
			if err := p.Flush(); err != nil {
				return err
			}
		}
	}
	if err := p.Flush(); err != nil {
		return err
	}
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
	return nil
}

// cleanPosting takes a pass over the deleted vectors one posting further,
// with nobody else in the index. It tells whether the pass is done, and
// whether the index was shut down or had stopped.
func (p *Persistent) cleanPosting(ctx context.Context, pass *growClean) (done, stopped bool, err error) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.failed != nil {
		return false, true, nil
	}
	// What it writes of a posting is not left halfway because whoever asked
	// stopped waiting.
	if done, err = pass.step(context.WithoutCancel(ctx), 1); err != nil {
		p.failed = err
		return false, true, err
	}
	p.dirty = true
	return done, false, nil
}

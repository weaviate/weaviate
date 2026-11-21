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
	"sync"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// What the index a shard opens does in the background: one goroutine of its
// own, which does one thing at a time when it is told there may be work, and
// ends when the index is shut down or dropped.
//
// It is a goroutine of the index and not a queue of the scheduler of the
// node, which is where this work belongs once the index shares the workers
// of the node (integration-plan.md, section 7): a provisional choice.
//
// What it does takes the locks of the index as its other callers do. A pass
// over the vectors reads next to the searches and the inserts, a few vectors
// at a time, and only its end keeps everybody else out.

// layoutStep is how many vectors a pass over the vectors copies between two
// looks at whether anybody waits to write.
const layoutStep = 256

// startMaintenance starts the goroutine. With noMaintenance there is none,
// and whoever set it calls the work itself.
func (p *Persistent) startMaintenance() {
	p.wake = make(chan struct{}, 1)
	if p.cfg.noMaintenance {
		p.stopMaintenance = func() {}
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var once sync.Once
	p.stopMaintenance = func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	enterrors.GoWrapper(func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.wake:
			}
			if err := p.maintain(ctx); err != nil && ctx.Err() == nil {
				p.logger.WithFields(logrus.Fields{
					"action": "postingedge_maintenance",
					"id":     p.cfg.ID,
				}).Errorf("the maintenance of the index stopped before it ended: %v", err)
			}
		}
	}, p.logger)
	// Whatever was due when the index was last open.
	p.wakeMaintenance()
}

// wakeMaintenance tells the goroutine there may be work. It never waits.
func (p *Persistent) wakeMaintenance() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// maintain does what is due, one thing after another: first the deleted
// vectors leave, and then the vectors that are left are laid out.
func (p *Persistent) maintain(ctx context.Context) error {
	p.mu.RLock()
	due := !p.closed && p.failed == nil && p.g != nil && p.g.cleanDue()
	p.mu.RUnlock()
	if due {
		if err := p.clean(ctx); err != nil {
			return err
		}
	}

	p.mu.RLock()
	due = !p.closed && p.failed == nil && p.g != nil && p.g.layoutDue() &&
		int64(p.g.nextSlot) >= p.layoutNotBefore.Load()
	slots := int64(0)
	if due {
		slots = int64(p.g.nextSlot)
	}
	p.mu.RUnlock()
	if !due {
		return nil
	}
	if err := p.layOut(ctx); err != nil {
		// A pass writes every vector again: one that failed is not tried
		// again with every insert, but once the index has grown by a tenth
		// more.
		p.layoutNotBefore.Store(slots + max(slots/10, layoutStep))
		return err
	}
	return nil
}

// layOut writes the exact vectors again in the order of their postings
// (grow_layout.go), next to the searches and the inserts. Its end waits for
// the insert that is running and keeps the searches out while it copies what
// came in since it started and swaps the files. If it fails, or the index is
// shut down first, the index keeps the files it had.
func (p *Persistent) layOut(ctx context.Context) error {
	// One pass at a time: two would write the files of the same generation.
	p.passMu.Lock()
	defer p.passMu.Unlock()
	p.mu.RLock()
	if p.closed || p.failed != nil || p.g == nil {
		p.mu.RUnlock()
		return nil
	}
	g := p.g
	pass, err := g.beginLayout()
	p.mu.RUnlock()
	if err != nil {
		return err
	}
	for done := false; !done; {
		if err := ctx.Err(); err != nil {
			pass.abort()
			return err
		}
		p.mu.RLock()
		if p.closed || p.failed != nil {
			p.mu.RUnlock()
			pass.abort()
			return nil
		}
		done, err = pass.step(layoutStep)
		p.mu.RUnlock()
		if err != nil {
			pass.abort()
			return err
		}
	}
	// The bulk of the new file reaches the disk before anybody waits for it.
	if err := pass.settle(); err != nil {
		pass.abort()
		return err
	}

	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.failed != nil || ctx.Err() != nil {
		pass.abort()
		return ctx.Err()
	}
	if err := pass.finish(); err != nil {
		return err
	}
	p.layouts.Add(1)
	p.logger.WithFields(logrus.Fields{
		"action":     "postingedge_layout",
		"id":         p.cfg.ID,
		"vectors":    g.nextSlot,
		"generation": g.meta.VectorGen,
	}).Info("wrote the vectors of the index again in the order of their postings")
	return nil
}
